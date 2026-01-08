/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_file_handle.h"

/**
 * @brief 是否需要把一次写操作记录到事务 write_set_ 中？
 *
 * 说明（答辩高频点）：
 * - write_set_ 用于 abort 时做 UNDO。
 * - UNDO 过程本身会调用 delete/update/insert 等接口，如果我们不做保护，会出现：
 *   “回滚时又往 write_set_ 里追加回滚操作” -> 无限膨胀甚至死循环。
 * - 这里用 txn->state 是否处于 GROWING 来作为“正常执行阶段”的判定：
 *   - 正常执行：GROWING -> 记录写集合；
 *   - commit/abort 阶段：SHRINKING/ABORTED/COMMITTED -> 不再记录。
 */
static inline bool should_record_write(Context *context) {
    return context != nullptr && context->txn_ != nullptr &&
           context->txn_->get_state() == TransactionState::GROWING;
}

/**
 * @description: 获取当前表中记录号为rid的记录
 * @param {Rid&} rid 记录号，指定记录的位置
 * @param {Context*} context
 * @return {unique_ptr<RmRecord>} rid对应的记录对象指针
 */
std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid& rid, Context* context) const {
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 初始化一个指向RmRecord的指针（赋值其内部的data和size）
    
    // ========== 并发控制（Strict 2PL）==========
    // 读记录需要持有：
    // - 表级 IS（意向共享）：声明“我要读表中的某些行”，便于多粒度锁正确工作
    // - 行级 S（共享锁）：防止其他事务在我提交前写该行，避免脏读/不可重复读
    if (context != nullptr && context->txn_ != nullptr && context->lock_mgr_ != nullptr) {
        context->lock_mgr_->lock_IS_on_table(context->txn_, fd_);
        context->lock_mgr_->lock_shared_on_record(context->txn_, rid, fd_);
    }

    // 1. 获取指定记录所在的page handle
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    
    // 检查该slot是否有记录
    if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }
    
    // 2. 初始化一个指向RmRecord的指针
    std::unique_ptr<RmRecord> record = std::make_unique<RmRecord>(file_hdr_.record_size);
    
    // 复制记录数据
    char* slot_data = page_handle.get_slot(rid.slot_no);
    memcpy(record->data, slot_data, file_hdr_.record_size);
    
    // 释放page handle
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
    
    return record;
}

/**
 * @description: 在当前表中插入一条记录，不指定插入位置
 * @param {char*} buf 要插入的记录的数据
 * @param {Context*} context
 * @return {Rid} 插入的记录的记录号（位置）
 */
Rid RmFileHandle::insert_record(char* buf, Context* context) {
    // Todo:
    // 1. 获取当前未满的page handle
    // 2. 在page handle中找到空闲slot位置
    // 3. 将buf复制到空闲slot位置
    // 4. 更新page_handle.page_hdr中的数据结构
    // 注意考虑插入一条记录后页面已满的情况，需要更新file_hdr_.first_free_page_no
    
    // 1. 获取当前未满的page handle
    // ========== 并发控制（Strict 2PL）==========
    // 插入属于写操作：只拿表级 IX（意向排他）即可表达“我要在表里写一些行”。
    // 行级 X 对“新插入的记录”在本实验基础测试中不是必须，但表级 IX 是后续支持更强隔离（如幻读）的一块基础。
    if (context != nullptr && context->txn_ != nullptr && context->lock_mgr_ != nullptr) {
        context->lock_mgr_->lock_IX_on_table(context->txn_, fd_);
    }

    RmPageHandle page_handle = create_page_handle();
    
    // 2. 在page handle中找到空闲slot位置
    int slot_no = Bitmap::first_bit(false, page_handle.bitmap, file_hdr_.num_records_per_page);
    
    // 检查是否找到空闲slot
    if (slot_no == file_hdr_.num_records_per_page) {
        throw InternalError("No free slot available");
    }
    
    // 3. 将buf复制到空闲slot位置
    char* slot_data = page_handle.get_slot(slot_no);
    memcpy(slot_data, buf, file_hdr_.record_size);
    
    // 4. 更新page_handle.page_hdr中的数据结构
    Bitmap::set(page_handle.bitmap, slot_no);
    page_handle.page_hdr->num_records++;
    
    // 如果插入后页面已满，需要更新file_hdr_.first_free_page_no
    if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page) {
        file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
    }
    
    // 构造Rid并返回
    Rid rid = {page_handle.page->get_page_id().page_no, slot_no};

    // 事务写集合记录（用于 abort 时删除这条新插入的记录）
    if (should_record_write(context)) {
        // tab_name_：这里用 DiskManager 的 fd->path 映射来得到表名（创建/打开表文件时用的就是 tab_name）
        std::string tab_name = disk_manager_->get_file_name(fd_);
        context->txn_->append_write_record(new WriteRecord(WType::INSERT_TUPLE, tab_name, rid));
    }
    
    // 释放page handle（标记为dirty）
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
    
    return rid;
}

/**
 * @description: 在当前表中的指定位置插入一条记录
 * @param {Rid&} rid 要插入记录的位置
 * @param {char*} buf 要插入记录的数据
 */
void RmFileHandle::insert_record(const Rid& rid, char* buf) {
    /**
     * @brief 在“指定位置”插入记录（原位插回）
     *
     * 这是事务回滚 DELETE 的关键能力：回滚时必须把旧记录插回原 rid。
     * 如果不原位插回，会产生两个严重问题：
     * 1) RID 改变：索引/上层算子持有的 rid 失效，查询会读不到正确数据；
     * 2) 空闲链表/位图错乱：可能导致重复插入/覆盖已有记录。
     */

    // 1. page_no 合法性检查
    if (rid.page_no <= RM_FILE_HDR_PAGE || rid.page_no >= file_hdr_.num_pages) {
        throw PageNotExistError(disk_manager_->get_file_name(fd_), rid.page_no);
    }

    // 2. fetch 对应页面
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);

    // 3. slot 合法性检查
    if (rid.slot_no < 0 || rid.slot_no >= file_hdr_.num_records_per_page) {
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        throw InternalError("RmFileHandle::insert_record(rid): invalid slot_no");
    }

    // 4. 目标 slot 必须为空，否则等价于“覆盖记录”，这会破坏一致性
    if (Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        throw InternalError("RmFileHandle::insert_record(rid): slot already occupied");
    }

    // 5. 插入记录：写数据 + bitmap 置 1 + num_records++
    memcpy(page_handle.get_slot(rid.slot_no), buf, file_hdr_.record_size);
    Bitmap::set(page_handle.bitmap, rid.slot_no);
    page_handle.page_hdr->num_records++;

    // 6. 如果该页之前是满页（没有空闲 slot），而现在插入后仍可能是满页/非满页：
    //    这里只处理“页从满->非满”的反向情况不需要；这里是插入，所以只需处理“页从非满->满”：
    if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page) {
        // 如果当前页被 first_free_page_no 指向，需要把 first_free_page_no 前移到 next_free_page_no
        if (file_hdr_.first_free_page_no == rid.page_no) {
            file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
        }
    }

    // 7. 释放页面（dirty=true）
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

/**
 * @description: 删除记录文件中记录号为rid的记录
 * @param {Rid&} rid 要删除的记录的记录号（位置）
 * @param {Context*} context
 */
void RmFileHandle::delete_record(const Rid& rid, Context* context) {
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 更新page_handle.page_hdr中的数据结构
    // 注意考虑删除一条记录后页面未满的情况，需要调用release_page_handle()
    
    // 1. 获取指定记录所在的page handle
    // ========== 并发控制（Strict 2PL）==========
    // 删除属于写操作：表 IX + 行 X
    if (context != nullptr && context->txn_ != nullptr && context->lock_mgr_ != nullptr) {
        context->lock_mgr_->lock_IX_on_table(context->txn_, fd_);
        context->lock_mgr_->lock_exclusive_on_record(context->txn_, rid, fd_);
    }

    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    
    // 检查该slot是否有记录
    if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }
    
    // 记录删除前页面是否已满
    bool was_full = (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page);

    // 事务写集合记录（用于 abort 时把旧记录插回）
    if (should_record_write(context)) {
        std::string tab_name = disk_manager_->get_file_name(fd_);
        // DELETE/UPDATE 的 WriteRecord 需要保存 before image
        RmRecord before(file_hdr_.record_size);
        memcpy(before.data, page_handle.get_slot(rid.slot_no), file_hdr_.record_size);
        context->txn_->append_write_record(new WriteRecord(WType::DELETE_TUPLE, tab_name, rid, before));
    }
    
    // 2. 更新page_handle.page_hdr中的数据结构
    Bitmap::reset(page_handle.bitmap, rid.slot_no);
    page_handle.page_hdr->num_records--;
    
    // 如果删除前页面已满，删除后变为未满，需要调用release_page_handle()
    if (was_full) {
        release_page_handle(page_handle);
    }
    
    // 释放page handle（标记为dirty）
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}


/**
 * @description: 更新记录文件中记录号为rid的记录
 * @param {Rid&} rid 要更新的记录的记录号（位置）
 * @param {char*} buf 新记录的数据
 * @param {Context*} context
 */
void RmFileHandle::update_record(const Rid& rid, char* buf, Context* context) {
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 更新记录
    
    // 1. 获取指定记录所在的page handle
    // ========== 并发控制（Strict 2PL）==========
    // 更新属于写操作：表 IX + 行 X
    // 注意：如果该事务之前在扫描阶段对该行拿过 S 锁，这里会触发 S->X 升级。
    if (context != nullptr && context->txn_ != nullptr && context->lock_mgr_ != nullptr) {
        context->lock_mgr_->lock_IX_on_table(context->txn_, fd_);
        context->lock_mgr_->lock_exclusive_on_record(context->txn_, rid, fd_);
    }

    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    
    // 检查该slot是否有记录
    if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }

    // 事务写集合记录（用于 abort 时把旧值写回）
    if (should_record_write(context)) {
        std::string tab_name = disk_manager_->get_file_name(fd_);
        RmRecord before(file_hdr_.record_size);
        memcpy(before.data, page_handle.get_slot(rid.slot_no), file_hdr_.record_size);
        context->txn_->append_write_record(new WriteRecord(WType::UPDATE_TUPLE, tab_name, rid, before));
    }
    
    // 2. 更新记录
    char* slot_data = page_handle.get_slot(rid.slot_no);
    memcpy(slot_data, buf, file_hdr_.record_size);
    
    // 释放page handle（标记为dirty）
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

/**
 * 以下函数为辅助函数，仅提供参考，可以选择完成如下函数，也可以删除如下函数，在单元测试中不涉及如下函数接口的直接调用
*/
/**
 * @description: 获取指定页面的页面句柄
 * @param {int} page_no 页面号
 * @return {RmPageHandle} 指定页面的句柄
 */
RmPageHandle RmFileHandle::fetch_page_handle(int page_no) const {
    // Todo:
    // 使用缓冲池获取指定页面，并生成page_handle返回给上层
    // if page_no is invalid, throw PageNotExistError exception
    
    // 检查page_no是否有效
    if (page_no >= file_hdr_.num_pages) {
        throw PageNotExistError("", page_no);
    }
    
    // 使用缓冲池获取指定页面
    PageId page_id = {fd_, page_no};
    Page* page = buffer_pool_manager_->fetch_page(page_id);
    
    // 生成并返回RmPageHandle
    return RmPageHandle(&file_hdr_, page);
}

/**
 * @description: 创建一个新的page handle
 * @return {RmPageHandle} 新的PageHandle
 */
RmPageHandle RmFileHandle::create_new_page_handle() {
    // Todo:
    // 1.使用缓冲池来创建一个新page
    // 2.更新page handle中的相关信息
    // 3.更新file_hdr_
    
    // 1. 使用缓冲池创建一个新page
    PageId page_id = {fd_, INVALID_PAGE_ID};
    Page* page = buffer_pool_manager_->new_page(&page_id);
    
    // 2. 创建page handle
    RmPageHandle page_handle(&file_hdr_, page);
    
    // 初始化page header
    page_handle.page_hdr->next_free_page_no = RM_NO_PAGE;
    page_handle.page_hdr->num_records = 0;
    
    // 初始化bitmap
    Bitmap::init(page_handle.bitmap, file_hdr_.bitmap_size);
    
    // 3. 更新file_hdr_
    file_hdr_.num_pages++;
    file_hdr_.first_free_page_no = page_id.page_no;
    
    return page_handle;
}

/**
 * @brief 创建或获取一个空闲的page handle
 *
 * @return RmPageHandle 返回生成的空闲page handle
 * @note pin the page, remember to unpin it outside!
 */
RmPageHandle RmFileHandle::create_page_handle() {
    // Todo:
    // 1. 判断file_hdr_中是否还有空闲页
    //     1.1 没有空闲页：使用缓冲池来创建一个新page；可直接调用create_new_page_handle()
    //     1.2 有空闲页：直接获取第一个空闲页
    // 2. 生成page handle并返回给上层
    
    // 1. 判断是否有空闲页
    if (file_hdr_.first_free_page_no == RM_NO_PAGE) {
        // 1.1 没有空闲页，创建新页面
        return create_new_page_handle();
    }
    
    // 1.2 有空闲页，获取第一个空闲页
    return fetch_page_handle(file_hdr_.first_free_page_no);
}

/**
 * @description: 当一个页面从没有空闲空间的状态变为有空闲空间状态时，更新文件头和页头中空闲页面相关的元数据
 */
void RmFileHandle::release_page_handle(RmPageHandle&page_handle) {
    // Todo:
    // 当page从已满变成未满，考虑如何更新：
    // 1. page_handle.page_hdr->next_free_page_no
    // 2. file_hdr_.first_free_page_no
    
    // 1. 更新page_hdr的next_free_page_no，指向原来的第一个空闲页
    page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
    
    // 2. 更新file_hdr的first_free_page_no，指向当前页面
    file_hdr_.first_free_page_no = page_handle.page->get_page_id().page_no;
}