/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "transaction_manager.h"
#include "record/rm_file_handle.h"
#include "system/sm_manager.h"
#include "common/context.h"

#include <algorithm>

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

/**
 * @brief 释放事务持有的全部锁（2PL 下通常在 commit/abort 阶段一次性释放）。
 *
 * 设计说明（答辩高频点）：
 * - 我们把“释放锁”集中在提交/回滚阶段做，属于 Strict 2PL 的常见实现方式。
 * - lock_set_ 里存的是 LockDataId（表锁/行锁统一表示），逐个调用 LockManager::unlock 即可。
 * - unlock 需要做两件事：从锁表删除该事务的请求、必要时唤醒/让后续请求可继续（no-wait 下通常直接失败，不需要 cv）。
 */
static void release_all_locks(TransactionManager *txn_mgr, Transaction *txn) {
    if (txn == nullptr) return;
    auto lock_mgr = txn_mgr->get_lock_manager();
    if (lock_mgr == nullptr) return;

    // 这里复制一份 lock_set 是为了避免 unlock 内部如果也会更新 txn->lock_set_ 导致迭代器失效。
    std::vector<LockDataId> to_release;
    to_release.reserve(txn->get_lock_set()->size());
    for (const auto &lid : *(txn->get_lock_set())) {
        to_release.push_back(lid);
    }
    for (const auto &lid : to_release) {
        lock_mgr->unlock(txn, lid);
    }
    txn->get_lock_set()->clear();
}

/**
 * @brief 清理 write_set_ 中的 WriteRecord*，避免内存泄漏。
 *
 * 事务写集合里用的是裸指针（WriteRecord*），因此 commit/abort 结束时必须 delete。
 * 我们在这里集中清理，确保：
 * - commit：不需要回滚，只清理；
 * - abort：先回滚再清理。
 */
static void cleanup_write_set(Transaction *txn) {
    if (txn == nullptr) return;
    auto ws = txn->get_write_set();
    if (!ws) return;
    for (auto *wr : *ws) {
        delete wr;
    }
    ws->clear();
}

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
Transaction * TransactionManager::begin(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 判断传入事务参数是否为空指针
    // 2. 如果为空指针，创建新事务
    // 3. 把开始事务加入到全局事务表中
    // 4. 返回当前事务指针

    // 1) 已有事务：直接返回（一般用于恢复/上层显式管理的场景）
    if (txn != nullptr) {
        txn->set_state(TransactionState::GROWING);
        return txn;
    }

    // 2) 新事务：分配事务ID + 构造 Transaction 对象
    txn_id_t txn_id = next_txn_id_.fetch_add(1);
    auto *new_txn = new Transaction(txn_id);
    new_txn->set_state(TransactionState::GROWING);
    new_txn->set_start_ts(next_timestamp_.fetch_add(1));

    // 3) 插入全局事务表（txn_map 是跨线程共享结构，需要互斥保护）
    {
        std::unique_lock<std::mutex> lock(latch_);
        TransactionManager::txn_map.emplace(txn_id, new_txn);
    }

    // 4) WAL：本实验的事务测试不强依赖日志内容，但需要保证 log_mgr 不为空时能正常落盘。
    // 如果后续实验需要写 BEGIN/COMMIT/ABORT 日志，可在此补充。
    (void)log_manager;

    return new_txn;
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 如果存在未提交的写操作，提交所有的写操作
    // 2. 释放所有锁
    // 3. 释放事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态
    if (txn == nullptr) return;

    // 1) 本系统的写操作是“写穿”（执行时已写入 buffer/page），提交阶段无需额外 apply。
    //    真正关键是：释放锁 + 刷日志 + 清理 write_set。

    // 5) 状态先进入 SHRINKING（严格 2PL：释放锁意味着进入 shrinking；之后标记 committed）
    txn->set_state(TransactionState::SHRINKING);

    // 2 & 3) 释放锁并清空锁集
    release_all_locks(this, txn);

    // 4) WAL 刷盘：保证 commit 语义可持久（本实验测试中也会依赖 output 的稳定性）
    if (log_manager != nullptr) {
        log_manager->flush_log_to_disk();
    }

    // 清理写集合（避免泄漏）
    cleanup_write_set(txn);

    txn->set_state(TransactionState::COMMITTED);

    // 提交后可以从全局事务表移除，避免 txn_map 无限增长。
    // 注意：rmdb.cpp::SetTransaction 会检查 COMMITTED/ABORTED 然后创建新事务，因此移除是安全的。
    {
        std::unique_lock<std::mutex> lock(latch_);
        TransactionManager::txn_map.erase(txn->get_transaction_id());
    }
    delete txn;
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Transaction * txn, LogManager *log_manager) {
    // Todo:
    // 1. 回滚所有写操作
    // 2. 释放所有锁
    // 3. 清空事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态
    if (txn == nullptr) return;

    // 事务进入 ABORTED/SHRINKING：从这一步开始我们不应该再把“回滚产生的写”记入 write_set_。
    // 因此我们先标记为 SHRINKING（或 ABORTED），下层如果有“写集合记录逻辑”应当以此为判断条件。
    txn->set_state(TransactionState::SHRINKING);

    // 1) 回滚写集合：必须“逆序”执行（后写先撤销），否则会破坏一致性。
    //    例如：先 UPDATE 再 DELETE 同一条记录，回滚时必须先撤销 DELETE（恢复记录），再撤销 UPDATE（恢复旧值）。
    auto ws = txn->get_write_set();
    if (ws != nullptr) {
        // 这里构造一个“无事务”的 Context 来调用存储层接口：
        // - txn_ = nullptr：避免在 undo 时再次 append_write_record（否则无限增长/递归）。
        // - lock_mgr/log_mgr 仍可传入，便于后续扩展（例如 undo 也需要写 CLR 日志）。
        Context undo_ctx(lock_manager_, log_manager, nullptr);

        for (auto it = ws->rbegin(); it != ws->rend(); ++it) {
            WriteRecord *wr = *it;
            if (wr == nullptr) continue;

            const std::string &tab_name = wr->GetTableName();
            RmFileHandle *fh = sm_manager_->fhs_.at(tab_name).get();

            // 统一说明（答辩高频点）：
            // - INSERT 的 undo：删除这条新插入的记录（把位图 bit 置 0）
            // - DELETE 的 undo：把旧记录插回原 rid（必须原位插回，否则 rid 会变化，索引/外键等都会错）
            // - UPDATE 的 undo：用 before image 覆盖回去（update_record）
            switch (wr->GetWriteType()) {
                case WType::INSERT_TUPLE: {
                    fh->delete_record(wr->GetRid(), &undo_ctx);
                    break;
                }
                case WType::DELETE_TUPLE: {
                    // 关键：原位插回（需要实现 RmFileHandle::insert_record(const Rid&, char*)）
                    fh->insert_record(wr->GetRid(), wr->GetRecord().data);
                    break;
                }
                case WType::UPDATE_TUPLE: {
                    fh->update_record(wr->GetRid(), wr->GetRecord().data, &undo_ctx);
                    break;
                }
                default: {
                    throw InternalError("TransactionManager::abort: unknown write type");
                }
            }
        }
    }

    // 2 & 3) 释放锁并清理锁集
    release_all_locks(this, txn);

    // 4) 刷日志
    if (log_manager != nullptr) {
        log_manager->flush_log_to_disk();
    }

    // 清理写集合
    cleanup_write_set(txn);

    txn->set_state(TransactionState::ABORTED);

    // 从全局事务表移除，释放事务对象
    {
        std::unique_lock<std::mutex> lock(latch_);
        TransactionManager::txn_map.erase(txn->get_transaction_id());
    }
    delete txn;
}