/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sm_manager.h"

#include <sys/stat.h>
#include <unistd.h>

#include <fstream>

#include "index/ix.h"
#include "record/rm.h"
#include "record_printer.h"

/**
 * @description: 判断是否为一个文件夹
 * @return {bool} 返回是否为一个文件夹
 * @param {string&} db_name 数据库文件名称，与文件夹同名
 */
bool SmManager::is_dir(const std::string& db_name) {
    struct stat st;
    return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

/**
 * @description: 创建数据库，所有的数据库相关文件都放在数据库同名文件夹下
 * @param {string&} db_name 数据库名称
 */
void SmManager::create_db(const std::string& db_name) {
    if (is_dir(db_name)) {
        throw DatabaseExistsError(db_name);
    }
    //为数据库创建一个子目录
    std::string cmd = "mkdir " + db_name;
    if (system(cmd.c_str()) < 0) {  // 创建一个名为db_name的目录
        throw UnixError();
    }
    if (chdir(db_name.c_str()) < 0) {  // 进入名为db_name的目录
        throw UnixError();
    }
    //创建系统目录
    DbMeta *new_db = new DbMeta();
    new_db->name_ = db_name;

    // 注意，此处ofstream会在当前目录创建(如果没有此文件先创建)和打开一个名为DB_META_NAME的文件
    std::ofstream ofs(DB_META_NAME);

    // 将new_db中的信息，按照定义好的operator<<操作符，写入到ofs打开的DB_META_NAME文件中
    ofs << *new_db;  // 注意：此处重载了操作符<<

    delete new_db;

    // 创建日志文件
    disk_manager_->create_file(LOG_FILE_NAME);

    // 回到根目录
    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 删除数据库，同时需要清空相关文件以及数据库同名文件夹
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::drop_db(const std::string& db_name) {
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    std::string cmd = "rm -r " + db_name;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

/**
 * @description: 打开数据库，找到数据库对应的文件夹，并加载数据库元数据和相关文件
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::open_db(const std::string& db_name) {
    // 1. 安全检查：如果文件夹不存在，说明数据库还没创建，直接抛出异常
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    // 2. 切换当前工作目录进入数据库文件夹。
    // 这步非常关键，因为后续的表文件、索引文件和 db.meta 都是以相对路径存储在这个文件夹下的。
    if (chdir(db_name.c_str()) < 0) {
        throw UnixError();
    }
    // 3. 读取元数据文件 (db.meta)。
    // 这个文件存储了当前数据库里有哪些表，每张表有哪些列，以及每张表上有哪些索引。
    {
        std::ifstream ifs(DB_META_NAME);
        if (!ifs.good()) {
            throw FileNotFoundError(DB_META_NAME);
        }
        // 使用重载的 >> 运算符，将文件内容反序列化到 db_ 成员变量中
        ifs >> db_;
    }
    // 4. 加载所有表的记录文件句柄 (fhs_)。
    // 系统启动时需要把磁盘上的文件打开，获取句柄后存入内存哈希表，以便后续增删改查算子能直接使用。
    fhs_.clear();
    for (auto &entry : db_.tabs_) {
        const std::string &tab_name = entry.first;
        // rm_manager_ 负责打开记录文件并返回一个文件句柄
        fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));
    }
    // 5. 加载所有索引的文件句柄 (ihs_)。
    // 索引也是以文件形式存储的，我们需要根据 TabMeta 记录的索引信息逐一打开。
    ihs_.clear();
    for (auto &entry : db_.tabs_) {
        auto &tab = entry.second;
        for (auto &index_meta : tab.indexes) {
            // 根据表名和列名列表，通过 ix_manager 计算出索引在磁盘上的文件名
            auto ih = ix_manager_->open_index(tab.name, index_meta.cols);
            std::string ix_name = ix_manager_->get_index_name(tab.name, index_meta.cols);
            // 将索引句柄存入哈希表，key 是索引文件名
            ihs_.emplace(ix_name, std::move(ih));
        }
    }
}

/**
 * @description: 把数据库相关的元数据刷入磁盘中
 */
void SmManager::flush_meta() {
    // 默认清空文件并重新写入当前的 db_ 结构。
    // 这是保证内存中的元数据变更（如新建表、删索引）能够持久化到磁盘的唯一手段。
    std::ofstream ofs(DB_META_NAME);
    ofs << db_;
}

/**
 * @description: 关闭数据库并把数据落盘
 */
void SmManager::close_db() {
    // 1. 将最新的元数据写回 db.meta 文件体体
    flush_meta();
    // 2. 依次关闭所有打开的表文件句柄。
    // close_file 会将记录文件的 header 信息刷盘，并确保 BufferPool 里的脏页全部写入磁盘。
    for (auto &entry : fhs_) {
        rm_manager_->close_file(entry.second.get());
    }
    fhs_.clear();
    // 3. 依次关闭所有打开的索引文件句柄。
    // 同理，close_index 会处理 B+ 树节点刷盘和文件头更新。
    for (auto &entry : ihs_) {
        ix_manager_->close_index(entry.second.get());
    }
    ihs_.clear();
    // 4. 清空内存中 db_ 结构体，标志当前没有打开任何数据库
    db_.name_.clear();
    db_.tabs_.clear();
    // 5. 退出数据库文件夹，回到父目录。
    // 这一步与 open_db 时的 chdir 对称，防止系统在后续操作中路径错乱。
    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 显示所有的表,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {Context*} context 
 */
void SmManager::show_tables(Context* context) {
    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);
    outfile << "| Tables |\n";
    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"Tables"}, context);
    printer.print_separator(context);
    for (auto &entry : db_.tabs_) {
        auto &tab = entry.second;
        printer.print_record({tab.name}, context);
        outfile << "| " << tab.name << " |\n";
    }
    printer.print_separator(context);
    outfile.close();
}

/**
 * @description: 显示表的元数据
 * @param {string&} tab_name 表名称
 * @param {Context*} context 
 */
void SmManager::desc_table(const std::string& tab_name, Context* context) {
    TabMeta &tab = db_.get_table(tab_name);

    std::vector<std::string> captions = {"Field", "Type", "Index"};
    RecordPrinter printer(captions.size());
    // Print header
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);
    // Print fields
    for (auto &col : tab.cols) {
        std::vector<std::string> field_info = {col.name, coltype2str(col.type), col.index ? "YES" : "NO"};
        printer.print_record(field_info, context);
    }
    // Print footer
    printer.print_separator(context);
}

/**
 * @description: 创建表
 * @param {string&} tab_name 表的名称
 * @param {vector<ColDef>&} col_defs 表的字段
 * @param {Context*} context 
 */
void SmManager::create_table(const std::string& tab_name, const std::vector<ColDef>& col_defs, Context* context) {
    if (db_.is_table(tab_name)) {
        throw TableExistsError(tab_name);
    }
    // Create table meta
    int curr_offset = 0;
    TabMeta tab;
    tab.name = tab_name;
    for (auto &col_def : col_defs) {
        ColMeta col = {.tab_name = tab_name,
                       .name = col_def.name,
                       .type = col_def.type,
                       .len = col_def.len,
                       .offset = curr_offset,
                       .index = false};
        curr_offset += col_def.len;
        tab.cols.push_back(col);
    }
    // Create & open record file
    int record_size = curr_offset;  // record_size就是col meta所占的大小（表的元数据也是以记录的形式进行存储的）
    rm_manager_->create_file(tab_name, record_size);
    db_.tabs_[tab_name] = tab;
    // fhs_[tab_name] = rm_manager_->open_file(tab_name);
    fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));

    flush_meta();
}

/**
 * @description: 删除表
 * @param {string&} tab_name 表的名称
 * @param {Context*} context
 */
void SmManager::drop_table(const std::string& tab_name, Context* context) {
    // 1. 检查表是否存在体
    if (!db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }
    // 2. 级联删除索引。
    // 在删除表之前，必须先清理该表上的所有索引，否则会留下孤立的索引文件。
    TabMeta &tab = db_.get_table(tab_name);
    for (auto &index_meta : tab.indexes) {
        // 先根据索引定义的列，计算出它在磁盘的文件名
        std::string ix_name = ix_manager_->get_index_name(tab_name, index_meta.cols);
        auto it_ih = ihs_.find(ix_name);
        // 如果该索引目前被打开了，先关闭它的句柄并从 ihs_ 缓存中移除
        if (it_ih != ihs_.end()) {
            ix_manager_->close_index(it_ih->second.get());
            ihs_.erase(it_ih);
        }
        // 物理删除磁盘上的 .idx 文件
        ix_manager_->destroy_index(tab_name, index_meta.cols);
    }
    // 3. 删除记录文件。
    // 同样，先在缓存中查找是否有打开的句柄 (fhs_)
    auto it_fh = fhs_.find(tab_name);
    if (it_fh != fhs_.end()) {
        // 关闭并从句柄池中移除，避免后续对已删除文件的非法引用
        rm_manager_->close_file(it_fh->second.get());
        fhs_.erase(it_fh);
    }
    // 物理删除磁盘上的记录文件（通常无后缀名）
    rm_manager_->destroy_file(tab_name);
    // 4. 更新内存中的数据库元数据，将该表的信息彻底移除，并同步到 db.meta 文件
    db_.tabs_.erase(tab_name);
    flush_meta();
}

/**
 * @description: 创建索引
 * @param {string&} tab_name 表的名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::create_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    // 1. 基础校验：表必须存在
    if (!db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }
    TabMeta &tab = db_.get_table(tab_name);
    // 2. 校验索引是否已经存在。不支持在同一组列（且顺序一致）上创建重复索引。
    if (tab.is_index(col_names)) {
        throw IndexExistsError(tab_name, col_names);
    }
    // 3. 构建索引元数据体体 (IndexMeta)
    IndexMeta index_meta;
    index_meta.tab_name = tab_name;
    index_meta.col_tot_len = 0;
    index_meta.col_num = static_cast<int>(col_names.size());
    index_meta.cols.clear();
    for (auto &name : col_names) {
        // 定位列的详细信息（类型、长度等），索引需要知道如何存储和比较这些列
        auto it = tab.get_col(name);
        index_meta.cols.push_back(*it);
        index_meta.col_tot_len += it->len;
        // 标记该列“存在索引”，这样 desc table 时能展示 YES
        it->index = true;
    }
    // 4. 物理创建索引文件。ix_manager 负责初始化 B+ 树的根节点和 header。
    ix_manager_->create_index(tab_name, index_meta.cols);
    // 5. 将索引信息加入到表的元数据中，并打开它以便立即可用
    tab.indexes.push_back(index_meta);
    std::string ix_name = ix_manager_->get_index_name(tab_name, col_names);
    ihs_.emplace(ix_name, ix_manager_->open_index(tab_name, col_names));
    // 6. 元数据变更落盘体体
    flush_meta();
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    // 1. 校验表是否存在
    if (!db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }
    TabMeta &tab = db_.get_table(tab_name);
    // 2. 校验索引是否存在体体体体
    if (!tab.is_index(col_names)) {
        throw IndexNotFoundError(tab_name, col_names);
    }
    // 3. 释放资源。先找到内存中打开的索引句柄体体
    std::string ix_name = ix_manager_->get_index_name(tab_name, col_names);
    auto it_ih = ihs_.find(ix_name);
    if (it_ih != ihs_.end()) {
        // 关闭并销毁内存句柄
        ix_manager_->close_index(it_ih->second.get());
        ihs_.erase(it_ih);
    }
    // 物理删除磁盘上的 .idx 文件
    ix_manager_->destroy_index(tab_name, col_names);
    // 4. 更新元数据：将列上的索引标记设为 false，并从 TabMeta 的索引列表中移除该项体体体体
    for (auto &name : col_names) {
        auto it_col = tab.get_col(name);
        it_col->index = false;
    }
    auto it = tab.indexes.begin();
    while (it != tab.indexes.end()) {
        // 查找字段列表完全一致的索引条目体体
        if (it->col_num == (int)col_names.size()) {
            bool match = true;
            for (int i = 0; i < it->col_num; ++i) {
                if (it->cols[i].name != col_names[i]) { match = false; break; }
            }
            if (match) { it = tab.indexes.erase(it); break; } 
            else { ++it; }
        } else { ++it; }
    }
    // 5. 同步元数据体体
    flush_meta();
}

/**
 * @description: 删除索引 (重载版本，直接接收 ColMeta 列表)
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<ColMeta>& cols, Context* context) {
    // 逻辑与上述版本基本一致体体
    if (!db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }
    TabMeta &tab = db_.get_table(tab_name);
    std::string ix_name = ix_manager_->get_index_name(tab_name, cols);
    auto it_ih = ihs_.find(ix_name);
    if (it_ih != ihs_.end()) {
        ix_manager_->close_index(it_ih->second.get());
        ihs_.erase(it_ih);
    }
    ix_manager_->destroy_index(tab_name, cols);
    for (auto &col : cols) {
        auto it_col = tab.get_col(col.name);
        it_col->index = false;
    }
    auto it = tab.indexes.begin();
    while (it != tab.indexes.end()) {
        if (it->col_num == (int)cols.size()) {
            bool match = true;
            for (int i = 0; i < it->col_num; ++i) {
                if (it->cols[i].name != cols[i].name) { match = false; break; }
            }
            if (match) { it = tab.indexes.erase(it); break; }
            else { ++it; }
        } else { ++it; }
    }
    flush_meta();
}