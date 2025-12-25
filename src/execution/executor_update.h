/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;

   public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }
    std::unique_ptr<RmRecord> Next() override {
        // 1. 预处理：确定哪些索引受到本次 UPDATE 影响体体。
        // 如果被更新的列包含在索引中，该索引就需要维护（删除旧键、插入新键）。
        std::vector<IndexMeta> affected_indexes;
        for (auto &index : tab_.indexes) {
            bool affected = false;
            for (int i = 0; i < index.col_num && !affected; ++i) {
                for (auto &sc : set_clauses_) {
                    if (index.cols[i].name == sc.lhs.col_name) {
                        affected = true;
                        break;
                    }
                }
            }
            if (affected) affected_indexes.push_back(index);
        }

        // 2. 逐条处理待更新记录体体
        for (auto &rid : rids_) {
            // 获取更新前的原始记录体体
            auto rec = fh_->get_record(rid, context_);

            // 3. 维护受影响索引：删除旧键体体。
            for (auto &index : affected_indexes) {
                auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                std::unique_ptr<char[]> old_key(new char[index.col_tot_len]);
                int offset = 0;
                for (int i = 0; i < index.col_num; ++i) {
                    memcpy(old_key.get() + offset, rec->data + index.cols[i].offset, index.cols[i].len);
                    offset += index.cols[i].len;
                }
                ih->delete_entry(old_key.get(), context_->txn_);
            }

            // 4. 应用更新到内存 Buffer 体体。
            for (auto &sc : set_clauses_) {
                auto it_col = tab_.get_col(sc.lhs.col_name);
                // 关键点：将 Value 对象的具体值（int/float/string）直接序列化到记录 Buffer 的对应偏移位置体体。
                // 我们不直接调用 Value::init_raw，因为那会涉及额外的内存分配和潜在的断言失败体体。
                char *dest = rec->data + it_col->offset;
                if (it_col->type == TYPE_INT) {
                    memcpy(dest, &sc.rhs.int_val, sizeof(int));
                } else if (it_col->type == TYPE_FLOAT) {
                    memcpy(dest, &sc.rhs.float_val, sizeof(float));
                } else {
                    // 字符串处理：填充 0 并按列长度截断/拷贝体体
                    memset(dest, 0, it_col->len);
                    size_t n = std::min((int)sc.rhs.str_val.size(), it_col->len);
                    memcpy(dest, sc.rhs.str_val.data(), n);
                }
            }

            // 5. 将修改后的记录写回磁盘体体
            fh_->update_record(rid, rec->data, context_);

            // 6. 维护受影响索引：根据更新后的数据插入新键体体。
            for (auto &index : affected_indexes) {
                auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                std::unique_ptr<char[]> new_key(new char[index.col_tot_len]);
                int offset = 0;
                for (int i = 0; i < index.col_num; ++i) {
                    // 注意：此时 rec->data 已经是更新后的数据了体体
                    memcpy(new_key.get() + offset, rec->data + index.cols[i].offset, index.cols[i].len);
                    offset += index.cols[i].len;
                }
                ih->insert_entry(new_key.get(), rid, context_->txn_);
            }
        }
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};