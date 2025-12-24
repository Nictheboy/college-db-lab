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
        // 针对每条记录执行更新，并维护相关索引
        // 预处理：确定受影响的索引（包含任一被更新列）
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
        for (auto &rid : rids_) {
            auto rec = fh_->get_record(rid, context_);
            // 删除旧索引项
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
            // 应用更新到record buffer
            for (auto &sc : set_clauses_) {
                auto it_col = tab_.get_col(sc.lhs.col_name);
                // 将rhs按列类型序列化到记录
                char *dest = rec->data + it_col->offset;
                if (it_col->type == TYPE_INT) {
                    memcpy(dest, &sc.rhs.int_val, sizeof(int));
                } else if (it_col->type == TYPE_FLOAT) {
                    memcpy(dest, &sc.rhs.float_val, sizeof(float));
                } else {
                    // string，按列长度填充
                    memset(dest, 0, it_col->len);
                    size_t n = std::min((int)sc.rhs.str_val.size(), it_col->len);
                    memcpy(dest, sc.rhs.str_val.data(), n);
                }
            }
            // 写回记录
            fh_->update_record(rid, rec->data, context_);
            // 插入新索引项（仅对受影响的索引）
            for (auto &index : affected_indexes) {
                auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                std::unique_ptr<char[]> new_key(new char[index.col_tot_len]);
                int offset = 0;
                for (int i = 0; i < index.col_num; ++i) {
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