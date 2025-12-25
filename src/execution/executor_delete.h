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

class DeleteExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                   // 表的元数据
    std::vector<Condition> conds_;  // delete的条件
    RmFileHandle *fh_;              // 表的数据文件句柄
    std::vector<Rid> rids_;         // 需要删除的记录的位置
    std::string tab_name_;          // 表名称
    SmManager *sm_manager_;

   public:
    DeleteExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Condition> conds,
                   std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }

    std::unique_ptr<RmRecord> Next() override {
        // 1. 迭代执行计划中提供的所有待删除记录的 RID 集合体体。
        // 这些 RID 通常由底层的扫描算子预先收集。
        for (auto &rid : rids_) {
            // 2. 获取旧记录内容体体。
            // 必须在物理删除前拉取数据，因为我们需要旧数据来构造索引的 Key。
            auto rec = fh_->get_record(rid, context_);

            // 3. 维护索引：逐个删除该表上的所有索引项体体。
            for (auto &index : tab_.indexes) {
                // 定位索引句柄体体
                auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                
                // 组装当前记录在该索引下的复合键体体
                std::unique_ptr<char[]> key(new char[index.col_tot_len]);
                int offset = 0;
                for (int i = 0; i < index.col_num; ++i) {
                    // 按照索引定义的列顺序，从记录 Buffer 中拷贝数据体体
                    memcpy(key.get() + offset, rec->data + index.cols[i].offset, index.cols[i].len);
                    offset += index.cols[i].len;
                }
                // 从 B+ 树中物理删除对应的 Entry (Key, RID) 体体
                ih->delete_entry(key.get(), context_->txn_);
            }

            // 4. 物理删除记录体体。
            // 在 RmFileHandle 中将该 rid 对应的位图标记设为无效体体。
            fh_->delete_record(rid, context_);
        }
        // 按照 DML 算子规约，返回空表示操作执行完毕体体
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};