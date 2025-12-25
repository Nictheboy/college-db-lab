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

class ProjectionExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;        // 投影节点的儿子节点
    std::vector<ColMeta> cols_;                     // 需要投影的字段
    size_t len_;                                    // 字段总长度
    std::vector<size_t> sel_idxs_;                  

   public:
    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol> &sel_cols) {
        prev_ = std::move(prev);

        size_t curr_offset = 0;
        auto &prev_cols = prev_->cols();
        for (auto &sel_col : sel_cols) {
            auto pos = get_col(prev_cols, sel_col);
            sel_idxs_.push_back(pos - prev_cols.begin());
            auto col = *pos;
            col.offset = curr_offset;
            curr_offset += col.len;
            cols_.push_back(col);
        }
        len_ = curr_offset;
    }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    bool is_end() const override { return prev_->is_end(); }

    ColMeta get_col_offset(const TabCol &target) override {
        auto it = get_col(cols_, target);
        return *it;
    }

    void beginTuple() override { 
        // 投影算子是中间节点，直接调用子算子的起始接口体体
        prev_->beginTuple(); 
    }

    void nextTuple() override { 
        // 步进逻辑也直接透传给子算子体体
        prev_->nextTuple(); 
    }

    std::unique_ptr<RmRecord> Next() override {
        // 1. 检查子算子是否还有数据体体
        if (is_end()) return nullptr;
        // 2. 从子算子获取当前的原始元组体体
        auto in = prev_->Next();
        
        // 3. 创建一个新的、长度为投影后总长度的空元组体体
        auto out = std::make_unique<RmRecord>(len_);
        
        // 4. 执行字段拷贝逻辑体体。
        // sel_idxs_ 存储了“投影后的第 i 列对应原始元组的第几个字段”。
        const auto &prev_cols = prev_->cols();
        for (size_t i = 0; i < sel_idxs_.size(); ++i) {
            // 获取源列的元数据（提供源偏移量）体体
            const auto &src_col = prev_cols[sel_idxs_[i]];
            // 获取目标列的元数据（提供目标偏移量）体体
            const auto &dst_col = cols_[i];
            
            // 使用 memcpy 按照字段长度，将数据从原始 Buffer 拷贝到新记录的对应位置体体。
            // 这一步实现了 SQL 中的列筛选（裁剪不需要的列）和列重排（改变输出顺序）。
            memcpy(out->data + dst_col.offset, in->data + src_col.offset, dst_col.len);
        }
        return out;
    }

    Rid &rid() override { return _abstract_rid; }
};