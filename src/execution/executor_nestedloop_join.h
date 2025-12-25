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

class NestedLoopJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;    // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;   // 右儿子节点（需要join的表）
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段

    std::vector<Condition> fed_conds_;          // join条件
    bool isend;

   public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right, 
                            std::vector<Condition> conds) {
        left_ = std::move(left);
        right_ = std::move(right);
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();
        }

        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        isend = false;
        fed_conds_ = std::move(conds);

    }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    bool is_end() const override { return isend; }

    ColMeta get_col_offset(const TabCol &target) override {
        auto it = get_col(cols_, target);
        return *it;
    }

    void beginTuple() override {
        // 1. 初始化连接状态。isend 为 true 时表示整个连接扫描结束。
        isend = false;
        left_->beginTuple();
        if (left_->is_end()) {
            isend = true; // 左表为空，连接结果必然为空。
            return;
        }
        // 2. 初始化右子算子。嵌套循环连接的逻辑是：固定左表的一行，遍历右表的所有行。
        right_->beginTuple();

        // 3. 定义连接谓词判断逻辑。
        // 与单表不同，这里的条件通常涉及两个表的列（如 student.id = grade.student_id）。
        auto satisfy = [&]() -> bool {
            if (left_->is_end() || right_->is_end()) return false;
            auto lrec = left_->Next();
            auto rrec = right_->Next();
            for (auto &cond : fed_conds_) {
                // 关键点：从左、右子算子的元数据中分别定位需要比较的列。
                auto l_it = left_->get_col(left_->cols(), cond.lhs_col);
                auto r_it = right_->get_col(right_->cols(), cond.rhs_col);
                char *lhs_ptr = lrec->data + l_it->offset;
                char *rhs_ptr = rrec->data + r_it->offset;
                ColType type = l_it->type;
                int len = l_it->len;
                int cmp = 0;
                if (type == TYPE_INT) {
                    int a = *(int *)lhs_ptr, b = *(int *)rhs_ptr;
                    cmp = (a < b) ? -1 : ((a > b) ? 1 : 0);
                } else if (type == TYPE_FLOAT) {
                    float a = *(float *)lhs_ptr, b = *(float *)rhs_ptr;
                    cmp = (a < b) ? -1 : ((a > b) ? 1 : 0);
                } else {
                    cmp = memcmp(lhs_ptr, rhs_ptr, len);
                }
                // 如果任一连接条件不成立，则该左右元组组合不符合结果。
                switch (cond.op) {
                    case OP_EQ: if (cmp != 0) return false; break;
                    case OP_NE: if (cmp == 0) return false; break;
                    case OP_LT: if (!(cmp < 0)) return false; break;
                    case OP_GT: if (!(cmp > 0)) return false; break;
                    case OP_LE: if (!(cmp <= 0)) return false; break;
                    case OP_GE: if (!(cmp >= 0)) return false; break;
                }
            }
            return true;
        };

        // 4. 执行双层循环寻找第一对匹配。
        for (;;) {
            if (left_->is_end()) { isend = true; return; }
            while (!right_->is_end()) {
                // 如果当前组合满足连接条件，则停止，当前状态即为第一条结果。
                if (satisfy()) return;
                right_->nextTuple(); // 步进内层（右表）。
            }
            // 右表遍历完了，左表进位，并将右表重置回开头。
            left_->nextTuple();
            if (left_->is_end()) { isend = true; return; }
            right_->beginTuple();
        }
    }

    void nextTuple() override {
        if (is_end()) return;

        // 定义满足条件的闭包。
        auto satisfy = [&]() -> bool {
            if (left_->is_end() || right_->is_end()) return false;
            auto lrec = left_->Next();
            auto rrec = right_->Next();
            for (auto &cond : fed_conds_) {
                auto l_it = left_->get_col(left_->cols(), cond.lhs_col);
                auto r_it = right_->get_col(right_->cols(), cond.rhs_col);
                char *lhs_ptr = lrec->data + l_it->offset;
                char *rhs_ptr = rrec->data + r_it->offset;
                ColType type = l_it->type;
                int len = l_it->len;
                int cmp = 0;
                if (type == TYPE_INT) {
                    int a = *(int *)lhs_ptr, b = *(int *)rhs_ptr;
                    cmp = (a < b) ? -1 : ((a > b) ? 1 : 0);
                } else if (type == TYPE_FLOAT) {
                    float a = *(float *)lhs_ptr, b = *(float *)rhs_ptr;
                    cmp = (a < b) ? -1 : ((a > b) ? 1 : 0);
                } else {
                    cmp = memcmp(lhs_ptr, rhs_ptr, len);
                }
                switch (cond.op) {
                    case OP_EQ: if (cmp != 0) return false; break;
                    case OP_NE: if (cmp == 0) return false; break;
                    case OP_LT: if (!(cmp < 0)) return false; break;
                    case OP_GT: if (!(cmp > 0)) return false; break;
                    case OP_LE: if (!(cmp <= 0)) return false; break;
                    case OP_GE: if (!(cmp >= 0)) return false; break;
                }
            }
            return true;
        };

        // 从“右表的下一条”开始寻找下一对匹配组合。
        right_->nextTuple();
        for (;;) {
            if (left_->is_end()) { isend = true; return; }
            while (!right_->is_end()) {
                if (satisfy()) return;
                right_->nextTuple();
            }
            // 内层循环结束，外层进位并重置内层。
            left_->nextTuple();
            if (left_->is_end()) { isend = true; return; }
            right_->beginTuple();
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        // 1. 结束检查。
        if (is_end()) return nullptr;
        // 2. 获取当前匹配的左右元组。
        auto lrec = left_->Next();
        auto rrec = right_->Next();
        
        // 3. 创建拼接后的新元组。长度为左右两表元组长度之和。
        auto out = std::make_unique<RmRecord>(len_);
        
        // 4. 数据拼接。
        // 将左表记录拷贝到新 Buffer 的起始位置。
        memcpy(out->data, lrec->data, left_->tupleLen());
        // 将右表记录拷贝到左表数据之后，实现拼接。
        memcpy(out->data + left_->tupleLen(), rrec->data, right_->tupleLen());
        
        return out;
    }

    Rid &rid() override { return _abstract_rid; }
};