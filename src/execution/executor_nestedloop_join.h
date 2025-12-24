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
        isend = false;
        left_->beginTuple();
        if (left_->is_end()) {
            isend = true;
            return;
        }
        right_->beginTuple();
        // 前进到第一个满足连接条件的组合
        auto satisfy = [&]() -> bool {
            if (left_->is_end() || right_->is_end()) return false;
            auto lrec = left_->Next();
            auto rrec = right_->Next();
            for (auto &cond : fed_conds_) {
                // 左右列在各自元组中
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
                    case OP_EQ:
                        if (cmp != 0) return false;
                        break;
                    case OP_NE:
                        if (cmp == 0) return false;
                        break;
                    case OP_LT:
                        if (!(cmp < 0)) return false;
                        break;
                    case OP_GT:
                        if (!(cmp > 0)) return false;
                        break;
                    case OP_LE:
                        if (!(cmp <= 0)) return false;
                        break;
                    case OP_GE:
                        if (!(cmp >= 0)) return false;
                        break;
                }
            }
            return true;
        };
        for (;;) {
            if (left_->is_end()) {
                isend = true;
                return;
            }
            while (!right_->is_end()) {
                if (satisfy()) return;
                right_->nextTuple();
            }
            left_->nextTuple();
            if (left_->is_end()) {
                isend = true;
                return;
            }
            right_->beginTuple();
        }
    }

    void nextTuple() override {
        if (is_end()) return;
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
                    case OP_EQ:
                        if (cmp != 0) return false;
                        break;
                    case OP_NE:
                        if (cmp == 0) return false;
                        break;
                    case OP_LT:
                        if (!(cmp < 0)) return false;
                        break;
                    case OP_GT:
                        if (!(cmp > 0)) return false;
                        break;
                    case OP_LE:
                        if (!(cmp <= 0)) return false;
                        break;
                    case OP_GE:
                        if (!(cmp >= 0)) return false;
                        break;
                }
            }
            return true;
        };
        // 从当前(right前进一位)寻找下一个匹配
        right_->nextTuple();
        for (;;) {
            if (left_->is_end()) {
                isend = true;
                return;
            }
            while (!right_->is_end()) {
                if (satisfy()) return;
                right_->nextTuple();
            }
            left_->nextTuple();
            if (left_->is_end()) {
                isend = true;
                return;
            }
            right_->beginTuple();
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) return nullptr;
        auto lrec = left_->Next();
        auto rrec = right_->Next();
        auto out = std::make_unique<RmRecord>(len_);
        // 拷贝左表
        memcpy(out->data, lrec->data, left_->tupleLen());
        // 拷贝右表
        memcpy(out->data + left_->tupleLen(), rrec->data, right_->tupleLen());
        return out;
    }

    Rid &rid() override { return _abstract_rid; }
};