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

class IndexScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;                      // 表名称
    TabMeta tab_;                               // 表的元数据
    std::vector<Condition> conds_;              // 扫描条件
    RmFileHandle *fh_;                          // 表的数据文件句柄
    std::vector<ColMeta> cols_;                 // 需要读取的字段
    size_t len_;                                // 选取出来的一条记录的长度
    std::vector<Condition> fed_conds_;          // 扫描条件，和conds_字段相同

    std::vector<std::string> index_col_names_;  // index scan涉及到的索引包含的字段
    IndexMeta index_meta_;                      // index scan涉及到的索引元数据

    Rid rid_;
    std::unique_ptr<RecScan> scan_;

    SmManager *sm_manager_;

   public:
    IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, std::vector<std::string> index_col_names,
                    Context *context) {
        sm_manager_ = sm_manager;
        context_ = context;
        tab_name_ = std::move(tab_name);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(conds);
        // index_no_ = index_no;
        index_col_names_ = index_col_names; 
        index_meta_ = *(tab_.get_index_meta(index_col_names_));
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;
        std::map<CompOp, CompOp> swap_op = {
            {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
        };

        for (auto &cond : conds_) {
            if (cond.lhs_col.tab_name != tab_name_) {
                // lhs is on other table, now rhs must be on this table
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                // swap lhs and rhs
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }
        fed_conds_ = conds_;
    }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    bool is_end() const override { return scan_ == nullptr || scan_->is_end(); }

    ColMeta get_col_offset(const TabCol &target) override {
        auto it = get_col(cols_, target);
        return *it;
    }

    void beginTuple() override {
        // ========== 并发控制：防止幻读（保守版：表级 S 锁）==========
        // 即便是索引扫描，本质上仍是“扫描表的一段范围”，同样可能出现幻读（别的事务插入新记录）。
        // 这里采取最简单的办法：扫描前对表加 S 锁。
        if (context_ != nullptr && context_->txn_ != nullptr && context_->lock_mgr_ != nullptr) {
            context_->lock_mgr_->lock_shared_on_table(context_->txn_, fh_->GetFd());
        }

        // 1. 获取索引句柄体体。通过 sm_manager 查找预先打开的 B+ 树句柄体体。
        auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_col_names_)).get();
        
        // 2. 尝试构建等值查询的 Key 体体。
        // 对于复合索引（多个列），我们需要把各个列的等值常量拼接成一个完整的字节串体体。
        std::unique_ptr<char[]> key(new char[index_meta_.col_tot_len]);
        int offset = 0;
        bool full_match = true;
        for (size_t i = 0; i < index_meta_.cols.size(); ++i) {
            const auto &col = index_meta_.cols[i];
            bool found = false;
            // 在 WHERE 条件中寻找匹配该列的等值谓词体体
            for (auto &cond : fed_conds_) {
                if (cond.is_rhs_val && cond.op == OP_EQ && cond.lhs_col.tab_name == tab_name_ &&
                    cond.lhs_col.col_name == col.name) {
                    memcpy(key.get() + offset, cond.rhs_val.raw->data, col.len);
                    found = true;
                    break;
                }
            }
            if (!found) {
                // 如果索引中的某一列没有等值条件，我们无法使用 B+ 树范围查询体体。
                // 此时退化为全索引扫描（即遍历整个 B+ 树的叶子节点链表）体体。
                scan_ = std::make_unique<IxScan>(ih, ih->leaf_begin(), ih->leaf_end(), sm_manager_->get_bpm());
                full_match = false;
                break;
            }
            offset += col.len;
        }

        // 3. 执行 B+ 树查找体体。
        // 如果所有索引列都有等值条件，使用 lower_bound 和 upper_bound 确定扫描范围体体。
        if (full_match) {
            auto lower = ih->lower_bound(key.get());
            auto upper = ih->upper_bound(key.get());
            scan_ = std::make_unique<IxScan>(ih, lower, upper, sm_manager_->get_bpm());
        }

        // 4. 定义谓词过滤逻辑体体。
        // 注意：即便索引返回了 rid，我们仍需通过 satisfy 检查那些没被索引覆盖的条件体体。
        auto satisfy = [&](const Rid &rid) -> bool {
            auto rec = fh_->get_record(rid, context_);
            for (auto &cond : fed_conds_) {
                auto lhs_it = get_col(cols_, cond.lhs_col);
                char *lhs_ptr = rec->data + lhs_it->offset;
                char *rhs_ptr = cond.is_rhs_val ? cond.rhs_val.raw->data : (rec->data + get_col(cols_, cond.rhs_col)->offset);
                int cmp = 0;
                if (lhs_it->type == TYPE_INT) {
                    int a = *(int *)lhs_ptr, b = *(int *)rhs_ptr;
                    cmp = (a < b) ? -1 : ((a > b) ? 1 : 0);
                } else if (lhs_it->type == TYPE_FLOAT) {
                    float a = *(float *)lhs_ptr, b = *(float *)rhs_ptr;
                    cmp = (a < b) ? -1 : ((a > b) ? 1 : 0);
                } else {
                    cmp = memcmp(lhs_ptr, rhs_ptr, lhs_it->len);
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

        // 5. 移动到扫描范围内的第一个符合所有条件的记录体体
        for (; !scan_->is_end(); scan_->next()) {
            Rid r = scan_->rid();
            if (fh_->is_record(r) && satisfy(r)) {
                rid_ = r;
                break;
            }
        }
    }

    void nextTuple() override {
        if (is_end()) return;

        // 复用过滤逻辑体体
        auto satisfy = [&](const Rid &rid) -> bool {
            auto rec = fh_->get_record(rid, context_);
            for (auto &cond : fed_conds_) {
                auto lhs_it = get_col(cols_, cond.lhs_col);
                char *lhs_ptr = rec->data + lhs_it->offset;
                char *rhs_ptr = cond.is_rhs_val ? cond.rhs_val.raw->data : (rec->data + get_col(cols_, cond.rhs_col)->offset);
                int cmp = 0;
                if (lhs_it->type == TYPE_INT) {
                    int a = *(int *)lhs_ptr, b = *(int *)rhs_ptr;
                    cmp = (a < b) ? -1 : ((a > b) ? 1 : 0);
                } else if (lhs_it->type == TYPE_FLOAT) {
                    float a = *(float *)lhs_ptr, b = *(float *)rhs_ptr;
                    cmp = (a < b) ? -1 : ((a > b) ? 1 : 0);
                } else {
                    cmp = memcmp(lhs_ptr, rhs_ptr, lhs_it->len);
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

        // 在当前索引扫描范围内继续步进体体
        for (scan_->next(); !scan_->is_end(); scan_->next()) {
            Rid r = scan_->rid();
            if (fh_->is_record(r) && satisfy(r)) {
                rid_ = r;
                return;
            }
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        // 如果 B+ 树扫描器结束，则返回空体体
        if (is_end()) return nullptr;
        // 否则返回当前 rid 指向的真实数据记录体体
        return fh_->get_record(rid_, context_);
    }

    Rid &rid() override { return rid_; }
};