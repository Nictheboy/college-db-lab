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
        // 构建等值key（当前规划仅支持完全匹配的等值查询）
        auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_col_names_)).get();
        std::unique_ptr<char[]> key(new char[index_meta_.col_tot_len]);
        int offset = 0;
        for (size_t i = 0; i < index_meta_.cols.size(); ++i) {
            const auto &col = index_meta_.cols[i];
            bool found = false;
            for (auto &cond : fed_conds_) {
                if (cond.is_rhs_val && cond.op == OP_EQ && cond.lhs_col.tab_name == tab_name_ &&
                    cond.lhs_col.col_name == col.name) {
                    memcpy(key.get() + offset, cond.rhs_val.raw->data, col.len);
                    found = true;
                    break;
                }
            }
            if (!found) {
                // 若未找到完整等值条件，则退化为全索引扫描
                scan_ = std::make_unique<IxScan>(ih, ih->leaf_begin(), ih->leaf_end(), sm_manager_->get_bpm());
                break;
            }
            offset += col.len;
        }
        if (scan_ == nullptr) {
            auto lower = ih->lower_bound(key.get());
            auto upper = ih->upper_bound(key.get());
            scan_ = std::make_unique<IxScan>(ih, lower, upper, sm_manager_->get_bpm());
        }
        // 前进到第一个满足其他谓词（若有）的元组
        auto satisfy = [&](const Rid &rid) -> bool {
            auto rec = fh_->get_record(rid, context_);
            for (auto &cond : fed_conds_) {
                auto lhs_it = get_col(cols_, cond.lhs_col);
                char *lhs_ptr = rec->data + lhs_it->offset;
                char *rhs_ptr = nullptr;
                ColType type = lhs_it->type;
                int len = lhs_it->len;
                if (cond.is_rhs_val) {
                    rhs_ptr = cond.rhs_val.raw->data;
                } else {
                    auto rhs_it = get_col(cols_, cond.rhs_col);
                    rhs_ptr = rec->data + rhs_it->offset;
                }
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
        auto satisfy = [&](const Rid &rid) -> bool {
            auto rec = fh_->get_record(rid, context_);
            for (auto &cond : fed_conds_) {
                auto lhs_it = get_col(cols_, cond.lhs_col);
                char *lhs_ptr = rec->data + lhs_it->offset;
                char *rhs_ptr = nullptr;
                ColType type = lhs_it->type;
                int len = lhs_it->len;
                if (cond.is_rhs_val) {
                    rhs_ptr = cond.rhs_val.raw->data;
                } else {
                    auto rhs_it = get_col(cols_, cond.rhs_col);
                    rhs_ptr = rec->data + rhs_it->offset;
                }
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
        for (scan_->next(); !scan_->is_end(); scan_->next()) {
            Rid r = scan_->rid();
            if (fh_->is_record(r) && satisfy(r)) {
                rid_ = r;
                return;
            }
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) return nullptr;
        return fh_->get_record(rid_, context_);
    }

    Rid &rid() override { return rid_; }
};