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

class SeqScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;              // 表的名称
    std::vector<Condition> conds_;      // scan的条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // scan后生成的记录的字段
    size_t len_;                        // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同

    Rid rid_;
    std::unique_ptr<RecScan> scan_;     // table_iterator

    SmManager *sm_manager_;

   public:
    SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;

        context_ = context;

        fed_conds_ = conds_;
    }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    bool is_end() const override { return scan_ == nullptr || scan_->is_end(); }

    ColMeta get_col_offset(const TabCol &target) override {
        auto it = get_col(cols_, target);
        return *it;
    }

    /**
     * @brief 构建表迭代器scan_,并开始迭代扫描,直到扫描到第一个满足谓词条件的元组停止,并赋值给rid_
     *
     */
    void beginTuple() override {
        // 1. 初始化扫描器体体。RmScan 是底层记录层的迭代器，用于遍历表中的所有记录。
        scan_ = std::make_unique<RmScan>(fh_);

        // 2. 定义谓词判断闭包体体 (Lambda)。用于检查给定的记录是否满足 WHERE 条件。
        auto satisfy = [&](const Rid &rid) -> bool {
            // 从底层拉取完整的记录数据体体
            auto rec = fh_->get_record(rid, context_);
            // 遍历执行计划中的所有条件（AND 关系）
            for (auto &cond : fed_conds_) {
                // 获取左侧列的偏移量和元数据体体
                auto lhs_it = get_col(cols_, cond.lhs_col);
                char *lhs_ptr = rec->data + lhs_it->offset;
                char *rhs_ptr = nullptr;
                ColType type = lhs_it->type;
                int len = lhs_it->len;

                // 确定右侧操作数的值指针体体
                if (cond.is_rhs_val) {
                    // 右侧是常量：在分析阶段已经转换成了原始二进制体体
                    rhs_ptr = cond.rhs_val.raw->data;
                } else {
                    // 右侧是另一列：从当前记录中提取偏移体体
                    auto rhs_it = get_col(cols_, cond.rhs_col);
                    rhs_ptr = rec->data + rhs_it->offset;
                }

                // 执行类型相关的比较逻辑体体
                int cmp = 0;
                if (type == TYPE_INT) {
                    int a = *(int *)lhs_ptr, b = *(int *)rhs_ptr;
                    cmp = (a < b) ? -1 : ((a > b) ? 1 : 0);
                } else if (type == TYPE_FLOAT) {
                    float a = *(float *)lhs_ptr, b = *(float *)rhs_ptr;
                    cmp = (a < b) ? -1 : ((a > b) ? 1 : 0);
                } else {
                    // 字符串：直接使用字节比较体体
                    cmp = memcmp(lhs_ptr, rhs_ptr, len);
                }

                // 根据操作符判断比较结果是否成立体体
                switch (cond.op) {
                    case OP_EQ: if (cmp != 0) return false; break;
                    case OP_NE: if (cmp == 0) return false; break;
                    case OP_LT: if (!(cmp < 0)) return false; break;
                    case OP_GT: if (!(cmp > 0)) return false; break;
                    case OP_LE: if (!(cmp <= 0)) return false; break;
                    case OP_GE: if (!(cmp >= 0)) return false; break;
                }
            }
            return true; // 所有条件均通过
        };

        // 3. 寻找起始位置。从头开始遍历记录，直到找到第一个满足条件的记录体体
        for (; !scan_->is_end(); scan_->next()) {
            Rid r = scan_->rid();
            // 注意：必须检查该 slot 是否真的存有有效记录（位图标记）体体
            if (fh_->is_record(r) && satisfy(r)) {
                rid_ = r; // 记录当前找到的符合条件的 rid
                break;
            }
        }
    }

    /**
     * @brief 从当前scan_指向的记录开始迭代扫描,直到扫描到第一个满足谓词条件的元组停止,并赋值给rid_
     *
     */
    void nextTuple() override {
        // 如果已经到结尾了，直接返回体体
        if (is_end()) return;

        // 复用同样的 satisfy 逻辑体体
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

        // 从当前位置的“下一条”开始寻找体体
        for (scan_->next(); !scan_->is_end(); scan_->next()) {
            Rid r = scan_->rid();
            if (fh_->is_record(r) && satisfy(r)) {
                rid_ = r; // 更新当前找到的 rid
                return;
            }
        }
    }

    /**
     * @brief 返回下一个满足扫描条件的记录
     *
     * @return std::unique_ptr<RmRecord>
     */
    std::unique_ptr<RmRecord> Next() override {
        // 按照火山模型，如果迭代结束则返回空指针体体
        if (is_end()) return nullptr;
        // 否则返回当前 rid 指向的记录体体
        return fh_->get_record(rid_, context_);
    }

    Rid &rid() override { return rid_; }
};