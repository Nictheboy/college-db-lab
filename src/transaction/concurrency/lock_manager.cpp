/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "lock_manager.h"
#include <optional>

/**
 * 这里实现 Lab4 要求的两阶段封锁(2PL) + no-wait 死锁预防。
 *
 * 核心约束（背诵版）：
 * 1) 2PL：事务在 GROWING 阶段可以申请锁；一旦进入 SHRINKING 阶段，禁止再申请任何锁。
 * 2) no-wait：如果当前锁请求与“已授予(granted)”的锁不相容，则不等待，直接让当前事务回滚（抛 TransactionAbortException）。
 * 3) 多粒度锁：表锁(意向锁/表级S/X) 与 行锁(记录S/X) 共同工作。
 */

bool LockManager::compatible_with_granted(const LockRequestQueue &rq, txn_id_t self, LockMode requested) const {
    auto compat = [](LockMode a, LockMode b) -> bool {
        // 多粒度锁相容矩阵（简化版，满足本实验）：
        // IS 兼容 IS/IX/S/SIX；不兼容 X
        // IX 兼容 IS/IX；不兼容 S/SIX/X
        // S  兼容 IS/S；不兼容 IX/SIX/X
        // SIX兼容 IS；不兼容 IX/S/SIX/X
        // X  不兼容任何
        if (a == LockMode::EXLUCSIVE || b == LockMode::EXLUCSIVE) return false;
        if (a == LockMode::S_IX || b == LockMode::S_IX) {
            LockMode other = (a == LockMode::S_IX) ? b : a;
            return other == LockMode::INTENTION_SHARED;
        }
        if (a == LockMode::SHARED || b == LockMode::SHARED) {
            LockMode other = (a == LockMode::SHARED) ? b : a;
            return other == LockMode::SHARED || other == LockMode::INTENTION_SHARED;
        }
        if (a == LockMode::INTENTION_EXCLUSIVE || b == LockMode::INTENTION_EXCLUSIVE) {
            LockMode other = (a == LockMode::INTENTION_EXCLUSIVE) ? b : a;
            return other == LockMode::INTENTION_EXCLUSIVE || other == LockMode::INTENTION_SHARED;
        }
        return true; // IS vs IS
    };

    for (const auto &req : rq.request_queue_) {
        if (!req.granted_) continue;
        if (req.txn_id_ == self) continue;
        if (!compat(requested, req.lock_mode_)) return false;
    }
    return true;
}

bool LockManager::lock_internal(Transaction *txn, const LockDataId &lock_data_id, LockMode mode) {
    // 无事务上下文：不加锁（例如系统内部的 undo_ctx）
    if (txn == nullptr) return true;

    // 2PL：shrinking 阶段禁止再申请任何锁
    if (txn->get_state() == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }

    std::unique_lock<std::mutex> lk(latch_);
    auto &rq = lock_table_[lock_data_id];

    // ========= 升级/包含关系判定小工具 =========
    // 说明：同一事务可能在同一对象上先申请“弱锁”，后续又申请“强锁/组合锁”。
    // - 行级：S -> X（UPDATE/DELETE 常见：扫描阶段先读后写）
    // - 表级（多粒度）：IS/IX/S/SIX/X 之间存在升级与“组合”关系
    //   例如：先写表（IX）后读表（S），等价于 SIX（S + IX）。
    auto combine_mode = [&](LockMode cur, LockMode req) -> std::optional<LockMode> {
        // 任何场景：已持有 X，后续任何请求都满足
        if (cur == LockMode::EXLUCSIVE) return cur;
        // 任何场景：请求 IS，总是被“当前已持有锁”蕴含
        if (req == LockMode::INTENTION_SHARED) return cur;

        // 行级只会出现 S/X（实验中 record 不使用 IS/IX/SIX）
        if (lock_data_id.type_ == LockDataType::RECORD) {
            if (cur == req) return cur;
            if (cur == LockMode::SHARED && req == LockMode::EXLUCSIVE) return LockMode::EXLUCSIVE;
            // 例如：cur==X 上面已提前 return；cur==S req==S 已处理；其它都视为不支持
            return std::nullopt;
        }

        // ===== 表级：多粒度锁升级矩阵（覆盖本实验所需场景）=====
        // 目标：保证“单事务先写后读/先读后写”不会因为锁升级缺失而错误 abort。
        if (cur == req) return cur;

        // IS -> S / IX / SIX / X（X 可视为最强）
        if (cur == LockMode::INTENTION_SHARED) {
            if (req == LockMode::SHARED) return LockMode::SHARED;
            if (req == LockMode::INTENTION_EXCLUSIVE) return LockMode::INTENTION_EXCLUSIVE;  // 已有专门分支也没关系
            if (req == LockMode::S_IX) return LockMode::S_IX;
            if (req == LockMode::EXLUCSIVE) return LockMode::EXLUCSIVE;
        }

        // IX -> SIX / X
        if (cur == LockMode::INTENTION_EXCLUSIVE) {
            if (req == LockMode::SHARED) return LockMode::S_IX;      // 关键：IX + S => SIX
            if (req == LockMode::S_IX) return LockMode::S_IX;
            if (req == LockMode::EXLUCSIVE) return LockMode::EXLUCSIVE;
            // req==IX：cur==req 已处理
        }

        // S -> SIX / X
        if (cur == LockMode::SHARED) {
            if (req == LockMode::INTENTION_EXCLUSIVE) return LockMode::S_IX; // 关键：S + IX => SIX
            if (req == LockMode::S_IX) return LockMode::S_IX;
            if (req == LockMode::EXLUCSIVE) return LockMode::EXLUCSIVE;
        }

        // SIX -> X
        if (cur == LockMode::S_IX) {
            if (req == LockMode::SHARED) return LockMode::S_IX;
            if (req == LockMode::INTENTION_EXCLUSIVE) return LockMode::S_IX;
            if (req == LockMode::EXLUCSIVE) return LockMode::EXLUCSIVE;
        }

        return std::nullopt;
    };

    // 1) 重入/升级：查找该事务是否已经在队列中
    for (auto it = rq.request_queue_.begin(); it != rq.request_queue_.end(); ++it) {
        if (it->txn_id_ != txn->get_transaction_id()) continue;

        if (!it->granted_) {
            // no-wait 下我们不会有“等待中的本事务请求”，理论上不会出现；出现就直接 abort，避免状态机走飞。
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
        }

        auto cur = it->lock_mode_;
        auto new_mode_opt = combine_mode(cur, mode);
        if (!new_mode_opt.has_value()) {
            // 不支持的升级（例如 record 上出现 IX/SIX），或升级关系未定义
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::UPGRADE_CONFLICT);
        }

        LockMode new_mode = new_mode_opt.value();
        if (new_mode == cur) {
            // 当前锁已经“蕴含/覆盖”请求锁
            return true;
        }

        // 升级需要与“其它事务已授予锁”相容，否则 no-wait 直接 abort
        if (!compatible_with_granted(rq, txn->get_transaction_id(), new_mode)) {
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::UPGRADE_CONFLICT);
        }

        it->lock_mode_ = new_mode;
        return true;
    }

    // 2) 新请求：检查与其他已授予锁相容性
    if (!compatible_with_granted(rq, txn->get_transaction_id(), mode)) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }

    // 3) 直接授予（no-wait，不进入等待队列）
    rq.request_queue_.emplace_back(txn->get_transaction_id(), mode);
    rq.request_queue_.back().granted_ = true;

    // 4) 记录到事务 lock_set_，便于 commit/abort 统一释放
    txn->get_lock_set()->insert(lock_data_id);

    return true;
}

/**
 * @description: 申请行级共享锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID 记录所在的表的fd
 * @param {int} tab_fd
 */
bool LockManager::lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    // 行锁：S
    LockDataId lid(tab_fd, rid, LockDataType::RECORD);
    return lock_internal(txn, lid, LockMode::SHARED);
}

/**
 * @description: 申请行级排他锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的fd
 */
bool LockManager::lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    // 行锁：X（可能触发从 S 升级到 X）
    LockDataId lid(tab_fd, rid, LockDataType::RECORD);
    return lock_internal(txn, lid, LockMode::EXLUCSIVE);
}

/**
 * @description: 申请表级读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_shared_on_table(Transaction* txn, int tab_fd) {
    LockDataId lid(tab_fd, LockDataType::TABLE);
    return lock_internal(txn, lid, LockMode::SHARED);
}

/**
 * @description: 申请表级写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_exclusive_on_table(Transaction* txn, int tab_fd) {
    LockDataId lid(tab_fd, LockDataType::TABLE);
    return lock_internal(txn, lid, LockMode::EXLUCSIVE);
}

/**
 * @description: 申请表级意向读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IS_on_table(Transaction* txn, int tab_fd) {
    LockDataId lid(tab_fd, LockDataType::TABLE);
    return lock_internal(txn, lid, LockMode::INTENTION_SHARED);
}

/**
 * @description: 申请表级意向写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IX_on_table(Transaction* txn, int tab_fd) {
    LockDataId lid(tab_fd, LockDataType::TABLE);
    return lock_internal(txn, lid, LockMode::INTENTION_EXCLUSIVE);
}

/**
 * @description: 释放锁
 * @return {bool} 返回解锁是否成功
 * @param {Transaction*} txn 要释放锁的事务对象指针
 * @param {LockDataId} lock_data_id 要释放的锁ID
 */
bool LockManager::unlock(Transaction* txn, LockDataId lock_data_id) {
    if (txn == nullptr) return true;

    std::unique_lock<std::mutex> lk(latch_);
    auto it = lock_table_.find(lock_data_id);
    if (it == lock_table_.end()) {
        // 解一个不存在的锁：视为幂等成功（更利于上层“释放全部锁”的简单实现）
        return true;
    }

    auto &rq = it->second;
    // 从队列中移除该事务的请求（无论 granted 与否）
    for (auto qit = rq.request_queue_.begin(); qit != rq.request_queue_.end(); ) {
        if (qit->txn_id_ == txn->get_transaction_id()) {
            qit = rq.request_queue_.erase(qit);
        } else {
            ++qit;
        }
    }

    // no-wait 策略下没有阻塞队列需要唤醒，但我们仍然维护锁表大小，避免泄漏
    if (rq.request_queue_.empty()) {
        lock_table_.erase(it);
    }

    // 从事务 lock_set_ 中删除该锁
    txn->get_lock_set()->erase(lock_data_id);

    // 释放锁意味着事务进入 SHRINKING（严格 2PL：一旦释放任何锁就进入 shrinking）
    // 注意：事务管理器在 commit/abort 前会主动设置 SHRINKING，这里再设置是幂等的。
    if (txn->get_state() == TransactionState::GROWING) {
        txn->set_state(TransactionState::SHRINKING);
    }

    return true;
}