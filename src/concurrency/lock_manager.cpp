//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"

#include <utility>
#include <vector>
#include <algorithm>

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  auto txn_id = txn->GetTransactionId();
  auto level = txn->GetIsolationLevel();

  if (level == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }
  
  LockPrepare(txn, rid);

  lock_table_[rid].request_queue_.emplace_back(LockRequest{txn_id, LockMode::SHARED});
  
  if (lock_table_[rid].is_writting_) {
    lock_table_[rid].cv_.wait(lock, [&txn, this, &rid] () {
      return txn->GetState() == TransactionState::ABORTED || !this->lock_table_[rid].is_writting_;
   });
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }

  lock_table_[rid].reading_count_++;
  for (auto &lock_request : lock_table_[rid].request_queue_) {
    if (lock_request.txn_id_ == txn_id && lock_request.lock_mode_ == LockMode::SHARED) {
      lock_request.granted_ = true;
      break;
    }
  }
  txn->GetSharedLockSet()->emplace(rid);
  
  return true;
}

bool LockManager::LockPrepare(Transaction *txn, const RID &rid) {
  if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED && 
      txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  if (lock_table_.count(rid) == 0) {
    lock_table_.emplace(rid, LockRequestQueue());
  }

  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  auto txn_id = txn->GetTransactionId();
  
  LockPrepare(txn, rid);

  lock_table_[rid].request_queue_.emplace_back(LockRequest{txn_id, LockMode::EXCLUSIVE});
  
  if (lock_table_[rid].is_writting_ || 
      lock_table_[rid].reading_count_ > 0) {
    lock_table_[rid].cv_.wait(lock, [&txn, this, &rid] () {
      return txn->GetState() == TransactionState::ABORTED || (!this->lock_table_[rid].is_writting_ && this->lock_table_[rid].reading_count_ == 0);
   });
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }

  lock_table_[rid].is_writting_ = true;
  for (auto &lock_request : lock_table_[rid].request_queue_) {
    if (lock_request.txn_id_ == txn_id && lock_request.lock_mode_ == LockMode::EXCLUSIVE) {
      lock_request.granted_ = true;
      break;
    }
  }
  txn->GetExclusiveLockSet()->emplace(rid);
  
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  auto txn_id = txn->GetTransactionId();
  
  if (lock_table_[rid].is_writting_) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }

  LockPrepare(txn, rid);

  txn->GetSharedLockSet()->erase(rid);
  lock_table_[rid].reading_count_--;
  auto lock_request_list = lock_table_[rid].request_queue_;
  auto itr = lock_request_list.begin();
  while (itr != lock_request_list.end()) {
    if (itr->txn_id_ == txn_id) {
      itr = lock_request_list.erase(itr);
    }
    else {
      itr++;
    }
  }

  lock_table_[rid].request_queue_.emplace_back(LockRequest{txn_id, LockMode::EXCLUSIVE});

  if (lock_table_[rid].reading_count_ > 0) {
    lock_table_[rid].cv_.wait(lock, [&txn, this, &rid] () {
      return txn->GetState() == TransactionState::ABORTED || this->lock_table_[rid].reading_count_ == 0;
   });
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }

  lock_table_[rid].is_writting_ = true;
  for (auto &lock_request : lock_table_[rid].request_queue_) {
    if (lock_request.txn_id_ == txn_id && lock_request.lock_mode_ == LockMode::EXCLUSIVE) {
      lock_request.granted_ = true;
      break;
    }
  }
  txn->GetExclusiveLockSet()->emplace(rid);

  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  auto txn_id = txn->GetTransactionId();
  auto lock_request_list = lock_table_[rid].request_queue_;

  if (txn->GetSharedLockSet()->erase(rid)) {
    lock_table_[rid].reading_count_--;
  }
  if (txn->GetExclusiveLockSet()->erase(rid)) {
    lock_table_[rid].is_writting_ = false;
  }

  auto itr = lock_request_list.begin();
  while (itr != lock_request_list.end()) {
    if (itr->txn_id_ == txn_id) {
      itr = lock_request_list.erase(itr);
    }
    else {
      itr++;
    }
  }

  lock_table_[rid].cv_.notify_all();
  
  if (txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }

  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  if (waits_for_.count(t1) == 0) {
    waits_for_.emplace(t1, std::vector<txn_id_t>{t2});
    return;
  }

  for (const auto &wait_txn : waits_for_[t1]) {
    if (wait_txn == t2) {
      return;
    }
  }

  waits_for_[t1].emplace_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto itr = waits_for_[t1].begin();
  while (itr != waits_for_[t1].end()) {
    if (*itr == t2) {
      waits_for_[t1].erase(itr);
      break;
    }
    itr++;
  }
}

bool LockManager::HasCycle(txn_id_t *txn_id) { 
  std::unordered_set<txn_id_t> visited;

  for (const auto &pair : waits_for_) {
    std::vector<txn_id_t> trial;
    if (dfs(pair.first, trial, visited, txn_id)) {
      return true;
    } 
  }

  return false;
}

bool LockManager::dfs(txn_id_t current, std::vector<txn_id_t> &trial, std::unordered_set<txn_id_t> &visited, txn_id_t *txn_id) {
  trial.emplace_back(current);
  visited.emplace(current);

  for (const auto &child : waits_for_[current]) {
    bool found = false;
    for (const auto &i : trial) {
      if (child == i) {
        *txn_id = i;
        found = true;
      }
      else if (found) {
        *txn_id = std::max(*txn_id, i);
      }
    }
    
    if (found) {
      return true;
    }

    if(!visited.count(child)) {
      if (dfs(child, trial, visited, txn_id)) {
        return true;
      }
    }
  }

  trial.pop_back();
  return false;
}

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::vector<std::pair<txn_id_t, txn_id_t>> res;
  for (const auto &pair : waits_for_) {
    txn_id_t a = pair.first;
    for (const auto &b : pair.second) {
      res.emplace_back(std::make_pair(a, b));
    }
  }

  return res; 
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
      std::unique_lock<std::mutex> l(latch_);
      
      waits_for_.clear();
      for (const auto &pair : lock_table_) {
        auto &rid = pair.first;
        auto &lock_request_queue = pair.second;

        if (!lock_request_queue.is_writting_ && lock_request_queue.reading_count_ == 0) {
          continue;
        }

        std::vector<txn_id_t> granted_share;
        std::vector<txn_id_t> granted_exclusive;
        std::vector<txn_id_t> ungranted_share;
        std::vector<txn_id_t> ungranted_exclusive;
        for (const auto &lock_request : lock_request_queue.request_queue_) {
          if (lock_request.granted_) {
            if (lock_request.lock_mode_ == LockMode::SHARED) {
              granted_share.emplace_back(lock_request.txn_id_);
            }
            else {
              granted_exclusive.emplace_back(lock_request.txn_id_);
            }
          }
          else {
            if (lock_request.lock_mode_ == LockMode::SHARED) {
              ungranted_share.emplace_back(lock_request.txn_id_);
            }
            else {
              ungranted_exclusive.emplace_back(lock_request.txn_id_);
            }
          }
        }

        for (const auto &a : ungranted_share) {
          for (const auto &b : granted_exclusive) {
            AddEdge(a, b);
          }
        }

        for (const auto &a : ungranted_exclusive) {
          for (const auto &b : granted_share) {
            AddEdge(a, b);
          }

          for (const auto &b : granted_exclusive) {
            AddEdge(a, b);
          }
        }

        txn_id_t txn_id;
        while (HasCycle(&txn_id)) {
          auto *txn = TransactionManager::GetTransaction(txn_id);
          txn->SetState(TransactionState::ABORTED);
          lock_table_[rid].cv_.notify_all();

          auto itr = waits_for_.begin();
          while (itr != waits_for_.end()) {
            if (itr->first == txn_id) {
              itr = waits_for_.erase(itr);
            }
            else{
              RemoveEdge(itr->first, txn_id);
              itr++;
            }
          }
        }
      }
  }
}

}  // namespace bustub
