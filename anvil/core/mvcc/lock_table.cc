#include "anvil/core/mvcc/lock_table.h"

#include <algorithm>

namespace anvil::mvcc {
namespace {

bool conflicts(LockKind held, LockKind wanted) noexcept {
  // Shared readers coexist; anything involving a write does not.
  return held == LockKind::kExclusive || wanted == LockKind::kExclusive;
}

}  // namespace

AcquireOutcome LockTable::acquire(TxnId txn, CommitTs start_ts, std::string_view key,
                                  LockKind kind, LockHolder* blocker) {
  ++stats_.acquired;
  start_ts_[txn.value()] = start_ts;

  auto& holders = locks_[std::string{key}];

  // Already ours?
  for (const LockHolder& holder : holders) {
    if (holder.txn != txn) continue;
    if (holder.kind == LockKind::kExclusive || holder.kind == kind) {
      return AcquireOutcome::kAlreadyHeld;
    }
  }

  // The first conflicting holder decides the outcome. Holders are kept in
  // acquisition order, so this is deterministic.
  for (const LockHolder& holder : holders) {
    if (holder.txn == txn) continue;
    if (!conflicts(holder.kind, kind)) continue;

    *blocker = holder;
    if (is_older(start_ts, txn, holder.start_ts, holder.txn)) {
      // The requester is older. It does not queue behind a younger
      // transaction; the younger one is wounded and the requester retries.
      ++stats_.wounds;
      waits_for_.erase(txn.value());
      return AcquireOutcome::kWoundHolder;
    }
    ++stats_.waits;
    waits_for_[txn.value()] = holder.txn;
    return AcquireOutcome::kWaiting;
  }

  waits_for_.erase(txn.value());
  holders.push_back(LockHolder{txn, start_ts, kind});
  ++stats_.granted_immediately;
  return AcquireOutcome::kGranted;
}

void LockTable::release(TxnId txn, std::string_view key) {
  const auto it = locks_.find(std::string{key});
  if (it == locks_.end()) return;
  auto& holders = it->second;
  const auto removed =
      std::remove_if(holders.begin(), holders.end(),
                     [txn](const LockHolder& holder) { return holder.txn == txn; });
  if (removed != holders.end()) ++stats_.releases;
  holders.erase(removed, holders.end());
  if (holders.empty()) locks_.erase(it);
}

void LockTable::release_all(TxnId txn) {
  for (auto it = locks_.begin(); it != locks_.end();) {
    auto& holders = it->second;
    const auto removed =
        std::remove_if(holders.begin(), holders.end(),
                       [txn](const LockHolder& holder) { return holder.txn == txn; });
    if (removed != holders.end()) ++stats_.releases;
    holders.erase(removed, holders.end());
    it = holders.empty() ? locks_.erase(it) : std::next(it);
  }
  waits_for_.erase(txn.value());
  start_ts_.erase(txn.value());
}

void LockTable::stop_waiting(TxnId txn) { waits_for_.erase(txn.value()); }

bool LockTable::holds(TxnId txn, std::string_view key) const {
  const auto it = locks_.find(std::string{key});
  if (it == locks_.end()) return false;
  for (const LockHolder& holder : it->second) {
    if (holder.txn == txn) return true;
  }
  return false;
}

std::set<std::uint64_t> LockTable::participants() const {
  std::set<std::uint64_t> out;
  for (const auto& [key, holders] : locks_) {
    for (const LockHolder& holder : holders) out.insert(holder.txn.value());
  }
  for (const auto& [waiter, blocker] : waits_for_) {
    out.insert(waiter);
    out.insert(blocker.value());
  }
  return out;
}

bool LockTable::find_cycle(std::vector<TxnId>* cycle) const {
  cycle->clear();
  // Iterative depth-first search over a functional graph -- each waiter has at
  // most one outgoing edge -- so "follow the chain and look for a repeat" is
  // the whole algorithm. Started from the lowest id first so that if several
  // cycles existed the reported one would still be the same every run.
  std::set<std::uint64_t> finished;
  for (const auto& [start, unused] : waits_for_) {
    (void)unused;
    if (finished.contains(start)) continue;

    std::vector<std::uint64_t> path;
    std::set<std::uint64_t> on_path;
    std::uint64_t current = start;
    for (;;) {
      if (on_path.contains(current)) {
        // Report from the first occurrence, so the witness is the cycle itself
        // and not the tail that led into it.
        const auto begin = std::find(path.begin(), path.end(), current);
        for (auto it = begin; it != path.end(); ++it) cycle->emplace_back(*it);
        return true;
      }
      if (finished.contains(current)) break;
      path.push_back(current);
      on_path.insert(current);
      const auto next = waits_for_.find(current);
      if (next == waits_for_.end()) break;
      current = next->second.value();
    }
    for (const std::uint64_t node : path) finished.insert(node);
  }
  return false;
}

}  // namespace anvil::mvcc
