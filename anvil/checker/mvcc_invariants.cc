#include "anvil/checker/mvcc_invariants.h"

#include <algorithm>

namespace anvil::checker {
namespace {

std::string txn_name(TxnId id) { return "t" + std::to_string(id.value()); }

using Result = std::optional<std::string>;

Predicate audited(MvccObserver* obs, std::string id) {
  return [obs, id]() -> Result {
    obs->refresh();
    return obs->take(id);
  };
}

}  // namespace

void MvccObserver::configure(const mvcc::TxnManager* txns, const mvcc::LockTable* locks,
                             std::function<std::uint64_t()> tick) {
  txns_ = txns;
  locks_ = locks;
  tick_ = std::move(tick);
}

void MvccObserver::record(const std::string& id, std::string detail) {
  auto& queue = pending_[id];
  if (queue.size() < 4) queue.push_back(std::move(detail));
}

std::optional<std::string> MvccObserver::take(const std::string& id) {
  const auto it = pending_.find(id);
  if (it == pending_.end() || it->second.empty()) return std::nullopt;
  std::string out = std::move(it->second.front());
  it->second.erase(it->second.begin());
  return out;
}

void MvccObserver::refresh() {
  const std::uint64_t tick = tick_ ? tick_() : 0;
  if (tick == last_tick_) return;
  last_tick_ = tick;
  ++counters_.refreshes;
}

void MvccObserver::note_safepoint(mvcc::CommitTs ts, mvcc::CommitTs floor_now,
                                  std::string_view owner) {
  ++counters_.safepoints_seen;
  highest_safepoint_ = std::max(highest_safepoint_, ts);
  if (ts > floor_now) {
    record("INV-MVCC-02", "safepoint " + std::to_string(ts) + " was published while " +
                              std::string{owner} + " still held " + std::to_string(floor_now));
  }
}

// ---------------------------------------------------------------------------

void arm_mvcc_invariants(InvariantRegistry& registry, MvccObserver* observer) {
  // ---- audited: the version store is behind a coroutine -------------------

  registry.arm("INV-MVCC-01", "GC never removes a version a live snapshot resolves to",
               CostClass::kTick, audited(observer, "INV-MVCC-01"));
  registry.arm("INV-MVCC-03", "versions of a key are strictly descending and unique",
               CostClass::kEpoch, audited(observer, "INV-MVCC-03"));
  registry.arm("INV-MVCC-05", "an intent and a committed version never share a timestamp",
               CostClass::kTick, audited(observer, "INV-MVCC-05"));
  registry.arm("INV-MVCC-04", "a read returns the newest version at or below its snapshot",
               CostClass::kTick, audited(observer, "INV-MVCC-04"));

  // ---- live: ordinary memory ---------------------------------------------

  // INV-MVCC-02. The safepoint must never exceed the oldest thing that could
  // still read below it. Checked against the *highest safepoint ever
  // published*, not the current one: a safepoint that was briefly too high has
  // already told the collector to delete, and by the time it settles back down
  // the damage is done and invisible.
  registry.arm("INV-MVCC-02", "the safepoint is at most the oldest live reader",
               CostClass::kTick, audited(observer, "INV-MVCC-02"));

  // INV-MVCC-06. Wound-wait makes a cycle impossible by construction, so this
  // is a check on the reasoning rather than a recovery mechanism. If it ever
  // fires, the age comparison is wrong -- not the detector.
  registry.arm(
      "INV-MVCC-06", "the wait-for graph is acyclic", CostClass::kEpoch,
      [observer]() -> Result {
        observer->refresh();
        const mvcc::LockTable* locks = observer->locks();
        if (locks == nullptr) return std::nullopt;
        std::vector<TxnId> cycle;
        if (!locks->find_cycle(&cycle)) return std::nullopt;
        std::string detail = "wait-for cycle:";
        for (const TxnId id : cycle) detail += " " + txn_name(id);
        return detail;
      });

  // INV-MVCC-07. Every wait edge must point from younger to older. That is the
  // whole safety argument for wound-wait, and it is one comparison -- which is
  // exactly the kind of thing that gets written backwards and never noticed,
  // because a backwards wound-wait still makes progress most of the time.
  registry.arm(
      "INV-MVCC-07", "no younger transaction is waited on by an older one", CostClass::kTick,
      [observer]() -> Result {
        observer->refresh();
        const mvcc::LockTable* locks = observer->locks();
        const mvcc::TxnManager* txns = observer->txns();
        if (locks == nullptr || txns == nullptr) return std::nullopt;

        for (const auto& [waiter_id, blocker] : locks->waits_for()) {
          const mvcc::Txn* waiter = txns->find(TxnId{waiter_id});
          const mvcc::Txn* held_by = txns->find(blocker);
          if (waiter == nullptr || held_by == nullptr) continue;
          if (mvcc::is_older(waiter->start_ts, waiter->id, held_by->start_ts, held_by->id)) {
            return txn_name(waiter->id) + " (start " + std::to_string(waiter->start_ts) +
                   ") is waiting for the younger " + txn_name(held_by->id) + " (start " +
                   std::to_string(held_by->start_ts) +
                   ") -- wound-wait would have wounded it instead";
          }
        }
        return std::nullopt;
      });

  // INV-MVCC-08. A lock outliving its transaction is how an orphaned intent
  // blocks a key forever: every later reader finds an intent, tries to resolve
  // it, and discovers the owner does not exist. Catching it here is much
  // cheaper than catching it as a workload that mysteriously stops progressing.
  registry.arm(
      "INV-MVCC-08", "every lock is owned by a transaction the system knows about",
      CostClass::kEpoch, [observer]() -> Result {
        observer->refresh();
        const mvcc::LockTable* locks = observer->locks();
        const mvcc::TxnManager* txns = observer->txns();
        if (locks == nullptr || txns == nullptr) return std::nullopt;

        for (const auto& [key, holders] : locks->locks()) {
          for (const mvcc::LockHolder& holder : holders) {
            const mvcc::Txn* owner = txns->find(holder.txn);
            if (owner == nullptr) {
              return "a lock on a key is held by " + txn_name(holder.txn) +
                     ", which the transaction manager has never heard of";
            }
            if (owner->state == mvcc::TxnState::kCommitted ||
                owner->state == mvcc::TxnState::kAborted) {
              return "a lock is still held by " + txn_name(holder.txn) + ", which is " +
                     mvcc::to_string(owner->state) +
                     " -- its locks should have been released with it";
            }
          }
        }
        return std::nullopt;
      });
}

}  // namespace anvil::checker
