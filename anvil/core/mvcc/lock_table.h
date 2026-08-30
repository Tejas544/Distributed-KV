// The lock table: who is waiting for whom, and who has to die.
//
// Wound-wait, by transaction start timestamp. When T asks for a lock held by H:
//
//   T older than H   T *wounds* H. H is aborted and T proceeds. An older
//                    transaction never waits behind a younger one.
//   T younger than H   T waits.
//
// The asymmetry is the entire point: every wait edge points from younger to
// older, so a cycle would need a strictly-increasing sequence of start
// timestamps that closes on itself, which cannot exist. Wound-wait is
// deadlock-free *by construction*, not by detection.
//
// So why is there a wait-for graph and a cycle detector in here at all? Because
// "deadlock-free by construction" is an argument, and an argument is worth
// exactly as much as the code that implements it. INV-MVCC-06 asserts the graph
// is acyclic; if the wound-wait comparison is ever written backwards -- one `<`
// becoming `<=`, or start timestamps that tie -- the construction stops holding
// and the detector is what says so. It is a check on the reasoning, not a
// fallback mechanism, and it should never fire.
//
// Ties matter and are handled explicitly. Two transactions with identical start
// timestamps would each consider the other "not older" and both would wait, and
// that *is* a cycle. The tiebreak is the transaction id, which is unique and
// totally ordered, so the comparison is a strict order on (start_ts, id).

#ifndef ANVIL_CORE_MVCC_LOCK_TABLE_H_
#define ANVIL_CORE_MVCC_LOCK_TABLE_H_

#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "anvil/core/mvcc/key.h"
#include "anvil/core/types.h"

namespace anvil::mvcc {

enum class LockKind : std::uint8_t {
  kShared,     // a read that must not be overwritten before it commits
  kExclusive,  // a write intent
};

enum class AcquireOutcome : std::uint8_t {
  kGranted,
  kWaiting,      // the requester is younger and must wait for `blocker`
  kWoundHolder,  // the requester is older; `blocker` must be aborted, then retry
  kAlreadyHeld,  // this transaction already holds a strong enough lock
};

struct LockHolder {
  TxnId txn{};
  CommitTs start_ts = 0;
  LockKind kind = LockKind::kExclusive;
};

// Strictly orders transactions by age. Older means smaller start timestamp;
// identical timestamps are broken by id, which is unique -- without that, two
// transactions that started in the same nanosecond each decide the other is not
// older and both wait, which is a two-cycle and a genuine deadlock.
inline bool is_older(CommitTs a_ts, TxnId a, CommitTs b_ts, TxnId b) noexcept {
  if (a_ts != b_ts) return a_ts < b_ts;
  return a.value() < b.value();
}

struct LockStats {
  std::uint64_t acquired = 0;
  std::uint64_t granted_immediately = 0;
  std::uint64_t waits = 0;
  std::uint64_t wounds = 0;
  std::uint64_t releases = 0;
  std::uint64_t cycles_detected = 0;
};

class LockTable {
 public:
  // Ordered containers throughout. Lock acquisition order decides which
  // transaction wounds which, and a hash-container iteration would make that
  // depend on a seed the simulator does not control (INV-SIM-01).
  AcquireOutcome acquire(TxnId txn, CommitTs start_ts, std::string_view key, LockKind kind,
                         LockHolder* blocker);

  void release(TxnId txn, std::string_view key);
  void release_all(TxnId txn);

  // Waiting is recorded separately from holding so the wait-for graph can be
  // built without walking every key.
  void stop_waiting(TxnId txn);

  bool holds(TxnId txn, std::string_view key) const;
  const std::map<std::string, std::vector<LockHolder>>& locks() const noexcept {
    return locks_;
  }
  const std::map<std::uint64_t, TxnId>& waits_for() const noexcept { return waits_for_; }

  // Depth-first over the wait-for graph, in id order so the reported cycle is
  // the same on every machine. A witness nobody can reproduce is not a witness.
  bool find_cycle(std::vector<TxnId>* cycle) const;

  // Every transaction currently holding or waiting for anything.
  std::set<std::uint64_t> participants() const;

  const LockStats& stats() const noexcept { return stats_; }
  void reset_stats() noexcept { stats_ = LockStats{}; }

 private:
  std::map<std::string, std::vector<LockHolder>> locks_;
  // waiter -> the transaction it is waiting for. One edge per waiter, because a
  // transaction blocks on exactly one lock at a time.
  std::map<std::uint64_t, TxnId> waits_for_;
  std::map<std::uint64_t, CommitTs> start_ts_;
  LockStats stats_;
};

}  // namespace anvil::mvcc

#endif  // ANVIL_CORE_MVCC_LOCK_TABLE_H_
