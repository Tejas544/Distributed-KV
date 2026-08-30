// The god's-eye view of MVCC: safepoints, locks, and version chains.
//
// The roadmap's warning for P4 is blunt and correct -- "GC safepoints are the
// classic silent-corruption source. Over-invest here; the invariant is cheap and
// the bug is catastrophic." So these are armed before the collector is trusted,
// not after it has been observed to work.
//
// What makes GC bugs so nasty is that nothing complains. Delete the version a
// live reader resolves to and the reader does not get an error: it falls
// through to an older value, or to nothing, and returns that. Every checksum
// passes. Every status code is ok. The only way to catch it is to hold the
// question "could any live reader still need this?" somewhere outside the
// collector, which is what this file is.
//
// Two kinds of check live here, split by what can be answered synchronously:
//
//   live       the lock table and the transaction manager are ordinary memory,
//              so INV-MVCC-02/06/07/08 are predicates over them and run at
//              tick class.
//   audited    the version store is behind a coroutine, and an invariant
//              predicate cannot await. A workload task reads it periodically
//              and posts what it finds here; the predicate reports within one
//              event of the post. INV-MVCC-01/03/05 work this way.
//
// The split is a real limitation and worth stating rather than hiding: an
// audited invariant fires at the next audit, not at the instant of the fault.
// The audit interval is therefore part of the detection latency, and the
// workload keeps it short.

#ifndef ANVIL_CHECKER_MVCC_INVARIANTS_H_
#define ANVIL_CHECKER_MVCC_INVARIANTS_H_

#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "anvil/checker/invariant.h"
#include "anvil/core/mvcc/lock_table.h"
#include "anvil/core/mvcc/txn.h"
#include "anvil/core/types.h"

namespace anvil::checker {

class MvccObserver {
 public:
  void configure(const mvcc::TxnManager* txns, const mvcc::LockTable* locks,
                 std::function<std::uint64_t()> tick);

  // Posted by the workload's auditor for the checks that need to read the store.
  void record(const std::string& id, std::string detail);
  std::optional<std::string> take(const std::string& id);

  // Advances the synchronous mirror. Idempotent within one scheduler tick, for
  // the same reason the Raft observer's is: fourteen predicates each doing the
  // same O(n) diff would be fourteen times the cost for one answer.
  void refresh();

  const mvcc::TxnManager* txns() const noexcept { return txns_; }
  const mvcc::LockTable* locks() const noexcept { return locks_; }

  // The highest safepoint ever published. INV-MVCC-02 is a statement about
  // every safepoint the collector has ever used, not only the current one --
  // a safepoint that was briefly too high has already done its damage by the
  // time it comes back down.
  // Judged at the instant it is published, against the floor in force at that
  // instant. Comparing the highest safepoint ever seen against the floor as it
  // stands later is not a weaker check, it is an unsound one: a transaction that
  // begins after a perfectly legal collection lowers the floor beneath a
  // safepoint that was correct when it was used, and the invariant reports a bug
  // that never existed. Same lesson as INV-RAFT-10 -- a decision is judged
  // against the state in force when it was made (CONTEXT gotcha 10.11).
  void note_safepoint(mvcc::CommitTs ts, mvcc::CommitTs floor_now, std::string_view owner);
  mvcc::CommitTs highest_safepoint() const noexcept { return highest_safepoint_; }

  struct Counters {
    std::uint64_t refreshes = 0;
    std::uint64_t audits = 0;
    std::uint64_t versions_audited = 0;
    std::uint64_t safepoints_seen = 0;
  };
  const Counters& counters() const noexcept { return counters_; }
  void note_audit(std::uint64_t versions) noexcept {
    ++counters_.audits;
    counters_.versions_audited += versions;
  }

 private:
  const mvcc::TxnManager* txns_ = nullptr;
  const mvcc::LockTable* locks_ = nullptr;
  std::function<std::uint64_t()> tick_;

  std::map<std::string, std::vector<std::string>> pending_;
  std::uint64_t last_tick_ = UINT64_MAX;
  mvcc::CommitTs highest_safepoint_ = 0;
  Counters counters_;
};

// Arms INV-MVCC-01..08. The audited ones (01, 03, 05) only ever report what the
// workload's auditor posted; the live ones (02, 06, 07, 08) evaluate directly.
void arm_mvcc_invariants(InvariantRegistry& registry, MvccObserver* observer);

}  // namespace anvil::checker

#endif  // ANVIL_CHECKER_MVCC_INVARIANTS_H_
