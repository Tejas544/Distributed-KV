// The invariant framework: protocol-aware checking, from the inside out.
//
// Jepsen and Antithesis test a system through its client API and search the
// resulting histories for anomalies. That finds real bugs, and it structurally
// cannot find a whole category of them: anything that corrupts internal state
// but is masked before a client could observe it. A Raft log that briefly holds
// an entry it should not, a GC safepoint that advances past a live snapshot, a
// range-descriptor gap that closes before anyone routes through it -- all
// invisible at the boundary, all latent, all shipped.
//
// This is the other half. Every invariant here is a predicate over the global
// state the simulator can see, evaluated *while the system runs*, and a
// violation is reported at the tick it happens with the causal trace attached
// rather than inferred hours later from a history.
//
// Cost classes exist because that is only affordable if most predicates are
// cheap. A tick-class invariant runs after every scheduler event and must be
// O(nodes); a quiesce-class one runs once at the end and can do anything.
// Getting this wrong is not subtle -- an O(n^2) predicate at tick class turns
// 38,000 simulated node-hours per core-hour into a few hundred, which is the
// difference between a fleet and a demo.
//
// The most important thing in this file is `never_fired()`. An invariant that
// has never been observed to fail is not evidence of correctness; it is an
// untested assertion, and quite possibly a vacuous one (a predicate over a set
// that is always empty, a comparison that cannot be false). INV-SIM-05 requires
// every armed invariant to have fired at least once against *some* seeded
// mutation, and this is where that gets measured.

#ifndef ANVIL_CHECKER_INVARIANT_H_
#define ANVIL_CHECKER_INVARIANT_H_

#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "anvil/core/types.h"

namespace anvil::checker {

// When a predicate runs. Ordered cheapest-to-most-expensive.
enum class CostClass : std::uint8_t {
  kTick,     // after every scheduler event; must be O(nodes) or better
  kEpoch,    // every N events
  kCommit,   // on each transaction commit or abort
  kQuiesce,  // once, when faults have healed and the system is idle
  kOffline,  // post-hoc, over the recorded trace
};

const char* to_string(CostClass cost) noexcept;

struct Violation {
  std::string id;      // INV-RAFT-04
  std::string name;    // "Leader Completeness"
  std::string detail;  // the offending state, concretely
  Timestamp when;
  std::uint64_t tick = 0;

  std::string render() const;
};

// Returns a description of what went wrong, or nullopt if the predicate holds.
// Returning the detail string rather than a bool is deliberate: a violation
// report that says only "INV-RAFT-04 failed" costs an hour of bisecting to turn
// into something actionable.
using Predicate = std::function<std::optional<std::string>()>;

class InvariantRegistry {
 public:
  void arm(std::string id, std::string name, CostClass cost, Predicate predicate);

  // Evaluates every invariant of this class. Returns violations in registration
  // order, which is stable across runs.
  std::vector<Violation> evaluate(CostClass cost, Timestamp now, std::uint64_t tick);

  bool empty() const noexcept { return invariants_.empty(); }
  std::size_t size() const noexcept { return invariants_.size(); }

  struct Stats {
    std::string name;
    CostClass cost = CostClass::kTick;
    std::uint64_t evaluations = 0;
    std::uint64_t violations = 0;
  };

  const std::map<std::string, Stats>& stats() const noexcept { return stats_; }

  // Armed invariants that have never once reported a violation. Suspicious by
  // default: each needs either a seeded mutation that makes it fire, or a
  // written justification. See INV-SIM-05.
  std::vector<std::string> never_fired() const;

  // How many events between kEpoch evaluations.
  void set_epoch(std::uint64_t events) noexcept { epoch_ = events; }
  std::uint64_t epoch() const noexcept { return epoch_; }

 private:
  struct Entry {
    std::string id;
    std::string name;
    CostClass cost = CostClass::kTick;
    Predicate predicate;
  };

  std::vector<Entry> invariants_;  // registration order, deliberately not a map
  std::map<std::string, Stats> stats_;
  std::uint64_t epoch_ = 1000;
};

}  // namespace anvil::checker

#endif  // ANVIL_CHECKER_INVARIANT_H_
