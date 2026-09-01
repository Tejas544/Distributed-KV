// P6 exit criteria: three transaction engines, one coordinator, checked from
// inside and from outside at once.
//
// Four questions:
//
//   1. Does the bank's total ever move?
//      The client-visible oracle, same shape as P5's: every failure this phase
//      is about -- a lost intent, a resolved-twice commit, a transaction that
//      both committed and rolled back -- moves a single integer a black-box
//      client could compute. Run under the full adversary, with the topology
//      underneath the transactions splitting and merging at the same time, so
//      a transaction routinely finds its ranges have moved between two of its
//      own steps.
//
//   2. Does the checker's verdict match the level each engine actually claims?
//      The list-append workload makes every key's version order directly
//      observable, so the Elle-style checker can say, not assume, whether a
//      run was free of the anomalies its declared level forbids. Snapshot
//      isolation must show write skew and no cycle without one; serializable
//      and strict serializable must show none at all; strict serializable
//      additionally must respect real time. This is confirmed on a hand-built
//      history first -- so a bug in the checker itself is not mistaken for a
//      bug in the engine -- and then checked live against every seed.
//
//   3. Is a seed still a complete description of the run?
//      One placement group, one oracle group, one Raft group per range, a
//      coordinator on every node -- more moving state than any phase before
//      it, and if determinism breaks anywhere it breaks here.
//
//   4. Would any of this notice if a transaction engine were wrong?
//      Nine deliberate bugs across the coordinator and the version store,
//      every default correct. Each must be detected, and for each we record
//      whether a client could have seen it.
//
// Nothing here asserts on wall-clock time, and every failure prints its seed.

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "anvil/checker/elle.h"
#include "anvil/checker/txn_invariants.h"
#include "anvil/sim/simulation.h"
#include "workloads/txn_bank.h"

namespace {

using anvil::Duration;
using anvil::NodeId;
using anvil::Timestamp;
namespace sim = anvil::sim;
namespace shard = anvil::shard;
namespace txn = anvil::txn;
namespace checker = anvil::checker;
namespace workloads = anvil::workloads;

int g_failures = 0;

void check(bool condition, std::string_view what) {
  if (!condition) {
    std::cerr << "FAIL: " << what << "\n";
    ++g_failures;
  }
}

checker::IsolationLevel to_isolation_level(txn::Level level) {
  switch (level) {
    case txn::Level::kSnapshot: return checker::IsolationLevel::kSnapshotIsolation;
    case txn::Level::kSerializable: return checker::IsolationLevel::kSerializable;
    case txn::Level::kStrictSerializable: return checker::IsolationLevel::kStrictSerializable;
  }
  return checker::IsolationLevel::kSerializable;
}

// ---------------------------------------------------------------------------
// 2, confirmed on paper before it is trusted live: a hand-built history at
// two keys, so the version order needs no third "observer" transaction to be
// recovered -- each key here is written by exactly one transaction, which
// makes its order trivial and lets the test isolate exactly what it claims to.
// ---------------------------------------------------------------------------

void test_snapshot_isolation_shows_write_skew_and_serializable_does_not() {
  checker::History history;
  const Timestamp t0{1000, 0};
  const Timestamp t1{1100, 0};
  const Timestamp t2{1200, 0};
  const Timestamp t3{1300, 0};

  // Two concurrent transactions, each reading the key the other is about to
  // write and appending to a key of its own -- the classic multi-key write
  // skew shape (two doctors, one on-call constraint, each looks at the
  // other's key before deciding to leave).
  const checker::TxnId a = history.begin(/*process=*/1, t0);
  history.read(a, /*key=*/2, {});
  history.append(a, /*key=*/1, /*element=*/100);
  history.complete(a, checker::Outcome::kCommitted, t1);

  const checker::TxnId b = history.begin(/*process=*/2, t0);
  history.read(b, /*key=*/1, {});
  history.append(b, /*key=*/2, /*element=*/200);
  history.complete(b, checker::Outcome::kCommitted, t2);

  // The version order for a key is recovered *from reads that saw it* (see
  // elle.cc's recover_version_orders) -- an append nobody ever reads back is
  // invisible to the graph, not merely unconfirmed. Two observer transactions,
  // each reading one key after both writers have committed, are what make the
  // anti-dependency edges constructible at all; this is the same role the
  // observer transaction plays in the single-key write-skew case one layer
  // down (mvcc_faults.cc), for the same reason.
  const checker::TxnId observe_1 = history.begin(/*process=*/3, t3);
  history.read(observe_1, /*key=*/1, {100});
  history.complete(observe_1, checker::Outcome::kCommitted, t3);

  const checker::TxnId observe_2 = history.begin(/*process=*/3, t3);
  history.read(observe_2, /*key=*/2, {200});
  history.complete(observe_2, checker::Outcome::kCommitted, t3);

  const checker::CheckResult at_si =
      checker::check(history, checker::IsolationLevel::kSnapshotIsolation);
  check(at_si.valid, "write skew across two keys is legal at snapshot isolation: " + at_si.summary());
  check(!at_si.has(checker::Anomaly::kG1c),
        "and it is not G1c -- reporting it as one would be over-reporting");

  const checker::CheckResult at_serializable =
      checker::check(history, checker::IsolationLevel::kSerializable);
  check(!at_serializable.valid,
        "the same history must be rejected at serializable: " + at_serializable.summary());
  check(at_serializable.has(checker::Anomaly::kG2Item),
        "and classified as G2-item -- two anti-dependencies, not some other anomaly");

  check(checker::forbids(checker::IsolationLevel::kSerializable, checker::Anomaly::kG2Item),
        "the level table forbids write skew at serializable");
  check(!checker::forbids(checker::IsolationLevel::kSnapshotIsolation, checker::Anomaly::kG2Item),
        "and permits it at snapshot isolation -- which is what makes SI a distinct level");
}

void test_strict_serializable_catches_a_real_time_violation() {
  checker::History history;
  const Timestamp t0{1000, 0};
  const Timestamp t1{1100, 0};  // T1 fully committed here
  const Timestamp t2{1200, 0};  // T2 invoked strictly afterward
  const Timestamp t3{1300, 0};
  const Timestamp t4{1400, 0};

  const checker::TxnId a = history.begin(/*process=*/1, t0);
  history.append(a, /*key=*/9, /*element=*/900);
  history.complete(a, checker::Outcome::kCommitted, t1);

  const checker::TxnId b = history.begin(/*process=*/2, t2);
  history.read(b, /*key=*/9, {});  // reads as though T1 never happened
  history.complete(b, checker::Outcome::kCommitted, t3);

  // Establishes key 9's recovered order -- see the comment in the write-skew
  // test above for why a read that observes the append is what makes it exist
  // in the graph at all.
  const checker::TxnId observe = history.begin(/*process=*/3, t4);
  history.read(observe, /*key=*/9, {900});
  history.complete(observe, checker::Outcome::kCommitted, t4);

  const checker::CheckResult at_serializable =
      checker::check(history, checker::IsolationLevel::kSerializable);
  check(at_serializable.valid,
        "the order T2-before-T1 is a legal serialization with no clock in the argument: " +
            at_serializable.summary());

  const checker::CheckResult at_strict =
      checker::check(history, checker::IsolationLevel::kStrictSerializable);
  check(!at_strict.valid,
        "but it contradicts real time -- T1 finished before T2 was even invoked: " +
            at_strict.summary());
  check(at_strict.has(checker::Anomaly::kRealTimeViolation),
        "and is classified as exactly that, not folded into G-single or G2-item");
}

// ---------------------------------------------------------------------------
// one seed, live
// ---------------------------------------------------------------------------

struct Summary {
  std::uint64_t seed = 0;
  std::uint32_t nodes = 0;
  txn::Level level = txn::Level::kSerializable;
  workloads::TxnWorkloadKind kind = workloads::TxnWorkloadKind::kBank;

  sim::StopReason faulted_reason = sim::StopReason::kQuiesced;
  sim::StopReason settled_reason = sim::StopReason::kQuiesced;
  std::string panic_message;
  std::vector<anvil::checker::Violation> violations;

  std::uint64_t committed = 0;
  std::uint64_t aborted = 0;
  std::uint64_t unknown = 0;
  std::uint64_t restarts = 0;
  std::uint64_t reads = 0;
  std::uint64_t writes = 0;
  std::uint64_t cross_range = 0;
  std::uint64_t single_range = 0;

  std::int64_t expected_total = 0;
  std::int64_t final_total = 0;
  std::uint64_t lost_elements = 0;
  std::uint64_t duplicated_elements = 0;
  std::vector<std::string> workload_violations;
  bool converged = false;
  std::uint64_t orphaned_intents = 0;

  checker::TxnObserver::Counters observer;
  std::optional<checker::CheckResult> elle;

  sim::FaultSummary faults;
  std::uint64_t digest_low = 0;
  std::uint64_t events = 0;
  Timestamp sim_time;
  std::map<std::string, anvil::checker::InvariantRegistry::Stats> invariants;

  bool client_safe() const {
    if (kind == workloads::TxnWorkloadKind::kBank) {
      return final_total == expected_total && workload_violations.empty();
    }
    return lost_elements == 0 && duplicated_elements == 0 && workload_violations.empty();
  }

  bool checker_safe() const { return violations.empty(); }

  bool safe() const {
    if (faulted_reason == sim::StopReason::kPanic) return false;
    if (settled_reason == sim::StopReason::kPanic) return false;
    if (!client_safe()) return false;
    if (!checker_safe()) return false;
    if (elle.has_value() && !elle->valid) return false;
    return true;
  }

  bool detected() const { return !safe(); }

  // What a black-box client could have seen: money that does not add up, an
  // acknowledged element the cluster lost or duplicated. The checker's own
  // verdict is deliberately excluded -- that is the internal half.
  bool api_visible() const { return !client_safe(); }

  std::set<std::string> fired_ids() const {
    std::set<std::string> out;
    for (const auto& violation : violations) out.insert(violation.id);
    if (elle.has_value() && !elle->valid) {
      for (const checker::Anomaly a : elle->anomalies) out.insert(checker::to_string(a));
    }
    return out;
  }
};

// Everything the adversary did, undone. Same shape as P5's: the topology needs
// time to finish any merge in flight before the audit runs, so this is called
// directly rather than through heal_and_settle.
void heal_everything(sim::Simulation& simulation) {
  simulation.net().heal_all();
  simulation.net().stop_injecting();
  simulation.disk().stop_injecting();
  for (std::uint32_t i = 1; i <= simulation.node_count(); ++i) {
    const NodeId id{i};
    simulation.clock().thaw(id);
    simulation.scheduler().resume_node(id);
    if (!simulation.process().alive(id)) simulation.process().restart(id);
  }
  simulation.faults().disarm();
}

struct RunOptions {
  bool inject_faults = true;
  Duration max_time = Duration::seconds(20);
  Duration settle = Duration::seconds(25);
  std::uint32_t nodes = 5;
  workloads::TxnBankConfig workload;
};

// The topology is not allowed to sit still, same reasoning as P5's
// churn_profile: overlapping split and merge thresholds so a transaction's
// ranges routinely move between two of its own steps.
workloads::TxnBankConfig base_profile(txn::Level level,
                                      workloads::TxnWorkloadKind kind =
                                          workloads::TxnWorkloadKind::kBank) {
  workloads::TxnBankConfig config;
  config.kind = kind;
  config.txn.level = level;
  // Strict serializability's distinguishing mechanism -- commit-wait, and the
  // uncertainty window a read can be forced to restart out of -- only exists
  // under the HLC. Leaving the source at its kOracle default would make this
  // level behave exactly like plain serializable and never exercise either
  // one.
  config.txn.source = level == txn::Level::kStrictSerializable ? txn::TsSource::kHybrid
                                                                : txn::TsSource::kOracle;
  config.keys = 16;
  config.neighbourhood = 4;
  config.txns_per_client = 16;
  config.store.placement.split_threshold_keys = 6;
  config.store.placement.merge_threshold_keys = 4;
  config.store.placement.change_cooldown_entries = 24;
  return config;
}

Summary run_seed(std::uint64_t seed, RunOptions options) {
  sim::SimConfig cfg = sim::SimConfig::from_seed(seed);
  if (!options.inject_faults) cfg.faults = sim::FaultProfile::none();
  cfg.max_time = options.max_time;
  cfg.nodes = options.nodes;

  Summary summary;
  summary.seed = seed;
  summary.nodes = cfg.nodes;
  summary.level = options.workload.txn.level;
  summary.kind = options.workload.kind;

  sim::Simulation simulation{cfg};
  checker::TxnObserver observer;
  workloads::TxnBankState state;
  workloads::install(simulation, options.workload, &state, &observer);

  const sim::RunResult faulted = simulation.run();
  summary.faulted_reason = faulted.reason;
  summary.panic_message = faulted.panic_message;
  summary.violations = faulted.violations;

  heal_everything(simulation);
  const sim::RunResult settled = simulation.heal_and_settle(options.settle);
  summary.settled_reason = settled.reason;
  summary.panic_message += settled.panic_message;
  summary.violations.insert(summary.violations.end(), settled.violations.begin(),
                            settled.violations.end());

  workloads::audit(simulation, &state);

  summary.committed = state.committed;
  summary.aborted = state.aborted;
  summary.unknown = state.unknown;
  summary.restarts = state.restarts;
  summary.reads = state.reads;
  summary.writes = state.writes;
  summary.cross_range = state.cross_range;
  summary.single_range = state.single_range;
  summary.expected_total = state.expected_total;
  summary.final_total = state.final_total;
  summary.lost_elements = state.lost_elements;
  summary.duplicated_elements = state.duplicated_elements;
  summary.workload_violations = state.violations;
  summary.converged = workloads::converged(simulation, state);
  summary.orphaned_intents = workloads::orphaned_intents(simulation, state);

  if (state.config.kind == workloads::TxnWorkloadKind::kListAppend) {
    summary.elle = checker::check(state.history, to_isolation_level(summary.level));
  }

  summary.observer = observer.counters();
  summary.faults = simulation.faults().summary();
  summary.digest_low = settled.digest.low();
  summary.events = settled.events;
  summary.sim_time = settled.sim_time;
  summary.invariants = simulation.invariants().stats();
  return summary;
}

void report(const Summary& s, const char* what) {
  std::cerr << "  seed " << s.seed << " (" << what << ", " << txn::to_string(s.level)
            << "): committed=" << s.committed << " aborted=" << s.aborted
            << " unknown=" << s.unknown;
  if (s.kind == workloads::TxnWorkloadKind::kBank) {
    std::cerr << " total=" << s.final_total << "/" << s.expected_total;
  } else {
    std::cerr << " lost_elements=" << s.lost_elements
               << " duplicated_elements=" << s.duplicated_elements;
  }
  std::cerr << " converged=" << (s.converged ? "yes" : "no") << "\n";
  for (const auto& violation : s.violations) std::cerr << "    " << violation.render() << "\n";
  for (const auto& line : s.workload_violations) std::cerr << "    " << line << "\n";
  if (s.elle.has_value() && !s.elle->valid) std::cerr << "    elle: " << s.elle->summary() << "\n";
}

// ---------------------------------------------------------------------------
// 1: the bank's total, under faults, with the topology churning underneath it
// ---------------------------------------------------------------------------

void test_safety_under_faults(std::uint64_t seeds_per_level) {
  const txn::Level levels[] = {txn::Level::kSnapshot, txn::Level::kSerializable,
                               txn::Level::kStrictSerializable};
  bool all_safe = true;
  std::uint64_t committed = 0;
  std::uint64_t aborted = 0;
  std::uint64_t cross_range = 0;
  std::uint64_t orphans_total = 0;
  Duration node_time{};

  for (const txn::Level level : levels) {
    RunOptions options;
    options.workload = base_profile(level, workloads::TxnWorkloadKind::kBank);
    for (std::uint64_t seed = 1; seed <= seeds_per_level; ++seed) {
      const Summary s = run_seed(seed, options);
      if (!s.safe()) {
        all_safe = false;
        report(s, "faults");
      }
      committed += s.committed;
      aborted += s.aborted;
      cross_range += s.cross_range;
      orphans_total += s.orphaned_intents;
      node_time = node_time + Duration{static_cast<std::int64_t>(s.sim_time.physical) *
                                       static_cast<std::int64_t>(s.nodes)};
    }
  }

  check(all_safe,
        "no seed at any level may lose the bank's total, violate an INV-TXN-* invariant, "
        "or leave an orphaned intent past its TTL");
  check(cross_range > 0,
        "the workload must actually exercise a transaction that spans more than one range");

  const std::int64_t minutes = node_time.nanos() / 60'000'000'000LL;
  std::cout << "  under faults (3 levels x " << seeds_per_level
            << " seeds): " << committed << " committed, " << aborted << " aborted, "
            << cross_range << " cross-range transactions, " << orphans_total
            << " orphaned intents observed after settling, " << minutes
            << "m simulated node-time\n";
}

// ---------------------------------------------------------------------------
// 2: the checker's verdict matches the declared level, live
// ---------------------------------------------------------------------------

void test_isolation_matches_the_declared_level(std::uint64_t seeds) {
  const txn::Level levels[] = {txn::Level::kSnapshot, txn::Level::kSerializable,
                               txn::Level::kStrictSerializable};
  for (const txn::Level level : levels) {
    RunOptions options;
    options.workload = base_profile(level, workloads::TxnWorkloadKind::kListAppend);
    std::uint64_t clean = 0;
    for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
      const Summary s = run_seed(seed, options);
      const bool ok = s.elle.has_value() && s.elle->valid && s.client_safe() && s.checker_safe();
      if (ok) {
        ++clean;
      } else {
        report(s, "isolation");
      }
    }
    check(clean == seeds, std::string{"every seed at "} + txn::to_string(level) +
                              " must show no anomaly its own level forbids");
    std::cout << "  " << txn::to_string(level) << ": " << clean << "/" << seeds
              << " seeds show a checker-clean history\n";
  }
}

// ---------------------------------------------------------------------------
// 3: determinism
// ---------------------------------------------------------------------------

void test_determinism(std::uint64_t seeds) {
  RunOptions options;
  options.workload = base_profile(txn::Level::kSerializable, workloads::TxnWorkloadKind::kBank);
  options.max_time = Duration::seconds(10);
  options.settle = Duration::seconds(10);

  std::size_t identical = 0;
  for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
    const Summary first = run_seed(seed, options);
    const Summary second = run_seed(seed, options);
    const bool same = first.digest_low == second.digest_low && first.events == second.events &&
                      first.committed == second.committed &&
                      first.final_total == second.final_total;
    if (same) {
      ++identical;
    } else {
      std::cerr << "  seed " << seed << " diverged: digest " << first.digest_low << " vs "
                << second.digest_low << ", events " << first.events << " vs " << second.events
                << ", committed " << first.committed << " vs " << second.committed << "\n";
    }
  }
  check(identical == seeds, "the same seed must produce the same run, coordinators and all");
  std::cout << "  determinism: " << identical << "/" << seeds
            << " seeds reproduce exactly, with the oracle group and every range group "
               "created and destroyed mid-run\n";
}

// ---------------------------------------------------------------------------
// 4: the seeded-mutation drill
// ---------------------------------------------------------------------------

enum class Expectation { kMustDetect, kControl };

struct Mutation {
  const char* name;
  Expectation expectation;
  txn::Level level;  // the level at which this flag's effect is observable
  void (*apply)(workloads::TxnBankConfig*);
  const char* note;
};

struct DrillResult {
  std::uint64_t detected = 0;
  std::uint64_t seeds = 0;
  std::uint64_t api_visible = 0;
  std::set<std::string> fired;
  std::uint64_t first_seed = 0;
};

DrillResult run_mutation(const Mutation& mutation, std::uint64_t seeds) {
  DrillResult result;
  result.seeds = seeds;
  RunOptions options;
  options.workload = base_profile(mutation.level, workloads::TxnWorkloadKind::kBank);
  mutation.apply(&options.workload);

  for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
    const Summary s = run_seed(seed, options);
    if (s.detected()) {
      ++result.detected;
      if (result.first_seed == 0) result.first_seed = seed;
      if (s.api_visible()) ++result.api_visible;
      for (const std::string& id : s.fired_ids()) result.fired.insert(id);
      if (!s.client_safe() && s.kind == workloads::TxnWorkloadKind::kBank) {
        result.fired.insert("conservation");
      }
    }
  }
  return result;
}

void test_seeded_mutation_drill(std::uint64_t seeds) {
  const Mutation mutations[] = {
      {"no refresh on push", Expectation::kMustDetect, txn::Level::kSerializable,
       [](workloads::TxnBankConfig* c) { c->txn.refresh_reads_on_push = false; },
       "a pushed transaction commits without re-checking what it read: write skew wearing "
       "serializability's name"},
      {"secondaries before primary", Expectation::kMustDetect, txn::Level::kSnapshot,
       [](workloads::TxnBankConfig* c) { c->txn.primary_first = false; },
       "a crash between the two orders leaves intents with no record to resolve them against"},
      {"uncertain reads never restart", Expectation::kMustDetect, txn::Level::kSnapshot,
       [](workloads::TxnBankConfig* c) { c->txn.restart_on_uncertainty = false; },
       "an ambiguous read is treated as unambiguous instead of restarting"},
      {"parallel commit", Expectation::kControl, txn::Level::kSnapshot,
       [](workloads::TxnBankConfig* c) { c->txn.parallel_commit = true; },
       "a configuration change, not a bug; the recovery protocol must reach the same verdict"},
      {"no commit-wait", Expectation::kControl, txn::Level::kStrictSerializable,
       [](workloads::TxnBankConfig* c) { c->txn.commit_wait = false; },
       "still serializable; the loss is external consistency, which the bank's conserved "
       "total and the internal invariants here cannot see -- it needs a real-time history, "
       "which is test_isolation_matches_the_declared_level's job, not the drill's"},
      {"lost update", Expectation::kMustDetect, txn::Level::kSnapshot,
       [](workloads::TxnBankConfig* c) { c->store.range.txn.first_committer_wins = false; },
       "two transactions that both read a key and both write it both commit"},
      {"intents invisible to readers", Expectation::kMustDetect, txn::Level::kSnapshot,
       [](workloads::TxnBankConfig* c) { c->store.range.txn.reads_respect_intents = false; },
       "a reader is never blocked by a live writer and never discovers the conflict"},
      {"terminal status is not final", Expectation::kMustDetect, txn::Level::kSnapshot,
       [](workloads::TxnBankConfig* c) { c->store.range.txn.terminal_status_is_final = false; },
       "a committed transaction's record can flip to aborted under a retried push"},
      {"uncertainty never honoured", Expectation::kMustDetect, txn::Level::kSnapshot,
       [](workloads::TxnBankConfig* c) { c->store.range.txn.honour_uncertainty = false; },
       "a read silently answers instead of restarting when a version might have preceded it"},
  };

  std::cout << "\n  seeded-mutation drill (" << seeds << " seeds each)\n";
  std::cout << "  ------------------------------------------------------------------------"
               "----------------------\n";
  std::cout << "  mutation                        detected  first  invariants that fired"
               "            API?\n";
  std::cout << "  ------------------------------------------------------------------------"
               "----------------------\n";

  std::set<std::string> ever_fired;
  std::uint64_t must_detect = 0;
  std::uint64_t caught = 0;

  for (const Mutation& mutation : mutations) {
    const DrillResult result = run_mutation(mutation, seeds);
    for (const std::string& id : result.fired) ever_fired.insert(id);

    std::string names;
    for (const std::string& id : result.fired) {
      if (!names.empty()) names += " ";
      names += id;
    }
    if (names.empty()) names = "-- NOTHING FIRED --";

    std::string row = "  ";
    row += mutation.name;
    row.resize(34, ' ');
    row += std::to_string(result.detected) + "/" + std::to_string(result.seeds);
    row.resize(44, ' ');
    row += result.first_seed == 0 ? "-" : std::to_string(result.first_seed);
    row.resize(51, ' ');
    row += names;
    row.resize(86, ' ');
    row += result.api_visible == 0 ? "no"
                                   : std::to_string(result.api_visible) + "/" +
                                         std::to_string(result.detected);
    std::cout << row << "\n";

    if (mutation.expectation == Expectation::kMustDetect) {
      ++must_detect;
      if (result.detected > 0) ++caught;
      check(result.detected > 0, std::string{"the drill must catch: "} + mutation.name);
    } else {
      check(result.detected == 0, std::string{"the control must stay silent: "} + mutation.name);
    }
  }

  std::cout << "  ------------------------------------------------------------------------"
               "----------------------\n";
  std::cout << "  detected " << caught << "/" << must_detect
            << " deliberate bugs; invariants observed firing:";
  for (const std::string& id : ever_fired) std::cout << " " << id;
  std::cout << "\n";
}

// ---------------------------------------------------------------------------
// invariant health
// ---------------------------------------------------------------------------

void test_invariant_health() {
  RunOptions options;
  options.workload = base_profile(txn::Level::kSerializable, workloads::TxnWorkloadKind::kBank);
  const Summary s = run_seed(1, options);
  std::cout << "\n  invariant health on a clean seed\n";
  for (const auto& [id, stats] : s.invariants) {
    std::cout << "    " << id << "  evaluated " << stats.evaluations << " times\n";
    check(stats.evaluations > 0,
          std::string{"every armed invariant must actually be evaluated: "} + id);
  }
  std::cout << "    observer: " << s.observer.scans << " scans, " << s.observer.records_seen
            << " records seen, " << s.observer.intents_seen << " intents seen, "
            << s.observer.oracle_reservations << " oracle reservations, high water "
            << s.observer.oracle_high_water << "\n";
}

}  // namespace

int main(int argc, char** argv) {
  const std::uint64_t seeds = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 12;
  const std::uint64_t per_level = std::max<std::uint64_t>(2, seeds / 3);
  const std::uint64_t drill_seeds = std::max<std::uint64_t>(4, seeds / 3);

  std::cout << "distributed transactions under fault injection: " << seeds << " seeds\n";

  test_snapshot_isolation_shows_write_skew_and_serializable_does_not();
  test_strict_serializable_catches_a_real_time_violation();

  test_safety_under_faults(per_level);
  test_isolation_matches_the_declared_level(std::min<std::uint64_t>(per_level, 4));
  test_determinism(std::min<std::uint64_t>(seeds, 3));
  test_seeded_mutation_drill(drill_seeds);
  test_invariant_health();

  if (g_failures != 0) {
    std::cerr << "\ndistributed transactions under fault injection: " << g_failures
              << " failure(s)\n";
    return 1;
  }
  std::cout << "\ndistributed transactions under fault injection: all checks passed\n";
  return 0;
}
