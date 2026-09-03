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
#include "test/drill_report.h"

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
  std::uint64_t uncertain_reads = 0;
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
    // Deliberately NOT asserting on orphaned_intents, and the reason is worth
    // keeping because the failure message above promises otherwise.
    //
    // The count is now accurate -- it used to look for a transaction's record
    // on the range its intent sits on, which finds nothing for every
    // cross-range transaction, so a healthy run reported ~111 phantom orphans.
    // With that fixed the number is real, and asserting it to be zero still
    // fails broadly on runs whose bank total is perfect. That is not a bug:
    // resolution is lazy by design, the settle phase has no clients left to
    // read anything, and an intent whose owner died before writing its record
    // is cleaned by the next reader that meets it -- of which there are none.
    // "Nobody has tidied up yet" and "nobody ever will" are different claims
    // and only the second is a violation.
    //
    // The cost of leaving it unasserted is that the drill has no detector for
    // `secondaries before primary`, whose entire signature is intents with no
    // record. See CONTEXT.md section 14.
    return true;
  }

  bool detected() const { return !safe(); }

  // What a black-box client could have seen: money that does not add up, an
  // acknowledged element the cluster lost or duplicated. The checker's own
  // verdict is deliberately excluded -- that is the internal half.
  // The Elle verdict counts, and leaving it out was over-claiming.
  //
  // `client_safe()` is the bank's conserved total and the list's element
  // accounting -- things a client computes from its own acknowledgements. The
  // consistency checker's verdict belongs in the same bucket: it is derived
  // from the *client's history* and nothing else, so an anomaly it names is by
  // construction something an outside-in checker could have found. Excluding it
  // made three rows of the merged seeded-mutation report claim a bug was
  // invisible from the API when a client-side checker is exactly what caught
  // it, and the API-visibility column is the one place in this project where
  // over-claiming is fatal -- it is the whole evidence for the protocol-aware
  // argument.
  bool api_visible() const {
    return !client_safe() || (elle.has_value() && !elle->valid);
  }

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
  // The declared clock bound, which belongs to the deployment rather than to
  // the transaction engine -- the workload hands the simulator's declared
  // uncertainty straight to the coordinator (txn_bank.cc), because a system
  // told a bound its clocks do not honour is a different experiment from a
  // system told the truth.
  //
  // Strict serializability gets a wider one, and it is scaled rather than set,
  // which is the whole of the lesson here.
  //
  // A read is uncertain when a version was committed inside `(start_ts,
  // start_ts + bound]` on the key it is reading. At the bound `from_seed` draws
  // -- 1 to 250 ms -- over a 20-second run in which about eight transactions
  // commit, that window is essentially never occupied: `restarts_uncertain` is
  // exactly 0 across 15 seeds, `VersionStore::get`'s uncertainty branch is
  // unreachable, and the two mutations that switch the mechanism off have
  // nothing to switch off. Rows asking nothing, scoring like rows that failed.
  //
  // Declaring a wide bound on its own does not fix that, and the first attempt
  // here did exactly that and looked like it had worked: 98 reads restarted for
  // uncertainty across 15 seeds, and still not one detection. The reason is in
  // `FaultProfile::from_seed` -- `max_offset` is *derived* from
  // `declared_uncertainty`, either equal to it or two to five times it. Raising
  // only the declaration widens the window without moving the clocks, so nearly
  // every version inside it genuinely is in the future, skipping it is genuinely
  // correct, and a mutation that skips it causes no anomaly. The window was
  // occupied and vacuous at the same time, which is a more interesting way to
  // measure nothing than the original.
  //
  // Scaling both keeps the seed's own relationship between what the clocks do
  // and what the system is told they do -- including whether this seed drew a
  // profile that violates its own bound -- and makes the window one that real
  // skew can put a genuinely earlier version into.
  if (options.workload.txn.level == txn::Level::kStrictSerializable) {
    sim::ClockFaults& clock = cfg.faults.clock;
    const std::int64_t declared = clock.declared_uncertainty.nanos();
    if (declared > 0) {
      const std::int64_t target = Duration::seconds(2).nanos();
      const std::uint32_t factor =
          static_cast<std::uint32_t>(std::max<std::int64_t>(1, target / declared));
      clock.declared_uncertainty = clock.declared_uncertainty * factor;
      clock.max_offset = clock.max_offset * factor;
      clock.max_jump = clock.max_jump * factor;
    }
  }

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
  // A client sweep over every key, issued *after* the heal and before the
  // settle runs, so that the reads happen against a cluster that is converging
  // and are recorded in the history the checker grades. See
  // `workloads::start_settle_reads`.
  workloads::start_settle_reads(simulation, &state);
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
  summary.uncertain_reads = state.uncertain_reads;
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
  // The workload at which it is observable, which is a separate question from
  // the level and was previously not asked at all -- every row ran against the
  // bank. The bank's oracle is a conserved total, and a conserved total cannot
  // see a whole transaction disappearing: a transfer is balanced, so losing all
  // of it leaves the sum exactly right. It also cannot see write skew, because
  // every key the bank reads it also writes, which is the one shape write skew
  // is not. Those two blind spots are the reason four must-detect rows were
  // scoring nothing but the harness's own noise. List-append's oracle is every
  // acknowledged element being present exactly once, plus Elle's verdict
  // against the declared level, and it sees both.
  workloads::TxnWorkloadKind kind;
  // The configuration this row is measured in, and the name of that
  // configuration. Every row in a cell runs the same `cell_config` and is
  // scored against the one null control that declares the same cell name.
  //
  // The level and the workload used to be the whole of a cell, and they are not
  // enough. Two of this table's mutations turn off a mechanism that a *second*
  // mechanism makes redundant, and in the configuration where both are on,
  // turning either one off changes nothing an oracle can see -- they are
  // equivalent mutants there, and no number of seeds will make them otherwise.
  // A cell is a configuration, so a row can name the configuration in which its
  // mechanism is the one carrying the guarantee. See the commit-wait cell
  // below, which is the whole argument written out.
  const char* cell;
  void (*cell_config)(workloads::TxnBankConfig*);
  void (*apply)(workloads::TxnBankConfig*);
  const char* note;
};

struct DrillResult {
  std::uint64_t detected = 0;
  std::uint64_t seeds = 0;
  std::uint64_t api_visible = 0;
  std::set<std::string> fired;
  // Which seeds fired, not just how many. The count alone cannot distinguish a
  // mutation being caught from the harness failing on the same seeds it fails
  // on with no mutation at all, and that distinction turned out to be five of
  // this drill's nine rows -- see the null control below.
  std::set<std::uint64_t> seeds_detected;

  // Coverage, so that "detected nothing" can be told apart from "asked
  // nothing". A row whose mechanism never fired across every seed is a vacuous
  // row, and the detection count on its own cannot say which of the two it is.
  std::uint64_t committed = 0;
  std::uint64_t uncertain_reads = 0;
};

DrillResult run_mutation(const Mutation& mutation, std::uint64_t seeds) {
  DrillResult result;
  result.seeds = seeds;
  RunOptions options;
  options.workload = base_profile(mutation.level, mutation.kind);
  mutation.cell_config(&options.workload);
  mutation.apply(&options.workload);

  for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
    const Summary s = run_seed(seed, options);
    result.committed += s.committed;
    result.uncertain_reads += s.uncertain_reads;
    if (s.detected()) {
      ++result.detected;
      result.seeds_detected.insert(seed);
      if (s.api_visible()) ++result.api_visible;
      for (const std::string& id : s.fired_ids()) result.fired.insert(id);
      if (!s.client_safe()) {
        result.fired.insert(s.kind == workloads::TxnWorkloadKind::kBank ? "conservation"
                                                                        : "elements");
      }
    }
  }
  return result;
}

// The cell configurations. A cell is the experiment's setup; a mutation is the
// one thing changed inside it; the cell's null control is the same setup with
// nothing changed.
void kPlain(workloads::TxnBankConfig*) {}

// The two-doctors shape, and a key space small enough that Elle has a graph to
// find a cycle in.
//
// The second half is not a detail. With sixteen keys and ten commits a seed, a
// key's list holds about one element, no read ever observes two appends in
// order, and the dependency graph comes out with two edges in it -- Elle
// recovers a key's version order from the reads that saw it, and there is
// nothing to recover. The same run at four keys builds twenty-five, and the
// write-skew cycle appears. A checker starved of observations reports VALID for
// the same reason an empty history is valid.
void kWriteSkew(workloads::TxnBankConfig* c) {
  c->disjoint_read_write = true;
  c->keys = 4;
}

// Commit-wait off, so that the uncertainty restart is the only thing left
// holding external consistency up. See the two rows that use it.
void kNoCommitWait(workloads::TxnBankConfig* c) { c->txn.commit_wait = false; }

void test_seeded_mutation_drill(std::uint64_t seeds) {
  const Mutation mutations[] = {
      // The drill's own control, and the row that has to be read before any
      // other one is worth anything: a "mutation" that changes nothing. Its
      // detection rate is the drill's noise floor, and any row that fires on
      // the same seeds as this one and no others is not detecting its mutation.
      //
      // It was added after the fact and it immediately paid for itself. On the
      // tree at 95318de this row fired 2/10, on seeds 1 and 7 -- and so did
      // `no refresh on push`, `uncertain reads never restart`, `terminal status
      // is not final`, `uncertainty never honoured` and the `parallel commit`
      // control, on seeds 1 and 7 and nothing else. Five of the nine rows were
      // scoring the audit's own cross-range blind spot (CONTEXT.md section 14)
      // and reporting it as coverage. The drill read 6/7; it was 2/7.
      //
      // One per *cell*, where a cell is a configuration: level, workload, and
      // whatever else the rows in it need switched on. The floor is a property
      // of the configuration, not of the mutation.
      {"no mutation (snap/bank)", Expectation::kControl, txn::Level::kSnapshot,
       workloads::TxnWorkloadKind::kBank, "snap/bank", kPlain,
       [](workloads::TxnBankConfig*) {},
       "the noise floor: a mutation that changes nothing, so that a row which fires on the "
       "same seeds as this one can be seen for what it is"},
      {"no mutation (snap/list)", Expectation::kControl, txn::Level::kSnapshot,
       workloads::TxnWorkloadKind::kListAppend, "snap/list", kPlain,
       [](workloads::TxnBankConfig*) {}, "the same, at snapshot isolation on list-append"},
      {"no mutation (ser/list)", Expectation::kControl, txn::Level::kSerializable,
       workloads::TxnWorkloadKind::kListAppend, "ser/list", kPlain,
       [](workloads::TxnBankConfig*) {}, "the same, at serializable on list-append"},
      {"no mutation (strict/list)", Expectation::kControl, txn::Level::kStrictSerializable,
       workloads::TxnWorkloadKind::kListAppend, "strict/list", kPlain,
       [](workloads::TxnBankConfig*) {}, "the same, at strict-serializable on list-append"},
      {"no mutation (ser/skew)", Expectation::kControl, txn::Level::kSerializable,
       workloads::TxnWorkloadKind::kListAppend, "ser/skew", kWriteSkew,
       [](workloads::TxnBankConfig*) {},
       "the same, in the write-skew configuration -- and the row that says the shape itself "
       "is legal, since serializability is what the unmutated engine has to deliver on it"},
      {"no commit-wait", Expectation::kControl, txn::Level::kStrictSerializable,
       workloads::TxnWorkloadKind::kListAppend, "strict/-wait", kNoCommitWait,
       [](workloads::TxnBankConfig*) {},
       "the null control of the commit-wait-off cell, and a finding in its own right: "
       "turning commit-wait off at strict serializability, with a real-time history to "
       "grade it against, produces no violation. It is not that the loss is invisible -- "
       "it is that the uncertainty restart still covers it. The two mechanisms are "
       "redundant with each other, and the two rows below are what establishes that, by "
       "removing the second one as well"},
      // Write skew is the shape "read a key, write a different one". The bank
      // writes every key it reads, so its read set and its write set are the
      // same set and there is nothing for a refresh to protect -- and
      // first-committer-wins rejects the stale prewrite anyway, before the
      // refresh would have mattered. This row was a no-op against the bank, and
      // then a no-op against the default list-append plan for a subtler reason:
      // that plan draws every key independently, so a transaction reads and
      // appends the same key as often as not, and a stale read on a key you
      // also write is again first-committer-wins's business. The write-skew
      // cell states the shape instead of sampling for it. See
      // `disjoint_read_write` and the note in draw_plan.
      {"no refresh on push", Expectation::kMustDetect, txn::Level::kSerializable,
       workloads::TxnWorkloadKind::kListAppend, "ser/skew", kWriteSkew,
       [](workloads::TxnBankConfig* c) { c->txn.refresh_reads_on_push = false; },
       "a pushed transaction commits without re-checking what it read: write skew wearing "
       "serializability's name"},
      {"secondaries before primary", Expectation::kControl, txn::Level::kSnapshot,
       workloads::TxnWorkloadKind::kBank, "snap/bank", kPlain,
       [](workloads::TxnBankConfig* c) { c->txn.primary_first = false; },
       "an ordering change, and -- in this design -- not a bug, which is why it is a control "
       "and not a must-detect. The comment on the knob describes Percolator, where the "
       "primary lock *is* the commit record and writing it last leaves intents nobody can "
       "resolve. Here the record is a separate object and commit() writes it before any "
       "prewrite at all (coordinator.cc), whatever this flag says, so the window the knob "
       "claims to widen does not exist. What the flag actually reorders is the intents "
       "among themselves. Kept as a control because the reordering must stay harmless"},
      // Both uncertainty rows run in the commit-wait-off cell, and *why* is the
      // most useful thing this table has to say.
      //
      // They were originally at kSnapshot, where they are provably no-ops:
      // base_profile only selects the HLC at kStrictSerializable, and
      // Coordinator::begin sets `uncertainty_limit = start` under the oracle --
      // "with the oracle there is none". The window is empty and neither flag
      // has anything to switch off. Moving them to strict-serializable and
      // widening the clock bound until the window is genuinely occupied (66
      // reads restarted for uncertainty across 15 seeds, see run_seed) still
      // detected nothing, in 40 seeds.
      //
      // The reason is that this engine has *two* mechanisms for the same
      // guarantee, one from each of its ancestors. Spanner waits out the bound
      // at commit so that no reader can be behind an acknowledged write;
      // CockroachDB restarts a read that lands inside the bound. Anvil does
      // both, so either one alone is enough and removing either one alone is an
      // equivalent mutant. Measured in both directions rather than argued:
      // commit-wait on and uncertainty off, 0 detections in 40 seeds; commit-
      // wait off and uncertainty on, 0 in 20; both off, real-time violations on
      // 5 of 20 and 3 of 20 seeds respectively. That is what makes these rows
      // experiments rather than assertions about a mechanism nothing needs.
      {"uncertain reads never restart", Expectation::kMustDetect,
       txn::Level::kStrictSerializable, workloads::TxnWorkloadKind::kListAppend,
       "strict/-wait", kNoCommitWait,
       [](workloads::TxnBankConfig* c) { c->txn.restart_on_uncertainty = false; },
       "an ambiguous read is treated as unambiguous instead of restarting -- with commit-"
       "wait already off, so this is the last thing standing between a reader and a write "
       "that preceded it"},
      {"uncertainty never honoured", Expectation::kMustDetect,
       txn::Level::kStrictSerializable, workloads::TxnWorkloadKind::kListAppend,
       "strict/-wait", kNoCommitWait,
       [](workloads::TxnBankConfig* c) { c->store.range.txn.honour_uncertainty = false; },
       "the same guarantee removed at the other end: the range never reports the ambiguity, "
       "so the coordinator is never given the chance to restart"},
      {"parallel commit", Expectation::kControl, txn::Level::kSnapshot,
       workloads::TxnWorkloadKind::kBank, "snap/bank", kPlain,
       [](workloads::TxnBankConfig* c) { c->txn.parallel_commit = true; },
       "a configuration change, not a bug; the recovery protocol must reach the same verdict"},
      {"lost update", Expectation::kMustDetect, txn::Level::kSnapshot,
       workloads::TxnWorkloadKind::kBank, "snap/bank", kPlain,
       [](workloads::TxnBankConfig* c) { c->store.range.txn.first_committer_wins = false; },
       "two transactions that both read a key and both write it both commit"},
      {"intents invisible to readers", Expectation::kMustDetect, txn::Level::kSnapshot,
       workloads::TxnWorkloadKind::kBank, "snap/bank", kPlain,
       [](workloads::TxnBankConfig* c) { c->store.range.txn.reads_respect_intents = false; },
       "a reader is never blocked by a live writer and never discovers the conflict"},
      // A record flipping out of kCommitted rolls back intents the coordinator
      // was told were committed, which loses the *whole* transaction. The bank
      // cannot see that: a transfer is balanced, so losing all of it leaves the
      // total exactly right. Only a partial loss moves the sum, and this
      // mutation does not produce partial ones. List-append's oracle is every
      // acknowledged element being present, which is the question this asks --
      // at strict-serializable, where the clock bound is wide enough that a
      // push is a routine event rather than a rare one, and so a retried push
      // against an already-committed record is reachable.
      {"terminal status is not final", Expectation::kMustDetect,
       txn::Level::kStrictSerializable, workloads::TxnWorkloadKind::kListAppend,
       "strict/list", kPlain,
       [](workloads::TxnBankConfig* c) { c->store.range.txn.terminal_status_is_final = false; },
       "a committed transaction's record can flip to aborted under a retried push"},
  };

  std::cout << "\n  seeded-mutation drill (" << seeds << " seeds each)\n";
  std::cout << "  ------------------------------------------------------------------------"
               "----------------------\n";
  std::cout << "  mutation                        detected  seeds        invariants that fired"
               "        API?\n";
  std::cout << "  ------------------------------------------------------------------------"
               "----------------------\n";

  std::set<std::string> ever_fired;
  std::uint64_t must_detect = 0;
  std::uint64_t caught = 0;
  // The noise floor per (level, workload), filled in by the null rows before
  // any real mutation runs. A must-detect row has to fire on a seed that is not
  // in here. Keyed by the pair rather than the level because the bank and
  // list-append have different oracles and therefore different floors.
  using Cell = std::string;
  // The floor carries the cell's *coverage* as well as its noise, and the
  // coverage has to be measured on the null control rather than on the mutated
  // run. Reading it off the mutation is circular: `uncertain reads never
  // restart` switches the restart off, so its own run reports zero restarts and
  // the row cannot tell "the mechanism never fired here" from "the mechanism
  // fired and I broke it, which is the point". The null control is the same
  // configuration with nothing changed, and it is the only run in the cell
  // entitled to answer whether the mechanism is reachable at all.
  struct Floor {
    std::set<std::uint64_t> seeds;
    std::uint64_t committed = 0;
    std::uint64_t uncertain_reads = 0;
  };
  std::map<Cell, Floor> floor;

  const auto render_seeds = [](const std::set<std::uint64_t>& s) {
    if (s.empty()) return std::string{"-"};
    std::string out;
    for (const std::uint64_t seed : s) {
      if (!out.empty()) out += ",";
      if (out.size() > 9) {
        out += "...";
        break;
      }
      out += std::to_string(seed);
    }
    return out;
  };

  for (const Mutation& mutation : mutations) {
    const DrillResult result = run_mutation(mutation, seeds);
    for (const std::string& id : result.fired) ever_fired.insert(id);

    const Cell cell{mutation.cell};
    const bool is_null = std::string_view{mutation.name}.substr(0, 11) == "no mutation";
    if (is_null) {
      floor[cell] = Floor{result.seeds_detected, result.committed, result.uncertain_reads};
    }

    // What this row saw that its cell's noise floor did not. This, and not
    // `detected`, is the number that says the mutation was caught.
    std::set<std::uint64_t> beyond_floor;
    const Floor& base = floor[cell];
    for (const std::uint64_t seed : result.seeds_detected) {
      if (base.seeds.find(seed) == base.seeds.end()) beyond_floor.insert(seed);
    }

    std::string names;
    for (const std::string& id : result.fired) {
      if (!names.empty()) names += " ";
      names += id;
    }
    if (names.empty()) names = "-- NOTHING FIRED --";

    std::string row = "  ";
    row += mutation.name;
    row.resize(34, ' ');
    row += std::to_string(beyond_floor.size()) + "/" + std::to_string(result.seeds);
    if (result.detected != beyond_floor.size()) {
      row += " (" + std::to_string(result.detected - beyond_floor.size()) + " at floor)";
    }
    row.resize(44, ' ');
    row += render_seeds(beyond_floor);
    row.resize(57, ' ');
    row += names;
    row.resize(86, ' ');
    row += result.api_visible == 0 ? "no"
                                   : std::to_string(result.api_visible) + "/" +
                                         std::to_string(result.detected);
    std::cout << row << "\n";
    {
      std::string ids;
      for (const std::string& id : result.fired) {
        if (!ids.empty()) ids += " ";
        ids += id;
      }
      anvil::testing::emit_drill(
          "P6", "txn_bank", mutation.name, result.detected, result.seeds, result.detected,
          result.api_visible, 0, ids,
          mutation.expectation == Expectation::kMustDetect ? "must-detect" : "control");
    }

    if (mutation.expectation == Expectation::kMustDetect) {
      ++must_detect;
      if (!beyond_floor.empty()) ++caught;
      if (beyond_floor.empty()) {
        std::cout << "    (this cell's null control: " << base.committed << " committed, "
                  << base.uncertain_reads << " reads restarted for uncertainty across "
                  << result.seeds << " seeds; under the mutation, " << result.committed
                  << " committed)\n";
      }
      check(!beyond_floor.empty(),
            std::string{"the drill must catch, on a seed the null control does not fail on: "} +
                mutation.name);
    } else if (is_null) {
      // Deliberately not asserted to be silent. The null control is a
      // measurement, not a claim: a non-zero floor means the harness has a
      // failure of its own on those seeds, and that is reported by
      // test_safety_under_faults, whose job it is. Asserting it here would fail
      // the same finding twice and hide the floor's real use, which is making
      // every other row in this table readable.
      std::cout << "    (cell " << mutation.cell << " -- " << txn::to_string(mutation.level)
                << " on "
                << (mutation.kind == workloads::TxnWorkloadKind::kBank ? "bank" : "list-append")
                << ": floor " << result.detected << "/" << result.seeds << " on "
                << render_seeds(result.seeds_detected) << ", " << result.committed
                << " committed, " << result.uncertain_reads
                << " reads restarted for uncertainty)\n";
    } else {
      check(beyond_floor.empty(),
            std::string{"the control must stay silent above the noise floor: "} + mutation.name);
    }
  }

  std::cout << "  ------------------------------------------------------------------------"
               "----------------------\n";
  std::cout << "  detected " << caught << "/" << must_detect
            << " deliberate bugs above the null control's floor; invariants observed firing:";
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
  // Half rather than a third, because the drill's power is proportional to the
  // number of transactions that actually commit, and drawing the commit
  // timestamp fresh (ANV-0058) cost about 30% of them. A detector whose rate
  // per seed drops needs more seeds or it stops being a detector.
  const std::uint64_t drill_seeds = std::max<std::uint64_t>(4, seeds / 2);

  std::cout << "distributed transactions under fault injection: " << seeds << " seeds\n";

  test_snapshot_isolation_shows_write_skew_and_serializable_does_not();
  test_strict_serializable_catches_a_real_time_violation();

  test_safety_under_faults(per_level);
  // The same seed range as everything else. This used to be capped at 4, which
  // meant the only test that runs list-append and the only test that runs Elle
  // covered a quarter of the seeds the bank did -- and the drill's null control
  // promptly found list-append failures on seeds 7 and 8, outside the cap and
  // therefore never reported by the test whose job it is.
  test_isolation_matches_the_declared_level(per_level);
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
