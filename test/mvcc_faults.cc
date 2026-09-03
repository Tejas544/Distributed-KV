// P4 exit criteria: versions, safepoints and locks under an adversary.
//
// Four questions:
//
//   1. Does aggressive GC ever eat a version somebody can still see?
//      Long readers hold snapshots open across many collector passes while
//      writers churn versions underneath them. Every INV-MVCC-* armed, faults
//      from the seed. This is the phase's whole risk in one sentence, and the
//      roadmap says so: "GC safepoints are the classic silent-corruption
//      source. Over-invest here."
//
//   2. Does wound-wait actually resolve the deadlocks it is given?
//      Pairs of transactions take the same two keys in opposite orders,
//      continuously. Every cycle must be resolved, no wait-for cycle may ever
//      exist, and no younger transaction may wound an older one.
//
//   3. Would any of this notice if MVCC were wrong?
//      Four deliberate bugs behind named flags. Each must be detected, and the
//      API-visibility column recorded -- the same table P3 produces, because it
//      is the same claim.
//
//   4. Is the isolation level the one being claimed?
//      A list-append workload's history goes through the Elle-style checker
//      twice: it must be valid at snapshot isolation, and it must be *allowed*
//      to show write skew at serializable. A checker that reports SI histories
//      as serializable is over-reporting; one that rejects them at SI is
//      broken. Both directions are asserted.

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "anvil/checker/elle.h"
#include "anvil/checker/history.h"
#include "anvil/checker/mvcc_invariants.h"
#include "anvil/sim/simulation.h"
#include "workloads/mvcc_txn.h"
#include "test/drill_report.h"

namespace {

using anvil::Duration;
using anvil::NodeId;
using anvil::Status;
using anvil::Timestamp;
using anvil::TxnId;
namespace checker = anvil::checker;
namespace mvcc = anvil::mvcc;
namespace sim = anvil::sim;
namespace workloads = anvil::workloads;

int g_failures = 0;

void check(bool condition, std::string_view what) {
  if (!condition) {
    std::cerr << "FAIL: " << what << "\n";
    ++g_failures;
  }
}

// ---------------------------------------------------------------------------
// running one seed
// ---------------------------------------------------------------------------

struct Summary {
  std::uint64_t seed = 0;
  sim::StopReason reason = sim::StopReason::kQuiesced;
  std::string panic_message;
  std::vector<checker::Violation> violations;

  std::uint64_t committed = 0;
  std::uint64_t aborted = 0;
  std::uint64_t wounded = 0;
  std::uint64_t write_conflicts = 0;
  std::uint64_t gc_passes = 0;
  std::uint64_t collected = 0;
  std::uint64_t long_reader_checks = 0;
  std::uint64_t reads_checked = 0;
  std::uint64_t versions_audited = 0;

  std::uint64_t blocked_reads = 0;
  std::uint64_t lost_versions = 0;
  std::uint64_t order_violations = 0;
  std::uint64_t orphan_intents = 0;
  std::vector<std::string> workload_violations;
  bool final_audit_clean = false;

  sim::FaultSummary faults;
  Timestamp sim_time;
  std::uint64_t digest_low = 0;

  bool safe() const {
    return violations.empty() && lost_versions == 0 && order_violations == 0 &&
           orphan_intents == 0 && reason != sim::StopReason::kPanic;
  }
  bool detected() const { return !safe(); }
  bool api_visible() const { return lost_versions > 0; }
};

struct RunOptions {
  bool inject_faults = true;
  // A backstop, not the expected length: the workload stops the clock when its
  // writers are done, which is normally a second or two in.
  Duration max_time = Duration::seconds(10);
  workloads::MvccWorkloadConfig workload;
};

Summary run_seed(std::uint64_t seed, RunOptions options) {
  sim::SimConfig cfg = sim::SimConfig::from_seed(seed);
  if (!options.inject_faults) cfg.faults = sim::FaultProfile::none();
  cfg.nodes = 1;  // P4 is single-node: versions and locks, no consensus underneath
  cfg.max_time = options.max_time;

  // No process crashes, and this is a scope statement rather than a convenience.
  //
  // The single node here has no redundancy: kill it and there is nothing left to
  // check, because every property P4 claims -- snapshot reads, first-committer-
  // wins, a safepoint that respects live readers -- is about the *live*
  // transaction state, which a crash destroys by definition. Recovering intents
  // and re-deriving transaction outcomes after a crash is a real problem and it
  // is P5's, where the durability work lives. Leaving crashes on here would not
  // test that; it would test a workload reading from a dead node, which is what
  // it did before this line existed, and it manufactured findings that said
  // nothing about MVCC.
  //
  // Everything else the adversary does stays on: EIO, torn writes, latency, bit
  // rot, and scheduling noise.
  cfg.faults.process.crash_per_second = sim::Chance::never();
  cfg.faults.process.pause_per_second = sim::Chance::never();

  Summary summary;
  summary.seed = seed;

  sim::Simulation simulation{cfg};
  checker::MvccObserver observer;
  workloads::MvccWorkloadState state;
  workloads::install(simulation, options.workload, &state, &observer);

  const sim::RunResult result = simulation.run();
  summary.reason = result.reason;
  summary.panic_message = result.panic_message;
  summary.violations = result.violations;
  summary.sim_time = result.sim_time;
  summary.digest_low = result.digest.low();

  // The final audit runs after the workload stops, when nothing is in flight
  // and every question has a definite answer.
  state.done = true;
  simulation.scheduler().clear_stop();  // the workload stopped the clock; the audit needs it

  // The adversary goes home before the audit. A god's-eye pass that is itself
  // racing new EIOs cannot distinguish "the engine lost this version" from "the
  // audit's own read failed", and the whole point of the final audit is to give
  // a definite answer. It is also what makes it affordable: fault injection
  // costs more per event as the run accumulates files.
  simulation.disk().stop_injecting();
  simulation.faults().disarm();
  if (state.store != nullptr) {
    anvil::Runtime& rt = simulation.node(NodeId{1});
    auto* clean = &summary.final_audit_clean;
    rt.spawn([](workloads::MvccWorkloadState* st, bool* out) -> anvil::Task<void> {
      *out = co_await workloads::audit_everything(st);
    }(&state, clean));
    simulation.run_more(Duration::seconds(5));
  }

  summary.committed = state.txns_committed;
  summary.aborted = state.txns_aborted;
  summary.wounded = state.wounded;
  summary.write_conflicts = state.write_conflicts;
  summary.gc_passes = state.gc_passes;
  summary.collected = state.versions_collected;
  summary.long_reader_checks = state.long_reader_checks;
  summary.reads_checked = state.reads_checked;
  summary.versions_audited = state.versions_audited;
  if (state.txns != nullptr) summary.blocked_reads = state.txns->stats().blocked_reads;
  summary.lost_versions = state.lost_versions;
  summary.order_violations = state.order_violations;
  summary.orphan_intents = state.orphan_intents;
  summary.workload_violations = state.violations;
  summary.faults = simulation.faults().summary();
  return summary;
}

void report(const Summary& summary, const char* label) {
  std::cerr << "  seed " << summary.seed << " (" << label
            << "): committed=" << summary.committed << " aborted=" << summary.aborted
            << " gc=" << summary.gc_passes << " collected=" << summary.collected
            << " lost=" << summary.lost_versions << " eio=" << summary.faults.disk.io_errors
            << "\n";
  if (!summary.panic_message.empty()) {
    std::cerr << "    panic: " << summary.panic_message << "\n";
  }
  for (std::size_t i = 0; i < summary.violations.size() && i < 2; ++i) {
    std::cerr << "    " << summary.violations[i].render() << "\n";
  }
  for (std::size_t i = 0; i < summary.workload_violations.size() && i < 3; ++i) {
    std::cerr << "    " << summary.workload_violations[i] << "\n";
  }
}

// ---------------------------------------------------------------------------
// 1. the collector never takes a version somebody can still see
// ---------------------------------------------------------------------------

void test_gc_never_eats_a_live_version(std::uint64_t seeds) {
  std::uint64_t unsafe = 0;
  std::uint64_t total_committed = 0;
  std::uint64_t total_collected = 0;
  std::uint64_t total_reader_checks = 0;
  std::uint64_t total_audited = 0;

  for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
    const Summary summary = run_seed(seed, RunOptions{});
    total_committed += summary.committed;
    total_collected += summary.collected;
    total_reader_checks += summary.long_reader_checks;
    total_audited += summary.versions_audited;

    if (!summary.safe() || !summary.final_audit_clean) {
      ++unsafe;
      if (unsafe <= 4) report(summary, "gc");
    }
  }

  check(unsafe == 0,
        "no seed may lose a version a live snapshot can still resolve, violate the "
        "version order, or orphan an intent");
  check(total_committed > 0, "the sweep must actually commit transactions");
  check(total_collected > 0,
        "and the collector must actually collect -- a GC that never runs proves nothing");
  check(total_reader_checks > 0, "long readers must actually re-read across GC passes");
  std::cout << "  gc under faults: " << seeds << " seeds, " << total_committed
            << " transactions committed, " << total_collected << " versions collected, "
            << total_reader_checks << " long-reader re-reads, " << total_audited
            << " versions audited\n";
}

// ---------------------------------------------------------------------------
// 2. wound-wait resolves every deadlock it is given
// ---------------------------------------------------------------------------

void test_deadlocks_are_resolved(std::uint64_t seeds) {
  std::uint64_t total_wounded = 0;
  std::uint64_t cycles = 0;
  std::uint64_t stuck = 0;

  for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
    RunOptions options;
    options.workload.deadlockers = 3;
    options.workload.keys = 4;  // more contention, so the window actually opens
    const Summary summary = run_seed(seed, options);
    total_wounded += summary.wounded;

    for (const auto& violation : summary.violations) {
      if (violation.id == "INV-MVCC-06") ++cycles;
      if (violation.id == "INV-MVCC-07") ++cycles;
    }
    // Progress is the liveness half: a cluster of transactions that all wound
    // each other forever commits nothing.
    if (summary.committed == 0) ++stuck;
  }

  check(cycles == 0, "the wait-for graph must never contain a cycle, and no younger "
                     "transaction may ever be waited on by an older one");
  check(stuck == 0, "wound-wait must make progress, not just avoid deadlock");
  check(total_wounded > 0,
        "the deadlock workload must actually reach the wound-wait path -- if nothing is "
        "ever wounded, INV-MVCC-07 is a predicate over an empty set");
  std::cout << "  deadlocks: " << seeds << " seeds, " << total_wounded
            << " transactions wounded, 0 cycles, 0 stalls\n";
}

// ---------------------------------------------------------------------------
// 3. determinism
// ---------------------------------------------------------------------------

void test_determinism(std::uint64_t seeds) {
  std::uint64_t divergences = 0;
  for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
    const Summary a = run_seed(seed, RunOptions{});
    const Summary b = run_seed(seed, RunOptions{});
    const bool same = a.digest_low == b.digest_low && a.committed == b.committed &&
                      a.aborted == b.aborted && a.collected == b.collected &&
                      a.sim_time == b.sim_time;
    if (!same) {
      ++divergences;
      if (divergences <= 3) std::cerr << "  seed " << seed << " diverged\n";
    }
  }
  check(divergences == 0, "a seed must reproduce exactly, versions and locks and all");
}

// ---------------------------------------------------------------------------
// 4. the seeded-mutation drill
// ---------------------------------------------------------------------------

struct Mutation {
  enum class Kind {
    kMustDetect,  // a real defect; the suite has to notice
    kControl,     // changes nothing that matters; must NOT be reported
    kEquivalent,  // disables a real mechanism whose absence this configuration
                  // cannot observe -- see the note on each one
  };

  const char* name;
  const char* breaks;
  void (*apply)(workloads::MvccWorkloadConfig*);
  Kind kind = Kind::kMustDetect;
};

const Mutation kMutations[] = {
    {"gc drops the boundary version", "the version a safepoint reader resolves to",
     [](workloads::MvccWorkloadConfig* c) { c->mvcc.gc_keeps_safepoint_version = false; }},
    {"gc ignores the safepoint", "the safepoint being the oldest live reader",
     [](workloads::MvccWorkloadConfig* c) { c->gc_uses_real_safepoint = false; }},
    // Equivalent, and the argument is worth stating because it took a failing
    // drill row to work out.
    //
    // Blocking a read on somebody else's intent protects a reader from missing a
    // version that commits at or below its snapshot. On this configuration that
    // cannot happen: every timestamp comes from one monotonic source, so a
    // transaction still live when a reader takes snapshot S must commit at some
    // C > S -- its commit timestamp is drawn later than S was. The version is
    // above the snapshot either way, and the reader's answer is the same whether
    // it blocks and retries or reads straight past the intent.
    //
    // It stops being equivalent as soon as timestamps stop being totally ordered
    // by one clock: with an uncertainty interval, or in P6 with skew across
    // nodes, C can land below S in real time and the reader does have to wait.
    // This row is expected to move to kMustDetect there. Until then the suite
    // asserts the mechanism is genuinely disabled rather than claiming an
    // observation it cannot make.
    {"reads ignore intents", "nothing observable at SI with one monotonic clock",
     [](workloads::MvccWorkloadConfig* c) { c->mvcc.reads_respect_intents = false; },
     Mutation::Kind::kEquivalent},
    {"more contention, fewer keys", "nothing -- the control",
     [](workloads::MvccWorkloadConfig* c) { c->keys = 3; }, Mutation::Kind::kControl},
};

void test_seeded_mutation_drill(std::uint64_t seeds) {
  std::cout << "\n  seeded-mutation drill (" << seeds << " seeds each)\n";
  std::cout << "  " << std::string(92, '-') << "\n";
  std::cout << "  mutation                        detected   invariants that fired"
               "               API?\n";
  std::cout << "  " << std::string(92, '-') << "\n";

  std::set<std::string> all_fired;
  for (const Mutation& mutation : kMutations) {
    const bool is_control = mutation.kind == Mutation::Kind::kControl;
    std::uint64_t detected = 0;
    std::uint64_t api_visible = 0;
    std::uint64_t blocked_reads = 0;
    std::set<std::string> fired;

    for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
      RunOptions options;
      mutation.apply(&options.workload);
      const Summary summary = run_seed(seed, options);
      blocked_reads += summary.blocked_reads;
      if (summary.detected()) {
        ++detected;
        for (const auto& violation : summary.violations) fired.insert(violation.id);
        if (summary.lost_versions > 0) fired.insert("model-audit");
      }
      if (summary.api_visible()) ++api_visible;
    }
    for (const std::string& id : fired) all_fired.insert(id);

    std::string ids;
    for (const std::string& id : fired) {
      if (!ids.empty()) ids += " ";
      ids += id;
    }
    if (ids.empty()) ids = is_control ? "--" : "-- NOTHING FIRED --";

    std::string row = std::string("  ") + mutation.name;
    row.resize(34, ' ');
    row += std::to_string(detected) + "/" + std::to_string(seeds);
    row.resize(45, ' ');
    row += ids;
    row.resize(80, ' ');
    row += api_visible > 0 ? (std::to_string(api_visible) + "/" + std::to_string(seeds))
                           : std::string("no");
    if (mutation.kind == Mutation::Kind::kEquivalent) row += "   (equivalent here)";
    std::cout << row << "\n";
    anvil::testing::emit_drill(
        "P4", "mvcc_txn", mutation.name, detected, seeds, detected, api_visible, 0,
        fired.empty() ? std::string("-") : ids,
        mutation.kind == Mutation::Kind::kMustDetect   ? "must-detect"
        : mutation.kind == Mutation::Kind::kEquivalent ? "equivalent"
                                                       : "control");

    if (mutation.kind == Mutation::Kind::kEquivalent) {
      // Two things have to hold for this classification to be honest. The
      // outcome must genuinely not change -- otherwise it is a missed defect
      // wearing a label -- and the mechanism must actually be disabled, or the
      // flag does nothing and the row proves nothing at all.
      check(detected == 0, std::string("mutation \"") + mutation.name +
                               "\" is classified equivalent, so reporting it means the "
                               "classification is wrong");
      check(blocked_reads == 0,
            "disabling intent-blocking must actually stop reads blocking, or the "
            "equivalence argument is about a flag that does nothing");
      continue;
    }

    if (is_control) {
      // The control is the honesty check on the other three. If the suite
      // reports a violation on unmutated code, every "detected" above it is
      // meaningless.
      check(detected == 0,
            "the control must NOT be detected -- a suite that fires on correct code "
            "cannot be trusted when it fires on broken code");
      continue;
    }
    check(detected > 0, std::string("mutation \"") + mutation.name +
                            "\" must be detected -- it breaks " + mutation.breaks);
  }
  std::cout << "  " << std::string(92, '-') << "\n";
  std::cout << "  invariants observed firing:";
  for (const std::string& id : all_fired) std::cout << " " << id;
  std::cout << "\n";
}

// ---------------------------------------------------------------------------
// 5. the level is the one being claimed
// ---------------------------------------------------------------------------

// Snapshot isolation permits write skew and forbids G1c. Both halves are
// asserted, because a checker that rejects a legal SI history is as broken as
// one that accepts an illegal serializable one -- and only running it at two
// levels tells them apart.
//
// The history is built by hand rather than harvested from the workload. That is
// deliberate: the question here is whether the *checker* agrees with the level
// the store implements, and a hand-built history states the scenario exactly
// instead of hoping a random run produces it.
void test_snapshot_isolation_is_the_level_claimed() {
  checker::History history;

  // Classic write skew, with the observer that makes it checkable.
  //
  // Two transactions each read the key the other is about to write, and both
  // commit. That is the anomaly. But an anomaly nobody can see is not in the
  // history: with only those two transactions there is no evidence of which
  // version of each key came first, so the checker builds no version order, no
  // anti-dependency edges, and returns "valid" at every level -- which is
  // correct and useless. A third transaction reading both keys afterwards
  // supplies the order, and only then does the cycle exist to be found. The
  // first version of this test omitted it and reported a write-skew history as
  // serializable, which is exactly the kind of vacuous pass this suite is
  // supposed to be unable to produce.
  const checker::TxnId a = history.begin(1, Timestamp{1000, 0});
  history.read(a, 2, {});          // reads y, sees nothing
  history.append(a, 1, 100);       // writes x
  history.complete(a, checker::Outcome::kCommitted, Timestamp{1100, 0});

  const checker::TxnId b = history.begin(2, Timestamp{1000, 0});
  history.read(b, 1, {});          // reads x, sees nothing
  history.append(b, 2, 200);       // writes y
  history.complete(b, checker::Outcome::kCommitted, Timestamp{1100, 0});

  const checker::TxnId observer = history.begin(3, Timestamp{2000, 0});
  history.read(observer, 1, {100});
  history.read(observer, 2, {200});
  history.complete(observer, checker::Outcome::kCommitted, Timestamp{2100, 0});

  const checker::CheckResult at_si =
      checker::check(history, checker::IsolationLevel::kSnapshotIsolation);
  check(at_si.valid, "write skew is legal at snapshot isolation: " + at_si.summary());
  check(!at_si.has(checker::Anomaly::kG1c),
        "and it is not G1c -- reporting it as one would be over-reporting");

  const checker::CheckResult at_serializable =
      checker::check(history, checker::IsolationLevel::kSerializable);
  check(!at_serializable.valid,
        "the same history must be rejected at serializable: " + at_serializable.summary());
  check(at_serializable.has(checker::Anomaly::kG2Item),
        "and classified as G2-item, not as some other anomaly");

  check(checker::forbids(checker::IsolationLevel::kSerializable, checker::Anomaly::kG2Item),
        "the level table forbids write skew at serializable");
  check(!checker::forbids(checker::IsolationLevel::kSnapshotIsolation,
                          checker::Anomaly::kG2Item),
        "and permits it at snapshot isolation -- which is what makes SI a distinct level");

  std::cout << "  isolation levels: the same write-skew history is " << at_si.summary()
            << " and " << at_serializable.summary() << "\n";
}

void test_clean_profile_is_boring(std::uint64_t seeds) {
  for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
    RunOptions options;
    options.inject_faults = false;
    const Summary summary = run_seed(seed, options);
    if (!summary.safe() || !summary.final_audit_clean || summary.committed == 0) {
      check(false, "with no faults, transactions must commit and no invariant may fire");
      report(summary, "clean");
      return;
    }
  }
  std::cout << "  clean profile: " << seeds << " seeds, no violations\n";
}

}  // namespace

int main(int argc, char** argv) {
  std::uint64_t seeds = 20;
  if (argc > 1) seeds = std::strtoull(argv[1], nullptr, 10);

  std::cout << "mvcc under fault injection: " << seeds << " seeds\n";

  test_clean_profile_is_boring(seeds / 4 + 1);
  test_gc_never_eats_a_live_version(seeds);
  test_deadlocks_are_resolved(seeds / 2 + 1);
  test_determinism(seeds / 4 + 1);
  test_snapshot_isolation_is_the_level_claimed();
  test_seeded_mutation_drill(seeds / 2 + 1);

  if (g_failures == 0) {
    std::cout << "\nmvcc under fault injection: all checks passed\n";
    return EXIT_SUCCESS;
  }
  std::cerr << "\nmvcc under fault injection: " << g_failures << " check(s) failed\n";
  return EXIT_FAILURE;
}
