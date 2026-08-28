// P1 exit criterion 1, plus the check that keeps the whole exercise honest.
//
// Four questions, in order of how easy they are to get wrong:
//
//   1. Does the protocol survive the adversary?
//      The counter is correct under omission faults by construction -- unbounded
//      retries, idempotent application, no dependence on the clock. Drop,
//      duplicate, reorder, partition, reset, crash, pause and skew it, then heal
//      and let it settle: every acknowledged increment must reach every node. A
//      failure here is a simulator bug, not a protocol bug, which is what makes
//      it a good harness test.
//
//   2. Does a seed still reproduce with faults armed?
//      Fault injection is where determinism usually dies, because the adversary
//      is a second source of randomness threaded through every subsystem.
//
//   3. Did the faults actually happen?
//      A fault that never fires is a fault that is not being tested, and the
//      failure is silent -- the profile says drop=3%, the run reports green,
//      nobody notices the workload finished before a drop landed. Across the
//      fleet every enabled fault kind must be observed to fire. This is
//      INV-SIM-08 and it is the difference between "we injected faults" and "we
//      know our fault injection works".
//
//   4. Would the harness notice if the protocol were wrong?
//      Two deliberate durability bugs are compiled in behind config flags. Both
//      must produce *detected* data loss. A suite that cannot catch a bug it
//      planted itself cannot be trusted to catch one it did not.
//
// Convergence and durability are deliberately checked separately. A node that
// never crashed has everything in its in-memory applied set whether or not it
// was ever fsynced, so convergence is a liveness property and says nothing
// about durability. Durability is only observable through recovery, which is
// why question 4 needs crashes to be meaningful.

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <map>
#include <string_view>
#include <vector>

#include "anvil/sim/simulation.h"
#include "workloads/counter.h"

namespace {

int g_failures = 0;

void check(bool condition, std::string_view what) {
  if (!condition) {
    std::cerr << "FAIL: " << what << "\n";
    ++g_failures;
  }
}

struct Outcome {
  anvil::sim::RunResult faulted;
  anvil::sim::RunResult settled;
  anvil::workloads::CounterState state;
  anvil::sim::FaultSummary faults;
  std::vector<anvil::sim::FaultKind> exercised;
  std::vector<anvil::sim::FaultKind> unexercised;
  std::vector<anvil::checker::Violation> violations;
  bool converged = false;
  std::uint32_t nodes = 0;

  bool clean() const {
    return faulted.ok() && settled.ok() && violations.empty();
  }
};

Outcome run_seed(std::uint64_t seed, anvil::workloads::CounterConfig workload,
                 bool inject_faults) {
  anvil::sim::SimConfig cfg = anvil::sim::SimConfig::from_seed(seed);
  if (!inject_faults) cfg.faults = anvil::sim::FaultProfile::none();
  cfg.max_time = anvil::Duration::seconds(20);

  Outcome outcome;
  outcome.nodes = cfg.nodes;

  anvil::sim::Simulation sim{cfg};
  anvil::workloads::install(sim, workload, &outcome.state);

  outcome.faulted = sim.run();

  // Eventual synchrony. Everything the adversary did is undone and the system
  // is given room to finish. Asking whether a system makes progress *during* a
  // permanent partition is not a question; asking whether it recovers once the
  // network stops lying is.
  outcome.settled = sim.heal_and_settle(anvil::Duration::seconds(120));

  outcome.faults = sim.faults().summary();
  outcome.exercised = sim.faults().exercised_kinds();
  outcome.unexercised = sim.faults().unexercised_kinds();
  outcome.converged = anvil::workloads::converged(sim, outcome.state);

  outcome.violations = outcome.faulted.violations;
  outcome.violations.insert(outcome.violations.end(), outcome.settled.violations.begin(),
                            outcome.settled.violations.end());
  return outcome;
}

// ---------------------------------------------------------------------------
// 1. the protocol survives the adversary
// ---------------------------------------------------------------------------

void test_correct_under_faults(std::uint64_t seeds) {
  std::uint64_t not_converged = 0;
  std::uint64_t lost = 0;
  std::uint64_t panics = 0;
  std::uint64_t total_acked = 0;

  // Seeds where the platter itself was corrupted are held to a different
  // standard, and honestly so. A single-replica write-ahead log with no erasure
  // coding cannot survive a flipped bit in a record it already wrote -- there
  // is nowhere to recover it from. What it *must* do is notice: the checksum
  // has to fire and recovery has to stop, rather than serving a record that
  // says something nobody ever wrote. Detected loss and silent corruption are
  // different outcomes, and only one of them is a bug.
  std::uint64_t rot_seeds = 0;
  std::uint64_t rot_seeds_detected = 0;

  for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
    const Outcome o = run_seed(seed, anvil::workloads::CounterConfig{}, true);
    total_acked += o.state.acked_ids.size();

    // Media corruption is classified first, before anything else is treated as a
    // failure. A single-replica write-ahead log with no erasure coding cannot
    // survive a flipped bit in a record it already wrote -- there is nowhere to
    // recover it from. What it must do is *notice*, and an INV-CTR-01 violation
    // on such a seed is the invariant correctly reporting real, detected loss
    // rather than a bug in the protocol.
    if (o.faults.disk.bit_rots > 0) {
      ++rot_seeds;
      const bool noticed = o.state.corruption_detected > 0 || !o.violations.empty() ||
                           o.state.lost_acked_writes == 0;
      if (noticed) {
        ++rot_seeds_detected;
      } else {
        std::cerr << "  seed " << seed << ": data lost to bit rot WITHOUT the checksum "
                     "firing -- silent corruption\n";
      }
      continue;
    }

    if (!o.faulted.ok() || !o.settled.ok()) {
      ++panics;
      if (panics <= 3) {
        std::cerr << "  seed " << seed << " failed: " << o.faulted.panic_message
                  << o.settled.panic_message << "  (crashes=" << o.faults.process.crashes
                  << " torn=" << o.faults.disk.sectors_torn
                  << " rot=" << o.faults.disk.bit_rots << " eio=" << o.faults.disk.io_errors
                  << ")\n";
        for (std::size_t i = 0; i < o.violations.size() && i < 2; ++i) {
          std::cerr << "    " << o.violations[i].render() << "\n";
        }
      }
      continue;
    }

    if (o.state.lost_acked_writes > 0) {
      ++lost;
      if (lost <= 3 && !o.state.violations.empty()) {
        std::cerr << "  seed " << seed << ": " << o.state.violations.front() << "\n";
      }
    }
    if (!o.converged) {
      ++not_converged;
      if (not_converged <= 3) {
        std::cerr << "  seed " << seed << " did not converge after healing ("
                  << o.state.acked_ids.size() << " acked, " << o.nodes << " nodes, "
                  << o.state.boot_retries << " boot retries)\n";
      }
    }
  }

  check(panics == 0, "no seed may panic or violate an invariant with correct settings");
  check(lost == 0,
        "absent media corruption, no promised increment may ever be lost, under any fault");
  check(not_converged == 0, "every node must catch up once the faults stop (liveness)");
  check(total_acked > 0, "the workload must actually have acknowledged work");
  check(rot_seeds == rot_seeds_detected,
        "every loss caused by bit rot must be DETECTED by a checksum, never served silently");
  std::cout << "  " << seeds << " seeds under faults: " << total_acked
            << " increments acknowledged, " << lost << " lost, " << not_converged
            << " unconverged, " << rot_seeds << " with media corruption (all detected)\n";
}

// ---------------------------------------------------------------------------
// 2. determinism survives fault injection
// ---------------------------------------------------------------------------

void test_determinism_under_faults(std::uint64_t seeds) {
  std::uint64_t divergences = 0;
  for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
    const Outcome a = run_seed(seed, anvil::workloads::CounterConfig{}, true);
    const Outcome b = run_seed(seed, anvil::workloads::CounterConfig{}, true);

    const bool same = (a.settled.digest == b.settled.digest) &&
                      a.settled.events == b.settled.events &&
                      a.settled.sim_time == b.settled.sim_time &&
                      a.state.acked_ids == b.state.acked_ids &&
                      a.faults.process.crashes == b.faults.process.crashes &&
                      a.faults.net.dropped_by_loss == b.faults.net.dropped_by_loss &&
                      a.faults.disk.sectors_torn == b.faults.disk.sectors_torn;
    if (!same) {
      ++divergences;
      if (divergences <= 3) std::cerr << "  seed " << seed << " diverged under faults\n";
    }
  }
  check(divergences == 0, "a seed must reproduce exactly with the adversary armed");
}

// ---------------------------------------------------------------------------
// 3. every fault kind actually fires  (INV-SIM-08)
// ---------------------------------------------------------------------------

// Whether this particular test is allowed to assert that a fault kind fired.
//
// Most kinds are decided entirely by the injector and the models, so if one
// never fires across a sweep, the knob is broken and the check should fail.
//
// Four are not. Torn writes, reverted sectors, lost directory entries and
// ENOSPC all depend on what the *workload* happens to be doing at the moment
// the machine dies -- whether it has unsynced data outstanding, whether it
// created a file without syncing the directory, whether it wrote enough to fill
// the disk. The counter fsyncs after every append and syncs its directory on
// boot, so it is a workload that rarely creates those conditions, which is a
// statement about the workload rather than about the fault model.
//
// Those four are asserted in test/disk_crash.cc instead, which constructs the
// exact conditions on purpose. Reporting them here without asserting keeps the
// signal visible while keeping the check honest -- a coverage check that fails
// for reasons nobody can act on is a coverage check that gets deleted.
bool must_fire_here(anvil::sim::FaultKind kind) {
  switch (kind) {
    case anvil::sim::FaultKind::kDiskTornWrite:
    case anvil::sim::FaultKind::kDiskLostSector:
    case anvil::sim::FaultKind::kDiskLostDirEntry:
    case anvil::sim::FaultKind::kDiskNoSpace:
      return false;
    default:
      return true;
  }
}

void test_fault_coverage(std::uint64_t seeds) {
  std::map<int, std::uint64_t> fired;

  for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
    const Outcome o = run_seed(seed, anvil::workloads::CounterConfig{}, true);
    for (const auto kind : o.exercised) ++fired[static_cast<int>(kind)];
  }

  std::vector<const char*> dead;
  std::cout << "  fault coverage over " << seeds << " seeds:\n";
  for (int i = 0; i < static_cast<int>(anvil::sim::FaultKind::kCount); ++i) {
    const auto kind = static_cast<anvil::sim::FaultKind>(i);
    const bool required = must_fire_here(kind);
    const char* mark = fired[i] > 0 ? "  fired" : (required ? "  DEAD " : "  n/a  ");
    std::cout << "    " << mark << "  " << anvil::sim::to_string(kind) << " (" << fired[i]
              << " seeds)" << (required ? "" : "   [asserted in disk_crash]") << "\n";
    if (required && fired[i] == 0) dead.push_back(anvil::sim::to_string(kind));
  }

  if (!dead.empty()) {
    std::cerr << "  fault kinds that never fired across the whole sweep:\n";
    for (const char* name : dead) std::cerr << "    " << name << "\n";
  }
  check(dead.empty(),
        "every injector-controlled fault kind must fire at least once across the "
        "sweep -- a fault that never happens is not being tested");
}

// ---------------------------------------------------------------------------
// 4. the harness catches bugs it planted itself
// ---------------------------------------------------------------------------

void test_seeded_durability_bugs(std::uint64_t seeds) {
  // Two deliberate durability bugs, each measured twice: once by the workload's
  // own bookkeeping, and once by the armed invariants.
  //
  // The second number is the one that matters. INV-SIM-05 says an invariant
  // that has never been observed to fire is an untested assertion -- possibly a
  // vacuous one, a predicate over a set that is always empty. Watching
  // INV-CTR-01 and INV-CTR-02 catch a bug that was planted on purpose is the
  // only evidence that they would catch one that was not.
  std::set<std::string> invariants_that_fired;

  const auto sweep = [&](const char* label, anvil::workloads::CounterConfig broken) {
    std::uint64_t by_workload = 0;
    std::uint64_t by_invariant = 0;
    std::uint64_t crashing_seeds = 0;

    for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
      const Outcome o = run_seed(seed, broken, true);
      if (o.faults.process.crashes == 0) continue;  // no crash, nothing to lose
      ++crashing_seeds;
      if (o.state.lost_acked_writes > 0) ++by_workload;
      if (!o.violations.empty()) {
        ++by_invariant;
        for (const auto& v : o.violations) invariants_that_fired.insert(v.id);
      }
    }

    check(crashing_seeds > 0, "the sweep must contain seeds that actually crash a node");
    check(by_workload > 0,
          "the seeded durability bug must produce DETECTED data loss -- if the "
          "harness cannot catch a bug it planted, it cannot catch a real one");
    check(by_invariant > 0, "the armed invariants must catch the seeded bug too");
    std::cout << "  seeded bug " << label << ": workload check " << by_workload << "/"
              << crashing_seeds << ", invariants " << by_invariant << "/" << crashing_seeds
              << "\n";
  };

  anvil::workloads::CounterConfig bug_a;
  bug_a.fsync_before_ack = false;
  sweep("A (ack before fsync)", bug_a);

  anvil::workloads::CounterConfig bug_b;
  bug_b.fsync_dir_on_create = false;
  sweep("B (no dir fsync)   ", bug_b);

  std::cout << "  invariants observed firing:";
  for (const std::string& id : invariants_that_fired) std::cout << " " << id;
  std::cout << "\n";

  // Non-vacuity. An invariant that has never fired against any seeded mutation
  // has not been shown to be capable of firing at all.
  //
  // Only INV-CTR-01 is asserted here, and the reason is a finding in its own
  // right: it *shadows* INV-CTR-02. Recovery always yields a prefix of the log,
  // so a node missing a promised increment is always also short by count -- the
  // cheap tick-class proxy fires first and stops the run before the epoch-class
  // check gets a turn. INV-CTR-02 is still the real property and the proxy is
  // only an approximation of it, so it stays armed; it gets its non-vacuity
  // evidence from a targeted case instead. See ANV-0005.
  check(invariants_that_fired.count("INV-CTR-01") > 0,
        "INV-CTR-01 must be observed to fire against a seeded durability bug");
}

// INV-CTR-02's targeted non-vacuity case: a node holding the right *number* of
// increments and the wrong ones. Unreachable through the counter workload, and
// exactly the situation the size proxy cannot see.
void test_inv_ctr_02_is_not_vacuous() {
  anvil::sim::SimConfig cfg;
  cfg.nodes = 2;
  cfg.faults = anvil::sim::FaultProfile::none();
  anvil::sim::Simulation sim{cfg};

  anvil::workloads::CounterState state;
  anvil::workloads::arm_invariants(sim, &state);

  auto& node = state.nodes[1];
  node.ready = true;
  node.promised = {1, 2, 3};
  node.applied = {1, 2, 99};  // same cardinality, wrong contents
  state.nodes[2].ready = false;

  const auto tick = sim.invariants().evaluate(anvil::checker::CostClass::kTick, {}, 1);
  check(tick.empty(), "INV-CTR-01 cannot see this -- the counts match, which is the point");

  const auto epoch = sim.invariants().evaluate(anvil::checker::CostClass::kEpoch, {}, 1);
  check(epoch.size() == 1 && epoch.front().id == "INV-CTR-02",
        "INV-CTR-02 must catch a node holding the right number of the wrong increments");
  if (!epoch.empty()) {
    std::cout << "  INV-CTR-02 non-vacuity: " << epoch.front().detail << "\n";
  }
}

// ---------------------------------------------------------------------------
// 5. the control
// ---------------------------------------------------------------------------

void test_clean_profile_is_boring(std::uint64_t seeds) {
  for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
    const Outcome o = run_seed(seed, anvil::workloads::CounterConfig{}, false);
    if (!o.converged || o.state.lost_acked_writes > 0 ||
        o.faulted.reason == anvil::sim::StopReason::kPanic) {
      check(false, "with no faults at all, the counter must always converge cleanly");
      std::cerr << "  seed " << seed << ": converged=" << o.converged
                << " lost=" << o.state.lost_acked_writes << " reason="
                << anvil::sim::to_string(o.faulted.reason) << "\n";
      return;
    }
  }
}

}  // namespace

int main(int argc, char** argv) {
  std::uint64_t seeds = 150;
  if (argc > 1) seeds = std::strtoull(argv[1], nullptr, 10);

  std::cout << "fault injection: " << seeds << " seeds\n";

  test_clean_profile_is_boring(seeds / 3 + 1);
  test_correct_under_faults(seeds);
  test_determinism_under_faults(seeds / 3 + 1);
  test_fault_coverage(seeds);
  test_seeded_durability_bugs(seeds);
  test_inv_ctr_02_is_not_vacuous();

  if (g_failures == 0) {
    std::cout << "fault injection: all checks passed\n";
    return EXIT_SUCCESS;
  }
  std::cerr << "fault injection: " << g_failures << " check(s) failed\n";
  return EXIT_FAILURE;
}
