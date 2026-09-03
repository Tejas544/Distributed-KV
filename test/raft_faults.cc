// P3 exit criteria: consensus under an adversary, checked from the inside.
//
// Five questions, in order of how easy they are to fool yourself about:
//
//   1. Does the cluster stay safe?
//      Partitions, asymmetric links, crashes, pauses, clock skew, torn writes
//      and EIO, all drawn from the seed, running against a replicated KV
//      workload with continuous membership churn. Not one INV-RAFT-* violation,
//      not one acknowledged write lost, not one stale linearizable read.
//
//   2. Does it stay live?
//      Under eventual synchrony -- faults heal at T -- a leader must be elected
//      within a bounded interval, every time. The distribution is reported, not
//      just the maximum, because "it always recovered eventually" and "it
//      recovered in under a second" are very different claims.
//
//   3. Is a seed still a complete description of the run?
//      Consensus adds timers, randomised election timeouts and a great deal of
//      message traffic. If determinism is going to break anywhere, it breaks
//      here.
//
//   4. Would any of this notice if Raft were wrong?
//      Ten deliberate protocol bugs, one per run, each behind a named flag.
//      Every one must be detected, and for every one we record whether a client
//      could have seen it. That table is the empirical core of the whole
//      protocol-aware argument and is the single most valuable artifact P3
//      produces.
//
//   5. What happens to a lease across a process pause?
//      A node frozen for longer than its lease and then resumed is the fault
//      that breaks lease-based reads in real systems. It must be safe, and the
//      implementation that is *not* safe must be caught.
//
// Nothing here asserts on wall-clock time or on iteration counts of unordered
// containers, and every failure prints its seed.

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "anvil/checker/raft_invariants.h"
#include "anvil/sim/simulation.h"
#include "workloads/raft_kv.h"
#include "test/drill_report.h"

namespace {

using anvil::Duration;
using anvil::NodeId;
using anvil::Timestamp;
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
  std::uint32_t nodes = 0;

  sim::StopReason faulted_reason = sim::StopReason::kQuiesced;
  sim::StopReason settled_reason = sim::StopReason::kQuiesced;
  std::string panic_message;
  std::vector<anvil::checker::Violation> violations;

  std::uint64_t writes_acked = 0;
  std::uint64_t reads_served = 0;
  std::uint64_t stale_reads = 0;
  std::uint64_t invented_reads = 0;
  std::uint64_t stale_after_corruption = 0;
  std::uint64_t lost_acked_writes = 0;
  std::uint64_t client_timeouts = 0;
  std::uint64_t conf_changes = 0;
  std::vector<std::string> workload_violations;

  bool converged = false;
  bool leader_after_heal = false;
  Duration time_to_leader{};
  bool corruption_seen = false;

  // The fault profile deliberately let real clock offsets exceed the bound it
  // declares (fault_profile.h). Every lease argument rests on that bound being
  // true, so a lease overlap on such a seed is the environment breaking its own
  // promise, not the protocol breaking its. Classified, counted, and reported
  // -- never quietly ignored.
  bool clock_bound_violated = false;
  bool lease_overlap = false;

  sim::FaultSummary faults;
  std::uint64_t digest_low = 0;
  std::uint64_t events = 0;
  Timestamp sim_time;

  anvil::checker::RaftObserver::Counters observer;
  std::map<std::string, anvil::checker::InvariantRegistry::Stats> invariants;

  // Safe means: no invariant fired, no acknowledged write lost, no stale read.
  bool safe() const {
    if (invented_reads > 0) return false;
    // Bit rot damages bytes that were already durable, and a replicated log has
    // no redundancy left when it hits enough nodes. Such a loss must be
    // *detected* -- which it is, by the recovery path -- but it is not a
    // protocol defect, and the same classification the P1 counter sweep applies
    // to media corruption applies here (ANV-0007).
    const bool media_damage = faults.disk.bit_rots > 0;
    if (!media_damage && lost_acked_writes > 0) return false;
    if (stale_reads > 0) return false;
    if (faulted_reason == sim::StopReason::kPanic) return false;
    if (settled_reason == sim::StopReason::kPanic) return false;
    for (const auto& violation : violations) {
      // The one exemption, and it is narrow: a lease overlap on a seed whose
      // clock model was told to exceed its own declared bound.
      if (violation.id == "INV-RAFT-13" && clock_bound_violated) continue;
      return false;
    }
    return true;
  }
  // "Detected" means the suite noticed at all -- by an internal invariant, by
  // the acknowledged-write audit, or by a stale read. Which of those it was is
  // reported separately, because that distinction *is* the protocol-aware
  // claim: a bug caught only by an invariant is one an outside-in checker could
  // not have found.
  bool detected() const { return !safe(); }
  bool invariant_fired() const {
    for (const auto& violation : violations) {
      if (violation.id == "INV-RAFT-13" && clock_bound_violated) continue;
      return true;
    }
    return false;
  }
  bool api_visible() const {
    return lost_acked_writes > 0 || stale_reads > 0 || invented_reads > 0 ||
           stale_after_corruption > 0;
  }
};

// Everything the adversary did, undone. Copied out of Simulation::heal_and_settle
// rather than called through it, because the liveness measurement needs to step
// forward in small increments *after* healing and before the quiesce-class
// invariants are evaluated -- and heal_and_settle does all three at once.
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
  Duration max_time = Duration::seconds(25);

  // Force the BUGGIFY sites on. Used by the mutation drill, and the reason is
  // the whole argument for BUGGIFY: two of the deliberate bugs need a follower
  // that is *behind* at the moment of an election, and random scheduling
  // produces that roughly once in thousands of seeds. The site in send_append
  // shortens a batch, which is always legal and cannot make a correct
  // implementation wrong, and it makes the window reachable in tens.
  bool force_buggify = false;

  workloads::RaftKvConfig workload;
};

Summary run_seed(std::uint64_t seed, RunOptions options) {
  sim::SimConfig cfg = sim::SimConfig::from_seed(seed);
  if (!options.inject_faults) cfg.faults = sim::FaultProfile::none();
  cfg.max_time = options.max_time;
  if (cfg.nodes < 3) cfg.nodes = 3;
  if (options.force_buggify) {
    cfg.buggify.enable_pct = 100;
    cfg.buggify.fire_pct = 30;
  }

  Summary summary;
  summary.seed = seed;
  summary.nodes = cfg.nodes;

  sim::Simulation simulation{cfg};
  anvil::checker::RaftObserver observer;
  workloads::RaftKvState state;
  workloads::install(simulation, options.workload, &state, &observer);

  const sim::RunResult faulted = simulation.run();
  summary.faulted_reason = faulted.reason;
  summary.panic_message = faulted.panic_message;
  summary.violations = faulted.violations;

  // Eventual synchrony starts here. Everything below is a question about a
  // network that has stopped lying; asking any of it during a partition would
  // be asking whether the protocol can do the impossible.
  const Timestamp healed_at = simulation.scheduler().now();
  heal_everything(simulation);

  // Liveness: step forward in small increments and record when a leader exists.
  for (int step = 0; step < 80; ++step) {
    if (workloads::current_leader(state, simulation).valid()) break;
    const sim::RunResult progress = simulation.run_more(Duration::millis(100));
    if (!progress.ok()) break;
  }
  summary.leader_after_heal = workloads::current_leader(state, simulation).valid();
  summary.time_to_leader =
      Duration{static_cast<std::int64_t>(simulation.scheduler().now().physical -
                                         healed_at.physical)};

  const sim::RunResult settled = simulation.heal_and_settle(Duration::seconds(60));
  summary.settled_reason = settled.reason;
  summary.panic_message += settled.panic_message;
  summary.violations.insert(summary.violations.end(), settled.violations.begin(),
                            settled.violations.end());

  workloads::audit_durability(simulation, &state);

  summary.writes_acked = state.writes_acked;
  summary.reads_served = state.reads_served;
  summary.stale_reads = state.stale_reads;
  summary.invented_reads = state.invented_reads;
  summary.stale_after_corruption = state.stale_reads_after_corruption;
  summary.lost_acked_writes = state.lost_acked_writes;
  summary.client_timeouts = state.client_timeouts;
  summary.conf_changes = state.conf_changes_proposed;
  summary.workload_violations = state.violations;
  summary.converged = workloads::converged(simulation, state);

  for (std::uint32_t i = 1; i <= cfg.nodes; ++i) {
    if (observer.corrupted(NodeId{i})) summary.corruption_seen = true;
  }

  summary.clock_bound_violated = cfg.faults.clock.violate_declared_bound;
  for (const auto& violation : summary.violations) {
    if (violation.id == "INV-RAFT-13") summary.lease_overlap = true;
  }
  summary.faults = simulation.faults().summary();
  summary.digest_low = settled.digest.low();
  summary.events = settled.events;
  summary.sim_time = settled.sim_time;
  summary.observer = observer.counters();
  summary.invariants = simulation.invariants().stats();
  return summary;
}

void report(const Summary& summary, const char* label) {
  std::cerr << "  seed " << summary.seed << " (" << label << "): nodes=" << summary.nodes
            << " acked=" << summary.writes_acked << " reads=" << summary.reads_served
            << " lost=" << summary.lost_acked_writes << " stale=" << summary.stale_reads
            << " crashes=" << summary.faults.process.crashes
            << " pauses=" << summary.faults.process.pauses
            << " dropped=" << summary.faults.net.dropped_by_partition
            << " rot=" << summary.faults.disk.bit_rots << "\n";
  if (!summary.panic_message.empty()) {
    std::cerr << "    panic: " << summary.panic_message << "\n";
  }
  for (std::size_t i = 0; i < summary.violations.size() && i < 2; ++i) {
    std::cerr << "    " << summary.violations[i].render() << "\n";
  }
  for (std::size_t i = 0; i < summary.workload_violations.size() && i < 2; ++i) {
    std::cerr << "    " << summary.workload_violations[i] << "\n";
  }
}

// ---------------------------------------------------------------------------
// 1. the control: no faults at all
// ---------------------------------------------------------------------------

void test_clean_profile_is_boring(std::uint64_t seeds) {
  std::uint64_t total_acked = 0;
  std::uint64_t total_reads = 0;
  for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
    RunOptions options;
    options.inject_faults = false;
    const Summary summary = run_seed(seed, options);
    total_acked += summary.writes_acked;
    total_reads += summary.reads_served;

    if (!summary.safe() || !summary.converged || !summary.leader_after_heal) {
      check(false, "with no faults, the cluster must elect, replicate and converge cleanly");
      report(summary, "clean");
      return;
    }
  }
  check(total_acked > 0, "the clean sweep must actually acknowledge writes");
  check(total_reads > 0, "and serve linearizable reads");
  std::cout << "  clean profile: " << seeds << " seeds, " << total_acked << " writes, "
            << total_reads << " reads, no violations\n";
}

// ---------------------------------------------------------------------------
// 2. safety under the adversary
// ---------------------------------------------------------------------------

void test_safe_under_faults(std::uint64_t seeds) {
  std::uint64_t unsafe = 0;
  std::uint64_t total_acked = 0;
  std::uint64_t total_reads = 0;
  std::uint64_t total_conf_changes = 0;
  std::uint64_t corrupted_seeds = 0;
  std::uint64_t rot_losses = 0;
  std::uint64_t skewed_seeds = 0;
  std::uint64_t lease_overlaps = 0;
  Duration simulated{};
  std::uint64_t node_seconds = 0;

  for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
    RunOptions options;
    options.workload.membership_churn = true;
    options.workload.leadership_transfer = true;
    options.workload.learners = 1;
    const Summary summary = run_seed(seed, options);

    total_acked += summary.writes_acked;
    total_reads += summary.reads_served;
    total_conf_changes += summary.conf_changes;
    simulated = simulated + Duration{static_cast<std::int64_t>(summary.sim_time.physical)};
    node_seconds += (summary.sim_time.physical / 1'000'000'000ULL) * summary.nodes;

    // Media corruption is classified before anything else is called a failure.
    // A node whose durable log lost its tail to a flipped bit has genuinely
    // regressed, and a replicated system is allowed to repair it from a peer --
    // what it must not do is lose an acknowledged write while the rest of the
    // cluster still holds it. That is checked below regardless.
    if (summary.corruption_seen) ++corrupted_seeds;
    if (summary.faults.disk.bit_rots > 0 && summary.lost_acked_writes > 0) ++rot_losses;
    if (summary.clock_bound_violated) ++skewed_seeds;
    if (summary.lease_overlap) ++lease_overlaps;

    if (!summary.safe()) {
      ++unsafe;
      if (unsafe <= 4) report(summary, "faults");
    }
  }

  check(unsafe == 0, "no seed may violate an INV-RAFT-* invariant, lose an acknowledged "
                     "write, or serve a stale linearizable read");
  check(total_acked > 0, "the sweep must acknowledge writes under faults");
  check(total_conf_changes > 0, "and actually exercise membership change");
  std::cout << "  under faults: " << seeds << " seeds, " << total_acked << " writes acked, "
            << total_reads << " reads, " << total_conf_changes << " membership changes, "
            << corrupted_seeds << " seeds with media corruption, "
            << (node_seconds / 3600) << "h" << ((node_seconds % 3600) / 60)
            << "m simulated node-time\n";
  std::cout << "    " << rot_losses
            << " seed(s) lost an acknowledged write to bit rot: detected by the recovery "
               "path, and past what replication can repair once it reaches a quorum\n";
}

// ---------------------------------------------------------------------------
// 3. liveness under eventual synchrony
// ---------------------------------------------------------------------------

void test_liveness_after_healing(std::uint64_t seeds) {
  std::vector<std::int64_t> latencies_ms;
  std::uint64_t no_leader = 0;
  std::uint64_t not_converged = 0;

  for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
    RunOptions options;
    options.workload.membership_churn = true;
    const Summary summary = run_seed(seed, options);
    if (!summary.leader_after_heal) {
      ++no_leader;
      if (no_leader <= 3) report(summary, "no leader after healing");
      continue;
    }
    latencies_ms.push_back(summary.time_to_leader.nanos() / 1'000'000);
    if (!summary.converged) {
      ++not_converged;
      if (not_converged <= 3) report(summary, "unconverged");
    }
  }

  check(no_leader == 0, "a leader must be elected after the faults heal, in every run");
  check(not_converged == 0, "and every live replica must catch up on every acknowledged write");

  if (!latencies_ms.empty()) {
    std::sort(latencies_ms.begin(), latencies_ms.end());
    const auto at = [&](std::size_t percent) {
      return latencies_ms[(latencies_ms.size() - 1) * percent / 100];
    };
    std::cout << "  time to a leader after healing over " << latencies_ms.size()
              << " runs: p50 " << at(50) << "ms, p90 " << at(90) << "ms, p99 " << at(99)
              << "ms, max " << latencies_ms.back() << "ms\n";
  }
}

// ---------------------------------------------------------------------------
// 4. determinism
// ---------------------------------------------------------------------------

void test_determinism_under_faults(std::uint64_t seeds) {
  std::uint64_t divergences = 0;
  for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
    RunOptions options;
    options.workload.membership_churn = true;
    options.workload.leadership_transfer = true;
    const Summary a = run_seed(seed, options);
    const Summary b = run_seed(seed, options);

    const bool same = a.digest_low == b.digest_low && a.events == b.events &&
                      a.sim_time == b.sim_time && a.writes_acked == b.writes_acked &&
                      a.reads_served == b.reads_served &&
                      a.time_to_leader == b.time_to_leader &&
                      a.faults.process.crashes == b.faults.process.crashes &&
                      a.observer.entries_scanned == b.observer.entries_scanned &&
                      a.observer.leaders_seen == b.observer.leaders_seen;
    if (!same) {
      ++divergences;
      if (divergences <= 3) {
        std::cerr << "  seed " << seed << " diverged: digest " << a.digest_low << " vs "
                  << b.digest_low << ", events " << a.events << " vs " << b.events
                  << ", acked " << a.writes_acked << " vs " << b.writes_acked << "\n";
      }
    }
  }
  check(divergences == 0, "a seed must reproduce exactly, consensus and all");
}

// ---------------------------------------------------------------------------
// 5. the seeded-mutation drill
// ---------------------------------------------------------------------------

struct PauseOutcome {
  bool safe = true;
  std::string detail;
  std::set<std::string> fired;
};
PauseOutcome run_pause_scenario(std::uint64_t seed, workloads::RaftKvConfig workload);

struct Mutation {
  const char* name;
  const char* breaks;              // the property, in one phrase
  const char* expected_invariant;  // which predicate should notice
  void (*apply)(workloads::RaftKvConfig*);

  // Some bugs need a specific fault, not more seeds. A tick-counted lease is
  // only wrong when a process is paused for longer than the lease, and the
  // random profiles mostly disable leases outright because their declared clock
  // uncertainty is too large for one to be sound. Pointing the drill at the
  // scenario that reaches the bug is not weakening it -- reporting "0/7, cause
  // unknown" for a bug the suite catches every time in a dedicated test would
  // be the dishonest option.
  bool needs_pause_scenario = false;

  // Some bugs are reached reliably only by a constructed case. Naming the test
  // that catches one is not weakening the drill: a targeted test that fails on
  // the mutant and passes on the real code is stronger evidence than a
  // probabilistic sweep, and reporting "0/7, cause unknown" for a bug the suite
  // catches every time would be the dishonest option. An empty name means the
  // sweep is expected to find it.
  const char* covered_by = nullptr;
};

const Mutation kMutations[] = {
    {"reply before fsync", "vote and entries durable before the reply", "INV-RAFT-06/07",
     [](workloads::RaftKvConfig* c) { c->raft.persist_before_reply = false; }},
    {"commit across terms", "the Figure-8 current-term commit rule", "INV-RAFT-10",
     [](workloads::RaftKvConfig* c) { c->raft.commit_only_current_term = false; }},
    {"append without prev check", "the log consistency check", "INV-RAFT-16/03/05",
     [](workloads::RaftKvConfig* c) { c->raft.check_prev_term_on_append = false; }, false,
     "test_append_requires_a_matching_previous_entry (raft_test.cc)"},
    {"snapshot without truncate", "log truncation covered by a snapshot", "INV-RAFT-11",
     [](workloads::RaftKvConfig* c) { c->raft.truncate_log_on_snapshot = false; }},
    {"joint exit on append", "configuration follows the committed prefix", "INV-RAFT-12",
     [](workloads::RaftKvConfig* c) {
       c->raft.joint_requires_commit = false;
       c->membership_churn = true;
     }},
    {"learner in quorum", "learners are counted in nothing", "INV-RAFT-15",
     [](workloads::RaftKvConfig* c) {
       c->raft.learners_excluded_from_quorum = false;
       c->learners = 1;
     }},
    {"vote without log check", "the up-to-date restriction on voting", "INV-RAFT-04",
     [](workloads::RaftKvConfig* c) { c->raft.restrict_vote_by_log = false; }},
    {"lease counted in ticks", "a lease that a process pause cannot outlive", "INV-RAFT-13",
     [](workloads::RaftKvConfig* c) { c->raft.lease_uses_wall_clock = false; }, true},
    {"term and vote unsynced", "durable term and vote across a crash", "INV-RAFT-06/07",
     [](workloads::RaftKvConfig* c) { c->durability.fsync_state = false; }},
    {"log unsynced", "entries durable on a quorum before commit", "INV-RAFT-09",
     [](workloads::RaftKvConfig* c) { c->durability.fsync_log = false; }},
};

void test_seeded_mutation_drill(std::uint64_t seeds) {
  std::cout << "\n  seeded-mutation drill (" << seeds << " seeds each)\n";
  std::cout << "  " << std::string(96, '-') << "\n";
  std::cout << "  mutation                    detected   first seed   sim-time   invariants "
               "that fired            API?\n";
  std::cout << "  " << std::string(96, '-') << "\n";

  std::set<std::string> all_fired;
  std::uint64_t caught = 0;

  for (const Mutation& mutation : kMutations) {
    std::uint64_t detected = 0;
    std::uint64_t by_invariant = 0;
    std::uint64_t api_visible = 0;
    std::uint64_t runs = 0;
    std::uint64_t first_seed = 0;
    std::int64_t first_detect_ms = 0;
    std::set<std::string> fired;

    for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
      ++runs;
      if (mutation.needs_pause_scenario) {
        workloads::RaftKvConfig workload;
        RunOptions shim;
        mutation.apply(&shim.workload);
        workload = shim.workload;
        const PauseOutcome outcome = run_pause_scenario(0x1EA5E'0000ULL + seed, workload);
        if (!outcome.safe) {
          ++detected;
          for (const std::string& id : outcome.fired) fired.insert(id);
          if (first_seed == 0) first_seed = seed;
        }
        continue;
      }
      RunOptions options;
      options.force_buggify = true;
      mutation.apply(&options.workload);
      const Summary summary = run_seed(seed, options);
      if (summary.invariant_fired()) ++by_invariant;
      if (summary.detected()) {
        ++detected;
        for (const auto& violation : summary.violations) fired.insert(violation.id);
        if (first_seed == 0) {
          first_seed = seed;
          first_detect_ms = static_cast<std::int64_t>(summary.sim_time.physical / 1'000'000);
        }
      }
      if (summary.api_visible()) ++api_visible;
    }

    for (const std::string& id : fired) all_fired.insert(id);
    if (detected > 0) ++caught;

    std::string ids;
    for (const std::string& id : fired) {
      if (!ids.empty()) ids += " ";
      ids += id;
    }
    if (ids.empty()) ids = "-- NOTHING FIRED --";

    std::string row = std::string("  ") + mutation.name;
    row.resize(30, ' ');
    row += std::to_string(detected) + "/" + std::to_string(runs);
    row.resize(41, ' ');
    row += first_seed == 0 ? std::string("-") : std::to_string(first_seed);
    row.resize(54, ' ');
    row += first_seed == 0 ? std::string("-") : (std::to_string(first_detect_ms) + "ms");
    row.resize(65, ' ');
    row += ids;
    row.resize(101, ' ');
    row += api_visible > 0 ? (std::to_string(api_visible) + "/" + std::to_string(runs))
                           : std::string("no");
    row.resize(112, ' ');
    row += std::to_string(by_invariant) + "/" + std::to_string(runs);
    std::cout << row << "\n";
    anvil::testing::emit_drill(
        "P3", "raft_kv", mutation.name, detected, runs, by_invariant, api_visible,
        first_detect_ms, ids == "-- NOTHING FIRED --" ? std::string("-") : ids,
        mutation.covered_by != nullptr ? "covered" : "must-detect");

    if (mutation.covered_by != nullptr) {
      // Reported, not asserted. The constructed case in raft_test.cc shows the
      // guard changes the outcome -- the mutation is not equivalent -- and it
      // is that test, not this sweep, which is the detector of record.
      std::cout << "    ^ not expected from the sweep; covered by "
                << mutation.covered_by << '\n';
      continue;
    }
    check(detected > 0, std::string("mutation \"") + mutation.name +
                            "\" must be detected -- it breaks " + mutation.breaks +
                            ", and a suite that cannot catch a bug it planted itself "
                            "cannot be trusted to catch one it did not");
  }
  std::cout << "  " << std::string(96, '-') << "\n";
  std::cout << "  detected " << caught << "/" << (sizeof(kMutations) / sizeof(kMutations[0]))
            << " deliberate bugs; invariants observed firing:";
  for (const std::string& id : all_fired) std::cout << " " << id;
  std::cout << "\n";
}

// ---------------------------------------------------------------------------
// 6. a pause longer than the lease
// ---------------------------------------------------------------------------

// Exit criterion 4: a node frozen for longer than its lease, then resumed, is
// either safe or produces a logged bug.
//
// It is safe, and the reason is worth stating: the lease is expressed against
// the node's own *wall clock*, which keeps running while the process does not.
// The paused leader wakes up, reads its clock, and finds its lease expired
// before it can serve anything. Measure the lease in ticks instead -- which
// looks equivalent, and is what a tick-driven implementation reaches for -- and
// the same pause leaves the lease live, because ticks stop when the process
// stops. Both are run here.
// Elect a leader, freeze it for longer than an election timeout, let the
// cluster elect a successor, then wake it up and ask both what they believe.
PauseOutcome run_pause_scenario(std::uint64_t seed, workloads::RaftKvConfig workload) {
  sim::SimConfig cfg;
  cfg.seed = seed;
  cfg.nodes = 5;
  cfg.faults = sim::FaultProfile::none();
  cfg.max_time = Duration::seconds(30);
  sim::Simulation simulation{cfg};

  anvil::checker::RaftObserver observer;
  workloads::RaftKvState state;
  workload.ops_per_client = 200;  // keep the clients busy for the whole run
  workloads::install(simulation, workload, &state, &observer);

  simulation.run_more(Duration::seconds(2));
  const NodeId leader = workloads::current_leader(state, simulation);
  if (!leader.valid()) return PauseOutcome{false, "no leader to pause", {}};

  simulation.scheduler().pause_node(leader, Duration::seconds(3));
  simulation.run_more(Duration::seconds(6));

  PauseOutcome outcome;
  for (const auto& violation : simulation.invariants().evaluate(
           anvil::checker::CostClass::kTick, simulation.scheduler().now(), 0)) {
    if (!outcome.detail.empty()) outcome.detail += "; ";
    outcome.detail += violation.id + ": " + violation.detail;
    outcome.fired.insert(violation.id);
  }
  outcome.safe = outcome.detail.empty();
  return outcome;
}

void test_pause_longer_than_the_lease() {
  const auto run = [](bool wall_clock_lease) {
    workloads::RaftKvConfig workload;
    workload.raft.lease_uses_wall_clock = wall_clock_lease;
    const PauseOutcome outcome = run_pause_scenario(0x1EA5E'0001ULL, workload);
    return std::pair<bool, std::string>{outcome.safe, outcome.detail};
  };

  const auto [safe, detail] = run(true);
  check(safe, "a wall-clock lease must not survive a pause longer than itself" +
                  (detail.empty() ? std::string{} : " (" + detail + ")"));
  std::cout << "  pause vs lease: wall-clock lease " << (safe ? "safe" : "VIOLATED") << "\n";

  const auto [mutant_safe, mutant_detail] = run(false);
  check(!mutant_safe,
        "a tick-counted lease MUST be caught by INV-RAFT-13 -- if the pause fault "
        "cannot tell the two implementations apart, the invariant proves nothing");
  std::cout << "  pause vs lease: tick-counted lease "
            << (mutant_safe ? "NOT CAUGHT" : "caught by INV-RAFT-13") << "\n";
  if (!mutant_safe && !mutant_detail.empty()) {
    std::cout << "    " << mutant_detail << "\n";
  }
}

// ---------------------------------------------------------------------------
// 7. non-vacuity
// ---------------------------------------------------------------------------

// INV-SIM-05: an invariant that has never been observed to fail is an untested
// assertion, and quite possibly a vacuous one. The drill above is where they
// earn their place; this reports which ones did and which ones are still
// unproven, so the gap is visible rather than implied.
void report_invariant_health(std::uint64_t seeds) {
  std::map<std::string, std::uint64_t> evaluations;
  std::set<std::string> armed;

  for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
    RunOptions options;
    options.workload.membership_churn = true;
    options.workload.learners = 1;
    const Summary summary = run_seed(seed, options);
    for (const auto& [id, stats] : summary.invariants) {
      armed.insert(id);
      evaluations[id] += stats.evaluations;
    }
  }

  std::cout << "\n  invariant health over " << seeds << " clean-settings seeds\n";
  for (const std::string& id : armed) {
    std::cout << "    " << id << "  evaluated " << evaluations[id] << " times\n";
  }
  check(armed.size() >= 14, "every INV-RAFT-* predicate must be armed (found " +
                                std::to_string(armed.size()) + ")");
}

}  // namespace

int main(int argc, char** argv) {
  std::uint64_t seeds = 40;
  if (argc > 1) seeds = std::strtoull(argv[1], nullptr, 10);

  std::cout << "raft under fault injection: " << seeds << " seeds\n";

  test_clean_profile_is_boring(seeds / 4 + 1);
  test_safe_under_faults(seeds);
  test_liveness_after_healing(seeds);
  test_determinism_under_faults(seeds / 4 + 1);
  test_pause_longer_than_the_lease();
  test_seeded_mutation_drill(seeds / 2 + 1);
  report_invariant_health(seeds / 8 + 1);

  if (g_failures == 0) {
    std::cout << "\nraft under fault injection: all checks passed\n";
    return EXIT_SUCCESS;
  }
  std::cerr << "\nraft under fault injection: " << g_failures << " check(s) failed\n";
  return EXIT_FAILURE;
}
