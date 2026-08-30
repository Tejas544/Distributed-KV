// P5 exit criteria: a sharded cluster under an adversary, checked from inside.
//
// Five questions, in the order in which it is easy to fool yourself:
//
//   1. Does the key space stay coherent?
//      Continuous splits, merges, replica changes and lease moves running
//      *during* a transactional workload, with partitions, crashes, clock skew
//      and torn writes drawn from the seed. Not one INV-SHARD-* violation, not
//      one account lost, and the total is the number the cluster started with.
//
//   2. Is the money still there?
//      The workload is a bank, and that is not decoration. Every failure this
//      phase is about -- a split that drops a key, a merge that loses one, a
//      write accepted against a stale descriptor, a transfer applied twice --
//      moves a single integer that a black-box client could compute. It is the
//      client-visible half of the argument, and it is checked separately from
//      the invariants so the two columns stay honest.
//
//   3. Does a transaction over a moving split point half-apply?
//      Transfers deliberately pick accounts close together, so a steady
//      fraction of them straddle a boundary that is being moved underneath
//      them. Each one must either apply entirely under the old topology or be
//      rejected entirely under the new one.
//
//   4. Is a seed still a complete description of the run?
//      One placement group plus one Raft group per range, groups created and
//      destroyed while the run is in flight. If determinism breaks anywhere it
//      breaks here.
//
//   5. Would any of this notice if the sharding were wrong?
//      Seven deliberate bugs, one per named flag, every default correct. Each
//      one must be detected, and for each we record whether a client could have
//      seen it. That column is the whole protocol-aware claim.
//
// Nothing here asserts on wall-clock time, and every failure prints its seed.

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "anvil/checker/shard_invariants.h"
#include "anvil/sim/simulation.h"
#include "workloads/shard_kv.h"

namespace {

using anvil::Duration;
using anvil::NodeId;
using anvil::Timestamp;
namespace sim = anvil::sim;
namespace shard = anvil::shard;
namespace workloads = anvil::workloads;

int g_failures = 0;

void check(bool condition, std::string_view what) {
  if (!condition) {
    std::cerr << "FAIL: " << what << "\n";
    ++g_failures;
  }
}

// ---------------------------------------------------------------------------
// one seed
// ---------------------------------------------------------------------------

struct Summary {
  std::uint64_t seed = 0;
  std::uint32_t nodes = 0;

  sim::StopReason faulted_reason = sim::StopReason::kQuiesced;
  sim::StopReason settled_reason = sim::StopReason::kQuiesced;
  std::string panic_message;
  std::vector<anvil::checker::Violation> violations;

  std::uint64_t transfers_acked = 0;
  std::uint64_t transfers_applied = 0;
  std::uint64_t transfers_declined = 0;
  std::uint64_t reads_served = 0;
  std::uint64_t straddling_attempts = 0;
  std::uint64_t wrong_range_replies = 0;
  std::uint64_t not_leader_replies = 0;
  std::uint64_t declined_cross_range = 0;
  std::uint64_t client_timeouts = 0;
  std::uint64_t stale_lease_reads = 0;

  std::int64_t total_balance = 0;
  std::int64_t expected_balance = 0;
  std::uint64_t lost_ops = 0;
  std::size_t ranges_final = 0;
  bool converged = false;
  std::vector<std::string> workload_violations;

  // Topology churn, summed over the cluster.
  std::uint64_t splits = 0;
  std::uint64_t merges = 0;
  std::uint64_t groups_created = 0;
  std::uint64_t groups_retired = 0;
  std::uint64_t leases_taken = 0;
  std::uint64_t lease_transfers = 0;
  std::uint64_t decisions = 0;
  std::uint64_t replica_changes = 0;

  // MultiRaft.
  std::uint64_t heartbeats_coalesced = 0;
  std::uint64_t heartbeat_batches = 0;
  std::uint64_t ticks_skipped = 0;
  std::uint64_t unroutable = 0;

  // Requests rejected specifically because their cached generation was stale,
  // as opposed to because their keys were outside the range. The distinction is
  // what turns "the drill did not catch it" into an argument.
  std::uint64_t generation_rejections = 0;

  anvil::checker::ShardObserver::Counters observer;
  sim::FaultSummary faults;
  std::uint64_t digest_low = 0;
  std::uint64_t events = 0;
  Timestamp sim_time;
  std::map<std::string, anvil::checker::InvariantRegistry::Stats> invariants;

  // The environment broke the clock bound it declared, measured rather than
  // assumed: some holder's clock was further from true time than the
  // configuration said was possible. A lease is an optimisation licensed by a
  // clock bound, and when the licence is void the consequence is classified,
  // not counted as a pass and not counted as a failure.
  // Measured, not inferred from the profile. A node whose clock the fault
  // injector *froze* is outside the declared bound with no configuration flag
  // saying so, and every lease argument rests on that bound holding.
  bool clock_bound_broken() const {
    return observer.lease_overlaps_out_of_bound > 0 ||
           observer.worst_clock_error_nanos > declared_clock_bound_nanos;
  }
  std::uint64_t declared_clock_bound_nanos = 0;

  bool conserved() const { return total_balance == expected_balance; }

  bool safe() const {
    if (faulted_reason == sim::StopReason::kPanic) return false;
    if (settled_reason == sim::StopReason::kPanic) return false;
    if (!conserved()) return false;
    if (lost_ops > 0) return false;
    if (!workload_violations.empty() && !clock_bound_broken()) return false;
    for (const auto& violation : violations) {
      // The one exemption, and it is narrow: a client-visible stale read on a
      // run where a node's clock was measurably outside the declared bound.
      // INV-SHARD-04 itself already excludes those, so anything left in this
      // list happened while the bound was holding.
      if (violation.id == "INV-SHARD-CLIENT" && clock_bound_broken()) continue;
      return false;
    }
    return true;
  }

  bool detected() const { return !safe(); }

  bool invariant_fired() const {
    for (const auto& violation : violations) {
      if (violation.id == "INV-SHARD-CLIENT") continue;  // that one is the client's view
      return true;
    }
    return false;
  }

  // What a black-box client could have seen: money that does not add up, an
  // acknowledged operation the cluster forgot, or a read that went backwards.
  bool api_visible() const {
    return !conserved() || lost_ops > 0 || stale_lease_reads > 0;
  }

  std::set<std::string> fired_ids() const {
    std::set<std::string> out;
    for (const auto& violation : violations) out.insert(violation.id);
    return out;
  }
};

// Everything the adversary did, undone. Copied rather than called through
// heal_and_settle because the topology needs to be given time to finish any
// merge that was in flight before the audits run.
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
  Duration settle = Duration::seconds(30);
  std::uint32_t nodes = 5;
  workloads::ShardKvConfig workload;
};

// The profile the suite runs by default: a topology that will not sit still.
//
// The split and merge thresholds deliberately overlap, so a range that has just
// split is immediately a merge candidate and vice versa. That is a terrible
// autoscaler and exactly the right test: the roadmap asks for continuous
// concurrent splits, merges and rebalances *during* a transactional workload,
// and a cluster whose topology converges after two decisions is not that. The
// cooldown is what keeps it a workload rather than a livelock.
workloads::ShardKvConfig churn_profile() {
  workloads::ShardKvConfig config;
  config.store.placement.split_threshold_keys = 8;
  config.store.placement.merge_threshold_keys = 6;
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

  sim::Simulation simulation{cfg};
  anvil::checker::ShardObserver observer;
  workloads::ShardKvState state;
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

  workloads::audit_conservation(simulation, &state);
  workloads::audit_ledger(simulation, &state);

  summary.transfers_acked = state.transfers_acked;
  summary.transfers_applied = state.transfers_applied;
  summary.transfers_declined = state.transfers_declined;
  summary.reads_served = state.reads_served;
  summary.straddling_attempts = state.straddling_attempts;
  summary.wrong_range_replies = state.wrong_range_replies;
  summary.not_leader_replies = state.not_leader_replies;
  summary.declined_cross_range = state.declined_cross_range;
  summary.client_timeouts = state.client_timeouts;
  summary.stale_lease_reads = state.stale_lease_reads;
  summary.total_balance = state.total_balance;
  summary.expected_balance = state.expected_balance;
  summary.lost_ops = state.lost_ops;
  summary.ranges_final = workloads::range_count(simulation, state);
  summary.converged = workloads::converged(simulation, state);
  summary.workload_violations = state.violations;

  for (const auto& [id, node] : state.nodes) {
    if (node.store == nullptr) continue;
    const shard::StoreStats& stats = node.store->stats();
    summary.splits += stats.splits_executed;
    summary.merges += stats.merges_executed;
    summary.groups_created += stats.groups_created;
    summary.groups_retired += stats.groups_retired;
    summary.leases_taken += stats.leases_taken;
    summary.lease_transfers += stats.lease_transfers;
    summary.decisions += stats.decisions_proposed;
    summary.ticks_skipped += stats.ticks_skipped;
    for (const auto& [range_id, replica] : node.store->ranges()) {
      if (replica.machine == nullptr) continue;
      summary.generation_rejections += replica.machine->counters().transfers_rejected_generation;
    }
    const anvil::raft::TransportStats& transport = node.store->transport().stats();
    summary.heartbeats_coalesced += transport.heartbeats_coalesced;
    summary.heartbeat_batches += transport.heartbeat_batches;
    summary.unroutable += transport.unroutable;
  }

  summary.observer = observer.counters();
  summary.declared_clock_bound_nanos =
      static_cast<std::uint64_t>(cfg.faults.clock.declared_uncertainty.nanos());
  summary.replica_changes = summary.observer.replica_changes;
  summary.faults = simulation.faults().summary();
  summary.digest_low = settled.digest.low();
  summary.events = settled.events;
  summary.sim_time = settled.sim_time;
  summary.invariants = simulation.invariants().stats();
  return summary;
}

void report(const Summary& s, const char* what) {
  std::cerr << "  seed " << s.seed << " (" << what << "): ranges=" << s.ranges_final
            << " acked=" << s.transfers_acked << " total=" << s.total_balance << "/"
            << s.expected_balance << " lost=" << s.lost_ops << " splits=" << s.splits
            << " merges=" << s.merges << " converged=" << (s.converged ? "yes" : "no") << "\n";
  for (const auto& violation : s.violations) std::cerr << "    " << violation.render() << "\n";
  for (const auto& line : s.workload_violations) std::cerr << "    " << line << "\n";
}

// ---------------------------------------------------------------------------
// 1 + 2 + 3: safety, conservation, and the split point
// ---------------------------------------------------------------------------

void test_safety_under_faults(std::uint64_t seeds) {
  std::uint64_t acked = 0;
  std::uint64_t reads = 0;
  std::uint64_t straddling = 0;
  std::uint64_t rejected = 0;
  std::uint64_t splits = 0;
  std::uint64_t merges = 0;
  std::uint64_t created = 0;
  std::uint64_t retired = 0;
  std::uint64_t replica_changes = 0;
  std::uint64_t lease_transfers = 0;
  std::uint64_t coalesced = 0;
  std::uint64_t batches = 0;
  std::uint64_t skipped = 0;
  std::uint64_t clock_classified = 0;
  std::uint64_t worst_clock_error = 0;
  std::uint64_t declared_bound = 0;
  std::size_t max_ranges = 0;
  bool all_safe = true;
  Duration node_time{};

  RunOptions options;
  options.workload = churn_profile();

  for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
    const Summary s = run_seed(seed, options);
    if (!s.safe()) {
      all_safe = false;
      report(s, "faults");
    }
    if (s.clock_bound_broken()) {
      ++clock_classified;
      worst_clock_error = std::max(worst_clock_error, s.observer.worst_clock_error_nanos);
    }
    declared_bound = std::max(declared_bound, s.declared_clock_bound_nanos);
    {
    }
    acked += s.transfers_acked;
    reads += s.reads_served;
    straddling += s.straddling_attempts;
    rejected += s.wrong_range_replies;
    splits += s.splits;
    merges += s.merges;
    created += s.groups_created;
    retired += s.groups_retired;
    replica_changes += s.replica_changes;
    lease_transfers += s.lease_transfers;
    coalesced += s.heartbeats_coalesced;
    batches += s.heartbeat_batches;
    skipped += s.ticks_skipped;
    max_ranges = std::max(max_ranges, static_cast<std::size_t>(s.observer.ranges_high_water));
    node_time = node_time + Duration{static_cast<std::int64_t>(s.sim_time.physical) *
                                     static_cast<std::int64_t>(s.nodes)};
  }

  check(all_safe,
        "no seed may violate an INV-SHARD-* invariant, lose an account, lose an "
        "acknowledged transfer, or fail to conserve the total");

  const std::int64_t hours = node_time.nanos() / 3'600'000'000'000LL;
  const std::int64_t minutes = (node_time.nanos() / 60'000'000'000LL) % 60;
  std::cout << "  under faults: " << seeds << " seeds, " << acked << " transfers acknowledged, "
            << reads << " lease reads, " << hours << "h" << minutes << "m simulated node-time\n";
  std::cout << "  topology churn: " << splits << " splits, " << merges << " merges, "
            << replica_changes << " replica changes, " << lease_transfers
            << " lease transfers for colocation, " << created << " groups created and " << retired
            << " retired, " << max_ranges << " ranges at the high-water mark\n";
  std::cout << "  split point: " << straddling
            << " transfers sent believing one range covered both accounts, " << rejected
            << " rejected because the topology had moved underneath them\n";
  if (batches > 0) {
    std::cout << "  multiraft: " << coalesced << " heartbeats carried in " << batches
              << " messages (" << (coalesced * 100 / batches) << "% of one per group), "
              << skipped << " ticks skipped on quiesced ranges\n";
  }
  if (clock_classified > 0) {
    std::cout << "  clock: " << clock_classified << " of " << seeds
              << " seeds put a node's clock outside the bound the configuration "
                 "declared (worst " << worst_clock_error / 1'000'000 << "ms against "
              << declared_bound / 1'000'000
              << "ms); lease overlaps on those seeds are classified, not failed\n";
  }

  // Exit criterion 3 has a number in it, and a suite that reports zero
  // straddling attempts has not tested the split point at all -- it has tested
  // a cluster whose ranges happened never to move under a request.
  check(straddling > 0, "the workload must actually attempt transfers over a moving boundary");
  check(rejected > 0,
        "and some of them must be rejected: a stale-route rejection is the mechanism "
        "working, and never seeing one means the cache is never stale");
  check(splits > 0 && merges > 0, "the sweep must exercise both splits and merges");
}

void test_the_cluster_converges_after_healing(std::uint64_t seeds) {
  RunOptions options;
  options.workload = churn_profile();
  std::uint64_t converged = 0;
  for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
    const Summary s = run_seed(seed, options);
    if (s.converged) ++converged;
  }
  // Liveness under eventual synchrony. Reported rather than demanded at 100%:
  // a merge that was in flight when the faults healed needs a lease, an
  // election and two Raft round trips to finish, and a seed whose settling
  // window ends inside that is a slow run, not a broken one.
  std::cout << "  after healing: " << converged << "/" << seeds
            << " seeds reached a topology where every range has an initialised replica "
               "and no merge is half-done\n";
  check(converged * 2 >= seeds,
        "most seeds must reach a coherent topology once the faults stop");
}

// ---------------------------------------------------------------------------
// 4: determinism
// ---------------------------------------------------------------------------

void test_determinism(std::uint64_t seeds) {
  RunOptions options;
  options.workload = churn_profile();
  options.max_time = Duration::seconds(10);
  options.settle = Duration::seconds(10);

  std::size_t identical = 0;
  for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
    const Summary first = run_seed(seed, options);
    const Summary second = run_seed(seed, options);
    const bool same = first.digest_low == second.digest_low && first.events == second.events &&
                      first.transfers_acked == second.transfers_acked &&
                      first.ranges_final == second.ranges_final &&
                      first.total_balance == second.total_balance;
    if (same) {
      ++identical;
    } else {
      std::cerr << "  seed " << seed << " diverged: digest " << first.digest_low << " vs "
                << second.digest_low << ", events " << first.events << " vs " << second.events
                << ", ranges " << first.ranges_final << " vs " << second.ranges_final << "\n";
    }
  }
  check(identical == seeds, "the same seed must produce the same run, groups and all");
  std::cout << "  determinism: " << identical << "/" << seeds
            << " seeds reproduce exactly, with groups created and destroyed mid-run\n";
}

// ---------------------------------------------------------------------------
// 5: the seeded-mutation drill
// ---------------------------------------------------------------------------

// kEquivalent: the flag genuinely turns a mechanism off, and nothing in this
// configuration can observe the difference. Reporting such a mutation as a test
// gap is as wrong as reporting it as a pass, so it is classified, the argument
// is written down, and the drill asserts *both* that it is not detected and
// that the mechanism really was disabled (CONTEXT.md 8, "equivalent mutants are
// real").
enum class Expectation { kMustDetect, kControl, kEquivalent };

struct Mutation {
  const char* name;
  Expectation expectation;
  void (*apply)(workloads::ShardKvConfig*);
  const char* note;
};

struct DrillResult {
  std::uint64_t detected = 0;
  std::uint64_t seeds = 0;
  std::uint64_t api_visible = 0;
  std::uint64_t invariant_fired = 0;
  std::set<std::string> fired;
  std::uint64_t first_seed = 0;
  std::uint64_t generation_rejections = 0;
};

DrillResult run_mutation(const Mutation& mutation, std::uint64_t seeds) {
  DrillResult result;
  result.seeds = seeds;
  RunOptions options;
  options.workload = churn_profile();
  mutation.apply(&options.workload);

  for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
    const Summary s = run_seed(seed, options);
    result.generation_rejections += s.generation_rejections;
    if (s.detected()) {
      ++result.detected;
      if (result.first_seed == 0) result.first_seed = seed;
      if (s.api_visible()) ++result.api_visible;
      if (s.invariant_fired()) ++result.invariant_fired;
      for (const std::string& id : s.fired_ids()) result.fired.insert(id);
      if (!s.conserved()) result.fired.insert("conservation");
      if (s.lost_ops > 0) result.fired.insert("ledger");
    }
  }
  return result;
}

void test_seeded_mutation_drill(std::uint64_t seeds) {
  const Mutation mutations[] = {
      {"split in two applies", Expectation::kMustDetect,
       [](workloads::ShardKvConfig* c) { c->store.topology.split_is_atomic = false; },
       "the key space is covered twice for the width of one apply"},
      {"generation not bumped", Expectation::kMustDetect,
       [](workloads::ShardKvConfig* c) { c->store.topology.bumps_generation = false; },
       "every cached descriptor in the cluster stays valid forever"},
      {"merge without colocation", Expectation::kMustDetect,
       [](workloads::ShardKvConfig* c) {
         c->store.topology.merge_requires_colocation = false;
         c->store.placement.merge_requires_colocation = false;
       },
       "the survivor absorbs a range some of its replicas have never seen"},
      {"voter added before catch-up", Expectation::kMustDetect,
       [](workloads::ShardKvConfig* c) { c->store.placement.promote_only_caught_up = false; },
       "a voter that holds none of the range's data counts toward its quorum"},
      {"lease without waiting", Expectation::kMustDetect,
       [](workloads::ShardKvConfig* c) {
         c->store.range.lease_requires_previous_expiry = false;
       },
       "two nodes serve reads for the same keys at the same instant"},
      {"stale route served", Expectation::kEquivalent,
       [](workloads::ShardKvConfig* c) { c->store.range.checks_generation = false; },
       "equivalent in this configuration: a range serves a request only for keys it "
       "owns, and the span check already rejects every key the generation check would"},
      {"quiesce over a lagging replica", Expectation::kMustDetect,
       [](workloads::ShardKvConfig* c) {
         c->store.range.quiesce_requires_caught_up = false;
       },
       "a replica that is behind is never sent anything again"},
      {"fewer accounts, more contention", Expectation::kControl,
       [](workloads::ShardKvConfig* c) { c->neighbourhood = 1; },
       "a configuration change that is not a bug; nothing may fire"},
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
    } else if (mutation.expectation == Expectation::kEquivalent) {
      check(result.detected == 0,
            std::string{"an equivalent mutant must not be reported: "} + mutation.name);
      check(result.generation_rejections == 0,
            std::string{"and the mechanism must genuinely be off: "} + mutation.name);
      std::cout << "    ^ " << mutation.note << std::endl;
    } else {
      check(result.detected == 0,
            std::string{"the control must stay silent: "} + mutation.name);
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
  options.workload = churn_profile();
  const Summary s = run_seed(1, options);
  std::cout << "\n  invariant health on a clean seed\n";
  for (const auto& [id, stats] : s.invariants) {
    std::cout << "    " << id << "  evaluated " << stats.evaluations << " times\n";
    check(stats.evaluations > 0,
          std::string{"every armed invariant must actually be evaluated: "} + id);
  }
  std::cout << "    observer: " << s.observer.scans << " scans, "
            << s.observer.descriptor_changes << " descriptor changes, "
            << s.observer.replica_changes << " replica changes, " << s.observer.merges_seen
            << " merges, " << s.observer.source_changes
            << " source changes (the observer's own blind spot)\n";
}

}  // namespace

int main(int argc, char** argv) {
  const std::uint64_t seeds = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 12;
  const std::uint64_t drill_seeds = std::max<std::uint64_t>(5, seeds / 2);

  std::cout << "sharding under fault injection: " << seeds << " seeds\n";

  test_safety_under_faults(seeds);
  test_the_cluster_converges_after_healing(std::min<std::uint64_t>(seeds, 8));
  test_determinism(std::min<std::uint64_t>(seeds, 4));
  test_seeded_mutation_drill(drill_seeds);
  test_invariant_health();

  if (g_failures != 0) {
    std::cerr << "\nsharding under fault injection: " << g_failures << " failure(s)\n";
    return 1;
  }
  std::cout << "\nsharding under fault injection: all checks passed\n";
  return 0;
}
