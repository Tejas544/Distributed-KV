// P8: the bug hunt at scale. One shard of the seed fleet.
//
//   anvil_fleet --seeds 2000 --shard 0 --shards 12 --out /tmp/fleet
//
// Turns compute into ledger rows, which is the whole of P8's goal. Every other
// phase built an instrument; this one runs them, at volume, and does the three
// things that decide whether a large fleet is useful or just expensive:
//
//   1. It reports **simulated node-hours**, not wall-clock. A fleet that has
//      run for eight hours has told you nothing until you know how much
//      cluster-time that bought.
//   2. It **minimises the fault set of every failure** before recording it,
//      using anvil/sim/minimiser.h. A failure recorded as "seed 0x... with
//      everything on" is a failure nobody can reason about; "needs exactly a
//      crash and a torn write" is a root cause with the narrative attached.
//   3. It **deduplicates by (workload, invariant, minimised signature)**. The
//      first thing a fleet produces at scale is four hundred copies of one bug,
//      and a report that does not collapse them is a report nobody reads.
//
// Output is JSONL, one object per seed, for tools/fleet_report.py to aggregate.
// A shard writes its own file and nothing is shared, so `--shards N` with one
// process per core needs no locking and no scheduler.
//
// ---------------------------------------------------------------------------
// What "a failure" means here, which is not the same as "a test failed"
// ---------------------------------------------------------------------------
//
// The fault suites in test/ assert. This does not: it *records*. A seed that
// violates an invariant is a candidate finding, and roughly a third of this
// project's candidate findings have historically been the harness rather than
// the engine. So the fleet's job is to produce a deduplicated, minimised list
// with a reproduction for each, and a human decides which of the two it is.
//
// The one thing it does assert is its own premise: a workload that reports a
// violation on *every* seed is a broken harness, not a discovery, and the
// report says so rather than filing two thousand rows.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "anvil/checker/mvcc_invariants.h"
#include "anvil/checker/raft_invariants.h"
#include "anvil/checker/shard_invariants.h"
#include "anvil/checker/txn_invariants.h"
#include "anvil/core/buggify.h"
#include "anvil/sim/minimiser.h"
#include "anvil/sim/simulation.h"
#include "workloads/counter.h"
#include "workloads/mvcc_txn.h"
#include "workloads/raft_kv.h"
#include "workloads/shard_kv.h"
#include "workloads/txn_bank.h"

namespace {

using anvil::Duration;
using anvil::NodeId;
using anvil::sim::BuggifyConfig;
using anvil::sim::FaultProfile;
using anvil::sim::FaultSet;
using anvil::sim::MinimiseOptions;
using anvil::sim::MinimiseResult;
using anvil::sim::SimConfig;
using anvil::sim::Simulation;

// What one run produced. Deliberately not a pass/fail: a fleet records.
struct RunOutcome {
  bool violated = false;
  std::string invariant;   // the first one that fired, or "panic"
  std::string detail;
  std::uint64_t events = 0;
  std::uint64_t node_nanos = 0;  // simulated time x nodes
  bool converged = true;
};

// A workload the fleet knows how to run. `run` performs one simulation under a
// given configuration; everything else is bookkeeping the driver does.
struct Workload {
  std::string name;
  std::function<RunOutcome(const SimConfig&)> run;
};

// The simulated node-time a run bought: how far the clock went, times the
// number of nodes. This is the number the exit criterion is stated in and it is
// the only honest measure of how much cluster the fleet has actually exercised.
std::uint64_t node_nanos_of(const Simulation& sim, const anvil::sim::RunResult& last) {
  return static_cast<std::uint64_t>(last.sim_time.physical) * sim.node_count();
}

RunOutcome finish(Simulation& sim, const anvil::sim::RunResult& faulted,
                  const anvil::sim::RunResult& settled, bool converged) {
  RunOutcome out;
  out.events = faulted.events + settled.events;
  out.node_nanos = node_nanos_of(sim, settled.sim_time > faulted.sim_time ? settled : faulted);
  out.converged = converged;

  std::vector<anvil::checker::Violation> all = faulted.violations;
  all.insert(all.end(), settled.violations.begin(), settled.violations.end());
  if (!all.empty()) {
    out.violated = true;
    out.invariant = all.front().id;
    out.detail = all.front().detail;
  } else if (!faulted.ok() || !settled.ok()) {
    out.violated = true;
    out.invariant = "panic";
    out.detail = faulted.ok() ? settled.panic_message : faulted.panic_message;
  }
  return out;
}

// ---------------------------------------------------------------------------
// the workloads
// ---------------------------------------------------------------------------

RunOutcome run_counter(const SimConfig& cfg) {
  Simulation sim{cfg};
  anvil::workloads::CounterState state;
  anvil::workloads::install(sim, anvil::workloads::CounterConfig{}, &state);
  const auto faulted = sim.run();
  const auto settled = sim.heal_and_settle(Duration::seconds(120));
  RunOutcome out = finish(sim, faulted, settled, anvil::workloads::converged(sim, state));
  if (!out.violated && state.lost_acked_writes > 0) {
    out.violated = true;
    out.invariant = "INV-CTR-durability";
    out.detail = "an acknowledged increment did not survive recovery";
  }
  return out;
}

RunOutcome run_raft_kv(const SimConfig& cfg) {
  Simulation sim{cfg};
  anvil::workloads::RaftKvState state;
  anvil::checker::RaftObserver observer;
  anvil::workloads::install(sim, anvil::workloads::RaftKvConfig{}, &state, &observer);
  const auto faulted = sim.run();
  const auto settled = sim.heal_and_settle(Duration::seconds(120));
  return finish(sim, faulted, settled, anvil::workloads::converged(sim, state));
}

RunOutcome run_mvcc(const SimConfig& cfg) {
  // The MVCC workload is single-node and has no crash recovery, which is a
  // scope statement written into test/mvcc_faults.cc rather than something to
  // rediscover here. Reproduced rather than inferred.
  SimConfig local = cfg;
  local.nodes = 1;
  local.faults.process.crash_per_second = anvil::sim::Chance::never();

  Simulation sim{local};
  anvil::workloads::MvccWorkloadState state;
  anvil::checker::MvccObserver observer;
  anvil::workloads::install(sim, anvil::workloads::MvccWorkloadConfig{}, &state, &observer);
  const auto faulted = sim.run();
  const auto settled = sim.heal_and_settle(Duration::seconds(60));
  return finish(sim, faulted, settled, true);
}

RunOutcome run_shard_kv(const SimConfig& cfg) {
  Simulation sim{cfg};
  anvil::workloads::ShardKvState state;
  anvil::checker::ShardObserver observer;
  anvil::workloads::install(sim, anvil::workloads::ShardKvConfig{}, &state, &observer);
  const auto faulted = sim.run();
  const auto settled = sim.heal_and_settle(Duration::seconds(200));
  return finish(sim, faulted, settled, anvil::workloads::converged(sim, state));
}

RunOutcome run_txn_bank(const SimConfig& cfg) {
  Simulation sim{cfg};
  anvil::workloads::TxnBankState state;
  anvil::checker::TxnObserver observer;
  anvil::workloads::install(sim, anvil::workloads::TxnBankConfig{}, &state, &observer);
  const auto faulted = sim.run();
  const auto settled = sim.heal_and_settle(Duration::seconds(200));
  return finish(sim, faulted, settled, anvil::workloads::converged(sim, state));
}

std::vector<Workload> all_workloads() {
  return {
      {"counter", run_counter},
      {"raft_kv", run_raft_kv},
      {"mvcc", run_mvcc},
      {"shard_kv", run_shard_kv},
      {"txn_bank", run_txn_bank},
  };
}

// ---------------------------------------------------------------------------
// JSON, hand-rolled
//
// A dependency for this would be absurd and a quoting bug here would be a
// report nobody can parse, so the one thing that needs care is the escaping.
// ---------------------------------------------------------------------------

std::string json_escape(const std::string& s) {
  std::string out;
  for (const char c : s) {
    switch (c) {
      case '"': out += "\\\""; break;
      case '\\': out += "\\\\"; break;
      case '\n': out += "\\n"; break;
      case '\r': out += "\\r"; break;
      case '\t': out += "\\t"; break;
      default:
        if (static_cast<unsigned char>(c) < 0x20) {
          char buf[8];
          std::snprintf(buf, sizeof(buf), "\\u%04x", c);
          out += buf;
        } else {
          out += c;
        }
    }
  }
  return out;
}

std::string buggify_sidecar_path(const std::string& out_path) {
  const std::string suffix = ".jsonl";
  if (out_path.size() >= suffix.size() &&
      out_path.compare(out_path.size() - suffix.size(), suffix.size(), suffix) == 0) {
    return out_path.substr(0, out_path.size() - suffix.size()) + ".buggify.jsonl";
  }
  return out_path + ".buggify.jsonl";
}

// One line per site this process's registry knows about (every site any seed
// in this shard reached), and how often it fired. A single shard only sees
// the sites its own seeds' code paths touched; tools/fleet_report.py unions
// "ever activated" across every shard's sidecar against the full site count
// to measure P8 exit criterion 2 (BUGGIFY activation coverage).
void write_buggify_sidecar(const std::string& path) {
  std::ofstream out(path);
  if (!out) return;
  const anvil::BuggifyRegistry& registry = anvil::BuggifyRegistry::instance();
  for (std::size_t i = 0; i < registry.size(); ++i) {
    const anvil::BuggifySite* site = registry.at(i);
    if (site == nullptr) continue;
    out << "{\"id\":" << site->id()
        << ",\"file\":\"" << json_escape(std::string(site->file())) << "\""
        << ",\"line\":" << site->line()
        << ",\"evaluations\":" << site->evaluations()
        << ",\"activations\":" << site->activations() << "}\n";
  }
}

}  // namespace

int main(int argc, char** argv) {
  std::uint64_t seeds = 200;
  std::uint64_t shard = 0;
  std::uint64_t shards = 1;
  std::string out_path = "/tmp/fleet/shard.jsonl";
  std::string only;
  bool minimise = true;

  for (int i = 1; i < argc; ++i) {
    const std::string a = argv[i];
    const auto next = [&]() { return i + 1 < argc ? argv[++i] : ""; };
    if (a == "--seeds") seeds = std::strtoull(next(), nullptr, 10);
    else if (a == "--shard") shard = std::strtoull(next(), nullptr, 10);
    else if (a == "--shards") shards = std::strtoull(next(), nullptr, 10);
    else if (a == "--out") out_path = next();
    else if (a == "--workload") only = next();
    else if (a == "--no-minimise") minimise = false;
    else {
      std::cerr << "usage: anvil_fleet [--seeds N] [--shard I --shards N] "
                   "[--workload NAME] [--out FILE] [--no-minimise]\n";
      return 2;
    }
  }
  if (shards == 0) shards = 1;

  std::ofstream out(out_path);
  if (!out) {
    std::cerr << "cannot write " << out_path << "\n";
    return 1;
  }

  const std::vector<Workload> workloads = all_workloads();
  const auto started = std::chrono::steady_clock::now();

  std::uint64_t runs = 0;
  std::uint64_t failures = 0;
  std::uint64_t node_nanos = 0;

  for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
    if (seed % shards != shard) continue;
    for (const Workload& w : workloads) {
      if (!only.empty() && w.name != only) continue;

      SimConfig cfg = SimConfig::from_seed(seed);
      cfg.max_time = Duration::seconds(30);

      const RunOutcome outcome = w.run(cfg);
      ++runs;
      node_nanos += outcome.node_nanos;

      std::string signature = "not-minimised";
      std::uint32_t predicate_runs = 0;
      bool one_minimal = false;
      if (outcome.violated) {
        ++failures;
        if (minimise) {
          // The same failure, not any failure: a predicate that accepts
          // "something went wrong" minimises one bug down to the faults that
          // cause a different one, and the row it produces names the wrong
          // cause.
          const std::string want = outcome.invariant;
          const auto reproduces = [&](const FaultSet& faults, std::uint32_t attempt) {
            SimConfig probe = SimConfig::from_seed(seed);
            probe.max_time = Duration::seconds(30);
            probe.faults = faults.profile;
            probe.buggify = faults.buggify;
            // Attempt 0 is the *original* schedule, not a fresh one. The
            // minimiser varies the schedule between attempts because disabling
            // a fault changes which dice are rolled and the execution diverges
            // -- but the run being minimised is a specific one, and starting
            // anywhere else throws away the only schedule known to fail. With a
            // fresh seed first, an eleven-fault failure minimised to eleven
            // faults with 1-minimality unverified, which is the signature of a
            // predicate that never reproduced anything.
            probe.seed = attempt == 0 ? seed : (seed ^ (0x9e3779b97f4a7c15ull * attempt));
            const RunOutcome r = w.run(probe);
            return r.violated && r.invariant == want;
          };
          MinimiseOptions opts;
          opts.attempts = 2;
          opts.max_runs = 120;
          const MinimiseResult m =
              anvil::sim::minimise(cfg.faults, cfg.buggify, reproduces, opts);
          signature = m.minimal.render();
          predicate_runs = m.predicate_runs;
          one_minimal = m.verified_one_minimal;
        }
      }

      out << "{\"seed\":" << seed << ",\"workload\":\"" << w.name << "\""
          << ",\"violated\":" << (outcome.violated ? "true" : "false")
          << ",\"invariant\":\"" << json_escape(outcome.invariant) << "\""
          << ",\"detail\":\"" << json_escape(outcome.detail) << "\""
          << ",\"signature\":\"" << json_escape(signature) << "\""
          << ",\"one_minimal\":" << (one_minimal ? "true" : "false")
          << ",\"minimiser_runs\":" << predicate_runs
          << ",\"converged\":" << (outcome.converged ? "true" : "false")
          << ",\"events\":" << outcome.events
          << ",\"node_nanos\":" << outcome.node_nanos << "}\n";
      out.flush();
    }
  }

  write_buggify_sidecar(buggify_sidecar_path(out_path));

  const double seconds =
      std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
  const double node_hours = static_cast<double>(node_nanos) / 3.6e12;

  std::cout << "shard " << shard << "/" << shards << ": " << runs << " runs, " << failures
            << " with a violation, " << node_hours << " simulated node-hours in " << seconds
            << "s wall clock";
  if (seconds > 0) {
    std::cout << " (" << (node_hours * 3600.0 / seconds) << "x)";
  }
  std::cout << "\n";
  return 0;
}
