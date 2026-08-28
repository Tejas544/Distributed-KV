// INV-SIM-01, in-process: same seed, same execution.
//
// This is the gate that makes every other claim in the project mean something.
// If a seed does not reproduce, then a bug ledger row's seed is decoration, the
// regression corpus is inert, and "we ran 20,000 simulated node-hours" is just
// a statement about electricity.
//
// Three things are checked, and the third is the one people forget:
//
//   1. Reproducibility.  Same seed, twice, must agree on the execution digest,
//      the workload checksum, the event count, and the simulated end time.
//
//   2. Trace independence.  Recording the trace must not change the digest. If
//      it did, the verbose replay of a failing seed would be a different
//      experiment from the silent fleet run that found it -- the worst possible
//      failure mode, because it only shows up when you are already debugging.
//
//   3. Non-vacuity.  Different seeds must mostly produce different digests, and
//      the workload must actually have done something. A digest function that
//      returned a constant would sail through checks 1 and 2. Same discipline
//      as the negative control for the hermeticity gate: a check that cannot
//      fail is not a check.
//
// The cross-compiler and cross-architecture halves of INV-SIM-01 cannot run in
// one process; CI does those by comparing `anvil-sim --sweep` output across the
// build matrix.

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <set>
#include <string>
#include <string_view>

#include "anvil/sim/simulation.h"
#include "workloads/pingpong.h"

namespace {

int g_failures = 0;

void check(bool condition, std::string_view what) {
  if (!condition) {
    std::cerr << "FAIL: " << what << "\n";
    ++g_failures;
  }
}

struct Outcome {
  anvil::sim::RunResult run;
  anvil::workloads::PingPongState state;
};

// Draws the configuration from the seed too, so the sweep covers a spread of
// cluster sizes and token counts rather than re-testing one shape a thousand
// times. This is swarm testing in miniature; the full configuration space is in
// docs/SCOPE.md section 5.
Outcome run_seed(std::uint64_t seed, bool record_trace) {
  anvil::DeterministicRandom picker{seed ^ 0xA5A5'5A5A'C3C3'3C3CULL};

  anvil::sim::SimConfig cfg;
  cfg.seed = seed;
  cfg.nodes = static_cast<std::uint32_t>(picker.uniform_range(2, 7));
  cfg.max_time = anvil::Duration::seconds(30);
  cfg.record_trace = record_trace;
  // Faults stay off here on purpose. This test is about the *scheduler* being
  // reproducible; determinism with the adversary armed is a separate and
  // stronger check, and it lives in test/sim_faults.cc.
  cfg.faults = anvil::sim::FaultProfile::none();
  cfg.faults.net.max_latency = anvil::Duration::micros(picker.uniform_range(200, 5000));
  cfg.buggify.enable_pct = static_cast<std::uint32_t>(picker.uniform_range(0, 100));

  anvil::workloads::PingPongConfig workload;
  workload.tokens = static_cast<std::uint32_t>(picker.uniform_range(1, 6));
  workload.laps = static_cast<std::uint64_t>(picker.uniform_range(5, 60));

  anvil::sim::Simulation simulation{cfg};
  Outcome outcome;
  anvil::workloads::install(simulation, workload, &outcome.state);
  outcome.run = simulation.run();
  return outcome;
}

bool identical(const Outcome& a, const Outcome& b, std::string* field) {
  if (!(a.run.digest == b.run.digest)) { *field = "execution digest"; return false; }
  if (a.run.events != b.run.events) { *field = "event count"; return false; }
  if (a.run.sim_time != b.run.sim_time) { *field = "simulated end time"; return false; }
  if (a.run.reason != b.run.reason) { *field = "stop reason"; return false; }
  if (a.state.checksum != b.state.checksum) { *field = "workload checksum"; return false; }
  if (a.state.laps_completed != b.state.laps_completed) { *field = "laps"; return false; }
  if (a.state.forwards != b.state.forwards) { *field = "forwards"; return false; }
  if (a.state.heartbeats != b.state.heartbeats) { *field = "heartbeats"; return false; }
  return true;
}

void test_reproducibility(std::uint64_t seeds) {
  std::uint64_t divergences = 0;
  for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
    const Outcome first = run_seed(seed, false);
    const Outcome second = run_seed(seed, false);
    std::string field;
    if (!identical(first, second, &field)) {
      ++divergences;
      if (divergences <= 5) {
        std::cerr << "  seed " << seed << " diverged on " << field << "\n";
      }
    }
  }
  check(divergences == 0, "every seed must reproduce exactly across two runs");
  if (divergences != 0) {
    std::cerr << "  " << divergences << " of " << seeds << " seeds diverged\n";
  }
}

void test_trace_does_not_perturb(std::uint64_t seeds) {
  std::uint64_t perturbed = 0;
  for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
    const Outcome silent = run_seed(seed, false);
    const Outcome traced = run_seed(seed, true);
    std::string field;
    if (!identical(silent, traced, &field)) {
      ++perturbed;
      if (perturbed <= 3) {
        std::cerr << "  seed " << seed << ": tracing changed " << field << "\n";
      }
    }
  }
  check(perturbed == 0, "enabling the trace must not change the execution");
}

void test_non_vacuity(std::uint64_t seeds) {
  std::set<std::string> digests;
  std::uint64_t did_work = 0;
  std::uint64_t not_quiesced = 0;

  for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
    const Outcome o = run_seed(seed, false);
    digests.insert(std::string{o.run.digest.hex().data()});
    if (o.state.laps_completed > 0 && o.state.forwards > 0) ++did_work;

    // The ping-pong workload has no faults to survive yet, so anything other
    // than a clean quiesce means the harness itself misbehaved.
    if (o.run.reason != anvil::sim::StopReason::kQuiesced) {
      ++not_quiesced;
      if (not_quiesced <= 3) {
        std::cerr << "  seed " << seed << " ended as "
                  << anvil::sim::to_string(o.run.reason) << " " << o.run.panic_message << "\n";
      }
    }
  }

  check(not_quiesced == 0, "v0 ping-pong has no faults, so every run should quiesce cleanly");
  check(did_work == seeds, "every run must actually complete laps and forward tokens");

  // Distinct seeds should almost always produce distinct executions. Not
  // strictly all: a two-node ring with one token and five laps has few degrees
  // of freedom, so genuine collisions are legitimate. A large *drop* here would
  // mean the digest had stopped distinguishing runs. Integer comparison rather
  // than a ratio, to keep floating point out of a pass/fail decision even in
  // test code.
  check(digests.size() * 100 > seeds * 95,
        "distinct seeds must produce distinct digests (the digest must not be vacuous)");
  std::cout << "  distinct digests: " << digests.size() << " / " << seeds << "\n";
}

void test_replay_of_a_specific_seed() {
  // The workflow a bug ledger row implies: take one seed, run it, get exactly
  // the same thing every time, forever.
  constexpr std::uint64_t kSeed = 0x8F3A'91C4'0D2E'77B1ULL;
  const Outcome baseline = run_seed(kSeed, false);
  for (int i = 0; i < 20; ++i) {
    const Outcome again = run_seed(kSeed, false);
    std::string field;
    if (!identical(baseline, again, &field)) {
      check(false, "a pinned seed must replay identically on every attempt");
      std::cerr << "  attempt " << i << " diverged on " << field << "\n";
      return;
    }
  }
}

}  // namespace

int main(int argc, char** argv) {
  // The fleet runs far more; this is the per-commit gate, sized to finish in
  // about a second so nobody is tempted to skip it.
  std::uint64_t seeds = 500;
  if (argc > 1) seeds = std::strtoull(argv[1], nullptr, 10);

  std::cout << "sim determinism: " << seeds << " seeds\n";

  test_reproducibility(seeds);
  test_trace_does_not_perturb(seeds / 5 + 1);
  test_non_vacuity(seeds);
  test_replay_of_a_specific_seed();

  if (g_failures == 0) {
    std::cout << "sim determinism: all checks passed\n";
    return EXIT_SUCCESS;
  }
  std::cerr << "sim determinism: " << g_failures << " check(s) failed\n";
  return EXIT_FAILURE;
}
