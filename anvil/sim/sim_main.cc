// anvil-sim: run one seed.
//
// The whole debugging loop is meant to be this binary and a 64-bit integer.
// A failing nightly run reports a seed; you paste the seed here and watch the
// same failure happen again, in the same order, at the same virtual nanosecond,
// against the same adversary.
//
//   anvil-sim --seed 0x8f3a91c40d2e77b1 --workload counter --faults
//   anvil-sim --seed 0x8f3a91c40d2e77b1 --trace out.jsonl
//   anvil-sim --seed 0x8f3a91c40d2e77b1 --verify      # run twice, diff digests
//   anvil-sim --sweep 2000                            # 2000 seeds, each twice
//
// --sweep is what CI calls on each toolchain; comparing its output across
// gcc/x86-64 and clang/arm64 is INV-SIM-01.

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <string_view>

#include "anvil/sim/simulation.h"
#include "workloads/counter.h"
#include "workloads/pingpong.h"

namespace {

enum class Workload { kPingPong, kCounter };

struct Options {
  std::uint64_t seed = 1;
  std::uint32_t nodes = 0;  // 0 == drawn from the seed
  std::uint64_t sweep = 0;
  Workload workload = Workload::kPingPong;
  bool faults = false;
  bool verify = false;
  bool quiet = false;
  std::string trace_path;
};

[[noreturn]] void usage(int code) {
  std::cerr <<
      R"(anvil-sim -- run one deterministic simulation

  --seed <u64>      run seed; accepts 0x-prefixed hex        (default 1)
  --workload <name> pingpong | counter                       (default pingpong)
  --faults          draw an adversary from the seed          (default: none)
  --nodes <n>       override the cluster size
  --trace <path>    write the causal trace as JSONL
  --verify          run the seed twice and compare everything observable
  --sweep <n>       run seeds 1..n, each twice, report divergence
  --quiet           print only the digest
  --help
)";
  std::exit(code);
}

std::uint64_t parse_u64(std::string_view s) {
  const int base = (s.size() > 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X')) ? 16 : 10;
  return std::strtoull(std::string{s}.c_str(), nullptr, base);
}

struct Outcome {
  anvil::sim::RunResult run;
  anvil::sim::RunResult settled;
  anvil::sim::FaultSummary faults;
  anvil::workloads::PingPongState pingpong;
  anvil::workloads::CounterState counter;
  std::uint64_t config_hash = 0;
  std::uint32_t nodes = 0;
  bool converged = false;
};

Outcome run_once(const Options& opt, bool record_trace, const std::string& trace_path) {
  anvil::sim::SimConfig cfg = anvil::sim::SimConfig::from_seed(opt.seed);
  if (!opt.faults) cfg.faults = anvil::sim::FaultProfile::none();
  if (opt.nodes > 0) cfg.nodes = opt.nodes;
  if (cfg.nodes < 2) cfg.nodes = 2;
  cfg.record_trace = record_trace;

  Outcome outcome;
  outcome.config_hash = cfg.config_hash();
  outcome.nodes = cfg.nodes;

  anvil::sim::Simulation simulation{cfg};

  if (opt.workload == Workload::kPingPong) {
    anvil::workloads::install(simulation, anvil::workloads::PingPongConfig{}, &outcome.pingpong);
    outcome.run = simulation.run();
    outcome.settled = outcome.run;
  } else {
    anvil::workloads::install(simulation, anvil::workloads::CounterConfig{}, &outcome.counter);
    outcome.run = simulation.run();
    outcome.settled = simulation.heal_and_settle(anvil::Duration::seconds(120));
    outcome.converged = anvil::workloads::converged(simulation, outcome.counter);
  }

  outcome.faults = simulation.faults().summary();

  if (record_trace && !trace_path.empty()) {
    if (!simulation.trace().write_jsonl(trace_path)) {
      std::cerr << "warning: could not write trace to " << trace_path << "\n";
    }
  }
  return outcome;
}

void print(const Options& opt, const Outcome& o) {
  const auto hex = o.settled.digest.hex();
  if (opt.quiet) {
    std::cout << hex.data() << "\n";
    return;
  }

  std::cout << "seed          0x" << std::hex << opt.seed << std::dec << "\n"
            << "config        0x" << std::hex << o.config_hash << std::dec << "\n"
            << "nodes         " << o.nodes << "\n"
            << "digest        " << hex.data() << "\n"
            << "result        " << anvil::sim::to_string(o.run.reason) << "\n"
            << "sim-time      " << o.settled.sim_time.physical / 1'000'000 << " ms\n"
            << "events        " << o.settled.events << "\n";

  if (opt.workload == Workload::kPingPong) {
    std::cout << "laps          " << o.pingpong.laps_completed << "\n"
              << "checksum      0x" << std::hex << o.pingpong.checksum << std::dec << "\n";
  } else {
    std::cout << "acknowledged  " << o.counter.acked_ids.size() << "\n"
              << "converged     " << (o.converged ? "yes" : "NO") << "\n"
              << "lost writes   " << o.counter.lost_acked_writes
              << (o.counter.lost_acked_writes > 0 ? "   <-- DURABILITY VIOLATION" : "") << "\n"
              << "corruption    " << o.counter.corruption_detected << " detected\n"
              << "recoveries    " << o.counter.recoveries << " (" << o.counter.boot_retries
              << " retries)\n";
    for (const std::string& v : o.counter.violations) std::cout << "  ! " << v << "\n";
  }

  if (opt.faults) {
    const auto& n = o.faults.net;
    const auto& d = o.faults.disk;
    const auto& p = o.faults.process;
    std::cout << "-- adversary --\n"
              << "  net       " << n.sent << " sent, " << n.dropped_by_loss << " dropped, "
              << n.dropped_by_partition << " partitioned, " << n.duplicated << " duplicated, "
              << n.reset << " reset, " << n.reordered << " reordered\n"
              << "  disk      " << d.fsyncs << " fsyncs, " << d.sectors_torn << " torn, "
              << d.sectors_lost << " reverted, " << d.files_lost_to_entry << " files lost, "
              << d.bit_rots << " bit rot, " << d.io_errors << " EIO, " << d.no_space
              << " ENOSPC\n"
              << "  process   " << p.crashes << " crashes, " << p.restarts << " restarts, "
              << p.pauses << " pauses\n"
              << "  clock     " << o.faults.clock_jumps << " jumps, " << o.faults.clock_freezes
              << " freezes, " << o.faults.nodes_outside_declared_bound
              << " nodes outside the declared bound\n";
  }

  if (!o.settled.panic_message.empty()) {
    std::cout << "panic         " << o.settled.panic_message << "\n";
  }
}

// Two runs of one seed must agree on everything observable. Comparing workload
// results and fault counts as well as the scheduler digest is not redundancy
// for its own sake: they are independent observations, and if one could drift
// while the others held, the gate would be weaker than it looks.
bool agrees(const Outcome& a, const Outcome& b, std::string* why) {
  if (!(a.settled.digest == b.settled.digest)) { *why = "execution digest"; return false; }
  if (a.settled.events != b.settled.events) { *why = "event count"; return false; }
  if (a.settled.sim_time != b.settled.sim_time) { *why = "simulated time"; return false; }
  if (a.run.reason != b.run.reason) { *why = "stop reason"; return false; }
  if (a.pingpong.checksum != b.pingpong.checksum) { *why = "workload checksum"; return false; }
  if (a.pingpong.laps_completed != b.pingpong.laps_completed) { *why = "laps"; return false; }
  if (a.counter.acked_ids != b.counter.acked_ids) { *why = "acknowledged set"; return false; }
  if (a.counter.lost_acked_writes != b.counter.lost_acked_writes) {
    *why = "lost writes";
    return false;
  }
  if (a.faults.process.crashes != b.faults.process.crashes) { *why = "crash count"; return false; }
  if (a.faults.net.dropped_by_loss != b.faults.net.dropped_by_loss) {
    *why = "drop count";
    return false;
  }
  if (a.faults.disk.sectors_torn != b.faults.disk.sectors_torn) { *why = "torn sectors"; return false; }
  return true;
}

int sweep(Options opt) {
  std::uint64_t divergences = 0;
  std::uint64_t node_nanos = 0;
  anvil::Digest rollup;

  for (std::uint64_t s = 1; s <= opt.sweep; ++s) {
    opt.seed = s;
    const Outcome first = run_once(opt, false, "");
    const Outcome second = run_once(opt, false, "");

    std::string why;
    if (!agrees(first, second, &why)) {
      ++divergences;
      std::cerr << "DIVERGENCE seed=" << s << " field=" << why << "\n";
    }
    node_nanos += first.settled.sim_time.physical * first.nodes;
    // A rollup over every seed's digest, so two toolchains can be compared with
    // one number instead of diffing a million lines.
    rollup.mix(s).mix(first.settled.digest.low()).mix(first.settled.digest.high());
  }

  const auto hex = rollup.hex();
  std::cout << "seeds         " << opt.sweep << "\n"
            << "divergences   " << divergences << "\n"
            << "node-hours    " << node_nanos / 3'600'000'000'000ULL << " simulated\n"
            << "rollup        " << hex.data() << "\n";
  return divergences == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}

}  // namespace

int main(int argc, char** argv) {
  Options opt;

  for (int i = 1; i < argc; ++i) {
    const std::string_view arg{argv[i]};
    const auto next = [&]() -> std::string_view {
      if (i + 1 >= argc) usage(EXIT_FAILURE);
      return argv[++i];
    };
    if (arg == "--seed") opt.seed = parse_u64(next());
    else if (arg == "--nodes") opt.nodes = static_cast<std::uint32_t>(parse_u64(next()));
    else if (arg == "--sweep") opt.sweep = parse_u64(next());
    else if (arg == "--trace") opt.trace_path = std::string{next()};
    else if (arg == "--faults") opt.faults = true;
    else if (arg == "--verify") opt.verify = true;
    else if (arg == "--quiet") opt.quiet = true;
    else if (arg == "--workload") {
      const std::string_view name = next();
      if (name == "pingpong") opt.workload = Workload::kPingPong;
      else if (name == "counter") opt.workload = Workload::kCounter;
      else { std::cerr << "unknown workload: " << name << "\n"; return EXIT_FAILURE; }
    }
    else if (arg == "--help" || arg == "-h") usage(EXIT_SUCCESS);
    else { std::cerr << "unknown argument: " << arg << "\n"; usage(EXIT_FAILURE); }
  }

  try {
    if (opt.sweep > 0) return sweep(opt);

    const Outcome first = run_once(opt, !opt.trace_path.empty(), opt.trace_path);
    print(opt, first);

    if (opt.verify) {
      const Outcome second = run_once(opt, false, "");
      std::string why;
      if (!agrees(first, second, &why)) {
        std::cerr << "\nDETERMINISM FAILURE: the same seed diverged on " << why << ".\n"
                  << "Something above the Runtime seam is reading state the seed does\n"
                  << "not control. Start with tools/hermetic_check.py, then look for\n"
                  << "unordered-container iteration and pointer-value comparisons.\n";
        return EXIT_FAILURE;
      }
      std::cout << "verify        identical across two runs\n";
    }

    if (first.settled.reason == anvil::sim::StopReason::kPanic) return EXIT_FAILURE;
    if (opt.workload == Workload::kCounter && first.counter.lost_acked_writes > 0) {
      return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
  } catch (const std::exception& e) {
    std::cerr << "fatal: " << e.what() << "\n";
    return EXIT_FAILURE;
  }
}
