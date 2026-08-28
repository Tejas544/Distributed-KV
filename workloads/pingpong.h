// The ping-pong workload: a token ring, and the first thing the simulator ever
// simulated.
//
// It exists to exercise the scheduler, not to be interesting in itself. But it
// is deliberately not the simplest possible thing, because a workload with one
// message in flight has exactly one possible schedule and would make the
// determinism gate vacuous. This one has:
//
//   * several tokens circulating at once, so deliveries interleave
//   * a random think time per hop, so the interleaving depends on the seed
//   * a heartbeat timer per node, so timers and messages compete for ordering
//   * a workload-level checksum computed from the order hops actually happened
//
// That last one is the point. The scheduler's execution digest and the
// workload's checksum are independent observations of the same run: the digest
// sees scheduling decisions, the checksum sees what the protocol did with them.
// If the digest matched while the checksum diverged, the digest would be
// failing to cover something, and the determinism gate would be quietly weaker
// than it claims to be. Checking both means neither can rot unnoticed.
//
// With two nodes this degenerates to literal ping-pong, which is the
// configuration P0's exit criteria name.

#ifndef ANVIL_WORKLOADS_PINGPONG_H_
#define ANVIL_WORKLOADS_PINGPONG_H_

#include <cstdint>

#include "anvil/core/types.h"
#include "anvil/sim/simulation.h"

namespace anvil::workloads {

struct PingPongConfig {
  std::uint32_t tokens = 3;
  std::uint64_t laps = 20;  // total laps completed at the origin, across all tokens
  Duration think_min = Duration::micros(50);
  Duration think_max = Duration::millis(2);
  Duration heartbeat = Duration::millis(10);
  Duration inject_stagger = Duration::micros(700);
};

struct PingPongState {
  std::uint64_t laps_completed = 0;
  std::uint64_t forwards = 0;
  std::uint64_t heartbeats = 0;
  std::uint64_t checksum = 0;  // order-sensitive; see the header comment
  bool done = false;
};

// Spawns the workload onto every node. `state` must outlive the simulation.
void install(sim::Simulation& simulation, PingPongConfig config, PingPongState* state);

}  // namespace anvil::workloads

#endif  // ANVIL_WORKLOADS_PINGPONG_H_
