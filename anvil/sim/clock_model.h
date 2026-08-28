// The simulated clock, per node.
//
// Every node gets its own opinion of the time. That opinion is wrong by an
// offset drawn once at boot, drifts at its own rate, occasionally gets stepped
// by an imaginary NTP daemon, and sometimes stops entirely while the node
// carries on believing no time has passed.
//
// Two things here are worth more than the rest of the file.
//
// The first is that `now_uncertain()` returns a bracket, not a point, and the
// width of that bracket is what the system was *told* to expect -- while the
// actual offset is drawn separately. When `violate_declared_bound` is set, the
// two disagree, and the bracket is a lie. That is deliberate. "Is this system
// correct under its stated assumptions" and "what does it do when the
// assumption is false" are different questions, and the second one is where
// Spanner-style designs either degrade or corrupt. Knowing which is a result.
//
// The second is freeze. A frozen clock is not a stopped node -- the node keeps
// running, keeps answering, and keeps believing its lease is valid, while real
// time moves on without it. That combination is how leases actually get
// violated in production, and it is much nastier than a crash.
//
// All arithmetic is integer and saturating at zero. A node's clock reading is
// never negative, and the physical component is unsigned, so every subtraction
// here is guarded.

#ifndef ANVIL_SIM_CLOCK_MODEL_H_
#define ANVIL_SIM_CLOCK_MODEL_H_

#include <cstdint>
#include <map>

#include "anvil/core/random.h"
#include "anvil/core/types.h"
#include "anvil/sim/fault_profile.h"

namespace anvil::sim {

class ClockModel {
 public:
  ClockModel(std::uint64_t seed, ClockFaults faults, std::uint32_t nodes);

  // What `node` believes the time is, given that true simulated time is `truth`.
  Timestamp node_now(NodeId node, Timestamp truth) const;

  // The node's honest bracket. Width comes from the *declared* bound, which may
  // or may not actually contain the truth.
  TimeInterval node_now_uncertain(NodeId node, Timestamp truth) const;

  // True if this node's real offset currently exceeds the declared bound, i.e.
  // the system's core timing assumption is false for it right now. Recorded in
  // the fault statistics so a run can be classified honestly: a failure under a
  // violated bound is a different finding from one under a respected bound.
  bool bound_violated_for(NodeId node) const;

  // Fault injector hooks.
  void step(NodeId node, Duration delta);           // NTP correction
  void freeze(NodeId node, Timestamp at);
  void thaw(NodeId node);
  bool frozen(NodeId node) const;

  // A node that crashes and restarts gets a fresh offset: the machine rebooted,
  // NTP resynced, and whatever the old skew was is gone.
  void reseed_node(NodeId node, std::uint64_t incarnation);

  Duration declared_uncertainty() const noexcept { return faults_.declared_uncertainty; }

 private:
  struct NodeClock {
    std::int64_t offset_nanos = 0;
    std::int64_t drift_ppm = 0;
    std::int64_t step_nanos = 0;   // accumulated NTP corrections
    bool frozen = false;
    Timestamp frozen_at;
  };

  const NodeClock& clock_for(NodeId node) const;
  NodeClock& clock_for(NodeId node);
  NodeClock make_clock(std::uint64_t stream) const;

  std::uint64_t seed_;
  ClockFaults faults_;
  std::map<std::uint64_t, NodeClock> clocks_;
};

}  // namespace anvil::sim

#endif  // ANVIL_SIM_CLOCK_MODEL_H_
