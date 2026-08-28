#include "anvil/sim/clock_model.h"

namespace anvil::sim {
namespace {

// Saturating add of a signed delta onto an unsigned nanosecond count. A node's
// clock can be behind true time, but it can never read as negative.
std::uint64_t offset_apply(std::uint64_t base, std::int64_t delta) noexcept {
  if (delta >= 0) return base + static_cast<std::uint64_t>(delta);
  const auto magnitude = static_cast<std::uint64_t>(-delta);
  return base > magnitude ? base - magnitude : 0;
}

}  // namespace

ClockModel::ClockModel(std::uint64_t seed, ClockFaults faults, std::uint32_t nodes)
    : seed_(seed), faults_(faults) {
  for (std::uint32_t i = 1; i <= nodes; ++i) {
    clocks_.emplace(i, make_clock(i));
  }
}

ClockModel::NodeClock ClockModel::make_clock(std::uint64_t stream) const {
  auto rng = DeterministicRandom{seed_}.fork(RandomDomain::kClock, stream);
  NodeClock c;
  const std::int64_t bound = faults_.max_offset.nanos();
  if (bound > 0) c.offset_nanos = rng.uniform_range(-bound, bound);
  if (faults_.max_drift_ppm > 0) {
    c.drift_ppm = rng.uniform_range(-faults_.max_drift_ppm, faults_.max_drift_ppm);
  }
  return c;
}

const ClockModel::NodeClock& ClockModel::clock_for(NodeId node) const {
  static const NodeClock kPerfect{};
  const auto it = clocks_.find(node.value());
  return it == clocks_.end() ? kPerfect : it->second;
}

ClockModel::NodeClock& ClockModel::clock_for(NodeId node) {
  auto it = clocks_.find(node.value());
  if (it == clocks_.end()) it = clocks_.emplace(node.value(), make_clock(node.value())).first;
  return it->second;
}

Timestamp ClockModel::node_now(NodeId node, Timestamp truth) const {
  const NodeClock& c = clock_for(node);

  // A frozen clock reports the instant it stopped, indefinitely. The node has
  // no way to notice; that is the point.
  const Timestamp base = c.frozen ? c.frozen_at : truth;

  // Drift accumulates against elapsed time. Integer math throughout: nanos are
  // at most ~10^11 for a sixty-second run and ppm is bounded by a few hundred,
  // so the product cannot come close to overflowing int64.
  const auto elapsed = static_cast<std::int64_t>(base.physical);
  const std::int64_t drift = (elapsed / 1'000'000) * c.drift_ppm;

  Timestamp out = base;
  out.physical = offset_apply(base.physical, c.offset_nanos + c.step_nanos + drift);
  return out;
}

TimeInterval ClockModel::node_now_uncertain(NodeId node, Timestamp truth) const {
  const Timestamp t = node_now(node, truth);
  const auto width = static_cast<std::uint64_t>(faults_.declared_uncertainty.nanos());

  Timestamp earliest = t;
  earliest.physical = t.physical > width ? t.physical - width : 0;

  Timestamp latest = t;
  latest.physical = t.physical + width;

  // Note what is NOT happening here: the bracket is built from the *declared*
  // bound, with no reference to the node's real offset. If the real offset is
  // larger, the interval genuinely fails to contain true time and every
  // consumer of it is being lied to -- which is exactly the experiment
  // `violate_declared_bound` sets up.
  return TimeInterval{earliest, latest};
}

bool ClockModel::bound_violated_for(NodeId node) const {
  const NodeClock& c = clock_for(node);
  const std::int64_t total = c.offset_nanos + c.step_nanos;
  const std::int64_t magnitude = total < 0 ? -total : total;
  return magnitude > faults_.declared_uncertainty.nanos();
}

void ClockModel::step(NodeId node, Duration delta) {
  clock_for(node).step_nanos += delta.nanos();
}

void ClockModel::freeze(NodeId node, Timestamp at) {
  NodeClock& c = clock_for(node);
  if (c.frozen) return;
  c.frozen = true;
  c.frozen_at = at;
}

void ClockModel::thaw(NodeId node) { clock_for(node).frozen = false; }

bool ClockModel::frozen(NodeId node) const { return clock_for(node).frozen; }

void ClockModel::reseed_node(NodeId node, std::uint64_t incarnation) {
  // A reboot resyncs the clock. Carrying the old offset across a restart would
  // model a machine that came back with exactly the same broken oscillator,
  // which is neither realistic nor the interesting case.
  clocks_[node.value()] = make_clock(node.value() ^ (incarnation << 32));
}

}  // namespace anvil::sim
