#include "anvil/sim/fault_injector.h"

namespace anvil::sim {
namespace {

// How often the adversary rolls its dice. Fine enough that a five-second run
// still gets fifty chances at each node, coarse enough that ticking does not
// dominate the event count on a clean profile.
constexpr Duration kTickInterval = Duration::millis(100);
constexpr std::int64_t kTicksPerSecond = 10;

}  // namespace

const char* to_string(FaultKind kind) noexcept {
  switch (kind) {
    case FaultKind::kMessageDrop: return "message_drop";
    case FaultKind::kMessageDuplicate: return "message_duplicate";
    case FaultKind::kMessageReorder: return "message_reorder";
    case FaultKind::kConnectionReset: return "connection_reset";
    case FaultKind::kPartition: return "partition";
    case FaultKind::kHalfOpenLink: return "half_open_link";
    case FaultKind::kBandwidthDelay: return "bandwidth_delay";
    case FaultKind::kProcessCrash: return "process_crash";
    case FaultKind::kProcessPause: return "process_pause";
    case FaultKind::kClockJump: return "clock_jump";
    case FaultKind::kClockFreeze: return "clock_freeze";
    case FaultKind::kClockBoundViolation: return "clock_bound_violation";
    case FaultKind::kDiskTornWrite: return "disk_torn_write";
    case FaultKind::kDiskLostSector: return "disk_lost_sector";
    case FaultKind::kDiskLostDirEntry: return "disk_lost_dir_entry";
    case FaultKind::kDiskBitRot: return "disk_bit_rot";
    case FaultKind::kDiskIoError: return "disk_io_error";
    case FaultKind::kDiskNoSpace: return "disk_no_space";
    case FaultKind::kDiskSlowIo: return "disk_slow_io";
    case FaultKind::kCount: return "invalid";
  }
  return "unknown";
}

FaultInjector::FaultInjector(Scheduler* scheduler, NetworkModel* net, DiskModel* disk,
                             ClockModel* clock, ProcessModel* process, std::uint64_t seed,
                             FaultProfile profile, std::uint32_t nodes)
    : scheduler_(scheduler),
      net_(net),
      disk_(disk),
      clock_(clock),
      process_(process),
      rng_(DeterministicRandom{seed}.fork(RandomDomain::kProcess, 0xFA17)),
      profile_(profile),
      nodes_(nodes) {}

bool FaultInjector::needs_ticking() const {
  // A recurring tick keeps the event queue permanently non-empty, which means
  // the run can never quiesce and every seed costs a full max_time. When no
  // per-node fault is possible there is nothing to roll for, so the ticker is
  // never armed and a clean profile finishes as soon as the workload does.
  return profile_.process.crash_per_second.possible() ||
         profile_.process.pause_per_second.possible() || profile_.clock.jump.possible() ||
         profile_.clock.freeze.possible() || profile_.disk.bit_rot.possible();
}

void FaultInjector::start() {
  armed_ = true;
  if (needs_ticking()) {
    scheduler_->at(kTickInterval, NodeId{}, EventKind::kNote, "fault_tick", [this]() { tick(); });
  }
  schedule_partitions();
}

// ---------------------------------------------------------------------------
// per-node dice
// ---------------------------------------------------------------------------

void FaultInjector::tick() {
  if (!armed_) return;  // disarmed: stop rolling, and stop rescheduling

  for (std::uint32_t i = 1; i <= nodes_; ++i) {
    const NodeId node{i};

    // Rates in the profile are per simulated second; the tick is finer, so the
    // denominator is scaled rather than the numerator. Scaling the numerator
    // would round small probabilities to zero and silently disable them.
    const auto per_tick = [](Chance c) {
      return Chance{c.numerator, c.denominator * static_cast<std::uint32_t>(kTicksPerSecond)};
    };

    if (process_->alive(node) && per_tick(profile_.process.crash_per_second).roll(rng_)) {
      const Duration delay = rng_.uniform_duration(profile_.process.min_restart_delay,
                                                   profile_.process.max_restart_delay);
      process_->crash(node, delay);
      continue;  // a crashed node is not also paused or clock-stepped this tick
    }

    if (!process_->alive(node)) continue;

    if (per_tick(profile_.process.pause_per_second).roll(rng_)) {
      process_->pause(node,
                      rng_.uniform_duration(profile_.process.min_pause, profile_.process.max_pause));
    }

    if (profile_.clock.jump.roll(rng_)) {
      const Duration magnitude =
          rng_.uniform_duration(Duration{0}, profile_.clock.max_jump) * (rng_.coin() ? 1 : -1);
      clock_->step(node, magnitude);
      ++clock_jumps_;
    }

    if (profile_.clock.freeze.roll(rng_) && !clock_->frozen(node)) {
      clock_->freeze(node, scheduler_->now());
      ++clock_freezes_;
      const Duration held = rng_.uniform_duration(Duration::millis(1), profile_.clock.max_freeze);
      // A frozen clock thaws on its own; the node never learns it happened,
      // which is the whole reason this fault is dangerous.
      scheduler_->at(held, node, EventKind::kNote, "clock_thaw",
                     [this, node]() { clock_->thaw(node); });
    }

    if (profile_.disk.bit_rot.roll(rng_)) disk_->scrub_corrupt(node);
  }

  scheduler_->at(kTickInterval, NodeId{}, EventKind::kNote, "fault_tick", [this]() { tick(); });
}

// ---------------------------------------------------------------------------
// partitions
// ---------------------------------------------------------------------------

void FaultInjector::schedule_partitions() {
  if (profile_.partition.style == PartitionStyle::kNone) return;
  scheduler_->at(profile_.partition.first_at, NodeId{}, EventKind::kNote, "partition_begin",
                 [this]() { apply_partition(); });
}

void FaultInjector::apply_partition() {
  if (!armed_ || partition_active_) return;

  // A partition needs two sides, so it needs two nodes.
  //
  // Without this the loop below spins forever on a single-node cluster: it
  // redraws until both groups are non-empty, and with one node one of them
  // never is. Nothing fails and nothing crashes -- the simulation simply stops
  // advancing, which reads from the outside as a hung test and gets debugged as
  // one, several layers away from here (ANV-0028). Single-node configurations
  // are not exotic; P4's whole workload is one.
  if (nodes_ < 2) return;

  partition_active_ = true;
  ++partitions_applied_;

  const LinkState state = profile_.partition.style == PartitionStyle::kHalfOpen
                              ? LinkState::kHalfOpen
                              : LinkState::kDown;

  if (profile_.partition.style == PartitionStyle::kMajorityIsolating) {
    // One node severed from everyone. The classic test for whether a minority
    // can be talked into believing it is still in charge.
    const NodeId victim{1 + rng_.uniform(nodes_)};
    for (std::uint32_t i = 1; i <= nodes_; ++i) {
      const NodeId other{i};
      if (other == victim) continue;
      net_->set_link(victim, other, state);
      net_->set_link(other, victim, state);
    }
  } else {
    // Split into two non-empty groups by coin flip, retrying the degenerate
    // draws rather than biasing the assignment.
    std::vector<NodeId> left;
    std::vector<NodeId> right;
    do {
      left.clear();
      right.clear();
      for (std::uint32_t i = 1; i <= nodes_; ++i) {
        (rng_.coin() ? left : right).push_back(NodeId{i});
      }
    } while (left.empty() || right.empty());

    for (const NodeId a : left) {
      for (const NodeId b : right) {
        net_->set_link(a, b, state);
        // Asymmetric leaves the reverse direction up, so each side has a
        // different view of who is reachable. A node that can send but not
        // receive still looks healthy to itself and keeps asserting whatever it
        // believed before the split -- which is where split-brain comes from,
        // and which a symmetric partition never produces.
        if (profile_.partition.style != PartitionStyle::kAsymmetric) {
          net_->set_link(b, a, state);
        }
      }
    }
  }

  scheduler_->digest().mix(std::string_view{"partition"}).mix(partitions_applied_);
  if (scheduler_->trace().recording()) {
    const TraceField fields[] = {{"style", static_cast<std::uint64_t>(profile_.partition.style)},
                                 {"n", partitions_applied_}};
    scheduler_->trace().emit(scheduler_->now(), NodeId{}, EventKind::kNote, "partition_begin",
                             fields);
  }

  scheduler_->at(profile_.partition.duration, NodeId{}, EventKind::kNote, "partition_heal",
                 [this]() { heal_partition(); });
}

void FaultInjector::heal_partition() {
  net_->heal_all();
  partition_active_ = false;
  scheduler_->digest().mix(std::string_view{"heal"});

  if (armed_ && profile_.partition.style == PartitionStyle::kFlapping) {
    scheduler_->at(profile_.partition.period, NodeId{}, EventKind::kNote, "partition_begin",
                   [this]() { apply_partition(); });
  }
}

// ---------------------------------------------------------------------------
// coverage accounting
// ---------------------------------------------------------------------------

FaultSummary FaultInjector::summary() const {
  FaultSummary s;
  s.net = net_->stats();
  s.disk = disk_->stats();
  s.process = process_->stats();
  s.clock_jumps = clock_jumps_;
  s.clock_freezes = clock_freezes_;
  for (std::uint32_t i = 1; i <= nodes_; ++i) {
    if (clock_->bound_violated_for(NodeId{i})) ++s.nodes_outside_declared_bound;
  }
  return s;
}

bool FaultInjector::enabled(FaultKind kind) const {
  switch (kind) {
    case FaultKind::kMessageDrop: return profile_.net.drop.possible();
    case FaultKind::kMessageDuplicate: return profile_.net.duplicate.possible();
    case FaultKind::kMessageReorder: return profile_.net.allow_reorder;
    case FaultKind::kConnectionReset: return profile_.net.reset.possible();
    case FaultKind::kPartition:
      return profile_.partition.style != PartitionStyle::kNone &&
             profile_.partition.style != PartitionStyle::kHalfOpen;
    case FaultKind::kHalfOpenLink: return profile_.partition.style == PartitionStyle::kHalfOpen;
    case FaultKind::kBandwidthDelay: return profile_.net.bandwidth_bytes_per_sec > 0;
    case FaultKind::kProcessCrash: return profile_.process.crash_per_second.possible();
    case FaultKind::kProcessPause: return profile_.process.pause_per_second.possible();
    case FaultKind::kClockJump: return profile_.clock.jump.possible();
    case FaultKind::kClockFreeze: return profile_.clock.freeze.possible();
    case FaultKind::kClockBoundViolation: return profile_.clock.violate_declared_bound;
    // The three crash-resolution outcomes are only reachable if something
    // actually crashes. Reporting them as "enabled but never fired" on a
    // crash-free profile would be noise, and noise is how a coverage check gets
    // ignored and then deleted.
    case FaultKind::kDiskTornWrite:
      return profile_.disk.torn_write.possible() && profile_.disk.page_cache &&
             profile_.process.crash_per_second.possible();
    case FaultKind::kDiskLostSector:
      return profile_.disk.page_cache && profile_.process.crash_per_second.possible();
    case FaultKind::kDiskLostDirEntry:
      return profile_.process.crash_per_second.possible();
    case FaultKind::kDiskBitRot: return profile_.disk.bit_rot.possible();
    case FaultKind::kDiskIoError: return profile_.disk.io_error.possible();
    case FaultKind::kDiskNoSpace: return profile_.disk.capacity_bytes > 0;
    case FaultKind::kDiskSlowIo: return profile_.disk.slow_io.possible();
    case FaultKind::kCount: return false;
  }
  return false;
}

bool FaultInjector::fired(FaultKind kind) const {
  const NetStats& n = net_->stats();
  const DiskStats& d = disk_->stats();
  const ProcessStats& p = process_->stats();
  switch (kind) {
    case FaultKind::kMessageDrop: return n.dropped_by_loss > 0;
    case FaultKind::kMessageDuplicate: return n.duplicated > 0;
    case FaultKind::kMessageReorder: return n.reordered > 0;
    case FaultKind::kConnectionReset: return n.reset > 0;
    case FaultKind::kPartition: return n.dropped_by_partition > 0;
    case FaultKind::kHalfOpenLink: return n.dropped_by_half_open > 0;
    case FaultKind::kBandwidthDelay: return n.bandwidth_delays > 0;
    case FaultKind::kProcessCrash: return p.crashes > 0;
    case FaultKind::kProcessPause: return p.pauses > 0;
    case FaultKind::kClockJump: return clock_jumps_ > 0;
    case FaultKind::kClockFreeze: return clock_freezes_ > 0;
    case FaultKind::kClockBoundViolation: {
      for (std::uint32_t i = 1; i <= nodes_; ++i) {
        if (clock_->bound_violated_for(NodeId{i})) return true;
      }
      return false;
    }
    case FaultKind::kDiskTornWrite: return d.sectors_torn > 0;
    case FaultKind::kDiskLostSector: return d.sectors_lost > 0;
    case FaultKind::kDiskLostDirEntry: return d.files_lost_to_entry > 0;
    case FaultKind::kDiskBitRot: return d.bit_rots > 0;
    case FaultKind::kDiskIoError: return d.io_errors > 0;
    case FaultKind::kDiskNoSpace: return d.no_space > 0;
    case FaultKind::kDiskSlowIo: return d.slow_ios > 0;
    case FaultKind::kCount: return false;
  }
  return false;
}

std::vector<FaultKind> FaultInjector::unexercised_kinds() const {
  std::vector<FaultKind> out;
  for (std::uint8_t i = 0; i < static_cast<std::uint8_t>(FaultKind::kCount); ++i) {
    const auto kind = static_cast<FaultKind>(i);
    if (enabled(kind) && !fired(kind)) out.push_back(kind);
  }
  return out;
}

std::vector<FaultKind> FaultInjector::exercised_kinds() const {
  std::vector<FaultKind> out;
  for (std::uint8_t i = 0; i < static_cast<std::uint8_t>(FaultKind::kCount); ++i) {
    const auto kind = static_cast<FaultKind>(i);
    if (fired(kind)) out.push_back(kind);
  }
  return out;
}

}  // namespace anvil::sim
