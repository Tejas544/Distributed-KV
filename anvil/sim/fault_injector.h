// The adversary's schedule.
//
// The models know *how* to break things; this decides *when*. It ticks on a
// fixed simulated interval, rolling each node's crash, pause, clock-jump,
// clock-freeze and bit-rot dice, and it drives the partition plan independently
// on its own timeline.
//
// The most useful thing in this file is not the injection. It is
// `unexercised_kinds()`.
//
// A fault that never fires is a fault that is not being tested, and the failure
// mode is silent: the profile says drop=3%, the run reports green, and nobody
// notices that the workload finished before a single drop landed. Over a fleet,
// every enabled fault kind must be observed to have actually happened, and --
// the stronger form in INV-SIM-08 -- must be observed to have caused something.
// That check is the difference between "we injected faults" and "we know our
// fault injection works", and it is the same discipline as the negative control
// on the hermeticity gate.

#ifndef ANVIL_SIM_FAULT_INJECTOR_H_
#define ANVIL_SIM_FAULT_INJECTOR_H_

#include <cstdint>
#include <vector>

#include "anvil/core/random.h"
#include "anvil/sim/clock_model.h"
#include "anvil/sim/disk_model.h"
#include "anvil/sim/fault_profile.h"
#include "anvil/sim/net_model.h"
#include "anvil/sim/process_model.h"
#include "anvil/sim/scheduler.h"

namespace anvil::sim {

enum class FaultKind : std::uint8_t {
  kMessageDrop,
  kMessageDuplicate,
  kMessageReorder,
  kConnectionReset,
  kPartition,
  kHalfOpenLink,
  kBandwidthDelay,
  kProcessCrash,
  kProcessPause,
  kClockJump,
  kClockFreeze,
  kClockBoundViolation,
  kDiskTornWrite,
  kDiskLostSector,
  kDiskLostDirEntry,
  kDiskBitRot,
  kDiskIoError,
  kDiskNoSpace,
  kDiskSlowIo,
  kCount,
};

const char* to_string(FaultKind kind) noexcept;

struct FaultSummary {
  NetStats net;
  DiskStats disk;
  ProcessStats process;
  std::uint64_t clock_jumps = 0;
  std::uint64_t clock_freezes = 0;
  std::uint64_t nodes_outside_declared_bound = 0;
};

class FaultInjector {
 public:
  FaultInjector(Scheduler* scheduler, NetworkModel* net, DiskModel* disk, ClockModel* clock,
                ProcessModel* process, std::uint64_t seed, FaultProfile profile,
                std::uint32_t nodes);

  // Arms the schedule. Nothing happens before this is called, so a test can
  // construct a hostile profile and still run a clean control.
  void start();

  // Stops rolling dice without discarding what has already been counted.
  // Needed for eventual-synchrony testing: "healed" has to mean healed, not
  // healed-until-the-next-tick. Constructing a fresh injector instead would
  // reset the coverage counters and quietly lose the evidence that the run's
  // faults ever fired.
  void disarm() noexcept { armed_ = false; }

  FaultSummary summary() const;

  // Fault kinds the profile enabled but that never actually happened during
  // this run. Empty is the goal for a long run; a fleet-wide report of these is
  // how a dead fault knob gets noticed.
  std::vector<FaultKind> unexercised_kinds() const;

  // Fault kinds that did fire. Used by the fleet to prove coverage.
  std::vector<FaultKind> exercised_kinds() const;

 private:
  bool needs_ticking() const;
  void tick();
  void schedule_partitions();
  void apply_partition();
  void heal_partition();
  bool enabled(FaultKind kind) const;
  bool fired(FaultKind kind) const;

  Scheduler* scheduler_;
  NetworkModel* net_;
  DiskModel* disk_;
  ClockModel* clock_;
  ProcessModel* process_;
  DeterministicRandom rng_;
  FaultProfile profile_;
  std::uint32_t nodes_;

  std::uint64_t clock_jumps_ = 0;
  std::uint64_t clock_freezes_ = 0;
  std::uint64_t partitions_applied_ = 0;
  bool partition_active_ = false;
  bool armed_ = false;
};

}  // namespace anvil::sim

#endif  // ANVIL_SIM_FAULT_INJECTOR_H_
