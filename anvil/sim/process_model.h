// Crash, restart, and pause.
//
// A crash is not an exception and not a clean shutdown. Nothing gets to run: no
// destructors in the simulated program, no flush, no goodbye message. Volatile
// state evaporates, in-flight work vanishes, buffered inbound messages are
// gone, and the disk keeps only what was fsynced. The node comes back later
// with a new incarnation number, a resynced clock, and whatever recovery can
// reconstruct from durable storage.
//
// A pause is the opposite kind of nasty. The node keeps everything -- memory,
// data, open connections, its belief about who the leader is -- and simply
// stops running for a while. Then it resumes, convinced no time has passed,
// still holding a lease that expired four seconds ago. Crashes are honest;
// pauses lie. In production this is a stop-the-world collection, a hypervisor
// migration, or a laptop lid closing, and it is the fault that breaks
// lease-based protocols that survive everything else.
//
// Ordering inside crash() is load-bearing and is documented at the call site.
// Getting it wrong is a use-after-free that only appears under crash-heavy
// seeds, which is exactly the kind of bug that takes a weekend.

#ifndef ANVIL_SIM_PROCESS_MODEL_H_
#define ANVIL_SIM_PROCESS_MODEL_H_

#include <cstdint>
#include <functional>
#include <map>

#include "anvil/core/types.h"
#include "anvil/sim/clock_model.h"
#include "anvil/sim/disk_model.h"
#include "anvil/sim/fault_profile.h"
#include "anvil/sim/net_model.h"
#include "anvil/sim/scheduler.h"

namespace anvil::sim {

struct ProcessStats {
  std::uint64_t crashes = 0;
  std::uint64_t restarts = 0;
  std::uint64_t pauses = 0;
  Duration total_paused;
  Duration total_down;
};

class ProcessModel {
 public:
  // Spawns a node's tasks from nothing. Called once at startup and again after
  // every restart, so it must be safe to run against durable state left behind
  // by a previous incarnation -- which is to say, it must perform recovery.
  using BootFn = std::function<void()>;

  ProcessModel(Scheduler* scheduler, NetworkModel* net, DiskModel* disk, ClockModel* clock,
               ProcessFaults faults);

  void set_boot(NodeId node, BootFn boot);

  bool alive(NodeId node) const;
  bool paused(NodeId node) const;
  std::uint64_t incarnation(NodeId node) const;

  // Kills the node now and schedules its restart. A restart_delay long enough
  // to outlast the run models permanent loss.
  void crash(NodeId node, Duration restart_delay);
  void restart(NodeId node);
  void pause(NodeId node, Duration duration);

  const ProcessStats& stats() const noexcept { return stats_; }

 private:
  struct NodeState {
    BootFn boot;
    bool alive = true;
    std::uint64_t incarnation = 1;
    Timestamp down_since;
  };

  NodeState& state(NodeId node) { return nodes_[node.value()]; }
  const NodeState* find(NodeId node) const;

  Scheduler* scheduler_;
  NetworkModel* net_;
  DiskModel* disk_;
  ClockModel* clock_;
  ProcessFaults faults_;
  std::map<std::uint64_t, NodeState> nodes_;
  ProcessStats stats_;
};

}  // namespace anvil::sim

#endif  // ANVIL_SIM_PROCESS_MODEL_H_
