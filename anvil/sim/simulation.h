// Simulation: the whole world for one seed.
//
// Owns the scheduler, the models, the adversary, and one SimRuntime per node.
// Construct it, register each node's boot function, run it, read the digest.
//
// A seed is a complete description of an experiment: it determines the fault
// profile, every latency, every crash time, every torn sector, and every
// scheduling decision. Nothing else is an input.
//
// Nodes are numbered from 1. NodeId{0} is reserved as "unset", because
// Id::valid() treats zero as invalid and a zero-based scheme would make the
// most common node in every test indistinguishable from a default-constructed
// field.

#ifndef ANVIL_SIM_SIMULATION_H_
#define ANVIL_SIM_SIMULATION_H_

#include <cstdint>
#include <memory>
#include <vector>

#include "anvil/sim/buggify_policy.h"
#include "anvil/sim/clock_model.h"
#include "anvil/sim/disk_model.h"
#include "anvil/sim/fault_injector.h"
#include "anvil/sim/fault_profile.h"
#include "anvil/sim/net_model.h"
#include "anvil/sim/process_model.h"
#include "anvil/sim/scheduler.h"
#include "anvil/sim/sim_runtime.h"
#include "anvil/sim/trace.h"

namespace anvil::sim {

struct SimConfig {
  std::uint64_t seed = 1;
  std::uint32_t nodes = 3;
  Duration max_time = Duration::seconds(60);
  FaultProfile faults = FaultProfile::none();
  BuggifyConfig buggify;
  bool record_trace = false;

  // The usual way to build one: everything, including the adversary, drawn
  // from the seed. `nodes` is drawn too unless overridden afterwards.
  static SimConfig from_seed(std::uint64_t seed);

  // Identifies the experiment. Deliberately excludes `seed` and `record_trace`:
  // enabling tracing must not change which experiment this is, or a recorded
  // replay would not be the same run as the silent fleet execution that failed.
  std::uint64_t config_hash() const noexcept;
};

class Simulation {
 public:
  explicit Simulation(SimConfig config);
  ~Simulation();

  Simulation(const Simulation&) = delete;
  Simulation& operator=(const Simulation&) = delete;

  const SimConfig& config() const noexcept { return config_; }
  std::uint32_t node_count() const noexcept { return config_.nodes; }

  // 1-based. Panics on an out-of-range id rather than returning something
  // plausible, since an off-by-one here would silently make two workload tasks
  // share a node.
  Runtime& node(NodeId id);

  // Registers how a node starts itself. Called once before the run and again
  // after every restart, so it must perform recovery rather than assume a blank
  // machine. A workload that cannot be booted twice cannot be crash-tested.
  void set_boot(NodeId id, ProcessModel::BootFn boot);

  // Arm invariants here. Tick- and epoch-class predicates are evaluated inside
  // the run loop; quiesce-class ones run when the simulation settles, which is
  // the only point at which "after the faults stop, does everything converge"
  // is a meaningful question to ask.
  checker::InvariantRegistry& invariants() noexcept { return invariants_; }

  Scheduler& scheduler() noexcept { return *scheduler_; }
  Trace& trace() noexcept { return *trace_; }
  NetworkModel& net() noexcept { return *net_; }
  DiskModel& disk() noexcept { return *disk_; }
  ClockModel& clock() noexcept { return *clock_; }
  ProcessModel& process() noexcept { return *process_; }
  FaultInjector& faults() noexcept { return *faults_; }

  // Arms the adversary and runs to quiescence, the deadline, or a panic.
  RunResult run();

  // Runs for a further `extra` of simulated time beyond wherever the clock now
  // stands. Needed because run()'s budget is measured from time zero, so
  // calling it twice would return instantly the second time -- a trap worth
  // having an explicit method for rather than rediscovering.
  RunResult run_more(Duration extra);

  // Heals every fault and lets the system settle. Liveness assertions are only
  // meaningful under eventual synchrony -- "does it recover once the network
  // stops lying" is a question, "does it make progress during a permanent
  // partition" is not. Returns the result of the settling period.
  RunResult heal_and_settle(Duration grace);

 private:
  SimConfig config_;
  checker::InvariantRegistry invariants_;
  std::unique_ptr<Trace> trace_;
  std::unique_ptr<Scheduler> scheduler_;
  std::unique_ptr<ClockModel> clock_;
  std::unique_ptr<NetworkModel> net_;
  std::unique_ptr<DiskModel> disk_;
  std::unique_ptr<ProcessModel> process_;
  std::unique_ptr<FaultInjector> faults_;
  std::unique_ptr<SimBuggifyPolicy> buggify_;
  std::vector<std::unique_ptr<SimRuntime>> runtimes_;
  bool started_ = false;
};

}  // namespace anvil::sim

#endif  // ANVIL_SIM_SIMULATION_H_
