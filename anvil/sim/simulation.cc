#include "anvil/sim/simulation.h"

#include <utility>

#include "anvil/core/digest.h"

namespace anvil::sim {

SimConfig SimConfig::from_seed(std::uint64_t seed) {
  SimConfig cfg;
  cfg.seed = seed;
  cfg.faults = FaultProfile::draw(seed);

  auto rng = DeterministicRandom{seed}.fork(RandomDomain::kScheduler, 0x5112E);
  cfg.nodes = static_cast<std::uint32_t>(rng.uniform_range(3, 7));
  cfg.max_time = Duration::seconds(rng.uniform_range(10, 60));
  cfg.buggify.enable_pct = static_cast<std::uint32_t>(rng.uniform_range(0, 100));
  cfg.buggify.fire_pct = static_cast<std::uint32_t>(rng.uniform_range(1, 50));
  return cfg;
}

std::uint64_t SimConfig::config_hash() const noexcept {
  Digest d;
  d.mix(static_cast<std::uint64_t>(nodes))
      .mix(max_time)
      .mix(faults.hash())
      .mix(static_cast<std::uint64_t>(buggify.enable_pct))
      .mix(static_cast<std::uint64_t>(buggify.fire_pct));
  return d.low();
}

Simulation::Simulation(SimConfig config) : config_(config) {
  trace_ = std::make_unique<Trace>(config_.record_trace);
  scheduler_ = std::make_unique<Scheduler>(config_.seed, trace_.get());
  scheduler_->set_invariants(&invariants_);
  clock_ = std::make_unique<ClockModel>(config_.seed, config_.faults.clock, config_.nodes);
  net_ = std::make_unique<NetworkModel>(scheduler_.get(), config_.seed, config_.faults.net);
  disk_ = std::make_unique<DiskModel>(scheduler_.get(), config_.seed, config_.faults.disk);
  process_ = std::make_unique<ProcessModel>(scheduler_.get(), net_.get(), disk_.get(),
                                            clock_.get(), config_.faults.process);
  faults_ = std::make_unique<FaultInjector>(scheduler_.get(), net_.get(), disk_.get(),
                                            clock_.get(), process_.get(), config_.seed,
                                            config_.faults, config_.nodes);
  buggify_ =
      std::make_unique<SimBuggifyPolicy>(config_.seed, config_.buggify, scheduler_.get());

  // Global, because the ANVIL_BUGGIFY macro cannot reach a Runtime from an
  // arbitrary line of protocol code. Safe only because the core is
  // single-threaded and exactly one Simulation is live at a time; worth
  // remembering if anyone ever tries to run two seeds concurrently in one
  // process.
  set_buggify_policy(buggify_.get());

  runtimes_.reserve(config_.nodes);
  for (std::uint32_t i = 1; i <= config_.nodes; ++i) {
    runtimes_.push_back(std::make_unique<SimRuntime>(NodeId{i}, scheduler_.get(), net_.get(),
                                                     disk_.get(), clock_.get(), config_.seed));
  }
}

Simulation::~Simulation() {
  // Order matters. Coroutine frames die first, while the models they might
  // touch are still alive; then the policy is uninstalled so a stray BUGGIFY
  // evaluation during teardown cannot reach freed memory.
  if (scheduler_) scheduler_->destroy_all_tasks();
  set_buggify_policy(nullptr);
}

Runtime& Simulation::node(NodeId id) {
  const std::uint64_t index = id.value();
  if (index < 1 || index > config_.nodes) {
    throw SimulationPanic("Simulation::node() called with an out-of-range NodeId");
  }
  return *runtimes_[index - 1];
}

void Simulation::set_boot(NodeId id, ProcessModel::BootFn boot) {
  process_->set_boot(id, std::move(boot));
}

RunResult Simulation::run() {
  if (!started_) {
    started_ = true;
    faults_->start();
  }
  return scheduler_->run(config_.max_time);
}

RunResult Simulation::run_more(Duration extra) {
  if (!started_) {
    started_ = true;
    faults_->start();
  }
  const Timestamp deadline = scheduler_->now().advanced_by(extra);
  return scheduler_->run(Duration{static_cast<std::int64_t>(deadline.physical)});
}

RunResult Simulation::heal_and_settle(Duration grace) {
  // Eventual synchrony. Everything the adversary did is undone: links come
  // back, per-message loss stops, the disk stops returning errors, clocks
  // unfreeze, dead nodes restart. Anything still broken after the grace period
  // is the system's own doing, not the network's.
  //
  // Healing the *links* alone is not enough, and getting that wrong is subtle:
  // drop, duplicate, reset, EIO and bit rot are properties of the models rather
  // than of the link table, so a partially-healed cluster keeps losing messages
  // and failing reads forever. Liveness then looks violated when nothing is
  // actually wrong with the protocol.
  net_->heal_all();
  net_->stop_injecting();
  disk_->stop_injecting();
  for (std::uint32_t i = 1; i <= config_.nodes; ++i) {
    const NodeId id{i};
    clock_->thaw(id);
    scheduler_->resume_node(id);
    if (!process_->alive(id)) process_->restart(id);
  }

  // The injector keeps ticking on its own schedule, so its dice have to stop
  // rolling too -- otherwise "healed" would last exactly one tick and the
  // liveness question would be unanswerable. Disarming rather than replacing
  // it preserves the coverage counters that prove the run's faults ever fired.
  faults_->disarm();

  // run() interprets its argument as an absolute deadline measured from time
  // zero, so the remaining grace has to be expressed that way.
  const Timestamp deadline = scheduler_->now().advanced_by(grace);
  RunResult result = scheduler_->run(Duration{static_cast<std::int64_t>(deadline.physical)});

  // Quiesce-class invariants run only here, and only here is where they mean
  // anything. "Every replica has converged" is false by design during a
  // partition; asking it before the adversary has stopped would produce a
  // stream of violations that are correct behaviour, and a check that reports
  // correct behaviour as a failure gets switched off.
  if (result.ok()) {
    auto fired = invariants_.evaluate(checker::CostClass::kQuiesce, scheduler_->now(),
                                      result.events);
    if (!fired.empty()) {
      result.reason = StopReason::kInvariantViolated;
      result.violations.insert(result.violations.end(), fired.begin(), fired.end());
    }
  }
  return result;
}

}  // namespace anvil::sim
