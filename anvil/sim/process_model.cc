#include "anvil/sim/process_model.h"

#include <utility>

namespace anvil::sim {

ProcessModel::ProcessModel(Scheduler* scheduler, NetworkModel* net, DiskModel* disk,
                           ClockModel* clock, ProcessFaults faults)
    : scheduler_(scheduler), net_(net), disk_(disk), clock_(clock), faults_(faults) {}

void ProcessModel::set_boot(NodeId node, BootFn boot) {
  state(node).boot = std::move(boot);
}

const ProcessModel::NodeState* ProcessModel::find(NodeId node) const {
  const auto it = nodes_.find(node.value());
  return it == nodes_.end() ? nullptr : &it->second;
}

bool ProcessModel::alive(NodeId node) const {
  const NodeState* s = find(node);
  return s == nullptr || s->alive;
}

bool ProcessModel::paused(NodeId node) const { return scheduler_->node_paused(node); }

std::uint64_t ProcessModel::incarnation(NodeId node) const {
  const NodeState* s = find(node);
  return s == nullptr ? 1 : s->incarnation;
}

void ProcessModel::crash(NodeId node, Duration restart_delay) {
  NodeState& s = state(node);
  if (!s.alive) return;

  s.alive = false;
  s.down_since = scheduler_->now();
  ++stats_.crashes;

  // ---- ordering below is load-bearing ------------------------------------
  //
  // 1. Purge the node's queued events first. They capture coroutine handles by
  //    value; if the frames were destroyed while a resume event was still
  //    queued, dispatching it would resume freed memory.
  scheduler_->drop_events_for(node);

  // 2. Clear network endpoints before destroying frames, for the same reason:
  //    a parked receiver's handle is stored in the endpoint, and a later
  //    delivery would try to resume it.
  net_->reset_node(node);

  // 3. Now the frames can go. No destructors of the *simulated* program run in
  //    any meaningful sense -- the machine is gone. Only durable state survives.
  scheduler_->destroy_tasks_for(node);

  // 4. A crash ends any pause. The frozen process no longer exists.
  scheduler_->resume_node(node);

  // 5. Resolve the disk: unsynced sectors take their chances, files whose
  //    directory entry was never persisted disappear, pending deletions that
  //    were never made durable come back.
  disk_->crash_node(node);

  if (scheduler_->trace().recording()) {
    const TraceField fields[] = {{"incarnation", s.incarnation}};
    scheduler_->trace().emit(scheduler_->now(), node, EventKind::kNote, "crash", fields);
  }
  scheduler_->digest().mix(node).mix(s.incarnation).mix(std::string_view{"crash"});

  // kNote rather than kTimer so the restart is never held by a pause: the
  // process being paused when it died has no bearing on when the new one boots.
  scheduler_->at(restart_delay, node, EventKind::kNote, "restart",
                 [this, node]() { restart(node); });
}

void ProcessModel::restart(NodeId node) {
  NodeState& s = state(node);
  if (s.alive) return;

  s.alive = true;
  ++s.incarnation;
  ++stats_.restarts;
  stats_.total_down = stats_.total_down + Duration{static_cast<std::int64_t>(
                                              scheduler_->now().physical - s.down_since.physical)};

  // A reboot resyncs the clock. Carrying the previous incarnation's skew across
  // a restart would model a machine that came back with the same broken
  // oscillator -- possible, but not the interesting case, and it would hide the
  // fact that offsets are supposed to be re-drawn.
  clock_->reseed_node(node, s.incarnation);

  scheduler_->digest().mix(node).mix(s.incarnation).mix(std::string_view{"restart"});
  if (scheduler_->trace().recording()) {
    const TraceField fields[] = {{"incarnation", s.incarnation}};
    scheduler_->trace().emit(scheduler_->now(), node, EventKind::kNote, "restart", fields);
  }

  // The boot function must perform recovery. Everything it needs is on disk, or
  // it is gone.
  if (s.boot) s.boot();
}

void ProcessModel::pause(NodeId node, Duration duration) {
  if (!alive(node)) return;
  ++stats_.pauses;
  stats_.total_paused = stats_.total_paused + duration;

  scheduler_->pause_node(node, duration);
  scheduler_->digest().mix(node).mix(duration).mix(std::string_view{"pause"});
  if (scheduler_->trace().recording()) {
    const TraceField fields[] = {{"nanos", static_cast<std::uint64_t>(duration.nanos())}};
    scheduler_->trace().emit(scheduler_->now(), node, EventKind::kNote, "pause", fields);
  }
}

}  // namespace anvil::sim
