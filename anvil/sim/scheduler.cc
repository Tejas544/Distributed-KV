#include "anvil/sim/scheduler.h"

#include <utility>
#include <vector>

namespace anvil::sim {

const char* to_string(StopReason reason) noexcept {
  switch (reason) {
    case StopReason::kQuiesced: return "quiesced";
    case StopReason::kDeadlineHit: return "deadline";
    case StopReason::kStopRequested: return "stopped";
    case StopReason::kPanic: return "panic";
  }
  return "unknown";
}

Scheduler::Scheduler(std::uint64_t seed, Trace* trace)
    : rng_(DeterministicRandom{seed}.fork(RandomDomain::kScheduler)), trace_(trace) {}

Scheduler::~Scheduler() { destroy_all_tasks(); }

void Scheduler::destroy_all_tasks() noexcept {
  // Tasks still parked on a recv that will never arrive are the normal end
  // state of a quiesced run, not an error. Their frames still have to go, and
  // destroying a suspended coroutine is well-defined: it runs the destructors
  // of everything in scope, which in turn destroys any nested Task frames the
  // await chain owns.
  for (auto& [id, root] : roots_) {
    if (root.handle) root.handle.destroy();
  }
  roots_.clear();

  // Queued actions capture coroutine handles by value. Once the frames are
  // gone, any surviving action would resume a destroyed coroutine.
  queue_.clear();
  index_.clear();
}

std::uint64_t Scheduler::at(Duration delay, NodeId node, EventKind kind, const char* name,
                            std::function<void()> action, std::uint64_t caused_by) {
  const std::uint64_t seq = next_seq_++;
  Timestamp when = delay.nanos() <= 0 ? now_.next_logical() : now_.advanced_by(delay);

  // Work scheduled *during* a pause has to be held too, not just the work that
  // was already queued when the pause began. A delivery that arrives mid-pause
  // will schedule a wakeup for the frozen node, and that wakeup must wait.
  if (is_execution_event(kind)) {
    const auto paused = paused_until_.find(node.value());
    if (paused != paused_until_.end() && when < paused->second) {
      when = paused->second;
    }
  }

  // Zero-delay events land at the same physical instant but a later logical
  // tick, which keeps the key strictly ordered without needing the physical
  // clock to move. yield() depends on this: it must resume *after* everything
  // already queued for now, not before it.
  const EventKey key{when, seq};

  Event ev;
  ev.node = node;
  ev.kind = kind;
  ev.name = name;
  ev.caused_by = caused_by;
  ev.action = std::move(action);

  queue_.emplace(key, std::move(ev));
  index_.emplace(seq, key);
  return seq;
}

void Scheduler::cancel(std::uint64_t event_id) {
  const auto it = index_.find(event_id);
  if (it == index_.end()) return;  // already fired, or never existed
  queue_.erase(it->second);
  index_.erase(it);
}

void Scheduler::spawn(NodeId node, Task<void> task) {
  if (!task.valid()) return;
  const std::uint64_t root_id = next_root_id_++;
  auto handle = task.release();
  roots_.emplace(root_id, Root{node, handle});

  at(Duration{0}, node, EventKind::kSpawn, "spawn", [handle]() {
    if (!handle.done()) handle.resume();
  });
}

void Scheduler::drop_events_for(NodeId node) {
  for (auto it = queue_.begin(); it != queue_.end();) {
    if (it->second.node == node) {
      index_.erase(it->first.seq);
      it = queue_.erase(it);
    } else {
      ++it;
    }
  }
}

void Scheduler::destroy_tasks_for(NodeId node) noexcept {
  for (auto it = roots_.begin(); it != roots_.end();) {
    if (it->second.node != node) {
      ++it;
      continue;
    }
    // A crash is not a shutdown: no destructors of the *simulated* program run,
    // because the machine is gone. Destroying the coroutine frame does run C++
    // destructors of in-scope objects, which is unavoidable -- we have to
    // reclaim the memory -- but nothing in core/ may use those to persist
    // state, which is why durability lives entirely in the disk model.
    if (it->second.handle) it->second.handle.destroy();
    it = roots_.erase(it);
  }
}

void Scheduler::pause_node(NodeId node, Duration duration) {
  const Timestamp until = now_.advanced_by(duration);
  auto& current = paused_until_[node.value()];
  if (until > current) current = until;

  // Shift the node's already-queued execution events past the end of the
  // pause. Re-keying means extracting and reinserting: the queue is ordered by
  // (time, seq), and mutating the key in place would corrupt the ordering.
  std::vector<std::pair<EventKey, Event>> moved;
  for (auto it = queue_.begin(); it != queue_.end();) {
    if (it->second.node == node && is_execution_event(it->second.kind) &&
        it->first.when < until) {
      moved.emplace_back(it->first, std::move(it->second));
      index_.erase(it->first.seq);
      it = queue_.erase(it);
    } else {
      ++it;
    }
  }

  for (auto& [key, event] : moved) {
    // Keep the original seq so relative order among the resumed events is
    // preserved. A pause reorders nothing; it only delays.
    const EventKey shifted{until, key.seq};
    index_.emplace(key.seq, shifted);
    queue_.emplace(shifted, std::move(event));
  }
}

void Scheduler::resume_node(NodeId node) {
  // A crash ends a pause: the frozen process is gone, and the restarted one has
  // no memory of having been stopped. Leaving the pause in place would silently
  // delay the new incarnation's first events.
  paused_until_.erase(node.value());
}

bool Scheduler::node_paused(NodeId node) const {
  const auto it = paused_until_.find(node.value());
  return it != paused_until_.end() && now_ < it->second;
}

Timestamp Scheduler::paused_until(NodeId node) const {
  const auto it = paused_until_.find(node.value());
  return it == paused_until_.end() ? Timestamp{} : it->second;
}

void Scheduler::reap_roots() {
  for (auto it = roots_.begin(); it != roots_.end();) {
    auto handle = it->second.handle;
    if (!handle || !handle.done()) {
      ++it;
      continue;
    }
    // A root task's exception has nobody to propagate to -- no one awaits it --
    // so without this check a panic inside a spawned task would be swallowed
    // and the run would report a clean quiesce.
    if (handle.promise().error && !panicked_) {
      panicked_ = true;
      try {
        std::rethrow_exception(handle.promise().error);
      } catch (const std::exception& e) {
        panic_message_ = e.what();
      } catch (...) {
        panic_message_ = "unknown exception in spawned task";
      }
    }
    handle.destroy();
    it = roots_.erase(it);
  }
}

RunResult Scheduler::run(Duration max_time) {
  const Timestamp deadline = Timestamp{}.advanced_by(max_time);
  StopReason reason = StopReason::kQuiesced;

  while (!queue_.empty()) {
    if (stop_requested_) {
      reason = StopReason::kStopRequested;
      break;
    }

    // Peek before extracting. The first version pulled the event out and *then*
    // noticed the deadline had passed, discarding it -- which silently orphaned
    // whatever coroutine it was going to resume. Any code that called run()
    // twice, or ran to a deadline and then healed and continued, would find one
    // task permanently wedged with no pending work and no way to tell why.
    // Fault injection surfaced this as "one node never finishes recovery"; the
    // node was fine, its wakeup had been thrown away.
    if (queue_.begin()->first.when > deadline) {
      reason = StopReason::kDeadlineHit;
      break;
    }

    auto node_handle = queue_.extract(queue_.begin());
    const EventKey key = node_handle.key();
    Event ev = std::move(node_handle.mapped());
    index_.erase(key.seq);

    now_ = key.when;
    ++tick_;

    // The digest covers scheduling decisions and message content (mixed in by
    // the network model at send time), not individual RNG draws. Digesting
    // every draw would be strictly more sensitive but far noisier: what we
    // actually care about is whether the two runs *did the same things*, and a
    // divergent draw changes what happens next, which this catches one event
    // later at most.
    digest_.mix(key.when)
        .mix(key.seq)
        .mix(ev.node)
        .mix(static_cast<std::uint64_t>(ev.kind));

    if (trace_->recording()) {
      const TraceField fields[] = {{"seq", key.seq}, {"tick", tick_}};
      trace_->emit(now_, ev.node, ev.kind, ev.name, fields, ev.caused_by);
    }

    try {
      ev.action();
    } catch (const SimulationPanic& e) {
      // Thrown from a plain callback rather than a coroutine, so it unwinds
      // directly to here.
      panicked_ = true;
      panic_message_ = e.what();
    }

    reap_roots();

    if (panicked_) {
      reason = StopReason::kPanic;
      break;
    }
  }

  RunResult result;
  result.reason = reason;
  result.sim_time = now_;
  result.events = tick_;
  result.tasks_outstanding = roots_.size();
  result.digest = digest_;
  result.panic_message = panic_message_;
  return result;
}

}  // namespace anvil::sim
