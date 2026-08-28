// The deterministic scheduler: one thread, virtual time, quantized execution.
//
// Everything that will ever happen is an entry in one ordered queue, keyed by
// (virtual time, sequence number). Task wakeups, timer callbacks, and message
// deliveries are all the same kind of thing -- there is no separate "run queue"
// racing a "timer wheel", because two queues means two orderings to keep
// consistent and one more place for platform-dependent behaviour to hide.
//
// Three properties make this replayable:
//
//   No parallelism.        One OS thread. A coroutine runs until it awaits.
//   Quantized execution.   Interleaving happens only at explicit await points,
//                          so the set of possible schedules is finite and
//                          enumerable rather than continuous.
//   Deterministic order.   The queue is keyed on (time, monotonic seq). Never
//                          on a pointer, an address, or a hash-container
//                          iteration order.
//
// Virtual time advances only when nothing is runnable at the current instant.
// A consequence worth knowing before it confuses you: a busy-wait loop that
// polls now() will hang the simulation instead of spinning, because time cannot
// move while a task is runnable. That is a feature. It converts "this code
// spins under load" from a production mystery into an immediate stall.

#ifndef ANVIL_SIM_SCHEDULER_H_
#define ANVIL_SIM_SCHEDULER_H_

#include <coroutine>
#include <cstdint>
#include <functional>
#include <map>
#include <span>
#include <stdexcept>
#include <string>

#include "anvil/checker/invariant.h"
#include "anvil/core/digest.h"
#include "anvil/core/random.h"
#include "anvil/core/runtime/task.h"
#include "anvil/core/types.h"
#include "anvil/sim/trace.h"

namespace anvil::sim {

// Thrown by Runtime::panic(). Inside a coroutine it is captured by the promise
// and surfaces when the root task is reaped; from a plain callback it unwinds
// straight into the run loop. Both paths end at the same place.
class SimulationPanic : public std::runtime_error {
 public:
  explicit SimulationPanic(const std::string& what) : std::runtime_error(what) {}
};

enum class StopReason : std::uint8_t {
  kQuiesced,      // nothing left to do; tasks may still be parked on recv
  kDeadlineHit,   // max_time reached with work outstanding
  kStopRequested,
  kPanic,
  kInvariantViolated,  // a protocol invariant failed mid-run
};

const char* to_string(StopReason reason) noexcept;

struct RunResult {
  StopReason reason = StopReason::kQuiesced;
  Timestamp sim_time;
  std::uint64_t events = 0;
  std::uint64_t tasks_outstanding = 0;
  Digest digest;
  std::string panic_message;

  // Every invariant that fired during the run. The first one is the
  // interesting one -- after an invariant breaks, downstream state is already
  // corrupt and later violations are usually consequences rather than causes.
  std::vector<checker::Violation> violations;

  bool ok() const noexcept {
    return reason != StopReason::kPanic && reason != StopReason::kInvariantViolated;
  }
};

class Scheduler {
 public:
  Scheduler(std::uint64_t seed, Trace* trace);
  ~Scheduler();

  Scheduler(const Scheduler&) = delete;
  Scheduler& operator=(const Scheduler&) = delete;

  Timestamp now() const noexcept { return now_; }
  std::uint64_t tick() const noexcept { return tick_; }
  Digest& digest() noexcept { return digest_; }
  Trace& trace() noexcept { return *trace_; }
  DeterministicRandom& rng() noexcept { return rng_; }

  // Queue an action. Returns the event id, which doubles as a cancellation
  // token and as a `caused_by` link for the trace.
  std::uint64_t at(Duration delay, NodeId node, EventKind kind, const char* name,
                   std::function<void()> action, std::uint64_t caused_by = 0);

  // Cancelled events are erased outright rather than flagged and skipped. A
  // flagged event still sits in the queue holding a timestamp, and popping it
  // would advance virtual time to a moment nothing was going to happen at --
  // which quietly changes every subsequent scheduling decision.
  void cancel(std::uint64_t event_id);

  // Takes ownership of the coroutine frame until it completes.
  void spawn(NodeId node, Task<void> task);

  void request_stop() noexcept { stop_requested_ = true; }

  // Armed invariants are evaluated inside the run loop: tick-class after every
  // event, epoch-class every N. A violation stops the run immediately, because
  // continuing past a broken invariant means every subsequent observation is
  // about a system that is already corrupt -- and the resulting cascade buries
  // the one violation that mattered.
  void set_invariants(checker::InvariantRegistry* registry) noexcept {
    invariants_ = registry;
  }
  checker::InvariantRegistry* invariants() const noexcept { return invariants_; }

  RunResult run(Duration max_time);

  // Destroys every outstanding coroutine frame. Idempotent, and called
  // explicitly by Simulation's destructor *before* the network and disk models
  // go away. Relying on member destruction order for this would work today and
  // break the first time a frame's destructor touches a model -- an ordering
  // bug that would present as a use-after-free in the teardown of a passing
  // test, which is about the worst place to have to debug one.
  void destroy_all_tasks() noexcept;

  // ---- process faults ----------------------------------------------------
  //
  // Crash is destructive and ordering-sensitive. Queued events capture
  // coroutine handles by value, so the frames must not be destroyed while an
  // event that would resume them is still in the queue. drop_events_for() runs
  // first, destroy_tasks_for() second, and getting that backwards is a
  // use-after-free that only fires under a crash-heavy seed.
  void drop_events_for(NodeId node);
  void destroy_tasks_for(NodeId node) noexcept;

  // A pause freezes execution without losing state: the node keeps its memory
  // and its data and simply stops running. Only task-execution events are held
  // -- deliveries still land in the node's inbox, because the network and the
  // peer's kernel do not stop just because this process did. That asymmetry is
  // the whole reason pauses break leases in ways crashes do not.
  void pause_node(NodeId node, Duration duration);
  void resume_node(NodeId node);
  bool node_paused(NodeId node) const;
  Timestamp paused_until(NodeId node) const;

  std::uint64_t pending_events() const noexcept { return queue_.size(); }

 private:
  struct EventKey {
    Timestamp when;
    std::uint64_t seq;  // unique, so this is a strict total order
    friend auto operator<=>(const EventKey&, const EventKey&) noexcept = default;
  };

  struct Event {
    NodeId node;
    EventKind kind = EventKind::kNote;
    const char* name = "";
    std::uint64_t caused_by = 0;
    std::function<void()> action;
  };

  // Destroys frames of completed root tasks and propagates any exception they
  // captured. Iterated in id order, so the reap sequence is deterministic.
  void reap_roots();

  Timestamp now_;
  std::uint64_t tick_ = 0;
  std::uint64_t next_seq_ = 1;
  bool stop_requested_ = false;
  std::string panic_message_;
  bool panicked_ = false;

  // Events whose delivery is suspended while a node is paused. Deliveries are
  // deliberately absent: the network keeps running.
  static bool is_execution_event(EventKind kind) noexcept {
    return kind == EventKind::kResume || kind == EventKind::kTimer ||
           kind == EventKind::kSpawn || kind == EventKind::kSleep;
  }

  struct Root {
    NodeId node;
    Task<void>::handle_type handle;
  };

  std::map<EventKey, Event> queue_;
  std::map<std::uint64_t, EventKey> index_;  // event id -> key, for cancel()

  std::map<std::uint64_t, Root> roots_;
  std::uint64_t next_root_id_ = 1;
  std::map<std::uint64_t, Timestamp> paused_until_;

  Digest digest_;
  DeterministicRandom rng_;
  Trace* trace_;
  checker::InvariantRegistry* invariants_ = nullptr;
  std::vector<checker::Violation> violations_;
};

// Awaitable that parks the current coroutine and hands it back to the scheduler
// for resumption after `delay`. The building block under sleep_for() and
// yield().
struct ScheduledResume {
  Scheduler* scheduler;
  Duration delay;
  NodeId node;
  const char* name;
  std::uint64_t caused_by = 0;

  bool await_ready() const noexcept { return false; }

  void await_suspend(std::coroutine_handle<> h) const {
    scheduler->at(
        delay, node, EventKind::kResume, name, [h]() { h.resume(); }, caused_by);
    // Returning void hands control back to whoever called resume(), which is
    // the run loop. The coroutine stays suspended until its event fires.
  }

  void await_resume() const noexcept {}
};

}  // namespace anvil::sim

#endif  // ANVIL_SIM_SCHEDULER_H_
