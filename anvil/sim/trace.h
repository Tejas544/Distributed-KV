// The causal trace: a structured record of everything the simulation did.
//
// A seed reproduces a failure, but reproducing it is only half of debugging --
// you still have to understand it. The trace is the other half: an ordered log
// of every scheduling decision, message, and I/O operation, each stamped with
// virtual time and linked to the event that caused it.
//
// `caused_by` is the field that earns its keep. A message delivery points at
// the send that produced it; a task wakeup points at the timer that fired it.
// Follow those backwards from a violation and you get the causal chain that led
// to it, rather than a flat log you have to correlate by eye.
//
// Recording is off by default. The fleet runs millions of events per second and
// a trace of all of them is neither affordable nor useful; you enable it on the
// one seed that failed.

#ifndef ANVIL_SIM_TRACE_H_
#define ANVIL_SIM_TRACE_H_

#include <cstdint>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "anvil/core/types.h"

namespace anvil::sim {

enum class EventKind : std::uint8_t {
  kSpawn,
  kResume,
  kTimer,
  kSleep,
  kSend,
  kDeliver,
  kRecvPark,
  kConnect,
  kFileOp,
  kBuggify,
  kPanic,
  kNote,
};

const char* to_string(EventKind kind) noexcept;

using TraceField = std::pair<const char*, std::uint64_t>;

struct TraceEvent {
  std::uint64_t id = 0;
  Timestamp when;
  NodeId node;
  EventKind kind = EventKind::kNote;
  const char* name = "";  // always a string literal; never owned
  std::vector<TraceField> fields;
  std::uint64_t caused_by = 0;  // 0 == no recorded cause
};

class Trace {
 public:
  explicit Trace(bool recording) noexcept : recording_(recording) {}

  bool recording() const noexcept { return recording_; }
  std::uint64_t count() const noexcept { return count_; }
  const std::vector<TraceEvent>& events() const noexcept { return events_; }

  // Returns the event id so a later event can name this one as its cause. Ids
  // are handed out even when recording is off, so causality links stay stable
  // between a silent fleet run and the verbose replay of the same seed.
  std::uint64_t emit(Timestamp when, NodeId node, EventKind kind, const char* name,
                     std::span<const TraceField> fields, std::uint64_t caused_by = 0);

  // JSONL, one event per line. Uses real file I/O -- anvil/sim is deliberately
  // outside the hermeticity gate, since it is the component that owns the
  // outside world rather than one that must be insulated from it.
  bool write_jsonl(const std::string& path) const;

 private:
  bool recording_;
  std::uint64_t count_ = 0;
  std::vector<TraceEvent> events_;
};

}  // namespace anvil::sim

#endif  // ANVIL_SIM_TRACE_H_
