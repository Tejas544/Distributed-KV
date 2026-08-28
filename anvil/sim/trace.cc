#include "anvil/sim/trace.h"

#include <fstream>

namespace anvil::sim {
namespace {

void append_escaped(std::string& out, const char* s) {
  out.push_back('"');
  for (const char* p = s; *p != '\0'; ++p) {
    switch (*p) {
      case '"': out += "\\\""; break;
      case '\\': out += "\\\\"; break;
      case '\n': out += "\\n"; break;
      case '\t': out += "\\t"; break;
      default: out.push_back(*p);
    }
  }
  out.push_back('"');
}

void append_u64(std::string& out, std::uint64_t v) {
  char buf[21];
  std::size_t n = 0;
  if (v == 0) {
    buf[n++] = '0';
  } else {
    while (v > 0) {
      buf[n++] = static_cast<char>('0' + (v % 10));
      v /= 10;
    }
  }
  while (n > 0) out.push_back(buf[--n]);
}

}  // namespace

const char* to_string(EventKind kind) noexcept {
  switch (kind) {
    case EventKind::kSpawn: return "spawn";
    case EventKind::kResume: return "resume";
    case EventKind::kTimer: return "timer";
    case EventKind::kSleep: return "sleep";
    case EventKind::kSend: return "send";
    case EventKind::kDeliver: return "deliver";
    case EventKind::kRecvPark: return "recv_park";
    case EventKind::kConnect: return "connect";
    case EventKind::kFileOp: return "file_op";
    case EventKind::kBuggify: return "buggify";
    case EventKind::kPanic: return "panic";
    case EventKind::kNote: return "note";
  }
  return "unknown";
}

std::uint64_t Trace::emit(Timestamp when, NodeId node, EventKind kind, const char* name,
                          std::span<const TraceField> fields, std::uint64_t caused_by) {
  const std::uint64_t id = ++count_;
  if (!recording_) return id;

  TraceEvent ev;
  ev.id = id;
  ev.when = when;
  ev.node = node;
  ev.kind = kind;
  ev.name = name;
  ev.fields.assign(fields.begin(), fields.end());
  ev.caused_by = caused_by;
  events_.push_back(std::move(ev));
  return id;
}

bool Trace::write_jsonl(const std::string& path) const {
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  if (!out) return false;

  std::string line;
  for (const TraceEvent& ev : events_) {
    line.clear();
    line += "{\"id\":";
    append_u64(line, ev.id);
    line += ",\"t\":";
    append_u64(line, ev.when.physical);
    line += ",\"l\":";
    append_u64(line, ev.when.logical);
    line += ",\"node\":";
    append_u64(line, ev.node.value());
    line += ",\"kind\":";
    append_escaped(line, to_string(ev.kind));
    line += ",\"name\":";
    append_escaped(line, ev.name);
    if (ev.caused_by != 0) {
      line += ",\"caused_by\":";
      append_u64(line, ev.caused_by);
    }
    if (!ev.fields.empty()) {
      line += ",\"fields\":{";
      bool first = true;
      for (const auto& [key, value] : ev.fields) {
        if (!first) line.push_back(',');
        first = false;
        append_escaped(line, key);
        line.push_back(':');
        append_u64(line, value);
      }
      line.push_back('}');
    }
    line += "}\n";
    out << line;
  }
  return out.good();
}

}  // namespace anvil::sim
