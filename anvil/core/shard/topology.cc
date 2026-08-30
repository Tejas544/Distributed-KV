#include "anvil/core/shard/topology.h"

#include <algorithm>

#include "anvil/core/lsm/format.h"

namespace anvil::shard {
namespace {

std::vector<NodeId> sorted(std::vector<NodeId> nodes) {
  std::sort(nodes.begin(), nodes.end());
  nodes.erase(std::unique(nodes.begin(), nodes.end()), nodes.end());
  return nodes;
}

std::size_t overlap(const std::vector<NodeId>& a, const std::vector<NodeId>& b) {
  std::size_t n = 0;
  for (const NodeId id : a) {
    if (std::find(b.begin(), b.end(), id) != b.end()) ++n;
  }
  return n;
}

void put_nodes(std::string* out, const std::vector<NodeId>& nodes) {
  lsm::put_varint32(out, static_cast<std::uint32_t>(nodes.size()));
  for (const NodeId n : nodes) lsm::put_varint64(out, n.value());
}

const char* get_nodes(const char* p, const char* limit, std::vector<NodeId>* out) {
  std::uint32_t count = 0;
  p = lsm::get_varint32(p, limit, &count);
  if (p == nullptr) return nullptr;
  out->clear();
  for (std::uint32_t i = 0; i < count; ++i) {
    std::uint64_t id = 0;
    p = lsm::get_varint64(p, limit, &id);
    if (p == nullptr) return nullptr;
    out->push_back(NodeId{id});
  }
  return p;
}

}  // namespace

const char* to_string(AdminOp op) noexcept {
  switch (op) {
    case AdminOp::kBootstrap: return "bootstrap";
    case AdminOp::kSplit: return "split";
    case AdminOp::kBeginMerge: return "begin-merge";
    case AdminOp::kFinishMerge: return "finish-merge";
    case AdminOp::kAbortMerge: return "abort-merge";
    case AdminOp::kChangeReplicas: return "change-replicas";
    case AdminOp::kGrantLease: return "grant-lease";
    case AdminOp::kNodeHeartbeat: return "node-heartbeat";
    case AdminOp::kReportSize: return "report-size";
    case AdminOp::kReportCatchup: return "report-catchup";
    case AdminOp::kSetDescriptor: return "set-descriptor";
  }
  return "?";
}

std::string AdminCommand::describe() const {
  std::string out = to_string(op);
  if (range.valid()) out += " r" + std::to_string(range.value());
  if (other.valid()) out += "+r" + std::to_string(other.value());
  if (!key.empty()) out += " at " + key;
  if (generation != 0) out += " gen " + std::to_string(generation);
  if (node.valid()) out += " n" + std::to_string(node.value());
  if (!replicas.empty()) {
    out += " ->";
    for (const NodeId n : replicas) out += " n" + std::to_string(n.value());
  }
  return out;
}

std::string encode_admin(const AdminCommand& cmd) {
  std::string out;
  out.push_back(static_cast<char>(cmd.op));
  lsm::put_varint64(&out, cmd.range.value());
  lsm::put_varint64(&out, cmd.other.value());
  lsm::put_length_prefixed(&out, cmd.key);
  lsm::put_varint64(&out, cmd.generation);
  put_nodes(&out, cmd.replicas);
  put_nodes(&out, cmd.learners);
  lsm::put_varint64(&out, cmd.node.value());
  lsm::put_varint64(&out, cmd.time);
  lsm::put_varint64(&out, cmd.value);
  return out;
}

bool decode_admin(std::string_view in, AdminCommand* out) {
  const char* p = in.data();
  const char* limit = p + in.size();
  if (p >= limit) return false;
  const auto op = static_cast<std::uint8_t>(*p++);
  if (op > static_cast<std::uint8_t>(AdminOp::kSetDescriptor)) return false;
  out->op = static_cast<AdminOp>(op);

  std::uint64_t range = 0;
  std::uint64_t other = 0;
  p = lsm::get_varint64(p, limit, &range);
  if (p == nullptr) return false;
  p = lsm::get_varint64(p, limit, &other);
  if (p == nullptr) return false;
  std::string_view key;
  p = lsm::get_length_prefixed(p, limit, &key);
  if (p == nullptr) return false;
  p = lsm::get_varint64(p, limit, &out->generation);
  if (p == nullptr) return false;
  p = get_nodes(p, limit, &out->replicas);
  if (p == nullptr) return false;
  p = get_nodes(p, limit, &out->learners);
  if (p == nullptr) return false;
  std::uint64_t node = 0;
  p = lsm::get_varint64(p, limit, &node);
  if (p == nullptr) return false;
  p = lsm::get_varint64(p, limit, &out->time);
  if (p == nullptr) return false;
  p = lsm::get_varint64(p, limit, &out->value);
  if (p == nullptr) return false;

  out->range = RangeId{range};
  out->other = RangeId{other};
  out->key.assign(key);
  out->node = NodeId{node};
  return true;
}

// ---------------------------------------------------------------------------
// TopologyState
// ---------------------------------------------------------------------------

const RangeDescriptor* TopologyState::find(RangeId id) const {
  const auto it = by_id.find(id.value());
  if (it == by_id.end()) return nullptr;
  const auto range = ranges.find(it->second);
  return range == ranges.end() ? nullptr : &range->second;
}

RangeDescriptor* TopologyState::mutable_find(RangeId id) {
  const auto it = by_id.find(id.value());
  if (it == by_id.end()) return nullptr;
  const auto range = ranges.find(it->second);
  return range == ranges.end() ? nullptr : &range->second;
}

const RangeDescriptor* TopologyState::find_by_key(std::string_view key) const {
  auto it = ranges.upper_bound(std::string{key});
  if (it == ranges.begin()) return nullptr;
  --it;
  return it->second.contains(key) ? &it->second : nullptr;
}

const RangeDescriptor* TopologyState::right_neighbour(RangeId id) const {
  const RangeDescriptor* desc = find(id);
  if (desc == nullptr || desc->end.empty()) return nullptr;
  const auto it = ranges.find(desc->end);
  return it == ranges.end() ? nullptr : &it->second;
}

std::optional<std::string> TopologyState::coverage_violation() const {
  if (ranges.empty()) return std::nullopt;  // before bootstrap there is nothing to cover
  std::string expected;                     // the next start the tiling requires
  bool first = true;
  const RangeDescriptor* previous = nullptr;
  for (const auto& [start, desc] : ranges) {
    if (first) {
      if (!start.empty()) {
        return "the key space starts at r" + std::to_string(desc.id.value()) + " with start '" +
               start + "'; nothing covers everything below it";
      }
      first = false;
    } else if (start != expected) {
      const std::string what = start < expected ? "overlap" : "gap";
      return what + " between r" + std::to_string(previous->id.value()) + " ending at '" +
             previous->end + "' and r" + std::to_string(desc.id.value()) + " starting at '" +
             start + "'";
    }
    if (!desc.end.empty() && desc.end <= desc.start) {
      return "r" + std::to_string(desc.id.value()) + " is empty or inverted: [" + desc.start +
             "," + desc.end + ")";
    }
    expected = desc.end;
    previous = &desc;
    if (desc.end.empty()) {
      // The unbounded range must be the last one, and there must be exactly one.
      if (&desc != &ranges.rbegin()->second) {
        return "r" + std::to_string(desc.id.value()) + " is unbounded above but is not last";
      }
    }
  }
  if (previous != nullptr && !previous->end.empty()) {
    return "the key space ends at '" + previous->end + "'; nothing covers everything above it";
  }
  return std::nullopt;
}

std::uint64_t TopologyState::total_size() const {
  std::uint64_t total = 0;
  for (const auto& [id, stat] : stats) total += stat.keys;
  return total;
}

// ---------------------------------------------------------------------------
// TopologyMachine
// ---------------------------------------------------------------------------

void TopologyMachine::insert(const RangeDescriptor& desc) {
  state_.ranges[desc.start] = desc;
  state_.by_id[desc.id.value()] = desc.start;
}

void TopologyMachine::erase(RangeId id) {
  const auto it = state_.by_id.find(id.value());
  if (it == state_.by_id.end()) return;
  state_.ranges.erase(it->second);
  state_.by_id.erase(it);
  state_.stats.erase(id.value());
}

void TopologyMachine::bump(RangeDescriptor* desc, std::uint64_t at) {
  if (options_.bumps_generation) ++desc->generation;
  if (at > desc->changed_at) desc->changed_at = at;
  desc->changed_index = state_.applied;
}

void TopologyMachine::rebuild_meta() {
  state_.meta.rebuild(state_.ranges, options_.meta_records_per_bucket);
}

void TopologyMachine::apply(LogIndex index, std::string_view command) {
  applied_ = index;
  state_.applied = index.value();
  AdminCommand cmd;
  if (!decode_admin(command, &cmd)) {
    // A malformed entry is not skippable: every replica must apply the same
    // sequence or their states diverge, and "this one did not parse" is a
    // divergence with a friendly face. Counted and ignored identically
    // everywhere, which is the only safe way to ignore anything here.
    ++rejected_;
    return;
  }
  ++applied_commands_;
  apply_one(cmd);
  ++revision_;
}

void TopologyMachine::apply_one(const AdminCommand& cmd) {
  // The other half of a non-atomic split, if one is owed. Deliberately at the
  // top of the next apply rather than at the bottom of the previous one: the
  // window has to be observable by anything that looks at the topology in
  // between, or the bug is not a bug.
  if (state_.deferred.has_value()) {
    const AdminCommand deferred = *state_.deferred;
    state_.deferred.reset();
    RangeDescriptor* left = state_.mutable_find(deferred.range);
    if (left != nullptr) {
      left->end = deferred.key;
      bump(left, deferred.time);
      rebuild_meta();
    }
  }

  switch (cmd.op) {
    case AdminOp::kBootstrap: {
      if (!state_.ranges.empty()) { ++rejected_; return; }
      RangeDescriptor desc;
      desc.id = RangeId{state_.next_range_id++};
      desc.generation = 1;
      desc.changed_at = cmd.time;
      desc.changed_index = state_.applied;
      desc.replicas = sorted(cmd.replicas);
      desc.learners = sorted(cmd.learners);
      insert(desc);
      rebuild_meta();
      return;
    }

    case AdminOp::kSplit: {
      RangeDescriptor* left = state_.mutable_find(cmd.range);
      if (left == nullptr) { ++rejected_; return; }
      if (cmd.generation != 0 && left->generation != cmd.generation) { ++rejected_; return; }
      if (left->frozen) { ++rejected_; return; }
      // The split key must be strictly inside the range. At the boundary it
      // produces an empty range, which tiles the key space perfectly and breaks
      // everything downstream that assumes a range has a key in it.
      if (cmd.key.empty() || cmd.key <= left->start) { ++rejected_; return; }
      if (!left->end.empty() && cmd.key >= left->end) { ++rejected_; return; }

      RangeDescriptor right;
      right.id = RangeId{state_.next_range_id++};
      right.start = cmd.key;
      right.end = left->end;
      right.generation = 1;
      right.changed_at = cmd.time;
      right.changed_index = state_.applied;
      right.replicas = left->replicas;
      right.learners = left->learners;
      // No lease. The right-hand side has never been served and has to acquire
      // one through its own group, which is what makes the split point a place
      // where a request can legitimately fail and be retried.

      if (options_.split_is_atomic) {
        left->end = cmd.key;
        bump(left, cmd.time);
      } else {
        AdminCommand fixup = cmd;
        fixup.op = AdminOp::kSetDescriptor;
        state_.deferred = fixup;
      }
      insert(right);
      rebuild_meta();
      return;
    }

    case AdminOp::kBeginMerge: {
      RangeDescriptor* left = state_.mutable_find(cmd.range);
      RangeDescriptor* right = state_.mutable_find(cmd.other);
      if (left == nullptr || right == nullptr) { ++rejected_; return; }
      if (left->end != right->start || left->end.empty()) { ++rejected_; return; }
      if (left->frozen || right->frozen) { ++rejected_; return; }
      if (cmd.generation != 0 && left->generation != cmd.generation) { ++rejected_; return; }
      if (options_.merge_requires_colocation) {
        // Both conditions, and both matter for different reasons. Identical
        // replicas means every replica of the survivor already hosts the
        // subsumed range and can be handed its data; a common lease holder
        // means one node can order the freeze against the absorb without a
        // distributed agreement of its own.
        if (sorted(left->replicas) != sorted(right->replicas)) { ++rejected_; return; }
        if (!left->lease.holder.valid() || left->lease.holder != right->lease.holder) {
          ++rejected_;
          return;
        }
      }
      right->frozen = true;
      bump(right, cmd.time);
      rebuild_meta();
      return;
    }

    case AdminOp::kAbortMerge: {
      RangeDescriptor* right = state_.mutable_find(cmd.other);
      if (right == nullptr || !right->frozen) { ++rejected_; return; }
      right->frozen = false;
      bump(right, cmd.time);
      rebuild_meta();
      return;
    }

    case AdminOp::kFinishMerge: {
      RangeDescriptor* left = state_.mutable_find(cmd.range);
      const RangeDescriptor* right = state_.find(cmd.other);
      if (left == nullptr || right == nullptr) {
        last_reject_ = "finish: missing range";
        ++rejected_; return;
      }
      if (!right->frozen) { last_reject_ = "finish: not frozen"; ++rejected_; return; }
      if (left->end != right->start) {
        last_reject_ = "finish: left ends at '" + left->end + "', right starts at '" +
                       right->start + "'";
        ++rejected_; return;
      }
      left->end = right->end;
      bump(left, cmd.time);
      erase(cmd.other);
      rebuild_meta();
      return;
    }

    case AdminOp::kChangeReplicas: {
      RangeDescriptor* desc = state_.mutable_find(cmd.range);
      if (desc == nullptr) { ++rejected_; return; }
      if (cmd.generation != 0 && desc->generation != cmd.generation) { ++rejected_; return; }
      const std::vector<NodeId> next = sorted(cmd.replicas);
      if (next.empty()) { ++rejected_; return; }
      if (options_.rebalance_keeps_quorum) {
        // The condition that makes a one-at-a-time membership change safe: the
        // old and new voter sets must share a majority of each. Two disjoint
        // majorities can commit two different entries at the same index, which
        // is the whole reason joint consensus exists.
        const std::size_t common = overlap(desc->replicas, next);
        if (common < desc->replicas.size() / 2 + 1 || common < next.size() / 2 + 1) {
          ++rejected_;
          return;
        }
      }
      desc->replicas = next;
      desc->learners = sorted(cmd.learners);
      // A lease held by a node that is no longer a replica is not a lease.
      if (desc->lease.holder.valid() && !desc->is_voter(desc->lease.holder)) {
        desc->lease = Lease{};
      }
      bump(desc, cmd.time);
      rebuild_meta();
      return;
    }

    case AdminOp::kGrantLease: {
      RangeDescriptor* desc = state_.mutable_find(cmd.range);
      if (desc == nullptr) { ++rejected_; return; }
      // Published, not decided. The range's own log granted this lease; the
      // topology is recording it so that placement can see who to talk to. A
      // stale publication -- one that names an interval already superseded --
      // is dropped rather than applied backwards.
      if (cmd.time < desc->lease.start) { ++rejected_; return; }
      desc->lease.holder = cmd.node;
      desc->lease.start = cmd.time;
      desc->lease.expiry = cmd.value;
      // Deliberately no generation bump: a lease moving is not a routing
      // change, and bumping here would invalidate every client cache in the
      // cluster every few hundred milliseconds.
      rebuild_meta();
      return;
    }

    case AdminOp::kNodeHeartbeat: {
      NodeHealth& health = state_.nodes[cmd.node.value()];
      health.id = cmd.node;
      if (cmd.time > health.last_seen) health.last_seen = cmd.time;
      return;
    }

    case AdminOp::kReportSize: {
      if (state_.find(cmd.range) == nullptr) { ++rejected_; return; }
      RangeStats& stats = state_.stats[cmd.range.value()];
      stats.keys = cmd.value;
      stats.median = cmd.key;
      stats.reported_at = cmd.time;
      return;
    }

    case AdminOp::kReportCatchup: {
      const RangeDescriptor* desc = state_.find(cmd.range);
      if (desc == nullptr) { ++rejected_; return; }
      if (!desc->hosts(cmd.node)) { ++rejected_; return; }
      RangeStats& stats = state_.stats[cmd.range.value()];
      if (std::find(stats.caught_up.begin(), stats.caught_up.end(), cmd.node) ==
          stats.caught_up.end()) {
        stats.caught_up.push_back(cmd.node);
        std::sort(stats.caught_up.begin(), stats.caught_up.end());
      }
      return;
    }

    case AdminOp::kSetDescriptor: {
      RangeDescriptor* desc = state_.mutable_find(cmd.range);
      if (desc == nullptr) { ++rejected_; return; }
      desc->end = cmd.key;
      bump(desc, cmd.time);
      rebuild_meta();
      return;
    }
  }
}

std::string TopologyMachine::snapshot() const {
  std::string out;
  lsm::put_varint64(&out, state_.next_range_id);
  lsm::put_varint64(&out, state_.applied);
  lsm::put_varint32(&out, static_cast<std::uint32_t>(state_.ranges.size()));
  for (const auto& [start, desc] : state_.ranges) {
    lsm::put_length_prefixed(&out, encode_descriptor(desc));
  }
  lsm::put_varint32(&out, static_cast<std::uint32_t>(state_.nodes.size()));
  for (const auto& [id, health] : state_.nodes) {
    lsm::put_varint64(&out, id);
    lsm::put_varint64(&out, health.last_seen);
  }
  lsm::put_varint32(&out, static_cast<std::uint32_t>(state_.stats.size()));
  for (const auto& [id, stat] : state_.stats) {
    lsm::put_varint64(&out, id);
    lsm::put_varint64(&out, stat.keys);
    lsm::put_length_prefixed(&out, stat.median);
    lsm::put_varint64(&out, stat.reported_at);
    lsm::put_varint32(&out, static_cast<std::uint32_t>(stat.caught_up.size()));
    for (const NodeId n : stat.caught_up) lsm::put_varint64(&out, n.value());
  }
  // The deferred half of a non-atomic split travels with the snapshot. It has
  // to: a replica restored without it would silently repair the overlap the
  // mutation is supposed to create, and the drill would report a bug as caught
  // when what actually happened is that the evidence was thrown away.
  out.push_back(state_.deferred.has_value() ? 1 : 0);
  if (state_.deferred.has_value()) {
    lsm::put_length_prefixed(&out, encode_admin(*state_.deferred));
  }
  return out;
}

void TopologyMachine::restore(std::string_view data) {
  state_ = TopologyState{};
  const char* p = data.data();
  const char* limit = p + data.size();
  p = lsm::get_varint64(p, limit, &state_.next_range_id);
  if (p == nullptr) return;
  p = lsm::get_varint64(p, limit, &state_.applied);
  if (p == nullptr) return;

  std::uint32_t count = 0;
  p = lsm::get_varint32(p, limit, &count);
  if (p == nullptr) return;
  for (std::uint32_t i = 0; i < count; ++i) {
    std::string_view encoded;
    p = lsm::get_length_prefixed(p, limit, &encoded);
    if (p == nullptr) return;
    RangeDescriptor desc;
    if (!decode_descriptor(encoded, &desc)) return;
    insert(desc);
  }
  p = lsm::get_varint32(p, limit, &count);
  if (p == nullptr) return;
  for (std::uint32_t i = 0; i < count; ++i) {
    std::uint64_t id = 0;
    std::uint64_t seen = 0;
    p = lsm::get_varint64(p, limit, &id);
    if (p == nullptr) return;
    p = lsm::get_varint64(p, limit, &seen);
    if (p == nullptr) return;
    state_.nodes[id] = NodeHealth{NodeId{id}, seen};
  }
  p = lsm::get_varint32(p, limit, &count);
  if (p == nullptr) return;
  for (std::uint32_t i = 0; i < count; ++i) {
    std::uint64_t id = 0;
    RangeStats stat;
    p = lsm::get_varint64(p, limit, &id);
    if (p == nullptr) return;
    p = lsm::get_varint64(p, limit, &stat.keys);
    if (p == nullptr) return;
    std::string_view median;
    p = lsm::get_length_prefixed(p, limit, &median);
    if (p == nullptr) return;
    stat.median.assign(median);
    p = lsm::get_varint64(p, limit, &stat.reported_at);
    if (p == nullptr) return;
    std::uint32_t caught = 0;
    p = lsm::get_varint32(p, limit, &caught);
    if (p == nullptr) return;
    for (std::uint32_t j = 0; j < caught; ++j) {
      std::uint64_t node = 0;
      p = lsm::get_varint64(p, limit, &node);
      if (p == nullptr) return;
      stat.caught_up.push_back(NodeId{node});
    }
    state_.stats[id] = std::move(stat);
  }
  if (p < limit && *p++ != 0) {
    std::string_view encoded;
    p = lsm::get_length_prefixed(p, limit, &encoded);
    if (p != nullptr) {
      AdminCommand cmd;
      if (decode_admin(encoded, &cmd)) state_.deferred = cmd;
    }
  }
  rebuild_meta();
  ++revision_;
}

}  // namespace anvil::shard
