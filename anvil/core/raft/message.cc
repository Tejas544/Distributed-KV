#include "anvil/core/raft/message.h"

#include <cstring>

#include "anvil/core/lsm/format.h"

namespace anvil::raft {
namespace {

constexpr std::uint8_t kFlagReject = 1u << 0;
constexpr std::uint8_t kFlagLastChunk = 1u << 1;
constexpr std::uint8_t kFlagHasSnapshot = 1u << 2;
constexpr std::uint8_t kFlagForce = 1u << 3;

}  // namespace

const char* to_string(Role role) noexcept {
  switch (role) {
    case Role::kFollower: return "follower";
    case Role::kPreCandidate: return "pre-candidate";
    case Role::kCandidate: return "candidate";
    case Role::kLeader: return "leader";
  }
  return "?";
}

const char* to_string(EntryType type) noexcept {
  switch (type) {
    case EntryType::kNormal: return "normal";
    case EntryType::kNoop: return "noop";
    case EntryType::kConfChange: return "conf-change";
  }
  return "?";
}

const char* to_string(RaftMessageType type) noexcept {
  switch (type) {
    case RaftMessageType::kPreVote: return "pre-vote";
    case RaftMessageType::kPreVoteReply: return "pre-vote-reply";
    case RaftMessageType::kRequestVote: return "request-vote";
    case RaftMessageType::kRequestVoteReply: return "request-vote-reply";
    case RaftMessageType::kAppend: return "append";
    case RaftMessageType::kAppendReply: return "append-reply";
    case RaftMessageType::kHeartbeat: return "heartbeat";
    case RaftMessageType::kHeartbeatReply: return "heartbeat-reply";
    case RaftMessageType::kInstallSnapshot: return "install-snapshot";
    case RaftMessageType::kInstallSnapshotReply: return "install-snapshot-reply";
    case RaftMessageType::kTimeoutNow: return "timeout-now";
  }
  return "?";
}

MessageKind transport_kind(RaftMessageType type) noexcept {
  switch (type) {
    case RaftMessageType::kPreVote: return MessageKind::kPreVote;
    case RaftMessageType::kPreVoteReply: return MessageKind::kPreVoteReply;
    case RaftMessageType::kRequestVote: return MessageKind::kRequestVote;
    case RaftMessageType::kRequestVoteReply: return MessageKind::kRequestVoteReply;
    case RaftMessageType::kAppend: return MessageKind::kAppendEntries;
    case RaftMessageType::kAppendReply: return MessageKind::kAppendEntriesReply;
    case RaftMessageType::kHeartbeat: return MessageKind::kHeartbeat;
    case RaftMessageType::kHeartbeatReply: return MessageKind::kHeartbeatReply;
    case RaftMessageType::kInstallSnapshot: return MessageKind::kInstallSnapshot;
    case RaftMessageType::kInstallSnapshotReply: return MessageKind::kInstallSnapshotReply;
    case RaftMessageType::kTimeoutNow: return MessageKind::kTimeoutNow;
  }
  return MessageKind::kUnknown;
}

std::string encode_message(const RaftMessage& msg) {
  std::string out;
  // First, and unconditionally. The transport demultiplexes on this without
  // decoding anything else, and a batch envelope is distinguished by a zero
  // here -- both of which stop working the moment it moves or becomes optional.
  lsm::put_varint64(&out, msg.group.value());
  out.push_back(static_cast<char>(msg.type));
  lsm::put_varint64(&out, msg.term.value());
  lsm::put_varint64(&out, msg.from.value());
  lsm::put_varint64(&out, msg.to.value());
  lsm::put_varint64(&out, msg.prev_index.value());
  lsm::put_varint64(&out, msg.prev_term.value());
  lsm::put_varint64(&out, msg.commit.value());
  lsm::put_varint64(&out, msg.match.value());
  lsm::put_varint64(&out, msg.reject_hint.value());
  lsm::put_varint64(&out, msg.reject_term.value());
  lsm::put_varint64(&out, msg.read_context);
  lsm::put_varint64(&out, msg.echo_time);
  // Outside the snapshot block, deliberately. These two are the snapshot
  // transfer's flow control, and the *reply* carries them while carrying no
  // snapshot body at all -- so encoding them alongside the payload silently
  // drops the acknowledged offset on every reply, and the leader ships chunk
  // zero forever. Two bytes to keep them unconditional.
  lsm::put_varint64(&out, msg.chunk_offset);
  lsm::put_varint64(&out, msg.chunk_total);

  std::uint8_t flags = 0;
  if (msg.reject) flags |= kFlagReject;
  if (msg.last_chunk) flags |= kFlagLastChunk;
  if (!msg.snapshot.empty()) flags |= kFlagHasSnapshot;
  if (msg.force) flags |= kFlagForce;
  out.push_back(static_cast<char>(flags));

  lsm::put_varint32(&out, static_cast<std::uint32_t>(msg.entries.size()));
  for (const LogEntry& e : msg.entries) {
    lsm::put_varint64(&out, e.term.value());
    lsm::put_varint64(&out, e.index.value());
    out.push_back(static_cast<char>(e.type));
    lsm::put_length_prefixed(&out, e.data);
  }

  if (flags & kFlagHasSnapshot) {
    lsm::put_varint64(&out, msg.snapshot.index.value());
    lsm::put_varint64(&out, msg.snapshot.term.value());
    lsm::put_length_prefixed(&out, msg.snapshot.config);
    lsm::put_length_prefixed(&out, msg.snapshot.data);
  }
  return out;
}

bool peek_group(std::string_view payload, GroupId* out) {
  const char* p = payload.data();
  const char* limit = p + payload.size();
  std::uint64_t group = 0;
  p = lsm::get_varint64(p, limit, &group);
  if (p == nullptr) return false;
  *out = GroupId{group};
  return true;
}

bool decode_message(std::string_view payload, RaftMessage* out) {
  const char* p = payload.data();
  const char* limit = p + payload.size();

  std::uint64_t group = 0;
  p = lsm::get_varint64(p, limit, &group);
  if (p == nullptr) return false;
  if (group == 0) return false;  // a batch envelope, not a message
  out->group = GroupId{group};

  if (p >= limit) return false;
  const auto type = static_cast<std::uint8_t>(*p++);
  if (type > static_cast<std::uint8_t>(RaftMessageType::kTimeoutNow)) return false;
  out->type = static_cast<RaftMessageType>(type);

  std::uint64_t values[13] = {};
  for (std::uint64_t& value : values) {
    p = lsm::get_varint64(p, limit, &value);
    if (p == nullptr) return false;
  }
  out->term = Term{values[0]};
  out->from = NodeId{values[1]};
  out->to = NodeId{values[2]};
  out->prev_index = LogIndex{values[3]};
  out->prev_term = Term{values[4]};
  out->commit = LogIndex{values[5]};
  out->match = LogIndex{values[6]};
  out->reject_hint = LogIndex{values[7]};
  out->reject_term = Term{values[8]};
  out->read_context = values[9];
  out->echo_time = values[10];
  out->chunk_offset = values[11];
  out->chunk_total = values[12];

  if (p >= limit) return false;
  const auto flags = static_cast<std::uint8_t>(*p++);
  out->reject = (flags & kFlagReject) != 0;
  out->last_chunk = (flags & kFlagLastChunk) != 0;
  out->force = (flags & kFlagForce) != 0;

  std::uint32_t count = 0;
  p = lsm::get_varint32(p, limit, &count);
  if (p == nullptr) return false;
  out->entries.clear();
  out->entries.reserve(count);
  for (std::uint32_t i = 0; i < count; ++i) {
    LogEntry entry;
    std::uint64_t term = 0;
    std::uint64_t index = 0;
    p = lsm::get_varint64(p, limit, &term);
    if (p == nullptr) return false;
    p = lsm::get_varint64(p, limit, &index);
    if (p == nullptr) return false;
    if (p >= limit) return false;
    const auto entry_type = static_cast<std::uint8_t>(*p++);
    if (entry_type > static_cast<std::uint8_t>(EntryType::kConfChange)) return false;
    std::string_view data;
    p = lsm::get_length_prefixed(p, limit, &data);
    if (p == nullptr) return false;
    entry.term = Term{term};
    entry.index = LogIndex{index};
    entry.type = static_cast<EntryType>(entry_type);
    entry.data.assign(data);
    out->entries.push_back(std::move(entry));
  }

  out->snapshot = Snapshot{};
  if (flags & kFlagHasSnapshot) {
    std::uint64_t index = 0;
    std::uint64_t term = 0;
    p = lsm::get_varint64(p, limit, &index);
    if (p == nullptr) return false;
    p = lsm::get_varint64(p, limit, &term);
    if (p == nullptr) return false;
    std::string_view config;
    std::string_view data;
    p = lsm::get_length_prefixed(p, limit, &config);
    if (p == nullptr) return false;
    p = lsm::get_length_prefixed(p, limit, &data);
    if (p == nullptr) return false;
    out->snapshot.index = LogIndex{index};
    out->snapshot.term = Term{term};
    out->snapshot.config.assign(config);
    out->snapshot.data.assign(data);
  }
  return true;
}

Message to_transport(const RaftMessage& msg) {
  Message out;
  out.from = msg.from;
  out.to = msg.to;
  out.kind = transport_kind(msg.type);
  out.correlation = msg.term.value();
  const std::string encoded = encode_message(msg);
  out.payload.resize(encoded.size());
  if (!encoded.empty()) std::memcpy(out.payload.data(), encoded.data(), encoded.size());
  return out;
}

bool from_transport(const Message& msg, RaftMessage* out) {
  const std::string_view view{reinterpret_cast<const char*>(msg.payload.data()),
                              msg.payload.size()};
  return decode_message(view, out);
}

}  // namespace anvil::raft
