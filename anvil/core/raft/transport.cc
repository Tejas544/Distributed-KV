#include "anvil/core/raft/transport.h"

#include <utility>

#include "anvil/core/lsm/format.h"
#include "anvil/core/raft/message.h"

namespace anvil::raft {
namespace {

// A batch envelope: group 0, then the count, then each inner message
// length-prefixed. Group 0 is not a group, so a receiver that reads the leading
// varint knows immediately which of the two shapes it has -- no separate flag,
// no MessageKind overloading, and an old node reading a new batch sees an
// undecodable message rather than a plausible wrong one.
std::string encode_batch(const std::vector<std::string>& messages) {
  std::string out;
  lsm::put_varint64(&out, 0);
  lsm::put_varint32(&out, static_cast<std::uint32_t>(messages.size()));
  for (const std::string& m : messages) lsm::put_length_prefixed(&out, m);
  return out;
}

bool decode_batch(std::string_view payload, std::vector<std::string_view>* out) {
  const char* p = payload.data();
  const char* limit = p + payload.size();
  std::uint64_t marker = 0;
  p = lsm::get_varint64(p, limit, &marker);
  if (p == nullptr || marker != 0) return false;
  std::uint32_t count = 0;
  p = lsm::get_varint32(p, limit, &count);
  if (p == nullptr) return false;
  out->clear();
  out->reserve(count);
  for (std::uint32_t i = 0; i < count; ++i) {
    std::string_view one;
    p = lsm::get_length_prefixed(p, limit, &one);
    if (p == nullptr) return false;
    out->push_back(one);
  }
  return true;
}

Message envelope_for(NodeId from, NodeId to, MessageKind kind, std::string payload) {
  Message out;
  out.from = from;
  out.to = to;
  out.kind = kind;
  out.payload.resize(payload.size());
  for (std::size_t i = 0; i < payload.size(); ++i) {
    out.payload[i] = static_cast<std::byte>(static_cast<unsigned char>(payload[i]));
  }
  return out;
}

bool is_heartbeat(RaftMessageType type) noexcept {
  return type == RaftMessageType::kHeartbeat || type == RaftMessageType::kHeartbeatReply;
}

}  // namespace

void RaftTransport::listen_to(NodeId peer) {
  if (peer == self_ || !peer.valid()) return;
  if (!listening_.insert(peer.value()).second) return;
  runtime_->spawn(recv_loop(peer));
}

Task<void> RaftTransport::recv_loop(NodeId peer) {
  // Outer loop, because a connection reset must not end the conversation. The
  // failure mode of getting this wrong is a node that keeps sending, keeps
  // ticking, and never hears one peer again -- which presents as a replica that
  // stops converging for no reason anyone can see, hours later.
  for (;;) {
    ConnHandle conn{};
    co_await runtime_->connect(peer, &conn);
    connections_[peer.value()] = conn;

    for (;;) {
      Message envelope;
      if (!(co_await runtime_->recv(conn, &envelope)).is_ok()) break;
      co_await deliver(std::move(envelope));
    }

    ++stats_.reconnects;
    co_await runtime_->sleep_for(reconnect_delay_);
  }
}

Task<void> RaftTransport::deliver(Message envelope) {
  if (envelope.kind == MessageKind::kClientRequest ||
      envelope.kind == MessageKind::kClientReply) {
    ++stats_.foreign;
    if (foreign_) foreign_(envelope);
    co_return;
  }

  const std::string_view view{reinterpret_cast<const char*>(envelope.payload.data()),
                              envelope.payload.size()};
  GroupId group{};
  if (!peek_group(view, &group)) {
    ++stats_.malformed;
    co_return;
  }

  if (!group.valid()) {
    // A coalesced batch. Unpacked here and dispatched one inner message at a
    // time, so nothing below this line can tell whether it arrived on its own.
    std::vector<std::string_view> inner;
    if (!decode_batch(view, &inner)) {
      ++stats_.malformed;
      co_return;
    }
    ++stats_.batches_received;
    // The views point into `envelope.payload`, which lives in this coroutine
    // frame for the duration -- but a handler suspends, and a handler is allowed
    // to unregister groups while it does. Copy each inner message out before
    // awaiting anything.
    std::vector<std::string> owned;
    owned.reserve(inner.size());
    for (std::string_view one : inner) owned.emplace_back(one);
    for (std::string& one : owned) {
      GroupId inner_group{};
      if (!peek_group(one, &inner_group) || !inner_group.valid()) {
        ++stats_.malformed;
        continue;
      }
      const auto it = handlers_.find(inner_group.value());
      if (it == handlers_.end()) {
        ++stats_.unroutable;
        continue;
      }
      ++stats_.received;
      Handler handler = it->second;
      co_await handler(envelope_for(envelope.from, envelope.to, envelope.kind, std::move(one)));
    }
    co_return;
  }

  const auto it = handlers_.find(group.value());
  if (it == handlers_.end()) {
    // Normal during a split: the right-hand side's first append can beat the
    // split entry that creates the local replica. Raft retransmits; a queue
    // here would be an unbounded buffer with a good excuse.
    ++stats_.unroutable;
    co_return;
  }
  ++stats_.received;
  // Copied, not referenced. The handler suspends inside a persist, and a merge
  // applying on another group during that suspension erases entries from
  // handlers_ -- at which point a reference into the map is a dangling one.
  Handler handler = it->second;
  co_await handler(std::move(envelope));
}

Task<Status> RaftTransport::send(const RaftMessage& msg) {
  std::string payload = encode_message(msg);
  if (coalesce_ && is_heartbeat(msg.type)) {
    pending_[msg.to.value()].push_back(std::move(payload));
    ++stats_.heartbeats_coalesced;
    co_return Status::ok();
  }
  co_return co_await raw_send(msg.to, envelope_for(msg.from, msg.to, transport_kind(msg.type),
                                                   std::move(payload)));
}

Task<void> RaftTransport::flush() {
  if (pending_.empty()) co_return;
  // Swapped out first. Sending suspends, and a group stepping inside that
  // suspension can buffer another heartbeat -- which would be a mutation of the
  // container being iterated, and the coroutine form of that corrupts the heap
  // rather than failing cleanly (CONTEXT.md 10.15).
  std::map<std::uint64_t, std::vector<std::string>> batch;
  batch.swap(pending_);
  for (auto& [peer, messages] : batch) {
    if (messages.empty()) continue;
    ++stats_.heartbeat_batches;
    const NodeId to{peer};
    co_await raw_send(to, envelope_for(self_, to, MessageKind::kHeartbeat,
                                       encode_batch(messages)));
  }
}

Task<Status> RaftTransport::send_envelope(Message envelope) {
  const NodeId to = envelope.to;
  co_return co_await raw_send(to, std::move(envelope));
}

Task<Status> RaftTransport::raw_send(NodeId peer, Message envelope) {
  ConnHandle conn{};
  const auto it = connections_.find(peer.value());
  if (it == connections_.end()) {
    co_await runtime_->connect(peer, &conn);
    connections_[peer.value()] = conn;
  } else {
    conn = it->second;
  }

  const Status status = co_await runtime_->send(conn, std::move(envelope));
  if (status.is_ok()) {
    ++stats_.sent;
    co_return status;
  }
  ++stats_.send_failures;
  // The handle goes. A send fails because the connection is gone, and keeping a
  // dead handle means every future message to this peer fails the same way
  // forever -- a node that has silently stopped talking to one peer while
  // believing it is fine.
  connections_.erase(peer.value());
  co_return status;
}

}  // namespace anvil::raft
