// One link per pair of nodes, shared by every replication group on them.
//
// P3 had one Raft group per node, so the driver could own its connections and
// its receive loops outright. P5 has one group per range, and the transport
// underneath is still a single ordered link between each pair of nodes -- one
// inbox per ordered pair, in the simulator's network model. Two receive loops on
// one endpoint would race for the same queue, so the demultiplexing has to
// happen above the link and below the protocol. That is this file.
//
// It does three things and no more:
//
//   1. Owns the connection and the receive loop for each peer, and reconnects
//      when the link resets. A reset that ends the conversation leaves a node
//      permanently deaf to one peer while looking perfectly healthy.
//   2. Routes an arriving message to the group it names, or drops it if no such
//      group lives here -- which is normal and expected during a split, when the
//      right-hand side's messages can arrive before the local replica has
//      applied the split that creates it. Dropped rather than queued, because
//      Raft's own retransmission is the recovery mechanism and a queue with no
//      bound is a memory leak with a plausible excuse.
//   3. Coalesces heartbeats. This is the reason MultiRaft exists at all: with
//      one group per range, per-group heartbeats are O(ranges x peers) messages
//      per tick, and almost all of them carry nothing. Buffering them per peer
//      and flushing one envelope per tick makes the message count O(peers)
//      while leaving the protocol untouched -- each inner message is byte for
//      byte the one the group produced.
//
// Coalescing is off by default. It delays a heartbeat by up to one tick, which
// is safe in both directions but only *obviously* safe when something is
// actually driving the flush; a caller that enables it and never calls flush()
// has built a cluster that never heartbeats. The multi-group store drives both,
// which is why they are enabled together there and nowhere else.

#ifndef ANVIL_CORE_RAFT_TRANSPORT_H_
#define ANVIL_CORE_RAFT_TRANSPORT_H_

#include <cstdint>
#include <functional>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "anvil/core/raft/types.h"
#include "anvil/core/runtime/runtime.h"

namespace anvil::raft {

struct TransportStats {
  std::uint64_t sent = 0;
  std::uint64_t received = 0;
  std::uint64_t send_failures = 0;
  std::uint64_t reconnects = 0;
  std::uint64_t foreign = 0;

  // Arrived for a group this node does not host. Counted, not logged: during a
  // split the right-hand side's first messages routinely beat the split entry
  // that creates the local replica, and a number is the only way to tell that
  // normal case apart from a routing bug that drops everything.
  std::uint64_t unroutable = 0;
  std::uint64_t malformed = 0;

  // The coalescing ratio, which is the whole claim: heartbeats that were folded
  // into batch envelopes, and the number of envelopes they cost.
  std::uint64_t heartbeats_coalesced = 0;
  std::uint64_t heartbeat_batches = 0;
  std::uint64_t batches_received = 0;
};

class RaftTransport {
 public:
  // Invoked on the receiving node for each message routed to a group. Returns a
  // Task so the receive loop can await the group's pump, exactly as the
  // single-group driver did when it owned the loop: the alternative -- spawning
  // the pump and continuing to receive -- lets a second message be stepped
  // while the first batch is still being persisted, which is the one thing the
  // Ready model exists to prevent (CONTEXT.md 10.8).
  using Handler = std::function<Task<void>(Message)>;

  RaftTransport(Runtime* runtime, NodeId self, Duration reconnect_delay)
      : runtime_(runtime), self_(self), reconnect_delay_(reconnect_delay) {}

  RaftTransport(const RaftTransport&) = delete;
  RaftTransport& operator=(const RaftTransport&) = delete;

  void register_group(GroupId group, Handler handler) {
    handlers_[group.value()] = std::move(handler);
  }
  void unregister_group(GroupId group) { handlers_.erase(group.value()); }
  bool hosts(GroupId group) const { return handlers_.count(group.value()) != 0; }
  std::size_t group_count() const noexcept { return handlers_.size(); }

  // Non-Raft traffic on the same links: client requests and replies. One
  // handler per node, because the demultiplexing that matters here is by
  // MessageKind and the kinds are node-wide.
  void set_foreign_handler(std::function<void(const Message&)> handler) {
    foreign_ = std::move(handler);
  }

  // Starts a receive loop for this peer if there is not one already. Idempotent,
  // and it must be: groups are created and destroyed continuously by splits and
  // merges, and every one of them wants to hear from the same set of nodes.
  void listen_to(NodeId peer);

  void set_coalesce_heartbeats(bool on) noexcept { coalesce_ = on; }
  bool coalescing() const noexcept { return coalesce_; }

  // Sends one message. Heartbeats are buffered when coalescing is on; everything
  // else goes immediately, because delaying an append delays a commit.
  Task<Status> send(const RaftMessage& msg);

  // Sends whatever is buffered, one envelope per peer. Called once per tick by
  // whatever drives the groups.
  Task<void> flush();

  // Client traffic, addressed by node rather than by group.
  Task<Status> send_envelope(Message envelope);

  const TransportStats& stats() const noexcept { return stats_; }

 private:
  Task<void> recv_loop(NodeId peer);
  Task<void> deliver(Message envelope);
  Task<Status> raw_send(NodeId peer, Message envelope);

  Runtime* runtime_;
  NodeId self_;
  Duration reconnect_delay_;

  std::map<std::uint64_t, Handler> handlers_;
  std::function<void(const Message&)> foreign_;

  std::map<std::uint64_t, ConnHandle> connections_;
  std::set<std::uint64_t> listening_;

  // Buffered heartbeats, keyed by destination. A map rather than a hash so the
  // flush order is the peer order and not an allocation artifact.
  std::map<std::uint64_t, std::vector<std::string>> pending_;

  bool coalesce_ = false;
  TransportStats stats_;
};

}  // namespace anvil::raft

#endif  // ANVIL_CORE_RAFT_TRANSPORT_H_
