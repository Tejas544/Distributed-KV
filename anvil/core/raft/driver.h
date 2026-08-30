// The only part of Raft that touches a Runtime.
//
// The state machine in raft.h decides *what* must happen; this decides when the
// bytes hit the disk and when they hit the wire, and it does so in one loop so
// that the ordering is a property of six lines rather than of a hundred call
// sites:
//
//     truncate -> append -> hard state -> fsync -> SEND -> apply -> advance
//
// Reverse the fsync and the send and Raft is still, superficially, Raft. Every
// test passes. Elections work, replication works, throughput improves. What
// breaks is only visible when a node crashes in a window of a few hundred
// microseconds: a voter forgets a vote it already granted and grants a second
// one in the same term, and the cluster ends up with two leaders that both
// believe they are legitimate. That is `persist_before_reply`, and it is the
// most valuable single line in this file.
//
// Re-entrancy is handled by a busy flag rather than a lock. Ticks and message
// arrivals both want to drain the state machine, and they are both coroutines
// on one thread, so the hazard is not a data race -- it is two interleaved
// persist-then-send sequences, which would let a message from the second batch
// overtake the fsync of the first. The flag collapses them into one loop.

#ifndef ANVIL_CORE_RAFT_DRIVER_H_
#define ANVIL_CORE_RAFT_DRIVER_H_

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "anvil/core/raft/config.h"
#include "anvil/core/raft/raft.h"
#include "anvil/core/raft/storage.h"
#include "anvil/core/raft/transport.h"
#include "anvil/core/raft/types.h"
#include "anvil/core/runtime/runtime.h"

namespace anvil::raft {

// What the replicated log is replicating. Deliberately narrow: apply, snapshot,
// restore. A state machine that needs anything else from Raft (the current
// term, the leader's identity) is a state machine whose correctness depends on
// consensus internals, which is the coupling this interface exists to prevent.
class StateMachine {
 public:
  virtual ~StateMachine();
  virtual void apply(LogIndex index, std::string_view command) = 0;
  virtual std::string snapshot() const = 0;
  virtual void restore(std::string_view data) = 0;
};

struct DriverStats {
  std::uint64_t applied = 0;
  std::uint64_t ready_batches = 0;
  std::uint64_t entries_persisted = 0;
  std::uint64_t truncations = 0;
  std::uint64_t fsyncs = 0;
  std::uint64_t messages_sent = 0;
  std::uint64_t messages_received = 0;
  std::uint64_t send_failures = 0;
  std::uint64_t reconnects = 0;
  std::uint64_t storage_failures = 0;
  std::uint64_t recover_retries = 0;
  std::uint64_t snapshots_taken = 0;
  std::uint64_t snapshots_installed = 0;
  // Recovery discarded durable state because a checksum failed. Counted rather
  // than logged: the difference between "the media was damaged and we noticed"
  // and "the protocol lost data" cannot be established after the fact without
  // a number (ANV-0009).
  std::uint64_t log_truncations_on_recovery = 0;
  std::uint64_t state_truncations_on_recovery = 0;
  std::uint64_t snapshot_corruptions = 0;
  std::uint64_t recoveries = 0;
  std::uint64_t persisted_high = 0;
  std::uint64_t fsynced_high = 0;
  std::uint64_t rewrites = 0;
  const char* last_truncate_reason = "none";

  // What the last recovery actually read back. Kept for the same reason
  // counter.cc keeps its per-stage counters: "the node came back wrong" has
  // several causes and none of them are distinguishable without these.
  std::uint64_t recovered_entries = 0;
  std::uint64_t recovered_snapshot_index = 0;
  std::uint64_t recovered_hard_term = 0;
  std::uint64_t recovered_hard_commit = 0;  // highest index this incarnation made durable
};

class RaftDriver {
 public:
  // The transport is shared with every other group on this node and outlives
  // this driver; the driver registers itself on boot and deregisters in its
  // destructor. Passing it in rather than owning it is what makes one group per
  // range possible at all -- see transport.h.
  RaftDriver(Runtime* runtime, RaftTransport* transport, GroupId group, NodeId self,
             RaftOptions options, RaftDurability durability, Config bootstrap,
             StateMachine* machine, DeterministicRandom rng);
  ~RaftDriver();

  RaftDriver(const RaftDriver&) = delete;
  RaftDriver& operator=(const RaftDriver&) = delete;

  // Recovers from disk and starts the tick loop. Re-invoked on every restart,
  // so it must assume durable state left by a previous incarnation -- which is
  // to say, it must perform recovery.
  Task<void> boot();

  // Suppresses this driver's own tick loop. With hundreds of groups on a node,
  // hundreds of independent tick timers is both a scheduling cost and a
  // correctness hazard: heartbeats can only be coalesced if the groups that
  // produce them tick together. The multi-group store ticks every group and
  // then flushes, which is what real MultiRaft does.
  void set_external_ticks(bool external) noexcept { external_ticks_ = external; }

  // One tick from outside. Only meaningful with external ticks on.
  Task<void> tick_now();

  GroupId group() const noexcept { return group_; }

  // Client entry points. All return kNotLeader away from the leader; the caller
  // is expected to redirect rather than to retry blindly.
  Status propose(std::string command, LogIndex* assigned);
  Status propose_conf_change(const ConfChange& change);
  Status read_index(std::uint64_t context);
  Status transfer_leadership(NodeId target);

  // Invoked when a ReadIndex becomes safe to serve. The caller must still wait
  // for applied_index() to reach the reported index.
  void set_read_callback(std::function<void(ReadState)> callback) {
    read_callback_ = std::move(callback);
  }

  // Invoked at the end of every recovery with whether durable state had to be
  // discarded because a checksum failed. Synchronous on purpose: a harness that
  // learns about media damage by polling learns about it after the invariant
  // that the damage tripped has already fired.
  void set_recovery_callback(std::function<void(bool damaged)> callback) {
    recovery_callback_ = std::move(callback);
  }

  const RaftNode& node() const noexcept { return node_; }
  RaftNode& node() noexcept { return node_; }
  const DriverStats& stats() const noexcept { return stats_; }
  bool ready() const noexcept { return booted_; }

  // No batch is being drained right now. The multi-group store uses this to
  // decide when a retired group is safe to free: every path into this driver
  // goes through pump(), and pump() holds this flag for the whole of a
  // persist-then-send sequence, so "not busy" means nothing is suspended
  // inside it.
  bool idle() const noexcept { return !busy_; }
  LogIndex applied_index() const noexcept { return node_.log().applied_index(); }

  // Drains one Ready batch. Public because tests drive it directly; the loops
  // call it on every tick and every received message.
  Task<void> pump();

 private:
  Task<void> tick_loop();
  Task<void> apply_ready(Ready& ready);
  Task<Status> persist(Ready& ready);
  Task<void> dispatch(const std::vector<RaftMessage>& messages);
  Task<void> maybe_compact();

  Runtime* runtime_;
  RaftTransport* transport_;
  GroupId group_;
  NodeId self_;
  RaftOptions options_;
  RaftNode node_;
  RaftStorage storage_;
  StateMachine* machine_;

  // Messages that have arrived but have not been stepped yet, and ticks that
  // have fired but have not been applied.
  //
  // Nothing mutates the state machine while a persist is in flight. The fsync
  // is a real suspension, and stepping a message inside it means the batch
  // being written no longer describes the log by the time it lands: a reply
  // generated before the suspension can claim entries a conflicting append has
  // since truncated, and the leader counts that claim toward a quorum. Real
  // implementations serialise the two for exactly this reason; queueing here is
  // what makes the Ready model equivalent to a single-threaded step loop.
  std::vector<Message> inbox_;
  std::uint32_t pending_ticks_ = 0;

  std::function<void(ReadState)> read_callback_;
  std::function<void(bool)> recovery_callback_;
  Config bootstrap_;

  bool booted_ = false;
  bool busy_ = false;
  bool again_ = false;
  bool external_ticks_ = false;
  DriverStats stats_;
};

}  // namespace anvil::raft

#endif  // ANVIL_CORE_RAFT_DRIVER_H_
