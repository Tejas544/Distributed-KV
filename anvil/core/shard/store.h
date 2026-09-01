// One node's range server: many Raft groups, one set of links, one tick loop.
//
// This is to the sharding layer what driver.cc is to consensus -- the only file
// here that holds a Runtime. Everything above it is a state machine that cannot
// tell the time.
//
// It owns:
//   * the transport, shared by every group on this node;
//   * a replica of the placement driver's group, on every node, so that the
//     topology is something each store reads out of its own applied state
//     rather than something it asks a service for;
//   * a replica of every range assigned to this node;
//   * one tick loop that ticks all of them and then flushes the coalesced
//     heartbeats. Per-group tick loops would make coalescing impossible and
//     would put one timer per range on the scheduler.
//
// And it does four jobs on a timer:
//   1. Reconcile. On the placement leader, run decide() over the replicated
//      topology and propose what it returns.
//   2. Materialise. Create a local group for every range this node should host
//      and retire the ones it should not, so "the topology says so" is the only
//      reason a group exists.
//   3. Maintain. On a range's leader: take or renew the lease, publish the
//      descriptor into the range's own log, report size and catch-up back to
//      the placement driver, and carry out splits and merges.
//   4. Quiesce. Stop ticking a group that has nothing to do, which is the point
//      of MultiRaft: a thousand idle ranges should cost nothing.
//
// The one rule that governs all of it: a group is created or destroyed only as
// a consequence of an entry that has been applied. Never because a message
// arrived, never because a timer fired, never because this node's opinion
// changed. Two replicas that disagree about which groups exist are two replicas
// that will disagree about everything else a moment later.

#ifndef ANVIL_CORE_SHARD_STORE_H_
#define ANVIL_CORE_SHARD_STORE_H_

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "anvil/core/raft/driver.h"
#include "anvil/core/raft/transport.h"
#include "anvil/core/shard/descriptor.h"
#include "anvil/core/shard/placement.h"
#include "anvil/core/shard/range.h"
#include "anvil/core/shard/router.h"
#include "anvil/core/shard/topology.h"
#include "anvil/core/txn/command.h"
#include "anvil/core/txn/timestamp.h"
#include "anvil/core/runtime/runtime.h"

namespace anvil::shard {

struct StoreOptions {
  std::uint32_t cluster_size = 5;
  raft::RaftOptions raft;
  raft::RaftDurability durability;
  TopologyOptions topology;
  PlacementOptions placement;
  RangeOptions range;

  Duration lease_duration = Duration::millis(600);
  Duration lease_renew_before = Duration::millis(200);
  Duration reconcile_interval = Duration::millis(250);
  Duration maintain_interval = Duration::millis(100);
  Duration heartbeat_interval = Duration::millis(300);

  // Ticks of silence before a replica stops ticking. The follower number is
  // larger on purpose: the leader must go quiet first, or the followers stop
  // ticking while the leader is still heartbeating and wake up immediately.
  std::uint32_t quiesce_after_leader_ticks = 4;
  std::uint32_t quiesce_after_follower_ticks = 9;
  bool quiescence_enabled = true;

  // The accounts every range starts with, and what each one holds. The store
  // needs these because bootstrap writes them into the first range.
  std::uint32_t accounts = 24;
  std::int64_t initial_balance = 100;
};

struct StoreStats {
  std::uint64_t ticks = 0;
  std::uint64_t groups_created = 0;
  std::uint64_t groups_retired = 0;
  std::uint64_t groups_freed = 0;
  std::uint64_t decisions_proposed = 0;
  std::uint64_t splits_executed = 0;
  std::uint64_t merges_executed = 0;
  std::uint64_t merges_abandoned = 0;
  std::uint64_t finishes_proposed = 0;
  std::uint64_t refreezes_proposed = 0;
  std::uint64_t leases_taken = 0;
  std::uint64_t lease_renewals = 0;
  std::uint64_t lease_transfers = 0;
  std::uint64_t descriptors_published = 0;
  std::uint64_t client_requests = 0;
  std::uint64_t client_wrong_range = 0;
  std::uint64_t client_not_leader = 0;
  std::uint64_t reads_served = 0;
  std::uint64_t quiesced_ticks = 0;
  std::uint64_t ticks_skipped = 0;
  std::uint64_t wakeups = 0;
};

// One hosted range: its state machine, its Raft group, and the bookkeeping the
// store needs to decide whether to tick it.
struct RangeReplica {
  RangeReplica();
  ~RangeReplica();
  RangeReplica(RangeReplica&&) noexcept;
  RangeReplica& operator=(RangeReplica&&) noexcept;

  std::unique_ptr<RangeMachine> machine;
  std::unique_ptr<raft::RaftDriver> driver;

  bool quiesced = false;
  std::uint32_t idle_ticks = 0;
  std::uint64_t last_revision = 0;
  std::uint64_t retired_at_tick = 0;
  bool retired = false;

  // Set once this replica has told the placement driver it has caught up, so
  // the report is not resent on every maintenance pass.
  bool reported_catchup = false;
  std::uint64_t reported_keys = ~0ULL;
  std::uint64_t published_generation = 0;
  bool init_proposed = false;
  bool freeze_proposed = false;
};

class ShardStore {
 public:
  ShardStore(Runtime* runtime, NodeId self, StoreOptions options, DeterministicRandom rng);
  ~ShardStore();

  ShardStore(const ShardStore&) = delete;
  ShardStore& operator=(const ShardStore&) = delete;

  // Starts the placement replica and every loop. Called on every boot,
  // including after a crash, so it recovers rather than assuming a blank node.
  // `bootstrap` is true for exactly one node in the cluster and only has an
  // effect when the topology is empty.
  void start(bool bootstrap);

  // ---- observation, for the checker and the workload ---------------------
  NodeId self() const noexcept { return self_; }
  const TopologyMachine& topology() const noexcept { return *topology_machine_; }
  const raft::RaftDriver* placement_driver() const noexcept { return placement_driver_.get(); }
  const std::map<std::uint64_t, RangeReplica>& ranges() const noexcept { return ranges_; }
  const raft::RaftTransport& transport() const noexcept { return *transport_; }
  raft::RaftTransport& transport() noexcept { return *transport_; }
  const StoreStats& stats() const noexcept { return stats_; }
  const StoreOptions& options() const noexcept { return options_; }
  RangeCache& cache() noexcept { return cache_; }
  const RangeCache& cache() const noexcept { return cache_; }

  // The client half. The workload calls this; it resolves the key through the
  // cache and the meta index, and sends the request to whoever should serve it.
  // Returns the node it addressed, or NodeId{} if it could not resolve one.
  struct Request {
    bool read = false;
    std::string from;
    std::string to;
    std::int64_t amount = 0;
    std::uint64_t op_id = 0;
    std::uint64_t client = 0;
    std::uint64_t seq = 0;
  };
  struct Route {
    NodeId node{};
    RangeId range{};
    std::uint64_t generation = 0;
    bool one_range = false;  // the cached view says one range covers both keys
  };
  bool route(const Request& request, Route* out);

  // By value, and every coroutine below it too. A Task is lazy: nothing in the
  // body runs until it is awaited or spawned, by which point the caller's frame
  // may be gone. A reference parameter is captured as a reference in the
  // coroutine frame, so `spawn(reply_to(to, local_reply))` reads a destroyed
  // object -- reliably, silently, and as plausible-looking garbage rather than
  // as a crash. See ANV-0034.
  Task<Status> send_request(Request request, Route route);

  // Replies arrive here, decoded, for the workload's client to handle.
  struct Reply {
    std::uint64_t client = 0;
    std::uint64_t seq = 0;
    std::uint8_t status = 0;
    std::int64_t value = 0;
    std::uint64_t applied_index = 0;
    std::uint64_t generation = 0;
    NodeId leader_hint{};
    RangeId range{};
  };
  void set_reply_handler(std::function<void(const Reply&)> handler) {
    on_reply_ = std::move(handler);
  }

  // ---- the transactional path (P6) ---------------------------------------
  //
  // A second client protocol beside the bank's, rather than an extension of
  // it. The two ask different questions: a transfer is one round trip whose
  // answer is yes or no, and a transactional operation is one step of a
  // protocol whose answer may be "someone else holds this and here is who".
  // Folding them together would mean a Reply struct with two disjoint halves
  // and a flag saying which one is real.
  struct TxnRequest {
    bool read = false;
    RangeId range{};
    std::uint64_t generation = 0;
    std::uint64_t client = 0;
    std::uint64_t seq = 0;

    // read
    std::string key;
    txn::Ts read_ts = 0;
    txn::Ts uncertainty_limit = 0;
    txn::TxnId reader = 0;

    // write
    txn::TxnCommand command;
  };

  struct TxnReply {
    std::uint64_t client = 0;
    std::uint64_t seq = 0;
    std::uint8_t status = 0;
    RangeId range{};
    std::uint64_t generation = 0;
    NodeId leader_hint{};
    txn::TxnResult result;   // writes
    txn::ReadResult read;    // reads
    std::uint64_t applied_index = 0;
  };

  Task<Status> send_txn(TxnRequest request, NodeId to);

  // Reserves `count` timestamps from the replicated oracle, returning the
  // first. Asks the local replica when it leads the oracle group and
  // forwards to the leader otherwise; either way the reservation is a
  // committed Raft entry before a single number is handed out, which is the
  // whole of INV-TXN-09.
  Task<bool> reserve_timestamps(std::uint64_t count, txn::Ts* first);

  const txn::OracleMachine& oracle() const noexcept { return *oracle_machine_; }
  const raft::RaftDriver* oracle_driver() const noexcept { return oracle_driver_.get(); }
  void set_oracle_options(txn::OracleOptions options) { oracle_options_ = options; }
  void set_txn_reply_handler(std::function<void(const TxnReply&)> handler) {
    on_txn_reply_ = std::move(handler);
  }

  // Where a key lives, by the cache and then the meta index. The
  // transactional coordinator needs this directly: it addresses several
  // ranges in one transaction and cannot express that as a single Route.
  bool locate(std::string_view key, RangeDescriptor* out);

  // Status codes on the wire. Deliberately not StatusCode: this is a protocol
  // between two nodes and it has to survive one of them being older.
  static constexpr std::uint8_t kOk = 0;
  static constexpr std::uint8_t kWrongRange = 1;
  static constexpr std::uint8_t kNotLeader = 2;
  static constexpr std::uint8_t kNoFunds = 3;
  static constexpr std::uint8_t kUnavailable = 4;

 private:
  Task<void> tick_loop();
  Task<void> reconcile_loop();
  Task<void> maintain_loop();
  Task<void> heartbeat_loop();

  void materialise();                 // create and retire groups to match the topology
  bool absorbed_by_neighbour(const RangeDescriptor& desc) const;
  void maintain_range(RangeId id);    // lease, descriptor, size, split, merge
  Task<void> maintain_all();

  void create_group(const RangeDescriptor& desc, bool initialised);
  void retire_group(RangeId id);
  void collect_retired();

  void handle_envelope(const Message& envelope);
  void handle_request(const Message& envelope);
  void handle_reply(const Message& envelope);
  void handle_txn_request(const Message& envelope, std::string_view body);
  void handle_txn_reply(std::string_view body);
  void handle_ts_request(const Message& envelope, std::string_view body);
  void handle_ts_reply(std::string_view body);
  Task<void> ts_reply_to(NodeId to, std::uint64_t client, std::uint64_t seq,
                         std::uint8_t status, txn::Ts first, std::uint64_t count,
                         NodeId hint);
  Task<void> reply_to(NodeId to, Reply reply);
  Task<void> txn_reply_to(NodeId to, TxnReply reply);

  bool is_placement_leader() const;
  const RangeDescriptor* topology_descriptor(RangeId id) const;

  // Proposes an admin command to the placement group: directly when this node
  // leads it, and by forwarding to the leader otherwise. Every range's leader
  // has things to tell the placement driver -- its lease, its size, which of
  // its learners have caught up -- and requiring it to also lead the placement
  // group would mean those reports only ever arrive from one node.
  void propose_admin(const AdminCommand& command);

  Runtime* runtime_;
  NodeId self_;
  StoreOptions options_;
  DeterministicRandom rng_;

  std::unique_ptr<raft::RaftTransport> transport_;
  std::unique_ptr<TopologyMachine> topology_machine_;
  std::unique_ptr<raft::RaftDriver> placement_driver_;

  txn::OracleOptions oracle_options_;
  std::unique_ptr<txn::OracleMachine> oracle_machine_;
  std::unique_ptr<raft::RaftDriver> oracle_driver_;

  // Timestamp reservations proposed and waiting for their entry to apply,
  // in proposal order. The oracle's log is a sequence of "take the next N",
  // so the reservation that applies first belongs to the request that was
  // proposed first.
  struct PendingTs {
    NodeId reply_to{};
    std::uint64_t client = 0;
    std::uint64_t seq = 0;
    std::uint64_t count = 0;
  };
  std::vector<PendingTs> pending_ts_;

  // A reservation this node is waiting for, as a client.
  struct TsWaiter {
    bool answered = false;
    std::uint8_t status = 0;
    txn::Ts first = 0;
    std::uint64_t count = 0;
  };
  std::map<std::uint64_t, TsWaiter> ts_inbox_;
  std::uint64_t next_ts_seq_ = 1;

  std::map<std::uint64_t, RangeReplica> ranges_;
  std::vector<RangeReplica> retired_;

  // Ranges this node has destroyed because another range absorbed them.
  //
  // Without this the store loops: the merge trigger retires the subsumed group
  // locally, the topology still lists it for one more Raft round trip, and the
  // next materialise pass sees a range it should host and no local replica --
  // so it creates one, which elects a leader, takes a lease, and drives the
  // whole merge again. It looks like a working cluster doing an enormous amount
  // of work, and the only visible symptom is the group churn counter (ANV-0035).
  std::set<std::uint64_t> subsumed_;

  RangeCache cache_;

  // Client requests accepted and waiting for their entry to be applied.
  struct Pending {
    NodeId reply_to{};
    std::uint64_t client = 0;
    std::uint64_t seq = 0;
    RangeId range{};
  };
  std::map<std::uint64_t, Pending> pending_;  // keyed by op id

  std::function<void(const Reply&)> on_reply_;
  std::function<void(const TxnReply&)> on_txn_reply_;

  // Transactional commands proposed and waiting for their entry to apply,
  // keyed by (range, the log index this node's own propose() was assigned).
  // That index is the one and only fact that ties a pending reply to the
  // exact apply that decides it: a range's log can carry proposals from any
  // number of concurrent client requests, possibly from different nodes
  // entirely, and nothing about apply order is derivable from client or
  // sequence number -- a client with a numerically larger id or a later
  // sequence number is not guaranteed to have proposed later. Draining
  // "whichever pending entry for this range sorts first" (the previous
  // scheme, keyed by (client << 32) | seq) matched replies to applies by
  // coincidence, and under two coordinators contending for one range the
  // coincidence fails: an apply is paired with the wrong client's request,
  // that client is told someone else's result, and the request whose result
  // it stole waits out its RPC timeout having genuinely committed -- which is
  // indistinguishable, from the coordinator's own reply cache, from having
  // never happened at all.
  struct PendingTxn {
    NodeId reply_to{};
    std::uint64_t client = 0;
    std::uint64_t seq = 0;
    RangeId range{};
  };
  std::map<std::pair<std::uint64_t, std::uint64_t>, PendingTxn> pending_txn_;

  bool started_ = false;
  bool bootstrap_ = false;
  bool bootstrap_proposed_ = false;
  std::uint64_t tick_ = 0;
  StoreStats stats_;
};

}  // namespace anvil::shard

#endif  // ANVIL_CORE_SHARD_STORE_H_
