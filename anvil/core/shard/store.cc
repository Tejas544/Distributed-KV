#include "anvil/core/shard/store.h"

#include <algorithm>
#include <cstring>
#include <utility>

#include "anvil/core/lsm/format.h"
#include "anvil/core/raft/config.h"
#include "anvil/core/raft/message.h"

namespace anvil::shard {
namespace {

std::string account_key(std::uint32_t index) {
  std::string out = "acct";
  const std::string digits = std::to_string(index);
  out.append(4 - std::min<std::size_t>(4, digits.size()), '0');
  out += digits;
  return out;
}

Message make_envelope(NodeId from, NodeId to, MessageKind kind, const std::string& payload) {
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

std::string_view payload_view(const Message& msg) {
  return std::string_view{reinterpret_cast<const char*>(msg.payload.data()), msg.payload.size()};
}

void put_signed(std::string* out, std::int64_t value) {
  const std::uint64_t zigzag =
      (static_cast<std::uint64_t>(value) << 1) ^ static_cast<std::uint64_t>(value >> 63);
  lsm::put_varint64(out, zigzag);
}

const char* get_signed(const char* p, const char* limit, std::int64_t* value) {
  std::uint64_t zigzag = 0;
  p = lsm::get_varint64(p, limit, &zigzag);
  if (p == nullptr) return nullptr;
  *value = static_cast<std::int64_t>((zigzag >> 1) ^ (~(zigzag & 1) + 1));
  return p;
}

raft::ConfState conf_state_of(const RangeDescriptor& desc) {
  raft::ConfState state;
  for (const NodeId n : desc.replicas) state.voters.push_back(n.value());
  for (const NodeId n : desc.learners) state.learners.push_back(n.value());
  std::sort(state.voters.begin(), state.voters.end());
  std::sort(state.learners.begin(), state.learners.end());
  return state;
}

}  // namespace

RangeReplica::RangeReplica() = default;
RangeReplica::~RangeReplica() = default;
RangeReplica::RangeReplica(RangeReplica&&) noexcept = default;
RangeReplica& RangeReplica::operator=(RangeReplica&&) noexcept = default;

// ---------------------------------------------------------------------------
// construction
// ---------------------------------------------------------------------------

ShardStore::ShardStore(Runtime* runtime, NodeId self, StoreOptions options,
                       DeterministicRandom rng)
    : runtime_(runtime), self_(self), options_(std::move(options)), rng_(rng) {}

ShardStore::~ShardStore() = default;

void ShardStore::start(bool bootstrap) {
  bootstrap_ = bootstrap;
  transport_ = std::make_unique<raft::RaftTransport>(runtime_, self_, options_.raft.tick_interval);
  transport_->set_coalesce_heartbeats(true);
  transport_->set_foreign_handler([this](const Message& envelope) { handle_envelope(envelope); });
  // Every node in the cluster, not just the current members of some group. A
  // range can be assigned here at any moment by a rebalance, and a node that
  // only listens to the peers it already knows about is deaf to the group it is
  // about to join.
  for (std::uint32_t i = 1; i <= options_.cluster_size; ++i) transport_->listen_to(NodeId{i});

  topology_machine_ = std::make_unique<TopologyMachine>(options_.topology);

  raft::ConfState placement_conf;
  for (std::uint32_t i = 1; i <= options_.cluster_size; ++i) placement_conf.voters.push_back(i);
  placement_driver_ = std::make_unique<raft::RaftDriver>(
      runtime_, transport_.get(), GroupId{kMetaGroup}, self_, options_.raft, options_.durability,
      raft::Config::from_conf_state(placement_conf), topology_machine_.get(),
      DeterministicRandom{rng_.next_u64()});
  placement_driver_->set_external_ticks(true);

  // The timestamp oracle. Its own group, replicated on every node, so that a
  // reservation does not queue behind whatever the placement driver is doing
  // and so that its failover story is Raft's rather than a special case.
  oracle_machine_ = std::make_unique<txn::OracleMachine>();
  oracle_machine_->set_reserved_callback([this](txn::Ts first, std::uint64_t count) {
    // Every replica applies this; only the one that proposed it has anyone to
    // answer. Reservations are answered in proposal order because the log is in
    // proposal order -- there is nothing else to match them by, and nothing
    // else needed.
    if (pending_ts_.empty()) return;
    const PendingTs pending = pending_ts_.front();
    pending_ts_.erase(pending_ts_.begin());
    runtime_->spawn(ts_reply_to(pending.reply_to, pending.client, pending.seq, kOk, first,
                                count, NodeId{}));
  });
  oracle_driver_ = std::make_unique<raft::RaftDriver>(
      runtime_, transport_.get(), GroupId{kOracleGroup}, self_, options_.raft,
      options_.durability, raft::Config::from_conf_state(placement_conf),
      oracle_machine_.get(), DeterministicRandom{rng_.next_u64()});
  oracle_driver_->set_external_ticks(true);

  runtime_->spawn(placement_driver_->boot());
  runtime_->spawn(oracle_driver_->boot());
  runtime_->spawn(tick_loop());
  runtime_->spawn(reconcile_loop());
  runtime_->spawn(maintain_loop());
  runtime_->spawn(heartbeat_loop());
  started_ = true;
}

bool ShardStore::is_placement_leader() const {
  return placement_driver_ != nullptr && placement_driver_->ready() &&
         placement_driver_->node().role() == raft::Role::kLeader;
}

const RangeDescriptor* ShardStore::topology_descriptor(RangeId id) const {
  return topology_machine_->state().find(id);
}

void ShardStore::propose_admin(const AdminCommand& command) {
  if (placement_driver_ == nullptr || !placement_driver_->ready()) return;
  if (is_placement_leader()) {
    LogIndex assigned{};
    placement_driver_->propose(encode_admin(command), &assigned);
    return;
  }
  const NodeId leader = placement_driver_->node().leader();
  if (!leader.valid()) return;
  std::string out;
  out.push_back(2);  // an admin proposal forwarded to the placement leader
  lsm::put_length_prefixed(&out, encode_admin(command));
  runtime_->spawn([](ShardStore* store, NodeId to, std::string body) -> Task<void> {
    co_await store->transport_->send_envelope(
        make_envelope(store->self_, to, MessageKind::kClientRequest, body));
  }(this, leader, std::move(out)));
}

// ---------------------------------------------------------------------------
// the tick loop
// ---------------------------------------------------------------------------

Task<void> ShardStore::tick_loop() {
  for (;;) {
    co_await runtime_->sleep_for(options_.raft.tick_interval);
    ++tick_;
    ++stats_.ticks;

    // The placement group never quiesces. It is the one group every node needs
    // to be able to reach at any moment, and a topology that has to be woken up
    // before it can answer is a topology nobody can read during an incident.
    if (placement_driver_ != nullptr) co_await placement_driver_->tick_now();
    if (oracle_driver_ != nullptr) co_await oracle_driver_->tick_now();

    // Snapshotted first: ticking a group can create or retire another one, and
    // mutating the map being iterated across a suspension is the coroutine form
    // of iterator invalidation -- it corrupts the heap rather than failing
    // cleanly (CONTEXT.md 10.15).
    std::vector<std::uint64_t> ids;
    ids.reserve(ranges_.size());
    for (const auto& [id, replica] : ranges_) ids.push_back(id);

    for (const std::uint64_t id : ids) {
      auto it = ranges_.find(id);
      if (it == ranges_.end()) continue;
      RangeReplica& replica = it->second;
      if (replica.driver == nullptr || !replica.driver->ready()) continue;

      // Quiescence, and the reason MultiRaft exists: an idle range should cost
      // nothing. A replica that has not changed for a while stops ticking, so
      // it stops heartbeating and stops counting down to an election. Anything
      // arriving for it wakes it up again.
      const std::uint64_t revision = replica.machine->revision() + replica.driver->node().revision();
      if (revision != replica.last_revision) {
        replica.last_revision = revision;
        replica.idle_ticks = 0;
        if (replica.quiesced) {
          replica.quiesced = false;
          ++stats_.wakeups;
        }
      } else {
        ++replica.idle_ticks;
      }

      // A group with no leader may never quiesce, and this is not a tuning
      // choice. Ticking is what drives the election timeout, so a leaderless
      // group whose replicas have all stopped ticking has no way to elect one:
      // waking requires a message, and a message requires a leader. The range
      // stops serving, stops replicating, and stops being repairable, and
      // nothing anywhere reports an error -- the cluster simply has a hole in
      // its key space that no client happens to have addressed yet (ANV-0044).
      const bool leaderless = !replica.driver->node().leader().valid();
      if (leaderless && replica.quiesced) {
        replica.quiesced = false;
        replica.idle_ticks = 0;
        ++stats_.wakeups;
      }

      if (options_.quiescence_enabled && !replica.quiesced && !leaderless) {
        const bool leader = replica.driver->node().role() == raft::Role::kLeader;
        const std::uint32_t threshold = leader ? options_.quiesce_after_leader_ticks
                                               : options_.quiesce_after_follower_ticks;
        if (replica.idle_ticks >= threshold) {
          bool may_quiesce = true;
          if (leader && options_.range.quiesce_requires_caught_up) {
            // Every replica must hold the whole log. Quiescing over a follower
            // that is behind stops the only thing that would have caught it up:
            // it receives nothing, so it never advances, so it stays behind --
            // and the range looks perfectly healthy the entire time.
            const LogIndex last = replica.driver->node().log().last_index();
            for (const auto& [peer, progress] : replica.driver->node().progress()) {
              if (NodeId{peer} == self_) continue;
              if (progress.match < last) may_quiesce = false;
            }
          }
          if (may_quiesce) replica.quiesced = true;
        }
      }

      if (replica.quiesced) {
        ++stats_.ticks_skipped;
        ++stats_.quiesced_ticks;
        continue;
      }
      co_await replica.driver->tick_now();
    }

    co_await transport_->flush();
    collect_retired();
  }
}

// ---------------------------------------------------------------------------
// reconcile: the placement driver's decisions
// ---------------------------------------------------------------------------

Task<void> ShardStore::reconcile_loop() {
  for (;;) {
    co_await runtime_->sleep_for(options_.reconcile_interval);
    if (!is_placement_leader()) continue;

    const TopologyState& state = topology_machine_->state();
    if (state.ranges.empty()) {
      // Whichever node leads the placement group bootstraps. Nominating one
      // node for the job looks tidier and deadlocks the cluster whenever that
      // node is not the one elected; the command is idempotent -- the topology
      // is empty exactly once -- so there is nothing to coordinate.
      if (bootstrap_proposed_) continue;
      AdminCommand cmd;
      cmd.op = AdminOp::kBootstrap;
      cmd.time = runtime_->now().physical;
      for (std::uint32_t i = 1;
           i <= std::min<std::uint32_t>(options_.placement.target_replicas, options_.cluster_size);
           ++i) {
        cmd.replicas.push_back(NodeId{i});
      }
      LogIndex assigned{};
      if (placement_driver_->propose(encode_admin(cmd), &assigned).is_ok()) {
        bootstrap_proposed_ = true;
      }
      continue;
    }

    const std::vector<Decision> decisions =
        decide(state, options_.placement, runtime_->now(), options_.cluster_size);
    for (const Decision& decision : decisions) {
      LogIndex assigned{};
      if (placement_driver_->propose(encode_admin(decision.command), &assigned).is_ok()) {
        ++stats_.decisions_proposed;
      }
    }
  }
}

Task<void> ShardStore::heartbeat_loop() {
  for (;;) {
    co_await runtime_->sleep_for(options_.heartbeat_interval);
    if (placement_driver_ == nullptr || !placement_driver_->ready()) continue;
    // Liveness is replicated, not observed. A placement driver that decided
    // from its own view of who is reachable would decide differently on every
    // node, and every leader change would undo the previous leader's repairs.
    AdminCommand cmd;
    cmd.op = AdminOp::kNodeHeartbeat;
    cmd.node = self_;
    cmd.time = runtime_->now().physical;
    propose_admin(cmd);
  }
}

// ---------------------------------------------------------------------------
// materialise: groups exist because the topology says so
// ---------------------------------------------------------------------------

// Has the left neighbour already absorbed this frozen range, according to the
// survivor's own applied state?
//
// This is re-derived rather than remembered, and the difference is a crash. An
// in-memory tombstone is lost on restart, at which point the node sees a range
// the topology still lists, creates it, and its durable log replays the data
// the survivor already holds -- twelve accounts in two places and a total that
// does not add up (ANV-0047). The survivor's log survives the crash, so the
// answer can be recomputed from it.
bool ShardStore::absorbed_by_neighbour(const RangeDescriptor& desc) const {
  if (!desc.frozen) return false;
  // Any local range whose own applied span covers this one, not just the
  // immediate left neighbour in the topology. Merges chain: by the time the
  // topology catches up, the survivor may have swallowed two ranges and the
  // one in between is no longer anybody's neighbour -- so looking only at the
  // adjacent descriptor misses exactly the case that matters.
  for (const auto& [id, replica] : ranges_) {
    if (RangeId{id} == desc.id || replica.machine == nullptr) continue;
    if (!replica.machine->initialised()) continue;
    const RangeDescriptor& local = replica.machine->descriptor();
    if (local.start > desc.start) continue;
    // An empty end is +infinity, not the empty string. Compared as a string it
    // is the smallest value there is, which makes every survivor look like it
    // has absorbed nothing at all.
    if (local.end.empty()) return true;
    if (!desc.end.empty() && local.end >= desc.end) return true;
  }
  return false;
}

void ShardStore::materialise() {
  const TopologyState& state = topology_machine_->state();

  for (const auto& [start, desc] : state.ranges) {
    const bool mine = desc.hosts(self_);
    const auto existing = ranges_.find(desc.id.value());
    const bool gone = subsumed_.count(desc.id.value()) != 0 || absorbed_by_neighbour(desc);
    if (gone && existing != ranges_.end() && !existing->second.retired) {
      retire_group(desc.id);
      continue;
    }
    if (mine && existing == ranges_.end()) {
      // Not if this node already destroyed it as part of a merge, and not if
      // the survivor's log says it has already been absorbed. The topology
      // keeps the subsumed descriptor until the merge is finished, and
      // recreating the group in that window brings its data back from the dead.
      if (gone) continue;
      create_group(desc, false);
    } else if (!mine && existing != ranges_.end() && !existing->second.retired) {
      retire_group(desc.id);
    }
  }

  // Ranges that no longer exist at all -- the subsumed half of a merge.
  std::vector<RangeId> gone;
  for (const auto& [id, replica] : ranges_) {
    if (replica.retired) continue;
    if (state.find(RangeId{id}) == nullptr) gone.push_back(RangeId{id});
  }
  for (const RangeId id : gone) retire_group(id);

  // The tombstone is only needed while the topology still names the range.
  // Keeping it forever would stop a later rebalance from ever placing that id
  // here again -- ids are never reused, so this is belt and braces, but a
  // tombstone with no expiry is how a store slowly forgets how to host things.
  for (auto it = subsumed_.begin(); it != subsumed_.end();) {
    it = state.find(RangeId{*it}) == nullptr ? subsumed_.erase(it) : std::next(it);
  }
}

void ShardStore::create_group(const RangeDescriptor& desc, bool initialised) {
  auto machine = std::make_unique<RangeMachine>(desc, options_.range, initialised);
  RangeMachine* machine_ptr = machine.get();
  const RangeId id = desc.id;
  machine->set_apply_callback(
      [this, id](LogIndex index, const RangeCommand& cmd, ApplyOutcome outcome) {
        if (cmd.op != RangeOp::kTransfer) return;
        const auto it = pending_.find(cmd.op_id);
        if (it == pending_.end()) return;
        const Pending pending = it->second;
        pending_.erase(it);

        Reply reply;
        reply.client = pending.client;
        reply.seq = pending.seq;
        reply.range = id;
        reply.applied_index = index.value();  // the entry that decided it
        const auto replica = ranges_.find(id.value());
        if (replica != ranges_.end() && replica->second.machine != nullptr) {
          reply.generation = replica->second.machine->descriptor().generation;
          const auto& balances = replica->second.machine->balances();
          const auto account = balances.find(cmd.from);
          reply.value = account == balances.end() ? 0 : account->second;
        }
        switch (outcome) {
          case ApplyOutcome::kApplied:
          case ApplyOutcome::kDuplicate: reply.status = kOk; break;
          case ApplyOutcome::kInsufficientFunds: reply.status = kNoFunds; break;
          case ApplyOutcome::kWrongRange: reply.status = kWrongRange; break;
          case ApplyOutcome::kFrozen:
          case ApplyOutcome::kUninitialised: reply.status = kUnavailable; break;
        }
        runtime_->spawn(reply_to(pending.reply_to, reply));
      });

  machine->set_txn_callback([this, id](LogIndex index, const txn::TxnCommand& command,
                                      const txn::TxnResult& result) {
    (void)command;
    // Every replica applies this; only the one that proposed the entry at this
    // exact log index has a pending reply for it. Matched by (range, index),
    // not by draining whatever this node happens to have outstanding for the
    // range -- see the comment on pending_txn_ in store.h for why apply order
    // is not derivable from client id or sequence number.
    const auto it = pending_txn_.find({id.value(), index.value()});
    if (it == pending_txn_.end()) return;
    const PendingTxn pending = it->second;
    pending_txn_.erase(it);

    TxnReply reply;
    reply.client = pending.client;
    reply.seq = pending.seq;
    reply.status = kOk;
    reply.range = id;
    reply.result = result;
    reply.applied_index = index.value();
    const auto replica = ranges_.find(id.value());
    if (replica != ranges_.end() && replica->second.machine != nullptr) {
      reply.generation = replica->second.machine->descriptor().generation;
    }
    runtime_->spawn(txn_reply_to(pending.reply_to, reply));
  });

  RangeReplica replica;
  replica.machine = std::move(machine);
  replica.driver = std::make_unique<raft::RaftDriver>(
      runtime_, transport_.get(), desc.group(), self_, options_.raft, options_.durability,
      raft::Config::from_conf_state(conf_state_of(desc)), machine_ptr,
      DeterministicRandom{rng_.next_u64()});
  replica.driver->set_external_ticks(true);
  raft::RaftDriver* driver_ptr = replica.driver.get();
  ranges_[desc.id.value()] = std::move(replica);
  ++stats_.groups_created;
  runtime_->spawn(driver_ptr->boot());
}

void ShardStore::retire_group(RangeId id) {
  const auto it = ranges_.find(id.value());
  if (it == ranges_.end()) return;
  // Unregistered from the transport now, destroyed later.
  //
  // A group is retired from inside another group's apply, and its own receive
  // handler may be suspended in the middle of an fsync at that moment. Freeing
  // it here is a use-after-free that only fires on merge-heavy seeds. So it
  // stops being routable immediately -- which is the part that has to be
  // instant -- and the object is collected once its driver is idle.
  if (it->second.driver != nullptr) {
    transport_->unregister_group(GroupId{id.value()});
  }
  it->second.retired = true;
  it->second.retired_at_tick = tick_;
  retired_.push_back(std::move(it->second));
  ranges_.erase(it);
  ++stats_.groups_retired;
}

void ShardStore::collect_retired() {
  for (auto it = retired_.begin(); it != retired_.end();) {
    // `ready()` as well as `idle()`, and the difference is a use-after-free.
    //
    // A group can be created and retired again before its boot coroutine has
    // been scheduled -- a merge decided in the window between materialise() and
    // the first tick. Such a driver has never been busy, so it looks idle, and
    // freeing it leaves a spawned boot() pointing at a destroyed object. The
    // crash lands inside boot with a stack that says nothing about merges.
    const bool safe = it->driver == nullptr ||
                      (it->driver->ready() && it->driver->idle() &&
                       tick_ > it->retired_at_tick + 4);
    if (safe) {
      it = retired_.erase(it);
      ++stats_.groups_freed;
    } else {
      ++it;
    }
  }
}

// ---------------------------------------------------------------------------
// maintain: everything a range's leader owes the rest of the cluster
// ---------------------------------------------------------------------------

Task<void> ShardStore::maintain_loop() {
  for (;;) {
    co_await runtime_->sleep_for(options_.maintain_interval);
    if (topology_machine_ == nullptr) continue;
    materialise();
    co_await maintain_all();
  }
}

Task<void> ShardStore::maintain_all() {
  std::vector<std::uint64_t> ids;
  ids.reserve(ranges_.size());
  for (const auto& [id, replica] : ranges_) ids.push_back(id);
  for (const std::uint64_t id : ids) {
    if (ranges_.count(id) == 0) continue;
    maintain_range(RangeId{id});
  }
  co_return;
}

void ShardStore::maintain_range(RangeId id) {
  const auto it = ranges_.find(id.value());
  if (it == ranges_.end()) return;
  RangeReplica& replica = it->second;
  if (replica.driver == nullptr || !replica.driver->ready()) return;
  RangeMachine& machine = *replica.machine;

  // Groups whose merge trigger has been applied here: the subsumed group is
  // now redundant on this node and its data is in this range's log.
  for (const RangeId gone : machine.take_subsumed()) {
    subsumed_.insert(gone.value());
    retire_group(gone);
  }

  const bool leader = replica.driver->node().role() == raft::Role::kLeader;
  if (!leader) return;

  // A leader that has not yet applied its committed prefix does not know what
  // this range is. Everything below is a decision about the range's state, and
  // making one from a partial view is how a range gets initialised twice or
  // splits at a key it already split at.
  if (replica.driver->node().log().applied_index() < replica.driver->node().log().commit_index()) {
    return;
  }

  const RangeDescriptor* topo = topology_descriptor(id);

  // ---- initialisation ------------------------------------------------------
  if (!machine.initialised()) {
    if (replica.init_proposed) return;
    RangeCommand cmd;
    cmd.op = RangeOp::kInit;

    // Data from the parent, if this range was born from a split on this node.
    bool have_payload = false;
    for (const auto& [other_id, other] : ranges_) {
      if (other.machine == nullptr) continue;
      const auto& pending = other.machine->pending_split();
      if (pending.has_value() && pending->id == id) {
        cmd.payload = pending->payload;
        cmd.start = pending->start;
        cmd.end = pending->end;
        cmd.generation = 1;
        have_payload = true;
        break;
      }
    }
    // Otherwise this is the genesis range, and its contents are a function of
    // the configuration rather than of anything that happened.
    if (!have_payload && topo != nullptr && topo->start.empty() && topo->end.empty()) {
      std::map<std::string, std::int64_t> balances;
      for (std::uint32_t i = 0; i < options_.accounts; ++i) {
        balances[account_key(i)] = options_.initial_balance;
      }
      cmd.payload = RangeMachine::encode_payload(balances, {});
      cmd.start.clear();
      cmd.end.clear();
      cmd.generation = 1;
      have_payload = true;
    }
    if (!have_payload) {
      // This node leads a range born from a split and does not hold the half it
      // was split off with. That happens when the two ranges' replica sets have
      // drifted apart -- a rebalance moved the parent, or moved this range --
      // and it is terminal if nothing acts on it: the only copy of the data is
      // the parent's pending payload, on nodes that are not this group's leader
      // and therefore cannot propose anything into its log. The range stays
      // empty for the rest of the run and twelve accounts are simply gone
      // (ANV-0049).
      //
      // So leadership goes to a replica that does hold the parent. Chosen from
      // the replicated topology and in id order, so every node that runs this
      // picks the same one.
      const RangeDescriptor* parent = nullptr;
      for (const auto& [start, other] : topology_machine_->state().ranges) {
        if (other.id != id && other.end == topo->start) parent = &other;
      }
      if (parent != nullptr) {
        for (const NodeId candidate : topo->replicas) {
          if (candidate == self_ || !parent->hosts(candidate)) continue;
          replica.driver->transfer_leadership(candidate);
          break;
        }
      }
      return;  // otherwise the data is coming from the leader as a snapshot
    }

    LogIndex assigned{};
    if (replica.driver->propose(encode_range_command(cmd), &assigned).is_ok()) {
      replica.init_proposed = true;
    }
    return;
  }

  if (topo == nullptr) return;

  // ---- the lease -----------------------------------------------------------
  const std::uint64_t now = runtime_->now().physical;
  const Lease& lease = machine.descriptor().lease;
  const bool mine = lease.holder == self_;
  const bool expiring =
      mine && lease.expiry <= now + static_cast<std::uint64_t>(options_.lease_renew_before.nanos());
  if (!lease.valid_at(now) || expiring) {
    RangeCommand cmd;
    cmd.op = RangeOp::kGrantLease;
    cmd.node = self_;
    cmd.time = now;
    cmd.expiry = now + static_cast<std::uint64_t>(options_.lease_duration.nanos());
    LogIndex assigned{};
    if (replica.driver->propose(encode_range_command(cmd), &assigned).is_ok()) {
      if (mine) {
        ++stats_.lease_renewals;
      } else {
        ++stats_.leases_taken;
      }
    }
    // Published to the placement driver, so that clients can be routed to the
    // holder and so that a merge can require the two ranges to share one.
    AdminCommand publish;
    publish.op = AdminOp::kGrantLease;
    publish.range = id;
    publish.node = self_;
    publish.time = cmd.time;
    publish.value = cmd.expiry;
    propose_admin(publish);
  }

  // ---- freeze, when the topology says this range is being subsumed ---------
  if (topo->frozen && !machine.frozen()) {
    if (!replica.freeze_proposed) {
      RangeCommand cmd;
      cmd.op = RangeOp::kFreeze;
      LogIndex assigned{};
      if (replica.driver->propose(encode_range_command(cmd), &assigned).is_ok()) {
        replica.freeze_proposed = true;
      }
    }
    return;
  }

  // ---- the split trigger ---------------------------------------------------
  //
  // The topology has already decided; this turns that decision into an entry in
  // *this range's* log, which is what makes the split atomic with respect to
  // every transfer in flight. A transfer earlier in the log applies under the
  // old descriptor; one later is rejected against the new one. There is no
  // ordering in which half of one applies.
  if (!topo->end.empty() &&
      (machine.descriptor().end.empty() || topo->end < machine.descriptor().end)) {
    const auto right = topology_machine_->state().ranges.find(topo->end);
    if (right != topology_machine_->state().ranges.end() &&
        subsumed_.count(right->second.id.value()) == 0) {
      RangeCommand cmd;
      cmd.op = RangeOp::kSplitTrigger;
      cmd.end = topo->end;  // the split key: this range's new end
      cmd.other = right->second.id;
      cmd.generation = topo->generation;
      cmd.replicas = right->second.replicas;
      cmd.learners = right->second.learners;
      LogIndex assigned{};
      if (replica.driver->propose(encode_range_command(cmd), &assigned).is_ok()) {
        ++stats_.splits_executed;
      }
      return;
    }
  }

  // ---- the merge trigger ---------------------------------------------------
  //
  // Requires this node to be the leader of both groups. The subsumed range's
  // data is read from the local replica, and a follower's replica can be behind
  // the commit point -- reading it there would silently lose every write
  // committed after whatever this node happened to have applied.
  if (!machine.descriptor().end.empty()) {
    const auto right_topo = topology_machine_->state().ranges.find(machine.descriptor().end);
    if (right_topo != topology_machine_->state().ranges.end() && right_topo->second.frozen) {
      const auto right = ranges_.find(right_topo->second.id.value());
      if (right != ranges_.end() && right->second.driver != nullptr &&
          right->second.driver->ready() &&
          right->second.driver->node().role() == raft::Role::kLeader &&
          right->second.machine->frozen() &&
          right->second.driver->node().log().applied_index() >=
              right->second.driver->node().log().commit_index()) {
        // The span and the data both come from the subsumed range's own
        // machine, and that is the whole of the correctness argument.
        //
        // Taking the span from the topology and the data from the machine reads
        // two halves of one fact at two different times: the subsumed range may
        // itself have absorbed a neighbour that the topology has not caught up
        // with, and then the survivor takes in twelve accounts while recording
        // that it now owns six. The six left over are held by a range that does
        // not claim them, which no client can see and no single-range check can
        // find (ANV-0042).
        const RangeDescriptor& right_desc = right->second.machine->descriptor();
        RangeCommand cmd;
        cmd.op = RangeOp::kMergeTrigger;
        cmd.other = right_topo->second.id;
        cmd.start = right_desc.start;
        cmd.end = right_desc.end;
        cmd.generation = topo->generation + 1;
        cmd.payload = right->second.machine->encode_span(std::string_view{}, std::string_view{});
        LogIndex assigned{};
        if (replica.driver->propose(encode_range_command(cmd), &assigned).is_ok()) {
          ++stats_.merges_executed;
        }
        return;
      }
    }
  }

  // The merge is done here once this range's end has reached past its old
  // neighbour: tell the placement driver so the descriptor disappears.
  for (const auto& [start, desc] : topology_machine_->state().ranges) {
    if (desc.start != topo->end) continue;
    if (machine.descriptor().end != desc.end) continue;
    // This range has already absorbed that one: its own log says so. If the
    // topology has meanwhile *unfrozen* the subsumed range -- a merge timeout
    // that raced the absorb -- freezing it again is the only way forward. The
    // data is already here; the alternative is a descriptor for a range that
    // exists nowhere, which is a gap in the key space wearing a disguise.
    if (!desc.frozen) {
      AdminCommand refreeze;
      refreeze.op = AdminOp::kBeginMerge;
      refreeze.range = id;
      refreeze.other = desc.id;
      refreeze.time = now;
      ++stats_.refreezes_proposed;
      propose_admin(refreeze);
      break;
    }
    AdminCommand finish;
    finish.op = AdminOp::kFinishMerge;
    finish.range = id;
    finish.other = desc.id;
    finish.time = now;
    ++stats_.finishes_proposed;
    propose_admin(finish);
    break;
  }

  // ---- lease colocation, so that a merge can happen at all -----------------
  //
  // A merge requires both ranges to be led by the same node, and nothing in the
  // ordinary run of things makes that true: leases land wherever elections put
  // them. So the *right-hand* range's leader is the one that gives way. It can
  // see, from replicated state alone, that it and its left neighbour are both
  // small and share a replica set, and it hands its leadership to the left
  // neighbour's lease holder. Only one node can do this for any pair, which is
  // why there is nothing to coordinate.
  if (!topo->start.empty()) {
    const RangeDescriptor* left = nullptr;
    for (const auto& [start, desc] : topology_machine_->state().ranges) {
      if (desc.end == topo->start && desc.id != id) left = &desc;
    }
    const auto my_stats = topology_machine_->state().stats.find(id.value());
    const auto left_stats =
        left == nullptr ? topology_machine_->state().stats.end()
                        : topology_machine_->state().stats.find(left->id.value());
    if (left != nullptr && !left->frozen && left_stats != topology_machine_->state().stats.end() &&
        my_stats != topology_machine_->state().stats.end() &&
        my_stats->second.keys <= options_.placement.merge_threshold_keys &&
        left_stats->second.keys <= options_.placement.merge_threshold_keys &&
        left->replicas == topo->replicas && left->lease.holder.valid() &&
        left->lease.holder != self_ && topo->hosts(left->lease.holder)) {
      if (replica.driver->transfer_leadership(left->lease.holder).is_ok()) {
        ++stats_.lease_transfers;
      }
      return;
    }
  }

  // ---- publish the descriptor ---------------------------------------------
  if (topo->generation > machine.descriptor().generation ||
      topo->replicas != machine.descriptor().replicas ||
      topo->learners != machine.descriptor().learners) {
    if (replica.published_generation != topo->generation) {
      RangeCommand cmd;
      cmd.op = RangeOp::kSetDescriptor;
      cmd.generation = topo->generation;
      cmd.start = topo->start;
      cmd.end = topo->end;
      cmd.replicas = topo->replicas;
      cmd.learners = topo->learners;
      LogIndex assigned{};
      if (replica.driver->propose(encode_range_command(cmd), &assigned).is_ok()) {
        replica.published_generation = topo->generation;
        ++stats_.descriptors_published;
      }
    }
  }

  // ---- membership ----------------------------------------------------------
  //
  // The descriptor is the intent; the Raft configuration is the mechanism. They
  // are reconciled here, one change at a time, and Raft's joint consensus does
  // the actual transition.
  const raft::ConfState want = conf_state_of(*topo);
  const raft::ConfState have = replica.driver->node().config().to_conf_state();
  if (!replica.driver->node().config().joint() && want.voters != have.voters) {
    raft::ConfChange change;
    change.kind = raft::ConfChangeKind::kEnterJoint;
    change.voters = want.voters;
    change.learners = want.learners;
    replica.driver->propose_conf_change(change);
  } else if (!replica.driver->node().config().joint() && want.learners != have.learners) {
    raft::ConfChange change;
    change.kind = raft::ConfChangeKind::kEnterJoint;
    change.voters = want.voters;
    change.learners = want.learners;
    replica.driver->propose_conf_change(change);
  }

  // ---- reports back to the placement driver --------------------------------
  const std::uint64_t keys = machine.key_count();
  if (keys != replica.reported_keys) {
    AdminCommand report;
    report.op = AdminOp::kReportSize;
    report.range = id;
    report.value = keys;
    report.key = machine.median_key();
    report.time = now;
    propose_admin(report);
    replica.reported_keys = keys;
  }

  // Learners that now hold the committed prefix. Reported rather than inferred,
  // because the placement driver must be able to reach the same conclusion from
  // the replicated log alone (INV-SHARD-09).
  const LogIndex commit = replica.driver->node().log().commit_index();
  for (const auto& [peer, progress] : replica.driver->node().progress()) {
    if (NodeId{peer} == self_) continue;
    if (!topo->hosts(NodeId{peer})) continue;
    const bool is_learner =
        std::find(topo->learners.begin(), topo->learners.end(), NodeId{peer}) !=
        topo->learners.end();
    if (!is_learner || progress.match < commit) continue;
    const auto stats = topology_machine_->state().stats.find(id.value());
    if (stats != topology_machine_->state().stats.end() &&
        std::find(stats->second.caught_up.begin(), stats->second.caught_up.end(),
                  NodeId{peer}) != stats->second.caught_up.end()) {
      continue;
    }
    AdminCommand report;
    report.op = AdminOp::kReportCatchup;
    report.range = id;
    report.node = NodeId{peer};
    propose_admin(report);
  }

  // ---- confirm a split -----------------------------------------------------
  const auto& pending = machine.pending_split();
  if (pending.has_value()) {
    const auto child = ranges_.find(pending->id.value());
    if (child != ranges_.end() && child->second.machine != nullptr &&
        child->second.machine->initialised()) {
      RangeCommand cmd;
      cmd.op = RangeOp::kSplitConfirmed;
      cmd.other = pending->id;
      LogIndex assigned{};
      replica.driver->propose(encode_range_command(cmd), &assigned);
    }
  }
}

// ---------------------------------------------------------------------------
// the client protocol
// ---------------------------------------------------------------------------

bool ShardStore::route(const Request& request, Route* out) {
  RangeDescriptor desc;
  if (!cache_.lookup(request.from, &desc)) {
    if (!cache_.resolve(topology_machine_->state().meta, request.from, &desc)) return false;
  }
  out->range = desc.id;
  out->generation = desc.generation;
  out->one_range = request.read || desc.contains(request.to);
  // The lease holder if there is one, and otherwise any replica -- which will
  // answer kNotLeader with a hint, and the hint is how the client finds the
  // one that can serve it.
  out->node = desc.lease.holder.valid() ? desc.lease.holder
                                        : (desc.replicas.empty() ? NodeId{} : desc.replicas[0]);
  return out->node.valid();
}

Task<Status> ShardStore::send_request(Request request, Route route) {
  std::string out;
  out.push_back(request.read ? 1 : 0);
  lsm::put_varint64(&out, request.client);
  lsm::put_varint64(&out, request.seq);
  lsm::put_varint64(&out, route.range.value());
  lsm::put_varint64(&out, route.generation);
  lsm::put_length_prefixed(&out, request.from);
  lsm::put_length_prefixed(&out, request.to);
  put_signed(&out, request.amount);
  lsm::put_varint64(&out, request.op_id);
  co_return co_await transport_->send_envelope(
      make_envelope(self_, route.node, MessageKind::kClientRequest, out));
}

void ShardStore::handle_envelope(const Message& envelope) {
  if (envelope.kind == MessageKind::kClientRequest) {
    handle_request(envelope);
  } else if (envelope.kind == MessageKind::kClientReply) {
    handle_reply(envelope);
  }
}

void ShardStore::handle_request(const Message& envelope) {
  const std::string_view view = payload_view(envelope);
  const char* p = view.data();
  const char* limit = p + view.size();
  if (p >= limit) return;
  const auto type = static_cast<std::uint8_t>(*p++);

  if (type == 5) {
    handle_ts_request(envelope, std::string_view{p, static_cast<std::size_t>(limit - p)});
    return;
  }

  if (type == 3 || type == 4) {
    handle_txn_request(envelope, std::string_view{p, static_cast<std::size_t>(limit - p)});
    return;
  }

  if (type == 2) {
    // An admin proposal forwarded by another node. Accepted only here, on the
    // placement leader, and dropped otherwise: forwarding it again would build
    // a routing loop out of two nodes that each think the other is the leader.
    std::string_view body;
    p = lsm::get_length_prefixed(p, limit, &body);
    if (p == nullptr || !is_placement_leader()) return;
    AdminCommand cmd;
    if (!decode_admin(body, &cmd)) return;
    // Liveness is restamped with the leader's own clock. A heartbeat carrying
    // the sender's clock is compared, here, against this node's clock -- so a
    // node whose clock runs slow is declared dead while it is heartbeating
    // perfectly well, and its replicas are moved off it. One clock decides who
    // is alive, and it is the clock of whoever is doing the deciding.
    if (cmd.op == AdminOp::kNodeHeartbeat) cmd.time = runtime_->now().physical;
    LogIndex assigned{};
    placement_driver_->propose(encode_admin(cmd), &assigned);
    return;
  }

  Request request;
  request.read = type == 1;
  std::uint64_t range_id = 0;
  std::uint64_t generation = 0;
  p = lsm::get_varint64(p, limit, &request.client);
  if (p == nullptr) return;
  p = lsm::get_varint64(p, limit, &request.seq);
  if (p == nullptr) return;
  p = lsm::get_varint64(p, limit, &range_id);
  if (p == nullptr) return;
  p = lsm::get_varint64(p, limit, &generation);
  if (p == nullptr) return;
  std::string_view from;
  std::string_view to;
  p = lsm::get_length_prefixed(p, limit, &from);
  if (p == nullptr) return;
  p = lsm::get_length_prefixed(p, limit, &to);
  if (p == nullptr) return;
  p = get_signed(p, limit, &request.amount);
  if (p == nullptr) return;
  p = lsm::get_varint64(p, limit, &request.op_id);
  if (p == nullptr) return;
  request.from.assign(from);
  request.to.assign(to);
  ++stats_.client_requests;

  Reply reply;
  reply.client = request.client;
  reply.seq = request.seq;
  reply.range = RangeId{range_id};

  const auto it = ranges_.find(range_id);
  if (it == ranges_.end() || it->second.driver == nullptr || !it->second.driver->ready()) {
    reply.status = kWrongRange;
    const RangeDescriptor* topo = topology_descriptor(RangeId{range_id});
    if (topo != nullptr) {
      reply.generation = topo->generation;
      reply.leader_hint = topo->lease.holder;
    }
    ++stats_.client_wrong_range;
    runtime_->spawn(reply_to(envelope.from, reply));
    return;
  }
  RangeReplica& replica = it->second;
  RangeMachine& machine = *replica.machine;
  reply.generation = machine.descriptor().generation;

  // Any request wakes the range. A quiesced group that answered without
  // resuming its ticks would answer once and then stop replicating.
  if (replica.quiesced) {
    replica.quiesced = false;
    replica.idle_ticks = 0;
    ++stats_.wakeups;
  }

  const std::uint64_t now = runtime_->now().physical;

  if (request.read) {
    // Served locally under the lease, which is the only reason the lease exists.
    // Without a valid one this node cannot know that a newer leader has not
    // already served a write it has not seen.
    // A valid lease is not enough. The holder must also have applied everything
    // it knows is committed, and the case that makes this necessary is a
    // restart: the lease is in the durable log, so it comes back with the node,
    // and the node comes back with its state machine at the last snapshot and
    // its applied index climbing again. Serving in that window returns state
    // from before entries the *previous* holder had already served, which is a
    // read going backwards in real time with a perfectly valid lease behind it
    // (ANV-0048). The same condition covers a leader that has just been elected
    // and has committed entries it has not yet applied.
    const bool caught_up = replica.driver->node().log().applied_index() >=
                           replica.driver->node().log().commit_index();
    if (!machine.lease_valid(self_, now) || !machine.initialised() || !caught_up) {
      reply.status = kNotLeader;
      reply.leader_hint = machine.descriptor().lease.holder;
      ++stats_.client_not_leader;
      runtime_->spawn(reply_to(envelope.from, reply));
      return;
    }
    if (!machine.descriptor().contains(request.from)) {
      reply.status = kWrongRange;
      ++stats_.client_wrong_range;
      runtime_->spawn(reply_to(envelope.from, reply));
      return;
    }
    const auto& balances = machine.balances();
    const auto account = balances.find(request.from);
    reply.status = kOk;
    reply.value = account == balances.end() ? 0 : account->second;
    // The *log's* applied index, not the state machine's. The machine records
    // the index of the last entry it was handed, and a snapshot install hands
    // it a whole state with no index at all -- so the machine's number goes
    // backwards across an install while the state it describes goes forwards.
    // A client using it as a freshness measure reports a stale read that never
    // happened (ANV-0041).
    reply.applied_index = replica.driver->node().log().applied_index().value();
    ++stats_.reads_served;
    runtime_->spawn(reply_to(envelope.from, reply));
    return;
  }

  if (replica.driver->node().role() != raft::Role::kLeader) {
    reply.status = kNotLeader;
    reply.leader_hint = replica.driver->node().leader();
    ++stats_.client_not_leader;
    runtime_->spawn(reply_to(envelope.from, reply));
    return;
  }
  if (machine.frozen() || !machine.initialised()) {
    reply.status = kUnavailable;
    runtime_->spawn(reply_to(envelope.from, reply));
    return;
  }

  RangeCommand cmd;
  cmd.op = RangeOp::kTransfer;
  cmd.from = request.from;
  cmd.to = request.to;
  cmd.amount = request.amount;
  cmd.op_id = request.op_id;
  cmd.generation = generation;

  Pending pending;
  pending.reply_to = envelope.from;
  pending.client = request.client;
  pending.seq = request.seq;
  pending.range = RangeId{range_id};
  pending_[request.op_id] = pending;

  LogIndex assigned{};
  if (!replica.driver->propose(encode_range_command(cmd), &assigned).is_ok()) {
    pending_.erase(request.op_id);
    reply.status = kNotLeader;
    reply.leader_hint = replica.driver->node().leader();
    ++stats_.client_not_leader;
    runtime_->spawn(reply_to(envelope.from, reply));
  }
}

// ---------------------------------------------------------------------------
// the timestamp oracle
// ---------------------------------------------------------------------------

Task<bool> ShardStore::reserve_timestamps(std::uint64_t count, txn::Ts* first) {
  if (oracle_driver_ == nullptr || !oracle_driver_->ready()) co_return false;
  // A freshly-elected leader that has not finished replaying its own log does
  // not yet know the true high-water mark -- ready() only means booted, not
  // caught up. This is the same guard maintain_range() already applies before
  // treating a range's leadership as usable (ANV-0048's shape); the oracle
  // group needs it just as much, and serving a reservation from a stale
  // state_.high_water hands out a timestamp the previous leader already gave
  // to somebody else, which is exactly what INV-TXN-09 exists to catch.
  const bool leader = oracle_driver_->node().role() == raft::Role::kLeader;
  if (leader && oracle_driver_->node().log().applied_index() <
                    oracle_driver_->node().log().commit_index()) {
    co_return false;
  }

  const std::uint64_t seq = next_ts_seq_++;
  ts_inbox_[seq] = TsWaiter{};

  if (leader) {
    PendingTs pending;
    pending.reply_to = self_;
    pending.client = self_.value();
    pending.seq = seq;
    pending.count = count;
    pending_ts_.push_back(pending);
    LogIndex assigned{};
    if (!oracle_driver_->propose(txn::encode_reservation(count), &assigned).is_ok()) {
      pending_ts_.pop_back();
      ts_inbox_.erase(seq);
      co_return false;
    }
  } else {
    const NodeId leader = oracle_driver_->node().leader();
    if (!leader.valid()) {
      ts_inbox_.erase(seq);
      co_return false;
    }
    std::string out;
    out.push_back(5);
    lsm::put_varint64(&out, self_.value());
    lsm::put_varint64(&out, seq);
    lsm::put_varint64(&out, count);
    co_await transport_->send_envelope(
        make_envelope(self_, leader, MessageKind::kClientRequest, out));
  }

  const Timestamp deadline = runtime_->now().advanced_by(Duration::millis(1500));
  while (runtime_->now() < deadline) {
    co_await runtime_->sleep_for(Duration::millis(5));
    const auto it = ts_inbox_.find(seq);
    if (it == ts_inbox_.end()) break;
    if (!it->second.answered) continue;
    const bool ok = it->second.status == kOk;
    *first = it->second.first;
    ts_inbox_.erase(it);
    co_return ok;
  }
  ts_inbox_.erase(seq);
  co_return false;
}

void ShardStore::handle_ts_request(const Message& envelope, std::string_view body) {
  const char* p = body.data();
  const char* limit = p + body.size();
  std::uint64_t client = 0;
  std::uint64_t seq = 0;
  std::uint64_t count = 0;
  p = lsm::get_varint64(p, limit, &client);
  if (p == nullptr) return;
  p = lsm::get_varint64(p, limit, &seq);
  if (p == nullptr) return;
  p = lsm::get_varint64(p, limit, &count);
  if (p == nullptr) return;

  if (oracle_driver_ == nullptr || !oracle_driver_->ready() ||
      oracle_driver_->node().role() != raft::Role::kLeader ||
      oracle_driver_->node().log().applied_index() <
          oracle_driver_->node().log().commit_index()) {
    const NodeId hint =
        oracle_driver_ == nullptr ? NodeId{} : oracle_driver_->node().leader();
    runtime_->spawn(ts_reply_to(envelope.from, client, seq, kNotLeader, 0, 0, hint));
    return;
  }

  PendingTs pending;
  pending.reply_to = envelope.from;
  pending.client = client;
  pending.seq = seq;
  pending.count = count;
  pending_ts_.push_back(pending);
  LogIndex assigned{};
  if (!oracle_driver_->propose(txn::encode_reservation(count), &assigned).is_ok()) {
    pending_ts_.pop_back();
    runtime_->spawn(ts_reply_to(envelope.from, client, seq, kNotLeader, 0, 0, NodeId{}));
  }
}

Task<void> ShardStore::ts_reply_to(NodeId to, std::uint64_t client, std::uint64_t seq,
                                   std::uint8_t status, txn::Ts first, std::uint64_t count,
                                   NodeId hint) {
  std::string out;
  out.push_back(static_cast<char>(0xF7));
  lsm::put_varint64(&out, client);
  lsm::put_varint64(&out, seq);
  out.push_back(static_cast<char>(status));
  lsm::put_varint64(&out, first);
  lsm::put_varint64(&out, count);
  lsm::put_varint64(&out, hint.value());
  if (to == self_) {
    // The oracle leader asking itself. Delivering through the transport would
    // work in the simulator and is a lie: a node does not send itself a packet,
    // and pretending it does adds a network round trip to every timestamp on
    // whichever node happens to be the leader.
    handle_ts_reply(std::string_view{out.data() + 1, out.size() - 1});
    co_return;
  }
  co_await transport_->send_envelope(make_envelope(self_, to, MessageKind::kClientReply, out));
}

void ShardStore::handle_ts_reply(std::string_view body) {
  const char* p = body.data();
  const char* limit = p + body.size();
  std::uint64_t client = 0;
  std::uint64_t seq = 0;
  txn::Ts first = 0;
  std::uint64_t count = 0;
  std::uint64_t hint = 0;
  p = lsm::get_varint64(p, limit, &client);
  if (p == nullptr) return;
  p = lsm::get_varint64(p, limit, &seq);
  if (p == nullptr) return;
  if (p >= limit) return;
  const auto status = static_cast<std::uint8_t>(*p++);
  p = lsm::get_varint64(p, limit, &first);
  if (p == nullptr) return;
  p = lsm::get_varint64(p, limit, &count);
  if (p == nullptr) return;
  p = lsm::get_varint64(p, limit, &hint);
  if (p == nullptr) return;

  const auto it = ts_inbox_.find(seq);
  if (it == ts_inbox_.end()) return;
  it->second.answered = true;
  it->second.status = status;
  it->second.first = first;
  it->second.count = count;
}

// ---------------------------------------------------------------------------
// the transactional path
// ---------------------------------------------------------------------------

bool ShardStore::locate(std::string_view key, RangeDescriptor* out) {
  if (cache_.lookup(key, out)) return true;
  return cache_.resolve(topology_machine_->state().meta, key, out);
}

Task<Status> ShardStore::send_txn(TxnRequest request, NodeId to) {
  std::string out;
  out.push_back(request.read ? 4 : 3);
  lsm::put_varint64(&out, request.client);
  lsm::put_varint64(&out, request.seq);
  lsm::put_varint64(&out, request.range.value());
  lsm::put_varint64(&out, request.generation);
  if (request.read) {
    lsm::put_length_prefixed(&out, request.key);
    lsm::put_varint64(&out, request.read_ts);
    lsm::put_varint64(&out, request.uncertainty_limit);
    lsm::put_varint64(&out, request.reader);
  } else {
    lsm::put_length_prefixed(&out, txn::encode_txn_command(request.command));
  }
  co_return co_await transport_->send_envelope(
      make_envelope(self_, to, MessageKind::kClientRequest, out));
}

void ShardStore::handle_txn_request(const Message& envelope, std::string_view body) {
  const char* p = body.data();
  const char* limit = p + body.size();
  TxnReply reply;
  std::uint64_t range_id = 0;
  p = lsm::get_varint64(p, limit, &reply.client);
  if (p == nullptr) return;
  p = lsm::get_varint64(p, limit, &reply.seq);
  if (p == nullptr) return;
  p = lsm::get_varint64(p, limit, &range_id);
  if (p == nullptr) return;
  std::uint64_t generation = 0;
  p = lsm::get_varint64(p, limit, &generation);
  if (p == nullptr) return;
  reply.range = RangeId{range_id};
  ++stats_.client_requests;

  // Which of the two shapes follows is in the type byte the dispatcher already
  // consumed, so it is re-read from the original payload rather than guessed at
  // from the length. Deciding by length works until the day a field grows.
  const std::string_view whole = payload_view(envelope);
  const bool is_read = !whole.empty() && static_cast<std::uint8_t>(whole.front()) == 4;

  const auto it = ranges_.find(range_id);
  if (it == ranges_.end() || it->second.driver == nullptr || !it->second.driver->ready()) {
    reply.status = kWrongRange;
    const RangeDescriptor* topo = topology_descriptor(RangeId{range_id});
    if (topo != nullptr) {
      reply.generation = topo->generation;
      reply.leader_hint = topo->lease.holder;
    }
    ++stats_.client_wrong_range;
    runtime_->spawn(txn_reply_to(envelope.from, reply));
    return;
  }

  RangeReplica& replica = it->second;
  RangeMachine& machine = *replica.machine;
  reply.generation = machine.descriptor().generation;
  if (replica.quiesced) {
    replica.quiesced = false;
    replica.idle_ticks = 0;
    ++stats_.wakeups;
  }

  if (is_read) {
    std::string_view key;
    txn::Ts read_ts = 0;
    txn::Ts uncertainty = 0;
    txn::TxnId reader = 0;
    p = lsm::get_length_prefixed(p, limit, &key);
    if (p == nullptr) return;
    p = lsm::get_varint64(p, limit, &read_ts);
    if (p == nullptr) return;
    p = lsm::get_varint64(p, limit, &uncertainty);
    if (p == nullptr) return;
    p = lsm::get_varint64(p, limit, &reader);
    if (p == nullptr) return;

    // A snapshot read is served locally, under the lease, by a replica that has
    // applied everything it knows is committed. Both conditions, for the reason
    // ANV-0048 records: the lease is durable and comes back with a restarted
    // node while its state machine is still catching up.
    const std::uint64_t now = runtime_->now().physical;
    const bool caught_up = replica.driver->node().log().applied_index() >=
                           replica.driver->node().log().commit_index();
    if (!machine.lease_valid(self_, now) || !machine.initialised() || !caught_up) {
      reply.status = kNotLeader;
      reply.leader_hint = machine.descriptor().lease.holder;
      ++stats_.client_not_leader;
      runtime_->spawn(txn_reply_to(envelope.from, reply));
      return;
    }
    if (!machine.descriptor().contains(key) ||
        (generation != 0 && generation != machine.descriptor().generation)) {
      reply.status = kWrongRange;
      ++stats_.client_wrong_range;
      runtime_->spawn(txn_reply_to(envelope.from, reply));
      return;
    }
    reply.status = kOk;
    reply.read = machine.txn_store().get(key, read_ts, reader, uncertainty);
    reply.applied_index = replica.driver->node().log().applied_index().value();
    ++stats_.reads_served;
    runtime_->spawn(txn_reply_to(envelope.from, reply));
    return;
  }

  std::string_view encoded;
  p = lsm::get_length_prefixed(p, limit, &encoded);
  if (p == nullptr) return;
  txn::TxnCommand command;
  if (!txn::decode_txn_command(encoded, &command)) return;

  if (replica.driver->node().role() != raft::Role::kLeader) {
    reply.status = kNotLeader;
    reply.leader_hint = replica.driver->node().leader();
    ++stats_.client_not_leader;
    runtime_->spawn(txn_reply_to(envelope.from, reply));
    return;
  }
  if (machine.frozen() || !machine.initialised()) {
    reply.status = kUnavailable;
    runtime_->spawn(txn_reply_to(envelope.from, reply));
    return;
  }

  RangeCommand entry;
  entry.op = RangeOp::kTxn;
  entry.from = command.key;
  entry.generation = generation;
  entry.txn_command = std::string{encoded};

  LogIndex assigned{};
  if (!replica.driver->propose(encode_range_command(entry), &assigned).is_ok()) {
    reply.status = kNotLeader;
    reply.leader_hint = replica.driver->node().leader();
    ++stats_.client_not_leader;
    runtime_->spawn(txn_reply_to(envelope.from, reply));
    return;
  }

  // Keyed by the index this exact proposal was assigned, known only now that
  // propose() has returned it -- see the comment on pending_txn_ in store.h.
  PendingTxn pending;
  pending.reply_to = envelope.from;
  pending.client = reply.client;
  pending.seq = reply.seq;
  pending.range = RangeId{range_id};
  pending_txn_[{range_id, assigned.value()}] = pending;
}

Task<void> ShardStore::txn_reply_to(NodeId to, TxnReply reply) {
  std::string out;
  out.push_back(static_cast<char>(0xF6));
  lsm::put_varint64(&out, reply.client);
  lsm::put_varint64(&out, reply.seq);
  out.push_back(static_cast<char>(reply.status));
  lsm::put_varint64(&out, reply.range.value());
  lsm::put_varint64(&out, reply.generation);
  lsm::put_varint64(&out, reply.leader_hint.value());
  lsm::put_varint64(&out, reply.applied_index);
  lsm::put_length_prefixed(&out, txn::encode_txn_result(reply.result));
  out.push_back(static_cast<char>(reply.read.status));
  out.push_back(reply.read.found ? 1 : 0);
  lsm::put_length_prefixed(&out, reply.read.value);
  lsm::put_varint64(&out, reply.read.commit_ts);
  lsm::put_varint64(&out, reply.read.blocker);
  lsm::put_varint32(&out, reply.read.blocker_epoch);
  lsm::put_varint64(&out, reply.read.blocker_start);
  lsm::put_length_prefixed(&out, reply.read.blocker_primary);
  lsm::put_varint64(&out, reply.read.uncertain_at);
  co_await transport_->send_envelope(make_envelope(self_, to, MessageKind::kClientReply, out));
}

void ShardStore::handle_txn_reply(std::string_view body) {
  const char* p = body.data();
  const char* limit = p + body.size();
  TxnReply reply;
  p = lsm::get_varint64(p, limit, &reply.client);
  if (p == nullptr) return;
  p = lsm::get_varint64(p, limit, &reply.seq);
  if (p == nullptr) return;
  if (p >= limit) return;
  reply.status = static_cast<std::uint8_t>(*p++);
  std::uint64_t range_id = 0;
  p = lsm::get_varint64(p, limit, &range_id);
  if (p == nullptr) return;
  reply.range = RangeId{range_id};
  p = lsm::get_varint64(p, limit, &reply.generation);
  if (p == nullptr) return;
  std::uint64_t hint = 0;
  p = lsm::get_varint64(p, limit, &hint);
  if (p == nullptr) return;
  reply.leader_hint = NodeId{hint};
  p = lsm::get_varint64(p, limit, &reply.applied_index);
  if (p == nullptr) return;
  std::string_view result;
  p = lsm::get_length_prefixed(p, limit, &result);
  if (p == nullptr) return;
  if (!txn::decode_txn_result(result, &reply.result)) return;
  if (p + 2 > limit) return;
  const auto read_status = static_cast<std::uint8_t>(*p++);
  if (read_status > static_cast<std::uint8_t>(txn::ReadStatus::kUnavailable)) return;
  reply.read.status = static_cast<txn::ReadStatus>(read_status);
  reply.read.found = *p++ != 0;
  std::string_view value;
  p = lsm::get_length_prefixed(p, limit, &value);
  if (p == nullptr) return;
  reply.read.value.assign(value);
  p = lsm::get_varint64(p, limit, &reply.read.commit_ts);
  if (p == nullptr) return;
  p = lsm::get_varint64(p, limit, &reply.read.blocker);
  if (p == nullptr) return;
  p = lsm::get_varint32(p, limit, &reply.read.blocker_epoch);
  if (p == nullptr) return;
  p = lsm::get_varint64(p, limit, &reply.read.blocker_start);
  if (p == nullptr) return;
  std::string_view primary;
  p = lsm::get_length_prefixed(p, limit, &primary);
  if (p == nullptr) return;
  reply.read.blocker_primary.assign(primary);
  p = lsm::get_varint64(p, limit, &reply.read.uncertain_at);
  if (p == nullptr) return;

  if (reply.status == kWrongRange) {
    cache_.note_stale_rejection();
    cache_.invalidate(reply.range);
  }
  if (on_txn_reply_) on_txn_reply_(reply);
}

Task<void> ShardStore::reply_to(NodeId to, Reply reply) {
  std::string out;
  lsm::put_varint64(&out, reply.client);
  lsm::put_varint64(&out, reply.seq);
  out.push_back(static_cast<char>(reply.status));
  put_signed(&out, reply.value);
  lsm::put_varint64(&out, reply.applied_index);
  lsm::put_varint64(&out, reply.generation);
  lsm::put_varint64(&out, reply.leader_hint.value());
  lsm::put_varint64(&out, reply.range.value());
  co_await transport_->send_envelope(make_envelope(self_, to, MessageKind::kClientReply, out));
}

void ShardStore::handle_reply(const Message& envelope) {
  const std::string_view view = payload_view(envelope);
  const char* p = view.data();
  const char* limit = p + view.size();
  if (p < limit && static_cast<std::uint8_t>(*p) == 0xF7) {
    handle_ts_reply(std::string_view{p + 1, static_cast<std::size_t>(limit - p - 1)});
    return;
  }
  if (p < limit && static_cast<std::uint8_t>(*p) == 0xF6) {
    // A transactional reply. Tagged rather than inferred from length, because
    // "whatever does not parse as the other one" is a decoder that silently
    // hands a client somebody else's answer the first time a field grows.
    handle_txn_reply(std::string_view{p + 1, static_cast<std::size_t>(limit - p - 1)});
    return;
  }
  Reply reply;
  p = lsm::get_varint64(p, limit, &reply.client);
  if (p == nullptr) return;
  p = lsm::get_varint64(p, limit, &reply.seq);
  if (p == nullptr) return;
  if (p >= limit) return;
  reply.status = static_cast<std::uint8_t>(*p++);
  p = get_signed(p, limit, &reply.value);
  if (p == nullptr) return;
  p = lsm::get_varint64(p, limit, &reply.applied_index);
  if (p == nullptr) return;
  p = lsm::get_varint64(p, limit, &reply.generation);
  if (p == nullptr) return;
  std::uint64_t hint = 0;
  std::uint64_t range = 0;
  p = lsm::get_varint64(p, limit, &hint);
  if (p == nullptr) return;
  p = lsm::get_varint64(p, limit, &range);
  if (p == nullptr) return;
  reply.leader_hint = NodeId{hint};
  reply.range = RangeId{range};

  if (reply.status == kWrongRange) {
    // The cluster says this client's cached descriptor is out of date. Dropping
    // the entry is what forces the next attempt through the meta index; keeping
    // it means retrying against the same wrong range forever.
    cache_.note_stale_rejection();
    cache_.invalidate(reply.range);
  }
  if (on_reply_) on_reply_(reply);
}

}  // namespace anvil::shard
