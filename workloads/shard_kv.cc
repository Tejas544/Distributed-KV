#include "workloads/shard_kv.h"

#include <algorithm>
#include <string>

namespace anvil::workloads {
namespace {

using anvil::shard::ShardStore;

std::string account_key(std::uint32_t index) {
  std::string out = "acct";
  const std::string digits = std::to_string(index);
  out.append(4 - std::min<std::size_t>(4, digits.size()), '0');
  out += digits;
  return out;
}

ShardKvNode* node_of(ShardKvState* state, NodeId self) {
  const auto it = state->nodes.find(self.value());
  if (it == state->nodes.end()) return nullptr;
  return &it->second;
}

// The freshest live view of the topology. Not "the leader's": during an
// election there is no leader, and an audit that gives up whenever the cluster
// is between leaders gives up exactly when it matters.
const shard::TopologyState* freshest_topology(sim::Simulation& simulation,
                                              const ShardKvState& state) {
  const shard::TopologyState* best = nullptr;
  LogIndex best_at{};
  for (const auto& [id, node] : state.nodes) {
    if (node.store == nullptr) continue;
    if (!simulation.process().alive(NodeId{id})) continue;
    const raft::RaftDriver* driver = node.store->placement_driver();
    if (driver == nullptr || !driver->ready()) continue;
    const LogIndex applied = driver->node().log().applied_index();
    if (best == nullptr || applied > best_at) {
      best = &node.store->topology().state();
      best_at = applied;
    }
  }
  return best;
}

// The replica of a range whose machine is the most advanced among live nodes.
// Preferring the leader would be more principled and less useful: after a heal
// there may be no leader for a moment, and every replica of a converged range
// holds the same data anyway.
const shard::RangeMachine* best_replica(sim::Simulation& simulation, const ShardKvState& state,
                                        RangeId range) {
  const shard::RangeMachine* best = nullptr;
  LogIndex best_at{};
  for (const auto& [id, node] : state.nodes) {
    if (node.store == nullptr) continue;
    if (!simulation.process().alive(NodeId{id})) continue;
    const auto it = node.store->ranges().find(range.value());
    if (it == node.store->ranges().end() || it->second.machine == nullptr) continue;
    if (!it->second.machine->initialised()) continue;
    const LogIndex applied = it->second.machine->applied_index();
    if (best == nullptr || applied > best_at) {
      best = it->second.machine.get();
      best_at = applied;
    }
  }
  return best;
}

// ---------------------------------------------------------------------------
// the client
// ---------------------------------------------------------------------------

void start_request(Runtime& rt, const ShardKvConfig& cfg, ShardKvState* state, NodeId self) {
  ShardKvNode* node = node_of(state, self);
  if (node == nullptr || node->store == nullptr) return;

  DeterministicRandom& rng = rt.rng(RandomDomain::kWorkload);
  const std::uint32_t from_index =
      static_cast<std::uint32_t>(rng.next_u64() % cfg.accounts);
  const std::uint32_t offset =
      1 + static_cast<std::uint32_t>(rng.next_u64() % std::max<std::uint32_t>(1, cfg.neighbourhood));
  const std::uint32_t to_index = (from_index + offset) % cfg.accounts;

  ShardStore::Request request;
  request.read = (rng.next_u64() % 100) < cfg.read_percent;
  request.from = account_key(from_index);
  request.to = account_key(to_index);
  request.amount = 1 + static_cast<std::int64_t>(rng.next_u64() % cfg.max_transfer);
  request.client = self.value();
  request.seq = node->next_seq;
  // The operation id is what makes a retry a retry. It is stable across every
  // attempt, so the range's decision ledger answers the second attempt with
  // what it decided for the first rather than applying the transfer twice.
  request.op_id = (self.value() << 32) | node->next_seq;

  node->pending = request;
  node->in_flight = true;
  node->reply_pending = false;
}

Task<void> client_loop(Runtime& rt, ShardKvConfig cfg, ShardKvState* state, NodeId self) {
  // Let the cluster bootstrap before asking it for anything. Without this every
  // client spends its first second discovering that the topology is empty,
  // which is true, uninteresting, and burns the run's budget.
  co_await rt.sleep_for(Duration::millis(400));

  while (true) {
    ShardKvNode* node = node_of(state, self);
    if (node == nullptr || node->store == nullptr) co_return;
    if (node->ops_done >= cfg.ops_per_client) co_return;

    if (!node->in_flight) start_request(rt, cfg, state, self);
    ShardKvNode* current = node_of(state, self);
    if (current == nullptr) co_return;

    ShardStore::Route route;
    if (!current->store->route(current->pending, &route)) {
      // No topology yet, or none that covers this key. Both resolve by waiting.
      co_await rt.sleep_for(cfg.client_interval);
      continue;
    }
    if (current->leader_hint.valid()) {
      route.node = current->leader_hint;
      current->leader_hint = NodeId{};
    } else {
      const auto known = current->range_leader.find(route.range.value());
      if (known != current->range_leader.end()) route.node = known->second;
    }
    current->route = route;
    if (!current->pending.read) {
      if (route.one_range) {
        ++state->straddling_attempts;
      } else {
        // The cache says the two accounts are in different ranges. A single
        // range cannot move money between them and this phase has no
        // distributed transaction, so the client gives up on this pair and
        // picks another. Counted, because the number is the honest measure of
        // how much of the workload P6 is going to have to carry.
        ++state->declined_cross_range;
        current->in_flight = false;
        ++current->next_seq;
        co_await rt.sleep_for(cfg.client_interval);
        continue;
      }
    }

    current->reply_pending = false;
    co_await current->store->send_request(current->pending, route);

    const Timestamp deadline = rt.now().advanced_by(cfg.client_timeout);
    bool answered = false;
    while (rt.now() < deadline) {
      co_await rt.sleep_for(cfg.client_poll);
      ShardKvNode* n = node_of(state, self);
      if (n == nullptr || n->store == nullptr) co_return;
      if (!n->reply_pending) continue;
      answered = true;
      break;
    }

    ShardKvNode* n = node_of(state, self);
    if (n == nullptr || n->store == nullptr) co_return;
    if (!answered) {
      ++state->client_timeouts;
      ++state->client_retries;
      // Same request, same op id. Anything else manufactures duplicate
      // applications that are the harness's fault, not the system's.
      continue;
    }

    const ShardStore::Reply reply = n->reply;
    n->reply_pending = false;

    if (reply.status == ShardStore::kOk) {
      if (n->pending.read) {
        ++state->reads_served;
        // The client-visible half of the lease property. A read served under a
        // lease must never come back from further behind than one already has:
        // the whole point of the lease is that its holder has applied
        // everything any previous holder served.
        auto& mark = state->read_high_water[reply.range.value()];
        if (reply.applied_index < mark) {
          ++state->stale_lease_reads;
          state->violations.push_back(
              "a lease read of r" + std::to_string(reply.range.value()) + " came back at index " +
              std::to_string(reply.applied_index) + " after one at index " +
              std::to_string(mark));
        } else {
          mark = reply.applied_index;
        }
      } else {
        AckedOp op;
        op.op_id = n->pending.op_id;
        op.from = n->pending.from;
        op.to = n->pending.to;
        op.amount = n->pending.amount;
        op.applied = true;
        op.range = reply.range;
        op.when = rt.now();
        state->acked[op.op_id] = op;
        ++state->transfers_acked;
        ++state->transfers_applied;
      }
      n->in_flight = false;
      ++n->next_seq;
      ++n->ops_done;
    } else if (reply.status == ShardStore::kNoFunds) {
      AckedOp op;
      op.op_id = n->pending.op_id;
      op.from = n->pending.from;
      op.to = n->pending.to;
      op.amount = n->pending.amount;
      op.applied = false;
      op.range = reply.range;
      op.when = rt.now();
      state->acked[op.op_id] = op;
      ++state->transfers_acked;
      ++state->transfers_declined;
      n->in_flight = false;
      ++n->next_seq;
      ++n->ops_done;
    } else if (reply.status == ShardStore::kWrongRange) {
      ++state->wrong_range_replies;
      ++state->client_retries;
      // The store has already dropped the cached descriptor. The next attempt
      // re-resolves through the meta index; the op id does not change, so if
      // the request did land somewhere the retry will get that answer back.
    } else {
      if (reply.status == ShardStore::kNotLeader) {
        ++state->not_leader_replies;
        // Redirected. The hint is whatever that node's Raft replica believes,
        // which is exactly as good a guess as any the client could make.
        if (reply.leader_hint.valid()) {
          n->leader_hint = reply.leader_hint;
          n->range_leader[reply.range.value()] = reply.leader_hint;
        }
      }
      ++state->client_retries;
      co_await rt.sleep_for(cfg.client_interval);
    }
  }
}

// ---------------------------------------------------------------------------
// boot
// ---------------------------------------------------------------------------

void boot_node(sim::Simulation& simulation, ShardKvConfig cfg, ShardKvState* state, NodeId self,
               checker::ShardObserver* observer) {
  Runtime& rt = simulation.node(self);
  ShardKvNode& node = state->nodes[self.value()];

  // A crash destroys volatile state. Everything is rebuilt and then recovered
  // from disk; carrying anything across would be the node remembering something
  // it has no right to remember.
  node.in_flight = false;
  node.reply_pending = false;
  node.leader_hint = NodeId{};
  node.range_leader.clear();
  node.booted = false;
  ++node.boots;
  node.store.reset();

  shard::StoreOptions store_options = cfg.store;
  store_options.cluster_size = state->node_count;
  store_options.accounts = cfg.accounts;
  store_options.initial_balance = cfg.initial_balance;
  store_options.range.clock_uncertainty_nanos = static_cast<std::uint64_t>(
      simulation.config().faults.clock.declared_uncertainty.nanos());

  auto store = std::make_unique<shard::ShardStore>(
      &rt, self, store_options, DeterministicRandom{rt.rng(RandomDomain::kPlacement).next_u64()});
  shard::ShardStore* store_ptr = store.get();
  store->set_reply_handler([state, self](const ShardStore::Reply& reply) {
    ShardKvNode* n = node_of(state, self);
    if (n == nullptr) return;
    if (reply.client != self.value() || reply.seq != n->pending.seq) return;
    n->reply = reply;
    n->reply_pending = true;
  });

  node.store = std::move(store);
  node.booted = true;
  if (observer != nullptr) observer->set_store(self, store_ptr);

  store_ptr->start(self == NodeId{1});
  rt.spawn(client_loop(rt, cfg, state, self));
}

}  // namespace

ShardKvNode::ShardKvNode() = default;
ShardKvNode::~ShardKvNode() = default;
ShardKvNode::ShardKvNode(ShardKvNode&&) noexcept = default;
ShardKvNode& ShardKvNode::operator=(ShardKvNode&&) noexcept = default;

// ---------------------------------------------------------------------------
// installation
// ---------------------------------------------------------------------------

void install(sim::Simulation& simulation, ShardKvConfig config, ShardKvState* state,
             checker::ShardObserver* observer) {
  const std::uint32_t nodes = simulation.node_count();
  if (nodes < 3) throw sim::SimulationPanic("the shard workload needs at least three nodes");

  state->node_count = nodes;
  state->simulation = &simulation;
  state->observer = observer;
  state->config = config;
  state->expected_balance =
      static_cast<std::int64_t>(config.accounts) * config.initial_balance;

  // The lease is only as good as the clock bound the environment declares, so
  // take it from the environment rather than assuming one -- the same rule the
  // Raft lease follows.
  config.store.raft.max_clock_uncertainty =
      simulation.config().faults.clock.declared_uncertainty;

  if (observer != nullptr) {
    checker::ShardObserver::Hooks hooks;
    hooks.tick = [&simulation]() { return simulation.scheduler().tick(); };
    hooks.true_now = [&simulation]() { return simulation.scheduler().now(); };
    hooks.alive = [&simulation](NodeId id) { return simulation.process().alive(id); };
    hooks.node_now = [&simulation](NodeId id) { return simulation.node(id).now(); };
    observer->configure(nodes, std::move(hooks));
    observer->set_clock_uncertainty(simulation.config().faults.clock.declared_uncertainty);
    checker::arm_shard_invariants(simulation.invariants(), observer);
  }

  // The client-visible checks, armed here rather than in the checker because
  // both are statements about what a client saw.
  simulation.invariants().arm(
      "INV-SHARD-CLIENT", "a lease read never returns state older than one already returned",
      checker::CostClass::kTick, [state]() -> std::optional<std::string> {
        if (state->stale_lease_reads == 0) return std::nullopt;
        state->stale_lease_reads = 0;  // reported once per occurrence
        return state->violations.empty() ? std::string{"a stale lease read"}
                                         : state->violations.back();
      });

  for (std::uint32_t i = 1; i <= nodes; ++i) {
    const NodeId self{i};
    state->nodes[i] = ShardKvNode{};
    simulation.set_boot(self, [&simulation, config, state, self, observer]() {
      boot_node(simulation, config, state, self, observer);
    });
  }
  for (std::uint32_t i = 1; i <= nodes; ++i) {
    boot_node(simulation, config, state, NodeId{i}, observer);
  }
}

// ---------------------------------------------------------------------------
// audits
// ---------------------------------------------------------------------------

NodeId placement_leader(const ShardKvState& state, sim::Simulation& simulation) {
  for (const auto& [id, node] : state.nodes) {
    if (node.store == nullptr) continue;
    if (!simulation.process().alive(NodeId{id})) continue;
    const raft::RaftDriver* driver = node.store->placement_driver();
    if (driver != nullptr && driver->ready() &&
        driver->node().role() == raft::Role::kLeader) {
      return NodeId{id};
    }
  }
  return NodeId{};
}

std::size_t range_count(sim::Simulation& simulation, const ShardKvState& state) {
  const shard::TopologyState* topology = freshest_topology(simulation, state);
  return topology == nullptr ? 0 : topology->ranges.size();
}

std::int64_t audit_conservation(sim::Simulation& simulation, ShardKvState* state) {
  const shard::TopologyState* topology = freshest_topology(simulation, *state);
  if (topology == nullptr) {
    state->violations.push_back("no live node has a topology to audit");
    return 0;
  }
  state->ranges_final = topology->ranges.size();

  std::int64_t total = 0;
  std::map<std::string, int> owners;  // key -> how many ranges hold it
  for (const auto& [start, desc] : topology->ranges) {
    // A frozen range whose survivor has already absorbed it still exists in the
    // topology -- the descriptor goes away one Raft round trip later -- and its
    // own machine still holds the data that is now also in the survivor.
    // Counting both is the audit double-counting a legal intermediate state,
    // which is a finding manufactured by the checker rather than one about the
    // system (CONTEXT.md 10.20).
    if (desc.frozen) {
      bool absorbed = false;
      for (const auto& [other_start, other] : topology->ranges) {
        if (other.id == desc.id) continue;
        const shard::RangeMachine* survivor = best_replica(simulation, *state, other.id);
        if (survivor == nullptr) continue;
        // Any range whose applied span already covers this one, not just the
        // immediate neighbour: merges chain, and by the time the topology
        // catches up the survivor may be two descriptors away.
        if (survivor->descriptor().start > desc.start) continue;
        const std::string& survivor_end = survivor->descriptor().end;
        // An empty end means +infinity, and comparing it as a string makes it
        // the *smallest* value instead of the largest. Written as a plain
        // `survivor_end >= desc.end`, every frozen range at the top of the key
        // space looks absorbed, and the audit skips a range that is very much
        // still there -- reporting six accounts missing out of twenty-four on a
        // cluster that had not lost anything at all (ANV-0043).
        const bool covers = survivor_end.empty() ||
                            (!desc.end.empty() && survivor_end >= desc.end);
        if (covers) absorbed = true;
      }
      if (absorbed) continue;
    }
    const shard::RangeMachine* machine = best_replica(simulation, *state, desc.id);
    if (machine == nullptr) {
      // The range exists in the topology and no live replica has its data yet.
      // That is the split window: the parent has moved the keys out and the
      // child has not committed its kInit. The parent still holds the payload,
      // and it is counted below.
      continue;
    }
    for (const auto& [key, value] : machine->balances()) {
      total += value;
      ++owners[key];
    }
  }

  // The half of a split that has left its parent and not yet reached its child.
  for (const auto& [id, node] : state->nodes) {
    if (node.store == nullptr) continue;
    if (!simulation.process().alive(NodeId{id})) continue;
    for (const auto& [range_id, replica] : node.store->ranges()) {
      if (replica.machine == nullptr) continue;
      const auto& pending = replica.machine->pending_split();
      if (!pending.has_value()) continue;
      // Only when the child genuinely has no data anywhere, or it would be
      // counted twice. The parent keeps the payload until the split is
      // confirmed, which is deliberately later than the child having it.
      if (best_replica(simulation, *state, pending->id) != nullptr) continue;
      std::map<std::string, std::int64_t> balances;
      std::map<std::uint64_t, shard::ApplyOutcome> decided;
      if (!shard::RangeMachine::decode_payload(pending->payload, &balances, &decided)) continue;
      for (const auto& [key, value] : balances) {
        if (owners.count(key) != 0) continue;  // already counted from a replica
        total += value;
        ++owners[key];
      }
      // No break: a node can be the parent of more than one unhanded-over
      // split at a time, and stopping at the first one silently drops the rest.
      // The owners map is what stops the same payload being counted twice when
      // several replicas of the parent hold it.
    }
  }

  for (const auto& [key, count] : owners) {
    if (count > 1) {
      state->violations.push_back("'" + key + "' is held by " + std::to_string(count) +
                                  " ranges at once");
    }
  }
  if (owners.size() != state->config.accounts) {
    state->violations.push_back(
        "the cluster holds " + std::to_string(owners.size()) + " accounts; it started with " +
        std::to_string(state->config.accounts));
  }

  state->total_balance = total;
  return total;
}

void audit_ledger(sim::Simulation& simulation, ShardKvState* state) {
  // Every decision the cluster still remembers, unioned over live replicas. A
  // range's ledger is copied into both halves of a split and merged into the
  // survivor of a merge, so the union is complete as long as some replica of
  // every live range is up.
  std::map<std::uint64_t, shard::ApplyOutcome> found;
  std::map<std::uint64_t, int> copies;
  const shard::TopologyState* topology = freshest_topology(simulation, *state);
  if (topology == nullptr) return;

  for (const auto& [start, desc] : topology->ranges) {
    const shard::RangeMachine* machine = best_replica(simulation, *state, desc.id);
    if (machine == nullptr) continue;
    for (const auto& [op_id, outcome] : machine->decided()) {
      found[op_id] = outcome;
      ++copies[op_id];
    }
  }

  for (const auto& [op_id, op] : state->acked) {
    const auto it = found.find(op_id);
    if (it == found.end()) {
      ++state->lost_ops;
      state->violations.push_back(
          "operation " + std::to_string(op_id) + " (" + op.from + "->" + op.to + " " +
          std::to_string(op.amount) + ") was acknowledged and no live range remembers it");
      continue;
    }
    const bool applied = it->second == shard::ApplyOutcome::kApplied;
    if (applied != op.applied) {
      state->violations.push_back(
          "operation " + std::to_string(op_id) + " was acknowledged as " +
          (op.applied ? "applied" : "declined") + " and the cluster now says " +
          shard::to_string(it->second));
    }
  }
}

bool converged(sim::Simulation& simulation, const ShardKvState& state) {
  const shard::TopologyState* topology = freshest_topology(simulation, state);
  if (topology == nullptr || topology->ranges.empty()) return false;
  for (const auto& [start, desc] : topology->ranges) {
    if (desc.frozen) return false;  // a merge is half-done
    if (best_replica(simulation, state, desc.id) == nullptr) return false;
  }
  return true;
}

}  // namespace anvil::workloads
