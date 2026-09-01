#include "workloads/txn_bank.h"

#include <algorithm>
#include <string>

#include "anvil/core/lsm/format.h"

namespace anvil::workloads {
namespace {

std::string data_key(std::uint32_t index) {
  std::string out = "key";
  const std::string digits = std::to_string(index);
  out.append(5 - std::min<std::size_t>(5, digits.size()), '0');
  out += digits;
  return out;
}

// A list value is a length-prefixed sequence of elements. Explicit rather than
// a delimiter, because an element is a 64-bit integer and any delimiter is a
// value some element can take.
std::string encode_list(const std::vector<checker::Element>& list) {
  std::string out;
  lsm::put_varint32(&out, static_cast<std::uint32_t>(list.size()));
  for (const checker::Element e : list) lsm::put_varint64(&out, e);
  return out;
}

bool decode_list(std::string_view in, std::vector<checker::Element>* out) {
  out->clear();
  if (in.empty()) return true;
  const char* p = in.data();
  const char* limit = p + in.size();
  std::uint32_t count = 0;
  p = lsm::get_varint32(p, limit, &count);
  if (p == nullptr) return false;
  out->reserve(count);
  for (std::uint32_t i = 0; i < count; ++i) {
    std::uint64_t e = 0;
    p = lsm::get_varint64(p, limit, &e);
    if (p == nullptr) return false;
    out->push_back(e);
  }
  return true;
}

std::string encode_balance(std::int64_t value) {
  std::string out;
  lsm::put_varint64(&out, static_cast<std::uint64_t>(value));
  return out;
}

bool decode_balance(std::string_view in, std::int64_t* out) {
  if (in.empty()) {
    *out = 0;
    return true;
  }
  const char* p = in.data();
  std::uint64_t raw = 0;
  if (lsm::get_varint64(p, p + in.size(), &raw) == nullptr) return false;
  *out = static_cast<std::int64_t>(raw);
  return true;
}

TxnBankNode* node_of(TxnBankState* state, NodeId self) {
  const auto it = state->nodes.find(self.value());
  return it == state->nodes.end() ? nullptr : &it->second;
}

const shard::TopologyState* freshest_topology(sim::Simulation& simulation,
                                              const TxnBankState& state) {
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

const shard::RangeMachine* best_replica(sim::Simulation& simulation, const TxnBankState& state,
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

// The value a client would see if it asked right now, accounting for a live
// intent whose transaction has already committed.
//
// VersionStore::committed_value deliberately ignores intents -- it exists for
// exactly the case where the *last* writer's cleanup has not run yet, and
// treating that as "nothing here" would report every element a lazily-
// resolved commit ever wrote as lost (that was the bug the comment used to
// warn about here, and it undersold the fix: committed_value alone reports
// the value from *before* this transaction, which is just as wrong -- a
// client that read this key after the commit was acknowledged would see the
// new value, intent or not, because a reader that meets an intent goes to the
// primary and asks (coordinator.h). The audit has to do the same asking: an
// intent whose owning record says committed *is* the value; only a pending or
// aborted one is skipped, and a version already materialised is the fallback
// when there is no live intent at all.
bool audited_value(const txn::VersionStore& store, std::string_view key, txn::Ts at,
                   std::string* out, txn::Ts* version_at) {
  const auto& intents = store.intents();
  const auto it = intents.find(std::string{key});
  if (it != intents.end()) {
    const txn::TxnRecord* record = store.find_record(it->second.txn);
    if (record != nullptr && record->status == txn::TxnStatus::kCommitted) {
      if (it->second.tombstone) return false;
      *out = it->second.value;
      *version_at = record->commit_ts;
      return true;
    }
  }
  return store.committed_value(key, at, out, version_at);
}

// ---------------------------------------------------------------------------
// one transaction
// ---------------------------------------------------------------------------

struct Plan {
  struct Step {
    bool read = false;
    std::uint32_t key = 0;
    std::int64_t amount = 0;  // kBank
  };
  std::vector<Step> steps;
};

// Drawn once and replayed on every restart. A transaction that redraws its
// operations after an abort is a *different* transaction wearing the same name,
// and the history then records two transactions as one -- which is the harness
// manufacturing anomalies rather than finding them (ANV-0022, one layer up).
Plan draw_plan(DeterministicRandom& rng, const TxnBankConfig& cfg) {
  Plan plan;
  const std::uint32_t base = static_cast<std::uint32_t>(rng.next_u64() % cfg.keys);
  const std::uint32_t count = 2 + static_cast<std::uint32_t>(rng.next_u64() % cfg.ops_per_txn);
  for (std::uint32_t i = 0; i < count; ++i) {
    Plan::Step step;
    const std::uint32_t offset =
        static_cast<std::uint32_t>(rng.next_u64() % std::max<std::uint32_t>(1, cfg.neighbourhood));
    step.key = (base + offset) % cfg.keys;
    step.read = (rng.next_u64() % 100) < cfg.read_percent;
    step.amount = 1 + static_cast<std::int64_t>(rng.next_u64() % cfg.max_transfer);
    plan.steps.push_back(step);
  }
  if (cfg.kind == TxnWorkloadKind::kBank) {
    // A transfer needs two distinct accounts and both halves, or the total is
    // not conserved by construction and the oracle is worthless.
    plan.steps.clear();
    const std::uint32_t from = base;
    const std::uint32_t to =
        (base + 1 +
         static_cast<std::uint32_t>(rng.next_u64() %
                                    std::max<std::uint32_t>(1, cfg.neighbourhood))) %
        cfg.keys;
    if (from == to) return plan;  // degenerate; the client skips it
    Plan::Step debit;
    debit.key = from;
    debit.read = false;
    debit.amount = -(1 + static_cast<std::int64_t>(rng.next_u64() % cfg.max_transfer));
    Plan::Step credit;
    credit.key = to;
    credit.read = false;
    credit.amount = -debit.amount;
    plan.steps.push_back(debit);
    plan.steps.push_back(credit);
  }
  return plan;
}

Task<void> client_loop(Runtime& rt, TxnBankConfig cfg, TxnBankState* state, NodeId self) {
  co_await rt.sleep_for(cfg.settle_before_start);

  std::uint64_t element_counter = 0;
  while (true) {
    TxnBankNode* node = node_of(state, self);
    if (node == nullptr || node->coordinator == nullptr) co_return;
    if (node->done >= cfg.txns_per_client) co_return;

    DeterministicRandom& rng = rt.rng(RandomDomain::kWorkload);
    const Plan plan = draw_plan(rng, cfg);
    if (plan.steps.empty()) {
      ++node->done;
      co_await rt.sleep_for(cfg.client_interval);
      continue;
    }

    // The element ids a list-append transaction will use, drawn once so that a
    // restart appends the same elements. Globally unique by construction:
    // element -> writer has to be a function or the checker cannot recover the
    // version order at all.
    std::vector<checker::Element> elements;
    for (std::size_t i = 0; i < plan.steps.size(); ++i) {
      elements.push_back((self.value() << 40) | (++element_counter));
    }

    const checker::TxnId history_id = state->history.begin(self.value(), rt.now());
    bool recorded_mops = false;
    txn::TxnOutcome outcome = txn::TxnOutcome::kAborted;
    std::set<std::uint64_t> ranges_touched;

    for (std::uint32_t attempt = 0; attempt < 6; ++attempt) {
      TxnBankNode* n = node_of(state, self);
      if (n == nullptr || n->coordinator == nullptr) co_return;

      txn::Handle handle;
      if (!co_await n->coordinator->begin(&handle)) {
        co_await rt.sleep_for(cfg.client_interval);
        continue;
      }

      bool restart = false;
      std::vector<checker::Mop> mops;
      for (std::size_t i = 0; i < plan.steps.size(); ++i) {
        const Plan::Step& step = plan.steps[i];
        const std::string key = data_key(step.key);

        shard::RangeDescriptor located;
        if (n->store->locate(key, &located)) ranges_touched.insert(located.id.value());

        bool found = false;
        std::string value;
        const txn::ReadStatus status = co_await n->coordinator->get(&handle, key, &found, &value);
        if (status == txn::ReadStatus::kUncertain) {
          restart = true;
          ++state->restarts;
          break;
        }
        if (status != txn::ReadStatus::kOk) {
          restart = true;
          break;
        }

        if (cfg.kind == TxnWorkloadKind::kListAppend) {
          std::vector<checker::Element> list;
          if (!decode_list(value, &list)) {
            state->violations.push_back("a list value did not decode; key " + key);
            restart = true;
            break;
          }
          if (step.read) {
            checker::Mop mop;
            mop.type = checker::MopType::kRead;
            mop.key = step.key;
            mop.observed = list;
            mops.push_back(mop);
          } else {
            list.push_back(elements[i]);
            n->coordinator->put(&handle, key, encode_list(list));
            checker::Mop mop;
            mop.type = checker::MopType::kAppend;
            mop.key = step.key;
            mop.element = elements[i];
            mops.push_back(mop);
          }
        } else {
          std::int64_t balance = 0;
          if (!decode_balance(value, &balance)) {
            restart = true;
            break;
          }
          if (!found) balance = cfg.initial_balance;
          if (step.amount < 0 && balance + step.amount < 0) {
            // Not enough money. A real outcome, not a failure: the transaction
            // is rolled back and the client moves on.
            restart = false;
            co_await n->coordinator->rollback(&handle);
            mops.clear();
            goto decided;
          }
          n->coordinator->put(&handle, key, encode_balance(balance + step.amount));
        }
      }

      if (restart) {
        co_await n->coordinator->rollback(&handle);
        ++state->restarts;
        co_await rt.sleep_for(cfg.client_interval);
        continue;
      }

      outcome = co_await n->coordinator->commit(&handle);
      if (outcome == txn::TxnOutcome::kCommitted) {
        for (const checker::Mop& mop : mops) {
          if (mop.type == checker::MopType::kAppend) {
            state->acked_elements[mop.element] = data_key(static_cast<std::uint32_t>(mop.key));
            ++state->writes;
          } else {
            ++state->reads;
          }
          if (!recorded_mops) {
            if (mop.type == checker::MopType::kAppend) {
              state->history.append(history_id, mop.key, mop.element);
            } else {
              state->history.read(history_id, mop.key, mop.observed);
            }
          }
        }
        recorded_mops = true;
        break;
      }
      if (outcome == txn::TxnOutcome::kUnknown) break;
      ++state->restarts;
      co_await rt.sleep_for(cfg.client_interval);
    }

  decided:
    if (ranges_touched.size() > 1) {
      ++state->cross_range;
    } else if (!ranges_touched.empty()) {
      ++state->single_range;
    }

    checker::Outcome recorded = checker::Outcome::kAborted;
    if (outcome == txn::TxnOutcome::kCommitted) {
      recorded = checker::Outcome::kCommitted;
      ++state->committed;
    } else if (outcome == txn::TxnOutcome::kUnknown) {
      recorded = checker::Outcome::kUnknown;
      ++state->unknown;
    } else {
      ++state->aborted;
    }
    state->history.complete(history_id, recorded, rt.now());

    TxnBankNode* n = node_of(state, self);
    if (n == nullptr) co_return;
    ++n->done;
    co_await rt.sleep_for(cfg.client_interval);
  }
}

Task<void> heartbeat_loop(Runtime& rt, TxnBankConfig cfg, TxnBankState* state, NodeId self) {
  for (;;) {
    co_await rt.sleep_for(cfg.txn.heartbeat_interval);
    TxnBankNode* node = node_of(state, self);
    if (node == nullptr || node->coordinator == nullptr) co_return;
    co_await node->coordinator->heartbeat_all();
  }
}

void boot_node(sim::Simulation& simulation, TxnBankConfig cfg, TxnBankState* state, NodeId self,
               checker::TxnObserver* observer) {
  Runtime& rt = simulation.node(self);
  TxnBankNode& node = state->nodes[self.value()];

  node.booted = false;
  ++node.boots;
  // The coordinator holds pointers into the store, so it goes first.
  node.coordinator.reset();
  node.store.reset();

  shard::StoreOptions store_options = cfg.store;
  store_options.cluster_size = state->node_count;
  store_options.accounts = 0;  // P6 seeds its keys through transactions, not bootstrap
  store_options.range.clock_uncertainty_nanos = static_cast<std::uint64_t>(
      simulation.config().faults.clock.declared_uncertainty.nanos());

  auto store = std::make_unique<shard::ShardStore>(
      &rt, self, store_options, DeterministicRandom{rt.rng(RandomDomain::kPlacement).next_u64()});
  shard::ShardStore* store_ptr = store.get();

  txn::CoordinatorOptions txn_options = cfg.txn;
  txn_options.clock_uncertainty = simulation.config().faults.clock.declared_uncertainty;
  auto coordinator =
      std::make_unique<txn::Coordinator>(&rt, self, store_ptr, txn_options);
  txn::Coordinator* coordinator_ptr = coordinator.get();

  store->set_txn_reply_handler([coordinator_ptr](const shard::ShardStore::TxnReply& reply) {
    coordinator_ptr->on_reply(reply);
  });
  coordinator->set_oracle([store_ptr](std::uint64_t count, txn::Ts* first) -> Task<bool> {
    co_return co_await store_ptr->reserve_timestamps(count, first);
  });

  node.store = std::move(store);
  node.coordinator = std::move(coordinator);
  node.booted = true;

  if (observer != nullptr) {
    observer->set_store(self, store_ptr);
    observer->set_coordinator(self, coordinator_ptr);
  }

  store_ptr->start(/*bootstrap=*/true);
  rt.spawn(client_loop(rt, cfg, state, self));
  rt.spawn(heartbeat_loop(rt, cfg, state, self));
}

}  // namespace

const char* to_string(TxnWorkloadKind kind) noexcept {
  switch (kind) {
    case TxnWorkloadKind::kListAppend: return "list-append";
    case TxnWorkloadKind::kBank: return "bank";
  }
  return "?";
}

TxnBankNode::TxnBankNode() = default;
TxnBankNode::~TxnBankNode() = default;
TxnBankNode::TxnBankNode(TxnBankNode&&) noexcept = default;
TxnBankNode& TxnBankNode::operator=(TxnBankNode&&) noexcept = default;

void install(sim::Simulation& simulation, TxnBankConfig config, TxnBankState* state,
             checker::TxnObserver* observer) {
  const std::uint32_t nodes = simulation.node_count();
  if (nodes < 3) throw sim::SimulationPanic("the transaction workload needs three nodes");

  state->node_count = nodes;
  state->simulation = &simulation;
  state->observer = observer;
  state->config = config;
  state->expected_total =
      static_cast<std::int64_t>(config.keys) * config.initial_balance;

  config.store.raft.max_clock_uncertainty =
      simulation.config().faults.clock.declared_uncertainty;

  if (observer != nullptr) {
    checker::TxnObserver::Hooks hooks;
    hooks.tick = [&simulation]() { return simulation.scheduler().tick(); };
    hooks.true_now = [&simulation]() { return simulation.scheduler().now(); };
    hooks.alive = [&simulation](NodeId id) { return simulation.process().alive(id); };
    observer->configure(nodes, std::move(hooks));
    checker::arm_txn_invariants(simulation.invariants(), observer);
  }

  for (std::uint32_t i = 1; i <= nodes; ++i) {
    const NodeId self{i};
    state->nodes[i] = TxnBankNode{};
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

void audit(sim::Simulation& simulation, TxnBankState* state) {
  const shard::TopologyState* topology = freshest_topology(simulation, *state);
  if (topology == nullptr) {
    state->violations.push_back("no live node has a topology to audit");
    return;
  }

  // The committed state, read at the highest timestamp anything used. Reading
  // at "now" would be wrong for the oracle, whose timestamps are not times;
  // reading at the maximum is reading the final state of the database.
  txn::Ts at = txn::kMaxTs;

  if (state->config.kind == TxnWorkloadKind::kBank) {
    std::int64_t total = 0;
    std::uint32_t seen = 0;
    for (std::uint32_t i = 0; i < state->config.keys; ++i) {
      const std::string key = data_key(i);
      const shard::RangeDescriptor* desc = topology->find_by_key(key);
      if (desc == nullptr) continue;
      const shard::RangeMachine* machine = best_replica(simulation, *state, desc->id);
      if (machine == nullptr) continue;
      // audited_value resolves a committed-but-not-yet-materialised intent the
      // same way a live reader would, rather than reading straight past it.
      std::string value;
      txn::Ts version_at = 0;
      std::int64_t balance = state->config.initial_balance;
      if (audited_value(machine->txn_store(), key, at, &value, &version_at) && !value.empty()) {
        if (!decode_balance(value, &balance)) continue;
      }
      total += balance;
      ++seen;
    }
    state->final_total = total;
    if (seen != state->config.keys) {
      state->violations.push_back("the audit could only reach " + std::to_string(seen) +
                                  " of " + std::to_string(state->config.keys) + " accounts");
    }
    if (total != state->expected_total) {
      state->violations.push_back("the cluster holds " + std::to_string(total) +
                                  " and started with " +
                                  std::to_string(state->expected_total));
    }
    return;
  }

  // list-append: every acknowledged element must be present exactly once.
  std::map<checker::Element, std::uint32_t> found;
  for (std::uint32_t i = 0; i < state->config.keys; ++i) {
    const std::string key = data_key(i);
    const shard::RangeDescriptor* desc = topology->find_by_key(key);
    if (desc == nullptr) continue;
    const shard::RangeMachine* machine = best_replica(simulation, *state, desc->id);
    if (machine == nullptr) continue;
    std::string value;
    txn::Ts version_at = 0;
    if (!audited_value(machine->txn_store(), key, at, &value, &version_at)) continue;
    std::vector<checker::Element> list;
    if (!decode_list(value, &list)) {
      state->violations.push_back("a final list did not decode; key " + key);
      continue;
    }
    for (const checker::Element e : list) ++found[e];
  }

  for (const auto& [element, key] : state->acked_elements) {
    const auto it = found.find(element);
    if (it == found.end()) {
      ++state->lost_elements;
      state->violations.push_back("element " + std::to_string(element) +
                                  " was acknowledged as committed to " + key +
                                  " and is not in the final list");
    } else if (it->second > 1) {
      ++state->duplicated_elements;
      state->violations.push_back("element " + std::to_string(element) + " appears " +
                                  std::to_string(it->second) + " times");
    }
  }
}

bool converged(sim::Simulation& simulation, const TxnBankState& state) {
  const shard::TopologyState* topology = freshest_topology(simulation, state);
  if (topology == nullptr || topology->ranges.empty()) return false;
  for (const auto& [start, desc] : topology->ranges) {
    if (desc.frozen) return false;
    if (best_replica(simulation, state, desc.id) == nullptr) return false;
  }
  return true;
}

std::uint64_t orphaned_intents(sim::Simulation& simulation, const TxnBankState& state) {
  std::uint64_t orphans = 0;
  for (const auto& [id, node] : state.nodes) {
    if (node.store == nullptr) continue;
    if (!simulation.process().alive(NodeId{id})) continue;
    for (const auto& [range_id, replica] : node.store->ranges()) {
      if (replica.machine == nullptr) continue;
      const txn::VersionStore& store = replica.machine->txn_store();
      for (const auto& [key, intent] : store.intents()) {
        (void)key;
        const txn::TxnRecord* record = store.find_record(intent.txn);
        // An intent whose record is terminal is simply uncleaned, which is the
        // mechanism working. One whose record is still pending after everything
        // has settled is a lock nobody will ever release.
        if (record != nullptr && terminal(record->status)) continue;
        ++orphans;
      }
    }
  }
  return orphans;
}

}  // namespace anvil::workloads
