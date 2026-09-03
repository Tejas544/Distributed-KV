#include "workloads/txn_bank.h"

#include <algorithm>
#include <optional>
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

// An element id, in three fields: which node issued it, which incarnation of
// that node, and the counter within that incarnation. The middle field is the
// load-bearing one -- see the draw site in client_loop.
constexpr checker::Element element_id(NodeId node, std::uint64_t epoch, std::uint64_t counter) {
  return (node.value() << 48) | ((epoch & 0xffff) << 32) | (counter & 0xffff'ffff);
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

// The replica that actually holds a key, by its own applied descriptor rather
// than by the topology's opinion of who should.
//
// The two disagree for a whole merge. A survivor absorbs its neighbour's data
// and widens its own span in its own log; the topology drops the subsumed
// descriptor one round trip later. In that window the topology still routes
// the key to a range that has been retired on every node -- best_replica finds
// nothing, the audit skips the key, and every element ever written to it is
// reported as an acknowledged write the cluster lost. It is the same
// two-halves-of-one-fact trap ANV-0042 records on the serving path, and P5's
// audit_conservation already walks around it; this one did not.
//
// Asking the machines is the fix rather than a workaround: a range's own
// descriptor is what it will and will not serve, so "who claims this key" is
// exactly the question.
//
// The freshest claimant is *not* the one with the highest applied index, and
// that mistake is [ANV-0059]. An applied index numbers entries in one range's
// own Raft log, so comparing two of them across two ranges compares two
// unrelated counters -- gotcha 10.23's rule with the two consumers being two
// logs instead of a log and a state machine. Seed 10 lost two acknowledged
// elements to exactly that: node 2 held a lagging replica of range 3 whose
// descriptor was still the pre-split `[, )` at generation 6 and index 141,
// while the range that had actually owned the keys since the split -- range 8,
// `[key00010, )`, generation 1 -- sat at index 78 on four nodes with both
// elements durably in its version chain. 141 > 78, so the audit read a replica
// that had not yet learned it no longer owned the key, found nothing, and
// reported two acknowledged writes as lost.
//
// What *is* comparable is a descriptor generation within one range: it is
// bumped by every split and merge the range takes part in (INV-SHARD-05), so
// among replicas of one range the highest generation is the freshest opinion
// that range has of its own span. So the claim is settled per range first --
// each range speaks with its newest descriptor, never an older replica's -- and
// only then is the key matched against it. A replica that is behind no longer
// answers for keys its own range has since given away.
//
// Two distinct ranges can still both claim a key at their newest descriptors,
// for the width of a merge: the survivor has widened its span and the subsumed
// range has not yet been torn down. `frozen` is what tells them apart, and it
// is the subsumed one that carries it. Anything left after that is a key held
// by two live ranges at once, which is a finding rather than a tie to break --
// the audit counts them and reports them (`ambiguous_keys`) rather than
// picking one and reporting nothing.
const shard::RangeMachine* holder_of(sim::Simulation& simulation, const TxnBankState& state,
                                     std::string_view key) {
  // Pass one: the newest descriptor each range has reached anywhere in the
  // cluster. Keyed by range, because that is the only scope in which a
  // generation means anything.
  std::map<std::uint64_t, std::uint64_t> newest;
  for (const auto& [id, node] : state.nodes) {
    if (node.store == nullptr) continue;
    if (!simulation.process().alive(NodeId{id})) continue;
    for (const auto& [range_id, replica] : node.store->ranges()) {
      if (replica.machine == nullptr || !replica.machine->initialised()) continue;
      std::uint64_t& top = newest[range_id];
      top = std::max(top, replica.machine->descriptor().generation);
    }
  }

  // Pass two: among the replicas that speak for their range, the ones that
  // claim this key. Within a single range the applied index *is* the right
  // discriminator, and that is the only place it is used.
  const shard::RangeMachine* best = nullptr;
  for (const auto& [id, node] : state.nodes) {
    if (node.store == nullptr) continue;
    if (!simulation.process().alive(NodeId{id})) continue;
    for (const auto& [range_id, replica] : node.store->ranges()) {
      if (replica.machine == nullptr || !replica.machine->initialised()) continue;
      const shard::RangeDescriptor& desc = replica.machine->descriptor();
      if (desc.generation != newest[range_id]) continue;  // a lagging incarnation
      if (!desc.contains(key)) continue;
      if (best == nullptr) {
        best = replica.machine.get();
        continue;
      }
      const shard::RangeDescriptor& incumbent = best->descriptor();
      if (incumbent.id == desc.id) {
        if (replica.machine->applied_index() > best->applied_index()) best = replica.machine.get();
        continue;
      }
      // Different ranges. A frozen one has stopped accepting writes and is
      // waiting to be torn down; a live one has not.
      if (incumbent.frozen && !desc.frozen) best = replica.machine.get();
    }
  }
  return best;
}

// How many live ranges claim this key with their newest descriptor. One is the
// answer; zero happens legally in the split window `pending_split_value` covers;
// two or more is INV-SHARD-02's territory -- a key inside two ranges at once --
// and the audit reports it rather than quietly picking a winner, because a
// checker that breaks a tie it should be reporting is the failure mode this
// phase has already spent four findings on.
std::size_t claiming_ranges(sim::Simulation& simulation, const TxnBankState& state,
                            std::string_view key) {
  std::map<std::uint64_t, std::uint64_t> newest;
  for (const auto& [id, node] : state.nodes) {
    if (node.store == nullptr) continue;
    if (!simulation.process().alive(NodeId{id})) continue;
    for (const auto& [range_id, replica] : node.store->ranges()) {
      if (replica.machine == nullptr || !replica.machine->initialised()) continue;
      std::uint64_t& top = newest[range_id];
      top = std::max(top, replica.machine->descriptor().generation);
    }
  }
  std::set<std::uint64_t> claimants;
  for (const auto& [id, node] : state.nodes) {
    if (node.store == nullptr) continue;
    if (!simulation.process().alive(NodeId{id})) continue;
    for (const auto& [range_id, replica] : node.store->ranges()) {
      if (replica.machine == nullptr || !replica.machine->initialised()) continue;
      const shard::RangeDescriptor& desc = replica.machine->descriptor();
      if (desc.generation != newest[range_id] || desc.frozen) continue;
      if (desc.contains(key)) claimants.insert(range_id);
    }
  }
  return claimants.size();
}

// The parent's payload for a split that has been applied and not yet collected
// by its child, decoded into `out`. False when no parent anywhere is holding
// this key.
//
// This is the only form the data exists in for the width of the handover: no
// range machine has it, and the child that will is not initialised yet. P5's
// audit has always walked these (shard_kv.cc); this phase's did not, and every
// split caught mid-handover was reported as unreachable accounts and a cluster
// that had lost money. The topology here churns continuously by design, so
// there is essentially always one in flight.
bool pending_split_store(sim::Simulation& simulation, const TxnBankState& state,
                         std::string_view key, txn::VersionStore* out) {
  for (const auto& [id, node] : state.nodes) {
    if (node.store == nullptr) continue;
    if (!simulation.process().alive(NodeId{id})) continue;
    for (const auto& [range_id, replica] : node.store->ranges()) {
      if (replica.machine == nullptr) continue;
      for (const auto& [child_id, pending] : replica.machine->pending_splits()) {
        if (key < pending.start) continue;
        if (!pending.end.empty() && key >= pending.end) continue;
        std::string_view section;
        if (!shard::RangeMachine::decode_txn_section(pending.payload, &section)) continue;
        if (!out->load(section)) continue;
        return true;
      }
    }
  }
  return false;
}

// The version store that owns a key right now -- a live range's, or a payload
// in mid-handover -- and the single place the audit asks that question.
//
// Having two places was the second half of [ANV-0059]. The audit had one branch
// that read a range machine through `audited_value`, which resolves a committed
// intent the way a client's reader would, and a second branch that read a split
// payload through `committed_value`, which does not. A cross-range transfer
// caught across that seam had its credit counted from the range that stayed put
// and its debit skipped inside the payload, and seed 7 finished 14 *over* the
// total it started with. An audit that finds money is the same defect as one
// that loses it, and the fix is not to teach the second branch the same trick --
// it is to stop having two branches. Everything below reads a key through this
// function, so a key resolves identically no matter where the topology happens
// to have left it.
//
// `scratch` backs the payload case, and the returned pointer is valid only as
// long as it is.
const txn::VersionStore* owning_store(sim::Simulation& simulation, const TxnBankState& state,
                                      std::string_view key, txn::VersionStore* scratch) {
  const shard::RangeMachine* machine = holder_of(simulation, state, key);
  if (machine != nullptr) return &machine->txn_store();
  if (pending_split_store(simulation, state, key, scratch)) return scratch;
  return nullptr;
}

// The record that decides a live intent's fate, read from the range that owns
// the transaction's *primary* key -- and from nowhere else.
//
// A record lives on the primary's range and nowhere else (record.h), so asking
// the range the *intent* sits on finds nothing for exactly the transactions
// this phase exists to exercise. Every cross-range transaction's secondary
// intents sit beside no record at all, the audit read straight past them, and
// the balance it counted was the one from before the transaction. That is the
// two conservation shortfalls, and it is why both were smaller than an account:
// each is one half of one transfer, counted on the debit side and missed on the
// credit side.
//
// Searching *every* range for the record instead was tried and rejected, and
// the difference between the two searches is the whole of the finding. A search
// that accepts any replica anywhere that says kCommitted will take a stale or
// divergent copy's word for it and resolve the intent the way the transaction
// *meant* to go -- which reconstructs the total the engine should have had and
// hides the lost update a seeded mutation caused. It dropped the drill from 7/7
// to 3/7. Asking the primary's range asks one authority and gets one answer,
// which is what INV-TXN-02 says a reader gets and literally what
// `resolve_blocker` does with `locate(blocked.blocker_primary)`.
//
// Returned by value rather than by pointer because the owning store may be a
// payload decoded on the spot, and a pointer into it outlives nothing.
std::optional<txn::TxnRecord> deciding_record(sim::Simulation& simulation,
                                              const TxnBankState& state,
                                              const txn::Intent& intent) {
  // "An intent that does not name its primary is an intent nobody can resolve"
  // (record.h). The audit is nobody too, and guessing here would be the
  // cluster-wide search wearing a narrower name.
  if (intent.primary.empty()) return std::nullopt;
  txn::VersionStore scratch;
  const txn::VersionStore* home = owning_store(simulation, state, intent.primary, &scratch);
  if (home == nullptr) return std::nullopt;
  const txn::TxnRecord* record = home->find_record(intent.txn);
  if (record == nullptr) return std::nullopt;
  return *record;
}

// Parallel commit's rule, applied by the audit because a recovering reader
// would apply it: a kStaging record is committed exactly when every key in its
// list carries that transaction's intent. Nothing else about the transaction is
// consulted, and nothing is reconstructed -- the record names the keys itself,
// which is what the key list is for (record.h).
//
// Without this the audit read past every intent belonging to a transaction that
// committed the parallel way, and the `parallel commit` drill control fired
// through the conserved total: a configuration change reported as data loss.
// The control staying silent is the check on this function, and the drill's
// null control is the check on the control.
bool implicitly_committed(sim::Simulation& simulation, const TxnBankState& state,
                          const txn::TxnRecord& record) {
  if (record.status != txn::TxnStatus::kStaging) return false;
  if (record.keys.empty()) return false;  // nothing to verify against
  for (const std::string& key : record.keys) {
    txn::VersionStore scratch;
    const txn::VersionStore* home = owning_store(simulation, state, key, &scratch);
    if (home == nullptr) return false;  // cannot see the key; cannot claim it
    const auto& intents = home->intents();
    const auto it = intents.find(key);
    if (it != intents.end() && it->second.txn == record.id &&
        it->second.epoch == record.epoch) {
      continue;  // the intent is present, which is what staging asks
    }
    // Already materialised counts as present: resolution ran here and the
    // intent became a version. Absent-and-never-written does not, and the two
    // are told apart by the timestamp -- resolution writes the version *at* the
    // record's commit timestamp, so that exact version is the evidence and
    // nothing else is.
    //
    // Which makes a staging record with no commit timestamp undecidable by this
    // branch, and it must say so. It used to accept any version chain at all on
    // the key, so an unrelated transaction's version stood in as proof that
    // *this* transaction's intent had been resolved. Seed 12: t164 staging with
    // `[key00009, key00010]`, its intent on key00009 only, and a version on
    // key00010 left by a different transfer entirely -- counted as implicitly
    // committed, its debit added to the total and its credit never written.
    // Eight units short, attributed to parallel commit, which was innocent.
    if (record.commit_ts == 0) return false;
    const auto chain = home->versions().find(key);
    if (chain == home->versions().end()) return false;
    if (chain->second.find(record.commit_ts) == chain->second.end()) return false;
  }
  return true;
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
//
// Takes the version store rather than the range machine because a split payload
// is a version store with no machine attached, and the payload needs the same
// resolution for the same reason -- see `pending_split_value`.
bool audited_value(sim::Simulation& simulation, const TxnBankState& state,
                   const txn::VersionStore& store, std::string_view key, txn::Ts at,
                   std::string* out, txn::Ts* version_at) {
  const auto& intents = store.intents();
  const auto it = intents.find(std::string{key});
  if (it != intents.end()) {
    const std::optional<txn::TxnRecord> record = deciding_record(simulation, state, it->second);
    // The epoch test is the engine's own rule rather than an extra margin:
    // commit_intent refuses an intent whose epoch is not the one being
    // committed (kStaleEpoch), so an intent left behind by an earlier
    // incarnation of a restarted transaction can never become a version.
    // Counting it would be counting a write that is already dead.
    if (record.has_value() && record->epoch == it->second.epoch &&
        (record->status == txn::TxnStatus::kCommitted ||
         implicitly_committed(simulation, state, *record))) {
      if (it->second.tombstone) return false;
      *out = it->second.value;
      *version_at = record->commit_ts;
      return true;
    }
  }
  return store.committed_value(key, at, out, version_at);
}

// The final value of a key, wherever the topology has left it. False means no
// range and no payload anywhere claims the key, which is the one condition the
// audit reports as unreachable.
//
// Absent from the store it is found in is still "reached": whoever owns the key
// owns it empty, and an account nobody has written to holds its opening
// balance.
bool final_value(sim::Simulation& simulation, const TxnBankState& state, std::string_view key,
                 txn::Ts at, std::string* out, txn::Ts* version_at) {
  txn::VersionStore scratch;
  const txn::VersionStore* store = owning_store(simulation, state, key, &scratch);
  if (store == nullptr) return false;
  out->clear();
  audited_value(simulation, state, *store, key, at, out, version_at);
  return true;
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
  // An odd key count leaves a pair with no partner, so the shape below is only
  // well defined for an even one; asking for it with an odd count gets the
  // default plan rather than a subtly-not-disjoint one.
  if (cfg.disjoint_read_write && cfg.kind == TxnWorkloadKind::kListAppend && cfg.keys >= 2 &&
      cfg.keys % 2 == 0) {
    // The two-doctors shape, built rather than hoped for: pick a key *pair*,
    // pick a side, read that side and append to the other. Two clients that
    // draw the same pair from opposite sides are a write-skew cycle, and that
    // is the only history in which a missing read refresh is an anomaly at
    // serializable rather than a merely permissive schedule.
    //
    // Hoping for it was tried first. Drawing reads and appends from disjoint
    // halves of the key space, with the keys still drawn independently, put the
    // mechanism squarely under load -- 60 refresh failures a seed, five extra
    // transactions per seed committing without one -- and produced no anomaly
    // in 15 seeds, because an anti-dependency in one direction is not a cycle.
    // The reciprocal pair has to happen *concurrently on the same pair of
    // keys*, and with sixteen keys and ten commits a seed, random draws will
    // not deliver it. This is the same move the checker's own write-skew test
    // makes when it constructs the history by hand: the shape is stated, not
    // sampled.
    //
    // Pairs are adjacent keys so that a transaction still routinely spans a
    // range boundary, which is the property the rest of the phase is about.
    const std::uint32_t pair =
        static_cast<std::uint32_t>(rng.next_u64() % (cfg.keys / 2)) * 2;
    const std::uint32_t side = static_cast<std::uint32_t>(rng.next_u64() % 2);
    plan.steps.clear();
    Plan::Step observe;
    observe.read = true;
    observe.key = pair + side;
    Plan::Step write;
    write.read = false;
    write.key = pair + (1 - side);
    plan.steps.push_back(observe);
    plan.steps.push_back(write);
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

// `epoch` is the incarnation of the process this loop was spawned into. It has
// to be a parameter rather than something the loop reads for itself, because it
// identifies *this* client, and the whole point of it is that the number
// outlives the frame it is used in.
Task<void> client_loop(Runtime& rt, TxnBankConfig cfg, TxnBankState* state, NodeId self,
                       std::uint64_t epoch) {
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
    //
    // The incarnation is in the id because `element_counter` is a local of this
    // coroutine, the coroutine is spawned by boot_node, and a crash destroys
    // every frame on the node -- so the next incarnation's loop starts counting
    // from zero again and hands out ids the previous one already used. That is
    // [ANV-0055]: two unrelated transactions appending the same number, which
    // the checker is obliged to call a duplicate element because a broken
    // generator and a doubly-applied write are indistinguishable from the
    // history alone. A client's identity has to survive the process it runs in.
    std::vector<checker::Element> elements;
    for (std::size_t i = 0; i < plan.steps.size(); ++i) {
      elements.push_back(element_id(self, epoch, ++element_counter));
    }

    // The precondition, enforced rather than assumed. If it is ever broken
    // again this reports as what it is -- the workload's fault -- instead of
    // arriving at the checker disguised as a database anomaly.
    for (const checker::Element e : elements) {
      if (!state->issued_elements.insert(e).second) {
        state->violations.push_back(
            "the workload issued element " + std::to_string(e) +
            " twice; element -> writer is not a function and every list-append "
            "verdict on this run is void");
      }
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
          ++state->uncertain_reads;
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
            state->element_stamps[mop.element] = {handle.start_ts, handle.commit_ts};
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

// One read-only transaction per key, after the faults have healed and every
// writer has finished. Recorded in the history like any other transaction,
// because it is one.
//
// Three separate things in this phase needed a reader here and there was none,
// which is the finding this exists to close:
//
//   * Elle recovers a key's version order *from reads that saw it*
//     (elle.cc, recover_version_orders), so an append that nobody ever reads
//     back is invisible to the dependency graph -- not unconfirmed, absent. A
//     run whose last writer commits and stops leaves most of its own writes
//     unobserved, and the checker then has almost no edges to find a cycle in.
//     The constructed write-skew test in txn_faults.cc says this in its own
//     comment and adds two observer transactions by hand; the workload never
//     did the same, which is why `no refresh on push` could commit five
//     transactions per seed that the refresh would have aborted and still
//     produce no anomaly at all.
//
//   * An intent whose owner died is cleaned up by the next reader that meets
//     it. With no readers left, "nobody has tidied up yet" is indistinguishable
//     from "nobody ever will", which is exactly why `orphaned_intents` cannot
//     be asserted to be zero (CONTEXT.md section 14) and why the drill has no
//     detector for a mutation whose signature is unresolvable intents.
//
//   * A read is the only thing that exercises the *serving* path after a heal.
//     The audit reads state machines directly; a client read goes through
//     routing, the lease and intent resolution.
//
// Read-only and one key at a time, deliberately: this must not be able to
// change the state it is measuring, and a multi-key reader that spanned a range
// boundary would abort under exactly the topology churn it is meant to observe
// through.
Task<void> settle_reader(Runtime& rt, TxnBankConfig cfg, TxnBankState* state, NodeId self) {
  for (std::uint32_t i = 0; i < cfg.keys; ++i) {
    TxnBankNode* node = node_of(state, self);
    if (node == nullptr || node->coordinator == nullptr) co_return;

    const std::string key = data_key(i);
    const checker::TxnId history_id = state->history.begin(self.value(), rt.now());

    bool done = false;
    for (std::uint32_t attempt = 0; attempt < 4 && !done; ++attempt) {
      TxnBankNode* n = node_of(state, self);
      if (n == nullptr || n->coordinator == nullptr) co_return;

      txn::Handle handle;
      if (!co_await n->coordinator->begin(&handle)) {
        co_await rt.sleep_for(cfg.client_interval);
        continue;
      }
      bool found = false;
      std::string value;
      const txn::ReadStatus status = co_await n->coordinator->get(&handle, key, &found, &value);
      if (status != txn::ReadStatus::kOk) {
        co_await n->coordinator->rollback(&handle);
        co_await rt.sleep_for(cfg.client_interval);
        continue;
      }
      if (cfg.kind == TxnWorkloadKind::kListAppend) {
        std::vector<checker::Element> list;
        if (decode_list(value, &list)) {
          state->history.read(history_id, static_cast<checker::KeyId>(i), list);
        }
      }
      // A read-only transaction has no writes to commit, so this is the
      // coordinator's own no-op commit path rather than a second round trip.
      const txn::TxnOutcome outcome = co_await n->coordinator->commit(&handle);
      state->history.complete(history_id,
                              outcome == txn::TxnOutcome::kCommitted
                                  ? checker::Outcome::kCommitted
                                  : checker::Outcome::kAborted,
                              rt.now());
      ++state->settle_reads;
      done = true;
    }
    if (!done) {
      state->history.complete(history_id, checker::Outcome::kUnknown, rt.now());
    }
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
  rt.spawn(client_loop(rt, cfg, state, self, simulation.process().incarnation(self)));
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

void start_settle_reads(sim::Simulation& simulation, TxnBankState* state) {
  // The lowest-numbered live node with a coordinator, so the choice is a
  // function of the state rather than of iteration order.
  for (const auto& [id, node] : state->nodes) {
    const NodeId self{static_cast<std::uint32_t>(id)};
    if (node.coordinator == nullptr || !simulation.process().alive(self)) continue;
    Runtime& rt = simulation.node(self);
    rt.spawn(settle_reader(rt, state->config, state, self));
    return;
  }
}

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

  // Counted once for every key, before anything is read from anywhere, because
  // it is the precondition the rest of the audit rests on: every value below is
  // read from one range, and that is only an answer if exactly one range is
  // entitled to give it.
  for (std::uint32_t i = 0; i < state->config.keys; ++i) {
    if (claiming_ranges(simulation, *state, data_key(i)) > 1) ++state->ambiguous_keys;
  }

  if (state->config.kind == TxnWorkloadKind::kBank) {
    std::int64_t total = 0;
    std::uint32_t seen = 0;
    for (std::uint32_t i = 0; i < state->config.keys; ++i) {
      const std::string key = data_key(i);
      std::string value;
      txn::Ts version_at = 0;
      std::int64_t balance = state->config.initial_balance;

      // One call, one answer, whether the key is sitting in a range or in a
      // split payload -- see `owning_store`.
      if (!final_value(simulation, *state, key, at, &value, &version_at)) continue;
      if (!value.empty() && !decode_balance(value, &balance)) continue;
      state->final_balances[key] = balance;
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
    std::string value;
    txn::Ts version_at = 0;

    // The same one call the bank audit makes, for the same reason: an element
    // written to a key whose range is mid-handover when the run ends is not a
    // lost write, and the loudest failure this suite can produce used to be the
    // checker looking in one fewer place than the protocol allows the data to
    // be.
    if (!final_value(simulation, *state, key, at, &value, &version_at)) continue;
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
  // A record lives on the range that owns its transaction's *primary* key, and
  // a cross-range transaction has its intents on other ranges entirely. Asking
  // only the range the intent sits on finds nothing for exactly the
  // transactions this phase exists to exercise, so every one of them counted
  // as an intent nobody will ever release -- 111 phantom orphans on a run with
  // nothing wrong with it. A number that large and that wrong is worse than no
  // number: it is why this was reported and never asserted on, which in turn
  // is why the drill had no detector for `secondaries before primary`, whose
  // entire signature is intents with no record to resolve them against.
  const auto record_anywhere = [&](txn::TxnId txn) -> const txn::TxnRecord* {
    const txn::TxnRecord* best = nullptr;
    for (const auto& [node_id, node] : state.nodes) {
      if (node.store == nullptr) continue;
      if (!simulation.process().alive(NodeId{node_id})) continue;
      for (const auto& [range_id, replica] : node.store->ranges()) {
        if (replica.machine == nullptr) continue;
        const txn::TxnRecord* found = replica.machine->txn_store().find_record(txn);
        if (found == nullptr) continue;
        if (terminal(found->status)) return found;  // a verdict beats a guess
        if (best == nullptr) best = found;
      }
    }
    return best;
  };

  std::uint64_t orphans = 0;
  for (const auto& [id, node] : state.nodes) {
    if (node.store == nullptr) continue;
    if (!simulation.process().alive(NodeId{id})) continue;
    for (const auto& [range_id, replica] : node.store->ranges()) {
      if (replica.machine == nullptr) continue;
      const txn::VersionStore& store = replica.machine->txn_store();
      for (const auto& [key, intent] : store.intents()) {
        (void)key;
        const txn::TxnRecord* record = record_anywhere(intent.txn);
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
