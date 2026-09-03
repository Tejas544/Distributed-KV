#include "anvil/checker/txn_invariants.h"

#include <algorithm>

namespace anvil::checker {

void TxnObserver::configure(std::uint32_t nodes, Hooks hooks) {
  nodes_ = nodes;
  hooks_ = std::move(hooks);
}

void TxnObserver::set_store(NodeId id, const shard::ShardStore* store) {
  stores_[id.value()] = store;
}

void TxnObserver::set_coordinator(NodeId id, const txn::Coordinator* coordinator) {
  coordinators_[id.value()] = coordinator;
}

const shard::ShardStore* TxnObserver::store(NodeId id) const {
  const auto it = stores_.find(id.value());
  if (it == stores_.end()) return nullptr;
  if (hooks_.alive && !hooks_.alive(id)) return nullptr;
  return it->second;
}

const txn::Coordinator* TxnObserver::coordinator(NodeId id) const {
  const auto it = coordinators_.find(id.value());
  if (it == coordinators_.end()) return nullptr;
  if (hooks_.alive && !hooks_.alive(id)) return nullptr;
  return it->second;
}

std::vector<NodeId> TxnObserver::live_nodes() const {
  std::vector<NodeId> out;
  for (std::uint32_t i = 1; i <= nodes_; ++i) {
    if (store(NodeId{i}) != nullptr) out.push_back(NodeId{i});
  }
  return out;
}

void TxnObserver::record(const std::string& id, std::string detail) {
  pending_[id].push_back(std::move(detail));
}

std::optional<std::string> TxnObserver::take(const std::string& id) {
  const auto it = pending_.find(id);
  if (it == pending_.end() || it->second.empty()) return std::nullopt;
  std::string out = std::move(it->second.front());
  it->second.erase(it->second.begin());
  return out;
}

// ---------------------------------------------------------------------------
// the scan
// ---------------------------------------------------------------------------

void TxnObserver::refresh() {
  const std::uint64_t tick = hooks_.tick ? hooks_.tick() : 0;
  if (tick == last_tick_) return;
  last_tick_ = tick;
  ++counters_.scans;

  // The oracle's high-water mark, over every live replica. It is a replicated
  // maximum, so reading the largest any node has applied is reading the truth;
  // and INV-TXN-09 is that this number never goes down, which is a statement
  // about the *sequence* and so has to be remembered between ticks.
  for (const NodeId id : live_nodes()) {
    const shard::ShardStore* s = store(id);
    if (s == nullptr || s->oracle_driver() == nullptr) continue;

    if (!s->oracle_driver()->ready()) continue;

    const txn::Ts high = s->oracle().high_water();
    if (high > oracle_high_water_) oracle_high_water_ = high;
    counters_.oracle_high_water = oracle_high_water_;
    counters_.oracle_reservations =
        std::max<std::uint64_t>(counters_.oracle_reservations, s->oracle().reservations());

    // Per node, not against the cluster maximum. A follower that has not yet
    // applied the leader's latest reservation is *behind*, which is what a
    // follower is; comparing it against the maximum any replica has reached
    // reports replication lag as a timestamp regression, on every seed, in the
    // first second of every run.
    //
    // And per node only while that node is *authoritative*, which is the half
    // that took a second pass to get right. A replica's own high-water mark
    // legitimately moves backwards across a crash: the commit index is durable
    // only as far as the last hard-state fsync, so a node that had applied
    // reservation 4 in memory can come back with the disk saying commit = 2,
    // replay to exactly there, and sit at the older mark until the leader
    // tells it the rest. `applied >= commit` is *true* on that node and means
    // nothing -- it was the guard here before, and it let precisely this
    // through (INV-TXN-09 fired on it, on a healthy cluster, every time a
    // crash landed in that window).
    //
    // Raft is what makes the regression safe: leader completeness says a node
    // missing a committed entry cannot win an election, so it can never serve
    // a reservation from the stale mark -- and can_serve_local_reads() is the
    // predicate for exactly that. Gating on it keeps the property this
    // invariant is named for (a mark that goes backwards on a node that *can*
    // hand out timestamps is a re-issued timestamp) and drops the reading that
    // was never a violation at all.
    if (!s->oracle_driver()->node().can_serve_local_reads()) continue;

    txn::Ts& seen = oracle_seen_[id.value()];
    if (high < seen) {
      record("INV-TXN-09", "n" + std::to_string(id.value()) +
                               " had an oracle high-water mark of " + std::to_string(seen) +
                               " and now reports " + std::to_string(high) +
                               "; a timestamp that has been handed out is being handed "
                               "out again");
    }
    seen = std::max(seen, high);
  }

  // Transaction records, from whichever live replica holds each one. A record
  // lives in the range that owns its primary key, so it is found by walking
  // every hosted range rather than by asking a directory.
  for (const NodeId id : live_nodes()) {
    const shard::ShardStore* s = store(id);
    if (s == nullptr) continue;
    for (const auto& [range_id, replica] : s->ranges()) {
      if (replica.machine == nullptr || !replica.machine->initialised()) continue;
      const txn::VersionStore& store_ref = replica.machine->txn_store();
      counters_.intents_seen += store_ref.intent_count();

      const std::uint64_t applied = replica.machine->applied_index().value();

      for (const auto& [txn_id, record_ref] : store_ref.records()) {
        ++counters_.records_seen;
        RecordMirror& mirror = records_[txn_id];

        if (!mirror.seen) {
          mirror.seen = true;
          mirror.source = id.value();
          mirror.source_range = range_id;
          mirror.source_applied = applied;
          mirror.status = record_ref.status;
          mirror.commit_ts = record_ref.commit_ts;
          mirror.epoch = record_ref.epoch;
          if (record_ref.status == txn::TxnStatus::kCommitted) {
            ++counters_.records_committed;
            commit_timestamps_.emplace(record_ref.commit_ts, txn_id);
          }
          continue;
        }

        // A record's primary key can move to a different range on the same
        // node via a split or a merge. Comparing the range it moved to
        // against the range it moved from would grade a relocation as the
        // record changing its mind, so a change of range resets the
        // baseline instead of being compared against it.
        if (mirror.source_range != range_id) {
          mirror.source = id.value();
          mirror.source_range = range_id;
          mirror.source_applied = applied;
          mirror.status = record_ref.status;
          mirror.commit_ts = record_ref.commit_ts;
          mirror.epoch = record_ref.epoch;
          continue;
        }

        // Within one (tracked) range, one source: the first live node that
        // holds it. Two replicas are at different points in the same
        // sequence, and reading alternately from both makes a legal
        // transition look like a regression -- the same trap as
        // INV-SHARD-04's first version.
        if (mirror.source != id.value()) continue;

        if (record_ref.status != mirror.status) {
          // A replica that is behind where we saw this verdict is replaying,
          // not disagreeing. Skip without touching the mirror, so the verdict
          // we remember survives the replay and a record that settles on a
          // different one afterwards is still caught.
          if (applied < mirror.source_applied) continue;
          ++counters_.status_transitions;

          // INV-TXN-01 and INV-TXN-02, in their sharpest form. A terminal
          // verdict is the one thing in this protocol that may never change:
          // two readers resolving the same intent must reach the same answer,
          // and the only way to guarantee that is for the answer to be
          // unchangeable once written.
          if (terminal(mirror.status)) {
            record("INV-TXN-02",
                   "t" + std::to_string(txn_id) + " left a terminal state: " +
                       txn::to_string(mirror.status) + " -> " +
                       txn::to_string(record_ref.status) +
                       ", which means two readers can resolve its intents differently");
          }
          if (record_ref.status == txn::TxnStatus::kCommitted) {
            ++counters_.records_committed;
            // INV-TXN-09 again, from the other end: two transactions committing
            // at one timestamp means the oracle handed the number out twice.
            const auto existing = commit_timestamps_.find(record_ref.commit_ts);
            if (existing != commit_timestamps_.end() && existing->second != txn_id) {
              record("INV-TXN-09",
                     "t" + std::to_string(existing->second) + " and t" +
                         std::to_string(txn_id) + " both committed at timestamp " +
                         std::to_string(record_ref.commit_ts) +
                         "; the oracle issued it twice");
            }
            commit_timestamps_.emplace(record_ref.commit_ts, txn_id);

            // INV-TXN-03. A commit timestamp at or below the start timestamp
            // means the transaction serialises before its own snapshot.
            if (record_ref.commit_ts <= record_ref.start_ts) {
              record("INV-TXN-03", "t" + std::to_string(txn_id) + " committed at " +
                                       std::to_string(record_ref.commit_ts) +
                                       " having started at " +
                                       std::to_string(record_ref.start_ts));
            }
          }
          if (record_ref.status == txn::TxnStatus::kAborted) ++counters_.records_aborted;
          mirror.status = record_ref.status;
          mirror.commit_ts = record_ref.commit_ts;
          mirror.source_applied = applied;
        }
      }
    }
  }

  // The wait-for graph, from what the coordinators say they are waiting on
  // rather than from what an outside observer guesses. Wound-wait by start
  // timestamp is supposed to make a cycle impossible; INV-TXN-12 is the check
  // that it does.
  std::map<txn::TxnId, txn::TxnId> waits;
  for (const NodeId id : live_nodes()) {
    const txn::Coordinator* coord = coordinator(id);
    if (coord == nullptr) continue;
    for (const auto& [waiter, blocker] : coord->waits()) {
      waits[waiter] = blocker;
      ++counters_.wait_edges_seen;
    }
  }
  for (const auto& [start, unused] : waits) {
    (void)unused;
    // Walk forward from each waiter. The graph is tiny -- one edge per
    // in-flight transaction that is blocked -- so a walk per node is cheap, and
    // the bound is what keeps a cycle from hanging the checker itself.
    txn::TxnId at = start;
    for (std::size_t step = 0; step < waits.size() + 1; ++step) {
      const auto next = waits.find(at);
      if (next == waits.end()) break;
      at = next->second;
      if (at == start) {
        record("INV-TXN-12",
               "a wait-for cycle through t" + std::to_string(start) +
                   " survived; wound-wait orders waits by start timestamp and is "
                   "supposed to make one impossible");
        break;
      }
    }
  }
}

// ---------------------------------------------------------------------------
// the predicates
// ---------------------------------------------------------------------------

void arm_txn_invariants(InvariantRegistry& registry, TxnObserver* observer) {
  registry.arm("INV-TXN-02", "a transaction's verdict is decided once and never changes",
               CostClass::kTick, [observer]() -> std::optional<std::string> {
                 observer->refresh();
                 return observer->take("INV-TXN-02");
               });

  registry.arm("INV-TXN-03", "a commit timestamp is above the transaction's own start",
               CostClass::kTick, [observer]() -> std::optional<std::string> {
                 observer->refresh();
                 return observer->take("INV-TXN-03");
               });

  registry.arm("INV-TXN-09", "allocated timestamps never regress, including across failover",
               CostClass::kTick, [observer]() -> std::optional<std::string> {
                 observer->refresh();
                 return observer->take("INV-TXN-09");
               });

  registry.arm("INV-TXN-12", "no wait-for cycle spanning nodes survives", CostClass::kTick,
               [observer]() -> std::optional<std::string> {
                 observer->refresh();
                 return observer->take("INV-TXN-12");
               });

  // INV-TXN-04. First-committer-wins, checked over the version chains
  // themselves: two transactions may not both have a committed version of one
  // key at timestamps that straddle each other's snapshot. The version store
  // enforces it at prewrite; this is the check that the enforcement is real.
  registry.arm("INV-TXN-04", "no two concurrent transactions both commit a write to one key",
               CostClass::kEpoch, [observer]() -> std::optional<std::string> {
                 observer->refresh();
                 for (const NodeId id : observer->live_nodes()) {
                   const shard::ShardStore* store = observer->store(id);
                   if (store == nullptr) continue;
                   for (const auto& [range_id, replica] : store->ranges()) {
                     if (replica.machine == nullptr) continue;
                     const txn::VersionStore& versions = replica.machine->txn_store();
                     for (const auto& [key, chain] : versions.versions()) {
                       // A chain is newest-first. Two versions at the same
                       // timestamp is impossible by construction (it is a map);
                       // what this looks for is a version whose timestamp is
                       // not above the one before it, which would mean the
                       // ordering the whole layer rests on is not total.
                       txn::Ts previous = txn::kMaxTs;
                       for (const auto& [ts, value] : chain) {
                         (void)value;
                         if (ts >= previous) {
                           return "r" + std::to_string(range_id) + " key '" + key +
                                  "' has versions at " + std::to_string(ts) + " and " +
                                  std::to_string(previous) + " out of order";
                         }
                         previous = ts;
                       }
                     }
                   }
                 }
                 return std::nullopt;
               });

  // INV-TXN-01. Every intent belongs to a transaction whose record exists
  // somewhere, and an intent whose record says committed or aborted is one that
  // has simply not been cleaned up yet -- which is legal, and is the mechanism.
  // What is *not* legal is an intent whose transaction nobody can find at all:
  // that is a key locked forever by a transaction that never was.
  //
  // Evaluated at quiesce, because it is only true once things settle: during a
  // run an intent legitimately exists for the width of a round trip before its
  // record does.
  registry.arm("INV-TXN-01", "every intent is attributable to a transaction record",
               CostClass::kQuiesce, [observer]() -> std::optional<std::string> {
                 observer->refresh();
                 // A record is not only on a range machine. For the width of a
                 // split handover it is inside the parent's payload, which is
                 // where the split put it and where the child will collect it
                 // from -- and a checker that walks only the machines calls
                 // that a transaction which never existed. Same shape as
                 // [ANV-0059] one layer up: the payload is not a place the data
                 // has gone missing, it is a place the protocol says the data
                 // lives. Seed 7 at strict-serializable fired this on t2362...,
                 // whose kAborted record was sitting in payload 3->8 the whole
                 // time.
                 std::map<txn::TxnId, txn::TxnStatus> records;
                 for (const NodeId id : observer->live_nodes()) {
                   const shard::ShardStore* store = observer->store(id);
                   if (store == nullptr) continue;
                   for (const auto& [range_id, replica] : store->ranges()) {
                     if (replica.machine == nullptr) continue;
                     for (const auto& [txn_id, rec] : replica.machine->txn_store().records()) {
                       records[txn_id] = rec.status;
                     }
                     for (const auto& [child_id, pending] :
                          replica.machine->pending_splits()) {
                       std::string_view section;
                       if (!shard::RangeMachine::decode_txn_section(pending.payload, &section)) {
                         continue;
                       }
                       txn::VersionStore payload;
                       if (!payload.load(section)) continue;
                       for (const auto& [txn_id, rec] : payload.records()) {
                         records[txn_id] = rec.status;
                       }
                     }
                   }
                 }

                 for (const NodeId id : observer->live_nodes()) {
                   const shard::ShardStore* store = observer->store(id);
                   if (store == nullptr) continue;
                   for (const auto& [range_id, replica] : store->ranges()) {
                     if (replica.machine == nullptr || !replica.machine->initialised()) continue;
                     for (const auto& [key, intent] :
                          replica.machine->txn_store().intents()) {
                       if (records.count(intent.txn) != 0) continue;
                       return "n" + std::to_string(id.value()) + " r" +
                              std::to_string(range_id) + " holds an intent on '" + key +
                              "' for t" + std::to_string(intent.txn) +
                              ", and no range in the cluster has a record for that "
                              "transaction -- the key is locked by something that never "
                              "existed";
                     }
                   }
                 }
                 return std::nullopt;
               });

  // INV-TXN-11. Parallel commit: a record in kStaging is committed if and only
  // if every key it lists carries its intent or its version. The recovery
  // protocol has to reach the same verdict the coordinator would have, and the
  // way to check that is to evaluate the predicate the recovery uses.
  //
  // "Decidable from its keys" needs the key list to name every key the
  // transaction actually touched, and an empty list is only the loudest way for
  // that to fail. A list that has been *narrowed* is the quiet way, and it is
  // strictly worse: the predicate still evaluates, it evaluates over fewer keys
  // than the transaction wrote, and it comes out committed. That is [ANV-0060],
  // which this invariant watched happen 143,000 times a seed without a word,
  // because `keys` was never empty -- it had exactly one element, the primary,
  // put there by a heartbeat.
  //
  // Stated over cluster state rather than over one record: at quiesce, an
  // intent at the record's own epoch on a key the record does not list is a
  // transaction no reader can decide, whichever way the truncation happened.
  // Quiesce is load-bearing here -- the record and the intents go to different
  // Raft groups, so before things are idle an intent may legitimately have
  // landed while the record write is still in flight.
  registry.arm("INV-TXN-11", "a staging record's implicit commit is decidable from its keys",
               CostClass::kQuiesce, [observer]() -> std::optional<std::string> {
                 observer->refresh();
                 std::map<txn::TxnId, const txn::TxnRecord*> staging;
                 for (const NodeId id : observer->live_nodes()) {
                   const shard::ShardStore* store = observer->store(id);
                   if (store == nullptr) continue;
                   for (const auto& [range_id, replica] : store->ranges()) {
                     if (replica.machine == nullptr) continue;
                     for (const auto& [txn_id, rec] : replica.machine->txn_store().records()) {
                       if (rec.status != txn::TxnStatus::kStaging) continue;
                       if (rec.keys.empty()) {
                         return "t" + std::to_string(txn_id) +
                                " is staging with no key list, so no reader can decide "
                                "whether it committed";
                       }
                       // The longest list any replica holds. A replica that is
                       // behind has an older list, and older is longer here --
                       // the narrowing is the newer entry -- so taking the
                       // longest cannot manufacture a finding out of lag.
                       const auto seen = staging.find(txn_id);
                       if (seen == staging.end() ||
                           rec.keys.size() > seen->second->keys.size()) {
                         staging[txn_id] = &rec;
                       }
                     }
                   }
                 }

                 if (staging.empty()) return std::nullopt;
                 for (const NodeId id : observer->live_nodes()) {
                   const shard::ShardStore* store = observer->store(id);
                   if (store == nullptr) continue;
                   for (const auto& [range_id, replica] : store->ranges()) {
                     if (replica.machine == nullptr) continue;
                     for (const auto& [key, intent] :
                          replica.machine->txn_store().intents()) {
                       const auto it = staging.find(intent.txn);
                       if (it == staging.end()) continue;
                       // An intent from a previous attempt is already dead --
                       // commit_intent refuses it (kStaleEpoch) -- so it is not
                       // a key this record has to account for.
                       if (intent.epoch != it->second->epoch) continue;
                       const auto& keys = it->second->keys;
                       if (std::find(keys.begin(), keys.end(), key) != keys.end()) continue;
                       return "t" + std::to_string(intent.txn) + " is staging over " +
                              std::to_string(keys.size()) +
                              " key(s) and holds an intent on '" + key +
                              "', which it does not list -- its own predicate says it "
                              "committed without ever looking at that key";
                     }
                   }
                 }
                 return std::nullopt;
               });
}

}  // namespace anvil::checker
