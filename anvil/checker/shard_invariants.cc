#include "anvil/checker/shard_invariants.h"

#include <algorithm>

namespace anvil::checker {
namespace {

std::string node_list(const std::vector<NodeId>& nodes) {
  std::string out;
  for (const NodeId n : nodes) {
    if (!out.empty()) out += ",";
    out += "n" + std::to_string(n.value());
  }
  return out.empty() ? "(none)" : out;
}

bool has(const std::vector<NodeId>& nodes, NodeId id) {
  return std::find(nodes.begin(), nodes.end(), id) != nodes.end();
}

// An order-sensitive digest of the whole topology. Two replicas that have
// applied the same entries must produce the same number; comparing the states
// field by field would be more informative and vastly more code, and the
// number is enough to *find* the divergence -- which is the hard part.
std::uint64_t digest(const shard::TopologyState& state) {
  std::uint64_t h = 0xcbf29ce484222325ULL;
  const auto mix = [&h](std::uint64_t v) {
    h ^= v + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
  };
  const auto mix_bytes = [&mix](std::string_view s) {
    for (const char c : s) mix(static_cast<std::uint64_t>(static_cast<unsigned char>(c)));
    mix(s.size());
  };
  for (const auto& [start, desc] : state.ranges) {
    mix_bytes(start);
    mix(desc.id.value());
    mix_bytes(desc.end);
    mix(desc.generation);
    for (const NodeId n : desc.replicas) mix(n.value());
    mix(0xF0F0);
    for (const NodeId n : desc.learners) mix(n.value());
    mix(desc.lease.holder.value());
    mix(desc.frozen ? 1 : 0);
  }
  mix(state.next_range_id);
  for (const auto& [id, stat] : state.stats) {
    mix(id);
    mix(stat.keys);
    mix_bytes(stat.median);
    for (const NodeId n : stat.caught_up) mix(n.value());
  }
  return h;
}

}  // namespace

void ShardObserver::configure(std::uint32_t nodes, Hooks hooks) {
  nodes_ = nodes;
  hooks_ = std::move(hooks);
}

void ShardObserver::set_store(NodeId id, const shard::ShardStore* store) {
  stores_[id.value()] = store;
}

const shard::ShardStore* ShardObserver::store(NodeId id) const {
  const auto it = stores_.find(id.value());
  if (it == stores_.end()) return nullptr;
  if (hooks_.alive && !hooks_.alive(id)) return nullptr;
  return it->second;
}

Timestamp ShardObserver::node_now(NodeId id) const {
  return hooks_.node_now ? hooks_.node_now(id) : Timestamp{};
}

Timestamp ShardObserver::true_now() const {
  return hooks_.true_now ? hooks_.true_now() : Timestamp{};
}

std::vector<NodeId> ShardObserver::live_nodes() const {
  std::vector<NodeId> out;
  for (std::uint32_t i = 1; i <= nodes_; ++i) {
    if (store(NodeId{i}) != nullptr) out.push_back(NodeId{i});
  }
  return out;
}

void ShardObserver::record(const std::string& id, std::string detail) {
  pending_[id].push_back(std::move(detail));
}

std::optional<std::string> ShardObserver::take(const std::string& id) {
  const auto it = pending_.find(id);
  if (it == pending_.end() || it->second.empty()) return std::nullopt;
  std::string out = std::move(it->second.front());
  it->second.erase(it->second.begin());
  return out;
}

// ---------------------------------------------------------------------------
// the scan
// ---------------------------------------------------------------------------

void ShardObserver::refresh() {
  const std::uint64_t tick = hooks_.tick ? hooks_.tick() : 0;
  if (tick == last_tick_) return;
  last_tick_ = tick;
  ++counters_.scans;

  // The topology is read from whichever live node has applied the most of it.
  // Not from "the leader": during an election there is no leader, and a checker
  // that stops checking whenever the cluster is between leaders is a checker
  // that is asleep during precisely the interesting moments.
  // The lowest live node, not the most advanced one.
  //
  // "Most advanced" flaps between replicas several times a second -- they take
  // turns being one entry ahead -- and every flap re-seeds the mirrors, so the
  // transitions the observer exists to check are mostly not checked. A stable
  // source lags a little and sees every transition in order, which is the
  // property that matters: this is a replicated state machine, so any replica's
  // applied sequence is the same sequence.
  const shard::ShardStore* freshest = nullptr;
  NodeId freshest_id{};
  for (const NodeId id : live_nodes()) {
    const shard::ShardStore* s = store(id);
    if (s == nullptr) continue;
    const raft::RaftDriver* driver = s->placement_driver();
    if (driver == nullptr || !driver->ready() || driver->node().snapshot_pending()) continue;
    if (driver->node().log().applied_index().value() == 0) continue;
    freshest = s;
    freshest_id = id;
    break;
  }
  if (freshest == nullptr) return;

  // Transitions are read from one replica's own applied sequence. When that
  // replica changes -- it crashed, or another overtook it -- the mirrors are
  // re-seeded and nothing is reported for that tick: the difference between
  // two nodes is not a transition, and reporting it as one produces a
  // generation that appears to move backwards and a merge that appears to
  // happen twice.
  if (freshest_id.value() != source_) {
    source_ = freshest_id.value();
    ++counters_.source_changes;
    mirrors_.clear();
  }

  // How far every live node's clock actually is from true time, measured every
  // tick. The declared bound is what every lease argument rests on, and a node
  // whose clock was *frozen* by the fault injector is outside it without any
  // configuration flag saying so -- so the precondition is measured rather than
  // inferred from the profile.
  const std::uint64_t truth = true_now().physical;
  for (const NodeId id : live_nodes()) {
    const std::uint64_t theirs = node_now(id).physical;
    const std::uint64_t error = theirs > truth ? theirs - truth : truth - theirs;
    if (error > counters_.worst_clock_error_nanos) counters_.worst_clock_error_nanos = error;
  }

  const shard::TopologyState& state = freshest->topology().state();
  counters_.ranges_high_water = std::max<std::uint64_t>(counters_.ranges_high_water,
                                                        state.ranges.size());

  for (const auto& [start, desc] : state.ranges) {
    history_[desc.id.value()] = desc;
    RangeMirror& mirror = mirrors_[desc.id.value()];
    const shard::RangeStats* stats = nullptr;
    const auto stat_it = state.stats.find(desc.id.value());
    if (stat_it != state.stats.end()) stats = &stat_it->second;

    if (!mirror.seen) {
      mirror.seen = true;
      mirror.descriptor = desc;
      mirror.max_generation = desc.generation;
      if (stats != nullptr) mirror.caught_up = stats->caught_up;
      continue;
    }

    const shard::RangeDescriptor& before = mirror.descriptor;

    // ---- INV-SHARD-05: generations ---------------------------------------
    if (desc.generation < mirror.max_generation) {
      record("INV-SHARD-05",
             "r" + std::to_string(desc.id.value()) + " went back from generation " +
                 std::to_string(mirror.max_generation) + " to " +
                 std::to_string(desc.generation));
    }
    const bool span_changed = before.start != desc.start || before.end != desc.end;
    const bool members_changed =
        before.replicas != desc.replicas || before.learners != desc.learners;
    if ((span_changed || members_changed) && desc.generation == before.generation) {
      // The change nobody can see. A descriptor whose span moves without its
      // generation moving leaves every cached copy in the cluster valid
      // forever, so a client keeps writing keys to a range that no longer owns
      // them and nothing anywhere reports an error.
      record("INV-SHARD-05",
             "r" + std::to_string(desc.id.value()) + " changed from [" + before.start + "," +
                 before.end + ") on " + node_list(before.replicas) + " to [" + desc.start + "," +
                 desc.end + ") on " + node_list(desc.replicas) + " without its generation moving (" +
                 std::to_string(desc.generation) + ")");
    }

    // ---- INV-SHARD-03: merge preconditions -------------------------------
    if (desc.frozen && !before.frozen) {
      ++counters_.merges_seen;
      // The left neighbour as it was *before* the freeze, taken from the
      // mirrors rather than from the state in hand.
      //
      // The apply that accepted this merge judged it against the state in force
      // at that moment; by the time the observer sees the freeze, a lease may
      // already have moved. Judging the decision against the state as it stands
      // afterwards reports correct merges as violations -- the same mistake as
      // ANV-0021 and the same fix (CONTEXT.md 10.11).
      const shard::RangeDescriptor* left = nullptr;
      for (const auto& [other_id, other_mirror] : mirrors_) {
        if (!other_mirror.seen) continue;
        if (other_mirror.descriptor.end == before.start &&
            other_mirror.descriptor.id != desc.id) {
          left = &other_mirror.descriptor;
        }
      }
      if (left == nullptr) {
        record("INV-SHARD-03", "r" + std::to_string(desc.id.value()) +
                                   " was frozen for a merge with no adjacent left neighbour");
      } else {
        if (left->replicas != before.replicas) {
          record("INV-SHARD-03",
                 "r" + std::to_string(left->id.value()) + " on " + node_list(left->replicas) +
                     " is merging r" + std::to_string(desc.id.value()) + " on " +
                     node_list(before.replicas) + "; the two are not colocated, so some replica of "
                     "the survivor has never seen the data it is about to own");
        }
        // The other half of colocation -- that both ranges are led by one node
        // -- is deliberately NOT checked here, and the reason is worth stating
        // rather than leaving as an omission.
        //
        // A lease moves several times a second. An observer that diffs state
        // once per tick cannot reconstruct which lease was in force at the
        // instant the merge was applied, so grading the decision against the
        // lease it can see reports correct merges as violations -- which it did,
        // on the control run of the drill, until this clause came out. The
        // alternative is a hook in the topology machine recording its own
        // inputs, and a state machine that carries evidence for its checker is
        // no longer the thing that ships.
        //
        // So the lease half is enforced at apply time and checked directly in
        // test_a_merge_requires_colocation (shard_test.cc), where the state at
        // the moment of the decision is known because the test wrote it. This
        // is a blind spot in the sweep, and it is named as one.
      }
    }

    // ---- INV-SHARD-06: replica changes -----------------------------------
    if (before.replicas != desc.replicas) {
      ++counters_.replica_changes;
      // The *current* catch-up set, not the mirrored one from before the
      // change. The set only ever grows within a range's life, so using the
      // newest is the permissive direction: it still catches a voter that was
      // never reported caught up at any point -- which is what the mutation
      // produces -- without reporting a legal promotion whose catch-up report
      // landed in the same batch as the promotion itself. Grading a decision
      // against a sample taken at a different moment is how a checker invents
      // findings (ANV-0040, ANV-0043, and the lease clause of INV-SHARD-03).
      const std::vector<NodeId> caught_up =
          stats == nullptr ? mirror.caught_up : stats->caught_up;
      for (const NodeId added : desc.replicas) {
        if (has(before.replicas, added)) continue;
        const bool was_learner = has(before.learners, added);
        if (!was_learner || !has(caught_up, added)) {
          record("INV-SHARD-06",
                 "r" + std::to_string(desc.id.value()) + " promoted n" +
                     std::to_string(added.value()) + " to voter " +
                     (was_learner ? "before it had caught up" : "without it ever being a learner") +
                     "; voters went from " + node_list(before.replicas) + " to " +
                     node_list(desc.replicas) + ", caught up: " + node_list(caught_up));
        }
      }
      std::size_t common = 0;
      for (const NodeId n : before.replicas) {
        if (has(desc.replicas, n)) ++common;
      }
      if (common < before.replicas.size() / 2 + 1 || common < desc.replicas.size() / 2 + 1) {
        record("INV-SHARD-06",
               "r" + std::to_string(desc.id.value()) + " changed voters from " +
                   node_list(before.replicas) + " to " + node_list(desc.replicas) +
                   ", which share only " + std::to_string(common) +
                   " members -- two disjoint majorities are possible");
      }
    }

    if (span_changed) ++counters_.descriptor_changes;
    if (before.lease.holder != desc.lease.holder) ++counters_.lease_changes;

    mirror.descriptor = desc;
    mirror.max_generation = std::max(mirror.max_generation, desc.generation);
    if (stats != nullptr) mirror.caught_up = stats->caught_up;
  }

  check_lease_sequence();
}

// INV-SHARD-04, checked as a rule rather than as a coincidence.
//
// Waiting to catch two nodes *simultaneously* believing they hold a lease finds
// the bug only when a replica is also lagging, which needs a partition on top
// of the defect -- so a broken lease rule can sit there for thousands of seeds
// looking fine. The rule itself is directly checkable: successive leases over
// one range, as recorded in that range's own log, must not overlap once the
// declared clock bound is allowed for in *both* directions. A grant that starts
// less than two bounds after the previous expiry is unsound whether or not
// anybody was there to see the overlap.
//
// The sequence is read from the range's own machine, never from the topology.
// The topology's copy is a publication -- best-effort, reordered, sometimes
// dropped -- and checking a rule against a copy that does not implement it
// reports violations that never happened.
void ShardObserver::check_lease_sequence() {
  const std::uint64_t margin = 2 * static_cast<std::uint64_t>(uncertainty_.nanos());
  std::set<std::uint64_t> seen;
  for (const NodeId id : live_nodes()) {
    const shard::ShardStore* s = store(id);
    if (s == nullptr) continue;
    for (const auto& [range_id, replica] : s->ranges()) {
      if (replica.machine == nullptr || !replica.machine->initialised()) continue;
      // One source per range, the first live node that hosts it, so that the
      // sequence observed is one replica's applied sequence and not a
      // interleaving of several.
      if (!seen.insert(range_id).second) continue;
      const shard::Lease& lease = replica.machine->descriptor().lease;
      // The pair comes from the machine, which keeps the lease this one
      // replaced. Diffing the *current* lease between two ticks would compare
      // two leases that were never adjacent whenever two grants land in one
      // apply batch, and report a handover gap that never existed.
      const shard::Lease& previous = replica.machine->previous_lease();
      auto& mirror = leases_[range_id];
      if (mirror.source != id.value()) {
        mirror.source = id.value();
        mirror.lease = lease;
        continue;
      }
      // Only a change of *holder* is a handover. A renewal moves the start and
      // the expiry and leaves the holder alone, and the machine's recorded
      // previous lease is still the one before the handover -- so comparing a
      // renewal's start against it measures an interval that was never a
      // handover gap, and reports a violation on a perfectly legal lease.
      const bool handover = lease.holder != mirror.lease.holder;
      mirror.lease = lease;
      if (!handover) continue;
      if (!previous.holder.valid() || !lease.holder.valid()) continue;
      if (lease.holder == previous.holder) continue;
      ++counters_.lease_changes;
      if (lease.start >= previous.expiry + margin) continue;
      record("INV-SHARD-04",
             "r" + std::to_string(range_id) + ": n" + std::to_string(lease.holder.value()) +
                 " took the lease at " + std::to_string(lease.start / 1000000) +
                 "ms while n" + std::to_string(previous.holder.value()) + " held it until " +
                 std::to_string(previous.expiry / 1000000) +
                 "ms -- less than two clock bounds (" + std::to_string(margin / 1000000) +
                 "ms) apart, so both can be live at the same real instant");
    }
  }
}

// ---------------------------------------------------------------------------
// the predicates
// ---------------------------------------------------------------------------

void arm_shard_invariants(InvariantRegistry& registry, ShardObserver* observer) {
  // INV-SHARD-01. Coverage, on every live node's own applied view -- not just
  // on the leader's. Each command is one apply, so every replica's prefix of
  // the topology log is itself a legal topology; a replica that has applied
  // half of a split has applied a state in which the key space is covered
  // twice, and that is exactly the bug.
  registry.arm("INV-SHARD-01", "range descriptors tile the key space exactly once",
               CostClass::kEpoch, [observer]() -> std::optional<std::string> {
                 observer->refresh();
                 for (const NodeId id : observer->live_nodes()) {
                   const shard::ShardStore* store = observer->store(id);
                   if (store == nullptr) continue;
                   const auto violation = store->topology().state().coverage_violation();
                   if (violation.has_value()) {
                     return "n" + std::to_string(id.value()) + " at applied index " +
                            std::to_string(store->topology().applied_index().value()) + ": " +
                            *violation;
                   }
                 }
                 return std::nullopt;
               });

  // INV-SHARD-02. Every key a range holds is a key that range owns.
  //
  // This is the split-atomicity invariant stated where it can be checked
  // cheaply. A transaction that spanned a split point and half-applied leaves a
  // key on the wrong side; so does a write accepted against a stale descriptor;
  // so does a split that moved the wrong half. All three show up here, at the
  // tick they happen, rather than as a total that fails to add up at the end of
  // the run.
  registry.arm("INV-SHARD-02", "a range holds only keys inside its own span", CostClass::kEpoch,
               [observer]() -> std::optional<std::string> {
                 observer->refresh();
                 for (const NodeId id : observer->live_nodes()) {
                   const shard::ShardStore* store = observer->store(id);
                   if (store == nullptr) continue;
                   for (const auto& [range_id, replica] : store->ranges()) {
                     if (replica.machine == nullptr || !replica.machine->initialised()) continue;
                     const shard::RangeDescriptor& desc = replica.machine->descriptor();
                     for (const auto& [key, value] : replica.machine->balances()) {
                       if (desc.contains(key)) continue;
                       return "n" + std::to_string(id.value()) + " holds '" + key + "' in r" +
                              std::to_string(range_id) + " which owns [" + desc.start + "," +
                              desc.end + ") at generation " + std::to_string(desc.generation);
                     }
                   }
                 }
                 return std::nullopt;
               });

  registry.arm("INV-SHARD-03", "a merge requires colocation, a common lease and adjacency",
               CostClass::kTick, [observer]() -> std::optional<std::string> {
                 observer->refresh();
                 return observer->take("INV-SHARD-03");
               });

  // INV-SHARD-04. At most one node believes it holds a valid lease for a range.
  //
  // Each node judges its own lease against its own clock, which is the only
  // clock it has. The simulator can see all of them at once, and the fault
  // profile can push them apart by more than the bound the configuration
  // declared -- so this is stated as "not two holders, judged by each holder's
  // own clock", which is the property a lease read actually depends on.
  registry.arm("INV-SHARD-04", "at most one valid lease per range at any instant", CostClass::kTick,
               [observer]() -> std::optional<std::string> {
                 observer->refresh();
                 std::map<std::uint64_t, std::vector<std::pair<NodeId, std::uint64_t>>> holders;
                 for (const NodeId id : observer->live_nodes()) {
                   const shard::ShardStore* store = observer->store(id);
                   if (store == nullptr) continue;
                   // Judged against this node's own clock, because that is the
                   // clock it would use to decide it may serve a read. Judging
                   // every node against one true clock would test a lease
                   // nobody implements.
                   const std::uint64_t now = observer->node_now(id).physical;
                   for (const auto& [range_id, replica] : store->ranges()) {
                     if (replica.machine == nullptr) continue;
                     if (!replica.machine->lease_valid(id, now)) continue;
                     holders[range_id].push_back(
                         {id, replica.machine->descriptor().lease.expiry});
                   }
                 }
                 if (const auto recorded = observer->take("INV-SHARD-04")) return recorded;
                 for (const auto& [range_id, list] : holders) {
                   if (list.size() < 2) continue;
                   observer->note_lease_overlap();

                   // Was the bound actually holding when this happened?
                   //
                   // Keying the exemption off the configuration flag would be
                   // easier and weaker: a node whose clock was *frozen* by the
                   // fault injector is outside the bound whatever the flag
                   // says. So the precondition is measured, at the instant the
                   // overlap is observed, against the bound the configuration
                   // declared.
                   const std::uint64_t bound =
                       static_cast<std::uint64_t>(observer->clock_uncertainty().nanos());
                   const std::uint64_t truth = observer->true_now().physical;
                   std::uint64_t worst = 0;
                   for (const auto& [node, expiry] : list) {
                     const std::uint64_t theirs = observer->node_now(node).physical;
                     const std::uint64_t error =
                         theirs > truth ? theirs - truth : truth - theirs;
                     worst = std::max(worst, error);
                   }
                   std::string detail = "r" + std::to_string(range_id) + " is claimed by";
                   for (const auto& [node, expiry] : list) {
                     const std::uint64_t theirs = observer->node_now(node).physical;
                     detail += " n" + std::to_string(node.value()) + "(until " +
                               std::to_string(expiry / 1000000) + "ms, clock " +
                               std::to_string(theirs / 1000000) + "ms)";
                   }
                   detail += "; true time " + std::to_string(truth / 1000000) +
                             "ms, worst clock error " + std::to_string(worst / 1000000) +
                             "ms against a declared bound of " +
                             std::to_string(bound / 1000000) + "ms";
                   if (worst > bound) {
                     observer->note_lease_overlap_out_of_bound(worst);
                     continue;  // the environment broke its own promise
                   }
                   return detail;
                 }
                 return std::nullopt;
               });

  registry.arm("INV-SHARD-05", "descriptor generations move whenever the descriptor does",
               CostClass::kTick, [observer]() -> std::optional<std::string> {
                 observer->refresh();
                 return observer->take("INV-SHARD-05");
               });

  registry.arm("INV-SHARD-06", "a rebalance never adds a voter that does not hold the data",
               CostClass::kTick, [observer]() -> std::optional<std::string> {
                 observer->refresh();
                 return observer->take("INV-SHARD-06");
               });

  // INV-SHARD-07. Two halves, and they fail for different reasons.
  //
  //   (a) The meta index a client reads must describe the descriptor table it
  //       was built from. Exact: they are on the same machine.
  //   (b) A range's own descriptor may lag the topology's -- that lag is a Raft
  //       round trip and the design permits it -- but not forever. A lag that
  //       outlives the bound is a range that has stopped following the
  //       topology, and a client routed to it will be rejected until somebody
  //       notices.
  registry.arm("INV-SHARD-07", "the meta index agrees with the range topology", CostClass::kEpoch,
               [observer]() -> std::optional<std::string> {
                 observer->refresh();
                 for (const NodeId id : observer->live_nodes()) {
                   const shard::ShardStore* store = observer->store(id);
                   if (store == nullptr) continue;
                   const shard::TopologyState& state = store->topology().state();
                   if (state.meta.size() != state.ranges.size()) {
                     return "n" + std::to_string(id.value()) + " has " +
                            std::to_string(state.ranges.size()) + " ranges but " +
                            std::to_string(state.meta.size()) + " meta records";
                   }
                   for (const auto& [start, desc] : state.ranges) {
                     shard::RangeDescriptor found;
                     shard::MetaBucket bucket;
                     const std::string probe = start.empty() ? std::string(1, '\x01') : start;
                     if (!state.meta.lookup_bucket(probe, &bucket) ||
                         !state.meta.lookup_range(bucket.id, probe, &found)) {
                       return "n" + std::to_string(id.value()) + ": the meta index cannot resolve '" +
                              probe + "', which r" + std::to_string(desc.id.value()) + " owns";
                     }
                     if (found.id != desc.id || found.generation != desc.generation) {
                       return "n" + std::to_string(id.value()) + ": meta resolves '" + probe +
                              "' to r" + std::to_string(found.id.value()) + " generation " +
                              std::to_string(found.generation) + ", the topology says r" +
                              std::to_string(desc.id.value()) + " generation " +
                              std::to_string(desc.generation);
                     }
                   }
                 }
                 return std::nullopt;
               });

  // INV-SHARD-08. A quiesced range is one nobody is sending anything to, so a
  // replica that is behind when it quiesces stays behind. The check is on the
  // leader, because it is the leader that stops sending.
  registry.arm("INV-SHARD-08", "a quiesced range has every replica holding the whole log",
               CostClass::kTick, [observer]() -> std::optional<std::string> {
                 observer->refresh();
                 for (const NodeId id : observer->live_nodes()) {
                   const shard::ShardStore* store = observer->store(id);
                   if (store == nullptr) continue;
                   for (const auto& [range_id, replica] : store->ranges()) {
                     if (!replica.quiesced || replica.driver == nullptr) continue;
                     if (replica.driver->node().role() != raft::Role::kLeader) continue;
                     const LogIndex last = replica.driver->node().log().last_index();
                     for (const auto& [peer, progress] : replica.driver->node().progress()) {
                       if (NodeId{peer} == id) continue;
                       if (observer->store(NodeId{peer}) == nullptr) continue;  // crashed
                       if (progress.match >= last) continue;
                       return "r" + std::to_string(range_id) + " quiesced on leader n" +
                              std::to_string(id.value()) + " at index " +
                              std::to_string(last.value()) + " with n" + std::to_string(peer) +
                              " at " + std::to_string(progress.match.value()) +
                              "; nothing will be sent to it again";
                     }
                   }
                 }
                 return std::nullopt;
               });

  // INV-SHARD-09. Two replicas that have applied the same entries must hold the
  // same topology and must decide the same things from it. The first half is
  // state-machine determinism; the second is the property the placement driver
  // was written to have, and the only reason decide() is a pure function.
  registry.arm("INV-SHARD-09", "placement decisions are a function of replicated state alone",
               CostClass::kEpoch, [observer]() -> std::optional<std::string> {
                 observer->refresh();
                 std::map<std::uint64_t, std::pair<NodeId, std::uint64_t>> by_index;
                 std::map<std::uint64_t, std::pair<NodeId, std::string>> decisions;
                 for (const NodeId id : observer->live_nodes()) {
                   const shard::ShardStore* store = observer->store(id);
                   if (store == nullptr) continue;
                   // The *Raft log's* applied index, not the state machine's.
                   // A machine restored from a snapshot has a state at index K
                   // and an applied_ field that never saw K go past, so two
                   // nodes can report the same number for different prefixes --
                   // and the checker then reports a divergence that is entirely
                   // its own (ANV-0040, and CONTEXT.md 10.20 again).
                   const raft::RaftDriver* driver = store->placement_driver();
                   if (driver == nullptr || !driver->ready()) continue;
                   // A node mid-snapshot-install is between two states: its log
                   // says one thing and its state machine still says the
                   // previous one. Skipped rather than compared.
                   if (driver->node().snapshot_pending()) continue;
                   const std::uint64_t applied = driver->node().log().applied_index().value();
                   if (applied == 0) continue;  // nothing applied yet; no state to compare
                   const shard::TopologyState& state = store->topology().state();
                   const std::uint64_t d = digest(state);

                   const auto seen = by_index.find(applied);
                   if (seen == by_index.end()) {
                     by_index[applied] = {id, d};
                   } else if (seen->second.second != d) {
                     const shard::ShardStore* other = observer->store(seen->second.first);
                     std::string detail = "n" + std::to_string(seen->second.first.value()) +
                                          " and n" + std::to_string(id.value()) +
                                          " have both applied topology entry " +
                                          std::to_string(applied) + " and hold different states: ";
                     if (other != nullptr) {
                       for (const auto& [start, desc] : other->topology().state().ranges) {
                         detail += desc.describe() + " | ";
                       }
                     }
                     if (other != nullptr && other->placement_driver() != nullptr) {
                       const raft::RaftNode& n = other->placement_driver()->node();
                       detail += "[term " + std::to_string(n.term().value()) + " commit " +
                                 std::to_string(n.log().commit_index().value()) + " last " +
                                 std::to_string(n.log().last_index().value()) + " snap " +
                                 std::to_string(n.log().snapshot_index().value()) + " cmds " +
                                 std::to_string(other->topology().applied_commands()) + "]";
                     }
                     detail += "   versus   ";
                     for (const auto& [start, desc] : state.ranges) {
                       detail += desc.describe() + " | ";
                     }
                     const raft::RaftNode& mine = driver->node();
                     detail += "[term " + std::to_string(mine.term().value()) + " commit " +
                               std::to_string(mine.log().commit_index().value()) + " last " +
                               std::to_string(mine.log().last_index().value()) + " snap " +
                               std::to_string(mine.log().snapshot_index().value()) + " cmds " +
                               std::to_string(store->topology().applied_commands()) + "]";
                     return detail;
                   }

                   // The same state must yield the same decisions. `now` is
                   // fixed across the comparison on purpose: it is the one
                   // input that is not replicated, and holding it constant is
                   // what isolates the property being checked.
                   std::string rendered;
                   for (const shard::Decision& decision :
                        shard::decide(state, store->options().placement, Timestamp{},
                                      store->options().cluster_size)) {
                     rendered += decision.command.describe() + ";";
                   }
                   const auto other = decisions.find(applied);
                   if (other == decisions.end()) {
                     decisions[applied] = {id, rendered};
                   } else if (other->second.second != rendered) {
                     return "at topology entry " + std::to_string(applied) + ", n" +
                            std::to_string(other->second.first.value()) + " would decide [" +
                            other->second.second + "] and n" + std::to_string(id.value()) +
                            " would decide [" + rendered + "]";
                   }
                 }
                 return std::nullopt;
               });
}

}  // namespace anvil::checker
