#include "anvil/core/shard/placement.h"

#include <algorithm>

namespace anvil::shard {
namespace {

bool contains(const std::vector<NodeId>& nodes, NodeId id) {
  return std::find(nodes.begin(), nodes.end(), id) != nodes.end();
}

const RangeStats* stats_for(const TopologyState& state, RangeId id) {
  const auto it = state.stats.find(id.value());
  return it == state.stats.end() ? nullptr : &it->second;
}

}  // namespace

std::vector<NodeId> live_nodes(const TopologyState& state, const PlacementOptions& options,
                               Timestamp now, std::uint32_t cluster_size) {
  std::vector<NodeId> live;
  for (std::uint32_t i = 1; i <= cluster_size; ++i) {
    const auto it = state.nodes.find(i);
    if (it == state.nodes.end()) continue;
    const std::uint64_t age =
        now.physical > it->second.last_seen ? now.physical - it->second.last_seen : 0;
    if (age <= static_cast<std::uint64_t>(options.node_dead_after.nanos())) {
      live.push_back(NodeId{i});
    }
  }
  return live;
}

std::vector<Decision> decide(const TopologyState& state, const PlacementOptions& options,
                             Timestamp now, std::uint32_t cluster_size) {
  std::vector<Decision> out;
  if (state.ranges.empty()) return out;

  const std::vector<NodeId> live = live_nodes(state, options, now, cluster_size);

  // A range that has just changed is left alone for a while. Without this, a
  // split threshold and a merge threshold that overlap oscillate as fast as
  // Raft will commit, and the cluster spends every cycle undoing its own last
  // decision -- which looks like a busy rebalancer and is a livelock.
  const auto settled = [&](const RangeDescriptor& desc) {
    if (state.applied <= desc.changed_index) return false;
    return state.applied - desc.changed_index >= options.change_cooldown_entries;
  };

  // Ranges are considered in key order, always. Iterating a map keyed by start
  // is what makes two replicas emit the same decisions in the same order; over
  // an unordered container they would emit the same *set* on one machine and a
  // different order on another, and "the same set in a different order" is a
  // different sequence of Raft entries.
  for (const auto& [start, desc] : state.ranges) {
    if (out.size() >= options.max_commands) break;
    if (desc.frozen) {
      // A frozen range is left alone, and in particular it is never unfrozen.
      //
      // An abort looks like obvious hygiene -- a merge whose coordinator died
      // should not block writes to its span forever -- and it is a loaded gun.
      // The survivor absorbs the subsumed range's data in its own log, and the
      // topology cannot see that happen; an abort that lands after the absorb
      // un-freezes a range whose contents are now in two places at once, and
      // the total stops adding up. Seed 8 of the fault sweep did exactly that:
      // twelve accounts held by two ranges and 4,200 in a cluster that started
      // with 2,400 (ANV-0046).
      //
      // So a merge, once begun, is completed and never abandoned. The cost is
      // real and it is availability, not correctness: the subsumed span rejects
      // writes until some node leads both groups again, which under eventual
      // synchrony is one election. An operator-driven abort is still possible
      // -- kAbortMerge exists and the topology applies it -- but nothing
      // automatic proposes one, because nothing automatic can know whether the
      // absorb has already happened.
      continue;
    }

    // ---- repair before anything else -------------------------------------
    //
    // A range short of replicas is a durability problem; a range that is too
    // large is a performance problem. Doing them in the other order means a
    // cluster that has just lost a node spends its next several decisions
    // splitting ranges it cannot replicate.
    // A range next to one that is being merged is left alone as well. A merge
    // requires the two to be on the same replicas, and moving one of them
    // mid-merge strands the subsumed range's data on nodes that no longer host
    // the survivor.
    bool neighbour_merging = false;
    for (const auto& [other_start, other] : state.ranges) {
      if (!other.frozen) continue;
      if (other.start == desc.end || other.end == desc.start) neighbour_merging = true;
    }
    if (neighbour_merging) continue;

    std::vector<NodeId> dead_voters;
    for (const NodeId voter : desc.replicas) {
      if (!contains(live, voter)) dead_voters.push_back(voter);
    }

    const RangeStats* stats = stats_for(state, desc.id);
    const std::vector<NodeId> caught_up =
        stats == nullptr ? std::vector<NodeId>{} : stats->caught_up;

    if (!dead_voters.empty() && desc.replicas.size() > 1) {
      // A learner that has caught up can be promoted in place of a dead voter.
      NodeId replacement{};
      for (const NodeId learner : desc.learners) {
        if (!contains(live, learner)) continue;
        if (options.promote_only_caught_up && !contains(caught_up, learner)) continue;
        replacement = learner;
        break;
      }
      if (replacement.valid()) {
        AdminCommand cmd;
        cmd.op = AdminOp::kChangeReplicas;
        cmd.range = desc.id;
        cmd.generation = desc.generation;
        cmd.time = now.physical;
        cmd.replicas = desc.replicas;
        cmd.replicas.erase(std::remove(cmd.replicas.begin(), cmd.replicas.end(), dead_voters[0]),
                           cmd.replicas.end());
        cmd.replicas.push_back(replacement);
        for (const NodeId learner : desc.learners) {
          if (learner != replacement) cmd.learners.push_back(learner);
        }
        out.push_back({cmd, "n" + std::to_string(dead_voters[0].value()) +
                                " is dead; n" + std::to_string(replacement.value()) +
                                " has caught up and takes its place"});
        continue;
      }

      // Otherwise add a learner and wait for it. The waiting is the point: a
      // replacement voter that holds nothing is a replica in name only, and the
      // range's real replication factor has quietly dropped by one.
      NodeId candidate{};
      for (const NodeId node : live) {
        if (desc.hosts(node)) continue;
        candidate = node;
        break;
      }
      if (candidate.valid()) {
        AdminCommand cmd;
        cmd.op = AdminOp::kChangeReplicas;
        cmd.range = desc.id;
        cmd.generation = desc.generation;
        cmd.time = now.physical;
        if (options.promote_only_caught_up) {
          cmd.replicas = desc.replicas;
          cmd.learners = desc.learners;
          cmd.learners.push_back(candidate);
        } else {
          // The mutation. Straight into the voter set, holding nothing.
          cmd.replicas = desc.replicas;
          cmd.replicas.erase(
              std::remove(cmd.replicas.begin(), cmd.replicas.end(), dead_voters[0]),
              cmd.replicas.end());
          cmd.replicas.push_back(candidate);
          cmd.learners = desc.learners;
        }
        out.push_back({cmd, "n" + std::to_string(dead_voters[0].value()) + " is dead; adding n" +
                                std::to_string(candidate.value())});
        continue;
      }
    }

    // Under-replicated with everything alive: bring it up to the target.
    if (dead_voters.empty() && desc.replicas.size() < options.target_replicas) {
      NodeId candidate{};
      for (const NodeId node : live) {
        if (desc.hosts(node)) continue;
        candidate = node;
        break;
      }
      // A caught-up learner is promoted; otherwise one is added.
      NodeId ready{};
      for (const NodeId learner : desc.learners) {
        if (!contains(live, learner)) continue;
        if (options.promote_only_caught_up && !contains(caught_up, learner)) continue;
        ready = learner;
        break;
      }
      if (ready.valid()) {
        AdminCommand cmd;
        cmd.op = AdminOp::kChangeReplicas;
        cmd.range = desc.id;
        cmd.generation = desc.generation;
        cmd.time = now.physical;
        cmd.replicas = desc.replicas;
        cmd.replicas.push_back(ready);
        for (const NodeId learner : desc.learners) {
          if (learner != ready) cmd.learners.push_back(learner);
        }
        out.push_back({cmd, "r" + std::to_string(desc.id.value()) + " promotes n" +
                                std::to_string(ready.value()) + " to reach the replication target"});
        continue;
      }
      if (candidate.valid()) {
        AdminCommand cmd;
        cmd.op = AdminOp::kChangeReplicas;
        cmd.range = desc.id;
        cmd.generation = desc.generation;
        cmd.time = now.physical;
        cmd.replicas = desc.replicas;
        cmd.learners = desc.learners;
        cmd.learners.push_back(candidate);
        out.push_back({cmd, "r" + std::to_string(desc.id.value()) + " is under-replicated; n" +
                                std::to_string(candidate.value()) + " joins as a learner"});
        continue;
      }
    }

    // ---- size ------------------------------------------------------------
    if (stats != nullptr && settled(desc) && stats->keys >= options.split_threshold_keys &&
        !stats->median.empty() && stats->median > desc.start &&
        (desc.end.empty() || stats->median < desc.end)) {
      AdminCommand cmd;
      cmd.op = AdminOp::kSplit;
      cmd.range = desc.id;
      cmd.generation = desc.generation;
      cmd.key = stats->median;
      cmd.time = now.physical;
      out.push_back({cmd, "r" + std::to_string(desc.id.value()) + " holds " +
                              std::to_string(stats->keys) + " keys"});
      continue;
    }

    // ---- merge -----------------------------------------------------------
    if (stats != nullptr && settled(desc) && stats->keys <= options.merge_threshold_keys &&
        !desc.end.empty()) {
      const RangeDescriptor* right = state.right_neighbour(desc.id);
      const RangeStats* right_stats = right == nullptr ? nullptr : stats_for(state, right->id);
      if (right != nullptr && right_stats != nullptr && !right->frozen && settled(*right) &&
          right_stats->keys <= options.merge_threshold_keys) {
        bool colocated = true;
        if (options.merge_requires_colocation) {
          colocated = desc.replicas == right->replicas && desc.lease.holder.valid() &&
                      desc.lease.holder == right->lease.holder;
        }
        if (colocated) {
          AdminCommand cmd;
          cmd.op = AdminOp::kBeginMerge;
          cmd.range = desc.id;
          cmd.other = right->id;
          cmd.generation = desc.generation;
          cmd.time = now.physical;
          out.push_back({cmd, "r" + std::to_string(desc.id.value()) + " and r" +
                                  std::to_string(right->id.value()) + " are both small"});
          continue;
        }
      }
    }
  }
  return out;
}

}  // namespace anvil::shard
