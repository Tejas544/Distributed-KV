// The placement driver: what should the topology become, and why.
//
// It is a pure function. `decide()` takes the replicated topology state and the
// current time, and returns commands. It does not read a socket, a disk, a
// local counter, or this node's opinion of who is alive -- every input is
// something every replica of the placement group has applied. That is not
// tidiness, it is INV-SHARD-09: a placement driver that decides from local
// state decides differently after every leader change, and the symptom is a
// range that splits and merges forever because two nodes disagree about how big
// it is. Making the function pure is what makes "the decision is a function of
// replicated state" a thing a checker can evaluate rather than a claim.
//
// The one input that is not replicated is `now`, and it cannot be: a decision
// about a node that stopped heartbeating four seconds ago has to know what time
// it is. So the property that is actually checked is the one that is actually
// true -- two replicas at the same applied index, given the same `now`, produce
// identical commands -- and the time dependence is written down here rather
// than discovered by whoever tries to reproduce a decision later.
//
// Every command decide() returns is a *proposal*. It carries the generation the
// decision was made against, and the topology's apply rejects it if the range
// has changed since. Deciding and applying are separated by a Raft round trip,
// and in that window a range can split, merge, or vanish.

#ifndef ANVIL_CORE_SHARD_PLACEMENT_H_
#define ANVIL_CORE_SHARD_PLACEMENT_H_

#include <cstdint>
#include <string>
#include <vector>

#include "anvil/core/shard/topology.h"
#include "anvil/core/types.h"

namespace anvil::shard {

struct PlacementOptions {
  // Splits and merges. The hysteresis between them is deliberate and has to be
  // wide: a merge threshold anywhere near half the split threshold makes the
  // pair oscillate, and a range that splits and merges forever looks exactly
  // like a working rebalancer until you count the topology changes.
  std::uint64_t split_threshold_keys = 24;
  std::uint64_t merge_threshold_keys = 6;

  std::uint32_t target_replicas = 3;

  // How many placement entries must pass before a range that has just changed
  // may be split or merged again. Entries and not seconds, deliberately: the
  // descriptor's timestamp came from whichever node proposed the change, and
  // this comparison happens on whichever node is currently the placement
  // leader. Two clocks, one subtraction, and a cooldown that is either infinite
  // or zero depending on which way the skew points (ANV-0045).
  //
  // It is also the only thing standing between a split threshold and a merge
  // threshold that overlap -- which the test profile does on purpose, because
  // the point of a chaos-admin workload is to run the machinery, not to be a
  // good autoscaler.
  std::uint64_t change_cooldown_entries = 24;

  // A node is dead when nothing has been heard from it for this long. Nothing
  // acts on that until `replacement_grace` has passed as well, because the
  // common cause of silence is a leader change or a partition that heals, and
  // replacing three replicas every time the network hiccups is a rebalancing
  // storm rather than a repair.
  Duration node_dead_after = Duration::seconds(3);
  Duration replacement_grace = Duration::seconds(2);

  // A merge frozen for this many placement entries without finishing is
  // abandoned. The coordinator is the lease holder and it can die between the
  // freeze and the absorb, leaving a range that answers every request with a
  // rejection until somebody unfreezes it. Counted in entries for the same
  // reason as the cooldown, and generously: abandoning a merge that was about
  // to succeed costs a full round of freeze, absorb and finish to redo, and
  // doing that in a loop is a cluster that looks busy and never finishes
  // anything.
  std::uint64_t merge_timeout_entries = 90;

  // How many commands one decision round may emit. Bounded so that a topology
  // that has drifted badly repairs itself over several rounds instead of
  // proposing forty conflicting changes into one Raft log.
  std::uint32_t max_commands = 2;

  // ---- deliberate-bug knobs (all default to correct) ----------------------

  // false: a replica is added straight to the voter set without spending time
  // as a learner, so a voter that holds none of the range's committed prefix is
  // counted toward its quorum. Nothing breaks immediately -- Raft will not
  // commit on a replica whose match is behind -- and what is lost is durability:
  // the range's data is now on fewer nodes than its replication factor claims.
  // INV-SHARD-06.
  bool promote_only_caught_up = true;

  // false: the driver merges any two adjacent small ranges, without requiring
  // them to be on the same replicas with the same lease holder. INV-SHARD-03.
  bool merge_requires_colocation = true;
};

// A decision, with the reason attached. The reason is not decoration: a
// topology that keeps changing is the normal state of this system, and telling
// a rebalance apart from a repair apart from an oscillation after the fact is
// impossible without it.
struct Decision {
  AdminCommand command;
  std::string reason;
};

std::vector<Decision> decide(const TopologyState& state, const PlacementOptions& options,
                             Timestamp now, std::uint32_t cluster_size);

// Nodes considered live at `now`, by the replicated heartbeat record alone.
std::vector<NodeId> live_nodes(const TopologyState& state, const PlacementOptions& options,
                               Timestamp now, std::uint32_t cluster_size);

}  // namespace anvil::shard

#endif  // ANVIL_CORE_SHARD_PLACEMENT_H_
