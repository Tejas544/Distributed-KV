// The range topology, and the state machine that owns it.
//
// Every range descriptor in the cluster lives here, in the placement driver's
// own Raft group. That group is group 1, it is replicated on every node, and it
// is the single authority on which ranges exist -- so "the topology changed" is
// one committed log entry, applied identically everywhere, rather than a message
// that some nodes got and others did not.
//
// One entry is one apply, and that is where the atomicity of a split comes from.
// A split is not "shorten the left range, then create the right one": it is one
// command whose apply does both, so no observer of this state ever sees the key
// space covered twice or not at all. Writing it as two commands is a one-line
// change and it is the first deliberate bug in the drill, because the window it
// opens is a few microseconds wide and completely invisible from the client.
//
// What this state machine is NOT is the data. It knows a range exists, where it
// lives and who holds its lease; it does not know a single key's value. The data
// is in the range's own group (range.h), and the descriptor is replicated into
// *both* -- here as the routing authority, and there as the thing an incoming
// request's generation is checked against. Two copies of one fact is a smell,
// and the alternative is worse: a range that cannot validate a request without a
// round trip to the placement driver, on every request. INV-SHARD-07 is the
// price of that decision, and it is stated as a bounded lag rather than as
// equality, because a lag is what the design actually permits.

#ifndef ANVIL_CORE_SHARD_TOPOLOGY_H_
#define ANVIL_CORE_SHARD_TOPOLOGY_H_

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "anvil/core/raft/driver.h"
#include "anvil/core/shard/descriptor.h"
#include "anvil/core/types.h"

namespace anvil::shard {

enum class AdminOp : std::uint8_t {
  kBootstrap = 0,   // create the one range that covers everything
  kSplit,           // range -> [start, key) + [key, end)
  kBeginMerge,      // freeze the right-hand range; records the intent
  kFinishMerge,     // the left range absorbs the right; the right is removed
  kAbortMerge,      // unfreeze
  kChangeReplicas,  // one step of a rebalance
  kGrantLease,      // publish the lease the range's own log has already granted
  kNodeHeartbeat,   // liveness, replicated so decisions are not local opinions
  kReportSize,      // the lease holder's key count and median key, likewise
  kReportCatchup,   // a learner now holds the range's committed prefix
  kSetDescriptor,   // the range's own view, echoed back after a trigger applied
};

const char* to_string(AdminOp op) noexcept;

struct AdminCommand {
  AdminOp op = AdminOp::kBootstrap;
  RangeId range{};
  RangeId other{};   // merge partner
  std::string key;   // split point
  // The generation the proposer believed the range had. Zero means "any".
  // A decision made against a descriptor that has since changed is a decision
  // made about a range that no longer exists in that shape, and applying it
  // anyway is how a split lands at a key the range no longer owns.
  std::uint64_t generation = 0;
  std::vector<NodeId> replicas;
  std::vector<NodeId> learners;
  NodeId node{};
  std::uint64_t time = 0;
  std::uint64_t value = 0;  // reported size, or the lease expiry

  friend bool operator==(const AdminCommand&, const AdminCommand&) noexcept = default;
  std::string describe() const;
};

std::string encode_admin(const AdminCommand& cmd);
bool decode_admin(std::string_view in, AdminCommand* out);

struct NodeHealth {
  NodeId id{};
  std::uint64_t last_seen = 0;  // on the placement driver leader's clock
};

// What the lease holder tells the placement driver about its range.
//
// All of it is replicated, and that is the point rather than an implementation
// detail. A placement driver that splits a range because *its own node* thinks
// the range is large is making a decision no other replica can reproduce, and
// the first thing that happens after a leader change is that the new leader
// decides something different from the same log. INV-SHARD-09 exists to catch
// exactly that, and it can only be stated if every input is in here.
struct RangeStats {
  std::uint64_t keys = 0;
  std::string median;  // the split point the lease holder suggests
  // Learners known to hold the range's committed prefix. A voter may only be
  // added from this set: promoting a replica that holds nothing is what
  // INV-SHARD-06 is about.
  std::vector<NodeId> caught_up;
  std::uint64_t reported_at = 0;
};

// Every deliberate bug this layer can be given, one flag each, all defaulting
// to correct. Same discipline as RaftOptions and MvccOptions: a test that flips
// one is planting a bug, and a suite that cannot tell the difference is
// evidence of nothing (ANV-0006).
struct TopologyOptions {
  std::uint32_t meta_records_per_bucket = 4;

  // false: a split installs the right-hand descriptor and shortens the left one
  // in the *next* apply. Both ranges then claim the upper half for a window --
  // the key space is covered twice, two leases can be granted over the same
  // keys, and a client can be routed to either. INV-SHARD-01.
  bool split_is_atomic = true;

  // false: a descriptor changes without its generation moving, so every cached
  // copy in the cluster stays valid forever and a stale route is served rather
  // than rejected. INV-SHARD-05.
  bool bumps_generation = true;

  // false: a merge proceeds without requiring the two ranges to be on the same
  // replicas with the same lease holder. The survivor then absorbs a range whose
  // data some of its replicas have never seen. INV-SHARD-03.
  bool merge_requires_colocation = true;

  // false: a replica change is applied without checking that the old and new
  // voter sets still share a quorum, which is what makes a two-step rebalance
  // safe. INV-SHARD-06.
  bool rebalance_keeps_quorum = true;
};

struct TopologyState {
  std::map<std::string, RangeDescriptor> ranges;  // keyed by start key
  std::map<std::uint64_t, std::string> by_id;     // range id -> start key
  std::map<std::uint64_t, NodeHealth> nodes;
  std::map<std::uint64_t, RangeStats> stats;      // range id -> what its leader reports
  std::uint64_t next_range_id = 2;                // 1 is the placement group

  // The index of the last entry applied. Replicated by construction, so a
  // decision expressed as "N entries since" is the same decision on every
  // replica, which "N seconds since" is not: the seconds come from whichever
  // clock happened to stamp the descriptor.
  std::uint64_t applied = 0;
  MetaIndex meta;

  // The left-hand shortening a non-atomic split has not done yet. Exists only
  // so the deliberate bug can be a real two-step apply rather than a permanent
  // corruption.
  std::optional<AdminCommand> deferred;

  const RangeDescriptor* find(RangeId id) const;
  RangeDescriptor* mutable_find(RangeId id);
  const RangeDescriptor* find_by_key(std::string_view key) const;

  // The right-hand neighbour, or null at the end of the key space.
  const RangeDescriptor* right_neighbour(RangeId id) const;

  // INV-SHARD-01, as a function: the ranges must tile [-inf, +inf) exactly once.
  // Returns a description of the first gap or overlap.
  std::optional<std::string> coverage_violation() const;

  std::uint64_t total_size() const;
};

class TopologyMachine : public raft::StateMachine {
 public:
  explicit TopologyMachine(TopologyOptions options) : options_(options) {}

  void apply(LogIndex index, std::string_view command) override;
  std::string snapshot() const override;
  void restore(std::string_view data) override;

  const TopologyState& state() const noexcept { return state_; }
  LogIndex applied_index() const noexcept { return applied_; }
  const TopologyOptions& options() const noexcept { return options_; }

  // Bumped on every change, so a checker can skip a machine that cannot have
  // moved. The same trick RaftNode::revision() plays, for the same reason.
  std::uint64_t revision() const noexcept { return revision_; }

  // Commands that were applied and had no effect because their precondition no
  // longer held. Not an error -- a placement decision racing a merge is normal
  // -- but a number, because "the topology stopped changing" has two very
  // different causes and no way to tell them apart without one.
  std::uint64_t rejected() const noexcept { return rejected_; }
  std::uint64_t applied_commands() const noexcept { return applied_commands_; }
  const std::string& last_reject() const noexcept { return last_reject_; }

 private:
  void apply_one(const AdminCommand& cmd);
  void insert(const RangeDescriptor& desc);
  void erase(RangeId id);
  void bump(RangeDescriptor* desc, std::uint64_t at);
  void rebuild_meta();

  TopologyOptions options_;
  TopologyState state_;
  LogIndex applied_{};
  std::uint64_t revision_ = 0;
  std::uint64_t rejected_ = 0;
  std::uint64_t applied_commands_ = 0;
  std::string last_reject_;
};

}  // namespace anvil::shard

#endif  // ANVIL_CORE_SHARD_TOPOLOGY_H_
