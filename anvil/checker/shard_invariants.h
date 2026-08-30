// The god's-eye view of a sharded cluster.
//
// The sharding layer is where this project's central claim is easiest to
// demonstrate, because almost everything that can go wrong here is invisible at
// the client API for as long as anyone is likely to look. A range descriptor
// gap that closes before a client routes through it. Two ranges that both claim
// the same span for four hundred microseconds. A lease held by two nodes whose
// clocks disagree by more than the bound they declared. A replica promoted to
// voter before it holds any of the range's data, so the range quietly has one
// fewer copy than it says. A quiesced range whose follower is behind and will
// now never catch up, because being quiesced is precisely the state of not
// being sent anything.
//
// None of those return a wrong answer to anybody. Every one of them is a bug,
// and every one of them is one unlucky crash away from being data loss.
//
// The observer holds a pointer to each node's ShardStore and reads it. No hooks
// in the sharding layer, for the same reason RaftObserver has none in the
// protocol: the thing being checked has to be the thing that ships.

#ifndef ANVIL_CHECKER_SHARD_INVARIANTS_H_
#define ANVIL_CHECKER_SHARD_INVARIANTS_H_

#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "anvil/checker/invariant.h"
#include "anvil/core/shard/store.h"
#include "anvil/core/types.h"

namespace anvil::checker {

class ShardObserver {
 public:
  struct Hooks {
    std::function<std::uint64_t()> tick;
    std::function<Timestamp()> true_now;
    std::function<bool(NodeId)> alive;
    std::function<Timestamp(NodeId)> node_now;
  };

  void configure(std::uint32_t nodes, Hooks hooks);

  // Re-registered on every boot: a crash destroys the store and the next one is
  // a different object.
  void set_store(NodeId id, const shard::ShardStore* store);

  // How far apart two nodes' clocks are allowed to be. INV-SHARD-04 is stated
  // with this in it rather than as a bare "at most one lease", because a lease
  // is an optimisation licensed by a clock bound and the honest question is
  // whether it holds *within the bound the configuration declared*.
  void set_clock_uncertainty(Duration bound) noexcept { uncertainty_ = bound; }

  // How long a range's own descriptor may lag the topology's before that
  // counts as an inconsistency rather than as replication in progress.
  void set_meta_staleness_bound(Duration bound) noexcept { staleness_ = bound; }

  void refresh();
  std::optional<std::string> take(const std::string& id);

  Duration clock_uncertainty() const noexcept { return uncertainty_; }
  void note_lease_overlap() noexcept { ++counters_.lease_overlaps; }
  void note_lease_overlap_out_of_bound(std::uint64_t error) noexcept {
    ++counters_.lease_overlaps_out_of_bound;
    counters_.worst_clock_error_nanos =
        error > counters_.worst_clock_error_nanos ? error : counters_.worst_clock_error_nanos;
  }

  std::vector<NodeId> live_nodes() const;
  const shard::ShardStore* store(NodeId id) const;

  // That node's *opinion* of the time, which is what its lease is expressed in
  // and the only clock it can judge its own lease against.
  Timestamp node_now(NodeId id) const;
  Timestamp true_now() const;
  std::uint32_t node_count() const noexcept { return nodes_; }

  struct Counters {
    std::uint64_t scans = 0;
    std::uint64_t descriptor_changes = 0;
    std::uint64_t splits_seen = 0;
    std::uint64_t merges_seen = 0;
    std::uint64_t replica_changes = 0;
    std::uint64_t lease_changes = 0;
    std::uint64_t quiesced_ranges_seen = 0;
    std::uint64_t placement_comparisons = 0;
    std::uint64_t coverage_checks = 0;
    std::uint64_t ranges_high_water = 0;

    // How often the node the observer reads the topology from changed. Every
    // change re-seeds the mirrors without reporting, so this is the size of the
    // observer's own blind spot -- a transition that happened exactly across a
    // source change is not checked. Counted rather than hidden, because an
    // unattributable observation is a blind spot, not a finding (CONTEXT.md
    // 10.20).
    std::uint64_t source_changes = 0;

    // Lease overlaps that happened while some holder's clock was further from
    // true time than the configuration declared possible. Not failures: a lease
    // is an optimisation licensed by a clock bound, and when the environment
    // breaks the bound the licence is void. Counted, named and reported --
    // never silently dropped, and never counted as a pass either.
    std::uint64_t lease_overlaps = 0;
    std::uint64_t lease_overlaps_out_of_bound = 0;
    std::uint64_t worst_clock_error_nanos = 0;
  };
  const Counters& counters() const noexcept { return counters_; }

  // The union, over all history, of every range id ever seen and its last known
  // span. A merge removes a descriptor; the checker still needs it to explain
  // what happened afterwards.
  const std::map<std::uint64_t, shard::RangeDescriptor>& history() const noexcept {
    return history_;
  }

 private:
  void scan_topology(NodeId id);
  void check_lease_sequence();
  void record(const std::string& id, std::string detail);

  // What we last saw of a range, so a transition can be checked at the moment
  // it happens rather than reconstructed later.
  struct RangeMirror {
    shard::RangeDescriptor descriptor;
    bool seen = false;
    std::uint64_t max_generation = 0;
    std::vector<NodeId> caught_up;
  };

  std::uint32_t nodes_ = 0;
  Hooks hooks_;
  Duration uncertainty_ = Duration::millis(10);
  Duration staleness_ = Duration::seconds(3);

  std::map<std::uint64_t, const shard::ShardStore*> stores_;
  std::map<std::uint64_t, RangeMirror> mirrors_;
  std::map<std::uint64_t, shard::RangeDescriptor> history_;

  // When a range's local descriptor was first seen to disagree with the
  // topology's. Cleared when they agree again.
  std::map<std::uint64_t, std::uint64_t> disagreed_since_;

  // The last lease observed for each range, and which node it was read from.
  // One source per range: two replicas are at different points in the same
  // sequence, and reading alternately from both makes a legal handover look
  // like a lease going backwards.
  struct LeaseMirror {
    std::uint64_t source = 0;
    shard::Lease lease;
  };
  std::map<std::uint64_t, LeaseMirror> leases_;

  std::map<std::string, std::vector<std::string>> pending_;
  std::uint64_t last_tick_ = UINT64_MAX;
  // Which node the mirrors were built from. Transitions are only meaningful
  // within one replica's own applied sequence: two nodes are at different
  // points in the same history, so reading the topology from a different one
  // than last tick shows generations moving backwards and descriptors changing
  // shape, neither of which happened.
  std::uint64_t source_ = 0;
  Counters counters_;
};

// Arms INV-SHARD-01..09.
void arm_shard_invariants(InvariantRegistry& registry, ShardObserver* observer);

}  // namespace anvil::checker

#endif  // ANVIL_CHECKER_SHARD_INVARIANTS_H_
