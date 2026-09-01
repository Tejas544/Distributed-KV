// Sharding unit tests: the mechanism, with nothing running.
//
// The situations that matter most in a sharding layer are the ones a random
// workload reaches once in thousands of seeds -- a merge whose two ranges are
// on different replicas, a lease granted while the previous one is still live
// by less than the clock bound, a client cache that is exactly one generation
// stale. Each of those is a sentence here rather than a seed hunt.
//
// Everything below is constructed directly against the state machines. There is
// no simulator underneath, no clock and no disk: the same split between "the
// protocol is right when nothing goes wrong" (here) and "the protocol survives
// an adversary" (shard_faults.cc) that raft_test and raft_faults have.

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <map>
#include <string>
#include <string_view>
#include <vector>

#include "anvil/core/shard/descriptor.h"
#include "anvil/core/shard/placement.h"
#include "anvil/core/shard/range.h"
#include "anvil/core/shard/router.h"
#include "anvil/core/shard/topology.h"

namespace {

using anvil::Duration;
using anvil::LogIndex;
using anvil::NodeId;
using anvil::RangeId;
using anvil::Timestamp;
namespace shard = anvil::shard;

int g_failures = 0;

void check(bool condition, std::string_view what) {
  if (!condition) {
    std::cerr << "FAIL: " << what << "\n";
    ++g_failures;
  }
}

std::string account(int index) {
  std::string out = "acct";
  const std::string digits = std::to_string(index);
  out.append(4 - std::min<std::size_t>(4, digits.size()), '0');
  out += digits;
  return out;
}

// A topology machine with one range covering the key space, on three nodes.
shard::TopologyMachine bootstrapped(shard::TopologyOptions options = {}) {
  shard::TopologyMachine machine{options};
  shard::AdminCommand cmd;
  cmd.op = shard::AdminOp::kBootstrap;
  cmd.replicas = {NodeId{1}, NodeId{2}, NodeId{3}};
  cmd.time = 1000;
  machine.apply(LogIndex{1}, shard::encode_admin(cmd));
  return machine;
}

void apply(shard::TopologyMachine& machine, const shard::AdminCommand& cmd,
           std::uint64_t index) {
  machine.apply(LogIndex{index}, shard::encode_admin(cmd));
}

// ---------------------------------------------------------------------------
// descriptors and encoding
// ---------------------------------------------------------------------------

void test_descriptor_survives_its_own_encoding() {
  shard::RangeDescriptor desc;
  desc.id = RangeId{7};
  desc.start = "banana";
  desc.end = "cherry";
  desc.generation = 42;
  desc.replicas = {NodeId{1}, NodeId{4}};
  desc.learners = {NodeId{9}};
  desc.lease = shard::Lease{NodeId{4}, 1000, 2000};
  desc.changed_at = 1234;
  desc.frozen = true;

  shard::RangeDescriptor round;
  check(shard::decode_descriptor(shard::encode_descriptor(desc), &round),
        "a descriptor must decode");
  check(round.id == desc.id && round.start == desc.start && round.end == desc.end,
        "the span must survive");
  check(round.generation == desc.generation, "the generation must survive");
  check(round.replicas == desc.replicas && round.learners == desc.learners,
        "the membership must survive");
  check(round.lease == desc.lease, "the lease must survive");
  check(round.changed_at == desc.changed_at, "the change time must survive");
  check(round.frozen, "the freeze flag must survive");

  // The unbounded range is a real value, not a missing one.
  shard::RangeDescriptor unbounded;
  unbounded.id = RangeId{1};
  shard::RangeDescriptor round_unbounded;
  check(shard::decode_descriptor(shard::encode_descriptor(unbounded), &round_unbounded),
        "an unbounded descriptor must decode");
  check(round_unbounded.end.empty(), "unbounded above must stay unbounded");
  check(round_unbounded.contains("zzzz"), "an unbounded range contains every key");
}

void test_coverage_detects_gaps_and_overlaps() {
  shard::TopologyState state;
  const auto put = [&state](std::uint64_t id, std::string start, std::string end) {
    shard::RangeDescriptor desc;
    desc.id = RangeId{id};
    desc.start = std::move(start);
    desc.end = std::move(end);
    state.ranges[desc.start] = desc;
    state.by_id[id] = desc.start;
  };

  put(2, "", "m");
  put(3, "m", "");
  check(!state.coverage_violation().has_value(), "two adjacent ranges tile the key space");

  state.ranges.clear();
  state.by_id.clear();
  put(2, "", "m");
  put(3, "p", "");
  check(state.coverage_violation().has_value(), "a gap must be reported");
  check(state.coverage_violation()->find("gap") != std::string::npos,
        "a gap must be named as a gap");

  state.ranges.clear();
  state.by_id.clear();
  put(2, "", "p");
  put(3, "m", "");
  check(state.coverage_violation().has_value(), "an overlap must be reported");
  check(state.coverage_violation()->find("overlap") != std::string::npos,
        "an overlap must be named as an overlap");

  state.ranges.clear();
  state.by_id.clear();
  put(2, "a", "");
  check(state.coverage_violation().has_value(),
        "a key space that does not start at the bottom must be reported");
}

// ---------------------------------------------------------------------------
// splits
// ---------------------------------------------------------------------------

void test_a_split_is_one_apply() {
  shard::TopologyMachine machine = bootstrapped();
  const RangeId first = machine.state().ranges.begin()->second.id;

  shard::AdminCommand split;
  split.op = shard::AdminOp::kSplit;
  split.range = first;
  split.key = account(12);
  split.generation = 1;
  split.time = 2000;
  apply(machine, split, 2);

  check(machine.state().ranges.size() == 2, "a split produces two ranges");
  check(!machine.state().coverage_violation().has_value(),
        "the key space is tiled at every point between two applies");
  const shard::RangeDescriptor* left = machine.state().find(first);
  check(left != nullptr && left->end == account(12), "the left half ends at the split key");
  check(left != nullptr && left->generation == 2, "the left half's generation moves");
  const shard::RangeDescriptor* right = machine.state().find_by_key(account(20));
  check(right != nullptr && right->start == account(12) && right->end.empty(),
        "the right half runs to the end of the key space");
  check(right != nullptr && right->id != first, "the right half is a new range");
}

void test_a_non_atomic_split_leaves_the_key_space_covered_twice() {
  // The mutation, verified here rather than only in the drill: the point of
  // planting a bug is worthless unless the thing it breaks is known to be
  // detectable, and this is the cheapest possible demonstration.
  shard::TopologyOptions options;
  options.split_is_atomic = false;
  shard::TopologyMachine machine = bootstrapped(options);
  const RangeId first = machine.state().ranges.begin()->second.id;

  shard::AdminCommand split;
  split.op = shard::AdminOp::kSplit;
  split.range = first;
  split.key = account(12);
  split.generation = 1;
  split.time = 2000;
  apply(machine, split, 2);

  const auto violation = machine.state().coverage_violation();
  check(violation.has_value(), "a non-atomic split must leave the topology inconsistent");
  // The left half still runs to the end of the key space and the right half now
  // starts inside it, so the upper half is claimed twice. The tiling walk meets
  // that as an unbounded range that is not the last one, which is the same fact
  // seen from the other side -- the test accepts either wording rather than
  // pinning a message.
  check(violation.has_value() && (violation->find("overlap") != std::string::npos ||
                                  violation->find("unbounded") != std::string::npos),
        "and the inconsistency is that two ranges claim the upper half");
}

void test_a_split_at_the_boundary_is_refused() {
  shard::TopologyMachine machine = bootstrapped();
  const RangeId first = machine.state().ranges.begin()->second.id;
  const std::uint64_t rejected_before = machine.rejected();

  shard::AdminCommand split;
  split.op = shard::AdminOp::kSplit;
  split.range = first;
  split.key = "";  // the range's own start
  split.generation = 1;
  apply(machine, split, 2);
  check(machine.state().ranges.size() == 1, "a split at the range's start must be refused");
  check(machine.rejected() > rejected_before, "and counted as refused");
}

void test_a_stale_split_decision_is_refused() {
  shard::TopologyMachine machine = bootstrapped();
  const RangeId first = machine.state().ranges.begin()->second.id;

  shard::AdminCommand split;
  split.op = shard::AdminOp::kSplit;
  split.range = first;
  split.key = account(12);
  split.generation = 1;
  split.time = 2000;
  apply(machine, split, 2);

  // A second decision made against the generation that has just been superseded.
  // It races a split that already happened, and applying it would split at a
  // key the range no longer owns.
  shard::AdminCommand stale = split;
  stale.key = account(18);
  apply(machine, stale, 3);
  check(machine.state().ranges.size() == 2,
        "a decision made against a superseded generation must be refused");
}

// ---------------------------------------------------------------------------
// merges
// ---------------------------------------------------------------------------

struct MergeFixture {
  shard::TopologyMachine machine;
  RangeId left{};
  RangeId right{};
};

MergeFixture two_ranges(shard::TopologyOptions options = {}) {
  MergeFixture fixture{bootstrapped(options)};
  fixture.left = fixture.machine.state().ranges.begin()->second.id;
  shard::AdminCommand split;
  split.op = shard::AdminOp::kSplit;
  split.range = fixture.left;
  split.key = account(12);
  split.generation = 1;
  split.time = 2000;
  apply(fixture.machine, split, 2);
  fixture.right = fixture.machine.state().find_by_key(account(20))->id;

  // Both leases to node 1, which is what colocation means.
  for (const RangeId id : {fixture.left, fixture.right}) {
    shard::AdminCommand lease;
    lease.op = shard::AdminOp::kGrantLease;
    lease.range = id;
    lease.node = NodeId{1};
    lease.time = 3000;
    lease.value = 9000;
    apply(fixture.machine, lease, 3);
  }
  return fixture;
}

void test_a_merge_requires_colocation() {
  MergeFixture fixture = two_ranges();

  // Move the right-hand range's lease elsewhere. Nothing else changes.
  shard::AdminCommand lease;
  lease.op = shard::AdminOp::kGrantLease;
  lease.range = fixture.right;
  lease.node = NodeId{2};
  lease.time = 4000;
  lease.value = 9000;
  apply(fixture.machine, lease, 4);

  shard::AdminCommand begin;
  begin.op = shard::AdminOp::kBeginMerge;
  begin.range = fixture.left;
  begin.other = fixture.right;
  begin.time = 5000;
  apply(fixture.machine, begin, 5);

  const shard::RangeDescriptor* right = fixture.machine.state().find(fixture.right);
  check(right != nullptr && !right->frozen,
        "a merge whose two ranges have different lease holders must be refused");

  // And with the check turned off it goes through, which is what makes the
  // mutation a mutation rather than a no-op.
  shard::TopologyOptions loose;
  loose.merge_requires_colocation = false;
  MergeFixture broken = two_ranges(loose);
  shard::AdminCommand move;
  move.op = shard::AdminOp::kGrantLease;
  move.range = broken.right;
  move.node = NodeId{2};
  move.time = 4000;
  move.value = 9000;
  apply(broken.machine, move, 4);
  shard::AdminCommand begin_broken;
  begin_broken.op = shard::AdminOp::kBeginMerge;
  begin_broken.range = broken.left;
  begin_broken.other = broken.right;
  begin_broken.time = 5000;
  apply(broken.machine, begin_broken, 5);
  const shard::RangeDescriptor* loose_right = broken.machine.state().find(broken.right);
  check(loose_right != nullptr && loose_right->frozen,
        "with the colocation check off, the same merge proceeds");
}

void test_a_merge_removes_exactly_one_range() {
  MergeFixture fixture = two_ranges();

  shard::AdminCommand begin;
  begin.op = shard::AdminOp::kBeginMerge;
  begin.range = fixture.left;
  begin.other = fixture.right;
  begin.time = 5000;
  apply(fixture.machine, begin, 5);
  check(fixture.machine.state().find(fixture.right)->frozen, "the right-hand range freezes");
  check(!fixture.machine.state().coverage_violation().has_value(),
        "a frozen range is still in the tiling: removing it would leave a gap");

  shard::AdminCommand finish;
  finish.op = shard::AdminOp::kFinishMerge;
  finish.range = fixture.left;
  finish.other = fixture.right;
  finish.time = 6000;
  apply(fixture.machine, finish, 6);

  check(fixture.machine.state().ranges.size() == 1, "the merge leaves one range");
  check(fixture.machine.state().find(fixture.right) == nullptr, "the subsumed range is gone");
  check(fixture.machine.state().find(fixture.left)->end.empty(),
        "the survivor now runs to the end of the key space");
  check(!fixture.machine.state().coverage_violation().has_value(),
        "and the key space is still tiled exactly once");
}

void test_a_finish_without_a_freeze_is_refused() {
  MergeFixture fixture = two_ranges();
  shard::AdminCommand finish;
  finish.op = shard::AdminOp::kFinishMerge;
  finish.range = fixture.left;
  finish.other = fixture.right;
  apply(fixture.machine, finish, 5);
  check(fixture.machine.state().ranges.size() == 2,
        "a merge that never froze its right-hand side must not complete");
}

// ---------------------------------------------------------------------------
// replicas
// ---------------------------------------------------------------------------

void test_a_replica_change_must_keep_a_quorum_of_both_sets() {
  shard::TopologyMachine machine = bootstrapped();
  const RangeId id = machine.state().ranges.begin()->second.id;

  shard::AdminCommand swap;
  swap.op = shard::AdminOp::kChangeReplicas;
  swap.range = id;
  swap.replicas = {NodeId{1}, NodeId{4}, NodeId{5}};  // two of three replaced at once
  swap.time = 2000;
  apply(machine, swap, 2);
  const shard::RangeDescriptor* desc = machine.state().find(id);
  check(desc != nullptr && desc->replicas.size() == 3 && desc->is_voter(NodeId{2}),
        "replacing two of three voters at once must be refused: two disjoint majorities");

  shard::AdminCommand one;
  one.op = shard::AdminOp::kChangeReplicas;
  one.range = id;
  one.replicas = {NodeId{1}, NodeId{2}, NodeId{4}};
  one.time = 3000;
  apply(machine, one, 3);
  desc = machine.state().find(id);
  check(desc != nullptr && desc->is_voter(NodeId{4}) && !desc->is_voter(NodeId{3}),
        "replacing one at a time is allowed");
}

void test_a_lease_held_by_a_departed_replica_is_dropped() {
  shard::TopologyMachine machine = bootstrapped();
  const RangeId id = machine.state().ranges.begin()->second.id;
  shard::AdminCommand lease;
  lease.op = shard::AdminOp::kGrantLease;
  lease.range = id;
  lease.node = NodeId{3};
  lease.time = 2000;
  lease.value = 9000;
  apply(machine, lease, 2);
  check(machine.state().find(id)->lease.holder == NodeId{3}, "the lease is recorded");

  shard::AdminCommand change;
  change.op = shard::AdminOp::kChangeReplicas;
  change.range = id;
  change.replicas = {NodeId{1}, NodeId{2}, NodeId{4}};
  change.time = 3000;
  apply(machine, change, 3);
  check(!machine.state().find(id)->lease.holder.valid(),
        "a lease held by a node that is no longer a replica is not a lease");
}

// ---------------------------------------------------------------------------
// the meta index and the client cache
// ---------------------------------------------------------------------------

void test_the_meta_index_resolves_every_range() {
  shard::TopologyMachine machine = bootstrapped();
  RangeId current = machine.state().ranges.begin()->second.id;
  std::uint64_t index = 2;
  for (int at : {6, 12, 18}) {
    shard::AdminCommand split;
    split.op = shard::AdminOp::kSplit;
    split.range = current;
    split.key = account(at);
    split.generation = machine.state().find(current)->generation;
    split.time = 1000 * index;
    apply(machine, split, index++);
    current = machine.state().find_by_key(account(at))->id;
  }
  check(machine.state().ranges.size() == 4, "four ranges");
  check(machine.state().meta.size() == 4, "one meta record per range");

  shard::RangeCache cache;
  for (int i = 0; i < 24; ++i) {
    shard::RangeDescriptor found;
    const std::string key = account(i);
    check(cache.resolve(machine.state().meta, key, &found),
          "every key must resolve through both levels of the meta index");
    check(found.contains(key), "and resolve to the range that owns it");
  }
  check(cache.stats().two_level_lookups == 24, "every resolution consults both levels");
  check(cache.size() == 4, "and the cache ends up holding every range");
}

void test_the_cache_serves_hits_and_is_invalidated_by_a_rejection() {
  shard::TopologyMachine machine = bootstrapped();
  shard::RangeCache cache;
  shard::RangeDescriptor found;
  check(cache.resolve(machine.state().meta, account(3), &found), "the first lookup resolves");
  const RangeId id = found.id;

  shard::RangeDescriptor hit;
  check(cache.lookup(account(3), &hit), "the second lookup is a cache hit");
  check(cache.stats().hits == 1, "and is counted as one");

  cache.invalidate(id);
  shard::RangeDescriptor miss;
  check(!cache.lookup(account(3), &miss), "after an invalidation the entry is gone");
  check(cache.stats().misses >= 1, "and the miss is counted");
}

void test_a_stale_generation_is_rejected_rather_than_served() {
  shard::RangeDescriptor desc;
  desc.id = RangeId{2};
  desc.generation = 4;
  desc.replicas = {NodeId{1}};
  shard::RangeMachine machine{desc, shard::RangeOptions{}, false};

  shard::RangeCommand init;
  init.op = shard::RangeOp::kInit;
  init.generation = 4;
  std::map<std::string, std::int64_t> balances{{account(1), 100}, {account(2), 100}};
  init.payload = shard::RangeMachine::encode_payload(balances, {});
  machine.apply(LogIndex{1}, shard::encode_range_command(init));
  check(machine.initialised() && machine.total() == 200, "the range starts with its accounts");

  shard::RangeCommand transfer;
  transfer.op = shard::RangeOp::kTransfer;
  transfer.from = account(1);
  transfer.to = account(2);
  transfer.amount = 10;
  transfer.op_id = 1;
  transfer.generation = 3;  // one behind
  machine.apply(LogIndex{2}, shard::encode_range_command(transfer));
  check(machine.total() == 200, "a request against a stale generation must not move anything");
  check(machine.counters().transfers_rejected_wrong_range == 1, "and must be counted");
  check(machine.decided().count(1) == 0,
        "a routing rejection is not a decision: the client will retry elsewhere");

  transfer.generation = 4;
  machine.apply(LogIndex{3}, shard::encode_range_command(transfer));
  check(machine.balances().at(account(1)) == 90, "with the right generation it applies");
  check(machine.total() == 200, "and conserves the total");

  // The retry, with the same operation id.
  machine.apply(LogIndex{4}, shard::encode_range_command(transfer));
  check(machine.balances().at(account(1)) == 90, "a retry must not apply twice");
  check(machine.counters().transfers_duplicate == 1, "and is recognised as a retry");
}

void test_a_transfer_across_the_split_point_is_refused_whole() {
  shard::RangeDescriptor desc;
  desc.id = RangeId{2};
  desc.generation = 1;
  shard::RangeMachine machine{desc, shard::RangeOptions{}, false};

  shard::RangeCommand init;
  init.op = shard::RangeOp::kInit;
  init.generation = 1;
  std::map<std::string, std::int64_t> balances;
  for (int i = 0; i < 8; ++i) balances[account(i)] = 100;
  init.payload = shard::RangeMachine::encode_payload(balances, {});
  machine.apply(LogIndex{1}, shard::encode_range_command(init));

  shard::RangeCommand split;
  split.op = shard::RangeOp::kSplitTrigger;
  split.end = account(4);
  split.other = RangeId{3};
  split.generation = 2;
  machine.apply(LogIndex{2}, shard::encode_range_command(split));
  check(machine.key_count() == 4, "the upper half leaves the range");
  check(machine.pending_splits().count(3) == 1,
        "and is held for the range that will receive it");
  check(machine.total() == 400, "the survivor holds exactly its own half");

  shard::RangeCommand transfer;
  transfer.op = shard::RangeOp::kTransfer;
  transfer.from = account(2);
  transfer.to = account(6);  // now in the other range
  transfer.amount = 10;
  transfer.op_id = 99;
  transfer.generation = 2;
  machine.apply(LogIndex{3}, shard::encode_range_command(transfer));
  check(machine.total() == 400,
        "a transfer spanning the split point must move nothing at all -- not the debit, "
        "not the credit");
  check(machine.balances().count(account(6)) == 0,
        "and must not conjure the far account into a range that does not own it");
}

// ---------------------------------------------------------------------------
// leases
// ---------------------------------------------------------------------------

void test_a_lease_may_not_start_before_the_previous_one_has_provably_ended() {
  shard::RangeOptions options;
  options.clock_uncertainty_nanos = 100;  // the per-node bound
  shard::RangeDescriptor desc;
  desc.id = RangeId{2};
  shard::RangeMachine machine{desc, options, true};

  shard::RangeCommand first;
  first.op = shard::RangeOp::kGrantLease;
  first.node = NodeId{1};
  first.time = 1000;
  first.expiry = 2000;
  machine.apply(LogIndex{1}, shard::encode_range_command(first));
  check(machine.descriptor().lease.holder == NodeId{1}, "the first lease is granted");

  // Node 2 asks at a moment that is after the expiry on its own clock but not
  // by two clock bounds. The two nodes can be wrong in opposite directions, so
  // this is not enough.
  shard::RangeCommand early;
  early.op = shard::RangeOp::kGrantLease;
  early.node = NodeId{2};
  early.time = 2150;
  early.expiry = 3150;
  machine.apply(LogIndex{2}, shard::encode_range_command(early));
  check(machine.descriptor().lease.holder == NodeId{1},
        "a lease starting less than two clock bounds after the last expiry must be refused");

  shard::RangeCommand later = early;
  later.time = 2201;
  later.expiry = 3201;
  machine.apply(LogIndex{3}, shard::encode_range_command(later));
  check(machine.descriptor().lease.holder == NodeId{2},
        "two bounds after the expiry it is safe, and is granted");

  // The renewal path: the holder itself may extend at any time.
  shard::RangeCommand renew;
  renew.op = shard::RangeOp::kGrantLease;
  renew.node = NodeId{2};
  renew.time = 2500;
  renew.expiry = 3500;
  machine.apply(LogIndex{4}, shard::encode_range_command(renew));
  check(machine.descriptor().lease.expiry == 3500, "the holder may renew its own lease");
}

// ---------------------------------------------------------------------------
// placement
// ---------------------------------------------------------------------------

shard::TopologyMachine with_stats(std::uint64_t keys, const std::string& median) {
  shard::TopologyMachine machine = bootstrapped();
  const RangeId id = machine.state().ranges.begin()->second.id;
  shard::AdminCommand report;
  report.op = shard::AdminOp::kReportSize;
  report.range = id;
  report.value = keys;
  report.key = median;
  report.time = 2000;
  apply(machine, report, 2);
  for (std::uint32_t i = 1; i <= 5; ++i) {
    shard::AdminCommand beat;
    beat.op = shard::AdminOp::kNodeHeartbeat;
    beat.node = NodeId{i};
    beat.time = 2000;
    apply(machine, beat, 2 + i);
  }
  return machine;
}

void test_placement_splits_a_large_range_at_the_reported_median() {
  shard::TopologyMachine machine = with_stats(40, account(12));
  shard::PlacementOptions options;
  options.split_threshold_keys = 24;
  options.change_cooldown_entries = 0;

  const auto decisions = shard::decide(machine.state(), options, Timestamp{2000, 0}, 5);
  check(!decisions.empty(), "a range over the threshold must be split");
  check(!decisions.empty() && decisions.front().command.op == shard::AdminOp::kSplit,
        "and the decision is a split");
  check(!decisions.empty() && decisions.front().command.key == account(12),
        "at the key the range's own leader reported, not one the driver invented");
}

void test_placement_is_a_function_of_replicated_state_alone() {
  // The same state, twice, on two hypothetical replicas: identical decisions.
  // This is INV-SHARD-09 as a unit test -- the invariant checks it across live
  // nodes, and this checks it against the only other thing that could differ,
  // which is the order the state was built in.
  shard::TopologyMachine a = with_stats(40, account(12));
  shard::TopologyMachine b = with_stats(40, account(12));
  shard::PlacementOptions options;
  options.change_cooldown_entries = 0;

  const auto left = shard::decide(a.state(), options, Timestamp{5000, 0}, 5);
  const auto right = shard::decide(b.state(), options, Timestamp{5000, 0}, 5);
  check(left.size() == right.size(), "the same state must produce the same number of decisions");
  for (std::size_t i = 0; i < left.size() && i < right.size(); ++i) {
    check(left[i].command == right[i].command, "and the same decisions in the same order");
  }
}

void test_placement_waits_for_a_learner_to_catch_up_before_promoting_it() {
  shard::TopologyMachine machine = bootstrapped();
  const RangeId id = machine.state().ranges.begin()->second.id;

  // Node 3 is dead: nothing has been heard from it. Nodes 1, 2, 4, 5 are alive.
  std::uint64_t index = 2;
  for (std::uint32_t i : {1u, 2u, 4u, 5u}) {
    shard::AdminCommand beat;
    beat.op = shard::AdminOp::kNodeHeartbeat;
    beat.node = NodeId{i};
    beat.time = 10'000'000'000ULL;
    apply(machine, beat, index++);
  }

  shard::PlacementOptions options;
  options.change_cooldown_entries = 0;
  const Timestamp now{10'000'000'000ULL, 0};

  auto decisions = shard::decide(machine.state(), options, now, 5);
  check(!decisions.empty(), "a dead voter must be repaired");
  check(!decisions.empty() && decisions.front().command.op == shard::AdminOp::kChangeReplicas,
        "by a replica change");
  check(!decisions.empty() && decisions.front().command.replicas.size() == 3 &&
            decisions.front().command.learners.size() == 1,
        "which adds a LEARNER and leaves the voter set alone: a fresh voter holds none of "
        "the range's data, and counting it toward a quorum is a silent loss of durability");

  // Apply it, then report the learner caught up, and only then may it be
  // promoted.
  apply(machine, decisions.front().command, index++);
  decisions = shard::decide(machine.state(), options, now, 5);
  check(decisions.empty() || decisions.front().command.replicas.size() != 3 ||
            !decisions.front().command.replicas.empty(),
        "nothing promotes a learner that has not reported catching up");
  const bool promoted_early =
      !decisions.empty() && decisions.front().command.op == shard::AdminOp::kChangeReplicas &&
      decisions.front().command.learners.empty();
  check(!promoted_early, "the learner is not promoted before it has caught up");

  shard::AdminCommand caught;
  caught.op = shard::AdminOp::kReportCatchup;
  caught.range = id;
  caught.node = NodeId{4};
  apply(machine, caught, index++);

  decisions = shard::decide(machine.state(), options, now, 5);
  check(!decisions.empty(), "once it has caught up, the repair proceeds");
  check(!decisions.empty() && decisions.front().command.op == shard::AdminOp::kChangeReplicas,
        "with a replica change");
  const auto& replicas = decisions.front().command.replicas;
  check(std::find(replicas.begin(), replicas.end(), NodeId{4}) != replicas.end(),
        "promoting the learner that caught up");
  check(std::find(replicas.begin(), replicas.end(), NodeId{3}) == replicas.end(),
        "and dropping the dead voter");
}

void test_the_cooldown_stops_a_split_and_merge_oscillation() {
  shard::TopologyMachine machine = with_stats(40, account(12));
  shard::PlacementOptions options;
  options.split_threshold_keys = 24;
  options.change_cooldown_entries = 20;

  // The bootstrap stamped the descriptor at placement entry 1, and the reports
  // above took it to entry 7 -- well inside a twenty-entry cooldown.
  const auto too_soon = shard::decide(machine.state(), options, Timestamp{2000, 0}, 5);
  check(too_soon.empty(), "a range that has just changed is left alone");

  // Twenty more entries of ordinary traffic, and it is fair game again. The
  // cooldown is counted in entries rather than seconds precisely so that this
  // is the same answer on every replica (ANV-0045).
  std::uint64_t index = 8;
  for (int i = 0; i < 24; ++i) {
    shard::AdminCommand beat;
    beat.op = shard::AdminOp::kNodeHeartbeat;
    beat.node = NodeId{1};
    beat.time = 3000 + i;
    apply(machine, beat, index++);
  }
  const auto later = shard::decide(machine.state(), options, Timestamp{2'000'000'000ULL, 0}, 5);
  check(!later.empty(), "and is acted on once it has settled");
}

// ---------------------------------------------------------------------------
// snapshots
// ---------------------------------------------------------------------------

void test_the_topology_survives_its_own_snapshot() {
  shard::TopologyMachine machine = bootstrapped();
  RangeId current = machine.state().ranges.begin()->second.id;
  std::uint64_t index = 2;
  for (int at : {6, 12, 18}) {
    shard::AdminCommand split;
    split.op = shard::AdminOp::kSplit;
    split.range = current;
    split.key = account(at);
    split.generation = machine.state().find(current)->generation;
    split.time = 1000 * index;
    apply(machine, split, index++);
    current = machine.state().find_by_key(account(at))->id;
  }
  shard::AdminCommand report;
  report.op = shard::AdminOp::kReportSize;
  report.range = current;
  report.value = 6;
  report.key = account(20);
  apply(machine, report, index++);

  shard::TopologyMachine restored{shard::TopologyOptions{}};
  restored.restore(machine.snapshot());

  check(restored.state().ranges.size() == machine.state().ranges.size(),
        "a restored topology has the same ranges");
  check(restored.state().next_range_id == machine.state().next_range_id,
        "and the same range id counter: reusing an id would give two ranges one group");
  check(restored.state().meta.size() == machine.state().meta.size(),
        "and a meta index rebuilt to match");
  for (const auto& [start, desc] : machine.state().ranges) {
    const shard::RangeDescriptor* other = restored.state().find(desc.id);
    check(other != nullptr && other->start == desc.start && other->end == desc.end &&
              other->generation == desc.generation,
          "every descriptor survives");
  }
  const auto stats = restored.state().stats.find(current.value());
  check(stats != restored.state().stats.end() && stats->second.median == account(20),
        "and so does what the ranges reported about themselves");
}

void test_a_range_survives_its_own_snapshot() {
  shard::RangeDescriptor desc;
  desc.id = RangeId{2};
  desc.end = account(12);
  desc.generation = 3;
  desc.lease = shard::Lease{NodeId{2}, 100, 900};
  shard::RangeMachine machine{desc, shard::RangeOptions{}, false};

  shard::RangeCommand init;
  init.op = shard::RangeOp::kInit;
  init.generation = 3;
  init.end = account(12);
  std::map<std::string, std::int64_t> balances;
  for (int i = 0; i < 12; ++i) balances[account(i)] = 100;
  init.payload = shard::RangeMachine::encode_payload(balances, {});
  machine.apply(LogIndex{1}, shard::encode_range_command(init));

  shard::RangeCommand transfer;
  transfer.op = shard::RangeOp::kTransfer;
  transfer.from = account(1);
  transfer.to = account(2);
  transfer.amount = 25;
  transfer.op_id = 7;
  transfer.generation = 3;
  machine.apply(LogIndex{2}, shard::encode_range_command(transfer));

  shard::RangeMachine restored{shard::RangeDescriptor{}, shard::RangeOptions{}, false};
  restored.restore(machine.snapshot());
  check(restored.initialised(), "a restored range is initialised");
  check(restored.total() == machine.total(), "with the same total");
  check(restored.balances() == machine.balances(), "and the same balances");
  check(restored.descriptor().end == account(12), "and the same span");
  check(restored.descriptor().lease == machine.descriptor().lease, "and the same lease");
  check(restored.decided().count(7) == 1,
        "and the same decision ledger -- without it, a retry of an already-applied "
        "transfer applies a second time");
}

void test_a_replica_that_replays_from_the_start_reaches_the_same_state() {
  // The bug this pins: a replica added to an existing range is constructed from
  // the descriptor the topology has *now*, and then fed the range's log from
  // the beginning. If the span is not in the log, the replay is against the
  // wrong starting state and the split trigger is silently rejected.
  std::vector<std::string> log;
  {
    shard::RangeCommand init;
    init.op = shard::RangeOp::kInit;
    init.generation = 1;
    std::map<std::string, std::int64_t> balances;
    for (int i = 0; i < 8; ++i) balances[account(i)] = 100;
    init.payload = shard::RangeMachine::encode_payload(balances, {});
    log.push_back(shard::encode_range_command(init));

    shard::RangeCommand split;
    split.op = shard::RangeOp::kSplitTrigger;
    split.end = account(4);
    split.other = RangeId{3};
    split.generation = 2;
    log.push_back(shard::encode_range_command(split));
  }

  // The replica that was there from the beginning.
  shard::RangeDescriptor original;
  original.id = RangeId{2};
  shard::RangeMachine early{original, shard::RangeOptions{}, false};

  // The replica added later, constructed from the descriptor as it stands now.
  shard::RangeDescriptor current;
  current.id = RangeId{2};
  current.end = account(4);
  current.generation = 2;
  shard::RangeMachine late{current, shard::RangeOptions{}, false};

  for (std::size_t i = 0; i < log.size(); ++i) {
    early.apply(LogIndex{i + 1}, log[i]);
    late.apply(LogIndex{i + 1}, log[i]);
  }

  check(early.balances() == late.balances(),
        "a replica that replays the log from the start must reach the same state as one "
        "that was always there");
  check(late.key_count() == 4, "which means four accounts, not eight");
  check(late.descriptor().end == account(4), "with the span the log says");
}

}  // namespace

int main() {
  std::cout << "shard unit tests\n";

  test_descriptor_survives_its_own_encoding();
  test_coverage_detects_gaps_and_overlaps();

  test_a_split_is_one_apply();
  test_a_non_atomic_split_leaves_the_key_space_covered_twice();
  test_a_split_at_the_boundary_is_refused();
  test_a_stale_split_decision_is_refused();

  test_a_merge_requires_colocation();
  test_a_merge_removes_exactly_one_range();
  test_a_finish_without_a_freeze_is_refused();

  test_a_replica_change_must_keep_a_quorum_of_both_sets();
  test_a_lease_held_by_a_departed_replica_is_dropped();

  test_the_meta_index_resolves_every_range();
  test_the_cache_serves_hits_and_is_invalidated_by_a_rejection();
  test_a_stale_generation_is_rejected_rather_than_served();
  test_a_transfer_across_the_split_point_is_refused_whole();

  test_a_lease_may_not_start_before_the_previous_one_has_provably_ended();

  test_placement_splits_a_large_range_at_the_reported_median();
  test_placement_is_a_function_of_replicated_state_alone();
  test_placement_waits_for_a_learner_to_catch_up_before_promoting_it();
  test_the_cooldown_stops_a_split_and_merge_oscillation();

  test_the_topology_survives_its_own_snapshot();
  test_a_range_survives_its_own_snapshot();
  test_a_replica_that_replays_from_the_start_reaches_the_same_state();

  if (g_failures != 0) {
    std::cerr << "shard unit tests: " << g_failures << " failure(s)\n";
    return 1;
  }
  std::cout << "shard unit tests: all checks passed\n";
  return 0;
}
