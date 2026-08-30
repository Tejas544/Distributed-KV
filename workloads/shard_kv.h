// A sharded bank over many ranges: P5's exercise vehicle.
//
// Every node runs a ShardStore and a client. The clients move money between
// accounts, addressing whichever node their range cache says should serve the
// range, over the *real* transport -- so a partition between a client and the
// lease holder looks exactly like a partition, and a stale cache entry looks
// exactly like a stale cache entry.
//
// The workload is a bank for one reason: the total never changes. Every failure
// this phase is about moves it.
//
//   a split that drops a key                      total falls
//   a split that duplicates one                   total rises
//   a merge that loses the subsumed range's data  total falls
//   a write accepted against a stale descriptor   total moves, and the money
//                                                 lands in a range nobody will
//                                                 ever read it from again
//   a transfer applied twice                      total is preserved but the
//                                                 op ledger shows it, which is
//                                                 why there is a ledger as well
//
// So the client-visible oracle is one integer, checked at the end of the run
// against the number the cluster started with, plus the requirement that every
// acknowledged operation is findable in exactly one range's decision ledger.
// Both of those are things a black-box client could compute. Everything the
// *checker* finds -- gaps in the key space, two leases at once, a voter
// promoted before it holds any data -- is invisible to them, and the gap
// between the two columns is the point of the phase.
//
// Transfers deliberately pick accounts that are close together in key order, so
// that most of them land inside one range and a steady fraction of them straddle
// a boundary that is moving underneath. That fraction is P5's exit criterion 3.

#ifndef ANVIL_WORKLOADS_SHARD_KV_H_
#define ANVIL_WORKLOADS_SHARD_KV_H_

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "anvil/checker/shard_invariants.h"
#include "anvil/core/shard/store.h"
#include "anvil/core/types.h"
#include "anvil/sim/simulation.h"

namespace anvil::workloads {

struct ShardKvConfig {
  std::uint64_t ops_per_client = 40;
  Duration client_interval = Duration::millis(20);
  Duration client_poll = Duration::millis(10);
  Duration client_timeout = Duration::millis(2500);

  std::uint32_t accounts = 24;
  std::int64_t initial_balance = 100;
  std::int64_t max_transfer = 20;

  // How far apart, in account order, the two ends of a transfer may be. Small,
  // because the interesting transfers are the ones that straddle a range
  // boundary that is moving, and a uniformly random pair almost never does once
  // there are more than a handful of ranges.
  std::uint32_t neighbourhood = 3;

  // Reads, served under the range lease. They exist to give the lease a job:
  // without a read path a lease is a field nobody consults, and INV-SHARD-04
  // would be checking an ornament.
  std::uint32_t read_percent = 25;

  shard::StoreOptions store;
};

struct ShardKvNode {
  ShardKvNode();
  ~ShardKvNode();
  ShardKvNode(ShardKvNode&&) noexcept;
  ShardKvNode& operator=(ShardKvNode&&) noexcept;

  std::unique_ptr<shard::ShardStore> store;

  std::uint64_t next_seq = 1;
  std::uint64_t ops_done = 0;

  // The request currently being attempted, held stable across retries. A retry
  // that changes the operation is a different request wearing the same name,
  // and a late reply then attaches to the wrong one (ANV-0022).
  bool in_flight = false;
  shard::ShardStore::Request pending;
  shard::ShardStore::Route route;

  // Where to send the next attempt, when the last answer named somebody. A
  // client that ignores the redirect it was just given re-sends to the node
  // that redirected it, forever -- which reads as a partition and is really a
  // client that cannot take advice.
  NodeId leader_hint{};

  // Who last answered for each range. A client that re-resolves through the
  // meta index on every redirect pays a round trip per request forever: the
  // meta record names the lease holder as of the last topology change, and the
  // node that just said "not me" knows better.
  std::map<std::uint64_t, NodeId> range_leader;
  bool reply_pending = false;
  shard::ShardStore::Reply reply;

  bool booted = false;
  std::uint64_t boots = 0;
};

// One operation the cluster acknowledged, and what it said.
struct AckedOp {
  std::uint64_t op_id = 0;
  std::string from;
  std::string to;
  std::int64_t amount = 0;
  bool applied = false;  // false means it was decided as "insufficient funds"
  RangeId range{};
  Timestamp when;
};

struct ShardKvState {
  std::map<std::uint64_t, AckedOp> acked;

  std::uint64_t transfers_acked = 0;
  std::uint64_t transfers_applied = 0;
  std::uint64_t transfers_declined = 0;   // insufficient funds; a real outcome
  std::uint64_t reads_served = 0;
  std::uint64_t client_retries = 0;
  std::uint64_t client_timeouts = 0;

  // The exit-criterion-3 counters. `straddling_attempts` is the number of
  // transfers the client believed one range covered; `wrong_range` is how many
  // of those the cluster rejected because the topology had moved underneath.
  std::uint64_t straddling_attempts = 0;
  std::uint64_t wrong_range_replies = 0;
  std::uint64_t not_leader_replies = 0;
  std::uint64_t declined_cross_range = 0;  // the cache correctly said "two ranges"

  // Client-visible read freshness: a lease read that came back from a range at
  // a lower applied index than a read of the same range already returned.
  std::uint64_t stale_lease_reads = 0;
  std::map<std::uint64_t, std::uint64_t> read_high_water;  // range id -> applied index

  // Filled by the audits.
  std::int64_t total_balance = 0;
  std::int64_t expected_balance = 0;
  std::uint64_t lost_ops = 0;
  std::uint64_t duplicated_ops = 0;
  std::uint64_t ranges_final = 0;
  std::vector<std::string> violations;

  std::map<std::uint64_t, ShardKvNode> nodes;
  std::uint32_t node_count = 0;

  sim::Simulation* simulation = nullptr;
  checker::ShardObserver* observer = nullptr;
  ShardKvConfig config;
};

// Builds the cluster, arms the invariants and boots every node.
void install(sim::Simulation& simulation, ShardKvConfig config, ShardKvState* state,
             checker::ShardObserver* observer);

// Sums every range's balances across the cluster, counting each range once and
// including the data a split has moved but not yet handed over. Returns the
// total and appends an explanation of anything that could not be accounted for.
std::int64_t audit_conservation(sim::Simulation& simulation, ShardKvState* state);

// Every acknowledged operation must be findable in exactly one live range's
// decision ledger, with the outcome the client was told.
void audit_ledger(sim::Simulation& simulation, ShardKvState* state);

// True once the topology has settled: every range in it has an initialised
// replica with a leader, and no merge is half-done.
bool converged(sim::Simulation& simulation, const ShardKvState& state);

// The node hosting the placement group's leader, or NodeId{} if there is none.
NodeId placement_leader(const ShardKvState& state, sim::Simulation& simulation);

// How many ranges the cluster currently has, by the freshest live view.
std::size_t range_count(sim::Simulation& simulation, const ShardKvState& state);

}  // namespace anvil::workloads

#endif  // ANVIL_WORKLOADS_SHARD_KV_H_
