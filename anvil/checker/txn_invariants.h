// The god's-eye view of a distributed transaction protocol.
//
// This is the layer where the gap between "what a client can see" and "what is
// actually true" is widest, and the invariant catalogue's API column says so:
// of the fifteen INV-TXN-*, six are marked No. A primary lock whose record says
// committed while a secondary intent still sits unresolved is invisible until
// somebody reads that key. A transaction record that leaves a terminal state is
// invisible until two readers disagree. A timestamp oracle that re-issues a
// number after failover is invisible until two transactions commit at the same
// version and one of them disappears.
//
// The observer reads every node's ranges directly -- versions, intents and
// records -- and every coordinator's in-flight state. No hooks in the protocol,
// for the same reason as everywhere else: the thing being checked has to be the
// thing that ships.

#ifndef ANVIL_CHECKER_TXN_INVARIANTS_H_
#define ANVIL_CHECKER_TXN_INVARIANTS_H_

#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "anvil/checker/invariant.h"
#include "anvil/core/shard/store.h"
#include "anvil/core/txn/coordinator.h"
#include "anvil/core/types.h"

namespace anvil::checker {

class TxnObserver {
 public:
  struct Hooks {
    std::function<std::uint64_t()> tick;
    std::function<Timestamp()> true_now;
    std::function<bool(NodeId)> alive;
  };

  void configure(std::uint32_t nodes, Hooks hooks);

  // Re-registered on every boot: a crash destroys both objects and the next
  // incarnation is a different one.
  void set_store(NodeId id, const shard::ShardStore* store);
  void set_coordinator(NodeId id, const txn::Coordinator* coordinator);

  void refresh();
  std::optional<std::string> take(const std::string& id);

  std::vector<NodeId> live_nodes() const;
  const shard::ShardStore* store(NodeId id) const;
  const txn::Coordinator* coordinator(NodeId id) const;

  struct Counters {
    std::uint64_t scans = 0;
    std::uint64_t records_seen = 0;
    std::uint64_t records_committed = 0;
    std::uint64_t records_aborted = 0;
    std::uint64_t intents_seen = 0;
    std::uint64_t status_transitions = 0;
    std::uint64_t oracle_reservations = 0;
    std::uint64_t oracle_high_water = 0;
    std::uint64_t wait_edges_seen = 0;
    std::uint64_t source_changes = 0;
  };
  const Counters& counters() const noexcept { return counters_; }

  // Every commit timestamp any transaction has ever been observed to use, and
  // which transaction used it. A second transaction arriving at the same
  // timestamp means the oracle handed one out twice, which is the failure
  // INV-TXN-09 exists for and which no client can see.
  const std::map<txn::Ts, txn::TxnId>& commit_timestamps() const noexcept {
    return commit_timestamps_;
  }

 private:
  void record(const std::string& id, std::string detail);

  struct RecordMirror {
    txn::TxnStatus status = txn::TxnStatus::kPending;
    txn::Ts commit_ts = 0;
    std::uint32_t epoch = 0;
    bool seen = false;
    std::uint64_t source = 0;
    // A record's primary key can move to a different range on the same node
    // via a split or a merge; comparing what the *new* home reports against
    // what the old one last said would grade a relocation as a transition.
    // Tracking the range alongside the node is what keeps "moved" from
    // looking like "changed its mind".
    std::uint64_t source_range = 0;
  };

  std::uint32_t nodes_ = 0;
  Hooks hooks_;
  std::map<std::uint64_t, const shard::ShardStore*> stores_;
  std::map<std::uint64_t, const txn::Coordinator*> coordinators_;

  std::map<txn::TxnId, RecordMirror> records_;
  std::map<txn::Ts, txn::TxnId> commit_timestamps_;
  txn::Ts oracle_high_water_ = 0;
  // The highest mark each replica has been seen at. Per node, because a
  // follower behind the leader is a follower, not a regression.
  std::map<std::uint64_t, txn::Ts> oracle_seen_;

  std::map<std::string, std::vector<std::string>> pending_;
  std::uint64_t last_tick_ = UINT64_MAX;
  Counters counters_;
};

// Arms INV-TXN-01..04, 07, 09, 11, 12 and 15. The rest are history properties:
// INV-TXN-05 (SSI), 06 (real-time order) and 08 (commit-wait) are checked
// offline by the Elle-style checker over the recorded history, because they are
// statements about an *ordering of transactions* and no amount of looking at
// live state can decide them. 10 is a quiesce-class property of the workload.
// 13 and 14 belong to the change feed, which P6 does not ship.
void arm_txn_invariants(InvariantRegistry& registry, TxnObserver* observer);

}  // namespace anvil::checker

#endif  // ANVIL_CHECKER_TXN_INVARIANTS_H_
