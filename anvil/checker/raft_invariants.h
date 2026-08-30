// The god's-eye view of a Raft cluster.
//
// This is the file the whole project is an argument for. Jepsen-style checking
// observes a system through its client API and searches the resulting history
// for anomalies; it finds real bugs and it structurally cannot find the ones
// that corrupt internal state and are masked before a client could look. A Raft
// log that briefly holds an entry it should not, a vote granted twice in one
// term because the first was never durable, a commit index advanced on a
// previous term's entry -- all of these are recoverable, all are invisible from
// outside, and all are the bug that eats your data six months later under a
// different interleaving.
//
// Here every node's log, term, vote, commit index, configuration and lease are
// simply readable, because the simulator runs the entire cluster in one address
// space. The predicates below are evaluated *while the system runs*, and a
// violation is reported at the tick it happens.
//
// Two design decisions are worth defending:
//
//   No hooks in the protocol. RaftNode does not know this file exists. The
//   observer diffs state between ticks instead, which costs a little more and
//   means the thing being checked is exactly the thing that ships.
//
//   Incremental scanning. A predicate at tick class must be O(nodes), so the
//   observer keeps a cursor per node and only looks at what changed. Log
//   matching over full logs runs at epoch class, where O(nodes^2 * log) is
//   affordable. Getting this wrong is not subtle -- it is the difference
//   between 38,000 simulated node-hours per core-hour and a few hundred.

#ifndef ANVIL_CHECKER_RAFT_INVARIANTS_H_
#define ANVIL_CHECKER_RAFT_INVARIANTS_H_

#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "anvil/checker/invariant.h"
#include "anvil/core/raft/raft.h"
#include "anvil/core/types.h"

namespace anvil::checker {

class RaftObserver {
 public:
  // The simulator-side facts the checker needs and cannot derive.
  //
  //   tick      gates the incremental scan so it runs once per scheduler event
  //             rather than once per predicate.
  //   true_now  the simulator's real time, for comparing leases held by nodes
  //             whose own clocks disagree.
  //   alive     a crashed node's volatile state is gone; reading it would
  //             report a dead leader's stale lease as a live one.
  //   node_now  that node's *opinion* of the time, which is what its lease is
  //             expressed in.
  struct Hooks {
    std::function<std::uint64_t()> tick;
    std::function<Timestamp()> true_now;
    std::function<bool(NodeId)> alive;
    std::function<Timestamp(NodeId)> node_now;
  };

  void configure(std::uint32_t nodes, Hooks hooks);

  // Re-registered on every boot, because a restart constructs a new RaftNode.
  void set_node(NodeId id, const raft::RaftNode* node);

  // Recovery discarded durable state. Whether that excuses anything depends on
  // *why*: media damage is unavoidable and only has to be detected, whereas a
  // tail that was never synced is a durability defect wearing the same symptom.
  // The harness knows which faults are armed and says so here; without that,
  // the exemption meant for bit rot would quietly excuse a missing fsync, which
  // is the single most important durability guarantee there is.
  void note_corruption(NodeId id);
  void set_media_faults_possible(bool possible) noexcept { media_faults_ = possible; }

  // Advances the per-node cursors and records anything the scan finds. Called
  // by every predicate; a no-op after the first call within one scheduler tick.
  void refresh();

  // Pops the oldest violation recorded for an invariant id, if any.
  std::optional<std::string> take(const std::string& id);

  // ---- accessors used by the live predicates -----------------------------
  const raft::RaftNode* node(NodeId id) const;
  std::vector<NodeId> live_nodes() const;
  std::uint32_t node_count() const noexcept { return nodes_; }
  bool corrupted(NodeId id) const;
  Timestamp true_now() const;
  Timestamp node_now(NodeId id) const;

  // Where this node's commit index stood when it most recently became leader.
  // A leader inherits a commit index pointing at an older term's entry, which
  // is legal; what is not legal is *advancing* onto one. Without this the
  // Figure-8 check would fire on every election.
  LogIndex commit_at_election(NodeId id) const;

  // The union, over all history, of everything any node has ever considered
  // committed. Leader Completeness is checked against this.
  const std::map<std::uint64_t, std::pair<Term, std::uint64_t>>& committed() const noexcept {
    return committed_;
  }

  struct Counters {
    std::uint64_t scans = 0;
    std::uint64_t entries_scanned = 0;
    std::uint64_t rescans_after_truncation = 0;
    std::uint64_t compaction_gaps = 0;  // entries compacted before we saw them
    std::uint64_t leaders_seen = 0;
    std::uint64_t elections_checked = 0;
  };
  const Counters& counters() const noexcept { return counters_; }

 private:
  struct Mirror {
    const raft::RaftNode* node = nullptr;
    std::uint64_t revision = UINT64_MAX;
    LogIndex scanned_to{};
    Term scanned_to_term{};
    Term max_term{};          // highest durable term ever observed
    LogIndex max_persisted{};  // highest index this node ever had durably
    LogIndex max_commit{};    // scan cursor over committed entries (in-memory)
    LogIndex max_commit_durable{};
    raft::Role role = raft::Role::kFollower;
    LogIndex last_index{};
    Term role_term{};
    LogIndex commit_at_election{};
    LogIndex commit_seen_as_leader{};
    bool corrupted = false;
    bool seen = false;
    bool rebooted = false;

    // The configuration this node *should* hold, replayed from its own
    // committed prefix. Compared against what it actually holds (INV-RAFT-12).
    raft::Config derived_config;
    LogIndex derived_to{};
    bool derived_valid = false;
  };

  void scan_node(NodeId id, Mirror& mirror);
  std::string vote_report(Term term, NodeId winner) const;
  void record(const std::string& id, std::string detail);

  std::uint32_t nodes_ = 0;
  Hooks hooks_;
  std::map<std::uint64_t, Mirror> mirrors_;

  // (index, term) -> command digest. Two entries that agree on index and term
  // must be the same entry; this is the inductive core of Log Matching and is
  // INV-RAFT-16.
  std::map<std::pair<std::uint64_t, std::uint64_t>, std::uint64_t> entries_;

  // Which node first showed us each entry. Only used to make a Log Matching
  // violation report name both sides, which is the difference between a
  // finding you can act on and one you have to reproduce first.
  std::map<std::pair<std::uint64_t, std::uint64_t>, std::uint64_t> entry_origin_;
  std::map<std::pair<std::uint64_t, std::uint64_t>, std::string> entry_shape_;

  // index -> (term, digest) for everything any node has committed.
  std::map<std::uint64_t, std::pair<Term, std::uint64_t>> committed_;
  std::map<std::uint64_t, std::pair<std::uint64_t, std::string>> committed_origin_;

  // (node, term) -> the vote it cast. Survives the node's own restart, which
  // is the entire point: a vote that was not durable comes back as a different
  // answer to the same question.
  std::map<std::pair<std::uint64_t, std::uint64_t>, std::uint64_t> votes_;

  // term -> the leader elected in it.
  std::map<std::uint64_t, std::uint64_t> leaders_;

  std::map<std::string, std::vector<std::string>> pending_;
  std::uint64_t last_tick_ = UINT64_MAX;
  bool media_faults_ = false;
  Counters counters_;
};

// Arms INV-RAFT-01..13, 15 and 16. INV-RAFT-14 (a linearizable read never
// returns state older than a completed write) is client-visible and belongs
// with the workload that issues the reads.
void arm_raft_invariants(InvariantRegistry& registry, RaftObserver* observer);

}  // namespace anvil::checker

#endif  // ANVIL_CHECKER_RAFT_INVARIANTS_H_
