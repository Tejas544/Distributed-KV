// Raft, as a pure state machine.
//
// No clock, no sockets, no files, no threads. Inputs are `tick`, `step`,
// `propose`, `read_index` and `transfer_leadership`; the only output is a
// `Ready` batch that says what must become durable and what must then be sent.
// The driver (driver.h) is the only thing that touches a Runtime.
//
// This split buys three things that matter more than the elegance:
//
//   1. The durability ordering exists in one place. "Persist the vote before
//      replying" is not a discipline scattered over a dozen call sites; it is
//      the shape of the loop in driver.cc, and the seeded mutation that breaks
//      it is one line.
//   2. The invariant checker can read the whole global state -- every node's
//      log, term, vote, commit index, progress table and lease -- with no hooks
//      compiled into the protocol. The core does not know it is being watched.
//   3. Every protocol test can run without a simulator at all. A unit test that
//      needs "candidate at term 5 with a stale log" builds one directly.
//
// What is implemented: pre-vote, CheckQuorum, randomised elections, batching
// and pipelined replication with term-based conflict backtracking, the
// current-term commit restriction (Figure 8), log compaction, chunked snapshot
// install with flow control, joint-consensus membership change, learners, the
// leader lease, ReadIndex, and leadership transfer.
//
// What is deliberately not: witness replicas, quorum leases, follower reads
// (P6, they need closed timestamps), and asynchronous log writes that let the
// leader ack before its own disk. The last one is a real performance technique
// and it changes the durability argument, so it belongs behind its own flag
// with its own invariant rather than as an unremarked default.

#ifndef ANVIL_CORE_RAFT_RAFT_H_
#define ANVIL_CORE_RAFT_RAFT_H_

#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "anvil/core/raft/config.h"
#include "anvil/core/raft/log.h"
#include "anvil/core/raft/types.h"
#include "anvil/core/random.h"
#include "anvil/core/types.h"

namespace anvil::raft {

// How the leader is currently talking to one peer.
enum class ProgressState : std::uint8_t {
  kProbe,      // one unacknowledged message at a time, hunting for the match point
  kReplicate,  // pipelining: send optimistically, up to the inflight window
  kSnapshot,   // the peer is behind the log's start; shipping a snapshot instead
};

struct Progress {
  LogIndex next{1};
  LogIndex match{};
  ProgressState state = ProgressState::kProbe;
  std::uint32_t inflight = 0;

  // CheckQuorum: has this peer been heard from during the current election
  // interval? A leader that cannot see a quorum steps down instead of
  // continuing to accept writes it can never commit.
  bool recent_active = false;

  // Heartbeat rounds since anything was heard from this peer.
  //
  // Pipelining is what makes replication fast and it is also what makes it
  // possible to wedge: `inflight` counts messages the leader has sent and not
  // seen answered, and a partition answers nothing. Without this counter the
  // window fills, `send_append` returns early forever, and the follower stays
  // permanently behind after the partition heals -- with the leader reporting
  // it as a healthy peer the whole time.
  std::uint32_t idle_rounds = 0;

  // The send-timestamp of the most recent message this peer has acknowledged.
  // The lease is computed from *send* times, not receipt times -- see
  // update_lease() for why that is the only version that is safe.
  Timestamp acked_send;
  Timestamp pending_send;

  // Snapshot flow control: the offset of the chunk currently in flight.
  std::uint64_t snapshot_offset = 0;
  LogIndex snapshot_index{};

  bool is_learner = false;
};

// Whether a lease can be safe at all under these settings: the uncertainty it
// must absorb has to be smaller than the lease, and the lease plus that
// uncertainty has to fit inside the minimum election timeout. When it does not,
// lease reads are simply unavailable and every read pays for a quorum round.
bool lease_is_sound(const RaftOptions& options) noexcept;

class RaftNode {
 public:
  RaftNode(NodeId self, RaftOptions options, DeterministicRandom rng);

  // ---- lifecycle ---------------------------------------------------------
  // Restores a node from durable state. Called once per boot, including the
  // first: a node that has never run recovers an empty log and a zero term,
  // which is the same code path as one that crashed mid-election.
  void restore(const HardState& hard, std::vector<LogEntry> entries, const Snapshot& snapshot,
               const Config& bootstrap);

  // ---- inputs ------------------------------------------------------------
  void tick(Timestamp now);
  void step(const RaftMessage& msg, Timestamp now);

  // Leader only. Returns kNotLeader elsewhere, which the client turns into a
  // redirect rather than an error.
  Status propose(EntryType type, std::string data, LogIndex* assigned, Timestamp now);
  Status propose_conf_change(const ConfChange& change, Timestamp now);

  // Registers a linearizable read. The result arrives in Ready::read_states
  // once it is safe to serve; the caller then waits for applied >= index.
  Status read_index(std::uint64_t context, Timestamp now);

  Status transfer_leadership(NodeId target, Timestamp now);

  // Forces an election immediately, skipping the timeout and pre-vote. Used by
  // TimeoutNow and by tests that need a leader without waiting.
  void campaign_now(Timestamp now);

  // ---- outputs -----------------------------------------------------------
  Ready ready(Timestamp now);
  void advance(const Ready& ready, Timestamp now);

  // ---- log compaction ----------------------------------------------------
  // True when the state machine should be snapshotted. The driver asks, builds
  // the snapshot bytes, and calls compacted() once it is durable -- the state
  // machine's contents are not this class's business.
  //
  // The bytes are then held here, because the leader has to be able to ship
  // them to a follower that has fallen off the start of the log, and the
  // alternative (calling back into the state machine mid-replication) would
  // reintroduce exactly the coupling the Ready model exists to remove.
  bool wants_snapshot() const;
  void compacted(const Snapshot& snapshot);
  const Snapshot& snapshot() const noexcept { return snapshot_; }

  // ---- observation (const; the invariant checker's whole interface) -------
  NodeId self() const noexcept { return self_; }
  Role role() const noexcept { return role_; }
  Term term() const noexcept { return term_; }
  NodeId vote() const noexcept { return vote_; }
  NodeId leader() const noexcept { return leader_; }
  const RaftLog& log() const noexcept { return log_; }
  const Config& config() const noexcept { return config_; }

  // What is actually on disk, as opposed to what this incarnation currently
  // believes. The two differ for the width of one fsync, and the difference
  // matters: a term bump or a vote that has not reached the disk has also not
  // reached any peer -- the driver sends nothing until it is durable -- so it
  // is a decision the cluster never saw and losing it in a crash costs nothing.
  // Checking the volatile field instead reports every crash during an election
  // as a term regression, which is a checker bug that reads exactly like a
  // protocol bug.
  const HardState& persisted_hard_state() const noexcept { return persisted_hard_; }
  const std::map<std::uint64_t, Progress>& progress() const noexcept { return progress_; }
  const RaftOptions& options() const noexcept { return options_; }

  // A snapshot has been installed into the log and has not yet been handed to
  // the state machine. Between those two moments the log's applied index and
  // the state machine's contents describe different points in history, and
  // anything comparing one node against another has to skip a node in that
  // state or it reports a divergence that does not exist (ANV-0040).
  bool snapshot_pending() const noexcept { return pending_install_; }

  // When this node believes it may serve a lease read. Zero means no lease.
  Timestamp lease_expiry() const noexcept { return lease_expiry_; }
  bool lease_valid(Timestamp now) const;

  // Whether this node's local state is authoritative enough to answer from
  // without a round trip.
  //
  // The condition every caller reached for before this existed -- `applied >=
  // commit` -- is not sufficient on its own, and the reason survived two
  // fixes already (ANV-0041, ANV-0048). The commit index *is* durable, but
  // only as far as the last hard-state fsync: a node can crash having
  // committed and applied index 4 in memory while the disk still says
  // commit = 2. It recovers with commit = 2, replays to applied = 2, and
  // reports `applied >= commit` -- perfectly true, and perfectly useless,
  // because entries 3 and 4 are committed on the cluster and this node has
  // not heard of them yet. Every guard built on that comparison alone is
  // satisfied by a node that is genuinely behind.
  //
  // What closes it is Raft's own leader-completeness property: a candidate
  // can only win with a log at least as up to date as a quorum, so a
  // *leader* that has committed an entry in its own term necessarily holds
  // every entry committed before it. That is the standard read-index
  // precondition, and establishing it is exactly what the no-op appended in
  // become_leader is for.
  bool can_serve_local_reads() const;

  // Monotonic, bumped by every state mutation. The checker uses it to skip
  // nodes that cannot have changed, which is what keeps a tick-class predicate
  // over the whole cluster affordable.
  std::uint64_t revision() const noexcept { return revision_; }

  // Diagnostics, for violation messages. Allocates; never called on a hot path.
  std::string describe() const;

 private:
  // ---- role transitions --------------------------------------------------
  void become_follower(Term term, NodeId leader);
  void become_pre_candidate(Timestamp now);
  void become_candidate(Timestamp now);
  void become_leader(Timestamp now);
  void campaign(bool pre_vote, bool force, Timestamp now);

  // ---- message handling --------------------------------------------------
  void step_vote_request(const RaftMessage& msg, Timestamp now);
  void step_vote_reply(const RaftMessage& msg, Timestamp now);
  void step_append(const RaftMessage& msg, Timestamp now);
  void step_append_reply(const RaftMessage& msg, Timestamp now);
  void step_heartbeat(const RaftMessage& msg, Timestamp now);
  void step_heartbeat_reply(const RaftMessage& msg, Timestamp now);
  void step_snapshot(const RaftMessage& msg, Timestamp now);
  void step_snapshot_reply(const RaftMessage& msg, Timestamp now);

  // ---- leader work -------------------------------------------------------
  void broadcast_append(Timestamp now);
  void broadcast_heartbeat(std::uint64_t read_context, Timestamp now);
  void send_append(NodeId peer, Timestamp now);
  void send_snapshot_chunk(NodeId peer, Timestamp now);
  bool maybe_advance_commit();
  void update_lease(Timestamp now);
  void check_quorum_or_step_down(Timestamp now);
  void maybe_finish_joint(Timestamp now);
  void resolve_pending_reads(Timestamp now);

  // ---- helpers -----------------------------------------------------------
  void reset_election_timer();
  void send(RaftMessage msg);
  void set_term(Term term, NodeId vote);
  void apply_conf_change(const LogEntry& entry);
  void reset_progress(Timestamp now);
  bool promotable() const;
  std::set<std::uint64_t> granted_votes() const;
  std::set<std::uint64_t> rejected_votes() const;
  Progress& progress_for(NodeId peer);

  NodeId self_;
  RaftOptions options_;
  DeterministicRandom rng_;

  Role role_ = Role::kFollower;
  Term term_{};
  NodeId vote_{};
  NodeId leader_{};

  RaftLog log_;
  Config config_;
  std::map<std::uint64_t, Progress> progress_;

  // Election bookkeeping. `votes_` maps voter -> granted, and is kept for both
  // the pre-vote and the real vote round; they never overlap because a
  // pre-candidate that wins immediately becomes a candidate and clears it.
  std::map<std::uint64_t, bool> votes_;

  std::uint32_t election_elapsed_ = 0;
  std::uint32_t heartbeat_elapsed_ = 0;
  std::uint32_t randomized_election_timeout_ = 0;

  // Lease state. `lease_expiry_` is expressed on this node's own clock, which
  // is the whole point: a node whose clock is skewed or that was paused for
  // four seconds will disagree with the rest of the cluster about whether its
  // lease is live, and INV-RAFT-13 is where that disagreement becomes visible.
  Timestamp lease_expiry_{};

  // The broken alternative, kept behind RaftOptions::lease_uses_wall_clock so
  // the drill can select it: a lease counted in ticks. Ticks do not advance
  // while a process is paused, so the lease survives a stop-the-world pause
  // that a wall-clock lease would have expired -- which is the bug, and it is
  // completely invisible until something actually pauses.
  std::uint32_t lease_ticks_ = 0;

  // Pending ReadIndex requests. Context -> the commit index at request time,
  // plus which voters have confirmed the heartbeat round.
  struct PendingRead {
    LogIndex index{};
    std::set<std::uint64_t> acks;
    bool waiting_for_term_commit = false;
  };
  std::map<std::uint64_t, PendingRead> pending_reads_;

  // The most recent snapshot this node has taken or installed, held so the
  // leader can ship it. Empty until the first compaction.
  Snapshot snapshot_;

  // Snapshot assembly on the receiving side.
  Snapshot incoming_snapshot_;
  std::uint64_t incoming_offset_ = 0;
  bool has_incoming_snapshot_ = false;

  // Outputs accumulated since the last ready().
  std::vector<RaftMessage> outbox_;
  std::vector<ReadState> ready_reads_;
  bool hard_state_dirty_ = false;
  HardState persisted_hard_;
  bool pending_install_ = false;
  Snapshot pending_install_snapshot_;

  // At most one membership change may be in flight. `pending_conf_index_` is
  // the index of the last conf-change entry appended but not yet applied; a
  // second proposal while one is outstanding is refused rather than queued,
  // because queueing it means deciding what it means when the first one is
  // reverted by a leader change.
  LogIndex pending_conf_index_{};

  NodeId leader_transferee_{};
  std::uint32_t transfer_elapsed_ = 0;

  std::uint64_t revision_ = 0;
};

}  // namespace anvil::raft

#endif  // ANVIL_CORE_RAFT_RAFT_H_
