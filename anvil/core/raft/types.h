// Raft value types: entries, durable state, messages, and the Ready batch.
//
// Everything here is plain data. The state machine in raft.h consumes and
// produces these; the driver in driver.h is the only thing that touches a
// Runtime. That split is what makes the protocol testable without a clock and
// checkable from the outside without hooks compiled into it.
//
// The one design decision worth arguing about is `Ready`. Instead of letting
// the protocol call send() and fsync() wherever it likes, every step produces a
// batch describing what must become durable and what must then be sent. The
// driver persists first and sends second, in one place, for the whole protocol.
//
// That matters because the two most dangerous Raft bugs are both orderings:
//
//   * A vote reply sent before the vote is durable. The voter crashes, forgets,
//     and votes again in the same term. Two leaders, one term.
//   * An AppendEntries reply sent before the entries are durable. The leader
//     counts the follower toward a quorum for an entry that is not on disk.
//
// Scattered through a thousand lines of protocol code, those are two mistakes
// nobody can see in review. Concentrated in one loop, they are two lines -- and
// two lines that can be deliberately reversed to prove the invariants notice.

#ifndef ANVIL_CORE_RAFT_TYPES_H_
#define ANVIL_CORE_RAFT_TYPES_H_

#include <cstdint>
#include <string>
#include <vector>

#include "anvil/core/types.h"

namespace anvil::raft {

// ---------------------------------------------------------------------------
// roles
// ---------------------------------------------------------------------------

// kPreCandidate is a distinct role, not a flag on kCandidate. A pre-candidate
// has NOT incremented its term and has NOT voted for itself durably; treating
// the two as one state is how implementations accidentally leak a term bump
// into the pre-vote path and lose the entire benefit of pre-vote.
enum class Role : std::uint8_t {
  kFollower = 0,
  kPreCandidate,
  kCandidate,
  kLeader,
};

const char* to_string(Role role) noexcept;

// ---------------------------------------------------------------------------
// log entries
// ---------------------------------------------------------------------------

enum class EntryType : std::uint8_t {
  kNormal = 0,      // an opaque state-machine command
  kNoop,            // the leader's own-term entry, appended on election
  kConfChange,      // enter/leave joint consensus; payload is an encoded ConfChange
};

const char* to_string(EntryType type) noexcept;

struct LogEntry {
  Term term{};
  LogIndex index{};
  EntryType type = EntryType::kNormal;
  std::string data;

  friend bool operator==(const LogEntry& a, const LogEntry& b) noexcept {
    return a.term == b.term && a.index == b.index && a.type == b.type && a.data == b.data;
  }
};

// ---------------------------------------------------------------------------
// durable state
// ---------------------------------------------------------------------------

// The three fields Raft requires to survive a crash. `commit` is not strictly
// required by the paper -- it can be recovered by re-replication -- but
// persisting it lets a restarted node apply its log without waiting for a
// leader, and makes "commit index never regresses" (INV-RAFT-08) checkable
// across restarts rather than vacuous.
struct HardState {
  Term term{};
  NodeId vote{};      // NodeId{0} == did not vote in this term
  LogIndex commit{};

  friend bool operator==(const HardState&, const HardState&) noexcept = default;
};

// ---------------------------------------------------------------------------
// snapshots
// ---------------------------------------------------------------------------

// A snapshot replaces the log prefix [1, index]. `config` travels with it
// because a node restored from a snapshot has no conf-change entries left to
// replay, and a node that does not know its own membership cannot compute a
// quorum -- which presents much later as a stuck election nobody can explain.
struct Snapshot {
  LogIndex index{};
  Term term{};
  std::string config;  // encoded ConfState
  std::string data;    // opaque state-machine bytes

  bool empty() const noexcept { return index.value() == 0; }
};

// ---------------------------------------------------------------------------
// membership
// ---------------------------------------------------------------------------

// Wire form of a configuration. Sorted vectors rather than sets so the encoding
// is canonical: two nodes must produce byte-identical bytes for the same
// membership or the snapshot digest differs across replicas for no reason.
struct ConfState {
  std::vector<std::uint64_t> voters;           // C_new (or C, outside a joint transition)
  std::vector<std::uint64_t> voters_outgoing;  // C_old; non-empty exactly while joint
  std::vector<std::uint64_t> learners;

  bool joint() const noexcept { return !voters_outgoing.empty(); }
  friend bool operator==(const ConfState&, const ConfState&) noexcept = default;
};

enum class ConfChangeKind : std::uint8_t {
  kEnterJoint = 0,  // C -> C_old,new. Carries the target membership.
  kLeaveJoint = 1,  // C_old,new -> C_new. Carries nothing; the target is already known.
};

struct ConfChange {
  ConfChangeKind kind = ConfChangeKind::kEnterJoint;
  std::vector<std::uint64_t> voters;    // target voters, for kEnterJoint
  std::vector<std::uint64_t> learners;  // target learners, for kEnterJoint
};

// ---------------------------------------------------------------------------
// messages
// ---------------------------------------------------------------------------

enum class RaftMessageType : std::uint8_t {
  kPreVote = 0,
  kPreVoteReply,
  kRequestVote,
  kRequestVoteReply,
  kAppend,
  kAppendReply,
  kHeartbeat,
  kHeartbeatReply,
  kInstallSnapshot,
  kInstallSnapshotReply,
  kTimeoutNow,
};

const char* to_string(RaftMessageType type) noexcept;

struct RaftMessage {
  // Which replication group this belongs to. One node runs many groups (one per
  // range, plus the placement driver's), and the transport between two nodes is
  // a single ordered link shared by all of them -- so the group has to travel on
  // the wire or the receiver cannot tell which state machine to step.
  //
  // It is encoded first, before the type byte, for two reasons: the transport
  // can demultiplex by decoding one varint instead of the whole message, and
  // group 0 -- which is never a real group -- is free to mark a coalesced batch.
  GroupId group{1};

  RaftMessageType type = RaftMessageType::kHeartbeat;
  NodeId from{};
  NodeId to{};
  Term term{};  // the sender's term, or for pre-vote the term it *would* use

  // append / vote
  LogIndex prev_index{};
  Term prev_term{};
  LogIndex commit{};
  std::vector<LogEntry> entries;

  // replies
  bool reject = false;
  LogIndex match{};         // AppendReply: highest index now known replicated
  LogIndex reject_hint{};   // AppendReply: the follower's suggestion for the next probe
  Term reject_term{};       // AppendReply: the conflicting term, for term-skipping

  // ReadIndex round-trips through heartbeats; zero means "no read attached".
  std::uint64_t read_context = 0;

  // The sender's clock reading when this message was put on the wire, echoed
  // verbatim by the reply. The leader's lease is computed from these, and the
  // echo is what makes it safe: crediting a reply to the *newest* outstanding
  // heartbeat instead would extend the lease using a round trip that never
  // happened, which is precisely the direction the bound must not be wrong in.
  std::uint64_t echo_time = 0;

  // Set on a vote request driven by leadership transfer. A voter honouring
  // CheckQuorum ignores ordinary vote requests while it can still see a leader;
  // a transfer is the leader's own instruction, so it overrides that.
  bool force = false;

  // snapshot chunking. `snapshot` carries metadata on every chunk so a follower
  // that missed the first one can reject coherently rather than assembling a
  // Frankenstein image out of two different snapshots.
  Snapshot snapshot;
  std::uint64_t chunk_offset = 0;
  std::uint64_t chunk_total = 0;  // total bytes in the snapshot payload
  bool last_chunk = false;
};

// ---------------------------------------------------------------------------
// reads
// ---------------------------------------------------------------------------

// The answer to a ReadIndex request: the read may be served once the local
// state machine has applied at least `index`.
struct ReadState {
  std::uint64_t context = 0;
  LogIndex index{};
};

// ---------------------------------------------------------------------------
// Ready
// ---------------------------------------------------------------------------

// One batch of work for the driver. The contract, in order:
//
//   1. If `truncate`, remove every durable entry at or after `truncate_from`.
//   2. Append `entries` to durable storage.
//   3. If `has_hard_state`, persist it.
//   4. fsync.
//   5. Only now, send `messages`.
//   6. Apply `committed` to the state machine, then call advance().
//
// Steps 1-4 before step 5 is the entire durability argument of the protocol.
struct Ready {
  bool has_hard_state = false;
  HardState hard_state;

  bool truncate = false;
  LogIndex truncate_from{};

  std::vector<LogEntry> entries;    // to persist
  std::vector<LogEntry> committed;  // to apply
  std::vector<RaftMessage> messages;
  std::vector<ReadState> read_states;

  // A snapshot received from the leader, to be handed to the state machine and
  // written durably before it counts as installed.
  bool has_snapshot = false;
  Snapshot snapshot;

  bool empty() const noexcept {
    return !has_hard_state && !truncate && entries.empty() && committed.empty() &&
           messages.empty() && read_states.empty() && !has_snapshot;
  }
};

// ---------------------------------------------------------------------------
// options
// ---------------------------------------------------------------------------

// Every knob a test needs to break on purpose. Defaults are the correct
// settings; the mutation drill flips them one at a time, and a suite that
// cannot tell the difference is a suite that proves nothing (ANV-0006).
struct RaftOptions {
  Duration tick_interval = Duration::millis(10);
  std::uint32_t election_timeout_ticks = 20;   // 200 ms base, randomised to 2x
  std::uint32_t heartbeat_timeout_ticks = 5;   // 50 ms

  bool pre_vote = true;
  bool check_quorum = true;

  // A leader may serve reads without a quorum round-trip until the lease
  // expires. Must be comfortably shorter than the minimum election timeout, or
  // a new leader can be elected while the old lease is still live -- which is
  // INV-RAFT-13 and the whole reason the bound is written down.
  Duration lease_duration = Duration::millis(120);
  Duration max_clock_uncertainty = Duration::millis(10);

  std::uint32_t max_entries_per_append = 16;  // batching
  std::uint32_t max_inflight_appends = 8;     // pipelining window, in messages
  std::uint64_t max_append_bytes = 64 * 1024;

  // Log compaction. Snapshot once this many applied entries sit above the last
  // snapshot; ship it in chunks of this size.
  std::uint64_t snapshot_threshold = 64;
  std::uint32_t snapshot_chunk_bytes = 512;

  // ---- deliberate-bug knobs (all default to correct) ----------------------
  // Each one is a seeded mutation with a name. See test/raft_faults.cc.
  bool persist_before_reply = true;      // vote and append durability before the reply
  bool commit_only_current_term = true;  // the Figure-8 rule (INV-RAFT-10)
  bool lease_uses_wall_clock = true;     // false: leases measured in ticks, so a pause
                                         //        cannot expire one (INV-RAFT-13)
  bool truncate_log_on_snapshot = true;  // false: install a snapshot without truncating
  bool joint_requires_commit = true;     // false: leave joint before C_old,new commits
  bool learners_excluded_from_quorum = true;
  bool check_prev_term_on_append = true;
  bool restrict_vote_by_log = true;      // the up-to-date check on RequestVote
};

}  // namespace anvil::raft

#endif  // ANVIL_CORE_RAFT_TYPES_H_
