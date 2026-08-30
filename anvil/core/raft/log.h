// The replicated log, as the state machine sees it.
//
// One vector of entries sitting above a snapshot base. Index 0 does not exist:
// the log starts at 1, and `snapshot_index_` is the index of the last entry the
// snapshot subsumes, so `term_at(snapshot_index_)` is answerable even though the
// entry itself is gone. Losing that one term is how a compacted follower stops
// being able to validate any AppendEntries and falls back to a full snapshot
// transfer forever.
//
// Three indices move independently and are constantly confused for one another,
// so they are named after what they mean rather than after where they are:
//
//   persisted_  the highest index that survived an fsync. Not "written".
//   commit_     the highest index a quorum is known to hold durably.
//   applied_    the highest index handed to the state machine.
//
// persisted_ >= commit_ is NOT an invariant on a follower -- the leader may have
// told it about a commit index beyond its own log tail -- so commit_ is clamped
// on every advance. Getting that clamp wrong produces a follower that applies
// entries it does not have, which is a null dereference in the best case and
// silent divergence in the worst.

#ifndef ANVIL_CORE_RAFT_LOG_H_
#define ANVIL_CORE_RAFT_LOG_H_

#include <cstdint>
#include <vector>

#include "anvil/core/raft/types.h"
#include "anvil/core/types.h"

namespace anvil::raft {

class RaftLog {
 public:
  RaftLog() = default;

  // ---- geometry ----------------------------------------------------------
  LogIndex first_index() const noexcept { return LogIndex{snapshot_index_.value() + 1}; }
  LogIndex last_index() const noexcept;
  Term last_term() const noexcept;
  LogIndex snapshot_index() const noexcept { return snapshot_index_; }
  Term snapshot_term() const noexcept { return snapshot_term_; }
  std::size_t size() const noexcept { return entries_.size(); }

  LogIndex commit_index() const noexcept { return commit_; }
  LogIndex applied_index() const noexcept { return applied_; }
  LogIndex persisted_index() const noexcept { return persisted_; }

  // ---- reads -------------------------------------------------------------
  // Returns false when the index is below the snapshot (compacted away) or
  // above the tail. Callers must check: "term 0" is not a safe sentinel to
  // compare against, because a real term is never 0 but an unavailable one and
  // a mismatching one need different responses.
  bool term_at(LogIndex index, Term* out) const;
  bool match(LogIndex index, Term term) const;
  const LogEntry* at(LogIndex index) const;

  // Raft's log-comparison rule: later term wins, then longer log wins.
  bool is_up_to_date(LogIndex index, Term term) const;

  // The largest index at or below `index` whose term is at or below `term`.
  //
  // Both sides of the conflict protocol use this, and they have to use the
  // *same* function or the backtracking degenerates. The follower reports where
  // its divergent run begins; the leader jumps to where its own log could still
  // agree. Two different heuristics either overshoot -- costing a round trip per
  // entry, which is hours for a follower that has been away -- or undershoot,
  // and then the probe never converges at all.
  LogIndex find_conflict_by_term(LogIndex index, Term term) const;

  // Entries in [from, ...] bounded by count and byte budget. Empty if `from` is
  // at or below the snapshot, which is the caller's signal to send a snapshot
  // instead.
  std::vector<LogEntry> slice(LogIndex from, std::uint32_t max_count,
                              std::uint64_t max_bytes) const;

  // Entries the driver has not yet made durable.
  std::vector<LogEntry> unstable() const;

  // Committed but not yet applied.
  std::vector<LogEntry> next_applicable() const;

  // ---- writes ------------------------------------------------------------
  // Leader-side append. Entries arrive with term and index already assigned.
  void append(const std::vector<LogEntry>& entries);

  // Follower-side append with the consistency check.
  //
  // Returns true if the entries were accepted (which includes the case where
  // they were already present and nothing changed). On rejection, fills the
  // hints the leader uses to backtrack by term rather than one index at a time:
  // a follower that is 10,000 entries behind must not cost 10,000 round trips.
  bool try_append(LogIndex prev_index, Term prev_term, const std::vector<LogEntry>& entries,
                  LogIndex* hint_index, Term* hint_term, bool check_prev_term);

  // Removes every entry at or above `from`. Records the lowest index removed so
  // the driver can shorten the durable file before appending anything new.
  void truncate_suffix(LogIndex from);

  void commit_to(LogIndex index);
  void apply_to(LogIndex index);
  void mark_persisted(LogIndex index);

  // The highest index that has been handed to the driver to write. Distinct
  // from persisted_ by exactly the width of one fsync, and that gap is where a
  // whole class of bug lives: a truncation arriving while a write is in flight
  // must still shorten the *file*, even though nothing at that index has been
  // acknowledged as durable yet. Tracking only what has landed leaves the
  // in-flight records behind, and recovery then reads a log with the same index
  // in it twice.
  void mark_writing(LogIndex index);
  LogIndex writing_index() const noexcept { return writing_; }

  // ---- snapshots ---------------------------------------------------------
  // Replaces everything at or below `index`. Entries above it are kept when the
  // snapshot is consistent with the local log (a leader compacting), and
  // discarded when it is not (a follower installing a leader's snapshot).
  void restore_snapshot(LogIndex index, Term term);

  // Log compaction: drop the prefix up to and including `index`. Only ever
  // called with an index the state machine has applied AND that is covered by a
  // durable snapshot -- INV-RAFT-11 is exactly this precondition.
  void compact(LogIndex index, Term term);

  // ---- durability bookkeeping for the driver ------------------------------
  bool has_pending_truncate() const noexcept { return pending_truncate_; }
  LogIndex pending_truncate_from() const noexcept { return truncate_from_; }
  void clear_pending_truncate() noexcept {
    pending_truncate_ = false;
    truncate_from_ = LogIndex{0};
  }

 private:
  std::size_t offset_of(LogIndex index) const;

  std::vector<LogEntry> entries_;
  LogIndex snapshot_index_{};
  Term snapshot_term_{};
  LogIndex commit_{};
  LogIndex applied_{};
  LogIndex persisted_{};
  LogIndex writing_{};

  bool pending_truncate_ = false;
  LogIndex truncate_from_{};
};

}  // namespace anvil::raft

#endif  // ANVIL_CORE_RAFT_LOG_H_
