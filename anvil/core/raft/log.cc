#include "anvil/core/raft/log.h"

#include <algorithm>

namespace anvil::raft {

LogIndex RaftLog::last_index() const noexcept {
  return entries_.empty() ? snapshot_index_ : entries_.back().index;
}

Term RaftLog::last_term() const noexcept {
  return entries_.empty() ? snapshot_term_ : entries_.back().term;
}

std::size_t RaftLog::offset_of(LogIndex index) const {
  return static_cast<std::size_t>(index.value() - snapshot_index_.value() - 1);
}

bool RaftLog::term_at(LogIndex index, Term* out) const {
  if (index == snapshot_index_) {
    *out = snapshot_term_;
    return true;
  }
  if (index.value() <= snapshot_index_.value()) return false;  // compacted away
  if (index > last_index()) return false;
  *out = entries_[offset_of(index)].term;
  return true;
}

bool RaftLog::match(LogIndex index, Term term) const {
  Term found{};
  return term_at(index, &found) && found == term;
}

const LogEntry* RaftLog::at(LogIndex index) const {
  if (index.value() <= snapshot_index_.value() || index > last_index()) return nullptr;
  return &entries_[offset_of(index)];
}

bool RaftLog::is_up_to_date(LogIndex index, Term term) const {
  const Term mine = last_term();
  if (term != mine) return term > mine;
  return index >= last_index();
}

LogIndex RaftLog::find_conflict_by_term(LogIndex index, Term term) const {
  if (index > last_index()) index = last_index();
  for (;;) {
    Term found{};
    if (!term_at(index, &found)) break;  // below the snapshot; nothing more to say
    if (found <= term) break;
    if (index.value() == 0) break;
    index = LogIndex{index.value() - 1};
  }
  return index;
}

std::vector<LogEntry> RaftLog::slice(LogIndex from, std::uint32_t max_count,
                                     std::uint64_t max_bytes) const {
  std::vector<LogEntry> out;
  if (from.value() <= snapshot_index_.value()) return out;
  if (from > last_index()) return out;
  std::uint64_t bytes = 0;
  for (std::size_t i = offset_of(from); i < entries_.size(); ++i) {
    if (out.size() >= max_count) break;
    // The byte budget is checked *after* the first entry is admitted, so a
    // single entry larger than the budget still makes progress. A strict
    // budget stalls replication forever on one oversized command, which
    // presents as a follower that lags permanently for no visible reason.
    if (!out.empty() && bytes + entries_[i].data.size() > max_bytes) break;
    bytes += entries_[i].data.size();
    out.push_back(entries_[i]);
  }
  return out;
}

std::vector<LogEntry> RaftLog::unstable() const {
  if (persisted_ >= last_index()) return {};
  return slice(LogIndex{persisted_.value() + 1}, UINT32_MAX, UINT64_MAX);
}

std::vector<LogEntry> RaftLog::next_applicable() const {
  if (applied_ >= commit_) return {};
  const LogIndex from{applied_.value() + 1};
  if (from.value() <= snapshot_index_.value()) return {};
  std::vector<LogEntry> out;
  for (std::size_t i = offset_of(from); i < entries_.size(); ++i) {
    if (entries_[i].index > commit_) break;
    out.push_back(entries_[i]);
  }
  return out;
}

void RaftLog::append(const std::vector<LogEntry>& entries) {
  for (const LogEntry& e : entries) entries_.push_back(e);
}

bool RaftLog::try_append(LogIndex prev_index, Term prev_term,
                         const std::vector<LogEntry>& entries, LogIndex* hint_index,
                         Term* hint_term, bool check_prev_term) {
  *hint_index = LogIndex{0};
  *hint_term = Term{0};

  // The consistency check. A follower accepts entries only if it has the
  // leader's previous entry, with the leader's term for it. Skipping this is
  // seeded mutation "append accepted with a mismatched previous term", and the
  // resulting divergence is invisible at the API until a leader change.
  // Backtrack by term, not by index. The hint is the highest index at which
  // this log could still agree with a leader whose entry at `prev_index` has
  // term `prev_term`; everything above it is a divergent run the leader can
  // skip in a single round trip.
  const auto reject = [&]() {
    const LogIndex probe =
        find_conflict_by_term(LogIndex{std::min(prev_index.value(), last_index().value())},
                              prev_term);
    Term at_probe{};
    term_at(probe, &at_probe);
    *hint_index = probe;
    *hint_term = at_probe;
    return false;
  };

  if (prev_index > last_index()) return reject();
  if (check_prev_term) {
    // No exemption for the snapshot boundary. An earlier version skipped the
    // check when prev_index was at or below snapshot_index_, on the reasoning
    // that there is nothing left to compare -- but term_at() answers for the
    // snapshot index itself, and skipping it let a stale leader append over the
    // entries immediately above a snapshot without ever proving it agreed with
    // the snapshot. Below the snapshot the honest answer is "I cannot verify
    // this", which is a rejection, not an acceptance.
    Term local{};
    if (!term_at(prev_index, &local)) return reject();
    if (local != prev_term) return reject();
  }

  // Accept. Entries already present with the same term are skipped rather than
  // rewritten: a duplicate AppendEntries (the network model produces them on
  // purpose) must not truncate a log that is ahead of it.
  for (const LogEntry& e : entries) {
    if (e.index <= last_index()) {
      Term local{};
      if (term_at(e.index, &local) && local == e.term) continue;
      // A genuine conflict: same index, different term. Everything from here
      // on is wrong and must go, including entries the follower may have
      // already made durable.
      truncate_suffix(e.index);
    }
    if (e.index != LogIndex{last_index().value() + 1}) {
      // A gap. Only reachable if the leader sent a discontinuous batch, which
      // would be a protocol bug rather than a network fault -- refuse it
      // instead of building a log with a hole in it.
      *hint_index = LogIndex{last_index().value() + 1};
      return false;
    }
    entries_.push_back(e);
  }
  return true;
}

void RaftLog::truncate_suffix(LogIndex from) {
  if (from > last_index()) return;
  if (from.value() <= snapshot_index_.value()) return;
  entries_.resize(offset_of(from));

  // Anything that has reached the file at or above `from` has to be removed
  // from it, and before the replacement entries are appended -- otherwise
  // recovery reads the old suffix behind the new one and rebuilds a log that
  // never existed on any node.
  //
  // The comparison is against `writing_`, not `persisted_`. Records handed to
  // the driver are already being written while the fsync is outstanding; a
  // truncation that arrives in that window sees persisted_ still low, concludes
  // there is nothing on disk to remove, and leaves them there. The replacement
  // entries are then appended *after* them, and the file ends up holding the
  // same index twice -- which recovery reads back as a log with a duplicate,
  // and the node comes up with a history no leader ever produced.
  if (std::max(persisted_, writing_) >= from) {
    if (!pending_truncate_ || from < truncate_from_) truncate_from_ = from;
    pending_truncate_ = true;
  }
  if (persisted_ >= from) persisted_ = LogIndex{from.value() - 1};
  if (writing_ >= from) writing_ = LogIndex{from.value() - 1};
}

void RaftLog::commit_to(LogIndex index) {
  // Clamped, deliberately. A leader's commit index legitimately runs ahead of a
  // follower's log tail; applying to it blindly would apply entries the
  // follower does not have.
  const LogIndex bounded{std::min(index.value(), last_index().value())};
  if (bounded > commit_) commit_ = bounded;
}

void RaftLog::apply_to(LogIndex index) {
  if (index > applied_) applied_ = std::min(index, commit_);
}

void RaftLog::mark_persisted(LogIndex index) {
  std::uint64_t bound = std::min(index.value(), last_index().value());

  // Never past an outstanding truncation point.
  //
  // The driver takes a Ready, writes its entries, and awaits the fsync. That
  // await is a real suspension: a message can arrive during it, conflict, and
  // truncate the log the batch was written from. When the fsync returns, the
  // indices in that batch no longer describe the entries now in memory -- the
  // file holds the old ones and the log holds the new ones -- so marking them
  // persisted claims durability for entries that were never written.
  //
  // A follower then acknowledges them, the leader counts the acknowledgement
  // toward a quorum, and one crash later a committed entry is gone from a node
  // that swore it had it. Clamping here is the whole fix: everything at or
  // above the truncation point is not durable until it is rewritten, which the
  // very next Ready does.
  if (pending_truncate_ && truncate_from_.value() > 0) {
    bound = std::min(bound, truncate_from_.value() - 1);
  }
  if (bound > persisted_.value()) persisted_ = LogIndex{bound};
  if (persisted_ > writing_) writing_ = persisted_;
}

void RaftLog::mark_writing(LogIndex index) {
  const std::uint64_t bound = std::min(index.value(), last_index().value());
  if (bound > writing_.value()) writing_ = LogIndex{bound};
}

void RaftLog::restore_snapshot(LogIndex index, Term term) {
  // A snapshot from the leader supersedes everything. Even entries above the
  // snapshot index are discarded unless they are provably consistent with it:
  // the follower cannot verify the prefix any more, so keeping a tail it can no
  // longer justify is exactly the "snapshot installed without truncating the
  // log" bug (INV-RAFT-11).
  const bool consistent = match(index, term);
  if (consistent && index < last_index()) {
    entries_.erase(entries_.begin(), entries_.begin() + static_cast<std::ptrdiff_t>(
                                                            offset_of(index) + 1));
  } else {
    entries_.clear();
  }
  snapshot_index_ = index;
  snapshot_term_ = term;
  if (commit_ < index) commit_ = index;
  if (applied_ < index) applied_ = index;
  // The whole durable log is being replaced, so nothing above the snapshot is
  // durable, or on its way to being durable, until it is rewritten.
  persisted_ = index;
  writing_ = index;
  pending_truncate_ = true;
  truncate_from_ = LogIndex{index.value() + 1};
}

void RaftLog::compact(LogIndex index, Term term) {
  if (index.value() <= snapshot_index_.value()) return;
  if (index > applied_) return;  // never discard what the state machine has not applied
  if (index > last_index()) return;
  entries_.erase(entries_.begin(),
                 entries_.begin() + static_cast<std::ptrdiff_t>(offset_of(index) + 1));
  snapshot_index_ = index;
  snapshot_term_ = term;
}

}  // namespace anvil::raft
