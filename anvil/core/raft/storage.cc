#include "anvil/core/raft/storage.h"

#include "anvil/core/lsm/format.h"
#include "anvil/core/lsm/wal.h"

namespace anvil::raft {
namespace {

constexpr std::size_t kRecordHeaderSize = 8;  // must match lsm::wal's framing
constexpr const char* kDir = "raft";

// One set of files per (node, group). A node hosts a replica of many ranges and
// each is a separate log, so the group has to be in the path -- two groups
// sharing a log file is not a subtle bug, it is one replica's entries appearing
// in another replica's recovery.
std::string node_path(NodeId node, GroupId group, const char* suffix) {
  return std::string{kDir} + "/n" + std::to_string(node.value()) + "g" +
         std::to_string(group.value()) + suffix;
}

}  // namespace

// ---------------------------------------------------------------------------
// encoding
// ---------------------------------------------------------------------------

std::string encode_entry(const LogEntry& entry) {
  std::string out;
  lsm::put_varint64(&out, entry.term.value());
  lsm::put_varint64(&out, entry.index.value());
  out.push_back(static_cast<char>(entry.type));
  lsm::put_length_prefixed(&out, entry.data);
  return out;
}

bool decode_entry(std::string_view in, LogEntry* out) {
  const char* p = in.data();
  const char* limit = p + in.size();
  std::uint64_t term = 0;
  std::uint64_t index = 0;
  p = lsm::get_varint64(p, limit, &term);
  if (p == nullptr) return false;
  p = lsm::get_varint64(p, limit, &index);
  if (p == nullptr) return false;
  if (p >= limit) return false;
  const auto type = static_cast<std::uint8_t>(*p++);
  if (type > static_cast<std::uint8_t>(EntryType::kConfChange)) return false;
  std::string_view data;
  p = lsm::get_length_prefixed(p, limit, &data);
  if (p == nullptr) return false;
  out->term = Term{term};
  out->index = LogIndex{index};
  out->type = static_cast<EntryType>(type);
  out->data.assign(data);
  return true;
}

std::string encode_hard_state(const HardState& hard) {
  std::string out;
  lsm::put_varint64(&out, hard.term.value());
  lsm::put_varint64(&out, hard.vote.value());
  lsm::put_varint64(&out, hard.commit.value());
  return out;
}

bool decode_hard_state(std::string_view in, HardState* out) {
  const char* p = in.data();
  const char* limit = p + in.size();
  std::uint64_t values[3] = {};
  for (std::uint64_t& value : values) {
    p = lsm::get_varint64(p, limit, &value);
    if (p == nullptr) return false;
  }
  out->term = Term{values[0]};
  out->vote = NodeId{values[1]};
  out->commit = LogIndex{values[2]};
  return true;
}

std::string encode_snapshot(const Snapshot& snapshot) {
  std::string out;
  lsm::put_varint64(&out, snapshot.index.value());
  lsm::put_varint64(&out, snapshot.term.value());
  lsm::put_length_prefixed(&out, snapshot.config);
  lsm::put_length_prefixed(&out, snapshot.data);
  return out;
}

bool decode_snapshot(std::string_view in, Snapshot* out) {
  const char* p = in.data();
  const char* limit = p + in.size();
  std::uint64_t index = 0;
  std::uint64_t term = 0;
  p = lsm::get_varint64(p, limit, &index);
  if (p == nullptr) return false;
  p = lsm::get_varint64(p, limit, &term);
  if (p == nullptr) return false;
  std::string_view config;
  std::string_view data;
  p = lsm::get_length_prefixed(p, limit, &config);
  if (p == nullptr) return false;
  p = lsm::get_length_prefixed(p, limit, &data);
  if (p == nullptr) return false;
  out->index = LogIndex{index};
  out->term = Term{term};
  out->config.assign(config);
  out->data.assign(data);
  return true;
}

// ---------------------------------------------------------------------------
// files
// ---------------------------------------------------------------------------

Task<Status> RaftStorage::open_files() {
  const OpenFlags flags = OpenFlags::kRead | OpenFlags::kWrite | OpenFlags::kCreate;

  FileHandle log{};
  Status status = co_await runtime_->open(node_path(node_, group_, ".log"), flags, &log);
  if (!status.is_ok()) co_return status;
  FileHandle state{};
  status = co_await runtime_->open(node_path(node_, group_, ".state"), flags, &state);
  if (!status.is_ok()) {
    co_await runtime_->close_file(log);
    co_return status;
  }

  // The directory entry becomes durable here and nowhere else. Contents can be
  // fsynced flawlessly and the file still will not exist after a crash --
  // ANV-EX02 in miniature, and the reason the disk model tracks directory
  // entries separately from contents.
  if (durability_.fsync_dir_on_create) {
    const Status dir = co_await runtime_->fsync_dir(kDir);
    if (!dir.is_ok()) {
      co_await runtime_->close_file(log);
      co_await runtime_->close_file(state);
      co_return dir;
    }
  }

  log_ = log;
  state_ = state;
  open_ = true;
  co_return Status::ok();
}

Task<Status> RaftStorage::recover(RecoveredState* out) {
  *out = RecoveredState{};
  if (open_) co_await close();

  Status status = co_await open_files();
  if (!status.is_ok()) co_return status;

  // ---- snapshot ----------------------------------------------------------
  FileHandle snap{};
  status = co_await runtime_->open(node_path(node_, group_, ".snap"), OpenFlags::kRead, &snap);
  if (status.is_ok()) {
    lsm::WalReadResult snap_result;
    const Status read = co_await lsm::wal_read_all(runtime_, snap, &snap_result);
    co_await runtime_->close_file(snap);
    // A device error reading the snapshot is not "there is no snapshot". Fail
    // the whole recovery so the caller retries; treating EIO as absence would
    // silently roll the node back to an empty state machine.
    if (!read.is_ok()) co_return read;
    if (!snap_result.records.empty()) {
      if (!decode_snapshot(snap_result.records.back(), &out->snapshot)) {
        out->snapshot_corrupt = true;
        out->snapshot = Snapshot{};
      }
    }
    if (snap_result.truncated) out->snapshot_corrupt = true;
  } else if (status.code() != StatusCode::kNotFound) {
    co_return status;
  }

  // ---- hard state --------------------------------------------------------
  lsm::WalReadResult state_result;
  status = co_await lsm::wal_read_all(runtime_, state_, &state_result);
  if (!status.is_ok()) co_return status;
  out->state_truncated = state_result.truncated;
  out->records_discarded += state_result.records_discarded;
  for (auto it = state_result.records.rbegin(); it != state_result.records.rend(); ++it) {
    if (decode_hard_state(*it, &out->hard)) break;
  }
  state_offset_ = state_result.valid_bytes;
  // The truncation is *deferred* to the end of recovery. See the note there.

  // ---- log ---------------------------------------------------------------
  lsm::WalReadResult log_result;
  status = co_await lsm::wal_read_all(runtime_, log_, &log_result);
  if (!status.is_ok()) co_return status;
  out->log_truncated = log_result.truncated;
  if (log_result.truncated) out->truncate_reason = "record-invalid-or-partial";
  out->records_discarded += log_result.records_discarded;

  // Rebuilding the in-memory log from the file, with three rules.
  //
  //   1. Records at or below the snapshot index are superseded and dropped.
  //      They exist when a crash landed between writing a snapshot and
  //      rewriting the log, which is a perfectly ordinary window.
  //
  //   2. What remains must be contiguous and must start exactly one past the
  //      snapshot. A gap or a repeated index means the file is not a log; the
  //      honest response is to treat the rest as damaged rather than to
  //      assemble a history no leader ever produced.
  //
  //   3. The first surviving entry's term must not be *below* the snapshot's.
  //      Terms never decrease along a log, so a tail at term 2 above a snapshot
  //      from term 3 is a stale tail the snapshot has already replaced -- and
  //      keeping it produces a log whose terms run backwards, which every
  //      consistency check downstream will then faithfully trust.
  offsets_.clear();
  std::uint64_t offset = 0;
  std::uint64_t expected = out->snapshot.empty() ? 1 : out->snapshot.index.value() + 1;
  bool stop = false;

  for (const std::string& record : log_result.records) {
    LogEntry entry;
    if (!decode_entry(record, &entry)) {
      // A record that passed its CRC and does not decode is a format bug, not
      // media damage. Stop here: everything after it is unanchored.
      out->log_truncated = true;
      out->truncate_reason = "decode-failed";
      break;
    }
    if (!out->snapshot.empty() && entry.index <= out->snapshot.index) {
      offset += kRecordHeaderSize + record.size();
      continue;
    }
    if (out->entries.empty() && !out->snapshot.empty() &&
        entry.term < out->snapshot.term) {
      stop = true;  // rule 3: a tail the snapshot has already superseded
    } else if (entry.index.value() != expected) {
      stop = true;  // rule 2
    }
    if (stop) {
      out->log_truncated = true;
      out->truncate_reason = "non-contiguous-or-stale-tail";
      break;
    }
    offsets_.emplace_back(entry.index.value(), offset);
    offset += kRecordHeaderSize + record.size();
    ++expected;
    out->entries.push_back(std::move(entry));
  }
  log_offset_ = offset;
  rollback_pending_ = false;

  // Recovery is read-only until here.
  //
  // It has to be. These two truncations cut away the garbage tails so that the
  // next append does not write *after* them -- necessary, but destructive, and
  // an earlier version did them the moment each file had been read. A device
  // error anywhere later then returned failure to a caller that retries
  // recovery unboundedly (ANV-0003), and the retry read a file that recovery
  // itself had already cut down. One EIO on an fsync was enough to turn a
  // healthy 43-entry log into a one-entry log, with the second attempt
  // reporting a clean, undamaged read of exactly what the first attempt had
  // left behind.
  //
  // Deferring both to the end makes recovery idempotent: everything above this
  // point is pure computation over bytes that are still all there, and the
  // offsets applied below are the same ones any retry would compute.
  if (state_result.truncated) {
    Status trunc = co_await runtime_->ftruncate(state_, state_offset_);
    if (trunc.is_ok() && durability_.fsync_state) trunc = co_await runtime_->fsync(state_);
    if (!trunc.is_ok()) co_return trunc;
  }
  if (log_offset_ < log_result.valid_bytes || log_result.truncated) {
    Status trunc = co_await runtime_->ftruncate(log_, log_offset_);
    if (trunc.is_ok() && durability_.fsync_log) trunc = co_await runtime_->fsync(log_);
    if (!trunc.is_ok()) co_return trunc;
  }

  co_return Status::ok();
}

Task<Status> RaftStorage::append(const std::vector<LogEntry>& entries) {
  if (!open_) co_return Status{StatusCode::kUnavailable, "storage not open"};

  // All or nothing.
  //
  // A batch that fails halfway must leave the file exactly as it found it. The
  // caller does not advance anything on failure, so the next attempt writes the
  // *same* entries again -- and without the rollback it writes them after the
  // records the failed attempt already left behind. The file then holds a
  // repeated run of indices, recovery stops at the repeat, and every entry
  // above it is gone. That is an acknowledged, committed, genuinely durable
  // entry lost to a transient EIO and a retry, with no corruption anywhere.
  // Finish any rollback the previous attempt could not complete. Appending on
  // top of records a failed append left behind is how a durable, fsynced entry
  // ends up unreadable: the next batch is shorter, the leftovers survive it, and
  // recovery stops at the repeated index -- discarding everything above.
  if (rollback_pending_) {
    const Status trunc = co_await runtime_->ftruncate(log_, rollback_to_);
    if (!trunc.is_ok()) co_return trunc;
    rollback_pending_ = false;
    log_offset_ = rollback_to_;
  }

  // Appending an index the file already holds means a previous attempt at this
  // same batch got that far and then failed *after* the write -- in the hard
  // state, or in the fsync. The caller advances nothing on failure, so it hands
  // the identical entries back, and without this the file ends up with the run
  // of indices written twice. Recovery stops at the repeat and discards every
  // entry above it: acknowledged, committed, fsynced data gone, with no crash
  // and no corruption anywhere in the run (ANV-0023).
  //
  // Rewinding to where those entries start makes the whole persist path
  // idempotent under retry, which is the property the caller was already
  // assuming it had.
  if (!entries.empty()) {
    const Status rewind = co_await truncate_suffix(entries.front().index);
    if (!rewind.is_ok()) co_return rewind;
  }

  const std::uint64_t start = log_offset_;
  const std::size_t offsets_before = offsets_.size();

  lsm::WalWriter writer{runtime_, log_, log_offset_};
  for (const LogEntry& entry : entries) {
    const std::uint64_t at = writer.offset();
    const Status status = co_await writer.append(encode_entry(entry));
    if (!status.is_ok()) {
      offsets_.resize(offsets_before);
      log_offset_ = start;
      // Sync the rollback immediately. Every ftruncate is a size change, and an
      // unsynced size change leaves the whole file indeterminate until the next
      // fsync -- a crash in that window splices the surviving prefix onto
      // whatever the old tail held.
      Status trunc = co_await runtime_->ftruncate(log_, start);
      if (trunc.is_ok() && durability_.fsync_log) trunc = co_await runtime_->fsync(log_);
      if (!trunc.is_ok()) {
        // The rollback could not be completed now. Remember it and refuse to
        // append again until it has been; the contiguity rule in recover() is
        // the last-resort backstop if the process dies in between.
        rollback_pending_ = true;
        rollback_to_ = start;
      }
      co_return status;
    }
    offsets_.emplace_back(entry.index.value(), at);
    log_dirty_ = true;
  }
  log_offset_ = writer.offset();
  co_return Status::ok();
}

Task<Status> RaftStorage::truncate_suffix(LogIndex from) {
  if (!open_) co_return Status{StatusCode::kUnavailable, "storage not open"};
  std::uint64_t cut = log_offset_;
  std::size_t keep = offsets_.size();
  for (std::size_t i = 0; i < offsets_.size(); ++i) {
    if (offsets_[i].first >= from.value()) {
      cut = offsets_[i].second;
      keep = i;
      break;
    }
  }
  if (keep == offsets_.size() && cut == log_offset_) co_return Status::ok();

  const Status status = co_await runtime_->ftruncate(log_, cut);
  if (!status.is_ok()) co_return status;
  offsets_.resize(keep);
  log_offset_ = cut;
  log_dirty_ = true;

  // Sync the truncation immediately, before a single new entry is written.
  //
  // A truncation is not a local edit to the tail: until it is durable, the
  // whole file is in an indeterminate state -- the disk model marks every
  // sector dirty, which is a fair reading of what a filesystem is entitled to
  // do with a size change that has not been committed. A crash anywhere in the
  // window between the ftruncate and the next fsync can therefore revert
  // sectors this node made durable long ago, and it comes back having lost
  // entries from far below the truncation point.
  //
  // Deferring this to the batch's own fsync leaves that window open for the
  // whole append, which is several simulated milliseconds of disk latency and
  // exactly where the crashes land.
  if (durability_.fsync_log) {
    const Status sync_status = co_await runtime_->fsync(log_);
    if (!sync_status.is_ok()) co_return sync_status;
    log_dirty_ = false;
  }
  co_return Status::ok();
}

Task<Status> RaftStorage::put_hard_state(const HardState& hard) {
  if (!open_) co_return Status{StatusCode::kUnavailable, "storage not open"};
  const std::uint64_t start = state_offset_;
  lsm::WalWriter writer{runtime_, state_, state_offset_};
  const Status status = co_await writer.append(encode_hard_state(hard));
  if (!status.is_ok()) {
    // Same rollback rule as the log. A half-written hard-state record left in
    // place would be read back as the tail on recovery, and the retry would
    // append a second one after it.
    state_offset_ = start;
    const Status trunc = co_await runtime_->ftruncate(state_, start);
    if (trunc.is_ok() && durability_.fsync_state) co_await runtime_->fsync(state_);
    co_return status;
  }
  state_offset_ = writer.offset();
  state_dirty_ = true;
  co_return Status::ok();
}

Task<Status> RaftStorage::sync() {
  if (!open_) co_return Status{StatusCode::kUnavailable, "storage not open"};
  if (log_dirty_ && durability_.fsync_log) {
    const Status status = co_await runtime_->fsync(log_);
    if (!status.is_ok()) co_return status;
  }
  if (state_dirty_ && durability_.fsync_state) {
    const Status status = co_await runtime_->fsync(state_);
    if (!status.is_ok()) co_return status;
  }
  log_dirty_ = false;
  state_dirty_ = false;
  co_return Status::ok();
}

Task<Status> RaftStorage::rewrite_log(const std::vector<LogEntry>& entries) {
  if (!open_) co_return Status{StatusCode::kUnavailable, "storage not open"};

  // Into a temporary file, then renamed over the old one. Not truncate-and-
  // rewrite-in-place, which is what this used to do, on the reasoning that a
  // crash mid-rewrite "loses a suffix, which is what a crash mid-append would
  // have done". That reasoning is wrong and the simulator proved it.
  //
  // A truncation leaves every sector of the file unsynced. The new contents are
  // shorter than the old, so the sectors past the new end are never rewritten
  // at all -- and a crash there resolves them by leaving the *old* bytes in
  // place. The file comes back as the new log followed by the tail of the old
  // one: records at indices the new prefix already used, which recovery reads
  // as a repeated index and truncates at. Entries the node had fsynced and
  // acknowledged are gone, with no torn write and no bit flip anywhere in the
  // run.
  //
  // write -> fsync -> rename -> fsync_dir makes the replacement atomic, which
  // is the same discipline the LSM's CURRENT file uses for the same reason.
  const std::string final_path = node_path(node_, group_, ".log");
  const std::string tmp_path = node_path(node_, group_, ".log.tmp");

  FileHandle tmp{};
  const OpenFlags flags =
      OpenFlags::kRead | OpenFlags::kWrite | OpenFlags::kCreate | OpenFlags::kTruncate;
  Status status = co_await runtime_->open(tmp_path, flags, &tmp);
  if (!status.is_ok()) co_return status;

  std::vector<std::pair<std::uint64_t, std::uint64_t>> rebuilt;
  lsm::WalWriter writer{runtime_, tmp, 0};
  for (const LogEntry& entry : entries) {
    const std::uint64_t at = writer.offset();
    status = co_await writer.append(encode_entry(entry));
    if (!status.is_ok()) {
      co_await runtime_->close_file(tmp);
      co_return status;
    }
    rebuilt.emplace_back(entry.index.value(), at);
  }
  if (durability_.fsync_log) {
    status = co_await writer.sync();
    if (!status.is_ok()) {
      co_await runtime_->close_file(tmp);
      co_return status;
    }
  }
  const std::uint64_t rewritten_size = writer.offset();
  co_await runtime_->close_file(tmp);

  // The temporary file's *directory entry* has to be durable before the rename,
  // not only after it. A crash in the window between the two erases a file
  // whose entry was never persisted -- and because this rename replaces the
  // log, what is lost is not a suffix but the entire thing: the node comes back
  // with an empty log and no snapshot to fall back on.
  if (durability_.fsync_dir_on_create) {
    status = co_await runtime_->fsync_dir(kDir);
    if (!status.is_ok()) co_return status;
  }

  status = co_await runtime_->rename(tmp_path, final_path);
  if (!status.is_ok()) co_return status;
  if (durability_.fsync_dir_on_create) {
    status = co_await runtime_->fsync_dir(kDir);
    if (!status.is_ok()) co_return status;
  }

  // The old handle names a file that no longer exists under that path.
  co_await runtime_->close_file(log_);
  FileHandle reopened{};
  status = co_await runtime_->open(final_path,
                                   OpenFlags::kRead | OpenFlags::kWrite | OpenFlags::kCreate,
                                   &reopened);
  if (!status.is_ok()) {
    open_ = false;  // the caller retries recovery, which reopens everything
    co_return status;
  }

  log_ = reopened;
  offsets_ = std::move(rebuilt);
  log_offset_ = rewritten_size;
  rollback_pending_ = false;
  log_dirty_ = false;
  co_return Status::ok();
}

Task<Status> RaftStorage::save_snapshot(const Snapshot& snapshot) {
  const std::string final_path = node_path(node_, group_, ".snap");
  const std::string tmp_path = node_path(node_, group_, ".snap.tmp");

  FileHandle tmp{};
  const OpenFlags flags =
      OpenFlags::kRead | OpenFlags::kWrite | OpenFlags::kCreate | OpenFlags::kTruncate;
  Status status = co_await runtime_->open(tmp_path, flags, &tmp);
  if (!status.is_ok()) co_return status;

  lsm::WalWriter writer{runtime_, tmp, 0};
  status = co_await writer.append(encode_snapshot(snapshot));
  if (status.is_ok() && durability_.fsync_snapshot) status = co_await writer.sync();
  if (!status.is_ok()) {
    co_await runtime_->close_file(tmp);
    co_return status;
  }
  co_await runtime_->close_file(tmp);

  // write -> fsync -> fsync_dir -> rename -> fsync_dir. Dropping the last step
  // leaves a perfectly synced file with no durable directory entry, which is
  // the canonical "the data is there and the filesystem disagrees" failure.
  // Dropping the *first* fsync_dir loses the file to a crash inside the rename
  // window, which is the same failure one step earlier.
  if (durability_.fsync_dir_on_create) {
    status = co_await runtime_->fsync_dir(kDir);
    if (!status.is_ok()) co_return status;
  }

  status = co_await runtime_->rename(tmp_path, final_path);
  if (!status.is_ok()) co_return status;
  if (durability_.fsync_dir_on_create) {
    status = co_await runtime_->fsync_dir(kDir);
    if (!status.is_ok()) co_return status;
  }
  co_return Status::ok();
}

Task<Status> RaftStorage::close() {
  if (!open_) co_return Status::ok();
  open_ = false;
  co_await runtime_->close_file(log_);
  co_await runtime_->close_file(state_);
  offsets_.clear();
  log_offset_ = 0;
  state_offset_ = 0;
  log_dirty_ = false;
  state_dirty_ = false;
  co_return Status::ok();
}

}  // namespace anvil::raft
