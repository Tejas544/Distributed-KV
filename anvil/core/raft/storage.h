// Durable Raft state: the log, the hard state, and the latest snapshot.
//
// Three files per node, under `raft/`:
//
//   n<id>.log     append-only entries, [crc32c][len][term|index|type|data]
//   n<id>.state   append-only HardState records; recovery takes the last valid
//   n<id>.snap    the newest snapshot, written via tmp -> fsync -> rename ->
//                 fsync_dir
//
// The record framing and the reader are the LSM's (anvil/core/lsm/wal.h),
// reused rather than reimplemented. That is worth stating explicitly: the WAL
// reader already distinguishes a torn tail from an I/O error, which is the
// exact bug ANV-0003 was, and writing a second reader here would be writing a
// second chance to get it wrong.
//
// The hard state is an append-only log rather than a rewritten file because a
// vote must be durable *before* the reply goes out, and "rewrite a small file
// atomically" costs a create, a write, two fsyncs and a rename per vote. An
// append plus one fsync is the same guarantee at a fraction of the cost. The
// file is rewritten at snapshot time, so it does not grow without bound.
//
// Suffix truncation is a real ftruncate at a recorded byte offset, not a
// logical marker. A follower whose log diverges must not leave the old suffix
// on disk behind the new entries: recovery reads until the first invalid
// record, and a stale suffix is perfectly valid -- it would rebuild a log that
// never existed on any node.

#ifndef ANVIL_CORE_RAFT_STORAGE_H_
#define ANVIL_CORE_RAFT_STORAGE_H_

#include <cstdint>
#include <string>
#include <vector>

#include "anvil/core/raft/types.h"
#include "anvil/core/runtime/runtime.h"

namespace anvil::raft {

// Every guarantee as a separate flag, so the mutation drill can break exactly
// one at a time. Every default is correct; a test that flips one is planting a
// bug (see docs/CONTEXT.md section 6.4).
struct RaftDurability {
  bool fsync_log = true;         // entries durable before the append is acknowledged
  bool fsync_state = true;       // term and vote durable before the reply
  bool fsync_dir_on_create = true;
  bool fsync_snapshot = true;
};

struct RecoveredState {
  HardState hard;
  std::vector<LogEntry> entries;
  Snapshot snapshot;
  // The tail was dropped. Says nothing about *why*, and the difference matters:
  bool log_truncated = false;
  bool state_truncated = false;
  bool snapshot_corrupt = false;

  // A record that failed its checksum, or whose length header was impossible.
  // This is media damage -- something rewrote bytes that were already written.
  //
  // A truncation with `records_discarded == 0` is a different thing entirely: a
  // clean partial tail, which is what an unsynced write looks like after a
  // crash. Treating the two the same lets the corruption exemption excuse a
  // missing fsync, and a missing fsync is the one guarantee that must never be
  // excused.
  std::uint64_t records_discarded = 0;
  const char* truncate_reason = "none";
};

class RaftStorage {
 public:
  RaftStorage(Runtime* runtime, NodeId node, GroupId group, RaftDurability durability)
      : runtime_(runtime), node_(node), group_(group), durability_(durability) {}

  // Opens the files and reads everything back. Safe to call on a machine that
  // has never run, on one that crashed mid-append, and on one whose disk is
  // returning EIO -- the last returns a non-ok Status and must be retried
  // rather than treated as an empty log.
  Task<Status> recover(RecoveredState* out);

  Task<Status> append(const std::vector<LogEntry>& entries);
  Task<Status> truncate_suffix(LogIndex from);
  Task<Status> put_hard_state(const HardState& hard);
  Task<Status> sync();

  // Replaces the log file with exactly these entries. Used after a snapshot
  // install (the old log is subsumed) and after compaction (the prefix is).
  Task<Status> rewrite_log(const std::vector<LogEntry>& entries);

  Task<Status> save_snapshot(const Snapshot& snapshot);

  Task<Status> close();

  std::uint64_t log_bytes() const noexcept { return log_offset_; }
  bool open() const noexcept { return open_; }

 private:
  Task<Status> open_files();

  Runtime* runtime_;
  NodeId node_;
  GroupId group_;
  RaftDurability durability_;

  FileHandle log_{};
  FileHandle state_{};
  bool open_ = false;

  std::uint64_t log_offset_ = 0;
  std::uint64_t state_offset_ = 0;

  // Byte offset of the record that starts each entry, so a suffix truncation
  // is an ftruncate rather than a rewrite.
  std::vector<std::pair<std::uint64_t, std::uint64_t>> offsets_;  // (index, offset)

  bool log_dirty_ = false;
  bool state_dirty_ = false;

  // A rollback that could not be completed because the truncate itself failed.
  // Until the file is actually shortened back to this offset, the records the
  // failed append left behind are still there, and writing over them with a
  // shorter batch would leave a tail that recovery reads as a second copy of
  // indices it has already seen. Retried at the start of every append.
  bool rollback_pending_ = false;
  std::uint64_t rollback_to_ = 0;
};

// Encoding, exposed for the unit tests: a round-trip test that goes through the
// file system is testing the disk model, not the codec.
std::string encode_entry(const LogEntry& entry);
bool decode_entry(std::string_view in, LogEntry* out);
std::string encode_hard_state(const HardState& hard);
bool decode_hard_state(std::string_view in, HardState* out);
std::string encode_snapshot(const Snapshot& snapshot);
bool decode_snapshot(std::string_view in, Snapshot* out);

}  // namespace anvil::raft

#endif  // ANVIL_CORE_RAFT_STORAGE_H_
