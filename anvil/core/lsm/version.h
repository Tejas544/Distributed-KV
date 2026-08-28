// The version set: which files make up the database right now.
//
// A Version is an immutable snapshot of the level structure. Changes are
// expressed as VersionEdits -- "these files were added at these levels, these
// were removed" -- appended to a MANIFEST log and then applied in memory. The
// MANIFEST is itself a write-ahead log with the same record framing and the
// same truncate-at-first-invalid-record rule as the data WAL.
//
// The part that matters, and the part that is most often wrong in real systems:
//
//     CURRENT names the live MANIFEST, and is replaced by
//     write CURRENT.tmp -> fsync(CURRENT.tmp) -> rename -> fsync_dir(.)
//
// Skip that final fsync of the *directory* and the rename is not durable. After
// a crash the file contents are immaculate and the name is simply not there, so
// recovery falls back to the previous manifest and every file added since is
// orphaned -- along with every acknowledged write inside them. The engine looks
// correct under any test that does not crash, which is most of them.
//
// The disk model in anvil/sim tracks directory-entry persistence separately from
// file contents precisely so this is reachable, and the seeded-mutation drill in
// test/lsm_crash.cc removes the fsync_dir on purpose to confirm it is caught.
//
// Levels: L0 holds files that may overlap (they come straight from memtable
// flushes, in arrival order, so a key can appear in several). L1 and below hold
// files with disjoint key ranges, which is what makes a lookup a binary search
// per level instead of a scan. INV-LSM-05 asserts that disjointness.

#ifndef ANVIL_CORE_LSM_VERSION_H_
#define ANVIL_CORE_LSM_VERSION_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "anvil/core/lsm/format.h"
#include "anvil/core/lsm/wal.h"
#include "anvil/core/runtime/runtime.h"

namespace anvil::lsm {

constexpr int kNumLevels = 7;

struct FileMetadata {
  std::uint64_t number = 0;
  std::uint64_t file_size = 0;
  std::string smallest;  // internal keys, inclusive
  std::string largest;

  bool overlaps_user_range(std::string_view lo, std::string_view hi) const;
};

class VersionEdit {
 public:
  void set_log_number(std::uint64_t n) { log_number_ = n; }
  void set_next_file_number(std::uint64_t n) { next_file_number_ = n; }
  void set_last_sequence(SequenceNumber s) { last_sequence_ = s; }
  void add_file(int level, FileMetadata file) {
    added_.emplace_back(level, std::move(file));
  }
  void delete_file(int level, std::uint64_t number) { deleted_.emplace_back(level, number); }

  std::string encode() const;
  bool decode(std::string_view input);

  const std::optional<std::uint64_t>& log_number() const noexcept { return log_number_; }
  const std::optional<std::uint64_t>& next_file_number() const noexcept {
    return next_file_number_;
  }
  const std::optional<SequenceNumber>& last_sequence() const noexcept {
    return last_sequence_;
  }
  const std::vector<std::pair<int, FileMetadata>>& added() const noexcept { return added_; }
  const std::vector<std::pair<int, std::uint64_t>>& deleted() const noexcept {
    return deleted_;
  }

 private:
  std::optional<std::uint64_t> log_number_;
  std::optional<std::uint64_t> next_file_number_;
  std::optional<SequenceNumber> last_sequence_;
  std::vector<std::pair<int, FileMetadata>> added_;
  std::vector<std::pair<int, std::uint64_t>> deleted_;
};

// An immutable snapshot of the level structure.
struct Version {
  std::vector<FileMetadata> levels[kNumLevels];

  // L0 newest-first (files overlap, so recency decides); L1+ by smallest key
  // (files are disjoint, so key order is a total order).
  void sort_levels();
  std::uint64_t total_bytes(int level) const;
  std::size_t total_files() const;

  // Files at `level` whose key range overlaps [lo, hi] on user keys.
  std::vector<const FileMetadata*> overlapping(int level, std::string_view lo,
                                               std::string_view hi) const;
};

struct DbPaths {
  static std::string manifest(std::uint64_t number);
  static std::string table(std::uint64_t number);
  static std::string wal(std::uint64_t number);
  static constexpr const char* kCurrent = "CURRENT";
  static constexpr const char* kDir = "";  // the database lives in the node's root
};

// Durability behaviour, exposed so the seeded-mutation drill can turn individual
// guarantees off and confirm the harness notices. Every field defaults to
// correct; a test that flips one is planting a bug on purpose.
struct DurabilityOptions {
  bool sync_wal_on_write = true;      // fsync the WAL before acknowledging
  bool sync_manifest = true;          // fsync the MANIFEST before applying an edit
  bool sync_dir_after_rename = true;  // fsync the directory after CURRENT is replaced
  bool sync_table_on_finish = true;   // fsync a new SSTable before it enters a version
};

class VersionSet {
 public:
  VersionSet(Runtime* runtime, DurabilityOptions durability)
      : runtime_(runtime), durability_(durability) {}

  // Reads CURRENT, replays the MANIFEST, and rebuilds the current Version.
  // `*fresh` is true when no database was found, which is a normal first start
  // and not an error.
  Task<Status> recover(bool* fresh);

  // Appends the edit to the MANIFEST, makes it durable, then applies it. The
  // order is the whole point: applying before the record is durable would let a
  // crash produce an in-memory version that references files no manifest
  // mentions.
  Task<Status> log_and_apply(const VersionEdit& edit);

  const Version& current() const noexcept { return current_; }
  std::uint64_t new_file_number() { return next_file_number_++; }
  std::uint64_t log_number() const noexcept { return log_number_; }
  SequenceNumber last_sequence() const noexcept { return last_sequence_; }
  void set_last_sequence(SequenceNumber s) { last_sequence_ = s; }
  std::uint64_t manifest_number() const noexcept { return manifest_number_; }

  // A MANIFEST whose tail failed its checksum. Survivable -- the dropped edits
  // were never durable -- but it must be *reported*, not silently absorbed.
  // Without this, media corruption of the manifest loses version edits and the
  // only symptom is data quietly reverting to an older state, which is
  // indistinguishable from an engine bug.
  std::uint64_t manifest_truncations() const noexcept { return manifest_truncations_; }

  // Every file number the live version references. Used by INV-LSM-09 and
  // INV-LSM-10 to detect dangling references and orphaned files respectively.
  std::vector<std::uint64_t> live_file_numbers() const;

 private:
  Task<Status> write_current(std::uint64_t manifest_number);
  void apply(const VersionEdit& edit);

  Runtime* runtime_;
  DurabilityOptions durability_;
  Version current_;
  std::uint64_t next_file_number_ = 2;  // 1 is reserved for the first manifest
  std::uint64_t manifest_number_ = 1;
  std::uint64_t log_number_ = 0;
  SequenceNumber last_sequence_ = 0;
  std::uint64_t manifest_truncations_ = 0;

  FileHandle manifest_file_{};
  std::unique_ptr<WalWriter> manifest_writer_;
  bool manifest_open_ = false;
};

}  // namespace anvil::lsm

#endif  // ANVIL_CORE_LSM_VERSION_H_
