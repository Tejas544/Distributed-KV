// The LSM engine.
//
// Write path:   batch -> WAL record -> (fsync) -> memtable
// Flush:        full memtable -> sorted SSTable at L0 -> version edit
// Compaction:   merge a file at level N with its overlaps at N+1 -> new files
// Read path:    memtable, then immutable memtable, then L0 newest-first, then
//               L1..L6 by binary search; first hit wins, and a tombstone hit
//               means "absent" rather than "keep looking"
//
// Two ordering rules run through the whole file and both are durability
// arguments rather than performance ones:
//
//   1. A write is acknowledged only after its WAL record is durable. Everything
//      else -- the memtable insert, the eventual flush -- is recoverable from
//      that record. Acknowledging first would make the engine fast and wrong,
//      and the wrongness would only appear on a crash.
//
//   2. A file enters a version only after both its contents *and* its directory
//      entry are durable. Contents alone are not enough: an SSTable that is
//      perfectly fsynced but whose name was never persisted simply does not
//      exist after a crash, and the manifest then references a file that is not
//      there.
//
// Both are switchable via DurabilityOptions so the seeded-mutation drill can
// break them deliberately and confirm the harness notices. Every default is
// correct; a test that flips one is planting a bug.
//
// Known simplification, stated rather than hidden: compaction reads its inputs
// fully into memory before merging. A production engine streams through a
// k-way merging iterator with bounded memory. The correctness properties --
// what the merge outputs, which tombstones it may drop -- are identical, so the
// invariants and the fault tests are unaffected; the memory profile is not.

#ifndef ANVIL_CORE_LSM_DB_H_
#define ANVIL_CORE_LSM_DB_H_

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "anvil/core/lsm/memtable.h"
#include "anvil/core/lsm/sstable.h"
#include "anvil/core/lsm/version.h"
#include "anvil/core/lsm/wal.h"
#include "anvil/core/runtime/runtime.h"

namespace anvil::lsm {

struct DbOptions {
  std::uint64_t seed = 1;
  std::size_t memtable_bytes = 32 * 1024;
  std::size_t block_bytes = 4096;
  std::size_t block_cache_bytes = 1 << 20;
  std::size_t l0_compaction_trigger = 4;
  std::uint64_t base_level_bytes = 128 * 1024;
  DurabilityOptions durability;
};

struct DbStats {
  std::uint64_t writes = 0;
  std::uint64_t reads = 0;
  std::uint64_t flushes = 0;
  std::uint64_t compactions = 0;
  std::uint64_t files_created = 0;
  std::uint64_t files_deleted = 0;
  std::uint64_t bytes_written = 0;
  std::uint64_t wal_records_replayed = 0;
  std::uint64_t wal_truncations = 0;
  std::uint64_t manifest_truncations = 0;
  std::uint64_t corruptions_detected = 0;
};

class Db {
 public:
  ~Db();
  Db(const Db&) = delete;
  Db& operator=(const Db&) = delete;

  // Opens or creates the database, replaying any WAL left by a previous
  // incarnation. Safe to call on a directory left in any state a crash can
  // produce -- that is the whole point.
  static Task<Status> open(Runtime* runtime, DbOptions options, std::unique_ptr<Db>* out);

  Task<Status> put(std::string_view key, std::string_view value);
  Task<Status> del(std::string_view key);
  Task<Status> write(const WriteBatch& batch);

  // `*found` is false for both "never written" and "deleted"; the caller cannot
  // and should not distinguish them.
  Task<Status> get(std::string_view key, std::string* value, bool* found);

  // Ordered scan over user keys in [lo, hi). An empty `hi` means unbounded.
  Task<Status> scan(std::string_view lo, std::string_view hi, std::size_t limit,
                    std::vector<std::pair<std::string, std::string>>* out);

  Task<Status> flush();            // memtable -> L0, unconditionally
  Task<Status> maybe_compact();    // one compaction step if any level is over budget
  Task<Status> close();

  const VersionSet& versions() const noexcept { return *versions_; }
  const DbStats& stats() const noexcept { return stats_; }
  SequenceNumber last_sequence() const noexcept { return versions_->last_sequence(); }

  // Files present on disk that no live version references. INV-LSM-10: after
  // quiesce this must be empty, or compaction is leaking space.
  Task<Status> orphaned_files(std::vector<std::string>* out);

 private:
  Db(Runtime* runtime, DbOptions options);

  Task<Status> recover();
  // Deletes SSTables no live version references. Run at startup: a crash
  // between "the file is durable" and "the edit naming it is durable" leaves a
  // valid file that nothing points at, and without this every crash leaks.
  Task<Status> sweep_orphans();
  Task<Status> replay_wal(std::uint64_t log_number);
  Task<Status> open_new_wal();
  Task<Status> write_table_from(const MemTable& table, FileMetadata* meta);
  // Returns a *reference* to the table, not a bare pointer.
  //
  // A read suspends inside pread while holding the Table it is reading from,
  // and compaction deletes obsolete files whenever it likes. Handing out a raw
  // pointer means the table can be destroyed underneath a suspended read, which
  // resumes into freed memory -- a crash if you are lucky, and silently wrong
  // bytes if you are not. The reference keeps it alive until the read is done
  // (ANV-0026).
  Task<Status> open_table(std::uint64_t number, std::shared_ptr<Table>* out);
  Task<Status> compact_level(int level);
  int pick_compaction_level() const;
  Task<Status> delete_obsolete_files(const std::vector<std::uint64_t>& removed);

  // Closes the file handles of retired tables nobody is reading any more.
  Task<void> sweep_retired_tables();

  Runtime* runtime_;
  DbOptions options_;
  std::unique_ptr<VersionSet> versions_;
  std::unique_ptr<BlockCache> cache_;

  std::unique_ptr<MemTable> memtable_;
  std::unique_ptr<MemTable> immutable_;

  FileHandle wal_file_{};
  std::unique_ptr<WalWriter> wal_;
  std::uint64_t wal_number_ = 0;

  // Open SSTables, keyed by file number. Held open because reopening on every
  // read would make the file-handle churn dominate the trace.
  // A flush and a compaction each suspend many times, and both mutate structures
  // the other reads. Only one of each may be in flight (ANV-0027).
  bool flushing_ = false;
  bool compacting_ = false;

  std::map<std::uint64_t, std::shared_ptr<Table>> tables_;

  // Tables removed from the cache that a suspended read may still hold. Their
  // file handles are closed once nothing refers to them.
  struct RetiredTable {
    std::uint64_t number = 0;
    std::shared_ptr<Table> table;
    FileHandle file{};
  };
  std::vector<RetiredTable> retired_tables_;
  std::map<std::uint64_t, FileHandle> table_files_;

  std::uint64_t memtable_seed_ = 0;
  DbStats stats_;
  bool closed_ = false;
};

}  // namespace anvil::lsm

#endif  // ANVIL_CORE_LSM_DB_H_
