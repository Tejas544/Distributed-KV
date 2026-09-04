#include "anvil/core/lsm/db.h"

#include <algorithm>
#include <utility>

#include "anvil/core/buggify.h"

namespace anvil::lsm {
namespace {

// Level N holds roughly 10x what level N-1 does. The multiplier is what keeps
// total write amplification logarithmic rather than linear.
std::uint64_t level_target_bytes(int level, std::uint64_t base) {
  std::uint64_t target = base;
  for (int i = 1; i < level; ++i) target *= 10;
  return target;
}

}  // namespace

Db::Db(Runtime* runtime, DbOptions options) : runtime_(runtime), options_(options) {
  versions_ = std::make_unique<VersionSet>(runtime, options.durability);
  cache_ = std::make_unique<BlockCache>(options.block_cache_bytes);
  memtable_seed_ = options.seed;
  memtable_ = std::make_unique<MemTable>(memtable_seed_++);
}

Db::~Db() = default;

Task<Status> Db::open(Runtime* runtime, DbOptions options, std::unique_ptr<Db>* out) {
  std::unique_ptr<Db> db{new Db(runtime, options)};
  const Status status = co_await db->recover();
  if (!status.is_ok()) co_return status;
  *out = std::move(db);
  co_return Status::ok();
}

// ---------------------------------------------------------------------------
// recovery
// ---------------------------------------------------------------------------

Task<Status> Db::recover() {
  bool fresh = false;
  Status status = co_await versions_->recover(&fresh);
  if (!status.is_ok()) co_return status;
  stats_.manifest_truncations = versions_->manifest_truncations();

  if (!fresh && versions_->log_number() != 0) {
    status = co_await replay_wal(versions_->log_number());
    if (!status.is_ok()) co_return status;
  }

  status = co_await open_new_wal();
  if (!status.is_ok()) co_return status;

  // Record the new log number in the manifest before accepting writes, so a
  // crash immediately after this point still finds the right log to replay.
  VersionEdit edit;
  edit.set_log_number(wal_number_);
  status = co_await versions_->log_and_apply(edit);
  if (!status.is_ok()) co_return status;

  // Sweep files the previous incarnation orphaned.
  //
  // A crash between "the SSTable is durable" and "the version edit naming it is
  // durable" leaves a perfectly good file that no manifest mentions. It is not
  // corruption and it is not data loss -- the writes it holds were never
  // acknowledged -- but it is space that will never be reclaimed, and INV-LSM-10
  // says a quiesced database has none of it. Without this sweep every crash
  // leaks, which over a long fault run looks exactly like a compaction bug.
  co_return co_await sweep_orphans();
}

Task<Status> Db::sweep_orphans() {
  std::vector<std::string> orphans;
  Status status = co_await orphaned_files(&orphans);
  if (!status.is_ok()) co_return status;

  for (const std::string& name : orphans) {
    status = co_await runtime_->unlink(name);
    if (status.is_ok()) ++stats_.files_deleted;
  }
  if (!orphans.empty() && options_.durability.sync_dir_after_rename) {
    co_await runtime_->fsync_dir(DbPaths::kDir);
  }
  co_return Status::ok();
}

Task<Status> Db::replay_wal(std::uint64_t log_number) {
  FileHandle file{};
  Status status = co_await runtime_->open(DbPaths::wal(log_number), OpenFlags::kRead, &file);
  if (status.code() == StatusCode::kNotFound) {
    // The log the manifest names is gone. That is survivable only because the
    // manifest is written before the log is reused; anything the log held was
    // never acknowledged.
    co_return Status::ok();
  }
  if (!status.is_ok()) co_return status;

  WalReadResult records;
  status = co_await wal_read_all(runtime_, file, &records);
  co_await runtime_->close_file(file);
  // A device error during replay must propagate so the caller retries. Treating
  // it as end-of-log would silently discard acknowledged writes (ANV-0003).
  if (!status.is_ok()) co_return status;

  if (records.truncated) ++stats_.wal_truncations;

  SequenceNumber max_sequence = versions_->last_sequence();
  for (const std::string& record : records.records) {
    std::vector<WriteBatch::Entry> entries;
    SequenceNumber first = 0;
    if (!WriteBatch::decode(record, &entries, &first)) {
      // Passed its CRC but will not decode: the record is structurally wrong,
      // and everything after it is untrustworthy for the same reason a torn
      // write is.
      ++stats_.corruptions_detected;
      break;
    }
    for (const WriteBatch::Entry& entry : entries) {
      memtable_->add(entry.sequence, entry.type, entry.key, entry.value);
      max_sequence = std::max(max_sequence, entry.sequence);
      ++stats_.wal_records_replayed;
    }
  }
  versions_->set_last_sequence(max_sequence);
  co_return Status::ok();
}

Task<Status> Db::open_new_wal() {
  wal_number_ = versions_->new_file_number();
  Status status = co_await runtime_->open(
      DbPaths::wal(wal_number_),
      OpenFlags::kRead | OpenFlags::kWrite | OpenFlags::kCreate | OpenFlags::kTruncate,
      &wal_file_);
  if (!status.is_ok()) co_return status;

  // The log's directory entry has to be durable too. A log whose name never
  // persisted is a log recovery cannot find, and every write in it is gone.
  if (options_.durability.sync_dir_after_rename) {
    status = co_await runtime_->fsync_dir(DbPaths::kDir);
    if (!status.is_ok()) co_return status;
  }

  wal_ = std::make_unique<WalWriter>(runtime_, wal_file_);
  co_return Status::ok();
}

// ---------------------------------------------------------------------------
// writes
// ---------------------------------------------------------------------------

Task<Status> Db::write(const WriteBatch& batch) {
  if (closed_) co_return Status{StatusCode::kUnavailable, "database closed"};
  if (batch.empty()) co_return Status::ok();

  // Reserved before the first suspension point, so two writers in flight at once
  // cannot be handed the same numbers (ANV-0025).
  const SequenceNumber first = versions_->allocate_sequence(batch.count());
  const std::string record = batch.encode(first);

  // Every exit from here on has to account for the range, successfully or not.
  // An allocated range that is never published is a permanent hole: publication
  // advances along the contiguous prefix, so one abandoned range makes every
  // later write invisible for the rest of the process's life. Abandoning is
  // safe precisely because the entries were never added to the memtable --
  // there is nothing at those sequences to reveal.
  Status status = co_await wal_->append(record);
  if (!status.is_ok()) {
    versions_->publish_sequence(first, batch.count());
    co_return status;
  }

  // The acknowledgement boundary. Everything before this point is recoverable
  // from the log; everything after assumes it already is.
  if (options_.durability.sync_wal_on_write) {
    status = co_await wal_->sync();
    if (!status.is_ok()) {
      versions_->publish_sequence(first, batch.count());
      co_return status;
    }
  }

  std::vector<WriteBatch::Entry> entries;
  SequenceNumber decoded_first = 0;
  if (!WriteBatch::decode(record, &entries, &decoded_first)) {
    versions_->publish_sequence(first, batch.count());
    co_return Status{StatusCode::kCorruption, "self-encoded batch failed to decode"};
  }
  for (const WriteBatch::Entry& entry : entries) {
    memtable_->add(entry.sequence, entry.type, entry.key, entry.value);
  }

  versions_->publish_sequence(first, batch.count());
  stats_.writes += batch.count();

  if (memtable_->memory_usage() >= options_.memtable_bytes) {
    status = co_await flush();
    if (!status.is_ok()) co_return status;
    status = co_await maybe_compact();
    if (!status.is_ok()) co_return status;
  }
  co_return Status::ok();
}

Task<Status> Db::put(std::string_view key, std::string_view value) {
  WriteBatch batch;
  batch.put(key, value);
  co_return co_await write(batch);
}

Task<Status> Db::del(std::string_view key) {
  WriteBatch batch;
  batch.del(key);
  co_return co_await write(batch);
}

// ---------------------------------------------------------------------------
// flush
// ---------------------------------------------------------------------------

Task<Status> Db::write_table_from(const MemTable& table, FileMetadata* meta) {
  const std::uint64_t number = versions_->new_file_number();
  FileHandle file{};
  Status status = co_await runtime_->open(
      DbPaths::table(number),
      OpenFlags::kRead | OpenFlags::kWrite | OpenFlags::kCreate | OpenFlags::kTruncate, &file);
  if (!status.is_ok()) co_return status;

  TableBuilder builder{runtime_, file, options_.block_bytes};
  auto it = table.iterator();
  for (it.seek_to_first(); it.valid(); it.next()) {
    status = co_await builder.add(it.internal_key(), it.value());
    if (!status.is_ok()) co_return status;
  }
  status = co_await builder.finish();
  if (!status.is_ok()) co_return status;

  // Contents durable, then the directory entry. Both, in that order, before the
  // file is allowed into a version -- see the header comment.
  if (options_.durability.sync_table_on_finish) {
    status = co_await runtime_->fsync(file);
    if (!status.is_ok()) co_return status;
    if (options_.durability.sync_dir_after_rename) {
      status = co_await runtime_->fsync_dir(DbPaths::kDir);
      if (!status.is_ok()) co_return status;
    }
  }
  co_await runtime_->close_file(file);

  meta->number = number;
  meta->file_size = builder.file_size();
  meta->smallest.assign(builder.smallest());
  meta->largest.assign(builder.largest());
  ++stats_.files_created;
  stats_.bytes_written += builder.file_size();
  co_return Status::ok();
}

Task<Status> Db::flush() {
  if (memtable_->empty()) co_return Status::ok();

  // One flush at a time.
  //
  // A flush installs the full memtable as `immutable_` and then suspends for the
  // whole of writing an SSTable. A second flush starting in that window
  // overwrites `immutable_` -- destroying the memtable the first one is still
  // iterating, and losing every entry it had not yet written. The caller loses
  // nothing by returning here: its write is already in the new memtable and
  // already in the WAL, and the next write over the threshold will flush it
  // (ANV-0027).
  if (flushing_) co_return Status::ok();
  flushing_ = true;

  immutable_ = std::move(memtable_);
  memtable_ = std::make_unique<MemTable>(memtable_seed_++);

  FileMetadata meta;
  Status status = co_await write_table_from(*immutable_, &meta);
  if (!status.is_ok()) {
    flushing_ = false;
    co_return status;
  }

  // A new WAL before the edit: the edit records which log is live, and the
  // memtable that produced this file is about to stop being replayable.
  const FileHandle old_wal = wal_file_;
  const std::uint64_t old_wal_number = wal_number_;
  status = co_await open_new_wal();
  if (!status.is_ok()) {
    flushing_ = false;
    co_return status;
  }

  VersionEdit edit;
  edit.add_file(0, meta);
  edit.set_log_number(wal_number_);
  status = co_await versions_->log_and_apply(edit);
  if (!status.is_ok()) {
    flushing_ = false;
    co_return status;
  }

  immutable_.reset();
  flushing_ = false;
  ++stats_.flushes;

  co_await runtime_->close_file(old_wal);
  co_await runtime_->unlink(DbPaths::wal(old_wal_number));
  co_return Status::ok();
}

// ---------------------------------------------------------------------------
// reads
// ---------------------------------------------------------------------------

Task<Status> Db::open_table(std::uint64_t number, std::shared_ptr<Table>* out) {
  const auto cached = tables_.find(number);
  if (cached != tables_.end()) {
    *out = cached->second;
    co_return Status::ok();
  }

  FileHandle file{};
  Status status = co_await runtime_->open(DbPaths::table(number), OpenFlags::kRead, &file);
  if (!status.is_ok()) co_return status;

  std::unique_ptr<Table> table;
  status = co_await Table::open(runtime_, file, number, cache_.get(), &table);
  if (!status.is_ok()) {
    if (status.code() == StatusCode::kCorruption) ++stats_.corruptions_detected;
    co_await runtime_->close_file(file);
    co_return status;
  }

  table_files_[number] = file;
  auto shared = std::shared_ptr<Table>{std::move(table)};
  tables_[number] = shared;
  *out = std::move(shared);
  co_return Status::ok();
}

Task<Status> Db::get(std::string_view key, std::string* value, bool* found) {
  *found = false;
  ++stats_.reads;

  const SequenceNumber snapshot = versions_->last_sequence();
  bool is_deletion = false;

  // Memtables first: they hold the newest versions by construction.
  if (memtable_->get(key, snapshot, value, &is_deletion)) {
    *found = !is_deletion;
    co_return Status::ok();
  }
  if (immutable_ != nullptr && immutable_->get(key, snapshot, value, &is_deletion)) {
    *found = !is_deletion;
    co_return Status::ok();
  }

  const std::string probe = make_internal_key(key, snapshot, ValueType::kValue);
  const Version& version = versions_->current();

  for (int level = 0; level < kNumLevels; ++level) {
    // L0 is already sorted newest-first, so scanning it in order stops at the
    // newest version. L1+ files are disjoint, so at most one can contain the
    // key -- but the loop handles both uniformly rather than special-casing,
    // since the overlap filter makes the disjoint case a single candidate
    // anyway.
    for (const FileMetadata& file : version.levels[level]) {
      if (!file.overlaps_user_range(key, key)) continue;

      std::shared_ptr<Table> table;
      Status status = co_await open_table(file.number, &table);
      if (!status.is_ok()) co_return status;

      bool hit = false;
      status = co_await table->get(probe, &hit, value, &is_deletion);
      if (!status.is_ok()) {
        if (status.code() == StatusCode::kCorruption) ++stats_.corruptions_detected;
        co_return status;
      }
      if (hit) {
        // First hit wins, including a tombstone. Continuing past a tombstone
        // would resurrect an older value from a lower level -- one of the
        // classic LSM bugs, and completely invisible until someone deletes a
        // key that was flushed twice.
        *found = !is_deletion;
        co_return Status::ok();
      }
    }
  }
  co_return Status::ok();
}

Task<Status> Db::scan(std::string_view lo, std::string_view hi, std::size_t limit,
                      std::vector<std::pair<std::string, std::string>>* out) {
  out->clear();
  const SequenceNumber snapshot = versions_->last_sequence();

  // Merge every source into one map keyed by user key, newest version winning.
  // Sources are visited oldest-first so newer writes overwrite older ones.
  std::map<std::string, std::pair<SequenceNumber, std::string>> merged;
  std::map<std::string, SequenceNumber> tombstones;

  const auto consider = [&](std::string_view internal_key, std::string_view value) {
    const std::uint64_t trailer = trailer_of(internal_key);
    const SequenceNumber seq = sequence_of(trailer);
    if (seq > snapshot) return;

    const std::string user{user_key_of(internal_key)};
    if (!lo.empty() && compare_user(user, lo) < 0) return;
    if (!hi.empty() && compare_user(user, hi) >= 0) return;

    if (type_of(trailer) == ValueType::kDeletion) {
      auto& existing = tombstones[user];
      if (seq >= existing) existing = seq;
      return;
    }
    auto it = merged.find(user);
    if (it == merged.end() || seq >= it->second.first) {
      merged[user] = {seq, std::string{value}};
    }
  };

  const Version& version = versions_->current();
  for (int level = kNumLevels - 1; level >= 0; --level) {
    // Bottom-up, and within L0 oldest-first, so newer data always overwrites.
    const auto& files = version.levels[level];
    for (auto it = files.rbegin(); it != files.rend(); ++it) {
      if (!it->overlaps_user_range(lo, hi)) continue;
      std::shared_ptr<Table> table;
      Status status = co_await open_table(it->number, &table);
      if (!status.is_ok()) co_return status;
      status = co_await table->for_each(consider);
      if (!status.is_ok()) co_return status;
    }
  }

  if (immutable_ != nullptr) {
    auto it = immutable_->iterator();
    for (it.seek_to_first(); it.valid(); it.next()) consider(it.internal_key(), it.value());
  }
  {
    auto it = memtable_->iterator();
    for (it.seek_to_first(); it.valid(); it.next()) consider(it.internal_key(), it.value());
  }

  for (const auto& [user, entry] : merged) {
    const auto tomb = tombstones.find(user);
    if (tomb != tombstones.end() && tomb->second > entry.first) continue;
    out->emplace_back(user, entry.second);
    if (limit > 0 && out->size() >= limit) break;
  }
  co_return Status::ok();
}

// ---------------------------------------------------------------------------
// compaction
// ---------------------------------------------------------------------------

int Db::pick_compaction_level() const {
  const Version& version = versions_->current();

  // L0 is scored by file count, not bytes: its files overlap, so every one of
  // them has to be consulted on a read. Too many L0 files is a read-latency
  // problem long before it is a space problem.
  if (version.levels[0].size() >= options_.l0_compaction_trigger) return 0;

  // A compaction that starts well under the trigger is always safe -- the
  // trigger is a scheduling heuristic, not a correctness bound -- so it is a
  // legitimate rare path to nominate: real bugs in recovery/version-edit
  // bookkeeping are far likelier to show up when compactions overlap flushes
  // and other compactions than when they run one at a time, evenly spaced.
  if (!version.levels[0].empty() && ANVIL_BUGGIFY) return 0;

  int best_level = -1;
  double best_score = 1.0;
  for (int level = 1; level < kNumLevels - 1; ++level) {
    const std::uint64_t target = level_target_bytes(level, options_.base_level_bytes);
    const double score =
        static_cast<double>(version.total_bytes(level)) / static_cast<double>(target);
    if (score > best_score) {
      best_score = score;
      best_level = level;
    }
  }
  return best_level;
}

Task<Status> Db::compact_level(int level) {
  const Version& version = versions_->current();
  if (version.levels[level].empty()) co_return Status::ok();

  // Inputs: one file from `level` (the oldest, for fairness) plus every file at
  // level+1 whose key range overlaps it.
  std::vector<FileMetadata> inputs;
  std::vector<FileMetadata> next_inputs;

  if (level == 0) {
    // L0 files overlap each other, so a compaction has to take all of them --
    // compacting a subset would leave a newer file below an older one and
    // silently invert version order.
    //
    // Copied element by element rather than by vector assignment: at -O2, GCC
    // inlines the copy-assignment through the coroutine frame, loses track of
    // the frame pointer, and emits a -Wnull-dereference false positive. The
    // loop is equivalent and keeps -Werror meaningful, which matters more than
    // the one-liner.
    inputs.reserve(version.levels[0].size());
    for (const FileMetadata& file : version.levels[0]) inputs.push_back(file);
  } else {
    inputs.push_back(version.levels[level].front());
  }

  std::string lo = user_key_of(inputs.front().smallest).empty()
                       ? std::string{}
                       : std::string{user_key_of(inputs.front().smallest)};
  std::string hi{user_key_of(inputs.front().largest)};
  for (const FileMetadata& file : inputs) {
    if (compare_user(user_key_of(file.smallest), lo) < 0) lo.assign(user_key_of(file.smallest));
    if (compare_user(user_key_of(file.largest), hi) > 0) hi.assign(user_key_of(file.largest));
  }

  for (const FileMetadata* file : version.overlapping(level + 1, lo, hi)) {
    next_inputs.push_back(*file);
  }

  // Collect every entry from every input. See the header note: a production
  // engine streams this through a merging iterator; the outputs are identical.
  std::vector<std::pair<std::string, std::string>> entries;
  const auto collect = [&](const std::vector<FileMetadata>& files) -> Task<Status> {
    for (const FileMetadata& file : files) {
      std::shared_ptr<Table> table;
      Status status = co_await open_table(file.number, &table);
      if (!status.is_ok()) co_return status;
      status = co_await table->for_each(
          [&](std::string_view key, std::string_view value) {
            entries.emplace_back(std::string{key}, std::string{value});
          });
      if (!status.is_ok()) co_return status;
    }
    co_return Status::ok();
  };

  Status status = co_await collect(inputs);
  if (!status.is_ok()) co_return status;
  status = co_await collect(next_inputs);
  if (!status.is_ok()) co_return status;

  std::sort(entries.begin(), entries.end(),
            [](const auto& a, const auto& b) { return compare_internal(a.first, b.first) < 0; });

  const int output_level = level + 1;
  // Tombstones may only be dropped where nothing below could still hold an
  // older value. Dropping one too early resurrects deleted data, which is
  // silent, permanent, and reported months later as "the delete didn't work".
  const bool bottom_most = [&] {
    for (int l = output_level + 1; l < kNumLevels; ++l) {
      if (!version.levels[l].empty()) return false;
    }
    return true;
  }();

  const std::uint64_t number = versions_->new_file_number();
  FileHandle out_file{};
  status = co_await runtime_->open(
      DbPaths::table(number),
      OpenFlags::kRead | OpenFlags::kWrite | OpenFlags::kCreate | OpenFlags::kTruncate,
      &out_file);
  if (!status.is_ok()) co_return status;

  TableBuilder builder{runtime_, out_file, options_.block_bytes};
  std::string previous_user;
  bool have_previous = false;
  std::size_t written = 0;

  for (const auto& [key, value] : entries) {
    const std::string_view user = user_key_of(key);
    // Entries are sorted newest-first within a user key, so the first one seen
    // is the survivor and everything after it is shadowed.
    if (have_previous && compare_user(user, previous_user) == 0) continue;
    previous_user.assign(user);
    have_previous = true;

    if (bottom_most && type_of(trailer_of(key)) == ValueType::kDeletion) continue;

    status = co_await builder.add(key, value);
    if (!status.is_ok()) co_return status;
    ++written;
  }

  status = co_await builder.finish();
  if (!status.is_ok()) co_return status;
  if (options_.durability.sync_table_on_finish) {
    status = co_await runtime_->fsync(out_file);
    if (!status.is_ok()) co_return status;
    if (options_.durability.sync_dir_after_rename) {
      status = co_await runtime_->fsync_dir(DbPaths::kDir);
      if (!status.is_ok()) co_return status;
    }
  }
  co_await runtime_->close_file(out_file);

  VersionEdit edit;
  std::vector<std::uint64_t> removed;
  for (const FileMetadata& file : inputs) {
    edit.delete_file(level, file.number);
    removed.push_back(file.number);
  }
  for (const FileMetadata& file : next_inputs) {
    edit.delete_file(output_level, file.number);
    removed.push_back(file.number);
  }

  if (written > 0) {
    FileMetadata meta;
    meta.number = number;
    meta.file_size = builder.file_size();
    meta.smallest.assign(builder.smallest());
    meta.largest.assign(builder.largest());
    edit.add_file(output_level, meta);
    ++stats_.files_created;
    stats_.bytes_written += builder.file_size();
  }

  status = co_await versions_->log_and_apply(edit);
  if (!status.is_ok()) co_return status;

  // Only now, with the edit durable, may the inputs be deleted. Deleting first
  // and crashing would leave the manifest pointing at files that no longer
  // exist -- INV-LSM-09, and unrecoverable.
  status = co_await delete_obsolete_files(removed);
  if (!status.is_ok()) co_return status;

  if (written == 0) {
    co_await runtime_->unlink(DbPaths::table(number));
    ++stats_.files_deleted;
  }

  ++stats_.compactions;
  co_return Status::ok();
}

Task<Status> Db::delete_obsolete_files(const std::vector<std::uint64_t>& removed) {
  for (const std::uint64_t number : removed) {
    // Retired, not destroyed, and its handle stays open: a read suspended inside
    // pread still holds this table and will resume into it. Closing the handle
    // here would hand that read a stale descriptor, which is the same defect as
    // freeing the table, one level down.
    RetiredTable retired;
    retired.number = number;
    const auto live = tables_.find(number);
    if (live != tables_.end()) {
      retired.table = live->second;
      tables_.erase(live);
    }
    const auto handle = table_files_.find(number);
    if (handle != table_files_.end()) {
      retired.file = handle->second;
      table_files_.erase(handle);
    }
    retired_tables_.push_back(std::move(retired));
    // INV-LSM-13: the cache must not outlive the file. Blocks keyed by a
    // deleted file number would otherwise sit there indefinitely.
    cache_->erase_file(number);

    const Status status = co_await runtime_->unlink(DbPaths::table(number));
    if (status.is_ok()) ++stats_.files_deleted;
  }
  if (options_.durability.sync_dir_after_rename) {
    co_await runtime_->fsync_dir(DbPaths::kDir);
  }
  co_await sweep_retired_tables();
  co_return Status::ok();
}

Task<void> Db::sweep_retired_tables() {
  // The list is taken away from the member before anything is awaited. Closing a
  // file suspends, and a compaction that retires another table during that
  // suspension would push onto the very vector this loop is walking -- which
  // reallocates it and leaves the loop reading freed memory. Iterating a
  // container across a suspension point is the coroutine equivalent of mutating
  // it while iterating, and it corrupts the heap rather than failing cleanly.
  std::vector<RetiredTable> candidates;
  candidates.swap(retired_tables_);

  // `use_count() == 1` means this list holds the only reference: every read that
  // was inside the table has resumed and let go. Anything still held goes back
  // for the next sweep.
  std::vector<RetiredTable> still_in_use;
  for (RetiredTable& retired : candidates) {
    if (retired.table != nullptr && retired.table.use_count() > 1) {
      still_in_use.push_back(std::move(retired));
      continue;
    }
    retired.table.reset();
    if (retired.file.value() != 0) co_await runtime_->close_file(retired.file);
  }
  for (RetiredTable& retired : still_in_use) {
    retired_tables_.push_back(std::move(retired));
  }
  co_return;
}

Task<Status> Db::maybe_compact() {
  // One compaction at a time, for the same reason as flush: compact_level picks
  // its inputs, suspends across the whole merge, and then edits the version. Two
  // in flight pick overlapping inputs from the same version and the second one's
  // edit deletes files the first one's edit already replaced.
  if (compacting_) co_return Status::ok();
  const int level = pick_compaction_level();
  if (level < 0) co_return Status::ok();
  compacting_ = true;
  const Status status = co_await compact_level(level);
  compacting_ = false;
  co_return status;
}

// ---------------------------------------------------------------------------
// housekeeping
// ---------------------------------------------------------------------------

Task<Status> Db::orphaned_files(std::vector<std::string>* out) {
  out->clear();
  std::vector<std::string> names;
  const Status status = co_await runtime_->list_dir(DbPaths::kDir, &names);
  if (!status.is_ok()) co_return status;

  const std::vector<std::uint64_t> live = versions_->live_file_numbers();
  for (const std::string& name : names) {
    if (name.size() < 5 || name.compare(name.size() - 4, 4, ".sst") != 0) continue;
    const std::uint64_t number = std::strtoull(name.c_str(), nullptr, 10);
    if (!std::binary_search(live.begin(), live.end(), number)) out->push_back(name);
  }
  co_return Status::ok();
}

Task<Status> Db::close() {
  if (closed_) co_return Status::ok();
  closed_ = true;
  for (const auto& [number, handle] : table_files_) co_await runtime_->close_file(handle);
  table_files_.clear();
  tables_.clear();
  co_await runtime_->close_file(wal_file_);
  co_return Status::ok();
}

}  // namespace anvil::lsm
