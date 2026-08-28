#include "anvil/core/lsm/version.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <utility>

namespace anvil::lsm {
namespace {

// Tags in the MANIFEST record encoding. Explicit numbers because a manifest
// written by one build has to be readable by the next.
enum Tag : std::uint32_t {
  kTagLogNumber = 1,
  kTagNextFileNumber = 2,
  kTagLastSequence = 3,
  kTagDeletedFile = 4,
  kTagAddedFile = 5,
};

ByteView as_bytes(std::string_view s) {
  return ByteView{reinterpret_cast<const std::byte*>(s.data()), s.size()};
}

std::string numbered(const char* prefix, std::uint64_t number, const char* suffix) {
  char buf[64];
  std::snprintf(buf, sizeof(buf), "%s%06llu%s", prefix,
                static_cast<unsigned long long>(number), suffix);
  return buf;
}

}  // namespace

std::string DbPaths::manifest(std::uint64_t number) { return numbered("MANIFEST-", number, ""); }
std::string DbPaths::table(std::uint64_t number) { return numbered("", number, ".sst"); }
std::string DbPaths::wal(std::uint64_t number) { return numbered("", number, ".log"); }

bool FileMetadata::overlaps_user_range(std::string_view lo, std::string_view hi) const {
  // Compared on user keys, not internal keys: a range query is about user keys,
  // and the internal trailer would make an equal boundary compare unequal.
  if (!hi.empty() && compare_user(user_key_of(smallest), hi) > 0) return false;
  if (!lo.empty() && compare_user(user_key_of(largest), lo) < 0) return false;
  return true;
}

// ---------------------------------------------------------------------------
// VersionEdit
// ---------------------------------------------------------------------------

std::string VersionEdit::encode() const {
  std::string out;
  if (log_number_.has_value()) {
    put_varint32(&out, kTagLogNumber);
    put_varint64(&out, *log_number_);
  }
  if (next_file_number_.has_value()) {
    put_varint32(&out, kTagNextFileNumber);
    put_varint64(&out, *next_file_number_);
  }
  if (last_sequence_.has_value()) {
    put_varint32(&out, kTagLastSequence);
    put_varint64(&out, *last_sequence_);
  }
  for (const auto& [level, number] : deleted_) {
    put_varint32(&out, kTagDeletedFile);
    put_varint32(&out, static_cast<std::uint32_t>(level));
    put_varint64(&out, number);
  }
  for (const auto& [level, file] : added_) {
    put_varint32(&out, kTagAddedFile);
    put_varint32(&out, static_cast<std::uint32_t>(level));
    put_varint64(&out, file.number);
    put_varint64(&out, file.file_size);
    put_length_prefixed(&out, file.smallest);
    put_length_prefixed(&out, file.largest);
  }
  return out;
}

bool VersionEdit::decode(std::string_view input) {
  const char* p = input.data();
  const char* const limit = p + input.size();

  while (p < limit) {
    std::uint32_t tag = 0;
    p = get_varint32(p, limit, &tag);
    if (p == nullptr) return false;

    switch (tag) {
      case kTagLogNumber: {
        std::uint64_t value = 0;
        p = get_varint64(p, limit, &value);
        if (p == nullptr) return false;
        log_number_ = value;
        break;
      }
      case kTagNextFileNumber: {
        std::uint64_t value = 0;
        p = get_varint64(p, limit, &value);
        if (p == nullptr) return false;
        next_file_number_ = value;
        break;
      }
      case kTagLastSequence: {
        std::uint64_t value = 0;
        p = get_varint64(p, limit, &value);
        if (p == nullptr) return false;
        last_sequence_ = value;
        break;
      }
      case kTagDeletedFile: {
        std::uint32_t level = 0;
        std::uint64_t number = 0;
        p = get_varint32(p, limit, &level);
        if (p != nullptr) p = get_varint64(p, limit, &number);
        if (p == nullptr || level >= kNumLevels) return false;
        deleted_.emplace_back(static_cast<int>(level), number);
        break;
      }
      case kTagAddedFile: {
        std::uint32_t level = 0;
        FileMetadata file;
        std::string_view smallest;
        std::string_view largest;
        p = get_varint32(p, limit, &level);
        if (p != nullptr) p = get_varint64(p, limit, &file.number);
        if (p != nullptr) p = get_varint64(p, limit, &file.file_size);
        if (p != nullptr) p = get_length_prefixed(p, limit, &smallest);
        if (p != nullptr) p = get_length_prefixed(p, limit, &largest);
        if (p == nullptr || level >= kNumLevels) return false;
        file.smallest.assign(smallest);
        file.largest.assign(largest);
        added_.emplace_back(static_cast<int>(level), std::move(file));
        break;
      }
      default:
        // An unknown tag means the record is either from a newer format or is
        // garbage that happened to pass its CRC. Either way, guessing is worse
        // than refusing.
        return false;
    }
  }
  return true;
}

// ---------------------------------------------------------------------------
// Version
// ---------------------------------------------------------------------------

void Version::sort_levels() {
  // L0 files overlap, so recency is the ordering that matters: a newer file
  // shadows an older one for the same key. File numbers are monotonic, so
  // descending number is descending age.
  std::sort(levels[0].begin(), levels[0].end(),
            [](const FileMetadata& a, const FileMetadata& b) { return a.number > b.number; });

  // L1+ files are disjoint, so smallest-key order is a total order and makes
  // lookups a binary search.
  for (int level = 1; level < kNumLevels; ++level) {
    std::sort(levels[level].begin(), levels[level].end(),
              [](const FileMetadata& a, const FileMetadata& b) {
                return compare_internal(a.smallest, b.smallest) < 0;
              });
  }
}

std::uint64_t Version::total_bytes(int level) const {
  std::uint64_t sum = 0;
  for (const FileMetadata& file : levels[level]) sum += file.file_size;
  return sum;
}

std::size_t Version::total_files() const {
  std::size_t count = 0;
  for (int level = 0; level < kNumLevels; ++level) count += levels[level].size();
  return count;
}

std::vector<const FileMetadata*> Version::overlapping(int level, std::string_view lo,
                                                      std::string_view hi) const {
  std::vector<const FileMetadata*> out;
  for (const FileMetadata& file : levels[level]) {
    if (file.overlaps_user_range(lo, hi)) out.push_back(&file);
  }
  return out;
}

// ---------------------------------------------------------------------------
// VersionSet
// ---------------------------------------------------------------------------

void VersionSet::apply(const VersionEdit& edit) {
  if (edit.log_number().has_value()) log_number_ = *edit.log_number();
  if (edit.next_file_number().has_value()) {
    next_file_number_ = std::max(next_file_number_, *edit.next_file_number());
  }
  if (edit.last_sequence().has_value()) {
    last_sequence_ = std::max(last_sequence_, *edit.last_sequence());
  }

  // Deletions before additions: a compaction edit can legitimately remove a
  // file number and add a different one at the same level in one edit, and
  // processing additions first would leave the level briefly inconsistent.
  for (const auto& [level, number] : edit.deleted()) {
    auto& files = current_.levels[level];
    files.erase(std::remove_if(files.begin(), files.end(),
                               [n = number](const FileMetadata& f) { return f.number == n; }),
                files.end());
  }
  for (const auto& [level, file] : edit.added()) {
    current_.levels[level].push_back(file);
  }
  current_.sort_levels();
}

Task<Status> VersionSet::write_current(std::uint64_t manifest_number) {
  const std::string tmp = "CURRENT.tmp";
  // CURRENT is checksummed like everything else. It is fifteen bytes and it is
  // the single pointer the whole database hangs from; without a checksum, media
  // corruption here surfaces as "the manifest file does not exist", which sends
  // whoever is debugging it looking in entirely the wrong place.
  const std::string name = DbPaths::manifest(manifest_number);
  std::string contents;
  put_fixed32(&contents, crc32c(name));
  contents.append(name);

  FileHandle file{};
  Status status = co_await runtime_->open(
      tmp, OpenFlags::kWrite | OpenFlags::kCreate | OpenFlags::kTruncate, &file);
  if (!status.is_ok()) co_return status;

  status = co_await runtime_->pwrite(file, as_bytes(contents), 0);
  if (!status.is_ok()) co_return status;

  // Contents durable before the rename, or the rename could publish a
  // half-written pointer.
  status = co_await runtime_->fsync(file);
  if (!status.is_ok()) co_return status;
  co_await runtime_->close_file(file);

  status = co_await runtime_->rename(tmp, DbPaths::kCurrent);
  if (!status.is_ok()) co_return status;

  // And now the one everybody forgets. Without it the rename is not durable:
  // after a crash the contents are perfect and the *name* is gone, so recovery
  // falls back to an older manifest and silently orphans everything added
  // since. The mutation drill removes this line on purpose.
  if (durability_.sync_dir_after_rename) {
    status = co_await runtime_->fsync_dir(DbPaths::kDir);
    if (!status.is_ok()) co_return status;
  }
  co_return Status::ok();
}

Task<Status> VersionSet::recover(bool* fresh) {
  *fresh = false;

  FileHandle current{};
  Status status = co_await runtime_->open(DbPaths::kCurrent, OpenFlags::kRead, &current);
  if (status.code() == StatusCode::kNotFound) {
    // No CURRENT: either a brand-new database, or one whose CURRENT rename was
    // never made durable. The two are indistinguishable from here, which is
    // exactly why the fsync_dir above matters.
    *fresh = true;
    co_return Status::ok();
  }
  if (!status.is_ok()) co_return status;

  std::uint64_t size = 0;
  status = co_await runtime_->file_size(current, &size);
  if (!status.is_ok()) co_return status;
  if (size < 5 || size > 256) co_return Status{StatusCode::kCorruption, "bad CURRENT"};

  std::string raw(static_cast<std::size_t>(size), '\0');
  std::size_t read = 0;
  status = co_await runtime_->pread(
      current, MutableByteView{reinterpret_cast<std::byte*>(raw.data()), raw.size()}, 0, &read);
  if (!status.is_ok()) co_return status;
  raw.resize(read);
  co_await runtime_->close_file(current);

  if (raw.size() < 5) co_return Status{StatusCode::kCorruption, "truncated CURRENT"};
  const std::string name = raw.substr(4);
  if (crc32c(name) != decode_fixed32(raw.data())) {
    // Without this check, media corruption of CURRENT surfaces as "the manifest
    // file does not exist", which sends whoever is debugging it looking in
    // entirely the wrong place.
    co_return Status{StatusCode::kCorruption, "CURRENT checksum mismatch"};
  }

  FileHandle manifest{};
  status = co_await runtime_->open(name, OpenFlags::kRead, &manifest);
  if (!status.is_ok()) co_return status;

  WalReadResult records;
  status = co_await wal_read_all(runtime_, manifest, &records);
  if (!status.is_ok()) co_return status;

  // A truncated manifest means the CRC fired and later edits were dropped. The
  // database is consistent -- those edits were never durable -- but the caller
  // has to know, or corruption looks like data spontaneously reverting.
  if (records.truncated) ++manifest_truncations_;

  for (const std::string& record : records.records) {
    VersionEdit edit;
    if (!edit.decode(record)) {
      // A manifest record that fails to decode after passing its CRC means the
      // format is wrong, not the bytes. Continuing would build a version that
      // does not describe what is on disk.
      co_return Status{StatusCode::kCorruption, "undecodable manifest record"};
    }
    apply(edit);
  }
  co_await runtime_->close_file(manifest);

  // A truncated manifest is survivable -- the tail records were never durable,
  // so the version they describe was never acknowledged. Everything before the
  // damage stands.
  manifest_number_ = 0;
  for (std::size_t i = 0; i + 1 < name.size(); ++i) {
    if (name.compare(i, 1, "-") == 0) {
      manifest_number_ = std::strtoull(name.c_str() + i + 1, nullptr, 10);
      break;
    }
  }
  next_file_number_ = std::max(next_file_number_, manifest_number_ + 1);
  co_return Status::ok();
}

Task<Status> VersionSet::log_and_apply(const VersionEdit& edit) {
  if (!manifest_open_) {
    manifest_number_ = new_file_number();
    Status status = co_await runtime_->open(
        DbPaths::manifest(manifest_number_),
        OpenFlags::kWrite | OpenFlags::kCreate | OpenFlags::kTruncate, &manifest_file_);
    if (!status.is_ok()) co_return status;

    manifest_writer_ = std::make_unique<WalWriter>(runtime_, manifest_file_);

    // A fresh manifest starts with a full snapshot of the current version, so
    // recovery never has to chain back through older manifests.
    VersionEdit snapshot;
    snapshot.set_log_number(log_number_);
    snapshot.set_next_file_number(next_file_number_);
    snapshot.set_last_sequence(last_sequence_);
    for (int level = 0; level < kNumLevels; ++level) {
      for (const FileMetadata& file : current_.levels[level]) snapshot.add_file(level, file);
    }
    status = co_await manifest_writer_->append(snapshot.encode());
    if (!status.is_ok()) co_return status;

    status = co_await write_current(manifest_number_);
    if (!status.is_ok()) co_return status;
    manifest_open_ = true;
  }

  VersionEdit persisted = edit;
  persisted.set_next_file_number(next_file_number_);
  persisted.set_last_sequence(last_sequence_);
  persisted.set_log_number(edit.log_number().value_or(log_number_));

  Status status = co_await manifest_writer_->append(persisted.encode());
  if (!status.is_ok()) co_return status;

  // Durable before applied. Applying first would let a crash leave an in-memory
  // version referencing files no manifest mentions -- and since the in-memory
  // version is what compaction reasons about, that is how files get deleted
  // while still live.
  if (durability_.sync_manifest) {
    status = co_await manifest_writer_->sync();
    if (!status.is_ok()) co_return status;
  }

  apply(persisted);
  co_return Status::ok();
}

std::vector<std::uint64_t> VersionSet::live_file_numbers() const {
  std::vector<std::uint64_t> out;
  for (int level = 0; level < kNumLevels; ++level) {
    for (const FileMetadata& file : current_.levels[level]) out.push_back(file.number);
  }
  std::sort(out.begin(), out.end());
  return out;
}

}  // namespace anvil::lsm
