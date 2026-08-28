#include "anvil/sim/disk_model.h"

#include <algorithm>
#include <cstring>

#include "anvil/sim/scheduler.h"

namespace anvil::sim {
namespace {

constexpr Status kBadHandle{StatusCode::kInvalidArgument, "unknown file handle"};
constexpr Status kNoSuchFile{StatusCode::kNotFound, "no such file"};
constexpr Status kIoError{StatusCode::kIoError, "EIO"};
constexpr Status kNoSpace{StatusCode::kNoSpace, "ENOSPC"};

std::uint64_t sector_of(std::uint64_t offset) {
  return offset / DiskModel::kSectorSize;
}

}  // namespace

DiskModel::DiskModel(Scheduler* scheduler, std::uint64_t seed, DiskFaults faults)
    : scheduler_(scheduler),
      rng_(DeterministicRandom{seed}.fork(RandomDomain::kDisk)),
      faults_(faults) {}

std::string DiskModel::dir_of(std::string_view path) {
  const auto slash = path.rfind('/');
  return slash == std::string_view::npos ? std::string{} : std::string{path.substr(0, slash)};
}

DiskModel::File* DiskModel::lookup(const FileKey& key) {
  const auto it = files_.find(key);
  if (it == files_.end() || it->second.deleted_in_cache) return nullptr;
  return &it->second;
}

const DiskModel::File* DiskModel::lookup(const FileKey& key) const {
  const auto it = files_.find(key);
  if (it == files_.end() || it->second.deleted_in_cache) return nullptr;
  return &it->second;
}

DiskModel::File* DiskModel::file_for(FileHandle f, OpenFile** meta) {
  const auto it = open_.find(f.value());
  if (it == open_.end()) return nullptr;
  *meta = &it->second;
  return lookup(it->second.key);
}

void DiskModel::stop_injecting() {
  faults_.io_error = Chance::never();
  faults_.bit_rot = Chance::never();
  faults_.slow_io = Chance::never();
  faults_.capacity_bytes = 0;
}

Duration DiskModel::io_latency() {
  Duration base = rng_.uniform_duration(faults_.min_latency, faults_.max_latency);
  if (faults_.slow_io.roll(rng_)) {
    ++stats_.slow_ios;
    // A latency spike, not an error. Devices do this, and the protocols above
    // must not confuse "slow" with "dead" -- confusing the two is how a healthy
    // replica gets evicted and a cluster loses quorum for no reason.
    base = base + faults_.slow_io_penalty;
  }
  return base;
}

bool DiskModel::maybe_io_error() {
  if (!faults_.io_error.roll(rng_)) return false;
  ++stats_.io_errors;
  return true;
}

bool DiskModel::would_exceed_capacity(NodeId node, std::size_t additional) const {
  if (faults_.capacity_bytes == 0) return false;
  std::uint64_t used = 0;
  for (const auto& [key, file] : files_) {
    if (key.node == node && !file.deleted_in_cache) used += file.cache.size();
  }
  return used + additional > faults_.capacity_bytes;
}

// ---------------------------------------------------------------------------
// file operations
// ---------------------------------------------------------------------------

Status DiskModel::open(NodeId node, Path path, OpenFlags flags, FileHandle* out) {
  if (maybe_io_error()) return kIoError;

  const FileKey key{node, std::string{path}};
  auto it = files_.find(key);
  const bool visible = it != files_.end() && !it->second.deleted_in_cache;

  if (!visible) {
    if (!has_flag(flags, OpenFlags::kCreate)) return kNoSuchFile;
    if (it == files_.end()) it = files_.emplace(key, File{}).first;
    File& f = it->second;
    f.deleted_in_cache = false;
    f.delete_pending = false;
    // A newly created file's directory entry is NOT durable until the parent
    // directory is fsynced. Crash before that and the file never existed --
    // regardless of how carefully its contents were flushed.
    f.entry_durable = false;
  } else if (has_flag(flags, OpenFlags::kExclusive) && has_flag(flags, OpenFlags::kCreate)) {
    return Status{StatusCode::kInvalidArgument, "file already exists"};
  }

  File& f = it->second;
  if (has_flag(flags, OpenFlags::kTruncate)) {
    f.cache.clear();
    for (std::uint64_t s = 0; s * kSectorSize < f.durable.size(); ++s) f.dirty.insert(s);
  }

  const std::uint64_t handle = next_handle_++;
  open_.emplace(handle, OpenFile{key, flags});
  *out = FileHandle{handle};
  return Status::ok();
}

Status DiskModel::pread(FileHandle f, MutableByteView dst, std::uint64_t offset,
                        std::size_t* bytes_read) {
  OpenFile* meta = nullptr;
  File* file = file_for(f, &meta);
  if (meta == nullptr) return kBadHandle;
  if (file == nullptr) return kNoSuchFile;
  if (maybe_io_error()) return kIoError;

  ++stats_.reads;
  // Reads see the cache, so a process always observes its own writes. The gap
  // between cache and durable is invisible until something crashes -- which is
  // precisely why missing-fsync bugs survive every test that does not crash.
  const std::vector<std::byte>& data = file->cache;
  if (offset >= data.size()) {
    *bytes_read = 0;
    return Status::ok();
  }
  const std::size_t available = data.size() - static_cast<std::size_t>(offset);
  const std::size_t n = std::min(dst.size(), available);
  if (n > 0) std::memcpy(dst.data(), data.data() + offset, n);
  *bytes_read = n;
  return Status::ok();
}

Status DiskModel::pwrite(FileHandle f, ByteView src, std::uint64_t offset) {
  OpenFile* meta = nullptr;
  File* file = file_for(f, &meta);
  if (meta == nullptr) return kBadHandle;
  if (file == nullptr) return kNoSuchFile;
  if (!has_flag(meta->flags, OpenFlags::kWrite)) {
    return Status{StatusCode::kInvalidArgument, "file not opened for writing"};
  }
  if (maybe_io_error()) return kIoError;

  const std::size_t end = static_cast<std::size_t>(offset) + src.size();
  if (end > file->cache.size() && would_exceed_capacity(meta->key.node, end - file->cache.size())) {
    ++stats_.no_space;
    return kNoSpace;
  }

  if (file->cache.size() < end) file->cache.resize(end, std::byte{0});
  if (!src.empty()) std::memcpy(file->cache.data() + offset, src.data(), src.size());
  ++stats_.writes;

  if (faults_.page_cache) {
    for (std::uint64_t s = sector_of(offset); s <= sector_of(end == 0 ? 0 : end - 1); ++s) {
      file->dirty.insert(s);
    }
  } else {
    // The control condition: no page cache, every write instantly durable. The
    // configuration in which a missing fsync cannot possibly be detected.
    if (file->durable.size() < end) file->durable.resize(end, std::byte{0});
    std::memcpy(file->durable.data() + offset, file->cache.data() + offset, src.size());
  }
  return Status::ok();
}

Status DiskModel::fsync(FileHandle f) {
  OpenFile* meta = nullptr;
  File* file = file_for(f, &meta);
  if (meta == nullptr) return kBadHandle;
  if (file == nullptr) return kNoSuchFile;
  if (maybe_io_error()) return kIoError;

  ++stats_.fsyncs;
  if (file->durable.size() < file->cache.size()) {
    file->durable.resize(file->cache.size(), std::byte{0});
  } else if (file->durable.size() > file->cache.size()) {
    file->durable.resize(file->cache.size());
  }

  for (const std::uint64_t s : file->dirty) {
    const std::size_t begin = static_cast<std::size_t>(s) * kSectorSize;
    if (begin >= file->cache.size()) continue;
    const std::size_t n = std::min(kSectorSize, file->cache.size() - begin);
    std::memcpy(file->durable.data() + begin, file->cache.data() + begin, n);
  }
  file->dirty.clear();

  // Note what fsync does NOT do: it does not make the directory entry durable.
  // A file can have perfectly synced contents and still not exist after a
  // crash. That is the bug this whole model exists to expose.
  return Status::ok();
}

Status DiskModel::ftruncate(FileHandle f, std::uint64_t size) {
  OpenFile* meta = nullptr;
  File* file = file_for(f, &meta);
  if (meta == nullptr) return kBadHandle;
  if (file == nullptr) return kNoSuchFile;
  if (maybe_io_error()) return kIoError;

  file->cache.resize(static_cast<std::size_t>(size), std::byte{0});
  if (faults_.page_cache) {
    for (std::uint64_t s = 0; s * kSectorSize < file->durable.size(); ++s) file->dirty.insert(s);
  } else {
    file->durable = file->cache;
  }
  return Status::ok();
}

Status DiskModel::file_size(FileHandle f, std::uint64_t* out) {
  OpenFile* meta = nullptr;
  File* file = file_for(f, &meta);
  if (meta == nullptr) return kBadHandle;
  if (file == nullptr) return kNoSuchFile;
  *out = file->cache.size();
  return Status::ok();
}

Status DiskModel::close_file(FileHandle f) {
  return open_.erase(f.value()) > 0 ? Status::ok() : kBadHandle;
}

Status DiskModel::rename(NodeId node, Path from, Path to) {
  if (maybe_io_error()) return kIoError;

  const FileKey src{node, std::string{from}};
  File* source = lookup(src);
  if (source == nullptr) return kNoSuchFile;

  // The classic atomic-replace idiom. Contents move; the *entry* for the new
  // name is fresh and not durable until the directory is fsynced, and the
  // removal of the old name is likewise pending. Crash in between and you get
  // the old file back -- or, if the caller synced neither, nothing at all.
  File moved;
  moved.cache = source->cache;
  moved.durable = source->durable;
  moved.dirty = source->dirty;
  moved.entry_durable = false;

  source->deleted_in_cache = true;
  source->delete_pending = true;

  files_[FileKey{node, std::string{to}}] = std::move(moved);
  return Status::ok();
}

Status DiskModel::unlink(NodeId node, Path path) {
  if (maybe_io_error()) return kIoError;
  const FileKey key{node, std::string{path}};
  File* file = lookup(key);
  if (file == nullptr) return kNoSuchFile;
  file->deleted_in_cache = true;
  file->delete_pending = true;
  return Status::ok();
}

Status DiskModel::fsync_dir(NodeId node, Path dir) {
  if (maybe_io_error()) return kIoError;
  ++stats_.dir_fsyncs;

  const std::string target{dir};
  for (auto it = files_.begin(); it != files_.end();) {
    if (it->first.node != node || dir_of(it->first.path) != target) {
      ++it;
      continue;
    }
    if (it->second.delete_pending) {
      it = files_.erase(it);  // the removal is now durable
      continue;
    }
    it->second.entry_durable = true;
    ++it;
  }
  return Status::ok();
}

Status DiskModel::list_dir(NodeId node, Path dir, std::vector<std::string>* out) {
  if (maybe_io_error()) return kIoError;
  out->clear();
  const std::string target{dir};
  // std::map iterates in key order, so the listing is sorted and stable. A
  // directory listing whose order varied by run would be a determinism hazard:
  // compaction pickers and recovery scans both walk it.
  for (const auto& [key, file] : files_) {
    if (key.node != node || file.deleted_in_cache) continue;
    if (dir_of(key.path) != target) continue;
    out->push_back(target.empty() ? key.path : key.path.substr(target.size() + 1));
  }
  return Status::ok();
}

// ---------------------------------------------------------------------------
// crash
// ---------------------------------------------------------------------------

void DiskModel::crash_node(NodeId node) {
  ++stats_.crashes;

  for (auto it = files_.begin(); it != files_.end();) {
    if (it->first.node != node) {
      ++it;
      continue;
    }
    File& f = it->second;

    // A pending deletion that was never made durable did not happen. The file
    // comes back, which is exactly the "I deleted the old WAL and crashed"
    // scenario that leaves two logs on disk and confuses recovery.
    if (f.delete_pending) {
      f.delete_pending = false;
      f.deleted_in_cache = false;
    }

    // Contents may be immaculate; if the name was never persisted, none of that
    // matters. This is the fsync-the-file-but-not-the-directory bug.
    if (!f.entry_durable) {
      ++stats_.files_lost_to_entry;
      it = files_.erase(it);
      continue;
    }

    for (const std::uint64_t s : f.dirty) {
      const std::size_t begin = static_cast<std::size_t>(s) * kSectorSize;
      if (begin >= f.cache.size()) continue;
      const std::size_t n = std::min(kSectorSize, f.cache.size() - begin);

      const TornResolution resolution =
          faults_.torn_write.roll(rng_)
              ? TornResolution::kTorn
              : (rng_.coin() ? TornResolution::kNewContent : TornResolution::kOldContent);

      if (f.durable.size() < begin + n) f.durable.resize(begin + n, std::byte{0});

      switch (resolution) {
        case TornResolution::kOldContent:
          ++stats_.sectors_lost;
          break;  // durable already holds the old bytes
        case TornResolution::kNewContent:
          std::memcpy(f.durable.data() + begin, f.cache.data() + begin, n);
          break;
        case TornResolution::kTorn: {
          // A partial sector write. The device got part-way through and the
          // power went. Nothing about the result is self-describing, which is
          // why every persisted block needs a checksum and why a WAL must stop
          // at its first invalid record rather than trusting a length header.
          ++stats_.sectors_torn;
          const auto cut = static_cast<std::size_t>(rng_.uniform(n + 1));
          if (cut > 0) std::memcpy(f.durable.data() + begin, f.cache.data() + begin, cut);
          break;
        }
      }
    }

    f.dirty.clear();
    f.cache = f.durable;  // the page cache is gone; only the platter remains
    ++it;
  }

  // Handles belong to a process that no longer exists.
  for (auto it = open_.begin(); it != open_.end();) {
    it = it->second.key.node == node ? open_.erase(it) : std::next(it);
  }
}

bool DiskModel::scrub_corrupt(NodeId node) {
  std::vector<FileKey> candidates;
  for (const auto& [key, file] : files_) {
    if (key.node == node && !file.deleted_in_cache && !file.durable.empty()) {
      candidates.push_back(key);
    }
  }
  if (candidates.empty()) return false;

  File& f = files_[candidates[rng_.uniform(candidates.size())]];
  const auto index = static_cast<std::size_t>(rng_.uniform(f.durable.size()));
  const auto bit = static_cast<int>(rng_.uniform(8));
  f.durable[index] ^= static_cast<std::byte>(1u << bit);
  // Corrupt the cache too: after a scrub the process reading the file must see
  // the damage. Corrupting only the durable copy would hide the rot until the
  // next restart, which would understate how often checksums have to fire.
  if (index < f.cache.size()) f.cache[index] = f.durable[index];
  ++stats_.bit_rots;
  return true;
}

std::uint64_t DiskModel::durable_size(NodeId node, Path path) const {
  const File* f = lookup(FileKey{node, std::string{path}});
  return f == nullptr ? 0 : f->durable.size();
}

bool DiskModel::durably_exists(NodeId node, Path path) const {
  const File* f = lookup(FileKey{node, std::string{path}});
  return f != nullptr && f->entry_durable;
}

}  // namespace anvil::sim
