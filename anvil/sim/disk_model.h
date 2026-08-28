// The simulated disk, with crash semantics.
//
// v0 made every write instantly durable, which meant a missing fsync was
// undetectable. This is the model that can punish it.
//
// Three separate pieces of state per file, and keeping them separate is the
// entire point:
//
//   cache      what the process sees. Its own writes are visible immediately,
//              exactly as a real page cache behaves.
//   durable    what survives a crash. Only fsync promotes cache into durable.
//   entry      whether the *directory entry* has been persisted, tracked
//              independently of the file's contents.
//
// That third one is the piece almost every model omits, and it is where the
// most expensive real-world bug in this class lives: write a new MANIFEST to a
// temp file, fsync the file, rename it into place, fsync the file again -- and
// never fsync the containing directory. The contents are durable. The name is
// not. After a crash the file is simply not there, recovery falls back to the
// previous version, and acknowledged writes are gone. A page-cache-only model
// reports that code as correct.
//
// At crash time each unsynced sector resolves to one of three outcomes, chosen
// by the seed: the old content, the new content, or a torn mix at sector
// granularity. The third is why checksums are not optional, and why a WAL has
// to be able to detect and truncate at its first invalid record rather than
// trusting a length field.
//
// What is still not modelled, and should be before the storage engine claims
// completeness in P2: reordering *between* sectors within one fsync, delayed
// allocation, and fsync itself failing and losing errors (the "fsyncgate"
// behaviour, where a failed writeback is reported once and then forgotten).

#ifndef ANVIL_SIM_DISK_MODEL_H_
#define ANVIL_SIM_DISK_MODEL_H_

#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "anvil/core/random.h"
#include "anvil/core/runtime/runtime.h"
#include "anvil/core/types.h"
#include "anvil/sim/fault_profile.h"

namespace anvil::sim {

class Scheduler;

struct DiskStats {
  std::uint64_t writes = 0;
  std::uint64_t reads = 0;
  std::uint64_t fsyncs = 0;
  std::uint64_t dir_fsyncs = 0;
  std::uint64_t crashes = 0;
  std::uint64_t sectors_lost = 0;       // unsynced, resolved to old content
  std::uint64_t sectors_torn = 0;       // unsynced, resolved to a partial write
  std::uint64_t files_lost_to_entry = 0;  // content durable, directory entry not
  std::uint64_t bit_rots = 0;
  std::uint64_t io_errors = 0;
  std::uint64_t no_space = 0;
  std::uint64_t slow_ios = 0;
};

class DiskModel {
 public:
  static constexpr std::size_t kSectorSize = 512;

  DiskModel(Scheduler* scheduler, std::uint64_t seed, DiskFaults faults);

  Status open(NodeId node, Path path, OpenFlags flags, FileHandle* out);
  Status pread(FileHandle f, MutableByteView dst, std::uint64_t offset, std::size_t* bytes_read);
  Status pwrite(FileHandle f, ByteView src, std::uint64_t offset);
  Status fsync(FileHandle f);
  Status ftruncate(FileHandle f, std::uint64_t size);
  Status file_size(FileHandle f, std::uint64_t* out);
  Status close_file(FileHandle f);

  Status rename(NodeId node, Path from, Path to);
  Status unlink(NodeId node, Path path);
  Status fsync_dir(NodeId node, Path dir);
  Status list_dir(NodeId node, Path dir, std::vector<std::string>* out);

  // ---- fault control -----------------------------------------------------

  // The machine died. Unsynced sectors resolve; files whose directory entry was
  // never persisted disappear; pending deletions that were never persisted come
  // back. Open handles are invalidated.
  void crash_node(NodeId node);

  // Corrupt one byte of durable storage. Models bit rot on the platter, not a
  // transient read error: once written, it stays wrong until overwritten, so
  // every subsequent read must detect it.
  bool scrub_corrupt(NodeId node);

  // Stops returning errors and corrupting media. Latency is left alone: a slow
  // disk is not a broken one. Used to establish eventual synchrony -- without
  // it, EIO keeps firing after "healing" and a node that gives up on recovery
  // never comes back, which is precisely how the first fault sweep produced
  // four seeds that never converged.
  void stop_injecting();

  Duration io_latency();
  Duration fsync_latency() const noexcept { return faults_.fsync_latency; }

  const DiskStats& stats() const noexcept { return stats_; }

  // Test hooks: what a crash right now would actually keep. Used by the disk
  // negative test to prove an unsynced write is lost rather than assuming it.
  std::uint64_t durable_size(NodeId node, Path path) const;
  bool durably_exists(NodeId node, Path path) const;

 private:
  struct FileKey {
    NodeId node;
    std::string path;
    friend auto operator<=>(const FileKey&, const FileKey&) noexcept = default;
  };

  struct File {
    std::vector<std::byte> cache;    // process view
    std::vector<std::byte> durable;  // survives a crash
    std::set<std::uint64_t> dirty;   // sector indices written since last fsync
    bool entry_durable = false;      // has the directory entry been persisted?
    bool deleted_in_cache = false;   // unlinked, but the removal may not be durable
    bool delete_pending = false;
  };

  struct OpenFile {
    FileKey key;
    OpenFlags flags = OpenFlags::kRead;
  };

  File* lookup(const FileKey& key);
  const File* lookup(const FileKey& key) const;
  File* file_for(FileHandle f, OpenFile** meta);
  bool would_exceed_capacity(NodeId node, std::size_t additional) const;
  bool maybe_io_error();
  static std::string dir_of(std::string_view path);

  Scheduler* scheduler_;
  DeterministicRandom rng_;
  DiskFaults faults_;

  std::map<FileKey, File> files_;
  std::map<std::uint64_t, OpenFile> open_;
  std::uint64_t next_handle_ = 1;
  DiskStats stats_;
};

}  // namespace anvil::sim

#endif  // ANVIL_SIM_DISK_MODEL_H_
