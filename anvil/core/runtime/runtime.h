// The Runtime seam.
//
// FROZEN as of P0. Everything above this interface is a pure state machine;
// everything nondeterministic lives below it. Two implementations exist and
// must stay behaviourally interchangeable:
//
//   SimRuntime   virtual clock, modelled network and disk, seeded PRNG,
//                crash/pause, BUGGIFY. Single thread, quantized execution.
//   ProdRuntime  CLOCK_MONOTONIC + HLC, io_uring/epoll, real files and sockets,
//                thread-per-core.
//
// The value of the seam is that the *same* protocol code runs under both. A bug
// found by the simulator is a bug in production code, not in a test double --
// which is the difference between this technique and mocking.
//
// Changing this file is a reviewed event: all four workstreams compile against
// it, and a signature change means a merge conflict in every one of them. See
// docs/SCOPE.md section 4.

#ifndef ANVIL_CORE_RUNTIME_RUNTIME_H_
#define ANVIL_CORE_RUNTIME_RUNTIME_H_

#include <cstdint>
#include <functional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "anvil/core/buggify.h"
#include "anvil/core/random.h"
#include "anvil/core/runtime/task.h"
#include "anvil/core/types.h"

namespace anvil {

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

enum class MessageKind : std::uint16_t {
  kUnknown = 0,
  // consensus
  kRequestVote, kRequestVoteReply,
  kPreVote, kPreVoteReply,
  kAppendEntries, kAppendEntriesReply,
  kInstallSnapshot, kInstallSnapshotReply,
  kTimeoutNow,
  // transactions
  kPrewrite, kPrewriteReply,
  kCommit, kCommitReply,
  kResolveLock, kResolveLockReply,
  kGetTimestamp, kGetTimestampReply,
  kPushTxn, kPushTxnReply,
  // routing and control
  kRangeLookup, kRangeLookupReply,
  kSplit, kMerge, kTransferLease,
  kHeartbeat, kHeartbeatReply,
  // client
  kClientRequest, kClientReply,
};

struct Message {
  NodeId from;
  NodeId to;
  MessageKind kind = MessageKind::kUnknown;
  std::uint64_t correlation = 0;
  std::vector<std::byte> payload;
};

// ---------------------------------------------------------------------------
// Files
// ---------------------------------------------------------------------------

enum class OpenFlags : std::uint32_t {
  kRead = 1u << 0,
  kWrite = 1u << 1,
  kCreate = 1u << 2,
  kTruncate = 1u << 3,
  kExclusive = 1u << 4,
  kDirect = 1u << 5,  // ProdRuntime: O_DIRECT. SimRuntime: bypass the page-cache
                      // model, so unsynced data is not recoverable after crash.
};

constexpr OpenFlags operator|(OpenFlags a, OpenFlags b) noexcept {
  return static_cast<OpenFlags>(static_cast<std::uint32_t>(a) | static_cast<std::uint32_t>(b));
}
constexpr bool has_flag(OpenFlags set, OpenFlags f) noexcept {
  return (static_cast<std::uint32_t>(set) & static_cast<std::uint32_t>(f)) != 0;
}

// Paths are plain strings so the simulator can own a virtual filesystem per
// node without the core knowing whether it is real.
using Path = std::string_view;

// ---------------------------------------------------------------------------
// Runtime
// ---------------------------------------------------------------------------

class Runtime {
 public:
  virtual ~Runtime();

  // -- identity ------------------------------------------------------------
  // Node identity comes from here, never from gethostname() or getpid(); those
  // are ambient inputs the seed does not control (tools/hermetic.toml).
  virtual NodeId self() const = 0;

  // -- time ----------------------------------------------------------------
  // Under the simulator, time advances only when every task is blocked. A busy
  // loop polling now() therefore hangs the simulation rather than spinning,
  // which is a feature: it makes accidental spin-waits impossible to miss.
  virtual Timestamp now() const = 0;

  // The honest reading: a bracket whose width is the clock uncertainty bound.
  // Strict serializability is built on this being a true bound, and the
  // simulator can deliberately violate it to characterise the failure mode.
  virtual TimeInterval now_uncertain() const = 0;

  virtual Task<void> sleep_for(Duration d) = 0;
  virtual Task<void> sleep_until(Timestamp t) = 0;

  virtual TimerId schedule(Duration delay, std::function<void()> callback) = 0;
  virtual void cancel(TimerId id) = 0;

  // -- randomness ----------------------------------------------------------
  // The only entropy source in the system. Everything downstream of a seed is
  // reproducible; anything that is not, is not.
  virtual std::uint64_t random_u64() = 0;
  virtual DeterministicRandom& rng(RandomDomain domain) = 0;

  // Production returns false unconditionally; the branch is already gone at
  // compile time via the ANVIL_BUGGIFY macro.
  virtual bool buggify(const BuggifySite& site) = 0;

  // -- network -------------------------------------------------------------
  // All of these can fail, hang, or silently drop under the network model. Code
  // that assumes send() means delivered is exactly what the simulator is for.
  virtual Task<Status> connect(NodeId peer, ConnHandle* out) = 0;
  virtual Task<Status> send(ConnHandle conn, Message msg) = 0;
  virtual Task<Status> recv(ConnHandle conn, Message* out) = 0;
  virtual void close(ConnHandle conn) = 0;

  // -- storage -------------------------------------------------------------
  // File-level rather than block-level, because the crash semantics we care
  // about are file-level: which bytes survive an fsync, whether a rename is
  // atomic, and whether a directory entry persists independently of the file's
  // contents. That last one is not a hypothetical -- it is where the classic
  // "MANIFEST exists but the directory entry does not" data-loss bug lives, and
  // a page-cache-only disk model cannot express it.
  virtual Task<Status> open(Path path, OpenFlags flags, FileHandle* out) = 0;
  virtual Task<Status> pread(FileHandle f, MutableByteView dst, std::uint64_t offset,
                             std::size_t* bytes_read) = 0;
  virtual Task<Status> pwrite(FileHandle f, ByteView src, std::uint64_t offset) = 0;
  virtual Task<Status> fsync(FileHandle f) = 0;
  virtual Task<Status> ftruncate(FileHandle f, std::uint64_t size) = 0;
  virtual Task<Status> file_size(FileHandle f, std::uint64_t* out) = 0;
  virtual Task<Status> close_file(FileHandle f) = 0;

  virtual Task<Status> rename(Path from, Path to) = 0;
  virtual Task<Status> unlink(Path path) = 0;
  // Durability of the *directory entry*, distinct from the file's contents.
  virtual Task<Status> fsync_dir(Path dir) = 0;
  virtual Task<Status> list_dir(Path dir, std::vector<std::string>* out) = 0;

  // -- scheduling ----------------------------------------------------------
  // spawn() detaches: the runtime owns the coroutine frame until it completes.
  virtual void spawn(Task<void> task) = 0;
  // An explicit preemption point. Under the simulator this is where the
  // adversarial scheduler gets to reorder, so protocol code should yield at
  // every point where a real implementation could be descheduled.
  virtual Task<void> yield() = 0;

  // -- observability -------------------------------------------------------
  // Structured events into the causal trace, with a virtual timestamp attached.
  // The core does not print; see the "console-io" rule in tools/hermetic.toml.
  virtual void trace(std::string_view event, std::span<const std::pair<
                         std::string_view, std::uint64_t>> fields) = 0;

  // Fatal, unrecoverable invariant breach detected by the code itself. Under
  // the simulator this fails the run with the full trace; in production it
  // aborts rather than continuing with corrupt state.
  [[noreturn]] virtual void panic(std::string_view reason) = 0;
};

}  // namespace anvil

#endif  // ANVIL_CORE_RUNTIME_RUNTIME_H_
