// The versioned store: writes leave versions, reads pick one.
//
// Everything here sits on top of the LSM's ordinary key space (see key.h) and
// touches the engine only through put/get/scan/del. That is a deliberate
// boundary. The engine's job is "make these bytes durable and ordered"; the
// question "which of these bytes may this reader see" belongs to the layer that
// knows what a transaction is, and mixing them is how storage engines grow
// transaction semantics they cannot explain.
//
// Three kinds of record live in the key space:
//
//   version   'd' | key | ~commit_ts  ->  value        a committed version
//   intent    'l' | key               ->  {txn, start_ts, value, kind}
//   (nothing) absence is not distinguishable from a tombstone at this layer,
//             which is correct: a delete is a version whose value is empty and
//             whose type says tombstone.
//
// The intent is the uncommitted half of a write. It lives in a separate key
// space rather than as a version at a provisional timestamp, because a reader
// that encounters one has to *do* something -- wait, abort the writer, or read
// past it -- and that decision needs the writer's identity, which a version
// does not carry. One intent per key at a time; that is what makes the lock
// table and the intent space the same thing viewed from two directions.

#ifndef ANVIL_CORE_MVCC_MVCC_H_
#define ANVIL_CORE_MVCC_MVCC_H_

#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "anvil/core/lsm/db.h"
#include "anvil/core/mvcc/key.h"
#include "anvil/core/runtime/runtime.h"
#include "anvil/core/types.h"

namespace anvil::mvcc {

// What a transaction has done to a key but not yet committed.
struct Intent {
  TxnId txn{};
  CommitTs start_ts = 0;
  bool tombstone = false;
  std::string value;
};

std::string encode_intent(const Intent& intent);
bool decode_intent(std::string_view in, Intent* out);

struct ReadResult {
  bool found = false;
  std::string value;
  CommitTs commit_ts = 0;

  // A committed version this read could not use because an uncommitted intent
  // from another transaction sits on the key. The caller decides what to do:
  // wait for it, push it, or resolve it. Returning the blocker rather than a
  // bare error is what lets the lock table make that decision without a second
  // lookup.
  bool blocked = false;
  Intent blocker;
};

struct MvccStats {
  std::uint64_t versions_written = 0;
  std::uint64_t intents_written = 0;
  std::uint64_t intents_committed = 0;
  std::uint64_t intents_aborted = 0;
  std::uint64_t reads = 0;
  std::uint64_t reads_blocked = 0;
  std::uint64_t versions_scanned = 0;
  std::uint64_t versions_collected = 0;
  std::uint64_t gc_passes = 0;

  // Reads whose uncertainty interval straddled a committed version. The caller
  // must restart at a higher timestamp; counting them is how the cost of a
  // clock's honesty becomes visible instead of being a mystery latency spike.
  std::uint64_t uncertainty_restarts = 0;
};

// Every deliberate bug the drill can plant, one flag each, all defaulting to
// correct. A test that flips one is planting a bug; the discipline is the same
// as DurabilityOptions in the LSM and RaftOptions in consensus (CONTEXT.md 6.4).
struct MvccOptions {
  // false: the collector also deletes the newest version at or below the
  // safepoint. Looks like an obvious simplification -- everything at or below
  // the safepoint is "old" -- and it is the single most destructive bug in this
  // layer, because a reader sitting exactly on the safepoint resolves to that
  // version and now finds an older one, or nothing, with no error anywhere.
  bool gc_keeps_safepoint_version = true;

  // false: a read ignores an intent it cannot resolve and returns the newest
  // committed version instead. That is read-committed wearing snapshot
  // isolation's name.
  bool reads_respect_intents = true;
};

class MvccStore {
 public:
  MvccStore(Runtime* runtime, lsm::Db* db) : runtime_(runtime), db_(db) {}
  MvccStore(Runtime* runtime, lsm::Db* db, MvccOptions options)
      : runtime_(runtime), db_(db), options_(options) {}

  const MvccOptions& options() const noexcept { return options_; }

  // ---- reads -------------------------------------------------------------

  // The newest version with commit_ts <= read_ts. One seek, because the
  // timestamp is inverted in the key: versions of a key sort newest-first, so
  // the first record at or after the seek point is the answer.
  Task<Status> get(std::string_view key, CommitTs read_ts, TxnId reader, ReadResult* out);

  // The same, but honest about the clock. A version committed inside
  // [read_ts, uncertainty_limit] might have happened before this read in real
  // time even though its timestamp is higher, so the read cannot claim to have
  // seen a consistent snapshot: it must restart at a timestamp above it.
  // Returns kAborted with `out->commit_ts` set to the version that forced it.
  Task<Status> get_uncertain(std::string_view key, CommitTs read_ts, CommitTs uncertainty_limit,
                             TxnId reader, ReadResult* out);

  // ---- writes ------------------------------------------------------------

  // Leaves an intent. Fails with kAborted if another transaction already holds
  // one, with the holder in `*conflict` so the caller can decide.
  Task<Status> put_intent(std::string_view key, const Intent& intent, Intent* conflict);

  // Turns this transaction's intent into a committed version, or removes it.
  //
  // Single-key forms, used by tests and by the resolution of one stray intent.
  // A multi-key transaction must use the batched forms below.
  Task<Status> commit_intent(std::string_view key, TxnId txn, CommitTs commit_ts);
  Task<Status> abort_intent(std::string_view key, TxnId txn);

  // Resolves every key of one transaction in a single atomic batch.
  //
  // Not a convenience. Applying a multi-key commit one key at a time leaves a
  // window in which a concurrent reader sees *half a transaction* -- some keys
  // at their new values, the rest at their old ones -- which is exactly the
  // atomicity snapshot isolation is supposed to provide. The engine's write
  // batch is one WAL record with one checksum, so a reader either sees all of
  // it or none of it, and the window closes.
  Task<Status> commit_all(const std::set<std::string>& keys, TxnId txn, CommitTs commit_ts);
  Task<Status> abort_all(const std::set<std::string>& keys, TxnId txn);

  Task<Status> read_intent(std::string_view key, bool* found, Intent* out);

  // ---- garbage collection ------------------------------------------------

  // Removes versions that no reader at or above `safepoint` could ever see.
  //
  // The rule, and the whole reason INV-MVCC-01 exists: for each key, the newest
  // version with commit_ts <= safepoint must be KEPT, because a reader at
  // exactly `safepoint` still resolves to it. Everything strictly older than
  // that one is unreachable and may go. Deleting the newest-below-safepoint is
  // the classic silent corruption: the reader falls through to an older version
  // or to nothing at all, and nothing anywhere reports an error.
  Task<Status> collect_garbage(CommitTs safepoint, std::size_t max_keys,
                               std::uint64_t* collected);

  // Every version of one key, newest first. For the invariant checker and for
  // tests; not on any hot path.
  Task<Status> versions_of(std::string_view key,
                           std::vector<std::pair<CommitTs, std::string>>* out) const;

  Task<Status> keys_with_versions(std::vector<std::string>* out) const;

  const MvccStats& stats() const noexcept { return stats_; }

 private:
  Task<Status> scan_versions(std::string_view key, CommitTs from_ts, std::size_t limit,
                             std::vector<std::pair<std::string, std::string>>* out) const;

  Runtime* runtime_;
  lsm::Db* db_;
  MvccOptions options_;
  MvccStats stats_;
};

}  // namespace anvil::mvcc

#endif  // ANVIL_CORE_MVCC_MVCC_H_
