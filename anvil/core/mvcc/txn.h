// Single-node transactions over the versioned store.
//
// Snapshot isolation, done the way it is actually specified rather than the way
// it is usually described:
//
//   * A read sees the newest version committed at or before `start_ts`, and
//     never anything newer, for the whole life of the transaction. That is the
//     snapshot, and it does not move.
//   * A write takes an exclusive lock and leaves an intent. If any version of
//     that key committed *after* this transaction started, the transaction
//     aborts -- first-committer-wins. This is the rule that makes SI SI, and
//     leaving it out gives you read-committed with extra steps.
//   * Commit assigns a timestamp above every start timestamp in flight and
//     turns the intents into versions.
//
// What SI deliberately does NOT prevent is write skew: two transactions each
// reading what the other writes, neither touching the same key, both committing.
// That is not a bug, it is the level, and the checker in P7 is expected to
// *observe* write skew here and to report no G1c -- which is how the level gets
// confirmed rather than assumed (roadmap P4 exit criterion 4).
//
// The read spans and write set recorded for `kSerializable` are the SSI
// mechanism arriving ahead of the engine that uses it. They are collected and
// exposed; the certifier that turns them into aborts is P6's, because it needs
// the distributed conflict tracking to mean anything.

#ifndef ANVIL_CORE_MVCC_TXN_H_
#define ANVIL_CORE_MVCC_TXN_H_

#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "anvil/core/mvcc/lock_table.h"
#include "anvil/core/mvcc/mvcc.h"
#include "anvil/core/runtime/runtime.h"
#include "anvil/core/types.h"

namespace anvil::mvcc {

enum class IsolationLevel : std::uint8_t {
  kSnapshot,      // SI: write skew is legal
  kSerializable,  // SSI mechanism recorded; certifier lands in P6
};

enum class TxnState : std::uint8_t {
  kActive,
  kCommitted,
  kAborted,
  kWounded,   // an older transaction wants a lock this one holds
  kResolving, // the outcome is decided; writing it down did not survive the disk
};

const char* to_string(TxnState state) noexcept;

struct Txn {
  TxnId id{};
  CommitTs start_ts = 0;
  CommitTs commit_ts = 0;
  IsolationLevel level = IsolationLevel::kSnapshot;
  TxnState state = TxnState::kActive;

  // Keys this transaction has written, in order. Also the set whose intents
  // have to be resolved on commit or abort -- which is why it is a sorted
  // container: intent resolution order must not depend on a hash seed.
  std::set<std::string> writes;

  // SSI mechanism. Point reads are recorded as degenerate spans so that one
  // structure covers both and the certifier does not need two code paths.
  struct Span {
    std::string lo;
    std::string hi;  // empty means a point read of `lo`
    friend auto operator<=>(const Span&, const Span&) = default;
  };
  std::set<Span> reads;

  // Set when the transaction's outcome is decided but its intents could not be
  // written down. `commit_ts != 0` means the decision was commit.
  std::uint32_t resolve_attempts = 0;

  // When the transaction reached a final state. Retirement is gated on this:
  // anything auditing intents attributes them by owner, and an owner that has
  // already been forgotten reads as "unknown" -- which is indistinguishable
  // from a genuinely orphaned intent. Keeping resolved transactions around for
  // a while is what makes that distinction possible at all.
  Timestamp resolved_at{};

  bool live() const noexcept { return state == TxnState::kActive; }
  bool resolving() const noexcept { return state == TxnState::kResolving; }
};

struct TxnStats {
  std::uint64_t begun = 0;
  std::uint64_t committed = 0;
  std::uint64_t aborted = 0;
  std::uint64_t wounded = 0;
  std::uint64_t write_conflicts = 0;   // first-committer-wins fired
  std::uint64_t blocked_reads = 0;
  std::uint64_t resolved_intents = 0;
  std::uint64_t deadlocks_detected = 0;
  std::uint64_t resolutions_deferred = 0;  // a commit or abort whose write failed
  std::uint64_t resolutions_retried = 0;
};

class TxnManager {
 public:
  TxnManager(Runtime* runtime, MvccStore* store, LockTable* locks)
      : runtime_(runtime), store_(store), locks_(locks) {}

  // ---- lifecycle ---------------------------------------------------------
  Txn* begin(CommitTs start_ts, IsolationLevel level);
  Txn* find(TxnId id);
  const Txn* find(TxnId id) const;

  Task<Status> read(TxnId id, std::string_view key, bool* found, std::string* value);
  Task<Status> write(TxnId id, std::string_view key, std::string_view value, bool tombstone);
  Task<Status> commit(TxnId id, CommitTs commit_ts);
  Task<Status> abort(TxnId id);

  // Retries the intent resolution of every transaction whose outcome is decided
  // but whose write failed.
  //
  // This exists because the alternative is worse than it looks. When a commit's
  // batch fails, the obvious thing is to hand the caller an error and move on --
  // and that leaves a transaction that is neither committed nor aborted, holding
  // intents nobody will ever clear, pinning the GC safepoint at its start
  // timestamp forever. One transient EIO and the collector stops collecting for
  // the rest of the process's life. So the decision is durable in memory, the
  // write is retried, and the transaction is not finished until the write lands.
  //
  // Retrying is sound because resolution is a single atomic batch: it landed or
  // it did not, and replaying it writes the same keys with the same values at
  // the same timestamp.
  Task<Status> resolve_pending(std::uint64_t* resolved);
  std::vector<TxnId> pending_resolutions() const;

  // Drops a finished transaction's bookkeeping. Refuses while it is unresolved,
  // and refuses until `min_age` has passed since it resolved, because forgetting
  // one too early is how its intents become unattributable.
  bool retire(TxnId id, Duration min_age = Duration::nanos(0));

  // ---- the safepoint -----------------------------------------------------
  //
  // INV-MVCC-02: the GC safepoint is at most the minimum of every live read
  // snapshot, the closed timestamp, and the oldest live transaction's start.
  // Computed in one place, because a second implementation of this expression
  // is a second chance to be one greater than it should be -- and being one
  // greater is exactly the bug that eats a live reader's version.
  CommitTs safepoint() const;
  void set_closed_timestamp(CommitTs ts) noexcept { closed_ts_ = ts; }

  // The newest timestamp anything has committed at. The safepoint is clamped to
  // it, because a safepoint above every committed version means "collect
  // everything", and a bound of infinity is not a bound.
  CommitTs highest_commit() const noexcept { return highest_commit_; }
  CommitTs closed_timestamp() const noexcept { return closed_ts_; }

  // A reader that is not a transaction: an iterator or a long scan holding a
  // snapshot open. Registered so the safepoint accounts for it.
  void open_snapshot(std::uint64_t handle, CommitTs ts);
  void close_snapshot(std::uint64_t handle);

  const std::map<std::uint64_t, Txn>& transactions() const noexcept { return txns_; }
  const std::map<std::uint64_t, CommitTs>& snapshots() const noexcept { return snapshots_; }
  const TxnStats& stats() const noexcept { return stats_; }

  // Runs the wait-for-graph check. Wound-wait should make this impossible; a
  // cycle here means the age comparison is wrong, not that the detector is
  // earning its keep.
  bool detect_deadlock(std::vector<TxnId>* cycle);

 private:
  Task<Status> resolve_conflict(Txn& txn, const LockHolder& blocker);

  Runtime* runtime_;
  MvccStore* store_;
  LockTable* locks_;

  std::map<std::uint64_t, Txn> txns_;
  std::map<std::uint64_t, CommitTs> snapshots_;
  CommitTs closed_ts_ = kMaxCommitTs;
  CommitTs highest_commit_ = 0;
  std::uint64_t next_id_ = 1;
  TxnStats stats_;
};

}  // namespace anvil::mvcc

#endif  // ANVIL_CORE_MVCC_TXN_H_
