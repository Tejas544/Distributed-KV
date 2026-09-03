// Three transaction engines behind one interface.
//
// The interface is `begin`, `get`, `put`, `commit`. The engine is a setting.
// That is not an abstraction for its own sake -- it is the only honest way to
// ship the claim, because the three levels differ in *mechanism* and a system
// that ships one mechanism and names it after another is the single most common
// way this class of project falls apart under questioning:
//
//   kSnapshot           Percolator. Start timestamp from the oracle, reads at
//                       that timestamp, writes buffered and prewritten at
//                       commit, primary lock decides the outcome. Gives
//                       snapshot isolation. Write skew is legal here and the
//                       checker is *expected* to find it.
//
//   kSerializable       The same 2PC, plus read tracking. A transaction that is
//                       pushed forward must refresh its reads -- re-verify that
//                       nothing it read has changed between its start timestamp
//                       and its new commit timestamp -- or restart. That is
//                       what removes write skew, and it is why the read set is
//                       recorded rather than discarded.
//
//   kStrictSerializable Serializable, plus commit-wait. The commit timestamp is
//                       the *top* of the clock's uncertainty interval and the
//                       acknowledgement is held back until the local clock's
//                       lower bound has passed it. That is what buys real-time
//                       ordering, and it costs one uncertainty width of latency
//                       per commit, which is the honest price and is measured.
//
// Everything else is shared: the same prewrite, the same primary record, the
// same lazy resolution, the same push protocol. The engines differ by two
// booleans and a wait, which is the point.
//
// The commit point is one Raft entry in one range: the primary record moving to
// kCommitted. Every other range's intents are cleanup, resolved lazily by
// whoever next reads them. A reader that meets an intent goes to the primary
// and asks; that is INV-TXN-02 and it is the whole of the atomicity argument.

#ifndef ANVIL_CORE_TXN_COORDINATOR_H_
#define ANVIL_CORE_TXN_COORDINATOR_H_

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "anvil/core/shard/store.h"
#include "anvil/core/txn/command.h"
#include "anvil/core/txn/record.h"
#include "anvil/core/txn/timestamp.h"
#include "anvil/core/runtime/runtime.h"

namespace anvil::txn {

enum class Level : std::uint8_t {
  kSnapshot = 0,
  kSerializable,
  kStrictSerializable,
};

const char* to_string(Level level) noexcept;

struct CoordinatorOptions {
  Level level = Level::kSerializable;
  TsSource source = TsSource::kOracle;

  Duration rpc_timeout = Duration::millis(1500);
  Duration poll = Duration::millis(5);
  Duration retry_backoff = Duration::millis(20);

  // How long an intent outlives its coordinator's last heartbeat before anyone
  // blocked on it may abort the transaction outright. Short enough that a dead
  // coordinator does not block a key for the rest of the run; long enough that
  // a slow one is not killed for being slow.
  Duration txn_ttl = Duration::millis(1200);
  Duration heartbeat_interval = Duration::millis(300);

  std::uint32_t max_restarts = 12;

  // The clock bound the configuration declares. Used for the uncertainty window
  // and, in strict-serializable mode, for how long commit-wait waits.
  Duration clock_uncertainty = Duration::millis(10);

  // ---- deliberate-bug knobs (all default to correct) ----------------------

  // false: a transaction that was pushed forward commits at the higher
  // timestamp without re-checking what it read. That is exactly the write skew
  // that serializability forbids, wearing serializability's name. INV-TXN-05.
  bool refresh_reads_on_push = true;

  // false: the acknowledgement is sent as soon as the primary commits, without
  // waiting for the clock's lower bound to pass the commit timestamp. The
  // transaction is still serializable; it is no longer externally consistent,
  // and the only way to see it is a real-time edge in the history. INV-TXN-08.
  bool commit_wait = true;

  // false: the secondaries are prewritten before the primary.
  //
  // This comment used to claim that a crash between the two orders "leaves
  // intents with no record to resolve them against, and nobody can tell whether
  // the transaction happened". That is Percolator, where the primary *lock* is
  // the commit record. It is not this design, and the difference is worth
  // stating rather than leaving as a trap: here the record is a separate object
  // and `commit()` writes it -- kPending, or kStaging under parallel commit --
  // before any prewrite at all, whichever order this flag selects. The window
  // the claim describes never opens. What the flag reorders is the intents
  // among themselves, which changes which key a conflicting transaction
  // collides on first and nothing about safety, and it is why the drill carries
  // this knob as a control rather than a must-detect (test/txn_faults.cc).
  //
  // The property that actually carries the atomicity argument is the one
  // `push_record` enforces: a push against a transaction with no record writes
  // an *aborted* record, and `put_record` refuses to leave a terminal state, so
  // a coordinator that died before writing its own record can never come back
  // and commit over the verdict a reader already published. INV-TXN-02.
  bool primary_first = true;

  // false: parallel commit. The record goes to kStaging with the full key list
  // and the transaction is implicitly committed once every intent is present,
  // which saves a round trip. A recovering reader must reach the same verdict
  // the coordinator would have. INV-TXN-11.
  bool parallel_commit = false;

  // false: an uncertain read returns the older version instead of restarting
  // above the version that caused the doubt. INV-TXN-07.
  bool restart_on_uncertainty = true;
};

struct CoordinatorStats {
  std::uint64_t begun = 0;
  std::uint64_t committed = 0;
  std::uint64_t aborted = 0;
  std::uint64_t restarts = 0;
  std::uint64_t restarts_uncertain = 0;
  std::uint64_t restarts_refresh_failed = 0;
  std::uint64_t reads = 0;
  std::uint64_t reads_blocked = 0;
  std::uint64_t writes = 0;
  std::uint64_t prewrite_conflicts = 0;
  std::uint64_t pushes = 0;
  std::uint64_t pushes_aborted_expired = 0;
  std::uint64_t refreshes = 0;
  std::uint64_t commit_waits = 0;
  std::uint64_t commit_wait_nanos = 0;
  std::uint64_t rpc_timeouts = 0;
  std::uint64_t wrong_range = 0;
  std::uint64_t not_leader = 0;
  std::uint64_t unknown_outcomes = 0;
  std::uint64_t staging_commits = 0;
};

enum class TxnOutcome : std::uint8_t {
  kCommitted = 0,
  kAborted,
  kUnknown,  // the commit's fate is genuinely undecided from here
};

const char* to_string(TxnOutcome outcome) noexcept;

// One in-flight transaction, from the coordinator's side.
struct Handle {
  TxnId id = 0;
  std::uint32_t epoch = 1;
  Ts start_ts = 0;
  Ts commit_ts = 0;
  Ts uncertainty_limit = 0;
  Level level = Level::kSerializable;

  std::string primary;                            // the first key written
  std::map<std::string, Intent> writes;           // buffered until commit
  std::map<std::string, Ts> reads;                // key -> the version we saw
  std::uint32_t restarts = 0;
  bool committed = false;
};

// The coordinator runs on a node and drives transactions over the store's
// transactional client protocol. It holds no data of its own: everything
// durable is in a range.
class Coordinator {
 public:
  Coordinator(Runtime* runtime, NodeId self, shard::ShardStore* store,
              CoordinatorOptions options);

  // The timestamp source. The oracle needs somewhere to ask; the HLC does not.
  using OracleFn = std::function<Task<bool>(std::uint64_t count, Ts* first)>;
  void set_oracle(OracleFn oracle) { oracle_ = std::move(oracle); }

  const CoordinatorOptions& options() const noexcept { return options_; }
  void set_options(CoordinatorOptions options) { options_ = options; }
  const CoordinatorStats& stats() const noexcept { return stats_; }
  HybridClock& clock() noexcept { return clock_; }

  // ---- the interface -----------------------------------------------------
  Task<bool> begin(Handle* handle);
  Task<ReadStatus> get(Handle* handle, std::string_view key, bool* found, std::string* value);
  void put(Handle* handle, std::string_view key, std::string_view value);
  Task<TxnOutcome> commit(Handle* handle);
  Task<void> rollback(Handle* handle);

  // Keeps every in-flight transaction's record alive. Driven by the caller for
  // the same reason the MVCC janitor is: a state machine that starts its own
  // I/O is a state machine you cannot test deterministically.
  Task<void> heartbeat_all();

  const std::map<TxnId, Handle>& live() const noexcept { return live_; }

  // Who this coordinator is currently waiting on, for the distributed deadlock
  // check. Reported rather than inferred: a wait-for graph reconstructed from
  // the outside is a guess about what a coordinator was about to do.
  const std::map<TxnId, TxnId>& waits() const noexcept { return waits_; }

 private:
  struct Response {
    bool answered = false;
    std::uint8_t status = 0;
    TxnResult result;
    ReadResult read;
    NodeId hint{};
  };

  // `fresh` bypasses the reservation and asks the oracle for a timestamp now.
  // Required for a commit timestamp; see coordinator.cc.
  Task<bool> next_timestamp(Ts* out, bool fresh = false);
  Task<Response> send(bool read, const shard::RangeDescriptor& range, const TxnCommand& command,
                      std::string_view key, Ts read_ts, Ts uncertainty, TxnId reader);
  Task<bool> resolve_blocker(Handle* handle, std::string_view blocked_key,
                             const ReadResult& blocked);
  Task<bool> refresh_reads(Handle* handle, Ts up_to);
  Task<void> resolve_intents(Handle* handle, bool committed);
  Task<bool> put_record(Handle* handle, TxnStatus status, Ts commit_ts, bool with_keys);

  Runtime* runtime_;
  NodeId self_;
  shard::ShardStore* store_;
  CoordinatorOptions options_;
  OracleFn oracle_;
  HybridClock clock_;

  std::map<TxnId, Handle> live_;
  std::map<TxnId, TxnId> waits_;

  // Per range, the node the last answer said could serve it. Not a cache of the
  // topology -- the descriptor is that -- but of who is answering for it right
  // now, which is the question a lease makes different from membership. See
  // `send` and [ANV-0061].
  std::map<std::uint64_t, NodeId> leaders_;

  // Reserved oracle timestamps not yet handed out. Start timestamps only:
  // a commit timestamp is always drawn fresh, and the reason is ANV-0058 and
  // the long note on `next_timestamp` in coordinator.cc.
  Ts batch_next_ = 0;
  Ts batch_end_ = 0;

  std::uint64_t next_seq_ = 1;
  CoordinatorStats stats_;

  // Replies land here, keyed by sequence number. One slot per in-flight
  // request, and the coordinator issues one at a time per transaction.
  std::map<std::uint64_t, Response> inbox_;

 public:
  // Called by whoever owns the store's reply handler.
  void on_reply(const shard::ShardStore::TxnReply& reply);
};

}  // namespace anvil::txn

#endif  // ANVIL_CORE_TXN_COORDINATOR_H_
