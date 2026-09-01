// One range's replicated state machine: the data, the lease, and the triggers.
//
// The data is a bank. Every key is an account holding an integer, and the only
// operation is a transfer of some amount from one account to another. That is
// not a toy chosen for brevity -- it is chosen because it comes with an oracle
// that no amount of sharding can weaken: the total never changes. A split that
// drops a key, a merge that loses one, a transfer that applies twice or applies
// half, a range that serves keys it no longer owns -- all of them move the
// total, and the total is one number checked at the end of the run.
//
// The descriptor lives here as well as in the placement driver's topology, and
// this copy is the one that decides whether a request is served. A request
// carries the generation its client had cached; the range compares it against
// the generation *it* has applied, and rejects a mismatch. Doing that check
// against the topology instead would mean a round trip to the placement driver
// on every request, and doing it nowhere is how a write lands in a range that
// no longer owns the key and is never read again.
//
// Splits and merges are triggers in this log, not operations on it from
// outside. That is the whole reason they are atomic with respect to a
// transaction over the split point: a transfer either precedes the trigger in
// the log and applies under the old descriptor, or follows it and is rejected
// against the new one. There is no third possibility, and no lock anywhere.

#ifndef ANVIL_CORE_SHARD_RANGE_H_
#define ANVIL_CORE_SHARD_RANGE_H_

#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "anvil/core/raft/driver.h"
#include "anvil/core/shard/descriptor.h"
#include "anvil/core/txn/command.h"
#include "anvil/core/txn/store.h"
#include "anvil/core/types.h"

namespace anvil::shard {

enum class RangeOp : std::uint8_t {
  kNoop = 0,
  kTransfer,
  kSetDescriptor,   // the topology's view of this range, replicated into it
  kGrantLease,
  kSplitTrigger,    // this range becomes [start, key); the rest becomes a new range
  kInit,            // a newly split range receives its half of the data
  kSplitConfirmed,  // the new range is durable; the payload may be dropped
  kFreeze,          // subsumed by a merge: no further writes
  kMergeTrigger,    // absorb a frozen neighbour's data
  kTxn,             // P6: an opaque transactional command (txn/command.h)
};

const char* to_string(RangeOp op) noexcept;

struct RangeCommand {
  RangeOp op = RangeOp::kNoop;

  // transfer
  std::string from;
  std::string to;
  std::int64_t amount = 0;
  std::uint64_t op_id = 0;       // idempotence: a retry must be the same request
  std::uint64_t generation = 0;  // the descriptor the client had cached

  // descriptor, triggers
  RangeId other{};
  std::string start;
  std::string end;
  std::vector<NodeId> replicas;
  std::vector<NodeId> learners;
  NodeId node{};
  std::uint64_t time = 0;
  std::uint64_t expiry = 0;
  std::string payload;  // split or merge data

  // P6. An encoded txn::TxnCommand. Opaque here on purpose: the range
  // machine decodes exactly one field of it -- the key, which it needs for
  // the span and generation checks -- and hands the rest to the version
  // store. A range that knows what a prewrite is has learned something it
  // has no use for.
  std::string txn_command;

  std::string describe() const;
};

std::string encode_range_command(const RangeCommand& cmd);
bool decode_range_command(std::string_view in, RangeCommand* out);

enum class ApplyOutcome : std::uint8_t {
  kApplied = 0,
  kInsufficientFunds,
  kWrongRange,     // the key or the generation does not belong to this range
  kFrozen,         // subsumed by a merge in progress
  kDuplicate,      // already decided; the recorded outcome is returned
  kUninitialised,  // a split range that has not received its data yet
};

const char* to_string(ApplyOutcome outcome) noexcept;

struct RangeOptions {
  // The bound the configuration declares on how far apart two nodes' clocks
  // may be. A lease may not begin until the previous one has expired *by this
  // much*, because the node granting it is reading its own clock. A cluster
  // whose real skew exceeds the bound it declared can still see two holders --
  // that is the environment breaking its own promise, and the fault profile
  // does it on purpose so the consequence can be characterised rather than
  // assumed away.
  std::uint64_t clock_uncertainty_nanos = 10'000'000;

  // false: a lease is granted without checking that the previous one has
  // expired, so two nodes can hold overlapping leases over the same keys and
  // both serve reads under them. INV-SHARD-04.
  bool lease_requires_previous_expiry = true;

  // false: a range serves a request whose cached generation does not match the
  // one it has applied. This is the stale-route bug: a client that cached a
  // descriptor before a split writes a key the range no longer owns, the write
  // is accepted, and nobody ever reads it again. INV-SHARD-05, and the total
  // moves, so the conservation audit sees it too.
  bool checks_generation = true;

  // false: a range quiesces without requiring its replicas to be caught up, so
  // a replica that is behind stops receiving the entries that would catch it
  // up. INV-SHARD-08.
  bool quiesce_requires_caught_up = true;

  // P6. The deliberate-bug flags for this range's version store (first-
  // committer-wins, intent blocking, the terminal-status lattice, and
  // uncertainty handling -- see anvil/core/txn/store.h). Threaded through
  // rather than left at the default so the fault-injected drill can reach
  // them the same way it reaches every other layer's.
  txn::StoreOptions txn;
};

struct RangeStatsCounters {
  std::uint64_t transfers_applied = 0;
  std::uint64_t transfers_rejected_wrong_range = 0;
  // Split out from the span rejection above, because the drill needs to know
  // whether the generation check is doing anything the span check was not
  // already doing. Without the split, "the mutation was not detected" and "the
  // mutation did not change anything" are indistinguishable.
  std::uint64_t transfers_rejected_generation = 0;
  std::uint64_t transfers_rejected_funds = 0;
  std::uint64_t transfers_duplicate = 0;
  std::uint64_t txn_commands_applied = 0;
  std::uint64_t leases_granted = 0;
  std::uint64_t splits_applied = 0;
  std::uint64_t merges_applied = 0;
};

class RangeMachine : public raft::StateMachine {
 public:
  // `initialised` is false for a range born from a split: it exists, it knows
  // its span, and it holds nothing until its kInit entry commits. Serving a
  // read from a range in that state would return "the account does not exist"
  // for an account that certainly does.
  RangeMachine(RangeDescriptor descriptor, RangeOptions options, bool initialised);

  void apply(LogIndex index, std::string_view command) override;
  std::string snapshot() const override;
  void restore(std::string_view data) override;

  // ---- observation -------------------------------------------------------
  const RangeDescriptor& descriptor() const noexcept { return desc_; }
  const std::map<std::string, std::int64_t>& balances() const noexcept { return balances_; }
  const std::map<std::uint64_t, ApplyOutcome>& decided() const noexcept { return decided_; }
  LogIndex applied_index() const noexcept { return applied_; }
  bool initialised() const noexcept { return initialised_; }
  bool frozen() const noexcept { return desc_.frozen; }
  const RangeStatsCounters& counters() const noexcept { return counters_; }
  std::uint64_t revision() const noexcept { return revision_; }

  std::int64_t total() const;
  // Both halves of what this range holds: the bank's balances (P5) and the
  // transactional keys (P6). A range that only ever sees one of the two --
  // every range in practice, since a workload picks a mechanism, not both --
  // still needs the sum, because a placement decision drawn from only one
  // half of an empty-looking range never splits it.
  std::size_t key_count() const noexcept { return balances_.size() + txn_.key_count(); }

  // The key that would divide this range's data in half. Empty when the range
  // is too small to split. Reported to the placement driver, because the driver
  // must not need to know a single key to decide anything (INV-SHARD-09).
  std::string median_key() const;

  // The lease this one replaced, kept so that the pair can be checked as a
  // pair. An observer that samples the current lease every tick cannot do that:
  // two grants can be applied in one batch, and it then compares two leases
  // that were never adjacent and reports a gap that nobody ever had.
  const Lease& previous_lease() const noexcept { return previous_lease_; }

  // Whether the local node may serve a read for this range right now.
  bool lease_valid(NodeId self, std::uint64_t now) const {
    return desc_.lease.holder == self && desc_.lease.valid_at(now);
  }

  // The splits this range has performed whose right-hand sides have not yet
  // been given their data. Held until a kSplitConfirmed arrives, because the new
  // range's data exists nowhere else until then: it is not in any log, and a
  // crash that loses it loses half a range.
  //
  // A map rather than a single slot, keyed by the child's id, and both halves
  // of that matter. A range can owe data to more than one child at a time --
  // split off a right-hand side, then split again before the first child's
  // leader has picked its payload up -- and a single slot silently overwrote
  // the first, destroying the only copy of those keys. The audit has always
  // assumed several ("a node can be the parent of more than one unhanded-over
  // split at a time", store.cc); the state it audits just could not represent
  // it.
  struct PendingSplit {
    RangeId id{};
    std::string start;
    std::string end;
    std::vector<NodeId> replicas;
    std::vector<NodeId> learners;
    std::string payload;
  };
  const std::map<std::uint64_t, PendingSplit>& pending_splits() const noexcept {
    return pending_splits_;
  }

  // A merge this range has absorbed; the subsumed group must now be destroyed
  // on this node. Drained by the store, which is the only thing that can.
  std::vector<RangeId> take_subsumed();

  // The result of the last apply, for the store to reply to the client with.
  // Set on every transfer, including the ones that were rejected.
  using ApplyCallback = std::function<void(LogIndex, const RangeCommand&, ApplyOutcome)>;
  void set_apply_callback(ApplyCallback callback) { on_apply_ = std::move(callback); }

  // P6. What a transactional command decided, reported to whoever proposed
  // it. Separate from the callback above because the answers are different
  // shapes: a transfer succeeded or it did not, and a prewrite may have
  // been blocked by a transaction the caller now has to go and push.
  using TxnCallback =
      std::function<void(LogIndex, const txn::TxnCommand&, const txn::TxnResult&)>;
  void set_txn_callback(TxnCallback callback) { on_txn_ = std::move(callback); }

  // The transactional state this range holds. Read directly by the lease
  // holder to serve a snapshot read, and by the checker.
  const txn::VersionStore& txn_store() const noexcept { return txn_; }
  txn::VersionStore& txn_store() noexcept { return txn_; }

  // Builds the payload a newly split range needs. Used by the new range's
  // leader, reading its own node's copy of the parent.
  static std::string encode_payload(const std::map<std::string, std::int64_t>& balances,
                                    const std::map<std::uint64_t, ApplyOutcome>& decided);
  static bool decode_payload(std::string_view in, std::map<std::string, std::int64_t>* balances,
                             std::map<std::uint64_t, ApplyOutcome>* decided);

  // The transactional section of a span payload, for a reader that has the
  // bytes but no RangeMachine to load them into. The audit needs it: a split
  // that has left its parent and not yet reached its child holds the only copy
  // of those keys in a payload, and a checker that cannot read it reports every
  // in-flight split as missing money (which is what P5's audit already walks
  // pending splits to avoid, one layer down).
  static bool decode_txn_section(std::string_view in, std::string_view* out);

  // The whole of this range's state for [lo, hi) -- balances, the decision
  // ledger, and the transactional half -- and the two ways of taking one back
  // in. `merge` distinguishes absorbing a neighbour from being initialised.
  // A merge trigger's payload is `encode_span({}, {})`, the whole range: the
  // now-retired `payload()` gave callers only the bank half, encode_payload(
  // balances_, decided_) with no txn_ section at all, and a merge that used it
  // absorbed a neighbour's committed transactions, live intents, and records
  // into nothing -- every one of them silently gone the moment the merge
  // committed, with the coordinator that heard "committed" none the wiser.
  std::string encode_span(std::string_view lo, std::string_view hi) const;
  bool load_span(std::string_view in, bool merge);

 private:
  ApplyOutcome apply_transfer(const RangeCommand& cmd);
  static const char* decode_bank(const char* p, const char* limit,
                                 std::map<std::string, std::int64_t>* balances,
                                 std::map<std::uint64_t, ApplyOutcome>* decided);

  RangeDescriptor desc_;
  RangeOptions options_;
  bool initialised_ = true;

  std::map<std::string, std::int64_t> balances_;

  // Every transfer this range has decided, and what it decided. The set is the
  // idempotence table and the audit ledger at once: a retry gets the recorded
  // answer rather than a second application, and at the end of the run every
  // acknowledged operation has to be findable in the union of these.
  std::map<std::uint64_t, ApplyOutcome> decided_;

  Lease previous_lease_;

  // P6. Versions, intents and transaction records for the keys in this
  // range. Empty for a range that only ever sees transfers, which is why
  // adding it did not disturb anything in P5.
  txn::VersionStore txn_;
  TxnCallback on_txn_;

  std::map<std::uint64_t, PendingSplit> pending_splits_;  // child range id -> its data
  std::vector<RangeId> subsumed_;

  LogIndex applied_{};
  std::uint64_t revision_ = 0;
  RangeStatsCounters counters_;
  ApplyCallback on_apply_;
};

}  // namespace anvil::shard

#endif  // ANVIL_CORE_SHARD_RANGE_H_
