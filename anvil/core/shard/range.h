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
  std::size_t key_count() const noexcept { return balances_.size(); }

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

  // The split this range has performed and whose right-hand side has not yet
  // been given its data. Held until a kSplitConfirmed arrives, because the new
  // range's data exists nowhere else until then: it is not in any log, and a
  // crash that loses it loses half a range.
  struct PendingSplit {
    RangeId id{};
    std::string start;
    std::string end;
    std::vector<NodeId> replicas;
    std::vector<NodeId> learners;
    std::string payload;
  };
  const std::optional<PendingSplit>& pending_split() const noexcept { return pending_split_; }

  // A merge this range has absorbed; the subsumed group must now be destroyed
  // on this node. Drained by the store, which is the only thing that can.
  std::vector<RangeId> take_subsumed();

  // The result of the last apply, for the store to reply to the client with.
  // Set on every transfer, including the ones that were rejected.
  using ApplyCallback = std::function<void(LogIndex, const RangeCommand&, ApplyOutcome)>;
  void set_apply_callback(ApplyCallback callback) { on_apply_ = std::move(callback); }

  // Builds the payload a newly split range needs. Used by the new range's
  // leader, reading its own node's copy of the parent.
  static std::string encode_payload(const std::map<std::string, std::int64_t>& balances,
                                    const std::map<std::uint64_t, ApplyOutcome>& decided);
  static bool decode_payload(std::string_view in, std::map<std::string, std::int64_t>* balances,
                             std::map<std::uint64_t, ApplyOutcome>* decided);

  // The whole of this range's data, for a merge trigger.
  std::string payload() const { return encode_payload(balances_, decided_); }

 private:
  ApplyOutcome apply_transfer(const RangeCommand& cmd);

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
  std::optional<PendingSplit> pending_split_;
  std::vector<RangeId> subsumed_;

  LogIndex applied_{};
  std::uint64_t revision_ = 0;
  RangeStatsCounters counters_;
  ApplyCallback on_apply_;
};

}  // namespace anvil::shard

#endif  // ANVIL_CORE_SHARD_RANGE_H_
