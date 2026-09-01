#include "anvil/core/shard/range.h"

#include <algorithm>
#include <utility>

#include "anvil/core/lsm/format.h"

namespace anvil::shard {
namespace {

void put_nodes(std::string* out, const std::vector<NodeId>& nodes) {
  lsm::put_varint32(out, static_cast<std::uint32_t>(nodes.size()));
  for (const NodeId n : nodes) lsm::put_varint64(out, n.value());
}

const char* get_nodes(const char* p, const char* limit, std::vector<NodeId>* out) {
  std::uint32_t count = 0;
  p = lsm::get_varint32(p, limit, &count);
  if (p == nullptr) return nullptr;
  out->clear();
  for (std::uint32_t i = 0; i < count; ++i) {
    std::uint64_t id = 0;
    p = lsm::get_varint64(p, limit, &id);
    if (p == nullptr) return nullptr;
    out->push_back(NodeId{id});
  }
  return p;
}

// Amounts are signed and travel as a zigzag varint. A plain varint of a
// negative int64 is ten bytes of ones, which is correct and wasteful; more to
// the point, a sign that survives the round trip only by accident is a sign
// that will not survive the day somebody stores it in a different width.
void put_signed(std::string* out, std::int64_t value) {
  const std::uint64_t zigzag =
      (static_cast<std::uint64_t>(value) << 1) ^ static_cast<std::uint64_t>(value >> 63);
  lsm::put_varint64(out, zigzag);
}

const char* get_signed(const char* p, const char* limit, std::int64_t* value) {
  std::uint64_t zigzag = 0;
  p = lsm::get_varint64(p, limit, &zigzag);
  if (p == nullptr) return nullptr;
  *value = static_cast<std::int64_t>((zigzag >> 1) ^ (~(zigzag & 1) + 1));
  return p;
}

}  // namespace

const char* to_string(RangeOp op) noexcept {
  switch (op) {
    case RangeOp::kNoop: return "noop";
    case RangeOp::kTransfer: return "transfer";
    case RangeOp::kSetDescriptor: return "set-descriptor";
    case RangeOp::kGrantLease: return "grant-lease";
    case RangeOp::kSplitTrigger: return "split-trigger";
    case RangeOp::kInit: return "init";
    case RangeOp::kSplitConfirmed: return "split-confirmed";
    case RangeOp::kFreeze: return "freeze";
    case RangeOp::kMergeTrigger: return "merge-trigger";
    case RangeOp::kTxn: return "txn";
  }
  return "?";
}

const char* to_string(ApplyOutcome outcome) noexcept {
  switch (outcome) {
    case ApplyOutcome::kApplied: return "applied";
    case ApplyOutcome::kInsufficientFunds: return "insufficient-funds";
    case ApplyOutcome::kWrongRange: return "wrong-range";
    case ApplyOutcome::kFrozen: return "frozen";
    case ApplyOutcome::kDuplicate: return "duplicate";
    case ApplyOutcome::kUninitialised: return "uninitialised";
  }
  return "?";
}

std::string RangeCommand::describe() const {
  std::string out = to_string(op);
  if (op == RangeOp::kTransfer) {
    out += " #" + std::to_string(op_id) + " " + from + "->" + to + " " + std::to_string(amount) +
           " gen " + std::to_string(generation);
  }
  if (other.valid()) out += " r" + std::to_string(other.value());
  if (!end.empty()) out += " end " + end;
  return out;
}

std::string encode_range_command(const RangeCommand& cmd) {
  std::string out;
  out.push_back(static_cast<char>(cmd.op));
  lsm::put_length_prefixed(&out, cmd.from);
  lsm::put_length_prefixed(&out, cmd.to);
  put_signed(&out, cmd.amount);
  lsm::put_varint64(&out, cmd.op_id);
  lsm::put_varint64(&out, cmd.generation);
  lsm::put_varint64(&out, cmd.other.value());
  lsm::put_length_prefixed(&out, cmd.start);
  lsm::put_length_prefixed(&out, cmd.end);
  put_nodes(&out, cmd.replicas);
  put_nodes(&out, cmd.learners);
  lsm::put_varint64(&out, cmd.node.value());
  lsm::put_varint64(&out, cmd.time);
  lsm::put_varint64(&out, cmd.expiry);
  lsm::put_length_prefixed(&out, cmd.payload);
  lsm::put_length_prefixed(&out, cmd.txn_command);
  return out;
}

bool decode_range_command(std::string_view in, RangeCommand* out) {
  const char* p = in.data();
  const char* limit = p + in.size();
  if (p >= limit) return false;
  const auto op = static_cast<std::uint8_t>(*p++);
  if (op > static_cast<std::uint8_t>(RangeOp::kTxn)) return false;
  out->op = static_cast<RangeOp>(op);

  std::string_view from;
  std::string_view to;
  p = lsm::get_length_prefixed(p, limit, &from);
  if (p == nullptr) return false;
  p = lsm::get_length_prefixed(p, limit, &to);
  if (p == nullptr) return false;
  p = get_signed(p, limit, &out->amount);
  if (p == nullptr) return false;
  p = lsm::get_varint64(p, limit, &out->op_id);
  if (p == nullptr) return false;
  p = lsm::get_varint64(p, limit, &out->generation);
  if (p == nullptr) return false;
  std::uint64_t other = 0;
  p = lsm::get_varint64(p, limit, &other);
  if (p == nullptr) return false;
  std::string_view start;
  std::string_view end;
  p = lsm::get_length_prefixed(p, limit, &start);
  if (p == nullptr) return false;
  p = lsm::get_length_prefixed(p, limit, &end);
  if (p == nullptr) return false;
  p = get_nodes(p, limit, &out->replicas);
  if (p == nullptr) return false;
  p = get_nodes(p, limit, &out->learners);
  if (p == nullptr) return false;
  std::uint64_t node = 0;
  p = lsm::get_varint64(p, limit, &node);
  if (p == nullptr) return false;
  p = lsm::get_varint64(p, limit, &out->time);
  if (p == nullptr) return false;
  p = lsm::get_varint64(p, limit, &out->expiry);
  if (p == nullptr) return false;
  std::string_view payload;
  p = lsm::get_length_prefixed(p, limit, &payload);
  if (p == nullptr) return false;
  std::string_view txn_command;
  p = lsm::get_length_prefixed(p, limit, &txn_command);
  if (p == nullptr) return false;

  out->from.assign(from);
  out->to.assign(to);
  out->other = RangeId{other};
  out->start.assign(start);
  out->end.assign(end);
  out->node = NodeId{node};
  out->payload.assign(payload);
  out->txn_command.assign(txn_command);
  return true;
}

// ---------------------------------------------------------------------------
// payload
// ---------------------------------------------------------------------------

std::string RangeMachine::encode_payload(const std::map<std::string, std::int64_t>& balances,
                                         const std::map<std::uint64_t, ApplyOutcome>& decided) {
  std::string out;
  lsm::put_varint32(&out, static_cast<std::uint32_t>(balances.size()));
  for (const auto& [key, value] : balances) {
    lsm::put_length_prefixed(&out, key);
    put_signed(&out, value);
  }
  lsm::put_varint32(&out, static_cast<std::uint32_t>(decided.size()));
  for (const auto& [id, outcome] : decided) {
    lsm::put_varint64(&out, id);
    out.push_back(static_cast<char>(outcome));
  }
  return out;
}

// The bank half of a payload, returning where it ended so that the
// transactional half -- which P6 appends and P5 does not write at all -- can be
// parsed from the same buffer. Two sections rather than one combined format,
// because a range that only ever sees transfers still has to be able to read a
// payload written by a range that also holds versions, and vice versa.
const char* RangeMachine::decode_bank(const char* p, const char* limit,
                                      std::map<std::string, std::int64_t>* balances,
                                      std::map<std::uint64_t, ApplyOutcome>* decided) {
  std::uint32_t count = 0;
  p = lsm::get_varint32(p, limit, &count);
  if (p == nullptr) return nullptr;
  for (std::uint32_t i = 0; i < count; ++i) {
    std::string_view key;
    std::int64_t value = 0;
    p = lsm::get_length_prefixed(p, limit, &key);
    if (p == nullptr) return nullptr;
    p = get_signed(p, limit, &value);
    if (p == nullptr) return nullptr;
    (*balances)[std::string{key}] = value;
  }
  p = lsm::get_varint32(p, limit, &count);
  if (p == nullptr) return nullptr;
  for (std::uint32_t i = 0; i < count; ++i) {
    std::uint64_t id = 0;
    p = lsm::get_varint64(p, limit, &id);
    if (p == nullptr) return nullptr;
    if (p >= limit) return nullptr;
    (*decided)[id] = static_cast<ApplyOutcome>(static_cast<std::uint8_t>(*p++));
  }
  return p;
}

bool RangeMachine::decode_payload(std::string_view in,
                                  std::map<std::string, std::int64_t>* balances,
                                  std::map<std::uint64_t, ApplyOutcome>* decided) {
  return decode_bank(in.data(), in.data() + in.size(), balances, decided) != nullptr;
}

// The whole of this range's state for [lo, hi): balances, the decision ledger,
// and the transactional half. The transactional section is length-prefixed and
// may be absent, which is what makes a P5 payload readable here.
std::string RangeMachine::encode_span(std::string_view lo, std::string_view hi) const {
  std::map<std::string, std::int64_t> slice;
  for (const auto& [key, value] : balances_) {
    if (key < lo) continue;
    if (!hi.empty() && key >= hi) continue;
    slice.emplace(key, value);
  }
  std::string out = encode_payload(slice, decided_);
  lsm::put_length_prefixed(&out, txn_.encode_span(lo, hi));
  return out;
}

bool RangeMachine::load_span(std::string_view in, bool merge) {
  std::map<std::string, std::int64_t> balances;
  std::map<std::uint64_t, ApplyOutcome> decided;
  const char* p = decode_bank(in.data(), in.data() + in.size(), &balances, &decided);
  if (p == nullptr) return false;

  if (merge) {
    for (const auto& [key, value] : balances) balances_[key] += value;
    for (const auto& [id, outcome] : decided) decided_.emplace(id, outcome);
  } else {
    balances_ = std::move(balances);
    decided_ = std::move(decided);
  }

  const char* limit = in.data() + in.size();
  if (p >= limit) return true;  // a payload written before P6 existed
  std::string_view txn_part;
  p = lsm::get_length_prefixed(p, limit, &txn_part);
  if (p == nullptr) return false;
  return merge ? txn_.absorb(txn_part) : txn_.load(txn_part);
}

// ---------------------------------------------------------------------------
// RangeMachine
// ---------------------------------------------------------------------------

RangeMachine::RangeMachine(RangeDescriptor descriptor, RangeOptions options, bool initialised)
    : desc_(std::move(descriptor)), options_(options), initialised_(initialised) {
  txn_.set_options(options_.txn);
}

std::int64_t RangeMachine::total() const {
  std::int64_t sum = 0;
  for (const auto& [key, value] : balances_) sum += value;
  return sum;
}

std::string RangeMachine::median_key() const {
  if (balances_.size() >= 2) {
    auto it = balances_.begin();
    std::advance(it, static_cast<std::ptrdiff_t>(balances_.size() / 2));
    // A split key equal to the range's own start produces an empty left half,
    // which tiles the key space perfectly and breaks everything that assumes a
    // range contains a key.
    if (it != balances_.begin() && it->first > desc_.start) return it->first;
  }
  // P6: a range that only ever sees transactional commands has an empty
  // balances_ forever (bootstrap seeds no accounts when the workload is a
  // coordinator's, not a transfer's), so this is not a fallback for a
  // near-empty range -- it is the only place a purely transactional range's
  // median comes from.
  const std::string txn_median = txn_.median_key();
  if (!txn_median.empty() && txn_median > desc_.start) return txn_median;
  return {};
}

std::vector<RangeId> RangeMachine::take_subsumed() {
  std::vector<RangeId> out;
  out.swap(subsumed_);
  return out;
}

ApplyOutcome RangeMachine::apply_transfer(const RangeCommand& cmd) {
  const auto seen = decided_.find(cmd.op_id);
  if (seen != decided_.end()) {
    // The *recorded* outcome, not "duplicate". A retry has to be answered with
    // what was decided the first time: reporting a second attempt at a transfer
    // that failed for want of funds as a success is a lost write invented by
    // the idempotence table, which is the one place it must not come from.
    ++counters_.transfers_duplicate;
    return seen->second;
  }
  if (!initialised_) return ApplyOutcome::kUninitialised;
  if (desc_.frozen) return ApplyOutcome::kFrozen;

  // The routing check, and the reason this state machine holds a descriptor at
  // all. Both keys, because a transfer that spans the split point must fail
  // rather than move half the money: the left key would be debited here and the
  // right key credited in a range that never heard of it.
  if (!desc_.covers(cmd.from, cmd.to) && !desc_.covers(cmd.to, cmd.from)) {
    ++counters_.transfers_rejected_wrong_range;
    return ApplyOutcome::kWrongRange;
  }
  if (options_.checks_generation && cmd.generation != desc_.generation) {
    ++counters_.transfers_rejected_wrong_range;
    ++counters_.transfers_rejected_generation;
    return ApplyOutcome::kWrongRange;
  }

  std::int64_t& from = balances_[cmd.from];
  if (from < cmd.amount) {
    ++counters_.transfers_rejected_funds;
    decided_[cmd.op_id] = ApplyOutcome::kInsufficientFunds;
    return ApplyOutcome::kInsufficientFunds;
  }
  from -= cmd.amount;
  balances_[cmd.to] += cmd.amount;
  decided_[cmd.op_id] = ApplyOutcome::kApplied;
  ++counters_.transfers_applied;
  return ApplyOutcome::kApplied;
}

void RangeMachine::apply(LogIndex index, std::string_view command) {
  applied_ = index;
  ++revision_;
  RangeCommand cmd;
  if (!decode_range_command(command, &cmd)) return;

  ApplyOutcome outcome = ApplyOutcome::kApplied;
  switch (cmd.op) {
    case RangeOp::kNoop:
      break;

    case RangeOp::kTransfer:
      outcome = apply_transfer(cmd);
      break;

    case RangeOp::kSetDescriptor: {
      // The topology's view of the *membership*, arriving through this range's
      // own log so that every replica adopts it at the same index.
      //
      // It deliberately does not touch the span. A range's span changes only
      // through a trigger, because a trigger is what moves the data with it:
      // adopting a shorter end from the topology leaves this machine holding
      // keys it no longer claims, and that is not a transient -- it is a set of
      // accounts that exist in a range nobody will route to again. It happens
      // for real whenever a merge is aborted after the survivor absorbed
      // (ANV-0037), and the fix is the rule rather than the special case.
      if (cmd.generation < desc_.generation) break;
      desc_.generation = cmd.generation;
      desc_.replicas = cmd.replicas;
      desc_.learners = cmd.learners;
      break;
    }

    case RangeOp::kGrantLease: {
      // The previous expiry, plus TWICE the declared clock bound.
      //
      // Twice, and the factor is the whole point. The bound is what one node's
      // clock may be wrong by; a lease handover compares two nodes' clocks, and
      // they can be wrong in opposite directions -- the old holder slow, the new
      // one fast -- so the interval that has to have elapsed on the old holder's
      // clock is two bounds wide, not one. With one bound the implementation is
      // safe on most seeds and wrong on the ones where the two errors happen to
      // point apart, which is exactly the shape of bug that ships (ANV-0038).
      //
      // Nothing about this is visible from the client except as a stale read
      // that a retry makes go away.
      const std::uint64_t margin = 2 * options_.clock_uncertainty_nanos;
      if (options_.lease_requires_previous_expiry && cmd.node != desc_.lease.holder &&
          cmd.time < desc_.lease.expiry + margin) {
        // The previous lease has not expired on the granting node's clock. Two
        // holders at once is not a race that resolves itself: both serve reads
        // locally, and one of them is serving state the other has already
        // overwritten.
        break;
      }
      if (cmd.node != desc_.lease.holder) previous_lease_ = desc_.lease;
      desc_.lease.holder = cmd.node;
      desc_.lease.start = cmd.time;
      desc_.lease.expiry = cmd.expiry;
      ++counters_.leases_granted;
      break;
    }

    case RangeOp::kSplitTrigger: {
      if (desc_.frozen || !initialised_) break;
      if (cmd.end.empty() || cmd.end <= desc_.start) break;
      if (!desc_.end.empty() && cmd.end >= desc_.end) break;

      PendingSplit split;
      split.id = cmd.other;
      split.start = cmd.end;  // the split key is the new range's start
      split.end = desc_.end;
      split.replicas = cmd.replicas.empty() ? desc_.replicas : cmd.replicas;
      split.learners = cmd.learners.empty() ? desc_.learners : cmd.learners;

      // Everything in [split.start, old end) leaves, balances and versions and
      // intents and the records whose primary key went with them. The decision
      // ledger goes to *both* halves: it is the idempotence table, and a client
      // retrying an operation decided before the split must get the recorded
      // answer from whichever range now owns its keys.
      split.payload = encode_span(split.start, split.end);
      for (auto it = balances_.lower_bound(split.start); it != balances_.end();) {
        it = balances_.erase(it);
      }
      txn_.erase_span(split.start, split.end);

      desc_.end = split.start;
      if (cmd.generation > desc_.generation) desc_.generation = cmd.generation;
      pending_split_ = std::move(split);
      ++counters_.splits_applied;
      break;
    }

    case RangeOp::kInit: {
      if (initialised_) break;  // a second leader proposing the same thing
      if (!load_span(cmd.payload, /*merge=*/false)) break;
      // The span comes from the log too, and this is not decoration.
      //
      // A replica added to an existing range later -- a rebalance, a learner --
      // is constructed from the descriptor the topology has *now*, and is then
      // fed this range's log from the beginning. Replaying a history against
      // the wrong starting state is not a replay: the split trigger that took
      // this range from [a,c) to [a,b) is rejected, because the machine already
      // thinks it ends at b, and the keys the trigger would have moved stay
      // behind. The result is a replica holding accounts it does not claim, and
      // the only symptom is a god's-eye invariant (ANV-0039).
      desc_.start = cmd.start;
      desc_.end = cmd.end;
      if (cmd.generation > 0) desc_.generation = cmd.generation;
      initialised_ = true;
      break;
    }

    case RangeOp::kSplitConfirmed:
      if (pending_split_.has_value() && pending_split_->id == cmd.other) pending_split_.reset();
      break;

    case RangeOp::kFreeze:
      desc_.frozen = true;
      break;

    case RangeOp::kMergeTrigger: {
      if (desc_.frozen) break;
      if (cmd.start != desc_.end) break;  // not our right-hand neighbour any more
      if (!load_span(cmd.payload, /*merge=*/true)) break;
      desc_.end = cmd.end;
      if (cmd.generation > desc_.generation) desc_.generation = cmd.generation;
      subsumed_.push_back(cmd.other);
      ++counters_.merges_applied;
      break;
    }

    case RangeOp::kTxn: {
      // The range checks two things and hands over the rest: that the key is
      // one it owns, and that the client was looking at the descriptor it has
      // now. Everything else about a prewrite belongs to the version store.
      txn::TxnCommand command;
      if (!decode_txn_command(cmd.txn_command, &command)) break;

      txn::TxnResult result;
      if (!initialised_) {
        result.outcome = txn::WriteOutcome::kRejected;
      } else if (desc_.frozen) {
        result.outcome = txn::WriteOutcome::kRejected;
      } else if (!desc_.contains(command.key)) {
        ++counters_.transfers_rejected_wrong_range;
        result.outcome = txn::WriteOutcome::kRejected;
      } else if (options_.checks_generation && cmd.generation != 0 &&
                 cmd.generation != desc_.generation) {
        ++counters_.transfers_rejected_wrong_range;
        ++counters_.transfers_rejected_generation;
        result.outcome = txn::WriteOutcome::kRejected;
      } else {
        result = txn::apply_txn_command(&txn_, command);
        ++counters_.txn_commands_applied;
      }
      if (on_txn_) on_txn_(index, command, result);
      return;  // the transactional callback is the reply; there is no ApplyOutcome
    }
  }

  if (on_apply_) on_apply_(index, cmd, outcome);
}

std::string RangeMachine::snapshot() const {
  std::string out;
  lsm::put_length_prefixed(&out, encode_descriptor(desc_));
  out.push_back(initialised_ ? 1 : 0);
  lsm::put_length_prefixed(&out, encode_span(std::string_view{}, std::string_view{}));
  out.push_back(pending_split_.has_value() ? 1 : 0);
  if (pending_split_.has_value()) {
    lsm::put_varint64(&out, pending_split_->id.value());
    lsm::put_length_prefixed(&out, pending_split_->start);
    lsm::put_length_prefixed(&out, pending_split_->end);
    put_nodes(&out, pending_split_->replicas);
    put_nodes(&out, pending_split_->learners);
    lsm::put_length_prefixed(&out, pending_split_->payload);
  }
  return out;
}

void RangeMachine::restore(std::string_view data) {
  const char* p = data.data();
  const char* limit = p + data.size();
  std::string_view encoded;
  p = lsm::get_length_prefixed(p, limit, &encoded);
  if (p == nullptr) return;
  RangeDescriptor desc;
  if (!decode_descriptor(encoded, &desc)) return;
  if (p >= limit) return;
  const bool initialised = *p++ != 0;
  std::string_view payload;
  p = lsm::get_length_prefixed(p, limit, &payload);
  if (p == nullptr) return;

  desc_ = std::move(desc);
  initialised_ = initialised;
  pending_split_.reset();
  if (!load_span(payload, /*merge=*/false)) return;

  if (p < limit && *p++ != 0) {
    PendingSplit split;
    std::uint64_t id = 0;
    p = lsm::get_varint64(p, limit, &id);
    if (p == nullptr) return;
    std::string_view start;
    std::string_view end;
    p = lsm::get_length_prefixed(p, limit, &start);
    if (p == nullptr) return;
    p = lsm::get_length_prefixed(p, limit, &end);
    if (p == nullptr) return;
    p = get_nodes(p, limit, &split.replicas);
    if (p == nullptr) return;
    p = get_nodes(p, limit, &split.learners);
    if (p == nullptr) return;
    std::string_view split_payload;
    p = lsm::get_length_prefixed(p, limit, &split_payload);
    if (p == nullptr) return;
    split.id = RangeId{id};
    split.start.assign(start);
    split.end.assign(end);
    split.payload.assign(split_payload);
    pending_split_ = std::move(split);
  }
  ++revision_;
}

}  // namespace anvil::shard
