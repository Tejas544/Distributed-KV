#include "anvil/core/txn/command.h"

#include "anvil/core/lsm/format.h"

namespace anvil::txn {

const char* to_string(TxnOp op) noexcept {
  switch (op) {
    case TxnOp::kPrewrite: return "prewrite";
    case TxnOp::kCommitIntent: return "commit-intent";
    case TxnOp::kRollbackIntent: return "rollback-intent";
    case TxnOp::kPutRecord: return "put-record";
    case TxnOp::kPushRecord: return "push-record";
  }
  return "?";
}

std::string TxnCommand::describe() const {
  std::string out = to_string(op);
  out += " t" + std::to_string(txn) + "." + std::to_string(epoch);
  if (!key.empty()) out += " '" + key + "'";
  if (start_ts != 0) out += " start " + std::to_string(start_ts);
  if (commit_ts != 0) out += " commit " + std::to_string(commit_ts);
  if (push_to != 0) out += " push " + std::to_string(push_to);
  return out;
}

std::string encode_txn_command(const TxnCommand& cmd) {
  std::string out;
  out.push_back(static_cast<char>(cmd.op));
  lsm::put_length_prefixed(&out, cmd.key);
  lsm::put_varint64(&out, cmd.txn);
  lsm::put_varint32(&out, cmd.epoch);
  lsm::put_varint64(&out, cmd.start_ts);
  lsm::put_varint64(&out, cmd.commit_ts);
  lsm::put_varint64(&out, cmd.push_to);
  out.push_back(static_cast<char>((cmd.tombstone ? 1 : 0) | (cmd.abort_expired ? 2 : 0)));
  lsm::put_length_prefixed(&out, cmd.value);
  lsm::put_length_prefixed(&out, cmd.primary);
  lsm::put_length_prefixed(&out, encode_record(cmd.record));
  lsm::put_varint64(&out, cmd.now);
  return out;
}

bool decode_txn_command(std::string_view in, TxnCommand* out) {
  const char* p = in.data();
  const char* limit = p + in.size();
  if (p >= limit) return false;
  const auto op = static_cast<std::uint8_t>(*p++);
  if (op > static_cast<std::uint8_t>(TxnOp::kPushRecord)) return false;
  out->op = static_cast<TxnOp>(op);

  std::string_view key;
  p = lsm::get_length_prefixed(p, limit, &key);
  if (p == nullptr) return false;
  p = lsm::get_varint64(p, limit, &out->txn);
  if (p == nullptr) return false;
  p = lsm::get_varint32(p, limit, &out->epoch);
  if (p == nullptr) return false;
  p = lsm::get_varint64(p, limit, &out->start_ts);
  if (p == nullptr) return false;
  p = lsm::get_varint64(p, limit, &out->commit_ts);
  if (p == nullptr) return false;
  p = lsm::get_varint64(p, limit, &out->push_to);
  if (p == nullptr) return false;
  if (p >= limit) return false;
  const auto flags = static_cast<std::uint8_t>(*p++);
  out->tombstone = (flags & 1) != 0;
  out->abort_expired = (flags & 2) != 0;
  std::string_view value;
  std::string_view primary;
  std::string_view record;
  p = lsm::get_length_prefixed(p, limit, &value);
  if (p == nullptr) return false;
  p = lsm::get_length_prefixed(p, limit, &primary);
  if (p == nullptr) return false;
  p = lsm::get_length_prefixed(p, limit, &record);
  if (p == nullptr) return false;
  if (!decode_record(record, &out->record)) return false;
  p = lsm::get_varint64(p, limit, &out->now);
  if (p == nullptr) return false;

  out->key.assign(key);
  out->value.assign(value);
  out->primary.assign(primary);
  return true;
}

std::string encode_txn_result(const TxnResult& result) {
  std::string out;
  out.push_back(static_cast<char>(result.outcome));
  out.push_back(static_cast<char>(result.status));
  lsm::put_varint64(&out, result.commit_ts);
  out.push_back(result.have_blocker ? 1 : 0);
  lsm::put_varint64(&out, result.blocker);
  lsm::put_varint32(&out, result.blocker_epoch);
  lsm::put_varint64(&out, result.blocker_start);
  lsm::put_length_prefixed(&out, result.blocker_primary);
  return out;
}

bool decode_txn_result(std::string_view in, TxnResult* out) {
  const char* p = in.data();
  const char* limit = p + in.size();
  if (p + 2 > limit) return false;
  const auto outcome = static_cast<std::uint8_t>(*p++);
  const auto status = static_cast<std::uint8_t>(*p++);
  if (outcome > static_cast<std::uint8_t>(WriteOutcome::kRejected)) return false;
  if (status > static_cast<std::uint8_t>(TxnStatus::kAborted)) return false;
  out->outcome = static_cast<WriteOutcome>(outcome);
  out->status = static_cast<TxnStatus>(status);
  p = lsm::get_varint64(p, limit, &out->commit_ts);
  if (p == nullptr) return false;
  if (p >= limit) return false;
  out->have_blocker = *p++ != 0;
  p = lsm::get_varint64(p, limit, &out->blocker);
  if (p == nullptr) return false;
  p = lsm::get_varint32(p, limit, &out->blocker_epoch);
  if (p == nullptr) return false;
  p = lsm::get_varint64(p, limit, &out->blocker_start);
  if (p == nullptr) return false;
  std::string_view primary;
  p = lsm::get_length_prefixed(p, limit, &primary);
  if (p == nullptr) return false;
  out->blocker_primary.assign(primary);
  return true;
}

TxnResult apply_txn_command(VersionStore* store, const TxnCommand& cmd) {
  TxnResult result;
  switch (cmd.op) {
    case TxnOp::kPrewrite: {
      Intent intent;
      intent.txn = cmd.txn;
      intent.epoch = cmd.epoch;
      intent.start_ts = cmd.start_ts;
      intent.tombstone = cmd.tombstone;
      intent.value = cmd.value;
      intent.primary = cmd.primary;
      Intent blocker;
      result.outcome = store->prewrite(cmd.key, intent, &blocker);
      if (result.outcome == WriteOutcome::kLocked) {
        result.have_blocker = true;
        result.blocker = blocker.txn;
        result.blocker_epoch = blocker.epoch;
        result.blocker_start = blocker.start_ts;
        result.blocker_primary = blocker.primary;
      }
      break;
    }

    case TxnOp::kCommitIntent:
      result.outcome = store->commit_intent(cmd.key, cmd.txn, cmd.epoch, cmd.commit_ts);
      result.commit_ts = cmd.commit_ts;
      break;

    case TxnOp::kRollbackIntent:
      result.outcome = store->rollback_intent(cmd.key, cmd.txn, cmd.epoch);
      break;

    case TxnOp::kPutRecord: {
      result.outcome = store->put_record(cmd.record);
      const TxnRecord* after = store->find_record(cmd.record.id);
      if (after != nullptr) {
        result.status = after->status;
        result.commit_ts = after->commit_ts;
      }
      break;
    }

    case TxnOp::kPushRecord: {
      TxnRecord after;
      result.outcome = store->push_record(cmd.txn, cmd.push_to, cmd.now, cmd.abort_expired,
                                          cmd.key, &after);
      result.status = after.status;
      result.commit_ts = after.commit_ts;
      break;
    }
  }
  return result;
}

}  // namespace anvil::txn
