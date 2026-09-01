#include "anvil/core/txn/record.h"

#include "anvil/core/lsm/format.h"

namespace anvil::txn {

const char* to_string(TxnStatus status) noexcept {
  switch (status) {
    case TxnStatus::kPending: return "pending";
    case TxnStatus::kStaging: return "staging";
    case TxnStatus::kCommitted: return "committed";
    case TxnStatus::kAborted: return "aborted";
  }
  return "?";
}

const char* to_string(ReadStatus status) noexcept {
  switch (status) {
    case ReadStatus::kOk: return "ok";
    case ReadStatus::kBlocked: return "blocked";
    case ReadStatus::kUncertain: return "uncertain";
    case ReadStatus::kWrongRange: return "wrong-range";
    case ReadStatus::kUnavailable: return "unavailable";
  }
  return "?";
}

std::string encode_record(const TxnRecord& record) {
  std::string out;
  lsm::put_varint64(&out, record.id);
  lsm::put_varint32(&out, record.epoch);
  out.push_back(static_cast<char>(record.status));
  lsm::put_varint64(&out, record.start_ts);
  lsm::put_varint64(&out, record.commit_ts);
  lsm::put_varint32(&out, static_cast<std::uint32_t>(record.keys.size()));
  for (const std::string& key : record.keys) lsm::put_length_prefixed(&out, key);
  lsm::put_varint64(&out, record.heartbeat);
  lsm::put_varint64(&out, record.ttl_nanos);
  lsm::put_varint64(&out, record.pushed_to);
  return out;
}

bool decode_record(std::string_view in, TxnRecord* out) {
  const char* p = in.data();
  const char* limit = p + in.size();
  p = lsm::get_varint64(p, limit, &out->id);
  if (p == nullptr) return false;
  p = lsm::get_varint32(p, limit, &out->epoch);
  if (p == nullptr) return false;
  if (p >= limit) return false;
  const auto status = static_cast<std::uint8_t>(*p++);
  if (status > static_cast<std::uint8_t>(TxnStatus::kAborted)) return false;
  out->status = static_cast<TxnStatus>(status);
  p = lsm::get_varint64(p, limit, &out->start_ts);
  if (p == nullptr) return false;
  p = lsm::get_varint64(p, limit, &out->commit_ts);
  if (p == nullptr) return false;
  std::uint32_t count = 0;
  p = lsm::get_varint32(p, limit, &count);
  if (p == nullptr) return false;
  out->keys.clear();
  out->keys.reserve(count);
  for (std::uint32_t i = 0; i < count; ++i) {
    std::string_view key;
    p = lsm::get_length_prefixed(p, limit, &key);
    if (p == nullptr) return false;
    out->keys.emplace_back(key);
  }
  p = lsm::get_varint64(p, limit, &out->heartbeat);
  if (p == nullptr) return false;
  p = lsm::get_varint64(p, limit, &out->ttl_nanos);
  if (p == nullptr) return false;
  p = lsm::get_varint64(p, limit, &out->pushed_to);
  return p != nullptr;
}

std::string encode_intent(const Intent& intent) {
  std::string out;
  lsm::put_varint64(&out, intent.txn);
  lsm::put_varint32(&out, intent.epoch);
  lsm::put_varint64(&out, intent.start_ts);
  out.push_back(intent.tombstone ? 1 : 0);
  lsm::put_length_prefixed(&out, intent.value);
  lsm::put_length_prefixed(&out, intent.primary);
  return out;
}

bool decode_intent(std::string_view in, Intent* out) {
  const char* p = in.data();
  const char* limit = p + in.size();
  p = lsm::get_varint64(p, limit, &out->txn);
  if (p == nullptr) return false;
  p = lsm::get_varint32(p, limit, &out->epoch);
  if (p == nullptr) return false;
  p = lsm::get_varint64(p, limit, &out->start_ts);
  if (p == nullptr) return false;
  if (p >= limit) return false;
  out->tombstone = *p++ != 0;
  std::string_view value;
  std::string_view primary;
  p = lsm::get_length_prefixed(p, limit, &value);
  if (p == nullptr) return false;
  p = lsm::get_length_prefixed(p, limit, &primary);
  if (p == nullptr) return false;
  out->value.assign(value);
  out->primary.assign(primary);
  return true;
}

}  // namespace anvil::txn
