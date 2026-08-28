#include "anvil/core/lsm/wal.h"

#include <cstring>

namespace anvil::lsm {
namespace {

constexpr std::size_t kHeaderSize = 8;  // crc32c u32 + length u32

// Cap on a single record. Not a format limit so much as a sanity bound: a
// corrupt length field that passed its CRC would otherwise ask for an
// arbitrary allocation.
constexpr std::uint32_t kMaxRecordSize = 64u * 1024 * 1024;

ByteView as_bytes(std::string_view s) {
  return ByteView{reinterpret_cast<const std::byte*>(s.data()), s.size()};
}

}  // namespace

// ---------------------------------------------------------------------------
// WriteBatch
// ---------------------------------------------------------------------------

void WriteBatch::put(std::string_view key, std::string_view value) {
  rep_.push_back(static_cast<char>(ValueType::kValue));
  put_length_prefixed(&rep_, key);
  put_length_prefixed(&rep_, value);
  ++count_;
}

void WriteBatch::del(std::string_view key) {
  rep_.push_back(static_cast<char>(ValueType::kDeletion));
  put_length_prefixed(&rep_, key);
  // A tombstone still carries an (empty) value field so the decoder can walk
  // records uniformly without branching on type.
  put_length_prefixed(&rep_, std::string_view{});
  ++count_;
}

void WriteBatch::clear() {
  rep_.clear();
  count_ = 0;
}

std::string WriteBatch::encode(SequenceNumber first_sequence) const {
  std::string out;
  out.reserve(rep_.size() + 12);
  put_fixed64(&out, first_sequence);
  put_fixed32(&out, static_cast<std::uint32_t>(count_));
  out.append(rep_);
  return out;
}

bool WriteBatch::decode(std::string_view payload, std::vector<Entry>* out,
                        SequenceNumber* first_sequence) {
  if (payload.size() < 12) return false;
  const SequenceNumber first = decode_fixed64(payload.data());
  const std::uint32_t count = decode_fixed32(payload.data() + 8);
  *first_sequence = first;

  const char* p = payload.data() + 12;
  const char* const limit = payload.data() + payload.size();

  for (std::uint32_t i = 0; i < count; ++i) {
    if (p >= limit) return false;
    const auto type = static_cast<ValueType>(static_cast<unsigned char>(*p++));
    if (type != ValueType::kValue && type != ValueType::kDeletion) return false;

    Entry entry;
    entry.type = type;
    p = get_length_prefixed(p, limit, &entry.key);
    if (p == nullptr) return false;
    p = get_length_prefixed(p, limit, &entry.value);
    if (p == nullptr) return false;
    // Sequence numbers within a batch run consecutively from the batch's first,
    // so a batch occupies a contiguous range and ordering between batches is
    // unambiguous.
    entry.sequence = first + i;
    out->push_back(entry);
  }
  return true;
}

// ---------------------------------------------------------------------------
// WalWriter
// ---------------------------------------------------------------------------

Task<Status> WalWriter::append(std::string_view payload) {
  if (payload.size() > kMaxRecordSize) {
    co_return Status{StatusCode::kInvalidArgument, "wal record too large"};
  }

  std::string header;
  put_fixed32(&header, 0);  // placeholder for the CRC
  put_fixed32(&header, static_cast<std::uint32_t>(payload.size()));

  // The CRC covers the length field as well as the payload. Checksumming only
  // the payload would let a torn write corrupt the length into something
  // plausible and send the reader walking into whatever follows.
  std::uint32_t crc = crc32c(std::string_view{header.data() + 4, 4});
  crc = crc32c_extend(crc, payload);
  std::string crc_bytes;
  put_fixed32(&crc_bytes, crc);
  std::memcpy(header.data(), crc_bytes.data(), 4);

  std::string record = header;
  record.append(payload);

  const Status status = co_await runtime_->pwrite(file_, as_bytes(record), offset_);
  if (!status.is_ok()) co_return status;
  offset_ += record.size();
  co_return Status::ok();
}

Task<Status> WalWriter::sync() { co_return co_await runtime_->fsync(file_); }

// ---------------------------------------------------------------------------
// reading
// ---------------------------------------------------------------------------

Task<Status> wal_read_all(Runtime* runtime, FileHandle file, WalReadResult* out) {
  std::uint64_t size = 0;
  Status status = co_await runtime->file_size(file, &size);
  if (!status.is_ok()) co_return status;

  std::uint64_t offset = 0;
  while (offset + kHeaderSize <= size) {
    std::string header(kHeaderSize, '\0');
    std::size_t read = 0;
    status = co_await runtime->pread(
        file, MutableByteView{reinterpret_cast<std::byte*>(header.data()), header.size()},
        offset, &read);
    // A device error is not corruption. Propagating it lets the caller retry;
    // treating it as end-of-log would turn a transient EIO into permanent loss
    // (ANV-0003).
    if (!status.is_ok()) co_return status;
    if (read < kHeaderSize) break;

    const std::uint32_t expected_crc = decode_fixed32(header.data());
    const std::uint32_t length = decode_fixed32(header.data() + 4);

    if (length > kMaxRecordSize || offset + kHeaderSize + length > size) {
      out->truncated = true;
      ++out->records_discarded;
      break;
    }

    std::string payload(length, '\0');
    if (length > 0) {
      status = co_await runtime->pread(
          file, MutableByteView{reinterpret_cast<std::byte*>(payload.data()), payload.size()},
          offset + kHeaderSize, &read);
      if (!status.is_ok()) co_return status;
      if (read < length) {
        out->truncated = true;
        ++out->records_discarded;
        break;
      }
    }

    std::uint32_t crc = crc32c(std::string_view{header.data() + 4, 4});
    crc = crc32c_extend(crc, payload);
    if (crc != expected_crc) {
      out->truncated = true;
      ++out->records_discarded;
      break;
    }

    out->records.push_back(std::move(payload));
    offset += kHeaderSize + length;
    out->valid_bytes = offset;
  }

  // Trailing bytes that are not a complete header are a partial write, not a
  // clean end. Reporting them as truncation matters: the caller uses
  // valid_bytes to position the next append, and appending after garbage would
  // make the log unreadable from that point forever.
  if (!out->truncated && offset < size) out->truncated = true;
  co_return Status::ok();
}

}  // namespace anvil::lsm
