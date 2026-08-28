// The write-ahead log.
//
// Record layout:
//
//     [crc32c u32][length u32][payload]
//
// where the CRC covers the length field and the payload together. Covering the
// length is not decoration: a torn write that mangles the length but leaves a
// plausible CRC on the payload would let the reader walk off into garbage and
// then "recover" whatever it landed on.
//
// Reading stops at the first record that does not validate, and does not
// resume. That rule is the whole robustness argument, and it is worth being
// precise about why, because the alternative looks more helpful:
//
//   * A torn write leaves a partial record. Everything after it was written
//     later, so a device that reordered its writeback may have persisted some of
//     it and not others. Scanning past the damage and salvaging later records
//     would "recover" data that was never durably committed -- silent
//     corruption dressed up as resilience.
//
//   * A transient read error is NOT a corrupt record and must never be treated
//     as one. Truncating the log on EIO converts a recoverable device hiccup
//     into permanent data loss. This is ANV-0003, found by fault injection in
//     the P1 counter workload, and it is the reason `read_all` distinguishes
//     Status failures from validation failures rather than folding both into
//     "stop here".
//
// What is deliberately not implemented: LevelDB's block-oriented fragmentation
// (kFirst/kMiddle/kLast across 32 KiB blocks). Its purpose is to bound how much
// of a log a single torn sector can cost you. With whole-record CRCs the damage
// is already bounded at "this record and everything after it", which is the
// same guarantee recovery relies on; fragmentation would improve how much
// survives, not whether corruption is detected. Noted rather than pretended.

#ifndef ANVIL_CORE_LSM_WAL_H_
#define ANVIL_CORE_LSM_WAL_H_

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "anvil/core/lsm/format.h"
#include "anvil/core/runtime/runtime.h"

namespace anvil::lsm {

// A batch of mutations that become durable together. Atomicity at this layer is
// just "one record, one CRC": either the whole batch validates on recovery or
// none of it does.
class WriteBatch {
 public:
  void put(std::string_view key, std::string_view value);
  void del(std::string_view key);

  std::size_t count() const noexcept { return count_; }
  bool empty() const noexcept { return count_ == 0; }
  void clear();

  // [sequence u64][count u32][ops...], op := [type u8][len-prefixed key][len-prefixed value]
  std::string encode(SequenceNumber first_sequence) const;

  struct Entry {
    ValueType type = ValueType::kValue;
    std::string_view key;
    std::string_view value;
    SequenceNumber sequence = 0;
  };

  // Decodes a batch payload. Returns false on any malformed field rather than
  // reading past the end -- a corrupt payload that passed its CRC is
  // vanishingly unlikely but not impossible, and "unlikely" is not a memory
  // safety argument.
  static bool decode(std::string_view payload, std::vector<Entry>* out,
                     SequenceNumber* first_sequence);

 private:
  std::string rep_;  // ops only; the header is prepended by encode()
  std::size_t count_ = 0;
};

// ---------------------------------------------------------------------------

class WalWriter {
 public:
  WalWriter(Runtime* runtime, FileHandle file, std::uint64_t offset = 0)
      : runtime_(runtime), file_(file), offset_(offset) {}

  // Appends one record. Does NOT make it durable; call sync() for that. The
  // split is the point -- an engine that cannot express "written but not yet
  // durable" cannot have a missing-fsync bug, and therefore cannot be tested
  // for one.
  Task<Status> append(std::string_view payload);
  Task<Status> sync();

  std::uint64_t offset() const noexcept { return offset_; }

 private:
  Runtime* runtime_;
  FileHandle file_;
  std::uint64_t offset_;
};

struct WalReadResult {
  std::vector<std::string> records;
  std::uint64_t valid_bytes = 0;  // where a clean log ends
  bool truncated = false;         // a record failed validation; the tail was dropped
  std::uint64_t records_discarded = 0;
};

// Reads until the first invalid record. `out->truncated` distinguishes "the log
// ended cleanly" from "the log was cut short", which recovery reports and the
// invariants care about. A non-ok Status means the *device* failed, which is a
// different situation entirely and must be retried, not accepted as truncation.
Task<Status> wal_read_all(Runtime* runtime, FileHandle file, WalReadResult* out);

}  // namespace anvil::lsm

#endif  // ANVIL_CORE_LSM_WAL_H_
