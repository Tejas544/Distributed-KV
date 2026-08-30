// Core value types shared across every Anvil layer.
//
// Two rules govern everything here:
//   1. No floating point. Ever. FP contraction and rounding differ across
//      compilers and architectures, and INV-SIM-01 requires bit-identical
//      execution digests on gcc/x86-64 and clang/arm64. Probabilities are
//      rationals; sizes and times are integers.
//   2. Fixed-width integers only. `long` is 64-bit on Linux and 32-bit on
//      Windows, which is exactly the kind of difference that turns into a
//      cross-platform digest mismatch three months from now.

#ifndef ANVIL_CORE_TYPES_H_
#define ANVIL_CORE_TYPES_H_

#include <compare>
#include <cstddef>
#include <cstdint>
#include <span>

namespace anvil {

// ---------------------------------------------------------------------------
// Strong identifiers
// ---------------------------------------------------------------------------

// Distinct types for distinct concepts. A NodeId that implicitly converts to a
// RangeId is a bug waiting for the one interleaving where the values differ.
template <typename Tag, typename Rep = std::uint64_t>
class Id {
 public:
  using rep_type = Rep;

  constexpr Id() noexcept = default;
  constexpr explicit Id(Rep v) noexcept : v_(v) {}

  constexpr Rep value() const noexcept { return v_; }
  constexpr bool valid() const noexcept { return v_ != Rep{}; }

  friend constexpr auto operator<=>(Id, Id) noexcept = default;
  friend constexpr bool operator==(Id, Id) noexcept = default;

 private:
  Rep v_{};
};

struct NodeTag {};
struct RangeTag {};
struct TxnTag {};
struct ConnTag {};
struct FileTag {};
struct TimerTag {};
struct StoreTag {};

using NodeId = Id<NodeTag>;
using RangeId = Id<RangeTag>;

// A replication group. One Raft instance per group, many groups per node: that
// is what makes this MultiRaft rather than a single replicated log. The sharding
// layer uses the range's own id as its group id, and reserves group 1 for the
// placement driver's group, so a group id and a range id are numerically the
// same thing viewed from the two layers. GroupId{0} is not a group: the wire
// uses it as the marker for a coalesced batch (see raft/transport.h).
using GroupId = Id<struct GroupTag>;
using TxnId = Id<TxnTag>;
using ConnHandle = Id<ConnTag>;
using FileHandle = Id<FileTag>;
using TimerId = Id<TimerTag>;
using StoreId = Id<StoreTag>;

// Raft coordinates. Separate types because "term" and "index" are both
// monotonically increasing u64s and mixing them up compiles fine.
using Term = Id<struct TermTag>;
using LogIndex = Id<struct LogIndexTag>;

// LSM sequence number: the total order of writes within one store.
using SeqNum = Id<struct SeqTag>;

// ---------------------------------------------------------------------------
// Time
// ---------------------------------------------------------------------------

// A duration in simulated nanoseconds. Signed so that differences are
// expressible without underflow surprises.
class Duration {
 public:
  constexpr Duration() noexcept = default;
  constexpr explicit Duration(std::int64_t nanos) noexcept : nanos_(nanos) {}

  static constexpr Duration nanos(std::int64_t n) noexcept { return Duration{n}; }
  static constexpr Duration micros(std::int64_t n) noexcept { return Duration{n * 1'000}; }
  static constexpr Duration millis(std::int64_t n) noexcept { return Duration{n * 1'000'000}; }
  static constexpr Duration seconds(std::int64_t n) noexcept { return Duration{n * 1'000'000'000}; }

  constexpr std::int64_t nanos() const noexcept { return nanos_; }

  constexpr Duration operator+(Duration o) const noexcept { return Duration{nanos_ + o.nanos_}; }
  constexpr Duration operator-(Duration o) const noexcept { return Duration{nanos_ - o.nanos_}; }
  constexpr Duration operator*(std::int64_t k) const noexcept { return Duration{nanos_ * k}; }
  constexpr Duration operator/(std::int64_t k) const noexcept { return Duration{nanos_ / k}; }

  friend constexpr auto operator<=>(Duration, Duration) noexcept = default;
  friend constexpr bool operator==(Duration, Duration) noexcept = default;

 private:
  std::int64_t nanos_{};
};

// A hybrid logical clock reading: physical nanoseconds plus a logical counter
// that breaks ties without needing the physical component to advance.
//
// This is also the MVCC version timestamp and the transaction commit timestamp,
// deliberately: INV-TXN-03 and INV-MVCC-03 both compare against it, and having
// one totally-ordered type removes a whole class of conversion bug.
struct Timestamp {
  std::uint64_t physical = 0;  // simulated nanoseconds since cluster epoch
  std::uint32_t logical = 0;

  static constexpr Timestamp zero() noexcept { return {}; }
  static constexpr Timestamp max() noexcept {
    return {UINT64_MAX, UINT32_MAX};
  }

  constexpr Timestamp advanced_by(Duration d) const noexcept {
    return {physical + static_cast<std::uint64_t>(d.nanos()), logical};
  }
  constexpr Timestamp next_logical() const noexcept {
    return {physical, logical + 1};
  }

  friend constexpr auto operator<=>(const Timestamp&, const Timestamp&) noexcept = default;
  friend constexpr bool operator==(const Timestamp&, const Timestamp&) noexcept = default;
};

// The clock's honest answer: a bracket, not a point.
//
// Every strict-serializability argument in this system rests on this interval
// being a true bound. The simulator can deliberately violate it (see
// docs/SCOPE.md section 5) to characterise what breaks when the assumption
// fails -- which is a different and more interesting question than whether the
// system is correct while it holds.
struct TimeInterval {
  Timestamp earliest;
  Timestamp latest;

  constexpr Duration width() const noexcept {
    return Duration{static_cast<std::int64_t>(latest.physical - earliest.physical)};
  }
  constexpr bool contains(Timestamp t) const noexcept {
    return earliest <= t && t <= latest;
  }
};

// ---------------------------------------------------------------------------
// Bytes
// ---------------------------------------------------------------------------

// Non-owning views at the interface boundary. Ownership lives in the arena that
// belongs to the node, so that a node restart drops every allocation at once
// and pointer values are a deterministic function of the seed (which is what
// makes "accidentally sorted by address" a caught bug rather than a hidden one).
using ByteView = std::span<const std::byte>;
using MutableByteView = std::span<std::byte>;

using Key = ByteView;
using Value = ByteView;

// Lexicographic byte order, the ordering the whole key space is built on.
constexpr int compare_bytes(ByteView a, ByteView b) noexcept {
  const std::size_t n = a.size() < b.size() ? a.size() : b.size();
  for (std::size_t i = 0; i < n; ++i) {
    if (a[i] != b[i]) return a[i] < b[i] ? -1 : 1;
  }
  if (a.size() == b.size()) return 0;
  return a.size() < b.size() ? -1 : 1;
}

// A half-open key span [start, end). The entire sharding layer is built on
// these, and INV-SHARD-01 asserts that the live set of them tiles the key space
// exactly once.
struct KeyRange {
  Key start;
  Key end;  // empty == unbounded above

  constexpr bool contains(Key k) const noexcept {
    if (compare_bytes(k, start) < 0) return false;
    return end.empty() || compare_bytes(k, end) < 0;
  }
};

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

enum class StatusCode : std::uint8_t {
  kOk = 0,
  kNotFound,
  kInvalidArgument,
  kCorruption,        // checksum mismatch -- never served as data (INV-LSM-11)
  kIoError,
  kNoSpace,
  kAborted,           // transaction lost a conflict; retry is legal
  kTimedOut,
  kNotLeader,         // retry against the hinted leader
  kRangeKeyMismatch,  // stale client range cache (INV-SHARD-05)
  kUnavailable,
  kUnknown,           // commit outcome genuinely indeterminate (INV-TXN-15)
};

// Deliberately allocation-free: messages are string literals. The core has no
// business formatting text, and a Status that allocates cannot be returned from
// the out-of-memory path.
class Status {
 public:
  constexpr Status() noexcept = default;
  constexpr Status(StatusCode code, const char* msg) noexcept : code_(code), msg_(msg) {}

  static constexpr Status ok() noexcept { return {}; }

  constexpr bool is_ok() const noexcept { return code_ == StatusCode::kOk; }
  constexpr explicit operator bool() const noexcept { return is_ok(); }
  constexpr StatusCode code() const noexcept { return code_; }
  constexpr const char* message() const noexcept { return msg_ ? msg_ : ""; }

  // kUnknown is not a failure to be papered over. A client that treats it as
  // "aborted" will double-apply; one that treats it as "committed" will lose
  // writes. It has to be resolved, and INV-TXN-15 checks that it always is.
  constexpr bool is_indeterminate() const noexcept {
    return code_ == StatusCode::kUnknown;
  }

 private:
  StatusCode code_ = StatusCode::kOk;
  const char* msg_ = nullptr;
};

}  // namespace anvil

#endif  // ANVIL_CORE_TYPES_H_
