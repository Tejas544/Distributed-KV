// The transaction record, the intent, and the version.
//
// Three pieces of state, and which range each of them lives in is the whole
// protocol:
//
//   version   ('key', commit_ts) -> bytes.  Lives in the range that owns the
//             key. Immutable once written.
//
//   intent    'key' -> {txn, epoch, start_ts, value, primary}. Lives in the
//             range that owns the key. At most one per key, which is why the
//             intent space and the lock table are the same thing seen from two
//             directions.
//
//   record    txn -> {status, epoch, commit_ts, keys}. Lives in the range that
//             owns the transaction's *primary key*, and nowhere else. This is
//             the single object whose state defines whether the transaction
//             committed. Everything else is derivable from it, which is
//             INV-TXN-02, and it is the reason a distributed commit needs only
//             one range to agree with itself.
//
// The status lattice is the part worth getting right, because every recovery
// path in the layer is an argument about it:
//
//   kPending  -> kStaging -> kCommitted        (parallel commit)
//   kPending  -> kCommitted                    (ordinary commit)
//   kPending  -> kAborted                      (conflict, push, or TTL)
//   kStaging  -> kAborted                      (recovery found a missing intent)
//
// kCommitted and kAborted are terminal. A transition out of a terminal state is
// not a race to be resolved, it is a bug: two readers would resolve the same
// intent two different ways and the database would disagree with itself about
// what happened.

#ifndef ANVIL_CORE_TXN_RECORD_H_
#define ANVIL_CORE_TXN_RECORD_H_

#include <cstdint>
#include <set>
#include <string>
#include <vector>

#include "anvil/core/txn/timestamp.h"
#include "anvil/core/types.h"

namespace anvil::txn {

using TxnId = std::uint64_t;

enum class TxnStatus : std::uint8_t {
  kPending = 0,
  kStaging,     // parallel commit: committed iff every listed intent is present
  kCommitted,
  kAborted,
};

const char* to_string(TxnStatus status) noexcept;

inline bool terminal(TxnStatus status) noexcept {
  return status == TxnStatus::kCommitted || status == TxnStatus::kAborted;
}

struct TxnRecord {
  TxnId id = 0;
  std::uint32_t epoch = 1;  // bumped on restart; an intent from an old epoch is dead
  TxnStatus status = TxnStatus::kPending;
  Ts start_ts = 0;
  Ts commit_ts = 0;

  // Every key the transaction intends to write, recorded *before* the intents
  // are written when parallel commit is on. Without the list, a recovering
  // reader that finds a STAGING record has no way to check whether the
  // transaction finished: it would have to search the whole key space.
  std::vector<std::string> keys;

  // When the coordinator last said it was alive, on the record range's clock.
  // A record whose heartbeat has lapsed may be aborted by anyone who is blocked
  // on it -- which is the only thing standing between a crashed coordinator and
  // a key that is locked forever.
  std::uint64_t heartbeat = 0;
  std::uint64_t ttl_nanos = 0;

  // The highest timestamp any reader has pushed this transaction to. A pushed
  // transaction must either commit above it or refresh its reads; it may not
  // simply ignore the push, because the reader has already been told there was
  // nothing there.
  Ts pushed_to = 0;

  bool expired(std::uint64_t now) const noexcept {
    return ttl_nanos != 0 && heartbeat != 0 && now > heartbeat + ttl_nanos;
  }
};

std::string encode_record(const TxnRecord& record);
bool decode_record(std::string_view in, TxnRecord* out);

// ---------------------------------------------------------------------------
// intents
// ---------------------------------------------------------------------------

struct Intent {
  TxnId txn = 0;
  std::uint32_t epoch = 1;
  Ts start_ts = 0;
  bool tombstone = false;
  std::string value;

  // The transaction's primary key, so that a reader who finds this intent knows
  // which range to ask about its fate. An intent that does not name its primary
  // is an intent nobody can resolve.
  std::string primary;

  bool is_primary() const noexcept { return false; }  // decided by the caller
};

std::string encode_intent(const Intent& intent);
bool decode_intent(std::string_view in, Intent* out);

// ---------------------------------------------------------------------------
// what a read saw
// ---------------------------------------------------------------------------

enum class ReadStatus : std::uint8_t {
  kOk = 0,
  kBlocked,      // an intent from another transaction is in the way
  kUncertain,    // a version inside the uncertainty window; the read must restart
  kWrongRange,   // the range does not own this key any more
  kUnavailable,
};

const char* to_string(ReadStatus status) noexcept;

struct ReadResult {
  ReadStatus status = ReadStatus::kOk;
  bool found = false;
  std::string value;
  Ts commit_ts = 0;

  // Set when status is kBlocked: who is in the way, and where their record is.
  TxnId blocker = 0;
  std::uint32_t blocker_epoch = 0;
  Ts blocker_start = 0;
  std::string blocker_primary;

  // Set when status is kUncertain: the version that forced the restart.
  Ts uncertain_at = 0;
};

}  // namespace anvil::txn

#endif  // ANVIL_CORE_TXN_RECORD_H_
