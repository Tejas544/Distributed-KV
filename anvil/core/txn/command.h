// The transactional commands that go into a range's Raft log.
//
// One opaque command type rather than six fields bolted onto the range's own
// command struct, for a layering reason: the range machine knows about spans,
// leases and triggers, and it should not learn what a prewrite is. It decodes
// exactly one thing from a transactional command -- `key`, so it can check that
// the key is inside its span and that the client's generation is current -- and
// hands the rest to the version store.
//
// Every command carries the key it touches, including the ones that operate on
// a transaction *record*: a record lives in the range that owns the
// transaction's primary key, so the primary key is how it is routed and how it
// is found. Making that explicit in the command is what lets a record follow
// its primary key across a split without any special case.

#ifndef ANVIL_CORE_TXN_COMMAND_H_
#define ANVIL_CORE_TXN_COMMAND_H_

#include <cstdint>
#include <string>

#include "anvil/core/txn/record.h"
#include "anvil/core/txn/store.h"
#include "anvil/core/txn/timestamp.h"

namespace anvil::txn {

enum class TxnOp : std::uint8_t {
  kPrewrite = 0,     // leave an intent
  kCommitIntent,     // intent -> version at commit_ts
  kRollbackIntent,   // drop the intent
  kPutRecord,        // create or advance the transaction record
  kPushRecord,       // a blocked reader moves the record forward, or aborts it
};

const char* to_string(TxnOp op) noexcept;

struct TxnCommand {
  TxnOp op = TxnOp::kPrewrite;

  // The key this command is about, and the one the range checks against its
  // span. For record commands it is the transaction's primary key.
  std::string key;

  TxnId txn = 0;
  std::uint32_t epoch = 1;
  Ts start_ts = 0;
  Ts commit_ts = 0;
  Ts push_to = 0;

  bool tombstone = false;
  bool abort_expired = true;

  std::string value;    // prewrite
  std::string primary;  // prewrite: where this transaction's record lives

  TxnRecord record;  // put-record

  // The proposer's clock reading, carried in the command so that every replica
  // makes the same TTL decision from the same number. A replica that consulted
  // its own clock here would abort a transaction on one node and not on
  // another, and the two would then disagree about whether it happened.
  std::uint64_t now = 0;

  std::string describe() const;
};

std::string encode_txn_command(const TxnCommand& cmd);
bool decode_txn_command(std::string_view in, TxnCommand* out);

// What the apply decided, sent back to the coordinator.
struct TxnResult {
  WriteOutcome outcome = WriteOutcome::kOk;
  TxnStatus status = TxnStatus::kPending;
  Ts commit_ts = 0;

  // Set when the outcome is kLocked: who is in the way, so the coordinator can
  // go and push them rather than spinning.
  bool have_blocker = false;
  TxnId blocker = 0;
  std::uint32_t blocker_epoch = 0;
  Ts blocker_start = 0;
  std::string blocker_primary;
};

std::string encode_txn_result(const TxnResult& result);
bool decode_txn_result(std::string_view in, TxnResult* out);

// Applies one command to a store. Pure: the same store and the same command
// give the same result on every replica, which is the only reason a replicated
// state machine works at all.
TxnResult apply_txn_command(VersionStore* store, const TxnCommand& cmd);

}  // namespace anvil::txn

#endif  // ANVIL_CORE_TXN_COMMAND_H_
