// The transactional state that lives inside a range.
//
// Versions, intents and transaction records, with no Raft and no Runtime
// anywhere near them: this is a plain data structure whose mutations are pure
// functions of their arguments, so that a range's replicas reach identical
// state by applying identical commands and nothing else. Everything that needs
// a clock or a socket is above it.
//
// It is deliberately a separate type from the range machine that holds it. The
// range machine's job is spans, leases and triggers -- what a range *is* -- and
// the transaction protocol's job is what the keys inside it mean. Mixing them
// gives you a class where a split has to know about intent resolution.
//
// The layering runs the other way from what the directory suggests: this file
// is in `txn/` because it is transaction state, and `shard/range.h` includes it
// rather than the reverse. It depends on nothing but record.h and timestamp.h,
// which is what keeps that from being a cycle.

#ifndef ANVIL_CORE_TXN_STORE_H_
#define ANVIL_CORE_TXN_STORE_H_

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "anvil/core/txn/record.h"
#include "anvil/core/txn/timestamp.h"

namespace anvil::txn {

// Why a write was refused. Returned rather than thrown, and distinguished
// rather than collapsed into "failed", because the caller's correct response is
// different for each: a conflict means restart, a lock means push or wait, and
// a stale epoch means this intent belongs to a previous incarnation of a
// transaction that has already given up on it.
enum class WriteOutcome : std::uint8_t {
  kOk = 0,
  kConflict,     // a version committed above start_ts -- first-committer-wins
  kLocked,       // another transaction holds the intent
  kStaleEpoch,   // the command is from an epoch this transaction has left behind
  kNotFound,     // no intent to resolve; usually a duplicate resolution
  kTerminal,     // the record is already committed or aborted; this is not a race
  kRejected,     // a precondition the caller should have checked
};

const char* to_string(WriteOutcome outcome) noexcept;

// One committed version. A tombstone is recorded rather than absent so that a
// reader below it still sees whatever the tombstone shadows; the flag is what
// keeps that value distinguishable from a version whose payload is genuinely
// the empty string -- collapsing the two would make a deleted key resurrect
// itself the next time somebody happens to write "" to it.
struct VersionEntry {
  bool tombstone = false;
  std::string value;
};

struct VersionStoreStats {
  std::uint64_t versions_written = 0;
  std::uint64_t intents_written = 0;
  std::uint64_t intents_committed = 0;
  std::uint64_t intents_rolled_back = 0;
  std::uint64_t reads = 0;
  std::uint64_t reads_blocked = 0;
  std::uint64_t reads_uncertain = 0;
  std::uint64_t write_conflicts = 0;
  std::uint64_t records_created = 0;
  std::uint64_t records_committed = 0;
  std::uint64_t records_aborted = 0;
  std::uint64_t pushes = 0;
  std::uint64_t pushes_aborting_expired = 0;
};

// Every deliberate bug this layer can be given, one flag each, all defaulting
// to correct. Same discipline as everywhere else: a test that flips one is
// planting a bug.
struct StoreOptions {
  // false: a prewrite succeeds even when a newer version exists, so two
  // transactions that both read at t and both write the same key both commit --
  // the lost update that first-committer-wins exists to prevent. INV-TXN-04.
  bool first_committer_wins = true;

  // false: a read at ts returns the newest committed version and ignores any
  // intent in its way. That is read-committed wearing snapshot isolation's
  // name, and it makes a reader see a transaction that has not committed.
  bool reads_respect_intents = true;

  // false: a record may leave a terminal state. The single most destructive
  // flag here: two readers resolve the same intent two different ways and the
  // database disagrees with itself about whether a transaction happened.
  // INV-TXN-01, INV-TXN-02.
  bool terminal_status_is_final = true;

  // false: a read whose uncertainty window straddles a committed version
  // returns the older value instead of restarting. The value it skips may have
  // been written before the read in real time. INV-TXN-07.
  bool honour_uncertainty = true;
};

class VersionStore {
 public:
  VersionStore() = default;
  explicit VersionStore(StoreOptions options) : options_(options) {}

  void set_options(StoreOptions options) { options_ = options; }
  const StoreOptions& options() const noexcept { return options_; }

  // ---- reads -------------------------------------------------------------

  // The newest version at or below `read_ts`, subject to intents.
  //
  // `uncertainty_limit` is the top of the reader's uncertainty window and is
  // equal to read_ts when the reader has no uncertainty (the oracle) or has
  // already restarted past it. A version committed inside (read_ts, limit] is
  // not returned and not skipped: the read reports kUncertain and the caller
  // restarts above it, because that version may have been written before this
  // read began in real time and there is no way to tell from here.
  ReadResult get(std::string_view key, Ts read_ts, TxnId reader, Ts uncertainty_limit) const;

  // The newest committed version at or below `read_ts`, ignoring any intent in
  // the way. This is NOT a read a client may perform -- ignoring an intent is
  // read-committed at best -- and it exists for the audit, which runs after
  // everything has settled and needs the committed state of a key whose last
  // intent nobody has got around to cleaning up. Using `get` there reports the
  // key as blocked and every element in it as lost, which is the checker
  // inventing a finding out of a mechanism working as designed.
  bool committed_value(std::string_view key, Ts read_ts, std::string* out, Ts* at) const;

  // ---- writes ------------------------------------------------------------

  // Leaves an intent. Fails with kConflict when a version committed above
  // `start_ts`, and with kLocked when someone else's intent is already there.
  WriteOutcome prewrite(std::string_view key, const Intent& intent, Intent* blocker);

  // Turns this transaction's intent into a version, or removes it.
  WriteOutcome commit_intent(std::string_view key, TxnId txn, std::uint32_t epoch,
                             Ts commit_ts);
  WriteOutcome rollback_intent(std::string_view key, TxnId txn, std::uint32_t epoch);

  // ---- records -----------------------------------------------------------

  const TxnRecord* find_record(TxnId txn) const;

  // Creates or advances a record. The status lattice is enforced here and
  // nowhere else: pending -> staging -> committed, pending -> committed,
  // pending|staging -> aborted, and nothing out of a terminal state.
  WriteOutcome put_record(const TxnRecord& record);

  // A blocked reader's move. Pushes the record's timestamp forward, or aborts
  // it outright when its heartbeat has lapsed. Returns the record's status
  // after the push so the caller can decide what to do about the intent.
  //
  // `primary` is the key the blocked reader found this transaction's intent
  // naming as its primary -- the same key a record is always keyed by (see
  // encode_span). It is only used to seed a *new* record's location when none
  // exists yet; an existing record keeps whatever location it already has.
  WriteOutcome push_record(TxnId txn, Ts push_to, std::uint64_t now, bool abort_expired,
                           std::string_view primary, TxnRecord* after);

  // ---- observation, splitting and merging ---------------------------------

  std::size_t key_count() const noexcept { return versions_.size(); }
  std::size_t intent_count() const noexcept { return intents_.size(); }
  std::size_t record_count() const noexcept { return records_.size(); }
  const std::map<std::string, std::map<Ts, VersionEntry, std::greater<Ts>>>& versions() const
      noexcept {
    return versions_;
  }
  const std::map<std::string, Intent>& intents() const noexcept { return intents_; }
  const std::map<TxnId, TxnRecord>& records() const noexcept { return records_; }
  const VersionStoreStats& stats() const noexcept { return stats_; }

  // The key that would divide this store's keys in half, or empty when there is
  // nothing to divide.
  std::string median_key() const;

  // Everything in [lo, hi), encoded. Records travel with the primary key they
  // belong to, which is what keeps a split from separating a record from the
  // range that has to answer questions about it.
  std::string encode_span(std::string_view lo, std::string_view hi) const;
  void erase_span(std::string_view lo, std::string_view hi);
  bool absorb(std::string_view payload);
  std::string encode_all() const;
  bool load(std::string_view payload);
  void clear();

  // Versions strictly older than the newest one at or below `safepoint` are
  // unreachable and may go. The boundary version is kept, for the reason
  // INV-MVCC-01 exists: a reader at exactly the safepoint still resolves to it.
  std::uint64_t collect(Ts safepoint);

 private:
  using VersionChain = std::map<Ts, VersionEntry, std::greater<Ts>>;

  StoreOptions options_;
  // Newest first, so a read at a timestamp is a lower_bound rather than a scan.
  std::map<std::string, VersionChain> versions_;
  std::map<std::string, Intent> intents_;
  std::map<TxnId, TxnRecord> records_;
  VersionStoreStats stats_;
};

}  // namespace anvil::txn

#endif  // ANVIL_CORE_TXN_STORE_H_
