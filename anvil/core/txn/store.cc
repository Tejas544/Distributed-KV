#include "anvil/core/txn/store.h"

#include <algorithm>

#include "anvil/core/lsm/format.h"

namespace anvil::txn {
namespace {

// A version chain is keyed newest-first, so "the newest at or below ts" is the
// first entry whose key is <= ts -- one lower_bound, no scan. Same trick as the
// LSM's inverted timestamp encoding (mvcc/key.h), for the same reason.
const VersionEntry* newest_at_or_below(
    const std::map<Ts, VersionEntry, std::greater<Ts>>& chain, Ts ts, Ts* found_at) {
  const auto it = chain.lower_bound(ts);
  if (it == chain.end()) return nullptr;
  *found_at = it->first;
  return &it->second;
}

}  // namespace

const char* to_string(WriteOutcome outcome) noexcept {
  switch (outcome) {
    case WriteOutcome::kOk: return "ok";
    case WriteOutcome::kConflict: return "write-conflict";
    case WriteOutcome::kLocked: return "locked";
    case WriteOutcome::kStaleEpoch: return "stale-epoch";
    case WriteOutcome::kNotFound: return "not-found";
    case WriteOutcome::kTerminal: return "already-terminal";
    case WriteOutcome::kRejected: return "rejected";
  }
  return "?";
}

// ---------------------------------------------------------------------------
// reads
// ---------------------------------------------------------------------------

ReadResult VersionStore::get(std::string_view key, Ts read_ts, TxnId reader,
                             Ts uncertainty_limit) const {
  ReadResult out;
  const_cast<VersionStoreStats&>(stats_).reads++;

  const std::string key_str{key};

  // The intent first. A reader that looks at versions and then at intents can
  // return a value that a concurrent commit has already superseded, because the
  // two lookups are at different instants -- and here they are not, because
  // nothing suspends in between, but the ordering is still the one that
  // expresses the rule: an intent from someone else means this read cannot be
  // answered without knowing that transaction's fate.
  const auto intent = intents_.find(key_str);
  if (intent != intents_.end() && intent->second.txn != reader &&
      options_.reads_respect_intents) {
    // Only an intent from a transaction that started at or below our read
    // timestamp can matter. One that started above us will commit above us too,
    // so it is invisible at this snapshot whatever it does.
    if (intent->second.start_ts <= read_ts) {
      const_cast<VersionStoreStats&>(stats_).reads_blocked++;
      out.status = ReadStatus::kBlocked;
      out.blocker = intent->second.txn;
      out.blocker_epoch = intent->second.epoch;
      out.blocker_start = intent->second.start_ts;
      out.blocker_primary = intent->second.primary;
      return out;
    }
  }

  const auto chain = versions_.find(key_str);
  if (chain == versions_.end()) {
    // A key this transaction has written but not committed still reads back its
    // own intent, which is what makes read-your-writes work inside a
    // transaction without a client-side buffer.
    if (intent != intents_.end() && intent->second.txn == reader) {
      out.found = !intent->second.tombstone;
      out.value = intent->second.value;
      out.commit_ts = 0;
    }
    return out;
  }

  // The uncertainty window. A version committed in (read_ts, limit] may have
  // been written before this read began in real time -- the timestamps say
  // otherwise but the timestamps are only accurate to the declared bound. The
  // read cannot return it (it is above the snapshot) and cannot skip it (it may
  // precede the read), so it restarts above it.
  if (options_.honour_uncertainty && uncertainty_limit > read_ts) {
    Ts found_at = 0;
    const auto upper = chain->second.lower_bound(uncertainty_limit);
    if (upper != chain->second.end() && upper->first > read_ts) {
      found_at = upper->first;
      const_cast<VersionStoreStats&>(stats_).reads_uncertain++;
      out.status = ReadStatus::kUncertain;
      out.uncertain_at = found_at;
      return out;
    }
  }

  if (intent != intents_.end() && intent->second.txn == reader) {
    out.found = !intent->second.tombstone;
    out.value = intent->second.value;
    out.commit_ts = 0;
    return out;
  }

  Ts at = 0;
  const VersionEntry* entry = newest_at_or_below(chain->second, read_ts, &at);
  if (entry == nullptr) return out;
  // A tombstone is a version, not an absence: it shadows everything older but
  // is not itself a value, so `found` says so even though a version genuinely
  // exists at this timestamp.
  out.found = !entry->tombstone;
  out.value = entry->value;
  out.commit_ts = at;
  return out;
}

bool VersionStore::committed_value(std::string_view key, Ts read_ts, std::string* out,
                                   Ts* at) const {
  const auto chain = versions_.find(std::string{key});
  if (chain == versions_.end()) return false;
  Ts found_at = 0;
  const VersionEntry* entry = newest_at_or_below(chain->second, read_ts, &found_at);
  if (entry == nullptr || entry->tombstone) return false;
  *out = entry->value;
  *at = found_at;
  return true;
}

// ---------------------------------------------------------------------------
// writes
// ---------------------------------------------------------------------------

WriteOutcome VersionStore::prewrite(std::string_view key, const Intent& intent,
                                    Intent* blocker) {
  const std::string key_str{key};

  const auto existing = intents_.find(key_str);
  if (existing != intents_.end()) {
    if (existing->second.txn == intent.txn) {
      if (existing->second.epoch > intent.epoch) return WriteOutcome::kStaleEpoch;
      // Same transaction, same or newer epoch: idempotent. A prewrite is
      // retried whenever its reply is lost, and a retry that failed here would
      // abort transactions for the crime of a dropped packet.
      intents_[key_str] = intent;
      return WriteOutcome::kOk;
    }
    if (blocker != nullptr) *blocker = existing->second;
    return WriteOutcome::kLocked;
  }

  if (options_.first_committer_wins) {
    const auto chain = versions_.find(key_str);
    if (chain != versions_.end() && !chain->second.empty()) {
      // The newest version overall, not the newest at our snapshot. If anything
      // committed above start_ts then somebody else wrote this key while we
      // were deciding what to write, and one of us has to lose.
      const Ts newest = chain->second.begin()->first;
      if (newest > intent.start_ts) {
        ++stats_.write_conflicts;
        return WriteOutcome::kConflict;
      }
    }
  }

  intents_[key_str] = intent;
  ++stats_.intents_written;
  return WriteOutcome::kOk;
}

WriteOutcome VersionStore::commit_intent(std::string_view key, TxnId txn,
                                         std::uint32_t epoch, Ts commit_ts) {
  const std::string key_str{key};
  const auto it = intents_.find(key_str);
  if (it == intents_.end()) {
    // Already resolved. Resolution is retried by anyone who finds a stale
    // intent, so this is the common case rather than an error.
    return WriteOutcome::kNotFound;
  }
  if (it->second.txn != txn) return WriteOutcome::kRejected;
  if (it->second.epoch != epoch) return WriteOutcome::kStaleEpoch;

  // A delete is a version whose tombstone flag says the key is gone at and
  // above this timestamp. Storing it rather than erasing the chain is what
  // lets a reader at an older timestamp still see the value that was there.
  VersionEntry& entry = versions_[key_str][commit_ts];
  entry.tombstone = it->second.tombstone;
  entry.value = it->second.tombstone ? std::string{} : it->second.value;
  ++stats_.versions_written;
  ++stats_.intents_committed;
  intents_.erase(it);
  return WriteOutcome::kOk;
}

WriteOutcome VersionStore::rollback_intent(std::string_view key, TxnId txn,
                                           std::uint32_t epoch) {
  const std::string key_str{key};
  const auto it = intents_.find(key_str);
  if (it == intents_.end()) return WriteOutcome::kNotFound;
  if (it->second.txn != txn) return WriteOutcome::kRejected;
  if (it->second.epoch > epoch) return WriteOutcome::kStaleEpoch;
  intents_.erase(it);
  ++stats_.intents_rolled_back;
  return WriteOutcome::kOk;
}

// ---------------------------------------------------------------------------
// records
// ---------------------------------------------------------------------------

const TxnRecord* VersionStore::find_record(TxnId txn) const {
  const auto it = records_.find(txn);
  return it == records_.end() ? nullptr : &it->second;
}

WriteOutcome VersionStore::put_record(const TxnRecord& record) {
  const auto it = records_.find(record.id);
  if (it == records_.end()) {
    records_[record.id] = record;
    ++stats_.records_created;
    if (record.status == TxnStatus::kCommitted) ++stats_.records_committed;
    if (record.status == TxnStatus::kAborted) ++stats_.records_aborted;
    return WriteOutcome::kOk;
  }

  TxnRecord& current = it->second;
  if (options_.terminal_status_is_final && terminal(current.status)) {
    // Not a race. Two readers that resolve the same intent must reach the same
    // verdict, and the only way to guarantee that is for the verdict to be
    // unchangeable once written. A transition out of here is the difference
    // between "a transaction committed" and "a transaction committed for some
    // readers".
    if (current.status == record.status && current.commit_ts == record.commit_ts) {
      return WriteOutcome::kOk;  // idempotent replay
    }
    return WriteOutcome::kTerminal;
  }

  if (record.epoch < current.epoch) return WriteOutcome::kStaleEpoch;

  // The heartbeat and the push move independently of the status, because a
  // record can be kept alive and pushed forward while still pending.
  current.epoch = record.epoch;
  current.start_ts = record.start_ts;
  if (record.heartbeat > current.heartbeat) current.heartbeat = record.heartbeat;
  if (record.ttl_nanos != 0) current.ttl_nanos = record.ttl_nanos;
  if (record.pushed_to > current.pushed_to) current.pushed_to = record.pushed_to;
  if (!record.keys.empty()) current.keys = record.keys;
  if (record.status != TxnStatus::kPending) {
    current.status = record.status;
    current.commit_ts = record.commit_ts;
    if (record.status == TxnStatus::kCommitted) ++stats_.records_committed;
    if (record.status == TxnStatus::kAborted) ++stats_.records_aborted;
  }
  return WriteOutcome::kOk;
}

WriteOutcome VersionStore::push_record(TxnId txn, Ts push_to, std::uint64_t now,
                                       bool abort_expired, std::string_view primary,
                                       TxnRecord* after) {
  ++stats_.pushes;
  auto it = records_.find(txn);
  if (it == records_.end()) {
    // No record: the transaction has not written its primary yet, or never
    // will. Writing an aborted record for it is the right move and it is what
    // stops a coordinator that died before its first heartbeat from locking a
    // key forever -- the abort is a tombstone that any later attempt to commit
    // will collide with. It needs the primary in its own key list the same as
    // any other record does, or it is exactly as invisible to a later split or
    // merge as the bug this fixes elsewhere -- and a lost tombstone is a
    // resurrected transaction, not merely a stranded one.
    TxnRecord record;
    record.id = txn;
    record.status = TxnStatus::kAborted;
    record.pushed_to = push_to;
    if (!primary.empty()) record.keys.push_back(std::string{primary});
    records_[txn] = record;
    ++stats_.records_created;
    ++stats_.records_aborted;
    if (after != nullptr) *after = record;
    return WriteOutcome::kOk;
  }

  TxnRecord& record = it->second;
  if (terminal(record.status)) {
    if (after != nullptr) *after = record;
    return WriteOutcome::kOk;  // nothing to push; the answer is already known
  }

  if (abort_expired && record.expired(now)) {
    record.status = TxnStatus::kAborted;
    ++stats_.records_aborted;
    ++stats_.pushes_aborting_expired;
    if (after != nullptr) *after = record;
    return WriteOutcome::kOk;
  }

  if (push_to > record.pushed_to) record.pushed_to = push_to;
  if (after != nullptr) *after = record;
  return WriteOutcome::kOk;
}

// ---------------------------------------------------------------------------
// splitting, merging, snapshots
// ---------------------------------------------------------------------------

std::string VersionStore::median_key() const {
  if (versions_.size() < 2) return {};
  auto it = versions_.begin();
  std::advance(it, static_cast<std::ptrdiff_t>(versions_.size() / 2));
  if (it == versions_.begin() || it == versions_.end()) return {};
  return it->first;
}

namespace {

bool in_span(std::string_view key, std::string_view lo, std::string_view hi) {
  if (key < lo) return false;
  return hi.empty() || key < hi;
}

}  // namespace

std::string VersionStore::encode_span(std::string_view lo, std::string_view hi) const {
  std::string out;

  std::vector<const std::pair<const std::string, VersionChain>*> chains;
  for (const auto& entry : versions_) {
    if (in_span(entry.first, lo, hi)) chains.push_back(&entry);
  }
  lsm::put_varint32(&out, static_cast<std::uint32_t>(chains.size()));
  for (const auto* entry : chains) {
    lsm::put_length_prefixed(&out, entry->first);
    lsm::put_varint32(&out, static_cast<std::uint32_t>(entry->second.size()));
    for (const auto& [ts, version] : entry->second) {
      lsm::put_varint64(&out, ts);
      out.push_back(version.tombstone ? 1 : 0);
      lsm::put_length_prefixed(&out, version.value);
    }
  }

  std::vector<const std::pair<const std::string, Intent>*> intents;
  for (const auto& entry : intents_) {
    if (in_span(entry.first, lo, hi)) intents.push_back(&entry);
  }
  lsm::put_varint32(&out, static_cast<std::uint32_t>(intents.size()));
  for (const auto* entry : intents) {
    lsm::put_length_prefixed(&out, entry->first);
    lsm::put_length_prefixed(&out, encode_intent(entry->second));
  }

  // A record travels with its primary key. That is what keeps a split from
  // separating a transaction's verdict from the range every reader will ask for
  // it -- the primary key is how the record is found, so the record has to be
  // wherever that key is.
  std::vector<const TxnRecord*> records;
  for (const auto& [id, record] : records_) {
    const std::string& primary = record.keys.empty() ? std::string{} : record.keys.front();
    if (!record.keys.empty() && in_span(primary, lo, hi)) records.push_back(&record);
  }
  lsm::put_varint32(&out, static_cast<std::uint32_t>(records.size()));
  for (const TxnRecord* record : records) {
    lsm::put_length_prefixed(&out, encode_record(*record));
  }
  return out;
}

void VersionStore::erase_span(std::string_view lo, std::string_view hi) {
  for (auto it = versions_.begin(); it != versions_.end();) {
    it = in_span(it->first, lo, hi) ? versions_.erase(it) : std::next(it);
  }
  for (auto it = intents_.begin(); it != intents_.end();) {
    it = in_span(it->first, lo, hi) ? intents_.erase(it) : std::next(it);
  }
  for (auto it = records_.begin(); it != records_.end();) {
    const bool mine = !it->second.keys.empty() && in_span(it->second.keys.front(), lo, hi);
    it = mine ? records_.erase(it) : std::next(it);
  }
}

bool VersionStore::absorb(std::string_view payload) {
  const char* p = payload.data();
  const char* limit = p + payload.size();

  std::uint32_t count = 0;
  p = lsm::get_varint32(p, limit, &count);
  if (p == nullptr) return false;
  for (std::uint32_t i = 0; i < count; ++i) {
    std::string_view key;
    p = lsm::get_length_prefixed(p, limit, &key);
    if (p == nullptr) return false;
    std::uint32_t versions = 0;
    p = lsm::get_varint32(p, limit, &versions);
    if (p == nullptr) return false;
    VersionChain& chain = versions_[std::string{key}];
    for (std::uint32_t v = 0; v < versions; ++v) {
      Ts ts = 0;
      std::string_view value;
      p = lsm::get_varint64(p, limit, &ts);
      if (p == nullptr) return false;
      if (p >= limit) return false;
      const bool tombstone = *p++ != 0;
      p = lsm::get_length_prefixed(p, limit, &value);
      if (p == nullptr) return false;
      chain[ts] = VersionEntry{tombstone, std::string{value}};
    }
  }

  p = lsm::get_varint32(p, limit, &count);
  if (p == nullptr) return false;
  for (std::uint32_t i = 0; i < count; ++i) {
    std::string_view key;
    std::string_view encoded;
    p = lsm::get_length_prefixed(p, limit, &key);
    if (p == nullptr) return false;
    p = lsm::get_length_prefixed(p, limit, &encoded);
    if (p == nullptr) return false;
    Intent intent;
    if (!decode_intent(encoded, &intent)) return false;
    intents_[std::string{key}] = std::move(intent);
  }

  p = lsm::get_varint32(p, limit, &count);
  if (p == nullptr) return false;
  for (std::uint32_t i = 0; i < count; ++i) {
    std::string_view encoded;
    p = lsm::get_length_prefixed(p, limit, &encoded);
    if (p == nullptr) return false;
    TxnRecord record;
    if (!decode_record(encoded, &record)) return false;
    const auto existing = records_.find(record.id);
    // A terminal verdict already here wins. Absorbing a merge payload must not
    // resurrect a pending record over a committed one.
    if (existing == records_.end() || !terminal(existing->second.status)) {
      records_[record.id] = std::move(record);
    }
  }
  return true;
}

std::string VersionStore::encode_all() const { return encode_span(std::string_view{}, {}); }

bool VersionStore::load(std::string_view payload) {
  clear();
  return absorb(payload);
}

void VersionStore::clear() {
  versions_.clear();
  intents_.clear();
  records_.clear();
}

std::uint64_t VersionStore::collect(Ts safepoint) {
  std::uint64_t collected = 0;
  for (auto& [key, chain] : versions_) {
    // The newest version at or below the safepoint is KEPT: a reader sitting
    // exactly on the safepoint still resolves to it. Everything strictly older
    // than that one is unreachable. Deleting the boundary version is the
    // classic silent corruption and it is INV-MVCC-01's whole subject.
    const auto boundary = chain.lower_bound(safepoint);
    if (boundary == chain.end()) continue;
    auto it = std::next(boundary);
    while (it != chain.end()) {
      it = chain.erase(it);
      ++collected;
    }
  }
  return collected;
}

}  // namespace anvil::txn
