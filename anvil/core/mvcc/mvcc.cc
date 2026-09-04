#include "anvil/core/mvcc/mvcc.h"

#include "anvil/core/buggify.h"
#include "anvil/core/lsm/format.h"
#include "anvil/core/lsm/wal.h"

namespace anvil::mvcc {
namespace {

constexpr char kValue = 1;
constexpr char kTombstone = 0;

std::string encode_version(bool tombstone, std::string_view value) {
  std::string out;
  out.reserve(value.size() + 1);
  out.push_back(tombstone ? kTombstone : kValue);
  out.append(value);
  return out;
}

bool decode_version(std::string_view in, bool* tombstone, std::string_view* value) {
  if (in.empty()) return false;
  *tombstone = in.front() == kTombstone;
  *value = in.substr(1);
  return true;
}

// The whole data key space, for scans that are not restricted to one key.
std::string all_data_lo() { return std::string(1, kDataPrefix); }
std::string all_data_hi() { return std::string(1, static_cast<char>(kDataPrefix + 1)); }

}  // namespace

std::string encode_intent(const Intent& intent) {
  std::string out;
  lsm::put_varint64(&out, intent.txn.value());
  lsm::put_varint64(&out, intent.start_ts);
  out.push_back(intent.tombstone ? kTombstone : kValue);
  lsm::put_length_prefixed(&out, intent.value);
  return out;
}

bool decode_intent(std::string_view in, Intent* out) {
  const char* p = in.data();
  const char* limit = p + in.size();
  std::uint64_t txn = 0;
  std::uint64_t start = 0;
  p = lsm::get_varint64(p, limit, &txn);
  if (p == nullptr) return false;
  p = lsm::get_varint64(p, limit, &start);
  if (p == nullptr) return false;
  if (p >= limit) return false;
  out->tombstone = *p++ == kTombstone;
  std::string_view value;
  p = lsm::get_length_prefixed(p, limit, &value);
  if (p == nullptr) return false;
  out->txn = TxnId{txn};
  out->start_ts = start;
  out->value.assign(value);
  return true;
}

// ---------------------------------------------------------------------------
// reads
// ---------------------------------------------------------------------------

Task<Status> MvccStore::scan_versions(std::string_view key, CommitTs from_ts, std::size_t limit,
                                      std::vector<std::pair<std::string, std::string>>* out)
    const {
  // Inverted timestamps put newer versions first, so a forward scan starting at
  // `from_ts` yields exactly the versions this reader may see, newest first --
  // no reverse iteration and no reading versions above the snapshot.
  co_return co_await db_->scan(seek_for_read(key, from_ts), data_upper_bound(key), limit, out);
}

Task<Status> MvccStore::get(std::string_view key, CommitTs read_ts, TxnId reader,
                            ReadResult* out) {
  *out = ReadResult{};
  ++stats_.reads;

  // The intent first. A committed version older than an intent is not
  // necessarily the answer: the intent may be about to commit at a timestamp
  // this reader would have to see. Only the intent's owner can read through its
  // own intent, which is what makes a transaction see its own writes.
  bool has_intent = false;
  Intent intent;
  Status status = co_await read_intent(key, &has_intent, &intent);
  if (!status.is_ok()) co_return status;

  if (has_intent && intent.txn == reader) {
    out->found = !intent.tombstone;
    out->value = intent.value;
    out->commit_ts = read_ts;
    co_return Status::ok();
  }
  if (has_intent && intent.start_ts <= read_ts && options_.reads_respect_intents) {
    // Somebody else's uncommitted write, and it started before this read's
    // snapshot -- so it might commit below `read_ts` and change the answer.
    // The caller resolves it; guessing here would be guessing at another
    // transaction's outcome.
    ++stats_.reads_blocked;
    out->blocked = true;
    out->blocker = intent;
    co_return Status::ok();
  }

  std::vector<std::pair<std::string, std::string>> found;
  status = co_await scan_versions(key, read_ts, 1, &found);
  if (!status.is_ok()) co_return status;
  ++stats_.versions_scanned;
  if (found.empty()) co_return Status::ok();

  std::string user_key;
  CommitTs commit_ts = 0;
  if (!decode_data_key(found.front().first, &user_key, &commit_ts)) {
    co_return Status{StatusCode::kCorruption, "undecodable mvcc data key"};
  }
  bool tombstone = false;
  std::string_view value;
  if (!decode_version(found.front().second, &tombstone, &value)) {
    co_return Status{StatusCode::kCorruption, "undecodable mvcc version"};
  }
  out->commit_ts = commit_ts;
  out->found = !tombstone;
  out->value.assign(value);
  co_return Status::ok();
}

Task<Status> MvccStore::get_uncertain(std::string_view key, CommitTs read_ts,
                                      CommitTs uncertainty_limit, TxnId reader,
                                      ReadResult* out) {
  // Look above the snapshot first, as far as the clock's honesty requires.
  //
  // A version committed at a timestamp between read_ts and the uncertainty
  // limit may, in real time, have happened *before* this read started -- the
  // clocks simply disagree by less than the bound. Serving the older version
  // would be a stale read that no amount of timestamp arithmetic can excuse, so
  // the read restarts above it instead. This is the cost of external
  // consistency and it is supposed to be visible.
  if (uncertainty_limit > read_ts) {
    std::vector<std::pair<std::string, std::string>> found;
    const Status status = co_await scan_versions(key, uncertainty_limit, 1, &found);
    if (!status.is_ok()) co_return status;
    if (!found.empty()) {
      std::string user_key;
      CommitTs commit_ts = 0;
      if (!decode_data_key(found.front().first, &user_key, &commit_ts)) {
        co_return Status{StatusCode::kCorruption, "undecodable mvcc data key"};
      }
      if (commit_ts > read_ts) {
        ++stats_.uncertainty_restarts;
        *out = ReadResult{};
        out->commit_ts = commit_ts;
        co_return Status{StatusCode::kAborted, "uncertainty restart"};
      }
    }
  }
  co_return co_await get(key, read_ts, reader, out);
}

// ---------------------------------------------------------------------------
// writes
// ---------------------------------------------------------------------------

Task<Status> MvccStore::read_intent(std::string_view key, bool* found, Intent* out) {
  std::string raw;
  const Status status = co_await db_->get(lock_key(key), &raw, found);
  if (!status.is_ok()) co_return status;
  if (!*found) co_return Status::ok();
  if (!decode_intent(raw, out)) {
    co_return Status{StatusCode::kCorruption, "undecodable mvcc intent"};
  }
  co_return Status::ok();
}

Task<Status> MvccStore::put_intent(std::string_view key, const Intent& intent,
                                   Intent* conflict) {
  bool found = false;
  Intent existing;
  Status status = co_await read_intent(key, &found, &existing);
  if (!status.is_ok()) co_return status;
  if (found && existing.txn != intent.txn) {
    *conflict = existing;
    co_return Status{StatusCode::kAborted, "key already has an intent"};
  }

  status = co_await db_->put(lock_key(key), encode_intent(intent));
  if (!status.is_ok()) co_return status;
  ++stats_.intents_written;
  co_return Status::ok();
}

Task<Status> MvccStore::commit_intent(std::string_view key, TxnId txn, CommitTs commit_ts) {
  bool found = false;
  Intent intent;
  Status status = co_await read_intent(key, &found, &intent);
  if (!status.is_ok()) co_return status;
  if (!found) co_return Status::ok();  // already resolved; committing twice is a no-op
  if (intent.txn != txn) {
    co_return Status{StatusCode::kAborted, "intent belongs to another transaction"};
  }

  // The version first, then the intent removal. A crash between them leaves a
  // committed version and an intent for the same transaction, which the next
  // reader resolves by finding the version -- recoverable. The other order
  // leaves neither, which loses a committed write.
  status = co_await db_->put(encode_data_key(key, commit_ts),
                             encode_version(intent.tombstone, intent.value));
  if (!status.is_ok()) co_return status;
  ++stats_.versions_written;

  status = co_await db_->del(lock_key(key));
  if (!status.is_ok()) co_return status;
  ++stats_.intents_committed;
  co_return Status::ok();
}

Task<Status> MvccStore::commit_all(const std::set<std::string>& keys, TxnId txn,
                                   CommitTs commit_ts) {
  // Read every intent first, then apply one batch. The reads can suspend; the
  // batch cannot be observed partially.
  lsm::WriteBatch batch;
  std::vector<std::string> staged;  // keeps the encoded keys alive for the batch
  staged.reserve(keys.size() * 2);
  std::size_t resolved = 0;

  for (const std::string& key : keys) {
    bool found = false;
    Intent intent;
    const Status status = co_await read_intent(key, &found, &intent);
    if (!status.is_ok()) co_return status;
    if (!found) {
      // A missing intent means one of two very different things, and committing
      // cannot treat them alike. Either this is a retry and the batch already
      // landed -- in which case the version at commit_ts exists and skipping is
      // right -- or the intent never made it to disk, in which case skipping
      // would commit the transaction without one of its keys and call that
      // success. The second is a lost write inside a committed transaction,
      // which is the worst outcome this layer can produce, so it is worth a
      // read to tell them apart.
      std::vector<std::pair<CommitTs, std::string>> versions;
      const Status probe = co_await versions_of(key, &versions);
      if (!probe.is_ok()) co_return probe;
      bool already_written = false;
      for (const auto& [ts, unused] : versions) {
        (void)unused;
        if (ts == commit_ts) already_written = true;
      }
      if (already_written) continue;
      co_return Status{StatusCode::kAborted,
                       "no intent for a key in the write set: the write never landed"};
    }
    if (intent.txn != txn) {
      co_return Status{StatusCode::kAborted, "intent belongs to another transaction"};
    }
    staged.push_back(encode_data_key(key, commit_ts));
    batch.put(staged.back(), encode_version(intent.tombstone, intent.value));
    staged.push_back(lock_key(key));
    batch.del(staged.back());
    ++resolved;
  }
  if (resolved == 0) co_return Status::ok();

  const Status status = co_await db_->write(batch);
  if (!status.is_ok()) co_return status;
  stats_.versions_written += resolved;
  stats_.intents_committed += resolved;
  co_return Status::ok();
}

Task<Status> MvccStore::abort_all(const std::set<std::string>& keys, TxnId txn) {
  lsm::WriteBatch batch;
  std::vector<std::string> staged;
  staged.reserve(keys.size());
  std::size_t resolved = 0;

  for (const std::string& key : keys) {
    bool found = false;
    Intent intent;
    const Status status = co_await read_intent(key, &found, &intent);
    if (!status.is_ok()) co_return status;
    if (!found || intent.txn != txn) continue;
    staged.push_back(lock_key(key));
    batch.del(staged.back());
    ++resolved;
  }
  if (resolved == 0) co_return Status::ok();

  const Status status = co_await db_->write(batch);
  if (!status.is_ok()) co_return status;
  stats_.intents_aborted += resolved;
  co_return Status::ok();
}

Task<Status> MvccStore::abort_intent(std::string_view key, TxnId txn) {
  bool found = false;
  Intent intent;
  const Status status = co_await read_intent(key, &found, &intent);
  if (!status.is_ok()) co_return status;
  if (!found) co_return Status::ok();
  if (intent.txn != txn) {
    co_return Status{StatusCode::kAborted, "intent belongs to another transaction"};
  }
  ++stats_.intents_aborted;
  co_return co_await db_->del(lock_key(key));
}

// ---------------------------------------------------------------------------
// garbage collection
// ---------------------------------------------------------------------------

Task<Status> MvccStore::versions_of(
    std::string_view key, std::vector<std::pair<CommitTs, std::string>>* out) const {
  out->clear();
  std::vector<std::pair<std::string, std::string>> raw;
  const Status status =
      co_await db_->scan(data_prefix(key), data_upper_bound(key), 4096, &raw);
  if (!status.is_ok()) co_return status;
  for (const auto& [encoded, value] : raw) {
    std::string user_key;
    CommitTs commit_ts = 0;
    if (!decode_data_key(encoded, &user_key, &commit_ts)) continue;
    bool tombstone = false;
    std::string_view payload;
    if (!decode_version(value, &tombstone, &payload)) continue;
    out->emplace_back(commit_ts, tombstone ? std::string{} : std::string{payload});
  }
  co_return Status::ok();
}

Task<Status> MvccStore::keys_with_versions(std::vector<std::string>* out) const {
  out->clear();
  std::vector<std::pair<std::string, std::string>> raw;
  const Status status = co_await db_->scan(all_data_lo(), all_data_hi(), 65536, &raw);
  if (!status.is_ok()) co_return status;
  std::string previous;
  for (const auto& [encoded, value] : raw) {
    std::string user_key;
    CommitTs commit_ts = 0;
    if (!decode_data_key(encoded, &user_key, &commit_ts)) continue;
    if (user_key != previous) {
      out->push_back(user_key);
      previous = user_key;
    }
  }
  co_return Status::ok();
}

Task<Status> MvccStore::collect_garbage(CommitTs safepoint, std::size_t max_keys,
                                        std::uint64_t* collected) {
  *collected = 0;
  ++stats_.gc_passes;

  std::vector<std::string> keys;
  Status status = co_await keys_with_versions(&keys);
  if (!status.is_ok()) co_return status;

  // A GC pass that keeps suspending after a single key looks nothing like one
  // that walks its whole batch in a tight loop: it interleaves with far more
  // reads, writes and other GC passes per key touched. The safepoint math
  // (INV-MVCC's boundary-version rule, just above) does not depend on batch
  // size, so shrinking it is safe -- it only changes how often this
  // coroutine hands control back to the scheduler.
  if (ANVIL_BUGGIFY) max_keys = 1;

  std::size_t touched = 0;
  for (const std::string& key : keys) {
    if (touched++ >= max_keys) break;

    std::vector<std::pair<std::string, std::string>> raw;
    status = co_await db_->scan(data_prefix(key), data_upper_bound(key), 4096, &raw);
    if (!status.is_ok()) co_return status;

    // `raw` is newest-first. Walk down to the first version at or below the
    // safepoint: that one is still reachable by a reader sitting exactly on the
    // safepoint and must be KEPT. Everything after it is unreachable.
    //
    // Off-by-one here is the entire bug class. Keeping `>= safepoint` and
    // dropping the rest would delete the version a safepoint reader resolves
    // to, and the reader would silently fall through to an older value or to
    // nothing -- no error, no checksum, no complaint from any layer.
    bool kept_boundary = false;
    for (const auto& [encoded, value] : raw) {
      std::string user_key;
      CommitTs commit_ts = 0;
      if (!decode_data_key(encoded, &user_key, &commit_ts)) continue;
      if (commit_ts > safepoint) continue;  // above the safepoint: always visible
      if (!kept_boundary && options_.gc_keeps_safepoint_version) {
        kept_boundary = true;  // the newest version at or below the safepoint
        continue;
      }
      status = co_await db_->del(encoded);
      if (!status.is_ok()) co_return status;
      ++*collected;
      ++stats_.versions_collected;
    }
  }
  co_return Status::ok();
}

}  // namespace anvil::mvcc
