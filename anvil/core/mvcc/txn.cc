#include "anvil/core/mvcc/txn.h"

#include <algorithm>

namespace anvil::mvcc {

const char* to_string(TxnState state) noexcept {
  switch (state) {
    case TxnState::kActive: return "active";
    case TxnState::kCommitted: return "committed";
    case TxnState::kAborted: return "aborted";
    case TxnState::kWounded: return "wounded";
    case TxnState::kResolving: return "resolving";
  }
  return "?";
}

Txn* TxnManager::begin(CommitTs start_ts, IsolationLevel level) {
  const TxnId id{next_id_++};
  Txn txn;
  txn.id = id;
  txn.start_ts = start_ts;
  txn.level = level;
  txn.state = TxnState::kActive;
  ++stats_.begun;
  return &txns_.emplace(id.value(), std::move(txn)).first->second;
}

Txn* TxnManager::find(TxnId id) {
  const auto it = txns_.find(id.value());
  return it == txns_.end() ? nullptr : &it->second;
}

const Txn* TxnManager::find(TxnId id) const {
  const auto it = txns_.find(id.value());
  return it == txns_.end() ? nullptr : &it->second;
}

// ---------------------------------------------------------------------------
// the safepoint
// ---------------------------------------------------------------------------

CommitTs TxnManager::safepoint() const {
  // Clamped to the newest commit. Without this, a moment with no live reader
  // yields a safepoint of kMaxCommitTs -- "collect everything" -- which is
  // technically defensible and practically a loaded gun: any later change that
  // makes a reader visible slightly late turns it into total data loss, and the
  // invariant that would have caught it cannot even be stated against a bound
  // of infinity.
  CommitTs floor = std::min(closed_ts_, highest_commit_);
  for (const auto& [id, txn] : txns_) {
    // A transaction still waiting to write its outcome down holds the floor too.
    // Conservative on purpose: its intents are not versions and GC will not
    // touch them, but pinning the floor keeps the store in a state its own
    // resolution can still be applied to, and an unresolved transaction is
    // exactly the case where being clever costs more than being slow.
    if (!txn.live() && !txn.resolving()) continue;
    floor = std::min(floor, txn.start_ts);
  }
  for (const auto& [handle, ts] : snapshots_) {
    floor = std::min(floor, ts);
  }
  return floor;
}

void TxnManager::open_snapshot(std::uint64_t handle, CommitTs ts) { snapshots_[handle] = ts; }
void TxnManager::close_snapshot(std::uint64_t handle) { snapshots_.erase(handle); }

bool TxnManager::detect_deadlock(std::vector<TxnId>* cycle) {
  if (!locks_->find_cycle(cycle)) return false;
  ++stats_.deadlocks_detected;
  return true;
}

// ---------------------------------------------------------------------------
// reads
// ---------------------------------------------------------------------------

Task<Status> TxnManager::read(TxnId id, std::string_view key, bool* found,
                              std::string* value) {
  *found = false;
  value->clear();
  Txn* txn = find(id);
  if (txn == nullptr) co_return Status{StatusCode::kNotFound, "no such transaction"};
  if (!txn->live()) co_return Status{StatusCode::kAborted, "transaction is not active"};

  // The SSI mechanism: record the span before doing the read, so a transaction
  // that aborts partway still has an honest record of what it looked at.
  if (txn->level == IsolationLevel::kSerializable) {
    txn->reads.insert(Txn::Span{std::string{key}, std::string{}});
  }

  ReadResult result;
  const Status status = co_await store_->get(key, txn->start_ts, id, &result);
  if (!status.is_ok()) co_return status;

  if (result.blocked) {
    // Somebody else's intent is in the way. Wound-wait decides which of the two
    // gives ground, and the answer is a function of their ages only -- never of
    // who arrived first, which would make the outcome depend on scheduling.
    ++stats_.blocked_reads;
    LockHolder blocker;
    blocker.txn = result.blocker.txn;
    blocker.start_ts = result.blocker.start_ts;
    blocker.kind = LockKind::kExclusive;
    co_return co_await resolve_conflict(*txn, blocker);
  }

  *found = result.found;
  *value = result.value;
  co_return Status::ok();
}

// ---------------------------------------------------------------------------
// writes
// ---------------------------------------------------------------------------

Task<Status> TxnManager::write(TxnId id, std::string_view key, std::string_view value,
                               bool tombstone) {
  Txn* txn = find(id);
  if (txn == nullptr) co_return Status{StatusCode::kNotFound, "no such transaction"};
  if (!txn->live()) co_return Status{StatusCode::kAborted, "transaction is not active"};

  LockHolder blocker;
  const AcquireOutcome outcome =
      locks_->acquire(id, txn->start_ts, key, LockKind::kExclusive, &blocker);
  if (outcome == AcquireOutcome::kWaiting || outcome == AcquireOutcome::kWoundHolder) {
    co_return co_await resolve_conflict(*txn, blocker);
  }

  // First-committer-wins. If any version of this key committed after this
  // transaction's snapshot was taken, the write is based on a value that is no
  // longer current and the transaction must abort. Without this check the
  // engine offers read-committed while claiming snapshot isolation, and the
  // difference only shows up as lost updates under concurrency.
  std::vector<std::pair<CommitTs, std::string>> versions;
  Status status = co_await store_->versions_of(key, &versions);
  if (!status.is_ok()) co_return status;
  for (const auto& [commit_ts, unused] : versions) {
    (void)unused;
    if (commit_ts > txn->start_ts) {
      // Abort properly rather than just flipping the state field.
      //
      // The transaction may already hold intents from earlier writes, and
      // setting `kAborted` here leaves them on disk with an owner that says it
      // is finished -- a state that is a lie until whoever called us gets around
      // to calling abort(). It is not a small window either: the caller's abort
      // has to reach the disk, and under injected latency that is long enough
      // for another writer to find the key blocked by a transaction that no
      // longer exists. Announcing an outcome the durable state does not yet
      // reflect is the same defect as ANV-0021, one layer down.
      ++stats_.write_conflicts;
      const Status cleanup = co_await abort(id);
      (void)cleanup;  // a failed cleanup leaves it kResolving for the janitor
      co_return Status{StatusCode::kAborted, "write-write conflict (first committer wins)"};
    }
  }

  Intent intent;
  intent.txn = id;
  intent.start_ts = txn->start_ts;
  intent.tombstone = tombstone;
  intent.value.assign(value);

  // The key joins the write set *before* the write is attempted, and this
  // ordering is the whole point.
  //
  // put_intent can fail with the intent already on disk: the batch landed and
  // the fsync did not, and the status says only "error". Recording the key
  // afterwards means that on failure the transaction does not know it touched
  // this key, so neither commit nor abort will ever resolve it -- the intent
  // sits there forever, blocking every later writer of that key, with no error
  // anywhere and no way to attribute it once the transaction is forgotten. An
  // extra key in the write set costs one no-op lookup at resolution time;
  // a missing one costs the key.
  txn->writes.insert(std::string{key});

  Intent conflict;
  status = co_await store_->put_intent(key, intent, &conflict);
  if (!status.is_ok()) {
    // The lock table said the key was free and the store disagrees. That can
    // only happen if an intent outlived its transaction, which INV-MVCC-08 is
    // there to catch -- surface it rather than silently overwriting.
    co_return status;
  }
  co_return Status::ok();
}

// ---------------------------------------------------------------------------
// conflict resolution
// ---------------------------------------------------------------------------

Task<Status> TxnManager::resolve_conflict(Txn& txn, const LockHolder& blocker) {
  if (is_older(txn.start_ts, txn.id, blocker.start_ts, blocker.txn)) {
    // We are older: the holder is wounded. It is not aborted here -- the
    // transaction that owns it does that when it next touches the system, which
    // keeps every state change to a transaction in the hands of its owner and
    // means no coroutine ever mutates another's bookkeeping mid-flight.
    Txn* victim = find(blocker.txn);
    if (victim != nullptr && victim->live()) {
      victim->state = TxnState::kWounded;
      ++stats_.wounded;
    }
    co_return Status{StatusCode::kAborted, "retry: the blocking transaction was wounded"};
  }
  // We are younger: we wait, which for this caller means "back off and retry".
  co_return Status{StatusCode::kUnavailable, "blocked by an older transaction"};
}

// ---------------------------------------------------------------------------
// commit and abort
// ---------------------------------------------------------------------------

Task<Status> TxnManager::commit(TxnId id, CommitTs commit_ts) {
  Txn* txn = find(id);
  if (txn == nullptr) co_return Status{StatusCode::kNotFound, "no such transaction"};
  if (txn->state == TxnState::kWounded) {
    co_await abort(id);
    co_return Status{StatusCode::kAborted, "wounded by an older transaction"};
  }
  if (!txn->live()) co_return Status{StatusCode::kAborted, "transaction is not active"};
  if (commit_ts <= txn->start_ts) {
    // A commit at or below the snapshot would make the transaction's own writes
    // invisible to itself on a re-read, and would break the strict descending
    // order versions are stored in (INV-MVCC-03).
    co_return Status{StatusCode::kInvalidArgument, "commit timestamp must exceed start"};
  }

  // One atomic batch, not a key at a time.
  //
  // Resolving intents one by one leaves a window in which a concurrent reader
  // sees some of this transaction's keys at their new values and the rest at
  // their old ones -- half a transaction, which is precisely what snapshot
  // isolation promises never to show. The engine's batch is one record with one
  // checksum: all of it lands or none of it does.
  // The decision is taken here and recorded before the write is attempted, so
  // that a failed write leaves a transaction with a known outcome rather than an
  // unknown one.
  txn->commit_ts = commit_ts;
  const Status status = co_await store_->commit_all(txn->writes, id, commit_ts);
  if (!status.is_ok()) {
    txn->state = TxnState::kResolving;
    ++stats_.resolutions_deferred;
    co_return status;
  }
  stats_.resolved_intents += txn->writes.size();

  txn->state = TxnState::kCommitted;
  txn->resolved_at = runtime_->now();
  highest_commit_ = std::max(highest_commit_, commit_ts);
  locks_->release_all(id);
  ++stats_.committed;
  co_return Status::ok();
}

Task<Status> TxnManager::abort(TxnId id) {
  Txn* txn = find(id);
  if (txn == nullptr) co_return Status{StatusCode::kNotFound, "no such transaction"};
  if (txn->state == TxnState::kCommitted) {
    co_return Status{StatusCode::kAborted, "already committed"};
  }
  if (txn->resolving()) {
    // Its outcome is already decided and merely unwritten. Letting an abort
    // through here would turn a commit that failed to land into an abort, which
    // is not a retry of the decision -- it is a reversal of one, and the client
    // that got the ambiguous answer may have been told "committed" by anyone
    // else who read the state in between.
    co_return Status{StatusCode::kUnavailable, "outcome already decided; awaiting resolution"};
  }

  txn->commit_ts = 0;  // the decision is abort
  const Status status = co_await store_->abort_all(txn->writes, id);
  if (!status.is_ok()) {
    txn->state = TxnState::kResolving;
    ++stats_.resolutions_deferred;
    co_return status;
  }
  stats_.resolved_intents += txn->writes.size();
  txn->state = TxnState::kAborted;
  txn->resolved_at = runtime_->now();
  locks_->release_all(id);
  ++stats_.aborted;
  co_return Status::ok();
}

// ---------------------------------------------------------------------------
// finishing what the disk interrupted
// ---------------------------------------------------------------------------

std::vector<TxnId> TxnManager::pending_resolutions() const {
  std::vector<TxnId> pending;
  for (const auto& [id, txn] : txns_) {
    if (txn.resolving()) pending.push_back(txn.id);
  }
  return pending;  // ordered by id: the retry order does not depend on a hash seed
}

Task<Status> TxnManager::resolve_pending(std::uint64_t* resolved) {
  *resolved = 0;
  Status last = Status::ok();
  for (const TxnId id : pending_resolutions()) {
    Txn* txn = find(id);
    if (txn == nullptr || !txn->resolving()) continue;
    ++txn->resolve_attempts;
    ++stats_.resolutions_retried;

    const bool committing = txn->commit_ts != 0;
    const Status status = committing
                              ? co_await store_->commit_all(txn->writes, id, txn->commit_ts)
                              : co_await store_->abort_all(txn->writes, id);
    if (!status.is_ok()) {
      last = status;
      continue;  // still stuck; the next pass tries again
    }
    stats_.resolved_intents += txn->writes.size();
    txn->state = committing ? TxnState::kCommitted : TxnState::kAborted;
    txn->resolved_at = runtime_->now();
    locks_->release_all(id);
    ++(committing ? stats_.committed : stats_.aborted);
    if (committing) highest_commit_ = std::max(highest_commit_, txn->commit_ts);
    ++*resolved;
  }
  co_return last;
}

bool TxnManager::retire(TxnId id, Duration min_age) {
  const auto it = txns_.find(id.value());
  if (it == txns_.end()) return false;
  if (it->second.live() || it->second.resolving()) return false;
  if (runtime_->now().physical <
      it->second.resolved_at.physical + static_cast<std::uint64_t>(min_age.nanos())) {
    return false;
  }
  txns_.erase(it);
  return true;
}

}  // namespace anvil::mvcc
