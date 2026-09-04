#include "anvil/core/txn/coordinator.h"

#include <algorithm>
#include <utility>

#include "anvil/core/buggify.h"

namespace anvil::txn {

const char* to_string(Level level) noexcept {
  switch (level) {
    case Level::kSnapshot: return "snapshot-isolation";
    case Level::kSerializable: return "serializable";
    case Level::kStrictSerializable: return "strict-serializable";
  }
  return "?";
}

const char* to_string(TxnOutcome outcome) noexcept {
  switch (outcome) {
    case TxnOutcome::kCommitted: return "committed";
    case TxnOutcome::kAborted: return "aborted";
    case TxnOutcome::kUnknown: return "unknown";
  }
  return "?";
}

Coordinator::Coordinator(Runtime* runtime, NodeId self, shard::ShardStore* store,
                         CoordinatorOptions options)
    : runtime_(runtime),
      self_(self),
      store_(store),
      options_(options),
      clock_(options.clock_uncertainty) {}

void Coordinator::on_reply(const shard::ShardStore::TxnReply& reply) {
  const auto it = inbox_.find(reply.seq);
  if (it == inbox_.end()) return;  // a late reply to a request we gave up on
  it->second.answered = true;
  it->second.status = reply.status;
  it->second.result = reply.result;
  it->second.read = reply.read;
  it->second.hint = reply.leader_hint;
}

// ---------------------------------------------------------------------------
// timestamps
// ---------------------------------------------------------------------------

// A timestamp from the oracle. `fresh` is the whole of ANV-0058.
//
// Reserving a run of timestamps and handing them out locally is the standard
// way to make an oracle affordable, and for a *start* timestamp it is sound: a
// stale start reads an older snapshot, which snapshot isolation permits, and
// the prewrite conflict test is taken against that same start, so a stale one
// makes the test stricter rather than weaker.
//
// For a *commit* timestamp it is not sound, and the failure is invisible from
// inside either transaction involved. A reserved batch is a promise that these
// numbers are yours. It is not a promise that they are still current. Two
// coordinators holding [12,76) and [386,450) hand out timestamps whose order
// has nothing to do with the order their transactions actually run in, and the
// whole of first-committer-wins rests on that order:
//
//   T_b starts at 390, reads key k, finds nothing -- correctly, k is empty.
//   T_a starts at 12, from a batch reserved long ago. It prewrites k (no
//     version above 12 exists, so this is legal) and commits it at 13.
//   T_b prewrites k. The conflict test is "a version committed above my
//     start_ts". The version is at 13, and 13 < 390, so there is no conflict
//     and the prewrite succeeds.
//   T_b commits at 391, overwriting a value it never saw.
//
// Both transactions were told they committed and one of the two writes is
// gone, with no rule broken -- because the rule assumes a commit timestamp is
// allocated *after* every read that could conflict with it, which is exactly
// what a reservation stops being true. Percolator's oracle hands out strictly
// increasing timestamps on demand, and "on demand" is load-bearing at commit
// rather than an implementation detail.
//
// Drawing *both* ends fresh was tried first and is worse than the disease: on
// the fault profiles this phase runs, an extra round trip per begin took seed 7
// from 15 commits to zero, and a run that commits nothing proves nothing. One
// fresh draw per commit, batched starts, keeps the ordering the levels promise
// at a cost that is measured rather than assumed -- see CONTEXT.md section 14.
Task<bool> Coordinator::next_timestamp(Ts* out, bool fresh) {
  if (options_.source == TsSource::kHybrid) {
    // The HLC has the property for free: every reading is taken now and folded
    // forward from every peer already seen, so it cannot be stale the way a
    // reservation can.
    *out = clock_.now(runtime_->now());
    co_return true;
  }
  if (!fresh && batch_next_ < batch_end_) {
    *out = batch_next_++;
    co_return true;
  }
  if (!oracle_) co_return false;
  Ts first = 0;
  if (!co_await oracle_(fresh ? 1 : 64, &first)) co_return false;
  if (fresh) {
    // Deliberately not used to refill the batch. A commit draw is a point, not
    // the head of a run, and treating it as one would put the next start back
    // in the past the moment this commit's neighbours are handed out.
    *out = first;
    co_return true;
  }
  batch_next_ = first;
  batch_end_ = first + 64;
  *out = batch_next_++;
  co_return true;
}

// ---------------------------------------------------------------------------
// one request
// ---------------------------------------------------------------------------

Task<Coordinator::Response> Coordinator::send(bool read, const shard::RangeDescriptor& range,
                                              const TxnCommand& command, std::string_view key,
                                              Ts read_ts, Ts uncertainty, TxnId reader) {
  Response response;
  shard::ShardStore::TxnRequest request;
  request.read = read;
  request.range = range.id;
  request.generation = range.generation;
  request.client = self_.value();
  request.seq = next_seq_++;
  request.key.assign(key);
  request.read_ts = read_ts;
  request.uncertainty_limit = uncertainty;
  request.reader = reader;
  request.command = command;

  // Whoever holds the lease, or any replica -- which answers with a hint, and
  // the hint is how the coordinator finds the one that can serve it.
  //
  // The hint used to be recorded by `on_reply` and read by nobody, which is
  // [ANV-0061]. Every retry above this went back to the same replica the cached
  // descriptor named, so a coordinator whose descriptor was one lease behind
  // spent all eight of its attempts asking a node that had already told it, in
  // as many words, which node to ask instead. It shows up as `not_leader`
  // counted in the thousands beside a few hundred transactions begun: on seed 1
  // at snapshot isolation, 1,986 rejections against 321 transactions, of which
  // 5 committed. A retry that does not use what the last attempt returned is
  // not a retry, it is the same request again -- gotcha 10.12 with the repeated
  // half being the destination rather than the operation.
  NodeId to;
  const auto hinted = leaders_.find(range.id.value());
  if (hinted != leaders_.end()) to = hinted->second;
  if (!to.valid()) to = range.lease.holder;
  if (!to.valid()) to = range.replicas.empty() ? NodeId{} : range.replicas.front();
  if (!to.valid()) co_return response;

  inbox_[request.seq] = Response{};
  co_await store_->send_txn(request, to);

  const Timestamp deadline = runtime_->now().advanced_by(options_.rpc_timeout);
  while (runtime_->now() < deadline) {
    co_await runtime_->sleep_for(options_.poll);
    const auto it = inbox_.find(request.seq);
    if (it == inbox_.end()) break;
    if (!it->second.answered) continue;
    response = it->second;
    inbox_.erase(it);
    // Learned from the answer, whichever way it went: a hint names the node
    // that can serve this range, and an answer that came back kOk names it by
    // having answered. Cleared rather than kept when neither holds, so that a
    // stale hint cannot outlive the lease it came from -- the next attempt then
    // falls back to the descriptor, which is where a wrong-range reply sends it
    // anyway.
    if (response.hint.valid()) {
      leaders_[range.id.value()] = response.hint;
    } else if (response.status == shard::ShardStore::kOk) {
      leaders_[range.id.value()] = to;
    } else {
      leaders_.erase(range.id.value());
    }
    co_return response;
  }
  inbox_.erase(request.seq);
  ++stats_.rpc_timeouts;
  // Silence is not evidence about who the leader is, but it is evidence that
  // this node did not answer, and going back to it unconditionally is how a
  // crashed replica takes a coordinator down with it.
  leaders_.erase(range.id.value());
  co_return response;
}

// ---------------------------------------------------------------------------
// begin, get, put
// ---------------------------------------------------------------------------

Task<bool> Coordinator::begin(Handle* handle) {
  Ts start = 0;
  if (!co_await next_timestamp(&start)) co_return false;

  // The transaction id is the start timestamp itself, not a counter. A
  // per-coordinator counter is exactly the kind of in-memory decision that
  // does not survive a crash (CONTEXT.md gotcha 10.25): every restart builds
  // a fresh Coordinator with next_id starting back at 1, so a node that
  // crashes and reboots mid-run reissues an id a transaction from its
  // previous incarnation may already hold a record or an intent under --
  // two genuinely different transactions sharing one identity. `start` has
  // no such problem: it is reserved from the replicated oracle (or, for the
  // HLC, folded from observed peers) precisely so that it is never handed out
  // twice, which is INV-TXN-09 and is exactly the property an id needs here.
  handle->id = start;
  handle->epoch = 1;
  handle->start_ts = start;
  handle->commit_ts = 0;
  handle->level = options_.level;
  handle->primary.clear();
  handle->writes.clear();
  handle->reads.clear();
  handle->committed = false;

  // The uncertainty window. With the oracle there is none -- its timestamps say
  // nothing about real time, so nothing can be "recent enough to be ambiguous".
  // With the HLC it is the declared bound, and it is the reason a read can come
  // back saying "restart above this" rather than an answer.
  handle->uncertainty_limit =
      options_.source == TsSource::kHybrid ? clock_.interval(start).latest : start;

  ++stats_.begun;
  live_[handle->id] = *handle;
  co_return true;
}

Task<ReadStatus> Coordinator::get(Handle* handle, std::string_view key, bool* found,
                                  std::string* value) {
  *found = false;
  value->clear();
  ++stats_.reads;

  // A key this transaction has already written reads back its own value. Doing
  // it here rather than at the range is what makes read-your-writes work
  // without every range knowing about the write set.
  const auto buffered = handle->writes.find(std::string{key});
  if (buffered != handle->writes.end()) {
    *found = !buffered->second.tombstone;
    *value = buffered->second.value;
    co_return ReadStatus::kOk;
  }

  for (std::uint32_t attempt = 0; attempt < 8; ++attempt) {
    shard::RangeDescriptor range;
    if (!store_->locate(key, &range)) {
      co_await runtime_->sleep_for(options_.retry_backoff);
      continue;
    }

    TxnCommand unused;
    const Response response = co_await send(/*read=*/true, range, unused, key, handle->start_ts,
                                            handle->uncertainty_limit, handle->id);
    if (!response.answered) continue;
    if (response.status == shard::ShardStore::kWrongRange) {
      ++stats_.wrong_range;
      store_->cache().invalidate(range.id);
      continue;
    }
    if (response.status != shard::ShardStore::kOk) {
      ++stats_.not_leader;
      co_await runtime_->sleep_for(options_.retry_backoff);
      continue;
    }

    if (response.read.status == ReadStatus::kBlocked) {
      ++stats_.reads_blocked;
      if (!co_await resolve_blocker(handle, key, response.read)) {
        co_await runtime_->sleep_for(options_.retry_backoff);
      }
      continue;  // the intent is gone or resolved; read again
    }

    if (response.read.status == ReadStatus::kUncertain) {
      if (!options_.restart_on_uncertainty) {
        // The mutation: pretend the ambiguous version is simply in the future.
        // It may have been written before this read began in real time, and
        // skipping it is a read that missed a committed write.
        co_return ReadStatus::kOk;
      }
      ++stats_.restarts_uncertain;
      co_return ReadStatus::kUncertain;
    }

    if (response.read.status != ReadStatus::kOk) {
      co_await runtime_->sleep_for(options_.retry_backoff);
      continue;
    }

    *found = response.read.found;
    *value = response.read.value;
    // The version this read saw, remembered for the refresh. A serializable
    // transaction that is later pushed forward has to prove nothing changed
    // here in between, and it cannot do that without knowing what it saw.
    handle->reads[std::string{key}] = response.read.commit_ts;
    live_[handle->id] = *handle;
    co_return ReadStatus::kOk;
  }
  co_return ReadStatus::kUnavailable;
}

void Coordinator::put(Handle* handle, std::string_view key, std::string_view value) {
  Intent intent;
  intent.txn = handle->id;
  intent.epoch = handle->epoch;
  intent.start_ts = handle->start_ts;
  intent.value.assign(value);
  handle->writes[std::string{key}] = intent;
  if (handle->primary.empty()) handle->primary.assign(key);
  ++stats_.writes;
  live_[handle->id] = *handle;
}

// ---------------------------------------------------------------------------
// resolving somebody else's intent
// ---------------------------------------------------------------------------

Task<bool> Coordinator::resolve_blocker(Handle* handle, std::string_view blocked_key,
                                        const ReadResult& blocked) {
  // Wound-wait by start timestamp, which is what keeps the wait-for graph
  // acyclic without a detector: an older transaction may push a younger one out
  // of the way, and a younger one waits. Two transactions can therefore never
  // be waiting on each other, because the age comparison is a total order.
  waits_[handle->id] = blocked.blocker;
  ++stats_.pushes;

  shard::RangeDescriptor record_range;
  if (blocked.blocker_primary.empty() || !store_->locate(blocked.blocker_primary, &record_range)) {
    waits_.erase(handle->id);
    co_return false;
  }

  TxnCommand push;
  push.op = TxnOp::kPushRecord;
  push.key = blocked.blocker_primary;
  push.txn = blocked.blocker;
  push.epoch = blocked.blocker_epoch;
  push.push_to = handle->start_ts + 1;
  push.now = runtime_->now().physical;
  // An older transaction aborts a younger blocker outright; a younger one only
  // aborts a blocker whose heartbeat has lapsed. Both are the same command with
  // one flag, which is what keeps the two policies from drifting apart.
  push.abort_expired = true;

  // Widen the window between deciding to push and the push actually landing.
  // Wound-wait's acyclicity argument rests on the age comparison being a total
  // order at the moment the push is evaluated, not on the push being prompt --
  // so a blocker's own state (heartbeat, commit, abort) is free to change
  // while this is in flight, and the wait-for bookkeeping above must survive
  // that. That race is otherwise only reachable by unlucky scheduling.
  if (ANVIL_BUGGIFY) co_await runtime_->sleep_for(options_.retry_backoff);

  const Response response =
      co_await send(/*read=*/false, record_range, push, push.key, 0, 0, handle->id);
  waits_.erase(handle->id);
  if (!response.answered) co_return false;
  if (response.status != shard::ShardStore::kOk) co_return false;

  if (response.result.status == TxnStatus::kAborted) {
    ++stats_.pushes_aborted_expired;
    // Clean up the intent that blocked us, so the next reader does not have to
    // repeat the whole exchange. Best effort: if this fails, the next reader
    // will do it.
    // The key we were blocked *on*, not the blocker's primary. Rolling back the
    // primary's intent instead leaves the one in our way exactly where it was,
    // so the next read blocks on it again -- and quietly drops an intent on a
    // key nobody asked about.
    shard::RangeDescriptor key_range;
    if (store_->locate(blocked_key, &key_range)) {
      TxnCommand rollback;
      rollback.op = TxnOp::kRollbackIntent;
      rollback.key.assign(blocked_key);
      rollback.txn = blocked.blocker;
      rollback.epoch = blocked.blocker_epoch;
      co_await send(/*read=*/false, key_range, rollback, rollback.key, 0, 0, handle->id);
    }
    co_return true;
  }
  if (response.result.status == TxnStatus::kCommitted) {
    // The blocker committed, so the intent in our way is a version that has
    // not been turned into one yet -- the same resolution its own coordinator
    // would have done in resolve_intents, performed here instead because that
    // was best-effort and this one is not optional: without it, the intent is
    // exactly as blocking after this call as before it, and every future
    // reader of this key repeats the same round trip forever rather than
    // resolving anything (this is the lazy half of INV-TXN-02's atomicity
    // argument -- "a reader that meets an intent goes to the primary and
    // asks" is only true if asking also resolves what it finds).
    shard::RangeDescriptor key_range;
    if (store_->locate(blocked_key, &key_range)) {
      TxnCommand resolve;
      resolve.op = TxnOp::kCommitIntent;
      resolve.key.assign(blocked_key);
      resolve.txn = blocked.blocker;
      resolve.epoch = blocked.blocker_epoch;
      resolve.commit_ts = response.result.commit_ts;
      co_await send(/*read=*/false, key_range, resolve, resolve.key, 0, 0, handle->id);
    }
    co_return true;
  }
  co_return false;  // still pending; the caller backs off and tries again
}

// ---------------------------------------------------------------------------
// the read refresh
// ---------------------------------------------------------------------------

Task<bool> Coordinator::refresh_reads(Handle* handle, Ts up_to) {
  if (!options_.refresh_reads_on_push) {
    // The mutation. Committing above a timestamp somebody has already read from
    // without re-checking what we read is precisely write skew, and it is
    // invisible from inside the transaction.
    co_return true;
  }
  ++stats_.refreshes;
  for (const auto& [key, seen_at] : handle->reads) {
    shard::RangeDescriptor range;
    if (!store_->locate(key, &range)) co_return false;
    TxnCommand unused;
    const Response response =
        co_await send(/*read=*/true, range, unused, key, up_to, up_to, handle->id);
    if (!response.answered || response.status != shard::ShardStore::kOk) co_return false;
    if (response.read.status != ReadStatus::kOk) co_return false;
    if (response.read.commit_ts != seen_at) {
      // Something committed to this key between our snapshot and the timestamp
      // we are being pushed to. Committing above it would mean serialising
      // after a transaction we did not read -- which is the cycle.
      co_return false;
    }
  }
  co_return true;
}

// ---------------------------------------------------------------------------
// commit
// ---------------------------------------------------------------------------

Task<bool> Coordinator::put_record(Handle* handle, TxnStatus status, Ts commit_ts,
                                   bool with_keys) {
  shard::RangeDescriptor range;
  if (!store_->locate(handle->primary, &range)) co_return false;

  TxnCommand command;
  command.op = TxnOp::kPutRecord;
  command.key = handle->primary;
  command.txn = handle->id;
  command.epoch = handle->epoch;
  command.now = runtime_->now().physical;
  command.record.id = handle->id;
  command.record.epoch = handle->epoch;
  command.record.status = status;
  command.record.start_ts = handle->start_ts;
  command.record.commit_ts = commit_ts;
  command.record.heartbeat = runtime_->now().physical;
  command.record.ttl_nanos = static_cast<std::uint64_t>(options_.txn_ttl.nanos());
  // The primary always goes in first, regardless of with_keys: VersionStore
  // keys a record by keys.front() when it partitions a span for a split or a
  // merge (see store.cc), so this is the record's location, not just the
  // parallel-commit recovery list. A record created without it is invisible
  // to every span partition -- neither moved to a child range on a split nor
  // carried into a merge payload -- which strands it on whichever range
  // object it happened to be created on and loses it outright the day that
  // object is retired.
  command.record.keys.push_back(handle->primary);
  if (with_keys) {
    // The rest, in key order. The order is part of the contract: a recovering
    // reader walks this list, and a list whose order depended on a hash seed
    // would make recovery non-deterministic.
    for (const auto& [key, intent] : handle->writes) {
      if (key != handle->primary) command.record.keys.push_back(key);
    }
  }

  const Response response =
      co_await send(/*read=*/false, range, command, command.key, 0, 0, handle->id);
  if (!response.answered || response.status != shard::ShardStore::kOk) co_return false;
  co_return response.result.outcome == WriteOutcome::kOk;
}

Task<void> Coordinator::resolve_intents(Handle* handle, bool committed) {
  for (const auto& [key, intent] : handle->writes) {
    shard::RangeDescriptor range;
    if (!store_->locate(key, &range)) continue;
    TxnCommand command;
    command.op = committed ? TxnOp::kCommitIntent : TxnOp::kRollbackIntent;
    command.key = key;
    command.txn = handle->id;
    command.epoch = handle->epoch;
    command.commit_ts = handle->commit_ts;
    command.now = runtime_->now().physical;
    // Best effort, and that is not a shortcut: the primary record already
    // decided the outcome, so an intent left behind is a slow lookup for the
    // next reader rather than a wrong answer. Lazy resolution is the mechanism,
    // not a fallback.
    co_await send(/*read=*/false, range, command, key, 0, 0, handle->id);
  }
}

Task<TxnOutcome> Coordinator::commit(Handle* handle) {
  if (handle->writes.empty()) {
    // A read-only transaction has nothing to commit and nothing that could
    // fail. It does not touch a record, which is why read-only work costs no
    // writes at any of the three levels.
    live_.erase(handle->id);
    handle->committed = true;
    ++stats_.committed;
    co_return TxnOutcome::kCommitted;
  }

  // ---- the record ---------------------------------------------------------
  const TxnStatus initial =
      options_.parallel_commit ? TxnStatus::kStaging : TxnStatus::kPending;
  if (!co_await put_record(handle, initial, 0, /*with_keys=*/options_.parallel_commit)) {
    co_await resolve_intents(handle, /*committed=*/false);
    live_.erase(handle->id);
    ++stats_.aborted;
    co_return TxnOutcome::kAborted;
  }

  // ---- prewrite -----------------------------------------------------------
  //
  // The primary goes first by default. Note what has *already* happened above:
  // the record is written before this loop runs, unconditionally, so neither
  // order can produce an intent with no record to resolve it against. The
  // ordering here is conventional rather than load-bearing; see the long note
  // on `primary_first` in coordinator.h for why the knob is a drill control and
  // what actually carries the argument.
  std::vector<std::string> order;
  order.reserve(handle->writes.size());
  if (options_.primary_first) order.push_back(handle->primary);
  for (const auto& [key, intent] : handle->writes) {
    if (!options_.primary_first || key != handle->primary) order.push_back(key);
  }
  if (!options_.primary_first) {
    // The mutation: the primary is prewritten last, so the window in which
    // intents exist with no record is as wide as the whole prewrite phase.
    order.erase(std::remove(order.begin(), order.end(), handle->primary), order.end());
    order.push_back(handle->primary);
  }

  Ts pushed_to = 0;
  for (const std::string& key : order) {
    shard::RangeDescriptor range;
    if (!store_->locate(key, &range)) {
      co_await put_record(handle, TxnStatus::kAborted, 0, false);
      co_await resolve_intents(handle, false);
      live_.erase(handle->id);
      ++stats_.aborted;
      co_return TxnOutcome::kAborted;
    }

    TxnCommand command;
    command.op = TxnOp::kPrewrite;
    command.key = key;
    command.txn = handle->id;
    command.epoch = handle->epoch;
    command.start_ts = handle->start_ts;
    command.value = handle->writes[key].value;
    command.tombstone = handle->writes[key].tombstone;
    command.primary = handle->primary;
    command.now = runtime_->now().physical;

    const Response response =
        co_await send(/*read=*/false, range, command, key, 0, 0, handle->id);
    if (!response.answered) {
      // The prewrite may or may not have landed. Aborting is safe -- the record
      // is the truth and it says pending -- and it is the only answer available
      // without another round trip.
      co_await put_record(handle, TxnStatus::kAborted, 0, false);
      co_await resolve_intents(handle, false);
      live_.erase(handle->id);
      ++stats_.aborted;
      co_return TxnOutcome::kAborted;
    }
    if (response.status != shard::ShardStore::kOk ||
        response.result.outcome == WriteOutcome::kConflict ||
        response.result.outcome == WriteOutcome::kLocked ||
        response.result.outcome == WriteOutcome::kRejected) {
      if (response.result.outcome == WriteOutcome::kConflict) ++stats_.prewrite_conflicts;
      co_await put_record(handle, TxnStatus::kAborted, 0, false);
      co_await resolve_intents(handle, false);
      live_.erase(handle->id);
      ++stats_.aborted;
      co_return TxnOutcome::kAborted;
    }
    (void)pushed_to;
  }

  // ---- the commit timestamp ----------------------------------------------
  Ts commit_ts = 0;
  if (options_.level == Level::kStrictSerializable && options_.source == TsSource::kHybrid) {
    // The *top* of the uncertainty interval, not a point. Committing at the
    // reading itself and waiting out the bound would be equivalent; committing
    // at the top and waiting until the bottom has passed it is the same
    // statement written so that the wait is visible.
    commit_ts = clock_.interval(clock_.now(runtime_->now())).latest;
  } else if (!co_await next_timestamp(&commit_ts, /*fresh=*/true)) {
    co_await put_record(handle, TxnStatus::kAborted, 0, false);
    co_await resolve_intents(handle, false);
    live_.erase(handle->id);
    ++stats_.aborted;
    co_return TxnOutcome::kAborted;
  }
  if (commit_ts <= handle->start_ts) commit_ts = handle->start_ts + 1;

  // A transaction that somebody pushed forward has to prove that nothing it
  // read changed underneath, or restart. Snapshot isolation skips this by
  // definition -- write skew is legal there -- which is the one line that
  // separates the two levels.
  if (options_.level != Level::kSnapshot) {
    if (!co_await refresh_reads(handle, commit_ts)) {
      ++stats_.restarts_refresh_failed;
      co_await put_record(handle, TxnStatus::kAborted, 0, false);
      co_await resolve_intents(handle, false);
      live_.erase(handle->id);
      ++stats_.aborted;
      co_return TxnOutcome::kAborted;
    }
  }
  handle->commit_ts = commit_ts;

  // ---- the commit point ---------------------------------------------------
  if (options_.parallel_commit) {
    // Every intent is present and the record already lists them, so the
    // transaction is implicitly committed at this instant. The record is moved
    // to kCommitted for the benefit of readers who would otherwise have to
    // check every key, but the commit happened before this call.
    ++stats_.staging_commits;
  }
  if (!co_await put_record(handle, TxnStatus::kCommitted, commit_ts, false)) {
    // The commit point did not land, and this is the one place where the answer
    // is genuinely unknown: the record may have committed and the reply been
    // lost. Reporting either outcome would be a guess, and a client that treats
    // a guess as fact double-applies or loses a write (INV-TXN-15).
    ++stats_.unknown_outcomes;
    live_.erase(handle->id);
    co_return TxnOutcome::kUnknown;
  }

  // ---- commit-wait --------------------------------------------------------
  if (options_.level == Level::kStrictSerializable && options_.commit_wait &&
      options_.source == TsSource::kHybrid) {
    const Timestamp began = runtime_->now();
    // Wait until this node's clock can say, with the bound it declared, that
    // the commit timestamp is in the past. Only then may the client be told.
    // Without it the transaction is still serializable and no longer externally
    // consistent, which is a distinction only a real-time edge can see.
    for (std::uint32_t i = 0; i < 512; ++i) {
      if (clock_.interval(clock_.now(runtime_->now())).earliest > commit_ts) break;
      co_await runtime_->sleep_for(options_.poll);
    }
    ++stats_.commit_waits;
    stats_.commit_wait_nanos +=
        static_cast<std::uint64_t>(runtime_->now().physical - began.physical);
  }

  handle->committed = true;
  live_.erase(handle->id);
  ++stats_.committed;

  // Lazy resolution. The transaction is already committed; this only saves the
  // next reader a trip to the primary.
  co_await resolve_intents(handle, /*committed=*/true);
  co_return TxnOutcome::kCommitted;
}

Task<void> Coordinator::rollback(Handle* handle) {
  if (!handle->writes.empty()) {
    co_await put_record(handle, TxnStatus::kAborted, 0, false);
    co_await resolve_intents(handle, /*committed=*/false);
  }
  live_.erase(handle->id);
  ++stats_.aborted;
}

Task<void> Coordinator::heartbeat_all() {
  std::vector<Handle> handles;
  handles.reserve(live_.size());
  for (const auto& [id, handle] : live_) {
    if (!handle.primary.empty()) handles.push_back(handle);
  }
  // Snapshotted first: a heartbeat suspends, and a transaction that commits
  // inside that suspension erases itself from the map being iterated.
  for (Handle& handle : handles) {
    if (live_.count(handle.id) == 0) continue;
    co_await put_record(&handle, TxnStatus::kPending, 0, false);
  }
}

}  // namespace anvil::txn
