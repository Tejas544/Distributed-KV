#include "anvil/core/raft/driver.h"

#include <utility>

#include "anvil/core/raft/message.h"

namespace anvil::raft {

StateMachine::~StateMachine() = default;

RaftDriver::RaftDriver(Runtime* runtime, RaftTransport* transport, GroupId group, NodeId self,
                       RaftOptions options, RaftDurability durability, Config bootstrap,
                       StateMachine* machine, DeterministicRandom rng)
    : runtime_(runtime),
      transport_(transport),
      group_(group),
      self_(self),
      options_(options),
      node_(self, options, rng),
      storage_(runtime, self, group, durability),
      machine_(machine),
      bootstrap_(std::move(bootstrap)) {}

RaftDriver::~RaftDriver() {
  // Deregistered here rather than by whoever destroys the driver, because a
  // merge destroys the subsumed group's driver from inside an apply and there
  // is no other moment at which the transport's routing table and the set of
  // live groups are guaranteed to agree.
  if (transport_ != nullptr) transport_->unregister_group(group_);
}

// ---------------------------------------------------------------------------
// boot
// ---------------------------------------------------------------------------

Task<void> RaftDriver::boot() {
  // Unbounded, deliberately. Any retry cap is a decision to give up and stay
  // dark forever, and there is no number that is right -- whatever cap you
  // pick, some seed's EIO burst is one longer. The run's deadline is the
  // termination condition, which is the honest one (ANV-0003).
  RecoveredState recovered;
  for (;;) {
    const Status status = co_await storage_.recover(&recovered);
    if (status.is_ok()) break;
    ++stats_.recover_retries;
    co_await runtime_->sleep_for(Duration::millis(20));
  }
  ++stats_.recoveries;
  if (recovered.log_truncated) ++stats_.log_truncations_on_recovery;
  stats_.last_truncate_reason = recovered.truncate_reason;
  stats_.recovered_entries = recovered.entries.size();
  stats_.recovered_snapshot_index = recovered.snapshot.index.value();
  stats_.recovered_hard_term = recovered.hard.term.value();
  stats_.recovered_hard_commit = recovered.hard.commit.value();
  if (recovered.state_truncated) ++stats_.state_truncations_on_recovery;
  if (recovered.snapshot_corrupt) ++stats_.snapshot_corruptions;
  if (recovery_callback_) {
    // Any loss of durable state is reported; whether it *excuses* anything is
    // decided by the caller, which knows whether the run injected a fault
    // capable of damaging already-synced bytes. Splitting it this way keeps the
    // two questions apart: this is "did I lose something", not "was it my
    // fault".
    recovery_callback_(recovered.log_truncated || recovered.state_truncated ||
                       recovered.snapshot_corrupt);
  }

  if (!recovered.snapshot.empty()) {
    machine_->restore(recovered.snapshot.data);
  }
  node_.restore(recovered.hard, std::move(recovered.entries), recovered.snapshot, bootstrap_);
  booted_ = true;

  // Registered only now. Before recovery finishes there is no state machine to
  // step, and a message stepped into a node that has not yet read its own
  // durable term is a node voting twice.
  transport_->register_group(group_, [this](Message envelope) -> Task<void> {
    ++stats_.messages_received;
    inbox_.push_back(std::move(envelope));
    co_await pump();
  });
  for (const NodeId peer : node_.config().members()) transport_->listen_to(peer);

  if (!external_ticks_) runtime_->spawn(tick_loop());
  co_await pump();
}

Task<void> RaftDriver::tick_loop() {
  for (;;) {
    co_await runtime_->sleep_for(options_.tick_interval);
    ++pending_ticks_;
    co_await pump();
  }
}

Task<void> RaftDriver::tick_now() {
  ++pending_ticks_;
  co_await pump();
}

// ---------------------------------------------------------------------------
// the loop that matters
// ---------------------------------------------------------------------------

Task<void> RaftDriver::pump() {
  if (busy_) {
    // Someone is already draining. Tell them there is more rather than
    // starting a second persist-then-send sequence that could interleave with
    // the first.
    again_ = true;
    co_return;
  }
  busy_ = true;
  do {
    again_ = false;

    // Step everything that has arrived, then produce exactly one batch from the
    // state that results. Between here and the end of apply_ready the state
    // machine is not touched by anyone.
    std::vector<Message> inbox;
    inbox.swap(inbox_);
    for (const Message& envelope : inbox) {
      RaftMessage msg;
      if (!from_transport(envelope, &msg)) continue;  // malformed; drop it
      node_.step(msg, runtime_->now());
    }
    const std::uint32_t ticks = pending_ticks_;
    pending_ticks_ = 0;
    for (std::uint32_t i = 0; i < ticks; ++i) node_.tick(runtime_->now());

    Ready ready = node_.ready(runtime_->now());
    if (ready.empty()) break;
    co_await apply_ready(ready);
  } while (again_ || !inbox_.empty() || pending_ticks_ > 0);
  busy_ = false;
}

Task<Status> RaftDriver::persist(Ready& ready) {
  // Set once the log file has been rewritten to match memory in full. Both the
  // snapshot-install path and the truncation path do that, and both therefore
  // cover the entries this batch would otherwise append -- appending them again
  // afterwards writes a second copy of indices the file already holds, which
  // recovery reads as a splice and truncates at.
  bool log_rewritten = false;

  if (ready.has_snapshot) {
    // The snapshot must be durable before the log that it supersedes is
    // discarded, and before the follower tells the leader it has installed it.
    // INV-RAFT-11 is exactly this ordering.
    Status status = co_await storage_.save_snapshot(ready.snapshot);
    if (!status.is_ok()) co_return status;
    machine_->restore(ready.snapshot.data);
    ++stats_.snapshots_installed;
    // Everything the snapshot covers is gone from the log; whatever the node
    // kept above it is rewritten from memory.
    status = co_await storage_.rewrite_log(
        node_.log().slice(node_.log().first_index(), UINT32_MAX, UINT64_MAX));
    if (!status.is_ok()) co_return status;
    ready.truncate = false;  // the rewrite subsumes it
    log_rewritten = true;
    stats_.entries_persisted += ready.entries.size();
    ++stats_.rewrites;
  }

  if (ready.truncate && !log_rewritten) {
    // A suffix truncation is a size change, and a size change is not crash-safe
    // in place: until it is durable the whole file is indeterminate, and the
    // region past the new end keeps whatever the old file had there. A crash in
    // that window brings the node back with the new prefix spliced onto the old
    // tail -- indices repeated, recovery stopping at the repeat, and fsynced
    // entries gone.
    //
    // So the file is rewritten atomically to exactly what memory holds, which
    // also covers the entries this batch would otherwise have appended. It costs
    // a full rewrite, and log divergence is rare enough that this is the right
    // trade: correctness now, a delta-based scheme later if it ever shows up in
    // a profile.
    const Status status = co_await storage_.rewrite_log(
        node_.log().slice(node_.log().first_index(), UINT32_MAX, UINT64_MAX));
    if (!status.is_ok()) co_return status;
    ++stats_.truncations;
    ++stats_.rewrites;
    log_rewritten = true;
    stats_.entries_persisted += ready.entries.size();
  } else if (!log_rewritten && !ready.entries.empty()) {
    const Status status = co_await storage_.append(ready.entries);
    if (!status.is_ok()) co_return status;
    stats_.entries_persisted += ready.entries.size();
  }
  if (ready.has_hard_state) {
    const Status status = co_await storage_.put_hard_state(ready.hard_state);
    if (!status.is_ok()) co_return status;
  }
  const Status status = co_await storage_.sync();
  if (status.is_ok()) {
    ++stats_.fsyncs;
    if (!ready.entries.empty() &&
        ready.entries.back().index.value() > stats_.fsynced_high) {
      stats_.fsynced_high = ready.entries.back().index.value();
    }
  }
  co_return status;
}

Task<void> RaftDriver::dispatch(const std::vector<RaftMessage>& messages) {
  for (const RaftMessage& msg : messages) {
    // The state machine does not know which group it is; it produces messages
    // for peers and lets the layer that owns the wire say so. Stamping it here
    // is the only place it can be stamped, and a message that leaves without a
    // group is one the receiver silently drops.
    RaftMessage stamped = msg;
    stamped.group = group_;
    transport_->listen_to(msg.to);
    const Status status = co_await transport_->send(stamped);
    if (status.is_ok()) {
      ++stats_.messages_sent;
    } else {
      // A failed send tells the sender nothing useful about what the peer has,
      // and Raft's answer to all of it is the same: the next heartbeat, the
      // next election timeout, the next probe. Retrying here would only add a
      // second, worse retry loop. The dead connection handle is dropped inside
      // the transport, which is where it now lives -- keeping one is how a node
      // stays reachable and answers nothing (ANV-0017, and the outbound half of
      // ANV-0032).
      ++stats_.send_failures;
    }
  }
}

Task<void> RaftDriver::apply_ready(Ready& ready) {
  ++stats_.ready_batches;

  if (options_.persist_before_reply) {
    const Status status = co_await persist(ready);
    if (!status.is_ok()) {
      // Nothing is acknowledged and nothing is advanced. The batch will be
      // regenerated on the next tick because the state machine still considers
      // those entries unpersisted; the messages in it are dropped, which is
      // indistinguishable from the network dropping them -- a case the protocol
      // already has to survive.
      ++stats_.storage_failures;
      co_return;
    }
    co_await dispatch(ready.messages);
  } else {
    // The seeded mutation. Replies leave before the vote or the entries are
    // durable, so a crash inside this window makes the node forget a promise it
    // has already made.
    co_await dispatch(ready.messages);
    const Status status = co_await persist(ready);
    if (!status.is_ok()) {
      ++stats_.storage_failures;
      co_return;
    }
  }

  for (const LogEntry& entry : ready.committed) {
    if (entry.type == EntryType::kNormal) {
      machine_->apply(entry.index, entry.data);
      ++stats_.applied;
    }
  }

  node_.advance(ready, runtime_->now());
  if (node_.log().persisted_index().value() > stats_.persisted_high) {
    stats_.persisted_high = node_.log().persisted_index().value();
  }

  if (read_callback_) {
    for (const ReadState& read : ready.read_states) read_callback_(read);
  }

  co_await maybe_compact();
}

Task<void> RaftDriver::maybe_compact() {
  if (!node_.wants_snapshot()) co_return;

  const LogIndex index = node_.log().applied_index();
  Term term{};
  if (!node_.log().term_at(index, &term)) co_return;

  Snapshot snapshot;
  snapshot.index = index;
  snapshot.term = term;
  // The membership travels with the snapshot. A node restored from one has no
  // conf-change entries left to replay, and a node that does not know its own
  // membership cannot compute a quorum.
  snapshot.config = encode_conf_state(node_.config().to_conf_state());
  snapshot.data = machine_->snapshot();

  // Durable first, then discard. The other order loses the log and the
  // snapshot together on the wrong crash.
  const Status status = co_await storage_.save_snapshot(snapshot);
  if (!status.is_ok()) {
    ++stats_.storage_failures;
    co_return;
  }
  node_.compacted(snapshot);
  ++stats_.rewrites;
  const Status rewrite = co_await storage_.rewrite_log(
      node_.log().slice(node_.log().first_index(), UINT32_MAX, UINT64_MAX));
  if (!rewrite.is_ok()) {
    ++stats_.storage_failures;
    co_return;
  }
  ++stats_.snapshots_taken;
}

// ---------------------------------------------------------------------------
// client entry points
// ---------------------------------------------------------------------------

Status RaftDriver::propose(std::string command, LogIndex* assigned) {
  if (!booted_) return Status{StatusCode::kUnavailable, "not booted"};
  const Status status =
      node_.propose(EntryType::kNormal, std::move(command), assigned, runtime_->now());
  if (status.is_ok()) runtime_->spawn(pump());
  return status;
}

Status RaftDriver::propose_conf_change(const ConfChange& change) {
  if (!booted_) return Status{StatusCode::kUnavailable, "not booted"};
  const Status status = node_.propose_conf_change(change, runtime_->now());
  if (status.is_ok()) runtime_->spawn(pump());
  return status;
}

Status RaftDriver::read_index(std::uint64_t context) {
  if (!booted_) return Status{StatusCode::kUnavailable, "not booted"};
  const Status status = node_.read_index(context, runtime_->now());
  if (status.is_ok()) runtime_->spawn(pump());
  return status;
}

Status RaftDriver::transfer_leadership(NodeId target) {
  if (!booted_) return Status{StatusCode::kUnavailable, "not booted"};
  const Status status = node_.transfer_leadership(target, runtime_->now());
  if (status.is_ok()) runtime_->spawn(pump());
  return status;
}

}  // namespace anvil::raft
