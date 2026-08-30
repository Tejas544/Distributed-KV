#include "anvil/core/raft/raft.h"

#include <algorithm>

#include "anvil/core/buggify.h"
#include "anvil/core/raft/message.h"

namespace anvil::raft {
namespace {

constexpr Term inc(Term t) noexcept { return Term{t.value() + 1}; }
constexpr LogIndex inc(LogIndex i) noexcept { return LogIndex{i.value() + 1}; }

bool is_vote_request(RaftMessageType type) noexcept {
  return type == RaftMessageType::kPreVote || type == RaftMessageType::kRequestVote;
}

bool is_leader_message(RaftMessageType type) noexcept {
  return type == RaftMessageType::kAppend || type == RaftMessageType::kHeartbeat ||
         type == RaftMessageType::kInstallSnapshot;
}

}  // namespace

RaftNode::RaftNode(NodeId self, RaftOptions options, DeterministicRandom rng)
    : self_(self), options_(options), rng_(rng) {
  reset_election_timer();
}

// ---------------------------------------------------------------------------
// lifecycle
// ---------------------------------------------------------------------------

void RaftNode::restore(const HardState& hard, std::vector<LogEntry> entries,
                       const Snapshot& snapshot, const Config& bootstrap) {
  term_ = hard.term;
  vote_ = hard.vote;
  persisted_hard_ = hard;
  hard_state_dirty_ = false;
  role_ = Role::kFollower;
  leader_ = NodeId{};
  lease_expiry_ = Timestamp{};
  lease_ticks_ = 0;

  log_ = RaftLog{};
  config_ = bootstrap;

  if (!snapshot.empty()) {
    snapshot_ = snapshot;
    log_.restore_snapshot(snapshot.index, snapshot.term);
    ConfState state;
    if (decode_conf_state(snapshot.config, &state)) config_ = Config::from_conf_state(state);
  }
  log_.append(entries);
  log_.mark_persisted(log_.last_index());
  log_.mark_writing(log_.last_index());
  // Nothing was truncated: this *is* what is on disk.
  log_.clear_pending_truncate();

  // The commit index is durable, so it is trusted -- but only as far as this
  // node's own log actually reaches. A node that crashed between "leader told
  // me commit=90" and "I received entry 90" recovers with a commit index ahead
  // of its log, and commit_to clamps rather than pretending.
  log_.commit_to(hard.commit);

  // Configuration is *derived*, never persisted separately: it is whatever the
  // committed conf-change entries say, and a separately persisted copy would be
  // a second source of truth that disagrees exactly when it matters.
  //
  // Deliberately NOT replayed here. Recovery leaves applied_ at the snapshot
  // index, so every committed conf-change entry above it comes back through
  // next_applicable() and is applied by advance() on the first pump, exactly as
  // it was the first time. An earlier version replayed them here as well, and
  // the result was every conf change above the snapshot being applied twice.
  //
  // Twice is not harmless. Applying kEnterJoint a second time sets the outgoing
  // set to the incoming one, they compare equal, and the joint state is dropped
  // -- so a node that restarts mid-transition skips straight to C_new while its
  // peers are still running C_old,new. Two nodes then compute quorums over sets
  // that need not intersect, which is the precise thing joint consensus exists
  // to prevent, and the visible symptom is an election that should have been
  // impossible.
  reset_election_timer();
  ++revision_;
}

// ---------------------------------------------------------------------------
// timers
// ---------------------------------------------------------------------------

void RaftNode::reset_election_timer() {
  election_elapsed_ = 0;
  // [T, 2T). Randomisation is the entire liveness argument for Raft elections:
  // synchronised timeouts produce split votes forever, and the failure is a
  // livelock rather than a crash, so nothing reports it.
  randomized_election_timeout_ =
      options_.election_timeout_ticks +
      static_cast<std::uint32_t>(rng_.uniform(options_.election_timeout_ticks));
}

void RaftNode::tick(Timestamp now) {
  ++revision_;
  if (role_ == Role::kLeader) {
    ++heartbeat_elapsed_;
    ++election_elapsed_;
    if (lease_ticks_ > 0) --lease_ticks_;

    if (leader_transferee_.valid()) {
      ++transfer_elapsed_;
      if (transfer_elapsed_ >= options_.election_timeout_ticks) {
        // The target never caught up. Abandon the transfer rather than
        // blocking proposals forever.
        leader_transferee_ = NodeId{};
        transfer_elapsed_ = 0;
      }
    }

    if (election_elapsed_ >= randomized_election_timeout_) {
      election_elapsed_ = 0;
      if (options_.check_quorum) check_quorum_or_step_down(now);
    }
    if (role_ == Role::kLeader && heartbeat_elapsed_ >= options_.heartbeat_timeout_ticks) {
      heartbeat_elapsed_ = 0;
      broadcast_heartbeat(0, now);
    }
    return;
  }

  ++election_elapsed_;
  if (promotable() && election_elapsed_ >= randomized_election_timeout_) {
    campaign(options_.pre_vote, false, now);
  }
}

// ---------------------------------------------------------------------------
// role transitions
// ---------------------------------------------------------------------------

void RaftNode::set_term(Term term, NodeId vote) {
  if (term_ != term || vote_ != vote) hard_state_dirty_ = true;
  term_ = term;
  vote_ = vote;
  votes_.clear();
}

void RaftNode::become_follower(Term term, NodeId leader) {
  if (term != term_) {
    set_term(term, NodeId{});
  }
  role_ = Role::kFollower;
  leader_ = leader;
  // Stepping down drops the lease immediately. A former leader that keeps
  // serving lease reads is the single most direct way to return stale data
  // under a linearizable mode (INV-RAFT-14).
  lease_expiry_ = Timestamp{};
  lease_ticks_ = 0;
  leader_transferee_ = NodeId{};
  pending_reads_.clear();
  reset_election_timer();
  ++revision_;
}

void RaftNode::campaign(bool pre_vote, bool force, Timestamp now) {
  if (!promotable()) return;  // learners and removed nodes never campaign

  reset_election_timer();
  votes_.clear();
  leader_ = NodeId{};

  Term campaign_term = term_;
  if (pre_vote) {
    role_ = Role::kPreCandidate;
    // Deliberately NOT incremented and NOT persisted. That is the whole point
    // of pre-vote: a node partitioned away from the cluster can ask whether it
    // would win without inflating the cluster's term when it comes back.
    campaign_term = inc(term_);
  } else {
    role_ = Role::kCandidate;
    set_term(inc(term_), self_);
    campaign_term = term_;
  }

  votes_[self_.value()] = true;
  if (config_.has_quorum(granted_votes())) {
    if (pre_vote) {
      campaign(false, force, now);
    } else {
      become_leader(now);
    }
    return;
  }

  for (const NodeId peer : config_.voters()) {
    if (peer == self_) continue;
    RaftMessage m;
    m.type = pre_vote ? RaftMessageType::kPreVote : RaftMessageType::kRequestVote;
    m.to = peer;
    m.term = campaign_term;
    m.prev_index = log_.last_index();
    m.prev_term = log_.last_term();
    m.force = force;
    send(std::move(m));
  }
  ++revision_;
}

void RaftNode::campaign_now(Timestamp now) { campaign(false, true, now); }

void RaftNode::become_pre_candidate(Timestamp now) { campaign(true, false, now); }
void RaftNode::become_candidate(Timestamp now) { campaign(false, false, now); }

void RaftNode::reset_progress(Timestamp now) {
  std::map<std::uint64_t, Progress> next;
  for (const NodeId member : config_.members()) {
    Progress pr;
    const auto existing = progress_.find(member.value());
    if (existing != progress_.end()) pr = existing->second;
    pr.next = inc(log_.last_index());
    pr.match = member == self_ ? log_.persisted_index() : LogIndex{};
    pr.state = ProgressState::kProbe;
    pr.inflight = 0;
    pr.snapshot_offset = 0;
    pr.recent_active = member == self_;
    pr.acked_send = member == self_ ? now : Timestamp{};
    pr.is_learner = config_.is_learner(member);
    next.emplace(member.value(), pr);
  }
  progress_ = std::move(next);
}

void RaftNode::become_leader(Timestamp now) {
  role_ = Role::kLeader;
  leader_ = self_;
  heartbeat_elapsed_ = 0;
  election_elapsed_ = 0;
  reset_progress(now);

  // No lease yet. A freshly elected leader has not confirmed anything with
  // anybody, and the previous leader's lease may still be running; claiming one
  // here is INV-RAFT-13 waiting to happen.
  lease_expiry_ = Timestamp{};
  lease_ticks_ = 0;

  // The no-op. Without it a leader cannot commit anything from a previous term
  // (the current-term restriction), so entries that are present on a quorum sit
  // uncommitted indefinitely and the first client read after an election blocks
  // for no reason a user could explain.
  LogEntry noop;
  noop.term = term_;
  noop.index = inc(log_.last_index());
  noop.type = EntryType::kNoop;
  log_.append({noop});

  // No membership change may be proposed until everything from previous terms
  // has been applied, because the configuration those entries imply is not
  // known yet.
  pending_conf_index_ = log_.last_index();

  broadcast_append(now);
  ++revision_;
}

// ---------------------------------------------------------------------------
// step
// ---------------------------------------------------------------------------

void RaftNode::step(const RaftMessage& msg, Timestamp now) {
  ++revision_;

  if (msg.term > term_) {
    if (is_vote_request(msg.type)) {
      // CheckQuorum's lease rule. A node that can still hear a leader refuses
      // to even consider a vote request, which is what stops a single
      // partitioned node from repeatedly disrupting a healthy cluster by
      // returning with a higher term. `force` is leadership transfer, which is
      // the leader's own instruction and overrides it.
      const bool in_lease = options_.check_quorum && leader_.valid() &&
                            election_elapsed_ < randomized_election_timeout_;
      if (in_lease && !msg.force) return;
    }

    if (msg.type == RaftMessageType::kPreVote) {
      // A pre-vote request carries the candidate's *prospective* term. Adopting
      // it would hand the disruption back to exactly the node pre-vote exists
      // to contain.
    } else if (msg.type == RaftMessageType::kPreVoteReply && !msg.reject) {
      // Our own prospective term coming back. Not a real term either.
    } else {
      become_follower(msg.term, is_leader_message(msg.type) ? msg.from : NodeId{});
    }
  } else if (msg.term < term_) {
    if ((options_.check_quorum || options_.pre_vote) &&
        (msg.type == RaftMessageType::kAppend || msg.type == RaftMessageType::kHeartbeat)) {
      // A stale leader is still sending. Reply with our term so it steps down
      // on the next round trip rather than after its own election timeout --
      // with CheckQuorum that difference is seconds of unavailability.
      RaftMessage reply;
      reply.type = RaftMessageType::kAppendReply;
      reply.to = msg.from;
      reply.term = term_;
      reply.reject = true;
      reply.match = log_.last_index();
      send(std::move(reply));
    } else if (msg.type == RaftMessageType::kPreVote) {
      // Must answer, and must answer with our term. Silence leaves the
      // pre-candidate looping until something else tells it the truth.
      RaftMessage reply;
      reply.type = RaftMessageType::kPreVoteReply;
      reply.to = msg.from;
      reply.term = term_;
      reply.reject = true;
      send(std::move(reply));
    }
    return;
  }

  switch (msg.type) {
    case RaftMessageType::kPreVote:
    case RaftMessageType::kRequestVote:
      step_vote_request(msg, now);
      break;
    case RaftMessageType::kPreVoteReply:
    case RaftMessageType::kRequestVoteReply:
      step_vote_reply(msg, now);
      break;
    case RaftMessageType::kAppend:
      step_append(msg, now);
      break;
    case RaftMessageType::kAppendReply:
      step_append_reply(msg, now);
      break;
    case RaftMessageType::kHeartbeat:
      step_heartbeat(msg, now);
      break;
    case RaftMessageType::kHeartbeatReply:
      step_heartbeat_reply(msg, now);
      break;
    case RaftMessageType::kInstallSnapshot:
      step_snapshot(msg, now);
      break;
    case RaftMessageType::kInstallSnapshotReply:
      step_snapshot_reply(msg, now);
      break;
    case RaftMessageType::kTimeoutNow:
      // The current leader is handing leadership over. Skip pre-vote and the
      // election timeout: the leader has already established that this node is
      // caught up, and the whole point is to make the gap short.
      if (promotable()) campaign(false, true, now);
      break;
  }
}

void RaftNode::step_vote_request(const RaftMessage& msg, Timestamp now) {
  const bool pre = msg.type == RaftMessageType::kPreVote;

  // Three ways a node may vote: it already voted for this candidate (an
  // idempotent retry, which matters because the reply may have been lost), it
  // has not voted and sees no leader, or this is a pre-vote for a future term
  // (which commits it to nothing).
  const bool can_vote = vote_ == msg.from || (!vote_.valid() && !leader_.valid()) ||
                        (pre && msg.term > term_);

  // The log restriction. A candidate whose log is behind must never win, or a
  // committed entry could be overwritten. This is the one check that makes
  // Leader Completeness (INV-RAFT-04) hold, and turning it off is a seeded
  // mutation whose damage is entirely invisible until a leader change.
  const bool up_to_date =
      !options_.restrict_vote_by_log || log_.is_up_to_date(msg.prev_index, msg.prev_term);

  RaftMessage reply;
  reply.type = pre ? RaftMessageType::kPreVoteReply : RaftMessageType::kRequestVoteReply;
  reply.to = msg.from;

  if (can_vote && up_to_date) {
    // A granted pre-vote is answered in the candidate's prospective term; a
    // granted real vote in ours, which is the same number.
    reply.term = msg.term;
    reply.reject = false;
    if (!pre) {
      // Recorded here, made durable by the driver before this reply is sent.
      // Reversing those two is the "vote not persisted before reply" mutation:
      // the voter crashes, forgets, votes again in the same term, and two
      // leaders exist with no client ever seeing anything odd.
      vote_ = msg.from;
      hard_state_dirty_ = true;
      reset_election_timer();
    }
  } else {
    reply.term = term_;
    reply.reject = true;
  }
  send(std::move(reply));
  (void)now;
}

void RaftNode::step_vote_reply(const RaftMessage& msg, Timestamp now) {
  const bool pre = msg.type == RaftMessageType::kPreVoteReply;
  if (pre && role_ != Role::kPreCandidate) return;
  if (!pre && role_ != Role::kCandidate) return;

  votes_[msg.from.value()] = !msg.reject;

  if (config_.has_quorum(granted_votes())) {
    if (pre) {
      campaign(false, false, now);
    } else {
      become_leader(now);
    }
    return;
  }
  if (config_.has_quorum(rejected_votes())) {
    // A quorum said no. Waiting for the election timeout would work, but
    // stepping down immediately shortens every split-vote round by a full
    // timeout and makes the liveness distribution far tighter.
    become_follower(term_, NodeId{});
  }
}

void RaftNode::step_append(const RaftMessage& msg, Timestamp now) {
  if (role_ != Role::kFollower) become_follower(msg.term, msg.from);
  leader_ = msg.from;
  reset_election_timer();

  LogIndex hint{};
  Term hint_term{};
  const bool ok = log_.try_append(msg.prev_index, msg.prev_term, msg.entries, &hint, &hint_term,
                                  options_.check_prev_term_on_append);

  RaftMessage reply;
  reply.type = RaftMessageType::kAppendReply;
  reply.to = msg.from;
  reply.term = term_;
  reply.echo_time = msg.echo_time;

  if (ok) {
    const LogIndex last_new{msg.prev_index.value() + msg.entries.size()};
    // Clamped to the entries this message actually established. Using the local
    // log tail instead would commit entries from a previous term that the
    // leader has not yet overwritten -- a real bug with a very long fuse.
    log_.commit_to(LogIndex{std::min(msg.commit.value(), last_new.value())});
    reply.reject = false;
    reply.match = last_new;

    // Applying a conf change on *append* rather than on commit is the
    // "joint consensus exits early" mutation. The correct path applies it in
    // advance(), from the committed batch.
    if (!options_.joint_requires_commit) {
      for (const LogEntry& e : msg.entries) {
        if (e.type == EntryType::kConfChange) apply_conf_change(e);
      }
    }
  } else {
    reply.reject = true;
    reply.reject_hint = hint;
    reply.reject_term = hint_term;
    reply.match = log_.last_index();
  }
  send(std::move(reply));
  (void)now;
}

void RaftNode::step_append_reply(const RaftMessage& msg, Timestamp now) {
  if (role_ != Role::kLeader) return;
  if (!config_.is_member(msg.from)) return;

  Progress& pr = progress_for(msg.from);
  pr.recent_active = true;
  pr.idle_rounds = 0;
  if (pr.inflight > 0) --pr.inflight;

  if (msg.reject) {
    // Backtrack by term, using the same search the follower used. The follower
    // reported the highest index at which its log could still agree; the leader
    // resolves that against its own, and the next probe goes just above it. One
    // round trip per divergent *term* rather than per divergent entry.
    LogIndex candidate = inc(msg.reject_hint);
    if (msg.reject_term.valid()) {
      candidate = inc(log_.find_conflict_by_term(msg.reject_hint, msg.reject_term));
    }
    if (candidate.value() == 0) candidate = LogIndex{1};
    // Never move next forward on a rejection; a stale reject must not undo
    // progress a later accept already established.
    pr.next = LogIndex{std::min(candidate.value(), pr.next.value())};
    if (pr.next.value() <= pr.match.value()) pr.next = inc(pr.match);
    if (pr.next.value() == 0) pr.next = LogIndex{1};
    pr.state = ProgressState::kProbe;
    pr.inflight = 0;
    send_append(msg.from, now);
    return;
  }

  if (msg.match > pr.match) pr.match = msg.match;
  if (pr.next <= pr.match) pr.next = inc(pr.match);
  pr.state = ProgressState::kReplicate;
  if (msg.echo_time > pr.acked_send.physical) pr.acked_send = Timestamp{msg.echo_time, 0};
  update_lease(now);

  if (maybe_advance_commit()) broadcast_append(now);
  maybe_finish_joint(now);
  resolve_pending_reads(now);

  if (leader_transferee_ == msg.from && pr.match == log_.last_index()) {
    RaftMessage m;
    m.type = RaftMessageType::kTimeoutNow;
    m.to = msg.from;
    m.term = term_;
    send(std::move(m));
  }
  send_append(msg.from, now);
}

void RaftNode::step_heartbeat(const RaftMessage& msg, Timestamp now) {
  if (role_ != Role::kFollower) become_follower(msg.term, msg.from);
  leader_ = msg.from;
  reset_election_timer();
  log_.commit_to(msg.commit);

  RaftMessage reply;
  reply.type = RaftMessageType::kHeartbeatReply;
  reply.to = msg.from;
  reply.term = term_;
  reply.read_context = msg.read_context;
  reply.echo_time = msg.echo_time;
  reply.match = log_.last_index();
  send(std::move(reply));
  (void)now;
}

void RaftNode::step_heartbeat_reply(const RaftMessage& msg, Timestamp now) {
  if (role_ != Role::kLeader) return;
  if (!config_.is_member(msg.from)) return;

  Progress& pr = progress_for(msg.from);
  pr.recent_active = true;
  pr.idle_rounds = 0;
  // A heartbeat reply frees one slot in the pipelining window. etcd does the
  // same thing for the same reason: it is the only signal that arrives when a
  // follower is reachable but every append in flight was lost.
  if (pr.inflight > 0) --pr.inflight;
  if (msg.echo_time > pr.acked_send.physical) pr.acked_send = Timestamp{msg.echo_time, 0};
  update_lease(now);

  if (msg.read_context != 0) {
    const auto it = pending_reads_.find(msg.read_context);
    if (it != pending_reads_.end()) {
      it->second.acks.insert(msg.from.value());
      it->second.acks.insert(self_.value());
      if (config_.has_quorum(it->second.acks)) {
        ready_reads_.push_back(ReadState{msg.read_context, it->second.index});
        pending_reads_.erase(it);
      }
    }
  }

  // Judged against the leader's own record of what this peer has acknowledged,
  // not against the tail the follower reports.
  //
  // Those two differ exactly when it matters. A follower with a divergent tail
  // reports a *longer* log than the leader has -- thirty uncommitted entries
  // from a term that lost -- so a comparison against its reported tail decides
  // it needs nothing, and the leader never sends the append that would have
  // truncated it. The follower then sits there, permanently divergent, while
  // the leader's heartbeats keep succeeding and everything looks healthy.
  if (pr.match < log_.last_index()) send_append(msg.from, now);
}

void RaftNode::step_snapshot(const RaftMessage& msg, Timestamp now) {
  if (role_ != Role::kFollower) become_follower(msg.term, msg.from);
  leader_ = msg.from;
  reset_election_timer();

  RaftMessage reply;
  reply.type = RaftMessageType::kInstallSnapshotReply;
  reply.to = msg.from;
  reply.term = term_;

  // A snapshot that is behind our commit index tells us nothing. Answer with
  // the index we already have so the leader fast-forwards instead of shipping
  // the whole thing again.
  if (msg.snapshot.index <= log_.commit_index()) {
    reply.match = log_.commit_index();
    send(std::move(reply));
    return;
  }

  const bool starting = msg.chunk_offset == 0;
  const bool same_image = has_incoming_snapshot_ &&
                          incoming_snapshot_.index == msg.snapshot.index &&
                          incoming_snapshot_.term == msg.snapshot.term;

  if (starting) {
    incoming_snapshot_ = msg.snapshot;
    incoming_snapshot_.data.clear();
    incoming_offset_ = 0;
    has_incoming_snapshot_ = true;
  } else if (!same_image || msg.chunk_offset != incoming_offset_) {
    // Out of order, or a chunk from a different snapshot. Ask for the offset we
    // actually need rather than stitching two images together -- the result
    // would pass every checksum and be nonsense.
    reply.reject = true;
    reply.chunk_offset = same_image ? incoming_offset_ : 0;
    send(std::move(reply));
    return;
  }

  incoming_snapshot_.data.append(msg.snapshot.data);
  incoming_offset_ = msg.chunk_offset + msg.snapshot.data.size();

  if (!msg.last_chunk) {
    reply.chunk_offset = incoming_offset_;
    send(std::move(reply));
    return;
  }

  // Complete. The log is replaced and the membership comes from the snapshot,
  // because there are no conf-change entries left to replay.
  if (options_.truncate_log_on_snapshot) {
    log_.restore_snapshot(incoming_snapshot_.index, incoming_snapshot_.term);
  } else {
    // The mutation: adopt the snapshot without discarding the log it
    // supersedes. Recovery then replays entries the snapshot already contains,
    // on top of it (INV-RAFT-11).
    log_.commit_to(incoming_snapshot_.index);
  }
  ConfState state;
  if (decode_conf_state(incoming_snapshot_.config, &state)) {
    config_ = Config::from_conf_state(state);
  }
  snapshot_ = incoming_snapshot_;
  pending_install_ = true;
  pending_install_snapshot_ = incoming_snapshot_;
  has_incoming_snapshot_ = false;
  incoming_offset_ = 0;

  reply.match = snapshot_.index;
  reply.chunk_offset = msg.chunk_offset + msg.snapshot.data.size();
  send(std::move(reply));
  (void)now;
}

void RaftNode::step_snapshot_reply(const RaftMessage& msg, Timestamp now) {
  if (role_ != Role::kLeader) return;
  if (!config_.is_member(msg.from)) return;

  Progress& pr = progress_for(msg.from);
  pr.recent_active = true;
  pr.idle_rounds = 0;
  pr.inflight = 0;

  if (msg.reject) {
    pr.snapshot_offset = msg.chunk_offset;
    send_snapshot_chunk(msg.from, now);
    return;
  }

  if (msg.match.valid()) {
    if (msg.match > pr.match) pr.match = msg.match;
    pr.next = inc(pr.match);
    pr.state = ProgressState::kProbe;
    pr.snapshot_offset = 0;
    if (maybe_advance_commit()) broadcast_append(now);
    send_append(msg.from, now);
    return;
  }

  pr.snapshot_offset = msg.chunk_offset;
  send_snapshot_chunk(msg.from, now);
}

// ---------------------------------------------------------------------------
// leader work
// ---------------------------------------------------------------------------

void RaftNode::broadcast_append(Timestamp now) {
  for (const NodeId peer : config_.members()) {
    if (peer == self_) continue;
    send_append(peer, now);
  }
}

void RaftNode::broadcast_heartbeat(std::uint64_t read_context, Timestamp now) {
  for (const NodeId peer : config_.members()) {
    if (peer == self_) continue;
    Progress& pr = progress_for(peer);

    // Nothing has come back for two heartbeat rounds, so whatever is "in
    // flight" is not coming. Free the window and retry from a known point.
    // Leaving it paused is not a stall that resolves itself: no reply means no
    // decrement, and no decrement means no send, forever.
    ++pr.idle_rounds;
    if (pr.idle_rounds >= 2) {
      pr.idle_rounds = 0;
      pr.inflight = 0;
      if (pr.state == ProgressState::kSnapshot) {
        send_snapshot_chunk(peer, now);
      } else {
        pr.state = ProgressState::kProbe;
        send_append(peer, now);
      }
    }

    RaftMessage m;
    m.type = RaftMessageType::kHeartbeat;
    m.to = peer;
    m.term = term_;
    // Never advertise a commit index beyond what this peer is known to hold;
    // it would tell the follower to apply entries it does not have.
    m.commit = LogIndex{std::min(pr.match.value(), log_.commit_index().value())};
    m.read_context = read_context;
    m.echo_time = now.physical;
    pr.pending_send = now;
    send(std::move(m));
  }
}

void RaftNode::send_append(NodeId peer, Timestamp now) {
  Progress& pr = progress_for(peer);

  if (pr.state == ProgressState::kSnapshot) return;  // one chunk in flight; wait

  // The peer needs entries the log no longer has.
  if (pr.next.value() <= log_.snapshot_index().value()) {
    if (snapshot_.empty()) return;  // nothing to ship yet; the next tick retries
    pr.state = ProgressState::kSnapshot;
    pr.snapshot_offset = 0;
    pr.snapshot_index = snapshot_.index;
    send_snapshot_chunk(peer, now);
    return;
  }

  if (pr.state == ProgressState::kProbe && pr.inflight > 0) return;
  if (pr.state == ProgressState::kReplicate && pr.inflight >= options_.max_inflight_appends) {
    return;
  }

  const LogIndex prev{pr.next.value() - 1};
  Term prev_term{};
  if (!log_.term_at(prev, &prev_term)) {
    if (snapshot_.empty()) return;
    pr.state = ProgressState::kSnapshot;
    pr.snapshot_offset = 0;
    pr.snapshot_index = snapshot_.index;
    send_snapshot_chunk(peer, now);
    return;
  }

  std::uint32_t batch = options_.max_entries_per_append;
  // The first BUGGIFY site in the core, and it earns its place.
  //
  // Sending a shorter batch is always legal -- the follower simply gets fewer
  // entries this round -- so this cannot make a correct implementation wrong.
  // What it does is manufacture *lagging followers*, and lag at the moment of an
  // election is the precondition for two of the nastiest bugs in Raft: the
  // Figure-8 commit hazard needs a follower whose match index sits below the new
  // leader's own-term entry, and a divergent append needs a follower far enough
  // behind for the leader to be probing at an index they disagree about.
  //
  // Both are reachable by random scheduling alone, at roughly one seed in
  // thousands. With this site they are reachable in tens. That is the entire
  // argument for BUGGIFY: rare interleavings need a site, not more seeds.
  if (ANVIL_BUGGIFY) batch = 1;

  std::vector<LogEntry> entries = log_.slice(pr.next, batch, options_.max_append_bytes);
  if (entries.empty() && pr.state == ProgressState::kReplicate) return;

  RaftMessage m;
  m.type = RaftMessageType::kAppend;
  m.to = peer;
  m.term = term_;
  m.prev_index = prev;
  m.prev_term = prev_term;
  m.commit = log_.commit_index();
  m.echo_time = now.physical;
  m.entries = entries;
  send(std::move(m));

  ++pr.inflight;
  pr.pending_send = now;
  if (pr.state == ProgressState::kReplicate && !entries.empty()) {
    // Optimistic pipelining: assume it lands, and keep sending. A rejection
    // rewinds `next`, which is why the rejection path must never move it
    // forward.
    pr.next = inc(entries.back().index);
  }
}

void RaftNode::send_snapshot_chunk(NodeId peer, Timestamp now) {
  Progress& pr = progress_for(peer);
  if (snapshot_.empty()) return;
  if (pr.snapshot_index != snapshot_.index) {
    // A newer snapshot replaced the one being shipped. Restart rather than
    // splicing two images.
    pr.snapshot_index = snapshot_.index;
    pr.snapshot_offset = 0;
  }

  const std::uint64_t total = snapshot_.data.size();
  const std::uint64_t offset = std::min(pr.snapshot_offset, total);
  const std::uint64_t remaining = total - offset;
  const std::uint64_t take =
      std::min<std::uint64_t>(remaining, options_.snapshot_chunk_bytes);

  RaftMessage m;
  m.type = RaftMessageType::kInstallSnapshot;
  m.to = peer;
  m.term = term_;
  m.snapshot.index = snapshot_.index;
  m.snapshot.term = snapshot_.term;
  m.snapshot.config = snapshot_.config;
  m.snapshot.data = snapshot_.data.substr(static_cast<std::size_t>(offset),
                                          static_cast<std::size_t>(take));
  m.chunk_offset = offset;
  m.chunk_total = total;
  m.last_chunk = offset + take >= total;
  m.echo_time = now.physical;
  send(std::move(m));

  pr.inflight = 1;
  pr.pending_send = now;
  pr.snapshot_offset = offset + take;
}

bool RaftNode::maybe_advance_commit() {
  if (role_ != Role::kLeader) return false;

  std::map<std::uint64_t, LogIndex> match;
  for (const auto& [id, pr] : progress_) match.emplace(id, pr.match);

  const LogIndex candidate =
      config_.committed_index(match, !options_.learners_excluded_from_quorum);
  if (candidate <= log_.commit_index()) return false;

  if (options_.commit_only_current_term) {
    // Figure 8. An entry from a previous term that happens to sit on a quorum
    // is NOT safe to commit: a future leader with a shorter log could still
    // overwrite it. Only an entry of the leader's own term may be committed
    // directly, and that commit carries every earlier entry with it.
    Term t{};
    if (!log_.term_at(candidate, &t) || t != term_) return false;
  }

  log_.commit_to(candidate);
  hard_state_dirty_ = true;
  return true;
}

void RaftNode::update_lease(Timestamp now) {
  if (role_ != Role::kLeader) return;

  if (!options_.lease_uses_wall_clock) {
    // The mutation: a lease counted in ticks. Correct-looking, and it survives
    // a process pause because ticks stop when the process does.
    lease_ticks_ = options_.election_timeout_ticks;
    return;
  }

  // Collect the send-time of the most recent message each voter has
  // acknowledged, then take the majority element. The result is the latest
  // instant T at which a quorum was demonstrably in touch, so the lease may
  // safely run to T + lease_duration - uncertainty.
  //
  // Using *send* times rather than receipt times is what makes the bound
  // conservative: the follower's promise not to vote starts no later than when
  // it received the message, which is no earlier than when we sent it.
  std::vector<std::uint64_t> times;
  for (const NodeId voter : config_.voters()) {
    if (voter == self_) {
      times.push_back(now.physical);
      continue;
    }
    const auto it = progress_.find(voter.value());
    times.push_back(it == progress_.end() ? 0 : it->second.acked_send.physical);
  }
  if (times.empty()) return;
  std::sort(times.begin(), times.end(), std::greater<std::uint64_t>{});
  const std::uint64_t quorum_time = times[(times.size() - 1) / 2];
  if (quorum_time == 0) return;

  const std::int64_t window =
      options_.lease_duration.nanos() - options_.max_clock_uncertainty.nanos();
  if (window <= 0) return;
  const Timestamp expiry{quorum_time + static_cast<std::uint64_t>(window), 0};
  if (expiry > lease_expiry_) lease_expiry_ = expiry;
}

bool RaftNode::lease_valid(Timestamp now) const {
  if (role_ != Role::kLeader) return false;

  // A lease is only sound if the clock uncertainty it has to absorb is smaller
  // than the lease itself, and if the whole thing fits inside the minimum
  // election timeout -- otherwise a successor can be elected while the previous
  // lease is still live, which is INV-RAFT-13.
  //
  // When the declared bound is too large for that, the honest answer is to have
  // no lease at all and pay for the ReadIndex round trip, which depends on no
  // clock whatsoever. Real systems make exactly this call; pretending the lease
  // is safe because it is configured is how a stale read ships.
  if (!lease_is_sound(options_)) return false;

  if (!options_.lease_uses_wall_clock) return lease_ticks_ > 0;
  return lease_expiry_.physical != 0 && now < lease_expiry_;
}

bool lease_is_sound(const RaftOptions& options) noexcept {
  if (options.lease_duration <= options.max_clock_uncertainty) return false;
  const Duration min_election =
      options.tick_interval * static_cast<std::int64_t>(options.election_timeout_ticks);
  return options.lease_duration + options.max_clock_uncertainty < min_election;
}

void RaftNode::check_quorum_or_step_down(Timestamp now) {
  std::set<std::uint64_t> active;
  active.insert(self_.value());
  for (const auto& [id, pr] : progress_) {
    if (pr.recent_active) active.insert(id);
  }
  if (!config_.has_quorum(active)) {
    // The leader cannot see a quorum. Stepping down is not just tidiness: a
    // leader that keeps its lease while partitioned is a leader that serves
    // stale reads for as long as the partition lasts.
    become_follower(term_, NodeId{});
    return;
  }
  for (auto& [id, pr] : progress_) pr.recent_active = (NodeId{id} == self_);
  (void)now;
}

void RaftNode::maybe_finish_joint(Timestamp now) {
  if (role_ != Role::kLeader) return;
  if (!config_.joint()) return;
  // config_ is only ever joint because the EnterJoint entry was applied, and
  // entries are applied only after they commit -- so reaching here means
  // C_old,new is committed and it is safe to propose the exit.
  if (pending_conf_index_ > log_.commit_index()) return;

  ConfChange leave;
  leave.kind = ConfChangeKind::kLeaveJoint;
  LogIndex assigned{};
  const std::string encoded = encode_conf_change(leave);
  // Propose directly rather than through propose_conf_change(), which refuses
  // while joint -- that guard is for clients, and this is the transition
  // completing itself.
  LogEntry entry;
  entry.term = term_;
  entry.index = inc(log_.last_index());
  entry.type = EntryType::kConfChange;
  entry.data = encoded;
  log_.append({entry});
  pending_conf_index_ = entry.index;
  assigned = entry.index;
  (void)assigned;
  broadcast_append(now);
}

void RaftNode::resolve_pending_reads(Timestamp now) {
  if (role_ != Role::kLeader) return;
  if (pending_reads_.empty()) return;

  Term t{};
  const bool own_term_committed =
      log_.term_at(log_.commit_index(), &t) && t == term_;
  if (!own_term_committed) return;

  std::vector<std::uint64_t> promoted;
  for (auto& [context, read] : pending_reads_) {
    if (!read.waiting_for_term_commit) continue;
    read.waiting_for_term_commit = false;
    read.index = log_.commit_index();
    promoted.push_back(context);
  }
  for (const std::uint64_t context : promoted) {
    const auto it = pending_reads_.find(context);
    if (it == pending_reads_.end()) continue;
    if (lease_valid(now)) {
      ready_reads_.push_back(ReadState{context, it->second.index});
      pending_reads_.erase(it);
    } else {
      it->second.acks.insert(self_.value());
      broadcast_heartbeat(context, now);
    }
  }
}

// ---------------------------------------------------------------------------
// proposals
// ---------------------------------------------------------------------------

Status RaftNode::propose(EntryType type, std::string data, LogIndex* assigned, Timestamp now) {
  if (role_ != Role::kLeader) return Status{StatusCode::kNotLeader, "not the leader"};
  if (leader_transferee_.valid()) {
    return Status{StatusCode::kUnavailable, "leadership transfer in progress"};
  }

  LogEntry entry;
  entry.term = term_;
  entry.index = inc(log_.last_index());
  entry.type = type;
  entry.data = std::move(data);
  log_.append({entry});
  if (assigned != nullptr) *assigned = entry.index;

  broadcast_append(now);
  ++revision_;
  return Status::ok();
}

Status RaftNode::propose_conf_change(const ConfChange& change, Timestamp now) {
  if (role_ != Role::kLeader) return Status{StatusCode::kNotLeader, "not the leader"};
  if (config_.joint()) {
    return Status{StatusCode::kUnavailable, "a membership change is already in progress"};
  }
  if (pending_conf_index_ > log_.applied_index()) {
    return Status{StatusCode::kUnavailable, "previous membership change not yet applied"};
  }
  if (change.kind != ConfChangeKind::kEnterJoint) {
    return Status{StatusCode::kInvalidArgument, "clients propose only kEnterJoint"};
  }
  if (change.voters.empty()) {
    return Status{StatusCode::kInvalidArgument, "a configuration needs at least one voter"};
  }

  LogIndex assigned{};
  const Status status =
      propose(EntryType::kConfChange, encode_conf_change(change), &assigned, now);
  if (status.is_ok()) pending_conf_index_ = assigned;
  return status;
}

Status RaftNode::read_index(std::uint64_t context, Timestamp now) {
  if (role_ != Role::kLeader) return Status{StatusCode::kNotLeader, "not the leader"};
  if (context == 0) return Status{StatusCode::kInvalidArgument, "context 0 is reserved"};

  // A leader's commit index is only meaningful once it has committed an entry
  // of its own term. Before that it may be behind a previous leader's, and
  // serving a read at it would return state older than an acknowledged write.
  Term t{};
  const bool own_term_committed = log_.term_at(log_.commit_index(), &t) && t == term_;

  PendingRead read;
  read.index = log_.commit_index();
  read.waiting_for_term_commit = !own_term_committed;

  if (!own_term_committed) {
    pending_reads_[context] = read;
    ++revision_;
    return Status::ok();
  }

  if (lease_valid(now)) {
    // The lease read. No round trip: the leader knows no other leader can have
    // been elected before its lease expires.
    ready_reads_.push_back(ReadState{context, read.index});
    ++revision_;
    return Status::ok();
  }

  read.acks.insert(self_.value());
  if (config_.has_quorum(read.acks)) {
    ready_reads_.push_back(ReadState{context, read.index});
    ++revision_;
    return Status::ok();
  }
  pending_reads_[context] = read;
  broadcast_heartbeat(context, now);
  ++revision_;
  return Status::ok();
}

Status RaftNode::transfer_leadership(NodeId target, Timestamp now) {
  if (role_ != Role::kLeader) return Status{StatusCode::kNotLeader, "not the leader"};
  if (target == self_) return Status::ok();
  if (!config_.is_voter(target)) {
    return Status{StatusCode::kInvalidArgument, "target is not a voter"};
  }
  leader_transferee_ = target;
  transfer_elapsed_ = 0;

  Progress& pr = progress_for(target);
  if (pr.match == log_.last_index()) {
    RaftMessage m;
    m.type = RaftMessageType::kTimeoutNow;
    m.to = target;
    m.term = term_;
    send(std::move(m));
  } else {
    send_append(target, now);
  }
  ++revision_;
  return Status::ok();
}

// ---------------------------------------------------------------------------
// ready / advance
// ---------------------------------------------------------------------------

Ready RaftNode::ready(Timestamp now) {
  Ready out;
  out.entries = log_.unstable();
  // Handing entries out is the moment they start reaching the file, which is
  // what a later truncation has to know about. See RaftLog::mark_writing.
  if (!out.entries.empty()) log_.mark_writing(out.entries.back().index);
  out.truncate = log_.has_pending_truncate();
  out.truncate_from = log_.pending_truncate_from();
  out.committed = log_.next_applicable();
  out.messages = std::move(outbox_);
  outbox_.clear();
  out.read_states = std::move(ready_reads_);
  ready_reads_.clear();

  if (pending_install_) {
    out.has_snapshot = true;
    out.snapshot = pending_install_snapshot_;
  }

  const HardState current{term_, vote_, log_.commit_index()};
  if (hard_state_dirty_ || !(current == persisted_hard_)) {
    out.has_hard_state = true;
    out.hard_state = current;
  }
  (void)now;
  return out;
}

void RaftNode::advance(const Ready& r, Timestamp now) {
  if (r.truncate) log_.clear_pending_truncate();
  if (!r.entries.empty()) log_.mark_persisted(r.entries.back().index);
  if (r.has_snapshot) pending_install_ = false;
  if (r.has_hard_state) {
    persisted_hard_ = r.hard_state;
    hard_state_dirty_ = false;
  }

  if (!r.committed.empty()) {
    for (const LogEntry& e : r.committed) {
      // The correct path: a configuration change takes effect when it commits.
      // Under the mutation it was already applied on append, and applying it
      // twice would be wrong, so it is skipped here.
      if (e.type == EntryType::kConfChange && options_.joint_requires_commit) {
        apply_conf_change(e);
      }
    }
    log_.apply_to(r.committed.back().index);
  }

  if (role_ == Role::kLeader) {
    Progress& self_pr = progress_for(self_);
    // The leader counts itself only for what its own disk has accepted. This
    // one line is INV-RAFT-09: "durably fsynced on a quorum" includes the
    // leader, and a leader that counts an entry it has merely buffered turns a
    // single crash into a lost acknowledged write.
    if (log_.persisted_index() > self_pr.match) self_pr.match = log_.persisted_index();
    self_pr.next = inc(self_pr.match);
    self_pr.acked_send = now;
    if (maybe_advance_commit()) broadcast_append(now);
    maybe_finish_joint(now);
    resolve_pending_reads(now);
  }
  ++revision_;
}

// ---------------------------------------------------------------------------
// compaction
// ---------------------------------------------------------------------------

bool RaftNode::wants_snapshot() const {
  const std::uint64_t applied = log_.applied_index().value();
  const std::uint64_t base = log_.snapshot_index().value();
  return applied > base && applied - base >= options_.snapshot_threshold;
}

void RaftNode::compacted(const Snapshot& snapshot) {
  snapshot_ = snapshot;
  log_.compact(snapshot.index, snapshot.term);
  ++revision_;
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

void RaftNode::send(RaftMessage msg) {
  msg.from = self_;
  outbox_.push_back(std::move(msg));
}

void RaftNode::apply_conf_change(const LogEntry& entry) {
  ConfChange change;
  if (!decode_conf_change(entry.data, &change)) return;
  config_ = config_.apply(change);
  if (entry.index >= pending_conf_index_) pending_conf_index_ = LogIndex{};

  // Rebuild the progress table around the new membership. Departed nodes are
  // dropped so they stop being counted; arrivals start at the log tail and get
  // probed down, which is the same path a lagging follower takes.
  std::map<std::uint64_t, Progress> next;
  for (const NodeId member : config_.members()) {
    const auto it = progress_.find(member.value());
    Progress pr = it == progress_.end() ? Progress{} : it->second;
    if (it == progress_.end()) {
      pr.next = inc(log_.last_index());
      pr.match = LogIndex{};
      pr.state = ProgressState::kProbe;
    }
    pr.is_learner = config_.is_learner(member);
    next.emplace(member.value(), pr);
  }
  progress_ = std::move(next);

  // A leader that has removed itself steps down once the change is applied.
  // Continuing to lead a configuration it is not part of is how a removed node
  // keeps a cluster hostage.
  if (role_ == Role::kLeader && !config_.is_voter(self_)) {
    become_follower(term_, NodeId{});
  }
  ++revision_;
}

bool RaftNode::promotable() const {
  return config_.is_voter(self_);
}

std::set<std::uint64_t> RaftNode::granted_votes() const {
  std::set<std::uint64_t> out;
  for (const auto& [id, granted] : votes_) {
    if (granted) out.insert(id);
  }
  return out;
}

std::set<std::uint64_t> RaftNode::rejected_votes() const {
  std::set<std::uint64_t> out;
  for (const auto& [id, granted] : votes_) {
    if (!granted) out.insert(id);
  }
  return out;
}

Progress& RaftNode::progress_for(NodeId peer) {
  const auto it = progress_.find(peer.value());
  if (it != progress_.end()) return it->second;
  Progress pr;
  pr.next = inc(log_.last_index());
  pr.is_learner = config_.is_learner(peer);
  return progress_.emplace(peer.value(), pr).first->second;
}

std::string RaftNode::describe() const {
  std::string out = "n" + std::to_string(self_.value());
  out += " ";
  out += to_string(role_);
  out += " term=" + std::to_string(term_.value());
  out += " vote=" + std::to_string(vote_.value());
  out += " last=" + std::to_string(log_.last_index().value());
  out += "@" + std::to_string(log_.last_term().value());
  out += " commit=" + std::to_string(log_.commit_index().value());
  out += " applied=" + std::to_string(log_.applied_index().value());
  out += " snap=" + std::to_string(log_.snapshot_index().value());
  out += " " + config_.describe();
  return out;
}

}  // namespace anvil::raft
