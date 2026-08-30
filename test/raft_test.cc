// Raft unit tests: the state machine, with no simulator underneath it.
//
// The whole reason raft.h is a pure state machine is that these tests can
// exist. "A candidate at term 5 whose log is two entries behind, receiving a
// vote request from a node with a longer log" is a sentence, not a seed hunt --
// and the situations that matter most in Raft are exactly the ones a random
// workload reaches once in ten thousand runs.
//
// The harness below is a deliberately dumb driver: it persists everything
// instantly, delivers every message, and never fails. That is the point. Fault
// injection happens in test/raft_faults.cc against the real driver; here the
// question is only whether the protocol is right when nothing goes wrong, and
// mixing the two makes both harder to read.

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "anvil/core/raft/config.h"
#include "anvil/core/raft/message.h"
#include "anvil/core/raft/raft.h"
#include "anvil/core/raft/storage.h"
#include "anvil/sim/simulation.h"

namespace {

using anvil::Duration;
using anvil::LogIndex;
using anvil::NodeId;
using anvil::Term;
using anvil::Timestamp;
namespace raft = anvil::raft;

int g_failures = 0;

void check(bool condition, std::string_view what) {
  if (!condition) {
    std::cerr << "FAIL: " << what << "\n";
    ++g_failures;
  }
}

// ---------------------------------------------------------------------------
// harness
// ---------------------------------------------------------------------------

class Harness {
 public:
  Harness(const std::vector<std::uint64_t>& voters,
          const std::vector<std::uint64_t>& learners = {},
          raft::RaftOptions options = raft::RaftOptions{})
      : options_(options) {
    raft::ConfState state;
    state.voters = voters;
    state.learners = learners;
    config_ = raft::Config::from_conf_state(state);
    for (const std::uint64_t id : voters) add(id);
    for (const std::uint64_t id : learners) add(id);
  }

  raft::RaftNode& node(std::uint64_t id) { return *nodes_.at(id); }
  const std::vector<std::string>& applied(std::uint64_t id) { return applied_[id]; }
  const std::vector<raft::ReadState>& reads(std::uint64_t id) { return reads_[id]; }
  Timestamp now() const { return now_; }

  // Partitions are the interesting fault, so they are the one the harness
  // models. Nodes in different groups cannot exchange messages; everything
  // else -- ticks, timers, local state -- carries on exactly as before, which
  // is what a network partition actually looks like from inside a process.
  void partition(const std::vector<std::vector<std::uint64_t>>& groups) {
    group_.clear();
    int next = 1;
    for (const auto& group : groups) {
      for (const std::uint64_t id : group) group_[id] = next;
      ++next;
    }
  }
  void heal() { group_.clear(); }

  void isolate(std::uint64_t id, bool isolated) {
    if (isolated) {
      group_[id] = --loner_;
    } else {
      group_.erase(id);
    }
  }

  // One tick on every node, then settle the resulting message storm.
  //
  // Isolated nodes tick too. A partitioned process does not stop running -- it
  // keeps timing out, keeps campaigning, and keeps believing whatever it
  // believed. Skipping their ticks would quietly disable CheckQuorum, pre-vote
  // and lease expiry, which are precisely the mechanisms a partition exists to
  // test.
  void tick(std::uint32_t count = 1) {
    for (std::uint32_t i = 0; i < count; ++i) {
      now_ = now_.advanced_by(options_.tick_interval);
      for (auto& [id, node] : nodes_) node->tick(now_);
      settle();
    }
  }

  // Drains every node's Ready and delivers every message, repeatedly, until
  // nothing is left. Bounded, because a protocol bug that produces an infinite
  // message loop should fail the test rather than hang it.
  void settle(int rounds = 40) {
    for (int r = 0; r < rounds; ++r) {
      bool progress = false;
      for (auto& [id, node] : nodes_) {
        if (pump(id)) progress = true;
      }
      std::vector<raft::RaftMessage> wire;
      wire.swap(wire_);
      for (const raft::RaftMessage& msg : wire) {
        if (!reachable(msg.from.value(), msg.to.value())) continue;
        const auto it = nodes_.find(msg.to.value());
        if (it == nodes_.end()) continue;
        // The message goes through its wire encoding on every hop. A codec bug
        // that only shows up on the sixth field is a codec bug this test should
        // find, not one the fault sweep should find a week later.
        raft::RaftMessage decoded;
        check(raft::decode_message(raft::encode_message(msg), &decoded),
              "every message must survive its own encoding");
        it->second->step(decoded, now_);
        progress = true;
      }
      if (!progress) return;
    }
  }

  std::uint64_t leader() const {
    for (const auto& [id, node] : nodes_) {
      if (node->role() == raft::Role::kLeader) return id;
    }
    return 0;
  }

  // The leader on one side of a partition. During a split there can legitimately
  // be two, in different terms, and asking for "the" leader would return
  // whichever the map iterated first.
  std::uint64_t leader_among(const std::vector<std::uint64_t>& ids) const {
    for (const std::uint64_t id : ids) {
      const auto it = nodes_.find(id);
      if (it != nodes_.end() && it->second->role() == raft::Role::kLeader) return id;
    }
    return 0;
  }

  std::size_t leaders_in_term(std::uint64_t term) const {
    std::size_t count = 0;
    for (const auto& [id, node] : nodes_) {
      if (node->role() == raft::Role::kLeader && node->term().value() == term) ++count;
    }
    return count;
  }

  // Ticks until a leader appears, or gives up. Returns the number of ticks.
  std::uint32_t elect(std::uint32_t limit = 400) {
    for (std::uint32_t i = 0; i < limit; ++i) {
      if (leader() != 0) return i;
      tick();
    }
    return limit;
  }

  bool propose(std::uint64_t leader_id, const std::string& command) {
    LogIndex assigned{};
    const bool ok =
        node(leader_id)
            .propose(raft::EntryType::kNormal, command, &assigned, now_)
            .is_ok();
    settle();
    return ok;
  }

  std::uint64_t messages_sent() const noexcept { return sent_; }

 private:
  bool reachable(std::uint64_t from, std::uint64_t to) const {
    const auto a = group_.find(from);
    const auto b = group_.find(to);
    const int ga = a == group_.end() ? 0 : a->second;
    const int gb = b == group_.end() ? 0 : b->second;
    return ga == gb;
  }

  void add(std::uint64_t id) {
    auto node = std::make_unique<raft::RaftNode>(NodeId{id}, options_,
                                                 anvil::DeterministicRandom{0x51EED000 + id});
    node->restore(raft::HardState{}, {}, raft::Snapshot{}, config_);
    nodes_.emplace(id, std::move(node));
  }

  // The dumb driver: persist instantly, apply, send.
  bool pump(std::uint64_t id) {
    raft::RaftNode& node = *nodes_.at(id);
    raft::Ready ready = node.ready(now_);
    if (ready.empty()) return false;
    for (const raft::LogEntry& entry : ready.committed) {
      if (entry.type == raft::EntryType::kNormal) applied_[id].push_back(entry.data);
    }
    for (const raft::RaftMessage& msg : ready.messages) {
      wire_.push_back(msg);
      ++sent_;
    }
    for (const raft::ReadState& read : ready.read_states) reads_[id].push_back(read);
    node.advance(ready, now_);
    return true;
  }

  raft::RaftOptions options_;
  raft::Config config_;
  std::map<std::uint64_t, std::unique_ptr<raft::RaftNode>> nodes_;
  std::map<std::uint64_t, std::vector<std::string>> applied_;
  std::map<std::uint64_t, std::vector<raft::ReadState>> reads_;
  std::vector<raft::RaftMessage> wire_;
  std::map<std::uint64_t, int> group_;
  int loner_ = 0;
  Timestamp now_;
  std::uint64_t sent_ = 0;
};

// ---------------------------------------------------------------------------
// quorum arithmetic
// ---------------------------------------------------------------------------

void test_quorum_math() {
  const raft::Config three = raft::Config::from_voters({1, 2, 3});
  check(three.has_quorum({1, 2}), "two of three is a quorum");
  check(!three.has_quorum({1}), "one of three is not");
  check(!three.has_quorum({}), "nobody is not a quorum");

  raft::ConfState joint_state;
  joint_state.voters = {3, 4, 5};
  joint_state.voters_outgoing = {1, 2, 3};
  const raft::Config joint = raft::Config::from_conf_state(joint_state);
  check(joint.joint(), "a configuration with an outgoing set is joint");
  check(!joint.has_quorum({3, 4, 5}), "a C_new majority alone is not a joint quorum");
  check(!joint.has_quorum({1, 2, 3}), "a C_old majority alone is not a joint quorum");
  check(joint.has_quorum({1, 2, 3, 4}), "both majorities together are");

  // INV-RAFT-15 in its smallest form.
  raft::ConfState with_learner;
  with_learner.voters = {1, 2, 3};
  with_learner.learners = {4, 5};
  const raft::Config learners = raft::Config::from_conf_state(with_learner);
  check(!learners.has_quorum({1, 4, 5}), "learners never make up the numbers");
  check(learners.has_quorum({1, 2, 4}), "voters still do");
  check(learners.is_learner(NodeId{4}), "a learner is not a voter");

  std::map<std::uint64_t, LogIndex> match;
  match[1] = LogIndex{10};
  match[2] = LogIndex{8};
  match[3] = LogIndex{4};
  check(learners.committed_index(match).value() == 8,
        "the committed index is the majority's, not the maximum");
  match[4] = LogIndex{99};
  check(learners.committed_index(match).value() == 8,
        "and a learner racing ahead does not move it");
  check(learners.committed_index(match, true).value() == 10,
        "counting the learner deliberately does -- which is the mutation");

  // ANV-0013, pinned. An even voter count needs a strict majority too, and the
  // off-by-one that gets it wrong is invisible on three and five nodes. Four
  // voters at 12/12/6/6 have a majority at 6, not at 12; the wrong index
  // commits an entry that half the cluster does not hold, and a later election
  // can then legitimately produce a leader without it. Every joint-consensus
  // transition passes through an even voter count, so this is not a corner.
  const raft::Config four = raft::Config::from_voters({1, 2, 3, 4});
  std::map<std::uint64_t, LogIndex> even;
  even[1] = LogIndex{12};
  even[2] = LogIndex{12};
  even[3] = LogIndex{6};
  even[4] = LogIndex{6};
  check(four.committed_index(even).value() == 6,
        "four voters: two of four is not a majority, so the commit index is 6 (got " +
            std::to_string(four.committed_index(even).value()) + ")");
  even[3] = LogIndex{12};
  check(four.committed_index(even).value() == 12, "three of four is");
  check(!four.has_quorum({1, 2}), "and has_quorum agrees: two of four is not a quorum");
  check(four.has_quorum({1, 2, 3}), "three of four is");

  const raft::Config six = raft::Config::from_voters({1, 2, 3, 4, 5, 6});
  std::map<std::uint64_t, LogIndex> wide;
  for (std::uint64_t id = 1; id <= 6; ++id) wide[id] = LogIndex{id <= 3 ? 40ULL : 10ULL};
  check(six.committed_index(wide).value() == 10,
        "six voters: three of six is not a majority either");
}

// ---------------------------------------------------------------------------
// elections
// ---------------------------------------------------------------------------

void test_election_produces_exactly_one_leader() {
  Harness cluster{{1, 2, 3}};
  const std::uint32_t ticks = cluster.elect();
  check(cluster.leader() != 0, "a three-node cluster must elect a leader");
  check(ticks < 400, "and it must not take the whole budget");

  const std::uint64_t term = cluster.node(cluster.leader()).term().value();
  check(cluster.leaders_in_term(term) == 1, "exactly one leader per term (INV-RAFT-01)");

  // The no-op. Without it the new leader could not commit anything from a
  // previous term, and the first client write after every election would block.
  const raft::RaftNode& leader = cluster.node(cluster.leader());
  check(leader.log().last_index().valid(), "a new leader appends its own-term entry");
  Term at_last{};
  check(leader.log().term_at(leader.log().last_index(), &at_last) && at_last == leader.term(),
        "and that entry is of its own term");
}

void test_pre_vote_stops_a_partitioned_node_disrupting() {
  raft::RaftOptions options;
  options.pre_vote = true;
  options.check_quorum = true;
  Harness cluster{{1, 2, 3, 4, 5}, {}, options};
  cluster.elect();
  const std::uint64_t leader = cluster.leader();
  check(leader != 0, "a five-node cluster elects a leader");
  const std::uint64_t leader_term = cluster.node(leader).term().value();

  // Isolate a follower and let it time out repeatedly. Under pre-vote it can
  // never win, so it must never raise its own term -- which is the entire
  // point: when the partition heals, it rejoins without forcing an election.
  std::uint64_t victim = 0;
  for (std::uint64_t id = 1; id <= 5; ++id) {
    if (id != leader) {
      victim = id;
      break;
    }
  }
  cluster.isolate(victim, true);
  const std::uint64_t victim_term_before = cluster.node(victim).term().value();
  cluster.tick(200);
  const std::uint64_t victim_term_after = cluster.node(victim).term().value();
  check(victim_term_after == victim_term_before,
        "pre-vote: an isolated node must not inflate its term");

  cluster.isolate(victim, false);
  cluster.tick(20);
  check(cluster.node(leader).role() == raft::Role::kLeader,
        "and the leader must survive the node rejoining");
  check(cluster.node(leader).term().value() == leader_term,
        "without a disruptive election");
}

void test_check_quorum_steps_a_partitioned_leader_down() {
  raft::RaftOptions options;
  options.check_quorum = true;
  Harness cluster{{1, 2, 3}, {}, options};
  cluster.elect();
  const std::uint64_t leader = cluster.leader();
  check(leader != 0, "elected");

  cluster.isolate(leader, true);
  cluster.tick(120);
  check(cluster.node(leader).role() != raft::Role::kLeader,
        "a leader that cannot see a quorum must step down (CheckQuorum)");
  check(!cluster.node(leader).lease_valid(cluster.now()),
        "and it must drop its lease when it does -- otherwise it keeps serving "
        "stale reads for the length of the partition");
}

void test_a_stale_log_cannot_win() {
  Harness cluster{{1, 2, 3}};
  cluster.elect();
  const std::uint64_t leader = cluster.leader();
  for (int i = 0; i < 5; ++i) cluster.propose(leader, "v" + std::to_string(i));

  // Pick a follower, rewind its log by hand, and let it campaign. The
  // up-to-date restriction must refuse it: a candidate with a short log winning
  // is how a committed entry gets overwritten (INV-RAFT-04).
  std::uint64_t behind = 0;
  for (std::uint64_t id = 1; id <= 3; ++id) {
    if (id != leader) {
      behind = id;
      break;
    }
  }
  raft::RaftNode& stale = cluster.node(behind);
  const LogIndex full = stale.log().last_index();
  check(full.value() >= 5, "the follower replicated the entries first");

  raft::RaftMessage vote;
  vote.type = raft::RaftMessageType::kRequestVote;
  vote.from = NodeId{behind};
  vote.to = NodeId{leader};
  vote.term = Term{stale.term().value() + 5};
  vote.prev_index = LogIndex{1};
  vote.prev_term = Term{1};
  cluster.node(leader).step(vote, cluster.now());

  raft::Ready ready = cluster.node(leader).ready(cluster.now());
  bool granted = false;
  for (const raft::RaftMessage& msg : ready.messages) {
    if (msg.type == raft::RaftMessageType::kRequestVoteReply && !msg.reject) granted = true;
  }
  check(!granted, "a candidate whose log is behind must not be granted a vote");
}

// ---------------------------------------------------------------------------
// replication
// ---------------------------------------------------------------------------

void test_replication_and_commit() {
  Harness cluster{{1, 2, 3}};
  cluster.elect();
  const std::uint64_t leader = cluster.leader();

  for (int i = 0; i < 10; ++i) check(cluster.propose(leader, "cmd" + std::to_string(i)), "propose");
  cluster.tick(5);

  for (std::uint64_t id = 1; id <= 3; ++id) {
    check(cluster.applied(id).size() == 10,
          "every node applies every committed command (n" + std::to_string(id) + " applied " +
              std::to_string(cluster.applied(id).size()) + ")");
  }
  for (std::uint64_t id = 2; id <= 3; ++id) {
    check(cluster.applied(id) == cluster.applied(1),
          "and in the same order -- INV-RAFT-05, State Machine Safety");
  }
}

void test_conflict_backtracking_is_by_term_not_by_index() {
  raft::RaftOptions options;
  options.max_entries_per_append = 4;
  Harness cluster{{1, 2, 3, 4, 5}, {}, options};
  cluster.elect();
  const std::uint64_t old_leader = cluster.leader();
  for (int i = 0; i < 5; ++i) cluster.propose(old_leader, "committed" + std::to_string(i));
  cluster.tick(3);

  // Split the old leader into a minority with one follower. It keeps accepting
  // proposals that can never commit, building a divergent tail at its own term
  // -- which is exactly how divergence happens in the field, and is very
  // different from entries nobody ever had.
  std::vector<std::uint64_t> minority{old_leader};
  std::vector<std::uint64_t> majority;
  for (std::uint64_t id = 1; id <= 5; ++id) {
    if (id == old_leader) continue;
    if (minority.size() < 2) {
      minority.push_back(id);
    } else {
      majority.push_back(id);
    }
  }
  cluster.partition({minority, majority});

  for (int i = 0; i < 30; ++i) cluster.propose(old_leader, "orphan" + std::to_string(i));
  cluster.tick(5);
  const LogIndex orphaned_to = cluster.node(old_leader).log().last_index();

  // The majority elects its own leader and commits different entries at the
  // same indices.
  cluster.tick(120);
  const std::uint64_t new_leader = cluster.leader_among(majority);
  check(new_leader != 0, "the majority side elects a leader of its own");
  if (new_leader == 0) return;
  for (int i = 0; i < 20; ++i) cluster.propose(new_leader, "real" + std::to_string(i));
  cluster.tick(5);

  const std::uint64_t before = cluster.messages_sent();
  cluster.heal();
  cluster.tick(60);
  const std::uint64_t used = cluster.messages_sent() - before;

  check(cluster.node(old_leader).role() != raft::Role::kLeader,
        "the stale leader steps down when the partition heals");
  check(cluster.applied(old_leader) == cluster.applied(new_leader),
        "and its divergent tail is replaced by the real log (got " +
            std::to_string(cluster.applied(old_leader).size()) + " vs " +
            std::to_string(cluster.applied(new_leader).size()) + " entries)");
  check(orphaned_to.value() > 30, "the divergent tail really was long");
  // One round trip per divergent *term*, not per divergent entry. The whole
  // 30-entry run shares a term, so repairing it must not cost 30 probes; the
  // budget here is loose on purpose, because what matters is the shape of the
  // growth, not the constant.
  check(used < 900, "repaired in a bounded number of messages (used " + std::to_string(used) +
                        ")");
}

// The log-consistency check, and the proof that it is load-bearing.
//
// The seeded-mutation drill turns `check_prev_term_on_append` off and the random
// sweep does not notice, which is exactly the situation CONTEXT.md section 8
// warns about: an uncaught mutation is either a test gap or an equivalent
// mutant, and reporting one as the other is the mistake. So the question gets
// answered directly here.
//
// It is not equivalent. `try_append` has two other guards -- entries must be
// contiguous with the log tail, and an entry at an existing index with a
// different term truncates -- and between them they mask the damage in most
// interleavings. What they cannot mask is a leader whose *previous* entry the
// follower does not have at that term, appending on top: with the check the
// follower rejects and the leader backtracks; without it the follower splices a
// foreign entry onto a log it never agreed with, and Log Matching is gone.
void test_append_requires_a_matching_previous_entry() {
  const raft::Config config = raft::Config::from_voters({1, 2, 3});

  const auto attempt = [&](bool check_prev) {
    raft::RaftOptions options;
    options.check_prev_term_on_append = check_prev;
    raft::RaftNode follower{NodeId{2}, options, anvil::DeterministicRandom{11}};

    std::vector<raft::LogEntry> existing;
    for (std::uint64_t i = 1; i <= 10; ++i) {
      raft::LogEntry entry;
      entry.term = Term{1};
      entry.index = LogIndex{i};
      entry.data = "mine";
      existing.push_back(entry);
    }
    follower.restore(raft::HardState{Term{5}, NodeId{}, LogIndex{0}}, existing,
                     raft::Snapshot{}, config);

    // A leader at the same term whose entry 10 is from term 4, not term 1. The
    // follower's log therefore does not match it, and entry 11 must not be
    // accepted on top.
    raft::RaftMessage append;
    append.type = raft::RaftMessageType::kAppend;
    append.from = NodeId{1};
    append.to = NodeId{2};
    append.term = Term{5};
    append.prev_index = LogIndex{10};
    append.prev_term = Term{4};
    raft::LogEntry foreign;
    foreign.term = Term{5};
    foreign.index = LogIndex{11};
    foreign.data = "theirs";
    append.entries.push_back(foreign);

    Timestamp now;
    follower.step(append, now);
    raft::Ready ready = follower.ready(now);
    bool rejected = false;
    for (const raft::RaftMessage& msg : ready.messages) {
      if (msg.type == raft::RaftMessageType::kAppendReply && msg.reject) rejected = true;
    }
    follower.advance(ready, now);
    return std::pair<bool, std::uint64_t>{rejected, follower.log().last_index().value()};
  };

  const auto [rejected, tail] = attempt(true);
  check(rejected, "an append whose previous entry does not match must be rejected");
  check(tail == 10, "and must not extend the log (tail is " + std::to_string(tail) + ")");

  const auto [mutant_rejected, mutant_tail] = attempt(false);
  check(!mutant_rejected && mutant_tail == 11,
        "without the check the follower splices the foreign entry on -- which is "
        "what makes the mutation non-equivalent, and what the random sweep misses");
}

// The Figure-8 hazard, constructed rather than waited for.
//
// A fresh leader that finds an entry from a previous term sitting on a quorum
// must NOT commit it. The entry being on a quorum today does not make it safe:
// a future leader elected from the nodes that lack it could still overwrite it,
// and if it had been reported committed, an acknowledged write would vanish.
// Only committing an entry of the leader's own term makes the earlier ones
// safe, which is why every leader appends a no-op on election.
//
// Reaching this state through a random workload takes a partition, a leader
// change and a crash inside a narrow window. Constructing it takes twelve
// lines, and the property is exactly as real.
void test_commit_is_restricted_to_the_current_term() {
  raft::RaftOptions options;
  const raft::Config config = raft::Config::from_voters({1, 2, 3});

  raft::RaftNode leader{NodeId{1}, options, anvil::DeterministicRandom{7}};
  std::vector<raft::LogEntry> inherited;
  for (std::uint64_t i = 1; i <= 5; ++i) {
    raft::LogEntry entry;
    entry.term = Term{1};
    entry.index = LogIndex{i};
    entry.type = raft::EntryType::kNormal;
    entry.data = "old";
    inherited.push_back(entry);
  }
  // Term 5, nothing committed, five entries from term 1 in the log.
  leader.restore(raft::HardState{Term{5}, NodeId{}, LogIndex{0}}, inherited, raft::Snapshot{},
                 config);

  Timestamp now;
  const auto pump = [&]() {
    raft::Ready ready = leader.ready(now);
    leader.advance(ready, now);
    return ready;
  };
  pump();

  leader.campaign_now(now);
  pump();
  for (std::uint64_t voter : {2ULL, 3ULL}) {
    raft::RaftMessage granted;
    granted.type = raft::RaftMessageType::kRequestVoteReply;
    granted.from = NodeId{voter};
    granted.to = NodeId{1};
    granted.term = leader.term();
    granted.reject = false;
    leader.step(granted, now);
  }
  pump();
  check(leader.role() == raft::Role::kLeader, "the node wins the election");
  check(leader.log().last_index().value() == 6, "and appends its own-term no-op at index 6");

  // Both followers report the *inherited* entries as replicated, and nothing
  // more. A quorum holds index 5. Committing it here is the bug.
  for (std::uint64_t voter : {2ULL, 3ULL}) {
    raft::RaftMessage ack;
    ack.type = raft::RaftMessageType::kAppendReply;
    ack.from = NodeId{voter};
    ack.to = NodeId{1};
    ack.term = leader.term();
    ack.match = LogIndex{5};
    leader.step(ack, now);
  }
  pump();
  check(leader.log().commit_index().value() == 0,
        "an entry from a previous term is not committed even on a quorum (INV-RAFT-10); "
        "commit index is " +
            std::to_string(leader.log().commit_index().value()));

  // Now one follower acknowledges the no-op. The leader's own term is
  // committed, and everything below it becomes committed with it.
  raft::RaftMessage ack;
  ack.type = raft::RaftMessageType::kAppendReply;
  ack.from = NodeId{2};
  ack.to = NodeId{1};
  ack.term = leader.term();
  ack.match = LogIndex{6};
  leader.step(ack, now);
  pump();
  check(leader.log().commit_index().value() == 6,
        "committing an own-term entry carries the earlier ones with it");
}

// ---------------------------------------------------------------------------
// membership
// ---------------------------------------------------------------------------

void test_joint_consensus_round_trip() {
  Harness cluster{{1, 2, 3}};
  cluster.elect();
  const std::uint64_t leader = cluster.leader();
  cluster.propose(leader, "before");
  cluster.tick(3);

  raft::ConfChange change;
  change.kind = raft::ConfChangeKind::kEnterJoint;
  change.voters = {1, 2, 3};
  change.learners = {};
  // A membership change that alters only the learners must not enter a joint
  // state at all; there is no quorum boundary to cross.
  check(cluster.node(leader).propose_conf_change(change, cluster.now()).is_ok(),
        "a no-op membership change is accepted");
  cluster.tick(5);
  check(!cluster.node(leader).config().joint(),
        "and does not leave the cluster in a joint configuration");

  // Now a real one: drop a node that is not the leader. Removing the leader is
  // legal and works, but it makes this test about self-removal rather than
  // about the joint transition.
  std::uint64_t victim = 0;
  for (std::uint64_t id = 1; id <= 3; ++id) {
    if (id != leader) {
      victim = id;
      break;
    }
  }
  raft::ConfChange shrink;
  shrink.kind = raft::ConfChangeKind::kEnterJoint;
  for (std::uint64_t id = 1; id <= 3; ++id) {
    if (id != victim) shrink.voters.push_back(id);
  }
  check(cluster.node(leader).propose_conf_change(shrink, cluster.now()).is_ok(),
        "a real membership change is accepted");

  // While the change is in flight -- appended, not yet applied -- a second one
  // must be refused. Queueing it would mean deciding what it means if the first
  // is reverted by a leader change, and there is no good answer to that.
  raft::ConfChange second;
  second.kind = raft::ConfChangeKind::kEnterJoint;
  second.voters = {1, 2, 3};
  check(!cluster.node(leader).propose_conf_change(second, cluster.now()).is_ok(),
        "a second membership change while one is in flight is refused");

  cluster.tick(12);
  check(!cluster.node(leader).config().joint(),
        "the transition completes and leaves the joint configuration");
  check(cluster.node(leader).config().incoming() ==
            std::set<std::uint64_t>(shrink.voters.begin(), shrink.voters.end()),
        "with the new membership in place, and " + std::to_string(victim) + " removed");
}

void test_learners_replicate_but_do_not_vote() {
  Harness cluster{{1, 2, 3}, {4}};
  cluster.elect();
  const std::uint64_t leader = cluster.leader();
  check(leader != 0 && leader != 4, "a learner is never elected");

  for (int i = 0; i < 5; ++i) cluster.propose(leader, "x" + std::to_string(i));
  cluster.tick(5);
  check(cluster.applied(4).size() == 5, "a learner receives and applies the log");

  // A learner that times out must not campaign.
  cluster.isolate(leader, true);
  const std::uint64_t learner_term = cluster.node(4).term().value();
  cluster.tick(80);
  check(cluster.node(4).term().value() == learner_term ||
            cluster.node(4).role() != raft::Role::kCandidate,
        "a learner never campaigns");
  check(cluster.node(4).role() != raft::Role::kLeader, "and never becomes leader");
}

// ---------------------------------------------------------------------------
// snapshots
// ---------------------------------------------------------------------------

void test_snapshot_install_catches_a_lagging_follower_up() {
  raft::RaftOptions options;
  options.snapshot_threshold = 8;
  options.snapshot_chunk_bytes = 16;  // several chunks, so flow control is exercised
  Harness cluster{{1, 2, 3}, {}, options};
  cluster.elect();
  const std::uint64_t leader = cluster.leader();
  std::uint64_t lagger = 0;
  for (std::uint64_t id = 1; id <= 3; ++id) {
    if (id != leader) {
      lagger = id;
      break;
    }
  }

  cluster.isolate(lagger, true);
  for (int i = 0; i < 30; ++i) cluster.propose(leader, "entry" + std::to_string(i));
  cluster.tick(5);

  // Compact the leader past the follower's tail. The harness plays the role of
  // the driver: build a snapshot, declare it durable, discard the prefix.
  raft::RaftNode& node = cluster.node(leader);
  raft::Snapshot snapshot;
  snapshot.index = node.log().applied_index();
  anvil::Term term{};
  check(node.log().term_at(snapshot.index, &term), "the applied index has a term");
  snapshot.term = term;
  snapshot.config = raft::encode_conf_state(node.config().to_conf_state());
  snapshot.data = std::string(200, 'z');  // big enough to need several chunks
  node.compacted(snapshot);
  check(node.log().snapshot_index() == snapshot.index, "the leader's log is compacted");

  cluster.isolate(lagger, false);
  cluster.tick(40);

  const raft::RaftNode& caught_up = cluster.node(lagger);
  check(caught_up.log().snapshot_index() >= snapshot.index,
        "the lagging follower is caught up by snapshot, not by log");
  check(caught_up.log().last_index() >= node.log().last_index(),
        "and then keeps up with the tail");
  check(caught_up.config().incoming() == node.config().incoming(),
        "the membership travels with the snapshot");
}

// ---------------------------------------------------------------------------
// leases and reads
// ---------------------------------------------------------------------------

void test_a_new_leader_has_no_lease_until_a_quorum_confirms() {
  Harness cluster{{1, 2, 3}};
  cluster.elect();
  const std::uint64_t leader = cluster.leader();
  check(cluster.node(leader).lease_valid(cluster.now()),
        "after a heartbeat round the leader holds a lease");

  // A leader elected in isolation holds nothing.
  Harness alone{{1, 2, 3}};
  alone.isolate(2, true);
  alone.isolate(3, true);
  alone.node(1).campaign_now(alone.now());
  alone.settle();
  check(!alone.node(1).lease_valid(alone.now()),
        "a candidate that cannot reach a quorum holds no lease");
}

void test_read_index_waits_for_a_quorum() {
  Harness cluster{{1, 2, 3}};
  cluster.elect();
  const std::uint64_t leader = cluster.leader();
  cluster.propose(leader, "value");
  cluster.tick(3);

  check(!cluster.node(leader).read_index(0, cluster.now()).is_ok(),
        "context zero is reserved");
  check(cluster.node(leader).read_index(77, cluster.now()).is_ok(), "a read is accepted");
  cluster.settle();
  bool served = false;
  LogIndex at{};
  for (const raft::ReadState& read : cluster.reads(leader)) {
    if (read.context == 77) {
      served = true;
      at = read.index;
    }
  }
  check(served, "and served once the leader can prove it is still the leader");
  check(at == cluster.node(leader).log().commit_index(),
        "at the commit index, which is the earliest point that is safe to read");

  // A follower must refuse: it cannot know whether it is behind.
  std::uint64_t follower = leader == 1 ? 2 : 1;
  check(!cluster.node(follower).read_index(78, cluster.now()).is_ok(),
        "a follower refuses a linearizable read rather than guessing");
}

void test_leadership_transfer() {
  Harness cluster{{1, 2, 3}};
  cluster.elect();
  const std::uint64_t leader = cluster.leader();
  for (int i = 0; i < 5; ++i) cluster.propose(leader, "before-transfer");
  cluster.tick(3);

  const std::uint64_t target = leader == 1 ? 2 : 1;
  check(cluster.node(leader).transfer_leadership(NodeId{target}, cluster.now()).is_ok(),
        "a transfer to a caught-up voter is accepted");
  cluster.settle();
  cluster.tick(10);
  check(cluster.leader() == target,
        "and the target takes over without waiting for an election timeout");
}

// ---------------------------------------------------------------------------
// storage
// ---------------------------------------------------------------------------

void test_codecs_round_trip() {
  raft::LogEntry entry;
  entry.term = Term{9};
  entry.index = LogIndex{4242};
  entry.type = raft::EntryType::kConfChange;
  entry.data = std::string("\0\1\2binary", 9);
  raft::LogEntry decoded;
  check(raft::decode_entry(raft::encode_entry(entry), &decoded) && decoded == entry,
        "log entries round-trip, including embedded NULs");

  raft::HardState hard{Term{17}, NodeId{3}, LogIndex{99}};
  raft::HardState hard_out;
  check(raft::decode_hard_state(raft::encode_hard_state(hard), &hard_out) && hard_out == hard,
        "hard state round-trips");

  raft::ConfState state;
  state.voters = {1, 2, 3};
  state.voters_outgoing = {1, 2};
  state.learners = {9};
  raft::ConfState state_out;
  check(raft::decode_conf_state(raft::encode_conf_state(state), &state_out) &&
            state_out == state,
        "conf state round-trips");

  raft::RaftMessage msg;
  msg.type = raft::RaftMessageType::kAppend;
  msg.from = NodeId{2};
  msg.to = NodeId{5};
  msg.term = Term{11};
  msg.prev_index = LogIndex{40};
  msg.prev_term = Term{10};
  msg.commit = LogIndex{38};
  msg.echo_time = 123456789;
  msg.force = true;
  msg.entries.push_back(entry);
  raft::RaftMessage msg_out;
  check(raft::decode_message(raft::encode_message(msg), &msg_out), "messages decode");
  check(msg_out.term == msg.term && msg_out.prev_index == msg.prev_index &&
            msg_out.echo_time == msg.echo_time && msg_out.force == msg.force &&
            msg_out.entries.size() == 1 && msg_out.entries[0] == entry,
        "and round-trip every field");

  // ANV-0012, pinned. A snapshot *reply* carries the acknowledged chunk offset
  // and no snapshot body. Encoding the flow-control fields inside the optional
  // snapshot block therefore dropped them on every reply, the leader read back
  // offset zero, and it shipped chunk one forever -- a follower that needed a
  // snapshot could never receive one. Invisible in any test that steps messages
  // without encoding them, which is why the harness encodes on every hop.
  raft::RaftMessage ack;
  ack.type = raft::RaftMessageType::kInstallSnapshotReply;
  ack.from = NodeId{3};
  ack.to = NodeId{1};
  ack.term = Term{4};
  ack.chunk_offset = 4096;
  ack.chunk_total = 65536;
  raft::RaftMessage ack_out;
  check(raft::decode_message(raft::encode_message(ack), &ack_out), "a snapshot reply decodes");
  check(ack_out.chunk_offset == 4096 && ack_out.chunk_total == 65536,
        "and keeps its flow-control fields even with no snapshot attached");

  // Truncated input must be refused, not read past.
  const std::string encoded = raft::encode_message(msg);
  for (std::size_t cut = 1; cut < encoded.size(); ++cut) {
    raft::RaftMessage partial;
    if (raft::decode_message(encoded.substr(0, cut), &partial)) {
      // Decoding a prefix is allowed only if it happens to be a complete
      // message; what is not allowed is reading past the end, which ASan would
      // catch. Nothing to assert beyond "it returned".
    }
  }
}

// A storage round trip through the real disk model: append, truncate the
// suffix, recover, and confirm the log is exactly what was left behind.
void test_storage_truncation_survives_recovery() {
  anvil::sim::SimConfig cfg;
  cfg.nodes = 1;
  cfg.faults = anvil::sim::FaultProfile::none();
  cfg.max_time = Duration::seconds(30);
  anvil::sim::Simulation sim{cfg};

  struct Outcome {
    bool ok = false;
    std::size_t entries_after_truncate = 0;
    std::uint64_t last_index = 0;
    std::uint64_t hard_term = 0;
  } outcome;

  anvil::Runtime& rt = sim.node(NodeId{1});
  rt.spawn([](anvil::Runtime& runtime, Outcome* out) -> anvil::Task<void> {
    raft::RaftStorage storage{&runtime, NodeId{1}, anvil::GroupId{1}, raft::RaftDurability{}};
    raft::RecoveredState recovered;
    if (!(co_await storage.recover(&recovered)).is_ok()) co_return;

    std::vector<raft::LogEntry> entries;
    for (std::uint64_t i = 1; i <= 12; ++i) {
      raft::LogEntry entry;
      entry.term = Term{i <= 6 ? 1ULL : 2ULL};
      entry.index = LogIndex{i};
      entry.data = "payload-" + std::to_string(i);
      entries.push_back(entry);
    }
    if (!(co_await storage.append(entries)).is_ok()) co_return;
    if (!(co_await storage.put_hard_state(raft::HardState{Term{2}, NodeId{1}, LogIndex{12}}))
             .is_ok()) {
      co_return;
    }
    if (!(co_await storage.sync()).is_ok()) co_return;

    // Truncate away everything from index 8 -- the case where a follower's log
    // diverged and the tail has to physically leave the file.
    if (!(co_await storage.truncate_suffix(LogIndex{8})).is_ok()) co_return;
    if (!(co_await storage.sync()).is_ok()) co_return;

    raft::RecoveredState after;
    if (!(co_await storage.recover(&after)).is_ok()) co_return;
    out->entries_after_truncate = after.entries.size();
    out->last_index = after.entries.empty() ? 0 : after.entries.back().index.value();
    out->hard_term = after.hard.term.value();
    out->ok = true;
    co_await storage.close();
  }(rt, &outcome));

  sim.run();
  check(outcome.ok, "the storage round trip completed");
  check(outcome.entries_after_truncate == 7,
        "a truncated suffix does not come back on recovery (got " +
            std::to_string(outcome.entries_after_truncate) + ")");
  check(outcome.last_index == 7, "and the log ends where it was cut");
  check(outcome.hard_term == 2, "while the hard state survives independently");
}

}  // namespace

int main() {
  std::cout << "raft unit tests\n";

  test_quorum_math();
  test_codecs_round_trip();

  test_election_produces_exactly_one_leader();
  test_pre_vote_stops_a_partitioned_node_disrupting();
  test_check_quorum_steps_a_partitioned_leader_down();
  test_a_stale_log_cannot_win();

  test_replication_and_commit();
  test_conflict_backtracking_is_by_term_not_by_index();
  test_append_requires_a_matching_previous_entry();
  test_commit_is_restricted_to_the_current_term();

  test_joint_consensus_round_trip();
  test_learners_replicate_but_do_not_vote();

  test_snapshot_install_catches_a_lagging_follower_up();

  test_a_new_leader_has_no_lease_until_a_quorum_confirms();
  test_read_index_waits_for_a_quorum();
  test_leadership_transfer();

  test_storage_truncation_survives_recovery();

  if (g_failures == 0) {
    std::cout << "raft unit tests: all checks passed\n";
    return EXIT_SUCCESS;
  }
  std::cerr << "raft unit tests: " << g_failures << " check(s) failed\n";
  return EXIT_FAILURE;
}
