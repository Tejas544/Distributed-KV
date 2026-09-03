// P7 exit criterion 3, second half: trace validation.
//
//   "Export simulator traces in the spec's variable vocabulary and replay them
//    against the TLA+ spec to confirm the implementation refines it. This is
//    the step almost every 'we wrote a TLA+ spec' project omits, and it is the
//    one that means anything."
//
// Model checking a specification tells you the specification is sound. Testing
// an implementation tells you the implementation survived the schedules you
// tried. Neither says the two are related, and that gap is where a spec becomes
// decoration: it is entirely possible to have a verified specification and an
// implementation that does something else, and nothing in either activity would
// notice. Trace validation closes it by taking a run the implementation
// actually performed and asking TLC whether the specification permits it.
//
// This binary writes `spec/TraceLog.tla`; `spec/TraceRaft.tla` replays it and
// `tools/tlc.sh` grades the result.
//
// ---------------------------------------------------------------------------
// What is validated, stated precisely
// ---------------------------------------------------------------------------
//
// The subject is `anvil::raft::RaftNode` driven by test/raft_model.h -- the same
// model the state-space search explores, which is the shipping state machine
// with no simulator underneath it. It is *not* the full simulator: no transport,
// no disk model, no clock model. Mapping those onto the specification's message
// shapes is a larger job and the specification does not model them, so a trace
// that included them would have to project them away again.
//
// The implementation is configured to the specification's action set --
// pre_vote off, CheckQuorum off, one entry per append, no snapshots, fixed
// membership -- because a trace containing a pre-vote round is not a trace of
// this specification. That is a real limitation and it is the honest one to
// state: what is validated is Raft-without-pre-vote, which is the protocol the
// spec describes. Pre-vote only restricts when an election may start, so the
// implementation with it enabled produces a subset of these behaviours.
//
// ---------------------------------------------------------------------------
// Granularity, which is the whole engineering problem
// ---------------------------------------------------------------------------
//
// One implementation transition is one node consuming one input and running its
// `Ready` loop to completion. The specification's actions are finer: receiving a
// message at a higher term is `UpdateTerm` *and then* a handler; a candidate
// collecting its last vote is `HandleRequestVoteResponse` *and then*
// `BecomeLeader`; and every message the node then sends is its own action.
//
// So one implementation step is several specification steps, and a naive
// "assert Next holds between consecutive observed states" is simply false.
// Two halves to the answer:
//
//   1. This exporter decomposes the *sends*. After a transition it writes one
//      observed state for the node's new internal state, then one more for each
//      message the transition put on the wire, added in order. Each of those is
//      exactly one `AppendEntries` / `Heartbeat` / `RequestVote` action, and
//      that is a projection rather than an invention -- the implementation
//      really did produce those messages, in that order, from that state.
//
//   2. TraceRaft.tla allows a bounded number of *hidden* steps between observed
//      states, for the receive-side pairs that genuinely cannot be separated
//      (`UpdateTerm` then a handler, a handler then `AdvanceCommitIndex`, a
//      vote reply then `BecomeLeader`). The bound is a constant in the .cfg and
//      it is small -- three -- because with the sends already decomposed there
//      is nothing else left to hide.
//
// Without the first half the bound would need to be six or more, and TLC would
// be enumerating the whole successor set six times per checkpoint.

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "anvil/core/random.h"
#include "test/raft_model.h"

namespace {

using anvil::LogIndex;
using anvil::NodeId;
using anvil::Term;
using anvil::sim::Transition;
using anvil::testing::ModelConfig;
using anvil::testing::RaftModel;

namespace raft = anvil::raft;

// ---------------------------------------------------------------------------
// The specification's variables, mirrored
//
// Everything here exists in the implementation too, but three of them are
// private (`votesGranted`) or are history the implementation has no reason to
// keep (`elections`, `committedLog`, `commitRuleBroken`). They are maintained
// here, by the driver, from what it can see happening -- which is the same
// discipline the invariant checker follows: observe, never hook.
// ---------------------------------------------------------------------------

struct SpecMessage {
  std::string type;
  std::uint64_t term = 0;
  std::uint64_t source = 0;
  std::uint64_t dest = 0;
  // vote
  std::uint64_t last_term = 0;
  std::uint64_t last_index = 0;
  bool granted = false;
  // append
  std::uint64_t prev_index = 0;
  std::uint64_t prev_term = 0;
  std::uint64_t index = 0;      // 0 means "no entry", i.e. a heartbeat
  std::uint64_t entry_term = 0;
  std::uint64_t commit = 0;
  bool success = false;
  std::uint64_t match = 0;

  // Sorted, because `messages` is a *set* in the specification and a set has no
  // order. Two traces that differ only in the order two in-flight messages were
  // written down are the same trace.
  bool operator<(const SpecMessage& o) const {
    auto key = [](const SpecMessage& m) {
      return std::tuple(m.type, m.term, m.source, m.dest, m.last_term, m.last_index,
                        m.granted, m.prev_index, m.prev_term, m.index, m.entry_term,
                        m.commit, m.success, m.match);
    };
    return key(*this) < key(o);
  }
};

struct SpecElection {
  std::uint64_t term = 0;
  std::uint64_t leader = 0;
  std::vector<std::pair<std::uint64_t, std::string>> log;  // (term, value)
  std::set<std::uint64_t> voters;
};

struct SpecCommitted {
  std::uint64_t index = 0;
  std::uint64_t entry_term = 0;
  std::string value;
  std::uint64_t cterm = 0;

  bool operator<(const SpecCommitted& o) const {
    return std::tie(index, entry_term, value, cterm) <
           std::tie(o.index, o.entry_term, o.value, o.cterm);
  }
};

struct SpecState {
  std::vector<std::uint64_t> current_term;
  std::vector<std::string> role;
  std::vector<std::uint64_t> voted_for;  // 0 is Nil, i.e. "has not voted"
  std::vector<std::vector<std::pair<std::uint64_t, std::string>>> log;
  std::vector<std::uint64_t> commit_index;
  std::vector<std::set<std::uint64_t>> votes_granted;
  std::vector<SpecMessage> messages;
  std::vector<SpecElection> elections;
  std::set<SpecCommitted> committed;
  bool commit_rule_broken = false;
};

// ---------------------------------------------------------------------------
// TLA+ rendering
// ---------------------------------------------------------------------------

std::string fn(const std::vector<std::string>& values) {
  std::string out = "(";
  for (std::size_t i = 0; i < values.size(); ++i) {
    if (i != 0) out += " @@ ";
    out += std::to_string(i + 1) + " :> " + values[i];
  }
  return out + ")";
}

std::string quoted(const std::string& s) { return "\"" + s + "\""; }

std::string render_entry(const std::pair<std::uint64_t, std::string>& e) {
  return "[term |-> " + std::to_string(e.first) + ", kind |-> \"val\", value |-> " +
         quoted(e.second) + "]";
}

std::string render_log(const std::vector<std::pair<std::uint64_t, std::string>>& log) {
  std::string out = "<<";
  for (std::size_t i = 0; i < log.size(); ++i) {
    if (i != 0) out += ", ";
    out += render_entry(log[i]);
  }
  return out + ">>";
}

std::string render_set(const std::set<std::uint64_t>& s) {
  std::string out = "{";
  bool first = true;
  for (const std::uint64_t v : s) {
    if (!first) out += ", ";
    first = false;
    out += std::to_string(v);
  }
  return out + "}";
}

std::string render_message(const SpecMessage& m) {
  std::string out = "[mtype |-> " + quoted(m.type) + ", mterm |-> " + std::to_string(m.term) +
                    ", msource |-> " + std::to_string(m.source) + ", mdest |-> " +
                    std::to_string(m.dest);
  if (m.type == "RequestVoteRequest") {
    out += ", mlastTerm |-> " + std::to_string(m.last_term) + ", mlastIndex |-> " +
           std::to_string(m.last_index);
  } else if (m.type == "RequestVoteResponse") {
    out += std::string(", mgranted |-> ") + (m.granted ? "TRUE" : "FALSE");
  } else if (m.type == "AppendEntriesRequest") {
    out += ", mprevIndex |-> " + std::to_string(m.prev_index) + ", mprevTerm |-> " +
           std::to_string(m.prev_term) + ", mentry |-> [term |-> " +
           std::to_string(m.entry_term) + ", kind |-> \"val\", value |-> \"v\"], mindex |-> " +
           std::to_string(m.index) + ", mcommit |-> " + std::to_string(m.commit);
  } else {
    out += std::string(", msuccess |-> ") + (m.success ? "TRUE" : "FALSE") + ", mmatch |-> " +
           std::to_string(m.match);
  }
  return out + "]";
}

std::string render_messages(const std::vector<SpecMessage>& msgs) {
  std::vector<SpecMessage> sorted = msgs;
  std::sort(sorted.begin(), sorted.end());
  std::string out = "{";
  for (std::size_t i = 0; i < sorted.size(); ++i) {
    if (i != 0) out += ", ";
    out += render_message(sorted[i]);
  }
  return out + "}";
}

std::string render_elections(const std::vector<SpecElection>& es) {
  std::string out = "{";
  for (std::size_t i = 0; i < es.size(); ++i) {
    if (i != 0) out += ", ";
    out += "[eterm |-> " + std::to_string(es[i].term) + ", eleader |-> " +
           std::to_string(es[i].leader) + ", elog |-> " + render_log(es[i].log) +
           ", evoters |-> " + render_set(es[i].voters) + "]";
  }
  return out + "}";
}

std::string render_committed(const std::set<SpecCommitted>& cs) {
  std::string out = "{";
  bool first = true;
  for (const SpecCommitted& c : cs) {
    if (!first) out += ", ";
    first = false;
    out += "[cindex |-> " + std::to_string(c.index) + ", centry |-> " +
           render_entry({c.entry_term, c.value}) + ", cterm |-> " + std::to_string(c.cterm) + "]";
  }
  return out + "}";
}

std::string render_state(const SpecState& s) {
  std::vector<std::string> terms, roles, votes, logs, commits, granted;
  for (std::size_t i = 0; i < s.current_term.size(); ++i) {
    terms.push_back(std::to_string(s.current_term[i]));
    roles.push_back(quoted(s.role[i]));
    votes.push_back(s.voted_for[i] == 0 ? "0" : std::to_string(s.voted_for[i]));
    logs.push_back(render_log(s.log[i]));
    commits.push_back(std::to_string(s.commit_index[i]));
    granted.push_back(render_set(s.votes_granted[i]));
  }
  return "[currentTerm |-> " + fn(terms) + ",\n   state |-> " + fn(roles) +
         ",\n   votedFor |-> " + fn(votes) + ",\n   log |-> " + fn(logs) +
         ",\n   commitIndex |-> " + fn(commits) + ",\n   votesGranted |-> " + fn(granted) +
         ",\n   messages |-> " + render_messages(s.messages) + ",\n   elections |-> " +
         render_elections(s.elections) + ",\n   committedLog |-> " + render_committed(s.committed) +
         ",\n   commitRuleBroken |-> " + (s.commit_rule_broken ? "TRUE" : "FALSE") + "]";
}

// ---------------------------------------------------------------------------
// The driver
// ---------------------------------------------------------------------------

const char* role_name(raft::Role r) {
  switch (r) {
    case raft::Role::kLeader: return "Leader";
    case raft::Role::kCandidate: return "Candidate";
    default: return "Follower";
  }
}

SpecMessage project(const raft::RaftMessage& m) {
  SpecMessage out;
  out.term = m.term.value();
  out.source = m.from.value();
  out.dest = m.to.value();
  switch (m.type) {
    case raft::RaftMessageType::kRequestVote:
      out.type = "RequestVoteRequest";
      out.last_term = m.prev_term.value();
      out.last_index = m.prev_index.value();
      break;
    case raft::RaftMessageType::kRequestVoteReply:
      out.type = "RequestVoteResponse";
      out.granted = !m.reject;
      break;
    case raft::RaftMessageType::kAppend:
      out.type = "AppendEntriesRequest";
      out.prev_index = m.prev_index.value();
      out.prev_term = m.prev_term.value();
      out.commit = m.commit.value();
      if (!m.entries.empty()) {
        out.index = m.entries.front().index.value();
        out.entry_term = m.entries.front().term.value();
      }
      break;
    case raft::RaftMessageType::kHeartbeat:
      // The specification models a heartbeat as an AppendEntriesRequest with no
      // entry, which is exactly what it is.
      out.type = "AppendEntriesRequest";
      out.prev_index = m.prev_index.value();
      out.prev_term = m.prev_term.value();
      out.commit = m.commit.value();
      break;
    case raft::RaftMessageType::kAppendReply:
    case raft::RaftMessageType::kHeartbeatReply:
      out.type = "AppendEntriesResponse";
      out.success = !m.reject;
      out.match = m.match.value();
      break;
    default:
      out.type = "AppendEntriesResponse";
      break;
  }
  return out;
}

class Exporter {
 public:
  explicit Exporter(std::uint32_t nodes) : nodes_(nodes) {
    spec_.current_term.assign(nodes, 1);
    spec_.role.assign(nodes, "Follower");
    spec_.voted_for.assign(nodes, 0);
    spec_.log.assign(nodes, {});
    spec_.commit_index.assign(nodes, 0);
    spec_.votes_granted.assign(nodes, {});
    last_term_.assign(nodes, 1);
    last_commit_.assign(nodes, 0);
    last_role_.assign(nodes, "Follower");
  }

  // Reads everything the specification calls a variable out of the model, and
  // maintains the three the implementation does not keep.
  void observe(const RaftModel& model, const raft::RaftMessage* consumed) {
    const auto& st = model.state();
    for (std::uint32_t i = 0; i < nodes_; ++i) {
      const raft::RaftNode& n = st.nodes[i];
      const std::uint64_t term = n.term().value();

      // votesGranted: reset on a term change, seeded with the node's own vote
      // when it is campaigning. The implementation keeps this privately; the
      // driver reconstructs it from what it can see, which is a vote reply
      // arriving and a term moving.
      if (term != last_term_[i]) {
        // Campaigning always bumps the term, so "the term moved and this node
        // is a candidate" is exactly "it just started an election" -- including
        // a candidate that timed out again and campaigned at a *higher* term,
        // which is where the first version of this was wrong: it only seeded
        // the vote on the follower-to-candidate edge, so a re-campaigning
        // candidate lost its own vote and the replay stalled at that step.
        spec_.votes_granted[i].clear();
        if (n.role() == raft::Role::kCandidate) spec_.votes_granted[i] = {i + 1};
        last_term_[i] = term;
      }
      // ...and only if it was still campaigning when the vote arrived, which is
      // `last_role_` and not the role it holds now. The specification's
      // HandleRequestVoteResponse accumulates votes for a Candidate and does
      // nothing for anyone else, so a late vote credited to an established
      // leader produces a votesGranted the specification cannot reach. But the
      // *winning* vote is consumed by a node that is a Leader by the time this
      // observation runs -- BecomeLeader happened inside the same transition --
      // so testing the current role instead rejects exactly the vote that
      // matters, and every trace stalls at the first election.
      if (consumed != nullptr && consumed->to.value() == i + 1 &&
          consumed->type == raft::RaftMessageType::kRequestVoteReply && !consumed->reject &&
          consumed->term.value() == term && last_role_[i] == "Candidate") {
        spec_.votes_granted[i].insert(consumed->from.value());
      }

      spec_.current_term[i] = term;
      spec_.role[i] = role_name(n.role());
      spec_.voted_for[i] = n.vote().value();
      spec_.commit_index[i] = n.log().commit_index().value();

      // The log *before* this transition, kept because an election records it.
      const std::vector<std::pair<std::uint64_t, std::string>> log_before = spec_.log[i];
      spec_.log[i].clear();
      for (std::uint64_t k = 1; k <= n.log().last_index().value(); ++k) {
        const raft::LogEntry* e = n.log().at(LogIndex{k});
        if (e != nullptr) spec_.log[i].push_back({e->term.value(), "v"});
      }

      // elections: recorded the moment a node is first seen leading a term, and
      // with the log it held *at that moment* -- which is `log_before`, not the
      // log it has by the end of the transition.
      //
      // The difference is the no-op entry the implementation appends on
      // becoming leader, which is how a new leader gets an entry of its own
      // term to commit (and therefore how anything from a previous term becomes
      // committable at all, given the Figure-8 restriction). The specification
      // does not model that as part of BecomeLeader; in its vocabulary it is an
      // ordinary ClientRequest, taken as a separate step. So the election
      // records the pre-append log, the append shows up as the next step, and
      // the two line up. Recording the post-append log instead stalled the
      // replay here, and the stall is what pointed at it.
      if (n.role() == raft::Role::kLeader && !seen_leader_.count({i + 1, term})) {
        seen_leader_.insert({i + 1, term});
        SpecElection e;
        e.term = term;
        e.leader = i + 1;
        e.log = log_before;
        e.voters = spec_.votes_granted[i];
        spec_.elections.push_back(e);
      }

      // committedLog, and INV-RAFT-10 alongside it.
      const std::uint64_t commit = n.log().commit_index().value();
      if (n.role() == raft::Role::kLeader && commit > last_commit_[i]) {
        const raft::LogEntry* at = n.log().at(LogIndex{commit});
        if (at != nullptr && at->term.value() != term) spec_.commit_rule_broken = true;
      }
      // Only what this transition newly committed. The specification adds to
      // committedLog when a commit *decision* is made, so re-recording an index
      // that was already committed -- with whatever term the node happens to
      // hold now -- invents a decision nobody made. It showed up as a node that
      // merely learned of a higher term appearing to re-commit index 1 in it.
      for (std::uint64_t k = last_commit_[i] + 1; k <= commit; ++k) {
        const raft::LogEntry* e = n.log().at(LogIndex{k});
        if (e == nullptr) continue;
        SpecCommitted c;
        c.index = k;
        c.entry_term = e->term.value();
        c.value = "v";
        // The term the commit *decision* was made in: the leader's own term, or
        // for a follower the term of the leader whose message advanced it.
        c.cterm = n.role() == raft::Role::kLeader ? term : term;
        spec_.committed.insert(c);
      }
      last_commit_[i] = commit;
      last_role_[i] = spec_.role[i];
    }

    spec_.messages.clear();
    for (const auto& [link, queue] : st.wire) {
      for (const auto& wm : queue) spec_.messages.push_back(project(wm.msg));
    }
  }

  const SpecState& spec() const noexcept { return spec_; }

  // The observed state carrying `before`, plus every reply, plus the first
  // `keep` proactive sends. See `partition_sends` for why those are different.
  SpecState with_sends(const std::vector<SpecMessage>& before,
                       const std::vector<SpecMessage>& replies,
                       const std::vector<SpecMessage>& sends, std::size_t keep) const {
    SpecState out = spec_;
    std::vector<SpecMessage> kept = before;
    kept.insert(kept.end(), replies.begin(), replies.end());
    for (std::size_t i = 0; i < keep && i < sends.size(); ++i) kept.push_back(sends[i]);
    out.messages = kept;
    return out;
  }

  std::vector<SpecMessage> added_since(const std::vector<SpecMessage>& before) const {
    std::multiset<SpecMessage> pool(before.begin(), before.end());
    std::vector<SpecMessage> added;
    for (const SpecMessage& m : spec_.messages) {
      const auto it = pool.find(m);
      if (it != pool.end()) {
        pool.erase(it);
      } else {
        added.push_back(m);
      }
    }
    return added;
  }

 private:
  std::uint32_t nodes_;
  SpecState spec_;
  std::vector<std::uint64_t> last_term_;
  std::vector<std::uint64_t> last_commit_;
  std::vector<std::string> last_role_;
  std::set<std::pair<std::uint64_t, std::uint64_t>> seen_leader_;
};

}  // namespace

int main(int argc, char** argv) {
  const std::uint64_t seed = argc > 1 ? std::strtoull(argv[1], nullptr, 0) : 7;
  const std::size_t want = argc > 2 ? std::strtoull(argv[2], nullptr, 10) : 60;
  const std::string path = argc > 3 ? argv[3] : "spec/TraceLog.tla";
  const std::string mutation = argc > 4 ? argv[4] : "none";

  ModelConfig cfg;
  cfg.nodes = 3;
  cfg.tick_budget = 6;
  cfg.proposals = 1;
  cfg.max_in_flight = 64;  // no bound: this is one run, not a search

  // The specification's action set, exactly. Anything the spec does not model
  // would produce a trace it cannot replay, and that would be a limitation of
  // the mapping reported as a conformance failure -- the worst of both.
  cfg.options.pre_vote = false;
  cfg.options.check_quorum = false;
  cfg.options.election_timeout_ticks = 2;
  // No heartbeats, and this is the sharpest limitation of the whole exercise,
  // so it is a configured value with a paragraph rather than an omission.
  //
  // Both sides clamp the commit index a heartbeat carries, and they clamp it in
  // different places. The implementation clamps at the *sender*:
  // `broadcast_heartbeat` advertises `min(pr.match, commit_index)`, never
  // telling a follower to apply what it is not known to hold. The specification
  // clamps at the *receiver*: `HandleAppendEntries` takes
  // `min(mcommit, verified)`, where `verified` is the prefix that message
  // actually checked. Both are sound and they are not the same mechanism, so a
  // heartbeat crossing the boundary has an mprevIndex the other side does not
  // expect and the replay stalls on it.
  //
  // Reconciling them needs a matchIndex variable in the specification, which it
  // deliberately does not have -- AdvanceCommitIndex reads the logs directly,
  // which is one less variable to keep consistent. So heartbeats are out of
  // scope here and what is validated is elections, replication and the commit
  // rule. That is a real gap and it is worth more written down than papered
  // over: it says the two agree on *what* must hold and differ on *where* it is
  // enforced, which is exactly the kind of thing trace validation exists to
  // surface.
  cfg.options.heartbeat_timeout_ticks = 1'000'000;
  cfg.options.max_entries_per_append = 1;
  cfg.options.snapshot_threshold = 1'000'000;

  // The negative control. A conformance check that has only ever been observed
  // to pass is indistinguishable from one that always passes -- the same
  // argument as the hermeticity gate's negative control, and the reason
  // ANV-0005 is in the ledger. So the exporter can be asked for a trace of a
  // deliberately broken implementation, and the replay of *that* must stall.
  //
  //   vote      restrict_vote_by_log off: a voter grants to a candidate whose
  //             log is behind its own. The specification's HandleRequestVote
  //             will not, so the observed votedFor has no action that produces
  //             it. Chosen over the Figure-8 knob because it is reachable in a
  //             few dozen steps with three voters, where Figure-8 needs five.
  //   figure8   commit_only_current_term off. Kept because it is the sharper
  //             bug, with the honest note that three voters rarely reach it.
  if (mutation == "vote") {
    cfg.options.restrict_vote_by_log = false;
  } else if (mutation == "figure8") {
    cfg.options.commit_only_current_term = false;
  } else if (mutation != "none") {
    std::cerr << "unknown mutation: " << mutation << " (none|vote|figure8)\n";
    return 2;
  }

  RaftModel model{cfg};
  anvil::DeterministicRandom rng{seed};

  std::vector<SpecState> trace;
  // What the implementation did to produce each state. Documentary only -- it
  // is emitted as a TLA+ comment beside each state -- but a replay that stalls
  // is otherwise a number with no story attached, and this is the difference
  // between "stopped at 54" and "stopped where node 3 answered a heartbeat".
  std::vector<std::string> why;
  Exporter exporter{cfg.nodes};
  exporter.observe(model, nullptr);
  trace.push_back(exporter.spec());
  why.push_back("initial state");

  std::size_t steps = 0;
  while (trace.size() < want) {
    const std::vector<Transition> enabled = model.enabled();
    if (enabled.empty()) break;
    const Transition& t = enabled[rng.uniform(enabled.size())];

    const std::vector<SpecMessage> before_all = exporter.spec().messages;

    // By value, and this is not defensive style. `message_of` returns a pointer
    // into the link's deque, and `fire` pops the front of that deque -- so a
    // pointer taken before the transition dangles during the observation after
    // it. It reads plausible bytes rather than crashing, which is why the
    // symptom was a candidate that became leader with one vote instead of two
    // and a replay that stalled sixty steps later. CONTEXT.md gotcha 10.14 with
    // `fire()` in the place of the suspension point.
    const raft::RaftMessage* peek = model.message_of(t);
    const bool had_message = peek != nullptr;
    const raft::RaftMessage consumed_copy = had_message ? *peek : raft::RaftMessage{};
    const raft::RaftMessage* consumed = had_message ? &consumed_copy : nullptr;
    // The consumed message leaves the set as part of the receive action, so the
    // "before" the sends are added to is the set minus it.
    std::vector<SpecMessage> before;
    {
      bool dropped = false;
      const SpecMessage target = consumed != nullptr ? project(*consumed) : SpecMessage{};
      for (const SpecMessage& m : before_all) {
        if (!dropped && consumed != nullptr && !(m < target) && !(target < m)) {
          dropped = true;
          continue;
        }
        before.push_back(m);
      }
    }

    model.fire(t);
    ++steps;
    exporter.observe(model, consumed);

    // A reply is not a separate specification action: `HandleRequestVote` and
    // `HandleAppendEntries` consume the request and emit the response in one
    // step. Only *proactive* sends -- a fresh candidate's vote requests, a new
    // leader's appends -- are actions of their own. Splitting a reply out
    // produced an observed state with the request consumed and no response,
    // which the specification cannot produce, and the replay stalled there.
    const std::vector<SpecMessage> added = exporter.added_since(before);
    std::vector<SpecMessage> replies;
    std::vector<SpecMessage> sends;
    for (const SpecMessage& m : added) {
      const bool is_reply = consumed != nullptr && m.dest == consumed->from.value() &&
                            (m.type == "RequestVoteResponse" || m.type == "AppendEntriesResponse");
      (is_reply ? replies : sends).push_back(m);
    }

    // One observed state for the receive and its reply, then one per proactive
    // send.
    for (std::size_t k = 0; k <= sends.size() && trace.size() < want; ++k) {
      trace.push_back(exporter.with_sends(before, replies, sends, k));
      why.push_back(k == 0 ? t.name : t.name + "  [send " + std::to_string(k) + "]");
    }
  }

  std::ofstream out(path);
  if (!out) {
    std::cerr << "cannot write " << path << "\n";
    return 1;
  }
  out << "---------------------------- MODULE TraceLog ----------------------------\n"
      << "(***************************************************************************)\n"
      << "(* GENERATED by anvil_trace_export -- do not edit.                         *)\n"
      << "(*                                                                         *)\n"
      << "(* One run of anvil::raft::RaftNode, driven by test/raft_model.h, written   *)\n"
      << "(* in Raft.tla's variable vocabulary. spec/TraceRaft.tla replays it.        *)\n"
      << "(*                                                                         *)\n"
      // Line comments rather than more block-comment lines: every "(* ... *)"
      // above is already closed, so anything after them that is not itself a
      // comment is parsed as module body.
      << "\\* seed                 " << seed << "\n"
      << "\\* implementation steps " << steps << "\n"
      << "\\* observed states      " << trace.size() << "\n"
      // TLC, not just Naturals and Sequences: `:>` and `@@` are how a function
      // literal is written, and they live in the TLC standard module.
      << "EXTENDS Naturals, Sequences, TLC\n\n"
      << "Trace ==\n  << ";
  for (std::size_t i = 0; i < trace.size(); ++i) {
    if (i != 0) out << ",";
    out << "\n     \\* " << (i + 1) << ": " << why[i] << "\n     ";
    out << render_state(trace[i]);
  }
  out << " >>\n\n"
      << "=============================================================================\n";
  out.close();

  std::cout << "trace: seed " << seed << ", " << steps << " implementation steps, "
            << trace.size() << " observed states -> " << path << "\n";
  return 0;
}
