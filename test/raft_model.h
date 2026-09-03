// The Raft cluster as an explorable/observable model, shared by the two things
// that drive the real state machine with no simulator underneath it:
//
//   test/dpor_test.cc      systematic exploration of every interleaving
//   test/trace_export.cc   one interleaving at a time, written out in the TLA+
//                          specification's variable vocabulary
//
// It lives in a header because those two want the *same* model. A second copy
// would drift, and the whole value of trace validation is that the thing being
// replayed against the specification is the thing the state-space search
// explored -- and both are `anvil::raft::RaftNode` itself, unmodified.
//
// One transition is one node consuming one input -- a delivery, a tick, or a
// proposal -- and running its `Ready` loop to completion. That is the correct
// granularity for the implementation (nothing may step the state machine while
// a persist is in flight, CONTEXT.md gotcha 10.8) and it is *coarser* than the
// specification's, which is the fact TraceRaft.tla is built around.

#ifndef ANVIL_TEST_RAFT_MODEL_H_
#define ANVIL_TEST_RAFT_MODEL_H_

#include <algorithm>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "anvil/core/raft/config.h"
#include "anvil/core/raft/raft.h"
#include "anvil/core/raft/types.h"
#include "anvil/sim/dpor.h"

namespace anvil::testing {

using anvil::Digest;
using anvil::LogIndex;
using anvil::NodeId;
using anvil::Term;
using anvil::Timestamp;
using anvil::sim::Transition;

namespace raft = anvil::raft;

// ---------------------------------------------------------------------------
// the model
// ---------------------------------------------------------------------------

struct ModelConfig {
  std::uint32_t nodes = 3;
  std::uint32_t tick_budget = 5;   // per node
  std::uint32_t proposals = 2;     // in total, and only at a leader
  std::size_t max_in_flight = 6;
  raft::RaftOptions options;

  // A constructed starting point, for the configurations whose interesting
  // region is too deep to reach from an empty cluster inside a budget a search
  // can finish. Empty means "start from nothing", which is the default.
  std::function<void(struct ModelState&)> prepare;
};

// Label spaces, kept disjoint so a transition's identity is stable and readable.
constexpr std::uint64_t kTickLabel = 1ull << 40;
constexpr std::uint64_t kProposeLabel = 1ull << 41;

// An ordered pair of node ids. One queue per link, rather than one global list
// of messages, and the reason is the independence relation rather than tidiness.
//
// A single list with globally numbered messages does not commute: node 1
// sending and node 2 sending, in either order, produce the same set of messages
// carrying different numbers in a different list order -- so "transitions of
// different nodes are independent" is false, and a partial-order reduction
// resting on it prunes real states. Which is what it did, until the exhaustive
// search caught it (ANV-0064). Per-link queues with per-link sequence numbers
// fix it at the root: a node appends only to links it is the source of,
// consumes only from links it is the destination of, and appending to the tail
// of a deque commutes with popping its head.
using Link = std::pair<std::uint64_t, std::uint64_t>;  // (from, to)

struct WireMessage {
  std::uint64_t seq = 0;  // per link, so it does not depend on global ordering
  raft::RaftMessage msg;
};

struct ModelState {
  std::vector<raft::RaftNode> nodes;  // index i is NodeId{i + 1}
  std::vector<Timestamp> now;
  std::vector<std::uint32_t> ticks_used;
  std::vector<std::vector<std::string>> applied;
  std::vector<LogIndex> max_commit;
  std::map<Link, std::deque<WireMessage>> wire;
  std::map<Link, std::uint64_t> next_seq;

  // Per node, for the same reason the message numbering is per link: one shared
  // counter makes "node 1 proposes" and "node 2 proposes" disagree about which
  // value each of them wrote depending on the order they ran in.
  std::vector<std::uint32_t> proposals_used;

  // Branch-local history. Both are monotone along a path and both are derivable
  // from what the nodes hold, so carrying them costs almost no state-space
  // dedup -- and without them leader completeness and vote uniqueness are not
  // expressible as predicates over the current state at all.
  std::map<std::uint64_t, std::pair<std::uint64_t, std::string>> committed;  // index -> (term, data)
  std::map<std::pair<std::uint64_t, std::uint64_t>, std::uint64_t> votes;    // (node, term) -> vote

  // Where each node's commit index stood when it most recently became leader.
  // INV-RAFT-10 is about a leader *advancing* onto a previous term's entry;
  // inheriting a commit index that already points at one is legal and happens
  // at every election, so without this the check would fire constantly.
  std::vector<std::uint64_t> leader_term;
  std::vector<LogIndex> commit_at_election;
};

class RaftModel : public anvil::sim::ExplorableSystem {
 public:
  explicit RaftModel(ModelConfig config) : config_(std::move(config)) { reset(); }

  void reset() override {
    state_ = ModelState{};
    for (std::uint32_t i = 0; i < config_.nodes; ++i) {
      // A distinct substream per node, exactly as the simulator does it. The
      // election timeout randomisation draws from it, so two nodes with the
      // same stream would time out together forever.
      state_.nodes.emplace_back(NodeId{i + 1}, config_.options,
                                anvil::DeterministicRandom{0x0D9002000ull + i});
      state_.nodes.back().restore(raft::HardState{}, {}, raft::Snapshot{}, voters());
      state_.now.emplace_back();
      state_.ticks_used.push_back(0);
      state_.applied.emplace_back();
      state_.max_commit.emplace_back();
      state_.leader_term.push_back(0);
      state_.commit_at_election.emplace_back();
      state_.proposals_used.push_back(0);
    }
    if (config_.prepare) config_.prepare(state_);
    for (std::uint32_t i = 0; i < config_.nodes; ++i) pump(i);
    observe();
  }

  std::vector<Transition> enabled() const override {
    std::vector<Transition> out;

    // Deliveries, FIFO per ordered pair: only the oldest message on each link
    // is deliverable, because that is what a TCP stream offers.
    for (const auto& [link, queue] : state_.wire) {
      if (queue.empty()) continue;
      Transition t;
      t.actor = static_cast<std::uint32_t>(link.second);
      t.label = delivery_label(link, queue.front().seq);
      t.name = "deliver " + std::to_string(link.first) + "->" + std::to_string(link.second) +
               "#" + std::to_string(queue.front().seq) + " " + kind_name(queue.front().msg.type);
      out.push_back(std::move(t));
    }

    for (std::uint32_t i = 0; i < config_.nodes; ++i) {
      if (state_.ticks_used[i] < config_.tick_budget) {
        Transition t;
        t.actor = i + 1;
        t.label = kTickLabel + state_.ticks_used[i];
        t.name = "tick(" + std::to_string(i + 1) + ")";
        out.push_back(std::move(t));
      }
      if (state_.proposals_used[i] < config_.proposals &&
          state_.nodes[i].role() == raft::Role::kLeader) {
        Transition t;
        t.actor = i + 1;
        t.label = kProposeLabel + state_.proposals_used[i];
        t.name = "propose(" + std::to_string(i + 1) + "," + value_of(i) + ")";
        out.push_back(std::move(t));
      }
    }

    std::sort(out.begin(), out.end());
    return out;
  }

  void fire(const Transition& t) override {
    const std::uint32_t index = t.actor - 1;
    if (t.label >= kProposeLabel) {
      LogIndex assigned{};
      state_.nodes[index].propose(raft::EntryType::kNormal, value_of(index), &assigned,
                                  state_.now[index]);
      ++state_.proposals_used[index];
    } else if (t.label >= kTickLabel) {
      state_.now[index] = state_.now[index].advanced_by(config_.options.tick_interval);
      ++state_.ticks_used[index];
      state_.nodes[index].tick(state_.now[index]);
    } else {
      const auto it = state_.wire.find(link_of(t.label));
      if (it == state_.wire.end() || it->second.empty()) return;  // cannot happen
      const raft::RaftMessage msg = it->second.front().msg;
      it->second.pop_front();
      if (it->second.empty()) state_.wire.erase(it);
      state_.nodes[index].step(msg, state_.now[index]);
    }
    pump(index);
    observe();
  }

  Digest fingerprint() const override {
    Digest d;
    for (std::uint32_t i = 0; i < config_.nodes; ++i) {
      // Everything that decides what this node does next, including the private
      // timers and the generator position. The hand-rolled version of this that
      // preceded it used the public accessors only, and merged states that
      // differ in the leader's replication progress -- see ANV-0063, which is
      // in the ledger because the reduced search found a terminal state the
      // "exhaustive" one could not reach.
      d.mix(state_.nodes[i].state_digest().high()).mix(state_.nodes[i].state_digest().low());
      d.mix(state_.ticks_used[i]).mix(state_.leader_term[i]).mix(state_.commit_at_election[i]);
      for (const std::string& applied : state_.applied[i]) d.mix(applied);
      d.mix(std::uint64_t{0});
    }
    // Message *contents*, in link order and never sequence numbers: two states
    // that differ only in which integers were handed out are the same state,
    // and mixing them would defeat deduplication entirely. Iterating a std::map
    // is what makes the order canonical rather than insertion-dependent.
    for (const auto& [link, queue] : state_.wire) {
      d.mix(link.first).mix(link.second).mix(static_cast<std::uint64_t>(queue.size()));
      for (const WireMessage& wm : queue) {
        const raft::RaftMessage& m = wm.msg;
        d.mix(static_cast<std::uint64_t>(m.type))
            .mix(m.term)
            .mix(m.prev_index)
            .mix(m.prev_term)
            .mix(m.commit)
            .mix(m.reject)
            .mix(m.match)
            .mix(m.reject_hint)
            .mix(m.reject_term)
            .mix(m.read_context)
            .mix(m.echo_time)
            .mix(static_cast<std::uint64_t>(m.entries.size()));
        for (const raft::LogEntry& e : m.entries) d.mix(e.index).mix(e.term).mix(e.data);
      }
    }
    for (const std::uint32_t used : state_.proposals_used) d.mix(used);
    return d;
  }

  bool out_of_bounds() const override { return in_flight() > config_.max_in_flight; }

  std::vector<std::string> violations() const override { return violations_; }

  Snapshot save() const override { return std::make_shared<const ModelState>(state_); }

  void restore(const Snapshot& snapshot) override {
    state_ = *std::static_pointer_cast<const ModelState>(snapshot);
    observe();
  }

  // Reporting aid: how big the reachable region actually got.
  std::size_t node_count() const noexcept { return config_.nodes; }

  // ---- for a driver that needs to narrate what it did -------------------
  //
  // The state-space search needs none of this: it only ever asks for the
  // enabled set, fires one, and reads a fingerprint. Trace export needs to see
  // *which* message a delivery consumed and what the resulting state looks
  // like, because it is writing the run down in a different vocabulary.
  const ModelState& state() const noexcept { return state_; }
  const ModelConfig& config() const noexcept { return config_; }

  // The message a delivery transition is about to consume; null for a tick or
  // a proposal. Valid only until the next fire().
  const raft::RaftMessage* message_of(const Transition& t) const {
    if (t.label >= kTickLabel) return nullptr;
    const auto it = state_.wire.find(link_of(t.label));
    if (it == state_.wire.end() || it->second.empty()) return nullptr;
    return &it->second.front().msg;
  }

  bool is_tick(const Transition& t) const noexcept {
    return t.label >= kTickLabel && t.label < kProposeLabel;
  }
  bool is_propose(const Transition& t) const noexcept { return t.label >= kProposeLabel; }

 private:
  raft::Config voters() const {
    std::vector<std::uint64_t> ids;
    for (std::uint32_t i = 0; i < config_.nodes; ++i) ids.push_back(i + 1);
    return raft::Config::from_voters(ids);
  }

  static const char* kind_name(raft::RaftMessageType type) noexcept {
    return raft::to_string(type);
  }

  std::size_t in_flight() const {
    std::size_t total = 0;
    for (const auto& [link, queue] : state_.wire) total += queue.size();
    return total;
  }

  // A delivery's identity: which link, and which position in that link's own
  // sequence. Both halves are independent of anything another node did, which
  // is the property the independence relation needs.
  static std::uint64_t delivery_label(const Link& link, std::uint64_t seq) noexcept {
    return (link.first << 32) | (link.second << 24) | seq;
  }
  static Link link_of(std::uint64_t label) noexcept {
    return Link{label >> 32, (label >> 24) & 0xFF};
  }

  // Node-local value names, so two nodes proposing in either order write the
  // same two values.
  std::string value_of(std::uint32_t index) const {
    return "n" + std::to_string(index + 1) + "v" + std::to_string(state_.proposals_used[index]);
  }

  // The driver, in one transition: persist, apply, send, advance -- and repeat
  // until the node has nothing more to say. Splitting this across transitions
  // would be modelling something the real driver cannot do.
  void pump(std::uint32_t index) {
    raft::RaftNode& node = state_.nodes[index];
    for (int round = 0; round < 32; ++round) {
      raft::Ready ready = node.ready(state_.now[index]);
      if (ready.empty()) break;
      for (const raft::LogEntry& entry : ready.committed) {
        if (entry.type == raft::EntryType::kNormal) state_.applied[index].push_back(entry.data);
      }
      for (const raft::RaftMessage& msg : ready.messages) {
        if (msg.to.value() == 0 || msg.to.value() > config_.nodes) continue;
        const Link link{msg.from.value(), msg.to.value()};
        state_.wire[link].push_back(WireMessage{state_.next_seq[link]++, msg});
      }
      node.advance(ready, state_.now[index]);
    }
  }

  // Records the branch-local history the predicates need, then evaluates them.
  void observe() {
    violations_.clear();
    const std::uint32_t n = config_.nodes;

    // ---- history, recorded before it is checked --------------------------
    for (std::uint32_t i = 0; i < n; ++i) {
      const raft::RaftNode& node = state_.nodes[i];

      // INV-RAFT-04: one vote per node per term.
      if (node.vote().value() != 0) {
        const std::pair<std::uint64_t, std::uint64_t> key{i + 1, node.term().value()};
        const auto [it, fresh] = state_.votes.emplace(key, node.vote().value());
        if (!fresh && it->second != node.vote().value()) {
          violations_.push_back("INV-RAFT-04 vote uniqueness: node " + std::to_string(i + 1) +
                                " voted for " + std::to_string(it->second) + " and then for " +
                                std::to_string(node.vote().value()) + " in term " +
                                std::to_string(node.term().value()));
        }
      }

      // INV-RAFT-10: the Figure-8 restriction. A leader counts replicas only
      // for entries of its own term; an entry from an earlier term becomes
      // committed as a *consequence* of a current-term entry committing above
      // it, never on its own replica count. So the entry sitting at a leader's
      // commit index, once that leader has moved it at all, must carry the
      // leader's own term.
      //
      // Checking the rule rather than its consequence is deliberate and is the
      // lesson of ANV-0038. The consequence -- a later leader missing a
      // committed entry -- needs five voters to be reachable at all, because
      // with three a minority is one node. The rule is violated with three, and
      // it is the rule the implementation claims to obey.
      if (node.role() == raft::Role::kLeader) {
        if (state_.leader_term[i] != node.term().value()) {
          state_.leader_term[i] = node.term().value();
          state_.commit_at_election[i] = node.log().commit_index();
        }
        const LogIndex commit = node.log().commit_index();
        if (commit > state_.commit_at_election[i]) {
          const raft::LogEntry* e = node.log().at(commit);
          if (e != nullptr && e->term.value() != node.term().value()) {
            violations_.push_back(
                "INV-RAFT-10 figure 8: node " + std::to_string(i + 1) + " leading term " +
                std::to_string(node.term().value()) + " advanced its commit index to " +
                std::to_string(commit.value()) + ", whose entry is from term " +
                std::to_string(e->term.value()));
          }
        }
      }

      // INV-RAFT-06: a commit index never goes backwards.
      if (node.log().commit_index() < state_.max_commit[i]) {
        violations_.push_back("INV-RAFT-06 commit monotonicity: node " + std::to_string(i + 1) +
                              " went from " + std::to_string(state_.max_commit[i].value()) +
                              " back to " + std::to_string(node.log().commit_index().value()));
      }
      state_.max_commit[i] = std::max(state_.max_commit[i], node.log().commit_index());

      // Everything anybody has ever committed. Recording it here rather than
      // deriving it later is what makes leader completeness checkable: the
      // entry that was committed may already have been overwritten by the time
      // the violation is visible, which is exactly the bug.
      for (std::uint64_t k = 1; k <= node.log().commit_index().value(); ++k) {
        const raft::LogEntry* e = node.log().at(LogIndex{k});
        if (e == nullptr) continue;
        const auto [it, fresh] = state_.committed.emplace(k, std::pair{e->term.value(), e->data});
        if (!fresh && (it->second.first != e->term.value() || it->second.second != e->data)) {
          violations_.push_back(
              "INV-RAFT-02 state machine safety: index " + std::to_string(k) +
              " was committed as (term " + std::to_string(it->second.first) + ", '" +
              it->second.second + "') and node " + std::to_string(i + 1) + " committed (term " +
              std::to_string(e->term.value()) + ", '" + e->data + "')");
        }
      }
    }

    // ---- INV-RAFT-01: at most one leader per term ------------------------
    std::map<std::uint64_t, std::uint64_t> leader_in_term;
    for (std::uint32_t i = 0; i < n; ++i) {
      if (state_.nodes[i].role() != raft::Role::kLeader) continue;
      const auto [it, fresh] = leader_in_term.emplace(state_.nodes[i].term().value(), i + 1);
      if (!fresh) {
        violations_.push_back("INV-RAFT-01 election safety: nodes " + std::to_string(it->second) +
                              " and " + std::to_string(i + 1) + " both lead term " +
                              std::to_string(state_.nodes[i].term().value()));
      }
    }

    // ---- INV-RAFT-03: log matching ---------------------------------------
    for (std::uint32_t a = 0; a < n; ++a) {
      for (std::uint32_t b = a + 1; b < n; ++b) {
        const raft::RaftLog& la = state_.nodes[a].log();
        const raft::RaftLog& lb = state_.nodes[b].log();
        const std::uint64_t top = std::min(la.last_index().value(), lb.last_index().value());
        for (std::uint64_t k = top; k >= 1; --k) {
          const raft::LogEntry* ea = la.at(LogIndex{k});
          const raft::LogEntry* eb = lb.at(LogIndex{k});
          if (ea == nullptr || eb == nullptr) continue;
          if (ea->term != eb->term) continue;
          // Same index and term: every preceding entry must be identical.
          for (std::uint64_t j = 1; j <= k; ++j) {
            const raft::LogEntry* pa = la.at(LogIndex{j});
            const raft::LogEntry* pb = lb.at(LogIndex{j});
            if (pa == nullptr || pb == nullptr) continue;
            if (pa->term == pb->term && pa->data == pb->data) continue;
            violations_.push_back("INV-RAFT-03 log matching: nodes " + std::to_string(a + 1) +
                                  " and " + std::to_string(b + 1) + " agree at index " +
                                  std::to_string(k) + " but differ at " + std::to_string(j));
            break;
          }
          break;
        }
      }
    }

    // ---- INV-RAFT-09: leader completeness --------------------------------
    //
    // Everything committed anywhere must be present, at the same term and with
    // the same bytes, in the log of anybody now leading a later term. This is
    // the property the Figure-8 restriction exists to preserve.
    for (std::uint32_t i = 0; i < n; ++i) {
      const raft::RaftNode& node = state_.nodes[i];
      if (node.role() != raft::Role::kLeader) continue;
      for (const auto& [index, committed] : state_.committed) {
        if (committed.first > node.term().value()) continue;  // committed in a later term
        const raft::LogEntry* e = node.log().at(LogIndex{index});
        if (e != nullptr && e->term.value() == committed.first && e->data == committed.second) {
          continue;
        }
        violations_.push_back("INV-RAFT-09 leader completeness: node " + std::to_string(i + 1) +
                              " leads term " + std::to_string(node.term().value()) +
                              " without committed index " + std::to_string(index) + " (term " +
                              std::to_string(committed.first) + ", '" + committed.second + "')");
      }
    }

    // ---- INV-RAFT-02: no two nodes apply different commands at one index --
    for (std::uint32_t a = 0; a < n; ++a) {
      for (std::uint32_t b = a + 1; b < n; ++b) {
        const std::size_t top = std::min(state_.applied[a].size(), state_.applied[b].size());
        for (std::size_t k = 0; k < top; ++k) {
          if (state_.applied[a][k] == state_.applied[b][k]) continue;
          violations_.push_back("INV-RAFT-02 state machine safety: nodes " + std::to_string(a + 1) +
                                " and " + std::to_string(b + 1) + " applied '" +
                                state_.applied[a][k] + "' and '" + state_.applied[b][k] +
                                "' at position " + std::to_string(k));
          break;
        }
      }
    }
  }

  ModelConfig config_;
  ModelState state_;
  std::vector<std::string> violations_;
};

}  // namespace anvil::testing

#endif  // ANVIL_TEST_RAFT_MODEL_H_
