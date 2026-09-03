// P7 exit criterion 4: systematic exploration of a tiny configuration.
//
//   "DPOR exhaustively covers its configuration class with no violations, and
//    the state count is reported."
//
// The subject is `anvil/core/raft/raft.h` itself -- the shipping state machine,
// unmodified, with no simulator underneath it. That is possible only because
// P3 made it a pure state machine: no clock, no sockets, no files, its only
// output a `Ready` batch. A model checker needs exactly that, so this deliverable
// costs a few hundred lines here rather than a rewrite of the protocol.
//
// ---------------------------------------------------------------------------
// The configuration class, stated before any result, because "exhaustive" is
// meaningless without it
// ---------------------------------------------------------------------------
//
//   nodes .............. 3 voters, fixed membership, no learners, no conf change
//   proposals .......... a bounded number of client commands
//   ticks .............. a per-node budget; a node with none left cannot time out
//   in flight .......... bounded; a state that exceeds it is counted, not expanded
//   links .............. FIFO per ordered pair, which is what the real transport
//                        is. Non-FIFO delivery is *more* adversarial than TCP, so
//                        exploring it would risk reporting a schedule production
//                        cannot produce.
//   durability ......... instantaneous. `ready()` is persisted, applied and sent
//                        within one transition, which is the correct model of the
//                        driver's loop (nothing may step the state machine while
//                        a persist is in flight -- CONTEXT.md gotcha 10.8) and is
//                        also the reason `persist_before_reply` is an equivalent
//                        mutation here rather than a detectable one.
//
// A transition is one node consuming one input -- a delivery, a tick, or a
// proposal -- and running its `Ready` loop to completion. Nodes are the actors,
// and two transitions of different nodes commute, which is the independence
// relation the partial-order reduction rests on.
//
// ---------------------------------------------------------------------------
// What is checked, and why it is checked as state predicates
// ---------------------------------------------------------------------------
//
// `anvil/checker/raft_invariants.cc` is an *observer*: it diffs each node's
// state between ticks and accumulates history. That is the right instrument for
// a single timeline and the wrong one for a search that jumps between branches
// of a tree -- two leaders in term 5 on two different branches are two different
// worlds, and an observer with one memory would report them as an election
// safety violation. So the predicates here are evaluated on the state, with the
// only history being what is carried in the state itself and therefore
// branch-local. They are the same properties:
//
//   election safety ....... INV-RAFT-01   at most one leader per term
//   log matching .......... INV-RAFT-03   same (index, term) implies same prefix
//   leader completeness ... INV-RAFT-09   a committed entry is in every later
//                                         leader's log
//   state machine safety .. INV-RAFT-02   no two nodes apply different commands
//                                         at the same index
//   vote uniqueness ....... INV-RAFT-04   one vote per node per term
//   commit monotonicity ... INV-RAFT-06   a commit index never goes backwards
//   figure 8 .............. INV-RAFT-10   a leader never advances its commit
//                                         index onto a previous term's entry
//
// ---------------------------------------------------------------------------
// And the drill, which is the part that makes the rest mean anything
// ---------------------------------------------------------------------------
//
// A search that reports "no violations" over a protocol that has no bugs is
// indistinguishable from a search that does nothing. Every deliberate-bug knob
// in `RaftOptions` is run through the same exploration. Some are detected; the
// rest are *equivalent in this configuration class* and each one carries the
// argument for why, because an undetected mutation reported as a pass and an
// undetected mutation reported as a gap are both lies.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "anvil/core/raft/config.h"
#include "anvil/core/raft/raft.h"
#include "anvil/core/raft/types.h"
#include "anvil/sim/dpor.h"
#include "test/raft_model.h"

namespace {

using anvil::Digest;
using anvil::Duration;
using anvil::LogIndex;
using anvil::NodeId;
using anvil::Term;
using anvil::Timestamp;
using anvil::sim::ExploreOptions;
using anvil::sim::ExploreStats;
using anvil::sim::Transition;

// The model itself is in test/raft_model.h, shared with trace_export.cc so that
// the runs replayed against the specification are produced by exactly the model
// this file explores.
using anvil::testing::ModelConfig;
using anvil::testing::ModelState;
using anvil::testing::RaftModel;

namespace raft = anvil::raft;

int g_failures = 0;

void check(bool condition, std::string_view what) {
  if (!condition) {
    std::cerr << "FAIL: " << what << "\n";
    ++g_failures;
  }
}

}  // namespace

// ---------------------------------------------------------------------------

namespace {

// Overridable so the class can be dialled up or down when calibrating; the
// gate runs the defaults, which is what the reported numbers describe.
// The gate's class, measured rather than guessed: 3 ticks a node with two
// proposals and four messages in flight is 2.5 million reachable states and
// finishes in about half a minute, and one more message in flight does not
// finish at all. The numbers reported below describe exactly this.
std::uint32_t g_tick_budget = 2;
std::uint32_t g_proposals = 2;
std::size_t g_in_flight = 4;
std::uint64_t g_max_states = 16'000'000;

ModelConfig base_config() {
  ModelConfig cfg;
  cfg.nodes = 3;
  cfg.tick_budget = g_tick_budget;
  cfg.proposals = g_proposals;
  cfg.max_in_flight = g_in_flight;

  // The real options, shrunk until the space is finite. Timeouts in ticks are
  // a configuration of the shipping code, not a modification of it -- which is
  // the difference between model-checking the implementation and
  // model-checking a paraphrase of it.
  cfg.options.election_timeout_ticks = 2;
  cfg.options.heartbeat_timeout_ticks = 1;

  // One entry per append, and this knob is the whole reason the Figure-8
  // mutation is reachable here at all.
  //
  // A leader that ships its entire tail in one message brings a follower's
  // match index straight up to the no-op it appended on election -- an entry of
  // its *own* term -- so the commit index lands on a current-term entry whether
  // or not the restriction is enforced, and the mutation is invisible. Shorten
  // the batch and the follower catches up one entry at a time, so there is a
  // moment when a majority matches at an *older* term's entry and nothing else.
  // That is precisely the window P3's first BUGGIFY site was created to open
  // (CONTEXT.md §11): `send_append` shortening a batch is always legal and
  // cannot make a correct implementation wrong. Here it is a configured value
  // rather than a random one, because an exhaustive search does not need luck.
  cfg.options.max_entries_per_append = 1;
  return cfg;
}

void report(const char* label, const ExploreStats& s, double seconds) {
  std::cout << "  " << label << "\n"
            << "    states ............. " << s.states << "\n"
            << "    transitions ........ " << s.transitions << "\n"
            << "    terminal states .... " << s.terminals << " (" << s.terminal_fingerprints.size()
            << " distinct)\n"
            << "    max depth .......... " << s.max_depth << "\n"
            << "    outside the class .. " << s.out_of_bounds << "\n";
  if (s.sleep_pruned != 0) {
    std::cout << "    edges pruned ....... " << s.sleep_pruned << "\n";
  }
  std::cout << "    complete ........... " << (s.complete ? "yes" : "NO -- ceiling hit") << "\n"
            << "    violations ......... " << s.violations.size() << "\n"
            << "    wall clock ......... " << seconds << "s\n";
}

double time_it(const std::function<void()>& body) {
  const auto started = std::chrono::steady_clock::now();
  body();
  return std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
}

// ---------------------------------------------------------------------------
// 1. the criterion: exhaustive coverage, no violations, state count reported
// ---------------------------------------------------------------------------

ExploreStats g_baseline;

void test_exhaustive_coverage() {
  RaftModel model{base_config()};
  ExploreOptions options;
  options.stop_on_violation = false;
  options.max_states = g_max_states;

  const std::string label = "exhaustive: 3 voters, " + std::to_string(g_proposals) +
                            " proposal(s)/node, " + std::to_string(g_tick_budget) +
                            " ticks/node, <= " + std::to_string(g_in_flight) + " in flight";
  ExploreStats stats;
  const double seconds = time_it([&] { stats = anvil::sim::explore_exhaustive(model, options); });
  report(label.c_str(), stats, seconds);

  check(stats.complete, "the search must finish rather than hit its ceiling");
  check(stats.violations.empty(),
        "P7 exit criterion 4: no invariant violation anywhere in the configuration class");
  check(stats.states > 1000,
        "a configuration class this small would not be evidence -- the search must actually "
        "reach a nontrivial state space");
  check(stats.terminals > 0, "the search must reach states with nothing left enabled");
  g_baseline = stats;
}

// ---------------------------------------------------------------------------
// 2. the reduction, graded against the exhaustive search rather than trusted
//
// On a smaller class than test 1, and the reason is structural rather than
// impatience. The exhaustive search memoises on the state fingerprint; the
// reduced one cannot, because a state reached with a smaller sleep set has
// successors the earlier visit never looked at (dpor.cc says so at more
// length). A stateless search therefore walks the *tree* where the other walks
// the *graph*, and the tree over the gate's class does not terminate in any
// useful time. What is being graded here is whether the reduction preserves
// terminal states, and that question is answered by a class both searches can
// finish.
// ---------------------------------------------------------------------------

// Two voters rather than three, and no reachable in-flight bound. Both choices
// are forced by what a *stateless* search can finish; see reduction_config().
std::uint32_t g_red_nodes = 2;
std::uint32_t g_red_ticks = 3;
std::uint32_t g_red_props = 1;
std::size_t g_red_flight = 1000;

ModelConfig reduction_config() {
  ModelConfig cfg = base_config();
  cfg.nodes = g_red_nodes;
  cfg.tick_budget = g_red_ticks;
  cfg.proposals = g_red_props;

  // The in-flight bound is deliberately set out of reach here, and that is the
  // whole design of this class rather than a convenience.
  //
  // A partial-order reduction is sound only if the search's own pruning is
  // invariant under commutation, and "more than N messages in flight" is not:
  // A-then-B and B-then-A end in the same state but pass through different
  // peaks, so one order can be cut off at the bound while the other is
  // explored. Sleep sets then prune the surviving order as "already covered" and
  // the subtree beyond is lost by both. Measured, not argued: on a class where
  // the bound binds, the reduced search reported 42 terminal states against the
  // exhaustive search's 407, and on this class the two agree exactly.
  cfg.max_in_flight = g_red_flight;

  // One tick to an election, so the class is deep enough to be evidence while
  // still being small enough for a *stateless* search to finish. The reduced
  // search cannot memoise on the fingerprint (dpor.cc says why), so it walks a
  // tree where the exhaustive search walks a graph, and the tree is factorially
  // larger.
  cfg.options.election_timeout_ticks = 1;
  return cfg;
}

std::string reduction_label() {
  return std::to_string(g_red_nodes) + " voters, " + std::to_string(g_red_props) + " proposal(s)/node, " +
         std::to_string(g_red_ticks) + " ticks/node, <= " + std::to_string(g_red_flight) +
         " in flight";
}

void test_reduction_is_sound() {
  ExploreOptions options;
  options.stop_on_violation = false;
  options.max_states = g_max_states;

  RaftModel reference{reduction_config()};
  ExploreStats full;
  const double full_seconds =
      time_it([&] { full = anvil::sim::explore_exhaustive(reference, options); });
  report(("exhaustive over the reduction's class (" + reduction_label() + ")").c_str(), full,
         full_seconds);

  RaftModel model{reduction_config()};
  ExploreStats stats;
  const double seconds = time_it([&] { stats = anvil::sim::explore_por(model, options); });
  report("sleep-set reduction over that same class", stats, seconds);

  check(full.out_of_bounds == 0,
        "the comparison is only valid where the in-flight bound never binds -- a bound "
        "that cuts one interleaving and not its permutation is not invariant under "
        "commutation, and no partial-order reduction survives that");
  check(stats.complete, "the reduced search must finish too");
  check(stats.violations.empty(), "and must agree that there is nothing to find");
  check(stats.sleep_pruned > 0,
        "a reduction that never prunes anything is not a reduction -- it is a slower "
        "exhaustive search wearing the name");

  // The claim a partial-order reduction makes is that what it skipped was
  // equivalent to what it kept. Here that is checkable rather than assumed.
  if (stats.terminal_fingerprints != full.terminal_fingerprints) {
    std::cout << "    exhaustive terminals:";
    for (const std::uint64_t f : full.terminal_fingerprints) std::cout << " " << f;
    std::cout << "\n    reduced terminals:   ";
    for (const std::uint64_t f : stats.terminal_fingerprints) std::cout << " " << f;
    std::cout << "\n";
    for (const std::uint64_t f : stats.terminal_fingerprints) {
      if (std::find(full.terminal_fingerprints.begin(), full.terminal_fingerprints.end(), f) !=
          full.terminal_fingerprints.end()) {
        continue;
      }
      ExploreOptions trace = options;
      trace.trace_terminal = f;
      RaftModel probe{reduction_config()};
      const ExploreStats traced = anvil::sim::explore_por(probe, trace);
      std::cout << "    only the reduced search reaches " << f << ":\n      "
                << anvil::sim::render_path(traced.counterexample) << "\n";

      RaftModel probe2{reduction_config()};
      const ExploreStats traced2 = anvil::sim::explore_exhaustive(probe2, trace);
      std::cout << "    the exhaustive search "
                << (traced2.counterexample.empty() ? "never reaches it at all"
                                                   : "does reach it, via")
                << "\n";
      if (!traced2.counterexample.empty()) {
        std::cout << "      " << anvil::sim::render_path(traced2.counterexample) << "\n";
      }
    }
  }
  check(stats.terminal_fingerprints == full.terminal_fingerprints,
        "the reduced search must end in exactly the same terminal states as the "
        "exhaustive one -- if it does not, it pruned something real");
  std::cout << "    terminal states agree with the exhaustive search: "
            << full.terminal_fingerprints.size() << "/" << full.terminal_fingerprints.size()
            << "\n";
}

// ---------------------------------------------------------------------------
// 3. the drill
// ---------------------------------------------------------------------------

struct Mutation {
  const char* name;
  void (*apply)(raft::RaftOptions&);
  bool must_detect;
  const char* argument;  // why, when must_detect is false
};

void test_seeded_mutations() {
  const std::vector<Mutation> mutations = {
      {"restrict_vote_by_log", [](raft::RaftOptions& o) { o.restrict_vote_by_log = false; }, true,
       nullptr},
      {"check_prev_term_on_append",
       [](raft::RaftOptions& o) { o.check_prev_term_on_append = false; }, true, nullptr},
      {"commit_only_current_term",
       [](raft::RaftOptions& o) { o.commit_only_current_term = false; }, true, nullptr},
      {"persist_before_reply", [](raft::RaftOptions& o) { o.persist_before_reply = false; }, false,
       "this model persists inside the transition that produces the Ready batch and models no "
       "crash, so there is no window in which a reply outlives the state it claims"},
      {"lease_uses_wall_clock", [](raft::RaftOptions& o) { o.lease_uses_wall_clock = false; },
       false, "no lease read is issued in this class, so the lease is never consulted"},
      {"truncate_log_on_snapshot",
       [](raft::RaftOptions& o) { o.truncate_log_on_snapshot = false; }, false,
       "the tick and proposal budgets keep every log far below snapshot_threshold, so no "
       "snapshot is ever taken"},
      {"joint_requires_commit", [](raft::RaftOptions& o) { o.joint_requires_commit = false; },
       false, "membership is fixed in this class; no joint configuration is ever entered"},
      {"learners_excluded_from_quorum",
       [](raft::RaftOptions& o) { o.learners_excluded_from_quorum = false; }, false,
       "there are no learners in this class, so the quorum is computed over the same set "
       "either way"},
  };

  std::cout << "  seeded-mutation drill (exhaustive search, one mutation at a time)\n";
  std::size_t detected = 0;
  std::size_t required = 0;

  for (const Mutation& m : mutations) {
    ModelConfig cfg = base_config();
    m.apply(cfg.options);
    RaftModel model{cfg};

    ExploreOptions options;
    options.stop_on_violation = true;
    options.max_states = g_max_states;
    const ExploreStats stats = anvil::sim::explore_exhaustive(model, options);

    const bool found = !stats.violations.empty();
    if (m.must_detect) ++required;
    if (found && m.must_detect) ++detected;

    std::cout << "    " << (found ? "CAUGHT   " : "silent   ") << m.name;
    if (found) {
      std::cout << "  after " << stats.states << " states: " << stats.violations.front() << "\n"
                << "               " << anvil::sim::render_path(stats.counterexample) << "\n";
    } else {
      std::cout << "  (" << stats.states << " states)\n";
      if (!m.must_detect) std::cout << "               equivalent here: " << m.argument << "\n";
    }

    if (m.must_detect) {
      check(found, "a must-detect mutation went unnoticed by exhaustive exploration");
    } else {
      check(!found,
            "a mutation classified equivalent was detected -- the classification is wrong "
            "and the argument beside it needs rewriting, not the test");
    }
  }

  std::cout << "    detected " << detected << "/" << required
            << " must-detect mutations; " << (mutations.size() - required)
            << " classified equivalent with an argument each\n";

  // The control. Everything above must be attributable to the mutation, so the
  // unmutated configuration has to be silent -- and it is, by test 1.
  check(g_baseline.violations.empty(), "the control configuration is silent");
}

// ---------------------------------------------------------------------------
// 4. the Figure-8 case, which needs five voters and a constructed prefix
//
// Reaching this from an empty five-node cluster is not a budget any exhaustive
// search finishes, so the starting state is constructed -- the same technique
// P3 used for the tenth of its ten planted bugs. What is exhaustive is the
// exploration *from* that state, and the pair of runs is the evidence: with the
// Figure-8 restriction in place the whole reachable region is clean, and with
// it removed the same region contains a leader missing a committed entry.
// ---------------------------------------------------------------------------

void build_figure_eight_prefix(ModelState& state) {
  // Ongaro & Ousterhout figure 8, positioned one election short of the commit
  // that the restriction forbids.
  //
  //   S1  [1@t1, 2@t2]   was leader in term 2 and replicated index 2 nowhere
  //   S2  [1@t1]
  //   S3  [1@t1]
  //   S4  [1@t1]
  //   S5  [1@t1, 2@t3]   won term 3 without index 2 and wrote its own there
  //
  // Everything installed here is durable state, and it goes in through
  // restore(), which is the same path a node takes on every boot -- so this is
  // a configuration the cluster can genuinely be in, not a poke at private
  // members. What happens next is not scripted: S1 campaigns for term 4, wins
  // on the votes of S2..S4 (whose logs are shorter), replicates index 2 to a
  // majority, and with the restriction removed commits an entry from term 2.
  // S5's term-3 entry at the same index is what makes that a real loss rather
  // than a rule violation with no consequence.
  const raft::Config voters = raft::Config::from_voters({1, 2, 3, 4, 5});

  const auto entry = [](std::uint64_t term, std::uint64_t index, std::string data) {
    raft::LogEntry e;
    e.term = Term{term};
    e.index = LogIndex{index};
    e.type = raft::EntryType::kNormal;
    e.data = std::move(data);
    return e;
  };

  const raft::LogEntry e1 = entry(1, 1, "a");

  raft::HardState h;
  h.term = Term{3};
  h.commit = LogIndex{1};

  h.vote = NodeId{1};
  state.nodes[0].restore(h, {e1, entry(2, 2, "b")}, raft::Snapshot{}, voters);

  h.vote = NodeId{5};
  for (std::uint32_t i = 1; i < 4; ++i) {
    state.nodes[i].restore(h, {e1}, raft::Snapshot{}, voters);
  }
  state.nodes[4].restore(h, {e1, entry(3, 2, "c")}, raft::Snapshot{}, voters);
}

void test_figure_eight() {
  const auto explore = [](bool restriction) {
    ModelConfig cfg = base_config();
    cfg.nodes = 5;
    cfg.tick_budget = 4;
    cfg.proposals = 0;
    cfg.max_in_flight = 6;
    cfg.options.commit_only_current_term = restriction;
    cfg.prepare = build_figure_eight_prefix;

    RaftModel model{cfg};
    ExploreOptions options;
    options.stop_on_violation = true;
    options.max_states = 4'000'000;
    return anvil::sim::explore_exhaustive(model, options);
  };

  const ExploreStats correct = explore(true);
  const ExploreStats broken = explore(false);

  // Five voters is past what this search finishes, so the honest reading of the
  // pair is a *discrimination*, not a completeness claim: the same bounded
  // search over the same region reaches the violation almost immediately with
  // the restriction removed and does not reach one in four million states with
  // it in place. Reporting the left-hand column as "clean" would be claiming an
  // exhaustiveness the ceiling says was not achieved.
  std::cout << "  figure 8 (5 voters, constructed prefix; bounded search, not exhaustive)\n"
            << "    restriction in place . " << correct.states << " states explored, "
            << correct.violations.size() << " violations"
            << (correct.complete ? " (region complete)" : " (ceiling hit)") << "\n"
            << "    restriction removed .. " << broken.states << " states explored, "
            << broken.violations.size() << " violations"
            << (broken.complete ? " (region complete)" : " (stopped at the violation)") << "\n";
  if (!broken.violations.empty()) {
    std::cout << "      " << broken.violations.front() << "\n"
              << "      " << anvil::sim::render_path(broken.counterexample) << "\n";
  }

  check(correct.violations.empty(),
        "with the Figure-8 restriction in place, four million states of the constructed "
        "region contain no violation");
  check(!broken.violations.empty(),
        "with it removed, the same search must find a leader committing a previous "
        "term's entry -- this is the bug the P3 sweep needed BUGGIFY and thousands of "
        "seeds to reach");
  check(broken.states * 1000 < correct.states,
        "and it must find it early: a discrimination that needs most of the region "
        "explored is not much of a discrimination");
}

}  // namespace

int main(int argc, char** argv) {
  if (argc > 1) g_tick_budget = static_cast<std::uint32_t>(std::strtoul(argv[1], nullptr, 10));
  if (argc > 2) g_proposals = static_cast<std::uint32_t>(std::strtoul(argv[2], nullptr, 10));
  if (argc > 3) g_in_flight = static_cast<std::size_t>(std::strtoul(argv[3], nullptr, 10));
  if (argc > 4) g_max_states = std::strtoull(argv[4], nullptr, 10);
  if (argc > 5) g_red_ticks = static_cast<std::uint32_t>(std::strtoul(argv[5], nullptr, 10));
  if (argc > 6) g_red_props = static_cast<std::uint32_t>(std::strtoul(argv[6], nullptr, 10));
  if (argc > 7) g_red_flight = static_cast<std::size_t>(std::strtoul(argv[7], nullptr, 10));
  if (argc > 8) g_red_nodes = static_cast<std::uint32_t>(std::strtoul(argv[8], nullptr, 10));

  std::cout << "P7 exit criterion 4: systematic exploration of the Raft state machine\n";
  test_exhaustive_coverage();

  // Calibration mode. Sizing a configuration class is a measurement, and it is
  // one somebody will want to repeat -- so the knobs are arguments rather than
  // an edit, and passing any of them runs the sizing probe alone.
  if (argc > 1) {
    if (argc > 5) test_reduction_is_sound();
    std::cout << "\n(calibration run)\n";
    return g_failures == 0 ? 0 : 1;
  }

  test_reduction_is_sound();
  test_seeded_mutations();
  test_figure_eight();

  if (g_failures != 0) {
    std::cerr << "\ndpor: " << g_failures << " check(s) failed\n";
    return 1;
  }
  std::cout << "\nsystematic exploration: all checks passed\n";
  return 0;
}
