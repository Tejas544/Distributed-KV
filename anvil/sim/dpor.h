// Systematic exploration: every interleaving of a tiny configuration, rather
// than a random sample of a large one.
//
// The fault sweep answers "does this survive the adversary" by sampling. That
// is the right instrument for a five-node cluster running for two simulated
// minutes, and it is the wrong one for "is there *any* schedule under which two
// leaders exist in one term" -- because the answer to the second question is a
// single interleaving out of billions, and sampling finds it with probability
// approximately zero. P3 measured exactly this: two of its ten planted bugs
// need a lagging follower at the instant of an election, which random
// scheduling produces about once in thousands of seeds, and reaching them at
// all needed a BUGGIFY site.
//
// So: shrink the configuration until the whole space fits, then look at all of
// it.
//
// ---------------------------------------------------------------------------
// What is implemented, stated precisely, because "we did DPOR" is a claim
// people ask follow-up questions about
// ---------------------------------------------------------------------------
//
// Two explorers over the same system, and the second is graded against the
// first:
//
//   explore_exhaustive   Full reachable-state enumeration with fingerprint
//                        deduplication. Complete for the configuration class:
//                        every reachable global state is visited and every
//                        armed invariant is evaluated in it. This is the ground
//                        truth and it is what the exit criterion is measured
//                        against.
//
//   explore_por          The same search with sleep-set partial-order
//                        reduction over an actor-independence relation. This
//                        is the *reduction* half of dynamic partial-order
//                        reduction. The persistent-set half (dynamically
//                        computed backtracking points) is deliberately not
//                        implemented: with nondeterministic actors -- a node
//                        that may consume any of several pending messages --
//                        the classical formulation does not apply unchanged,
//                        and a subtly unsound reduction that silently skips the
//                        one interleaving containing the bug is strictly worse
//                        than a slower search. See the note on validation
//                        below.
//
// **The reduction is validated, not asserted.** A partial-order reduction is a
// claim that the traces it skips are equivalent to traces it kept. That claim
// is checkable here for free, because the exhaustive search is available: both
// explorers must report the same set of terminal-state fingerprints and the
// same violations. If the reduction ever prunes something real, that comparison
// fails loudly instead of the search quietly reporting "no violations found".
// This is the same discipline as the hermeticity gate's negative control -- a
// reduction only ever observed to agree is indistinguishable from one that does
// nothing.
//
// ---------------------------------------------------------------------------
// The independence relation
// ---------------------------------------------------------------------------
//
// Two transitions are independent when they belong to different actors. For a
// message-passing cluster the actor is the node: nodes hold disjoint state and
// interact only by sending messages, and a message becomes a transition of its
// *recipient*. Two nodes stepping in either order therefore reach the same
// global state.
//
// That sentence is easy to write and was wrong three times. Each way it was
// wrong cost a disagreement between the two searches, and each is worth
// knowing before writing another `ExplorableSystem`:
//
//   1. The fingerprint has to be complete. A digest built from a node's public
//      accessors omitted the replication progress and the election timers, so
//      the exhaustive search merged states that behave differently and stopped
//      being exhaustive -- while the reduced search, which memoises nothing,
//      reached a terminal state the "complete" one could not. ANV-0063.
//
//   2. The state has to be canonical. Globally numbered messages in one list do
//      not commute: two nodes sending, in either order, produce the same
//      messages with different numbers in a different order. Per-link queues
//      with per-link numbering do commute. Same for anything else shared -- a
//      single proposal counter made "node 1 proposes" and "node 2 proposes"
//      disagree about who wrote which value. ANV-0064.
//
//   3. The *search's own pruning* has to be invariant under commutation, and a
//      bound on messages in flight is not. A-then-B and B-then-A end in the
//      same state but peak differently, so one order can be cut off at the
//      bound while the other survives -- and sleep sets then prune the survivor
//      as already covered, losing the subtree from both. Measured: on a class
//      where the bound binds, the reduced search found 42 terminal states
//      against the exhaustive search's 407; on a class where it never binds,
//      382 against 382.
//
// Hence `Transition::label`: a stable identity, which for a delivery is the
// message's position in its own link's sequence and never a position in a
// global list.

#ifndef ANVIL_SIM_DPOR_H_
#define ANVIL_SIM_DPOR_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "anvil/core/digest.h"

namespace anvil::sim {

// One step the system may take. `actor` carries the independence relation;
// `label` identifies the step within its actor and must be stable across
// interleavings.
struct Transition {
  std::uint32_t actor = 0;
  std::uint64_t label = 0;
  std::string name;

  // Canonical order, so that two runs enumerate a state's successors
  // identically. Never order by a pointer or a container's iteration order.
  friend bool operator<(const Transition& a, const Transition& b) noexcept {
    if (a.actor != b.actor) return a.actor < b.actor;
    return a.label < b.label;
  }
  friend bool operator==(const Transition& a, const Transition& b) noexcept {
    return a.actor == b.actor && a.label == b.label;
  }
};

// The system under exploration.
//
// The explorer drives it by save/restore rather than by replaying from the
// initial state. Replay needs nothing extra and is O(depth) per edge, which for
// the state graphs this is aimed at is the difference between a gate that runs
// in CI and one that does not.
class ExplorableSystem {
 public:
  using Snapshot = std::shared_ptr<const void>;

  virtual ~ExplorableSystem() = default;

  virtual void reset() = 0;

  // Sorted, and identical for identical states. An enabled() that depends on
  // how the state was reached makes the fingerprint a lie.
  virtual std::vector<Transition> enabled() const = 0;

  virtual void fire(const Transition& transition) = 0;

  // Canonical fingerprint of the whole observable state. Two states with the
  // same fingerprint are treated as the same state, so anything the invariants
  // can distinguish must be mixed in.
  virtual Digest fingerprint() const = 0;

  // Evaluated at every reachable state. Empty means "nothing fired here".
  virtual std::vector<std::string> violations() const = 0;

  // True when the state has run past the configuration class being claimed --
  // more messages in flight than the bound allows, more ticks than budgeted.
  // Such states are counted and not expanded, and the count is reported: a
  // search that silently truncates is a search whose "exhaustive" is worthless.
  virtual bool out_of_bounds() const { return false; }

  virtual Snapshot save() const = 0;
  virtual void restore(const Snapshot& snapshot) = 0;
};

struct ExploreOptions {
  // 0 means no ceiling. A search that hits a ceiling reports `complete = false`,
  // which is the difference between "there is no violation" and "we did not
  // look everywhere".
  std::uint64_t max_states = 0;
  std::uint64_t max_depth = 0;

  // Stop at the first violation, with the path to it. Off when the point is to
  // count states rather than to find a bug.
  bool stop_on_violation = true;

  // Diagnostic: turning the reduction off leaves a plain stateless tree search,
  // which is the control that separates "the sleep sets are wrong" from "the
  // tree search is wrong". Without it, a disagreement with the exhaustive search
  // has two suspects and no way to tell them apart.
  bool use_sleep_sets = true;

  // Diagnostic: when the two explorers disagree about the terminal states, the
  // question is always "how did you get there", and reconstructing it by hand
  // from a 300,000-edge search is not a thing anybody does twice. Set this to a
  // terminal fingerprint and `counterexample` comes back holding the path to
  // it. Zero disables.
  std::uint64_t trace_terminal = 0;
};

struct ExploreStats {
  std::uint64_t states = 0;        // distinct fingerprints reached
  std::uint64_t transitions = 0;   // edges fired, including those into seen states
  std::uint64_t terminals = 0;     // states with nothing enabled
  std::uint64_t out_of_bounds = 0; // states outside the configuration class
  std::uint64_t max_depth = 0;
  std::uint64_t sleep_pruned = 0;  // edges the reduction skipped (POR only)
  bool complete = false;           // the frontier emptied within the ceilings

  std::vector<std::string> violations;
  std::vector<Transition> counterexample;  // the path to the first violation

  // Fingerprints of every terminal state, sorted. The reduction is graded
  // against this: an equivalent search must end in the same places.
  std::vector<std::uint64_t> terminal_fingerprints;
};

std::string render_path(const std::vector<Transition>& path);

// Complete enumeration of the reachable state graph.
ExploreStats explore_exhaustive(ExplorableSystem& system, ExploreOptions options = {});

// The same search under sleep-set partial-order reduction.
ExploreStats explore_por(ExplorableSystem& system, ExploreOptions options = {});

}  // namespace anvil::sim

#endif  // ANVIL_SIM_DPOR_H_
