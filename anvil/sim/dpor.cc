#include "anvil/sim/dpor.h"

#include <algorithm>
#include <deque>
#include <map>
#include <set>
#include <utility>

namespace anvil::sim {
namespace {

using Key = std::pair<std::uint64_t, std::uint64_t>;

Key key_of(const Digest& digest) noexcept { return {digest.high(), digest.low()}; }

// Two transitions commute exactly when they belong to different actors. The
// header's note on why this needs stable labels is load-bearing, not colour.
bool independent(const Transition& a, const Transition& b) noexcept {
  return a.actor != b.actor;
}

}  // namespace

std::string render_path(const std::vector<Transition>& path) {
  std::string out;
  for (const Transition& t : path) {
    if (!out.empty()) out += " -> ";
    out += t.name;
  }
  return out.empty() ? "<initial state>" : out;
}

// ---------------------------------------------------------------------------
// exhaustive: every reachable state, deduplicated by fingerprint
// ---------------------------------------------------------------------------

// Depth-first rather than breadth-first, and the reason is memory. A BFS
// frontier over a graph like this holds tens of thousands of states at once,
// and a state here is three whole Raft nodes -- the first version of this ran
// out of address space long before it ran out of states. A DFS stack is
// O(depth) snapshots, and it comes with a bonus: the stack *is* the path to the
// current state, so a counterexample needs no parent pointers to reconstruct.
ExploreStats explore_exhaustive(ExplorableSystem& system, ExploreOptions options) {
  ExploreStats stats;

  struct Frame {
    ExplorableSystem::Snapshot snapshot;
    std::vector<Transition> enabled;
    std::size_t next = 0;
  };

  std::set<Key> seen;
  std::vector<Frame> stack;
  std::vector<Transition> path;

  system.reset();
  seen.insert(key_of(system.fingerprint()));
  ++stats.states;

  {
    const auto initial = system.violations();
    if (!initial.empty()) {
      stats.violations = initial;
      if (options.stop_on_violation) return stats;
    }
  }

  {
    std::vector<Transition> enabled = system.enabled();
    if (enabled.empty()) {
      ++stats.terminals;
      stats.terminal_fingerprints.push_back(system.fingerprint().low());
      stats.complete = true;
      return stats;
    }
    stack.push_back(Frame{system.save(), std::move(enabled), 0});
  }

  while (!stack.empty()) {
    if (options.max_states != 0 && stats.states >= options.max_states) return stats;

    Frame& frame = stack.back();
    if (frame.next == frame.enabled.size()) {
      stack.pop_back();
      if (!path.empty()) path.pop_back();
      continue;
    }

    const Transition t = frame.enabled[frame.next++];
    system.restore(frame.snapshot);
    system.fire(t);
    ++stats.transitions;

    if (!seen.insert(key_of(system.fingerprint())).second) continue;
    ++stats.states;
    stats.max_depth = std::max<std::uint64_t>(stats.max_depth, path.size() + 1);

    if (options.trace_terminal != 0 && system.fingerprint().low() == options.trace_terminal &&
        stats.counterexample.empty()) {
      stats.counterexample = path;
      stats.counterexample.push_back(t);
    }

    const auto fired = system.violations();
    if (!fired.empty()) {
      stats.violations.insert(stats.violations.end(), fired.begin(), fired.end());
      if (stats.counterexample.empty()) {
        stats.counterexample = path;
        stats.counterexample.push_back(t);
      }
      if (options.stop_on_violation) return stats;
    }

    // Terminality is decided before the bound, and the order matters: a state
    // with nothing enabled is an end of the system, not an end of the search,
    // and classifying it as "outside the class" instead would make the two
    // explorers disagree about what a terminal state is. That disagreement is
    // exactly what the reduction check would then report as unsoundness -- a
    // finding manufactured by the harness, which is the failure mode this
    // codebase has spent four ledger rows on.
    std::vector<Transition> enabled = system.enabled();
    if (enabled.empty()) {
      ++stats.terminals;
      stats.terminal_fingerprints.push_back(system.fingerprint().low());
      continue;
    }

    // A state past the configuration bound is counted and not expanded. It is
    // still checked above, because a violation found just outside the class is
    // a real violation and hiding it would be absurd -- what the bound governs
    // is how far the *claim* of exhaustiveness reaches.
    if (system.out_of_bounds()) {
      ++stats.out_of_bounds;
      continue;
    }
    if (options.max_depth != 0 && path.size() + 1 >= options.max_depth) {
      ++stats.out_of_bounds;
      continue;
    }
    path.push_back(t);
    stack.push_back(Frame{system.save(), std::move(enabled), 0});
  }

  std::sort(stats.terminal_fingerprints.begin(), stats.terminal_fingerprints.end());
  stats.terminal_fingerprints.erase(
      std::unique(stats.terminal_fingerprints.begin(), stats.terminal_fingerprints.end()),
      stats.terminal_fingerprints.end());
  stats.complete = true;
  return stats;
}

// ---------------------------------------------------------------------------
// sleep-set partial-order reduction
//
// Stateless depth-first search. It is deliberately *not* combined with the
// fingerprint dedup above: a state reached with a smaller sleep set has
// successors the earlier visit never explored, so memoising on the fingerprint
// alone silently drops them. That interaction is the classic unsoundness in
// stateful partial-order reduction, and the whole reason this file ships an
// exhaustive search to grade against.
// ---------------------------------------------------------------------------

namespace {

struct PorSearch {
  ExplorableSystem* system;
  ExploreOptions options;
  ExploreStats stats;
  std::vector<Transition> path;
  std::set<Key> distinct;
  bool stopped = false;

  void visit(const std::set<Transition>& sleep) {
    if (stopped) return;
    if (options.max_states != 0 && stats.states >= options.max_states) {
      stopped = true;
      return;
    }

    const std::vector<Transition> enabled = system->enabled();
    if (enabled.empty()) {
      ++stats.terminals;
      const std::uint64_t fp = system->fingerprint().low();
      if (options.trace_terminal != 0 && fp == options.trace_terminal &&
          stats.counterexample.empty()) {
        stats.counterexample = path;
      }
      stats.terminal_fingerprints.push_back(fp);
      return;
    }
    if (system->out_of_bounds()) {
      ++stats.out_of_bounds;
      return;
    }
    if (options.max_depth != 0 && path.size() >= options.max_depth) {
      ++stats.out_of_bounds;
      return;
    }

    const ExplorableSystem::Snapshot here = system->save();

    // Explored-so-far at this state. A transition already taken here is added to
    // the child's sleep set when it commutes with the one being taken now,
    // which is exactly the statement "that interleaving has been covered".
    std::vector<Transition> explored;

    for (const Transition& t : enabled) {
      if (sleep.count(t) != 0) {
        ++stats.sleep_pruned;
        explored.push_back(t);
        continue;
      }

      std::set<Transition> child_sleep;
      if (options.use_sleep_sets) {
        for (const Transition& u : sleep) {
          if (independent(u, t)) child_sleep.insert(u);
        }
        for (const Transition& u : explored) {
          if (independent(u, t)) child_sleep.insert(u);
        }
      }

      system->restore(here);
      system->fire(t);
      ++stats.transitions;

      const Key fingerprint = key_of(system->fingerprint());
      if (distinct.insert(fingerprint).second) ++stats.states;
      stats.max_depth = std::max<std::uint64_t>(stats.max_depth, path.size() + 1);

      path.push_back(t);
      const auto fired = system->violations();
      if (!fired.empty()) {
        stats.violations.insert(stats.violations.end(), fired.begin(), fired.end());
        if (stats.counterexample.empty()) stats.counterexample = path;
        if (options.stop_on_violation) {
          stopped = true;
          path.pop_back();
          return;
        }
      }

      visit(child_sleep);
      path.pop_back();
      if (stopped) return;

      explored.push_back(t);
    }
    system->restore(here);
  }
};

}  // namespace

ExploreStats explore_por(ExplorableSystem& system, ExploreOptions options) {
  PorSearch search;
  search.system = &system;
  search.options = options;

  system.reset();
  search.distinct.insert(key_of(system.fingerprint()));
  search.stats.states = 1;

  const auto initial = system.violations();
  if (!initial.empty()) {
    search.stats.violations = initial;
    if (options.stop_on_violation) return search.stats;
  }

  search.visit({});

  auto& fingerprints = search.stats.terminal_fingerprints;
  std::sort(fingerprints.begin(), fingerprints.end());
  fingerprints.erase(std::unique(fingerprints.begin(), fingerprints.end()), fingerprints.end());
  search.stats.complete = !search.stopped;
  return search.stats;
}

}  // namespace anvil::sim
