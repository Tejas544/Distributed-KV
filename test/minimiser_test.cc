// P7 exit criterion 5: the delta-debugging fault-schedule minimiser.
//
//   "The minimiser reduces a known failing run from >= 10 faults to <= 3 in
//    under 5 minutes."
//
// The vehicle is P1's durable counter, because it is the smallest thing in the
// tree that can lose data and it has two deliberate durability knobs whose
// *correct* answer is known in advance. That matters more than it sounds. A
// minimiser is a search, and a search you cannot grade is a search you cannot
// trust: run it on a bug whose cause you do not know and any answer looks
// plausible. Here the answer is derivable from first principles --
//
//   fsync_before_ack = false     acknowledges a write that is only in the page
//                                cache. The page cache is lost by a crash and
//                                by nothing else in this model, so the minimal
//                                fault set is exactly {process.crash}.
//   fsync_dir_on_create = false  never persists the WAL's directory entry, so
//                                the file itself does not survive. Same single
//                                cause, reached through a different mechanism.
//
// -- so "did it find the right one" is a question with a right answer, and the
// suite asserts the answer rather than the size.
//
// Five properties, in order of how easy each is to get wrong:
//
//   1. The criterion itself: >= 10 armed features in, <= 3 out, inside the
//      budget, with 1-minimality *verified* rather than assumed.
//   2. The minimum is the right one, not merely a small one.
//   2b. A different failure gets a different answer. Both bugs above need a
//      crash, so a search that had learned to say "process.crash" would pass
//      everything so far; the third case chases detected corruption instead,
//      which a crash cannot cause on its own.
//   3. Handed a configuration that does not fail, the minimiser says so instead
//      of triumphantly reducing it to nothing. A minimiser without this check
//      reports a beautiful result for every green run in the fleet.
//   4. It is deterministic. A minimised fault set goes in a ledger row, and a
//      row whose `faults_minimised` field changes between runs is not evidence.

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#include "anvil/sim/minimiser.h"
#include "anvil/sim/simulation.h"
#include "workloads/counter.h"

namespace {

using anvil::Duration;
using anvil::sim::BuggifyConfig;
using anvil::sim::FaultFeature;
using anvil::sim::FaultProfile;
using anvil::sim::FaultSet;
using anvil::sim::MinimiseOptions;
using anvil::sim::MinimiseResult;
using anvil::sim::SimConfig;
using anvil::workloads::CounterConfig;

int g_failures = 0;

void check(bool condition, std::string_view what) {
  if (!condition) {
    std::cerr << "FAIL: " << what << "\n";
    ++g_failures;
  }
}

// The experiment, minus the adversary. Held fixed across every candidate the
// minimiser proposes: the node count and the workload are the machine, and a
// machine that changes size between attempts makes the search meaningless.
struct Fixture {
  std::uint32_t nodes = 3;
  CounterConfig workload;
  Duration budget = Duration::seconds(20);
};

// Which failure the minimiser is chasing. The minimiser is only as specific as
// its predicate, and a search that answers "process.crash" whatever it is asked
// is indistinguishable from a stub -- so the suite asks two different questions
// of the same workload and requires two different answers.
enum class Failure {
  kLostAcknowledgedWrite,  // durability: something promised did not come back
  kCorruptionDetected,     // integrity: a record failed its checksum on replay
};

bool reproduces_failure(Failure failure, const Fixture& fixture, const FaultSet& faults,
                        std::uint64_t schedule_seed);

bool loses_an_acknowledged_write(const Fixture& fixture, const FaultSet& faults,
                                 std::uint64_t schedule_seed) {
  return reproduces_failure(Failure::kLostAcknowledgedWrite, fixture, faults, schedule_seed);
}

// One run under a specific adversary. `schedule_seed` is the only thing that
// varies between attempts at the same fault set -- see the header's note on why
// a subset that "does not reproduce" may only have failed to reproduce on the
// schedules tried.
bool reproduces_failure(Failure failure, const Fixture& fixture, const FaultSet& faults,
                        std::uint64_t schedule_seed) {
  SimConfig cfg;
  cfg.seed = schedule_seed;
  cfg.nodes = fixture.nodes;
  cfg.max_time = fixture.budget;
  cfg.faults = faults.profile;
  cfg.buggify = faults.buggify;

  anvil::sim::Simulation sim{cfg};
  anvil::workloads::CounterState state;
  anvil::workloads::install(sim, fixture.workload, &state);

  sim.run();
  sim.heal_and_settle(Duration::seconds(120));

  // The specific failure, not any failure. A predicate that accepts "something
  // went wrong" will cheerfully minimise one bug down to the faults that cause
  // a different one, and the ledger row it produces names the wrong cause.
  switch (failure) {
    case Failure::kLostAcknowledgedWrite:
      return state.lost_acked_writes > 0;
    case Failure::kCorruptionDetected:
      return state.corruption_detected > 0;
  }
  return false;
}

std::uint64_t schedule_seed_for(std::uint64_t base, std::uint32_t attempt) {
  // A different schedule under the same adversary. Mixed rather than added, so
  // attempt 1 of seed N is not attempt 0 of seed N+1 -- otherwise the attempts
  // of neighbouring seeds overlap and the extra confidence is imaginary.
  return base ^ (0x9e3779b97f4a7c15ull * (attempt + 1));
}

std::string render(const std::vector<FaultFeature>& features) {
  if (features.empty()) return "none";
  std::string out;
  for (const FaultFeature f : features) {
    if (!out.empty()) out += " + ";
    out += anvil::sim::to_string(f);
  }
  return out;
}

// A seed whose drawn adversary arms at least `want` features and which, with
// the given durability bug compiled in, actually loses an acknowledged write.
// Both halves are needed: the criterion is about reducing a *large* fault set,
// and a large fault set that does not reproduce is not a starting point.
struct Candidate {
  bool found = false;
  std::uint64_t seed = 0;
  FaultProfile profile;
  BuggifyConfig buggify;
  std::vector<FaultFeature> features;
};

Candidate find_failing_seed(Failure failure, const Fixture& fixture, std::uint64_t seeds,
                            std::size_t want) {
  Candidate best;
  for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
    const SimConfig drawn = SimConfig::from_seed(seed);
    const auto features = anvil::sim::features_of(drawn.faults, drawn.buggify);
    if (features.size() < want) continue;

    FaultSet as_drawn;
    as_drawn.profile = drawn.faults;
    as_drawn.buggify = drawn.buggify;
    as_drawn.features = features;
    if (!reproduces_failure(failure, fixture, as_drawn, seed)) continue;

    best.found = true;
    best.seed = seed;
    best.profile = drawn.faults;
    best.buggify = drawn.buggify;
    best.features = features;
    return best;
  }
  return best;
}

MinimiseResult minimise_for(Failure failure, const Fixture& fixture, const Candidate& candidate,
                            MinimiseOptions options) {
  const auto reproduces = [&](const FaultSet& faults, std::uint32_t attempt) {
    return reproduces_failure(failure, fixture, faults,
                              schedule_seed_for(candidate.seed, attempt));
  };
  return anvil::sim::minimise(candidate.profile, candidate.buggify, reproduces, options);
}

void report(const char* label, const Candidate& candidate, const MinimiseResult& r,
            double seconds) {
  std::cout << "  " << label << "\n"
            << "    seed ................ 0x" << std::hex << candidate.seed << std::dec << "\n"
            << "    armed as drawn ..... " << r.started_with << "  (" << render(candidate.features)
            << ")\n"
            << "    minimised to ....... " << r.ended_with << "  (" << r.minimal.render() << ")\n"
            << "    load-bearing ....... " << render(r.load_bearing) << "\n"
            << "    predicate runs ..... " << r.predicate_runs << "\n"
            << "    converged .......... " << (r.converged ? "yes" : "no") << "\n"
            << "    1-minimal verified . " << (r.verified_one_minimal ? "yes" : "no") << "\n"
            << "    wall clock ......... " << seconds << "s\n";
}

// ---------------------------------------------------------------------------
// 1 + 2. the criterion, and the answer being the right one
// ---------------------------------------------------------------------------

void test_reduces_a_known_failure(std::uint64_t search_seeds) {
  struct Case {
    const char* label;
    CounterConfig workload;
    FaultFeature expected;  // the cause derivable from first principles
  };

  std::vector<Case> cases;
  {
    Case a;
    a.label = "fsync_before_ack = false (an ack for a page-cache-only write)";
    a.workload.fsync_before_ack = false;
    a.expected = FaultFeature::kProcessCrash;
    cases.push_back(a);

    Case b;
    b.label = "fsync_dir_on_create = false (a synced file with no directory entry)";
    b.workload.fsync_dir_on_create = false;
    b.expected = FaultFeature::kProcessCrash;
    cases.push_back(b);
  }

  for (const Case& c : cases) {
    Fixture fixture;
    fixture.workload = c.workload;

    const Candidate candidate =
        find_failing_seed(Failure::kLostAcknowledgedWrite, fixture, search_seeds, /*want=*/10);
    check(candidate.found,
          "a seed arming >= 10 fault features and reproducing the planted bug must exist "
          "within the search budget");
    if (!candidate.found) continue;

    MinimiseOptions options;
    options.attempts = 3;
    options.max_runs = 400;

    const auto started = std::chrono::steady_clock::now();
    const MinimiseResult r =
        minimise_for(Failure::kLostAcknowledgedWrite, fixture, candidate, options);
    const double seconds =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();

    report(c.label, candidate, r, seconds);

    check(r.started_with >= 10, "the criterion's premise: the run starts with >= 10 faults");
    check(r.ended_with <= 3, "P7 exit criterion 5: minimised to <= 3 faults");
    check(seconds < 300.0, "P7 exit criterion 5: under 5 minutes");
    check(r.converged, "the minimisation converged rather than exhausting its run budget");
    check(r.verified_one_minimal,
          "every remaining fault was individually shown to be load-bearing");

    // The graded half. Both knobs lose data that is in memory or in the page
    // cache, and in this model a crash is the only thing that takes either
    // away -- so the search is not merely allowed to end small, it is required
    // to end on the right feature.
    check(r.minimal.contains(c.expected),
          "the minimal set must contain the cause the mechanism actually needs");
    check(r.ended_with == 1,
          "and it needs exactly that one -- anything else means the predicate is "
          "accepting a failure the planted bug did not cause");
  }
}

// ---------------------------------------------------------------------------
// 2b. a different failure minimises to a different cause
//
// Everything above is a durability bug, and every durability bug in this model
// needs a crash. A minimiser that has learned to say "process.crash" would pass
// all of it. So ask a question with a different right answer: this time the
// workload has no planted bug at all, and the failure being chased is the
// checksum firing during replay -- damage to bytes already written, which a
// crash cannot cause and a corrupting disk can.
// ---------------------------------------------------------------------------

void test_a_different_failure_minimises_to_a_different_cause(std::uint64_t search_seeds) {
  Fixture fixture;  // every durability knob correct: this is not a code bug

  const Candidate candidate =
      find_failing_seed(Failure::kCorruptionDetected, fixture, search_seeds, /*want=*/10);
  check(candidate.found, "a seed that both arms >= 10 features and corrupts a record must exist");
  if (!candidate.found) return;

  MinimiseOptions options;
  options.attempts = 3;
  options.max_runs = 400;

  const auto started = std::chrono::steady_clock::now();
  const MinimiseResult r =
      minimise_for(Failure::kCorruptionDetected, fixture, candidate, options);
  const double seconds =
      std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();

  report("detected corruption, with no planted bug at all", candidate, r, seconds);

  check(r.converged, "the minimisation converged");
  check(r.ended_with <= 3, "minimised to <= 3 faults");
  check(!r.minimal.contains(FaultFeature::kProcessCrash) || r.ended_with > 1,
        "a checksum failure is not caused by a crash alone -- if this is the answer, the "
        "predicate is picking up a different failure than the one it names");
  check(r.minimal.contains(FaultFeature::kDiskBitRot) ||
            r.minimal.contains(FaultFeature::kDiskTornWrite),
        "damage to bytes already written comes from the disk, and the minimal set must say so");
}

// ---------------------------------------------------------------------------
// 3. the premise check
// ---------------------------------------------------------------------------

void test_refuses_a_configuration_that_does_not_fail() {
  Fixture fixture;  // every durability knob at its correct default

  const SimConfig drawn = SimConfig::from_seed(1);
  const auto reproduces = [&](const FaultSet& faults, std::uint32_t attempt) {
    return loses_an_acknowledged_write(fixture, faults, schedule_seed_for(1, attempt));
  };

  MinimiseOptions options;
  options.attempts = 2;
  const MinimiseResult r =
      anvil::sim::minimise(drawn.faults, drawn.buggify, reproduces, options);

  check(!r.converged,
        "handed a configuration that does not reproduce, the minimiser must report "
        "failure rather than reduce it to nothing");
  check(r.ended_with == r.started_with,
        "and it must hand the input back unchanged rather than a fabricated minimum");
  std::cout << "  premise check ........ a passing configuration is rejected after "
            << r.predicate_runs << " runs\n";
}

// ---------------------------------------------------------------------------
// 4. determinism, and the identity that makes "minimised to nothing" meaningful
// ---------------------------------------------------------------------------

void test_deterministic_and_grounded() {
  Fixture fixture;
  fixture.workload.fsync_before_ack = false;

  const Candidate candidate =
      find_failing_seed(Failure::kLostAcknowledgedWrite, fixture, 200, /*want=*/10);
  check(candidate.found, "the determinism case needs a failing seed too");
  if (!candidate.found) return;

  MinimiseOptions options;
  options.attempts = 2;
  options.max_runs = 400;

  const MinimiseResult a = minimise_for(Failure::kLostAcknowledgedWrite, fixture, candidate, options);
  const MinimiseResult b = minimise_for(Failure::kLostAcknowledgedWrite, fixture, candidate, options);

  check(a.minimal.features == b.minimal.features,
        "the same failing run minimises to the same fault set every time -- a "
        "faults_minimised field that moves between runs is not evidence");
  check(a.predicate_runs == b.predicate_runs,
        "and by the same search: the same number of predicate evaluations");

  // The empty subset is the codebase's own control condition rather than
  // something that merely resembles it. If these ever diverge, "minimised to
  // nothing" stops meaning "reproduces with the adversary switched off".
  const FaultSet empty = anvil::sim::restrict_to(candidate.profile, candidate.buggify, {});
  check(anvil::sim::features_of(empty.profile, empty.buggify).empty(),
        "the empty subset arms nothing -- it is FaultProfile::none()");

  // And the full subset is the original: restriction must not quietly weaken
  // the adversary it was handed, or every minimisation starts from a straw man.
  const FaultSet full =
      anvil::sim::restrict_to(candidate.profile, candidate.buggify, candidate.features);
  check(anvil::sim::features_of(full.profile, full.buggify) == candidate.features,
        "restricting to everything gives back everything");

  std::cout << "  determinism .......... 2/2 identical minimisations ("
            << a.minimal.render() << "), " << a.predicate_runs << " runs each\n";
}

}  // namespace

int main(int argc, char** argv) {
  const std::uint64_t search_seeds = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 200;

  std::cout << "P7 exit criterion 5: fault-schedule minimisation\n";
  test_reduces_a_known_failure(search_seeds);
  test_a_different_failure_minimises_to_a_different_cause(search_seeds);
  test_refuses_a_configuration_that_does_not_fail();
  test_deterministic_and_grounded();

  if (g_failures != 0) {
    std::cerr << "\nminimiser: " << g_failures << " check(s) failed\n";
    return 1;
  }
  std::cout << "\nfault-schedule minimiser: all checks passed\n";
  return 0;
}
