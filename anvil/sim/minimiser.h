// Delta-debugging over the adversary: which faults did this failure actually
// need?
//
// A failing seed arrives with a whole drawn profile -- lossy links, a flapping
// partition, torn writes, skewed clocks, crashing processes, a dozen live
// BUGGIFY sites. Almost none of that is load-bearing. A ledger row that says
// "reproduces under seed 0x... with everything on" is a row nobody can reason
// about; one that says "needs exactly a crash and a torn write" is a root cause
// with the narrative already written. That difference is the `faults_minimised`
// field in the BUGS.md schema, and until now it has been filled in by hand.
//
// This is Zeller's ddmin over a set of *fault features* -- one entry per knob
// the profile can arm, plus BUGGIFY. The empty subset is exactly
// `FaultProfile::none()`, so "minimised to nothing" and "the codebase's own
// control condition" are the same configuration rather than two things that
// happen to look alike.
//
// ---------------------------------------------------------------------------
// The honest caveat, stated up front because it determines how to read a result
// ---------------------------------------------------------------------------
//
// ddmin's guarantee (1-minimality: no single remaining element can be dropped
// and still reproduce) assumes a *deterministic* predicate. This one is not
// deterministic in the way that assumption wants. Disabling a fault does not
// remove an event from a fixed schedule -- it changes which dice are rolled,
// so the whole execution downstream diverges. Two consequences, and neither is
// a defect to be fixed:
//
//   1. A subset that "does not reproduce" may only have failed to reproduce on
//      the schedules tried. `MinimiseOptions::attempts` exists for this: the
//      predicate is run over several schedule seeds with the profile held
//      fixed, and a subset counts as reproducing if any of them fails. More
//      attempts cost time and buy confidence; the count is reported so a reader
//      can judge how much was bought.
//
//   2. The result is minimal *with respect to this predicate*, not necessarily
//      globally. `MinimiseResult::verified_one_minimal` records whether the
//      final 1-minimality check actually passed -- i.e. whether every kept
//      feature was individually shown to be load-bearing -- rather than
//      assuming it from the algorithm's structure.
//
// Reporting a minimisation without both numbers would be claiming a
// determinism the search does not have.

#ifndef ANVIL_SIM_MINIMISER_H_
#define ANVIL_SIM_MINIMISER_H_

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "anvil/sim/buggify_policy.h"
#include "anvil/sim/fault_profile.h"

namespace anvil::sim {

// One independently disableable knob. The order is stable and is part of what
// a corpus entry means, so entries are appended rather than inserted.
enum class FaultFeature : std::uint8_t {
  kNetDrop,
  kNetDuplicate,
  kNetReset,
  kNetReorder,
  kNetBandwidth,
  kPartition,
  kDiskTornWrite,
  kDiskIoError,
  kDiskSlowIo,
  kDiskBitRot,
  kDiskNoSpace,
  kClockSkew,
  kClockJump,
  kClockFreeze,
  kClockBoundViolation,
  kProcessCrash,
  kProcessPause,
  kBuggify,
  kCount,
};

const char* to_string(FaultFeature feature) noexcept;

// The adversary as a set, which is the form ddmin operates on.
struct FaultSet {
  FaultProfile profile;
  BuggifyConfig buggify;
  std::vector<FaultFeature> features;  // sorted, unique

  bool contains(FaultFeature f) const noexcept;
  std::string render() const;  // "crash + torn_write + partition", or "none"
};

// Which features the given configuration actually has armed. A knob set to
// `Chance::never()` is not armed and is not a candidate for removal, so the
// starting size is the honest count of what the seed drew rather than the size
// of the enum.
std::vector<FaultFeature> features_of(const FaultProfile& profile, const BuggifyConfig& buggify);

// `original` restricted to `keep`: every feature in `keep` carries its original
// setting, every other knob is returned to its `FaultProfile::none()` value.
// Latency ranges are not faults and are preserved throughout -- a network with
// no latency is not a less hostile network, it is a different machine.
FaultSet restrict_to(const FaultProfile& original, const BuggifyConfig& buggify,
                     const std::vector<FaultFeature>& keep);

// Does the failure still happen? Returning true means "yes, and it is the same
// failure" -- a predicate that accepts any failure will happily minimise one
// bug down to the faults that cause a different one.
//
// `attempt` counts from zero and is the minimiser asking for a *different
// schedule* under the same adversary. The caller owns the mapping from attempt
// to seed, because only the caller knows which of its inputs are the schedule
// and which are the experiment.
using Reproduces = std::function<bool(const FaultSet&, std::uint32_t attempt)>;

struct MinimiseOptions {
  // Schedule seeds tried per candidate before concluding "does not reproduce".
  // 1 is a fast, noisy answer; 3-5 is usually enough for this simulator.
  std::uint32_t attempts = 3;

  // Hard ceiling on predicate calls, since each one is a whole simulation. 0
  // means no ceiling. A minimisation that hits the ceiling is reported as
  // unconverged rather than silently returned as a result.
  std::uint32_t max_runs = 0;

  // Run the closing 1-minimality check. Costs |result| more predicate calls and
  // is what turns "ddmin terminated" into "every kept fault was shown to be
  // load-bearing". On by default: the check is the evidence.
  bool verify_one_minimal = true;
};

struct MinimiseResult {
  FaultSet minimal;
  std::size_t started_with = 0;
  std::size_t ended_with = 0;
  std::uint32_t predicate_runs = 0;
  bool converged = false;              // false if max_runs stopped it early
  bool verified_one_minimal = false;   // the closing check ran and passed
  std::vector<FaultFeature> load_bearing;  // features the closing check proved necessary
};

// ddmin. `failing` must reproduce -- the caller is expected to have established
// that already, and this asserts it rather than discovering it, because a
// minimiser handed a passing configuration will "minimise" it to nothing and
// look like it worked.
MinimiseResult minimise(const FaultProfile& failing, const BuggifyConfig& buggify,
                        const Reproduces& reproduces, MinimiseOptions options = {});

}  // namespace anvil::sim

#endif  // ANVIL_SIM_MINIMISER_H_
