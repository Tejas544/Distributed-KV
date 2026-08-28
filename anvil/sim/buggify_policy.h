// Per-run BUGGIFY policy: swarm testing over the injection sites.
//
// Two independent decisions, and keeping them separate is the whole design:
//
//   enablement  Is this site live at all for this run? Decided once, as a pure
//               function of (seed, site id). Different seeds enable different
//               subsets, which is what "swarm testing" means -- a run where
//               every site is live is one very strange system, and a run where
//               none are is the un-buggified system. The interesting behaviour
//               is at the combinations in between.
//
//   firing      Given the site is live, does it fire on this evaluation? Drawn
//               from the run's RNG stream, so it varies within the run.
//
// Enablement must not come from the RNG stream. If it did, it would depend on
// how many draws happened before the site was first evaluated -- i.e. on the
// code path taken -- and the same seed would enable different site sets after
// an unrelated change upstream. Deriving it from hash(seed, site id) makes a
// seed's meaning stable, which is what lets the corpus in test/corpus/ keep
// reproducing its bugs.

#ifndef ANVIL_SIM_BUGGIFY_POLICY_H_
#define ANVIL_SIM_BUGGIFY_POLICY_H_

#include <cstdint>
#include <map>

#include "anvil/core/buggify.h"
#include "anvil/core/random.h"

namespace anvil::sim {

class Scheduler;

struct BuggifyConfig {
  // Percentages, as rationals over 100. No floating point anywhere near a
  // decision the digest depends on.
  std::uint32_t enable_pct = 25;
  std::uint32_t fire_pct = 25;
};

class SimBuggifyPolicy final : public BuggifyPolicy {
 public:
  SimBuggifyPolicy(std::uint64_t seed, BuggifyConfig config, Scheduler* scheduler);

  bool fire(const BuggifySite& site) override;

  std::uint64_t sites_seen() const noexcept { return enabled_.size(); }
  std::uint64_t activations() const noexcept { return activations_; }

 private:
  bool site_enabled(std::uint64_t site_id);

  std::uint64_t seed_;
  BuggifyConfig config_;
  Scheduler* scheduler_;
  DeterministicRandom fire_rng_;
  std::map<std::uint64_t, bool> enabled_;  // memoised, not stateful
  std::uint64_t activations_ = 0;
};

}  // namespace anvil::sim

#endif  // ANVIL_SIM_BUGGIFY_POLICY_H_
