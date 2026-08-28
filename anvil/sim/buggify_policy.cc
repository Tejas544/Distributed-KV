#include "anvil/sim/buggify_policy.h"

#include "anvil/sim/scheduler.h"

namespace anvil::sim {

SimBuggifyPolicy::SimBuggifyPolicy(std::uint64_t seed, BuggifyConfig config,
                                   Scheduler* scheduler)
    : seed_(seed),
      config_(config),
      scheduler_(scheduler),
      fire_rng_(DeterministicRandom{seed}.fork(RandomDomain::kBuggify)) {}

bool SimBuggifyPolicy::site_enabled(std::uint64_t site_id) {
  const auto it = enabled_.find(site_id);
  if (it != enabled_.end()) return it->second;

  // A private stream seeded from (run seed, site id). Deliberately not drawn
  // from fire_rng_: enablement must not depend on evaluation order.
  std::uint64_t mixed = seed_ ^ site_id;
  DeterministicRandom site_rng{detail::splitmix64(mixed)};
  const bool enabled = site_rng.bernoulli(config_.enable_pct, 100);
  enabled_.emplace(site_id, enabled);
  return enabled;
}

bool SimBuggifyPolicy::fire(const BuggifySite& site) {
  if (!site_enabled(site.id())) return false;
  if (!fire_rng_.bernoulli(config_.fire_pct, 100)) return false;

  ++activations_;
  if (scheduler_ != nullptr) {
    // Every activation goes into the digest. A run where a rare path fired must
    // not hash the same as one where it did not, or the digest would be blind
    // to exactly the behaviour BUGGIFY exists to produce.
    scheduler_->digest().mix(site.id());
    if (scheduler_->trace().recording()) {
      const TraceField fields[] = {{"site", site.id()},
                                   {"line", static_cast<std::uint64_t>(site.line())}};
      scheduler_->trace().emit(scheduler_->now(), NodeId{}, EventKind::kBuggify, site.file(),
                               fields);
    }
  }
  return true;
}

}  // namespace anvil::sim
