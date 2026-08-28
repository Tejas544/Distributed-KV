#include "anvil/core/buggify.h"

namespace anvil {
namespace {

// A single-threaded core needs no atomics and no thread-local storage. If this
// pointer ever needs synchronisation, something upstream has violated the
// no-parallelism rule and the hermeticity gate should already have failed.
BuggifyPolicy* g_policy = nullptr;

}  // namespace

BuggifySite::BuggifySite(const char* file, int line) noexcept
    : file_(file), line_(line), id_(buggify_site_id(file, line)) {
  BuggifyRegistry::instance().add(this);
}

BuggifyRegistry& BuggifyRegistry::instance() noexcept {
  static BuggifyRegistry registry;
  return registry;
}

void BuggifyRegistry::add(const BuggifySite* site) noexcept {
  if (count_ >= kMaxSites) {
    // Silently dropping sites would understate coverage and quietly weaken the
    // fault model, so count the overflow; the fleet report asserts it is zero.
    ++overflowed_;
    return;
  }
  sites_[count_++] = site;
}

std::size_t BuggifyRegistry::never_activated() const noexcept {
  std::size_t n = 0;
  for (std::size_t i = 0; i < count_; ++i) {
    if (sites_[i]->activations() == 0) ++n;
  }
  return n;
}

BuggifyPolicy::~BuggifyPolicy() = default;

void set_buggify_policy(BuggifyPolicy* policy) noexcept { g_policy = policy; }

BuggifyPolicy* buggify_policy() noexcept { return g_policy; }

bool buggify_fire(const BuggifySite& site) noexcept {
  const bool fired = g_policy != nullptr && g_policy->fire(site);
  // Recorded on every evaluation, not only on activation: the ratio of
  // activations to evaluations is what tells you whether a site's probability
  // is doing anything useful, or whether it is set so low that it has never
  // fired in a million seeds.
  site.record(fired);
  return fired;
}

}  // namespace anvil
