// BUGGIFY: deliberate injection of rare code paths.
//
// Random fault injection finds the interleavings that are merely unlikely. It
// does not find the ones that need a specific 40-microsecond window, because the
// scheduler will not hit that window in any number of seeds you can afford. The
// FoundationDB answer is to let the code itself nominate its own rare paths:
//
//     if (ANVIL_BUGGIFY) {
//       // pretend the disk stalled here
//       co_await runtime_->sleep_for(Duration::seconds(5));
//     }
//
// In production the macro is the literal `false` and the branch is deleted. In
// simulation the site is enabled for a run with some probability, and enabled
// sites fire with some probability, so different seeds explore different
// combinations (swarm testing). A site enabled for the whole run rather than
// per-evaluation matters: it keeps behaviour coherent instead of flickering.
//
// Two design decisions worth stating, because both look arbitrary and neither
// is:
//
// 1. A site's identity is hash(file, line), not its registration order. Sites
//    are function-local statics, so they register lazily on first execution --
//    which means registration order depends on the code path taken, which
//    depends on the seed. An order-derived id would make "site 37" mean a
//    different site in every run, and the coverage numbers in INV-SIM-06 would
//    be meaningless.
//
// 2. The registry counts sites that have *executed*, which is the numerator of
//    BUGGIFY coverage. The denominator -- every site that exists -- cannot come
//    from the registry, because an unreached site never registers. tools/
//    obtains it by scanning the source for the macro. If those two numbers
//    disagree in the wrong direction, there is dead code in the core.

#ifndef ANVIL_CORE_BUGGIFY_H_
#define ANVIL_CORE_BUGGIFY_H_

#include <cstddef>
#include <cstdint>

namespace anvil {

// Stable across runs, builds, and machines: a pure function of the source
// location. FNV-1a over "file:line".
constexpr std::uint64_t buggify_site_id(const char* file, int line) noexcept {
  std::uint64_t h = 0xCBF29CE484222325ULL;
  for (const char* p = file; *p != '\0'; ++p) {
    h = (h ^ static_cast<std::uint64_t>(static_cast<unsigned char>(*p))) * 0x100000001B3ULL;
  }
  h = (h ^ static_cast<std::uint64_t>(line)) * 0x100000001B3ULL;
  return h;
}

class BuggifySite {
 public:
  BuggifySite(const char* file, int line) noexcept;

  const char* file() const noexcept { return file_; }
  int line() const noexcept { return line_; }
  std::uint64_t id() const noexcept { return id_; }

  std::uint64_t evaluations() const noexcept { return evaluations_; }
  std::uint64_t activations() const noexcept { return activations_; }
  void record(bool fired) const noexcept {
    ++evaluations_;
    if (fired) ++activations_;
  }

 private:
  const char* file_;
  int line_;
  std::uint64_t id_;
  mutable std::uint64_t evaluations_ = 0;
  mutable std::uint64_t activations_ = 0;
};

// Fixed capacity, no allocation: the registry must work in the crash path and
// must not perturb the allocator's state, since arena addresses are part of the
// determinism contract.
class BuggifyRegistry {
 public:
  static constexpr std::size_t kMaxSites = 4096;

  static BuggifyRegistry& instance() noexcept;

  void add(const BuggifySite* site) noexcept;
  std::size_t size() const noexcept { return count_; }
  const BuggifySite* at(std::size_t i) const noexcept {
    return i < count_ ? sites_[i] : nullptr;
  }
  // Sites that registered but whose bodies never ran under any seed. Reported
  // by the nightly fleet; each one is either unreachable code or a gap in the
  // fault model.
  std::size_t never_activated() const noexcept;

 private:
  BuggifyRegistry() noexcept = default;

  const BuggifySite* sites_[kMaxSites]{};
  std::size_t count_ = 0;
  std::size_t overflowed_ = 0;
};

// Implemented by the simulator. Absent in production, where the macro compiles
// the call away entirely.
class BuggifyPolicy {
 public:
  virtual ~BuggifyPolicy();
  virtual bool fire(const BuggifySite& site) = 0;
};

// Set once per simulated run, before any node starts. Null means "never fire",
// which is also the production behaviour if someone builds with BUGGIFY on.
void set_buggify_policy(BuggifyPolicy* policy) noexcept;
BuggifyPolicy* buggify_policy() noexcept;

bool buggify_fire(const BuggifySite& site) noexcept;

}  // namespace anvil

#if defined(ANVIL_ENABLE_BUGGIFY) && ANVIL_ENABLE_BUGGIFY

// The lambda gives each expansion its own function-local static without needing
// a unique name, so the macro can appear twice on one line of a template.
#define ANVIL_BUGGIFY                                                        \
  ([]() noexcept -> bool {                                                   \
    static const ::anvil::BuggifySite anvil_buggify_site__{__FILE__,         \
                                                           __LINE__};        \
    return ::anvil::buggify_fire(anvil_buggify_site__);                      \
  }())

#else

// Production: literally `false`. The optimiser removes the branch, so a BUGGIFY
// site costs nothing in the shipped binary and cannot be triggered by anything
// an operator does.
#define ANVIL_BUGGIFY (false)

#endif

#endif  // ANVIL_CORE_BUGGIFY_H_
