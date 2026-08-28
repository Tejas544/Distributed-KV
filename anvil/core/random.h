// The only source of randomness in Anvil.
//
// xoshiro256** seeded through SplitMix64. Integer arithmetic exclusively -- no
// float anywhere, because `uniform_real` would make the digest architecture
// dependent and quietly break INV-SIM-01.
//
// The important design decision here is `fork()`. A single global stream shared
// by every subsystem means that adding one random call to the compaction picker
// shifts every subsequent draw in Raft, so every previously-recorded seed stops
// reproducing its bug. Corpus rot like that destroys the regression suite the
// week you notice it. Independent per-domain substreams make a seed's meaning
// stable against unrelated code changes.

#ifndef ANVIL_CORE_RANDOM_H_
#define ANVIL_CORE_RANDOM_H_

#include <cstdint>

#include "anvil/core/types.h"

namespace anvil {

namespace detail {

constexpr std::uint64_t rotl(std::uint64_t x, int k) noexcept {
  return (x << k) | (x >> (64 - k));
}

// SplitMix64: used only to expand a seed into well-distributed state.
constexpr std::uint64_t splitmix64(std::uint64_t& x) noexcept {
  x += 0x9E3779B97F4A7C15ULL;
  std::uint64_t z = x;
  z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
  z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
  return z ^ (z >> 31);
}

// 64x64 -> high 64 bits. Hand-rolled fallback so the result is identical on
// toolchains without __int128; Lemire's rejection method depends on this being
// exact, and "close enough" would diverge the two architectures' digests.
constexpr std::uint64_t mulhi64(std::uint64_t a, std::uint64_t b) noexcept {
#if defined(__SIZEOF_INT128__)
  return static_cast<std::uint64_t>(
      (static_cast<__uint128_t>(a) * static_cast<__uint128_t>(b)) >> 64);
#else
  const std::uint64_t a_lo = a & 0xFFFFFFFFULL, a_hi = a >> 32;
  const std::uint64_t b_lo = b & 0xFFFFFFFFULL, b_hi = b >> 32;
  const std::uint64_t p0 = a_lo * b_lo;
  const std::uint64_t p1 = a_lo * b_hi;
  const std::uint64_t p2 = a_hi * b_lo;
  const std::uint64_t p3 = a_hi * b_hi;
  const std::uint64_t carry = ((p0 >> 32) + (p1 & 0xFFFFFFFFULL) + (p2 & 0xFFFFFFFFULL)) >> 32;
  return p3 + (p1 >> 32) + (p2 >> 32) + carry;
#endif
}

}  // namespace detail

// Domains for fork(). Each gets a substream whose sequence is unaffected by how
// many draws the other domains take.
//
// The values are arbitrary distinct odd salts -- they only need to be stable
// and well separated, because they are fed through splitmix64 before use.
// Never renumber an existing one: doing so silently changes what every archived
// seed means, and the whole regression corpus stops reproducing its bugs.
enum class RandomDomain : std::uint64_t {
  kScheduler   = 0x1D5C'0FED'0000'0001ULL,
  kNetwork     = 0x2E70'C0DE'0000'0003ULL,
  kDisk        = 0x3D15'CB0A'0000'0005ULL,
  kClock       = 0x4C10'CBEE'0000'0007ULL,
  kProcess     = 0x59F0'CAFE'0000'0009ULL,
  kBuggify     = 0x6B06'61F0'0000'000BULL,
  kWorkload    = 0x70F1'0AD0'0000'000DULL,
  kApplication = 0x8A99'1100'0000'000FULL,
};

class DeterministicRandom {
 public:
  constexpr explicit DeterministicRandom(std::uint64_t seed) noexcept {
    std::uint64_t s = seed;
    for (auto& word : s_) word = detail::splitmix64(s);
  }

  constexpr std::uint64_t next_u64() noexcept {
    const std::uint64_t result = detail::rotl(s_[1] * 5, 7) * 9;
    const std::uint64_t t = s_[1] << 17;
    s_[2] ^= s_[0];
    s_[3] ^= s_[1];
    s_[1] ^= s_[2];
    s_[0] ^= s_[3];
    s_[2] ^= t;
    s_[3] = detail::rotl(s_[3], 45);
    return result;
  }

  constexpr std::uint32_t next_u32() noexcept {
    return static_cast<std::uint32_t>(next_u64() >> 32);
  }

  // Unbiased value in [0, n), by Lemire's multiply-shift with rejection.
  // The rejection loop is what makes it unbiased; a plain modulo would skew
  // toward small values, which matters when the draw picks which node to crash.
  constexpr std::uint64_t uniform(std::uint64_t n) noexcept {
    if (n <= 1) return 0;
    std::uint64_t x = next_u64();
    std::uint64_t hi = detail::mulhi64(x, n);
    std::uint64_t lo = x * n;
    if (lo < n) {
      const std::uint64_t threshold = (~n + 1) % n;  // (2^64 - n) % n
      while (lo < threshold) {
        x = next_u64();
        hi = detail::mulhi64(x, n);
        lo = x * n;
      }
    }
    return hi;
  }

  // Inclusive range.
  constexpr std::int64_t uniform_range(std::int64_t lo, std::int64_t hi) noexcept {
    if (hi <= lo) return lo;
    const auto span = static_cast<std::uint64_t>(hi - lo) + 1;
    return lo + static_cast<std::int64_t>(uniform(span));
  }

  constexpr Duration uniform_duration(Duration lo, Duration hi) noexcept {
    return Duration{uniform_range(lo.nanos(), hi.nanos())};
  }

  // Probability as a rational, never a double. `bernoulli(3, 100)` is 3%.
  constexpr bool bernoulli(std::uint64_t numerator, std::uint64_t denominator) noexcept {
    if (numerator == 0 || denominator == 0) return false;
    if (numerator >= denominator) return true;
    return uniform(denominator) < numerator;
  }

  constexpr bool coin() noexcept { return (next_u64() >> 63) != 0; }

  // An independent substream. Deterministic in (this stream's seed, domain,
  // instance) and NOT in the number of draws already taken, which is the whole
  // point -- see the header comment.
  constexpr DeterministicRandom fork(RandomDomain domain,
                                     std::uint64_t instance = 0) const noexcept {
    std::uint64_t mixed = s_[0] ^ static_cast<std::uint64_t>(domain);
    mixed ^= detail::splitmix64(instance);
    return DeterministicRandom{detail::splitmix64(mixed)};
  }

  // Fisher-Yates over an index permutation. Exposed because "visit the peers in
  // a shuffled order" appears in several protocols, and every ad-hoc shuffle is
  // another chance to introduce an order dependence the digest will catch but
  // nobody will enjoy debugging.
  template <typename RandomIt>
  constexpr void shuffle(RandomIt first, RandomIt last) noexcept {
    const auto n = static_cast<std::uint64_t>(last - first);
    if (n < 2) return;
    for (std::uint64_t i = n - 1; i > 0; --i) {
      const std::uint64_t j = uniform(i + 1);
      if (i != j) {
        auto tmp = *(first + static_cast<std::ptrdiff_t>(i));
        *(first + static_cast<std::ptrdiff_t>(i)) = *(first + static_cast<std::ptrdiff_t>(j));
        *(first + static_cast<std::ptrdiff_t>(j)) = tmp;
      }
    }
  }

 private:
  std::uint64_t s_[4]{};
};

}  // namespace anvil

#endif  // ANVIL_CORE_RANDOM_H_
