// The execution digest: a 128-bit rolling hash over every decision the
// scheduler makes and every message it delivers.
//
// This is the mechanism behind INV-SIM-01. Two runs of the same seed must
// produce the same digest; so must gcc and clang; so must x86-64 and arm64. It
// is a cheap, total check -- it does not tell you *where* determinism broke, but
// it tells you immediately *that* it broke, which is the part that is easy to
// not notice for two months.
//
// Deliberately order-sensitive: mix(a) then mix(b) differs from mix(b) then
// mix(a). A commutative digest would accept a reordered message delivery as
// identical, which is exactly the divergence we are trying to catch.

#ifndef ANVIL_CORE_DIGEST_H_
#define ANVIL_CORE_DIGEST_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <string_view>

#include "anvil/core/types.h"

namespace anvil {

namespace detail {

// Pure (non-mutating) SplitMix64 finaliser.
constexpr std::uint64_t mix64(std::uint64_t z) noexcept {
  z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
  z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
  return z ^ (z >> 31);
}

}  // namespace detail

class Digest {
 public:
  constexpr Digest() noexcept = default;

  constexpr Digest& mix(std::uint64_t v) noexcept {
    lo_ = detail::mix64(lo_ ^ v);
    // Rotate-then-fold so the two lanes stay coupled; without the coupling the
    // high lane degenerates into a second independent 64-bit hash and we lose
    // most of the collision resistance we paid for.
    hi_ = detail::mix64(((hi_ << 31) | (hi_ >> 33)) + lo_ + 0x9E3779B97F4A7C15ULL);
    ++events_;
    return *this;
  }

  constexpr Digest& mix(std::int64_t v) noexcept {
    return mix(static_cast<std::uint64_t>(v));
  }
  constexpr Digest& mix(std::uint32_t v) noexcept { return mix(static_cast<std::uint64_t>(v)); }
  constexpr Digest& mix(bool v) noexcept { return mix(static_cast<std::uint64_t>(v ? 1 : 0)); }
  constexpr Digest& mix(Timestamp t) noexcept { return mix(t.physical).mix(t.logical); }
  constexpr Digest& mix(Duration d) noexcept { return mix(d.nanos()); }

  template <typename Tag, typename Rep>
  constexpr Digest& mix(Id<Tag, Rep> id) noexcept {
    return mix(static_cast<std::uint64_t>(id.value()));
  }

  constexpr Digest& mix(std::string_view s) noexcept {
    mix(static_cast<std::uint64_t>(s.size()));
    for (const char c : s) mix(static_cast<std::uint64_t>(static_cast<unsigned char>(c)));
    return *this;
  }

  constexpr Digest& mix(ByteView bytes) noexcept {
    mix(static_cast<std::uint64_t>(bytes.size()));
    for (const std::byte b : bytes) mix(static_cast<std::uint64_t>(b));
    return *this;
  }

  constexpr std::uint64_t low() const noexcept { return lo_; }
  constexpr std::uint64_t high() const noexcept { return hi_; }
  constexpr std::uint64_t events() const noexcept { return events_; }

  friend constexpr bool operator==(const Digest& a, const Digest& b) noexcept {
    return a.lo_ == b.lo_ && a.hi_ == b.hi_;
  }

  // Hand-rolled because snprintf is on the denylist (tools/hermetic.toml,
  // "console-io"): the core does not get to touch stdio, not even to format a
  // number. Returns a NUL-terminated 32-hex-digit string.
  constexpr std::array<char, 33> hex() const noexcept {
    constexpr char kDigits[] = "0123456789abcdef";
    std::array<char, 33> out{};
    std::size_t i = 0;
    for (const std::uint64_t lane : {hi_, lo_}) {
      for (int shift = 60; shift >= 0; shift -= 4) {
        out[i++] = kDigits[(lane >> shift) & 0xF];
      }
    }
    out[32] = '\0';
    return out;
  }

 private:
  // Distinct non-zero starting lanes so an all-zero event stream still produces
  // a distinctive digest rather than something that looks uninitialised.
  std::uint64_t lo_ = 0x243F6A8885A308D3ULL;
  std::uint64_t hi_ = 0x13198A2E03707344ULL;
  std::uint64_t events_ = 0;
};

}  // namespace anvil

#endif  // ANVIL_CORE_DIGEST_H_
