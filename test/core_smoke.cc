// P0 smoke test: the frozen seam compiles, links, and behaves.
//
// This is not a substitute for the real suite (GoogleTest arrives in P1). It
// exists so that "the skeleton builds" is a claim ctest can check, and so that
// the pieces P0 promises -- deterministic RNG, order-sensitive digest, BUGGIFY
// registry, coroutine tasks -- are exercised at least once.
//
// Note that this file may use iostreams freely: it is a test binary, not part
// of anvil_core, and the hermeticity gate is scoped to the core archive.

#include <cstdlib>
#include <iostream>
#include <string_view>
#include <vector>

#include "anvil/core/buggify.h"
#include "anvil/core/digest.h"
#include "anvil/core/random.h"
#include "anvil/core/runtime/task.h"
#include "anvil/core/types.h"

namespace {

int g_failures = 0;

void check(bool condition, std::string_view what) {
  if (!condition) {
    std::cerr << "FAIL: " << what << "\n";
    ++g_failures;
  }
}

// -- types ------------------------------------------------------------------

void test_key_ordering() {
  const auto bytes = [](const char* s) {
    return anvil::ByteView{reinterpret_cast<const std::byte*>(s), std::char_traits<char>::length(s)};
  };
  check(anvil::compare_bytes(bytes("a"), bytes("b")) < 0, "a < b");
  check(anvil::compare_bytes(bytes("ab"), bytes("a")) > 0, "prefix orders before extension");
  check(anvil::compare_bytes(bytes("x"), bytes("x")) == 0, "equal keys compare equal");

  const anvil::KeyRange r{bytes("b"), bytes("d")};
  check(!r.contains(bytes("a")), "range excludes below start");
  check(r.contains(bytes("b")), "range includes start");
  check(r.contains(bytes("c")), "range includes interior");
  check(!r.contains(bytes("d")), "range excludes end (half-open)");

  const anvil::KeyRange unbounded{bytes("b"), anvil::Key{}};
  check(unbounded.contains(bytes("zzzz")), "empty end means unbounded above");
}

void test_timestamps() {
  const anvil::Timestamp a{100, 0};
  const anvil::Timestamp b{100, 1};
  check(a < b, "logical component breaks physical ties");
  check(a.advanced_by(anvil::Duration::millis(1)).physical == 100 + 1'000'000,
        "millis convert to nanos");
  check(anvil::Timestamp::zero() < anvil::Timestamp::max(), "zero precedes max");

  const anvil::TimeInterval iv{{100, 0}, {900, 0}};
  check(iv.contains({500, 0}), "interval contains interior point");
  check(!iv.contains({901, 0}), "interval excludes points past latest");
  check(iv.width().nanos() == 800, "interval width");
}

// -- randomness -------------------------------------------------------------

void test_random_is_reproducible() {
  anvil::DeterministicRandom a{0xA1B2C3D4E5F60718ULL};
  anvil::DeterministicRandom b{0xA1B2C3D4E5F60718ULL};
  for (int i = 0; i < 1000; ++i) {
    if (a.next_u64() != b.next_u64()) {
      check(false, "same seed must produce the same sequence");
      return;
    }
  }

  anvil::DeterministicRandom c{0xA1B2C3D4E5F60719ULL};
  anvil::DeterministicRandom d{0xA1B2C3D4E5F60718ULL};
  check(c.next_u64() != d.next_u64(), "adjacent seeds must not collide immediately");
}

void test_uniform_is_in_range_and_unbiased() {
  anvil::DeterministicRandom rng{42};
  std::vector<int> buckets(7, 0);
  constexpr int kDraws = 70'000;
  for (int i = 0; i < kDraws; ++i) {
    const auto v = rng.uniform(7);
    if (v >= 7) {
      check(false, "uniform(n) must stay below n");
      return;
    }
    ++buckets[static_cast<std::size_t>(v)];
  }
  // Loose bound: this is a smoke test, not a statistics suite. A modulo-biased
  // implementation skews badly enough to trip even a check this generous.
  for (const int count : buckets) {
    check(count > kDraws / 7 - 1500 && count < kDraws / 7 + 1500,
          "uniform() should be roughly flat");
  }
}

void test_fork_is_independent_of_draw_count() {
  // The property that keeps the regression corpus alive: adding a random draw
  // in one subsystem must not shift another subsystem's stream.
  anvil::DeterministicRandom base{0xDEADBEEF};
  const auto expected = base.fork(anvil::RandomDomain::kNetwork, 3).next_u64();

  anvil::DeterministicRandom same_seed{0xDEADBEEF};
  const auto actual = same_seed.fork(anvil::RandomDomain::kNetwork, 3).next_u64();
  check(expected == actual, "fork() is a pure function of (seed, domain, instance)");

  anvil::DeterministicRandom other{0xDEADBEEF};
  check(other.fork(anvil::RandomDomain::kDisk, 3).next_u64() != expected,
        "different domains yield different substreams");
  check(other.fork(anvil::RandomDomain::kNetwork, 4).next_u64() != expected,
        "different instances yield different substreams");
}

void test_bernoulli_bounds() {
  anvil::DeterministicRandom rng{7};
  check(!rng.bernoulli(0, 100), "p=0 never fires");
  check(rng.bernoulli(100, 100), "p=1 always fires");
  int hits = 0;
  for (int i = 0; i < 10'000; ++i) {
    if (rng.bernoulli(25, 100)) ++hits;
  }
  check(hits > 2200 && hits < 2800, "p=0.25 fires about a quarter of the time");
}

// -- digest -----------------------------------------------------------------

void test_digest_is_order_sensitive() {
  anvil::Digest a, b;
  a.mix(std::uint64_t{1}).mix(std::uint64_t{2});
  b.mix(std::uint64_t{2}).mix(std::uint64_t{1});
  check(!(a == b), "digest must distinguish reordered events");

  anvil::Digest c, d;
  c.mix(std::uint64_t{1}).mix(std::uint64_t{2});
  d.mix(std::uint64_t{1}).mix(std::uint64_t{2});
  check(c == d, "identical event streams must digest identically");
  check(c.events() == 2, "event count tracks mixes");

  anvil::Digest e;
  e.mix(anvil::NodeId{3}).mix(anvil::Timestamp{99, 1}).mix(std::string_view{"append"});
  const auto hex = e.hex();
  check(std::string_view{hex.data()}.size() == 32, "hex renders 128 bits");
}

// -- buggify ----------------------------------------------------------------

class AlwaysFire final : public anvil::BuggifyPolicy {
 public:
  bool fire(const anvil::BuggifySite&) override {
    ++calls;
    return true;
  }
  int calls = 0;
};

void test_buggify_site_identity() {
  // Stable across runs and machines because it is a pure function of the
  // source location, not of registration order.
  const auto id_a = anvil::buggify_site_id("anvil/core/raft/log.cc", 412);
  const auto id_b = anvil::buggify_site_id("anvil/core/raft/log.cc", 412);
  const auto id_c = anvil::buggify_site_id("anvil/core/raft/log.cc", 413);
  check(id_a == id_b, "site id is deterministic");
  check(id_a != id_c, "site id distinguishes lines");
}

void test_buggify_policy() {
#if defined(ANVIL_ENABLE_BUGGIFY) && ANVIL_ENABLE_BUGGIFY
  check(!ANVIL_BUGGIFY, "with no policy installed, BUGGIFY never fires");

  AlwaysFire policy;
  anvil::set_buggify_policy(&policy);
  check(ANVIL_BUGGIFY, "an installed policy is consulted");
  check(policy.calls == 1, "policy sees exactly one call per evaluation");
  anvil::set_buggify_policy(nullptr);

  check(anvil::BuggifyRegistry::instance().size() >= 1,
        "evaluated sites register themselves");
#else
  check(!ANVIL_BUGGIFY, "BUGGIFY compiles to false when disabled");
  std::cout << "  (BUGGIFY disabled in this build; policy path not exercised)\n";
#endif
}

// -- tasks ------------------------------------------------------------------

anvil::Task<int> answer() { co_return 42; }

anvil::Task<int> doubled() {
  const int v = co_await answer();
  co_return v * 2;
}

anvil::Task<void> nested(int* out) {
  *out = co_await doubled();
  co_return;
}

// Minimal driver. The real scheduler lands with SimRuntime; this only proves
// the coroutine machinery links and that symmetric transfer resumes correctly.
void drive(anvil::Task<void> task) {
  auto handle = task.release();
  handle.resume();
  while (!handle.done()) handle.resume();
  handle.destroy();
}

void test_tasks() {
  int result = 0;
  drive(nested(&result));
  check(result == 84, "awaited task chain produces the right value");
}

}  // namespace

int main() {
  test_key_ordering();
  test_timestamps();
  test_random_is_reproducible();
  test_uniform_is_in_range_and_unbiased();
  test_fork_is_independent_of_draw_count();
  test_bernoulli_bounds();
  test_digest_is_order_sensitive();
  test_buggify_site_identity();
  test_buggify_policy();
  test_tasks();

  if (g_failures == 0) {
    std::cout << "core smoke: all checks passed\n";
    return EXIT_SUCCESS;
  }
  std::cerr << "core smoke: " << g_failures << " check(s) failed\n";
  return EXIT_FAILURE;
}
