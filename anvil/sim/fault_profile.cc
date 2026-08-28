#include "anvil/sim/fault_profile.h"

#include "anvil/core/digest.h"

namespace anvil::sim {
namespace {

// Each axis is drawn from its own substream, so adding a knob to the disk
// profile does not change which network profile a given seed gets. Without
// this, every archived seed would mean something different the next time
// anybody extended this file -- and the regression corpus would rot silently.
DeterministicRandom axis(std::uint64_t seed, std::uint64_t which) {
  return DeterministicRandom{seed}.fork(RandomDomain::kProcess, 0xFA07'0000ULL + which);
}

}  // namespace

const char* to_string(PartitionStyle style) noexcept {
  switch (style) {
    case PartitionStyle::kNone: return "none";
    case PartitionStyle::kOneShot: return "one_shot";
    case PartitionStyle::kFlapping: return "flapping";
    case PartitionStyle::kAsymmetric: return "asymmetric";
    case PartitionStyle::kMajorityIsolating: return "majority_isolating";
    case PartitionStyle::kHalfOpen: return "half_open";
  }
  return "unknown";
}

FaultProfile FaultProfile::none() noexcept {
  FaultProfile p;
  p.disk.torn_write = Chance::never();
  p.disk.page_cache = true;  // still honest about durability; just never crashes
  return p;
}

FaultProfile FaultProfile::draw(std::uint64_t seed) {
  FaultProfile p;

  // -- network ------------------------------------------------------------
  {
    auto rng = axis(seed, 1);
    p.net.min_latency = Duration::micros(rng.uniform_range(20, 500));
    p.net.max_latency = p.net.min_latency + Duration::micros(rng.uniform_range(100, 40'000));

    if (rng.bernoulli(7, 8)) {  // one run in eight has a perfect network
      p.net.drop = Chance::bp(static_cast<std::uint32_t>(rng.uniform_range(0, 1500)));
      p.net.duplicate = Chance::bp(static_cast<std::uint32_t>(rng.uniform_range(0, 400)));
      p.net.reset = Chance::bp(static_cast<std::uint32_t>(rng.uniform_range(0, 200)));
    }
    p.net.allow_reorder = rng.bernoulli(1, 5);
    if (rng.bernoulli(1, 4)) {
      p.net.bandwidth_bytes_per_sec =
          static_cast<std::uint64_t>(rng.uniform_range(64'000, 100'000'000));
    }
  }

  // -- partitions ---------------------------------------------------------
  {
    auto rng = axis(seed, 2);
    static constexpr PartitionStyle kStyles[] = {
        PartitionStyle::kNone,       PartitionStyle::kOneShot,
        PartitionStyle::kFlapping,   PartitionStyle::kAsymmetric,
        PartitionStyle::kMajorityIsolating, PartitionStyle::kHalfOpen,
    };
    p.partition.style = kStyles[rng.uniform(sizeof(kStyles) / sizeof(kStyles[0]))];
    p.partition.first_at = Duration::millis(rng.uniform_range(100, 5'000));
    p.partition.duration = Duration::millis(rng.uniform_range(200, 8'000));
    p.partition.period = p.partition.duration + Duration::millis(rng.uniform_range(100, 4'000));
  }

  // -- disk ---------------------------------------------------------------
  {
    auto rng = axis(seed, 3);
    p.disk.min_latency = Duration::micros(rng.uniform_range(10, 200));
    p.disk.max_latency = p.disk.min_latency + Duration::micros(rng.uniform_range(50, 20'000));
    p.disk.fsync_latency = Duration::micros(rng.uniform_range(100, 20'000));

    // The page cache stays on almost always. Turning it off is the control
    // condition in which a missing fsync cannot be detected, and a fleet that
    // spent a quarter of its runs there would be a quarter wasted.
    p.disk.page_cache = !rng.bernoulli(1, 20);
    p.disk.torn_write = Chance::pct(static_cast<std::uint32_t>(rng.uniform_range(0, 60)));

    if (rng.bernoulli(1, 3)) {
      p.disk.io_error = Chance::bp(static_cast<std::uint32_t>(rng.uniform_range(0, 300)));
    }
    if (rng.bernoulli(1, 3)) {
      p.disk.slow_io = Chance::bp(static_cast<std::uint32_t>(rng.uniform_range(0, 500)));
      p.disk.slow_io_penalty = Duration::millis(rng.uniform_range(10, 2'000));
    }
    if (rng.bernoulli(1, 6)) {
      p.disk.bit_rot = Chance::pct(static_cast<std::uint32_t>(rng.uniform_range(1, 40)));
    }
    if (rng.bernoulli(1, 8)) {
      // Log-uniform over 1 KiB .. 4 MiB, not uniform. A uniform draw over that
      // range puts almost all its mass at the top, so the first sweep produced
      // capacities no workload here could come close to filling and ENOSPC was
      // a knob that looked armed and could never fire. Capacities in the real
      // world are distributed by order of magnitude anyway.
      p.disk.capacity_bytes = 1ULL << rng.uniform_range(10, 22);
    }
  }

  // -- clock --------------------------------------------------------------
  {
    auto rng = axis(seed, 4);
    p.clock.declared_uncertainty = Duration::millis(rng.uniform_range(1, 250));
    p.clock.max_drift_ppm = rng.uniform_range(0, 400);

    if (rng.bernoulli(1, 10)) {
      // The interesting experiment: the real skew exceeds what the system was
      // told to expect. Rare on purpose -- most runs should test the system
      // under its stated assumptions, not outside them.
      p.clock.violate_declared_bound = true;
      p.clock.max_offset = p.clock.declared_uncertainty * rng.uniform_range(2, 5);
    } else {
      p.clock.max_offset = p.clock.declared_uncertainty;
    }

    if (rng.bernoulli(1, 4)) {
      p.clock.jump = Chance::pct(static_cast<std::uint32_t>(rng.uniform_range(1, 20)));
      p.clock.max_jump = Duration::millis(rng.uniform_range(10, 2'000));
    }
    if (rng.bernoulli(1, 5)) {
      p.clock.freeze = Chance::pct(static_cast<std::uint32_t>(rng.uniform_range(1, 15)));
      p.clock.max_freeze = Duration::millis(rng.uniform_range(10, 1'500));
    }
  }

  // -- process ------------------------------------------------------------
  {
    auto rng = axis(seed, 5);
    if (rng.bernoulli(2, 3)) {
      p.process.crash_per_second = Chance::bp(static_cast<std::uint32_t>(rng.uniform_range(0, 800)));
    }
    if (rng.bernoulli(1, 2)) {
      p.process.pause_per_second = Chance::bp(static_cast<std::uint32_t>(rng.uniform_range(0, 600)));
      p.process.min_pause = Duration::millis(rng.uniform_range(5, 100));
      p.process.max_pause = p.process.min_pause + Duration::millis(rng.uniform_range(50, 8'000));
    }
    p.process.min_restart_delay = Duration::millis(rng.uniform_range(10, 200));
    p.process.max_restart_delay =
        p.process.min_restart_delay + Duration::millis(rng.uniform_range(50, 5'000));
  }

  return p;
}

std::uint64_t FaultProfile::hash() const noexcept {
  Digest d;
  const auto chance = [&d](const Chance& c) {
    d.mix(static_cast<std::uint64_t>(c.numerator)).mix(static_cast<std::uint64_t>(c.denominator));
  };

  d.mix(net.min_latency).mix(net.max_latency).mix(net.allow_reorder).mix(net.bandwidth_bytes_per_sec);
  chance(net.drop);
  chance(net.duplicate);
  chance(net.reset);

  d.mix(static_cast<std::uint64_t>(partition.style))
      .mix(partition.first_at)
      .mix(partition.duration)
      .mix(partition.period);

  d.mix(disk.min_latency).mix(disk.max_latency).mix(disk.fsync_latency)
      .mix(disk.page_cache).mix(disk.slow_io_penalty).mix(disk.capacity_bytes);
  chance(disk.torn_write);
  chance(disk.io_error);
  chance(disk.slow_io);
  chance(disk.bit_rot);

  d.mix(clock.max_offset).mix(clock.max_drift_ppm).mix(clock.max_jump)
      .mix(clock.max_freeze).mix(clock.declared_uncertainty).mix(clock.violate_declared_bound);
  chance(clock.jump);
  chance(clock.freeze);

  d.mix(process.min_restart_delay).mix(process.max_restart_delay)
      .mix(process.min_pause).mix(process.max_pause);
  chance(process.crash_per_second);
  chance(process.pause_per_second);

  return d.low();
}

}  // namespace anvil::sim
