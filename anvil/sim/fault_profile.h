// The fault profile: every knob the simulator can turn against the system.
//
// One struct, drawn from the seed. That is the whole design, and it matters for
// two reasons.
//
// First, swarm testing. A fixed fault profile explores one adversary over and
// over. Drawing the profile from the seed means each run faces a *different*
// adversary -- one seed gets a lossy but fast network with reliable disks,
// another gets clean links and a dying disk, another gets clocks that lie. The
// interesting bugs live at the combinations, and no human writes those
// combinations down.
//
// Second, reproducibility. Because the profile is a pure function of the seed,
// a bug ledger row needs one 64-bit integer and not a page of configuration.
//
// Probabilities are exact rationals. There is no floating point anywhere in
// this file, and none anywhere near a decision the execution digest depends on
// -- see anvil/core/types.h for why.

#ifndef ANVIL_SIM_FAULT_PROFILE_H_
#define ANVIL_SIM_FAULT_PROFILE_H_

#include <cstdint>

#include "anvil/core/random.h"
#include "anvil/core/types.h"

namespace anvil::sim {

// An exact probability. `Chance::pct(3)` is three percent, always, on every
// platform, forever.
struct Chance {
  std::uint32_t numerator = 0;
  std::uint32_t denominator = 1;

  static constexpr Chance never() noexcept { return {0, 1}; }
  static constexpr Chance always() noexcept { return {1, 1}; }
  static constexpr Chance pct(std::uint32_t p) noexcept { return {p, 100}; }
  static constexpr Chance per_mille(std::uint32_t p) noexcept { return {p, 1000}; }
  static constexpr Chance bp(std::uint32_t p) noexcept { return {p, 10'000}; }

  bool roll(DeterministicRandom& rng) const noexcept {
    return rng.bernoulli(numerator, denominator);
  }
  constexpr bool possible() const noexcept { return numerator > 0; }
};

// ---------------------------------------------------------------------------
// Network
// ---------------------------------------------------------------------------

struct NetFaults {
  Duration min_latency = Duration::micros(100);
  Duration max_latency = Duration::millis(2);

  Chance drop = Chance::never();
  Chance duplicate = Chance::never();
  Chance reset = Chance::never();  // connection torn down mid-conversation

  // When false, deliveries on one connection keep FIFO order, which is what a
  // TCP stream does. Enabling reordering models UDP or a connection-per-request
  // client; it is off by default because a simulator that reports failures the
  // production transport cannot produce gets switched off within a fortnight.
  bool allow_reorder = false;

  // 0 means unlimited. Otherwise deliveries on a link queue behind each other
  // at this rate, so a large message delays the small one behind it -- which is
  // how snapshot transfer starves heartbeats in real clusters.
  std::uint64_t bandwidth_bytes_per_sec = 0;
};

enum class PartitionStyle : std::uint8_t {
  kNone,
  kOneShot,            // one partition, heals after a while
  kFlapping,           // repeatedly splits and heals
  kAsymmetric,         // A can reach B, B cannot reach A
  kMajorityIsolating,  // one node cut off from everyone else
  kHalfOpen,           // sends succeed, nothing is ever delivered
};

const char* to_string(PartitionStyle style) noexcept;

struct PartitionFaults {
  PartitionStyle style = PartitionStyle::kNone;
  Duration first_at = Duration::seconds(1);
  Duration duration = Duration::seconds(3);
  Duration period = Duration::seconds(5);  // flapping only
};

// ---------------------------------------------------------------------------
// Disk
// ---------------------------------------------------------------------------

// How an unsynced sector resolves when the machine dies underneath it. All
// three outcomes are real; the third is the one people forget, and it is the
// reason a checksum is not optional.
enum class TornResolution : std::uint8_t { kOldContent, kNewContent, kTorn };

struct DiskFaults {
  Duration min_latency = Duration::micros(50);
  Duration max_latency = Duration::micros(500);
  Duration fsync_latency = Duration::millis(1);

  // With the page cache on, a write is only durable once fsync returns. Turning
  // it off makes every write instantly durable, which is the v0 behaviour and
  // is useful only as a control: it is the configuration in which a missing
  // fsync is undetectable.
  bool page_cache = true;

  // Given an unsynced sector at crash time, the odds it tears rather than
  // resolving cleanly to the old or the new content.
  Chance torn_write = Chance::pct(20);

  Chance io_error = Chance::never();   // EIO on any single operation
  Chance slow_io = Chance::never();    // a latency spike
  Duration slow_io_penalty = Duration::millis(250);

  Chance bit_rot = Chance::never();    // per scheduled scrub event
  std::uint64_t capacity_bytes = 0;    // 0 == unlimited; otherwise ENOSPC
};

// ---------------------------------------------------------------------------
// Clock
// ---------------------------------------------------------------------------

struct ClockFaults {
  // What each node's clock is actually allowed to be wrong by.
  Duration max_offset = Duration{0};
  std::int64_t max_drift_ppm = 0;  // parts per million, signed

  Chance jump = Chance::never();  // an NTP step correction
  Duration max_jump = Duration::millis(500);

  Chance freeze = Chance::never();  // clock stops; the node does not notice
  Duration max_freeze = Duration::millis(200);

  // What the system is *told* the bound is. Every strict-serializability
  // argument rests on this being true.
  Duration declared_uncertainty = Duration::millis(10);

  // Deliberately let the real offset exceed the declared bound. This is not a
  // bug in the simulator, it is an experiment: "is this system safe under its
  // stated assumptions" and "what does it do when the assumption is false" are
  // different questions, and the second one is the interesting one.
  bool violate_declared_bound = false;
};

// ---------------------------------------------------------------------------
// Process
// ---------------------------------------------------------------------------

struct ProcessFaults {
  // Per node, per simulated second.
  Chance crash_per_second = Chance::never();
  Chance pause_per_second = Chance::never();

  Duration min_restart_delay = Duration::millis(50);
  Duration max_restart_delay = Duration::seconds(2);

  // A pause is a VM freeze or a stop-the-world collection: the node is alive,
  // keeps its memory, and loses no data -- it simply stops running for a while
  // and then carries on believing no time has passed. This is how leases get
  // violated in production, and it is much harder to survive than a crash.
  Duration min_pause = Duration::millis(10);
  Duration max_pause = Duration::seconds(5);
};

// ---------------------------------------------------------------------------
// The whole adversary
// ---------------------------------------------------------------------------

struct FaultProfile {
  NetFaults net;
  PartitionFaults partition;
  DiskFaults disk;
  ClockFaults clock;
  ProcessFaults process;

  // No faults at all. The control condition: if a test fails under this, the
  // bug is in the protocol or the harness, not in the adversary.
  static FaultProfile none() noexcept;

  // Draw an adversary from the seed. Roughly one run in eight is drawn benign
  // on each axis, so the corpus keeps covering the easy configurations too --
  // a fleet where every run is maximally hostile never exercises the fast paths
  // where a surprising number of bugs actually live.
  static FaultProfile draw(std::uint64_t seed);

  std::uint64_t hash() const noexcept;
};

}  // namespace anvil::sim

#endif  // ANVIL_SIM_FAULT_PROFILE_H_
