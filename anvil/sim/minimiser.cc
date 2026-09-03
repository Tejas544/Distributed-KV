#include "anvil/sim/minimiser.h"

#include <algorithm>

namespace anvil::sim {
namespace {

bool armed(const Chance& c) noexcept { return c.possible(); }

}  // namespace

const char* to_string(FaultFeature feature) noexcept {
  switch (feature) {
    case FaultFeature::kNetDrop: return "net.drop";
    case FaultFeature::kNetDuplicate: return "net.duplicate";
    case FaultFeature::kNetReset: return "net.reset";
    case FaultFeature::kNetReorder: return "net.reorder";
    case FaultFeature::kNetBandwidth: return "net.bandwidth";
    case FaultFeature::kPartition: return "partition";
    case FaultFeature::kDiskTornWrite: return "disk.torn_write";
    case FaultFeature::kDiskIoError: return "disk.io_error";
    case FaultFeature::kDiskSlowIo: return "disk.slow_io";
    case FaultFeature::kDiskBitRot: return "disk.bit_rot";
    case FaultFeature::kDiskNoSpace: return "disk.no_space";
    case FaultFeature::kClockSkew: return "clock.skew";
    case FaultFeature::kClockJump: return "clock.jump";
    case FaultFeature::kClockFreeze: return "clock.freeze";
    case FaultFeature::kClockBoundViolation: return "clock.bound_violation";
    case FaultFeature::kProcessCrash: return "process.crash";
    case FaultFeature::kProcessPause: return "process.pause";
    case FaultFeature::kBuggify: return "buggify";
    case FaultFeature::kCount: break;
  }
  return "?";
}

bool FaultSet::contains(FaultFeature f) const noexcept {
  return std::find(features.begin(), features.end(), f) != features.end();
}

std::string FaultSet::render() const {
  if (features.empty()) return "none";
  std::string out;
  for (const FaultFeature f : features) {
    if (!out.empty()) out += " + ";
    out += to_string(f);
  }
  return out;
}

std::vector<FaultFeature> features_of(const FaultProfile& p, const BuggifyConfig& buggify) {
  std::vector<FaultFeature> out;
  const auto add = [&out](bool present, FaultFeature f) {
    if (present) out.push_back(f);
  };

  add(armed(p.net.drop), FaultFeature::kNetDrop);
  add(armed(p.net.duplicate), FaultFeature::kNetDuplicate);
  add(armed(p.net.reset), FaultFeature::kNetReset);
  add(p.net.allow_reorder, FaultFeature::kNetReorder);
  add(p.net.bandwidth_bytes_per_sec != 0, FaultFeature::kNetBandwidth);
  add(p.partition.style != PartitionStyle::kNone, FaultFeature::kPartition);

  add(armed(p.disk.torn_write), FaultFeature::kDiskTornWrite);
  add(armed(p.disk.io_error), FaultFeature::kDiskIoError);
  add(armed(p.disk.slow_io), FaultFeature::kDiskSlowIo);
  add(armed(p.disk.bit_rot), FaultFeature::kDiskBitRot);
  add(p.disk.capacity_bytes != 0, FaultFeature::kDiskNoSpace);

  // Skew is one feature (offset and drift are the same physical claim: this
  // node's clock does not agree with that one). The declared bound is not a
  // fault -- it is what the system was told -- so it is preserved rather than
  // minimised away, and only *exceeding* it is a feature.
  add(p.clock.max_offset.nanos() != 0 || p.clock.max_drift_ppm != 0, FaultFeature::kClockSkew);
  add(armed(p.clock.jump), FaultFeature::kClockJump);
  add(armed(p.clock.freeze), FaultFeature::kClockFreeze);
  add(p.clock.violate_declared_bound, FaultFeature::kClockBoundViolation);

  add(armed(p.process.crash_per_second), FaultFeature::kProcessCrash);
  add(armed(p.process.pause_per_second), FaultFeature::kProcessPause);

  add(buggify.enable_pct != 0 && buggify.fire_pct != 0, FaultFeature::kBuggify);
  return out;
}

FaultSet restrict_to(const FaultProfile& original, const BuggifyConfig& buggify,
                     const std::vector<FaultFeature>& keep) {
  FaultSet out;
  out.features = keep;
  std::sort(out.features.begin(), out.features.end());
  out.features.erase(std::unique(out.features.begin(), out.features.end()), out.features.end());

  // Start from the control condition, then re-arm only what was kept. Building
  // it this way round rather than by clearing knobs means a knob added to
  // FaultProfile later is disabled by default here instead of silently
  // surviving every minimisation.
  out.profile = FaultProfile::none();

  // Latencies and restart/pause durations describe the machine, not the
  // adversary. A minimised run on a zero-latency network would be a different
  // system, and a fault that only reproduces at 40ms would look non-minimal.
  out.profile.net.min_latency = original.net.min_latency;
  out.profile.net.max_latency = original.net.max_latency;
  out.profile.disk.min_latency = original.disk.min_latency;
  out.profile.disk.max_latency = original.disk.max_latency;
  out.profile.disk.fsync_latency = original.disk.fsync_latency;
  out.profile.disk.page_cache = original.disk.page_cache;
  out.profile.clock.declared_uncertainty = original.clock.declared_uncertainty;
  out.profile.process.min_restart_delay = original.process.min_restart_delay;
  out.profile.process.max_restart_delay = original.process.max_restart_delay;
  out.profile.process.min_pause = original.process.min_pause;
  out.profile.process.max_pause = original.process.max_pause;
  out.profile.disk.slow_io_penalty = original.disk.slow_io_penalty;
  out.profile.clock.max_jump = original.clock.max_jump;
  out.profile.clock.max_freeze = original.clock.max_freeze;

  out.buggify.enable_pct = 0;
  out.buggify.fire_pct = 0;

  const auto has = [&out](FaultFeature f) { return out.contains(f); };

  if (has(FaultFeature::kNetDrop)) out.profile.net.drop = original.net.drop;
  if (has(FaultFeature::kNetDuplicate)) out.profile.net.duplicate = original.net.duplicate;
  if (has(FaultFeature::kNetReset)) out.profile.net.reset = original.net.reset;
  if (has(FaultFeature::kNetReorder)) out.profile.net.allow_reorder = true;
  if (has(FaultFeature::kNetBandwidth)) {
    out.profile.net.bandwidth_bytes_per_sec = original.net.bandwidth_bytes_per_sec;
  }
  if (has(FaultFeature::kPartition)) out.profile.partition = original.partition;

  if (has(FaultFeature::kDiskTornWrite)) out.profile.disk.torn_write = original.disk.torn_write;
  if (has(FaultFeature::kDiskIoError)) out.profile.disk.io_error = original.disk.io_error;
  if (has(FaultFeature::kDiskSlowIo)) out.profile.disk.slow_io = original.disk.slow_io;
  if (has(FaultFeature::kDiskBitRot)) out.profile.disk.bit_rot = original.disk.bit_rot;
  if (has(FaultFeature::kDiskNoSpace)) {
    out.profile.disk.capacity_bytes = original.disk.capacity_bytes;
  }

  if (has(FaultFeature::kClockSkew)) {
    out.profile.clock.max_offset = original.clock.max_offset;
    out.profile.clock.max_drift_ppm = original.clock.max_drift_ppm;
  }
  if (has(FaultFeature::kClockJump)) out.profile.clock.jump = original.clock.jump;
  if (has(FaultFeature::kClockFreeze)) out.profile.clock.freeze = original.clock.freeze;
  if (has(FaultFeature::kClockBoundViolation)) {
    // Exceeding the declared bound is only meaningful if there is an offset to
    // exceed it with, so this feature carries the offset whether or not
    // kClockSkew survived. Without that, dropping kClockSkew would silently
    // disarm kClockBoundViolation too and the pair would never be separable.
    out.profile.clock.violate_declared_bound = true;
    out.profile.clock.max_offset = original.clock.max_offset;
  }

  if (has(FaultFeature::kProcessCrash)) {
    out.profile.process.crash_per_second = original.process.crash_per_second;
  }
  if (has(FaultFeature::kProcessPause)) {
    out.profile.process.pause_per_second = original.process.pause_per_second;
  }
  if (has(FaultFeature::kBuggify)) out.buggify = buggify;

  return out;
}

namespace {

// Complement of `subset` within `whole`, both sorted.
std::vector<FaultFeature> complement(const std::vector<FaultFeature>& whole,
                                     const std::vector<FaultFeature>& subset) {
  std::vector<FaultFeature> out;
  std::set_difference(whole.begin(), whole.end(), subset.begin(), subset.end(),
                      std::back_inserter(out));
  return out;
}

// The i'th of n near-equal contiguous slices.
std::vector<FaultFeature> slice(const std::vector<FaultFeature>& c, std::size_t i, std::size_t n) {
  const std::size_t begin = (c.size() * i) / n;
  const std::size_t end = (c.size() * (i + 1)) / n;
  return std::vector<FaultFeature>(c.begin() + static_cast<std::ptrdiff_t>(begin),
                                   c.begin() + static_cast<std::ptrdiff_t>(end));
}

}  // namespace

MinimiseResult minimise(const FaultProfile& failing, const BuggifyConfig& buggify,
                        const Reproduces& reproduces, MinimiseOptions options) {
  if (options.attempts == 0) options.attempts = 1;

  MinimiseResult result;
  std::vector<FaultFeature> c = features_of(failing, buggify);
  std::sort(c.begin(), c.end());
  result.started_with = c.size();

  bool budget_spent = false;
  const auto test = [&](const std::vector<FaultFeature>& keep) -> bool {
    const FaultSet candidate = restrict_to(failing, buggify, keep);
    for (std::uint32_t attempt = 0; attempt < options.attempts; ++attempt) {
      if (options.max_runs != 0 && result.predicate_runs >= options.max_runs) {
        budget_spent = true;
        return false;
      }
      ++result.predicate_runs;
      if (reproduces(candidate, attempt)) return true;
    }
    return false;
  };

  // A minimiser handed a configuration that does not fail will happily reduce
  // it to nothing and report a triumphant "minimised 18 faults to 0". Establish
  // the premise instead of assuming it.
  if (!test(c)) {
    result.minimal = restrict_to(failing, buggify, c);
    result.ended_with = c.size();
    result.converged = false;
    return result;
  }

  // ---- ddmin -------------------------------------------------------------
  std::size_t n = 2;
  while (c.size() >= 2 && !budget_spent) {
    bool reduced = false;

    // Try each slice on its own: the aggressive move, and the one that makes
    // this converge in log time when the failure really does need only a
    // couple of faults.
    for (std::size_t i = 0; i < n && !budget_spent; ++i) {
      const std::vector<FaultFeature> part = slice(c, i, n);
      if (part.empty() || part.size() == c.size()) continue;
      if (test(part)) {
        c = part;
        n = 2;
        reduced = true;
        break;
      }
    }
    if (reduced) continue;
    if (budget_spent) break;

    // Then try dropping each slice: the move that makes progress when no single
    // slice is sufficient but some are unnecessary.
    for (std::size_t i = 0; i < n && !budget_spent; ++i) {
      const std::vector<FaultFeature> rest = complement(c, slice(c, i, n));
      if (rest.size() == c.size() || rest.empty()) continue;
      if (test(rest)) {
        c = rest;
        n = std::max<std::size_t>(n - 1, 2);
        reduced = true;
        break;
      }
    }
    if (reduced) continue;
    if (budget_spent) break;

    if (n >= c.size()) break;  // granularity exhausted: c is 1-minimal
    n = std::min(n * 2, c.size());
  }

  result.minimal = restrict_to(failing, buggify, c);
  result.ended_with = c.size();
  result.converged = !budget_spent;

  // ---- the closing check -------------------------------------------------
  //
  // ddmin terminating is not the same claim as "every one of these is
  // necessary", because a non-deterministic predicate can answer differently
  // the second time. Ask directly: drop each remaining feature and see whether
  // the failure survives without it.
  // A feature that turns out to be droppable is not merely a failed claim of
  // 1-minimality -- it is a smaller answer, already measured. Adopt it and check
  // again, so the result reported is the smallest set any pass reproduced with.
  // `verified_one_minimal` then means what it says: a whole pass ran over the
  // final set and every member was individually shown to be load-bearing.
  if (options.verify_one_minimal && result.converged) {
    bool verified = false;
    while (!budget_spent) {
      bool all_necessary = true;
      std::vector<FaultFeature> load_bearing;
      for (const FaultFeature f : c) {
        std::vector<FaultFeature> without;
        for (const FaultFeature g : c) {
          if (g != f) without.push_back(g);
        }
        if (budget_spent) break;
        if (test(without)) {
          c = without;         // strictly smaller, and it reproduced
          all_necessary = false;
          break;
        }
        load_bearing.push_back(f);
      }
      if (budget_spent) break;
      if (all_necessary) {
        result.load_bearing = load_bearing;
        verified = true;
        break;
      }
    }
    result.minimal = restrict_to(failing, buggify, c);
    result.ended_with = c.size();
    result.verified_one_minimal = verified && !budget_spent;
    result.converged = !budget_spent;
  }

  return result;
}

}  // namespace anvil::sim
