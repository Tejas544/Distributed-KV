#include "anvil/core/txn/timestamp.h"

#include <algorithm>

#include "anvil/core/lsm/format.h"

namespace anvil::txn {

const char* to_string(TsSource source) noexcept {
  switch (source) {
    case TsSource::kOracle: return "oracle";
    case TsSource::kHybrid: return "hybrid-logical";
  }
  return "?";
}

// ---------------------------------------------------------------------------
// the oracle
// ---------------------------------------------------------------------------

std::string encode_reservation(std::uint64_t count) {
  std::string out;
  lsm::put_varint64(&out, count);
  return out;
}

bool decode_reservation(std::string_view in, std::uint64_t* count) {
  const char* p = in.data();
  const char* limit = p + in.size();
  p = lsm::get_varint64(p, limit, count);
  return p != nullptr;
}

void OracleMachine::apply(LogIndex index, std::string_view command) {
  (void)index;
  std::uint64_t count = 0;
  if (!decode_reservation(command, &count)) return;
  // Saturating, not wrapping. A timestamp that wraps is a timestamp that
  // regresses, and the whole point of this machine is that it never does.
  const Ts first = state_.high_water + 1;
  if (count > kMaxTs - state_.high_water) {
    state_.high_water = kMaxTs;
  } else {
    state_.high_water += count;
  }
  ++reservations_;
  ++revision_;
  if (on_reserved_) on_reserved_(first, count);
}

std::string OracleMachine::snapshot() const {
  std::string out;
  lsm::put_varint64(&out, state_.high_water);
  return out;
}

void OracleMachine::restore(std::string_view data) {
  const char* p = data.data();
  const char* limit = p + data.size();
  Ts high = kMinTs;
  if (lsm::get_varint64(p, limit, &high) == nullptr) return;
  // Never downward. A snapshot is a statement about the past, and this field is
  // a high-water mark: restoring an older one is exactly the regression the
  // machine exists to prevent, and it would happen on a node that receives a
  // stale snapshot from a leader that has since been superseded.
  state_.high_water = std::max(state_.high_water, high);
  ++revision_;
}

// ---------------------------------------------------------------------------
// the hybrid logical clock
// ---------------------------------------------------------------------------

Ts HybridClock::now(Timestamp wall) {
  const Ts physical = pack_hlc(wall.physical, 0);
  if (physical > last_) {
    last_ = physical;
  } else {
    // The clock has not advanced, or has gone backwards -- an NTP step, a
    // frozen VM, or simply two calls inside one simulated nanosecond. The
    // logical counter is what keeps the sequence strictly increasing without
    // pretending the physical clock did something it did not.
    ++last_;
  }
  return last_;
}

Ts HybridClock::observe(Ts remote, Timestamp wall) {
  const Ts local = now(wall);
  if (remote >= last_) {
    // Someone else's clock is ahead of ours. Adopting it is what makes the HLC
    // capture happens-before: a message carries the sender's time, and the
    // receiver's next timestamp is above it, so causality is in the numbers
    // rather than in the physics.
    last_ = remote + 1;
  }
  return std::max(local, last_);
}

TsInterval HybridClock::interval(Ts ts) const {
  const std::uint64_t bound =
      static_cast<std::uint64_t>(uncertainty_.nanos() < 0 ? 0 : uncertainty_.nanos());
  const Ts span = pack_hlc(bound, 0);
  TsInterval out;
  out.earliest = ts > span ? ts - span : kMinTs;
  out.latest = ts > kMaxTs - span ? kMaxTs : ts + span;
  return out;
}

}  // namespace anvil::txn
