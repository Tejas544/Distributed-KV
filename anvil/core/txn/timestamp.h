// Where transaction timestamps come from, and what they promise.
//
// Two sources, because they promise different things and the difference is the
// whole of P6's clock story:
//
//   kOracle   a replicated timestamp oracle. One Raft group hands out
//             monotonically increasing integers. They are totally ordered by
//             construction and mean nothing about real time -- two transactions
//             an hour apart get adjacent numbers if nothing happened in
//             between. That is enough for snapshot isolation and for
//             serializability, and not enough for external consistency.
//
//   kHybrid   a hybrid logical clock per node, with an uncertainty interval.
//             A timestamp is a *reading of a real clock* plus a logical tiebreak,
//             so it carries meaning about real time -- but only within the bound
//             the configuration declares, which is why every timestamp from this
//             source arrives with an interval rather than a point.
//
// The oracle exists so that SI and SSI can be tested without the clock in the
// way; the HLC exists because commit-wait cannot work without a clock that
// claims to bound its own error. Running both, against the same engines, is
// what makes "the level is the mechanism, not the marketing" checkable.
//
// The one property that matters for the oracle, and it is INV-TXN-09: allocated
// timestamps never regress, *including across leader failover and restart*. A
// TSO that hands out numbers from memory and persists nothing is correct until
// the first crash, at which point it re-issues timestamps that transactions
// have already committed at -- and the version order of the whole database
// stops being a total order. The mechanism is a reservation: the leader commits
// "I am taking the next N" through Raft before handing out any of them, so a new
// leader starts above every number the old one could possibly have issued.

#ifndef ANVIL_CORE_TXN_TIMESTAMP_H_
#define ANVIL_CORE_TXN_TIMESTAMP_H_

#include <cstdint>
#include <functional>
#include <string>

#include "anvil/core/raft/driver.h"
#include "anvil/core/types.h"

namespace anvil::txn {

// The transaction timestamp. A plain integer, totally ordered, and deliberately
// not `anvil::Timestamp`: an MVCC version needs a total order and nothing else,
// and giving it a physical component invites code that compares it against a
// clock reading it has no business comparing against.
using Ts = std::uint64_t;

inline constexpr Ts kMinTs = 1;
inline constexpr Ts kMaxTs = ~static_cast<Ts>(0);

// What a timestamp source says about real time. `earliest == latest` for the
// oracle, which makes no claim at all; the HLC returns a real interval whose
// width is the declared clock bound.
struct TsInterval {
  Ts earliest = 0;
  Ts latest = 0;

  Ts width() const noexcept { return latest > earliest ? latest - earliest : 0; }
};

enum class TsSource : std::uint8_t {
  kOracle,  // replicated, totally ordered, says nothing about real time
  kHybrid,  // HLC with an uncertainty interval
};

const char* to_string(TsSource source) noexcept;

// ---------------------------------------------------------------------------
// the replicated oracle
// ---------------------------------------------------------------------------

// The oracle's replicated state: one number. `high_water` is the highest
// timestamp any leader has *reserved*, which is at or above the highest any
// leader has issued. That inequality is the safety property.
struct OracleState {
  Ts high_water = kMinTs;
};

struct OracleOptions {
  // How many timestamps a leader reserves per Raft round. Larger is faster and
  // wastes more on failover; the waste is harmless because the sequence only
  // has to be monotone, not dense.
  std::uint64_t batch = 64;

  // false: the leader hands out timestamps from memory and lets the reservation
  // catch up behind it. Faster, and it re-issues timestamps that committed
  // transactions already used the first time the leader changes. INV-TXN-09.
  bool reserve_before_issuing = true;
};

// The oracle's state machine. Lives in its own Raft group so that its failover
// story is the ordinary Raft one and needs no special case.
class OracleMachine : public raft::StateMachine {
 public:
  void apply(LogIndex index, std::string_view command) override;
  std::string snapshot() const override;
  void restore(std::string_view data) override;

  // Invoked on every applied reservation with the half-open range it took.
  // The leader answers the waiting request from here rather than from the
  // propose call, because a reservation is only real once it has committed --
  // handing out a number before that is the whole of ANV's INV-TXN-09 story.
  using ReservedCallback = std::function<void(Ts first, std::uint64_t count)>;
  void set_reserved_callback(ReservedCallback callback) { on_reserved_ = std::move(callback); }

  Ts high_water() const noexcept { return state_.high_water; }
  std::uint64_t revision() const noexcept { return revision_; }
  std::uint64_t reservations() const noexcept { return reservations_; }

 private:
  OracleState state_;
  ReservedCallback on_reserved_;
  std::uint64_t revision_ = 0;
  std::uint64_t reservations_ = 0;
};

// A reservation command: "take the next `count`".
std::string encode_reservation(std::uint64_t count);
bool decode_reservation(std::string_view in, std::uint64_t* count);

// ---------------------------------------------------------------------------
// the hybrid logical clock
// ---------------------------------------------------------------------------

// An HLC over the node's own runtime clock.
//
// The logical component exists so that two timestamps taken in the same
// nanosecond are still ordered, and so that a timestamp received from another
// node drags this node's clock forward -- which is what makes the HLC capture
// happens-before rather than merely approximating it.
class HybridClock {
 public:
  explicit HybridClock(Duration uncertainty) : uncertainty_(uncertainty) {}

  // A fresh timestamp from this node's clock reading.
  Ts now(Timestamp wall);

  // Fold in a timestamp received from elsewhere. Returns the new local value.
  Ts observe(Ts remote, Timestamp wall);

  // The interval this node is willing to claim: [ts - bound, ts + bound] in the
  // reading direction that matters. A read at `ts` must treat anything it finds
  // in (ts, ts + bound] as possibly-earlier-in-real-time, which is INV-TXN-07.
  TsInterval interval(Ts ts) const;

  Duration uncertainty() const noexcept { return uncertainty_; }
  Ts last() const noexcept { return last_; }

 private:
  Duration uncertainty_;
  Ts last_ = kMinTs;
};

// The packing is explicit rather than implied: 48 bits of nanoseconds and 16 of
// logical counter, so a timestamp is one integer everywhere and the components
// can still be recovered for a diagnostic. 48 bits of nanoseconds is about
// three days of simulated time, which is more than any run.
constexpr Ts pack_hlc(std::uint64_t nanos, std::uint16_t logical) noexcept {
  return (nanos << 16) | logical;
}
constexpr std::uint64_t hlc_nanos(Ts ts) noexcept { return ts >> 16; }
constexpr std::uint16_t hlc_logical(Ts ts) noexcept {
  return static_cast<std::uint16_t>(ts & 0xFFFF);
}

}  // namespace anvil::txn

#endif  // ANVIL_CORE_TXN_TIMESTAMP_H_
