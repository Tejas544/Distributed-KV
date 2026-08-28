// The simulated network, with faults armed.
//
// Every message crosses four decision points, and each one is a fault the
// protocol above must survive:
//
//   1. Is the link usable?   down (send fails), half-open (send succeeds, the
//                            message evaporates), or up.
//   2. Is it dropped?        an independent per-message loss roll.
//   3. Is it duplicated?     if so, the copy gets its own independent latency,
//                            so the duplicate can overtake the original.
//   4. When does it arrive?  latency, plus queueing if the link has a bandwidth
//                            cap, clamped to preserve FIFO unless reordering is
//                            enabled.
//
// Links are directed. That is not a detail -- it is what makes asymmetric
// partitions expressible, and asymmetric partitions are the single most
// productive network fault there is. A node that can send but not receive still
// looks alive to itself, keeps asserting leadership, and drives exactly the
// split-brain scenarios that symmetric partitions never reach.
//
// Half-open deserves its own state rather than being folded into "down",
// because the two are distinguishable *by the protocol*: a down link fails the
// send, so the sender learns something; a half-open link accepts everything and
// tells the sender nothing. Code that treats a successful send as evidence of
// delivery only breaks on the second one.

#ifndef ANVIL_SIM_NET_MODEL_H_
#define ANVIL_SIM_NET_MODEL_H_

#include <coroutine>
#include <cstdint>
#include <deque>
#include <map>

#include "anvil/core/random.h"
#include "anvil/core/runtime/runtime.h"
#include "anvil/core/types.h"
#include "anvil/sim/fault_profile.h"
#include "anvil/sim/scheduler.h"

namespace anvil::sim {

enum class LinkState : std::uint8_t {
  kUp,
  kDown,      // send() fails; the sender finds out
  kHalfOpen,  // send() succeeds; nothing is ever delivered
};

struct NetStats {
  std::uint64_t sent = 0;
  std::uint64_t delivered = 0;
  std::uint64_t dropped_by_loss = 0;
  std::uint64_t dropped_by_partition = 0;
  std::uint64_t dropped_by_half_open = 0;
  std::uint64_t duplicated = 0;
  std::uint64_t reset = 0;
  std::uint64_t reordered = 0;
  std::uint64_t send_failures = 0;
  std::uint64_t bandwidth_delays = 0;
};

class NetworkModel {
 public:
  NetworkModel(Scheduler* scheduler, std::uint64_t seed, NetFaults faults);

  // A ConnHandle is the ordered pair (self, peer) packed into 64 bits.
  static ConnHandle make_handle(NodeId self, NodeId peer) noexcept;
  static NodeId owner_of(ConnHandle conn) noexcept;
  static NodeId peer_of(ConnHandle conn) noexcept;

  void connect(NodeId self, NodeId peer);
  Status send(NodeId self, NodeId peer, Message msg);
  bool try_recv(NodeId self, NodeId peer, Message* out);
  void park_receiver(NodeId self, NodeId peer, std::coroutine_handle<> waiter);
  void close(NodeId self, NodeId peer);

  // ---- fault control -----------------------------------------------------
  void set_link(NodeId from, NodeId to, LinkState state);
  LinkState link(NodeId from, NodeId to) const;

  // Restores every link. Note that this does NOT stop per-message loss: drop,
  // duplicate and reset are properties of the model, not of the link table.
  void heal_all();

  // Stops every per-message fault. Separate from heal_all() because the two
  // mean different things, and conflating them made "healed" a lie during the
  // first fault sweep: links came back while messages kept vanishing, so
  // eventual-synchrony liveness could never be established.
  void stop_injecting();

  // Everything a crashed node had in flight or buffered. In-flight messages
  // *to* the node are dropped by the scheduler when its events are purged;
  // this clears what had already landed in its inbox and any parked receiver,
  // whose coroutine frame is about to be destroyed.
  void reset_node(NodeId node);

  const NetStats& stats() const noexcept { return stats_; }

 private:
  struct EndpointKey {
    NodeId to;
    NodeId from;
    friend auto operator<=>(const EndpointKey&, const EndpointKey&) noexcept = default;
  };

  struct Endpoint {
    std::deque<Message> inbox;
    std::coroutine_handle<> waiter{};
    Timestamp last_delivery;   // FIFO clamp
    Timestamp link_free_at;    // bandwidth queueing
  };

  Endpoint& endpoint(EndpointKey key) { return endpoints_[key]; }
  Duration schedule_delivery_delay(Endpoint& dst, std::size_t bytes);
  void enqueue_delivery(EndpointKey key, Message msg, Duration delay, std::uint64_t cause);

  Scheduler* scheduler_;
  DeterministicRandom rng_;
  NetFaults faults_;
  std::map<EndpointKey, Endpoint> endpoints_;
  std::map<EndpointKey, LinkState> links_;  // keyed {to, from} for symmetry with endpoints
  NetStats stats_;
};

}  // namespace anvil::sim

#endif  // ANVIL_SIM_NET_MODEL_H_
