#include "anvil/sim/net_model.h"

#include <utility>

namespace anvil::sim {

NetworkModel::NetworkModel(Scheduler* scheduler, std::uint64_t seed, NetFaults faults)
    : scheduler_(scheduler),
      rng_(DeterministicRandom{seed}.fork(RandomDomain::kNetwork)),
      faults_(faults) {}

ConnHandle NetworkModel::make_handle(NodeId self, NodeId peer) noexcept {
  return ConnHandle{(self.value() << 32) | (peer.value() & 0xFFFF'FFFFULL)};
}

NodeId NetworkModel::owner_of(ConnHandle conn) noexcept { return NodeId{conn.value() >> 32}; }

NodeId NetworkModel::peer_of(ConnHandle conn) noexcept {
  return NodeId{conn.value() & 0xFFFF'FFFFULL};
}

void NetworkModel::connect(NodeId self, NodeId peer) {
  endpoint({peer, self});
  endpoint({self, peer});
}

// ---------------------------------------------------------------------------
// links
// ---------------------------------------------------------------------------

void NetworkModel::set_link(NodeId from, NodeId to, LinkState state) {
  links_[EndpointKey{to, from}] = state;
}

LinkState NetworkModel::link(NodeId from, NodeId to) const {
  const auto it = links_.find(EndpointKey{to, from});
  return it == links_.end() ? LinkState::kUp : it->second;
}

void NetworkModel::heal_all() { links_.clear(); }

void NetworkModel::stop_injecting() {
  faults_.drop = Chance::never();
  faults_.duplicate = Chance::never();
  faults_.reset = Chance::never();
  // Latency and bandwidth stay as they were: a slow network is not a faulty
  // one, and a protocol that only makes progress on a fast link has a problem
  // worth keeping visible.
}

void NetworkModel::reset_node(NodeId node) {
  for (auto& [key, ep] : endpoints_) {
    if (key.to == node) {
      ep.inbox.clear();
      // The waiter's frame is about to be destroyed by the scheduler. Leaving
      // the handle here would let a later delivery resume freed memory.
      ep.waiter = {};
    }
    if (key.from == node) ep.inbox.clear();
  }
}

// ---------------------------------------------------------------------------
// timing
// ---------------------------------------------------------------------------

Duration NetworkModel::schedule_delivery_delay(Endpoint& dst, std::size_t bytes) {
  Duration latency = rng_.uniform_duration(faults_.min_latency, faults_.max_latency);
  Timestamp arrival = scheduler_->now().advanced_by(latency);

  if (faults_.bandwidth_bytes_per_sec > 0) {
    // Serialisation delay, queued behind whatever is already on the wire. This
    // is what makes a large snapshot transfer starve the heartbeats behind it,
    // which is a real and frequently fatal interaction.
    const auto transmit_nanos = static_cast<std::int64_t>(
        (static_cast<std::uint64_t>(bytes) * 1'000'000'000ULL) /
        faults_.bandwidth_bytes_per_sec);
    Timestamp start = arrival;
    if (dst.link_free_at > start) {
      start = dst.link_free_at;
      ++stats_.bandwidth_delays;
    }
    arrival = start.advanced_by(Duration{transmit_nanos});
    dst.link_free_at = arrival;
  }

  if (!faults_.allow_reorder) {
    // Hold FIFO per connection: a TCP stream does not reorder. Without this
    // clamp, two messages sent a microsecond apart across a 2 ms latency spread
    // would arrive in either order, and the simulator would report failures the
    // production transport cannot produce.
    if (arrival <= dst.last_delivery) arrival = dst.last_delivery.next_logical();
  } else if (arrival < dst.last_delivery) {
    ++stats_.reordered;
  }
  dst.last_delivery = arrival > dst.last_delivery ? arrival : dst.last_delivery;

  const auto now = scheduler_->now().physical;
  return Duration{static_cast<std::int64_t>(arrival.physical > now ? arrival.physical - now : 0)};
}

void NetworkModel::enqueue_delivery(EndpointKey key, Message msg, Duration delay,
                                    std::uint64_t cause) {
  scheduler_->at(
      delay, key.to, EventKind::kDeliver, "deliver",
      [this, key, message = std::move(msg)]() mutable {
        ++stats_.delivered;
        Endpoint& ep = endpoint(key);
        ep.inbox.push_back(std::move(message));
        if (ep.waiter) {
          // Hand the parked receiver back to the scheduler rather than resuming
          // it inline. Resuming here would run protocol code in the middle of a
          // delivery callback, nesting one event inside another and making the
          // trace's causal structure a lie.
          auto waiter = ep.waiter;
          ep.waiter = {};
          scheduler_->at(Duration{0}, key.to, EventKind::kResume, "recv_wake",
                         [waiter]() { waiter.resume(); });
        }
      },
      cause);
}

// ---------------------------------------------------------------------------
// send
// ---------------------------------------------------------------------------

Status NetworkModel::send(NodeId self, NodeId peer, Message msg) {
  ++stats_.sent;
  msg.from = self;
  msg.to = peer;

  // Content goes into the digest at send time, before any fault decision. This
  // is the point at which the *protocol* chose to do something; what the
  // network then does to it is the network's decision, and the scheduler
  // already digests those events.
  scheduler_->digest()
      .mix(self)
      .mix(peer)
      .mix(static_cast<std::uint64_t>(msg.kind))
      .mix(msg.correlation)
      .mix(ByteView{msg.payload.data(), msg.payload.size()});

  const EndpointKey key{peer, self};
  const std::size_t bytes = msg.payload.size();

  std::uint64_t send_event = 0;
  if (scheduler_->trace().recording()) {
    const TraceField fields[] = {{"to", peer.value()},
                                 {"kind", static_cast<std::uint64_t>(msg.kind)},
                                 {"corr", msg.correlation},
                                 {"bytes", bytes}};
    send_event =
        scheduler_->trace().emit(scheduler_->now(), self, EventKind::kSend, "send", fields);
  }

  // 1. link state
  switch (link(self, peer)) {
    case LinkState::kDown:
      ++stats_.dropped_by_partition;
      ++stats_.send_failures;
      return Status{StatusCode::kUnavailable, "network partition"};
    case LinkState::kHalfOpen:
      // The cruel one: the sender is told everything went fine.
      ++stats_.dropped_by_half_open;
      return Status::ok();
    case LinkState::kUp:
      break;
  }

  // 2. connection reset -- the sender learns the connection died, but has no
  //    way to know whether earlier messages on it were delivered.
  if (faults_.reset.roll(rng_)) {
    ++stats_.reset;
    ++stats_.send_failures;
    Endpoint& ep = endpoint(key);
    ep.inbox.clear();
    return Status{StatusCode::kUnavailable, "connection reset"};
  }

  // 3. loss
  if (faults_.drop.roll(rng_)) {
    ++stats_.dropped_by_loss;
    return Status::ok();  // successful send, silent loss: the common case
  }

  // 4. delivery, and possibly a duplicate with its own independent latency so
  //    it can overtake the original.
  const bool duplicate = faults_.duplicate.roll(rng_);
  Endpoint& dst = endpoint(key);

  Message copy;
  if (duplicate) copy = msg;

  enqueue_delivery(key, std::move(msg), schedule_delivery_delay(dst, bytes), send_event);

  if (duplicate) {
    ++stats_.duplicated;
    enqueue_delivery(key, std::move(copy), schedule_delivery_delay(dst, bytes), send_event);
  }
  return Status::ok();
}

bool NetworkModel::try_recv(NodeId self, NodeId peer, Message* out) {
  Endpoint& ep = endpoint({self, peer});
  if (ep.inbox.empty()) return false;
  *out = std::move(ep.inbox.front());
  ep.inbox.pop_front();
  return true;
}

void NetworkModel::park_receiver(NodeId self, NodeId peer, std::coroutine_handle<> waiter) {
  Endpoint& ep = endpoint({self, peer});
  if (ep.waiter) {
    throw SimulationPanic("two concurrent recv() calls on one connection");
  }
  ep.waiter = waiter;
  if (scheduler_->trace().recording()) {
    const TraceField fields[] = {{"peer", peer.value()}};
    scheduler_->trace().emit(scheduler_->now(), self, EventKind::kRecvPark, "recv_park", fields);
  }
}

void NetworkModel::close(NodeId self, NodeId peer) {
  Endpoint& ep = endpoint({self, peer});
  ep.inbox.clear();
  ep.waiter = {};
}

}  // namespace anvil::sim
