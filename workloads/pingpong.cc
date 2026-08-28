#include "workloads/pingpong.h"

#include <cstring>

#include "anvil/core/digest.h"

namespace anvil::workloads {
namespace {

std::uint64_t token_of(const Message& msg) {
  std::uint64_t token = 0;
  if (msg.payload.size() >= sizeof(token)) {
    std::memcpy(&token, msg.payload.data(), sizeof(token));
  }
  return token;
}

std::vector<std::byte> encode_token(std::uint64_t token) {
  std::vector<std::byte> payload(sizeof(token));
  std::memcpy(payload.data(), &token, sizeof(token));
  return payload;
}

// Folds one hop into the running checksum. Order-sensitive by construction, so
// two runs that performed the same hops in a different order do not agree.
void record_hop(PingPongState* state, NodeId node, std::uint64_t token, std::uint64_t hop) {
  Digest d;
  d.mix(state->checksum).mix(node).mix(token).mix(hop);
  state->checksum = d.low();
}

Task<void> forwarder(Runtime& rt, PingPongConfig cfg, PingPongState* state, NodeId pred,
                     NodeId succ, bool is_origin) {
  ConnHandle in{};
  ConnHandle out{};
  co_await rt.connect(pred, &in);
  co_await rt.connect(succ, &out);

  auto& rng = rt.rng(RandomDomain::kWorkload);

  for (;;) {
    Message msg;
    const Status status = co_await rt.recv(in, &msg);
    if (!status.is_ok()) co_return;

    // Once the origin has counted enough laps, everything in flight is dropped
    // on arrival. The forwarders then park on recv forever, which is what a
    // quiesced distributed system looks like from the inside -- not an error.
    if (state->done) continue;

    const std::uint64_t token = token_of(msg);
    const std::uint64_t hop = msg.correlation;
    record_hop(state, rt.self(), token, hop);

    if (is_origin) {
      ++state->laps_completed;
      if (state->laps_completed >= cfg.laps) {
        state->done = true;
        continue;
      }
    }

    co_await rt.sleep_for(rng.uniform_duration(cfg.think_min, cfg.think_max));
    if (state->done) continue;

    Message forward;
    forward.kind = MessageKind::kClientRequest;
    forward.correlation = hop + 1;
    forward.payload = encode_token(token);
    co_await rt.send(out, std::move(forward));
    ++state->forwards;
  }
}

Task<void> injector(Runtime& rt, PingPongConfig cfg, PingPongState* state, NodeId succ) {
  ConnHandle out{};
  co_await rt.connect(succ, &out);

  for (std::uint32_t i = 0; i < cfg.tokens; ++i) {
    co_await rt.sleep_for(cfg.inject_stagger);
    if (state->done) co_return;

    Message msg;
    msg.kind = MessageKind::kClientRequest;
    msg.correlation = 1;
    msg.payload = encode_token(i + 1);
    co_await rt.send(out, std::move(msg));
    ++state->forwards;
  }
}

Task<void> heartbeat(Runtime& rt, PingPongConfig cfg, PingPongState* state) {
  // Contributes no messages, only timer events. Its job is to make sure timers
  // and deliveries compete for the same instants, so the scheduler's tie-break
  // between them is exercised rather than assumed.
  while (!state->done) {
    co_await rt.sleep_for(cfg.heartbeat);
    ++state->heartbeats;
  }
}

}  // namespace

void install(sim::Simulation& simulation, PingPongConfig config, PingPongState* state) {
  const std::uint32_t n = simulation.node_count();
  if (n < 2) throw sim::SimulationPanic("pingpong needs at least two nodes");

  for (std::uint32_t i = 1; i <= n; ++i) {
    const NodeId self{i};
    const NodeId pred{i == 1 ? n : i - 1};
    const NodeId succ{i == n ? 1 : i + 1};
    const bool is_origin = (i == 1);

    Runtime& rt = simulation.node(self);
    rt.spawn(forwarder(rt, config, state, pred, succ, is_origin));
    rt.spawn(heartbeat(rt, config, state));
    if (is_origin) rt.spawn(injector(rt, config, state, succ));
  }
}

}  // namespace anvil::workloads
