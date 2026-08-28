#include "workloads/counter.h"

#include <cstring>

#include "anvil/core/digest.h"

namespace anvil::workloads {
namespace {

constexpr std::uint64_t kMagic = 0xABCD'EF01'2345'6789ULL;
constexpr std::size_t kRecordSize = 24;  // magic | id | checksum
constexpr const char* kWalDir = "wal";

std::string wal_path(NodeId node) {
  std::string path = "wal/node-";
  std::uint64_t v = node.value();
  char buf[21];
  std::size_t n = 0;
  if (v == 0) buf[n++] = '0';
  while (v > 0) {
    buf[n++] = static_cast<char>('0' + (v % 10));
    v /= 10;
  }
  while (n > 0) path.push_back(buf[--n]);
  return path + ".log";
}

std::uint64_t checksum_for(std::uint64_t id) {
  Digest d;
  d.mix(kMagic).mix(id);
  return d.low();
}

void put_u64(std::byte* dst, std::uint64_t v) { std::memcpy(dst, &v, sizeof(v)); }
std::uint64_t get_u64(const std::byte* src) {
  std::uint64_t v = 0;
  std::memcpy(&v, src, sizeof(v));
  return v;
}

std::vector<std::byte> encode_record(std::uint64_t id) {
  std::vector<std::byte> record(kRecordSize);
  put_u64(record.data(), kMagic);
  put_u64(record.data() + 8, id);
  put_u64(record.data() + 16, checksum_for(id));
  return record;
}

Message make_message(MessageKind kind, std::uint64_t id) {
  Message m;
  m.kind = kind;
  m.correlation = id;
  return m;
}

// ---------------------------------------------------------------------------
// durability
// ---------------------------------------------------------------------------

// Reads the WAL and rebuilds the applied set, stopping at the first record that
// does not validate.
//
// Stopping rather than skipping is the important part. A torn write leaves a
// half-written record whose tail is garbage; everything after it is untrusted,
// because the device may have reordered. Scanning past a bad record and
// salvaging later ones would "recover" data that was never durably written --
// which looks like resilience and is actually corruption.
Task<Status> recover_wal(Runtime& rt, CounterConfig cfg, CounterState* st, NodeId self) {
  CounterNodeState& node = st->nodes[self.value()];
  node.applied.clear();
  node.wal_size = 0;

  OpenFlags flags = OpenFlags::kRead | OpenFlags::kWrite | OpenFlags::kCreate;
  FileHandle handle{};
  Status status = co_await rt.open(wal_path(self), flags, &handle);
  if (!status.is_ok()) {
    ++st->recover_open_failures;
    co_return status;
  }
  node.wal = handle;

  // The directory entry only becomes durable here. Skip this and the entire log
  // -- every acknowledged write in it -- evaporates on the next crash, no
  // matter how diligently its contents were fsynced.
  if (cfg.fsync_dir_on_create) {
    const Status dir = co_await rt.fsync_dir(kWalDir);
    if (!dir.is_ok()) {
      ++st->recover_dirsync_failures;
      co_return dir;
    }
  }

  std::uint64_t size = 0;
  status = co_await rt.file_size(handle, &size);
  if (!status.is_ok()) {
    ++st->recover_size_failures;
    co_return status;
  }

  std::vector<std::byte> buffer(kRecordSize);
  for (std::uint64_t offset = 0; offset + kRecordSize <= size; offset += kRecordSize) {
    std::size_t read = 0;
    status = co_await rt.pread(handle, MutableByteView{buffer.data(), buffer.size()}, offset,
                               &read);

    // A transient read error is NOT a corrupt record, and conflating the two is
    // a real bug: it truncates the log at an arbitrary point and turns an EIO
    // into permanent, silent data loss. Fail the whole recovery so the caller
    // retries instead.
    if (!status.is_ok()) {
      ++st->recover_read_failures;
      co_return status;
    }
    if (read < kRecordSize) break;

    const std::uint64_t id = get_u64(buffer.data() + 8);
    if (get_u64(buffer.data()) != kMagic || get_u64(buffer.data() + 16) != checksum_for(id)) {
      // A genuinely invalid record. Stop here rather than scanning past it:
      // everything after a torn write is untrustworthy, and salvaging later
      // records would "recover" data that was never durably written.
      ++st->corruption_detected;
      break;
    }
    node.applied.insert(id);
    node.wal_size = offset + kRecordSize;
  }

  // The finding. Anything this node promised must be here; if it is not,
  // durability was violated and the run has a real result to report.
  for (const std::uint64_t id : node.promised) {
    if (!node.applied.contains(id)) {
      ++st->lost_acked_writes;
      if (st->violations.size() < 8) {
        st->violations.push_back("node " + std::to_string(self.value()) +
                                 " lost promised increment " + std::to_string(id));
      }
    }
  }

  ++st->recoveries;
  node.ready = true;
  co_return Status::ok();
}

Task<Status> append_wal(Runtime& rt, CounterConfig cfg, CounterState* st, NodeId self,
                        std::uint64_t id) {
  CounterNodeState& node = st->nodes[self.value()];
  if (!node.ready) co_return Status{StatusCode::kUnavailable, "wal not open"};

  const std::vector<std::byte> record = encode_record(id);
  Status status = co_await rt.pwrite(node.wal, ByteView{record.data(), record.size()},
                                     node.wal_size);
  if (!status.is_ok()) co_return status;

  if (cfg.fsync_before_ack) {
    status = co_await rt.fsync(node.wal);
    if (!status.is_ok()) co_return status;
  }

  node.wal_size += kRecordSize;
  node.applied.insert(id);
  co_return Status::ok();
}

// ---------------------------------------------------------------------------
// follower
// ---------------------------------------------------------------------------

Task<void> follower_loop(Runtime& rt, CounterConfig cfg, CounterState* st, NodeId leader) {
  ConnHandle conn{};
  co_await rt.connect(leader, &conn);

  while (!st->done) {
    Message msg;
    if (!(co_await rt.recv(conn, &msg)).is_ok()) co_return;
    if (msg.kind != MessageKind::kAppendEntries) continue;

    const std::uint64_t id = msg.correlation;
    CounterNodeState& node = st->nodes[rt.self().value()];

    // Idempotent by id. Duplicates, retries and reordered pushes are all the
    // same thing to a follower, which is what makes the protocol survive the
    // network faults at all.
    if (!node.applied.contains(id)) {
      const Status status = co_await append_wal(rt, cfg, st, rt.self(), id);
      if (!status.is_ok()) continue;  // no ack: the leader will retry
      ++st->follower_applies;
    }

    // The follower is about to promise the leader this increment is durable.
    // Recording it here, before the ack goes out, is what makes a later
    // recovery able to prove the promise was kept.
    node.promised.insert(id);

    // Acknowledge even for a duplicate. A lost ack is indistinguishable from a
    // lost push, and refusing to re-acknowledge would wedge the leader forever.
    co_await rt.send(conn, make_message(MessageKind::kAppendEntriesReply, id));
  }
}

// ---------------------------------------------------------------------------
// leader
// ---------------------------------------------------------------------------

Task<void> client_loop(Runtime& rt, CounterConfig cfg, CounterState* st) {
  for (std::uint64_t id = 1; id <= cfg.increments; ++id) {
    // A client that resumes after a leader restart does not re-issue work the
    // leader already has. Skipping ids recovered from the WAL keeps a crash
    // from silently doubling the offered load, which would make the throughput
    // and progress numbers meaningless.
    if (st->nodes[rt.self().value()].applied.contains(id)) continue;

    co_await rt.sleep_for(cfg.client_interval);

    const Status status = co_await append_wal(rt, cfg, st, rt.self(), id);
    if (!status.is_ok()) {
      // The write failed outright, so nothing was promised. An indeterminate
      // outcome would be a different and much harder case -- that is what
      // INV-TXN-15 is about, and it arrives with real transactions.
      continue;
    }

    // The client is told the increment is durable. From here on, losing it is a
    // correctness violation rather than a retry.
    st->acked_ids.insert(id);
    st->nodes[rt.self().value()].promised.insert(id);
  }
}

Task<void> pusher(Runtime& rt, CounterConfig cfg, CounterState* st, NodeId follower,
                  std::uint32_t follower_index) {
  ConnHandle conn{};
  co_await rt.connect(follower, &conn);

  while (!st->done) {
    // Snapshot the outstanding ids before awaiting anything. client_loop runs
    // concurrently and inserts into `applied`, so iterating it across a suspend
    // point would invalidate the iterator -- a use-after-free that would only
    // fire on the seeds where a client write lands mid-push.
    std::vector<std::uint64_t> outstanding;
    {
      const CounterNodeState& node = st->nodes[rt.self().value()];
      for (const std::uint64_t id : node.applied) {
        if (!node.acked_by[follower_index].contains(id)) outstanding.push_back(id);
      }
    }

    for (const std::uint64_t id : outstanding) {
      // Send failures are ignored on purpose: a partition or reset tells the
      // leader nothing useful, and the retry loop is the answer to all of it.
      co_await rt.send(conn, make_message(MessageKind::kAppendEntries, id));
    }
    co_await rt.sleep_for(cfg.retry_interval);
  }
}

// Ends the run once everything has been replicated everywhere. Without it the
// pushers retry until the deadline and every seed costs a full max_time, which
// over a fleet is most of the compute budget spent on nothing.
Task<void> completion_monitor(Runtime& rt, CounterConfig cfg, CounterState* st,
                              std::uint32_t nodes) {
  while (!st->done) {
    co_await rt.sleep_for(cfg.retry_interval);
    if (st->acked_ids.size() < cfg.increments) continue;

    const CounterNodeState& node = st->nodes[rt.self().value()];
    bool complete = true;
    for (std::uint32_t f = 0; f + 2 <= nodes && complete; ++f) {
      for (const std::uint64_t id : st->acked_ids) {
        if (!node.acked_by[f].contains(id)) {
          complete = false;
          break;
        }
      }
    }
    if (complete) st->done = true;
  }
}

Task<void> ack_reader(Runtime& rt, CounterState* st, NodeId follower,
                      std::uint32_t follower_index) {
  ConnHandle conn{};
  co_await rt.connect(follower, &conn);

  while (!st->done) {
    Message msg;
    if (!(co_await rt.recv(conn, &msg)).is_ok()) co_return;
    if (msg.kind != MessageKind::kAppendEntriesReply) continue;
    st->nodes[rt.self().value()].acked_by[follower_index].insert(msg.correlation);
  }
}

// Recovery has to be retried, not abandoned.
//
// The first version gave up if the initial open or read returned EIO, and the
// node then stayed dark forever -- no crash to trigger another restart, no
// retry, permanently absent from the cluster. Seven seeds in the first sweep
// failed to converge for exactly this reason. A transient device error is not a
// reason to never come back, and this is the sort of thing that reads as
// obviously correct in review and is obviously wrong the moment an adversary
// returns EIO at the wrong moment.
Task<bool> recover_with_retry(Runtime& rt, CounterConfig cfg, CounterState* st) {
  // Unbounded, deliberately. A bounded retry count is a decision to give up and
  // stay dark forever, and there is no number that is right: whatever cap you
  // pick, some seed's error burst is one longer. The run's deadline is the
  // termination condition, which is the honest one.
  //
  // The `done` check goes *after* the first attempt, not before it. Checking it
  // first meant a node restarting near the end of a run skipped recovery
  // entirely -- and since booting clears the in-memory applied set, the node
  // came back empty and stayed that way. That turned 4 unconverged seeds into
  // 25 and simultaneously halved the seeded-bug detection rate, because
  // durability is only ever observed *through* recovery.
  // No `done` escape at all. Recovery has to keep going until it succeeds,
  // because booting clears the in-memory applied set: a node that abandons
  // recovery is not merely idle, it is a node that has forgotten its own data
  // and will never remember it. Termination comes from the run deadline, or
  // from another crash destroying this frame.
  for (;;) {
    if ((co_await recover_wal(rt, cfg, st, rt.self())).is_ok()) co_return true;
    ++st->boot_retries;
    co_await rt.sleep_for(Duration::millis(20));
  }
}

Task<void> boot_leader(Runtime& rt, CounterConfig cfg, CounterState* st, std::uint32_t nodes) {
  if (!(co_await recover_with_retry(rt, cfg, st))) co_return;

  rt.spawn(client_loop(rt, cfg, st));
  rt.spawn(completion_monitor(rt, cfg, st, nodes));
  for (std::uint32_t i = 2; i <= nodes; ++i) {
    rt.spawn(pusher(rt, cfg, st, NodeId{i}, i - 2));
    rt.spawn(ack_reader(rt, st, NodeId{i}, i - 2));
  }
}

Task<void> boot_follower(Runtime& rt, CounterConfig cfg, CounterState* st) {
  if (!(co_await recover_with_retry(rt, cfg, st))) co_return;
  rt.spawn(follower_loop(rt, cfg, st, NodeId{1}));
}

}  // namespace

void arm_invariants(sim::Simulation& simulation, CounterState* state) {
  auto& invariants = simulation.invariants();
  const std::uint32_t nodes = simulation.node_count();

  // A node mid-recovery has deliberately cleared its applied set and has not
  // rebuilt it yet, so every predicate here skips nodes that are not `ready`.
  // Without that guard these would fire on every single crash -- which is how
  // an invariant earns a reputation for false positives and gets disarmed.
  invariants.arm(
      "INV-CTR-01", "a node never promises more than it holds",
      checker::CostClass::kTick, [state, nodes]() -> std::optional<std::string> {
        for (std::uint32_t i = 1; i <= nodes; ++i) {
          const auto it = state->nodes.find(i);
          if (it == state->nodes.end() || !it->second.ready) continue;
          if (it->second.applied.size() < it->second.promised.size()) {
            return "node " + std::to_string(i) + " has promised " +
                   std::to_string(it->second.promised.size()) + " increments but holds only " +
                   std::to_string(it->second.applied.size());
          }
        }
        return std::nullopt;
      });

  invariants.arm(
      "INV-CTR-02", "every promise is durably held",
      checker::CostClass::kEpoch, [state, nodes]() -> std::optional<std::string> {
        for (std::uint32_t i = 1; i <= nodes; ++i) {
          const auto it = state->nodes.find(i);
          if (it == state->nodes.end() || !it->second.ready) continue;
          for (const std::uint64_t id : it->second.promised) {
            if (!it->second.applied.contains(id)) {
              return "node " + std::to_string(i) + " promised increment " +
                     std::to_string(id) + " and does not hold it";
            }
          }
        }
        return std::nullopt;
      });

  invariants.arm(
      "INV-CTR-03", "every node converges on every acknowledged increment",
      checker::CostClass::kQuiesce, [state, nodes]() -> std::optional<std::string> {
        for (std::uint32_t i = 1; i <= nodes; ++i) {
          const auto it = state->nodes.find(i);
          if (it == state->nodes.end()) {
            return "node " + std::to_string(i) + " has no recorded state at quiesce";
          }
          for (const std::uint64_t id : state->acked_ids) {
            if (!it->second.applied.contains(id)) {
              return "node " + std::to_string(i) + " is missing acknowledged increment " +
                     std::to_string(id) + " after the faults healed";
            }
          }
        }
        return std::nullopt;
      });
}

void install(sim::Simulation& simulation, CounterConfig config, CounterState* state) {
  const std::uint32_t nodes = simulation.node_count();
  if (nodes < 2) throw sim::SimulationPanic("counter workload needs at least two nodes");
  if (nodes > 9) throw sim::SimulationPanic("counter workload supports at most nine nodes");

  for (std::uint32_t i = 1; i <= nodes; ++i) {
    const NodeId self{i};
    Runtime& rt = simulation.node(self);
    state->nodes[i] = CounterNodeState{};

    // Re-invoked on every restart. A crash wipes the applied set and the open
    // handle; recovery rebuilds both from durable storage, or discovers that
    // durable storage did not keep what it promised.
    simulation.set_boot(self, [&rt, config, state, i, nodes]() {
      auto& node = state->nodes[i];
      node.ready = false;
      node.applied.clear();
      node.wal_size = 0;
      ++state->boots_started;
      // acked_by is volatile leader state. Carrying it across a restart would
      // leave the new incarnation believing followers hold data it can no
      // longer prove they have -- the leader would stop pushing, and a follower
      // that crashed in the same window would silently stay behind forever.
      for (auto& acks : node.acked_by) acks.clear();
      if (i == 1) {
        rt.spawn(boot_leader(rt, config, state, nodes));
      } else {
        rt.spawn(boot_follower(rt, config, state));
      }
    });
  }

  arm_invariants(simulation, state);

  // Boot everything for the first time.
  for (std::uint32_t i = 1; i <= nodes; ++i) {
    Runtime& rt = simulation.node(NodeId{i});
    if (i == 1) {
      rt.spawn(boot_leader(rt, config, state, nodes));
    } else {
      rt.spawn(boot_follower(rt, config, state));
    }
  }
}

bool converged(const sim::Simulation& simulation, const CounterState& state) {
  for (std::uint32_t i = 1; i <= simulation.node_count(); ++i) {
    const auto it = state.nodes.find(i);
    if (it == state.nodes.end()) return false;
    for (const std::uint64_t id : state.acked_ids) {
      if (!it->second.applied.contains(id)) return false;
    }
  }
  return true;
}

}  // namespace anvil::workloads
