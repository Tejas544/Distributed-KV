// A replicated key-value service on top of Raft: P3's exercise vehicle.
//
// Every node runs a RaftDriver, a deterministic map as its state machine, and a
// client. Clients send requests over the *simulated network* to whichever node
// they currently believe is the leader, retry on redirect and on timeout, and
// record what they were told succeeded. Nothing here bypasses the transport:
// a partition between a client's node and the leader looks exactly like a
// partition, and a lost reply looks exactly like a lost reply.
//
// Two properties are checked here rather than in the invariant file, because
// both are statements about what a *client* saw:
//
//   INV-RAFT-14  a linearizable read never returns state older than a write
//                that completed before the read was invoked. Checked exactly,
//                using the log index at which each value was written: a read
//                returning index R is stale iff some acknowledged write to the
//                same key landed at a higher index before the read began.
//
//   durability   every acknowledged write is still present, at the same index,
//                once the faults heal. This is the client-visible half of
//                INV-RAFT-09, and it is the one an outside-in checker can see.
//
// Requests are idempotent by (client, sequence). That is not a convenience --
// it is the only correct way for a client to retry a write whose outcome is
// unknown, and a workload that retried non-idempotently would manufacture
// duplicate-application "bugs" that are really the harness's fault.

#ifndef ANVIL_WORKLOADS_RAFT_KV_H_
#define ANVIL_WORKLOADS_RAFT_KV_H_

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "anvil/checker/raft_invariants.h"
#include "anvil/core/raft/driver.h"
#include "anvil/core/raft/raft.h"
#include "anvil/core/types.h"
#include "anvil/sim/simulation.h"

namespace anvil::workloads {

struct RaftKvConfig {
  std::uint64_t ops_per_client = 30;
  Duration client_interval = Duration::millis(25);
  Duration client_poll = Duration::millis(10);
  Duration client_timeout = Duration::millis(3000);
  std::uint32_t keys = 6;
  std::uint32_t read_percent = 30;  // integer percent; no floating point anywhere

  // The last `learners` nodes join as learners rather than voters. They
  // replicate everything and are counted in nothing (INV-RAFT-15).
  std::uint32_t learners = 0;

  // Continuous joint-consensus membership change during the workload.
  bool membership_churn = false;
  Duration churn_interval = Duration::millis(1500);

  bool leadership_transfer = false;
  Duration transfer_interval = Duration::millis(2500);

  raft::RaftOptions raft;
  raft::RaftDurability durability;
};

// One acknowledged write, with the log index the value landed at. The index is
// what makes the staleness check exact rather than heuristic.
struct AckedWrite {
  std::string value;
  LogIndex index;
  Timestamp when;  // true simulated time at acknowledgement
};

class KvMachine;

struct RaftKvNode {
  // Declared, not defaulted: KvMachine is incomplete here on purpose, so that
  // the state machine's internals stay out of every translation unit that only
  // wants to run the workload.
  RaftKvNode();
  ~RaftKvNode();
  RaftKvNode(RaftKvNode&&) noexcept;
  RaftKvNode& operator=(RaftKvNode&&) noexcept;

  std::unique_ptr<KvMachine> machine;

  // The links to every peer, shared by every group on this node. P3 ran one
  // group per node so the driver owned them; P5 made that impossible, and this
  // workload takes the same seam even though it still has only one group --
  // two ways of owning a connection is one more than the tree should have.
  std::unique_ptr<raft::RaftTransport> transport;
  std::unique_ptr<raft::RaftDriver> driver;

  // Client progress, kept outside the coroutine so a restart resumes rather
  // than replaying from the beginning.
  std::uint64_t next_seq = 1;
  std::uint64_t inflight_seq = 0;

  // The request currently being attempted, held stable across retries. A retry
  // that changes the operation is a different request wearing the same name,
  // and late replies then attach to the wrong one.
  std::uint64_t pending_request_seq = 0;
  std::string pending_key;
  bool pending_is_read = false;
  bool reply_pending = false;
  std::uint8_t reply_status = 0;
  LogIndex reply_index{};
  std::string reply_value;
  NodeId leader_hint{};

  // Server side: requests accepted and waiting for their entry to be applied.
  struct PendingWrite {
    NodeId reply_to{};
    std::uint64_t client = 0;
    std::uint64_t seq = 0;
  };
  std::map<std::uint64_t, PendingWrite> pending_writes;  // keyed by (client<<32)|seq

  struct PendingRead {
    NodeId reply_to{};
    std::uint64_t client = 0;
    std::uint64_t seq = 0;
    std::string key;
    LogIndex index{};
    bool ready = false;
  };
  std::map<std::uint64_t, PendingRead> pending_reads;  // keyed by read context
  std::uint64_t next_read_context = 1;

  bool booted = false;

  // What the previous incarnation had achieved, captured at the moment this one
  // replaces it. Kept rather than deleted after the bug hunt, for the same
  // reason counter.cc keeps its per-stage recovery counters: "the node came back
  // short" is a symptom with several plausible causes, and narrowing it without
  // these took far longer than adding them would have. They are what
  // characterises ANV-0023 -- fsynced watermark before the crash, versus what
  // recovery could read afterwards.
  std::uint64_t boots = 0;
  std::uint64_t previous_persisted_high = 0;
  std::uint64_t previous_recovered_last = 0;
  std::uint64_t previous_fsynced_high = 0;
  std::uint64_t previous_rewrites = 0;
};

struct RaftKvState {
  // The client-visible history, per key, in acknowledgement order.
  std::map<std::string, std::vector<AckedWrite>> acked;

  std::uint64_t writes_acked = 0;
  std::uint64_t reads_served = 0;

  // A read that came back older than a write acknowledged before it started.
  // Split, in the taxonomy ANV-0007 established for the storage engine, because
  // the two are different findings:
  //
  //   stale     the value returned was genuinely written and acknowledged at
  //             some point -- durability loss, which a node whose durable log
  //             was corrupted cannot avoid and must merely detect.
  //   invented  the value was never acknowledged by anyone. There is no fault
  //             that excuses this one.
  std::uint64_t stale_reads = 0;
  std::uint64_t invented_reads = 0;
  std::uint64_t stale_reads_after_corruption = 0;
  std::uint64_t not_leader_replies = 0;
  std::uint64_t client_timeouts = 0;
  std::uint64_t client_retries = 0;
  std::uint64_t conf_changes_proposed = 0;
  std::uint64_t conf_changes_applied = 0;
  std::uint64_t transfers_requested = 0;

  // Durability findings, checked at quiesce.
  std::uint64_t lost_acked_writes = 0;
  std::vector<std::string> violations;

  std::map<std::uint64_t, RaftKvNode> nodes;
  std::uint32_t node_count = 0;
  bool done = false;

  // Set by the harness so the workload can ask whether a node is up before
  // handing it a request. A real client would simply time out; the harness
  // knows, and pretending otherwise only wastes simulated time.
  sim::Simulation* simulation = nullptr;
  checker::RaftObserver* observer = nullptr;
};

// Builds the cluster, arms INV-RAFT-14, and boots every node. The boot function
// is re-invoked on every restart and performs full recovery.
void install(sim::Simulation& simulation, RaftKvConfig config, RaftKvState* state,
             checker::RaftObserver* observer);

// Arms the client-visible read-freshness invariant. Exposed separately so a
// test can confirm it is what does the catching.
void arm_read_invariant(sim::Simulation& simulation, RaftKvState* state);

// True once every live node has applied every acknowledged write. The liveness
// property, only meaningful after the faults heal.
bool converged(sim::Simulation& simulation, const RaftKvState& state);

// Checks every acknowledged write against every live node's state machine.
// Returns the number of losses and appends a description of each.
std::uint64_t audit_durability(sim::Simulation& simulation, RaftKvState* state);

// The leader as the cluster currently sees it, or NodeId{} if there is none.
NodeId current_leader(const RaftKvState& state, sim::Simulation& simulation);

}  // namespace anvil::workloads

#endif  // ANVIL_WORKLOADS_RAFT_KV_H_
