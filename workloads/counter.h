// A durable replicated counter: the smallest protocol that can actually lose
// data, and the vehicle for P1's exit criteria.
//
// One leader, N-1 followers, no elections (that is P3). The leader accepts
// increments, writes each to its own write-ahead log, acknowledges the client,
// and then pushes to followers forever until they acknowledge. Followers
// deduplicate by id, log durably, and acknowledge. Every node recovers its
// state from its WAL on restart.
//
// The protocol is correct under omission faults *by construction*: retries are
// unbounded, application is idempotent, and nothing depends on the clock. Drop
// every message, partition the cluster, reorder and duplicate everything, kill
// nodes at random -- once the faults stop, every acknowledged increment must be
// on every node. If that ever fails, the bug is in the simulator, not the
// protocol, which is exactly what makes this a good harness test.
//
// Correctness under *crash* faults is a different matter, and it is where the
// two deliberate knobs come in:
//
//   fsync_before_ack     off, and the leader acknowledges writes that are only
//                        in the page cache. A crash loses them. This is
//                        INV-LSM-01 in miniature.
//   fsync_dir_on_create  off, and the WAL's directory entry is never persisted.
//                        The contents can be flawlessly synced and the file
//                        still will not exist after a crash -- every
//                        acknowledged write in it, gone.
//
// Both knobs default to correct. Turning either off must produce detected data
// loss, and a harness that fails to detect it is a harness that proves nothing.
// That pairing is the point: the same discipline as the negative control on the
// hermeticity gate.

#ifndef ANVIL_WORKLOADS_COUNTER_H_
#define ANVIL_WORKLOADS_COUNTER_H_

#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "anvil/core/types.h"
#include "anvil/sim/simulation.h"

namespace anvil::workloads {

struct CounterConfig {
  std::uint64_t increments = 60;
  Duration client_interval = Duration::millis(15);
  Duration retry_interval = Duration::millis(60);

  // The correctness knobs. Both default to correct; tests flip them to prove
  // the disk model can tell the difference.
  bool fsync_before_ack = true;
  bool fsync_dir_on_create = true;
};

struct CounterNodeState {
  FileHandle wal{};
  std::uint64_t wal_size = 0;
  std::set<std::uint64_t> applied;      // volatile: rebuilt from the WAL on boot
  std::set<std::uint64_t> acked_by[8];  // leader only: per-follower ack sets, volatile

  // Every id this node has told somebody was durable -- the client for the
  // leader, the leader for a follower. Kept outside the node, because it is the
  // *promise* that must survive the crash, not the node's memory of it. This is
  // what recovery is checked against.
  //
  // Checking a follower against the client's acknowledged set instead would be
  // wrong and would fire constantly: a follower that has not yet replicated an
  // increment has not lost anything, it is simply behind.
  std::set<std::uint64_t> promised;

  bool ready = false;
};

struct CounterState {
  // What the client was told was durable. Survives crashes, because the client
  // is outside the cluster and remembers what it was promised.
  std::set<std::uint64_t> acked_ids;

  std::uint64_t recoveries = 0;
  std::uint64_t boot_retries = 0;
  std::uint64_t follower_applies = 0;

  // Where recovery gave up, broken down by stage. Worth keeping rather than
  // deleting after the bug hunt: "the node never came back" is a symptom with
  // five plausible causes, and narrowing it without these took longer than
  // adding them would have.
  std::uint64_t recover_open_failures = 0;
  std::uint64_t recover_dirsync_failures = 0;
  std::uint64_t recover_size_failures = 0;
  std::uint64_t recover_read_failures = 0;
  std::uint64_t boots_started = 0;

  // Recovery stopped at a record that failed its checksum. Distinct from data
  // loss: the damage was *detected*, which is the checksum doing its job. A
  // single-replica log has no way to repair it, but silently serving it would
  // be far worse.
  std::uint64_t corruption_detected = 0;

  // The finding. A node came back up and its recovered log was missing
  // something it had promised.
  std::uint64_t lost_acked_writes = 0;
  std::vector<std::string> violations;

  std::map<std::uint64_t, CounterNodeState> nodes;

  bool done = false;
};

// Registers boot functions for every node and starts the cluster. The boot
// function is re-invoked on every restart, so it performs recovery rather than
// assuming a blank machine.
void install(sim::Simulation& simulation, CounterConfig config, CounterState* state);

// True once every node's applied set contains every acknowledged id. The
// liveness property, checked after faults heal.
bool converged(const sim::Simulation& simulation, const CounterState& state);

}  // namespace anvil::workloads

#endif  // ANVIL_WORKLOADS_COUNTER_H_
