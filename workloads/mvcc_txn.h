// P4's exercise vehicle: transactions, long readers, and a collector that is
// trying to delete the ground from under them.
//
// The shape of the workload is chosen to attack one thing. Version GC is safe
// exactly when the safepoint is computed correctly and the boundary version is
// kept, and both of those are silent when wrong -- a reader gets an older value
// or no value, with every status code ok. So the workload runs:
//
//   writers        short transactions that commit versions constantly, giving
//                  the collector something to collect
//   long readers   snapshots deliberately held open across many GC passes,
//                  which is what makes the safepoint have to *mean* something
//   collector      GC at the safepoint, as aggressively as the run allows
//   deadlockers    pairs of transactions taking the same two keys in opposite
//                  orders, which is the only way to exercise wound-wait
//   auditor        a god's-eye pass that re-reads every live snapshot against a
//                  model of everything ever committed
//
// The auditor is what turns "the reads looked fine" into a claim. It holds the
// full version history in memory -- affordable because this is a simulation --
// and asks, for every live snapshot and every key, whether the store still
// returns what the model says it must. That question is the whole of
// INV-MVCC-01, and it cannot be answered from inside the collector.

#ifndef ANVIL_WORKLOADS_MVCC_TXN_H_
#define ANVIL_WORKLOADS_MVCC_TXN_H_

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "anvil/checker/mvcc_invariants.h"
#include "anvil/core/lsm/db.h"
#include "anvil/core/mvcc/lock_table.h"
#include "anvil/core/mvcc/mvcc.h"
#include "anvil/core/mvcc/txn.h"
#include "anvil/sim/simulation.h"

namespace anvil::workloads {

struct MvccWorkloadConfig {
  std::uint32_t writers = 3;
  std::uint32_t long_readers = 2;
  std::uint32_t deadlockers = 2;   // pairs; each pair takes two keys in opposite orders
  std::uint64_t ops_per_writer = 40;
  std::uint32_t keys = 8;
  std::uint32_t writes_per_txn = 2;
  std::uint32_t reads_per_txn = 2;

  Duration writer_interval = Duration::millis(15);
  Duration reader_hold = Duration::millis(400);   // how long a long reader keeps its snapshot
  Duration gc_interval = Duration::millis(50);    // aggressive on purpose
  Duration audit_interval = Duration::millis(75);
  Duration deadlock_interval = Duration::millis(120);

  mvcc::MvccOptions mvcc;
  mvcc::IsolationLevel level = mvcc::IsolationLevel::kSnapshot;

  // false: the collector uses its own idea of the safepoint instead of the
  // transaction manager's. The deliberate bug for INV-MVCC-02.
  bool gc_uses_real_safepoint = true;
};

// Everything ever committed, by key and commit timestamp. The oracle the
// auditor compares against; a model, not a cache, and deliberately never
// consulted by the code under test.
using VersionModel = std::map<std::string, std::map<mvcc::CommitTs, std::string>>;

struct MvccWorkloadState {
  VersionModel model;

  std::uint64_t txns_committed = 0;
  std::uint64_t txns_aborted = 0;
  std::uint64_t write_conflicts = 0;
  std::uint64_t wounded = 0;
  std::uint64_t retries = 0;
  std::uint64_t reads_checked = 0;
  std::uint64_t gc_passes = 0;
  std::uint64_t versions_collected = 0;
  std::uint64_t audits = 0;
  std::uint64_t versions_audited = 0;
  std::uint64_t long_reader_checks = 0;
  std::uint64_t boots = 0;
  std::uint32_t writers_finished = 0;
  std::uint64_t ambiguous_commits = 0;   // the write failed; the outcome was not yet known
  std::uint64_t resolutions = 0;         // finished later by the janitor
  std::uint64_t unresolved_at_end = 0;   // the disk never came back
  std::uint64_t retired = 0;             // finished transactions dropped from the table
  std::uint64_t unattributable_intents = 0;  // owner already retired; a checker blind spot

  // Findings, all of which are silent in the system under test.
  std::uint64_t lost_versions = 0;      // INV-MVCC-01
  std::uint64_t wrong_reads = 0;        // INV-MVCC-04
  std::uint64_t order_violations = 0;   // INV-MVCC-03
  std::uint64_t orphan_intents = 0;     // INV-MVCC-05
  std::uint64_t transient_intents = 0;  // seen once, gone by the next pass
  std::uint64_t model_lag_reads = 0;    // the oracle had not caught up; not a fault
  std::vector<std::string> violations;

  bool done = false;

  // Intents seen sitting on a resolved transaction, keyed by key and owner.
  // An intent has to survive a whole audit interval in that state before it is
  // called an orphan -- see the note in the auditor.
  std::map<std::string, std::uint64_t> suspicious_intents;

  // Owned by install(); exposed so tests can inspect them after the run.
  std::unique_ptr<lsm::Db> db;
  std::unique_ptr<mvcc::MvccStore> store;
  std::unique_ptr<mvcc::LockTable> locks;
  std::unique_ptr<mvcc::TxnManager> txns;
  checker::MvccObserver* observer = nullptr;
  sim::Simulation* simulation = nullptr;
};

// Opens the database on node 1, arms INV-MVCC-*, and starts every task.
// Single-node by design: P4 is about versions and locks, and putting consensus
// underneath would confuse which layer a finding belongs to.
void install(sim::Simulation& simulation, MvccWorkloadConfig config,
             MvccWorkloadState* state, checker::MvccObserver* observer);

// True when the store agrees with the model at every timestamp the model knows
// about. The end-of-run check; the auditor is the during-the-run one.
Task<bool> audit_everything(MvccWorkloadState* state);

}  // namespace anvil::workloads

#endif  // ANVIL_WORKLOADS_MVCC_TXN_H_
