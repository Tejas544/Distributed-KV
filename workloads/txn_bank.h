// Distributed transactions over a sharded store: P6's exercise vehicle.
//
// Two workloads, one client, because they answer different questions and both
// answers are needed:
//
//   kListAppend  every key holds a list and a transaction appends a globally
//                unique element to some and reads others whole. This is the
//                shape the Elle-style checker needs: a read returns the entire
//                list, so the version order for that key is *directly
//                observable* rather than searched for. The recorded history
//                goes through `checker::check` at the level the coordinator
//                claims, and the verdict is the exit criterion -- snapshot
//                isolation must show write skew and no G1c, serializable must
//                show an acyclic graph, strict serializable must additionally
//                respect real time.
//
//   kBank        every key holds an integer and a transaction moves some of it
//                from one account to another, across ranges. The oracle is one
//                number: the total never changes. It is the client-visible half
//                and it needs no checker at all, which is exactly why it is
//                worth having beside one.
//
// Both run over the same coordinator, so a mutation that breaks one breaks the
// other -- and the drill's API-visibility column is the difference between
// which of them noticed.

#ifndef ANVIL_WORKLOADS_TXN_BANK_H_
#define ANVIL_WORKLOADS_TXN_BANK_H_

#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "anvil/checker/elle.h"
#include "anvil/checker/history.h"
#include "anvil/checker/shard_invariants.h"
#include "anvil/checker/txn_invariants.h"
#include "anvil/core/shard/store.h"
#include "anvil/core/txn/coordinator.h"
#include "anvil/core/types.h"
#include "anvil/sim/simulation.h"

namespace anvil::workloads {

enum class TxnWorkloadKind : std::uint8_t {
  kListAppend = 0,
  kBank,
};

const char* to_string(TxnWorkloadKind kind) noexcept;

struct TxnBankConfig {
  TxnWorkloadKind kind = TxnWorkloadKind::kListAppend;

  std::uint64_t txns_per_client = 24;
  Duration client_interval = Duration::millis(20);
  Duration settle_before_start = Duration::millis(600);

  std::uint32_t keys = 12;
  std::uint32_t ops_per_txn = 3;      // reads and writes together
  std::uint32_t read_percent = 40;    // of the operations inside a transaction
  std::int64_t initial_balance = 100;  // kBank
  std::int64_t max_transfer = 20;      // kBank

  // Keys are chosen close together so that a transaction usually spans two
  // ranges rather than one: a distributed transaction that never leaves a
  // single range is a local transaction with extra steps.
  std::uint32_t neighbourhood = 4;

  txn::CoordinatorOptions txn;
  shard::StoreOptions store;
};

struct TxnBankNode {
  TxnBankNode();
  ~TxnBankNode();
  TxnBankNode(TxnBankNode&&) noexcept;
  TxnBankNode& operator=(TxnBankNode&&) noexcept;

  std::unique_ptr<shard::ShardStore> store;
  std::unique_ptr<txn::Coordinator> coordinator;

  std::uint64_t done = 0;
  bool booted = false;
  std::uint64_t boots = 0;
};

struct TxnBankState {
  // The recorded history, for the Elle-style checker. Every transaction the
  // client issued, with its invocation and completion times and the outcome it
  // was told -- including kUnknown, which is a real answer and the one that
  // makes checkers disagree when it is forced into a bucket.
  checker::History history;

  std::uint64_t committed = 0;
  std::uint64_t aborted = 0;
  std::uint64_t unknown = 0;
  std::uint64_t restarts = 0;
  std::uint64_t reads = 0;
  std::uint64_t writes = 0;
  std::uint64_t cross_range = 0;   // transactions that touched more than one range
  std::uint64_t single_range = 0;

  // kBank
  std::int64_t expected_total = 0;
  std::int64_t final_total = 0;

  // kListAppend: every element the client was told was committed. Every one of
  // them must be present in the final state exactly once.
  std::map<checker::Element, std::string> acked_elements;  // element -> key

  // Every element id the workload has handed out, across every client and every
  // incarnation of every node. The checker's central precondition is that
  // `element -> writer` is a function; this is the harness holding itself to it,
  // because a generator that reissues an id produces a history the checker must
  // call a duplicate-element anomaly and there is no way to tell that apart from
  // a real one after the fact. See [ANV-0055].
  std::set<checker::Element> issued_elements;
  std::uint64_t lost_elements = 0;
  std::uint64_t duplicated_elements = 0;

  std::vector<std::string> violations;

  std::map<std::uint64_t, TxnBankNode> nodes;
  std::uint32_t node_count = 0;
  sim::Simulation* simulation = nullptr;
  checker::TxnObserver* observer = nullptr;
  TxnBankConfig config;
};

void install(sim::Simulation& simulation, TxnBankConfig config, TxnBankState* state,
             checker::TxnObserver* observer);

// kBank: sums every account across the cluster. kListAppend: checks that every
// acknowledged element is present exactly once in the final lists.
void audit(sim::Simulation& simulation, TxnBankState* state);

// True once every range has an initialised replica and no intent is older than
// the transaction TTL.
bool converged(sim::Simulation& simulation, const TxnBankState& state);

// Intents that outlived their transaction's TTL without being resolved.
std::uint64_t orphaned_intents(sim::Simulation& simulation, const TxnBankState& state);

}  // namespace anvil::workloads

#endif  // ANVIL_WORKLOADS_TXN_BANK_H_
