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

  // kListAppend: every key a transaction reads is a key it does not write, and
  // vice versa. Off by default, because the read-your-own-writes shape is worth
  // exercising too.
  //
  // It exists because the two are not equally interesting to *every* mechanism.
  // The default plan draws each operation's key independently, so a transaction
  // routinely reads and appends the same key -- and a stale read on a key you
  // also write is caught by first-committer-wins at prewrite, long before the
  // read refresh would have mattered. The refresh is the only guard for keys
  // read and *not* written, which is the write-skew shape, and it is that shape
  // the `no refresh on push` mutation exists to test. Drawing the two sets
  // disjointly is what turns "the mechanism sometimes matters" into "the
  // mechanism is the only thing standing here".
  bool disjoint_read_write = false;

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
  // Restarts caused specifically by a read landing inside the uncertainty
  // window, counted apart from every other reason a transaction goes round
  // again. It is the coverage number for the two uncertainty mutations: a
  // drill row that switches off a mechanism which never fires is not a
  // detector that failed, it is a row that asked nothing, and the two are
  // indistinguishable from the detection count alone. Both rows spent this
  // phase at snapshot isolation, where Coordinator::begin sets the window to
  // empty under the oracle, so this number was structurally zero there.
  std::uint64_t uncertain_reads = 0;
  // Reads issued by the settle-phase sweep, which is the coverage number for
  // every claim that rests on the final state having been observed by a client
  // rather than only by the audit.
  std::uint64_t settle_reads = 0;
  std::uint64_t reads = 0;
  std::uint64_t writes = 0;
  // For each acknowledged element, the snapshot its transaction read at and the
  // timestamp it committed at. Cheap to keep and the difference between a
  // diagnosis and a guess when an element goes missing: [ANV-0058] was found by
  // reading these two numbers off the version that survived and the one that
  // did not, and seeing that the survivor's snapshot was three hundred
  // timestamps *above* the version it had failed to see.
  std::map<checker::Element, std::pair<txn::Ts, txn::Ts>> element_stamps;

  std::uint64_t cross_range = 0;   // transactions that touched more than one range
  std::uint64_t single_range = 0;

  // kBank
  std::int64_t expected_total = 0;
  std::int64_t final_total = 0;
  // Per account, so that "the cluster holds 1592 and started with 1600" can be
  // turned into "this account is eight short and that one is eight over"
  // without a second run. A conserved total is one number, and one number
  // cannot say which transfer it lost -- every conservation finding this phase
  // has chased began by recovering this breakdown by hand.
  std::map<std::string, std::int64_t> final_balances;

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

  // Keys that more than one live range claimed with its newest descriptor when
  // the audit ran. Reported rather than asserted: every value the audit reads
  // comes from one range, so this is the number that says whether "the range
  // that holds this key" was a well-posed question at all. See [ANV-0059] for
  // what a badly-posed one costs.
  std::uint64_t ambiguous_keys = 0;

  std::vector<std::string> violations;

  std::map<std::uint64_t, TxnBankNode> nodes;
  std::uint32_t node_count = 0;
  sim::Simulation* simulation = nullptr;
  checker::TxnObserver* observer = nullptr;
  TxnBankConfig config;
};

void install(sim::Simulation& simulation, TxnBankConfig config, TxnBankState* state,
             checker::TxnObserver* observer);

// Spawns one read-only transaction per key on a live node, to be run during the
// settle phase after the faults have healed. Call it, then give the simulation
// time to run, then audit.
//
// It is not optional decoration. Elle builds a key's version order out of the
// reads that observed it, so a run that stops writing and never reads back
// hands the checker a history with almost no edges in it -- and an intent whose
// coordinator died is cleaned up by the next reader, of which there were
// previously none. See the long note at `settle_reader` in txn_bank.cc.
void start_settle_reads(sim::Simulation& simulation, TxnBankState* state);

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
