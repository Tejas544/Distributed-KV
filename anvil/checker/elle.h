// An Elle-style transactional consistency checker.
//
// The method, in four steps:
//
//   1. Recover the version order for each key directly from the observed lists.
//      This is what list-append buys us and it is why the whole thing is
//      tractable -- no search, no guessing.
//   2. Build the direct serialization graph over transactions, with three edge
//      types:
//        ww  T1 wrote version n, T2 wrote version n+1
//        wr  T2 read the version T1 wrote
//        rw  T1 read version n, T2 wrote version n+1   (anti-dependency)
//   3. Find strongly connected components. Any cycle is a serializability
//      violation -- that is Adya's theorem, and it is the entire justification
//      for this approach.
//   4. Classify each cycle by the edges it uses. Which anomaly you have, and
//      therefore which isolation levels it violates, is determined purely by
//      the edge multiset:
//        ww only        -> G0        (write cycle)
//        ww + wr        -> G1c       (circular information flow)
//        exactly one rw -> G-single  (forbidden by snapshot isolation)
//        two or more rw -> G2-item   (write skew; SI permits this, SER does not)
//
// Two things are checked outside the graph because they are not cycles at all:
// G1a (reading a value written by a transaction that aborted) and G1b (reading
// a value a transaction later overwrote within the same transaction).
//
// **Where this is an approximation, stated plainly.** Snapshot isolation is
// modelled as "forbids G0, G1, and G-single". Adya's PL-SI is defined in terms
// of G-SIa and G-SIb over a start-ordered graph, and the two are not identical
// in every corner. The approximation is standard, it is sound for the anomalies
// this workload can produce, and cross-validation against Jepsen's Elle in P7
// is what will actually settle whether it is good enough. Until then the
// approximation is documented rather than hidden, because a checker whose
// limits are unknown is a checker whose verdicts mean nothing.
//
// Also not implemented: predicate anti-dependencies (G2 proper). This workload
// has no predicate reads, so G2-item is the ceiling. Range scans in P4 will
// change that.

#ifndef ANVIL_CHECKER_ELLE_H_
#define ANVIL_CHECKER_ELLE_H_

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "anvil/checker/history.h"

namespace anvil::checker {

enum class IsolationLevel : std::uint8_t {
  kReadUncommitted,
  kReadCommitted,
  kSnapshotIsolation,
  kSerializable,
  kStrictSerializable,
};

const char* to_string(IsolationLevel level) noexcept;

enum class Anomaly : std::uint8_t {
  kG0,                    // cycle of write dependencies
  kG1a,                   // read of a value written by an aborted transaction
  kG1b,                   // read of an intermediate value
  kG1c,                   // cycle of write and read dependencies
  kGSingle,               // cycle with exactly one anti-dependency
  kG2Item,                // cycle with two or more anti-dependencies (write skew)
  kVersionOrderConflict,  // two reads disagree about a key's version order
  kDuplicateElement,      // one element appended by two transactions
  kRealTimeViolation,     // serialization order contradicts real-time order
  kCount,
};

const char* to_string(Anomaly anomaly) noexcept;

enum class EdgeType : std::uint8_t { kWW, kWR, kRW, kRealTime };

const char* to_string(EdgeType edge) noexcept;

struct CycleWitness {
  Anomaly anomaly = Anomaly::kG0;
  std::vector<TxnId> txns;      // the cycle, in order; txns[0] is repeated implicitly
  std::vector<EdgeType> edges;  // edges[i] connects txns[i] -> txns[i+1 mod n]
  std::string detail;

  std::string render() const;
};

struct CheckResult {
  bool valid = true;
  IsolationLevel level = IsolationLevel::kSerializable;
  std::vector<Anomaly> anomalies;  // sorted, deduplicated
  std::vector<CycleWitness> witnesses;
  std::size_t transactions = 0;
  std::size_t edges = 0;

  bool has(Anomaly anomaly) const;
  std::string summary() const;
};

// The main entry point. Deterministic: the same history always yields the same
// verdict, the same witnesses, in the same order.
CheckResult check(const History& history, IsolationLevel level);

// Whether a given anomaly is forbidden at a given level. Exposed because the
// corpus needs it to state what each pathological history is supposed to
// violate, and because tests should assert against the table rather than
// re-deriving it.
bool forbids(IsolationLevel level, Anomaly anomaly);

}  // namespace anvil::checker

#endif  // ANVIL_CHECKER_ELLE_H_
