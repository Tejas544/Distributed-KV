// The checker's own test corpus.
//
// This file is the reason the checker's verdicts mean anything.
//
// A consistency checker has two ways to be useless and only one of them is
// obvious. Everyone worries about false negatives -- a checker that misses real
// anomalies. Almost nobody tests for the other direction, and a checker that
// reports anomalies in correct histories is arguably worse: it will be argued
// with, then distrusted, then disabled, and the argument will take a week.
//
// So two corpora:
//
//   anomalous_corpus()  histories built to contain exactly one known anomaly.
//                       The checker must flag every one. (INV-SIM-03)
//
//   valid_history()     histories produced by executing transactions serially
//                       through the reference model. Serial execution is
//                       trivially serializable, so the checker must accept
//                       every one, at every level. (INV-SIM-04)
//
// The anomalous cases are constructed by hand rather than found by fuzzing,
// because each one has to be a *known* instance of a *named* anomaly -- the
// point is to verify the classification, not just the detection. A history that
// is merely "invalid somehow" would confirm far less.

#ifndef ANVIL_CHECKER_CORPUS_H_
#define ANVIL_CHECKER_CORPUS_H_

#include <cstdint>
#include <string>
#include <vector>

#include "anvil/checker/elle.h"
#include "anvil/checker/history.h"

namespace anvil::checker {

struct CorpusCase {
  std::string name;
  History history;

  // The anomaly this history is built to contain. The checker must report
  // exactly this, not merely "something is wrong".
  Anomaly expected = Anomaly::kG0;

  // The weakest isolation level that must reject it. Checking at this level and
  // above must fail; checking strictly below it must pass, which is how the
  // corpus also verifies that the checker is not simply rejecting everything.
  IsolationLevel rejected_from = IsolationLevel::kSerializable;
};

// One case per anomaly class, repeated with fresh keys and elements until
// `count` cases exist. Deterministic in `seed`.
std::vector<CorpusCase> anomalous_corpus(std::uint64_t seed, std::size_t count);

// A history produced by running `txns` transactions serially through the
// reference model. Correct by construction at every isolation level.
History valid_history(std::uint64_t seed, std::size_t txns, std::size_t keys);

}  // namespace anvil::checker

#endif  // ANVIL_CHECKER_CORPUS_H_
