// P7 exit criterion 1, at the scale the criterion actually states.
//
// `checker_test` already asserts soundness and precision, but at the size that
// belongs in a unit suite: 90 anomalous cases and 500 valid histories, a couple
// of seconds. The roadmap asks for something else -- a *reported mutation
// score* over a corpus of roughly two hundred anomalous histories, and zero
// false positives over ten thousand reference-model ones. That is a gate, not a
// unit test, and it is slow enough to deserve its own binary.
//
// Why this is the first thing P7 gets, ahead of TLA+ and DPOR: this project's
// last three findings were all checker bugs, and two of them had been reporting
// confidently wrong verdicts for an entire phase (ANV-0051, ANV-0053). A
// checker whose own error rate is unmeasured is not evidence of anything. The
// number this binary prints is the claim the rest of the suite rests on.
//
// Three questions:
//
//   1. Mutation score. Of N histories each containing exactly one known,
//      named anomaly, how many does the checker flag -- and of those, how many
//      does it file under the right name? Detection alone is not enough: the
//      anomaly's name is what says which isolation levels it rules out, so a
//      misfiled anomaly is a wrong answer wearing a right one.
//
//   2. False positives, at scale. Ten thousand histories produced by executing
//      transactions serially through the reference model. Serial execution is
//      trivially serializable, so a single rejection at any level is a bug in
//      the checker. This is the half almost nobody tests, and it is the half
//      that decides whether the checker survives contact with an argument.
//
//   3. Level discrimination. A checker that rejects everything scores 100% on
//      question 1. So each anomaly is also checked at a level that *permits*
//      it -- write skew under snapshot isolation, a real-time violation under
//      plain serializable -- and must come back valid there.

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "anvil/checker/corpus.h"
#include "anvil/checker/elle.h"
#include "anvil/checker/history.h"

namespace {

using namespace anvil::checker;  // NOLINT(build/namespaces) -- a test, and it reads better

int g_failures = 0;

void check(bool condition, const std::string& what) {
  if (!condition) {
    std::cerr << "FAIL: " << what << "\n";
    ++g_failures;
  }
}

constexpr IsolationLevel kLevels[] = {
    IsolationLevel::kReadUncommitted, IsolationLevel::kReadCommitted,
    IsolationLevel::kSnapshotIsolation, IsolationLevel::kSerializable,
    IsolationLevel::kStrictSerializable,
};

// ---------------------------------------------------------------------------
// 1: the mutation score
// ---------------------------------------------------------------------------

struct ClassScore {
  std::size_t total = 0;
  std::size_t detected = 0;
  std::size_t classified = 0;
};

void test_mutation_score(std::size_t cases) {
  const auto corpus = anomalous_corpus(20250902, cases);
  std::map<std::string, ClassScore> by_class;
  std::size_t detected = 0;
  std::size_t classified = 0;

  for (const CorpusCase& c : corpus) {
    ClassScore& score = by_class[to_string(c.expected)];
    ++score.total;

    const CheckResult result = check(c.history, c.rejected_from);
    if (!result.valid) {
      ++detected;
      ++score.detected;
    } else {
      std::cerr << "  MISSED " << c.name << " (" << to_string(c.expected) << " at "
                << to_string(c.rejected_from) << ")\n"
                << c.history.render();
    }
    if (result.has(c.expected)) {
      ++classified;
      ++score.classified;
    } else if (!result.valid) {
      std::cerr << "  MISFILED " << c.name << ": expected " << to_string(c.expected) << ", got "
                << result.summary() << "\n";
    }
  }

  std::cout << "\n  mutation score over " << corpus.size() << " anomalous histories\n";
  std::cout << "  ---------------------------------------------------------------------\n";
  std::cout << "  anomaly                              cases  detected  correctly named\n";
  std::cout << "  ---------------------------------------------------------------------\n";
  for (const auto& [name, score] : by_class) {
    std::string row = "  ";
    row += name;
    row.resize(39, ' ');
    row += std::to_string(score.total);
    row.resize(46, ' ');
    row += std::to_string(score.detected) + "/" + std::to_string(score.total);
    row.resize(56, ' ');
    row += std::to_string(score.classified) + "/" + std::to_string(score.total);
    std::cout << row << "\n";
  }
  std::cout << "  ---------------------------------------------------------------------\n";
  std::cout << "  mutation score: " << detected << "/" << corpus.size() << " detected ("
            << (detected * 100 / corpus.size()) << "%), " << classified << "/" << corpus.size()
            << " correctly named (" << (classified * 100 / corpus.size()) << "%)\n";

  check(detected == corpus.size(), "P7 exit criterion 1: every anomalous history must be flagged");
  check(classified == corpus.size(),
        "P7 exit criterion 1: every anomaly must be filed under its own name");
}

// ---------------------------------------------------------------------------
// 2: false positives, at the criterion's scale
// ---------------------------------------------------------------------------

void test_no_false_positives(std::size_t histories) {
  std::size_t rejected = 0;
  std::size_t transactions = 0;
  std::map<std::string, std::size_t> spurious;

  for (std::size_t i = 0; i < histories; ++i) {
    // Shapes vary so the corpus is not ten thousand copies of one history:
    // length and key count both move, which changes how much contention the
    // reference model produces and therefore how dense the graph is.
    const History history = valid_history(i + 1, 8 + (i % 25), 3 + (i % 7));
    transactions += history.size();

    for (const IsolationLevel level : kLevels) {
      const CheckResult result = check(history, level);
      if (result.valid) continue;
      ++rejected;
      for (const Anomaly a : result.anomalies) ++spurious[to_string(a)];
      if (rejected <= 3) {
        std::cerr << "  FALSE POSITIVE at " << to_string(level) << " on serial history " << (i + 1)
                  << ": " << result.summary() << "\n"
                  << history.render();
      }
    }
  }

  std::cout << "\n  precision: " << histories << " serial histories (" << transactions
            << " transactions) checked at all " << (sizeof(kLevels) / sizeof(kLevels[0]))
            << " levels, " << rejected << " rejected\n";
  if (!spurious.empty()) {
    std::cout << "  spurious anomalies:";
    for (const auto& [name, n] : spurious) std::cout << " " << name << "x" << n;
    std::cout << "\n";
  }
  check(rejected == 0,
        "P7 exit criterion 1: a serially-executed history is serializable, at every level");
}

// ---------------------------------------------------------------------------
// 3: the checker is discriminating, not merely censorious
// ---------------------------------------------------------------------------

void test_levels_discriminate(std::size_t cases) {
  const auto corpus = anomalous_corpus(4242, cases);
  std::size_t permitted_cases = 0;
  std::size_t accepted_where_permitted = 0;

  for (const CorpusCase& c : corpus) {
    // The levels below the one this case is rejected from. If the anomaly is
    // not forbidden there, the history has to come back valid -- otherwise the
    // checker is reporting a violation of a rule that level does not have, and
    // the mutation score above is measuring nothing but its eagerness.
    for (const IsolationLevel level : kLevels) {
      if (forbids(level, c.expected)) continue;
      ++permitted_cases;
      const CheckResult result = check(c.history, level);
      if (result.valid) {
        ++accepted_where_permitted;
      } else if (result.has(c.expected)) {
        std::cerr << "  OVER-REPORTED " << c.name << ": " << to_string(c.expected)
                  << " is permitted at " << to_string(level) << " but was reported there\n";
      } else {
        // Rejected for a *different* anomaly it also happens to contain. Not a
        // bug: a history built around G2-item may contain a G1c as well.
        ++accepted_where_permitted;
      }
    }
  }

  std::cout << "\n  discrimination: " << accepted_where_permitted << "/" << permitted_cases
            << " (anomaly, level) pairs where the level permits the anomaly came back clean\n";
  check(accepted_where_permitted == permitted_cases,
        "an anomaly must not be reported at a level that permits it");
}

}  // namespace

int main(int argc, char** argv) {
  // Defaults are the roadmap's numbers. Overridable so the gate can be run
  // small during development without editing it.
  const std::size_t cases = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 200;
  const std::size_t histories = argc > 2 ? std::strtoull(argv[2], nullptr, 10) : 10000;

  std::cout << "checker verification (P7 exit criterion 1): " << cases
            << " anomalous histories, " << histories << " serial histories\n";

  test_mutation_score(cases);
  test_no_false_positives(histories);
  test_levels_discriminate(cases < 100 ? cases : 100);

  if (g_failures != 0) {
    std::cerr << "\nchecker verification: " << g_failures << " failure(s)\n";
    return EXIT_FAILURE;
  }
  std::cout << "\nchecker verification: all checks passed\n";
  return EXIT_SUCCESS;
}
