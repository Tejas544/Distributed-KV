// P1 exit criterion 3, and the two meta-invariants that make the checker
// worth trusting.
//
//   INV-SIM-03 (soundness).  The checker flags 100% of a corpus of histories
//                            built to contain a known anomaly, and names the
//                            right one.
//   INV-SIM-04 (precision).  The checker accepts 100% of histories produced by
//                            serial execution through the reference model.
//
// Both directions are required and the second is the one people skip. A checker
// with false positives does not get fixed, it gets argued with and then
// disabled -- and every result that depended on it evaporates.
//
// A third property is checked here too, and it is the sharpest test of whether
// the classification is real rather than a rubber stamp: **the level boundaries
// must be exact**. Write skew must be rejected at serializable and *accepted*
// at snapshot isolation. A real-time violation must be rejected at strict
// serializable and accepted at serializable. A checker that just rejected
// everything would sail through soundness; only the "must accept" half proves
// it is discriminating.

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <string_view>

#include "anvil/checker/corpus.h"
#include "anvil/checker/elle.h"
#include "anvil/checker/history.h"
#include "anvil/checker/invariant.h"

namespace {

using namespace anvil::checker;

int g_failures = 0;

void check(bool condition, std::string_view what) {
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

int level_rank(IsolationLevel level) {
  for (int i = 0; i < 5; ++i) {
    if (kLevels[i] == level) return i;
  }
  return 0;
}

// ---------------------------------------------------------------------------
// INV-SIM-03: soundness
// ---------------------------------------------------------------------------

void test_soundness(std::size_t cases) {
  const auto corpus = anomalous_corpus(1, cases);

  std::size_t detected = 0;
  std::size_t correctly_classified = 0;
  std::map<int, std::size_t> by_anomaly;

  for (const CorpusCase& c : corpus) {
    const CheckResult result = check(c.history, c.rejected_from);
    ++by_anomaly[static_cast<int>(c.expected)];

    if (!result.valid) {
      ++detected;
    } else {
      std::cerr << "  MISSED: " << c.name << "\n" << c.history.render()
                << "  -> " << result.summary() << "\n";
    }

    if (result.has(c.expected)) {
      ++correctly_classified;
    } else if (!result.valid) {
      // Detected but misfiled. Not as bad as a miss, but it means the anomaly
      // taxonomy is wrong somewhere, and the taxonomy is what tells you which
      // isolation levels are actually affected.
      std::cerr << "  MISCLASSIFIED: " << c.name << " expected "
                << anvil::checker::to_string(c.expected) << ", got:\n    "
                << result.summary() << "\n";
    }
  }

  check(detected == corpus.size(), "the checker must flag every history in the anomaly corpus");
  check(correctly_classified == corpus.size(),
        "the checker must name the correct anomaly, not merely detect one");
  std::cout << "  soundness: " << detected << "/" << corpus.size() << " detected, "
            << correctly_classified << "/" << corpus.size() << " correctly classified across "
            << by_anomaly.size() << " anomaly classes\n";
}

// ---------------------------------------------------------------------------
// INV-SIM-04: precision
// ---------------------------------------------------------------------------

void test_precision(std::size_t histories) {
  std::size_t false_positives = 0;

  for (std::size_t i = 0; i < histories; ++i) {
    const History history = valid_history(i + 1, 12 + (i % 20), 4 + (i % 5));

    // Checked at the *strictest* level. A serial history satisfies every level,
    // so anything reported here is unambiguously a false positive.
    const CheckResult result = check(history, IsolationLevel::kStrictSerializable);
    if (!result.valid) {
      ++false_positives;
      if (false_positives <= 3) {
        std::cerr << "  FALSE POSITIVE on serial history " << i << ":\n"
                  << history.render() << "  -> " << result.summary() << "\n";
      }
    }
  }

  check(false_positives == 0,
        "a serially-executed history must be accepted at every level -- a checker "
        "with false positives gets disabled, and takes every result with it");
  std::cout << "  precision: " << (histories - false_positives) << "/" << histories
            << " serial histories accepted at strict-serializable\n";
}

// ---------------------------------------------------------------------------
// The level boundaries have to be exact
// ---------------------------------------------------------------------------

void test_level_boundaries() {
  const auto corpus = anomalous_corpus(7, 45);

  std::size_t wrongly_rejected = 0;
  std::size_t wrongly_accepted = 0;

  for (const CorpusCase& c : corpus) {
    for (const IsolationLevel level : kLevels) {
      const CheckResult result = check(c.history, level);
      const bool should_reject = level_rank(level) >= level_rank(c.rejected_from);

      if (should_reject && result.valid) {
        ++wrongly_accepted;
        if (wrongly_accepted <= 3) {
          std::cerr << "  " << c.name << " accepted at " << anvil::checker::to_string(level)
                    << " but should be rejected from " << anvil::checker::to_string(c.rejected_from)
                    << " upward\n";
        }
      }
      if (!should_reject && !result.valid) {
        ++wrongly_rejected;
        if (wrongly_rejected <= 3) {
          std::cerr << "  " << c.name << " rejected at " << anvil::checker::to_string(level)
                    << " but that level permits it:\n    " << result.summary() << "\n";
        }
      }
    }
  }

  check(wrongly_accepted == 0, "every anomaly must be rejected at and above its level");
  check(wrongly_rejected == 0, "no anomaly may be rejected at a level that permits it");
}

// The single most important discrimination test in the file. Write skew is the
// defining permitted anomaly of snapshot isolation -- if the checker flagged it
// at SI, the SI transaction engine in P6 would look broken while behaving
// exactly as specified.
void test_write_skew_is_permitted_under_snapshot_isolation() {
  const auto corpus = anomalous_corpus(3, 45);
  std::size_t checked = 0;
  for (const CorpusCase& c : corpus) {
    if (c.expected != Anomaly::kG2Item) continue;
    ++checked;

    const CheckResult si = check(c.history, IsolationLevel::kSnapshotIsolation);
    const CheckResult ser = check(c.history, IsolationLevel::kSerializable);

    check(si.valid, "write skew must be ACCEPTED under snapshot isolation");
    check(!ser.valid, "write skew must be rejected under serializability");
    check(ser.has(Anomaly::kG2Item), "write skew must be classified as G2-item");
  }
  check(checked > 0, "the corpus must actually contain write-skew cases");
  std::cout << "  level discrimination: " << checked
            << " write-skew histories accepted at SI, rejected at serializable\n";
}

// Likewise for real time: a history can be perfectly serializable and still
// violate the order the client observed. Only strict serializability sees it.
void test_realtime_only_matters_at_strict() {
  const auto corpus = anomalous_corpus(5, 45);
  std::size_t checked = 0;
  for (const CorpusCase& c : corpus) {
    if (c.expected != Anomaly::kRealTimeViolation) continue;
    ++checked;
    check(check(c.history, IsolationLevel::kSerializable).valid,
          "a real-time violation must be ACCEPTED at serializable");
    const CheckResult strict = check(c.history, IsolationLevel::kStrictSerializable);
    check(!strict.valid, "a real-time violation must be rejected at strict-serializable");
    check(strict.has(Anomaly::kRealTimeViolation), "it must be classified as a real-time violation");
  }
  check(checked > 0, "the corpus must actually contain real-time cases");
}

// ---------------------------------------------------------------------------
// determinism and witness quality
// ---------------------------------------------------------------------------

void test_verdicts_are_deterministic() {
  const auto corpus = anomalous_corpus(11, 45);
  for (const CorpusCase& c : corpus) {
    const CheckResult a = check(c.history, c.rejected_from);
    const CheckResult b = check(c.history, c.rejected_from);
    if (a.summary() != b.summary()) {
      check(false, "the checker must produce identical verdicts and witnesses every time");
      std::cerr << "  " << c.name << "\n    A: " << a.summary() << "\n    B: " << b.summary()
                << "\n";
      return;
    }
  }
}

void test_witnesses_are_small_and_readable() {
  const auto corpus = anomalous_corpus(13, 45);
  std::size_t largest = 0;
  for (const CorpusCase& c : corpus) {
    const CheckResult result = check(c.history, c.rejected_from);
    for (const CycleWitness& witness : result.witnesses) {
      if (witness.txns.size() > largest) largest = witness.txns.size();
    }
  }
  // Reporting a whole strongly-connected component is technically a correct
  // answer and useless to a human. Every corpus history has a minimal cycle of
  // exactly two transactions -- including the nested-cycle case, whose SCC spans
  // eight -- so anything larger means minimisation is not working.
  //
  // The bound is tight on purpose. It started as `<= 3`, which a seeded mutation
  // that reported the *longest* cycle sailed straight through, because at the
  // time no corpus history had an SCC bigger than its shortest cycle.
  check(largest <= 2, "cycle witnesses must be minimal, not whole components");
  std::cout << "  witnesses: largest reported cycle is " << largest << " transactions\n";
}

// ---------------------------------------------------------------------------
// the invariant framework
// ---------------------------------------------------------------------------

void test_invariant_registry() {
  InvariantRegistry registry;
  int counter = 0;
  bool tripwire = false;

  registry.arm("INV-TEST-01", "always holds", CostClass::kTick,
               [&counter]() -> std::optional<std::string> {
                 ++counter;
                 return std::nullopt;
               });
  registry.arm("INV-TEST-02", "fires when armed", CostClass::kTick,
               [&tripwire]() -> std::optional<std::string> {
                 if (!tripwire) return std::nullopt;
                 return std::string{"the tripwire was set"};
               });
  registry.arm("INV-TEST-03", "quiesce only", CostClass::kQuiesce,
               []() -> std::optional<std::string> { return std::nullopt; });

  const anvil::Timestamp now{1'000'000, 0};

  auto fired = registry.evaluate(CostClass::kTick, now, 1);
  check(fired.empty(), "no invariant should fire while everything holds");
  check(counter == 1, "tick-class invariants run when tick class is evaluated");

  registry.evaluate(CostClass::kQuiesce, now, 2);
  check(counter == 1, "a quiesce-class evaluation must not run tick-class predicates");

  tripwire = true;
  fired = registry.evaluate(CostClass::kTick, now, 3);
  check(fired.size() == 1, "exactly the tripped invariant should fire");
  check(fired.front().id == "INV-TEST-02", "the violation must name the right invariant");
  check(fired.front().detail == "the tripwire was set",
        "the violation must carry an actionable detail, not just an id");
  check(fired.front().tick == 3, "the violation must record when it happened");

  // INV-SIM-05. Two of the three have never fired, which is exactly the signal
  // the fleet report needs: an assertion nobody has ever seen fail is an
  // assertion nobody has tested.
  const auto never = registry.never_fired();
  check(never.size() == 2, "never_fired must identify invariants that have never triggered");
  check(std::set<std::string>(never.begin(), never.end()) ==
            std::set<std::string>{"INV-TEST-01", "INV-TEST-03"},
        "never_fired must name the untriggered invariants");

  const auto& stats = registry.stats();
  check(stats.at("INV-TEST-01").evaluations == 2, "evaluation counts are tracked per invariant");
  check(stats.at("INV-TEST-02").violations == 1, "violation counts are tracked per invariant");
}

// The reference model is the oracle everything else leans on, so it gets its
// own check rather than being trusted because it looks simple.
void test_reference_model() {
  ReferenceModel model;
  std::vector<Mop> mops;

  Mop append;
  append.type = MopType::kAppend;
  append.key = 1;
  append.element = 42;
  mops.push_back(append);

  Mop read;
  read.type = MopType::kRead;
  read.key = 1;
  mops.push_back(read);

  const auto applied = model.apply(mops);
  check(applied.size() == 2, "apply returns one result per mop");
  check(applied[1].observed == std::vector<Element>{42},
        "a read must observe writes made earlier in the same transaction");

  Mop read_missing;
  read_missing.type = MopType::kRead;
  read_missing.key = 99;
  const auto empty = model.apply({read_missing});
  check(empty[0].observed.empty(), "reading an absent key yields the empty list, not an error");
}

}  // namespace

int main() {
  std::cout << "checker\n";

  test_reference_model();
  test_invariant_registry();
  test_soundness(90);
  test_precision(500);
  test_level_boundaries();
  test_write_skew_is_permitted_under_snapshot_isolation();
  test_realtime_only_matters_at_strict();
  test_verdicts_are_deterministic();
  test_witnesses_are_small_and_readable();

  if (g_failures == 0) {
    std::cout << "checker: all checks passed\n";
    return EXIT_SUCCESS;
  }
  std::cerr << "checker: " << g_failures << " check(s) failed\n";
  return EXIT_FAILURE;
}
