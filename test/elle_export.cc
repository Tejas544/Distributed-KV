// P7 exit criterion 2, the Anvil half: emit histories in Jepsen Elle's own
// format, with Anvil's verdict attached, so that a second checker can be asked
// the same question.
//
//   "Zero unexplained disagreements with Jepsen Elle over 10,000 shared
//    histories."
//
// The point of this criterion is worth stating plainly, because it is the one
// deliverable in P7 that cannot be satisfied by writing more of our own code.
// Every other gate in this repository is Anvil checking Anvil: the mutation
// score is our corpus against our checker, the state-space search is our model
// against our invariants, the TLA+ specs are our understanding of the protocol
// written down twice. All of that is worth doing and none of it can catch a
// mistake we made consistently. Elle is a checker with no shared ancestry,
// written by other people, that has found real bugs in real databases. Agreeing
// with it is the only evidence here that is not self-referential.
//
// This binary writes; `tools/elle/` reads and compares. The split is not
// incidental -- Elle is Clojure, and a comparison that required linking a JVM
// into the C++ test suite would be a comparison nobody ever ran.
//
// ---------------------------------------------------------------------------
// The format
// ---------------------------------------------------------------------------
//
// Elle consumes a Jepsen history: a flat sequence of operations, each an EDN
// map, where a transaction is an `:invoke` followed by an `:ok`, `:fail` or
// `:info`. A micro-operation is `[:append key element]` or `[:r key list]`, and
// on the invoke a read's value is `nil` because it has not happened yet.
//
// The three completion types matter and are not interchangeable:
//
//   :ok    committed
//   :fail  definitely did not commit -- nothing it wrote is visible anywhere
//   :info  indeterminate. A commit that timed out says nothing about whether
//          it happened, and a checker that forces it into one of the other two
//          buckets will either invent anomalies or hide them. Anvil's
//          Outcome::kUnknown maps here, and getting this wrong is a large
//          fraction of why two checkers disagree about a correct system.

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "anvil/checker/corpus.h"
#include "anvil/checker/elle.h"
#include "anvil/checker/history.h"

namespace {

using namespace anvil::checker;

// Anvil's anomaly names, in Elle's vocabulary. Where the two disagree about
// naming rather than about the verdict, tools/elle/ reports it as a naming
// difference and not as a disagreement -- see the note there on why a cycle
// that is legitimately both G-single and G2-item gets named differently by two
// correct checkers.
const char* elle_name(Anomaly anomaly) {
  switch (anomaly) {
    case Anomaly::kG0: return ":G0";
    case Anomaly::kG1a: return ":G1a";
    case Anomaly::kG1b: return ":G1b";
    case Anomaly::kG1c: return ":G1c";
    case Anomaly::kGSingle: return ":G-single";
    case Anomaly::kG2Item: return ":G2-item";
    case Anomaly::kVersionOrderConflict: return ":incompatible-order";
    case Anomaly::kDuplicateElement: return ":duplicate-elements";
    case Anomaly::kRealTimeViolation: return ":G0-realtime";
    case Anomaly::kCount: break;
  }
  return ":unknown";
}

const char* completion_of(Outcome outcome) {
  switch (outcome) {
    case Outcome::kCommitted: return ":ok";
    case Outcome::kAborted: return ":fail";
    case Outcome::kUnknown: return ":info";
  }
  return ":info";
}

// One transaction's value vector, at invoke time or at completion time. The
// difference is exactly the reads: an invoke does not know what it will see.
std::string value_of(const Txn& txn, bool completed) {
  std::string out = "[";
  for (const Mop& mop : txn.mops) {
    if (out.size() > 1) out += " ";
    if (mop.type == MopType::kAppend) {
      out += "[:append " + std::to_string(mop.key) + " " + std::to_string(mop.element) + "]";
    } else if (!completed) {
      out += "[:r " + std::to_string(mop.key) + " nil]";
    } else {
      out += "[:r " + std::to_string(mop.key) + " [";
      for (std::size_t i = 0; i < mop.observed.size(); ++i) {
        if (i != 0) out += " ";
        out += std::to_string(mop.observed[i]);
      }
      out += "]]";
    }
  }
  return out + "]";
}

void write_history(std::ostream& out, const History& history) {
  out << "  :history [";
  bool first = true;
  for (const Txn& txn : history.txns()) {
    if (!first) out << "\n            ";
    first = false;
    out << "{:process " << txn.process << " :type :invoke :f :txn :value "
        << value_of(txn, false) << "}";
    out << "\n            {:process " << txn.process << " :type " << completion_of(txn.outcome)
        << " :f :txn :value " << value_of(txn, txn.outcome == Outcome::kCommitted) << "}";
  }
  out << "]\n";
}

void write_case(std::ostream& out, const std::string& name, const History& history,
                const CheckResult& verdict) {
  out << " {:name \"" << name << "\"\n";
  out << "  :anvil {:valid? " << (verdict.valid ? "true" : "false") << "\n";
  out << "          :anomalies #{";
  for (std::size_t i = 0; i < verdict.anomalies.size(); ++i) {
    if (i != 0) out << " ";
    out << elle_name(verdict.anomalies[i]);
  }
  out << "}\n";
  out << "          :transactions " << verdict.transactions << "\n";
  out << "          :edges " << verdict.edges << "}\n";
  write_history(out, history);
  out << " }\n";
}

}  // namespace

int main(int argc, char** argv) {
  // How many of each. The criterion names 10,000 shared histories; the split
  // between correct and anomalous is deliberate and roughly even, because the
  // two directions catch different mistakes. A checker that is too strict is
  // only visible on correct histories, and one that is too permissive is only
  // visible on broken ones -- and this repository has shipped both.
  const std::size_t valid_count = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 5000;
  const std::size_t anomalous_count = argc > 2 ? std::strtoull(argv[2], nullptr, 10) : 5000;
  const std::string path = argc > 3 ? argv[3] : "/tmp/anvil-histories.edn";

  std::ofstream out(path);
  if (!out) {
    std::cerr << "cannot write " << path << "\n";
    return 1;
  }

  out << ";; Generated by anvil_elle_export. One EDN map per history:\n"
      << ";;   :name    what it is\n"
      << ";;   :anvil   Anvil's own verdict, so the comparison needs one file\n"
      << ";;   :history the Jepsen history, for elle.list-append/check\n"
      << "[\n";

  std::size_t written = 0;

  // Correct by construction: run through the serial reference model. Any
  // anomaly reported on one of these, by either checker, is a false positive.
  for (std::size_t i = 0; i < valid_count; ++i) {
    const History history = valid_history(i + 1, 8 + (i % 25), 3 + (i % 7));
    const CheckResult verdict = check(history, IsolationLevel::kSerializable);
    write_case(out, "valid/" + std::to_string(i), history, verdict);
    ++written;
  }

  // Known-bad, one per anomaly class, with the class each is built to contain.
  if (anomalous_count > 0) {
    const auto corpus = anomalous_corpus(20250903, anomalous_count);
    for (std::size_t i = 0; i < corpus.size(); ++i) {
      const CheckResult verdict = check(corpus[i].history, IsolationLevel::kSerializable);
      write_case(out, "anomalous/" + corpus[i].name + "/" + std::to_string(i), corpus[i].history,
                 verdict);
      ++written;
    }
  }

  out << "]\n";
  out.close();

  std::cout << "wrote " << written << " histories to " << path << "\n";
  return 0;
}
