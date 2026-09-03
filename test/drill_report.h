// One machine-readable line per seeded-mutation drill row.
//
// P8 exit criterion 6 asks for "the full seeded-mutation suite: every deliberate
// bug from every phase, re-run as one report -- detection rate, mean
// simulated-time-to-detect, and API visibility". The drills already exist, one
// per phase, each inside the fault suite that owns it. Moving them into a single
// binary would mean six harnesses reimplemented in a seventh, which is how a
// report ends up measuring something other than what ships.
//
// So each drill keeps its own harness and its own human-readable table, and
// gains one extra line per row that a script can parse. `tools/mutation_report.py`
// runs the suites and merges. The human table stays exactly as it was, because
// it is what somebody debugging a single phase actually reads.
//
// The API-visibility column is the reason the format has a field for it. It is
// the evidence for the whole protocol-aware claim: every `no` is a bug class
// that an outside-in checker structurally cannot find, and a report that drops
// the column is a report that has thrown away the argument.

#ifndef ANVIL_TEST_DRILL_REPORT_H_
#define ANVIL_TEST_DRILL_REPORT_H_

#include <cstdint>
#include <iostream>
#include <string>

namespace anvil::testing {

// `classification` is one of:
//   must-detect   a deliberate bug the suite is required to catch
//   control       a knob that changes configuration rather than correctness,
//                 and must stay silent
//   equivalent    genuinely undetectable in this configuration, with the
//                 argument written down beside it in the suite
//   covered       not expected from this sweep; a named test is the detector
//
// A row that is neither detected nor classified is the one worth finding in the
// merged report, which is why the field is mandatory rather than optional.
inline void emit_drill(const char* phase, const char* workload, const std::string& mutation,
                       std::uint64_t detected, std::uint64_t runs, std::uint64_t by_invariant,
                       std::uint64_t api_visible, std::int64_t first_detect_ms,
                       const std::string& fired, const char* classification) {
  std::cout << "DRILL|" << phase << '|' << workload << '|' << mutation << '|' << detected << '|'
            << runs << '|' << by_invariant << '|' << api_visible << '|' << first_detect_ms << '|'
            << (fired.empty() ? "-" : fired) << '|' << classification << '\n';
}

}  // namespace anvil::testing

#endif  // ANVIL_TEST_DRILL_REPORT_H_
