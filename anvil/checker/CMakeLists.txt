# anvil_checker -- the oracles.
#
# Starts in P1 with the reference model and the invariant framework, because
# every layer after that should arrive with an oracle already waiting for it.
# Completed in P7.
#
#   reference_model.{h,cc}  serial std::map + transaction log; deliberately naive
#   invariant.{h,cc}        registration, cost classes, violation reporting
#   history.{h,cc}          recorded client operations with invoke/complete times
#   dsg.{h,cc}              direct serialization graph: ww / wr / rw edges
#   adya.{h,cc}             G0, G1a, G1b, G1c, G-single, G2-item, G2
#   tarjan.h                SCC, plus minimal-cycle witness extraction
#   mutate/                 the anomalous-history corpus the checker must catch
#
# The corpus is not optional. INV-SIM-03 and INV-SIM-04 are what make the
# checker's verdicts mean anything: it must flag every known-bad history and
# accept every known-good one. A checker that has never been observed to fire
# is not evidence of correctness.

add_library(anvil_checker INTERFACE)
add_library(anvil::checker ALIAS anvil_checker)
target_link_libraries(anvil_checker INTERFACE anvil::core)
