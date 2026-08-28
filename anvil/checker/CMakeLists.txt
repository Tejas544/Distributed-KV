# anvil_checker -- the oracles.
#
# Two independent ways of deciding whether the system is behaving, and they
# catch different things:
#
#   invariant.{h,cc}   protocol-aware checking from the *inside*. Predicates
#                      over global state, evaluated while the system runs, with
#                      cost classes so the cheap ones can run after every event.
#                      Catches internal corruption that never reaches a client.
#
#   elle.{h,cc}        black-box checking from the *outside*. Recovers version
#                      orders from list-append histories, builds the direct
#                      serialization graph, and reports minimal cycles
#                      classified by Adya's taxonomy.
#
# corpus.{h,cc} is not optional and is not scaffolding. INV-SIM-03 and INV-SIM-04
# are what make the checker's verdicts mean anything: it must flag every
# known-bad history and accept every known-good one. A checker that has never
# been observed to fire is not evidence of correctness, and one with false
# positives gets argued with and then switched off.
#
# Still to come in P7: cross-validation against Jepsen's Elle, predicate
# anti-dependencies (G2 proper), and the trace-validation harness.

add_library(anvil_checker STATIC
  history.cc
  invariant.cc
  elle.cc
  corpus.cc
)
add_library(anvil::checker ALIAS anvil_checker)

target_include_directories(anvil_checker PUBLIC "${PROJECT_SOURCE_DIR}")

target_link_libraries(anvil_checker
  PUBLIC anvil::core anvil::warnings
  PRIVATE anvil::sanitizers)

target_sources(anvil_checker PRIVATE
  history.h
  invariant.h
  elle.h
  corpus.h
)
