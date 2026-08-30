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

#   mvcc_invariants.{h,cc}
#                      the same idea for versions and transactions (P4).
#                      Split deliberately into audited and live predicates:
#                      anything that has to read the version store goes
#                      through the workload's auditor, because the store is
#                      behind a coroutine and an invariant cannot await.
#
#   raft_invariants.{h,cc}
#                      the god's-eye view of a Raft cluster (P3). Fifteen
#                      predicates over every node's log, term, vote, commit
#                      index, configuration and lease, evaluated while the
#                      cluster runs. This is the file the protocol-aware claim
#                      rests on: none of these questions can be asked from
#                      outside the client API.

add_library(anvil_checker STATIC
  history.cc
  invariant.cc
  elle.cc
  corpus.cc
  raft_invariants.cc
  mvcc_invariants.cc
  shard_invariants.cc
)
add_library(anvil::checker ALIAS anvil_checker)

target_include_directories(anvil_checker PUBLIC "${PROJECT_SOURCE_DIR}")

target_link_libraries(anvil_checker
  PUBLIC anvil::core anvil::raft anvil::mvcc anvil::shard anvil::warnings
  PRIVATE anvil::sanitizers)

target_sources(anvil_checker PRIVATE
  history.h
  invariant.h
  elle.h
  corpus.h
  raft_invariants.h
  mvcc_invariants.h
  shard_invariants.h
)
