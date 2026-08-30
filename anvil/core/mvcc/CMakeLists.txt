# anvil_mvcc -- versions, intents, locks and single-node transactions.
#
# Part of the hermetic core: nothing here reads a clock, spawns a thread or
# makes a syscall. Timestamps arrive as arguments and storage arrives as an
# lsm::Db, which is what lets the same code run under the simulator's disk and
# under a real one.
#
# The layering is deliberate and worth stating, because it is what made the P4
# sweep useful as a test of P2 as well as of itself:
#
#   key.{h,cc}         the encoding. Inverted commit timestamps, so a forward
#                      scan yields newest-first and a snapshot read is a seek.
#   mvcc.{h,cc}        the version store: reads at a timestamp, intents, atomic
#                      multi-key resolution, and garbage collection.
#   lock_table.{h,cc}  wound-wait, plus the wait-for graph the deadlock check
#                      runs over.
#   txn.{h,cc}         transactions: snapshots, first-committer-wins, commit and
#                      abort, the GC safepoint, and the retry path for outcomes
#                      whose write did not survive the disk.
#
# Landed in P4. Deferred and noted rather than pretended:
#   * the SSI certifier -- read spans and write sets are recorded here, but the
#     engine that turns them into aborts needs distributed conflict tracking and
#     belongs in P6
#   * crash recovery of in-flight transactions; intents are durable, the
#     transaction table is not, and rebuilding outcomes after a crash is P5's

add_library(anvil_mvcc STATIC
  key.cc
  mvcc.cc
  lock_table.cc
  txn.cc
)
add_library(anvil::mvcc ALIAS anvil_mvcc)

target_include_directories(anvil_mvcc PUBLIC "${PROJECT_SOURCE_DIR}")

target_link_libraries(anvil_mvcc
  PUBLIC anvil::core anvil::lsm anvil::warnings
  PRIVATE anvil::sanitizers)

target_sources(anvil_mvcc PRIVATE
  key.h
  mvcc.h
  lock_table.h
  txn.h
)

anvil_add_hermetic_check(anvil_mvcc)
