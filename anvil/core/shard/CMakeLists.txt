# anvil_shard -- ranges, the placement driver, and the store that hosts them.
#
# Part of the hermetic core with one deliberate exception, and it is the same
# exception raft makes: store.{h,cc} holds a Runtime, exactly as raft/driver.cc
# does, because something has to own the timers and the links. Everything else
# here is a state machine that cannot tell the time.
#
#   descriptor.{h,cc}  the range descriptor, the lease, and the two-level meta
#                      index the client routes through.
#   topology.{h,cc}    the placement group's replicated state machine: every
#                      descriptor in the cluster, changed one committed entry at
#                      a time. One entry is one apply, which is where the
#                      atomicity of a split comes from.
#   placement.{h,cc}   decide(): a pure function from replicated state to
#                      commands. Purity is INV-SHARD-09, not tidiness.
#   range.{h,cc}       one range's data, lease and triggers. Splits and merges
#                      are entries in this log, which is what makes them atomic
#                      with respect to the transfers around them.
#   router.{h,cc}      the client range cache, invalidated by generation.
#   store.{h,cc}       the node's range server: many groups, one transport, one
#                      tick loop, and quiescence.
#
# Landed in P5. Deferred and noted rather than pretended:
#   * the second level of the meta index is a logical bucket rather than its own
#     Raft group -- there is one meta group in this cluster and nothing to gain
#     from splitting it, and the client still pays for both lookups
#   * a range's applied state is durable through its Raft log rather than
#     through the LSM, so a split's data moves as a log entry. That is correct
#     and it is not how a production store would do it; see CONTEXT.md.

add_library(anvil_shard STATIC
  descriptor.cc
  topology.cc
  placement.cc
  range.cc
  router.cc
  store.cc
)
add_library(anvil::shard ALIAS anvil_shard)

target_include_directories(anvil_shard PUBLIC "${PROJECT_SOURCE_DIR}")

target_link_libraries(anvil_shard
  PUBLIC anvil::core anvil::raft anvil::txn_core anvil::warnings
  PRIVATE anvil::sanitizers)

target_sources(anvil_shard PRIVATE
  descriptor.h
  topology.h
  placement.h
  range.h
  router.h
  store.h
)

anvil_add_hermetic_check(anvil_shard)
