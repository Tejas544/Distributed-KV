# anvil_raft -- consensus.
#
# Part of the hermetic core. The state machine (raft.cc) touches nothing but
# memory; the driver (driver.cc) is the only file here that holds a Runtime, and
# every byte it persists or sends goes through that interface. The same
# consensus code therefore runs against the simulated network -- which drops,
# duplicates, reorders and partitions -- and against a real one.
#
# Landed in P3. Deferred, and noted rather than pretended:
#   * witness replicas and quorum leases
#   * asynchronous log writes (leader acks before its own fsync); this changes
#     the durability argument, so it belongs behind its own flag and its own
#     invariant rather than as an unremarked default
#   * follower reads -- they need closed timestamps, which arrive in P6

add_library(anvil_raft STATIC
  config.cc
  log.cc
  message.cc
  raft.cc
  storage.cc
  driver.cc
)
add_library(anvil::raft ALIAS anvil_raft)

target_include_directories(anvil_raft PUBLIC "${PROJECT_SOURCE_DIR}")

target_link_libraries(anvil_raft
  PUBLIC anvil::core anvil::lsm anvil::warnings
  PRIVATE anvil::sanitizers)

target_sources(anvil_raft PRIVATE
  types.h
  config.h
  log.h
  message.h
  raft.h
  storage.h
  driver.h
)

anvil_add_hermetic_check(anvil_raft)
