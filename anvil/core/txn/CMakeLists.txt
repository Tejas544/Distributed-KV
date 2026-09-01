# The transaction layer, in two targets rather than one.
#
# The split is not cosmetic: it is where the dependency actually runs. A range
# holds versions, intents and transaction records, so `anvil::shard` depends on
# the *data* half of this layer; and the coordinator drives round trips through
# a ShardStore, so it depends on `anvil::shard`. One target would be a cycle.
#
#   anvil::txn_core   timestamp, record, store, command. No Raft driver, no
#                     Runtime, no knowledge that ranges exist. Pure data and
#                     pure functions over it.
#   anvil::txn        the coordinator: begin / get / put / commit, with the
#                     isolation level as a setting rather than a codebase.
#
# Landed in P6. Deferred and noted rather than pretended:
#   * the watch change feed and follower reads at a closed timestamp
#     (INV-TXN-13, INV-TXN-14). Both need closed timestamps published by the
#     placement driver, and neither is load-bearing for the guarantee matrix.
#   * a distributed deadlock *detector*. Wound-wait by start timestamp makes a
#     wait-for cycle impossible rather than resolvable, and INV-TXN-12 checks
#     that none ever forms -- which is the stronger statement.

add_library(anvil_txn_core STATIC
  timestamp.cc
  record.cc
  store.cc
  command.cc
)
add_library(anvil::txn_core ALIAS anvil_txn_core)
target_include_directories(anvil_txn_core PUBLIC "${PROJECT_SOURCE_DIR}")
target_link_libraries(anvil_txn_core
  PUBLIC anvil::core anvil::raft anvil::warnings
  PRIVATE anvil::sanitizers)
target_sources(anvil_txn_core PRIVATE timestamp.h record.h store.h command.h)
anvil_add_hermetic_check(anvil_txn_core)

add_library(anvil_txn STATIC coordinator.cc)
add_library(anvil::txn ALIAS anvil_txn)
target_include_directories(anvil_txn PUBLIC "${PROJECT_SOURCE_DIR}")
target_link_libraries(anvil_txn
  PUBLIC anvil::txn_core anvil::shard anvil::warnings
  PRIVATE anvil::sanitizers)
target_sources(anvil_txn PRIVATE coordinator.h)
anvil_add_hermetic_check(anvil_txn)
