# Workloads: the programs the simulator runs.
#
# pingpong is the P0 determinism vehicle. The real ones -- YCSB A-F, a TPC-C
# subset, bank-transfer, list-append and register workloads for the checker, and
# chaos-admin -- arrive alongside the layers they exercise.

add_library(anvil_workloads STATIC
  pingpong.cc
  counter.cc
  raft_kv.cc
  mvcc_txn.cc
  shard_kv.cc
  txn_bank.cc
)
add_library(anvil::workloads ALIAS anvil_workloads)

target_include_directories(anvil_workloads PUBLIC "${PROJECT_SOURCE_DIR}")

target_link_libraries(anvil_workloads
  PUBLIC anvil::sim anvil::checker anvil::raft anvil::mvcc anvil::shard anvil::txn anvil::warnings
  PRIVATE anvil::sanitizers)

target_sources(anvil_workloads PRIVATE pingpong.h counter.h raft_kv.h mvcc_txn.h shard_kv.h
                                     txn_bank.h)
