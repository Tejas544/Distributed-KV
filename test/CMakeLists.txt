# Test targets.
#
# P0 ships two things: the compile-and-link smoke test for the frozen Runtime
# seam, and the negative test that proves the hermeticity gate can fail.
# GoogleTest arrives with the first real unit tests in P1; there is no point
# pulling in a dependency to assert nothing yet.

# ---------------------------------------------------------------------------
# The core must compile, link, and stay hermetic.
# ---------------------------------------------------------------------------
add_executable(anvil_core_smoke core_smoke.cc)
target_link_libraries(anvil_core_smoke PRIVATE anvil::core anvil::sanitizers)
add_test(NAME core.smoke COMMAND anvil_core_smoke)
set_tests_properties(core.smoke PROPERTIES LABELS "smoke" TIMEOUT 60)

# ---------------------------------------------------------------------------
# INV-SIM-01, in-process half: same seed -> same execution.
#
# The cross-compiler and cross-architecture halves cannot run in one process;
# CI does those by comparing `anvil-sim --sweep` output across the build matrix.
# ---------------------------------------------------------------------------
add_executable(anvil_sim_determinism sim_determinism.cc)
target_link_libraries(anvil_sim_determinism PRIVATE anvil::workloads anvil::sanitizers)
add_test(NAME sim.determinism COMMAND anvil_sim_determinism 500)
set_tests_properties(sim.determinism PROPERTIES LABELS "determinism;gate" TIMEOUT 300)

# The CLI path the bug ledger depends on: one seed in, the same run out.
add_test(NAME sim.replay_cli COMMAND anvil-sim --seed 0x8f3a91c40d2e77b1 --verify)
set_tests_properties(sim.replay_cli PROPERTIES LABELS "determinism" TIMEOUT 120)

# ---------------------------------------------------------------------------
# P1: fault injection.
#
# disk_crash is the negative control for the storage layer -- it proves the disk
# model can actually lose data, without which every durability claim above it is
# unfalsifiable. sim_faults runs the replicated counter against the full
# adversary and additionally checks that every fault kind fires and that two
# deliberately planted durability bugs are caught.
# ---------------------------------------------------------------------------
add_executable(anvil_disk_crash disk_crash.cc)
target_link_libraries(anvil_disk_crash PRIVATE anvil::sim anvil::sanitizers)
add_test(NAME sim.disk_crash COMMAND anvil_disk_crash)
set_tests_properties(sim.disk_crash PROPERTIES LABELS "faults;gate" TIMEOUT 300)

# ---------------------------------------------------------------------------
# P1 stream D: the oracles.
#
# INV-SIM-03 (the checker flags every known-bad history) and INV-SIM-04 (it
# accepts every known-good one). The second is the one people skip, and it is
# the one that decides whether the checker survives contact with a real team.
# ---------------------------------------------------------------------------
add_executable(anvil_checker_test checker_test.cc)
target_link_libraries(anvil_checker_test PRIVATE anvil::checker anvil::sanitizers)
add_test(NAME checker.elle COMMAND anvil_checker_test)
set_tests_properties(checker.elle PROPERTIES LABELS "checker;gate" TIMEOUT 300)

# P7 exit criterion 1, at the size the criterion states rather than the size
# that belongs in a unit suite: a reported mutation score over ~200 anomalous
# histories, and zero false positives over 10,000 serial ones. checker.elle
# above asserts the same two properties small and fast; this is the gate that
# produces the number the rest of the suite's credibility rests on.
add_executable(anvil_checker_mutation checker_mutation.cc)
target_link_libraries(anvil_checker_mutation PRIVATE anvil::checker anvil::sanitizers)
add_test(NAME checker.mutation COMMAND anvil_checker_mutation 200 10000)
set_tests_properties(checker.mutation PROPERTIES LABELS "checker;verification;gate" TIMEOUT 1800)

add_executable(anvil_sim_faults sim_faults.cc)
target_link_libraries(anvil_sim_faults PRIVATE anvil::workloads anvil::sanitizers)
add_test(NAME sim.faults COMMAND anvil_sim_faults 150)
set_tests_properties(sim.faults PROPERTIES LABELS "faults;gate" TIMEOUT 900)

# ---------------------------------------------------------------------------
# P2: the storage engine.
#
# lsm_test covers the building blocks in isolation -- a bug in internal key
# ordering or in the block encoder is far cheaper to find here than as "a read
# returned the wrong version under a specific compaction interleaving".
#
# lsm_crash is the engine under the disk model's full adversary, and carries
# P2's exit criteria: no acknowledged write lost, every corruption detected and
# none served, no orphaned files, and four planted durability bugs all caught.
# The other four planted bugs are source mutations, driven by
# tools/lsm_mutations.sh, which cannot run from ctest because it rewrites the
# tree it is testing.
# ---------------------------------------------------------------------------
add_executable(anvil_lsm_test lsm_test.cc)
target_link_libraries(anvil_lsm_test PRIVATE anvil::lsm anvil::sim anvil::sanitizers)
add_test(NAME lsm.units COMMAND anvil_lsm_test)
set_tests_properties(lsm.units PROPERTIES LABELS "lsm" TIMEOUT 300)

add_executable(anvil_lsm_crash lsm_crash.cc)
target_link_libraries(anvil_lsm_crash PRIVATE anvil::lsm anvil::sim anvil::sanitizers)
add_test(NAME lsm.crash COMMAND anvil_lsm_crash 60)
set_tests_properties(lsm.crash PROPERTIES LABELS "lsm;faults;gate" TIMEOUT 900)

# ---------------------------------------------------------------------------
# P3: consensus.
#
# raft_test is the protocol itself with no simulator underneath -- elections,
# pre-vote, CheckQuorum, conflict backtracking, the Figure-8 commit restriction,
# joint consensus, learners, snapshot install, leases and ReadIndex, each
# constructed directly rather than waited for. raft_faults is the exit criteria:
# safety and liveness under the full adversary, determinism, the pause-versus-
# lease experiment, and the ten-bug seeded-mutation drill with its API-visibility
# column.
# ---------------------------------------------------------------------------
add_executable(anvil_raft_test raft_test.cc)
target_link_libraries(anvil_raft_test PRIVATE anvil::raft anvil::sim anvil::sanitizers)
add_test(NAME raft.units COMMAND anvil_raft_test)
set_tests_properties(raft.units PROPERTIES LABELS "raft" TIMEOUT 300)

add_executable(anvil_raft_faults raft_faults.cc)
target_link_libraries(anvil_raft_faults PRIVATE anvil::workloads anvil::checker anvil::sanitizers)
add_test(NAME raft.faults COMMAND anvil_raft_faults 40)
set_tests_properties(raft.faults PROPERTIES LABELS "raft;faults;gate" TIMEOUT 3600)

# ---------------------------------------------------------------------------
# P4: MVCC and single-node transactions.
#
# mvcc_test is the mechanism with nothing running: key encoding order, snapshot
# reads, intents, first-committer-wins, wound-wait, the safepoint, and the GC
# boundary version -- each constructed rather than waited for, including the
# mutant of the boundary rule so the test that guards it is known to fail when
# the rule is wrong.
#
# mvcc_faults is the exit criteria: a collector running as aggressively as the
# run allows against long readers holding snapshots open, checked by a god's-eye
# auditor against a model of every version ever committed; wound-wait under
# deliberate deadlocks; determinism; the isolation level confirmed rather than
# asserted, by running one write-skew history through the checker at two levels;
# and the seeded-mutation drill, whose rows are classified as must-detect,
# control, or equivalent-with-an-argument.
#
# It is fast -- the whole sweep is well under a second -- because the workload
# stops the clock when its writers are done. That was not always true; see
# ANV-0028.
# ---------------------------------------------------------------------------
add_executable(anvil_mvcc_test mvcc_test.cc)
target_link_libraries(anvil_mvcc_test PRIVATE anvil::mvcc anvil::sim anvil::sanitizers)
add_test(NAME mvcc.units COMMAND anvil_mvcc_test)
set_tests_properties(mvcc.units PROPERTIES LABELS "mvcc" TIMEOUT 300)

add_executable(anvil_mvcc_faults mvcc_faults.cc)
target_link_libraries(anvil_mvcc_faults PRIVATE anvil::workloads anvil::checker anvil::sanitizers)
add_test(NAME mvcc.faults COMMAND anvil_mvcc_faults 40)
set_tests_properties(mvcc.faults PROPERTIES LABELS "mvcc;faults;gate" TIMEOUT 900)

# ---------------------------------------------------------------------------
# P5: sharding, split/merge, and the placement driver.
#
# shard_test is the mechanism with nothing running: descriptor encoding, the
# coverage tiling, split and merge preconditions, the two-level meta index, the
# client range cache, the lease rule with its two clock bounds, and the
# placement driver's decisions -- each constructed rather than waited for,
# including the non-atomic split so the thing that guards it is known to fail
# when the rule is wrong.
#
# shard_faults is the exit criteria: continuous splits, merges, replica changes
# and lease moves running *during* a transactional workload under the full
# adversary, with the total balance as the client-visible oracle and nine
# INV-SHARD-* predicates as the internal one; determinism with Raft groups
# created and destroyed mid-run; and the seeded-mutation drill, whose rows are
# classified as must-detect, control, or equivalent-with-an-argument.
# ---------------------------------------------------------------------------
add_executable(anvil_shard_test shard_test.cc)
target_link_libraries(anvil_shard_test PRIVATE anvil::shard anvil::sim anvil::sanitizers)
add_test(NAME shard.units COMMAND anvil_shard_test)
set_tests_properties(shard.units PROPERTIES LABELS "shard" TIMEOUT 300)

add_executable(anvil_shard_faults shard_faults.cc)
target_link_libraries(anvil_shard_faults PRIVATE anvil::workloads anvil::checker anvil::sanitizers)
add_test(NAME shard.faults COMMAND anvil_shard_faults 24)
set_tests_properties(shard.faults PROPERTIES LABELS "shard;faults;gate" TIMEOUT 1800)

# ---------------------------------------------------------------------------
# P6: distributed transactions -- three engines behind one coordinator.
#
# txn_test is the mechanism with nothing running: the version store, the
# record status lattice (pending -> staging -> committed, pending -> aborted,
# and nothing out of a terminal state), the opaque command interface a range
# actually applies, the GC boundary version, and the two timestamp sources --
# the replicated oracle's monotonicity and the hybrid logical clock's
# uncertainty interval -- each constructed rather than waited for.
#
# txn_faults is the exit criteria: a coordinator running all three isolation
# levels over a sharded, fault-injected cluster, checked by the Elle-style
# checker at the level each engine claims and by the bank's conserved total as
# the client-visible oracle; and the seeded-mutation drill over the
# deliberate-bug flags in CoordinatorOptions and StoreOptions.
# ---------------------------------------------------------------------------
add_executable(anvil_txn_test txn_test.cc)
target_link_libraries(anvil_txn_test PRIVATE anvil::txn_core anvil::sanitizers)
add_test(NAME txn.units COMMAND anvil_txn_test)
set_tests_properties(txn.units PROPERTIES LABELS "txn" TIMEOUT 300)

add_executable(anvil_txn_faults txn_faults.cc)
target_link_libraries(anvil_txn_faults PRIVATE anvil::workloads anvil::checker anvil::sanitizers)
add_test(NAME txn.faults COMMAND anvil_txn_faults 20)
set_tests_properties(txn.faults PROPERTIES LABELS "txn;faults;gate" TIMEOUT 1800)

# ---------------------------------------------------------------------------
# P7 exit criterion 2: cross-validation against Jepsen's Elle.
#
# This binary only writes: 10,000 histories in Elle's own format, each carrying
# Anvil's verdict, for tools/elle/ to read back. The comparison itself needs a
# JVM and Clojure and therefore cannot be a ctest target without making the
# whole suite depend on them -- so what is registered here is the export, and
# `tools/elle_cross.sh` runs both halves.
#
# It is the only gate in the tree that is not Anvil checking Anvil, which is
# the entire reason it is worth the dependency.
# ---------------------------------------------------------------------------
add_executable(anvil_elle_export elle_export.cc)
target_link_libraries(anvil_elle_export PRIVATE anvil::checker anvil::sanitizers)
add_test(NAME checker.elle_export COMMAND anvil_elle_export 200 200 /tmp/anvil-histories-smoke.edn)
set_tests_properties(checker.elle_export PROPERTIES LABELS "checker;verification" TIMEOUT 300)

# ---------------------------------------------------------------------------
# P7 exit criterion 3, second half: trace validation.
#
# Writes one run of anvil::raft::RaftNode in spec/Raft.tla's variable
# vocabulary. `tools/trace_validate.sh` drives it across seeds and replays each
# trace through TLC, which is why the grading is not here: the comparison needs
# a JVM, and making the C++ suite depend on one is how a gate stops being run.
#
# The ctest entry is a smoke test -- the exporter runs and produces a module --
# not the conformance check itself.
# ---------------------------------------------------------------------------
add_executable(anvil_trace_export trace_export.cc)
target_link_libraries(anvil_trace_export PRIVATE anvil::sim anvil::sanitizers)
add_test(NAME verification.trace_export COMMAND anvil_trace_export 1 60 /tmp/anvil-trace-smoke.tla)
set_tests_properties(verification.trace_export PROPERTIES LABELS "verification" TIMEOUT 300)

# ---------------------------------------------------------------------------
# P7 exit criterion 4: systematic exploration.
#
# Every reachable state of a tiny Raft configuration, with the real state
# machine and no simulator underneath it, plus the seeded-mutation drill over
# the same class. This is the gate that reaches the interleavings the fault
# sweep would need thousands of seeds and a BUGGIFY site to sample.
#
# It takes about six and a half minutes, and most of that is the five mutations
# that are *not* detected: each one has to traverse the whole 2.2M-state class
# to establish that it stays silent, which is the point of running them.
# ---------------------------------------------------------------------------
add_executable(anvil_dpor_test dpor_test.cc)
target_link_libraries(anvil_dpor_test PRIVATE anvil::sim anvil::sanitizers)
add_test(NAME verification.dpor COMMAND anvil_dpor_test)
set_tests_properties(verification.dpor PROPERTIES LABELS "verification;gate" TIMEOUT 1800)

# ---------------------------------------------------------------------------
# P7 exit criterion 5: the fault-schedule minimiser.
#
# A ledger row whose `faults_minimised` field says "everything the seed drew" is
# a row nobody can reason about. This is the gate that says the field is
# computed rather than guessed, and it is graded against causes that are known
# in advance rather than merely against a smaller number.
# ---------------------------------------------------------------------------
add_executable(anvil_minimiser_test minimiser_test.cc)
target_link_libraries(anvil_minimiser_test PRIVATE anvil::workloads anvil::sanitizers)
add_test(NAME verification.minimiser COMMAND anvil_minimiser_test 400)
set_tests_properties(verification.minimiser PROPERTIES LABELS "verification;gate" TIMEOUT 900)

# ---------------------------------------------------------------------------
# The gate's negative test.
#
# This archive is deliberately full of forbidden symbols. The test passes only
# if hermetic_check.py rejects it. Without this, "hermetic_check passed" is an
# unfalsifiable statement.
# ---------------------------------------------------------------------------
add_library(anvil_hermetic_negative STATIC hermetic/negative_wallclock.cc)
target_include_directories(anvil_hermetic_negative PRIVATE "${PROJECT_SOURCE_DIR}")

# The whole point of this target is to do forbidden things, so the warning set
# that polices the rest of the tree would only get in the way.
if(NOT MSVC)
  target_compile_options(anvil_hermetic_negative PRIVATE -w)
endif()

anvil_add_hermetic_check(anvil_hermetic_negative
  EXPECT_VIOLATIONS
  NAME hermetic.negative_control)
