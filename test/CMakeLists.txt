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
