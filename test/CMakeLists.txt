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
