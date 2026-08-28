# anvil_lsm -- the storage engine.
#
# Part of the hermetic core: every byte it reads or writes goes through the
# Runtime interface, so the same engine runs against the simulated disk (which
# tears writes at sector granularity and loses directory entries) and against a
# real one. tools/hermetic_check.py enforces that.
#
# Landed in P2. Deferred, and noted rather than pretended:
#   * block compression (LZ4 / Zstd) -- external dependencies, no correctness
#     property attached
#   * ribbon filters -- a space/CPU tradeoff against Bloom, same guarantees
#   * size-tiered compaction -- leveled only for now
#   * reverse iteration
#   * streaming k-way merge in compaction; it currently buffers its inputs

add_library(anvil_lsm STATIC
  format.cc
  wal.cc
  memtable.cc
  sstable.cc
  version.cc
  db.cc
)
add_library(anvil::lsm ALIAS anvil_lsm)

target_include_directories(anvil_lsm PUBLIC "${PROJECT_SOURCE_DIR}")

target_link_libraries(anvil_lsm
  PUBLIC anvil::core anvil::warnings
  PRIVATE anvil::sanitizers)

target_sources(anvil_lsm PRIVATE
  format.h
  wal.h
  memtable.h
  sstable.h
  version.h
  db.h
)

# The engine must stay as hermetic as the rest of the core. It is the layer most
# likely to reach for a real syscall by accident -- it is, after all, the one
# doing all the I/O.
anvil_add_hermetic_check(anvil_lsm)
