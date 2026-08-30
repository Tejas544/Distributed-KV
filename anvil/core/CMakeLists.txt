# anvil_core -- the hermetic state machine.
#
# Every symbol this archive leaves undefined is scrutinised by
# tools/hermetic_check.py. If a change to this target adds a dependency on the
# clock, threads, entropy or syscalls, the gate fails the build. Route it
# through Runtime instead.

add_library(anvil_core STATIC
  buggify.cc
  runtime/runtime.cc
)
add_library(anvil::core ALIAS anvil_core)

target_include_directories(anvil_core PUBLIC "${PROJECT_SOURCE_DIR}")

target_link_libraries(anvil_core
  PUBLIC anvil::warnings
  PRIVATE anvil::sanitizers)

target_compile_definitions(anvil_core PUBLIC
  $<$<BOOL:${ANVIL_ENABLE_BUGGIFY}>:ANVIL_ENABLE_BUGGIFY=1>)

# Headers are listed so IDEs and `cmake --build --target` see them; they carry
# most of the logic in this layer.
target_sources(anvil_core PRIVATE
  types.h
  random.h
  digest.h
  buggify.h
  runtime/task.h
  runtime/runtime.h
)

add_subdirectory(lsm)
add_subdirectory(raft)
add_subdirectory(mvcc)
add_subdirectory(shard)

anvil_add_hermetic_check(anvil_core)
