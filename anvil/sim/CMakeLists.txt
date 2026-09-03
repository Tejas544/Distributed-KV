# anvil_sim -- SimRuntime: the deterministic simulator.
#
# This target is NOT hermetic and is not meant to be. It is the component that
# owns the nondeterminism -- real time, real files, the seeded PRNG that stands
# in for the world -- so that everything above the Runtime seam does not have to.
# No hermeticity gate is registered for it, deliberately.
#
# Still outstanding here (P1, docs/ROADMAP.md): fault injection. The network
# model delivers everything, the disk model loses nothing, and the clock does
# not skew. The seams for all three are marked in the corresponding headers.

add_library(anvil_sim STATIC
  trace.cc
  fault_profile.cc
  scheduler.cc
  clock_model.cc
  net_model.cc
  disk_model.cc
  process_model.cc
  fault_injector.cc
  buggify_policy.cc
  minimiser.cc
  dpor.cc
  sim_runtime.cc
  simulation.cc
)
add_library(anvil::sim ALIAS anvil_sim)

target_include_directories(anvil_sim PUBLIC "${PROJECT_SOURCE_DIR}")

# anvil::checker is PUBLIC because scheduler.h exposes InvariantRegistry in its
# interface: invariants are evaluated inside the run loop, not bolted on
# afterwards, so anything that includes the scheduler needs the checker's types.
target_link_libraries(anvil_sim
  PUBLIC anvil::core anvil::checker anvil::warnings
  PRIVATE anvil::sanitizers)

# BUGGIFY is always compiled in for anything that links the simulator. A
# simulator built without the injection sites would silently test a different
# program than the one the fleet runs.
target_compile_definitions(anvil_sim PUBLIC ANVIL_ENABLE_BUGGIFY=1)

target_sources(anvil_sim PRIVATE
  trace.h
  fault_profile.h
  scheduler.h
  clock_model.h
  net_model.h
  disk_model.h
  process_model.h
  fault_injector.h
  buggify_policy.h
  sim_runtime.h
  simulation.h
)

# The anvil-sim executable lives in the root CMakeLists rather than here,
# because it links anvil::workloads, which in turn links anvil::sim -- and an
# ALIAS target has to exist before it is referenced. Defining the binary after
# both libraries is the least surprising way to break that ordering knot.
