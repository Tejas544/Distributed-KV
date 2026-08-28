cmake_minimum_required(VERSION 3.24)

project(anvil
  VERSION 0.1.0
  DESCRIPTION "Distributed transactional KV store, validated by deterministic simulation"
  LANGUAGES CXX)

if(PROJECT_SOURCE_DIR STREQUAL PROJECT_BINARY_DIR)
  message(FATAL_ERROR "In-source builds are not supported. Use: cmake -B build")
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)          # -std=c++20, never -std=gnu++20
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)  # clang-tidy needs this

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "" FORCE)
endif()

option(ANVIL_BUILD_TESTS      "Build the test suite"                        ON)
option(ANVIL_WERROR           "Treat warnings as errors"                    ON)
option(ANVIL_HERMETIC_CHECK   "Run the hermeticity gate as part of ctest"   ON)
option(ANVIL_ENABLE_BUGGIFY   "Compile BUGGIFY sites in (simulation only)"  OFF)
set(ANVIL_SANITIZER "none" CACHE STRING "none|address|undefined|thread|memory")
set_property(CACHE ANVIL_SANITIZER PROPERTY STRINGS none address undefined thread memory)

list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")
include(AnvilWarnings)
include(AnvilSanitizers)
include(AnvilHermetic)

find_package(Python3 3.11 COMPONENTS Interpreter)
if(NOT Python3_FOUND AND ANVIL_HERMETIC_CHECK)
  message(WARNING
    "Python 3.11+ not found; the hermeticity gate will be skipped. "
    "This is the check that makes the determinism claim credible -- do not "
    "leave it off in CI.")
  set(ANVIL_HERMETIC_CHECK OFF)
endif()

# ---------------------------------------------------------------------------
# ccache, if present. The nightly seed fleet rebuilds a lot.
# ---------------------------------------------------------------------------
find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM)
  set(CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
endif()

# Before any add_subdirectory: anvil_add_hermetic_check() registers ctest cases
# from inside the component directories, and add_test() is only honoured if
# testing was enabled in an enclosing scope first.
if(ANVIL_BUILD_TESTS)
  enable_testing()
endif()

add_subdirectory(anvil/core)
add_subdirectory(anvil/sim)
add_subdirectory(anvil/prod)
add_subdirectory(anvil/checker)
add_subdirectory(workloads)

# ---------------------------------------------------------------------------
# anvil-sim: run one seed, print the digest.
#
# The entire debugging loop is meant to be this binary plus a 64-bit integer.
# Defined here rather than in anvil/sim because it links anvil::workloads, which
# links anvil::sim -- and an ALIAS target must exist before it is referenced.
# ---------------------------------------------------------------------------
add_executable(anvil-sim anvil/sim/sim_main.cc)
target_link_libraries(anvil-sim PRIVATE anvil::workloads anvil::sanitizers)

if(ANVIL_BUILD_TESTS)
  add_subdirectory(test)
endif()

message(STATUS "")
message(STATUS "anvil ${PROJECT_VERSION}")
message(STATUS "  build type ....... ${CMAKE_BUILD_TYPE}")
message(STATUS "  compiler ......... ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
message(STATUS "  sanitizer ........ ${ANVIL_SANITIZER}")
message(STATUS "  BUGGIFY .......... ${ANVIL_ENABLE_BUGGIFY}")
message(STATUS "  hermetic gate .... ${ANVIL_HERMETIC_CHECK}")
message(STATUS "  ccache ........... ${CCACHE_PROGRAM}")
message(STATUS "")
