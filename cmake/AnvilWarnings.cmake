# anvil::warnings -- the warning set, and the determinism-relevant flags.
#
# A few of these are not about warnings at all. They are here because they are
# compile flags that affect *behaviour*, and behaviour that varies by compiler
# is the thing INV-SIM-01 exists to catch.

add_library(anvil_warnings INTERFACE)
add_library(anvil::warnings ALIAS anvil_warnings)

if(MSVC)
  target_compile_options(anvil_warnings INTERFACE
    /W4 /permissive- /Zc:__cplusplus /Zc:preprocessor
    /fp:strict            # no FP contraction; see below
    $<$<BOOL:${ANVIL_WERROR}>:/WX>)
else()
  target_compile_options(anvil_warnings INTERFACE
    -Wall
    -Wextra
    -Wpedantic
    -Wshadow
    -Wnon-virtual-dtor
    -Wold-style-cast
    -Wcast-align
    -Wunused
    -Woverloaded-virtual
    -Wconversion            # narrowing is how u64 timestamps become u32 bugs
    -Wsign-conversion
    -Wdouble-promotion
    -Wformat=2
    -Wimplicit-fallthrough
    -Wnull-dereference

    # -- determinism, not style ------------------------------------------
    # Fused multiply-add changes results, and gcc and clang contract in
    # different places. There is no floating point in the core anyway, but this
    # makes an accidental one behave identically on both.
    -ffp-contract=off

    # Function-local statics normally get a thread-safe guard. The core is
    # single-threaded by construction, so the guard is pure overhead -- and its
    # absence keeps __cxa_guard_acquire out of the symbol table, which keeps the
    # hermeticity report readable.
    -fno-threadsafe-statics

    # Signed overflow is UB, and UB makes optimisers make different choices at
    # different -O levels. Wrapping is deterministic and boring, which is what
    # we want from arithmetic in a replayable system.
    -fwrapv

    $<$<BOOL:${ANVIL_WERROR}>:-Werror>)

  # gcc-only extras
  if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    target_compile_options(anvil_warnings INTERFACE
      -Wduplicated-cond
      -Wduplicated-branches
      -Wlogical-op)
    # -Wuseless-cast is deliberately NOT enabled. It fires on
    # static_cast<uint64_t>(x.size()) wherever size_t happens to already be
    # uint64_t -- which is true on LP64 Linux and on this Windows toolchain, but
    # not on 32-bit targets. Those explicit width casts are exactly what keeps
    # the execution digest identical across architectures (INV-SIM-01), so a
    # warning that makes -Werror platform-dependent and pressures us to delete
    # them is working against the thing this project exists to guarantee.
  endif()
endif()
