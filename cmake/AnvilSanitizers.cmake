# anvil::sanitizers -- ASan/UBSan/TSan/MSan wiring.
#
# Note the asymmetry: ASan and UBSan run against the *simulator*, where they are
# enormously valuable because a seed reproduces the exact allocation sequence.
# TSan only makes sense against the *production* runtime, since the core is
# single-threaded and TSan would have nothing to observe there.
#
# A caveat worth knowing before it wastes an afternoon: sanitizers change
# allocation addresses and therefore change any behaviour that depends on
# pointer values. If a seed reproduces under a normal build but not under ASan,
# that is not a flaky test -- it is evidence of an address dependence in the
# core, which is itself a determinism bug worth a ledger row.

add_library(anvil_sanitizers INTERFACE)
add_library(anvil::sanitizers ALIAS anvil_sanitizers)

if(ANVIL_SANITIZER STREQUAL "none")
  return()
endif()

if(MSVC)
  if(ANVIL_SANITIZER STREQUAL "address")
    target_compile_options(anvil_sanitizers INTERFACE /fsanitize=address)
  else()
    message(WARNING "MSVC only supports ANVIL_SANITIZER=address; ignoring '${ANVIL_SANITIZER}'")
  endif()
  return()
endif()

set(_anvil_san_flags "")

if(ANVIL_SANITIZER STREQUAL "address")
  set(_anvil_san_flags -fsanitize=address,undefined -fno-sanitize-recover=all)
elseif(ANVIL_SANITIZER STREQUAL "undefined")
  set(_anvil_san_flags -fsanitize=undefined -fno-sanitize-recover=all)
elseif(ANVIL_SANITIZER STREQUAL "thread")
  set(_anvil_san_flags -fsanitize=thread)
elseif(ANVIL_SANITIZER STREQUAL "memory")
  if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message(FATAL_ERROR "ANVIL_SANITIZER=memory requires clang")
  endif()
  set(_anvil_san_flags -fsanitize=memory -fsanitize-memory-track-origins=2)
else()
  message(FATAL_ERROR "unknown ANVIL_SANITIZER '${ANVIL_SANITIZER}'")
endif()

target_compile_options(anvil_sanitizers INTERFACE
  ${_anvil_san_flags} -fno-omit-frame-pointer -g)
target_link_options(anvil_sanitizers INTERFACE ${_anvil_san_flags})
