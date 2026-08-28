# anvil_add_hermetic_check(<target> [EXPECT_VIOLATIONS])
#
# Registers a ctest case that runs tools/hermetic_check.py over the target's
# built archive. EXPECT_VIOLATIONS inverts the verdict, which is how the
# negative test proves the gate can actually fail -- a check that has never been
# observed to fail is indistinguishable from a check that does nothing.
#
# Deliberately a ctest case rather than a POST_BUILD step: a POST_BUILD failure
# is easy to lose in build output, and this gate should be as loud as a failing
# test, because that is what it is.

function(anvil_add_hermetic_check target)
  cmake_parse_arguments(ARG "EXPECT_VIOLATIONS" "NAME" "" ${ARGN})

  if(NOT ANVIL_HERMETIC_CHECK)
    return()
  endif()

  set(_name "hermetic.${target}")
  if(ARG_NAME)
    set(_name "${ARG_NAME}")
  endif()

  set(_args
    "${PROJECT_SOURCE_DIR}/tools/hermetic_check.py"
    --config "${PROJECT_SOURCE_DIR}/tools/hermetic.toml"
    --json   "${PROJECT_BINARY_DIR}/artifacts/hermetic/${target}.json")

  if(ARG_EXPECT_VIOLATIONS)
    list(APPEND _args --expect-violations)
  endif()

  add_test(NAME "${_name}"
    COMMAND "${Python3_EXECUTABLE}" ${_args} "$<TARGET_FILE:${target}>")

  set_tests_properties("${_name}" PROPERTIES
    LABELS "hermetic;gate"
    # If this ever starts taking minutes, nm is being run on something far
    # larger than intended.
    TIMEOUT 120)
endfunction()
