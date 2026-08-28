// Deliberately non-hermetic. Do not fix this file.
//
// This is the negative test for tools/hermetic_check.py, and it satisfies
// exit criterion 1 of P0: "hermetic_check.py passes, and provably fails when a
// clock_gettime call is added."
//
// The reasoning is the same one behind the seeded-mutation drills in
// docs/ROADMAP.md. A gate that has only ever been observed to pass is
// indistinguishable from a gate that does nothing, and a green build then
// proves exactly nothing. So we keep one archive around that the gate must
// reject, and ctest asserts the rejection.
//
// Every deny category in tools/hermetic.toml gets at least one representative
// here, so that deleting a rule from the config breaks this test rather than
// silently widening what the core is allowed to do.

#include <ctime>

#include <cstdint>
#include <cstdio>
#include <cstdlib>

#if defined(_WIN32)
#include <windows.h>
#else
#include <fcntl.h>
#include <pthread.h>
#include <unistd.h>
#endif

namespace anvil_hermetic_negative {

// wall-clock
std::uint64_t read_the_clock() {
#if defined(_WIN32)
  FILETIME ft;
  GetSystemTimeAsFileTime(&ft);
  return (static_cast<std::uint64_t>(ft.dwHighDateTime) << 32) | ft.dwLowDateTime;
#else
  timespec ts{};
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return static_cast<std::uint64_t>(ts.tv_sec);
#endif
}

// unseeded-entropy
int roll_a_die() { return std::rand() % 6; }

// process-and-environment
const char* read_the_environment() { return std::getenv("ANVIL_SOMETHING"); }

// console-io
void print_something() { std::puts("anvil should never do this"); }

#if !defined(_WIN32)
// file-io
int open_a_file() { return ::open("/tmp/anvil-negative", O_RDONLY); }

// threads
void* nothing(void*) { return nullptr; }
int start_a_thread() {
  pthread_t t{};
  return pthread_create(&t, nullptr, &nothing, nullptr);
}
#endif

// Referenced from a single exported function so no sane linker or optimiser
// discards the calls before nm gets to look at the archive.
extern "C" std::uint64_t anvil_hermetic_negative_touch_everything() {
  std::uint64_t acc = read_the_clock();
  acc += static_cast<std::uint64_t>(roll_a_die());
  acc += read_the_environment() != nullptr ? 1u : 0u;
  print_something();
#if !defined(_WIN32)
  acc += static_cast<std::uint64_t>(open_a_file());
  acc += static_cast<std::uint64_t>(start_a_thread());
#endif
  return acc;
}

}  // namespace anvil_hermetic_negative
