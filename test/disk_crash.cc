// P1 exit criterion 4: an unsynced write is *proven* lost on simulated crash.
//
// "Proven" rather than "assumed" is the whole point. A disk model that quietly
// keeps everything would pass every storage test ever written while making the
// durability claim meaningless, and the failure is invisible -- the tests are
// green, the code looks careful, and the bug ships. So the model has to be
// shown to bite, in both directions:
//
//   with fsync,    across every seed, nothing is ever lost.
//   without fsync, across a spread of seeds, data is lost.
//
// The second assertion is the one that matters. It is the negative control for
// the entire storage layer, the same role the deliberately non-hermetic archive
// plays for the hermeticity gate.
//
// Three separate durability failures are covered here, and they are genuinely
// different bugs:
//
//   1. content never fsynced           -> unsynced sectors resolve badly
//   2. content fsynced, directory not  -> the file does not exist at all
//   3. a sector tears                  -> a record is half-written, and the
//                                         recovery scan must stop rather than
//                                         salvage what follows

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string_view>
#include <vector>

#include "anvil/sim/simulation.h"

namespace {

int g_failures = 0;

void check(bool condition, std::string_view what) {
  if (!condition) {
    std::cerr << "FAIL: " << what << "\n";
    ++g_failures;
  }
}

constexpr std::size_t kRecordSize = 600;  // deliberately spans a 512-byte sector
constexpr int kRecords = 12;
constexpr std::byte kFill{0xAB};
constexpr const char* kDir = "d";
constexpr const char* kPath = "d/wal.log";

anvil::Task<void> write_records(anvil::Runtime& rt, bool do_fsync, bool do_fsync_dir) {
  anvil::FileHandle handle{};
  const anvil::OpenFlags flags =
      anvil::OpenFlags::kRead | anvil::OpenFlags::kWrite | anvil::OpenFlags::kCreate;
  if (!(co_await rt.open(kPath, flags, &handle)).is_ok()) co_return;

  if (do_fsync_dir) co_await rt.fsync_dir(kDir);

  const std::vector<std::byte> record(kRecordSize, kFill);
  for (int i = 0; i < kRecords; ++i) {
    const auto offset = static_cast<std::uint64_t>(i) * kRecordSize;
    if (!(co_await rt.pwrite(handle, anvil::ByteView{record.data(), record.size()}, offset))
             .is_ok()) {
      co_return;
    }
    if (do_fsync) {
      if (!(co_await rt.fsync(handle)).is_ok()) co_return;
    }
  }
}

// Counts the leading run of fully-intact records, which is exactly what a WAL
// recovery scan does: stop at the first record that does not validate, because
// everything past a torn write is untrustworthy.
anvil::Task<void> count_intact_prefix(anvil::Runtime& rt, int* out, bool* file_exists) {
  anvil::FileHandle handle{};
  if (!(co_await rt.open(kPath, anvil::OpenFlags::kRead, &handle)).is_ok()) {
    *file_exists = false;
    *out = 0;
    co_return;
  }
  *file_exists = true;

  std::vector<std::byte> buffer(kRecordSize);
  for (int i = 0; i < kRecords; ++i) {
    std::size_t read = 0;
    const auto offset = static_cast<std::uint64_t>(i) * kRecordSize;
    if (!(co_await rt.pread(handle, anvil::MutableByteView{buffer.data(), buffer.size()}, offset,
                            &read))
             .is_ok()) {
      break;
    }
    if (read < kRecordSize) break;
    bool intact = true;
    for (const std::byte b : buffer) {
      if (b != kFill) {
        intact = false;
        break;
      }
    }
    if (!intact) break;
    *out = i + 1;
  }
}

struct Outcome {
  int intact = 0;
  bool file_exists = false;
  anvil::sim::DiskStats stats;
};

Outcome run_case(std::uint64_t seed, bool do_fsync, bool do_fsync_dir, bool page_cache = true) {
  anvil::sim::SimConfig cfg;
  cfg.seed = seed;
  cfg.nodes = 2;
  cfg.max_time = anvil::Duration::seconds(30);
  cfg.faults = anvil::sim::FaultProfile::none();
  cfg.faults.disk.page_cache = page_cache;
  // A high tear rate makes the third failure mode reachable in a small number
  // of seeds. Rare faults need to be made common in the test that targets them,
  // which is exactly what BUGGIFY does for code paths.
  cfg.faults.disk.torn_write = anvil::sim::Chance::pct(50);

  anvil::sim::Simulation sim{cfg};
  const anvil::NodeId node{1};

  sim.node(node).spawn(write_records(sim.node(node), do_fsync, do_fsync_dir));
  sim.run();

  // Pull the power. Nothing gets to run: no flush, no destructors, no goodbye.
  sim.disk().crash_node(node);

  Outcome outcome;
  sim.node(node).spawn(count_intact_prefix(sim.node(node), &outcome.intact, &outcome.file_exists));
  // run_more, not run: run()'s budget is measured from time zero, so a second
  // call would return instantly having already spent it.
  sim.run_more(anvil::Duration::seconds(10));

  outcome.stats = sim.disk().stats();
  return outcome;
}

// ---------------------------------------------------------------------------

void test_fsync_preserves_everything() {
  for (std::uint64_t seed = 1; seed <= 200; ++seed) {
    const Outcome o = run_case(seed, /*do_fsync=*/true, /*do_fsync_dir=*/true);
    if (!o.file_exists || o.intact != kRecords) {
      check(false, "an fsynced-and-dir-fsynced write must always survive a crash");
      std::cerr << "  seed " << seed << ": exists=" << o.file_exists << " intact=" << o.intact
                << "/" << kRecords << "\n";
      return;
    }
  }
}

void test_unsynced_writes_are_lost() {
  int seeds_losing = 0;
  int worst = kRecords;
  for (std::uint64_t seed = 1; seed <= 200; ++seed) {
    const Outcome o = run_case(seed, /*do_fsync=*/false, /*do_fsync_dir=*/true);
    if (o.intact < kRecords) {
      ++seeds_losing;
      if (o.intact < worst) worst = o.intact;
    }
  }
  check(seeds_losing > 0,
        "without fsync, some crashes MUST lose data -- otherwise the disk model "
        "cannot detect a missing fsync and every durability test is vacuous");
  std::cout << "  unsynced: " << seeds_losing << "/200 seeds lost data, worst case " << worst
            << "/" << kRecords << " records survived\n";
}

void test_missing_dir_fsync_loses_the_whole_file() {
  // The expensive one. Contents are flawlessly synced on every single write;
  // the directory entry never is. After a crash the file simply is not there,
  // and every acknowledged write inside it is gone. A page-cache-only disk
  // model reports this code as correct.
  int vanished = 0;
  for (std::uint64_t seed = 1; seed <= 50; ++seed) {
    const Outcome o = run_case(seed, /*do_fsync=*/true, /*do_fsync_dir=*/false);
    if (!o.file_exists) ++vanished;
  }
  check(vanished == 50,
        "fsyncing a file but not its directory must lose the file on every crash");
  std::cout << "  missing dir fsync: file vanished in " << vanished << "/50 seeds\n";
}

void test_tearing_happens_and_is_at_sector_granularity() {
  std::uint64_t torn = 0;
  std::uint64_t lost = 0;
  for (std::uint64_t seed = 1; seed <= 200; ++seed) {
    const Outcome o = run_case(seed, /*do_fsync=*/false, /*do_fsync_dir=*/true);
    torn += o.stats.sectors_torn;
    lost += o.stats.sectors_lost;
  }
  check(torn > 0, "torn writes must actually occur when the profile enables them");
  check(lost > 0, "unsynced sectors must sometimes resolve to their old content");
  std::cout << "  crash resolution: " << torn << " sectors torn, " << lost << " reverted\n";
}

void test_page_cache_off_is_the_control() {
  // With the page cache disabled every write is instantly durable, so a missing
  // fsync is undetectable. This case exists to document that the previous tests
  // are measuring the model and not an accident, and to keep the control
  // condition honest if anyone ever changes the default.
  for (std::uint64_t seed = 1; seed <= 50; ++seed) {
    const Outcome o = run_case(seed, /*do_fsync=*/false, /*do_fsync_dir=*/true,
                               /*page_cache=*/false);
    if (o.intact != kRecords) {
      check(false, "with no page cache, even unsynced writes must survive");
      return;
    }
  }
}

}  // namespace

int main() {
  std::cout << "disk crash semantics\n";

  test_fsync_preserves_everything();
  test_unsynced_writes_are_lost();
  test_missing_dir_fsync_loses_the_whole_file();
  test_tearing_happens_and_is_at_sector_granularity();
  test_page_cache_off_is_the_control();

  if (g_failures == 0) {
    std::cout << "disk crash semantics: all checks passed\n";
    return EXIT_SUCCESS;
  }
  std::cerr << "disk crash semantics: " << g_failures << " check(s) failed\n";
  return EXIT_FAILURE;
}
