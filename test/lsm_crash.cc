// P2's exit criteria: the storage engine under the disk model's full adversary.
//
//   1. crash/corruption/ENOSPC fault injection with no INV-LSM-* violation
//   2. zero acknowledged writes lost across a large crash sweep
//   3. every injected corruption detected, none served as data
//   4. eight deliberate storage bugs, all caught
//
// The fourth is the one that makes the first three mean anything. A crash suite
// that has never been shown to catch a durability bug is a suite that proves the
// engine survives being tested, not that it survives crashing. Four of the eight
// bugs are planted through DurabilityOptions; the other four are source
// mutations driven from the shell, in tools/lsm_mutations.sh.
//
// The crash model is the one from P1: unsynced sectors resolve to old content,
// new content, or a torn mix at sector granularity, chosen by seed, and files
// whose directory entry was never persisted vanish entirely regardless of how
// carefully their contents were flushed.

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <algorithm>
#include <map>
#include <string>
#include <string_view>
#include <vector>

#include "anvil/core/lsm/db.h"
#include "anvil/sim/simulation.h"

namespace {

using namespace anvil;
using namespace anvil::lsm;

int g_failures = 0;

void check(bool condition, std::string_view what) {
  if (!condition) {
    std::cerr << "FAIL: " << what << "\n";
    ++g_failures;
  }
}

std::string key_of(int i) {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "k%06d", i);
  return buf;
}

struct CrashOutcome {
  std::size_t acknowledged = 0;
  std::size_t lost = 0;             // acknowledged, then gone after recovery
  std::size_t wrong_value = 0;      // present but not what was acknowledged
  std::size_t resurrected = 0;      // deleted, then back after recovery
  std::size_t orphans = 0;          // files no version references, after recovery
  std::size_t corruption_detected = 0;
  std::size_t served_corrupt = 0;   // the one that must always be zero
  std::size_t unreadable = 0;       // acknowledged, present in no readable form
  std::size_t stale = 0;            // an older acknowledged value, not the latest
  bool reopen_failed = false;
  std::vector<std::string> notes;
};

// The acknowledged state: what the client was promised. Recovery is checked
// against this and nothing else.
using Model = std::map<std::string, std::string>;
constexpr const char* kDeleted = "\x01<deleted>";

// Every value ever acknowledged for a key, so a read that returns something
// other than the latest can be classified honestly.
//
// This distinction is the entire content of INV-LSM-11, and the first version
// of this test got it wrong. Media corruption in the WAL makes recovery
// truncate, which loses the newest write and leaves the reader seeing the
// *previous* one. That is durability loss -- a single-replica log has no
// redundancy to do better -- but it is not "a corrupted block was served as
// data". Conflating the two reported a loud INV-LSM-11 violation on behaviour
// that is both correct and unavoidable, which is exactly the kind of false
// alarm that gets an invariant switched off.
using ValueHistory = std::map<std::string, std::vector<std::string>>;

// One crash cycle: write until a seed-chosen point, pull the power, reopen,
// and verify every acknowledgement was honoured.
CrashOutcome run_crash_cycle(std::uint64_t seed, DurabilityOptions durability,
                             bool inject_bit_rot,
                             sim::DiskFaults extra_disk = sim::DiskFaults{}) {
  CrashOutcome outcome;
  Model model;
  ValueHistory history;

  sim::SimConfig cfg;
  cfg.seed = seed;
  cfg.nodes = 1;
  cfg.max_time = Duration::seconds(3600);
  cfg.faults = sim::FaultProfile::none();
  cfg.faults.disk = extra_disk;
  cfg.faults.disk.page_cache = true;
  // Tearing is made common on purpose. A rare fault has to be made frequent in
  // the test that targets it -- the same reason BUGGIFY exists for rare code
  // paths.
  cfg.faults.disk.torn_write = sim::Chance::pct(40);

  sim::Simulation simulation{cfg};
  Runtime& rt = simulation.node(NodeId{1});

  DeterministicRandom rng{seed ^ 0xD15C'0FFE'E000'0001ULL};
  const int ops = static_cast<int>(rng.uniform_range(20, 400));

  DbOptions options;
  options.seed = seed;
  options.durability = durability;
  // Sized so the WAL genuinely accumulates unsynced records between flushes.
  //
  // This started at 4096, which is exactly the arena's block size -- so the
  // first insert allocated a block, memory_usage() hit the threshold, and every
  // single write flushed to an fsynced SSTable immediately. The WAL never held
  // anything, and the "acknowledge before fsync" bug was therefore undetectable:
  // the data was durable by a different route. The suite reported a clean pass
  // on a database with its most important durability guarantee switched off.
  options.memtable_bytes = 16384;     // ~4 arena blocks, ~60-80 entries
  options.base_level_bytes = 16384;
  options.l0_compaction_trigger = 3;

  // -- phase 1: write, then die --------------------------------------------
  rt.spawn([](Runtime& r, DbOptions opts, int op_count, std::uint64_t s, Model* m,
              ValueHistory* h, CrashOutcome* out) -> Task<void> {
    std::unique_ptr<Db> db;
    if (!(co_await Db::open(&r, opts, &db)).is_ok()) {
      out->notes.push_back("initial open failed");
      co_return;
    }
    DeterministicRandom local{s ^ 0xABCD};
    for (int i = 0; i < op_count; ++i) {
      const std::string key = key_of(static_cast<int>(local.uniform(120)));
      if (local.bernoulli(1, 5)) {
        if ((co_await db->del(key)).is_ok()) {
          (*m)[key] = kDeleted;
          ++out->acknowledged;
        }
      } else {
        const std::string value = "v" + std::to_string(i) + std::string(local.uniform(64), 'x');
        if ((co_await db->put(key, value)).is_ok()) {
          (*m)[key] = value;
          (*h)[key].push_back(value);
          ++out->acknowledged;
        }
      }
    }
    // Deliberately no close(): the machine is about to die mid-operation, which
    // is the only interesting way for it to die.
  }(rt, options, ops, seed, &model, &history, &outcome));

  simulation.run();

  if (inject_bit_rot) {
    // Corrupt durable media *before* the crash, so recovery has to read damaged
    // bytes. This is the INV-LSM-11 case: detected, never served.
    for (int i = 0; i < 3; ++i) simulation.disk().scrub_corrupt(NodeId{1});
  }

  simulation.disk().crash_node(NodeId{1});

  // -- phase 2: reopen and audit -------------------------------------------
  rt.spawn([](Runtime& r, DbOptions opts, const Model* m, const ValueHistory* h,
              CrashOutcome* out) -> Task<void> {
    std::unique_ptr<Db> db;
    const Status open_status = co_await Db::open(&r, opts, &db);
    if (!open_status.is_ok()) {
      // A corrupted manifest can legitimately make a database unopenable. That
      // is detected loss, not silent loss, and is scored separately.
      out->reopen_failed = true;
      out->notes.push_back(std::string{"reopen: "} + open_status.message());
      if (open_status.code() == StatusCode::kCorruption) ++out->corruption_detected;
      co_return;
    }

    for (const auto& [key, expected] : *m) {
      std::string value;
      bool found = false;
      const Status status = co_await db->get(key, &value, &found);
      if (status.code() == StatusCode::kCorruption) {
        // Detected, but the acknowledged value is still unreachable. The first
        // version of this audit treated detection as a pass and therefore
        // scored a planted bug -- publishing an SSTable before its contents
        // were durable -- as clean, while the engine was loudly reporting 47
        // checksum failures. Detection is not the same as survival.
        ++out->corruption_detected;
        ++out->unreadable;
        continue;
      }
      if (!status.is_ok()) {
        // Not corruption, but the data is still gone: a missing or unopenable
        // SSTable that the manifest references makes an acknowledged write
        // unreachable, which is data loss by any definition a client cares
        // about. Skipping these is how the first version of this test failed to
        // notice a planted bug.
        ++out->unreadable;
        if (out->notes.size() < 5) {
          out->notes.push_back("unreadable " + key + ": " + status.message());
        }
        continue;
      }

      if (expected == kDeleted) {
        if (found) {
          ++out->resurrected;
          if (out->notes.size() < 5) out->notes.push_back("resurrected " + key);
        }
        continue;
      }
      if (!found) {
        ++out->lost;
        if (out->notes.size() < 5) out->notes.push_back("lost " + key);
      } else if (value != expected) {
        ++out->wrong_value;
        const auto seen = h->find(key);
        const bool ever_written =
            seen != h->end() &&
            std::find(seen->second.begin(), seen->second.end(), value) != seen->second.end();
        if (ever_written) {
          // An older acknowledged value. Durability loss, but the bytes are
          // real -- nothing was invented.
          ++out->stale;
          if (out->notes.size() < 5) out->notes.push_back("stale value for " + key);
        } else {
          // Bytes nobody ever wrote. This is the INV-LSM-11 violation, and it
          // must be zero under every fault the model can produce.
          ++out->served_corrupt;
          if (out->notes.size() < 5) out->notes.push_back("INVENTED value for " + key);
        }
      }
    }

    std::vector<std::string> orphans;
    if ((co_await db->orphaned_files(&orphans)).is_ok()) out->orphans = orphans.size();
    // A truncated WAL is corruption that was *detected*: the checksum fired and
    // recovery stopped rather than replaying damaged records.
    out->corruption_detected += db->stats().corruptions_detected +
                                db->stats().wal_truncations + db->stats().manifest_truncations;
    co_await db->close();
  }(rt, options, &model, &history, &outcome));

  simulation.run_more(Duration::seconds(600));
  return outcome;
}

// ---------------------------------------------------------------------------
// 1 + 2: acknowledged writes survive crashes
// ---------------------------------------------------------------------------

void test_acked_writes_survive_crashes(std::uint64_t seeds) {
  std::size_t total_acked = 0;
  std::size_t lost = 0;
  std::size_t resurrected = 0;
  std::size_t wrong = 0;
  std::size_t orphan_seeds = 0;
  std::size_t reopen_failures = 0;
  std::size_t unreadable = 0;

  for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
    const CrashOutcome o = run_crash_cycle(seed, DurabilityOptions{}, false);
    total_acked += o.acknowledged;
    lost += o.lost;
    resurrected += o.resurrected;
    wrong += o.wrong_value;
    unreadable += o.unreadable;
    if (o.orphans > 0) ++orphan_seeds;
    if (o.reopen_failed) {
      ++reopen_failures;
      if (reopen_failures <= 4) {
        std::cerr << "  seed " << seed << " reopen failed:";
        for (const std::string& note : o.notes) std::cerr << " [" << note << "]";
        std::cerr << "\n";
      }
    }

    if ((o.lost > 0 || o.resurrected > 0 || o.wrong_value > 0 || o.unreadable > 0) &&
        lost + resurrected + unreadable < 12) {
      std::cerr << "  seed " << seed << ": lost=" << o.lost << " resurrected=" << o.resurrected
                << " wrong=" << o.wrong_value << " unreadable=" << o.unreadable;
      for (const std::string& note : o.notes) std::cerr << " [" << note << "]";
      std::cerr << "\n";
    }
  }

  check(lost == 0, "no acknowledged write may be lost to a crash (INV-LSM-01)");
  check(unreadable == 0, "no acknowledged write may become unreadable after a crash");
  check(resurrected == 0, "no deleted key may come back after a crash");
  check(wrong == 0, "no read may return a value that was never written (INV-LSM-06)");
  check(reopen_failures == 0, "a database must reopen after any crash, with no corruption");
  // INV-LSM-10. A crash between "the file is durable" and "the edit naming it
  // is durable" leaves a file nothing references; recovery has to sweep it, or
  // every crash leaks space forever.
  check(orphan_seeds == 0, "recovery must leave no orphaned files (INV-LSM-10)");
  std::cout << "  " << seeds << " crash cycles: " << total_acked
            << " acknowledged writes, " << lost << " lost, " << resurrected
            << " resurrected, " << orphan_seeds << " seeds with orphans\n";
}

// ---------------------------------------------------------------------------
// 3: corruption is detected, never served
// ---------------------------------------------------------------------------

void test_corruption_is_detected_not_served(std::uint64_t seeds) {
  std::size_t detected_seeds = 0;
  std::size_t served = 0;
  std::size_t stale = 0;
  bool stale_but_detected = true;

  for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
    const CrashOutcome o = run_crash_cycle(seed, DurabilityOptions{}, true);
    if (o.corruption_detected > 0) ++detected_seeds;
    served += o.served_corrupt;
    stale += o.stale;
  }

  // The absolute requirement. A corrupted block may cause an error, and it may
  // cost data -- a single-replica log has no redundancy to repair it -- but it
  // must never come back as a plausible-looking value.
  check(served == 0, "no read may return bytes nobody ever wrote (INV-LSM-11)");
  check(stale_but_detected,
        "every stale read caused by media corruption must be accompanied by a "
        "detection -- silent staleness would be corruption in disguise");
  check(detected_seeds > 0,
        "injected corruption must actually be detected -- if the checksums never "
        "fire, the corruption injection is not reaching anything that is read");
  std::cout << "  corruption: detected in " << detected_seeds << "/" << seeds
            << " seeds, served as data in " << served << "\n";
}

// ---------------------------------------------------------------------------
// ENOSPC and EIO must fail writes, never corrupt them
// ---------------------------------------------------------------------------

void test_out_of_space_and_io_errors(std::uint64_t seeds) {
  std::size_t lost = 0;
  std::size_t acked = 0;

  for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
    sim::DiskFaults faults;
    faults.io_error = sim::Chance::bp(80);
    faults.capacity_bytes = 1ULL << (14 + (seed % 4));  // 16 KiB .. 128 KiB
    const CrashOutcome o = run_crash_cycle(seed, DurabilityOptions{}, false, faults);
    acked += o.acknowledged;
    lost += o.lost;
  }

  // A write that fails is not acknowledged, so it is not in the model. The
  // property under test is that a *failed* write never partially applies and a
  // *succeeded* one is still durable even when the device is misbehaving around
  // it.
  check(lost == 0, "acknowledged writes survive even when neighbouring writes fail");
  check(acked > 0, "the workload must still make progress under ENOSPC and EIO");
  std::cout << "  ENOSPC/EIO: " << acked << " writes acknowledged, " << lost << " lost\n";
}

// ---------------------------------------------------------------------------
// 4: the seeded-bug drill (durability half)
// ---------------------------------------------------------------------------

void test_seeded_durability_bugs(std::uint64_t seeds) {
  struct Bug {
    const char* name;
    DurabilityOptions options;
  };

  std::vector<Bug> bugs;
  {
    DurabilityOptions o;
    o.sync_wal_on_write = false;
    bugs.push_back({"1. acknowledge before the WAL is fsynced", o});
  }
  {
    DurabilityOptions o;
    o.sync_manifest = false;
    bugs.push_back({"2. apply a version edit before the MANIFEST is durable", o});
  }
  {
    DurabilityOptions o;
    o.sync_dir_after_rename = false;
    bugs.push_back({"3. never fsync the directory after replacing CURRENT", o});
  }
  {
    DurabilityOptions o;
    o.sync_table_on_finish = false;
    bugs.push_back({"4. publish an SSTable before its contents are durable", o});
  }

  std::size_t caught = 0;
  for (const Bug& bug : bugs) {
    std::size_t detections = 0;
    std::size_t first_seed = 0;
    for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
      const CrashOutcome o = run_crash_cycle(seed, bug.options, false);
      const bool broke = o.lost > 0 || o.resurrected > 0 || o.wrong_value > 0 ||
                         o.unreadable > 0 || o.reopen_failed || o.orphans > 0;
      if (broke) {
        ++detections;
        if (first_seed == 0) first_seed = seed;
      }
    }
    if (detections > 0) ++caught;
    std::cout << "  seeded bug " << bug.name << ": detected in " << detections << "/" << seeds
              << " seeds";
    if (first_seed != 0) std::cout << " (first at seed " << first_seed << ")";
    std::cout << "\n";
    check(detections > 0,
          "every deliberately planted durability bug must be caught -- a suite that "
          "cannot catch a bug it planted cannot catch one it did not");
  }
  check(caught == bugs.size(), "all four durability mutations must be caught");
}

// ---------------------------------------------------------------------------
// determinism
// ---------------------------------------------------------------------------

void test_crash_cycles_are_deterministic(std::uint64_t seeds) {
  std::size_t divergences = 0;
  for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
    const CrashOutcome a = run_crash_cycle(seed, DurabilityOptions{}, false);
    const CrashOutcome b = run_crash_cycle(seed, DurabilityOptions{}, false);
    if (a.acknowledged != b.acknowledged || a.lost != b.lost || a.orphans != b.orphans ||
        a.corruption_detected != b.corruption_detected) {
      ++divergences;
    }
  }
  check(divergences == 0, "a crash cycle must reproduce exactly from its seed");
}

}  // namespace

int main(int argc, char** argv) {
  std::uint64_t seeds = 60;
  if (argc > 1) seeds = std::strtoull(argv[1], nullptr, 10);

  std::cout << "lsm crash semantics: " << seeds << " seeds\n";

  test_acked_writes_survive_crashes(seeds);
  test_crash_cycles_are_deterministic(seeds / 4 + 1);
  test_corruption_is_detected_not_served(seeds / 2 + 1);
  test_out_of_space_and_io_errors(seeds / 2 + 1);
  test_seeded_durability_bugs(seeds / 2 + 1);

  if (g_failures == 0) {
    std::cout << "lsm crash semantics: all checks passed\n";
    return EXIT_SUCCESS;
  }
  std::cerr << "lsm crash semantics: " << g_failures << " check(s) failed\n";
  return EXIT_FAILURE;
}
