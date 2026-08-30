// MVCC unit tests: versions, snapshots, intents, wound-wait, and the GC
// boundary that INV-MVCC-01 exists to defend.
//
// The store needs a real LSM underneath it, so these run against a one-node
// simulation with no faults. That is not fault injection -- it is a filesystem
// that behaves -- and it keeps the question here to "is the versioning right",
// with the adversary saved for test/mvcc_faults.cc.
//
// The most important test in the file is the GC boundary. Every other bug in
// this layer announces itself; deleting the version a live reader resolves to
// does not. It returns an older value, or nothing, with every status code ok
// and every checksum passing.

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#include "anvil/core/mvcc/key.h"
#include "anvil/core/mvcc/lock_table.h"
#include "anvil/core/mvcc/mvcc.h"
#include "anvil/core/mvcc/txn.h"
#include "anvil/sim/simulation.h"

namespace {

using anvil::Duration;
using anvil::NodeId;
using anvil::Status;
using anvil::TxnId;
namespace mvcc = anvil::mvcc;
namespace lsm = anvil::lsm;
namespace sim = anvil::sim;

int g_failures = 0;

void check(bool condition, std::string_view what) {
  if (!condition) {
    std::cerr << "FAIL: " << what << "\n";
    ++g_failures;
  }
}

// ---------------------------------------------------------------------------
// key encoding -- pure, no simulation needed
// ---------------------------------------------------------------------------

void test_key_encoding() {
  std::string key;
  mvcc::CommitTs ts = 0;
  const std::string encoded = mvcc::encode_data_key("hello", 4242);
  check(mvcc::decode_data_key(encoded, &key, &ts) && key == "hello" && ts == 4242,
        "a data key round-trips");

  // Newest first. This is the whole reason the timestamp is inverted: a forward
  // seek at `ts` lands on the newest version at or below it.
  check(mvcc::encode_data_key("k", 30) < mvcc::encode_data_key("k", 20),
        "a newer version sorts before an older one");
  check(mvcc::encode_data_key("k", 20) < mvcc::encode_data_key("k", 10),
        "and the order is total, not just adjacent");

  // The escape. Without it "a" plus version bytes is indistinguishable from
  // "a\x00..." plus different ones, and a scan for one key silently returns
  // versions of another.
  check(mvcc::data_prefix("a") < mvcc::data_prefix("a\x00b"),
        "an embedded NUL does not reorder keys");
  check(mvcc::data_prefix("a") < mvcc::data_prefix("ab"), "nor does a longer key");
  check(mvcc::encode_data_key("a", 1) < mvcc::data_upper_bound("a"),
        "every version of a key is below that key's upper bound");
  check(mvcc::data_upper_bound("a") < mvcc::encode_data_key("a\x00b", 999),
        "and the upper bound is below the next key's versions -- the escape working");
  check(mvcc::data_upper_bound("a") < mvcc::encode_data_key("ab", 999),
        "including for a key that merely extends it");

  std::string round;
  check(mvcc::decode_lock_key(mvcc::lock_key("a\x00\x00b"), &round) && round == "a\x00\x00b",
        "a lock key round-trips through repeated NULs");

  mvcc::Intent intent;
  intent.txn = TxnId{7};
  intent.start_ts = 99;
  intent.tombstone = true;
  intent.value = "v";
  mvcc::Intent decoded;
  check(mvcc::decode_intent(mvcc::encode_intent(intent), &decoded) &&
            decoded.txn == intent.txn && decoded.start_ts == 99 && decoded.tombstone,
        "an intent round-trips");
}

// ---------------------------------------------------------------------------
// the lock table -- also pure
// ---------------------------------------------------------------------------

void test_wound_wait() {
  mvcc::LockTable locks;
  mvcc::LockHolder blocker;

  const TxnId older{1};
  const TxnId younger{2};

  check(locks.acquire(younger, 200, "k", mvcc::LockKind::kExclusive, &blocker) ==
            mvcc::AcquireOutcome::kGranted,
        "an uncontended lock is granted");
  check(locks.acquire(younger, 200, "k", mvcc::LockKind::kExclusive, &blocker) ==
            mvcc::AcquireOutcome::kAlreadyHeld,
        "and re-acquiring it is a no-op");

  // The older transaction does not queue behind the younger one.
  check(locks.acquire(older, 100, "k", mvcc::LockKind::kExclusive, &blocker) ==
            mvcc::AcquireOutcome::kWoundHolder,
        "an older transaction wounds a younger holder rather than waiting");
  check(blocker.txn == younger, "and is told who to wound");

  locks.release_all(younger);
  check(locks.acquire(older, 100, "k", mvcc::LockKind::kExclusive, &blocker) ==
            mvcc::AcquireOutcome::kGranted,
        "once the victim releases, the older transaction proceeds");

  const TxnId youngest{3};
  check(locks.acquire(youngest, 300, "k", mvcc::LockKind::kExclusive, &blocker) ==
            mvcc::AcquireOutcome::kWaiting,
        "a younger transaction waits for an older holder");

  // Wound-wait makes a cycle impossible, so the detector must find none.
  std::vector<TxnId> cycle;
  check(!locks.find_cycle(&cycle), "wound-wait leaves no cycle in the wait-for graph");

  // Ties. Two transactions with the same start timestamp would each decide the
  // other is not older and both would wait -- a two-cycle, and a real deadlock.
  // The id tiebreak is what prevents it.
  mvcc::LockTable tied;
  check(tied.acquire(TxnId{5}, 500, "a", mvcc::LockKind::kExclusive, &blocker) ==
            mvcc::AcquireOutcome::kGranted,
        "t5 takes a");
  check(tied.acquire(TxnId{4}, 500, "a", mvcc::LockKind::kExclusive, &blocker) ==
            mvcc::AcquireOutcome::kWoundHolder,
        "with equal timestamps the lower id is treated as older and wounds");
  check(!mvcc::is_older(500, TxnId{5}, 500, TxnId{4}), "the age order is strict");
  check(mvcc::is_older(500, TxnId{4}, 500, TxnId{5}), "and antisymmetric");
}

// The deadlock detector's non-vacuity case. Wound-wait should mean this never
// happens, so the only way to know the detector works is to build a cycle by
// hand -- otherwise INV-MVCC-06 is a predicate over a set that is always empty,
// which is exactly what INV-SIM-05 exists to catch (see ANV-0005).
void test_deadlock_detector_is_not_vacuous() {
  mvcc::LockTable locks;
  mvcc::LockHolder blocker;

  // Build t1 -> t2 -> t3 -> t1 by acquiring in an order wound-wait would never
  // produce: each transaction is younger than the one it waits for, except the
  // last, which closes the loop.
  locks.acquire(TxnId{1}, 100, "a", mvcc::LockKind::kExclusive, &blocker);
  locks.acquire(TxnId{2}, 200, "b", mvcc::LockKind::kExclusive, &blocker);
  locks.acquire(TxnId{3}, 300, "c", mvcc::LockKind::kExclusive, &blocker);

  check(locks.acquire(TxnId{2}, 200, "a", mvcc::LockKind::kExclusive, &blocker) ==
            mvcc::AcquireOutcome::kWaiting,
        "t2 waits for t1");
  check(locks.acquire(TxnId{3}, 300, "b", mvcc::LockKind::kExclusive, &blocker) ==
            mvcc::AcquireOutcome::kWaiting,
        "t3 waits for t2");

  std::vector<TxnId> cycle;
  check(!locks.find_cycle(&cycle), "a chain is not a cycle");

  // Close the loop by making t1 wait for t3.
  //
  // A transaction waits on at most one lock at a time, so a successful acquire
  // clears its wait edge -- which means the three edges have to be established
  // in an order where nothing later cancels an earlier one. t1's wait goes last,
  // and it only forms at all because t1 is presented here with a start
  // timestamp *younger* than t3's. Under wound-wait that never happens; the
  // point of the test is that if it ever did, the detector would say so.
  check(locks.acquire(TxnId{1}, 400, "c", mvcc::LockKind::kExclusive, &blocker) ==
            mvcc::AcquireOutcome::kWaiting,
        "t1, presented as the youngest, waits for t3 and closes the loop");

  check(locks.find_cycle(&cycle), "the detector finds a cycle that does exist");
  check(cycle.size() == 3, "and reports exactly the three transactions in it (got " +
                               std::to_string(cycle.size()) + ")");
}

// ---------------------------------------------------------------------------
// the store, over a real LSM
// ---------------------------------------------------------------------------

struct StoreFixture {
  sim::SimConfig config;
  std::unique_ptr<sim::Simulation> simulation;
  std::unique_ptr<lsm::Db> db;
  std::unique_ptr<mvcc::MvccStore> store;
  bool opened = false;
};

// Runs `body` inside a one-node simulation with an open database. Everything in
// this layer is a coroutine, so the tests have to be too.
template <typename Body>
void with_store(mvcc::MvccOptions options, Body body) {
  sim::SimConfig config;
  config.nodes = 1;
  config.faults = sim::FaultProfile::none();
  config.max_time = Duration::seconds(120);
  sim::Simulation simulation{config};

  anvil::Runtime& runtime = simulation.node(NodeId{1});
  simulation.node(NodeId{1});

  auto shared = std::make_shared<StoreFixture>();
  runtime.spawn([](anvil::Runtime& rt, mvcc::MvccOptions opts,
                   std::shared_ptr<StoreFixture> fixture, Body run) -> anvil::Task<void> {
    lsm::DbOptions db_options;
    std::unique_ptr<lsm::Db> db;
    const Status status = co_await lsm::Db::open(&rt, db_options, &db);
    if (!status.is_ok()) co_return;
    fixture->db = std::move(db);
    fixture->store = std::make_unique<mvcc::MvccStore>(&rt, fixture->db.get(), opts);
    fixture->opened = true;
    co_await run(rt, *fixture->store, *fixture->db);
  }(runtime, options, shared, std::move(body)));

  simulation.run();
  check(shared->opened, "the database opened");
}

anvil::Task<void> commit_version(mvcc::MvccStore& store, std::string_view key,
                                 mvcc::CommitTs ts, std::string_view value, TxnId txn,
                                 bool tombstone = false) {
  mvcc::Intent intent;
  intent.txn = txn;
  intent.start_ts = ts - 1;
  intent.tombstone = tombstone;
  intent.value.assign(value);
  mvcc::Intent conflict;
  co_await store.put_intent(key, intent, &conflict);
  co_await store.commit_intent(key, txn, ts);
}

void test_snapshot_reads() {
  with_store(mvcc::MvccOptions{}, [](anvil::Runtime& rt, mvcc::MvccStore& store,
                                     lsm::Db& db) -> anvil::Task<void> {
    (void)rt;
    (void)db;
    co_await commit_version(store, "k", 10, "v10", TxnId{1});
    co_await commit_version(store, "k", 20, "v20", TxnId{2});
    co_await commit_version(store, "k", 30, "v30", TxnId{3});

    struct Case {
      mvcc::CommitTs read_ts;
      bool found;
      const char* value;
    };
    // A read resolves to the newest version at or *below* its snapshot. The
    // boundary cases are the ones worth writing down: exactly on a commit
    // timestamp must see that version, one below must not.
    const Case cases[] = {
        {5, false, ""},    {9, false, ""},   {10, true, "v10"}, {19, true, "v10"},
        {20, true, "v20"}, {29, true, "v20"}, {30, true, "v30"}, {99, true, "v30"},
    };
    for (const Case& c : cases) {
      mvcc::ReadResult result;
      co_await store.get("k", c.read_ts, TxnId{9}, &result);
      check(result.found == c.found,
            "read at " + std::to_string(c.read_ts) + " finds a version");
      if (c.found) {
        check(result.value == c.value, "read at " + std::to_string(c.read_ts) +
                                           " returns " + c.value + ", got " + result.value);
      }
    }

    // A tombstone is a version, not an absence: a read below it still sees the
    // older value.
    co_await commit_version(store, "k", 40, "", TxnId{4}, /*tombstone=*/true);
    mvcc::ReadResult after;
    co_await store.get("k", 45, TxnId{9}, &after);
    check(!after.found, "a read above a tombstone finds nothing");
    mvcc::ReadResult before;
    co_await store.get("k", 35, TxnId{9}, &before);
    check(before.found && before.value == "v30",
          "and a read below it still sees the version the tombstone shadows");
  });
}

void test_intents_block_and_resolve() {
  with_store(mvcc::MvccOptions{}, [](anvil::Runtime& rt, mvcc::MvccStore& store,
                                     lsm::Db& db) -> anvil::Task<void> {
    (void)rt;
    (void)db;
    co_await commit_version(store, "k", 10, "committed", TxnId{1});

    mvcc::Intent intent;
    intent.txn = TxnId{5};
    intent.start_ts = 20;
    intent.value = "uncommitted";
    mvcc::Intent conflict;
    check((co_await store.put_intent("k", intent, &conflict)).is_ok(), "an intent is left");

    // Its owner reads through it.
    mvcc::ReadResult own;
    co_await store.get("k", 25, TxnId{5}, &own);
    check(own.found && own.value == "uncommitted",
          "a transaction sees its own uncommitted write");

    // Anybody else is blocked rather than being given a guess.
    mvcc::ReadResult other;
    co_await store.get("k", 25, TxnId{6}, &other);
    check(other.blocked && other.blocker.txn == TxnId{5},
          "another reader is blocked and told who by");

    // A second writer is refused.
    mvcc::Intent rival;
    rival.txn = TxnId{7};
    rival.start_ts = 21;
    check(!(co_await store.put_intent("k", rival, &conflict)).is_ok() &&
              conflict.txn == TxnId{5},
          "a second intent on the same key is refused");

    co_await store.commit_intent("k", TxnId{5}, 30);
    mvcc::ReadResult resolved;
    co_await store.get("k", 35, TxnId{6}, &resolved);
    check(!resolved.blocked && resolved.found && resolved.value == "uncommitted",
          "once committed, the value is visible to everyone above its timestamp");
    mvcc::ReadResult below;
    co_await store.get("k", 25, TxnId{6}, &below);
    check(below.found && below.value == "committed",
          "and a snapshot below the commit still sees the older version");
  });
}

void test_uncertainty_restart() {
  with_store(mvcc::MvccOptions{}, [](anvil::Runtime& rt, mvcc::MvccStore& store,
                                     lsm::Db& db) -> anvil::Task<void> {
    (void)rt;
    (void)db;
    co_await commit_version(store, "k", 100, "v100", TxnId{1});

    // A read at 90 whose clock could be wrong by 20 cannot rule out that the
    // version at 100 actually happened first. It must restart, not guess.
    mvcc::ReadResult result;
    const Status status = co_await store.get_uncertain("k", 90, 110, TxnId{2}, &result);
    check(status.code() == anvil::StatusCode::kAborted,
          "a version inside the uncertainty window forces a restart");
    check(result.commit_ts == 100, "and names the version that forced it");

    // Narrow the window below the version and the read proceeds.
    mvcc::ReadResult narrow;
    const Status ok = co_await store.get_uncertain("k", 90, 99, TxnId{2}, &narrow);
    check(ok.is_ok() && !narrow.found,
          "with the window below the version, the read completes and sees nothing");
  });
}

// The test this whole layer exists for.
void test_gc_keeps_the_version_a_reader_resolves_to() {
  with_store(mvcc::MvccOptions{}, [](anvil::Runtime& rt, mvcc::MvccStore& store,
                                     lsm::Db& db) -> anvil::Task<void> {
    (void)rt;
    (void)db;
    for (mvcc::CommitTs ts : {mvcc::CommitTs{10}, mvcc::CommitTs{20}, mvcc::CommitTs{30}}) {
      co_await commit_version(store, "k", ts, "v" + std::to_string(ts), TxnId{ts});
    }

    std::uint64_t collected = 0;
    check((co_await store.collect_garbage(25, 100, &collected)).is_ok(), "gc runs");

    // 10 is unreachable: a reader at 25 resolves to 20, and a reader below 20
    // cannot exist because 25 is the safepoint. 20 must survive; 30 is above the
    // safepoint and was never a candidate.
    check(collected == 1, "exactly the unreachable version is collected (got " +
                              std::to_string(collected) + ")");

    std::vector<std::pair<mvcc::CommitTs, std::string>> versions;
    co_await store.versions_of("k", &versions);
    check(versions.size() == 2, "two versions remain");

    mvcc::ReadResult at_safepoint;
    co_await store.get("k", 25, TxnId{9}, &at_safepoint);
    check(at_safepoint.found && at_safepoint.value == "v20",
          "a reader sitting exactly on the safepoint still resolves to its version");

    mvcc::ReadResult above;
    co_await store.get("k", 35, TxnId{9}, &above);
    check(above.found && above.value == "v30", "and a newer reader is unaffected");
  });
}

// The same case with the guard removed. This is what makes the mutation
// non-equivalent: without the boundary rule the reader at the safepoint gets a
// wrong answer, silently, with every status ok.
void test_gc_without_the_boundary_rule_corrupts_a_reader() {
  mvcc::MvccOptions broken;
  broken.gc_keeps_safepoint_version = false;
  with_store(broken, [](anvil::Runtime& rt, mvcc::MvccStore& store,
                        lsm::Db& db) -> anvil::Task<void> {
    (void)rt;
    (void)db;
    for (mvcc::CommitTs ts : {mvcc::CommitTs{10}, mvcc::CommitTs{20}, mvcc::CommitTs{30}}) {
      co_await commit_version(store, "k", ts, "v" + std::to_string(ts), TxnId{ts});
    }
    std::uint64_t collected = 0;
    co_await store.collect_garbage(25, 100, &collected);
    check(collected == 2, "the mutant collects the boundary version too");

    mvcc::ReadResult at_safepoint;
    const Status status = co_await store.get("k", 25, TxnId{9}, &at_safepoint);
    check(status.is_ok(), "and the read still succeeds -- there is no error to notice");
    check(!at_safepoint.found,
          "but the reader at the safepoint now sees nothing, which is the corruption");
  });
}

}  // namespace

int main() {
  std::cout << "mvcc unit tests\n";

  test_key_encoding();
  test_wound_wait();
  test_deadlock_detector_is_not_vacuous();

  test_snapshot_reads();
  test_intents_block_and_resolve();
  test_uncertainty_restart();
  test_gc_keeps_the_version_a_reader_resolves_to();
  test_gc_without_the_boundary_rule_corrupts_a_reader();

  if (g_failures == 0) {
    std::cout << "mvcc unit tests: all checks passed\n";
    return EXIT_SUCCESS;
  }
  std::cerr << "mvcc unit tests: " << g_failures << " check(s) failed\n";
  return EXIT_FAILURE;
}
