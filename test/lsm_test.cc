// Unit tests for the LSM building blocks.
//
// These run before the engine exists as a whole, on purpose. A bug in internal
// key ordering or in the block encoder surfaces as "a read returned the wrong
// version under a specific compaction interleaving" if you only ever test the
// assembled engine, and that is a far more expensive way to find it.
//
// The interesting cases here are the negative ones: a Bloom filter must never
// produce a false negative, a corrupted block must never be served, and a
// damaged WAL must truncate rather than salvage.

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#include "anvil/core/lsm/db.h"
#include "anvil/core/lsm/format.h"
#include "anvil/core/lsm/memtable.h"
#include "anvil/core/lsm/sstable.h"
#include "anvil/core/lsm/wal.h"
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

// Drives a coroutine to completion inside a fault-free simulation.
void run(const std::function<Task<void>(Runtime&)>& body) {
  sim::SimConfig cfg;
  cfg.nodes = 1;
  cfg.faults = sim::FaultProfile::none();
  cfg.max_time = Duration::seconds(600);
  sim::Simulation simulation{cfg};
  Runtime& rt = simulation.node(NodeId{1});
  rt.spawn(body(rt));
  simulation.run();
}

// ---------------------------------------------------------------------------
// format
// ---------------------------------------------------------------------------

void test_varint_roundtrip() {
  const std::uint64_t values[] = {0, 1, 127, 128, 16383, 16384, 1u << 20, 1ULL << 40,
                                  0xFFFF'FFFF'FFFF'FFFFULL};
  for (const std::uint64_t v : values) {
    std::string encoded;
    put_varint64(&encoded, v);
    std::uint64_t decoded = 0;
    const char* end = get_varint64(encoded.data(), encoded.data() + encoded.size(), &decoded);
    check(end != nullptr && decoded == v, "varint64 round-trips");
  }
  // A truncated varint must be rejected, not read past the end.
  std::string truncated;
  put_varint64(&truncated, 1ULL << 40);
  truncated.resize(2);
  std::uint64_t out = 0;
  check(get_varint64(truncated.data(), truncated.data() + truncated.size(), &out) == nullptr,
        "a truncated varint is rejected rather than over-read");
}

void test_fixed_encoding_is_explicitly_little_endian() {
  std::string encoded;
  put_fixed32(&encoded, 0x01020304u);
  check(static_cast<unsigned char>(encoded[0]) == 0x04 &&
            static_cast<unsigned char>(encoded[3]) == 0x01,
        "fixed32 is little-endian byte by byte, not a native memcpy");
  check(decode_fixed32(encoded.data()) == 0x01020304u, "fixed32 round-trips");

  std::string wide;
  put_fixed64(&wide, 0x0102030405060708ULL);
  check(decode_fixed64(wide.data()) == 0x0102030405060708ULL, "fixed64 round-trips");
}

void test_internal_key_ordering() {
  const std::string a = make_internal_key("apple", 5, ValueType::kValue);
  const std::string b = make_internal_key("banana", 1, ValueType::kValue);
  check(compare_internal(a, b) < 0, "user keys order ascending");

  // The critical one: within a user key, higher sequence sorts FIRST, so a seek
  // lands on the newest version without scanning.
  const std::string older = make_internal_key("apple", 3, ValueType::kValue);
  const std::string newer = make_internal_key("apple", 9, ValueType::kValue);
  check(compare_internal(newer, older) < 0, "newer versions of a key sort before older ones");

  check(user_key_of(a) == "apple", "the user key is recoverable");
  check(sequence_of(trailer_of(a)) == 5, "the sequence number is recoverable");
  check(type_of(trailer_of(a)) == ValueType::kValue, "the value type is recoverable");

  const std::string tomb = make_internal_key("apple", 7, ValueType::kDeletion);
  check(type_of(trailer_of(tomb)) == ValueType::kDeletion, "tombstones are distinguishable");
}

void test_crc32c() {
  // A stable, non-trivial value: the point is that the implementation does not
  // silently change between builds, since a file written by one and read by
  // another would fail every checksum.
  const std::uint32_t empty = crc32c("");
  const std::uint32_t a = crc32c("a");
  const std::uint32_t ab = crc32c("ab");
  check(a != ab && a != empty, "crc32c distinguishes inputs");
  check(crc32c("hello world") == crc32c("hello world"), "crc32c is deterministic");
  check(crc32c_extend(crc32c("hello "), "world") == crc32c("hello world"),
        "crc32c_extend composes with crc32c");
}

// ---------------------------------------------------------------------------
// memtable
// ---------------------------------------------------------------------------

void test_memtable_basic() {
  MemTable table{42};
  table.add(1, ValueType::kValue, "a", "one");
  table.add(2, ValueType::kValue, "b", "two");
  table.add(3, ValueType::kValue, "a", "one-updated");

  std::string value;
  bool deleted = false;

  check(table.get("a", 3, &value, &deleted) && value == "one-updated",
        "a read at the latest snapshot sees the newest version");
  check(table.get("a", 2, &value, &deleted) && value == "one",
        "a read at an older snapshot sees the older version");
  check(!table.get("a", 0, &value, &deleted), "a read below every version sees nothing");
  check(!table.get("zzz", 99, &value, &deleted), "an absent key is absent");
  check(table.entries() == 3, "entries are counted");
}

void test_memtable_tombstones() {
  MemTable table{7};
  table.add(1, ValueType::kValue, "k", "v");
  table.add(2, ValueType::kDeletion, "k", "");

  std::string value;
  bool deleted = false;
  // A tombstone must report found=true with is_deletion set. Reporting "not
  // found" would let a value in a lower level show through, resurrecting
  // deleted data -- one of the classic LSM bugs.
  check(table.get("k", 2, &value, &deleted) && deleted,
        "a tombstone is found, and reports itself as a deletion");
  check(table.get("k", 1, &value, &deleted) && !deleted && value == "v",
        "a snapshot below the tombstone still sees the value");
}

void test_memtable_iteration_is_sorted() {
  MemTable table{99};
  const char* keys[] = {"delta", "alpha", "charlie", "bravo", "echo"};
  SequenceNumber seq = 1;
  for (const char* key : keys) table.add(seq++, ValueType::kValue, key, "v");

  auto it = table.iterator();
  std::vector<std::string> seen;
  for (it.seek_to_first(); it.valid(); it.next()) {
    seen.emplace_back(user_key_of(it.internal_key()));
  }
  check(seen == std::vector<std::string>{"alpha", "bravo", "charlie", "delta", "echo"},
        "iteration is in sorted user-key order");
}

void test_memtable_is_deterministic() {
  // The skiplist's heights come from the seed, so two memtables built the same
  // way must be structurally identical. A skiplist seeded from the clock would
  // break replay for the whole engine above it.
  const auto build = [](std::uint64_t seed) {
    MemTable table{seed};
    for (int i = 0; i < 200; ++i) {
      table.add(static_cast<SequenceNumber>(i + 1), ValueType::kValue,
                "key" + std::to_string(i % 37), "value" + std::to_string(i));
    }
    return table.memory_usage();
  };
  check(build(1234) == build(1234), "the same seed produces the same skiplist shape");
}

// ---------------------------------------------------------------------------
// write-ahead log
// ---------------------------------------------------------------------------

void test_write_batch_roundtrip() {
  WriteBatch batch;
  batch.put("a", "1");
  batch.del("b");
  batch.put("c", "3");
  check(batch.count() == 3, "batch counts its operations");

  const std::string encoded = batch.encode(100);
  std::vector<WriteBatch::Entry> entries;
  SequenceNumber first = 0;
  check(WriteBatch::decode(encoded, &entries, &first), "a batch round-trips");
  check(first == 100 && entries.size() == 3, "the sequence base and count survive");
  check(entries[0].key == "a" && entries[0].value == "1" &&
            entries[0].type == ValueType::kValue,
        "puts survive");
  check(entries[1].key == "b" && entries[1].type == ValueType::kDeletion, "deletes survive");
  // Sequence numbers run consecutively within a batch, so a batch occupies a
  // contiguous range and inter-batch ordering is unambiguous.
  check(entries[2].sequence == 102, "sequence numbers advance within the batch");

  std::vector<WriteBatch::Entry> junk;
  check(!WriteBatch::decode("short", &junk, &first),
        "a malformed batch is rejected rather than read past the end");
}

void test_wal_roundtrip_and_truncation() {
  run([](Runtime& rt) -> Task<void> {
    FileHandle file{};
    const OpenFlags flags = OpenFlags::kRead | OpenFlags::kWrite | OpenFlags::kCreate;
    if (!(co_await rt.open("wal.log", flags, &file)).is_ok()) {
      check(false, "wal file opens");
      co_return;
    }

    WalWriter writer{&rt, file};
    for (int i = 0; i < 20; ++i) {
      const Status status = co_await writer.append("record-" + std::to_string(i));
      if (!status.is_ok()) {
        check(false, "wal append succeeds");
        co_return;
      }
    }
    co_await writer.sync();

    WalReadResult result;
    check((co_await wal_read_all(&rt, file, &result)).is_ok(), "wal reads back");
    check(result.records.size() == 20, "every appended record is read back");
    check(!result.truncated, "a cleanly written log is not reported as truncated");
    check(result.records[7] == "record-7", "records survive intact and in order");

    // Corrupt one byte in the middle. Everything from that record on must be
    // discarded, not salvaged: the device may have reordered later writes, so
    // recovering past the damage would "recover" data that was never durable.
    const std::uint64_t corrupt_at = 8 + 5;  // inside the payload of record 0
    const std::byte flipped{0xFF};
    check((co_await rt.pwrite(file, ByteView{&flipped, 1}, corrupt_at)).is_ok(),
          "corruption can be injected");

    WalReadResult damaged;
    check((co_await wal_read_all(&rt, file, &damaged)).is_ok(),
          "reading a damaged log is not itself an error");
    check(damaged.truncated, "the damage is reported as truncation");
    check(damaged.records.empty(), "nothing after the first bad record is salvaged");
    check(damaged.records_discarded > 0, "the discard is counted");
  });
}

// ---------------------------------------------------------------------------
// sstable
// ---------------------------------------------------------------------------

void test_sstable_roundtrip() {
  run([](Runtime& rt) -> Task<void> {
    FileHandle file{};
    const OpenFlags flags = OpenFlags::kRead | OpenFlags::kWrite | OpenFlags::kCreate;
    if (!(co_await rt.open("table.sst", flags, &file)).is_ok()) {
      check(false, "table file opens");
      co_return;
    }

    // Enough keys to span many blocks and exercise restart points, prefix
    // compression, and the index.
    constexpr int kCount = 2000;
    std::vector<std::string> keys;
    keys.reserve(kCount);
    for (int i = 0; i < kCount; ++i) {
      char buf[32];
      std::snprintf(buf, sizeof(buf), "key%06d", i);
      keys.emplace_back(buf);
    }

    TableBuilder builder{&rt, file};
    for (int i = 0; i < kCount; ++i) {
      const std::string internal =
          make_internal_key(keys[static_cast<std::size_t>(i)],
                            static_cast<SequenceNumber>(i + 1), ValueType::kValue);
      if (!(co_await builder.add(internal, "value-" + std::to_string(i))).is_ok()) {
        check(false, "table add succeeds");
        co_return;
      }
    }
    check((co_await builder.finish()).is_ok(), "table finishes");
    check(builder.entries() == kCount, "every entry is accounted for");
    co_await rt.fsync(file);

    BlockCache cache{1 << 20};
    std::unique_ptr<Table> table;
    check((co_await Table::open(&rt, file, 1, &cache, &table)).is_ok(), "table opens");
    if (table == nullptr) co_return;

    // Every key must be findable. A Bloom filter false negative would show up
    // here as a key that simply stopped existing (INV-LSM-07).
    int missing = 0;
    for (int i = 0; i < kCount; ++i) {
      const std::string probe = make_internal_key(keys[static_cast<std::size_t>(i)],
                                                  kMaxSequenceNumber, ValueType::kValue);
      bool found = false;
      bool deleted = false;
      std::string value;
      const Status status = co_await table->get(probe, &found, &value, &deleted);
      if (!status.is_ok() || !found || value != "value-" + std::to_string(i)) ++missing;
    }
    check(missing == 0, "every written key is readable back (no false negatives)");

    // Absent keys should mostly be rejected by the filter. This is a
    // performance property, not a correctness one, so the bound is loose --
    // but a filter rejecting nothing means it is not working at all.
    int absent_found = 0;
    for (int i = 0; i < 500; ++i) {
      const std::string probe = make_internal_key("absent" + std::to_string(i),
                                                  kMaxSequenceNumber, ValueType::kValue);
      bool found = false;
      bool deleted = false;
      std::string value;
      co_await table->get(probe, &found, &value, &deleted);
      if (found) ++absent_found;
    }
    check(absent_found == 0, "absent keys are not found");
    check(table->filter_rejects() > 0, "the Bloom filter actually rejects probes");

    // Full scan must return everything, in order.
    std::vector<std::string> scanned;
    check((co_await table->for_each([&](std::string_view key, std::string_view) {
            scanned.emplace_back(user_key_of(key));
          })).is_ok(),
          "a full scan succeeds");
    check(scanned.size() == kCount, "a full scan sees every entry");
    check(scanned.front() == "key000000" && scanned.back() == "key001999",
          "a full scan is in sorted order");
  });
}

void test_sstable_corruption_is_detected_not_served() {
  run([](Runtime& rt) -> Task<void> {
    FileHandle file{};
    const OpenFlags flags = OpenFlags::kRead | OpenFlags::kWrite | OpenFlags::kCreate;
    co_await rt.open("corrupt.sst", flags, &file);

    TableBuilder builder{&rt, file};
    for (int i = 0; i < 500; ++i) {
      char buf[32];
      std::snprintf(buf, sizeof(buf), "key%06d", i);
      co_await builder.add(make_internal_key(buf, static_cast<SequenceNumber>(i + 1),
                                             ValueType::kValue),
                           "value");
    }
    co_await builder.finish();
    co_await rt.fsync(file);

    // Flip a byte inside the first data block.
    const std::byte flipped{0x7E};
    co_await rt.pwrite(file, ByteView{&flipped, 1}, 40);

    BlockCache cache{1 << 20};
    std::unique_ptr<Table> table;
    const Status open_status = co_await Table::open(&rt, file, 2, &cache, &table);
    if (!open_status.is_ok()) {
      // Also acceptable: the damage landed somewhere open() had to read.
      check(open_status.code() == StatusCode::kCorruption,
            "a corrupted table reports corruption, not some other error");
      co_return;
    }

    // INV-LSM-11. Whatever happens, the caller must never receive the corrupted
    // bytes as data. Either the read errors, or the checksum caught it.
    int corruption_reported = 0;
    int served = 0;
    for (int i = 0; i < 500; ++i) {
      char buf[32];
      std::snprintf(buf, sizeof(buf), "key%06d", i);
      bool found = false;
      bool deleted = false;
      std::string value;
      const Status status =
          co_await table->get(make_internal_key(buf, kMaxSequenceNumber, ValueType::kValue),
                              &found, &value, &deleted);
      if (status.code() == StatusCode::kCorruption) {
        ++corruption_reported;
      } else if (found && value != "value") {
        ++served;
      }
    }
    check(served == 0, "a corrupted block is NEVER returned as data (INV-LSM-11)");
    check(corruption_reported > 0, "the corruption is actually detected");
  });
}

// A tombstone may only be dropped where nothing below it could still hold an
// older value. Drop one too early and the deleted key comes back.
//
// This test exists because the seeded-mutation drill found that removing the
// `bottom_most` guard in compaction was NOT caught by the crash suite. The
// crash workload never builds more than two levels, so `bottom_most` is
// almost always true and the guard is dead weight in every run -- the mutation
// changed nothing observable, not because the property held but because the
// workload could not reach the situation where it matters.
//
// The fix is a case that reaches it on purpose: enough data for three levels,
// a delete that compacts into a middle level, and an older value still sitting
// underneath it.
void test_tombstones_survive_until_the_bottom_level() {
  run([](Runtime& rt) -> Task<void> {
    DbOptions options;
    options.seed = 20250829;
    options.memtable_bytes = 8192;
    options.base_level_bytes = 4096;   // small, so levels cascade quickly
    options.l0_compaction_trigger = 2;
    options.block_bytes = 512;

    std::unique_ptr<Db> db;
    if (!(co_await Db::open(&rt, options, &db)).is_ok()) {
      check(false, "db opens");
      co_return;
    }

    constexpr int kKeys = 900;
    for (int i = 0; i < kKeys; ++i) {
      char buf[32];
      std::snprintf(buf, sizeof(buf), "key%06d", i);
      if (!(co_await db->put(buf, std::string(80, 'v'))).is_ok()) {
        check(false, "puts succeed");
        co_return;
      }
    }
    co_await db->flush();
    for (int i = 0; i < 40; ++i) co_await db->maybe_compact();

    int populated_levels = 0;
    for (int level = 0; level < kNumLevels; ++level) {
      if (!db->versions().current().levels[level].empty()) ++populated_levels;
    }
    // Without this the rest of the test proves nothing: if everything sits in
    // one level, `bottom_most` is trivially true and the guard is never tested.
    check(populated_levels >= 2,
          "the workload must actually build multiple levels, or the tombstone "
          "guard is never exercised");

    // Delete every third key, then compact the tombstones downward -- but with
    // older values still living below them.
    for (int i = 0; i < kKeys; i += 3) {
      char buf[32];
      std::snprintf(buf, sizeof(buf), "key%06d", i);
      if (!(co_await db->del(buf)).is_ok()) {
        check(false, "deletes succeed");
        co_return;
      }
    }
    co_await db->flush();
    for (int i = 0; i < 40; ++i) co_await db->maybe_compact();

    int resurrected = 0;
    int missing = 0;
    for (int i = 0; i < kKeys; ++i) {
      char buf[32];
      std::snprintf(buf, sizeof(buf), "key%06d", i);
      std::string value;
      bool found = false;
      if (!(co_await db->get(buf, &value, &found)).is_ok()) {
        check(false, "gets succeed");
        co_return;
      }
      if (i % 3 == 0) {
        if (found) ++resurrected;
      } else if (!found) {
        ++missing;
      }
    }

    check(resurrected == 0, "a deleted key must never reappear after compaction");
    check(missing == 0, "compaction must not lose live keys");
    co_await db->close();
  });
}

// ANV-0025. A write that returns ok must be readable by the very next get, even
// while other writers are in flight.
//
// The failure this pins is not subtle once you see it and is invisible until
// then. `write` used to take its sequence numbers from the published watermark
// and *then* suspend for the log append and the fsync, so every writer that
// started during that window was handed the same numbers; when they finished in
// the other order, the second one published a watermark below the first one's
// entries and those entries stopped being visible. They came back later, when an
// unrelated write pushed the watermark past them again -- a key that is absent
// for a while and then returns, which no layer above can make sense of. It
// surfaced four layers up, as a transaction intent that its own owner could not
// find and therefore never cleaned up.
//
// The test runs writers concurrently, each reading back its own key immediately
// after the write returns.
void test_concurrent_writers_never_lose_read_after_write() {
  run([](Runtime& rt) -> Task<void> {
    DbOptions options;
    options.seed = 7;
    options.memtable_bytes = 4096;  // small, so flushes interleave with the writes
    std::unique_ptr<Db> db;
    if (!(co_await Db::open(&rt, options, &db)).is_ok()) {
      check(false, "db opens");
      co_return;
    }

    int invisible = 0;
    int done = 0;
    constexpr int kWriters = 6;
    constexpr int kOps = 25;

    auto writer = [&](int id) -> Task<void> {
      for (int op = 0; op < kOps; ++op) {
        char key[32];
        std::snprintf(key, sizeof(key), "w%02d-k%03d", id, op);
        const std::string value(48, static_cast<char>('a' + id));
        if (!(co_await db->put(key, value)).is_ok()) continue;

        // The write said ok. There is exactly one correct answer here.
        std::string got;
        bool found = false;
        if (!(co_await db->get(key, &got, &found)).is_ok()) continue;
        if (!found || got != value) ++invisible;
      }
      ++done;
    };

    for (int id = 0; id < kWriters; ++id) rt.spawn(writer(id));
    for (int i = 0; i < 2000 && done < kWriters; ++i) co_await rt.sleep_for(Duration::millis(1));

    check(done == kWriters, "every writer finishes");
    check(invisible == 0, "a write that returned ok is visible to the next read");

    // And the mechanism itself: no two batches may be handed the same sequence.
    check(db->last_sequence() >= static_cast<SequenceNumber>(kWriters * kOps),
          "each write consumed its own sequence number");
  });
}

void test_block_cache() {
  BlockCache cache{100};
  cache.insert(1, 0, std::string(40, 'a'));
  cache.insert(1, 40, std::string(40, 'b'));

  std::string out;
  check(cache.lookup(1, 0, &out) && out.size() == 40, "a cached block is returned");
  check(cache.hits() == 1 && cache.misses() == 0, "hits and misses are counted");
  check(!cache.lookup(9, 9, &out), "an absent block misses");

  cache.insert(1, 80, std::string(40, 'c'));  // exceeds capacity; evicts the LRU
  check(cache.bytes() <= 100, "the cache stays within capacity");

  // INV-LSM-13: when a file is deleted by compaction, its blocks must go with
  // it. A cache that outlives the file is one bug away from serving blocks that
  // belong to nothing.
  cache.insert(2, 0, std::string(10, 'd'));
  cache.erase_file(2);
  check(!cache.lookup(2, 0, &out), "erasing a file drops its cached blocks");
}

}  // namespace

int main() {
  std::cout << "lsm units\n";

  test_varint_roundtrip();
  test_fixed_encoding_is_explicitly_little_endian();
  test_internal_key_ordering();
  test_crc32c();

  test_memtable_basic();
  test_memtable_tombstones();
  test_memtable_iteration_is_sorted();
  test_memtable_is_deterministic();

  test_write_batch_roundtrip();
  test_wal_roundtrip_and_truncation();

  test_sstable_roundtrip();
  test_sstable_corruption_is_detected_not_served();
  test_tombstones_survive_until_the_bottom_level();
  test_block_cache();
  test_concurrent_writers_never_lose_read_after_write();

  if (g_failures == 0) {
    std::cout << "lsm units: all checks passed\n";
    return EXIT_SUCCESS;
  }
  std::cerr << "lsm units: " << g_failures << " check(s) failed\n";
  return EXIT_FAILURE;
}
