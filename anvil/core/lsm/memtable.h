// The memtable: a skiplist over internal keys, backed by an arena.
//
// Two decisions worth stating.
//
// **The arena.** Every node and every key/value byte lives in one bump-allocated
// region that is freed in a single stroke when the memtable is dropped. That is
// partly for speed, but mostly for determinism: node addresses become a pure
// function of the insertion sequence rather than of whatever the general
// allocator happened to be doing, which is what makes "this code accidentally
// depends on a pointer value" a reproducible bug instead of a heisenbug.
//
// **The height source.** Skiplist level selection is random, and that randomness
// has to come from the seed like everything else. The memtable takes a
// DeterministicRandom rather than reaching for a global, so the same seed
// produces the same tree shape -- and therefore the same iteration cost, the
// same flush timing, and the same downstream scheduling. A skiplist seeded from
// the clock would silently break replay for the entire engine above it.
//
// Lookups take a snapshot sequence number and return the newest version at or
// below it. Because internal keys sort newest-first within a user key
// (format.h), that is a single seek with no scanning.

#ifndef ANVIL_CORE_LSM_MEMTABLE_H_
#define ANVIL_CORE_LSM_MEMTABLE_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "anvil/core/lsm/format.h"
#include "anvil/core/random.h"

namespace anvil::lsm {

// A bump allocator. No individual frees: the whole region goes at once.
class Arena {
 public:
  Arena() = default;
  Arena(const Arena&) = delete;
  Arena& operator=(const Arena&) = delete;

  char* allocate(std::size_t bytes);
  char* allocate_aligned(std::size_t bytes);
  std::string_view copy(std::string_view data);

  std::size_t memory_usage() const noexcept { return usage_; }

 private:
  static constexpr std::size_t kBlockSize = 4096;

  char* allocate_fallback(std::size_t bytes);
  char* allocate_new_block(std::size_t block_bytes);

  std::vector<std::unique_ptr<char[]>> blocks_;
  char* cursor_ = nullptr;
  std::size_t remaining_ = 0;
  std::size_t usage_ = 0;
};

class MemTable {
 public:
  static constexpr int kMaxHeight = 12;

  explicit MemTable(std::uint64_t seed);
  MemTable(const MemTable&) = delete;
  MemTable& operator=(const MemTable&) = delete;

  void add(SequenceNumber sequence, ValueType type, std::string_view key,
           std::string_view value);

  // Returns true if `key` was found at or below `snapshot`. `*found_value` is
  // set only for kValue; a tombstone reports found==true with is_deletion set,
  // because "deleted" and "never existed" must not look the same to the layer
  // above -- a delete has to shadow an older value in a lower level.
  bool get(std::string_view key, SequenceNumber snapshot, std::string* found_value,
           bool* is_deletion) const;

  std::size_t memory_usage() const noexcept { return arena_.memory_usage(); }
  std::size_t entries() const noexcept { return entries_; }
  bool empty() const noexcept { return entries_ == 0; }

  // Forward iteration over internal keys, in sort order.
  class Iterator {
   public:
    explicit Iterator(const MemTable* table) : table_(table) {}

    bool valid() const noexcept { return node_ != nullptr; }
    void seek_to_first();
    void seek(std::string_view internal_key);
    void next();

    std::string_view internal_key() const;
    std::string_view value() const;

   private:
    const MemTable* table_;
    const void* node_ = nullptr;
  };

  Iterator iterator() const { return Iterator{this}; }

 private:
  friend class Iterator;

  struct Node;

  int random_height();
  Node* find_greater_or_equal(std::string_view internal_key, Node** prev) const;
  Node* new_node(std::string_view internal_key, std::string_view value, int height);

  Arena arena_;
  DeterministicRandom rng_;
  Node* head_ = nullptr;
  int height_ = 1;
  std::size_t entries_ = 0;
};

}  // namespace anvil::lsm

#endif  // ANVIL_CORE_LSM_MEMTABLE_H_
