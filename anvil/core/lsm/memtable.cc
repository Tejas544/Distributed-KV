#include "anvil/core/lsm/memtable.h"

#include <cstring>

namespace anvil::lsm {

// ---------------------------------------------------------------------------
// Arena
// ---------------------------------------------------------------------------

char* Arena::allocate_new_block(std::size_t block_bytes) {
  blocks_.push_back(std::make_unique<char[]>(block_bytes));
  usage_ += block_bytes;
  return blocks_.back().get();
}

char* Arena::allocate_fallback(std::size_t bytes) {
  if (bytes > kBlockSize / 4) {
    // A large request gets its own block rather than wasting the remainder of
    // the current one.
    return allocate_new_block(bytes);
  }
  cursor_ = allocate_new_block(kBlockSize);
  remaining_ = kBlockSize;
  char* result = cursor_;
  cursor_ += bytes;
  remaining_ -= bytes;
  return result;
}

char* Arena::allocate(std::size_t bytes) {
  if (bytes == 0) bytes = 1;
  if (bytes <= remaining_) {
    char* result = cursor_;
    cursor_ += bytes;
    remaining_ -= bytes;
    return result;
  }
  return allocate_fallback(bytes);
}

char* Arena::allocate_aligned(std::size_t bytes) {
  constexpr std::size_t kAlign = alignof(std::max_align_t);
  const auto current = reinterpret_cast<std::uintptr_t>(cursor_);
  const std::size_t slop = current == 0 ? 0 : (kAlign - (current & (kAlign - 1))) & (kAlign - 1);
  const std::size_t needed = bytes + slop;
  if (needed <= remaining_) {
    char* result = cursor_ + slop;
    cursor_ += needed;
    remaining_ -= needed;
    return result;
  }
  // Fresh blocks come from new[], which is already suitably aligned.
  return allocate_fallback(bytes);
}

std::string_view Arena::copy(std::string_view data) {
  if (data.empty()) return std::string_view{};
  char* dst = allocate(data.size());
  std::memcpy(dst, data.data(), data.size());
  return std::string_view{dst, data.size()};
}

// ---------------------------------------------------------------------------
// MemTable
// ---------------------------------------------------------------------------

struct MemTable::Node {
  std::string_view internal_key;
  std::string_view value;
  int height = 1;
  // Trailing flexible array of `height` forward pointers, allocated inline so a
  // node is one arena allocation rather than two.
  Node* next[1];
};

MemTable::MemTable(std::uint64_t seed)
    : rng_(DeterministicRandom{seed}.fork(RandomDomain::kApplication, 0x5C171157ULL)) {
  head_ = new_node(std::string_view{}, std::string_view{}, kMaxHeight);
  for (int i = 0; i < kMaxHeight; ++i) head_->next[i] = nullptr;
}

MemTable::Node* MemTable::new_node(std::string_view internal_key, std::string_view value,
                                   int height) {
  const std::size_t bytes =
      sizeof(Node) + sizeof(Node*) * static_cast<std::size_t>(height - 1);
  char* mem = arena_.allocate_aligned(bytes);
  auto* node = new (mem) Node{};
  node->internal_key = internal_key;
  node->value = value;
  node->height = height;
  for (int i = 0; i < height; ++i) node->next[i] = nullptr;
  return node;
}

int MemTable::random_height() {
  // One in four, integer arithmetic, from the seeded stream. No floating point,
  // and no global RNG -- the tree shape is part of the replayable execution.
  int height = 1;
  while (height < kMaxHeight && rng_.bernoulli(1, 4)) ++height;
  return height;
}

MemTable::Node* MemTable::find_greater_or_equal(std::string_view internal_key,
                                                Node** prev) const {
  Node* node = head_;
  int level = height_ - 1;
  for (;;) {
    Node* next = node->next[level];
    if (next != nullptr && compare_internal(next->internal_key, internal_key) < 0) {
      node = next;
      continue;
    }
    if (prev != nullptr) prev[level] = node;
    if (level == 0) return next;
    --level;
  }
}

void MemTable::add(SequenceNumber sequence, ValueType type, std::string_view key,
                   std::string_view value) {
  const std::string encoded = make_internal_key(key, sequence, type);
  const std::string_view internal_key = arena_.copy(encoded);
  const std::string_view stored_value =
      type == ValueType::kValue ? arena_.copy(value) : std::string_view{};

  Node* prev[kMaxHeight];
  find_greater_or_equal(internal_key, prev);

  const int height = random_height();
  if (height > height_) {
    for (int i = height_; i < height; ++i) prev[i] = head_;
    height_ = height;
  }

  Node* node = new_node(internal_key, stored_value, height);
  for (int i = 0; i < height; ++i) {
    node->next[i] = prev[i]->next[i];
    prev[i]->next[i] = node;
  }
  ++entries_;
}

bool MemTable::get(std::string_view key, SequenceNumber snapshot, std::string* found_value,
                   bool* is_deletion) const {
  // Seek to (key, snapshot, kValue). Because the trailer sorts descending, the
  // first entry at or after this position is the newest version visible to the
  // snapshot -- no scanning required.
  const std::string target = make_internal_key(key, snapshot, ValueType::kValue);
  Node* node = find_greater_or_equal(target, nullptr);
  if (node == nullptr) return false;
  if (compare_user(user_key_of(node->internal_key), key) != 0) return false;

  const std::uint64_t trailer = trailer_of(node->internal_key);
  if (sequence_of(trailer) > snapshot) return false;  // not visible to this snapshot

  if (type_of(trailer) == ValueType::kDeletion) {
    *is_deletion = true;
    return true;
  }
  *is_deletion = false;
  found_value->assign(node->value);
  return true;
}

// ---------------------------------------------------------------------------
// Iterator
// ---------------------------------------------------------------------------

void MemTable::Iterator::seek_to_first() { node_ = table_->head_->next[0]; }

void MemTable::Iterator::seek(std::string_view internal_key) {
  node_ = table_->find_greater_or_equal(internal_key, nullptr);
}

void MemTable::Iterator::next() {
  if (node_ == nullptr) return;
  node_ = static_cast<const Node*>(node_)->next[0];
}

std::string_view MemTable::Iterator::internal_key() const {
  return node_ == nullptr ? std::string_view{} : static_cast<const Node*>(node_)->internal_key;
}

std::string_view MemTable::Iterator::value() const {
  return node_ == nullptr ? std::string_view{} : static_cast<const Node*>(node_)->value;
}

}  // namespace anvil::lsm
