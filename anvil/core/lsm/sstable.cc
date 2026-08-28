#include "anvil/core/lsm/sstable.h"

#include <algorithm>
#include <cstring>
#include <utility>

namespace anvil::lsm {
namespace {

constexpr std::size_t kBlockTrailerSize = 4;  // crc32c

ByteView as_bytes(std::string_view s) {
  return ByteView{reinterpret_cast<const std::byte*>(s.data()), s.size()};
}

std::size_t shared_prefix(std::string_view a, std::string_view b) {
  const std::size_t limit = std::min(a.size(), b.size());
  std::size_t i = 0;
  while (i < limit && a[i] == b[i]) ++i;
  return i;
}

// MurmurHash-ish 32-bit mixer. Only needs to be well-distributed and stable
// across builds -- a filter whose hash changed between the writer and the
// reader would produce false negatives, which is the one failure mode a Bloom
// filter is not allowed to have.
std::uint32_t bloom_hash(std::string_view key) {
  std::uint32_t h = 0x9747B28Cu ^ static_cast<std::uint32_t>(key.size());
  for (const char c : key) {
    h ^= static_cast<std::uint32_t>(static_cast<unsigned char>(c));
    h *= 0x5BD1E995u;
    h ^= h >> 15;
  }
  return h;
}

}  // namespace

// ---------------------------------------------------------------------------
// BlockBuilder
// ---------------------------------------------------------------------------

void BlockBuilder::add(std::string_view key, std::string_view value) {
  std::size_t shared = 0;
  if (since_restart_ < restart_interval_) {
    shared = shared_prefix(last_key_, key);
  } else {
    // Start a new restart interval: this key is stored in full so a binary
    // search can decode it without replaying everything before it.
    restarts_.push_back(static_cast<std::uint32_t>(buffer_.size()));
    since_restart_ = 0;
  }

  put_varint32(&buffer_, static_cast<std::uint32_t>(shared));
  put_varint32(&buffer_, static_cast<std::uint32_t>(key.size() - shared));
  put_varint32(&buffer_, static_cast<std::uint32_t>(value.size()));
  buffer_.append(key.substr(shared));
  buffer_.append(value);

  last_key_.assign(key);
  ++since_restart_;
  ++entries_;
}

std::string BlockBuilder::finish() {
  for (const std::uint32_t restart : restarts_) put_fixed32(&buffer_, restart);
  put_fixed32(&buffer_, static_cast<std::uint32_t>(restarts_.size()));
  return buffer_;
}

void BlockBuilder::reset() {
  buffer_.clear();
  restarts_.assign(1, 0);
  last_key_.clear();
  since_restart_ = 0;
  entries_ = 0;
}

std::size_t BlockBuilder::size_estimate() const {
  return buffer_.size() + (restarts_.size() + 1) * sizeof(std::uint32_t);
}

// ---------------------------------------------------------------------------
// BlockReader
// ---------------------------------------------------------------------------

bool BlockReader::init(std::string contents) {
  if (contents.size() < sizeof(std::uint32_t)) return false;
  contents_ = std::move(contents);

  const std::uint32_t num_restarts =
      decode_fixed32(contents_.data() + contents_.size() - sizeof(std::uint32_t));
  const std::size_t restart_bytes = (num_restarts + 1) * sizeof(std::uint32_t);
  if (restart_bytes > contents_.size()) return false;

  num_restarts_ = num_restarts;
  restart_offset_ = static_cast<std::uint32_t>(contents_.size() - restart_bytes);
  return true;
}

bool BlockReader::Iterator::parse_entry_at(std::uint32_t offset) {
  const char* p = block_->contents_.data() + offset;
  const char* const limit = block_->contents_.data() + block_->restart_offset_;
  if (p >= limit) {
    valid_ = false;
    return false;
  }

  std::uint32_t shared = 0;
  std::uint32_t non_shared = 0;
  std::uint32_t value_len = 0;
  p = get_varint32(p, limit, &shared);
  if (p != nullptr) p = get_varint32(p, limit, &non_shared);
  if (p != nullptr) p = get_varint32(p, limit, &value_len);
  if (p == nullptr || shared > key_.size()) {
    valid_ = false;
    return false;
  }
  if (static_cast<std::size_t>(limit - p) < static_cast<std::size_t>(non_shared) + value_len) {
    valid_ = false;
    return false;
  }

  key_.resize(shared);
  key_.append(p, non_shared);
  p += non_shared;
  value_ = std::string_view{p, value_len};
  p += value_len;

  next_offset_ = static_cast<std::uint32_t>(p - block_->contents_.data());
  valid_ = true;
  return true;
}

void BlockReader::Iterator::seek_to_first() {
  key_.clear();
  parse_entry_at(0);
}

void BlockReader::Iterator::seek(std::string_view internal_key) {
  // Binary search over restart points for the last one whose first key is <=
  // the target, then scan forward within that interval.
  std::uint32_t left = 0;
  std::uint32_t right = block_->num_restarts_ == 0 ? 0 : block_->num_restarts_ - 1;
  const char* const restarts =
      block_->contents_.data() + block_->restart_offset_;

  while (left < right) {
    const std::uint32_t mid = (left + right + 1) / 2;
    const std::uint32_t region = decode_fixed32(restarts + mid * sizeof(std::uint32_t));
    key_.clear();
    if (!parse_entry_at(region)) {
      right = mid - 1;
      continue;
    }
    if (compare_internal(key_, internal_key) < 0) {
      left = mid;
    } else {
      right = mid - 1;
    }
  }

  const std::uint32_t start =
      block_->num_restarts_ == 0 ? 0 : decode_fixed32(restarts + left * sizeof(std::uint32_t));
  key_.clear();
  if (!parse_entry_at(start)) return;
  while (valid_ && compare_internal(key_, internal_key) < 0) next();
}

void BlockReader::Iterator::next() {
  if (!valid_) return;
  parse_entry_at(next_offset_);
}

// ---------------------------------------------------------------------------
// BloomFilter
// ---------------------------------------------------------------------------

std::string BloomFilter::build(const std::vector<std::string>& user_keys, int bits_per_key) {
  // 0.69 * bits_per_key is the optimal probe count; clamped so a pathological
  // configuration cannot make lookups arbitrarily expensive.
  int k = (bits_per_key * 69) / 100;
  k = std::clamp(k, 1, 30);

  std::size_t bits = user_keys.size() * static_cast<std::size_t>(bits_per_key);
  if (bits < 64) bits = 64;  // tiny filters are almost all false positives
  const std::size_t bytes = (bits + 7) / 8;
  bits = bytes * 8;

  std::string filter(bytes, '\0');
  for (const std::string& key : user_keys) {
    std::uint32_t h = bloom_hash(key);
    const std::uint32_t delta = (h >> 17) | (h << 15);  // rotate right by 17
    for (int i = 0; i < k; ++i) {
      const std::size_t bit = h % bits;
      filter[bit / 8] = static_cast<char>(static_cast<unsigned char>(filter[bit / 8]) |
                                          (1u << (bit % 8)));
      h += delta;
    }
  }
  filter.push_back(static_cast<char>(k));  // probe count travels with the filter
  return filter;
}

bool BloomFilter::may_contain(std::string_view filter, std::string_view user_key) {
  // An absent or malformed filter must answer "maybe", never "no". Returning
  // false here on a damaged filter would turn corruption into silent data loss
  // -- the key would simply stop being found (INV-LSM-07).
  if (filter.size() < 2) return true;

  const auto k = static_cast<int>(static_cast<unsigned char>(filter.back()));
  if (k < 1 || k > 30) return true;

  const std::size_t bytes = filter.size() - 1;
  const std::size_t bits = bytes * 8;

  std::uint32_t h = bloom_hash(user_key);
  const std::uint32_t delta = (h >> 17) | (h << 15);
  for (int i = 0; i < k; ++i) {
    const std::size_t bit = h % bits;
    if ((static_cast<unsigned char>(filter[bit / 8]) & (1u << (bit % 8))) == 0) return false;
    h += delta;
  }
  return true;
}

// ---------------------------------------------------------------------------
// BlockCache
// ---------------------------------------------------------------------------

bool BlockCache::lookup(std::uint64_t file_number, std::uint64_t offset, std::string* out) {
  const auto it = index_.find(Key{file_number, offset});
  if (it == index_.end()) {
    ++misses_;
    return false;
  }
  ++hits_;
  lru_.splice(lru_.begin(), lru_, it->second);
  *out = it->second->contents;
  return true;
}

void BlockCache::insert(std::uint64_t file_number, std::uint64_t offset,
                        std::string contents) {
  const Key key{file_number, offset};
  const auto existing = index_.find(key);
  if (existing != index_.end()) {
    bytes_ -= existing->second->contents.size();
    lru_.erase(existing->second);
    index_.erase(existing);
  }
  bytes_ += contents.size();
  lru_.push_front(Entry{key, std::move(contents)});
  index_[key] = lru_.begin();
  evict_to_fit();
}

void BlockCache::evict_to_fit() {
  while (bytes_ > capacity_ && !lru_.empty()) {
    const Entry& victim = lru_.back();
    bytes_ -= victim.contents.size();
    index_.erase(victim.key);
    lru_.pop_back();
  }
}

void BlockCache::erase_file(std::uint64_t file_number) {
  for (auto it = lru_.begin(); it != lru_.end();) {
    if (it->key.file_number != file_number) {
      ++it;
      continue;
    }
    bytes_ -= it->contents.size();
    index_.erase(it->key);
    it = lru_.erase(it);
  }
}

void BlockCache::clear() {
  lru_.clear();
  index_.clear();
  bytes_ = 0;
}

// ---------------------------------------------------------------------------
// TableBuilder
// ---------------------------------------------------------------------------

Task<Status> TableBuilder::write_block(std::string_view contents, BlockHandle* handle) {
  std::string payload{contents};
  put_fixed32(&payload, crc32c(contents));

  const Status status = co_await runtime_->pwrite(file_, as_bytes(payload), offset_);
  if (!status.is_ok()) co_return status;

  handle->offset = offset_;
  handle->size = contents.size();  // excludes the trailer; readers add it back
  offset_ += payload.size();
  co_return Status::ok();
}

Task<Status> TableBuilder::flush_data_block() {
  if (data_block_.empty()) co_return Status::ok();

  const std::string last = std::string{data_block_.last_key()};
  const std::string contents = data_block_.finish();
  BlockHandle handle;
  const Status status = co_await write_block(contents, &handle);
  if (!status.is_ok()) co_return status;
  data_block_.reset();

  // The index entry for a block is added only once the *next* block starts, so
  // the separator key can be the block's last key rather than something longer.
  pending_index_key_ = last;
  pending_handle_ = handle;
  has_pending_index_ = true;
  co_return Status::ok();
}

Task<Status> TableBuilder::add(std::string_view internal_key, std::string_view value) {
  if (has_pending_index_) {
    std::string encoded;
    put_varint64(&encoded, pending_handle_.offset);
    put_varint64(&encoded, pending_handle_.size);
    index_block_.add(pending_index_key_, encoded);
    has_pending_index_ = false;
  }

  if (entries_ == 0) smallest_.assign(internal_key);
  largest_.assign(internal_key);

  data_block_.add(internal_key, value);
  filter_keys_.emplace_back(user_key_of(internal_key));
  ++entries_;

  if (data_block_.size_estimate() >= block_size_) {
    co_return co_await flush_data_block();
  }
  co_return Status::ok();
}

Task<Status> TableBuilder::finish() {
  Status status = co_await flush_data_block();
  if (!status.is_ok()) co_return status;

  if (has_pending_index_) {
    std::string encoded;
    put_varint64(&encoded, pending_handle_.offset);
    put_varint64(&encoded, pending_handle_.size);
    index_block_.add(pending_index_key_, encoded);
    has_pending_index_ = false;
  }

  // Deduplicate before building the filter: a key written many times costs
  // probes but adds no information.
  std::sort(filter_keys_.begin(), filter_keys_.end());
  filter_keys_.erase(std::unique(filter_keys_.begin(), filter_keys_.end()), filter_keys_.end());

  BlockHandle filter_handle;
  status = co_await write_block(BloomFilter::build(filter_keys_), &filter_handle);
  if (!status.is_ok()) co_return status;

  BlockHandle index_handle;
  status = co_await write_block(index_block_.finish(), &index_handle);
  if (!status.is_ok()) co_return status;

  std::string footer;
  put_fixed64(&footer, index_handle.offset);
  put_fixed64(&footer, index_handle.size);
  put_fixed64(&footer, filter_handle.offset);
  put_fixed64(&footer, filter_handle.size);
  put_fixed64(&footer, kTableMagic);

  status = co_await runtime_->pwrite(file_, as_bytes(footer), offset_);
  if (!status.is_ok()) co_return status;
  offset_ += footer.size();

  // The caller is responsible for fsync and for the directory entry. Doing it
  // here would hide the durability decision inside the writer, and hiding it is
  // exactly how a missing fsync survives review.
  co_return Status::ok();
}

// ---------------------------------------------------------------------------
// Table
// ---------------------------------------------------------------------------

Task<Status> Table::read_block(const BlockHandle& handle, std::string* out) const {
  if (cache_ != nullptr && cache_->lookup(file_number_, handle.offset, out)) {
    co_return Status::ok();
  }

  const std::size_t total = handle.size + kBlockTrailerSize;
  std::string buffer(total, '\0');
  std::size_t read = 0;
  const Status status = co_await runtime_->pread(
      file_, MutableByteView{reinterpret_cast<std::byte*>(buffer.data()), buffer.size()},
      handle.offset, &read);
  if (!status.is_ok()) co_return status;
  if (read < total) co_return Status{StatusCode::kCorruption, "short block read"};

  const std::string_view contents{buffer.data(), handle.size};
  const std::uint32_t stored = decode_fixed32(buffer.data() + handle.size);
  if (crc32c(contents) != stored) {
    // INV-LSM-11. The bytes are never returned to the caller; a corrupted block
    // is an error, and an error is the only honest answer.
    co_return Status{StatusCode::kCorruption, "block checksum mismatch"};
  }

  out->assign(contents);
  if (cache_ != nullptr) cache_->insert(file_number_, handle.offset, *out);
  co_return Status::ok();
}

Task<Status> Table::open(Runtime* runtime, FileHandle file, std::uint64_t file_number,
                         BlockCache* cache, std::unique_ptr<Table>* out) {
  auto table = std::make_unique<Table>();
  table->runtime_ = runtime;
  table->file_ = file;
  table->file_number_ = file_number;
  table->cache_ = cache;

  std::uint64_t size = 0;
  Status status = co_await runtime->file_size(file, &size);
  if (!status.is_ok()) co_return status;
  if (size < kFooterSize) co_return Status{StatusCode::kCorruption, "file too short for footer"};

  std::string footer(kFooterSize, '\0');
  std::size_t read = 0;
  status = co_await runtime->pread(
      file, MutableByteView{reinterpret_cast<std::byte*>(footer.data()), footer.size()},
      size - kFooterSize, &read);
  if (!status.is_ok()) co_return status;
  if (read < kFooterSize) co_return Status{StatusCode::kCorruption, "short footer read"};

  if (decode_fixed64(footer.data() + 32) != kTableMagic) {
    co_return Status{StatusCode::kCorruption, "bad table magic"};
  }

  const BlockHandle index_handle{decode_fixed64(footer.data()),
                                 decode_fixed64(footer.data() + 8)};
  const BlockHandle filter_handle{decode_fixed64(footer.data() + 16),
                                  decode_fixed64(footer.data() + 24)};

  if (index_handle.offset + index_handle.size > size ||
      filter_handle.offset + filter_handle.size > size) {
    co_return Status{StatusCode::kCorruption, "footer handles out of range"};
  }

  std::string index_contents;
  status = co_await table->read_block(index_handle, &index_contents);
  if (!status.is_ok()) co_return status;
  if (!table->index_.init(std::move(index_contents))) {
    co_return Status{StatusCode::kCorruption, "malformed index block"};
  }

  status = co_await table->read_block(filter_handle, &table->filter_);
  if (!status.is_ok()) co_return status;

  *out = std::move(table);
  co_return Status::ok();
}

Task<Status> Table::get(std::string_view internal_key, bool* found, std::string* value,
                        bool* is_deletion) const {
  *found = false;

  const std::string_view user = user_key_of(internal_key);
  ++filter_probes_;
  if (!BloomFilter::may_contain(filter_, user)) {
    ++filter_rejects_;
    co_return Status::ok();
  }

  auto index_it = index_.iterator();
  index_it.seek(internal_key);
  if (!index_it.valid()) co_return Status::ok();  // beyond the last block

  const char* p = index_it.value().data();
  const char* const limit = p + index_it.value().size();
  BlockHandle handle;
  p = get_varint64(p, limit, &handle.offset);
  if (p != nullptr) p = get_varint64(p, limit, &handle.size);
  if (p == nullptr) co_return Status{StatusCode::kCorruption, "malformed index entry"};

  std::string block_contents;
  const Status status = co_await read_block(handle, &block_contents);
  if (!status.is_ok()) co_return status;

  BlockReader block;
  if (!block.init(std::move(block_contents))) {
    co_return Status{StatusCode::kCorruption, "malformed data block"};
  }

  auto it = block.iterator();
  it.seek(internal_key);
  if (!it.valid()) co_return Status::ok();
  if (compare_user(user_key_of(it.key()), user) != 0) co_return Status::ok();

  *found = true;
  *is_deletion = type_of(trailer_of(it.key())) == ValueType::kDeletion;
  if (!*is_deletion) value->assign(it.value());
  co_return Status::ok();
}

Task<Status> Table::for_each(
    const std::function<void(std::string_view, std::string_view)>& fn) const {
  auto index_it = index_.iterator();
  for (index_it.seek_to_first(); index_it.valid(); index_it.next()) {
    const char* p = index_it.value().data();
    const char* const limit = p + index_it.value().size();
    BlockHandle handle;
    p = get_varint64(p, limit, &handle.offset);
    if (p != nullptr) p = get_varint64(p, limit, &handle.size);
    if (p == nullptr) co_return Status{StatusCode::kCorruption, "malformed index entry"};

    std::string contents;
    const Status status = co_await read_block(handle, &contents);
    if (!status.is_ok()) co_return status;

    BlockReader block;
    if (!block.init(std::move(contents))) {
      co_return Status{StatusCode::kCorruption, "malformed data block"};
    }
    auto it = block.iterator();
    for (it.seek_to_first(); it.valid(); it.next()) {
      fn(it.key(), it.value());
    }
  }
  co_return Status::ok();
}

}  // namespace anvil::lsm
