// Sorted string tables: the immutable on-disk form.
//
// Layout, bottom to top:
//
//   data block *      entries with restart points every 16 keys and
//                     shared-prefix compression against the previous key
//   filter block      Bloom filter over the *user* keys in the file
//   index block       last key of each data block -> that block's handle
//   footer            fixed 40 bytes: index handle, filter handle, magic
//
// Every block on disk carries a trailing CRC32C, and every read verifies it
// before the bytes are interpreted. That is INV-LSM-11 -- a corrupted block is
// reported as an error and *never* returned as data. The distinction matters
// more than it sounds: an engine that serves a corrupt block returns plausible
// garbage, and the resulting bug report is about the layer four levels above.
//
// Prefix compression is against the previous key within a restart interval, and
// the restart array is what makes binary search possible despite it. Without
// restarts a seek would have to scan the block from byte zero to reconstruct
// keys, which turns every point lookup into a linear scan of 4 KiB.
//
// The Bloom filter is over user keys, not internal keys, because a point lookup
// knows the user key and not which sequence number it wants. INV-LSM-07 says it
// must never produce a false *negative*; false positives merely cost a block
// read. Getting that backwards silently loses data, and it is invisible until
// someone reads a key nobody has read before.
//
// Not implemented, and noted rather than pretended: block compression (LZ4 /
// Zstd, both external dependencies) and ribbon filters. Neither changes a
// correctness property; both are P2 stretch items in docs/SCOPE.md.

#ifndef ANVIL_CORE_LSM_SSTABLE_H_
#define ANVIL_CORE_LSM_SSTABLE_H_

#include <cstdint>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "anvil/core/lsm/format.h"
#include "anvil/core/runtime/runtime.h"

namespace anvil::lsm {

// An arbitrary sentinel at a fixed offset from the end of the file. Its only
// job is to fail loudly when something that is not an SSTable is opened as one,
// rather than letting a garbage footer produce a garbage index.
constexpr std::uint64_t kTableMagic = 0xA0FE'DCBA'1234'5678ULL;
constexpr std::size_t kFooterSize = 40;  // 4 fixed64 handles + magic

struct BlockHandle {
  std::uint64_t offset = 0;
  std::uint64_t size = 0;
};

// ---------------------------------------------------------------------------
// blocks
// ---------------------------------------------------------------------------

class BlockBuilder {
 public:
  explicit BlockBuilder(int restart_interval = 16) : restart_interval_(restart_interval) {}

  void add(std::string_view key, std::string_view value);
  std::string finish();  // returns block contents (no CRC; the writer appends it)
  void reset();

  std::size_t size_estimate() const;
  bool empty() const noexcept { return entries_ == 0; }
  std::string_view last_key() const noexcept { return last_key_; }

 private:
  int restart_interval_;
  std::string buffer_;
  std::vector<std::uint32_t> restarts_{0};
  std::string last_key_;
  int since_restart_ = 0;
  std::size_t entries_ = 0;
};

// Reads a block that has already been fetched and checksum-verified.
class BlockReader {
 public:
  BlockReader() = default;
  // Returns false if the block is structurally malformed. A block that passed
  // its CRC can still be nonsense if the CRC itself was written over garbage,
  // so the parser validates rather than trusting.
  bool init(std::string contents);

  class Iterator {
   public:
    explicit Iterator(const BlockReader* block) : block_(block) {}

    bool valid() const noexcept { return valid_; }
    void seek_to_first();
    void seek(std::string_view internal_key);
    void next();

    std::string_view key() const noexcept { return key_; }
    std::string_view value() const noexcept { return value_; }

   private:
    bool parse_entry_at(std::uint32_t offset);

    const BlockReader* block_ = nullptr;
    std::uint32_t next_offset_ = 0;
    std::string key_;
    std::string_view value_;
    bool valid_ = false;
  };

  Iterator iterator() const { return Iterator{this}; }
  const std::string& contents() const noexcept { return contents_; }

 private:
  friend class Iterator;

  std::string contents_;
  std::uint32_t restart_offset_ = 0;  // where the restart array begins
  std::uint32_t num_restarts_ = 0;
};

// ---------------------------------------------------------------------------
// bloom filter
// ---------------------------------------------------------------------------

class BloomFilter {
 public:
  static std::string build(const std::vector<std::string>& user_keys, int bits_per_key = 10);
  // Conservative: returns true when unsure. A false negative would lose data.
  static bool may_contain(std::string_view filter, std::string_view user_key);
};

// ---------------------------------------------------------------------------
// block cache
// ---------------------------------------------------------------------------

// LRU over decoded blocks, keyed by (file number, offset).
//
// `erase_file` is the interesting method and exists for INV-LSM-13. File
// numbers are handed out monotonically, so reuse should be impossible -- but a
// cache that outlives a compaction and is never told the file went away is one
// bug away from serving blocks that belong to nothing. Making eviction explicit
// at the point of deletion is cheaper than reasoning about whether it can
// matter.
class BlockCache {
 public:
  explicit BlockCache(std::size_t capacity_bytes) : capacity_(capacity_bytes) {}

  bool lookup(std::uint64_t file_number, std::uint64_t offset, std::string* out);
  void insert(std::uint64_t file_number, std::uint64_t offset, std::string contents);
  void erase_file(std::uint64_t file_number);
  void clear();

  std::uint64_t hits() const noexcept { return hits_; }
  std::uint64_t misses() const noexcept { return misses_; }
  std::size_t bytes() const noexcept { return bytes_; }

 private:
  struct Key {
    std::uint64_t file_number;
    std::uint64_t offset;
    friend auto operator<=>(const Key&, const Key&) noexcept = default;
  };
  struct Entry {
    Key key;
    std::string contents;
  };

  void evict_to_fit();

  std::size_t capacity_;
  std::size_t bytes_ = 0;
  std::list<Entry> lru_;  // front = most recently used
  std::map<Key, std::list<Entry>::iterator> index_;
  std::uint64_t hits_ = 0;
  std::uint64_t misses_ = 0;
};

// ---------------------------------------------------------------------------
// table builder and reader
// ---------------------------------------------------------------------------

class TableBuilder {
 public:
  TableBuilder(Runtime* runtime, FileHandle file, std::size_t block_size = 4096)
      : runtime_(runtime), file_(file), block_size_(block_size) {}

  // Keys must arrive in internal-key order. Violating that produces a file that
  // reads back wrong rather than one that fails loudly, so the DB layer's merge
  // is responsible for the ordering and INV-LSM-04 checks it.
  Task<Status> add(std::string_view internal_key, std::string_view value);
  Task<Status> finish();

  std::uint64_t file_size() const noexcept { return offset_; }
  std::size_t entries() const noexcept { return entries_; }
  std::string_view smallest() const noexcept { return smallest_; }
  std::string_view largest() const noexcept { return largest_; }

 private:
  Task<Status> flush_data_block();
  Task<Status> write_block(std::string_view contents, BlockHandle* handle);

  Runtime* runtime_;
  FileHandle file_;
  std::size_t block_size_;
  std::uint64_t offset_ = 0;

  BlockBuilder data_block_;
  BlockBuilder index_block_;
  std::vector<std::string> filter_keys_;
  std::string pending_index_key_;
  bool has_pending_index_ = false;
  BlockHandle pending_handle_;

  std::string smallest_;
  std::string largest_;
  std::size_t entries_ = 0;
};

class Table {
 public:
  Table() = default;

  // Reads the footer, index and filter. Any checksum failure here is fatal for
  // the file: without a valid index there is nothing to serve.
  static Task<Status> open(Runtime* runtime, FileHandle file, std::uint64_t file_number,
                           BlockCache* cache, std::unique_ptr<Table>* out);

  // Looks up an internal key. `*found` is false when the key is absent from
  // this file, which is not an error.
  Task<Status> get(std::string_view internal_key, bool* found, std::string* value,
                   bool* is_deletion) const;

  // Streams every entry in order. Used by compaction and by full scans.
  Task<Status> for_each(
      const std::function<void(std::string_view internal_key, std::string_view value)>& fn)
      const;

  std::uint64_t file_number() const noexcept { return file_number_; }
  std::uint64_t filter_probes() const noexcept { return filter_probes_; }
  std::uint64_t filter_rejects() const noexcept { return filter_rejects_; }

 private:
  Task<Status> read_block(const BlockHandle& handle, std::string* out) const;

  Runtime* runtime_ = nullptr;
  FileHandle file_{};
  std::uint64_t file_number_ = 0;
  BlockCache* cache_ = nullptr;
  BlockReader index_;
  std::string filter_;
  mutable std::uint64_t filter_probes_ = 0;
  mutable std::uint64_t filter_rejects_ = 0;
};

}  // namespace anvil::lsm

#endif  // ANVIL_CORE_LSM_SSTABLE_H_
