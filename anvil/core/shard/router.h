// The client's range cache.
//
// A client that reads the meta index before every request has turned a
// distributed store into a store with one bottleneck, so it caches: key span,
// range id, generation, and who held the lease last time. The cache is
// therefore wrong for a while after every split, merge and lease move, and the
// whole design rests on being wrong *detectably*. Every request carries the
// generation the cache had; the range compares it against the generation it has
// applied and rejects a mismatch. The client then invalidates and re-resolves.
//
// The failure this prevents is the quiet one. A stale descriptor sends a write
// to a range that no longer owns the key. Without the generation check the
// range accepts it, the write lands in a range nobody will ever ask for that
// key again, and there is no error anywhere -- not at the client, not in the
// log, not in any invariant that only looks at one range at a time. It shows up
// weeks later as a balance that does not add up.

#ifndef ANVIL_CORE_SHARD_ROUTER_H_
#define ANVIL_CORE_SHARD_ROUTER_H_

#include <cstdint>
#include <map>
#include <string>
#include <string_view>

#include "anvil/core/shard/descriptor.h"

namespace anvil::shard {

struct CacheStats {
  std::uint64_t hits = 0;
  std::uint64_t misses = 0;
  std::uint64_t inserts = 0;
  std::uint64_t invalidations = 0;

  // Requests the cluster rejected because this cache was out of date. Not a
  // failure -- it is the mechanism working -- but the number is the only way to
  // tell "the cache is fine" from "the cache is never used because everything
  // is rejected".
  std::uint64_t stale_rejections = 0;
  std::uint64_t two_level_lookups = 0;
  std::uint64_t bucket_misses = 0;
};

class RangeCache {
 public:
  // The cached descriptor covering this key, if there is one.
  bool lookup(std::string_view key, RangeDescriptor* out);

  void insert(const RangeDescriptor& desc);

  // Drops everything that could have been affected. Called with the range the
  // cluster rejected: its span is the part of the cache known to be wrong, and
  // a neighbouring entry may be wrong too -- a merge invalidates both sides.
  void invalidate(RangeId id);
  void invalidate_key(std::string_view key);
  void clear();

  // The two-level resolution: meta1 for the bucket, meta2 for the range. Both
  // levels are consulted every time, because a level that is only consulted on
  // a cache miss is a level that is never tested.
  bool resolve(const MetaIndex& meta, std::string_view key, RangeDescriptor* out);

  void note_stale_rejection() noexcept { ++stats_.stale_rejections; }

  const CacheStats& stats() const noexcept { return stats_; }
  std::size_t size() const noexcept { return by_start_.size(); }

 private:
  std::map<std::string, RangeDescriptor> by_start_;
  std::map<std::uint64_t, std::string> by_id_;
  // The level-one answer, cached separately: a client that re-resolves a key
  // after a split usually finds the bucket unchanged.
  std::map<std::string, std::uint64_t> buckets_;
  CacheStats stats_;
};

}  // namespace anvil::shard

#endif  // ANVIL_CORE_SHARD_ROUTER_H_
