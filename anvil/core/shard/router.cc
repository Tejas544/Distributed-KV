#include "anvil/core/shard/router.h"

namespace anvil::shard {

bool RangeCache::lookup(std::string_view key, RangeDescriptor* out) {
  auto it = by_start_.upper_bound(std::string{key});
  if (it == by_start_.begin()) {
    ++stats_.misses;
    return false;
  }
  --it;
  if (!it->second.contains(key)) {
    ++stats_.misses;
    return false;
  }
  ++stats_.hits;
  *out = it->second;
  return true;
}

void RangeCache::insert(const RangeDescriptor& desc) {
  // An older generation never replaces a newer one. Replies arrive out of
  // order, and a client that lets a late reply reinstate the descriptor it just
  // learned was stale will keep re-sending against it -- a livelock that looks
  // like a partition.
  const auto existing = by_id_.find(desc.id.value());
  if (existing != by_id_.end()) {
    const auto current = by_start_.find(existing->second);
    if (current != by_start_.end() && current->second.generation > desc.generation) return;
    if (current != by_start_.end() && current->first != desc.start) {
      by_start_.erase(current);
    }
  }
  by_start_[desc.start] = desc;
  by_id_[desc.id.value()] = desc.start;
  ++stats_.inserts;
}

void RangeCache::invalidate(RangeId id) {
  const auto it = by_id_.find(id.value());
  if (it == by_id_.end()) return;
  const std::string start = it->second;
  by_id_.erase(it);
  const auto entry = by_start_.find(start);
  if (entry == by_start_.end()) return;

  // The neighbour goes too. The two ways a descriptor becomes stale are a split
  // and a merge; a merge makes the *other* range's entry wrong as well, and
  // keeping it means the next request to the subsumed span is sent to a range
  // that no longer exists.
  auto next = by_start_.upper_bound(start);
  if (next != by_start_.end()) {
    by_id_.erase(next->second.id.value());
    by_start_.erase(next);
  }
  by_start_.erase(entry);
  ++stats_.invalidations;
}

void RangeCache::invalidate_key(std::string_view key) {
  auto it = by_start_.upper_bound(std::string{key});
  if (it == by_start_.begin()) return;
  --it;
  invalidate(it->second.id);
}

void RangeCache::clear() {
  by_start_.clear();
  by_id_.clear();
  buckets_.clear();
}

bool RangeCache::resolve(const MetaIndex& meta, std::string_view key, RangeDescriptor* out) {
  ++stats_.two_level_lookups;
  MetaBucket bucket;
  if (!meta.lookup_bucket(key, &bucket)) return false;
  if (!meta.lookup_range(bucket.id, key, out)) {
    // The bucket the first level named does not hold this key any more, which
    // is what a topology change looks like from below. Drop the level-one entry
    // and let the caller try again.
    ++stats_.bucket_misses;
    buckets_.erase(bucket.start);
    return false;
  }
  buckets_[bucket.start] = bucket.id;
  insert(*out);
  return true;
}

}  // namespace anvil::shard
