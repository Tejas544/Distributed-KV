#include "anvil/core/shard/descriptor.h"

#include <algorithm>

#include "anvil/core/lsm/format.h"

namespace anvil::shard {
namespace {

void put_nodes(std::string* out, const std::vector<NodeId>& nodes) {
  lsm::put_varint32(out, static_cast<std::uint32_t>(nodes.size()));
  for (const NodeId n : nodes) lsm::put_varint64(out, n.value());
}

const char* get_nodes(const char* p, const char* limit, std::vector<NodeId>* out) {
  std::uint32_t count = 0;
  p = lsm::get_varint32(p, limit, &count);
  if (p == nullptr) return nullptr;
  out->clear();
  out->reserve(count);
  for (std::uint32_t i = 0; i < count; ++i) {
    std::uint64_t id = 0;
    p = lsm::get_varint64(p, limit, &id);
    if (p == nullptr) return nullptr;
    out->push_back(NodeId{id});
  }
  return p;
}

}  // namespace

bool RangeDescriptor::is_voter(NodeId node) const noexcept {
  return std::find(replicas.begin(), replicas.end(), node) != replicas.end();
}

bool RangeDescriptor::hosts(NodeId node) const noexcept {
  return is_voter(node) ||
         std::find(learners.begin(), learners.end(), node) != learners.end();
}

std::string RangeDescriptor::describe() const {
  std::string out = "r" + std::to_string(id.value()) + "[" + (start.empty() ? "-inf" : start) +
                    "," + (end.empty() ? "+inf" : end) + ") gen " + std::to_string(generation) +
                    " on";
  for (const NodeId n : replicas) out += " n" + std::to_string(n.value());
  for (const NodeId n : learners) out += " n" + std::to_string(n.value()) + "(l)";
  if (lease.holder.valid()) out += " lease n" + std::to_string(lease.holder.value());
  if (frozen) out += " frozen";
  return out;
}

std::string encode_descriptor(const RangeDescriptor& desc) {
  std::string out;
  lsm::put_varint64(&out, desc.id.value());
  lsm::put_length_prefixed(&out, desc.start);
  lsm::put_length_prefixed(&out, desc.end);
  lsm::put_varint64(&out, desc.generation);
  put_nodes(&out, desc.replicas);
  put_nodes(&out, desc.learners);
  lsm::put_varint64(&out, desc.lease.holder.value());
  lsm::put_varint64(&out, desc.lease.start);
  lsm::put_varint64(&out, desc.lease.expiry);
  lsm::put_varint64(&out, desc.changed_at);
  lsm::put_varint64(&out, desc.changed_index);
  out.push_back(desc.frozen ? 1 : 0);
  return out;
}

bool decode_descriptor(std::string_view in, RangeDescriptor* out) {
  const char* p = in.data();
  const char* limit = p + in.size();
  std::uint64_t id = 0;
  p = lsm::get_varint64(p, limit, &id);
  if (p == nullptr) return false;
  std::string_view start;
  std::string_view end;
  p = lsm::get_length_prefixed(p, limit, &start);
  if (p == nullptr) return false;
  p = lsm::get_length_prefixed(p, limit, &end);
  if (p == nullptr) return false;
  std::uint64_t generation = 0;
  p = lsm::get_varint64(p, limit, &generation);
  if (p == nullptr) return false;
  std::vector<NodeId> replicas;
  std::vector<NodeId> learners;
  p = get_nodes(p, limit, &replicas);
  if (p == nullptr) return false;
  p = get_nodes(p, limit, &learners);
  if (p == nullptr) return false;
  std::uint64_t values[5] = {};
  for (std::uint64_t& v : values) {
    p = lsm::get_varint64(p, limit, &v);
    if (p == nullptr) return false;
  }
  if (p >= limit) return false;
  const bool frozen = *p++ != 0;

  out->id = RangeId{id};
  out->start.assign(start);
  out->end.assign(end);
  out->generation = generation;
  out->replicas = std::move(replicas);
  out->learners = std::move(learners);
  out->lease.holder = NodeId{values[0]};
  out->lease.start = values[1];
  out->lease.expiry = values[2];
  out->changed_at = values[3];
  out->changed_index = values[4];
  out->frozen = frozen;
  return true;
}

// ---------------------------------------------------------------------------
// meta index
// ---------------------------------------------------------------------------

std::string meta1_key(std::string_view bucket_start) {
  std::string out(1, '\x02');
  out.append(bucket_start);
  return out;
}

std::string meta2_key(std::string_view range_start) {
  std::string out(1, '\x03');
  out.append(range_start);
  return out;
}

void MetaIndex::rebuild(const std::map<std::string, RangeDescriptor>& ranges,
                        std::uint32_t records_per_bucket) {
  meta1_.clear();
  meta2_.clear();
  bucket_start_.clear();
  if (records_per_bucket == 0) records_per_bucket = 1;

  std::uint64_t bucket = 0;
  std::uint32_t in_bucket = records_per_bucket;  // force a new bucket on the first record
  for (const auto& [start, desc] : ranges) {
    if (in_bucket >= records_per_bucket) {
      ++bucket;
      in_bucket = 0;
      meta1_[meta1_key(start)] = bucket;
      bucket_start_[bucket] = start;
    }
    meta2_[meta2_key(start)] = encode_descriptor(desc);
    ++in_bucket;
  }
}

bool MetaIndex::lookup_bucket(std::string_view key, MetaBucket* out) const {
  // The greatest bucket start not above the key. upper_bound then step back:
  // the map is keyed by start, so there is no sentinel for the unbounded range
  // and no special case for the first one.
  const std::string probe = meta1_key(key);
  auto it = meta1_.upper_bound(probe);
  if (it == meta1_.begin()) return false;
  --it;
  out->id = it->second;
  out->start = it->first.substr(1);
  return true;
}

bool MetaIndex::lookup_range(std::uint64_t bucket, std::string_view key,
                             RangeDescriptor* out) const {
  const std::string probe = meta2_key(key);
  auto it = meta2_.upper_bound(probe);
  if (it == meta2_.begin()) return false;
  --it;
  RangeDescriptor desc;
  if (!decode_descriptor(it->second, &desc)) return false;
  if (!desc.contains(key)) return false;

  // The bucket the caller named has to be the bucket this record is actually
  // in. Skipping this check makes level one decorative: a stale bucket would
  // still resolve, through a level-two lookup that never needed it, and the
  // invalidation path the second level exists to exercise would never run.
  const auto bucket_it = bucket_start_.find(bucket);
  if (bucket_it == bucket_start_.end()) return false;
  auto next_bucket = meta1_.upper_bound(meta1_key(bucket_it->second));
  const bool above_bucket_start = desc.start >= bucket_it->second;
  const bool below_next_bucket =
      next_bucket == meta1_.end() || desc.start < next_bucket->first.substr(1);
  if (!above_bucket_start || !below_next_bucket) return false;

  *out = std::move(desc);
  return true;
}

void MetaIndex::encode(std::string* out) const {
  lsm::put_varint32(out, static_cast<std::uint32_t>(meta1_.size()));
  for (const auto& [key, bucket] : meta1_) {
    lsm::put_length_prefixed(out, key);
    lsm::put_varint64(out, bucket);
  }
  lsm::put_varint32(out, static_cast<std::uint32_t>(meta2_.size()));
  for (const auto& [key, value] : meta2_) {
    lsm::put_length_prefixed(out, key);
    lsm::put_length_prefixed(out, value);
  }
}

bool MetaIndex::decode(std::string_view in) {
  const char* p = in.data();
  const char* limit = p + in.size();
  std::uint32_t count = 0;
  p = lsm::get_varint32(p, limit, &count);
  if (p == nullptr) return false;
  meta1_.clear();
  bucket_start_.clear();
  for (std::uint32_t i = 0; i < count; ++i) {
    std::string_view key;
    std::uint64_t bucket = 0;
    p = lsm::get_length_prefixed(p, limit, &key);
    if (p == nullptr) return false;
    p = lsm::get_varint64(p, limit, &bucket);
    if (p == nullptr) return false;
    meta1_[std::string{key}] = bucket;
    bucket_start_[bucket] = std::string{key.substr(1)};
  }
  p = lsm::get_varint32(p, limit, &count);
  if (p == nullptr) return false;
  meta2_.clear();
  for (std::uint32_t i = 0; i < count; ++i) {
    std::string_view key;
    std::string_view value;
    p = lsm::get_length_prefixed(p, limit, &key);
    if (p == nullptr) return false;
    p = lsm::get_length_prefixed(p, limit, &value);
    if (p == nullptr) return false;
    meta2_[std::string{key}] = std::string{value};
  }
  return true;
}

}  // namespace anvil::shard
