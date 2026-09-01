// Range descriptors and the meta index.
//
// A range is three things at once, and keeping them in one object is what makes
// the coverage invariant statable:
//
//   * a half-open key span [start, end), which is the unit of routing;
//   * a replica set, which is the unit of consensus -- one Raft group per range;
//   * a generation, which is the unit of cache invalidation.
//
// The generation is the least obvious and the most load-bearing. A client caches
// a descriptor and sends requests against it; the range checks the generation
// the request names against the one it currently has, and rejects a mismatch
// with kRangeKeyMismatch rather than serving it. Without that check a client
// holding a pre-split descriptor writes a key the range no longer owns, and the
// write lands in a range that will never be asked for it again. It is not
// detectable afterwards -- the data is simply somewhere nobody looks.
//
// `end` empty means unbounded above. That is a real value, not a sentinel to be
// avoided: it is the end of the last range, forever, and every comparison here
// handles it explicitly. What the meta index deliberately does NOT do is key
// itself by the end bound, which would put the unbounded range's record at the
// bottom of the map instead of the top. Keying by `start` and looking up with
// "greatest start not above the key" needs no sentinel and no special case.

#ifndef ANVIL_CORE_SHARD_DESCRIPTOR_H_
#define ANVIL_CORE_SHARD_DESCRIPTOR_H_

#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <vector>

#include "anvil/core/types.h"

namespace anvil::shard {

// The two groups that are not ranges. Reserved, so that a range id and a
// group id are the same number and range ids start at 3.
//
// The timestamp oracle gets its own group rather than a field in the
// placement group's state, and the reason is failover: a timestamp
// reservation and a range split are unrelated decisions, and putting them in
// one log means every timestamp batch waits behind whatever the placement
// driver is doing. The monotonicity argument (INV-TXN-09) is identical
// either way -- it is Raft's -- so the split costs nothing but a group.
inline constexpr std::uint64_t kMetaGroup = 1;
inline constexpr std::uint64_t kOracleGroup = 2;

// A lease: the right to serve reads for a range without a quorum round-trip,
// for as long as the clock says it lasts.
//
// Both bounds are physical nanoseconds on the granting node's clock, recorded
// in the entry that grants it, so every replica agrees on the interval without
// consulting its own clock. Whether the interval has *passed* is the one
// question each node answers locally, and that is precisely where clock skew
// enters -- which is why INV-SHARD-04 is stated with the declared uncertainty
// bound in it rather than as a bare "at most one".
struct Lease {
  NodeId holder{};
  std::uint64_t start = 0;
  std::uint64_t expiry = 0;

  bool held_by(NodeId node) const noexcept { return holder == node && expiry > start; }
  bool valid_at(std::uint64_t now) const noexcept {
    return holder.valid() && now >= start && now < expiry;
  }
  friend bool operator==(const Lease&, const Lease&) noexcept = default;
};

struct RangeDescriptor {
  RangeId id{};
  std::string start;  // inclusive; empty == unbounded below
  std::string end;    // exclusive; empty == unbounded above
  std::uint64_t generation = 1;

  // Sorted, always. Two nodes must produce byte-identical descriptor bytes for
  // the same membership or a snapshot digest differs across replicas for no
  // reason -- the same rule as raft::ConfState, for the same reason.
  std::vector<NodeId> replicas;
  std::vector<NodeId> learners;

  Lease lease;

  // When this descriptor last changed, on the clock of the node that proposed
  // the change. It exists so that placement can require a range to have been
  // stable for a while before touching it again: without a cooldown, a split
  // threshold and a merge threshold that overlap put the cluster into an
  // endless split-merge oscillation that looks exactly like a working
  // rebalancer until you count the topology changes.
  std::uint64_t changed_at = 0;

  // And the placement log index at which it changed. This is the one the
  // placement driver actually uses, because `changed_at` is a clock reading
  // from whichever node proposed the change and every consumer of it is on a
  // different node with a different clock. Subtracting one from the other is
  // how a merge timeout of five seconds fires instantly on a cluster whose
  // clocks are half a second apart (ANV-0045). The log index is replicated,
  // monotone, and the same number everywhere.
  std::uint64_t changed_index = 0;

  // Set between the two halves of a merge: this range is being subsumed, has
  // stopped accepting writes, and its data is final. A frozen range is still
  // routed to, and still answers -- with a rejection. Removing it from the meta
  // index instead would leave a gap in the coverage, which is the one thing the
  // topology may never have.
  bool frozen = false;

  GroupId group() const noexcept { return GroupId{id.value()}; }

  bool contains(std::string_view key) const noexcept {
    if (key < start) return false;
    return end.empty() || key < end;
  }

  // Whether this range owns every key in [lo, hi] -- the question a multi-key
  // operation has to ask, and the one whose wrong answer is a transaction that
  // half-applies across a split point.
  bool covers(std::string_view lo, std::string_view hi) const noexcept {
    return contains(lo) && contains(hi);
  }

  bool is_voter(NodeId node) const noexcept;
  bool hosts(NodeId node) const noexcept;  // voter or learner

  // The majority of the voter set. n/2 and not (n-1)/2: they agree for odd n,
  // which is every hand-written test, and differ for even n, which is every
  // membership transition (ANV-0013).
  std::size_t quorum() const noexcept { return replicas.size() / 2 + 1; }

  std::string describe() const;
};

std::string encode_descriptor(const RangeDescriptor& desc);
bool decode_descriptor(std::string_view in, RangeDescriptor* out);

// ---------------------------------------------------------------------------
// the meta index
// ---------------------------------------------------------------------------

// Two levels, both stored as records in the key space rather than as a service:
//
//   meta1  '\x02' + bucket_start  ->  bucket id
//   meta2  '\x03' + range_start   ->  encoded descriptor
//
// A client resolves a key by finding its bucket in meta1 and then its range in
// that bucket's slice of meta2. Two lookups, both cached, both invalidated by
// generation -- which is the property the second level exists to exercise.
//
// What this is not: the second level does not live in its own Raft group. With
// one meta group there is nothing to gain from that and a great deal to get
// wrong, so the buckets are logical. Said out loud rather than implied, because
// "two-level meta index" usually means the other thing.
std::string meta1_key(std::string_view bucket_start);
std::string meta2_key(std::string_view range_start);

struct MetaBucket {
  std::uint64_t id = 0;
  std::string start;
};

class MetaIndex {
 public:
  // Rebuilds both levels from the descriptor table. Called on every topology
  // change, because a meta index maintained incrementally is a second
  // implementation of the topology and the two disagree the first time an edit
  // is applied on one and not the other (INV-SHARD-07 exists to catch exactly
  // that, and would be checking a copy of itself if this were incremental).
  void rebuild(const std::map<std::string, RangeDescriptor>& ranges,
               std::uint32_t records_per_bucket);

  // Level one: the bucket that would hold this key's descriptor.
  bool lookup_bucket(std::string_view key, MetaBucket* out) const;

  // Level two: the descriptor for this key, within that bucket. Fails if the
  // bucket named is not the one that holds the key -- which is what a stale
  // level-one cache looks like, and the client's cue to re-resolve.
  bool lookup_range(std::uint64_t bucket, std::string_view key, RangeDescriptor* out) const;

  const std::map<std::string, std::uint64_t>& level1() const noexcept { return meta1_; }
  const std::map<std::string, std::string>& level2() const noexcept { return meta2_; }

  std::size_t size() const noexcept { return meta2_.size(); }

  void encode(std::string* out) const;
  bool decode(std::string_view in);

 private:
  std::map<std::string, std::uint64_t> meta1_;  // bucket start -> bucket id
  std::map<std::string, std::string> meta2_;    // range start -> descriptor bytes
  std::map<std::uint64_t, std::string> bucket_start_;
};

}  // namespace anvil::shard

#endif  // ANVIL_CORE_SHARD_DESCRIPTOR_H_
