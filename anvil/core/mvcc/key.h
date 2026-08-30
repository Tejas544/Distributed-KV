// MVCC key encoding.
//
// Versions live in the storage engine's ordinary key space, above it rather
// than inside it. The engine already inverts its own sequence number so a seek
// lands on the newest *write*; this layer inverts a transaction commit
// timestamp so a seek lands on the newest *version visible to a reader*. The
// two are different questions and it is worth keeping them in different layers:
// the engine's sequence numbers are an implementation detail of durability, and
// commit timestamps are a property of the transaction protocol.
//
//     data key   'd' | escape(user_key) | be64(~commit_ts)
//     lock key   'l' | escape(user_key)
//
// `~commit_ts` big-endian is the inverted-timestamp trick: byte order then
// sorts versions of one key newest-first, so a snapshot read at `ts` is a single
// seek to `'d' | escape(key) | be64(~ts)` followed by taking the first record --
// no scan backwards, no reverse iterator, and no reading versions the reader
// cannot see.
//
// The escape matters and is easy to skip. Without it, key "a" followed by
// version bytes is indistinguishable from key "a\x00..." followed by different
// ones, and a scan for one key silently returns versions of another. The
// standard order-preserving escape is used: 0x00 becomes 0x00 0xFF, and the
// encoded key is terminated with 0x00 0x00. Both are less than every escaped
// byte pair, so the terminator sorts before any continuation and lexicographic
// order over encoded keys equals lexicographic order over the originals.

#ifndef ANVIL_CORE_MVCC_KEY_H_
#define ANVIL_CORE_MVCC_KEY_H_

#include <cstdint>
#include <string>
#include <string_view>

#include "anvil/core/types.h"

namespace anvil::mvcc {

// A commit timestamp. The same totally-ordered type the clock and the
// transaction layer use, flattened to 64 bits for encoding: the logical
// component is folded in so that two commits in the same physical nanosecond
// still order, which they must, because INV-MVCC-03 forbids duplicates.
using CommitTs = std::uint64_t;

constexpr CommitTs kMaxCommitTs = UINT64_MAX;

// Timestamp <-> CommitTs. Deliberately lossy in the same direction every time:
// 20 bits of logical counter under 44 bits of physical nanoseconds gives ~195
// days of simulated time and a million commits per nanosecond, which is more
// than any run will ever need and keeps the encoding one word wide.
constexpr CommitTs to_commit_ts(Timestamp ts) noexcept {
  return (ts.physical << 20) | (static_cast<std::uint64_t>(ts.logical) & 0xFFFFF);
}
constexpr Timestamp from_commit_ts(CommitTs ts) noexcept {
  return Timestamp{ts >> 20, static_cast<std::uint32_t>(ts & 0xFFFFF)};
}

constexpr char kDataPrefix = 'd';
constexpr char kLockPrefix = 'l';

// Order-preserving escape. Appends to `out`.
void append_escaped(std::string* out, std::string_view key);
// Returns the position after the terminator, or nullptr if malformed.
const char* decode_escaped(const char* p, const char* limit, std::string* out);

// Everything under one user key, in one string: the prefix a version scan runs
// over. `data_prefix(k)` is a prefix of every version of k and of nothing else.
std::string data_prefix(std::string_view user_key);
std::string lock_key(std::string_view user_key);

// The full key for one version.
std::string encode_data_key(std::string_view user_key, CommitTs commit_ts);

// Where a reader at `read_ts` seeks. The first key at or after this point that
// still carries `data_prefix(user_key)` is the newest version the reader may
// see, because the inverted timestamp puts newer versions *before* it.
std::string seek_for_read(std::string_view user_key, CommitTs read_ts);

// The exclusive upper bound of one key's versions -- the prefix with its last
// byte incremented, which is where a scan must stop.
std::string data_upper_bound(std::string_view user_key);

// Splits a data key back into its parts. False if it is not a data key.
bool decode_data_key(std::string_view encoded, std::string* user_key, CommitTs* commit_ts);
bool decode_lock_key(std::string_view encoded, std::string* user_key);

}  // namespace anvil::mvcc

#endif  // ANVIL_CORE_MVCC_KEY_H_
