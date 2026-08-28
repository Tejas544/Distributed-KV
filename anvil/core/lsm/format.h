// On-disk encoding primitives.
//
// Everything persisted by the engine goes through this file, and two properties
// matter more than efficiency:
//
//   Explicit endianness. Integers are encoded byte by byte, never memcpy'd from
//   native representation. Both target architectures are little-endian today,
//   which is exactly why this is easy to get wrong and never notice -- and a
//   file written on one machine that decodes differently on another would break
//   the cross-architecture determinism gate in a way that looks like a
//   simulator bug.
//
//   Checksums on everything. INV-LSM-11 says a corrupted block is reported as
//   an error and never served as data. That is only enforceable if every
//   persisted unit carries a checksum, so the primitives make it the default
//   rather than something each call site remembers.
//
// The internal key layout is LevelDB's, and the trailing sequence number is
// inverted at comparison time rather than at encode time:
//
//     internal_key := user_key || fixed64_le(sequence << 8 | type)
//
// Ordering is user_key ascending, then the trailer *descending*, so the newest
// version of a key sorts first and a seek lands on it directly. Getting that
// backwards produces an engine that reads stale data under exactly the
// interleavings that are hardest to reproduce.

#ifndef ANVIL_CORE_LSM_FORMAT_H_
#define ANVIL_CORE_LSM_FORMAT_H_

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include "anvil/core/types.h"

namespace anvil::lsm {

// A write is either a value or a tombstone. Deletions are records, not
// absences: an LSM cannot delete in place, so a delete is a write that shadows
// everything below it until compaction drops both.
enum class ValueType : std::uint8_t {
  kDeletion = 0,
  kValue = 1,
};

using SequenceNumber = std::uint64_t;

// Leaves 8 bits for the type in the packed trailer.
constexpr SequenceNumber kMaxSequenceNumber = (1ULL << 56) - 1;

constexpr std::uint64_t pack_trailer(SequenceNumber seq, ValueType type) {
  return (seq << 8) | static_cast<std::uint64_t>(type);
}
constexpr SequenceNumber sequence_of(std::uint64_t trailer) { return trailer >> 8; }
constexpr ValueType type_of(std::uint64_t trailer) {
  return static_cast<ValueType>(trailer & 0xFF);
}

// ---------------------------------------------------------------------------
// fixed-width little-endian
// ---------------------------------------------------------------------------

inline void put_fixed32(std::string* dst, std::uint32_t value) {
  for (int i = 0; i < 4; ++i) {
    dst->push_back(static_cast<char>((value >> (8 * i)) & 0xFF));
  }
}

inline void put_fixed64(std::string* dst, std::uint64_t value) {
  for (int i = 0; i < 8; ++i) {
    dst->push_back(static_cast<char>((value >> (8 * i)) & 0xFF));
  }
}

inline std::uint32_t decode_fixed32(const char* p) {
  std::uint32_t value = 0;
  for (int i = 0; i < 4; ++i) {
    value |= static_cast<std::uint32_t>(static_cast<unsigned char>(p[i])) << (8 * i);
  }
  return value;
}

inline std::uint64_t decode_fixed64(const char* p) {
  std::uint64_t value = 0;
  for (int i = 0; i < 8; ++i) {
    value |= static_cast<std::uint64_t>(static_cast<unsigned char>(p[i])) << (8 * i);
  }
  return value;
}

// ---------------------------------------------------------------------------
// varints
// ---------------------------------------------------------------------------

inline void put_varint32(std::string* dst, std::uint32_t value) {
  while (value >= 0x80) {
    dst->push_back(static_cast<char>((value & 0x7F) | 0x80));
    value >>= 7;
  }
  dst->push_back(static_cast<char>(value));
}

inline void put_varint64(std::string* dst, std::uint64_t value) {
  while (value >= 0x80) {
    dst->push_back(static_cast<char>((value & 0x7F) | 0x80));
    value >>= 7;
  }
  dst->push_back(static_cast<char>(value));
}

// Returns nullptr on a malformed or truncated varint. Callers must check:
// a corrupt block is a normal, expected input here, not an exceptional one.
inline const char* get_varint32(const char* p, const char* limit, std::uint32_t* value) {
  std::uint32_t result = 0;
  for (int shift = 0; shift <= 28 && p < limit; shift += 7) {
    const auto byte = static_cast<std::uint32_t>(static_cast<unsigned char>(*p++));
    if (byte & 0x80) {
      result |= (byte & 0x7F) << shift;
    } else {
      *value = result | (byte << shift);
      return p;
    }
  }
  return nullptr;
}

inline const char* get_varint64(const char* p, const char* limit, std::uint64_t* value) {
  std::uint64_t result = 0;
  for (int shift = 0; shift <= 63 && p < limit; shift += 7) {
    const auto byte = static_cast<std::uint64_t>(static_cast<unsigned char>(*p++));
    if (byte & 0x80) {
      result |= (byte & 0x7F) << shift;
    } else {
      *value = result | (byte << shift);
      return p;
    }
  }
  return nullptr;
}

inline void put_length_prefixed(std::string* dst, std::string_view value) {
  put_varint32(dst, static_cast<std::uint32_t>(value.size()));
  dst->append(value);
}

inline const char* get_length_prefixed(const char* p, const char* limit,
                                       std::string_view* value) {
  std::uint32_t length = 0;
  p = get_varint32(p, limit, &length);
  if (p == nullptr || static_cast<std::size_t>(limit - p) < length) return nullptr;
  *value = std::string_view{p, length};
  return p + length;
}

// ---------------------------------------------------------------------------
// CRC32C
// ---------------------------------------------------------------------------

// Software table-driven Castagnoli CRC. Deliberately not the SSE4.2 intrinsic:
// the results are identical, but a build that silently switches implementations
// based on the host CPU is one more thing that can differ between the machine
// that wrote a file and the machine that reads it back.
std::uint32_t crc32c(std::string_view data);
std::uint32_t crc32c_extend(std::uint32_t crc, std::string_view data);

// ---------------------------------------------------------------------------
// internal keys
// ---------------------------------------------------------------------------

inline std::string make_internal_key(std::string_view user_key, SequenceNumber seq,
                                     ValueType type) {
  std::string out;
  out.reserve(user_key.size() + 8);
  out.append(user_key);
  put_fixed64(&out, pack_trailer(seq, type));
  return out;
}

inline std::string_view user_key_of(std::string_view internal_key) {
  return internal_key.size() < 8 ? std::string_view{}
                                 : internal_key.substr(0, internal_key.size() - 8);
}

inline std::uint64_t trailer_of(std::string_view internal_key) {
  return internal_key.size() < 8 ? 0
                                 : decode_fixed64(internal_key.data() + internal_key.size() - 8);
}

// Bytewise on the user key, then *descending* on the trailer so the newest
// version of a key sorts first. Returns <0, 0, >0.
int compare_internal(std::string_view a, std::string_view b);

// Bytewise. Used for user keys and for range bounds.
inline int compare_user(std::string_view a, std::string_view b) {
  const int cmp = a.compare(b);
  return cmp < 0 ? -1 : (cmp > 0 ? 1 : 0);
}

}  // namespace anvil::lsm

#endif  // ANVIL_CORE_LSM_FORMAT_H_
