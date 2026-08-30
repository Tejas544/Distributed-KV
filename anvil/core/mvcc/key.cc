#include "anvil/core/mvcc/key.h"

namespace anvil::mvcc {
namespace {

void append_be64(std::string* out, std::uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    out->push_back(static_cast<char>((value >> shift) & 0xFF));
  }
}

std::uint64_t read_be64(const char* p) {
  std::uint64_t value = 0;
  for (int i = 0; i < 8; ++i) {
    value = (value << 8) | static_cast<unsigned char>(p[i]);
  }
  return value;
}

}  // namespace

void append_escaped(std::string* out, std::string_view key) {
  for (const char c : key) {
    out->push_back(c);
    if (c == '\0') out->push_back('\xFF');
  }
  out->push_back('\0');
  out->push_back('\0');
}

const char* decode_escaped(const char* p, const char* limit, std::string* out) {
  out->clear();
  while (p < limit) {
    const char c = *p++;
    if (c != '\0') {
      out->push_back(c);
      continue;
    }
    if (p >= limit) return nullptr;
    const char next = *p++;
    if (next == '\0') return p;                 // terminator
    if (next != '\xFF') return nullptr;         // malformed
    out->push_back('\0');                       // an escaped NUL
  }
  return nullptr;
}

std::string data_prefix(std::string_view user_key) {
  std::string out;
  out.reserve(user_key.size() + 4);
  out.push_back(kDataPrefix);
  append_escaped(&out, user_key);
  return out;
}

std::string lock_key(std::string_view user_key) {
  std::string out;
  out.reserve(user_key.size() + 4);
  out.push_back(kLockPrefix);
  append_escaped(&out, user_key);
  return out;
}

std::string encode_data_key(std::string_view user_key, CommitTs commit_ts) {
  std::string out = data_prefix(user_key);
  append_be64(&out, ~commit_ts);
  return out;
}

std::string seek_for_read(std::string_view user_key, CommitTs read_ts) {
  // Identical to encode_data_key. Named separately because the two are
  // conceptually different operations and conflating them is how a reader ends
  // up seeking to a timestamp it is not allowed to see.
  return encode_data_key(user_key, read_ts);
}

std::string data_upper_bound(std::string_view user_key) {
  // The prefix ends with the two-byte terminator 0x00 0x00, so incrementing the
  // last byte gives 0x00 0x01 -- greater than every encoded version of this key
  // and less than every encoded key that shares the prefix and continues.
  std::string out = data_prefix(user_key);
  out.back() = '\x01';
  return out;
}

bool decode_data_key(std::string_view encoded, std::string* user_key, CommitTs* commit_ts) {
  if (encoded.size() < 1 + 2 + 8 || encoded.front() != kDataPrefix) return false;
  const char* p = encoded.data() + 1;
  const char* limit = encoded.data() + encoded.size();
  p = decode_escaped(p, limit, user_key);
  if (p == nullptr || limit - p != 8) return false;
  *commit_ts = ~read_be64(p);
  return true;
}

bool decode_lock_key(std::string_view encoded, std::string* user_key) {
  if (encoded.size() < 1 + 2 || encoded.front() != kLockPrefix) return false;
  const char* p = encoded.data() + 1;
  const char* limit = encoded.data() + encoded.size();
  return decode_escaped(p, limit, user_key) == limit;
}

}  // namespace anvil::mvcc
