#include "anvil/core/lsm/format.h"

#include <array>

namespace anvil::lsm {
namespace {

// Castagnoli polynomial, reflected.
constexpr std::uint32_t kPoly = 0x82F63B78u;

std::array<std::uint32_t, 256> make_table() {
  std::array<std::uint32_t, 256> table{};
  for (std::uint32_t i = 0; i < 256; ++i) {
    std::uint32_t crc = i;
    for (int bit = 0; bit < 8; ++bit) {
      crc = (crc & 1) ? ((crc >> 1) ^ kPoly) : (crc >> 1);
    }
    table[i] = crc;
  }
  return table;
}

const std::array<std::uint32_t, 256>& table() {
  static const std::array<std::uint32_t, 256> kTable = make_table();
  return kTable;
}

}  // namespace

std::uint32_t crc32c_extend(std::uint32_t crc, std::string_view data) {
  const auto& t = table();
  std::uint32_t value = crc ^ 0xFFFFFFFFu;
  for (const char c : data) {
    value = t[(value ^ static_cast<unsigned char>(c)) & 0xFF] ^ (value >> 8);
  }
  return value ^ 0xFFFFFFFFu;
}

std::uint32_t crc32c(std::string_view data) { return crc32c_extend(0, data); }

int compare_internal(std::string_view a, std::string_view b) {
  const int cmp = compare_user(user_key_of(a), user_key_of(b));
  if (cmp != 0) return cmp;

  // Same user key: the newer version (higher sequence) sorts FIRST, so the
  // trailer comparison is inverted. A seek to (key, snapshot) then lands on the
  // newest visible version in one step instead of having to scan forward.
  const std::uint64_t ta = trailer_of(a);
  const std::uint64_t tb = trailer_of(b);
  if (ta > tb) return -1;
  if (ta < tb) return 1;
  return 0;
}

}  // namespace anvil::lsm
