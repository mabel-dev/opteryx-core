#pragma once

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>

#include "carchar_simd.hpp"

namespace opteryx::carchar {

constexpr std::uint8_t kEmpty = 0x80;
constexpr std::size_t kMinCapacity = 16;
constexpr std::size_t kGroupWidth = detail::kProbeGroupWidth;
inline std::size_t next_power_of_two(std::size_t value) {
    return std::bit_ceil(std::max<std::size_t>(value, 1U));
}

inline std::uint8_t key_tag(std::uint64_t key) {
    return static_cast<std::uint8_t>((key >> 57U) & 0x7FU);
}

}  // namespace opteryx::carchar