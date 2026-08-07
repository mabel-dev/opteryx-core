#pragma once

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>

#include "carchar_simd.hpp"

namespace opteryx::carchar {

namespace detail {
// Allocator adaptor that DEFAULT-initializes elements instead of
// value-initializing them. For trivial types that means "allocate and leave
// the bytes alone" — std::vector otherwise has no way to allocate without
// filling. Every other vector behaviour (growth, copy, move, data()) is
// unchanged, which matters because CarcharIndex must stay copyable
// (CarcharJoinIndex holds one by value and is copied into vectors).
// Shared by CarcharIndex and CarcharSet: in both, an empty slot's hash is
// never read (a tag is <= 0x7F and kEmpty is 0x80, so a probe can never
// confirm against an empty slot's hash), so pre-filling the hash array is a
// dead memset on every allocation and every doubling.
template <typename T>
struct uninitialized_allocator : std::allocator<T> {
    using std::allocator<T>::allocator;

    template <typename U>
    struct rebind {
        using other = uninitialized_allocator<U>;
    };

    template <typename U>
    void construct(U* ptr) noexcept(std::is_nothrow_default_constructible_v<U>) {
        ::new (static_cast<void*>(ptr)) U;   // default-init: no fill for trivial U
    }

    template <typename U, typename... Args>
    void construct(U* ptr, Args&&... args) {
        std::allocator_traits<std::allocator<T>>::construct(
            static_cast<std::allocator<T>&>(*this), ptr, std::forward<Args>(args)...);
    }
};
}  // namespace detail

constexpr std::uint8_t kEmpty = 0x80;
constexpr std::size_t kMinCapacity = 16;
constexpr std::size_t kGroupWidth = detail::kProbeGroupWidth;
inline std::size_t next_power_of_two(std::size_t value) {
    return std::bit_ceil(std::max<std::size_t>(value, 1U));
}

inline std::uint8_t key_tag(std::uint64_t key) {
    return static_cast<std::uint8_t>((key >> 57U) & 0x7FU);
}

struct CarcharStats {
    std::size_t capacity = 0;
    std::size_t size = 0;
    std::size_t resize_count = 0;
    std::size_t lookup_count = 0;
    std::size_t insert_count = 0;
    std::size_t total_probes = 0;
    std::size_t max_probe_length = 0;
    std::size_t lookup_total_probes = 0;
    std::size_t insert_total_probes = 0;
    std::size_t max_lookup_probe_length = 0;
    std::size_t max_insert_probe_length = 0;
    std::size_t bytes_estimate = 0;

    double load_factor() const {
        if (capacity == 0) {
            return 0.0;
        }
        return static_cast<double>(size) / static_cast<double>(capacity);
    }

    double average_probe_length() const {
        const std::size_t operations = lookup_count + insert_count;
        if (operations == 0) {
            return 0.0;
        }
        return static_cast<double>(total_probes) / static_cast<double>(operations);
    }

    double average_lookup_probe_length() const {
        if (lookup_count == 0) {
            return 0.0;
        }
        return static_cast<double>(lookup_total_probes) / static_cast<double>(lookup_count);
    }

    double average_insert_probe_length() const {
        if (insert_count == 0) {
            return 0.0;
        }
        return static_cast<double>(insert_total_probes) / static_cast<double>(insert_count);
    }
};

}  // namespace opteryx::carchar