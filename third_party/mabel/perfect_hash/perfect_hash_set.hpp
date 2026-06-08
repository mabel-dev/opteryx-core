#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <vector>

namespace opteryx::perfect_hash {

// Direct-addressed bit-set for bounded integer keys (min_val to max_val inclusive).
//
// All hot-path operations are noexcept. Null handling: callers must check
// null_bitmap and skip null rows before calling batch methods; this class has
// no null slot.
//
// Bit-array layout:
//   slot  = key - min_val              (always in [0, range))
//   word  = words_[slot >> 6]          (uint64_t)
//   mask  = 1ULL << (slot & 63)
//   test  = word & mask
//   set   = word |= mask

class PerfectHashSet {
   public:
    explicit PerfectHashSet(std::int64_t min_val, std::int64_t max_val) noexcept
        : min_val_(min_val), range_(max_val - min_val + 1) {
        const std::size_t n_words = (range_ + 63) / 64;
        words_.resize(n_words, 0);
    }

    PerfectHashSet(const PerfectHashSet&) = delete;
    PerfectHashSet& operator=(const PerfectHashSet&) = delete;

    // Single-value operations.
    bool insert_i64(std::int64_t val) noexcept {
        const std::int64_t idx = val - min_val_;
        const std::size_t word_idx = static_cast<std::size_t>(idx >> 6);
        const std::uint64_t mask = 1ULL << (idx & 63);
        const bool is_new = !(words_[word_idx] & mask);
        words_[word_idx] |= mask;
        return is_new;
    }

    bool contains_i64(std::int64_t val) const noexcept {
        const std::int64_t idx = val - min_val_;
        if (idx < 0 || idx >= range_) {
            return false;
        }
        const std::size_t word_idx = static_cast<std::size_t>(idx >> 6);
        const std::uint64_t mask = 1ULL << (idx & 63);
        return !!(words_[word_idx] & mask);
    }

    // Batch operations for int8_t keys.
    std::size_t find_new_indices_out_32_i8(
        const std::int8_t* keys,
        std::int32_t* out,
        std::size_t length
    ) noexcept {
        std::size_t count = 0;
        for (std::size_t i = 0; i < length; ++i) {
            const std::int64_t idx = static_cast<std::int64_t>(keys[i]) - min_val_;
            const std::size_t word_idx = static_cast<std::size_t>(idx >> 6);
            const std::uint64_t mask = 1ULL << (idx & 63);
            if (!(words_[word_idx] & mask)) {
                words_[word_idx] |= mask;
                out[count++] = static_cast<std::int32_t>(i);
            }
        }
        return count;
    }

    std::size_t probe_found_32_i8(
        const std::int8_t* keys,
        std::int32_t* out,
        std::size_t length
    ) const noexcept {
        std::size_t count = 0;
        for (std::size_t i = 0; i < length; ++i) {
            const std::int64_t idx = static_cast<std::int64_t>(keys[i]) - min_val_;
            const std::size_t word_idx = static_cast<std::size_t>(idx >> 6);
            const std::uint64_t mask = 1ULL << (idx & 63);
            if (words_[word_idx] & mask) {
                out[count++] = static_cast<std::int32_t>(i);
            }
        }
        return count;
    }

    std::size_t probe_not_found_32_i8(
        const std::int8_t* keys,
        std::int32_t* out,
        std::size_t length
    ) const noexcept {
        std::size_t count = 0;
        for (std::size_t i = 0; i < length; ++i) {
            const std::int64_t idx = static_cast<std::int64_t>(keys[i]) - min_val_;
            const std::size_t word_idx = static_cast<std::size_t>(idx >> 6);
            const std::uint64_t mask = 1ULL << (idx & 63);
            if (!(words_[word_idx] & mask)) {
                out[count++] = static_cast<std::int32_t>(i);
            }
        }
        return count;
    }

    // Batch operations for int16_t keys.
    std::size_t find_new_indices_out_32_i16(
        const std::int16_t* keys,
        std::int32_t* out,
        std::size_t length
    ) noexcept {
        std::size_t count = 0;
        for (std::size_t i = 0; i < length; ++i) {
            const std::int64_t idx = static_cast<std::int64_t>(keys[i]) - min_val_;
            const std::size_t word_idx = static_cast<std::size_t>(idx >> 6);
            const std::uint64_t mask = 1ULL << (idx & 63);
            if (!(words_[word_idx] & mask)) {
                words_[word_idx] |= mask;
                out[count++] = static_cast<std::int32_t>(i);
            }
        }
        return count;
    }

    std::size_t probe_found_32_i16(
        const std::int16_t* keys,
        std::int32_t* out,
        std::size_t length
    ) const noexcept {
        std::size_t count = 0;
        for (std::size_t i = 0; i < length; ++i) {
            const std::int64_t idx = static_cast<std::int64_t>(keys[i]) - min_val_;
            const std::size_t word_idx = static_cast<std::size_t>(idx >> 6);
            const std::uint64_t mask = 1ULL << (idx & 63);
            if (words_[word_idx] & mask) {
                out[count++] = static_cast<std::int32_t>(i);
            }
        }
        return count;
    }

    std::size_t probe_not_found_32_i16(
        const std::int16_t* keys,
        std::int32_t* out,
        std::size_t length
    ) const noexcept {
        std::size_t count = 0;
        for (std::size_t i = 0; i < length; ++i) {
            const std::int64_t idx = static_cast<std::int64_t>(keys[i]) - min_val_;
            const std::size_t word_idx = static_cast<std::size_t>(idx >> 6);
            const std::uint64_t mask = 1ULL << (idx & 63);
            if (!(words_[word_idx] & mask)) {
                out[count++] = static_cast<std::int32_t>(i);
            }
        }
        return count;
    }

    // Batch operations for int32_t keys (Date32Vector physical storage).
    std::size_t find_new_indices_out_32_i32(
        const std::int32_t* keys,
        std::int32_t* out,
        std::size_t length
    ) noexcept {
        std::size_t count = 0;
        for (std::size_t i = 0; i < length; ++i) {
            const std::int64_t idx = static_cast<std::int64_t>(keys[i]) - min_val_;
            const std::size_t word_idx = static_cast<std::size_t>(idx >> 6);
            const std::uint64_t mask = 1ULL << (idx & 63);
            if (!(words_[word_idx] & mask)) {
                words_[word_idx] |= mask;
                out[count++] = static_cast<std::int32_t>(i);
            }
        }
        return count;
    }

    std::size_t probe_found_32_i32(
        const std::int32_t* keys,
        std::int32_t* out,
        std::size_t length
    ) const noexcept {
        std::size_t count = 0;
        for (std::size_t i = 0; i < length; ++i) {
            const std::int64_t idx = static_cast<std::int64_t>(keys[i]) - min_val_;
            const std::size_t word_idx = static_cast<std::size_t>(idx >> 6);
            const std::uint64_t mask = 1ULL << (idx & 63);
            if (words_[word_idx] & mask) {
                out[count++] = static_cast<std::int32_t>(i);
            }
        }
        return count;
    }

    std::size_t probe_not_found_32_i32(
        const std::int32_t* keys,
        std::int32_t* out,
        std::size_t length
    ) const noexcept {
        std::size_t count = 0;
        for (std::size_t i = 0; i < length; ++i) {
            const std::int64_t idx = static_cast<std::int64_t>(keys[i]) - min_val_;
            const std::size_t word_idx = static_cast<std::size_t>(idx >> 6);
            const std::uint64_t mask = 1ULL << (idx & 63);
            if (!(words_[word_idx] & mask)) {
                out[count++] = static_cast<std::int32_t>(i);
            }
        }
        return count;
    }

    // Batch operations for int64_t keys (IN-list literals, Timestamp, Time).
    // int64 keys require bounds checking since they can be out-of-range.
    std::size_t find_new_indices_out_32_i64(
        const std::int64_t* keys,
        std::int32_t* out,
        std::size_t length
    ) noexcept {
        std::size_t count = 0;
        for (std::size_t i = 0; i < length; ++i) {
            const std::int64_t idx = keys[i] - min_val_;
            if (idx < 0 || idx >= range_) {
                out[count++] = static_cast<std::int32_t>(i);  // out-of-range → not yet seen
                continue;
            }
            const std::size_t word_idx = static_cast<std::size_t>(idx >> 6);
            const std::uint64_t mask = 1ULL << (idx & 63);
            if (!(words_[word_idx] & mask)) {
                words_[word_idx] |= mask;
                out[count++] = static_cast<std::int32_t>(i);
            }
        }
        return count;
    }

    std::size_t probe_found_32_i64(
        const std::int64_t* keys,
        std::int32_t* out,
        std::size_t length
    ) const noexcept {
        std::size_t count = 0;
        for (std::size_t i = 0; i < length; ++i) {
            const std::int64_t idx = keys[i] - min_val_;
            if (idx < 0 || idx >= range_) {
                continue;  // out-of-range → not in set
            }
            const std::size_t word_idx = static_cast<std::size_t>(idx >> 6);
            const std::uint64_t mask = 1ULL << (idx & 63);
            if (words_[word_idx] & mask) {
                out[count++] = static_cast<std::int32_t>(i);
            }
        }
        return count;
    }

    std::size_t probe_not_found_32_i64(
        const std::int64_t* keys,
        std::int32_t* out,
        std::size_t length
    ) const noexcept {
        std::size_t count = 0;
        for (std::size_t i = 0; i < length; ++i) {
            const std::int64_t idx = keys[i] - min_val_;
            if (idx < 0 || idx >= range_) {
                out[count++] = static_cast<std::int32_t>(i);  // out-of-range → not in set
                continue;
            }
            const std::size_t word_idx = static_cast<std::size_t>(idx >> 6);
            const std::uint64_t mask = 1ULL << (idx & 63);
            if (!(words_[word_idx] & mask)) {
                out[count++] = static_cast<std::int32_t>(i);
            }
        }
        return count;
    }

   private:
    std::int64_t min_val_;
    std::int64_t range_;
    std::vector<std::uint64_t> words_;
};

}  // namespace opteryx::perfect_hash
