#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <vector>

namespace opteryx::perfect_hash {

// Direct-addressed hash map for bounded integer keys (min_val to max_val inclusive).
//
// Similar to PerfectHashSet but stores int64_t payload values alongside keys.
// Useful for GROUP BY aggregates and join builds where the key domain is narrow
// and known in advance.
//
// All hot-path operations are noexcept. Null handling: callers must check
// null_bitmap and skip null rows before calling batch methods.
//
// Layout:
//   slot     = key - min_val              (always in [0, range))
//   valid    = valid_[slot >> 6]          (uint64_t bitmask)
//   mask     = 1ULL << (slot & 63)
//   is_set   = valid & mask
//   payload  = payloads_[slot]            (int64_t)

class PerfectHashMap {
   public:
    explicit PerfectHashMap(std::int64_t min_val, std::int64_t max_val) noexcept
        : min_val_(min_val), range_(max_val - min_val + 1) {
        const std::size_t n_words = (range_ + 63) / 64;
        valid_.resize(n_words, 0);
        payloads_.resize(static_cast<std::size_t>(range_), 0);
    }

    PerfectHashMap(const PerfectHashMap&) = delete;
    PerfectHashMap& operator=(const PerfectHashMap&) = delete;

    // Single-value operations.
    // insert: mark key as present, store payload. Returns true if newly inserted.
    bool insert_i64(std::int64_t key, std::int64_t payload) noexcept {
        const std::int64_t idx = key - min_val_;
        const std::size_t word_idx = static_cast<std::size_t>(idx >> 6);
        const std::uint64_t mask = 1ULL << (idx & 63);
        const bool is_new = !(valid_[word_idx] & mask);
        valid_[word_idx] |= mask;
        payloads_[static_cast<std::size_t>(idx)] = payload;
        return is_new;
    }

    // lookup: retrieve payload for key. Returns false if key not present.
    bool lookup_i64(std::int64_t key, std::int64_t& payload_out) const noexcept {
        const std::int64_t idx = key - min_val_;
        if (idx < 0 || idx >= range_) {
            return false;
        }
        const std::size_t word_idx = static_cast<std::size_t>(idx >> 6);
        const std::uint64_t mask = 1ULL << (idx & 63);
        const bool found = !!(valid_[word_idx] & mask);
        if (found) {
            payload_out = payloads_[static_cast<std::size_t>(idx)];
        }
        return found;
    }

    // Batch operations for int8_t keys.
    // find_or_insert: for each key, return payload (insert if new) and mark found.
    // Returns count of newly inserted entries.
    std::size_t find_or_insert_32_i8(
        const std::int8_t* keys,
        std::int64_t* payloads_in,
        std::int32_t* out_is_new,
        std::int64_t* out_payloads,
        std::size_t length
    ) noexcept {
        std::size_t count = 0;
        for (std::size_t i = 0; i < length; ++i) {
            const std::int64_t idx = static_cast<std::int64_t>(keys[i]) - min_val_;
            const std::size_t word_idx = static_cast<std::size_t>(idx >> 6);
            const std::uint64_t mask = 1ULL << (idx & 63);
            const bool is_new = !(valid_[word_idx] & mask);
            if (is_new) {
                valid_[word_idx] |= mask;
                payloads_[static_cast<std::size_t>(idx)] = payloads_in[i];
                out_is_new[count] = static_cast<std::int32_t>(i);
                count++;
            }
            out_payloads[i] = payloads_[static_cast<std::size_t>(idx)];
        }
        return count;
    }

    // probe_found: for each key, if present, write index to out_indices.
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
            if (valid_[word_idx] & mask) {
                out[count++] = static_cast<std::int32_t>(i);
            }
        }
        return count;
    }

    // probe_not_found: for each key, if absent, write index to out_indices.
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
            if (!(valid_[word_idx] & mask)) {
                out[count++] = static_cast<std::int32_t>(i);
            }
        }
        return count;
    }

    // Batch operations for int16_t keys.
    std::size_t find_or_insert_32_i16(
        const std::int16_t* keys,
        std::int64_t* payloads_in,
        std::int32_t* out_is_new,
        std::int64_t* out_payloads,
        std::size_t length
    ) noexcept {
        std::size_t count = 0;
        for (std::size_t i = 0; i < length; ++i) {
            const std::int64_t idx = static_cast<std::int64_t>(keys[i]) - min_val_;
            const std::size_t word_idx = static_cast<std::size_t>(idx >> 6);
            const std::uint64_t mask = 1ULL << (idx & 63);
            const bool is_new = !(valid_[word_idx] & mask);
            if (is_new) {
                valid_[word_idx] |= mask;
                payloads_[static_cast<std::size_t>(idx)] = payloads_in[i];
                out_is_new[count] = static_cast<std::int32_t>(i);
                count++;
            }
            out_payloads[i] = payloads_[static_cast<std::size_t>(idx)];
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
            if (valid_[word_idx] & mask) {
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
            if (!(valid_[word_idx] & mask)) {
                out[count++] = static_cast<std::int32_t>(i);
            }
        }
        return count;
    }

    // Batch operations for int32_t keys (Date32Vector physical storage).
    std::size_t find_or_insert_32_i32(
        const std::int32_t* keys,
        std::int64_t* payloads_in,
        std::int32_t* out_is_new,
        std::int64_t* out_payloads,
        std::size_t length
    ) noexcept {
        std::size_t count = 0;
        for (std::size_t i = 0; i < length; ++i) {
            const std::int64_t idx = static_cast<std::int64_t>(keys[i]) - min_val_;
            const std::size_t word_idx = static_cast<std::size_t>(idx >> 6);
            const std::uint64_t mask = 1ULL << (idx & 63);
            const bool is_new = !(valid_[word_idx] & mask);
            if (is_new) {
                valid_[word_idx] |= mask;
                payloads_[static_cast<std::size_t>(idx)] = payloads_in[i];
                out_is_new[count] = static_cast<std::int32_t>(i);
                count++;
            }
            out_payloads[i] = payloads_[static_cast<std::size_t>(idx)];
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
            if (valid_[word_idx] & mask) {
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
            if (!(valid_[word_idx] & mask)) {
                out[count++] = static_cast<std::int32_t>(i);
            }
        }
        return count;
    }

    // Batch operations for int64_t keys (IN-list literals, Timestamp, Time).
    // int64 keys require bounds checking since they can be out-of-range.
    std::size_t find_or_insert_32_i64(
        const std::int64_t* keys,
        std::int64_t* payloads_in,
        std::int32_t* out_is_new,
        std::int64_t* out_payloads,
        std::size_t length
    ) noexcept {
        std::size_t count = 0;
        for (std::size_t i = 0; i < length; ++i) {
            const std::int64_t idx = keys[i] - min_val_;
            if (idx < 0 || idx >= range_) {
                // Out-of-range key: treat as new, but don't store
                out_is_new[count] = static_cast<std::int32_t>(i);
                out_payloads[i] = 0;  // Sentinel value
                count++;
                continue;
            }
            const std::size_t word_idx = static_cast<std::size_t>(idx >> 6);
            const std::uint64_t mask = 1ULL << (idx & 63);
            const bool is_new = !(valid_[word_idx] & mask);
            if (is_new) {
                valid_[word_idx] |= mask;
                payloads_[static_cast<std::size_t>(idx)] = payloads_in[i];
                out_is_new[count] = static_cast<std::int32_t>(i);
                count++;
            }
            out_payloads[i] = payloads_[static_cast<std::size_t>(idx)];
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
                continue;  // out-of-range → not in map
            }
            const std::size_t word_idx = static_cast<std::size_t>(idx >> 6);
            const std::uint64_t mask = 1ULL << (idx & 63);
            if (valid_[word_idx] & mask) {
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
                out[count++] = static_cast<std::int32_t>(i);  // out-of-range → not in map
                continue;
            }
            const std::size_t word_idx = static_cast<std::size_t>(idx >> 6);
            const std::uint64_t mask = 1ULL << (idx & 63);
            if (!(valid_[word_idx] & mask)) {
                out[count++] = static_cast<std::int32_t>(i);
            }
        }
        return count;
    }

   private:
    std::int64_t min_val_;
    std::int64_t range_;
    std::vector<std::uint64_t> valid_;       // Bitmap: which slots are occupied
    std::vector<std::int64_t> payloads_;     // Payload for each slot
};

}  // namespace opteryx::perfect_hash
