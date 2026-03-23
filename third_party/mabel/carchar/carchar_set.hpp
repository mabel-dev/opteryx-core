#pragma once

#include <cassert>
#include <cstddef>
#include <stdexcept>
#include <vector>

#include "carchar_common.hpp"

namespace opteryx::carchar {

class CarcharSet {
   public:
    explicit CarcharSet(std::size_t initial_capacity = kMinCapacity, double load_factor = 0.80)
        : load_factor_(load_factor) {
        if (!(load_factor > 0.0 && load_factor < 1.0)) {
            throw std::invalid_argument("load_factor must be between 0 and 1");
        }
        const std::size_t capacity = std::max(kMinCapacity, next_power_of_two(initial_capacity));
        initialize_storage(capacity);
    }

    std::size_t size() const noexcept { return size_; }
    std::size_t capacity() const noexcept { return capacity_; }

    void reserve(std::size_t expected_entries) {
        if (expected_entries == 0) {
            return;
        }
        const double desired = static_cast<double>(expected_entries) / load_factor_;
        const auto target =
            std::max(kMinCapacity, next_power_of_two(static_cast<std::size_t>(desired + 0.999999)));
        if (target > capacity_) {
            resize(target);
        }
    }

    void tighten() {
        if (size_ == 0) {
            return;
        }
        const double desired = static_cast<double>(size_) / load_factor_;
        const std::size_t target =
            std::max(kMinCapacity, next_power_of_two(static_cast<std::size_t>(desired + 0.999999)));
        if (target < capacity_) {
            resize(target);
        }
    }

    bool contains(std::uint64_t key) const noexcept {
        return find_slot(key).found;
    }

    bool insert_or_ignore(std::uint64_t key) {
        ensure_insert_capacity();
        const auto result = find_slot(key);
        if (result.found) {
            return false;
        }
        insert_at(result.slot, key);
        return true;
    }

    // Bulk insert for COUNT DISTINCT style set-and-forget workloads.
    // Returns number of newly inserted keys.
    std::size_t insert_many(const std::uint64_t* keys, std::size_t length) {
        if (keys == nullptr || length == 0) {
            return 0;
        }
        reserve(size_ + length);
        const auto probe_finder = detail::select_probe_finder();
        const std::size_t capacity_mask = capacity_ - 1U;
        std::size_t inserted = 0;
        const std::size_t prime = std::min(kPrefetchAhead, length);
        for (std::size_t p = 0; p < prime; ++p) {
            prefetch_entry(keys[p] & capacity_mask);
        }
        for (std::size_t i = 0; i < length; ++i) {
            if (i + kPrefetchAhead < length) {
                prefetch_entry(keys[i + kPrefetchAhead] & capacity_mask);
            }
            if (insert_or_ignore_no_reserve(keys[i], probe_finder)) {
                ++inserted;
            }
        }
        return inserted;
    }

    // Bulk probe count for membership checks.
    std::size_t contains_many_count(const std::uint64_t* keys, std::size_t length) const {
        if (keys == nullptr || length == 0) {
            return 0;
        }
        const auto probe_finder = detail::select_probe_finder();
        std::size_t hits = 0;
        for (std::size_t i = 0; i < length; ++i) {
            if (find_slot(keys[i], probe_finder).found) {
                ++hits;
            }
        }
        return hits;
    }

    // Bulk insert for DISTINCT-style filtering; writes row indices of newly inserted keys
    // directly into out_indices (int32).  Caller must supply a buffer of at least `length`
    // int32 slots.  Returns the number of newly inserted keys written.
    //
    // Note: software prefetch (__builtin_prefetch) was evaluated here and showed neutral to
    // slightly negative results on Apple M-series (large L2 cache, strong hardware prefetcher).
    // The working sets tested (100k–500k rows, ~1–5 MB) fit within M-series L2, so the hardware
    // prefetcher handles the random access pattern without assistance.  The prefetch adds
    // instruction overhead without hiding latency on that platform.  Re-evaluate if this code
    // runs on x86 server hardware where tables routinely exceed L3 cache.
    std::size_t mark_new_indices_32(
        const std::uint64_t* keys,
        std::int32_t*        out_indices,
        std::size_t          length
    ) {
        if (keys == nullptr || out_indices == nullptr || length == 0) {
            return 0;
        }
        reserve(size_ + length);
        const auto probe_finder = detail::select_probe_finder();
        const std::size_t capacity_mask = capacity_ - 1U;
        std::size_t inserted = 0;
        const std::size_t prime = std::min(kPrefetchAhead, length);
        for (std::size_t p = 0; p < prime; ++p) {
            prefetch_entry(keys[p] & capacity_mask);
        }
        for (std::size_t i = 0; i < length; ++i) {
            if (i + kPrefetchAhead < length) {
                prefetch_entry(keys[i + kPrefetchAhead] & capacity_mask);
            }
            if (insert_or_ignore_no_reserve(keys[i], probe_finder)) {
                out_indices[inserted++] = static_cast<std::int32_t>(i);
            }
        }
        return inserted;
    }

    // Same as mark_new_indices_32 but writes int64 row indices.
    // Used for datasets with more than 2^31 rows.
    std::size_t mark_new_indices_64(
        const std::uint64_t* keys,
        std::int64_t*        out_indices,
        std::size_t          length
    ) {
        if (keys == nullptr || out_indices == nullptr || length == 0) {
            return 0;
        }
        reserve(size_ + length);
        const auto probe_finder = detail::select_probe_finder();
        const std::size_t capacity_mask = capacity_ - 1U;
        std::size_t inserted = 0;
        const std::size_t prime = std::min(kPrefetchAhead, length);
        for (std::size_t p = 0; p < prime; ++p) {
            prefetch_entry(keys[p] & capacity_mask);
        }
        for (std::size_t i = 0; i < length; ++i) {
            if (i + kPrefetchAhead < length) {
                prefetch_entry(keys[i + kPrefetchAhead] & capacity_mask);
            }
            if (insert_or_ignore_no_reserve(keys[i], probe_finder)) {
                out_indices[inserted++] = static_cast<std::int64_t>(i);
            }
        }
        return inserted;
    }

    // Bulk build for DISTINCT-style filtering.
    // Writes 1 into out_is_new[i] when keys[i] is new, else 0.
    // Returns total number of newly inserted keys.
    std::size_t mark_new(const std::uint64_t* keys, std::uint8_t* out_is_new, std::size_t length) {
        if (keys == nullptr || out_is_new == nullptr || length == 0) {
            return 0;
        }
        reserve(size_ + length);
        const auto probe_finder = detail::select_probe_finder();
        const std::size_t capacity_mask = capacity_ - 1U;
        std::size_t inserted = 0;
        const std::size_t prime = std::min(kPrefetchAhead, length);
        for (std::size_t p = 0; p < prime; ++p) {
            prefetch_entry(keys[p] & capacity_mask);
        }
        for (std::size_t i = 0; i < length; ++i) {
            if (i + kPrefetchAhead < length) {
                prefetch_entry(keys[i + kPrefetchAhead] & capacity_mask);
            }
            if (insert_or_ignore_no_reserve(keys[i], probe_finder)) {
                out_is_new[i] = 1U;
                ++inserted;
            } else {
                out_is_new[i] = 0U;
            }
        }
        return inserted;
    }

    std::size_t estimated_bytes() const noexcept { return capacity_ * (1U + 8U); }

   private:
    // Number of entries to prefetch ahead of the current insert position.
    // Sized to cover ~1 µs of DRAM latency at typical x86 insert throughput.
    static constexpr std::size_t kPrefetchAhead = 16;

    // Prefetch the control byte and hash slot for a given table slot into L2.
    // On x86-64 this emits PREFETCHT1 for both arrays, hiding DRAM latency on
    // large tables.  On all other architectures (ARM/Apple Silicon) the body is
    // empty; the compiler dead-code-eliminates all call sites and the surrounding
    // priming/rolling loops at -O2, leaving zero runtime overhead.
    inline void prefetch_entry([[maybe_unused]] std::size_t slot) const noexcept {
#if defined(__x86_64__) || defined(_M_X64)
        __builtin_prefetch(control_.data() + slot, 0, 1);
        __builtin_prefetch(hashes_.data()  + slot, 0, 1);
#endif
    }

    struct FindResult {
        std::size_t slot = 0;
        bool found = false;
        std::size_t probes = 0;
    };

    void initialize_storage(std::size_t capacity) {
        capacity_ = capacity;
        control_.assign(capacity_ + (kGroupWidth - 1U), kEmpty);
        hashes_.assign(capacity_, 0U);
        size_ = 0;
    }

    void ensure_insert_capacity() {
        if (size_ + 1 > static_cast<std::size_t>(static_cast<double>(capacity_) * load_factor_)) {
            resize(capacity_ * 2U);
        }
    }

    bool insert_or_ignore_no_reserve(std::uint64_t key, detail::ProbeFn probe_finder) {
        const auto result = find_slot(key, probe_finder);
        if (result.found) {
            return false;
        }
        insert_at(result.slot, key);
        return true;
    }

    void insert_at(std::size_t slot, std::uint64_t key) {
        const std::uint8_t tag = key_tag(key);
        control_[slot] = tag;
        if (slot < (kGroupWidth - 1U)) {
            control_[capacity_ + slot] = tag;
        }
        hashes_[slot] = key;
        ++size_;
    }

    std::size_t find_empty_slot_for_resize(std::uint64_t key) const noexcept {
        const std::size_t mask = capacity_ - 1U;
        std::size_t slot = static_cast<std::size_t>(key & mask);
        while (control_[slot] != kEmpty) {
            slot = (slot + 1U) & mask;
        }
        return slot;
    }

    FindResult find_slot(std::uint64_t key) const {
        return find_slot(key, detail::select_probe_finder());
    }

    FindResult find_slot(std::uint64_t key, detail::ProbeFn probe_finder) const noexcept {
        const std::uint8_t tag = key_tag(key);
        const auto result = probe_finder(control_.data(), hashes_.data(), capacity_, key, tag);
        assert(result.probes < capacity_ && "CarcharSet probe exhausted table capacity");
        return {result.slot, result.found, result.probes};
    }

    void resize(std::size_t new_capacity) {
        new_capacity = std::max(kMinCapacity, next_power_of_two(new_capacity));

        auto old_control = std::move(control_);
        auto old_hashes  = std::move(hashes_);
        const auto old_capacity = capacity_;

        initialize_storage(new_capacity);

        for (std::size_t slot = 0; slot < old_capacity; ++slot) {
            if (old_control[slot] == kEmpty) {
                continue;
            }
            insert_at(find_empty_slot_for_resize(old_hashes[slot]), old_hashes[slot]);
        }
    }

    std::size_t capacity_ = 0;
    std::vector<std::uint8_t> control_;
    std::vector<std::uint64_t> hashes_;
    std::size_t size_ = 0;
    double load_factor_ = 0.80;
};

}  // namespace opteryx::carchar