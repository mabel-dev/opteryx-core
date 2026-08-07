#pragma once

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
        std::size_t inserted = 0;
        for (std::size_t i = 0; i < length; ++i) {
            if (insert_or_ignore_no_reserve(keys[i])) {
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
        std::size_t hits = 0;
        for (std::size_t i = 0; i < length; ++i) {
            if (find_slot(keys[i]).found) {
                ++hits;
            }
        }
        return hits;
    }

    // Bulk insert for DISTINCT-style filtering; writes row indices of newly inserted keys
    // directly into out_indices (int32).  Caller must supply a buffer of at least `length`
    // int32 slots.  Returns the number of newly inserted keys written.
    std::size_t mark_new_indices_32(
        const std::uint64_t* keys,
        std::int32_t*        out_indices,
        std::size_t          length
    ) {
        if (keys == nullptr || out_indices == nullptr || length == 0) {
            return 0;
        }
        reserve(size_ + length);
        std::size_t inserted = 0;
        for (std::size_t i = 0; i < length; ++i) {
            if (insert_or_ignore_no_reserve(keys[i])) {
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
        std::size_t inserted = 0;
        for (std::size_t i = 0; i < length; ++i) {
            if (insert_or_ignore_no_reserve(keys[i])) {
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
        std::size_t inserted = 0;
        for (std::size_t i = 0; i < length; ++i) {
            if (insert_or_ignore_no_reserve(keys[i])) {
                out_is_new[i] = 1U;
                ++inserted;
            } else {
                out_is_new[i] = 0U;
            }
        }
        return inserted;
    }

    // Pure read-only probe for SEMI JOIN (IN subquery): returns indices of keys
    // that ARE present in the set.  Caller supplies out_indices with at least
    // `length` int32 slots.  Returns the count written.
    // const — never modifies the set; safe to call NoGIL with a shared build-side set.
    std::size_t probe_found_32(
        const std::uint64_t* keys,
        std::int32_t*        out_indices,
        std::size_t          length
    ) const noexcept {
        if (keys == nullptr || out_indices == nullptr || length == 0) {
            return 0;
        }
        std::size_t found = 0;
        for (std::size_t i = 0; i < length; ++i) {
            if (find_slot(keys[i]).found) {
                out_indices[found++] = static_cast<std::int32_t>(i);
            }
        }
        return found;
    }

    // Pure read-only probe for ANTI JOIN (NOT IN subquery): returns indices of
    // keys that are NOT present in the set.  Caller supplies out_indices with
    // at least `length` int32 slots.  Returns the count written.
    // const — never modifies the set; safe to call NoGIL with a shared build-side set.
    std::size_t probe_not_found_32(
        const std::uint64_t* keys,
        std::int32_t*        out_indices,
        std::size_t          length
    ) const noexcept {
        if (keys == nullptr || out_indices == nullptr || length == 0) {
            return 0;
        }
        std::size_t not_found = 0;
        for (std::size_t i = 0; i < length; ++i) {
            if (!find_slot(keys[i]).found) {
                out_indices[not_found++] = static_cast<std::int32_t>(i);
            }
        }
        return not_found;
    }

    std::size_t estimated_bytes() const noexcept { return capacity_ * (1U + 8U); }

   private:
    struct FindResult {
        std::size_t slot = 0;
        bool found = false;
        std::size_t probes = 0;
    };

    void initialize_storage(std::size_t capacity) {
        capacity_ = capacity;
        // control_ IS initialized: kEmpty is the authority on slot occupancy.
        // hashes_ is deliberately left UNINITIALIZED (uninitialized_allocator
        // default-init) — an empty slot's hash is never read: a tag is always
        // <= 0x7F while kEmpty is 0x80, so a probe can never confirm against an
        // empty slot (same proof as CarcharIndex). Pre-filling was a dead
        // memset of 8 bytes/slot on every alloc and every doubling.
        control_.assign(capacity_ + (kGroupWidth - 1U), kEmpty);
        hashes_.clear();
        hashes_.resize(capacity_);
        size_ = 0;
        // Integer threshold computed once per (re)size — the per-insert
        // capacity check is one integer compare (see CarcharIndex).
        resize_threshold_ =
            static_cast<std::size_t>(static_cast<double>(capacity_) * load_factor_);
    }

    void ensure_insert_capacity() {
        if (size_ >= resize_threshold_) {
            resize(capacity_ * 2U);
        }
    }

    bool insert_or_ignore_no_reserve(std::uint64_t key) {
        const auto result = find_slot(key);
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

    // Direct call — the ISA variant is a compile-time choice, so the tag
    // compare inlines into the caller's row loop instead of hiding behind a
    // function pointer.
    FindResult find_slot(std::uint64_t key) const {
        const std::uint8_t tag = key_tag(key);
        const auto result = detail::probe_find_slot_direct(
            control_.data(), hashes_.data(), capacity_, key, tag);
        if (result.probes >= capacity_) {
            // Unreachable while the load-factor invariant holds (an empty slot
            // always exists). Throw like CarcharIndex — the old release-mode
            // assert silently returned a fabricated slot. Callers reached
            // through noexcept boundaries terminate with this message instead.
            throw std::runtime_error("CarcharSet probe exhausted table capacity");
        }
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
    // Uninitialized-allocator vector: allocated but never pre-filled — an
    // empty slot's hash is never read (see initialize_storage).
    std::vector<std::uint64_t, detail::uninitialized_allocator<std::uint64_t>> hashes_;
    std::size_t size_ = 0;
    std::size_t resize_threshold_ = 0;
    double load_factor_ = 0.80;
};

}  // namespace opteryx::carchar