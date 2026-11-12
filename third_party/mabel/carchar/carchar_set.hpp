#pragma once

#include <cstddef>
#include <stdexcept>
#include <vector>

#include "carchar_common.hpp"
#include "core/tracked_allocator.h"   // draken::TrackedAllocator — charged to the process memory account

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

    // Keys per block of the batched probe (mark_new): phase 1 issues this many
    // independent first-probe loads before phase 2 consumes any of them.
    static constexpr std::size_t kBatch = 32;

    // BATCHED insert-or-ignore over a key run. out_is_new[i] = 1 iff keys[i] was
    // absent (it is present afterwards); returns how many were inserted.
    //
    // One key at a time, every probe is a dependent chain — tag word, then stored
    // hash, then the hit/miss branch — and the core cannot start the next key's
    // misses until this one's resolve. Here a block of kBatch keys runs in two
    // phases. Phase 1 performs each key's FIRST probe step with no dependence on
    // any other key: the home group's first viable tag match (group_first_candidate)
    // and that candidate's stored hash. Those loads are all in flight together.
    // Phase 2 consumes them: a candidate hash equal to the key is a hit decided on
    // the spot — the slot was occupied when read and keys are never moved or
    // removed, so an insert earlier in the same block cannot stale it. Every other
    // key (no candidate, a tag false-positive, a full home group, a duplicate
    // inserted earlier in the block) takes the ordinary probe, now against lines
    // phase 1 brought in. The block's headroom is ensured before it starts, so no
    // resize can run between a key's phase 1 and its phase 2.
    //
    // No capacity is reserved for the whole run — the table keeps its doubling
    // policy, one block of headroom at a time (feedback_hashtable_sizing).
    std::size_t mark_new(const std::uint64_t* keys, std::uint8_t* out_is_new, std::size_t length) {
        if (keys == nullptr || out_is_new == nullptr || length == 0) {
            return 0;
        }
        std::size_t inserted = 0;
        std::uint64_t cand_hash[kBatch];
        std::uint8_t  has_cand[kBatch];
        static constexpr std::uint64_t kNoHash = 0;
        for (std::size_t base = 0; base < length; base += kBatch) {
            const std::size_t n = std::min(kBatch, length - base);
            while (size_ + n > resize_threshold_) {
                resize(capacity_ * 2U);
            }
            const std::uint8_t*  ctrl = control_.data();
            const std::uint64_t* hs   = hashes_.data();
            const std::size_t    mask = capacity_ - 1U;
            const std::uint64_t* run  = keys + base;
            // Phase 1: independent first-probe loads for every key in the block.
            for (std::size_t j = 0; j < n; ++j) {
                const std::uint64_t key = run[j];
                const std::size_t home = static_cast<std::size_t>(key & mask);
                const std::size_t index = detail::group_first_candidate(ctrl + home, key_tag(key));
                const bool has = index < detail::kProbeGroupWidth;
                // Branch-free: a key without a candidate reads the sentinel, never
                // an empty slot's (uninitialised) hash.
                const std::uint64_t* src = has ? hs + ((home + index) & mask) : &kNoHash;
                has_cand[j] = static_cast<std::uint8_t>(has);
                cand_hash[j] = *src;
            }
            // Phase 2: consume the loads; anything undecided takes the full probe.
            std::uint8_t* verdict = out_is_new + base;
            for (std::size_t j = 0; j < n; ++j) {
                const std::uint64_t key = run[j];
                if (has_cand[j] != 0U && cand_hash[j] == key) {
                    verdict[j] = 0U;
                    continue;
                }
                const bool fresh = insert_or_ignore_no_reserve(key);
                verdict[j] = static_cast<std::uint8_t>(fresh);
                inserted += static_cast<std::size_t>(fresh);
            }
        }
        return inserted;
    }

   private:
    struct FindResult {
        std::size_t slot = 0;
        bool found = false;
        std::size_t probes = 0;
    };

    void initialize_storage(std::size_t capacity) {
        capacity_ = capacity;
        // control_ IS initialized: kEmpty is the authority on slot occupancy.
        // hashes_ is deliberately left UNINITIALIZED (TrackedUninitAllocator
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
        if (result.probes > capacity_) {
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

        // Allocate the new storage BEFORE touching the table: if either allocation
        // throws, the table is exactly as it was. Nothing after this can throw.
        decltype(control_) old_control(new_capacity + (kGroupWidth - 1U), kEmpty);
        decltype(hashes_)  old_hashes(new_capacity);   // default-init: not filled
        old_control.swap(control_);
        old_hashes.swap(hashes_);                      // old_* now hold the old table
        const auto old_capacity = capacity_;
        capacity_ = new_capacity;
        size_ = 0;
        resize_threshold_ =
            static_cast<std::size_t>(static_cast<double>(capacity_) * load_factor_);

        for (std::size_t slot = 0; slot < old_capacity; ++slot) {
            if (old_control[slot] == kEmpty) {
                continue;
            }
            insert_at(find_empty_slot_for_resize(old_hashes[slot]), old_hashes[slot]);
        }
    }

    std::size_t capacity_ = 0;
    std::vector<std::uint8_t, draken::TrackedAllocator<std::uint8_t>> control_;
    // Uninitialized-allocator vector: allocated but never pre-filled — an
    // empty slot's hash is never read (see initialize_storage).
    std::vector<std::uint64_t, draken::TrackedUninitAllocator<std::uint64_t>> hashes_;
    std::size_t size_ = 0;
    std::size_t resize_threshold_ = 0;
    double load_factor_ = 0.80;
};

}  // namespace opteryx::carchar