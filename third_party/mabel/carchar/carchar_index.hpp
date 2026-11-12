#pragma once

#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>

#include "carchar_common.hpp"
#include "core/tracked_allocator.h"   // draken::TrackedAllocator — charged to the process memory account

namespace opteryx::carchar {

class CarcharIndex {
   public:
    explicit CarcharIndex(std::size_t initial_capacity = kMinCapacity, double load_factor = 0.80)
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

    // Forget every entry, keeping the capacity. A GROUP BY flush window re-uses its
    // table instead of allocating a fresh one (docs/GROUPBY_REDESIGN_2026_10_12.md R3).
    // Only control_ is reset: an empty slot's hash/payload are never read (see
    // initialize_storage), so slots_ keeps whatever it held.
    void clear() noexcept {
        std::fill(control_.begin(), control_.end(), kEmpty);
        size_ = 0;
    }

    bool lookup_fast(std::uint64_t key, std::int64_t& payload_ref_out) const {
        const auto result = find_slot(key);
        if (!result.found) {
            return false;
        }
        payload_ref_out = slots_[result.slot].payload;
        return true;
    }

    std::size_t insert_new(std::uint64_t key, std::int64_t payload_ref) {
        ensure_insert_capacity();
        const auto result = find_slot(key);
        if (result.found) {
            throw std::runtime_error("key already exists");
        }
        insert_at(result.slot, key, payload_ref);
        return result.slot;
    }

    template <typename PayloadFactory>
    std::pair<std::int64_t, bool> find_or_insert(std::uint64_t key, PayloadFactory&& payload_factory) {
        ensure_insert_capacity();
        const auto result = find_slot(key);
        if (result.found) {
            return {slots_[result.slot].payload, false};
        }
        const std::int64_t payload_ref = std::forward<PayloadFactory>(payload_factory)();
        insert_at(result.slot, key, payload_ref);
        return {payload_ref, true};
    }

    // Non-template variant for callers that already have the new payload value
    // (e.g. a monotonically-assigned slot id from num_groups++).  One probe for
    // both hit and miss.
    //
    // Returns true  → new_id was inserted (caller should treat row as a new group).
    // Returns false → existing payload was placed in payload_out (no new group).
    //
    // Hit-path is hot: no capacity check.
    // Miss path runs the capacity check and re-probes if a resize fired.
    bool find_or_insert_id(std::uint64_t key, std::int64_t new_id, std::int64_t& payload_out) {
        auto result = find_slot(key);
        if (result.found) {
            payload_out = slots_[result.slot].payload;
            return false;
        }
        // Miss: ensure capacity (may resize, invalidating the slot), then re-probe.
        if (size_ >= resize_threshold_) {
            resize(capacity_ * 2U);
            result = find_slot(key);
        }
        insert_at(result.slot, key, new_id);
        payload_out = new_id;
        return true;
    }

    std::vector<std::pair<std::uint64_t, std::int64_t>> items() const {
        std::vector<std::pair<std::uint64_t, std::int64_t>> out;
        out.reserve(size_);
        for (std::size_t i = 0; i < capacity_; ++i) {
            if (control_[i] != kEmpty) {
                out.emplace_back(slots_[i].hash, slots_[i].payload);
            }
        }
        return out;
    }

   private:
    struct FindResult {
        std::size_t slot = 0;
        bool found = false;
        std::size_t probes = 0;
    };

    void initialize_storage(std::size_t capacity) {
        capacity_ = capacity;
        // control_ IS initialized: kEmpty is the authority on slot occupancy and
        // every probe reads it. hashes_/payload_refs_ are deliberately left
        // UNINITIALIZED (make_unique_for_overwrite default-initializes) — an
        // empty slot's hash and payload are never read. Proof: key_tag() is
        // (key >> 57) & 0x7F, so a tag is always <= 0x7F while kEmpty is 0x80;
        // a tag can therefore never match an empty slot, so the probe never
        // confirms against hashes_[empty], and every payload read is guarded by
        // result.found (items()/resize() guard on control_ directly). Filling
        // them was a dead memset of 16 bytes/slot on every alloc AND every
        // doubling — GB-scale on high-cardinality GROUP BY, never read once.
        control_.assign(capacity_ + (kGroupWidth - 1U), kEmpty);
        slots_.clear();
        slots_.resize(capacity_);
        size_ = 0;
        // Integer threshold computed once per (re)size so the per-insert
        // capacity check is a single integer compare, not an int→double
        // convert + multiply + double compare on every inserted row.
        // size_ + 1 > size_t(capacity * lf)  ⇔  size_ >= size_t(capacity * lf).
        resize_threshold_ =
            static_cast<std::size_t>(static_cast<double>(capacity_) * load_factor_);
    }

    void ensure_insert_capacity() {
        if (size_ >= resize_threshold_) {
            resize(capacity_ * 2U);
        }
    }

    void insert_at(std::size_t slot, std::uint64_t key, std::int64_t payload_ref) {
        const std::uint8_t tag = key_tag(key);
        control_[slot] = tag;
        if (slot < (kGroupWidth - 1U)) {
            control_[capacity_ + slot] = tag;
        }
        slots_[slot].hash = key;
        slots_[slot].payload = payload_ref;
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

    // noexcept: exhaustion is REPORTED (probes > capacity_), not thrown, so the
    // hot probe path carries no unwind state. The throw moves to find_slot()
    // below, which every existing caller already used — behaviour unchanged.
    FindResult find_slot_nothrow(std::uint64_t key) const noexcept {
        const std::uint8_t tag = key_tag(key);
        const auto result = detail::probe_find_slot_direct<2>(
            control_.data(), reinterpret_cast<const std::uint64_t*>(slots_.data()),
            capacity_, key, tag);
        return {result.slot, result.found, result.probes};
    }

    FindResult find_slot(std::uint64_t key) const {
        const FindResult r = find_slot_nothrow(key);
        if (r.probes <= capacity_) {
            return r;
        }
        throw std::runtime_error("Carchar probe exhausted table capacity");
    }

    void resize(std::size_t new_capacity) {
        new_capacity = std::max(kMinCapacity, next_power_of_two(new_capacity));

        // Allocate the new storage BEFORE touching the table: if either allocation
        // throws, the table is exactly as it was. Nothing after this can throw.
        decltype(control_) old_control(new_capacity + (kGroupWidth - 1U), kEmpty);
        decltype(slots_)   old_slots(new_capacity);   // default-init: not filled
        old_control.swap(control_);
        old_slots.swap(slots_);                       // old_* now hold the old table
        const auto old_capacity = capacity_;
        capacity_ = new_capacity;
        size_ = 0;
        resize_threshold_ =
            static_cast<std::size_t>(static_cast<double>(capacity_) * load_factor_);

        for (std::size_t slot = 0; slot < old_capacity; ++slot) {
            if (old_control[slot] == kEmpty) {
                continue;
            }
            insert_at(find_empty_slot_for_resize(old_slots[slot].hash),
                      old_slots[slot].hash, old_slots[slot].payload);
        }
    }

    std::size_t capacity_ = 0;
    std::vector<std::uint8_t, draken::TrackedAllocator<std::uint8_t>> control_;
    // Uninitialized-allocator vectors: allocated but never pre-filled, because
    // an empty slot's hash/payload are never read (see initialize_storage).
    // H16 (2026-08-14): hashes_ and payload_refs_ were SEPARATE allocations, so a
    // probe hit cost two random accesses at the same index into two arrays (plus
    // control_) — three dependent misses across ~544 MB at 17.6M groups. Merged
    // into one 16-byte slot so the confirm (hash) and the fetch (payload) land on
    // the SAME cache line: one random access instead of two. This is what DuckDB
    // (salt packed into the pointer word) and ClickHouse (key+mapped in one cell)
    // both do. The SIMD probe reads it as a uint64 array with HStride = 2.
    struct Slot {
        std::uint64_t hash;
        std::int64_t  payload;
    };
    static_assert(sizeof(Slot) == 16, "Slot must be exactly two 64-bit words");
    static_assert(alignof(Slot) >= 8, "Slot must be 8-byte aligned for the probe");
    std::vector<Slot, draken::TrackedUninitAllocator<Slot>> slots_;
    std::size_t size_ = 0;
    std::size_t resize_threshold_ = 0;
    double load_factor_ = 0.80;
};

}  // namespace opteryx::carchar