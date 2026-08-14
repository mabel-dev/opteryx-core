#pragma once

#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>

#include "carchar_common.hpp"

// Hot-path probe/insert statistics (probe lengths, lookup/insert counts).
// Diagnostic-only — no production consumer reads them — but they cost two adds
// and two compare-branch max-updates on every insert and stats-lookup.
// Compiled out by default; build with -DCARCHAR_HOT_STATS=1 to re-enable.
#ifndef CARCHAR_HOT_STATS
#define CARCHAR_HOT_STATS 0
#endif

namespace opteryx::carchar {

// detail::uninitialized_allocator lives in carchar_common.hpp (shared with
// CarcharSet, which has the same never-read-empty-slot proof).

class CarcharIndex {
    static constexpr bool kHotStats = CARCHAR_HOT_STATS != 0;

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
    std::size_t probe_group_count() const noexcept { return capacity_ / kGroupWidth; }
    std::size_t probe_group_index(std::uint64_t key) const noexcept {
        if (capacity_ == 0) {
            return 0;
        }
        return static_cast<std::size_t>((key & (capacity_ - 1U)) / kGroupWidth);
    }

    // ⛔ DEAD — kept only so the measurement is not repeated a third time.
    // Compute-path software prefetch is banned (architect, 2026-07-02). The call
    // site in native_group_sinks.hpp pass B was removed then, on Apple Silicon
    // measurements. Re-tested on x86 2026-08-14, prefetching this line 8/16 rows
    // ahead of the dependent probe (and in a variant the hashes_ line too):
    // 8% SLOWER on a cache-resident 9k-group table, flat at 6M, 2.8% (ranges
    // barely separating) at 17.6M, noise at 18.3M. Net negative on both
    // architectures. If the probe is slow, restructure the passes.
    void prefetch(std::uint64_t key) const noexcept {
        if (capacity_ == 0) {
            return;
        }
        const std::size_t slot = static_cast<std::size_t>(key & (capacity_ - 1U));
        __builtin_prefetch(&control_[slot], 0 /*read*/, 1 /*low temporal locality*/);
    }

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
        tighten(load_factor_);
    }

    void tighten(double target_load_factor) {
        if (size_ == 0) {
            return;
        }
        if (!(target_load_factor > 0.0 && target_load_factor < 1.0)) {
            throw std::invalid_argument("target_load_factor must be between 0 and 1");
        }
        const double desired = static_cast<double>(size_) / target_load_factor;
        const std::size_t target =
            std::max(kMinCapacity, next_power_of_two(static_cast<std::size_t>(desired + 0.999999)));
        if (target < capacity_) {
            resize(target);
        }
    }

    bool lookup(std::uint64_t key, std::int64_t& payload_ref_out) {
        const auto result = find_slot(key);
        if constexpr (kHotStats) {
            ++lookup_count_;
            record_lookup_probe_length(result.probes);
        }
        if (!result.found) {
            return false;
        }
        payload_ref_out = slots_[result.slot].payload;
        return true;
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
        if constexpr (kHotStats) {
            ++insert_count_;
            record_insert_probe_length(result.probes);
        }
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
        if constexpr (kHotStats) {
            ++insert_count_;
            record_insert_probe_length(result.probes);
        }
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
    // Hit-path is hot: no floating-point capacity check, no insert-stat update.
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
        if constexpr (kHotStats) {
            ++insert_count_;
            record_insert_probe_length(result.probes);
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

    CarcharStats stats() const {
        CarcharStats stats;
        stats.capacity = capacity_;
        stats.size = size_;
        stats.resize_count = resize_count_;
        stats.lookup_count = lookup_count_;
        stats.insert_count = insert_count_;
        stats.total_probes = total_probes_;
        stats.max_probe_length = max_probe_length_;
        stats.lookup_total_probes = lookup_total_probes_;
        stats.insert_total_probes = insert_total_probes_;
        stats.max_lookup_probe_length = max_lookup_probe_length_;
        stats.max_insert_probe_length = max_insert_probe_length_;
        stats.bytes_estimate = estimated_bytes();
        return stats;
    }

   protected:
    std::size_t estimated_bytes() const noexcept { return capacity_ * (1U + sizeof(Slot)); }

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

    void record_lookup_probe_length(std::size_t probes) {
        lookup_total_probes_ += probes;
        if (probes > max_lookup_probe_length_) {
            max_lookup_probe_length_ = probes;
        }
        record_probe_length(probes);
    }

    void record_insert_probe_length(std::size_t probes) {
        insert_total_probes_ += probes;
        if (probes > max_insert_probe_length_) {
            max_insert_probe_length_ = probes;
        }
        record_probe_length(probes);
    }

    void record_probe_length(std::size_t probes) {
        total_probes_ += probes;
        if (probes > max_probe_length_) {
            max_probe_length_ = probes;
        }
    }

    std::size_t find_empty_slot_for_resize(std::uint64_t key) const noexcept {
        const std::size_t mask = capacity_ - 1U;
        std::size_t slot = static_cast<std::size_t>(key & mask);
        while (control_[slot] != kEmpty) {
            slot = (slot + 1U) & mask;
        }
        return slot;
    }

    // noexcept: exhaustion is REPORTED (probes == capacity_), not thrown, so the
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
        if (r.probes < capacity_) {
            return r;
        }
        throw std::runtime_error("Carchar probe exhausted table capacity");
    }

    void resize(std::size_t new_capacity) {
        new_capacity = std::max(kMinCapacity, next_power_of_two(new_capacity));

        auto old_control      = std::move(control_);
        auto old_slots        = std::move(slots_);
        const auto old_capacity = capacity_;

        initialize_storage(new_capacity);
        ++resize_count_;

        for (std::size_t slot = 0; slot < old_capacity; ++slot) {
            if (old_control[slot] == kEmpty) {
                continue;
            }
            insert_at(find_empty_slot_for_resize(old_slots[slot].hash),
                      old_slots[slot].hash, old_slots[slot].payload);
        }
    }

    std::size_t capacity_ = 0;
    std::vector<std::uint8_t> control_;
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
    std::vector<Slot, detail::uninitialized_allocator<Slot>> slots_;
    std::size_t size_ = 0;
    std::size_t resize_threshold_ = 0;
    double load_factor_ = 0.80;

    std::size_t resize_count_ = 0;
    std::size_t lookup_count_ = 0;
    std::size_t insert_count_ = 0;
    std::size_t total_probes_ = 0;
    std::size_t max_probe_length_ = 0;
    std::size_t lookup_total_probes_ = 0;
    std::size_t insert_total_probes_ = 0;
    std::size_t max_lookup_probe_length_ = 0;
    std::size_t max_insert_probe_length_ = 0;
};

}  // namespace opteryx::carchar