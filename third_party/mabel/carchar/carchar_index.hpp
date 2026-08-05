#pragma once

#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>

#include "carchar_common.hpp"

namespace opteryx::carchar {

namespace detail {
// Allocator adaptor that DEFAULT-initializes elements instead of
// value-initializing them. For trivial types that means "allocate and leave
// the bytes alone" — std::vector otherwise has no way to allocate without
// filling. Every other vector behaviour (growth, copy, move, data()) is
// unchanged, which matters because CarcharIndex must stay copyable
// (CarcharJoinIndex holds one by value and is copied into vectors).
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
    std::size_t probe_group_count() const noexcept { return capacity_ / kGroupWidth; }
    std::size_t probe_group_index(std::uint64_t key) const noexcept {
        if (capacity_ == 0) {
            return 0;
        }
        return static_cast<std::size_t>((key & (capacity_ - 1U)) / kGroupWidth);
    }

    // Prefetch the control-array cache line a probe for `key` would touch first.
    // Callers that know future keys (precomputed morsel hashes) issue the miss
    // ahead of the dependent find_or_insert_id, raising memory-level parallelism
    // on the latency-bound probe. Semantically a no-op. Helps high-cardinality
    // (all-miss) probes ~10%; negligible overhead when the table is cache-resident.
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
        ++lookup_count_;
        record_lookup_probe_length(result.probes);
        if (!result.found) {
            return false;
        }
        payload_ref_out = payload_refs_[result.slot];
        return true;
    }

    bool lookup_fast(std::uint64_t key, std::int64_t& payload_ref_out) const {
        const auto result = find_slot(key);
        if (!result.found) {
            return false;
        }
        payload_ref_out = payload_refs_[result.slot];
        return true;
    }

    std::size_t insert_new(std::uint64_t key, std::int64_t payload_ref) {
        ensure_insert_capacity();
        const auto result = find_slot(key);
        ++insert_count_;
        record_insert_probe_length(result.probes);
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
        ++insert_count_;
        record_insert_probe_length(result.probes);
        if (result.found) {
            return {payload_refs_[result.slot], false};
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
            payload_out = payload_refs_[result.slot];
            return false;
        }
        // Miss: ensure capacity (may resize, invalidating the slot), then re-probe.
        if (size_ + 1 > static_cast<std::size_t>(static_cast<double>(capacity_) * load_factor_)) {
            resize(capacity_ * 2U);
            result = find_slot(key);
        }
        ++insert_count_;
        record_insert_probe_length(result.probes);
        insert_at(result.slot, key, new_id);
        payload_out = new_id;
        return true;
    }

    std::vector<std::pair<std::uint64_t, std::int64_t>> items() const {
        std::vector<std::pair<std::uint64_t, std::int64_t>> out;
        out.reserve(size_);
        for (std::size_t i = 0; i < capacity_; ++i) {
            if (control_[i] != kEmpty) {
                out.emplace_back(hashes_[i], payload_refs_[i]);
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
    std::size_t estimated_bytes() const noexcept { return capacity_ * (1U + 8U + 8U); }

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
        hashes_.clear();
        hashes_.resize(capacity_);
        payload_refs_.clear();
        payload_refs_.resize(capacity_);
        size_ = 0;
        probe_finder_ = detail::select_probe_finder();
    }

    void ensure_insert_capacity() {
        if (size_ + 1 > static_cast<std::size_t>(static_cast<double>(capacity_) * load_factor_)) {
            resize(capacity_ * 2U);
        }
    }

    void insert_at(std::size_t slot, std::uint64_t key, std::int64_t payload_ref) {
        const std::uint8_t tag = key_tag(key);
        control_[slot] = tag;
        if (slot < (kGroupWidth - 1U)) {
            control_[capacity_ + slot] = tag;
        }
        hashes_[slot] = key;
        payload_refs_[slot] = payload_ref;
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

    FindResult find_slot(std::uint64_t key) const {
        const std::uint8_t tag = key_tag(key);
        const auto result = probe_finder_(control_.data(), hashes_.data(), capacity_, key, tag);
        if (result.probes < capacity_) {
            return {result.slot, result.found, result.probes};
        }
        throw std::runtime_error("Carchar probe exhausted table capacity");
    }

    void resize(std::size_t new_capacity) {
        new_capacity = std::max(kMinCapacity, next_power_of_two(new_capacity));

        auto old_control      = std::move(control_);
        auto old_hashes       = std::move(hashes_);
        auto old_payload_refs = std::move(payload_refs_);
        const auto old_capacity = capacity_;

        initialize_storage(new_capacity);
        ++resize_count_;

        for (std::size_t slot = 0; slot < old_capacity; ++slot) {
            if (old_control[slot] == kEmpty) {
                continue;
            }
            insert_at(
                find_empty_slot_for_resize(old_hashes[slot]), old_hashes[slot], old_payload_refs[slot]
            );
        }
    }

    std::size_t capacity_ = 0;
    std::vector<std::uint8_t> control_;
    // Uninitialized-allocator vectors: allocated but never pre-filled, because
    // an empty slot's hash/payload are never read (see initialize_storage).
    std::vector<std::uint64_t, detail::uninitialized_allocator<std::uint64_t>> hashes_;
    std::vector<std::int64_t, detail::uninitialized_allocator<std::int64_t>> payload_refs_;
    std::size_t size_ = 0;
    double load_factor_ = 0.80;
    detail::ProbeFn probe_finder_ = nullptr;

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