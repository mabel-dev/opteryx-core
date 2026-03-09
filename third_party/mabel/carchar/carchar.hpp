#pragma once

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <utility>
#include <vector>

#include "carchar_simd.hpp"

namespace opteryx::carchar {

constexpr std::uint8_t kEmpty = 0x80;
constexpr std::size_t kMinCapacity = 16;
constexpr std::size_t kGroupWidth = detail::kProbeGroupWidth;
constexpr std::uint64_t kMask64 = std::numeric_limits<std::uint64_t>::max();

inline std::size_t next_power_of_two(std::size_t value) {
    return std::bit_ceil(std::max<std::size_t>(value, 1U));
}

inline std::uint64_t normalize_key(std::uint64_t key) {
    return key & kMask64;
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
        return static_cast<std::size_t>((normalize_key(key) & (capacity_ - 1U)) / kGroupWidth);
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
        key = normalize_key(key);
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
        key = normalize_key(key);
        const auto result = find_slot(key);
        if (!result.found) {
            return false;
        }
        payload_ref_out = payload_refs_[result.slot];
        return true;
    }

    std::size_t insert_new(std::uint64_t key, std::int64_t payload_ref) {
        ensure_insert_capacity();
        key = normalize_key(key);
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
        key = normalize_key(key);
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
        control_.assign(capacity_ + (kGroupWidth - 1U), kEmpty);
        hashes_.assign(capacity_, 0U);
        payload_refs_.assign(capacity_, -1);
        size_ = 0;
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
        const auto probe_finder = detail::select_probe_finder();
        const auto result = probe_finder(control_.data(), hashes_.data(), capacity_, key, tag);
        if (result.probes < capacity_) {
            return {result.slot, result.found, result.probes};
        }
        throw std::runtime_error("Carchar probe exhausted table capacity");
    }

    void resize(std::size_t new_capacity) {
        new_capacity = std::max(kMinCapacity, next_power_of_two(new_capacity));

        const auto old_control = control_;
        const auto old_hashes = hashes_;
        const auto old_payload_refs = payload_refs_;
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
    std::vector<std::uint64_t> hashes_;
    std::vector<std::int64_t> payload_refs_;
    std::size_t size_ = 0;
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

class CarcharJoinIndex {
   public:
    explicit CarcharJoinIndex(std::size_t initial_capacity = kMinCapacity, double load_factor = 0.80)
        : index_(initial_capacity, load_factor) {}

    std::size_t size() const noexcept { return index_.size(); }
    std::size_t capacity() const noexcept { return index_.capacity(); }
    void reserve(std::size_t expected_entries) {
        index_.reserve(expected_entries);
        row_lists_.reserve(expected_entries);
        row_counts_.reserve(expected_entries);
    }

    void tighten() {
        index_.tighten();
        row_lists_.shrink_to_fit();
        row_counts_.shrink_to_fit();
    }

    void tighten(double target_load_factor) {
        index_.tighten(target_load_factor);
        row_lists_.shrink_to_fit();
        row_counts_.shrink_to_fit();
    }

    std::pair<std::int64_t, bool> insert_row(std::uint64_t key, std::int64_t row_id) {
        auto result = index_.find_or_insert(key, [this, row_id]() { return allocate_row_list(row_id); });
        if (!result.second) {
            append_join_row(result.first, row_id);
        }
        return result;
    }

    void append_join_row(std::int64_t payload_ref, std::int64_t row_id) {
        const auto payload_index = static_cast<std::size_t>(payload_ref);
        auto& entry = row_lists_[payload_index];
        const std::uint32_t count = row_counts_[payload_index];
        if (count == 1U) {
            entry.inline1 = row_id;
        } else if (count >= 2U) {
            entry.overflow.push_back(row_id);
        } else {
            throw std::runtime_error("invalid row-list count");
        }
        ++row_counts_[payload_index];
    }

    std::vector<std::int64_t> rows_for(std::uint64_t key) {
        std::int64_t payload_ref = -1;
        if (!index_.lookup(key, payload_ref)) {
            return {};
        }
        return rows_from_payload(payload_ref);
    }

    std::vector<std::int64_t> get(std::uint64_t key) { return rows_for(key); }

    void append_probe_matches(
        std::uint64_t key,
        std::int64_t probe_row,
        std::vector<std::int64_t>& left_out,
        std::vector<std::int64_t>& right_out
    ) const {
        std::int64_t payload_ref = -1;
        if (!index_.lookup_fast(key, payload_ref)) {
            return;
        }
        append_probe_matches_from_payload(payload_ref, probe_row, left_out, right_out);
    }

    std::vector<std::pair<std::uint64_t, std::int64_t>> items() const { return index_.items(); }

    std::size_t row_count_for(std::uint64_t key) const {
        std::int64_t payload_ref = -1;
        if (!index_.lookup_fast(key, payload_ref)) {
            return 0;
        }
        return static_cast<std::size_t>(row_counts_[static_cast<std::size_t>(payload_ref)]);
    }

    std::size_t row_count_from_payload(std::int64_t payload_ref) const {
        return static_cast<std::size_t>(row_counts_[static_cast<std::size_t>(payload_ref)]);
    }

    std::uint64_t probe_row_count_sum(const std::uint64_t* keys, std::size_t length) const {
        if (keys == nullptr || length == 0) {
            return 0;
        }
        if (!should_group_probe_batch(length)) {
            return probe_row_count_sum_linear(keys, length);
        }

        const std::size_t group_count = index_.probe_group_count();
        grouped_probe_keys_.resize(length);
        probe_group_counts_.assign(group_count, 0U);
        probe_group_offsets_.assign(group_count, 0U);

        for (std::size_t i = 0; i < length; ++i) {
            ++probe_group_counts_[index_.probe_group_index(keys[i])];
        }

        std::size_t running = 0;
        for (std::size_t group_index = 0; group_index < group_count; ++group_index) {
            probe_group_offsets_[group_index] = running;
            running += probe_group_counts_[group_index];
        }

        std::vector<std::size_t> write_offsets = probe_group_offsets_;
        for (std::size_t i = 0; i < length; ++i) {
            const std::size_t group_index = index_.probe_group_index(keys[i]);
            grouped_probe_keys_[write_offsets[group_index]++] = keys[i];
        }

        return probe_row_count_sum_linear(grouped_probe_keys_.data(), length);
    }

   private:
    bool should_group_probe_batch(std::size_t length) const noexcept {
        const std::size_t group_count = index_.probe_group_count();
        return length >= 4096U && group_count >= 8U && group_count <= 1024U;
    }

    std::uint64_t probe_row_count_sum_linear(const std::uint64_t* keys, std::size_t length) const {
        constexpr std::size_t kProbeCacheSize = 256U;
        std::array<std::uint64_t, kProbeCacheSize> cache_keys {};
        std::array<std::uint32_t, kProbeCacheSize> cache_counts {};
        std::array<std::uint8_t, kProbeCacheSize> cache_valid {};
        std::uint64_t total = 0;
        std::int64_t payload_ref = -1;
        for (std::size_t i = 0; i < length; ++i) {
            const std::uint64_t key = keys[i];
            const std::size_t cache_slot = static_cast<std::size_t>(key & (kProbeCacheSize - 1U));
            if (cache_valid[cache_slot] != 0U && cache_keys[cache_slot] == key) {
                total += static_cast<std::uint64_t>(cache_counts[cache_slot]);
                continue;
            }
            if (index_.lookup_fast(keys[i], payload_ref)) {
                const std::uint32_t count = row_counts_[static_cast<std::size_t>(payload_ref)];
                cache_valid[cache_slot] = 1U;
                cache_keys[cache_slot] = key;
                cache_counts[cache_slot] = count;
                total += static_cast<std::uint64_t>(count);
            } else {
                cache_valid[cache_slot] = 1U;
                cache_keys[cache_slot] = key;
                cache_counts[cache_slot] = 0U;
            }
        }
        return total;
    }

   public:
    void append_probe_matches_from_payload(
        std::int64_t payload_ref,
        std::int64_t probe_row,
        std::vector<std::int64_t>& left_out,
        std::vector<std::int64_t>& right_out
    ) const {
        const auto payload_index = static_cast<std::size_t>(payload_ref);
        const auto& entry = row_lists_[payload_index];
        const std::uint32_t count = row_counts_[payload_index];
        if (count >= 1U) {
            left_out.push_back(entry.inline0);
            right_out.push_back(probe_row);
        }
        if (count >= 2U) {
            left_out.push_back(entry.inline1);
            right_out.push_back(probe_row);
        }
        for (const std::int64_t row_id : entry.overflow) {
            left_out.push_back(row_id);
            right_out.push_back(probe_row);
        }
    }

    std::vector<std::int64_t> rows_from_payload(std::int64_t payload_ref) const {
        const auto payload_index = static_cast<std::size_t>(payload_ref);
        const auto& entry = row_lists_[payload_index];
        const std::uint32_t count = row_counts_[payload_index];
        std::vector<std::int64_t> out;
        out.reserve(static_cast<std::size_t>(count));
        if (count >= 1U) {
            out.push_back(entry.inline0);
        }
        if (count >= 2U) {
            out.push_back(entry.inline1);
        }
        out.insert(out.end(), entry.overflow.begin(), entry.overflow.end());
        return out;
    }

    std::pair<std::vector<std::int64_t>, std::vector<std::int64_t>> probe_join_indices(
        const std::uint64_t* keys,
        const std::int64_t* probe_rows,
        std::size_t length
    ) const {
        std::pair<std::vector<std::int64_t>, std::vector<std::int64_t>> out;
        if (keys == nullptr || probe_rows == nullptr || length == 0) {
            return out;
        }

        const std::uint64_t total_matches = prepare_materialized_probe_batch(keys, probe_rows, length);
        out.first.reserve(static_cast<std::size_t>(total_matches));
        out.second.reserve(static_cast<std::size_t>(total_matches));
        materialize_probe_matches(active_probe_keys_, active_probe_rows_, length, out.first, out.second);

        return out;
    }

    CarcharStats stats() const {
        auto stats = index_.stats();
        stats.bytes_estimate += row_lists_.size() * (8U * 2U + 24U + 4U);
        for (const auto& entry : row_lists_) {
            stats.bytes_estimate += entry.overflow.size() * 8U;
        }
        return stats;
    }

   private:
    struct RowListEntry {
        std::int64_t inline0 = -1;
        std::int64_t inline1 = -1;
        std::vector<std::int64_t> overflow;
    };

    std::uint64_t prepare_materialized_probe_batch(
        const std::uint64_t* keys,
        const std::int64_t* probe_rows,
        std::size_t length
    ) const {
        active_probe_keys_ = keys;
        active_probe_rows_ = probe_rows;
        if (!should_group_probe_batch(length)) {
            return probe_row_count_sum_linear(keys, length);
        }

        const std::size_t group_count = index_.probe_group_count();
        grouped_probe_keys_.resize(length);
        grouped_probe_rows_.resize(length);
        probe_group_counts_.assign(group_count, 0U);
        probe_group_offsets_.assign(group_count, 0U);

        for (std::size_t i = 0; i < length; ++i) {
            ++probe_group_counts_[index_.probe_group_index(keys[i])];
        }

        std::size_t running = 0;
        for (std::size_t group_index = 0; group_index < group_count; ++group_index) {
            probe_group_offsets_[group_index] = running;
            running += probe_group_counts_[group_index];
        }

        std::vector<std::size_t> write_offsets = probe_group_offsets_;
        for (std::size_t i = 0; i < length; ++i) {
            const std::size_t group_index = index_.probe_group_index(keys[i]);
            const std::size_t output_index = write_offsets[group_index]++;
            grouped_probe_keys_[output_index] = keys[i];
            grouped_probe_rows_[output_index] = probe_rows[i];
        }

        active_probe_keys_ = grouped_probe_keys_.data();
        active_probe_rows_ = grouped_probe_rows_.data();
        return probe_row_count_sum_linear(active_probe_keys_, length);
    }

    void materialize_probe_matches(
        const std::uint64_t* keys,
        const std::int64_t* probe_rows,
        std::size_t length,
        std::vector<std::int64_t>& left_out,
        std::vector<std::int64_t>& right_out
    ) const {
        std::int64_t payload_ref = -1;
        for (std::size_t i = 0; i < length; ++i) {
            if (!index_.lookup_fast(keys[i], payload_ref)) {
                continue;
            }
            append_probe_matches_from_payload(payload_ref, probe_rows[i], left_out, right_out);
        }
    }

    std::int64_t allocate_row_list(std::int64_t row_id) {
        const auto payload_ref = static_cast<std::int64_t>(row_lists_.size());
        row_lists_.emplace_back();
        row_lists_.back().inline0 = row_id;
        row_counts_.push_back(1U);
        return payload_ref;
    }

    CarcharIndex index_;
    std::vector<RowListEntry> row_lists_;
    std::vector<std::uint32_t> row_counts_;
    mutable std::vector<std::uint64_t> grouped_probe_keys_;
    mutable std::vector<std::int64_t> grouped_probe_rows_;
    mutable std::vector<std::size_t> probe_group_counts_;
    mutable std::vector<std::size_t> probe_group_offsets_;
    mutable const std::uint64_t* active_probe_keys_ = nullptr;
    mutable const std::int64_t* active_probe_rows_ = nullptr;
};

class CarcharJoinEngine {
   public:
    explicit CarcharJoinEngine(
        std::size_t expected_entries = 0,
        std::size_t partition_bits = 0,
        double load_factor = 0.80,
        double probe_load_factor = 0.80
    )
        : partition_bits_(partition_bits),
          partition_count_(partition_bits == 0 ? 1U : (std::size_t{1} << partition_bits)),
          load_factor_(load_factor),
          probe_load_factor_(probe_load_factor) {
        if (partition_bits > 16U) {
            throw std::invalid_argument("partition_bits must be between 0 and 16");
        }
        if (!(probe_load_factor > 0.0 && probe_load_factor < 1.0)) {
            throw std::invalid_argument("probe_load_factor must be between 0 and 1");
        }
        const std::size_t per_partition =
            expected_entries == 0 ? kMinCapacity
                                  : std::max(
                                        kMinCapacity,
                                        next_power_of_two(
                                            (expected_entries + partition_count_ - 1U) / partition_count_
                                        )
                                    );
        partitions_.reserve(partition_count_);
        sealed_partitions_.resize(partition_count_);
        for (std::size_t i = 0; i < partition_count_; ++i) {
            partitions_.emplace_back(per_partition, load_factor_);
        }
    }

    std::size_t size() const noexcept { return size_; }

    std::size_t capacity() const noexcept {
        std::size_t total = 0;
        for (const auto& partition : partitions_) {
            total += partition.capacity();
        }
        return total;
    }

    std::size_t partition_bits() const noexcept { return partition_bits_; }
    std::size_t partition_count() const noexcept { return partition_count_; }

    void reserve(std::size_t expected_entries) {
        if (sealed_) {
            throw std::runtime_error("cannot reserve sealed CarcharJoinEngine");
        }
        if (expected_entries == 0) {
            return;
        }
        const std::size_t per_partition =
            std::max(kMinCapacity, (expected_entries + partition_count_ - 1U) / partition_count_);
        for (auto& partition : partitions_) {
            partition.reserve(per_partition);
        }
    }

    void seal() {
        if (partition_count_ == 1U) {
            partitions_[0].tighten(probe_load_factor_);
            sealed_ = true;
            return;
        }
        for (std::size_t partition_index = 0; partition_index < partition_count_; ++partition_index) {
            sealed_partitions_[partition_index].build_from(
                partitions_[partition_index].items(), partitions_[partition_index], probe_load_factor_
            );
        }
        sealed_ = true;
    }

    std::pair<std::int64_t, bool> insert_row(std::uint64_t key, std::int64_t row_id) {
        if (sealed_) {
            throw std::runtime_error("cannot insert into sealed CarcharJoinEngine");
        }
        const std::size_t partition_index = partition_for_key(key);
        auto result = partitions_[partition_index].insert_row(key, row_id);
        if (result.second) {
            ++size_;
        }
        return {pack_payload_ref(partition_index, result.first), result.second};
    }

    void insert_batch(const std::uint64_t* keys, const std::int64_t* row_ids, std::size_t length) {
        if (sealed_) {
            throw std::runtime_error("cannot insert into sealed CarcharJoinEngine");
        }
        if (keys == nullptr || row_ids == nullptr || length == 0) {
            return;
        }

        std::vector<std::size_t> counts(partition_count_, 0U);
        for (std::size_t i = 0; i < length; ++i) {
            ++counts[partition_for_key(keys[i])];
        }
        for (std::size_t partition_index = 0; partition_index < partition_count_; ++partition_index) {
            if (counts[partition_index] == 0U) {
                continue;
            }
            partitions_[partition_index].reserve(partitions_[partition_index].size() + counts[partition_index]);
        }
        for (std::size_t i = 0; i < length; ++i) {
            auto result = partitions_[partition_for_key(keys[i])].insert_row(keys[i], row_ids[i]);
            if (result.second) {
                ++size_;
            }
        }
    }

    void insert_batch(const std::uint64_t* keys, std::size_t length, std::int64_t row_id_offset = 0) {
        if (sealed_) {
            throw std::runtime_error("cannot insert into sealed CarcharJoinEngine");
        }
        if (keys == nullptr || length == 0) {
            return;
        }

        std::vector<std::size_t> counts(partition_count_, 0U);
        for (std::size_t i = 0; i < length; ++i) {
            ++counts[partition_for_key(keys[i])];
        }
        for (std::size_t partition_index = 0; partition_index < partition_count_; ++partition_index) {
            if (counts[partition_index] == 0U) {
                continue;
            }
            partitions_[partition_index].reserve(partitions_[partition_index].size() + counts[partition_index]);
        }
        for (std::size_t i = 0; i < length; ++i) {
            auto result =
                partitions_[partition_for_key(keys[i])].insert_row(keys[i], row_id_offset + static_cast<std::int64_t>(i));
            if (result.second) {
                ++size_;
            }
        }
    }

    void append_join_row(std::int64_t payload_ref, std::int64_t row_id) {
        if (sealed_) {
            throw std::runtime_error("cannot append to sealed CarcharJoinEngine");
        }
        const auto [partition_index, local_payload_ref] = unpack_payload_ref(payload_ref);
        partitions_[partition_index].append_join_row(local_payload_ref, row_id);
    }

    std::vector<std::int64_t> rows_for(std::uint64_t key) {
        const std::size_t partition_index = partition_for_key(key);
        if (!sealed_ || partition_count_ == 1U) {
            return partitions_[partition_index].rows_for(key);
        }
        std::int64_t payload_ref = -1;
        if (!sealed_partitions_[partition_index].lookup_payload_ref(key, payload_ref)) {
            return {};
        }
        return partitions_[partition_index].rows_from_payload(payload_ref);
    }

    std::vector<std::int64_t> get(std::uint64_t key) { return rows_for(key); }

    std::size_t row_count_for(std::uint64_t key) const {
        const std::size_t partition_index = partition_for_key(key);
        if (!sealed_ || partition_count_ == 1U) {
            return partitions_[partition_index].row_count_for(key);
        }
        return sealed_partitions_[partition_index].row_count_for_key(key);
    }

    std::uint64_t probe_row_count_sum(const std::uint64_t* keys, std::size_t length) const {
        if (keys == nullptr || length == 0) {
            return 0;
        }
        if (partition_count_ == 1U) {
            return partitions_[0].probe_row_count_sum(keys, length);
        }

        if (sealed_) {
            constexpr std::size_t kProbeCacheSize = 256U;
            std::array<std::uint64_t, kProbeCacheSize> cache_keys {};
            std::array<std::uint32_t, kProbeCacheSize> cache_counts {};
            std::array<std::uint8_t, kProbeCacheSize> cache_valid {};
            std::uint64_t total = 0;

            for (std::size_t i = 0; i < length; ++i) {
                const std::uint64_t key = keys[i];
                const std::size_t cache_slot = static_cast<std::size_t>(key & (kProbeCacheSize - 1U));
                if (cache_valid[cache_slot] != 0U && cache_keys[cache_slot] == key) {
                    total += static_cast<std::uint64_t>(cache_counts[cache_slot]);
                    continue;
                }

                const std::size_t partition_index = partition_for_key(key);
                const std::uint32_t count =
                    static_cast<std::uint32_t>(sealed_partitions_[partition_index].row_count_for_key(key));
                cache_valid[cache_slot] = 1U;
                cache_keys[cache_slot] = key;
                cache_counts[cache_slot] = count;
                total += static_cast<std::uint64_t>(count);
            }
            return total;
        }

        partition_counts_.assign(partition_count_, 0U);
        partition_offsets_.assign(partition_count_, 0U);
        for (std::size_t i = 0; i < length; ++i) {
            ++partition_counts_[partition_for_key(keys[i])];
        }

        std::size_t running = 0;
        for (std::size_t partition_index = 0; partition_index < partition_count_; ++partition_index) {
            partition_offsets_[partition_index] = running;
            running += partition_counts_[partition_index];
        }

        grouped_probe_keys_.resize(length);
        std::vector<std::size_t> write_offsets = partition_offsets_;
        for (std::size_t i = 0; i < length; ++i) {
            const std::size_t partition_index = partition_for_key(keys[i]);
            grouped_probe_keys_[write_offsets[partition_index]++] = keys[i];
        }

        std::uint64_t total = 0;
        for (std::size_t partition_index = 0; partition_index < partition_count_; ++partition_index) {
            const std::size_t count = partition_counts_[partition_index];
            if (count == 0U) {
                continue;
            }
            if (sealed_) {
                total += sealed_partitions_[partition_index].probe_row_count_sum(
                    grouped_probe_keys_.data() + partition_offsets_[partition_index], count
                );
            } else {
                total += partitions_[partition_index].probe_row_count_sum(
                    grouped_probe_keys_.data() + partition_offsets_[partition_index], count
                );
            }
        }
        return total;
    }

    std::vector<std::int64_t> rows_from_payload(std::int64_t payload_ref) const {
        const auto [partition_index, local_payload_ref] = unpack_payload_ref(payload_ref);
        return partitions_[partition_index].rows_from_payload(local_payload_ref);
    }

    void append_probe_matches(
        std::uint64_t key,
        std::int64_t probe_row,
        std::vector<std::int64_t>& left_out,
        std::vector<std::int64_t>& right_out
    ) const {
        const std::size_t partition_index = partition_for_key(key);
        if (!sealed_ || partition_count_ == 1U) {
            partitions_[partition_index].append_probe_matches(key, probe_row, left_out, right_out);
            return;
        }
        std::int64_t payload_ref = -1;
        if (!sealed_partitions_[partition_index].lookup_payload_ref(key, payload_ref)) {
            return;
        }
        partitions_[partition_index].append_probe_matches_from_payload(
            payload_ref, probe_row, left_out, right_out
        );
    }

    std::pair<std::vector<std::int64_t>, std::vector<std::int64_t>> probe_join_indices(
        const std::uint64_t* keys,
        const std::int64_t* probe_rows,
        std::size_t length
    ) const {
        if (partition_count_ == 1U) {
            return partitions_[0].probe_join_indices(keys, probe_rows, length);
        }

        std::pair<std::vector<std::int64_t>, std::vector<std::int64_t>> out;
        if (keys == nullptr || probe_rows == nullptr || length == 0) {
            return out;
        }

        const std::uint64_t total_matches = probe_row_count_sum(keys, length);
        out.first.reserve(static_cast<std::size_t>(total_matches));
        out.second.reserve(static_cast<std::size_t>(total_matches));

        for (std::size_t i = 0; i < length; ++i) {
            const std::size_t partition_index = partition_for_key(keys[i]);
            const auto matches = partitions_[partition_index].probe_join_indices(keys + i, probe_rows + i, 1U);
            out.first.insert(out.first.end(), matches.first.begin(), matches.first.end());
            out.second.insert(out.second.end(), matches.second.begin(), matches.second.end());
        }

        return out;
    }

    CarcharStats stats() const {
        CarcharStats totals;
        for (const auto& partition : partitions_) {
            const auto stats = partition.stats();
            totals.capacity += stats.capacity;
            totals.size += stats.size;
            totals.resize_count += stats.resize_count;
            totals.lookup_count += stats.lookup_count;
            totals.insert_count += stats.insert_count;
            totals.total_probes += stats.total_probes;
            totals.max_probe_length = std::max(totals.max_probe_length, stats.max_probe_length);
            totals.lookup_total_probes += stats.lookup_total_probes;
            totals.insert_total_probes += stats.insert_total_probes;
            totals.max_lookup_probe_length =
                std::max(totals.max_lookup_probe_length, stats.max_lookup_probe_length);
            totals.max_insert_probe_length =
                std::max(totals.max_insert_probe_length, stats.max_insert_probe_length);
            totals.bytes_estimate += stats.bytes_estimate;
        }
        if (sealed_) {
            for (const auto& partition : sealed_partitions_) {
                totals.capacity += partition.capacity;
                totals.bytes_estimate += partition.bytes_estimate();
            }
        }
        return totals;
    }

   private:
    struct SealedPartition {
        std::size_t size = 0;
        std::size_t capacity = 0;
        std::vector<std::uint8_t> control;
        std::vector<std::uint64_t> hashes;
        std::vector<std::uint32_t> row_counts;
        std::vector<std::int64_t> payload_refs;

        void build_from(
            const std::vector<std::pair<std::uint64_t, std::int64_t>>& items,
            const CarcharJoinIndex& payload_source,
            double load_factor
        ) {
            size = items.size();
            if (items.empty()) {
                capacity = 0;
                control.clear();
                hashes.clear();
                row_counts.clear();
                payload_refs.clear();
                return;
            }

            const double desired_slots = static_cast<double>(items.size()) / load_factor;
            const std::size_t desired_buckets =
                std::max<std::size_t>(1U, static_cast<std::size_t>(
                                              (desired_slots + static_cast<double>(kGroupWidth) - 1.0) /
                                              static_cast<double>(kGroupWidth)
                                          ));
            const std::size_t bucket_count = std::max<std::size_t>(
                1U,
                next_power_of_two(desired_buckets)
            );
            capacity = std::max(kMinCapacity, bucket_count * kGroupWidth);
            control.assign(capacity, kEmpty);
            hashes.assign(capacity, 0U);
            row_counts.assign(capacity, 0U);
            payload_refs.assign(capacity, -1);

            const std::size_t sealed_bucket_count = capacity / kGroupWidth;
            const std::size_t bucket_mask = sealed_bucket_count - 1U;
            for (const auto& [key, payload_ref] : items) {
                std::size_t bucket_index = static_cast<std::size_t>(key) & bucket_mask;
                while (true) {
                    const std::size_t bucket_base = bucket_index * kGroupWidth;
                    bool inserted = false;
                    for (std::size_t lane = 0; lane < kGroupWidth; ++lane) {
                        const std::size_t slot = bucket_base + lane;
                        if (control[slot] != kEmpty) {
                            continue;
                        }
                        control[slot] = key_tag(key);
                        hashes[slot] = key;
                        row_counts[slot] =
                            static_cast<std::uint32_t>(payload_source.row_count_from_payload(payload_ref));
                        payload_refs[slot] = payload_ref;
                        inserted = true;
                        break;
                    }
                    if (inserted) {
                        break;
                    }
                    bucket_index = (bucket_index + 1U) & bucket_mask;
                }
            }
        }

        bool lookup_payload_ref(std::uint64_t key, std::int64_t& payload_ref_out) const {
            if (capacity == 0) {
                return false;
            }
            const auto result =
                detail::select_bucket_probe_finder()(control.data(), hashes.data(), capacity, key, key_tag(key));
            if (!result.found) {
                return false;
            }
            payload_ref_out = payload_refs[result.slot];
            return true;
        }

        std::size_t row_count_for_key(std::uint64_t key) const {
            if (capacity == 0) {
                return 0;
            }
            const auto result =
                detail::select_bucket_probe_finder()(control.data(), hashes.data(), capacity, key, key_tag(key));
            if (!result.found) {
                return 0;
            }
            return static_cast<std::size_t>(row_counts[result.slot]);
        }

        std::uint64_t probe_row_count_sum(const std::uint64_t* keys, std::size_t length) const {
            if (capacity == 0 || keys == nullptr || length == 0) {
                return 0;
            }
            const auto probe_finder = detail::select_bucket_probe_finder();
            std::uint64_t total = 0;
            for (std::size_t i = 0; i < length; ++i) {
                const auto result = probe_finder(control.data(), hashes.data(), capacity, keys[i], key_tag(keys[i]));
                if (result.found) {
                    total += static_cast<std::uint64_t>(row_counts[result.slot]);
                }
            }
            return total;
        }

        std::size_t bytes_estimate() const noexcept {
            return control.size() * sizeof(std::uint8_t) +
                   hashes.size() * sizeof(std::uint64_t) +
                   row_counts.size() * sizeof(std::uint32_t) +
                   payload_refs.size() * sizeof(std::int64_t);
        }
    };

    static constexpr std::uint64_t kPackedPartitionShift = 48U;
    static constexpr std::uint64_t kPackedPayloadMask = (std::uint64_t{1} << kPackedPartitionShift) - 1U;

    std::size_t partition_for_key(std::uint64_t key) const noexcept {
        if (partition_bits_ == 0U) {
            return 0U;
        }
        return static_cast<std::size_t>(key >> (64U - partition_bits_));
    }

    static std::int64_t pack_payload_ref(std::size_t partition_index, std::int64_t payload_ref) noexcept {
        return static_cast<std::int64_t>(
            (static_cast<std::uint64_t>(partition_index) << kPackedPartitionShift) |
            (static_cast<std::uint64_t>(payload_ref) & kPackedPayloadMask)
        );
    }

    static std::pair<std::size_t, std::int64_t> unpack_payload_ref(std::int64_t payload_ref) noexcept {
        const std::uint64_t raw = static_cast<std::uint64_t>(payload_ref);
        return {
            static_cast<std::size_t>(raw >> kPackedPartitionShift),
            static_cast<std::int64_t>(raw & kPackedPayloadMask),
        };
    }

    std::size_t partition_bits_ = 0;
    std::size_t partition_count_ = 1;
    double load_factor_ = 0.80;
    double probe_load_factor_ = 0.80;
    std::size_t size_ = 0;
    bool sealed_ = false;
    std::vector<CarcharJoinIndex> partitions_;
    std::vector<SealedPartition> sealed_partitions_;
    mutable std::vector<std::uint64_t> grouped_probe_keys_;
    mutable std::vector<std::size_t> partition_counts_;
    mutable std::vector<std::size_t> partition_offsets_;
};

}  // namespace opteryx::carchar
