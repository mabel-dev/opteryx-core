#pragma once

#include <array>
#include <cstdint>
#include <stdexcept>
#include <utility>
#include <vector>

#include "carchar_index.hpp"

namespace opteryx::carchar {

class CarcharJoinIndex {
   public:
    explicit CarcharJoinIndex(std::size_t initial_capacity = kMinCapacity, double load_factor = 0.80)
        : index_(initial_capacity, load_factor) {}

    // Persistent probe cache — survives across calls so repeated probes of the same
    // keys (probe-heavy workloads, repeated joins) pay zero probe cost after the first hit.
    struct ProbeCache {
        static constexpr std::size_t CACHE_SIZE = 256U;  // Power of 2; matches old stack cache size
        std::array<std::uint64_t, CACHE_SIZE> keys{};
        std::array<std::uint32_t, CACHE_SIZE> counts{};
        std::array<std::uint8_t,  CACHE_SIZE> valid{};

        bool lookup(std::uint64_t key, std::uint32_t& count_out) const noexcept {
            const std::size_t slot = static_cast<std::size_t>(key) & (CACHE_SIZE - 1U);
            if (valid[slot] != 0U && keys[slot] == key) {
                count_out = counts[slot];
                return true;
            }
            return false;
        }

        void update(std::uint64_t key, std::uint32_t count) noexcept {
            const std::size_t slot = static_cast<std::size_t>(key) & (CACHE_SIZE - 1U);
            keys[slot] = key;
            counts[slot] = count;
            valid[slot] = 1U;
        }
    };

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

    // Sum of all chain lengths — i.e. total rows inserted into this index.
    // Used by adaptive join statistics (Phase 1) to compute average chain length
    // (per docs/adaptive_join_statistics.md).
    std::uint64_t total_row_count() const noexcept {
        std::uint64_t total = 0;
        for (const std::uint32_t count : row_counts_) {
            total += count;
        }
        return total;
    }

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
        // Raised upper bound from 1024 → 4096 so medium-dup workloads (~2K groups) also
        // benefit from locality grouping.  Low-dup tables (~32K+ groups) still fall through
        // to linear because scatter overhead exceeds the locality benefit at ~3 keys/group.
        return length >= 4096U && group_count >= 8U && group_count <= 4096U;
    }

    std::uint64_t probe_row_count_sum_linear(const std::uint64_t* keys, std::size_t length) const {
        std::uint64_t total = 0;
        std::int64_t payload_ref = -1;
        for (std::size_t i = 0; i < length; ++i) {
            const std::uint64_t key = keys[i];
            std::uint32_t count = 0;
            // Persistent cache: hits survive across calls — probe-heavy and repeated workloads
            // pay zero probe cost for hot keys after the first miss.
            if (probe_cache_.lookup(key, count)) {
                total += static_cast<std::uint64_t>(count);
                continue;
            }
            if (index_.lookup_fast(key, payload_ref)) {
                count = row_counts_[static_cast<std::size_t>(payload_ref)];
            }
            probe_cache_.update(key, count);
            total += static_cast<std::uint64_t>(count);
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
    mutable ProbeCache probe_cache_;
    mutable std::vector<std::uint64_t> grouped_probe_keys_;
    mutable std::vector<std::int64_t> grouped_probe_rows_;
    mutable std::vector<std::size_t> probe_group_counts_;
    mutable std::vector<std::size_t> probe_group_offsets_;
    mutable const std::uint64_t* active_probe_keys_ = nullptr;
    mutable const std::int64_t* active_probe_rows_ = nullptr;
};

}  // namespace opteryx::carchar