#pragma once

#include <cstdint>
#include <stdexcept>
#include <utility>
#include <vector>

#include "carchar_index.hpp"

namespace opteryx::carchar {

// A key hash -> the build rows that carry it. What remains of the old join index is
// what the ASOF join still reads its build through (native_join2.hpp): insert rows,
// then walk every key (items) and take its rows (rows_from_payload).
class CarcharJoinIndex {
   public:
    explicit CarcharJoinIndex(std::size_t initial_capacity = kMinCapacity, double load_factor = 0.80)
        : index_(initial_capacity, load_factor) {}

    void reserve(std::size_t expected_entries) {
        index_.reserve(expected_entries);
        row_lists_.reserve(expected_entries);
        row_counts_.reserve(expected_entries);
    }

    std::pair<std::int64_t, bool> insert_row(std::uint64_t key, std::int64_t row_id) {
        auto result = index_.find_or_insert(key, [this, row_id]() { return allocate_row_list(row_id); });
        if (!result.second) {
            append_join_row(result.first, row_id);
        }
        return result;
    }

    std::vector<std::pair<std::uint64_t, std::int64_t>> items() const { return index_.items(); }

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

   private:
    struct RowListEntry {
        std::int64_t inline0 = -1;
        std::int64_t inline1 = -1;
        std::vector<std::int64_t> overflow;
    };

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
};

}  // namespace opteryx::carchar
