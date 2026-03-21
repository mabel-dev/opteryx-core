#pragma once

#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "../../third_party/eyalz800/zpp_bits/zpp_bits.h"

namespace opteryx::zpp_key_codec {

struct group_key_record {
    std::vector<std::optional<std::int64_t>> fixed_values;
    std::vector<std::optional<std::string>> encoded_values;

    using serialize = zpp::bits::members<2>;
};

inline bool append_single_fixed_record(std::vector<std::uint8_t> &payload_bytes,
                                       std::vector<std::int64_t> &payload_offsets,
                                       std::int64_t value,
                                       std::int64_t valid_flag) {
    auto out = zpp::bits::data_out<std::uint8_t>();
    group_key_record record;
    record.fixed_values.emplace_back(valid_flag != 0 ? std::optional<std::int64_t>{value}
                                                     : std::nullopt);
    if (auto result = out.output(record); zpp::bits::failure(result)) {
        return false;
    }
    payload_bytes.insert(payload_bytes.end(), out.data.begin(), out.data.end());
    payload_offsets.push_back(static_cast<std::int64_t>(payload_bytes.size()));
    return true;
}

inline bool append_single_encoded_record(std::vector<std::uint8_t> &payload_bytes,
                                         std::vector<std::int64_t> &payload_offsets,
                                         const char *data_ptr,
                                         std::size_t data_len,
                                         std::int64_t valid_flag) {
    auto out = zpp::bits::data_out<std::uint8_t>();
    group_key_record record;
    record.encoded_values.emplace_back(
        valid_flag != 0 ? std::optional<std::string>{std::string(data_ptr, data_len)} : std::nullopt);
    if (auto result = out.output(record); zpp::bits::failure(result)) {
        return false;
    }
    payload_bytes.insert(payload_bytes.end(), out.data.begin(), out.data.end());
    payload_offsets.push_back(static_cast<std::int64_t>(payload_bytes.size()));
    return true;
}

inline bool append_multi_record(std::vector<std::uint8_t> &payload_bytes,
                                std::vector<std::int64_t> &payload_offsets,
                                const std::vector<std::int64_t> &fixed_values,
                                const std::vector<std::int64_t> &fixed_valids,
                                const std::vector<std::string> &encoded_values,
                                const std::vector<std::int64_t> &encoded_valids) {
    if (fixed_values.size() != fixed_valids.size() || encoded_values.size() != encoded_valids.size()) {
        return false;
    }

    auto out = zpp::bits::data_out<std::uint8_t>();
    group_key_record record;
    record.fixed_values.reserve(fixed_values.size());
    record.encoded_values.reserve(encoded_values.size());

    for (std::size_t idx = 0; idx < fixed_values.size(); ++idx) {
        record.fixed_values.emplace_back(
            fixed_valids[idx] != 0 ? std::optional<std::int64_t>{fixed_values[idx]} : std::nullopt);
    }

    for (std::size_t idx = 0; idx < encoded_values.size(); ++idx) {
        record.encoded_values.emplace_back(
            encoded_valids[idx] != 0 ? std::optional<std::string>{encoded_values[idx]} : std::nullopt);
    }

    if (auto result = out.output(record); zpp::bits::failure(result)) {
        return false;
    }
    payload_bytes.insert(payload_bytes.end(), out.data.begin(), out.data.end());
    payload_offsets.push_back(static_cast<std::int64_t>(payload_bytes.size()));
    return true;
}

inline bool decode_record(const std::vector<std::uint8_t> &payload_bytes,
                          const std::vector<std::int64_t> &payload_offsets,
                          std::size_t state_index,
                          group_key_record &record) {
    if (payload_offsets.size() < state_index + 2) {
        return false;
    }

    const auto start = static_cast<std::size_t>(payload_offsets[state_index]);
    const auto stop = static_cast<std::size_t>(payload_offsets[state_index + 1]);
    if (stop < start || stop > payload_bytes.size()) {
        return false;
    }

    auto view = std::span<const std::uint8_t>{payload_bytes.data() + start, stop - start};
    auto in = zpp::bits::in(view);
    if (auto result = in(record); zpp::bits::failure(result)) {
        return false;
    }
    return true;
}

inline bool decode_single_fixed_record(const std::vector<std::uint8_t> &payload_bytes,
                                       const std::vector<std::int64_t> &payload_offsets,
                                       std::size_t state_index,
                                       std::int64_t *value_out,
                                       std::int64_t *valid_flag_out) {
    group_key_record record;
    if (!decode_record(payload_bytes, payload_offsets, state_index, record) || record.fixed_values.size() != 1u) {
        return false;
    }
    if (record.fixed_values[0].has_value()) {
        *valid_flag_out = 1;
        *value_out = *record.fixed_values[0];
    } else {
        *valid_flag_out = 0;
        *value_out = 0;
    }
    return true;
}

inline bool decode_single_encoded_record(const std::vector<std::uint8_t> &payload_bytes,
                                         const std::vector<std::int64_t> &payload_offsets,
                                         std::size_t state_index,
                                         std::string &value_out,
                                         std::int64_t *valid_flag_out) {
    group_key_record record;
    if (!decode_record(payload_bytes, payload_offsets, state_index, record) ||
        record.encoded_values.size() != 1u) {
        return false;
    }
    if (record.encoded_values[0].has_value()) {
        *valid_flag_out = 1;
        value_out = *record.encoded_values[0];
    } else {
        *valid_flag_out = 0;
        value_out.clear();
    }
    return true;
}

inline bool decode_multi_record(const std::vector<std::uint8_t> &payload_bytes,
                                const std::vector<std::int64_t> &payload_offsets,
                                std::size_t state_index,
                                std::vector<std::int64_t> &fixed_values_out,
                                std::vector<std::int64_t> &fixed_valids_out,
                                std::vector<std::string> &encoded_values_out,
                                std::vector<std::int64_t> &encoded_valids_out) {
    group_key_record record;
    if (!decode_record(payload_bytes, payload_offsets, state_index, record)) {
        return false;
    }

    fixed_values_out.clear();
    fixed_valids_out.clear();
    encoded_values_out.clear();
    encoded_valids_out.clear();

    fixed_values_out.reserve(record.fixed_values.size());
    fixed_valids_out.reserve(record.fixed_values.size());
    for (const auto &item : record.fixed_values) {
        if (item.has_value()) {
            fixed_values_out.push_back(*item);
            fixed_valids_out.push_back(1);
        } else {
            fixed_values_out.push_back(0);
            fixed_valids_out.push_back(0);
        }
    }

    encoded_values_out.reserve(record.encoded_values.size());
    encoded_valids_out.reserve(record.encoded_values.size());
    for (const auto &item : record.encoded_values) {
        if (item.has_value()) {
            encoded_values_out.push_back(*item);
            encoded_valids_out.push_back(1);
        } else {
            encoded_values_out.emplace_back();
            encoded_valids_out.push_back(0);
        }
    }

    return true;
}

}  // namespace opteryx::zpp_key_codec
