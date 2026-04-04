#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <type_traits>
#include <vector>

namespace opteryx {
namespace group_key_codec {

constexpr std::int64_t KEY_MULTI_FIXED_INT = 1;
constexpr std::int64_t KEY_MULTI_FIXED_DATE32 = 2;
constexpr std::int64_t KEY_MULTI_FIXED_TIME32 = 3;
constexpr std::int64_t KEY_MULTI_FIXED_TIME64 = 4;
constexpr std::int64_t KEY_MULTI_FIXED_TIMESTAMP64 = 5;
constexpr std::int64_t KEY_MULTI_ENCODED_STRING = 6;

constexpr std::int32_t ENCODED_NULL_LENGTH = -1;

inline bool is_fixed_kind(const std::int64_t key_kind) {
    return key_kind == KEY_MULTI_FIXED_INT || key_kind == KEY_MULTI_FIXED_DATE32 ||
           key_kind == KEY_MULTI_FIXED_TIME32 || key_kind == KEY_MULTI_FIXED_TIME64 ||
           key_kind == KEY_MULTI_FIXED_TIMESTAMP64;
}

inline std::size_t bitmap_size_bytes(const std::size_t key_count) {
    return (key_count + 7u) >> 3u;
}

inline bool bitmap_is_valid(const std::uint8_t *bitmap, const std::size_t key_index) {
    return (bitmap[key_index >> 3u] & static_cast<std::uint8_t>(1u << (key_index & 7u))) != 0;
}

inline void bitmap_set_valid(std::uint8_t *bitmap, const std::size_t key_index) {
    bitmap[key_index >> 3u] |= static_cast<std::uint8_t>(1u << (key_index & 7u));
}

inline bool append_bytes(std::vector<std::uint8_t> &payload_bytes,
                         const void *data,
                         const std::size_t length) {
    if (length == 0) {
        return true;
    }
    if (data == nullptr) {
        return false;
    }
    const auto *ptr = static_cast<const std::uint8_t *>(data);
    payload_bytes.insert(payload_bytes.end(), ptr, ptr + length);
    return true;
}

template <typename T>
inline bool append_pod(std::vector<std::uint8_t> &payload_bytes, const T value) {
    static_assert(std::is_trivially_copyable<T>::value, "T must be trivially copyable");
    return append_bytes(payload_bytes, &value, sizeof(T));
}

template <typename T>
inline bool read_pod(const std::uint8_t *record_ptr,
                     const std::size_t record_size,
                     std::size_t &cursor,
                     T &out) {
    static_assert(std::is_trivially_copyable<T>::value, "T must be trivially copyable");
    if (cursor > record_size || sizeof(T) > record_size - cursor) {
        return false;
    }
    std::memcpy(&out, record_ptr + cursor, sizeof(T));
    cursor += sizeof(T);
    return true;
}

inline bool validate_offsets(const std::vector<std::int64_t> &payload_offsets) {
    if (payload_offsets.empty()) {
        return false;
    }
    if (payload_offsets[0] != 0) {
        return false;
    }
    for (std::size_t i = 1; i < payload_offsets.size(); ++i) {
        if (payload_offsets[i] < payload_offsets[i - 1]) {
            return false;
        }
    }
    return true;
}

inline bool append_record_offset(std::vector<std::uint8_t> &payload_bytes,
                                 std::vector<std::int64_t> &payload_offsets) {
    if (payload_bytes.size() >
        static_cast<std::size_t>(std::numeric_limits<std::int64_t>::max())) {
        return false;
    }
    payload_offsets.push_back(static_cast<std::int64_t>(payload_bytes.size()));
    return true;
}

inline bool append_single_fixed_record(std::vector<std::uint8_t> &payload_bytes,
                                       std::vector<std::int64_t> &payload_offsets,
                                       const std::int64_t value,
                                       const std::int64_t valid_flag) {
    if (payload_offsets.empty()) {
        payload_offsets.push_back(0);
    }
    if (!validate_offsets(payload_offsets)) {
        return false;
    }

    const std::uint8_t bitmap = valid_flag != 0 ? 0x01u : 0x00u;
    if (!append_pod(payload_bytes, bitmap)) {
        return false;
    }

    const std::int64_t stored_value = valid_flag != 0 ? value : 0;
    if (!append_pod(payload_bytes, stored_value)) {
        return false;
    }

    return append_record_offset(payload_bytes, payload_offsets);
}

inline bool append_single_encoded_record(std::vector<std::uint8_t> &payload_bytes,
                                         std::vector<std::int64_t> &payload_offsets,
                                         const char *data_ptr,
                                         const std::size_t data_len,
                                         const std::int64_t valid_flag) {
    if (payload_offsets.empty()) {
        payload_offsets.push_back(0);
    }
    if (!validate_offsets(payload_offsets)) {
        return false;
    }

    const std::uint8_t bitmap = valid_flag != 0 ? 0x01u : 0x00u;
    if (!append_pod(payload_bytes, bitmap)) {
        return false;
    }

    const std::int32_t encoded_length =
        valid_flag != 0 ? static_cast<std::int32_t>(data_len) : ENCODED_NULL_LENGTH;

    if (valid_flag != 0 &&
        data_len > static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())) {
        return false;
    }

    if (!append_pod(payload_bytes, encoded_length)) {
        return false;
    }

    if (valid_flag != 0 && data_len > 0) {
        if (!append_bytes(payload_bytes, data_ptr, data_len)) {
            return false;
        }
    }

    return append_record_offset(payload_bytes, payload_offsets);
}

inline bool append_multi_record(std::vector<std::uint8_t> &payload_bytes,
                                std::vector<std::int64_t> &payload_offsets,
                                const std::vector<std::int64_t> &fixed_values,
                                const std::vector<std::int64_t> &fixed_valids,
                                const std::vector<std::string> &encoded_values,
                                const std::vector<std::int64_t> &encoded_valids) {
    if (payload_offsets.empty()) {
        payload_offsets.push_back(0);
    }
    if (!validate_offsets(payload_offsets)) {
        return false;
    }
    if (fixed_values.size() != fixed_valids.size() || encoded_values.size() != encoded_valids.size()) {
        return false;
    }

    const std::size_t key_count = fixed_values.size() + encoded_values.size();
    const std::size_t bitmap_bytes = bitmap_size_bytes(key_count);
    const std::size_t bitmap_start = payload_bytes.size();

    payload_bytes.resize(payload_bytes.size() + bitmap_bytes, 0);

    std::size_t key_index = 0;
    for (std::size_t i = 0; i < fixed_values.size(); ++i, ++key_index) {
        if (fixed_valids[i] != 0) {
            bitmap_set_valid(payload_bytes.data() + bitmap_start, key_index);
        }
        const std::int64_t stored_value = fixed_valids[i] != 0 ? fixed_values[i] : 0;
        if (!append_pod(payload_bytes, stored_value)) {
            return false;
        }
    }

    for (std::size_t i = 0; i < encoded_values.size(); ++i, ++key_index) {
        const bool valid = encoded_valids[i] != 0;
        if (valid) {
            bitmap_set_valid(payload_bytes.data() + bitmap_start, key_index);
        }

        if (valid &&
            encoded_values[i].size() >
                static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())) {
            return false;
        }

        const std::int32_t encoded_length =
            valid ? static_cast<std::int32_t>(encoded_values[i].size()) : ENCODED_NULL_LENGTH;

        if (!append_pod(payload_bytes, encoded_length)) {
            return false;
        }

        if (valid && !encoded_values[i].empty()) {
            if (!append_bytes(payload_bytes, encoded_values[i].data(), encoded_values[i].size())) {
                return false;
            }
        }
    }

    return append_record_offset(payload_bytes, payload_offsets);
}

inline bool decode_record_bounds(const std::vector<std::uint8_t> &payload_bytes,
                                 const std::vector<std::int64_t> &payload_offsets,
                                 const std::size_t state_index,
                                 const std::uint8_t *&record_ptr,
                                 std::size_t &record_size) {
    if (!validate_offsets(payload_offsets)) {
        return false;
    }
    if (payload_offsets.back() < 0) {
        return false;
    }
    if (static_cast<std::size_t>(payload_offsets.back()) != payload_bytes.size()) {
        return false;
    }
    if (payload_offsets.size() < state_index + 2u) {
        return false;
    }

    const std::int64_t start_i64 = payload_offsets[state_index];
    const std::int64_t stop_i64 = payload_offsets[state_index + 1];

    if (start_i64 < 0 || stop_i64 < 0 || stop_i64 < start_i64) {
        return false;
    }

    const std::size_t start = static_cast<std::size_t>(start_i64);
    const std::size_t stop = static_cast<std::size_t>(stop_i64);

    if (stop > payload_bytes.size()) {
        return false;
    }

    record_ptr = payload_bytes.data() + start;
    record_size = stop - start;
    return true;
}

inline bool decode_single_fixed_record(const std::vector<std::uint8_t> &payload_bytes,
                                       const std::vector<std::int64_t> &payload_offsets,
                                       const std::size_t state_index,
                                       std::int64_t *value_out,
                                       std::int64_t *valid_flag_out) {
    if (value_out == nullptr || valid_flag_out == nullptr) {
        return false;
    }

    const std::uint8_t *record_ptr = nullptr;
    std::size_t record_size = 0;
    if (!decode_record_bounds(payload_bytes, payload_offsets, state_index, record_ptr, record_size)) {
        return false;
    }

    constexpr std::size_t key_count = 1;
    constexpr std::size_t bitmap_bytes = 1;
    constexpr std::size_t required_size = bitmap_bytes + sizeof(std::int64_t);

    if (record_size != required_size) {
        return false;
    }

    std::size_t cursor = 0;
    std::uint8_t bitmap = 0;
    std::int64_t stored_value = 0;

    if (!read_pod(record_ptr, record_size, cursor, bitmap)) {
        return false;
    }
    if (!read_pod(record_ptr, record_size, cursor, stored_value)) {
        return false;
    }
    if (cursor != record_size) {
        return false;
    }

    const bool valid = bitmap_is_valid(&bitmap, 0);
    *valid_flag_out = valid ? 1 : 0;
    *value_out = valid ? stored_value : 0;
    return true;
}

inline bool decode_single_encoded_record(const std::vector<std::uint8_t> &payload_bytes,
                                         const std::vector<std::int64_t> &payload_offsets,
                                         const std::size_t state_index,
                                         std::string &value_out,
                                         std::int64_t *valid_flag_out) {
    if (valid_flag_out == nullptr) {
        return false;
    }

    const std::uint8_t *record_ptr = nullptr;
    std::size_t record_size = 0;
    if (!decode_record_bounds(payload_bytes, payload_offsets, state_index, record_ptr, record_size)) {
        return false;
    }

    constexpr std::size_t bitmap_bytes = 1;
    if (record_size < bitmap_bytes + sizeof(std::int32_t)) {
        return false;
    }

    std::size_t cursor = 0;
    std::uint8_t bitmap = 0;
    std::int32_t encoded_length = 0;

    if (!read_pod(record_ptr, record_size, cursor, bitmap)) {
        return false;
    }
    if (!read_pod(record_ptr, record_size, cursor, encoded_length)) {
        return false;
    }

    const bool valid = bitmap_is_valid(&bitmap, 0);
    if (!valid) {
        if (encoded_length != ENCODED_NULL_LENGTH) {
            return false;
        }
        if (cursor != record_size) {
            return false;
        }
        *valid_flag_out = 0;
        value_out.clear();
        return true;
    }

    if (encoded_length < 0) {
        return false;
    }
    if (static_cast<std::size_t>(encoded_length) > record_size - cursor) {
        return false;
    }

    value_out.assign(reinterpret_cast<const char *>(record_ptr + cursor),
                     static_cast<std::size_t>(encoded_length));
    cursor += static_cast<std::size_t>(encoded_length);

    if (cursor != record_size) {
        return false;
    }

    *valid_flag_out = 1;
    return true;
}

inline bool decode_multi_record(const std::vector<std::uint8_t> &payload_bytes,
                                const std::vector<std::int64_t> &payload_offsets,
                                const std::size_t state_index,
                                std::vector<std::int64_t> &fixed_values_out,
                                std::vector<std::int64_t> &fixed_valids_out,
                                std::vector<std::string> &encoded_values_out,
                                std::vector<std::int64_t> &encoded_valids_out) {
    const std::size_t key_count = fixed_values_out.size() + encoded_values_out.size();
    if (key_count == 0) {
        fixed_valids_out.clear();
        encoded_valids_out.clear();
        return false;
    }
    if (fixed_values_out.size() != fixed_valids_out.size() ||
        encoded_values_out.size() != encoded_valids_out.size()) {
        return false;
    }

    const std::uint8_t *record_ptr = nullptr;
    std::size_t record_size = 0;
    if (!decode_record_bounds(payload_bytes, payload_offsets, state_index, record_ptr, record_size)) {
        return false;
    }

    const std::size_t bitmap_bytes = bitmap_size_bytes(key_count);
    const std::size_t fixed_section_bytes = fixed_values_out.size() * sizeof(std::int64_t);
    if (record_size < bitmap_bytes + fixed_section_bytes) {
        return false;
    }

    std::size_t cursor = 0;
    const std::uint8_t *bitmap = record_ptr;
    cursor += bitmap_bytes;

    for (std::size_t i = 0; i < fixed_values_out.size(); ++i) {
        std::int64_t stored_value = 0;
        if (!read_pod(record_ptr, record_size, cursor, stored_value)) {
            return false;
        }
        const bool valid = bitmap_is_valid(bitmap, i);
        fixed_valids_out[i] = valid ? 1 : 0;
        fixed_values_out[i] = valid ? stored_value : 0;
    }

    for (std::size_t i = 0; i < encoded_values_out.size(); ++i) {
        const std::size_t key_index = fixed_values_out.size() + i;
        const bool valid = bitmap_is_valid(bitmap, key_index);

        std::int32_t encoded_length = 0;
        if (!read_pod(record_ptr, record_size, cursor, encoded_length)) {
            return false;
        }

        if (!valid) {
            if (encoded_length != ENCODED_NULL_LENGTH) {
                return false;
            }
            encoded_valids_out[i] = 0;
            encoded_values_out[i].clear();
            continue;
        }

        if (encoded_length < 0) {
            return false;
        }
        if (static_cast<std::size_t>(encoded_length) > record_size - cursor) {
            return false;
        }

        encoded_valids_out[i] = 1;
        encoded_values_out[i].assign(reinterpret_cast<const char *>(record_ptr + cursor),
                                     static_cast<std::size_t>(encoded_length));
        cursor += static_cast<std::size_t>(encoded_length);
    }

    if (cursor != record_size) {
        return false;
    }

    return true;
}

}  // namespace group_key_codec
}  // namespace opteryx