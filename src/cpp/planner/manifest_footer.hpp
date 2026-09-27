// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// See the License at http://www.apache.org/licenses/LICENSE-2.0
// Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

// src/cpp/planner/manifest_footer.hpp — a parquet file's footer statistics
// (rugo's AggColumnStat, aggregated across row groups) into a NativeManifest
// cell (native plan graph Q8, M-c).
//
// Bounds are decoded the way rugo's `decode_value` decodes them for every other
// statistics consumer, so the cell classifies a bound as the planner always has:
//   - DECIMAL(P,S): the unscaled integer (little-endian int32/int64, big-endian
//     two's complement bytes) at scale S;
//   - int32 / int64: an integer, unsigned when the annotation says uint<w> (a
//     uint64 above INT64_MAX is UINT64) - temporal columns included, as their
//     stored int;
//   - float32 / float64: a float;
//   - byte_array / fixed_len_byte_array: text when the annotation is a string
//     one (or, for json / array<...>, when the bytes are printable UTF-8),
//     opaque bytes otherwise;
//   - int96: the formatted text rugo produces; boolean: a bool; anything else:
//     the bytes as hex text.
// Every bound also gets its ordinal key (draken/ops/ordinalize.h) where one is
// defined: integers, floats, decimals and booleans by value, text and bytes by
// their 8-byte prefix.

#pragma once

#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>

#include "metadata.hpp"      // AggColumnStat, StatsLogicalIsUnsigned
#include "ops/ordinalize.h"
#include "planner/native_manifest.hpp"

namespace opteryx::planner {

namespace footer_detail {

inline bool logical_is_string(const std::string& logical) {
    return logical == "varchar" || logical == "UTF8" || logical == "JSON" || logical == "BSON" ||
           logical == "ENUM" || logical.rfind("array<string", 0) == 0 ||
           logical.rfind("array<varchar", 0) == 0;
}

// Valid UTF-8 with no control characters (tab, newline, carriage return aside)
// and no DEL - rugo's `_text_is_printable` over a replacement-free decode.
inline bool printable_utf8(const std::string& bytes) {
    size_t i = 0, n = bytes.size();
    while (i < n) {
        unsigned char c = static_cast<unsigned char>(bytes[i]);
        size_t width = c < 0x80 ? 1 : (c >> 5) == 0x6 ? 2 : (c >> 4) == 0xE ? 3 : (c >> 3) == 0x1E ? 4 : 0;
        if (width == 0 || i + width > n) return false;
        for (size_t k = 1; k < width; ++k) {
            if ((static_cast<unsigned char>(bytes[i + k]) & 0xC0) != 0x80) return false;
        }
        if (width == 1 && ((c < 32 && c != '\t' && c != '\n' && c != '\r') || c == 127)) return false;
        i += width;
    }
    return true;
}

inline std::string hex(const std::string& bytes) {
    static const char digits[] = "0123456789abcdef";
    std::string out;
    out.reserve(bytes.size() * 2);
    for (unsigned char c : bytes) {
        out.push_back(digits[c >> 4]);
        out.push_back(digits[c & 0xF]);
    }
    return out;
}

// The scale S of a "decimal(P,S)" annotation; false when it does not parse.
inline bool decimal_scale(const std::string& logical, int& scale) {
    if (logical.rfind("decimal(", 0) != 0 || logical.back() != ')') return false;
    size_t comma = logical.find(',');
    if (comma == std::string::npos) return false;
    std::string digits = logical.substr(comma + 1, logical.size() - comma - 2);
    size_t start = digits.find_first_not_of(' ');
    if (start == std::string::npos) return false;
    digits = digits.substr(start);
    if (digits.empty()) return false;
    for (char c : digits) {
        if (c < '0' || c > '9') return false;
    }
    scale = std::stoi(digits);
    return true;
}

// One bound's decoded value and ordinal into the footer's min or max.
inline void decode_bound(const std::string& physical, const std::string& logical,
                         const std::string& raw, Bounds& cell, bool is_min) {
    int64_t& ordinal = is_min ? cell.min_ordinal : cell.max_ordinal;
    int64_t& as_int = is_min ? cell.min_int : cell.max_int;
    double& as_double = is_min ? cell.min_double : cell.max_double;
    int32_t& as_scale = is_min ? cell.min_scale : cell.max_scale;
    std::string& as_text = is_min ? cell.min_text : cell.max_text;
    DecodedTag& tag = is_min ? cell.min_tag : cell.max_tag;
    const bool is_string = logical_is_string(logical);
    const bool prefer_text = logical == "json" || logical.rfind("array<", 0) == 0;
    const bool byte_array = physical == "byte_array" || physical == "fixed_len_byte_array";

    if (raw.empty()) {
        if (byte_array && (is_string || prefer_text)) {
            tag = DECODED_TEXT;
        } else {
            tag = DECODED_BYTES;
        }
        as_text.clear();
        ordinal = draken::ops::ordinalize_scalar_bytes8(nullptr, 0);
        return;
    }

    int scale = 0;
    if (decimal_scale(logical, scale)) {
        int64_t unscaled = 0;
        if (physical == "int32" && raw.size() >= 4) {
            int32_t v;
            std::memcpy(&v, raw.data(), 4);
            unscaled = v;
        } else if (physical == "int64" && raw.size() >= 8) {
            std::memcpy(&unscaled, raw.data(), 8);
        } else if (byte_array && raw.size() <= 8) {
            // big-endian two's complement
            uint64_t bits = (static_cast<unsigned char>(raw[0]) & 0x80) ? ~0ULL : 0ULL;
            for (unsigned char c : raw) bits = (bits << 8) | c;
            unscaled = static_cast<int64_t>(bits);
        } else {
            tag = DECODED_OTHER;   // wider than 64 bits: ordered by nothing here
            return;
        }
        tag = DECODED_DECIMAL;
        as_int = unscaled;
        as_scale = scale;
        as_double = static_cast<double>(unscaled) * std::pow(10.0, -scale);
        ordinal = unscaled;
        return;
    }

    const bool is_unsigned = StatsLogicalIsUnsigned(logical);
    if (physical == "int32" && raw.size() >= 4) {
        if (is_unsigned) {
            uint32_t v;
            std::memcpy(&v, raw.data(), 4);
            as_int = static_cast<int64_t>(v);
        } else {
            int32_t v;
            std::memcpy(&v, raw.data(), 4);
            as_int = v;
        }
        tag = DECODED_INT64;
        ordinal = as_int;
        return;
    }
    if (physical == "int64" && raw.size() >= 8) {
        if (is_unsigned) {
            uint64_t v;
            std::memcpy(&v, raw.data(), 8);
            ordinal = draken::ops::ordinalize_scalar_u64(v);
            as_int = static_cast<int64_t>(v);   // UINT64 carries the bits
            tag = v > static_cast<uint64_t>(INT64_MAX) ? DECODED_UINT64 : DECODED_INT64;
            return;
        }
        std::memcpy(&as_int, raw.data(), 8);
        ordinal = as_int;
        tag = DECODED_INT64;
        return;
    }
    if (physical == "float32" && raw.size() >= 4) {
        float v;
        std::memcpy(&v, raw.data(), 4);
        as_double = v;
        tag = DECODED_DOUBLE;
        ordinal = draken::ops::ordinalize_scalar_f64(as_double);
        return;
    }
    if (physical == "float64" && raw.size() >= 8) {
        std::memcpy(&as_double, raw.data(), 8);
        tag = DECODED_DOUBLE;
        ordinal = draken::ops::ordinalize_scalar_f64(as_double);
        return;
    }
    if (byte_array) {
        const uint8_t* data = reinterpret_cast<const uint8_t*>(raw.data());
        ordinal = draken::ops::ordinalize_scalar_bytes8(data, static_cast<uint32_t>(raw.size()));
        as_text = raw;
        if (is_string || (prefer_text && physical == "byte_array" && printable_utf8(raw))) {
            tag = DECODED_TEXT;
        } else {
            tag = DECODED_BYTES;
        }
        return;
    }
    if (physical == "int96" && raw.size() == 12) {
        int64_t nanos;
        uint32_t julian_day;
        std::memcpy(&nanos, raw.data(), 8);
        std::memcpy(&julian_day, raw.data() + 8, 4);
        // rugo: date + " " + f"{seconds:02d}:{(micros/1e6):.6f}"
        int64_t days = static_cast<int64_t>(julian_day) - 2440588;
        int64_t z = days + 719468;
        int64_t era = (z >= 0 ? z : z - 146096) / 146097;
        int64_t doe = z - era * 146097;
        int64_t yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
        int64_t y = yoe + era * 400;
        int64_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        int64_t mp = (5 * doy + 2) / 153;
        int64_t d = doy - (153 * mp + 2) / 5 + 1;
        int64_t m = mp < 10 ? mp + 3 : mp - 9;
        if (m <= 2) ++y;
        int64_t seconds = nanos / 1000000000;
        int64_t micros = (nanos % 1000000000) / 1000;
        char buffer[96];
        std::snprintf(buffer, sizeof(buffer), "%04lld-%02lld-%02lld %02lld:%.6f",
                      static_cast<long long>(y), static_cast<long long>(m), static_cast<long long>(d),
                      static_cast<long long>(seconds), static_cast<double>(micros) / 1e6);
        as_text = buffer;
        tag = DECODED_OTHER;
        return;
    }
    if (physical == "boolean") {
        as_int = static_cast<unsigned char>(raw[0]) != 0 ? 1 : 0;
        tag = DECODED_BOOL;
        ordinal = as_int;
        return;
    }
    as_text = hex(raw);
    tag = DECODED_OTHER;
}

}  // namespace footer_detail

// A footer column statistic into the cell's footer statistics - each unknown
// where the footer does not know it. The manifest's own statistics are left as
// they are.
inline void apply_footer_stat(const AggColumnStat& stat, ManifestCell& cell) {
    FooterStats& footer = cell.footer;
    footer = FooterStats();
    if (stat.has_min) footer_detail::decode_bound(stat.physical_type, stat.logical_type, stat.min_bytes, footer.bounds, true);
    if (stat.has_max) footer_detail::decode_bound(stat.physical_type, stat.logical_type, stat.max_bytes, footer.bounds, false);
    footer.null_count = stat.null_count_complete ? stat.null_count : kUnknown;
    footer.distinct_count = stat.distinct_count >= 0 ? stat.distinct_count : kUnknown;
    footer.uncompressed_size = stat.total_uncompressed_size > 0 ? stat.total_uncompressed_size : kUnknown;
}

}  // namespace opteryx::planner
