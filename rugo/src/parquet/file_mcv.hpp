#pragma once
// rugo/src/parquet/file_mcv.hpp — the `rugo.mcv.<column>` file-level statistic:
// the column's most frequent values and how often each occurs.
//
// The NDV alone prices `col = x` at rows / ndv, as if every value were equally
// common. On a skewed column the values a query names are the common ones:
// JOB's movie_info.info holds 2.72M distinct values over 14.8M rows (5.5 rows
// each), yet 'Horror' occurs 30,801 times. The writer already hashes every
// value (XXH64 of its PLAIN bytes, the bloom filter's hash), so it counts those
// hashes per row group and states the file's heaviest ones in FileMetaData
// key_value_metadata (parquet.thrift field 5):
//
//   key   "rugo.mcv." + the column's TOP-LEVEL name
//   value "h32c32:" + base64 of up to kFileMcvMaxEntries entries of 8 bytes:
//         uint32 LE  low 32 bits of the value's XXH64 (seed 0) over its PLAIN
//                    bytes, as the bloom filter hashes it
//         uint32 LE  rows holding that value in this file, a LOWER BOUND
//                    (row groups keep only their heaviest hashes, so a value
//                    outside one group's list is not counted for that group)
//         ordered by count descending, then hash ascending.
//
// 96 entries = 768 bytes = 1,024 base64 characters. Only values above twice the
// file's uniform rate (non-null rows / NDV) are stated, so a uniform or key
// column carries no key. A column without the key means NOT STATED, never "no
// frequent values". Readers trust the key only in a file whose `created_by`
// identifies rugo — the same rule as `rugo.ndv` and `rugo.sum`.
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace rugo_parquet {

inline constexpr std::string_view kFileMcvKeyPrefix = "rugo.mcv.";
inline constexpr std::string_view kFileMcvTag = "h32c32:";
inline constexpr size_t kFileMcvMaxEntries = 96;

struct FileMcvEntry {
    uint32_t hash;   // low 32 bits of the value's XXH64
    uint32_t count;  // rows holding it (a lower bound)
};

namespace file_mcv_detail {

inline constexpr char kAlphabet[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

inline int decode_char(char c) {
    if (c >= 'A' && c <= 'Z') return c - 'A';
    if (c >= 'a' && c <= 'z') return c - 'a' + 26;
    if (c >= '0' && c <= '9') return c - '0' + 52;
    if (c == '+') return 62;
    if (c == '/') return 63;
    return -1;
}

inline void put_u32(std::string& out, uint32_t v) {
    for (int k = 0; k < 4; ++k) out.push_back(static_cast<char>((v >> (8 * k)) & 0xFF));
}

inline uint32_t get_u32(const unsigned char* p) {
    return static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
           (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
}

}  // namespace file_mcv_detail

// Standard base64 with '=' padding.
inline std::string format_file_mcv(const std::vector<FileMcvEntry>& entries) {
    using namespace file_mcv_detail;
    std::string raw;
    raw.reserve(entries.size() * 8);
    for (const FileMcvEntry& e : entries) {
        put_u32(raw, e.hash);
        put_u32(raw, e.count);
    }
    std::string out(kFileMcvTag);
    out.reserve(kFileMcvTag.size() + (raw.size() + 2) / 3 * 4);
    size_t i = 0;
    for (; i + 3 <= raw.size(); i += 3) {
        const uint32_t n = (static_cast<uint32_t>(static_cast<unsigned char>(raw[i])) << 16) |
                           (static_cast<uint32_t>(static_cast<unsigned char>(raw[i + 1])) << 8) |
                           static_cast<uint32_t>(static_cast<unsigned char>(raw[i + 2]));
        out.push_back(kAlphabet[(n >> 18) & 63]);
        out.push_back(kAlphabet[(n >> 12) & 63]);
        out.push_back(kAlphabet[(n >> 6) & 63]);
        out.push_back(kAlphabet[n & 63]);
    }
    const size_t rest = raw.size() - i;
    if (rest > 0) {
        uint32_t n = static_cast<uint32_t>(static_cast<unsigned char>(raw[i])) << 16;
        if (rest == 2) n |= static_cast<uint32_t>(static_cast<unsigned char>(raw[i + 1])) << 8;
        out.push_back(kAlphabet[(n >> 18) & 63]);
        out.push_back(kAlphabet[(n >> 12) & 63]);
        out.push_back(rest == 2 ? kAlphabet[(n >> 6) & 63] : '=');
        out.push_back('=');
    }
    return out;
}

// False (output untouched) for anything that is not the tag followed by valid
// padded base64 of 1..kFileMcvMaxEntries whole 8-byte entries.
inline bool parse_file_mcv(std::string_view text, std::vector<FileMcvEntry>* entries) {
    using namespace file_mcv_detail;
    if (text.substr(0, kFileMcvTag.size()) != kFileMcvTag) return false;
    const std::string_view b64 = text.substr(kFileMcvTag.size());
    if (b64.empty() || b64.size() % 4 != 0) return false;
    std::string raw;
    raw.reserve(b64.size() / 4 * 3);
    for (size_t i = 0; i < b64.size(); i += 4) {
        const bool last = i + 4 == b64.size();
        int v[4];
        int pad = 0;
        for (int k = 0; k < 4; ++k) {
            const char c = b64[i + k];
            if (c == '=') {
                // padding only in the last quad's final one or two places
                if (!last || k < 2) return false;
                v[k] = 0;
                ++pad;
                continue;
            }
            if (pad > 0) return false;
            v[k] = decode_char(c);
            if (v[k] < 0) return false;
        }
        const uint32_t n = (static_cast<uint32_t>(v[0]) << 18) | (static_cast<uint32_t>(v[1]) << 12) |
                           (static_cast<uint32_t>(v[2]) << 6) | static_cast<uint32_t>(v[3]);
        raw.push_back(static_cast<char>((n >> 16) & 0xFF));
        if (pad < 2) raw.push_back(static_cast<char>((n >> 8) & 0xFF));
        if (pad < 1) raw.push_back(static_cast<char>(n & 0xFF));
    }
    if (raw.empty() || raw.size() % 8 != 0 || raw.size() / 8 > kFileMcvMaxEntries) return false;
    std::vector<FileMcvEntry> out(raw.size() / 8);
    const unsigned char* p = reinterpret_cast<const unsigned char*>(raw.data());
    for (size_t k = 0; k < out.size(); ++k) {
        out[k].hash = get_u32(p + 8 * k);
        out[k].count = get_u32(p + 8 * k + 4);
    }
    *entries = std::move(out);
    return true;
}

}  // namespace rugo_parquet
