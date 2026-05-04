#pragma once
#include "decode.hpp"
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <vector>

/**
 * Binary IPC format for DecodedColumn.
 *
 * The serialized blob is committed to MemoryPool as the handoff from C++
 * worker threads to the Cython engine. No Python, no JSON, no struct module.
 *
 * Layout (all integers little-endian, native on x86/ARM):
 *
 *   uint8_t  type_tag
 *     1 = int64   2 = int32   3 = float32   4 = float64   5 = bool
 *     6 = string (dict+arena)   7 = string (plain, no dict)
 *     8 = int64 dict   9 = float32 dict   10 = float64 dict
 *
 *   uint32_t num_rows
 *   uint32_t null_bitmap_len   (0 if column is non-nullable)
 *   uint8_t[null_bitmap_len]   null_bitmap  (Arrow-style validity bits)
 *
 *   Fixed-width body (type_tag 1-5):
 *     uint32_t data_len
 *     uint8_t[data_len]   raw values (native-endian)
 *
 *   String dict body (type_tag 6):
 *     uint32_t dict_size
 *     uint8_t  code_width      (1, 2, or 4)
 *     uint32_t codes_len       (== num_rows * code_width)
 *     uint8_t[codes_len]       packed per-row codes
 *     uint32_t offsets_count   (== dict_size + 1, sentinel-terminated)
 *     int32_t[offsets_count]   start byte offsets into arena
 *     uint8_t[offsets[dict_size]] arena bytes
 *
 *   String plain body (type_tag 7):
 *     uint32_t num_strings
 *     for each string:
 *       uint32_t len
 *       uint8_t[len] bytes
 *
 *   Numeric dict body (type_tag 8/9/10):
 *     uint32_t dict_size
 *     uint8_t  code_width      (1, 2, or 4)
 *     uint32_t codes_len       (== num_rows * code_width)
 *     uint8_t[codes_len]       packed per-row codes
 *     uint32_t values_len      (== dict_size * sizeof(T))
 *     uint8_t[values_len]      dict values as raw T[] (int64/float/double)
 */

namespace rugo {

// ── helpers ────────────────────────────────────────────────────────────────

static inline void write_u8(std::vector<uint8_t>& out, uint8_t v) {
    out.push_back(v);
}

static inline void write_u32(std::vector<uint8_t>& out, uint32_t v) {
    out.resize(out.size() + 4);
    std::memcpy(out.data() + out.size() - 4, &v, 4);
}

static inline void write_bytes(std::vector<uint8_t>& out, const void* src, size_t n) {
    if (n == 0) return;
    size_t pos = out.size();
    out.resize(pos + n);
    std::memcpy(out.data() + pos, src, n);
}

static inline uint8_t code_width_for(size_t dict_size) {
    if (dict_size <= 256)   return 1;
    if (dict_size <= 65536) return 2;
    return 4;
}

// Pack plain int32 codes into code_width bytes each.
static inline void pack_codes(std::vector<uint8_t>& out,
                               const std::vector<int32_t>& codes,
                               uint8_t cw) {
    size_t pos = out.size();
    out.resize(pos + codes.size() * cw);
    uint8_t* dst = out.data() + pos;
    for (size_t i = 0; i < codes.size(); ++i) {
        uint32_t c = static_cast<uint32_t>(codes[i]);
        if (cw == 1) {
            dst[i] = static_cast<uint8_t>(c);
        } else if (cw == 2) {
            uint16_t v = static_cast<uint16_t>(c);
            std::memcpy(dst + i * 2, &v, 2);
        } else {
            std::memcpy(dst + i * 4, &c, 4);
        }
    }
}

// ── null bitmap ────────────────────────────────────────────────────────────

static inline void write_null_bitmap(std::vector<uint8_t>& out,
                                     const DecodedColumn& col) {
    uint32_t nbytes = 0;
    if (!col.valid_bits.empty()) {
        nbytes = static_cast<uint32_t>(col.valid_bits.size());
    }
    write_u32(out, nbytes);
    if (nbytes > 0) {
        write_bytes(out, col.valid_bits.data(), nbytes);
    }
}

// ── RLE-output expanders ────────────────────────────────────────────────────
// Non-nullable dict-encoded columns take the "rle skip-dense" path in
// DecodeColumnFromChunk and produce rle_*_values + rle_run_lengths instead of
// the usual int32_values / dict_indices / dict_codes_array layout. The IPC
// format is dense, so we expand here at serialize time.

static void serialize_rle_int_as_int64(std::vector<uint8_t>& out,
                                       const DecodedColumn& col) {
    write_u8(out, 1);  // TAG_INT64
    write_u32(out, static_cast<uint32_t>(col.num_rows));
    write_null_bitmap(out, col);  // empty: rle path is non-nullable

    uint32_t data_len = static_cast<uint32_t>(col.num_rows) * 8;
    write_u32(out, data_len);
    size_t pos = out.size();
    out.resize(pos + data_len);
    int64_t* dst = reinterpret_cast<int64_t*>(out.data() + pos);

    size_t off = 0;
    const size_t n_runs = col.rle_run_lengths.size();
    for (size_t r = 0; r < n_runs; ++r) {
        const int64_t v = col.rle_int64_values[r];
        const int32_t cnt = col.rle_run_lengths[r];
        for (int32_t j = 0; j < cnt; ++j) dst[off + j] = v;
        off += static_cast<size_t>(cnt);
    }
}

static void serialize_rle_float_as_float64(std::vector<uint8_t>& out,
                                           const DecodedColumn& col) {
    write_u8(out, 4);  // TAG_FLOAT64
    write_u32(out, static_cast<uint32_t>(col.num_rows));
    write_null_bitmap(out, col);  // empty: rle path is non-nullable

    uint32_t data_len = static_cast<uint32_t>(col.num_rows) * 8;
    write_u32(out, data_len);
    size_t pos = out.size();
    out.resize(pos + data_len);
    double* dst = reinterpret_cast<double*>(out.data() + pos);

    size_t off = 0;
    const size_t n_runs = col.rle_run_lengths.size();
    for (size_t r = 0; r < n_runs; ++r) {
        const double v = col.rle_float64_values[r];
        const int32_t cnt = col.rle_run_lengths[r];
        for (int32_t j = 0; j < cnt; ++j) dst[off + j] = v;
        off += static_cast<size_t>(cnt);
    }
}

static void serialize_rle_string_as_plain(std::vector<uint8_t>& out,
                                          const DecodedColumn& col) {
    // Expand RLE strings to plain length-prefixed string list (tag=7).
    write_u8(out, 7);  // TAG_STR_PLAIN
    write_u32(out, static_cast<uint32_t>(col.num_rows));
    write_null_bitmap(out, col);  // empty: rle path is non-nullable

    write_u32(out, static_cast<uint32_t>(col.num_rows));  // num_strings
    const size_t n_runs = col.rle_run_lengths.size();
    for (size_t r = 0; r < n_runs; ++r) {
        const uint32_t off  = col.rle_str_offsets[r];
        const int32_t  slen = col.rle_str_lens[r];
        const int32_t  cnt  = col.rle_run_lengths[r];
        for (int32_t j = 0; j < cnt; ++j) {
            write_u32(out, static_cast<uint32_t>(slen));
            if (slen > 0) {
                write_bytes(out, col.rle_str_arena.data() + off,
                            static_cast<size_t>(slen));
            }
        }
    }
}

// ── per-type serializers ────────────────────────────────────────────────────

// ── numeric dict serializer ─────────────────────────────────────────────────
// Emits: tag, num_rows, null_bitmap, dict_size, code_width,
//        codes_len, codes[], values_len, values[].
// dict_values must be a contiguous buffer of dict_size elements of width value_stride.

static void serialize_numeric_dict(std::vector<uint8_t>& out,
                                   uint8_t tag,
                                   const DecodedColumn& col,
                                   const void* dict_values,
                                   uint32_t dict_size,
                                   uint32_t value_stride) {
    write_u8(out, tag);
    write_u32(out, static_cast<uint32_t>(col.num_rows));
    write_null_bitmap(out, col);

    uint8_t cw = col.code_width > 0 ? col.code_width : code_width_for(dict_size);
    write_u32(out, dict_size);
    write_u8(out, cw);

    uint32_t codes_len = static_cast<uint32_t>(col.num_rows) * cw;
    write_u32(out, codes_len);

    if (!col.dict_codes_array.empty()) {
        // Fast path: pre-packed codes array already has the right layout — copy directly.
        write_bytes(out, col.dict_codes_array.data(), codes_len);
    } else {
        // Sparse path: dict_indices contains only non-null entries; null rows get code 0.
        size_t pos = out.size();
        out.resize(pos + codes_len);
        uint8_t* dst = out.data() + pos;
        int32_t di_idx = 0;
        for (int32_t row = 0; row < col.num_rows; ++row) {
            bool is_null = !col.valid_bits.empty() &&
                           !((col.valid_bits[row >> 3] >> (row & 7)) & 1);
            uint32_t c = is_null ? 0 : static_cast<uint32_t>(col.dict_indices[di_idx++]);
            if (cw == 1) {
                dst[row] = static_cast<uint8_t>(c);
            } else if (cw == 2) {
                uint16_t v = static_cast<uint16_t>(c);
                std::memcpy(dst + row * 2, &v, 2);
            } else {
                std::memcpy(dst + row * 4, &c, 4);
            }
        }
    }

    uint32_t values_len = dict_size * value_stride;
    write_u32(out, values_len);
    write_bytes(out, dict_values, values_len);
}

static void serialize_int64(std::vector<uint8_t>& out, const DecodedColumn& col) {
    if (!col.rle_int64_values.empty()) {
        serialize_rle_int_as_int64(out, col);
        return;
    }
    if ((!col.dict_indices.empty() || !col.dict_codes_array.empty()) &&
        !col.dict_int64_values.empty()) {
        serialize_numeric_dict(out, 8, col,
                               col.dict_int64_values.data(),
                               static_cast<uint32_t>(col.dict_int64_values.size()),
                               sizeof(int64_t));
        return;
    }
    write_u8(out, 1);
    write_u32(out, static_cast<uint32_t>(col.num_rows));
    write_null_bitmap(out, col);
    uint32_t data_len = static_cast<uint32_t>(col.int64_values.size()) * 8;
    write_u32(out, data_len);
    write_bytes(out, col.int64_values.data(), data_len);
}

static void serialize_int32(std::vector<uint8_t>& out, const DecodedColumn& col) {
    if (!col.rle_int64_values.empty()) {
        // RLE path widened int32 → int64 in C++; emit as plain int64.
        serialize_rle_int_as_int64(out, col);
        return;
    }
    if ((!col.dict_indices.empty() || !col.dict_codes_array.empty()) &&
        !col.dict_int32_values.empty()) {
        // Widen int32 dictionary to int64 — Draken has no Int32 dict vector type.
        std::vector<int64_t> wide(col.dict_int32_values.size());
        for (size_t i = 0; i < col.dict_int32_values.size(); ++i)
            wide[i] = static_cast<int64_t>(col.dict_int32_values[i]);
        serialize_numeric_dict(out, 8, col,
                               wide.data(),
                               static_cast<uint32_t>(wide.size()),
                               sizeof(int64_t));
        return;
    }
    write_u8(out, 2);
    write_u32(out, static_cast<uint32_t>(col.num_rows));
    write_null_bitmap(out, col);
    uint32_t data_len = static_cast<uint32_t>(col.int32_values.size()) * 4;
    write_u32(out, data_len);
    write_bytes(out, col.int32_values.data(), data_len);
}

static void serialize_float32(std::vector<uint8_t>& out, const DecodedColumn& col) {
    if (!col.rle_float64_values.empty()) {
        // RLE path widened float32 → float64 in C++; emit as plain float64.
        serialize_rle_float_as_float64(out, col);
        return;
    }
    if ((!col.dict_indices.empty() || !col.dict_codes_array.empty()) &&
        !col.dict_float32_values.empty()) {
        serialize_numeric_dict(out, 9, col,
                               col.dict_float32_values.data(),
                               static_cast<uint32_t>(col.dict_float32_values.size()),
                               sizeof(float));
        return;
    }
    write_u8(out, 3);
    write_u32(out, static_cast<uint32_t>(col.num_rows));
    write_null_bitmap(out, col);
    uint32_t data_len = static_cast<uint32_t>(col.float32_values.size()) * 4;
    write_u32(out, data_len);
    write_bytes(out, col.float32_values.data(), data_len);
}

static void serialize_float64(std::vector<uint8_t>& out, const DecodedColumn& col) {
    if (!col.rle_float64_values.empty()) {
        serialize_rle_float_as_float64(out, col);
        return;
    }
    if ((!col.dict_indices.empty() || !col.dict_codes_array.empty()) &&
        !col.dict_float64_values.empty()) {
        serialize_numeric_dict(out, 10, col,
                               col.dict_float64_values.data(),
                               static_cast<uint32_t>(col.dict_float64_values.size()),
                               sizeof(double));
        return;
    }
    write_u8(out, 4);
    write_u32(out, static_cast<uint32_t>(col.num_rows));
    write_null_bitmap(out, col);
    uint32_t data_len = static_cast<uint32_t>(col.float64_values.size()) * 8;
    write_u32(out, data_len);
    write_bytes(out, col.float64_values.data(), data_len);
}

static void serialize_bool(std::vector<uint8_t>& out, const DecodedColumn& col) {
    write_u8(out, 5);
    write_u32(out, static_cast<uint32_t>(col.num_rows));
    write_null_bitmap(out, col);

    uint32_t data_len = static_cast<uint32_t>(col.boolean_values.size());
    write_u32(out, data_len);
    write_bytes(out, col.boolean_values.data(), data_len);
}

static void serialize_string_dict(std::vector<uint8_t>& out, const DecodedColumn& col) {
    // Uses the flat arena (string_dict_arena / string_dict_offsets / string_dict_lens)
    // plus either dict_codes_array (pre-packed, nullable path) or dict_indices (sparse path).
    write_u8(out, 6);
    write_u32(out, static_cast<uint32_t>(col.num_rows));
    write_null_bitmap(out, col);

    uint32_t dict_size = static_cast<uint32_t>(col.string_dict_lens.size());
    uint8_t cw = col.code_width > 0 ? col.code_width
                                     : code_width_for(dict_size);
    write_u32(out, dict_size);
    write_u8(out, cw);

    uint32_t codes_len = static_cast<uint32_t>(col.num_rows) * cw;
    write_u32(out, codes_len);

    if (!col.dict_codes_array.empty()) {
        // Fast path: pre-packed codes array already has the right layout — copy directly.
        write_bytes(out, col.dict_codes_array.data(), codes_len);
    } else {
        // Validity-aware path: null rows get code 0, non-null rows advance di_idx.
        size_t pos = out.size();
        out.resize(pos + codes_len);
        uint8_t* dst = out.data() + pos;
        int32_t di_idx = 0;
        for (int32_t row = 0; row < col.num_rows; ++row) {
            bool is_null = !col.valid_bits.empty() &&
                           !((col.valid_bits[row >> 3] >> (row & 7)) & 1);
            uint32_t c = is_null ? 0
                                 : static_cast<uint32_t>(col.dict_indices[di_idx++]);
            if (cw == 1) {
                dst[row] = static_cast<uint8_t>(c);
            } else if (cw == 2) {
                uint16_t v = static_cast<uint16_t>(c);
                std::memcpy(dst + row * 2, &v, 2);
            } else {
                std::memcpy(dst + row * 4, &c, 4);
            }
        }
    }

    // Sentinel-terminated offsets: dict_size+1 int32_t values.
    // col.string_dict_offsets[i] = byte start of entry i in the arena (uint32_t).
    // sentinel = total arena size.
    uint32_t offsets_count = dict_size + 1;
    write_u32(out, offsets_count);
    size_t pos = out.size();
    out.resize(pos + offsets_count * 4);
    int32_t* dst_off = reinterpret_cast<int32_t*>(out.data() + pos);
    for (uint32_t i = 0; i < dict_size; ++i) {
        dst_off[i] = static_cast<int32_t>(col.string_dict_offsets[i]);
    }
    dst_off[dict_size] = static_cast<int32_t>(col.string_dict_arena.size());

    // Arena
    write_bytes(out, col.string_dict_arena.data(), col.string_dict_arena.size());
}

static void serialize_string_plain(std::vector<uint8_t>& out, const DecodedColumn& col) {
    // Fallback: plain std::string values (no dict).
    write_u8(out, 7);
    write_u32(out, static_cast<uint32_t>(col.num_rows));
    write_null_bitmap(out, col);

    uint32_t n = static_cast<uint32_t>(col.string_values.size());
    write_u32(out, n);
    for (const auto& s : col.string_values) {
        write_u32(out, static_cast<uint32_t>(s.size()));
        write_bytes(out, s.data(), s.size());
    }
}

// ── public entry point ──────────────────────────────────────────────────────

/**
 * Serialize a DecodedColumn to a flat byte buffer for MemoryPool IPC.
 * Called from C++ worker threads — no GIL, no Python, no allocator contention.
 */
static void serialize_decoded_column(const DecodedColumn& col,
                                     std::vector<uint8_t>& out) {
    out.clear();

    const std::string& t = col.type;

    if (t == "int64") {
        serialize_int64(out, col);
    } else if (t == "int32") {
        serialize_int32(out, col);
    } else if (t == "float32") {
        serialize_float32(out, col);
    } else if (t == "float64") {
        serialize_float64(out, col);
    } else if (t == "boolean") {
        serialize_bool(out, col);
    } else if (t == "string" || t == "byte_array") {
        if (!col.rle_str_lens.empty()) {
            // RLE path: non-nullable dict-encoded byte_array column.
            serialize_rle_string_as_plain(out, col);
        } else if ((!col.dict_indices.empty() || !col.dict_codes_array.empty()) &&
            !col.string_dict_lens.empty()) {
            serialize_string_dict(out, col);
        } else if (!col.dict_indices.empty() && !col.string_values.empty()) {
            // dict_indices present but old-style string_values dict — build arena
            // by promoting string_values to flat arena format first.
            DecodedColumn promoted = col;  // shallow copy is fine (vectors share data)
            size_t total = 0;
            for (const auto& s : col.string_values) total += s.size();
            promoted.string_dict_arena.reserve(total);
            promoted.string_dict_offsets.clear();
            promoted.string_dict_lens.clear();
            for (const auto& s : col.string_values) {
                uint32_t off = static_cast<uint32_t>(promoted.string_dict_arena.size());
                promoted.string_dict_offsets.push_back(off);
                promoted.string_dict_lens.push_back(static_cast<int32_t>(s.size()));
                promoted.string_dict_arena.insert(
                    promoted.string_dict_arena.end(), s.begin(), s.end());
            }
            promoted.code_width = col.code_width;
            serialize_string_dict(out, promoted);
        } else {
            // Plain (non-dict) strings
            serialize_string_plain(out, col);
        }
    } else {
        // Unknown type — emit an empty int64 placeholder so the pipeline
        // doesn't stall; the column will be empty but not fatal.
        write_u8(out, 1);
        write_u32(out, 0);
        write_u32(out, 0);
        write_u32(out, 0);
    }
}

} // namespace rugo
