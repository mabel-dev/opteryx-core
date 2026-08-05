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
 *     11 = array   12 = int128 (DECIMAL128)
 *     13 = uint8   14 = uint16   15 = uint32   16 = uint64  (E33, plain — exact
 *       declared width, never widened, unlike the int32->int64 convention above)
 *     17 = uint8 dict   18 = uint16 dict   19 = uint32 dict   20 = uint64 dict
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

// ── ByteSink: one writer, two modes ─────────────────────────────────────────
// A serialize pass runs over a ByteSink. With buf == nullptr it COUNTS (advance
// only bumps pos); with buf set it WRITES into that buffer. Size and write use
// the EXACT SAME code path, so the computed size and the bytes written can never
// diverge — the only way to break it is to forget an `if (p)` guard around a
// raw-pointer write loop, which null-derefs on the very first count pass (loud,
// caught immediately) rather than corrupting memory.
//
// This lets a worker compute the exact serialized size, reserve precisely that
// many bytes from the MemoryPool, and serialize directly into the reserved
// region — eliminating the intermediate heap vector + the commit() memcpy.
struct ByteSink {
    uint8_t* buf;   // nullptr → count-only mode
    size_t   pos;   // bytes written (write mode) / would-be written (count mode)

    // Return a write pointer for the next n bytes (nullptr in count mode) and
    // advance the position. Callers writing through the returned pointer MUST
    // guard with `if (p)`.
    inline uint8_t* advance(size_t n) {
        uint8_t* p = buf ? buf + pos : nullptr;
        pos += n;
        return p;
    }
};

// ── helpers ────────────────────────────────────────────────────────────────

static inline void write_u8(ByteSink& out, uint8_t v) {
    uint8_t* p = out.advance(1);
    if (p) *p = v;
}

static inline void write_u32(ByteSink& out, uint32_t v) {
    uint8_t* p = out.advance(4);
    if (p) std::memcpy(p, &v, 4);
}

static inline void write_bytes(ByteSink& out, const void* src, size_t n) {
    if (n == 0) return;
    uint8_t* p = out.advance(n);
    if (p) std::memcpy(p, src, n);
}

static inline uint8_t code_width_for(size_t dict_size) {
    if (dict_size <= 256)   return 1;
    if (dict_size <= 65536) return 2;
    return 4;
}

// Pack plain int32 codes into code_width bytes each.
static inline void pack_codes(ByteSink& out,
                               const std::vector<int32_t>& codes,
                               uint8_t cw) {
    uint8_t* dst = out.advance(codes.size() * cw);
    if (!dst) return;
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

static inline void write_null_bitmap(ByteSink& out,
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

static void serialize_rle_int_as_int64(ByteSink& out,
                                       const DecodedColumn& col) {
    write_u8(out, 1);  // TAG_INT64
    write_u32(out, static_cast<uint32_t>(col.num_rows));
    write_null_bitmap(out, col);  // empty: rle path is non-nullable

    uint32_t data_len = static_cast<uint32_t>(col.num_rows) * 8;
    write_u32(out, data_len);
    uint8_t* raw = out.advance(data_len);
    if (raw) {
        // `raw` points at the current ByteSink offset (after a tag byte + u32
        // headers), so it is NOT 8-byte aligned. A typed `int64_t*` store would
        // fault on architectures that trap unaligned access (RISC-V); memcpy is
        // unaligned-safe and compiles to the same store on x86/ARM.
        size_t off = 0;
        const size_t n_runs = col.rle_run_lengths.size();
        for (size_t r = 0; r < n_runs; ++r) {
            const int64_t v = col.rle_int64_values[r];
            const int32_t cnt = col.rle_run_lengths[r];
            for (int32_t j = 0; j < cnt; ++j)
                std::memcpy(raw + (off + j) * sizeof(int64_t), &v, sizeof(int64_t));
            off += static_cast<size_t>(cnt);
        }
    }
}

// E33: exact-width analogue of serialize_rle_int_as_int64, for both signed and
// unsigned narrow columns. col.rle_int64_values holds the correct value in either
// domain (zero-extended for unsigned — decode_column.cpp's is_unsigned branch
// never sign-extends it; sign-extended for signed), and its low elem_bytes are
// that value at the declared width. So this only narrows to elem_bytes (1/2/4/8)
// instead of always expanding to a fixed 8-byte int64 slot.
static void serialize_rle_int_as_narrow(ByteSink& out, const DecodedColumn& col,
                                        int elem_bytes, uint8_t tag) {
    write_u8(out, tag);
    write_u32(out, static_cast<uint32_t>(col.num_rows));
    write_null_bitmap(out, col);  // empty: rle path is non-nullable

    uint32_t data_len = static_cast<uint32_t>(col.num_rows) * static_cast<uint32_t>(elem_bytes);
    write_u32(out, data_len);
    uint8_t* raw = out.advance(data_len);
    if (raw) {
        size_t off = 0;
        const size_t n_runs = col.rle_run_lengths.size();
        for (size_t r = 0; r < n_runs; ++r) {
            const uint64_t v = static_cast<uint64_t>(col.rle_int64_values[r]);
            const int32_t cnt = col.rle_run_lengths[r];
            for (int32_t j = 0; j < cnt; ++j)
                std::memcpy(raw + (off + j) * elem_bytes, &v, static_cast<size_t>(elem_bytes));
            off += static_cast<size_t>(cnt);
        }
    }
}

static void serialize_rle_float_as_float64(ByteSink& out,
                                           const DecodedColumn& col) {
    write_u8(out, 4);  // TAG_FLOAT64
    write_u32(out, static_cast<uint32_t>(col.num_rows));
    write_null_bitmap(out, col);  // empty: rle path is non-nullable

    uint32_t data_len = static_cast<uint32_t>(col.num_rows) * 8;
    write_u32(out, data_len);
    uint8_t* raw = out.advance(data_len);
    if (raw) {
        // Unaligned-safe store (see serialize_rle_int_as_int64): `raw` is not
        // 8-byte aligned, so a typed `double*` store would fault on RISC-V.
        size_t off = 0;
        const size_t n_runs = col.rle_run_lengths.size();
        for (size_t r = 0; r < n_runs; ++r) {
            const double v = col.rle_float64_values[r];
            const int32_t cnt = col.rle_run_lengths[r];
            for (int32_t j = 0; j < cnt; ++j)
                std::memcpy(raw + (off + j) * sizeof(double), &v, sizeof(double));
            off += static_cast<size_t>(cnt);
        }
    }
}

static void serialize_rle_string_as_dict(ByteSink& out,
                                         const DecodedColumn& col) {
    // Preserve RLE-encoded strings as TAG_STR_DICT (6): runs are the dict
    // entries, per-row codes index the run.  This keeps data_length == num_runs
    // << num_rows through the pipeline — never expand compressed data.
    const size_t n_runs = col.rle_run_lengths.size();
    const uint32_t dict_size = static_cast<uint32_t>(n_runs);
    const uint32_t num_rows  = static_cast<uint32_t>(col.num_rows);

    write_u8(out, 6);  // TAG_STR_DICT
    write_u32(out, num_rows);
    write_null_bitmap(out, col);  // empty: rle path is non-nullable

    uint8_t cw = code_width_for(dict_size);
    write_u32(out, dict_size);
    write_u8(out, cw);
    write_u8(out, 0);  // is_sorted: RLE runs are in occurrence order, not sorted

    uint32_t codes_len = num_rows * cw;
    write_u32(out, codes_len);

    // Per-row codes: every row in run r gets code r.
    uint8_t* dst = out.advance(codes_len);
    if (dst) {
        uint32_t row = 0;
        for (uint32_t r = 0; r < dict_size; ++r) {
            const int32_t cnt = col.rle_run_lengths[r];
            for (int32_t j = 0; j < cnt; ++j) {
                if (cw == 1) {
                    dst[row] = static_cast<uint8_t>(r);
                } else if (cw == 2) {
                    uint16_t v = static_cast<uint16_t>(r);
                    std::memcpy(dst + row * 2, &v, 2);
                } else {
                    std::memcpy(dst + row * 4, &r, 4);
                }
                ++row;
            }
        }
    }

    // Sentinel-terminated offsets: dict_size+1 int32_t values.
    write_u32(out, dict_size + 1);
    for (uint32_t r = 0; r < dict_size; ++r) {
        int32_t tmp = static_cast<int32_t>(col.rle_str_offsets[r]);
        write_bytes(out, &tmp, 4);
    }
    {
        int32_t sentinel = static_cast<int32_t>(col.rle_str_arena.size());
        write_bytes(out, &sentinel, 4);
    }

    // Arena
    write_bytes(out, col.rle_str_arena.data(), col.rle_str_arena.size());
}

// ── per-type serializers ────────────────────────────────────────────────────

// ── numeric dict serializer ─────────────────────────────────────────────────
// Emits: tag, num_rows, null_bitmap, dict_size, code_width,
//        codes_len, codes[], values_len, values[].
// dict_values must be a contiguous buffer of dict_size elements of width value_stride.

static void serialize_numeric_dict(ByteSink& out,
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
    write_u8(out, col.dict_ordered ? 1 : 0);  // is_sorted (sorted-dictionary hint)

    uint32_t codes_len = static_cast<uint32_t>(col.num_rows) * cw;
    write_u32(out, codes_len);

    if (!col.dict_codes_array.empty()) {
        // Fast path: pre-packed codes array already has the right layout — copy directly.
        write_bytes(out, col.dict_codes_array.data(), codes_len);
    } else {
        // Sparse path: dict_indices contains only non-null entries; null rows get code 0.
        uint8_t* dst = out.advance(codes_len);
        if (dst) {
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
    }

    uint32_t values_len = dict_size * value_stride;
    write_u32(out, values_len);
    write_bytes(out, dict_values, values_len);
}

static void serialize_int64(ByteSink& out, const DecodedColumn& col) {
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

static void serialize_int32(ByteSink& out, const DecodedColumn& col) {
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

// E33: exact-width integer dispatcher — mirrors serialize_int64/serialize_int32's
// RLE/dict/plain branching, but always preserves the exact declared width (never
// widens, unlike the int32->int64 convention those two use). Handles both
// physical carriers: "int32" (UINT8/16/32 and INT8/16/32 all arrive this way —
// Parquet has no int8/int16 physical storage) and "int64" (UINT64).
//
// Signedness-agnostic: keeping the low elem_bytes of the source value is correct
// for both domains. Unsigned sources (int32_values / dict_int32_values /
// rle_int64_values / int64_values / dict_int64_values) already hold the correct
// zero-extended magnitude — decode_column.cpp's is_unsigned branch never
// sign-extends them — and signed sources hold the sign-extended value, whose low
// elem_bytes are exactly its two's-complement form at the declared width. Either
// way this only narrows, never re-derives a value.
static void serialize_narrow_int(ByteSink& out, const DecodedColumn& col,
                                 int elem_bytes, uint8_t plain_tag, uint8_t dict_tag) {
    const bool src_is_32 = (col.type == "int32");

    if (!col.rle_int64_values.empty()) {
        serialize_rle_int_as_narrow(out, col, elem_bytes, plain_tag);
        return;
    }

    const bool has_dict = src_is_32 ? !col.dict_int32_values.empty()
                                     : !col.dict_int64_values.empty();
    if ((!col.dict_indices.empty() || !col.dict_codes_array.empty()) && has_dict) {
        const size_t dsz = src_is_32 ? col.dict_int32_values.size() : col.dict_int64_values.size();
        std::vector<uint8_t> narrowed(dsz * static_cast<size_t>(elem_bytes));
        for (size_t k = 0; k < dsz; ++k) {
            const uint64_t v = src_is_32
                ? static_cast<uint64_t>(static_cast<uint32_t>(col.dict_int32_values[k]))
                : static_cast<uint64_t>(col.dict_int64_values[k]);
            std::memcpy(narrowed.data() + k * elem_bytes, &v, static_cast<size_t>(elem_bytes));
        }
        serialize_numeric_dict(out, dict_tag, col, narrowed.data(),
                               static_cast<uint32_t>(dsz), static_cast<uint32_t>(elem_bytes));
        return;
    }

    write_u8(out, plain_tag);
    write_u32(out, static_cast<uint32_t>(col.num_rows));
    write_null_bitmap(out, col);
    const size_t count = src_is_32 ? col.int32_values.size() : col.int64_values.size();
    std::vector<uint8_t> narrowed(count * static_cast<size_t>(elem_bytes));
    for (size_t i = 0; i < count; ++i) {
        const uint64_t v = src_is_32
            ? static_cast<uint64_t>(static_cast<uint32_t>(col.int32_values[i]))
            : static_cast<uint64_t>(col.int64_values[i]);
        std::memcpy(narrowed.data() + i * elem_bytes, &v, static_cast<size_t>(elem_bytes));
    }
    uint32_t data_len = static_cast<uint32_t>(narrowed.size());
    write_u32(out, data_len);
    write_bytes(out, narrowed.data(), data_len);
}

static void serialize_float32(ByteSink& out, const DecodedColumn& col) {
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

static void serialize_float64(ByteSink& out, const DecodedColumn& col) {
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

// TAG 12: int128 (DECIMAL128, FLBA width 9..16).
// Wire format: tag(1) num_rows(4) null_bitmap_len(4) [null_bitmap] precision(1) scale(1)
//              data_len(4) [int128_values] — the compact payload stores K present values
//              (same convention as TAG_INT64); the deserialiser scatters to positional.
// precision and scale are carried so the deserialiser can attach the LogicalType directly,
// without a schema-driven coerce pass (DECIMAL128 vectors cannot be reinterpreted from i64).
static void serialize_int128(ByteSink& out, const DecodedColumn& col,
                              uint8_t precision, uint8_t scale) {
    write_u8(out, 12);   // TAG_INT128
    write_u32(out, static_cast<uint32_t>(col.num_rows));
    write_null_bitmap(out, col);
    write_u8(out, precision);
    write_u8(out, scale);
    uint32_t data_len = static_cast<uint32_t>(col.int128_values.size()) * 16;
    write_u32(out, data_len);
    write_bytes(out, col.int128_values.data(), data_len);
}

static void serialize_bool(ByteSink& out, const DecodedColumn& col) {
    write_u8(out, 5);
    write_u32(out, static_cast<uint32_t>(col.num_rows));
    write_null_bitmap(out, col);

    // DRAKEN_BOOL is bit-packed (1 bit/value, LSB-first) — enforce the contract
    // here at the producer rather than shipping one byte per value. boolean_values
    // holds the K present (non-null) values compactly (parquet omits null rows from
    // the value stream); we pack those K bits. For a non-nullable column K == num_rows
    // so the packed buffer is already positional; for a nullable column the
    // deserialiser bit-scatters these K bits to their row positions.
    const size_t k = col.boolean_values.size();
    const uint32_t data_len = static_cast<uint32_t>((k + 7) / 8);
    write_u32(out, data_len);
    uint8_t* dst = out.advance(data_len);
    if (dst) {
        std::memset(dst, 0, data_len);   // zero-fill: unset bits must read as false
        for (size_t i = 0; i < k; ++i) {
            if (col.boolean_values[i] & 1)
                dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
    }
}

static void serialize_string_dict(ByteSink& out, const DecodedColumn& col) {
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
    write_u8(out, col.dict_ordered ? 1 : 0);  // is_sorted (sorted-dictionary hint)

    uint32_t codes_len = static_cast<uint32_t>(col.num_rows) * cw;
    write_u32(out, codes_len);

    if (!col.dict_codes_array.empty()) {
        // Fast path: pre-packed codes array already has the right layout — copy directly.
        write_bytes(out, col.dict_codes_array.data(), codes_len);
    } else {
        // Validity-aware path: null rows get code 0, non-null rows advance di_idx.
        uint8_t* dst = out.advance(codes_len);
        if (dst) {
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
    }

    // Sentinel-terminated offsets: dict_size+1 int32_t values.
    // col.string_dict_offsets[i] = byte start of entry i in the arena (uint32_t).
    // sentinel = total arena size.
    uint32_t offsets_count = dict_size + 1;
    write_u32(out, offsets_count);
    // Use memcpy for each 4-byte write: the buffer is not guaranteed to be
    // 4-byte aligned, and reinterpret_cast<int32_t*> would trigger UBSAN
    // "store to misaligned address".
    uint8_t* dst_off = out.advance(static_cast<size_t>(offsets_count) * 4);
    if (dst_off) {
        for (uint32_t i = 0; i < dict_size; ++i) {
            int32_t tmp = static_cast<int32_t>(col.string_dict_offsets[i]);
            std::memcpy(dst_off + i * 4, &tmp, sizeof(int32_t));
        }
        int32_t tmp = static_cast<int32_t>(col.string_dict_arena.size());
        std::memcpy(dst_off + dict_size * 4, &tmp, sizeof(int32_t));
    }

    // Arena
    write_bytes(out, col.string_dict_arena.data(), col.string_dict_arena.size());
}

static void serialize_string_plain(ByteSink& out, const DecodedColumn& col) {
    // Fallback: dense string arena values (no dict). Wire format unchanged:
    // per-value u32 length + bytes.
    write_u8(out, 7);
    write_u32(out, static_cast<uint32_t>(col.num_rows));
    write_null_bitmap(out, col);

    uint32_t n = static_cast<uint32_t>(col.string_lens.size());
    write_u32(out, n);
    for (uint32_t i = 0; i < n; ++i) {
        const uint32_t len = static_cast<uint32_t>(col.string_lens[i]);
        write_u32(out, len);
        write_bytes(out, col.string_arena.data() + col.string_offsets[i], len);
    }
}

// ── list (array) column serializer ─────────────────────────────────────────
// TAG_ARRAY = 11
//
// Layout:
//   uint8_t   tag = 11
//   uint32_t  num_rows          (outer row count, e.g. 357)
//   uint32_t  list_null_bmap_len
//   uint8_t[list_null_bmap_len] list_null_bmap  (bit i=1: row i has non-null list)
//   uint8_t   child_type_tag    (CHILD_STRING=6, CHILD_INT64=1, CHILD_INT32=2,
//                                 CHILD_FLOAT32=3, CHILD_FLOAT64=4, CHILD_BOOL=5)
//   uint32_t  child_count       (total child slots, e.g. 869)
//   int32_t[(num_rows+1)]       offsets         (Arrow-style child start indices)
//   uint32_t  child_null_bmap_len (0 if no null child elements)
//   uint8_t[child_null_bmap_len]  child_null_bmap
//   child body, shape depends on child_type_tag:
//     CHILD_STRING: for each child slot i in 0..child_count-1:
//                     uint32_t len; uint8_t[len] bytes (0 bytes for null elements)
//     CHILD_BOOL:   uint8_t[(child_count+7)/8]  bit-packed values, LSB-first
//                     (matches DRAKEN_BOOL's in-memory contract directly; null
//                      slots read as 0 but are masked out by child_null_bmap)
//     otherwise (fixed-width numeric): uint8_t[child_count * elem_size]
//                     packed native-endian values, one elem_size slot per child
//                     (null slots are zero-filled but still occupy a slot)

static const uint8_t CHILD_INT64   = 1;
static const uint8_t CHILD_INT32   = 2;
static const uint8_t CHILD_FLOAT32 = 3;
static const uint8_t CHILD_FLOAT64 = 4;
static const uint8_t CHILD_BOOL    = 5;
static const uint8_t CHILD_STRING  = 6;
static const uint8_t CHILD_UINT64  = 7;   // unsigned int leaf, widened to 64-bit
static const uint8_t CHILD_ARRAY   = 8;   // nested list child (recursive block)

static inline int32_t _decode_packed_parquet_code(const uint8_t* arr, size_t idx, uint8_t width) {
    if (width == 1) return static_cast<int32_t>(arr[idx]);
    if (width == 2) return static_cast<int32_t>(arr[2*idx] | (static_cast<uint32_t>(arr[2*idx+1]) << 8));
    return static_cast<int32_t>(arr[4*idx] | (static_cast<uint32_t>(arr[4*idx+1]) << 8) |
                                (static_cast<uint32_t>(arr[4*idx+2]) << 16) |
                                (static_cast<uint32_t>(arr[4*idx+3]) << 24));
}

// Pack a 0/1-per-entry validity vector into an Arrow bitmap (bit set = valid)
// and write it as [u32 byte-length][bytes]. Used for every list level's own
// row/list validity.
static inline void write_validity_bmap(ByteSink& out, const std::vector<uint8_t>& valid) {
    uint32_t n     = static_cast<uint32_t>(valid.size());
    uint32_t bytes = (n + 7) / 8;
    std::vector<uint8_t> bmap(bytes, 0);
    for (uint32_t i = 0; i < n; ++i)
        if (valid[i]) bmap[i >> 3] |= static_cast<uint8_t>(1 << (i & 7));
    write_u32(out, bytes);
    if (bytes) write_bytes(out, bmap.data(), bytes);
}

// Serialize a list column of arbitrary nesting depth D (= max_rep_level) into
// the recursive TAG_ARRAY wire format. Each nesting level is a block carrying
// its own validity + offsets; a level whose children are themselves lists uses
// CHILD_ARRAY and is followed by the child level's block (its num_rows equals
// this level's child_count). The innermost level uses a scalar child tag and
// carries the leaf body. Unsigned int leaves are widened to CHILD_UINT64.
static void serialize_list_column(ByteSink& out, const DecodedColumn& col) {
    const int32_t D        = col.max_rep_level;
    const int32_t max_def  = col.max_def_level;
    const size_t  n_levels = col.rep_levels.size();

    // Leaf (innermost element) wire tag from the element's physical type. An
    // unsigned int leaf is widened to 64-bit unsigned (CHILD_UINT64) so the full
    // range survives — matching the Cython Path-A reader (_make_array_vector).
    uint8_t leaf_tag;
    if (col.type == "string" || col.type == "byte_array") {
        leaf_tag = CHILD_STRING;
    } else if ((col.type == "int64" || col.type == "int32") && col.is_unsigned) {
        leaf_tag = CHILD_UINT64;
    } else if (col.type == "int64") {
        leaf_tag = CHILD_INT64;
    } else if (col.type == "int32") {
        leaf_tag = CHILD_INT32;
    } else if (col.type == "float32") {
        leaf_tag = CHILD_FLOAT32;
    } else if (col.type == "float64") {
        leaf_tag = CHILD_FLOAT64;
    } else if (col.type == "boolean") {
        leaf_tag = CHILD_BOOL;
    } else {
        throw std::runtime_error(
            "ARRAY column with element type '" + col.type +
            "' is not supported (only string / (u)int32 / (u)int64 / float32 / "
            "float64 / boolean list elements can be read)");
    }

    if (D < 1) {
        throw std::runtime_error(
            "ARRAY column has max_rep_level=" + std::to_string(D) +
            " (expected >= 1 for a list column)");
    }
    if (max_def != 2 * D + 1) {
        throw std::runtime_error(
            "ARRAY column has an unsupported list level scheme (max_rep_level=" +
            std::to_string(D) + ", max_def_level=" + std::to_string(max_def) +
            "; expected max_def_level=" + std::to_string(2 * D + 1) +
            " for all-nullable nesting)");
    }

    const bool use_codes = !col.dict_codes_array.empty();
    const bool use_arena = !col.string_dict_arena.empty();
    const bool use_dix   = !col.dict_indices.empty();

    if (leaf_tag == CHILD_BOOL && (use_codes || use_dix)) {
        throw std::runtime_error("ARRAY<boolean> with dictionary-encoded child values is not supported");
    }

    // Per-level structural state (levels 1..D). level_offsets[k][p] is the start
    // index of level-k entry p's children (into level k+1, or the leaf for k==D);
    // level_valid[k][p] is 1 if that entry is a non-null list. level_valid[k].size()
    // doubles as the running count of level-k entries created so far.
    std::vector<std::vector<int32_t>> level_offsets(D + 1);
    std::vector<std::vector<uint8_t>> level_valid(D + 1);

    // Leaf value buffers (extraction identical to the flat/D==1 path).
    struct ChildEntry { const uint8_t* ptr; uint32_t len; bool valid; };
    std::vector<ChildEntry> children;    // CHILD_STRING
    std::vector<uint8_t>    num_bytes;   // fixed-width numeric (incl widened uint64)
    std::vector<uint8_t>    bool_flags;  // CHILD_BOOL
    std::vector<uint8_t>    leaf_valid;  // 0/1 per leaf slot

    const uint32_t elem_size = (leaf_tag == CHILD_INT64 || leaf_tag == CHILD_FLOAT64 ||
                                leaf_tag == CHILD_UINT64) ? 8
                             : (leaf_tag == CHILD_INT32 || leaf_tag == CHILD_FLOAT32) ? 4
                             : 0;

    // Load one fixed-width leaf value into raw[0..elem_size). `idx` indexes the
    // plain value stream (from_dict=false) or the dict value stream (true).
    auto load_numeric = [&](uint8_t* raw, int32_t idx, bool from_dict) {
        switch (leaf_tag) {
            case CHILD_INT64:
                std::memcpy(raw, from_dict ? &col.dict_int64_values[idx] : &col.int64_values[idx], 8);
                break;
            case CHILD_INT32:
                std::memcpy(raw, from_dict ? &col.dict_int32_values[idx] : &col.int32_values[idx], 4);
                break;
            case CHILD_FLOAT32:
                std::memcpy(raw, from_dict ? &col.dict_float32_values[idx] : &col.float32_values[idx], 4);
                break;
            case CHILD_UINT64: {
                uint64_t u;
                if (col.type == "int64") {
                    int64_t v = from_dict ? col.dict_int64_values[idx] : col.int64_values[idx];
                    std::memcpy(&u, &v, 8);          // reinterpret the 64 bits as unsigned
                } else {                              // int32 physical → zero-extend
                    int32_t v = from_dict ? col.dict_int32_values[idx] : col.int32_values[idx];
                    u = static_cast<uint64_t>(static_cast<uint32_t>(v));
                }
                std::memcpy(raw, &u, 8);
                break;
            }
            default: // CHILD_FLOAT64
                std::memcpy(raw, from_dict ? &col.dict_float64_values[idx] : &col.float64_values[idx], 8);
                break;
        }
    };

    int32_t val_idx = 0;    // index into dict_indices / string / plain numeric streams
    int32_t leaf_n  = 0;    // running count of leaf slots (present or null), all leaf types

    for (size_t i = 0; i < n_levels; ++i) {
        const int32_t rep = col.rep_levels[i];
        const int32_t def = col.def_levels[i];

        // Structural: open a new list at each depth from rep+1 downward, as deep
        // as `def` defines. Record each new entry's child-start offset + validity.
        bool has_leaf = true;
        for (int32_t k = rep + 1; k <= D; ++k) {
            const int32_t child_start =
                (k < D) ? static_cast<int32_t>(level_valid[k + 1].size())
                        : leaf_n;
            level_offsets[k].push_back(child_start);
            if (def >= 2 * k - 1) {          // list k present
                level_valid[k].push_back(1);
                if (def < 2 * k) { has_leaf = false; break; }   // present but empty
            } else {                          // list k null
                level_valid[k].push_back(0);
                has_leaf = false;
                break;
            }
        }
        if (!has_leaf) continue;

        // Leaf element under the innermost (level-D) list: def==max_def present,
        // def==max_def-1 null. (has_leaf implies def in {max_def-1, max_def}.)
        if (def == max_def) {
            if (leaf_tag == CHILD_STRING) {
                ChildEntry ce;
                ce.valid = true;
                if (use_codes) {
                    int32_t code = _decode_packed_parquet_code(col.dict_codes_array.data(), i, col.code_width);
                    ce.ptr = col.string_dict_arena.data() + col.string_dict_offsets[code];
                    ce.len = static_cast<uint32_t>(col.string_dict_lens[code]);
                } else if (use_dix && use_arena) {
                    int32_t code = col.dict_indices[val_idx++];
                    ce.ptr = col.string_dict_arena.data() + col.string_dict_offsets[code];
                    ce.len = static_cast<uint32_t>(col.string_dict_lens[code]);
                } else if (use_dix) {
                    const int32_t code = col.dict_indices[val_idx++];
                    ce.ptr = col.string_arena.data() + col.string_offsets[code];
                    ce.len = static_cast<uint32_t>(col.string_lens[code]);
                } else {
                    ce.ptr = col.string_arena.data() + col.string_offsets[val_idx];
                    ce.len = static_cast<uint32_t>(col.string_lens[val_idx]);
                    ++val_idx;
                }
                children.push_back(ce);
            } else if (leaf_tag == CHILD_BOOL) {
                bool_flags.push_back(col.boolean_values[val_idx++] & 1);
                leaf_valid.push_back(1);
            } else {
                uint8_t raw[8];
                if (use_codes) {
                    int32_t code = _decode_packed_parquet_code(col.dict_codes_array.data(), i, col.code_width);
                    load_numeric(raw, code, true);
                } else if (use_dix) {
                    load_numeric(raw, col.dict_indices[val_idx++], true);
                } else {
                    load_numeric(raw, val_idx++, false);
                }
                num_bytes.insert(num_bytes.end(), raw, raw + elem_size);
                leaf_valid.push_back(1);
            }
        } else {   // def == max_def - 1 : null leaf element within a present list
            // A null leaf has NO slot in any decoded value stream — Parquet stores
            // only defined values, so the string arena / dict_indices / the numeric
            // buffers contain non-null values only. val_idx must NOT advance here;
            // advancing it consumes the next defined value's slot, shifting every
            // subsequent element in the list.
            if (leaf_tag == CHILD_STRING) {
                children.push_back({nullptr, 0, false});
            } else if (leaf_tag == CHILD_BOOL) {
                bool_flags.push_back(0);
                leaf_valid.push_back(0);
            } else {
                num_bytes.insert(num_bytes.end(), elem_size, 0);
                leaf_valid.push_back(0);
            }
        }
        ++leaf_n;   // exactly one leaf slot (present or null) was appended above
    }

    // Terminal offset per level.
    for (int32_t k = 1; k <= D; ++k)
        level_offsets[k].push_back(
            (k < D) ? static_cast<int32_t>(level_valid[k + 1].size())
                    : leaf_n);

    const uint32_t leaf_count = static_cast<uint32_t>(
        leaf_tag == CHILD_STRING ? children.size() : leaf_valid.size());

    // Leaf child null bitmap only when there are null leaf elements.
    bool has_null_children = false;
    if (leaf_tag == CHILD_STRING) {
        for (const auto& ce : children) if (!ce.valid) { has_null_children = true; break; }
    } else {
        for (uint8_t v : leaf_valid) if (!v) { has_null_children = true; break; }
    }

    // Emit: outer generic header, then one block per nesting level.
    write_u8(out, 11);  // TAG_ARRAY
    write_u32(out, static_cast<uint32_t>(level_valid[1].size()));   // num_rows
    write_validity_bmap(out, level_valid[1]);                       // list_null_bmap

    for (int32_t k = 1; k <= D; ++k) {
        if (k < D) {
            // This level's entries each hold a nested child list.
            write_u8(out, CHILD_ARRAY);
            write_u32(out, static_cast<uint32_t>(level_valid[k + 1].size()));   // child_count
            write_bytes(out, level_offsets[k].data(), level_offsets[k].size() * sizeof(int32_t));
            // Inner block header (its num_rows == child_count, known to the reader).
            write_validity_bmap(out, level_valid[k + 1]);
        } else {
            // Innermost (leaf) level.
            write_u8(out, leaf_tag);
            write_u32(out, leaf_count);                                         // child_count
            write_bytes(out, level_offsets[k].data(), level_offsets[k].size() * sizeof(int32_t));
            if (has_null_children) {
                uint32_t bmap_bytes = (leaf_count + 7) / 8;
                std::vector<uint8_t> child_bmap(bmap_bytes, 0);
                if (leaf_tag == CHILD_STRING) {
                    for (uint32_t i = 0; i < leaf_count; ++i)
                        if (children[i].valid) child_bmap[i >> 3] |= static_cast<uint8_t>(1 << (i & 7));
                } else {
                    for (uint32_t i = 0; i < leaf_count; ++i)
                        if (leaf_valid[i]) child_bmap[i >> 3] |= static_cast<uint8_t>(1 << (i & 7));
                }
                write_u32(out, bmap_bytes);
                write_bytes(out, child_bmap.data(), bmap_bytes);
            } else {
                write_u32(out, 0);
            }
            if (leaf_tag == CHILD_STRING) {
                for (const auto& ce : children) {
                    write_u32(out, ce.len);
                    if (ce.len > 0) write_bytes(out, ce.ptr, ce.len);
                }
            } else if (leaf_tag == CHILD_BOOL) {
                uint32_t packed_bytes = (leaf_count + 7) / 8;
                uint8_t* dst = out.advance(packed_bytes);
                if (dst) {
                    std::memset(dst, 0, packed_bytes);
                    for (uint32_t i = 0; i < leaf_count; ++i)
                        if (bool_flags[i]) dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
                }
            } else {
                write_bytes(out, num_bytes.data(), num_bytes.size());
            }
        }
    }
}

// ── dispatch core (runs over a ByteSink: count or write) ─────────────────────

static void serialize_core(const DecodedColumn& col,
                           ByteSink& out,
                           uint8_t decimal_precision,
                           uint8_t decimal_scale) {
    // List columns (rep_levels present) take precedence over the flat byte_array path.
    if (!col.rep_levels.empty()) {
        serialize_list_column(out, col);
        return;
    }

    const std::string& t = col.type;

    if (t == "int128") {
        serialize_int128(out, col, decimal_precision, decimal_scale);
    } else if (col.is_unsigned && (t == "int64" || t == "int32")) {
        // E33: tags 13-16 plain (uint8/16/32/64), 17-20 dict.
        switch (col.int_bit_width) {
            case 8:  serialize_narrow_int(out, col, 1, 13, 17); break;
            case 16: serialize_narrow_int(out, col, 2, 14, 18); break;
            case 32: serialize_narrow_int(out, col, 4, 15, 19); break;
            default: serialize_narrow_int(out, col, 8, 16, 20); break;
        }
    } else if (t == "int64") {
        serialize_int64(out, col);
    } else if (t == "int32" && !col.is_decimal) {
        // E33: signed narrow — tags 21-23 plain (int8/16/32), 24-26 dict. A bare
        // physical int32 (no IntType annotation, width 0) IS a 32-bit signed
        // column, so it serializes at its own width rather than widening to
        // int64. This MUST stay in lockstep with direct_kind_for's int32 arm:
        // whether a column takes the direct or the pool path depends on encoding,
        // not type, so a disagreement here would make the same column arrive as
        // INT32 or INT64 depending on how it happened to be stored.
        //
        // int32-backed DECIMAL is excluded: it is not an int32 column but a
        // DECIMAL that happens to use int32 storage, and the consumer's
        // schema-driven coercion reads it from an INT64 vector (the historic
        // tpch Q01 decimal trap). It keeps the widening path below.
        switch (col.int_bit_width) {
            case 8:  serialize_narrow_int(out, col, 1, 21, 24); break;
            case 16: serialize_narrow_int(out, col, 2, 22, 25); break;
            default: serialize_narrow_int(out, col, 4, 23, 26); break;
        }
    } else if (t == "int32") {
        serialize_int32(out, col);   // int32-backed DECIMAL: widen, then coerce
    } else if (t == "float32") {
        serialize_float32(out, col);
    } else if (t == "float64") {
        serialize_float64(out, col);
    } else if (t == "boolean") {
        serialize_bool(out, col);
    } else if (t == "string" || t == "byte_array") {
        if (!col.rle_str_lens.empty()) {
            // RLE path: non-nullable dict-encoded byte_array column.
            // Preserve as dict — runs are the entries, per-row codes index them.
            serialize_rle_string_as_dict(out, col);
        } else if ((!col.dict_indices.empty() || !col.dict_codes_array.empty()) &&
            !col.string_dict_lens.empty()) {
            serialize_string_dict(out, col);
        } else if (!col.dict_indices.empty() && !col.string_lens.empty()) {
            // dict_indices present but the dict table lives in the dense string
            // arena (old-style producer) — the triples share a layout, so the
            // promotion is a straight copy into the dict fields.
            DecodedColumn promoted = col;
            promoted.string_dict_arena   = col.string_arena;
            promoted.string_dict_offsets = col.string_offsets;
            promoted.string_dict_lens    = col.string_lens;
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

// ── public entry points ─────────────────────────────────────────────────────

/**
 * Exact serialized byte length of `col`. Runs the serializer in count-only mode
 * (no buffer), so it cannot disagree with what serialize_into() actually writes.
 */
static inline size_t serialized_size(const DecodedColumn& col,
                                     uint8_t decimal_precision = 38,
                                     uint8_t decimal_scale = 0) {
    ByteSink counter{nullptr, 0};
    serialize_core(col, counter, decimal_precision, decimal_scale);
    return counter.pos;
}

/**
 * Serialize `col` directly into `dst`, which MUST be at least
 * serialized_size(col, ...) bytes. Returns the number of bytes written (== the
 * size). Called from C++ worker threads — no GIL, no allocator, no copy: the
 * destination is the MemoryPool-reserved region itself.
 */
static inline size_t serialize_decoded_column_into(const DecodedColumn& col,
                                                   uint8_t* dst,
                                                   uint8_t decimal_precision = 38,
                                                   uint8_t decimal_scale = 0) {
    ByteSink writer{dst, 0};
    serialize_core(col, writer, decimal_precision, decimal_scale);
    return writer.pos;
}

/**
 * Vector-producing variant retained for non-pool callers (tests, tools).
 * Two passes (size then write); production uses the *_into path above.
 */
static inline void serialize_decoded_column(const DecodedColumn& col,
                                            std::vector<uint8_t>& out,
                                            uint8_t decimal_precision = 38,
                                            uint8_t decimal_scale = 0) {
    out.resize(serialized_size(col, decimal_precision, decimal_scale));
    serialize_decoded_column_into(col, out.data(), decimal_precision, decimal_scale);
}

} // namespace rugo
