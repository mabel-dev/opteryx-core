// rugo/src/parquet/column_materialize.cpp — DecodedColumn -> owned Draken buffers.
// Pure C++; see column_materialize.hpp for the contract and the error mapping.

#include "column_materialize.hpp"

#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

#include "core/alloc.h"
#include "core/string_slot.h"
#include "ops/float_ops.h"   // draken::ops::fp_canon

namespace rugo::_parquet {

namespace {

// Owned buffers: freed on a throw, released into the MaterializedColumn on success.
struct DrakenFree { void operator()(void* p) const noexcept { draken_free(p); } };
struct LibcFree   { void operator()(void* p) const noexcept { std::free(p); } };
template <typename T> using DrakenPtr = std::unique_ptr<T, DrakenFree>;

template <typename T>
DrakenPtr<T> draken_alloc(size_t count) {
    T* p = (T*)draken_malloc(count * sizeof(T));
    if (!p) throw std::bad_alloc();
    return DrakenPtr<T>(p);
}

inline uint32_t read_code(const draken::AppendBuffer<uint8_t>& arr, size_t i, uint8_t width) {
    size_t off = (size_t)i * width;
    if (width == 1) return arr[off];
    if (width == 2) return arr[off] | ((uint32_t)arr[off + 1] << 8);
    return arr[off] | ((uint32_t)arr[off + 1] << 8)
         | ((uint32_t)arr[off + 2] << 16) | ((uint32_t)arr[off + 3] << 24);
}

// Draken validity bitmap: 1 = valid, LSB-first, byte i>>3 bit i&7, sized to
// a multiple of 8 bytes (min 8). Returned all-valid; callers clear nulls.
DrakenPtr<uint8_t> alloc_validity(uint32_t length) {
    const uint32_t bm     = (length + 7u) / 8u;
    const uint32_t padded = ((bm + 7u) & ~7u);
    const size_t   vbytes = padded > 0u ? padded : 8u;
    DrakenPtr<uint8_t> v = draken_alloc<uint8_t>(vbytes);
    std::memset(v.get(), 0xFF, vbytes);
    return v;
}

// Hand the buffers to the result. A column with no null rows carries no bitmap.
template <typename T>
MaterializedColumn finish(DrakenType type, uint32_t length, DrakenPtr<T>& data,
                          DrakenPtr<uint8_t>& validity, bool has_nulls) {
    if (!has_nulls) validity.reset();
    MaterializedColumn mc;
    mc.type     = type;
    mc.length   = length;
    mc.data     = (void*)data.release();
    mc.validity = validity.release();
    return mc;
}

// ---------------------------------------------------------------------------
// DECIMAL
//
// Parquet stores decimals as INT32/INT64/FIXED_LEN_BYTE_ARRAY; rugo's decoder
// tiers by byte width into `int64_values` (width <= 8, `col.type` int64/int32)
// or `int128_values` (width 9..16, `col.type` int128). Either way the value on
// the wire ALREADY IS the signed unscaled integer at the column's scale — which
// is exactly draken's DECIMAL/DECIMAL128 storage. So there is nothing to
// convert: scatter the decoded values into a draken_malloc'd buffer and build
// the validity bitmap.
//
// This replaces a per-row Python round trip (unscaled -> Python int ->
// Decimal.scaleb(-S) -> decimal_to_unscaled() -> unscaled) that cost ~360ns/row
// — 30x a plain int64 column of the same width and the same decode telemetry.
// The round trip was an identity on the stored representation, so dropping it
// changes no answer; the precision bound it enforced is kept below as a native
// integer compare.
//
// `col.is_unsigned` is not consulted: it is set only from a "uintN"/"intN"
// logical-type annotation, which a "decimal(P,S)" column never carries, and a
// DECIMAL is signed two's complement by definition.
// ---------------------------------------------------------------------------

// Scatter the decoded unscaled values of `col` into out[0, num_rows).
// Covers the four source shapes (dict via packed codes,
// dict via compact indices, RLE runs, plain compact values). Null rows get
// 0 and their validity bit cleared; the RLE shape carries no per-row
// validity. `scatter_int` below is its plain-integer twin.
// T is int64_t (width <= 8) or __int128 (width 9..16); DictT/PlainT let the
// int64 tier read a physical int32 column without a second copy of the loop.
template <typename T, typename DictT, typename PlainT>
void scatter_unscaled(const DecodedColumn& col, int32_t num_rows,
                      const draken::AppendBuffer<DictT>& dict_vals,
                      const draken::AppendBuffer<PlainT>& plain_vals,
                      bool has_dict, bool is_rle,
                      T* out, uint8_t* validity, bool* has_nulls) {
    const bool has_v = !col.valid_bits.empty();
    if (has_dict) {
        const size_t dict_sz = dict_vals.size();
        const bool use_codes = !col.dict_codes_array.empty();
        const uint8_t cw = (col.code_width == 1 || col.code_width == 2 ||
                            col.code_width == 4) ? col.code_width : 1;
        size_t vi = 0;
        for (int32_t i = 0; i < num_rows; ++i) {
            if (has_v && !((col.valid_bits[i >> 3] >> (i & 7)) & 1)) {
                out[i] = 0;
                validity[i >> 3] &= (uint8_t)~(1u << (i & 7));
                *has_nulls = true;
                continue;
            }
            uint32_t code = use_codes
                ? read_code(col.dict_codes_array, (size_t)i, cw)
                : col.dict_indices[vi++];
            if ((size_t)code >= dict_sz)  // fail safe on a corrupt code
                throw std::invalid_argument("decimal dict code out of range");
            out[i] = (T)dict_vals[code];
        }
        return;
    }
    if (is_rle) {
        // rle_int64_values holds resolved values for both int64 and int32.
        // Runs summing past num_rows would run off the end of `out` — the
        // old list-building path raised IndexError there, so fail here too
        // rather than truncate (a short column is a silent wrong answer).
        size_t off = 0;
        for (size_t r = 0; r < col.rle_run_lengths.size(); ++r) {
            const size_t cnt = col.rle_run_lengths[r];
            if (off + cnt > (size_t)num_rows)
                throw std::invalid_argument(
                    "decimal: RLE run lengths exceed the column's row count");
            const T val = (T)col.rle_int64_values[r];
            for (size_t j = 0; j < cnt; ++j) out[off + j] = val;
            off += cnt;
        }
        return;
    }
    size_t vi = 0;
    for (int32_t i = 0; i < num_rows; ++i) {
        if (has_v && !((col.valid_bits[i >> 3] >> (i & 7)) & 1)) {
            out[i] = 0;
            validity[i >> 3] &= (uint8_t)~(1u << (i & 7));
            *has_nulls = true;
            continue;
        }
        out[i] = (T)plain_vals[vi++];
    }
}

// |unscaled| < 10^precision — the bound decimal_to_unscaled() enforced per
// value on the old Python path, kept here as an integer compare. A file
// whose values overflow their own declared precision is a file that
// contradicts itself; it fails rather than yielding a silently wrong type.
template <typename T>
void check_precision(const T* data, uint32_t length, uint8_t precision) {
    T limit = 1;
    for (int i = 0; i < (int)precision; ++i) limit *= 10;
    for (uint32_t i = 0; i < length; ++i)
        if (data[i] >= limit || data[i] <= -limit)
            throw std::overflow_error("decimal: value exceeds declared precision");
}

// ---------------------------------------------------------------------------
// INT / FLOAT
//
// These replace a per-row Python round trip (a list of PyLong/PyFloat, which
// `vector_*_from_sequence` immediately parsed back out). MEASURED on a 1M-row
// PLAIN int64 column: C++ decode 1.08 ms against 17.68 ms in that round trip.
//
// The four source shapes (dict via packed codes, dict via compact indices, RLE
// runs, plain compact values) and their null handling are preserved exactly —
// including the two behaviours that are easy to lose:
//
//   * UNSIGNED REINTERPRETATION. Unsigned columns store the raw magnitude in a
//     signed int32/int64 slot. `MidT` is the same-width UNSIGNED type, so the
//     single `(MidT)` cast reinterprets the bits exactly as the old
//     `<uint64_t><uint32_t>` casts did — a plain widening would sign-extend
//     4e9 to -294967296.
//   * NARROWING IS CHECKED, NOT TRUNCATED. `vector_int8_from_sequence` and its
//     siblings raise OverflowError("int8: value out of range") for a value that
//     does not fit the DECLARED width. `put_int` reproduces that, message for
//     message; a silent C truncation here would turn a self-contradictory file
//     into a wrong answer. The check is `if constexpr`-gated on the width pair,
//     so the int64 and uint64 tiers compile to a bare store.
//
// FLOATS ARE CANONICALISED. `vector_float{32,64}_from_sequence` canonicalises
// -0.0 to +0.0 and NaN payloads to a quiet NaN inside the nanobind constructor;
// hashing and grouping key on RAW BITS, so skipping it splits one value across
// two GROUP BY groups while `f = 0.0` matches both. Going around the constructor
// means doing it here — the same `draken::ops::fp_canon` io_pipeline.hpp applies
// on the engine path, and the trap its PRECONDITION comment warns about.
// ---------------------------------------------------------------------------

// Store `v` narrowed to OutT, rejecting what the from_sequence constructor
// rejected. The range test exists only when OutT is strictly narrower than
// MidT; every other instantiation compiles to the bare store. OutT and MidT
// always share signedness (see the dispatch below), so one comparison pair
// covers both families.
template <typename OutT, typename MidT>
inline void put_int(OutT* out, size_t i, MidT v, const char* tname) {
    if constexpr (sizeof(OutT) < sizeof(MidT)) {
        if constexpr (std::is_signed<OutT>::value) {
            if (v < (MidT)std::numeric_limits<OutT>::min())
                throw std::overflow_error(std::string(tname) + ": value out of range");
        }
        if (v > (MidT)std::numeric_limits<OutT>::max())
            throw std::overflow_error(std::string(tname) + ": value out of range");
    }
    out[i] = (OutT)v;
}

// Scatter a decoded integer column into out[0, num_rows). Mirrors
// the four source shapes. Null rows get 0 and their validity bit
// cleared; the RLE shape carries no per-row validity, matching the list
// path. RleMidT is the reinterpretation for RLE values, which are held as
// int64 for BOTH the int32 and int64 tiers — so it is uint64/int64 and does
// NOT follow MidT's width.
template <typename OutT, typename MidT, typename RleMidT,
          typename DictT, typename PlainT>
void scatter_int(const DecodedColumn& col, int32_t num_rows,
                 const draken::AppendBuffer<DictT>& dict_vals,
                 const draken::AppendBuffer<PlainT>& plain_vals,
                 bool has_dict, bool is_rle,
                 OutT* out, uint8_t* validity, bool* has_nulls,
                 const char* tname) {
    const bool has_v = !col.valid_bits.empty();
    if (has_dict) {
        const size_t dict_sz = dict_vals.size();
        const bool use_codes = !col.dict_codes_array.empty();
        const uint8_t cw = (col.code_width == 1 || col.code_width == 2 ||
                            col.code_width == 4) ? col.code_width : 1;
        size_t vi = 0;
        for (int32_t i = 0; i < num_rows; ++i) {
            if (has_v && !((col.valid_bits[i >> 3] >> (i & 7)) & 1)) {
                out[i] = 0;
                validity[i >> 3] &= (uint8_t)~(1u << (i & 7));
                *has_nulls = true;
                continue;
            }
            uint32_t code = use_codes
                ? read_code(col.dict_codes_array, (size_t)i, cw)
                : (vi < col.dict_indices.size() ? col.dict_indices[vi++] : 0xFFFFFFFFu);
            if ((size_t)code >= dict_sz)  // fail safe on a corrupt/short code stream
                throw std::invalid_argument("dictionary code out of range");
            put_int<OutT, MidT>(out, (size_t)i, (MidT)dict_vals[code], tname);
        }
        return;
    }
    if (is_rle) {
        // Runs summing past num_rows would run off the end of `out` — the
        // list path raised IndexError there, so fail rather than truncate.
        size_t off = 0;
        for (size_t r = 0; r < col.rle_run_lengths.size(); ++r) {
            const size_t cnt = col.rle_run_lengths[r];
            if (off + cnt > (size_t)num_rows)
                throw std::invalid_argument("RLE run lengths exceed the column's row count");
            const RleMidT val = (RleMidT)col.rle_int64_values[r];
            for (size_t j = 0; j < cnt; ++j)
                put_int<OutT, RleMidT>(out, off + j, val, tname);
            off += cnt;
        }
        return;
    }
    size_t vi = 0;
    const size_t avail = plain_vals.size();
    for (int32_t i = 0; i < num_rows; ++i) {
        if (has_v && !((col.valid_bits[i >> 3] >> (i & 7)) & 1)) {
            out[i] = 0;
            validity[i >> 3] &= (uint8_t)~(1u << (i & 7));
            *has_nulls = true;
            continue;
        }
        if (vi >= avail)  // value stream shorter than the valid-row count
            throw std::invalid_argument(
                "value stream shorter than the column's valid row count");
        put_int<OutT, MidT>(out, (size_t)i, (MidT)plain_vals[vi++], tname);
    }
}

template <typename OutT, bool FROM32, bool UNS>
MaterializedColumn build_int(const DecodedColumn& col, int32_t num_rows,
                             bool has_dict, bool is_rle,
                             DrakenType dtype, const char* tname) {
    using MidT = std::conditional_t<UNS,
                     std::conditional_t<FROM32, uint32_t, uint64_t>,
                     std::conditional_t<FROM32, int32_t,  int64_t>>;
    using RleMidT = std::conditional_t<UNS, uint64_t, int64_t>;
    const uint32_t length = (uint32_t)(num_rows > 0 ? num_rows : 0);
    const size_t   slots  = length > 0u ? length : 1u;

    DrakenPtr<uint8_t> validity = alloc_validity(length);
    DrakenPtr<OutT>    data     = draken_alloc<OutT>(slots);
    bool has_nulls = false;

    if constexpr (FROM32)
        scatter_int<OutT, MidT, RleMidT, int32_t, int32_t>(
            col, num_rows, col.dict_int32_values, col.int32_values,
            has_dict, is_rle, data.get(), validity.get(), &has_nulls, tname);
    else
        scatter_int<OutT, MidT, RleMidT, int64_t, int64_t>(
            col, num_rows, col.dict_int64_values, col.int64_values,
            has_dict, is_rle, data.get(), validity.get(), &has_nulls, tname);
    return finish(dtype, length, data, validity, has_nulls);
}

// Scatter a decoded float column into out[0, num_rows). Mirrors
// the four source shapes. Every value passes through fp_canon on the
// way in — see the banner above.
template <typename OutT, typename DictT, typename PlainT>
void scatter_float(const DecodedColumn& col, int32_t num_rows,
                   const draken::AppendBuffer<DictT>& dict_vals,
                   const draken::AppendBuffer<PlainT>& plain_vals,
                   bool has_dict, bool is_rle,
                   OutT* out, uint8_t* validity, bool* has_nulls) {
    const bool has_v = !col.valid_bits.empty();
    if (has_dict) {
        const size_t dict_sz = dict_vals.size();
        const bool use_codes = !col.dict_codes_array.empty();
        const uint8_t cw = (col.code_width == 1 || col.code_width == 2 ||
                            col.code_width == 4) ? col.code_width : 1;
        size_t vi = 0;
        for (int32_t i = 0; i < num_rows; ++i) {
            if (has_v && !((col.valid_bits[i >> 3] >> (i & 7)) & 1)) {
                out[i] = (OutT)0;
                validity[i >> 3] &= (uint8_t)~(1u << (i & 7));
                *has_nulls = true;
                continue;
            }
            uint32_t code = use_codes
                ? read_code(col.dict_codes_array, (size_t)i, cw)
                : (vi < col.dict_indices.size() ? col.dict_indices[vi++] : 0xFFFFFFFFu);
            if ((size_t)code >= dict_sz)
                throw std::invalid_argument("dictionary code out of range");
            out[i] = draken::ops::fp_canon((OutT)dict_vals[code]);
        }
        return;
    }
    if (is_rle) {
        size_t off = 0;
        for (size_t r = 0; r < col.rle_run_lengths.size(); ++r) {
            const size_t cnt = col.rle_run_lengths[r];
            if (off + cnt > (size_t)num_rows)
                throw std::invalid_argument("RLE run lengths exceed the column's row count");
            // rle_float64_values holds resolved values for float32 AND
            // float64; the narrowing to float is exact for a binary32 value.
            const OutT val = draken::ops::fp_canon((OutT)col.rle_float64_values[r]);
            for (size_t j = 0; j < cnt; ++j) out[off + j] = val;
            off += cnt;
        }
        return;
    }
    size_t vi = 0;
    const size_t avail = plain_vals.size();
    for (int32_t i = 0; i < num_rows; ++i) {
        if (has_v && !((col.valid_bits[i >> 3] >> (i & 7)) & 1)) {
            out[i] = (OutT)0;
            validity[i >> 3] &= (uint8_t)~(1u << (i & 7));
            *has_nulls = true;
            continue;
        }
        if (vi >= avail)
            throw std::invalid_argument(
                "value stream shorter than the column's valid row count");
        out[i] = draken::ops::fp_canon((OutT)plain_vals[vi++]);
    }
}

// ---------------------------------------------------------------------------
// BYTE_ARRAY
//
// Replaces a `vector_from_string_sequence` round trip that built one PyBytes per
// row only to have the constructor parse it straight back out. MEASURED on
// 100k rows x 4 string columns: 6.63 ms -> 1.42 ms.
//
// The output is a DENSE VARCHAR/VARBINARY column — byte-for-byte the shape the
// list path produced, including for a dict-encoded column, whose repeated values
// are expanded per row exactly as the flatten did. `is_text` is the caller's
// `_logical_is_string` verdict, kept in Cython so the String-annotation
// discriminator stays in ONE place (shared with the statistics path and the
// array leaf) rather than being re-derived here.
//
// hash32 is NOT computed: the slot field is dead (see string_slot.h), and this
// is the same `draken_build_string_slot` the engine path and the jsonl builder
// use. A column that is a downstream key gets its seed elsewhere.
// ---------------------------------------------------------------------------

// Per-row source index, resolved once for all four shapes so the two build
// passes below are shape-blind. kStrNull marks a null row.
constexpr uint32_t kStrNull = 0xFFFFFFFFu;

// Resolve row -> index into (offs, lens) for the column's shape, and hand
// back the arena those offsets address.
void str_resolve(const DecodedColumn& col, int32_t num_rows,
                 bool has_dict, bool is_rle, uint32_t* idx,
                 const uint8_t** base_out,
                 const uint32_t** offs_out, const int32_t** lens_out,
                 size_t* count_out) {
    const bool has_v = !col.valid_bits.empty();
    if (has_dict) {
        *base_out = col.string_dict_arena.data();
        *offs_out = col.string_dict_offsets.data();
        *lens_out = col.string_dict_lens.data();
        *count_out = col.string_dict_lens.size();
        const bool use_codes = !col.dict_codes_array.empty();
        const uint8_t cw = (col.code_width == 1 || col.code_width == 2 ||
                            col.code_width == 4) ? col.code_width : 1;
        size_t vi = 0;
        for (int32_t i = 0; i < num_rows; ++i) {
            if (has_v && !((col.valid_bits[i >> 3] >> (i & 7)) & 1)) {
                idx[i] = kStrNull;
                continue;
            }
            idx[i] = use_codes
                ? read_code(col.dict_codes_array, (size_t)i, cw)
                : (vi < col.dict_indices.size() ? col.dict_indices[vi++] : kStrNull);
        }
        return;
    }
    if (is_rle) {
        // Skip-dense RLE: run r's bytes live at rle_str_offsets[r] for
        // rle_str_lens[r]. No per-row validity on this shape.
        *base_out = col.rle_str_arena.data();
        *offs_out = col.rle_str_offsets.data();
        *lens_out = col.rle_str_lens.data();
        *count_out = col.rle_str_lens.size();
        size_t off = 0;
        for (size_t r = 0; r < col.rle_run_lengths.size(); ++r) {
            const size_t cnt = col.rle_run_lengths[r];
            if (off + cnt > (size_t)num_rows)
                throw std::invalid_argument("RLE run lengths exceed the column's row count");
            for (size_t j = 0; j < cnt; ++j) idx[off + j] = (uint32_t)r;
            off += cnt;
        }
        for (size_t i = off; i < (size_t)num_rows; ++i) idx[i] = kStrNull;
        return;
    }
    *base_out = col.string_arena.data();
    *offs_out = col.string_offsets.data();
    *lens_out = col.string_lens.data();
    *count_out = col.string_lens.size();
    size_t vi = 0;
    for (int32_t i = 0; i < num_rows; ++i) {
        if (has_v && !((col.valid_bits[i >> 3] >> (i & 7)) & 1)) {
            idx[i] = kStrNull;
            continue;
        }
        idx[i] = (uint32_t)vi++;
    }
}

}  // namespace

MaterializedColumn materialize_decimal(const DecodedColumn& col, int32_t num_rows) {
    const uint8_t precision = col.decimal_precision;
    const uint8_t scale     = col.decimal_scale;
    const bool    is_i128   = (col.type == "int128");
    const uint32_t length   = (uint32_t)(num_rows > 0 ? num_rows : 0);
    const size_t   slots    = length > 0u ? length : 1u;

    if (precision < 1 || precision > (is_i128 ? 38 : 18))
        throw std::invalid_argument(is_i128 ? "DECIMAL128 precision must be in [1, 38]"
                                            : "DECIMAL precision must be in [1, 18]");
    if (scale > precision)
        throw std::invalid_argument(is_i128 ? "DECIMAL128 scale must be <= precision"
                                            : "DECIMAL scale must be <= precision");

    DrakenPtr<uint8_t> validity = alloc_validity(length);
    bool has_nulls = false;
    MaterializedColumn mc;

    if (is_i128) {
        DrakenPtr<__int128> data = draken_alloc<__int128>(slots);
        const bool has_codes = !col.dict_codes_array.empty() || !col.dict_indices.empty();
        const bool has_dict = !col.dict_int128_values.empty() && has_codes;
        scatter_unscaled<__int128, __int128, __int128>(
            col, num_rows, col.dict_int128_values, col.int128_values,
            has_dict, /*is_rle=*/false, data.get(), validity.get(), &has_nulls);
        check_precision<__int128>(data.get(), length, precision);
        mc = finish(DRAKEN_DECIMAL128, length, data, validity, has_nulls);
    } else {
        DrakenPtr<int64_t> data = draken_alloc<int64_t>(slots);
        const bool from_int32 = (col.type == "int32");
        const bool has_codes = !col.dict_codes_array.empty() || !col.dict_indices.empty();
        const bool has_dict = has_codes && (from_int32 ? !col.dict_int32_values.empty()
                                                       : !col.dict_int64_values.empty());
        const bool is_rle = !has_dict && !col.rle_run_lengths.empty();
        if (from_int32)
            scatter_unscaled<int64_t, int32_t, int32_t>(
                col, num_rows, col.dict_int32_values, col.int32_values,
                has_dict, is_rle, data.get(), validity.get(), &has_nulls);
        else
            scatter_unscaled<int64_t, int64_t, int64_t>(
                col, num_rows, col.dict_int64_values, col.int64_values,
                has_dict, is_rle, data.get(), validity.get(), &has_nulls);
        check_precision<int64_t>(data.get(), length, precision);
        mc = finish(DRAKEN_DECIMAL, length, data, validity, has_nulls);
    }
    mc.precision = precision;
    mc.scale     = scale;
    return mc;
}

MaterializedColumn materialize_int(const DecodedColumn& col, int32_t num_rows, bool from_int32) {
    const int32_t w   = col.int_bit_width;
    const bool    uns = col.is_unsigned;
    const bool has_codes = !col.dict_codes_array.empty() || !col.dict_indices.empty();
    const bool has_dict  = has_codes && (from_int32 ? !col.dict_int32_values.empty()
                                                    : !col.dict_int64_values.empty());
    const bool is_rle    = !has_dict && !col.rle_run_lengths.empty();

#define RUGO_MAT_INT(OUTT, F32, U, DT, TN) \
    build_int<OUTT, F32, U>(col, num_rows, has_dict, is_rle, DT, TN)

    if (from_int32) {
        if (uns) {
            if (w == 8)  return RUGO_MAT_INT(uint8_t,  true, true, DRAKEN_UINT8,  "uint8");
            if (w == 16) return RUGO_MAT_INT(uint16_t, true, true, DRAKEN_UINT16, "uint16");
            if (w == 32) return RUGO_MAT_INT(uint32_t, true, true, DRAKEN_UINT32, "uint32");
            return RUGO_MAT_INT(uint64_t, true, true, DRAKEN_UINT64, "uint64");
        }
        if (w == 8)  return RUGO_MAT_INT(int8_t,  true, false, DRAKEN_INT8,  "int8");
        if (w == 16) return RUGO_MAT_INT(int16_t, true, false, DRAKEN_INT16, "int16");
        if (w == 32 || w == 0)
                     return RUGO_MAT_INT(int32_t, true, false, DRAKEN_INT32, "int32");
        return RUGO_MAT_INT(int64_t, true, false, DRAKEN_INT64, "int64");
    }
    if (uns) {
        if (w == 8)  return RUGO_MAT_INT(uint8_t,  false, true, DRAKEN_UINT8,  "uint8");
        if (w == 16) return RUGO_MAT_INT(uint16_t, false, true, DRAKEN_UINT16, "uint16");
        if (w == 32) return RUGO_MAT_INT(uint32_t, false, true, DRAKEN_UINT32, "uint32");
        return RUGO_MAT_INT(uint64_t, false, true, DRAKEN_UINT64, "uint64");
    }
    if (w == 8)  return RUGO_MAT_INT(int8_t,  false, false, DRAKEN_INT8,  "int8");
    if (w == 16) return RUGO_MAT_INT(int16_t, false, false, DRAKEN_INT16, "int16");
    if (w == 32) return RUGO_MAT_INT(int32_t, false, false, DRAKEN_INT32, "int32");
    return RUGO_MAT_INT(int64_t, false, false, DRAKEN_INT64, "int64");
#undef RUGO_MAT_INT
}

// A parquet `float` column becomes a FLOAT32 column, not a widened FLOAT64
// one: the CARRIER was the bug the list path fixed, and a FLOAT64 tag makes
// every consumer read the column at 8 bytes.
MaterializedColumn materialize_float(const DecodedColumn& col, int32_t num_rows,
                                     bool from_float32) {
    const uint32_t length = (uint32_t)(num_rows > 0 ? num_rows : 0);
    const size_t   slots  = length > 0u ? length : 1u;
    const bool has_codes = !col.dict_codes_array.empty() || !col.dict_indices.empty();
    const bool has_dict  = has_codes && (from_float32 ? !col.dict_float32_values.empty()
                                                      : !col.dict_float64_values.empty());
    const bool is_rle    = !has_dict && !col.rle_run_lengths.empty();

    DrakenPtr<uint8_t> validity = alloc_validity(length);
    bool has_nulls = false;

    if (from_float32) {
        DrakenPtr<float> data = draken_alloc<float>(slots);
        scatter_float<float, float, float>(
            col, num_rows, col.dict_float32_values, col.float32_values,
            has_dict, is_rle, data.get(), validity.get(), &has_nulls);
        return finish(DRAKEN_FLOAT32, length, data, validity, has_nulls);
    }
    DrakenPtr<double> data = draken_alloc<double>(slots);
    scatter_float<double, double, double>(
        col, num_rows, col.dict_float64_values, col.float64_values,
        has_dict, is_rle, data.get(), validity.get(), &has_nulls);
    return finish(DRAKEN_FLOAT64, length, data, validity, has_nulls);
}

MaterializedColumn materialize_string(const DecodedColumn& col, int32_t num_rows, bool is_text) {
    const uint32_t length = (uint32_t)(num_rows > 0 ? num_rows : 0);
    const bool has_codes = !col.dict_codes_array.empty() || !col.dict_indices.empty();
    const bool has_dict  = has_codes && !col.string_dict_lens.empty();
    const bool is_rle    = !has_dict && !col.rle_run_lengths.empty();

    std::unique_ptr<uint32_t, LibcFree> idx_owner(
        (uint32_t*)std::malloc((length > 0u ? length : 1u) * sizeof(uint32_t)));
    if (!idx_owner) throw std::bad_alloc();
    uint32_t* idx = idx_owner.get();
    const uint8_t*  base = nullptr;
    const uint32_t* offs = nullptr;
    const int32_t*  lens = nullptr;
    size_t count = 0;
    str_resolve(col, num_rows, has_dict, is_rle, idx, &base, &offs, &lens, &count);

    // Pass 1 — size the arena and find the nulls.
    size_t total_extern = 0;
    bool has_nulls = false;
    for (uint32_t i = 0; i < length; ++i) {
        const uint32_t k = idx[i];
        if (k == kStrNull) { has_nulls = true; continue; }
        if ((size_t)k >= count)
            throw std::invalid_argument("dictionary code out of range");
        const int32_t ln = lens[k];
        if (ln > STR_INLINE_MAX) total_extern += (size_t)ln;
    }
    // Arena offsets are u32 — the same 4 GB cap the sequence constructor enforced.
    if (total_extern > (size_t)UINT32_MAX)
        throw std::overflow_error(
            "parquet byte_array column: total arena bytes exceed 4 GB limit");

    // Pass 2 — write the arena and build the slots.
    DrakenPtr<DrakenStringSlot> slots =
        draken_alloc<DrakenStringSlot>(length > 0u ? length : 1u);
    DrakenPtr<uint8_t> arena;
    if (total_extern > 0u) arena = draken_alloc<uint8_t>(total_extern);
    DrakenPtr<uint8_t> validity;
    if (has_nulls) validity = alloc_validity(length);

    DrakenStringSlot* sl = slots.get();
    uint8_t* ar = arena.get();
    uint8_t* va = validity.get();
    uint32_t apos = 0;
    for (uint32_t i = 0; i < length; ++i) {
        const uint32_t k = idx[i];
        if (k == kStrNull) {
            str_init_null(&sl[i]);
            va[i >> 3] &= (uint8_t)~(1u << (i & 7));
            continue;
        }
        const uint8_t* sp = base + offs[k];
        const uint32_t ln = (uint32_t)lens[k];
        if (ln > STR_INLINE_MAX) {
            std::memcpy(ar + apos, sp, ln);
            draken_build_string_slot(&sl[i], ar + apos, ln, apos);
            apos += ln;
        } else {
            draken_build_string_slot(&sl[i], sp, ln, 0u);
        }
    }

    MaterializedColumn mc = finish(is_text ? DRAKEN_VARCHAR : DRAKEN_VARBINARY,
                                   length, slots, validity, has_nulls);
    mc.arena     = arena.release();
    mc.arena_len = total_extern;
    return mc;
}

// ---------------------------------------------------------------------------
// BOOLEAN
//
// Replaces a `vector_from_bool_sequence` round trip. MEASURED on a 1M-row
// BOOLEAN column: 5.93 ms of an 8.08 ms read was the per-row PyBool round trip;
// now 0.27 ms.
//
// Layout matches `make_bool_from_sequence` exactly: data is BIT-PACKED, 1 bit
// per row, LSB-first, null rows' value bit left 0. The validity tail mask is
// NOT the same one the other builders here use — the bool constructor is the
// one sequence constructor that masks validity bits past `length` so they do
// not look valid, so this reproduces that rather than using alloc_validity.
//
// BOOLEAN has one source shape: the plain compact value stream. Parquet's
// boolean columns carry neither a dictionary nor rugo's RLE-run path.
// ---------------------------------------------------------------------------
MaterializedColumn materialize_bool(const DecodedColumn& col, int32_t num_rows) {
    const uint32_t n      = (uint32_t)(num_rows > 0 ? num_rows : 0);
    const uint32_t bm     = (n + 7u) >> 3;
    const uint32_t padded = ((bm + 7u) & ~7u);
    const size_t   alloc  = (padded > 0u) ? (size_t)padded : 8u;

    DrakenPtr<uint8_t> data_owner = draken_alloc<uint8_t>(alloc);
    std::memset(data_owner.get(), 0, alloc);
    DrakenPtr<uint8_t> validity_owner = draken_alloc<uint8_t>(alloc);
    std::memset(validity_owner.get(), 0xFF, alloc);
    uint8_t* data     = data_owner.get();
    uint8_t* validity = validity_owner.get();

    const bool   has_v = !col.valid_bits.empty();
    const size_t avail = col.boolean_values.size();
    const uint8_t* bv  = col.boolean_values.data();
    bool has_nulls = false;

    if (!has_v) {
        // Dense: every row takes a value, so the length check is one compare
        // for the whole column and the bits accumulate in a register — no
        // per-row bounds test and no read-modify-write on `data`.
        if (avail < (size_t)n)
            throw std::invalid_argument(
                "value stream shorter than the column's valid row count");
        uint32_t i = 0u;
        for (; i + 8u <= n; i += 8u) {
            uint8_t byte = 0u;
            for (uint32_t b = 0u; b < 8u; ++b)
                byte |= (uint8_t)((bv[i + b] != 0) << b);
            data[i >> 3] = byte;
        }
        uint8_t tail = 0u;
        for (uint32_t b = 0u; i + b < n; ++b)
            tail |= (uint8_t)((bv[i + b] != 0) << b);
        if (i < n) data[i >> 3] = tail;
    } else {
        // A column written OPTIONAL carries valid_bits even when nothing in
        // it is null — which is what pyarrow emits by default, so this is
        // the COMMON path, not the exceptional one. Accumulate both bytes in
        // registers and store each once: a per-row `data[i>>3] |= ...` is a
        // read-modify-write with a loop-carried dependency across every
        // group of 8, and measured 5x the cost per row of the plain
        // independent stores the int/float builders do.
        const uint8_t* vbits = col.valid_bits.data();
        const size_t   vbn   = col.valid_bits.size();
        // Valid rows can never exceed n, so one compare retires the bounds
        // test for the whole column in the case that matters.
        const bool unchecked = (avail >= (size_t)n);
        size_t vi = 0;
        for (uint32_t base = 0u; base < n; base += 8u) {
            const uint32_t lim = (n - base) < 8u ? (n - base) : 8u;
            const uint8_t  vin = ((base >> 3) < vbn) ? vbits[base >> 3] : 0u;
            uint8_t dbyte = 0u, vbyte = 0u;
            for (uint32_t b = 0u; b < lim; ++b) {
                if (!((vin >> b) & 1)) { has_nulls = true; continue; }
                vbyte |= (uint8_t)(1u << b);
                if (!unchecked && vi >= avail)
                    throw std::invalid_argument(
                        "value stream shorter than the column's valid row count");
                dbyte |= (uint8_t)((bv[vi++] != 0) << b);
            }
            data[base >> 3]     = dbyte;
            validity[base >> 3] = vbyte;
        }
    }

    // Tail bits past n must not look valid (mirrors the constructor).
    if (has_nulls && (n & 7u) != 0u && bm > 0u)
        validity[bm - 1u] &= (uint8_t)((1u << (n & 7u)) - 1u);
    return finish(DRAKEN_BOOL, n, data_owner, validity_owner, has_nulls);
}

}  // namespace rugo::_parquet
