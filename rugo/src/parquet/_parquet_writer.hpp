#pragma once
// Parquet WRITER core (Phase 1) for rugo.
//
// Structural inverse of the reader under rugo/src/parquet/. This header is the
// pure-C++ encoder: it works on plain typed buffers + a validity bitmap and
// knows nothing about DrakenVector. The .pyx edge (a later phase) extracts
// those buffers from vectors and calls in here — mirroring how the reader's
// decode core produces DecodedColumn independently of vector construction.
//
// Scope (see docs/PARQUET_WRITER_DESIGN.md, Phase 1):
//   - PLAIN encoding for INT64, DOUBLE, BOOLEAN, BYTE_ARRAY(UTF8 string)
//   - flat columns only (no repetition); nullability via def level 0/1, RLE
//   - single data page (v1) per column chunk; UNCOMPRESSED
//   - one row group (the whole input)
//   - statistics, zstd, decimal/temporal logical types: later phases
//
// HARD REQUIREMENT: output must be readable by PyArrow (and DuckDB). A file
// only rugo can read is a defect.

#include "_thrift_writer.hpp"
#include "page_index_writer.hpp"
#include "_bloom_writer.hpp"

#include "core/kmv_sketch.h"  // THE shared KMV sketch (draken, header-only)

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#ifdef HAVE_ZSTD
#include "zstd.h"           // canonical vendored copy: third_party/zstd
#endif

// `created_by` footer string. Baked at build time from the package version
// (setup.py passes -DRUGO_PARQUET_CREATED_BY); falls back if unset.
#ifndef RUGO_PARQUET_CREATED_BY
#define RUGO_PARQUET_CREATED_BY "opteryx-rugo"
#endif

namespace rugo_pq_write {

// ---- compression ----
//
// ZSTD is gated on HAVE_ZSTD so the pure-C++ core (and the scratch test) still
// compile and run uncompressed without linking zstd. Requesting zstd in a build
// without it is a hard error — never a silent fallback to uncompressed.
inline std::vector<uint8_t> zstd_compress_block(const std::vector<uint8_t> &src,
                                                int level) {
#ifdef HAVE_ZSTD
  size_t bound = ZSTD_compressBound(src.size());
  std::vector<uint8_t> out(bound);
  size_t n = ZSTD_compress(out.data(), bound, src.data(), src.size(), level);
  if (ZSTD_isError(n))
    throw std::runtime_error(std::string("parquet writer: zstd compress failed: ") +
                             ZSTD_getErrorName(n));
  out.resize(n);
  return out;
#else
  (void)src;
  (void)level;
  throw std::runtime_error(
      "parquet writer: zstd compression requested but built without HAVE_ZSTD");
#endif
}

// ---- compression profile ----
//
// FAST is the default: the write side of CTAS and uploads, where latency is
// the constraint. STORAGE is for the defragmenter, which rewrites bytes that
// are then read many times and can afford to compress slowly.
enum WriteProfile : int32_t {
  PROFILE_FAST = 0,
  PROFILE_STORAGE = 1,
};

// Parquet physical Type enum (parquet.thrift `enum Type`).
enum PType : int32_t {
  PT_BOOLEAN = 0,
  PT_INT32 = 1,
  PT_INT64 = 2,
  PT_FLOAT = 4,
  PT_DOUBLE = 5,
  PT_BYTE_ARRAY = 6,
  PT_FLBA = 7,
};

// Encoding / codec / page / repetition / converted-type enum values.
enum { ENC_PLAIN = 0, ENC_PLAIN_DICTIONARY = 2, ENC_RLE = 3, ENC_RLE_DICTIONARY = 8 };
enum { CODEC_UNCOMPRESSED = 0, CODEC_ZSTD = 6 };

// ---- keep-compressed floor ----
//
// A column chunk is stored compressed only when the compressed form is below
// this fraction of the raw bytes; otherwise the raw pages are stored and the
// chunk records CODEC_UNCOMPRESSED. Compression that barely works is the worst
// of both: it costs a full decompression pass on every read for a rounding
// error in bytes, and decompression is 80.5% of parquet read CPU (measured,
// make clickbench-profile 2026-08-11).
//
// 0.95, NOT the 0.85 first proposed. The deciding case is the REMOTE path
// (architect ruling, 2026-08-11), and there the exchange rate is set by the
// ~64 MB/s production GCS ceiling against the decompressor:
//
//     C/64 + R/decomp_rate  <  R/64
//
// which solves to C < 0.96R single-threaded (zstd's worst measured decode,
// 1590 MB/s) and C < 0.998R with parallel decode (40,501 MB/s aggregate at 18
// threads). An 0.85 floor would give away the entire 0.85-0.95 band, where
// compression is still comfortably net-positive remotely. 0.95 keeps that band
// and still rejects the "barely worth it" tail the floor exists for.
//
// Measurements: dev/codec_matrix_bench.cpp, dev/codec_parallel_scaling.cpp
// (both ARM and x86 — ratios are bit-identical across architectures).
inline constexpr double kKeepCompressedFloor = 0.95;

inline bool compressed_clears_floor(size_t compressed_bytes, size_t raw_bytes) {
  return static_cast<double>(compressed_bytes)
       < kKeepCompressedFloor * static_cast<double>(raw_bytes);
}
enum { PAGE_DATA = 0, PAGE_DICTIONARY = 2 };
enum { REP_REQUIRED = 0, REP_OPTIONAL = 1, REP_REPEATED = 2 };
// ConvertedType values (parquet.thrift enum ConvertedType).
enum {
  CONV_UTF8 = 0,
  CONV_LIST = 3,
  CONV_DECIMAL = 5,
  CONV_DATE = 6,
  CONV_TIME_MILLIS = 7,
  CONV_TIME_MICROS = 8,
  CONV_TIMESTAMP_MILLIS = 9,
  CONV_TIMESTAMP_MICROS = 10,
  CONV_UINT_8 = 11,
  CONV_UINT_16 = 12,
  CONV_UINT_32 = 13,
  CONV_UINT_64 = 14,
  CONV_INT_8 = 15,
  CONV_INT_16 = 16,
  CONV_INT_32 = 17,
  CONV_INT_64 = 18,
  CONV_INTERVAL = 21,
};

// Logical-type annotation the writer attaches to a column (drives both the
// ConvertedType and the LogicalType union in the schema). NONE = plain
// physical type.
enum LogicalKindPq {
  LK_NONE = 0,
  LK_DATE,
  LK_TIMESTAMP,
  LK_DECIMAL,
  LK_TIME,
  LK_INTERVAL,
};

// Parquet TimeUnit union member ids (1=MILLIS, 2=MICROS, 3=NANOS).
enum { TU_MILLIS = 1, TU_MICROS = 2, TU_NANOS = 3 };

// A byte-array value (string / binary). For null rows the slice is ignored.
struct StrSlice {
  const uint8_t *ptr;
  uint32_t len;
};

// One column to write. Exactly one of the typed buffers is consulted, chosen
// by `type`. `validity` is a 1-bit-per-row mask (1 = valid); NULL = all valid.
// Buffers hold one entry PER ROW (including null rows — null entries are read
// but not emitted), matching the reader's positional layout.
struct ColumnInput {
  std::string name;
  PType type;
  bool is_utf8 = false; // BYTE_ARRAY annotated as STRING (ConvertedType UTF8)

  const uint8_t *validity = nullptr; // bit per row, 1=valid; NULL => all valid

  const int32_t *i32 = nullptr;     // DATE32 (days)
  const int64_t *i64 = nullptr;     // INT64 / TIMESTAMP64
  // FLOAT32 is written as parquet FLOAT (4 bytes), NOT widened to DOUBLE.
  // Widening was lossless per value but not per COLUMN: the file then declares
  // float64, so every reader — including opteryx's own schema inference — binds
  // the column at 8 bytes and rugo cannot round-trip a 4-byte float at all.
  const float *f32 = nullptr;
  const double *f64 = nullptr;
  const uint8_t *boolean = nullptr; // one byte per row, 0/1
  const StrSlice *strs = nullptr;   // one per row
  // DECIMAL/DECIMAL128: native-endian unscaled values, `dec_width` bytes each
  // (8 = int64-backed, 16 = int128-backed). Emitted big-endian as FLBA.
  const uint8_t *dec_raw = nullptr;

  // Logical-type annotation.
  LogicalKindPq logical = LK_NONE;
  int dec_width = 0;        // FLBA byte width (== source width)
  int dec_scale = 0;
  int dec_precision = 0;
  int ts_unit = TU_MICROS;  // TIMESTAMP TimeUnit member id
  bool ts_utc = false;      // TIMESTAMP isAdjustedToUTC

  // ---- Draken logical descriptor for kinds parquet CANNOT express ----
  //
  // `logical` above covers every kind parquet has a logical type to map onto
  // (DATE / TIME / TIMESTAMP / DECIMAL / INTERVAL): those survive a round trip
  // through the schema annotation and need nothing more. Draken's IPV4 has no
  // parquet equivalent at all — it is DRAKEN_UINT32 plus a descriptor carried
  // out-of-band on the VectorOwner — so without a side channel it is written as
  // a bare unsigned integer and read back as one: a perfectly well-formed
  // column, wrong type, no error. Measured end to end 2026-08-19.
  //
  // This is the draken LogicalKind ORDINAL (draken/core/draken_bridge.h:
  // 0 NONE, 1 TIMESTAMP, 2 TIME, 3 DECIMAL, 4 VECTOR, 5 IPV4). 0 emits nothing.
  // Only kinds parquet cannot express belong here — annotating a kind the
  // schema already carries would create two sources of truth that can disagree.
  // Value-carrying kinds (unit / precision / dimension) add their own fields to
  // this struct when they are wired; the WIRE format already accommodates them
  // (see write_draken_logical_kv).
  int draken_logical_kind = 0;

  bool bloom = false;       // emit a split-block bloom filter for this column

  // ---- dictionary encoding ----
  //
  // Two ways a column becomes dictionary-encoded (RLE_DICTIONARY):
  //
  //   1. PRESERVE — the edge already holds a dictionary (the incoming
  //      DrakenVector was dict/constant-shaped). It sets `codes` (one dict
  //      code per logical row; null rows carry an arbitrary in-range code),
  //      `dict_count`, and points the typed buffers (i32/i64/f32/f64/strs) at the
  //      `dict_count` DICTIONARY VALUES rather than per-row values. `codes !=
  //      nullptr` is the discriminator for this mode: encode/stats/bloom then
  //      read value[codes[i]] for logical row i.
  //
  //   2. AUTO-BUILD — the edge holds plain per-row buffers and sets
  //      `dict_enabled`. The encoder hashes the column; if the cardinality is
  //      low enough it builds a dictionary internally, otherwise it falls back
  //      to PLAIN. Never set together with `codes`.
  const uint32_t *codes = nullptr; // per-row dict codes (PRESERVE mode)
  uint32_t dict_count = 0;         // number of dictionary entries
  bool dict_enabled = false;       // attempt AUTO-BUILD for plain buffers

  // ---- integer width/signedness logical annotation ----
  // Parquet's only integer physical types are INT32 and INT64. Every narrower
  // width — and unsignedness at any width — is expressed as an INTEGER(bitWidth,
  // isSigned) LogicalType annotation over the next physical type up that holds
  // it: int8/int16/uint8/uint16/uint32 ride on INT32, uint64 on INT64. The
  // stored bits are the plain signed reinterpret (lossless), so nothing changes
  // in the value encoders — only the schema gains the annotation, and a
  // conformant reader (PyArrow/DuckDB/Polars/rugo) recovers the declared type.
  //
  // `int_bit_width` is 8/16/32/64 and, with `is_unsigned`, selects the
  // annotation; 0 means emit none. For a scalar column it annotates the column,
  // for an ARRAY it annotates the leaf `element`.
  //
  // INT32 and INT64 are deliberately left un-annotated (int_bit_width == 0):
  // the physical type alone already says exactly that, which is what PyArrow
  // and parquet-mr emit. Readers must therefore treat a bare physical INT32 as
  // a 32-bit signed column, not widen it.
  //
  // NOTE: when `is_unsigned`, values at or above the signed midpoint occupy
  // NEGATIVE physical slots. Anything that ORDERS those slots (column
  // statistics, sorted dictionaries) must compare them as unsigned or it will
  // record a wrong min/max — see compute_stats and build_dict_column.
  bool is_unsigned = false;
  int int_bit_width = 0;

  // ---- ARRAY (LIST) columns ----
  // When `is_array`, this column is a list nested `array_depth` levels deep
  // (1 = list<scalar>, 2 = list<list<scalar>>). The leaf element values live in
  // the typed buffers above (i32/i64/f32/f64/boolean/strs), holding only the
  // num_elements PRESENT elements in order; `elem_type`/`elem_is_utf8` describe
  // the leaf. `rep_levels`/`def_levels` hold `num_levels` entries under the
  // all-nullable nesting scheme (max_rep == array_depth, max_def ==
  // 2*array_depth + 1). `is_unsigned`/`int_bit_width` (above), when set,
  // annotate the leaf element's declared width/signedness — the leaf is stored
  // at its OWN physical width (INT8/16/32 and UINT8/16/32 on physical INT32,
  // INT64/UINT64 on INT64, FLOAT32 on FLOAT), exactly like a scalar column.
  bool is_array = false;
  int array_depth = 1;
  PType elem_type = PT_INT64;
  bool elem_is_utf8 = false;
  const uint8_t *rep_levels = nullptr;
  const uint8_t *def_levels = nullptr;
  size_t num_levels = 0;
  size_t num_elements = 0;
  // Row-group splitting for arrays: rep/def levels and flat element values are
  // NOT one-per-row (a row can expand into 0, 1, or many level/element entries
  // via nesting/repetition), so a row-group's [rg_start, rg_start+rg_rows)
  // window can't be pointer-sliced the way scalar per-row buffers are. These
  // two arrays (size num_rows+1, monotonic) give, for each row, the starting
  // index into rep_levels/def_levels and into the flat element buffer
  // respectively — row r's levels are [row_level_offsets[r], row_level_offsets
  // [r+1]) and its elements are [row_element_offsets[r], row_element_offsets
  // [r+1]). Built once by the caller while it constructs rep_levels/def_levels/
  // the elem_* buffers (it already walks rows in order to do so).
  const uint32_t *row_level_offsets = nullptr;
  const uint32_t *row_element_offsets = nullptr;

  // ---- clustering / sort-order hint ----
  // Caller-supplied assertion that this column's values, within each row
  // group written from this ColumnInput, are already ordered. Written
  // verbatim into RowGroup.sorting_columns (parquet.thrift SortingColumn)
  // with NO verification by the writer — the caller (e.g. compaction merging
  // pre-sorted runs) is the source of truth. A wrong hint produces a wrong
  // footer claim; see the reader's created_by trust gate in metadata.cpp,
  // which only believes sorting_columns from files rugo itself wrote.
  bool sorted_hint = false;
  bool sorted_descending = false;
  bool sorted_nulls_first = false;
};

// Minimum bit width to hold values in [0, maxval]. bit_width(0)=0, (1)=1,
// (2)=2, (5)=3. Used for RLE level packing of rep/def streams.
inline int level_bit_width(uint32_t maxval) {
  int bw = 0;
  while (maxval) { bw++; maxval >>= 1; }
  return bw;
}

// ---- compression policy ----
//
// The zstd level is chosen PER COLUMN from its physical type; callers pick a
// profile, never a level. Measured on ClickBench with
// dev/rugo_parquet_codec_bench.cpp (10M rows, interleaved rounds): BYTE_ARRAY
// is the only physical type that responds to level — 4 -> 7 buys 13.7% on the
// string columns, but only ~2% on INT32/INT64 for 2-3x the compress time, and
// high-cardinality integers (WatchID) are incompressible at EVERY level. So
// numerics stay at 4 in both profiles and only the string level moves; a
// blanket level 7 costs 14% more compress time than "strings 7, ints 4" for
// 1.2% fewer bytes.
inline int zstd_level_for(const ColumnInput &col, int profile) {
  const PType t = col.is_array ? col.elem_type : col.type;
  if (t == PT_BYTE_ARRAY)
    return (profile == PROFILE_STORAGE) ? 7 : 4;
  return 4;
}

// ---- small endian helpers ----

inline void put_u32_le(std::vector<uint8_t> &b, uint32_t v) {
  b.push_back((uint8_t)(v & 0xFF));
  b.push_back((uint8_t)((v >> 8) & 0xFF));
  b.push_back((uint8_t)((v >> 16) & 0xFF));
  b.push_back((uint8_t)((v >> 24) & 0xFF));
}

inline void put_u64_le(std::vector<uint8_t> &b, uint64_t v) {
  for (int i = 0; i < 8; i++) {
    b.push_back((uint8_t)(v & 0xFF));
    v >>= 8;
  }
}

inline void put_varint(std::vector<uint8_t> &b, uint64_t v) {
  while (v >= 0x80) {
    b.push_back((uint8_t)v | 0x80);
    v >>= 7;
  }
  b.push_back((uint8_t)v);
}

inline bool is_valid(const uint8_t *validity, size_t i) {
  return validity == nullptr || (validity[i >> 3] >> (i & 7)) & 1;
}

// RLE/bit-packing-hybrid encode of `n` definition/repetition levels at the
// given bit width (run-length form; value stored in ceil(bit_width/8) bytes,
// = 1 for bit widths up to 8). Used for array rep/def levels (rep bw=1, def
// bw=2 for the 3-level LIST encoding).
inline std::vector<uint8_t> encode_levels_rle(const uint8_t *levels, size_t n,
                                              int bit_width) {
  std::vector<uint8_t> out;
  int value_bytes = (bit_width + 7) / 8;
  size_t i = 0;
  while (i < n) {
    uint8_t v = levels[i];
    size_t run = 1;
    while (i + run < n && levels[i + run] == v)
      run++;
    put_varint(out, (uint64_t)run << 1); // low bit 0 => RLE run
    for (int b = 0; b < value_bytes; b++)
      out.push_back((uint8_t)((v >> (8 * b)) & 0xFF));
    i += run;
  }
  return out;
}

// Append a `width`-byte big-endian copy of a native-endian (little-endian
// target) unscaled decimal value. Targets are all little-endian, so this is a
// byte reversal; sign is preserved because width == source width.
inline void put_be_from_le(std::vector<uint8_t> &out, const uint8_t *le,
                           int width) {
  for (int k = width - 1; k >= 0; k--)
    out.push_back(le[k]);
}

// ---- definition levels (max def level = 1, bit width 1) ----
//
// RLE/bit-packing hybrid, run-length form: collapse consecutive equal levels
// into RLE runs. Header = (run_len << 1) | 0 (low bit 0 = RLE run), then the
// value packed in ceil(bitWidth/8) = 1 byte. Correct for any null pattern and
// what PyArrow's reader expects in a v1 data page.
inline std::vector<uint8_t> encode_def_levels(const uint8_t *validity,
                                              size_t num_rows) {
  std::vector<uint8_t> out;
  size_t i = 0;
  while (i < num_rows) {
    uint8_t v = is_valid(validity, i) ? 1 : 0;
    size_t run = 1;
    while (i + run < num_rows &&
           (is_valid(validity, i + run) ? 1 : 0) == v)
      run++;
    put_varint(out, (uint64_t)run << 1);
    out.push_back(v);
    i += run;
  }
  return out;
}

// ---- PLAIN value encoders (non-null values only) ----

inline void encode_values(const ColumnInput &col, size_t num_rows,
                          std::vector<uint8_t> &out) {
  switch (col.type) {
  case PT_INT32:
    for (size_t i = 0; i < num_rows; i++)
      if (is_valid(col.validity, i))
        put_u32_le(out, (uint32_t)col.i32[i]);
    break;
  case PT_INT64:
    for (size_t i = 0; i < num_rows; i++)
      if (is_valid(col.validity, i))
        put_u64_le(out, (uint64_t)col.i64[i]);
    break;
  case PT_FLOAT:
    for (size_t i = 0; i < num_rows; i++)
      if (is_valid(col.validity, i)) {
        uint32_t bits;
        std::memcpy(&bits, &col.f32[i], 4);
        put_u32_le(out, bits);
      }
    break;
  case PT_DOUBLE:
    for (size_t i = 0; i < num_rows; i++)
      if (is_valid(col.validity, i)) {
        uint64_t bits;
        std::memcpy(&bits, &col.f64[i], 8);
        put_u64_le(out, bits);
      }
    break;
  case PT_BOOLEAN: {
    // bit-pack non-null values, LSB-first.
    uint8_t cur = 0;
    int nbits = 0;
    for (size_t i = 0; i < num_rows; i++) {
      if (!is_valid(col.validity, i))
        continue;
      if (col.boolean[i])
        cur |= (uint8_t)(1 << nbits);
      if (++nbits == 8) {
        out.push_back(cur);
        cur = 0;
        nbits = 0;
      }
    }
    if (nbits > 0)
      out.push_back(cur);
    break;
  }
  case PT_BYTE_ARRAY:
    for (size_t i = 0; i < num_rows; i++)
      if (is_valid(col.validity, i)) {
        const StrSlice &s = col.strs[i];
        put_u32_le(out, s.len);
        out.insert(out.end(), s.ptr, s.ptr + s.len);
      }
    break;
  case PT_FLBA:
    // DECIMAL: big-endian unscaled value. INTERVAL: 12 bytes verbatim (the
    // edge already laid them out as 3 little-endian uint32 months/days/millis).
    for (size_t i = 0; i < num_rows; i++)
      if (is_valid(col.validity, i)) {
        const uint8_t *p = col.dec_raw + (size_t)i * col.dec_width;
        if (col.logical == LK_INTERVAL)
          out.insert(out.end(), p, p + col.dec_width);
        else
          put_be_from_le(out, p, col.dec_width);
      }
    break;
  default:
    throw std::runtime_error("parquet writer: unsupported physical type");
  }
}

// ---- column statistics ----
//
// min_value/max_value are PLAIN-encoded (same byte layout as page values) so
// readers decode them the same way. Ordering MUST match the reader's pruning
// comparison: signed for ints, IEEE for doubles (NaN ignored), unsigned-byte
// lexicographic for strings. We emit fields 5/6 (+null_count) and a
// TypeDefinedOrder column_orders entry so the v2 stats are trusted.
struct ColumnStats {
  bool has_minmax = false;
  std::vector<uint8_t> min_bytes;
  std::vector<uint8_t> max_bytes;
  int64_t null_count = 0;
  // Hash-derived NDV (distinct non-null values), set only for bloom-eligible
  // columns where the bloom build already computes it for free. -1 = not
  // present. Hash-distinct, so a collision (xxhash64, vanishingly rare at these
  // counts) would undercount — same hash-only equality basis as the bloom.
  int64_t distinct_count = -1;
};

// unsigned-byte lexicographic compare (memcmp + shorter-is-smaller tiebreak),
// matching how the reader (Python bytes) orders BYTE_ARRAY stats.
inline bool str_lt(const StrSlice &a, const StrSlice &b) {
  uint32_t n = a.len < b.len ? a.len : b.len;
  int c = (n == 0) ? 0 : std::memcmp(a.ptr, b.ptr, n);
  if (c != 0)
    return c < 0;
  return a.len < b.len;
}

inline ColumnStats compute_stats(const ColumnInput &col, size_t num_rows) {
  ColumnStats st;
  for (size_t i = 0; i < num_rows; i++)
    if (!is_valid(col.validity, i))
      st.null_count++;

  // PRESERVE mode: typed buffers hold dict values; logical row i reads
  // value[codes[i]]. Plain/auto-build: codes==nullptr => value[i].
  const uint32_t *codes = col.codes;

  switch (col.type) {
  // Integer min/max are compared in the column's DECLARED domain, not the
  // physical slot's. An unsigned column at or above the signed midpoint sits in
  // a negative slot, so a signed compare would report a wrong min/max and a
  // reader doing sound unsigned pruning could skip a row group that holds
  // matching rows. Both branches emit the same PLAIN bytes either way — only
  // WHICH value wins changes.
  case PT_INT32: {
    bool any = false;
    int32_t lo = 0, hi = 0;
    for (size_t i = 0; i < num_rows; i++) {
      if (!is_valid(col.validity, i))
        continue;
      int32_t v = col.i32[codes ? codes[i] : i];
      if (!any) { lo = hi = v; any = true; }
      else if (col.is_unsigned) {
        if ((uint32_t)v < (uint32_t)lo) lo = v;
        if ((uint32_t)v > (uint32_t)hi) hi = v;
      } else {
        if (v < lo) lo = v;
        if (v > hi) hi = v;
      }
    }
    if (any) {
      st.has_minmax = true;
      put_u32_le(st.min_bytes, (uint32_t)lo);
      put_u32_le(st.max_bytes, (uint32_t)hi);
    }
    break;
  }
  case PT_INT64: {
    bool any = false;
    int64_t lo = 0, hi = 0;
    for (size_t i = 0; i < num_rows; i++) {
      if (!is_valid(col.validity, i))
        continue;
      int64_t v = col.i64[codes ? codes[i] : i];
      if (!any) {
        lo = hi = v;
        any = true;
      } else if (col.is_unsigned) {
        if ((uint64_t)v < (uint64_t)lo) lo = v;
        if ((uint64_t)v > (uint64_t)hi) hi = v;
      } else {
        if (v < lo) lo = v;
        if (v > hi) hi = v;
      }
    }
    if (any) {
      st.has_minmax = true;
      put_u64_le(st.min_bytes, (uint64_t)lo);
      put_u64_le(st.max_bytes, (uint64_t)hi);
    }
    break;
  }
  case PT_FLBA: { // DECIMAL: numeric min/max of the unscaled value, BE-encoded
    if (col.logical == LK_INTERVAL)
      break; // parquet INTERVAL has UNKNOWN sort order — emit null_count only
    bool any = false;
    __int128 lo = 0, hi = 0;
    size_t lo_i = 0, hi_i = 0;
    for (size_t i = 0; i < num_rows; i++) {
      if (!is_valid(col.validity, i))
        continue;
      const uint8_t *p = col.dec_raw + i * col.dec_width;
      __int128 v = 0;
      if (col.dec_width == 8) {
        int64_t t;
        std::memcpy(&t, p, 8);
        v = t;
      } else {
        std::memcpy(&v, p, 16); // dec_width == 16
      }
      if (!any) { lo = hi = v; lo_i = hi_i = i; any = true; }
      else {
        if (v < lo) { lo = v; lo_i = i; }
        if (v > hi) { hi = v; hi_i = i; }
      }
    }
    if (any) {
      st.has_minmax = true;
      put_be_from_le(st.min_bytes, col.dec_raw + lo_i * col.dec_width,
                     col.dec_width);
      put_be_from_le(st.max_bytes, col.dec_raw + hi_i * col.dec_width,
                     col.dec_width);
    }
    break;
  }
  case PT_FLOAT: {
    // Same rules as PT_DOUBLE below, at binary32 width: NaN excluded from
    // min/max (parquet spec), bytes written little-endian at the PHYSICAL
    // width — a 4-byte column whose stats carry 8 bytes is unreadable.
    bool any = false;
    float lo = 0, hi = 0;
    for (size_t i = 0; i < num_rows; i++) {
      if (!is_valid(col.validity, i))
        continue;
      float v = col.f32[codes ? codes[i] : i];
      if (v != v) // skip NaN (parquet: NaN excluded from min/max)
        continue;
      if (!any) {
        lo = hi = v;
        any = true;
      } else {
        if (v < lo) lo = v;
        if (v > hi) hi = v;
      }
    }
    if (any) {
      st.has_minmax = true;
      uint32_t b;
      std::memcpy(&b, &lo, 4);
      put_u32_le(st.min_bytes, b);
      std::memcpy(&b, &hi, 4);
      put_u32_le(st.max_bytes, b);
    }
    break;
  }
  case PT_DOUBLE: {
    bool any = false;
    double lo = 0, hi = 0;
    for (size_t i = 0; i < num_rows; i++) {
      if (!is_valid(col.validity, i))
        continue;
      double v = col.f64[codes ? codes[i] : i];
      if (v != v) // skip NaN (parquet: NaN excluded from min/max)
        continue;
      if (!any) {
        lo = hi = v;
        any = true;
      } else {
        if (v < lo) lo = v;
        if (v > hi) hi = v;
      }
    }
    if (any) {
      st.has_minmax = true;
      uint64_t b;
      std::memcpy(&b, &lo, 8);
      put_u64_le(st.min_bytes, b);
      std::memcpy(&b, &hi, 8);
      put_u64_le(st.max_bytes, b);
    }
    break;
  }
  case PT_BOOLEAN: {
    bool any = false, saw_false = false, saw_true = false;
    for (size_t i = 0; i < num_rows; i++) {
      if (!is_valid(col.validity, i))
        continue;
      any = true;
      if (col.boolean[i]) saw_true = true;
      else saw_false = true;
    }
    if (any) {
      st.has_minmax = true;
      st.min_bytes.push_back(saw_false ? 0 : 1);
      st.max_bytes.push_back(saw_true ? 1 : 0);
    }
    break;
  }
  case PT_BYTE_ARRAY: {
    bool any = false;
    StrSlice lo{nullptr, 0}, hi{nullptr, 0};
    for (size_t i = 0; i < num_rows; i++) {
      if (!is_valid(col.validity, i))
        continue;
      const StrSlice &s = col.strs[codes ? codes[i] : i];
      if (!any) {
        lo = hi = s;
        any = true;
      } else {
        if (str_lt(s, lo)) lo = s;
        if (str_lt(hi, s)) hi = s;
      }
    }
    if (any) {
      st.has_minmax = true;
      st.min_bytes.assign(lo.ptr, lo.ptr + lo.len);
      st.max_bytes.assign(hi.ptr, hi.ptr + hi.len);
    }
    break;
  }
  default:
    throw std::runtime_error("parquet writer: unsupported physical type");
  }
  return st;
}

// XXH64 hash of each value's PLAIN-encoded bytes — the exact bytes a reader
// hashes when probing. Matches encode_values: INT64/TIMESTAMP = 8 LE,
// INT32/DATE = 4 LE, DOUBLE = 8 LE IEEE, BYTE_ARRAY = raw value bytes (no
// length prefix), FLBA/DECIMAL = the big-endian fixed-width bytes.
//
// ⭐ ONE hash pass per column, shared by BOTH consumers that need a value hash:
// the bloom filter and the dictionary-encoding decision. They hash the same
// values with the same function over the same byte images, so computing them
// separately was the same work done twice. The buffer is indexed BY ROW (null
// rows hold an unused 0) because the dictionary build needs a code per logical
// row; bloom takes the compacted present-only view via compact_present_hashes.
//
// Returns false for a physical type with no value hash (bool), leaving
// `row_hashes` untouched — neither consumer runs for those.
inline bool hash_column_rows(const ColumnInput &col, size_t num_rows,
                             std::vector<uint64_t> &row_hashes) {
  switch (col.type) {
  case PT_INT32:
  case PT_INT64:
  case PT_FLOAT:
  case PT_DOUBLE:
  case PT_BYTE_ARRAY:
  case PT_FLBA:
    break;
  default:
    return false; // bool / unsupported: no bloom, no dictionary
  }
  row_hashes.assign(num_rows, 0);
  uint8_t buf[16];
  // PRESERVE mode resolves value[codes[i]]; see compute_stats.
  const uint32_t *codes = col.codes;
  for (size_t i = 0; i < num_rows; i++) {
    if (!is_valid(col.validity, i))
      continue;
    size_t vi = codes ? codes[i] : i;
    switch (col.type) {
    case PT_INT32: {
      int32_t v = col.i32[vi];
      std::memcpy(buf, &v, 4);
      row_hashes[i] = bloom_hash(buf, 4);
      break;
    }
    case PT_INT64: {
      int64_t v = col.i64[vi];
      std::memcpy(buf, &v, 8);
      row_hashes[i] = bloom_hash(buf, 8);
      break;
    }
    // Floats are hashed over their BIT PATTERN, which is also how the
    // dictionary keys them: -0.0 and +0.0 must not share an entry and each NaN
    // payload stays distinct, so the sketch must count them as distinct too.
    case PT_FLOAT:
      std::memcpy(buf, &col.f32[vi], 4);
      row_hashes[i] = bloom_hash(buf, 4);
      break;
    case PT_DOUBLE:
      std::memcpy(buf, &col.f64[vi], 8);
      row_hashes[i] = bloom_hash(buf, 8);
      break;
    case PT_BYTE_ARRAY: {
      const StrSlice &s = col.strs[vi];
      row_hashes[i] = bloom_hash(s.ptr, s.len);
      break;
    }
    case PT_FLBA: {
      // Hash the big-endian fixed-width bytes (as written to the page).
      const uint8_t *le = col.dec_raw + (size_t)i * col.dec_width;
      for (int k = 0; k < col.dec_width; k++)
        buf[k] = le[col.dec_width - 1 - k];
      row_hashes[i] = bloom_hash(buf, col.dec_width);
      break;
    }
    default:
      break;
    }
  }
  return true;
}

// The present (non-null) hashes in row order — what the bloom filter indexes.
inline std::vector<uint64_t> compact_present_hashes(
    const std::vector<uint64_t> &row_hashes, const uint8_t *validity,
    size_t num_rows) {
  std::vector<uint64_t> present;
  present.reserve(num_rows);
  for (size_t i = 0; i < num_rows; i++) {
    if (is_valid(validity, i))
      present.push_back(row_hashes[i]);
  }
  return present;
}
inline size_t bloom_ndv(std::vector<uint64_t> hashes) {
  std::sort(hashes.begin(), hashes.end());
  hashes.erase(std::unique(hashes.begin(), hashes.end()), hashes.end());
  return hashes.size();
}

// Serialize a BloomFilterHeader (Compact Protocol): numBytes(1),
// algorithm(2)=SplitBlockAlgorithm{}, hash(3)=XxHash{}, compression(4)=
// Uncompressed{} — each union member is field 1 holding an empty struct.
inline std::vector<uint8_t> build_bloom_header(int32_t num_bytes) {
  TCompactWriter h;
  h.structBegin();
  h.writeI32Field(1, num_bytes);
  h.writeFieldHeader(CT_STRUCT, 2); // algorithm
  h.structBegin();
  h.writeFieldHeader(CT_STRUCT, 1); // BLOCK = SplitBlockAlgorithm
  h.structBegin();
  h.structEnd();
  h.structEnd();
  h.writeFieldHeader(CT_STRUCT, 3); // hash
  h.structBegin();
  h.writeFieldHeader(CT_STRUCT, 1); // XXHASH = XxHash
  h.structBegin();
  h.structEnd();
  h.structEnd();
  h.writeFieldHeader(CT_STRUCT, 4); // compression
  h.structBegin();
  h.writeFieldHeader(CT_STRUCT, 1); // UNCOMPRESSED = Uncompressed
  h.structBegin();
  h.structEnd();
  h.structEnd();
  h.structEnd();
  return h.buf;
}

// A built column chunk: the on-disk bytes (page header + possibly-compressed
// body) plus the uncompressed total (page header + raw body) for metadata.
//
// `plain_bytes` carries the SAME pages built under CODEC_UNCOMPRESSED, so the
// caller can apply the keep-whichever-is-smaller rule (see
// write_row_group_chunks) without re-encoding the column. It is populated only
// when a compressing codec was requested — under CODEC_UNCOMPRESSED `bytes` is
// already the plain form and duplicating it would be pure waste. Holding both
// costs no extra peak memory: the raw body and the compressed body are both
// live inside the builders anyway.
struct PageBuild {
  std::vector<uint8_t> bytes;
  std::vector<uint8_t> plain_bytes;
  size_t uncompressed_total;
  // One entry per data page in `bytes` / `plain_bytes`, in order. Empty unless
  // a page index was requested.
  std::vector<PageMeta> pages;
};

// Per-page bounds/null state for one page, from that page's own column slice.
// Reuses compute_stats so a page bound is derived by EXACTLY the same rules
// (and the same declared-domain integer ordering) as the chunk bound the footer
// carries — a page bound that disagreed with the chunk bound would be a bug.
inline PageMeta make_page_meta(const ColumnInput &sub, size_t count,
                               int64_t first_row, const PageBuild &pb, int codec) {
  const ColumnStats ps = compute_stats(sub, count);
  PageMeta pm;
  pm.first_row_index = first_row;
  pm.null_count = ps.null_count;
  pm.null_page = (ps.null_count == (int64_t)count);
  pm.has_bounds = ps.has_minmax;
  if (ps.has_minmax) {
    pm.min_bytes = ps.min_bytes;
    pm.max_bytes = ps.max_bytes;
  }
  pm.stored_size = pb.bytes.size();
  pm.plain_size = (codec == CODEC_ZSTD) ? pb.plain_bytes.size() : pb.bytes.size();
  return pm;
}

// Rows per data page for a byte-size-triggered split. Estimated from
// column-specific bytes/row (exact for fixed-width types, measured for
// variable-width) and rounded up to a multiple of 8 so validity byte offsets
// stay exact, mirroring row-group splitting's rounding.
//
// `col.codes != nullptr` (dictionary-encoded chunk) is sized on the PLAIN
// footprint of the values a row resolves to, NOT on the encoded code stream.
// That is deliberate: a dict column's code stream is so small that a byte
// budget over it would put a whole row group in one page, which is exactly the
// case the page index exists to break up. Sizing on the plain footprint gives a
// dict column the same page GRID as it would have had unencoded — the
// dictionary's win shows up as a smaller stored page, not as a coarser index.
inline size_t rows_per_page_for(const ColumnInput &col, size_t rg_rows,
                                size_t max_page_bytes) {
  const uint32_t *codes = col.codes;
  size_t rows_per_page;
  if (col.type == PT_BYTE_ARRAY) {
    // Variable width: measure actual encoded bytes (4-byte length + payload,
    // present rows only) to get an honest average.
    size_t total = 0;
    for (size_t i = 0; i < rg_rows; i++)
      if (is_valid(col.validity, i)) total += 4 + col.strs[codes ? codes[i] : i].len;
    double bpr = rg_rows > 0 ? (double)total / (double)rg_rows : 1.0;
    rows_per_page = (size_t)((double)max_page_bytes / std::max(1.0, bpr));
  } else if (col.type == PT_FLBA) {
    rows_per_page = max_page_bytes / (size_t)std::max(1, col.dec_width);
  } else if (col.type == PT_BOOLEAN) {
    rows_per_page = max_page_bytes * 8; // ~1 bit/row (def-level overhead ignored, small)
  } else {
    // 4-byte physical types (INT32, FLOAT) vs 8-byte (INT64, DOUBLE).
    size_t width = (col.type == PT_INT32 || col.type == PT_FLOAT) ? 4 : 8;
    rows_per_page = max_page_bytes / width;
  }
  if (rows_per_page == 0) rows_per_page = 8;
  return (rows_per_page + 7) & ~(size_t)7; // byte-aligned validity slicing
}

// Offset a scalar ColumnInput's per-row buffers to start at row `start`.
// PRESERVE/auto-dict columns (codes != nullptr) keep their typed buffers
// pointing at the DICTIONARY values and slice `codes` instead.
inline ColumnInput slice_rows(const ColumnInput &col, size_t start) {
  ColumnInput sub = col;
  if (col.codes) {
    sub.codes = col.codes + start;
  } else {
    if (col.i32)     sub.i32     = col.i32     + start;
    if (col.i64)     sub.i64     = col.i64     + start;
    if (col.f32)     sub.f32     = col.f32     + start;
    if (col.f64)     sub.f64     = col.f64     + start;
    if (col.boolean) sub.boolean = col.boolean + start;
    if (col.strs)    sub.strs    = col.strs    + start;
    if (col.dec_raw) sub.dec_raw = col.dec_raw + start * (size_t)col.dec_width;
  }
  if (col.validity) sub.validity = col.validity + (start >> 3);
  return sub;
}

// header ++ body, sized exactly.
inline std::vector<uint8_t> concat_page(const std::vector<uint8_t> &header,
                                        const std::vector<uint8_t> &body) {
  std::vector<uint8_t> out;
  out.reserve(header.size() + body.size());
  out.insert(out.end(), header.begin(), header.end());
  out.insert(out.end(), body.begin(), body.end());
  return out;
}

// ---- per-column data page (v1) ----
//
// body = [4-byte LE def-rle length][def rle][values]. Repetition levels
// omitted (max rep level = 0). The body is compressed per `codec`; the page
// header records both uncompressed and compressed body sizes.
inline PageBuild build_data_page(const ColumnInput &col, size_t num_rows,
                                 int codec, int zstd_level) {
  std::vector<uint8_t> body;
  std::vector<uint8_t> def = encode_def_levels(col.validity, num_rows);
  put_u32_le(body, (uint32_t)def.size());
  body.insert(body.end(), def.begin(), def.end());
  encode_values(col, num_rows, body);

  size_t uncompressed_body = body.size();

  // PageHeader (Compact Protocol). Only compressed_page_size differs between
  // the stored and plain variants, so the header is built per variant.
  auto header = [&](size_t stored_size) {
    TCompactWriter h;
    h.structBegin();
    h.writeI32Field(1, PAGE_DATA);                    // type
    h.writeI32Field(2, (int32_t)uncompressed_body);   // uncompressed_page_size
    h.writeI32Field(3, (int32_t)stored_size);         // compressed_page_size
    h.writeFieldHeader(CT_STRUCT, 5);                 // data_page_header
    h.structBegin();
    h.writeI32Field(1, (int32_t)num_rows);            // num_values (incl. nulls)
    h.writeI32Field(2, ENC_PLAIN);                    // encoding
    h.writeI32Field(3, ENC_RLE);                      // definition_level_encoding
    h.writeI32Field(4, ENC_RLE);                      // repetition_level_encoding (required)
    h.structEnd();
    h.structEnd();
    return h.buf;
  };

  PageBuild pb;
  if (codec == CODEC_ZSTD) {
    std::vector<uint8_t> stored = zstd_compress_block(body, zstd_level);
    pb.bytes = concat_page(header(stored.size()), stored);
    pb.plain_bytes = concat_page(header(uncompressed_body), body);
    pb.uncompressed_total = pb.plain_bytes.size();
  } else {
    pb.bytes = concat_page(header(uncompressed_body), body);
    pb.uncompressed_total = pb.bytes.size();
  }
  return pb;
}

// ---- per-column ARRAY (LIST) data page (v1) ----
//
// body = [4-byte rep-rle length][rep rle][4-byte def-rle length][def rle]
//        [element values]. Repetition levels are present (max rep = 1, bw 1);
// definition levels use the 3-level LIST scheme (max def = 3, bw 2). Only the
// num_elements present (def==3) element values are PLAIN-encoded.
inline PageBuild build_array_data_page(const ColumnInput &col, int codec,
                                       int zstd_level) {
  // All-nullable nesting scheme: max_rep == array_depth, max_def ==
  // 2*array_depth + 1. RLE level packing needs the bit width of each max
  // (depth 1: rep bw 1, def bw 2; depth 2: rep bw 2, def bw 3).
  const int rep_bw = level_bit_width((uint32_t)col.array_depth);
  const int def_bw = level_bit_width((uint32_t)(2 * col.array_depth + 1));
  std::vector<uint8_t> body;
  std::vector<uint8_t> rep = encode_levels_rle(col.rep_levels, col.num_levels, rep_bw);
  put_u32_le(body, (uint32_t)rep.size());
  body.insert(body.end(), rep.begin(), rep.end());
  std::vector<uint8_t> def = encode_levels_rle(col.def_levels, col.num_levels, def_bw);
  put_u32_le(body, (uint32_t)def.size());
  body.insert(body.end(), def.begin(), def.end());

  // Element values: a primitive view over the flattened present elements.
  ColumnInput elem = col;
  elem.type = col.elem_type;
  elem.is_utf8 = col.elem_is_utf8;
  elem.validity = nullptr; // present-only; nullity is in the def levels
  elem.logical = LK_NONE;
  encode_values(elem, col.num_elements, body);

  size_t uncompressed_body = body.size();

  auto header = [&](size_t stored_size) {
    TCompactWriter h;
    h.structBegin();
    h.writeI32Field(1, PAGE_DATA);
    h.writeI32Field(2, (int32_t)uncompressed_body);
    h.writeI32Field(3, (int32_t)stored_size);
    h.writeFieldHeader(CT_STRUCT, 5); // data_page_header
    h.structBegin();
    h.writeI32Field(1, (int32_t)col.num_levels); // num_values = level count
    h.writeI32Field(2, ENC_PLAIN);
    h.writeI32Field(3, ENC_RLE); // definition_level_encoding
    h.writeI32Field(4, ENC_RLE); // repetition_level_encoding
    h.structEnd();
    h.structEnd();
    return h.buf;
  };

  PageBuild pb;
  if (codec == CODEC_ZSTD) {
    std::vector<uint8_t> stored = zstd_compress_block(body, zstd_level);
    pb.bytes = concat_page(header(stored.size()), stored);
    pb.plain_bytes = concat_page(header(uncompressed_body), body);
    pb.uncompressed_total = pb.plain_bytes.size();
  } else {
    pb.bytes = concat_page(header(uncompressed_body), body);
    pb.uncompressed_total = pb.bytes.size();
  }
  return pb;
}

// ---- page splitting (byte-size-triggered, independent per column) ----
//
// A column chunk's ColumnMetaData fields (num_values, data_page_offset,
// total_compressed/uncompressed_size) are CHUNK-level aggregates — how many
// data pages compose the chunk is invisible at that level, so splitting here
// needs no footer changes (unlike row-group splitting, where num_values is
// itself a per-row-group value). The reader already loops page headers until
// it has consumed total_compressed_size bytes (needed to read files written
// by other tools, which always page-split), so an unmodified reader handles
// multi-page chunks with zero changes on that side either.
//
// max_page_bytes == 0 disables splitting (single page per chunk, previous
// behavior — the only path exercised before this feature existed). rows/page
// comes from rows_per_page_for.
//
// Dictionary-encoded chunks (build_dict_column) split too, on the same knob and
// the same row grid: one shared dictionary page followed by N RLE_DICTIONARY
// data pages. They have to — a page index over single-page dict chunks would
// prune at row-group granularity on exactly the low-cardinality clustered
// columns page pruning exists for. Arrays split via build_array_data_pages and
// carry no page index (no leaf statistics, same rule as the footer's
// Statistics).
//
// `want_index` asks for per-page bounds/offsets (PageMeta) alongside the bytes.
// It costs one extra stats pass over the column, so it is off unless a page
// index is actually going to be written.

inline PageBuild build_data_pages(const ColumnInput &col, size_t rg_rows,
                                  int codec, int zstd_level,
                                  size_t max_page_bytes, bool want_index) {
  if (max_page_bytes == 0 || rg_rows <= 1) {
    PageBuild pb = build_data_page(col, rg_rows, codec, zstd_level);
    if (want_index)
      pb.pages.push_back(make_page_meta(col, rg_rows, 0, pb, codec));
    return pb;
  }

  const size_t rows_per_page = rows_per_page_for(col, rg_rows, max_page_bytes);

  std::vector<uint8_t> out;
  std::vector<uint8_t> plain_out;
  std::vector<PageMeta> pages;
  size_t total_uncompressed = 0;
  for (size_t start = 0; start < rg_rows; start += rows_per_page) {
    size_t count = std::min(rows_per_page, rg_rows - start);
    ColumnInput sub = slice_rows(col, start);
    PageBuild pb = build_data_page(sub, count, codec, zstd_level);
    if (want_index)
      pages.push_back(make_page_meta(sub, count, (int64_t)start, pb, codec));
    out.insert(out.end(), pb.bytes.begin(), pb.bytes.end());
    plain_out.insert(plain_out.end(), pb.plain_bytes.begin(), pb.plain_bytes.end());
    total_uncompressed += pb.uncompressed_total;
  }
  PageBuild result;
  result.bytes = std::move(out);
  result.plain_bytes = std::move(plain_out);
  result.uncompressed_total = total_uncompressed;
  result.pages = std::move(pages);
  return result;
}

inline PageBuild build_array_data_pages(const ColumnInput &rg_col, int codec,
                                        int zstd_level, size_t rg_rows,
                                        size_t max_page_bytes) {
  if (max_page_bytes == 0 || rg_rows <= 1)
    return build_array_data_page(rg_col, codec, zstd_level);

  // Derive this row group's own row->level and row->element boundaries by
  // walking its rep/def streams once (rep==0 marks a new row; def==max_def
  // marks a present leaf element, i.e. one consumed elem_* entry). Entirely
  // local to rg_col's already row-group-scoped buffers — no dependency on
  // the caller's global row_level_offsets/row_element_offsets.
  const int max_def = 2 * rg_col.array_depth + 1;
  std::vector<uint32_t> row_lvl(rg_rows + 1);
  std::vector<uint32_t> row_elem(rg_rows + 1);
  size_t row = 0, elem_count = 0;
  row_lvl[0] = 0;
  row_elem[0] = 0;
  for (size_t i = 0; i < rg_col.num_levels; i++) {
    if (rg_col.rep_levels[i] == 0 && i > 0) {
      row++;
      row_lvl[row] = (uint32_t)i;
      row_elem[row] = (uint32_t)elem_count;
    }
    if (rg_col.def_levels[i] == max_def) elem_count++;
  }
  row++;
  row_lvl[row] = (uint32_t)rg_col.num_levels;
  row_elem[row] = (uint32_t)elem_count;

  // Rough bytes/row estimate (levels are 1 byte each pre-RLE; element payload
  // measured for strings, fixed-width otherwise) — good enough to pick a page
  // boundary; RLE/zstd make the final byte size the authority, not this.
  size_t elem_bytes;
  if (rg_col.elem_type == PT_BYTE_ARRAY) {
    size_t total_str = 0;
    for (size_t i = 0; i < rg_col.num_elements; i++) total_str += 4 + rg_col.strs[i].len;
    elem_bytes = total_str;
  } else if (rg_col.elem_type == PT_BOOLEAN) {
    elem_bytes = (rg_col.num_elements + 7) / 8;
  } else {
    // Leaves are stored at their own width (INT32/FLOAT are 4 bytes, INT64/
    // DOUBLE are 8) — this only sizes a page boundary, but an 8-byte assumption
    // over a 4-byte leaf halves the rows a page holds for no reason.
    const size_t elem_width =
        (rg_col.elem_type == PT_INT32 || rg_col.elem_type == PT_FLOAT) ? 4 : 8;
    elem_bytes = rg_col.num_elements * elem_width;
  }
  size_t approx_bytes = rg_col.num_levels * 2 /* rep+def, pre-RLE */ + elem_bytes;
  double bpr = rg_rows > 0 ? (double)approx_bytes / (double)rg_rows : 1.0;
  size_t rows_per_page = (size_t)((double)max_page_bytes / std::max(1.0, bpr));
  if (rows_per_page == 0) rows_per_page = 8;
  rows_per_page = (rows_per_page + 7) & ~(size_t)7;

  std::vector<uint8_t> out;
  std::vector<uint8_t> plain_out;
  size_t total_uncompressed = 0;
  for (size_t start = 0; start < rg_rows; start += rows_per_page) {
    size_t count = std::min(rows_per_page, rg_rows - start);
    uint32_t lvl_s = row_lvl[start],  lvl_e = row_lvl[start + count];
    uint32_t el_s  = row_elem[start], el_e  = row_elem[start + count];
    ColumnInput sub = rg_col;
    sub.rep_levels   = rg_col.rep_levels + lvl_s;
    sub.def_levels   = rg_col.def_levels + lvl_s;
    sub.num_levels   = lvl_e - lvl_s;
    sub.num_elements = el_e - el_s;
    if (rg_col.i32)     sub.i32     = rg_col.i32     + el_s;
    if (rg_col.i64)     sub.i64     = rg_col.i64     + el_s;
    if (rg_col.f32)     sub.f32     = rg_col.f32     + el_s;
    if (rg_col.f64)     sub.f64     = rg_col.f64     + el_s;
    if (rg_col.boolean) sub.boolean = rg_col.boolean + el_s;
    if (rg_col.strs)    sub.strs    = rg_col.strs    + el_s;
    PageBuild pb = build_array_data_page(sub, codec, zstd_level);
    out.insert(out.end(), pb.bytes.begin(), pb.bytes.end());
    plain_out.insert(plain_out.end(), pb.plain_bytes.begin(), pb.plain_bytes.end());
    total_uncompressed += pb.uncompressed_total;
  }
  PageBuild result;
  result.bytes = std::move(out);
  result.plain_bytes = std::move(plain_out);
  result.uncompressed_total = total_uncompressed;
  return result;
}

// ---- dictionary encoding ----
//
// Auto-build gate: BYTES, not entry count. A dictionary is built when the
// bytes it would occupy beat the bytes PLAIN would occupy:
//
//     dict_page_bytes + code_bytes  <=  plain_bytes * DICT_BYTE_TOLERANCE
//
// where, over the `n` present (non-null) values of one column chunk:
//
//   plain_bytes     = n*w                 (fixed width w)
//                   = n*4 + sum(len)      (BYTE_ARRAY: 4-byte length prefix)
//   dict_page_bytes = NDV*w    /  NDV*4 + sum(len over DISTINCT values)
//   code_bytes      = n * bit_width(NDV) / 8
//
// This replaces a single distinct-ratio constant (0.5) that priced an 8-byte
// integer exactly like a 54-byte URL. The ratio at which dictionary encoding
// starts to pay is width-dependent, and the inequality above IS that
// dependence: rearranged for a distinct ratio r = NDV/n it reads
// r = 1 - bit_width(NDV)/(8*value_bytes). Measured against public.github.events
// that reproduces the observed break-evens — INT64 `repo_id` 0.719 predicted vs
// 0.734 measured, and a 54-byte `actor_avatar_url` 0.963 predicted vs 0.963
// measured. The old 0.5 was far too tight for wide strings and right only by
// accident for narrow integers.
//
// ⛔ The comparison is UNCOMPRESSED bytes on both sides. ZSTD may reorder the
// winner; deciding truthfully would mean encoding both ways and keeping the
// smaller, which doubles the encode work. Ruled (2026-09-16): stay
// uncompressed-only and say so here rather than pretend the gate is
// codec-aware.
//
// The tolerance lets a dictionary win at slight byte parity, because a
// dictionary column is also cheaper to READ — a sorted dictionary turns a
// predicate into a contiguous code range, and the dict-skip probe can reject a
// chunk without touching the data page. 1.05 buys that for at most 5% of bytes.
static const double DICT_BYTE_TOLERANCE = 1.05;

// Ceiling on the dictionary PAGE — the constraint the old entry-count cap
// claimed to enforce and did not: entry count is not byte count for BYTE_ARRAY,
// and 2^20 sixty-byte URLs is a 60 MB dictionary page.
//
// The ceiling is RELATIVE to the column chunk, not absolute. A fixed ceiling
// (1 MiB, pyarrow's `dictionary_pagesize_limit` default) was measured to be the
// binding constraint in exactly the regime this gate was redesigned for: 114k
// distinct 55-byte URLs is a 6.7 MB dictionary page against an 11.8 MB PLAIN
// column, so a fixed 1 MiB refused it before the width-aware inequality above
// got a vote — 0.00% where the inequality alone measured -12.5%. A ceiling
// below ~0.6x of PLAIN re-creates that block, which is why the fraction is
// generous: the INEQUALITY is the economics, and the ceiling is only a guard
// against an absolutely enormous page. A dictionary can never be larger than
// PLAIN anyway — the inequality already forbids it.
//
// The absolute floor keeps small columns from being gated into nothing: below
// ~1.4 MB of PLAIN the floor is what applies.
static const size_t DICT_MIN_PAGE_BYTES = 1u << 20; // 1 MiB floor
static const double DICT_MAX_PAGE_FRACTION = 0.75;  // of the chunk's PLAIN bytes

// Codes are uint32_t, so the dictionary cannot exceed UINT32_MAX entries. With
// a relative ceiling that bound is no longer implied by the ceiling itself (the
// old 2^20 cap implied it), and the smallest BYTE_ARRAY entry is 4 bytes, so it
// is clamped explicitly. Reaching it needs a ~17 GB dictionary page; the clamp
// exists so overflow is impossible rather than merely implausible.
static const size_t DICT_MAX_BUDGET_BYTES = (size_t)0xFFFFFFFFull * 4ull;

// PLAIN-encoded width of one value, or 0 for the variable-width types. Only the
// types the auto-build switch below actually handles return non-zero; BOOLEAN
// and FLBA never reach the gate.
inline size_t dict_fixed_value_bytes(int type) {
  switch (type) {
  case PT_INT32:
  case PT_FLOAT:
    return 4;
  case PT_INT64:
  case PT_DOUBLE:
    return 8;
  default:
    return 0;
  }
}

// BuiltDict owns the dictionary values + per-row codes produced by an
// auto-build; it must outlive the build_dict_column call that reads it.
struct BuiltDict {
  std::vector<int32_t> i32;
  std::vector<int64_t> i64;
  std::vector<float> f32;
  std::vector<double> f64;
  std::vector<StrSlice> strs;
  std::vector<uint32_t> codes; // one code per logical row (null rows => 0)
};

// Minimum bit width to represent dict codes [0, dict_count). Never 0: a
// single-entry dictionary still needs bit_width==1 so the RLE_DICTIONARY data
// page carries a real RLE run of zero-codes. Emitting bit_width==0 (empty index
// stream) only round-trips with our own reader's bit_width==0 fast path — strict
// readers (Arrow "Invalid number of indices: 0", DuckDB "Out of buffer") reject
// it. Conformance beats the byte we'd save on degenerate columns.
inline int dict_bit_width(uint32_t dict_count) {
  if (dict_count <= 1)
    return 1;
  uint32_t maxv = dict_count - 1;
  int bw = 0;
  while (maxv) {
    bw++;
    maxv >>= 1;
  }
  return bw;
}

// RLE/bit-packing-hybrid encode of dictionary codes (the RLE_DICTIONARY data
// stream, no length prefix). A run of >=8 equal codes becomes an RLE run;
// everything else is bit-packed, LSB-first, at `bit_width`. Inverse of
// DecodeRLEBitPackedIndicesNoPrefix in decode_encodings.cpp.
//
// INVARIANT (load-bearing): a bit-packed run always decodes exactly groups*8
// values — the reader has no per-run value count, it unpacks whole groups of 8
// and only the page-level num_values caps the total. So zero-padding the final
// group of a bit-packed run is ONLY safe when that group is the last thing in
// the stream (its pad values fall past num_values and are dropped). Padding a
// bit-packed run that is followed by any further run injects phantom values
// that shift every subsequent code — catastrophic corruption on high-cardinality
// columns (mostly-literal streams with occasional runs). Therefore a literal
// segment emits only whole groups of 8; a non-multiple-of-8 tail that precedes
// more data is emitted as RLE runs instead (RLE run length may be any value >=1,
// so this is always legal), and only a tail at the very end of the stream is
// bit-packed with padding.
inline std::vector<uint8_t> encode_dict_indices(const uint32_t *codes, size_t n,
                                                int bit_width) {
  std::vector<uint8_t> out;
  const int value_bytes = (bit_width + 7) / 8;
  const uint32_t mask =
      (bit_width >= 32) ? 0xFFFFFFFFu : ((1u << bit_width) - 1u);
  auto emit_rle = [&](uint32_t val, size_t run) {
    put_varint(out, (uint64_t)run << 1); // low bit 0 => RLE run
    uint32_t v = val & mask;
    for (int b = 0; b < value_bytes; b++)
      out.push_back((uint8_t)((v >> (8 * b)) & 0xFF));
  };
  // Bit-pack `count` codes starting at `base`, padding the final group with
  // zeros. Safe mid-stream only when count is a multiple of 8 (no padding);
  // the padded (count % 8 != 0) form is reserved for the stream's final run.
  auto emit_bitpacked = [&](const uint32_t *base, size_t count) {
    size_t groups = (count + 7) / 8;
    put_varint(out, ((uint64_t)groups << 1) | 1u); // low bit 1 => bit-packed
    uint64_t acc = 0;
    int nbits = 0;
    size_t total = groups * 8; // values, zero-padded to a whole group
    for (size_t k = 0; k < total; k++) {
      uint32_t v = (k < count) ? (base[k] & mask) : 0u;
      acc |= (uint64_t)v << nbits;
      nbits += bit_width;
      while (nbits >= 8) {
        out.push_back((uint8_t)(acc & 0xFF));
        acc >>= 8;
        nbits -= 8;
      }
    }
    // total*bit_width is a whole number of bytes, so acc is drained here.
  };
  size_t i = 0;
  while (i < n) {
    size_t run = 1;
    while (i + run < n && codes[i + run] == codes[i])
      run++;
    if (run >= 8) {
      emit_rle(codes[i], run);
      i += run;
      continue;
    }
    // Literal segment [lit_start, j): consecutive codes up to the next run>=8
    // (or the end of the stream).
    size_t lit_start = i;
    size_t j = i;
    while (j < n) {
      size_t r = 1;
      while (j + r < n && codes[j + r] == codes[j])
        r++;
      if (r >= 8)
        break;
      j += r;
    }
    size_t lit_n = j - lit_start;
    size_t full = (lit_n / 8) * 8; // whole groups of 8
    if (full > 0)
      emit_bitpacked(codes + lit_start, full);
    size_t leftover = lit_n - full; // 0..7 trailing codes
    if (leftover > 0) {
      if (j >= n) {
        // Final run of the stream: padding is safe (reader caps at num_values).
        emit_bitpacked(codes + lit_start + full, leftover);
      } else {
        // Followed by more data: emit the tail as RLE runs so no padding is
        // injected mid-stream.
        size_t k = lit_start + full;
        while (k < lit_start + lit_n) {
          size_t r = 1;
          while (k + r < lit_start + lit_n && codes[k + r] == codes[k])
            r++;
          emit_rle(codes[k], r);
          k += r;
        }
      }
    }
    i = j;
  }
  return out;
}

// ── Dictionary-encoding decision: ONE hash pass, KMV, then reuse ────────────
//
// Every present value is hashed EXACTLY ONCE, into `row_hashes`. Those hashes
// then serve both halves of the decision:
//
//   1. a KMV sketch over them estimates the column's distinct count, and
//   2. if that says proceed, the dictionary is built FROM THE SAME HASHES
//      rather than hashing the column a second time.
//
// What this replaces: a 512-row strided sample whose distinct RATIO decided
// whether to attempt the build. That statistic was structurally wrong, not
// merely coarse. Distinct-count-in-a-sample is not an estimator of
// distinct-fraction-in-a-column: drawing m values from a column holding NDV
// distinct ones yields about NDV(1 - e^(-m/NDV)) distinct, which saturates
// toward m — that is, toward "100% distinct" — for every NDV much larger than
// m. Expected collisions are ~m^2/(2*NDV), so at m=512 the ratio only falls
// below 0.95 when NDV is under ~5,000, and the pre-check therefore declined
// dictionary encoding for essentially EVERY column above ~5k distinct values,
// however repetitive.
//
// Measured on public.github.events row group 0 (262,144 rows), before this:
// dictionary=True and dictionary=False produced BYTE-IDENTICAL output
// (11,574,990 bytes both) — the pre-check rejected all 8 actor/repo columns,
// including the integer ones. Their real distinct ratios are 0.23-0.28 against
// a 0.5 gate, and feeding the writer dict-shaped vectors instead (PRESERVE
// mode, which bypasses this gate entirely) measured 44.15 -> 31.83 B/row,
// -27.9%, ~4.1 GB on that one dataset.
//
// K=1024, the transient width: this sketch dies at the end of the column, so
// its only cost is 8KB and it buys ~3% relative standard error against ~18.9%
// at K=32. Measured against true NDV on that same row group: actor_login
// 61,450 vs 60,171 true; repo_id 74,494 vs 73,432; type 16 vs 16 (EXACT, the
// sketch never filled); id 263,037 vs 262,144.
//
// The family tag is DECISION-ONLY and that is the point: this sketch is never
// stored anywhere, so it is bound by nothing except being consistent with
// itself for one column, and tagging it means it can never be merged with a
// STORED skene or ANALYZE sketch (which would be meaningless — see
// draken/core/kmv_sketch.h).
using DictDecisionSketch =
    draken::KmvSketch<1024u, draken::KmvHashFamily::kRugoWriterDecision>;

// Estimated distinct count over the present rows, from hashes already computed.
inline double dict_estimated_ndv(const std::vector<uint64_t> &row_hashes,
                                 const uint8_t *validity, size_t num_rows) {
  DictDecisionSketch sketch;
  for (size_t i = 0; i < num_rows; i++) {
    if (!is_valid(validity, i))
      continue;
    sketch.add(row_hashes[i]);
  }
  return sketch.estimate();
}

// How far over the budget the ESTIMATE must sit before the build is skipped.
//
// This is a skip-the-work threshold, not the gate. The gate is the BYTE budget,
// enforced exactly, per value, inside the builders below — this only decides
// whether attempting the build is worth the pass. The margin exists so the
// estimator's own error cannot decline a column the builder would have
// accepted: at K=1024 the relative standard error is ~3%, so 1.25x is beyond 8
// sigma. It carries a second job for BYTE_ARRAY, where the DISTINCT values'
// byte size is unknowable before the build and is priced at the mean
// present-value length — the margin absorbs that length skew too.
static const double DICT_ESTIMATE_SKIP_MARGIN = 1.25;

// Total PLAIN bytes the present values of this column chunk would occupy. For
// BYTE_ARRAY this walks the lengths only — the payload bytes are never touched.
inline size_t dict_plain_bytes(const ColumnInput &col, size_t num_rows,
                               size_t present) {
  const size_t w = dict_fixed_value_bytes(col.type);
  if (w != 0)
    return present * w;
  if (col.type != PT_BYTE_ARRAY)
    return 0;
  size_t total = 0;
  for (size_t i = 0; i < num_rows; i++) {
    if (!is_valid(col.validity, i))
      continue;
    total += 4u + (size_t)col.strs[i].len;
  }
  return total;
}

// The byte budget a dictionary build must stay inside, plus whether attempting
// the build is worth the pass at all.
//
// `ndv_lo` / `ndv_hi` bracket the distinct count: equal when the bloom filter
// handed us an exact one, spread by the estimator's margin otherwise. They are
// used in the directions that keep each decision honest — the HIGH count sets
// the code bit width (more code bytes => tighter budget, never optimistic), the
// LOW count decides whether to attempt (never skips a column the builder might
// have accepted).
struct DictBudget {
  bool worth_attempting = false;
  size_t byte_budget = 0; // dictionary-page bytes the build may not exceed
};

inline DictBudget dict_byte_budget(const ColumnInput &col, size_t num_rows,
                                   size_t present, size_t ndv_lo,
                                   size_t ndv_hi) {
  DictBudget out;
  const size_t plain_bytes = dict_plain_bytes(col, num_rows, present);
  if (plain_bytes == 0 || present == 0 || ndv_lo == 0)
    return out;

  const uint32_t bw_ndv =
      (uint32_t)std::min<size_t>(ndv_hi ? ndv_hi : 1u, 0xFFFFFFFFu);
  const size_t code_bytes =
      (present * (size_t)dict_bit_width(bw_ndv) + 7u) / 8u;

  const double allowed =
      (double)plain_bytes * DICT_BYTE_TOLERANCE - (double)code_bytes;
  if (allowed <= 0.0)
    return out; // the codes alone already cost more than PLAIN
  const size_t page_ceiling = std::max<size_t>(
      DICT_MIN_PAGE_BYTES, (size_t)((double)plain_bytes * DICT_MAX_PAGE_FRACTION));
  out.byte_budget = std::min<size_t>((size_t)allowed, page_ceiling);
  out.byte_budget = std::min<size_t>(out.byte_budget, DICT_MAX_BUDGET_BYTES);

  // Skip-the-work prediction. For a fixed width the dictionary's byte size is
  // known exactly from the distinct count, so this is the real answer when the
  // count is exact. For BYTE_ARRAY the DISTINCT values' byte size is unknowable
  // until they are seen, so this prices them at the mean present-value length
  // and leans generous by the same margin — the running budget inside
  // build_string_dict is what actually enforces the gate, per value.
  const size_t w = dict_fixed_value_bytes(col.type);
  double predicted;
  double ceiling = (double)out.byte_budget;
  if (w != 0) {
    predicted = (double)ndv_lo * (double)w;
  } else {
    predicted = (double)ndv_lo * ((double)plain_bytes / (double)present);
    ceiling *= DICT_ESTIMATE_SKIP_MARGIN;
  }
  out.worth_attempting = predicted <= ceiling;
  return out;
}

// ── Dictionary build over PRECOMPUTED row hashes ───────────────────────────
//
// Open-addressed hash -> code table keyed by the hash computed above, so the
// build costs a probe per row and no second hash pass. Power-of-two capacity,
// linear probing, doubled at 70% load; a rehash only moves (hash, row, code)
// triples and never re-reads a value.
//
// ⛔ The VALUE is compared on every hash match, and that is a correctness
// obligation rather than a tuning choice: XXH64 collides, and two distinct
// values sharing one dictionary entry would silently write one value in place
// of the other. `eq` is bit equality — which is what the std::unordered_map
// keys this replaces meant for every type, integers by `==` and floats by the
// bit pattern they were explicitly memcpy'd into.
class HashedDictIndex {
public:
  explicit HashedDictIndex(size_t expected) {
    size_t want = 16;
    while (want < expected * 2u)
      want <<= 1;
    slots_.assign(want, Slot{});
    mask_ = want - 1u;
  }

  // Returns the existing code for `hash`/`row`, or kAbsent with `at` set to the
  // slot the caller must fill via `insert`.
  static const uint32_t kAbsent = 0xFFFFFFFFu;

  template <typename EqFn>
  uint32_t find(uint64_t hash, size_t row, EqFn &&eq, size_t &at) const {
    size_t idx = (size_t)(hash * 0x9E3779B97F4A7C15ull >> 32) & mask_;
    for (;;) {
      const Slot &s = slots_[idx];
      if (!s.used) {
        at = idx;
        return kAbsent;
      }
      if (s.hash == hash && eq(s.row, row))
        return s.code;
      idx = (idx + 1u) & mask_;
    }
  }

  void insert(size_t at, uint64_t hash, size_t row, uint32_t code) {
    slots_[at] = Slot{hash, (uint32_t)row, code, true};
    if (++count_ * 10u > slots_.size() * 7u)
      grow();
  }

private:
  struct Slot {
    uint64_t hash = 0;
    uint32_t row = 0;
    uint32_t code = 0;
    bool used = false;
  };

  void grow() {
    std::vector<Slot> bigger(slots_.size() * 2u, Slot{});
    const size_t mask = bigger.size() - 1u;
    for (const Slot &s : slots_) {
      if (!s.used)
        continue;
      size_t idx = (size_t)(s.hash * 0x9E3779B97F4A7C15ull >> 32) & mask;
      while (bigger[idx].used)
        idx = (idx + 1u) & mask;
      bigger[idx] = s;
    }
    slots_.swap(bigger);
    mask_ = mask;
  }

  std::vector<Slot> slots_;
  size_t mask_ = 0;
  size_t count_ = 0;
};

// Auto-build a dictionary over fixed-width values from the precomputed row
// hashes. Returns false (=> emit PLAIN) once the distinct count exceeds the
// gate; on success `dict` holds the unique values in first-seen order and
// `codes` one code per logical row.
//
// Values are keyed on their BIT PATTERN via memcmp, so this one template serves
// integers and floats alike: -0.0 and +0.0 stay distinct dictionary entries and
// NaN payloads are preserved verbatim, and the dictionary round-trips the exact
// stored value rather than an ==-equivalent one.
template <typename T>
inline bool build_numeric_dict(const T *vals, const uint8_t *validity,
                               size_t num_rows, size_t byte_budget,
                               const std::vector<uint64_t> &row_hashes,
                               size_t expected_distinct, std::vector<T> &dict,
                               std::vector<uint32_t> &codes) {
  const size_t cap = byte_budget / sizeof(T); // entries the budget affords
  HashedDictIndex index(std::min(expected_distinct, cap));
  auto eq = [vals](size_t a, size_t b) {
    return std::memcmp(&vals[a], &vals[b], sizeof(T)) == 0;
  };
  codes.assign(num_rows, 0);
  for (size_t i = 0; i < num_rows; i++) {
    if (!is_valid(validity, i))
      continue;
    size_t at = 0;
    const uint32_t found = index.find(row_hashes[i], i, eq, at);
    if (found != HashedDictIndex::kAbsent) {
      codes[i] = found;
    } else {
      if (dict.size() >= cap)
        return false;
      const uint32_t code = (uint32_t)dict.size();
      index.insert(at, row_hashes[i], i, code);
      dict.push_back(vals[i]);
      codes[i] = code;
    }
  }
  return true;
}

inline bool build_string_dict(const StrSlice *vals, const uint8_t *validity,
                              size_t num_rows, size_t byte_budget,
                              const std::vector<uint64_t> &row_hashes,
                              size_t expected_distinct,
                              std::vector<StrSlice> &dict,
                              std::vector<uint32_t> &codes) {
  // The dictionary's byte size is only knowable as the distinct values arrive,
  // so the budget is enforced here, per value, as a RUNNING total: 4 bytes of
  // PLAIN length prefix plus the payload, exactly what the dictionary page will
  // hold. `expected_distinct` sizes the index and is not a gate.
  size_t dict_bytes = 0;
  HashedDictIndex index(expected_distinct);
  auto eq = [vals](size_t a, size_t b) {
    return vals[a].len == vals[b].len &&
           std::memcmp(vals[a].ptr, vals[b].ptr, vals[a].len) == 0;
  };
  codes.assign(num_rows, 0);
  for (size_t i = 0; i < num_rows; i++) {
    if (!is_valid(validity, i))
      continue;
    size_t at = 0;
    const uint32_t found = index.find(row_hashes[i], i, eq, at);
    if (found != HashedDictIndex::kAbsent) {
      codes[i] = found;
    } else {
      const size_t entry_bytes = 4u + (size_t)vals[i].len;
      if (dict_bytes + entry_bytes > byte_budget)
        return false;
      dict_bytes += entry_bytes;
      const uint32_t code = (uint32_t)dict.size();
      index.insert(at, row_hashes[i], i, code);
      dict.push_back(vals[i]);
      codes[i] = code;
    }
  }
  return true;
}
// Build a dictionary page (PLAIN-encoded dict values) followed by an
// RLE_DICTIONARY data page. `col` must point its typed buffers at the
// `col.dict_count` dictionary values, with `col.codes` (one per logical row)
// and `col.validity` (per logical row) set. Returns the concatenated page
// bytes plus the dictionary-page length so the caller can locate the data page.
struct DictColumnBuild {
  std::vector<uint8_t> bytes;
  size_t uncompressed_total; // both page headers + both raw bodies
  size_t dict_page_len;      // bytes occupied by the dictionary page
  // The same two pages under CODEC_UNCOMPRESSED, for the
  // keep-whichever-is-smaller rule. Populated only when a compressing codec was
  // requested. The dictionary page has its own length in this variant, so the
  // caller must swap BOTH fields together or data_page_offset will point into
  // the middle of the dictionary page.
  std::vector<uint8_t> plain_bytes;
  size_t plain_dict_page_len = 0;
  // One entry per DATA page (the dictionary page is not listed). Empty unless
  // a page index was requested.
  std::vector<PageMeta> pages;
};

inline DictColumnBuild build_dict_column(const ColumnInput &col, size_t num_rows,
                                         int codec, int zstd_level,
                                         size_t max_page_bytes, bool want_index) {
  ColumnInput dv = col;
  dv.validity = nullptr; // dict values carry no nulls
  dv.codes = nullptr;    // read the dict buffer positionally [0, dict_count)

  // ---- sort the dictionary (WORM: pay the ordering cost once at write) ----
  //
  // A sorted dictionary lets a reader turn a range/equality predicate into a
  // contiguous CODE interval (two binary searches + an integer range compare),
  // advertised via DictionaryPageHeader.is_sorted. Sortable types: INT32/INT64
  // (incl. DATE32/TIMESTAMP64) numerically, BYTE_ARRAY by unsigned-byte
  // lexicographic order (matches compute_stats / Parquet BYTE_ARRAY ordering).
  // Floats are deliberately excluded — NaN / -0.0 break monotonic code ranges.
  // We emit the dictionary-page values in sorted order and remap each row's
  // code through the inverse permutation; column stats and bloom were computed
  // from the original codes upstream, so they are unaffected.
  const uint32_t D = col.dict_count;
  bool sorted = (col.type == PT_INT32 || col.type == PT_INT64 ||
                 col.type == PT_BYTE_ARRAY);
  std::vector<uint32_t> inv;     // inv[old_code] = new_code (sorted only)
  std::vector<int32_t> s_i32;    // dict values reordered for the dict page
  std::vector<int64_t> s_i64;
  std::vector<StrSlice> s_str;
  if (sorted && D > 0) {
    std::vector<uint32_t> perm(D); // perm[new_code] = old_code
    for (uint32_t k = 0; k < D; k++)
      perm[k] = k;
    // Order in the DECLARED domain: an unsigned value at or above the signed
    // midpoint occupies a negative slot, so a signed compare would emit a
    // dictionary that is not actually ordered while still advertising
    // is_sorted=true — breaking the code-interval search that flag promises.
    if (col.type == PT_INT64)
      std::sort(perm.begin(), perm.end(), [&](uint32_t a, uint32_t b) {
        return col.is_unsigned ? (uint64_t)col.i64[a] < (uint64_t)col.i64[b]
                               : col.i64[a] < col.i64[b];
      });
    else if (col.type == PT_INT32)
      std::sort(perm.begin(), perm.end(), [&](uint32_t a, uint32_t b) {
        return col.is_unsigned ? (uint32_t)col.i32[a] < (uint32_t)col.i32[b]
                               : col.i32[a] < col.i32[b];
      });
    else // PT_BYTE_ARRAY
      std::sort(perm.begin(), perm.end(), [&](uint32_t a, uint32_t b) {
        return str_lt(col.strs[a], col.strs[b]);
      });
    inv.assign(D, 0);
    for (uint32_t k = 0; k < D; k++)
      inv[perm[k]] = k;
    if (col.type == PT_INT64) {
      s_i64.resize(D);
      for (uint32_t k = 0; k < D; k++)
        s_i64[k] = col.i64[perm[k]];
      dv.i64 = s_i64.data();
    } else if (col.type == PT_INT32) {
      s_i32.resize(D);
      for (uint32_t k = 0; k < D; k++)
        s_i32[k] = col.i32[perm[k]];
      dv.i32 = s_i32.data();
    } else {
      s_str.resize(D);
      for (uint32_t k = 0; k < D; k++)
        s_str[k] = col.strs[perm[k]];
      dv.strs = s_str.data();
    }
  }

  // ---- dictionary page: PLAIN values, all present (in sorted order) ----
  std::vector<uint8_t> dict_body;
  encode_values(dv, col.dict_count, dict_body);
  size_t dict_uncompressed = dict_body.size();
  std::vector<uint8_t> dict_stored = (codec == CODEC_ZSTD)
                                         ? zstd_compress_block(dict_body, zstd_level)
                                         : dict_body;

  auto dict_header = [&](size_t stored_size) {
    TCompactWriter dh;
    dh.structBegin();
    dh.writeI32Field(1, PAGE_DICTIONARY);            // type
    dh.writeI32Field(2, (int32_t)dict_uncompressed); // uncompressed_page_size
    dh.writeI32Field(3, (int32_t)stored_size);       // compressed_page_size
    dh.writeFieldHeader(CT_STRUCT, 7);               // dictionary_page_header
    dh.structBegin();
    dh.writeI32Field(1, (int32_t)col.dict_count);    // num_values
    dh.writeI32Field(2, ENC_PLAIN);                  // encoding (PLAIN values)
    dh.writeBoolField(3, sorted);                    // is_sorted (see above)
    dh.structEnd();
    dh.structEnd();
    return dh.buf;
  };
  std::vector<uint8_t> dh_buf = dict_header(dict_stored.size());

  // ---- data pages: def levels, bit_width byte, RLE/bit-packed indices ----
  //
  // One page per row range. The dictionary page above is SHARED by all of them
  // (parquet allows exactly one dictionary page per column chunk); each data
  // page carries its own definition levels and its own slice of the code
  // stream, so a reader that jumps straight to page k still needs the
  // dictionary page but no other data page — which is what makes page-level
  // pruning work on a dict-encoded column.
  const int bw = dict_bit_width(col.dict_count);
  const size_t rows_per_page =
      (max_page_bytes == 0 || num_rows <= 1)
          ? num_rows
          : rows_per_page_for(col, num_rows, max_page_bytes);

  // Build one data page covering rows [start, start+count) of the chunk.
  auto build_one = [&](size_t start, size_t count) {
    std::vector<uint8_t> data_body;
    std::vector<uint8_t> def =
        encode_def_levels(col.validity ? col.validity + (start >> 3) : nullptr, count);
    put_u32_le(data_body, (uint32_t)def.size());
    data_body.insert(data_body.end(), def.begin(), def.end());
    data_body.push_back((uint8_t)bw);
    std::vector<uint32_t> present;
    present.reserve(count);
    for (size_t i = start; i < start + count; i++)
      if (is_valid(col.validity, i))
        present.push_back(sorted ? inv[col.codes[i]] : col.codes[i]);
    std::vector<uint8_t> idx =
        encode_dict_indices(present.data(), present.size(), bw);
    data_body.insert(data_body.end(), idx.begin(), idx.end());

    const size_t data_uncompressed = data_body.size();
    std::vector<uint8_t> data_stored =
        (codec == CODEC_ZSTD) ? zstd_compress_block(data_body, zstd_level) : data_body;

    auto data_header = [&](size_t stored_size) {
      TCompactWriter ph;
      ph.structBegin();
      ph.writeI32Field(1, PAGE_DATA);
      ph.writeI32Field(2, (int32_t)data_uncompressed);
      ph.writeI32Field(3, (int32_t)stored_size);
      ph.writeFieldHeader(CT_STRUCT, 5); // data_page_header
      ph.structBegin();
      ph.writeI32Field(1, (int32_t)count);     // num_values (incl. nulls)
      ph.writeI32Field(2, ENC_RLE_DICTIONARY); // encoding
      ph.writeI32Field(3, ENC_RLE);            // definition_level_encoding
      ph.writeI32Field(4, ENC_RLE);            // repetition_level_encoding
      ph.structEnd();
      ph.structEnd();
      return ph.buf;
    };

    PageBuild pb;
    pb.bytes = concat_page(data_header(data_stored.size()), data_stored);
    if (codec == CODEC_ZSTD) {
      pb.plain_bytes = concat_page(data_header(data_uncompressed), data_body);
      pb.uncompressed_total = pb.plain_bytes.size();
    } else {
      pb.uncompressed_total = pb.bytes.size();
    }
    return pb;
  };

  std::vector<uint8_t> data_stored_all;  // every data page, compressed variant
  std::vector<uint8_t> data_plain_all;   // every data page, uncompressed variant
  std::vector<PageMeta> pages;
  // A zero-row chunk still emits one (empty) data page, as the single-page
  // build always did — rows_per_page is 0 there and the loop runs exactly once.
  for (size_t start = 0;;) {
    const size_t count = (rows_per_page == 0) ? 0 : std::min(rows_per_page, num_rows - start);
    PageBuild pb = build_one(start, count);
    if (want_index) {
      ColumnInput sub = slice_rows(col, start);
      pages.push_back(make_page_meta(sub, count, (int64_t)start, pb, codec));
    }
    data_stored_all.insert(data_stored_all.end(), pb.bytes.begin(), pb.bytes.end());
    data_plain_all.insert(data_plain_all.end(), pb.plain_bytes.begin(), pb.plain_bytes.end());
    start += count;
    if (start >= num_rows) break;
  }

  auto assemble = [](const std::vector<uint8_t> &dhb,
                     const std::vector<uint8_t> &dbody,
                     const std::vector<uint8_t> &data_pages) {
    std::vector<uint8_t> b;
    b.reserve(dhb.size() + dbody.size() + data_pages.size());
    b.insert(b.end(), dhb.begin(), dhb.end());
    b.insert(b.end(), dbody.begin(), dbody.end());
    b.insert(b.end(), data_pages.begin(), data_pages.end());
    return b;
  };

  DictColumnBuild out;
  out.dict_page_len = dh_buf.size() + dict_stored.size();
  out.bytes = assemble(dh_buf, dict_stored, data_stored_all);
  out.pages = std::move(pages);
  if (codec == CODEC_ZSTD) {
    std::vector<uint8_t> pdh = dict_header(dict_uncompressed);
    out.plain_dict_page_len = pdh.size() + dict_uncompressed;
    out.plain_bytes = assemble(pdh, dict_body, data_plain_all);
    out.uncompressed_total = out.plain_bytes.size();
  } else {
    out.uncompressed_total = out.bytes.size();
  }
  return out;
}

// ---- schema serialization ----

// Emit an INTEGER(bitWidth, isSigned) LogicalType annotation as the current
// schema element's field 10 (logicalType). Matches PyArrow/parquet-mr output
// for narrow and unsigned integer columns: the physical type stays INT32/INT64
// and a conformant reader recovers the declared width and signedness. Field 10
// is the last field in a SchemaElement, preserving ascending field-id order.
inline void emit_int_logical(TCompactWriter &w, int bit_width, bool is_signed) {
  // Emits the legacy ConvertedType (field 6) AND the modern logicalType union
  // (field 10); the caller owns the enclosing SchemaElement struct's
  // structBegin/structEnd. Both are written because some readers (DuckDB) key
  // width/signedness detection on the legacy ConvertedType, matching
  // parquet-mr/PyArrow output. Field ids stay ascending (6 before 10); caller
  // has already written fields <= 4.
  int conv;
  if (is_signed)
    conv = bit_width == 8 ? CONV_INT_8
         : bit_width == 16 ? CONV_INT_16
         : bit_width == 32 ? CONV_INT_32
         : CONV_INT_64;
  else
    conv = bit_width == 8 ? CONV_UINT_8
         : bit_width == 16 ? CONV_UINT_16
         : bit_width == 32 ? CONV_UINT_32
         : CONV_UINT_64;
  w.writeI32Field(6, conv);            // ConvertedType INT_N / UINT_N
  w.writeFieldHeader(CT_STRUCT, 10);   // logicalType
  w.structBegin();                     //   LogicalType union
  w.writeFieldHeader(CT_STRUCT, 10);   //   INTEGER member (union field id 10)
  w.structBegin();                     //     IntType { 1: i8 bitWidth; 2: bool isSigned }
  w.writeFieldHeader(CT_BYTE, 1);      //       bitWidth (i8)
  w.writeByte((uint8_t)bit_width);
  w.writeBoolField(2, is_signed);      //       isSigned
  w.structEnd();                       //     IntType
  w.structEnd();                       //   LogicalType union
}

inline void write_schema(TCompactWriter &w, const std::vector<ColumnInput> &cols) {
  // Flat pre-order list: root + each column's subtree. A primitive is 1 element;
  // a LIST nested `array_depth` deep is (2*array_depth + 1): each level adds a
  // LIST group + a repeated "list" group, plus the single leaf "element".
  uint32_t n_elems = 1;
  for (const auto &c : cols)
    n_elems += c.is_array ? (uint32_t)(2 * c.array_depth + 1) : 1;
  w.writeFieldHeader(CT_LIST, 2); // FileMetaData.schema
  w.writeListHeader(CT_STRUCT, n_elems);

  // root
  w.structBegin();
  w.writeStringField(4, "schema");                 // name
  w.writeI32Field(5, (int32_t)cols.size());        // num_children
  w.structEnd();

  for (const auto &c : cols) {
    if (c.is_array) {
      // `array_depth` nested LIST levels then the leaf. Each level is a LIST
      // group (OPTIONAL) followed by a repeated "list" group. The outermost
      // group carries the column name; deeper LIST groups are named "element"
      // (the element of the enclosing list is itself a list). Mirrors Arrow/
      // PyArrow's encoding, e.g. depth 2:
      //   name(List) -> list -> element(List) -> list -> element(leaf).
      for (int lvl = 0; lvl < c.array_depth; lvl++) {
        w.structBegin();                           // LIST group
        w.writeI32Field(3, REP_OPTIONAL);          // repetition_type
        w.writeStringField(4, lvl == 0 ? c.name : std::string("element"));
        w.writeI32Field(5, 1);                     // num_children
        w.writeI32Field(6, CONV_LIST);             // converted_type LIST
        w.writeFieldHeader(CT_STRUCT, 10);         // logicalType
        w.structBegin();                           //   LogicalType union
        w.writeFieldHeader(CT_STRUCT, 3);          //   LIST member
        w.structBegin();                           //   ListType {}
        w.structEnd();
        w.structEnd();
        w.structEnd();

        w.structBegin();                           // repeated group "list"
        w.writeI32Field(3, REP_REPEATED);
        w.writeStringField(4, "list");
        w.writeI32Field(5, 1);                     // num_children
        w.structEnd();
      }

      w.structBegin();                             // "element" leaf
      w.writeI32Field(1, (int32_t)c.elem_type);    // type
      w.writeI32Field(3, REP_OPTIONAL);            // repetition_type
      w.writeStringField(4, "element");
      if (c.elem_type == PT_BYTE_ARRAY && c.elem_is_utf8)
        w.writeI32Field(6, CONV_UTF8);
      if (c.int_bit_width > 0)                    // narrow/unsigned leaf annotation
        emit_int_logical(w, c.int_bit_width, !c.is_unsigned);
      w.structEnd();
      continue;
    }
    w.structBegin();
    // Field order MUST be ascending: type(1), type_length(2), repetition(3),
    // name(4), converted_type(6), scale(7), precision(8), logicalType(10).
    w.writeI32Field(1, (int32_t)c.type);           // type
    if (c.type == PT_FLBA)
      w.writeI32Field(2, c.dec_width);             // type_length
    w.writeI32Field(3, REP_OPTIONAL);              // repetition_type
    w.writeStringField(4, c.name);                 // name

    if (c.logical == LK_DECIMAL) {
      w.writeI32Field(6, CONV_DECIMAL);
      w.writeI32Field(7, c.dec_scale);             // scale
      w.writeI32Field(8, c.dec_precision);         // precision
      w.writeFieldHeader(CT_STRUCT, 10);           // logicalType
      w.structBegin();                             //   LogicalType union
      w.writeFieldHeader(CT_STRUCT, 5);            //   DECIMAL member
      w.structBegin();                             //     DecimalType
      w.writeI32Field(1, c.dec_scale);
      w.writeI32Field(2, c.dec_precision);
      w.structEnd();
      w.structEnd();
    } else if (c.logical == LK_DATE) {
      w.writeI32Field(6, CONV_DATE);
      w.writeFieldHeader(CT_STRUCT, 10);
      w.structBegin();                             // LogicalType union
      w.writeFieldHeader(CT_STRUCT, 6);            // DATE member
      w.structBegin();                             // DateType {}
      w.structEnd();
      w.structEnd();
    } else if (c.logical == LK_INTERVAL) {
      // FLBA(12); only ConvertedType INTERVAL (no LogicalType union member).
      w.writeI32Field(6, CONV_INTERVAL);
    } else if (c.logical == LK_TIME) {
      if (c.ts_unit == TU_MILLIS)
        w.writeI32Field(6, CONV_TIME_MILLIS);
      else if (c.ts_unit == TU_MICROS)
        w.writeI32Field(6, CONV_TIME_MICROS);
      w.writeFieldHeader(CT_STRUCT, 10);
      w.structBegin();                             // LogicalType union
      w.writeFieldHeader(CT_STRUCT, 7);            // TIME member
      w.structBegin();                             // TimeType
      w.writeBoolField(1, c.ts_utc);               //   isAdjustedToUTC
      w.writeFieldHeader(CT_STRUCT, 2);            //   unit
      w.structBegin();                             //   TimeUnit union
      w.writeFieldHeader(CT_STRUCT, c.ts_unit);    //     MILLIS/MICROS/NANOS
      w.structBegin();
      w.structEnd();
      w.structEnd();
      w.structEnd();
      w.structEnd();
    } else if (c.logical == LK_TIMESTAMP) {
      // ConvertedType only covers MILLIS/MICROS; NANOS relies on logicalType.
      if (c.ts_unit == TU_MILLIS)
        w.writeI32Field(6, CONV_TIMESTAMP_MILLIS);
      else if (c.ts_unit == TU_MICROS)
        w.writeI32Field(6, CONV_TIMESTAMP_MICROS);
      w.writeFieldHeader(CT_STRUCT, 10);
      w.structBegin();                             // LogicalType union
      w.writeFieldHeader(CT_STRUCT, 8);            // TIMESTAMP member
      w.structBegin();                             // TimestampType
      w.writeBoolField(1, c.ts_utc);               //   isAdjustedToUTC
      w.writeFieldHeader(CT_STRUCT, 2);            //   unit
      w.structBegin();                             //   TimeUnit union
      w.writeFieldHeader(CT_STRUCT, c.ts_unit);    //     MILLIS/MICROS/NANOS
      w.structBegin();                             //     (empty)
      w.structEnd();
      w.structEnd();
      w.structEnd();
      w.structEnd();
    } else if (c.type == PT_BYTE_ARRAY && c.is_utf8) {
      w.writeI32Field(6, CONV_UTF8);               // converted_type
    } else if (c.int_bit_width > 0) {
      // Narrow and/or unsigned integer column: physical INT32/INT64 +
      // INTEGER(width, isSigned) logicalType. Bits are the signed reinterpret;
      // a conformant reader recovers the declared width and signedness. INT32
      // and INT64 carry no annotation (int_bit_width == 0) — the physical type
      // already states them exactly.
      emit_int_logical(w, c.int_bit_width, !c.is_unsigned);
    }
    w.structEnd();
  }
}

// ---- PageIndex serialization ----
//
// PageMeta and the two Thrift serializers live in page_index_writer.hpp, which
// depends on nothing but the Compact Protocol writer so page_index_test.cpp can
// round-trip writer output through the reader's parser directly.

// Map a column's parquet physical type onto the bound ordering the PageIndex
// writer needs. Returns false for a column that cannot carry a sound
// ColumnIndex: arrays (no leaf statistics, same rule as the footer's
// Statistics) and FLBA (DECIMAL/INTERVAL), whose stats bytes are big-endian
// two's complement and do not order byte-wise across zero.
inline bool page_index_bound_kind(const ColumnInput &c, PageBoundKind &kind) {
  if (c.is_array) return false;
  switch (c.type) {
  case PT_INT32:   kind = PB_INT32;  return true;
  case PT_INT64:   kind = PB_INT64;  return true;
  case PT_FLOAT:   kind = PB_FLOAT;  return true;
  case PT_DOUBLE:  kind = PB_DOUBLE; return true;
  case PT_BOOLEAN: kind = PB_BYTES;  return true;  // 1 byte, byte-wise order
  case PT_BYTE_ARRAY: kind = PB_BYTES; return true;
  default: return false;
  }
}

// ---- ColumnMetaData / ColumnChunk / RowGroup ----

inline void write_column_chunk(TCompactWriter &w, const ColumnInput &c,
                               size_t num_rows, int64_t data_page_offset,
                               int64_t dict_page_offset,
                               size_t compressed_total, size_t uncompressed_total,
                               int codec, const ColumnStats &stats,
                               int64_t bloom_offset, int32_t bloom_length,
                               int64_t oi_offset, int32_t oi_length,
                               int64_t ci_offset, int32_t ci_length) {
  w.structBegin(); // ColumnChunk
  // file_offset points at the first page of the chunk (the dictionary page
  // when present, otherwise the data page).
  w.writeI64Field(2, dict_page_offset >= 0 ? dict_page_offset : data_page_offset);
  w.writeFieldHeader(CT_STRUCT, 3);                // meta_data
  {
    w.structBegin(); // ColumnMetaData
    w.writeI32Field(1, (int32_t)(c.is_array ? c.elem_type : c.type)); // type
    // encodings: dict chunks declare [RLE_DICTIONARY, PLAIN, RLE] (data page,
    // dict page, def levels); plain chunks declare [PLAIN, RLE].
    if (dict_page_offset >= 0) {
      w.writeFieldHeader(CT_LIST, 2);
      w.writeListHeader(CT_I32, 3);
      w.writeListI32(ENC_RLE_DICTIONARY);
      w.writeListI32(ENC_PLAIN);
      w.writeListI32(ENC_RLE);
    } else {
      w.writeFieldHeader(CT_LIST, 2);
      w.writeListHeader(CT_I32, 2);
      w.writeListI32(ENC_PLAIN);
      w.writeListI32(ENC_RLE);
    }
    // path_in_schema: [name] for primitives; for a list nested `array_depth`
    // deep, [name, ("list","element") x array_depth] — e.g. depth 1
    // [name,"list","element"], depth 2 [name,"list","element","list","element"].
    w.writeFieldHeader(CT_LIST, 3);
    if (c.is_array) {
      w.writeListHeader(CT_BINARY, (uint32_t)(1 + 2 * c.array_depth));
      w.writeListString(c.name);
      for (int lvl = 0; lvl < c.array_depth; lvl++) {
        w.writeListString("list");
        w.writeListString("element");
      }
    } else {
      w.writeListHeader(CT_BINARY, 1);
      w.writeListString(c.name);
    }
    w.writeI32Field(4, codec);                     // codec
    w.writeI64Field(5, (int64_t)(c.is_array ? c.num_levels : num_rows)); // num_values
    w.writeI64Field(6, (int64_t)uncompressed_total); // total_uncompressed_size
    w.writeI64Field(7, (int64_t)compressed_total);   // total_compressed_size
    w.writeI64Field(9, data_page_offset);          // data_page_offset
    if (dict_page_offset >= 0)
      w.writeI64Field(11, dict_page_offset);       // dictionary_page_offset
    // statistics (field 12): null_count(3), distinct_count(4), max_value(5),
    // min_value(6), is_max_value_exact(7), is_min_value_exact(8). Ascending
    // ids. Omitted for LIST columns (nested null semantics — no leaf stats).
    if (!c.is_array) {
      w.writeFieldHeader(CT_STRUCT, 12);
      w.structBegin(); // Statistics
      w.writeI64Field(3, stats.null_count);
      if (stats.distinct_count >= 0)
        w.writeI64Field(4, stats.distinct_count); // hash-derived NDV
      if (stats.has_minmax) {
        w.writeBinaryField(5, stats.max_bytes.data(), stats.max_bytes.size());
        w.writeBinaryField(6, stats.min_bytes.data(), stats.min_bytes.size());
        w.writeBoolField(7, true); // exact: PLAIN values, not truncated
        w.writeBoolField(8, true);
      }
      w.structEnd();
    }
    // bloom_filter_offset(14) / bloom_filter_length(15) — ascending after 12.
    if (bloom_offset >= 0) {
      w.writeI64Field(14, bloom_offset);
      w.writeI32Field(15, bloom_length);
    }
    w.structEnd();
  }
  // PageIndex locations (ColumnChunk fields 4-7, ascending after meta_data).
  // Both pairs are written together or not at all — rugo's reader needs both to
  // prune, and a lone OffsetIndex would cost tail bytes nothing here consumes.
  if (oi_offset >= 0 && ci_offset >= 0) {
    w.writeI64Field(4, oi_offset); // offset_index_offset
    w.writeI32Field(5, oi_length); // offset_index_length
    w.writeI64Field(6, ci_offset); // column_index_offset
    w.writeI32Field(7, ci_length); // column_index_length
  }
  w.structEnd();
}

// ---- shared row-group / footer assembly ----
//
// Per-row-group metadata collected during the write loop and consumed by the
// footer. Namespace-scope so both the one-shot WriteParquet and the streaming
// writer below share it (no drift — CLAUDE.md §11).
struct RGMeta {
  std::vector<int64_t>     data_offsets;
  std::vector<int64_t>     dict_offsets;
  std::vector<size_t>      sizes;
  std::vector<size_t>      uncompressed;
  std::vector<ColumnStats> stats;
  std::vector<int64_t>     bloom_offset;
  std::vector<int32_t>     bloom_length;
  // Codec is PER COLUMN CHUNK, not per file: a chunk whose compressed form is
  // no smaller than its raw form is stored raw and records CODEC_UNCOMPRESSED
  // here. Parquet declares the codec in ColumnMetaData, so this is legal and
  // every conforming reader dispatches on it (rugo's own does).
  std::vector<int32_t>     codecs;
  // Per-column data-page metadata for the PageIndex. Empty (per column, or
  // entirely) when no page index was requested or the column carries none
  // (arrays). ci_*/oi_* are filled by write_page_index once the tail is laid
  // out; -1/0 means "this chunk has no index" and the footer omits the fields.
  std::vector<std::vector<PageMeta>> pages;
  std::vector<int64_t>     ci_offset;
  std::vector<int32_t>     ci_length;
  std::vector<int64_t>     oi_offset;
  std::vector<int32_t>     oi_length;
  size_t row_count      = 0;
  size_t total_byte_size = 0;

  // Size every per-column vector for a row group of `ncols` columns, with the
  // "nothing here" defaults the footer reads as absence. ONE place, so a new
  // per-column field cannot be initialised by write_row_group_chunks and missed
  // by the patch path (whose footer would then index past the end).
  void init_columns(size_t ncols, int codec) {
    data_offsets.assign(ncols, 0);
    dict_offsets.assign(ncols, -1);
    sizes.assign(ncols, 0);
    uncompressed.assign(ncols, 0);
    stats.assign(ncols, ColumnStats{});
    bloom_offset.assign(ncols, -1);
    bloom_length.assign(ncols, 0);
    codecs.assign(ncols, codec);
    pages.assign(ncols, std::vector<PageMeta>{});
    ci_offset.assign(ncols, -1);
    ci_length.assign(ncols, 0);
    oi_offset.assign(ncols, -1);
    oi_length.assign(ncols, 0);
  }
};

// ---- shared row-group encoding and placement ----
//
// Encoding and placement are two steps. encode_row_group serialises every
// column chunk of ONE row group into position-independent buffers; the
// place_* helpers append those buffers to the file and pin the absolute
// offsets the footer records. Splitting the two is what lets a file be laid
// out COLUMN-MAJOR IN BLOCKS (write_block) with every bloom filter in the tail
// (write_bloom_tail) — the grouped layout of
// docs/PARQUET_GROUPED_COLUMN_MAJOR_DESIGN.md — while the column patcher keeps
// its row-major, bloom-before-chunk arrangement through write_row_group_chunks.
// One encoder feeds both layouts, so the two cannot drift.
struct EncodedChunk {
  std::vector<uint8_t> bytes;    // [dictionary page][data pages...], as stored
  int64_t dict_len = -1;         // bytes of the dictionary page at the front; -1 = none
  std::vector<PageMeta> pages;   // data pages; file_offset RELATIVE to bytes[0]
};

struct EncodedRowGroup {
  size_t rows = 0;
  std::vector<EncodedChunk> chunks;            // per column, schema order
  std::vector<std::vector<uint8_t>> blooms;    // per column; empty = no filter
  RGMeta meta;                                 // offsets are filled by placement
};

// Encode every column chunk of ONE row group. `rg_cols` are the already-sliced
// per-row-group column views; `rg_rows` their row count. Nothing here knows
// where the bytes will land: data_offsets / dict_offsets / bloom_offset and
// the page offsets are assigned by place_chunk / place_bloom.
inline EncodedRowGroup encode_row_group(const std::vector<ColumnInput> &rg_cols,
                                        size_t rg_rows, int codec, int profile,
                                        size_t max_page_bytes, bool want_index) {
  const size_t ncols = rg_cols.size();
  EncodedRowGroup erg;
  erg.rows = rg_rows;
  erg.chunks.resize(ncols);
  erg.blooms.resize(ncols);
  RGMeta &meta = erg.meta;
  meta.row_count = rg_rows;
  meta.init_columns(ncols, codec);

  // Settle each page's stored-vs-plain size and its offset RELATIVE to the
  // chunk's first byte, now that the chunk's variant is decided. Data pages run
  // contiguously from `first` (0, or the dictionary page's length).
  auto relativise_pages = [&](size_t i, std::vector<PageMeta> pages, int64_t first) {
    if (pages.empty()) return;
    const bool use_plain = (codec == CODEC_ZSTD && meta.codecs[i] == CODEC_UNCOMPRESSED);
    int64_t at = first;
    for (PageMeta &pm : pages) {
      pm.size = (int64_t)(use_plain ? pm.plain_size : pm.stored_size);
      pm.file_offset = at;
      at += pm.size;
    }
    erg.chunks[i].pages = std::move(pages);
  };

  // Keep the compressed chunk only when it clears kKeepCompressedFloor. NOT
  // "whichever is smaller" — a chunk compressing to 96-100% of raw costs a full
  // decompression pass on every read to save a rounding error in bytes, and
  // decompression is 80.5% of parquet read CPU (make clickbench-profile,
  // 2026-08-11). The floor buys those reads back at a bounded byte cost.
  //
  // zstd emits a frame header even when it finds nothing to compress, so an
  // incompressible chunk comes back LARGER than the bytes that went in — 17 of
  // ClickBench hits' 105 columns land at 0.847x. The floor subsumes that case;
  // the wasted compress work is bounded because the chunks that fail it are
  // precisely the ones zstd bails out of at near-memcpy speed.
  auto keep_compressed = [&](PageBuild &pb, size_t i) {
    if (codec != CODEC_ZSTD) return;
    if (compressed_clears_floor(pb.bytes.size(), pb.plain_bytes.size())) return;
    pb.bytes = std::move(pb.plain_bytes);
    meta.codecs[i] = CODEC_UNCOMPRESSED;
  };

  for (size_t i = 0; i < ncols; i++) {
    const int level = zstd_level_for(rg_cols[i], profile);
    if (rg_cols[i].is_array) {
      PageBuild pb = build_array_data_pages(rg_cols[i], codec, level,
                                            rg_rows, max_page_bytes);
      keep_compressed(pb, i);
      meta.sizes[i]        = pb.bytes.size();
      meta.uncompressed[i] = pb.uncompressed_total;
      erg.chunks[i].bytes  = std::move(pb.bytes);
      continue;   // arrays carry no statistics, no bloom and no page index
    }

    meta.stats[i] = compute_stats(rg_cols[i], rg_rows);

    // ONE XXH64 pass per column, shared by the bloom filter and the
    // dictionary-encoding decision below — see hash_column_rows. Skipped
    // entirely when neither consumer wants it.
    const size_t present_rows = rg_rows - (size_t)meta.stats[i].null_count;
    const bool wants_dict_decision = rg_cols[i].codes == nullptr &&
                                     rg_cols[i].dict_enabled && present_rows > 0;
    std::vector<uint64_t> row_hashes;
    bool have_row_hashes = false;
    if (rg_cols[i].bloom || wants_dict_decision)
      have_row_hashes = hash_column_rows(rg_cols[i], rg_rows, row_hashes);

    // Set by the bloom block below, which sorts the hashes and therefore knows
    // the distinct count EXACTLY. The dictionary decision reuses it rather than
    // estimating a number it has already been handed.
    size_t exact_ndv = 0;
    bool have_exact_ndv = false;

    if (rg_cols[i].bloom && have_row_hashes) {
      std::vector<uint64_t> hashes =
          compact_present_hashes(row_hashes, rg_cols[i].validity, rg_rows);
      if (!hashes.empty()) {
        size_t ndv = bloom_ndv(hashes);
        meta.stats[i].distinct_count = (int64_t)ndv;
        exact_ndv = ndv;
        have_exact_ndv = true;
        BloomFilter bf = bloom_build(hashes, ndv, 0.01);
        std::vector<uint8_t> hdr = build_bloom_header((int32_t)bf.bitset.size());
        std::vector<uint8_t> &bloom = erg.blooms[i];
        bloom.reserve(hdr.size() + bf.bitset.size());
        bloom.insert(bloom.end(), hdr.begin(), hdr.end());
        bloom.insert(bloom.end(), bf.bitset.begin(), bf.bitset.end());
        meta.bloom_length[i] = (int32_t)bloom.size();
      }
    }

    bool use_dict = false;
    DictColumnBuild dcb;
    BuiltDict bd;
    if (rg_cols[i].codes != nullptr) {
      use_dict = true;
      dcb = build_dict_column(rg_cols[i], rg_rows, codec, level,
                              max_page_bytes, want_index);
    } else if (rg_cols[i].dict_enabled) {
      const size_t present = present_rows;
      ColumnInput dcol = rg_cols[i];
      bool built = false;
      if (have_row_hashes) {
        // Bracket the distinct count. With an EXACT one in hand (the bloom
        // filter already sorted these hashes) the bracket collapses to a point
        // and no margin is bought; otherwise the KMV sketch estimates it and
        // the bracket spans the estimator's error. dict_byte_budget uses the
        // two ends in the directions that keep it honest.
        size_t ndv_lo, ndv_hi, expected;
        if (have_exact_ndv) {
          ndv_lo = ndv_hi = expected = exact_ndv;
        } else {
          const double estimate =
              dict_estimated_ndv(row_hashes, rg_cols[i].validity, rg_rows);
          ndv_lo = (size_t)(estimate / DICT_ESTIMATE_SKIP_MARGIN);
          ndv_hi = (size_t)(estimate * DICT_ESTIMATE_SKIP_MARGIN) + 1u;
          expected = (size_t)estimate + 1u;
        }
        const DictBudget budget = dict_byte_budget(rg_cols[i], rg_rows, present,
                                                   ndv_lo, ndv_hi);
        const size_t byte_budget = budget.byte_budget;
        if (budget.worth_attempting) {
          switch (rg_cols[i].type) {
          case PT_INT32:
            built = build_numeric_dict<int32_t>(rg_cols[i].i32, rg_cols[i].validity,
                                                rg_rows, byte_budget, row_hashes, expected,
                                                bd.i32, bd.codes);
            if (built) { dcol.i32 = bd.i32.data(); dcol.dict_count = (uint32_t)bd.i32.size(); }
            break;
          case PT_INT64:
            built = build_numeric_dict<int64_t>(rg_cols[i].i64, rg_cols[i].validity,
                                                rg_rows, byte_budget, row_hashes, expected,
                                                bd.i64, bd.codes);
            if (built) { dcol.i64 = bd.i64.data(); dcol.dict_count = (uint32_t)bd.i64.size(); }
            break;
          case PT_FLOAT:
            built = build_numeric_dict<float>(rg_cols[i].f32, rg_cols[i].validity,
                                              rg_rows, byte_budget, row_hashes, expected,
                                              bd.f32, bd.codes);
            if (built) { dcol.f32 = bd.f32.data(); dcol.dict_count = (uint32_t)bd.f32.size(); }
            break;
          case PT_DOUBLE:
            built = build_numeric_dict<double>(rg_cols[i].f64, rg_cols[i].validity,
                                               rg_rows, byte_budget, row_hashes, expected,
                                               bd.f64, bd.codes);
            if (built) { dcol.f64 = bd.f64.data(); dcol.dict_count = (uint32_t)bd.f64.size(); }
            break;
          case PT_BYTE_ARRAY:
            built = build_string_dict(rg_cols[i].strs, rg_cols[i].validity,
                                      rg_rows, byte_budget, row_hashes, expected,
                                      bd.strs, bd.codes);
            if (built) { dcol.strs = bd.strs.data(); dcol.dict_count = (uint32_t)bd.strs.size(); }
            break;
          default:
            break;
          }
        }
      }
      if (built) {
        dcol.codes = bd.codes.data();
        use_dict = true;
        dcb = build_dict_column(dcol, rg_rows, codec, level,
                                max_page_bytes, want_index);
      }
    }

    if (use_dict) {
      // Both the bytes and the dictionary-page length move together — the
      // plain dictionary page is a different size, and data_page_offset is
      // derived from it.
      if (codec == CODEC_ZSTD &&
          !compressed_clears_floor(dcb.bytes.size(), dcb.plain_bytes.size())) {
        dcb.bytes = std::move(dcb.plain_bytes);
        dcb.dict_page_len = dcb.plain_dict_page_len;
        meta.codecs[i] = CODEC_UNCOMPRESSED;
      }
      erg.chunks[i].dict_len = (int64_t)dcb.dict_page_len;
      meta.sizes[i]        = dcb.bytes.size();
      meta.uncompressed[i] = dcb.uncompressed_total;
      relativise_pages(i, std::move(dcb.pages), (int64_t)dcb.dict_page_len);
      erg.chunks[i].bytes = std::move(dcb.bytes);
    } else {
      PageBuild pb = build_data_pages(rg_cols[i], rg_rows, codec, level,
                                      max_page_bytes, want_index);
      keep_compressed(pb, i);
      meta.sizes[i]        = pb.bytes.size();
      meta.uncompressed[i] = pb.uncompressed_total;
      relativise_pages(i, std::move(pb.pages), 0);
      erg.chunks[i].bytes = std::move(pb.bytes);
    }
  }

  meta.total_byte_size = 0;
  for (size_t s : meta.uncompressed) meta.total_byte_size += s;
  return erg;
}

// Append column `i`'s chunk to `out` and pin its absolute offsets (dictionary
// page, first data page, every indexed page) into erg.meta. `base_offset` is
// the absolute file position of out[0]. The chunk's bytes are released once
// copied: a placed chunk is dead weight, and a block writer that kept them
// would hold two copies of every block.
inline void place_chunk(std::vector<uint8_t> &out, int64_t base_offset,
                        EncodedRowGroup &erg, size_t i) {
  EncodedChunk &ch = erg.chunks[i];
  const int64_t at = base_offset + (int64_t)out.size();
  if (ch.dict_len >= 0) {
    erg.meta.dict_offsets[i] = at;
    erg.meta.data_offsets[i] = at + ch.dict_len;
  } else {
    erg.meta.dict_offsets[i] = -1;
    erg.meta.data_offsets[i] = at;
  }
  for (PageMeta &pm : ch.pages) pm.file_offset += at;
  erg.meta.pages[i] = std::move(ch.pages);
  out.insert(out.end(), ch.bytes.begin(), ch.bytes.end());
  std::vector<uint8_t>().swap(ch.bytes);
}

// Append one column's bloom filter (if it has one) and pin bloom_offset.
inline void place_bloom(std::vector<uint8_t> &out, int64_t base_offset,
                        RGMeta &meta, std::vector<uint8_t> &bloom, size_t i) {
  if (bloom.empty()) return;
  meta.bloom_offset[i] = base_offset + (int64_t)out.size();
  out.insert(out.end(), bloom.begin(), bloom.end());
  std::vector<uint8_t>().swap(bloom);
}

// ROW-MAJOR placement of one row group: for each column, its bloom filter then
// its chunk, in schema order — the arrangement the column patcher appends its
// synthesised chunks in (the bloom rides in the same range as its chunk). This
// is NOT the layout WriteParquet / StreamingParquetWriter produce; they place
// blocks column-major and put every bloom in the file tail (see write_block).
inline void write_row_group_chunks(std::vector<uint8_t> &out, int64_t base_offset,
                                   const std::vector<ColumnInput> &rg_cols,
                                   size_t rg_rows, int codec, int profile,
                                   size_t max_page_bytes, bool want_index,
                                   RGMeta &meta) {
  EncodedRowGroup erg = encode_row_group(rg_cols, rg_rows, codec, profile,
                                         max_page_bytes, want_index);
  for (size_t i = 0; i < rg_cols.size(); i++) {
    place_bloom(out, base_offset, erg.meta, erg.blooms[i], i);
    place_chunk(out, base_offset, erg, i);
  }
  meta = std::move(erg.meta);
}

// COLUMN-MAJOR placement of one block of row groups: every column's chunks for
// the block's row groups byte-adjacent, columns in schema order —
//
//     [rg1.c1 rg2.c1 .. rgG.c1][rg1.c2 rg2.c2 .. rgG.c2] ...
//
// so a reader projecting c1 over the block fetches ONE range instead of G. No
// bloom filter is placed here (they go to the tail, write_bloom_tail): a filter
// between two chunks of the same column would split that range. The row group
// stays the unit of decode, statistics and pruning; only the byte order in the
// file changes, and every chunk is still located by its own footer offsets.
inline void write_block(std::vector<uint8_t> &out, int64_t base_offset,
                        std::vector<EncodedRowGroup> &block) {
  if (block.empty()) return;
  const size_t ncols = block[0].chunks.size();
  for (size_t i = 0; i < ncols; i++)
    for (EncodedRowGroup &erg : block)
      place_chunk(out, base_offset, erg, i);
}

// Every bloom filter of the file, after the last block and before the page
// index. Column-major over the WHOLE file — column c's filter for row group k
// is immediately followed by its filter for row group k+1 — so one column's
// filters for any run of row groups (a block, or the entire file) are one
// contiguous range. `blooms[rg][i]` is consumed (released) as it is written.
inline void write_bloom_tail(std::vector<uint8_t> &out, int64_t base_offset,
                             std::vector<RGMeta> &rg_meta,
                             std::vector<std::vector<std::vector<uint8_t>>> &blooms) {
  if (rg_meta.empty()) return;
  const size_t ncols = rg_meta[0].bloom_offset.size();
  for (size_t i = 0; i < ncols; i++)
    for (size_t rg = 0; rg < rg_meta.size(); rg++)
      place_bloom(out, base_offset, rg_meta[rg], blooms[rg][i], i);
}

// Per-row-group column views of `cols` for rows [rg_start, rg_start + rg_rows):
// pointers offset, nothing copied. Validity is bit-packed, so rg_start MUST be a
// multiple of 8 (WriteParquet and the streaming writer round the row-group size
// up to one) for the byte offset rg_start>>3 to be exact.
// PRESERVE-dict columns (codes != nullptr): the typed buffers hold dictionary
// VALUES, not per-row data, so only the codes are offset.
inline std::vector<ColumnInput> slice_row_group_cols(const std::vector<ColumnInput> &cols,
                                                     size_t rg_start, size_t rg_rows) {
  std::vector<ColumnInput> rg_cols(cols.size());
  for (size_t i = 0; i < cols.size(); i++) {
    rg_cols[i] = cols[i];
    if (cols[i].is_array) {
      // Arrays aren't one-per-row in rep_levels/def_levels/elem_* — slice
      // via the row->level and row->element offset indexes instead of a
      // flat rg_start pointer add.
      const uint32_t lvl_start = cols[i].row_level_offsets[rg_start];
      const uint32_t lvl_end   = cols[i].row_level_offsets[rg_start + rg_rows];
      const uint32_t el_start  = cols[i].row_element_offsets[rg_start];
      const uint32_t el_end    = cols[i].row_element_offsets[rg_start + rg_rows];
      rg_cols[i].rep_levels  = cols[i].rep_levels + lvl_start;
      rg_cols[i].def_levels  = cols[i].def_levels + lvl_start;
      rg_cols[i].num_levels  = lvl_end - lvl_start;
      rg_cols[i].num_elements = el_end - el_start;
      if (cols[i].i32)     rg_cols[i].i32     = cols[i].i32     + el_start;
      if (cols[i].i64)     rg_cols[i].i64     = cols[i].i64     + el_start;
      if (cols[i].f32)     rg_cols[i].f32     = cols[i].f32     + el_start;
      if (cols[i].f64)     rg_cols[i].f64     = cols[i].f64     + el_start;
      if (cols[i].boolean) rg_cols[i].boolean = cols[i].boolean + el_start;
      if (cols[i].strs)    rg_cols[i].strs    = cols[i].strs    + el_start;
      // Row-level validity bitmap (outer-list null/not-null) is still one
      // bit per ROW like any scalar column — same rg_start>>3 slice below.
    } else if (!cols[i].codes) {
      if (cols[i].i32)     rg_cols[i].i32     = cols[i].i32     + rg_start;
      if (cols[i].i64)     rg_cols[i].i64     = cols[i].i64     + rg_start;
      if (cols[i].f32)     rg_cols[i].f32     = cols[i].f32     + rg_start;
      if (cols[i].f64)     rg_cols[i].f64     = cols[i].f64     + rg_start;
      if (cols[i].boolean) rg_cols[i].boolean = cols[i].boolean + rg_start;
      if (cols[i].strs)    rg_cols[i].strs    = cols[i].strs    + rg_start;
      if (cols[i].dec_raw) rg_cols[i].dec_raw = cols[i].dec_raw
                                                 + rg_start * (size_t)cols[i].dec_width;
    }
    if (cols[i].validity)
      rg_cols[i].validity = cols[i].validity + (rg_start >> 3);
    if (cols[i].codes)
      rg_cols[i].codes = cols[i].codes + rg_start;
  }
  return rg_cols;
}

// Row-group splitting needs every ARRAY column to be sliceable per row group
// (see ColumnInput's row_level_offsets / row_element_offsets). Fail loud rather
// than silently degrading to a single row group: a caller that asked for N-row
// row groups and got one giant row group with no error is exactly the "hidden
// behaviour" this project forbids.
inline void require_array_row_offsets(const std::vector<ColumnInput> &cols,
                                      const char *who) {
  for (const auto &c : cols) {
    if (c.is_array && (!c.row_level_offsets || !c.row_element_offsets)) {
      throw std::invalid_argument(
          std::string(who) + ": row-group splitting requires row_level_offsets/"
          "row_element_offsets on every ARRAY column (needed to slice "
          "rep/def levels and element values per row group)");
    }
  }
}

// Row-group size as written: rounded UP to a multiple of 8 so validity
// bit-offsets stay byte-aligned (slice_row_group_cols). 0 = one row group.
inline size_t aligned_rows_per_row_group(size_t max_rows_per_rg) {
  return max_rows_per_rg > 0 ? ((max_rows_per_rg + 7) & ~(size_t)7) : 0;
}

// Emit RowGroup.sorting_columns (field 4): one SortingColumn per schema
// column whose ColumnInput carries sorted_hint, in schema order. Omits the
// field entirely when no column in this row group is hinted (matches the
// optional-field-by-omission pattern used elsewhere in this writer, e.g.
// bloom_filter_offset/length). The writer does not verify the hint — see
// ColumnInput::sorted_hint.
inline void write_sorting_columns(TCompactWriter &fm,
                                  const std::vector<ColumnInput> &rg_cols) {
  std::vector<size_t> idxs;
  for (size_t i = 0; i < rg_cols.size(); i++)
    if (rg_cols[i].sorted_hint) idxs.push_back(i);
  if (idxs.empty())
    return;

  fm.writeFieldHeader(CT_LIST, 4);
  fm.writeListHeader(CT_STRUCT, (uint32_t)idxs.size());
  for (size_t i : idxs) {
    fm.structBegin(); // SortingColumn
    fm.writeI32Field(1, (int32_t)i);                     // column_idx
    fm.writeBoolField(2, rg_cols[i].sorted_descending);   // descending
    fm.writeBoolField(3, rg_cols[i].sorted_nulls_first);  // nulls_first
    fm.structEnd();
  }
}

// Draken logical-descriptor side channel — FileMetaData.key_value_metadata.
//
// KEY:   "draken.logical." + the column's TOP-LEVEL name (the same name
//        FileStats::schema_columns reports, so a reader matches without
//        re-deriving a path). The prefix is stripped whole, so a name that
//        itself contains dots parses back unambiguously.
// VALUE: comma-separated `name=value` pairs drawn from the draken descriptor —
//        kind, unit, offset, precision, scale, dimension. Only non-default
//        fields are emitted; an absent field means 0, and a reader IGNORES a
//        name it does not know. That is what lets the remaining LogicalKind
//        ordinals be added later without a second format change. Today only
//        `kind` is ever written (IPV4 is the only kind wired — see
//        ColumnInput::draken_logical_kind), so the value is "kind=5".
//
// FILE-LEVEL, not ColumnChunk-level (parquet.thrift also offers field 8 on
// ColumnMetaData), ratified 2026-08-19. The descriptor is a property of the
// SCHEMA, not of a chunk: a per-row-group copy is written N times and can
// disagree with itself. Decisively, the reader skips row groups entirely on a
// `schema_only` parse (ReadParquetMetadata's read_metadata path) and skips
// ColumnMetaData key_value_metadata again when `include_statistics` is off — so
// a chunk-level annotation would be invisible to exactly the type-discovery
// read that needs it.
//
// Emitted only when at least one column carries a descriptor, matching the
// optional-field-by-omission pattern used for sorting_columns and the bloom
// filter offsets: an unannotated file is byte-identical to one written before
// this existed.
inline void write_draken_logical_kv(TCompactWriter &fm,
                                    const std::vector<ColumnInput> &schema_cols) {
  std::vector<size_t> idxs;
  for (size_t i = 0; i < schema_cols.size(); i++)
    if (schema_cols[i].draken_logical_kind != 0) idxs.push_back(i);
  if (idxs.empty())
    return;

  fm.writeFieldHeader(CT_LIST, 5);
  fm.writeListHeader(CT_STRUCT, (uint32_t)idxs.size());
  for (size_t i : idxs) {
    fm.structBegin(); // KeyValue
    fm.writeStringField(1, "draken.logical." + schema_cols[i].name); // key
    fm.writeStringField(
        2, "kind=" + std::to_string(schema_cols[i].draken_logical_kind)); // value
    fm.structEnd();
  }
}

// Append the file's PageIndex tail to `out` and record each chunk's index
// locations in `rg_meta`. MUST be called after every row group's bytes and
// before write_parquet_footer.
//
// Layout: ALL ColumnIndex structs first, then ALL OffsetIndex structs — the
// spec's recommended order, and the one rugo's reader relies on when it fetches
// the whole index region of a file as a single range.
//
// A chunk gets both structs or neither: page_index_bound_kind declines a column
// whose bounds cannot be ordered soundly (arrays, FLBA decimals) and
// serialize_column_index declines a page that is neither all-NULL nor bounded,
// and an OffsetIndex alone would be tail bytes no reader here consumes.
//
// ⛔ A SINGLE-PAGE CHUNK GETS NO INDEX, whatever max_page_bytes says. The knob
// is a byte budget; whether a chunk actually splits depends on its width and the
// row group's row count, so `max_page_bytes > 0` does NOT imply more than one
// page — a 1 MiB budget over a 262 144-row row group leaves every 4-byte column
// in one page. A one-entry index restates the footer's own Statistics, and it is
// not merely wasted: the reader fetches the whole index region as ONE range and
// gates on its size against the bytes it could save, so degenerate entries push
// that gate toward declining and penalise the columns that DID split.
inline void write_page_index(std::vector<uint8_t> &out, int64_t base_offset,
                             std::vector<RGMeta> &rg_meta,
                             const std::vector<std::vector<ColumnInput>> &all_rg_cols) {
  std::vector<std::vector<std::vector<uint8_t>>> ci(rg_meta.size());
  for (size_t rg = 0; rg < rg_meta.size(); rg++) {
    RGMeta &meta = rg_meta[rg];
    ci[rg].resize(meta.pages.size());
    if (meta.row_count == 0) continue;
    for (size_t i = 0; i < meta.pages.size(); i++) {
      if (meta.pages[i].size() < 2) continue; // see the single-page rule above
      PageBoundKind kind;
      if (!page_index_bound_kind(all_rg_cols[rg][i], kind)) continue;
      ci[rg][i] = serialize_column_index(meta.pages[i], kind,
                                         all_rg_cols[rg][i].is_unsigned);
    }
  }

  for (size_t rg = 0; rg < rg_meta.size(); rg++) {
    RGMeta &meta = rg_meta[rg];
    for (size_t i = 0; i < ci[rg].size(); i++) {
      if (ci[rg][i].empty()) continue;
      meta.ci_offset[i] = base_offset + (int64_t)out.size();
      meta.ci_length[i] = (int32_t)ci[rg][i].size();
      out.insert(out.end(), ci[rg][i].begin(), ci[rg][i].end());
    }
  }
  for (size_t rg = 0; rg < rg_meta.size(); rg++) {
    RGMeta &meta = rg_meta[rg];
    for (size_t i = 0; i < ci[rg].size(); i++) {
      if (ci[rg][i].empty()) continue;
      std::vector<uint8_t> oi = serialize_offset_index(meta.pages[i]);
      meta.oi_offset[i] = base_offset + (int64_t)out.size();
      meta.oi_length[i] = (int32_t)oi.size();
      out.insert(out.end(), oi.begin(), oi.end());
    }
  }
}

// Append the FileMetaData footer + footer length + trailing PAR1 to `out`.
// `schema_cols` supplies the schema/column shape (types, names, array depth);
// `all_rg_cols[rg][i]` supplies each chunk's per-row-group shape (num_levels for
// arrays). Data pointers in these are never read here (see write_column_chunk),
// so a stripped copy with dangling/nulled data buffers is fine.
inline void write_parquet_footer(std::vector<uint8_t> &out,
                                 const std::vector<ColumnInput> &schema_cols,
                                 size_t total_rows,
                                 const std::vector<RGMeta> &rg_meta,
                                 const std::vector<std::vector<ColumnInput>> &all_rg_cols) {
  const char *MAGIC = "PAR1";
  TCompactWriter fm;
  fm.structBegin();
  fm.writeI32Field(1, 1); // version
  write_schema(fm, schema_cols); // field 2
  fm.writeI64Field(3, (int64_t)total_rows);

  fm.writeFieldHeader(CT_LIST, 4);
  fm.writeListHeader(CT_STRUCT, (uint32_t)rg_meta.size());
  for (size_t rg = 0; rg < rg_meta.size(); rg++) {
    const RGMeta &meta = rg_meta[rg];
    fm.structBegin(); // RowGroup
    fm.writeFieldHeader(CT_LIST, 1);
    fm.writeListHeader(CT_STRUCT, (uint32_t)schema_cols.size());
    for (size_t i = 0; i < schema_cols.size(); i++)
      write_column_chunk(fm, all_rg_cols[rg][i], meta.row_count, meta.data_offsets[i],
                         meta.dict_offsets[i], meta.sizes[i], meta.uncompressed[i],
                         meta.codecs[i], meta.stats[i], meta.bloom_offset[i],
                         meta.bloom_length[i], meta.oi_offset[i], meta.oi_length[i],
                         meta.ci_offset[i], meta.ci_length[i]);
    fm.writeI64Field(2, (int64_t)meta.total_byte_size); // total_byte_size
    fm.writeI64Field(3, (int64_t)meta.row_count);       // num_rows
    write_sorting_columns(fm, all_rg_cols[rg]);         // sorting_columns (field 4, optional)
    // file_offset (5) / total_compressed_size (6): the row group's FIRST byte
    // and the SUM of its chunks' bytes. Honest under the grouped layout too,
    // where a row group's chunks are not contiguous (write_block interleaves
    // them with the block's other row groups): the pair then brackets more
    // than the row group's own bytes, and a reader must locate chunks by their
    // own ColumnChunk offsets — which every reader tested does (design doc §2).
    int64_t rg_first = -1, rg_compressed = 0;
    for (size_t i = 0; i < schema_cols.size(); i++) {
      const int64_t start = meta.dict_offsets[i] >= 0 ? meta.dict_offsets[i]
                                                      : meta.data_offsets[i];
      if (rg_first < 0 || start < rg_first) rg_first = start;
      rg_compressed += (int64_t)meta.sizes[i];
    }
    fm.writeI64Field(5, rg_first);                      // file_offset
    fm.writeI64Field(6, rg_compressed);                 // total_compressed_size
    fm.structEnd();
  }
  write_draken_logical_kv(fm, schema_cols);        // key_value_metadata (field 5, optional)
  fm.writeStringField(6, RUGO_PARQUET_CREATED_BY); // created_by
  // column_orders (field 7): one TypeDefinedOrder per leaf column.
  fm.writeFieldHeader(CT_LIST, 7);
  fm.writeListHeader(CT_STRUCT, (uint32_t)schema_cols.size());
  for (size_t i = 0; i < schema_cols.size(); i++) {
    fm.structBegin();                  // ColumnOrder (union)
    fm.writeFieldHeader(CT_STRUCT, 1); // TYPE_ORDER
    fm.structBegin();                  // TypeDefinedOrder {}
    fm.structEnd();
    fm.structEnd();
  }
  fm.structEnd();

  out.insert(out.end(), fm.buf.begin(), fm.buf.end());
  put_u32_le(out, (uint32_t)fm.buf.size()); // footer length
  out.insert(out.end(), MAGIC, MAGIC + 4);
}

// ---- top-level file assembly ----
//
// Returns the complete parquet file as bytes. All columns must have the same
// row count (`num_rows`).
//
// Layout (docs/PARQUET_GROUPED_COLUMN_MAJOR_DESIGN.md): row groups of at most
// `max_rows_per_rg` rows, written in BLOCKS of `row_groups_per_block` row
// groups. Within a block every column's chunks are byte-adjacent, columns in
// schema order (write_block); every bloom filter goes in the tail after the
// last block (write_bloom_tail), then the page index, then the footer. The
// last block of a file may be partial. The row group stays the unit of
// decode, statistics and pruning; a reader infers the blocks from the chunk
// offsets, nothing in the footer names them.
//
//   max_rows_per_rg      0 = a single row group. N > 0 is rounded up to a
//                        multiple of 8 so validity bit-offsets stay byte-aligned.
//   row_groups_per_block G >= 1. 1 = row-major (each row group's chunks
//                        contiguous, as parquet is conventionally written) —
//                        still with tail blooms. The defaults (64k rows, 4 per
//                        block) are the measured values in the design doc §3.
//
// out_stats, when given, receives whole-file per-column statistics (min/max/
// null_count over EVERY row group, computed over the full columns by the same
// compute_stats the chunks use) — so a bounds caller is not limited to
// single-row-group files.
inline std::vector<uint8_t> WriteParquet(const std::vector<ColumnInput> &cols,
                                         size_t num_rows,
                                         int codec = CODEC_UNCOMPRESSED,
                                         int profile = PROFILE_FAST,
                                         std::vector<ColumnStats> *out_stats =
                                             nullptr,
                                         size_t max_rows_per_rg = 65536,
                                         size_t max_page_bytes = 0,
                                         bool page_index = true,
                                         size_t row_groups_per_block = 4) {
  if (row_groups_per_block == 0)
    throw std::invalid_argument("WriteParquet: row_groups_per_block must be >= 1");
  // A PageIndex over one page per chunk would describe the granularity the
  // footer's own Statistics already carry, so it rides on page splitting being
  // on — see write_page_index.
  const bool want_index = page_index && max_page_bytes > 0;
  if (max_rows_per_rg > 0)
    require_array_row_offsets(cols, "WriteParquet");
  max_rows_per_rg = aligned_rows_per_row_group(max_rows_per_rg);

  size_t rg_size = (max_rows_per_rg > 0 && max_rows_per_rg < num_rows)
                       ? max_rows_per_rg
                       : num_rows;
  size_t n_rg = (num_rows == 0) ? 1 : (num_rows + rg_size - 1) / rg_size;

  // Per-row-group metadata and bloom filters, consumed by the tail and footer.
  std::vector<RGMeta> rg_meta(n_rg);
  std::vector<std::vector<std::vector<uint8_t>>> blooms(n_rg);
  // Per-row-group sliced ColumnInputs must stay alive until the footer is
  // written: for an array column, num_levels/num_elements/rep_levels/
  // def_levels are only correct for THIS row group's slice, not the global
  // `cols[i]` — the footer loop below must read from here, never from `cols`.
  std::vector<std::vector<ColumnInput>> all_rg_cols(n_rg);

  std::vector<uint8_t> file;
  file.reserve(1024);
  const char *MAGIC = "PAR1";
  file.insert(file.end(), MAGIC, MAGIC + 4);

  // One block of encoded row groups is held at a time; its chunk bytes are
  // released as the block is placed, so peak memory is the file so far plus
  // one block plus the file's bloom filters.
  std::vector<EncodedRowGroup> block;
  block.reserve(row_groups_per_block);
  size_t block_first = 0;
  auto flush_block = [&]() {
    // base_offset == 0: `file` already starts at absolute 0 and includes the
    // leading PAR1, so file.size() is the absolute position.
    write_block(file, /*base_offset=*/0, block);
    for (size_t k = 0; k < block.size(); k++) {
      rg_meta[block_first + k] = std::move(block[k].meta);
      blooms[block_first + k]  = std::move(block[k].blooms);
    }
    block_first += block.size();
    block.clear();
  };

  for (size_t rg = 0; rg < n_rg; rg++) {
    size_t rg_start = rg * rg_size;
    size_t rg_rows  = std::min(rg_size, num_rows - rg_start);
    all_rg_cols[rg] = slice_row_group_cols(cols, rg_start, rg_rows);
    block.push_back(encode_row_group(all_rg_cols[rg], rg_rows, codec, profile,
                                     max_page_bytes, want_index));
    if (block.size() == row_groups_per_block)
      flush_block();
  }
  if (!block.empty())
    flush_block();   // the file's last, possibly partial, block

  write_bloom_tail(file, /*base_offset=*/0, rg_meta, blooms);
  if (want_index)
    write_page_index(file, /*base_offset=*/0, rg_meta, all_rg_cols);
  write_parquet_footer(file, cols, num_rows, rg_meta, all_rg_cols);

  if (out_stats) {
    out_stats->assign(cols.size(), ColumnStats{});
    for (size_t i = 0; i < cols.size(); i++)
      if (!cols[i].is_array)
        (*out_stats)[i] = compute_stats(cols[i], num_rows);
  }
  return file;
}

// ---- streaming file assembly ----
//
// StreamingParquetWriter writes a parquet file incrementally, one row group per
// add_row_group() call, keeping only the current BLOCK's encoded bytes, the
// file's bloom filters and the (small, bounded) footer metadata in memory. Each
// call encodes its batch as ONE row group at once; a block is written to the
// pending buffer as soon as `row_groups_per_block` row groups are held. The
// caller drains the buffer with take_pending() after each call (and once more
// after finish()) and forwards the bytes to its sink. Absolute offsets survive
// draining because `abs_offset_` tracks how many bytes have already been
// handed out.
//
// One add_row_group == one parquet row group: the caller controls row-group
// sizing by how much it passes (the sinks batch to the 64k-row default, see
// DataFileStream); the writer owns only the grouping of those row groups into
// column-major blocks.
//
// Memory: one block of encoded chunks, plus EVERY bloom filter of the file
// until finish() (they are written in the tail, after the last block — the
// ruled placement, see write_bloom_tail). Peak = one block + the file's blooms.
//
// Every batch must share the same column schema (names/types); the schema is
// captured from the first batch.
class StreamingParquetWriter {
 public:
  StreamingParquetWriter(int codec, int profile, size_t max_page_bytes,
                         bool page_index, size_t row_groups_per_block = 4)
      : codec_(codec), profile_(profile), max_page_bytes_(max_page_bytes),
        want_index_(page_index && max_page_bytes > 0),
        row_groups_per_block_(row_groups_per_block) {
    if (row_groups_per_block == 0)
      throw std::invalid_argument(
          "StreamingParquetWriter: row_groups_per_block must be >= 1");
    const char *MAGIC = "PAR1";
    buf_.insert(buf_.end(), MAGIC, MAGIC + 4); // header (drained with block 1)
    block_.reserve(row_groups_per_block_);
  }

  // Append one row group built from `rg_cols` (the whole batch is one row
  // group). Data pointers in `rg_cols` need only stay valid for this call: the
  // row group is fully encoded before it returns.
  void add_row_group(const std::vector<ColumnInput> &rg_cols, size_t rg_rows) {
    if (!have_schema_) {
      schema_cols_ = strip_data(rg_cols);
      have_schema_ = true;
    }
    block_.push_back(encode_row_group(rg_cols, rg_rows, codec_, profile_,
                                      max_page_bytes_, want_index_));
    // Footer reads only shape fields (never data pointers) from these — store a
    // stripped copy so no per-batch data buffer is retained across row groups.
    all_rg_cols_.push_back(strip_data(rg_cols));
    total_rows_ += rg_rows;
    if (block_.size() == row_groups_per_block_)
      flush_block();
  }

  // Move out the bytes serialised so far; advance the absolute offset. The
  // caller forwards the returned bytes to its sink. Safe to call after every
  // add_row_group (constant memory) or just once before finish().
  std::vector<uint8_t> take_pending() {
    std::vector<uint8_t> out = std::move(buf_);
    buf_.clear();
    abs_offset_ += (int64_t)out.size();
    return out;
  }

  // Emit the last (possibly partial) block, the bloom tail, the page index and
  // the footer, then return all remaining pending bytes. After this the writer
  // is complete.
  std::vector<uint8_t> finish() {
    if (!block_.empty())
      flush_block();
    // abs_offset_ + buf_.size() is the absolute position whether or not the
    // caller has been draining as it goes.
    write_bloom_tail(buf_, abs_offset_, rg_meta_, blooms_);
    if (want_index_)
      write_page_index(buf_, abs_offset_, rg_meta_, all_rg_cols_);
    write_parquet_footer(buf_, schema_cols_, (size_t)total_rows_, rg_meta_,
                         all_rg_cols_);
    return take_pending();
  }

 private:
  void flush_block() {
    write_block(buf_, abs_offset_, block_);
    for (EncodedRowGroup &erg : block_) {
      rg_meta_.push_back(std::move(erg.meta));
      blooms_.push_back(std::move(erg.blooms));
    }
    block_.clear();
  }

  // Copy ColumnInput vector with all data/level/offset pointers nulled — keeps
  // only the shape/schema (name is a self-owning std::string). Used for the
  // footer-side copies so nothing dangles into freed per-batch buffers.
  static std::vector<ColumnInput> strip_data(const std::vector<ColumnInput> &in) {
    std::vector<ColumnInput> out = in;
    for (ColumnInput &c : out) {
      c.validity = nullptr;
      c.i32 = nullptr; c.i64 = nullptr; c.f32 = nullptr; c.f64 = nullptr;
      c.boolean = nullptr; c.strs = nullptr; c.dec_raw = nullptr;
      c.codes = nullptr;
      c.rep_levels = nullptr; c.def_levels = nullptr;
      c.row_level_offsets = nullptr; c.row_element_offsets = nullptr;
    }
    return out;
  }

  int codec_;
  int profile_;
  size_t max_page_bytes_;
  bool want_index_;
  size_t row_groups_per_block_;
  bool have_schema_ = false;
  int64_t abs_offset_ = 0;             // bytes already drained via take_pending
  int64_t total_rows_ = 0;
  std::vector<uint8_t> buf_;           // pending (undrained) bytes
  std::vector<ColumnInput> schema_cols_;
  std::vector<EncodedRowGroup> block_; // encoded, not yet placed
  std::vector<RGMeta> rg_meta_;
  std::vector<std::vector<std::vector<uint8_t>>> blooms_;   // [rg][col], until finish()
  std::vector<std::vector<ColumnInput>> all_rg_cols_;
};

} // namespace rugo_pq_write
