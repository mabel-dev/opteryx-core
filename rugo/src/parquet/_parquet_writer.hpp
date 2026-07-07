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
#include "_bloom_writer.hpp"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#ifdef HAVE_ZSTD
#include "vendor/zstd/zstd.h"
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

  bool bloom = false;       // emit a split-block bloom filter for this column

  // ---- dictionary encoding ----
  //
  // Two ways a column becomes dictionary-encoded (RLE_DICTIONARY):
  //
  //   1. PRESERVE — the edge already holds a dictionary (the incoming
  //      DrakenVector was dict/constant-shaped). It sets `codes` (one dict
  //      code per logical row; null rows carry an arbitrary in-range code),
  //      `dict_count`, and points the typed buffers (i32/i64/f64/strs) at the
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

  // ---- ARRAY (LIST) columns ----
  // When `is_array`, this column is a list. The element values live in the
  // typed buffers above (i32/i64/f64/boolean/strs), holding only the
  // num_elements PRESENT (def==3) elements in order; `elem_type`/`elem_is_utf8`
  // describe them. `rep_levels`/`def_levels` hold `num_levels` entries.
  bool is_array = false;
  PType elem_type = PT_INT64;
  bool elem_is_utf8 = false;
  const uint8_t *rep_levels = nullptr;
  const uint8_t *def_levels = nullptr;
  size_t num_levels = 0;
  size_t num_elements = 0;
};

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
  case PT_INT32: {
    bool any = false;
    int32_t lo = 0, hi = 0;
    for (size_t i = 0; i < num_rows; i++) {
      if (!is_valid(col.validity, i))
        continue;
      int32_t v = col.i32[codes ? codes[i] : i];
      if (!any) { lo = hi = v; any = true; }
      else { if (v < lo) lo = v; if (v > hi) hi = v; }
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

// XXH64 hash of each non-null value's PLAIN-encoded bytes — the exact bytes a
// reader hashes when probing. Matches encode_values: INT64/TIMESTAMP = 8 LE,
// INT32/DATE = 4 LE, DOUBLE = 8 LE IEEE, BYTE_ARRAY = raw value bytes (no
// length prefix), FLBA/DECIMAL = the big-endian fixed-width bytes.
inline std::vector<uint64_t> bloom_hashes(const ColumnInput &col, size_t num_rows) {
  std::vector<uint64_t> hashes;
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
      hashes.push_back(bloom_hash(buf, 4));
      break;
    }
    case PT_INT64: {
      int64_t v = col.i64[vi];
      std::memcpy(buf, &v, 8);
      hashes.push_back(bloom_hash(buf, 8));
      break;
    }
    case PT_DOUBLE: {
      std::memcpy(buf, &col.f64[vi], 8);
      hashes.push_back(bloom_hash(buf, 8));
      break;
    }
    case PT_BYTE_ARRAY: {
      const StrSlice &s = col.strs[vi];
      hashes.push_back(bloom_hash(s.ptr, s.len));
      break;
    }
    case PT_FLBA: {
      // Hash the big-endian fixed-width bytes (as written to the page).
      const uint8_t *le = col.dec_raw + (size_t)i * col.dec_width;
      for (int k = 0; k < col.dec_width; k++)
        buf[k] = le[col.dec_width - 1 - k];
      hashes.push_back(bloom_hash(buf, col.dec_width));
      break;
    }
    default:
      break; // bool / unsupported: no bloom
    }
  }
  return hashes;
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
struct PageBuild {
  std::vector<uint8_t> bytes;
  size_t uncompressed_total;
};

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
  std::vector<uint8_t> stored =
      (codec == CODEC_ZSTD) ? zstd_compress_block(body, zstd_level) : body;

  // PageHeader (Compact Protocol).
  TCompactWriter h;
  h.structBegin();
  h.writeI32Field(1, PAGE_DATA);                    // type
  h.writeI32Field(2, (int32_t)uncompressed_body);   // uncompressed_page_size
  h.writeI32Field(3, (int32_t)stored.size());       // compressed_page_size
  h.writeFieldHeader(CT_STRUCT, 5);                 // data_page_header
  h.structBegin();
  h.writeI32Field(1, (int32_t)num_rows);            // num_values (incl. nulls)
  h.writeI32Field(2, ENC_PLAIN);                    // encoding
  h.writeI32Field(3, ENC_RLE);                      // definition_level_encoding
  h.writeI32Field(4, ENC_RLE);                      // repetition_level_encoding (required)
  h.structEnd();
  h.structEnd();

  PageBuild pb;
  pb.bytes.reserve(h.buf.size() + stored.size());
  pb.bytes.insert(pb.bytes.end(), h.buf.begin(), h.buf.end());
  pb.bytes.insert(pb.bytes.end(), stored.begin(), stored.end());
  pb.uncompressed_total = h.buf.size() + uncompressed_body;
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
  std::vector<uint8_t> body;
  std::vector<uint8_t> rep = encode_levels_rle(col.rep_levels, col.num_levels, 1);
  put_u32_le(body, (uint32_t)rep.size());
  body.insert(body.end(), rep.begin(), rep.end());
  std::vector<uint8_t> def = encode_levels_rle(col.def_levels, col.num_levels, 2);
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
  std::vector<uint8_t> stored =
      (codec == CODEC_ZSTD) ? zstd_compress_block(body, zstd_level) : body;

  TCompactWriter h;
  h.structBegin();
  h.writeI32Field(1, PAGE_DATA);
  h.writeI32Field(2, (int32_t)uncompressed_body);
  h.writeI32Field(3, (int32_t)stored.size());
  h.writeFieldHeader(CT_STRUCT, 5); // data_page_header
  h.structBegin();
  h.writeI32Field(1, (int32_t)col.num_levels); // num_values = level count
  h.writeI32Field(2, ENC_PLAIN);
  h.writeI32Field(3, ENC_RLE); // definition_level_encoding
  h.writeI32Field(4, ENC_RLE); // repetition_level_encoding
  h.structEnd();
  h.structEnd();

  PageBuild pb;
  pb.bytes.reserve(h.buf.size() + stored.size());
  pb.bytes.insert(pb.bytes.end(), h.buf.begin(), h.buf.end());
  pb.bytes.insert(pb.bytes.end(), stored.begin(), stored.end());
  pb.uncompressed_total = h.buf.size() + uncompressed_body;
  return pb;
}

// ---- dictionary encoding ----
//
// Auto-build cardinality gate: build a dictionary only when it pays off — the
// distinct count must be at most half the present (non-null) values (>=2x
// repetition) AND not exceed DICT_MAX_CARDINALITY entries (bounds the
// dictionary-page size and the code bit width for pathological inputs). Both
// thresholds are deliberately conservative; ZSTD recovers most of what a looser
// gate would catch. Surfaced here for tuning.
static const uint32_t DICT_MAX_CARDINALITY = 1u << 20; // 1,048,576 entries

// BuiltDict owns the dictionary values + per-row codes produced by an
// auto-build; it must outlive the build_dict_column call that reads it.
struct BuiltDict {
  std::vector<int32_t> i32;
  std::vector<int64_t> i64;
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

// Cheap pre-check before the full auto-build hash pass: sample up to
// SAMPLE_CAP present values at an even stride across the column and check
// whether the sample already looks essentially all-distinct. If so, the full
// build (which hashes up to present/2 rows before its own cap kicks in — see
// DICT_MAX_CARDINALITY below) is very unlikely to succeed, so skip straight to
// PLAIN instead of paying for it. A stride (not a prefix) avoids being fooled
// by column locality (e.g. a value that's constant for a leading run then
// diverges). This can only cost a compression opportunity on a low-cardinality
// column whose sample happened to look diverse — never a correctness issue,
// since PLAIN is always a valid fallback and the full build's own cap still
// applies whenever this pre-check doesn't fire.
static const size_t DICT_SAMPLE_CAP = 512;
static const double DICT_SAMPLE_DISTINCT_THRESHOLD = 0.95;

template <typename T>
inline bool dict_sample_looks_high_cardinality(const T *vals, const uint8_t *validity,
                                               size_t num_rows, size_t present) {
  if (present <= DICT_SAMPLE_CAP)
    return false; // column is small enough that the full build is cheap anyway
  const size_t stride = num_rows / DICT_SAMPLE_CAP;
  std::unordered_set<T> seen;
  size_t sampled = 0;
  for (size_t i = 0; i < num_rows && sampled < DICT_SAMPLE_CAP; i += (stride ? stride : 1)) {
    if (!is_valid(validity, i))
      continue;
    seen.insert(vals[i]);
    sampled++;
  }
  return sampled > 0 && (double)seen.size() / (double)sampled >= DICT_SAMPLE_DISTINCT_THRESHOLD;
}

inline bool dict_sample_looks_high_cardinality_f64(const double *vals, const uint8_t *validity,
                                                    size_t num_rows, size_t present) {
  if (present <= DICT_SAMPLE_CAP)
    return false;
  const size_t stride = num_rows / DICT_SAMPLE_CAP;
  std::unordered_set<uint64_t> seen;
  size_t sampled = 0;
  for (size_t i = 0; i < num_rows && sampled < DICT_SAMPLE_CAP; i += (stride ? stride : 1)) {
    if (!is_valid(validity, i))
      continue;
    uint64_t bits;
    std::memcpy(&bits, &vals[i], 8);
    seen.insert(bits);
    sampled++;
  }
  return sampled > 0 && (double)seen.size() / (double)sampled >= DICT_SAMPLE_DISTINCT_THRESHOLD;
}

inline bool dict_sample_looks_high_cardinality_str(const StrSlice *vals, const uint8_t *validity,
                                                    size_t num_rows, size_t present) {
  if (present <= DICT_SAMPLE_CAP)
    return false;
  const size_t stride = num_rows / DICT_SAMPLE_CAP;
  std::unordered_set<std::string_view> seen;
  size_t sampled = 0;
  for (size_t i = 0; i < num_rows && sampled < DICT_SAMPLE_CAP; i += (stride ? stride : 1)) {
    if (!is_valid(validity, i))
      continue;
    const StrSlice &s = vals[i];
    seen.insert(std::string_view((const char *)s.ptr, s.len));
    sampled++;
  }
  return sampled > 0 && (double)seen.size() / (double)sampled >= DICT_SAMPLE_DISTINCT_THRESHOLD;
}

// Auto-build a dictionary over fixed-width values. Returns false (=> emit
// PLAIN) once the distinct count exceeds the gate; on success `dict` holds the
// unique values in first-seen order and `codes` one code per logical row.
template <typename T>
inline bool build_numeric_dict(const T *vals, const uint8_t *validity,
                               size_t num_rows, size_t present,
                               std::vector<T> &dict,
                               std::vector<uint32_t> &codes) {
  const size_t cap =
      std::min<size_t>((size_t)DICT_MAX_CARDINALITY, present / 2);
  std::unordered_map<T, uint32_t> seen;
  codes.assign(num_rows, 0);
  for (size_t i = 0; i < num_rows; i++) {
    if (!is_valid(validity, i))
      continue;
    T v = vals[i];
    auto it = seen.find(v);
    if (it != seen.end()) {
      codes[i] = it->second;
    } else {
      if (dict.size() >= cap)
        return false;
      uint32_t code = (uint32_t)dict.size();
      seen.emplace(v, code);
      dict.push_back(v);
      codes[i] = code;
    }
  }
  return true;
}

// Doubles are keyed by their bit pattern, not value: -0.0 and +0.0 must NOT
// share an entry (writing one for the other would change the bytes), and each
// NaN bit pattern stays distinct.
inline bool build_double_dict(const double *vals, const uint8_t *validity,
                              size_t num_rows, size_t present,
                              std::vector<double> &dict,
                              std::vector<uint32_t> &codes) {
  const size_t cap =
      std::min<size_t>((size_t)DICT_MAX_CARDINALITY, present / 2);
  std::unordered_map<uint64_t, uint32_t> seen;
  codes.assign(num_rows, 0);
  for (size_t i = 0; i < num_rows; i++) {
    if (!is_valid(validity, i))
      continue;
    uint64_t bits;
    std::memcpy(&bits, &vals[i], 8);
    auto it = seen.find(bits);
    if (it != seen.end()) {
      codes[i] = it->second;
    } else {
      if (dict.size() >= cap)
        return false;
      uint32_t code = (uint32_t)dict.size();
      seen.emplace(bits, code);
      dict.push_back(vals[i]);
      codes[i] = code;
    }
  }
  return true;
}

inline bool build_string_dict(const StrSlice *vals, const uint8_t *validity,
                              size_t num_rows, size_t present,
                              std::vector<StrSlice> &dict,
                              std::vector<uint32_t> &codes) {
  const size_t cap =
      std::min<size_t>((size_t)DICT_MAX_CARDINALITY, present / 2);
  std::unordered_map<std::string_view, uint32_t> seen;
  codes.assign(num_rows, 0);
  for (size_t i = 0; i < num_rows; i++) {
    if (!is_valid(validity, i))
      continue;
    const StrSlice &s = vals[i];
    std::string_view key((const char *)s.ptr, s.len);
    auto it = seen.find(key);
    if (it != seen.end()) {
      codes[i] = it->second;
    } else {
      if (dict.size() >= cap)
        return false;
      uint32_t code = (uint32_t)dict.size();
      seen.emplace(key, code);
      dict.push_back(s);
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
};

inline DictColumnBuild build_dict_column(const ColumnInput &col, size_t num_rows,
                                         int codec, int zstd_level) {
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
    if (col.type == PT_INT64)
      std::sort(perm.begin(), perm.end(),
                [&](uint32_t a, uint32_t b) { return col.i64[a] < col.i64[b]; });
    else if (col.type == PT_INT32)
      std::sort(perm.begin(), perm.end(),
                [&](uint32_t a, uint32_t b) { return col.i32[a] < col.i32[b]; });
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

  TCompactWriter dh;
  dh.structBegin();
  dh.writeI32Field(1, PAGE_DICTIONARY);             // type
  dh.writeI32Field(2, (int32_t)dict_uncompressed);  // uncompressed_page_size
  dh.writeI32Field(3, (int32_t)dict_stored.size()); // compressed_page_size
  dh.writeFieldHeader(CT_STRUCT, 7);                // dictionary_page_header
  dh.structBegin();
  dh.writeI32Field(1, (int32_t)col.dict_count);     // num_values
  dh.writeI32Field(2, ENC_PLAIN);                   // encoding (PLAIN values)
  dh.writeBoolField(3, sorted);                     // is_sorted (see above)
  dh.structEnd();
  dh.structEnd();

  // ---- data page: def levels, bit_width byte, RLE/bit-packed indices ----
  std::vector<uint8_t> data_body;
  std::vector<uint8_t> def = encode_def_levels(col.validity, num_rows);
  put_u32_le(data_body, (uint32_t)def.size());
  data_body.insert(data_body.end(), def.begin(), def.end());
  int bw = dict_bit_width(col.dict_count);
  data_body.push_back((uint8_t)bw);
  std::vector<uint32_t> present;
  present.reserve(num_rows);
  for (size_t i = 0; i < num_rows; i++)
    if (is_valid(col.validity, i))
      present.push_back(sorted ? inv[col.codes[i]] : col.codes[i]);
  std::vector<uint8_t> idx =
      encode_dict_indices(present.data(), present.size(), bw);
  data_body.insert(data_body.end(), idx.begin(), idx.end());

  size_t data_uncompressed = data_body.size();
  std::vector<uint8_t> data_stored = (codec == CODEC_ZSTD)
                                         ? zstd_compress_block(data_body, zstd_level)
                                         : data_body;

  TCompactWriter ph;
  ph.structBegin();
  ph.writeI32Field(1, PAGE_DATA);
  ph.writeI32Field(2, (int32_t)data_uncompressed);
  ph.writeI32Field(3, (int32_t)data_stored.size());
  ph.writeFieldHeader(CT_STRUCT, 5); // data_page_header
  ph.structBegin();
  ph.writeI32Field(1, (int32_t)num_rows);  // num_values (incl. nulls)
  ph.writeI32Field(2, ENC_RLE_DICTIONARY); // encoding
  ph.writeI32Field(3, ENC_RLE);            // definition_level_encoding
  ph.writeI32Field(4, ENC_RLE);            // repetition_level_encoding
  ph.structEnd();
  ph.structEnd();

  DictColumnBuild out;
  out.dict_page_len = dh.buf.size() + dict_stored.size();
  out.uncompressed_total =
      dh.buf.size() + dict_uncompressed + ph.buf.size() + data_uncompressed;
  out.bytes.reserve(out.dict_page_len + ph.buf.size() + data_stored.size());
  out.bytes.insert(out.bytes.end(), dh.buf.begin(), dh.buf.end());
  out.bytes.insert(out.bytes.end(), dict_stored.begin(), dict_stored.end());
  out.bytes.insert(out.bytes.end(), ph.buf.begin(), ph.buf.end());
  out.bytes.insert(out.bytes.end(), data_stored.begin(), data_stored.end());
  return out;
}

// ---- schema serialization ----

inline void write_schema(TCompactWriter &w, const std::vector<ColumnInput> &cols) {
  // Flat pre-order list: root + each column's subtree (1 element for a
  // primitive; 3 for a LIST: group, repeated "list" group, "element").
  uint32_t n_elems = 1;
  for (const auto &c : cols)
    n_elems += c.is_array ? 3 : 1;
  w.writeFieldHeader(CT_LIST, 2); // FileMetaData.schema
  w.writeListHeader(CT_STRUCT, n_elems);

  // root
  w.structBegin();
  w.writeStringField(4, "schema");                 // name
  w.writeI32Field(5, (int32_t)cols.size());        // num_children
  w.structEnd();

  for (const auto &c : cols) {
    if (c.is_array) {
      // LIST group (OPTIONAL) -> repeated "list" group -> "element".
      w.structBegin();
      w.writeI32Field(3, REP_OPTIONAL);            // repetition_type
      w.writeStringField(4, c.name);               // name
      w.writeI32Field(5, 1);                       // num_children
      w.writeI32Field(6, CONV_LIST);               // converted_type LIST
      w.writeFieldHeader(CT_STRUCT, 10);           // logicalType
      w.structBegin();                             //   LogicalType union
      w.writeFieldHeader(CT_STRUCT, 3);            //   LIST member
      w.structBegin();                             //   ListType {}
      w.structEnd();
      w.structEnd();
      w.structEnd();

      w.structBegin();                             // repeated group "list"
      w.writeI32Field(3, REP_REPEATED);
      w.writeStringField(4, "list");
      w.writeI32Field(5, 1);                       // num_children
      w.structEnd();

      w.structBegin();                             // "element" leaf
      w.writeI32Field(1, (int32_t)c.elem_type);    // type
      w.writeI32Field(3, REP_OPTIONAL);            // repetition_type
      w.writeStringField(4, "element");
      if (c.elem_type == PT_BYTE_ARRAY && c.elem_is_utf8)
        w.writeI32Field(6, CONV_UTF8);
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
    }
    w.structEnd();
  }
}

// ---- ColumnMetaData / ColumnChunk / RowGroup ----

inline void write_column_chunk(TCompactWriter &w, const ColumnInput &c,
                               size_t num_rows, int64_t data_page_offset,
                               int64_t dict_page_offset,
                               size_t compressed_total, size_t uncompressed_total,
                               int codec, const ColumnStats &stats,
                               int64_t bloom_offset, int32_t bloom_length) {
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
    // path_in_schema: [name] for primitives; [name,"list","element"] for lists.
    w.writeFieldHeader(CT_LIST, 3);
    if (c.is_array) {
      w.writeListHeader(CT_BINARY, 3);
      w.writeListString(c.name);
      w.writeListString("list");
      w.writeListString("element");
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
  w.structEnd();
}

// ---- top-level file assembly ----
//
// Returns the complete parquet file as bytes. All columns must have the same
// row count (`num_rows`). max_rows_per_rg controls row group splitting:
//   0 (default) — single row group (original behaviour).
//   N > 0       — at most N rows per row group; N is rounded up to the nearest
//                 multiple of 8 so validity bit-offsets stay byte-aligned.
//                 Array columns are not supported with splitting; if any column
//                 has is_array=true the value is ignored and a single row group
//                 is written.
// out_stats is filled only for single-row-group files; it is left empty for
// multi-row-group files.
inline std::vector<uint8_t> WriteParquet(const std::vector<ColumnInput> &cols,
                                         size_t num_rows,
                                         int codec = CODEC_UNCOMPRESSED,
                                         int zstd_level = 3,
                                         std::vector<ColumnStats> *out_stats =
                                             nullptr,
                                         size_t max_rows_per_rg = 0) {
  // Disable splitting when any column uses the array path (rep/def levels
  // are embedded in the page; pointer-slicing is not valid there).
  if (max_rows_per_rg > 0) {
    for (const auto &c : cols)
      if (c.is_array) { max_rows_per_rg = 0; break; }
  }
  // Round up to nearest multiple of 8 so validity byte offset = start >> 3.
  if (max_rows_per_rg > 0)
    max_rows_per_rg = (max_rows_per_rg + 7) & ~(size_t)7;

  size_t rg_size = (max_rows_per_rg > 0 && max_rows_per_rg < num_rows)
                       ? max_rows_per_rg
                       : num_rows;
  size_t n_rg = (num_rows == 0) ? 1 : (num_rows + rg_size - 1) / rg_size;

  // Per-row-group metadata collected during the write loop.
  struct RGMeta {
    std::vector<int64_t>     data_offsets;
    std::vector<int64_t>     dict_offsets;
    std::vector<size_t>      sizes;
    std::vector<size_t>      uncompressed;
    std::vector<ColumnStats> stats;
    std::vector<int64_t>     bloom_offset;
    std::vector<int32_t>     bloom_length;
    size_t row_count      = 0;
    size_t total_byte_size = 0;
  };
  std::vector<RGMeta> rg_meta(n_rg);

  std::vector<uint8_t> file;
  file.reserve(1024);
  const char *MAGIC = "PAR1";
  file.insert(file.end(), MAGIC, MAGIC + 4);

  for (size_t rg = 0; rg < n_rg; rg++) {
    size_t rg_start = rg * rg_size;
    size_t rg_rows  = std::min(rg_size, num_rows - rg_start);
    RGMeta &meta = rg_meta[rg];
    meta.row_count = rg_rows;
    meta.data_offsets.resize(cols.size());
    meta.dict_offsets.resize(cols.size(), -1);
    meta.sizes.resize(cols.size());
    meta.uncompressed.resize(cols.size());
    meta.stats.resize(cols.size());
    meta.bloom_offset.resize(cols.size(), -1);
    meta.bloom_length.resize(cols.size(), 0);

    // Build per-row-group column views by offsetting pointers.
    // Validity is bit-packed; rg_start is a multiple of 8 by construction so
    // the byte offset rg_start>>3 is exact.
    // PRESERVE-dict columns (codes!=nullptr): dict buffers (i32/i64/f64/strs)
    // point at dictionary values, NOT per-row data — do not offset them.
    std::vector<ColumnInput> rg_cols(cols.size());
    for (size_t i = 0; i < cols.size(); i++) {
      rg_cols[i] = cols[i];
      if (!cols[i].codes) {
        if (cols[i].i32)     rg_cols[i].i32     = cols[i].i32     + rg_start;
        if (cols[i].i64)     rg_cols[i].i64     = cols[i].i64     + rg_start;
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

    // Write all column chunks for this row group.
    for (size_t i = 0; i < rg_cols.size(); i++) {
      int64_t page_start = (int64_t)file.size();
      if (rg_cols[i].is_array) {
        PageBuild pb = build_array_data_page(rg_cols[i], codec, zstd_level);
        meta.data_offsets[i] = page_start;
        meta.sizes[i]        = pb.bytes.size();
        meta.uncompressed[i] = pb.uncompressed_total;
        file.insert(file.end(), pb.bytes.begin(), pb.bytes.end());
        continue;
      }

      meta.stats[i] = compute_stats(rg_cols[i], rg_rows);

      bool use_dict = false;
      DictColumnBuild dcb;
      BuiltDict bd;
      if (rg_cols[i].codes != nullptr) {
        use_dict = true;
        dcb = build_dict_column(rg_cols[i], rg_rows, codec, zstd_level);
      } else if (rg_cols[i].dict_enabled) {
        size_t present = rg_rows - (size_t)meta.stats[i].null_count;
        ColumnInput dcol = rg_cols[i];
        bool built = false;
        if (present > 0) {
          switch (rg_cols[i].type) {
          case PT_INT32:
            if (!dict_sample_looks_high_cardinality<int32_t>(rg_cols[i].i32, rg_cols[i].validity,
                                                              rg_rows, present))
              built = build_numeric_dict<int32_t>(rg_cols[i].i32, rg_cols[i].validity,
                                                  rg_rows, present, bd.i32, bd.codes);
            if (built) { dcol.i32 = bd.i32.data(); dcol.dict_count = (uint32_t)bd.i32.size(); }
            break;
          case PT_INT64:
            if (!dict_sample_looks_high_cardinality<int64_t>(rg_cols[i].i64, rg_cols[i].validity,
                                                              rg_rows, present))
              built = build_numeric_dict<int64_t>(rg_cols[i].i64, rg_cols[i].validity,
                                                  rg_rows, present, bd.i64, bd.codes);
            if (built) { dcol.i64 = bd.i64.data(); dcol.dict_count = (uint32_t)bd.i64.size(); }
            break;
          case PT_DOUBLE:
            if (!dict_sample_looks_high_cardinality_f64(rg_cols[i].f64, rg_cols[i].validity,
                                                         rg_rows, present))
              built = build_double_dict(rg_cols[i].f64, rg_cols[i].validity,
                                        rg_rows, present, bd.f64, bd.codes);
            if (built) { dcol.f64 = bd.f64.data(); dcol.dict_count = (uint32_t)bd.f64.size(); }
            break;
          case PT_BYTE_ARRAY:
            if (!dict_sample_looks_high_cardinality_str(rg_cols[i].strs, rg_cols[i].validity,
                                                         rg_rows, present))
              built = build_string_dict(rg_cols[i].strs, rg_cols[i].validity,
                                        rg_rows, present, bd.strs, bd.codes);
            if (built) { dcol.strs = bd.strs.data(); dcol.dict_count = (uint32_t)bd.strs.size(); }
            break;
          default:
            break;
          }
        }
        if (built) {
          dcol.codes = bd.codes.data();
          use_dict = true;
          dcb = build_dict_column(dcol, rg_rows, codec, zstd_level);
        }
      }

      if (use_dict) {
        meta.dict_offsets[i] = page_start;
        meta.data_offsets[i] = page_start + (int64_t)dcb.dict_page_len;
        meta.sizes[i]        = dcb.bytes.size();
        meta.uncompressed[i] = dcb.uncompressed_total;
        file.insert(file.end(), dcb.bytes.begin(), dcb.bytes.end());
      } else {
        PageBuild pb = build_data_page(rg_cols[i], rg_rows, codec, zstd_level);
        meta.data_offsets[i] = page_start;
        meta.sizes[i]        = pb.bytes.size();
        meta.uncompressed[i] = pb.uncompressed_total;
        file.insert(file.end(), pb.bytes.begin(), pb.bytes.end());
      }
    }

    meta.total_byte_size = 0;
    for (size_t s : meta.uncompressed) meta.total_byte_size += s;

    // Bloom filters for this row group.
    for (size_t i = 0; i < rg_cols.size(); i++) {
      if (!rg_cols[i].bloom) continue;
      std::vector<uint64_t> hashes = bloom_hashes(rg_cols[i], rg_rows);
      if (hashes.empty()) continue;
      size_t ndv = bloom_ndv(hashes);
      meta.stats[i].distinct_count = (int64_t)ndv;
      BloomFilter bf = bloom_build(hashes, ndv, 0.01);
      std::vector<uint8_t> hdr = build_bloom_header((int32_t)bf.bitset.size());
      meta.bloom_offset[i] = (int64_t)file.size();
      meta.bloom_length[i] = (int32_t)(hdr.size() + bf.bitset.size());
      file.insert(file.end(), hdr.begin(), hdr.end());
      file.insert(file.end(), bf.bitset.begin(), bf.bitset.end());
    }
  } // end row group loop

  // FileMetaData.
  TCompactWriter fm;
  fm.structBegin();
  fm.writeI32Field(1, 1); // version
  write_schema(fm, cols); // field 2
  fm.writeI64Field(3, (int64_t)num_rows);

  fm.writeFieldHeader(CT_LIST, 4);
  fm.writeListHeader(CT_STRUCT, (uint32_t)n_rg);
  for (size_t rg = 0; rg < n_rg; rg++) {
    const RGMeta &meta = rg_meta[rg];
    fm.structBegin(); // RowGroup
    fm.writeFieldHeader(CT_LIST, 1);
    fm.writeListHeader(CT_STRUCT, (uint32_t)cols.size());
    for (size_t i = 0; i < cols.size(); i++)
      write_column_chunk(fm, cols[i], meta.row_count, meta.data_offsets[i],
                         meta.dict_offsets[i], meta.sizes[i], meta.uncompressed[i],
                         codec, meta.stats[i], meta.bloom_offset[i], meta.bloom_length[i]);
    fm.writeI64Field(2, (int64_t)meta.total_byte_size); // total_byte_size
    fm.writeI64Field(3, (int64_t)meta.row_count);       // num_rows
    fm.structEnd();
  }
  fm.writeStringField(6, RUGO_PARQUET_CREATED_BY); // created_by
  // column_orders (field 7): one ColumnOrder per leaf column, each the
  // TypeDefinedOrder union member (field 1 = empty struct). Signals that
  // min_value/max_value use type-defined ordering so readers trust them.
  fm.writeFieldHeader(CT_LIST, 7);
  fm.writeListHeader(CT_STRUCT, (uint32_t)cols.size());
  for (size_t i = 0; i < cols.size(); i++) {
    fm.structBegin();                  // ColumnOrder (union)
    fm.writeFieldHeader(CT_STRUCT, 1); // TYPE_ORDER
    fm.structBegin();                  // TypeDefinedOrder {}
    fm.structEnd();
    fm.structEnd();
  }
  fm.structEnd();

  file.insert(file.end(), fm.buf.begin(), fm.buf.end());
  put_u32_le(file, (uint32_t)fm.buf.size()); // footer length
  file.insert(file.end(), MAGIC, MAGIC + 4);
  // out_stats: only meaningful for single-RG files; unsupported for multi-RG.
  if (out_stats && n_rg == 1)
    *out_stats = std::move(rg_meta[0].stats);
  return file;
}

} // namespace rugo_pq_write
