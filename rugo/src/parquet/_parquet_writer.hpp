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
enum { ENC_PLAIN = 0, ENC_RLE = 3 };
enum { CODEC_UNCOMPRESSED = 0, CODEC_ZSTD = 6 };
enum { PAGE_DATA = 0 };
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

  switch (col.type) {
  case PT_INT32: {
    bool any = false;
    int32_t lo = 0, hi = 0;
    for (size_t i = 0; i < num_rows; i++) {
      if (!is_valid(col.validity, i))
        continue;
      int32_t v = col.i32[i];
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
      int64_t v = col.i64[i];
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
      double v = col.f64[i];
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
      const StrSlice &s = col.strs[i];
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
  for (size_t i = 0; i < num_rows; i++) {
    if (!is_valid(col.validity, i))
      continue;
    switch (col.type) {
    case PT_INT32: {
      int32_t v = col.i32[i];
      std::memcpy(buf, &v, 4);
      hashes.push_back(bloom_hash(buf, 4));
      break;
    }
    case PT_INT64: {
      int64_t v = col.i64[i];
      std::memcpy(buf, &v, 8);
      hashes.push_back(bloom_hash(buf, 8));
      break;
    }
    case PT_DOUBLE: {
      std::memcpy(buf, &col.f64[i], 8);
      hashes.push_back(bloom_hash(buf, 8));
      break;
    }
    case PT_BYTE_ARRAY: {
      const StrSlice &s = col.strs[i];
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
                               size_t num_rows, int64_t page_offset,
                               size_t compressed_total, size_t uncompressed_total,
                               int codec, const ColumnStats &stats,
                               int64_t bloom_offset, int32_t bloom_length) {
  w.structBegin(); // ColumnChunk
  w.writeI64Field(2, page_offset);                 // file_offset
  w.writeFieldHeader(CT_STRUCT, 3);                // meta_data
  {
    w.structBegin(); // ColumnMetaData
    w.writeI32Field(1, (int32_t)(c.is_array ? c.elem_type : c.type)); // type
    // encodings: [PLAIN, RLE]
    w.writeFieldHeader(CT_LIST, 2);
    w.writeListHeader(CT_I32, 2);
    w.writeListI32(ENC_PLAIN);
    w.writeListI32(ENC_RLE);
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
    w.writeI64Field(9, page_offset);               // data_page_offset
    // statistics (field 12): null_count(3), max_value(5), min_value(6),
    // is_max_value_exact(7), is_min_value_exact(8). Ascending ids. Omitted for
    // LIST columns (nested null semantics — no leaf stats emitted).
    if (!c.is_array) {
      w.writeFieldHeader(CT_STRUCT, 12);
      w.structBegin(); // Statistics
      w.writeI64Field(3, stats.null_count);
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
// row count (`num_rows`). One row group, one data page per column.
inline std::vector<uint8_t> WriteParquet(const std::vector<ColumnInput> &cols,
                                         size_t num_rows,
                                         int codec = CODEC_UNCOMPRESSED,
                                         int zstd_level = 3,
                                         std::vector<ColumnStats> *out_stats =
                                             nullptr) {
  std::vector<uint8_t> file;
  file.reserve(1024);
  const char *MAGIC = "PAR1";
  file.insert(file.end(), MAGIC, MAGIC + 4);

  // Write all data pages, recording their offsets and sizes (compressed +
  // uncompressed) for the column metadata.
  std::vector<int64_t> offsets(cols.size());
  std::vector<size_t> sizes(cols.size());
  std::vector<size_t> uncompressed(cols.size());
  std::vector<ColumnStats> stats(cols.size());
  for (size_t i = 0; i < cols.size(); i++) {
    offsets[i] = (int64_t)file.size();
    PageBuild pb;
    if (cols[i].is_array) {
      pb = build_array_data_page(cols[i], codec, zstd_level); // stats[i] left empty
    } else {
      stats[i] = compute_stats(cols[i], num_rows);
      pb = build_data_page(cols[i], num_rows, codec, zstd_level);
    }
    sizes[i] = pb.bytes.size();
    uncompressed[i] = pb.uncompressed_total;
    file.insert(file.end(), pb.bytes.begin(), pb.bytes.end());
  }

  size_t total_byte_size = 0;
  for (size_t s : uncompressed)
    total_byte_size += s;

  // Bloom filters: written after the data pages, before the footer. For each
  // flagged column emit [BloomFilterHeader][bitset] and record offset/length.
  std::vector<int64_t> bloom_offset(cols.size(), -1);
  std::vector<int32_t> bloom_length(cols.size(), 0);
  for (size_t i = 0; i < cols.size(); i++) {
    if (!cols[i].bloom)
      continue;
    std::vector<uint64_t> hashes = bloom_hashes(cols[i], num_rows);
    if (hashes.empty())
      continue; // all-null column: nothing to filter
    BloomFilter bf = bloom_build(hashes, bloom_ndv(hashes), 0.01);
    std::vector<uint8_t> hdr = build_bloom_header((int32_t)bf.bitset.size());
    bloom_offset[i] = (int64_t)file.size();
    bloom_length[i] = (int32_t)(hdr.size() + bf.bitset.size());
    file.insert(file.end(), hdr.begin(), hdr.end());
    file.insert(file.end(), bf.bitset.begin(), bf.bitset.end());
  }

  // FileMetaData.
  TCompactWriter fm;
  fm.structBegin();
  fm.writeI32Field(1, 1); // version
  write_schema(fm, cols); // field 2
  fm.writeI64Field(3, (int64_t)num_rows);

  // row_groups: single element
  fm.writeFieldHeader(CT_LIST, 4);
  fm.writeListHeader(CT_STRUCT, 1);
  {
    fm.structBegin(); // RowGroup
    // columns
    fm.writeFieldHeader(CT_LIST, 1);
    fm.writeListHeader(CT_STRUCT, (uint32_t)cols.size());
    for (size_t i = 0; i < cols.size(); i++)
      write_column_chunk(fm, cols[i], num_rows, offsets[i], sizes[i],
                         uncompressed[i], codec, stats[i],
                         bloom_offset[i], bloom_length[i]);
    fm.writeI64Field(2, (int64_t)total_byte_size); // total_byte_size
    fm.writeI64Field(3, (int64_t)num_rows);        // num_rows
    fm.structEnd();
  }
  fm.writeStringField(6, RUGO_PARQUET_CREATED_BY); // created_by
  // column_orders (field 7): one ColumnOrder per leaf column, each the
  // TypeDefinedOrder union member (field 1 = empty struct). Signals that
  // min_value/max_value use type-defined ordering so readers trust them.
  fm.writeFieldHeader(CT_LIST, 7);
  fm.writeListHeader(CT_STRUCT, (uint32_t)cols.size());
  for (size_t i = 0; i < cols.size(); i++) {
    fm.structBegin();                 // ColumnOrder (union)
    fm.writeFieldHeader(CT_STRUCT, 1); // TYPE_ORDER
    fm.structBegin();                 // TypeDefinedOrder {}
    fm.structEnd();
    fm.structEnd();
  }
  fm.structEnd();

  file.insert(file.end(), fm.buf.begin(), fm.buf.end());
  put_u32_le(file, (uint32_t)fm.buf.size()); // footer length
  file.insert(file.end(), MAGIC, MAGIC + 4);
  if (out_stats)
    *out_stats = std::move(stats);
  return file;
}

} // namespace rugo_pq_write
