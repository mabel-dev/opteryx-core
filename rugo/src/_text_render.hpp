#pragma once
// Fast Morsel -> CSV / JSONL rendering.
//
// Per-column dispatch is resolved ONCE (into a function pointer), so the row
// loop has no per-cell type switch. int / bool / date / timestamp columns are
// rendered with draken's batch, compression-aware cast-to-string kernels
// (low-cardinality columns format each unique value once). float uses
// std::to_chars (shortest round-trip — the cast kernel is 6-dp display only),
// decimal/time use the dedicated formatters, strings/arrays render directly.
//
// The choice is made from the column's physical type plus its ColumnDesc (see
// value_format.hpp) — one descriptor carrying the logical kind and whatever
// parameters that kind implies. The kind matters because a UINT32 column is an
// IPv4 address or a plain integer depending on it, and nothing else can say.

#include "interop/value_format.hpp"  // moved into draken; resolved via -I draken

#include "core/alloc.h"          // draken_free
#include "core/fp16.h"           // draken_fp16_to_fp32 — VECTOR_FP16 renders as an array of floats
#include "ops/vec_result.h"      // VecResult
#include "ops/kernels/cast_kernels.h"

#include "BS_thread_pool.hpp"    // vendored bshoshany pool (same as the readers)

#include <algorithm>
#include <future>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace rugo_text {

// ---- Excel workbook limits (csv_write for_excel) ----
// A CSV file has no limits of its own; these are the limits of the grid the
// user is going to open it in (Excel 2007+ / .xlsx). Excel does not report
// them — it truncates the over-long cell and drops the rows and columns past
// the end of the sheet, silently — so for_excel refuses to write a file Excel
// would mangle rather than producing one that quietly loses data.
static constexpr size_t kExcelMaxRows = 1048576;      // sheet lines, header included
static constexpr size_t kExcelMaxCols = 16384;        // last column is XFD
static constexpr size_t kExcelMaxCellChars = 32767;

typedef void (*EmitFn)(std::string &, struct Col &, size_t);

struct Col {
  const DrakenVector *dv;    // original column (validity + per-cell formatters)
  DrakenVector sv;           // string source (cast result OR original string col)
  ColumnDesc desc;           // logical type of the column and (ARRAY) its element chain
  const std::string *name;   // for error messages; null on the JSONL path
  char delim;
  EmitFn emit;               // resolved once per column
  void *free_data;           // cast result block to draken_free (else null)
  const uint32_t *free_sel;  // owned cast selection to draken_free (else null)
  std::string scratch;       // reused per-row JSON staging buffer (ec_array only)
};

// Excel measures a cell in UTF-16 code units (what VBA's Len() counts), not in
// bytes and not in codepoints — an astral character costs two. Only ever called
// for a cell already over the limit in bytes, since n bytes can encode at most
// n code units.
static inline size_t utf16_length(const char *s, size_t n) {
  size_t units = 0;
  for (size_t i = 0; i < n; i++) {
    unsigned char c = (unsigned char)s[i];
    if ((c & 0xC0) == 0x80) continue; // continuation byte
    units += c >= 0xF0 ? 2 : 1;       // 4-byte sequence -> surrogate pair
  }
  return units;
}

[[noreturn]] static void excel_too_wide(const std::string &what, size_t chars) {
  throw std::invalid_argument(
      "write_csv(for_excel=True): " + what + " is " + std::to_string(chars) +
      " characters; Excel truncates cells over " +
      std::to_string(kExcelMaxCellChars) + ".");
}

// Fetch the rendered string cell at logical row i from a string-source vector.
static inline bool sv_cell(const DrakenVector &sv, size_t i, const char *&p,
                           uint32_t &n) {
  if (!row_valid(sv.validity, i))
    return false;
  const DrakenStringArena *a = (const DrakenStringArena *)sv.data;
  const DrakenStringSlot *s = &a->slots[sv.selection[i]];
  p = (const char *)str_data(s, a->arena);
  n = str_length(s);
  return true;
}

// ---- JSON cell emitters ----
static void ej_raw(std::string &o, Col &c, size_t i) {
  const char *p; uint32_t n;
  if (sv_cell(c.sv, i, p, n)) o.append(p, n); else o.append("null");
}
static void ej_quoted(std::string &o, Col &c, size_t i) {
  const char *p; uint32_t n;
  if (sv_cell(c.sv, i, p, n)) append_quoted_raw(o, p, n);
  else o.append("null");
}
static void ej_string(std::string &o, Col &c, size_t i) {
  const char *p; uint32_t n;
  if (sv_cell(c.sv, i, p, n)) json_string(o, p, n); else o.append("null");
}
// FLOAT32 renders through fmt_float on the un-promoted value, never through
// fmt_double on a widened double -- see fmt_float's comment in
// interop/value_format.hpp for why the two are not interchangeable.
// FLOAT32 renders through fmt_float on the un-promoted value, never through
// fmt_double on a widened double -- see fmt_float's comment in
// interop/value_format.hpp for why the two are not interchangeable.
static void ej_float(std::string &o, Col &c, size_t i) {
  if (!row_valid(c.dv->validity, i)) { o.append("null"); return; }
  uint32_t p = c.dv->selection[i];
  if (c.dv->type == DRAKEN_FLOAT64) {
    double v = ((const double *)c.dv->data)[p];
    if (double_is_nan_or_inf(v)) o.append("null"); else fmt_double(o, v);
  } else {
    float v = ((const float *)c.dv->data)[p];
    if (double_is_nan_or_inf((double)v)) o.append("null"); else fmt_float(o, v);
  }
}
static void ej_decimal(std::string &o, Col &c, size_t i) {
  if (!row_valid(c.dv->validity, i)) { o.append("null"); return; }
  uint32_t p = c.dv->selection[i];
  if (c.dv->type == DRAKEN_DECIMAL)
    fmt_decimal(o, (__int128)((const int64_t *)c.dv->data)[p], c.desc.column.scale);
  else { __int128 v; std::memcpy(&v, (const uint8_t *)c.dv->data + (size_t)p * 16, 16); fmt_decimal(o, v, c.desc.column.scale); }
}
static void ej_time(std::string &o, Col &c, size_t i) {
  if (!row_valid(c.dv->validity, i)) { o.append("null"); return; }
  uint32_t p = c.dv->selection[i];
  if (c.dv->type == DRAKEN_TIME64) fmt_time_quoted(o, ((const int64_t *)c.dv->data)[p], c.desc.column.unit);
  else fmt_time_quoted(o, ((const int32_t *)c.dv->data)[p], c.desc.column.unit);
}
static void ej_timestamp(std::string &o, Col &c, size_t i) {
  if (!row_valid(c.dv->validity, i)) { o.append("null"); return; }
  fmt_timestamp_quoted(o, ((const int64_t *)c.dv->data)[c.dv->selection[i]], c.desc.column.unit);
}
// Direct numeric/temporal emitters — used for DENSE columns (data_length >=
// length), where the batch cast kernel would format every value twice, allocate
// a slot block, and copy a third time. Reading data[selection[i]] straight into
// the output does one format and no allocation. (Dictionary columns, where the
// cast dedups repeated values, still take the cast path in resolve_col.)
template <typename T>
static void ej_int(std::string &o, Col &c, size_t i) {
  if (!row_valid(c.dv->validity, i)) { o.append("null"); return; }
  fmt_int64(o, (int64_t)((const T *)c.dv->data)[c.dv->selection[i]]);
}
// Unsigned widths widen to uint64_t, never int64_t: a UINT64 above INT64_MAX
// would otherwise render negative. See fmt_uint64 in value_format.hpp.
template <typename T>
static void ej_uint(std::string &o, Col &c, size_t i) {
  if (!row_valid(c.dv->validity, i)) { o.append("null"); return; }
  fmt_uint64(o, (uint64_t)((const T *)c.dv->data)[c.dv->selection[i]]);
}
static void ej_bool(std::string &o, Col &c, size_t i) {
  if (!row_valid(c.dv->validity, i)) { o.append("null"); return; }
  uint32_t p = c.dv->selection[i];
  o.append((((const uint8_t *)c.dv->data)[p >> 3] >> (p & 7)) & 1 ? "true" : "false");
}
static void ej_date(std::string &o, Col &c, size_t i) {
  if (!row_valid(c.dv->validity, i)) { o.append("null"); return; }
  fmt_date_quoted(o, ((const int32_t *)c.dv->data)[c.dv->selection[i]]);
}
static void ej_array(std::string &o, Col &c, size_t i) {
  // c.dv is already known DRAKEN_ARRAY (resolve_col only routes here for
  // that type), so this recurses through the whole ARRAY<ARRAY<...>> chain
  // in c.desc.levels rather than treating the immediate element vector as a
  // scalar leaf — see render_json_value in value_format.hpp.
  render_json_value(o, c.dv, i, c.desc.column, c.desc.levels, 0);
}
// VECTOR_FP16 has no wire representation in CSV/JSONL any more than Parquet
// does — rendered as an array of floats (fp16->fp32, same conversion as the
// Parquet writer's VECTOR_FP16 branch), matching ej_array/ec_array's style.
// Storage is dense fixed-width (dv->data + p*dim, exactly `dim` values, no
// per-element nulls), so there is no offsets buffer to read, unlike ARRAY.
static void ej_fp16(std::string &o, Col &c, size_t i) {
  if (!row_valid(c.dv->validity, i)) { o.append("null"); return; }
  uint32_t p = c.dv->selection[i];
  const uint16_t *base = (const uint16_t *)c.dv->data + (size_t)p * (size_t)c.desc.column.dim;
  o.push_back('[');
  for (int k = 0; k < c.desc.column.dim; k++) {
    if (k) o.push_back(',');
    double v = (double)draken_fp16_to_fp32(base[k]);
    if (double_is_nan_or_inf(v)) o.append("null"); else fmt_double(o, v);
  }
  o.push_back(']');
}
// An IPv4 column is physically DRAKEN_UINT32; only the descriptor kind says it
// is an address (see draken/logical_type.h). Rendered dotted-decimal via the
// canonical draken::ipv4::format, quoted in JSON like the other text-shaped
// scalars (date/timestamp/time) — an address is not a JSON number.
static void ej_ipv4(std::string &o, Col &c, size_t i) {
  if (!row_valid(c.dv->validity, i)) { o.append("null"); return; }
  fmt_ipv4_quoted(o, ((const uint32_t *)c.dv->data)[c.dv->selection[i]]);
}
static void ej_null(std::string &o, Col &c, size_t i) { (void)c; (void)i; o.append("null"); }

// ---- CSV cell emitters (null -> empty field) ----
static void ec_raw(std::string &o, Col &c, size_t i) {
  const char *p; uint32_t n; if (sv_cell(c.sv, i, p, n)) o.append(p, n);
}
static void ec_string(std::string &o, Col &c, size_t i) {
  const char *p; uint32_t n; if (sv_cell(c.sv, i, p, n)) csv_field(o, p, n, c.delim);
}
static void ec_float(std::string &o, Col &c, size_t i) {
  if (!row_valid(c.dv->validity, i)) return;
  uint32_t p = c.dv->selection[i];
  if (c.dv->type == DRAKEN_FLOAT64) {
    double v = ((const double *)c.dv->data)[p];
    if (!double_is_nan_or_inf(v)) fmt_double(o, v); // NaN/Infinity -> empty CSV field
  } else {
    float v = ((const float *)c.dv->data)[p];
    if (!double_is_nan_or_inf((double)v)) fmt_float(o, v);
  }
}
static void ec_decimal(std::string &o, Col &c, size_t i) {
  if (!row_valid(c.dv->validity, i)) return;
  uint32_t p = c.dv->selection[i];
  if (c.dv->type == DRAKEN_DECIMAL)
    fmt_decimal(o, (__int128)((const int64_t *)c.dv->data)[p], c.desc.column.scale);
  else { __int128 v; std::memcpy(&v, (const uint8_t *)c.dv->data + (size_t)p * 16, 16); fmt_decimal(o, v, c.desc.column.scale); }
}
static void ec_time(std::string &o, Col &c, size_t i) {
  if (!row_valid(c.dv->validity, i)) return;
  uint32_t p = c.dv->selection[i];
  if (c.dv->type == DRAKEN_TIME64) fmt_time(o, ((const int64_t *)c.dv->data)[p], c.desc.column.unit);
  else fmt_time(o, ((const int32_t *)c.dv->data)[p], c.desc.column.unit);
}
static void ec_timestamp(std::string &o, Col &c, size_t i) {
  if (!row_valid(c.dv->validity, i)) return;
  fmt_timestamp(o, ((const int64_t *)c.dv->data)[c.dv->selection[i]], c.desc.column.unit);
}
static void ec_array(std::string &o, Col &c, size_t i) {
  if (!row_valid(c.dv->validity, i)) return;
  c.scratch.clear(); ej_array(c.scratch, c, i); csv_field(o, c.scratch.data(), c.scratch.size(), c.delim);
}
static void ec_fp16(std::string &o, Col &c, size_t i) {
  if (!row_valid(c.dv->validity, i)) return;
  c.scratch.clear(); ej_fp16(c.scratch, c, i); csv_field(o, c.scratch.data(), c.scratch.size(), c.delim);
}
static void ec_null(std::string &o, Col &c, size_t i) { (void)o; (void)c; (void)i; }

// Direct CSV emitters (dense columns) — null -> empty field. See ej_int/bool/date.
template <typename T>
static void ec_int(std::string &o, Col &c, size_t i) {
  if (!row_valid(c.dv->validity, i)) return;
  fmt_int64(o, (int64_t)((const T *)c.dv->data)[c.dv->selection[i]]);
}
template <typename T>
static void ec_uint(std::string &o, Col &c, size_t i) {
  if (!row_valid(c.dv->validity, i)) return;
  fmt_uint64(o, (uint64_t)((const T *)c.dv->data)[c.dv->selection[i]]);
}
static void ec_bool(std::string &o, Col &c, size_t i) {
  if (!row_valid(c.dv->validity, i)) return;
  uint32_t p = c.dv->selection[i];
  o.append((((const uint8_t *)c.dv->data)[p >> 3] >> (p & 7)) & 1 ? "true" : "false");
}
static void ec_date(std::string &o, Col &c, size_t i) {
  if (!row_valid(c.dv->validity, i)) return;
  fmt_date(o, ((const int32_t *)c.dv->data)[c.dv->selection[i]]);
}
// Dotted-decimal, through csv_field so a '.' delimiter quotes the field instead
// of splitting the address across four columns. Formatted into a stack buffer
// (FORMAT_SCRATCH_BYTES — format() renders each octet with one 4-byte store, so
// it touches 16 even though an address is at most 15) rather than the shared
// scratch string, so the quote-awareness costs one pass over those bytes and no
// allocation.
static void ec_ipv4(std::string &o, Col &c, size_t i) {
  if (!row_valid(c.dv->validity, i)) return;
  char buf[draken::ipv4::FORMAT_SCRATCH_BYTES];
  uint32_t n = draken::ipv4::format(((const uint32_t *)c.dv->data)[c.dv->selection[i]], buf);
  csv_field(o, buf, n, c.delim);
}

// ---- Excel-checked CSV emitters ----
// Separate emitters rather than a flag tested per cell: the width check is only
// reachable for the three shapes that can plausibly produce a 32,767-character
// cell (text, ARRAY, VECTOR_FP16 — every numeric and temporal rendering is tens
// of bytes wide at most), and the default non-Excel path keeps the same single
// fixed emitter per column it has always had.
//
// The measurement is on the *cell content*, taken before csv_field quotes it:
// RFC 4180 quoting is transport, not content, and Excel does not count it.
static inline void check_cell_width(Col &c, size_t i, const char *p, size_t n) {
  if (n <= kExcelMaxCellChars)
    return;
  // VARBINARY is not text — its bytes are what lands in the cell, so they are
  // counted as-is rather than decoded as UTF-8.
  size_t chars = c.dv->type == DRAKEN_VARBINARY ? n : utf16_length(p, n);
  if (chars > kExcelMaxCellChars)
    excel_too_wide("column '" + (c.name ? *c.name : std::string("?")) +
                       "' row " + std::to_string(i),
                   chars);
}

static void ec_string_x(std::string &o, Col &c, size_t i) {
  const char *p; uint32_t n;
  if (!sv_cell(c.sv, i, p, n)) return;
  check_cell_width(c, i, p, n);
  csv_field(o, p, n, c.delim);
}
static void ec_array_x(std::string &o, Col &c, size_t i) {
  if (!row_valid(c.dv->validity, i)) return;
  c.scratch.clear(); ej_array(c.scratch, c, i);
  check_cell_width(c, i, c.scratch.data(), c.scratch.size());
  csv_field(o, c.scratch.data(), c.scratch.size(), c.delim);
}
static void ec_fp16_x(std::string &o, Col &c, size_t i) {
  if (!row_valid(c.dv->validity, i)) return;
  c.scratch.clear(); ej_fp16(c.scratch, c, i);
  check_cell_width(c, i, c.scratch.data(), c.scratch.size());
  csv_field(o, c.scratch.data(), c.scratch.size(), c.delim);
}

// Pick the width-specialized integer emitter so the row loop stays branch-free.
static inline EmitFn pick_int_emitter(DrakenType t, bool csv) {
  switch (t) {
  case DRAKEN_INT8:  return csv ? ec_int<int8_t>  : ej_int<int8_t>;
  case DRAKEN_INT16: return csv ? ec_int<int16_t> : ej_int<int16_t>;
  case DRAKEN_INT32: return csv ? ec_int<int32_t> : ej_int<int32_t>;
  default:           return csv ? ec_int<int64_t> : ej_int<int64_t>;
  }
}

static inline EmitFn pick_uint_emitter(DrakenType t, bool csv) {
  switch (t) {
  case DRAKEN_UINT8:  return csv ? ec_uint<uint8_t>  : ej_uint<uint8_t>;
  case DRAKEN_UINT16: return csv ? ec_uint<uint16_t> : ej_uint<uint16_t>;
  case DRAKEN_UINT32: return csv ? ec_uint<uint32_t> : ej_uint<uint32_t>;
  default:            return csv ? ec_uint<uint64_t> : ej_uint<uint64_t>;
  }
}

static inline void vr_to_dv(const VecResult &vr, DrakenVector &dv) {
  dv.data = vr.data; dv.selection = vr.selection; dv.validity = vr.validity;
  dv.data_length = vr.data_length; dv.length = vr.length; dv.type = vr.type;
  dv.flags = vr.flags;
}

// Resolve one column: choose the cell emitter, and (for int/bool/date/ts) run
// the batch cast kernel to produce the string source.
static void resolve_col(Col &c, const DrakenVector *dv,
                        const ColumnDesc &desc, const std::string *name,
                        char delim, bool csv, bool for_excel) {
  c.dv = dv; c.desc = desc; c.name = name; c.delim = delim;
  c.free_data = nullptr; c.free_sel = nullptr;
  VecResult vr;
  bool quoted = false;
  // The cast kernels format `data_length` physical values once each and dedup via
  // the preserved selection, so they only pay off when values repeat
  // (data_length < length, i.e. dictionary-encoded). For dense (== length) or
  // sliced (> length) columns the direct emitters are strictly cheaper.
  bool dense = dv->data_length >= dv->length;
  switch (dv->type) {
  case DRAKEN_INT8: case DRAKEN_INT16: case DRAKEN_INT32: case DRAKEN_INT64:
    if (dense) { c.emit = pick_int_emitter(dv->type, csv); return; }
    vr = draken_cast_integer_to_string(nullptr, dv); break;
  case DRAKEN_UINT8: case DRAKEN_UINT16: case DRAKEN_UINT32: case DRAKEN_UINT64:
    // IPv4 is a UINT32 whose descriptor kind says the bits are an address. The
    // physical tag cannot tell the two apart, so the kind is read HERE, once
    // per column, and never again — the row loop keeps its single fixed emitter.
    if (dv->type == DRAKEN_UINT32 && desc.column.kind == LogicalKind::IPV4) {
      c.emit = csv ? ec_ipv4 : ej_ipv4; return;
    }
    // No dict/dense split: draken's int->string cast kernel accepts only the
    // signed family (it rejects unsigned outright), so the direct emitter is
    // the only path. It reads via the uniform data[selection[i]] contract, so
    // it is correct for dense, constant and dictionary shapes alike — a
    // dictionary-encoded unsigned column simply formats repeated values more
    // than once.
    c.emit = pick_uint_emitter(dv->type, csv); return;
  case DRAKEN_BOOL:
    if (dense) { c.emit = csv ? ec_bool : ej_bool; return; }
    vr = draken_cast_bool_to_string(nullptr, dv); break;
  case DRAKEN_DATE32:
    if (dense) { c.emit = csv ? ec_date : ej_date; return; }
    vr = draken_cast_date_to_string(nullptr, dv); quoted = true; break;
  case DRAKEN_TIMESTAMP64:
    // RFC 3339 (T + Z) — draken's cast text is a non-compliant display format.
    c.emit = csv ? ec_timestamp : ej_timestamp; return;
  case DRAKEN_FLOAT32: case DRAKEN_FLOAT64:
    c.emit = csv ? ec_float : ej_float; return;
  case DRAKEN_DECIMAL: case DRAKEN_DECIMAL128:
    c.emit = csv ? ec_decimal : ej_decimal; return;
  case DRAKEN_TIME32: case DRAKEN_TIME64:
    c.emit = csv ? ec_time : ej_time; return;
  case DRAKEN_VARCHAR: case DRAKEN_NVARCHAR: case DRAKEN_VARBINARY: case DRAKEN_VARIANT:
    c.sv = *dv; c.emit = csv ? (for_excel ? ec_string_x : ec_string) : ej_string; return;
  case DRAKEN_ARRAY:
    c.emit = csv ? (for_excel ? ec_array_x : ec_array) : ej_array; return;
  case DRAKEN_VECTOR_FP16:
    c.emit = csv ? (for_excel ? ec_fp16_x : ec_fp16) : ej_fp16; return;
  default:
    c.emit = csv ? ec_null : ej_null; return;
  }
  // cast path
  vr_to_dv(vr, c.sv);
  c.free_data = vr.data;
  if (vr.owns_selection) c.free_sel = vr.selection;
  if (csv) c.emit = ec_raw;                 // numerics/temporals: raw in CSV
  else c.emit = quoted ? ej_quoted : ej_raw; // date/ts quoted in JSON
}

static inline void free_cols(std::vector<Col> &cols) {
  for (Col &c : cols) {
    if (c.free_data) draken_free(c.free_data);
    if (c.free_sel) draken_free((void *)c.free_sel);
  }
}

// ---- top-level writers ----

// Estimated rendered width (bytes) of one cell of `c`, for the output reserve.
// String-family columns are sized from their arena (exact long-payload bytes;
// inline payloads are <= 12 bytes each, folded into the constant). Everything
// else uses a fixed per-type width. Only a heuristic — an under-estimate costs
// a re-grow, an over-estimate costs slack — but arena-derived string widths
// remove the systematic under-reserve wide text columns used to cause.
static inline size_t est_cell_bytes(const Col &c) {
  const DrakenVector *dv = c.dv;
  switch (dv->type) {
  case DRAKEN_INT8: case DRAKEN_UINT8: return 4;
  case DRAKEN_INT16: case DRAKEN_UINT16: return 6;
  case DRAKEN_INT32: case DRAKEN_UINT32: return 11;
  case DRAKEN_INT64: case DRAKEN_UINT64: return 14;
  case DRAKEN_FLOAT32: case DRAKEN_FLOAT64: return 18;
  case DRAKEN_BOOL: return 5;
  case DRAKEN_DATE32: return 12;
  case DRAKEN_TIMESTAMP64: return 34;
  case DRAKEN_TIME32: case DRAKEN_TIME64: return 17;
  case DRAKEN_DECIMAL: case DRAKEN_DECIMAL128: return 22;
  case DRAKEN_VECTOR_FP16: return 1 + (size_t)c.desc.column.dim * 12;
  case DRAKEN_VARCHAR: case DRAKEN_NVARCHAR: case DRAKEN_VARBINARY: case DRAKEN_VARIANT: {
    const DrakenStringArena *sa = (const DrakenStringArena *)dv->data;
    size_t nvals = sa && sa->length ? sa->length : 1;
    return (sa ? sa->arena_used / nvals : 0) + 14; // + inline payload + quotes
  }
  default: return 16; // ARRAY and anything else: no cheap width signal
  }
}

// Render logical rows [r0, r1) of the resolved columns into `out`. `prefixes`
// are the baked per-column field prefixes ('{' or ',' + escaped name + ':'),
// so each field costs one append for its framing, not a push_back + append.
static inline void jsonl_render_rows(std::vector<Col> &cols, const std::string *prefixes,
                                     size_t ncols, size_t r0, size_t r1,
                                     size_t est_row_bytes, std::string &out) {
  out.reserve((r1 - r0) * est_row_bytes);
  if (ncols == 0) {
    for (size_t i = r0; i < r1; i++) out.append("{}\n", 3);
    return;
  }
  for (size_t i = r0; i < r1; i++) {
    for (size_t c = 0; c < ncols; c++) {
      out.append(prefixes[c]);
      cols[c].emit(out, cols[c], i);
    }
    out.append("}\n", 2);
  }
}

// Shared render pool, sized once to the core count (capped). Reused across every
// write so a large multi-morsel export doesn't spawn a fresh set of threads per
// morsel. Sound because the writer runs one export at a time per process (Cloud
// Run serves a single request per instance); submit_task is mutex-guarded, so
// even an accidental concurrent call stays correct, just capped at this width.
static constexpr size_t kJsonlMaxThreads = 8;
static inline size_t jsonl_pool_width() {
  size_t hw = std::thread::hardware_concurrency();
  if (hw == 0) hw = 1;
  if (hw > kJsonlMaxThreads) hw = kJsonlMaxThreads;
  return hw;
}
static inline BS::thread_pool<> &jsonl_render_pool() {
  static BS::thread_pool<> pool(jsonl_pool_width()); // magic-static: init once
  return pool;
}

// Serialize a morsel to JSONL, returning the rendered bytes as one buffer per
// worker (the caller concatenates straight into a Python `bytes`, so the whole
// output is never materialized in a std::string first). Rows are partitioned
// into contiguous ranges rendered in parallel on the shared pool; per-thread Col
// copies give each worker its own array-staging scratch. Small inputs render
// inline on the calling thread.
inline std::vector<std::string> jsonl_write(const DrakenVector **dvs,
                                            const ColumnDesc *descs,
                                            const std::string *prefixes, size_t ncols, size_t nrows) {
  std::vector<Col> cols(ncols);
  // Bake the field framing into the prefixes once: first column opens the row
  // ('{'), the rest lead with ','. The row loop then has no per-field branch.
  std::vector<std::string> baked(ncols);
  size_t est = 2; // "}\n"
  for (size_t c = 0; c < ncols; c++) {
    resolve_col(cols[c], dvs[c], descs[c], nullptr, 0, false, false);
    baked[c].reserve(prefixes[c].size() + 1);
    baked[c].push_back(c == 0 ? '{' : ',');
    baked[c].append(prefixes[c]);
    est += baked[c].size() + est_cell_bytes(cols[c]);
  }

  // One partition per ~MIN_ROWS rows, capped at the pool width.
  const size_t MIN_ROWS = 16384;
  size_t nt = nrows / MIN_ROWS;
  if (nt < 1) nt = 1;
  if (nt > jsonl_pool_width()) nt = jsonl_pool_width();

  std::vector<std::string> chunks(nt);
  if (nt <= 1) {
    jsonl_render_rows(cols, baked.data(), ncols, 0, nrows, est, chunks[0]);
  } else {
    BS::thread_pool<> &pool = jsonl_render_pool();
    std::vector<std::future<void>> futs;
    futs.reserve(nt);
    size_t per = (nrows + nt - 1) / nt;
    for (size_t t = 0; t < nt; t++) {
      size_t r0 = t * per;
      size_t r1 = std::min(nrows, r0 + per);
      if (r0 >= r1) break;
      futs.push_back(pool.submit_task([&, t, r0, r1]() {
        // Shallow copy: shares the read-only cast arenas but owns an independent
        // `scratch`. Never freed here — free_cols runs once on `cols` below.
        std::vector<Col> local = cols;
        jsonl_render_rows(local, baked.data(), ncols, r0, r1, est, chunks[t]);
      }));
    }
    for (auto &f : futs) f.get();
  }
  free_cols(cols);
  return chunks;
}

inline std::string csv_write(const DrakenVector **dvs,
                             const ColumnDesc *descs,
                             const std::string *names, size_t ncols, size_t nrows,
                             char delim, bool header, bool for_excel) {
  // Shape checks first: refuse before rendering a file that cannot be opened
  // whole, rather than after building it.
  if (for_excel) {
    size_t lines = nrows + (header ? 1 : 0);
    if (lines > kExcelMaxRows)
      throw std::invalid_argument(
          "write_csv(for_excel=True): " + std::to_string(lines) +
          " lines (" + std::to_string(nrows) + " rows" +
          (header ? " + header" : "") + "); an Excel sheet holds " +
          std::to_string(kExcelMaxRows) + ".");
    if (ncols > kExcelMaxCols)
      throw std::invalid_argument(
          "write_csv(for_excel=True): " + std::to_string(ncols) +
          " columns; an Excel sheet holds " + std::to_string(kExcelMaxCols) + ".");
    if (header)
      for (size_t c = 0; c < ncols; c++)
        if (names[c].size() > kExcelMaxCellChars) {
          size_t chars = utf16_length(names[c].data(), names[c].size());
          if (chars > kExcelMaxCellChars)
            excel_too_wide("the name of column " + std::to_string(c), chars);
        }
  }
  std::vector<Col> cols(ncols);
  // Bake the field separator into a per-column prefix (empty for column 0,
  // one delim byte otherwise) — same trick as jsonl_write's '{'/',' prefixes:
  // the row loop then has no per-field branch, just an append + the emitter.
  std::vector<char> seps(ncols, delim);
  if (ncols) seps[0] = 0; // sentinel: column 0 has no separator
  size_t namesum = 0, est = ncols + 4; // + one '\n' worth of slack
  for (size_t c = 0; c < ncols; c++) {
    resolve_col(cols[c], dvs[c], descs[c], &names[c], delim, true, for_excel);
    namesum += names[c].size();
    est += est_cell_bytes(cols[c]);
  }
  std::string out;
  out.reserve(nrows * est + namesum + ncols + 4);
  if (header) {
    for (size_t c = 0; c < ncols; c++) { if (seps[c]) out.push_back(seps[c]); csv_field(out, names[c].data(), names[c].size(), delim); }
    out.push_back('\n');
  }
  // for_excel's cell-width check throws from inside an emitter; the cast blocks
  // held by `cols` are ours to release on the way out.
  try {
    for (size_t i = 0; i < nrows; i++) {
      for (size_t c = 0; c < ncols; c++) { if (seps[c]) out.push_back(seps[c]); cols[c].emit(out, cols[c], i); }
      out.push_back('\n');
    }
  } catch (...) {
    free_cols(cols);
    throw;
  }
  free_cols(cols);
  return out;
}

} // namespace rugo_text
