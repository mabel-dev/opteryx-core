#pragma once
// Fast Morsel -> CSV / JSONL rendering.
//
// Per-column dispatch is resolved ONCE (into a function pointer), so the row
// loop has no per-cell type switch. int / bool / date / timestamp columns are
// rendered with draken's batch, compression-aware cast-to-string kernels
// (low-cardinality columns format each unique value once). float uses
// std::to_chars (shortest round-trip — the cast kernel is 6-dp display only),
// decimal/time use the dedicated formatters, strings/arrays render directly.

#include "interop/value_format.hpp"  // moved into draken; resolved via -I draken

#include "core/alloc.h"          // draken_free
#include "ops/vec_result.h"      // VecResult
#include "ops/kernels/cast_kernels.h"

#include "BS_thread_pool.hpp"    // vendored bshoshany pool (same as the readers)

#include <algorithm>
#include <future>
#include <thread>
#include <vector>

namespace rugo_text {

typedef void (*EmitFn)(std::string &, struct Col &, size_t);

struct Col {
  const DrakenVector *dv;    // original column (validity + per-cell formatters)
  const DrakenVector *child; // ARRAY element vector
  DrakenVector sv;           // string source (cast result OR original string col)
  int unit, scale, cunit, cscale;
  char delim;
  EmitFn emit;               // resolved once per column
  void *free_data;           // cast result block to draken_free (else null)
  const uint32_t *free_sel;  // owned cast selection to draken_free (else null)
  std::string scratch;       // reused per-row JSON staging buffer (ec_array only)
};

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
  if (sv_cell(c.sv, i, p, n)) { o.push_back('"'); o.append(p, n); o.push_back('"'); }
  else o.append("null");
}
static void ej_string(std::string &o, Col &c, size_t i) {
  const char *p; uint32_t n;
  if (sv_cell(c.sv, i, p, n)) json_string(o, p, n); else o.append("null");
}
static void ej_float(std::string &o, Col &c, size_t i) {
  if (!row_valid(c.dv->validity, i)) { o.append("null"); return; }
  uint32_t p = c.dv->selection[i];
  double v = c.dv->type == DRAKEN_FLOAT64 ? ((const double *)c.dv->data)[p]
                                          : ((const float *)c.dv->data)[p];
  if (double_is_nan_or_inf(v)) o.append("null"); else fmt_double(o, v);
}
static void ej_decimal(std::string &o, Col &c, size_t i) {
  if (!row_valid(c.dv->validity, i)) { o.append("null"); return; }
  uint32_t p = c.dv->selection[i];
  if (c.dv->type == DRAKEN_DECIMAL)
    fmt_decimal(o, (__int128)((const int64_t *)c.dv->data)[p], c.scale);
  else { __int128 v; std::memcpy(&v, (const uint8_t *)c.dv->data + (size_t)p * 16, 16); fmt_decimal(o, v, c.scale); }
}
static void ej_time(std::string &o, Col &c, size_t i) {
  if (!row_valid(c.dv->validity, i)) { o.append("null"); return; }
  uint32_t p = c.dv->selection[i];
  o.push_back('"');
  if (c.dv->type == DRAKEN_TIME64) fmt_time(o, ((const int64_t *)c.dv->data)[p], c.unit);
  else fmt_time(o, ((const int32_t *)c.dv->data)[p], c.unit);
  o.push_back('"');
}
static void ej_timestamp(std::string &o, Col &c, size_t i) {
  if (!row_valid(c.dv->validity, i)) { o.append("null"); return; }
  o.push_back('"');
  fmt_timestamp(o, ((const int64_t *)c.dv->data)[c.dv->selection[i]], c.unit);
  o.push_back('"');
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
static void ej_bool(std::string &o, Col &c, size_t i) {
  if (!row_valid(c.dv->validity, i)) { o.append("null"); return; }
  uint32_t p = c.dv->selection[i];
  o.append((((const uint8_t *)c.dv->data)[p >> 3] >> (p & 7)) & 1 ? "true" : "false");
}
static void ej_date(std::string &o, Col &c, size_t i) {
  if (!row_valid(c.dv->validity, i)) { o.append("null"); return; }
  o.push_back('"'); fmt_date(o, ((const int32_t *)c.dv->data)[c.dv->selection[i]]); o.push_back('"');
}
static void ej_array(std::string &o, Col &c, size_t i) {
  if (!row_valid(c.dv->validity, i)) { o.append("null"); return; }
  const int32_t *offs = (const int32_t *)c.dv->data;
  uint32_t p = c.dv->selection[i];
  int32_t s = offs[p], e = offs[p + 1];
  o.push_back('[');
  for (int32_t k = s; k < e; k++) { if (k > s) o.push_back(','); render_json_scalar(o, c.child, (size_t)k, c.cunit, c.cscale); }
  o.push_back(']');
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
  double v = c.dv->type == DRAKEN_FLOAT64 ? ((const double *)c.dv->data)[p]
                                          : ((const float *)c.dv->data)[p];
  if (!double_is_nan_or_inf(v)) fmt_double(o, v); // NaN/Infinity -> empty CSV field
}
static void ec_decimal(std::string &o, Col &c, size_t i) {
  if (!row_valid(c.dv->validity, i)) return;
  uint32_t p = c.dv->selection[i];
  if (c.dv->type == DRAKEN_DECIMAL)
    fmt_decimal(o, (__int128)((const int64_t *)c.dv->data)[p], c.scale);
  else { __int128 v; std::memcpy(&v, (const uint8_t *)c.dv->data + (size_t)p * 16, 16); fmt_decimal(o, v, c.scale); }
}
static void ec_time(std::string &o, Col &c, size_t i) {
  if (!row_valid(c.dv->validity, i)) return;
  uint32_t p = c.dv->selection[i];
  if (c.dv->type == DRAKEN_TIME64) fmt_time(o, ((const int64_t *)c.dv->data)[p], c.unit);
  else fmt_time(o, ((const int32_t *)c.dv->data)[p], c.unit);
}
static void ec_timestamp(std::string &o, Col &c, size_t i) {
  if (!row_valid(c.dv->validity, i)) return;
  fmt_timestamp(o, ((const int64_t *)c.dv->data)[c.dv->selection[i]], c.unit);
}
static void ec_array(std::string &o, Col &c, size_t i) {
  if (!row_valid(c.dv->validity, i)) return;
  c.scratch.clear(); ej_array(c.scratch, c, i); csv_field(o, c.scratch.data(), c.scratch.size(), c.delim);
}
static void ec_null(std::string &o, Col &c, size_t i) { (void)o; (void)c; (void)i; }

// Direct CSV emitters (dense columns) — null -> empty field. See ej_int/bool/date.
template <typename T>
static void ec_int(std::string &o, Col &c, size_t i) {
  if (!row_valid(c.dv->validity, i)) return;
  fmt_int64(o, (int64_t)((const T *)c.dv->data)[c.dv->selection[i]]);
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

// Pick the width-specialized integer emitter so the row loop stays branch-free.
static inline EmitFn pick_int_emitter(DrakenType t, bool csv) {
  switch (t) {
  case DRAKEN_INT8:  return csv ? ec_int<int8_t>  : ej_int<int8_t>;
  case DRAKEN_INT16: return csv ? ec_int<int16_t> : ej_int<int16_t>;
  case DRAKEN_INT32: return csv ? ec_int<int32_t> : ej_int<int32_t>;
  default:           return csv ? ec_int<int64_t> : ej_int<int64_t>;
  }
}

static inline void vr_to_dv(const VecResult &vr, DrakenVector &dv) {
  dv.data = vr.data; dv.selection = vr.selection; dv.validity = vr.validity;
  dv.data_length = vr.data_length; dv.length = vr.length; dv.type = vr.type;
  dv.flags = vr.flags;
}

// Resolve one column: choose the cell emitter, and (for int/bool/date/ts) run
// the batch cast kernel to produce the string source.
static void resolve_col(Col &c, const DrakenVector *dv, const DrakenVector *child,
                        int unit, int scale, int cunit, int cscale, char delim,
                        bool csv) {
  c.dv = dv; c.child = child; c.unit = unit; c.scale = scale;
  c.cunit = cunit; c.cscale = cscale; c.delim = delim;
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
    c.sv = *dv; c.emit = csv ? ec_string : ej_string; return;
  case DRAKEN_ARRAY:
    c.emit = csv ? ec_array : ej_array; return;
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

// Render logical rows [r0, r1) of the resolved columns into `out`.
static inline void jsonl_render_rows(std::vector<Col> &cols, const std::string *prefixes,
                                     size_t ncols, size_t r0, size_t r1,
                                     size_t est_row_bytes, std::string &out) {
  out.reserve((r1 - r0) * est_row_bytes);
  for (size_t i = r0; i < r1; i++) {
    out.push_back('{');
    for (size_t c = 0; c < ncols; c++) {
      if (c) out.push_back(',');
      out.append(prefixes[c]);
      cols[c].emit(out, cols[c], i);
    }
    out.append("}\n");
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
inline std::vector<std::string> jsonl_write(const DrakenVector **dvs, const DrakenVector **childs,
                                            const int *units, const int *scales,
                                            const int *cunits, const int *cscales,
                                            const std::string *prefixes, size_t ncols, size_t nrows) {
  std::vector<Col> cols(ncols);
  size_t prefsum = 0;
  for (size_t c = 0; c < ncols; c++) {
    resolve_col(cols[c], dvs[c], childs[c], units[c], scales[c], cunits[c], cscales[c], 0, false);
    prefsum += prefixes[c].size();
  }
  size_t est = prefsum + ncols + 2 + ncols * 8; // reserve heuristic (per row)

  // One partition per ~MIN_ROWS rows, capped at the pool width.
  const size_t MIN_ROWS = 16384;
  size_t nt = nrows / MIN_ROWS;
  if (nt < 1) nt = 1;
  if (nt > jsonl_pool_width()) nt = jsonl_pool_width();

  std::vector<std::string> chunks(nt);
  if (nt <= 1) {
    jsonl_render_rows(cols, prefixes, ncols, 0, nrows, est, chunks[0]);
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
        jsonl_render_rows(local, prefixes, ncols, r0, r1, est, chunks[t]);
      }));
    }
    for (auto &f : futs) f.get();
  }
  free_cols(cols);
  return chunks;
}

inline std::string csv_write(const DrakenVector **dvs, const DrakenVector **childs,
                             const int *units, const int *scales,
                             const int *cunits, const int *cscales,
                             const std::string *names, size_t ncols, size_t nrows,
                             char delim, bool header) {
  std::vector<Col> cols(ncols);
  size_t namesum = 0;
  for (size_t c = 0; c < ncols; c++) {
    resolve_col(cols[c], dvs[c], childs[c], units[c], scales[c], cunits[c], cscales[c], delim, true);
    namesum += names[c].size();
  }
  std::string out;
  out.reserve(nrows * (ncols * 10) + namesum + ncols + 4);
  if (header) {
    for (size_t c = 0; c < ncols; c++) { if (c) out.push_back(delim); csv_field(out, names[c].data(), names[c].size(), delim); }
    out.push_back('\n');
  }
  for (size_t i = 0; i < nrows; i++) {
    for (size_t c = 0; c < ncols; c++) { if (c) out.push_back(delim); cols[c].emit(out, cols[c], i); }
    out.push_back('\n');
  }
  free_cols(cols);
  return out;
}

} // namespace rugo_text
