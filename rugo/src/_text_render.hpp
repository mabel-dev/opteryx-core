#pragma once
// Fast Morsel -> CSV / JSONL rendering.
//
// Per-column dispatch is resolved ONCE (into a function pointer), so the row
// loop has no per-cell type switch. int / bool / date / timestamp columns are
// rendered with draken's batch, compression-aware cast-to-string kernels
// (low-cardinality columns format each unique value once). float uses
// std::to_chars (shortest round-trip — the cast kernel is 6-dp display only),
// decimal/time use the dedicated formatters, strings/arrays render directly.

#include "_value_format.hpp"

#include "core/alloc.h"          // draken_free
#include "ops/vec_result.h"      // VecResult
#include "ops/kernels/cast_kernels.h"

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
  if (c.dv->type == DRAKEN_FLOAT64) fmt_double(o, ((const double *)c.dv->data)[p]);
  else fmt_double(o, ((const float *)c.dv->data)[p]);
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
  if (c.dv->type == DRAKEN_FLOAT64) fmt_double(o, ((const double *)c.dv->data)[p]);
  else fmt_double(o, ((const float *)c.dv->data)[p]);
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
  switch (dv->type) {
  case DRAKEN_INT8: case DRAKEN_INT16: case DRAKEN_INT32: case DRAKEN_INT64:
    vr = draken_cast_integer_to_string(nullptr, dv); break;
  case DRAKEN_BOOL:
    vr = draken_cast_bool_to_string(nullptr, dv); break;
  case DRAKEN_DATE32:
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

inline std::string jsonl_write(const DrakenVector **dvs, const DrakenVector **childs,
                               const int *units, const int *scales,
                               const int *cunits, const int *cscales,
                               const std::string *prefixes, size_t ncols, size_t nrows) {
  std::vector<Col> cols(ncols);
  size_t prefsum = 0;
  for (size_t c = 0; c < ncols; c++) {
    resolve_col(cols[c], dvs[c], childs[c], units[c], scales[c], cunits[c], cscales[c], 0, false);
    prefsum += prefixes[c].size();
  }
  std::string out;
  out.reserve(nrows * (prefsum + ncols + 2 + ncols * 8));
  for (size_t i = 0; i < nrows; i++) {
    out.push_back('{');
    for (size_t c = 0; c < ncols; c++) {
      if (c) out.push_back(',');
      out.append(prefixes[c]);
      cols[c].emit(out, cols[c], i);
    }
    out.append("}\n");
  }
  free_cols(cols);
  return out;
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
