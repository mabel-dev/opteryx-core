#pragma once
// page_index_writer.hpp — writing the parquet PageIndex (ColumnIndex +
// OffsetIndex). The inverse of page_index.{hpp,cpp}, which parses and evaluates
// them on the read side.
//
// Deliberately depends on nothing but the Compact Protocol writer: no column
// model, no compression, no draken. That keeps page_index_test.cpp able to
// round-trip real writer output straight through the reader's parser, and keeps
// the two sides of the format in one small pair of files.
//
// The ordering a bound is compared under is passed in as a PageBoundKind rather
// than a parquet physical type — the caller owns the type system and maps into
// this (see page_index_bound_kind in _parquet_writer.hpp).

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <vector>

#include "_thrift_writer.hpp"

namespace rugo_pq_write {

// How a page bound is ordered. Byte-wise (PB_BYTES) covers BYTE_ARRAY and
// BOOLEAN; the integer kinds order in the column's DECLARED domain, which is
// what `is_unsigned` selects.
enum PageBoundKind : uint8_t {
  PB_INT32,
  PB_INT64,
  PB_FLOAT,
  PB_DOUBLE,
  PB_BYTES,
};

// ---- per-page index metadata (PageIndex writing) ----
//
// One entry per DATA page of a column chunk, in file order (dictionary pages
// are never listed — parquet.thrift OffsetIndex covers data pages only). Filled
// during page building, which is the only place that knows where a page
// boundary fell, and finalised with an ABSOLUTE file offset by
// write_row_group_chunks once the compressed-vs-plain variant of the chunk has
// been chosen: the two variants give the same page different sizes, so the
// offset cannot be known any earlier.
//
// Populated only when the caller asked for a page index (page_index=true AND
// max_page_bytes > 0). Per-page bounds cost a second stats pass over the
// column, so nothing pays for them when nothing will consume them.
struct PageMeta {
  size_t  stored_size = 0;      // page bytes (header + payload), compressed variant
  size_t  plain_size  = 0;      // the same page in the uncompressed variant
  int64_t file_offset = -1;     // absolute; finalised by write_row_group_chunks
  int64_t size        = 0;      // kept-variant size; finalised alongside file_offset
  int64_t first_row_index = 0;  // row index of the page's first row IN THE ROW GROUP
  int64_t null_count  = 0;
  bool    null_page   = false;  // every row of the page is NULL
  bool    has_bounds  = false;
  std::vector<uint8_t> min_bytes;
  std::vector<uint8_t> max_bytes;
};

// ---- PageIndex serialization (ColumnIndex + OffsetIndex) ----
//
// Layout contract (parquet spec, and what rugo's own reader assumes in
// ParquetIOPipeline::compute_page_prune): ALL ColumnIndex structs of the file
// first, then ALL OffsetIndex structs, in the tail AFTER every column chunk and
// BEFORE the footer. The reader fetches [min(all index offsets),
// max(all index ends)) as ONE range, so anything interleaved between them would
// be fetched too.

// BYTE_ARRAY page bounds are truncated at this length. A page min is replaced
// by its 64-byte prefix (<= the real min) and a page max by its 64-byte prefix
// incremented (>= the real max); both widen the interval, which every bounds
// test is monotone in, so pruning stays sound. Ints and fixed-width types are
// never truncated — they are already at or under this length.
constexpr size_t kPageIndexBoundTruncate = 64;

inline std::vector<uint8_t> truncate_min_bound(const std::vector<uint8_t> &v) {
  if (v.size() <= kPageIndexBoundTruncate) return v;
  return std::vector<uint8_t>(v.begin(), v.begin() + kPageIndexBoundTruncate);
}

// Shortened-and-incremented max: the shortest prefix-derived value that is >=
// every value the page's real max could be. Trailing 0xFF bytes cannot carry an
// increment, so they are dropped first; a prefix that is ALL 0xFF has no
// representable successor at this length and the bound is dropped entirely
// (empty = "no bound", which every reader treats as "keep the page").
inline std::vector<uint8_t> truncate_max_bound(const std::vector<uint8_t> &v) {
  if (v.size() <= kPageIndexBoundTruncate) return v;
  std::vector<uint8_t> out(v.begin(), v.begin() + kPageIndexBoundTruncate);
  while (!out.empty() && out.back() == 0xFF) out.pop_back();
  if (out.empty()) return out; // all 0xFF: no bound is representable
  out.back() = (uint8_t)(out.back() + 1);
  return out;
}

// Order two PLAIN-encoded bounds of `type` in the column's DECLARED domain —
// the same ordering compute_stats used to pick them. Returns <0, 0, >0.
// Types with no lexicographic-safe ordering here (FLBA decimals are big-endian
// two's complement, which does not sort byte-wise across zero) are rejected via
// page_index_orderable below rather than mis-ordered.
inline int compare_bound_bytes(PageBoundKind kind, bool is_unsigned,
                               const std::vector<uint8_t> &a,
                               const std::vector<uint8_t> &b) {
  switch (kind) {
  case PB_INT32: {
    if (a.size() != 4 || b.size() != 4) return 0;
    int32_t x, y;
    std::memcpy(&x, a.data(), 4);
    std::memcpy(&y, b.data(), 4);
    if (is_unsigned) {
      uint32_t ux = (uint32_t)x, uy = (uint32_t)y;
      return ux < uy ? -1 : (ux > uy ? 1 : 0);
    }
    return x < y ? -1 : (x > y ? 1 : 0);
  }
  case PB_INT64: {
    if (a.size() != 8 || b.size() != 8) return 0;
    int64_t x, y;
    std::memcpy(&x, a.data(), 8);
    std::memcpy(&y, b.data(), 8);
    if (is_unsigned) {
      uint64_t ux = (uint64_t)x, uy = (uint64_t)y;
      return ux < uy ? -1 : (ux > uy ? 1 : 0);
    }
    return x < y ? -1 : (x > y ? 1 : 0);
  }
  case PB_FLOAT: {
    if (a.size() != 4 || b.size() != 4) return 0;
    float x, y;
    std::memcpy(&x, a.data(), 4);
    std::memcpy(&y, b.data(), 4);
    return x < y ? -1 : (x > y ? 1 : 0);
  }
  case PB_DOUBLE: {
    if (a.size() != 8 || b.size() != 8) return 0;
    double x, y;
    std::memcpy(&x, a.data(), 8);
    std::memcpy(&y, b.data(), 8);
    return x < y ? -1 : (x > y ? 1 : 0);
  }
  default: { // BYTE_ARRAY / BOOLEAN: unsigned byte-wise, shorter-is-smaller
    const size_t n = std::min(a.size(), b.size());
    const int c = (n == 0) ? 0 : std::memcmp(a.data(), b.data(), n);
    if (c != 0) return c;
    if (a.size() == b.size()) return 0;
    return a.size() < b.size() ? -1 : 1;
  }
  }
}

// parquet.thrift BoundaryOrder: 0 UNORDERED, 1 ASCENDING, 2 DESCENDING.
// Computed from the bounds AS WRITTEN (post-truncation, which is order
// preserving). Null pages carry no bounds and are skipped — a run of them
// between two ordered pages does not break the order.
inline int32_t compute_boundary_order(const std::vector<PageMeta> &pages,
                                      const std::vector<std::vector<uint8_t>> &mins,
                                      const std::vector<std::vector<uint8_t>> &maxs,
                                      PageBoundKind kind, bool is_unsigned) {
  bool asc = true, desc = true;
  const PageMeta *prev = nullptr;
  size_t prev_i = 0;
  for (size_t i = 0; i < pages.size(); i++) {
    if (pages[i].null_page || mins[i].empty() || maxs[i].empty()) continue;
    if (prev != nullptr) {
      if (compare_bound_bytes(kind, is_unsigned, mins[prev_i], mins[i]) > 0 ||
          compare_bound_bytes(kind, is_unsigned, maxs[prev_i], maxs[i]) > 0)
        asc = false;
      if (compare_bound_bytes(kind, is_unsigned, mins[prev_i], mins[i]) < 0 ||
          compare_bound_bytes(kind, is_unsigned, maxs[prev_i], maxs[i]) < 0)
        desc = false;
    }
    prev = &pages[i];
    prev_i = i;
    if (!asc && !desc) return 0;
  }
  if (asc) return 1;
  if (desc) return 2;
  return 0;
}

// ColumnIndex { 1: required list<bool> null_pages, 2: required list<binary>
// min_values, 3: required list<binary> max_values, 4: required BoundaryOrder
// boundary_order, 5: optional list<i64> null_counts }
//
// Returns an empty buffer when no sound index can be written: a column with no
// orderable bounds, or a page that is neither all-NULL nor bounded (the spec
// has no spelling for "this page has values but I did not record them", and the
// three required lists must stay parallel).
inline std::vector<uint8_t> serialize_column_index(const std::vector<PageMeta> &pages,
                                                   PageBoundKind kind, bool is_unsigned) {
  if (pages.empty()) return {};

  const size_t n = pages.size();
  std::vector<std::vector<uint8_t>> mins(n), maxs(n);
  for (size_t i = 0; i < n; i++) {
    if (pages[i].null_page) continue; // required to be empty
    if (!pages[i].has_bounds) return {};
    if (kind == PB_BYTES) {
      mins[i] = truncate_min_bound(pages[i].min_bytes);
      maxs[i] = truncate_max_bound(pages[i].max_bytes);
    } else {
      mins[i] = pages[i].min_bytes;
      maxs[i] = pages[i].max_bytes;
    }
  }

  TCompactWriter w;
  w.structBegin();
  w.writeFieldHeader(CT_LIST, 1); // null_pages
  w.writeListHeader(CT_BOOL_TRUE, (uint32_t)n);
  for (const PageMeta &pm : pages) w.writeListBoolElem(pm.null_page);
  w.writeFieldHeader(CT_LIST, 2); // min_values
  w.writeListHeader(CT_BINARY, (uint32_t)n);
  for (const auto &v : mins) w.writeListBinary(v.data(), v.size());
  w.writeFieldHeader(CT_LIST, 3); // max_values
  w.writeListHeader(CT_BINARY, (uint32_t)n);
  for (const auto &v : maxs) w.writeListBinary(v.data(), v.size());
  w.writeI32Field(4, compute_boundary_order(pages, mins, maxs, kind, is_unsigned));
  w.writeFieldHeader(CT_LIST, 5); // null_counts
  w.writeListHeader(CT_I64, (uint32_t)n);
  for (const PageMeta &pm : pages) w.writeListI64(pm.null_count);
  w.structEnd();
  return w.buf;
}

// OffsetIndex { 1: required list<PageLocation> page_locations }
// PageLocation { 1: required i64 offset, 2: required i32 compressed_page_size,
//                3: required i64 first_row_index }
inline std::vector<uint8_t> serialize_offset_index(const std::vector<PageMeta> &pages) {
  if (pages.empty()) return {};
  TCompactWriter w;
  w.structBegin();
  w.writeFieldHeader(CT_LIST, 1);
  w.writeListHeader(CT_STRUCT, (uint32_t)pages.size());
  for (const PageMeta &pm : pages) {
    w.structBegin();
    w.writeI64Field(1, pm.file_offset);
    w.writeI32Field(2, (int32_t)pm.size);
    w.writeI64Field(3, pm.first_row_index);
    w.structEnd();
  }
  w.structEnd();
  return w.buf;
}

}  // namespace rugo_pq_write
