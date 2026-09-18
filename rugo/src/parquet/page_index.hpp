#pragma once
// page_index.hpp — Parquet PageIndex (ColumnIndex + OffsetIndex) support.
//
// The footer records, per column chunk, where its ColumnIndex (per-page
// min/max/null_count) and OffsetIndex (per-page byte offset + size + first row)
// live (ColumnStats::column_index_offset / offset_index_offset). Both structs sit
// in the file tail, after every column chunk and before the footer — a writer
// that emits them lays out ALL ColumnIndex structs of the file first, then ALL
// OffsetIndex structs, so one range read per file covers every row group's.
// rugo's own writer is page_index_writer.hpp, which holds to that layout; the
// round-trip between the two is pinned in page_index_test.cpp.
//
// This header owns two things:
//   1. parsing the two Thrift structs (parquet.thrift `ColumnIndex`, `OffsetIndex`);
//   2. deciding, per page, whether a pushed per-value predicate can match ANY row
//      of that page — the same conjunct predicates the dictionary decode-skip
//      consults (DictSkipPredicate kinds), evaluated against page bounds instead
//      of dictionary values.
//
// The evaluation is a BOUNDS test and is sound under truncated bounds: the spec
// lets a writer shorten a BYTE_ARRAY min (a prefix, so <= the real min) and
// shorten-and-increment a max (so >= the real max); every test below only ever
// asks "can a value in [min, max] satisfy the predicate", which is monotone in
// widening the interval. A page the writer marks null_pages[p] holds only NULLs
// and matches no per-value predicate (=, IN, LIKE all reject NULL).
//
// What this file does NOT do: fetch bytes, build row masks, or touch the decoder.
// ParquetIOPipeline (io_pipeline.hpp) does those with the results.

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

// parquet.thrift PageLocation. `offset` is ABSOLUTE in the file and points at the
// page HEADER; `compressed_page_size` counts header + payload; `first_row_index`
// is the row (not value) index of the page's first row within the row group.
struct PageLocation {
  int64_t offset = -1;
  int32_t compressed_page_size = 0;
  int64_t first_row_index = -1;
};

// parquet.thrift OffsetIndex: one PageLocation per DATA page, in file order.
// Dictionary pages are never listed.
struct OffsetIndexData {
  std::vector<PageLocation> page_locations;
};

// parquet.thrift ColumnIndex. Every vector is per DATA page, parallel to the
// OffsetIndex's page_locations. min_values/max_values are the PLAIN-encoded
// bytes of the page's bounds (little-endian for int32/int64, raw for byte_array)
// and are EMPTY/meaningless for a page whose null_pages entry is true.
// null_counts is optional in the spec — empty when the writer omitted it.
struct ColumnIndexData {
  std::vector<uint8_t>     null_pages;   // 1 = the page is all-NULL
  std::vector<std::string> min_values;
  std::vector<std::string> max_values;
  int32_t                  boundary_order = -1;  // 0 UNORDERED, 1 ASC, 2 DESC
  std::vector<int64_t>     null_counts;          // optional; empty when absent
};

// Parse one struct from exactly its footer-recorded byte range. Both throw
// std::runtime_error on a malformed encoding (EOF / bad list element type) —
// a corrupt index is a corrupt file, not a reason to guess.
ColumnIndexData ParseColumnIndex(const uint8_t *data, size_t size);
OffsetIndexData ParseOffsetIndex(const uint8_t *data, size_t size);

// Per-page predicate test. Fills `keep` (size = num_pages; 1 = the page may hold
// a matching row, 0 = it provably holds none) and returns the number of pages
// marked 0. `kind` and the value lists follow DictSkipPredicate (decode.hpp):
//   0 int membership (=/IN)   int_vals, column physical int32/int64
//   1 str membership (=/IN)   str_vals, column physical byte_array
//   2 str starts-with         str_vals (one or more prefixes; ANY may match)
//   3 str ends-with / 4 str contains: no bounds test exists — only null pages prune
// `physical_type` is ColumnStats::physical_type; `is_unsigned` the E33 IntType
// verdict (StatsLogicalIsUnsigned). A kind/type combination with no sound test
// keeps every non-null page. A ColumnIndex whose min/max lists are not exactly
// num_pages long is treated as carrying no bounds (null_pages alone still
// applies when IT is sized right).
size_t EvaluatePagePredicate(const ColumnIndexData &ci, size_t num_pages,
                             int kind,
                             const std::vector<int64_t> *int_vals,
                             const std::vector<std::string> *str_vals,
                             const std::string &physical_type,
                             bool is_unsigned,
                             std::vector<uint8_t> &keep);
