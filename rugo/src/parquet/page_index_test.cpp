// page_index_test.cpp — standalone driver for the PageIndex, both directions:
// the Thrift parse of ColumnIndex / OffsetIndex and the per-page predicate test
// (page_index.{hpp,cpp}), and the serialization of both structs
// (page_index_writer.hpp).
//
// Built and run by `make page-index-test` (tests/rugo/test_page_index.py wraps
// it for the suite). The reader sections serialize their fixtures here with the
// writer's own TCompactWriter (_thrift_writer.hpp), so the bytes under test are
// exactly what a Compact Protocol encoder emits — not a hand-typed byte string.
// The round-trip sections at the end go one better and feed the parser bytes
// the PRODUCTION writer produced, truncated bounds included.

#include "page_index.hpp"
#include "_thrift_writer.hpp"
#include "page_index_writer.hpp"

#include <cstdio>
#include <cstring>
#include <functional>
#include <stdexcept>
#include <string>
#include <vector>

static int g_failures = 0;
static void section(const char* name) { std::printf("%s\n", name); }
static void check(bool ok, const char* what) {
  std::printf("  %-58s %s\n", what, ok ? "ok" : "FAIL");
  if (!ok) ++g_failures;
}

using rugo_pq_write::TCompactWriter;
using rugo_pq_write::CT_BINARY;
using rugo_pq_write::CT_BOOL_TRUE;
using rugo_pq_write::CT_I64;
using rugo_pq_write::CT_LIST;
using rugo_pq_write::CT_STRUCT;

static std::string le32(int32_t v) { return std::string(reinterpret_cast<const char*>(&v), 4); }
static std::string le64(int64_t v) { return std::string(reinterpret_cast<const char*>(&v), 8); }

// ColumnIndex { 1: list<bool> null_pages, 2: list<binary> min, 3: list<binary> max,
//               4: i32 boundary_order, 5: list<i64> null_counts }
static std::vector<uint8_t> write_column_index(const std::vector<bool>& nulls,
                                               const std::vector<std::string>& mins,
                                               const std::vector<std::string>& maxs,
                                               int32_t boundary_order,
                                               const std::vector<int64_t>* null_counts) {
  TCompactWriter w;
  w.structBegin();
  w.writeFieldHeader(CT_LIST, 1);
  w.writeListHeader(CT_BOOL_TRUE, static_cast<uint32_t>(nulls.size()));
  for (bool b : nulls) w.writeListBoolElem(b);
  w.writeFieldHeader(CT_LIST, 2);
  w.writeListHeader(CT_BINARY, static_cast<uint32_t>(mins.size()));
  for (const auto& m : mins) w.writeListString(m);
  w.writeFieldHeader(CT_LIST, 3);
  w.writeListHeader(CT_BINARY, static_cast<uint32_t>(maxs.size()));
  for (const auto& m : maxs) w.writeListString(m);
  w.writeI32Field(4, boundary_order);
  if (null_counts != nullptr) {
    w.writeFieldHeader(CT_LIST, 5);
    w.writeListHeader(CT_I64, static_cast<uint32_t>(null_counts->size()));
    for (int64_t n : *null_counts) w.writeListI64(n);
  }
  w.structEnd();
  return w.buf;
}

// OffsetIndex { 1: list<PageLocation{1: i64 offset, 2: i32 size, 3: i64 first_row}> }
static std::vector<uint8_t> write_offset_index(const std::vector<PageLocation>& locs) {
  TCompactWriter w;
  w.structBegin();
  w.writeFieldHeader(CT_LIST, 1);
  w.writeListHeader(CT_STRUCT, static_cast<uint32_t>(locs.size()));
  for (const auto& l : locs) {
    w.structBegin();
    w.writeI64Field(1, l.offset);
    w.writeI32Field(2, l.compressed_page_size);
    w.writeI64Field(3, l.first_row_index);
    w.structEnd();
  }
  w.structEnd();
  return w.buf;
}

static bool throws(const std::function<void()>& f) {
  try { f(); } catch (const std::exception&) { return true; }
  return false;
}

int main() {
  // ── parse round trips ─────────────────────────────────────────────────────
  section("ColumnIndex parse round trip");
  {
    std::vector<int64_t> nc{0, 3, 5};
    auto bytes = write_column_index({false, true, false}, {le64(1), "", le64(40)},
                                    {le64(10), "", le64(90)}, 1, &nc);
    ColumnIndexData ci = ParseColumnIndex(bytes.data(), bytes.size());
    check(ci.null_pages == std::vector<uint8_t>({0, 1, 0}), "null_pages decoded");
    check(ci.min_values.size() == 3 && ci.min_values[0] == le64(1) && ci.min_values[2] == le64(40),
          "min_values decoded");
    check(ci.max_values.size() == 3 && ci.max_values[0] == le64(10) && ci.max_values[2] == le64(90),
          "max_values decoded");
    check(ci.boundary_order == 1, "boundary_order decoded");
    check(ci.null_counts == nc, "null_counts decoded");
  }
  section("ColumnIndex without null_counts");
  {
    auto bytes = write_column_index({false}, {"a"}, {"z"}, 0, nullptr);
    ColumnIndexData ci = ParseColumnIndex(bytes.data(), bytes.size());
    check(ci.null_counts.empty(), "null_counts absent stays empty");
    check(ci.min_values[0] == "a" && ci.max_values[0] == "z", "byte_array bounds verbatim");
  }
  section("OffsetIndex parse round trip");
  {
    std::vector<PageLocation> locs{{100, 50, 0}, {150, 60, 1000}, {210, 40, 2000}};
    auto bytes = write_offset_index(locs);
    OffsetIndexData oi = ParseOffsetIndex(bytes.data(), bytes.size());
    check(oi.page_locations.size() == 3, "three page locations");
    check(oi.page_locations[1].offset == 150 && oi.page_locations[1].compressed_page_size == 60 &&
          oi.page_locations[1].first_row_index == 1000, "middle location exact");
  }
  section("OffsetIndex out-of-order pages are refused");
  {
    std::vector<PageLocation> locs{{100, 50, 0}, {120, 60, 1000}};  // overlaps page 0
    auto bytes = write_offset_index(locs);
    check(throws([&] { ParseOffsetIndex(bytes.data(), bytes.size()); }), "overlapping offsets throw");
    std::vector<PageLocation> locs2{{100, 50, 500}, {150, 60, 100}};  // rows go backwards
    auto bytes2 = write_offset_index(locs2);
    check(throws([&] { ParseOffsetIndex(bytes2.data(), bytes2.size()); }), "non-monotone rows throw");
  }
  section("truncated bytes are refused, not tolerated");
  {
    auto bytes = write_column_index({false, false}, {le64(1), le64(2)}, {le64(3), le64(4)}, 0, nullptr);
    check(throws([&] { ParseColumnIndex(bytes.data(), bytes.size() - 3); }), "short ColumnIndex throws");
  }

  // ── predicate evaluation ─────────────────────────────────────────────────
  std::vector<uint8_t> keep;
  section("int64 membership against page bounds");
  {
    ColumnIndexData ci;
    ci.null_pages = {0, 0, 1, 0};
    ci.min_values = {le64(0), le64(100), "", le64(300)};
    ci.max_values = {le64(99), le64(199), "", le64(399)};
    std::vector<int64_t> needles{150};
    size_t pruned = EvaluatePagePredicate(ci, 4, 0, &needles, nullptr, "int64", false, keep);
    check(pruned == 3, "three of four pages pruned");
    check(keep == std::vector<uint8_t>({0, 1, 0, 0}), "only the page spanning 150 kept");
    std::vector<int64_t> two{5, 350};
    pruned = EvaluatePagePredicate(ci, 4, 0, &two, nullptr, "int64", false, keep);
    check(keep == std::vector<uint8_t>({1, 0, 0, 1}), "IN keeps every page any member hits");
    std::vector<int64_t> edge{99};
    EvaluatePagePredicate(ci, 4, 0, &edge, nullptr, "int64", false, keep);
    check(keep[0] == 1 && keep[1] == 0, "bound is inclusive");
    std::vector<int64_t> none{1000};
    pruned = EvaluatePagePredicate(ci, 4, 0, &none, nullptr, "int64", false, keep);
    check(pruned == 4, "needle outside every page prunes all");
  }
  section("int32 bounds, signed and unsigned");
  {
    ColumnIndexData ci;
    ci.null_pages = {0, 0};
    // page 0: [-5, 5]; page 1: [0xC0A80000, 0xC0A8FFFF] which reads negative as int32
    ci.min_values = {le32(-5), le32(static_cast<int32_t>(0xC0A80000u))};
    ci.max_values = {le32(5),  le32(static_cast<int32_t>(0xC0A8FFFFu))};
    std::vector<int64_t> needle{static_cast<int64_t>(0xC0A80488u)};  // 192.168.4.136 as uint32
    EvaluatePagePredicate(ci, 2, 0, &needle, nullptr, "int32", true, keep);
    check(keep == std::vector<uint8_t>({0, 1}), "unsigned column zero-extends its bounds");
    EvaluatePagePredicate(ci, 2, 0, &needle, nullptr, "int32", false, keep);
    check(keep == std::vector<uint8_t>({0, 0}), "signed column reads the same bits negative");
    std::vector<int64_t> zero{0};
    EvaluatePagePredicate(ci, 2, 0, &zero, nullptr, "int32", false, keep);
    check(keep == std::vector<uint8_t>({1, 0}), "signed page spanning 0 keeps 0");
  }
  section("string membership and starts-with");
  {
    ColumnIndexData ci;
    ci.null_pages = {0, 0, 0};
    ci.min_values = {"apple", "kiwi", "pear"};
    ci.max_values = {"grape", "orange", "zebra"};
    std::vector<std::string> eq{"mango"};
    EvaluatePagePredicate(ci, 3, 1, nullptr, &eq, "byte_array", false, keep);
    check(keep == std::vector<uint8_t>({0, 1, 0}), "equality keeps the page whose range covers it");
    std::vector<std::string> pre{"or"};
    EvaluatePagePredicate(ci, 3, 2, nullptr, &pre, "byte_array", false, keep);
    check(keep == std::vector<uint8_t>({0, 1, 0}), "starts-with keeps the page with an 'or*' value");
    std::vector<std::string> pre2{"pe"};
    EvaluatePagePredicate(ci, 3, 2, nullptr, &pre2, "byte_array", false, keep);
    check(keep == std::vector<uint8_t>({0, 0, 1}), "starts-with where min itself carries the prefix");
    std::vector<std::string> pre3{"h"};
    size_t pruned = EvaluatePagePredicate(ci, 3, 2, nullptr, &pre3, "byte_array", false, keep);
    check(pruned == 3 && keep == std::vector<uint8_t>({0, 0, 0}),
          "prefix falling between two pages' ranges prunes every page");
    std::vector<std::string> empty{""};
    pruned = EvaluatePagePredicate(ci, 3, 2, nullptr, &empty, "byte_array", false, keep);
    check(pruned == 0, "empty prefix matches everything");
    // Truncated max: the writer shortened+incremented "grapefruit" to "grapf".
    ColumnIndexData tr;
    tr.null_pages = {0};
    tr.min_values = {"apple"};
    tr.max_values = {"grapf"};
    std::vector<std::string> gf{"grapefruit"};
    EvaluatePagePredicate(tr, 1, 1, nullptr, &gf, "byte_array", false, keep);
    check(keep[0] == 1, "truncated max still bounds the real value (kept)");
  }
  section("kinds with no bounds test prune only null pages");
  {
    ColumnIndexData ci;
    ci.null_pages = {1, 0};
    ci.min_values = {"", "a"};
    ci.max_values = {"", "b"};
    std::vector<std::string> pat{"zzz"};
    size_t pruned = EvaluatePagePredicate(ci, 2, 4, nullptr, &pat, "byte_array", false, keep);
    check(pruned == 1 && keep == std::vector<uint8_t>({0, 1}), "contains: null page pruned, other kept");
    pruned = EvaluatePagePredicate(ci, 2, 3, nullptr, &pat, "byte_array", false, keep);
    check(pruned == 1, "ends-with: same");
  }
  section("mismatched index sizes carry no bounds");
  {
    ColumnIndexData ci;
    ci.null_pages = {0, 0};
    ci.min_values = {le64(0)};          // one entry for two pages
    ci.max_values = {le64(1)};
    std::vector<int64_t> needle{500};
    size_t pruned = EvaluatePagePredicate(ci, 2, 0, &needle, nullptr, "int64", false, keep);
    check(pruned == 0, "short min/max lists prune nothing");
    ColumnIndexData ci2;
    ci2.null_pages = {1};               // one entry for two pages
    pruned = EvaluatePagePredicate(ci2, 2, 0, &needle, nullptr, "int64", false, keep);
    check(pruned == 0, "short null_pages prunes nothing");
  }
  section("type/kind mismatches keep every page");
  {
    ColumnIndexData ci;
    ci.null_pages = {0};
    ci.min_values = {le64(0)};
    ci.max_values = {le64(10)};
    std::vector<int64_t> needle{500};
    check(EvaluatePagePredicate(ci, 1, 0, &needle, nullptr, "float64", false, keep) == 0,
          "int needles on a float column: kept");
    std::vector<std::string> s{"x"};
    check(EvaluatePagePredicate(ci, 1, 1, nullptr, &s, "int64", false, keep) == 0,
          "string needles on an int column: kept");
    check(EvaluatePagePredicate(ci, 1, -1, nullptr, nullptr, "int64", false, keep) == 0,
          "no predicate: kept");
  }

  // ── writer → reader round trip ─────────────────────────────────────────────
  //
  // Everything above feeds the parser bytes this test file wrote. These feed it
  // bytes THE WRITER wrote (page_index_writer.hpp, the same code path
  // _parquet_writer.hpp uses), so the two halves of the format are pinned
  // against each other rather than against a local restatement of it.
  {
    using rugo_pq_write::PageMeta;
    using rugo_pq_write::PB_BYTES;
    using rugo_pq_write::PB_INT64;
    using rugo_pq_write::serialize_column_index;
    using rugo_pq_write::serialize_offset_index;

    auto int_page = [](int64_t first_row, int64_t off, int64_t size,
                       int64_t lo, int64_t hi) {
      PageMeta pm;
      pm.first_row_index = first_row;
      pm.file_offset = off;
      pm.size = size;
      pm.has_bounds = true;
      pm.min_bytes.assign(reinterpret_cast<const uint8_t*>(&lo),
                          reinterpret_cast<const uint8_t*>(&lo) + 8);
      pm.max_bytes.assign(reinterpret_cast<const uint8_t*>(&hi),
                          reinterpret_cast<const uint8_t*>(&hi) + 8);
      return pm;
    };
    auto bytes_of = [](const std::string& v) {
      return std::vector<uint8_t>(v.begin(), v.end());
    };

    section("writer round trip: OffsetIndex");
    {
      std::vector<PageMeta> pages{int_page(0, 1000, 100, 0, 9),
                                  int_page(10, 1100, 120, 10, 19),
                                  int_page(20, 1220, 90, 20, 29)};
      OffsetIndexData oi = ParseOffsetIndex(serialize_offset_index(pages).data(),
                                            serialize_offset_index(pages).size());
      check(oi.page_locations.size() == 3, "three page locations survive");
      check(oi.page_locations[1].offset == 1100 &&
                oi.page_locations[1].compressed_page_size == 120 &&
                oi.page_locations[1].first_row_index == 10,
            "offset / size / first_row round trip");
      check(oi.page_locations[2].offset == 1220, "pages stay contiguous and in order");
    }

    section("writer round trip: ColumnIndex bounds prune");
    {
      std::vector<PageMeta> pages{int_page(0, 1000, 100, 0, 9),
                                  int_page(10, 1100, 100, 100, 109),
                                  int_page(20, 1200, 100, 200, 209)};
      pages[1].null_count = 3;
      std::vector<uint8_t> buf = serialize_column_index(pages, PB_INT64, false);
      ColumnIndexData ci = ParseColumnIndex(buf.data(), buf.size());
      check(ci.null_pages.size() == 3 && ci.min_values.size() == 3 &&
                ci.max_values.size() == 3,
            "three pages, three bounds");
      check(ci.boundary_order == 1, "ascending bounds are reported ASCENDING");
      check(ci.null_counts.size() == 3 && ci.null_counts[1] == 3, "null_counts round trip");
      std::vector<int64_t> needle{105};
      size_t pruned = EvaluatePagePredicate(ci, 3, 0, &needle, nullptr, "int64", false, keep);
      check(pruned == 2 && keep[1] == 1, "only the page whose range holds 105 survives");
    }

    section("writer round trip: all-NULL page");
    {
      std::vector<PageMeta> pages{int_page(0, 1000, 100, 0, 9), int_page(10, 1100, 100, 0, 0)};
      pages[1].null_page = true;
      pages[1].has_bounds = false;
      pages[1].null_count = 10;
      pages[1].min_bytes.clear();
      pages[1].max_bytes.clear();
      std::vector<uint8_t> buf = serialize_column_index(pages, PB_INT64, false);
      ColumnIndexData ci = ParseColumnIndex(buf.data(), buf.size());
      check(ci.null_pages[1] == 1, "the null page is flagged");
      check(ci.min_values[1].empty() && ci.max_values[1].empty(),
            "a null page's bounds are written empty");
      std::vector<int64_t> needle{5};
      size_t pruned = EvaluatePagePredicate(ci, 2, 0, &needle, nullptr, "int64", false, keep);
      check(pruned == 1 && keep[0] == 1, "the null page prunes, the matching page stays");
    }

    section("writer round trip: an unbounded non-null page writes NO index");
    {
      std::vector<PageMeta> pages{int_page(0, 1000, 100, 0, 9), int_page(10, 1100, 100, 0, 0)};
      pages[1].has_bounds = false;   // neither all-NULL nor bounded
      check(serialize_column_index(pages, PB_INT64, false).empty(),
            "no ColumnIndex rather than a hole in a required list");
    }

    section("writer round trip: BYTE_ARRAY bound truncation");
    {
      // A 200-byte min and max, identical for the first 64 bytes: without
      // truncation the bounds are exact; with it the min shortens to a prefix
      // and the max shortens-and-increments, which must still BRACKET the real
      // values or a matching page could be pruned away.
      const std::string lo(100, 'a');
      const std::string hi = std::string(64, 'a') + std::string(36, 'z');
      std::vector<PageMeta> pages(1);
      pages[0].has_bounds = true;
      pages[0].min_bytes = bytes_of(lo);
      pages[0].max_bytes = bytes_of(hi);
      std::vector<uint8_t> buf = serialize_column_index(pages, PB_BYTES, false);
      ColumnIndexData ci = ParseColumnIndex(buf.data(), buf.size());
      check(ci.min_values[0].size() == 64 && ci.max_values[0].size() == 64,
            "both bounds truncate to 64 bytes");
      check(ci.min_values[0] == std::string(64, 'a'), "min is the plain prefix");
      check(ci.max_values[0] == std::string(63, 'a') + "b", "max is the prefix incremented");
      check(ci.min_values[0] <= lo, "truncated min does not exceed the real min");
      check(ci.max_values[0] >= hi, "truncated max is not below the real max");
      // The real values must still be found through the truncated bounds.
      std::vector<std::string> needles{lo};
      check(EvaluatePagePredicate(ci, 1, 1, nullptr, &needles, "byte_array", false, keep) == 0,
            "the real min is still reachable through the truncated bounds");
      needles = {hi};
      check(EvaluatePagePredicate(ci, 1, 1, nullptr, &needles, "byte_array", false, keep) == 0,
            "the real max is still reachable through the truncated bounds");
      needles = {std::string(64, 'a') + "m"};
      check(EvaluatePagePredicate(ci, 1, 1, nullptr, &needles, "byte_array", false, keep) == 0,
            "a value inside the widened interval keeps the page");
      needles = {"zzzz"};
      check(EvaluatePagePredicate(ci, 1, 1, nullptr, &needles, "byte_array", false, keep) == 1,
            "a value outside it still prunes");
    }

    section("writer round trip: an all-0xFF max has no successor");
    {
      std::vector<PageMeta> pages(1);
      pages[0].has_bounds = true;
      pages[0].min_bytes.assign(70, 0x00);
      pages[0].max_bytes.assign(70, 0xFF);
      std::vector<uint8_t> buf = serialize_column_index(pages, PB_BYTES, false);
      ColumnIndexData ci = ParseColumnIndex(buf.data(), buf.size());
      check(ci.max_values[0].empty(), "the bound is dropped rather than wrapped");
      std::vector<std::string> needles{std::string(70, 0xFF)};
      check(EvaluatePagePredicate(ci, 1, 1, nullptr, &needles, "byte_array", false, keep) == 0,
            "an empty bound keeps the page");
    }

    section("writer round trip: boundary_order");
    {
      std::vector<PageMeta> desc{int_page(0, 1000, 100, 200, 209),
                                 int_page(10, 1100, 100, 100, 109),
                                 int_page(20, 1200, 100, 0, 9)};
      std::vector<uint8_t> b1 = serialize_column_index(desc, PB_INT64, false);
      check(ParseColumnIndex(b1.data(), b1.size()).boundary_order == 2, "descending");
      std::vector<PageMeta> mixed{int_page(0, 1000, 100, 100, 109),
                                  int_page(10, 1100, 100, 0, 9),
                                  int_page(20, 1200, 100, 200, 209)};
      std::vector<uint8_t> b2 = serialize_column_index(mixed, PB_INT64, false);
      check(ParseColumnIndex(b2.data(), b2.size()).boundary_order == 0, "unordered");
    }
  }

  if (g_failures == 0) std::printf("\nALL PASS (0 failures)\n");
  else std::printf("\n%d FAILURE(S)\n", g_failures);
  return g_failures == 0 ? 0 : 1;
}
