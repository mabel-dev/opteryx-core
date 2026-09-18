// page_index.cpp — see page_index.hpp.

#include "page_index.hpp"
#include "thrift.hpp"

#include <algorithm>
#include <cstring>
#include <limits>
#include <stdexcept>

namespace {

// Thrift Compact Protocol wire nibbles (the values ReadFieldHeader /
// ReadListHeader hand back). thrift.hpp's ThriftType enum is NOT these — it is
// the TType numbering — so the codes are spelled out here.
constexpr uint8_t kCtBoolTrue  = 1;
constexpr uint8_t kCtBoolFalse = 2;
constexpr uint8_t kCtI64       = 6;
constexpr uint8_t kCtBinary    = 8;
constexpr uint8_t kCtStruct    = 12;

void ExpectElemType(const ListHeader &lh, uint8_t want, const char *what) {
  if (lh.elem_type != want) {
    throw std::runtime_error(std::string("page index: ") + what +
                             " list has element type " + std::to_string(lh.elem_type) +
                             ", expected " + std::to_string(want));
  }
}

// list<bool>: elements are one byte each, 1 = true, 2 = false. The element
// type nibble a writer records for a bool list is BOOL_TRUE (1); accept
// BOOL_FALSE too, which some encoders emit for an all-false list.
std::vector<uint8_t> ReadBoolList(TInput &in, const char *what) {
  ListHeader lh = ReadListHeader(in);
  if (lh.elem_type != kCtBoolTrue && lh.elem_type != kCtBoolFalse)
    ExpectElemType(lh, kCtBoolTrue, what);
  std::vector<uint8_t> out;
  out.reserve(lh.size);
  for (uint32_t i = 0; i < lh.size; ++i) {
    const uint8_t b = in.readByte();
    if (b != kCtBoolTrue && b != kCtBoolFalse)
      throw std::runtime_error(std::string("page index: ") + what +
                               " has a non-bool element byte " + std::to_string(b));
    out.push_back(b == kCtBoolTrue ? 1 : 0);
  }
  return out;
}

std::vector<std::string> ReadBinaryList(TInput &in, const char *what) {
  ListHeader lh = ReadListHeader(in);
  ExpectElemType(lh, kCtBinary, what);
  std::vector<std::string> out;
  out.reserve(lh.size);
  for (uint32_t i = 0; i < lh.size; ++i) out.push_back(ReadString(in));
  return out;
}

std::vector<int64_t> ReadI64List(TInput &in, const char *what) {
  ListHeader lh = ReadListHeader(in);
  ExpectElemType(lh, kCtI64, what);
  std::vector<int64_t> out;
  out.reserve(lh.size);
  for (uint32_t i = 0; i < lh.size; ++i) out.push_back(ReadI64(in));
  return out;
}

// parquet.thrift PageLocation { 1: i64 offset, 2: i32 compressed_page_size,
//                               3: i64 first_row_index }
PageLocation ReadPageLocation(TInput &in) {
  PageLocation loc;
  int16_t last_id = 0;
  while (true) {
    FieldHeader fh = ReadFieldHeader(in, last_id);
    if (fh.type == 0) break;
    switch (fh.id) {
    case 1: loc.offset = ReadI64(in); break;
    case 2: loc.compressed_page_size = ReadI32(in); break;
    case 3: loc.first_row_index = ReadI64(in); break;
    default: SkipField(in, fh.type); break;
    }
  }
  if (loc.offset < 0 || loc.compressed_page_size <= 0 || loc.first_row_index < 0)
    throw std::runtime_error("page index: PageLocation is missing a required field");
  return loc;
}

// Unsigned byte-wise order — parquet's TYPE_DEFINED_ORDER for BYTE_ARRAY.
int CompareBytes(const std::string &a, const std::string &b) {
  const size_t n = std::min(a.size(), b.size());
  const int c = n == 0 ? 0 : std::memcmp(a.data(), b.data(), n);
  if (c != 0) return c;
  if (a.size() == b.size()) return 0;
  return a.size() < b.size() ? -1 : 1;
}

bool StartsWith(const std::string &s, const std::string &prefix) {
  return s.size() >= prefix.size() &&
         (prefix.empty() || std::memcmp(s.data(), prefix.data(), prefix.size()) == 0);
}

// Decode a PLAIN int32/int64 bound into the int64 domain the needles live in.
// A declared-unsigned column zero-extends (E33) — the same rule the dictionary
// probe applies in decode_column.cpp. int64 unsigned bounds above INT64_MAX
// cannot be represented as int64: `overflow` reports that so the caller keeps
// the page rather than comparing a wrapped value.
bool DecodeIntBound(const std::string &raw, const std::string &physical_type,
                    bool is_unsigned, int64_t &out, bool &overflow) {
  overflow = false;
  if (physical_type == "int32") {
    if (raw.size() != 4) return false;
    int32_t v;
    std::memcpy(&v, raw.data(), 4);
    out = is_unsigned ? static_cast<int64_t>(static_cast<uint32_t>(v))
                      : static_cast<int64_t>(v);
    return true;
  }
  if (physical_type == "int64") {
    if (raw.size() != 8) return false;
    int64_t v;
    std::memcpy(&v, raw.data(), 8);
    if (is_unsigned && v < 0) { overflow = true; out = std::numeric_limits<int64_t>::max(); return true; }
    out = v;
    return true;
  }
  return false;
}

}  // namespace

ColumnIndexData ParseColumnIndex(const uint8_t *data, size_t size) {
  ColumnIndexData ci;
  TInput in{data, data + size};
  int16_t last_id = 0;
  while (true) {
    FieldHeader fh = ReadFieldHeader(in, last_id);
    if (fh.type == 0) break;
    switch (fh.id) {
    case 1: ci.null_pages = ReadBoolList(in, "ColumnIndex.null_pages"); break;
    case 2: ci.min_values = ReadBinaryList(in, "ColumnIndex.min_values"); break;
    case 3: ci.max_values = ReadBinaryList(in, "ColumnIndex.max_values"); break;
    case 4: ci.boundary_order = ReadI32(in); break;
    case 5: ci.null_counts = ReadI64List(in, "ColumnIndex.null_counts"); break;
    // 6/7: repetition/definition level histograms — not consumed.
    default: SkipField(in, fh.type); break;
    }
  }
  return ci;
}

OffsetIndexData ParseOffsetIndex(const uint8_t *data, size_t size) {
  OffsetIndexData oi;
  TInput in{data, data + size};
  int16_t last_id = 0;
  while (true) {
    FieldHeader fh = ReadFieldHeader(in, last_id);
    if (fh.type == 0) break;
    switch (fh.id) {
    case 1: {
      ListHeader lh = ReadListHeader(in);
      ExpectElemType(lh, kCtStruct, "OffsetIndex.page_locations");
      oi.page_locations.reserve(lh.size);
      for (uint32_t i = 0; i < lh.size; ++i) oi.page_locations.push_back(ReadPageLocation(in));
      break;
    }
    // 2: unencoded_byte_array_data_bytes — not consumed.
    default: SkipField(in, fh.type); break;
    }
  }
  // The decoder's page jump and the row-mask construction both rely on the
  // locations being in file order with non-decreasing first rows. A writer that
  // violates this has written a corrupt index.
  for (size_t p = 1; p < oi.page_locations.size(); ++p) {
    const PageLocation &a = oi.page_locations[p - 1];
    const PageLocation &b = oi.page_locations[p];
    if (b.offset < a.offset + a.compressed_page_size || b.first_row_index < a.first_row_index)
      throw std::runtime_error("page index: OffsetIndex page_locations are not in file order");
  }
  return oi;
}

size_t EvaluatePagePredicate(const ColumnIndexData &ci, size_t num_pages,
                             int kind,
                             const std::vector<int64_t> *int_vals,
                             const std::vector<std::string> *str_vals,
                             const std::string &physical_type,
                             bool is_unsigned,
                             std::vector<uint8_t> &keep) {
  keep.assign(num_pages, 1);
  size_t pruned = 0;

  // Null pages match no per-value predicate, whatever its kind.
  const bool have_null_pages = ci.null_pages.size() == num_pages;
  const bool have_bounds = ci.min_values.size() == num_pages && ci.max_values.size() == num_pages;

  if (have_null_pages) {
    for (size_t p = 0; p < num_pages; ++p) {
      if (ci.null_pages[p]) { keep[p] = 0; ++pruned; }
    }
  }
  if (!have_bounds) return pruned;

  const bool int_kind = kind == 0 && int_vals != nullptr && !int_vals->empty() &&
                        (physical_type == "int32" || physical_type == "int64");
  const bool str_kind = (kind == 1 || kind == 2) && str_vals != nullptr && !str_vals->empty() &&
                        physical_type == "byte_array";
  if (!int_kind && !str_kind) return pruned;

  for (size_t p = 0; p < num_pages; ++p) {
    if (!keep[p]) continue;
    // A page the writer flagged null carries no meaningful bounds; if the writer
    // omitted null_pages entirely, an empty bound is the same signal.
    if (ci.min_values[p].empty() || ci.max_values[p].empty()) continue;

    bool any_match = false;
    if (int_kind) {
      int64_t lo, hi;
      bool lo_ovf, hi_ovf;
      if (!DecodeIntBound(ci.min_values[p], physical_type, is_unsigned, lo, lo_ovf) ||
          !DecodeIntBound(ci.max_values[p], physical_type, is_unsigned, hi, hi_ovf))
        continue;  // malformed bound width → keep the page
      if (lo_ovf) continue;  // the page's whole range is above INT64_MAX: no int64 needle
                             // can be compared soundly, keep it
      for (int64_t v : *int_vals) {
        if (v >= lo && (hi_ovf || v <= hi)) { any_match = true; break; }
      }
    } else {
      const std::string &lo = ci.min_values[p];
      const std::string &hi = ci.max_values[p];
      for (const std::string &s : *str_vals) {
        if (kind == 1) {
          if (CompareBytes(s, lo) >= 0 && CompareBytes(s, hi) <= 0) { any_match = true; break; }
        } else {  // starts-with: some v in [lo, hi] has prefix s
          if (s.empty()) { any_match = true; break; }
          if (CompareBytes(hi, s) < 0) continue;                       // every v < s
          if (CompareBytes(lo, s) > 0 && !StartsWith(lo, s)) continue; // every v > all s-prefixed
          any_match = true; break;
        }
      }
    }
    if (!any_match) { keep[p] = 0; ++pruned; }
  }
  return pruned;
}
