// parquet_footer_map.hpp — the ONE footer-map type every parquet scan plan and
// native Source shares, and the byte accounting the parsed-footer cache budgets by.
//
// A parsed footer is immutable once built, so it is held by
// `shared_ptr<const FileStats>`: the process-global parsed-footer cache
// (opteryx/compiled/structures/footer_cache.pyx) owns one reference, and every scan
// plan that uses the footer owns another for the plan's lifetime. A scan therefore
// never copies a footer, and an entry the cache evicts mid-query stays alive until
// the last plan holding it is freed. The cache's byte budget bounds what the CACHE
// keeps resident; a footer pinned only by a running query is that query's memory.
#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>

#include "metadata.hpp"   // FileStats (rugo/src/parquet)

using ParquetFooterRef = std::shared_ptr<const FileStats>;
using ParquetFooterMap = std::unordered_map<std::string, ParquetFooterRef>;

namespace parquet_footer_bytes_detail {

inline int64_t str_bytes(const std::string& s) {
    return static_cast<int64_t>(sizeof(std::string)) + static_cast<int64_t>(s.capacity());
}

inline int64_t kv_bytes(const std::unordered_map<std::string, std::string>& kv) {
    int64_t total = static_cast<int64_t>(kv.bucket_count() * sizeof(void*));
    for (const auto& e : kv) total += str_bytes(e.first) + str_bytes(e.second) + 2 * sizeof(void*);
    return total;
}

inline int64_t schema_element_bytes(const SchemaElement& e) {
    int64_t total = static_cast<int64_t>(sizeof(SchemaElement)) + str_bytes(e.name) +
                    str_bytes(e.full_name) + str_bytes(e.physical_type) +
                    str_bytes(e.logical_type) - 4 * static_cast<int64_t>(sizeof(std::string));
    for (const SchemaElement& c : e.children) total += schema_element_bytes(c);
    return total;
}

}  // namespace parquet_footer_bytes_detail

// Heap + inline bytes a parsed footer occupies: every vector's capacity, every
// string's buffer, every map's nodes. What the parsed-footer cache charges an entry
// against its budget — the parsed size, not the encoded footer's.
inline int64_t parquet_footer_bytes(const FileStats& fs) {
    using namespace parquet_footer_bytes_detail;
    int64_t total = static_cast<int64_t>(sizeof(FileStats));
    total += static_cast<int64_t>(fs.row_groups.capacity() * sizeof(RowGroupStats));
    for (const RowGroupStats& rg : fs.row_groups) {
        total += static_cast<int64_t>(rg.columns.capacity() * sizeof(ColumnStats));
        for (const ColumnStats& c : rg.columns) {
            // sizeof(ColumnStats) already counts the inline std::string headers.
            total += static_cast<int64_t>(c.name.capacity() + c.physical_type.capacity() +
                                          c.logical_type.capacity() + c.min.capacity() +
                                          c.max.capacity());
            total += static_cast<int64_t>(c.encodings.capacity() * sizeof(int32_t));
            total += static_cast<int64_t>(c.list_def_thresholds.capacity() * sizeof(int32_t));
            if (!c.key_value_metadata.empty()) total += kv_bytes(c.key_value_metadata);
        }
    }
    total += static_cast<int64_t>(fs.schema.capacity() * sizeof(SchemaElement));
    for (const SchemaElement& e : fs.schema)
        total += schema_element_bytes(e) - static_cast<int64_t>(sizeof(SchemaElement));
    total += static_cast<int64_t>(fs.schema_columns.capacity() * sizeof(SchemaField));
    for (const SchemaField& f : fs.schema_columns)
        total += static_cast<int64_t>(f.name.capacity() + f.physical_type.capacity() +
                                      f.logical_type.capacity());
    total += kv_bytes(fs.key_value_metadata);
    return total;
}
