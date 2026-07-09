#pragma once
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

struct MetadataParseOptions {
  bool schema_only = false;
  bool include_statistics = true;
  int64_t max_row_groups = -1;
};

struct LogicalTypeInfo {
  std::string type_name; // e.g. "STRING", "TIMESTAMP_MILLIS", "DECIMAL"
  // Additional logical type parameters could be added here if needed
};

struct ColumnStats {
  std::string name;          // joined path_in_schema: "a.b.c"
  std::string physical_type; // e.g. "INT64", "BYTE_ARRAY"
  std::string logical_type;  // e.g. "STRING", "TIMESTAMP_MILLIS", "DECIMAL"

  // Sizes & counts
  int64_t num_values = -1;
  int64_t total_uncompressed_size = -1;
  int64_t total_compressed_size = -1;

  // Offsets
  int64_t data_page_offset = -1;
  int64_t index_page_offset = -1;
  int64_t dictionary_page_offset = -1;
  int64_t column_chunk_file_offset = -1;  // Optional ColumnChunk.file_offset

  // Statistics
  bool has_min = false;
  bool has_max = false;
  std::string min;
  std::string max;
  int64_t null_count = -1;
  int64_t distinct_count = -1;

  // Bloom filter
  int64_t bloom_offset = -1;
  int64_t bloom_length = -1;

  // Page Index (Parquet 2.x). All -1 when the file was written without page indexes.
  // column_index = per-page min/max/null_count (for predicate pruning)
  // offset_index = per-page byte offset + compressed length (to skip pages on read)
  int64_t column_index_offset = -1;
  int32_t column_index_length = -1;
  int64_t offset_index_offset = -1;
  int32_t offset_index_length = -1;

  // Encodings & codec
  std::vector<int32_t> encodings;
  int32_t codec = -1;

  // Schema information
  int32_t repetition_type = -1;  // 0=REQUIRED, 1=OPTIONAL, 2=REPEATED
  int32_t max_definition_level = -1;
  int32_t max_repetition_level = -1;

  // Width of FIXED_LEN_BYTE_ARRAY values (bytes). 0 for other physical types.
  int32_t type_length = 0;

  // Raw key/value metadata (flattened for now)
  std::unordered_map<std::string, std::string> key_value_metadata;
};

struct RowGroupStats {
  int64_t num_rows = 0;
  int64_t total_byte_size = 0;
  std::vector<ColumnStats> columns;
};

struct SchemaElement {
  std::string name;
  std::string full_name;
  std::string physical_type;
  std::string logical_type;
  int num_children = 0;
  int32_t type_length = 0; // for FIXED_LEN_BYTE_ARRAY (e.g. flba5)
  int32_t scale = 0;       // for DECIMAL
  int32_t precision = 0;   // for DECIMAL
  int32_t repetition_type = -1;
  std::vector<SchemaElement> children;
};

struct SchemaField {
  std::string name;
  std::string physical_type;
  std::string logical_type;
  bool nullable = true;
};

struct FileStats {
  int64_t num_rows = 0;
  std::vector<RowGroupStats> row_groups;
  std::vector<SchemaElement> schema;
  std::vector<SchemaField> schema_columns;
};

FileStats ReadParquetMetadata(const std::string &path,
                             const MetadataParseOptions &options);
FileStats ReadParquetMetadata(const std::string &path);
FileStats ReadParquetMetadataFromBuffer(const uint8_t *buf, size_t size,
                                        const MetadataParseOptions &options);
FileStats ReadParquetMetadataFromBuffer(const uint8_t *buf, size_t size);

inline FileStats ReadParquetMetadataC(const char *path) {
  return ReadParquetMetadata(std::string(path));
}

inline FileStats ReadParquetMetadataC(const char *path,
                                      const MetadataParseOptions &options) {
  return ReadParquetMetadata(std::string(path), options);
}

// Helper functions to convert enums to strings
const char *EncodingToString(int32_t enc);
const char *CompressionCodecToString(int32_t codec);

// New functions for bloom filter testing
bool TestBloomFilter(const std::string &file_path, int64_t bloom_offset,
                     int64_t bloom_length, const std::string &value);

// In-memory sibling of TestBloomFilter: probe a bloom filter whose serialized
// bytes (header + bitset) are already in memory, rather than reading them from a
// file. `data`/`len` span exactly the bloom region (the `bloom_length` bytes the
// footer records). `value` is the PLAIN-encoded needle bytes, encoded identically
// to the writer's bloom_hashes (int32=4 LE, int64=8 LE, byte_array=raw). Returns
// true if the value MAY be present, false only if it is provably absent. Used on
// the remote decode path to skip a row group's decode without a separate fetch.
bool TestBloomFilterBytes(const uint8_t *data, size_t len,
                          const std::string &value);

// Aggregated per-column statistics across all row groups.
// Used by the planning phase to extract min/max/null_count without building
// per-row-group Python dicts.
struct AggColumnStat {
  std::string name;          // top-level (display) name
  std::string physical_type; // from ColumnStats leaf
  std::string logical_type;  // from ColumnStats leaf (enriched by schema)
  std::string min_bytes;     // raw bytes of the minimum value (global min of mins)
  std::string max_bytes;     // raw bytes of the maximum value (global max of maxes)
  int64_t null_count = 0;
  bool has_min = false;
  bool has_max = false;
  bool null_count_complete = true;
};

// Aggregate column statistics across all row groups in a single C++ pass.
// Binary comparison avoids decoding intermediate values — only the winner
// per column is decoded by the caller (Cython).
std::vector<AggColumnStat> AggregateColumnStats(const FileStats &fs);
