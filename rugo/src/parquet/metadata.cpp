#include "metadata.hpp"
#include "thrift.hpp"
#include <algorithm>
#include <cstring>
#include <fstream>
#include <iostream>
#include <functional>
#include <stdexcept>
#include <vector>

// ------------------- Helpers -------------------

static inline uint32_t ReadLE32(const uint8_t *p) {
  return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) |
         ((uint32_t)p[3] << 24);
}

static inline const char *ParquetTypeToString(int t) {
  switch (t) {
  case 0:
    return "boolean";
  case 1:
    return "int32";
  case 2:
    return "int64";
  case 3:
    return "int96";
  case 4:
    return "float32";
  case 5:
    return "float64";
  case 6:
    return "byte_array";
  case 7:
    return "fixed_len_byte_array";
  default:
    return "unknown";
  }
}

// Legacy `converted_type` (parquet.thrift enum ConvertedType) -> the SAME logical
// type vocabulary ParseLogicalType emits for the modern `logicalType` union.
//
// Both properties here are load-bearing and both were previously wrong:
//
//   1. The enum values must match the spec exactly:
//        UTF8=0 MAP=1 MAP_KEY_VALUE=2 LIST=3 ENUM=4 DECIMAL=5 DATE=6
//        TIME_MILLIS=7 TIME_MICROS=8 TIMESTAMP_MILLIS=9 TIMESTAMP_MICROS=10
//        UINT_8=11 UINT_16=12 UINT_32=13 UINT_64=14
//        INT_8=15 INT_16=16 INT_32=17 INT_64=18 JSON=19 BSON=20 INTERVAL=21
//      MAP_KEY_VALUE=2 was missing, shifting every value above it by one: a DATE
//      column came back "TIME_MILLIS", a DECIMAL came back "DATE", an INT_64 came
//      back "JSON".
//
//   2. The strings must be the ones consumers actually match, which are the
//      lowercase forms the modern path produces ("uint8", "date32[day]",
//      "timestamp[ms]"). The old UPPER_SNAKE spellings matched nothing —
//      decode_column.cpp's IntType detection, io_pipeline's admission gate and
//      _rugo_schema's type maps are all lowercase — so a legacy-annotated
//      unsigned column silently decoded as signed.
//
// Only reached for files carrying NO modern logicalType (pre-2.4 writers, some
// Hive/Impala/Spark-legacy output); modern writers emit both and field 10 wins.
//
// DECIMAL needs the element's precision/scale to build "decimal(p,s)", and those
// arrive in LATER thrift fields (7/8) than converted_type (6) — so this is called
// after the field loop completes, not inline. See ParseSchemaElement.
static inline std::string ConvertedTypeToString(int t, int32_t precision,
                                                int32_t scale) {
  switch (t) {
  case 0:
    return "varchar"; // UTF8
  case 1:
  case 2:
    return "map"; // MAP, MAP_KEY_VALUE
  case 3:
    return "array"; // LIST
  case 4:
    return "enum";
  case 5:
    return "decimal(" + std::to_string(precision) + "," + std::to_string(scale) + ")";
  case 6:
    return "date32[day]";
  case 7:
    return "time[ms]";
  case 8:
    return "time[us]";
  case 9:
    return "timestamp[ms]";
  case 10:
    return "timestamp[us]";
  case 11:
    return "uint8";
  case 12:
    return "uint16";
  case 13:
    return "uint32";
  case 14:
    return "uint64";
  case 15:
    return "int8";
  case 16:
    return "int16";
  case 17:
    return "int32";
  case 18:
    return "int64";
  case 19:
    return "json";
  case 20:
    return "bson";
  case 21:
    return "interval";
  default:
    return "";
  }
}

const char *EncodingToString(int32_t enc) {
  switch (enc) {
  case 0:
    return "PLAIN";
  case 1:
    return "GROUP_VAR_INT";        // deprecated
  case 2:
    return "PLAIN_DICTIONARY";
  case 3:
    return "RLE";
  case 4:
    return "BIT_PACKED";           // deprecated
  case 5:
    return "DELTA_BINARY_PACKED";
  case 6:
    return "DELTA_LENGTH_BYTE_ARRAY";
  case 7:
    return "DELTA_BYTE_ARRAY";
  case 8:
    return "RLE_DICTIONARY";
  case 9:
    return "BYTE_STREAM_SPLIT";
  default:
    return "UNKNOWN";
  }
}

const char *CompressionCodecToString(int32_t codec) {
  switch (codec) {
  case 0:
    return "UNCOMPRESSED";
  case 1:
    return "SNAPPY";
  case 2:
    return "GZIP";
  case 3:
    return "LZO";
  case 4:
    return "BROTLI";
  case 5:
    return "LZ4";
  case 6:
    return "ZSTD";
  case 7:
    return "LZ4_RAW";
  default:
    return "UNKNOWN";
  }
}

static inline std::string CanonicalizeColumnName(std::string name) {
  if (name.rfind("schema.", 0) == 0) {
    name.erase(0, 7); // strip schema.
  }
  // A nested list flattens to a repeated ".list.element" (or ".list.item")
  // suffix PER nesting level, e.g. list<list<int64>> -> "x.list.element.list.element".
  // Strip every trailing list-wrapper suffix so a nested column reports its
  // top-level field name ("x"), matching single-level lists. Struct paths
  // (e.g. "address.city") are preserved — only list wrappers are removed.
  for (;;) {
    if (name.size() >= 13 &&
        name.compare(name.size() - 13, 13, ".list.element") == 0) {
      name.erase(name.size() - 13);
    } else if (name.size() >= 10 &&
               name.compare(name.size() - 10, 10, ".list.item") == 0) {
      name.erase(name.size() - 10);
    } else {
      break;
    }
  }
  return name;
}

// ------------------- Schema parsing -------------------

// Correct logical type structure parsing
static std::string ParseLogicalType(TInput &in) {
  std::string result;
  int16_t last_id = 0;

  while (true) {
    auto fh = ReadFieldHeader(in, last_id);
    if (fh.type == 0)
      break;

    switch (fh.id) {
    case 1: {             // STRING (StringType - empty struct)
      SkipStruct(in);     // Just skip the empty StringType struct
      result = "varchar"; // Use varchar for STRING type
      break;
    }
    case 2: { // MAP (MapType - empty struct)
      SkipStruct(in);
      result = "map";
      break;
    }
    case 3: { // LIST (ListType - empty struct)
      SkipStruct(in);
      result = "array";
      break;
    }
    case 4: { // ENUM (EnumType - empty struct)
      SkipStruct(in);
      result = "enum";
      break;
    }
    case 5: { // DECIMAL (DecimalType)
      int32_t scale = 0, precision = 0;
      int16_t decimal_last = 0;
      while (true) {
        auto inner = ReadFieldHeader(in, decimal_last);
        if (inner.type == 0)
          break;
        if (inner.id == 1)
          scale = ReadI32(in);
        else if (inner.id == 2)
          precision = ReadI32(in);
        else
          SkipField(in, inner.type);
      }
      result = "decimal(" + std::to_string(precision) + "," +
               std::to_string(scale) + ")";
      break;
    }
    case 6: { // DATE (DateType - empty struct)
      SkipStruct(in);
      result = "date32[day]";
      break;
    }
    case 7: { // TIME (TimeType)
      int16_t time_last = 0;
      bool isAdjustedToUTC = false;
      std::string unit = "ms";
      while (true) {
        auto inner = ReadFieldHeader(in, time_last);
        if (inner.type == 0)
          break;
        if (inner.id == 1)
          isAdjustedToUTC = ReadBool(inner.type);
        else if (inner.id == 2) { // unit
          int16_t unit_last = 0;
          while (true) {
            auto unit_fh = ReadFieldHeader(in, unit_last);
            if (unit_fh.type == 0)
              break;
            if (unit_fh.id == 1) { // MILLISECONDS
              SkipStruct(in);
              unit = "ms";
            } else if (unit_fh.id == 2) { // MICROSECONDS
              SkipStruct(in);
              unit = "us";
            } else if (unit_fh.id == 3) { // NANOSECONDS
              SkipStruct(in);
              unit = "ns";
            } else {
              SkipField(in, unit_fh.type);
            }
          }
        } else {
          SkipField(in, inner.type);
        }
      }
      result = "time[" + unit + (isAdjustedToUTC ? ",UTC" : "") + "]";
      break;
    }
    case 8: { // TIMESTAMP (TimestampType)
      int16_t ts_last = 0;
      bool isAdjustedToUTC = false;
      std::string unit = "ms";
      while (true) {
        auto inner = ReadFieldHeader(in, ts_last);
        if (inner.type == 0)
          break;
        if (inner.id == 1)
          isAdjustedToUTC = ReadBool(inner.type);
        else if (inner.id == 2) { // unit
          int16_t unit_last = 0;
          while (true) {
            auto unit_fh = ReadFieldHeader(in, unit_last);
            if (unit_fh.type == 0)
              break;
            if (unit_fh.id == 1) { // MILLISECONDS
              SkipStruct(in);
              unit = "ms";
            } else if (unit_fh.id == 2) { // MICROSECONDS
              SkipStruct(in);
              unit = "us";
            } else if (unit_fh.id == 3) { // NANOSECONDS
              SkipStruct(in);
              unit = "ns";
            } else {
              SkipField(in, unit_fh.type);
            }
          }
        } else {
          SkipField(in, inner.type);
        }
      }
      result = "timestamp[" + unit + (isAdjustedToUTC ? ",UTC" : "") + "]";
      break;
    }
    case 10: { // INTEGER (IntType)
      int16_t int_last = 0;
      int8_t bitWidth = 0;
      bool isSigned = true;

      while (true) {
        auto inner = ReadFieldHeader(in, int_last);
        if (inner.type == 0)
          break; // STOP

        if (inner.id == 1) {
          // bitWidth is just a single byte
          bitWidth = static_cast<int8_t>(in.readByte());
        } else if (inner.id == 2) {
          if (inner.type == T_BOOL_TRUE) {
            isSigned = true;
          } else if (inner.type == T_BOOL_FALSE) {
            isSigned = false;
          } else {
            isSigned = ReadBool(inner.type);
          }
        } else {
          SkipField(in, inner.type); // future-proof
        }
      }

      result = (isSigned ? "int" : "uint") + std::to_string((int)bitWidth);
      break;
    }
    case 11: { // UNKNOWN (NullType - empty)
      SkipStruct(in);
      result = "unknown";
      break;
    }
    case 12: { // JSON (JsonType - empty)
      SkipStruct(in);
      result = "json";
      break;
    }
    case 13: { // BSON (BsonType - empty)
      SkipStruct(in);
      result = "bson";
      break;
    }
    case 15: {        // FLOAT16 (Float16Type - empty struct)
      SkipStruct(in); // it’s defined as an empty struct
      result = "float16";
      break;
    }
    default:
      std::cerr << "Skipping unknown logical type id " << fh.id << " type "
                << (int)fh.type << "\n";
      SkipField(in, fh.type);
      break;
    }
  }

  return result;
}

// Parse a SchemaElement
static SchemaElement ParseSchemaElement(TInput &in) {
  SchemaElement elem;
  int16_t last_id = 0;
  bool saw_physical_type = false;
  // Legacy converted_type (field 6), resolved AFTER the loop: DECIMAL needs
  // precision/scale, which arrive in later fields (7/8). -1 = absent.
  int32_t converted_type = -1;

  while (true) {
    auto fh = ReadFieldHeader(in, last_id);
    if (fh.type == 0)
      break;

    switch (fh.id) {
    case 1: { // type (Physical type)
      int32_t t = ReadI32(in);
      saw_physical_type = true;
      elem.physical_type = ParquetTypeToString(t);
      break;
    }
    case 2: { // type_length (for FIXED_LEN_BYTE_ARRAY)
      int32_t len = ReadI32(in);
      elem.type_length = len;
      break;
    }
    case 3: { // repetition_type
      elem.repetition_type = ReadI32(in);
      break;
    }
    case 4: { // name
      elem.name = ReadString(in);
      break;
    }
    case 5: { // num_children
      elem.num_children = ReadI32(in);
      break;
    }
    case 6: { // converted_type (legacy logical type) — resolved after the loop
      converted_type = ReadI32(in);
      break;
    }
    case 7: { // scale (for DECIMAL)
      int32_t scale = ReadI32(in);
      elem.scale = scale;
      break;
    }
    case 8: { // precision (for DECIMAL)
      int32_t precision = ReadI32(in);
      elem.precision = precision;
      break;
    }
    case 9: { // field_id
      int32_t field_id = ReadI32(in);
      (void)field_id;
      break;
    }
    case 10: { // logicalType (newer format)
      std::string logical = ParseLogicalType(in);
      if (!logical.empty()) {
        elem.logical_type = logical;
      }
      break;
    }
    default:
      SkipField(in, fh.type);
      break;
    }
  }

  // Legacy converted_type applies ONLY when no modern logicalType was present.
  // Deferred to here so DECIMAL can read the precision/scale fields, which are
  // parsed after converted_type. Field 10 (logicalType) always wins when set —
  // same precedence as before, just resolved later.
  if (elem.logical_type.empty() && converted_type >= 0) {
    elem.logical_type =
        ConvertedTypeToString(converted_type, elem.precision, elem.scale);
  }

  // Detect struct nodes: no physical type, has children, no logical_type
  if (elem.num_children > 0 && !saw_physical_type &&
      elem.logical_type.empty()) {
    elem.logical_type = "struct";
  }

  return elem;
}

// ------------------- Parsers -------------------

// parquet.thrift Statistics
// 1: optional binary max
// 2: optional binary min
// 3: optional i64 null_count
// 4: optional i64 distinct_count
// 5: optional binary max_value
// 6: optional binary min_value
static void ParseStatistics(TInput &in, ColumnStats &cs) {
  // Hold string_views into the (still-live) footer buffer; only the chosen
  // version is materialized as std::string at the end. Avoids allocating both
  // legacy and v2 min/max when only one is used downstream.
  std::string_view legacy_min, legacy_max, v2_min, v2_max;
  bool legacy_min_set = false;
  bool legacy_max_set = false;
  bool v2_min_set = false;
  bool v2_max_set = false;
  int16_t last_id = 0;
  while (true) {
    auto fh = ReadFieldHeader(in, last_id);
    if (fh.type == 0)
      break;
    switch (fh.id) {
    case 1:
      legacy_max = ReadStringView(in);
      legacy_max_set = true;
      break;
    case 2:
      legacy_min = ReadStringView(in);
      legacy_min_set = true;
      break;
    case 3:
      cs.null_count = ReadI64(in);
      break;
    case 4:
      cs.distinct_count = ReadI64(in);
      break;
    case 5:
      v2_max = ReadStringView(in);
      v2_max_set = true;
      break;
    case 6:
      v2_min = ReadStringView(in);
      v2_min_set = true;
      break;
    default:
      SkipField(in, fh.type);
      break;
    }
  }
  if (v2_min_set) {
    cs.min.assign(v2_min.data(), v2_min.size());
    cs.has_min = true;
  } else if (legacy_min_set) {
    cs.min.assign(legacy_min.data(), legacy_min.size());
    cs.has_min = true;
  } else {
    cs.min.clear();
    cs.has_min = false;
  }

  if (v2_max_set) {
    cs.max.assign(v2_max.data(), v2_max.size());
    cs.has_max = true;
  } else if (legacy_max_set) {
    cs.max.assign(legacy_max.data(), legacy_max.size());
    cs.has_max = true;
  } else {
    cs.max.clear();
    cs.has_max = false;
  }
}

// parquet.thrift ColumnMetaData
//  1: required Type type
//  2: required list<Encoding> encodings
//  3: required list<string> path_in_schema
//  4: required CompressionCodec codec
//  5: required i64 num_values
//  6: required i64 total_uncompressed_size
//  7: required i64 total_compressed_size
//  8: optional KeyValueMetaData key_value_metadata
//  9: optional i64 data_page_offset
// 10: optional i64 index_page_offset
// 11: optional i64 dictionary_page_offset
// 12: optional Statistics statistics
// 13: optional list<PageEncodingStats> encoding_stats
// 14+: later additions; Bloom filter fields are commonly (per spec updates):
//      14: optional i64 bloom_filter_offset
//      15: optional i64 bloom_filter_length
static void ParseColumnMeta(TInput &in, ColumnStats &cs,
                            const MetadataParseOptions &opts) {
  int16_t last_id = 0;
  while (true) {
    auto fh = ReadFieldHeader(in, last_id);
    if (fh.type == 0)
      break;

    switch (fh.id) {
    case 1: {
      int32_t t = ReadI32(in);
      cs.physical_type = ParquetTypeToString(t);
      break;
    }
    case 2: { // encodings (Thrift i32 enum — ZigZag encoded)
      auto lh = ReadListHeader(in);
      cs.encodings.reserve(lh.size);
      for (uint32_t i = 0; i < lh.size; i++) {
        int32_t enc = ReadI32(in);
        cs.encodings.push_back(enc);
      }
      break;
    }
    case 3: {
      auto lh = ReadListHeader(in);
      // Build path directly into one std::string instead of allocating a
      // temporary std::string per path component.
      std::string name;
      for (uint32_t i = 0; i < lh.size; i++) {
        std::string_view part = ReadStringView(in);
        if (!name.empty())
          name.push_back('.');
        name.append(part.data(), part.size());
      }
      cs.name = CanonicalizeColumnName(std::move(name));
      break;
    }
    case 4: {
      cs.codec = ReadI32(in);
      break;
    }
    case 5: {
      cs.num_values = ReadI64(in);
      break;
    }
    case 6: {
      cs.total_uncompressed_size = ReadI64(in);
      break;
    }
    case 7: {
      cs.total_compressed_size = ReadI64(in);
      break;
    }
    case 8: { // key_value_metadata: list<struct>
      auto lh = ReadListHeader(in);
      if (!opts.include_statistics) {
        // Skip without allocating when caller doesn't want stats-class data.
        for (uint32_t i = 0; i < lh.size; i++) {
          int16_t kv_last = 0;
          while (true) {
            auto kvfh = ReadFieldHeader(in, kv_last);
            if (kvfh.type == 0)
              break;
            SkipField(in, kvfh.type);
          }
        }
        break;
      }
      for (uint32_t i = 0; i < lh.size; i++) {
        int16_t kv_last = 0;
        std::string key, value;
        while (true) {
          auto kvfh = ReadFieldHeader(in, kv_last);
          if (kvfh.type == 0)
            break;
          switch (kvfh.id) {
          case 1:
            key = ReadString(in);
            break;
          case 2:
            value = ReadString(in);
            break;
          default:
            SkipField(in, kvfh.type);
            break;
          }
        }
        if (!key.empty()) {
          cs.key_value_metadata.emplace(std::move(key), std::move(value));
        }
      }
      break;
    }
    case 9: {
      cs.data_page_offset = ReadI64(in);
      break;
    }
    case 10: {
      cs.index_page_offset = ReadI64(in);
      break;
    }
    case 11: {
      cs.dictionary_page_offset = ReadI64(in);
      break;
    }
    case 12: {
      if (opts.include_statistics) {
        ParseStatistics(in, cs);
      } else {
        SkipField(in, fh.type);
      }
      break;
    } // statistics
    case 14: {
      if (opts.include_statistics) {
        cs.bloom_offset = ReadI64(in);
      } else {
        (void)ReadI64(in);
      }
      break;
    } // bloom_filter_offset (common)
    case 15: {
      if (opts.include_statistics) {
        cs.bloom_length = ReadI64(in);
      } else {
        (void)ReadI64(in);
      }
      break;
    } // bloom_filter_length (common)
    default:
      SkipField(in, fh.type);
      break;
    }
  }
}

// parse a ColumnChunk, and descend into meta_data when present
static void ParseColumnChunk(TInput &in, ColumnStats &out,
                             const MetadataParseOptions &opts) {
  int16_t last_id = 0;
  while (true) {
    auto fh = ReadFieldHeader(in, last_id);
    if (fh.type == 0)
      break;
    switch (fh.id) {
    case 1: {
      SkipBinary(in);
      break;
    } // file_path (always discarded)
    case 2: {
      (void)ReadI64(in); // file_offset: not used by any consumer, skip
      break;
    }
    case 3: { // meta_data (ColumnMetaData)
      ParseColumnMeta(in, out, opts);
      break;
    }
    case 4: { // offset_index_offset (i64)
      out.offset_index_offset = ReadI64(in);
      break;
    }
    case 5: { // offset_index_length (i32)
      out.offset_index_length = ReadI32(in);
      break;
    }
    case 6: { // column_index_offset (i64)
      out.column_index_offset = ReadI64(in);
      break;
    }
    case 7: { // column_index_length (i32)
      out.column_index_length = ReadI32(in);
      break;
    }
    // skip everything else
    default:
      SkipField(in, fh.type);
      break;
    }
  }
}

// A RowGroup.sorting_columns entry (parquet.thrift SortingColumn), as parsed
// off the wire before we know whether the file's creator is trusted. Applied
// to rg.columns[column_idx] only after the whole RowGroup struct has been
// read, so it does not matter whether field 4 arrives before or after field 1
// on the wire (our own writer always emits 1 then 4, but a malformed/foreign
// file is not assumed to).
struct RawSortingColumn {
  int32_t column_idx;
  bool descending;
  bool nulls_first;
};

// FIX: correct RowGroup field IDs (columns=1, total_byte_size=2, num_rows=3)
static void ParseRowGroup(TInput &in, RowGroupStats &rg,
                          const MetadataParseOptions &opts) {
  int16_t last_id = 0;
  std::vector<RawSortingColumn> raw_sorting_columns;
  while (true) {
    auto fh = ReadFieldHeader(in, last_id);
    if (fh.type == 0)
      break;

    switch (fh.id) {
    case 1: { // columns: list<ColumnChunk>
      auto lh = ReadListHeader(in);
      rg.columns.reserve(lh.size);
      for (uint32_t i = 0; i < lh.size; i++) {
        ColumnStats cs;
        ParseColumnChunk(in, cs, opts); // <-- go via ColumnChunk
        rg.columns.push_back(std::move(cs));
      }
      break;
    }
    case 2:
      rg.total_byte_size = ReadI64(in);
      break;
    case 3:
      rg.num_rows = ReadI64(in);
      break;
    case 4: { // sorting_columns: list<SortingColumn>
      auto lh = ReadListHeader(in);
      raw_sorting_columns.reserve(lh.size);
      for (uint32_t i = 0; i < lh.size; i++) {
        RawSortingColumn sc{-1, false, false};
        int16_t sc_last = 0;
        while (true) {
          auto sfh = ReadFieldHeader(in, sc_last);
          if (sfh.type == 0)
            break;
          switch (sfh.id) {
          case 1:
            sc.column_idx = ReadI32(in);
            break;
          case 2:
            sc.descending = ReadBool(sfh.type);
            break;
          case 3:
            sc.nulls_first = ReadBool(sfh.type);
            break;
          default:
            SkipField(in, sfh.type);
            break;
          }
        }
        raw_sorting_columns.push_back(sc);
      }
      break;
    }
    default:
      SkipField(in, fh.type);
      break;
    }
  }

  for (const auto &sc : raw_sorting_columns) {
    if (sc.column_idx < 0 || (size_t)sc.column_idx >= rg.columns.size())
      continue; // malformed/foreign file: out-of-range index, ignored not fatal
    ColumnStats &cs = rg.columns[(size_t)sc.column_idx];
    cs.is_sorted = true;
    cs.sort_descending = sc.descending;
    cs.sort_nulls_first = sc.nulls_first;
  }
}

// ------------------- Schema Walker -------------------

static std::vector<SchemaElement>
WalkSchema(TInput &in, int remaining, const std::string &parent_path = "") {
  std::vector<SchemaElement> nodes;
  nodes.reserve(remaining);

  for (int i = 0; i < remaining; i++) {
    SchemaElement elem = ParseSchemaElement(in);
    elem.full_name =
        parent_path.empty() ? elem.name : parent_path + "." + elem.name;

    if (elem.num_children > 0) {
      elem.children = WalkSchema(in, elem.num_children, elem.full_name);
    }

    nodes.push_back(std::move(elem));
  }
  return nodes;
}

static inline bool IsOptional(const SchemaElement &elem) {
  return elem.repetition_type == 1;
}

static std::string ResolveArrayLogicalType(const SchemaElement &elem) {
  std::string child_type = "unknown";
  if (!elem.children.empty()) {
    const SchemaElement *cur = &elem.children[0];
    while (cur) {
      if (!cur->logical_type.empty() && cur->logical_type != "struct" &&
          cur->logical_type != "array") {
        child_type = cur->logical_type;
        break;
      }
      if (!cur->physical_type.empty() && cur->logical_type.empty() &&
          cur->children.empty()) {
        child_type = cur->physical_type;
        break;
      }
      if (cur->children.empty())
        break;
      cur = &cur->children[0];
    }
  }
  return "array<" + child_type + ">";
}

static void EmitSchemaEntry(const SchemaElement &elem, bool ancestor_optional,
                            bool is_top_level,
                            std::vector<SchemaField> &columns,
                            std::unordered_map<std::string, std::string> &map) {
  const bool nullable = ancestor_optional || IsOptional(elem);
  const std::string canonical = CanonicalizeColumnName(
      elem.full_name.empty() ? elem.name : elem.full_name);

  if (elem.logical_type == "struct") {
    if (is_top_level) {
      SchemaField field;
      field.name = canonical;
      field.physical_type = "struct";
      field.logical_type = "json";
      field.nullable = nullable;
      columns.push_back(std::move(field));
    }

    map[canonical] = "json";
    if (elem.name != canonical) {
      map[elem.name] = "json";
    }

    for (const auto &child : elem.children) {
      EmitSchemaEntry(child, nullable, false, columns, map);
    }
    return;
  }

  if (elem.logical_type == "array") {
    const std::string array_type = ResolveArrayLogicalType(elem);
    if (is_top_level) {
      SchemaField field;
      field.name = canonical;
      field.physical_type = "list";
      field.logical_type = array_type;
      field.nullable = nullable;
      columns.push_back(std::move(field));
    }

    map[canonical] = array_type;
    if (elem.name != canonical) {
      map[elem.name] = array_type;
    }
    return;
  }

  std::string logical = elem.logical_type;
  if (logical.empty()) {
    if (elem.type_length > 0 && elem.physical_type == "fixed_len_byte_array") {
      logical =
          "fixed_len_byte_array[" + std::to_string(elem.type_length) + "]";
    } else if (elem.physical_type == "byte_array") {
      logical = "binary";
    } else if (elem.physical_type == "fixed_len_byte_array") {
      logical = "binary";
    } else if (!elem.physical_type.empty()) {
      logical = elem.physical_type;
    } else {
      logical = "unknown";
    }
  }

  if (is_top_level) {
    SchemaField field;
    field.name = canonical;
    field.physical_type =
        elem.physical_type.empty() ? logical : elem.physical_type;
    field.logical_type = logical;
    field.nullable = nullable;
    columns.push_back(std::move(field));
  }

  map[canonical] = logical;
  if (elem.name != canonical) {
    map[elem.name] = logical;
  }
}

static void
CollectSchemaArtifacts(const std::vector<SchemaElement> &top_level,
                       std::vector<SchemaField> &columns,
                       std::unordered_map<std::string, std::string> &map) {
  for (const auto &field : top_level) {
    EmitSchemaEntry(field, false, true, columns, map);
  }
}

// Substring both rugo-emitted created_by variants share: the opteryx_core
// wheel writes "opteryx-rugo version X.Y.Z (build N)", the standalone rugo
// wheel writes "rugo version X.Y.Z" (see build_common.py:draken_rugo_extensions).
// A substring match (not a fixed prefix) is deliberate — it survives either
// format changing around this token without the two ever needing to be kept
// in sync by hand, and no other real-world parquet writer's created_by string
// is expected to contain it.
static const char *kRugoCreatedByMarker = "rugo";

// Row-group sorting_columns is a claim the WRITER makes about its own bytes;
// unlike min/max stats (checkable via TestBloomFilter-style spot checks
// elsewhere), nothing here re-derives it from the data. So it is trusted only
// when `created_by` identifies rugo as the writer — a foreign tool's claim is
// parsed (ParseRowGroup already ran) and then discarded here, never surfaced.
static bool IsTrustedRugoWriter(const std::string &created_by) {
  return created_by.find(kRugoCreatedByMarker) != std::string::npos;
}

static FileStats ParseFileMeta(TInput &in, const MetadataParseOptions &opts) {
  FileStats fs;
  std::string created_by;

  int16_t last_id = 0;
  while (true) {
    auto fh = ReadFieldHeader(in, last_id);
    if (fh.type == 0)
      break;

    switch (fh.id) {
    case 2: { // schema (list<SchemaElement>)
      ReadListHeader(in);
      // The first SchemaElement is always the file's message-level root — a
      // Parquet-spec wrapper (pyarrow's "schema", arrow-rs's "arrow_schema",
      // Hive's "hive_schema", ...) whose own name must never appear in a
      // column's path, and nothing else about it is used. Parse it only to
      // learn how many top-level columns follow, then store those columns
      // themselves — not the wrapper — as fs.schema.
      SchemaElement msg_root = ParseSchemaElement(in);
      fs.schema = WalkSchema(in, msg_root.num_children);
      break;
    }
    case 3:
      fs.num_rows = ReadI64(in);
      break;
    case 4: { // row_groups (list<RowGroup>)
      auto lh = ReadListHeader(in);
      if (opts.schema_only) {
        for (uint32_t i = 0; i < lh.size; i++) {
          SkipStruct(in);
        }
      } else {
        uint32_t limit = lh.size;
        if (opts.max_row_groups >= 0) {
          limit = std::min<uint32_t>(
              lh.size, static_cast<uint32_t>(opts.max_row_groups));
        }
        fs.row_groups.reserve(limit);
        for (uint32_t i = 0; i < lh.size; i++) {
          if (i < limit) {
            RowGroupStats rg;
            ParseRowGroup(in, rg, opts);
            fs.row_groups.push_back(std::move(rg));
          } else {
            SkipStruct(in);
          }
        }
      }
      break;
    }
    case 6: // created_by
      created_by = ReadString(in);
      break;
    default:
      SkipField(in, fh.type);
      break;
    }
  }

  // Trust gate: sorting_columns was parsed unconditionally above (field 4
  // arrives before field 6 on the wire in every writer, including our own),
  // so it must be revoked here, after created_by is known, for any file rugo
  // did not write itself. See IsTrustedRugoWriter.
  const bool trusted = IsTrustedRugoWriter(created_by);
  for (auto &rg : fs.row_groups) {
    for (auto &col : rg.columns) {
      // Recorded for every file, not only untrusted ones: the decoder branches
      // on provenance rather than merely losing a claim (see ColumnStats).
      col.writer_is_rugo = trusted;
      if (!trusted) {
        col.is_sorted = false;
        col.sort_descending = false;
        col.sort_nulls_first = false;
      }
    }
  }
  return fs;
}
// Per-leaf schema info, in the same order as schema leaves (= row group column order).
// Built once per file; applied to every row group by index — no hash lookups.
struct LeafInfo {
  std::string logical_type;
  int32_t repetition_type  = -1;
  int32_t max_def_level    =  0;
  int32_t max_rep_level    =  0;
  int32_t type_length      =  0;
};

// Walk the schema tree once, collecting one LeafInfo per physical leaf in
// schema order.  acc_def/acc_rep carry the running Dremel level counts.
static void WalkLeaves(
    const SchemaElement &elem,
    int32_t acc_def, int32_t acc_rep,
    const std::unordered_map<std::string, std::string> &logical_type_map,
    std::vector<LeafInfo> &out)
{
  // Accumulate Dremel levels for this node.
  if (elem.repetition_type == 2) { acc_def++; acc_rep++; }
  else if (elem.repetition_type == 1) { acc_def++; }

  if (elem.children.empty()) {
    // Physical leaf: resolve logical type then emit.
    const std::string canonical = CanonicalizeColumnName(
        elem.full_name.empty() ? elem.name : elem.full_name);

    LeafInfo li;
    li.repetition_type = elem.repetition_type;
    li.max_def_level   = acc_def;
    li.max_rep_level   = acc_rep;
    li.type_length     = elem.type_length;

    auto it = logical_type_map.find(canonical);
    if (it != logical_type_map.end()) {
      li.logical_type = it->second;
    } else {
      // Fallback: derive from physical type (mirrors ApplyLogicalTypes).
      if (elem.physical_type == "int96") {
        li.logical_type = "timestamp[ns]";
      } else if (!elem.logical_type.empty()) {
        li.logical_type = elem.logical_type;
      } else if (elem.type_length > 0 &&
                 std::strcmp(elem.physical_type.c_str(), "fixed_len_byte_array") == 0) {
        li.logical_type = "fixed_len_byte_array[" +
                          std::to_string(elem.type_length) + "]";
      } else if (elem.physical_type == "byte_array" ||
                 elem.physical_type == "fixed_len_byte_array") {
        li.logical_type = "binary";
      } else if (!elem.physical_type.empty()) {
        li.logical_type = elem.physical_type;
      } else {
        li.logical_type = "unknown";
      }
    }

    out.push_back(std::move(li));
    return;
  }

  for (const auto &child : elem.children) {
    WalkLeaves(child, acc_def, acc_rep, logical_type_map, out);
  }
}

// Apply per-leaf schema info to every row group's columns by position.
// Replaces both ApplyLogicalTypes and EnrichColumnStatsWithSchemaInfo.
static void ApplyLeafInfosByIndex(FileStats &fs,
                                  const std::vector<LeafInfo> &leaf_infos) {
  if (leaf_infos.empty()) return;
  const size_t n = leaf_infos.size();

  for (auto &rg : fs.row_groups) {
    const size_t cols = std::min(rg.columns.size(), n);
    for (size_t i = 0; i < cols; ++i) {
      auto &col       = rg.columns[i];
      const auto &li  = leaf_infos[i];
      col.logical_type       = li.logical_type;
      col.repetition_type    = li.repetition_type;
      col.max_definition_level = li.max_def_level;
      col.max_repetition_level = li.max_rep_level;
      if (li.type_length > 0) col.type_length = li.type_length;
    }
  }
}

// ------------------- Entry point -------------------

FileStats ReadParquetMetadataFromBuffer(const uint8_t *buf, size_t size,
                                        const MetadataParseOptions &opts) {
  if (size < 8) {
    throw std::runtime_error("Buffer too small");
  }

  // trailer is always last 8 bytes
  const uint8_t *trailer = buf + size - 8;

  if (memcmp(trailer + 4, "PAR1", 4) != 0)
    throw std::runtime_error("Not a parquet file");

  uint32_t footer_len = ReadLE32(trailer);
  // Widen before adding: footer_len is uint32 and a crafted footer_len near
  // UINT32_MAX would wrap the sum, pass the check, and underflow footer_start
  // below into a wild pointer. Matches the file-path variant's guard.
  if (static_cast<uint64_t>(footer_len) + 8 > static_cast<uint64_t>(size))
    throw std::runtime_error("Footer length invalid");

  const uint8_t *footer_start = buf + size - 8 - footer_len;
  const uint8_t *footer_end = buf + size - 8;

  TInput in{footer_start, footer_end};
  FileStats fs = ParseFileMeta(in, opts);

  // Build schema_columns + logical_type_map from the schema tree (single pass
  // over the schema — typically 105 nodes, not 23k row-group columns).
  std::unordered_map<std::string, std::string> logical_type_map;
  if (!fs.schema.empty()) {
    CollectSchemaArtifacts(fs.schema, fs.schema_columns, logical_type_map);
  }

  // Walk schema leaves once to collect per-leaf info in schema order.
  // Then apply to all row groups by index — no hash lookups per row group.
  if (!fs.row_groups.empty() && !fs.schema.empty()) {
    std::vector<LeafInfo> leaf_infos;
    leaf_infos.reserve(fs.schema_columns.size() > 0
                           ? fs.schema_columns.size()
                           : 64);
    for (const auto &field : fs.schema) {
      WalkLeaves(field, 0, 0, logical_type_map, leaf_infos);
    }
    ApplyLeafInfosByIndex(fs, leaf_infos);
  }

  return fs;
}

FileStats ReadParquetMetadataFromBuffer(const uint8_t *buf, size_t size) {
  MetadataParseOptions opts;
  return ReadParquetMetadataFromBuffer(buf, size, opts);
}

FileStats ReadParquetMetadata(const std::string &path,
                              const MetadataParseOptions &options) {
  std::ifstream file(path, std::ios::binary);
  if (!file.is_open()) {
    throw std::runtime_error("Unable to open parquet file: " + path);
  }

  file.seekg(0, std::ios::end);
  const std::streamoff file_size = file.tellg();
  if (file_size < 8) {
    throw std::runtime_error("File too small to be a parquet file");
  }

  file.seekg(file_size - 8);
  uint8_t trailer[8];
  file.read(reinterpret_cast<char *>(trailer), 8);
  if (file.gcount() != 8) {
    throw std::runtime_error("Failed to read parquet footer");
  }

  if (std::memcmp(trailer + 4, "PAR1", 4) != 0) {
    throw std::runtime_error("Not a parquet file");
  }

  const uint32_t footer_len = ReadLE32(trailer);
  if (static_cast<uint64_t>(footer_len) + 8 >
      static_cast<uint64_t>(file_size)) {
    throw std::runtime_error("Footer length invalid");
  }

  std::vector<uint8_t> buffer(static_cast<size_t>(footer_len) + 8);
  file.seekg(file_size - 8 - footer_len);
  file.read(reinterpret_cast<char *>(buffer.data()), footer_len);
  if (file.gcount() != static_cast<std::streamsize>(footer_len)) {
    throw std::runtime_error("Failed to read parquet footer metadata");
  }

  std::memcpy(buffer.data() + footer_len, trailer, 8);

  return ReadParquetMetadataFromBuffer(buffer.data(), buffer.size(), options);
}

FileStats ReadParquetMetadata(const std::string &path) {
  MetadataParseOptions opts;
  return ReadParquetMetadata(path, opts);
}

// ------------------- AggregateColumnStats -------------------
//
// E33: does this logical type mark the column UNSIGNED? Matches the innermost
// "uint<width>" so a LIST leaf ("array<uint32>") is caught too — same rule as
// decode_column.cpp's IntType detection and rugo.parquet.decode_value.
static inline bool StatsLogicalIsUnsigned(const std::string &lt) {
  size_t pos = lt.rfind("uint");
  return pos != std::string::npos && pos + 4 < lt.size() &&
         lt[pos + 4] >= '0' && lt[pos + 4] <= '9';
}

// Compare two raw-bytes statistics values for a given physical type.
// Returns <0 if a < b, 0 if equal, >0 if a > b.
// Falls back to lexicographic for unrecognised types.
//
// An UNSIGNED column stores its magnitude in a signed int32/int64 slot, so any
// value at or above the signed midpoint has a NEGATIVE bit pattern. Comparing
// those as signed picks the WRONG winner when aggregating min-of-mins /
// max-of-maxes across row groups, which hands callers an inverted range and lets
// stats-based pruning discard files that genuinely match.
static int CompareStatBytes(const std::string &a, const std::string &b,
                             const std::string &physical_type,
                             const std::string &logical_type) {
  const bool is_unsigned = StatsLogicalIsUnsigned(logical_type);
  if (physical_type == "int32") {
    if (a.size() < 4 || b.size() < 4) return 0;
    if (is_unsigned) {
      uint32_t ua = 0, ub = 0;
      std::memcpy(&ua, a.data(), 4);
      std::memcpy(&ub, b.data(), 4);
      return (ua < ub) ? -1 : (ua > ub) ? 1 : 0;
    }
    int32_t va = 0, vb = 0;
    std::memcpy(&va, a.data(), 4);
    std::memcpy(&vb, b.data(), 4);
    return (va < vb) ? -1 : (va > vb) ? 1 : 0;
  }
  if (physical_type == "int64") {
    if (a.size() < 8 || b.size() < 8) return 0;
    if (is_unsigned) {
      uint64_t ua = 0, ub = 0;
      std::memcpy(&ua, a.data(), 8);
      std::memcpy(&ub, b.data(), 8);
      return (ua < ub) ? -1 : (ua > ub) ? 1 : 0;
    }
    int64_t va = 0, vb = 0;
    std::memcpy(&va, a.data(), 8);
    std::memcpy(&vb, b.data(), 8);
    return (va < vb) ? -1 : (va > vb) ? 1 : 0;
  }
  if (physical_type == "float32") {
    if (a.size() < 4 || b.size() < 4) return 0;
    float va = 0.0f, vb = 0.0f;
    std::memcpy(&va, a.data(), 4);
    std::memcpy(&vb, b.data(), 4);
    return (va < vb) ? -1 : (va > vb) ? 1 : 0;
  }
  if (physical_type == "float64") {
    if (a.size() < 8 || b.size() < 8) return 0;
    double va = 0.0, vb = 0.0;
    std::memcpy(&va, a.data(), 8);
    std::memcpy(&vb, b.data(), 8);
    return (va < vb) ? -1 : (va > vb) ? 1 : 0;
  }
  // byte_array, fixed_len_byte_array, boolean, int96: lexicographic
  return a.compare(b);
}

std::vector<AggColumnStat> AggregateColumnStats(const FileStats &fs) {
  // Build display-name → index map from schema_columns (top-level only).
  std::unordered_map<std::string, size_t> col_index;
  col_index.reserve(fs.schema_columns.size());
  std::vector<AggColumnStat> result;
  result.reserve(fs.schema_columns.size());

  for (size_t i = 0; i < fs.schema_columns.size(); ++i) {
    col_index[fs.schema_columns[i].name] = i;
    AggColumnStat agg;
    agg.name = fs.schema_columns[i].name;
    result.push_back(std::move(agg));
  }

  for (const auto &rg : fs.row_groups) {
    for (const auto &col : rg.columns) {
      // Map leaf path to top-level display name (everything before first dot).
      const std::string &col_name = col.name;
      size_t dot = col_name.find('.');
      std::string display = (dot == std::string::npos)
                                ? col_name
                                : col_name.substr(0, dot);

      auto it = col_index.find(display);
      if (it == col_index.end()) continue;
      AggColumnStat &agg = result[it->second];

      // Capture physical/logical type from first leaf encountered.
      if (agg.physical_type.empty() && !col.physical_type.empty()) {
        agg.physical_type = col.physical_type;
        agg.logical_type  = col.logical_type;
      }

      // Aggregate null count.
      if (agg.null_count_complete) {
        if (col.null_count >= 0) {
          agg.null_count += col.null_count;
        } else {
          agg.null_count_complete = false;
        }
      }

      // Aggregate uncompressed size (sum across row groups and, for a
      // nested column, across every leaf rolled into this display name).
      if (col.total_uncompressed_size > 0) {
        agg.total_uncompressed_size += col.total_uncompressed_size;
      }

      // Aggregate min: keep the smallest value.
      if (col.has_min) {
        if (!agg.has_min ||
            CompareStatBytes(col.min, agg.min_bytes, agg.physical_type,
                             agg.logical_type) < 0) {
          agg.min_bytes = col.min;
          agg.has_min   = true;
        }
      }

      // Aggregate max: keep the largest value.
      if (col.has_max) {
        if (!agg.has_max ||
            CompareStatBytes(col.max, agg.max_bytes, agg.physical_type,
                             agg.logical_type) > 0) {
          agg.max_bytes = col.max;
          agg.has_max   = true;
        }
      }
    }
  }

  return result;
}
