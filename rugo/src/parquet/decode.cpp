// decode.cpp
// Public API for the rugo Parquet decoder.
//
// Functions implemented here:
//   CanDecode(path), CanDecode(data, size)
//   ReadParquet(data, size, column_names), ReadParquet(data, size)
//   DecodeColumn(path, name, row_group, idx)  -- legacy file-based
//   DecodeColumn(path, name)                  -- backward-compat wrapper

#include "decode.hpp"
#include "decode_primitives.hpp"
#include "decode_page.hpp"
#include "compression.hpp"
#include "metadata.hpp"
#include "telemetry.hpp"
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <vector>
#include <string>

// ---------------------------------------------------------------------------
// CanDecode helpers
// ---------------------------------------------------------------------------

static bool CheckColumnCompatibility(const ColumnStats &col) {
  // Supported codecs: UNCOMPRESSED(0), SNAPPY(1), GZIP(2), ZSTD(6), LZ4_RAW(7).
  if (col.codec != 0 && col.codec != 1 && col.codec != 2 &&
      col.codec != 6 && col.codec != 7)
    return false;

  // int96 is the deprecated nanosecond-timestamp physical type; the decoder
  // converts it to int64 nanos (see Int96ToUnixNanos), so it decodes like an
  // int64 column from here on.
  if (col.physical_type != "int32"     && col.physical_type != "int64" &&
      col.physical_type != "byte_array" && col.physical_type != "boolean" &&
      col.physical_type != "float32"   && col.physical_type != "float64" &&
      col.physical_type != "int96"     &&
      col.physical_type != "fixed_len_byte_array") {
    return false;
  }

  // FIXED_LEN_BYTE_ARRAY is only supported for DECIMAL:
  //   width 1..8  → int64-backed DECIMAL  (precision <= 18)
  //   width 9..16 → int128-backed DECIMAL128 (precision > 18, PLAIN only;
  //                 dict-encoded p>18 is rejected inside DecodeColumnFromChunk)
  // Non-DECIMAL FLBA (UUID, fixed-width hashes, etc.) is a clean bail.
  if (col.physical_type == "fixed_len_byte_array") {
    if (col.type_length <= 0 || col.type_length > 16) return false;
    if (col.logical_type.rfind("decimal", 0) != 0) return false;
  }

  // Encodings we can actually decode, using Parquet spec IDs:
  //   0 PLAIN, 2 PLAIN_DICTIONARY, 3 RLE (boolean values / levels),
  //   5 DELTA_BINARY_PACKED, 7 DELTA_BYTE_ARRAY, 8 RLE_DICTIONARY.
  // The previous list {0,2,4,6,7} predates the ZigZag-decode fix in
  // metadata.cpp and was never renumbered: it advertised BIT_PACKED(4) and
  // DELTA_LENGTH_BYTE_ARRAY(6) — neither of which decodes — while omitting
  // DELTA_BINARY_PACKED(5) and RLE_DICTIONARY(8), which do.
  for (int32_t enc : col.encodings) {
    if (enc == 0 || enc == 2 || enc == 3 || enc == 5 || enc == 7 || enc == 8)
      return true;
  }
  return false;
}

bool CanDecode(const std::string &path) {
  try {
    FileStats metadata = ReadParquetMetadata(path);
    for (const auto &rg : metadata.row_groups) {
      for (const auto &col : rg.columns) {
        if (!CheckColumnCompatibility(col)) return false;
      }
    }
    return true;
  } catch (...) {
    return false;
  }
}

bool CanDecode(const uint8_t *data, size_t size) {
  try {
    FileStats metadata = ReadParquetMetadataFromBuffer(data, size);
    for (const auto &rg : metadata.row_groups) {
      for (const auto &col : rg.columns) {
        if (!CheckColumnCompatibility(col)) return false;
      }
    }
    return true;
  } catch (...) {
    return false;
  }
}

// ---------------------------------------------------------------------------
// ReadParquet (primary memory-based API)
// ---------------------------------------------------------------------------

DecodedTable ReadParquet(const uint8_t *data, size_t size,
                         const std::vector<std::string> &column_names) {
  static const std::vector<uint8_t> kNoMask;
  return ReadParquet(data, size, column_names, kNoMask);
}

DecodedTable ReadParquet(const uint8_t *data, size_t size,
                         const std::vector<std::string> &column_names,
                         const std::vector<uint8_t> &row_group_mask) {
  DecodedTable table;
  try {
    RUGO_TEL_START(_meta_t0);
    FileStats metadata = ReadParquetMetadataFromBuffer(data, size);
    RUGO_TEL_ACCUM(rugo_tel::metadata_ns, _meta_t0);

    table.column_names = column_names;
    table.row_groups.resize(metadata.row_groups.size());

    for (size_t rg_idx = 0; rg_idx < metadata.row_groups.size(); rg_idx++) {
      // Pushdown: skip row groups the caller pruned via footer stats. A short
      // or empty mask leaves the row group in (decode).
      if (rg_idx < row_group_mask.size() && row_group_mask[rg_idx] == 0)
        continue; // leave this row group empty

      // DecodeRowGroupColumns throws std::runtime_error, carrying the same
      // "row group N, column 'X': reason" message this used to build inline;
      // the catch below converts it to table.success=false/table.error,
      // matching the early-return-on-first-hard-error behaviour this had
      // before the row-group decode was factored out for the streaming API.
      table.row_groups[rg_idx] = DecodeRowGroupColumns(
          data, size, column_names, metadata.row_groups[rg_idx], (int)rg_idx);
    }
    table.success = true;
  } catch (const std::exception &e) {
    table.success = false;
    table.error = e.what();
  } catch (...) {
    table.success = false;
    table.error = "unknown error decoding parquet";
  }
  return table;
}

std::vector<DecodedColumn> DecodeRowGroupColumns(
    const uint8_t *data, size_t size,
    const std::vector<std::string> &column_names,
    const RowGroupStats &row_group, int row_group_index) {
  std::vector<DecodedColumn> out(column_names.size());
  for (size_t col_idx = 0; col_idx < column_names.size(); col_idx++) {
    DecodedColumn col = DecodeColumnFromMemory(
        data, size, column_names[col_idx], row_group, row_group_index);
    // A column that failed with a specific reason (decompression error,
    // corruption) is a hard error — fail loud with that reason rather than
    // silently dropping the column. A reason-less success==false (e.g. a
    // column absent from this row group) is an honest outcome this API
    // tolerates, so it does not throw.
    if (!col.success && !col.error_message.empty()) {
      throw std::runtime_error(
          "row group " + std::to_string(row_group_index) + ", column '" +
          column_names[col_idx] + "': " + col.error_message);
    }
    out[col_idx] = std::move(col);
  }
  return out;
}

DecodedTable ReadParquet(const uint8_t *data, size_t size) {
  DecodedTable table;
  try {
    FileStats metadata = ReadParquetMetadataFromBuffer(data, size);
    std::vector<std::string> all_column_names;
    if (!metadata.row_groups.empty()) {
      for (const auto &col : metadata.row_groups[0].columns) {
        all_column_names.push_back(col.name);
      }
    }
    return ReadParquet(data, size, all_column_names);
  } catch (const std::exception &e) {
    table.success = false;
    table.error = e.what();
  } catch (...) {
    table.success = false;
    table.error = "unknown error decoding parquet";
  }
  return table;
}

// ---------------------------------------------------------------------------
// DecodeColumn -- legacy file-based entry points
// ---------------------------------------------------------------------------

DecodedColumn DecodeColumn(const std::string &path,
                           const std::string &column_name,
                           const RowGroupStats &row_group,
                           int row_group_index) {
  DecodedColumn result;

  try {
    const ColumnStats *target_col = nullptr;
    for (const auto &col : row_group.columns) {
      if (col.name == column_name) { target_col = &col; break; }
    }
    if (!target_col) return result;

    bool has_supported_encoding = false;
    for (int32_t enc : target_col->encodings) {
      if (enc == 0 || enc == 4 || enc == 6 || enc == 2 || enc == 7 || enc == 8) {
        has_supported_encoding = true;
        break;
      }
    }
    if (!has_supported_encoding) return result;

    result.type = target_col->physical_type;

    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) return result;

    int64_t offset     = target_col->data_page_offset;
    int64_t total_size = target_col->total_compressed_size;
    if (offset < 0 || total_size <= 0) return result;

    file.seekg(offset);
    std::vector<uint8_t> chunk_data((size_t)total_size);
    file.read(reinterpret_cast<char *>(chunk_data.data()), total_size);
    if (file.gcount() != total_size) return result;

    // Delegate to the memory-based decoder.
    // NOTE: This legacy path passes only the column chunk as the data buffer.
    // Absolute offsets in target_col are only valid when the full file is
    // passed.  Kept as-is for backward compatibility.
    return DecodeColumnFromMemory(chunk_data.data(), chunk_data.size(),
                                  column_name, row_group, row_group_index);

  } catch (...) {
    result.success = false;
  }

  return result;
}

DecodedColumn DecodeColumn(const std::string &path,
                           const std::string &column_name) {
  try {
    FileStats metadata = ReadParquetMetadata(path);
    if (metadata.row_groups.empty()) return DecodedColumn{};
    return DecodeColumn(path, column_name, metadata.row_groups[0], 0);
  } catch (...) {
    return DecodedColumn{};
  }
}
