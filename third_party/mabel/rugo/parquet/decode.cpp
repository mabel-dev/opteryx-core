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
#include <fstream>
#include <iostream>
#include <vector>
#include <string>

// ---------------------------------------------------------------------------
// CanDecode helpers
// ---------------------------------------------------------------------------

static bool CheckColumnCompatibility(const ColumnStats &col) {
  if (col.codec != 0 && col.codec != 1 && col.codec != 6) return false;

  if (col.physical_type != "int32"     && col.physical_type != "int64" &&
      col.physical_type != "byte_array" && col.physical_type != "boolean" &&
      col.physical_type != "float32"   && col.physical_type != "float64") {
    return false;
  }

  for (int32_t enc : col.encodings) {
    if (enc == 0 || enc == 2 || enc == 4 || enc == 6 || enc == 7) return true;
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
  DecodedTable table;
  try {
    FileStats metadata = ReadParquetMetadataFromBuffer(data, size);

    table.column_names = column_names;
    table.row_groups.resize(metadata.row_groups.size());

    for (size_t rg_idx = 0; rg_idx < metadata.row_groups.size(); rg_idx++) {
      const RowGroupStats &row_group = metadata.row_groups[rg_idx];
      table.row_groups[rg_idx].resize(column_names.size());

      for (size_t col_idx = 0; col_idx < column_names.size(); col_idx++) {
        table.row_groups[rg_idx][col_idx] = DecodeColumnFromMemory(
            data, size, column_names[col_idx], row_group, (int)rg_idx);
      }
    }
    table.success = true;
  } catch (...) {
    table.success = false;
  }
  return table;
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
  } catch (...) {
    table.success = false;
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
