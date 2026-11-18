// RUGO-1 spike: the WASM edge of rugo's Parquet footer reader.
// Compiles rugo/src/parquet/metadata.cpp unchanged; this file only adds a C ABI
// and a JSON rendering of FileStats. All byte I/O (File.slice / HTTP Range) is
// the JS host's job — the module never sees anything but the footer bytes.
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>

#include "metadata.hpp"

namespace {

std::string g_out;  // last result; valid until the next call (single-threaded wasm32)

void json_str(std::string &o, const std::string &s) {
  o += '"';
  for (unsigned char c : s) {
    switch (c) {
      case '"': o += "\\\""; break;
      case '\\': o += "\\\\"; break;
      case '\n': o += "\\n"; break;
      case '\r': o += "\\r"; break;
      case '\t': o += "\\t"; break;
      default:
        if (c < 0x20) {
          char b[8];
          snprintf(b, sizeof b, "\\u%04x", c);
          o += b;
        } else {
          o += static_cast<char>(c);
        }
    }
  }
  o += '"';
}

void kv(std::string &o, const char *k, int64_t v) {
  o += '"'; o += k; o += "\":"; o += std::to_string(v); o += ',';
}
void kv(std::string &o, const char *k, const std::string &v) {
  o += '"'; o += k; o += "\":"; json_str(o, v); o += ',';
}
void kvb(std::string &o, const char *k, bool v) {
  o += '"'; o += k; o += "\":"; o += v ? "true" : "false"; o += ',';
}
void close(std::string &o, char c) {
  if (o.back() == ',') o.back() = c; else o += c;
}

void render(std::string &o, const FileStats &fs) {
  o += '{';
  kv(o, "num_rows", fs.num_rows);
  kv(o, "num_row_groups", static_cast<int64_t>(fs.row_groups.size()));
  o += "\"schema\":[";
  for (const auto &f : fs.schema_columns) {
    o += '{';
    kv(o, "name", f.name);
    kv(o, "physical_type", f.physical_type);
    kv(o, "logical_type", f.logical_type);
    kvb(o, "nullable", f.nullable);
    close(o, '}'); o += ',';
  }
  close(o, ']'); o += ',';
  o += "\"row_groups\":[";
  for (const auto &rg : fs.row_groups) {
    o += '{';
    kv(o, "num_rows", rg.num_rows);
    kv(o, "total_byte_size", rg.total_byte_size);
    kv(o, "total_compressed_size", rg.total_compressed_size);
    kv(o, "file_offset", rg.file_offset);
    o += "\"columns\":[";
    for (const auto &c : rg.columns) {
      o += '{';
      kv(o, "name", c.name);
      kv(o, "physical_type", c.physical_type);
      kv(o, "logical_type", c.logical_type);
      kv(o, "codec", std::string(CompressionCodecToString(c.codec)));
      o += "\"encodings\":[";
      for (int32_t e : c.encodings) { json_str(o, EncodingToString(e)); o += ','; }
      close(o, ']'); o += ',';
      kv(o, "num_values", c.num_values);
      kv(o, "total_compressed_size", c.total_compressed_size);
      kv(o, "total_uncompressed_size", c.total_uncompressed_size);
      kv(o, "null_count", c.null_count);
      kv(o, "distinct_count", c.distinct_count);
      kvb(o, "has_min", c.has_min);
      kvb(o, "has_max", c.has_max);
      kvb(o, "has_bloom_filter", c.bloom_offset >= 0);
      kvb(o, "has_page_index", c.column_index_offset >= 0);
      close(o, '}'); o += ',';
    }
    close(o, ']');
    o += "},";
  }
  close(o, ']');
  o += '}';
}

}  // namespace

extern "C" {

// Validates the 8-byte trailer ("<u32 LE footer_len>PAR1") and returns
// footer_len, or -1 when the bytes are not a parquet trailer.
int64_t rugo_footer_length(const uint8_t *trailer8) {
  if (std::memcmp(trailer8 + 4, "PAR1", 4) != 0) return -1;
  uint32_t n;
  std::memcpy(&n, trailer8, 4);  // wasm32 is little-endian
  return static_cast<int64_t>(n);
}

// `buf` holds the LAST `size` bytes of the file (footer + 8-byte trailer, or
// more). Returns 0 and a JSON document via rugo_result(), or 1 and the error.
int rugo_parse_footer(const uint8_t *buf, uint32_t size) {
  g_out.clear();
  try {
    FileStats fs = ReadParquetMetadataFromBuffer(buf, size);
    render(g_out, fs);
    return 0;
  } catch (const std::exception &e) {
    g_out = e.what();
    return 1;
  }
}

const char *rugo_result() { return g_out.c_str(); }
uint32_t rugo_result_len() { return static_cast<uint32_t>(g_out.size()); }

}  // extern "C"
