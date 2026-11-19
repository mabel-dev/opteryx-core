// The WASM edge of rugo's Parquet reader: footer (metadata.cpp) and column-chunk
// decode (decode_column.cpp + column_materialize.cpp), all compiled unchanged; this
// file only adds a C ABI and a JSON rendering of FileStats. All byte I/O
// (File.slice / HTTP Range) is the JS host's job — the module only ever sees the
// footer bytes and the column chunks the host chose to fetch.
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "avro/avro_reader.hpp"
#include "column_materialize.hpp"
#include "decode.hpp"
#include "ipc_serialize.hpp"
#include "metadata.hpp"
#include "orc/orc_reader.hpp"

namespace {

std::string g_out;  // last result; valid until the next call (single-threaded wasm32)
rugo::_parquet::MaterializedColumn g_col;  // last decoded column; freed by rugo_column_release
uint32_t g_col_info[8];
std::vector<uint8_t> g_list;  // last decoded LIST column, in the engine's TAG_ARRAY wire format
uint32_t g_list_info[3];
rugo::avro::AvroBatch g_batch;  // last decoded ORC stripe / Avro batch
uint32_t g_avcol_info[17];

// An ORC / Avro file held whole in the wasm heap (the native readers address the
// whole file; Range reads would need a reader change). Owns `buf` (malloc'd by JS).
struct WasmOrc {
  uint8_t *buf = nullptr;
  size_t n = 0;
  std::unique_ptr<rugo::orc::OrcFile> file;
  std::string bound;  // the column list `file` is bound to; "\x01" = all
  ~WasmOrc() { std::free(buf); }
};
struct WasmAvro {
  uint8_t *buf = nullptr;
  size_t n = 0;
  std::unique_ptr<rugo::avro::AvroFile> meta;  // every column: header, schema, framing
  std::unique_ptr<rugo::avro::AvroFile> proj;  // the projection last decoded
  std::unique_ptr<rugo::avro::AvroFile::Worker> worker;
  std::string key;
  ~WasmAvro() { worker.reset(); proj.reset(); meta.reset(); std::free(buf); }
};

// "a\0b\0" -> {"a", "b"}
std::vector<std::string> split_names(const char *names, uint32_t len) {
  std::vector<std::string> out;
  for (uint32_t at = 0; at < len;) {
    const size_t n = std::strlen(names + at);
    out.emplace_back(names + at, n);
    at += static_cast<uint32_t>(n) + 1;
  }
  return out;
}

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

// ── ORC / Avro: the column carrier both readers produce (avro_reader.hpp AvroColumn) ──

uint32_t rugo_batch_rows() { return g_batch.rows; }
uint32_t rugo_batch_width() { return static_cast<uint32_t>(g_batch.columns.size()); }
const rugo::avro::AvroColumn *rugo_batch_column(uint32_t i) { return &g_batch.columns[i]; }
void rugo_batch_release() { g_batch = rugo::avro::AvroBatch{}; }

// [kind, type, length, data, validity, slots, arena, arena_len, codes, dict_len, precision,
//  scale, offsets, child_validity, child_type, child_length, child_col]. Static: copy it out.
const uint32_t *rugo_avcol_info(const rugo::avro::AvroColumn *c) {
  auto p = [](const void *v) { return static_cast<uint32_t>(reinterpret_cast<uintptr_t>(v)); };
  const uint32_t v[17] = {static_cast<uint32_t>(c->kind), static_cast<uint32_t>(c->type), c->length,
                          p(c->data), p(c->validity), p(c->slots), p(c->arena),
                          static_cast<uint32_t>(c->arena_len), p(c->codes), c->dict_len, c->precision,
                          c->scale, p(c->offsets), p(c->child_validity),
                          static_cast<uint32_t>(c->child_type), c->child_length, p(c->child_col.get())};
  std::memcpy(g_avcol_info, v, sizeof v);
  return g_avcol_info;
}

// Takes ownership of `buf` (the whole file, malloc'd). Returns a handle with the file's
// description as JSON via rugo_result(), or 0 with the error there (and `buf` freed).
WasmOrc *rugo_orc_open(uint8_t *buf, uint32_t size) {
  auto *h = new WasmOrc;
  h->buf = buf;
  h->n = size;
  g_out.clear();
  try {
    h->file = std::make_unique<rugo::orc::OrcFile>(buf, size);
    const auto &f = *h->file;
    std::string &o = g_out;
    o += '{';
    kv(o, "num_rows", static_cast<int64_t>(f.num_rows()));
    kv(o, "compression", f.compression_name());
    o += "\"stripes\":[";
    for (size_t i = 0; i < f.stripe_count(); ++i) { o += std::to_string(f.stripe_rows(i)); o += ','; }
    close(o, ']'); o += ',';
    o += "\"columns\":[";
    for (const auto &c : f.file_columns()) {
      o += '{';
      kv(o, "name", c.name);
      kv(o, "kind", c.kind_name);
      kvb(o, "readable", c.readable);
      if (!c.readable) kv(o, "refusal", c.refusal);
      kvb(o, "nested", c.nested);
      close(o, '}'); o += ',';
    }
    close(o, ']');
    o += '}';
    return h;
  } catch (const std::exception &e) {
    g_out = e.what();
    delete h;
    return nullptr;
  }
}

void rugo_orc_close(WasmOrc *h) { delete h; }

// Decodes stripe `stripe`: every column when `len` == 0, else the NUL-separated names.
// 0 = batch via rugo_batch_*; 1 = error via rugo_result().
int rugo_orc_decode(WasmOrc *h, uint32_t stripe, const char *names, uint32_t len) {
  g_batch = rugo::avro::AvroBatch{};
  g_out.clear();
  try {
    const std::string key = len == 0 ? std::string("\x01") : std::string(names, len);
    if (key != h->bound) {
      if (len == 0) {
        h->file->bind_all();
      } else {
        // Each file column read as it states itself (the pyx edge's read_orc, by name).
        std::vector<rugo::orc::ReaderColumn> reader;
        for (const auto &c : h->file->file_columns()) {
          rugo::orc::ReaderColumn rc;
          rc.name = c.name;
          rc.type = c.type;
          rc.precision = c.precision;
          rc.scale = c.scale;
          rc.child_type = c.child_type;
          rc.array_depth = c.array_depth;
          reader.push_back(rc);
        }
        h->file->bind(reader, split_names(names, len), false);
      }
      h->bound = key;
    }
    g_batch = h->file->decode_stripe(stripe);
    return 0;
  } catch (const std::exception &e) {
    h->bound.clear();
    g_out = e.what();
    return 1;
  }
}

// Takes ownership of `buf` (the whole container file, malloc'd). Returns a handle with
// {schema, codec, columns, batches} as JSON via rugo_result(), or 0 with the error there.
WasmAvro *rugo_avro_open(uint8_t *buf, uint32_t size) {
  auto *h = new WasmAvro;
  h->buf = buf;
  h->n = size;
  g_out.clear();
  try {
    h->meta = std::make_unique<rugo::avro::AvroFile>(buf, size, std::vector<std::string>{}, true, std::string());
    const auto &f = *h->meta;
    std::string &o = g_out;
    o += "{\"schema\":";
    o += f.schema_json();  // the writer schema, itself JSON
    o += ',';
    std::string codec = "null";
    for (const auto &kvp : f.metadata()) if (kvp.first == "avro.codec") codec = kvp.second;
    kv(o, "codec", codec);
    o += "\"columns\":[";
    for (const auto &c : f.column_names()) { json_str(o, c); o += ','; }
    close(o, ']'); o += ',';
    int64_t rows = 0;
    o += "\"batches\":[";
    for (const auto &b : f.batches()) { o += std::to_string(b.rows); o += ','; rows += b.rows; }
    close(o, ']'); o += ',';
    kv(o, "num_rows", rows);
    close(o, '}');
    return h;
  } catch (const std::exception &e) {
    g_out = e.what();
    delete h;
    return nullptr;
  }
}

void rugo_avro_close(WasmAvro *h) { delete h; }

// Decodes batch `batch` (whole blocks, up to 65,536 rows): every top-level field when
// `len` == 0, else the NUL-separated (dotted) names. 0 = batch; 1 = error.
int rugo_avro_decode(WasmAvro *h, uint32_t batch, const char *names, uint32_t len) {
  g_batch = rugo::avro::AvroBatch{};
  g_out.clear();
  try {
    const std::string key = len == 0 ? std::string("\x01") : std::string(names, len);
    if (key != h->key || !h->worker) {
      h->worker.reset();
      h->proj = len == 0 ? std::make_unique<rugo::avro::AvroFile>(h->buf, h->n, std::vector<std::string>{}, true, std::string())
                         : std::make_unique<rugo::avro::AvroFile>(h->buf, h->n, split_names(names, len), false, std::string());
      h->worker = std::make_unique<rugo::avro::AvroFile::Worker>(*h->proj);
      h->key = key;
    }
    g_batch = h->worker->decode(h->proj->batches()[batch]);
    return 0;
  } catch (const std::exception &e) {
    h->worker.reset();
    h->key.clear();
    g_out = e.what();
    return 1;
  }
}

// Validates the 8-byte trailer ("<u32 LE footer_len>PAR1") and returns
// footer_len, or -1 when the bytes are not a parquet trailer.
int64_t rugo_footer_length(const uint8_t *trailer8) {
  if (std::memcmp(trailer8 + 4, "PAR1", 4) != 0) return -1;
  uint32_t n;
  std::memcpy(&n, trailer8, 4);  // wasm32 is little-endian
  return static_cast<int64_t>(n);
}

// `buf` holds the LAST `size` bytes of the file (footer + 8-byte trailer, or
// more). Returns a handle (the parsed footer, kept for rugo_decode_column) with
// the JSON document via rugo_result(), or 0 with the error via rugo_result().
// Free with rugo_close.
FileStats *rugo_open(const uint8_t *buf, uint32_t size) {
  g_out.clear();
  try {
    auto *fs = new FileStats(ReadParquetMetadataFromBuffer(buf, size));
    render(g_out, *fs);
    return fs;
  } catch (const std::exception &e) {
    g_out = e.what();
    return nullptr;
  }
}

void rugo_close(FileStats *fs) { delete fs; }

// The column chunk's byte range [offset, offset + length) in the file: from its
// first page (the dictionary page when it has one) for total_compressed_size bytes.
// Doubles: file offsets past 4 GB, exact to 2^53. The caller bounds-checks rg/col.
double rugo_chunk_offset(const FileStats *fs, int32_t rg, int32_t col) {
  const ColumnStats &c = fs->row_groups[rg].columns[col];
  int64_t base = c.data_page_offset;
  if (c.dictionary_page_offset >= 0 && c.dictionary_page_offset < base) base = c.dictionary_page_offset;
  return static_cast<double>(base);
}
double rugo_chunk_length(const FileStats *fs, int32_t rg, int32_t col) {
  return static_cast<double>(fs->row_groups[rg].columns[col].total_compressed_size);
}

// Decodes one column chunk (exactly the bytes of rugo_chunk_offset/length).
// A scalar column is materialized the way the native reader does
// (materialize_scalar): returns 0, column via rugo_column_info(). A LIST column
// is serialized by the engine's own list builder (ipc_serialize.hpp, TAG_ARRAY):
// returns 2, bytes via rugo_list_info(). 1 = error via rugo_result(). Either
// result stays valid until rugo_column_release() or the next decode.
int rugo_decode_column(const FileStats *fs, int32_t rg, int32_t col,
                       const uint8_t *chunk, uint32_t size) {
  rugo::_parquet::release_materialized(g_col);
  g_list.clear();
  g_out.clear();
  try {
    const RowGroupStats &g = fs->row_groups[rg];
    ColumnStats adjusted = g.columns[col];  // chunk-relative, as io_pipeline hands it over
    const int64_t base = static_cast<int64_t>(rugo_chunk_offset(fs, rg, col));
    adjusted.data_page_offset -= base;
    if (adjusted.dictionary_page_offset >= 0) adjusted.dictionary_page_offset -= base;
    DecodedColumn dc;
    DecodeColumnFromChunk(dc, chunk, size, &adjusted, static_cast<const uint8_t *>(nullptr));
    if (!dc.success) {
      g_out = "column '" + adjusted.name + "': " +
              (dc.error_message.empty() ? std::string("decode failed (no reason recorded)") : dc.error_message);
      return 1;
    }
    if (dc.max_rep_level > 0) {
      rugo::serialize_decoded_column(dc, g_list);
      g_list_info[0] = static_cast<uint32_t>(reinterpret_cast<uintptr_t>(g_list.data()));
      g_list_info[1] = static_cast<uint32_t>(g_list.size());
      g_list_info[2] = rugo::_parquet::logical_is_string(dc.logical_type) ? 1u : 0u;
      return 2;
    }
    if (dc.num_rows != g.num_rows) {
      g_out = "column '" + adjusted.name + "': decoded " + std::to_string(dc.num_rows) +
              " rows, row group has " + std::to_string(g.num_rows);
      return 1;
    }
    if (!rugo::_parquet::materialize_scalar(dc, dc.num_rows, g_col)) {
      g_out = "column '" + adjusted.name + "': " +
              "unsupported type " + dc.type;
      return 1;
    }
    return 0;
  } catch (const std::exception &e) {
    g_out = e.what();
    return 1;
  }
}

// [DrakenType, length, data, validity (0 = all valid), arena, arena_len, precision, scale]
const uint32_t *rugo_column_info() {
  g_col_info[0] = static_cast<uint32_t>(g_col.type);
  g_col_info[1] = g_col.length;
  g_col_info[2] = reinterpret_cast<uintptr_t>(g_col.data);
  g_col_info[3] = reinterpret_cast<uintptr_t>(g_col.validity);
  g_col_info[4] = reinterpret_cast<uintptr_t>(g_col.arena);
  g_col_info[5] = static_cast<uint32_t>(g_col.arena_len);
  g_col_info[6] = g_col.precision;
  g_col_info[7] = g_col.scale;
  return g_col_info;
}

// [wire bytes, wire length, leaf is text (1) or binary (0) for string leaves]
const uint32_t *rugo_list_info() { return g_list_info; }

void rugo_column_release() {
  rugo::_parquet::release_materialized(g_col);
  g_list.clear();
}

const char *rugo_result() { return g_out.c_str(); }
uint32_t rugo_result_len() { return static_cast<uint32_t>(g_out.size()); }

}  // extern "C"
