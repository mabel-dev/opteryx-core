#pragma once
// ---------------------------------------------------------------------------
// Parquet column patcher
//
// Rewrites a parquet file's SHAPE - add, drop, rename a column - without
// decoding a single value of the columns it does not touch. Parquet keeps the
// schema and the per-chunk byte offsets in a footer at the end of the file,
// separate from the encoded pages, so a column change is:
//
//     [PAR1][pages we copy verbatim][pages we add][new footer][len][PAR1]
//
// A surviving column's pages are `memcpy`d from the source and re-pointed by
// the new footer. Nothing is decompressed, no value is decoded, and the bytes
// on the other side are bit-identical - which is the property
// tests/storage/test_ddl_column_operations.py pins directly, because a
// decode-and-re-encode implementation produces equal VALUES and different
// BYTES and would otherwise look correct.
//
// This never mutates the source. The caller writes the result to a NEW path and
// commits it in a new snapshot, so older snapshots keep pointing at the bytes
// they were written against and time travel still answers correctly.
//
// The declared shape comes from the CALLER (the relation's schema), not from
// re-parsing the source's type strings back into writer types - that round trip
// is lossy for exactly the parameterized types (DECIMAL width/scale, TIMESTAMP
// unit, narrow/unsigned ints) that would then be silently mislabelled. The
// caller's declaration is checked against the source's physical type per chunk
// and disagreement is fatal, so a schema that has drifted from its own files
// fails loudly here instead of producing a file whose footer lies about its
// bytes.
// ---------------------------------------------------------------------------

#include "_parquet_writer.hpp"
#include "decode.hpp"
#include "decode_page.hpp"
#include "metadata.hpp"
#include "thrift.hpp"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

namespace rugo_pq_write {

// The parquet physical type name the READER reports for a writer PType. The two
// vocabularies have to be compared to check a declaration against real bytes;
// this is the one place they meet (mirrors ParquetTypeToString in metadata.cpp).
inline const char *ptype_reader_name(PType t) {
  switch (t) {
  case PT_BOOLEAN:    return "boolean";
  case PT_INT32:      return "int32";
  case PT_INT64:      return "int64";
  case PT_FLOAT:      return "float32";
  case PT_DOUBLE:     return "float64";
  case PT_BYTE_ARRAY: return "byte_array";
  case PT_FLBA:       return "fixed_len_byte_array";
  }
  return "unknown";
}

inline PType ptype_from_reader_name(const std::string &s) {
  if (s == "boolean")                return PT_BOOLEAN;
  if (s == "int32")                  return PT_INT32;
  if (s == "int64")                  return PT_INT64;
  if (s == "float32")                return PT_FLOAT;
  if (s == "float64")                return PT_DOUBLE;
  if (s == "byte_array")             return PT_BYTE_ARRAY;
  if (s == "fixed_len_byte_array")   return PT_FLBA;
  throw std::runtime_error("parquet patch: unsupported physical type '" + s + "'");
}

// The TimeUnit member id behind a "...[ms]" / "...[us]" / "...[ns]" suffix.
inline int ts_unit_from_logical(const std::string &lt) {
  if (lt.find("[ms") != std::string::npos)  return TU_MILLIS;
  if (lt.find("[us") != std::string::npos)  return TU_MICROS;
  if (lt.find("[ns") != std::string::npos)  return TU_NANOS;
  throw std::runtime_error("parquet patch: unreadable time unit in '" + lt + "'");
}

// Walk a LIST column's schema subtree down to its leaf, counting nesting depth.
//
// The only nesting this can re-declare is the all-nullable scheme the writer
// emits (write_schema in _parquet_writer.hpp):
//
//     name(OPTIONAL, LIST) -> "list"(REPEATED) -> element(OPTIONAL)
//
// giving max_rep == depth and max_def == 2*depth + 1. That matters far more
// here than it would in a decoder: the repetition types in the schema are what
// tell a reader how to interpret the definition levels stored in the pages
// being COPIED. A source using any other nullability - a REQUIRED element, or
// the legacy 2-level Hive/Avro list - encodes different level values in those
// very bytes, so re-declaring them under this scheme would make every row read
// back wrong while the file still looked well-formed. Refuse instead.
inline const SchemaElement *list_leaf_and_depth(const SchemaElement &e, int &depth) {
  depth = 0;
  const SchemaElement *node = &e;
  while (node->logical_type == "array") {
    if (node->repetition_type != REP_OPTIONAL || node->children.size() != 1)
      throw std::runtime_error(
          "parquet patch: column '" + e.name +
          "' is a LIST whose group is not the OPTIONAL single-child form this "
          "patcher can re-declare");
    const SchemaElement &repeated = node->children[0];
    if (repeated.repetition_type != REP_REPEATED || repeated.children.size() != 1)
      throw std::runtime_error(
          "parquet patch: column '" + e.name +
          "' is a LIST without the REPEATED single-child group the 3-level "
          "list encoding requires");
    node = &repeated.children[0];
    depth++;
  }
  if (!node->children.empty())
    throw std::runtime_error(
        "parquet patch: column '" + e.name +
        "' is a LIST of STRUCT; patching struct columns is not supported yet");
  if (node->repetition_type != REP_OPTIONAL)
    throw std::runtime_error(
        "parquet patch: column '" + e.name +
        "' is a LIST with a non-OPTIONAL element; its definition levels do not "
        "match the all-nullable scheme this patcher re-declares");
  return node;
}

// Rebuild a column's writer-side SHAPE from the source file's own schema.
//
// The source describes the very bytes being copied, so it - not a caller's
// declaration - is the authority on what they are. Anything this cannot
// represent EXACTLY throws: emitting an approximate annotation would relabel
// real data, which is a wrong answer rather than a missing optimisation.
inline ColumnInput shape_from_schema_element(const SchemaElement &e) {
  ColumnInput ci;
  ci.name = e.name;

  if (e.logical_type == "array") {
    // A LIST is one leaf column chunk however deep it nests, so its pages copy
    // exactly like a primitive's - only the schema subtree and the chunk's
    // num_values (levels, not rows) differ, and both are reproduced from the
    // source. DROP and RENAME therefore cost a LIST column nothing to carry.
    const SchemaElement *leaf = list_leaf_and_depth(e, ci.array_depth);
    ci.is_array = true;
    ci.elem_type = ptype_from_reader_name(leaf->physical_type);
    // Everything that consumes an array shape reads elem_type (`is_array ?
    // elem_type : type`); keeping the two agreeing means a stray read of
    // `type` cannot disagree with the bytes.
    ci.type = ci.elem_type;

    // The writer annotates an array leaf with UTF8 or INTEGER(width, signed)
    // and nothing else - see write_schema's is_array branch, which emits no
    // type_length, scale, precision or time unit. So a DECIMAL, DATE,
    // TIMESTAMP or FLBA leaf has no exact re-declaration available here.
    const std::string &elt = leaf->logical_type;
    if (elt.empty() || elt == leaf->physical_type) {
      // A bare physical leaf says everything there is to say.
    } else if (elt == "varchar" || elt == "utf8" || elt == "string") {
      ci.elem_is_utf8 = true;
    } else if (elt.rfind("uint", 0) == 0 || elt.rfind("int", 0) == 0) {
      ci.is_unsigned = (elt[0] == 'u');
      ci.int_bit_width = std::stoi(elt.substr(ci.is_unsigned ? 4 : 3));
      if (ci.int_bit_width != 8 && ci.int_bit_width != 16 &&
          ci.int_bit_width != 32 && ci.int_bit_width != 64)
        throw std::runtime_error(
            "parquet patch: column '" + e.name + "' has a list element of width " +
            std::to_string(ci.int_bit_width) + ", which is not a parquet integer width");
    } else {
      throw std::runtime_error(
          "parquet patch: column '" + e.name + "' has a list element carrying "
          "logical type '" + elt + "' which this patcher cannot reproduce exactly");
    }
    return ci;
  }

  if (!e.children.empty())
    throw std::runtime_error(
        "parquet patch: column '" + e.name +
        "' is a STRUCT; patching struct columns is not supported yet");

  ci.type = ptype_from_reader_name(e.physical_type);

  const std::string &lt = e.logical_type;

  if (lt.empty() || lt == e.physical_type) {
    // A bare physical type says everything there is to say.
  } else if (lt == "varchar" || lt == "utf8" || lt == "string") {
    ci.is_utf8 = true;
  } else if (lt.rfind("decimal(", 0) == 0) {
    ci.logical = LK_DECIMAL;
    ci.dec_precision = e.precision;
    ci.dec_scale = e.scale;
    ci.dec_width = e.type_length;  // FLBA width; 0 for int-backed decimals
  } else if (lt.rfind("date32", 0) == 0 || lt == "date") {
    ci.logical = LK_DATE;
  } else if (lt.rfind("timestamp", 0) == 0) {
    ci.logical = LK_TIMESTAMP;
    ci.ts_unit = ts_unit_from_logical(lt);
    ci.ts_utc = lt.find("UTC") != std::string::npos;
  } else if (lt.rfind("time[", 0) == 0) {
    ci.logical = LK_TIME;
    ci.ts_unit = ts_unit_from_logical(lt);
  } else if (lt == "interval") {
    ci.logical = LK_INTERVAL;
  } else if (lt.rfind("int", 0) == 0 || lt.rfind("uint", 0) == 0) {
    // INTEGER(bitWidth, isSigned) - how every width narrower than the physical
    // type, and unsignedness at any width, is carried.
    ci.is_unsigned = (lt[0] == 'u');
    ci.int_bit_width = std::stoi(lt.substr(ci.is_unsigned ? 4 : 3));
  } else {
    throw std::runtime_error(
        "parquet patch: column '" + e.name + "' carries logical type '" + lt +
        "' which this patcher cannot reproduce exactly");
  }
  return ci;
}

// ---------------------------------------------------------------------------
// Added columns
//
// An ADDed column has no pages to copy, so its chunk is synthesised: one value
// repeated for every existing row. What that value is, and what type it is
// annotated as, both come from a DONOR - a complete one-column, one-row parquet
// file the caller produced with rugo's own writer.
//
// The donor exists so this file does not need its own copy of the
// DrakenType -> parquet type/annotation mapping. That mapping is large
// (widths, signedness, decimal precision/scale, timestamp/time units, UTF8
// annotation) and a second copy of it would drift from the write path, at which
// point an ADDed column would be annotated differently from the identical
// column written by a CTAS. Round-tripping through a file the write path
// produced makes drift impossible: the annotation IS the write path's.
//
// The donor is required to be PLAIN and uncompressed so the value can be lifted
// out of its data page without a decoder - the caller writes it that way.
// ---------------------------------------------------------------------------

// One value, plus the shape to annotate it with. Backing storage for whatever
// the value is lives here, so a ColumnInput built from this stays valid for as
// long as this object does.
struct ConstantColumn {
  ColumnInput shape;
  bool is_null = false;  // fill existing rows with NULL rather than a value

  int32_t v_i32 = 0;
  int64_t v_i64 = 0;
  double v_f64 = 0.0;
  uint8_t v_bool = 0;
  // BYTE_ARRAY payload, or a DECIMAL/INTERVAL FLBA value in the
  // NATIVE-endian form ColumnInput::dec_raw expects (parquet stores it
  // big-endian, so decimals are byte-reversed on the way in).
  std::vector<uint8_t> v_bytes;
  StrSlice v_str{nullptr, 0};
};

// Whether a repeated value of this physical type can be written as a
// one-entry dictionary (PRESERVE mode: `codes` + `dict_count`).
//
// Only these four honour `codes` in encode_values/compute_stats - which is
// also exactly the set the writer's own auto-dictionary covers, so this is not
// a limitation this file invents. BOOLEAN and FLBA index their buffers by ROW
// there, so handing them codes would read past a one-entry buffer. They take
// the plain path below instead: a full-length repeated buffer, which costs
// transient memory but compresses to nothing.
inline bool ptype_takes_dict_codes(PType t) {
  return t == PT_INT32 || t == PT_INT64 || t == PT_DOUBLE || t == PT_BYTE_ARRAY;
}

// Read a donor file: its column's shape, and the single value it holds.
inline ConstantColumn parse_donor(const uint8_t *src, size_t src_len) {
  if (src == nullptr || src_len < 8)
    throw std::runtime_error("parquet patch: donor is too small to be a parquet file");

  MetadataParseOptions opts;
  opts.include_statistics = true;
  FileStats fs = ReadParquetMetadataFromBuffer(src, src_len, opts);

  if (fs.schema.size() != 1)
    throw std::runtime_error("parquet patch: donor must hold exactly one column");
  if (fs.row_groups.size() != 1 || fs.num_rows != 1)
    throw std::runtime_error("parquet patch: donor must hold exactly one row");

  ConstantColumn cc;
  cc.shape = shape_from_schema_element(fs.schema[0]);
  // A LIST shape is readable from a schema (the copy path needs it) but not
  // fillable: the synthesis below writes one repeated primitive value, and a
  // list column would additionally need rep/def levels and element offsets that
  // no donor carries.
  if (cc.shape.is_array)
    throw std::runtime_error(
        "parquet patch: donor column is a LIST; an ADDed column must be a primitive");
  cc.shape.bloom = false;
  cc.shape.dict_enabled = false;

  const ::ColumnStats &cs = fs.row_groups[0].columns[0];
  if (cs.null_count < 0)
    throw std::runtime_error("parquet patch: donor carries no null_count");
  cc.is_null = (cs.null_count == 1);
  if (cc.is_null)
    return cc;  // no value to read; the fill is NULL

  if (cs.codec != CODEC_UNCOMPRESSED)
    throw std::runtime_error("parquet patch: donor must be written uncompressed");
  if (cs.dictionary_page_offset >= 0)
    throw std::runtime_error("parquet patch: donor must be written without a dictionary");
  if (cs.data_page_offset < 0 ||
      (uint64_t)cs.data_page_offset >= (uint64_t)src_len)
    throw std::runtime_error("parquet patch: donor data page offset is out of range");

  TInput in{src + cs.data_page_offset, src + src_len};
  PageHeader ph = ParsePageHeader(in);
  if (ph.page_type != PAGE_DATA || ph.is_v2 || ph.encoding != ENC_PLAIN ||
      ph.num_values != 1)
    throw std::runtime_error(
        "parquet patch: donor must hold a single PLAIN v1 data page");

  // body = [4-byte LE def-rle length][def rle][PLAIN value]
  const uint8_t *body = in.p;
  if ((size_t)(in.end - body) < (size_t)ph.compressed_page_size)
    throw std::runtime_error("parquet patch: donor data page runs past the end of the file");
  const uint8_t *body_end = body + ph.compressed_page_size;
  if (body_end - body < 4)
    throw std::runtime_error("parquet patch: donor data page is truncated");
  uint32_t def_len;
  std::memcpy(&def_len, body, 4);
  const uint8_t *val = body + 4 + def_len;
  if (val > body_end)
    throw std::runtime_error("parquet patch: donor definition levels run past its page");

  const size_t avail = (size_t)(body_end - val);
  auto need = [&](size_t n) {
    if (avail < n)
      throw std::runtime_error("parquet patch: donor value is truncated");
  };

  switch (cc.shape.type) {
  case PT_INT32:
    need(4);
    std::memcpy(&cc.v_i32, val, 4);
    break;
  case PT_INT64:
    need(8);
    std::memcpy(&cc.v_i64, val, 8);
    break;
  case PT_DOUBLE:
    need(8);
    std::memcpy(&cc.v_f64, val, 8);
    break;
  case PT_BOOLEAN:
    need(1);
    cc.v_bool = (uint8_t)(val[0] & 1);  // PLAIN booleans are bit-packed, LSB first
    break;
  case PT_BYTE_ARRAY: {
    need(4);
    uint32_t n;
    std::memcpy(&n, val, 4);
    need(4 + (size_t)n);
    cc.v_bytes.assign(val + 4, val + 4 + n);
    // Points into v_bytes, whose heap buffer survives this object being moved
    // into the caller's list.
    cc.v_str.ptr = cc.v_bytes.data();
    cc.v_str.len = n;
    break;
  }
  case PT_FLBA: {
    const size_t w = (size_t)cc.shape.dec_width;
    if (w == 0)
      throw std::runtime_error("parquet patch: donor FIXED_LEN_BYTE_ARRAY has no width");
    need(w);
    // INTERVAL is stored verbatim; DECIMAL is stored big-endian on disk and
    // dec_raw wants it native-endian, so it is reversed back here.
    cc.v_bytes.assign(val, val + w);
    if (cc.shape.logical != LK_INTERVAL)
      std::reverse(cc.v_bytes.begin(), cc.v_bytes.end());
    break;
  }
  default:
    throw std::runtime_error("parquet patch: donor has an unsupported physical type");
  }
  return cc;
}

// ---------------------------------------------------------------------------
// Retyped columns
//
// A widening only reaches here when parquet's PHYSICAL type has to change,
// which in the widening lattice means exactly one transition: physical INT32
// (which carries declared INT8/INT16/INT32 and UINT8/UINT16/UINT32) to
// physical INT64. Every other legal widening is an annotation change over
// unchanged bytes - INT8 -> INT32 stays physical INT32, and FLOAT32 is already
// written as physical float64 - so those go the copy-the-pages path and never
// get here.
//
// This is the one place in the patcher that decodes anything. It decodes ONE
// column of ONE row group; every other column is still copied byte for byte.
// ---------------------------------------------------------------------------

// The dictionary code for logical row `row` / present-index `vi`, or the dense
// value, resolved to int64 in the column's DECLARED domain.
//
// Mirrors rugo's own reader (`_int64_list` in parquet_reader.pxi) shape for
// shape - dictionary with packed codes, dictionary with per-present indices,
// RLE runs, dense - because a fifth interpretation of a decoded column is
// exactly how a silently wrong value gets written.
inline void widen_int32_chunk_to_int64(const DecodedColumn &dc, size_t rg_rows,
                                       std::vector<int64_t> &out) {
  out.assign(rg_rows, 0);

  // An unsigned column keeps its magnitude in a signed slot, so it is widened
  // by zero-extension. A plain int64 cast would sign-extend and turn a uint32
  // of 4e9 into -294967296 - a wrong VALUE, silently written.
  const bool uns = dc.is_unsigned;
  auto widen = [&](int32_t v) -> int64_t {
    return uns ? (int64_t)(uint64_t)(uint32_t)v : (int64_t)v;
  };
  auto row_valid = [&](size_t i) {
    return dc.valid_bits.empty() || ((dc.valid_bits[i >> 3] >> (i & 7)) & 1) != 0;
  };
  auto read_code = [&](size_t row) -> uint32_t {
    const uint8_t w = (dc.code_width == 1 || dc.code_width == 2 || dc.code_width == 4)
                          ? dc.code_width
                          : (uint8_t)1;
    const size_t off = row * w;
    if (off + w > dc.dict_codes_array.size())
      throw std::runtime_error("parquet patch: dictionary codes are shorter than the row count");
    uint32_t c = dc.dict_codes_array[off];
    if (w >= 2) c |= (uint32_t)dc.dict_codes_array[off + 1] << 8;
    if (w == 4)
      c |= ((uint32_t)dc.dict_codes_array[off + 2] << 16) |
           ((uint32_t)dc.dict_codes_array[off + 3] << 24);
    return c;
  };
  auto dict_at = [&](uint32_t code) -> int32_t {
    if (code >= dc.dict_int32_values.size())
      throw std::runtime_error("parquet patch: dictionary code is out of range");
    return dc.dict_int32_values[code];
  };

  if (!dc.dict_int32_values.empty()) {
    if (!dc.dict_codes_array.empty()) {
      for (size_t i = 0; i < rg_rows; i++)
        if (row_valid(i))
          out[i] = widen(dict_at(read_code(i)));
    } else {
      size_t vi = 0;
      for (size_t i = 0; i < rg_rows; i++) {
        if (!row_valid(i))
          continue;
        if (vi >= dc.dict_indices.size())
          throw std::runtime_error("parquet patch: fewer dictionary codes than present rows");
        out[i] = widen(dict_at((uint32_t)dc.dict_indices[vi++]));
      }
    }
    return;
  }

  if (!dc.rle_run_lengths.empty()) {
    // rle_int64_values holds already-resolved values for the int32 rle path
    // too, and the runs cover every row position including null ones.
    size_t off = 0;
    for (size_t r = 0; r < dc.rle_run_lengths.size(); r++) {
      const size_t cnt = (size_t)dc.rle_run_lengths[r];
      for (size_t j = 0; j < cnt && off + j < rg_rows; j++)
        out[off + j] = uns ? (int64_t)(uint64_t)dc.rle_int64_values[r]
                           : dc.rle_int64_values[r];
      off += cnt;
    }
    return;
  }

  size_t vi = 0;
  for (size_t i = 0; i < rg_rows; i++) {
    if (!row_valid(i))
      continue;
    if (vi >= dc.int32_values.size())
      throw std::runtime_error("parquet patch: fewer decoded values than present rows");
    out[i] = widen(dc.int32_values[vi++]);
  }
}

// One column of the RESULT file.
struct PatchColumn {
  // Name/type/logical annotation for the footer. Data pointers are never read:
  // a surviving column's bytes come from the source file, not from here.
  ColumnInput shape;
  // Leaf-column index in the SOURCE file to copy the pages from. A RENAME is
  // simply a column whose `shape.name` differs from the source column's name.
  int src_index = -1;
  // Index into the caller's ConstantColumn list, for a column being ADDED.
  // Exactly one of src_index / const_index is set.
  int const_index = -1;
  // Set alongside `src_index` when this column is being RETYPED to a different
  // parquet PHYSICAL type: its pages are decoded and re-encoded rather than
  // copied. An annotation-only retype leaves this false and copies as usual.
  bool reencode = false;
};

// Produce a new parquet file from `src`, laid out per `cols`.
//
// Each entry of `cols` either copies a source column's pages verbatim
// (`src_index`), has its chunk synthesised from a constant (`const_index` into
// `consts`), or is decoded from the source and re-encoded (`src_index` with
// `reencode`, the physical-type-changing widening).
inline std::vector<uint8_t> PatchParquetColumns(const uint8_t *src, size_t src_len,
                                                const std::vector<PatchColumn> &cols,
                                                const std::vector<ConstantColumn> &consts) {
  if (src == nullptr || src_len < 8)
    throw std::runtime_error("parquet patch: source is too small to be a parquet file");
  if (cols.empty())
    throw std::runtime_error("parquet patch: the result would have no columns");

  MetadataParseOptions opts;
  opts.include_statistics = true;
  FileStats fs = ReadParquetMetadataFromBuffer(src, src_len, opts);

  const size_t ncols = cols.size();

  std::vector<ColumnInput> shapes;
  shapes.reserve(ncols);
  for (const PatchColumn &c : cols)
    shapes.push_back(c.shape);

  std::vector<uint8_t> out;
  out.reserve(src_len);
  const char *MAGIC = "PAR1";
  out.insert(out.end(), MAGIC, MAGIC + 4);

  std::vector<RGMeta> rg_metas;
  std::vector<std::vector<ColumnInput>> all_rg_cols;
  rg_metas.reserve(fs.row_groups.size());
  all_rg_cols.reserve(fs.row_groups.size());

  for (size_t rg = 0; rg < fs.row_groups.size(); rg++) {
    const RowGroupStats &src_rg = fs.row_groups[rg];

    // Per-row-group shapes. Identical to `shapes` except for a LIST column's
    // `num_levels`, which the footer writes as that chunk's num_values and
    // which is a per-chunk count (levels, not rows) - so it cannot live on the
    // one schema-level shape. Filled from the source chunk below.
    std::vector<ColumnInput> rg_cols = shapes;

    RGMeta meta;
    meta.data_offsets.assign(ncols, 0);
    meta.dict_offsets.assign(ncols, -1);
    meta.sizes.assign(ncols, 0);
    meta.uncompressed.assign(ncols, 0);
    meta.stats.assign(ncols, ColumnStats{});
    meta.bloom_offset.assign(ncols, -1);
    meta.bloom_length.assign(ncols, 0);
    meta.codecs.assign(ncols, CODEC_UNCOMPRESSED);
    meta.row_count = (size_t)src_rg.num_rows;
    meta.total_byte_size = 0;

    // Where each surviving column's bytes live in the SOURCE, filled in below
    // and then copied in source order (see the sort that follows).
    std::vector<int64_t> src_chunk_start(ncols, -1);
    std::vector<int64_t> src_bloom_start(ncols, -1);

    for (size_t j = 0; j < ncols; j++) {
      const int s = cols[j].src_index;
      if (s < 0 || cols[j].reencode)
        continue;  // synthesised or re-encoded further down, not copied
      if ((size_t)s >= src_rg.columns.size())
        throw std::runtime_error("parquet patch: source column index out of range");

      const ::ColumnStats &cs = src_rg.columns[(size_t)s];

      // The footer we are about to write DECLARES a type over bytes we are not
      // decoding. If the declaration and the bytes disagree the result is a file
      // that lies about itself and reads back as garbage, so this is fatal.
      const PType declared = cols[j].shape.is_array ? cols[j].shape.elem_type
                                                    : cols[j].shape.type;
      if (cs.physical_type != ptype_reader_name(declared))
        throw std::runtime_error(
            "parquet patch: column '" + cols[j].shape.name + "' is declared " +
            ptype_reader_name(declared) + " but its pages hold " + cs.physical_type);

      // A chunk starts at its dictionary page when it has one, otherwise at its
      // first data page, and spans total_compressed_size bytes (every page of
      // the chunk, dictionary included).
      const int64_t chunk_start = cs.dictionary_page_offset >= 0
                                      ? cs.dictionary_page_offset
                                      : cs.data_page_offset;
      const int64_t chunk_len = cs.total_compressed_size;
      if (chunk_start < 0 || chunk_len < 0 ||
          (uint64_t)chunk_start + (uint64_t)chunk_len > (uint64_t)src_len)
        throw std::runtime_error("parquet patch: column chunk runs past the end of the file");
      if (cs.data_page_offset < chunk_start)
        throw std::runtime_error("parquet patch: data page precedes its own chunk");

      // A null_count the source does not carry cannot be invented: claiming 0
      // would tell a reader the chunk has no nulls, and an IS NULL predicate
      // would then prune a row group that does have them - a wrong answer, not
      // a lost optimisation.
      //
      // A LIST column is exempt because it has no statistics to carry either
      // way: nested null semantics have no single leaf null_count, so neither
      // the writer nor the footer emitted below records Statistics for one
      // (write_column_chunk skips them when is_array). Demanding one here would
      // reject every array column over a fact that is not missing, just absent
      // by design.
      if (!cols[j].shape.is_array && cs.null_count < 0)
        throw std::runtime_error(
            "parquet patch: column '" + cols[j].shape.name +
            "' has no null_count in its source statistics; refusing to fabricate one");

      // A LIST chunk's num_values counts LEVELS, not rows - one row expands to
      // as many entries as it has elements. Carry the source's own count so the
      // new footer describes the copied pages exactly; deriving it from
      // row_count would under-declare every row holding more than one element
      // and the reader would stop short.
      if (rg_cols[j].is_array) {
        if (cs.num_values < 0)
          throw std::runtime_error(
              "parquet patch: list column '" + cols[j].shape.name +
              "' has no num_values in its source metadata; its levels cannot be "
              "re-declared");
        rg_cols[j].num_levels = (size_t)cs.num_values;
      }

      src_chunk_start[j] = chunk_start;
      meta.sizes[j] = (size_t)chunk_len;
      meta.uncompressed[j] = (size_t)(cs.total_uncompressed_size > 0
                                          ? cs.total_uncompressed_size
                                          : 0);
      meta.codecs[j] = cs.codec >= 0 ? cs.codec : CODEC_UNCOMPRESSED;

      ColumnStats st;
      st.has_minmax = cs.has_min && cs.has_max;
      if (st.has_minmax) {
        st.min_bytes.assign(cs.min.begin(), cs.min.end());
        st.max_bytes.assign(cs.max.begin(), cs.max.end());
      }
      st.null_count = cs.null_count;
      st.distinct_count = cs.distinct_count;
      meta.stats[j] = st;

      // The bloom filter is a region of its own, outside the chunk's pages.
      // Dropping it instead would keep the file correct but silently cost every
      // future reader the probe it was built for.
      if (cs.bloom_offset >= 0 && cs.bloom_length > 0) {
        if ((uint64_t)cs.bloom_offset + (uint64_t)cs.bloom_length > (uint64_t)src_len)
          throw std::runtime_error("parquet patch: bloom filter runs past the end of the file");
        src_bloom_start[j] = cs.bloom_offset;
        meta.bloom_length[j] = (int32_t)cs.bloom_length;
      }

      meta.total_byte_size += meta.uncompressed[j];
    }

    // Copy every surviving extent IN SOURCE ORDER, not in column order.
    //
    // The two differ: a file may group all its bloom filters after all its
    // column chunks, and emitting each column's bloom straight after its own
    // chunk would reshuffle the region into a different - still valid, still
    // correct - arrangement. Preserving source order means a patch that removes
    // nothing (a pure RENAME) reproduces the page region byte-for-byte, which
    // is the strongest available statement that no data was touched, and is
    // asserted directly by the tests.
    struct Extent { int64_t start; int64_t len; size_t col; bool is_bloom; };
    std::vector<Extent> extents;
    extents.reserve(ncols * 2);
    for (size_t j = 0; j < ncols; j++) {
      if (cols[j].src_index < 0 || cols[j].reencode)
        continue;
      extents.push_back({src_chunk_start[j], (int64_t)meta.sizes[j], j, false});
      if (src_bloom_start[j] >= 0)
        extents.push_back({src_bloom_start[j], (int64_t)meta.bloom_length[j], j, true});
    }
    std::sort(extents.begin(), extents.end(),
              [](const Extent &a, const Extent &b) { return a.start < b.start; });

    std::vector<int64_t> new_chunk_start(ncols, -1);
    for (const Extent &e : extents) {
      const int64_t new_off = (int64_t)out.size();
      out.insert(out.end(), src + e.start, src + e.start + e.len);
      if (e.is_bloom)
        meta.bloom_offset[e.col] = new_off;
      else
        new_chunk_start[e.col] = new_off;
    }

    for (size_t j = 0; j < ncols; j++) {
      if (cols[j].src_index < 0 || cols[j].reencode)
        continue;
      const ::ColumnStats &cs = src_rg.columns[(size_t)cols[j].src_index];
      meta.data_offsets[j] =
          new_chunk_start[j] + (cs.data_page_offset - src_chunk_start[j]);
      meta.dict_offsets[j] = cs.dictionary_page_offset >= 0 ? new_chunk_start[j] : -1;
    }

    // ---- build the chunks that are not copied, after the copied bytes ----
    //
    // Two kinds land here: an ADDed column (one value repeated `rg_rows` times)
    // and a RETYPED column whose parquet physical type changed (decoded from
    // the source and re-encoded). Both are built per row group so peak memory
    // tracks the largest row group rather than the whole file, and both go
    // through write_row_group_chunks - the writer's own chunk builder - so
    // their page layout, statistics and codec choice are made by exactly the
    // code that would have written them in the first place.
    const size_t rg_rows = (size_t)src_rg.num_rows;
    std::vector<size_t> add_positions;
    std::vector<ColumnInput> add_cols;
    std::vector<std::vector<uint32_t>> code_store;
    std::vector<std::vector<uint8_t>> mask_store;
    std::vector<std::vector<uint8_t>> plain_store;
    std::vector<std::vector<int64_t>> widened_store;
    for (size_t j = 0; j < ncols; j++) {
      if (cols[j].reencode) {
        const ::ColumnStats &cs = src_rg.columns[(size_t)cols[j].src_index];
        ColumnInput ci = cols[j].shape;
        if (cs.physical_type != "int32" || ci.type != PT_INT64)
          throw std::runtime_error(
              "parquet patch: column '" + ci.name + "' cannot be retyped from " +
              cs.physical_type + " to " + ptype_reader_name(ci.type) +
              "; only a physical int32 to int64 widening is supported");

        DecodedColumn dc =
            DecodeColumnFromChunk(src, src_len, &cs, (const uint8_t *)nullptr);
        if (!dc.success)
          throw std::runtime_error("parquet patch: could not decode column '" + ci.name +
                                   "' to retype it: " + dc.error_message);

        widened_store.emplace_back();
        widen_int32_chunk_to_int64(dc, rg_rows, widened_store.back());
        ci.i64 = widened_store.back().data();
        // The decoder's validity bitmap uses the same 1-bit-per-row, LSB-first
        // convention the writer reads, so it is handed straight over. An empty
        // one means all-valid, which is also the writer's nullptr.
        if (!dc.valid_bits.empty()) {
          mask_store.emplace_back(dc.valid_bits);
          ci.validity = mask_store.back().data();
        }
        // Let the writer's own auto-dictionary decide, exactly as it would for
        // this column in a fresh write. A column that carried a bloom filter
        // keeps one: the old filter was built over the old byte width and
        // cannot be copied, and dropping it silently would cost every future
        // reader the probe the column was written to support.
        ci.dict_enabled = true;
        ci.bloom = (cs.bloom_offset >= 0 && cs.bloom_length > 0);
        add_positions.push_back(j);
        add_cols.push_back(ci);
        continue;
      }
      if (cols[j].const_index < 0)
        continue;
      const ConstantColumn &cc = consts[(size_t)cols[j].const_index];
      ColumnInput ci = cols[j].shape;

      if (cc.is_null) {
        // An all-zero validity mask and no value buffers at all: every value
        // encoder and the statistics pass are guarded on validity, so nothing
        // is ever read through the null pointers. The chunk is then just a
        // definition-level run - a few bytes however many rows there are.
        mask_store.emplace_back((rg_rows + 7) / 8, (uint8_t)0);
        ci.validity = mask_store.back().data();
      } else if (ptype_takes_dict_codes(ci.type)) {
        // A one-entry dictionary: the value once, then an RLE run of the same
        // code. Also a handful of bytes regardless of row count.
        code_store.emplace_back(rg_rows, (uint32_t)0);
        ci.codes = code_store.back().data();
        ci.dict_count = 1;
        switch (ci.type) {
        case PT_INT32:      ci.i32 = &cc.v_i32; break;
        case PT_INT64:      ci.i64 = &cc.v_i64; break;
        case PT_DOUBLE:     ci.f64 = &cc.v_f64; break;
        default:            ci.strs = &cc.v_str; break;  // PT_BYTE_ARRAY
        }
      } else {
        // BOOLEAN / FLBA: see ptype_takes_dict_codes. Repeat the value once per
        // row and let the codec collapse it.
        const size_t w = (ci.type == PT_BOOLEAN) ? 1 : (size_t)ci.dec_width;
        std::vector<uint8_t> buf(rg_rows * w);
        for (size_t r = 0; r < rg_rows; r++) {
          if (ci.type == PT_BOOLEAN)
            buf[r] = cc.v_bool;
          else
            std::memcpy(buf.data() + r * w, cc.v_bytes.data(), w);
        }
        plain_store.push_back(std::move(buf));
        if (ci.type == PT_BOOLEAN)
          ci.boolean = plain_store.back().data();
        else
          ci.dec_raw = plain_store.back().data();
      }

      add_positions.push_back(j);
      add_cols.push_back(ci);
    }

    if (!add_cols.empty()) {
      RGMeta add_meta;
      // base_offset 0: `out` is the file from byte zero, so its size already is
      // the absolute offset the footer has to record.
      write_row_group_chunks(out, /*base_offset=*/0, add_cols, rg_rows,
                             CODEC_ZSTD, PROFILE_FAST, /*max_page_bytes=*/0,
                             add_meta);
      for (size_t k = 0; k < add_positions.size(); k++) {
        const size_t j = add_positions[k];
        meta.data_offsets[j] = add_meta.data_offsets[k];
        meta.dict_offsets[j] = add_meta.dict_offsets[k];
        meta.sizes[j] = add_meta.sizes[k];
        meta.uncompressed[j] = add_meta.uncompressed[k];
        meta.stats[j] = add_meta.stats[k];
        meta.bloom_offset[j] = add_meta.bloom_offset[k];
        meta.bloom_length[j] = add_meta.bloom_length[k];
        meta.codecs[j] = add_meta.codecs[k];
        meta.total_byte_size += add_meta.uncompressed[k];
      }
    }

    rg_metas.push_back(std::move(meta));
    all_rg_cols.push_back(std::move(rg_cols));
  }

  write_parquet_footer(out, shapes, (size_t)fs.num_rows, rg_metas, all_rg_cols);
  return out;
}

// Drop, rename, add and/or retype columns BY NAME.
//
// The result keeps the source's column order, minus `drop`, with `rename`
// applied, with each `retype` column re-declared, and with each donor in `add`
// appended as a new column. Shapes for untouched columns are taken from the
// source's own schema, so the caller does not have to restate types it is not
// changing - and cannot get them wrong.
//
// `retype` pairs a column name with a DONOR file (see ConstantColumn) whose
// shape is the TARGET type; the donor's value is irrelevant here, only its
// annotation. When the target's physical type matches the source's, the pages
// are copied verbatim and only the footer's annotation changes - the same cost
// as a rename. When it differs, that column alone is decoded and re-encoded.
//
// A name in `drop`, `rename` or `retype` that the source does not have is an
// error: the caller believes something about this file that is not true, and
// silently doing nothing would leave it believing it.
inline std::vector<uint8_t> PatchParquetColumnsByName(
    const uint8_t *src, size_t src_len,
    const std::vector<std::string> &drop,
    const std::vector<std::pair<std::string, std::string>> &rename,
    const std::vector<std::string> &add,
    const std::vector<std::pair<std::string, std::string>> &retype) {
  if (src == nullptr || src_len < 8)
    throw std::runtime_error("parquet patch: source is too small to be a parquet file");

  MetadataParseOptions schema_opts;
  schema_opts.schema_only = true;
  FileStats schema_fs = ReadParquetMetadataFromBuffer(src, src_len, schema_opts);

  std::vector<PatchColumn> cols;
  cols.reserve(schema_fs.schema.size());

  std::vector<bool> dropped_seen(drop.size(), false);
  std::vector<bool> renamed_seen(rename.size(), false);
  std::vector<bool> retyped_seen(retype.size(), false);

  for (size_t i = 0; i < schema_fs.schema.size(); i++) {
    const SchemaElement &e = schema_fs.schema[i];

    bool is_dropped = false;
    for (size_t d = 0; d < drop.size(); d++) {
      if (drop[d] == e.name) { is_dropped = true; dropped_seen[d] = true; }
    }
    if (is_dropped)
      continue;

    PatchColumn pc;
    pc.shape = shape_from_schema_element(e);
    pc.src_index = (int)i;

    // A retype replaces the shape wholesale with the donor's - the target type
    // is exactly what the write path would have annotated this column as.
    // Applied before the rename so the two compose on one column.
    for (size_t t = 0; t < retype.size(); t++) {
      if (retype[t].first != e.name)
        continue;
      retyped_seen[t] = true;
      ConstantColumn target =
          parse_donor((const uint8_t *)retype[t].second.data(), retype[t].second.size());
      const PType was = pc.shape.type;
      pc.shape = target.shape;
      pc.shape.name = e.name;
      // Same physical type => the bytes already are what the new annotation
      // says they are, so this costs exactly what a rename costs.
      pc.reencode = (target.shape.type != was);
    }

    for (size_t r = 0; r < rename.size(); r++) {
      if (rename[r].first == e.name) {
        pc.shape.name = rename[r].second;
        renamed_seen[r] = true;
      }
    }
    cols.push_back(std::move(pc));
  }

  for (size_t d = 0; d < drop.size(); d++)
    if (!dropped_seen[d])
      throw std::runtime_error("parquet patch: no column named '" + drop[d] + "' to drop");
  for (size_t r = 0; r < rename.size(); r++)
    if (!renamed_seen[r])
      throw std::runtime_error("parquet patch: no column named '" + rename[r].first +
                               "' to rename");
  for (size_t t = 0; t < retype.size(); t++)
    if (!retyped_seen[t])
      throw std::runtime_error("parquet patch: no column named '" + retype[t].first +
                               "' to retype");

  // Reserved before the first push_back: the ColumnInputs built later point at
  // members of these elements, so the vector must never reallocate.
  std::vector<ConstantColumn> consts;
  consts.reserve(add.size());
  for (const std::string &donor : add) {
    consts.push_back(parse_donor((const uint8_t *)donor.data(), donor.size()));
    const std::string &name = consts.back().shape.name;
    for (const PatchColumn &existing : cols)
      if (existing.shape.name == name)
        throw std::runtime_error("parquet patch: the result would have two columns "
                                 "named '" + name + "'");
    PatchColumn pc;
    pc.shape = consts.back().shape;
    pc.const_index = (int)consts.size() - 1;
    cols.push_back(std::move(pc));
  }

  if (cols.empty())
    throw std::runtime_error("parquet patch: dropping every column would leave no relation");

  return PatchParquetColumns(src, src_len, cols, consts);
}

} // namespace rugo_pq_write
