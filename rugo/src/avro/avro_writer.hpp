#pragma once
// rugo/src/avro/avro_writer.hpp — write Draken columns as an Avro object container file.
//
// Design: docs/AVRO_WRITER_DESIGN.md. Pure C++ (no Python): the Cython edge that
// unpacks a Morsel is avro/_avro_writer.pxi. The reader's container / varint headers
// are the other half of this format (avro_container.hpp, avro_varint.hpp).
//
// Input is the unified DrakenVector (CLAUDE.md §11): every value is read through
// data[selection[i]], no shape dispatch. Avro is row-major, so a block is encoded
// COLUMN AT A TIME with per-row write cursors (doc §6): a length pass sizes every
// row, a prefix sum places it, a write pass fills it — two passes over the input,
// one exactly-sized output buffer, no per-value type dispatch.
//
// Every field is `["null", T]` (D2). Field names must be valid Avro names (D14).
// Types the format cannot carry are refused by name (doc §5, §11).

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "core/buffers.h"  // DrakenVector, DrakenType


namespace rugo::avro {

enum class WriteCodec : uint8_t { Null, Deflate, Snappy, Zstandard, Bzip2 };

// The codec by its `avro.codec` name (null, deflate, snappy, zstandard, bzip2). Throws
// std::invalid_argument naming an unknown one.
WriteCodec parse_write_codec(const std::string& name);

// A Draken type with its logical descriptor (draken/logical_type.h ordinals: 0 NONE,
// 1 TIMESTAMP, 2 TIME, 3 DECIMAL, 4 VECTOR, 5 IPV4) and the descriptor's parameters,
// as the parquet writer's edge reads them from the Vector handle.
struct ElemSpec {
    DrakenType  type = DRAKEN_INT64;
    uint8_t     logical_kind = 0;
    int         unit = 0;       // temporal unit code: 0 s, 1 ms, 2 us, 3 ns
    int         scale = 0;      // DECIMAL
    int         precision = 0;  // DECIMAL
};

// One field of the schema: its name and the type it is written from. For an ARRAY,
// `elements` is the nesting chain below it: elements[0] is the top array's element
// type; while that is itself DRAKEN_ARRAY, elements[1] is ITS element, and so on down
// to the scalar leaf. An ARRAY column whose rows hold no element at all (every row
// null or empty) has no child vector and an empty chain.
//
// `required`: the field is written as plain T, not `["null", T]` — set by a one-shot
// write for a column with no nulls (a stream cannot know its later batches, so its
// fields keep the union). `symbols`: a VARCHAR / NVARCHAR column written as an Avro
// enum of these symbols (build_enum; one-shot writes only).
struct FieldSpec {
    std::string name;
    DrakenType  type = DRAKEN_INT64;
    uint8_t     logical_kind = 0;
    int         unit = 0;
    int         scale = 0;
    int         precision = 0;
    std::vector<ElemSpec> elements;
    bool        required = false;
    std::vector<std::string> symbols;
};

// One column of one batch: the vector the values are read from (its type must be the
// FieldSpec's); for an ARRAY, the child vectors down the same chain as
// FieldSpec::elements (levels[0] holds the top array's elements, ...); for an enum,
// the symbol index of every PHYSICAL value (indexed by selection[i], from build_enum).
struct WriteColumn {
    const DrakenVector* vec = nullptr;
    std::vector<const DrakenVector*> levels;
    const uint32_t* enum_codes = nullptr;
};

// How a field is encoded, decided once from its FieldSpec: `enc` is the column's
// encoder (an Enc, internal to avro_writer.cpp) and `width` a fixed decimal's byte
// width; for an ARRAY, `leaf` / `leaf_width` are the scalar leaf's and `depth` the
// number of ARRAY levels (1 = a flat array). `branch`: the field is `["null", T]`.
struct FieldPlan {
    uint8_t  enc = 0;
    uint8_t  width = 0;
    uint8_t  leaf = 0;
    uint8_t  leaf_width = 0;
    uint32_t depth = 0;
    bool     branch = true;
    bool operator==(const FieldPlan& o) const {
        return enc == o.enc && width == o.width && leaf == o.leaf && leaf_width == o.leaf_width &&
               depth == o.depth && branch == o.branch;
    }
};

struct WriteOptions {
    WriteCodec  codec = WriteCodec::Deflate;
    size_t      block_bytes = size_t{1} << 20;  // uncompressed payload per block (doc §6)
    std::string record_name = "record";         // root record name; namespace is "rugo"
    bool        have_sync = false;              // false: 16 random bytes
    uint8_t     sync[16] = {};
    size_t      max_threads = 0;                // block encode + compress threads; 0 = every core
};

// Rows per block, at most (keeps the reader's whole-block batching granular).
constexpr uint32_t kMaxBlockRows = 65536;

// A valid Avro name: [A-Za-z_][A-Za-z0-9_]*.
bool is_avro_name(const std::string& s);

// True when any row of `v` is null (a DRAKEN_NULL column with rows always is).
bool column_has_nulls(const DrakenVector* v);

// Decide whether a VARCHAR / NVARCHAR column is written as an Avro enum, and build it:
// every non-null value must be a valid Avro name (the spec's symbol rule), there must
// be at most `max_symbols` distinct values, and the enum (a code per row plus the
// symbols in the header) must be smaller than the strings. On true, `symbols` are the
// distinct values in byte order and `codes[j]` is the symbol of physical value j (a
// value no non-null row uses gets 0 and is never read). False leaves the column a
// string — an encoding choice, as parquet's dictionary is; the values are the same.
// Runs on up to `max_threads` threads (0 = every core) over row and physical-value
// ranges; the result does not depend on the thread count.
bool build_enum(const DrakenVector* v, size_t max_symbols, size_t max_threads, std::vector<std::string>& symbols,
                std::vector<uint32_t>& codes);

// Distinct values at most, for a one-shot write's enums (codes of 1–2 bytes).
constexpr size_t kMaxEnumSymbols = 4096;

// The schema (JSON text) for these fields: a record named `record_name` in namespace
// `rugo`, every field `["null", T]`. Throws std::invalid_argument on an invalid or
// duplicate name, an invalid record name, or a type the writer does not write.
std::string schema_json(const std::vector<FieldSpec>& fields, const std::string& record_name);
// The same, also returning the encoding plan per field (AvroWriter's own entry).
std::string schema_json(const std::vector<FieldSpec>& fields, const std::string& record_name,
                        std::vector<FieldPlan>& plans);

struct BlockEncoder;  // per-worker encode + compress state (avro_writer.cpp)

// A container file being written. Construction validates the fields, builds the
// schema and emits the header; each write_batch encodes its rows into one or more
// blocks (doc §6), encoded and compressed on up to `max_threads` threads and kept in
// file order. Output accumulates as CHUNKS — the header, then one per block — until
// take_pending() hands them out, so a one-shot writer joins them once and a streaming
// writer passes each to its sink. Avro has no footer: the file is complete after any
// block. The bytes do not depend on the thread count.
class AvroWriter {
public:
    AvroWriter(std::vector<FieldSpec> fields, const WriteOptions& opts);
    ~AvroWriter();
    AvroWriter(const AvroWriter&) = delete;
    AvroWriter& operator=(const AvroWriter&) = delete;

    const std::string& schema() const { return schema_json_; }

    // `fields` describe this batch's columns and must match the writer's (names, types,
    // encodings) — a streamed batch with another schema is refused here, naming the
    // column. `cols` has one entry per field, in field order. Throws
    // std::invalid_argument on any mismatch. Zero rows write nothing.
    void write_batch(const std::vector<FieldSpec>& fields, const WriteColumn* cols, uint32_t nrows);

    std::vector<std::vector<uint8_t>> take_pending();
    uint64_t rows_written() const { return rows_written_; }
    uint64_t blocks_written() const { return blocks_written_; }

private:
    void emit_header();
    void check_fields(const std::vector<FieldSpec>& fields) const;
    BlockEncoder& encoder(size_t worker);
    void encode_block(BlockEncoder& enc, const WriteColumn* cols, uint32_t first, uint32_t count,
                      uint64_t payload_bytes, std::vector<uint8_t>& out) const;

    std::vector<FieldSpec>   fields_;
    std::vector<FieldPlan>   plans_;     // per field: the encoding chosen from its FieldSpec
    std::vector<std::string> types_;     // per field: its Avro type JSON (the T of ["null", T])
    WriteOptions             opts_;
    size_t                   threads_ = 1;
    std::string              schema_json_;
    std::vector<std::vector<uint8_t>> chunks_;   // header / blocks not yet taken
    std::vector<uint64_t>    row_len_;   // per-row encoded size of the current batch
    std::vector<std::unique_ptr<BlockEncoder>> encoders_;  // one per worker, reused
    uint64_t                 rows_written_ = 0;
    uint64_t                 blocks_written_ = 0;
};

}  // namespace rugo::avro
