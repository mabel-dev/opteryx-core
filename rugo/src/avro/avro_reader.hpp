#pragma once
// rugo/src/avro/avro_reader.hpp — decode an Avro object container file into
// draken_malloc-owned column buffers, batch by batch. Pure C++ (no Python): the
// Python edge that wraps an AvroColumn into a Draken Vector is
// avro/_avro_column_wrap.hpp.
//
// Design: docs/AVRO_READER_DESIGN.md. The file's writer schema is compiled, per
// file, into a flat program of a closed set of opcodes (§5.1); one interpreter loop
// runs it once per record (§5.2, D1). Projected leaves are written straight into
// Draken buffers (D3 = vectors); everything else compiles to skip ops.
//
// Columns are dotted paths through records (`data_file.file_path`), including
// through a nullable record (a NULL record makes every leaf below it NULL).
//
// Nested output (docs §17.2): a whole record / map, or an array of records or arrays,
// is NVARCHAR JSON text under the parquet NESTEDJSON rules; an array of a plain
// scalar (bool / int / long / float / double / string / bytes / fixed), or of arrays of one
// (to any depth, as the parquet scan builds them), is ARRAY.
// An array of a logical type or an enum has no Draken ARRAY child, so it is a JSON array too.
//
// A reader schema (docs §19.3): fields match by field-id when both carry one, else
// by name; a reader field the file lacks is its default (or NULL) as a constant.
// Logical types follow the FILE (the fastavro / Apache convention).

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "core/buffers.h"      // DrakenType
#include "core/string_slot.h"  // DrakenStringSlot

namespace rugo::avro {

// How the Python edge wraps a column (which draken_vector_own_* producer).
enum class OutKind : uint8_t {
    Raw,         // BOOL (bit-packed), INT32, INT64, FLOAT32, FLOAT64, DATE32
    String,      // VARCHAR / VARBINARY: slots + arena
    Dict,        // enum: VARCHAR, `dict_len` symbol slots + per-row codes
    Decimal64,   // DECIMAL (p <= 18), int64 unscaled
    Decimal128,  // DECIMAL128 (p <= 38), __int128 unscaled
    Timestamp,   // TIMESTAMP64, microseconds, UTC
    Time64,      // TIME64, microseconds
    Ipv4,        // UINT32 with the IPV4 logical descriptor (`{"type":"long","opteryx.type":"IPV4"}`)
    Array,       // ARRAY: offsets + a child (numeric `data` or string `slots`/`arena`)
    // Constants (a reader-schema field the file does not hold): a value list of one
    // item and positions all 0 (`codes`), per the unified vector (CLAUDE.md §11).
    ConstRaw,    // `data` holds the one fixed-width value
    ConstString, // `slots`/`arena` hold the one string value
};

// One column of one batch, in draken_malloc'd buffers. Move-only; any buffer still
// held when it is destroyed (not handed to a Vector) is freed.
struct AvroColumn {
    OutKind           kind = OutKind::Raw;
    DrakenType        type = DRAKEN_INT64;
    uint32_t          length = 0;
    void*             data = nullptr;
    uint8_t*          validity = nullptr;   // NULL = all valid
    DrakenStringSlot* slots = nullptr;
    uint8_t*          arena = nullptr;
    size_t            arena_len = 0;
    uint32_t*         codes = nullptr;      // Dict
    uint32_t          dict_len = 0;         // Dict
    uint8_t           precision = 0;
    uint8_t           scale = 0;
    // Array: the child's buffers are data/slots/arena above; these describe it.
    int32_t*          offsets = nullptr;    // length + 1 entries
    uint8_t*          child_validity = nullptr;
    DrakenType        child_type = DRAKEN_INT64;
    uint32_t          child_length = 0;
    // A nested Array (an array of arrays): child_type is DRAKEN_ARRAY and the child is this whole
    // column (its offsets, validity and own child), with `length` == child_length. The leaf buffers
    // above are then unused.
    std::unique_ptr<AvroColumn> child_col;

    AvroColumn() = default;
    AvroColumn(AvroColumn&& o) noexcept { steal(o); }
    AvroColumn& operator=(AvroColumn&& o) noexcept {
        if (this != &o) { release_all(); steal(o); }
        return *this;
    }
    AvroColumn(const AvroColumn&) = delete;
    AvroColumn& operator=(const AvroColumn&) = delete;
    ~AvroColumn() { release_all(); }

    // Called by the edge after the buffers were transferred to a Vector.
    void disown() noexcept {
        data = nullptr; validity = nullptr; slots = nullptr; arena = nullptr; codes = nullptr;
        offsets = nullptr; child_validity = nullptr; child_col.reset();
    }

private:
    void steal(AvroColumn& o) noexcept;
    void release_all() noexcept;
};

struct AvroBatch {
    uint32_t                rows = 0;
    std::vector<AvroColumn> columns;
};

struct AvroRead {
    std::string                                      schema_json;
    std::vector<std::pair<std::string, std::string>> metadata;
    std::vector<std::string>                         column_names;
    std::vector<AvroBatch>                           batches;
};

// Rows per batch: whole blocks are packed into a batch up to this many rows; a
// block is never split (a single larger block is its own batch).
constexpr uint32_t kBatchRows = 65536;

// What a column decodes to, as the compiler decided it: the Draken type plus the
// logical-type descriptor the column carries (draken/logical_type.h ordinals:
// 0 NONE, 1 TIMESTAMP, 2 TIME, 3 DECIMAL; TIMESTAMP/TIME are always microseconds,
// UTC). `child_type` is an ARRAY's element type.
struct AvroColumnType {
    DrakenType type = DRAKEN_INT64;
    uint8_t    logical_kind = 0;
    uint8_t    precision = 0;
    uint8_t    scale = 0;
    DrakenType child_type = DRAKEN_INT64;   // an ARRAY's innermost element
    uint8_t    array_depth = 0;             // an ARRAY's levels of nesting (1 = a plain array)
};

// One batch of a file: a run of whole blocks, at most kBatchRows rows (a larger single
// block is its own batch). Indexes AvroFile's framed block list.
struct AvroBatchRange {
    uint32_t first_block = 0;
    uint32_t block_count = 0;
    uint32_t rows = 0;
};

// A container file opened for decoding: header, schema(s) and the compiled decode
// program, built once and then READ-ONLY, so any number of threads can decode its
// batches at once, each through its own Worker. Construction throws exactly as
// read_avro_buffer does. `data` must outlive the file and every Worker.
//
// batches() frames the file — walks the block headers, checking counts, sizes and
// sync markers, and groups whole blocks into batches — without decompressing
// anything. It runs once, on first call, and is safe to call from several threads.
//
// With no columns (`columns` empty and not `all_columns`) nothing is decoded: a
// batch carries only its row count, from the block headers.
class AvroFile {
public:
    AvroFile(const uint8_t* data, size_t size, const std::vector<std::string>& columns,
             bool all_columns, const std::string& reader_schema_json);
    ~AvroFile();
    AvroFile(const AvroFile&) = delete;
    AvroFile& operator=(const AvroFile&) = delete;

    const std::string& schema_json() const;
    // True when the file's writer schema has at least one top-level field and EVERY
    // top-level field carries a `field-id` (an Iceberg Avro data file always does).
    bool writer_fields_have_ids() const;
    const std::vector<std::pair<std::string, std::string>>& metadata() const;
    const std::vector<std::string>& column_names() const;
    // Parallel to column_names(); known from the header alone, before any block.
    std::vector<AvroColumnType> column_types() const;

    const std::vector<AvroBatchRange>& batches() const;

    // Two-pass decoding: `pass1` (a subset of the columns) is decoded for every record of
    // a batch, the rest only for the records the caller keeps (Worker::decode_pass1 /
    // decode_pass2). Call once, before any Worker exists. pass2_columns() is the rest,
    // in column order; it may be empty.
    void split(const std::vector<std::string>& pass1);
    const std::vector<std::string>& pass2_columns() const;
    // How many of `range`'s blocks hold its records `rows[0..n)` (indices into the
    // batch, strictly ascending) - what decode_pass2 would decompress for them.
    uint32_t blocks_with(const AvroBatchRange& range, const uint32_t* rows, uint32_t n) const;

    // Per-thread decode state (column sinks, JSON scratch, the codec's decompressor
    // and scratch). Not shareable between threads; reusable for any of its file's
    // batches.
    class Worker {
    public:
        explicit Worker(const AvroFile& file);
        ~Worker();
        Worker(const Worker&) = delete;
        Worker& operator=(const Worker&) = delete;
        AvroBatch decode(const AvroBatchRange& range);
        // Split files: pass 1's columns for every record of `range`, remembering where
        // each record is; then pass 2's columns for the records `rows[0..n)` of that
        // batch (indices into it, strictly ascending), decompressing again only the
        // blocks holding them. A decode_pass1 call replaces what the previous one kept.
        AvroBatch decode_pass1(const AvroBatchRange& range);
        AvroBatch decode_pass2(const uint32_t* rows, uint32_t n);

    private:
        struct Impl;
        std::unique_ptr<Impl> impl_;
    };

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

// Decode `data` (a whole container file). `columns` are dotted paths; empty with
// `all_columns` = every top-level field (of the reader schema when one is given). Throws std::runtime_error on a corrupt
// file, a refused schema construct, or a column that is missing or not built yet.
// `reader_schema_json` empty = read with the file's own schema. The file's batches are
// decoded on up to `max_threads` threads (0 = every core), and returned in file order.
void read_avro_buffer(const uint8_t* data, size_t size, const std::vector<std::string>& columns,
                      bool all_columns, const std::string& reader_schema_json, size_t max_threads,
                      AvroRead& out);

// Header only: the writer schema and the metadata map.
void read_avro_header(const uint8_t* data, size_t size, AvroRead& out);

}  // namespace rugo::avro
