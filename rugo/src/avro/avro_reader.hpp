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
// scalar (bool / int / long / float / double / string / bytes / fixed) is ARRAY.
// An array of a logical type or an enum is refused (no Draken ARRAY child for it).
//
// A reader schema (docs §19.3): fields match by field-id when both carry one, else
// by name; a reader field the file lacks is its default (or NULL) as a constant.
// Logical types follow the FILE (the fastavro / Apache convention).

#include <cstdint>
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
        offsets = nullptr; child_validity = nullptr;
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

// Decode `data` (a whole container file). `columns` are dotted paths; empty with
// `all_columns` = every top-level field (of the reader schema when one is given). Throws std::runtime_error on a corrupt
// file, a refused schema construct, or a column that is missing or not built yet.
// `reader_schema_json` empty = read with the file's own schema.
void read_avro_buffer(const uint8_t* data, size_t size, const std::vector<std::string>& columns,
                      bool all_columns, const std::string& reader_schema_json, AvroRead& out);

// Header only: the writer schema and the metadata map.
void read_avro_header(const uint8_t* data, size_t size, AvroRead& out);

}  // namespace rugo::avro
