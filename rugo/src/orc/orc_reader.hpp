#pragma once
// rugo/src/orc/orc_reader.hpp — decode an ORC file into draken_malloc-owned column
// buffers, a stripe at a time. Pure C++ (no Python): the Python edge that wraps an
// AvroColumn into a Draken Vector is avro/_avro_column_wrap.hpp, shared because the
// carrier is the same (docs/ORC_READER_DESIGN.md §6; a neutral rename of the carrier is
// a later cleanup).
//
// Basic support (design doc §1, §3.3): primitive columns, and STRUCT / LIST / MAP surfaced by
// docs/UUID_AND_NESTED_COLUMNS_DESIGN.md §3.1 (a LIST of a plain scalar, or of lists of one, is an ARRAY (as the parquet scan builds it), every other
// nested column is JSON text, rendered by the shared rugo_text renderer); UNION is refused.
// Compression NONE, ZLIB, SNAPPY, LZ4, ZSTD. Anything else throws std::runtime_error naming it.
//
// An OrcFile is opened over a caller-owned buffer that must outlive it. After bind(),
// decode_stripe() is const and re-entrant: any number of threads may decode different
// stripes of one file at once.

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "../declared_shape.hpp"   // DeclaredNode
#include "avro/avro_reader.hpp"  // AvroColumn / AvroBatch: the shared column carrier
#include "core/buffers.h"        // DrakenType
#include "orc_proto.hpp"

namespace rugo::orc {

using Column = rugo::avro::AvroColumn;
using Batch = rugo::avro::AvroBatch;

// What the caller wants a column to be. In a table scan this is the table's column (its
// Iceberg field id, its bound type); in a folder scan, the first file's column.
struct ReaderColumn {
    std::string name;
    int64_t     field_id = -1;
    DrakenType  type = DRAKEN_INT64;
    uint8_t     precision = 0;   // DECIMAL / DECIMAL128
    uint8_t     scale = 0;
    DrakenType  child_type = DRAKEN_INT64;   // ARRAY: the innermost element type
    uint8_t     array_depth = 1;             // ARRAY: levels of list nesting
    // A nested column of a TABLE: the shape the table declares, which each file's column must say
    // too (same field ids, same struct member names) or the read fails naming the field. Empty =
    // none declared (READ_ORC, a folder: the file is the schema).
    std::vector<DeclaredNode> declared;
    bool        nullable = true;
};

// One top-level column of the file, as it states itself.
struct FileColumn {
    std::string name;
    Kind        kind = Kind::Long;
    std::string kind_name;         // the ORC type kind, spelled
    int64_t     iceberg_id = -1;   // the `iceberg.id` type attribute; -1 = absent
    bool        readable = false;
    std::string refusal;           // why not, when !readable
    DrakenType  type = DRAKEN_INT64;   // what it decodes to
    uint8_t     precision = 0;
    uint8_t     scale = 0;
    uint32_t    type_id = 0;
    uint32_t    fixed_length = 0;  // iceberg.binary-type=FIXED: every value must be exactly this long
    // STRUCT / MAP / LIST. `type` is NVARCHAR (one JSON value per row) or, for a LIST of a plain
    // scalar, ARRAY with `child_type` the element.
    bool        nested = false;
    DrakenType  child_type = DRAKEN_INT64;   // ARRAY: the innermost element
    uint8_t     array_depth = 0;             // ARRAY: levels of list nesting (a list of lists of a scalar is 2)
};

class OrcFile {
public:
    // Parses the file tail. Throws on a file that is not ORC, is corrupt, is encrypted,
    // or uses a compression this reader does not support.
    OrcFile(const uint8_t* data, size_t size);
    ~OrcFile();
    OrcFile(const OrcFile&) = delete;
    OrcFile& operator=(const OrcFile&) = delete;

    uint64_t num_rows() const;
    size_t stripe_count() const;
    uint32_t stripe_rows(size_t stripe) const;
    Compression compression() const;
    std::string compression_name() const;
    const std::vector<FileColumn>& file_columns() const;

    // Choose what decode_stripe() returns: `decode` names (a subset of `reader`'s
    // columns, in output order), each read as its ReaderColumn. A reader column is
    // matched to the file's by `iceberg.id` when `match_by_id`, else by name; one the
    // file lacks is a NULL constant when nullable and an error when not. With
    // `match_by_id`, a file none of whose columns carries an `iceberg.id` is an error:
    // matching it would resolve every column to NULL without a word.
    void bind(const std::vector<ReaderColumn>& reader, const std::vector<std::string>& decode,
              bool match_by_id);

    // Every column of the file, each as it states itself, by name.
    void bind_all();

    // Decode one stripe's bound columns (none bound = a batch of only the row count).
    Batch decode_stripe(size_t stripe) const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace rugo::orc
