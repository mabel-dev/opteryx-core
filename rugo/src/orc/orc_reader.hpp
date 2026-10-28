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
// stripes of one file at once. A StripeCursor decodes one stripe a row range at a time.

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

// One conjunct of a WHERE the reader can test against an integer or date column's statistics
// (docs/ORC_READER_DESIGN.md §6.7): `column` is a bound column (an index into bind()'s `decode`),
// the values are the column's stored integers (a date is days since 1970-01-01).
enum class PruneOp : uint8_t { Eq = 0, NotEq = 1, Gt = 2, GtEq = 3, Lt = 4, LtEq = 5, InList = 6 };

struct PruneTerm {
    uint32_t column = 0;
    PruneOp op = PruneOp::Eq;
    std::vector<int64_t> values;   // one; InList: every listed value
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

    // Read compressed streams through the process's chunk cache (draken core/chunk_cache.h;
    // docs/ORC_CHUNK_CACHE_DESIGN.md), keyed by `path` — files are immutable for the process's
    // life. Whole streams are looked up and, with `admit`, filled on a miss; a part of a stream
    // (a row-index seek) is read from a cached whole one and never fills.
    void use_chunk_cache(const std::string& path, bool admit);

    // Read the file's stripe statistics (the Metadata section), once, before stripe_may_match.
    void load_stripe_statistics();
    // False only when the stripe's statistics prove no row of it passes every term (§6.7).
    bool stripe_may_match(size_t stripe, const std::vector<PruneTerm>& terms) const;

private:
    friend class StripeCursor;
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

// One stripe's bound columns (or `columns`, indices into them, in that order), decoded a row range
// at a time, so a stripe becomes batches of a bounded size (docs/ORC_READER_DESIGN.md §6.3). Ranges
// come in row order. A range may skip ahead: with a row index (§6.5) a range that starts a row
// group seeks there and decompresses only as far as its run needs; without one, the rows between
// are decoded and dropped. Nested columns are decoded whole for the stripe. One thread at a time;
// the OrcFile must stay bound, unchanged, for the cursor's lifetime.
class StripeCursor {
public:
    StripeCursor(const OrcFile& file, size_t stripe, const std::vector<uint32_t>* columns = nullptr);
    ~StripeCursor();
    StripeCursor(const StripeCursor&) = delete;
    StripeCursor& operator=(const StripeCursor&) = delete;

    uint32_t rows() const { return rows_; }
    uint32_t remaining() const { return rows_ - at_; }
    // Rows per row group; 0 when the file has no row index.
    uint32_t stride() const;
    // Whether every primitive column of the cursor has the stripe's row index (read on first ask).
    bool indexed();

    // Per row group (indexed() must be true): false only when its statistics prove no row of it
    // passes every term (§6.7). Every term's column must be one of the cursor's.
    std::vector<uint8_t> groups_may_match(const std::vector<PruneTerm>& terms);

    // The next `n` rows (none bound = a batch of only the row count).
    Batch next(uint32_t n);
    // Rows [first, first + n), at or after the last range's end, where [first, run_end) is the run
    // of rows the caller will read in order from `first` before it skips again.
    Batch read(uint32_t first, uint32_t n, uint32_t run_end);

private:
    struct Col;
    struct Stripe;
    uint32_t rows_ = 0;
    uint32_t at_ = 0;
    uint32_t window_end_ = 0;   // the open streams reach this row
    bool opened_ = false;
    std::vector<std::unique_ptr<Col>> cols_;
    std::unique_ptr<Stripe> stripe_;
};

}  // namespace rugo::orc
