#pragma once
// skene/reader.h — read a .skene file back into a CxxMorsel.
//
// Versioned from v1. A build reads at most two versions (the one it writes and
// its predecessor) and dispatches on the file's version; migration needs the
// older reader present in the SOURCE, not merely in a released binary, so the
// per-version split exists from the start rather than being retrofitted when it
// is first needed.
//
// Native end to end. No Python, no fallback, no partial read: the format copies
// buffers verbatim and rebuilds absolute pointers from stored offsets, so
// continuing past a detected inconsistency is memory corruption rather than a
// wrong answer.

#include <cstdint>
#include <string>
#include <vector>

#include "skene/format.h"
#include "skene/status.h"

// draken — imported, never copied.
#include "morsels/cxx_morsel.h"

namespace skene {

// ─── Metadata ───────────────────────────────────────────────────────────────

// Per-row-chunk code bounds (FORMAT.md §9.3), decoded.
//
// Present on every column of an orderable type with more than one chunk's worth
// of rows. The bounds are VALUE ordinals, so they mean the same thing whatever
// the column's encoding shape or ordering — see ZoneMapEntry in format.h.
//
// Coverage is guaranteed; effectiveness is not, and it does NOT come from value
// ordering. Value ordering sorts the dictionary and rewrites codes while leaving
// logical row order untouched — so the values a chunk sees are in original row
// order either way, and an ordered column's zone map is no tighter than an
// unordered one's.
//
// What makes bounds tight is CLUSTERING: values that vary slowly across rows give
// narrow ranges, values scattered across rows give ranges so wide they skip
// nothing. That is a property of the data as it arrived, not of anything the
// writer does. It is also why a bloom filter is not redundant with a zone map —
// on an unclustered column the zone map prunes nothing and the bloom is the only
// intra-file filter there is.
struct ZoneMap {
    uint32_t                  chunk_rows = 0;
    std::vector<ZoneMapEntry> chunks;

    bool present() const { return chunk_rows > 0 && !chunks.empty(); }

    // Rows covered by chunk `index`, clamped to the column length.
    void chunk_rows_range(size_t index, uint32_t length,
                          uint32_t* begin, uint32_t* end) const {
        *begin = static_cast<uint32_t>(index) * chunk_rows;
        const uint64_t stop = static_cast<uint64_t>(*begin) + chunk_rows;
        *end = stop < length ? static_cast<uint32_t>(stop) : length;
    }

    // True when chunk `index` could hold a value whose ordinal is in [low, high].
    // A false answer is PROOF the chunk holds no matching row; a true answer is
    // only "cannot rule it out", so a caller must still evaluate the rows it reads.
    //
    // An all-null chunk carries an empty range (min > max) and is therefore ruled
    // out for every probe, which is correct: a null satisfies no comparison.
    bool chunk_may_contain(size_t index, int64_t low, int64_t high) const {
        if (index >= chunks.size()) return true;
        return !(chunks[index].max_ordinal < low || chunks[index].min_ordinal > high);
    }
};

struct ColumnMetadata {
    std::string           name;
    uint32_t              field_id = 0;
    uint32_t              type = 0;          // DrakenType
    bool                  logical_present = false;
    LogicalTypeDescriptor logical{};
    uint32_t              length = 0;
    uint32_t              data_length = 0;
    uint8_t               vector_flags = 0;
    SelectionKind         selection_kind = SelectionKind::kIdentity;
    ValueOrder            value_order = ValueOrder::kAsWritten;

    // Contiguous byte extent covering this column AND its descendants. This is
    // the "read one column with one range request" affordance: fetch exactly
    // [byte_offset, byte_offset + byte_bytes) and the column is fully readable.
    uint64_t byte_offset = 0;
    uint64_t byte_bytes  = 0;

    bool             has_statistics = false;
    ColumnStatistics statistics{};
    // KMV min-hashes read from the tail of the statistics blob, ascending;
    // empty when the writer stored none. MERGEABLE — union two sketches by
    // taking the K smallest of their combined hashes. Only ever with ANOTHER
    // SKENE sketch: these are skene's XXH3 dedup hashes, not draken's
    // Vector.hash(), so they do not mix with ANALYZE/catalog sketches
    // (format.h, ColumnSketchHeader).
    std::vector<uint64_t> sketch;
    ZoneMap          zone_map;

    // Serialized bloom filter, empty when the column has none. Probe it with
    // bloom_may_contain() rather than interpreting the bytes.
    std::vector<uint8_t> bloom;

    std::vector<ColumnMetadata> children;  // ARRAY only
};

// Identity and type of one column: the part that CANNOT vary between the file's
// row groups, and all the FILE FOOTER carries about a column. Everything
// per-row-group (lengths, encoding shape, extents, zone maps, blooms) is in
// ColumnMetadata and needs that row group's own footer.
struct ColumnSchema {
    std::string           name;
    uint32_t              field_id = 0;
    uint32_t              type = 0;          // DrakenType
    bool                  logical_present = false;
    LogicalTypeDescriptor logical{};
    std::vector<ColumnSchema> children;      // ARRAY only
};

// One column's statistics in one row group, read straight out of the file
// footer. `present` means TRACKED — absent is never "zero" (format.h).
struct RowGroupColumnStatistics {
    bool             present = false;
    ColumnStatistics statistics{};
    // KMV min-hashes read from the tail of the statistics blob, ascending;
    // empty when the writer stored none. MERGEABLE — union two sketches by
    // taking the K smallest of their combined hashes. Only ever with ANOTHER
    // SKENE sketch: these are skene's XXH3 dedup hashes, not draken's
    // Vector.hash(), so they do not mix with ANALYZE/catalog sketches
    // (format.h, ColumnSketchHeader).
    std::vector<uint64_t> sketch;
};

// One row group as the FILE FOOTER describes it.
//
// Everything here is reachable from the file footer alone, which is the point:
// a reader prunes on `column_statistics` and only then range-reads the surviving
// row groups. Nothing in this struct required opening a row group footer.
struct RowGroupSummary {
    uint64_t row_count = 0;
    uint64_t first_row = 0;      // this row group's first row, in file row order

    // The row group's DATA + INDEX extent — everything but its footer.
    uint64_t byte_offset = 0;
    uint64_t byte_bytes  = 0;

    // Its own footer, which is where its column and section directories live.
    uint64_t footer_offset = 0;
    uint32_t footer_bytes  = 0;

    // Depth-first over FileMetadata::columns, ARRAY children included — the same
    // order the schema directory is written in, so index i means the same column
    // in every row group.
    std::vector<RowGroupColumnStatistics> column_statistics;
};

struct FileMetadata {
    uint16_t    version = 0;
    uint64_t    row_count = 0;   // TOTAL across every row group
    uint8_t     file_uuid[16] = {};
    uint64_t    created_at_unix_us = 0;
    std::string writer_tag;
    std::vector<ColumnSchema>    columns;
    std::vector<RowGroupSummary> row_groups;

    // v2: the sort keys the file's rows are GLOBALLY ordered by, verified by
    // the writer over the actual rows. Empty means unclustered — which is what
    // every v1 file reports, since v1 had no way to say otherwise.
    std::vector<SortKey> cluster_keys;
};

// One row group in full, from its own footer.
struct RowGroupMetadata {
    uint64_t                    row_count = 0;
    std::vector<ColumnMetadata> columns;
};

// ─── Remote reads ───────────────────────────────────────────────────────────

// Given the last kFileTailBytes of an object and its total size, returns where
// the footer lives — so a remote reader does tail-request then footer-request
// without pulling the whole object. Validates the tail before trusting it.
//
// A caller wanting the optional index sections too SHOULD extend the second
// request backwards: the INDEX region is contiguous with the footer for exactly
// this reason.
Status footer_extent(const void* tail, size_t tail_bytes, uint64_t file_bytes,
                     uint64_t* out_offset, uint64_t* out_bytes);

// ─── Reading ────────────────────────────────────────────────────────────────

struct ReadOptions {
    // Column identities to materialize. Empty means all of them. A name that is
    // not present is an error, not a silently-missing column — a caller asking
    // for a column that is not there has a bug, and returning fewer columns than
    // requested hides it.
    std::vector<std::string> columns;

    // LENGTH-ONLY decode. Parallel to `columns` (or empty, meaning "none"): a
    // non-zero entry says the caller has PROVEN every read of that column is
    // answerable from a value's stored length, so the long-form payload bytes
    // are never dereferenced.
    //
    // A skene string column stores its slots (length | prefix | hash | arena
    // offset) and its long-form payloads in SEPARATE sections, so honouring this
    // is not a cheaper copy — the arena section is never materialized at all,
    // which is the whole cost. What comes back records every value's true length
    // and its 4-byte prefix; long slots carry STR_ELIDED_PAYLOAD_OFFSET, so a
    // read of a payload that was skipped faults instead of returning adjacent
    // bytes, and DrakenStringArena.payloads_elided states it explicitly.
    //
    // Restricted to VARCHAR/VARBINARY, and enforced here rather than assumed:
    // NVARCHAR's LENGTH is a codepoint count that scans the bytes, and VARIANT
    // holds JSON that is parsed, so neither is length-answerable and asking for
    // it is a caller bug. Setting an entry for a non-string column is likewise
    // an error, never a no-op.
    //
    // Empty is always legal and always correct: eliding is an optimisation, so
    // a reader that ignores it (v1) returns the same answers, only slower.
    std::vector<uint8_t> length_only;
};

// Probes a column's bloom filter with a value's native bytes — the same bytes
// the column stores, so an int64 is 8 little-endian bytes and a string is its
// content.
//
// A FALSE result is proof the value is absent from the column. A true result
// only means it could not be ruled out, so a caller must still read and check.
// Returns true (cannot rule out) when the column has no filter, which keeps a
// missing accelerator from ever excluding a row.
Status bloom_may_contain(const ColumnMetadata& column, const void* value_bytes,
                         uint32_t value_length, bool* out_may_contain);

// Parses the FILE FOOTER only: the schema, the row group directory, and every
// row group's per-column statistics. Touches no data region and no row group
// footer, so this is cheap and is the input to any pruning decision.
Status read_metadata(const void* file, size_t file_bytes, FileMetadata* out);

// Parses ONE row group's own footer: per-column lengths, encoding shape, byte
// extents, zone maps and blooms. This is the expensive metadata — a row group
// directory is tens of kilobytes on a wide schema — which is exactly why it is
// a separate call reached per row group rather than folded into read_metadata.
Status read_row_group_metadata(const void* file, size_t file_bytes,
                               uint32_t row_group, RowGroupMetadata* out);

// Reconstructs ONE row group as a morsel. Validates before interpreting: magic,
// version, endianness, checksum algorithm, declared extents against the real
// size, the file footer checksum, the row group's footer checksum, then every
// section's checksum before that section is used, then structural consistency
// (selection kind against the counts, code bounds, the string arena invariants,
// array offset monotonicity).
//
// `row_group` is REQUIRED and has no default. A default of 0 would silently read
// one sixteenth of a packed file and return a well-formed morsel while doing it,
// which is the failure this format's whole validation posture exists to prevent.
Status read_morsel(const void* file, size_t file_bytes, uint32_t row_group,
                   const ReadOptions& options, CxxMorsel* out);

inline Status read_morsel(const void* file, size_t file_bytes, uint32_t row_group,
                          CxxMorsel* out) {
    return read_morsel(file, file_bytes, row_group, ReadOptions(), out);
}

}  // namespace skene
