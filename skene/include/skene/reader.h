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
    ZoneMap          zone_map;

    // Serialized bloom filter, empty when the column has none. Probe it with
    // bloom_may_contain() rather than interpreting the bytes.
    std::vector<uint8_t> bloom;

    std::vector<ColumnMetadata> children;  // ARRAY only
};

struct FileMetadata {
    uint16_t    version = 0;
    uint64_t    row_count = 0;
    uint8_t     file_uuid[16] = {};
    uint64_t    created_at_unix_us = 0;
    std::string writer_tag;
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

// Parses the footer. Does not touch the data region, so this is cheap and is the
// input to any pruning decision.
Status read_metadata(const void* file, size_t file_bytes, FileMetadata* out);

// Reconstructs a morsel. Validates before interpreting: magic, version,
// endianness, checksum algorithm, declared extents against the real size, the
// footer checksum, then every section's checksum before that section is used,
// then structural consistency (selection kind against the counts, code bounds,
// the string arena invariants, array offset monotonicity).
Status read_morsel(const void* file, size_t file_bytes, const ReadOptions& options,
                   CxxMorsel* out);

inline Status read_morsel(const void* file, size_t file_bytes, CxxMorsel* out) {
    return read_morsel(file, file_bytes, ReadOptions(), out);
}

}  // namespace skene
