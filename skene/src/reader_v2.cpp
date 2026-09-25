// The v2 reader — RETAINED, FROZEN at the v3 bump (2026-09-24). It reads v2
// files for as long as v2 is inside the read window (FORMAT.md §12): directly,
// and as migrate_file's input. It parses the v2 footers with the v2 structs
// frozen in format_v2.h; decoding a column's sections is shared with the v3
// reader (chunk_decode.h) because those bytes did not change between versions.

#include "reader_v2.h"

#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <memory>
#include <vector>

#include "chunk_decode.h"
#include "footer_common.h"
#include "format_v2.h"
#include "skene/checksum.h"
#include "skene/format.h"

// draken — imported, never copied.
#include "core/buffers.h"

namespace skene {
namespace v2 {
namespace {

Status fail(Code code, const char* fmt, ...) __attribute__((format(printf, 2, 3)));
Status fail(Code code, const char* fmt, ...) {
    char buffer[640];
    va_list args;
    va_start(args, fmt);
    std::vsnprintf(buffer, sizeof(buffer), fmt, args);
    va_end(args);
    return Status(code, buffer);
}

// ─── Parsed footer ──────────────────────────────────────────────────────────

struct ParsedColumn {
    ColumnEntryHead           head{};
    std::string               name;
    LogicalTypeDescriptor     logical{};
    bool                      has_statistics = false;
    ColumnStatistics          statistics{};
    std::vector<ParsedColumn> children;
};

// One row group-column's v2 statistics as the file footer carries them,
// including the per-row-group sketch v2 stored and v3 does not.
struct ParsedStatistics {
    bool                  present = false;
    ColumnStatistics      statistics{};
    uint32_t              sketch_k = 0;
    bool                  has_sketch = false;
    std::vector<uint64_t> sketch;
};

struct ParsedRowGroupFooter {
    RowGroupFooterHeader      header{};
    std::string               writer_tag;
    std::vector<ParsedColumn> columns;
    std::vector<SectionEntry> sections;
};

// ─── Parsed FILE footer (the file index) ────────────────────────────────────

using footer::Cursor;
using footer::ParsedSchema;
using footer::fill_schema;
using footer::parse_schema;

struct ParsedFileFooter {
    FileFooterHeader           header{};
    std::string                writer_tag;
    std::vector<RowGroupEntry> row_groups;
    std::vector<ParsedSchema>  schema;
    std::vector<SortKey>       cluster_keys;   // v2; empty == unclustered
    // Row group major, then the schema's depth-first column order.
    std::vector<std::vector<ParsedStatistics>> statistics;
};

Status parse_column(Cursor& cursor, ParsedColumn* out, int depth) {
    // Nesting is ARRAY children only, and draken's own array support is
    // shallow. A bounded depth keeps a corrupt child_count from recursing the
    // stack to death before any other check can fire.
    if (depth > 32)
        return fail(Code::kMalformed,
                    "column nesting exceeds 32 levels; refusing to recurse further");

    if (!cursor.take(&out->head, sizeof(ColumnEntryHead)))
        return fail(Code::kTruncated, "footer ends inside a column directory entry");

    const uint8_t* name = cursor.raw(out->head.name_bytes);
    if (name == nullptr)
        return fail(Code::kTruncated,
                    "column name claims %u bytes but only %zu remain in the footer",
                    out->head.name_bytes, cursor.remaining());
    out->name.assign(reinterpret_cast<const char*>(name), out->head.name_bytes);

    if (out->head.logical_present) {
        if (!cursor.take(&out->logical, sizeof(LogicalTypeDescriptor)))
            return fail(Code::kTruncated,
                        "column '%s' declares a logical type descriptor but the "
                        "footer ends before it", out->name.c_str());
    }

    // child_count is 1 exactly for ARRAY and 0 otherwise; anything else is a
    // corrupt entry, and checking here bounds the loop below.
    const bool is_array = out->head.type == static_cast<uint32_t>(DRAKEN_ARRAY);
    if (is_array && out->head.child_count != 1u)
        return fail(Code::kMalformed,
                    "column '%s' is ARRAY with child_count %u; exactly one child "
                    "is required", out->name.c_str(), out->head.child_count);
    if (!is_array && out->head.child_count != 0u)
        return fail(Code::kMalformed,
                    "column '%s' has child_count %u but type %u is not ARRAY",
                    out->name.c_str(), out->head.child_count, out->head.type);

    out->children.resize(out->head.child_count);
    for (uint32_t i = 0; i < out->head.child_count; ++i)
        SKENE_RETURN_IF_ERROR(parse_column(cursor, &out->children[i], depth + 1));

    return Status::ok();
}

// Reads one column's statistics blob, then its children's, depth first.
//
// A blob LONGER than this build understands is read prefix-first and the
// remainder skipped. That is deliberate and is what lets a statistic be added
// with no version bump: an older reader takes the fields it knows and ignores
// the rest, which costs it a pruning opportunity and nothing else.
// Reads one statistics blob: the fixed prefix this build understands, then the
// KMV sketch when the blob carries one.
//
// A blob LONGER than this build understands is read prefix-first and the
// remainder skipped — that is what lets a statistic be added with no version
// bump, and it is why the sketch is gated on kStatSketch rather than on the
// blob merely being long. A blob that CLAIMS a sketch it cannot hold is
// corruption, not growth, and fails.
Status parse_statistics_blob(const uint8_t* blob, uint32_t declared,
                             const char* what,
                             ColumnStatistics* statistics,
                             bool* has_sketch, uint32_t* sketch_k,
                             std::vector<uint64_t>* sketch) {
    const size_t known = declared < sizeof(ColumnStatistics)
                       ? declared : sizeof(ColumnStatistics);
    std::memcpy(statistics, blob, known);

    if ((statistics->flags & kStatSketch) == 0) return Status::ok();
    if (declared < sizeof(ColumnStatistics) + sizeof(ColumnSketchHeader))
        return fail(Code::kTruncated,
                    "%s declares a KMV sketch but its %u statistics bytes cannot "
                    "hold the sketch header", what, declared);

    ColumnSketchHeader header;
    std::memcpy(&header, blob + sizeof(ColumnStatistics), sizeof(header));
    const uint64_t need = static_cast<uint64_t>(sizeof(ColumnStatistics))
                        + sizeof(ColumnSketchHeader)
                        + static_cast<uint64_t>(header.count) * sizeof(uint64_t);
    if (need > declared)
        return fail(Code::kTruncated,
                    "%s declares a %u-hash KMV sketch needing %llu statistics "
                    "bytes but only %u were written", what, header.count,
                    static_cast<unsigned long long>(need), declared);

    *has_sketch = true;
    *sketch_k = header.k;
    sketch->resize(header.count);
    if (header.count != 0)
        std::memcpy(sketch->data(),
                    blob + sizeof(ColumnStatistics) + sizeof(ColumnSketchHeader),
                    static_cast<size_t>(header.count) * sizeof(uint64_t));
    return Status::ok();
}

Status parse_statistics(Cursor& cursor, ParsedColumn* column) {
    const uint32_t declared = column->head.stats_bytes;
    if (declared > 0) {
        const uint8_t* blob = cursor.raw(declared);
        if (blob == nullptr)
            return fail(Code::kTruncated,
                        "column '%s' declares %u statistics bytes but only %zu "
                        "remain in the footer", column->name.c_str(), declared,
                        cursor.remaining());
        // The row group footer's copy of the blob. Its sketch (if any) is the
        // same one the file footer carries; only the file footer's is reported.
        bool has_sketch = false;
        uint32_t sketch_k = 0;
        std::vector<uint64_t> sketch;
        SKENE_RETURN_IF_ERROR(parse_statistics_blob(blob, declared,
                                                    column->name.c_str(),
                                                    &column->statistics,
                                                    &has_sketch, &sketch_k, &sketch));
        column->has_statistics = true;
    }
    for (ParsedColumn& child : column->children)
        SKENE_RETURN_IF_ERROR(parse_statistics(cursor, &child));
    return Status::ok();
}

Status parse_row_group_footer(const uint8_t* footer, uint32_t footer_bytes,
                              uint32_t row_group, ParsedRowGroupFooter* out) {
    Cursor cursor(footer, footer_bytes);

    if (!cursor.take(&out->header, sizeof(RowGroupFooterHeader)))
        return fail(Code::kTruncated,
                    "row group %u footer is too small to hold its header",
                    row_group);

    const uint8_t* tag = cursor.raw(out->header.writer_tag_bytes);
    if (tag == nullptr)
        return fail(Code::kTruncated,
                    "row group %u: writer tag claims %u bytes but only %zu "
                    "remain in the footer", row_group,
                    out->header.writer_tag_bytes, cursor.remaining());
    out->writer_tag.assign(reinterpret_cast<const char*>(tag),
                           out->header.writer_tag_bytes);

    // Bound the counts by what could possibly fit before allocating for them: a
    // corrupt column_count of 4 billion must not become a 4-billion-element
    // reserve.
    if (static_cast<uint64_t>(out->header.column_count) * sizeof(ColumnEntryHead)
            > cursor.remaining())
        return fail(Code::kMalformed,
                    "row group %u claims %u columns, which cannot fit in its "
                    "remaining %zu footer bytes", row_group,
                    out->header.column_count, cursor.remaining());

    out->columns.resize(out->header.column_count);
    for (uint32_t i = 0; i < out->header.column_count; ++i)
        SKENE_RETURN_IF_ERROR(parse_column(cursor, &out->columns[i], 0));

    if (static_cast<uint64_t>(out->header.section_count) * sizeof(SectionEntry)
            > cursor.remaining())
        return fail(Code::kMalformed,
                    "row group %u claims %u sections, which cannot fit in its "
                    "remaining %zu footer bytes", row_group,
                    out->header.section_count, cursor.remaining());

    out->sections.resize(out->header.section_count);
    for (uint32_t i = 0; i < out->header.section_count; ++i) {
        if (!cursor.take(&out->sections[i], sizeof(SectionEntry)))
            return fail(Code::kTruncated,
                        "row group %u footer ends inside the section directory",
                        row_group);
    }

    // Statistics blobs: same depth-first order as the column directory, skipping
    // columns whose stats_bytes is 0. Located by ORDER, not by an offset.
    for (ParsedColumn& column : out->columns)
        SKENE_RETURN_IF_ERROR(parse_statistics(cursor, &column));

    return Status::ok();
}

// ─── FILE footer ────────────────────────────────────────────────────────────





// Validates one row group directory entry against the object it claims to live
// in. Every field here is an OFFSET a later read follows, so each is checked
// against the file footer's own start — the one bound this reader already knows
// is real, because the tail put it there and the footer checksum covered it.
Status validate_row_group_entry(const RowGroupEntry& entry, uint32_t index,
                                uint64_t file_footer_offset, uint64_t expected_first_row) {
    if (entry.reserved != 0)
        return fail(Code::kMalformed,
                    "row group %u: reserved bytes are %u, not 0", index,
                    entry.reserved);

    if (entry.first_row != expected_first_row)
        return fail(Code::kMalformed,
                    "row group %u declares first_row %llu but the row groups "
                    "before it hold %llu rows", index,
                    static_cast<unsigned long long>(entry.first_row),
                    static_cast<unsigned long long>(expected_first_row));

    if (entry.data_offset < kFileHeadBytes)
        return fail(Code::kMalformed,
                    "row group %u: data region starts at %llu, inside the "
                    "%zu-byte head", index,
                    static_cast<unsigned long long>(entry.data_offset),
                    kFileHeadBytes);

    // Written so an overflowing sum cannot wrap past the comparison.
    if (entry.data_bytes > file_footer_offset
            || entry.data_offset > file_footer_offset - entry.data_bytes)
        return fail(Code::kMalformed,
                    "row group %u: data region spans [%llu, %llu) which runs "
                    "past the file footer at %llu", index,
                    static_cast<unsigned long long>(entry.data_offset),
                    static_cast<unsigned long long>(entry.data_offset + entry.data_bytes),
                    static_cast<unsigned long long>(file_footer_offset));

    if (entry.footer_bytes == 0)
        return fail(Code::kMalformed,
                    "row group %u declares a zero-byte footer, which cannot "
                    "hold even its header", index);

    if (entry.footer_offset < entry.data_offset + entry.data_bytes)
        return fail(Code::kMalformed,
                    "row group %u: its footer at %llu overlaps its own data "
                    "region, which ends at %llu", index,
                    static_cast<unsigned long long>(entry.footer_offset),
                    static_cast<unsigned long long>(entry.data_offset + entry.data_bytes));

    if (entry.footer_bytes > file_footer_offset
            || entry.footer_offset > file_footer_offset - entry.footer_bytes)
        return fail(Code::kMalformed,
                    "row group %u: its footer spans [%llu, %llu) which runs past "
                    "the file footer at %llu", index,
                    static_cast<unsigned long long>(entry.footer_offset),
                    static_cast<unsigned long long>(entry.footer_offset)
                        + entry.footer_bytes,
                    static_cast<unsigned long long>(file_footer_offset));

    return Status::ok();
}

Status parse_file_footer(const uint8_t* footer, uint32_t footer_bytes,
                         uint64_t file_footer_offset, ParsedFileFooter* out) {
    Cursor cursor(footer, footer_bytes);

    if (!cursor.take(&out->header, sizeof(FileFooterHeader)))
        return fail(Code::kTruncated,
                    "file footer is too small to hold its header");

    // The guard that separates this layout from the single-row-group v1 files
    // written before it. Those are framed identically and their footer checksum
    // verifies, so nothing else in the file distinguishes them — parsing one as
    // a file index would read a row count as a magic and a writer tag as a row
    // group directory.
    if (out->header.footer_magic != kFileFooterMagic)
        return fail(Code::kMalformed,
                    "file footer magic is 0x%08X, not 0x%08X. This is almost "
                    "certainly a .skene file written before row groups were "
                    "packed into files, when one file WAS one row group; v1 was "
                    "draft and its layout changed. Regenerate the file with the "
                    "current writer.",
                    out->header.footer_magic, kFileFooterMagic);

    if (out->header.footer_version != kFileFooterVersion)
        return fail(Code::kUnsupportedVersion,
                    "file footer declares layout version %u; the v2 reader "
                    "implements %u", out->header.footer_version,
                    kFileFooterVersion);

    if (out->header.reserved != 0)
        return fail(Code::kMalformed,
                    "file footer header reserved bytes are %u, not 0",
                    out->header.reserved);

    const uint8_t* tag = cursor.raw(out->header.writer_tag_bytes);
    if (tag == nullptr)
        return fail(Code::kTruncated,
                    "file writer tag claims %u bytes but only %zu remain in the "
                    "file footer", out->header.writer_tag_bytes, cursor.remaining());
    out->writer_tag.assign(reinterpret_cast<const char*>(tag),
                           out->header.writer_tag_bytes);

    if (out->header.row_group_count == 0)
        return fail(Code::kMalformed,
                    "file declares 0 row groups; a .skene file with no row "
                    "groups describes no data");

    if (static_cast<uint64_t>(out->header.row_group_count) * sizeof(RowGroupEntry)
            > cursor.remaining())
        return fail(Code::kMalformed,
                    "file claims %u row groups, which cannot fit in its "
                    "remaining %zu footer bytes",
                    out->header.row_group_count, cursor.remaining());

    out->row_groups.resize(out->header.row_group_count);
    uint64_t running_rows = 0;
    for (uint32_t i = 0; i < out->header.row_group_count; ++i) {
        if (!cursor.take(&out->row_groups[i], sizeof(RowGroupEntry)))
            return fail(Code::kTruncated,
                        "file footer ends inside the row group directory");
        SKENE_RETURN_IF_ERROR(validate_row_group_entry(
            out->row_groups[i], i, file_footer_offset, running_rows));
        running_rows += out->row_groups[i].row_count;
    }

    if (running_rows != out->header.row_count)
        return fail(Code::kMalformed,
                    "file declares %llu rows but its row groups hold %llu",
                    static_cast<unsigned long long>(out->header.row_count),
                    static_cast<unsigned long long>(running_rows));

    if (static_cast<uint64_t>(out->header.column_count) * sizeof(SchemaEntryHead)
            > cursor.remaining())
        return fail(Code::kMalformed,
                    "file claims %u columns, which cannot fit in its remaining "
                    "%zu footer bytes", out->header.column_count,
                    cursor.remaining());

    out->schema.resize(out->header.column_count);
    for (uint32_t i = 0; i < out->header.column_count; ++i)
        SKENE_RETURN_IF_ERROR(parse_schema(cursor, &out->schema[i], 0));

    uint32_t flat_columns = 0;
    for (const ParsedSchema& node : out->schema) flat_columns += footer::count_schema_nodes(node);

    SKENE_RETURN_IF_ERROR(
        footer::parse_cluster_spec(cursor, out->header.column_count, &out->cluster_keys));

    // Per-row-group statistics, row group major, in the schema's depth-first
    // order. Each blob is length-prefixed, so a blob longer than this build
    // understands is read prefix-first and the rest skipped — the same growth
    // rule the row group footers' blobs follow.
    out->statistics.resize(out->header.row_group_count);
    for (uint32_t g = 0; g < out->header.row_group_count; ++g) {
        out->statistics[g].resize(flat_columns);
        for (uint32_t c = 0; c < flat_columns; ++c) {
            uint32_t declared = 0;
            if (!cursor.take(&declared, sizeof(declared)))
                return fail(Code::kTruncated,
                            "file footer ends inside row group %u's statistics", g);
            if (declared == 0) continue;
            const uint8_t* blob = cursor.raw(declared);
            if (blob == nullptr)
                return fail(Code::kTruncated,
                            "row group %u column %u declares %u statistics bytes "
                            "but only %zu remain in the file footer", g, c,
                            declared, cursor.remaining());
            char what[64];
            std::snprintf(what, sizeof(what), "row group %u column %u", g, c);
            ParsedStatistics& slot = out->statistics[g][c];
            SKENE_RETURN_IF_ERROR(parse_statistics_blob(
                blob, declared, what, &slot.statistics, &slot.has_sketch,
                &slot.sketch_k, &slot.sketch));
            out->statistics[g][c].present = true;
        }
    }

    return Status::ok();
}

// ─── Parsed footer → shared chunk decode ────────────────────────────────────

// A v2 column entry as the shared decoder's ChunkNode. Every node of one row
// group shares that row group's resolver.
void to_chunk_node(const ParsedColumn& parsed, const chunk::SectionResolver* resolver,
                   chunk::ChunkNode* out) {
    const ColumnEntryHead& h = parsed.head;
    out->head.field_id               = h.field_id;
    out->head.type                   = h.type;
    out->head.logical_present        = h.logical_present;
    out->head.vector_flags           = h.vector_flags;
    out->head.selection_kind         = h.selection_kind;
    out->head.value_order            = h.value_order;
    out->head.string_payloads_elided = h.string_payloads_elided;
    out->head.length                 = h.length;
    out->head.data_length            = h.data_length;
    out->head.section_index          = h.section_index;
    out->head.section_count          = h.section_count;
    out->head.index_section_index    = h.index_section_index;
    out->head.index_section_count    = h.index_section_count;
    out->head.string_slot_count      = h.string_slot_count;
    out->head.string_arena_used      = h.string_arena_used;
    out->head.string_arena_cap       = h.string_arena_cap;
    out->name           = parsed.name;
    out->logical        = parsed.logical;
    out->has_statistics = parsed.has_statistics;
    out->statistics     = parsed.statistics;
    out->resolver       = resolver;
    out->children.resize(parsed.children.size());
    for (size_t i = 0; i < parsed.children.size(); ++i)
        to_chunk_node(parsed.children[i], resolver, &out->children[i]);
}

// v2 stored one sketch per row group; the file-level answer v3 carries is their
// exact KMV union — the K smallest of the combined hashes. One row group
// without a sketch voids it, because a union missing a contributor would
// under-count with nothing to say so. So would unequal K: the union is only
// exact at a single K.
ColumnSketch union_row_group_sketches(const ParsedFileFooter& footer, size_t column) {
    ColumnSketch out;
    uint32_t k = 0;
    std::vector<uint64_t> all;
    for (const std::vector<ParsedStatistics>& row_group : footer.statistics) {
        const ParsedStatistics& slot = row_group[column];
        if (!slot.present || !slot.has_sketch) return ColumnSketch();
        if (k == 0) k = slot.sketch_k;
        if (slot.sketch_k != k || k == 0) return ColumnSketch();
        all.insert(all.end(), slot.sketch.begin(), slot.sketch.end());
    }
    std::sort(all.begin(), all.end());
    all.erase(std::unique(all.begin(), all.end()), all.end());
    if (all.size() > k) all.resize(k);
    out.hash_family = kSketchFamilyXxh3Value;
    out.k           = k;
    out.hashes      = std::move(all);
    return out;
}



// Opens ONE row group from its entry in an ALREADY-VERIFIED file footer:
// verifies the row group footer's checksum against that entry, then parses it.
//
// The two-step is the point of the layout: the file footer is small and is the
// only thing a pruning reader fetches, and a row group's directory is opened
// only once that reader has decided to read it. Taking the entry rather than
// the file footer is what lets a caller holding a parsed directory
// (skene::FileReader) open row group after row group without re-verifying and
// re-parsing the whole file footer — O(row groups in the file) — each time.
Status open_row_group_at(const uint8_t* file, const RowGroupEntry& entry,
                         uint32_t row_group, ParsedRowGroupFooter* out) {
    // The row group footer's checksum lives in the file footer, which has
    // already been checksum-verified as a whole — so this is a check against
    // something already trusted, not against a number sitting beside the bytes
    // it claims to cover.
    const uint64_t actual = checksum_xxh3_64(file + entry.footer_offset,
                                             entry.footer_bytes);
    if (actual != entry.footer_checksum && checksum_must_match())
        return fail(Code::kChecksumMismatch,
                    "row group %u footer checksum mismatch: recorded %llu, "
                    "computed %llu — its directory is corrupt and every offset "
                    "in it is suspect", row_group,
                    static_cast<unsigned long long>(entry.footer_checksum),
                    static_cast<unsigned long long>(actual));

    SKENE_RETURN_IF_ERROR(parse_row_group_footer(file + entry.footer_offset,
                                                 entry.footer_bytes, row_group, out));

    if (out->header.row_count != entry.row_count)
        return fail(Code::kMalformed,
                    "row group %u's footer declares %llu rows but the file's row "
                    "group directory says %llu", row_group,
                    static_cast<unsigned long long>(out->header.row_count),
                    static_cast<unsigned long long>(entry.row_count));

    return Status::ok();
}

// Parses the file footer and returns row group `row_group`'s entry from it —
// the bounds check lives here, once, for every path that starts from the bytes.
Status locate_row_group(const uint8_t* file, uint64_t file_footer_offset,
                        uint32_t file_footer_bytes, uint32_t row_group,
                        ParsedFileFooter* file_footer, RowGroupEntry* out_entry) {
    SKENE_RETURN_IF_ERROR(parse_file_footer(file + file_footer_offset,
                                            file_footer_bytes, file_footer_offset,
                                            file_footer));

    if (row_group >= file_footer->row_groups.size())
        return fail(Code::kMalformed,
                    "row group %u was requested but this file has %zu",
                    row_group, file_footer->row_groups.size());

    *out_entry = file_footer->row_groups[row_group];
    return Status::ok();
}

// Resolver and chunk nodes for one opened row group. The row group's DATA +
// INDEX region is both extents: v2 bounded every section by its row group.
struct OpenedRowGroup {
    ParsedRowGroupFooter              footer;
    chunk::ByteSource                 bytes;
    std::unique_ptr<chunk::SectionResolver> resolver;
    std::string                       extent_name;
    std::vector<chunk::ChunkNode>     nodes;
};

Status open_row_group_nodes(const uint8_t* file, const RowGroupEntry& entry,
                            uint32_t row_group, OpenedRowGroup* out) {
    SKENE_RETURN_IF_ERROR(open_row_group_at(file, entry, row_group, &out->footer));
    out->bytes = chunk::ByteSource({FetchedRange{entry.data_offset, entry.data_bytes,
                                                 file + entry.data_offset}});
    out->extent_name = "row group " + std::to_string(row_group) + "'s region";
    const chunk::Extent region{entry.data_offset, entry.data_bytes};
    out->resolver = std::make_unique<chunk::SectionResolver>(
        out->bytes, out->footer.sections, region, region, out->extent_name.c_str(), 2u);
    out->nodes.resize(out->footer.columns.size());
    for (size_t i = 0; i < out->footer.columns.size(); ++i)
        to_chunk_node(out->footer.columns[i], out->resolver.get(), &out->nodes[i]);
    return Status::ok();
}

}  // namespace

Status read_metadata(const uint8_t* file, size_t file_bytes,
                     uint64_t footer_offset, uint32_t footer_bytes,
                     FileMetadata* out, ReaderState* state) {
    (void)file_bytes;
    ParsedFileFooter footer;
    SKENE_RETURN_IF_ERROR(parse_file_footer(file + footer_offset, footer_bytes,
                                            footer_offset, &footer));
    if (state != nullptr) state->directory = footer.row_groups;

    out->version            = 2u;
    out->row_count          = footer.header.row_count;
    out->created_at_unix_us = footer.header.created_at_unix_us;
    out->writer_tag         = footer.writer_tag;
    out->cluster_keys       = footer.cluster_keys;
    out->block_row_groups   = 0;   // v2 recorded none
    std::memcpy(out->file_uuid, footer.header.file_uuid, sizeof(out->file_uuid));

    out->columns.resize(footer.schema.size());
    for (size_t i = 0; i < footer.schema.size(); ++i)
        fill_schema(footer.schema[i], &out->columns[i]);

    const size_t nodes = footer.statistics.empty() ? 0 : footer.statistics[0].size();
    out->sketches.resize(nodes);
    for (size_t c = 0; c < nodes; ++c)
        out->sketches[c] = union_row_group_sketches(footer, c);

    out->row_groups.resize(footer.row_groups.size());
    for (size_t i = 0; i < footer.row_groups.size(); ++i) {
        const RowGroupEntry& entry = footer.row_groups[i];
        RowGroupSummary& summary = out->row_groups[i];
        summary.row_count = entry.row_count;
        summary.first_row = entry.first_row;
        summary.column_statistics.resize(footer.statistics[i].size());
        for (size_t c = 0; c < footer.statistics[i].size(); ++c) {
            summary.column_statistics[c].present    = footer.statistics[i][c].present;
            summary.column_statistics[c].statistics = footer.statistics[i][c].statistics;
        }
    }
    return Status::ok();
}

Status read_row_group_metadata(const uint8_t* file, size_t file_bytes,
                               uint64_t footer_offset, uint32_t footer_bytes,
                               uint32_t row_group, RowGroupMetadata* out) {
    (void)file_bytes;
    ParsedFileFooter file_footer;
    RowGroupEntry entry{};
    SKENE_RETURN_IF_ERROR(locate_row_group(file, footer_offset, footer_bytes, row_group,
                                           &file_footer, &entry));
    OpenedRowGroup opened;
    SKENE_RETURN_IF_ERROR(open_row_group_nodes(file, entry, row_group, &opened));

    out->row_count = opened.footer.header.row_count;
    out->columns.resize(opened.nodes.size());
    for (size_t i = 0; i < opened.nodes.size(); ++i)
        SKENE_RETURN_IF_ERROR(chunk::fill_metadata(opened.nodes[i], &out->columns[i]));
    return Status::ok();
}

Status read_morsel_at(const uint8_t* file, const ReaderState& state, uint32_t row_group,
                      const ReadOptions& options, CxxMorsel* out) {
    if (row_group >= state.directory.size())
        return fail(Code::kMalformed, "row group %u was requested but this file has %zu",
                    row_group, state.directory.size());
    OpenedRowGroup opened;
    SKENE_RETURN_IF_ERROR(
        open_row_group_nodes(file, state.directory[row_group], row_group, &opened));
    return chunk::build_row_group(opened.nodes, opened.footer.header.row_count,
                                  options, out);
}

}  // namespace v2
}  // namespace skene
