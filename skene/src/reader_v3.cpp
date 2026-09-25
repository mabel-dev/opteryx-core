// The v3 reader. See reader_v3.h. Every offset in the footer is checked against
// the object before anything follows it; every directory block against the
// checksum the footer recorded for it; every section against its own checksum
// before it is decoded. Nothing is interpreted before it is verified (§11).

#include "reader_v3.h"

#include <algorithm>
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <memory>

#include "footer_common.h"
#include "skene/checksum.h"

// draken — imported, never copied.
#include "core/buffers.h"

namespace skene {
namespace v3 {
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

using footer::Cursor;
using footer::ParsedSchema;

// [offset, offset + bytes) lies inside [begin, end), written so an overflowing
// sum cannot wrap past the comparison.
bool inside(uint64_t offset, uint64_t bytes, uint64_t begin, uint64_t end) {
    return offset >= begin && bytes <= end && offset <= end - bytes;
}

// ─── Footer ─────────────────────────────────────────────────────────────────

// One column summary and its children, depth first, alongside the schema entry
// that describes the same node. Appends to state->nodes; `out_index` receives
// this node's index.
Status parse_summary(Cursor& cursor, const ParsedSchema& schema, ReaderState* state,
                     uint32_t block_count, int depth, uint32_t* out_index) {
    if (depth > 32)
        return fail(Code::kMalformed,
                    "column nesting exceeds 32 levels; refusing to recurse further");

    const uint32_t index = static_cast<uint32_t>(state->nodes.size());
    state->nodes.emplace_back();
    {
        Node& node = state->nodes.back();
        node.name            = schema.name;
        node.field_id        = schema.head.field_id;
        node.type            = schema.head.type;
        node.logical_present = schema.head.logical_present;
        node.logical         = schema.logical;
        node.extent_name     = "column '" + schema.name + "''s extent";

        if (!cursor.take(&node.head, sizeof(ColumnSummaryHead)))
            return fail(Code::kTruncated, "footer ends inside column '%s''s summary",
                        node.name.c_str());
        const ColumnSummaryHead& h = node.head;
        if (h.reserved0 != 0)
            return fail(Code::kMalformed,
                        "column '%s' summary reserved bytes are %u, not 0",
                        node.name.c_str(), h.reserved0);
        if (h.directory_bytes == 0)
            return fail(Code::kMalformed,
                        "column '%s' declares a zero-byte directory block",
                        node.name.c_str());
        if (h.block_count != block_count)
            return fail(Code::kMalformed,
                        "column '%s' declares %u blocks but %u row groups at %u "
                        "per block make %u", node.name.c_str(), h.block_count,
                        state->header.row_group_count, state->header.block_row_groups,
                        block_count);
        if (h.child_count != schema.head.child_count)
            return fail(Code::kMalformed,
                        "column '%s' summary declares %u children but its schema "
                        "entry %u", node.name.c_str(), h.child_count,
                        schema.head.child_count);

        if (static_cast<uint64_t>(h.block_count) * sizeof(BlockExtent) > cursor.remaining())
            return fail(Code::kMalformed,
                        "column '%s' claims %u block extents, which cannot fit in "
                        "the remaining %zu footer bytes", node.name.c_str(),
                        h.block_count, cursor.remaining());
        node.blocks.resize(h.block_count);
        for (uint32_t b = 0; b < h.block_count; ++b)
            if (!cursor.take(&node.blocks[b], sizeof(BlockExtent)))
                return fail(Code::kTruncated, "footer ends inside column '%s''s block "
                            "extents", node.name.c_str());

        SketchRecordHeader sketch{};
        if (!cursor.take(&sketch, sizeof(sketch)))
            return fail(Code::kTruncated, "footer ends inside column '%s''s sketch",
                        node.name.c_str());
        if (sketch.reserved != 0)
            return fail(Code::kMalformed, "column '%s' sketch reserved byte is %u, not 0",
                        node.name.c_str(), sketch.reserved);
        if (sketch.count == 0) {
            if (sketch.hash_family != kSketchFamilyNone || sketch.k != 0)
                return fail(Code::kMalformed,
                            "column '%s' has no sketch hashes but declares family %u "
                            "and k %u", node.name.c_str(), sketch.hash_family, sketch.k);
        } else {
            // One fact, one spelling: v3 sketches are draken's Vector.hash().
            if (sketch.hash_family != kSketchFamilyDrakenVectorHash)
                return fail(Code::kMalformed,
                            "column '%s' sketch declares hash family %u; a v3 file's "
                            "sketches are family %u (draken Vector.hash)",
                            node.name.c_str(), sketch.hash_family,
                            kSketchFamilyDrakenVectorHash);
            if (sketch.k == 0 || sketch.count > sketch.k)
                return fail(Code::kMalformed,
                            "column '%s' sketch holds %u hashes at k %u",
                            node.name.c_str(), sketch.count, sketch.k);
            if (static_cast<uint64_t>(sketch.count) * sizeof(uint64_t) > cursor.remaining())
                return fail(Code::kTruncated,
                            "column '%s' sketch claims %u hashes, which cannot fit in "
                            "the remaining %zu footer bytes", node.name.c_str(),
                            sketch.count, cursor.remaining());
            node.sketch.hash_family = sketch.hash_family;
            node.sketch.k = sketch.k;
            node.sketch.hashes.resize(sketch.count);
            if (!cursor.take(node.sketch.hashes.data(), sketch.count * sizeof(uint64_t)))
                return fail(Code::kTruncated, "footer ends inside column '%s''s sketch",
                            node.name.c_str());
            for (uint32_t i = 1; i < sketch.count; ++i)
                if (node.sketch.hashes[i] <= node.sketch.hashes[i - 1])
                    return fail(Code::kMalformed,
                                "column '%s' sketch hashes are not strictly ascending "
                                "at %u", node.name.c_str(), i);
        }
    }

    for (uint32_t c = 0; c < schema.head.child_count; ++c) {
        uint32_t child = 0;
        SKENE_RETURN_IF_ERROR(parse_summary(cursor, schema.children[c], state,
                                            block_count, depth + 1, &child));
        state->nodes[index].children.push_back(child);
    }
    *out_index = index;
    return Status::ok();
}

// Every node's extents against the object and against each other (§11 item 8).
Status validate_layout(const ReaderState& state) {
    const uint64_t region_begin = kFileHeadBytes;
    const uint64_t region_end   = state.footer_offset;

    uint64_t run_end = region_begin;   // end of the previous node's run
    for (const Node& node : state.nodes) {
        const ColumnSummaryHead& h = node.head;
        const char* name = node.name.c_str();
        if (!inside(h.directory_offset, h.directory_bytes, region_begin, region_end))
            return fail(Code::kMalformed,
                        "column '%s' directory block [%llu, +%u) is outside the data "
                        "region [%llu, %llu)", name,
                        static_cast<unsigned long long>(h.directory_offset),
                        h.directory_bytes, static_cast<unsigned long long>(region_begin),
                        static_cast<unsigned long long>(region_end));
        if (h.directory_offset < run_end)
            return fail(Code::kMalformed,
                        "column '%s' directory block at %llu overlaps the previous "
                        "column's run, which ends at %llu", name,
                        static_cast<unsigned long long>(h.directory_offset),
                        static_cast<unsigned long long>(run_end));
        const uint64_t directory_end = h.directory_offset + h.directory_bytes;
        if (!inside(h.data_offset, h.data_bytes, region_begin, region_end)
                || h.data_offset < directory_end)
            return fail(Code::kMalformed,
                        "column '%s' data extent [%llu, +%llu) is not inside the data "
                        "region after its own directory block", name,
                        static_cast<unsigned long long>(h.data_offset),
                        static_cast<unsigned long long>(h.data_bytes));

        uint64_t block_end = h.data_offset;
        for (size_t b = 0; b < node.blocks.size(); ++b) {
            const BlockExtent& block = node.blocks[b];
            if (block.bytes == 0) {
                if (block.offset != 0)
                    return fail(Code::kMalformed,
                                "column '%s' block %zu is empty but declares offset %llu",
                                name, b, static_cast<unsigned long long>(block.offset));
                continue;
            }
            if (!inside(block.offset, block.bytes, h.data_offset,
                        h.data_offset + h.data_bytes) || block.offset < block_end)
                return fail(Code::kMalformed,
                            "column '%s' block %zu [%llu, +%llu) is outside its data "
                            "extent or overlaps the block before it", name, b,
                            static_cast<unsigned long long>(block.offset),
                            static_cast<unsigned long long>(block.bytes));
            block_end = block.offset + block.bytes;
        }
        run_end = std::max(directory_end, h.data_offset + h.data_bytes);
    }

    // The INDEX region follows the last run; each node's index extent in order.
    uint64_t index_end = run_end;
    for (const Node& node : state.nodes) {
        const ColumnSummaryHead& h = node.head;
        if (h.index_bytes == 0) {
            if (h.index_offset != 0)
                return fail(Code::kMalformed,
                            "column '%s' has no index sections but declares index "
                            "offset %llu", node.name.c_str(),
                            static_cast<unsigned long long>(h.index_offset));
            continue;
        }
        if (!inside(h.index_offset, h.index_bytes, index_end, region_end))
            return fail(Code::kMalformed,
                        "column '%s' index extent [%llu, +%llu) is outside the index "
                        "region or overlaps the column before it", node.name.c_str(),
                        static_cast<unsigned long long>(h.index_offset),
                        static_cast<unsigned long long>(h.index_bytes));
        index_end = h.index_offset + h.index_bytes;
    }
    return Status::ok();
}

// Extent of chunk `g`'s required sections that hold bytes; false when none do.
bool chunk_extent(const Node& node, uint32_t g, uint64_t* begin, uint64_t* end) {
    const ChunkRecord& chunk = node.chunks[g];
    if (chunk.section_count == 0) return false;
    *begin = UINT64_MAX;
    *end = 0;
    for (uint32_t s = 0; s < chunk.section_count; ++s) {
        const SectionEntry& e = node.sections[chunk.section_index + s];
        if (e.stored_bytes == 0) continue;   // nothing to fetch
        *begin = std::min(*begin, e.offset);
        *end = std::max(*end, e.offset + e.stored_bytes);
    }
    return *begin != UINT64_MAX;
}

// The same merge rule as the parquet remote coalescer: ranges sorted by offset,
// a run extended while its cumulative gap stays <= waste_ratio * useful and its
// span <= max_bytes (0 = uncapped). Overlapping and touching ranges always
// merge — that costs no byte.
void coalesce(std::vector<ByteRange> ranges, const FetchPolicy& policy,
              std::vector<ByteRange>* out) {
    out->clear();
    if (ranges.empty()) return;
    std::sort(ranges.begin(), ranges.end(),
              [](const ByteRange& a, const ByteRange& b) { return a.offset < b.offset; });
    ByteRange cur = ranges[0];
    uint64_t useful = cur.bytes, waste = 0;
    for (size_t i = 1; i < ranges.size(); ++i) {
        const ByteRange& next = ranges[i];
        const uint64_t cur_end = cur.offset + cur.bytes;
        const uint64_t next_end = next.offset + next.bytes;
        const uint64_t new_end = std::max(cur_end, next_end);
        const uint64_t gap = next.offset > cur_end ? next.offset - cur_end : 0;
        const uint64_t new_span = new_end - cur.offset;
        const bool touches = gap == 0;
        const bool cheap = static_cast<double>(waste + gap)
                           <= policy.waste_ratio * static_cast<double>(useful + next.bytes);
        const bool fits = policy.max_bytes == 0 || new_span <= policy.max_bytes;
        // A range wholly inside the current one never grows the span.
        const bool contained = next_end <= cur_end;
        if ((touches || cheap) && (fits || contained)) {
            cur.bytes = new_span;
            useful += next.bytes;
            waste += gap;
        } else {
            out->push_back(cur);
            cur = next;
            useful = next.bytes;
            waste = 0;
        }
    }
    out->push_back(cur);
}

// A node's ChunkNode for row group `g`, with its own resolver.
void build_chunk_node(const ReaderState& state, uint32_t n, uint32_t g,
                      const chunk::ByteSource& bytes,
                      std::vector<std::unique_ptr<chunk::SectionResolver>>* resolvers,
                      chunk::ChunkNode* out) {
    const Node& node = state.nodes[n];
    const ChunkRecord& c = node.chunks[g];
    resolvers->push_back(std::make_unique<chunk::SectionResolver>(
        bytes, node.sections,
        chunk::Extent{node.head.data_offset, node.head.data_bytes},
        chunk::Extent{node.head.index_offset, node.head.index_bytes},
        node.extent_name.c_str(), 3u));
    out->head.field_id               = node.field_id;
    out->head.type                   = node.type;
    out->head.logical_present        = node.logical_present;
    out->head.vector_flags           = c.vector_flags;
    out->head.selection_kind         = c.selection_kind;
    out->head.value_order            = c.value_order;
    out->head.string_payloads_elided = c.string_payloads_elided;
    out->head.length                 = c.length;
    out->head.data_length            = c.data_length;
    out->head.section_index          = c.section_index;
    out->head.section_count          = c.section_count;
    out->head.index_section_index    = c.index_section_index;
    out->head.index_section_count    = c.index_section_count;
    out->head.string_slot_count      = c.string_slot_count;
    out->head.string_arena_used      = c.string_arena_used;
    out->head.string_arena_cap       = c.string_arena_cap;
    out->name           = node.name;
    out->logical        = node.logical;
    out->has_statistics = node.stats[g].present;
    out->statistics     = node.stats[g].statistics;
    out->resolver       = resolvers->back().get();
    out->children.resize(node.children.size());
    for (size_t i = 0; i < node.children.size(); ++i)
        build_chunk_node(state, node.children[i], g, bytes, resolvers, &out->children[i]);
}

Status require_attached(const ReaderState& state, uint32_t n) {
    if (!state.nodes[n].attached)
        return fail(Code::kMalformed,
                    "column '%s''s directory block is not attached; a ranged reader "
                    "must attach_directories() for every column it reads",
                    state.nodes[n].name.c_str());
    for (uint32_t child : state.nodes[n].children)
        SKENE_RETURN_IF_ERROR(require_attached(state, child));
    return Status::ok();
}

void collect_subtree(const ReaderState& state, uint32_t n, std::vector<uint32_t>* out) {
    out->push_back(n);
    for (uint32_t child : state.nodes[n].children) collect_subtree(state, child, out);
}

}  // namespace

Status parse_footer(const uint8_t* footer, size_t footer_bytes, uint64_t footer_offset,
                    FileMetadata* out, ReaderState* state) {
    *state = ReaderState();
    state->footer_offset = footer_offset;
    Cursor cursor(footer, footer_bytes);
    FileFooterHeader& h = state->header;

    if (!cursor.take(&h, sizeof(FileFooterHeader)))
        return fail(Code::kTruncated, "footer is too small to hold its header");
    if (h.footer_magic != kFileFooterMagic)
        return fail(Code::kMalformed,
                    "footer magic is 0x%08X, not 0x%08X — this is not a packed "
                    ".skene footer. Regenerate the file with the current writer.",
                    h.footer_magic, kFileFooterMagic);
    if (h.footer_version != kFileFooterVersion)
        return fail(Code::kUnsupportedVersion,
                    "footer declares layout version %u; the v3 reader implements %u",
                    h.footer_version, kFileFooterVersion);
    if (h.reserved != 0)
        return fail(Code::kMalformed, "footer header reserved bytes are %u, not 0",
                    h.reserved);
    if (h.row_group_count == 0)
        return fail(Code::kMalformed,
                    "file declares 0 row groups; a .skene file with no row groups "
                    "describes no data");
    if (h.block_row_groups == 0)
        return fail(Code::kMalformed, "file declares a block size of 0 row groups");
    if (h.data_region_bytes != footer_offset - kFileHeadBytes)
        return fail(Code::kMalformed,
                    "footer declares a %llu-byte data region but the footer starts "
                    "at %llu, after %llu", static_cast<unsigned long long>(h.data_region_bytes),
                    static_cast<unsigned long long>(footer_offset),
                    static_cast<unsigned long long>(footer_offset - kFileHeadBytes));

    const uint8_t* tag = cursor.raw(h.writer_tag_bytes);
    if (tag == nullptr)
        return fail(Code::kTruncated,
                    "writer tag claims %u bytes but only %zu remain in the footer",
                    h.writer_tag_bytes, cursor.remaining());

    // ── Row group table ──
    if (static_cast<uint64_t>(h.row_group_count) * sizeof(RowGroupEntry) > cursor.remaining())
        return fail(Code::kMalformed,
                    "file claims %u row groups, which cannot fit in the remaining "
                    "%zu footer bytes", h.row_group_count, cursor.remaining());
    state->row_groups.resize(h.row_group_count);
    uint64_t running = 0;
    for (uint32_t g = 0; g < h.row_group_count; ++g) {
        RowGroupEntry& entry = state->row_groups[g];
        if (!cursor.take(&entry, sizeof(entry)))
            return fail(Code::kTruncated, "footer ends inside the row group table");
        if (entry.first_row != running)
            return fail(Code::kMalformed,
                        "row group %u declares first_row %llu but the row groups "
                        "before it hold %llu rows", g,
                        static_cast<unsigned long long>(entry.first_row),
                        static_cast<unsigned long long>(running));
        if (entry.row_count > UINT32_MAX)
            return fail(Code::kMalformed,
                        "row group %u declares %llu rows, beyond a vector's 32-bit "
                        "length", g, static_cast<unsigned long long>(entry.row_count));
        running += entry.row_count;
    }
    if (running != h.row_count)
        return fail(Code::kMalformed, "file declares %llu rows but its row groups hold %llu",
                    static_cast<unsigned long long>(h.row_count),
                    static_cast<unsigned long long>(running));

    // ── Schema directory ──
    if (static_cast<uint64_t>(h.column_count) * sizeof(SchemaEntryHead) > cursor.remaining())
        return fail(Code::kMalformed,
                    "file claims %u columns, which cannot fit in the remaining %zu "
                    "footer bytes", h.column_count, cursor.remaining());
    std::vector<ParsedSchema> schema(h.column_count);
    for (uint32_t i = 0; i < h.column_count; ++i)
        SKENE_RETURN_IF_ERROR(footer::parse_schema(cursor, &schema[i], 0));

    std::vector<SortKey> cluster_keys;
    SKENE_RETURN_IF_ERROR(footer::parse_cluster_spec(cursor, h.column_count, &cluster_keys));

    // ── Column summaries ──
    const uint32_t block_count =
        (h.row_group_count + h.block_row_groups - 1u) / h.block_row_groups;
    uint32_t nodes_expected = 0;
    for (const ParsedSchema& node : schema) nodes_expected += footer::count_schema_nodes(node);
    if (static_cast<uint64_t>(nodes_expected) * sizeof(ColumnSummaryHead) > cursor.remaining())
        return fail(Code::kMalformed,
                    "schema describes %u column nodes, whose summaries cannot fit in "
                    "the remaining %zu footer bytes", nodes_expected, cursor.remaining());
    state->nodes.reserve(nodes_expected);
    for (uint32_t i = 0; i < h.column_count; ++i) {
        uint32_t index = 0;
        SKENE_RETURN_IF_ERROR(parse_summary(cursor, schema[i], state, block_count, 0, &index));
        state->top_level.push_back(index);
    }
    SKENE_RETURN_IF_ERROR(validate_layout(*state));

    // ── Per-row-group statistics, column-node major ──
    for (Node& node : state->nodes) {
        node.stats.resize(h.row_group_count);
        for (uint32_t g = 0; g < h.row_group_count; ++g) {
            uint32_t declared = 0;
            if (!cursor.take(&declared, sizeof(declared)))
                return fail(Code::kTruncated,
                            "footer ends inside column '%s''s statistics",
                            node.name.c_str());
            if (declared == 0) continue;
            const uint8_t* blob = cursor.raw(declared);
            if (blob == nullptr)
                return fail(Code::kTruncated,
                            "column '%s' row group %u declares %u statistics bytes but "
                            "only %zu remain in the footer", node.name.c_str(), g,
                            declared, cursor.remaining());
            // Read prefix-first: a longer blob is a newer statistic this build
            // does not know, and is skipped.
            ColumnStatistics& s = node.stats[g].statistics;
            std::memcpy(&s, blob, std::min<size_t>(declared, sizeof(ColumnStatistics)));
            if (s.flags & kStatSketch)
                return fail(Code::kMalformed,
                            "column '%s' row group %u statistics set the v2 per-row-"
                            "group sketch flag; v3 sketches live in the column summary",
                            node.name.c_str(), g);
            node.stats[g].present = true;
        }
    }
    if (cursor.remaining() != 0)
        return fail(Code::kMalformed,
                    "footer has %zu bytes after its last record — it must end exactly "
                    "at the tail", cursor.remaining());

    // ── Metadata ──
    out->version            = 3u;
    out->row_count          = h.row_count;
    out->created_at_unix_us = h.created_at_unix_us;
    out->writer_tag.assign(reinterpret_cast<const char*>(tag), h.writer_tag_bytes);
    std::memcpy(out->file_uuid, h.file_uuid, sizeof(out->file_uuid));
    out->cluster_keys       = std::move(cluster_keys);
    out->block_row_groups   = h.block_row_groups;
    out->columns.resize(schema.size());
    for (size_t i = 0; i < schema.size(); ++i) footer::fill_schema(schema[i], &out->columns[i]);
    out->sketches.resize(state->nodes.size());
    for (size_t n = 0; n < state->nodes.size(); ++n) out->sketches[n] = state->nodes[n].sketch;
    out->row_groups.resize(h.row_group_count);
    for (uint32_t g = 0; g < h.row_group_count; ++g) {
        RowGroupSummary& summary = out->row_groups[g];
        summary.row_count = state->row_groups[g].row_count;
        summary.first_row = state->row_groups[g].first_row;
        summary.column_statistics.resize(state->nodes.size());
        for (size_t n = 0; n < state->nodes.size(); ++n)
            summary.column_statistics[n] = state->nodes[n].stats[g];
    }
    return Status::ok();
}

Status attach_directory(ReaderState* state, uint32_t n, const uint8_t* bytes, size_t size) {
    Node& node = state->nodes[n];
    const char* name = node.name.c_str();
    if (size != node.head.directory_bytes)
        return fail(Code::kMalformed,
                    "column '%s' directory block was handed %zu bytes but is %u",
                    name, size, node.head.directory_bytes);
    // Recorded in the footer, which is already trusted — so this checks the
    // block against something verified, not against a number beside it.
    const uint64_t actual = checksum_xxh3_64(bytes, size);
    if (actual != node.head.directory_checksum && checksum_must_match())
        return fail(Code::kChecksumMismatch,
                    "column '%s' directory block checksum mismatch: recorded %llu, "
                    "computed %llu — every offset in it is suspect", name,
                    static_cast<unsigned long long>(node.head.directory_checksum),
                    static_cast<unsigned long long>(actual));

    Cursor cursor(bytes, size);
    DirectoryBlockHeader header{};
    if (!cursor.take(&header, sizeof(header)))
        return fail(Code::kTruncated, "column '%s' directory block is too small for its "
                    "header", name);
    if (header.directory_magic != kDirectoryMagic)
        return fail(Code::kMalformed, "column '%s' directory magic is 0x%08X, not 0x%08X",
                    name, header.directory_magic, kDirectoryMagic);
    if (header.node_ordinal != n)
        return fail(Code::kMalformed,
                    "column '%s' directory block names node %u but its summary is "
                    "node %u", name, header.node_ordinal, n);
    const uint32_t R = state->header.row_group_count;
    if (header.chunk_count != R)
        return fail(Code::kMalformed,
                    "column '%s' directory block holds %u chunk records for %u row "
                    "groups", name, header.chunk_count, R);
    const uint64_t expect = sizeof(DirectoryBlockHeader)
                          + static_cast<uint64_t>(R) * sizeof(ChunkRecord)
                          + static_cast<uint64_t>(header.section_count) * sizeof(SectionEntry);
    if (expect != size)
        return fail(Code::kMalformed,
                    "column '%s' directory block is %zu bytes but %u chunks and %u "
                    "sections need %llu", name, size, R, header.section_count,
                    static_cast<unsigned long long>(expect));

    std::vector<ChunkRecord> chunks(R);
    std::vector<SectionEntry> sections(header.section_count);
    for (uint32_t g = 0; g < R; ++g) cursor.take(&chunks[g], sizeof(ChunkRecord));
    for (uint32_t s = 0; s < header.section_count; ++s)
        cursor.take(&sections[s], sizeof(SectionEntry));

    const bool top_level = std::find(state->top_level.begin(), state->top_level.end(), n)
                           != state->top_level.end();
    const uint64_t data_end  = node.head.data_offset + node.head.data_bytes;
    const uint64_t index_end = node.head.index_offset + node.head.index_bytes;
    const uint32_t G = state->header.block_row_groups;
    std::vector<BlockExtent> implied(node.blocks.size(), BlockExtent{0, 0});
    uint64_t previous_end = 0;
    for (uint32_t g = 0; g < R; ++g) {
        const ChunkRecord& c = chunks[g];
        if (c.reserved0 != 0 || c.reserved1 != 0)
            return fail(Code::kMalformed,
                        "column '%s' row group %u chunk record reserved bytes are not 0",
                        name, g);
        if (top_level && c.length != state->row_groups[g].row_count)
            return fail(Code::kMalformed,
                        "column '%s' row group %u declares %u rows but the row group "
                        "table says %llu", name, g, c.length,
                        static_cast<unsigned long long>(state->row_groups[g].row_count));
        if (static_cast<uint64_t>(c.section_index) + c.section_count > header.section_count
                || static_cast<uint64_t>(c.index_section_index) + c.index_section_count
                       > header.section_count)
            return fail(Code::kMalformed,
                        "column '%s' row group %u names sections past the %u in its "
                        "directory block", name, g, header.section_count);
        uint64_t begin = UINT64_MAX, end = 0;         // every section
        uint64_t held_begin = UINT64_MAX, held_end = 0;  // sections holding bytes
        for (uint32_t s = 0; s < c.section_count; ++s) {
            const SectionEntry& e = sections[c.section_index + s];
            if (e.stored_bytes > 0) {
                held_begin = std::min(held_begin, e.offset);
                held_end = std::max(held_end, e.offset + e.stored_bytes);
            }
            if (!inside(e.offset, e.stored_bytes, node.head.data_offset, data_end))
                return fail(Code::kMalformed,
                            "column '%s' row group %u: required section kind %u at "
                            "[%llu, +%llu) is outside the column's data extent",
                            name, g, e.kind, static_cast<unsigned long long>(e.offset),
                            static_cast<unsigned long long>(e.stored_bytes));
            begin = std::min(begin, e.offset);
            end = std::max(end, e.offset + e.stored_bytes);
        }
        for (uint32_t s = 0; s < c.index_section_count; ++s) {
            const SectionEntry& e = sections[c.index_section_index + s];
            if (!inside(e.offset, e.stored_bytes, node.head.index_offset, index_end))
                return fail(Code::kMalformed,
                            "column '%s' row group %u: optional section kind %u at "
                            "[%llu, +%llu) is outside the column's index extent",
                            name, g, e.kind, static_cast<unsigned long long>(e.offset),
                            static_cast<unsigned long long>(e.stored_bytes));
        }
        if (c.section_count > 0) {
            if (begin < previous_end)
                return fail(Code::kMalformed,
                            "column '%s' row group %u's sections begin at %llu, before "
                            "the previous row group's end at %llu", name, g,
                            static_cast<unsigned long long>(begin),
                            static_cast<unsigned long long>(previous_end));
            previous_end = end;
        }
        // A block extent covers the sections that hold bytes (FORMAT.md §5.6).
        if (held_begin != UINT64_MAX) {
            BlockExtent& block = implied[g / G];
            if (block.bytes == 0) block.offset = held_begin;
            block.bytes = held_end - block.offset;
        }
    }
    for (size_t b = 0; b < implied.size(); ++b)
        if (implied[b].offset != node.blocks[b].offset
                || implied[b].bytes != node.blocks[b].bytes)
            return fail(Code::kMalformed,
                        "column '%s' block %zu: the footer records [%llu, +%llu) but "
                        "the directory block's chunks span [%llu, +%llu)", name, b,
                        static_cast<unsigned long long>(node.blocks[b].offset),
                        static_cast<unsigned long long>(node.blocks[b].bytes),
                        static_cast<unsigned long long>(implied[b].offset),
                        static_cast<unsigned long long>(implied[b].bytes));

    node.chunks   = std::move(chunks);
    node.sections = std::move(sections);
    node.attached = true;
    return Status::ok();
}

Status resolve_columns(const ReaderState& state, const std::vector<std::string>& columns,
                       std::vector<uint32_t>* top, std::vector<uint32_t>* all) {
    top->clear();
    all->clear();
    if (columns.empty()) {
        *top = state.top_level;
    } else {
        for (const std::string& name : columns) {
            bool found = false;
            for (uint32_t n : state.top_level) {
                if (state.nodes[n].name == name) {
                    top->push_back(n);
                    found = true;
                    break;
                }
            }
            if (!found)
                return fail(Code::kMalformed, "requested column '%s' is not in this file",
                            name.c_str());
        }
    }
    for (uint32_t n : *top) collect_subtree(state, n, all);
    std::sort(all->begin(), all->end());
    all->erase(std::unique(all->begin(), all->end()), all->end());
    return Status::ok();
}

void plan_directories(const ReaderState& state, const std::vector<uint32_t>& nodes,
                      bool through_first_block, const FetchPolicy& policy,
                      std::vector<ByteRange>* out) {
    std::vector<ByteRange> ranges;
    for (uint32_t n : nodes) {
        const Node& node = state.nodes[n];
        if (node.attached) continue;
        const uint64_t begin = node.head.directory_offset;
        uint64_t end = begin + node.head.directory_bytes;
        // Block 0's chunks follow the directory in the node's run (validated at
        // footer parse: every block extent lies inside the node's data extent,
        // after its directory). An all-empty block 0 is {0, 0} and adds nothing.
        if (through_first_block && !node.blocks.empty() && node.blocks[0].bytes > 0)
            end = std::max(end, node.blocks[0].offset + node.blocks[0].bytes);
        ranges.push_back(ByteRange{begin, end - begin});
    }
    coalesce(std::move(ranges), policy, out);
}

Status plan_chunks(const ReaderState& state, const std::vector<uint32_t>& nodes,
                   const std::vector<uint32_t>& row_groups, const FetchPolicy& policy,
                   std::vector<ByteRange>* out) {
    const uint32_t R = state.header.row_group_count;
    for (size_t i = 0; i < row_groups.size(); ++i) {
        if (row_groups[i] >= R)
            return fail(Code::kMalformed, "row group %u was requested but this file has %u",
                        row_groups[i], R);
        if (i > 0 && row_groups[i] <= row_groups[i - 1])
            return fail(Code::kMalformed,
                        "plan_fetch: row groups must be ascending and distinct");
    }
    std::vector<ByteRange> ranges;
    for (uint32_t n : nodes) {
        SKENE_RETURN_IF_ERROR(require_attached(state, n));
        const Node& node = state.nodes[n];
        // One range per maximal run of CONSECUTIVE row groups: a node's chunks
        // are contiguous (alignment padding included), which is the point of
        // the layout.
        size_t i = 0;
        while (i < row_groups.size()) {
            size_t j = i;
            while (j + 1 < row_groups.size() && row_groups[j + 1] == row_groups[j] + 1) ++j;
            uint64_t begin = UINT64_MAX, end = 0;
            for (size_t k = i; k <= j; ++k) {
                uint64_t b = 0, e = 0;
                if (!chunk_extent(node, row_groups[k], &b, &e)) continue;
                begin = std::min(begin, b);
                end = std::max(end, e);
            }
            if (begin != UINT64_MAX) ranges.push_back(ByteRange{begin, end - begin});
            i = j + 1;
        }
    }
    coalesce(std::move(ranges), policy, out);
    return Status::ok();
}

Status read_row_group(const ReaderState& state, const chunk::ByteSource& bytes,
                      uint32_t row_group, const ReadOptions& options, CxxMorsel* out) {
    if (row_group >= state.row_groups.size())
        return fail(Code::kMalformed, "row group %u was requested but this file has %zu",
                    row_group, state.row_groups.size());
    std::vector<uint32_t> top, all;
    SKENE_RETURN_IF_ERROR(resolve_columns(state, options.columns, &top, &all));
    for (uint32_t n : top) SKENE_RETURN_IF_ERROR(require_attached(state, n));

    std::vector<std::unique_ptr<chunk::SectionResolver>> resolvers;
    std::vector<chunk::ChunkNode> nodes(top.size());
    for (size_t i = 0; i < top.size(); ++i)
        build_chunk_node(state, top[i], row_group, bytes, &resolvers, &nodes[i]);
    return chunk::build_row_group(nodes, state.row_groups[row_group].row_count, options, out);
}

Status read_row_group_metadata(const ReaderState& state, const chunk::ByteSource& bytes,
                               uint32_t row_group, RowGroupMetadata* out) {
    if (row_group >= state.row_groups.size())
        return fail(Code::kMalformed, "row group %u was requested but this file has %zu",
                    row_group, state.row_groups.size());
    std::vector<std::unique_ptr<chunk::SectionResolver>> resolvers;
    out->row_count = state.row_groups[row_group].row_count;
    out->columns.resize(state.top_level.size());
    for (size_t i = 0; i < state.top_level.size(); ++i) {
        SKENE_RETURN_IF_ERROR(require_attached(state, state.top_level[i]));
        chunk::ChunkNode node;
        build_chunk_node(state, state.top_level[i], row_group, bytes, &resolvers, &node);
        SKENE_RETURN_IF_ERROR(chunk::fill_metadata(node, &out->columns[i]));
    }
    return Status::ok();
}

}  // namespace v3
}  // namespace skene
