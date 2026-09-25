// skene/src/patch.cpp — see include/skene/patch.h.
//
// v3 (2026-09-24). The layout is column-major (FORMAT.md §3), which is what
// makes this a copy of whole byte ranges rather than a walk of every section:
//
//   HEAD          copied verbatim
//   DATA region   per surviving column node: its directory block REBUILT (its
//                 section offsets moved), then its chunk run copied as ONE
//                 range; per added column: a directory block and one constant
//                 chunk per row group
//   INDEX region  per surviving column node: its index extent copied as ONE range
//   FOOTER        rebuilt — schema, cluster spec, summaries, statistics
//   TAIL          rebuilt
//
// A copied range keeps its offset MODULO kSectionAlign, so every section in it
// keeps the alignment it was written with (§3 rule 4) — a patched file is
// indistinguishable from a written one. The input is parsed and validated by
// the v3 reader's own footer and directory checks; nothing here re-states them.
//
// Every structure is re-emitted in the field order the writer uses
// (src/writer.cpp). Where this file and the writer disagree, the writer is
// right and this is a bug.

#include "skene/patch.h"

#include <cstring>
#include <functional>

#include "core/buffers.h"  // DRAKEN_SEL_* / DRAKEN_DICT_* layout-hint flags
#include "footer_common.h"
#include "reader_v3.h"
#include "skene/checksum.h"
#include "skene/format.h"

namespace skene {
namespace {

Status fail(Code code, std::string message) { return Status(code, std::move(message)); }

// ─── Reading: framing, then the v3 reader's own validation ──────────────────

struct Parsed {
    const uint8_t*   bytes = nullptr;
    size_t           size = 0;
    FileTail         tail{};
    FileMetadata     metadata;
    v3::ReaderState  state;
    std::vector<footer::ParsedSchema> schema;   // top-level entries, re-read for re-emission
};

Status parse_file(const uint8_t* src, size_t size, const char* what, Parsed* out) {
    if (size < kMinFileBytes)
        return fail(Code::kTruncated, std::string("patch_columns: ") + what +
                    " is too small to be a skene file");
    FileHead head{};
    std::memcpy(&head, src, sizeof(head));
    FileTail tail{};
    std::memcpy(&tail, src + size - kFileTailBytes, sizeof(tail));
    if (head.magic != kMagic || tail.magic != kMagic)
        return fail(Code::kNotSkene, std::string("patch_columns: ") + what +
                    " is not a skene file");
    if (head.version != kVersion || tail.version != kVersion)
        return fail(Code::kUnsupportedVersion,
                    std::string("patch_columns: ") + what + " is v" +
                    std::to_string(tail.version) + "; this build patches only v" +
                    std::to_string(kVersion) +
                    " — migrate the file forward first (skene::migrate_file)");
    const size_t footer_end = size - kFileTailBytes;
    if (tail.footer_bytes > footer_end - kFileHeadBytes)
        return fail(Code::kTruncated, std::string("patch_columns: ") + what +
                    " footer runs past its head");
    const size_t footer_start = footer_end - tail.footer_bytes;
    if (checksum_xxh3_64(src + footer_start, tail.footer_bytes) != tail.footer_checksum
            && checksum_must_match())
        return fail(Code::kChecksumMismatch, std::string("patch_columns: ") + what +
                    " footer checksum mismatch");

    SKENE_RETURN_IF_ERROR(v3::parse_footer(src + footer_start, tail.footer_bytes,
                                           footer_start, &out->metadata, &out->state));
    for (uint32_t n = 0; n < out->state.nodes.size(); ++n) {
        const ColumnSummaryHead& h = out->state.nodes[n].head;
        SKENE_RETURN_IF_ERROR(v3::attach_directory(&out->state, n, src + h.directory_offset,
                                                   h.directory_bytes));
    }

    // The schema entries themselves, for re-emission: the v3 reader keeps their
    // decoded form, and the entry bytes are rebuilt from exactly those fields.
    footer::Cursor cursor(src + footer_start, tail.footer_bytes);
    FileFooterHeader fh{};
    cursor.take(&fh, sizeof(fh));
    cursor.raw(fh.writer_tag_bytes);
    cursor.raw(static_cast<size_t>(fh.row_group_count) * sizeof(RowGroupEntry));
    out->schema.resize(fh.column_count);
    for (footer::ParsedSchema& node : out->schema)
        SKENE_RETURN_IF_ERROR(footer::parse_schema(cursor, &node, 0));

    out->bytes = src;
    out->size  = size;
    out->tail  = tail;
    return Status::ok();
}

// ─── Writing ────────────────────────────────────────────────────────────────

class Sink {
  public:
    explicit Sink(std::vector<uint8_t>* out) : out_(out) {}
    uint64_t position() const { return out_->size(); }
    void bytes(const void* src, size_t n) {
        const uint8_t* p = static_cast<const uint8_t*>(src);
        out_->insert(out_->end(), p, p + n);
    }
    void zeros(size_t n) { out_->insert(out_->end(), n, uint8_t{0}); }
    template <typename T>
    void pod(const T& value) { bytes(&value, sizeof(T)); }
    void u32(uint32_t value) { pod(value); }
    void u64(uint64_t value) { pod(value); }

  private:
    std::vector<uint8_t>* out_;
};

// The smallest position >= `at` that is congruent to `like` modulo
// kSectionAlign: a range moved there keeps every section's alignment.
uint64_t congruent_at_or_after(uint64_t at, uint64_t like) {
    const uint64_t want = like % kSectionAlign;
    const uint64_t have = at % kSectionAlign;
    return at + ((want + kSectionAlign - have) % kSectionAlign);
}

uint64_t align_up(uint64_t at) { return congruent_at_or_after(at, 0); }

void write_schema_entry(Sink& sink, const footer::ParsedSchema& node) {
    sink.pod(node.head);
    sink.bytes(node.name.data(), node.name.size());
    if (node.head.logical_present) sink.pod(node.logical);
    for (const footer::ParsedSchema& child : node.children) write_schema_entry(sink, child);
}

// ─── Added columns ──────────────────────────────────────────────────────────

// A donor, parsed: everything needed to emit the same column at any length.
struct Donor {
    footer::ParsedSchema  schema;
    ChunkRecord           chunk{};
    // The donor's required sections, bytes lifted out. kSelection is never among
    // them (a one-row column is CONSTANT or IDENTITY, and both store none) and
    // kValidity is dropped here because its size depends on the row count — it
    // is synthesised per row group instead.
    std::vector<SectionEntry>         sections;
    std::vector<std::vector<uint8_t>> section_bytes;
    bool null_fill = false;
};

// Whether the donor's single row is NULL, read from its own validity bit.
bool donor_row_is_null(const SectionEntry& validity, const uint8_t* src) {
    if (validity.stored_bytes == 0) return false;
    if (validity.encoding != static_cast<uint8_t>(Encoding::kPlain)) return false;
    if (validity.codec != static_cast<uint8_t>(SectionCodec::kNone)) return false;
    return (src[validity.offset] & 1u) == 0u;
}

Status parse_donor(const DonorFile& bytes, Donor* donor) {
    Parsed parsed;
    SKENE_RETURN_IF_ERROR(parse_file(bytes.data(), bytes.size(), "donor", &parsed));
    const v3::ReaderState& state = parsed.state;
    if (state.top_level.size() != 1 || state.row_groups.size() != 1
            || state.header.row_count != 1)
        return fail(Code::kMalformed,
                    "patch_columns: a donor must hold exactly one column of one row");
    const v3::Node& node = state.nodes[state.top_level[0]];
    if (!node.children.empty())
        return fail(Code::kMalformed,
                    "patch_columns: donor column '" + node.name +
                    "' is nested; adding an ARRAY column is not supported");

    donor->schema = parsed.schema[0];
    donor->chunk  = node.chunks[0];
    for (uint32_t s = 0; s < donor->chunk.section_count; ++s) {
        const SectionEntry& entry = node.sections[donor->chunk.section_index + s];
        if (entry.kind == static_cast<uint16_t>(SectionKind::kSelection))
            return fail(Code::kMalformed,
                        "patch_columns: donor column carries a stored selection");
        if (entry.kind == static_cast<uint16_t>(SectionKind::kValidity)) {
            donor->null_fill = donor_row_is_null(entry, bytes.data());
            continue;
        }
        donor->sections.push_back(entry);
        donor->section_bytes.emplace_back(bytes.data() + entry.offset,
                                          bytes.data() + entry.offset + entry.stored_bytes);
    }
    return Status::ok();
}

// One output column node, laid out before anything is written: directory
// blocks precede the chunks they describe, so every offset must be known first.
struct OutNode {
    int32_t                   source = -1;      // v3 node index in the input; -1 == added
    int32_t                   donor = -1;       // index into donors when added
    std::string               name;             // for messages
    uint32_t                  child_count = 0;

    std::vector<ChunkRecord>  chunks;
    std::vector<SectionEntry> sections;         // final offsets
    std::vector<BlockExtent>  blocks;
    ColumnSummaryHead         head{};
    ColumnSketch              sketch;
    std::vector<RowGroupColumnStatistics> stats;

    // Where the node's copied ranges move (surviving nodes).
    uint64_t data_shift_to = 0;
    uint64_t index_shift_to = 0;
    // Added nodes: per row group, the donor bytes each section entry points at.
    std::vector<const std::vector<uint8_t>*> added_bytes;
    std::vector<std::vector<uint8_t>>        validity_bytes;
};

}  // namespace

Status patch_columns(const void* file, size_t file_bytes,
                     const std::vector<std::string>& drop,
                     const std::vector<std::pair<std::string, std::string>>& rename,
                     const std::vector<DonorFile>& add,
                     std::vector<uint8_t>* out) {
    if (file == nullptr || out == nullptr)
        return fail(Code::kMalformed, "patch_columns: null file or output");
    if (drop.empty() && rename.empty() && add.empty())
        return fail(Code::kMalformed, "patch_columns: no changes to make");

    std::vector<Donor> donors(add.size());
    for (size_t i = 0; i < add.size(); ++i)
        SKENE_RETURN_IF_ERROR(parse_donor(add[i], &donors[i]));

    Parsed in;
    SKENE_RETURN_IF_ERROR(
        parse_file(static_cast<const uint8_t*>(file), file_bytes, "file", &in));
    const uint8_t* src = in.bytes;
    const v3::ReaderState& state = in.state;
    const uint32_t R = state.header.row_group_count;
    const uint32_t G = state.header.block_row_groups;
    const uint32_t block_count = (R + G - 1u) / G;
    std::vector<footer::ParsedSchema>& schema = in.schema;

    // ── resolve the requested changes against the schema ──
    std::vector<bool> keep(schema.size(), true);
    std::vector<std::string> new_names(schema.size());
    for (size_t i = 0; i < schema.size(); ++i) new_names[i] = schema[i].name;
    for (const std::string& name : drop) {
        bool found = false;
        for (size_t i = 0; i < schema.size(); ++i)
            if (schema[i].name == name) { keep[i] = false; found = true; }
        if (!found)
            return fail(Code::kMalformed, "patch_columns: no column named '" + name + "' to drop");
    }
    for (const auto& pair : rename) {
        bool found = false;
        for (size_t i = 0; i < schema.size(); ++i)
            if (schema[i].name == pair.first) { new_names[i] = pair.second; found = true; }
        if (!found)
            return fail(Code::kMalformed,
                        "patch_columns: no column named '" + pair.first + "' to rename");
    }
    std::vector<std::string> surviving_names;
    for (size_t i = 0; i < schema.size(); ++i)
        if (keep[i]) surviving_names.push_back(new_names[i]);
    for (const Donor& donor : donors) surviving_names.push_back(donor.schema.name);
    if (surviving_names.empty())
        return fail(Code::kMalformed,
                    "patch_columns: dropping every column would leave no relation");
    for (size_t i = 0; i < surviving_names.size(); ++i)
        for (size_t j = i + 1; j < surviving_names.size(); ++j)
            if (surviving_names[i] == surviving_names[j])
                return fail(Code::kMalformed,
                            "patch_columns: the result would have two columns named '" +
                            surviving_names[i] + "'");

    // ── output nodes, depth first: surviving subtrees, then added columns ──
    std::vector<OutNode> nodes;
    std::function<void(uint32_t)> add_subtree = [&](uint32_t n) {
        const v3::Node& source = state.nodes[n];
        OutNode node;
        node.source      = static_cast<int32_t>(n);
        node.name        = source.name;
        node.child_count = static_cast<uint32_t>(source.children.size());
        node.chunks      = source.chunks;
        node.sections    = source.sections;
        node.sketch      = source.sketch;
        node.stats       = source.stats;
        nodes.push_back(std::move(node));
        for (uint32_t child : source.children) add_subtree(child);
    };
    for (size_t i = 0; i < schema.size(); ++i)
        if (keep[i]) add_subtree(state.top_level[i]);
    for (size_t d = 0; d < donors.size(); ++d) {
        OutNode node;
        node.donor = static_cast<int32_t>(d);
        node.name  = donors[d].schema.name;
        node.stats.assign(R, RowGroupColumnStatistics{});   // NOT TRACKED: the donor
        // describes one row, and scaling its counts to N would fabricate a fact.
        nodes.push_back(std::move(node));
    }

    // ── layout ──
    uint64_t at = kFileHeadBytes;
    for (OutNode& node : nodes) {
        if (node.donor >= 0) {
            // One constant chunk per row group. CONSTANT is the whole trick:
            // one value, no selection section, the reader hands every row
            // data[0] — so the donor's data section is the right one for any
            // row count. Only a NULL fill's validity scales with N.
            const Donor& donor = donors[static_cast<size_t>(node.donor)];
            node.chunks.assign(R, donor.chunk);
            // added_bytes points INTO validity_bytes, so it must never reallocate.
            node.validity_bytes.reserve(R);
            for (uint32_t g = 0; g < R; ++g) {
                ChunkRecord& c = node.chunks[g];
                c.length         = static_cast<uint32_t>(state.row_groups[g].row_count);
                c.data_length    = 1;
                c.selection_kind = static_cast<uint8_t>(SelectionKind::kConstant);
                c.value_order    = static_cast<uint8_t>(ValueOrder::kAsWritten);
                // The donor's flags describe a ONE-ROW column. SEL_IDENTITY and
                // SEL_PERMUTATION imply data_length == length, false at N rows;
                // DICT_CODES_DENSE asserts every code has a VALID row, false for
                // a NULL fill. The reader rejects a hint that contradicts the
                // stored layout, so they are cleared, never copied.
                c.vector_flags &= static_cast<uint8_t>(
                    ~(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION | DRAKEN_DICT_CODES_DENSE));
                c.section_index       = static_cast<uint32_t>(node.sections.size());
                c.index_section_index = 0;
                c.index_section_count = 0;
                for (size_t s = 0; s < donor.sections.size(); ++s) {
                    node.sections.push_back(donor.sections[s]);
                    node.added_bytes.push_back(&donor.section_bytes[s]);
                }
                if (donor.null_fill) {
                    const size_t bitmap_bytes = static_cast<size_t>((c.length + 7u) / 8u);
                    node.validity_bytes.emplace_back(bitmap_bytes, uint8_t{0});
                    SectionEntry entry{};
                    entry.kind          = static_cast<uint16_t>(SectionKind::kValidity);
                    entry.encoding      = static_cast<uint8_t>(Encoding::kPlain);
                    entry.codec         = static_cast<uint8_t>(SectionCodec::kNone);
                    entry.stored_bytes  = bitmap_bytes;
                    entry.encoded_bytes = bitmap_bytes;
                    entry.plain_bytes   = bitmap_bytes;
                    entry.checksum      = checksum_xxh3_64(node.validity_bytes.back().data(),
                                                           bitmap_bytes);
                    node.sections.push_back(entry);
                    node.added_bytes.push_back(&node.validity_bytes.back());
                }
                c.section_count =
                    static_cast<uint32_t>(node.sections.size()) - c.section_index;
            }
        }

        const uint64_t directory_bytes = sizeof(DirectoryBlockHeader)
            + static_cast<uint64_t>(R) * sizeof(ChunkRecord)
            + static_cast<uint64_t>(node.sections.size()) * sizeof(SectionEntry);
        if (directory_bytes > UINT32_MAX)
            return fail(Code::kMalformed, "patch_columns: column '" + node.name +
                        "' directory block exceeds the 32-bit length");
        node.head.directory_offset = at;
        node.head.directory_bytes  = static_cast<uint32_t>(directory_bytes);
        at += directory_bytes;
        node.blocks.assign(block_count, BlockExtent{0, 0});

        if (node.source >= 0) {
            const v3::Node& source = state.nodes[static_cast<size_t>(node.source)];
            {
                // Moved even when it holds no bytes: a column of zero-row chunks
                // still has (empty) sections, and they sit at data_offset.
                node.head.data_offset = congruent_at_or_after(at, source.head.data_offset);
                node.head.data_bytes  = source.head.data_bytes;
                node.data_shift_to    = node.head.data_offset;
                const uint64_t delta  = node.head.data_offset - source.head.data_offset;
                for (const ChunkRecord& c : node.chunks)
                    for (uint32_t s = 0; s < c.section_count; ++s)
                        node.sections[c.section_index + s].offset += delta;
                for (uint32_t b = 0; b < block_count; ++b)
                    if (source.blocks[b].bytes > 0)
                        node.blocks[b] = BlockExtent{source.blocks[b].offset + delta,
                                                     source.blocks[b].bytes};
                at = node.head.data_offset + node.head.data_bytes;
            }
        } else {
            uint64_t begin = UINT64_MAX;
            for (uint32_t g = 0; g < R; ++g) {
                const ChunkRecord& c = node.chunks[g];
                BlockExtent& block = node.blocks[g / G];
                for (uint32_t s = 0; s < c.section_count; ++s) {
                    SectionEntry& entry = node.sections[c.section_index + s];
                    at = align_up(at);
                    entry.offset = at;
                    if (block.bytes == 0) block.offset = at;
                    at += entry.stored_bytes;
                    block.bytes = at - block.offset;
                    if (begin == UINT64_MAX) begin = entry.offset;
                }
            }
            node.head.data_offset = begin == UINT64_MAX ? node.head.directory_offset
                                                          + node.head.directory_bytes
                                                        : begin;
            node.head.data_bytes  = begin == UINT64_MAX ? 0 : at - begin;
        }
    }
    for (OutNode& node : nodes) {
        if (node.source < 0) continue;
        const v3::Node& source = state.nodes[static_cast<size_t>(node.source)];
        if (source.head.index_bytes == 0) continue;
        node.head.index_offset = congruent_at_or_after(at, source.head.index_offset);
        node.head.index_bytes  = source.head.index_bytes;
        node.index_shift_to    = node.head.index_offset;
        const uint64_t delta   = node.head.index_offset - source.head.index_offset;
        for (const ChunkRecord& c : node.chunks)
            for (uint32_t s = 0; s < c.index_section_count; ++s)
                node.sections[c.index_section_index + s].offset += delta;
        at = node.head.index_offset + node.head.index_bytes;
    }
    const uint64_t footer_offset = at;

    // ── emit: head, DATA, INDEX ──
    out->clear();
    out->reserve(file_bytes);
    Sink sink(out);
    sink.bytes(src, kFileHeadBytes);

    for (size_t k = 0; k < nodes.size(); ++k) {
        OutNode& node = nodes[k];
        std::vector<uint8_t> block;
        Sink bw(&block);
        DirectoryBlockHeader dh{};
        dh.directory_magic = kDirectoryMagic;
        dh.node_ordinal    = static_cast<uint32_t>(k);
        dh.chunk_count     = R;
        dh.section_count   = static_cast<uint32_t>(node.sections.size());
        bw.pod(dh);
        for (const ChunkRecord& c : node.chunks) bw.pod(c);
        for (const SectionEntry& e : node.sections) bw.pod(e);
        node.head.directory_checksum = checksum_xxh3_64(block.data(), block.size());
        sink.bytes(block.data(), block.size());

        if (node.source >= 0) {
            const v3::Node& source = state.nodes[static_cast<size_t>(node.source)];
            if (source.head.data_bytes > 0) {
                sink.zeros(static_cast<size_t>(node.data_shift_to - sink.position()));
                sink.bytes(src + source.head.data_offset,
                           static_cast<size_t>(source.head.data_bytes));
            }
        } else {
            for (size_t s = 0; s < node.sections.size(); ++s) {
                sink.zeros(static_cast<size_t>(node.sections[s].offset - sink.position()));
                sink.bytes(node.added_bytes[s]->data(), node.added_bytes[s]->size());
            }
        }
    }
    for (const OutNode& node : nodes) {
        if (node.source < 0) continue;
        const v3::Node& source = state.nodes[static_cast<size_t>(node.source)];
        if (source.head.index_bytes == 0) continue;
        sink.zeros(static_cast<size_t>(node.index_shift_to - sink.position()));
        sink.bytes(src + source.head.index_offset,
                   static_cast<size_t>(source.head.index_bytes));
    }
    if (sink.position() != footer_offset)
        return fail(Code::kMalformed, "patch_columns: internal — the regions do not end "
                    "where the layout put the footer");

    // ── FOOTER ──
    std::vector<uint8_t> footer;
    Sink fw(&footer);
    FileFooterHeader fh = state.header;
    fh.column_count      = static_cast<uint32_t>(surviving_names.size());
    fh.data_region_bytes = footer_offset - kFileHeadBytes;
    fw.pod(fh);
    fw.bytes(in.metadata.writer_tag.data(), in.metadata.writer_tag.size());
    for (const RowGroupEntry& entry : state.row_groups) fw.pod(entry);
    for (size_t i = 0; i < schema.size(); ++i) {
        if (!keep[i]) continue;
        schema[i].name = new_names[i];
        schema[i].head.name_bytes = static_cast<uint32_t>(new_names[i].size());
        write_schema_entry(fw, schema[i]);
    }
    for (const Donor& donor : donors) write_schema_entry(fw, donor.schema);

    // Cluster spec: renames leave it untouched (ordinals name positions, not
    // names); a drop keeps the longest PREFIX of keys whose columns all
    // survive, remapped to the surviving positions. Rows ordered by (a, b) are
    // still ordered by (a) when b goes, but NOT generally by (b) when a goes —
    // a promise shrinks to what remains provably true, never stretches.
    {
        std::vector<uint32_t> new_ordinal(schema.size(), UINT32_MAX);
        uint32_t position = 0;
        for (size_t i = 0; i < schema.size(); ++i)
            if (keep[i]) new_ordinal[i] = position++;
        std::vector<SortKey> kept_keys;
        for (const SortKey& key : in.metadata.cluster_keys) {
            if (key.column_ordinal >= schema.size()
                    || new_ordinal[key.column_ordinal] == UINT32_MAX)
                break;
            SortKey remapped = key;
            remapped.column_ordinal = new_ordinal[key.column_ordinal];
            kept_keys.push_back(remapped);
        }
        ClusterSpecHeader spec{};
        spec.key_count = static_cast<uint16_t>(kept_keys.size());
        fw.pod(spec);
        for (const SortKey& key : kept_keys) fw.pod(key);
    }

    for (OutNode& node : nodes) {
        node.head.block_count = block_count;
        node.head.child_count = node.child_count;
        fw.pod(node.head);
        for (const BlockExtent& b : node.blocks) fw.pod(b);
        SketchRecordHeader sketch{};
        if (node.sketch.present()) {
            sketch.hash_family = node.sketch.hash_family;
            sketch.k           = static_cast<uint16_t>(node.sketch.k);
            sketch.count       = static_cast<uint32_t>(node.sketch.hashes.size());
        }
        fw.pod(sketch);
        for (uint64_t hash : node.sketch.hashes) fw.u64(hash);
    }

    // Statistics are re-emitted from the reader's parse: the fields this build
    // knows. A longer blob's unknown tail is dropped — statistics are optional
    // and reconstructible (FORMAT.md §7.1), so dropping one costs a pruning
    // opportunity, never correctness.
    for (const OutNode& node : nodes)
        for (const RowGroupColumnStatistics& s : node.stats) {
            if (!s.present) { fw.u32(0); continue; }
            fw.u32(static_cast<uint32_t>(sizeof(ColumnStatistics)));
            fw.pod(s.statistics);
        }

    if (footer.size() > UINT32_MAX)
        return fail(Code::kMalformed, "patch_columns: footer exceeds 32-bit length");
    sink.bytes(footer.data(), footer.size());

    FileTail tail = in.tail;
    tail.footer_bytes    = static_cast<uint32_t>(footer.size());
    tail.footer_checksum = checksum_xxh3_64(footer.data(), footer.size());
    sink.pod(tail);
    return Status::ok();
}

}  // namespace skene
