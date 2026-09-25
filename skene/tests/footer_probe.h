#pragma once
// Independent navigation of a v3 .skene file, for tests.
//
// Deliberately NOT built on skene::read_metadata: several suites exist to check
// that the bytes are laid out as FORMAT.md says, and a probe that went through
// the reader would only be checking the reader against itself. This walks the
// tail, the footer, the column summaries and the directory blocks by hand,
// exactly as an independent implementation written from the specification
// would (FORMAT.md §4, §5).

#include <cstdint>
#include <cstring>
#include <vector>

#include "skene/format.h"

namespace skene_test {

// The FOOTER: the region the tail points at.
inline bool file_footer_extent(const std::vector<uint8_t>& bytes,
                               size_t* out_offset, size_t* out_bytes) {
    if (bytes.size() < skene::kMinFileBytes) return false;
    const size_t tail_at = bytes.size() - skene::kFileTailBytes;
    skene::FileTail tail;
    std::memcpy(&tail, bytes.data() + tail_at, sizeof(tail));
    if (tail.footer_bytes > tail_at) return false;
    *out_offset = tail_at - tail.footer_bytes;
    *out_bytes  = tail.footer_bytes;
    return true;
}

inline bool file_footer_header(const std::vector<uint8_t>& bytes,
                               skene::FileFooterHeader* out) {
    size_t offset = 0, length = 0;
    if (!file_footer_extent(bytes, &offset, &length)) return false;
    if (length < sizeof(skene::FileFooterHeader)) return false;
    std::memcpy(out, bytes.data() + offset, sizeof(*out));
    return out->footer_magic == skene::kFileFooterMagic;
}

// Where the row group table starts (FORMAT.md §5.3).
inline bool row_group_table_at(const std::vector<uint8_t>& bytes, size_t* out) {
    size_t offset = 0, length = 0;
    skene::FileFooterHeader header;
    if (!file_footer_extent(bytes, &offset, &length)) return false;
    if (!file_footer_header(bytes, &header)) return false;
    *out = offset + sizeof(skene::FileFooterHeader) + header.writer_tag_bytes;
    return true;
}

inline bool row_group_entry(const std::vector<uint8_t>& bytes, uint32_t index,
                            skene::RowGroupEntry* out, size_t* out_at = nullptr) {
    skene::FileFooterHeader header;
    size_t table = 0;
    if (!file_footer_header(bytes, &header) || !row_group_table_at(bytes, &table))
        return false;
    if (index >= header.row_group_count) return false;
    const size_t at = table + index * sizeof(skene::RowGroupEntry);
    if (at + sizeof(skene::RowGroupEntry) > bytes.size()) return false;
    std::memcpy(out, bytes.data() + at, sizeof(*out));
    if (out_at != nullptr) *out_at = at;
    return true;
}

// Skips one schema entry and its children; counts the nodes it held.
inline bool skip_schema_entry(const std::vector<uint8_t>& bytes, size_t* at,
                              uint32_t* nodes) {
    skene::SchemaEntryHead head;
    if (*at + sizeof(head) > bytes.size()) return false;
    std::memcpy(&head, bytes.data() + *at, sizeof(head));
    *at += sizeof(head) + head.name_bytes
         + (head.logical_present ? sizeof(skene::LogicalTypeDescriptor) : 0);
    *nodes += 1;
    for (uint32_t c = 0; c < head.child_count; ++c)
        if (!skip_schema_entry(bytes, at, nodes)) return false;
    return true;
}

// Where the column summaries start, and how many column nodes there are.
inline bool summaries_at(const std::vector<uint8_t>& bytes, size_t* out,
                         uint32_t* out_nodes) {
    skene::FileFooterHeader header;
    size_t at = 0;
    if (!file_footer_header(bytes, &header) || !row_group_table_at(bytes, &at))
        return false;
    at += static_cast<size_t>(header.row_group_count) * sizeof(skene::RowGroupEntry);
    uint32_t nodes = 0;
    for (uint32_t c = 0; c < header.column_count; ++c)
        if (!skip_schema_entry(bytes, &at, &nodes)) return false;
    skene::ClusterSpecHeader spec;
    if (at + sizeof(spec) > bytes.size()) return false;
    std::memcpy(&spec, bytes.data() + at, sizeof(spec));
    at += sizeof(spec) + spec.key_count * sizeof(skene::SortKey);
    *out = at;
    *out_nodes = nodes;
    return true;
}

// Column node `node`'s summary head, and where it sits in the file. Summaries
// are sequential in depth-first node order (§5.6): head, block extents, sketch.
inline bool column_summary(const std::vector<uint8_t>& bytes, uint32_t node,
                           skene::ColumnSummaryHead* out, size_t* out_at = nullptr) {
    size_t at = 0;
    uint32_t nodes = 0;
    if (!summaries_at(bytes, &at, &nodes) || node >= nodes) return false;
    for (uint32_t n = 0; n <= node; ++n) {
        skene::ColumnSummaryHead head;
        if (at + sizeof(head) > bytes.size()) return false;
        std::memcpy(&head, bytes.data() + at, sizeof(head));
        if (n == node) {
            *out = head;
            if (out_at != nullptr) *out_at = at;
            return true;
        }
        at += sizeof(head) + head.block_count * sizeof(skene::BlockExtent);
        skene::SketchRecordHeader sketch;
        if (at + sizeof(sketch) > bytes.size()) return false;
        std::memcpy(&sketch, bytes.data() + at, sizeof(sketch));
        at += sizeof(sketch) + sketch.count * sizeof(uint64_t);
    }
    return false;
}

// Where the per-row-group statistics start (§5.8): after every summary.
inline bool statistics_at(const std::vector<uint8_t>& bytes, size_t* out) {
    size_t at = 0;
    uint32_t nodes = 0;
    if (!summaries_at(bytes, &at, &nodes)) return false;
    for (uint32_t n = 0; n < nodes; ++n) {
        skene::ColumnSummaryHead head;
        if (at + sizeof(head) > bytes.size()) return false;
        std::memcpy(&head, bytes.data() + at, sizeof(head));
        at += sizeof(head) + head.block_count * sizeof(skene::BlockExtent);
        skene::SketchRecordHeader sketch;
        if (at + sizeof(sketch) > bytes.size()) return false;
        std::memcpy(&sketch, bytes.data() + at, sizeof(sketch));
        at += sizeof(sketch) + sketch.count * sizeof(uint64_t);
    }
    *out = at;
    return true;
}

// Node `node`'s directory block: its header, chunk records and section entries.
struct DirectoryBlock {
    size_t                            offset = 0;   // in the file
    skene::DirectoryBlockHeader       header{};
    std::vector<skene::ChunkRecord>   chunks;
    std::vector<skene::SectionEntry>  sections;

    // File offset of chunk record `g` / section entry `s`.
    size_t chunk_at(uint32_t g) const {
        return offset + sizeof(skene::DirectoryBlockHeader) + g * sizeof(skene::ChunkRecord);
    }
    size_t section_at(uint32_t s) const {
        return offset + sizeof(skene::DirectoryBlockHeader)
             + chunks.size() * sizeof(skene::ChunkRecord) + s * sizeof(skene::SectionEntry);
    }
};

inline bool directory_block(const std::vector<uint8_t>& bytes, uint32_t node,
                            DirectoryBlock* out) {
    skene::ColumnSummaryHead head;
    if (!column_summary(bytes, node, &head)) return false;
    if (head.directory_offset + head.directory_bytes > bytes.size()) return false;
    out->offset = static_cast<size_t>(head.directory_offset);
    const uint8_t* p = bytes.data() + out->offset;
    std::memcpy(&out->header, p, sizeof(out->header));
    p += sizeof(out->header);
    out->chunks.resize(out->header.chunk_count);
    for (auto& c : out->chunks) { std::memcpy(&c, p, sizeof(c)); p += sizeof(c); }
    out->sections.resize(out->header.section_count);
    for (auto& s : out->sections) { std::memcpy(&s, p, sizeof(s)); p += sizeof(s); }
    return true;
}

// Rewrites node `node`'s directory checksum in its summary so it matches the
// directory block's current bytes, then re-seals the footer — for tests that
// corrupt a directory field and want only the STRUCTURAL check to catch it.
inline void reseal_directory(std::vector<uint8_t>* bytes, uint32_t node);
inline void reseal_footer(std::vector<uint8_t>* bytes);

}  // namespace skene_test

#include "skene/checksum.h"

namespace skene_test {

inline void reseal_footer(std::vector<uint8_t>* bytes) {
    size_t offset = 0, length = 0;
    if (!file_footer_extent(*bytes, &offset, &length)) return;
    skene::FileTail tail;
    const size_t tail_at = bytes->size() - skene::kFileTailBytes;
    std::memcpy(&tail, bytes->data() + tail_at, sizeof(tail));
    tail.footer_checksum = skene::checksum_xxh3_64(bytes->data() + offset, length);
    std::memcpy(bytes->data() + tail_at, &tail, sizeof(tail));
}

inline void reseal_directory(std::vector<uint8_t>* bytes, uint32_t node) {
    skene::ColumnSummaryHead head;
    size_t at = 0;
    if (!column_summary(*bytes, node, &head, &at)) return;
    head.directory_checksum = skene::checksum_xxh3_64(
        bytes->data() + head.directory_offset, head.directory_bytes);
    std::memcpy(bytes->data() + at, &head, sizeof(head));
    reseal_footer(bytes);
}

}  // namespace skene_test
