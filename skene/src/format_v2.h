#pragma once
// Internal: the v2 on-disk structs, FROZEN at the layout v2 files were written
// with (skene/FORMAT_v2.md). format.h moved to v3 on 2026-09-24; these copies
// exist so the retained v2 reader keeps parsing the bytes v2 actually wrote.
// Inside namespace v2 they shadow the v3 names of the same spelling, which is
// exactly the intent — the v2 reader cannot accidentally parse a v3 struct.
//
// Every struct here is unchanged from v2; the static_asserts pin the sizes the
// golden fixtures in tests/fixtures/v2/ were written at. Structs whose layout
// did NOT change in v3 (FileHead, FileTail, SectionEntry, SchemaEntryHead,
// ClusterSpecHeader, SortKey, LogicalTypeDescriptor, ColumnStatistics, the
// zone map and bitpack headers) are used from format.h directly.

#include <cstdint>

#include "skene/format.h"

namespace skene {
namespace v2 {

// The v2 file footer layout version; the v2 reader requires exactly this.
inline constexpr uint16_t kFileFooterVersion = 2u;

#pragma pack(push, 1)

// First record of the v2 FILE FOOTER.
struct FileFooterHeader {
    uint32_t footer_magic;       // kFileFooterMagic ("SKNI"), shared with v3
    uint16_t footer_version;     // 2
    uint16_t reserved;           // 0
    uint64_t row_count;          // TOTAL logical rows, summed over row groups
    uint32_t row_group_count;    // >= 1
    uint32_t column_count;       // top-level schema columns; children nested
    uint8_t  file_uuid[16];      // all-zero means unset
    uint64_t created_at_unix_us; // provenance only
    uint32_t writer_tag_bytes;   // followed by writer_tag_bytes
    uint32_t file_flags;         // 0; reserved
};

// One entry of the v2 row group directory: a row group was a contiguous
// [DATA][INDEX][FOOTER] unit at a known offset.
struct RowGroupEntry {
    uint64_t row_count;
    uint64_t first_row;
    uint64_t data_offset;        // absolute; start of its DATA region
    uint64_t data_bytes;         // its DATA + INDEX regions, up to its footer
    uint64_t footer_offset;      // absolute; start of its own footer
    uint64_t footer_checksum;    // over exactly footer_bytes at footer_offset
    uint32_t footer_bytes;
    uint32_t reserved;           // 0
};

// First record of a v2 ROW GROUP footer.
struct RowGroupFooterHeader {
    uint64_t row_count;          // logical rows in THIS row group
    uint32_t column_count;       // top-level columns; ARRAY children nested
    uint32_t section_count;
    uint8_t  file_uuid[16];
    uint64_t created_at_unix_us;
    uint32_t writer_tag_bytes;
    uint32_t file_flags;
};

// Fixed head of a v2 column directory entry — one per column per row group,
// in that row group's own footer. Followed by the name, an optional
// LogicalTypeDescriptor, then child entries depth first.
struct ColumnEntryHead {
    uint32_t field_id;
    uint32_t name_bytes;
    uint32_t type;               // DrakenType, verbatim
    uint8_t  vector_flags;       // DrakenVector.flags, verbatim
    uint8_t  logical_present;    // 0/1
    uint8_t  selection_kind;     // SelectionKind
    uint8_t  value_order;        // ValueOrder
    uint32_t length;
    uint32_t data_length;
    uint32_t child_count;        // 0 except DRAKEN_ARRAY
    uint32_t section_index;      // first REQUIRED-section entry
    uint32_t section_count;
    uint32_t stats_bytes;        // 0 == no statistics tracked
    uint64_t string_slot_count;
    uint64_t string_arena_used;
    uint64_t string_arena_cap;
    uint8_t  string_payloads_elided;
    uint8_t  pad[3];
    uint32_t index_section_index;
    uint32_t index_section_count;
    uint32_t reserved;
};

// Appended after ColumnStatistics inside a v2 statistics blob when the blob's
// flags carry kStatSketch: a PER-ROW-GROUP sketch, hash family
// kSketchFamilyXxh3Value (skene's own XXH3 dedup hash).
struct ColumnSketchHeader {
    uint32_t k;                  // the K this sketch was built at
    uint32_t count;              // min-hashes that follow, 0..k
};

#pragma pack(pop)

static_assert(sizeof(FileFooterHeader) == 56u, "v2 FileFooterHeader layout drift");
static_assert(sizeof(RowGroupEntry) == 56u, "v2 RowGroupEntry layout drift");
static_assert(sizeof(RowGroupFooterHeader) == 48u, "v2 RowGroupFooterHeader layout drift");
static_assert(sizeof(ColumnEntryHead) == 80u, "v2 ColumnEntryHead layout drift");
static_assert(sizeof(ColumnSketchHeader) == 8u, "v2 ColumnSketchHeader layout drift");

}  // namespace v2
}  // namespace skene
