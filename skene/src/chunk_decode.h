#pragma once
// Internal: decoding ONE column node for ONE row group from its sections.
//
// Shared by the v2 and v3 readers because the bytes it reads are the same in
// both: a v3 CHUNK is exactly the set of sections v2 wrote for a column inside a
// row group — the section entry, encodings, codecs, string lanes, validity and
// selection are unchanged (FORMAT.md §3, §5.11, §7). What differs between the
// versions is only where the facts ABOUT those sections come from (a v2 row
// group footer vs a v3 directory block) and what extent bounds them (a v2 row
// group's region vs a v3 column node's data/index extent). Both readers
// translate their footer into a ChunkNode and hand it here.
//
// The day a version changes a chunk's bytes, this forks: the older reader keeps
// a frozen copy, as reader_v1 did at the v2 bump. Until then one implementation
// serves both, so a decode fix cannot land in one reader and miss the other.

#include <cstdint>
#include <string>
#include <vector>

#include "skene/format.h"
#include "skene/reader.h"
#include "skene/status.h"

// draken — imported, never copied.
#include "morsels/cxx_morsel.h"

namespace skene {
namespace chunk {

// Absolute file offsets to bytes the caller holds. A whole-buffer read is one
// range covering the file; a ranged read is the ranges the caller fetched.
// Ranges are sorted by offset (sort_ranges) so lookup is a binary search.
class ByteSource {
  public:
    ByteSource() = default;
    explicit ByteSource(std::vector<FetchedRange> ranges);

    // A pointer to [offset, offset + bytes), or nullptr when no single range
    // holds all of it.
    const uint8_t* find(uint64_t offset, uint64_t bytes) const;

  private:
    std::vector<FetchedRange> ranges_;   // ascending by offset
};

// A byte extent of the file: [offset, offset + bytes).
struct Extent {
    uint64_t offset = 0;
    uint64_t bytes  = 0;
};

// The per-row-group facts about one column node — version-neutral and in memory
// only. v2 fills it from a ColumnEntryHead, v3 from the schema entry plus a
// ChunkRecord. Field meanings are FORMAT.md §5.10's.
struct ChunkHead {
    uint32_t field_id = 0;
    uint32_t type = 0;                    // DrakenType
    uint8_t  logical_present = 0;
    uint8_t  vector_flags = 0;
    uint8_t  selection_kind = 0;
    uint8_t  value_order = 0;
    uint8_t  string_payloads_elided = 0;
    uint32_t length = 0;
    uint32_t data_length = 0;
    uint32_t section_index = 0;
    uint32_t section_count = 0;
    uint32_t index_section_index = 0;
    uint32_t index_section_count = 0;
    uint64_t string_slot_count = 0;
    uint64_t string_arena_used = 0;
    uint64_t string_arena_cap = 0;
};

// A resolved section: validated against its directory entry and its extent, its
// checksum verified, and its stored bytes located. §11: nothing is interpreted
// before it is verified.
struct SectionRef {
    bool           present = false;
    const uint8_t* stored = nullptr;
    uint64_t       stored_bytes = 0;
    uint64_t       encoded_bytes = 0;   // post-codec, pre-encoding
    uint64_t       plain_bytes = 0;
    Encoding       encoding = Encoding::kPlain;
    SectionCodec   codec = SectionCodec::kNone;
};

// Finds and verifies a column's sections in a section list. One per v2 row
// group (both extents are that row group's region) or per v3 column node (the
// node's data extent for required sections, its index extent for optional
// ones). `extent_name` names the extent in messages ("row group 3's region",
// "column 'x''s extent"); `version` names the file version in messages.
class SectionResolver {
  public:
    SectionResolver(const ByteSource& bytes, const std::vector<SectionEntry>& sections,
                    Extent required, Extent optional, const char* extent_name,
                    uint16_t version);

    Status find(const ChunkHead& head, const char* column_name, SectionKind kind,
                SectionRef* out) const;
    Status find_index(const ChunkHead& head, const char* column_name,
                      SectionKind kind, SectionRef* out) const;
    Status check_kinds(const ChunkHead& head, const char* column_name) const;
    Status check_data_slice(const ChunkHead& head, const char* column_name) const;

    const std::vector<SectionEntry>& sections() const noexcept { return sections_; }

  private:
    Status check_slice(uint32_t index, uint32_t count, const char* slice_name,
                       const char* column_name) const;
    Status resolve(const SectionEntry& entry, const char* column_name, bool optional,
                   SectionRef* out) const;

    const ByteSource&                bytes_;
    const std::vector<SectionEntry>& sections_;
    Extent                           required_;
    Extent                           optional_;
    const char*                      extent_name_;
    uint16_t                         version_;
};

// One column node in one row group, ready to decode. ARRAY children nest. Each
// node carries its own resolver: in v2 every node of a row group shares one, in
// v3 each node has its own section list and extents.
struct ChunkNode {
    ChunkHead               head;
    std::string             name;
    LogicalTypeDescriptor   logical{};
    bool                    has_statistics = false;
    ColumnStatistics        statistics{};
    const SectionResolver*  resolver = nullptr;
    std::vector<ChunkNode>  children;
};

// Rebuilds the column. `length_only`: see ReadOptions::length_only.
Status build_column(const ChunkNode& node, bool length_only, CxxColumn* out);

// Builds one row group from its top-level nodes: selects `options.columns` by
// name (all of `nodes` when empty — a requested name that is absent is an
// error), validates `length_only` against that selection, builds each column,
// and checks every column's length against `row_count`.
Status build_row_group(const std::vector<ChunkNode>& nodes, uint64_t row_count,
                       const ReadOptions& options, CxxMorsel* out);

// Per-row-group ColumnMetadata: shape, extent, bloom, zone map, statistics.
Status fill_metadata(const ChunkNode& node, ColumnMetadata* out);

}  // namespace chunk
}  // namespace skene
