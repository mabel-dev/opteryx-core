#pragma once
// Internal: the version-3 reader (FORMAT.md, v3). `reader.cpp` validates the
// framing and dispatches here on the file's version.
//
// v3 has ONE footer carrying every pruning input, and a DIRECTORY BLOCK per
// column node in the DATA region carrying what decode needs. So opening a file
// is two steps — parse the footer, then attach the directory blocks of the
// columns a read needs — and a ranged reader fetches the second from the
// planned byte ranges. Column decode is shared with v2 (chunk_decode.h).

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "chunk_decode.h"
#include "skene/format.h"
#include "skene/reader.h"
#include "skene/status.h"

namespace skene {
namespace v3 {

// One column node: its footer summary and schema identity, its per-row-group
// statistics, and — once attached — its directory block.
struct Node {
    ColumnSummaryHead        head{};
    std::vector<BlockExtent> blocks;
    ColumnSketch             sketch;

    std::string              name;
    uint32_t                 field_id = 0;
    uint32_t                 type = 0;
    uint8_t                  logical_present = 0;
    LogicalTypeDescriptor    logical{};
    std::vector<uint32_t>    children;          // node indices

    std::vector<RowGroupColumnStatistics> stats; // one per row group

    bool                      attached = false;
    std::vector<ChunkRecord>  chunks;            // one per row group
    std::vector<SectionEntry> sections;
    std::string               extent_name;       // "column 'x''s extent", for messages
};

struct ReaderState {
    FileFooterHeader           header{};
    uint64_t                   footer_offset = 0;
    std::vector<RowGroupEntry> row_groups;
    std::vector<Node>          nodes;          // depth first
    std::vector<uint32_t>      top_level;      // node index of each top-level column
};

// Parses and validates the footer (FORMAT.md §11 items 6-8) into `state`, and
// the metadata it carries into `out`. `footer` holds exactly `footer_bytes`,
// already checksum-verified, located at `footer_offset` in the file.
Status parse_footer(const uint8_t* footer, size_t footer_bytes, uint64_t footer_offset,
                    FileMetadata* out, ReaderState* state);

// Verifies node `node`'s directory block against the footer's checksum, parses
// and validates it (§11 item 9). `bytes` holds exactly the directory block.
Status attach_directory(ReaderState* state, uint32_t node, const uint8_t* bytes,
                        size_t size);

// The nodes a read of `columns` (top-level names; empty == all) touches: the
// named top-level nodes in request order, and every node of their subtrees.
Status resolve_columns(const ReaderState& state, const std::vector<std::string>& columns,
                       std::vector<uint32_t>* top, std::vector<uint32_t>* all);

// Directory blocks of `nodes` not yet attached, coalesced by `policy`; with
// `through_first_block`, each range runs on to the end of the node's block 0.
void plan_directories(const ReaderState& state, const std::vector<uint32_t>& nodes,
                      bool through_first_block, const FetchPolicy& policy,
                      std::vector<ByteRange>* out);

// Chunk ranges of `nodes` for `row_groups`: one range per node per maximal run
// of consecutive row groups (a node's chunks are contiguous), then coalesced
// across nodes by `policy`. Every node must be attached.
Status plan_chunks(const ReaderState& state, const std::vector<uint32_t>& nodes,
                   const std::vector<uint32_t>& row_groups, const FetchPolicy& policy,
                   std::vector<ByteRange>* out);

// Decodes one row group of the top-level nodes `options.columns` names (all
// when empty) from `bytes`. Every node involved must be attached.
Status read_row_group(const ReaderState& state, const chunk::ByteSource& bytes,
                      uint32_t row_group, const ReadOptions& options, CxxMorsel* out);

// Per-row-group ColumnMetadata of every top-level column. Every node must be
// attached.
Status read_row_group_metadata(const ReaderState& state, const chunk::ByteSource& bytes,
                               uint32_t row_group, RowGroupMetadata* out);

}  // namespace v3
}  // namespace skene
