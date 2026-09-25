#pragma once
// Internal: the version-2 reader — RETAINED and FROZEN since the v3 bump
// (2026-09-24). It reads v2 files while v2 is inside the read window, directly
// and as migrate_file's input. `reader.cpp` validates the framing and
// dispatches here on the file's version; this reader knows the v2 footers
// (format_v2.h) and nothing else. Column decode is shared with v3
// (chunk_decode.h) because the section bytes did not change.

#include <cstddef>
#include <cstdint>
#include <vector>

#include "format_v2.h"
#include "skene/reader.h"
#include "skene/status.h"

namespace skene {
namespace v2 {

// What a FileReader keeps for a v2 file: the verified row group directory.
struct ReaderState {
    std::vector<RowGroupEntry> directory;   // v2::RowGroupEntry, parallel to row groups
};

// `file` is the whole object and `footer_offset`/`footer_bytes` locate the FILE
// FOOTER; framing has already been validated by the caller. `state`, when
// non-null, receives the verified row group directory from the SAME parse.
Status read_metadata(const uint8_t* file, size_t file_bytes,
                     uint64_t footer_offset, uint32_t footer_bytes,
                     FileMetadata* out, ReaderState* state);

Status read_row_group_metadata(const uint8_t* file, size_t file_bytes,
                               uint64_t footer_offset, uint32_t footer_bytes,
                               uint32_t row_group, RowGroupMetadata* out);

// Reads one row group of a file whose footer has already been verified and
// parsed into `state`. Touches only that row group's footer and sections.
Status read_morsel_at(const uint8_t* file, const ReaderState& state, uint32_t row_group,
                      const ReadOptions& options, CxxMorsel* out);

}  // namespace v2
}  // namespace skene
