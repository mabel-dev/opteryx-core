#pragma once
// Internal: the version-1 reader.
//
// One reader per format version, retained in the source for as long as
// migration may need it. `reader.cpp` validates the framing and dispatches here
// on the file's version; this file knows the v1 footer and section layout and
// nothing else. When v2 arrives it gets reader_v2.{h,cpp} and this one stays
// exactly as it is — an old reader that keeps changing is not an old reader.

#include <cstddef>

#include "skene/reader.h"
#include "skene/status.h"

namespace skene {
namespace v1 {

// `file` is the whole object and `footer_offset`/`footer_bytes` locate the FILE
// FOOTER; framing has already been validated by the caller.
Status read_metadata(const uint8_t* file, size_t file_bytes,
                     uint64_t footer_offset, uint32_t footer_bytes,
                     FileMetadata* out);

Status read_row_group_metadata(const uint8_t* file, size_t file_bytes,
                               uint64_t footer_offset, uint32_t footer_bytes,
                               uint32_t row_group, RowGroupMetadata* out);

Status read_morsel(const uint8_t* file, size_t file_bytes,
                   uint64_t footer_offset, uint32_t footer_bytes,
                   uint32_t row_group, const ReadOptions& options, CxxMorsel* out);

}  // namespace v1
}  // namespace skene
