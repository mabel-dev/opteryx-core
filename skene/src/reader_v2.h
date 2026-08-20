#pragma once
// Internal: the version-2 reader.
//
// One reader per format version, retained in the source for as long as
// migration may need it. `reader.cpp` validates the framing and dispatches here
// on the file's version; this file knows the v2 footer and section layout and
// nothing else.
//
// v2 against v1 (format.h changelog): SectionEntry carries a codec axis and
// encoded_bytes (48 bytes), string slots are four u32 lanes, sections start
// 64-byte aligned (a writer obligation this reader does not compute with), and
// the file footer carries a cluster spec and NDV statistics. This reader uses
// format.h's structs directly — format.h describes the CURRENT version, and v2
// is current; reader_v1.h holds the frozen v1 forms.

#include <cstddef>

#include "skene/reader.h"
#include "skene/status.h"

namespace skene {
namespace v2 {

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

}  // namespace v2
}  // namespace skene
