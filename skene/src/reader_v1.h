#pragma once
// Internal: the version-1 reader.
//
// One reader per format version, retained in the source for as long as
// migration may need it. `reader.cpp` validates the framing and dispatches here
// on the file's version; this file knows the v1 footer and section layout and
// nothing else. When v2 arrives it gets reader_v2.{h,cpp} and this one stays
// exactly as it is — an old reader that keeps changing is not an old reader.

#include <cstddef>
#include <cstdint>

#include "skene/reader.h"
#include "skene/status.h"

namespace skene {
namespace v1 {

// The v1 section directory entry, FROZEN at the layout v1 files were written
// with. format.h's SectionEntry moved to the 48-byte v2 form (codec axis +
// encoded_bytes); this copy exists so reader_v1 keeps parsing the bytes v1
// actually wrote. Inside namespace v1 it shadows skene::SectionEntry for the
// whole v1 reader, which is exactly the intent.
//
// v1 has no codec field: the codec was spelled IN `encoding` (kZstd = 3,
// kLz4 = 4), and plain_bytes served as the codec's decode capacity.
#pragma pack(push, 1)
struct SectionEntry {
    uint16_t kind;           // SectionKind
    uint16_t encoding;       // Encoding, including the v1 codec spellings 3/4
    uint64_t offset;         // absolute, from file start
    uint64_t stored_bytes;   // on disk, post-encoding
    uint64_t plain_bytes;    // after decoding; == stored_bytes when kPlain
    uint64_t checksum;       // over the STORED bytes
};
#pragma pack(pop)

static_assert(sizeof(SectionEntry) == 36u, "v1 SectionEntry layout drift");

// The v1 statistics blob is a 48-byte PREFIX of the current ColumnStatistics
// (v2 appended `ndv`; blobs are length-prefixed and read prefix-first), so v1
// needs no frozen copy — reader_v1 reads the declared length into the shared
// struct and the appended field stays zero with its flag unset.

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
