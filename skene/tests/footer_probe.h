#pragma once
// Independent navigation of a .skene file's two footer levels, for tests.
//
// Deliberately NOT built on skene::read_metadata: several suites exist to check
// that the bytes are laid out as FORMAT.md says, and a probe that went through
// the reader would only be checking the reader against itself. This walks the
// tail, the file footer header and the row group directory by hand, exactly as
// an independent implementation written from the specification would.

#include <cstdint>
#include <cstring>
#include <vector>

#include "skene/format.h"

namespace skene_test {

// The FILE FOOTER: the region the tail points at.
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

// One row group's directory entry, read out of the file footer.
inline bool row_group_entry(const std::vector<uint8_t>& bytes, uint32_t index,
                            skene::RowGroupEntry* out) {
    size_t offset = 0, length = 0;
    if (!file_footer_extent(bytes, &offset, &length)) return false;
    skene::FileFooterHeader header;
    if (!file_footer_header(bytes, &header)) return false;
    if (index >= header.row_group_count) return false;

    const size_t directory_at =
        offset + sizeof(skene::FileFooterHeader) + header.writer_tag_bytes;
    const size_t entry_at = directory_at + index * sizeof(skene::RowGroupEntry);
    if (entry_at + sizeof(skene::RowGroupEntry) > offset + length) return false;
    std::memcpy(out, bytes.data() + entry_at, sizeof(*out));
    return true;
}

// A row group's own footer — where its column and section directories live.
inline bool row_group_footer_extent(const std::vector<uint8_t>& bytes, uint32_t index,
                                    size_t* out_offset, size_t* out_bytes) {
    skene::RowGroupEntry entry;
    if (!row_group_entry(bytes, index, &entry)) return false;
    *out_offset = static_cast<size_t>(entry.footer_offset);
    *out_bytes  = entry.footer_bytes;
    return true;
}

inline bool row_group_footer_header(const std::vector<uint8_t>& bytes, uint32_t index,
                                    skene::RowGroupFooterHeader* out) {
    size_t offset = 0, length = 0;
    if (!row_group_footer_extent(bytes, index, &offset, &length)) return false;
    if (length < sizeof(skene::RowGroupFooterHeader)) return false;
    std::memcpy(out, bytes.data() + offset, sizeof(*out));
    return true;
}

}  // namespace skene_test
