// Framing validation and version dispatch.
//
// Everything here is version-independent and must stay that way: it is the code
// that decides WHICH reader to use, so it cannot depend on any layout a version
// bump might move. In practice that means it touches only the head, the tail,
// and the object size.

#include <cstdarg>
#include <cstdio>
#include <cstring>

#include "bloom.h"
#include "chunk_decode.h"
#include "reader_v2.h"
#include "reader_v3.h"
#include "skene/checksum.h"
#include "skene/format.h"
#include "skene/probe.h"
#include "skene/reader.h"

namespace skene {
namespace {

Status fail(Code code, const char* fmt, ...) __attribute__((format(printf, 2, 3)));
Status fail(Code code, const char* fmt, ...) {
    char buffer[640];
    va_list args;
    va_start(args, fmt);
    std::vsnprintf(buffer, sizeof(buffer), fmt, args);
    va_end(args);
    return Status(code, buffer);
}

Status validate_tail(const FileTail& tail, uint64_t file_bytes,
                     uint64_t* out_footer_offset) {
    // Magic first, always: an unrelated or truncated object is rejected before
    // any of its bytes are given meaning.
    if (tail.magic != kMagic)
        return Status(Code::kNotSkene,
                      "not a .skene file (tail magic mismatch)");

    // Version second, and the message must name BOTH versions plus the way
    // forward — a build reads at most two versions, so "unsupported" without the
    // migration route leaves an operator guessing which binary to fetch.
    if (!version_is_supported(tail.version)) {
        char advice[448];
        migration_advice(tail.version, advice, sizeof(advice));
        return fail(Code::kUnsupportedVersion, "%s", advice);
    }

    if (tail.endianness != static_cast<uint8_t>(Endianness::kLittle))
        return fail(Code::kWrongEndianness,
                    "file declares endianness %u; this build reads only "
                    "little-endian (%u). The format copies buffers verbatim, so "
                    "byte-swapping is not a correct recovery.",
                    static_cast<unsigned>(tail.endianness),
                    static_cast<unsigned>(Endianness::kLittle));

    if (tail.checksum_algorithm != static_cast<uint8_t>(ChecksumAlgorithm::kXxh3_64))
        return fail(Code::kUnknownChecksum,
                    "file uses checksum algorithm %u; this build implements "
                    "only %u (XXH3-64), so its integrity cannot be verified",
                    static_cast<unsigned>(tail.checksum_algorithm),
                    static_cast<unsigned>(ChecksumAlgorithm::kXxh3_64));

    // Reserved bytes MUST be zero, and are CHECKED rather than ignored.
    //
    // Nothing checksums the head, and the tail's reserved bytes sit outside the
    // footer checksum, so an "ignore it" rule would leave 12 bytes of every file
    // unverified — a hole a corruption sweep finds immediately. Checking is free
    // and cannot cost forward compatibility: any future version that gives these
    // bytes meaning bumps the version, and this reader rejects that version
    // anyway.
    if (tail.reserved != 0)
        return fail(Code::kMalformed,
                    "tail reserved bytes are %u, not 0 — either the file is "
                    "corrupt or it was written by a version this build has "
                    "misidentified", tail.reserved);

    // Declared extents against the real size, before any of them is followed.
    const uint64_t footer_end = file_bytes - kFileTailBytes;
    if (tail.footer_bytes > footer_end - kFileHeadBytes)
        return fail(Code::kTruncated,
                    "footer claims %u bytes but only %llu are available between "
                    "the head and the tail",
                    tail.footer_bytes,
                    static_cast<unsigned long long>(footer_end - kFileHeadBytes));

    *out_footer_offset = footer_end - tail.footer_bytes;
    return Status::ok();
}

// Validates framing and hands back where the footer is. Shared by both entry
// points so they cannot diverge on what "valid enough to parse" means.
Status open_file(const void* file, size_t file_bytes, uint16_t* out_version,
                 uint64_t* out_footer_offset, uint32_t* out_footer_bytes) {
    if (file == nullptr)
        return fail(Code::kMalformed, "file buffer is null");
    if (file_bytes < kMinFileBytes)
        return fail(Code::kTruncated,
                    "object is %zu bytes; the smallest well-formed .skene file "
                    "is %zu", file_bytes, kMinFileBytes);

    const uint8_t* bytes = static_cast<const uint8_t*>(file);

    FileHead head;
    std::memcpy(&head, bytes, sizeof(head));
    if (head.magic != kMagic)
        return Status(Code::kNotSkene, "not a .skene file (head magic mismatch)");

    // See the note in validate_tail: reserved bytes are checked, not ignored,
    // because nothing else verifies the head at all.
    if (head.reserved != 0)
        return fail(Code::kMalformed,
                    "head reserved bytes are %llu, not 0 — either the file is "
                    "corrupt or it was written by a version this build has "
                    "misidentified",
                    static_cast<unsigned long long>(head.reserved));

    FileTail tail;
    std::memcpy(&tail, bytes + file_bytes - kFileTailBytes, sizeof(tail));

    // The head and tail duplicate version/endianness/checksum precisely so a
    // range-GET reader that only fetched the tail is as safe as one that read
    // byte 0. If they disagree, one of them is a lie and we cannot tell which.
    if (head.version != tail.version || head.endianness != tail.endianness
            || head.checksum_algorithm != tail.checksum_algorithm)
        return fail(Code::kMalformed,
                    "head and tail disagree (version %u/%u, endianness %u/%u, "
                    "checksum %u/%u) — the file is inconsistent with itself",
                    head.version, tail.version, head.endianness, tail.endianness,
                    head.checksum_algorithm, tail.checksum_algorithm);

    uint64_t footer_offset = 0;
    SKENE_RETURN_IF_ERROR(validate_tail(tail, file_bytes, &footer_offset));

    // Only now is it safe to look at footer content.
    const uint64_t actual = checksum_xxh3_64(bytes + footer_offset, tail.footer_bytes);
    if (actual != tail.footer_checksum && checksum_must_match())
        return fail(Code::kChecksumMismatch,
                    "footer checksum mismatch: recorded %llu, computed %llu — "
                    "the directory is corrupt and every offset in it is suspect",
                    static_cast<unsigned long long>(tail.footer_checksum),
                    static_cast<unsigned long long>(actual));

    *out_version       = tail.version;
    *out_footer_offset = footer_offset;
    *out_footer_bytes  = tail.footer_bytes;
    return Status::ok();
}

Status unsupported_version(uint16_t version) {
    char advice[448];
    migration_advice(version, advice, sizeof(advice));
    return Status(Code::kUnsupportedVersion, advice);
}

}  // namespace

Status bloom_may_contain(const ColumnMetadata& column, const void* value_bytes,
                         uint32_t value_length, bool* out_may_contain) {
    if (out_may_contain == nullptr)
        return fail(Code::kMalformed, "bloom_may_contain: null output");

    // No filter means no information, which must read as "cannot rule out".
    // Answering false here would let a missing accelerator drop real rows.
    if (column.bloom.empty()) { *out_may_contain = true; return Status::ok(); }

    return bloom_probe(column.bloom.data(), column.bloom.size(), value_bytes,
                       value_length, out_may_contain);
}

Status footer_extent(const void* tail_buffer, size_t tail_bytes, uint64_t file_bytes,
                     uint64_t* out_offset, uint64_t* out_bytes) {
    if (out_offset == nullptr || out_bytes == nullptr)
        return fail(Code::kMalformed, "footer_extent: null output");
    if (tail_buffer == nullptr || tail_bytes < kFileTailBytes)
        return fail(Code::kTruncated,
                    "footer_extent: need the last %zu bytes, got %zu",
                    kFileTailBytes, tail_bytes);
    if (file_bytes < kMinFileBytes)
        return fail(Code::kTruncated,
                    "footer_extent: object is %llu bytes; the smallest "
                    "well-formed .skene file is %zu",
                    static_cast<unsigned long long>(file_bytes), kMinFileBytes);

    // Take the LAST kFileTailBytes of whatever was supplied, so a caller that
    // over-read the tail (the sensible thing to do against object storage) is
    // handled without making them slice it themselves.
    FileTail tail;
    std::memcpy(&tail, static_cast<const uint8_t*>(tail_buffer) + tail_bytes
                           - kFileTailBytes, sizeof(tail));

    uint64_t footer_offset = 0;
    SKENE_RETURN_IF_ERROR(validate_tail(tail, file_bytes, &footer_offset));

    *out_offset = footer_offset;
    *out_bytes  = tail.footer_bytes;
    return Status::ok();
}

Status read_metadata(const void* file, size_t file_bytes, FileMetadata* out) {
    if (out == nullptr) return fail(Code::kMalformed, "read_metadata: out is null");

    uint16_t version = 0;
    uint64_t footer_offset = 0;
    uint32_t footer_bytes = 0;
    SKENE_RETURN_IF_ERROR(
        open_file(file, file_bytes, &version, &footer_offset, &footer_bytes));

    const uint8_t* bytes = static_cast<const uint8_t*>(file);
    switch (version) {
        case 2:
            return v2::read_metadata(bytes, file_bytes, footer_offset, footer_bytes, out,
                                     nullptr);
        case 3: {
            v3::ReaderState state;
            return v3::parse_footer(bytes + footer_offset, footer_bytes, footer_offset,
                                    out, &state);
        }
        default:
            // Unreachable while open_file enforces the window, but a new version
            // added there and forgotten here must fail loud, not fall through.
            return unsupported_version(version);
    }
}

Status read_row_group_metadata(const void* file, size_t file_bytes,
                               uint32_t row_group, RowGroupMetadata* out) {
    if (out == nullptr)
        return fail(Code::kMalformed, "read_row_group_metadata: out is null");

    uint16_t version = 0;
    uint64_t footer_offset = 0;
    uint32_t footer_bytes = 0;
    SKENE_RETURN_IF_ERROR(
        open_file(file, file_bytes, &version, &footer_offset, &footer_bytes));

    const uint8_t* bytes = static_cast<const uint8_t*>(file);
    switch (version) {
        case 2:
            return v2::read_row_group_metadata(bytes, file_bytes, footer_offset,
                                               footer_bytes, row_group, out);
        case 3: {
            FileReader reader;
            SKENE_RETURN_IF_ERROR(open_reader(file, file_bytes, &reader));
            const chunk::ByteSource source({FetchedRange{0, file_bytes, bytes}});
            return v3::read_row_group_metadata(*reader.v3_, source, row_group, out);
        }
        default:
            return unsupported_version(version);
    }
}

Status read_morsel(const void* file, size_t file_bytes, uint32_t row_group,
                   const ReadOptions& options, CxxMorsel* out) {
    if (out == nullptr) return fail(Code::kMalformed, "read_morsel: out is null");
    FileReader reader;
    SKENE_RETURN_IF_ERROR(open_reader(file, file_bytes, &reader));
    return read_morsel(reader, row_group, options, out);
}

Status open_reader(const void* file, size_t file_bytes, FileReader* out) {
    if (out == nullptr) return fail(Code::kMalformed, "open_reader: out is null");

    uint16_t version = 0;
    uint64_t footer_offset = 0;
    uint32_t footer_bytes = 0;
    SKENE_RETURN_IF_ERROR(
        open_file(file, file_bytes, &version, &footer_offset, &footer_bytes));

    // Built into a local and moved in only on success: a failed open leaves the
    // caller's reader unopened (version 0), never half-filled.
    FileReader opened;
    const uint8_t* bytes = static_cast<const uint8_t*>(file);
    switch (version) {
        case 2: {
            auto state = std::make_shared<v2::ReaderState>();
            SKENE_RETURN_IF_ERROR(v2::read_metadata(bytes, file_bytes, footer_offset,
                                                    footer_bytes, &opened.metadata_,
                                                    state.get()));
            opened.v2_ = std::move(state);
            break;
        }
        case 3: {
            auto state = std::make_shared<v3::ReaderState>();
            SKENE_RETURN_IF_ERROR(v3::parse_footer(bytes + footer_offset, footer_bytes,
                                                   footer_offset, &opened.metadata_,
                                                   state.get()));
            // Whole buffer: every column's directory block is here, so attach
            // them all now. parse_footer bounded each against the data region.
            for (uint32_t n = 0; n < state->nodes.size(); ++n) {
                const ColumnSummaryHead& h = state->nodes[n].head;
                SKENE_RETURN_IF_ERROR(v3::attach_directory(
                    state.get(), n, bytes + h.directory_offset, h.directory_bytes));
            }
            opened.v3_ = std::move(state);
            break;
        }
        default:
            return unsupported_version(version);
    }
    opened.file_       = bytes;
    opened.file_bytes_ = file_bytes;
    opened.version_    = version;
    *out = std::move(opened);
    return Status::ok();
}

Status open_reader_ranged(const void* tail_buffer, size_t tail_bytes,
                          const void* footer, size_t footer_bytes,
                          uint64_t footer_offset, uint64_t file_bytes,
                          FileReader* out) {
    if (out == nullptr) return fail(Code::kMalformed, "open_reader_ranged: out is null");
    if (tail_buffer == nullptr || tail_bytes < kFileTailBytes || footer == nullptr)
        return fail(Code::kTruncated,
                    "open_reader_ranged: need the last %zu bytes and the footer",
                    kFileTailBytes);
    if (file_bytes < kMinFileBytes)
        return fail(Code::kTruncated,
                    "object is %llu bytes; the smallest well-formed .skene file is %zu",
                    static_cast<unsigned long long>(file_bytes), kMinFileBytes);

    // A ranged reader never reads byte 0, so the TAIL's copies of version,
    // endianness and checksum algorithm are what it validates (FORMAT.md §3.1).
    FileTail tail;
    std::memcpy(&tail, static_cast<const uint8_t*>(tail_buffer) + tail_bytes
                           - kFileTailBytes, sizeof(tail));
    uint64_t expect_offset = 0;
    SKENE_RETURN_IF_ERROR(validate_tail(tail, file_bytes, &expect_offset));
    if (footer_offset != expect_offset || footer_bytes != tail.footer_bytes)
        return fail(Code::kMalformed,
                    "open_reader_ranged: handed a footer at [%llu, +%zu) but the tail "
                    "locates it at [%llu, +%u)",
                    static_cast<unsigned long long>(footer_offset), footer_bytes,
                    static_cast<unsigned long long>(expect_offset), tail.footer_bytes);
    if (tail.version != 3u)
        return fail(Code::kUnsupportedVersion,
                    "file is v%u; a ranged read needs v3, because a v%u file keeps "
                    "its decode metadata in per-row-group footers. Read it whole, or "
                    "migrate it to v3.", static_cast<unsigned>(tail.version),
                    static_cast<unsigned>(tail.version));

    const uint8_t* footer_bytes_ptr = static_cast<const uint8_t*>(footer);
    const uint64_t actual = checksum_xxh3_64(footer_bytes_ptr, footer_bytes);
    if (actual != tail.footer_checksum && checksum_must_match())
        return fail(Code::kChecksumMismatch,
                    "footer checksum mismatch: recorded %llu, computed %llu — "
                    "the directory is corrupt and every offset in it is suspect",
                    static_cast<unsigned long long>(tail.footer_checksum),
                    static_cast<unsigned long long>(actual));

    FileReader opened;
    auto state = std::make_shared<v3::ReaderState>();
    SKENE_RETURN_IF_ERROR(v3::parse_footer(footer_bytes_ptr, footer_bytes, footer_offset,
                                           &opened.metadata_, state.get()));
    opened.v3_         = std::move(state);
    opened.file_       = nullptr;
    opened.file_bytes_ = file_bytes;
    opened.version_    = 3u;
    *out = std::move(opened);
    return Status::ok();
}

namespace {

Status require_open(const FileReader* reader, const char* what) {
    if (reader->version() == 0)
        return fail(Code::kMalformed,
                    "%s: the FileReader was never opened (open_reader failed or was "
                    "not called)", what);
    return Status::ok();
}

Status require_v3(const FileReader* reader, const char* what) {
    SKENE_RETURN_IF_ERROR(require_open(reader, what));
    if (reader->version() != 3u)
        return fail(Code::kUnsupportedVersion,
                    "%s needs a v3 file; this one is v%u, whose decode metadata lives "
                    "in per-row-group footers", what,
                    static_cast<unsigned>(reader->version()));
    return Status::ok();
}

}  // namespace

Status read_morsel(const FileReader& reader, uint32_t row_group,
                   const ReadOptions& options, CxxMorsel* out) {
    if (out == nullptr) return fail(Code::kMalformed, "read_morsel: out is null");
    SKENE_RETURN_IF_ERROR(require_open(&reader, "read_morsel"));
    if (reader.ranged())
        return fail(Code::kMalformed,
                    "read_morsel: this reader was opened ranged and holds no file "
                    "bytes — pass the fetched ranges");
    switch (reader.version_) {
        case 2:
            return v2::read_morsel_at(reader.file_, *reader.v2_, row_group, options, out);
        case 3: {
            const chunk::ByteSource source(
                {FetchedRange{0, reader.file_bytes_, reader.file_}});
            return v3::read_row_group(*reader.v3_, source, row_group, options, out);
        }
        default:
            return unsupported_version(reader.version_);
    }
}

Status read_morsel(const FileReader& reader, uint32_t row_group,
                   const ReadOptions& options, const std::vector<FetchedRange>& ranges,
                   CxxMorsel* out) {
    if (out == nullptr) return fail(Code::kMalformed, "read_morsel: out is null");
    SKENE_RETURN_IF_ERROR(require_v3(&reader, "read_morsel (ranged)"));
    const chunk::ByteSource source(ranges);
    return v3::read_row_group(*reader.v3_, source, row_group, options, out);
}

Status plan_directory_fetch(const FileReader& reader,
                            const std::vector<std::string>& columns,
                            bool through_first_block, const FetchPolicy& policy,
                            std::vector<ByteRange>* out) {
    if (out == nullptr) return fail(Code::kMalformed, "plan_directory_fetch: out is null");
    SKENE_RETURN_IF_ERROR(require_v3(&reader, "plan_directory_fetch"));
    std::vector<uint32_t> top, all;
    SKENE_RETURN_IF_ERROR(v3::resolve_columns(*reader.v3_, columns, &top, &all));
    v3::plan_directories(*reader.v3_, all, through_first_block, policy, out);
    return Status::ok();
}

Status attach_directories(FileReader* reader, const std::vector<std::string>& columns,
                          const std::vector<FetchedRange>& ranges) {
    if (reader == nullptr) return fail(Code::kMalformed, "attach_directories: null reader");
    SKENE_RETURN_IF_ERROR(require_v3(reader, "attach_directories"));
    std::vector<uint32_t> top, all;
    SKENE_RETURN_IF_ERROR(v3::resolve_columns(*reader->v3_, columns, &top, &all));
    const chunk::ByteSource source(ranges);
    for (uint32_t n : all) {
        v3::Node& node = reader->v3_->nodes[n];
        if (node.attached) continue;
        const uint8_t* bytes =
            source.find(node.head.directory_offset, node.head.directory_bytes);
        if (bytes == nullptr)
            return fail(Code::kMalformed,
                        "attach_directories: column '%s''s directory block [%llu, +%u) "
                        "lies in no fetched range", node.name.c_str(),
                        static_cast<unsigned long long>(node.head.directory_offset),
                        node.head.directory_bytes);
        SKENE_RETURN_IF_ERROR(
            v3::attach_directory(reader->v3_.get(), n, bytes, node.head.directory_bytes));
    }
    return Status::ok();
}

Status plan_fetch(const FileReader& reader, const std::vector<std::string>& columns,
                  const std::vector<uint32_t>& row_groups, const FetchPolicy& policy,
                  std::vector<ByteRange>* out) {
    if (out == nullptr) return fail(Code::kMalformed, "plan_fetch: out is null");
    SKENE_RETURN_IF_ERROR(require_v3(&reader, "plan_fetch"));
    std::vector<uint32_t> top, all;
    SKENE_RETURN_IF_ERROR(v3::resolve_columns(*reader.v3_, columns, &top, &all));
    return v3::plan_chunks(*reader.v3_, all, row_groups, policy, out);
}

}  // namespace skene
