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
#include "reader_v1.h"
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
    if (actual != tail.footer_checksum)
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
        case 1:
            return v1::read_metadata(bytes, file_bytes, footer_offset, footer_bytes, out);
        default:
            // Unreachable while open_file enforces the window, but a new version
            // added there and forgotten here must fail loud, not fall through.
            return unsupported_version(version);
    }
}

Status read_morsel(const void* file, size_t file_bytes, const ReadOptions& options,
                   CxxMorsel* out) {
    if (out == nullptr) return fail(Code::kMalformed, "read_morsel: out is null");

    uint16_t version = 0;
    uint64_t footer_offset = 0;
    uint32_t footer_bytes = 0;
    SKENE_RETURN_IF_ERROR(
        open_file(file, file_bytes, &version, &footer_offset, &footer_bytes));

    const uint8_t* bytes = static_cast<const uint8_t*>(file);
    switch (version) {
        case 1:
            return v1::read_morsel(bytes, file_bytes, footer_offset, footer_bytes,
                                   options, out);
        default:
            return unsupported_version(version);
    }
}

}  // namespace skene
