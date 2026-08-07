#include "reader_v1.h"

#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <memory>
#include <vector>

#include "encoding.h"
#include "skene/checksum.h"
#include "skene/format.h"

// draken — imported, never copied.
#include "core/alloc.h"
#include "core/buffers.h"
#include "core/string_slot.h"
#include "core/vector_alloc.h"
#include "core/vector_owner.h"
#include "logical_type.h"

namespace skene {
namespace v1 {
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

// ─── Bounds-checked footer cursor ───────────────────────────────────────────
//
// Every read is checked against the footer extent. The footer is attacker- and
// corruption-adjacent data whose own checksum has passed, but a checksum proves
// the bytes are the bytes that were written — not that the writer was sane. A
// length field can still say "a billion columns follow".
class Cursor {
  public:
    Cursor(const uint8_t* begin, size_t bytes) : p_(begin), end_(begin + bytes) {}

    bool take(void* dst, size_t n) {
        if (static_cast<size_t>(end_ - p_) < n) return false;
        std::memcpy(dst, p_, n);
        p_ += n;
        return true;
    }

    const uint8_t* raw(size_t n) {
        if (static_cast<size_t>(end_ - p_) < n) return nullptr;
        const uint8_t* result = p_;
        p_ += n;
        return result;
    }

    size_t remaining() const { return static_cast<size_t>(end_ - p_); }

  private:
    const uint8_t* p_;
    const uint8_t* end_;
};

// ─── Parsed footer ──────────────────────────────────────────────────────────

struct ParsedColumn {
    ColumnEntryHead           head{};
    std::string               name;
    LogicalTypeDescriptor     logical{};
    bool                      has_statistics = false;
    ColumnStatistics          statistics{};
    std::vector<ParsedColumn> children;
};

struct ParsedFooter {
    FooterFileHeader          file_header{};
    std::string               writer_tag;
    std::vector<ParsedColumn> columns;
    std::vector<SectionEntry> sections;
};

Status parse_column(Cursor& cursor, ParsedColumn* out, int depth) {
    // Nesting is ARRAY children only, and draken's own array support is
    // shallow. A bounded depth keeps a corrupt child_count from recursing the
    // stack to death before any other check can fire.
    if (depth > 32)
        return fail(Code::kMalformed,
                    "column nesting exceeds 32 levels; refusing to recurse further");

    if (!cursor.take(&out->head, sizeof(ColumnEntryHead)))
        return fail(Code::kTruncated, "footer ends inside a column directory entry");

    const uint8_t* name = cursor.raw(out->head.name_bytes);
    if (name == nullptr)
        return fail(Code::kTruncated,
                    "column name claims %u bytes but only %zu remain in the footer",
                    out->head.name_bytes, cursor.remaining());
    out->name.assign(reinterpret_cast<const char*>(name), out->head.name_bytes);

    if (out->head.logical_present) {
        if (!cursor.take(&out->logical, sizeof(LogicalTypeDescriptor)))
            return fail(Code::kTruncated,
                        "column '%s' declares a logical type descriptor but the "
                        "footer ends before it", out->name.c_str());
    }

    // child_count is 1 exactly for ARRAY and 0 otherwise; anything else is a
    // corrupt entry, and checking here bounds the loop below.
    const bool is_array = out->head.type == static_cast<uint32_t>(DRAKEN_ARRAY);
    if (is_array && out->head.child_count != 1u)
        return fail(Code::kMalformed,
                    "column '%s' is ARRAY with child_count %u; exactly one child "
                    "is required", out->name.c_str(), out->head.child_count);
    if (!is_array && out->head.child_count != 0u)
        return fail(Code::kMalformed,
                    "column '%s' has child_count %u but type %u is not ARRAY",
                    out->name.c_str(), out->head.child_count, out->head.type);

    out->children.resize(out->head.child_count);
    for (uint32_t i = 0; i < out->head.child_count; ++i)
        SKENE_RETURN_IF_ERROR(parse_column(cursor, &out->children[i], depth + 1));

    return Status::ok();
}

// Reads one column's statistics blob, then its children's, depth first.
//
// A blob LONGER than this build understands is read prefix-first and the
// remainder skipped. That is deliberate and is what lets a statistic be added
// with no version bump: an older reader takes the fields it knows and ignores
// the rest, which costs it a pruning opportunity and nothing else.
Status parse_statistics(Cursor& cursor, ParsedColumn* column) {
    const uint32_t declared = column->head.stats_bytes;
    if (declared > 0) {
        const uint8_t* blob = cursor.raw(declared);
        if (blob == nullptr)
            return fail(Code::kTruncated,
                        "column '%s' declares %u statistics bytes but only %zu "
                        "remain in the footer", column->name.c_str(), declared,
                        cursor.remaining());
        const size_t known = declared < sizeof(ColumnStatistics)
                           ? declared : sizeof(ColumnStatistics);
        std::memcpy(&column->statistics, blob, known);
        column->has_statistics = true;
    }
    for (ParsedColumn& child : column->children)
        SKENE_RETURN_IF_ERROR(parse_statistics(cursor, &child));
    return Status::ok();
}

Status parse_footer(const uint8_t* footer, uint32_t footer_bytes, ParsedFooter* out) {
    Cursor cursor(footer, footer_bytes);

    if (!cursor.take(&out->file_header, sizeof(FooterFileHeader)))
        return fail(Code::kTruncated, "footer is too small to hold its file header");

    const uint8_t* tag = cursor.raw(out->file_header.writer_tag_bytes);
    if (tag == nullptr)
        return fail(Code::kTruncated,
                    "writer tag claims %u bytes but only %zu remain in the footer",
                    out->file_header.writer_tag_bytes, cursor.remaining());
    out->writer_tag.assign(reinterpret_cast<const char*>(tag),
                           out->file_header.writer_tag_bytes);

    // Bound the counts by what could possibly fit before allocating for them: a
    // corrupt column_count of 4 billion must not become a 4-billion-element
    // reserve.
    if (static_cast<uint64_t>(out->file_header.column_count) * sizeof(ColumnEntryHead)
            > cursor.remaining())
        return fail(Code::kMalformed,
                    "footer claims %u columns, which cannot fit in its remaining "
                    "%zu bytes", out->file_header.column_count, cursor.remaining());

    out->columns.resize(out->file_header.column_count);
    for (uint32_t i = 0; i < out->file_header.column_count; ++i)
        SKENE_RETURN_IF_ERROR(parse_column(cursor, &out->columns[i], 0));

    if (static_cast<uint64_t>(out->file_header.section_count) * sizeof(SectionEntry)
            > cursor.remaining())
        return fail(Code::kMalformed,
                    "footer claims %u sections, which cannot fit in its remaining "
                    "%zu bytes", out->file_header.section_count, cursor.remaining());

    out->sections.resize(out->file_header.section_count);
    for (uint32_t i = 0; i < out->file_header.section_count; ++i) {
        if (!cursor.take(&out->sections[i], sizeof(SectionEntry)))
            return fail(Code::kTruncated, "footer ends inside the section directory");
    }

    // Statistics blobs: same depth-first order as the column directory, skipping
    // columns whose stats_bytes is 0. Located by ORDER, not by an offset.
    for (ParsedColumn& column : out->columns)
        SKENE_RETURN_IF_ERROR(parse_statistics(cursor, &column));

    return Status::ok();
}

// ─── Section access ─────────────────────────────────────────────────────────

// Resolves a section, validating its extent and verifying its checksum BEFORE
// the bytes are used. §11: nothing is interpreted before it is verified.
//
// The checksum covers the STORED bytes, so verification happens on what is about
// to be decoded rather than on the decoded result — a corrupt body is caught
// before it is fed to a decoder, not after.
struct SectionRef {
    bool           present = false;
    const uint8_t* stored = nullptr;
    uint64_t       stored_bytes = 0;
    uint64_t       plain_bytes = 0;
    Encoding       encoding = Encoding::kPlain;
};

class SectionResolver {
  public:
    SectionResolver(const uint8_t* file, uint64_t data_region_end,
                    const std::vector<SectionEntry>& sections)
        : file_(file), data_region_end_(data_region_end), sections_(sections) {}

    // Finds the single section of `kind` within a column's slice. An absent
    // section is legal for several kinds (no validity means all-valid; no arena
    // means no long payloads), so absence is reported, not an error.
    Status find(const ColumnEntryHead& head, const char* column_name,
                SectionKind kind, SectionRef* out) const {
        *out = SectionRef();

        if (static_cast<uint64_t>(head.section_index) + head.section_count
                > sections_.size())
            return fail(Code::kMalformed,
                        "column '%s' references sections [%u, %u) but only %zu "
                        "exist", column_name, head.section_index,
                        head.section_index + head.section_count, sections_.size());

        for (uint32_t i = 0; i < head.section_count; ++i) {
            const SectionEntry& entry = sections_[head.section_index + i];
            if (entry.kind != static_cast<uint16_t>(kind)) continue;
            if (out->present)
                return fail(Code::kMalformed,
                            "column '%s' has two sections of kind %u",
                            column_name, entry.kind);
            SKENE_RETURN_IF_ERROR(resolve(entry, column_name, out));
        }
        return Status::ok();
    }

    // Same lookup, over the column's INDEX slice. Optional sections live there,
    // contiguous with the footer, so a pruning reader fetches them all at once.
    Status find_index(const ColumnEntryHead& head, const char* column_name,
                      SectionKind kind, SectionRef* out) const {
        *out = SectionRef();
        if (static_cast<uint64_t>(head.index_section_index) + head.index_section_count
                > sections_.size())
            return fail(Code::kMalformed,
                        "column '%s' references index sections [%u, %u) but only "
                        "%zu exist", column_name, head.index_section_index,
                        head.index_section_index + head.index_section_count,
                        sections_.size());

        for (uint32_t i = 0; i < head.index_section_count; ++i) {
            const SectionEntry& entry = sections_[head.index_section_index + i];

            // A REQUIRED kind in the index slice would be silently skipped by
            // any reader that does not know it — which is exactly the failure
            // the required/optional split exists to prevent. The split is only
            // safe if required sections never live where skipping is allowed.
            if (section_is_required(entry.kind))
                return fail(Code::kMalformed,
                            "column '%s': required section kind %u appears in the "
                            "index slice, where an unknown kind would be skipped",
                            column_name, entry.kind);

            if (entry.kind != static_cast<uint16_t>(kind)) continue;  // skipped
            if (out->present)
                return fail(Code::kMalformed,
                            "column '%s' has two index sections of kind %u",
                            column_name, entry.kind);
            SKENE_RETURN_IF_ERROR(resolve(entry, column_name, out));
        }
        return Status::ok();
    }

    // An unrecognised REQUIRED section kind is fatal; an unrecognised OPTIONAL
    // one is skipped. This is the rule the whole extensibility story rests on,
    // so it is enforced once, here.
    Status check_kinds(const ColumnEntryHead& head, const char* column_name) const {
        for (uint32_t i = 0; i < head.section_count; ++i) {
            const SectionEntry& entry = sections_[head.section_index + i];
            if (!section_is_required(entry.kind)) continue;  // skippable
            switch (static_cast<SectionKind>(entry.kind)) {
                case SectionKind::kData:
                case SectionKind::kSelection:
                case SectionKind::kValidity:
                case SectionKind::kStringSlots:
                case SectionKind::kStringArena:
                    break;
                default:
                    return fail(Code::kUnsupportedSection,
                                "column '%s' carries required section kind %u, "
                                "which this build does not implement; the column "
                                "cannot be reconstructed without it",
                                column_name, entry.kind);
            }
        }
        return Status::ok();
    }

  private:
    Status resolve(const SectionEntry& entry, const char* column_name,
                   SectionRef* out) const {
        switch (static_cast<Encoding>(entry.encoding)) {
            case Encoding::kPlain:
                if (entry.stored_bytes != entry.plain_bytes)
                    return fail(Code::kMalformed,
                                "column '%s': section kind %u is PLAIN but declares "
                                "%llu stored bytes and %llu plain bytes",
                                column_name, entry.kind,
                                static_cast<unsigned long long>(entry.stored_bytes),
                                static_cast<unsigned long long>(entry.plain_bytes));
                break;
            case Encoding::kBitpack:
            case Encoding::kDeltaBitpack:
            case Encoding::kZstd:
                break;
            default:
                // A required section this build cannot decode is fatal. Adding an
                // encoding for a required section is therefore a version bump —
                // an older reader must never guess at a body it cannot read.
                return fail(Code::kUnsupportedEncoding,
                            "column '%s': section kind %u uses encoding %u, which "
                            "this build does not implement",
                            column_name, entry.kind, entry.encoding);
        }

        if (entry.offset < kFileHeadBytes
                || entry.stored_bytes > data_region_end_
                || entry.offset > data_region_end_ - entry.stored_bytes)
            return fail(Code::kMalformed,
                        "column '%s': section kind %u spans [%llu, %llu) which "
                        "is outside the data region [%zu, %llu)",
                        column_name, entry.kind,
                        static_cast<unsigned long long>(entry.offset),
                        static_cast<unsigned long long>(entry.offset + entry.stored_bytes),
                        kFileHeadBytes,
                        static_cast<unsigned long long>(data_region_end_));

        const uint8_t* data = file_ + entry.offset;
        const uint64_t actual = checksum_xxh3_64(data, entry.stored_bytes);
        if (actual != entry.checksum)
            return fail(Code::kChecksumMismatch,
                        "column '%s': section kind %u fails its checksum "
                        "(recorded %llu, computed %llu)",
                        column_name, entry.kind,
                        static_cast<unsigned long long>(entry.checksum),
                        static_cast<unsigned long long>(actual));

        out->present      = true;
        out->stored       = data;
        out->stored_bytes = entry.stored_bytes;
        out->plain_bytes  = entry.plain_bytes;
        out->encoding     = static_cast<Encoding>(entry.encoding);
        return Status::ok();
    }

    const uint8_t*                   file_;
    uint64_t                         data_region_end_;
    const std::vector<SectionEntry>& sections_;
};

// Materializes a section into its own buffer.
//
// Every consumer of a section body must go through this or decode_into — reading
// `stored` directly is only correct for an uncompressed body, and a compressed
// one is both shorter and differently shaped. That mistake reads past the end of
// the frame, so the safe form is the only form offered.
Status materialize(const SectionRef& section, const char* column_name,
                   std::vector<uint8_t>* out);

// Decodes a zone map body. Returns OK with `out->chunk_rows == 0` when the
// section is absent — an optional section is allowed to be missing, and its
// absence costs a pruning opportunity, never correctness.
Status parse_zone_map(const SectionRef& section, const ParsedColumn& column,
                      ZoneMap* out) {
    *out = ZoneMap();
    if (!section.present) return Status::ok();

    if (section.plain_bytes < sizeof(ZoneMapHeader))
        return fail(Code::kMalformed,
                    "column '%s': zone map is too small to hold its header",
                    column.name.c_str());

    std::vector<uint8_t> body;
    SKENE_RETURN_IF_ERROR(materialize(section, column.name.c_str(), &body));

    ZoneMapHeader header;
    std::memcpy(&header, body.data(), sizeof(header));

    if (header.chunk_rows == 0)
        return fail(Code::kMalformed,
                    "column '%s': zone map declares zero rows per chunk",
                    column.name.c_str());

    // The chunk count is a FUNCTION of length and chunk_rows, so a count that
    // disagrees is a contradiction rather than a shape to be honoured.
    const uint64_t expect =
        (static_cast<uint64_t>(column.head.length) + header.chunk_rows - 1u)
        / header.chunk_rows;
    if (header.chunk_count != expect)
        return fail(Code::kMalformed,
                    "column '%s': zone map declares %u chunks but %u rows at %u "
                    "rows per chunk require %llu", column.name.c_str(),
                    header.chunk_count, column.head.length, header.chunk_rows,
                    static_cast<unsigned long long>(expect));

    const uint64_t needed = sizeof(ZoneMapHeader)
                          + static_cast<uint64_t>(header.chunk_count) * sizeof(ZoneMapEntry);
    if (section.plain_bytes != needed)
        return fail(Code::kMalformed,
                    "column '%s': zone map is %llu bytes but %u chunks require %llu",
                    column.name.c_str(),
                    static_cast<unsigned long long>(section.plain_bytes),
                    header.chunk_count, static_cast<unsigned long long>(needed));

    out->chunk_rows = header.chunk_rows;
    out->chunks.resize(header.chunk_count);
    if (header.chunk_count > 0)
        std::memcpy(out->chunks.data(), body.data() + sizeof(ZoneMapHeader),
                    static_cast<size_t>(header.chunk_count) * sizeof(ZoneMapEntry));

    // Ordinals have no structural bound to check against — unlike codes, they do
    // not index anything. What must hold is that a range is either well-formed or
    // the EXACT empty sentinel an all-null chunk carries. An arbitrary inverted
    // range is corruption and would silently prune rows that match.
    for (uint32_t i = 0; i < header.chunk_count; ++i) {
        const ZoneMapEntry& chunk = out->chunks[i];
        const bool well_formed = chunk.min_ordinal <= chunk.max_ordinal;
        const bool empty_sentinel =
            chunk.min_ordinal == INT64_MAX && chunk.max_ordinal == INT64_MIN;
        if (!well_formed && !empty_sentinel)
            return fail(Code::kMalformed,
                        "column '%s': zone map chunk %u spans ordinals "
                        "[%lld, %lld], which is inverted without being the "
                        "all-null sentinel",
                        column.name.c_str(), i,
                        static_cast<long long>(chunk.min_ordinal),
                        static_cast<long long>(chunk.max_ordinal));
    }
    return Status::ok();
}

// Materializes a section into `destination`, which must hold plain_bytes.
//
// Decodes STRAIGHT INTO the final draken buffer rather than into a scratch
// vector that is then copied — an encoded section otherwise costs two passes
// over the data for no reason.
Status decode_into(const SectionRef& section, const char* column_name,
                   uint32_t count, size_t item_bytes, uint8_t* destination) {
    switch (section.encoding) {
        case Encoding::kPlain:
            std::memcpy(destination, section.stored,
                        static_cast<size_t>(section.plain_bytes));
            return Status::ok();
        case Encoding::kBitpack:
            return bitpack_decode_codes(section.stored, section.stored_bytes, count,
                                        reinterpret_cast<uint32_t*>(destination));
        case Encoding::kDeltaBitpack:
            return delta_bitpack_decode(section.stored, section.stored_bytes, count,
                                        item_bytes, destination);
        case Encoding::kZstd:
            return zstd_decode(section.stored, section.stored_bytes,
                               section.plain_bytes, destination);
    }
    return fail(Code::kUnsupportedEncoding,
                "column '%s': unhandled encoding %u", column_name,
                static_cast<unsigned>(section.encoding));
}

Status materialize(const SectionRef& section, const char* column_name,
                   std::vector<uint8_t>* out) {
    out->resize(static_cast<size_t>(section.plain_bytes));
    if (section.plain_bytes == 0) return Status::ok();
    return decode_into(section, column_name, 0, 0, out->data());
}

// ─── Buffer construction ────────────────────────────────────────────────────

// Copies `bytes` into a draken-allocated buffer of at least `bytes`, padded up
// to `pad_to_multiple` and zero-filled beyond the copy. The padding matters for
// validity bitmaps: draken's own allocators pad them to 8 bytes so SIMD bitmap
// ops can read whole words without walking off the end.
// Allocates a zeroed draken buffer of at least `bytes`, padded up to
// `pad_to_multiple`. Padding matters for validity bitmaps: draken's own
// allocators pad them to 8 bytes so SIMD bitmap ops can read whole words without
// walking off the end.
Status allocate_buffer(size_t bytes, size_t pad_to_multiple,
                       OwnedBuffer<uint8_t>* out) {
    size_t allocate = bytes;
    if (pad_to_multiple > 1)
        allocate = (bytes + pad_to_multiple - 1u) & ~(pad_to_multiple - 1u);
    if (allocate == 0) allocate = pad_to_multiple > 0 ? pad_to_multiple : 1u;

    uint8_t* buffer = static_cast<uint8_t*>(draken_malloc(allocate));
    if (buffer == nullptr)
        return fail(Code::kOutOfMemory, "failed to allocate %zu bytes", allocate);
    std::memset(buffer, 0, allocate);
    out->reset(buffer);
    return Status::ok();
}

struct BuildContext {
    const SectionResolver* resolver;
};

Status build_column(const BuildContext& ctx, const ParsedColumn& parsed,
                    CxxColumn* out);

// Rebuilds the DrakenStringArena block. `slots` and `arena` are ABSOLUTE
// pointers in memory and are never stored, so the block is allocated fresh and
// the two pointers are pointed into it:
//   [ DrakenStringArena | DrakenStringSlot[n] | arena bytes ]
Status build_string_data(const BuildContext& ctx, const ParsedColumn& parsed,
                         OwnedBuffer<void>* out_block) {
    const ColumnEntryHead& head = parsed.head;
    const char* name = parsed.name.c_str();

    SectionRef slots_section;
    SKENE_RETURN_IF_ERROR(ctx.resolver->find(head, name, SectionKind::kStringSlots,
                                             &slots_section));
    if (!slots_section.present)
        return fail(Code::kMalformed,
                    "column '%s' is string-typed but has no slot section", name);
    const uint64_t slot_len = slots_section.plain_bytes;
    std::vector<uint8_t> slot_storage;
    SKENE_RETURN_IF_ERROR(materialize(slots_section, name, &slot_storage));
    const uint8_t* slot_bytes = slot_storage.data();

    const uint64_t expect_slots =
        head.string_slot_count * sizeof(DrakenStringSlot);
    if (slot_len != expect_slots)
        return fail(Code::kMalformed,
                    "column '%s': slot section is %llu bytes but %llu slots "
                    "require %llu", name,
                    static_cast<unsigned long long>(slot_len),
                    static_cast<unsigned long long>(head.string_slot_count),
                    static_cast<unsigned long long>(expect_slots));

    // Codes index into the slot array; a data_length beyond it would let the
    // uniform access path address slots that do not exist.
    if (head.data_length > head.string_slot_count)
        return fail(Code::kMalformed,
                    "column '%s': data_length %u exceeds slot count %llu",
                    name, head.data_length,
                    static_cast<unsigned long long>(head.string_slot_count));

    SectionRef arena_section;
    SKENE_RETURN_IF_ERROR(ctx.resolver->find(head, name, SectionKind::kStringArena,
                                             &arena_section));
    const uint64_t arena_len   = arena_section.plain_bytes;
    const bool     arena_present = arena_section.present;
    std::vector<uint8_t> arena_storage;
    if (arena_present)
        SKENE_RETURN_IF_ERROR(materialize(arena_section, name, &arena_storage));
    const uint8_t* arena_bytes = arena_storage.data();
    if (arena_present && arena_len != head.string_arena_used)
        return fail(Code::kMalformed,
                    "column '%s': arena section is %llu bytes but arena_used is "
                    "%llu", name, static_cast<unsigned long long>(arena_len),
                    static_cast<unsigned long long>(head.string_arena_used));
    if (!arena_present && head.string_arena_used != 0)
        return fail(Code::kMalformed,
                    "column '%s': arena_used is %llu but there is no arena section",
                    name, static_cast<unsigned long long>(head.string_arena_used));

    // payloads_elided, VERIFIED — not merely trusted. A length-only column has a
    // NULL arena and long slots stamped with the trap offset 0xFFFFFFFF; if the
    // flag and the slots disagree, a str_data() lands ~4 GB out and faults, or
    // worse, silently reads adjacent memory. This is the single most dangerous
    // inconsistency the format can carry across a process boundary.
    const DrakenStringSlot* slots =
        reinterpret_cast<const DrakenStringSlot*>(slot_bytes);
    if (head.string_payloads_elided) {
        if (head.string_arena_used != 0 || arena_present)
            return fail(Code::kMalformed,
                        "column '%s': payloads_elided is set but the file carries "
                        "%llu arena bytes", name,
                        static_cast<unsigned long long>(head.string_arena_used));
        for (uint64_t i = 0; i < head.string_slot_count; ++i) {
            if (str_is_inline(&slots[i])) continue;
            if (slots[i].ext.arena_offset != STR_ELIDED_PAYLOAD_OFFSET)
                return fail(Code::kMalformed,
                            "column '%s': payloads_elided is set but slot %llu "
                            "carries arena offset %u instead of the elided trap "
                            "value", name, static_cast<unsigned long long>(i),
                            slots[i].ext.arena_offset);
        }
    } else {
        for (uint64_t i = 0; i < head.string_slot_count; ++i) {
            if (str_is_inline(&slots[i])) continue;
            const uint64_t end = static_cast<uint64_t>(slots[i].ext.arena_offset)
                               + str_length(&slots[i]);
            if (end > head.string_arena_used)
                return fail(Code::kMalformed,
                            "column '%s': slot %llu spans arena bytes [%u, %llu) "
                            "but only %llu are present", name,
                            static_cast<unsigned long long>(i),
                            slots[i].ext.arena_offset,
                            static_cast<unsigned long long>(end),
                            static_cast<unsigned long long>(head.string_arena_used));
        }
    }

    const size_t struct_end  = sizeof(DrakenStringArena);
    const size_t slots_bytes = static_cast<size_t>(expect_slots);
    const size_t arena_size  = static_cast<size_t>(head.string_arena_used);
    const size_t total = struct_end + (slots_bytes > 0 ? slots_bytes
                                                       : sizeof(DrakenStringSlot))
                       + arena_size;

    uint8_t* block = static_cast<uint8_t*>(draken_malloc(total));
    if (block == nullptr)
        return fail(Code::kOutOfMemory, "failed to allocate %zu string bytes", total);
    std::memset(block, 0, total);
    OwnedBuffer<uint8_t> guard(block);

    DrakenStringSlot* dst_slots =
        reinterpret_cast<DrakenStringSlot*>(block + struct_end);
    if (slots_bytes > 0) std::memcpy(dst_slots, slot_bytes, slots_bytes);

    uint8_t* dst_arena = nullptr;
    if (arena_size > 0) {
        dst_arena = block + struct_end + slots_bytes;
        std::memcpy(dst_arena, arena_bytes, arena_size);
    }

    DrakenStringArena* sa = reinterpret_cast<DrakenStringArena*>(block);
    sa->slots           = dst_slots;
    sa->arena           = dst_arena;
    sa->length          = head.string_slot_count;
    sa->arena_used      = head.string_arena_used;
    sa->arena_cap       = head.string_arena_used;  // the block holds exactly this
    sa->null_bitmap     = nullptr;                 // set by the caller, which owns validity
    sa->owns_buffers    = 0;                       // the VectorOwner IS the record
    sa->payloads_elided = head.string_payloads_elided;
    sa->type            = static_cast<DrakenType>(head.type);

    out_block->reset(guard.release());
    return Status::ok();
}

Status validate_head_consistency(const ParsedColumn& parsed) {
    const ColumnEntryHead& head = parsed.head;
    const char* name = parsed.name.c_str();

    switch (static_cast<SelectionKind>(head.selection_kind)) {
        case SelectionKind::kConstant:
            if (head.data_length != 1u && head.length > 0)
                return fail(Code::kMalformed,
                            "column '%s': selection_kind is CONSTANT but "
                            "data_length is %u, not 1", name, head.data_length);
            break;
        case SelectionKind::kIdentity:
            if (head.data_length != head.length)
                return fail(Code::kMalformed,
                            "column '%s': selection_kind is IDENTITY but "
                            "data_length (%u) != length (%u)",
                            name, head.data_length, head.length);
            break;
        case SelectionKind::kStored:
            break;
        default:
            return fail(Code::kMalformed,
                        "column '%s': unknown selection_kind %u",
                        name, head.selection_kind);
    }

    if (head.value_order > static_cast<uint8_t>(ValueOrder::kAscending))
        return fail(Code::kMalformed, "column '%s': unknown value_order %u",
                    name, head.value_order);

    // Layout hints are pure hints, but a hint that contradicts the stored layout
    // means the file disagrees with itself, and DRAKEN_DICT_KEYS_SORTED in
    // particular is trusted absolutely by binary-search consumers. The footer
    // checksum protects these bytes from corruption; this catches a broken
    // writer.
    if ((head.vector_flags & DRAKEN_SEL_IDENTITY)
            && head.selection_kind != static_cast<uint8_t>(SelectionKind::kIdentity))
        return fail(Code::kMalformed,
                    "column '%s': SEL_IDENTITY is set but selection_kind is %u",
                    name, head.selection_kind);
    if ((head.vector_flags & DRAKEN_SEL_PERMUTATION)
            && head.data_length != head.length)
        return fail(Code::kMalformed,
                    "column '%s': SEL_PERMUTATION is set but data_length (%u) != "
                    "length (%u)", name, head.data_length, head.length);

    return Status::ok();
}

Status build_column(const BuildContext& ctx, const ParsedColumn& parsed,
                    CxxColumn* out) {
    const ColumnEntryHead& head = parsed.head;
    const char* name = parsed.name.c_str();
    const DrakenType type = static_cast<DrakenType>(head.type);

    SKENE_RETURN_IF_ERROR(validate_head_consistency(parsed));
    SKENE_RETURN_IF_ERROR(ctx.resolver->check_kinds(head, name));

    // ── Logical type: re-interned, never restored as a pointer ──
    const LogicalType* logical = nullptr;
    if (head.logical_present) {
        LogicalType lt;
        lt.kind           = static_cast<LogicalKind>(parsed.logical.kind);
        lt.unit           = static_cast<TimestampUnit>(parsed.logical.unit);
        lt.offset_minutes = parsed.logical.offset_minutes;
        lt.precision      = parsed.logical.precision;
        lt.scale          = parsed.logical.scale;
        lt.dimension      = parsed.logical.dimension;
        logical = logical_type_intern(lt);
    }

    // ── Validity ──
    SectionRef validity_section;
    SKENE_RETURN_IF_ERROR(ctx.resolver->find(head, name, SectionKind::kValidity,
                                             &validity_section));
    const bool has_validity = validity_section.present;
    const uint64_t validity_len = validity_section.plain_bytes;
    OwnedBuffer<uint8_t> validity_buf(nullptr);
    if (has_validity) {
        const uint64_t expect = (static_cast<uint64_t>(head.length) + 7u) / 8u;
        if (validity_len != expect)
            return fail(Code::kMalformed,
                        "column '%s': validity is %llu bytes but %u rows require "
                        "%llu", name, static_cast<unsigned long long>(validity_len),
                        head.length, static_cast<unsigned long long>(expect));
        SKENE_RETURN_IF_ERROR(allocate_buffer(static_cast<size_t>(validity_len), 8u,
                                             &validity_buf));
        SKENE_RETURN_IF_ERROR(decode_into(validity_section, name, 0, 0,
                                          validity_buf.get()));
    }

    // ── Payload ──
    OwnedBuffer<void> data_buf(nullptr);
    if (draken_type_is_string_storage(type)) {
        SKENE_RETURN_IF_ERROR(build_string_data(ctx, parsed, &data_buf));
        DrakenStringArena* sa = static_cast<DrakenStringArena*>(data_buf.get());
        // draken keeps this as a convenience alias of the vector's validity; the
        // DrakenVector's own `validity` stays authoritative.
        sa->null_bitmap = validity_buf.get();
    } else if (type != DRAKEN_NULL) {
        uint64_t expect = 0;
        if (type == DRAKEN_BOOL) {
            expect = (static_cast<uint64_t>(head.data_length) + 7u) / 8u;
        } else if (type == DRAKEN_ARRAY) {
            expect = (static_cast<uint64_t>(head.length) + 1u) * sizeof(int32_t);
        } else {
            const size_t itemsize = draken_type_itemsize(type, logical);
            if (itemsize == 0)
                return fail(Code::kUnsupportedType,
                            "column '%s': no fixed item width for physical type %u",
                            name, head.type);
            expect = static_cast<uint64_t>(head.data_length) * itemsize;
        }

        SectionRef data_section;
        SKENE_RETURN_IF_ERROR(ctx.resolver->find(head, name, SectionKind::kData,
                                                 &data_section));
        if (!data_section.present)
            return fail(Code::kMalformed, "column '%s' has no data section", name);

        // The DECODED size must match the shape the directory declares. Checking
        // plain_bytes rather than stored_bytes is what makes an encoding a pure
        // size optimization: the column's shape is decided by the directory, and
        // a body that decodes to a different size is a contradiction.
        if (data_section.plain_bytes != expect)
            return fail(Code::kMalformed,
                        "column '%s': data section decodes to %llu bytes but the "
                        "declared shape requires %llu", name,
                        static_cast<unsigned long long>(data_section.plain_bytes),
                        static_cast<unsigned long long>(expect));

        OwnedBuffer<uint8_t> raw(nullptr);
        SKENE_RETURN_IF_ERROR(allocate_buffer(static_cast<size_t>(expect), 8u, &raw));
        const size_t item_bytes = (type == DRAKEN_BOOL || type == DRAKEN_ARRAY)
                                ? 0u : draken_type_itemsize(type, logical);
        SKENE_RETURN_IF_ERROR(decode_into(data_section, name, head.data_length,
                                          item_bytes, raw.get()));
        data_buf.reset(raw.release());
    }

    // ── Selection ──
    const SelectionKind selection_kind = static_cast<SelectionKind>(head.selection_kind);
    OwnedBuffer<void> codes_buf(nullptr);
    const uint32_t* codes = nullptr;

    SectionRef selection_section;
    SKENE_RETURN_IF_ERROR(ctx.resolver->find(head, name, SectionKind::kSelection,
                                             &selection_section));
    const bool sel_present = selection_section.present;

    if (selection_kind == SelectionKind::kStored) {
        if (!sel_present)
            return fail(Code::kMalformed,
                        "column '%s': selection_kind is STORED but there is no "
                        "selection section", name);
        const uint64_t expect = static_cast<uint64_t>(head.length) * sizeof(uint32_t);
        if (selection_section.plain_bytes != expect)
            return fail(Code::kMalformed,
                        "column '%s': selection decodes to %llu bytes but %u rows "
                        "require %llu", name,
                        static_cast<unsigned long long>(selection_section.plain_bytes),
                        head.length, static_cast<unsigned long long>(expect));

        OwnedBuffer<uint8_t> raw(nullptr);
        SKENE_RETURN_IF_ERROR(allocate_buffer(static_cast<size_t>(expect), 8u, &raw));
        SKENE_RETURN_IF_ERROR(decode_into(selection_section, name, head.length,
                                          sizeof(uint32_t), raw.get()));
        codes = reinterpret_cast<const uint32_t*>(raw.get());

        // Every code in range. Without this a corrupt file turns the uniform
        // data[selection[i]] access into an out-of-bounds read on every
        // consumer, forever.
        for (uint32_t i = 0; i < head.length; ++i) {
            if (codes[i] >= head.data_length)
                return fail(Code::kMalformed,
                            "column '%s': selection[%u] == %u is out of range for "
                            "data_length %u", name, i, codes[i], head.data_length);
        }
        codes_buf.reset(raw.release());
    } else if (sel_present) {
        return fail(Code::kMalformed,
                    "column '%s': selection_kind is %u, which stores no selection "
                    "section, but one is present", name, head.selection_kind);
    }

    // ── Assemble ──
    DrakenVector vec;
    switch (selection_kind) {
        case SelectionKind::kConstant:
            vec = draken_vector_from_constant(data_buf.get(), head.length, type,
                                              validity_buf.get());
            break;
        case SelectionKind::kIdentity:
            vec = draken_vector_from_dense(data_buf.get(), head.length, type,
                                           validity_buf.get());
            break;
        case SelectionKind::kStored:
            vec = draken_vector_from_dict(data_buf.get(), head.data_length, codes,
                                          head.length, type, validity_buf.get());
            break;
    }

    // Layout hints restored VERBATIM. The constructors above set their own
    // conservative defaults; the file's flags are what the writer actually
    // knew, and re-deriving them instead of restoring them is exactly what
    // disqualified Parquet.
    vec.flags = head.vector_flags;

    VectorOwner owner(vec, std::move(data_buf), std::move(validity_buf),
                      std::move(codes_buf));
    owner.logical_type = logical;

    // ── ARRAY child ──
    if (type == DRAKEN_ARRAY) {
        if (parsed.children.size() != 1u)
            return fail(Code::kMalformed,
                        "column '%s': ARRAY with %zu children", name,
                        parsed.children.size());

        CxxColumn child;
        SKENE_RETURN_IF_ERROR(build_column(ctx, parsed.children[0], &child));

        // Offsets must be monotonic and must not address past the child, or
        // every array-row read walks off the end of the element vector.
        const int32_t* offsets = static_cast<const int32_t*>(owner.vec.data);
        const uint32_t child_length = child.view.length;
        int32_t previous = 0;
        for (uint32_t i = 0; i <= head.length; ++i) {
            const int32_t value = offsets[i];
            if (value < 0)
                return fail(Code::kMalformed,
                            "column '%s': array offset[%u] is negative (%d)",
                            name, i, value);
            if (i > 0 && value < previous)
                return fail(Code::kMalformed,
                            "column '%s': array offsets are not monotonic "
                            "(offset[%u] == %d follows %d)",
                            name, i, value, previous);
            if (static_cast<uint32_t>(value) > child_length)
                return fail(Code::kMalformed,
                            "column '%s': array offset[%u] == %d addresses past "
                            "the %u child elements", name, i, value, child_length);
            previous = value;
        }

        owner.child_owner = std::make_unique<VectorOwner>(std::move(*child.own));
    }

    out->own  = std::make_shared<VectorOwner>(std::move(owner));
    out->view = out->own->vec;
    return Status::ok();
}

Status fill_metadata(const ParsedColumn& parsed,
                     const std::vector<SectionEntry>& sections,
                     const SectionResolver& resolver,
                     ColumnMetadata* out) {
    const ColumnEntryHead& head = parsed.head;
    out->name            = parsed.name;
    out->field_id        = head.field_id;
    out->type            = head.type;
    out->logical_present = head.logical_present != 0;
    out->logical         = parsed.logical;
    out->length          = head.length;
    out->data_length     = head.data_length;
    out->vector_flags    = head.vector_flags;
    out->selection_kind  = static_cast<SelectionKind>(head.selection_kind);
    out->value_order     = static_cast<ValueOrder>(head.value_order);
    out->has_statistics  = parsed.has_statistics;
    out->statistics      = parsed.statistics;

    // Extent covering this column AND its descendants, so a caller can fetch a
    // whole column subtree with one range request.
    uint64_t begin = UINT64_MAX;
    uint64_t end = 0;
    for (uint32_t i = 0; i < head.section_count; ++i) {
        const uint64_t index = static_cast<uint64_t>(head.section_index) + i;
        if (index >= sections.size()) break;
        const SectionEntry& entry = sections[index];
        if (entry.offset < begin) begin = entry.offset;
        if (entry.offset + entry.stored_bytes > end) end = entry.offset + entry.stored_bytes;
    }

    SectionRef bloom_section;
    SKENE_RETURN_IF_ERROR(resolver.find_index(head, parsed.name.c_str(),
                                              SectionKind::kBloom, &bloom_section));
    if (bloom_section.present)
        SKENE_RETURN_IF_ERROR(
            materialize(bloom_section, parsed.name.c_str(), &out->bloom));

    SectionRef zone_section;
    SKENE_RETURN_IF_ERROR(resolver.find_index(head, parsed.name.c_str(),
                                              SectionKind::kZoneMap, &zone_section));
    SKENE_RETURN_IF_ERROR(parse_zone_map(zone_section, parsed, &out->zone_map));

    // DATA sections only. The index slice is deliberately NOT included: index
    // sections live in the index region, which is contiguous with the footer so
    // that a pruning reader gets every column's indexes in the same request as
    // the footer. Folding them into a column's extent would make that extent
    // span every other column's data, which is exactly what it exists to avoid.
    //
    // So the two requests are: footer+indexes (prune), then one data extent per
    // surviving column (read). Neither pays for the other.

    out->children.resize(parsed.children.size());
    for (size_t i = 0; i < parsed.children.size(); ++i) {
        SKENE_RETURN_IF_ERROR(
            fill_metadata(parsed.children[i], sections, resolver, &out->children[i]));
        const ColumnMetadata& child = out->children[i];
        if (child.byte_bytes > 0) {
            if (child.byte_offset < begin) begin = child.byte_offset;
            if (child.byte_offset + child.byte_bytes > end)
                end = child.byte_offset + child.byte_bytes;
        }
    }

    if (begin == UINT64_MAX) { out->byte_offset = 0; out->byte_bytes = 0; }
    else { out->byte_offset = begin; out->byte_bytes = end - begin; }
    return Status::ok();
}

}  // namespace

Status read_metadata(const uint8_t* file, size_t file_bytes,
                     uint64_t footer_offset, uint32_t footer_bytes,
                     FileMetadata* out) {
    (void)file_bytes;
    ParsedFooter footer;
    SKENE_RETURN_IF_ERROR(parse_footer(file + footer_offset, footer_bytes, &footer));

    out->version            = 1u;
    out->row_count          = footer.file_header.row_count;
    out->created_at_unix_us = footer.file_header.created_at_unix_us;
    out->writer_tag         = footer.writer_tag;
    std::memcpy(out->file_uuid, footer.file_header.file_uuid, sizeof(out->file_uuid));

    SectionResolver resolver(file, footer_offset, footer.sections);
    out->columns.resize(footer.columns.size());
    for (size_t i = 0; i < footer.columns.size(); ++i)
        SKENE_RETURN_IF_ERROR(fill_metadata(footer.columns[i], footer.sections,
                                            resolver, &out->columns[i]));

    return Status::ok();
}

Status read_morsel(const uint8_t* file, size_t file_bytes,
                   uint64_t footer_offset, uint32_t footer_bytes,
                   const ReadOptions& options, CxxMorsel* out) {
    (void)file_bytes;
    ParsedFooter footer;
    SKENE_RETURN_IF_ERROR(parse_footer(file + footer_offset, footer_bytes, &footer));

    SectionResolver resolver(file, footer_offset, footer.sections);
    BuildContext ctx{&resolver};

    // Select columns. A requested name that is not present is an error: silently
    // returning fewer columns than asked for hides the caller's bug.
    std::vector<const ParsedColumn*> wanted;
    if (options.columns.empty()) {
        for (const ParsedColumn& column : footer.columns) wanted.push_back(&column);
    } else {
        for (const std::string& name : options.columns) {
            const ParsedColumn* found = nullptr;
            for (const ParsedColumn& column : footer.columns)
                if (column.name == name) { found = &column; break; }
            if (found == nullptr)
                return fail(Code::kMalformed,
                            "requested column '%s' is not in this file",
                            name.c_str());
            wanted.push_back(found);
        }
    }

    CxxMorsel morsel;
    morsel.columns.reserve(wanted.size());
    morsel.names.reserve(wanted.size());
    for (const ParsedColumn* column : wanted) {
        CxxColumn built;
        SKENE_RETURN_IF_ERROR(build_column(ctx, *column, &built));
        if (built.view.length != footer.file_header.row_count)
            return fail(Code::kMalformed,
                        "column '%s' has %u rows but the file declares %llu",
                        column->name.c_str(), built.view.length,
                        static_cast<unsigned long long>(footer.file_header.row_count));
        morsel.columns.push_back(std::move(built));
        morsel.names.push_back(column->name);
    }

    // A zero-column morsel still has a row count, and it lives nowhere else.
    if (morsel.columns.empty())
        morsel.zero_col_rows =
            static_cast<uint32_t>(footer.file_header.row_count);

    *out = std::move(morsel);
    return Status::ok();
}

}  // namespace v1
}  // namespace skene
