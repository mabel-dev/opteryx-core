// The v2 reader. Forked from reader_v1.cpp at the v2 bump (2026-08-20) — the
// two share their skeleton by ancestry, not by reference: v1 is FROZEN and this
// file is the one that tracks format.h. The v2 differences are the section
// entry's codec axis (two-stage decode), the slot lanes, the cluster spec, and
// the file footer version.

#include "reader_v2.h"

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
namespace v2 {
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

struct ParsedRowGroupFooter {
    RowGroupFooterHeader      header{};
    std::string               writer_tag;
    std::vector<ParsedColumn> columns;
    std::vector<SectionEntry> sections;
};

// ─── Parsed FILE footer (the file index) ────────────────────────────────────

struct ParsedSchema {
    SchemaEntryHead           head{};
    std::string               name;
    LogicalTypeDescriptor     logical{};
    std::vector<ParsedSchema> children;
};

struct ParsedFileFooter {
    FileFooterHeader           header{};
    std::string                writer_tag;
    std::vector<RowGroupEntry> row_groups;
    std::vector<ParsedSchema>  schema;
    std::vector<SortKey>       cluster_keys;   // v2; empty == unclustered
    // Row group major, then the schema's depth-first column order.
    std::vector<std::vector<RowGroupColumnStatistics>> statistics;
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

Status parse_row_group_footer(const uint8_t* footer, uint32_t footer_bytes,
                              uint32_t row_group, ParsedRowGroupFooter* out) {
    Cursor cursor(footer, footer_bytes);

    if (!cursor.take(&out->header, sizeof(RowGroupFooterHeader)))
        return fail(Code::kTruncated,
                    "row group %u footer is too small to hold its header",
                    row_group);

    const uint8_t* tag = cursor.raw(out->header.writer_tag_bytes);
    if (tag == nullptr)
        return fail(Code::kTruncated,
                    "row group %u: writer tag claims %u bytes but only %zu "
                    "remain in the footer", row_group,
                    out->header.writer_tag_bytes, cursor.remaining());
    out->writer_tag.assign(reinterpret_cast<const char*>(tag),
                           out->header.writer_tag_bytes);

    // Bound the counts by what could possibly fit before allocating for them: a
    // corrupt column_count of 4 billion must not become a 4-billion-element
    // reserve.
    if (static_cast<uint64_t>(out->header.column_count) * sizeof(ColumnEntryHead)
            > cursor.remaining())
        return fail(Code::kMalformed,
                    "row group %u claims %u columns, which cannot fit in its "
                    "remaining %zu footer bytes", row_group,
                    out->header.column_count, cursor.remaining());

    out->columns.resize(out->header.column_count);
    for (uint32_t i = 0; i < out->header.column_count; ++i)
        SKENE_RETURN_IF_ERROR(parse_column(cursor, &out->columns[i], 0));

    if (static_cast<uint64_t>(out->header.section_count) * sizeof(SectionEntry)
            > cursor.remaining())
        return fail(Code::kMalformed,
                    "row group %u claims %u sections, which cannot fit in its "
                    "remaining %zu footer bytes", row_group,
                    out->header.section_count, cursor.remaining());

    out->sections.resize(out->header.section_count);
    for (uint32_t i = 0; i < out->header.section_count; ++i) {
        if (!cursor.take(&out->sections[i], sizeof(SectionEntry)))
            return fail(Code::kTruncated,
                        "row group %u footer ends inside the section directory",
                        row_group);
    }

    // Statistics blobs: same depth-first order as the column directory, skipping
    // columns whose stats_bytes is 0. Located by ORDER, not by an offset.
    for (ParsedColumn& column : out->columns)
        SKENE_RETURN_IF_ERROR(parse_statistics(cursor, &column));

    return Status::ok();
}

// ─── FILE footer ────────────────────────────────────────────────────────────

Status parse_schema(Cursor& cursor, ParsedSchema* out, int depth) {
    if (depth > 32)
        return fail(Code::kMalformed,
                    "schema nesting exceeds 32 levels; refusing to recurse further");

    if (!cursor.take(&out->head, sizeof(SchemaEntryHead)))
        return fail(Code::kTruncated, "file footer ends inside a schema entry");

    const uint8_t* name = cursor.raw(out->head.name_bytes);
    if (name == nullptr)
        return fail(Code::kTruncated,
                    "schema entry name claims %u bytes but only %zu remain in "
                    "the file footer", out->head.name_bytes, cursor.remaining());
    out->name.assign(reinterpret_cast<const char*>(name), out->head.name_bytes);

    if (out->head.logical_present) {
        if (!cursor.take(&out->logical, sizeof(LogicalTypeDescriptor)))
            return fail(Code::kTruncated,
                        "schema entry '%s' declares a logical type descriptor "
                        "but the file footer ends before it", out->name.c_str());
    }

    const bool is_array = out->head.type == static_cast<uint32_t>(DRAKEN_ARRAY);
    if (is_array && out->head.child_count != 1u)
        return fail(Code::kMalformed,
                    "schema entry '%s' is ARRAY with child_count %u; exactly one "
                    "child is required", out->name.c_str(), out->head.child_count);
    if (!is_array && out->head.child_count != 0u)
        return fail(Code::kMalformed,
                    "schema entry '%s' has child_count %u but type %u is not "
                    "ARRAY", out->name.c_str(), out->head.child_count,
                    out->head.type);

    out->children.resize(out->head.child_count);
    for (uint32_t i = 0; i < out->head.child_count; ++i)
        SKENE_RETURN_IF_ERROR(parse_schema(cursor, &out->children[i], depth + 1));
    return Status::ok();
}

uint32_t count_schema_columns(const ParsedSchema& node) {
    uint32_t total = 1;
    for (const ParsedSchema& child : node.children) total += count_schema_columns(child);
    return total;
}

// Validates one row group directory entry against the object it claims to live
// in. Every field here is an OFFSET a later read follows, so each is checked
// against the file footer's own start — the one bound this reader already knows
// is real, because the tail put it there and the footer checksum covered it.
Status validate_row_group_entry(const RowGroupEntry& entry, uint32_t index,
                                uint64_t file_footer_offset, uint64_t expected_first_row) {
    if (entry.reserved != 0)
        return fail(Code::kMalformed,
                    "row group %u: reserved bytes are %u, not 0", index,
                    entry.reserved);

    if (entry.first_row != expected_first_row)
        return fail(Code::kMalformed,
                    "row group %u declares first_row %llu but the row groups "
                    "before it hold %llu rows", index,
                    static_cast<unsigned long long>(entry.first_row),
                    static_cast<unsigned long long>(expected_first_row));

    if (entry.data_offset < kFileHeadBytes)
        return fail(Code::kMalformed,
                    "row group %u: data region starts at %llu, inside the "
                    "%zu-byte head", index,
                    static_cast<unsigned long long>(entry.data_offset),
                    kFileHeadBytes);

    // Written so an overflowing sum cannot wrap past the comparison.
    if (entry.data_bytes > file_footer_offset
            || entry.data_offset > file_footer_offset - entry.data_bytes)
        return fail(Code::kMalformed,
                    "row group %u: data region spans [%llu, %llu) which runs "
                    "past the file footer at %llu", index,
                    static_cast<unsigned long long>(entry.data_offset),
                    static_cast<unsigned long long>(entry.data_offset + entry.data_bytes),
                    static_cast<unsigned long long>(file_footer_offset));

    if (entry.footer_bytes == 0)
        return fail(Code::kMalformed,
                    "row group %u declares a zero-byte footer, which cannot "
                    "hold even its header", index);

    if (entry.footer_offset < entry.data_offset + entry.data_bytes)
        return fail(Code::kMalformed,
                    "row group %u: its footer at %llu overlaps its own data "
                    "region, which ends at %llu", index,
                    static_cast<unsigned long long>(entry.footer_offset),
                    static_cast<unsigned long long>(entry.data_offset + entry.data_bytes));

    if (entry.footer_bytes > file_footer_offset
            || entry.footer_offset > file_footer_offset - entry.footer_bytes)
        return fail(Code::kMalformed,
                    "row group %u: its footer spans [%llu, %llu) which runs past "
                    "the file footer at %llu", index,
                    static_cast<unsigned long long>(entry.footer_offset),
                    static_cast<unsigned long long>(entry.footer_offset)
                        + entry.footer_bytes,
                    static_cast<unsigned long long>(file_footer_offset));

    return Status::ok();
}

Status parse_file_footer(const uint8_t* footer, uint32_t footer_bytes,
                         uint64_t file_footer_offset, ParsedFileFooter* out) {
    Cursor cursor(footer, footer_bytes);

    if (!cursor.take(&out->header, sizeof(FileFooterHeader)))
        return fail(Code::kTruncated,
                    "file footer is too small to hold its header");

    // The guard that separates this layout from the single-row-group v1 files
    // written before it. Those are framed identically and their footer checksum
    // verifies, so nothing else in the file distinguishes them — parsing one as
    // a file index would read a row count as a magic and a writer tag as a row
    // group directory.
    if (out->header.footer_magic != kFileFooterMagic)
        return fail(Code::kMalformed,
                    "file footer magic is 0x%08X, not 0x%08X. This is almost "
                    "certainly a .skene file written before row groups were "
                    "packed into files, when one file WAS one row group; v1 was "
                    "draft and its layout changed. Regenerate the file with the "
                    "current writer.",
                    out->header.footer_magic, kFileFooterMagic);

    if (out->header.footer_version != kFileFooterVersion)
        return fail(Code::kUnsupportedVersion,
                    "file footer declares layout version %u; the v2 reader "
                    "implements %u", out->header.footer_version,
                    kFileFooterVersion);

    if (out->header.reserved != 0)
        return fail(Code::kMalformed,
                    "file footer header reserved bytes are %u, not 0",
                    out->header.reserved);

    const uint8_t* tag = cursor.raw(out->header.writer_tag_bytes);
    if (tag == nullptr)
        return fail(Code::kTruncated,
                    "file writer tag claims %u bytes but only %zu remain in the "
                    "file footer", out->header.writer_tag_bytes, cursor.remaining());
    out->writer_tag.assign(reinterpret_cast<const char*>(tag),
                           out->header.writer_tag_bytes);

    if (out->header.row_group_count == 0)
        return fail(Code::kMalformed,
                    "file declares 0 row groups; a .skene file with no row "
                    "groups describes no data");

    if (static_cast<uint64_t>(out->header.row_group_count) * sizeof(RowGroupEntry)
            > cursor.remaining())
        return fail(Code::kMalformed,
                    "file claims %u row groups, which cannot fit in its "
                    "remaining %zu footer bytes",
                    out->header.row_group_count, cursor.remaining());

    out->row_groups.resize(out->header.row_group_count);
    uint64_t running_rows = 0;
    for (uint32_t i = 0; i < out->header.row_group_count; ++i) {
        if (!cursor.take(&out->row_groups[i], sizeof(RowGroupEntry)))
            return fail(Code::kTruncated,
                        "file footer ends inside the row group directory");
        SKENE_RETURN_IF_ERROR(validate_row_group_entry(
            out->row_groups[i], i, file_footer_offset, running_rows));
        running_rows += out->row_groups[i].row_count;
    }

    if (running_rows != out->header.row_count)
        return fail(Code::kMalformed,
                    "file declares %llu rows but its row groups hold %llu",
                    static_cast<unsigned long long>(out->header.row_count),
                    static_cast<unsigned long long>(running_rows));

    if (static_cast<uint64_t>(out->header.column_count) * sizeof(SchemaEntryHead)
            > cursor.remaining())
        return fail(Code::kMalformed,
                    "file claims %u columns, which cannot fit in its remaining "
                    "%zu footer bytes", out->header.column_count,
                    cursor.remaining());

    out->schema.resize(out->header.column_count);
    for (uint32_t i = 0; i < out->header.column_count; ++i)
        SKENE_RETURN_IF_ERROR(parse_schema(cursor, &out->schema[i], 0));

    uint32_t flat_columns = 0;
    for (const ParsedSchema& node : out->schema) flat_columns += count_schema_columns(node);

    // ── Cluster spec (v2) ── a PROMISE consumers act on, so it is validated
    // structurally here: ordinals inside the schema, reserved bytes zero, the
    // null rule consistent. Whether the rows genuinely have this order was the
    // writer's obligation; a reader can only check that the record is coherent.
    {
        ClusterSpecHeader spec{};
        if (!cursor.take(&spec, sizeof(spec)))
            return fail(Code::kTruncated,
                        "file footer ends inside the cluster spec header");
        if (spec.reserved != 0)
            return fail(Code::kMalformed,
                        "cluster spec reserved bytes are %u, not 0", spec.reserved);
        if (static_cast<uint64_t>(spec.key_count) * sizeof(SortKey)
                > cursor.remaining())
            return fail(Code::kMalformed,
                        "cluster spec claims %u keys, which cannot fit in the "
                        "remaining %zu footer bytes", spec.key_count,
                        cursor.remaining());
        out->cluster_keys.resize(spec.key_count);
        for (uint16_t k = 0; k < spec.key_count; ++k) {
            SortKey& key = out->cluster_keys[k];
            if (!cursor.take(&key, sizeof(SortKey)))
                return fail(Code::kTruncated,
                            "file footer ends inside the cluster spec keys");
            if (key.reserved != 0)
                return fail(Code::kMalformed,
                            "cluster key %u: reserved bytes are %u, not 0", k,
                            key.reserved);
            if (key.column_ordinal >= out->header.column_count)
                return fail(Code::kMalformed,
                            "cluster key %u names column ordinal %u but the "
                            "schema has %u top-level columns", k,
                            key.column_ordinal, out->header.column_count);
            const bool expected_nulls_first = key.descending == 0;
            if ((key.nulls_first != 0) != expected_nulls_first)
                return fail(Code::kMalformed,
                            "cluster key %u: nulls_first=%u with descending=%u "
                            "violates draken's sort rule", k, key.nulls_first,
                            key.descending);
        }
    }

    // Per-row-group statistics, row group major, in the schema's depth-first
    // order. Each blob is length-prefixed, so a blob longer than this build
    // understands is read prefix-first and the rest skipped — the same growth
    // rule the row group footers' blobs follow.
    out->statistics.resize(out->header.row_group_count);
    for (uint32_t g = 0; g < out->header.row_group_count; ++g) {
        out->statistics[g].resize(flat_columns);
        for (uint32_t c = 0; c < flat_columns; ++c) {
            uint32_t declared = 0;
            if (!cursor.take(&declared, sizeof(declared)))
                return fail(Code::kTruncated,
                            "file footer ends inside row group %u's statistics", g);
            if (declared == 0) continue;
            const uint8_t* blob = cursor.raw(declared);
            if (blob == nullptr)
                return fail(Code::kTruncated,
                            "row group %u column %u declares %u statistics bytes "
                            "but only %zu remain in the file footer", g, c,
                            declared, cursor.remaining());
            const size_t known = declared < sizeof(ColumnStatistics)
                               ? declared : sizeof(ColumnStatistics);
            std::memcpy(&out->statistics[g][c].statistics, blob, known);
            out->statistics[g][c].present = true;
        }
    }

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
    uint64_t       encoded_bytes = 0;   // post-codec, pre-encoding (v2)
    uint64_t       plain_bytes = 0;
    Encoding       encoding = Encoding::kPlain;
    SectionCodec   codec = SectionCodec::kNone;
};

class SectionResolver {
  public:
    // The region is ONE ROW GROUP'S data+index extent, not the whole file's.
    // A section entry in row group 3 that addresses row group 0's bytes is
    // corruption, and bounding on the file would accept it — the checksum would
    // then pass, because it is computed over whatever bytes the offset names.
    SectionResolver(const uint8_t* file, uint64_t region_begin, uint64_t region_end,
                    const std::vector<SectionEntry>& sections)
        : file_(file), region_begin_(region_begin), region_end_(region_end),
          sections_(sections) {}

    // Finds the single section of `kind` within a column's slice. An absent
    // section is legal for several kinds (no validity means all-valid; no arena
    // means no long payloads), so absence is reported, not an error.
    Status find(const ColumnEntryHead& head, const char* column_name,
                SectionKind kind, SectionRef* out) const {
        *out = SectionRef();

        SKENE_RETURN_IF_ERROR(check_slice(head.section_index, head.section_count,
                                          "", column_name));

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
        SKENE_RETURN_IF_ERROR(check_slice(head.index_section_index,
                                          head.index_section_count,
                                          "index ", column_name));

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
        SKENE_RETURN_IF_ERROR(check_slice(head.section_index, head.section_count,
                                          "", column_name));

        for (uint32_t i = 0; i < head.section_count; ++i) {
            const SectionEntry& entry = sections_[head.section_index + i];
            if (!section_is_required(entry.kind)) continue;  // skippable
            switch (static_cast<SectionKind>(entry.kind)) {
                case SectionKind::kData:
                case SectionKind::kSelection:
                case SectionKind::kValidity:
                case SectionKind::kStringArena:
                case SectionKind::kSlotLane0:
                case SectionKind::kSlotLane1:
                case SectionKind::kSlotLane2:
                case SectionKind::kSlotLane3:
                    break;
                case SectionKind::kStringSlots:
                    // The v1 slot layout. A v2 writer stores lanes; a v2 file
                    // carrying the v1 kind was assembled by nothing this format
                    // ever shipped.
                    return fail(Code::kMalformed,
                                "column '%s' carries the v1 kStringSlots section "
                                "in a v2 file; v2 stores slot lanes",
                                column_name);
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

    // The data-slice bounds on their own, for the one caller that walks the
    // directory itself instead of going through find().
    Status check_data_slice(const ColumnEntryHead& head,
                            const char* column_name) const {
        return check_slice(head.section_index, head.section_count, "", column_name);
    }

  private:
    // A column's section slice is two footer fields, so it is exactly as
    // trustworthy as the rest of the footer: nothing stops a crafted head from
    // naming a slice that runs off the end of the section directory. The
    // footer checksum does not help — whoever writes the bytes computes the
    // checksum over them too.
    //
    // Indexing the directory on an unvalidated slice reads whatever follows the
    // vector, which is memory corruption rather than a wrong answer (status.h),
    // so EVERY walk of a slice validates it here first. One home for the rule,
    // because the one accessor that restated it independently is the one that
    // forgot it.
    //
    // `slice_name` is "" for the data slice and "index " for the index slice.
    Status check_slice(uint32_t index, uint32_t count, const char* slice_name,
                       const char* column_name) const {
        const uint64_t end = static_cast<uint64_t>(index) + count;
        if (end > sections_.size())
            return fail(Code::kMalformed,
                        "column '%s' references %ssections [%u, %llu) but only "
                        "%zu exist", column_name, slice_name, index,
                        static_cast<unsigned long long>(end), sections_.size());
        return Status::ok();
    }

    Status resolve(const SectionEntry& entry, const char* column_name,
                   SectionRef* out) const {
        if (entry.reserved != 0)
            return fail(Code::kMalformed,
                        "column '%s': section kind %u has reserved bytes %u, "
                        "not 0", column_name, entry.kind, entry.reserved);

        switch (static_cast<Encoding>(entry.encoding)) {
            case Encoding::kPlain:
                if (entry.encoded_bytes != entry.plain_bytes)
                    return fail(Code::kMalformed,
                                "column '%s': section kind %u is PLAIN but declares "
                                "%llu encoded bytes and %llu plain bytes",
                                column_name, entry.kind,
                                static_cast<unsigned long long>(entry.encoded_bytes),
                                static_cast<unsigned long long>(entry.plain_bytes));
                break;
            case Encoding::kBitpack:
            case Encoding::kDeltaBitpack:
                // The writer stores an encoded body only when it came out
                // SMALLER than plain, so a declared encoded size above plain is
                // a contradiction — and rejecting it here also bounds the
                // stacked-decode scratch by plain_bytes, which every consumer
                // validates against the column's declared shape.
                if (entry.encoded_bytes > entry.plain_bytes)
                    return fail(Code::kMalformed,
                                "column '%s': section kind %u declares %llu "
                                "encoded bytes, more than its %llu plain bytes",
                                column_name, entry.kind,
                                static_cast<unsigned long long>(entry.encoded_bytes),
                                static_cast<unsigned long long>(entry.plain_bytes));
                break;
            case Encoding::kZstd:
            case Encoding::kLz4:
                // The v1 spellings. v2 stores the codec in its own field, and
                // one fact gets one spelling — a v2 writer can never have
                // produced this, so it is corruption, not compatibility.
                return fail(Code::kMalformed,
                            "column '%s': section kind %u uses v1 codec-as-"
                            "encoding value %u in a v2 file",
                            column_name, entry.kind, entry.encoding);
            default:
                // A required section this build cannot decode is fatal. Adding an
                // encoding for a required section is therefore a version bump —
                // an older reader must never guess at a body it cannot read.
                return fail(Code::kUnsupportedEncoding,
                            "column '%s': section kind %u uses encoding %u, which "
                            "this build does not implement",
                            column_name, entry.kind, entry.encoding);
        }

        switch (static_cast<SectionCodec>(entry.codec)) {
            case SectionCodec::kNone:
                if (entry.stored_bytes != entry.encoded_bytes)
                    return fail(Code::kMalformed,
                                "column '%s': section kind %u has no codec but "
                                "declares %llu stored bytes and %llu encoded bytes",
                                column_name, entry.kind,
                                static_cast<unsigned long long>(entry.stored_bytes),
                                static_cast<unsigned long long>(entry.encoded_bytes));
                break;
            case SectionCodec::kZstd:
            case SectionCodec::kLz4:
                break;
            default:
                return fail(Code::kUnsupportedEncoding,
                            "column '%s': section kind %u uses codec %u, which "
                            "this build does not implement",
                            column_name, entry.kind, entry.codec);
        }

        if (entry.offset < region_begin_
                || entry.stored_bytes > region_end_
                || entry.offset > region_end_ - entry.stored_bytes)
            return fail(Code::kMalformed,
                        "column '%s': section kind %u spans [%llu, %llu) which "
                        "is outside this row group's region [%llu, %llu)",
                        column_name, entry.kind,
                        static_cast<unsigned long long>(entry.offset),
                        static_cast<unsigned long long>(entry.offset + entry.stored_bytes),
                        static_cast<unsigned long long>(region_begin_),
                        static_cast<unsigned long long>(region_end_));

        const uint8_t* data = file_ + entry.offset;
        const uint64_t actual = checksum_xxh3_64(data, entry.stored_bytes);
        if (actual != entry.checksum && checksum_must_match())
            return fail(Code::kChecksumMismatch,
                        "column '%s': section kind %u fails its checksum "
                        "(recorded %llu, computed %llu)",
                        column_name, entry.kind,
                        static_cast<unsigned long long>(entry.checksum),
                        static_cast<unsigned long long>(actual));

        out->present       = true;
        out->stored        = data;
        out->stored_bytes  = entry.stored_bytes;
        out->encoded_bytes = entry.encoded_bytes;
        out->plain_bytes   = entry.plain_bytes;
        out->encoding      = static_cast<Encoding>(entry.encoding);
        out->codec         = static_cast<SectionCodec>(entry.codec);
        return Status::ok();
    }

    const uint8_t*                   file_;
    uint64_t                         region_begin_;
    uint64_t                         region_end_;
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
// v2 decode is TWO stages in reverse of the writer: codec first (zstd/lz4 over
// the stored bytes, producing encoded_bytes), then encoding (bitpack/delta/
// plain, producing plain_bytes). The scratch buffer between them exists ONLY
// when both stages are real — a plain body codec-decodes straight into the
// final draken buffer, and an uncodec'd body encoding-decodes straight from
// the mapping, so the pre-v2 zero-scratch paths are unchanged.
Status decode_encoded(Encoding encoding, const uint8_t* body, uint64_t body_bytes,
                      uint64_t plain_bytes, const char* column_name,
                      uint32_t count, size_t item_bytes, uint8_t* destination) {
    switch (encoding) {
        case Encoding::kPlain:
            std::memcpy(destination, body, static_cast<size_t>(plain_bytes));
            return Status::ok();
        case Encoding::kBitpack:
            return bitpack_decode_codes(body, body_bytes, count,
                                        reinterpret_cast<uint32_t*>(destination));
        case Encoding::kDeltaBitpack:
            return delta_bitpack_decode(body, body_bytes, count,
                                        item_bytes, destination);
        default:
            // resolve() rejected everything else already; reaching this is a
            // reader bug, not a file property.
            return fail(Code::kUnsupportedEncoding,
                        "column '%s': unhandled encoding %u", column_name,
                        static_cast<unsigned>(encoding));
    }
}

Status decode_into(const SectionRef& section, const char* column_name,
                   uint32_t count, size_t item_bytes, uint8_t* destination) {
    switch (section.codec) {
        case SectionCodec::kNone:
            return decode_encoded(section.encoding, section.stored,
                                  section.stored_bytes, section.plain_bytes,
                                  column_name, count, item_bytes, destination);
        case SectionCodec::kZstd:
        case SectionCodec::kLz4: {
            const bool is_zstd = section.codec == SectionCodec::kZstd;
            if (section.encoding == Encoding::kPlain) {
                // encoded == plain (resolve() checked), so the codec output IS
                // the final bytes: decode straight into the draken buffer.
                return is_zstd
                    ? zstd_decode(section.stored, section.stored_bytes,
                                  section.plain_bytes, destination)
                    : lz4_decode(section.stored, section.stored_bytes,
                                 section.plain_bytes, destination);
            }
            // Stacked: codec into scratch sized to the directory's
            // encoded_bytes — the codec's EXACT capacity contract, same as
            // plain_bytes was in v1 (lz4's wildcopy makes over-declaring a
            // buffer overrun, see encoding.h) — then the encoding stage.
            std::vector<uint8_t> scratch(
                static_cast<size_t>(section.encoded_bytes));
            Status st = is_zstd
                ? zstd_decode(section.stored, section.stored_bytes,
                              section.encoded_bytes, scratch.data())
                : lz4_decode(section.stored, section.stored_bytes,
                             section.encoded_bytes, scratch.data());
            if (!st.is_ok()) return st;
            return decode_encoded(section.encoding, scratch.data(),
                                  section.encoded_bytes, section.plain_bytes,
                                  column_name, count, item_bytes, destination);
        }
    }
    return fail(Code::kUnsupportedEncoding,
                "column '%s': unhandled codec %u", column_name,
                static_cast<unsigned>(section.codec));
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

    // v2: the slot array arrives as four u32 lanes and is fused back into
    // 16-byte slots — a 4-way interleave that runs at memcpy speed. All four
    // lanes are REQUIRED for a string column and each must decode to exactly
    // slot_count u32s; a missing or short lane cannot be padded, because every
    // word of a slot is load-bearing (a guessed arena_offset is an OOB read).
    if (head.string_slot_count > UINT32_MAX)
        return fail(Code::kMalformed,
                    "column '%s': %llu slots exceed the 32-bit code space that "
                    "addresses them", name,
                    static_cast<unsigned long long>(head.string_slot_count));

    const uint64_t expect_lane =
        head.string_slot_count * sizeof(uint32_t);
    const SectionKind lane_kinds[4] = {
        SectionKind::kSlotLane0, SectionKind::kSlotLane1,
        SectionKind::kSlotLane2, SectionKind::kSlotLane3};
    std::vector<uint32_t> lanes[4];
    for (int k = 0; k < 4; ++k) {
        SectionRef lane_section;
        SKENE_RETURN_IF_ERROR(ctx.resolver->find(head, name, lane_kinds[k],
                                                 &lane_section));
        if (!lane_section.present)
            return fail(Code::kMalformed,
                        "column '%s' is string-typed but has no slot lane %d "
                        "section", name, k);
        if (lane_section.plain_bytes != expect_lane)
            return fail(Code::kMalformed,
                        "column '%s': slot lane %d decodes to %llu bytes but "
                        "%llu slots require %llu", name, k,
                        static_cast<unsigned long long>(lane_section.plain_bytes),
                        static_cast<unsigned long long>(head.string_slot_count),
                        static_cast<unsigned long long>(expect_lane));
        lanes[k].resize(static_cast<size_t>(head.string_slot_count));
        if (head.string_slot_count > 0)
            SKENE_RETURN_IF_ERROR(decode_into(
                lane_section, name,
                static_cast<uint32_t>(head.string_slot_count), sizeof(uint32_t),
                reinterpret_cast<uint8_t*>(lanes[k].data())));
    }

    const uint64_t expect_slots =
        head.string_slot_count * sizeof(DrakenStringSlot);
    std::vector<uint8_t> slot_storage(static_cast<size_t>(expect_slots));
    {
        uint32_t* words = reinterpret_cast<uint32_t*>(slot_storage.data());
        for (uint64_t i = 0; i < head.string_slot_count; ++i) {
            words[i * 4 + 0] = lanes[0][i];
            words[i * 4 + 1] = lanes[1][i];
            words[i * 4 + 2] = lanes[2][i];
            words[i * 4 + 3] = lanes[3][i];
        }
    }
    const uint8_t* slot_bytes = slot_storage.data();

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
    SKENE_RETURN_IF_ERROR(resolver.check_data_slice(head, parsed.name.c_str()));
    for (uint32_t i = 0; i < head.section_count; ++i) {
        const SectionEntry& entry = sections[head.section_index + i];
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

void fill_schema(const ParsedSchema& parsed, ColumnSchema* out) {
    out->name            = parsed.name;
    out->field_id        = parsed.head.field_id;
    out->type            = parsed.head.type;
    out->logical_present = parsed.head.logical_present != 0;
    out->logical         = parsed.logical;
    out->children.resize(parsed.children.size());
    for (size_t i = 0; i < parsed.children.size(); ++i)
        fill_schema(parsed.children[i], &out->children[i]);
}

// Parses the file footer, picks out one row group, and parses ITS footer —
// verifying that footer's checksum against the file footer's record of it first.
//
// The two-step is the point of the layout: the file footer is small and is the
// only thing a pruning reader fetches, and a row group's directory is opened
// only once that reader has decided to read it.
Status open_row_group(const uint8_t* file, uint64_t file_footer_offset,
                      uint32_t file_footer_bytes, uint32_t row_group,
                      ParsedFileFooter* file_footer,
                      ParsedRowGroupFooter* out, RowGroupEntry* out_entry) {
    SKENE_RETURN_IF_ERROR(parse_file_footer(file + file_footer_offset,
                                            file_footer_bytes, file_footer_offset,
                                            file_footer));

    if (row_group >= file_footer->row_groups.size())
        return fail(Code::kMalformed,
                    "row group %u was requested but this file has %zu",
                    row_group, file_footer->row_groups.size());

    const RowGroupEntry& entry = file_footer->row_groups[row_group];

    // The row group footer's checksum lives in the file footer, which has
    // already been checksum-verified as a whole — so this is a check against
    // something already trusted, not against a number sitting beside the bytes
    // it claims to cover.
    const uint64_t actual = checksum_xxh3_64(file + entry.footer_offset,
                                             entry.footer_bytes);
    if (actual != entry.footer_checksum && checksum_must_match())
        return fail(Code::kChecksumMismatch,
                    "row group %u footer checksum mismatch: recorded %llu, "
                    "computed %llu — its directory is corrupt and every offset "
                    "in it is suspect", row_group,
                    static_cast<unsigned long long>(entry.footer_checksum),
                    static_cast<unsigned long long>(actual));

    SKENE_RETURN_IF_ERROR(parse_row_group_footer(file + entry.footer_offset,
                                                 entry.footer_bytes, row_group, out));

    if (out->header.row_count != entry.row_count)
        return fail(Code::kMalformed,
                    "row group %u's footer declares %llu rows but the file's row "
                    "group directory says %llu", row_group,
                    static_cast<unsigned long long>(out->header.row_count),
                    static_cast<unsigned long long>(entry.row_count));

    *out_entry = entry;
    return Status::ok();
}

}  // namespace

Status read_metadata(const uint8_t* file, size_t file_bytes,
                     uint64_t footer_offset, uint32_t footer_bytes,
                     FileMetadata* out) {
    (void)file_bytes;
    ParsedFileFooter footer;
    SKENE_RETURN_IF_ERROR(parse_file_footer(file + footer_offset, footer_bytes,
                                            footer_offset, &footer));

    out->version            = 2u;
    out->row_count          = footer.header.row_count;
    out->created_at_unix_us = footer.header.created_at_unix_us;
    out->writer_tag         = footer.writer_tag;
    out->cluster_keys       = std::move(footer.cluster_keys);
    std::memcpy(out->file_uuid, footer.header.file_uuid, sizeof(out->file_uuid));

    out->columns.resize(footer.schema.size());
    for (size_t i = 0; i < footer.schema.size(); ++i)
        fill_schema(footer.schema[i], &out->columns[i]);

    out->row_groups.resize(footer.row_groups.size());
    for (size_t i = 0; i < footer.row_groups.size(); ++i) {
        const RowGroupEntry& entry = footer.row_groups[i];
        RowGroupSummary& summary = out->row_groups[i];
        summary.row_count         = entry.row_count;
        summary.first_row         = entry.first_row;
        summary.byte_offset       = entry.data_offset;
        summary.byte_bytes        = entry.data_bytes;
        summary.footer_offset     = entry.footer_offset;
        summary.footer_bytes      = entry.footer_bytes;
        summary.column_statistics = std::move(footer.statistics[i]);
    }

    return Status::ok();
}

Status read_row_group_metadata(const uint8_t* file, size_t file_bytes,
                               uint64_t footer_offset, uint32_t footer_bytes,
                               uint32_t row_group, RowGroupMetadata* out) {
    (void)file_bytes;
    ParsedFileFooter file_footer;
    ParsedRowGroupFooter footer;
    RowGroupEntry entry{};
    SKENE_RETURN_IF_ERROR(open_row_group(file, footer_offset, footer_bytes, row_group,
                                         &file_footer, &footer, &entry));

    SectionResolver resolver(file, entry.data_offset,
                             entry.data_offset + entry.data_bytes, footer.sections);

    out->row_count = footer.header.row_count;
    out->columns.resize(footer.columns.size());
    for (size_t i = 0; i < footer.columns.size(); ++i)
        SKENE_RETURN_IF_ERROR(fill_metadata(footer.columns[i], footer.sections,
                                            resolver, &out->columns[i]));

    return Status::ok();
}

Status read_morsel(const uint8_t* file, size_t file_bytes,
                   uint64_t footer_offset, uint32_t footer_bytes,
                   uint32_t row_group, const ReadOptions& options, CxxMorsel* out) {
    (void)file_bytes;
    ParsedFileFooter file_footer;
    ParsedRowGroupFooter footer;
    RowGroupEntry entry{};
    SKENE_RETURN_IF_ERROR(open_row_group(file, footer_offset, footer_bytes, row_group,
                                         &file_footer, &footer, &entry));

    SectionResolver resolver(file, entry.data_offset,
                             entry.data_offset + entry.data_bytes, footer.sections);
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
        if (built.view.length != footer.header.row_count)
            return fail(Code::kMalformed,
                        "column '%s' has %u rows but the file declares %llu",
                        column->name.c_str(), built.view.length,
                        static_cast<unsigned long long>(footer.header.row_count));
        morsel.columns.push_back(std::move(built));
        morsel.names.push_back(column->name);
    }

    // A zero-column morsel still has a row count, and it lives nowhere else.
    if (morsel.columns.empty())
        morsel.zero_col_rows =
            static_cast<uint32_t>(footer.header.row_count);

    *out = std::move(morsel);
    return Status::ok();
}

}  // namespace v2
}  // namespace skene
