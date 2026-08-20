// skene/src/patch.cpp — see include/skene/patch.h.
//
// The shape of the job:
//
//   HEAD          copied verbatim
//   per row group DATA sections of surviving columns, copied verbatim
//                 INDEX sections of surviving columns, copied verbatim
//                 FOOTER rebuilt — the section directory's offsets moved
//   FILE FOOTER   rebuilt — schema, row group directory, per-row-group stats
//   TAIL          rebuilt — new footer length and checksum
//
// Every structure here is re-emitted with the same field order the writer uses
// (src/writer.cpp), because a patched file has to be indistinguishable from one
// written that way. Where this file and the writer disagree, the writer is
// right and this is a bug.

#include "skene/patch.h"

#include <cstring>

#include "core/buffers.h"  // DRAKEN_SEL_* / DRAKEN_DICT_* layout-hint flags
#include "skene/checksum.h"
#include "skene/format.h"

namespace skene {
namespace {

Status fail(Code code, std::string message) { return Status(code, std::move(message)); }

// ─── reading ────────────────────────────────────────────────────────────────

// A bounds-checked cursor over the source. Every read is checked: this parses a
// footer in order to REWRITE it, so a read past the end would be copied into
// the output as if it were content.
class Cursor {
  public:
    Cursor(const uint8_t* base, size_t begin, size_t end)
        : base_(base), at_(begin), end_(end) {}

    bool take(void* dst, size_t bytes) {
        if (bytes > end_ - at_) return false;
        std::memcpy(dst, base_ + at_, bytes);
        at_ += bytes;
        return true;
    }
    bool take_string(std::string* dst, size_t bytes) {
        if (bytes > end_ - at_) return false;
        dst->assign(reinterpret_cast<const char*>(base_ + at_), bytes);
        at_ += bytes;
        return true;
    }
    template <typename T>
    bool pod(T* dst) { return take(dst, sizeof(T)); }

    size_t at() const { return at_; }
    size_t remaining() const { return end_ - at_; }

  private:
    const uint8_t* base_;
    size_t at_;
    size_t end_;
};

// ─── writing ────────────────────────────────────────────────────────────────

class Sink {
  public:
    explicit Sink(std::vector<uint8_t>* out) : out_(out) {}

    uint64_t position() const { return out_->size(); }
    void bytes(const void* src, size_t n) {
        const uint8_t* p = static_cast<const uint8_t*>(src);
        out_->insert(out_->end(), p, p + n);
    }
    // v2 sections start kSectionAlign-aligned; a patched file keeps that
    // property, since it must be indistinguishable from a written one.
    void align_section() {
        const uint64_t misaligned = out_->size() % kSectionAlign;
        if (misaligned != 0)
            out_->insert(out_->end(),
                         static_cast<size_t>(kSectionAlign - misaligned),
                         uint8_t{0});
    }
    template <typename T>
    void pod(const T& value) { bytes(&value, sizeof(T)); }
    void u32(uint32_t value) { pod(value); }

  private:
    std::vector<uint8_t>* out_;
};

// ─── parsed column tree ─────────────────────────────────────────────────────

// One column of one row group, as read out of that row group's footer. The
// section entries are held by VALUE rather than by index because the whole
// point is to re-emit them at different offsets.
struct Column {
    ColumnEntryHead        head{};
    std::string            name;
    LogicalTypeDescriptor  logical{};
    std::vector<SectionEntry> required;   // this column's own, in order
    std::vector<SectionEntry> optional;   // ditto, from the INDEX region
    std::vector<uint8_t>   statistics;    // verbatim blob; empty == not tracked
    std::vector<Column>    children;
};

// The invariant half of a column, from the FILE footer's schema directory.
struct SchemaNode {
    SchemaEntryHead       head{};
    std::string           name;
    LogicalTypeDescriptor logical{};
    std::vector<SchemaNode> children;
};

Status read_schema_node(Cursor& c, SchemaNode* node) {
    if (!c.pod(&node->head))
        return fail(Code::kTruncated, "file footer ends inside a schema entry");
    if (!c.take_string(&node->name, node->head.name_bytes))
        return fail(Code::kTruncated, "file footer ends inside a schema entry's name");
    if (node->head.logical_present && !c.pod(&node->logical))
        return fail(Code::kTruncated, "file footer ends inside a logical type descriptor");
    node->children.resize(node->head.child_count);
    for (SchemaNode& child : node->children) {
        Status s = read_schema_node(c, &child);
        if (!s.is_ok()) return s;
    }
    return Status::ok();
}

Status read_column_head(Cursor& c, Column* column) {
    if (!c.pod(&column->head))
        return fail(Code::kTruncated, "row group footer ends inside a column entry");
    if (!c.take_string(&column->name, column->head.name_bytes))
        return fail(Code::kTruncated, "row group footer ends inside a column name");
    if (column->head.logical_present && !c.pod(&column->logical))
        return fail(Code::kTruncated, "row group footer ends inside a logical type descriptor");
    column->children.resize(column->head.child_count);
    for (Column& child : column->children) {
        Status s = read_column_head(c, &child);
        if (!s.is_ok()) return s;
    }
    return Status::ok();
}

// Attach each column's section entries, by the index range its head records.
Status attach_sections(Column* column, const std::vector<SectionEntry>& all) {
    const uint64_t req_end =
        static_cast<uint64_t>(column->head.section_index) + column->head.section_count;
    const uint64_t idx_end =
        static_cast<uint64_t>(column->head.index_section_index) + column->head.index_section_count;
    if (req_end > all.size() || idx_end > all.size())
        return fail(Code::kMalformed,
                    "column '" + column->name + "' names section entries past the directory");

    column->required.assign(all.begin() + column->head.section_index,
                            all.begin() + static_cast<size_t>(req_end));
    column->optional.assign(all.begin() + column->head.index_section_index,
                            all.begin() + static_cast<size_t>(idx_end));
    for (Column& child : column->children) {
        Status s = attach_sections(&child, all);
        if (!s.is_ok()) return s;
    }
    return Status::ok();
}

// Statistics blobs appear depth-first, skipping columns whose stats_bytes is 0.
Status attach_statistics(Column* column, Cursor& c) {
    if (column->head.stats_bytes > 0) {
        column->statistics.resize(column->head.stats_bytes);
        if (!c.take(column->statistics.data(), column->head.stats_bytes))
            return fail(Code::kTruncated,
                        "row group footer ends inside column '" + column->name +
                        "' statistics");
    }
    for (Column& child : column->children) {
        Status s = attach_statistics(&child, c);
        if (!s.is_ok()) return s;
    }
    return Status::ok();
}

// ─── emitting ───────────────────────────────────────────────────────────────

// Copy one column subtree's sections of one kind, recording where they landed.
// Required and optional are copied in separate passes so the DATA and INDEX
// regions stay separate — an index scattered through the data region is an
// index you have to read the data to reach (FORMAT.md §3).
void copy_sections(Sink& sink, const uint8_t* src, Column* column, bool optional,
                   std::vector<SectionEntry>* directory) {
    std::vector<SectionEntry>& entries = optional ? column->optional : column->required;
    const uint32_t first = static_cast<uint32_t>(directory->size());
    for (SectionEntry entry : entries) {
        sink.align_section();
        const uint64_t at = sink.position();
        sink.bytes(src + entry.offset, static_cast<size_t>(entry.stored_bytes));
        entry.offset = at;  // checksum is over the STORED bytes, which did not change
        directory->push_back(entry);
    }
    if (optional) {
        column->head.index_section_index = first;
        column->head.index_section_count = static_cast<uint32_t>(directory->size()) - first;
    } else {
        column->head.section_index = first;
        column->head.section_count = static_cast<uint32_t>(directory->size()) - first;
    }
    for (Column& child : column->children)
        copy_sections(sink, src, &child, optional, directory);
}

void write_column_entry(Sink& sink, const Column& column) {
    sink.pod(column.head);
    sink.bytes(column.name.data(), column.name.size());
    if (column.head.logical_present) sink.pod(column.logical);
    for (const Column& child : column.children) write_column_entry(sink, child);
}

void write_statistics(Sink& sink, const Column& column) {
    if (!column.statistics.empty())
        sink.bytes(column.statistics.data(), column.statistics.size());
    for (const Column& child : column.children) write_statistics(sink, child);
}

void write_schema_entry(Sink& sink, const SchemaNode& node) {
    sink.pod(node.head);
    sink.bytes(node.name.data(), node.name.size());
    if (node.head.logical_present) sink.pod(node.logical);
    for (const SchemaNode& child : node.children) write_schema_entry(sink, child);
}

// ─── added columns ──────────────────────────────────────────────────────────

// A donor, parsed: everything needed to emit the same column at any length.
struct Donor {
    SchemaNode  schema;
    ColumnEntryHead head{};
    std::string name;
    LogicalTypeDescriptor logical{};
    // The donor's required sections, bytes lifted out. kSelection is never among
    // them (a one-row column is CONSTANT or IDENTITY, and both store none) and
    // kValidity is dropped here because its size depends on the row count — it
    // is synthesised per row group instead.
    std::vector<SectionEntry>          sections;
    std::vector<std::vector<uint8_t>>  section_bytes;
    bool null_fill = false;
};

// How many bits of a validity section are set for row 0 — i.e. whether the
// donor's single row carries a value or a NULL.
bool donor_row_is_null(const SectionEntry& validity, const uint8_t* src) {
    if (validity.stored_bytes == 0) return false;
    if (validity.encoding != static_cast<uint8_t>(Encoding::kPlain)) return false;
    if (validity.codec != static_cast<uint8_t>(SectionCodec::kNone)) return false;
    return (src[validity.offset] & 1u) == 0u;
}

// Read a donor file: one column, one row group, one row.
Status parse_donor(const DonorFile& bytes, Donor* donor) {
    const uint8_t* src = bytes.data();
    if (bytes.size() < kFileHeadBytes + kFileTailBytes)
        return fail(Code::kTruncated, "patch_columns: donor is too small to be a skene file");

    FileHead head{};
    std::memcpy(&head, src, sizeof(head));
    FileTail tail{};
    std::memcpy(&tail, src + bytes.size() - kFileTailBytes, sizeof(tail));
    if (head.magic != kMagic || tail.magic != kMagic)
        return fail(Code::kNotSkene, "patch_columns: donor is not a skene file");

    const size_t footer_end = bytes.size() - kFileTailBytes;
    if (tail.footer_bytes > footer_end - kFileHeadBytes)
        return fail(Code::kTruncated, "patch_columns: donor footer runs past its head");
    const size_t footer_start = footer_end - tail.footer_bytes;
    if (checksum_xxh3_64(src + footer_start, tail.footer_bytes) != tail.footer_checksum)
        return fail(Code::kChecksumMismatch, "patch_columns: donor footer checksum mismatch");

    Cursor fc(src, footer_start, footer_end);
    FileFooterHeader fh{};
    if (!fc.pod(&fh)) return fail(Code::kTruncated, "patch_columns: short donor footer");
    if (fh.footer_magic != kFileFooterMagic)
        return fail(Code::kMalformed, "patch_columns: donor footer magic missing");
    if (fh.column_count != 1 || fh.row_group_count != 1 || fh.row_count != 1)
        return fail(Code::kMalformed,
                    "patch_columns: a donor must hold exactly one column of one row");

    std::string writer_tag;
    if (!fc.take_string(&writer_tag, fh.writer_tag_bytes))
        return fail(Code::kTruncated, "patch_columns: donor footer ends in the writer tag");
    RowGroupEntry rg_entry{};
    if (!fc.pod(&rg_entry)) return fail(Code::kTruncated, "patch_columns: short donor row group directory");
    Status s = read_schema_node(fc, &donor->schema);
    if (!s.is_ok()) return s;

    if (rg_entry.footer_offset + rg_entry.footer_bytes > footer_start)
        return fail(Code::kMalformed, "patch_columns: donor row group footer out of range");
    Cursor rc(src, static_cast<size_t>(rg_entry.footer_offset),
              static_cast<size_t>(rg_entry.footer_offset + rg_entry.footer_bytes));
    RowGroupFooterHeader rh{};
    if (!rc.pod(&rh)) return fail(Code::kTruncated, "patch_columns: short donor row group footer");
    std::string rg_tag;
    if (!rc.take_string(&rg_tag, rh.writer_tag_bytes))
        return fail(Code::kTruncated, "patch_columns: donor row group footer ends in the tag");

    Column column;
    s = read_column_head(rc, &column);
    if (!s.is_ok()) return s;
    if (column.head.child_count != 0)
        return fail(Code::kMalformed,
                    "patch_columns: donor column '" + column.name +
                    "' is nested; adding an ARRAY column is not supported");

    std::vector<SectionEntry> sections(rh.section_count);
    for (SectionEntry& entry : sections)
        if (!rc.pod(&entry))
            return fail(Code::kTruncated, "patch_columns: short donor section directory");
    s = attach_sections(&column, sections);
    if (!s.is_ok()) return s;

    for (const SectionEntry& entry : column.required)
        if (entry.offset + entry.stored_bytes > bytes.size())
            return fail(Code::kMalformed, "patch_columns: donor section runs past its end");

    donor->head    = column.head;
    donor->name    = column.name;
    donor->logical = column.logical;

    // Split the donor's sections: everything but VALIDITY is length-independent
    // and copied verbatim; VALIDITY is re-made per row group because its size is
    // ceil(length/8). A SELECTION section would mean the donor was not constant-
    // or identity-shaped, which a one-row column cannot be.
    for (const SectionEntry& entry : column.required) {
        if (entry.kind == static_cast<uint16_t>(SectionKind::kSelection))
            return fail(Code::kMalformed,
                        "patch_columns: donor column carries a stored selection");
        if (entry.kind == static_cast<uint16_t>(SectionKind::kValidity)) {
            donor->null_fill = donor_row_is_null(entry, src);
            continue;
        }
        donor->sections.push_back(entry);
        donor->section_bytes.emplace_back(src + entry.offset,
                                          src + entry.offset + entry.stored_bytes);
    }
    return Status::ok();
}

// Emit one added column into `sink` at `rows` logical rows, appending its
// section entries to `directory`.
void emit_added_column(Sink& sink, const Donor& donor, uint64_t rows,
                       std::vector<SectionEntry>* directory, Column* out) {
    out->head    = donor.head;
    out->name    = donor.name;
    out->logical = donor.logical;

    // CONSTANT is the whole trick: one value, no selection section, and the
    // reader hands every row data[0]. So the donor's data section is already
    // the right data section for any number of rows.
    out->head.length         = static_cast<uint32_t>(rows);
    out->head.data_length    = 1;
    out->head.selection_kind = static_cast<uint8_t>(SelectionKind::kConstant);
    out->head.value_order    = static_cast<uint8_t>(ValueOrder::kAsWritten);

    // The donor's vector_flags describe a ONE-ROW column and must not be copied
    // wholesale. A one-row dense column is SEL_IDENTITY, and the reader checks
    // that flag against selection_kind — a hint contradicting the stored layout
    // means the file disagrees with itself, and it is rejected (reader_v1.cpp).
    //
    // Cleared:
    //   SEL_IDENTITY / SEL_PERMUTATION — false once data_length is 1 and
    //     length is N; both imply data_length == length.
    //   DICT_CODES_DENSE — asserts every code is referenced by at least one
    //     VALID row, which is untrue for a NULL fill, where no row is valid.
    // Everything else is left as the donor set it: they are pure hints and a
    // constant column satisfies them if a one-row column did.
    out->head.vector_flags &= static_cast<uint8_t>(
        ~(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION | DRAKEN_DICT_CODES_DENSE));
    // No statistics. The donor's describe ONE row; scaling a null count or a
    // distinct count to N would be fabricating a fact about data nobody
    // measured, and `stats_bytes == 0` means NOT TRACKED, which the format is
    // careful to distinguish from zero.
    out->head.stats_bytes = 0;
    out->statistics.clear();
    out->head.index_section_index = 0;
    out->head.index_section_count = 0;
    out->children.clear();

    const uint32_t first = static_cast<uint32_t>(directory->size());
    for (size_t i = 0; i < donor.sections.size(); ++i) {
        SectionEntry entry = donor.sections[i];
        entry.offset = sink.position();
        sink.bytes(donor.section_bytes[i].data(), donor.section_bytes[i].size());
        directory->push_back(entry);
    }

    if (donor.null_fill) {
        // The only part that scales with the row count: ceil(rows/8) bytes of
        // zero, meaning "no row is valid". Bits at or above `length` are
        // padding and carry no meaning (FORMAT.md §7.1).
        const size_t bitmap_bytes = static_cast<size_t>((rows + 7u) / 8u);
        std::vector<uint8_t> bitmap(bitmap_bytes, 0u);
        sink.align_section();
        SectionEntry entry{};
        entry.kind          = static_cast<uint16_t>(SectionKind::kValidity);
        entry.encoding      = static_cast<uint8_t>(Encoding::kPlain);
        entry.codec         = static_cast<uint8_t>(SectionCodec::kNone);
        entry.reserved      = 0;
        entry.offset        = sink.position();
        entry.stored_bytes  = bitmap_bytes;
        entry.encoded_bytes = bitmap_bytes;
        entry.plain_bytes   = bitmap_bytes;
        entry.checksum      = checksum_xxh3_64(bitmap.data(), bitmap_bytes);
        sink.bytes(bitmap.data(), bitmap_bytes);
        directory->push_back(entry);
    }

    out->head.section_index = first;
    out->head.section_count = static_cast<uint32_t>(directory->size()) - first;
}

// How many statistics slots a column subtree occupies in the FILE footer's
// per-row-group statistics, which are written for EVERY column including
// untracked ones (a zero length) and including ARRAY children.
size_t stat_slots(const SchemaNode& node) {
    size_t total = 1;
    for (const SchemaNode& child : node.children) total += stat_slots(child);
    return total;
}

}  // namespace

Status patch_columns(const void* file, size_t file_bytes,
                     const std::vector<std::string>& drop,
                     const std::vector<std::pair<std::string, std::string>>& rename,
                     const std::vector<DonorFile>& add,
                     std::vector<uint8_t>* out) {
    if (file == nullptr || out == nullptr)
        return fail(Code::kMalformed, "patch_columns: null file or output");
    if (drop.empty() && rename.empty() && add.empty())
        return fail(Code::kMalformed, "patch_columns: no changes to make");

    std::vector<Donor> donors(add.size());
    for (size_t i = 0; i < add.size(); ++i) {
        Status s = parse_donor(add[i], &donors[i]);
        if (!s.is_ok()) return s;
    }
    if (file_bytes < kFileHeadBytes + kFileTailBytes)
        return fail(Code::kTruncated, "patch_columns: too small to be a skene file");

    const uint8_t* src = static_cast<const uint8_t*>(file);

    // ── head and tail ──
    FileHead head{};
    std::memcpy(&head, src, sizeof(head));
    if (head.magic != kMagic)
        return fail(Code::kNotSkene, "patch_columns: head magic missing");

    FileTail tail{};
    std::memcpy(&tail, src + file_bytes - kFileTailBytes, sizeof(tail));
    if (tail.magic != kMagic)
        return fail(Code::kNotSkene, "patch_columns: tail magic missing");
    if (tail.version != kVersion)
        return fail(Code::kUnsupportedVersion,
                    "patch_columns: this build patches only v" + std::to_string(kVersion) +
                    "; migrate the file forward first (skene::migrate_file)");

    const size_t footer_end = file_bytes - kFileTailBytes;
    if (tail.footer_bytes > footer_end - kFileHeadBytes)
        return fail(Code::kTruncated, "patch_columns: file footer runs past the head");
    const size_t footer_start = footer_end - tail.footer_bytes;
    if (checksum_xxh3_64(src + footer_start, tail.footer_bytes) != tail.footer_checksum)
        return fail(Code::kChecksumMismatch, "patch_columns: file footer checksum mismatch");

    // ── file footer ──
    Cursor fc(src, footer_start, footer_end);
    FileFooterHeader fh{};
    if (!fc.pod(&fh)) return fail(Code::kTruncated, "patch_columns: short file footer");
    if (fh.footer_magic != kFileFooterMagic)
        return fail(Code::kMalformed, "patch_columns: file footer magic missing");

    std::string writer_tag;
    if (!fc.take_string(&writer_tag, fh.writer_tag_bytes))
        return fail(Code::kTruncated, "patch_columns: file footer ends inside the writer tag");

    std::vector<RowGroupEntry> row_groups(fh.row_group_count);
    for (RowGroupEntry& entry : row_groups)
        if (!fc.pod(&entry))
            return fail(Code::kTruncated, "patch_columns: short row group directory");

    std::vector<SchemaNode> schema(fh.column_count);
    for (SchemaNode& node : schema) {
        Status s = read_schema_node(fc, &node);
        if (!s.is_ok()) return s;
    }

    // Cluster spec (v2): sits between the schema and the statistics.
    ClusterSpecHeader cluster_header{};
    if (!fc.pod(&cluster_header))
        return fail(Code::kTruncated, "patch_columns: short cluster spec");
    std::vector<SortKey> cluster_keys(cluster_header.key_count);
    for (SortKey& key : cluster_keys)
        if (!fc.pod(&key))
            return fail(Code::kTruncated, "patch_columns: short cluster spec keys");

    // Per-row-group statistics, kept as raw (length, bytes) pairs per slot so an
    // entry longer than this build understands survives the round trip.
    std::vector<std::vector<std::vector<uint8_t>>> file_stats(row_groups.size());
    size_t slots_per_group = 0;
    for (const SchemaNode& node : schema) slots_per_group += stat_slots(node);
    for (std::vector<std::vector<uint8_t>>& group : file_stats) {
        group.resize(slots_per_group);
        for (std::vector<uint8_t>& slot : group) {
            uint32_t length = 0;
            if (!fc.pod(&length))
                return fail(Code::kTruncated, "patch_columns: short per-row-group statistics");
            slot.resize(length);
            if (length > 0 && !fc.take(slot.data(), length))
                return fail(Code::kTruncated, "patch_columns: short statistics blob");
        }
    }

    // ── resolve the requested changes against the schema ──
    std::vector<bool> keep(schema.size(), true);
    std::vector<std::string> new_names(schema.size());
    for (size_t i = 0; i < schema.size(); ++i) new_names[i] = schema[i].name;

    for (const std::string& name : drop) {
        bool found = false;
        for (size_t i = 0; i < schema.size(); ++i)
            if (schema[i].name == name) { keep[i] = false; found = true; }
        if (!found)
            return fail(Code::kMalformed, "patch_columns: no column named '" + name + "' to drop");
    }
    for (const auto& pair : rename) {
        bool found = false;
        for (size_t i = 0; i < schema.size(); ++i)
            if (schema[i].name == pair.first) { new_names[i] = pair.second; found = true; }
        if (!found)
            return fail(Code::kMalformed,
                        "patch_columns: no column named '" + pair.first + "' to rename");
    }

    size_t surviving = donors.size();
    for (size_t i = 0; i < schema.size(); ++i) if (keep[i]) ++surviving;
    if (surviving == 0)
        return fail(Code::kMalformed,
                    "patch_columns: dropping every column would leave no relation");
    std::vector<std::string> surviving_names;
    for (size_t i = 0; i < schema.size(); ++i)
        if (keep[i]) surviving_names.push_back(new_names[i]);
    for (const Donor& donor : donors) surviving_names.push_back(donor.name);
    for (size_t i = 0; i < surviving_names.size(); ++i)
        for (size_t j = i + 1; j < surviving_names.size(); ++j)
            if (surviving_names[i] == surviving_names[j])
                return fail(Code::kMalformed,
                            "patch_columns: the result would have two columns named '" +
                            surviving_names[i] + "'");

    // ── emit ──
    out->clear();
    out->reserve(file_bytes);
    Sink sink(out);
    sink.bytes(src, kFileHeadBytes);

    std::vector<RowGroupEntry> new_row_groups;
    new_row_groups.reserve(row_groups.size());
    std::vector<std::vector<std::vector<uint8_t>>> new_file_stats(row_groups.size());

    for (size_t rg = 0; rg < row_groups.size(); ++rg) {
        const RowGroupEntry& old_entry = row_groups[rg];
        if (old_entry.footer_offset + old_entry.footer_bytes > footer_start)
            return fail(Code::kMalformed, "patch_columns: row group footer runs past the file footer");
        if (checksum_xxh3_64(src + old_entry.footer_offset, old_entry.footer_bytes) !=
            old_entry.footer_checksum)
            return fail(Code::kChecksumMismatch,
                        "patch_columns: row group " + std::to_string(rg) +
                        " footer checksum mismatch");

        Cursor rc(src, static_cast<size_t>(old_entry.footer_offset),
                  static_cast<size_t>(old_entry.footer_offset + old_entry.footer_bytes));
        RowGroupFooterHeader rh{};
        if (!rc.pod(&rh)) return fail(Code::kTruncated, "patch_columns: short row group footer");
        std::string rg_tag;
        if (!rc.take_string(&rg_tag, rh.writer_tag_bytes))
            return fail(Code::kTruncated, "patch_columns: row group footer ends in the writer tag");
        if (rh.column_count != schema.size())
            return fail(Code::kMalformed,
                        "patch_columns: row group " + std::to_string(rg) +
                        " has a different column count from the file schema");

        std::vector<Column> columns(rh.column_count);
        for (Column& column : columns) {
            Status s = read_column_head(rc, &column);
            if (!s.is_ok()) return s;
        }
        std::vector<SectionEntry> sections(rh.section_count);
        for (SectionEntry& entry : sections)
            if (!rc.pod(&entry))
                return fail(Code::kTruncated, "patch_columns: short section directory");
        for (Column& column : columns) {
            Status s = attach_sections(&column, sections);
            if (!s.is_ok()) return s;
        }
        for (Column& column : columns) {
            Status s = attach_statistics(&column, rc);
            if (!s.is_ok()) return s;
        }

        for (const SectionEntry& entry : sections)
            if (entry.offset + entry.stored_bytes > file_bytes)
                return fail(Code::kMalformed, "patch_columns: a section runs past the end of the file");

        const uint64_t entry_row_count = old_entry.row_count;
        RowGroupEntry entry{};
        entry.row_count   = old_entry.row_count;
        entry.first_row   = old_entry.first_row;
        entry.data_offset = sink.position();

        // DATA region, then INDEX region — surviving columns only, in order.
        std::vector<SectionEntry> new_sections;
        new_sections.reserve(sections.size());
        for (size_t i = 0; i < columns.size(); ++i)
            if (keep[i]) copy_sections(sink, src, &columns[i], /*optional=*/false, &new_sections);

        // An added column's sections are REQUIRED, so they belong in the DATA
        // region with the other required ones — before the INDEX pass below.
        // Emitting them after it would put required sections inside the index
        // region, which breaks the one-range-request guarantee that region split
        // exists for (FORMAT.md §3).
        std::vector<Column> added(donors.size());
        for (size_t d = 0; d < donors.size(); ++d)
            emit_added_column(sink, donors[d], entry_row_count, &new_sections, &added[d]);

        for (size_t i = 0; i < columns.size(); ++i)
            if (keep[i]) copy_sections(sink, src, &columns[i], /*optional=*/true, &new_sections);

        entry.data_bytes    = sink.position() - entry.data_offset;
        entry.footer_offset = sink.position();

        RowGroupFooterHeader new_rh = rh;
        new_rh.column_count  = static_cast<uint32_t>(surviving);
        new_rh.section_count = static_cast<uint32_t>(new_sections.size());
        sink.pod(new_rh);
        sink.bytes(rg_tag.data(), rg_tag.size());

        for (size_t i = 0; i < columns.size(); ++i) {
            if (!keep[i]) continue;
            columns[i].name = new_names[i];
            columns[i].head.name_bytes = static_cast<uint32_t>(new_names[i].size());
            write_column_entry(sink, columns[i]);
        }
        for (const Column& column : added) write_column_entry(sink, column);
        for (const SectionEntry& section : new_sections) sink.pod(section);
        for (size_t i = 0; i < columns.size(); ++i)
            if (keep[i]) write_statistics(sink, columns[i]);
        // Added columns carry no statistics blob (stats_bytes == 0).

        const uint64_t footer_len = sink.position() - entry.footer_offset;
        if (footer_len > UINT32_MAX)
            return fail(Code::kMalformed, "patch_columns: row group footer exceeds 32-bit length");
        entry.footer_bytes    = static_cast<uint32_t>(footer_len);
        entry.footer_checksum = checksum_xxh3_64(out->data() + entry.footer_offset,
                                                 static_cast<size_t>(footer_len));
        entry.reserved = 0;
        new_row_groups.push_back(entry);

        // Keep this row group's statistics slots for the columns that survive.
        std::vector<std::vector<uint8_t>>& kept = new_file_stats[rg];
        size_t slot = 0;
        for (size_t i = 0; i < schema.size(); ++i) {
            const size_t width = stat_slots(schema[i]);
            if (keep[i])
                for (size_t k = 0; k < width; ++k) kept.push_back(file_stats[rg][slot + k]);
            slot += width;
        }
        for (size_t d = 0; d < donors.size(); ++d) kept.emplace_back();  // not tracked
    }

    // ── FILE FOOTER ──
    const uint64_t new_footer_start = sink.position();

    FileFooterHeader new_fh = fh;
    new_fh.column_count = static_cast<uint32_t>(surviving);
    sink.pod(new_fh);
    sink.bytes(writer_tag.data(), writer_tag.size());

    for (const RowGroupEntry& entry : new_row_groups) sink.pod(entry);
    for (size_t i = 0; i < schema.size(); ++i) {
        if (!keep[i]) continue;
        schema[i].name = new_names[i];
        schema[i].head.name_bytes = static_cast<uint32_t>(new_names[i].size());
        write_schema_entry(sink, schema[i]);
    }
    for (const Donor& donor : donors) write_schema_entry(sink, donor.schema);

    // Cluster spec: renames leave it untouched (ordinals name positions, not
    // names); a drop keeps the longest PREFIX of keys whose columns all
    // survive, with ordinals remapped to the surviving schema's positions.
    // Rows ordered by (a, b) are still ordered by (a) when b goes, but are NOT
    // generally ordered by (b) when a goes — a promise must shrink to what
    // remains provably true, never stretch.
    {
        std::vector<uint32_t> new_ordinal(schema.size(), UINT32_MAX);
        uint32_t position = 0;
        for (size_t i = 0; i < schema.size(); ++i)
            if (keep[i]) new_ordinal[i] = position++;

        std::vector<SortKey> kept_keys;
        for (const SortKey& key : cluster_keys) {
            if (key.column_ordinal >= schema.size()
                    || new_ordinal[key.column_ordinal] == UINT32_MAX)
                break;
            SortKey remapped = key;
            remapped.column_ordinal = new_ordinal[key.column_ordinal];
            kept_keys.push_back(remapped);
        }
        ClusterSpecHeader spec{};
        spec.key_count = static_cast<uint16_t>(kept_keys.size());
        spec.reserved  = 0;
        sink.pod(spec);
        for (const SortKey& key : kept_keys) sink.pod(key);
    }

    for (const std::vector<std::vector<uint8_t>>& group : new_file_stats)
        for (const std::vector<uint8_t>& slot : group) {
            sink.u32(static_cast<uint32_t>(slot.size()));
            if (!slot.empty()) sink.bytes(slot.data(), slot.size());
        }

    const uint64_t new_footer_len = sink.position() - new_footer_start;
    if (new_footer_len > UINT32_MAX)
        return fail(Code::kMalformed, "patch_columns: file footer exceeds 32-bit length");

    FileTail new_tail = tail;
    new_tail.footer_bytes    = static_cast<uint32_t>(new_footer_len);
    new_tail.footer_checksum = checksum_xxh3_64(out->data() + new_footer_start,
                                                static_cast<size_t>(new_footer_len));
    sink.pod(new_tail);

    return Status::ok();
}

}  // namespace skene
