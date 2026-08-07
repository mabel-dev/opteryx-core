// Writer tests.
//
// The reader does not exist yet, so these parse the footer independently rather
// than round-tripping through skene's own reader. That is deliberate: a
// writer/reader pair can agree with each other and both be wrong about the
// format. These assert against the format as written down.

#include <cstring>
#include <map>
#include <string>
#include <vector>

#include "build_vectors.h"
#include "harness.h"
#include "skene/checksum.h"
#include "skene/format.h"
#include "skene/writer.h"

#include "encoding.h"

using namespace skene;
using namespace skene_test;

// ─── Independent footer parser ──────────────────────────────────────────────

struct ParsedColumn {
    ColumnEntryHead       head;
    std::string           name;
    LogicalTypeDescriptor logical;
    std::vector<ParsedColumn> children;
};

struct ParsedFile {
    FileHead                  head;
    FileTail                  tail;
    FooterFileHeader          file_header;
    std::vector<ParsedColumn> columns;
    std::vector<SectionEntry> sections;
};

static const uint8_t* read_column(const uint8_t* p, ParsedColumn* out) {
    std::memcpy(&out->head, p, sizeof(ColumnEntryHead));
    p += sizeof(ColumnEntryHead);
    out->name.assign(reinterpret_cast<const char*>(p), out->head.name_bytes);
    p += out->head.name_bytes;
    if (out->head.logical_present) {
        std::memcpy(&out->logical, p, sizeof(LogicalTypeDescriptor));
        p += sizeof(LogicalTypeDescriptor);
    }
    out->children.resize(out->head.child_count);
    for (uint32_t i = 0; i < out->head.child_count; ++i)
        p = read_column(p, &out->children[i]);
    return p;
}

static bool parse(const std::vector<uint8_t>& bytes, ParsedFile* out) {
    if (bytes.size() < kFileHeadBytes + kFileTailBytes) return false;
    std::memcpy(&out->head, bytes.data(), sizeof(FileHead));
    std::memcpy(&out->tail, bytes.data() + bytes.size() - kFileTailBytes,
                sizeof(FileTail));

    const size_t footer_end   = bytes.size() - kFileTailBytes;
    const size_t footer_start = footer_end - out->tail.footer_bytes;
    const uint8_t* p = bytes.data() + footer_start;

    std::memcpy(&out->file_header, p, sizeof(FooterFileHeader));
    p += sizeof(FooterFileHeader) + out->file_header.writer_tag_bytes;

    out->columns.resize(out->file_header.column_count);
    for (uint32_t i = 0; i < out->file_header.column_count; ++i)
        p = read_column(p, &out->columns[i]);

    out->sections.resize(out->file_header.section_count);
    for (uint32_t i = 0; i < out->file_header.section_count; ++i) {
        std::memcpy(&out->sections[i], p, sizeof(SectionEntry));
        p += sizeof(SectionEntry);
    }
    return p == bytes.data() + footer_end;
}

// Sections belonging to one column, keyed by kind.
static std::map<uint16_t, SectionEntry> sections_of(const ParsedFile& f,
                                                    const ParsedColumn& c) {
    std::map<uint16_t, SectionEntry> by_kind;
    for (uint32_t i = 0; i < c.head.section_count; ++i) {
        const SectionEntry& e = f.sections[c.head.section_index + i];
        by_kind[e.kind] = e;
    }
    return by_kind;
}

static bool has(const std::map<uint16_t, SectionEntry>& m, SectionKind k) {
    return m.count(static_cast<uint16_t>(k)) != 0;
}

static std::vector<uint8_t> write_or_die(const CxxMorsel& m) {
    std::vector<uint8_t> bytes;
    Status st = write_morsel(m, WriteOptions::for_spill(), &bytes);
    if (!st.is_ok()) {
        std::fprintf(stderr, "  write failed: %s\n", st.message().c_str());
        ++skene_test::g_failures;
    }
    return bytes;
}

// ─── Framing ────────────────────────────────────────────────────────────────

static void test_framing_and_checksums() {
    auto m = morsel_of({{"a", dense_column<int64_t>({1, 2, 3}, DRAKEN_INT64)}});
    auto bytes = write_or_die(m);

    ParsedFile f;
    CHECK(parse(bytes, &f));

    CHECK_EQ(f.head.magic, kMagic);
    CHECK_EQ(f.tail.magic, kMagic);
    CHECK_EQ(f.head.version, kVersion);
    CHECK_EQ(f.tail.version, kVersion);
    CHECK_EQ(f.head.endianness, f.tail.endianness);
    CHECK_EQ(f.file_header.row_count, uint64_t{3});

    // The footer checksum must cover exactly the footer, so a corrupt directory
    // is caught before a single offset is followed.
    const size_t footer_end   = bytes.size() - kFileTailBytes;
    const size_t footer_start = footer_end - f.tail.footer_bytes;
    CHECK_EQ(checksum_xxh3_64(bytes.data() + footer_start, f.tail.footer_bytes),
             f.tail.footer_checksum);

    // Every section must lie inside the data region and match its own checksum.
    for (const SectionEntry& e : f.sections) {
        CHECK(e.offset >= kFileHeadBytes);
        CHECK(e.offset + e.stored_bytes <= footer_start);
        CHECK_EQ(checksum_xxh3_64(bytes.data() + e.offset, e.stored_bytes),
                 e.checksum);
    }
}

// ─── Selection kinds ────────────────────────────────────────────────────────

static void test_dense_stores_no_selection() {
    auto m = morsel_of({{"a", dense_column<int64_t>({10, 20, 30}, DRAKEN_INT64)}});
    auto bytes = write_or_die(m);
    ParsedFile f;
    CHECK(parse(bytes, &f));

    const ParsedColumn& c = f.columns[0];
    CHECK_EQ(c.head.selection_kind, static_cast<uint8_t>(SelectionKind::kIdentity));
    auto s = sections_of(f, c);
    CHECK(has(s, SectionKind::kData));
    CHECK(!has(s, SectionKind::kSelection));  // reader attaches the global
    CHECK_EQ(s[static_cast<uint16_t>(SectionKind::kData)].stored_bytes,
             uint64_t{3 * sizeof(int64_t)});
}

static void test_constant_stores_no_selection() {
    // Ruled: constant columns get no permutation/selection array at all.
    auto m = morsel_of({{"k", constant_column<int64_t>(42, 1000, DRAKEN_INT64)}});
    auto bytes = write_or_die(m);
    ParsedFile f;
    CHECK(parse(bytes, &f));

    const ParsedColumn& c = f.columns[0];
    CHECK_EQ(c.head.selection_kind, static_cast<uint8_t>(SelectionKind::kConstant));
    CHECK_EQ(c.head.length, uint32_t{1000});
    CHECK_EQ(c.head.data_length, uint32_t{1});

    auto s = sections_of(f, c);
    CHECK(!has(s, SectionKind::kSelection));
    // One value, not a thousand — and not 4 KB of zero codes either.
    CHECK_EQ(s[static_cast<uint16_t>(SectionKind::kData)].stored_bytes,
             uint64_t{sizeof(int64_t)});
    CHECK(bytes.size() < 512);
}

static void test_dict_selection_survives_verbatim() {
    // The whole reason Parquet was rejected: the encoding must be RESTORED, not
    // re-derived. These exact codes must appear in the file byte for byte.
    const std::vector<uint32_t> codes = {2, 0, 1, 1, 0, 2, 2, 0};
    auto m = morsel_of({{"d", dict_column<int64_t>({100, 200, 300}, codes, DRAKEN_INT64)}});
    auto bytes = write_or_die(m);
    ParsedFile f;
    CHECK(parse(bytes, &f));

    const ParsedColumn& c = f.columns[0];
    CHECK_EQ(c.head.selection_kind, static_cast<uint8_t>(SelectionKind::kStored));
    CHECK_EQ(c.head.data_length, uint32_t{3});
    CHECK_EQ(c.head.length, uint32_t{8});

    auto s = sections_of(f, c);
    const SectionEntry& sel = s[static_cast<uint16_t>(SectionKind::kSelection)];

    // plain_bytes is the shape the directory declares; stored_bytes is whatever
    // the encoding achieved. 8 codes over 3 distinct values need 2 bits each, so
    // the body is far smaller than the 32 plain bytes.
    CHECK_EQ(sel.plain_bytes, uint64_t{codes.size() * sizeof(uint32_t)});
    CHECK_EQ(sel.encoding, static_cast<uint16_t>(Encoding::kBitpack));
    CHECK(sel.stored_bytes < sel.plain_bytes);

    // What must survive is the CODES, not their byte layout — the encoding is a
    // size optimization and nothing else, so the test asserts the decoded values.
    std::vector<uint32_t> decoded(codes.size());
    CHECK(bitpack_decode_codes(bytes.data() + sel.offset, sel.stored_bytes,
                               static_cast<uint32_t>(codes.size()),
                               decoded.data()).is_ok());
    CHECK(decoded == codes);
}

static void test_permutation_selection_is_not_mistaken_for_identity() {
    // An all-distinct column with a REAL permutation has data_length == length —
    // exactly the shape a "data_length == length means identity" heuristic would
    // misread, silently reordering every row. This is the case value ordering
    // will produce constantly, so it must be classified by CONTENT.
    const std::vector<uint32_t> perm = {3, 1, 0, 2};
    auto m = morsel_of({{"p", dict_column<int64_t>({7, 8, 9, 10}, perm, DRAKEN_INT64)}});
    auto bytes = write_or_die(m);
    ParsedFile f;
    CHECK(parse(bytes, &f));

    const ParsedColumn& c = f.columns[0];
    CHECK_EQ(c.head.data_length, c.head.length);  // the trap
    CHECK_EQ(c.head.selection_kind, static_cast<uint8_t>(SelectionKind::kStored));

    auto s = sections_of(f, c);
    CHECK(has(s, SectionKind::kSelection));
    const SectionEntry& sel = s[static_cast<uint16_t>(SectionKind::kSelection)];
    std::vector<uint32_t> decoded(perm.size());
    CHECK(bitpack_decode_codes(bytes.data() + sel.offset, sel.stored_bytes,
                               static_cast<uint32_t>(perm.size()),
                               decoded.data()).is_ok());
    CHECK(decoded == perm);
}

// ─── Families ───────────────────────────────────────────────────────────────

static void test_validity_and_flags() {
    auto m = morsel_of({{"n", dense_column<int64_t>({1, 2, 3, 4},
                                                    DRAKEN_INT64,
                                                    {true, false, true, false})}});
    auto bytes = write_or_die(m);
    ParsedFile f;
    CHECK(parse(bytes, &f));

    auto s = sections_of(f, f.columns[0]);
    CHECK(has(s, SectionKind::kValidity));
    CHECK_EQ(s[static_cast<uint16_t>(SectionKind::kValidity)].stored_bytes, uint64_t{1});

    // flags travel verbatim — layout hints must survive.
    CHECK_EQ(f.columns[0].head.vector_flags,
             static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION));
}

static void test_bool_is_bit_packed() {
    // 20 rows must occupy ceil(20/8) == 3 bytes, not 20. Sizing a BOOL data
    // section by row count rather than by bits inflates it 8x and puts every
    // following section's offset out by the difference.
    std::vector<bool> bits(20, false);
    bits[0] = bits[7] = bits[8] = bits[19] = true;

    auto m = morsel_of({{"b", bool_column(bits)}});
    auto bytes = write_or_die(m);
    ParsedFile f;
    CHECK(parse(bytes, &f));

    const ParsedColumn& c = f.columns[0];
    CHECK_EQ(c.head.length, uint32_t{20});
    CHECK_EQ(c.head.data_length, uint32_t{20});

    auto s = sections_of(f, c);
    const SectionEntry& data = s[static_cast<uint16_t>(SectionKind::kData)];
    CHECK_EQ(data.stored_bytes, uint64_t{3});

    // And the bits themselves survive, in draken's LSB-first order.
    const uint8_t* written = bytes.data() + data.offset;
    CHECK_EQ(written[0], uint8_t{0x81});  // bits 0 and 7
    CHECK_EQ(written[1], uint8_t{0x01});  // bit 8
    CHECK_EQ(written[2], uint8_t{0x08});  // bit 19
}

static void test_string_slots_and_arena() {
    const std::vector<std::string> values = {
        "short",                      // inline (<= 12 bytes)
        "a considerably longer string that lives in the arena",
        "",
        "also quite long, definitely past twelve bytes",
    };
    auto m = morsel_of({{"s", string_column(values)}});
    auto bytes = write_or_die(m);
    ParsedFile f;
    CHECK(parse(bytes, &f));

    const ParsedColumn& c = f.columns[0];
    CHECK_EQ(c.head.string_slot_count, uint64_t{4});
    CHECK_EQ(c.head.string_payloads_elided, uint8_t{0});

    size_t expect_arena = 0;
    for (const std::string& v : values)
        if (v.size() > STR_INLINE_MAX) expect_arena += v.size();
    CHECK_EQ(c.head.string_arena_used, static_cast<uint64_t>(expect_arena));

    auto s = sections_of(f, c);
    CHECK(has(s, SectionKind::kStringSlots));
    CHECK(has(s, SectionKind::kStringArena));
    // No kData: the arena STRUCT is decomposed, never written as a blob — its
    // slots/arena members are absolute pointers.
    CHECK(!has(s, SectionKind::kData));
    CHECK_EQ(s[static_cast<uint16_t>(SectionKind::kStringSlots)].stored_bytes,
             uint64_t{4 * sizeof(DrakenStringSlot)});
    CHECK_EQ(s[static_cast<uint16_t>(SectionKind::kStringArena)].stored_bytes,
             static_cast<uint64_t>(expect_arena));
}

static void test_length_only_column_round_trips_the_elided_flag() {
    // A length-only column: no arena, long slots stamped with the trap value.
    // If payloads_elided did not survive, a reader would treat 0xFFFFFFFF as a
    // real arena offset and read ~4 GB out of bounds.
    const std::vector<std::string> values = {
        "tiny", "a long value whose bytes were never materialized"};
    auto m = morsel_of({{"len_only",
                         string_column(values, DRAKEN_VARCHAR, {}, /*elide=*/true)}});
    auto bytes = write_or_die(m);
    ParsedFile f;
    CHECK(parse(bytes, &f));

    const ParsedColumn& c = f.columns[0];
    CHECK_EQ(c.head.string_payloads_elided, uint8_t{1});
    CHECK_EQ(c.head.string_arena_used, uint64_t{0});

    auto s = sections_of(f, c);
    CHECK(has(s, SectionKind::kStringSlots));
    CHECK(!has(s, SectionKind::kStringArena));  // there are no payload bytes

    // The trap value is present in the written slots, not silently normalised.
    const SectionEntry& slots = s[static_cast<uint16_t>(SectionKind::kStringSlots)];
    const DrakenStringSlot* written =
        reinterpret_cast<const DrakenStringSlot*>(bytes.data() + slots.offset);
    CHECK(!str_is_inline(&written[1]));
    CHECK_EQ(written[1].ext.arena_offset, STR_ELIDED_PAYLOAD_OFFSET);
}

static void test_array_writes_offsets_and_child() {
    auto m = morsel_of({{"arr", array_column({{1, 2, 3}, {}, {4}, {5, 6}})}});
    auto bytes = write_or_die(m);
    ParsedFile f;
    CHECK(parse(bytes, &f));

    const ParsedColumn& c = f.columns[0];
    CHECK_EQ(c.head.child_count, uint32_t{1});
    CHECK_EQ(c.children.size(), size_t{1});
    CHECK_EQ(c.children[0].head.type, static_cast<uint32_t>(DRAKEN_INT64));
    CHECK_EQ(c.children[0].head.length, uint32_t{6});  // 3 + 0 + 1 + 2 elements

    // Offsets are sized by the LOGICAL row count: arrays are stored dense.
    auto s = sections_of(f, c);
    CHECK_EQ(s[static_cast<uint16_t>(SectionKind::kData)].stored_bytes,
             uint64_t{(4 + 1) * sizeof(int32_t)});

    // The child gets its own sections — nesting the directory costs nothing in
    // addressability.
    auto cs = sections_of(f, c.children[0]);
    CHECK(has(cs, SectionKind::kData));
}

// ─── LogicalType — the reason this format exists ────────────────────────────

static void test_ipv4_descriptor_is_carried() {
    // An IPv4 column is a UINT32 refined by an IPV4 descriptor. Parquet stores
    // the 32 bits and loses the refinement on every round trip; carrying it is
    // the point of this format.
    LogicalType lt;
    lt.kind = LogicalKind::IPV4;
    const LogicalType* interned = logical_type_intern(lt);

    auto m = morsel_of({{"ip", dense_column<uint32_t>(
        {0xC0A80101u, 0x08080808u}, DRAKEN_UINT32, {}, interned)}});
    auto bytes = write_or_die(m);
    ParsedFile f;
    CHECK(parse(bytes, &f));

    const ParsedColumn& c = f.columns[0];
    CHECK_EQ(c.head.type, static_cast<uint32_t>(DRAKEN_UINT32));
    CHECK_EQ(c.head.logical_present, uint8_t{1});
    CHECK_EQ(c.logical.kind, static_cast<uint8_t>(LogicalKind::IPV4));
}

static void test_decimal_descriptor_is_carried() {
    LogicalType lt;
    lt.kind = LogicalKind::DECIMAL;
    lt.precision = 18;
    lt.scale = 4;
    const LogicalType* interned = logical_type_intern(lt);

    auto m = morsel_of({{"amount", dense_column<int64_t>({12345, 67890},
                                                         DRAKEN_DECIMAL, {}, interned)}});
    auto bytes = write_or_die(m);
    ParsedFile f;
    CHECK(parse(bytes, &f));
    CHECK_EQ(f.columns[0].logical.precision, uint8_t{18});
    CHECK_EQ(f.columns[0].logical.scale, uint8_t{4});
}

// ─── Fail loud ──────────────────────────────────────────────────────────────

static void test_timestamp_without_descriptor_fails_loud() {
    // A TIMESTAMP64 with no descriptor is uninterpretable, not degraded. Writing
    // it would produce a file nothing can read correctly.
    auto m = morsel_of({{"ts", dense_column<int64_t>({1, 2}, DRAKEN_TIMESTAMP64)}});
    std::vector<uint8_t> bytes;
    Status st = write_morsel(m, WriteOptions::for_spill(), &bytes);
    CHECK(!st.is_ok());
    CHECK(st.code() == Code::kMalformed);
    CHECK(st.message().find("LogicalType") != std::string::npos);
}

static void test_out_of_range_selection_fails_loud() {
    // Caught at the boundary, where the producing operator is still nameable,
    // rather than as an out-of-bounds read in a later consumer.
    auto column = dict_column<int64_t>({1, 2}, {0, 1, 0}, DRAKEN_INT64);
    const_cast<uint32_t*>(column.view.selection)[2] = 99;

    auto m = morsel_of({{"bad", std::move(column)}});
    std::vector<uint8_t> bytes;
    Status st = write_morsel(m, WriteOptions::for_spill(), &bytes);
    CHECK(!st.is_ok());
    CHECK(st.message().find("out of range") != std::string::npos);
}

static void test_spill_profile_asks_for_nothing() {
    // The spill profile is the minimal shape of the format: read once, in
    // process, so no read acceleration is worth paying for. Value ordering and
    // statistics are what a spill file must NOT be paying for.
    const WriteOptions spill = WriteOptions::for_spill();
    
    CHECK(!spill.read_acceleration);

    auto m = morsel_of({{"a", dense_column<int64_t>({3, 1, 2}, DRAKEN_INT64)}});
    auto bytes = write_or_die(m);
    ParsedFile f;
    CHECK(parse(bytes, &f));
    CHECK_EQ(f.columns[0].head.value_order,
             static_cast<uint8_t>(ValueOrder::kAsWritten));
    CHECK_EQ(f.columns[0].head.stats_bytes, uint32_t{0});
}

static void test_field_id_must_be_complete_or_absent() {
    auto m = morsel_of({{"a", dense_column<int64_t>({1}, DRAKEN_INT64)},
                        {"b", dense_column<int64_t>({2}, DRAKEN_INT64)}});
    std::vector<uint8_t> bytes;

    WriteOptions partial;
    partial.field_ids = {7};  // two columns, one id
    CHECK(!write_morsel(m, partial, &bytes).is_ok());

    WriteOptions complete;
    complete.field_ids = {7, 9};
    CHECK(write_morsel(m, complete, &bytes).is_ok());

    ParsedFile f;
    CHECK(parse(bytes, &f));
    CHECK_EQ(f.columns[0].head.field_id, uint32_t{7});
    CHECK_EQ(f.columns[1].head.field_id, uint32_t{9});
}

int main() {
    test_framing_and_checksums();
    test_dense_stores_no_selection();
    test_constant_stores_no_selection();
    test_dict_selection_survives_verbatim();
    test_permutation_selection_is_not_mistaken_for_identity();
    test_validity_and_flags();
    test_bool_is_bit_packed();
    test_string_slots_and_arena();
    test_length_only_column_round_trips_the_elided_flag();
    test_array_writes_offsets_and_child();
    test_ipv4_descriptor_is_carried();
    test_decimal_descriptor_is_carried();
    test_timestamp_without_descriptor_fails_loud();
    test_out_of_range_selection_fails_loud();
    test_spill_profile_asks_for_nothing();
    test_field_id_must_be_complete_or_absent();
    return skene_test::summary("test_writer");
}
