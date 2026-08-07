// Format invariants that must hold before any writer or reader exists.
//
// These are the properties a future change is most likely to break silently:
// the on-disk struct layouts, the required/optional section split that the whole
// extensibility rule rests on, and the version support window.

#include "harness.h"
#include "skene/checksum.h"
#include "skene/format.h"

// skene serializes draken's structures, so it must see draken's ABI exactly as
// draken pins it. Including this here means draken's own static_asserts
// (sizeof(DrakenVector) == 40 and every field offset) are evaluated inside
// SKENE's translation units too. That is the only thing standing between a
// draken ABI change and skene silently writing a different layout, so it is a
// deliberate, tested property of this build rather than an accident of what
// happens to be included.
#include "core/buffers.h"
#include "core/string_slot.h"

using namespace skene;

static void test_draken_abi_visible_here() {
    // Re-asserted at skene's own compile time, not merely trusted.
    static_assert(sizeof(DrakenVector) == 40, "draken ABI drift seen from skene");
    static_assert(sizeof(DrakenStringSlot) == 16, "string slot width drift");
    static_assert(sizeof(DrakenStringArena) == 56, "string arena layout drift");
    CHECK_EQ(sizeof(DrakenVector), size_t{40});
}

static void test_head_and_tail_layout() {
    CHECK_EQ(sizeof(FileHead), kFileHeadBytes);
    CHECK_EQ(sizeof(FileTail), kFileTailBytes);

    // Magic must be the FIRST field of the head and the LAST of the tail: the
    // head magic rejects an unrelated object before anything else is read, and
    // the tail magic anchors the footer seek.
    CHECK_EQ(offsetof(FileHead, magic), size_t{0});
    CHECK_EQ(offsetof(FileTail, magic), kFileTailBytes - sizeof(uint32_t));

    // "SKEN" little-endian.
    const char* m = reinterpret_cast<const char*>(&kMagic);
    CHECK(m[0] == 'S' && m[1] == 'K' && m[2] == 'E' && m[3] == 'N');
}

static void test_section_required_optional_split() {
    // The extensibility rule depends entirely on this split being decidable
    // from the kind alone, with no registry: an old reader must know whether an
    // unknown kind is fatal or skippable.
    CHECK(section_is_required(static_cast<uint16_t>(SectionKind::kData)));
    CHECK(section_is_required(static_cast<uint16_t>(SectionKind::kSelection)));
    CHECK(section_is_required(static_cast<uint16_t>(SectionKind::kValidity)));
    CHECK(section_is_required(static_cast<uint16_t>(SectionKind::kStringSlots)));
    CHECK(section_is_required(static_cast<uint16_t>(SectionKind::kStringArena)));

    CHECK(!section_is_required(static_cast<uint16_t>(SectionKind::kBloom)));
    CHECK(!section_is_required(static_cast<uint16_t>(SectionKind::kPermutation)));
    CHECK(!section_is_required(static_cast<uint16_t>(SectionKind::kZoneMap)));

    // An unknown kind below the base is fatal; above it is skippable. A future
    // required section landing above the base would be silently ignored by an
    // old reader, which is exactly the failure the split exists to prevent.
    CHECK(section_is_required(255u));
    CHECK(!section_is_required(kSectionOptionalBase));
    CHECK(!section_is_required(60000u));
}

static void test_version_window() {
    CHECK(version_is_supported(kVersion));
    CHECK(version_is_supported(kMinReadVersion));
    CHECK(!version_is_supported(0u));
    CHECK(!version_is_supported(static_cast<uint16_t>(kVersion + 1)));

    // The window is deliberately NARROW — a build reads what it writes plus its
    // predecessor, and anything older is migrated forward one hop at a time by
    // retained binaries. test_probe.cpp owns the detail; this is only here so a
    // change to the window trips both suites.
    CHECK(kMinReadVersion <= kVersion);
    CHECK(static_cast<uint16_t>(kVersion - kMinReadVersion) <= 1u);
}

static void test_record_layouts() {
    CHECK_EQ(sizeof(SectionEntry), size_t{36});
    CHECK_EQ(sizeof(ColumnEntryHead), size_t{80});
    CHECK_EQ(sizeof(FooterFileHeader), size_t{48});
    CHECK_EQ(sizeof(ColumnStatistics), size_t{48});
    CHECK_EQ(sizeof(LogicalTypeDescriptor), size_t{12});
    CHECK_EQ(sizeof(ZoneMapEntry), size_t{16});
    CHECK_EQ(sizeof(SortKey), size_t{8});
}

// FORMAT.md documents every field's offset in byte tables, and a spec that
// quietly disagrees with the code is worse than no spec: an independent reader
// written from it produces garbage that still passes its own checksums. These
// pin each documented offset, so reordering a struct fails HERE, next to the
// tables it invalidates, rather than in someone else's implementation.
static void test_offsets_match_the_specification() {
    // FORMAT.md §4.1 — head
    CHECK_EQ(offsetof(FileHead, magic), size_t{0});
    CHECK_EQ(offsetof(FileHead, version), size_t{4});
    CHECK_EQ(offsetof(FileHead, endianness), size_t{6});
    CHECK_EQ(offsetof(FileHead, checksum_algorithm), size_t{7});
    CHECK_EQ(offsetof(FileHead, reserved), size_t{8});

    // FORMAT.md §4.2 — tail
    CHECK_EQ(offsetof(FileTail, footer_bytes), size_t{0});
    CHECK_EQ(offsetof(FileTail, footer_checksum), size_t{4});
    CHECK_EQ(offsetof(FileTail, version), size_t{12});
    CHECK_EQ(offsetof(FileTail, endianness), size_t{14});
    CHECK_EQ(offsetof(FileTail, checksum_algorithm), size_t{15});
    CHECK_EQ(offsetof(FileTail, reserved), size_t{16});
    CHECK_EQ(offsetof(FileTail, magic), size_t{20});

    // FORMAT.md §5.1 — footer file header
    CHECK_EQ(offsetof(FooterFileHeader, row_count), size_t{0});
    CHECK_EQ(offsetof(FooterFileHeader, column_count), size_t{8});
    CHECK_EQ(offsetof(FooterFileHeader, section_count), size_t{12});
    CHECK_EQ(offsetof(FooterFileHeader, file_uuid), size_t{16});
    CHECK_EQ(offsetof(FooterFileHeader, created_at_unix_us), size_t{32});
    CHECK_EQ(offsetof(FooterFileHeader, writer_tag_bytes), size_t{40});
    CHECK_EQ(offsetof(FooterFileHeader, file_flags), size_t{44});

    // FORMAT.md §5.2 — column directory entry
    CHECK_EQ(offsetof(ColumnEntryHead, field_id), size_t{0});
    CHECK_EQ(offsetof(ColumnEntryHead, name_bytes), size_t{4});
    CHECK_EQ(offsetof(ColumnEntryHead, type), size_t{8});
    CHECK_EQ(offsetof(ColumnEntryHead, vector_flags), size_t{12});
    CHECK_EQ(offsetof(ColumnEntryHead, logical_present), size_t{13});
    CHECK_EQ(offsetof(ColumnEntryHead, selection_kind), size_t{14});
    CHECK_EQ(offsetof(ColumnEntryHead, value_order), size_t{15});
    CHECK_EQ(offsetof(ColumnEntryHead, length), size_t{16});
    CHECK_EQ(offsetof(ColumnEntryHead, data_length), size_t{20});
    CHECK_EQ(offsetof(ColumnEntryHead, child_count), size_t{24});
    CHECK_EQ(offsetof(ColumnEntryHead, section_index), size_t{28});
    CHECK_EQ(offsetof(ColumnEntryHead, section_count), size_t{32});
    CHECK_EQ(offsetof(ColumnEntryHead, stats_bytes), size_t{36});
    CHECK_EQ(offsetof(ColumnEntryHead, string_slot_count), size_t{40});
    CHECK_EQ(offsetof(ColumnEntryHead, string_arena_used), size_t{48});
    CHECK_EQ(offsetof(ColumnEntryHead, string_arena_cap), size_t{56});
    CHECK_EQ(offsetof(ColumnEntryHead, string_payloads_elided), size_t{64});
    CHECK_EQ(offsetof(ColumnEntryHead, index_section_index), size_t{68});
    CHECK_EQ(offsetof(ColumnEntryHead, index_section_count), size_t{72});

    // FORMAT.md §5.3 — section directory entry
    CHECK_EQ(offsetof(SectionEntry, kind), size_t{0});
    CHECK_EQ(offsetof(SectionEntry, encoding), size_t{2});
    CHECK_EQ(offsetof(SectionEntry, offset), size_t{4});
    CHECK_EQ(offsetof(SectionEntry, stored_bytes), size_t{12});
    CHECK_EQ(offsetof(SectionEntry, plain_bytes), size_t{20});
    CHECK_EQ(offsetof(SectionEntry, checksum), size_t{28});

    // FORMAT.md §6 — logical type descriptor
    CHECK_EQ(offsetof(LogicalTypeDescriptor, kind), size_t{0});
    CHECK_EQ(offsetof(LogicalTypeDescriptor, unit), size_t{1});
    CHECK_EQ(offsetof(LogicalTypeDescriptor, offset_minutes), size_t{2});
    CHECK_EQ(offsetof(LogicalTypeDescriptor, precision), size_t{4});
    CHECK_EQ(offsetof(LogicalTypeDescriptor, scale), size_t{5});
    CHECK_EQ(offsetof(LogicalTypeDescriptor, dimension), size_t{8});

    // FORMAT.md §8 — statistics blob
    CHECK_EQ(offsetof(ColumnStatistics, flags), size_t{0});
    CHECK_EQ(offsetof(ColumnStatistics, min_ordinal), size_t{8});
    CHECK_EQ(offsetof(ColumnStatistics, max_ordinal), size_t{16});
    CHECK_EQ(offsetof(ColumnStatistics, null_count), size_t{24});
    CHECK_EQ(offsetof(ColumnStatistics, sum_low), size_t{32});
    CHECK_EQ(offsetof(ColumnStatistics, sum_high), size_t{40});
}

// FORMAT.md §7.1 and §8 give these numeric values normatively. A reader written
// from the spec hard-codes them, so they are part of the on-disk contract just
// as much as the offsets are.
static void test_enumerated_values_match_the_specification() {
    CHECK_EQ(static_cast<uint16_t>(SectionKind::kData), uint16_t{1});
    CHECK_EQ(static_cast<uint16_t>(SectionKind::kSelection), uint16_t{2});
    CHECK_EQ(static_cast<uint16_t>(SectionKind::kValidity), uint16_t{3});
    CHECK_EQ(static_cast<uint16_t>(SectionKind::kStringSlots), uint16_t{4});
    CHECK_EQ(static_cast<uint16_t>(SectionKind::kStringArena), uint16_t{5});
    CHECK_EQ(static_cast<uint16_t>(SectionKind::kBloom), uint16_t{256});
    CHECK_EQ(static_cast<uint16_t>(SectionKind::kPermutation), uint16_t{257});
    CHECK_EQ(static_cast<uint16_t>(SectionKind::kZoneMap), uint16_t{258});
    CHECK_EQ(kSectionOptionalBase, uint16_t{256});

    CHECK_EQ(static_cast<uint16_t>(Encoding::kPlain), uint16_t{0});
    CHECK_EQ(static_cast<uint16_t>(Encoding::kBitpack), uint16_t{1});
    CHECK_EQ(static_cast<uint16_t>(Encoding::kDeltaBitpack), uint16_t{2});
    CHECK_EQ(static_cast<uint16_t>(Encoding::kZstd), uint16_t{3});

    CHECK_EQ(static_cast<uint8_t>(SelectionKind::kConstant), uint8_t{0});
    CHECK_EQ(static_cast<uint8_t>(SelectionKind::kIdentity), uint8_t{1});
    CHECK_EQ(static_cast<uint8_t>(SelectionKind::kStored), uint8_t{2});

    CHECK_EQ(static_cast<uint8_t>(ValueOrder::kAsWritten), uint8_t{0});
    CHECK_EQ(static_cast<uint8_t>(ValueOrder::kAscending), uint8_t{1});

    CHECK_EQ(static_cast<uint32_t>(kStatMin), uint32_t{1u << 0});
    CHECK_EQ(static_cast<uint32_t>(kStatMax), uint32_t{1u << 1});
    CHECK_EQ(static_cast<uint32_t>(kStatNullCount), uint32_t{1u << 2});
    CHECK_EQ(static_cast<uint32_t>(kStatSum), uint32_t{1u << 3});
    CHECK_EQ(static_cast<uint32_t>(kStatRowSorted), uint32_t{1u << 4});
    CHECK_EQ(static_cast<uint32_t>(kStatRowSortedDescending), uint32_t{1u << 5});

    CHECK_EQ(static_cast<uint8_t>(Endianness::kLittle), uint8_t{0});
    CHECK_EQ(static_cast<uint8_t>(ChecksumAlgorithm::kXxh3_64), uint8_t{0});
    CHECK_EQ(kZoneMapDefaultChunkRows, uint32_t{8192});
}

static void test_checksum_is_wired_and_sensitive() {
    const char a[] = "skene";
    const char b[] = "skenf";
    const uint64_t ha = checksum_xxh3_64(a, sizeof(a) - 1);
    const uint64_t hb = checksum_xxh3_64(b, sizeof(b) - 1);
    CHECK(ha != hb);
    CHECK_EQ(ha, checksum_xxh3_64(a, sizeof(a) - 1));  // deterministic

    // A zero-length body is legal (an all-valid column has no validity section)
    // and must still produce a stable value rather than tripping the hash.
    CHECK_EQ(checksum_xxh3_64(nullptr, 0), checksum_xxh3_64("", 0));
}

int main() {
    test_draken_abi_visible_here();
    test_head_and_tail_layout();
    test_section_required_optional_split();
    test_version_window();
    test_record_layouts();
    test_offsets_match_the_specification();
    test_enumerated_values_match_the_specification();
    test_checksum_is_wired_and_sensitive();
    return skene_test::summary("test_format");
}
