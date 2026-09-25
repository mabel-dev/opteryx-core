// The v2 -> v3 migration window, exercised against COMMITTED v2 files.
//
// The fixtures under tests/fixtures/v2/ were written by the LAST v2 writer
// (2026-09-24, a one-shot generator retired with the bump) and are the only v2
// bytes this tree will ever have — the writer moved to v3. Losing them severs the
// migration chain's test coverage, so they are committed artifacts, not
// generated ones.
//
// What this suite pins:
//   - the retained v2 reader still reads real v2 files through the dispatch,
//     and reports their per-row-group sketches as one family-1 file sketch
//   - migrate_file rewrites them as v3, LOGICALLY identical row for row
//   - provenance (uuid, created_at, writer_tag, field ids) is carried
//   - sketches are RECOMPUTED in draken's Vector.hash() family, never carried
//   - a v3 file reads the same whole-buffer and ranged
//   - a v3 file is refused ("nothing to migrate"), as is a posture that
//     tries to supply its own provenance
//
// Equality is LOGICAL, not physical: migration re-runs value ordering and the
// codec, so buffers, flags and selection shapes may legitimately differ. What
// must not differ is what any query can observe — types, logical descriptors,
// row counts, null positions, and the value of every row.

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "harness.h"

#include "skene/file_io.h"
#include "skene/migrate.h"
#include "skene/probe.h"
#include "skene/reader.h"
#include "skene/writer.h"

#include "core/buffers.h"
#include "core/string_slot.h"
#include "logical_type.h"

using namespace skene;

namespace {

struct Fixture {
    const char* path;
    uint32_t    rows_per_group;
    bool        accelerated;   // written with read acceleration: carries sketches
    bool        clustered;     // carries a cluster spec on column 0
};

const Fixture kFixtures[] = {
    {"tests/fixtures/v2/v2_spill.skene",             3000,  false, false},
    {"tests/fixtures/v2/v2_accel_none.skene",        3000,  true,  false},
    {"tests/fixtures/v2/v2_accel_lz4.skene",         3000,  true,  false},
    {"tests/fixtures/v2/v2_accel_zstd7.skene",       3000,  true,  false},
    {"tests/fixtures/v2/v2_clustered_zonemap.skene", 10000, true,  true},
};

constexpr uint32_t kFixtureRowGroups = 2;
constexpr size_t   kFixtureColumns   = 16;

bool row_valid(const DrakenVector& v, uint32_t row) {
    if (v.validity == nullptr) return true;
    return (v.validity[row >> 3] & (1u << (row & 7u))) != 0;
}

void compare_logical(const DrakenVector& a, const DrakenVector& b,
                     const LogicalType* logical, const std::string& where) {
    CHECK_EQ(static_cast<int>(a.type), static_cast<int>(b.type));
    CHECK_EQ(a.length, b.length);
    if (a.type != b.type || a.length != b.length) return;

    for (uint32_t row = 0; row < a.length; ++row) {
        const bool va = row_valid(a, row);
        const bool vb = row_valid(b, row);
        ++skene_test::g_checks;
        if (va != vb) {
            skene_test::report(__FILE__, __LINE__, where.c_str(),
                               "null position differs at row " + std::to_string(row));
            return;
        }
        if (!va) continue;

        const uint32_t ca = a.selection[row];
        const uint32_t cb = b.selection[row];

        if (a.type == DRAKEN_NULL) continue;

        if (draken_type_is_string_storage(a.type)) {
            const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(a.data);
            const DrakenStringArena* sb = static_cast<const DrakenStringArena*>(b.data);
            const DrakenStringSlot* xa = &sa->slots[ca];
            const DrakenStringSlot* xb = &sb->slots[cb];
            ++skene_test::g_checks;
            if (str_length(xa) != str_length(xb)) {
                skene_test::report(__FILE__, __LINE__, where.c_str(),
                                   "string length differs at row " + std::to_string(row));
                return;
            }
            CHECK_EQ(sa->payloads_elided, sb->payloads_elided);
            if (sa->payloads_elided) continue;  // lengths are the whole value
            const uint8_t* pa = str_is_inline(xa) ? xa->inl.data
                                                  : sa->arena + xa->ext.arena_offset;
            const uint8_t* pb = str_is_inline(xb) ? xb->inl.data
                                                  : sb->arena + xb->ext.arena_offset;
            ++skene_test::g_checks;
            if (std::memcmp(pa, pb, str_length(xa)) != 0) {
                skene_test::report(__FILE__, __LINE__, where.c_str(),
                                   "string bytes differ at row " + std::to_string(row));
                return;
            }
            continue;
        }

        if (a.type == DRAKEN_BOOL) {
            const uint8_t* da = static_cast<const uint8_t*>(a.data);
            const uint8_t* db = static_cast<const uint8_t*>(b.data);
            const int bit_a = (da[ca >> 3] >> (ca & 7u)) & 1;
            const int bit_b = (db[cb >> 3] >> (cb & 7u)) & 1;
            ++skene_test::g_checks;
            if (bit_a != bit_b) {
                skene_test::report(__FILE__, __LINE__, where.c_str(),
                                   "bool differs at row " + std::to_string(row));
                return;
            }
            continue;
        }

        if (a.type == DRAKEN_ARRAY) {
            // Offsets are dense over LOGICAL rows; the elements are compared as
            // the child vector — here compare each row's width.
            const int32_t* oa = static_cast<const int32_t*>(a.data);
            const int32_t* ob = static_cast<const int32_t*>(b.data);
            ++skene_test::g_checks;
            if (oa[row + 1] - oa[row] != ob[row + 1] - ob[row]) {
                skene_test::report(__FILE__, __LINE__, where.c_str(),
                                   "array row width differs at row " + std::to_string(row));
                return;
            }
            continue;
        }

        // VECTOR_FP16's width comes from its descriptor's dimension.
        const size_t itemsize = draken_type_itemsize(a.type, logical);
        ++skene_test::g_checks;
        if (itemsize == 0) {
            skene_test::report(__FILE__, __LINE__, where.c_str(),
                               "no fixed width for type in fixture");
            return;
        }
        const uint8_t* da = static_cast<const uint8_t*>(a.data) + ca * itemsize;
        const uint8_t* db = static_cast<const uint8_t*>(b.data) + cb * itemsize;
        ++skene_test::g_checks;
        if (std::memcmp(da, db, itemsize) != 0) {
            skene_test::report(__FILE__, __LINE__, where.c_str(),
                               "value differs at row " + std::to_string(row));
            return;
        }
    }
}

void compare_column(const CxxColumn& a, const CxxColumn& b, const std::string& where) {
    // Logical descriptors are interned process-wide, so pointer equality IS
    // descriptor equality.
    ++skene_test::g_checks;
    if (a.own->logical_type != b.own->logical_type)
        skene_test::report(__FILE__, __LINE__, where.c_str(),
                           "logical type descriptor differs");
    compare_logical(a.view, b.view, a.own->logical_type, where);

    const bool child_a = a.own->child_owner != nullptr;
    const bool child_b = b.own->child_owner != nullptr;
    CHECK_EQ(child_a, child_b);
    if (child_a && child_b)
        compare_logical(a.own->child_owner->vec, b.own->child_owner->vec,
                        a.own->child_owner->logical_type, where + ".element");
}

void compare_morsels(const CxxMorsel& a, const CxxMorsel& b, const std::string& where) {
    CHECK_EQ(a.columns.size(), b.columns.size());
    if (a.columns.size() != b.columns.size()) return;
    for (size_t c = 0; c < a.columns.size(); ++c) {
        CHECK(a.names[c] == b.names[c]);
        compare_column(a.columns[c], b.columns[c], where + " " + a.names[c]);
    }
}

// Reads every row group of a v3 file through the RANGED path — tail, footer,
// planned directory ranges, planned chunk ranges — each range a separate
// buffer, and checks it against the whole-buffer read.
void check_ranged_matches_whole(const std::vector<uint8_t>& bytes,
                                const std::string& where) {
    const uint64_t size = bytes.size();
    uint64_t footer_offset = 0, footer_bytes = 0;
    CHECK(footer_extent(bytes.data() + size - kFileTailBytes, kFileTailBytes, size,
                        &footer_offset, &footer_bytes).is_ok());
    FileReader ranged;
    Status st = open_reader_ranged(bytes.data() + size - kFileTailBytes, kFileTailBytes,
                                   bytes.data() + footer_offset,
                                   static_cast<size_t>(footer_bytes), footer_offset, size,
                                   &ranged);
    ++skene_test::g_checks;
    if (!st.is_ok()) {
        skene_test::report(__FILE__, __LINE__, where.c_str(),
                           "open_reader_ranged failed: " + st.message());
        return;
    }

    // Each planned range is COPIED into its own buffer: the reader must never
    // reach past what it was handed.
    auto fetch = [&](const std::vector<ByteRange>& plan,
                     std::vector<std::vector<uint8_t>>* store,
                     std::vector<FetchedRange>* out) {
        store->clear();
        out->clear();
        store->reserve(plan.size());
        for (const ByteRange& r : plan) {
            store->emplace_back(bytes.begin() + static_cast<long>(r.offset),
                                bytes.begin() + static_cast<long>(r.offset + r.bytes));
            out->push_back(FetchedRange{r.offset, r.bytes, store->back().data()});
        }
    };

    std::vector<ByteRange> plan;
    std::vector<std::vector<uint8_t>> directory_store;
    std::vector<FetchedRange> directory_ranges;
    CHECK(plan_directory_fetch(ranged, {}, false, FetchPolicy{}, &plan).is_ok());
    fetch(plan, &directory_store, &directory_ranges);
    st = attach_directories(&ranged, {}, directory_ranges);
    ++skene_test::g_checks;
    if (!st.is_ok()) {
        skene_test::report(__FILE__, __LINE__, where.c_str(),
                           "attach_directories failed: " + st.message());
        return;
    }

    FileReader whole;
    CHECK(open_reader(bytes.data(), bytes.size(), &whole).is_ok());
    for (uint32_t rg = 0; rg < whole.metadata().row_groups.size(); ++rg) {
        std::vector<std::vector<uint8_t>> chunk_store;
        std::vector<FetchedRange> chunk_ranges;
        CHECK(plan_fetch(ranged, {}, {rg}, FetchPolicy{}, &plan).is_ok());
        fetch(plan, &chunk_store, &chunk_ranges);
        CxxMorsel from_ranges, from_whole;
        st = read_morsel(ranged, rg, ReadOptions(), chunk_ranges, &from_ranges);
        ++skene_test::g_checks;
        if (!st.is_ok()) {
            skene_test::report(__FILE__, __LINE__, where.c_str(),
                               "ranged read_morsel failed: " + st.message());
            return;
        }
        CHECK(read_morsel(whole, rg, ReadOptions(), &from_whole).is_ok());
        compare_morsels(from_whole, from_ranges, where + " ranged rg" + std::to_string(rg));
    }

    // The engine's open: each column's directory fetched THROUGH block 0, and
    // block 0's row groups decoded from those ranges alone — no plan_fetch.
    FileReader merged;
    CHECK(open_reader_ranged(bytes.data() + size - kFileTailBytes, kFileTailBytes,
                             bytes.data() + footer_offset,
                             static_cast<size_t>(footer_bytes), footer_offset, size,
                             &merged).is_ok());
    std::vector<std::vector<uint8_t>> merged_store;
    std::vector<FetchedRange> merged_ranges;
    CHECK(plan_directory_fetch(merged, {}, true, FetchPolicy{}, &plan).is_ok());
    fetch(plan, &merged_store, &merged_ranges);
    st = attach_directories(&merged, {}, merged_ranges);
    ++skene_test::g_checks;
    if (!st.is_ok()) {
        skene_test::report(__FILE__, __LINE__, where.c_str(),
                           "attach_directories through block 0 failed: " + st.message());
        return;
    }
    const uint32_t first_block_end = std::min<uint32_t>(
        merged.metadata().block_row_groups,
        static_cast<uint32_t>(merged.metadata().row_groups.size()));
    for (uint32_t rg = 0; rg < first_block_end; ++rg) {
        CxxMorsel from_merged, from_whole;
        st = read_morsel(merged, rg, ReadOptions(), merged_ranges, &from_merged);
        ++skene_test::g_checks;
        if (!st.is_ok()) {
            skene_test::report(__FILE__, __LINE__, where.c_str(),
                               "block-0 read from directory ranges failed: " + st.message());
            return;
        }
        CHECK(read_morsel(whole, rg, ReadOptions(), &from_whole).is_ok());
        compare_morsels(from_whole, from_merged,
                        where + " through-block-0 rg" + std::to_string(rg));
    }
}

void test_fixture_reads_and_migrates(const Fixture& fixture) {
    const char* path = fixture.path;
    std::vector<uint8_t> v2_bytes;
    Status st = read_file(path, &v2_bytes);
    ++skene_test::g_checks;
    if (!st.is_ok()) {
        skene_test::report(__FILE__, __LINE__, path, "fixture missing: " + st.message());
        return;
    }

    uint16_t version = 0;
    CHECK(probe_version(v2_bytes.data(), kProbeBytes, &version).is_ok());
    CHECK_EQ(version, uint16_t{2});

    // ── the retained v2 reader, through the dispatch ──
    FileMetadata v2_meta;
    CHECK(read_metadata(v2_bytes.data(), v2_bytes.size(), &v2_meta).is_ok());
    CHECK_EQ(v2_meta.version, uint16_t{2});
    CHECK_EQ(v2_meta.row_count, uint64_t{fixture.rows_per_group} * kFixtureRowGroups);
    CHECK_EQ(v2_meta.row_groups.size(), size_t{kFixtureRowGroups});
    CHECK_EQ(v2_meta.columns.size(), kFixtureColumns);
    CHECK_EQ(v2_meta.block_row_groups, 0u);   // v2 recorded none
    CHECK_EQ(v2_meta.cluster_keys.empty(), !fixture.clustered);

    // v2 per-row-group sketches reported as their union, family 1.
    bool any_v2_sketch = false;
    for (const ColumnSketch& sketch : v2_meta.sketches) {
        if (!sketch.present()) continue;
        any_v2_sketch = true;
        CHECK_EQ(sketch.hash_family, kSketchFamilyXxh3Value);
    }
    CHECK_EQ(any_v2_sketch, fixture.accelerated);

    // ── migrate ──
    WriteOptions posture = WriteOptions::for_fast_reads();
    if (fixture.clustered) posture.cluster_keys.push_back(SortKey{0u, 0u, 1u, 0u});
    std::vector<uint8_t> v3_bytes;
    st = migrate_file(v2_bytes.data(), v2_bytes.size(), posture, &v3_bytes);
    ++skene_test::g_checks;
    if (!st.is_ok()) {
        skene_test::report(__FILE__, __LINE__, path, "migrate failed: " + st.message());
        return;
    }

    CHECK(probe_version(v3_bytes.data(), kProbeBytes, &version).is_ok());
    CHECK_EQ(version, uint16_t{3});

    FileMetadata v3_meta;
    CHECK(read_metadata(v3_bytes.data(), v3_bytes.size(), &v3_meta).is_ok());
    CHECK_EQ(v3_meta.version, uint16_t{3});
    CHECK_EQ(v3_meta.row_count, v2_meta.row_count);
    CHECK_EQ(v3_meta.columns.size(), v2_meta.columns.size());
    CHECK_EQ(v3_meta.block_row_groups, 4u);
    CHECK_EQ(v3_meta.cluster_keys.size(), fixture.clustered ? size_t{1} : size_t{0});

    // Provenance carried, not reissued.
    CHECK(v3_meta.writer_tag == v2_meta.writer_tag);
    CHECK_EQ(v3_meta.created_at_unix_us, v2_meta.created_at_unix_us);
    CHECK_EQ(std::memcmp(v3_meta.file_uuid, v2_meta.file_uuid, 16), 0);
    for (size_t i = 0; i < v2_meta.columns.size(); ++i)
        CHECK_EQ(v3_meta.columns[i].field_id, v2_meta.columns[i].field_id);

    // Sketches RECOMPUTED in draken's family — the posture reads accelerated,
    // so every hashable column has one whatever the source carried.
    size_t family2 = 0;
    for (const ColumnSketch& sketch : v3_meta.sketches) {
        if (!sketch.present()) continue;
        CHECK_EQ(sketch.hash_family, kSketchFamilyDrakenVectorHash);
        ++family2;
    }
    CHECK(family2 > 0);

    // ── row-for-row logical equality, every row group ──
    for (uint32_t rg = 0; rg < kFixtureRowGroups; ++rg) {
        CxxMorsel from_v2, from_v3;
        CHECK(read_morsel(v2_bytes.data(), v2_bytes.size(), rg, &from_v2).is_ok());
        CHECK(read_morsel(v3_bytes.data(), v3_bytes.size(), rg, &from_v3).is_ok());
        compare_morsels(from_v2, from_v3, std::string(path) + " rg" + std::to_string(rg));
    }

    check_ranged_matches_whole(v3_bytes, path);

    // ── a v3 file is refused: there is nothing to migrate ──
    std::vector<uint8_t> again;
    st = migrate_file(v3_bytes.data(), v3_bytes.size(), WriteOptions::for_fast_reads(),
                      &again);
    CHECK(!st.is_ok());

    // ── provenance on the posture is refused ──
    WriteOptions bad = WriteOptions::for_fast_reads();
    bad.writer_tag = "impostor";
    st = migrate_file(v2_bytes.data(), v2_bytes.size(), bad, &again);
    CHECK(!st.is_ok());

    // ── a v2 file cannot be opened ranged ──
    uint64_t footer_offset = 0, footer_bytes = 0;
    CHECK(footer_extent(v2_bytes.data() + v2_bytes.size() - kFileTailBytes,
                        kFileTailBytes, v2_bytes.size(), &footer_offset,
                        &footer_bytes).is_ok());
    FileReader refused;
    st = open_reader_ranged(v2_bytes.data() + v2_bytes.size() - kFileTailBytes,
                            kFileTailBytes, v2_bytes.data() + footer_offset,
                            static_cast<size_t>(footer_bytes), footer_offset,
                            v2_bytes.size(), &refused);
    CHECK(!st.is_ok());
    CHECK(st.code() == Code::kUnsupportedVersion);
}

}  // namespace

int main() {
    for (const Fixture& fixture : kFixtures) test_fixture_reads_and_migrates(fixture);
    return skene_test::summary("test_migration");
}
