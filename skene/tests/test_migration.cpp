// The v1 -> v2 migration window, exercised against COMMITTED v1 files.
//
// The fixtures under tests/fixtures/v1/ were written by the LAST v1 writer
// (tree dc5c7aaf, 2026-08-20, dev/skene_gen_v1_fixtures.cpp) and are the only
// v1 bytes this tree can ever produce — the writer moved to v2. Losing them
// severs the migration chain's test coverage, so they are committed artifacts,
// not generated ones.
//
// What this suite pins:
//   - the retained v1 reader still reads real v1 files through the dispatch
//   - migrate_file rewrites them as v2, LOGICALLY identical row for row
//   - provenance (uuid, created_at, writer_tag) is carried, not reissued
//   - a v2 file is refused ("nothing to migrate"), as is a posture that
//     tries to supply its own provenance
//
// Equality is LOGICAL, not physical: migration re-runs value ordering and the
// codec, so buffers, flags and selection shapes may legitimately differ. What
// must not differ is what any query can observe — types, logical descriptors,
// row counts, null positions, and the value of every row.

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

const char* kFixtures[] = {
    "tests/fixtures/v1/v1_spill.skene",
    "tests/fixtures/v1/v1_accel_none.skene",
    "tests/fixtures/v1/v1_accel_lz4.skene",
    "tests/fixtures/v1/v1_accel_zstd7.skene",
};

constexpr uint64_t kFixtureRows      = 6000;  // 2 row groups x 3000
constexpr uint32_t kFixtureRowGroups = 2;
constexpr size_t   kFixtureColumns   = 14;

bool row_valid(const DrakenVector& v, uint32_t row) {
    if (v.validity == nullptr) return true;
    return (v.validity[row >> 3] & (1u << (row & 7u))) != 0;
}

void compare_logical(const DrakenVector& a, const DrakenVector& b,
                     const std::string& where) {
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
            // Offsets are dense over LOGICAL rows; elements compared via the
            // caller (children are separate vectors) — here compare the shape.
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

        const size_t itemsize = draken_type_fixed_itemsize(a.type);
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
    compare_logical(a.view, b.view, where);

    // Logical descriptors are interned process-wide, so pointer equality IS
    // descriptor equality.
    ++skene_test::g_checks;
    if (a.own->logical_type != b.own->logical_type)
        skene_test::report(__FILE__, __LINE__, where.c_str(),
                           "logical type descriptor differs");

    const bool child_a = a.own->child_owner != nullptr;
    const bool child_b = b.own->child_owner != nullptr;
    CHECK_EQ(child_a, child_b);
    if (child_a && child_b)
        compare_logical(a.own->child_owner->vec, b.own->child_owner->vec,
                        where + ".element");
}

void test_fixture_reads_and_migrates(const char* path) {
    std::vector<uint8_t> v1_bytes;
    Status st = read_file(path, &v1_bytes);
    ++skene_test::g_checks;
    if (!st.is_ok()) {
        skene_test::report(__FILE__, __LINE__, path,
                           "fixture missing: " + st.message());
        return;
    }

    uint16_t version = 0;
    CHECK(probe_version(v1_bytes.data(), kProbeBytes, &version).is_ok());
    CHECK_EQ(version, uint16_t{1});

    // ── the retained v1 reader, through the dispatch ──
    FileMetadata v1_meta;
    CHECK(read_metadata(v1_bytes.data(), v1_bytes.size(), &v1_meta).is_ok());
    CHECK_EQ(v1_meta.version, uint16_t{1});
    CHECK_EQ(v1_meta.row_count, kFixtureRows);
    CHECK_EQ(v1_meta.row_groups.size(), size_t{kFixtureRowGroups});
    CHECK_EQ(v1_meta.columns.size(), kFixtureColumns);
    CHECK(v1_meta.cluster_keys.empty());  // v1 had no way to say otherwise

    // ── migrate ──
    std::vector<uint8_t> v2_bytes;
    st = migrate_file(v1_bytes.data(), v1_bytes.size(),
                      WriteOptions::for_fast_reads(), &v2_bytes);
    ++skene_test::g_checks;
    if (!st.is_ok()) {
        skene_test::report(__FILE__, __LINE__, path,
                           "migrate failed: " + st.message());
        return;
    }

    CHECK(probe_version(v2_bytes.data(), kProbeBytes, &version).is_ok());
    CHECK_EQ(version, uint16_t{2});

    FileMetadata v2_meta;
    CHECK(read_metadata(v2_bytes.data(), v2_bytes.size(), &v2_meta).is_ok());
    CHECK_EQ(v2_meta.version, uint16_t{2});
    CHECK_EQ(v2_meta.row_count, v1_meta.row_count);
    CHECK_EQ(v2_meta.columns.size(), v1_meta.columns.size());

    // Provenance carried, not reissued.
    CHECK(v2_meta.writer_tag == v1_meta.writer_tag);
    CHECK_EQ(v2_meta.created_at_unix_us, v1_meta.created_at_unix_us);
    CHECK_EQ(std::memcmp(v2_meta.file_uuid, v1_meta.file_uuid, 16), 0);
    for (size_t i = 0; i < v1_meta.columns.size(); ++i)
        CHECK_EQ(v2_meta.columns[i].field_id, v1_meta.columns[i].field_id);

    // ── row-for-row logical equality, every row group ──
    for (uint32_t rg = 0; rg < kFixtureRowGroups; ++rg) {
        CxxMorsel from_v1, from_v2;
        CHECK(read_morsel(v1_bytes.data(), v1_bytes.size(), rg, &from_v1).is_ok());
        CHECK(read_morsel(v2_bytes.data(), v2_bytes.size(), rg, &from_v2).is_ok());
        CHECK_EQ(from_v1.columns.size(), from_v2.columns.size());
        for (size_t c = 0; c < from_v1.columns.size(); ++c) {
            CHECK(from_v1.names[c] == from_v2.names[c]);
            compare_column(from_v1.columns[c], from_v2.columns[c],
                           std::string(path) + " rg" + std::to_string(rg)
                               + " " + from_v1.names[c]);
        }
    }

    // ── a v2 file is refused: there is nothing to migrate ──
    std::vector<uint8_t> again;
    st = migrate_file(v2_bytes.data(), v2_bytes.size(),
                      WriteOptions::for_fast_reads(), &again);
    CHECK(!st.is_ok());

    // ── provenance on the posture is refused ──
    WriteOptions bad = WriteOptions::for_fast_reads();
    bad.writer_tag = "impostor";
    st = migrate_file(v1_bytes.data(), v1_bytes.size(), bad, &again);
    CHECK(!st.is_ok());
}

}  // namespace

int main() {
    for (const char* path : kFixtures) test_fixture_reads_and_migrates(path);
    return skene_test::summary("test_migration");
}
