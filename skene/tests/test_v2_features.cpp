// The v2 additions in isolation: the cluster spec (declared, VERIFIED,
// round-tripped, refused when false), the NDV statistic (exact under value
// ordering, KMV estimate on the string decline path), and section alignment.

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "build_vectors.h"
#include "footer_probe.h"
#include "harness.h"

#include "skene/format.h"
#include "skene/reader.h"
#include "skene/writer.h"

using namespace skene;
using namespace skene_test;

namespace {

SortKey key_asc(uint32_t ordinal) {
    SortKey key{};
    key.column_ordinal = ordinal;
    key.descending     = 0;
    key.nulls_first    = 1;   // draken's rule: NULLS FIRST ascending
    return key;
}

SortKey key_desc(uint32_t ordinal) {
    SortKey key{};
    key.column_ordinal = ordinal;
    key.descending     = 1;
    key.nulls_first    = 0;   // NULLS LAST descending
    return key;
}

// ─── Cluster spec ───────────────────────────────────────────────────────────

void test_cluster_spec_round_trips() {
    WriteOptions options;
    options.read_acceleration = true;
    options.cluster_keys = {key_asc(0)};

    FileWriter writer;
    std::vector<uint8_t> bytes;
    CHECK(writer.begin(options, &bytes).is_ok());

    auto rg1 = morsel_of({{"k", dense_column<int64_t>({1, 2, 2, 5}, DRAKEN_INT64)},
                          {"v", dense_column<int64_t>({9, 8, 7, 6}, DRAKEN_INT64)}});
    auto rg2 = morsel_of({{"k", dense_column<int64_t>({5, 6, 9, 9}, DRAKEN_INT64)},
                          {"v", dense_column<int64_t>({5, 4, 3, 2}, DRAKEN_INT64)}});
    CHECK(writer.add_row_group(rg1).is_ok());
    CHECK(writer.add_row_group(rg2).is_ok());   // seam 5 -> 5 is a legal tie
    CHECK(writer.finish().is_ok());

    FileMetadata meta;
    CHECK(read_metadata(bytes.data(), bytes.size(), &meta).is_ok());
    CHECK_EQ(meta.cluster_keys.size(), size_t{1});
    if (!meta.cluster_keys.empty()) {
        CHECK_EQ(meta.cluster_keys[0].column_ordinal, uint32_t{0});
        CHECK_EQ(meta.cluster_keys[0].descending, uint8_t{0});
        CHECK_EQ(meta.cluster_keys[0].nulls_first, uint8_t{1});
    }

    // An unclustered file says so with an EMPTY spec, not an absent record.
    auto plain = morsel_of({{"k", dense_column<int64_t>({3, 1, 2}, DRAKEN_INT64)}});
    std::vector<uint8_t> plain_bytes;
    CHECK(write_morsel(plain, WriteOptions::for_spill(), &plain_bytes).is_ok());
    FileMetadata plain_meta;
    CHECK(read_metadata(plain_bytes.data(), plain_bytes.size(), &plain_meta).is_ok());
    CHECK(plain_meta.cluster_keys.empty());
}

void test_cluster_out_of_order_rows_are_refused() {
    WriteOptions options;
    options.cluster_keys = {key_asc(0)};

    FileWriter writer;
    std::vector<uint8_t> bytes;
    CHECK(writer.begin(options, &bytes).is_ok());
    auto rg = morsel_of({{"k", dense_column<int64_t>({1, 3, 2}, DRAKEN_INT64)}});
    Status st = writer.add_row_group(rg);
    CHECK(!st.is_ok());
    CHECK(st.message().find("out of order") != std::string::npos);
}

void test_cluster_seam_violation_is_refused() {
    WriteOptions options;
    options.cluster_keys = {key_asc(0)};

    FileWriter writer;
    std::vector<uint8_t> bytes;
    CHECK(writer.begin(options, &bytes).is_ok());
    auto rg1 = morsel_of({{"k", dense_column<int64_t>({1, 2, 9}, DRAKEN_INT64)}});
    auto rg2 = morsel_of({{"k", dense_column<int64_t>({4, 5, 6}, DRAKEN_INT64)}});
    CHECK(writer.add_row_group(rg1).is_ok());
    Status st = writer.add_row_group(rg2);   // 9 -> 4 across the seam
    CHECK(!st.is_ok());
    CHECK(st.message().find("previous row group") != std::string::npos);
}

void test_cluster_descending_and_secondary_key() {
    WriteOptions options;
    options.cluster_keys = {key_desc(0), key_asc(1)};

    // Primary strictly descending except one tie, which the ascending
    // secondary must then order.
    FileWriter writer;
    std::vector<uint8_t> bytes;
    CHECK(writer.begin(options, &bytes).is_ok());
    auto ok = morsel_of({{"a", dense_column<int64_t>({9, 7, 7, 1}, DRAKEN_INT64)},
                         {"b", dense_column<int64_t>({0, 2, 5, 0}, DRAKEN_INT64)}});
    CHECK(writer.add_row_group(ok).is_ok());
    CHECK(writer.finish().is_ok());

    FileWriter writer2;
    std::vector<uint8_t> bytes2;
    CHECK(writer2.begin(options, &bytes2).is_ok());
    auto bad = morsel_of({{"a", dense_column<int64_t>({9, 7, 7, 1}, DRAKEN_INT64)},
                          {"b", dense_column<int64_t>({0, 5, 2, 0}, DRAKEN_INT64)}});
    CHECK(!writer2.add_row_group(bad).is_ok());   // tie broken the wrong way
}

void test_cluster_nulls_follow_drakens_rule() {
    // Ascending, nulls first: a leading null then values is LEGAL...
    WriteOptions options;
    options.cluster_keys = {key_asc(0)};
    {
        FileWriter writer;
        std::vector<uint8_t> bytes;
        CHECK(writer.begin(options, &bytes).is_ok());
        auto rg = morsel_of({{"k", dense_column<int64_t>({0, 1, 2}, DRAKEN_INT64,
                                                         {false, true, true})}});
        CHECK(writer.add_row_group(rg).is_ok());
        CHECK(writer.finish().is_ok());
    }
    // ...and a TRAILING null is not.
    {
        FileWriter writer;
        std::vector<uint8_t> bytes;
        CHECK(writer.begin(options, &bytes).is_ok());
        auto rg = morsel_of({{"k", dense_column<int64_t>({1, 2, 0}, DRAKEN_INT64,
                                                         {true, true, false})}});
        CHECK(!writer.add_row_group(rg).is_ok());
    }
    // A SortKey that contradicts draken's null rule is rejected up front.
    {
        WriteOptions wrong;
        SortKey key = key_asc(0);
        key.nulls_first = 0;   // ascending demands nulls FIRST
        wrong.cluster_keys = {key};
        FileWriter writer;
        std::vector<uint8_t> bytes;
        CHECK(!writer.begin(wrong, &bytes).is_ok());
    }
}

void test_cluster_string_keys_compare_full_bytes() {
    // Two strings sharing an 8-byte prefix have EQUAL ordinals — only the full
    // byte comparison can order them, and it must.
    WriteOptions options;
    options.cluster_keys = {key_asc(0)};
    {
        FileWriter writer;
        std::vector<uint8_t> bytes;
        CHECK(writer.begin(options, &bytes).is_ok());
        auto rg = morsel_of({{"s", string_column({"prefix-shared-aaa",
                                                  "prefix-shared-bbb"})}});
        CHECK(writer.add_row_group(rg).is_ok());
        CHECK(writer.finish().is_ok());
    }
    {
        FileWriter writer;
        std::vector<uint8_t> bytes;
        CHECK(writer.begin(options, &bytes).is_ok());
        auto rg = morsel_of({{"s", string_column({"prefix-shared-bbb",
                                                  "prefix-shared-aaa"})}});
        CHECK(!writer.add_row_group(rg).is_ok());
    }
}

void test_cluster_ineligible_key_columns_are_refused() {
    // A length-only string column has no comparable values.
    {
        WriteOptions options;
        options.cluster_keys = {key_asc(0)};
        FileWriter writer;
        std::vector<uint8_t> bytes;
        CHECK(writer.begin(options, &bytes).is_ok());
        auto rg = morsel_of({{"s", string_column({"aaaaaaaaaaaaaaaa",
                                                  "bbbbbbbbbbbbbbbb"},
                                                 DRAKEN_VARCHAR, {}, /*elide=*/true)}});
        CHECK(!writer.add_row_group(rg).is_ok());
    }
    // An ordinal past the schema is a key no reader could resolve.
    {
        WriteOptions options;
        options.cluster_keys = {key_asc(7)};
        FileWriter writer;
        std::vector<uint8_t> bytes;
        CHECK(writer.begin(options, &bytes).is_ok());
        auto rg = morsel_of({{"k", dense_column<int64_t>({1, 2}, DRAKEN_INT64)}});
        CHECK(!writer.add_row_group(rg).is_ok());
    }
}

// ─── NDV statistics ─────────────────────────────────────────────────────────

void test_ndv_exact_under_value_ordering() {
    // 40 distinct strings over 200 rows: ordering accepts and deduplicates, so
    // the NDV is exact by construction.
    std::vector<std::string> values;
    for (int i = 0; i < 200; ++i)
        values.push_back("value-padding-padding-" + std::to_string(i % 40));

    auto m = morsel_of({{"s", string_column(values)}});
    std::vector<uint8_t> bytes;
    WriteOptions options;
    options.read_acceleration = true;
    CHECK(write_morsel(m, options, &bytes).is_ok());

    RowGroupMetadata rg;
    CHECK(read_row_group_metadata(bytes.data(), bytes.size(), 0, &rg).is_ok());
    const ColumnStatistics& stats = rg.columns[0].statistics;
    CHECK(rg.columns[0].has_statistics);
    CHECK((stats.flags & kStatNdv) != 0);
    CHECK((stats.flags & kStatNdvExact) != 0);
    CHECK_EQ(stats.ndv, uint64_t{40});

    // The same numbers are reachable from the FILE footer alone.
    FileMetadata meta;
    CHECK(read_metadata(bytes.data(), bytes.size(), &meta).is_ok());
    CHECK(meta.row_groups[0].column_statistics[0].present);
    CHECK_EQ(meta.row_groups[0].column_statistics[0].statistics.ndv, uint64_t{40});
}

void test_ndv_estimate_on_the_string_decline_path() {
    // Near-unique strings past the sketch's sample floor: ordering declines,
    // and the KMV estimate it measured on the way out is written down.
    constexpr uint32_t rows = 8192;
    std::vector<std::string> values;
    values.reserve(rows);
    for (uint32_t i = 0; i < rows; ++i)
        values.push_back("unique-string-value-padding-" + std::to_string(i * 7919u));

    auto m = morsel_of({{"s", string_column(values)}});
    std::vector<uint8_t> bytes;
    WriteOptions options;
    options.read_acceleration = true;
    CHECK(write_morsel(m, options, &bytes).is_ok());

    RowGroupMetadata rg;
    CHECK(read_row_group_metadata(bytes.data(), bytes.size(), 0, &rg).is_ok());
    const ColumnStatistics& stats = rg.columns[0].statistics;
    CHECK((stats.flags & kStatNdv) != 0);
    CHECK((stats.flags & kStatNdvExact) == 0);   // an estimate, and says so
    // KMV at K=1024 is ±~3%; the bound here is loose enough to never flake and
    // tight enough to catch a broken estimator (0, or the row count doubled).
    CHECK(stats.ndv > rows - rows / 5);
    CHECK(stats.ndv < rows + rows / 5);
}

// ─── Alignment ──────────────────────────────────────────────────────────────

void test_sections_start_aligned_by_default() {
    auto m = morsel_of({
        {"n", dense_column<int64_t>({1, 2, 3, 4}, DRAKEN_INT64)},
        {"s", string_column({"alpha", "a longer value past twelve bytes",
                             "third", "fourth entry, also past twelve"})},
    });
    std::vector<uint8_t> bytes;
    CHECK(write_morsel(m, WriteOptions::for_spill(), &bytes).is_ok());

    size_t footer_at = 0, footer_len = 0;
    CHECK(row_group_footer_extent(bytes, 0, &footer_at, &footer_len));
    RowGroupFooterHeader fh;
    std::memcpy(&fh, bytes.data() + footer_at, sizeof(fh));
    const size_t sections_at = footer_at + footer_len
        - static_cast<size_t>(fh.section_count) * sizeof(SectionEntry);
    for (uint32_t i = 0; i < fh.section_count; ++i) {
        SectionEntry entry;
        std::memcpy(&entry, bytes.data() + sections_at + i * sizeof(entry),
                    sizeof(entry));
        ++skene_test::g_checks;
        if (entry.offset % kSectionAlign != 0)
            skene_test::report(__FILE__, __LINE__, "section offset misaligned",
                               "kind " + std::to_string(entry.kind) + " at "
                                   + std::to_string(entry.offset));
    }

    CxxMorsel out;
    CHECK(read_morsel(bytes.data(), bytes.size(), 0, &out).is_ok());
}

}  // namespace

int main() {
    test_cluster_spec_round_trips();
    test_cluster_out_of_order_rows_are_refused();
    test_cluster_seam_violation_is_refused();
    test_cluster_descending_and_secondary_key();
    test_cluster_nulls_follow_drakens_rule();
    test_cluster_string_keys_compare_full_bytes();
    test_cluster_ineligible_key_columns_are_refused();
    test_ndv_exact_under_value_ordering();
    test_ndv_estimate_on_the_string_decline_path();
    test_sections_start_aligned_by_default();
    return skene_test::summary("test_v2_features");
}
