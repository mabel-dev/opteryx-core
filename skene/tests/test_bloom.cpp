// Bloom filters (FORMAT.md §9.1).
//
// A bloom filter's ONE inviolable property is that it never says "absent" about
// a value that is present. False positives are its price; a false negative
// silently drops rows from an answer. So the central test inserts every value
// and asserts none is ever rejected.

#include <cstring>
#include <string>
#include <vector>

#include "bloom.h"
#include "build_vectors.h"
#include "harness.h"
#include "skene/reader.h"
#include "skene/writer.h"

using namespace skene;
using namespace skene_test;

static WriteOptions with_bloom(std::vector<std::string> columns) {
    WriteOptions options;
    options.read_acceleration = true;
    options.bloom_columns = std::move(columns);
    return options;
}

static std::vector<uint8_t> write_or_die(const CxxMorsel& m, const WriteOptions& o) {
    std::vector<uint8_t> bytes;
    Status st = write_morsel(m, o, &bytes);
    if (!st.is_ok()) {
        std::fprintf(stderr, "  write failed: %s\n", st.message().c_str());
        ++skene_test::g_failures;
    }
    return bytes;
}

// ─── The property that matters ──────────────────────────────────────────────

static void test_never_rejects_a_present_value() {
    // Bloom is string-only now (architect ruling), so the property is
    // exercised on a string column — an int64 column no longer gets a filter
    // at all (see test_non_string_types_get_no_filter).
    std::vector<std::string> values(5000);
    for (size_t i = 0; i < values.size(); ++i)
        values[i] = "n-" + std::to_string(static_cast<int64_t>(i * 7919) - 1000000);

    auto in = morsel_of({{"n", string_column(values)}});
    auto bytes = write_or_die(in, with_bloom({"n"}));

    RowGroupMetadata meta;
    CHECK(read_row_group_metadata(bytes.data(), bytes.size(), 0, &meta).is_ok());
    CHECK(!meta.columns[0].bloom.empty());

    // EVERY present value must probe positive. A single false negative would
    // mean a query silently returning fewer rows than it should.
    size_t rejected = 0;
    for (const std::string& value : values) {
        bool may = false;
        CHECK(bloom_may_contain(meta.columns[0], value.data(),
                                static_cast<uint32_t>(value.size()), &may).is_ok());
        if (!may) ++rejected;
    }
    ++skene_test::g_checks;
    if (rejected != 0)
        skene_test::report(__FILE__, __LINE__, "false negative",
                           std::to_string(rejected) + " present values were rejected");
}

static void test_rejects_most_absent_values() {
    std::vector<std::string> values(2000);
    for (size_t i = 0; i < values.size(); ++i)
        values[i] = "even-" + std::to_string(static_cast<int64_t>(i) * 2);

    auto in = morsel_of({{"even", string_column(values)}});
    auto bytes = write_or_die(in, with_bloom({"even"}));

    RowGroupMetadata meta;
    CHECK(read_row_group_metadata(bytes.data(), bytes.size(), 0, &meta).is_ok());

    // Odd values are all absent, so every acceptance is a false positive.
    //
    // Asserted against the CONFIGURED rate, not an order of magnitude above it.
    // Sizing is calibrated so a requested rate is the delivered rate (see
    // bits_per_key_for in bloom.cpp), which is only true while something checks
    // it — a filter quietly missing its stated rate misleads every caller
    // reasoning with that number. The allowance is 2x for sampling noise at this
    // sample size, still far tighter than the 10x it replaces.
    size_t accepted = 0;
    for (int64_t i = 0; i < 2000; ++i) {
        const std::string absent = "even-" + std::to_string(i * 2 + 1);
        bool may = false;
        CHECK(bloom_may_contain(meta.columns[0], absent.data(),
                                static_cast<uint32_t>(absent.size()), &may).is_ok());
        if (may) ++accepted;
    }
    ++skene_test::g_checks;
    const size_t allowed =
        static_cast<size_t>(2000 * skene::kDefaultFalsePositiveRate * 2.0);
    if (accepted > allowed)
        skene_test::report(__FILE__, __LINE__, "filter misses its configured rate",
                           std::to_string(accepted) + "/2000 absent values accepted, "
                           "allowed " + std::to_string(allowed));
}

static void test_strings() {
    std::vector<std::string> values;
    for (int i = 0; i < 3000; ++i)
        values.push_back("customer-" + std::to_string(i * 31));

    auto in = morsel_of({{"name", string_column(values)}});
    auto bytes = write_or_die(in, with_bloom({"name"}));

    RowGroupMetadata meta;
    CHECK(read_row_group_metadata(bytes.data(), bytes.size(), 0, &meta).is_ok());
    CHECK(!meta.columns[0].bloom.empty());

    // Content hashing, so inline and arena-backed slots behave identically —
    // a value's storage form must not change whether it is found.
    size_t rejected = 0;
    for (const std::string& value : values) {
        bool may = false;
        CHECK(bloom_may_contain(meta.columns[0], value.data(),
                                static_cast<uint32_t>(value.size()), &may).is_ok());
        if (!may) ++rejected;
    }
    ++skene_test::g_checks;
    if (rejected != 0)
        skene_test::report(__FILE__, __LINE__, "string false negative",
                           std::to_string(rejected) + " present values rejected");

    bool may = true;
    const std::string absent = "definitely-not-in-this-column-at-all";
    CHECK(bloom_may_contain(meta.columns[0], absent.data(),
                            static_cast<uint32_t>(absent.size()), &may).is_ok());
    CHECK(!may);
}

// ─── Policy ─────────────────────────────────────────────────────────────────

static void test_only_requested_columns_get_filters() {
    auto in = morsel_of({
        {"a", string_column({"1", "2", "3", "4"})},
        {"b", string_column({"5", "6", "7", "8"})},
    });
    auto bytes = write_or_die(in, with_bloom({"b"}));

    RowGroupMetadata meta;
    CHECK(read_row_group_metadata(bytes.data(), bytes.size(), 0, &meta).is_ok());
    CHECK(meta.columns[0].bloom.empty());    // not requested
    CHECK(!meta.columns[1].bloom.empty());

    // A column with no filter must answer "cannot rule out" rather than
    // "absent": a missing accelerator can cost speed, never rows.
    const std::string value = "999";
    bool may = false;
    CHECK(bloom_may_contain(meta.columns[0], value.data(),
                            static_cast<uint32_t>(value.size()), &may).is_ok());
    CHECK(may);
}

static void test_non_string_types_get_no_filter() {
    // ARRAY has no flat byte representation; BOOL has two values (a filter
    // over them could not exclude anything min/max does not already); and
    // fixed-width numerics are excluded BY POLICY (architect ruling, not a
    // byte-representation limit — int64 has hashable bytes) — zone maps
    // already give them cheap range/equality pruning, and a numeric column
    // that declined value ordering was paying a filter's full build cost
    // (hash + distinct every row) for a benefit zone maps mostly deliver
    // already. See value_bytes_at in bloom.cpp.
    auto in = morsel_of({
        {"arr", array_column({{1, 2}, {3}})},
        {"b",   bool_column({true, false})},
        {"n",   dense_column<int64_t>({1, 2, 3, 4}, DRAKEN_INT64)},
    });
    auto bytes = write_or_die(in, with_bloom({"arr", "b", "n"}));

    RowGroupMetadata meta;
    CHECK(read_row_group_metadata(bytes.data(), bytes.size(), 0, &meta).is_ok());
    CHECK(meta.columns[0].bloom.empty());
    CHECK(meta.columns[1].bloom.empty());
    CHECK(meta.columns[2].bloom.empty());
}

static void test_filter_is_built_over_distinct_values() {
    // 20000 rows, 40 distinct. Built over the deduplicated `data` array, the
    // filter is sized for 40 values — not 20000 — so it stays tiny.
    std::vector<std::string> values(20000);
    for (size_t i = 0; i < values.size(); ++i)
        values[i] = "code-" + std::to_string(i % 40);

    auto in = morsel_of({{"code", string_column(values)}});
    auto bytes = write_or_die(in, with_bloom({"code"}));

    RowGroupMetadata meta;
    CHECK(read_row_group_metadata(bytes.data(), bytes.size(), 0, &meta).is_ok());
    CHECK(meta.columns[0].value_order == ValueOrder::kAscending);
    CHECK_EQ(meta.columns[0].data_length, uint32_t{40});
    CHECK(meta.columns[0].bloom.size() < 1024);

    for (int i = 0; i < 40; ++i) {
        const std::string value = "code-" + std::to_string(i);
        bool may = false;
        CHECK(bloom_may_contain(meta.columns[0], value.data(),
                                static_cast<uint32_t>(value.size()), &may).is_ok());
        CHECK(may);
    }
}

// Sizing must track the DISTINCT count, not data_length.
//
// The test above goes through the writer, where value ordering deduplicates the
// data array first and data_length IS the distinct count — so it cannot see this
// bug. bloom_build is exercised DIRECTLY here instead, because the broken case is
// a vector that reaches it WITHOUT having been deduplicated: value ordering is
// declined per column, and a declined column arrives dense with data_length equal
// to its row count. Going through the writer would couple this test to whatever
// the ordering heuristic happens to be tuned to today, which is a different
// component's decision and free to change.
//
// Two dense vectors with IDENTICAL data_length and different distinct counts:
// sized on data_length these came out byte-identical, which is the defect.
static void test_sizing_tracks_distinct_not_data_length() {
    std::vector<std::string> repetitive(100000), distinct(100000);
    for (size_t i = 0; i < repetitive.size(); ++i) {
        repetitive[i] = "r-" + std::to_string(i % 5000);
        distinct[i]   = "d-" + std::to_string(i);
    }

    auto few  = string_column(repetitive);
    auto many = string_column(distinct);
    CHECK_EQ(few.view.data_length, uint32_t{100000});
    CHECK_EQ(many.view.data_length, uint32_t{100000});

    std::vector<uint8_t> few_body, many_body;
    CHECK(bloom_build(few.view, kDefaultFalsePositiveRate, &few_body));
    CHECK(bloom_build(many.view, kDefaultFalsePositiveRate, &many_body));

    // 20x apart by construction (5000 keys against 100000). A 4x bound proves
    // the sizing moved with distinct without pinning the bits-per-key curve,
    // which is measured and free to be recalibrated.
    CHECK(few_body.size() * 4 < many_body.size());

    // Smaller, and still a filter: the one inviolable property survives.
    for (int i = 0; i < 5000; ++i) {
        const std::string value = "r-" + std::to_string(i);
        bool may = false;
        CHECK(bloom_probe(few_body.data(), few_body.size(),
                          value.data(), static_cast<uint32_t>(value.size()), &may).is_ok());
        CHECK(may);
    }
}

// ─── Corruption ─────────────────────────────────────────────────────────────

static void test_corrupt_filter_is_rejected_not_answered() {
    std::vector<std::string> values(1000);
    for (size_t i = 0; i < values.size(); ++i) values[i] = "v-" + std::to_string(i);
    auto in = morsel_of({{"n", string_column(values)}});
    auto bytes = write_or_die(in, with_bloom({"n"}));

    RowGroupMetadata meta;
    CHECK(read_row_group_metadata(bytes.data(), bytes.size(), 0, &meta).is_ok());

    const std::string value = "v-5";
    bool may = false;

    // A block count that is not a power of two would make block selection
    // disagree with whatever wrote it, so the filter would answer about the
    // wrong block. Answering "absent" from it would drop rows.
    ColumnMetadata broken = meta.columns[0];
    const uint32_t not_power_of_two = 3;
    std::memcpy(broken.bloom.data(), &not_power_of_two, sizeof(not_power_of_two));
    CHECK(!bloom_may_contain(broken, value.data(), static_cast<uint32_t>(value.size()),
                             &may).is_ok());

    // A length that disagrees with the declared block count.
    ColumnMetadata truncated = meta.columns[0];
    truncated.bloom.resize(truncated.bloom.size() - 1);
    CHECK(!bloom_may_contain(truncated, value.data(), static_cast<uint32_t>(value.size()),
                             &may).is_ok());

    ColumnMetadata stub = meta.columns[0];
    stub.bloom.resize(4);
    CHECK(!bloom_may_contain(stub, value.data(), static_cast<uint32_t>(value.size()),
                             &may).is_ok());
}

int main() {
    test_never_rejects_a_present_value();
    test_rejects_most_absent_values();
    test_strings();
    test_only_requested_columns_get_filters();
    test_non_string_types_get_no_filter();
    test_filter_is_built_over_distinct_values();
    test_sizing_tracks_distinct_not_data_length();
    test_corrupt_filter_is_rejected_not_answered();
    return skene_test::summary("test_bloom");
}
