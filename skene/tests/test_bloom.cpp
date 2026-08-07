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
    std::vector<int64_t> values(5000);
    for (size_t i = 0; i < values.size(); ++i)
        values[i] = static_cast<int64_t>(i * 7919) - 1000000;

    auto in = morsel_of({{"n", dense_column<int64_t>(values, DRAKEN_INT64)}});
    auto bytes = write_or_die(in, with_bloom({"n"}));

    FileMetadata meta;
    CHECK(read_metadata(bytes.data(), bytes.size(), &meta).is_ok());
    CHECK(!meta.columns[0].bloom.empty());

    // EVERY present value must probe positive. A single false negative would
    // mean a query silently returning fewer rows than it should.
    size_t rejected = 0;
    for (int64_t value : values) {
        bool may = false;
        CHECK(bloom_may_contain(meta.columns[0], &value, sizeof(value), &may).is_ok());
        if (!may) ++rejected;
    }
    ++skene_test::g_checks;
    if (rejected != 0)
        skene_test::report(__FILE__, __LINE__, "false negative",
                           std::to_string(rejected) + " present values were rejected");
}

static void test_rejects_most_absent_values() {
    std::vector<int64_t> values(2000);
    for (size_t i = 0; i < values.size(); ++i) values[i] = static_cast<int64_t>(i) * 2;

    auto in = morsel_of({{"even", dense_column<int64_t>(values, DRAKEN_INT64)}});
    auto bytes = write_or_die(in, with_bloom({"even"}));

    FileMetadata meta;
    CHECK(read_metadata(bytes.data(), bytes.size(), &meta).is_ok());

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
        const int64_t absent = i * 2 + 1;
        bool may = false;
        CHECK(bloom_may_contain(meta.columns[0], &absent, sizeof(absent), &may).is_ok());
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

    FileMetadata meta;
    CHECK(read_metadata(bytes.data(), bytes.size(), &meta).is_ok());
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
        {"a", dense_column<int64_t>({1, 2, 3, 4}, DRAKEN_INT64)},
        {"b", dense_column<int64_t>({5, 6, 7, 8}, DRAKEN_INT64)},
    });
    auto bytes = write_or_die(in, with_bloom({"b"}));

    FileMetadata meta;
    CHECK(read_metadata(bytes.data(), bytes.size(), &meta).is_ok());
    CHECK(meta.columns[0].bloom.empty());    // not requested
    CHECK(!meta.columns[1].bloom.empty());

    // A column with no filter must answer "cannot rule out" rather than
    // "absent": a missing accelerator can cost speed, never rows.
    const int64_t value = 999;
    bool may = false;
    CHECK(bloom_may_contain(meta.columns[0], &value, sizeof(value), &may).is_ok());
    CHECK(may);
}

static void test_types_without_hashable_bytes_are_skipped() {
    // ARRAY has no flat byte representation, and BOOL has two values — a filter
    // over them could not exclude anything min/max does not already.
    auto in = morsel_of({
        {"arr", array_column({{1, 2}, {3}})},
        {"b",   bool_column({true, false})},
    });
    auto bytes = write_or_die(in, with_bloom({"arr", "b"}));

    FileMetadata meta;
    CHECK(read_metadata(bytes.data(), bytes.size(), &meta).is_ok());
    CHECK(meta.columns[0].bloom.empty());
    CHECK(meta.columns[1].bloom.empty());
}

static void test_filter_is_built_over_distinct_values() {
    // 20000 rows, 40 distinct. Built over the deduplicated `data` array, the
    // filter is sized for 40 values — not 20000 — so it stays tiny.
    std::vector<int64_t> values(20000);
    for (size_t i = 0; i < values.size(); ++i) values[i] = static_cast<int64_t>(i % 40);

    auto in = morsel_of({{"code", dense_column<int64_t>(values, DRAKEN_INT64)}});
    auto bytes = write_or_die(in, with_bloom({"code"}));

    FileMetadata meta;
    CHECK(read_metadata(bytes.data(), bytes.size(), &meta).is_ok());
    CHECK(meta.columns[0].value_order == ValueOrder::kAscending);
    CHECK_EQ(meta.columns[0].data_length, uint32_t{40});
    CHECK(meta.columns[0].bloom.size() < 1024);

    for (int64_t value = 0; value < 40; ++value) {
        bool may = false;
        CHECK(bloom_may_contain(meta.columns[0], &value, sizeof(value), &may).is_ok());
        CHECK(may);
    }
}

// ─── Corruption ─────────────────────────────────────────────────────────────

static void test_corrupt_filter_is_rejected_not_answered() {
    std::vector<int64_t> values(1000);
    for (size_t i = 0; i < values.size(); ++i) values[i] = static_cast<int64_t>(i);
    auto in = morsel_of({{"n", dense_column<int64_t>(values, DRAKEN_INT64)}});
    auto bytes = write_or_die(in, with_bloom({"n"}));

    FileMetadata meta;
    CHECK(read_metadata(bytes.data(), bytes.size(), &meta).is_ok());

    const int64_t value = 5;
    bool may = false;

    // A block count that is not a power of two would make block selection
    // disagree with whatever wrote it, so the filter would answer about the
    // wrong block. Answering "absent" from it would drop rows.
    ColumnMetadata broken = meta.columns[0];
    const uint32_t not_power_of_two = 3;
    std::memcpy(broken.bloom.data(), &not_power_of_two, sizeof(not_power_of_two));
    CHECK(!bloom_may_contain(broken, &value, sizeof(value), &may).is_ok());

    // A length that disagrees with the declared block count.
    ColumnMetadata truncated = meta.columns[0];
    truncated.bloom.resize(truncated.bloom.size() - 1);
    CHECK(!bloom_may_contain(truncated, &value, sizeof(value), &may).is_ok());

    ColumnMetadata stub = meta.columns[0];
    stub.bloom.resize(4);
    CHECK(!bloom_may_contain(stub, &value, sizeof(value), &may).is_ok());
}

int main() {
    test_never_rejects_a_present_value();
    test_rejects_most_absent_values();
    test_strings();
    test_only_requested_columns_get_filters();
    test_types_without_hashable_bytes_are_skipped();
    test_filter_is_built_over_distinct_values();
    test_corrupt_filter_is_rejected_not_answered();
    return skene_test::summary("test_bloom");
}
