// Value ordering (FORMAT.md §7.6) and statistics (§8).
//
// Value ordering reorders VALUES, never ROWS. Every test therefore checks the
// logical rows are byte-identical to what went in — a reordering that changed
// row order would be silent data corruption, and it is the single most likely
// way to get this wrong.

#include <cmath>
#include <cstring>
#include <string>
#include <vector>

#include "build_vectors.h"
#include "harness.h"
#include "skene/format.h"
#include "skene/reader.h"
#include "skene/writer.h"

using namespace skene;
using namespace skene_test;

static WriteOptions ordered_options() {
    WriteOptions options;
    options.read_acceleration = true;
    return options;
}

static std::vector<uint8_t> write_ordered(const CxxMorsel& in) {
    std::vector<uint8_t> bytes;
    Status st = write_morsel(in, ordered_options(), &bytes);
    if (!st.is_ok()) {
        std::fprintf(stderr, "  write failed: %s\n", st.message().c_str());
        ++skene_test::g_failures;
    }
    return bytes;
}

static void read_back(const std::vector<uint8_t>& bytes, CxxMorsel* out) {
    Status st = read_morsel(bytes.data(), bytes.size(), out);
    if (!st.is_ok()) {
        std::fprintf(stderr, "  read failed: %s\n", st.message().c_str());
        ++skene_test::g_failures;
    }
}

template <typename T>
static void check_rows(const DrakenVector& v, const std::vector<T>& expect) {
    const T* data = static_cast<const T*>(v.data);
    CHECK_EQ(v.length, static_cast<uint32_t>(expect.size()));
    for (uint32_t i = 0; i < v.length && i < expect.size(); ++i)
        CHECK_EQ(data[v.selection[i]], expect[i]);
}

static const ColumnMetadata& meta_of(const FileMetadata& meta, size_t i) {
    return meta.columns[i];
}

// ─── Ordering ───────────────────────────────────────────────────────────────

static void test_data_is_sorted_and_deduplicated_rows_unchanged() {
    // Deliberately unsorted with repeats: 3 distinct values in 8 rows.
    const std::vector<int64_t> rows = {50, 10, 30, 10, 50, 30, 10, 50};
    auto in = morsel_of({{"n", dense_column<int64_t>(rows, DRAKEN_INT64)}});

    auto bytes = write_ordered(in);
    CxxMorsel out;
    read_back(bytes, &out);

    const DrakenVector& v = out.columns[0].view;

    // THE contract: logical rows are untouched.
    check_rows(v, rows);

    // data is ascending and deduplicated — 3 distinct values, not 8.
    CHECK_EQ(v.data_length, uint32_t{3});
    const int64_t* data = static_cast<const int64_t*>(v.data);
    CHECK_EQ(data[0], int64_t{10});
    CHECK_EQ(data[1], int64_t{30});
    CHECK_EQ(data[2], int64_t{50});

    // ...so it is now a dict, and the sortedness is advertised.
    CHECK(draken_is_dict(&v));
    CHECK((v.flags & DRAKEN_DICT_KEYS_SORTED) != 0);
    CHECK((v.flags & DRAKEN_DICT_CODES_DENSE) != 0);

    FileMetadata meta;
    CHECK(read_metadata(bytes.data(), bytes.size(), &meta).is_ok());
    CHECK(meta_of(meta, 0).value_order == ValueOrder::kAscending);

    // data_length IS the exact distinct count — COUNT(DISTINCT n) with no read.
    CHECK_EQ(meta_of(meta, 0).data_length, uint32_t{3});
}

static void test_all_distinct_int_declines_ordering() {
    // All distinct, dense: ordering would only add a stored-selection
    // permutation the reader pays on every access — read performance is king
    // for storage writes (architect ruling 2026-08-07), so this declines for
    // EVERY type, delta-capable included. (The file-size win of sorted+delta
    // was the old justification; it does not buy back the read cost — measured
    // 34ms of a 43ms engine scan reconstructing a 97.6%-unique column.)
    const std::vector<int64_t> rows = {40, 10, 30, 20};
    auto in = morsel_of({{"n", dense_column<int64_t>(rows, DRAKEN_INT64)}});

    auto bytes = write_ordered(in);
    CxxMorsel out;
    read_back(bytes, &out);

    const DrakenVector& v = out.columns[0].view;
    check_rows(v, rows);                       // rows unchanged, as always
    CHECK_EQ(v.data_length, uint32_t{4});

    // Written as-is: original value order, identity selection, no permutation.
    const int64_t* data = static_cast<const int64_t*>(v.data);
    for (uint32_t i = 0; i < v.data_length; ++i) CHECK_EQ(data[i], rows[i]);
    CHECK_EQ(v.data_length, v.length);
    CHECK((v.flags & DRAKEN_SEL_PERMUTATION) == 0);
    CHECK((v.flags & DRAKEN_SEL_IDENTITY) != 0);

    FileMetadata meta;
    CHECK(read_metadata(bytes.data(), bytes.size(), &meta).is_ok());
    CHECK(meta_of(meta, 0).value_order == ValueOrder::kAsWritten);
}

static void test_near_unique_strings_are_not_ordered() {
    // Deduplication removes nothing, the column is dense, and strings cannot be
    // delta-encoded — so ordering could only add a permutation to a column that
    // had no selection at all. It must decline rather than pay to make the file
    // bigger.
    std::vector<std::string> rows(5000);
    for (size_t i = 0; i < rows.size(); ++i) rows[i] = "id-" + std::to_string(i * 7919);

    auto in = morsel_of({{"s", string_column(rows)}});
    auto bytes = write_ordered(in);

    FileMetadata meta;
    CHECK(read_metadata(bytes.data(), bytes.size(), &meta).is_ok());
    CHECK(meta.columns[0].value_order == ValueOrder::kAsWritten);
    CHECK(meta.columns[0].selection_kind == SelectionKind::kIdentity);

    // ...and it still round-trips, unordered.
    CxxMorsel out;
    read_back(bytes, &out);
    const DrakenVector& v = out.columns[0].view;
    for (uint32_t i = 0; i < rows.size(); ++i) {
        const DrakenStringArena* arena =
            static_cast<const DrakenStringArena*>(v.data);
        const DrakenStringSlot* slot = &arena->slots[v.selection[i]];
        CHECK(std::string(reinterpret_cast<const char*>(str_data(slot, arena->arena)),
                          str_length(slot)) == rows[i]);
    }
}

static void test_already_sorted_stays_identity() {
    // Sorted and distinct on the way in: ordering must recognise the codes are
    // the identity and store NO selection section, rather than writing 4 bytes
    // per row of 0,1,2,...
    const std::vector<int64_t> rows = {10, 20, 30, 40};
    auto in = morsel_of({{"n", dense_column<int64_t>(rows, DRAKEN_INT64)}});

    auto bytes = write_ordered(in);
    CxxMorsel out;
    read_back(bytes, &out);

    const DrakenVector& v = out.columns[0].view;
    check_rows(v, rows);
    CHECK((v.flags & DRAKEN_SEL_IDENTITY) != 0);
    CHECK(out.columns[0].own->codes_buf == nullptr);
}

static void test_nulls_do_not_enter_the_data_array() {
    // Row 1 and 3 are null. Their underlying values (999, 888) must NOT appear
    // in `data`, or data_length stops being the exact distinct count.
    const std::vector<int64_t> rows = {50, 999, 10, 888, 50};
    auto in = morsel_of({{"n", dense_column<int64_t>(
        rows, DRAKEN_INT64, {true, false, true, false, true})}});

    auto bytes = write_ordered(in);
    CxxMorsel out;
    read_back(bytes, &out);

    const DrakenVector& v = out.columns[0].view;
    CHECK_EQ(v.data_length, uint32_t{2});   // {10, 50} — not 4
    const int64_t* data = static_cast<const int64_t*>(v.data);
    CHECK_EQ(data[0], int64_t{10});
    CHECK_EQ(data[1], int64_t{50});

    // Valid rows keep their values; null rows stay null with an in-range code.
    CHECK_EQ(data[v.selection[0]], int64_t{50});
    CHECK_EQ(data[v.selection[2]], int64_t{10});
    CHECK_EQ(data[v.selection[4]], int64_t{50});
    CHECK(v.selection[1] < v.data_length);
    CHECK(v.selection[3] < v.data_length);
    CHECK((v.validity[0] & 0b00000010) == 0);
    CHECK((v.validity[0] & 0b00001000) == 0);
}

static void test_negative_zero_is_not_collapsed_into_zero() {
    // Under draken's float order -0.0 == 0.0, so an equality-based dedup would
    // collapse them and a column containing -0.0 would read back as 0.0 —
    // silent corruption on a round trip. Dedup keys on the BIT PATTERN.
    const std::vector<double> rows = {0.0, -0.0, 1.5, -0.0, 0.0};
    auto in = morsel_of({{"f", dense_column<double>(rows, DRAKEN_FLOAT64)}});

    auto bytes = write_ordered(in);
    CxxMorsel out;
    read_back(bytes, &out);

    const DrakenVector& v = out.columns[0].view;
    const double* data = static_cast<const double*>(v.data);

    // Both zeros survive as distinct stored values, plus 1.5.
    CHECK_EQ(v.data_length, uint32_t{3});

    // And every row keeps its own sign bit.
    for (uint32_t i = 0; i < rows.size(); ++i) {
        const double got = data[v.selection[i]];
        CHECK_EQ(got, rows[i]);
        CHECK_EQ(std::signbit(got), std::signbit(rows[i]));
    }
}

static void test_nan_sorts_highest_and_survives() {
    const double nan = std::nan("");
    const std::vector<double> rows = {5.0, nan, -1.0, nan};
    auto in = morsel_of({{"f", dense_column<double>(rows, DRAKEN_FLOAT64)}});

    auto bytes = write_ordered(in);
    CxxMorsel out;
    read_back(bytes, &out);

    const DrakenVector& v = out.columns[0].view;
    const double* data = static_cast<const double*>(v.data);

    // draken's convention: NaN is the highest value, so it lands last.
    CHECK(std::isnan(data[v.data_length - 1]));
    CHECK_EQ(data[0], double{-1.0});

    for (uint32_t i = 0; i < rows.size(); ++i) {
        const double got = data[v.selection[i]];
        if (std::isnan(rows[i])) CHECK(std::isnan(got));
        else CHECK_EQ(got, rows[i]);
    }
}

static void test_strings_are_ordered_and_arena_rebased() {
    const std::vector<std::string> rows = {
        "pear", "apple", "a very long string that must live in the arena",
        "apple", "banana", "a very long string that must live in the arena",
    };
    auto in = morsel_of({{"s", string_column(rows)}});

    auto bytes = write_ordered(in);
    CxxMorsel out;
    read_back(bytes, &out);

    const DrakenVector& v = out.columns[0].view;
    const DrakenStringArena* arena =
        static_cast<const DrakenStringArena*>(v.data);

    // 4 distinct values from 6 rows.
    CHECK_EQ(v.data_length, uint32_t{4});
    CHECK_EQ(arena->length, size_t{4});

    // Rows unchanged, with the long payloads correctly rebased into the new arena.
    for (uint32_t i = 0; i < rows.size(); ++i) {
        const DrakenStringSlot* slot = &arena->slots[v.selection[i]];
        CHECK(std::string(reinterpret_cast<const char*>(str_data(slot, arena->arena)),
                          str_length(slot)) == rows[i]);
    }

    // And `data` is in lexicographic order.
    for (uint32_t i = 1; i < v.data_length; ++i)
        CHECK(str_compare(&arena->slots[i - 1], arena->arena,
                          &arena->slots[i], arena->arena) < 0);
}

static void test_ineligible_columns_are_written_as_written() {
    // Each of these is ineligible for a different, deliberate reason. None may
    // claim ASCENDING — a false ordering claim makes binary-search consumers
    // wrong, not slow. Every column is 2 rows: a morsel's columns must agree on
    // row count, and the reader enforces it.
    auto in = morsel_of({
        {"b",       bool_column({true, false})},                         // bit-packed
        {"variant", string_column({"{\"a\":1}", "{\"b\":2}"}, DRAKEN_VARIANT)},  // no collation
        {"arr",     array_column({{1, 2}, {3}})},                        // no comparison
        {"elided",  string_column({"tiny", "a long elided value here"},
                                  DRAKEN_VARCHAR, {}, /*elide=*/true)},  // no payload bytes
        {"allnull", dense_column<int64_t>({7, 8}, DRAKEN_INT64, {false, false})},  // nothing to order
    });

    auto bytes = write_ordered(in);
    FileMetadata meta;
    CHECK(read_metadata(bytes.data(), bytes.size(), &meta).is_ok());

    for (size_t i = 0; i < meta.columns.size(); ++i) {
        ++skene_test::g_checks;
        if (meta.columns[i].value_order != ValueOrder::kAsWritten)
            skene_test::report(__FILE__, __LINE__, "ineligible column claims ordering",
                               meta.columns[i].name);
    }

    // ...and they still round-trip correctly.
    CxxMorsel out;
    read_back(bytes, &out);
    CHECK_EQ(out.num_columns(), size_t{5});
}

static void test_ordering_preserves_row_sortedness_flag() {
    // ROW_SORTED describes the LOGICAL ROW ORDER, which value ordering does not
    // touch. Dropping it would cost every downstream sorted-input fast path.
    auto column = dense_column<int64_t>({10, 20, 20, 30}, DRAKEN_INT64);
    column.view.flags |= DRAKEN_ROW_SORTED;
    column.own->vec.flags = column.view.flags;

    auto in = morsel_of({{"sorted", std::move(column)}});
    auto bytes = write_ordered(in);
    CxxMorsel out;
    read_back(bytes, &out);

    CHECK(draken_vector_is_row_sorted(&out.columns[0].view));
    CHECK((out.columns[0].view.flags & DRAKEN_DICT_KEYS_SORTED) != 0);
}

// ─── Statistics ─────────────────────────────────────────────────────────────

static void test_min_max_null_count_and_sum() {
    const std::vector<int64_t> rows = {30, 99, 10, 20, 99};
    auto in = morsel_of({{"n", dense_column<int64_t>(
        rows, DRAKEN_INT64, {true, false, true, true, false})}});

    auto bytes = write_ordered(in);
    FileMetadata meta;
    CHECK(read_metadata(bytes.data(), bytes.size(), &meta).is_ok());

    const ColumnMetadata& column = meta_of(meta, 0);
    CHECK(column.has_statistics);
    const ColumnStatistics& stats = column.statistics;

    CHECK((stats.flags & kStatNullCount) != 0);
    CHECK_EQ(stats.null_count, uint64_t{2});

    // min/max over NON-NULL values only. Including a null would make the min
    // ORDINAL_NULL (INT64_MIN) and prune nothing.
    CHECK((stats.flags & kStatMin) != 0);
    CHECK((stats.flags & kStatMax) != 0);
    CHECK_EQ(stats.min_ordinal, int64_t{10});
    CHECK_EQ(stats.max_ordinal, int64_t{30});

    // SUM over non-null values: 30 + 10 + 20 == 60. The nulls' underlying 99s
    // must not be counted.
    CHECK((stats.flags & kStatSum) != 0);
    CHECK_EQ(stats.sum_low, int64_t{60});
    CHECK_EQ(stats.sum_high, int64_t{0});
}

static void test_sum_is_128_bit() {
    // Four values that each nearly fill an int64: the total overflows int64 and
    // must land intact in the 128-bit accumulator.
    const int64_t big = 4611686018427387904LL;  // 2^62
    auto in = morsel_of({{"n", dense_column<int64_t>({big, big, big, big}, DRAKEN_INT64)}});

    auto bytes = write_ordered(in);
    FileMetadata meta;
    CHECK(read_metadata(bytes.data(), bytes.size(), &meta).is_ok());

    const ColumnStatistics& stats = meta_of(meta, 0).statistics;
    const __int128 total = (static_cast<__int128>(stats.sum_high) << 64)
                         | static_cast<uint64_t>(stats.sum_low);
    CHECK(total == static_cast<__int128>(big) * 4);
    CHECK(total > static_cast<__int128>(INT64_MAX));   // genuinely past int64
}

static void test_floats_get_no_sum() {
    // Floating-point addition is not associative, so a stored sum and a
    // recomputed one disagree in the low bits — the answer would depend on
    // whether the optimizer used the footer. Absent, deliberately.
    auto in = morsel_of({{"f", dense_column<double>({1.5, 2.5, 3.5}, DRAKEN_FLOAT64)}});

    auto bytes = write_ordered(in);
    FileMetadata meta;
    CHECK(read_metadata(bytes.data(), bytes.size(), &meta).is_ok());

    const ColumnStatistics& stats = meta_of(meta, 0).statistics;
    CHECK((stats.flags & kStatSum) == 0);
    CHECK_EQ(stats.sum_low, int64_t{0});
    CHECK_EQ(stats.sum_high, int64_t{0});
    // ...but min/max are perfectly well defined for floats.
    CHECK((stats.flags & kStatMin) != 0);
    CHECK((stats.flags & kStatMax) != 0);
}

static void test_types_without_order_get_no_min_max() {
    LogicalType lt;
    lt.kind = LogicalKind::DECIMAL;
    lt.precision = 38;
    lt.scale = 2;
    const LogicalType* interned = logical_type_intern(lt);

    auto in = morsel_of({
        {"variant", string_column({"{\"a\":1}"}, DRAKEN_VARIANT)},
        {"arr",     array_column({{1, 2}})},
        {"dec128",  dense_column<__int128>({__int128{5}}, DRAKEN_DECIMAL128, {}, interned)},
    });

    auto bytes = write_ordered(in);
    FileMetadata meta;
    CHECK(read_metadata(bytes.data(), bytes.size(), &meta).is_ok());

    for (const ColumnMetadata& column : meta.columns) {
        ++skene_test::g_checks;
        if ((column.statistics.flags & (kStatMin | kStatMax)) != 0)
            skene_test::report(__FILE__, __LINE__,
                               "unordered type reported min/max", column.name);
        // null_count is always available, even where ordering is not.
        CHECK((column.statistics.flags & kStatNullCount) != 0);
    }
}

static void test_spill_profile_carries_no_statistics() {
    auto in = morsel_of({{"n", dense_column<int64_t>({1, 2, 3}, DRAKEN_INT64)}});
    std::vector<uint8_t> bytes;
    CHECK(write_morsel(in, WriteOptions::for_spill(), &bytes).is_ok());

    FileMetadata meta;
    CHECK(read_metadata(bytes.data(), bytes.size(), &meta).is_ok());
    CHECK(!meta_of(meta, 0).has_statistics);
    CHECK(meta_of(meta, 0).value_order == ValueOrder::kAsWritten);
}

static void test_string_min_max_are_ordinals_not_bytes() {
    auto in = morsel_of({{"s", string_column({"delta", "alpha", "charlie"})}});
    auto bytes = write_ordered(in);
    FileMetadata meta;
    CHECK(read_metadata(bytes.data(), bytes.size(), &meta).is_ok());

    const ColumnStatistics& stats = meta_of(meta, 0).statistics;
    CHECK((stats.flags & kStatMin) != 0);
    // Ordinals are monotonic, so min < max for distinct prefixes. They are NOT
    // the values: a consumer must ordinalize its literal to compare.
    CHECK(stats.min_ordinal < stats.max_ordinal);
    CHECK(stats.min_ordinal >= 0);   // string ordinals are non-negative by construction
}

// ─── Zone maps ──────────────────────────────────────────────────────────────

static void test_zone_map_enables_chunk_skipping() {
    // 40000 rows in 5 chunks of 8192. Values are clustered by position, so a
    // predicate for one cluster should rule out most chunks — which is the whole
    // point: without a zone map a predicate reads the entire code stream.
    const uint32_t rows = 40000;
    std::vector<int64_t> values(rows);
    for (uint32_t i = 0; i < rows; ++i) values[i] = (i / 8000) * 100;  // 5 clusters

    auto in = morsel_of({{"clustered", dense_column<int64_t>(values, DRAKEN_INT64)}});
    auto bytes = write_ordered(in);

    FileMetadata meta;
    CHECK(read_metadata(bytes.data(), bytes.size(), &meta).is_ok());

    const ColumnMetadata& column = meta_of(meta, 0);
    CHECK(column.zone_map.present());
    CHECK_EQ(column.zone_map.chunk_rows, kZoneMapDefaultChunkRows);
    CHECK_EQ(column.zone_map.chunks.size(),
             size_t{(rows + kZoneMapDefaultChunkRows - 1) / kZoneMapDefaultChunkRows});

    // Bounds are VALUE ordinals, so a predicate maps straight onto them with no
    // dictionary lookup in between. For INT64 the ordinal IS the value.
    CxxMorsel out;
    read_back(bytes, &out);
    const DrakenVector& v = out.columns[0].view;
    const int64_t wanted = 300;

    size_t skipped = 0;
    for (size_t chunk = 0; chunk < column.zone_map.chunks.size(); ++chunk) {
        if (column.zone_map.chunk_may_contain(chunk, wanted, wanted)) continue;
        ++skipped;
        // A skip must be PROVABLE, not probable: verify no row in the skipped
        // range actually matches. A zone map that skips a matching row silently
        // drops data from the answer.
        uint32_t begin = 0, end = 0;
        column.zone_map.chunk_rows_range(chunk, v.length, &begin, &end);
        const int64_t* data = static_cast<const int64_t*>(v.data);
        for (uint32_t row = begin; row < end; ++row)
            CHECK(data[v.selection[row]] != wanted);
    }
    CHECK(skipped > 0);   // it earned its bytes

    // Cheap: 16 bytes per chunk.
    CHECK(column.zone_map.chunks.size() * sizeof(ZoneMapEntry) < 200);
}

// The case that used to produce NOTHING, and is the sharpest pruning shape there
// is: a sorted, unique key. It dedups to nothing, so it has an identity selection
// and therefore no codes — and a code-indexed zone map had nothing to describe.
static void test_sorted_unique_key_gets_a_zone_map() {
    const uint32_t rows = 40000;
    std::vector<int64_t> values(rows);
    for (uint32_t i = 0; i < rows; ++i) values[i] = static_cast<int64_t>(i);

    auto in = morsel_of({{"id", dense_column<int64_t>(values, DRAKEN_INT64)}});
    auto bytes = write_ordered(in);

    FileMetadata meta;
    CHECK(read_metadata(bytes.data(), bytes.size(), &meta).is_ok());
    const ColumnMetadata& column = meta_of(meta, 0);

    CHECK(column.selection_kind == SelectionKind::kIdentity);  // no codes at all
    CHECK(column.zone_map.present());                          // and still indexed

    // Perfectly clustered, so exactly one chunk survives an equality probe.
    size_t survivors = 0;
    for (size_t chunk = 0; chunk < column.zone_map.chunks.size(); ++chunk)
        if (column.zone_map.chunk_may_contain(chunk, 20000, 20000)) ++survivors;
    CHECK_EQ(survivors, size_t{1});
}

// Coverage does not depend on ordering. An unordered column still gets bounds —
// they may be too wide to skip anything, but "cannot prune" and "no index" are
// different states and only the second is unrecoverable.
static void test_unordered_column_still_gets_a_zone_map() {
    const uint32_t rows = 40000;
    std::vector<int64_t> values(rows);
    for (uint32_t i = 0; i < rows; ++i)
        values[i] = static_cast<int64_t>((i * 7919u) % 100003u);   // scattered

    auto in = morsel_of({{"scattered", dense_column<int64_t>(values, DRAKEN_INT64)}});
    std::vector<uint8_t> bytes;
    WriteOptions options;
    options.read_acceleration = true;
    CHECK(write_morsel(in, options, &bytes).is_ok());

    FileMetadata meta;
    CHECK(read_metadata(bytes.data(), bytes.size(), &meta).is_ok());
    CHECK(meta_of(meta, 0).zone_map.present());

    // Whatever the bounds are, they must never rule out a chunk that holds the
    // value — that is the only property a zone map may not break.
    const ZoneMap& zones = meta_of(meta, 0).zone_map;
    for (size_t chunk = 0; chunk < zones.chunks.size(); ++chunk) {
        uint32_t begin = 0, end = 0;
        zones.chunk_rows_range(chunk, rows, &begin, &end);
        for (uint32_t row = begin; row < end; ++row)
            CHECK(zones.chunk_may_contain(chunk, values[row], values[row]));
    }
}

// An all-null chunk carries an empty range, which rules itself out for every
// probe. That is correct — a null satisfies no comparison — and it must survive
// the reader's validation rather than being mistaken for an inverted range.
static void test_all_null_chunk_prunes_itself() {
    const uint32_t rows = 20000;
    std::vector<int64_t> values(rows, 0);
    std::vector<bool> valid(rows, true);
    for (uint32_t i = 0; i < rows; ++i) {
        values[i] = static_cast<int64_t>(i);
        if (i < kZoneMapDefaultChunkRows) valid[i] = false;   // first chunk all null
    }

    auto in = morsel_of({{"n", dense_column<int64_t>(values, DRAKEN_INT64, valid)}});
    std::vector<uint8_t> bytes;
    WriteOptions options;
    options.read_acceleration = true;
    CHECK(write_morsel(in, options, &bytes).is_ok());

    FileMetadata meta;
    CHECK(read_metadata(bytes.data(), bytes.size(), &meta).is_ok());
    const ZoneMap& zones = meta_of(meta, 0).zone_map;
    CHECK(zones.present());

    CHECK(!zones.chunk_may_contain(0, INT64_MIN + 1, INT64_MAX - 1));
    CHECK(!zones.chunk_may_contain(0, 5, 5));
    CHECK(zones.chunk_may_contain(1, 9000, 9000));   // a real chunk still answers
}

static void test_zone_map_absent_when_it_could_not_help() {
    // Below one chunk, the footer's own min/max already covers the column, so a
    // zone map would be overhead that can never skip anything.
    auto small = morsel_of({{"n", dense_column<int64_t>({3, 1, 2}, DRAKEN_INT64)}});
    auto bytes = write_ordered(small);
    FileMetadata meta;
    CHECK(read_metadata(bytes.data(), bytes.size(), &meta).is_ok());
    CHECK(!meta_of(meta, 0).zone_map.present());

    // And an UNORDERED column gets none either: its codes carry no order, so
    // per-chunk code bounds would not describe a value range.
    std::vector<int64_t> many(20000);
    for (size_t i = 0; i < many.size(); ++i) many[i] = static_cast<int64_t>(i % 100);
    auto unordered = morsel_of({{"n", dense_column<int64_t>(many, DRAKEN_INT64)}});
    std::vector<uint8_t> spill;
    CHECK(write_morsel(unordered, WriteOptions::for_spill(), &spill).is_ok());
    FileMetadata spill_meta;
    CHECK(read_metadata(spill.data(), spill.size(), &spill_meta).is_ok());
    CHECK(!spill_meta.columns[0].zone_map.present());
}

static void test_statistics_are_correct_on_both_paths() {
    // min/max come from the ends of the sorted array on an ordered column, and
    // from a per-row scan otherwise. Both must be CORRECT — asserted against
    // values computed here rather than against each other, since two paths can
    // agree on a wrong answer.
    // Repeat-heavy (500 distinct in 5000 rows) so the column still ORDERS
    // under the read-king near-unique decline — this test exercises the
    // sorted-ends statistics path, which needs an ordered column to exist.
    std::vector<int64_t> values(5000);
    for (size_t i = 0; i < values.size(); ++i)
        values[i] = static_cast<int64_t>((i * 7919) % 500) - 250;
    std::vector<bool> valid(values.size(), true);
    valid[0] = valid[13] = valid[4999] = false;

    int64_t  expect_min = INT64_MAX, expect_max = INT64_MIN;
    __int128 expect_sum = 0;
    uint64_t expect_nulls = 0;
    for (size_t i = 0; i < values.size(); ++i) {
        if (!valid[i]) { ++expect_nulls; continue; }
        if (values[i] < expect_min) expect_min = values[i];
        if (values[i] > expect_max) expect_max = values[i];
        expect_sum += values[i];
    }

    // Ordered: an INT64 column is delta-capable, so it orders and min/max come
    // from data[0] and data[data_length-1].
    auto in = morsel_of({{"n", dense_column<int64_t>(values, DRAKEN_INT64, valid)}});
    auto bytes = write_ordered(in);

    FileMetadata meta;
    CHECK(read_metadata(bytes.data(), bytes.size(), &meta).is_ok());
    const ColumnStatistics& stats = meta_of(meta, 0).statistics;

    CHECK(meta_of(meta, 0).value_order == ValueOrder::kAscending);
    CHECK_EQ(stats.min_ordinal, expect_min);   // int64 ordinal IS the value
    CHECK_EQ(stats.max_ordinal, expect_max);
    CHECK_EQ(stats.null_count, expect_nulls);
    const __int128 total = (static_cast<__int128>(stats.sum_high) << 64)
                         | static_cast<uint64_t>(stats.sum_low);
    CHECK(total == expect_sum);

    // Unordered path: BOOL is never value-ordered (bit-packed), so its
    // statistics come from the row scan.
    std::vector<bool> bits(1000, false);
    std::vector<bool> bool_valid(1000, true);
    for (size_t i = 0; i < bits.size(); ++i) bits[i] = (i % 3) == 0;
    bool_valid[7] = false;

    auto bool_in = morsel_of({{"b", bool_column(bits, bool_valid)}});
    auto bool_bytes = write_ordered(bool_in);

    FileMetadata bool_meta;
    CHECK(read_metadata(bool_bytes.data(), bool_bytes.size(), &bool_meta).is_ok());
    const ColumnMetadata& bc = bool_meta.columns[0];

    CHECK(bc.value_order == ValueOrder::kAsWritten);
    CHECK_EQ(bc.statistics.null_count, uint64_t{1});
    CHECK_EQ(bc.statistics.min_ordinal, int64_t{0});   // both values present
    CHECK_EQ(bc.statistics.max_ordinal, int64_t{1});
}

int main() {
    test_zone_map_enables_chunk_skipping();
    test_sorted_unique_key_gets_a_zone_map();
    test_unordered_column_still_gets_a_zone_map();
    test_all_null_chunk_prunes_itself();
    test_zone_map_absent_when_it_could_not_help();
    test_statistics_are_correct_on_both_paths();
    test_data_is_sorted_and_deduplicated_rows_unchanged();
    test_all_distinct_int_declines_ordering();
    test_near_unique_strings_are_not_ordered();
    test_already_sorted_stays_identity();
    test_nulls_do_not_enter_the_data_array();
    test_negative_zero_is_not_collapsed_into_zero();
    test_nan_sorts_highest_and_survives();
    test_strings_are_ordered_and_arena_rebased();
    test_ineligible_columns_are_written_as_written();
    test_ordering_preserves_row_sortedness_flag();
    test_min_max_null_count_and_sum();
    test_sum_is_128_bit();
    test_floats_get_no_sum();
    test_types_without_order_get_no_min_max();
    test_spill_profile_carries_no_statistics();
    test_string_min_max_are_ordinals_not_bytes();
    return skene_test::summary("test_value_order");
}
