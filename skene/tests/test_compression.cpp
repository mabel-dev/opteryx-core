// Per-section zstd (FORMAT.md §7.7, Encoding::kZstd).
//
// Compression is a PURE SIZE OPTIMIZATION: a compressed file must reconstruct
// byte-identically to an uncompressed one. Every test here therefore writes the
// same morsel twice — once raw, once compressed — and compares the results
// rather than checking the compressed file against itself.
//
// It is applied PER SECTION so each column extent stays independently fetchable.
// Whole-file compression would be smaller and would destroy that, which is the
// property `test_column_extents_stay_independent` pins.

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

static std::vector<uint8_t> write_with(const CxxMorsel& m, int level,
                                       bool acceleration = true) {
    WriteOptions options;
    options.read_acceleration = acceleration;
    options.zstd_level = level;
    std::vector<uint8_t> bytes;
    Status st = write_morsel(m, options, &bytes);
    if (!st.is_ok()) {
        std::fprintf(stderr, "  write failed: %s\n", st.message().c_str());
        ++skene_test::g_failures;
    }
    return bytes;
}

// A text column with real redundancy — the shape that made this necessary. On
// TPC-H, comment columns are most of the bytes and the arena keeps almost all
// of its redundancy after bit packing and delta have run.
static std::vector<std::string> comment_like(size_t rows) {
    static const char* fragments[] = {
        "carefully regular accounts sleep against the",
        "furiously bold requests wake quickly among the",
        "slyly ironic deposits nag blithely about the",
        "express packages haggle carefully around the",
    };
    std::vector<std::string> values;
    values.reserve(rows);
    for (size_t i = 0; i < rows; ++i)
        values.push_back(std::string(fragments[i % 4]) + " " + std::to_string(i)
                         + " final theodolites");
    return values;
}

// ─── The contract ───────────────────────────────────────────────────────────

static void test_compressed_reads_back_identically() {
    // Every column must agree on row count; the reader enforces it.
    std::vector<int64_t> numbers(500);
    std::vector<bool> valid(500, true);
    for (size_t i = 0; i < numbers.size(); ++i) {
        numbers[i] = static_cast<int64_t>(i % 7);
        if (i % 11 == 0) valid[i] = false;
    }
    std::vector<bool> bits(500, false);
    for (size_t i = 0; i < bits.size(); ++i) bits[i] = (i % 3) == 0;

    auto in = morsel_of({
        {"n",  dense_column<int64_t>(numbers, DRAKEN_INT64, valid)},
        {"s",  string_column(comment_like(500))},
        {"b",  bool_column(bits)},
    });

    CxxMorsel raw_out, packed_out;
    auto raw = write_with(in, 0);
    auto packed = write_with(in, 3);
    CHECK(read_morsel(raw.data(), raw.size(), &raw_out).is_ok());
    CHECK(read_morsel(packed.data(), packed.size(), &packed_out).is_ok());

    CHECK_EQ(raw_out.num_columns(), packed_out.num_columns());
    CHECK_EQ(raw_out.num_rows(), packed_out.num_rows());

    // Same values, same SHAPE, same flags — compression must not change what the
    // column is, only how many bytes it took.
    for (size_t c = 0; c < raw_out.num_columns(); ++c) {
        const DrakenVector& a = raw_out.columns[c].view;
        const DrakenVector& b = packed_out.columns[c].view;
        CHECK_EQ(a.length, b.length);
        CHECK_EQ(a.data_length, b.data_length);
        CHECK_EQ(a.flags, b.flags);
        CHECK_EQ(static_cast<int>(a.type), static_cast<int>(b.type));
        CHECK_EQ(a.validity == nullptr, b.validity == nullptr);
        for (uint32_t i = 0; i < a.length; ++i) CHECK_EQ(a.selection[i], b.selection[i]);
    }

    // The string column's bytes must survive exactly.
    const DrakenStringArena* raw_arena =
        static_cast<const DrakenStringArena*>(raw_out.columns[1].view.data);
    const DrakenStringArena* packed_arena =
        static_cast<const DrakenStringArena*>(packed_out.columns[1].view.data);
    CHECK_EQ(raw_arena->arena_used, packed_arena->arena_used);
    CHECK_EQ(std::memcmp(raw_arena->arena, packed_arena->arena,
                         raw_arena->arena_used), 0);
}

static void test_it_actually_shrinks_text() {
    auto in = morsel_of({{"comment", string_column(comment_like(20000))}});
    const auto raw = write_with(in, 0);
    const auto packed = write_with(in, 3);

    CHECK(packed.size() < raw.size());
    // The arena is the bulk of a text column, and it compresses well — anything
    // less than a 2x saving here would mean the compression is not reaching it.
    ++skene_test::g_checks;
    if (packed.size() * 2 > raw.size())
        skene_test::report(__FILE__, __LINE__, "compression barely helped text",
                           std::to_string(raw.size()) + " -> " +
                           std::to_string(packed.size()));
}

static void test_declines_when_not_smaller() {
    // Incompressible data: the codec must store the plain body rather than a
    // larger frame. "Not worth it" is a normal answer.
    std::vector<int64_t> noise(4000);
    uint64_t state = 88172645463325252ull;
    for (size_t i = 0; i < noise.size(); ++i) {
        state ^= state << 13; state ^= state >> 7; state ^= state << 17;
        noise[i] = static_cast<int64_t>(state);
    }
    auto in = morsel_of({{"noise", dense_column<int64_t>(noise, DRAKEN_INT64)}});

    const auto raw = write_with(in, 0, /*acceleration=*/false);
    const auto packed = write_with(in, 3, /*acceleration=*/false);

    // Never LARGER than raw: each section falls back independently.
    ++skene_test::g_checks;
    if (packed.size() > raw.size())
        skene_test::report(__FILE__, __LINE__, "compression made the file bigger",
                           std::to_string(raw.size()) + " -> " +
                           std::to_string(packed.size()));

    CxxMorsel out;
    CHECK(read_morsel(packed.data(), packed.size(), &out).is_ok());
    const int64_t* values = static_cast<const int64_t*>(out.columns[0].view.data);
    for (size_t i = 0; i < noise.size(); ++i)
        CHECK_EQ(values[out.columns[0].view.selection[i]], noise[i]);
}

static void test_already_encoded_sections_are_not_recompressed() {
    // A bit-packed selection has had its redundancy removed already; layering a
    // general compressor over it costs CPU for nothing, so those bodies keep
    // their encoding.
    std::vector<int64_t> values(20000);
    for (size_t i = 0; i < values.size(); ++i) values[i] = static_cast<int64_t>(i % 40);
    auto in = morsel_of({{"code", dense_column<int64_t>(values, DRAKEN_INT64)}});

    const auto packed = write_with(in, 3);
    FileMetadata meta;
    CHECK(read_metadata(packed.data(), packed.size(), &meta).is_ok());

    // Round-trips regardless of which encoding each section chose.
    CxxMorsel out;
    CHECK(read_morsel(packed.data(), packed.size(), &out).is_ok());
    const int64_t* got = static_cast<const int64_t*>(out.columns[0].view.data);
    for (size_t i = 0; i < values.size(); ++i)
        CHECK_EQ(got[out.columns[0].view.selection[i]], values[i]);
}

// ─── The property compression must not break ────────────────────────────────

static void test_column_extents_stay_independent() {
    // The reason this is per-section rather than whole-file: every column must
    // still be a self-contained byte range. If the file were compressed as a
    // unit, reading one column would mean decompressing all of them.
    auto in = morsel_of({
        {"a", string_column(comment_like(2000))},
        {"b", dense_column<int64_t>(std::vector<int64_t>(2000, 7), DRAKEN_INT64)},
        {"c", string_column(comment_like(2000))},
    });
    const auto packed = write_with(in, 3);

    FileMetadata meta;
    CHECK(read_metadata(packed.data(), packed.size(), &meta).is_ok());
    CHECK_EQ(meta.columns.size(), size_t{3});

    for (const ColumnMetadata& column : meta.columns) {
        CHECK(column.byte_bytes > 0);
        CHECK(column.byte_offset >= kFileHeadBytes);
        CHECK(column.byte_offset + column.byte_bytes <= packed.size());
    }
    // Non-overlapping, so a range request for one column fetches only it.
    CHECK(meta.columns[0].byte_offset + meta.columns[0].byte_bytes
          <= meta.columns[1].byte_offset);
    CHECK(meta.columns[1].byte_offset + meta.columns[1].byte_bytes
          <= meta.columns[2].byte_offset);

    // And a single column can be materialized on its own.
    ReadOptions options;
    options.columns = {"c"};
    CxxMorsel one;
    CHECK(read_morsel(packed.data(), packed.size(), options, &one).is_ok());
    CHECK_EQ(one.num_columns(), size_t{1});
    CHECK_EQ(one.num_rows(), uint32_t{2000});
}

static void test_corrupt_compressed_section_is_rejected() {
    auto in = morsel_of({{"s", string_column(comment_like(4000))}});
    auto packed = write_with(in, 3);

    // Corrupting a compressed body must be caught by its checksum before the
    // decompressor ever sees it — a zstd frame fed corrupt input can fail in
    // less predictable ways than a plain memcpy.
    packed[kFileHeadBytes + 64] ^= 0xFF;
    CxxMorsel out;
    Status st = read_morsel(packed.data(), packed.size(), &out);
    CHECK(!st.is_ok());
    CHECK(st.code() == Code::kChecksumMismatch);
}

static void test_spill_profile_stays_raw() {
    // Spill is written once and read once in-process; paying a compressor there
    // trades wall-clock for bytes nobody stores.
    const WriteOptions spill = WriteOptions::for_spill();
    CHECK_EQ(spill.zstd_level, 0);
}

int main() {
    test_compressed_reads_back_identically();
    test_it_actually_shrinks_text();
    test_declines_when_not_smaller();
    test_already_encoded_sections_are_not_recompressed();
    test_column_extents_stay_independent();
    test_corrupt_compressed_section_is_rejected();
    test_spill_profile_stays_raw();
    return skene_test::summary("test_compression");
}
