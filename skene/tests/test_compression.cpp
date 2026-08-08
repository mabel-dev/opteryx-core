// Per-section general-purpose compression (FORMAT.md §7.7, Encoding::kZstd and
// Encoding::kLz4).
//
// Compression is a PURE SIZE OPTIMIZATION: a compressed file must reconstruct
// byte-identically to an uncompressed one. Every test here therefore writes the
// same morsel twice — once raw, once compressed — and compares the results
// rather than checking the compressed file against itself.
//
// It is applied PER SECTION so each column extent stays independently fetchable.
// Whole-file compression would be smaller and would destroy that, which is the
// property `test_column_extents_stay_independent` pins.
//
// EVERY property here is checked against EVERY codec. The contract is the
// format's, not one codec's, and a second codec that only inherited the tests
// written for the first would be a codec whose own failure modes nothing covers.

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

// One writer posture, named so a failure says which codec produced it.
struct Posture {
    const char*  name;
    SectionCodec codec;
    int          level;        // zstd only; the writer REJECTS it on any other
    Encoding     encoding;     // what a compressed section must be tagged
    double       min_text_ratio;  // floor for `comment_like` text, measured
};

static const Posture kRaw{"none", SectionCodec::kNone, 0, Encoding::kPlain, 0.0};

// zstd at 3 rather than for_storage()'s 9: these tests exercise the code path,
// not the ratio curve, and level 3 reaches the same paths for a fraction of the
// suite's wall clock. The floors are set BELOW measured behaviour on this
// fixture, not at it, so they catch compression not reaching the arena rather
// than tracking codec releases.
static const Posture kCodecs[] = {
    {"zstd-3", SectionCodec::kZstd, 3, Encoding::kZstd, 2.0},
    {"lz4",    SectionCodec::kLz4,  0, Encoding::kLz4,  2.0},
};

static std::vector<uint8_t> write_with(const CxxMorsel& m, const Posture& posture,
                                       bool acceleration = true) {
    WriteOptions options;
    options.read_acceleration = acceleration;
    options.codec = posture.codec;
    options.zstd_level = posture.level;
    std::vector<uint8_t> bytes;
    Status st = write_morsel(m, options, &bytes);
    if (!st.is_ok()) {
        std::fprintf(stderr, "  write failed (%s): %s\n", posture.name,
                     st.message().c_str());
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

static void test_compressed_reads_back_identically(const Posture& posture) {
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
    auto raw = write_with(in, kRaw);
    auto packed = write_with(in, posture);
    CHECK(read_morsel(raw.data(), raw.size(), 0, &raw_out).is_ok());
    CHECK(read_morsel(packed.data(), packed.size(), 0, &packed_out).is_ok());

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

static void test_it_actually_shrinks_text(const Posture& posture) {
    auto in = morsel_of({{"comment", string_column(comment_like(20000))}});
    const auto raw = write_with(in, kRaw);
    const auto packed = write_with(in, posture);

    CHECK(packed.size() < raw.size());
    // The arena is the bulk of a text column, and it compresses well — falling
    // under the floor would mean the compression is not reaching it.
    ++skene_test::g_checks;
    if (static_cast<double>(packed.size()) * posture.min_text_ratio
            > static_cast<double>(raw.size()))
        skene_test::report(__FILE__, __LINE__, "compression barely helped text",
                           std::string(posture.name) + ": " +
                           std::to_string(raw.size()) + " -> " +
                           std::to_string(packed.size()));
}

static void test_declines_when_not_smaller(const Posture& posture) {
    // Incompressible data: the codec must store the plain body rather than a
    // larger frame. "Not worth it" is a normal answer.
    std::vector<int64_t> noise(4000);
    uint64_t state = 88172645463325252ull;
    for (size_t i = 0; i < noise.size(); ++i) {
        state ^= state << 13; state ^= state >> 7; state ^= state << 17;
        noise[i] = static_cast<int64_t>(state);
    }
    auto in = morsel_of({{"noise", dense_column<int64_t>(noise, DRAKEN_INT64)}});

    const auto raw = write_with(in, kRaw, /*acceleration=*/false);
    const auto packed = write_with(in, posture, /*acceleration=*/false);

    // Never LARGER than raw: each section falls back independently.
    ++skene_test::g_checks;
    if (packed.size() > raw.size())
        skene_test::report(__FILE__, __LINE__, "compression made the file bigger",
                           std::to_string(raw.size()) + " -> " +
                           std::to_string(packed.size()));

    CxxMorsel out;
    CHECK(read_morsel(packed.data(), packed.size(), 0, &out).is_ok());
    const int64_t* values = static_cast<const int64_t*>(out.columns[0].view.data);
    for (size_t i = 0; i < noise.size(); ++i)
        CHECK_EQ(values[out.columns[0].view.selection[i]], noise[i]);
}

static void test_already_encoded_sections_are_not_recompressed(const Posture& posture) {
    // A bit-packed selection has had its redundancy removed already; layering a
    // general compressor over it costs CPU for nothing, so those bodies keep
    // their encoding.
    std::vector<int64_t> values(20000);
    for (size_t i = 0; i < values.size(); ++i) values[i] = static_cast<int64_t>(i % 40);
    auto in = morsel_of({{"code", dense_column<int64_t>(values, DRAKEN_INT64)}});

    const auto packed = write_with(in, posture);
    RowGroupMetadata meta;
    CHECK(read_row_group_metadata(packed.data(), packed.size(), 0, &meta).is_ok());

    // Round-trips regardless of which encoding each section chose.
    CxxMorsel out;
    CHECK(read_morsel(packed.data(), packed.size(), 0, &out).is_ok());
    const int64_t* got = static_cast<const int64_t*>(out.columns[0].view.data);
    for (size_t i = 0; i < values.size(); ++i)
        CHECK_EQ(got[out.columns[0].view.selection[i]], values[i]);
}

// ─── The property compression must not break ────────────────────────────────

static void test_column_extents_stay_independent(const Posture& posture) {
    // The reason this is per-section rather than whole-file: every column must
    // still be a self-contained byte range. If the file were compressed as a
    // unit, reading one column would mean decompressing all of them.
    auto in = morsel_of({
        {"a", string_column(comment_like(2000))},
        {"b", dense_column<int64_t>(std::vector<int64_t>(2000, 7), DRAKEN_INT64)},
        {"c", string_column(comment_like(2000))},
    });
    const auto packed = write_with(in, posture);

    RowGroupMetadata meta;
    CHECK(read_row_group_metadata(packed.data(), packed.size(), 0, &meta).is_ok());
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
    CHECK(read_morsel(packed.data(), packed.size(), 0, options, &one).is_ok());
    CHECK_EQ(one.num_columns(), size_t{1});
    CHECK_EQ(one.num_rows(), uint32_t{2000});
}

static void test_corrupt_compressed_section_is_rejected(const Posture& posture) {
    auto in = morsel_of({{"s", string_column(comment_like(4000))}});
    auto packed = write_with(in, posture);

    // Corrupting a compressed body must be caught by its checksum before the
    // decompressor ever sees it — a compressed body fed corrupt input can fail
    // in less predictable ways than a plain memcpy. (What happens when the
    // checksum is repaired too, so the codec itself is the only thing left to
    // catch it, is test_reader_rejects' territory.)
    packed[kFileHeadBytes + 64] ^= 0xFF;
    CxxMorsel out;
    Status st = read_morsel(packed.data(), packed.size(), 0, &out);
    CHECK(!st.is_ok());
    CHECK(st.code() == Code::kChecksumMismatch);
}

// A compressed section must actually be TAGGED with the codec that produced it.
// Without this, a posture that silently fell back to plain everywhere would pass
// every round-trip test above — they all compare against the raw file, and a
// file that never compressed anything matches it perfectly.
static void test_the_selected_codec_is_the_one_recorded(const Posture& posture) {
    auto in = morsel_of({{"comment", string_column(comment_like(20000))}});
    // Written WITHOUT read acceleration so the section directory ends exactly at
    // the tail: statistics blobs follow it in the footer (FORMAT.md §5), and
    // walking back from the tail by section_count entries only lands on the
    // directory when there are none. Compression is independent of acceleration.
    const auto packed = write_with(in, posture, /*acceleration=*/false);

    // The section directory now ends at the end of the ROW GROUP's own footer,
    // not at the tail — the tail points at the file index, which sits after
    // every row group footer.
    size_t rg_footer_at = 0, rg_footer_bytes = 0;
    CHECK(skene_test::row_group_footer_extent(packed, 0, &rg_footer_at, &rg_footer_bytes));
    RowGroupFooterHeader fh;
    std::memcpy(&fh, packed.data() + rg_footer_at, sizeof(fh));
    const size_t sections_at = rg_footer_at + rg_footer_bytes
                             - static_cast<size_t>(fh.section_count) * sizeof(SectionEntry);

    size_t compressed_sections = 0;
    for (uint32_t i = 0; i < fh.section_count; ++i) {
        SectionEntry entry;
        std::memcpy(&entry, packed.data() + sections_at + i * sizeof(SectionEntry),
                    sizeof(entry));
        // No section may carry the OTHER codec's tag: the writer offers one.
        for (const Posture& other : kCodecs) {
            if (other.encoding == posture.encoding) continue;
            ++skene_test::g_checks;
            if (entry.encoding == static_cast<uint16_t>(other.encoding))
                skene_test::report(__FILE__, __LINE__,
                                   "a section carries the wrong codec's tag",
                                   std::string("wrote ") + posture.name +
                                   ", found " + other.name);
        }
        if (entry.encoding == static_cast<uint16_t>(posture.encoding))
            ++compressed_sections;
    }
    ++skene_test::g_checks;
    if (compressed_sections == 0)
        skene_test::report(__FILE__, __LINE__,
                           "no section used the selected codec", posture.name);
}

static void test_spill_profile_stays_raw() {
    // Spill is written once and read once in-process; paying a compressor there
    // trades wall-clock for bytes nobody stores.
    const WriteOptions spill = WriteOptions::for_spill();
    CHECK(spill.codec == SectionCodec::kNone);
    CHECK_EQ(spill.zstd_level, 0);
}

// The stored posture's level is a MEASURED decision, not a preference: zstd
// decodes at a rate independent of the level that produced the bytes, so a low
// level gives up ratio for nothing. Pinned here because the previous value (1)
// was the worst available choice on both axes and nothing said so.
static void test_storage_profile_uses_a_high_zstd_level() {
    const WriteOptions storage = WriteOptions::for_storage();
    CHECK(storage.read_acceleration);
    CHECK(storage.codec == SectionCodec::kZstd);
    ++skene_test::g_checks;
    if (storage.zstd_level < 9)
        skene_test::report(__FILE__, __LINE__,
                           "for_storage() writes a low zstd level",
                           std::to_string(storage.zstd_level));

    const WriteOptions fast = WriteOptions::for_fast_reads();
    CHECK(fast.read_acceleration);
    CHECK(fast.codec == SectionCodec::kLz4);
    CHECK_EQ(fast.zstd_level, 0);
}

// `codec` and `zstd_level` describe one setting between them. A combination
// that means two things at once is refused at the door rather than resolved,
// because either resolution writes a file the caller did not ask for and
// nothing downstream records which one happened.
static void test_contradictory_codec_and_level_are_rejected() {
    auto in = morsel_of({{"n", dense_column<int64_t>({1, 2, 3}, DRAKEN_INT64)}});
    std::vector<uint8_t> bytes;

    WriteOptions level_without_zstd;
    level_without_zstd.codec = SectionCodec::kLz4;
    level_without_zstd.zstd_level = 3;
    CHECK(!write_morsel(in, level_without_zstd, &bytes).is_ok());

    WriteOptions level_without_any_codec;
    level_without_any_codec.zstd_level = 3;
    CHECK(!write_morsel(in, level_without_any_codec, &bytes).is_ok());

    WriteOptions zstd_without_level;
    zstd_without_level.codec = SectionCodec::kZstd;
    CHECK(!write_morsel(in, zstd_without_level, &bytes).is_ok());

    WriteOptions out_of_range;
    out_of_range.codec = SectionCodec::kZstd;
    out_of_range.zstd_level = 23;
    CHECK(!write_morsel(in, out_of_range, &bytes).is_ok());

    // And the coherent ones still write.
    CHECK(write_morsel(in, WriteOptions::for_storage(), &bytes).is_ok());
    CHECK(write_morsel(in, WriteOptions::for_fast_reads(), &bytes).is_ok());
}

int main() {
    for (const Posture& posture : kCodecs) {
        std::printf("-- codec %s\n", posture.name);
        test_compressed_reads_back_identically(posture);
        test_it_actually_shrinks_text(posture);
        test_declines_when_not_smaller(posture);
        test_already_encoded_sections_are_not_recompressed(posture);
        test_column_extents_stay_independent(posture);
        test_corrupt_compressed_section_is_rejected(posture);
        test_the_selected_codec_is_the_one_recorded(posture);
    }
    test_spill_profile_stays_raw();
    test_storage_profile_uses_a_high_zstd_level();
    test_contradictory_codec_and_level_are_rejected();
    return skene_test::summary("test_compression");
}
