// Multiple row groups in one file.
//
// A .skene file used to be exactly one row group, and the packing that replaced
// that is only worth having if three things hold. Each has a section below:
//
//   1. every row group round-trips independently and exactly, whatever its
//      types, encoding shapes, or size — including a short final one;
//   2. a row group is addressable on its own — the file footer alone says where
//      each one's bytes and footer are, without opening any of them;
//   3. a file that lies about its row groups is REJECTED, not read as best it
//      can be. This format rebuilds absolute pointers from stored offsets, so
//      "as best it can" is memory corruption.

#include <cstring>
#include <string>
#include <vector>

#include "build_vectors.h"
#include "footer_probe.h"
#include "harness.h"
#include "skene/checksum.h"
#include "skene/format.h"
#include "skene/reader.h"
#include "skene/writer.h"

using namespace skene;
using namespace skene_test;

static void check_mentions(const Status& st, const char* needle) {
    ++skene_test::g_checks;
    if (st.message().find(needle) == std::string::npos)
        skene_test::report(__FILE__, __LINE__, "message content",
                           "did not mention '" + std::string(needle) +
                           "': " + st.message());
}

// ─── Fixtures ───────────────────────────────────────────────────────────────

// One row group per call, with the contents varying by `seed` so a reader that
// silently returned row group 0 for every index would be caught by the values
// rather than only by the row counts.
static CxxMorsel row_group_of(int seed, uint32_t rows) {
    std::vector<int64_t> numbers(rows);
    std::vector<bool>    valid(rows);
    std::vector<std::string> text(rows);
    for (uint32_t i = 0; i < rows; ++i) {
        numbers[i] = static_cast<int64_t>(seed) * 1000000 + i;
        valid[i]   = ((i + static_cast<uint32_t>(seed)) % 5) != 0;
        text[i]    = "rg" + std::to_string(seed) + "-row" + std::to_string(i)
                   + "-padding-past-twelve-bytes";
    }
    // A dictionary whose codes are genuinely per-row-group, so a mixed-up row
    // group shows up as wrong VALUES and not merely a wrong length.
    std::vector<uint32_t> codes(rows);
    for (uint32_t i = 0; i < rows; ++i) codes[i] = (i + static_cast<uint32_t>(seed)) % 3u;

    return morsel_of({
        {"n", dense_column<int64_t>(numbers, DRAKEN_INT64, valid)},
        {"s", string_column(text)},
        {"d", dict_column<int64_t>({seed * 10, seed * 20, seed * 30}, codes, DRAKEN_INT64)},
        {"k", constant_column<int64_t>(seed, rows, DRAKEN_INT64)},
    });
}

static std::vector<uint8_t> write_row_groups(const std::vector<uint32_t>& sizes,
                                             WriteOptions options = WriteOptions::for_spill()) {
    std::vector<uint8_t> bytes;
    FileWriter writer;
    Status st = writer.begin(options, &bytes);
    if (!st.is_ok()) {
        std::fprintf(stderr, "  begin failed: %s\n", st.message().c_str());
        ++skene_test::g_failures;
        return bytes;
    }
    for (size_t i = 0; i < sizes.size(); ++i) {
        st = writer.add_row_group(row_group_of(static_cast<int>(i) + 1, sizes[i]));
        if (!st.is_ok()) {
            std::fprintf(stderr, "  add_row_group %zu failed: %s\n", i,
                         st.message().c_str());
            ++skene_test::g_failures;
            return bytes;
        }
    }
    st = writer.finish();
    if (!st.is_ok()) {
        std::fprintf(stderr, "  finish failed: %s\n", st.message().c_str());
        ++skene_test::g_failures;
    }
    return bytes;
}

// ─── 1. Every row group round-trips exactly ─────────────────────────────────

// The value-level assertions, over a morsel from EITHER read path — so the
// FileReader path is held to exactly the standard the buffer path is.
static void check_contents(const CxxMorsel& out, int seed, uint32_t rows) {
    CHECK_EQ(out.num_rows(), rows);
    CHECK_EQ(out.num_columns(), size_t{4});

    // The dense INT64 column, value for value — including its validity, which is
    // the one buffer an off-by-one row group would leave subtly wrong rather
    // than obviously so.
    const DrakenVector& n = out.columns[0].view;
    const int64_t* values = static_cast<const int64_t*>(n.data);
    size_t wrong = 0, wrong_validity = 0;
    for (uint32_t i = 0; i < rows; ++i) {
        if (values[n.selection[i]] != static_cast<int64_t>(seed) * 1000000 + i) ++wrong;
        const bool expected = ((i + static_cast<uint32_t>(seed)) % 5) != 0;
        const bool actual = n.validity == nullptr
                          || (n.validity[i >> 3] & (1u << (i & 7u))) != 0;
        if (actual != expected) ++wrong_validity;
    }
    CHECK_EQ(wrong, size_t{0});
    CHECK_EQ(wrong_validity, size_t{0});

    // The constant column carries the seed, so reading the wrong row group is
    // visible in one value.
    const DrakenVector& k = out.columns[3].view;
    CHECK_EQ(static_cast<const int64_t*>(k.data)[k.selection[0]], static_cast<int64_t>(seed));
}

static void check_row_group_contents(const std::vector<uint8_t>& bytes,
                                     uint32_t index, int seed, uint32_t rows) {
    CxxMorsel out;
    Status st = read_morsel(bytes.data(), bytes.size(), index, &out);
    ++skene_test::g_checks;
    if (!st.is_ok()) {
        skene_test::report(__FILE__, __LINE__, "read a row group back",
                           "row group " + std::to_string(index) + ": " + st.message());
        return;
    }
    check_contents(out, seed, rows);
}

static void test_every_row_group_round_trips() {
    // Sixteen row groups — the packing default — with a SHORT FINAL ONE, which
    // is the shape every real dataset ends on and the one an "all row groups are
    // the same size" assumption breaks against.
    std::vector<uint32_t> sizes(16, 1000u);
    sizes.back() = 137u;
    const auto bytes = write_row_groups(sizes);

    for (uint32_t i = 0; i < sizes.size(); ++i)
        check_row_group_contents(bytes, i, static_cast<int>(i) + 1, sizes[i]);

    // The file's own totals must agree with the parts.
    FileMetadata meta;
    CHECK(read_metadata(bytes.data(), bytes.size(), &meta).is_ok());
    CHECK_EQ(meta.row_groups.size(), size_t{16});
    CHECK_EQ(meta.row_count, uint64_t{15 * 1000 + 137});
    CHECK_EQ(meta.columns.size(), size_t{4});

    uint64_t expected_first_row = 0;
    for (size_t i = 0; i < meta.row_groups.size(); ++i) {
        CHECK_EQ(meta.row_groups[i].row_count, static_cast<uint64_t>(sizes[i]));
        CHECK_EQ(meta.row_groups[i].first_row, expected_first_row);
        expected_first_row += sizes[i];
    }
}

// The degenerate case has to keep working, because it is what spill writes and
// what write_morsel produces.
static void test_one_row_group_is_a_normal_file() {
    auto m = row_group_of(7, 500);
    std::vector<uint8_t> bytes;
    CHECK(write_morsel(m, WriteOptions::for_spill(), &bytes).is_ok());

    FileMetadata meta;
    CHECK(read_metadata(bytes.data(), bytes.size(), &meta).is_ok());
    CHECK_EQ(meta.row_groups.size(), size_t{1});
    CHECK_EQ(meta.row_count, uint64_t{500});

    check_row_group_contents(bytes, 0, 7, 500);

    // And there is no row group 1 to read.
    CxxMorsel out;
    Status st = read_morsel(bytes.data(), bytes.size(), 1, &out);
    ++skene_test::g_checks;
    if (st.is_ok())
        skene_test::report(__FILE__, __LINE__, "row group 1 of a 1-row-group file",
                           "read SUCCEEDED");
}

// A zero-row row group is legal — a filter can empty one — and must not be
// confused with an absent one. The middle row group here holds no rows while its
// neighbours do, so the row group directory's first_row arithmetic has to step
// over it correctly.
//
// Dense and string columns only, NOT the four-column fixture. A zero-row DICT or
// CONSTANT column does not round trip at all: the writer classifies an empty
// selection as IDENTITY without applying the data_length == length cross-check
// (it is guarded on n > 0), and the reader applies that check unconditionally
// and rejects the file. That is a pre-existing defect in the single-row-group
// path — write_morsel produces a file its own reader refuses — and it is
// recorded separately rather than papered over here. Packing row groups neither
// causes it nor fixes it.
static void test_empty_row_group_round_trips() {
    auto simple = [](int seed, uint32_t rows) {
        std::vector<int64_t> numbers(rows);
        std::vector<std::string> text(rows);
        for (uint32_t i = 0; i < rows; ++i) {
            numbers[i] = static_cast<int64_t>(seed) * 1000 + i;
            text[i] = "value-" + std::to_string(seed) + "-" + std::to_string(i);
        }
        return morsel_of({{"n", dense_column<int64_t>(numbers, DRAKEN_INT64)},
                          {"s", string_column(text)}});
    };

    std::vector<uint8_t> bytes;
    FileWriter writer;
    CHECK(writer.begin(WriteOptions::for_spill(), &bytes).is_ok());
    CHECK(writer.add_row_group(simple(1, 100)).is_ok());
    CHECK(writer.add_row_group(simple(2, 0)).is_ok());
    CHECK(writer.add_row_group(simple(3, 50)).is_ok());
    CHECK(writer.finish().is_ok());

    FileMetadata meta;
    CHECK(read_metadata(bytes.data(), bytes.size(), &meta).is_ok());
    CHECK_EQ(meta.row_groups.size(), size_t{3});
    CHECK_EQ(meta.row_groups[1].row_count, uint64_t{0});
    CHECK_EQ(meta.row_count, uint64_t{150});
    // The empty row group must not shift the ones after it.
    CHECK_EQ(meta.row_groups[1].first_row, uint64_t{100});
    CHECK_EQ(meta.row_groups[2].first_row, uint64_t{100});

    CxxMorsel out;
    CHECK(read_morsel(bytes.data(), bytes.size(), 1, &out).is_ok());
    CHECK_EQ(out.num_rows(), uint32_t{0});

    CxxMorsel third;
    CHECK(read_morsel(bytes.data(), bytes.size(), 2, &third).is_ok());
    CHECK_EQ(third.num_rows(), uint32_t{50});
    const DrakenVector& n = third.columns[0].view;
    CHECK_EQ(static_cast<const int64_t*>(n.data)[n.selection[0]], int64_t{3000});
}

// Every family the format supports, in a file of more than one row group —
// arrays and FP16 in particular, because their children and their descriptors
// are the parts most likely to be captured once and reused wrongly.
static void test_all_families_across_row_groups() {
    // VECTOR_FP16 carries a MANDATORY descriptor, and it is the same object for
    // every row group — so this also proves the file's schema directory captured
    // it once and the per-row-group directories agree with it.
    LogicalType lt;
    lt.kind = LogicalKind::VECTOR;
    lt.dimension = 2u;
    const LogicalType* fp16_type = logical_type_intern(lt);

    std::vector<uint8_t> bytes;
    FileWriter writer;
    CHECK(writer.begin(WriteOptions::for_spill(), &bytes).is_ok());

    for (int seed = 1; seed <= 3; ++seed) {
        const uint32_t rows = static_cast<uint32_t>(seed) * 4u;
        std::vector<bool> bits(rows);
        std::vector<std::vector<int64_t>> arrays(rows);
        std::vector<uint16_t> halves(static_cast<size_t>(rows) * 2u);
        for (uint32_t i = 0; i < rows; ++i) {
            bits[i] = ((i + static_cast<uint32_t>(seed)) % 2) == 0;
            arrays[i] = {static_cast<int64_t>(seed), static_cast<int64_t>(i)};
            halves[i * 2u]      = static_cast<uint16_t>(0x3C00 + seed);
            halves[i * 2u + 1u] = static_cast<uint16_t>(0x3C00 + i);
        }
        CHECK(writer.add_row_group(morsel_of({
            {"b", bool_column(bits)},
            {"a", array_column(arrays)},
            {"f", fp16_column(halves, rows, fp16_type)},
        })).is_ok());
    }
    CHECK(writer.finish().is_ok());

    for (uint32_t g = 0; g < 3; ++g) {
        const uint32_t rows = (g + 1u) * 4u;
        CxxMorsel out;
        Status st = read_morsel(bytes.data(), bytes.size(), g, &out);
        ++skene_test::g_checks;
        if (!st.is_ok()) {
            skene_test::report(__FILE__, __LINE__, "mixed-family row group",
                               st.message());
            continue;
        }
        CHECK_EQ(out.num_rows(), rows);
        CHECK_EQ(out.num_columns(), size_t{3});
        CHECK_EQ(static_cast<int>(out.columns[0].view.type), static_cast<int>(DRAKEN_BOOL));
        CHECK_EQ(static_cast<int>(out.columns[1].view.type), static_cast<int>(DRAKEN_ARRAY));
        CHECK_EQ(static_cast<int>(out.columns[2].view.type),
                 static_cast<int>(DRAKEN_VECTOR_FP16));
    }
}

// Projection is per row group and must stay strict there.
static void test_projection_within_a_row_group() {
    const auto bytes = write_row_groups({64u, 64u, 64u});

    ReadOptions options;
    options.columns = {"k", "n"};
    CxxMorsel out;
    CHECK(read_morsel(bytes.data(), bytes.size(), 2, options, &out).is_ok());
    CHECK_EQ(out.num_columns(), size_t{2});
    CHECK(out.names[0] == "k");

    // The seed of row group 2 is 3.
    const DrakenVector& k = out.columns[0].view;
    CHECK_EQ(static_cast<const int64_t*>(k.data)[k.selection[0]], int64_t{3});

    ReadOptions missing;
    missing.columns = {"nope"};
    CxxMorsel ignored;
    CHECK(!read_morsel(bytes.data(), bytes.size(), 1, missing, &ignored).is_ok());
}

// ─── 2. Row groups are independently addressable ────────────────────────────

static void test_file_footer_locates_every_row_group_without_opening_one() {
    const auto sizes = std::vector<uint32_t>{300u, 300u, 300u, 91u};
    const auto bytes = write_row_groups(sizes, WriteOptions::for_storage());

    FileMetadata meta;
    CHECK(read_metadata(bytes.data(), bytes.size(), &meta).is_ok());
    CHECK_EQ(meta.row_groups.size(), sizes.size());
    CHECK_EQ(meta.block_row_groups, 4u);   // the writer's default G

    // The per-row-group statistics are in the footer, so pruning never opens a
    // directory block. This is the property that keeps row group pruning alive
    // once manifest bounds coarsen to the union over a file.
    for (const RowGroupSummary& rg : meta.row_groups) {
        CHECK_EQ(rg.column_statistics.size(), size_t{4});
        // "n" is the seeded INT64 column: min/max are tracked and differ per row
        // group, which is exactly what makes a row group prunable.
        CHECK(rg.column_statistics[0].present);
        CHECK((rg.column_statistics[0].statistics.flags & (kStatMin | kStatMax))
              == (kStatMin | kStatMax));
    }
    CHECK(meta.row_groups[0].column_statistics[0].statistics.max_ordinal
          < meta.row_groups[1].column_statistics[0].statistics.min_ordinal);

    // COLUMN-MAJOR (FORMAT.md §3): each column node's run — directory block,
    // then every row group's chunk in order — is disjoint from the next node's,
    // so one column over the whole file is one contiguous range.
    uint64_t previous_run_end = kFileHeadBytes;
    for (uint32_t node = 0; node < 4; ++node) {
        ColumnSummaryHead head;
        skene_test::DirectoryBlock dir;
        CHECK(skene_test::column_summary(bytes, node, &head));
        CHECK(skene_test::directory_block(bytes, node, &dir));
        CHECK(head.directory_offset >= previous_run_end);
        CHECK(head.data_offset >= head.directory_offset + head.directory_bytes);
        CHECK_EQ(dir.header.chunk_count, static_cast<uint32_t>(sizes.size()));
        uint64_t previous_chunk_end = head.data_offset;
        for (const ChunkRecord& chunk : dir.chunks) {
            for (uint32_t s = 0; s < chunk.section_count; ++s) {
                const SectionEntry& e = dir.sections[chunk.section_index + s];
                CHECK(e.offset >= previous_chunk_end);
                CHECK(e.offset + e.stored_bytes <= head.data_offset + head.data_bytes);
                previous_chunk_end = e.offset + e.stored_bytes;
            }
        }
        previous_run_end = head.data_offset + head.data_bytes;
    }

    // And the per-column detail of one row group lies inside its column's run.
    RowGroupMetadata detail;
    CHECK(read_row_group_metadata(bytes.data(), bytes.size(), 3, &detail).is_ok());
    CHECK_EQ(detail.row_count, uint64_t{91});
    CHECK_EQ(detail.columns.size(), size_t{4});
    for (uint32_t c = 0; c < 4; ++c) {
        ColumnSummaryHead head;
        CHECK(skene_test::column_summary(bytes, c, &head));
        const ColumnMetadata& column = detail.columns[c];
        CHECK(column.byte_bytes > 0);
        CHECK(column.byte_offset >= head.data_offset);
        CHECK(column.byte_offset + column.byte_bytes <= head.data_offset + head.data_bytes);
    }
}

// A column's chunks for a BLOCK of row groups are one range, and the footer
// records it (FORMAT.md §3.2, §5.6). That is the fetch unit: planning a block
// yields one range per column, whatever the block size.
static void test_blocks_are_one_range_per_column() {
    std::vector<uint32_t> sizes(10, 200u);
    WriteOptions options = WriteOptions::for_fast_reads();
    options.block_row_groups = 4;   // blocks {0-3}, {4-7}, {8-9}
    const auto bytes = write_row_groups(sizes, options);

    FileReader reader;
    CHECK(open_reader(bytes.data(), bytes.size(), &reader).is_ok());
    CHECK_EQ(reader.metadata().block_row_groups, 4u);

    for (uint32_t node = 0; node < 4; ++node) {
        ColumnSummaryHead head;
        CHECK(skene_test::column_summary(bytes, node, &head));
        CHECK_EQ(head.block_count, 3u);
    }

    // One column, one whole block: exactly one range.
    std::vector<ByteRange> plan;
    CHECK(plan_fetch(reader, {"n"}, {4, 5, 6, 7}, FetchPolicy{}, &plan).is_ok());
    CHECK_EQ(plan.size(), size_t{1});

    // A pruned row group in the middle splits the run; the byte-neutral policy
    // does not bridge it.
    CHECK(plan_fetch(reader, {"n"}, {4, 6, 7}, FetchPolicy{}, &plan).is_ok());
    CHECK_EQ(plan.size(), size_t{2});

    // A generous waste ratio bridges it again, at the cost of the gap's bytes.
    FetchPolicy bridge;
    bridge.waste_ratio = 1.0;
    CHECK(plan_fetch(reader, {"n"}, {4, 6, 7}, bridge, &plan).is_ok());
    CHECK_EQ(plan.size(), size_t{1});

    // Whole file, every column: one range per column, whatever the row groups.
    std::vector<uint32_t> all;
    for (uint32_t g = 0; g < sizes.size(); ++g) all.push_back(g);
    CHECK(plan_fetch(reader, {}, all, FetchPolicy{}, &plan).is_ok());
    CHECK_EQ(plan.size(), size_t{4});
}

// ─── 3. A file that lies about its row groups is rejected ───────────────────

static Status expect_rejected(const std::vector<uint8_t>& bytes, uint32_t row_group,
                              const char* what) {
    CxxMorsel out;
    Status st = read_morsel(bytes.data(), bytes.size(), row_group, ReadOptions(), &out);
    ++skene_test::g_checks;
    if (st.is_ok())
        skene_test::report(__FILE__, __LINE__, what,
                           "read SUCCEEDED on a file that should have been rejected");
    return st;
}

// Where the file footer header sits, and where its row group directory starts.
static bool file_footer_positions(const std::vector<uint8_t>& bytes,
                                  size_t* header_at, size_t* directory_at) {
    size_t offset = 0, length = 0;
    if (!skene_test::file_footer_extent(bytes, &offset, &length)) return false;
    FileFooterHeader header;
    if (!skene_test::file_footer_header(bytes, &header)) return false;
    *header_at    = offset;
    *directory_at = offset + sizeof(FileFooterHeader) + header.writer_tag_bytes;
    return true;
}

// Repairs only the FILE footer checksum: these tests corrupt the file index
// itself, so the row group footers below it are still internally consistent and
// only the file-level structural rules can reject the result.
static void repair_file_footer_checksum(std::vector<uint8_t>* bytes) {
    const size_t tail_at = bytes->size() - kFileTailBytes;
    FileTail tail;
    std::memcpy(&tail, bytes->data() + tail_at, sizeof(tail));
    const size_t footer_at = tail_at - tail.footer_bytes;
    tail.footer_checksum = checksum_xxh3_64(bytes->data() + footer_at, tail.footer_bytes);
    std::memcpy(bytes->data() + tail_at, &tail, sizeof(tail));
}

static void test_more_row_groups_than_the_file_holds() {
    const auto bytes = write_row_groups({50u, 50u, 50u});

    size_t header_at = 0, directory_at = 0;
    CHECK(file_footer_positions(bytes, &header_at, &directory_at));

    // Claim four row groups where three were written. The fourth entry's bytes
    // are whatever the schema directory happens to start with, so following it
    // would read a column name as a byte offset.
    std::vector<uint8_t> corrupt = bytes;
    FileFooterHeader header;
    std::memcpy(&header, corrupt.data() + header_at, sizeof(header));
    header.row_group_count = 4;
    std::memcpy(corrupt.data() + header_at, &header, sizeof(header));
    repair_file_footer_checksum(&corrupt);

    Status st = expect_rejected(corrupt, 0, "more row groups than were written");
    CHECK(st.code() == Code::kMalformed || st.code() == Code::kTruncated);
}

static void test_column_summary_pointing_outside_the_file() {
    const auto bytes = write_row_groups({50u, 50u});

    // Rewrites column node 1's summary head, re-seals the footer, and expects
    // the read to be rejected by the footer's structural checks alone.
    auto corrupt_summary = [&](const char* what, auto mutate) {
        std::vector<uint8_t> corrupt = bytes;
        ColumnSummaryHead head;
        size_t at = 0;
        CHECK(skene_test::column_summary(corrupt, 1, &head, &at));
        mutate(&head);
        std::memcpy(corrupt.data() + at, &head, sizeof(head));
        repair_file_footer_checksum(&corrupt);
        return expect_rejected(corrupt, 0, what);
    };

    // A directory block past the end of the object. Unchecked, this is a read
    // of whatever follows the mapping.
    Status st = corrupt_summary("directory block past the object",
        [&](ColumnSummaryHead* h) { h->directory_offset = bytes.size() + 4096; });
    CHECK(st.code() == Code::kMalformed);

    // A data extent that runs past the footer, so a section resolved inside it
    // could address the footer that described it.
    st = corrupt_summary("data extent past the footer",
        [&](ColumnSummaryHead* h) { h->data_bytes = bytes.size(); });
    CHECK(st.code() == Code::kMalformed);

    // A zero-byte directory block cannot hold even its header.
    corrupt_summary("zero-byte directory block",
        [](ColumnSummaryHead* h) { h->directory_bytes = 0; });

    // Reserved bytes are CHECKED, not ignored.
    corrupt_summary("non-zero summary reserved bytes",
        [](ColumnSummaryHead* h) { h->reserved0 = 1; });

    // Runs that overlap: node 1's directory moved back inside node 0's run.
    corrupt_summary("overlapping column runs",
        [](ColumnSummaryHead* h) { h->directory_offset = kFileHeadBytes; });
}

static void test_row_group_row_counts_must_add_up() {
    const auto bytes = write_row_groups({50u, 50u});

    size_t header_at = 0, directory_at = 0;
    CHECK(file_footer_positions(bytes, &header_at, &directory_at));
    // directory_at is the v3 row group TABLE (16-byte entries, FORMAT.md §5.3).

    // first_row that does not follow the row group before it: a reader using it
    // to place rows in file order would silently duplicate or drop a range.
    {
        std::vector<uint8_t> corrupt = bytes;
        RowGroupEntry entry;
        std::memcpy(&entry, corrupt.data() + directory_at + sizeof(RowGroupEntry),
                    sizeof(entry));
        entry.first_row = 999;
        std::memcpy(corrupt.data() + directory_at + sizeof(RowGroupEntry), &entry,
                    sizeof(entry));
        repair_file_footer_checksum(&corrupt);
        Status st = expect_rejected(corrupt, 0, "first_row that does not follow");
        check_mentions(st, "first_row");
    }

    // A file total that disagrees with the sum of its row groups.
    {
        std::vector<uint8_t> corrupt = bytes;
        FileFooterHeader header;
        std::memcpy(&header, corrupt.data() + header_at, sizeof(header));
        header.row_count = 12345;
        std::memcpy(corrupt.data() + header_at, &header, sizeof(header));
        repair_file_footer_checksum(&corrupt);
        expect_rejected(corrupt, 0, "row count that disagrees with the row groups");
    }
}

static void test_out_of_range_row_group_index() {
    const auto bytes = write_row_groups({10u, 10u, 10u});
    Status st = expect_rejected(bytes, 3, "row group index past the last one");
    CHECK(st.code() == Code::kMalformed);
    st = expect_rejected(bytes, 0xFFFFFFFFu, "row group index at UINT32_MAX");
    CHECK(st.code() == Code::kMalformed);
}

// A directory block whose checksum was recorded in the footer, then altered.
// The two live apart precisely so this is detectable.
static void test_directory_block_checksum_is_verified() {
    auto bytes = write_row_groups({50u, 50u});

    ColumnSummaryHead head;
    CHECK(skene_test::column_summary(bytes, 1, &head));
    bytes[head.directory_offset + head.directory_bytes - 4] ^= 0xFF;

    Status st = expect_rejected(bytes, 1, "corrupt directory block");
    CHECK(st.code() == Code::kChecksumMismatch);
    check_mentions(st, "directory block checksum");
}

// The guard against the pre-packing layout. Those files are framed identically
// and their footer checksum verifies, so nothing but this magic separates them.
static void test_pre_packing_layout_is_named_and_refused() {
    auto bytes = write_row_groups({50u});

    size_t header_at = 0, directory_at = 0;
    CHECK(file_footer_positions(bytes, &header_at, &directory_at));

    // What an old file's footer starts with: a row count, not a magic.
    const uint64_t row_count_as_first_field = 50;
    std::memcpy(bytes.data() + header_at, &row_count_as_first_field,
                sizeof(row_count_as_first_field));
    repair_file_footer_checksum(&bytes);

    Status st = expect_rejected(bytes, 0, "a pre-packing single-row-group file");
    CHECK(st.code() == Code::kMalformed);
    // The message has to say what to DO. An operator holding an unreadable
    // object needs the remedy, not the field name.
    check_mentions(st, "Regenerate");
}

// ─── The writer's own guards ────────────────────────────────────────────────

static void test_row_groups_must_share_a_schema() {
    std::vector<uint8_t> bytes;
    FileWriter writer;
    CHECK(writer.begin(WriteOptions::for_spill(), &bytes).is_ok());
    CHECK(writer.add_row_group(morsel_of({
        {"n", dense_column<int64_t>({1, 2, 3}, DRAKEN_INT64)}})).is_ok());

    // A different TYPE under the same name. The file footer's schema directory
    // describes the file, so this would make it describe only half of it.
    {
        Status st = writer.add_row_group(morsel_of({
            {"n", dense_column<int32_t>({1, 2, 3}, DRAKEN_INT32)}}));
        ++skene_test::g_checks;
        if (st.is_ok())
            skene_test::report(__FILE__, __LINE__, "row group with a different type",
                               "add_row_group SUCCEEDED");
        else
            check_mentions(st, "schema");
    }

    // A different NAME.
    {
        Status st = writer.add_row_group(morsel_of({
            {"m", dense_column<int64_t>({1, 2, 3}, DRAKEN_INT64)}}));
        ++skene_test::g_checks;
        if (st.is_ok())
            skene_test::report(__FILE__, __LINE__, "row group with a different name",
                               "add_row_group SUCCEEDED");
    }

    // A different COLUMN COUNT.
    {
        Status st = writer.add_row_group(morsel_of({
            {"n", dense_column<int64_t>({1, 2, 3}, DRAKEN_INT64)},
            {"x", dense_column<int64_t>({4, 5, 6}, DRAKEN_INT64)}}));
        ++skene_test::g_checks;
        if (st.is_ok())
            skene_test::report(__FILE__, __LINE__, "row group with an extra column",
                               "add_row_group SUCCEEDED");
    }
}

static void test_writer_call_order_is_enforced() {
    std::vector<uint8_t> bytes;

    {
        FileWriter writer;
        Status st = writer.add_row_group(row_group_of(1, 4));
        ++skene_test::g_checks;
        if (st.is_ok())
            skene_test::report(__FILE__, __LINE__, "add_row_group before begin",
                               "SUCCEEDED");
    }

    // A file with no row groups describes no data and has no schema: refused
    // rather than written as an object that reads back as a schema-less shell.
    {
        FileWriter writer;
        CHECK(writer.begin(WriteOptions::for_spill(), &bytes).is_ok());
        Status st = writer.finish();
        ++skene_test::g_checks;
        if (st.is_ok())
            skene_test::report(__FILE__, __LINE__, "finish with no row groups",
                               "SUCCEEDED");
    }

    {
        FileWriter writer;
        CHECK(writer.begin(WriteOptions::for_spill(), &bytes).is_ok());
        CHECK(writer.add_row_group(row_group_of(1, 4)).is_ok());
        CHECK(writer.finish().is_ok());
        ++skene_test::g_checks;
        if (writer.finish().is_ok())
            skene_test::report(__FILE__, __LINE__, "finish twice", "SUCCEEDED");
    }
}

// ─── Truncation, over the whole packed file ─────────────────────────────────

// The single-row-group suite sweeps every prefix; this repeats it on a packed
// file, where a prefix can end inside a row group directory rather than inside
// a column directory.
static void test_truncation_of_a_packed_file() {
    const auto bytes = write_row_groups({40u, 40u, 40u});
    size_t accepted = 0;
    for (size_t n = 0; n < bytes.size(); ++n) {
        std::vector<uint8_t> prefix(bytes.begin(), bytes.begin() + n);
        for (uint32_t g = 0; g < 3; ++g) {
            CxxMorsel out;
            if (read_morsel(prefix.data(), prefix.size(), g, ReadOptions(), &out).is_ok())
                ++accepted;
        }
    }
    CHECK_EQ(accepted, size_t{0});
}

// ─── 4. FileReader: the file footer once, then any row group ────────────────

// A FileReader must return what the per-call API returns, for every row group,
// in any order — it only moves WHERE the file footer is verified and parsed.
static void test_file_reader_reads_every_row_group() {
    std::vector<uint32_t> sizes(16, 1000u);
    sizes.back() = 137u;
    const auto bytes = write_row_groups(sizes);

    FileReader reader;
    Status st = open_reader(bytes.data(), bytes.size(), &reader);
    CHECK(st.is_ok());
    if (!st.is_ok()) return;

    // Its metadata is read_metadata's, from the same single parse.
    FileMetadata meta;
    CHECK(read_metadata(bytes.data(), bytes.size(), &meta).is_ok());
    CHECK_EQ(reader.metadata().row_groups.size(), meta.row_groups.size());
    CHECK_EQ(reader.metadata().row_count, meta.row_count);
    for (size_t i = 0; i < meta.row_groups.size(); ++i) {
        CHECK_EQ(reader.metadata().row_groups[i].row_count, meta.row_groups[i].row_count);
        CHECK_EQ(reader.metadata().row_groups[i].first_row, meta.row_groups[i].first_row);
    }

    // Reverse order, so "read group i" cannot be satisfied by "read the next one".
    for (uint32_t i = static_cast<uint32_t>(sizes.size()); i-- > 0;) {
        CxxMorsel out;
        st = read_morsel(reader, i, ReadOptions(), &out);
        ++skene_test::g_checks;
        if (!st.is_ok()) {
            skene_test::report(__FILE__, __LINE__, "FileReader read",
                               "row group " + std::to_string(i) + ": " + st.message());
            continue;
        }
        check_contents(out, static_cast<int>(i) + 1, sizes[i]);
    }

    // A projection goes through the same options path.
    ReadOptions narrow;
    narrow.columns = {"k"};
    CxxMorsel projected;
    CHECK(read_morsel(reader, 5, narrow, &projected).is_ok());
    CHECK_EQ(projected.num_columns(), size_t{1});
    CHECK_EQ(projected.num_rows(), uint32_t{1000});
}

static void test_file_reader_rejects_what_the_buffer_path_rejects() {
    // Out of range: the same Malformed, the same wording.
    const auto bytes = write_row_groups({10u, 10u, 10u});
    FileReader reader;
    CHECK(open_reader(bytes.data(), bytes.size(), &reader).is_ok());
    CxxMorsel out;
    Status st = read_morsel(reader, 3, ReadOptions(), &out);
    CHECK(st.code() == Code::kMalformed);
    check_mentions(st, "row group 3 was requested but this file has 3");
    st = read_morsel(reader, 0xFFFFFFFFu, ReadOptions(), &out);
    CHECK(st.code() == Code::kMalformed);

    // A never-opened reader is refused, not read as an empty file.
    FileReader unopened;
    st = read_morsel(unopened, 0, ReadOptions(), &out);
    CHECK(st.code() == Code::kMalformed);
    check_mentions(st, "never opened");

    // A failed open leaves the reader unopened rather than half-filled.
    std::vector<uint8_t> junk(bytes.size(), 0xAB);
    FileReader failed;
    CHECK(!open_reader(junk.data(), junk.size(), &failed).is_ok());
    CHECK(read_morsel(failed, 0, ReadOptions(), &out).code() == Code::kMalformed);

    // A corrupt FILE footer is caught at open — the verification moved, it did
    // not disappear.
    auto corrupt_file = write_row_groups({50u, 50u});
    size_t f_at = 0, f_len = 0;
    CHECK(skene_test::file_footer_extent(corrupt_file, &f_at, &f_len));
    corrupt_file[f_at + f_len / 2] ^= 0xFF;
    FileReader bad_footer;
    st = open_reader(corrupt_file.data(), corrupt_file.size(), &bad_footer);
    CHECK(st.code() == Code::kChecksumMismatch);

    // A corrupt DIRECTORY BLOCK is caught at a whole-buffer open, which
    // attaches every column's directory against the checksum the footer holds.
    auto corrupt_dir = write_row_groups({50u, 50u});
    ColumnSummaryHead head;
    CHECK(skene_test::column_summary(corrupt_dir, 1, &head));
    corrupt_dir[head.directory_offset + head.directory_bytes - 4] ^= 0xFF;
    FileReader dir_reader;
    st = open_reader(corrupt_dir.data(), corrupt_dir.size(), &dir_reader);
    CHECK(st.code() == Code::kChecksumMismatch);
    check_mentions(st, "directory block checksum");
}

int main() {
    test_every_row_group_round_trips();
    test_one_row_group_is_a_normal_file();
    test_empty_row_group_round_trips();
    test_all_families_across_row_groups();
    test_projection_within_a_row_group();

    test_file_footer_locates_every_row_group_without_opening_one();
    test_blocks_are_one_range_per_column();

    test_more_row_groups_than_the_file_holds();
    test_column_summary_pointing_outside_the_file();
    test_row_group_row_counts_must_add_up();
    test_out_of_range_row_group_index();
    test_directory_block_checksum_is_verified();
    test_pre_packing_layout_is_named_and_refused();

    test_row_groups_must_share_a_schema();
    test_writer_call_order_is_enforced();
    test_truncation_of_a_packed_file();

    test_file_reader_reads_every_row_group();
    test_file_reader_rejects_what_the_buffer_path_rejects();

    return skene_test::summary("test_row_groups");
}
