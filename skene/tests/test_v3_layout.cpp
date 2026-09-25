// v3-specific behaviour (FORMAT.md v3, docs/SKENE_V3_FORMAT_DESIGN.md):
//
//   - the two-pass writer streaming to a PATH through a scratch file produces
//     the same bytes as the buffer path, and removes its scratch file
//   - the per-file sketch is exactly the KMV of draken's Vector.hash() over
//     every row, a null row contributing its hash once
//   - a sketch is voided, not truncated, when one row group cannot be hashed
//   - a writer that failed part way refuses to finish
//   - a morsel whose columns disagree on length is refused at write time
//   - fetch planning coalesces across columns under a waste budget, and a
//     ranged read refuses a section nobody fetched
//   - a directory fetch planned through block 0 is one range per column that
//     decodes all of block 0 and nothing after it

#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include <unistd.h>

#include "build_vectors.h"
#include "footer_probe.h"
#include "harness.h"
#include "skene/file_io.h"
#include "skene/format.h"
#include "skene/reader.h"
#include "skene/writer.h"

#include "core/kmv_sketch.h"
#include "ops/hash.h"

using namespace skene;
using namespace skene_test;

namespace {

CxxMorsel row_group(int seed, uint32_t rows) {
    std::vector<int64_t> n(rows);
    std::vector<bool> valid(rows);
    std::vector<std::string> s(rows);
    for (uint32_t i = 0; i < rows; ++i) {
        n[i] = static_cast<int64_t>(seed) * 100000 + (i % 97);
        valid[i] = (i % 7) != 0;
        s[i] = "value-" + std::to_string((i * 31 + static_cast<uint32_t>(seed)) % 211)
             + "-with-a-long-enough-tail";
    }
    return morsel_of({{"n", dense_column(n, DRAKEN_INT64, valid)},
                      {"s", string_column(s)}});
}

std::string scratch_dir() {
    char buffer[] = "/tmp/skene_v3_layout_XXXXXX";
    const char* made = mkdtemp(buffer);
    return made == nullptr ? std::string() : std::string(made);
}

// ─── Path output: two passes through a scratch file ─────────────────────────

void test_path_output_matches_buffer_output() {
    const std::string dir = scratch_dir();
    CHECK(!dir.empty());
    const std::string path = dir + "/out.skene";
    const std::string scratch = dir + "/out.stage";

    WriteOptions buffered = WriteOptions::for_fast_reads();
    std::vector<uint8_t> in_memory;
    {
        FileWriter writer;
        CHECK(writer.begin(buffered, &in_memory).is_ok());
        for (int g = 0; g < 6; ++g) CHECK(writer.add_row_group(row_group(g, 3000)).is_ok());
        CHECK(writer.finish().is_ok());
    }

    WriteOptions streamed = buffered;
    streamed.scratch_path = scratch;
    {
        FileWriter writer;
        CHECK(writer.begin(streamed, path).is_ok());
        for (int g = 0; g < 6; ++g) CHECK(writer.add_row_group(row_group(g, 3000)).is_ok());
        CHECK(writer.staged_bytes() > 0);
        CHECK(writer.finish().is_ok());
    }

    std::vector<uint8_t> on_disk;
    CHECK(read_file(path, &on_disk).is_ok());
    // One layout, whichever way it was staged: byte-identical.
    CHECK(on_disk == in_memory);
    // The scratch file is the writer's, and it is gone.
    CHECK(access(scratch.c_str(), F_OK) != 0);
    CHECK(access((path + ".skene-partial").c_str(), F_OK) != 0);

    std::remove(path.c_str());
    rmdir(dir.c_str());
}

void test_scratch_path_rules() {
    std::vector<uint8_t> out;
    // Buffer output stages in memory; a scratch path there is a contradiction.
    {
        WriteOptions options;
        options.scratch_path = "/tmp/never-used";
        FileWriter writer;
        CHECK(!writer.begin(options, &out).is_ok());
    }
    // Path output needs somewhere to stage, and says so rather than guessing.
    {
        FileWriter writer;
        CHECK(!writer.begin(WriteOptions(), std::string("/tmp/skene-no-scratch.skene")).is_ok());
    }
    // A scratch path already in use is another writer's.
    {
        const std::string dir = scratch_dir();
        const std::string taken = dir + "/taken";
        std::vector<uint8_t> one{1};
        CHECK(write_file(taken, one).is_ok());
        WriteOptions options;
        options.scratch_path = taken;
        FileWriter writer;
        CHECK(!writer.begin(options, dir + "/out.skene").is_ok());
        std::remove(taken.c_str());
        std::remove((dir + "/out.skene.skene-partial").c_str());
        rmdir(dir.c_str());
    }
}

// ─── The sketch is draken's Vector.hash() ───────────────────────────────────

void test_sketch_is_the_kmv_of_drakens_row_hashes() {
    std::vector<CxxMorsel> groups;
    for (int g = 0; g < 3; ++g) groups.push_back(row_group(g, 2000));

    std::vector<uint8_t> bytes;
    FileWriter writer;
    CHECK(writer.begin(WriteOptions::for_fast_reads(), &bytes).is_ok());
    for (const CxxMorsel& m : groups) CHECK(writer.add_row_group(m).is_ok());
    CHECK(writer.finish().is_ok());

    FileMetadata meta;
    CHECK(read_metadata(bytes.data(), bytes.size(), &meta).is_ok());
    CHECK_EQ(meta.sketches.size(), size_t{2});

    // Independently: every row of every row group through draken_hash — the
    // kernel Vector.hash() and the catalog's sketches use.
    for (size_t c = 0; c < 2; ++c) {
        draken::KmvSketch<kSketchK, draken::KmvHashFamily::kDrakenVectorHash> expect;
        bool saw_null = false;
        for (const CxxMorsel& m : groups) {
            const DrakenVector& v = m.columns[c].view;
            std::vector<uint64_t> hashes(v.length);
            draken_hash(v, hashes.data(), v.length);
            for (uint64_t h : hashes) expect.add(h);
            if (v.validity != nullptr) saw_null = true;
        }
        const ColumnSketch& got = meta.sketches[c];
        CHECK(got.present());
        CHECK_EQ(got.hash_family, kSketchFamilyDrakenVectorHash);
        CHECK_EQ(got.k, kSketchK);
        CHECK(got.hashes == expect.hashes());

        // The null row's hash is IN the sketch space exactly once when the column
        // has nulls: that is what makes it the catalog's notion of the column.
        if (saw_null && c == 0) {
            DrakenVector one_null = groups[0].columns[0].view;
            uint64_t null_hash = 0;
            // Row 0 is null in every group (i % 7 == 0).
            std::vector<uint64_t> hashes(one_null.length);
            draken_hash(one_null, hashes.data(), one_null.length);
            null_hash = hashes[0];
            size_t occurrences = 0;
            for (uint64_t h : expect.hashes()) if (h == null_hash) ++occurrences;
            CHECK(occurrences <= 1);
        }
    }
}

void test_sketch_is_voided_when_a_row_group_cannot_be_hashed() {
    // Row group 0 hashes; row group 1's string column is length-only, whose
    // payload bytes are not there to hash. A union missing a contributor would
    // under-count with nothing to say so — so the sketch is absent, not partial.
    std::vector<uint8_t> bytes;
    FileWriter writer;
    CHECK(writer.begin(WriteOptions::for_fast_reads(), &bytes).is_ok());
    std::vector<std::string> values = {"alpha-long-value-1", "beta-long-value-22"};
    CHECK(writer.add_row_group(morsel_of({{"s", string_column(values)}})).is_ok());
    CHECK(writer.add_row_group(
        morsel_of({{"s", string_column(values, DRAKEN_VARCHAR, {}, /*elide=*/true)}})).is_ok());
    CHECK(writer.finish().is_ok());

    FileMetadata meta;
    CHECK(read_metadata(bytes.data(), bytes.size(), &meta).is_ok());
    CHECK(!meta.sketches[0].present());
}

// ─── Writer failure ─────────────────────────────────────────────────────────

void test_a_failed_row_group_poisons_the_writer() {
    std::vector<uint8_t> bytes;
    FileWriter writer;
    CHECK(writer.begin(WriteOptions::for_spill(), &bytes).is_ok());
    CHECK(writer.add_row_group(row_group(1, 100)).is_ok());
    // A schema change fails AFTER sections were staged.
    CHECK(!writer.add_row_group(morsel_of({
        {"n", dense_column<int32_t>({1, 2}, DRAKEN_INT32)},
        {"s", string_column({"a", "b"})}})).is_ok());
    // The staged state no longer describes a file.
    CHECK(!writer.add_row_group(row_group(2, 100)).is_ok());
    CHECK(!writer.finish().is_ok());
}

void test_columns_of_different_lengths_are_refused() {
    std::vector<uint8_t> bytes;
    Status st = write_morsel(morsel_of({
        {"a", dense_column<int64_t>({1, 2}, DRAKEN_INT64)},
        {"b", dense_column<int64_t>({1, 2, 3, 4}, DRAKEN_INT64)}}),
        WriteOptions::for_spill(), &bytes);
    CHECK(!st.is_ok());
    CHECK(st.code() == Code::kMalformed);
}

// ─── Fetch planning ─────────────────────────────────────────────────────────

void test_planning_coalesces_across_columns() {
    std::vector<uint8_t> bytes;
    FileWriter writer;
    CHECK(writer.begin(WriteOptions::for_fast_reads(), &bytes).is_ok());
    for (int g = 0; g < 4; ++g) CHECK(writer.add_row_group(row_group(g, 1000)).is_ok());
    CHECK(writer.finish().is_ok());

    FileReader reader;
    CHECK(open_reader(bytes.data(), bytes.size(), &reader).is_ok());
    std::vector<ByteRange> plan;

    // Byte-neutral: the two columns' runs are separated by column 1's directory
    // block, so two ranges.
    CHECK(plan_fetch(reader, {"n", "s"}, {0, 1, 2, 3}, FetchPolicy{}, &plan).is_ok());
    CHECK_EQ(plan.size(), size_t{2});
    uint64_t neutral_bytes = 0;
    for (const ByteRange& r : plan) neutral_bytes += r.bytes;

    // A budget that covers the directory between them: one range, and the only
    // extra bytes are that gap.
    FetchPolicy merge;
    merge.waste_ratio = 0.5;
    CHECK(plan_fetch(reader, {"n", "s"}, {0, 1, 2, 3}, merge, &plan).is_ok());
    CHECK_EQ(plan.size(), size_t{1});
    CHECK(plan[0].bytes > neutral_bytes);

    // A span cap below the merged size keeps them apart.
    merge.max_bytes = neutral_bytes / 2;
    CHECK(plan_fetch(reader, {"n", "s"}, {0, 1, 2, 3}, merge, &plan).is_ok());
    CHECK(plan.size() >= size_t{2});

    // Ascending and distinct is the contract.
    CHECK(!plan_fetch(reader, {"n"}, {2, 1}, FetchPolicy{}, &plan).is_ok());
    CHECK(!plan_fetch(reader, {"n"}, {9}, FetchPolicy{}, &plan).is_ok());
    CHECK(!plan_fetch(reader, {"missing"}, {0}, FetchPolicy{}, &plan).is_ok());
}

void test_ranged_read_refuses_unfetched_bytes() {
    std::vector<uint8_t> bytes;
    FileWriter writer;
    CHECK(writer.begin(WriteOptions::for_fast_reads(), &bytes).is_ok());
    for (int g = 0; g < 2; ++g) CHECK(writer.add_row_group(row_group(g, 1000)).is_ok());
    CHECK(writer.finish().is_ok());

    const uint64_t size = bytes.size();
    uint64_t footer_offset = 0, footer_bytes = 0;
    CHECK(footer_extent(bytes.data() + size - kFileTailBytes, kFileTailBytes, size,
                        &footer_offset, &footer_bytes).is_ok());
    FileReader reader;
    CHECK(open_reader_ranged(bytes.data() + size - kFileTailBytes, kFileTailBytes,
                             bytes.data() + footer_offset,
                             static_cast<size_t>(footer_bytes), footer_offset, size,
                             &reader).is_ok());

    std::vector<FetchedRange> none;
    CxxMorsel out;
    // Directories not attached: refused, naming what is missing.
    CHECK(!read_morsel(reader, 0, ReadOptions(), none, &out).is_ok());
    // A directory range that was not fetched cannot be attached.
    CHECK(!attach_directories(&reader, {"n"}, none).is_ok());

    std::vector<ByteRange> plan;
    CHECK(plan_directory_fetch(reader, {"n"}, false, FetchPolicy{}, &plan).is_ok());
    std::vector<FetchedRange> dirs;
    for (const ByteRange& r : plan)
        dirs.push_back(FetchedRange{r.offset, r.bytes, bytes.data() + r.offset});
    CHECK(attach_directories(&reader, {"n"}, dirs).is_ok());

    // Row group 1's chunk fetched, row group 0 asked for: every section of it
    // lies outside what the caller holds.
    CHECK(plan_fetch(reader, {"n"}, {1}, FetchPolicy{}, &plan).is_ok());
    std::vector<FetchedRange> chunks;
    for (const ByteRange& r : plan)
        chunks.push_back(FetchedRange{r.offset, r.bytes, bytes.data() + r.offset});
    ReadOptions just_n;
    just_n.columns = {"n"};
    Status st = read_morsel(reader, 0, just_n, chunks, &out);
    CHECK(!st.is_ok());
    CHECK(st.message().find("no fetched range") != std::string::npos);
    CHECK(read_morsel(reader, 1, just_n, chunks, &out).is_ok());
    CHECK_EQ(out.num_rows(), uint32_t{1000});
}


// ─── Directory + block 0 in one range per column ────────────────────────────

void test_directory_fetch_through_first_block() {
    // G = 4 (the default) and 6 row groups: block 0 is full, block 1 partial.
    std::vector<uint8_t> bytes;
    FileWriter writer;
    CHECK(writer.begin(WriteOptions::for_fast_reads(), &bytes).is_ok());
    for (int g = 0; g < 6; ++g) CHECK(writer.add_row_group(row_group(g, 1000)).is_ok());
    CHECK(writer.finish().is_ok());

    FileReader whole;
    CHECK(open_reader(bytes.data(), bytes.size(), &whole).is_ok());
    CHECK_EQ(whole.metadata().block_row_groups, uint32_t{4});

    const uint64_t size = bytes.size();
    uint64_t footer_offset = 0, footer_bytes = 0;
    CHECK(footer_extent(bytes.data() + size - kFileTailBytes, kFileTailBytes, size,
                        &footer_offset, &footer_bytes).is_ok());
    FileReader reader;
    CHECK(open_reader_ranged(bytes.data() + size - kFileTailBytes, kFileTailBytes,
                             bytes.data() + footer_offset,
                             static_cast<size_t>(footer_bytes), footer_offset, size,
                             &reader).is_ok());

    // Directories alone, for comparison: one range per column.
    std::vector<ByteRange> directories;
    CHECK(plan_directory_fetch(reader, {"n", "s"}, false, FetchPolicy{}, &directories).is_ok());
    CHECK_EQ(directories.size(), size_t{2});

    std::vector<ByteRange> plan;
    CHECK(plan_directory_fetch(reader, {"n", "s"}, true, FetchPolicy{}, &plan).is_ok());
    CHECK_EQ(plan.size(), size_t{2});   // still one request per column

    // Held as COPIES, so a read reaching outside them cannot land in `bytes`.
    std::vector<std::vector<uint8_t>> store;
    std::vector<FetchedRange> held;
    store.reserve(plan.size());
    for (size_t i = 0; i < plan.size(); ++i) {
        // Each range starts at its directory and goes further.
        CHECK_EQ(plan[i].offset, directories[i].offset);
        CHECK(plan[i].bytes > directories[i].bytes);
        store.emplace_back(bytes.begin() + static_cast<long>(plan[i].offset),
                           bytes.begin() + static_cast<long>(plan[i].offset + plan[i].bytes));
        held.push_back(FetchedRange{plan[i].offset, plan[i].bytes, store.back().data()});
    }
    CHECK(attach_directories(&reader, {"n", "s"}, held).is_ok());

    // It ends exactly where block 0's chunks end: what plan_fetch asks for
    // block 0 lies inside it, and nothing of row group 4 does.
    std::vector<ByteRange> block0;
    CHECK(plan_fetch(reader, {"n", "s"}, {0, 1, 2, 3}, FetchPolicy{}, &block0).is_ok());
    CHECK_EQ(block0.size(), size_t{2});
    for (size_t i = 0; i < block0.size(); ++i)
        CHECK_EQ(block0[i].offset + block0[i].bytes, plan[i].offset + plan[i].bytes);

    ReadOptions both;
    both.columns = {"n", "s"};
    for (uint32_t g = 0; g < 4; ++g) {
        // Values are compared across every fixture family in test_migration.
        CxxMorsel ranged, expected;
        CHECK(read_morsel(reader, g, both, held, &ranged).is_ok());
        CHECK(read_morsel(whole, g, both, &expected).is_ok());
        CHECK_EQ(ranged.num_rows(), expected.num_rows());
        CHECK_EQ(ranged.columns.size(), size_t{2});
    }
    CxxMorsel beyond;
    Status st = read_morsel(reader, 4, both, held, &beyond);
    CHECK(!st.is_ok());
    CHECK(st.message().find("no fetched range") != std::string::npos);

    // Attached columns plan nothing more.
    CHECK(plan_directory_fetch(reader, {"n", "s"}, true, FetchPolicy{}, &plan).is_ok());
    CHECK(plan.empty());
}

}  // namespace

int main() {
    test_path_output_matches_buffer_output();
    test_scratch_path_rules();
    test_sketch_is_the_kmv_of_drakens_row_hashes();
    test_sketch_is_voided_when_a_row_group_cannot_be_hashed();
    test_a_failed_row_group_poisons_the_writer();
    test_columns_of_different_lengths_are_refused();
    test_planning_coalesces_across_columns();
    test_ranged_read_refuses_unfetched_bytes();
    test_directory_fetch_through_first_block();
    return skene_test::summary("test_v3_layout");
}
