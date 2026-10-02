// The streaming writer: FileWriter::begin(options, OutputStream*, prefix).
//
// It exists for files larger than memory or local disk (a vector-index file on a
// Cloud Run worker, whose disk IS memory). What has to hold:
//
//   1. prefix + body IS the file the buffered writer writes — byte for byte, for
//      every storage posture and with the lead column large or small. Not
//      "equivalent": identical, so every guarantee the buffered path has tested
//      carries over without being re-proven here;
//   2. the lead column actually streams — its bytes reach the stream while row
//      groups are still arriving, not at finish();
//   3. misuse is refused, and a failing stream fails the file.

#include <cstring>
#include <string>
#include <vector>

#include "build_vectors.h"
#include "harness.h"
#include "skene/reader.h"
#include "skene/writer.h"

using namespace skene;
using namespace skene_test;

namespace {

struct VectorStream : OutputStream {
    std::vector<uint8_t> bytes;
    size_t               writes = 0;
    bool                 fail = false;
    Status write(const void* data, size_t n) override {
        if (fail) return Status(Code::kMalformed, "stream refused the write");
        ++writes;
        const uint8_t* p = static_cast<const uint8_t*>(data);
        bytes.insert(bytes.end(), p, p + n);
        return Status::ok();
    }
};

const LogicalType* vector_type(uint32_t dims) {
    LogicalType lt;
    lt.kind = LogicalKind::VECTOR;
    lt.dimension = dims;
    return logical_type_intern(lt);
}

// The vector-index shape: a wide fp16 lead column, then the ordinals.
CxxMorsel index_row_group(uint32_t seed, uint32_t rows, uint32_t dims) {
    std::vector<uint16_t> halves(static_cast<size_t>(rows) * dims);
    for (size_t i = 0; i < halves.size(); ++i)
        halves[i] = static_cast<uint16_t>(0x3000u + ((i * 7u + seed * 13u) % 0x0B00u));
    std::vector<int64_t> ordinals(rows);
    for (uint32_t i = 0; i < rows; ++i) ordinals[i] = static_cast<int64_t>(seed) * 100000 + i * 3;
    return morsel_of({
        {"embedding", fp16_column(halves, rows, vector_type(dims))},
        {"ordinal", dense_column<int64_t>(ordinals, DRAKEN_INT64)},
    });
}

// Every family behind a small lead column — the layout must not depend on the
// lead being the big one.
CxxMorsel mixed_row_group(uint32_t seed, uint32_t rows) {
    std::vector<int64_t> n(rows);
    std::vector<bool> valid(rows);
    std::vector<std::string> s(rows);
    std::vector<uint32_t> codes(rows);
    for (uint32_t i = 0; i < rows; ++i) {
        n[i] = static_cast<int64_t>(seed) * 1000 + i;
        valid[i] = (i + seed) % 4 != 0;
        s[i] = "row-" + std::to_string(seed) + "-" + std::to_string(i) + "-past-twelve-bytes";
        codes[i] = (i + seed) % 3u;
    }
    return morsel_of({
        {"n", dense_column<int64_t>(n, DRAKEN_INT64, valid)},
        {"s", string_column(s)},
        {"d", dict_column<int64_t>({1, 2, 3}, codes, DRAKEN_INT64)},
    });
}

WriteOptions fixed(WriteOptions options) {
    // Identity fields pinned so the two writes can be compared byte for byte.
    for (int i = 0; i < 16; ++i) options.file_uuid[i] = static_cast<uint8_t>(i + 1);
    options.created_at_unix_us = 1700000000000000ull;
    return options;
}

std::vector<uint8_t> buffered(const WriteOptions& options, const std::vector<CxxMorsel>& groups) {
    std::vector<uint8_t> out;
    FileWriter writer;
    CHECK(writer.begin(options, &out).is_ok());
    for (const CxxMorsel& g : groups) CHECK(writer.add_row_group(g).is_ok());
    CHECK(writer.finish().is_ok());
    return out;
}

std::vector<uint8_t> streamed(const WriteOptions& options, const std::vector<CxxMorsel>& groups,
                              size_t* body_before_finish = nullptr) {
    VectorStream body;
    std::vector<uint8_t> prefix;
    FileWriter writer;
    CHECK(writer.begin(options, &body, &prefix).is_ok());
    for (const CxxMorsel& g : groups) CHECK(writer.add_row_group(g).is_ok());
    if (body_before_finish != nullptr) *body_before_finish = body.bytes.size();
    CHECK(prefix.empty());                    // the prefix is only known at finish
    CHECK(writer.finish().is_ok());
    CHECK_EQ(prefix.size() % kSectionAlign, uint64_t{0});
    prefix.insert(prefix.end(), body.bytes.begin(), body.bytes.end());
    return prefix;
}

void check_identical(const char* what, const WriteOptions& options,
                     const std::vector<CxxMorsel>& groups) {
    const auto a = buffered(options, groups);
    const auto b = streamed(options, groups);
    ++g_checks;
    if (a != b) {
        size_t at = 0;
        while (at < a.size() && at < b.size() && a[at] == b[at]) ++at;
        report(__FILE__, __LINE__, what,
               "streamed file differs from buffered: sizes " + std::to_string(a.size()) + " vs "
                   + std::to_string(b.size()) + ", first difference at byte " + std::to_string(at));
    }
    // And it reads: every row group, through the ordinary reader.
    FileReader reader;
    CHECK(open_reader(b.data(), b.size(), &reader).is_ok());
    CHECK_EQ(reader.metadata().row_groups.size(), groups.size());
    for (uint32_t i = 0; i < groups.size(); ++i) {
        CxxMorsel out;
        CHECK(read_morsel(reader, i, ReadOptions(), &out).is_ok());
        CHECK_EQ(out.num_rows(), groups[i].num_rows());
    }
}

void test_streamed_file_is_the_buffered_file() {
    std::vector<CxxMorsel> index_groups;
    for (uint32_t g = 0; g < 9; ++g) index_groups.push_back(index_row_group(g, 37u + g * 101u, 24u));
    std::vector<CxxMorsel> mixed_groups;
    for (uint32_t g = 0; g < 5; ++g) mixed_groups.push_back(mixed_row_group(g, 1000u + g * 77u));

    const WriteOptions postures[] = {
        WriteOptions::for_spill(), WriteOptions::for_storage(), WriteOptions::for_fast_reads()};
    for (const WriteOptions& posture : postures) {
        WriteOptions small_blocks = fixed(posture);
        small_blocks.block_row_groups = 2;
        check_identical("index shape", fixed(posture), index_groups);
        check_identical("index shape, blocks of 2", small_blocks, index_groups);
        check_identical("mixed families", fixed(posture), mixed_groups);
        std::vector<CxxMorsel> one;
        one.push_back(index_row_group(3, 401u, 24u));
        check_identical("one row group", fixed(posture), one);
    }
}

void test_lead_column_streams_before_finish() {
    std::vector<CxxMorsel> groups;
    for (uint32_t g = 0; g < 6; ++g) groups.push_back(index_row_group(g, 4000u, 384u));
    size_t before = 0;
    const auto file = streamed(fixed(WriteOptions::for_spill()), groups, &before);
    // 6 x 4000 rows x 384 dims x 2 bytes of fp16 = 18.4 MB. The sink buffers 4 MB,
    // so at least everything but the last buffer-full must already be out.
    const size_t lead = size_t{6} * 4000u * 384u * 2u;
    CHECK(before >= lead - (size_t{4} << 20));
    CHECK(file.size() >= lead);
}

void test_misuse_is_refused() {
    VectorStream body;
    std::vector<uint8_t> prefix;
    {
        FileWriter writer;
        CHECK(!writer.begin(WriteOptions(), nullptr, &prefix).is_ok());
    }
    {
        FileWriter writer;
        CHECK(!writer.begin(WriteOptions(), &body, nullptr).is_ok());
    }
    {
        WriteOptions options;
        options.scratch_path = "/tmp/never-used";
        FileWriter writer;
        CHECK(!writer.begin(options, &body, &prefix).is_ok());
    }
    {
        FileWriter writer;
        CHECK(writer.begin(WriteOptions(), &body, &prefix).is_ok());
        CHECK(!writer.finish().is_ok());       // no row groups, same as every mode
    }
}

void test_a_failing_stream_fails_the_file() {
    std::vector<CxxMorsel> groups;
    for (uint32_t g = 0; g < 3; ++g) groups.push_back(index_row_group(g, 4000u, 384u));
    VectorStream body;
    body.fail = true;
    std::vector<uint8_t> prefix;
    FileWriter writer;
    CHECK(writer.begin(fixed(WriteOptions::for_spill()), &body, &prefix).is_ok());
    bool failed = false;
    for (const CxxMorsel& g : groups) failed = failed || !writer.add_row_group(g).is_ok();
    if (!failed) failed = !writer.finish().is_ok();
    CHECK(failed);
    CHECK(!writer.finish().is_ok());           // and it never finishes afterwards
}

}  // namespace

int main() {
    test_streamed_file_is_the_buffered_file();
    test_lead_column_streams_before_finish();
    test_misuse_is_refused();
    test_a_failing_stream_fails_the_file();
    return summary("test_streaming_writer");
}
