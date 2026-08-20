// Generates the GOLDEN v1 fixture files under skene/tests/fixtures/v1/.
//
// ⛔ ONE-SHOT TOOL, run 2026-08-20 against the LAST v1 writer (tree dc5c7aaf,
// before the v2 format bump). It links skene's writer, so rebuilding it against
// any later tree produces v2 files, NOT v1 fixtures — the committed fixture
// bytes are the artifact, this source is provenance. If the fixtures are ever
// lost, regenerate them from a checkout of dc5c7aaf.
//
// The fixture set exercises every family the reader branches on: fixed-width
// (with and without nulls), BOOL, ARRAY, DRAKEN_NULL, the string family
// (inline, long, elided payloads), all three selection kinds, mandatory logical
// descriptors (TIMESTAMP64, DECIMAL), value-ordering accepted and declined,
// multiple row groups, and all three codec postures.
//
// Build (from the repo root, AT dc5c7aaf):
//
//   make -C skene build/libskene.a
//   c++ -std=c++20 -O2 -Iskene/include -Iskene/tests \
//     -Ithird_party/zstd -Ithird_party/lz4 -Idraken -Idraken/core \
//     -Idraken/simd -Ithird_party/mabel/carchar -Ithird_party/cyan4973 \
//     dev/skene_gen_v1_fixtures.cpp skene/build/libskene.a \
//     -o /tmp/skene_gen_v1_fixtures
//   /tmp/skene_gen_v1_fixtures skene/tests/fixtures/v1
//
// Dev tooling only — never imported by production code (repo rules §5).

#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

#include "skene/writer.h"
#include "skene/file_io.h"
#include "build_vectors.h"

using namespace skene;
using namespace skene_test;

namespace {

constexpr uint32_t kRows = 3000;

// Deterministic pseudo-random stream — fixture bytes must be reproducible.
uint64_t rng_state = 0x736B656E65ULL;  // "skene"
uint64_t next_u64() {
    rng_state ^= rng_state << 13;
    rng_state ^= rng_state >> 7;
    rng_state ^= rng_state << 17;
    return rng_state;
}

CxxMorsel build_row_group(uint32_t seed) {
    rng_state = 0x736B656E65ULL + seed;

    // Fixed-width, ascending-ish so an ordered column takes the delta path.
    std::vector<int64_t> i64(kRows);
    for (uint32_t i = 0; i < kRows; ++i) i64[i] = static_cast<int64_t>(i) * 3 - 1000;

    // Low-cardinality int32 with nulls: ordering accepted, validity present.
    std::vector<int32_t> i32(kRows);
    std::vector<bool>    i32_valid(kRows);
    for (uint32_t i = 0; i < kRows; ++i) {
        i32[i]       = static_cast<int32_t>(next_u64() % 17u);
        i32_valid[i] = (next_u64() % 11u) != 0;
    }

    std::vector<double> f64(kRows);
    for (uint32_t i = 0; i < kRows; ++i)
        f64[i] = static_cast<double>(next_u64() % 100000u) / 7.0;

    std::vector<bool> bits(kRows);
    for (uint32_t i = 0; i < kRows; ++i) bits[i] = (next_u64() & 1u) != 0;

    LogicalType ts;
    ts.kind = LogicalKind::TIMESTAMP;
    ts.unit = TimestampUnit::MILLISECONDS;
    const LogicalType* ts_lt = logical_type_intern(ts);
    std::vector<int64_t> stamps(kRows);
    for (uint32_t i = 0; i < kRows; ++i)
        stamps[i] = 1700000000000LL + static_cast<int64_t>(i) * 60000;

    LogicalType dec;
    dec.kind      = LogicalKind::DECIMAL;
    dec.precision = 12;
    dec.scale     = 2;
    const LogicalType* dec_lt = logical_type_intern(dec);
    std::vector<int64_t> pennies(kRows);
    for (uint32_t i = 0; i < kRows; ++i)
        pennies[i] = static_cast<int64_t>(next_u64() % 1000000u);

    // Repetitive long strings: ordering accepted, dict shape, arena over the
    // compression floor.
    std::vector<std::string> repeat_pool;
    for (int p = 0; p < 40; ++p)
        repeat_pool.push_back("shared-value-" + std::to_string(p)
                              + "-padding-padding-padding-padding");
    std::vector<std::string> str_rep(kRows);
    for (uint32_t i = 0; i < kRows; ++i)
        str_rep[i] = repeat_pool[next_u64() % repeat_pool.size()];

    // Near-unique strings: ordering declined, mixed inline and long forms.
    std::vector<std::string> str_uniq(kRows);
    for (uint32_t i = 0; i < kRows; ++i) {
        const uint64_t r = next_u64();
        if ((r & 3u) == 0)
            str_uniq[i] = "s" + std::to_string(r % 1000u);  // inline form
        else
            str_uniq[i] = "unique-string-payload-" + std::to_string(r)
                          + "-" + std::to_string(i);
    }

    std::vector<std::string> binary(kRows);
    for (uint32_t i = 0; i < kRows; ++i)
        binary[i] = std::string(reinterpret_cast<const char*>(&i), 4)
                    + std::string("\x00\xFFraw", 5);

    // Length-only column: long slots stamped with the elided trap value.
    std::vector<std::string> elided(kRows);
    for (uint32_t i = 0; i < kRows; ++i)
        elided[i] = std::string(14 + (i % 5), 'x');

    std::vector<uint32_t> codes(kRows);
    for (uint32_t i = 0; i < kRows; ++i)
        codes[i] = static_cast<uint32_t>(next_u64() % 5u);

    std::vector<std::vector<int64_t>> arrays(kRows);
    for (uint32_t i = 0; i < kRows; ++i) {
        arrays[i].resize(i % 4);
        for (size_t j = 0; j < arrays[i].size(); ++j)
            arrays[i][j] = static_cast<int64_t>(i + j);
    }

    // DRAKEN_NULL is self-describing: every row is null, no data, no validity.
    CxxColumn nulls;
    {
        DrakenVector v = draken_vector_from_constant(nullptr, kRows, DRAKEN_NULL,
                                                     nullptr);
        nulls.view = v;
        nulls.own  = std::make_shared<VectorOwner>(
            v, OwnedBuffer<void>(nullptr), OwnedBuffer<uint8_t>(nullptr));
    }

    return morsel_of({
        {"i64",       dense_column(i64, DRAKEN_INT64)},
        {"i32_nulls", dense_column(i32, DRAKEN_INT32, i32_valid)},
        {"f64",       dense_column(f64, DRAKEN_FLOAT64)},
        {"flag",      bool_column(bits)},
        {"ts",        dense_column(stamps, DRAKEN_TIMESTAMP64, {}, ts_lt)},
        {"price",     dense_column(pennies, DRAKEN_DECIMAL, {}, dec_lt)},
        {"str_rep",   string_column(str_rep)},
        {"str_uniq",  string_column(str_uniq)},
        {"bin",       string_column(binary, DRAKEN_VARBINARY)},
        {"len_only",  string_column(elided, DRAKEN_VARCHAR, {}, /*elide=*/true)},
        {"dict5",     dict_column(std::vector<int64_t>{10, 20, 30, 40, 50}, codes,
                                  DRAKEN_INT64)},
        {"const1",    constant_column<int64_t>(42, kRows, DRAKEN_INT64)},
        {"arr",       array_column(arrays)},
        {"all_null",  std::move(nulls)},
    });
}

int write_fixture(const std::string& dir, const char* name,
                  const WriteOptions& options) {
    FileWriter writer;
    std::vector<uint8_t> out;
    Status st = writer.begin(options, &out);
    if (st.is_ok()) { CxxMorsel rg = build_row_group(1); st = writer.add_row_group(rg); }
    if (st.is_ok()) { CxxMorsel rg = build_row_group(2); st = writer.add_row_group(rg); }
    if (st.is_ok()) st = writer.finish();
    if (st.is_ok()) st = write_file(dir + "/" + name, out);
    if (!st.is_ok()) {
        std::fprintf(stderr, "%s: %s\n", name, st.message().c_str());
        return 1;
    }
    std::printf("wrote %s/%s (%zu bytes)\n", dir.c_str(), name, out.size());
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::fprintf(stderr, "usage: skene_gen_v1_fixtures <output-dir>\n");
        return 1;
    }
    const std::string dir = argv[1];

    int rc = 0;
    rc |= write_fixture(dir, "v1_spill.skene", WriteOptions::for_spill());

    WriteOptions accel;
    accel.read_acceleration = true;
    accel.writer_tag = "skene_gen_v1_fixtures";
    rc |= write_fixture(dir, "v1_accel_none.skene", accel);

    WriteOptions lz4 = WriteOptions::for_fast_reads();
    lz4.writer_tag = "skene_gen_v1_fixtures";
    rc |= write_fixture(dir, "v1_accel_lz4.skene", lz4);

    WriteOptions zstd = WriteOptions::for_storage();
    zstd.writer_tag = "skene_gen_v1_fixtures";
    rc |= write_fixture(dir, "v1_accel_zstd7.skene", zstd);

    return rc;
}
