// skene vs the rugo Parquet writer, same data, measured rather than assumed.
//
// The design rejected Parquet partly on performance grounds. That claim has to
// be tested, and tested honestly: skene has no general-purpose compressor, so
// the zstd comparison is the unfavourable one and it is reported alongside the
// like-for-like uncompressed figure rather than instead of it.
//
// NOT part of libskene.a. See bench/README.md.

#include <chrono>
#include <cinttypes>
#include <cstdio>
#include <cstring>
#include <functional>
#include <random>
#include <string>
#include <vector>

#include "skene/reader.h"
#include "skene/writer.h"

#include "core/alloc.h"
#include "core/buffers.h"
#include "core/string_slot.h"
#include "core/vector_alloc.h"
#include "core/vector_owner.h"
#include "logical_type.h"

// rugo — the comparison target. Header-only; HAVE_ZSTD is set by the Makefile.
#include "parquet/_parquet_writer.hpp"

using namespace skene;
using Clock = std::chrono::steady_clock;

static double ms_since(Clock::time_point start) {
    return std::chrono::duration<double, std::milli>(Clock::now() - start).count();
}

// ─── Column construction ────────────────────────────────────────────────────
//
// Each case builds the SAME values twice: once as a draken vector for skene, and
// once as a rugo ColumnInput. Any difference in the data would make the
// comparison meaningless, so both are derived from one source array.

struct Case {
    std::string name;
    std::string shape;
    CxxMorsel   morsel;
    std::vector<rugo_pq_write::ColumnInput> parquet_columns;
    // Backing storage the rugo ColumnInputs point into; must outlive the write.
    std::vector<std::vector<int64_t>>              i64_store;
    std::vector<std::vector<rugo_pq_write::StrSlice>> str_store;
    std::vector<std::string>                       str_bytes;
    size_t rows = 0;
};

static void add_int64(Case& c, const char* name, std::vector<int64_t> values) {
    c.rows = values.size();

    void* data = draken_malloc(values.size() * sizeof(int64_t));
    std::memcpy(data, values.data(), values.size() * sizeof(int64_t));
    DrakenVector v = draken_vector_from_dense(
        data, static_cast<uint32_t>(values.size()), DRAKEN_INT64, nullptr);

    CxxColumn column;
    column.view = v;
    column.own  = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(data),
                                                OwnedBuffer<uint8_t>(nullptr));
    column.view = column.own->vec;
    c.morsel.columns.push_back(std::move(column));
    c.morsel.names.push_back(name);

    c.i64_store.push_back(std::move(values));
    rugo_pq_write::ColumnInput input;
    input.name = name;
    input.type = rugo_pq_write::PT_INT64;
    input.i64  = c.i64_store.back().data();
    c.parquet_columns.push_back(input);
}

static void add_varchar(Case& c, const char* name, std::vector<std::string> values) {
    c.rows = values.size();

    size_t arena_bytes = 0;
    for (const std::string& s : values)
        if (s.size() > STR_INLINE_MAX) arena_bytes += s.size();

    const size_t struct_end  = sizeof(DrakenStringArena);
    const size_t slots_bytes = values.size() * sizeof(DrakenStringSlot);
    uint8_t* block = static_cast<uint8_t*>(
        draken_malloc(struct_end + slots_bytes + arena_bytes));
    std::memset(block, 0, struct_end + slots_bytes + arena_bytes);

    DrakenStringArena* arena = reinterpret_cast<DrakenStringArena*>(block);
    DrakenStringSlot*  slots = reinterpret_cast<DrakenStringSlot*>(block + struct_end);
    uint8_t* payload = arena_bytes > 0 ? block + struct_end + slots_bytes : nullptr;

    size_t used = 0;
    for (size_t i = 0; i < values.size(); ++i) {
        const uint8_t* text = reinterpret_cast<const uint8_t*>(values[i].data());
        const uint32_t n = static_cast<uint32_t>(values[i].size());
        if (n <= STR_INLINE_MAX) {
            str_init_inline(&slots[i], text, n);
        } else {
            std::memcpy(payload + used, text, n);
            str_init_extern(&slots[i], text, n, static_cast<uint32_t>(used));
            used += n;
        }
    }
    arena->slots = slots;    arena->arena = payload;
    arena->length = values.size();
    arena->arena_used = used; arena->arena_cap = arena_bytes;
    arena->null_bitmap = nullptr; arena->owns_buffers = 0;
    arena->payloads_elided = 0;   arena->type = DRAKEN_VARCHAR;

    DrakenVector v = draken_vector_from_dense(
        block, static_cast<uint32_t>(values.size()), DRAKEN_VARCHAR, nullptr);
    CxxColumn column;
    column.view = v;
    column.own  = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(block),
                                                OwnedBuffer<uint8_t>(nullptr));
    column.view = column.own->vec;
    c.morsel.columns.push_back(std::move(column));
    c.morsel.names.push_back(name);

    const size_t first = c.str_bytes.size();
    for (std::string& s : values) c.str_bytes.push_back(std::move(s));

    std::vector<rugo_pq_write::StrSlice> slices;
    slices.reserve(c.str_bytes.size() - first);
    for (size_t i = first; i < c.str_bytes.size(); ++i)
        slices.push_back(rugo_pq_write::StrSlice{
            reinterpret_cast<const uint8_t*>(c.str_bytes[i].data()),
            static_cast<uint32_t>(c.str_bytes[i].size())});
    c.str_store.push_back(std::move(slices));

    rugo_pq_write::ColumnInput input;
    input.name    = name;
    input.type    = rugo_pq_write::PT_BYTE_ARRAY;
    input.is_utf8 = true;
    input.strs    = c.str_store.back().data();
    c.parquet_columns.push_back(input);
}

// ─── Measurement ────────────────────────────────────────────────────────────

struct Result {
    size_t skene_bytes = 0,  parquet_plain_bytes = 0, parquet_zstd_bytes = 0;
    double skene_write_ms = 0, parquet_plain_ms = 0, parquet_zstd_ms = 0;
    double skene_read_ms = 0;
};

// Repeat and take the BEST time. A mean over a shared machine measures the
// machine's other work; the minimum is the closest available estimate of the
// cost with nothing else competing.
static double best_of(int repeats, const std::function<void()>& body) {
    double best = 1e18;
    for (int i = 0; i < repeats; ++i) {
        const auto start = Clock::now();
        body();
        const double took = ms_since(start);
        if (took < best) best = took;
    }
    return best;
}

static Result measure(Case& c, int repeats) {
    Result r;

    WriteOptions options;
    options.read_acceleration = true;

    std::vector<uint8_t> skene_bytes;
    r.skene_write_ms = best_of(repeats, [&] {
        skene_bytes.clear();
        Status st = write_morsel(c.morsel, options, &skene_bytes);
        if (!st.is_ok()) { std::fprintf(stderr, "skene: %s\n", st.message().c_str()); std::exit(1); }
    });
    r.skene_bytes = skene_bytes.size();

    r.skene_read_ms = best_of(repeats, [&] {
        CxxMorsel out;
        Status st = read_morsel(skene_bytes.data(), skene_bytes.size(), 0, &out);
        if (!st.is_ok()) { std::fprintf(stderr, "skene read: %s\n", st.message().c_str()); std::exit(1); }
    });

    std::vector<uint8_t> plain;
    r.parquet_plain_ms = best_of(repeats, [&] {
        plain = rugo_pq_write::WriteParquet(c.parquet_columns, c.rows,
                                            rugo_pq_write::CODEC_UNCOMPRESSED);
    });
    r.parquet_plain_bytes = plain.size();

    std::vector<uint8_t> compressed;
    r.parquet_zstd_ms = best_of(repeats, [&] {
        compressed = rugo_pq_write::WriteParquet(c.parquet_columns, c.rows,
                                                 rugo_pq_write::CODEC_ZSTD, 2);
    });
    r.parquet_zstd_bytes = compressed.size();
    return r;
}

static void report(const Case& c, const Result& r) {
    auto ratio = [](size_t a, size_t b) { return b == 0 ? 0.0 : double(a) / double(b); };
    std::printf("\n%s  (%s, %zu rows)\n", c.name.c_str(), c.shape.c_str(), c.rows);
    std::printf("  %-22s %10zu bytes  %8.1f ms write  %8.1f ms read\n",
                "skene", r.skene_bytes, r.skene_write_ms, r.skene_read_ms);
    std::printf("  %-22s %10zu bytes  %8.1f ms write   (skene is %.2fx its size)\n",
                "parquet uncompressed", r.parquet_plain_bytes, r.parquet_plain_ms,
                ratio(r.skene_bytes, r.parquet_plain_bytes));
    std::printf("  %-22s %10zu bytes  %8.1f ms write   (skene is %.2fx its size)\n",
                "parquet zstd-2", r.parquet_zstd_bytes, r.parquet_zstd_ms,
                ratio(r.skene_bytes, r.parquet_zstd_bytes));
}

int main(int argc, char** argv) {
    const size_t rows = (argc > 1) ? std::stoul(argv[1]) : 1000000;
    const int repeats = (argc > 2) ? std::stoi(argv[2]) : 3;

    std::printf("skene vs rugo parquet — %zu rows, best of %d\n", rows, repeats);
    std::printf("(skene carries no general-purpose compressor; the zstd row is "
                "the unfavourable comparison and the production one)\n");

    std::mt19937_64 rng(0x5CE7E5EEDull);

    {   // The shape job results are full of: a handful of repeated labels.
        Case c; c.name = "low-cardinality int64"; c.shape = "50 distinct";
        std::vector<int64_t> v(rows);
        for (size_t i = 0; i < rows; ++i) v[i] = static_cast<int64_t>(rng() % 50);
        add_int64(c, "code", std::move(v));
        report(c, measure(c, repeats));
    }
    {   // Event timestamps: ascending-ish, all distinct — the delta case.
        Case c; c.name = "timestamps"; c.shape = "all distinct, ascending";
        std::vector<int64_t> v(rows);
        int64_t at = 1700000000000000LL;
        for (size_t i = 0; i < rows; ++i) { v[i] = at; at += 1000 + (rng() % 1000); }
        add_int64(c, "event_time", std::move(v));
        report(c, measure(c, repeats));
    }
    {   // High-cardinality random: the worst case for dictionary encoding.
        Case c; c.name = "random int64"; c.shape = "high cardinality";
        std::vector<int64_t> v(rows);
        for (size_t i = 0; i < rows; ++i) v[i] = static_cast<int64_t>(rng());
        add_int64(c, "value", std::move(v));
        report(c, measure(c, repeats));
    }
    {   // Categorical strings — where dictionary encoding pays most.
        Case c; c.name = "low-cardinality varchar"; c.shape = "20 distinct";
        std::vector<std::string> v(rows);
        for (size_t i = 0; i < rows; ++i)
            v[i] = "category_value_" + std::to_string(rng() % 20);
        add_varchar(c, "category", std::move(v));
        report(c, measure(c, repeats));
    }
    {   // Near-unique strings — dictionary encoding cannot help.
        Case c; c.name = "high-cardinality varchar"; c.shape = "near-unique";
        std::vector<std::string> v(rows);
        for (size_t i = 0; i < rows; ++i)
            v[i] = "id-" + std::to_string(rng());
        add_varchar(c, "identifier", std::move(v));
        report(c, measure(c, repeats));
    }
    {   // A realistic mixed row: what a job result actually looks like.
        Case c; c.name = "mixed result table"; c.shape = "4 columns";
        std::vector<int64_t> ids(rows), times(rows), amounts(rows);
        std::vector<std::string> labels(rows);
        int64_t at = 1700000000000000LL;
        for (size_t i = 0; i < rows; ++i) {
            ids[i]     = static_cast<int64_t>(i);
            times[i]   = at; at += 1000 + (rng() % 5000);
            amounts[i] = static_cast<int64_t>(rng() % 1000000);
            labels[i]  = "status_" + std::to_string(rng() % 8);
        }
        add_int64(c, "id", std::move(ids));
        add_int64(c, "event_time", std::move(times));
        add_int64(c, "amount", std::move(amounts));
        add_varchar(c, "status", std::move(labels));
        report(c, measure(c, repeats));
    }

    std::printf("\n");
    return 0;
}
