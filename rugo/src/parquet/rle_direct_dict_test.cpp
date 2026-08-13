// Standalone tests for the RLE skip-dense -> §11 Dict-shaped direct builders in
// io_pipeline.hpp (build_direct_rle_dict / build_direct_rle_float_dict).
//
// WHY A STANDALONE DRIVER AND NOT A PARQUET FIXTURE: the RLE skip-dense outputs
// are only produced for max_definition_level == 0 (decode_column.cpp), and rugo's
// writer emits REP_OPTIONAL for every flat column (_parquet_writer.hpp), so no
// rugo-written file can reach this path at all. Driving DecodedColumn directly
// also covers what no real column here would: NaN runs, a value repeating across
// runs, and malformed run tables.
//
// Run with `make rle-dict-test` (or via tests/rugo/test_rle_direct_dict.py).
// Mirrors the c_abi_test.cpp / `make kernel-parity` pattern.
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>

#include "io_pipeline.hpp"

using rugo::ColumnOut;
// DecodedColumn is at global scope (decode.hpp)

static void* A(size_t n) { return std::malloc(n); }
static void  F(void* p) { std::free(p); }

static int failures = 0;
static void check(bool ok, const char* what) {
    std::printf("  %-58s %s\n", what, ok ? "ok" : "FAIL");
    if (!ok) ++failures;
}

// float64: values repeat ACROSS runs — the dedupe must collapse them to one code.
static void test_float64_dedupe() {
    std::printf("float64: values repeating across runs\n");
    DecodedColumn d;
    d.type = "float64";
    d.num_rows = 10;
    d.rle_float64_values = {1.5, 2.5, 1.5, 2.5};   // only TWO distinct values
    d.rle_run_lengths    = {3, 2, 4, 1};           // sums to 10

    ColumnOut out;
    const bool ok = rugo::build_direct_rle_float_dict(d, /*is_f32=*/false, A, F, out);
    check(ok, "build succeeds");
    if (!ok) return;
    check(out.data_length == 2, "data_length == 2 (deduped, not 4 runs)");
    check(out.length == 10, "length == num_rows");
    check(out.validity == nullptr, "no validity (non-nullable path)");
    check(out.dict_sorted == false, "dict_sorted false (first-appearance order)");

    const double* dict = static_cast<const double*>(out.data);
    const uint32_t* codes = static_cast<const uint32_t*>(out.codes);
    const double expect[10] = {1.5,1.5,1.5, 2.5,2.5, 1.5,1.5,1.5,1.5, 2.5};
    bool values_ok = true;
    for (int i = 0; i < 10; ++i)
        if (dict[codes[i]] != expect[i]) values_ok = false;
    check(values_ok, "every row reads back its original value");
    check(codes[0] == codes[5], "same value across runs shares ONE code");
    F(out.data); F(out.codes);
}

// NaN must dedupe by bit pattern: operator== would give every NaN run its own code.
static void test_float64_nan() {
    std::printf("float64: NaN runs dedupe by bit pattern\n");
    DecodedColumn d;
    d.type = "float64";
    d.num_rows = 6;
    const double nan_v = std::nan("");
    d.rle_float64_values = {nan_v, 7.0, nan_v};
    d.rle_run_lengths    = {2, 2, 2};

    ColumnOut out;
    const bool ok = rugo::build_direct_rle_float_dict(d, false, A, F, out);
    check(ok, "build succeeds");
    if (!ok) return;
    check(out.data_length == 2, "NaN + 7.0 => data_length 2, not 3");
    const double* dict = static_cast<const double*>(out.data);
    const uint32_t* codes = static_cast<const uint32_t*>(out.codes);
    check(codes[0] == codes[4], "both NaN runs share one code");
    check(std::isnan(dict[codes[0]]), "NaN survives as NaN");
    check(dict[codes[2]] == 7.0, "7.0 survives");
    F(out.data); F(out.codes);
}

// float32 columns carry their runs widened to double; narrowing back is exact.
static void test_float32() {
    std::printf("float32: narrowing back from the widened run table\n");
    DecodedColumn d;
    d.type = "float32";
    d.num_rows = 4;
    d.rle_float64_values = {1.25, -3.5};
    d.rle_run_lengths    = {1, 3};

    ColumnOut out;
    const bool ok = rugo::build_direct_rle_float_dict(d, /*is_f32=*/true, A, F, out);
    check(ok, "build succeeds");
    if (!ok) return;
    check(out.data_length == 2, "data_length == 2");
    const float* dict = static_cast<const float*>(out.data);
    const uint32_t* codes = static_cast<const uint32_t*>(out.codes);
    check(dict[codes[0]] == 1.25f && dict[codes[3]] == -3.5f, "values exact at float width");
    F(out.data); F(out.codes);
}

// A run table that does not cover exactly num_rows must FAIL, not write partially.
static void test_bad_run_coverage() {
    std::printf("malformed run tables fail loud\n");
    {
        DecodedColumn d;
        d.type = "float64"; d.num_rows = 10;
        d.rle_float64_values = {1.0, 2.0};
        d.rle_run_lengths    = {3, 3};            // sums to 6, not 10
        ColumnOut out;
        check(!rugo::build_direct_rle_float_dict(d, false, A, F, out), "short run table rejected");
    }
    {
        DecodedColumn d;
        d.type = "float64"; d.num_rows = 4;
        d.rle_float64_values = {1.0, 2.0};
        d.rle_run_lengths    = {3, 9};            // overruns num_rows
        ColumnOut out;
        check(!rugo::build_direct_rle_float_dict(d, false, A, F, out), "overrunning run table rejected");
    }
    {
        DecodedColumn d;
        d.type = "float64"; d.num_rows = 4;
        d.rle_float64_values = {1.0};             // fewer values than runs
        d.rle_run_lengths    = {2, 2};
        ColumnOut out;
        check(!rugo::build_direct_rle_float_dict(d, false, A, F, out), "values/runs length mismatch rejected");
    }
    {
        DecodedColumn d;
        d.type = "float64"; d.num_rows = 4;
        d.rle_float64_values = {1.0, 2.0};
        d.rle_run_lengths    = {2, 2};
        d.valid_bits = {0xFF};                    // RLE path is non-nullable by construction
        ColumnOut out;
        check(!rugo::build_direct_rle_float_dict(d, false, A, F, out), "validity bitmap rejected");
    }
}

// The int path shares the dedupe helper — guard it here too.
static void test_int_width() {
    std::printf("int: exact width + dedupe via the shared helper\n");
    DecodedColumn d;
    d.type = "int32";
    d.num_rows = 6;
    d.rle_int64_values = {5, -2, 5};
    d.rle_run_lengths  = {2, 2, 2};

    ColumnOut out;
    const bool ok = rugo::build_direct_rle_dict(d, /*elem_bytes=*/2, A, F, out);  // int16 width
    check(ok, "build succeeds at elem_bytes=2");
    if (!ok) return;
    check(out.data_length == 2, "5 and -2 => data_length 2");
    const int16_t* dict = static_cast<const int16_t*>(out.data);
    const uint32_t* codes = static_cast<const uint32_t*>(out.codes);
    check(dict[codes[0]] == 5 && dict[codes[2]] == -2 && dict[codes[4]] == 5,
          "values exact at int16 width, incl. negative");
    check(codes[0] == codes[4], "repeated value shares one code");
    F(out.data); F(out.codes);
}

int main() {
    test_float64_dedupe();
    test_float64_nan();
    test_float32();
    test_bad_run_coverage();
    test_int_width();
    std::printf("\n%s (%d failure%s)\n", failures ? "FAILED" : "ALL PASS",
                failures, failures == 1 ? "" : "s");
    return failures ? 1 : 0;
}
