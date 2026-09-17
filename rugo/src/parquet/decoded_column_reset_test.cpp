// Completeness test for DecodedColumn::reset() — the test named
// `test_decoded_column_reset_is_complete` in decode.hpp's reuse contract.
//
// WHY THIS EXISTS: a DecodedColumn is reused across column decodes via reset(),
// which clear()s its containers rather than destroying them so the next decode
// reuses their capacity. If a member is added to the struct and NOT cleared in
// reset(), the previous column's data survives into the next column's decode.
// That is a silent wrong answer — no crash, no error, just another column's
// bytes — which no parquet fixture reliably reproduces, because whether it
// corrupts anything depends on which columns happen to be adjacent in the file.
// So it is tested here, against the struct directly.
//
// WHY THE C++ LAYER: reset() is a private reuse behaviour of a plain struct with
// no Python entry point. The Cython binding only ever sees a column that has
// already been decoded; it cannot observe whether the buffers it reads were
// cleared between decodes. tests/rugo/ cannot see this at all.
//
// TWO GUARDS, DISJOINT COVERAGE — neither subsumes the other:
//   1. This runtime test catches a .clear() DELETED from reset(). The struct's
//      size is unchanged, so no static_assert fires.
//   2. The static_assert in decode.hpp catches a member ADDED. This test alone
//      cannot: a member the test never fills is trivially "empty" after reset().
// The kVectorCount / kStringCount asserts below chain the two together — bumping
// decode.hpp's counts to fix the build breaks THIS file until the new member is
// added to the X-lists and therefore actually filled and checked.
//
// A memcmp against a default-constructed object is NOT a viable third guard:
// after fill+reset the vectors legitimately hold non-null data pointers and
// non-zero capacity — that is the entire point of reset() — so the bytes differ
// from a fresh object by design.
//
// Run with `make decoded-column-reset-test` (or via
// tests/rugo/test_decoded_column_reset.py).
// Mirrors the rle_direct_dict_test.cpp / `make rle-dict-test` pattern.
#include <cstdio>
#include <cstdint>
#include <string>
#include <vector>

#include "decode.hpp"

static int failures = 0;
static void check(bool ok, const char* what) {
    std::printf("  %-58s %s\n", what, ok ? "ok" : "FAIL");
    if (!ok) ++failures;
}

// ── The member lists ────────────────────────────────────────────────────────
// Every owning container of DecodedColumn, enumerated once. A member missing
// from these lists is a member this test does not cover, and the count asserts
// below make that a compile error rather than a silent coverage hole.
#define DECODED_COLUMN_VECTORS(X)                                              \
    X(valid_bits)          X(int32_values)         X(int64_values)             \
    X(int128_values)       X(string_arena)         X(string_offsets)           \
    X(string_lens)         X(dict_indices)         X(dict_int32_values)        \
    X(dict_int64_values)   X(dict_int128_values)   X(dict_float32_values)      \
    X(dict_float64_values) X(boolean_values)       X(float32_values)           \
    X(float64_values)      X(rep_levels)           X(def_levels)               \
    X(list_def_thresholds) X(string_dict_arena)    X(string_dict_offsets)      \
    X(string_dict_lens)    X(dict_codes_array)     X(rle_int64_values)         \
    X(rle_float64_values)  X(rle_run_lengths)      X(rle_str_arena)            \
    X(rle_str_offsets)     X(rle_str_lens)

#define DECODED_COLUMN_STRINGS(X)                                              \
    X(type)                X(logical_type)         X(error_message)

#define COUNT_ONE(m) +1
constexpr int kVectorCount = 0 DECODED_COLUMN_VECTORS(COUNT_ONE);
constexpr int kStringCount = 0 DECODED_COLUMN_STRINGS(COUNT_ONE);
#undef COUNT_ONE

// Chain to decode.hpp's tripwire. These compare against the constants in the
// HEADER, never against literals — a literal here would let someone bump the
// header's count to clear its sizeof assert while leaving the new member
// uncovered by the X-lists below, which is precisely the vacuous pass this
// file exists to make impossible.
static_assert(kVectorCount == kDecodedColumnVectorMembers,
              "DECODED_COLUMN_VECTORS does not cover every vector member of DecodedColumn: "
              "add the new member to the X-list above (and clear it in reset()).");
static_assert(kStringCount == kDecodedColumnStringMembers,
              "DECODED_COLUMN_STRINGS does not cover every string member of DecodedColumn: "
              "add the new member to the X-list above (and clear it in reset()).");

// Fill depth. Large enough that every vector allocates (so the capacity-retention
// assertions below are meaningful) rather than sitting in any small-size buffer.
static constexpr size_t kFill = 16;

template <typename T>
static void fill_vec(std::vector<T>& v) {
    v.clear();
    for (size_t i = 0; i < kFill; ++i) v.push_back(static_cast<T>(i + 1));
}

// ── Test 1: every owning container is cleared ───────────────────────────────
static void test_containers_cleared() {
    std::printf("reset() clears every owning container\n");
    DecodedColumn d;

#define FILL(m) fill_vec(d.m);
    DECODED_COLUMN_VECTORS(FILL)
#undef FILL
#define FILL_STR(m) d.m = "stale-value-from-the-previous-column";
    DECODED_COLUMN_STRINGS(FILL_STR)
#undef FILL_STR

    // Prove the fill actually took, so an "empty after reset" pass cannot be
    // vacuous for a member whose fill silently did nothing.
    bool all_filled = true;
#define CONFIRM(m) all_filled = all_filled && d.m.size() == kFill;
    DECODED_COLUMN_VECTORS(CONFIRM)
#undef CONFIRM
#define CONFIRM_STR(m) all_filled = all_filled && !d.m.empty();
    DECODED_COLUMN_STRINGS(CONFIRM_STR)
#undef CONFIRM_STR
    check(all_filled, "pre-condition: all 32 containers hold data");

    d.reset();

#define ASSERT_EMPTY(m) check(d.m.empty(), #m " cleared");
    DECODED_COLUMN_VECTORS(ASSERT_EMPTY)
    DECODED_COLUMN_STRINGS(ASSERT_EMPTY)
#undef ASSERT_EMPTY
}

// ── Test 2: every scalar is back to its default ─────────────────────────────
// The base slice-assign resets these wholesale, so this guards the slice-assign
// not being dropped or narrowed — and pins rle_last_code's -1 default, which is
// the one value where "reset" and "zeroed" are NOT the same thing.
static void test_scalars_defaulted() {
    std::printf("reset() restores every scalar to its default\n");
    DecodedColumn d;
    const DecodedColumnMeta fresh{};

    d.is_unsigned = true;          d.int_bit_width = 16;
    d.is_decimal = true;           d.decimal_precision = 38;
    d.decimal_scale = 9;           d.draken_logical_kind = 5;
    d.num_rows = 12345;            d.pages_skipped = 7;
    d.pages_decoded = 9;           d.max_rep_level = 3;
    d.max_def_level = 4;           d.success = true;
    d.code_width = 4;              d.dict_ordered = true;
    d.dict_all_filtered = true;
    d.ext_written = 99;            d.rle_total_length = 4096;
    d.rle_last_code = 77;
    int64_t i64 = 0; double f64 = 0; int32_t i32 = 0; float f32 = 0;
    d.ext_int64 = &i64; d.ext_float64 = &f64; d.ext_int32 = &i32; d.ext_float32 = &f32;

    d.reset();

    check(d.is_unsigned == fresh.is_unsigned, "is_unsigned defaulted");
    check(d.int_bit_width == fresh.int_bit_width, "int_bit_width defaulted");
    check(d.is_decimal == fresh.is_decimal, "is_decimal defaulted");
    check(d.decimal_precision == fresh.decimal_precision, "decimal_precision defaulted");
    check(d.decimal_scale == fresh.decimal_scale, "decimal_scale defaulted");
    check(d.draken_logical_kind == fresh.draken_logical_kind, "draken_logical_kind defaulted");
    check(d.num_rows == fresh.num_rows, "num_rows defaulted");
    check(d.pages_skipped == fresh.pages_skipped, "pages_skipped defaulted");
    check(d.pages_decoded == fresh.pages_decoded, "pages_decoded defaulted");
    check(d.max_rep_level == fresh.max_rep_level, "max_rep_level defaulted");
    check(d.max_def_level == fresh.max_def_level, "max_def_level defaulted");
    check(d.success == fresh.success, "success defaulted");
    check(d.code_width == fresh.code_width, "code_width defaulted");
    check(d.dict_ordered == fresh.dict_ordered, "dict_ordered defaulted");
    check(d.dict_all_filtered == fresh.dict_all_filtered, "dict_all_filtered defaulted");
    check(d.ext_int64 == nullptr, "ext_int64 nulled");
    check(d.ext_float64 == nullptr, "ext_float64 nulled");
    check(d.ext_int32 == nullptr, "ext_int32 nulled");
    check(d.ext_float32 == nullptr, "ext_float32 nulled");
    check(d.ext_written == fresh.ext_written, "ext_written defaulted");
    check(d.rle_total_length == fresh.rle_total_length, "rle_total_length defaulted");
    // Spelled out rather than compared to `fresh`: -1, not 0, is the whole point.
    check(d.rle_last_code == -1, "rle_last_code back to -1 (NOT 0)");
}

// ── Test 3: the reuse contract — capacity survives reset() ──────────────────
// reset() exists to retain buffers. A correct-but-wasteful "fix" (*this = {})
// would pass every assertion above while silently throwing the capacity away on
// every column of every row group. This is what makes it reset() and not
// assignment, so it is tested.
static void test_capacity_retained() {
    std::printf("reset() retains vector capacity (the reason it is not assignment)\n");
    DecodedColumn d;

#define FILL(m) fill_vec(d.m);
    DECODED_COLUMN_VECTORS(FILL)
#undef FILL

    d.reset();

#define ASSERT_CAP(m) check(d.m.capacity() >= kFill, #m " kept its capacity");
    DECODED_COLUMN_VECTORS(ASSERT_CAP)
#undef ASSERT_CAP
}

// ── Test 4: end-to-end — no bytes cross a reuse boundary ────────────────────
// The failure this whole file exists to prevent, stated as one assertion: decode
// column A, reset, decode column B, and column A's values must be gone. Stands
// in for the real reuse loop in decode_column.cpp.
static void test_no_leak_across_reuse() {
    std::printf("no value survives a reuse boundary\n");
    DecodedColumn d;

    // "Column A": a dict-encoded string column.
    d.type = "string";
    d.logical_type = "varchar";
    d.num_rows = 3;
    d.append_string("alpha", 5);
    d.dict_indices = {0, 0, 0};
    d.string_dict_arena = {'a', 'l', 'p', 'h', 'a'};
    d.string_dict_offsets = {0};
    d.string_dict_lens = {5};
    d.rle_last_code = 0;
    d.success = true;

    d.reset();

    // "Column B": a plain int64 column that touches none of the string members.
    d.type = "int64";
    d.num_rows = 2;
    d.int64_values = {10, 20};
    d.success = true;

    check(d.string_arena.empty() && d.string_offsets.empty() && d.string_lens.empty(),
          "column A's dense string arena did not reach column B");
    check(d.string_dict_arena.empty() && d.string_dict_offsets.empty() &&
          d.string_dict_lens.empty(),
          "column A's dictionary did not reach column B");
    check(d.dict_indices.empty(), "column A's dict codes did not reach column B");
    check(d.logical_type.empty(), "column A's logical_type did not reach column B");
    check(d.rle_last_code == -1, "rle_last_code did not carry column A's 0 into column B");
    check(d.int64_values.size() == 2 && d.int64_values[0] == 10,
          "column B's own data is intact");
}

int main() {
    test_containers_cleared();
    test_scalars_defaulted();
    test_capacity_retained();
    test_no_leak_across_reuse();
    std::printf("\n%s (%d failure%s)\n", failures ? "FAILED" : "ALL PASS",
                failures, failures == 1 ? "" : "s");
    return failures ? 1 : 0;
}
