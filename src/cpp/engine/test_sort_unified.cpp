// src/cpp/engine/test_sort_unified.cpp — correctness proof for draken/morsels/sort.hpp.
//
// Standalone assert()-based test (same pattern as test_slice1.cpp — this repo has no
// C++ test framework). Not wired into CI; run it by hand when touching the sort.
//
// Build & run:
//   g++ -O2 -std=c++20 -I. -Idraken -Idraken/core -Isrc/cpp -Ithird_party/cyan4973 \
//       -pthread src/cpp/engine/test_sort_unified.cpp draken/core/vector_alloc.cpp draken/core/mem_account.cpp \
//       -o /tmp/test_sort_unified && /tmp/test_sort_unified
//
// What it proves:
//   1. The AoS fast path and the general SortKeyCmp path produce the IDENTICAL
//      permutation for every eligible key shape — the fast path is a faster route to
//      the same answer, never a different one. This is the load-bearing check.
//   2. The order matches an independently-computed reference (std::stable_sort over
//      the same rows with a hand-written tuple comparator), so "both agree" cannot
//      mean "both wrong the same way".
//   3. Default null placement (NULLS FIRST under ASC, NULLS LAST under DESC) and
//      explicit NULLS FIRST/LAST independent of direction, on every key lane.
//   4. Float sign order: negatives below positives (the bug that made the retired
//      compress()-based key path sort -2.5 above 1.0).
//   5. Both vergesort outcomes: already-sorted input (prepass hit) and shuffled input
//      (prepass declines, stage-2 sort runs) give the same answer.
//   6. Strings, DECIMAL128, and 5+ key columns route through SortKeyCmp correctly.
//   7. take_first (TopN/partial_sort) agrees with the full sort's prefix.

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include "core/mem_account.h"
#include "morsels/sort.hpp"

static int g_checks = 0;
#define CHECK(cond, what)                                                          \
    do {                                                                           \
        ++g_checks;                                                                \
        if (!(cond)) {                                                             \
            std::fprintf(stderr, "FAIL [%s:%d] %s\n", __FILE__, __LINE__, (what)); \
            std::abort();                                                          \
        }                                                                          \
    } while (0)

// ---- morsel construction helpers ---------------------------------------------------

static CxxColumn col_f64(const std::vector<double>& vals, const std::vector<bool>& valid) {
    uint32_t n = static_cast<uint32_t>(vals.size());
    auto* data = static_cast<double*>(draken_malloc(sizeof(double) * (n ? n : 1)));
    uint8_t* vbits = nullptr;
    bool any_null = std::any_of(valid.begin(), valid.end(), [](bool v) { return !v; });
    if (any_null) {
        size_t vb = (static_cast<size_t>(n) + 7) / 8;
        vbits = static_cast<uint8_t*>(draken_malloc(vb ? vb : 1));
        std::memset(vbits, 0xFF, vb ? vb : 1);
    }
    for (uint32_t i = 0; i < n; ++i) {
        data[i] = vals[i];
        if (vbits && !valid[i]) vbits[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7)));
    }
    DrakenVector v = draken_vector_from_dense(data, n, DRAKEN_FLOAT64, vbits);
    CxxColumn c;
    c.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(data), OwnedBuffer<uint8_t>(vbits));
    c.view = c.own->vec;
    return c;
}

static CxxColumn col_i64(const std::vector<int64_t>& vals, const std::vector<bool>& valid) {
    uint32_t n = static_cast<uint32_t>(vals.size());
    auto* data = static_cast<int64_t*>(draken_malloc(sizeof(int64_t) * (n ? n : 1)));
    uint8_t* vbits = nullptr;
    bool any_null = std::any_of(valid.begin(), valid.end(), [](bool v) { return !v; });
    if (any_null) {
        size_t vb = (static_cast<size_t>(n) + 7) / 8;
        vbits = static_cast<uint8_t*>(draken_malloc(vb ? vb : 1));
        std::memset(vbits, 0xFF, vb ? vb : 1);
    }
    for (uint32_t i = 0; i < n; ++i) {
        data[i] = vals[i];
        if (vbits && !valid[i]) vbits[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7)));
    }
    DrakenVector v = draken_vector_from_dense(data, n, DRAKEN_INT64, vbits);
    CxxColumn c;
    c.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(data), OwnedBuffer<uint8_t>(vbits));
    c.view = c.own->vec;
    return c;
}

// VARCHAR column in the canonical consolidated layout:
// [DrakenStringArena header | slots[n] | arena bytes]
static CxxColumn col_str(const std::vector<std::string>& vals, const std::vector<bool>& valid) {
    uint32_t n = static_cast<uint32_t>(vals.size());
    size_t total_arena = 0;
    for (uint32_t i = 0; i < n; ++i)
        if (valid[i] && vals[i].size() > STR_INLINE_MAX) total_arena += vals[i].size();

    size_t slots_off = sizeof(DrakenStringArena);
    size_t arena_off = slots_off + static_cast<size_t>(n ? n : 1) * sizeof(DrakenStringSlot);
    uint8_t* blk = static_cast<uint8_t*>(draken_malloc(arena_off + total_arena));
    auto* sa = reinterpret_cast<DrakenStringArena*>(blk);
    auto* slots = reinterpret_cast<DrakenStringSlot*>(blk + slots_off);
    uint8_t* arena = total_arena ? blk + arena_off : nullptr;
    sa->slots = slots; sa->arena = arena; sa->length = n;
    sa->arena_used = total_arena; sa->arena_cap = total_arena;
    sa->null_bitmap = nullptr; sa->owns_buffers = 0; sa->type = DRAKEN_VARCHAR;

    uint8_t* vbits = nullptr;
    bool any_null = std::any_of(valid.begin(), valid.end(), [](bool v) { return !v; });
    if (any_null) {
        size_t vb = (static_cast<size_t>(n) + 7) / 8;
        vbits = static_cast<uint8_t*>(draken_malloc(vb ? vb : 1));
        std::memset(vbits, 0xFF, vb ? vb : 1);
    }

    size_t pos = 0;
    for (uint32_t i = 0; i < n; ++i) {
        if (!valid[i]) {
            std::memset(&slots[i], 0, sizeof(DrakenStringSlot));
            if (vbits) vbits[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7)));
            continue;
        }
        const std::string& s = vals[i];
        const uint8_t* sp = reinterpret_cast<const uint8_t*>(s.data());
        if (s.size() <= STR_INLINE_MAX) {
            str_init_inline(&slots[i], sp, static_cast<uint32_t>(s.size()));
        } else {
            std::memcpy(arena + pos, s.data(), s.size());
            str_init_extern(&slots[i], sp, static_cast<uint32_t>(s.size()),
                            static_cast<uint32_t>(pos));
            pos += s.size();
        }
    }

    uint32_t* sel = static_cast<uint32_t*>(draken_malloc((n ? n : 1) * sizeof(uint32_t)));
    for (uint32_t i = 0; i < n; ++i) sel[i] = i;
    DrakenVector v;
    v.data = sa; v.selection = sel; v.data_length = n; v.length = n;
    v.validity = vbits; v.type = DRAKEN_VARCHAR;
    v.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
    CxxColumn c;
    c.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(blk),
                                          OwnedBuffer<uint8_t>(vbits), OwnedBuffer<void>(sel));
    c.view = c.own->vec;
    return c;
}

static CxxColumn col_dec128(const std::vector<__int128>& vals) {
    uint32_t n = static_cast<uint32_t>(vals.size());
    auto* data = static_cast<__int128*>(draken_malloc(sizeof(__int128) * (n ? n : 1)));
    for (uint32_t i = 0; i < n; ++i) data[i] = vals[i];
    DrakenVector v = draken_vector_from_dense(data, n, DRAKEN_DECIMAL128, nullptr);
    CxxColumn c;
    c.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(data), OwnedBuffer<uint8_t>(nullptr));
    c.view = c.own->vec;
    return c;
}

static MorselPtr make_morsel(std::vector<CxxColumn> cols) {
    auto m = std::make_shared<CxxMorsel>();
    for (size_t i = 0; i < cols.size(); ++i) {
        m->columns.push_back(std::move(cols[i]));
        m->names.push_back("c" + std::to_string(i));
    }
    return m;
}

// ---- the two comparators, run over the same keys ------------------------------------

// The public entry point — picks AoS or SortKeyCmp itself. Callers below that pass
// take_first == SIZE_MAX over AoS-eligible keys are exercising the AoS path.
// Fixed width so the parallel stable-sort path is exercised the same on every host.
static constexpr unsigned kSortThreads = 4;

static std::vector<uint32_t> sort_via_dispatch(const std::vector<SortKeyColumn>& keys, size_t n,
                                               size_t take_first) {
    std::vector<uint32_t> perm(n);
    std::iota(perm.begin(), perm.end(), 0u);
    sort_perm(keys, perm, take_first, kSortThreads);
    return perm;
}

static std::vector<uint32_t> sort_via_generic(const std::vector<SortKeyColumn>& keys, size_t n,
                                              size_t take_first) {
    std::vector<uint32_t> perm(n);
    std::iota(perm.begin(), perm.end(), 0u);
    sort_perm_cmp(SortKeyCmp{keys}, perm, take_first, kSortThreads);
    return perm;
}

static std::vector<SortKeyColumn> keys_of(const std::vector<MorselPtr>& ms,
                                          const std::vector<SortKeySpec>& spec, size_t n) {
    ErrCtx err;
    std::vector<SortKeyColumn> keys;
    bool ok = build_sort_keys(ms, spec, n, keys, err);
    CHECK(ok, err.msg ? err.msg : "build_sort_keys failed");
    return keys;
}

// ---- tests ---------------------------------------------------------------------------

// The load-bearing invariant: for every AoS-eligible shape, the fast path and the
// general path must agree exactly.
static void test_aos_matches_generic() {
    std::mt19937_64 rng(99);
    for (int nparts = 1; nparts <= 4; ++nparts) {
        for (int null_pct : {0, 20}) {
            for (int trial = 0; trial < 4; ++trial) {
                const size_t n = 4000;
                std::vector<CxxColumn> cols;
                std::vector<SortKeySpec> spec;
                for (int k = 0; k < nparts; ++k) {
                    std::vector<int64_t> vals(n);
                    std::vector<bool> valid(n, true);
                    // low cardinality -> lots of ties -> the later parts actually decide
                    std::uniform_int_distribution<int64_t> d(-20, 20);
                    std::uniform_int_distribution<int> p(1, 100);
                    for (size_t i = 0; i < n; ++i) {
                        vals[i] = d(rng);
                        if (null_pct && p(rng) <= null_pct) valid[i] = false;
                    }
                    cols.push_back(col_i64(vals, valid));
                    // Direction and null placement vary independently, so across
                    // the trials every (ASC|DESC) x (NULLS FIRST|LAST) pair is hit.
                    spec.push_back({static_cast<size_t>(k), (k + trial) % 2 == 0,
                                    (k + trial / 2) % 2 == 0});
                }
                std::vector<MorselPtr> ms{make_morsel(std::move(cols))};
                auto keys = keys_of(ms, spec, n);
                CHECK(sort_via_dispatch(keys, n, SIZE_MAX) == sort_via_generic(keys, n, SIZE_MAX),
                      "AoS and generic comparators disagree");
            }
        }
    }
}

// Both agree — but are they RIGHT? Independent reference comparator.
static void test_matches_independent_reference() {
    const size_t n = 3000;
    std::mt19937_64 rng(7);
    std::uniform_int_distribution<int64_t> d(-50, 50);
    std::uniform_int_distribution<int> p(1, 100);

    std::vector<int64_t> a(n), b(n);
    std::vector<bool> va(n, true), vb(n, true);
    for (size_t i = 0; i < n; ++i) {
        a[i] = d(rng); b[i] = d(rng);
        if (p(rng) <= 15) va[i] = false;
    }
    std::vector<MorselPtr> ms{make_morsel({col_i64(a, va), col_i64(b, vb)})};
    std::vector<SortKeySpec> spec{{0, true, true}, {1, false, false}};   // c0 ASC, c1 DESC
    auto keys = keys_of(ms, spec, n);

    std::vector<uint32_t> want(n);
    std::iota(want.begin(), want.end(), 0u);
    std::stable_sort(want.begin(), want.end(), [&](uint32_t x, uint32_t y) {
        if (va[x] != va[y]) return !va[x];        // NULLS FIRST under ASC
        if (va[x] && a[x] != a[y]) return a[x] < a[y];
        return b[x] > b[y];                        // DESC on c1
    });

    CHECK(sort_via_dispatch(keys, n, SIZE_MAX) == want, "AoS disagrees with reference");
    CHECK(sort_via_generic(keys, n, SIZE_MAX) == want, "generic disagrees with reference");
}

// NULLS FIRST under ASC, NULLS LAST under DESC.
static void test_null_placement() {
    std::vector<int64_t> v{3, 0, 1, 0, 2};
    std::vector<bool> valid{true, false, true, false, true};
    std::vector<MorselPtr> ms{make_morsel({col_i64(v, valid)})};

    auto asc_keys = keys_of(ms, {{0, true, true}}, 5);
    auto asc = sort_via_dispatch(asc_keys, 5, SIZE_MAX);
    CHECK(!valid[asc[0]] && !valid[asc[1]], "ASC must place NULLs first");
    CHECK(v[asc[2]] == 1 && v[asc[3]] == 2 && v[asc[4]] == 3, "ASC value order wrong");

    auto desc_keys = keys_of(ms, {{0, false, false}}, 5);
    auto desc = sort_via_dispatch(desc_keys, 5, SIZE_MAX);
    CHECK(!valid[desc[3]] && !valid[desc[4]], "DESC must place NULLs last");
    CHECK(v[desc[0]] == 3 && v[desc[1]] == 2 && v[desc[2]] == 1, "DESC value order wrong");
}

// The regression that motivated this work: negatives must sort below positives.
// The retired compress()-based key path put -2.5 between 1.0 and 3.14.
static void test_float_sign_order() {
    std::vector<double> v{3.14, 1.0, -2.5, 0.0, 100.0, -0.0000001, -1e300};
    std::vector<bool> valid(v.size(), true);
    std::vector<MorselPtr> ms{make_morsel({col_f64(v, valid)})};
    size_t n = v.size();

    auto keys = keys_of(ms, {{0, true, true}}, n);
    auto got = sort_via_dispatch(keys, n, SIZE_MAX);
    std::vector<double> sorted;
    for (uint32_t i : got) sorted.push_back(v[i]);
    std::vector<double> want = v;
    std::sort(want.begin(), want.end());
    CHECK(sorted == want, "float ascending order wrong (sign-bit handling)");
    CHECK(sorted.front() == -1e300, "most-negative must sort first");
    CHECK(sorted.back() == 100.0, "largest positive must sort last");

    // and the specific historical failure, stated directly
    auto pos = [&](double x) {
        return std::find(sorted.begin(), sorted.end(), x) - sorted.begin();
    };
    CHECK(pos(-2.5) < pos(0.0), "-2.5 must sort before 0.0");
    CHECK(pos(-2.5) < pos(1.0), "-2.5 must sort before 1.0 (the compress() bug)");
    CHECK(pos(-0.0000001) < pos(0.0), "small negative must sort before zero");
}

// Both vergesort outcomes must give the same answer: already-sorted input (prepass
// hits and returns early) vs shuffled input (prepass declines, stage 2 runs).
static void test_vergesort_hit_and_miss_agree() {
    const size_t n = 6000;
    std::vector<int64_t> base(n);
    for (size_t i = 0; i < n; ++i) base[i] = static_cast<int64_t>(i / 3);   // ties present
    std::vector<bool> valid(n, true);

    // already ascending -> single run -> prepass hit
    {
        std::vector<MorselPtr> ms{make_morsel({col_i64(base, valid)})};
        auto keys = keys_of(ms, {{0, true, true}}, n);
        auto got = sort_via_dispatch(keys, n, SIZE_MAX);
        std::vector<uint32_t> want(n);
        std::iota(want.begin(), want.end(), 0u);
        CHECK(got == want, "sorted input must come back as the identity permutation");
    }
    // strictly descending -> one reversed run -> prepass hit via the reversal arm
    {
        std::vector<int64_t> desc(n);
        for (size_t i = 0; i < n; ++i) desc[i] = static_cast<int64_t>(n - i);
        std::vector<MorselPtr> ms{make_morsel({col_i64(desc, valid)})};
        auto keys = keys_of(ms, {{0, true, true}}, n);
        auto got = sort_via_dispatch(keys, n, SIZE_MAX);
        for (size_t i = 0; i + 1 < n; ++i)
            CHECK(desc[got[i]] <= desc[got[i + 1]], "reversed-run output not ascending");
    }
    // shuffled -> many runs -> prepass declines, stage 2 sorts
    {
        std::vector<int64_t> shuf = base;
        std::shuffle(shuf.begin(), shuf.end(), std::mt19937_64(5));
        std::vector<MorselPtr> ms{make_morsel({col_i64(shuf, valid)})};
        auto keys = keys_of(ms, {{0, true, true}}, n);
        auto aos = sort_via_dispatch(keys, n, SIZE_MAX);
        auto gen = sort_via_generic(keys, n, SIZE_MAX);
        CHECK(aos == gen, "AoS/generic disagree on the vergesort-miss path");
        for (size_t i = 0; i + 1 < n; ++i)
            CHECK(shuf[aos[i]] <= shuf[aos[i + 1]], "stage-2 output not ascending");
    }
}

// Strings are not AoS-eligible — they must route to SortKeyCmp and still be correct,
// including the inline (<=12B) / arena (>12B) split.
static void test_string_keys() {
    std::vector<std::string> v{
        "pear", "apple", "", "zebra", "apple",
        "a-very-long-string-past-twelve-bytes-B",
        "a-very-long-string-past-twelve-bytes-A",
        "banana",
    };
    std::vector<bool> valid(v.size(), true);
    valid[2] = false;                      // the "" slot becomes NULL
    size_t n = v.size();
    std::vector<MorselPtr> ms{make_morsel({col_str(v, valid)})};
    auto keys = keys_of(ms, {{0, true, true}}, n);
    CHECK(!aos_keys_eligible(keys), "string keys must NOT be AoS-eligible");

    std::vector<uint32_t> perm(n);
    std::iota(perm.begin(), perm.end(), 0u);
    sort_perm(keys, perm, SIZE_MAX, kSortThreads);

    CHECK(!valid[perm[0]], "NULL string must sort first under ASC");
    std::vector<std::string> got;
    for (size_t i = 1; i < n; ++i) got.push_back(v[perm[i]]);
    std::vector<std::string> want;
    for (size_t i = 0; i < n; ++i) if (valid[i]) want.push_back(v[i]);
    std::sort(want.begin(), want.end());
    CHECK(got == want, "string ordering wrong (byte-wise, shorter prefix first)");
}

// DECIMAL128 uses the __int128 lane — also not AoS-eligible.
static void test_decimal128_keys() {
    __int128 big = (static_cast<__int128>(1) << 100);
    std::vector<__int128> v{big, -big, 0, big - 1, -1};
    size_t n = v.size();
    std::vector<MorselPtr> ms{make_morsel({col_dec128(v)})};
    auto keys = keys_of(ms, {{0, true, true}}, n);
    CHECK(!aos_keys_eligible(keys), "DECIMAL128 keys must NOT be AoS-eligible");

    std::vector<uint32_t> perm(n);
    std::iota(perm.begin(), perm.end(), 0u);
    sort_perm(keys, perm, SIZE_MAX, kSortThreads);
    for (size_t i = 0; i + 1 < n; ++i)
        CHECK(v[perm[i]] <= v[perm[i + 1]], "DECIMAL128 not ascending");
    CHECK(v[perm[0]] == -big, "most-negative int128 must sort first");
}

// Explicit placement is independent of direction: every (ASC|DESC) x (NULLS
// FIRST|LAST) pair, on the AoS lane (int64), the generic lane (string, forced by
// is_str) and the DECIMAL128 lane, full sort and TopN.
static void test_null_placement_explicit() {
    std::vector<int64_t> v{3, 0, 1, 0, 2};
    std::vector<bool> valid{true, false, true, false, true};
    std::vector<std::string> sv{"c", "", "a", "", "b"};
    std::vector<__int128> dv{3, 0, 1, 0, 2};
    std::vector<MorselPtr> ms_i{make_morsel({col_i64(v, valid)})};
    std::vector<MorselPtr> ms_s{make_morsel({col_str(sv, valid)})};
    for (bool asc : {true, false}) {
        for (bool nf : {true, false}) {
            for (int lane = 0; lane < 2; ++lane) {
                auto keys = keys_of(lane == 0 ? ms_i : ms_s, {{0, asc, nf}}, 5);
                for (size_t take : {SIZE_MAX, size_t{1}, size_t{2}, size_t{3}}) {
                    auto got = sort_via_dispatch(keys, 5, take);
                    auto gen = sort_via_generic(keys, 5, take);
                    const size_t m = take == SIZE_MAX ? 5 : take;
                    // Expected sequence of the first m rows: two NULLs at the chosen
                    // end, values 1,2,3 in the chosen direction.
                    std::vector<int> want;   // -1 = NULL, else the int value
                    std::vector<int> vals = asc ? std::vector<int>{1, 2, 3}
                                                : std::vector<int>{3, 2, 1};
                    if (nf) { want = {-1, -1}; want.insert(want.end(), vals.begin(), vals.end()); }
                    else    { want = vals; want.push_back(-1); want.push_back(-1); }
                    for (size_t i = 0; i < m; ++i) {
                        int g = valid[got[i]] ? static_cast<int>(v[got[i]]) : -1;
                        int h = valid[gen[i]] ? static_cast<int>(v[gen[i]]) : -1;
                        CHECK(g == want[i], "explicit null placement: dispatch order wrong");
                        CHECK(h == want[i], "explicit null placement: generic order wrong");
                    }
                }
            }
        }
    }
    // DECIMAL128 carries no validity helper here; its null arm is the same SortKeyCmp
    // branch as the string lane above. Check its value order is unaffected by nf.
    std::vector<MorselPtr> ms_d{make_morsel({col_dec128(dv)})};
    for (bool nf : {true, false}) {
        auto keys = keys_of(ms_d, {{0, true, nf}}, 5);
        auto got = sort_via_dispatch(keys, 5, SIZE_MAX);
        for (size_t i = 1; i < 5; ++i)
            CHECK(dv[got[i - 1]] <= dv[got[i]], "DECIMAL128 value order changed by nulls_first");
    }

    // Two keys, independent reference: c0 DESC NULLS FIRST, c1 ASC NULLS LAST.
    const size_t n = 3000;
    std::mt19937_64 rng(11);
    std::uniform_int_distribution<int64_t> d(-5, 5);
    std::uniform_int_distribution<int> p(1, 100);
    std::vector<int64_t> a(n), b(n);
    std::vector<bool> va(n, true), vb(n, true);
    for (size_t i = 0; i < n; ++i) {
        a[i] = d(rng); b[i] = d(rng);
        if (p(rng) <= 20) va[i] = false;
        if (p(rng) <= 20) vb[i] = false;
    }
    std::vector<MorselPtr> ms{make_morsel({col_i64(a, va), col_i64(b, vb)})};
    auto keys = keys_of(ms, {{0, false, true}, {1, true, false}}, n);
    std::vector<uint32_t> want(n);
    std::iota(want.begin(), want.end(), 0u);
    std::stable_sort(want.begin(), want.end(), [&](uint32_t x, uint32_t y) {
        if (va[x] != va[y]) return !va[x];                 // c0 NULLS FIRST
        if (va[x] && a[x] != a[y]) return a[x] > a[y];     // c0 DESC
        if (vb[x] != vb[y]) return vb[x] ? true : false;   // c1 NULLS LAST
        if (vb[x] && b[x] != b[y]) return b[x] < b[y];     // c1 ASC
        return false;
    });
    CHECK(sort_via_dispatch(keys, n, SIZE_MAX) == want, "AoS disagrees with explicit-nulls reference");
    CHECK(sort_via_generic(keys, n, SIZE_MAX) == want, "generic disagrees with explicit-nulls reference");
}

// 5+ key columns exceed SORT_AOS_MAX_PARTS and must fall back, still correctly.
static void test_five_columns_fall_back() {
    const size_t n = 800;
    std::mt19937_64 rng(3);
    std::uniform_int_distribution<int64_t> d(0, 3);
    std::vector<CxxColumn> cols;
    std::vector<SortKeySpec> spec;
    std::vector<std::vector<int64_t>> vals(5, std::vector<int64_t>(n));
    for (int k = 0; k < 5; ++k) {
        for (size_t i = 0; i < n; ++i) vals[k][i] = d(rng);
        cols.push_back(col_i64(vals[k], std::vector<bool>(n, true)));
        spec.push_back({static_cast<size_t>(k), true, true});
    }
    std::vector<MorselPtr> ms{make_morsel(std::move(cols))};
    auto keys = keys_of(ms, spec, n);
    CHECK(!aos_keys_eligible(keys), "5 columns must exceed SORT_AOS_MAX_PARTS");

    std::vector<uint32_t> perm(n);
    std::iota(perm.begin(), perm.end(), 0u);
    sort_perm(keys, perm, SIZE_MAX, kSortThreads);
    for (size_t i = 0; i + 1 < n; ++i) {
        uint32_t x = perm[i], y = perm[i + 1];
        bool ok = false;
        for (int k = 0; k < 5; ++k) {
            if (vals[k][x] != vals[k][y]) { ok = vals[k][x] < vals[k][y]; break; }
            if (k == 4) ok = true;   // fully equal
        }
        CHECK(ok, "5-column lexicographic order wrong");
    }
}

// take_first (TopN) must agree with the full sort's prefix.
static void test_take_first_prefix() {
    const size_t n = 2000;
    std::mt19937_64 rng(11);
    std::uniform_int_distribution<int64_t> d(0, 1000000);   // near-unique: no tie ambiguity
    std::vector<int64_t> v(n);
    for (size_t i = 0; i < n; ++i) v[i] = d(rng);
    std::vector<MorselPtr> ms{make_morsel({col_i64(v, std::vector<bool>(n, true))})};
    auto keys = keys_of(ms, {{0, true, true}}, n);

    auto full = sort_via_dispatch(keys, n, SIZE_MAX);
    for (size_t k : {size_t(1), size_t(10), size_t(500)}) {
        auto topn = sort_via_dispatch(keys, n, k);
        for (size_t i = 0; i < k; ++i)
            CHECK(v[topn[i]] == v[full[i]], "take_first prefix disagrees with full sort");
    }
}

// The multi-morsel entry point: rows must be ordered ACROSS morsel boundaries, and
// chunking must not change the answer.
static void test_sort_morsels_across_morsels() {
    std::vector<MorselPtr> ms;
    std::vector<int64_t> all;
    std::mt19937_64 rng(23);
    std::uniform_int_distribution<int64_t> d(0, 200);
    for (int m = 0; m < 5; ++m) {
        std::vector<int64_t> v(97);
        for (auto& x : v) { x = d(rng); all.push_back(x); }
        ms.push_back(make_morsel({col_i64(v, std::vector<bool>(v.size(), true))}));
    }
    std::sort(all.begin(), all.end());

    for (size_t chunk : {size_t(64), size_t(1000)}) {
        ErrCtx err;
        std::vector<MorselPtr> out;
        bool ok = sort_morsels(ms, {{0, true, true}}, SIZE_MAX, chunk, kSortThreads, out, err);
        CHECK(ok && err.code == 0, "sort_morsels failed");
        std::vector<int64_t> got;
        for (const MorselPtr& m : out) {
            const DrakenVector& v = m->columns[0].view;
            for (uint32_t i = 0; i < v.length; ++i)
                got.push_back(static_cast<const int64_t*>(v.data)[v.selection[i]]);
        }
        CHECK(got == all, "sort_morsels did not order rows across morsel boundaries");
    }

    // TopN across morsels
    ErrCtx err;
    std::vector<MorselPtr> out;
    bool ok = sort_morsels(ms, {{0, true, true}}, 10, 10, kSortThreads, out, err);
    CHECK(ok && err.code == 0, "sort_morsels TopN failed");
    size_t emitted = 0;
    for (const MorselPtr& m : out) emitted += m->num_rows();
    CHECK(emitted == 10, "TopN emitted the wrong row count");
    const DrakenVector& v0 = out.front()->columns[0].view;
    CHECK(static_cast<const int64_t*>(v0.data)[v0.selection[0]] == all.front(),
          "TopN first row is not the global minimum");

    // DRAKEN_ROW_SORTED implies the DEFAULT null placement (buffers.h). A sort with
    // the other placement must leave the key unstamped instead of mislabelling it.
    for (bool nf : {true, false}) {
        ErrCtx e2;
        std::vector<MorselPtr> o2;
        CHECK(sort_morsels(ms, {{0, true, nf}}, SIZE_MAX, 1000, kSortThreads, o2, e2),
              "sort_morsels failed");
        const bool stamped = (o2.front()->columns[0].view.flags & DRAKEN_ROW_SORTED) != 0;
        CHECK(stamped == nf, "ROW_SORTED stamped iff null placement is the default");
    }
}

// The AoS build gate: it decides SPEED, never the answer. Whichever way it goes, the
// resulting order must be identical — so the gate can be retuned freely without any
// risk to correctness.
static void test_aos_gate_does_not_change_the_answer() {
    // documented behaviour of the gate itself
    CHECK(aos_build_worth_it(1000, SIZE_MAX), "full sort must build AoS");
    CHECK(!aos_build_worth_it(5'000'000, 100), "small TopN must skip the AoS build");
    CHECK(aos_build_worth_it(5'000'000, 100'000), "large TopN must build AoS");
    CHECK(aos_build_worth_it(2000, 1500), "TopN over most of the input must build AoS");

    const size_t n = 5000;
    std::mt19937_64 rng(31);
    std::uniform_int_distribution<int64_t> d(0, 300);
    std::vector<int64_t> a(n), b(n);
    for (size_t i = 0; i < n; ++i) { a[i] = d(rng); b[i] = d(rng); }
    std::vector<MorselPtr> ms{make_morsel({col_i64(a, std::vector<bool>(n, true)),
                                           col_i64(b, std::vector<bool>(n, true))})};
    auto keys = keys_of(ms, {{0, true, true}, {1, false, false}}, n);
    CHECK(aos_keys_eligible(keys), "expected AoS-eligible keys");

    // Straddle the gate: k=10 skips the build, k=4000 takes it. Compare each against
    // the general comparator's prefix for the same k.
    for (size_t k : {size_t(10), size_t(4000)}) {
        auto viad = sort_via_dispatch(keys, n, k);
        auto gen = sort_via_generic(keys, n, k);
        for (size_t i = 0; i < k; ++i)
            CHECK(viad[i] == gen[i], "AoS gate changed the resulting order");
    }
}

// An unsupported key type must fail loudly, not silently mis-order.
static void test_unsupported_key_fails_loud() {
    CHECK(!sort_key_type_supported(DRAKEN_ARRAY), "ARRAY must not be a sortable key");
    CHECK(!sort_key_type_supported(DRAKEN_VARIANT), "VARIANT must not be a sortable key");
    CHECK(sort_key_type_supported(DRAKEN_INT64), "INT64 must be a sortable key");
    CHECK(sort_key_type_supported(DRAKEN_VARCHAR), "VARCHAR must be a sortable key");
}

// The radix full sort (radix_sort_perm, reached through sort_perm for AoS keys with
// n >= SORT_RADIX_MIN) must land on EXACTLY std::stable_sort's permutation under the
// general comparator — over both of its routes (packed key|index when the key bits fit
// beside the index, carried index otherwise), the folded and the separate NULL-rank
// pass, every direction x null-placement pair, and a non-identity starting perm (ties
// must keep THAT order, not row-id order). Each case also runs radix_sort_perm
// directly, so already-ordered input still exercises the radix rather than stopping
// at the census.
static std::vector<int64_t> radix_case_values(std::mt19937_64& rng, size_t n, int range) {
    std::vector<int64_t> v(n);
    for (size_t i = 0; i < n; ++i) {
        uint64_t r = rng();
        switch (range) {
            case 0: v[i] = static_cast<int64_t>(r % 7) - 3; break;             // ties
            case 1: v[i] = static_cast<int64_t>(r % 1000003); break;           // ~20 bits
            case 2: v[i] = static_cast<int64_t>(r >> 34); break;               // 30 bits
            case 3: v[i] = static_cast<int64_t>(r); break;                     // all 64
            case 4: v[i] = (r & 1) ? INT64_MIN + static_cast<int64_t>(r % 3)
                                   : INT64_MAX - static_cast<int64_t>(r % 3); break;
            default: v[i] = 42; break;                                         // constant
        }
    }
    return v;
}

template <int NP>
static std::vector<uint32_t> radix_direct(const std::vector<SortKeyColumn>& keys,
                                          const std::vector<uint32_t>& start) {
    std::vector<RowKeyN<NP>> rows;
    std::vector<uint8_t> masks;
    std::array<bool, NP> nf{};
    build_aos_keys<NP>(keys, start.size(), rows, masks, nf);
    std::vector<uint32_t> perm = start;
    radix_sort_perm<NP>(rows.data(), masks.data(), nf, perm, kSortThreads);
    return perm;
}

static std::vector<uint32_t> radix_direct_any(const std::vector<SortKeyColumn>& keys,
                                              const std::vector<uint32_t>& start) {
    switch (keys.size()) {
        case 1: return radix_direct<1>(keys, start);
        case 2: return radix_direct<2>(keys, start);
        case 3: return radix_direct<3>(keys, start);
        default: return radix_direct<4>(keys, start);
    }
}

static void test_radix_matches_stable_sort() {
    std::mt19937_64 rng(2026);
    int cases = 0;
    for (size_t n : {size_t(SORT_RADIX_MIN), size_t(5003), size_t(300000)}) {
        for (int nparts = 1; nparts <= 4; ++nparts) {
            for (int trial = 0; trial < 8; ++trial) {
                std::vector<CxxColumn> cols;
                std::vector<SortKeySpec> spec;
                for (int k = 0; k < nparts; ++k) {
                    const int range = static_cast<int>((rng() % 6));
                    const int null_pct = (trial % 4 == 0) ? 0 : (trial % 4 == 3 ? 100 : 20);
                    std::vector<int64_t> vals = radix_case_values(rng, n, range);
                    std::vector<bool> valid(n, true);
                    for (size_t i = 0; i < n; ++i)
                        if (null_pct && static_cast<int>(rng() % 100) < null_pct) valid[i] = false;
                    if (k == 1 && trial % 2 == 1) {
                        // a FLOAT column: NaN, +-0.0, +-inf, negatives
                        std::vector<double> f(n);
                        for (size_t i = 0; i < n; ++i) {
                            switch (rng() % 6) {
                                case 0: f[i] = std::nan(""); break;
                                case 1: f[i] = (rng() & 1) ? -0.0 : 0.0; break;
                                case 2: f[i] = (rng() & 1) ? -INFINITY : INFINITY; break;
                                default: f[i] = static_cast<double>(static_cast<int64_t>(rng() % 2001) - 1000) / 8.0;
                            }
                        }
                        cols.push_back(col_f64(f, valid));
                    } else {
                        cols.push_back(col_i64(vals, valid));
                    }
                    spec.push_back({static_cast<size_t>(k), (k + trial) % 2 == 0,
                                    (k + trial / 2) % 2 == 0});
                }
                std::vector<MorselPtr> ms{make_morsel(std::move(cols))};
                auto keys = keys_of(ms, spec, n);

                for (int order = 0; order < 3; ++order) {
                    // 0: identity start; 1: shuffled start; 2: start = the sorted order
                    std::vector<uint32_t> start(n);
                    std::iota(start.begin(), start.end(), 0u);
                    if (order == 1) std::shuffle(start.begin(), start.end(), rng);
                    if (order == 2) std::stable_sort(start.begin(), start.end(), SortKeyCmp{keys});
                    std::vector<uint32_t> ref = start;
                    std::stable_sort(ref.begin(), ref.end(), SortKeyCmp{keys});

                    std::vector<uint32_t> via = start;
                    sort_perm(keys, via, SIZE_MAX, kSortThreads);
                    CHECK(via == ref, "sort_perm (radix route) != std::stable_sort");
                    CHECK(radix_direct_any(keys, start) == ref,
                          "radix_sort_perm != std::stable_sort");
                    ++cases;
                }
            }
        }
    }
    std::printf("  radix vs stable_sort: %d cases\n", cases);
}

// The run census must agree with vergesort's own verdict on the shapes it decides:
// sorted, reversed, k sorted runs, and random — and the full sort still lands on the
// stable order in each (this is what exercises the census-skip and vergesort-merge
// routes of sort_perm_full on the comparator path, which the radix test bypasses).
static void test_run_census_routes() {
    std::mt19937_64 rng(77);
    const size_t n = 200000;
    for (int shape = 0; shape < 6; ++shape) {
        std::vector<int64_t> vals(n);
        for (size_t i = 0; i < n; ++i) {
            switch (shape) {
                case 0: vals[i] = static_cast<int64_t>(i / 3); break;               // sorted, ties
                case 1: vals[i] = static_cast<int64_t>(n - i); break;               // strictly reversed
                case 2: vals[i] = static_cast<int64_t>((i % (n / 8)) / 2); break;   // 8 sorted runs
                case 3: vals[i] = static_cast<int64_t>((i % (n / 40))); break;      // 40 runs
                case 4: vals[i] = static_cast<int64_t>((n - i) / 3); break;         // reversed, ties
                default: vals[i] = static_cast<int64_t>(rng() % 1000); break;       // random
            }
        }
        std::vector<CxxColumn> cols;
        cols.push_back(col_i64(vals, std::vector<bool>(n, true)));
        std::vector<SortKeySpec> spec{{0, true, true}};
        std::vector<MorselPtr> ms{make_morsel(std::move(cols))};
        auto keys = keys_of(ms, spec, n);
        std::vector<uint32_t> ref(n);
        std::iota(ref.begin(), ref.end(), 0u);
        std::stable_sort(ref.begin(), ref.end(), SortKeyCmp{keys});
        CHECK(sort_via_dispatch(keys, n, SIZE_MAX) == ref, "census route: AoS dispatch != stable_sort");
        CHECK(sort_via_generic(keys, n, SIZE_MAX) == ref, "census route: comparator path != stable_sort");

        std::vector<uint32_t> id(n);
        std::iota(id.begin(), id.end(), 0u);
        SortRunCensus c = sort_run_census(SortKeyCmp{keys}, id.data(), n, kSortThreads,
                                          SORT_VERGESORT_THRESHOLD);
        if (shape == 0) CHECK(c.descents == 0, "sorted input must census to zero descents");
        else CHECK(c.descents > 0, "unsorted input must census to some descent");
        if (shape <= 2) CHECK(!sort_census_declines(c, SORT_VERGESORT_THRESHOLD), "census declined a shape vergesort takes");
        // Soundness at every threshold either fallback uses: vergesort's own verdict
        // on a copy must be "decline" whenever the census says so.
        for (uint32_t th : {1u, 2u, 4u, SORT_VERGESORT_THRESHOLD}) {
            SortRunCensus ct = sort_run_census(SortKeyCmp{keys}, id.data(), n, kSortThreads, th);
            std::vector<uint32_t> p = id;
            std::unique_ptr<uint32_t[]> tmp(new uint32_t[n]);
            uint32_t runs[SORT_VERGESORT_THRESHOLD + 3];
            bool vg = vergesort_generic(p.data(), tmp.get(), SortKeyCmp{keys}, n, th, runs);
            if (sort_census_declines(ct, th)) CHECK(!vg, "census declined but vergesort would take it");
        }
    }
}

// The radix scratch is charged to the process memory account while it is live and
// fully released afterwards: peak rises by at least the two key buffers of the
// carried-index route (full-range int64 keys cannot pack), charged returns to where
// it started. Proves the TrackedUninitAllocator actually reaches the account.
static void test_radix_scratch_is_charged() {
    const size_t n = 300000;
    std::mt19937_64 rng(5);
    std::vector<int64_t> vals(n);
    for (size_t i = 0; i < n; ++i) vals[i] = static_cast<int64_t>(rng());
    std::vector<CxxColumn> cols;
    cols.push_back(col_i64(vals, std::vector<bool>(n, true)));
    std::vector<SortKeySpec> spec{{0, true, true}};
    std::vector<MorselPtr> ms{make_morsel(std::move(cols))};
    auto keys = keys_of(ms, spec, n);
    std::vector<uint32_t> start(n);
    std::iota(start.begin(), start.end(), 0u);

    const int64_t before = draken_mem_charged();
    draken_mem_reset_peak();
    std::vector<uint32_t> got = radix_direct<1>(keys, start);
    const int64_t peak = draken_mem_peak();
    CHECK(draken_mem_charged() == before, "radix scratch not fully uncharged");
    CHECK(peak - before >= static_cast<int64_t>(2 * n * sizeof(uint64_t)),
          "radix scratch not charged to the memory account");
    std::vector<uint32_t> ref = start;
    std::stable_sort(ref.begin(), ref.end(), SortKeyCmp{keys});
    CHECK(got == ref, "radix_sort_perm != std::stable_sort (charged run)");
}

// A wide radix team (16) takes the narrowest vergesort threshold (1): K=2 and K=4
// sorted runs go to the radix, a single reversed run stays with vergesort — and every
// route still lands on std::stable_sort's permutation.
static void test_wide_team_threshold_routes() {
    CHECK(sort_vergesort_radix_threshold(1) == 4 && sort_vergesort_radix_threshold(4) == 4 &&
          sort_vergesort_radix_threshold(6) == 2 && sort_vergesort_radix_threshold(8) == 2 &&
          sort_vergesort_radix_threshold(16) == 1, "radix vergesort threshold rule");
    const size_t n = 16 * SORT_TEAM_ROWS_PER_THREAD + 7;   // team width 16
    std::mt19937_64 rng(31);
    for (int shape = 0; shape < 4; ++shape) {
        std::vector<int64_t> vals(n);
        const size_t runs = shape == 0 ? 2 : (shape == 1 ? 4 : 1);
        for (size_t i = 0; i < n; ++i) vals[i] = static_cast<int64_t>(rng() % 50000);
        if (shape <= 1) {
            size_t chunk = (n + runs - 1) / runs;
            for (size_t s0 = 0; s0 < n; s0 += chunk)
                std::sort(vals.begin() + s0, vals.begin() + std::min(n, s0 + chunk));
        } else if (shape == 2) {
            for (size_t i = 0; i < n; ++i) vals[i] = static_cast<int64_t>(n - i);   // one reversed run
        }   // shape 3: random
        std::vector<bool> valid(n, true);
        for (size_t i = 0; i < n; i += 13) valid[i] = false;
        std::vector<CxxColumn> cols;
        cols.push_back(col_i64(vals, valid));
        std::vector<SortKeySpec> spec{{0, shape % 2 == 0, true}};
        std::vector<MorselPtr> ms{make_morsel(std::move(cols))};
        auto keys = keys_of(ms, spec, n);
        std::vector<uint32_t> ref(n);
        std::iota(ref.begin(), ref.end(), 0u);
        std::stable_sort(ref.begin(), ref.end(), SortKeyCmp{keys});
        std::vector<uint32_t> via(n);
        std::iota(via.begin(), via.end(), 0u);
        sort_perm(keys, via, SIZE_MAX, 16u);
        CHECK(via == ref, "wide-team sort_perm != std::stable_sort");
    }
}

// The census-rebuilt stage 1 (sort_census_runs + reverse + merge) must be EXACTLY
// vergesort_generic: same accept/decline verdict and, when accepted, the identical
// permutation — over ascending runs with ties, strictly descending runs, alternating
// asc/desc runs, uneven run lengths (transitions land on census range boundaries),
// and random input, at every threshold the engine uses.
static void test_census_runs_match_vergesort() {
    std::mt19937_64 rng(404);
    const size_t n = 300007;   // team width 4: probe + four census ranges
    int accepted = 0, declined = 0;
    for (int trial = 0; trial < 120; ++trial) {
        const int shape = trial % 6;
        const size_t K = 1 + static_cast<size_t>(rng() % 20);
        // run boundaries: equal (shapes 0-3) or random lengths (shapes 4-5)
        std::vector<size_t> cut{0};
        for (size_t r = 1; r < K; ++r)
            cut.push_back(shape >= 4 ? 1 + rng() % (n - 1) : r * n / K);
        cut.push_back(n);
        std::sort(cut.begin(), cut.end());
        cut.erase(std::unique(cut.begin(), cut.end()), cut.end());
        std::vector<int64_t> vals(n);
        for (size_t i = 0; i < n; ++i) vals[i] = static_cast<int64_t>(rng() % (shape == 0 ? 50 : 1000000));
        for (size_t r = 0; r + 1 < cut.size(); ++r) {
            auto b = vals.begin() + static_cast<ptrdiff_t>(cut[r]);
            auto e = vals.begin() + static_cast<ptrdiff_t>(cut[r + 1]);
            const bool desc = shape == 1 || (shape == 2 && r % 2 == 1) || (shape == 4 && (rng() & 1));
            if (shape == 5) continue;   // random: no runs imposed
            if (desc) {
                // strictly descending: distinct values (vergesort's desc runs are strict)
                int64_t v = static_cast<int64_t>(5000000 + rng() % 1000);
                for (auto it = b; it != e; ++it) *it = v--;
            } else {
                std::sort(b, e);
            }
        }
        std::vector<CxxColumn> cols;
        cols.push_back(col_i64(vals, std::vector<bool>(n, true)));
        std::vector<SortKeySpec> spec{{0, true, true}};
        std::vector<MorselPtr> ms{make_morsel(std::move(cols))};
        auto keys = keys_of(ms, spec, n);
        SortKeyCmp cmp{keys};
        for (uint32_t th : {1u, 2u, 4u, SORT_VERGESORT_THRESHOLD}) {
            std::vector<uint32_t> id(n);
            std::iota(id.begin(), id.end(), 0u);
            std::vector<uint32_t> vg = id;
            std::unique_ptr<uint32_t[]> tmp(new uint32_t[n]);
            uint32_t vruns[SORT_VERGESORT_THRESHOLD + 3];
            const bool vg_ok = vergesort_generic(vg.data(), tmp.get(), cmp, n, th, vruns);

            SortRunCensus c = sort_run_census(cmp, id.data(), n, 4u, th);
            bool cs_ok = false;
            std::vector<uint32_t> cs = id;
            if (c.descents == 0) {
                cs_ok = true;   // identity
            } else if (!sort_census_declines(c, th)) {
                uint32_t runs[SORT_VERGESORT_THRESHOLD + 1];
                bool rd[SORT_VERGESORT_THRESHOLD];
                uint32_t nr = 0;
                cs_ok = sort_census_runs(c, n, th, runs, rd, nr);
                if (cs_ok) {
                    for (uint32_t r = 0; r < nr; ++r)
                        if (rd[r]) std::reverse(cs.begin() + runs[r], cs.begin() + runs[r + 1]);
                    if (nr > 1) _vgs_merge_runs_cmp(cs.data(), tmp.get(), cmp, runs, nr,
                                                    static_cast<uint32_t>(n));
                }
            }
            CHECK(cs_ok == vg_ok, "census-rebuilt runs: accept/decline differs from vergesort");
            if (vg_ok) CHECK(cs == vg, "census-rebuilt runs: permutation differs from vergesort");
            (vg_ok ? accepted : declined)++;
        }
        // and the public route lands on the stable order whichever stage takes it
        std::vector<uint32_t> ref(n);
        std::iota(ref.begin(), ref.end(), 0u);
        std::stable_sort(ref.begin(), ref.end(), cmp);
        CHECK(sort_via_dispatch(keys, n, SIZE_MAX) == ref, "census route (AoS) != stable_sort");
        CHECK(sort_via_generic(keys, n, SIZE_MAX) == ref, "census route (generic) != stable_sort");
    }
    std::printf("  census runs vs vergesort: %d accepted, %d declined\n", accepted, declined);
}

// The parallel gather (sort_morsels at width 8) must emit EXACTLY what the serial one
// (width 1) does: the same chunk boundaries, and per chunk the same rows, values,
// validity and ROW_SORTED stamp — over fixed-width, float, inline and arena string,
// and bool columns with NULLs, across several source morsels, many small chunks, and
// an emit subset.
static std::string cell_repr(const CxxColumn& c, uint32_t i) {
    const DrakenVector& v = c.view;
    if (!sort_row_valid(v, i)) return "N";
    const uint32_t ph = v.selection[i];
    if (sort_type_is_string(v.type)) {
        const DrakenStringArena* sa = string_arena_of(v);
        const DrakenStringSlot* sl = &sa->slots[ph];
        return "s" + std::string(reinterpret_cast<const char*>(str_data(sl, sa->arena)), str_length(sl));
    }
    if (v.type == DRAKEN_BOOL)
        return ((static_cast<const uint8_t*>(v.data)[ph >> 3] >> (ph & 7)) & 1u) ? "t" : "f";
    const size_t es = draken_type_itemsize(v.type, c.own ? c.own->logical_type : nullptr);
    return "b" + std::string(static_cast<const char*>(v.data) + static_cast<size_t>(ph) * es, es);
}

static void test_parallel_gather_matches_serial() {
    std::mt19937_64 rng(808);
    std::vector<MorselPtr> ms;
    for (int m = 0; m < 5; ++m) {
        const size_t n = 30000 + static_cast<size_t>(rng() % 20000);
        std::vector<int64_t> k(n), pay(n);
        std::vector<double> f(n);
        std::vector<std::string> str(n);
        std::vector<bool> vk(n, true), vf(n, true), vs(n, true);
        for (size_t i = 0; i < n; ++i) {
            k[i] = static_cast<int64_t>(rng() % 5000);
            pay[i] = static_cast<int64_t>(rng());
            f[i] = static_cast<double>(static_cast<int64_t>(rng() % 20001) - 10000) / 3.0;
            const size_t len = rng() % 30;   // inline (<=12) and arena strings
            for (size_t j = 0; j < len; ++j) str[i].push_back(static_cast<char>('a' + rng() % 26));
            vk[i] = rng() % 17 != 0;
            vf[i] = rng() % 11 != 0;
            vs[i] = rng() % 13 != 0;
        }
        std::vector<CxxColumn> cols;
        cols.push_back(col_i64(k, vk));
        cols.push_back(col_f64(f, vf));
        cols.push_back(col_str(str, vs));
        cols.push_back(col_i64(pay, std::vector<bool>(n, true)));
        ms.push_back(make_morsel(std::move(cols)));
    }
    std::vector<SortKeySpec> spec{{0, true, true}, {1, false, false}};
    const std::vector<uint32_t> subset{2, 0};
    for (const std::vector<uint32_t>* emit : {static_cast<const std::vector<uint32_t>*>(nullptr), &subset}) {
        for (size_t chunk_rows : {size_t(777), size_t(65536)}) {
            std::vector<MorselPtr> serial, par;
            ErrCtx e1, e2;
            CHECK(sort_morsels(ms, spec, SIZE_MAX, chunk_rows, 1u, serial, e1, emit), "serial sort_morsels failed");
            CHECK(sort_morsels(ms, spec, SIZE_MAX, chunk_rows, 8u, par, e2, emit), "parallel sort_morsels failed");
            CHECK(serial.size() == par.size(), "parallel gather: chunk count differs");
            for (size_t c = 0; c < serial.size(); ++c) {
                CHECK(serial[c]->num_rows() == par[c]->num_rows(), "parallel gather: chunk size differs");
                CHECK(serial[c]->columns.size() == par[c]->columns.size(), "parallel gather: column count differs");
                for (size_t col = 0; col < serial[c]->columns.size(); ++col) {
                    const CxxColumn& a = serial[c]->columns[col];
                    const CxxColumn& b = par[c]->columns[col];
                    CHECK(a.view.flags == b.view.flags && a.own->vec.flags == b.own->vec.flags,
                          "parallel gather: flags (ROW_SORTED stamp) differ");
                    for (uint32_t i = 0; i < serial[c]->num_rows(); ++i)
                        CHECK(cell_repr(a, i) == cell_repr(b, i), "parallel gather: cell differs");
                }
            }
        }
    }
}

// String keys through the radix: prefix parts + radix_fixup_ties must land on EXACTLY
// std::stable_sort's permutation under SortKeyCmp — mixed lengths (inline and arena),
// shared prefixes (tie runs), embedded NULs ("ab" vs "ab\0": the zero-padded prefix
// alone cannot tell them apart), NULLs, both directions and null placements, the
// string as first / second / only key, two string keys, a shuffled start, and one run
// big enough for the parallel fix-up path.
static std::string rand_str(std::mt19937_64& rng, int style) {
    static const char* prefixes[] = {"", "https://www.", "id0000", "ab", "abcdefgh"};
    std::string s = prefixes[style % 5];
    const size_t extra = rng() % (style == 2 ? 4 : 24);
    for (size_t i = 0; i < extra; ++i) {
        const uint64_t r = rng() % 40;
        s.push_back(r == 0 ? '\0' : static_cast<char>('a' + r % 4));   // small alphabet + NULs
    }
    return s;
}

static void test_string_radix_matches_stable_sort() {
    std::mt19937_64 rng(1234);
    int cases = 0;
    for (size_t n : {size_t(SORT_RADIX_MIN), size_t(70001), size_t(400000)}) {
        for (int layout = 0; layout < 5; ++layout) {   // s | s,i | i,s | s,s | s,i,s
            for (int trial = 0; trial < 3; ++trial) {
                const bool big_group = n == 400000;
                std::vector<CxxColumn> cols;
                std::vector<SortKeySpec> spec;
                const int nkeys = layout == 0 ? 1 : (layout == 4 ? 3 : 2);
                for (int k = 0; k < nkeys; ++k) {
                    const bool is_s = layout == 0 || layout == 3 || (layout == 1 && k == 0) || (layout == 2 && k == 1) || (layout == 4 && k != 1);
                    std::vector<bool> valid(n, true);
                    for (size_t i = 0; i < n; ++i) valid[i] = rng() % 9 != 0;
                    if (is_s) {
                        std::vector<std::string> v(n);
                        for (size_t i = 0; i < n; ++i)
                            v[i] = rand_str(rng, big_group ? 1 : static_cast<int>(rng() % 5));
                        cols.push_back(col_str(v, valid));
                    } else {
                        std::vector<int64_t> v(n);
                        for (size_t i = 0; i < n; ++i) v[i] = static_cast<int64_t>(rng() % 7);
                        cols.push_back(col_i64(v, valid));
                    }
                    spec.push_back({static_cast<size_t>(k), (k + trial) % 2 == 0, (trial + k / 2) % 2 == 0});
                }
                std::vector<MorselPtr> ms{make_morsel(std::move(cols))};
                auto keys = keys_of(ms, spec, n);
                CHECK(keys_have_string(keys) && radix_keys_eligible(keys), "string radix route not taken");
                for (int order = 0; order < 2; ++order) {
                    std::vector<uint32_t> start(n);
                    std::iota(start.begin(), start.end(), 0u);
                    if (order == 1) std::shuffle(start.begin(), start.end(), rng);
                    std::vector<uint32_t> ref = start;
                    std::stable_sort(ref.begin(), ref.end(), SortKeyCmp{keys});
                    std::vector<uint32_t> via = start;
                    sort_perm(keys, via, SIZE_MAX, 8u);
                    CHECK(via == ref, "string radix sort_perm != std::stable_sort");
                    ++cases;
                }
            }
        }
    }
    std::printf("  string radix vs stable_sort: %d cases\n", cases);
}

int main() {
    test_aos_matches_generic();
    test_matches_independent_reference();
    test_null_placement();
    test_null_placement_explicit();
    test_float_sign_order();
    test_vergesort_hit_and_miss_agree();
    test_string_keys();
    test_decimal128_keys();
    test_five_columns_fall_back();
    test_take_first_prefix();
    test_sort_morsels_across_morsels();
    test_aos_gate_does_not_change_the_answer();
    test_unsupported_key_fails_loud();
    test_radix_matches_stable_sort();
    test_run_census_routes();
    test_radix_scratch_is_charged();
    test_wide_team_threshold_routes();
    test_census_runs_match_vergesort();
    test_parallel_gather_matches_serial();
    test_string_radix_matches_stable_sort();
    std::printf("test_sort_unified: all %d checks passed\n", g_checks);
    return 0;
}
