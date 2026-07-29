// src/cpp/engine/test_sort_unified.cpp — correctness proof for draken/morsels/sort.hpp.
//
// Standalone assert()-based test (same pattern as test_slice1.cpp — this repo has no
// C++ test framework). Not wired into CI; run it by hand when touching the sort.
//
// Build & run:
//   g++ -O2 -std=c++20 -I. -Idraken -Idraken/core -Isrc/cpp -Ithird_party/cyan4973 \
//       -pthread src/cpp/engine/test_sort_unified.cpp draken/core/vector_alloc.cpp \
//       -o /tmp/test_sort_unified && /tmp/test_sort_unified
//
// What it proves:
//   1. The AoS fast path and the general SortKeyCmp path produce the IDENTICAL
//      permutation for every eligible key shape — the fast path is a faster route to
//      the same answer, never a different one. This is the load-bearing check.
//   2. The order matches an independently-computed reference (std::stable_sort over
//      the same rows with a hand-written tuple comparator), so "both agree" cannot
//      mean "both wrong the same way".
//   3. NULLS FIRST under ASC, NULLS LAST under DESC.
//   4. Float sign order: negatives below positives (the bug that made the retired
//      compress()-based key path sort -2.5 above 1.0).
//   5. Both vergesort outcomes: already-sorted input (prepass hit) and shuffled input
//      (prepass declines, stage-2 sort runs) give the same answer.
//   6. Strings, DECIMAL128, and 5+ key columns route through SortKeyCmp correctly.
//   7. take_first (TopN/partial_sort) agrees with the full sort's prefix.

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <numeric>
#include <random>
#include <string>
#include <vector>

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
static std::vector<uint32_t> sort_via_dispatch(const std::vector<SortKeyColumn>& keys, size_t n,
                                               size_t take_first) {
    std::vector<uint32_t> perm(n);
    std::iota(perm.begin(), perm.end(), 0u);
    sort_perm(keys, perm, take_first);
    return perm;
}

static std::vector<uint32_t> sort_via_generic(const std::vector<SortKeyColumn>& keys, size_t n,
                                              size_t take_first) {
    std::vector<uint32_t> perm(n);
    std::iota(perm.begin(), perm.end(), 0u);
    sort_perm_cmp(SortKeyCmp{keys}, perm, take_first);
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
                    spec.push_back({static_cast<size_t>(k), (k + trial) % 2 == 0});
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
    std::vector<SortKeySpec> spec{{0, true}, {1, false}};   // c0 ASC, c1 DESC
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

    auto asc_keys = keys_of(ms, {{0, true}}, 5);
    auto asc = sort_via_dispatch(asc_keys, 5, SIZE_MAX);
    CHECK(!valid[asc[0]] && !valid[asc[1]], "ASC must place NULLs first");
    CHECK(v[asc[2]] == 1 && v[asc[3]] == 2 && v[asc[4]] == 3, "ASC value order wrong");

    auto desc_keys = keys_of(ms, {{0, false}}, 5);
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

    auto keys = keys_of(ms, {{0, true}}, n);
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
        auto keys = keys_of(ms, {{0, true}}, n);
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
        auto keys = keys_of(ms, {{0, true}}, n);
        auto got = sort_via_dispatch(keys, n, SIZE_MAX);
        for (size_t i = 0; i + 1 < n; ++i)
            CHECK(desc[got[i]] <= desc[got[i + 1]], "reversed-run output not ascending");
    }
    // shuffled -> many runs -> prepass declines, stage 2 sorts
    {
        std::vector<int64_t> shuf = base;
        std::shuffle(shuf.begin(), shuf.end(), std::mt19937_64(5));
        std::vector<MorselPtr> ms{make_morsel({col_i64(shuf, valid)})};
        auto keys = keys_of(ms, {{0, true}}, n);
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
    auto keys = keys_of(ms, {{0, true}}, n);
    CHECK(!aos_keys_eligible(keys), "string keys must NOT be AoS-eligible");

    std::vector<uint32_t> perm(n);
    std::iota(perm.begin(), perm.end(), 0u);
    sort_perm(keys, perm, SIZE_MAX);

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
    auto keys = keys_of(ms, {{0, true}}, n);
    CHECK(!aos_keys_eligible(keys), "DECIMAL128 keys must NOT be AoS-eligible");

    std::vector<uint32_t> perm(n);
    std::iota(perm.begin(), perm.end(), 0u);
    sort_perm(keys, perm, SIZE_MAX);
    for (size_t i = 0; i + 1 < n; ++i)
        CHECK(v[perm[i]] <= v[perm[i + 1]], "DECIMAL128 not ascending");
    CHECK(v[perm[0]] == -big, "most-negative int128 must sort first");
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
        spec.push_back({static_cast<size_t>(k), true});
    }
    std::vector<MorselPtr> ms{make_morsel(std::move(cols))};
    auto keys = keys_of(ms, spec, n);
    CHECK(!aos_keys_eligible(keys), "5 columns must exceed SORT_AOS_MAX_PARTS");

    std::vector<uint32_t> perm(n);
    std::iota(perm.begin(), perm.end(), 0u);
    sort_perm(keys, perm, SIZE_MAX);
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
    auto keys = keys_of(ms, {{0, true}}, n);

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
        bool ok = sort_morsels(ms, {{0, true}}, SIZE_MAX, chunk, out, err);
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
    bool ok = sort_morsels(ms, {{0, true}}, 10, 10, out, err);
    CHECK(ok && err.code == 0, "sort_morsels TopN failed");
    size_t emitted = 0;
    for (const MorselPtr& m : out) emitted += m->num_rows();
    CHECK(emitted == 10, "TopN emitted the wrong row count");
    const DrakenVector& v0 = out.front()->columns[0].view;
    CHECK(static_cast<const int64_t*>(v0.data)[v0.selection[0]] == all.front(),
          "TopN first row is not the global minimum");
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
    auto keys = keys_of(ms, {{0, true}, {1, false}}, n);
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

int main() {
    test_aos_matches_generic();
    test_matches_independent_reference();
    test_null_placement();
    test_float_sign_order();
    test_vergesort_hit_and_miss_agree();
    test_string_keys();
    test_decimal128_keys();
    test_five_columns_fall_back();
    test_take_first_prefix();
    test_sort_morsels_across_morsels();
    test_aos_gate_does_not_change_the_answer();
    test_unsupported_key_fails_loud();
    std::printf("test_sort_unified: all %d checks passed\n", g_checks);
    return 0;
}
