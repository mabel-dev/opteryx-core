// src/cpp/engine/test_sort_oracle.cpp — regression oracle for the sort unification.
//
// Includes BOTH the pre-unification engine sort (opteryx::engine::*, from
// native_sort.hpp) and the new unified sort (::*, from draken/morsels/sort.hpp) in one
// translation unit — they live in different namespaces, so both can be called side by
// side — and asserts they produce the IDENTICAL permutation on fixed-seed data.
//
// This is the check that the refactor changed no behavior. It is only meaningful
// while native_sort.hpp still carries its own copy of the algorithm; once Phase 2
// turns it into a re-export shim, both sides become the same code and this test
// becomes a tautology. Run it BEFORE that cutover.
//
// Note the two paths are NOT structurally identical: the old one goes straight to
// std::stable_sort, the new one runs a vergesort prepass first and may use the AoS
// comparator. Both are stable, so both must land on exactly the same permutation —
// that is the point.
//
// Build & run:
//   g++ -O2 -std=c++20 -I. -Idraken -Idraken/core -Isrc/cpp -Ithird_party/cyan4973 \
//       -pthread src/cpp/engine/test_sort_oracle.cpp draken/core/vector_alloc.cpp \
//       -o /tmp/test_sort_oracle && /tmp/test_sort_oracle

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include "engine/native_sort.hpp"   // OLD: opteryx::engine::*
#include "morsels/sort.hpp"         // NEW: ::*

static int g_cases = 0;
#define CHECK(cond, what)                                                          \
    do {                                                                           \
        if (!(cond)) {                                                             \
            std::fprintf(stderr, "FAIL [%s:%d] %s\n", __FILE__, __LINE__, (what)); \
            std::abort();                                                          \
        }                                                                          \
    } while (0)

static CxxColumn col_i64(const std::vector<int64_t>& vals, const std::vector<bool>& valid) {
    uint32_t n = static_cast<uint32_t>(vals.size());
    auto* data = static_cast<int64_t*>(draken_malloc(sizeof(int64_t) * (n ? n : 1)));
    uint8_t* vbits = nullptr;
    if (std::any_of(valid.begin(), valid.end(), [](bool v) { return !v; })) {
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

static CxxColumn col_f64(const std::vector<double>& vals) {
    uint32_t n = static_cast<uint32_t>(vals.size());
    auto* data = static_cast<double*>(draken_malloc(sizeof(double) * (n ? n : 1)));
    for (uint32_t i = 0; i < n; ++i) data[i] = vals[i];
    DrakenVector v = draken_vector_from_dense(data, n, DRAKEN_FLOAT64, nullptr);
    CxxColumn c;
    c.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(data), OwnedBuffer<uint8_t>(nullptr));
    c.view = c.own->vec;
    return c;
}

static CxxColumn col_str(const std::vector<std::string>& vals) {
    uint32_t n = static_cast<uint32_t>(vals.size());
    size_t total_arena = 0;
    for (const auto& s : vals) if (s.size() > STR_INLINE_MAX) total_arena += s.size();
    size_t slots_off = sizeof(DrakenStringArena);
    size_t arena_off = slots_off + static_cast<size_t>(n ? n : 1) * sizeof(DrakenStringSlot);
    uint8_t* blk = static_cast<uint8_t*>(draken_malloc(arena_off + total_arena));
    auto* sa = reinterpret_cast<DrakenStringArena*>(blk);
    auto* slots = reinterpret_cast<DrakenStringSlot*>(blk + slots_off);
    uint8_t* arena = total_arena ? blk + arena_off : nullptr;
    sa->slots = slots; sa->arena = arena; sa->length = n;
    sa->arena_used = total_arena; sa->arena_cap = total_arena;
    sa->null_bitmap = nullptr; sa->owns_buffers = 0; sa->type = DRAKEN_VARCHAR;
    size_t pos = 0;
    for (uint32_t i = 0; i < n; ++i) {
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
    v.validity = nullptr; v.type = DRAKEN_VARCHAR;
    v.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
    CxxColumn c;
    c.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(blk),
                                          OwnedBuffer<uint8_t>(nullptr), OwnedBuffer<void>(sel));
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

// Run the same logical ORDER BY through both implementations, assert identical output.
static void compare(const char* label, const std::vector<MorselPtr>& ms,
                    const std::vector<std::pair<size_t, bool>>& spec_pairs,
                    size_t n, size_t take_first) {
    ++g_cases;

    std::vector<opteryx::engine::SortKeySpec> old_spec;
    std::vector<::SortKeySpec> new_spec;
    for (auto& p : spec_pairs) {
        old_spec.push_back({p.first, p.second});
        new_spec.push_back({p.first, p.second});
    }

    ErrCtx e1;
    std::vector<opteryx::engine::SortKeyColumn> old_keys;
    CHECK(opteryx::engine::build_sort_keys(ms, old_spec, n, old_keys, e1),
          "old build_sort_keys failed");
    std::vector<uint32_t> old_perm(n);
    std::iota(old_perm.begin(), old_perm.end(), 0u);
    opteryx::engine::sort_perm(old_keys, old_perm, take_first);

    ErrCtx e2;
    std::vector<::SortKeyColumn> new_keys;
    CHECK(::build_sort_keys(ms, new_spec, n, new_keys, e2), "new build_sort_keys failed");
    std::vector<uint32_t> new_perm(n);
    std::iota(new_perm.begin(), new_perm.end(), 0u);
    ::sort_perm(new_keys, new_perm, take_first);

    size_t compare_len = std::min(take_first, n);
    for (size_t i = 0; i < compare_len; ++i) {
        if (old_perm[i] != new_perm[i]) {
            std::fprintf(stderr,
                         "FAIL [%s] permutation diverges at %zu: old=%u new=%u "
                         "(n=%zu, take_first=%zu, keys=%zu, aos_eligible=%d)\n",
                         label, i, old_perm[i], new_perm[i], n, take_first,
                         new_keys.size(), (int)aos_keys_eligible(new_keys));
            std::abort();
        }
    }
}

int main() {
    std::mt19937_64 rng(20260728);

    // 1-4 numeric columns (the AoS-eligible shapes), varying tie density, null rate,
    // direction mix, and sortedness — each must match the old implementation exactly.
    for (int nparts = 1; nparts <= 4; ++nparts) {
        for (int card : {3, 50, 100000}) {           // tie density: heavy .. near-unique
            for (int null_pct : {0, 25}) {
                for (int presorted = 0; presorted < 3; ++presorted) {
                    const size_t n = 3000;
                    std::vector<CxxColumn> cols;
                    std::vector<std::pair<size_t, bool>> spec;
                    for (int k = 0; k < nparts; ++k) {
                        std::vector<int64_t> v(n);
                        std::vector<bool> valid(n, true);
                        std::uniform_int_distribution<int64_t> d(0, card);
                        std::uniform_int_distribution<int> p(1, 100);
                        for (size_t i = 0; i < n; ++i) {
                            v[i] = d(rng);
                            if (null_pct && p(rng) <= null_pct) valid[i] = false;
                        }
                        if (presorted == 1) std::sort(v.begin(), v.end());
                        if (presorted == 2) std::sort(v.begin(), v.end(), std::greater<>());
                        cols.push_back(col_i64(v, valid));
                        spec.push_back({static_cast<size_t>(k), (k % 2) == 0});
                    }
                    std::vector<MorselPtr> ms{make_morsel(std::move(cols))};
                    compare("numeric-full", ms, spec, n, SIZE_MAX);
                    compare("numeric-topn", ms, spec, n, 25);
                }
            }
        }
    }

    // Floats, including the sign-boundary values the retired key path mis-ordered.
    {
        const size_t n = 2000;
        std::vector<double> v(n);
        std::uniform_real_distribution<double> d(-1e6, 1e6);
        for (size_t i = 0; i < n; ++i) v[i] = d(rng);
        v[0] = -0.0; v[1] = 0.0; v[2] = -1e-300; v[3] = 1e-300;
        v[4] = -1e300; v[5] = 1e300;
        std::vector<MorselPtr> ms{make_morsel({col_f64(v)})};
        compare("float-asc", ms, {{0, true}}, n, SIZE_MAX);
        compare("float-desc", ms, {{0, false}}, n, SIZE_MAX);
    }

    // Strings — NOT AoS-eligible, so this exercises the SortKeyCmp route on both sides,
    // including the inline/arena split.
    {
        const size_t n = 1500;
        std::vector<std::string> v(n);
        std::uniform_int_distribution<int> len(0, 30);
        std::uniform_int_distribution<int> ch('a', 'e');   // small alphabet -> shared prefixes
        for (size_t i = 0; i < n; ++i) {
            int L = len(rng);
            std::string s;
            for (int j = 0; j < L; ++j) s.push_back(static_cast<char>(ch(rng)));
            v[i] = std::move(s);
        }
        std::vector<MorselPtr> ms{make_morsel({col_str(v)})};
        compare("string-asc", ms, {{0, true}}, n, SIZE_MAX);
        compare("string-desc", ms, {{0, false}}, n, SIZE_MAX);
    }

    // Mixed string + numeric (the shape most real multi-column ORDER BYs have —
    // e.g. TPC-H q21 `numwait DESC, s_name`), which also routes to SortKeyCmp.
    {
        const size_t n = 1500;
        std::vector<std::string> s(n);
        std::vector<int64_t> k(n);
        std::uniform_int_distribution<int> ch('a', 'c');
        std::uniform_int_distribution<int64_t> d(0, 10);
        for (size_t i = 0; i < n; ++i) {
            s[i] = std::string(1 + (i % 3), static_cast<char>(ch(rng)));
            k[i] = d(rng);
        }
        std::vector<MorselPtr> ms{
            make_morsel({col_i64(k, std::vector<bool>(n, true)), col_str(s)})};
        compare("num+str", ms, {{0, false}, {1, true}}, n, SIZE_MAX);
        compare("str+num", ms, {{1, true}, {0, false}}, n, SIZE_MAX);
    }

    // Multiple morsels — the flattened global row space.
    {
        std::vector<MorselPtr> ms;
        size_t n = 0;
        for (int m = 0; m < 6; ++m) {
            std::vector<int64_t> v(211);
            std::uniform_int_distribution<int64_t> d(0, 40);
            for (auto& x : v) x = d(rng);
            n += v.size();
            ms.push_back(make_morsel({col_i64(v, std::vector<bool>(v.size(), true))}));
        }
        compare("multi-morsel", ms, {{0, true}}, n, SIZE_MAX);
        compare("multi-morsel-topn", ms, {{0, true}}, n, 17);
    }

    std::printf("test_sort_oracle: %d cases — new sort matches pre-unification "
                "native_sort.hpp exactly\n", g_cases);
    return 0;
}
