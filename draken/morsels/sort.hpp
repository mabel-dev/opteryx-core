#pragma once
// draken/morsels/sort.hpp — THE sort. One implementation, pure C++.
//
// Used by rugo (standalone, no query engine) and by opteryx's ORDER BY / Heap Sort /
// Window operators (src/cpp/engine/native_sort.hpp re-exports these names and wraps
// them in its Sink classes). Nothing here depends on the engine (no Sink, no
// MorselBuffer) or on Python — only draken's own vector/morsel types.
//
// TWO STAGES:
//   1. vergesort run-detection prepass — exploits pre-existing order (time-series,
//      already-clustered data) in O(n) instead of O(n log n).
//   2. if vergesort declines (too many runs), a parallel stable comparison sort.
//   The LIMIT/TopN case skips both and uses std::partial_sort — O(n log k) is already
//   cheaper than either stage when k is small.
//
// KEYS: each ORDER BY column is normalized to an order-preserving scalar
// (sort_num_key), so the comparator never re-interprets raw types. Where every key
// column normalizes to a single uint64 and there are at most SORT_AOS_MAX_PARTS of
// them, the keys are packed ROW-CONTIGUOUS (RowKeyN) and compared with a per-part
// short-circuit (AoSKeyCmpN) — measured 1.03-1.36x over the per-column SortKeyCmp,
// because a tie on part k finds part k+1 in the same cache line instead of in a
// separate array. Everything else (string keys, DECIMAL128, 5+ columns) uses
// SortKeyCmp, which is the same ordering by a slower route — never a different answer.
//
// Ordering contract (unchanged from the pre-unification engine sort):
//   - NULLS FIRST under ASC (null key < every value); DESC flips → NULLS LAST.
//   - Floats: IEEE total order -inf .. -0.0==+0.0 .. +inf, NaN sorts HIGHEST
//     (draken rule; -0.0 canonicalized to +0.0).
//   - Strings (VARCHAR/NVARCHAR/VARBINARY): unsigned byte-wise comparison
//     (== codepoint order for UTF-8), shorter prefix first.
//   - Multi-key: lexicographic, most significant first; the sort is stable.
//
// The row gather here is the general "take these rows, in this order, from this list
// of morsels" utility — also used by joins, UNNEST, and LIMIT's partial slice.

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <thread>
#include <vector>

#include "core/string_slot.h"    // DrakenStringSlot, str_length, str_data
#include "core/vector_owner.h"   // VectorOwner, OwnedBuffer
#include "core/vector_alloc.h"   // draken_zero_sel — the DRAKEN_NULL gather arm
#include "core/vergesort.h"      // vergesort_generic — stage 1
#include "logical_type.h"        // LogicalType + draken_type_itemsize
#include "morsels/cxx_morsel.h"  // CxxMorsel, CxxColumn, ErrCtx

using MorselPtr = std::shared_ptr<CxxMorsel>;

struct SortKeySpec {
    size_t col_idx;
    bool ascending;
};

// CANONICAL string layout (buffers.h / draken's own kernels, e.g.
// string_predicates.h): a string DrakenVector's `data` points at a
// DrakenStringArena STRUCT — slots and arena live inside it. NOT a raw
// DrakenStringSlot array with the arena on the owner; that convention exists in
// the GROUP BY key store, but mismatches everything the live scan actually
// produces — flagged, not copied. Read and WRITE the canonical form here.
inline const DrakenStringArena* string_arena_of(const DrakenVector& v) {
    return static_cast<const DrakenStringArena*>(v.data);
}

inline bool sort_row_valid(const DrakenVector& v, uint32_t row) {
    return v.validity == nullptr || ((v.validity[row >> 3] >> (row & 7)) & 1u);
}

// ORDER BY / sort-KEY string-ness: the types with a defined byte collation.
// NOT the storage predicate — VARIANT is stored identically (German string) but
// has no defined ordering, so it must never reach the key comparator. Value-moving
// paths (gather, join payload) use draken_type_is_string_storage instead; keeping
// the two apart is what lets VARIANT be carried without becoming sortable.
inline bool sort_type_is_string(DrakenType t) {
    return t == DRAKEN_VARCHAR || t == DRAKEN_NVARCHAR || t == DRAKEN_VARBINARY;
}

inline bool sort_key_type_supported(DrakenType t) {
    switch (t) {
        case DRAKEN_INT8: case DRAKEN_INT16: case DRAKEN_INT32: case DRAKEN_INT64:
        case DRAKEN_DECIMAL: case DRAKEN_DATE32: case DRAKEN_TIMESTAMP64:
        case DRAKEN_TIME32: case DRAKEN_TIME64: case DRAKEN_BOOL:
        case DRAKEN_FLOAT32: case DRAKEN_FLOAT64:
        case DRAKEN_VARCHAR: case DRAKEN_NVARCHAR: case DRAKEN_VARBINARY:
        case DRAKEN_DECIMAL128:   // int128 lane in SortKeyColumn
        case DRAKEN_UINT8: case DRAKEN_UINT16: case DRAKEN_UINT32: case DRAKEN_UINT64:  // E33
            return true;
        default:
            return false;   // ARRAY/INTERVAL/VARIANT keys: fail loud
    }
}

// Order-preserving uint64 key for a non-null fixed-width value (validity is the
// caller's dimension). Integers/temporals/bool: sign-flip into unsigned order.
// Floats: negatives -> ~bits, positives -> bits|SIGN — the CORRECT total order.
//
// NOTE the float arms below: the sign bit is PRESERVED (set on positives, inverted
// along with everything else on negatives), never masked off. Masking it away is
// exactly the bug that made the retired Vector.compress()-based key path sort small
// negatives above large positives; see tests/compiled/morsel_ops/test_sort.py's
// float sign-order regression case.
inline uint64_t sort_num_key(const DrakenVector& v, uint32_t row) {
    constexpr uint64_t SIGN = 0x8000000000000000ULL;
    uint32_t phys = v.selection[row];
    int64_t sv = 0;
    switch (v.type) {
        case DRAKEN_INT8:   sv = static_cast<const int8_t*>(v.data)[phys]; break;
        case DRAKEN_INT16:  sv = static_cast<const int16_t*>(v.data)[phys]; break;
        case DRAKEN_INT32:
        case DRAKEN_DATE32:
        case DRAKEN_TIME32: sv = static_cast<const int32_t*>(v.data)[phys]; break;
        case DRAKEN_INT64:
        case DRAKEN_DECIMAL:
        case DRAKEN_TIMESTAMP64:
        case DRAKEN_TIME64: sv = static_cast<const int64_t*>(v.data)[phys]; break;
        case DRAKEN_BOOL:
            sv = (static_cast<const uint8_t*>(v.data)[phys >> 3] >> (phys & 7)) & 1u;
            break;
        // E33 — genuinely unsigned: already naturally ordered when compared as
        // uint64_t directly, so no sign-flip (unlike the signed cases above,
        // which need `^ SIGN` to make unsigned-comparison equal signed-order).
        // Return here rather than falling through to the `sv ^ SIGN` tail.
        case DRAKEN_UINT8:  return static_cast<uint64_t>(static_cast<const uint8_t* >(v.data)[phys]);
        case DRAKEN_UINT16: return static_cast<uint64_t>(static_cast<const uint16_t*>(v.data)[phys]);
        case DRAKEN_UINT32: return static_cast<uint64_t>(static_cast<const uint32_t*>(v.data)[phys]);
        case DRAKEN_UINT64: return static_cast<const uint64_t*>(v.data)[phys];
        case DRAKEN_FLOAT32:
        case DRAKEN_FLOAT64: {
            double d = (v.type == DRAKEN_FLOAT32)
                ? static_cast<double>(static_cast<const float*>(v.data)[phys])
                : static_cast<const double*>(v.data)[phys];
            if (d != d) return UINT64_MAX;   // NaN sorts highest (draken rule)
            if (d == 0.0) d = 0.0;           // canonicalize -0.0
            uint64_t bits;
            std::memcpy(&bits, &d, sizeof(bits));
            return (bits & SIGN) ? ~bits : (bits | SIGN);
        }
        default: return 0;   // unreachable — sort_key_type_supported checked first
    }
    return static_cast<uint64_t>(sv) ^ SIGN;
}

// ---- normalized key columns over a flattened morsel list -------------------------

struct SortKeyColumn {
    bool asc = true;
    bool is_str = false;
    bool is_i128 = false;
    std::vector<uint8_t> valid;
    std::vector<uint64_t> num;              // fixed-width path
    std::vector<__int128> num128;           // DECIMAL128 path (raw ordering == value
                                            //  ordering at one scale)
    std::vector<const uint8_t*> sptr;       // string path (points into source buffers,
    std::vector<uint32_t> slen;             //  which the caller keeps alive)
};

inline bool build_sort_keys(const std::vector<MorselPtr>& ms,
                            const std::vector<SortKeySpec>& spec,
                            size_t n, std::vector<SortKeyColumn>& out, ErrCtx& err) {
    out.clear();
    out.resize(spec.size());
    for (size_t k = 0; k < spec.size(); ++k) {
        SortKeyColumn& col = out[k];
        col.asc = spec[k].ascending;
        col.valid.reserve(n);
        bool typed = false;
        for (const MorselPtr& m : ms) {
            if (m->num_rows() == 0) continue;
            if (spec[k].col_idx >= m->columns.size()) {
                err.code = 1;
                err.msg = "SortSink: key column index out of range";
                return false;
            }
            const CxxColumn& c = m->columns[spec[k].col_idx];
            const DrakenVector& v = c.view;
            if (!typed) {
                if (!sort_key_type_supported(v.type)) {
                    err.code = 1;
                    err.msg = "SortSink: unsupported ORDER BY key column type — fail "
                              "loud, never a silent wrong order";
                    return false;
                }
                col.is_str = sort_type_is_string(v.type);
                col.is_i128 = (v.type == DRAKEN_DECIMAL128);
                if (col.is_str) { col.sptr.reserve(n); col.slen.reserve(n); }
                else if (col.is_i128) { col.num128.reserve(n); }
                else { col.num.reserve(n); }
                typed = true;
            }
            const DrakenStringArena* sa = col.is_str ? string_arena_of(v) : nullptr;
            for (uint32_t r = 0; r < v.length; ++r) {
                bool ok = sort_row_valid(v, r);
                col.valid.push_back(ok ? 1 : 0);
                if (col.is_str) {
                    if (ok) {
                        const DrakenStringSlot* slot = &sa->slots[v.selection[r]];
                        col.slen.push_back(str_length(slot));
                        col.sptr.push_back(
                            reinterpret_cast<const uint8_t*>(str_data(slot, sa->arena)));
                    } else {
                        col.slen.push_back(0);
                        col.sptr.push_back(nullptr);
                    }
                } else if (col.is_i128) {
                    __int128 kv = 0;
                    if (ok) {
                        std::memcpy(&kv, static_cast<const uint8_t*>(v.data)
                                            + static_cast<size_t>(v.selection[r]) * 16u,
                                    16u);
                    }
                    col.num128.push_back(kv);
                } else {
                    col.num.push_back(ok ? sort_num_key(v, r) : 0);
                }
            }
        }
    }
    return true;
}

// Multi-key row comparator over normalized SortKeyColumns — the GENERAL path: any
// number of key columns, any supported key type. The AoS comparator below is a
// faster route to the SAME ordering for the subset of cases it accepts; this one is
// the definition both must agree with.
struct SortKeyCmp {
    const std::vector<SortKeyColumn>& keys;
    bool operator()(uint32_t a, uint32_t b) const {
        for (const SortKeyColumn& c : keys) {
            int cmp;
            uint8_t va = c.valid[a], vb = c.valid[b];
            if (!va || !vb) {
                cmp = (va == vb) ? 0 : (va ? 1 : -1);   // NULL below values (asc)
            } else if (c.is_str) {
                uint32_t la = c.slen[a], lb = c.slen[b];
                uint32_t common = la < lb ? la : lb;
                int r = common ? std::memcmp(c.sptr[a], c.sptr[b], common) : 0;
                cmp = r != 0 ? r : (la < lb ? -1 : (la > lb ? 1 : 0));
            } else if (c.is_i128) {
                cmp = c.num128[a] < c.num128[b] ? -1
                    : (c.num128[a] > c.num128[b] ? 1 : 0);
            } else {
                cmp = c.num[a] < c.num[b] ? -1 : (c.num[a] > c.num[b] ? 1 : 0);
            }
            if (cmp != 0) return c.asc ? (cmp < 0) : (cmp > 0);
        }
        return false;   // equal — stability preserves arrival order
    }
};

// ---- AoS (row-contiguous) key layout — the fast comparator ------------------------

// Widest ORDER BY worth specializing. Surveyed across ClickBench/TPC-H/H2O/JOB the
// widest real clause is 4 columns (TPC-H q02/q16); 80% are single-column. Beyond
// this, SortKeyCmp handles it — same order, one indirection more per compare.
inline constexpr size_t SORT_AOS_MAX_PARTS = 4;

// Validity lives in a SEPARATE compact side array, NOT in RowKeyN. A leading
// uint8_t next to uint64_t parts[] pads the struct up to the next 8-byte multiple
// (measured: RowKeyN<3> would be 32B instead of 24B, RowKeyN<4> 40B instead of 32B
// — 25-33% more memory traffic on EVERY comparison, to carry a byte only the rare
// null path reads). Keeping `parts` unpadded is the whole point of the layout; the
// masks pack 64-rows-per-cache-line in their own array.
template <int NPARTS>
struct RowKeyN {
    uint64_t parts[NPARTS];   // direction (asc/desc) pre-baked via bit-flip
};
static_assert(sizeof(RowKeyN<1>) == 8,  "RowKeyN<1> must be unpadded");
static_assert(sizeof(RowKeyN<2>) == 16, "RowKeyN<2> must be unpadded");
static_assert(sizeof(RowKeyN<3>) == 24, "RowKeyN<3> must be unpadded");
static_assert(sizeof(RowKeyN<4>) == 32, "RowKeyN<4> must be unpadded");

// Mirrors SortKeyCmp's null semantics exactly. SortKeyCmp does:
//     cmp = (va == vb) ? 0 : (va ? 1 : -1);            // invalid sorts "less" (raw)
//     if (cmp != 0) return asc ? (cmp < 0) : (cmp > 0);  // desc flips it
// With a_first := !va, `asc ? (cmp<0) : (cmp>0)` evaluates to `asc ? a_first
// : !a_first` for both cmp=-1 (a invalid) and cmp=+1 (b invalid) — which is what
// this returns.
//
// The hot (no-null) path is a plain `parts[k] < parts[k]` with NO direction branch
// (direction is baked into the value at construction) and NO type branch (the key
// is already normalized). The null check is only taken when a null is actually in
// the compared pair.
template <int NPARTS>
struct AoSKeyCmpN {
    const RowKeyN<NPARTS>* rows;
    const uint8_t* valid_masks;
    std::array<bool, NPARTS> asc;
    bool operator()(uint32_t a, uint32_t b) const {
        const RowKeyN<NPARTS>& ra = rows[a];
        const RowKeyN<NPARTS>& rb = rows[b];
        uint8_t ma = valid_masks[a], mb = valid_masks[b];
        for (int k = 0; k < NPARTS; ++k) {
            bool va = (ma >> k) & 1u;
            bool vb = (mb >> k) & 1u;
            if (!va || !vb) {
                if (va == vb) continue;              // both null here -> next part
                bool a_first = !va;
                return asc[k] ? a_first : !a_first;
            }
            if (ra.parts[k] != rb.parts[k]) return ra.parts[k] < rb.parts[k];
        }
        return false;   // equal — stability preserves arrival order
    }
};

// The AoS key packs one uint64 per column, so it can only represent columns whose
// normalized key IS one uint64: no strings (pointer+length), no DECIMAL128 (128
// bits). Those, and anything wider than SORT_AOS_MAX_PARTS, take SortKeyCmp.
inline bool aos_keys_eligible(const std::vector<SortKeyColumn>& keys) {
    if (keys.empty() || keys.size() > SORT_AOS_MAX_PARTS) return false;
    for (const SortKeyColumn& c : keys) {
        if (c.is_str || c.is_i128) return false;
    }
    return true;
}

// Smallest TopN limit at which building the AoS keys pays for itself.
//
// MEASURED 2026-07-28, dev/sort_key_bench/bench_unified_sort.cpp (5M rows, 2 key
// columns), AoS vs the per-column comparator on the std::partial_sort path:
//     LIMIT 10 / 100 / 1k -> 0.68x / 0.67x / 0.73x   (AoS LOSES)
//     LIMIT 10k           -> 1.07x                    (break-even)
//     LIMIT 50k / 100k    -> 1.40x / 1.84x            (AoS wins)
// A small-k partial_sort compares each row about once, against a heap top that stays
// hot in cache — there is almost nothing for a cheaper comparator to save, so the
// O(n) key build is pure added cost. A full sort does O(n log n) comparisons and
// amortises the same build many times over. 16384 sits clear of the break-even so
// the fast path is only taken where it is an unambiguous win.
inline constexpr size_t SORT_AOS_TOPN_MIN = 16384;

// Build the AoS keys only when the sort will do enough comparisons to earn the O(n)
// build. The `take_first * 2 >= n` arm catches a TopN that is really most of the
// input (small n, large limit), where the absolute threshold alone would wrongly
// send a near-full sort down the slow path.
inline bool aos_build_worth_it(size_t n, size_t take_first) {
    if (take_first >= n) return true;                     // full sort
    return take_first >= SORT_AOS_TOPN_MIN || take_first * 2 >= n;
}

template <int NPARTS>
inline void build_aos_keys(const std::vector<SortKeyColumn>& keys, size_t n,
                           std::vector<RowKeyN<NPARTS>>& rows_out,
                           std::vector<uint8_t>& masks_out,
                           std::array<bool, NPARTS>& asc_out) {
    rows_out.resize(n);
    masks_out.resize(n);
    for (int k = 0; k < NPARTS; ++k) asc_out[k] = keys[k].asc;
    for (size_t i = 0; i < n; ++i) {
        uint8_t mask = 0;
        for (int k = 0; k < NPARTS; ++k) {
            const SortKeyColumn& c = keys[k];
            if (c.valid[i]) mask |= static_cast<uint8_t>(1u << k);
            uint64_t v = c.num[i];
            // DESC is a bit-flip of the normalized key, so the comparator never
            // branches on direction. Value is irrelevant when !valid — the mask
            // check short-circuits before it is read.
            rows_out[i].parts[k] = c.asc ? v : ~v;
        }
        masks_out[i] = mask;
    }
}

// ---- stage 2: parallel stable comparison sort ------------------------------------

// Full stable sort of `perm`, parallelized: each of `nt` threads stable_sorts its
// own disjoint contiguous slice (no shared writes, safe without locks), then the
// slices are merged pairwise in a binary tree.
// One-shot pool-let, same idiom as GroupBySink::finalize / UngroupedAggGlobal's
// COUNT(DISTINCT) merge — thread only when it's worth it.
template <class Cmp>
inline void parallel_stable_sort_cmp(Cmp cmp, std::vector<uint32_t>& perm) {
    const size_t n = perm.size();

    unsigned hw = std::thread::hardware_concurrency();
    unsigned nt = hw > 2 ? static_cast<unsigned>(hw - 2) : 1u;
    if (nt > 16) nt = 16;
    if (n < 200000) nt = 1;   // small: thread spawn/join overhead isn't worth it
    if (nt < 1) nt = 1;

    if (nt <= 1) {
        std::stable_sort(perm.begin(), perm.end(), cmp);
        return;
    }

    size_t chunk = (n + nt - 1) / nt;
    std::vector<std::pair<size_t, size_t>> ranges;
    for (size_t s = 0; s < n; s += chunk) ranges.emplace_back(s, std::min(s + chunk, n));

    std::vector<std::thread> threads;
    threads.reserve(ranges.size() - 1);
    for (size_t r = 1; r < ranges.size(); ++r) {
        threads.emplace_back([&perm, &cmp, &ranges, r]() {
            std::stable_sort(perm.begin() + static_cast<ptrdiff_t>(ranges[r].first),
                             perm.begin() + static_cast<ptrdiff_t>(ranges[r].second), cmp);
        });
    }
    std::stable_sort(perm.begin() + static_cast<ptrdiff_t>(ranges[0].first),
                     perm.begin() + static_cast<ptrdiff_t>(ranges[0].second), cmp);
    for (std::thread& t : threads) t.join();

    if (ranges.size() == 1) return;

    // Bottom-up merge into an explicit scratch buffer, ping-ponging each round —
    // deliberately NOT std::inplace_merge. inplace_merge's O(n) path depends on
    // successfully allocating a same-sized internal temp buffer; when that fails
    // (a real risk merging multi-million-element ranges) it silently falls back
    // to an O(n log n) rotation-based algorithm — correct, but a catastrophic,
    // easy-to-miss perf cliff at this scale. An explicit buffer makes every
    // round's O(n) cost guaranteed rather than a library heuristic.
    std::vector<uint32_t> scratch(n);
    uint32_t* src = perm.data();
    uint32_t* dst = scratch.data();
    while (ranges.size() > 1) {
        std::vector<std::pair<size_t, size_t>> next_ranges;
        for (size_t i = 0; i + 1 < ranges.size(); i += 2) {
            size_t a0 = ranges[i].first, a1 = ranges[i].second, a2 = ranges[i + 1].second;
            std::merge(src + a0, src + a1, src + a1, src + a2, dst + a0, cmp);
            next_ranges.emplace_back(a0, a2);
        }
        if (ranges.size() % 2 == 1) {
            const auto& last = ranges.back();
            std::copy(src + last.first, src + last.second, dst + last.first);
            next_ranges.push_back(last);
        }
        std::swap(src, dst);
        ranges = std::move(next_ranges);
    }
    if (src != perm.data()) std::copy(src, src + n, perm.data());
}

// Pre-unification entry point, kept for callers that hold SortKeyColumns and want
// the general comparator explicitly.
inline void parallel_stable_sort_perm(const std::vector<SortKeyColumn>& keys,
                                      std::vector<uint32_t>& perm) {
    parallel_stable_sort_cmp(SortKeyCmp{keys}, perm);
}

// ---- the two-stage sort ------------------------------------------------------------

// Run-count cutoff for the vergesort prepass: merge K sorted runs only when K is
// small enough that log2(K) merge passes beat sorting outright.
//
// DERIVED EMPIRICALLY, 2026-07-28, via dev/sort_key_bench/bench_vergesort_threshold.cpp
// (5M rows, AoS comparator, run counts 1..2048, 1-4 key columns, 0/30% nulls):
//     K=16 -> vergesort wins 1.16x / 1.20x / 1.28x across the three configurations
//     K=32 -> 0.90x / 1.03x / 0.95x  (at or below parity — loses in 2 of 3)
// so 16, not the 32 this file's predecessor used. That 32 was correct for the RADIX
// fallback it was written against ("our fallback is radix sort (O(8n)), which is much
// cheaper" — draken/core/vergesort.h); a comparison-sort fallback is dearer, which
// moves the crossover DOWN, not up. Re-derive if the fallback changes again.
// The threshold is conservative at small n: below 200k rows the fallback runs
// single-threaded, so vergesort wins by more than these numbers (measured with the
// fallback at full parallel strength).
inline constexpr uint32_t SORT_VERGESORT_THRESHOLD = 16;

// Sort `perm` (pre-filled with row ids) by `cmp`.
//
// `take_first`: rows actually consumed downstream. SIZE_MAX (full sort) runs the
// vergesort prepass then the parallel stable sort. A real limit uses partial_sort —
// O(n log k) instead of O(n log n), the difference between compacting 65k TopN
// candidates to 10 and fully sorting them — and deliberately SKIPS vergesort: an
// extra O(n) run-detection scan cannot pay for itself against a bound that is
// already cheaper than one full pass. Ties at the boundary are unspecified either
// way (SQL's ORDER BY..LIMIT contract; cross-worker compaction is already
// tie-unstable).
template <class Cmp>
inline void sort_perm_cmp(Cmp cmp, std::vector<uint32_t>& perm, size_t take_first) {
    const size_t n = perm.size();
    if (take_first < n) {
        std::partial_sort(perm.begin(),
                          perm.begin() + static_cast<ptrdiff_t>(take_first),
                          perm.end(), cmp);
        return;
    }
    if (n < 2) return;

    // Stage 1. `tmp` is only written during the merge, so on a miss its pages are
    // never faulted in — new[] (not vector) to skip the value-initialization that
    // would touch all n*4 bytes up front.
    std::unique_ptr<uint32_t[]> tmp(new uint32_t[n]);
    // +3, not +2 — see vergesort_generic's contract: num_runs can reach
    // threshold+2 and the sentinel write needs one slot past that.
    uint32_t runs[SORT_VERGESORT_THRESHOLD + 3];
    if (vergesort_generic(perm.data(), tmp.get(), cmp, n,
                          SORT_VERGESORT_THRESHOLD, runs)) {
        return;
    }
    // Stage 2. On a miss vergesort has reversed some descending runs in place, but
    // that does NOT disturb stability: runs are extended with STRICT `<`, so a
    // reversed run contains no equal keys, and reversal keeps every element inside
    // its own run's index range — so the relative order of equal keys is unchanged
    // and this sort yields exactly what it would have from the identity permutation.
    parallel_stable_sort_cmp(cmp, perm);
}

// Stable multi-key permutation over `perm`, dispatching to the AoS comparator when
// the key shape allows it and to SortKeyCmp otherwise. Both produce the same order.
inline void sort_perm(const std::vector<SortKeyColumn>& keys, std::vector<uint32_t>& perm,
                      size_t take_first = SIZE_MAX) {
    const size_t n = perm.size();
    if (aos_keys_eligible(keys) && aos_build_worth_it(n, take_first)) {
        switch (keys.size()) {
#define DRAKEN_SORT_AOS_ARM(NP)                                                  \
            case NP: {                                                           \
                std::vector<RowKeyN<NP>> rows;                                   \
                std::vector<uint8_t> masks;                                      \
                std::array<bool, NP> asc{};                                      \
                build_aos_keys<NP>(keys, n, rows, masks, asc);                   \
                sort_perm_cmp(AoSKeyCmpN<NP>{rows.data(), masks.data(), asc},    \
                              perm, take_first);                                 \
                return;                                                          \
            }
            DRAKEN_SORT_AOS_ARM(1)
            DRAKEN_SORT_AOS_ARM(2)
            DRAKEN_SORT_AOS_ARM(3)
            DRAKEN_SORT_AOS_ARM(4)
#undef DRAKEN_SORT_AOS_ARM
            default: break;   // unreachable — aos_keys_eligible bounds the size
        }
    }
    sort_perm_cmp(SortKeyCmp{keys}, perm, take_first);
}

// Zero-row typed column — the courtesy empty-result morsel (schema visibility when a
// query legitimately returns no rows). String-family columns get a canonical empty
// DrakenStringArena header (buffers.h: a string vector's `data` points at the arena
// STRUCT, even when empty).
inline CxxColumn make_empty_col(DrakenType t, const LogicalType* lt) {
    void* data;
    if (draken_type_is_string_storage(t)) {
        auto* sa = static_cast<DrakenStringArena*>(draken_malloc(sizeof(DrakenStringArena)));
        sa->slots = nullptr; sa->arena = nullptr; sa->length = 0;
        sa->arena_used = 0; sa->arena_cap = 0; sa->null_bitmap = nullptr;
        sa->owns_buffers = 0; sa->type = t;
        sa->payloads_elided = 0;
        data = sa;
    } else {
        data = draken_malloc(1);
    }
    uint32_t* sel = static_cast<uint32_t*>(draken_malloc(sizeof(uint32_t)));
    DrakenVector v;
    v.data = data; v.selection = sel; v.data_length = 0; v.length = 0;
    v.validity = nullptr; v.type = t; v.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
    CxxColumn c;
    c.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(data),
                                          OwnedBuffer<uint8_t>(nullptr), OwnedBuffer<void>(sel));
    // TIMESTAMP64/DECIMAL/DECIMAL128 carry their descriptor out-of-band on the owner,
    // never on DrakenVector itself (draken/logical_type.h) — a zero-row column is no
    // exception; omitting it here left the courtesy empty-result morsel with a
    // TIMESTAMP64/DECIMAL column draken treats as a hard error the moment it's
    // re-encoded (e.g. a query with 0 rows still gets written out to Parquet).
    c.own->logical_type = lt;
    c.view = c.own->vec;
    return c;
}

// ---- general row gather -----------------------------------------------------------
// Copy `order[first..first+count)` (global row ids over `ms`, any order) into ONE
// fresh dense morsel. `row_m`/`row_r` map a global row id to (morsel, local row).

// Sentinel `order` entry meaning "this output row is NULL in every column" — it
// addresses no source row at all, so row_m/row_r are NOT consulted for it.
//
// Exists for LEFT OUTER / ASOF joins, whose unmatched probe rows must emit a fully
// NULL build side. Every type arm below already has a null path (for a source row
// whose validity bit is clear), so this rides that path instead of adding a second
// one — which is precisely why it costs nothing per type and cannot reintroduce
// per-type gaps. A caller with no unmatched rows never passes it.
inline constexpr uint32_t kGatherNullRow = UINT32_MAX;

// Callers reach this only for the plain fixed-width families — string
// (sort_type_is_string) and BOOL are intercepted earlier in gather_rows with
// their own row-store encodings, and ARRAY recurses on its child vector — so
// this is a pure alias to the canonical table, kept in one place so it can't
// drift from join_elem_size/concat_fixed_itemsize again. `lt` is the column's
// logical descriptor: only VECTOR_FP16 reads it (its stride is dimension × 2),
// every other type ignores it.
inline size_t gather_elem_size(DrakenType t, const LogicalType* lt) {
    return draken_type_itemsize(t, lt);
}

inline MorselPtr gather_rows(const std::vector<MorselPtr>& ms,
                             const std::vector<uint32_t>& order,
                             size_t first, size_t count,
                             const std::vector<uint32_t>& row_m,
                             const std::vector<uint32_t>& row_r,
                             const std::vector<std::string>& names,
                             ErrCtx& err) {
    uint32_t n = static_cast<uint32_t>(count);
    auto out = std::make_shared<CxxMorsel>();
    out->names = names;
    out->zero_col_rows = n;
    if (ms.empty()) return out;
    size_t ncols = ms.front()->columns.size();
    out->columns.reserve(ncols);
    size_t vbytes = (static_cast<size_t>(n) + 7) / 8;

    for (size_t ci = 0; ci < ncols; ++ci) {
        DrakenType t = ms.front()->columns[ci].view.type;
        // Parameterized physical types (DECIMAL scale, TIMESTAMP unit, …) carry a
        // registry-interned logical descriptor on the owner — it must survive the
        // gather or the cursor's materialization fails loud.
        const LogicalType* src_lt =
            ms.front()->columns[ci].own ? ms.front()->columns[ci].own->logical_type
                                        : nullptr;

        // Validity: allocate lazily on the first NULL encountered.
        uint8_t* vbits = nullptr;
        auto mark_null = [&](uint32_t i) {
            if (vbits == nullptr) {
                vbits = static_cast<uint8_t*>(draken_malloc(vbytes == 0 ? 1 : vbytes));
                std::memset(vbits, 0xFF, vbytes == 0 ? 1 : vbytes);
            }
            vbits[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7)));
        };

        if (t == DRAKEN_NULL) {
            // Self-describing null (buffers.h): type==NULL ⟹ every row is null,
            // no data buffer and no validity buffer. Which source rows were picked
            // is irrelevant — the gather of n rows is just a length-n NULL vector.
            // `selection` is the shared global zero vector (not owned); `data` is
            // genuinely nullptr, which is why this cannot go through the width path
            // below (0 there means "unsupported", not "no bytes by nature").
            DrakenVector v;
            v.data = nullptr;
            v.selection = draken_zero_sel(n > 0 ? n : 1);
            v.data_length = 0;
            v.length = n;
            v.validity = nullptr;
            v.type = DRAKEN_NULL;
            v.flags = 0;
            CxxColumn c;
            c.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(nullptr),
                                                  OwnedBuffer<uint8_t>(nullptr));
            c.own->logical_type = src_lt;
            c.view = c.own->vec;
            out->columns.push_back(std::move(c));
            continue;
        }

        if (draken_type_is_string_storage(t)) {
            // Two-pass string gather into ONE canonical consolidated block:
            // [DrakenStringArena header | slots[n] | arena bytes] — `data` points at
            // the header, exactly what draken's own kernels read (buffers.h contract).
            size_t total_arena = 0;
            for (uint32_t i = 0; i < n; ++i) {
                uint32_t g = order[first + i];
                if (g == kGatherNullRow) continue;   // NULL row: no arena bytes
                const DrakenVector& v = ms[row_m[g]]->columns[ci].view;
                uint32_t r = row_r[g];
                if (!sort_row_valid(v, r)) continue;
                const DrakenStringSlot* slot = &string_arena_of(v)->slots[v.selection[r]];
                if (!str_is_inline(slot)) total_arena += str_length(slot);
            }
            size_t slots_off = sizeof(DrakenStringArena);
            size_t arena_off = slots_off + static_cast<size_t>(n == 0 ? 1 : n) * sizeof(DrakenStringSlot);
            uint8_t* blk = static_cast<uint8_t*>(draken_malloc(arena_off + total_arena));
            auto* sa_out = reinterpret_cast<DrakenStringArena*>(blk);
            auto* dst = reinterpret_cast<DrakenStringSlot*>(blk + slots_off);
            uint8_t* out_arena = total_arena > 0 ? blk + arena_off : nullptr;
            sa_out->slots = dst;
            sa_out->arena = out_arena;
            sa_out->length = n;
            sa_out->arena_used = total_arena;
            sa_out->arena_cap = total_arena;
            sa_out->null_bitmap = nullptr;
            sa_out->owns_buffers = 0;   // the VectorOwner frees the one block
            sa_out->payloads_elided = 0;
            sa_out->type = t;
            size_t arena_pos = 0;
            for (uint32_t i = 0; i < n; ++i) {
                uint32_t g = order[first + i];
                if (g == kGatherNullRow) {
                    std::memset(&dst[i], 0, sizeof(DrakenStringSlot));
                    mark_null(i);
                    continue;
                }
                const DrakenVector& v = ms[row_m[g]]->columns[ci].view;
                uint32_t r = row_r[g];
                if (!sort_row_valid(v, r)) {
                    std::memset(&dst[i], 0, sizeof(DrakenStringSlot));
                    mark_null(i);
                    continue;
                }
                const DrakenStringArena* sa = string_arena_of(v);
                const DrakenStringSlot* slot = &sa->slots[v.selection[r]];
                if (str_is_inline(slot)) {
                    dst[i] = *slot;
                } else {
                    uint32_t slen = str_length(slot);
                    std::memcpy(out_arena + arena_pos, str_data(slot, sa->arena), slen);
                    str_clone_with_offset(&dst[i], slot, static_cast<uint32_t>(arena_pos));
                    arena_pos += slen;
                }
            }
            uint32_t* sel = static_cast<uint32_t*>(
                draken_malloc((n == 0 ? 1 : n) * sizeof(uint32_t)));
            for (uint32_t i = 0; i < n; ++i) sel[i] = i;
            DrakenVector v;
            v.data = sa_out; v.selection = sel; v.data_length = n; v.length = n;
            v.validity = vbits; v.type = t;
            v.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
            CxxColumn c;
            c.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(blk),
                                                  OwnedBuffer<uint8_t>(vbits),
                                                  OwnedBuffer<void>(sel));
            c.own->logical_type = src_lt;
            c.view = c.own->vec;
            out->columns.push_back(std::move(c));
            continue;
        }

        if (t == DRAKEN_BOOL) {
            // Bit-packed values: gather bit by bit.
            size_t dbytes = (static_cast<size_t>(n) + 7) / 8;
            uint8_t* data = static_cast<uint8_t*>(draken_malloc(dbytes == 0 ? 1 : dbytes));
            std::memset(data, 0, dbytes == 0 ? 1 : dbytes);
            for (uint32_t i = 0; i < n; ++i) {
                uint32_t g = order[first + i];
                if (g == kGatherNullRow) { mark_null(i); continue; }  // data bit stays 0
                const DrakenVector& v = ms[row_m[g]]->columns[ci].view;
                uint32_t r = row_r[g];
                if (!sort_row_valid(v, r)) { mark_null(i); continue; }
                uint32_t phys = v.selection[r];
                if ((static_cast<const uint8_t*>(v.data)[phys >> 3] >> (phys & 7)) & 1u) {
                    data[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
                }
            }
            uint32_t* sel = static_cast<uint32_t*>(
                draken_malloc((n == 0 ? 1 : n) * sizeof(uint32_t)));
            for (uint32_t i = 0; i < n; ++i) sel[i] = i;
            DrakenVector v;
            v.data = data; v.selection = sel; v.data_length = n; v.length = n;
            v.validity = vbits; v.type = t;
            v.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
            CxxColumn c;
            c.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(data),
                                                  OwnedBuffer<uint8_t>(vbits),
                                                  OwnedBuffer<void>(sel));
            c.own->logical_type = src_lt;
            c.view = c.own->vec;
            out->columns.push_back(std::move(c));
            continue;
        }

        if (t == DRAKEN_ARRAY) {
            // An ARRAY column is an int32 offsets buffer plus a flat child vector
            // hung off VectorOwner::child_owner. Gathering rows means gathering each
            // row's element RANGE out of the child — and the child can be any type
            // (including another ARRAY). Rather than restate every per-type gather
            // here, expand the ranges into child-level (morsel, row) pairs and RECURSE
            // on a synthetic one-column view of each source's child.
            std::vector<uint32_t> c_order, c_row_m, c_row_r;
            int32_t* offsets = static_cast<int32_t*>(
                draken_malloc((static_cast<size_t>(n) + 1) * sizeof(int32_t)));
            offsets[0] = 0;
            for (uint32_t i = 0; i < n; ++i) {
                uint32_t g = order[first + i];
                if (g == kGatherNullRow) {   // empty element range + NULL parent
                    offsets[i + 1] = offsets[i];
                    mark_null(i);
                    continue;
                }
                const CxxColumn& sc = ms[row_m[g]]->columns[ci];
                const DrakenVector& v = sc.view;
                uint32_t r = row_r[g];
                if (!sort_row_valid(v, r)) {
                    offsets[i + 1] = offsets[i];
                    mark_null(i);
                    continue;
                }
                if (!sc.own || !sc.own->child_owner) {
                    draken_free(offsets);
                    if (vbits) draken_free(vbits);
                    err.code = 1;
                    err.msg = "gather_rows: ARRAY column has no child vector — fail "
                              "loud, never silent corruption";
                    return nullptr;
                }
                const int32_t* soff = static_cast<const int32_t*>(v.data);
                uint32_t phys = v.selection[r];
                int32_t s0 = soff[phys], s1 = soff[phys + 1];
                for (int32_t j = s0; j < s1; ++j) {
                    c_order.push_back(static_cast<uint32_t>(c_row_m.size()));
                    c_row_m.push_back(row_m[g]);
                    c_row_r.push_back(static_cast<uint32_t>(j));
                }
                offsets[i + 1] = offsets[i] + (s1 - s0);
            }

            // One synthetic morsel per source morsel, index-parallel to `ms` so the
            // child-level row_m indices line up. The aliasing shared_ptr points at the
            // child owner while keeping the PARENT owner alive — the child's buffers
            // belong to that subtree, not to us.
            std::vector<MorselPtr> child_ms;
            child_ms.reserve(ms.size());
            for (const MorselPtr& m : ms) {
                auto cm = std::make_shared<CxxMorsel>();
                const CxxColumn& sc = m->columns[ci];
                CxxColumn cc;
                if (sc.own && sc.own->child_owner) {
                    cc.own = std::shared_ptr<VectorOwner>(sc.own, sc.own->child_owner.get());
                    cc.view = sc.own->child_owner->vec;
                }
                cm->columns.push_back(std::move(cc));
                cm->names.push_back("c");
                child_ms.push_back(std::move(cm));
            }
            std::vector<std::string> child_names{"c"};
            MorselPtr child_out = gather_rows(child_ms, c_order, 0, c_order.size(),
                                              c_row_m, c_row_r, child_names, err);
            if (child_out == nullptr || err.code != 0) {
                draken_free(offsets);
                if (vbits) draken_free(vbits);
                return nullptr;
            }

            uint32_t* sel = static_cast<uint32_t*>(
                draken_malloc((n == 0 ? 1 : n) * sizeof(uint32_t)));
            for (uint32_t i = 0; i < n; ++i) sel[i] = i;
            DrakenVector av;
            av.data = offsets; av.selection = sel; av.data_length = n; av.length = n;
            av.validity = vbits; av.type = DRAKEN_ARRAY;
            av.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
            CxxColumn c;
            c.own = std::make_shared<VectorOwner>(av, OwnedBuffer<void>(offsets),
                                                  OwnedBuffer<uint8_t>(vbits),
                                                  OwnedBuffer<void>(sel));
            c.own->logical_type = src_lt;
            // child_out's column owner was just built here (use_count()==1), so its
            // VectorOwner can be moved into sole ownership under this parent.
            c.own->child_owner =
                std::make_unique<VectorOwner>(std::move(*child_out->columns[0].own));
            c.view = c.own->vec;
            out->columns.push_back(std::move(c));
            continue;
        }

        size_t es = gather_elem_size(t, src_lt);
        if (es == 0) {
            err.code = 1;
            err.msg = "gather_rows: unsupported column type (e.g. VARIANT, or a "
                      "VECTOR with no dimension descriptor) — fail loud, never "
                      "silent corruption";
            return nullptr;
        }
        uint8_t* data = static_cast<uint8_t*>(
            draken_malloc((n == 0 ? 1 : static_cast<size_t>(n)) * es));
        for (uint32_t i = 0; i < n; ++i) {
            uint32_t g = order[first + i];
            if (g == kGatherNullRow) {
                std::memset(data + static_cast<size_t>(i) * es, 0, es);
                mark_null(i);
                continue;
            }
            const DrakenVector& v = ms[row_m[g]]->columns[ci].view;
            uint32_t r = row_r[g];
            if (!sort_row_valid(v, r)) {
                std::memset(data + static_cast<size_t>(i) * es, 0, es);
                mark_null(i);
                continue;
            }
            std::memcpy(data + static_cast<size_t>(i) * es,
                        static_cast<const uint8_t*>(v.data)
                            + static_cast<size_t>(v.selection[r]) * es,
                        es);
        }
        uint32_t* sel = static_cast<uint32_t*>(
            draken_malloc((n == 0 ? 1 : n) * sizeof(uint32_t)));
        for (uint32_t i = 0; i < n; ++i) sel[i] = i;
        DrakenVector v;
        v.data = data; v.selection = sel; v.data_length = n; v.length = n;
        v.validity = vbits; v.type = t;
        v.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
        CxxColumn c;
        c.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(data),
                                              OwnedBuffer<uint8_t>(vbits),
                                              OwnedBuffer<void>(sel));
        c.own->logical_type = src_lt;
        c.view = c.own->vec;
        out->columns.push_back(std::move(c));
    }
    return out;
}

// Flatten a morsel list into global-row maps. Returns total row count.
inline size_t flatten_rows(const std::vector<MorselPtr>& ms,
                           std::vector<uint32_t>& row_m, std::vector<uint32_t>& row_r) {
    size_t n = 0;
    for (const MorselPtr& m : ms) n += m->num_rows();
    row_m.reserve(n);
    row_r.reserve(n);
    for (uint32_t mi = 0; mi < ms.size(); ++mi) {
        uint32_t rows = ms[mi]->num_rows();
        for (uint32_t r = 0; r < rows; ++r) {
            row_m.push_back(mi);
            row_r.push_back(r);
        }
    }
    return n;
}

// ---- the entry point --------------------------------------------------------------

// Sort every row across `ms` by `spec`, keep the first `take_first` (SIZE_MAX = all),
// and append the sorted rows to `out` as dense morsels of at most `chunk_rows` rows.
//
// Engine-agnostic on purpose: no Sink, no MorselBuffer, no plan. This is what rugo
// calls with no query engine present, and what opteryx's SortSink/TopNSink wrap.
// Returns false with `err` set on failure — never a silent partial or wrong order.
inline bool sort_morsels(const std::vector<MorselPtr>& ms,
                         const std::vector<SortKeySpec>& spec,
                         size_t take_first, size_t chunk_rows,
                         std::vector<MorselPtr>& out, ErrCtx& err) {
    std::vector<MorselPtr> src;
    src.reserve(ms.size());
    for (const MorselPtr& m : ms) if (m->num_rows() > 0) src.push_back(m);
    if (src.empty()) return true;

    std::vector<uint32_t> row_m, row_r;
    size_t n = flatten_rows(src, row_m, row_r);
    std::vector<SortKeyColumn> keys;
    if (!build_sort_keys(src, spec, n, keys, err)) return false;
    std::vector<uint32_t> perm(n);
    for (size_t i = 0; i < n; ++i) perm[i] = static_cast<uint32_t>(i);
    sort_perm(keys, perm, take_first);

    size_t total = n < take_first ? n : take_first;
    const std::vector<std::string>& names = src.front()->names;
    for (size_t start = 0; start < total; start += chunk_rows) {
        size_t count = std::min(chunk_rows, total - start);
        MorselPtr m = gather_rows(src, perm, start, count, row_m, row_r, names, err);
        if (err.code != 0) return false;
        // Each chunk is a contiguous slice of the globally-sorted permutation, so
        // the PRIMARY (leading) sort key is PROVEN monotonic within it — not a
        // hint, this operator just produced the fact. Secondary keys are only
        // ordered within ties of the primary key, not globally, so they are not
        // marked. Covers every caller of sort_morsels (opteryx's SortSink/TopNSink/
        // Window operators and the standalone rugo wheel) uniformly, for free.
        //
        // CxxColumn.view is a hot-path-only inline COPY of own->vec (see
        // cxx_morsel.h) — the Python-visible Vector reads own->vec via
        // to_vectors(), so both copies must be set or the flag is invisible
        // outside this translation unit despite compiling clean.
        if (!spec.empty() && spec[0].col_idx < m->columns.size()) {
            CxxColumn& col = m->columns[spec[0].col_idx];
            uint8_t bits = DRAKEN_ROW_SORTED | (spec[0].ascending ? 0 : DRAKEN_ROW_SORTED_DESC);
            uint8_t clear = static_cast<uint8_t>(~(DRAKEN_ROW_SORTED | DRAKEN_ROW_SORTED_DESC));
            col.view.flags = static_cast<uint8_t>((col.view.flags & clear) | bits);
            if (col.own)
                col.own->vec.flags = static_cast<uint8_t>((col.own->vec.flags & clear) | bits);
        }
        out.push_back(std::move(m));
    }
    return true;
}
