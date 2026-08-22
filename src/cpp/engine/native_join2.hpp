#pragma once
// src/cpp/engine/native_join2.hpp — the engine's general hash join: multi-column
// keys of any supported type (serialized with the SAME key encoding GROUP BY and
// DISTINCT use — null byte + raw native bytes / length-prefixed strings, so
// DECIMAL128 and string keys come for free), and four probe modes:
//
//   INNER       — fan-out matches (build payload first, then probe payload).
//   LEFT_OUTER  — probe side is the PRESERVED side (the compiler maps the plan's
//                 preserved leg to the probe): every probe row emits; unmatched
//                 rows carry NULL build payload.
//   SEMI        — emit probe rows that have >=1 match (probe columns only).
//   ANTI_NULL_AWARE — NOT IN: emit probe rows with NO match; a NULL probe key never
//                 matches-out (NULL NOT IN <non-empty> is NULL → drop); if the build
//                 side contained ANY NULL key, NOTHING passes; an EMPTY build side
//                 passes every probe row (NOT IN () is TRUE).
//   ANTI        — plain anti (NOT EXISTS / EXCEPT / a full outer's unmatched leg):
//                 emit probe rows with NO match. NULLs are NOT special here — a NULL
//                 key simply never equals anything, so a NULL on EITHER side is a
//                 non-match and the probe row passes. This is NOT interchangeable
//                 with ANTI_NULL_AWARE: `NOT EXISTS (SELECT 1 FROM T WHERE T.k = x)`
//                 is TRUE when T.k is NULL (NULL = x is UNKNOWN → no match), whereas
//                 `x NOT IN (…, NULL)` is UNKNOWN → drop. Collapsing the two made
//                 NOT EXISTS return NOTHING whenever the inner key held a single
//                 NULL.
//
// Anything outside a mode's contract sets ErrCtx — loud, never silently wrong.

#include <algorithm>   // sort/lower_bound/upper_bound — ASOF probe
#include <atomic>      // parallel CSR build (histogram + scatter cursors)
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <thread>      // finalize pool-let for the parallel CSR build
#include <unordered_map>
#include <vector>

#include "operator.hpp"
#include "native_expression.hpp"    // ExprProgram/ExprEvalFn — SEMI/ANTI residual
#include "native_group_sinks.hpp"   // shared engine helpers
#include "native_key_hash.hpp"     // compute_row_hashes — draken owns the key hash
#include "native_sort.hpp"          // gather_rows, sort_row_valid, string helpers
#include "morsels/cxx_hash.h"       // cxx_hash_c — draken owns the join-key hash
#include "carchar_join_index.hpp"   // opteryx::carchar::CarcharJoinIndex

namespace opteryx::engine {

// SemiNotDistinct/AntiNotDistinct are Semi/Anti under SQL's IS NOT DISTINCT FROM key
// comparison — NULL is a value equal to itself. That is how INTERSECT and EXCEPT
// compare rows, and it is a THIRD rule, distinct from BOTH of the others: Semi/Anti
// make a NULL key unmatchable, and AntiNullAware propagates NOT IN's UNKNOWN (one
// NULL on the build side empties the result). Using either for a set operation is a
// wrong answer — see Join2BuildSink::null_equal.
enum class JoinMode : uint8_t {
    Inner = 0, LeftOuter = 1, Semi = 2, AntiNullAware = 3, Anti = 4, FullOuter = 5,
    SemiNotDistinct = 6, AntiNotDistinct = 7
};

// Does this mode compare keys with IS NOT DISTINCT FROM (NULL equals NULL)?
inline bool join_mode_null_equal(JoinMode m) {
    return m == JoinMode::SemiNotDistinct || m == JoinMode::AntiNotDistinct;
}

// Is this an ARRAY column that cannot have NULL rows gathered against it?
//
// gather_rows materializes an ARRAY row by recursing on the column's CHILD vector
// (vector_owner.h: child_owner is non-null for DRAKEN_ARRAY), and it does so even
// when every requested row is the null-row sentinel — an all-NULL ARRAY half still
// has to emit a typed, empty child. A childless ARRAY column therefore has no type
// to build that child from, and the recursion would read an uninitialized view.
//
// The join's plan-typed schema morsels build their child from the planner's element
// type (make_empty_col), so this only fires when the planner could not resolve one.
// It checks the whole subtree: ARRAY<ARRAY<T>> needs a child at every level.
inline bool array_child_missing(const CxxColumn& c) {
    if (c.view.type != DRAKEN_ARRAY) return false;
    for (const VectorOwner* o = c.own.get(); ; o = o->child_owner.get()) {
        if (o == nullptr || o->child_owner == nullptr) return true;
        if (o->child_owner->vec.type != DRAKEN_ARRAY) return false;
    }
}

// The build side RETAINS its payload columns rather than copying their values into a
// row-store. `morsels` holds one payload-column-only view per accepted build morsel
// (a CxxColumn is a shared_ptr to its VectorOwner, so slicing to the payload columns
// genuinely releases the rest of the morsel); `row_m`/`row_r` map a build row id to
// (morsel, row within it) — exactly the shape gather_rows already consumes, and the
// same thing SortSink does with its buffered input. That is what lets the emit path
// be the engine's ONE row gather for both halves of the output row, instead of a
// bespoke per-type materializer that has to be taught every new type.
struct Join2BuildLocal : LocalSinkState {
    std::vector<MorselPtr> morsels;    // payload-column views, in arrival order
    std::vector<uint32_t> row_m;       // build row id -> index into `morsels`
    std::vector<uint32_t> row_r;       // build row id -> row within that morsel
    std::vector<uint64_t> row_hashes;  // parallel to build rows: the 64-bit key hash
    std::vector<uint64_t> asof_keys;   // ASOF only: per build row, sort_num_key
    // ASOF with a STRING match column: parallel to asof_keys, pointing into the
    // retained morsel's arena (see AsofKey). Empty for a numeric/temporal key.
    std::vector<const uint8_t*> asof_str_ptr;
    std::vector<uint32_t> asof_str_len;
    std::vector<__int128> asof_wide;   // ASOF with a DECIMAL128 match column
    // FULL OUTER only (track_matches): NULL-keyed build rows. Every other mode
    // drops them (a NULL key can never equi-match), but FULL OUTER must still
    // emit them in the unmatched-build tail — so their addresses are retained
    // here, OUTSIDE the keyed row space the CSR is built over.
    std::vector<uint32_t> null_row_m, null_row_r;
    uint32_t next_row = 0;
    bool saw_null_key = false;
};
// ---- ASOF ordering key -------------------------------------------------------------
// One ASOF MATCH_CONDITION key. A numeric/temporal key rides `num`, which is
// sort_num_key-normalized so unsigned `<` IS value order (IEEE-correct for floats).
// A STRING-FAMILY key cannot be reduced to 8 bytes without losing order past the
// eighth, so it rides (ptr, len) pointing straight into the retained build morsel's
// string arena — the same no-copy ownership the build table already relies on
// ("no string-arena rebasing, because no arena was ever built: the slots still live
// in the source vectors, which we hold alive" — Join2BuildSink::combine).
//
// Before this existed, `sort_num_key` was applied to string vectors too. It returns
// a meaningless integer for them, so the bisect ordered garbage and ASOF emitted
// matches that VIOLATED THE MATCH_CONDITION THE USER WROTE — 1,999 of 2,000 rows on
// a VARCHAR self-join. Numeric ASOF was and is unaffected.
// The three-way split MIRRORS build_sort_keys' (num / num128 / str) deliberately:
// ASOF's "nearest" and ORDER BY's "next" must agree on what order the values are in,
// and the cheapest way to guarantee that is to order them the same way.
enum class AsofKeyKind : uint8_t { Numeric = 0, Int128 = 1, String = 2 };

inline AsofKeyKind asof_key_kind(DrakenType t) {
    if (sort_type_is_string(t)) return AsofKeyKind::String;
    if (t == DRAKEN_DECIMAL128) return AsofKeyKind::Int128;
    return AsofKeyKind::Numeric;
}

struct AsofKey {
    uint64_t       num  = 0;
    __int128       wide = 0;
    const uint8_t* ptr  = nullptr;
    uint32_t       len  = 0;
};

inline int asof_key_cmp(AsofKeyKind kind, const AsofKey& a, const AsofKey& b) {
    if (kind == AsofKeyKind::Numeric)
        return a.num < b.num ? -1 : (a.num > b.num ? 1 : 0);
    if (kind == AsofKeyKind::Int128)
        return a.wide < b.wide ? -1 : (a.wide > b.wide ? 1 : 0);
    const uint32_t common = a.len < b.len ? a.len : b.len;
    const int r = common ? std::memcmp(a.ptr, b.ptr, common) : 0;
    if (r != 0) return r < 0 ? -1 : 1;
    return a.len < b.len ? -1 : (a.len > b.len ? 1 : 0);
}

// Read one row's ASOF key out of a live vector.
inline AsofKey asof_key_of(const DrakenVector& v, uint32_t row, AsofKeyKind kind) {
    AsofKey key;
    if (kind == AsofKeyKind::Numeric) {
        key.num = sort_num_key(v, row);
        return key;
    }
    if (kind == AsofKeyKind::Int128) {
        std::memcpy(&key.wide,
                    static_cast<const uint8_t*>(v.data)
                        + static_cast<size_t>(v.selection[row]) * 16u, 16u);
        return key;
    }
    const DrakenStringArena* sa = string_arena_of(v);
    const DrakenStringSlot* slot = &sa->slots[v.selection[row]];
    key.ptr = reinterpret_cast<const uint8_t*>(str_data(slot, sa->arena));
    key.len = str_length(slot);
    return key;
}

// ---- the join build table: a hash-bucketed CSR, built in parallel ------------------
// Every non-ASOF join builds this. ASOF is the ONE exception (see Join2BuildSink::
// combine) because it reads the build through CarcharJoinIndex::items()/
// rows_from_payload(), which a CSR does not provide.
//
// The build used to insert every row into a global CarcharJoinIndex one at a time,
// under g.mtx in combine(). That made it the engine's dominant serial region: the
// build scaled 1.15x across 1→8 workers where a probe-heavy join scaled 2.59x, and it
// held TPC-H's parallel fraction down to 0.55. It is replaced by a two-pass scatter
// built once in finalize(): a per-bucket histogram, a prefix sum, then a scatter in
// which every row's destination is computed in advance. Nothing is merged, each row is
// written exactly once, and the per-key std::vector overflow lists are gone.
//
// GROUP BY has to merge its partitions because two workers holding the same group hold
// partial aggregates that must combine. A join build combines nothing — it only
// collects row ids — so placement can be precomputed and the merge disappears.
//
// Match semantics are IDENTICAL to the index this replaced: CarcharJoinIndex keys on
// the 64-bit draken hash alone (index_.lookup_fast(hash)), and so does this — bucket by
// that hash, then compare the stored hash. Measured on TPC-H SF1: the isolated
// build-heavy join went 1.15x → 2.88x across 1→8 workers (parallel fraction 0.15 →
// 0.75), the 21-query total 1.93x → 2.48x (0.55 → 0.68), and Q21 480ms → 165ms.
//
// `hashes` and `rows` stay SEPARATE arrays, deliberately.
//
// Interleaving them into one 16-byte {hash, row} entry looks like the obvious win —
// one stream instead of two — and it was tried. It is wrong for this table: a bucket
// scan compares hashes and reads the row id ONLY on a match, so a probe that misses
// touches `hashes` alone at 8 bytes per entry. Interleaving drags the row id into the
// same cache line and doubles the bytes touched on exactly the path that dominates
// (measured on JOB: 73.7% of 2.4bn probe rows match nothing). Two streams where only
// one is usually read beats one stream that is always read in full.
//
// `rows` is uint32: build row ids index row_m/row_r, which are uint32, and
// kNoBuildRow is UINT32_MAX.
struct JoinCsr {
    size_t mask = 0;
    std::vector<uint32_t> off;      // bucket offsets, size N+1
    std::vector<uint32_t> rows;     // build row ids, grouped by bucket
    std::vector<uint64_t> hashes;   // parallel to `rows`: the stored key hash
    bool built = false;

    // const and thread-safe: the data is read-only once built, so N probe workers
    // share one CSR with no scratch.
    void append_probe_matches(uint64_t key, uint32_t probe_row,
                              std::vector<uint32_t>& build_out,
                              std::vector<uint32_t>& probe_out) const {
        if (!built) return;
        const size_t b = static_cast<size_t>(key) & mask;
        for (uint32_t i = off[b]; i < off[b + 1]; ++i) {
            if (hashes[i] == key) {
                build_out.push_back(rows[i]);
                probe_out.push_back(probe_row);
            }
        }
    }

    size_t row_count_for(uint64_t key) const {
        if (!built) return 0;
        const size_t b = static_cast<size_t>(key) & mask;
        size_t n = 0;
        for (uint32_t i = off[b]; i < off[b + 1]; ++i)
            if (hashes[i] == key) ++n;
        return n;
    }
};

struct Join2BuildGlobal : GlobalSinkState {
    std::mutex mtx;
    // Global build table, keyed on the 64-bit draken hash → build-row-list. Built in
    // combine() under the mutex (single-threaded index construction, exactly as the
    // old unordered_map merge did); probed read-only/concurrently via const methods.
    opteryx::carchar::CarcharJoinIndex index;
    std::vector<MorselPtr> morsels;    // every worker's retained views, concatenated
    std::vector<uint32_t> row_m;       // global build row id -> index into `morsels`
    std::vector<uint32_t> row_r;       // global build row id -> row within that morsel
    // Per-worker row addresses, queued by combine() as an O(1) MOVE and concatenated
    // into row_m/row_r above by merge_build_rows() in finalize(). Same reason the key
    // hashes are queued rather than inserted (see combine()): the alternative is an
    // O(rows) copy inside the global build mutex, which serialises every build row in
    // the query through one critical section no matter how many workers there are.
    // `chunk_morsel_off[c]` is the value to add to chunk c's morsel indices, captured
    // at queue time because it depends on how many morsels were already present.
    std::vector<std::vector<uint32_t>> row_m_chunks, row_r_chunks;
    std::vector<uint32_t> chunk_morsel_off;
    // Zero-row, plan-typed payload columns. Used ONLY when no build morsel was ever
    // retained (a build side that streamed zero rows): gather_rows takes its column
    // count and types from ms.front(), so it needs a schema to emit a LEFT OUTER's
    // all-NULL build half against. Never consulted when real morsels exist — those
    // carry the authoritative (data-observed) types.
    MorselPtr schema_morsel;
    // Non-null once finalize() has decided the build payload is worth
    // CONSOLIDATING: every retained morsel gathered, in build-row-id order, into
    // ONE morsel whose columns are dense. Build row id then indexes it directly
    // (row i of the consolidated morsel IS build row i), which is what lets the
    // probe emit its build half as a DICT — codes over this one block — instead of
    // copying a physical value per output row.
    //
    // `morsels`/`row_m`/`row_r` stay valid and are NOT rewritten: ASOF holds raw
    // arena pointers into the source vectors (asof_str_ptr), so the originals
    // cannot be dropped here. The consolidated block is therefore an ADDITIONAL
    // copy of the build payload, which is why finalize() only builds it when the
    // estimated output is large enough to repay it many times over.
    MorselPtr consolidated;
    std::vector<uint64_t> asof_keys;   // ASOF only: parallel to build rows
    // ASOF with a STRING or DECIMAL128 match column — see AsofKey. `asof_kind` is
    // PLAN-known (the sink reads it off the payload types), never learned from data,
    // so every worker agrees on it before the first morsel arrives.
    std::vector<const uint8_t*> asof_str_ptr;
    std::vector<uint32_t> asof_str_len;
    std::vector<__int128> asof_wide;   // ASOF with a DECIMAL128 match column
    AsofKeyKind asof_kind = AsofKeyKind::Numeric;
    // ASOF only: draken hash → index into asof_sorted, materialized once at first
    // probe (ensure_sorted). CarcharJoinIndex stores each key's rows unsorted, so the
    // bisect needs this sorted view — keyed on the same 64-bit hash via a CarcharIndex.
    opteryx::carchar::CarcharIndex asof_index;
    std::vector<std::vector<int64_t>> asof_sorted;
    uint32_t total_rows = 0;
    bool saw_null_key = false;

    // `csr_active` is set in combine() and is the single discriminant every probe
    // reads. It is true for every join except ASOF, which keeps `index` above — see
    // the JoinCsr comment and combine().
    JoinCsr csr;
    std::vector<std::vector<uint64_t>> hash_chunks;   // per-worker hashes, queued O(1)
    bool csr_active = false;


    // FULL OUTER (track_matches) state, allocated/appended in finalize():
    //   matched[r]     — build row r received >= 1 probe match. mutable + atomic
    //                    because probes are const and concurrent; relaxed byte
    //                    stores (idempotent 0->1) are all that is needed.
    //   tail_null_rows — count of NULL-keyed build rows appended to row_m/row_r
    //                    AFTER the CSR was built: rows [total_rows,
    //                    total_rows + tail_null_rows) are addressable for the
    //                    tail gather but invisible to the CSR, so they can never
    //                    match — the tail emits them unconditionally.
    mutable std::unique_ptr<std::atomic<uint8_t>[]> matched;
    uint32_t tail_null_rows = 0;
    // combine()-time staging for the NULL-keyed rows (rebased to global morsel
    // indices); moved onto the end of row_m/row_r by finalize() once the keyed
    // row space is sealed.
    std::vector<uint32_t> null_row_m, null_row_r;

    // The two build-table operations the probe modes use.
    //
    // These route straight to the CSR: `index` is NOT an alternative table here. It
    // is populated only for ASOF, and AsofProbeOperator overrides execute() to walk
    // its own sorted view — it never calls either of these. The `csr_active ? csr :
    // index` dispatch these used to carry was therefore unreachable on the index
    // side. An empty build (combine() never ran, so csr.built is false) is handled
    // by the CSR itself, which returns no matches — identical to the empty index it
    // would have consulted.
    void probe_append(uint64_t key, uint32_t probe_row,
                      std::vector<uint32_t>& build_out,
                      std::vector<uint32_t>& probe_out) const {
        csr.append_probe_matches(key, probe_row, build_out, probe_out);
    }
    size_t probe_row_count(uint64_t key) const { return csr.row_count_for(key); }
};

// Concatenate the queued per-worker row addresses into row_m/row_r, rebasing each
// chunk's morsel indices as it goes. Called once from finalize(), before anything
// reads the build address space.
//
// This is the other half of "queue, don't insert". combine() used to do this work
// itself, inside the global mutex — an O(rows) push_back loop per worker, so the
// build side of every join was funnelled through one serial critical section while
// the other workers blocked on it. A 25s profile of TPC-H Q21 at SF100 put 62.5% of
// all samples in thread-wait primitives against 15.6% in the join probe itself, with
// 9.5% specifically in __psynch_mutexwait. The hashes were moved off this lock long
// ago for exactly this reason; the row addresses were left behind.
//
// Chunks own disjoint output ranges, so the copy needs no coordination: each worker
// claims whole chunks atomically and writes into its own slice. row_r is copied
// verbatim (a row index WITHIN a morsel is unaffected by concatenation); only row_m
// is rebased.
inline void merge_build_rows(Join2BuildGlobal& g) {
    const size_t nchunks = g.row_m_chunks.size();
    if (nchunks == 0) return;

    std::vector<size_t> base(nchunks, 0);
    size_t running = 0;
    for (size_t i = 0; i < nchunks; ++i) {
        base[i] = running;
        running += g.row_m_chunks[i].size();
    }
    g.row_m.resize(running);
    g.row_r.resize(running);

    unsigned hw = std::thread::hardware_concurrency();
    unsigned nt = hw > 2 ? hw - 2 : 1;
    if (nt > 16) nt = 16;
    if (running < 65536) nt = 1;   // small build: the threads cost more than they save

    std::atomic<size_t> next{0};
    auto work = [&](unsigned) {
        for (;;) {
            const size_t ci = next.fetch_add(1);
            if (ci >= nchunks) break;
            const std::vector<uint32_t>& src_m = g.row_m_chunks[ci];
            const std::vector<uint32_t>& src_r = g.row_r_chunks[ci];
            const uint32_t morsel_off = g.chunk_morsel_off[ci];
            const size_t n = src_m.size();
            uint32_t* dst_m = g.row_m.data() + base[ci];
            uint32_t* dst_r = g.row_r.data() + base[ci];
            for (size_t r = 0; r < n; ++r) dst_m[r] = morsel_off + src_m[r];
            if (n != 0) std::memcpy(dst_r, src_r.data(), n * sizeof(uint32_t));
        }
    };
    std::vector<std::thread> th;
    th.reserve(nt - 1);
    for (unsigned t = 1; t < nt; ++t) th.emplace_back(work, t);
    work(0);
    for (auto& x : th) x.join();

    // Release the per-worker buffers now rather than holding a second copy of the
    // whole build address space alive until the sink is destroyed.
    g.row_m_chunks.clear();
    g.row_m_chunks.shrink_to_fit();
    g.row_r_chunks.clear();
    g.row_r_chunks.shrink_to_fit();
    g.chunk_morsel_off.clear();
    g.chunk_morsel_off.shrink_to_fit();
}

// Two-pass parallel CSR construction. Called once from finalize(), which the executor
// runs single-threaded, so it spawns its own one-shot pool-let — the same pattern
// GroupBySink::finalize uses for its partition merge. Row ids are global build row ids:
// chunks were queued in combine() in the same order as row_m/row_r, so chunk c covers
// [base_c, base_c + chunk_c.size()).
inline void build_join_csr(Join2BuildGlobal& g) {
    const size_t total = g.total_rows;
    if (total == 0) return;
    size_t n = 1;
    while (n < total) n <<= 1;    // one bucket per row: ~0.36 distinct keys/bucket here

    JoinCsr& c = g.csr;
    c.mask = n - 1;
    c.off.assign(n + 1, 0);
    c.rows.resize(total);
    c.hashes.resize(total);

    // Flat view of the queued chunks: (chunk index, base global row id).
    std::vector<size_t> base(g.hash_chunks.size(), 0);
    size_t running = 0;
    for (size_t i = 0; i < g.hash_chunks.size(); ++i) {
        base[i] = running;
        running += g.hash_chunks[i].size();
    }

    unsigned hw = std::thread::hardware_concurrency();
    unsigned nt = hw > 2 ? hw - 2 : 1;
    if (nt > 16) nt = 16;
    if (total < 65536) nt = 1;   // small build: the threads cost more than they save

    std::vector<std::atomic<uint32_t>> counts(n);
    for (size_t i = 0; i < n; ++i) counts[i].store(0, std::memory_order_relaxed);


    // Pass 1: per-bucket histogram. Chunks are claimed atomically so a skewed chunk
    // distribution cannot leave a thread idle.
    {
        std::atomic<size_t> next{0};
        auto work = [&](unsigned) {
            for (;;) {
                size_t ci = next.fetch_add(1);
                if (ci >= g.hash_chunks.size()) break;
                for (uint64_t h : g.hash_chunks[ci])
                    counts[static_cast<size_t>(h) & c.mask].fetch_add(1, std::memory_order_relaxed);
            }
        };
        std::vector<std::thread> th;
        th.reserve(nt - 1);
        for (unsigned t = 1; t < nt; ++t) th.emplace_back(work, t);
        work(0);
        for (auto& x : th) x.join();
    }

    // Prefix sum over buckets — O(n), no hashing and no allocation.
    uint32_t run = 0;
    for (size_t b = 0; b < n; ++b) {
        c.off[b] = run;
        run += counts[b].load(std::memory_order_relaxed);
    }
    c.off[n] = run;

    // Pass 2: scatter. cursor[b] hands each writer a slot inside bucket b that no other
    // writer can receive, so the passes need no merge and no per-key allocation.
    {
        std::vector<std::atomic<uint32_t>> cursor(n);
        for (size_t b = 0; b < n; ++b)
            cursor[b].store(c.off[b], std::memory_order_relaxed);
        std::atomic<size_t> next{0};
        auto work = [&](unsigned) {
            for (;;) {
                size_t ci = next.fetch_add(1);
                if (ci >= g.hash_chunks.size()) break;
                const std::vector<uint64_t>& chunk = g.hash_chunks[ci];
                const size_t b0 = base[ci];
                for (size_t r = 0; r < chunk.size(); ++r) {
                    const uint64_t h = chunk[r];
                    const size_t b = static_cast<size_t>(h) & c.mask;
                    const uint32_t p = cursor[b].fetch_add(1, std::memory_order_relaxed);
                    c.rows[p] = static_cast<uint32_t>(b0 + r);
                    c.hashes[p] = h;
                }
            }
        };
        std::vector<std::thread> th;
        th.reserve(nt - 1);
        for (unsigned t = 1; t < nt; ++t) th.emplace_back(work, t);
        work(0);
        for (auto& x : th) x.join();
    }

    c.built = true;
    g.hash_chunks.clear();
    g.hash_chunks.shrink_to_fit();
}

struct Join2BuildSink : Sink {
    std::vector<size_t> key_idx;
    std::vector<size_t> payload_col_idx;
    std::vector<DrakenType> payload_types;             // PLAN-known — see engine.hpp
    std::vector<const LogicalType*> payload_logical;   // set_join2_build_sink's comment
    // Per payload column, the ARRAY element subtree (empty for every non-ARRAY
    // column). An ARRAY column with no child vector cannot have NULL rows emitted
    // against it — see make_empty_col and the guard in build_output below.
    std::vector<std::vector<EmptyColElem>> payload_element;
    int asof_idx = -1;   // >= 0: ASOF build — capture the asof column's normalized
                         // order key per row (rows with a NULL asof value are
                         // skipped: they can never satisfy the MATCH_CONDITION)
    // How is that asof column ordered? PLAN-known and passed in EXPLICITLY, not
    // inferred from `payload_types`: a coerced match column (the synthetic CAST the
    // compiler appends for a cross-type MATCH_CONDITION) sits PAST the payload, so
    // there is no payload entry to read it from — and the coercion target can be
    // DECIMAL128, which orders differently from every 64-bit numeric. See AsofKey.
    AsofKeyKind asof_kind = AsofKeyKind::Numeric;
    bool track_matches = false;   // FULL OUTER: allocate the matched[] flags and
                                  // retain NULL-keyed rows for the unmatched tail
    // SET OPERATIONS (`left semi/anti not-distinct`): NULL is an ordinary key VALUE
    // that equals itself, i.e. SQL's IS NOT DISTINCT FROM, which is how INTERSECT and
    // EXCEPT compare rows. This is a THIRD key rule, not a variant of the ANTI
    // `null_aware` flag on the probe — that one is NOT IN's UNKNOWN propagation and
    // makes a single NULL annihilate the whole result. Here NULL-keyed rows go into
    // the table like any other row and hash identity does the rest: draken hashes
    // NULL per column to the NULL_HASH sentinel before combining, so (2, NULL)
    // already hashes alike on both sides. The rule is therefore the ABSENCE of the
    // exclusion below, never a second comparison path.
    bool null_equal = false;
    // Rows this join is ESTIMATED to emit (JoinBuildShapeStrategy), or -1 for
    // unknown. The only input finalize() cannot measure for itself; it weighs this
    // against the REAL byte size of the retained build payload to decide whether to
    // consolidate. -1 keeps the pre-existing behaviour exactly, so a plan with no
    // statistics is never moved onto the consolidating path by a fabricated number.
    int64_t est_output_rows = -1;

    Join2BuildSink(std::vector<size_t> keys, std::vector<size_t> payload_idx,
                   std::vector<DrakenType> types, std::vector<const LogicalType*> logical,
                   std::vector<std::vector<EmptyColElem>> element,
                   int asof = -1, int asof_type = 0, bool track = false,
                   bool null_eq = false, int64_t est_rows = -1)
        : key_idx(std::move(keys)), payload_col_idx(std::move(payload_idx)),
          payload_types(std::move(types)), payload_logical(std::move(logical)),
          payload_element(std::move(element)),
          asof_idx(asof), track_matches(track), null_equal(null_eq),
          est_output_rows(est_rows) {
        if (asof_idx >= 0)
            asof_kind = asof_key_kind(static_cast<DrakenType>(asof_type));
    }

    // Zero-row payload columns at the PLAN-known types. This is the fallback schema
    // for a build side that streams zero rows (a filtered-to-empty subquery): with no
    // retained morsel there is nothing for gather_rows to read a column count or type
    // from, and a LEFT OUTER still has to emit an all-NULL build half. When any real
    // morsel was retained this is never consulted — observed types beat plan types.
    MorselPtr make_schema_morsel() const {
        auto m = std::make_shared<CxxMorsel>();
        m->columns.reserve(payload_col_idx.size());
        static const std::vector<EmptyColElem> kNoElement;
        for (size_t c = 0; c < payload_col_idx.size(); ++c)
            m->columns.push_back(make_empty_col(
                payload_types[c], payload_logical[c],
                c < payload_element.size() ? payload_element[c] : kNoElement));
        m->names.resize(payload_col_idx.size());
        m->zero_col_rows = 0;
        return m;
    }

    std::unique_ptr<GlobalSinkState> make_global() override {
        auto g = std::make_unique<Join2BuildGlobal>();
        g->schema_morsel = make_schema_morsel();
        g->asof_kind = asof_kind;
        return g;
    }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override {
        return std::make_unique<Join2BuildLocal>();
    }

    SinkResult sink(const MorselPtr& in, GlobalSinkState&, LocalSinkState& ls,
                    ErrCtx& err) override {
        auto& l = static_cast<Join2BuildLocal&>(ls);
        uint32_t rows = in->num_rows();
        if (rows == 0) return SinkResult::CONTINUE;
        // Retain this morsel's PAYLOAD COLUMNS ONLY. A CxxColumn is a shared_ptr to
        // its VectorOwner, so copying just the payload columns keeps those buffers
        // alive and lets every other column of `in` be released when the build
        // pipeline drops it. No values are copied and nothing is decompressed: a
        // dict-encoded build column stays dict-encoded until the probe gathers it.
        //
        // There is no payload TYPE check here any more, and no type learning: the
        // retained vectors ARE the types, and the emit path is the same gather every
        // other operator uses. A payload column is only ever carried, never compared,
        // so its type is not the join's business.
        auto view = std::make_shared<CxxMorsel>();
        view->columns.reserve(payload_col_idx.size());
        for (size_t pc : payload_col_idx) view->columns.push_back(in->columns[pc]);
        view->names.resize(payload_col_idx.size());
        view->zero_col_rows = rows;
        const uint32_t mi = static_cast<uint32_t>(l.morsels.size());
        const uint32_t rows_before = l.next_row;
        const size_t nulls_before = l.null_row_m.size();
        l.morsels.push_back(std::move(view));

        // Draken owns the key hash for the whole morsel; per-row nullness is read from
        // the key columns (not the hash) so NULL keys are excluded as before.
        std::vector<uint64_t> rowh;
        if (!compute_row_hashes(in, key_idx, rowh, err)) return SinkResult::CONTINUE;
        for (uint32_t i = 0; i < rows; ++i) {
            // NULL in ANY key column: the row can never equi-match; record for the
            // null-aware ANTI contract and skip the table insert. Under `null_equal`
            // (set operations) there is nothing to exclude — NULL is a value that
            // matches itself, so the row is inserted on the ordinary path below.
            bool any_null = false;
            if (!null_equal) {
                for (size_t k : key_idx) {
                    if (!sort_row_valid(in->columns[k].view, i)) { any_null = true; break; }
                }
            }
            if (any_null) {
                l.saw_null_key = true;
                // FULL OUTER: a NULL-keyed build row can never match but must
                // still be emitted (NULL-padded) by the unmatched tail — retain
                // its address outside the keyed row space.
                if (track_matches) {
                    l.null_row_m.push_back(mi);
                    l.null_row_r.push_back(i);
                }
                continue;
            }
            if (asof_idx >= 0
                    && !sort_row_valid(in->columns[static_cast<size_t>(asof_idx)].view, i))
                continue;   // NULL asof value never satisfies the MATCH_CONDITION
            // Record WHERE the row lives instead of copying its values out.
            l.row_m.push_back(mi);
            l.row_r.push_back(i);
            if (asof_idx >= 0) {
                const AsofKey key = asof_key_of(
                    in->columns[static_cast<size_t>(asof_idx)].view, i, asof_kind);
                l.asof_keys.push_back(key.num);
                if (asof_kind == AsofKeyKind::String) {
                    l.asof_str_ptr.push_back(key.ptr);
                    l.asof_str_len.push_back(key.len);
                } else if (asof_kind == AsofKeyKind::Int128) {
                    l.asof_wide.push_back(key.wide);
                }
            }
            l.row_hashes.push_back(rowh[i]);   // parallel to the build row just added
            ++l.next_row;
        }
        // Nothing addresses this morsel (no keyed row and — under FULL OUTER —
        // no retained NULL-keyed row either): don't pin its buffers for the
        // lifetime of the build table.
        if (l.next_row == rows_before && l.null_row_m.size() == nulls_before)
            l.morsels.pop_back();
        return SinkResult::CONTINUE;
    }

    void combine(GlobalSinkState& gs, LocalSinkState& ls, ErrCtx&) override {
        auto& g = static_cast<Join2BuildGlobal&>(gs);
        auto& l = static_cast<Join2BuildLocal&>(ls);
        std::lock_guard<std::mutex> lk(g.mtx);
        g.saw_null_key = g.saw_null_key || l.saw_null_key;
        // Concatenate this worker's retained views, then re-base its row addresses:
        // a local morsel index becomes a global one by adding the number of morsels
        // already present. No value copying and — the point of the whole exercise —
        // no string-arena rebasing, because no arena was ever built: the slots still
        // live in the source vectors, which we hold alive.
        const uint32_t morsel_off = static_cast<uint32_t>(g.morsels.size());
        const uint32_t row_off = g.total_rows;
        g.morsels.insert(g.morsels.end(),
                         std::make_move_iterator(l.morsels.begin()),
                         std::make_move_iterator(l.morsels.end()));
        // Queue this worker's row addresses — three O(1) moves, NOT an O(rows) copy
        // under the lock — and concatenate them once, in parallel, in finalize().
        // `morsel_off` is captured here because it is a property of WHEN this chunk
        // was queued; merge_build_rows() applies it. Chunks are queued in the same
        // order as hash_chunks below, which is what keeps global build row id r
        // meaning the same thing to both the CSR and the row addresses.
        g.row_m_chunks.push_back(std::move(l.row_m));
        g.row_r_chunks.push_back(std::move(l.row_r));
        g.chunk_morsel_off.push_back(morsel_off);
        g.asof_keys.insert(g.asof_keys.end(), l.asof_keys.begin(), l.asof_keys.end());
        g.asof_str_ptr.insert(g.asof_str_ptr.end(),
                              l.asof_str_ptr.begin(), l.asof_str_ptr.end());
        g.asof_str_len.insert(g.asof_str_len.end(),
                              l.asof_str_len.begin(), l.asof_str_len.end());
        g.asof_wide.insert(g.asof_wide.end(), l.asof_wide.begin(), l.asof_wide.end());
        // FULL OUTER: stage this worker's NULL-keyed rows, rebased to global
        // morsel indices. finalize() appends them after the keyed row space.
        for (size_t r = 0; r < l.null_row_m.size(); ++r) {
            g.null_row_m.push_back(morsel_off + l.null_row_m[r]);
            g.null_row_r.push_back(l.null_row_r[r]);
        }
        // Queue this worker's key hashes — an O(1) move, NOT O(rows) inserts under the
        // lock — and build the CSR once, in parallel, in finalize(). row_hashes[r] is
        // the key hash of local build row r → global row row_off+r, and chunks are
        // queued in the same order as row_m/row_r above, which is what lets finalize()
        // recover each chunk's global row base by a running sum.
        //
        // ASOF is the exception: it reads the build through CarcharJoinIndex::items()/
        // rows_from_payload() to materialize its per-key sorted view, so it keeps the
        // index and pays the serial insert. Porting ASOF onto the CSR is the remaining
        // work that would let CarcharJoinIndex leave this file entirely.
        if (asof_idx < 0) {
            g.csr_active = true;
            g.hash_chunks.push_back(std::move(l.row_hashes));
        } else {
            g.index.reserve(g.total_rows + l.next_row);
            for (uint32_t r = 0; r < l.next_row; ++r)
                g.index.insert_row(l.row_hashes[r], static_cast<int64_t>(row_off + r));
        }
        g.total_rows += l.next_row;
    }

    // Should the build payload be consolidated into one block so the probe can emit
    // its build half as a dict?
    //
    // The dense emit copies `dense_bpr` bytes per OUTPUT row. The dict emit copies
    // the payload ONCE (`block_bytes`) and then 4 bytes of code per output row, per
    // column. So consolidating pays exactly when
    //
    //     block_bytes + est*4*ncols  <  est*dense_bpr
    //
    // `block_bytes` and `dense_bpr` are MEASURED here from the retained vectors —
    // the sink is holding them, so there is nothing to estimate about the build
    // side. Only the output row count is a guess, and an unknown one refuses.
    //
    // A refusal costs nothing: the build side stays exactly as it is today (views,
    // no copy, no arena). A wrong ACCEPT costs one copy of the build payload and
    // keeps that block resident.
    //
    // ⚠️ kMargin is 2.0 and MUST NOT be relaxed to break-even. That was tried and
    // MEASURED: at 1.0 (plain byte break-even) TPC-H Q18 went from ~1.0s to 10.4s.
    //
    // The reason the byte model alone is not enough is a PARALLELISM asymmetry it
    // cannot see. The dense per-row copy happens inside the probe, spread across
    // every probe worker and every batch. Consolidation happens ONCE, in finalize(),
    // SINGLE-THREADED. So a job that merely moves fewer total bytes can still be far
    // slower in wall time, because it moved them all on one thread. Q18's largest
    // join consolidated 15M rows out of 1840 morsels (400MB) to buy a 28 -> 20
    // bytes/row saving: a 1.04x byte win, paid for with a serial 400MB gather.
    //
    // kMargin therefore has to cover the serial/parallel gap, not just estimate
    // error. 2.0 is the value at which the measured regressions disappear.
    static constexpr double kMargin = 2.0;

    // The second half of the same lesson: a payload has to be WIDE enough per row for
    // codes to be a real saving. Q18's killer had dense_bpr=28 against code_bpr=20 —
    // five fixed-width columns, where a 4-byte code replaces an 8-byte value and the
    // ratio is 1.4x. Consolidating can never repay a serial pass for that. A
    // string-carrying payload is 8-13x (Q10: 226 vs 28; cross join: 153 vs 12), which
    // is the shape this optimization is actually for.
    static constexpr double kMinPerRowRatio = 4.0;

    static bool should_consolidate(const Join2BuildGlobal& g, int64_t est_rows) {
        if (est_rows < 0) return false;            // unknown -> today's behaviour
        if (g.morsels.empty() || g.total_rows == 0) return false;
        // A single retained morsel is ALREADY one block; consolidating would copy it
        // for nothing. (The dict emit still cannot use it directly — build row ids
        // address it through row_m/row_r, not by position — so v1 simply declines.)
        if (g.morsels.size() == 1) return false;

        size_t block_bytes = 0;
        size_t ncols = 0;
        for (const MorselPtr& m : g.morsels) {
            ncols = m->columns.size();
            for (const CxxColumn& c : m->columns) {
                // ARRAY declines. A dict shares only `data` (the offsets), while an
                // ARRAY's values live in a CHILD VectorOwner reached through
                // `own->child_owner` — which a borrowing owner does not have, so the
                // emitted column would carry offsets into nothing. Sharing the child
                // subtree too is a bigger change than this one; until then an ARRAY
                // payload stays on the gather that already handles it.
                if (c.view.type == DRAKEN_ARRAY) return false;
                block_bytes += c.own ? draken_vector_owner_nbytes(c.own.get())
                                     : draken_vector_nbytes(&c.view);
            }
        }
        if (ncols == 0 || block_bytes == 0) return false;

        // Dense bytes per output row: what one row of every payload column costs when
        // materialized. Derived from the same measured block, so a dict-encoded build
        // column (which the dense gather EXPANDS) is not mistaken for a cheap one:
        // block_bytes/total_rows understates its per-row cost, making this test
        // conservative in the direction of refusing. Never the other way.
        const double dense_bpr =
            static_cast<double>(block_bytes) / static_cast<double>(g.total_rows);
        const double code_bpr = 4.0 * static_cast<double>(ncols);
        if (dense_bpr < kMinPerRowRatio * code_bpr) return false;  // payload too narrow

        const double est = static_cast<double>(est_rows);
        return static_cast<double>(block_bytes) + est * code_bpr
               < (est * dense_bpr) / kMargin;
    }

    void finalize(GlobalSinkState& gs, ErrCtx& err) override {
        auto& g = static_cast<Join2BuildGlobal&>(gs);
        // Unconditional, and FIRST: every path below addresses build rows through
        // row_m/row_r, and ASOF reads them too (its combine() populates `index`
        // instead of queuing hash chunks, but it queues row addresses like everyone
        // else). Nothing may touch the build address space before this returns.
        merge_build_rows(g);
        // No-op for ASOF, whose combine() populated `index` instead of queuing chunks.
        if (g.csr_active) build_join_csr(g);
        if (track_matches) {
            // The keyed row space [0, total_rows) is sealed (the CSR above was
            // built over it); NULL-keyed rows go on the END of row_m/row_r so
            // the tail source can gather every build row through one address
            // space, while staying invisible to the CSR (never matchable).
            g.tail_null_rows = static_cast<uint32_t>(g.null_row_m.size());
            g.row_m.insert(g.row_m.end(), g.null_row_m.begin(), g.null_row_m.end());
            g.row_r.insert(g.row_r.end(), g.null_row_r.begin(), g.null_row_r.end());
            g.null_row_m.clear();
            g.null_row_r.clear();
            const size_t flags = g.total_rows == 0 ? 1 : g.total_rows;
            g.matched = std::make_unique<std::atomic<uint8_t>[]>(flags);
            for (size_t i = 0; i < flags; ++i)
                g.matched[i].store(0, std::memory_order_relaxed);
        }

        // Consolidation runs LAST, after the block above has appended FULL OUTER's
        // NULL-keyed rows to row_m/row_r, so the consolidated morsel covers the whole
        // build address space — the unmatched tail gathers those rows too, and a
        // consolidated block that stopped at total_rows would leave them unreachable.
        if (!should_consolidate(g, est_output_rows)) return;
        const size_t addressable = g.row_m.size();
        std::vector<uint32_t> order(addressable);
        for (size_t i = 0; i < addressable; ++i) order[i] = static_cast<uint32_t>(i);
        // The engine's own row gather, over the entire build side in build-row-id
        // order. Deliberately NOT a hand-written per-type copy: this is the one
        // function that already handles every type a join can carry (strings with
        // their arena, ARRAY child subtrees, DECIMAL/TIMESTAMP descriptors), so
        // consolidation cannot develop a per-type gap the emit path does not have.
        MorselPtr block = gather_rows(g.morsels, order, 0, addressable, g.row_m,
                                      g.row_r, g.morsels.front()->names, err);
        if (err.code != 0 || block == nullptr) {
            // Consolidation is an OPTIMIZATION, and a failed one must not fail the
            // query: clear the error and leave `consolidated` null so the probe takes
            // the same gather it has always taken.
            err.code = 0;
            err.msg = nullptr;
            return;
        }
        g.consolidated = std::move(block);
    }
};

struct Join2Ref {
    const Join2BuildGlobal* g = nullptr;
    std::once_flag asof_sorted;   // ASOF probe: per-group rows sorted by asof key
};

// ---- INNER / LEFT_OUTER probe: fan-out matches, build payload first ----------------

struct Join2ProbeState : OperatorState {
    MorselPtr pending_in;
    uint32_t row = 0;
    std::vector<uint64_t> rowh;   // per-morsel probe-key hashes (draken-owned)
};


// Can ANY key column of this morsel carry a NULL? A DrakenVector with a null
// `validity` pointer is all-valid (draken invariant, see sort_row_valid), so when
// no key column has one, no probe row can have a NULL key and the per-row,
// per-column validity scan in the probe loops is dead work.
//
// Hoisted rather than turned into a packed per-row mask: a mask only helps when
// keys really are nullable, whereas this collapses the COMMON case (non-nullable
// join keys — every surrogate key in TPC-H and JOB) to a single check per morsel.
// The row loops keep their exact per-row logic for the nullable case.
inline bool probe_keys_nullable(const MorselPtr& in, const std::vector<size_t>& key_idx) {
    for (size_t k : key_idx)
        if (in->columns[k].view.validity != nullptr) return true;
    return false;
}

struct Join2ProbeOperator : Operator {
    std::vector<size_t> probe_key_idx;
    std::vector<size_t> probe_payload_idx;
    const Join2Ref* ref;
    bool left_outer;
    // FULL OUTER: mark matched build rows so the UnmatchedBuildSource tail can
    // emit the rest. Relaxed idempotent byte stores — probes stay const/parallel.
    bool track_matches = false;
    // Set-operation key rule — see Join2BuildSink::null_equal. Must agree with the
    // build sink's flag: one side excluding NULL keys while the other admits them
    // would silently drop every NULL row instead of matching it.
    bool null_equal = false;
    static constexpr size_t kBatch = 8192;
    static constexpr uint32_t kNoBuildRow = UINT32_MAX;

    Join2ProbeOperator(std::vector<size_t> keys, std::vector<size_t> payload,
                       const Join2Ref* r, bool outer, bool track = false,
                       bool null_eq = false)
        : probe_key_idx(std::move(keys)), probe_payload_idx(std::move(payload)),
          ref(r), left_outer(outer), track_matches(track), null_equal(null_eq) {}

    std::unique_ptr<OperatorState> make_state() override {
        return std::make_unique<Join2ProbeState>();
    }

    // Emit the build half as a DICT over the consolidated build payload — the whole
    // point of consolidating. Appends one column per consolidated column to `out`.
    //
    // Row i of `g.consolidated` IS build row i, so the physical index for output row
    // i is just build_rows[i] resolved through the source's own selection (read
    // uniformly — the consolidated column is dense, but nothing here depends on
    // that, per the data[selection[i]] contract).
    //
    // Two things are genuinely per-output-row and cannot be shared: the CODES, and
    // the VALIDITY. Validity is a per-LOGICAL-row mask (buffers.h), so a NULL in the
    // build side, or a LEFT OUTER row with no build match at all, has to be
    // reprojected onto the output's own bitmap. That is one bit per row against the
    // 16+ bytes the dense gather would have copied.
    static bool emit_build_dict(const Join2BuildGlobal& g,
                                const std::vector<uint32_t>& build_rows, uint32_t n,
                                CxxMorsel& out, ErrCtx& err) {
        const size_t vbytes = (static_cast<size_t>(n) + 7) / 8;
        for (const CxxColumn& src : g.consolidated->columns) {
            if (!src.own) {
                err.code = 1;
                err.msg = "Join2Probe: consolidated build column has no owner to "
                          "share its payload from — fail loud, never silent corruption";
                return false;
            }
            uint32_t* codes = static_cast<uint32_t*>(
                draken_malloc((n == 0 ? 1 : static_cast<size_t>(n)) * sizeof(uint32_t)));
            uint8_t* vbits = nullptr;
            auto mark_null = [&](uint32_t i) {
                if (vbits == nullptr) {
                    vbits = static_cast<uint8_t*>(draken_malloc(vbytes == 0 ? 1 : vbytes));
                    std::memset(vbits, 0xFF, vbytes == 0 ? 1 : vbytes);
                }
                vbits[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7)));
            };
            for (uint32_t i = 0; i < n; ++i) {
                const uint32_t br = build_rows[i];
                if (br == kNoBuildRow) {
                    // LEFT OUTER / ASOF unmatched probe row: no build row exists. Any
                    // in-range code will do — the validity bit is what makes it NULL.
                    codes[i] = 0;
                    mark_null(i);
                    continue;
                }
                if (br >= src.view.length) {
                    draken_free(codes);
                    if (vbits) draken_free(vbits);
                    err.code = 1;
                    err.msg = "Join2Probe: build row id outside the consolidated build "
                              "payload — fail loud, never silent corruption";
                    return false;
                }
                codes[i] = src.view.selection[br];
                if (!sort_row_valid(src.view, br)) mark_null(i);
            }
            DrakenVector v;
            v.data = src.view.data;              // BORROWED — see VectorOwner::data_source
            v.selection = codes;
            v.data_length = src.view.data_length;
            v.length = n;
            v.validity = vbits;
            v.type = src.view.type;
            v.flags = 0;                         // owned codes: neither identity nor a permutation
            CxxColumn c;
            c.own = std::make_shared<VectorOwner>(
                v, OwnedBuffer<void>(nullptr), OwnedBuffer<uint8_t>(vbits),
                OwnedBuffer<void>(codes));
            c.own->logical_type = src.own->logical_type;
            c.own->data_source = src.own;        // keeps the shared block alive
            c.view = c.own->vec;
            out.columns.push_back(std::move(c));
        }
        return true;
    }

    // Build one output morsel for the parallel (build_row | kNoBuildRow, probe_row)
    // arrays. Both are uint32: build row ids index row_m/row_r (uint32), probe row
    // ids are morsel-local, and kNoBuildRow is UINT32_MAX. They were int64, which
    // doubled the traffic through these buffers on every matched row and then had to
    // be narrowed again below to build the gather order — this removes a conversion
    // rather than adding one.
    MorselPtr build_output(const MorselPtr& probe_in,
                           const std::vector<uint32_t>& build_rows,
                           const std::vector<uint32_t>& probe_rows,
                           ErrCtx& err) {
        const Join2BuildGlobal& g = *ref->g;
        uint32_t n = static_cast<uint32_t>(build_rows.size());
        // The schema morsel always carries exactly one column per build payload
        // column, whether or not any row was ever retained.
        const size_t payload_col_count =
            g.schema_morsel ? g.schema_morsel->columns.size() : 0;
        auto out = std::make_shared<CxxMorsel>();
        out->zero_col_rows = n;
        out->columns.reserve(payload_col_count + probe_payload_idx.size());

        // Build payload: the engine's ONE row gather — the same call the probe half
        // makes below. The build side is not materialized into a row-store; its
        // columns were retained by the sink and are addressed by (morsel, row), which
        // is exactly what gather_rows consumes. Consequence: the set of types a join
        // can CARRY is now identical to the set gather_rows supports, for both halves.
        if (payload_col_count > 0) {
            // No retained morsel means the build side streamed zero rows; fall back to
            // the plan-typed zero-row schema so an all-NULL LEFT OUTER half still has
            // a column count and types to be built against.
            const bool empty_build = g.morsels.empty();
            std::vector<MorselPtr> schema_only;
            if (empty_build) schema_only.push_back(g.schema_morsel);
            const std::vector<MorselPtr>& bms = empty_build ? schema_only : g.morsels;
            if (empty_build) {
                // gather_rows recurses into an ARRAY column's child vector, so the
                // plan-typed schema column must carry one (make_empty_col builds it
                // from the plan's element type). When the planner could not resolve
                // that element type the column is childless — fail loud rather than
                // read an uninitialized child view.
                for (const CxxColumn& c : bms.front()->columns) {
                    if (array_child_missing(c)) {
                        err.code = 1;
                        err.msg = "Join2Probe: ARRAY build payload with an empty build "
                                  "side has no child vector to emit NULLs against — "
                                  "fail loud, never silent corruption";
                        return nullptr;
                    }
                }
            }
            // Consolidated build side: emit CODES over the one shared block instead
            // of copying a physical value per output row. Row i of `consolidated` IS
            // build row i (finalize gathered it in build-row-id order), so the code
            // for an output row is just its build row id — no rebasing, and no
            // per-type code here at all.
            if (!empty_build && g.consolidated != nullptr) {
                if (!emit_build_dict(g, build_rows, n, *out, err)) return nullptr;
            } else {
            std::vector<uint32_t> order(n);
            for (uint32_t i = 0; i < n; ++i) {
                // LEFT OUTER / ASOF unmatched probe row: no build row exists, so hand
                // gather_rows its null-row sentinel. A build row that EXISTS but holds
                // a NULL value needs nothing here — the source vector's own validity
                // already says so and the gather honours it.
                order[i] = (build_rows[i] == kNoBuildRow) ? kGatherNullRow : build_rows[i];
            }
            MorselPtr gathered = gather_rows(bms, order, 0, n, g.row_m, g.row_r,
                                             bms.front()->names, err);
            if (err.code != 0 || gathered == nullptr) return nullptr;
            for (CxxColumn& c : gathered->columns) out->columns.push_back(std::move(c));
            }
        }

        // Probe payload: the engine's one row gather (validity/strings/descriptors).
        if (!probe_payload_idx.empty()) {
            uint32_t pn = probe_in->num_rows();
            auto view = std::make_shared<CxxMorsel>();
            view->columns.reserve(probe_payload_idx.size());
            for (size_t pc : probe_payload_idx) view->columns.push_back(probe_in->columns[pc]);
            view->names.resize(view->columns.size());
            view->zero_col_rows = pn;
            // The probe half is ONE morsel, so its address map is trivial: every row
            // lives in morsel 0 at its own index. Rebuilding those two vectors per
            // 8192-row batch was allocating and filling 2*pn entries to express the
            // identity; thread_local scratch reuses them across batches (per-thread,
            // so concurrent probe workers still never share).
            static thread_local std::vector<uint32_t> row_m, row_r;
            if (row_r.size() < pn) {
                row_m.assign(pn, 0);
                row_r.resize(pn);
                for (uint32_t i = 0; i < pn; ++i) row_r[i] = i;
            }
            std::vector<MorselPtr> ms{view};
            MorselPtr gathered =
                gather_rows(ms, probe_rows, 0, n, row_m, row_r, view->names, err);
            if (err.code != 0 || gathered == nullptr) return nullptr;
            for (CxxColumn& c : gathered->columns) out->columns.push_back(std::move(c));
        }
        return out;
    }

    OpResult execute(const MorselPtr& in, OperatorState& st_, MorselPtr& out,
                     ErrCtx& err) override {
        // A morsel can legitimately carry 0 rows with an EMPTY columns vector (the
        // engine's zero-column-morsel convention for a row-count-only shape — see
        // e.g. GroupBySink::sink's identical guard). compute_row_hashes indexes
        // probe_key_idx into in->columns unconditionally, so skipping this BEFORE
        // that call (not just before the probe loop below) is required — every
        // other keyed sink/operator in this engine already guards n==0 first;
        // this one and AsofProbeOperator below were missing it.
        if (in->num_rows() == 0) return OpResult::NEED_INPUT;
        auto& st = static_cast<Join2ProbeState&>(st_);
        const Join2BuildGlobal& g = *ref->g;
        if (st.pending_in != in) {
            st.pending_in = in;
            st.row = 0;
            if (!compute_row_hashes(in, probe_key_idx, st.rowh, err))
                return OpResult::NEED_INPUT;
        }
        uint32_t n = in->num_rows();
        std::vector<uint32_t> build_rows, probe_rows;
        build_rows.reserve(kBatch);
        probe_rows.reserve(kBatch);
        // `null_equal` (set operations) makes NULL an ordinary matching value, so
        // there is no null row to special-case — the per-row checks below collapse
        // onto the same path a non-nullable key already takes.
        const bool keys_nullable =
            !null_equal && probe_keys_nullable(in, probe_key_idx);

        while (st.row < n) {
            uint32_t row = st.row;
            bool any_null = false;
            if (keys_nullable) {
                for (size_t k : probe_key_idx) {
                    if (!sort_row_valid(in->columns[k].view, row)) { any_null = true; break; }
                }
            }
            if (any_null) {
                if (left_outer) {   // unmatched preserved-side row → NULL build payload
                    build_rows.push_back(kNoBuildRow);
                    probe_rows.push_back(row);
                }
            } else {
                size_t before = build_rows.size();
                // const + thread-safe fan-out: appends (build_row, probe_row) for every
                // build row whose key hash matches. Equality is 64-bit hash identity.
                g.probe_append(st.rowh[row], row, build_rows, probe_rows);
                if (track_matches) {
                    std::atomic<uint8_t>* m = g.matched.get();
                    for (size_t bi = before; bi < build_rows.size(); ++bi)
                        m[static_cast<size_t>(build_rows[bi])].store(
                            1, std::memory_order_relaxed);
                }
                if (build_rows.size() == before && left_outer) {
                    build_rows.push_back(kNoBuildRow);
                    probe_rows.push_back(row);
                }
            }
            ++st.row;
            // Flush a batch after a full probe row (a single high-fan-out row may push
            // slightly past kBatch — correct, just a larger morsel).
            if (build_rows.size() >= kBatch) {
                out = build_output(in, build_rows, probe_rows, err);
                return (err.code != 0) ? OpResult::NEED_INPUT : OpResult::HAVE_MORE;
            }
        }
        if (!build_rows.empty()) {
            out = build_output(in, build_rows, probe_rows, err);
            return (err.code != 0) ? OpResult::NEED_INPUT : OpResult::EMIT;
        }
        return OpResult::NEED_INPUT;
    }
};

// ---- ASOF probe: nearest-match by ordered asof key within equi partitions ----------
// LEFT semantics (unmatched probe rows emit with NULL build payload — same emit as
// LEFT OUTER, inherited from Join2ProbeOperator::build_output). Each probe row
// yields EXACTLY ONE (build_row | kNoBuildRow) pair: within its equi-key group
// (sorted by asof key on first use), the match is selected by the MATCH_CONDITION
// operator, mirroring the legacy operator's bisect table:
//   GtEq (probe >= build): largest build key <= probe key   (upper_bound - 1)
//   Gt   (probe >  build): largest build key <  probe key   (lower_bound - 1)
//   LtEq (probe <= build): smallest build key >= probe key  (lower_bound)
//   Lt   (probe <  build): smallest build key >  probe key  (upper_bound)
// Ordering is AsofKey's: sort_num_key-normalized uint64 for numeric and temporal
// keys (unsigned `<` IS value order, IEEE-correct for floats), bytes-then-length for
// string-family ones. One comparator, `asof_key_cmp`, covers both.

enum class AsofOp : uint8_t { GtEq = 0, Gt = 1, LtEq = 2, Lt = 3 };

struct AsofProbeOperator : Join2ProbeOperator {
    size_t asof_probe_idx;
    AsofOp op;

    AsofProbeOperator(std::vector<size_t> keys, std::vector<size_t> payload,
                      const Join2Ref* r, size_t asof_idx, int op_code)
        : Join2ProbeOperator(std::move(keys), std::move(payload), r, /*outer=*/true),
          asof_probe_idx(asof_idx), op(static_cast<AsofOp>(op_code)) {}

    // Sort every equi group's row list by asof key, once, before the first probe.
    // Runs under the ref's once_flag: exactly one thread mutates; the rest wait.
    // Materialize, once, a hash → build-rows-sorted-by-asof-key view. CarcharJoinIndex
    // stores each key's rows unsorted; the bisect below needs them ordered.
    void ensure_sorted() {
        auto* mref = const_cast<Join2Ref*>(ref);
        std::call_once(mref->asof_sorted, [&]() {
            auto* g = const_cast<Join2BuildGlobal*>(ref->g);
            auto items = g->index.items();
            g->asof_sorted.reserve(items.size());
            g->asof_index.reserve(items.size());
            for (const auto& kv : items) {
                std::vector<int64_t> rows = g->index.rows_from_payload(kv.second);
                const AsofKeyKind kind = g->asof_kind;
                std::sort(rows.begin(), rows.end(), [&](int64_t a, int64_t b) {
                    return asof_key_cmp(kind, build_key(*g, a), build_key(*g, b)) < 0;
                });
                g->asof_index.insert_new(kv.first,
                                         static_cast<int64_t>(g->asof_sorted.size()));
                g->asof_sorted.push_back(std::move(rows));
            }
        });
    }

    // One build row's ASOF key, reassembled from the parallel build vectors.
    static AsofKey build_key(const Join2BuildGlobal& g, int64_t row) {
        AsofKey key;
        if (g.asof_kind == AsofKeyKind::String) {
            key.ptr = g.asof_str_ptr[static_cast<size_t>(row)];
            key.len = g.asof_str_len[static_cast<size_t>(row)];
        } else if (g.asof_kind == AsofKeyKind::Int128) {
            key.wide = g.asof_wide[static_cast<size_t>(row)];
        } else {
            key.num = g.asof_keys[static_cast<size_t>(row)];
        }
        return key;
    }

    int64_t match_row(const std::vector<int64_t>& rows, const AsofKey& k,
                      const Join2BuildGlobal& g) const {
        const int64_t none = static_cast<int64_t>(kNoBuildRow);
        const AsofKeyKind kind = g.asof_kind;
        auto cmp = [&](int64_t r, const AsofKey& v) {
            return asof_key_cmp(kind, build_key(g, r), v) < 0;
        };
        auto cmp2 = [&](const AsofKey& v, int64_t r) {
            return asof_key_cmp(kind, v, build_key(g, r)) < 0;
        };
        switch (op) {
            case AsofOp::GtEq: {   // largest build <= k
                auto it = std::upper_bound(rows.begin(), rows.end(), k, cmp2);
                return it == rows.begin() ? none : *(it - 1);
            }
            case AsofOp::Gt: {     // largest build < k
                auto it = std::lower_bound(rows.begin(), rows.end(), k, cmp);
                return it == rows.begin() ? none : *(it - 1);
            }
            case AsofOp::LtEq: {   // smallest build >= k
                auto it = std::lower_bound(rows.begin(), rows.end(), k, cmp);
                return it == rows.end() ? none : *it;
            }
            default: {             // Lt: smallest build > k
                auto it = std::upper_bound(rows.begin(), rows.end(), k, cmp2);
                return it == rows.end() ? none : *it;
            }
        }
    }

    OpResult execute(const MorselPtr& in, OperatorState& st_, MorselPtr& out,
                     ErrCtx& err) override {
        // See the identical guard in Join2ProbeOperator::execute above — a 0-row
        // morsel here can carry an EMPTY columns vector, and both compute_row_hashes
        // and the in->columns[asof_probe_idx] read below assume a real column exists.
        if (in->num_rows() == 0) return OpResult::NEED_INPUT;
        ensure_sorted();
        auto& st = static_cast<Join2ProbeState&>(st_);
        const Join2BuildGlobal& g = *ref->g;
        if (st.pending_in != in) {
            st.pending_in = in;
            st.row = 0;
            if (!compute_row_hashes(in, probe_key_idx, st.rowh, err))
                return OpResult::NEED_INPUT;
        }
        uint32_t n = in->num_rows();
        std::vector<uint32_t> build_rows, probe_rows;
        build_rows.reserve(kBatch);
        probe_rows.reserve(kBatch);
        const DrakenVector& av = in->columns[asof_probe_idx].view;

        while (st.row < n) {
            uint32_t row = st.row;
            uint32_t build_row = kNoBuildRow;
            bool usable = sort_row_valid(av, row);
            for (size_t k : probe_key_idx) {
                if (!usable) break;
                if (!sort_row_valid(in->columns[k].view, row)) usable = false;
            }
            if (usable) {
                int64_t idx = -1;
                if (g.asof_index.lookup_fast(st.rowh[row], idx))
                    // match_row still speaks int64 — it bisects asof_sorted, whose
                    // row lists come from CarcharJoinIndex. Its "no match" is
                    // kNoBuildRow, so the narrowing round-trips exactly.
                    build_row = static_cast<uint32_t>(
                        match_row(g.asof_sorted[static_cast<size_t>(idx)],
                                  asof_key_of(av, row, g.asof_kind), g));
            }
            build_rows.push_back(build_row);
            probe_rows.push_back(row);
            ++st.row;
            if (build_rows.size() >= kBatch) {
                out = build_output(in, build_rows, probe_rows, err);
                return (err.code != 0) ? OpResult::NEED_INPUT : OpResult::HAVE_MORE;
            }
        }
        if (!build_rows.empty()) {
            out = build_output(in, build_rows, probe_rows, err);
            return (err.code != 0) ? OpResult::NEED_INPUT : OpResult::EMIT;
        }
        return OpResult::NEED_INPUT;
    }
};

// ---- SEMI / ANTI probe: existence filter over the probe stream ---------------------

struct SemiAntiProbeState : OperatorState {};   // whole-morsel filter; no resume state

// Derives from Join2ProbeOperator purely to reuse `build_output` — the (build
// payload | probe payload) pair gather — for the residual path below. The emit is
// this class's own: probe rows only, never a joined row.
struct SemiAntiProbeOperator : Join2ProbeOperator, EmitSubset {
    bool anti;         // false = SEMI, true = ANTI (either flavour)
    bool null_aware;   // ANTI only: true = NOT IN's UNKNOWN rules, false = plain anti

    // Optional CORRELATED NON-EQUALITY residual — canonical TPC-H Q21's
    // `EXISTS (... WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey)`.
    // The equality becomes the join key as usual; this is everything else, and it
    // references BOTH sides. It must gate the EXISTENCE test, not filter the output:
    // this operator emits probe rows already collapsed to "has >=1 match", so a
    // post-join filter (what nested_loop does) would ask the question in the wrong
    // order. A probe row matches only if at least one KEY-matching build row also
    // satisfies this predicate.
    // Evaluated over the pair morsel in bounded batches: one vectorized call per
    // batch, never per pair (§3 — batch-oriented, not row-oriented).
    ExprProgram residual;
    ExprEvalFn residual_fn = nullptr;   // null = no residual; the cheap path below

    // What this operator EMITS, which is not what it READS. A SEMI/ANTI join is an
    // existence FILTER: it emits surviving probe rows unchanged, so its probe key —
    // read here on every row — is usually wanted by nothing above it. `emit_prune`
    // + `emit_cols` (EmitSubset, from native_sort.hpp) narrow the survivor gather
    // below; unset means emit every probe column, exactly as before.
    //
    // Distinct from `probe_payload_idx`, which this operator inherits and which
    // means something ELSE: the columns build_output puts in the PAIR morsel a
    // correlated residual reads. The residual needs the full pair layout even when
    // the output can be narrow, so the two must not be conflated into one field.
    // EXISTENCE-FLAG mode: emit EVERY probe row plus one BOOL column holding the
    // verdict, instead of emitting the rows the verdict kept. The verdict itself is
    // unchanged — this operator already computes exactly the boolean a projected
    // `EXISTS` needs, residual path included; only the emit differs. That is why it
    // is a flag here rather than a second operator.
    //
    // NOT to be confused with Join2MarkSink below, which marks BUILD rows for the
    // swapped RIGHT SEMI/ANTI path and emits nothing at all.
    bool emit_existence = false;
    // Is the emitted flag THREE-valued? `EXISTS` is not — an outer row either has a
    // matching inner row or it does not — but a projected `IN` is: `x IN (SELECT y)`
    // is UNKNOWN when x is NULL, and when x matched nothing while some y was NULL.
    // Only ever set for the single-key uncorrelated IN/NOT IN shape; a correlated IN
    // in the SELECT list is refused in the planner rather than guessed at here.
    bool existence_three_valued = false;
    // Output identity of the flag column, appended after the emitted probe columns.
    std::string existence_name;

    SemiAntiProbeOperator(std::vector<size_t> keys, std::vector<size_t> payload,
                          const Join2Ref* r, bool anti_, bool null_aware_,
                          ExprProgram res, ExprEvalFn res_fn,
                          bool emit_prune_ = false,
                          std::vector<uint32_t> emit_cols_ = {},
                          bool null_eq = false,
                          bool emit_existence_ = false,
                          bool existence_three_valued_ = false,
                          std::string existence_name_ = {})
        : Join2ProbeOperator(std::move(keys), std::move(payload), r, /*outer=*/false,
                             /*track=*/false, null_eq),
          anti(anti_), null_aware(null_aware_),
          residual(std::move(res)), residual_fn(res_fn),
          emit_existence(emit_existence_),
          existence_three_valued(existence_three_valued_),
          existence_name(std::move(existence_name_)) {
        emit_prune = emit_prune_;
        emit_cols = std::move(emit_cols_);
    }

    std::unique_ptr<OperatorState> make_state() override {
        return std::make_unique<SemiAntiProbeState>();
    }

    // Evaluate the residual over one batch of candidate (build_row, probe_row) pairs
    // and mark every probe row that has >=1 satisfying build row. Clears the batch.
    // Returns false with `err` set on failure.
    bool resolve_pairs(const MorselPtr& in, std::vector<uint32_t>& build_rows,
                       std::vector<uint32_t>& probe_rows, std::vector<uint8_t>& found,
                       ErrCtx& err) {
        if (build_rows.empty()) return true;
        MorselPtr pairs = build_output(in, build_rows, probe_rows, err);
        if (err.code != 0 || pairs == nullptr) return false;

        DrakenVector v;
        void* data = nullptr;
        uint8_t* validity = nullptr;
        void* sel = nullptr;
        int err_op = 0;
        const char* kernel_msg = nullptr;
        VecResult* child = nullptr;
        int rc = residual_fn(residual.instrs, residual.count, pairs.get(),
                             residual.col_idx.data(), residual.lit_dv.data(),
                             &v, &data, &validity, &sel, &err_op, &kernel_msg, &child);
        if (rc != 0) {
            err.code = 1;
            err.msg = format_kernel_error(
                "SemiAntiProbe: correlated residual evaluation failed", err_op, kernel_msg);
            return false;
        }
        // Take ownership of the span's buffers so they are released on every path out.
        VectorOwner owner(v, OwnedBuffer<void>(data), OwnedBuffer<uint8_t>(validity),
                          OwnedBuffer<void>(sel));
        if (child != nullptr) { delete child; }   // a BOOL residual has no ARRAY child
        if (v.type != DRAKEN_BOOL) {
            err.code = 1;
            err.msg = "SemiAntiProbe: correlated residual did not evaluate to BOOL — "
                      "fail loud rather than guess at the existence test";
            return false;
        }
        // BOOL data is BIT-PACKED and read through the uniform selection path
        // (data[selection[i]]); validity is 1 bit per LOGICAL row (CLAUDE.md §11).
        const uint8_t* bits = static_cast<const uint8_t*>(v.data);
        const uint32_t* codes = v.selection;   // never NULL (draken invariant)
        uint32_t np = static_cast<uint32_t>(build_rows.size());
        for (uint32_t k = 0; k < np; ++k) {
            // A NULL residual is UNKNOWN, and UNKNOWN is not TRUE — the pair does
            // not satisfy EXISTS.
            if (!sort_row_valid(v, k)) continue;
            uint32_t phys = codes[k];
            if ((bits[phys >> 3] >> (phys & 7)) & 1u)
                found[static_cast<size_t>(probe_rows[k])] = 1;
        }
        build_rows.clear();
        probe_rows.clear();
        return true;
    }
    // EXISTENCE-FLAG emit: every probe row, plus one BOOL column carrying the same
    // verdict the filter path would have applied. `anti` inverts the bit rather than
    // inverting which rows survive, so NOT EXISTS shares this path exactly.
    //
    // The probe columns are SHARED, not gathered: every input row is emitted, in
    // order, so the output column IS the input column (CxxColumn copies the 40-byte
    // view and shares the owner — see cxx_morsel.h). Only the flag is allocated.
    OpResult emit_flag(const MorselPtr& in, const Join2BuildGlobal& g,
                       const std::vector<uint64_t>& rowh,
                       const std::vector<uint8_t>& matched, bool keys_nullable,
                       bool build_empty, uint32_t n, MorselPtr& out, ErrCtx& err) {
        const size_t bytes = (static_cast<size_t>(n) + 7) / 8;
        uint8_t* bits = static_cast<uint8_t*>(draken_malloc(bytes == 0 ? 1 : bytes));
        uint32_t* sel = static_cast<uint32_t*>(
            draken_malloc((n == 0 ? 1 : static_cast<size_t>(n)) * sizeof(uint32_t)));
        if (bits == nullptr || sel == nullptr) {
            if (bits) draken_free(bits);
            if (sel) draken_free(sel);
            err.code = 1;
            err.msg = "SemiAntiProbe: out of memory allocating the existence flag";
            return OpResult::NEED_INPUT;
        }
        std::memset(bits, 0, bytes == 0 ? 1 : bytes);
        uint8_t* vbits = nullptr;   // stays NULL while every row is valid

        for (uint32_t i = 0; i < n; ++i) {
            sel[i] = i;
            bool any_null = false;
            if (keys_nullable) {
                for (size_t k : probe_key_idx) {
                    if (!sort_row_valid(in->columns[k].view, i)) { any_null = true; break; }
                }
            }
            // Same existence verdict as the filter path: a NULL probe key never
            // equi-matches, so it simply finds nothing.
            const bool found = any_null
                                   ? false
                                   : (residual_fn != nullptr
                                          ? (matched[i] != 0)
                                          : (!build_empty && g.probe_row_count(rowh[i]) > 0));
            // UNKNOWN, for a projected IN/NOT IN only. An EMPTY build is never
            // unknown: `x IN ()` is FALSE and `x NOT IN ()` is TRUE even for a NULL
            // x, so the rules below are gated on there being something to compare to.
            if (existence_three_valued && !build_empty && (any_null || (!found && g.saw_null_key))) {
                if (vbits == nullptr) {
                    vbits = static_cast<uint8_t*>(draken_malloc(bytes == 0 ? 1 : bytes));
                    if (vbits == nullptr) {
                        draken_free(bits);
                        draken_free(sel);
                        err.code = 1;
                        err.msg = "SemiAntiProbe: out of memory allocating the "
                                  "existence flag validity mask";
                        return OpResult::NEED_INPUT;
                    }
                    std::memset(vbits, 0xFF, bytes == 0 ? 1 : bytes);
                }
                vbits[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7)));
                continue;   // data bit already 0; validity is what makes it NULL
            }
            // `anti` inverts the ANSWER here, where the filter path inverts which
            // rows it keeps. NOT is NULL-preserving, so an UNKNOWN row took the
            // branch above and never reaches this.
            if (found != anti) bits[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }

        DrakenVector v;
        v.data = bits;
        v.selection = sel;
        v.data_length = n;
        v.length = n;
        v.validity = vbits;
        v.type = DRAKEN_BOOL;
        v.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
        CxxColumn flag;
        flag.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(bits),
                                                 OwnedBuffer<uint8_t>(vbits),
                                                 OwnedBuffer<void>(sel));
        flag.view = flag.own->vec;

        auto morsel = std::make_shared<CxxMorsel>();
        morsel->zero_col_rows = n;
        const std::vector<uint32_t>* keep = emit_ptr();
        if (keep == nullptr) {
            morsel->columns = in->columns;   // CxxColumn is a view + shared owner
            morsel->names = in->names;
        } else {
            morsel->columns.reserve(keep->size() + 1);
            morsel->names.reserve(keep->size() + 1);
            for (uint32_t c : *keep) {
                if (c >= in->columns.size()) {
                    err.code = 1;
                    err.msg = "SemiAntiProbe: existence emit column index outside the "
                              "probe morsel — fail loud, never silent corruption";
                    return OpResult::NEED_INPUT;
                }
                morsel->columns.push_back(in->columns[c]);
                morsel->names.push_back(in->names[c]);
            }
        }
        morsel->columns.push_back(std::move(flag));
        morsel->names.push_back(existence_name);
        out = std::move(morsel);
        return OpResult::EMIT;
    }

    OpResult execute(const MorselPtr& in, OperatorState& st_, MorselPtr& out,
                     ErrCtx& err) override {
        (void)st_;
        const Join2BuildGlobal& g = *ref->g;
        uint32_t n = in->num_rows();
        if (n == 0) return OpResult::NEED_INPUT;

        // NOT IN only: a NULL anywhere in the build makes every comparison UNKNOWN.
        // Plain ANTI (NOT EXISTS) must NOT do this — a NULL build key is just a row
        // that matches nothing.
        //
        // Dropping the whole morsel is how a FILTER expresses UNKNOWN. An existence
        // FLAG expresses it as a NULL in the flag column and must still emit every
        // row, so this early-out is a wrong answer there — `existence_three_valued`
        // reproduces the same rule per row below.
        if (anti && null_aware && g.saw_null_key && !emit_existence)
            return OpResult::NEED_INPUT;
        bool build_empty = (g.total_rows == 0 && !g.saw_null_key);

        std::vector<uint64_t> rowh;
        if (!build_empty && !compute_row_hashes(in, probe_key_idx, rowh, err))
            return OpResult::NEED_INPUT;
        // `null_equal` (set operations) makes NULL an ordinary matching value, so
        // there is no null row to special-case — the per-row checks below collapse
        // onto the same path a non-nullable key already takes.
        const bool keys_nullable =
            !null_equal && probe_keys_nullable(in, probe_key_idx);

        // With a residual, existence has to be decided per candidate pair, so the
        // key-matching pairs are materialized and the predicate evaluated over them
        // in batches. Without one, existence stays the cheap hash-identity count and
        // NO pairs are built — the common case pays nothing for this.
        std::vector<uint8_t> matched;
        if (residual_fn != nullptr) {
            matched.assign(n, 0);
            if (!build_empty) {
                std::vector<uint32_t> build_rows, probe_rows;
                build_rows.reserve(kBatch);
                probe_rows.reserve(kBatch);
                for (uint32_t i = 0; i < n; ++i) {
                    bool any_null = false;
                    if (keys_nullable) {
                        for (size_t k : probe_key_idx) {
                            if (!sort_row_valid(in->columns[k].view, i)) {
                                any_null = true;
                                break;
                            }
                        }
                    }
                    if (any_null) continue;   // NULL key never equi-matches
                    g.probe_append(rowh[i], i, build_rows, probe_rows);
                    // Bound the pair batch: fan-out is probe_rows x rows-per-key, which
                    // is unbounded in general. Flushing after a COMPLETE probe row keeps
                    // every row's pairs contiguous (a single high-fan-out row may push
                    // past kBatch — correct, just a larger batch).
                    if (build_rows.size() >= kBatch
                        && !resolve_pairs(in, build_rows, probe_rows, matched, err))
                        return OpResult::NEED_INPUT;
                }
                if (!resolve_pairs(in, build_rows, probe_rows, matched, err))
                    return OpResult::NEED_INPUT;
            }
        }

        if (emit_existence) return emit_flag(in, g, rowh, matched, keys_nullable,
                                             build_empty, n, out, err);

        std::vector<uint32_t> survivors;
        survivors.reserve(n);
        for (uint32_t i = 0; i < n; ++i) {
            bool any_null = false;
            if (keys_nullable) {
                for (size_t k : probe_key_idx) {
                    if (!sort_row_valid(in->columns[k].view, i)) { any_null = true; break; }
                }
            }
            if (any_null) {
                // NULL probe key never equi-matches, so SEMI drops it.
                // Plain ANTI: no match → NOT EXISTS is TRUE → the row passes.
                // NULL-aware ANTI: NULL NOT IN <non-empty> is UNKNOWN → drop; but
                // NOT IN an EMPTY set is TRUE.
                if (anti && (!null_aware || build_empty)) survivors.push_back(i);
                continue;
            }
            // Existence: per-pair residual verdict when there is one, else 64-bit
            // hash identity (const, thread-safe probe).
            bool found = residual_fn != nullptr
                             ? (matched[i] != 0)
                             : (!build_empty && g.probe_row_count(rowh[i]) > 0);
            if (found != anti) survivors.push_back(i);
        }
        if (survivors.empty()) return OpResult::NEED_INPUT;
        std::vector<MorselPtr> ms{in};
        std::vector<uint32_t> row_m(n, 0), row_r(n);
        for (uint32_t i = 0; i < n; ++i) row_r[i] = i;
        // The probe key's purpose is spent the moment `survivors` exists — every
        // existence verdict above is already decided — so it is gathered only if
        // something above this join actually reads it (see EmitSubset above).
        out = gather_rows(ms, survivors, 0, survivors.size(), row_m, row_r, in->names,
                          err, emit_ptr());
        return (err.code != 0 || out == nullptr) ? OpResult::NEED_INPUT : OpResult::EMIT;
    }
};

// ---- FULL OUTER tail: emit unmatched build rows, NULL-padded probe half ------------
// Runs as the SOURCE of its own pipeline, created AFTER the probe pipeline (pipelines
// execute in creation order), so by the time it pulls, every probe worker has finished
// and g.matched is complete. Output column order is identical to
// Join2ProbeOperator::build_output — build payload first, then probe payload — and both
// legs append into one shared buffer (the engine's UNION plumbing).
struct UnmatchedBuildSourceGlobal : GlobalSourceState {
    std::atomic<uint32_t> next{0};
};

struct UnmatchedBuildSource : Source {
    const Join2Ref* ref;
    // Zero-row, plan-typed PROBE payload columns — what the all-NULL probe half is
    // gathered against (the exact mirror of Join2BuildGlobal::schema_morsel).
    MorselPtr probe_schema;
    static constexpr uint32_t kChunk = 65536;

    UnmatchedBuildSource(const Join2Ref* r, MorselPtr schema)
        : ref(r), probe_schema(std::move(schema)) {}

    std::unique_ptr<GlobalSourceState> make_global() override {
        return std::make_unique<UnmatchedBuildSourceGlobal>();
    }
    std::unique_ptr<LocalSourceState> make_local(GlobalSourceState&) override {
        return std::make_unique<LocalSourceState>();
    }

    SourceResult get_morsel(GlobalSourceState& gs_, LocalSourceState&,
                            MorselPtr& out, ErrCtx& err) override {
        auto& gsrc = static_cast<UnmatchedBuildSourceGlobal&>(gs_);
        const Join2BuildGlobal& g = *ref->g;
        const uint32_t keyed = g.total_rows;
        const uint32_t domain = keyed + g.tail_null_rows;
        for (;;) {
            const uint32_t start = gsrc.next.fetch_add(kChunk);
            if (start >= domain) return SourceResult::FINISHED;
            const uint32_t end = std::min(domain, start + kChunk);

            // Rows [0, keyed) are CSR-visible: emit iff no probe matched them.
            // Rows [keyed, domain) are the NULL-keyed build rows finalize()
            // appended — never matchable, always emitted.
            std::vector<uint32_t> order;
            order.reserve(end - start);
            const std::atomic<uint8_t>* matched = g.matched.get();
            for (uint32_t r = start; r < end; ++r) {
                if (r >= keyed || matched[r].load(std::memory_order_relaxed) == 0)
                    order.push_back(r);
            }
            if (order.empty()) continue;   // fully-matched chunk — claim the next
            const uint32_t n = static_cast<uint32_t>(order.size());

            auto morsel = std::make_shared<CxxMorsel>();
            morsel->zero_col_rows = n;

            // Build payload: real rows, the engine's one row gather — or codes over
            // the consolidated block when the probe half is already emitting those.
            // On its own the tail is a SUBSET emit (fanout < 1), the one shape where
            // a dict costs more memory than it saves; but when `consolidated` exists
            // the probe's own outputs are already holding that block alive, so the
            // tail's marginal memory cost here is zero and it saves the copy. If the
            // block is absent the tail takes the gather exactly as before.
            if (g.consolidated != nullptr) {
                if (!Join2ProbeOperator::emit_build_dict(g, order, n, *morsel, err))
                    return SourceResult::FINISHED;
            } else if (!g.morsels.empty()) {
                MorselPtr bhalf = gather_rows(g.morsels, order, 0, n, g.row_m, g.row_r,
                                              g.morsels.front()->names, err);
                if (err.code != 0 || bhalf == nullptr) return SourceResult::FINISHED;
                for (CxxColumn& c : bhalf->columns)
                    morsel->columns.push_back(std::move(c));
            }

            // Probe payload: every row is the null-row sentinel against the
            // plan-typed zero-row schema — the same emit LEFT OUTER uses for its
            // build half, mirrored.
            if (probe_schema && !probe_schema->columns.empty()) {
                for (const CxxColumn& c : probe_schema->columns) {
                    if (array_child_missing(c)) {
                        err.code = 1;
                        err.msg = "FULL OUTER: ARRAY probe payload has no child "
                                  "vector to emit NULLs against — fail loud, "
                                  "never silent corruption";
                        return SourceResult::FINISHED;
                    }
                }
                std::vector<uint32_t> norder(n, kGatherNullRow);
                std::vector<MorselPtr> pm{probe_schema};
                std::vector<uint32_t> prow_m(1, 0), prow_r(1, 0);
                MorselPtr phalf = gather_rows(pm, norder, 0, n, prow_m, prow_r,
                                              probe_schema->names, err);
                if (err.code != 0 || phalf == nullptr) return SourceResult::FINISHED;
                for (CxxColumn& c : phalf->columns)
                    morsel->columns.push_back(std::move(c));
            }

            out = std::move(morsel);
            return SourceResult::HAVE_MORE;
        }
    }
};

// ---- RIGHT SEMI / RIGHT ANTI: build the SMALL side, stream the large one -----------
//
// The SAME logical answer as SemiAntiProbeOperator, with the two legs exchanged. A
// LEFT SEMI/ANTI emits rows of its LEFT leg, and compiler.py's `_compile_join` pins
// that leg to the PROBE ("the LEFT leg is the preserved/filtered side — it must be
// the PROBE; the RIGHT leg builds the table"). That pin is a correctness rule about
// which rows are EMITTED, but it also decides which side is MATERIALISED, and the two
// questions are independent: TPC-H Q21 at SF100 probes 7,313,671 rows against a
// 600,037,902-row build side, so the hash table is 82x the stream it serves.
//
// Exchanging them keeps the emitted rows identical and moves the materialisation onto
// the smaller leg:
//
//   build pipeline   left leg  -> Join2BuildSink(track_matches=true)
//   stream pipeline  right leg -> Join2MarkSink          (emits NOTHING)
//   emit pipeline    SemiAntiBuildSource(emit_matched)   (emits the build rows)
//
// Two consequences the planner — not this file — must own, because they are the price
// of the exchange and cannot be detected here:
//
//   * It is BLOCKING. Nothing is emitted until the streamed leg is exhausted, where
//     the LEFT form emits each surviving probe row as it is found. A LIMIT that could
//     have short-circuited the probe cannot short-circuit this.
//   * Output arrives in BUILD order, not probe order.
//
// NOT valid for AntiNullAware (NOT IN) or the NotDistinct set-operation modes: both
// derive their answer from a property of the BUILD side ("did it contain a NULL key",
// Join2BuildSink::null_equal), and exchanging the legs changes which relation that
// property is read from. Transposing them without re-deriving the rule would be the
// silent-wrong-answer class this file's header warns about. The planner admits only
// plain Semi and plain Anti.

// Emits the build rows whose match flag has the requested polarity: SEMI emits the
// MATCHED rows, ANTI the unmatched. Deliberately NOT a flag on UnmatchedBuildSource:
// that source exists to complete a FULL OUTER row and pads a NULL probe half onto
// every row it emits, which an existence filter must never do — it emits its own leg
// unchanged. What the two share is the build-half gather, and that is `gather_rows`,
// which both call directly.
struct SemiAntiBuildSourceGlobal : GlobalSourceState {
    std::atomic<uint32_t> next{0};
};

struct SemiAntiBuildSource : Source {
    const Join2Ref* ref;
    bool emit_matched;              // true = SEMI (emit matched), false = ANTI
    static constexpr uint32_t kChunk = 65536;

    SemiAntiBuildSource(const Join2Ref* r, bool matched) : ref(r), emit_matched(matched) {}

    std::unique_ptr<GlobalSourceState> make_global() override {
        return std::make_unique<SemiAntiBuildSourceGlobal>();
    }
    std::unique_ptr<LocalSourceState> make_local(GlobalSourceState&) override {
        return std::make_unique<LocalSourceState>();
    }

    SourceResult get_morsel(GlobalSourceState& gs_, LocalSourceState&,
                            MorselPtr& out, ErrCtx& err) override {
        auto& gsrc = static_cast<SemiAntiBuildSourceGlobal&>(gs_);
        const Join2BuildGlobal& g = *ref->g;
        const uint32_t keyed = g.total_rows;
        // Rows [keyed, keyed + tail_null_rows) are the NULL-keyed build rows finalize()
        // appended after the CSR was sealed. They are invisible to the CSR, so nothing
        // can ever have marked them: they are unmatched by construction. ANTI must
        // emit them (a NULL key equi-matches nothing, so NOT EXISTS holds); SEMI must
        // not. That is the same rule SemiAntiProbeOperator applies to a NULL PROBE key,
        // read from the side the exchange put them on.
        const uint32_t domain = emit_matched ? keyed : keyed + g.tail_null_rows;
        for (;;) {
            const uint32_t start = gsrc.next.fetch_add(kChunk);
            if (start >= domain) return SourceResult::FINISHED;
            const uint32_t end = std::min(domain, start + kChunk);

            std::vector<uint32_t> order;
            order.reserve(end - start);
            const std::atomic<uint8_t>* matched = g.matched.get();
            for (uint32_t r = start; r < end; ++r) {
                const bool hit = r < keyed
                                 && matched[r].load(std::memory_order_relaxed) != 0;
                if (hit == emit_matched) order.push_back(r);
            }
            if (order.empty()) continue;   // nothing of this polarity here — next chunk
            const uint32_t n = static_cast<uint32_t>(order.size());

            auto morsel = std::make_shared<CxxMorsel>();
            morsel->zero_col_rows = n;
            if (g.morsels.empty()) {   // build side streamed zero rows
                out = std::move(morsel);
                return SourceResult::HAVE_MORE;
            }
            MorselPtr half = gather_rows(g.morsels, order, 0, n, g.row_m, g.row_r,
                                         g.morsels.front()->names, err);
            if (err.code != 0 || half == nullptr) return SourceResult::FINISHED;
            for (CxxColumn& c : half->columns) morsel->columns.push_back(std::move(c));

            out = std::move(morsel);
            return SourceResult::HAVE_MORE;
        }
    }
};

// Consumes the streamed leg and marks the build rows it hits. Emits nothing — the
// pipeline's whole product is the mutation of g.matched, which SemiAntiBuildSource
// then reads. A Sink rather than an Operator because that is what this is: a pipeline
// terminator with no output, whose parallelism is already sound (`matched` is atomic
// and every store is an idempotent 0->1, so workers need no coordination and combine()
// has nothing to merge).
//
// `pair` is a Join2ProbeOperator held by COMPOSITION, purely to reuse `build_output`
// for the residual path. Building the (build|probe) pair morsel is not trivial — it is
// the gather that decides which types a join can carry — and a second copy of it here
// would be the duplication §3 forbids.
struct Join2MarkSink : Sink {
    Join2ProbeOperator pair;
    ExprProgram residual;
    ExprEvalFn residual_fn = nullptr;

    Join2MarkSink(std::vector<size_t> keys, std::vector<size_t> payload,
                  const Join2Ref* r, ExprProgram res, ExprEvalFn res_fn)
        : pair(std::move(keys), std::move(payload), r, /*outer=*/false,
               /*track=*/false, /*null_eq=*/false),
          residual(std::move(res)), residual_fn(res_fn) {}

    std::unique_ptr<GlobalSinkState> make_global() override {
        return std::make_unique<GlobalSinkState>();
    }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override {
        return std::make_unique<LocalSinkState>();
    }

    // Mark every build row in the batch that the residual accepts, then clear it.
    // The mirror of SemiAntiProbeOperator::resolve_pairs, indexed by BUILD row
    // instead of probe row — that index change IS the leg exchange.
    bool resolve_pairs(const MorselPtr& in, std::vector<uint32_t>& build_rows,
                       std::vector<uint32_t>& probe_rows, ErrCtx& err) {
        if (build_rows.empty()) return true;
        MorselPtr pairs = pair.build_output(in, build_rows, probe_rows, err);
        if (err.code != 0 || pairs == nullptr) return false;

        DrakenVector v;
        void* data = nullptr;
        uint8_t* validity = nullptr;
        void* sel = nullptr;
        int err_op = 0;
        const char* kernel_msg = nullptr;
        VecResult* child = nullptr;
        int rc = residual_fn(residual.instrs, residual.count, pairs.get(),
                             residual.col_idx.data(), residual.lit_dv.data(),
                             &v, &data, &validity, &sel, &err_op, &kernel_msg, &child);
        if (rc != 0) {
            err.code = 1;
            err.msg = format_kernel_error(
                "Join2MarkSink: correlated residual evaluation failed", err_op, kernel_msg);
            return false;
        }
        VectorOwner owner(v, OwnedBuffer<void>(data), OwnedBuffer<uint8_t>(validity),
                          OwnedBuffer<void>(sel));
        if (child != nullptr) { delete child; }
        if (v.type != DRAKEN_BOOL) {
            err.code = 1;
            err.msg = "Join2MarkSink: correlated residual did not evaluate to BOOL — "
                      "fail loud rather than guess at the existence test";
            return false;
        }
        const uint8_t* bits = static_cast<const uint8_t*>(v.data);
        const uint32_t* codes = v.selection;
        std::atomic<uint8_t>* marks = pair.ref->g->matched.get();
        uint32_t np = static_cast<uint32_t>(build_rows.size());
        for (uint32_t k = 0; k < np; ++k) {
            if (!sort_row_valid(v, k)) continue;   // UNKNOWN is not TRUE
            uint32_t phys = codes[k];
            if ((bits[phys >> 3] >> (phys & 7)) & 1u)
                marks[static_cast<size_t>(build_rows[k])].store(
                    1, std::memory_order_relaxed);
        }
        build_rows.clear();
        probe_rows.clear();
        return true;
    }

    SinkResult sink(const MorselPtr& in, GlobalSinkState&, LocalSinkState&,
                    ErrCtx& err) override {
        const Join2BuildGlobal& g = *pair.ref->g;
        uint32_t n = in->num_rows();
        if (n == 0 || g.total_rows == 0) return SinkResult::CONTINUE;

        std::vector<uint64_t> rowh;
        if (!compute_row_hashes(in, pair.probe_key_idx, rowh, err))
            return SinkResult::CONTINUE;
        const bool keys_nullable = probe_keys_nullable(in, pair.probe_key_idx);

        std::vector<uint32_t> build_rows, probe_rows;
        build_rows.reserve(Join2ProbeOperator::kBatch);
        probe_rows.reserve(Join2ProbeOperator::kBatch);
        std::atomic<uint8_t>* marks = g.matched.get();

        for (uint32_t i = 0; i < n; ++i) {
            if (keys_nullable) {
                bool any_null = false;
                for (size_t k : pair.probe_key_idx) {
                    if (!sort_row_valid(in->columns[k].view, i)) { any_null = true; break; }
                }
                if (any_null) continue;   // NULL key never equi-matches
            }
            size_t before = build_rows.size();
            g.probe_append(rowh[i], i, build_rows, probe_rows);
            if (residual_fn == nullptr) {
                // No residual: a key match IS the existence proof. Mark and drop the
                // pairs immediately rather than accumulating a batch nothing reads.
                for (size_t bi = before; bi < build_rows.size(); ++bi)
                    marks[static_cast<size_t>(build_rows[bi])].store(
                        1, std::memory_order_relaxed);
                build_rows.clear();
                probe_rows.clear();
            } else if (build_rows.size() >= Join2ProbeOperator::kBatch
                       && !resolve_pairs(in, build_rows, probe_rows, err)) {
                return SinkResult::CONTINUE;
            }
        }
        if (residual_fn != nullptr) resolve_pairs(in, build_rows, probe_rows, err);
        return SinkResult::CONTINUE;
    }

    // Marks were written straight to the global atomics, so there is no per-worker
    // state to merge and no result to produce. Both are deliberately empty.
    void combine(GlobalSinkState&, LocalSinkState&, ErrCtx&) override {}
    void finalize(GlobalSinkState&, ErrCtx&) override {}
};

// Deferred construction (build table exists only after the build pipeline runs).
struct DeferredJoin2Probe : Operator {
    std::vector<size_t> key_idx, payload_idx;
    const Join2Ref* ref;
    JoinMode mode;
    int asof_probe_idx = -1;   // >= 0: ASOF probe (asof column index + op below)
    int asof_op = 0;
    // SEMI/ANTI only: correlated non-equality residual (see SemiAntiProbeOperator).
    ExprProgram residual;
    ExprEvalFn residual_fn = nullptr;
    // SEMI/ANTI only: the emit subset (see SemiAntiProbeOperator's EmitSubset). Held
    // here only to hand to the inner operator when it is constructed.
    bool emit_prune = false;
    std::vector<uint32_t> emit_cols;
    // SEMI/ANTI only: emit the existence verdict as a BOOL column over every probe
    // row instead of filtering (see SemiAntiProbeOperator::emit_existence).
    bool emit_existence = false;
    bool existence_three_valued = false;
    std::string existence_name;
    std::once_flag once;
    std::unique_ptr<Operator> inner;

    DeferredJoin2Probe(std::vector<size_t> keys, std::vector<size_t> payload,
                       const Join2Ref* r, JoinMode m,
                       int asof_idx = -1, int asof_op_code = 0,
                       ExprProgram res = ExprProgram(), ExprEvalFn res_fn = nullptr,
                       bool emit_prune_ = false, std::vector<uint32_t> emit_cols_ = {},
                       bool emit_existence_ = false,
                       bool existence_three_valued_ = false,
                       std::string existence_name_ = {})
        : key_idx(std::move(keys)), payload_idx(std::move(payload)), ref(r), mode(m),
          asof_probe_idx(asof_idx), asof_op(asof_op_code),
          residual(std::move(res)), residual_fn(res_fn),
          emit_prune(emit_prune_), emit_cols(std::move(emit_cols_)),
          emit_existence(emit_existence_),
          existence_three_valued(existence_three_valued_),
          existence_name(std::move(existence_name_)) {}

    std::unique_ptr<OperatorState> make_state() override {
        std::call_once(once, [this] {
            if (asof_probe_idx >= 0) {
                inner = std::make_unique<AsofProbeOperator>(
                    key_idx, payload_idx, ref,
                    static_cast<size_t>(asof_probe_idx), asof_op);
            } else if (mode == JoinMode::Semi || mode == JoinMode::Anti
                       || mode == JoinMode::AntiNullAware
                       || mode == JoinMode::SemiNotDistinct
                       || mode == JoinMode::AntiNotDistinct) {
                const bool is_anti = mode != JoinMode::Semi
                                     && mode != JoinMode::SemiNotDistinct;
                inner = std::make_unique<SemiAntiProbeOperator>(
                    key_idx, payload_idx, ref, is_anti,
                    mode == JoinMode::AntiNullAware, residual, residual_fn,
                    emit_prune, emit_cols, join_mode_null_equal(mode),
                    emit_existence, existence_three_valued, existence_name);
            } else {
                // FULL OUTER probes exactly like LEFT OUTER (preserved probe side,
                // NULL build half on miss) and additionally marks matched build
                // rows for the UnmatchedBuildSource tail pipeline.
                inner = std::make_unique<Join2ProbeOperator>(
                    key_idx, payload_idx, ref,
                    mode == JoinMode::LeftOuter || mode == JoinMode::FullOuter,
                    mode == JoinMode::FullOuter);
            }
        });
        return inner->make_state();
    }
    OpResult execute(const MorselPtr& in, OperatorState& st, MorselPtr& out,
                     ErrCtx& err) override {
        return inner->execute(in, st, out, err);
    }
};

}  // namespace opteryx::engine
