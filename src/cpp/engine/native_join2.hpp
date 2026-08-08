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

enum class JoinMode : uint8_t {
    Inner = 0, LeftOuter = 1, Semi = 2, AntiNullAware = 3, Anti = 4, FullOuter = 5
};

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
    // FULL OUTER only (track_matches): NULL-keyed build rows. Every other mode
    // drops them (a NULL key can never equi-match), but FULL OUTER must still
    // emit them in the unmatched-build tail — so their addresses are retained
    // here, OUTSIDE the keyed row space the CSR is built over.
    std::vector<uint32_t> null_row_m, null_row_r;
    uint32_t next_row = 0;
    bool saw_null_key = false;
};
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
    // Zero-row, plan-typed payload columns. Used ONLY when no build morsel was ever
    // retained (a build side that streamed zero rows): gather_rows takes its column
    // count and types from ms.front(), so it needs a schema to emit a LEFT OUTER's
    // all-NULL build half against. Never consulted when real morsels exist — those
    // carry the authoritative (data-observed) types.
    MorselPtr schema_morsel;
    std::vector<uint64_t> asof_keys;   // ASOF only: parallel to build rows
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
    int asof_idx = -1;   // >= 0: ASOF build — capture the asof column's normalized
                         // order key per row (rows with a NULL asof value are
                         // skipped: they can never satisfy the MATCH_CONDITION)
    bool track_matches = false;   // FULL OUTER: allocate the matched[] flags and
                                  // retain NULL-keyed rows for the unmatched tail

    Join2BuildSink(std::vector<size_t> keys, std::vector<size_t> payload_idx,
                   std::vector<DrakenType> types, std::vector<const LogicalType*> logical,
                   int asof = -1, bool track = false)
        : key_idx(std::move(keys)), payload_col_idx(std::move(payload_idx)),
          payload_types(std::move(types)), payload_logical(std::move(logical)),
          asof_idx(asof), track_matches(track) {}

    // Zero-row payload columns at the PLAN-known types. This is the fallback schema
    // for a build side that streams zero rows (a filtered-to-empty subquery): with no
    // retained morsel there is nothing for gather_rows to read a column count or type
    // from, and a LEFT OUTER still has to emit an all-NULL build half. When any real
    // morsel was retained this is never consulted — observed types beat plan types.
    MorselPtr make_schema_morsel() const {
        auto m = std::make_shared<CxxMorsel>();
        m->columns.reserve(payload_col_idx.size());
        for (size_t c = 0; c < payload_col_idx.size(); ++c)
            m->columns.push_back(make_empty_col(payload_types[c], payload_logical[c]));
        m->names.resize(payload_col_idx.size());
        m->zero_col_rows = 0;
        return m;
    }

    std::unique_ptr<GlobalSinkState> make_global() override {
        auto g = std::make_unique<Join2BuildGlobal>();
        g->schema_morsel = make_schema_morsel();
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
            // null-aware ANTI contract and skip the table insert.
            bool any_null = false;
            for (size_t k : key_idx) {
                if (!sort_row_valid(in->columns[k].view, i)) { any_null = true; break; }
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
            if (asof_idx >= 0)
                l.asof_keys.push_back(
                    sort_num_key(in->columns[static_cast<size_t>(asof_idx)].view, i));
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
        g.row_m.reserve(g.row_m.size() + l.next_row);
        g.row_r.reserve(g.row_r.size() + l.next_row);
        for (uint32_t r = 0; r < l.next_row; ++r) {
            g.row_m.push_back(morsel_off + l.row_m[r]);
            g.row_r.push_back(l.row_r[r]);
        }
        g.asof_keys.insert(g.asof_keys.end(), l.asof_keys.begin(), l.asof_keys.end());
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

    void finalize(GlobalSinkState& gs, ErrCtx&) override {
        // No-op for ASOF, whose combine() populated `index` instead of queuing chunks.
        auto& g = static_cast<Join2BuildGlobal&>(gs);
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
    static constexpr size_t kBatch = 8192;
    static constexpr uint32_t kNoBuildRow = UINT32_MAX;

    Join2ProbeOperator(std::vector<size_t> keys, std::vector<size_t> payload,
                       const Join2Ref* r, bool outer, bool track = false)
        : probe_key_idx(std::move(keys)), probe_payload_idx(std::move(payload)),
          ref(r), left_outer(outer), track_matches(track) {}

    std::unique_ptr<OperatorState> make_state() override {
        return std::make_unique<Join2ProbeState>();
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
                // gather_rows recurses into an ARRAY column's child vector, which a
                // zero-row schema column does not carry — fail loud rather than read
                // an uninitialized child view.
                for (const CxxColumn& c : bms.front()->columns) {
                    if (c.view.type == DRAKEN_ARRAY) {
                        err.code = 1;
                        err.msg = "Join2Probe: ARRAY build payload with an empty build "
                                  "side has no child vector to emit NULLs against — "
                                  "fail loud, never silent corruption";
                        return nullptr;
                    }
                }
            }
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
        const bool keys_nullable = probe_keys_nullable(in, probe_key_idx);

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
// Keys are sort_num_key-normalized (uint64 order == value order, IEEE-correct for
// floats), so one comparator covers timestamps, ints and doubles alike.

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
                std::sort(rows.begin(), rows.end(), [&](int64_t a, int64_t b) {
                    return g->asof_keys[a] < g->asof_keys[b];
                });
                g->asof_index.insert_new(kv.first,
                                         static_cast<int64_t>(g->asof_sorted.size()));
                g->asof_sorted.push_back(std::move(rows));
            }
        });
    }

    int64_t match_row(const std::vector<int64_t>& rows, uint64_t k,
                      const std::vector<uint64_t>& keys) const {
        const int64_t none = static_cast<int64_t>(kNoBuildRow);
        auto cmp = [&](int64_t r, uint64_t v) { return keys[r] < v; };
        auto cmp2 = [&](uint64_t v, int64_t r) { return v < keys[r]; };
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
                                  sort_num_key(av, row), g.asof_keys));
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
struct SemiAntiProbeOperator : Join2ProbeOperator {
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

    SemiAntiProbeOperator(std::vector<size_t> keys, std::vector<size_t> payload,
                          const Join2Ref* r, bool anti_, bool null_aware_,
                          ExprProgram res, ExprEvalFn res_fn)
        : Join2ProbeOperator(std::move(keys), std::move(payload), r, /*outer=*/false),
          anti(anti_), null_aware(null_aware_),
          residual(std::move(res)), residual_fn(res_fn) {}

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
    OpResult execute(const MorselPtr& in, OperatorState& st_, MorselPtr& out,
                     ErrCtx& err) override {
        (void)st_;
        const Join2BuildGlobal& g = *ref->g;
        uint32_t n = in->num_rows();
        if (n == 0) return OpResult::NEED_INPUT;

        // NOT IN only: a NULL anywhere in the build makes every comparison UNKNOWN.
        // Plain ANTI (NOT EXISTS) must NOT do this — a NULL build key is just a row
        // that matches nothing.
        if (anti && null_aware && g.saw_null_key) return OpResult::NEED_INPUT;
        bool build_empty = (g.total_rows == 0 && !g.saw_null_key);

        std::vector<uint64_t> rowh;
        if (!build_empty && !compute_row_hashes(in, probe_key_idx, rowh, err))
            return OpResult::NEED_INPUT;
        const bool keys_nullable = probe_keys_nullable(in, probe_key_idx);

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
        out = gather_rows(ms, survivors, 0, survivors.size(), row_m, row_r, in->names, err);
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

            // Build payload: real rows, the engine's one row gather.
            if (!g.morsels.empty()) {
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
                    if (c.view.type == DRAKEN_ARRAY) {
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
    std::once_flag once;
    std::unique_ptr<Operator> inner;

    DeferredJoin2Probe(std::vector<size_t> keys, std::vector<size_t> payload,
                       const Join2Ref* r, JoinMode m,
                       int asof_idx = -1, int asof_op_code = 0,
                       ExprProgram res = ExprProgram(), ExprEvalFn res_fn = nullptr)
        : key_idx(std::move(keys)), payload_idx(std::move(payload)), ref(r), mode(m),
          asof_probe_idx(asof_idx), asof_op(asof_op_code),
          residual(std::move(res)), residual_fn(res_fn) {}

    std::unique_ptr<OperatorState> make_state() override {
        std::call_once(once, [this] {
            if (asof_probe_idx >= 0) {
                inner = std::make_unique<AsofProbeOperator>(
                    key_idx, payload_idx, ref,
                    static_cast<size_t>(asof_probe_idx), asof_op);
            } else if (mode == JoinMode::Semi || mode == JoinMode::Anti
                       || mode == JoinMode::AntiNullAware) {
                inner = std::make_unique<SemiAntiProbeOperator>(
                    key_idx, payload_idx, ref, mode != JoinMode::Semi,
                    mode == JoinMode::AntiNullAware, residual, residual_fn);
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
