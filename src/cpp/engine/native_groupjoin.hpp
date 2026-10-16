#pragma once
// src/cpp/engine/native_groupjoin.hpp — the GROUPJOIN: an equi-join fused with the
// aggregation that consumes it (docs/GROUPJOIN_DESIGN.md).
//
// Three pipelines, the same shape as the RIGHT SEMI/ANTI exchange
// (native_join2.hpp, Join2MarkSink / SemiAntiBuildSource), run in creation order:
//
//   build   R leg -> Join2BuildSink                         [unchanged]
//   stream  S leg -> GroupJoinAccumSink    accumulates, emits nothing
//   emit           GroupJoinSource         one row per build KEY or per build ROW
//
// ACCUMULATORS LIVE PER BUILD KEY, NOT PER BUILD ROW (design §3.1). With an equi-join
// and no residual, every build row carrying key k is matched by exactly the same
// streamed rows, so one slot per key is exact and a streamed row updates ONE slot
// whatever the build-side duplication. The slot is the index of the key's FIRST entry
// in the join table's entry arrays — the entry the probe already finds:
//   CSR    the first i in [off[b], off[b+1]) with hashes[i] == h
//   radix  the start of the key's run (entries are hash-sorted within a bucket)
// so the slot space is [0, total_rows) and no second lookup or table exists.
//
// Equality is 64-bit hash identity, exactly as the join's (RULED accepted).
//
// Lanes are per worker and allocated with draken_calloc over the whole slot space:
// the OS maps a zero page on first touch, so a worker only pays for the pages its
// rows touch, and a per-page touched flag lets the merge skip the rest. (A per-worker
// touched-SLOT list merging only Σ touched slots was measured and removed 2026-10-15:
// Q3/Q12 unchanged, Q13 8% slower from the first-touch bookkeeping in the hot loop.)
//
// EMIT MODES (design §3.1/§3.2):
//   per_key  one row per distinct build key. `multiply_d` scales duplicate-sensitive
//            partials by the key's build multiplicity d (Yan–Larson F⊗c), which makes
//            Γ_k;F(R ⋈ S) exact with no GROUP BY above and no uniqueness proof.
//   per_row  one row per build row, carrying its key's partial.
// UNMATCHED keys/rows: dropped (INNER), or emitted with
//   kOrganicLeft  COUNT(*) = d (or 1 per row), COUNT(col) = 0, others NULL — the
//                 GROUP BY over a LEFT OUTER JOIN's padded rows;
//   kNullPad      every aggregate NULL — exactly what a LEFT OUTER JOIN to a grouped
//                 relation emits for a row with no group.
//
// Only the plain row-kind aggregates are accumulated here (COUNT(*), COUNT, SUM, AVG,
// MIN, MAX over fixed-width numeric operands — gb_kind_row_resolvable — plus SUM/AVG
// over DECIMAL128). The planner admits nothing else; a kind outside that set reaching
// this file fails loud.

#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <vector>

#include "fork_join.hpp"
#include "native_group_sinks.hpp"
#include "native_join2.hpp"
#include "native_key_hash.hpp"

namespace opteryx::engine {

inline constexpr unsigned kGJPageShift = 12;   // 4096 slots per touched-page flag

enum class GJUnmatched : uint8_t { kDrop = 0, kOrganicLeft = 1, kNullPad = 2 };

// One worker's (or the merged) accumulators over the slot space.
struct GJLanes {
    size_t nslots = 0;
    int64_t* rows = nullptr;            // matched streamed rows per slot (always kept)
    std::vector<GBFlatLanes> spec;      // per aggregate spec
    std::vector<uint8_t> touched;       // per 2^kGJPageShift slots

    GJLanes() = default;
    GJLanes(const GJLanes&) = delete;
    GJLanes& operator=(const GJLanes&) = delete;
    ~GJLanes() { release(); }

    template <class T>
    static T* zalloc(size_t n) {
        return static_cast<T*>(draken_calloc(n == 0 ? 1 : n, sizeof(T)));
    }

    void allocate(size_t n, const std::vector<GBKind>& kinds) {
        nslots = n;
        rows = zalloc<int64_t>(n);
        spec.assign(kinds.size(), GBFlatLanes{});
        for (size_t s = 0; s < kinds.size(); ++s) {
            GBFlatLanes& f = spec[s];
            switch (kinds[s]) {
                case GBKind::Rows: break;
                case GBKind::Valid: f.valid = zalloc<int64_t>(n); break;
                case GBKind::SumI:
                    f.valid = zalloc<int64_t>(n); f.i64 = zalloc<int64_t>(n); break;
                case GBKind::SumF: case GBKind::AvgF:
                    f.valid = zalloc<int64_t>(n); f.f64 = zalloc<double>(n); break;
                case GBKind::AvgI: case GBKind::SumD128: case GBKind::AvgD128:
                    f.valid = zalloc<int64_t>(n); f.i128 = zalloc<__int128>(n); break;
                case GBKind::MinMaxNum:
                    f.valid = zalloc<int64_t>(n); f.i64 = zalloc<int64_t>(n);
                    f.mkey = zalloc<uint64_t>(n); break;
                default: break;   // refused before allocation (GroupJoinState ctor)
            }
        }
        touched.assign((n >> kGJPageShift) + 1, 0);
    }

    void release() {
        auto fr = [](auto*& p) { if (p != nullptr) { draken_free(p); p = nullptr; } };
        fr(rows);
        for (GBFlatLanes& f : spec) {
            fr(f.valid); fr(f.i64); fr(f.f64); fr(f.i128); fr(f.mkey);
        }
        spec.clear();
        touched.clear();
        nslots = 0;
    }
};

// Shared between the accumulate sink and the emit source; owned by the engine (like
// Join2Ref). Plan-known aggregate specs and operand types fix every kind before the
// first morsel, so an empty stream still emits correctly-typed columns.
struct GroupJoinState {
    const Join2Ref* ref = nullptr;
    std::vector<AggSpec2> specs;
    std::vector<AggColMeta> meta;
    std::vector<GBKind> kinds;
    std::mutex mtx;
    std::vector<std::unique_ptr<GJLanes>> queued;   // per-worker lanes, moved in by combine
    std::unique_ptr<GJLanes> merged;                // set by the sink's finalize
    // Telemetry (written once, after the run's pipelines complete).
    std::atomic<uint64_t> streamed_rows{0}, matched_rows{0};
    std::atomic<uint64_t> emitted_atomic{0};
    uint64_t workers = 0;
};

inline bool gj_kind_admitted(GBKind k) {
    return k == GBKind::Rows || k == GBKind::SumD128 || k == GBKind::AvgD128
           || gb_kind_row_resolvable(k);
}

// Per CSR bucket: each distinct hash's representative (its first entry in the bucket)
// and its build multiplicity d, in ONE pass. A bucket holds few distinct hashes but can
// hold one key's many duplicates, so the pass is O(entries x distinct-in-bucket); a
// rescan of the bucket per entry is O(entries²) and was measured at 17.6x on TPC-DS Q1
// (~53K rows over a handful of store keys).
struct GJBucketKeys {
    std::vector<uint64_t> hash;
    std::vector<uint32_t> rep, d;
    void scan(const JoinCsr& c, size_t b) {
        hash.clear(); rep.clear(); d.clear();
        for (uint32_t i = c.off[b]; i < c.off[b + 1]; ++i) {
            const uint64_t h = c.hashes[i];
            size_t k = 0;
            while (k < hash.size() && hash[k] != h) ++k;
            if (k == hash.size()) { hash.push_back(h); rep.push_back(i); d.push_back(0); }
            ++d[k];
        }
    }
    size_t find(uint64_t h) const {
        size_t k = 0;
        while (hash[k] != h) ++k;
        return k;
    }
};

// ---- stream half: accumulate ------------------------------------------------------

struct GroupJoinAccumLocal : LocalSinkState {
    std::unique_ptr<GJLanes> lanes;
    std::vector<uint64_t> h;
    std::vector<uint32_t> mrow, mslot;
    std::vector<uint32_t> codes;
    std::vector<uint8_t> vbits;
    uint64_t streamed = 0, matched = 0;
};

struct GroupJoinAccumSink : Sink {
    GroupJoinState* st;
    std::vector<size_t> key_idx;   // streamed leg's join keys

    GroupJoinAccumSink(GroupJoinState* s, std::vector<size_t> keys)
        : st(s), key_idx(std::move(keys)) {}

    std::unique_ptr<GlobalSinkState> make_global() override {
        return std::make_unique<GlobalSinkState>();
    }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override {
        return std::make_unique<GroupJoinAccumLocal>();
    }

    SinkResult sink(const MorselPtr& in, GlobalSinkState&, LocalSinkState& ls_,
                    ErrCtx& err) override {
        auto& ls = static_cast<GroupJoinAccumLocal&>(ls_);
        const Join2BuildGlobal& g = *st->ref->g;
        const uint32_t n = in->num_rows();
        if (n == 0) return SinkResult::CONTINUE;
        ls.streamed += n;
        if (g.total_rows == 0) return SinkResult::CONTINUE;   // nothing can match
        if (!ls.lanes) {
            ls.lanes = std::make_unique<GJLanes>();
            ls.lanes->allocate(g.total_rows, st->kinds);
        }
        GJLanes& L = *ls.lanes;
        if (!compute_row_hashes(in, key_idx, ls.h, err)) return SinkResult::CONTINUE;
        const bool nullable = probe_keys_nullable(in, key_idx);

        // Pass 1: each streamed row's slot (or none). The table is chosen per morsel.
        ls.mrow.clear();
        ls.mslot.clear();
        auto live = [&](uint32_t i) {
            if (!nullable) return true;
            for (size_t k : key_idx)
                if (!sort_row_valid(in->columns[k].view, i)) return false;
            return true;
        };
        if (g.radix_active) {
            const RadixJoinTable& t = g.radix;
            for (uint32_t i = 0; i < n; ++i) {
                if (!live(i)) continue;
                uint32_t s = 0, c = 0;
                t.find(ls.h[i], t.part_of(ls.h[i]), s, c);
                if (c == 0) continue;
                ls.mrow.push_back(i);
                ls.mslot.push_back(s);
            }
        } else {
            const JoinCsr& c = g.csr;
            for (uint32_t i = 0; i < n; ++i) {
                if (!live(i)) continue;
                const uint64_t key = ls.h[i];
                const size_t b = static_cast<size_t>(key) & c.mask;
                for (uint32_t e = c.off[b]; e < c.off[b + 1]; ++e) {
                    if (c.hashes[e] == key) {
                        ls.mrow.push_back(i);
                        ls.mslot.push_back(e);
                        break;
                    }
                }
            }
        }
        const uint32_t m = static_cast<uint32_t>(ls.mrow.size());
        if (m == 0) return SinkResult::CONTINUE;
        ls.matched += m;
        for (uint32_t k = 0; k < m; ++k) {
            const uint32_t s = ls.mslot[k];
            L.rows[s] += 1;
            L.touched[s >> kGJPageShift] = 1;
        }

        // Pass 2: per aggregate, one column pass over the MATCHED rows. The operand is
        // re-viewed as a dict over its own buffer (codes = the matched rows' physical
        // indices, validity re-projected) so the GROUP BY row-kind arms run unchanged
        // through the uniform data[selection[i]] contract (§11).
        for (size_t s = 0; s < st->specs.size(); ++s) {
            const GBKind kind = st->kinds[s];
            if (kind == GBKind::Rows) continue;
            const DrakenVector& v = in->columns[static_cast<size_t>(st->specs[s].col_idx)].view;
            if (v.type != st->meta[s].type) {
                err.code = 1;
                err.msg = "GroupJoin: aggregate operand type differs from the plan's — "
                          "fail loud, never a mis-read accumulator";
                return SinkResult::CONTINUE;
            }
            ls.codes.resize(m);
            for (uint32_t k = 0; k < m; ++k) ls.codes[k] = v.selection[ls.mrow[k]];
            DrakenVector cv = v;
            cv.selection = ls.codes.data();
            cv.length = m;
            cv.flags = 0;
            cv.validity = nullptr;
            if (v.validity != nullptr) {
                const size_t vb = (static_cast<size_t>(m) + 7) / 8;
                ls.vbits.assign(vb, 0);
                for (uint32_t k = 0; k < m; ++k)
                    if (sort_row_valid(v, ls.mrow[k]))
                        ls.vbits[k >> 3] |= static_cast<uint8_t>(1u << (k & 7));
                cv.validity = ls.vbits.data();
            }
            GBFlatLanes& F = L.spec[s];
            const uint32_t* slots = ls.mslot.data();
            if (kind == GBKind::SumD128 || kind == GBKind::AvgD128) {
                // DECIMAL128 sums are exact int128 (the GROUP BY's SumD128 lane);
                // not a row-kind arm there, so the one loop lives here.
                for (uint32_t k = 0; k < m; ++k) {
                    if (!sort_row_valid(cv, k)) continue;
                    F.i128[slots[k]] += agg2_read_i128(cv, k);
                    F.valid[slots[k]] += 1;
                }
                continue;
            }
            const bool ok = gb_apply_row_kind(
                kind, st->specs[s].fn == AggFn::Max, st->meta[s].is_float, cv, m,
                [&F, slots](uint32_t k) { return GBRowRef<GBFlatLanes>{&F, slots[k]}; },
                err);
            if (!ok) return SinkResult::CONTINUE;
        }
        return SinkResult::CONTINUE;
    }

    void combine(GlobalSinkState&, LocalSinkState& ls_, ErrCtx&) override {
        auto& ls = static_cast<GroupJoinAccumLocal&>(ls_);
        st->streamed_rows.fetch_add(ls.streamed, std::memory_order_relaxed);
        st->matched_rows.fetch_add(ls.matched, std::memory_order_relaxed);
        if (!ls.lanes) return;
        std::lock_guard<std::mutex> lk(st->mtx);
        st->queued.push_back(std::move(ls.lanes));
    }

    // Fold src into dst at the slots `each` yields. One kind dispatch per spec per call.
    template <class Each>
    void fold(GJLanes& dst, const GJLanes& src, Each&& each, std::atomic<bool>& overflow) {
        each([&](size_t e) { dst.rows[e] += src.rows[e]; });
        for (size_t s = 0; s < st->kinds.size(); ++s) {
            GBFlatLanes& D = dst.spec[s];
            const GBFlatLanes& S = src.spec[s];
            switch (st->kinds[s]) {
                case GBKind::Rows: break;
                case GBKind::Valid:
                    each([&](size_t e) { D.valid[e] += S.valid[e]; });
                    break;
                case GBKind::SumI:
                    each([&](size_t e) {
                        if (__builtin_add_overflow(D.i64[e], S.i64[e], &D.i64[e]))
                            overflow.store(true, std::memory_order_relaxed);
                        D.valid[e] += S.valid[e];
                    });
                    break;
                case GBKind::SumF: case GBKind::AvgF:
                    each([&](size_t e) { D.f64[e] += S.f64[e]; D.valid[e] += S.valid[e]; });
                    break;
                case GBKind::AvgI: case GBKind::SumD128: case GBKind::AvgD128:
                    each([&](size_t e) { D.i128[e] += S.i128[e]; D.valid[e] += S.valid[e]; });
                    break;
                case GBKind::MinMaxNum: {
                    const bool want_max = st->specs[s].fn == AggFn::Max;
                    each([&](size_t e) {
                        if (S.valid[e] == 0) return;
                        if (D.valid[e] == 0 || (want_max ? S.mkey[e] > D.mkey[e]
                                                         : S.mkey[e] < D.mkey[e])) {
                            D.mkey[e] = S.mkey[e];
                            D.i64[e] = S.i64[e];
                        }
                        D.valid[e] += S.valid[e];
                    });
                    break;
                }
                default: break;
            }
        }
    }

    // Fold every queued worker into worker 0's lanes, in parallel over touched pages.
    void finalize(GlobalSinkState& gs, ErrCtx& err) override {
        std::vector<std::unique_ptr<GJLanes>>& q = st->queued;
        st->workers = q.size();
        if (q.empty()) return;   // no worker saw a matchable row: merged stays null
        std::unique_ptr<GJLanes> dst = std::move(q[0]);
        const size_t npages = dst->touched.size();
        const size_t nslots = dst->nslots;
        const unsigned nt = (q.size() > 1 && nslots >= 65536 && gs.exec_dop > 1)
                                ? static_cast<unsigned>(gs.exec_dop) : 1u;
        std::atomic<size_t> next{0};
        std::atomic<bool> overflow{false};
        fork_join(nt, [&](unsigned) {
            for (;;) {
                const size_t pg = next.fetch_add(1);
                if (pg >= npages) break;
                const size_t lo = pg << kGJPageShift;
                const size_t hi = std::min(nslots, lo + (size_t(1) << kGJPageShift));
                for (size_t w = 1; w < q.size(); ++w) {
                    const GJLanes& src = *q[w];
                    if (!src.touched[pg]) continue;
                    dst->touched[pg] = 1;
                    fold(*dst, src, [lo, hi](auto&& f) {
                        for (size_t e = lo; e < hi; ++e) f(e);
                    }, overflow);
                }
            }
        });
        q.clear();
        if (overflow.load()) {
            err.code = 1;
            err.msg = "SUM overflow: exact integer sum exceeds INT64 — fail loud, never a "
                      "wrapped answer";
            return;
        }
        st->merged = std::move(dst);
    }
};

// ---- emit half --------------------------------------------------------------------

struct GroupJoinSourceGlobal : GlobalSourceState {
    std::atomic<size_t> next{0};
    std::atomic<bool> tail_claimed{false};
};

struct GroupJoinSource : Source {
    GroupJoinState* st;
    bool per_key;
    bool multiply_d;
    GJUnmatched unmatched;
    static constexpr size_t kChunk = 65536;

    GroupJoinSource(GroupJoinState* s, bool pk, bool md, GJUnmatched um)
        : st(s), per_key(pk), multiply_d(md), unmatched(um) {}

    std::unique_ptr<GlobalSourceState> make_global() override {
        return std::make_unique<GroupJoinSourceGlobal>();
    }
    std::unique_ptr<LocalSourceState> make_local(GlobalSourceState&) override {
        return std::make_unique<LocalSourceState>();
    }

    // One output row: the build row to gather, its slot (UINT32_MAX = NULL-keyed tail
    // row, never matchable), and the multiplier for duplicate-sensitive partials.
    struct Out { uint32_t build_row; uint32_t slot; uint32_t d; };

    bool matched(uint32_t slot) const {
        return slot != UINT32_MAX && st->merged && st->merged->rows[slot] > 0;
    }

    // Emit one item if it qualifies under the unmatched policy.
    void offer(std::vector<Out>& out, uint32_t build_row, uint32_t slot, uint32_t d) const {
        if (unmatched == GJUnmatched::kDrop && !matched(slot)) return;
        out.push_back({build_row, slot, d});
    }

    // Walk table entries [lo, hi) (radix) or buckets [lo, hi) (CSR).
    void collect(const Join2BuildGlobal& g, size_t lo, size_t hi, std::vector<Out>& out) const {
        if (g.radix_active) {
            const RadixJoinTable& t = g.radix;
            const size_t total = g.total_rows;
            // A run that STARTS before `lo` belongs to the chunk that owns its start.
            size_t i = lo;
            while (i < hi && i > 0 && t.hashes[i] == t.hashes[i - 1]) ++i;
            while (i < hi) {
                size_t e = i + 1;
                while (e < total && t.hashes[e] == t.hashes[i]) ++e;
                const uint32_t rep = static_cast<uint32_t>(i);
                const uint32_t d = static_cast<uint32_t>(e - i);
                if (per_key) {
                    offer(out, t.rows[i], rep, multiply_d ? d : 1);
                } else {
                    for (size_t j = i; j < e; ++j) offer(out, t.rows[j], rep, 1);
                }
                i = e;
            }
        } else {
            const JoinCsr& c = g.csr;
            GJBucketKeys keys;
            for (size_t b = lo; b < hi; ++b) {
                if (c.off[b] == c.off[b + 1]) continue;
                keys.scan(c, b);
                if (per_key) {
                    // emit each key once, at its first entry
                    for (size_t k = 0; k < keys.rep.size(); ++k)
                        offer(out, c.rows[keys.rep[k]], keys.rep[k], multiply_d ? keys.d[k] : 1);
                } else {
                    for (uint32_t i = c.off[b]; i < c.off[b + 1]; ++i)
                        offer(out, c.rows[i], keys.rep[keys.find(c.hashes[i])], 1);
                }
            }
        }
    }

    // NULL-keyed build rows (retained only when the build tracked matches): never
    // matched. per_key emits them as ONE group (the planner admits per_key LEFT for a
    // single key column only, where every NULL key is the same group); per_row emits
    // each.
    void collect_tail(const Join2BuildGlobal& g, std::vector<Out>& out) const {
        if (unmatched == GJUnmatched::kDrop || g.tail_null_rows == 0) return;
        const uint32_t first = g.total_rows;
        if (per_key) {
            out.push_back({first, UINT32_MAX, multiply_d ? g.tail_null_rows : 1u});
        } else {
            for (uint32_t r = 0; r < g.tail_null_rows; ++r)
                out.push_back({first + r, UINT32_MAX, 1});
        }
    }

    bool emit_aggregates(const std::vector<Out>& rows, CxxMorsel& out, ErrCtx& err) const {
        const uint32_t n = static_cast<uint32_t>(rows.size());
        const GJLanes* M = st->merged.get();
        const bool organic = unmatched == GJUnmatched::kOrganicLeft;
        std::vector<int64_t> ival(n), vgath(n);
        std::vector<uint8_t> ok(n);
        std::vector<double> fval;
        std::vector<__int128> wval;
        std::vector<uint64_t> kval;
        for (size_t s = 0; s < st->specs.size(); ++s) {
            const GBKind kind = st->kinds[s];
            CxxColumn col;
            if (kind == GBKind::Rows || kind == GBKind::Valid) {
                for (uint32_t i = 0; i < n; ++i) {
                    const Out& o = rows[i];
                    if (matched(o.slot)) {
                        const int64_t base = kind == GBKind::Rows ? M->rows[o.slot]
                                                                  : M->spec[s].valid[o.slot];
                        if (__builtin_mul_overflow(base, static_cast<int64_t>(o.d), &ival[i])) {
                            err.code = 1;
                            err.msg = "COUNT overflow in groupjoin multiplicity — fail loud";
                            return false;
                        }
                        ok[i] = 1;
                    } else if (organic) {
                        // A preserved row with no partner is ONE padded tuple: COUNT(*)
                        // counts it, COUNT(col) does not (its col is NULL).
                        ival[i] = kind == GBKind::Rows ? static_cast<int64_t>(o.d) : 0;
                        ok[i] = 1;
                    } else {
                        ival[i] = 0;
                        ok[i] = 0;   // kNullPad: the outer join's NULL
                    }
                }
                col = emit_fixed_column(ival.data(), ok.data(), n, DRAKEN_INT64, nullptr, err);
            } else {
                // Gather this spec's lanes for the output rows; valid == 0 is NULL,
                // which emit_lane_column already maps (valid_ok).
                GBLaneView V;
                const GBFlatLanes& F = M ? M->spec[s] : GBFlatLanes{};
                for (uint32_t i = 0; i < n; ++i)
                    vgath[i] = matched(rows[i].slot) ? F.valid[rows[i].slot] : 0;
                V.valid = vgath.data();
                switch (kind) {
                    case GBKind::SumI:
                        for (uint32_t i = 0; i < n; ++i) {
                            ival[i] = 0;
                            if (vgath[i] == 0) continue;
                            if (__builtin_mul_overflow(F.i64[rows[i].slot],
                                                       static_cast<int64_t>(rows[i].d), &ival[i])) {
                                err.code = 1;
                                err.msg = "SUM overflow: exact integer sum exceeds INT64 — "
                                          "fail loud, never a wrapped answer";
                                return false;
                            }
                        }
                        V.i64 = ival.data();
                        break;
                    case GBKind::MinMaxNum:
                        for (uint32_t i = 0; i < n; ++i)
                            ival[i] = vgath[i] ? F.i64[rows[i].slot] : 0;
                        V.i64 = ival.data();
                        break;
                    case GBKind::SumF:
                    case GBKind::AvgF:
                        fval.assign(n, 0.0);
                        for (uint32_t i = 0; i < n; ++i) {
                            if (vgath[i] == 0) continue;
                            const double x = F.f64[rows[i].slot];
                            fval[i] = kind == GBKind::SumF ? x * static_cast<double>(rows[i].d) : x;
                        }
                        V.f64 = fval.data();
                        break;
                    case GBKind::AvgI:
                    case GBKind::AvgD128:
                        wval.assign(n, 0);
                        for (uint32_t i = 0; i < n; ++i)
                            if (vgath[i] != 0) wval[i] = F.i128[rows[i].slot];
                        V.i128 = wval.data();
                        break;
                    case GBKind::SumD128:
                        wval.assign(n, 0);
                        for (uint32_t i = 0; i < n; ++i) {
                            if (vgath[i] == 0) continue;
                            if (__builtin_mul_overflow(F.i128[rows[i].slot],
                                                       static_cast<__int128>(rows[i].d), &wval[i])) {
                                err.code = 1;
                                err.msg = "SUM overflow: DECIMAL128 sum exceeds int128 in "
                                          "groupjoin multiplicity — fail loud";
                                return false;
                            }
                        }
                        V.i128 = wval.data();
                        break;
                    default:
                        err.code = 1;
                        err.msg = "GroupJoin: aggregate kind outside the admitted set reached "
                                  "the emit — planner and engine disagree, fail loud";
                        return false;
                }
                col = emit_lane_column(st->meta[s], kind, V, n, err);
            }
            if (err.code != 0) return false;
            out.columns.push_back(std::move(col));
            out.names.push_back(st->specs[s].name);
        }
        return true;
    }

    SourceResult get_morsel(GlobalSourceState& gs_, LocalSourceState&, MorselPtr& out,
                            ErrCtx& err) override {
        auto& gs = static_cast<GroupJoinSourceGlobal&>(gs_);
        const Join2BuildGlobal& g = *st->ref->g;
        const size_t domain = g.radix_active ? g.total_rows
                                             : (g.csr.built ? g.csr.mask + 1 : 0);
        std::vector<Out> rows;
        for (;;) {
            rows.clear();
            const size_t lo = gs.next.fetch_add(kChunk);
            if (lo < domain) {
                collect(g, lo, std::min(domain, lo + kChunk), rows);
            } else if (!gs.tail_claimed.exchange(true)) {
                collect_tail(g, rows);
                if (rows.empty()) return SourceResult::FINISHED;
            } else {
                return SourceResult::FINISHED;
            }
            if (rows.empty()) continue;
            const uint32_t n = static_cast<uint32_t>(rows.size());
            auto morsel = std::make_shared<CxxMorsel>();
            morsel->zero_col_rows = n;
            if (!g.morsels.empty() && !g.morsels.front()->columns.empty()) {
                std::vector<uint32_t> order(n);
                for (uint32_t i = 0; i < n; ++i) order[i] = rows[i].build_row;
                MorselPtr half = gather_rows(g.morsels, order, 0, n, g.row_m, g.row_r,
                                             g.morsels.front()->names, err);
                if (err.code != 0 || half == nullptr) return SourceResult::FINISHED;
                for (CxxColumn& c : half->columns) morsel->columns.push_back(std::move(c));
                morsel->names = half->names;
                morsel->names.resize(morsel->columns.size());
            }
            if (!emit_aggregates(rows, *morsel, err)) return SourceResult::FINISHED;
            st->emitted_atomic.fetch_add(n, std::memory_order_relaxed);
            out = std::move(morsel);
            return SourceResult::HAVE_MORE;
        }
    }
};

}  // namespace opteryx::engine
