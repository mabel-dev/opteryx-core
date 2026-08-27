#pragma once
// src/cpp/engine/native_window_frame.hpp — FRAMED aggregate window functions:
// SUM/COUNT/AVG/MIN/MAX OVER (PARTITION BY ... ORDER BY ... ROWS|RANGE BETWEEN ...).
//
// A SEPARATE sink from WindowSink (native_sort.hpp), deliberately. WindowSink's five
// kinds (ROW_NUMBER/RANK/DENSE_RANK/LAG/LEAD) are all "one value per row from the
// sorted order itself" — a position or a shifted gather, no accumulation. A framed
// aggregate is a genuinely different computation (a sliding-window reduction over the
// ORDERED partition) with its own per-row output TYPE (not always INT64), so it gets
// its own Sink rather than a kind code bolted onto WindowFnSpec.
//
// Shares the sort/partition machinery with WindowSink via native_sort.hpp
// (SortKeySpec, build_sort_keys, sort_perm, win_keys_equal, flatten_rows, gather_rows)
// and the per-type value-read primitives from native_group_sinks.hpp (agg2_read_raw,
// agg2_read_i128) — the SAME domain split GROUP BY aggregation already uses (int64
// exact / double / int128 exact), so a framed SUM answers the identical value a
// GROUP BY SUM would over the same rows.
//
// ---- Frame model ------------------------------------------------------------------
// FrameUnits::Rows  — bounds count PHYSICAL rows from the current row.
// FrameUnits::Range — bounds are PEER-GROUP relative: CurrentRow means "every row
//                     whose ORDER BY key ties with this row's", not just this row.
//                     Only UnboundedPreceding / CurrentRow / UnboundedFollowing are
//                     supported for RANGE (peer-group semantics) — a numeric `RANGE
//                     n PRECEDING/FOLLOWING` (value-distance on the ORDER BY column)
//                     is refused at plan time (compiler.py), not silently mistreated
//                     as ROWS. That is a distinct, materially larger feature (every
//                     numeric/date/timestamp type needs its own distance arithmetic)
//                     and is out of scope here.
//
// For every SUPPORTED bound combination (validated at plan time — start's rank in
// {UnboundedPreceding < Preceding < CurrentRow < Following < UnboundedFollowing} never
// after end's), the frame's [lo(i), hi(i)] (row-position bounds, inclusive, already
// clamped to the partition) is MONOTONIC NON-DECREASING in i within one partition —
// the reason a single left-to-right sweep with two pointers (SUM/COUNT/AVG) and a
// monotonic deque (MIN/MAX, the classic O(n) sliding-window-extremum algorithm) is
// sufficient; neither ever needs to re-scan a shrinking/growing window from scratch.
//
// ---- Scope ------------------------------------------------------------------------
// Argument types: the SAME set GROUP BY SUM/COUNT/AVG/MIN/MAX accept
// (agg2_operand_supported: integer family, UINT8-64, BOOL, DATE32/TIME32/TIME64/
// TIMESTAMP64, DECIMAL, DECIMAL128, FLOAT32/64) — checked by the compiler before this
// sink is ever constructed.
// FrameUnits::Groups (SQL's third frame unit) is refused at plan time — vanishingly
// rare and not needed by any query this engine has been asked to run.

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <deque>
#include <string>
#include <vector>

#include "logical_type.h"
#include "native_group_sinks.hpp"   // agg2_read_raw, agg2_read_i128, agg2_operand_supported
#include "native_sort.hpp"          // SortKeySpec, build_sort_keys, sort_perm, win_keys_equal,
                                    // sort_row_valid, flatten_rows, gather_rows, EmitSubset,
                                    // MorselBuffer

namespace opteryx::engine {

enum class WinAggFn : uint8_t { Sum = 0, Count = 1, Avg = 2, Min = 3, Max = 4 };
enum class FrameUnits : uint8_t { Rows = 0, Range = 1 };
enum class FrameBoundKind : uint8_t {
    UnboundedPreceding = 0,
    Preceding          = 1,
    CurrentRow         = 2,
    Following          = 3,
    UnboundedFollowing = 4,
};

struct FrameBound {
    FrameBoundKind kind = FrameBoundKind::CurrentRow;
    int64_t        offset = 0;   // meaningful only for Preceding/Following
};

struct FrameSpec {
    FrameUnits units = FrameUnits::Rows;
    FrameBound start;
    FrameBound end;
};

struct FramedAggFnSpec {
    WinAggFn    kind;
    std::string name;
    int         arg_col = -1;         // -1 only valid for Count (COUNT(*))
    FrameSpec   frame;
    DrakenType  out_type = DRAKEN_INT64;
    const LogicalType* out_logical = nullptr;   // DECIMAL/DECIMAL128 passthrough scale
};

struct FramedWindowLocal : LocalSinkState { std::vector<MorselPtr> morsels; };
struct FramedWindowGlobal : GlobalSinkState {
    std::mutex mtx;
    std::vector<MorselPtr> morsels;
};

// Per-function accumulation domain, chosen from the ARGUMENT column's physical type —
// the same three-way split native_group_sinks.hpp's GBKind::SumI/SumF/SumD128 uses.
// `Str` is a MIN/MAX-ONLY domain: the string family has a byte-lexicographic
// order (sort_type_is_string / SortKeyCmp's string arm) but no arithmetic, so
// SUM/AVG over it are rejected at plan time (compiler.py's FramedWindowNode
// branch) and refused again below. Unlike the other three domains its result
// is not a scalar — a string MIN/MAX carries the WINNING SOURCE ROW forward and
// the emit re-gathers the bytes, rather than copying them twice.
enum class AggDomain : uint8_t { I64 = 0, F64 = 1, I128 = 2, Str = 3 };

inline AggDomain framed_agg_domain(DrakenType t) {
    if (t == DRAKEN_DECIMAL128) return AggDomain::I128;
    if (t == DRAKEN_FLOAT32 || t == DRAKEN_FLOAT64) return AggDomain::F64;
    if (sort_type_is_string(t)) return AggDomain::Str;
    return AggDomain::I64;
}

// (pointer, length) of one non-null string row, read through the CANONICAL
// layout (`data` is the DrakenStringArena struct — see sort.hpp's note on the
// two conventions). Never called for a null row: validity is the caller's
// dimension, exactly as in build_sort_keys.
struct FramedStrRef { const uint8_t* p; uint32_t len; };

inline FramedStrRef framed_read_str(const DrakenVector& v, uint32_t row) {
    const DrakenStringArena* sa = string_arena_of(v);
    const DrakenStringSlot* slot = &sa->slots[v.selection[row]];
    return {reinterpret_cast<const uint8_t*>(str_data(slot, sa->arena)), str_length(slot)};
}

// Byte-wise compare, shorter prefix first — the SAME ordering SortKeyCmp's
// string arm defines, so a framed MIN/MAX and an ORDER BY agree on which value
// is smaller. Keep the two in step.
inline int framed_cmp_str(const FramedStrRef& a, const FramedStrRef& b) {
    uint32_t common = a.len < b.len ? a.len : b.len;
    int r = common ? std::memcmp(a.p, b.p, common) : 0;
    if (r != 0) return r;
    return a.len < b.len ? -1 : (a.len > b.len ? 1 : 0);
}

struct FramedWindowSink : Sink, EmitSubset {
    std::vector<SortKeySpec>     sort_spec;   // [partition keys asc..., order keys...]
    size_t                       n_part;
    std::vector<FramedAggFnSpec> funcs;
    MorselBuffer*                out;
    size_t                       chunk_rows;

    FramedWindowSink(std::vector<SortKeySpec> s, size_t np, std::vector<FramedAggFnSpec> f,
                     MorselBuffer* b, size_t chunk = 131072,
                     bool prune = false, std::vector<uint32_t> emit = {})
        : sort_spec(std::move(s)), n_part(np), funcs(std::move(f)), out(b),
          chunk_rows(chunk) {
        emit_prune = prune;
        emit_cols = std::move(emit);
    }

    std::unique_ptr<GlobalSinkState> make_global() override {
        return std::make_unique<FramedWindowGlobal>();
    }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override {
        return std::make_unique<FramedWindowLocal>();
    }
    SinkResult sink(const MorselPtr& in, GlobalSinkState&, LocalSinkState& ls,
                    ErrCtx&) override {
        if (in->num_rows() > 0) static_cast<FramedWindowLocal&>(ls).morsels.push_back(in);
        return SinkResult::CONTINUE;
    }
    void combine(GlobalSinkState& gs, LocalSinkState& ls, ErrCtx&) override {
        auto& g = static_cast<FramedWindowGlobal&>(gs);
        auto& l = static_cast<FramedWindowLocal&>(ls);
        std::lock_guard<std::mutex> lk(g.mtx);
        for (MorselPtr& m : l.morsels) g.morsels.push_back(std::move(m));
    }

    void finalize(GlobalSinkState& gs, ErrCtx& err) override {
        auto& g = static_cast<FramedWindowGlobal&>(gs);
        std::vector<MorselPtr> src;
        for (const MorselPtr& m : g.morsels) if (m->num_rows() > 0) src.push_back(m);
        if (src.empty()) return;

        if (sort_spec.size() <= n_part) {
            err.code = 1;
            err.msg = "FramedWindowSink: a window FRAME requires an ORDER BY";
            return;
        }

        std::vector<uint32_t> row_m, row_r;
        size_t n = flatten_rows(src, row_m, row_r);
        std::vector<SortKeyColumn> keys;
        if (!build_sort_keys(src, sort_spec, n, keys, err)) return;
        std::vector<uint32_t> perm(n);
        for (size_t i = 0; i < n; ++i) perm[i] = static_cast<uint32_t>(i);
        sort_perm(keys, perm);

        // Partition boundaries — one forward + one backward sweep, shared by every
        // function. `part_start[i]`/`part_end[i]` are the [first, one-past-last)
        // sorted positions of i's partition.
        std::vector<int64_t> part_start(n), part_end(n);
        {
            int64_t p_start = 0;
            for (size_t i = 0; i < n; ++i) {
                bool new_part = (i == 0) ||
                    !win_keys_equal(keys, perm[i], perm[i - 1], 0, n_part);
                if (new_part) p_start = static_cast<int64_t>(i);
                part_start[i] = p_start;
            }
            int64_t p_end = static_cast<int64_t>(n);
            for (size_t ii = n; ii-- > 0;) {
                bool new_part = (ii + 1 == n) ||
                    !win_keys_equal(keys, perm[ii], perm[ii + 1], 0, n_part);
                if (new_part) p_end = static_cast<int64_t>(ii + 1);
                part_end[ii] = p_end;
            }
        }

        // Peer-group boundaries (RANGE's CurrentRow), only built when a function
        // actually needs them.
        bool need_peers = false;
        for (const FramedAggFnSpec& fs : funcs) {
            if (fs.frame.units == FrameUnits::Range
                && (fs.frame.start.kind == FrameBoundKind::CurrentRow
                    || fs.frame.end.kind == FrameBoundKind::CurrentRow)) {
                need_peers = true;
                break;
            }
        }
        std::vector<int64_t> peer_start, peer_end;
        if (need_peers) {
            peer_start.resize(n);
            peer_end.resize(n);
            int64_t g_start = 0;
            for (size_t i = 0; i < n; ++i) {
                bool new_grp = (i == 0) || part_start[i] != part_start[i - 1] ||
                    !win_keys_equal(keys, perm[i], perm[i - 1], n_part, sort_spec.size());
                if (new_grp) g_start = static_cast<int64_t>(i);
                peer_start[i] = g_start;
            }
            int64_t g_end = static_cast<int64_t>(n);
            for (size_t ii = n; ii-- > 0;) {
                bool new_grp = (ii + 1 == n) || part_end[ii] != part_end[ii + 1] ||
                    !win_keys_equal(keys, perm[ii], perm[ii + 1], n_part, sort_spec.size());
                if (new_grp) g_end = static_cast<int64_t>(ii + 1);
                peer_end[ii] = g_end;
            }
        }

        // A frame bound's row position for sorted position i. Always clamped inside
        // [part_start[i], part_end[i]-1] by construction — see the header comment on
        // why this keeps lo(i)/hi(i) monotonic and free of empty-partition edge cases.
        auto bound_pos = [&](size_t i, const FrameBound& b, FrameUnits units,
                             bool is_start) -> int64_t {
            switch (b.kind) {
                case FrameBoundKind::UnboundedPreceding:
                    return part_start[i];
                case FrameBoundKind::UnboundedFollowing:
                    return part_end[i] - 1;
                case FrameBoundKind::CurrentRow:
                    if (units == FrameUnits::Range) {
                        return is_start ? peer_start[i] : peer_end[i] - 1;
                    }
                    return static_cast<int64_t>(i);
                case FrameBoundKind::Preceding:
                    return std::max<int64_t>(part_start[i], static_cast<int64_t>(i) - b.offset);
                case FrameBoundKind::Following:
                default:
                    return std::min<int64_t>(part_end[i] - 1, static_cast<int64_t>(i) + b.offset);
            }
        };

        size_t nf = funcs.size();
        std::vector<std::vector<uint8_t>>  res_valid(nf);
        std::vector<std::vector<int64_t>>  res_i64(nf);
        std::vector<std::vector<double>>   res_f64(nf);
        std::vector<std::vector<__int128>> res_i128(nf);
        // Str lane only: the GLOBAL row id (flatten_rows space) of the winning
        // value, re-read by the emit. kNoRow where the frame is empty.
        std::vector<std::vector<uint32_t>> res_row(nf);
        constexpr uint32_t kNoRow = UINT32_MAX;

        for (size_t f = 0; f < nf; ++f) {
            const FramedAggFnSpec& fs = funcs[f];
            res_valid[f].assign(n, 0);
            res_i64[f].assign(n, 0);
            res_f64[f].assign(n, 0.0);
            res_i128[f].assign(n, 0);

            AggDomain dom = (fs.kind == WinAggFn::Count || fs.arg_col < 0)
                ? AggDomain::I64
                : framed_agg_domain(
                      src[row_m[perm[0]]]->columns[static_cast<size_t>(fs.arg_col)].view.type);
            bool is_float = (dom == AggDomain::F64);
            bool is_i128 = (dom == AggDomain::I128);
            bool is_str = (dom == AggDomain::Str);
            if (is_str) {
                // COUNT never reaches here (its domain is forced to I64 above — it
                // reads validity, never the value). SUM/AVG over a string have no
                // meaning; compiler.py rejects them by name and type at plan time,
                // and this is the backstop for any plan-construction path that
                // bypasses it — loud, never a garbage answer.
                if (fs.kind != WinAggFn::Min && fs.kind != WinAggFn::Max) {
                    err.code = 1;
                    err.msg = "FramedWindowSink: only MIN and MAX accept a string "
                              "window aggregate argument";
                    return;
                }
                res_row[f].assign(n, kNoRow);
            }

            std::vector<int64_t> lo(n), hi(n);
            for (size_t i = 0; i < n; ++i) {
                lo[i] = bound_pos(i, fs.frame.start, fs.frame.units, true);
                hi[i] = bound_pos(i, fs.frame.end, fs.frame.units, false);
            }

            auto row_at = [&](int64_t sorted_pos) -> std::pair<uint32_t, uint32_t> {
                uint32_t g = perm[static_cast<size_t>(sorted_pos)];
                return {row_m[g], row_r[g]};
            };

            if (fs.kind == WinAggFn::Min || fs.kind == WinAggFn::Max) {
                std::deque<int64_t> dq;   // holds SORTED positions, front = current extreme
                int64_t cur_lo = 0, cur_hi = -1;   // window is empty when cur_hi < cur_lo

                auto push_pos = [&](int64_t pos) {
                    auto [mi, ri] = row_at(pos);
                    const DrakenVector& v =
                        src[mi]->columns[static_cast<size_t>(fs.arg_col)].view;
                    if (!sort_row_valid(v, ri)) return;
                    if (is_str) {
                        FramedStrRef kv = framed_read_str(v, ri);
                        while (!dq.empty()) {
                            auto [bmi, bri] = row_at(dq.back());
                            FramedStrRef bk = framed_read_str(
                                src[bmi]->columns[static_cast<size_t>(fs.arg_col)].view, bri);
                            int c = framed_cmp_str(bk, kv);
                            bool pop = (fs.kind == WinAggFn::Min) ? (c >= 0) : (c <= 0);
                            if (!pop) break;
                            dq.pop_back();
                        }
                    } else if (is_i128) {
                        __int128 kv = agg2_read_i128(v, ri);
                        while (!dq.empty()) {
                            auto [bmi, bri] = row_at(dq.back());
                            __int128 bk = agg2_read_i128(
                                src[bmi]->columns[static_cast<size_t>(fs.arg_col)].view, bri);
                            bool pop = (fs.kind == WinAggFn::Min) ? (bk >= kv) : (bk <= kv);
                            if (!pop) break;
                            dq.pop_back();
                        }
                    } else {
                        uint64_t kv = sort_num_key(v, ri);
                        while (!dq.empty()) {
                            auto [bmi, bri] = row_at(dq.back());
                            uint64_t bk = sort_num_key(
                                src[bmi]->columns[static_cast<size_t>(fs.arg_col)].view, bri);
                            bool pop = (fs.kind == WinAggFn::Min) ? (bk >= kv) : (bk <= kv);
                            if (!pop) break;
                            dq.pop_back();
                        }
                    }
                    dq.push_back(pos);
                };

                for (size_t i = 0; i < n; ++i) {
                    if (i == 0 || part_start[i] != part_start[i - 1]) {
                        dq.clear();
                        cur_lo = part_start[i];
                        cur_hi = part_start[i] - 1;
                    }
                    while (cur_hi < hi[i]) { ++cur_hi; push_pos(cur_hi); }
                    while (cur_lo < lo[i]) {
                        if (!dq.empty() && dq.front() == cur_lo) dq.pop_front();
                        ++cur_lo;
                    }
                    if (lo[i] > hi[i] || dq.empty()) {
                        res_valid[f][i] = 0;
                        continue;
                    }
                    res_valid[f][i] = 1;
                    if (is_str) {
                        // Carry the winning SOURCE ROW, not the bytes: the emit
                        // already has to build a consolidated arena block, so
                        // copying the value here would copy it twice.
                        res_row[f][i] = perm[static_cast<size_t>(dq.front())];
                        continue;
                    }
                    auto [wmi, wri] = row_at(dq.front());
                    const DrakenVector& wv =
                        src[wmi]->columns[static_cast<size_t>(fs.arg_col)].view;
                    // The result lane MUST match the lane `emit_framed_column` reads
                    // for `fs.out_type` — i128 -> res_i128, FLOAT32/FLOAT64 -> res_f64,
                    // everything else -> res_i64. agg2_read_raw returns a float as the
                    // DOUBLE's BIT PATTERN in an int64 container, so the float arm has
                    // to decode it into res_f64; parking those bits in res_i64 left
                    // res_f64 untouched and every float MIN/MAX emitted 0.0.
                    if (is_i128) {
                        res_i128[f][i] = agg2_read_i128(wv, wri);
                    } else if (is_float) {
                        int64_t bits = agg2_read_raw(wv, wri, true);
                        double d;
                        std::memcpy(&d, &bits, sizeof(d));
                        res_f64[f][i] = d;
                    } else {
                        res_i64[f][i] = agg2_read_raw(wv, wri, false);
                    }
                }
                continue;
            }

            // SUM / COUNT / AVG — two-pointer running accumulation.
            int64_t  sum_i64 = 0;
            double   sum_f64 = 0.0;
            __int128 sum_i128 = 0;
            int64_t  count = 0;
            int64_t  cur_lo = 0, cur_hi = -1;

            auto touch = [&](int64_t pos, int sign) {
                if (fs.arg_col < 0) { count += sign; return; }   // COUNT(*)
                auto [mi, ri] = row_at(pos);
                const DrakenVector& v = src[mi]->columns[static_cast<size_t>(fs.arg_col)].view;
                if (!sort_row_valid(v, ri)) return;
                count += sign;
                if (is_i128) {
                    sum_i128 += sign * agg2_read_i128(v, ri);
                } else if (is_float) {
                    int64_t raw = agg2_read_raw(v, ri, true);
                    double d;
                    std::memcpy(&d, &raw, sizeof(d));
                    sum_f64 += sign * d;
                } else {
                    sum_i64 += sign * agg2_read_raw(v, ri, false);
                }
            };

            for (size_t i = 0; i < n; ++i) {
                if (i == 0 || part_start[i] != part_start[i - 1]) {
                    sum_i64 = 0; sum_f64 = 0.0; sum_i128 = 0; count = 0;
                    cur_lo = part_start[i];
                    cur_hi = part_start[i] - 1;
                }
                while (cur_hi < hi[i]) { ++cur_hi; touch(cur_hi, 1); }
                while (cur_lo < lo[i]) { touch(cur_lo, -1); ++cur_lo; }

                if (fs.kind == WinAggFn::Count) {
                    res_valid[f][i] = 1;
                    res_i64[f][i] = (lo[i] > hi[i]) ? 0 : count;
                    continue;
                }
                bool empty = (lo[i] > hi[i]) || count == 0;
                if (empty) { res_valid[f][i] = 0; continue; }
                res_valid[f][i] = 1;
                if (fs.kind == WinAggFn::Sum) {
                    if (is_i128) res_i128[f][i] = sum_i128;
                    else if (is_float) res_f64[f][i] = sum_f64;
                    else res_i64[f][i] = sum_i64;
                } else {   // Avg — always FLOAT64
                    double total;
                    if (is_i128) {
                        int scale = (fs.out_logical != nullptr &&
                                    fs.out_logical->kind == LogicalKind::DECIMAL)
                            ? fs.out_logical->scale : 0;
                        double divisor = 1.0;
                        for (int s = 0; s < scale; ++s) divisor *= 10.0;
                        total = static_cast<double>(sum_i128) / divisor;
                    } else if (is_float) {
                        total = sum_f64;
                    } else {
                        total = static_cast<double>(sum_i64);
                    }
                    res_f64[f][i] = total / static_cast<double>(count);
                }
            }
        }

        // Chunked gather + typed emit, mirroring WindowSink's chunk loop.
        const std::vector<std::string>& names = src.front()->names;
        size_t num_chunks = (n + chunk_rows - 1) / chunk_rows;
        std::vector<MorselPtr> chunk_out(num_chunks);

        unsigned hw = std::thread::hardware_concurrency();
        unsigned nt = hw > 2 ? static_cast<unsigned>(hw - 2) : 1u;
        if (nt > 16) nt = 16;
        if (nt > num_chunks) nt = static_cast<unsigned>(num_chunks);
        if (n < 200000) nt = 1;
        if (nt < 1) nt = 1;

        std::vector<ErrCtx> errs(nt);
        std::atomic<size_t> next_chunk{0};
        auto worker = [&](unsigned tid) {
            for (;;) {
                size_t ci = next_chunk.fetch_add(1);
                if (ci >= num_chunks) break;
                size_t start = ci * chunk_rows;
                size_t count_rows = std::min(chunk_rows, n - start);
                MorselPtr m = gather_rows(src, perm, start, count_rows, row_m, row_r,
                                          names, errs[tid], emit_ptr());
                if (errs[tid].code != 0) return;
                uint32_t cn = static_cast<uint32_t>(count_rows);
                for (size_t f = 0; f < nf; ++f) {
                    const FramedAggFnSpec& fs = funcs[f];
                    CxxColumn col = emit_framed_column(fs, res_valid[f], res_i64[f],
                                                       res_f64[f], res_i128[f],
                                                       res_row[f], src, row_m, row_r,
                                                       static_cast<uint32_t>(start), cn);
                    m->columns.push_back(std::move(col));
                    m->names.push_back(fs.name);
                }
                chunk_out[ci] = std::move(m);
            }
        };
        std::vector<std::thread> threads;
        threads.reserve(nt > 0 ? nt - 1 : 0);
        for (unsigned t = 1; t < nt; ++t) threads.emplace_back(worker, t);
        worker(0);
        for (std::thread& t : threads) t.join();
        for (ErrCtx& e : errs) {
            if (e.code != 0) { err = e; return; }
        }
        for (MorselPtr& m : chunk_out) {
            if (!out->append(m)) {
                err.code = 1;
                err.msg = out->error().c_str();
                return;
            }
        }
    }

private:
    // String MIN/MAX emit. `rowv[i]` is the GLOBAL row id (flatten_rows space) of
    // the winning value for output row i; the bytes are re-read from the source
    // morsels here. Two-pass build of ONE canonical consolidated block —
    // [DrakenStringArena header | slots[cn] | arena bytes] with `data` at the
    // header — the same layout and the same reasoning as sort.hpp's gather_rows
    // string arm (buffers.h contract). Inline slots carry their own bytes and are
    // copied whole; only out-of-line ones consume arena.
    static CxxColumn emit_framed_string_column(const FramedAggFnSpec& fs,
                                               const std::vector<uint8_t>& valid,
                                               const std::vector<uint32_t>& rowv,
                                               const std::vector<MorselPtr>& src,
                                               const std::vector<uint32_t>& row_m,
                                               const std::vector<uint32_t>& row_r,
                                               uint32_t start, uint32_t cn) {
        size_t col = static_cast<size_t>(fs.arg_col);
        auto win = [&](uint32_t i) -> const DrakenStringSlot* {
            uint32_t g = rowv[start + i];
            const DrakenVector& v = src[row_m[g]]->columns[col].view;
            return &string_arena_of(v)->slots[v.selection[row_r[g]]];
        };

        size_t total_arena = 0;
        bool any_null = false;
        for (uint32_t i = 0; i < cn; ++i) {
            if (!valid[start + i]) { any_null = true; continue; }
            const DrakenStringSlot* slot = win(i);
            if (!str_is_inline(slot)) total_arena += str_length(slot);
        }

        size_t vbytes = (static_cast<size_t>(cn) + 7) / 8;
        uint8_t* vbits = nullptr;
        if (any_null) {
            vbits = static_cast<uint8_t*>(draken_malloc(vbytes == 0 ? 1 : vbytes));
            std::memset(vbits, 0xFF, vbytes == 0 ? 1 : vbytes);
            for (uint32_t i = 0; i < cn; ++i) {
                if (!valid[start + i]) vbits[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7)));
            }
        }

        size_t slots_off = sizeof(DrakenStringArena);
        size_t arena_off = slots_off
            + static_cast<size_t>(cn == 0 ? 1 : cn) * sizeof(DrakenStringSlot);
        uint8_t* blk = static_cast<uint8_t*>(draken_malloc(arena_off + total_arena));
        auto* sa_out = reinterpret_cast<DrakenStringArena*>(blk);
        auto* dst = reinterpret_cast<DrakenStringSlot*>(blk + slots_off);
        uint8_t* out_arena = total_arena > 0 ? blk + arena_off : nullptr;
        sa_out->slots = dst;
        sa_out->arena = out_arena;
        sa_out->length = cn;
        sa_out->arena_used = total_arena;
        sa_out->arena_cap = total_arena;
        sa_out->null_bitmap = nullptr;
        sa_out->owns_buffers = 0;   // the VectorOwner frees the one block
        sa_out->payloads_elided = 0;
        sa_out->type = fs.out_type;

        size_t arena_pos = 0;
        for (uint32_t i = 0; i < cn; ++i) {
            if (!valid[start + i]) {
                std::memset(&dst[i], 0, sizeof(DrakenStringSlot));
                continue;
            }
            uint32_t g = rowv[start + i];
            const DrakenVector& v = src[row_m[g]]->columns[col].view;
            const DrakenStringArena* sa = string_arena_of(v);
            const DrakenStringSlot* slot = &sa->slots[v.selection[row_r[g]]];
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
            draken_malloc((cn == 0 ? 1u : cn) * sizeof(uint32_t)));
        for (uint32_t i = 0; i < cn; ++i) sel[i] = i;
        DrakenVector v;
        v.data = sa_out; v.selection = sel; v.data_length = cn; v.length = cn;
        v.validity = vbits; v.type = fs.out_type;
        v.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
        CxxColumn c;
        c.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(blk),
                                              OwnedBuffer<uint8_t>(vbits),
                                              OwnedBuffer<void>(sel));
        c.own->logical_type = fs.out_logical;
        c.view = c.own->vec;
        return c;
    }

    // Build one output column, [start, start+cn) of the full per-row result arrays,
    // typed per `fs.out_type`. NULL where `valid[i] == 0`.
    static CxxColumn emit_framed_column(const FramedAggFnSpec& fs,
                                        const std::vector<uint8_t>& valid,
                                        const std::vector<int64_t>& i64v,
                                        const std::vector<double>& f64v,
                                        const std::vector<__int128>& i128v,
                                        const std::vector<uint32_t>& rowv,
                                        const std::vector<MorselPtr>& src,
                                        const std::vector<uint32_t>& row_m,
                                        const std::vector<uint32_t>& row_r,
                                        uint32_t start, uint32_t cn) {
        if (sort_type_is_string(fs.out_type)) {
            return emit_framed_string_column(fs, valid, rowv, src, row_m, row_r, start, cn);
        }
        size_t alloc_n = (cn == 0 ? 1 : cn);
        size_t vbytes = (static_cast<size_t>(cn) + 7) / 8;
        uint8_t* vbits = nullptr;
        bool any_null = false;
        for (uint32_t i = 0; i < cn; ++i) if (!valid[start + i]) { any_null = true; break; }
        if (any_null) {
            vbits = static_cast<uint8_t*>(draken_malloc(vbytes == 0 ? 1 : vbytes));
            std::memset(vbits, 0xFF, vbytes == 0 ? 1 : vbytes);
            for (uint32_t i = 0; i < cn; ++i) {
                if (!valid[start + i]) vbits[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7)));
            }
        }
        uint32_t* sel = static_cast<uint32_t*>(draken_malloc(alloc_n * sizeof(uint32_t)));
        for (uint32_t i = 0; i < cn; ++i) sel[i] = i;

        void* data = nullptr;
        if (fs.out_type == DRAKEN_DECIMAL128) {
            auto* d = static_cast<uint8_t*>(draken_malloc(alloc_n * 16u));
            for (uint32_t i = 0; i < cn; ++i) {
                __int128 vv = valid[start + i] ? i128v[start + i] : 0;
                std::memcpy(d + static_cast<size_t>(i) * 16u, &vv, 16u);
            }
            data = d;
        } else if (fs.out_type == DRAKEN_FLOAT64) {
            auto* d = static_cast<double*>(draken_malloc(alloc_n * sizeof(double)));
            for (uint32_t i = 0; i < cn; ++i) d[i] = valid[start + i] ? f64v[start + i] : 0.0;
            data = d;
        } else if (fs.out_type == DRAKEN_FLOAT32) {
            auto* d = static_cast<float*>(draken_malloc(alloc_n * sizeof(float)));
            for (uint32_t i = 0; i < cn; ++i)
                d[i] = valid[start + i] ? static_cast<float>(f64v[start + i]) : 0.0f;
            data = d;
        } else if (fs.out_type == DRAKEN_BOOL) {
            // 1 bit per row (not a flat byte array) — MIN/MAX(bool) passthrough only;
            // SUM/COUNT/AVG never emit BOOL (SUM(bool) promotes to INT64 per
            // `_aggregate_return_type`'s INTEGER-category rule — BOOL is not that
            // category, so it in fact passes through too, but as 0/1 in an INT64
            // container by the SAME int64 case below; this arm is MIN/MAX only).
            size_t bbytes = vbytes == 0 ? 1 : vbytes;
            auto* d = static_cast<uint8_t*>(draken_malloc(bbytes));
            std::memset(d, 0, bbytes);
            for (uint32_t i = 0; i < cn; ++i) {
                if (valid[start + i] && i64v[start + i] != 0) d[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
            }
            data = d;
        } else {
            // Every other fixed-width physical type — INT8/16/32/64, UINT8/16/32/64,
            // DATE32, TIME32/64, TIMESTAMP64, DECIMAL (int64-backed) — MIN/MAX
            // passthrough's only possible outputs beyond the arms above. `i64v` holds
            // the value in agg2_read_raw's convention (sign-extended for signed
            // narrower types, zero-extended for unsigned, the raw bit pattern for
            // UINT64/INT64/DECIMAL/TIMESTAMP64/TIME64) — narrowed here to the
            // OUTPUT type's actual byte width. Writing every result as a bare
            // int64_t regardless of `fs.out_type`'s declared width was the original
            // bug this arm exists to avoid — it let an INT8 column's reader walk
            // past its own element boundary into its neighbours' bytes (7 zero
            // bytes per element for an INT8 column of int64-sized slots), which
            // is exactly the "every row past the first reads 0" defect this
            // comment is now pinned next to.
            size_t itemsize = draken_type_fixed_itemsize(fs.out_type);
            if (itemsize == 0) itemsize = 8u;   // defensive: treat as INT64-width
            auto* d = static_cast<uint8_t*>(draken_malloc(alloc_n * itemsize));
            for (uint32_t i = 0; i < cn; ++i) {
                int64_t raw = valid[start + i] ? i64v[start + i] : 0;
                std::memcpy(d + static_cast<size_t>(i) * itemsize, &raw, itemsize);
            }
            data = d;
        }

        DrakenVector v;
        v.data = data; v.selection = sel; v.data_length = cn; v.length = cn;
        v.validity = vbits; v.type = fs.out_type;
        v.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
        CxxColumn c;
        c.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(data),
                                              OwnedBuffer<uint8_t>(vbits), OwnedBuffer<void>(sel));
        c.own->logical_type = fs.out_logical;
        c.view = c.own->vec;
        return c;
    }
};

}  // namespace opteryx::engine
