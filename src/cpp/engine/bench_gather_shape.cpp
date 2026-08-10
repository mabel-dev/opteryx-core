// src/cpp/engine/bench_gather_shape.cpp — what a compression-aware (dict-shaped)
// post-transformation morsel emit would be worth, versus the dense emit we build today.
//
// THE QUESTION. Every morsel built after the scan is dense: `gather_rows`
// (draken/morsels/sort.hpp) — the single output builder for joins, sort, TopN,
// window and LIMIT slicing — ends every type arm with `sel[i] = i`,
// `data_length == length`, and for strings a full two-pass arena rebuild that
// copies one physical value per OUTPUT row. The alternative is to emit the
// output as a dict: concatenate the source morsels' physical values ONCE and
// give each output morsel nothing but rebased uint32 codes over that block.
//
// The two shapes cost differently in a way that is entirely predictable from
// the inputs, which is why this benchmark sweeps exactly two axes:
//
//   fanout  = out_rows / distinct source rows touched.  A join gather replicates
//             (fanout > 1); a sort gather is a permutation (fanout == 1).
//   uniq    = physical values per source row.  A dict-encoded source column
//             (uniq << rows) is already compressed and the dense gather EXPANDS it.
//
// Per output row the dense emit writes a 16-byte DrakenStringSlot plus `len`
// arena bytes when len > STR_INLINE_MAX(12), plus a 4-byte identity selection
// entry. The dict emit writes 4 bytes. So the ceiling on the saving is
// (16 + max(0, len) - 4) bytes/row against a one-time concat — but a ceiling on
// bytes is not a ceiling on time, because the gather is a random-access read out
// of the source and may be latency-bound rather than write-bandwidth-bound.
// Hence: measure.
//
// MATCHED WRAPPER (feedback_microbench_matched_wrappers). Both arms run through
// the identical driver: same chunking, same per-chunk allocation of their own
// buffers, same rolling release, same untimed downstream checksum. The ONLY
// difference inside the timed region is which emit function builds the chunk.
// Arm B pays its one-time concat INSIDE its timed region.
//
// The downstream checksum is reported separately and deliberately: a dict output
// is cheaper to build and more indirect to read, so `gather + one full downstream
// scan` is the only honest total. Both arms checksum through the uniform
// data[selection[i]] path — the same code — so the difference there is the real
// cache behaviour of the shape, not wrapper skew.
//
// A/B is INTERLEAVED per repetition (benchmark_thermal_drift_requires_interleaved_ab)
// and reported as best-of, so a warming machine cannot manufacture a delta.
//
// Standalone assert()-based benchmark, same pattern as bench_join_csr_lookup.cpp —
// this repo has no C++ test framework. Not wired into CI; run by hand.
//
// Build & run:
//   g++ -O2 -std=c++20 -Idraken -Isrc/cpp -Ithird_party/cyan4973 -pthread \
//       src/cpp/engine/bench_gather_shape.cpp draken/core/vector_alloc.cpp \
//       -o /tmp/bench_gather_shape && /tmp/bench_gather_shape

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include "morsels/sort.hpp"  // the REAL gather_rows, plus CxxMorsel/CxxColumn/VectorOwner

namespace {

using Clock = std::chrono::steady_clock;

double ms_since(Clock::time_point t0) {
    return std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
}

// ---------------------------------------------------------------------------
// Source construction
// ---------------------------------------------------------------------------
// One string column of `rows` logical rows over `uniq` physical values of
// `slen` bytes. uniq == rows builds the DENSE shape the scan hands us for a
// high-cardinality column (identity selection); uniq < rows builds the DICT
// shape it hands us for a low-cardinality one (owned codes). Layout is the
// canonical consolidated block gather_rows itself emits:
//   [DrakenStringArena header | slots[uniq] | arena bytes]

CxxColumn make_string_col(uint32_t rows, uint32_t uniq, uint32_t slen, uint64_t seed) {
    std::mt19937_64 rng(seed);
    const bool extern_str = slen > STR_INLINE_MAX;
    const size_t arena_bytes = extern_str ? static_cast<size_t>(uniq) * slen : 0;
    const size_t slots_off = sizeof(DrakenStringArena);
    const size_t arena_off = slots_off + static_cast<size_t>(uniq) * sizeof(DrakenStringSlot);

    uint8_t* blk = static_cast<uint8_t*>(draken_malloc(arena_off + arena_bytes));
    auto* sa = reinterpret_cast<DrakenStringArena*>(blk);
    auto* slots = reinterpret_cast<DrakenStringSlot*>(blk + slots_off);
    uint8_t* arena = arena_bytes > 0 ? blk + arena_off : nullptr;
    sa->slots = slots;
    sa->arena = arena;
    sa->length = uniq;
    sa->arena_used = arena_bytes;
    sa->arena_cap = arena_bytes;
    sa->null_bitmap = nullptr;
    sa->owns_buffers = 0;
    sa->payloads_elided = 0;
    sa->type = DRAKEN_VARCHAR;

    std::string tmp(slen, 'a');
    for (uint32_t u = 0; u < uniq; ++u) {
        for (uint32_t b = 0; b < slen; ++b) tmp[b] = static_cast<char>('0' + ((u + b * 7 + rng()) & 31));
        const auto* bytes = reinterpret_cast<const uint8_t*>(tmp.data());
        if (!extern_str) {
            str_init_inline(&slots[u], bytes, slen);
        } else {
            size_t off = static_cast<size_t>(u) * slen;
            std::memcpy(arena + off, bytes, slen);
            str_init_extern(&slots[u], arena + off, slen, static_cast<uint32_t>(off));
        }
    }

    uint32_t* sel = static_cast<uint32_t*>(draken_malloc(static_cast<size_t>(rows) * sizeof(uint32_t)));
    if (uniq == rows) {
        for (uint32_t i = 0; i < rows; ++i) sel[i] = i;
    } else {
        for (uint32_t i = 0; i < rows; ++i) sel[i] = static_cast<uint32_t>(rng() % uniq);
    }

    DrakenVector v;
    v.data = sa;
    v.selection = sel;
    v.data_length = uniq;
    v.length = rows;
    v.validity = nullptr;
    v.type = DRAKEN_VARCHAR;
    v.flags = (uniq == rows) ? (DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION) : 0;
    CxxColumn c;
    c.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(blk), OwnedBuffer<uint8_t>(nullptr),
                                          OwnedBuffer<void>(sel));
    c.view = c.own->vec;
    return c;
}

// Fixed-width twin of the above — the non-string story (8-byte payload vs a
// 4-byte code is a 2x ceiling, not a 22x one, and that difference is the point).
CxxColumn make_int_col(uint32_t rows, uint32_t uniq, uint64_t seed) {
    std::mt19937_64 rng(seed);
    int64_t* data = static_cast<int64_t*>(draken_malloc(static_cast<size_t>(uniq) * sizeof(int64_t)));
    for (uint32_t u = 0; u < uniq; ++u) data[u] = static_cast<int64_t>(rng());
    uint32_t* sel = static_cast<uint32_t*>(draken_malloc(static_cast<size_t>(rows) * sizeof(uint32_t)));
    if (uniq == rows) {
        for (uint32_t i = 0; i < rows; ++i) sel[i] = i;
    } else {
        for (uint32_t i = 0; i < rows; ++i) sel[i] = static_cast<uint32_t>(rng() % uniq);
    }
    DrakenVector v;
    v.data = data;
    v.selection = sel;
    v.data_length = uniq;
    v.length = rows;
    v.validity = nullptr;
    v.type = DRAKEN_INT64;
    v.flags = (uniq == rows) ? (DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION) : 0;
    CxxColumn c;
    c.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(data), OwnedBuffer<uint8_t>(nullptr),
                                          OwnedBuffer<void>(sel));
    c.view = c.own->vec;
    return c;
}

// ---------------------------------------------------------------------------
// Arm B: the shape-preserving (dict) emit
// ---------------------------------------------------------------------------
// One concatenated physical block across all source morsels, built once; each
// output morsel is then just rebased uint32 codes over it. This is the whole
// proposal, in the smallest form that is a real DrakenVector.
//
// The concat is what makes it possible at all: a DrakenVector has ONE `data`
// pointer, and gather_rows reads from a VECTOR of source morsels, so a dict
// output cannot simply alias one source's buffer.

struct Concat {
    uint8_t* blk = nullptr;               // owned here; shared by every output chunk
    uint32_t total = 0;                   // physical values across all sources
    std::vector<uint32_t> base;           // per-source-morsel code base
    ~Concat() { if (blk) draken_free(blk); }
};

void build_concat_string(const std::vector<MorselPtr>& ms, size_t ci, Concat& out) {
    out.base.assign(ms.size(), 0);
    uint32_t total = 0;
    size_t arena_bytes = 0;
    for (size_t m = 0; m < ms.size(); ++m) {
        const DrakenVector& v = ms[m]->columns[ci].view;
        out.base[m] = total;
        total += v.data_length;
        const auto* sa = static_cast<const DrakenStringArena*>(v.data);
        arena_bytes += sa->arena_used;
    }
    size_t slots_off = sizeof(DrakenStringArena);
    size_t arena_off = slots_off + static_cast<size_t>(total) * sizeof(DrakenStringSlot);
    uint8_t* blk = static_cast<uint8_t*>(draken_malloc(arena_off + arena_bytes));
    auto* sa_out = reinterpret_cast<DrakenStringArena*>(blk);
    auto* dst = reinterpret_cast<DrakenStringSlot*>(blk + slots_off);
    uint8_t* arena = arena_bytes > 0 ? blk + arena_off : nullptr;
    sa_out->slots = dst;
    sa_out->arena = arena;
    sa_out->length = total;
    sa_out->arena_used = arena_bytes;
    sa_out->arena_cap = arena_bytes;
    sa_out->null_bitmap = nullptr;
    sa_out->owns_buffers = 0;
    sa_out->payloads_elided = 0;
    sa_out->type = DRAKEN_VARCHAR;

    size_t apos = 0;
    for (size_t m = 0; m < ms.size(); ++m) {
        const DrakenVector& v = ms[m]->columns[ci].view;
        const auto* sa = static_cast<const DrakenStringArena*>(v.data);
        for (uint32_t u = 0; u < v.data_length; ++u) {
            const DrakenStringSlot* s = &sa->slots[u];
            if (str_is_inline(s)) {
                dst[out.base[m] + u] = *s;
            } else {
                uint32_t len = str_length(s);
                std::memcpy(arena + apos, str_data(s, sa->arena), len);
                str_clone_with_offset(&dst[out.base[m] + u], s, static_cast<uint32_t>(apos));
                apos += len;
            }
        }
    }
    out.blk = blk;
    out.total = total;
}

void build_concat_int(const std::vector<MorselPtr>& ms, size_t ci, Concat& out) {
    out.base.assign(ms.size(), 0);
    uint32_t total = 0;
    for (size_t m = 0; m < ms.size(); ++m) {
        out.base[m] = total;
        total += ms[m]->columns[ci].view.data_length;
    }
    auto* blk = static_cast<int64_t*>(draken_malloc(static_cast<size_t>(total) * sizeof(int64_t)));
    for (size_t m = 0; m < ms.size(); ++m) {
        const DrakenVector& v = ms[m]->columns[ci].view;
        std::memcpy(blk + out.base[m], v.data, static_cast<size_t>(v.data_length) * sizeof(int64_t));
    }
    out.blk = reinterpret_cast<uint8_t*>(blk);
    out.total = total;
}

// The per-chunk half: rebase each output row's source physical index into the
// concatenated block. This is the entire per-row cost of arm B.
MorselPtr gather_dict(const std::vector<MorselPtr>& ms, size_t ci, const Concat& cc,
                      const std::vector<uint32_t>& order, size_t first, size_t count,
                      const std::vector<uint32_t>& row_m, const std::vector<uint32_t>& row_r,
                      DrakenType t) {
    uint32_t n = static_cast<uint32_t>(count);
    auto out = std::make_shared<CxxMorsel>();
    out->zero_col_rows = n;
    uint32_t* codes = static_cast<uint32_t*>(draken_malloc((n == 0 ? 1 : n) * sizeof(uint32_t)));
    for (uint32_t i = 0; i < n; ++i) {
        uint32_t g = order[first + i];
        uint32_t m = row_m[g];
        codes[i] = cc.base[m] + ms[m]->columns[ci].view.selection[row_r[g]];
    }
    DrakenVector v;
    v.data = cc.blk;
    v.selection = codes;
    v.data_length = cc.total;
    v.length = n;
    v.validity = nullptr;
    v.type = t;
    v.flags = 0;
    CxxColumn c;
    // data_buf is deliberately non-owning: the concatenated block is SHARED by every
    // output chunk. In production that would be a shared_ptr on the owner; here the
    // driver owns it. The per-chunk allocation — the only per-chunk cost — is `codes`.
    c.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(nullptr), OwnedBuffer<uint8_t>(nullptr),
                                          OwnedBuffer<void>(codes));
    c.view = c.own->vec;
    out->columns.push_back(std::move(c));
    out->names.push_back("c");
    return out;
}

// ---------------------------------------------------------------------------
// Downstream read — identical uniform data[selection[i]] path for both arms.
// Untimed against the gather; reported on its own line.
// ---------------------------------------------------------------------------
uint64_t checksum(const MorselPtr& m, DrakenType t) {
    const DrakenVector& v = m->columns[0].view;
    uint64_t h = 0;
    if (t == DRAKEN_INT64) {
        const auto* d = static_cast<const int64_t*>(v.data);
        for (uint32_t i = 0; i < v.length; ++i) h += static_cast<uint64_t>(d[v.selection[i]]);
    } else {
        const auto* sa = static_cast<const DrakenStringArena*>(v.data);
        for (uint32_t i = 0; i < v.length; ++i) {
            const DrakenStringSlot* s = &sa->slots[v.selection[i]];
            uint32_t len = str_length(s);
            h += len + *str_data(s, sa->arena);
        }
    }
    return h;
}

// ---------------------------------------------------------------------------
// The case table
// ---------------------------------------------------------------------------

struct Case {
    const char* group;
    const char* name;
    uint32_t src_morsels;
    uint32_t rows_per_morsel;
    uint32_t uniq_per_morsel;  // == rows_per_morsel => dense source
    uint32_t slen;             // 0 => INT64 column
    uint64_t out_rows;
    bool permutation;          // true: sort (fanout 1). false: join (random, replicated)
};

constexpr uint32_t kChunk = 64 * 1024;  // engine-scale output morsel

struct Result {
    double gather_ms;
    double scan_ms;
    size_t peak_bytes;  // bytes live in output buffers at the high-water chunk
};

Result run_arm(bool dict, const Case& cs, const std::vector<MorselPtr>& ms,
               const std::vector<uint32_t>& order, const std::vector<uint32_t>& row_m,
               const std::vector<uint32_t>& row_r, uint64_t& sink) {
    const DrakenType t = cs.slen == 0 ? DRAKEN_INT64 : DRAKEN_VARCHAR;
    const std::vector<std::string> names{"c"};
    double gather_ms = 0.0, scan_ms = 0.0;
    size_t peak = 0;

    Concat cc;
    auto t_all = Clock::now();
    if (dict) {
        if (t == DRAKEN_INT64) build_concat_int(ms, 0, cc);
        else build_concat_string(ms, 0, cc);
        peak += (t == DRAKEN_INT64)
                    ? static_cast<size_t>(cc.total) * sizeof(int64_t)
                    : sizeof(DrakenStringArena) + static_cast<size_t>(cc.total) * sizeof(DrakenStringSlot)
                          + static_cast<size_t>(cc.total) * (cs.slen > STR_INLINE_MAX ? cs.slen : 0);
    }
    gather_ms += ms_since(t_all);

    for (size_t first = 0; first < cs.out_rows; first += kChunk) {
        size_t count = std::min<size_t>(kChunk, cs.out_rows - first);
        ErrCtx err;
        auto t0 = Clock::now();
        MorselPtr m = dict ? gather_dict(ms, 0, cc, order, first, count, row_m, row_r, t)
                           : gather_rows(ms, order, first, count, row_m, row_r, names, err);
        gather_ms += ms_since(t0);
        if (!dict && err.code != 0) { std::fprintf(stderr, "gather_rows: %s\n", err.msg); std::abort(); }
        auto t1 = Clock::now();
        sink += checksum(m, t);
        scan_ms += ms_since(t1);
        if (first == 0) {
            // High-water for ONE resident output chunk — the streaming-consumer model
            // both arms are driven under.
            peak += dict ? count * sizeof(uint32_t)
                         : count * (sizeof(uint32_t) + (t == DRAKEN_INT64
                                                            ? sizeof(int64_t)
                                                            : sizeof(DrakenStringSlot)
                                                                  + (cs.slen > STR_INLINE_MAX ? cs.slen : 0)));
        }
        m.reset();
    }
    return {gather_ms, scan_ms, peak};
}

void run_case(const Case& cs, int reps) {
    const DrakenType t = cs.slen == 0 ? DRAKEN_INT64 : DRAKEN_VARCHAR;

    std::vector<MorselPtr> ms;
    ms.reserve(cs.src_morsels);
    for (uint32_t m = 0; m < cs.src_morsels; ++m) {
        auto mo = std::make_shared<CxxMorsel>();
        mo->columns.push_back(cs.slen == 0
                                  ? make_int_col(cs.rows_per_morsel, cs.uniq_per_morsel, 1234 + m)
                                  : make_string_col(cs.rows_per_morsel, cs.uniq_per_morsel, cs.slen, 1234 + m));
        mo->names.push_back("c");
        ms.push_back(std::move(mo));
    }

    const size_t total_rows = static_cast<size_t>(cs.src_morsels) * cs.rows_per_morsel;
    std::vector<uint32_t> row_m(total_rows), row_r(total_rows);
    for (size_t g = 0; g < total_rows; ++g) {
        row_m[g] = static_cast<uint32_t>(g / cs.rows_per_morsel);
        row_r[g] = static_cast<uint32_t>(g % cs.rows_per_morsel);
    }

    std::vector<uint32_t> order(cs.out_rows);
    std::mt19937_64 rng(99);
    if (cs.permutation) {
        std::iota(order.begin(), order.end(), 0u);
        std::shuffle(order.begin(), order.end(), rng);
    } else {
        for (size_t i = 0; i < cs.out_rows; ++i) order[i] = static_cast<uint32_t>(rng() % total_rows);
    }

    uint64_t sink = 0;
    Result best_a{1e18, 1e18, 0}, best_b{1e18, 1e18, 0};
    for (int r = 0; r < reps; ++r) {
        // INTERLEAVED per repetition — a warming machine cannot favour one arm.
        Result a = run_arm(false, cs, ms, order, row_m, row_r, sink);
        Result b = run_arm(true, cs, ms, order, row_m, row_r, sink);
        if (a.gather_ms < best_a.gather_ms) best_a = a;
        if (b.gather_ms < best_b.gather_ms) best_b = b;
    }

    double fanout = static_cast<double>(cs.out_rows) / static_cast<double>(total_rows);
    double a_tot = best_a.gather_ms + best_a.scan_ms;
    double b_tot = best_b.gather_ms + best_b.scan_ms;
    std::printf("%-10s %-22s %6s uniq/row=%-7.4f fanout=%-6.2f | "
                "gather %8.1f -> %8.1f ms (%5.2fx) | +scan %8.1f -> %8.1f ms (%5.2fx) | "
                "chunk-mem %7.1f -> %6.1f MB (%5.2fx) | sink=%llu\n",
                cs.group, cs.name, t == DRAKEN_INT64 ? "int64" : "str",
                static_cast<double>(cs.uniq_per_morsel) / cs.rows_per_morsel, fanout,
                best_a.gather_ms, best_b.gather_ms, best_a.gather_ms / best_b.gather_ms,
                a_tot, b_tot, a_tot / b_tot,
                best_a.peak_bytes / 1048576.0, best_b.peak_bytes / 1048576.0,
                static_cast<double>(best_a.peak_bytes) / static_cast<double>(best_b.peak_bytes),
                static_cast<unsigned long long>(sink));
    std::fflush(stdout);
}

}  // namespace

int main(int argc, char** argv) {
    int reps = argc > 1 ? std::atoi(argv[1]) : 3;
    std::printf("gather shape A/B — arm A = production gather_rows (dense), "
                "arm B = dict emit over one concatenated block\n");
    std::printf("reps=%d, output morsel = %u rows, best-of, interleaved\n\n", reps, kChunk);

    // ---- ORDER BY: pure permutation, fanout == 1 -------------------------------
    // TPC-H itself cannot show this (every TPC-H ORDER BY is post-aggregation and
    // tiny — Heap Sort self-time is ~0% of Q3/Q10/Q18), so these are the extremes.
    std::printf("-- ORDER BY (permutation, fanout 1) --\n");
    // DEGENERATE: every row a distinct 72-byte string. The concat copies exactly
    // what the dense gather would have copied, then pays 4 bytes/row on top.
    run_case({"orderby", "degenerate all-distinct", 32, 65536, 65536, 72, 32ull * 65536, true}, reps);
    // Mid: a 50:1 dictionary, the shape a medium-cardinality string column arrives in.
    run_case({"orderby", "dict 50:1", 32, 65536, 1310, 72, 32ull * 65536, true}, reps);
    // PERFECT: 8 distinct wide values — the dense gather expands a 576-byte dictionary
    // into 180 MB of output.
    run_case({"orderby", "perfect dict 8 vals", 32, 65536, 8, 72, 32ull * 65536, true}, reps);
    // Short strings live INLINE in the 16-byte slot, so there are no arena bytes to
    // save and the ceiling collapses from (16+len-4) to (16-4).
    run_case({"orderby", "perfect, inline (7B)", 32, 65536, 7, 7, 32ull * 65536, true}, reps);
    run_case({"orderby", "degenerate int64", 32, 65536, 65536, 0, 32ull * 65536, true}, reps);

    // ---- JOIN: replicating gather ----------------------------------------------
    // Output size held constant at 2M rows; fanout varies by shrinking the build side.
    // This is the axis that matters, because replication is what the dense emit pays
    // for and the dict emit does not.
    std::printf("\n-- JOIN (replicating gather, 2M output rows) --\n");
    constexpr uint64_t kOut = 2ull * 1024 * 1024;
    // DEGENERATE: fanout 1, all-distinct build side. Dict cannot win: same bytes copied.
    run_case({"join", "degenerate fanout 1", 32, 65536, 65536, 72, kOut, false}, reps);
    run_case({"join", "fanout 4  (c_comment)", 8, 65536, 65536, 72, kOut, false}, reps);
    run_case({"join", "fanout 16 (c_comment)", 2, 65536, 65536, 72, kOut, false}, reps);
    // PERFECT: a small build side replicated hard — the shape of a dimension join.
    run_case({"join", "fanout 256 (c_comment)", 1, 8192, 8192, 72, kOut, false}, reps);
    // TPC-H Q18 carries c_name (18 bytes, unique per customer) through the join.
    run_case({"join", "fanout 16 (c_name 18B)", 2, 65536, 65536, 18, kOut, false}, reps);
    // A short/inline carried string (l_shipmode) and a fixed-width carried key —
    // the two cases where the ceiling is structurally small.
    run_case({"join", "fanout 16, inline (7B)", 2, 65536, 7, 7, kOut, false}, reps);
    run_case({"join", "fanout 16, int64", 2, 65536, 65536, 0, kOut, false}, reps);
    return 0;
}
