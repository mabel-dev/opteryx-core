#pragma once
// src/cpp/engine/native_group_sinks.hpp — the engine's general aggregation and
// dedup breakers: UngroupedAggSink (COUNT(*)/COUNT/SUM/AVG/MIN/MAX/STDDEV/
// STDDEV_SAMP/VAR_POP/VAR_SAMP/MEDIAN, any mix), GroupBySink (multi-key,
// string keys, NULL-key groups), DistinctSink.
//
// Semantics (SQL, not demo shortcuts):
//   - COUNT(*) counts rows; COUNT(col) counts non-NULL values.
//   - SUM over integer-family/DECIMAL operands accumulates EXACT int64 (never a
//     double round-trip); float operands accumulate double. SUM/AVG/MIN/MAX over
//     zero valid values is NULL. AVG is FLOAT64.
//   - STDDEV (== STDDEV_POP) is POPULATION stddev (N denominator); STDDEV_SAMP
//     is SAMPLE stddev (N-1 denominator, Bessel's correction); VAR_POP/VAR_SAMP
//     are their pre-sqrt variances. All four are always FLOAT64, accumulated
//     from the SAME Σx/Σx²/count lanes (agg2_update_stddev — no exactness
//     requirement, unlike SUM/AVG — always double regardless of int or float
//     operand); only the finalize formula (emit_lane_column) differs. The N-1
//     forms are undefined below 2 valid rows and emit NULL, matching DuckDB/the
//     SQL standard — the N forms are instead defined (0) at exactly 1 valid row.
//     DECIMAL operands are rejected on all four (CAST to DOUBLE first): reading
//     the unscaled raw integer as a double would silently compute the wrong
//     numbers' variance.
//   - MEDIAN buffers every non-null value per group (MedianState — see
//     _agg_kernels.hpp), bounded by a global 512MB byte budget across all
//     groups (fails loud past the budget: use APPROX_PERCENTILE for larger
//     inputs), and computes the exact median via std::nth_element at
//     finalize (even counts interpolate). Always FLOAT64, numeric-only
//     (unlike STDDEV/SUM/AVG — no BOOL/DATE32/TIMESTAMP64/TIME32/TIME64/
//     DECIMAL; see median_operand_supported for why). NaN participates as a
//     value ranked above everything else, per draken's total order.
//   - MIN/MAX compare via the same normalized order keys the sort uses
//     (native_sort.hpp): NULLs skipped, NaN highest, -0.0 == +0.0; the OUTPUT is
//     the raw value at the operand's own type/width, logical descriptor carried.
//   - GROUP BY keys: any fixed-width type (raw-byte equality — draken canonicalizes
//     -0.0/NaN at storage) or strings (byte equality). A NULL key is a real group.
//   - DISTINCT dedups on serialized row keys, keeps FIRST-SEEN rows, emits them
//     via the engine's one row gather.
//
// Per-worker state is lock-free; combine merges under one mutex per worker;
// finalize materializes into a MorselBuffer. Anything outside scope sets ErrCtx —
// fail loud, never a silent wrong answer.

#include <algorithm>
#include <array>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "operator.hpp"
#include "pipeline_buffers.hpp"
#include "groupby_tel.hpp"       // diagnostic hash/probe/apply phase timing (GroupBySink::sink)
#include "native_sort.hpp"       // sort_num_key, sort_row_valid, sort_type_is_string,
                                 // string_arena_of, gather_elem_size, gather_rows,
                                 // make canonical string blocks
#include "core/string_slot.h"
#include "core/vector_owner.h"
#include "logical_type.h"        // LogicalType definition (owner only forward-declares it)
#include "xxhash.h"              // XXH3_64bits — long-slot hash32 (same as draken's builders)
#include "morsels/cxx_hash.h"    // cxx_hash_c — draken owns the key hash (DISTINCT/GROUP BY)
#include "carchar_set.hpp"       // opteryx::carchar::CarcharSet — hash-identity dedup set
#include "carchar_index.hpp"     // opteryx::carchar::CarcharIndex — hash → group-id
#include "native_cidr_emit.hpp"  // CIDR_AGG: Roaring32 address sets + minimal-cover emit
#include "medius.hpp"            // opteryx::medius::MediusMap — bounded middle tier
#include "parvi.hpp"             // opteryx::parvi::ParviMap — 64-slot low-card front map
#include "native_key_hash.hpp"     // compute_row_hashes — draken owns the key hash
#include "_agg_kernels.hpp"      // opteryx::ungrouped::MedianState — MEDIAN's per-group
                                 // value buffer, and the global byte budget that bounds it
#include "hllpp.h"               // HllppSketch — APPROX_COUNT_DISTINCT, global namespace
#include "tdigest.h"             // td_histogram_t — APPROX_PERCENTILE, C API (third_party/tdigest-c)

namespace opteryx::engine {

// ---- GROUP BY per-group key store ------------------------------------------
// Row-store stride for one GROUP BY key value. BOOL and the string family are
// resolved here (their stride differs from draken's raw vector width); everything
// else delegates to draken_type_itemsize so this can't drift from
// gather_elem_size/concat_fixed_itemsize. `lt` is the column's logical descriptor:
// only VECTOR_FP16 reads it (stride = dimension × 2), every other type ignores it.
//
// A restricted type set is CORRECT here, unlike for a carried payload: a GROUP BY
// key must hash and compare, so eligibility is checked separately by
// sort_key_type_supported (see capture()). This machinery served the join's build
// payload until the join stopped copying values (native_join2.hpp retains its
// columns and emits via gather_rows) — GROUP BY is now its only consumer.
inline size_t gb_key_elem_size(DrakenType t, const LogicalType* lt) {
    if (t == DRAKEN_BOOL) return 1;
    if (draken_type_is_string_storage(t)) return sizeof(DrakenStringSlot);
    return draken_type_itemsize(t, lt);
}

// (join_key_type_supported / join_read_key removed — the key is now hashed by
// draken via cxx_hash_c; nullness is read from the key column, and equality is
// 64-bit hash identity. draken hashes any supported key type, so there is no
// integer-only gate here anymore.)

// Arena-backed storage, i.e. "this row-store slot is a DrakenStringSlot and its
// long-form bytes need rebasing on every hop". A payload column is only being
// CARRIED, never compared, so this is the storage predicate (VARIANT included) —
// not sort_type_is_string, which answers the narrower "has a defined collation".
inline bool gb_key_is_string(DrakenType t) {
    return draken_type_is_string_storage(t);
}

inline bool gb_key_is_bool(DrakenType t) {
    return t == DRAKEN_BOOL;
}

// Read the bit for physical element `phys` out of a bit-packed BOOL vector.
inline uint8_t gb_read_bool_bit(const DrakenVector& v, uint32_t phys) {
    const uint8_t* d = static_cast<const uint8_t*>(v.data);
    return static_cast<uint8_t>((d[phys >> 3] >> (phys & 7)) & 1u);
}

// Allocate a zeroed bit-packed BOOL data buffer for `n` elements.
inline uint8_t* gb_alloc_bool_bits(uint32_t n) {
    size_t nbytes = (static_cast<size_t>(n) + 7u) / 8u;
    if (nbytes == 0) nbytes = 1;
    uint8_t* bits = static_cast<uint8_t*>(draken_malloc(nbytes));
    std::memset(bits, 0, nbytes);
    return bits;
}

// One materialized payload column: `raw` holds `elem_size` bytes per row,
// densely packed in row-store order (NOT the original morsel's own row
// order — rows are appended as build morsels stream in).
//
// `validity` mirrors the DrakenVector convention (1 bit/row, 1 = valid, bit
// index == row-store row index) but is LAZY: as long as every row appended
// so far is valid it stays empty ("empty = all valid"), matching a NULL
// DrakenVector::validity. The first NULL payload value seen allocates it,
// backfilling every prior row as valid — so the common NOT-NULL case pays
// zero bitmap cost.
struct GroupKeyColumn {
    DrakenType type = DRAKEN_INT64;
    size_t elem_size = 0;
    const LogicalType* logical = nullptr;  // borrowed; carried to output columns
    std::vector<uint8_t> raw;    // elem_size bytes/row (slots for strings)
    std::vector<uint8_t> arena;  // strings only: consolidated long-string bytes
    std::vector<uint8_t> validity;  // lazy — see comment above

    size_t row_count() const { return elem_size ? raw.size() / elem_size : 0; }

    // Record row-store row `row`'s null-ness. A no-op while every row so far
    // has been valid (the lazy "empty = all valid" state).
    void note_null(size_t row, bool is_null) {
        if (validity.empty() && !is_null) return;
        size_t nbytes = (row / 8) + 1;
        if (validity.size() < nbytes) validity.resize(nbytes, 0xFF);
        if (is_null) validity[row >> 3] &= static_cast<uint8_t>(~(1u << (row & 7)));
        else validity[row >> 3] |= static_cast<uint8_t>(1u << (row & 7));
    }

    void append_row(const DrakenVector& v, uint32_t row, ErrCtx&, const char*) {
        size_t out_row = row_count();
        bool is_null = v.validity != nullptr
            && !((v.validity[row >> 3] >> (row & 7)) & 1u);
        uint32_t phys = v.selection[row];
        if (gb_key_is_bool(type)) {
            // Bit-packed on the way in, one unpacked 0/1 byte per row in the store.
            raw.push_back(is_null ? 0 : gb_read_bool_bit(v, phys));
            note_null(out_row, is_null);
            return;
        }
        if (gb_key_is_string(type)) {
            if (is_null) {
                DrakenStringSlot zero;
                std::memset(&zero, 0, sizeof(zero));
                const uint8_t* rb = reinterpret_cast<const uint8_t*>(&zero);
                raw.insert(raw.end(), rb, rb + sizeof(DrakenStringSlot));
                note_null(out_row, true);
                return;
            }
            // CANONICAL layout (buffers.h): a string vector's `data` points at a
            // DrakenStringArena STRUCT — slots and arena resolve through it.
            const auto* sa = static_cast<const DrakenStringArena*>(v.data);
            const DrakenStringSlot* slot = &sa->slots[phys];
            DrakenStringSlot rebased;
            if (str_is_inline(slot)) {
                rebased = *slot;
            } else {
                uint32_t slen = str_length(slot);
                size_t arena_pos = arena.size();
                arena.resize(arena_pos + slen);
                std::memcpy(arena.data() + arena_pos, str_data(slot, sa->arena), slen);
                str_clone_with_offset(&rebased, slot, static_cast<uint32_t>(arena_pos));
            }
            const uint8_t* rb = reinterpret_cast<const uint8_t*>(&rebased);
            raw.insert(raw.end(), rb, rb + sizeof(DrakenStringSlot));
            note_null(out_row, false);
            return;
        }
        if (is_null) {
            raw.resize(raw.size() + elem_size, 0);
        } else {
            const uint8_t* src = static_cast<const uint8_t*>(v.data) + static_cast<size_t>(phys) * elem_size;
            raw.insert(raw.end(), src, src + elem_size);
        }
        note_null(out_row, is_null);
    }

    // Append row `r` of another GroupKeyColumn (same type) to this one, rebasing
    // a long-string slot into this column's arena. Used to merge per-group key
    // stores across worker partitions (GROUP BY).
    void append_from(const GroupKeyColumn& src, size_t r) {
        size_t out_row = row_count();
        bool is_null = !src.validity.empty()
            && !((src.validity[r >> 3] >> (r & 7)) & 1u);
        if (gb_key_is_bool(type)) {
            raw.push_back(is_null ? 0 : src.raw[r]);
            note_null(out_row, is_null);
            return;
        }
        if (gb_key_is_string(type)) {
            if (is_null) {
                DrakenStringSlot zero;
                std::memset(&zero, 0, sizeof(zero));
                const uint8_t* rb = reinterpret_cast<const uint8_t*>(&zero);
                raw.insert(raw.end(), rb, rb + sizeof(DrakenStringSlot));
                note_null(out_row, true);
                return;
            }
            const DrakenStringSlot* slot = reinterpret_cast<const DrakenStringSlot*>(
                src.raw.data() + r * sizeof(DrakenStringSlot));
            DrakenStringSlot rebased;
            if (str_is_inline(slot)) {
                rebased = *slot;
            } else {
                uint32_t slen = str_length(slot);
                size_t arena_pos = arena.size();
                arena.resize(arena_pos + slen);
                std::memcpy(arena.data() + arena_pos,
                            str_data(slot, src.arena.empty() ? nullptr : src.arena.data()),
                            slen);
                str_clone_with_offset(&rebased, slot, static_cast<uint32_t>(arena_pos));
            }
            const uint8_t* rb = reinterpret_cast<const uint8_t*>(&rebased);
            raw.insert(raw.end(), rb, rb + sizeof(DrakenStringSlot));
            note_null(out_row, false);
            return;
        }
        if (is_null) {
            raw.resize(raw.size() + elem_size, 0);
        } else {
            raw.insert(raw.end(), src.raw.data() + r * elem_size,
                       src.raw.data() + (r + 1) * elem_size);
        }
        note_null(out_row, is_null);
    }
};

// RAII wrapper over td_histogram_t* (third_party/tdigest-c — a plain C API, no
// C++ lifecycle of its own). Default-constructs a fresh digest so
// std::vector<TDigestPtr>::resize(n) "just works" for per-group growth, same
// as MedianState's resize contract.
struct TDigestPtr {
    td_histogram_t* h = nullptr;
    TDigestPtr() : h(td_new(100.0)) {}
    ~TDigestPtr() { if (h) td_free(h); }
    TDigestPtr(const TDigestPtr&) = delete;
    TDigestPtr& operator=(const TDigestPtr&) = delete;
    TDigestPtr(TDigestPtr&& o) noexcept : h(o.h) { o.h = nullptr; }
    TDigestPtr& operator=(TDigestPtr&& o) noexcept {
        if (this != &o) { if (h) td_free(h); h = o.h; o.h = nullptr; }
        return *this;
    }
};

// ---- aggregate spec + accumulator --------------------------------------------------

enum class AggFn : uint8_t {
    CountStar = 0, Count = 1, Sum = 2, Avg = 3, Min = 4, Max = 5,
    CountDistinct = 6,   // COUNT(DISTINCT col): dedup on serialized value bytes
    ArrayAgg = 7,        // ARRAY_AGG(col): one ARRAY per group; GROUP BY only
    Stddev = 8,          // STDDEV(col): population stddev (N denominator), always DOUBLE
    Median = 9,          // MEDIAN(col): exact median via MedianState (std::nth_element)
    AnyValue = 10,       // ANY_VALUE(col): any real non-null value — implemented by
                         // reusing the MIN machinery unchanged (always the minimum is
                         // A valid ANY_VALUE answer per SQL's contract — "unspecified
                         // which one" — see gb_kind_of / the two ternaries below).
                         // tests/operators/test_grouped_any_value.py's own docstring
                         // confirms callers must not depend on which value comes back.
    ApproxCountDistinct = 11,  // APPROX_COUNT_DISTINCT(col): HyperLogLog++ sketch
                               // (HllppSketch, src/cpp/hllpp.h) — same draken row
                               // hash CountDistinct uses (compute_row_hashes), fed
                               // into a sketch instead of an exact dedup set.
    ApproxPercentile = 12,     // APPROX_PERCENTILE(col, p): t-digest sketch
                               // (td_histogram_t, third_party/tdigest-c) — MEDIAN's
                               // approximate sibling. `p` is AggSpec2::percentile,
                               // a query-time constant, not a second operand column.
    Corr = 13,                 // CORR(x, y): Pearson correlation — the only
                               // aggregate with a SECOND operand COLUMN
                               // (AggSpec2::col_idx2). Rows where either operand
                               // is NULL are skipped (SQL's pairwise contract).
                               // Always DOUBLE; NULL when undefined (no pairs, or
                               // zero variance in either operand).
    CidrAgg = 14,              // CIDR_AGG(ip): the minimal list of CIDR blocks
                               // covering exactly the addresses seen, as
                               // ARRAY<VARCHAR>. Collects into a Roaring bitmap
                               // (native_roaring32.hpp), which dedups on insert,
                               // so its state grows with DISTINCT addresses
                               // rather than rows. Operand must be UINT32
                               // carrying LogicalKind::IPV4 — see
                               // cidr_operand_supported.
    StddevSamp = 15,           // STDDEV_SAMP(col): sample stddev (N-1 denominator,
                               // Bessel's correction). Shares STDDEV's accumulation
                               // (agg2_update_stddev/agg2_merge — Σx, Σx², count);
                               // only the finalize divisor differs. NULL for a
                               // group with fewer than 2 valid rows (N-1 == 0 is
                               // undefined, matching DuckDB/the SQL standard) —
                               // STDDEV_POP/STDDEV is instead defined for N==1
                               // (variance 0). STDDEV_POP is a pure alias for
                               // AggFn::Stddev, not a separate value here.
    VarPop = 16,               // VAR_POP(col): population variance — STDDEV's
                               // finalize formula minus the final sqrt. Same
                               // accumulation and NULL rule as STDDEV (only
                               // NULL when zero valid rows).
    VarSamp = 17,              // VAR_SAMP(col): sample variance — STDDEV_SAMP's
                               // finalize formula minus the final sqrt. Same
                               // accumulation and NULL rule as STDDEV_SAMP
                               // (NULL below 2 valid rows).
};

// AggSpec2.col_idx sentinels — named so a bare -1/-2 is never left for a future
// reader to decode. Both are negative (never a real column index, which is
// always >= 0), but each names a DIFFERENT "no single operand column" case —
// disambiguated together with `fn`, never by value alone.
constexpr int kAggNoOperand = -1;   // CountStar: no operand column
constexpr int kAggWholeRow  = -2;   // CountDistinct: dedup over EVERY column in
                                     // the stream (COUNT(DISTINCT *))

struct AggSpec2 {
    AggFn fn;
    int col_idx;        // operand column; kAggNoOperand for CountStar,
                         // kAggWholeRow for whole-row CountDistinct
    int col_idx2 = kAggNoOperand;   // Corr's second operand column (y);
                                    // kAggNoOperand for every other fn
    std::string name;   // output column identity
    // ARRAY_AGG modifiers — ignored by every other fn. DISTINCT/ORDER BY/LIMIT
    // all apply at finalize, AFTER the per-partition lists are merged: a worker
    // sees an arbitrary row subset, so ordering or truncating locally would give
    // a different answer than the serial plan.
    bool    aa_distinct   = false;
    bool    aa_ordered    = false;
    bool    aa_descending = false;
    int64_t aa_limit      = -1;     // < 0 == no LIMIT
    // No per-group element cap: retained memory is bounded by the GLOBAL byte
    // budget (kArrayAggBudgetBytes, below), the same guard shape MEDIAN uses.
    // A per-group count bounded nothing — the group count is unbounded — while
    // refusing ordinary group sizes.
    // APPROX_PERCENTILE's second argument — a query-time constant (0.0-1.0),
    // validated at plan time (compiler.py). Ignored by every other fn.
    double  percentile = 0.5;
};

struct AggCell {
    // Field order matters: the SUM/AVG/COUNT hot path touches ONLY the first
    // 40 bytes (one cache line); the MIN/MAX lanes sit after so aggregations
    // without extremes never pull their lines. fsumsq is STDDEV-only (second
    // moment, Σx²) — placed next to fsum so STDDEV shares that same cache
    // line; SUM/AVG/COUNT/MIN/MAX never touch it.
    __int128 isum = 0;   // EXACT integer-domain sum (int64 family AND DECIMAL128 raws)
    double   fsum = 0.0;
    double   fsumsq = 0.0;                // STDDEV: Σx², always double (no exactness need)
    int64_t  valid = 0;                  // non-NULL operand rows
    int64_t  rows = 0;                   // ALL rows — COUNT(*)
    __int128 min128 = 0, max128 = 0;   // DECIMAL128 order extremes (raw == value order)
    uint64_t min_key = 0, max_key = 0;   // normalized order keys (sort_num_key)
    int64_t  min_raw = 0, max_raw = 0;   // raw value container (widened / bit-stored)
    // CORR-only lanes (Σy, Σy², Σxy — fsum/fsumsq carry Σx/Σx²), placed after
    // the MIN/MAX lanes so every other aggregation's cache footprint is
    // untouched. `valid` counts pairs where BOTH operands are non-NULL.
    double   fsumy = 0.0, fsumyy = 0.0, fsumxy = 0.0;
};

// Captured once from the first morsel a worker sees; merged into the global.
struct AggColMeta {
    DrakenType type = DRAKEN_INT64;
    const LogicalType* logical = nullptr;
    bool is_float = false;
    bool is_string = false;   // MIN/MAX over a string column (parallel string store)
    bool captured = false;
    // Corr's second operand (col_idx2) — unset for every other fn.
    DrakenType type2 = DRAKEN_INT64;
    bool is_float2 = false;
};

inline bool agg2_operand_supported(DrakenType t) {
    switch (t) {
        case DRAKEN_INT8: case DRAKEN_INT16: case DRAKEN_INT32: case DRAKEN_INT64:
        case DRAKEN_DECIMAL: case DRAKEN_DATE32: case DRAKEN_TIMESTAMP64:
        case DRAKEN_TIME32: case DRAKEN_TIME64: case DRAKEN_BOOL:
        case DRAKEN_FLOAT32: case DRAKEN_FLOAT64:
        case DRAKEN_DECIMAL128:   // SUM/AVG/COUNT only — MIN/MAX guarded at capture
        case DRAKEN_UINT8: case DRAKEN_UINT16: case DRAKEN_UINT32: case DRAKEN_UINT64:  // E33
            return true;
        default:
            return false;
    }
}

// MEDIAN is numeric-only — NOT the general agg2_operand_supported set. No
// DECIMAL/DECIMAL128 (no descale — same "never a mis-scaled answer" reasoning
// as STDDEV), and no BOOL/DATE32/TIMESTAMP64/TIME32/TIME64 either, unlike
// SUM/AVG/STDDEV which treat those as meaningful numeric domains. Widening
// this set is a design decision, not a gap to be quietly filled: MEDIAN
// returns FLOAT64, so admitting a temporal or DECIMAL operand would answer in
// a type the caller never asked for. Refuse loudly instead.
// Single source of truth for the MEDIAN budget-overflow message — four call
// sites (ungrouped accumulate/merge, grouped accumulate/merge) all fail loud
// with the exact same text, never a silent approximate fallback (a query
// author who wants approximate opts in by name via APPROX_PERCENTILE, the
// budget never silently decides for them). The limit is a GLOBAL byte budget
// across all groups (kMedianBudgetBytes, _agg_kernels.hpp), not a per-group
// value cap — exact MEDIAN buffers every non-null input value, so total
// memory is what needs bounding.
// Names the budget, quotes no figure. A literal here is a SECOND copy of the
// limit, which a build with a different kMedianBytes silently falsifies —
// sending the reader to reason about a number nothing enforces. The variable is
// the discoverable value, and it reads the native constant so it cannot drift.
inline constexpr const char* kMedianCapExceededMsg =
    "MEDIAN — buffered values exceeded the memory budget (see "
    "@@median_memory_budget_bytes). Use APPROX_PERCENTILE(x, 0.5) for "
    "approximate median over large sets of values.";

// HllppSketch::register_index/rho (hllpp.cpp) read the hash's raw top/bottom
// bits directly — the standard HLL algorithm, correct ONLY if the hash has
// genuine avalanche across all 64 bits. draken's row hash (cxx_hash_c, via
// compute_row_hashes) is tuned for hash-table bucketing — collision-avoidance
// for equality, which is all COUNT(DISTINCT)'s exact dedup set needs — not
// full-bit avalanche. Confirmed empirically: APPROX_COUNT_DISTINCT fed raw
// draken hashes directly was ~25% off on a 50K-row/39K-distinct set, while
// COUNT(DISTINCT) on the identical hashes was exact. SplitMix64's finalizer
// (well-known, cheap: a few multiply-xor-shift ops) re-mixes here, not in
// draken, so every GROUP BY/JOIN/DISTINCT that relies on cxx_hash_c's actual
// tuning is untouched — only this one consumer's stricter requirement changes.
inline uint64_t hll_avalanche(uint64_t h) noexcept {
    h ^= h >> 30;
    h *= 0xbf58476d1ce4e5b9ULL;
    h ^= h >> 27;
    h *= 0x94d049bb133111ebULL;
    h ^= h >> 31;
    return h;
}

inline bool median_operand_supported(DrakenType t) {
    switch (t) {
        case DRAKEN_INT8: case DRAKEN_INT16: case DRAKEN_INT32: case DRAKEN_INT64:
        case DRAKEN_FLOAT32: case DRAKEN_FLOAT64:
        case DRAKEN_UINT8: case DRAKEN_UINT16: case DRAKEN_UINT32: case DRAKEN_UINT64:
            return true;
        default:
            return false;
    }
}

// CORR operand capture, shared by the grouped and ungrouped sinks' capture_meta:
// validates BOTH operands are numeric (median_operand_supported's set — no
// DECIMAL descale, same "never a mis-scaled answer" reasoning as STDDEV) and
// records the second operand's type/floatness in the spec's AggColMeta.
inline bool corr_capture_meta(const AggSpec2& sp, const CxxMorsel& in,
                              DrakenType t_first, AggColMeta& m, ErrCtx& err) {
    if (!median_operand_supported(t_first)) {
        err.code = 1;
        err.msg = "CORR over this column type is not supported — "
                  "only numeric inputs are accepted (CAST DECIMAL to DOUBLE first)";
        return false;
    }
    if (sp.col_idx2 < 0
            || static_cast<size_t>(sp.col_idx2) >= in.columns.size()) {
        err.code = 1;
        err.msg = "CORR second operand column missing from input "
                  "morsel — fail loud, never a silent wrong answer";
        return false;
    }
    DrakenType t2 = in.columns[static_cast<size_t>(sp.col_idx2)].view.type;
    if (!median_operand_supported(t2)) {
        err.code = 1;
        err.msg = "CORR over this column type is not supported — "
                  "only numeric inputs are accepted (CAST DECIMAL to DOUBLE first)";
        return false;
    }
    m.type2 = t2;
    m.is_float2 = (t2 == DRAKEN_FLOAT32 || t2 == DRAKEN_FLOAT64);
    return true;
}

inline __int128 agg2_read_i128(const DrakenVector& v, uint32_t row) {
    __int128 out;
    std::memcpy(&out, static_cast<const uint8_t*>(v.data)
                          + static_cast<size_t>(v.selection[row]) * 16u, 16u);
    return out;
}

// Raw value widened into an int64 container: integer family sign-extended; floats
// stored as the DOUBLE's bit pattern (round-trips losslessly to FLOAT32/64 output).
// Same semantics as agg2_read_raw below, but reading from values the caller has
// hoisted out of the row loop. Through a `const DrakenVector&` the compiler must
// re-load v.type / v.data / v.selection on every iteration — the accumulation
// loops store through GBLanes pointers that it cannot prove don't alias the
// vector — so the type switch could never be unswitched out of the loop. Passing
// them as plain locals makes them provably invariant.
inline int64_t agg2_read_raw_at(DrakenType vtype, const void* vdata,
                                const uint32_t* vsel, uint32_t row, bool is_float) {
    uint32_t phys = vsel[row];
    if (is_float) {
        double d = (vtype == DRAKEN_FLOAT32)
            ? static_cast<double>(static_cast<const float*>(vdata)[phys])
            : static_cast<const double*>(vdata)[phys];
        int64_t bits;
        std::memcpy(&bits, &d, sizeof(bits));
        return bits;
    }
    switch (vtype) {
        case DRAKEN_INT8:   return static_cast<const int8_t*>(vdata)[phys];
        case DRAKEN_INT16:  return static_cast<const int16_t*>(vdata)[phys];
        case DRAKEN_INT32:
        case DRAKEN_DATE32:
        case DRAKEN_TIME32: return static_cast<const int32_t*>(vdata)[phys];
        case DRAKEN_BOOL:
            return (static_cast<const uint8_t*>(vdata)[phys >> 3] >> (phys & 7)) & 1u;
        case DRAKEN_UINT8:  return static_cast<int64_t>(static_cast<const uint8_t* >(vdata)[phys]);
        case DRAKEN_UINT16: return static_cast<int64_t>(static_cast<const uint16_t*>(vdata)[phys]);
        case DRAKEN_UINT32: return static_cast<int64_t>(static_cast<const uint32_t*>(vdata)[phys]);
        default:            return static_cast<const int64_t*>(vdata)[phys];
    }
}

inline int64_t agg2_read_raw(const DrakenVector& v, uint32_t row, bool is_float) {
    if (is_float) {
        uint32_t phys = v.selection[row];
        double d = (v.type == DRAKEN_FLOAT32)
            ? static_cast<double>(static_cast<const float*>(v.data)[phys])
            : static_cast<const double*>(v.data)[phys];
        int64_t bits;
        std::memcpy(&bits, &d, sizeof(bits));
        return bits;
    }
    uint32_t phys = v.selection[row];
    switch (v.type) {
        case DRAKEN_INT8:   return static_cast<const int8_t*>(v.data)[phys];
        case DRAKEN_INT16:  return static_cast<const int16_t*>(v.data)[phys];
        case DRAKEN_INT32:
        case DRAKEN_DATE32:
        case DRAKEN_TIME32: return static_cast<const int32_t*>(v.data)[phys];
        case DRAKEN_BOOL:
            return (static_cast<const uint8_t*>(v.data)[phys >> 3] >> (phys & 7)) & 1u;
        // E33 — zero-extend (source is unsigned; sign-extending would corrupt).
        // UINT64 is NOT listed here — it falls to `default`, which reads the raw
        // 8 bytes as int64_t (a bit-pattern reinterpret, not a value cast) —
        // exactly the reinterpretation needed: c.isum accumulates via ordinary
        // int64_t `+=`, which is bit-identical to uint64_t `+=` (two's
        // complement), so the sum's bit pattern round-trips correctly as long as
        // the FINAL reported value is reinterpreted back to uint64_t for output.
        case DRAKEN_UINT8:  return static_cast<int64_t>(static_cast<const uint8_t* >(v.data)[phys]);
        case DRAKEN_UINT16: return static_cast<int64_t>(static_cast<const uint16_t*>(v.data)[phys]);
        case DRAKEN_UINT32: return static_cast<int64_t>(static_cast<const uint32_t*>(v.data)[phys]);
        default:            return static_cast<const int64_t*>(v.data)[phys];
    }
}

inline void agg2_update(AggCell& c, const DrakenVector& v, uint32_t row, bool is_float,
                        bool needs_minmax = true) {
    // caller has already established the row is valid (non-NULL operand)
    if (v.type == DRAKEN_DECIMAL128) {
        __int128 raw = agg2_read_i128(v, row);
        c.isum += raw;
        if (needs_minmax) {
            if (c.valid == 0 || raw < c.min128) c.min128 = raw;
            if (c.valid == 0 || raw > c.max128) c.max128 = raw;
        }
        c.valid += 1;
        return;
    }
    if (!needs_minmax) {
        // SUM/AVG/COUNT-only spec: no normalized order key, no extreme lanes —
        // profiled as pure waste (sort_num_key per row) on Q33-class queries.
        int64_t raw = agg2_read_raw(v, row, is_float);
        if (is_float) {
            double d;
            std::memcpy(&d, &raw, sizeof(d));
            c.fsum += d;
        } else {
            c.isum += raw;
        }
        c.valid += 1;
        return;
    }
    uint64_t k = sort_num_key(v, row);
    int64_t raw = agg2_read_raw(v, row, is_float);
    if (is_float) {
        double d;
        std::memcpy(&d, &raw, sizeof(d));
        c.fsum += d;
    } else {
        c.isum += raw;
    }
    if (c.valid == 0 || k < c.min_key) { c.min_key = k; c.min_raw = raw; }
    if (c.valid == 0 || k > c.max_key) { c.max_key = k; c.max_raw = raw; }
    c.valid += 1;
}

// STDDEV/STDDEV_SAMP/VAR_POP/VAR_SAMP all accumulate the identical Σx/Σx²/count
// lanes (agg2_update_stddev/agg2_merge below) — only their finalize formula
// differs (emit_lane_column). Every dispatch site that only cares "is this the
// STDDEV family" (accumulation, DECIMAL rejection) checks this instead of
// enumerating all four AggFn values separately.
inline bool agg_fn_is_stddev_family(AggFn fn) noexcept {
    return fn == AggFn::Stddev || fn == AggFn::StddevSamp
        || fn == AggFn::VarPop || fn == AggFn::VarSamp;
}

// STDDEV accumulation: always double (mean/variance are inherently non-exact,
// unlike SUM/AVG's int128-exact path), regardless of int or float operand.
// No normalized order key / min-max lanes — STDDEV never needs them.
inline void agg2_update_stddev(AggCell& c, const DrakenVector& v, uint32_t row,
                               bool is_float) noexcept {
    int64_t raw = agg2_read_raw(v, row, is_float);
    double d;
    if (is_float) {
        std::memcpy(&d, &raw, sizeof(d));
    } else {
        d = static_cast<double>(raw);
    }
    c.fsum += d;
    c.fsumsq += d * d;
    c.valid += 1;
}

// CORR accumulation: one (x, y) pair, both already known non-NULL. Same
// always-double posture as STDDEV (correlation is inherently non-exact).
inline void agg2_update_corr(AggCell& c, double x, double y) noexcept {
    c.fsum   += x;
    c.fsumsq += x * x;
    c.fsumy  += y;
    c.fsumyy += y * y;
    c.fsumxy += x * y;
    c.valid  += 1;
}

// Pearson r from the six CORR lanes. Returns false when undefined — no pairs,
// or zero variance in either operand — and the caller emits NULL.
inline bool corr_finalize(double n, double sx, double sxx, double sy, double syy,
                          double sxy, double& out) noexcept {
    if (n <= 0.0) return false;
    double mx = sx / n, my = sy / n;
    double varx = sxx / n - mx * mx;
    double vary = syy / n - my * my;
    // Float rounding can push a true-zero variance slightly negative (same
    // clamp STDDEV applies before its sqrt).
    if (varx < 0.0) varx = 0.0;
    if (vary < 0.0) vary = 0.0;
    double denom = std::sqrt(varx * vary);
    if (denom == 0.0) return false;
    double r = (sxy / n - mx * my) / denom;
    // Rounding can nudge |r| past 1 for perfectly-correlated inputs.
    if (r > 1.0) r = 1.0;
    if (r < -1.0) r = -1.0;
    out = r;
    return true;
}

inline void agg2_merge(AggCell& into, const AggCell& from) {
    into.isum += from.isum;
    into.fsum += from.fsum;
    into.fsumsq += from.fsumsq;
    into.fsumy += from.fsumy;
    into.fsumyy += from.fsumyy;
    into.fsumxy += from.fsumxy;
    if (from.valid > 0) {
        if (into.valid == 0 || from.min128 < into.min128) into.min128 = from.min128;
        if (into.valid == 0 || from.max128 > into.max128) into.max128 = from.max128;
        if (into.valid == 0 || from.min_key < into.min_key) {
            into.min_key = from.min_key;
            into.min_raw = from.min_raw;
        }
        if (into.valid == 0 || from.max_key > into.max_key) {
            into.max_key = from.max_key;
            into.max_raw = from.max_raw;
        }
    }
    into.valid += from.valid;
    into.rows += from.rows;
}

// ---- string MIN/MAX (parallel std::string store, one extreme per cell slot) ---------
// A slot's fn is EITHER Min OR Max, so one string per (group, slot) suffices.
// Byte-wise lexicographic order: correct for VARCHAR (ASCII), VARBINARY (bytes),
// AND NVARCHAR (UTF-8 byte order == codepoint order). AggCell.valid still counts.

inline void agg2_update_str(AggCell& c, std::string& sval, const DrakenVector& v,
                            uint32_t row, bool want_max) {
    const DrakenStringArena* sa = string_arena_of(v);
    const DrakenStringSlot* slot = &sa->slots[v.selection[row]];
    const char* p = reinterpret_cast<const char*>(str_data(slot, sa->arena));
    uint32_t len = str_length(slot);
    std::string_view sv(p, len);
    std::string_view cur(sval.data(), sval.size());
    if (c.valid == 0 || (want_max ? sv > cur : sv < cur)) sval.assign(p, len);
    c.valid += 1;
}

// §11 compressed-shape twin of agg2_update_str: address a PHYSICAL dict slot
// directly instead of a logical row. The string extreme over all rows equals the
// extreme over the REFERENCED unique values, so a compressed MIN/MAX reduces over
// data_length uniques rather than length rows. c.valid becomes a count of uniques,
// not rows — safe because string MIN/MAX only reads valid as a >0 "has-data" gate
// (agg2_merge_str, emit_string_lane_column) and never emits the count. Caller has
// established the slot is referenced by at least one valid row.
inline void agg2_update_str_phys(AggCell& c, std::string& sval, const DrakenVector& v,
                                 uint32_t phys, bool want_max) {
    const DrakenStringArena* sa = string_arena_of(v);
    const DrakenStringSlot* slot = &sa->slots[phys];
    const char* p = reinterpret_cast<const char*>(str_data(slot, sa->arena));
    uint32_t len = str_length(slot);
    std::string_view sv(p, len);
    std::string_view cur(sval.data(), sval.size());
    if (c.valid == 0 || (want_max ? sv > cur : sv < cur)) sval.assign(p, len);
    c.valid += 1;
}

// MUST run BEFORE agg2_merge on the same cell pair (reads into.valid pre-merge).
inline void agg2_merge_str(const AggCell& into, const AggCell& from,
                           std::string& into_s, const std::string& from_s,
                           bool want_max) {
    if (from.valid > 0 &&
        (into.valid == 0 || (want_max ? from_s > into_s : from_s < into_s)))
        into_s = from_s;
}

// Emit a string MIN/MAX result column as ONE canonical consolidated block
// (same layout emit_key_columns' string arm builds). NULL where valid[g] == 0.
// Inputs are per-group LANES (v3 columnar collectors), not interleaved cells.
inline CxxColumn emit_string_lane_column(const AggColMeta& meta,
                                         const std::string* svals,
                                         const int64_t* valid, uint32_t n) {
    size_t total_arena = 0;
    for (uint32_t g = 0; g < n; ++g) {
        if (valid[g] == 0) continue;
        size_t len = svals[g].size();
        if (len > STR_INLINE_MAX) total_arena += len;
    }
    size_t slots_off = sizeof(DrakenStringArena);
    size_t arena_off = slots_off
        + static_cast<size_t>(n == 0 ? 1 : n) * sizeof(DrakenStringSlot);
    uint8_t* blk = static_cast<uint8_t*>(draken_malloc(arena_off + total_arena));
    auto* sa = reinterpret_cast<DrakenStringArena*>(blk);
    auto* dst = reinterpret_cast<DrakenStringSlot*>(blk + slots_off);
    uint8_t* out_arena = total_arena > 0 ? blk + arena_off : nullptr;
    sa->slots = dst; sa->arena = out_arena; sa->length = n;
    sa->arena_used = total_arena; sa->arena_cap = total_arena;
    sa->null_bitmap = nullptr; sa->owns_buffers = 0; sa->type = meta.type;
    size_t vbytes = (static_cast<size_t>(n) + 7) / 8;
    uint8_t* vbits = nullptr;
    size_t arena_pos = 0;
    for (uint32_t g = 0; g < n; ++g) {
        if (valid[g] == 0) {
            std::memset(&dst[g], 0, sizeof(DrakenStringSlot));
            if (vbits == nullptr) {
                vbits = static_cast<uint8_t*>(draken_malloc(vbytes == 0 ? 1 : vbytes));
                std::memset(vbits, 0xFF, vbytes == 0 ? 1 : vbytes);
            }
            vbits[g >> 3] &= static_cast<uint8_t>(~(1u << (g & 7)));
            continue;
        }
        const std::string& val = svals[g];
        const uint8_t* bytes = reinterpret_cast<const uint8_t*>(val.data());
        uint32_t len = static_cast<uint32_t>(val.size());
        if (len <= STR_INLINE_MAX) {
            str_init_inline(&dst[g], bytes, len);
        } else {
            std::memcpy(out_arena + arena_pos, bytes, len);
            str_init_extern(&dst[g], out_arena + arena_pos, len,
                            static_cast<uint32_t>(arena_pos));
            arena_pos += len;
        }
    }
    uint32_t* sel = static_cast<uint32_t*>(
        draken_malloc((n == 0 ? 1 : n) * sizeof(uint32_t)));
    for (uint32_t i = 0; i < n; ++i) sel[i] = i;
    DrakenVector v;
    v.data = sa; v.selection = sel; v.data_length = n; v.length = n;
    v.validity = vbits; v.type = meta.type;
    v.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
    CxxColumn c;
    c.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(blk),
                                          OwnedBuffer<uint8_t>(vbits),
                                          OwnedBuffer<void>(sel));
    c.own->logical_type = meta.logical;
    c.view = c.own->vec;
    return c;
}

// ---- fixed-width column emit at a given type/width ---------------------------------
// `raws` carries widened int64 containers (float bit patterns for float types);
// `valid_flags[i]==0` marks a NULL output row.

inline CxxColumn emit_fixed_column(const int64_t* raws, const uint8_t* valid_flags,
                                   uint32_t n, DrakenType t, const LogicalType* logical,
                                   ErrCtx& err) {
    if (t == DRAKEN_BOOL) {
        // A BOOL DrakenVector's `data` is BIT-PACKED (consumer reads
        // data[phys>>3]>>(phys&7)&1) — NOT one byte per value. gather_elem_size
        // is not a byte width for BOOL, so this needs a dedicated arm.
        size_t dbytes = (static_cast<size_t>(n) + 7) / 8;
        uint8_t* data = static_cast<uint8_t*>(draken_malloc(dbytes == 0 ? 1 : dbytes));
        std::memset(data, 0, dbytes == 0 ? 1 : dbytes);
        size_t vbytes = (static_cast<size_t>(n) + 7) / 8;
        uint8_t* vbits = nullptr;
        for (uint32_t i = 0; i < n; ++i) {
            if (valid_flags != nullptr && valid_flags[i] == 0) {
                if (vbits == nullptr) {
                    vbits = static_cast<uint8_t*>(draken_malloc(vbytes == 0 ? 1 : vbytes));
                    std::memset(vbits, 0xFF, vbytes == 0 ? 1 : vbytes);
                }
                vbits[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7)));
                continue;  // data bit already 0
            }
            if (raws[i] != 0) data[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
        uint32_t* sel = static_cast<uint32_t*>(draken_malloc((n == 0 ? 1 : n) * sizeof(uint32_t)));
        for (uint32_t i = 0; i < n; ++i) sel[i] = i;
        DrakenVector v;
        v.data = data; v.selection = sel; v.data_length = n; v.length = n;
        v.validity = vbits; v.type = t;
        v.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
        CxxColumn c;
        c.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(data),
                                              OwnedBuffer<uint8_t>(vbits), OwnedBuffer<void>(sel));
        c.own->logical_type = logical;
        c.view = c.own->vec;
        return c;
    }
    // Tag-only width (NOT the descriptor-aware draken_type_itemsize): every value
    // here arrives in a widened int64 container, so this function's domain is the
    // types whose width the physical tag alone decides AND which fit in 8 bytes.
    // A parameterized-width type (VECTOR_FP16, dimension × 2) has no meaning in an
    // int64 lane and must keep resolving to 0 here — resolving its real width would
    // make the memcpy below read past the 8-byte container. DECIMAL128 likewise
    // never reaches here; it has its own int128 lane (emit_i128_lane_column).
    size_t es = draken_type_fixed_itemsize(t);
    if (es == 0) {
        // Previously this substituted a width of 1, allocating a 1-byte buffer and
        // then memcpy'ing ZERO bytes per row — an unsupported type emitted a column
        // of uninitialized garbage instead of an error. Unreachable today (every
        // aggregate lane resolves to a tag-width type), which is exactly why it had
        // to go: the next type added would have hit it silently.
        err.code = 1;
        err.msg = "aggregate result type has no fixed width — fail "
                  "loud, never silent corruption";
        return CxxColumn{};
    }
    size_t alloc_n = (n == 0 ? 1 : n);
    uint8_t* data = static_cast<uint8_t*>(draken_malloc(alloc_n * es));
    size_t vbytes = (static_cast<size_t>(n) + 7) / 8;
    uint8_t* vbits = nullptr;
    for (uint32_t i = 0; i < n; ++i) {
        if (valid_flags != nullptr && valid_flags[i] == 0) {
            if (vbits == nullptr) {
                vbits = static_cast<uint8_t*>(draken_malloc(vbytes == 0 ? 1 : vbytes));
                std::memset(vbits, 0xFF, vbytes == 0 ? 1 : vbytes);
            }
            vbits[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7)));
            std::memset(data + static_cast<size_t>(i) * es, 0, es);
            continue;
        }
        int64_t raw = raws[i];
        if (t == DRAKEN_FLOAT32) {
            double d;
            std::memcpy(&d, &raw, sizeof(d));
            float f = static_cast<float>(d);
            std::memcpy(data + static_cast<size_t>(i) * es, &f, es);
        } else if (t == DRAKEN_FLOAT64) {
            std::memcpy(data + static_cast<size_t>(i) * es, &raw, es);
        } else {
            // little-endian: the low `es` bytes of the widened container ARE the value
            std::memcpy(data + static_cast<size_t>(i) * es, &raw, es);
        }
    }
    uint32_t* sel = static_cast<uint32_t*>(draken_malloc(alloc_n * sizeof(uint32_t)));
    for (uint32_t i = 0; i < n; ++i) sel[i] = i;
    DrakenVector v;
    v.data = data; v.selection = sel; v.data_length = n; v.length = n;
    v.validity = vbits; v.type = t;
    v.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
    CxxColumn c;
    c.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(data),
                                          OwnedBuffer<uint8_t>(vbits), OwnedBuffer<void>(sel));
    c.own->logical_type = logical;
    c.view = c.own->vec;
    return c;
}

// ---- collector kinds (v3) -----------------------------------------------------------
// One enum per (fn, operand-type) shape: the grouped sink allocates ONLY the
// columnar lanes a kind needs, and update/merge/emit dispatch on it ONCE per
// column pass — never per row.

enum class GBKind : uint8_t {
    Rows,        // COUNT(*) — shared per-partition rows lane
    Valid,       // COUNT(col)
    SumI,        // SUM over int family / DECIMAL64 (exact, overflow fails loud)
    SumF,        // SUM over float
    SumD128,     // SUM over DECIMAL128 (exact int128)
    AvgI, AvgF, AvgD128,
    MinMaxNum,   // MIN or MAX over fixed-width (normalized order key + raw)
    MinMaxD128,
    MinMaxStr,
    CountDistinct,   // per-group dedup on serialized value bytes; count in valid
    ArrayAgg,        // per-group element list (capped); emits one ARRAY per group
    Stddev,      // population stddev — always double, one kind for int AND float
                 // operands (no exactness requirement, unlike Sum/Avg's I/F split)
    StddevSamp,  // sample stddev (N-1 denominator) — same Σx/Σx² lanes as Stddev,
                 // NULL below 2 valid rows (see AggFn::StddevSamp)
    VarPop,      // population variance — Stddev's lanes/NULL rule, no final sqrt
    VarSamp,     // sample variance — StddevSamp's lanes/NULL rule, no final sqrt
    Median,      // exact median — buffers per-group values (MedianState), sorts
                 // and interpolates at finalize; always double, numeric-only
    ApproxCountDistinct,  // HyperLogLog++ sketch per group; always INT64, never NULL
                          // (matches CountDistinct: 0 for an empty group, not NULL)
    ApproxPercentile,     // t-digest sketch per group; always DOUBLE, numeric-only
                          // (same restriction as Median — see median_operand_supported)
    Corr,        // Pearson correlation — six double lanes (Σx, Σx², Σy, Σy², Σxy
                 // + pair count in valid); always DOUBLE, numeric-only operands
    CidrAgg,     // per-group Roaring set of IPv4 addresses; emits one
                 // ARRAY<VARCHAR> of CIDR blocks per group. Never NULL — a group
                 // with no non-NULL addresses emits an EMPTY array, matching
                 // "the set of addresses seen", which is a real and empty answer
                 // rather than an unknown one.
};

// Which GBArrayAggState lane an ARRAY_AGG operand's values live in. One store per
// operand type — chosen once per spec, never per row.
enum class AAStore : uint8_t {
    Raw,    // int/uint family, BOOL, temporal, DECIMAL64, floats (bit pattern)
    I128,   // DECIMAL128
    Str,    // VARCHAR / NVARCHAR / VARBINARY
};

inline AAStore aa_store_of(DrakenType t) {
    if (sort_type_is_string(t)) return AAStore::Str;
    if (t == DRAKEN_DECIMAL128) return AAStore::I128;
    return AAStore::Raw;
}

// ARRAY_AGG accepts every type the other aggregates take PLUS the string family —
// it copies values rather than ordering or summing them.
inline bool aa_operand_supported(DrakenType t) {
    return agg2_operand_supported(t) || sort_type_is_string(t);
}

// CIDR_AGG requires a real IPv4 column: DRAKEN_UINT32 REFINED by
// LogicalKind::IPV4, never a bare UINT32.
//
// The descriptor is the only thing separating an address from any other 32-bit
// unsigned number, and the output is meaningless without it — CIDR_AGG over a
// count or an id column would fold integers into confident, well-formed,
// entirely fictional network ranges. That is the silent-wrong-answer shape this
// codebase refuses, so the gate is on the descriptor rather than the width.
//
// Known consequence, accepted with the architect: the catalog write path
// currently drops logical types, so a column that genuinely holds addresses but
// lost its IPV4 tag is refused here until that is fixed. A loud refusal on real
// data beats a plausible answer over data we cannot confirm is addresses.
inline bool cidr_operand_supported(DrakenType t, const LogicalType* logical) noexcept {
    return t == DRAKEN_UINT32 && logical != nullptr && logical->kind == LogicalKind::IPV4;
}

// ---------------------------------------------------------------------------
// ARRAY_AGG memory guard — a GLOBAL byte budget across every GBArrayAggState,
// the same shape MedianState uses (see _agg_kernels.hpp). ARRAY_AGG buffers
// every input row (NULLs included) into some group's list, so the real OOM risk
// is the TOTAL across all groups: the per-group element cap this replaced
// bounded nothing, because the group count is unbounded, while still refusing
// ordinary group sizes. Charged on capacity growth (amortized — one atomic op
// per doubling, never per append) and released on free. Past the budget,
// `overflowed` latches and appends are refused; emit_array_lane_column checks
// and raises. Silently truncating a list would be a wrong answer wearing the
// shape of a right one.
//
// The counter is a per-shared-object static (inline function local), matching
// the MedianState convention: the native engine is the only thing that
// compiles this header, so there is exactly one instance and every ARRAY_AGG
// buffer in the process accounts against it. A second .so including this
// header would get its own counter and its own budget.
// ---------------------------------------------------------------------------
constexpr int64_t kArrayAggBudgetBytes = opteryx::agg_budgets::kArrayAggBytes;   // 512MB, all groups

inline std::atomic<int64_t>& array_agg_budget_used() noexcept {
    static std::atomic<int64_t> used{0};
    return used;
}

// Reserve `delta` bytes against the global budget. Returns false (and reserves
// nothing) if that would breach the ceiling.
inline bool aa_budget_take(int64_t delta) noexcept {
    if (delta <= 0) return true;
    if (array_agg_budget_used().fetch_add(delta) + delta > kArrayAggBudgetBytes) {
        array_agg_budget_used().fetch_sub(delta);
        return false;
    }
    return true;
}

inline void aa_budget_give(int64_t delta) noexcept {
    if (delta > 0) array_agg_budget_used().fetch_sub(delta);
}

// Bytes a std::string holds inline before it touches the heap. libc++ and
// libstdc++ differ, so ask the implementation rather than hard-coding either.
// An inline variable (initialized at load time) rather than a function-local
// static, so the charge path is a plain load and not a guarded one.
inline const size_t kAaSsoCapacity = std::string().capacity();

// Heap bytes one element string costs ON TOP of its std::string header. A short
// string lives inside the header, which the per-element capacity charge already
// covers — charging its length again would systematically over-report exactly
// the workload (ARRAY_AGG over short codes) the budget should be most relaxed
// about.
inline int64_t aa_string_heap_bytes(uint32_t len) noexcept {
    return static_cast<size_t>(len) > kAaSsoCapacity
               ? static_cast<int64_t>(len) + 1   // + the NUL std::string keeps
               : 0;
}

// One group's ARRAY_AGG elements. Exactly one value lane is populated (the one
// aa_store_of picks); `nulls` is parallel to it and is the authoritative element
// count. NULLs are kept as elements — every other aggregate skips them, but
// ARRAY_AGG(col) over [1, NULL] is [1, NULL], not [1]. Null positions still push
// a placeholder into the value lane so the two stay index-aligned.
//
// Non-copyable and move-only: the destructor returns this state's charge to the
// global budget, so a copy would double-release. (GBLanes, which holds these,
// is already move-only for the same reason via MedianState.)
struct GBArrayAggState {
    std::vector<int64_t>     raws;
    std::vector<__int128>    i128s;
    std::vector<std::string> strs;
    std::vector<uint8_t>     nulls;   // 1 == element is NULL
    // The two halves of this state's budget charge, tracked apart because a
    // merge TRANSFERS the heap half (the strings change owner, not owner-count)
    // while re-charging the capacity half on the destination.
    int64_t vec_charged  = 0;         // reserved capacity: nulls + the live lane
    int64_t heap_charged = 0;         // element strings' out-of-header bytes
    size_t  cap          = 0;         // reserved elements (nulls and lane agree)
    bool    overflowed   = false;     // budget refused an append; raised at finalize

    GBArrayAggState() noexcept = default;
    GBArrayAggState(const GBArrayAggState&) = delete;
    GBArrayAggState& operator=(const GBArrayAggState&) = delete;

    GBArrayAggState(GBArrayAggState&& o) noexcept
        : raws(std::move(o.raws)), i128s(std::move(o.i128s)),
          strs(std::move(o.strs)), nulls(std::move(o.nulls)),
          vec_charged(o.vec_charged), heap_charged(o.heap_charged),
          cap(o.cap), overflowed(o.overflowed) {
        o.vec_charged = 0; o.heap_charged = 0; o.cap = 0;
    }

    GBArrayAggState& operator=(GBArrayAggState&& o) noexcept {
        if (this != &o) {
            aa_budget_give(vec_charged + heap_charged);
            raws = std::move(o.raws); i128s = std::move(o.i128s);
            strs = std::move(o.strs); nulls = std::move(o.nulls);
            vec_charged = o.vec_charged; heap_charged = o.heap_charged;
            cap = o.cap; overflowed = o.overflowed;
            o.vec_charged = 0; o.heap_charged = 0; o.cap = 0;
        }
        return *this;
    }

    ~GBArrayAggState() noexcept {
        aa_budget_give(vec_charged + heap_charged);
        vec_charged = 0; heap_charged = 0;
    }

    size_t size() const noexcept { return nulls.size(); }

    // Bytes one element costs in the nulls lane plus the live value lane. For
    // the string lane this is the std::string HEADER only — its heap bytes are
    // charged per string, where the length is known.
    static inline int64_t _elem_bytes(AAStore st) noexcept {
        switch (st) {
            case AAStore::Raw:  return 1 + static_cast<int64_t>(sizeof(int64_t));
            case AAStore::I128: return 1 + static_cast<int64_t>(sizeof(__int128));
            case AAStore::Str:  return 1 + static_cast<int64_t>(sizeof(std::string));
        }
        return 1 + static_cast<int64_t>(sizeof(int64_t));
    }

    // Reserve room for `need` elements, charging the growth to the global
    // budget. Doubling, so the charge is amortized. Reserving both lanes here
    // (rather than letting push_back grow them) is what keeps `cap` an honest
    // record of what has been charged. Returns false and latches `overflowed`
    // when the budget refuses — nothing is reserved and nothing is charged.
    inline bool _reserve(AAStore st, size_t need) noexcept {
        if (need <= cap) return true;
        size_t new_cap = cap == 0 ? 8 : cap * 2;
        while (new_cap < need) new_cap *= 2;
        const int64_t delta = static_cast<int64_t>(new_cap - cap) * _elem_bytes(st);
        if (!aa_budget_take(delta)) { overflowed = true; return false; }
        nulls.reserve(new_cap);
        switch (st) {
            case AAStore::Raw:  raws.reserve(new_cap); break;
            case AAStore::I128: i128s.reserve(new_cap); break;
            case AAStore::Str:  strs.reserve(new_cap); break;
        }
        cap = new_cap;
        vec_charged += delta;
        return true;
    }

    // Append one element. Returns false once the budget refuses (and latches
    // `overflowed`) so callers stop copying bytes into a doomed group.
    //
    // A long element string takes its heap charge here, one atomic per string
    // rather than per doubling. That is deliberate: the charge accompanies a
    // real malloc of the same bytes, which costs more than the fetch_add does,
    // and the alternative (reserving heap in geometric blocks like capacity)
    // would over-charge by up to 2x and halve the usable budget for exactly the
    // long-string workload the guard exists for. Short strings live in the
    // header and pay nothing extra — see aa_string_heap_bytes.
    inline bool push(AAStore st, bool is_null, int64_t raw, __int128 big,
                     const char* sp, uint32_t slen) noexcept {
        if (nulls.size() == cap && !_reserve(st, nulls.size() + 1)) return false;
        if (st == AAStore::Str && !is_null) {
            const int64_t heap = aa_string_heap_bytes(slen);
            if (heap != 0) {
                if (!aa_budget_take(heap)) { overflowed = true; return false; }
                heap_charged += heap;
            }
        }
        nulls.push_back(is_null ? 1 : 0);
        switch (st) {
            case AAStore::Raw:  raws.push_back(is_null ? 0 : raw); break;
            case AAStore::I128: i128s.push_back(is_null ? static_cast<__int128>(0) : big); break;
            case AAStore::Str:
                if (is_null) strs.emplace_back();
                else strs.emplace_back(sp, slen);
                break;
        }
        return true;
    }

    // Concatenate `src`'s elements onto this state, draining it — `src` is
    // released right after the merge, so its element strings are MOVED, never
    // copied. Returns false (latching `overflowed`) when the budget refuses the
    // destination's growth; `src` is left untouched in that case.
    //
    // The moved strings' heap bytes are TRANSFERRED, not re-charged: the bytes
    // changed owner, not owner-count, so the global counter must not move for
    // them. Only the destination's capacity growth is a new charge; the source's
    // own capacity is returned when it is destroyed.
    inline bool append_from(AAStore st, GBArrayAggState& src) noexcept {
        if (src.overflowed) overflowed = true;
        const size_t take = src.size();
        if (take == 0) return true;
        if (!_reserve(st, nulls.size() + take)) return false;
        nulls.insert(nulls.end(), src.nulls.begin(), src.nulls.end());
        switch (st) {
            case AAStore::Raw:
                raws.insert(raws.end(), src.raws.begin(), src.raws.end());
                break;
            case AAStore::I128:
                i128s.insert(i128s.end(), src.i128s.begin(), src.i128s.end());
                break;
            case AAStore::Str:
                strs.insert(strs.end(),
                            std::make_move_iterator(src.strs.begin()),
                            std::make_move_iterator(src.strs.end()));
                break;
        }
        heap_charged += src.heap_charged;
        src.heap_charged = 0;
        return true;
    }
};


inline GBKind gb_kind_of(const AggSpec2& sp, const AggColMeta& m) {
    switch (sp.fn) {
        case AggFn::CountStar:     return GBKind::Rows;
        case AggFn::Count:         return GBKind::Valid;
        case AggFn::CountDistinct: return GBKind::CountDistinct;
        case AggFn::ArrayAgg:      return GBKind::ArrayAgg;
        case AggFn::CidrAgg:       return GBKind::CidrAgg;
        case AggFn::Sum:
            if (m.type == DRAKEN_DECIMAL128) return GBKind::SumD128;
            return m.is_float ? GBKind::SumF : GBKind::SumI;
        case AggFn::Avg:
            if (m.type == DRAKEN_DECIMAL128) return GBKind::AvgD128;
            return m.is_float ? GBKind::AvgF : GBKind::AvgI;
        case AggFn::Min:
        case AggFn::Max:
        case AggFn::AnyValue:   // always the min-direction lane — see AggFn::AnyValue
            if (m.is_string) return GBKind::MinMaxStr;
            if (m.type == DRAKEN_DECIMAL128) return GBKind::MinMaxD128;
            return GBKind::MinMaxNum;
        case AggFn::Stddev:
            return GBKind::Stddev;
        case AggFn::StddevSamp:
            return GBKind::StddevSamp;
        case AggFn::VarPop:
            return GBKind::VarPop;
        case AggFn::VarSamp:
            return GBKind::VarSamp;
        case AggFn::Median:
            return GBKind::Median;
        case AggFn::ApproxCountDistinct:
            return GBKind::ApproxCountDistinct;
        case AggFn::ApproxPercentile:
            return GBKind::ApproxPercentile;
        case AggFn::Corr:
            return GBKind::Corr;
    }
    return GBKind::Rows;   // unreachable
}

// Exact 16-byte lane values (DECIMAL128 sums/extremes) as a DECIMAL128 column
// carrying the operand's own descriptor (the raw unscaled domain is closed
// under addition; int128 overflow at any real scale is astronomically out of
// range). NULL where valid[i] == 0 (valid == nullptr → all valid).
inline CxxColumn emit_i128_lane_column(const __int128* vals, const int64_t* valid,
                                       uint32_t n, const LogicalType* logical) {
    size_t alloc_n = (n == 0 ? 1 : n);
    auto* data = static_cast<uint8_t*>(draken_malloc(alloc_n * 16u));
    size_t vbytes = (static_cast<size_t>(n) + 7) / 8;
    uint8_t* vbits = nullptr;
    for (uint32_t i = 0; i < n; ++i) {
        if (valid != nullptr && valid[i] == 0) {
            if (vbits == nullptr) {
                vbits = static_cast<uint8_t*>(draken_malloc(vbytes == 0 ? 1 : vbytes));
                std::memset(vbits, 0xFF, vbytes == 0 ? 1 : vbytes);
            }
            vbits[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7)));
            std::memset(data + static_cast<size_t>(i) * 16u, 0, 16u);
            continue;
        }
        std::memcpy(data + static_cast<size_t>(i) * 16u, &vals[i], 16u);
    }
    uint32_t* sel = static_cast<uint32_t*>(draken_malloc(alloc_n * sizeof(uint32_t)));
    for (uint32_t i = 0; i < n; ++i) sel[i] = i;
    DrakenVector v;
    v.data = data; v.selection = sel; v.data_length = n; v.length = n;
    v.validity = vbits; v.type = DRAKEN_DECIMAL128;
    v.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
    CxxColumn c;
    c.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(data),
                                          OwnedBuffer<uint8_t>(vbits), OwnedBuffer<void>(sel));
    c.own->logical_type = logical;
    c.view = c.own->vec;
    return c;
}

// Per-spec lane slices for one emit chunk of `n` groups. Only the pointers the
// spec's kind reads are set.
struct GBLaneView {
    const int64_t*     rows  = nullptr;
    const int64_t*     valid = nullptr;
    const int64_t*     i64   = nullptr;   // int sums / MIN-MAX raw containers
    const double*      f64   = nullptr;   // float sums / STDDEV+CORR Σx
    const double*      f64sq = nullptr;   // STDDEV+CORR Σx²
    const double*      f64y  = nullptr;   // CORR Σy
    const double*      f64yy = nullptr;   // CORR Σy²
    const double*      f64xy = nullptr;   // CORR Σxy
    const __int128*    i128  = nullptr;   // DECIMAL128 sums / extremes
    const std::string* sval  = nullptr;   // string extremes
    GBArrayAggState*   aa    = nullptr;   // ARRAY_AGG element lists
    const AggSpec2*    aa_spec = nullptr; // ARRAY_AGG DISTINCT/ORDER BY/LIMIT modifiers
    opteryx::roaring32::Roaring32* cidr = nullptr;   // CIDR_AGG per-group address sets
    opteryx::ungrouped::MedianState* median = nullptr;   // MEDIAN per-group buffers
    const HllppSketch* hll = nullptr;     // APPROX_COUNT_DISTINCT per-group sketches
    TDigestPtr* td = nullptr;             // APPROX_PERCENTILE per-group sketches
    const AggSpec2* pct_spec = nullptr;   // APPROX_PERCENTILE's percentile parameter
};

// Steal a freshly-built column's owner into a unique_ptr for VectorOwner::child_owner.
// Safe ONLY for a column this call just created (use_count()==1): VectorOwner is
// move-constructible and its OwnedBuffers are unique_ptrs, so the husk left behind
// frees nothing. This reuses the real emitters for the child rather than growing a
// second copy of every per-type build.
inline std::unique_ptr<VectorOwner> aa_steal_owner(CxxColumn&& c) {
    return std::make_unique<VectorOwner>(std::move(*c.own));
}

// Apply DISTINCT → ORDER BY → LIMIT to one group's element list, in that order.
// Matches SQL evaluation order: DISTINCT collapses duplicates, ORDER BY sorts what
// survives, LIMIT truncates last. `idx` is rewritten to the surviving element
// positions so the caller can gather from whichever value lane is live.
inline void aa_finalize_group(const AggSpec2& sp, AAStore st,
                              const GBArrayAggState& A, std::vector<uint32_t>& idx) {
    const uint32_t n = static_cast<uint32_t>(A.size());
    idx.clear();
    idx.reserve(n);
    for (uint32_t i = 0; i < n; ++i) idx.push_back(i);

    if (sp.aa_distinct) {
        // First-seen wins (the order the rows arrived), so an unordered
        // ARRAY_AGG(DISTINCT x) keeps its first occurrence like the serial plan.
        // NULL is one distinct element, not one per row.
        std::vector<uint32_t> keep;
        keep.reserve(idx.size());
        bool seen_null = false;
        std::unordered_set<int64_t> seen_raw;
        std::set<__int128> seen_i128;
        std::unordered_set<std::string_view> seen_str;
        for (uint32_t i : idx) {
            if (A.nulls[i]) {
                if (seen_null) continue;
                seen_null = true;
                keep.push_back(i);
                continue;
            }
            bool fresh = false;
            switch (st) {
                case AAStore::Raw:  fresh = seen_raw.insert(A.raws[i]).second; break;
                case AAStore::I128: fresh = seen_i128.insert(A.i128s[i]).second; break;
                case AAStore::Str:
                    fresh = seen_str.insert(std::string_view(A.strs[i])).second;
                    break;
            }
            if (fresh) keep.push_back(i);
        }
        idx.swap(keep);
    }

    if (sp.aa_ordered) {
        // NULLs sort last in both directions (they carry no order key), matching
        // the ORDER BY the aggregate's own modifier expresses.
        const bool desc = sp.aa_descending;
        std::stable_sort(idx.begin(), idx.end(), [&](uint32_t a, uint32_t b) {
            if (A.nulls[a] != A.nulls[b]) return A.nulls[a] < A.nulls[b];
            if (A.nulls[a]) return false;
            switch (st) {
                case AAStore::Raw:
                    return desc ? A.raws[a] > A.raws[b] : A.raws[a] < A.raws[b];
                case AAStore::I128:
                    return desc ? A.i128s[a] > A.i128s[b] : A.i128s[a] < A.i128s[b];
                case AAStore::Str:
                    return desc ? A.strs[a] > A.strs[b] : A.strs[a] < A.strs[b];
            }
            return false;
        });
    }

    if (sp.aa_limit >= 0 && static_cast<int64_t>(idx.size()) > sp.aa_limit)
        idx.resize(static_cast<size_t>(sp.aa_limit));
}

// Emit ARRAY_AGG as a DRAKEN_ARRAY column: `data` is an int32 offsets buffer of
// n+1 entries and the elements live in a flat child vector hung off
// VectorOwner::child_owner (parent-owns-child RAII — the layout
// make_array_from_sequence builds and cxx_column_child_vec reads).
//
// The ARRAY rows themselves are never NULL: a group exists because it has rows, and
// LIMIT 0 yields an empty list, which is `[]` and not NULL. Element-level NULLs ride
// the child's validity bitmap.
inline CxxColumn emit_array_lane_column(const AggColMeta& meta, const AggSpec2& sp,
                                        GBArrayAggState* states, uint32_t n,
                                        ErrCtx& err) {
    for (uint32_t g = 0; g < n; ++g) {
        if (!states[g].overflowed) continue;
        err.code = 1;
        // Deliberately does NOT suggest the aggregate's own LIMIT: ARRAY_AGG's
        // DISTINCT/ORDER BY/LIMIT modifiers all apply at finalize, AFTER every
        // element has been buffered, so they reduce the OUTPUT and never the
        // memory this budget guards. Only reading fewer rows into the aggregate
        // does that.
        // Names the budget, quotes no figure — see kMedianCapExceededMsg for why
        // a literal limit in the text is a copy that a differently-configured
        // build silently falsifies.
        err.msg = "ARRAY_AGG — buffered elements exceeded the memory "
                  "budget (see @@array_agg_memory_budget_bytes). Filter the input or "
                  "narrow the groups so fewer rows reach the aggregate; ARRAY_AGG's "
                  "own LIMIT will not help, it truncates the finished list rather "
                  "than what is buffered.";
        return CxxColumn{};
    }

    const AAStore st = aa_store_of(meta.type);
    const size_t off_bytes = (static_cast<size_t>(n) + 1) * sizeof(int32_t);
    int32_t* offsets = static_cast<int32_t*>(draken_malloc(off_bytes));
    offsets[0] = 0;

    // Pass 1: finalize each group and flatten the survivors into child lanes.
    std::vector<int64_t>     craw;
    std::vector<__int128>    ci128;
    std::vector<std::string> cstr;
    std::vector<uint8_t>     cvalid;   // 0 == element is NULL
    std::vector<uint32_t>    idx;
    for (uint32_t g = 0; g < n; ++g) {
        GBArrayAggState& A = states[g];
        aa_finalize_group(sp, st, A, idx);
        for (uint32_t i : idx) {
            cvalid.push_back(A.nulls[i] ? 0 : 1);
            switch (st) {
                case AAStore::Raw:  craw.push_back(A.raws[i]); break;
                case AAStore::I128: ci128.push_back(A.i128s[i]); break;
                case AAStore::Str:  cstr.push_back(A.strs[i]); break;
            }
        }
        offsets[g + 1] = offsets[g] + static_cast<int32_t>(idx.size());
    }

    const uint32_t total = static_cast<uint32_t>(cvalid.size());
    CxxColumn child_col;
    switch (st) {
        case AAStore::Raw:
            child_col = emit_fixed_column(craw.data(), cvalid.data(), total,
                                          meta.type, meta.logical, err);
            if (err.code != 0) return CxxColumn{};
            break;
        case AAStore::I128: {
            // emit_i128_lane_column reads a per-row int64 "valid" lane, not the
            // uint8 flags the other emitters take.
            std::vector<int64_t> v64(total);
            for (uint32_t i = 0; i < total; ++i) v64[i] = cvalid[i];
            child_col = emit_i128_lane_column(ci128.data(), v64.data(), total,
                                              meta.logical);
            break;
        }
        case AAStore::Str: {
            std::vector<int64_t> v64(total);
            for (uint32_t i = 0; i < total; ++i) v64[i] = cvalid[i];
            child_col = emit_string_lane_column(meta, cstr.data(), v64.data(), total);
            break;
        }
    }

    uint32_t* sel = static_cast<uint32_t*>(
        draken_malloc((n == 0 ? 1 : n) * sizeof(uint32_t)));
    for (uint32_t i = 0; i < n; ++i) sel[i] = i;
    DrakenVector v;
    v.data = offsets; v.selection = sel; v.data_length = n; v.length = n;
    v.validity = nullptr; v.type = DRAKEN_ARRAY;
    v.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
    CxxColumn c;
    c.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(offsets),
                                          OwnedBuffer<uint8_t>(nullptr),
                                          OwnedBuffer<void>(sel));
    c.own->child_owner = aa_steal_owner(std::move(child_col));
    c.view = c.own->vec;
    return c;
}

// One ARRAY<VARCHAR> of CIDR blocks per group.
//
// NEVER NULL. A group whose addresses were every one of them NULL emits an
// EMPTY array, not a NULL one: the answer to "which addresses did this group
// hold" is a set, and the empty set is a real answer. That differs from
// MIN/SUM, where no values means no value is defined — hence no `valid` lane
// for this kind (see gb_lanes_resize).
inline CxxColumn emit_cidr_lane_column(opteryx::roaring32::Roaring32* states,
                                       uint32_t n, ErrCtx& err) {
    // Collection budget first. A set that stopped accepting addresses describes
    // FEWER addresses than the data held, so every block derived from it would
    // be well-formed, confident and wrong — and smaller, which is the direction
    // nothing downstream can detect.
    for (uint32_t g = 0; g < n; ++g) {
        if (!states[g].overflowed) continue;
        err.code = 1;
        // Names the budget, quotes no figure. `err.msg` is a const char*, so a
        // computed string would dangle; a hardcoded "512MB" would be worse still
        // — it is a second copy of the limit that a build with a different
        // constant silently falsifies, sending the reader to tune a number
        // nothing enforces. The variable IS the discoverable figure.
        err.msg = "CIDR_AGG — the address sets exceeded the state "
                  "memory budget (see @@cidr_agg_state_budget_bytes). Narrow the "
                  "groups or filter the input so fewer DISTINCT addresses reach the "
                  "aggregate; repeated addresses cost nothing, the set dedups on "
                  "insert, so only the distinct count counts against this.";
        return CxxColumn{};
    }

    // Built into vectors first and copied into draken buffers only once success
    // is certain: the emit budget can refuse partway through, and allocating the
    // offsets up front would mean a raw buffer to free on every error path.
    std::vector<std::string> cstr;
    std::vector<int32_t> offs(static_cast<size_t>(n) + 1, 0);
    char buf[opteryx::cidr::kMaxCidrTextBytes];
    int64_t charged = 0;
    bool over = false;

    for (uint32_t g = 0; g < n && !over; ++g) {
        opteryx::cidr::emit_cidrs(states[g], [&](uint32_t base, uint8_t prefix) {
            // Charge the EXACT text length before rendering it — cidr_text_length
            // predicts what format_cidr will write, so the budget is never
            // discovered to be blown only after the bytes exist.
            const int64_t need = opteryx::cidr::cidr_text_length(base, prefix);
            if (!opteryx::cidr::emit_budget_take(need)) { over = true; return false; }
            charged += need;
            const uint32_t len = opteryx::cidr::format_cidr(base, prefix, buf);
            cstr.emplace_back(buf, len);
            return true;
        });
        // int32 offsets cannot overflow here: the emit budget caps the block
        // count at 512MB / 9 bytes ~= 59.6M, two orders below INT32_MAX. Without
        // that budget the degenerate input (2^31 blocks) would exceed it by one.
        offs[g + 1] = static_cast<int32_t>(cstr.size());
    }

    opteryx::cidr::emit_budget_give(charged);
    if (over) {
        err.code = 1;
        err.msg = "CIDR_AGG — the emitted CIDR list exceeded the "
                  "output budget (see @@cidr_agg_emit_budget_bytes). This is a "
                  "SEPARATE limit from the state budget and fitting in memory does "
                  "not imply fitting here: the worst case is half-density input "
                  "(every other address), where no block can be folded and the "
                  "cover is one /32 per address.";
        return CxxColumn{};
    }

    const uint32_t total = static_cast<uint32_t>(cstr.size());
    std::vector<int64_t> v64(total == 0 ? 1 : total, 1);   // no element is ever NULL
    AggColMeta smeta;
    smeta.type = DRAKEN_VARCHAR;   // the ELEMENT type, not the operand's UINT32
    CxxColumn child_col = emit_string_lane_column(smeta, cstr.data(), v64.data(), total);

    const size_t off_bytes = (static_cast<size_t>(n) + 1) * sizeof(int32_t);
    int32_t* offsets = static_cast<int32_t*>(draken_malloc(off_bytes));
    std::memcpy(offsets, offs.data(), off_bytes);
    uint32_t* sel = static_cast<uint32_t*>(
        draken_malloc((n == 0 ? 1 : n) * sizeof(uint32_t)));
    for (uint32_t i = 0; i < n; ++i) sel[i] = i;
    DrakenVector v;
    v.data = offsets; v.selection = sel; v.data_length = n; v.length = n;
    v.validity = nullptr; v.type = DRAKEN_ARRAY;
    v.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
    CxxColumn c;
    c.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(offsets),
                                          OwnedBuffer<uint8_t>(nullptr),
                                          OwnedBuffer<void>(sel));
    c.own->child_owner = aa_steal_owner(std::move(child_col));
    c.view = c.own->vec;
    return c;
}

// One aggregate output column over `n` groups from columnar lanes.
inline CxxColumn emit_lane_column(const AggColMeta& meta, GBKind kind,
                                  const GBLaneView& L, uint32_t n, ErrCtx& err) {
    std::vector<uint8_t> ok;
    auto valid_ok = [&]() -> const uint8_t* {
        ok.resize(n);
        for (uint32_t i = 0; i < n; ++i) ok[i] = L.valid[i] > 0 ? 1 : 0;
        return ok.data();
    };
    switch (kind) {
        case GBKind::Rows:
            return emit_fixed_column(L.rows, nullptr, n, DRAKEN_INT64, nullptr, err);
        case GBKind::Valid:
        case GBKind::CountDistinct:
            // COUNT(col) / COUNT(DISTINCT col) over zero valid rows is 0, not NULL.
            return emit_fixed_column(L.valid, nullptr, n, DRAKEN_INT64, nullptr, err);
        case GBKind::SumI: {
            // exact integer sums (overflow already failed loud at accumulate);
            // DECIMAL keeps its type + descriptor.
            DrakenType t = (meta.type == DRAKEN_DECIMAL) ? DRAKEN_DECIMAL : DRAKEN_INT64;
            return emit_fixed_column(L.i64, valid_ok(), n, t,
                                     (meta.type == DRAKEN_DECIMAL) ? meta.logical
                                                                   : nullptr, err);
        }
        case GBKind::SumF: {
            std::vector<int64_t> raws(n, 0);
            for (uint32_t i = 0; i < n; ++i)
                std::memcpy(&raws[i], &L.f64[i], sizeof(double));
            return emit_fixed_column(raws.data(), valid_ok(), n, DRAKEN_FLOAT64,
                                     nullptr, err);
        }
        case GBKind::SumD128:
        case GBKind::MinMaxD128:
            return emit_i128_lane_column(L.i128, L.valid, n, meta.logical);
        case GBKind::AvgI:
        case GBKind::AvgF:
        case GBKind::AvgD128: {
            // DECIMAL operands accumulated EXACT raw integer sums; the average is
            // raw/valid rescaled by the operand's own descriptor scale.
            double denom_scale = 1.0;
            if (meta.type == DRAKEN_DECIMAL || meta.type == DRAKEN_DECIMAL128) {
                if (meta.logical == nullptr) {
                    err.code = 1;
                    err.msg = "AVG(DECIMAL): operand carries no scale descriptor — "
                              "fail loud, never a mis-scaled average";
                    return CxxColumn{};
                }
                denom_scale = std::pow(10.0, static_cast<double>(meta.logical->scale));
            }
            std::vector<int64_t> raws(n, 0);
            const uint8_t* okp = valid_ok();
            for (uint32_t i = 0; i < n; ++i) {
                if (okp[i] == 0) continue;
                // AvgI and AvgD128 both sum in the exact int128 lane — the
                // AVERAGE must survive sums that exceed INT64 (AVG(UserID)
                // over 99M hash-scale ids is a real workload).
                double num = (kind == GBKind::AvgF) ? L.f64[i]
                                                    : static_cast<double>(L.i128[i]);
                double a = num / static_cast<double>(L.valid[i]) / denom_scale;
                std::memcpy(&raws[i], &a, sizeof(double));
            }
            return emit_fixed_column(raws.data(), okp, n, DRAKEN_FLOAT64, nullptr, err);
        }
        case GBKind::MinMaxNum:
            return emit_fixed_column(L.i64, valid_ok(), n, meta.type, meta.logical, err);
        case GBKind::MinMaxStr:
            return emit_string_lane_column(meta, L.sval, L.valid, n);
        case GBKind::ArrayAgg:
            return emit_array_lane_column(meta, *L.aa_spec, L.aa, n, err);
        case GBKind::CidrAgg:
            return emit_cidr_lane_column(L.cidr, n, err);
        case GBKind::Stddev: {
            // Population variance: E[x^2] - E[x]^2, clamped to 0 (float rounding
            // can push a near-zero true variance slightly negative, which would
            // otherwise NaN the sqrt). Always DOUBLE, regardless of operand type.
            std::vector<int64_t> raws(n, 0);
            const uint8_t* okp = valid_ok();
            for (uint32_t i = 0; i < n; ++i) {
                if (okp[i] == 0) continue;
                double cnt = static_cast<double>(L.valid[i]);
                double mean = L.f64[i] / cnt;
                double variance = (L.f64sq[i] / cnt) - (mean * mean);
                if (variance < 0.0) variance = 0.0;
                double sd = std::sqrt(variance);
                std::memcpy(&raws[i], &sd, sizeof(double));
            }
            return emit_fixed_column(raws.data(), okp, n, DRAKEN_FLOAT64, nullptr, err);
        }
        case GBKind::VarPop: {
            // Same population-variance formula as GBKind::Stddev, without the
            // final sqrt — see that case for the clamp rationale.
            std::vector<int64_t> raws(n, 0);
            const uint8_t* okp = valid_ok();
            for (uint32_t i = 0; i < n; ++i) {
                if (okp[i] == 0) continue;
                double cnt = static_cast<double>(L.valid[i]);
                double mean = L.f64[i] / cnt;
                double variance = (L.f64sq[i] / cnt) - (mean * mean);
                if (variance < 0.0) variance = 0.0;
                std::memcpy(&raws[i], &variance, sizeof(double));
            }
            return emit_fixed_column(raws.data(), okp, n, DRAKEN_FLOAT64, nullptr, err);
        }
        case GBKind::StddevSamp:
        case GBKind::VarSamp: {
            // Sample (Bessel-corrected) variance: N/(N-1) * population variance —
            // algebraically identical to (Σx² - N·mean²)/(N-1), computed by
            // scaling the already-clamped population variance so the clamp still
            // holds. Undefined below 2 valid rows (N-1 == 0): NULL, not a
            // divide-by-zero or 0 — matches DuckDB/the SQL standard, unlike
            // GBKind::Stddev/VarPop, which ARE defined at N==1 (variance 0).
            std::vector<int64_t> raws(n, 0);
            std::vector<uint8_t> okp(n, 0);
            for (uint32_t i = 0; i < n; ++i) {
                if (L.valid[i] < 2) continue;
                okp[i] = 1;
                double cnt = static_cast<double>(L.valid[i]);
                double mean = L.f64[i] / cnt;
                double variance = (L.f64sq[i] / cnt) - (mean * mean);
                if (variance < 0.0) variance = 0.0;
                double sample_variance = variance * (cnt / (cnt - 1.0));
                double out_val = (kind == GBKind::StddevSamp) ? std::sqrt(sample_variance)
                                                               : sample_variance;
                std::memcpy(&raws[i], &out_val, sizeof(double));
            }
            return emit_fixed_column(raws.data(), okp.data(), n, DRAKEN_FLOAT64, nullptr, err);
        }
        case GBKind::Corr: {
            // NULL-ness is NOT just valid==0: a group with pairs but zero
            // variance in either operand has no defined correlation — its own
            // okp copy, not the shared valid_ok() view.
            std::vector<int64_t> raws(n, 0);
            std::vector<uint8_t> okp(n, 0);
            for (uint32_t i = 0; i < n; ++i) {
                double r;
                if (!corr_finalize(static_cast<double>(L.valid[i]), L.f64[i],
                                   L.f64sq[i], L.f64y[i], L.f64yy[i], L.f64xy[i], r))
                    continue;
                okp[i] = 1;
                std::memcpy(&raws[i], &r, sizeof(double));
            }
            return emit_fixed_column(raws.data(), okp.data(), n, DRAKEN_FLOAT64, nullptr, err);
        }
        case GBKind::Median: {
            // Per-group state, not a fixed lane — null-ness is the state's own
            // `size == 0` (zero non-null values seen), not a parallel valid[]
            // array (Median never allocates one — see gb_lanes_resize).
            std::vector<int64_t> raws(n, 0);
            std::vector<uint8_t> okp(n, 0);
            for (uint32_t i = 0; i < n; ++i) {
                if (L.median[i].size == 0) continue;
                okp[i] = 1;
                double med = L.median[i].finalize_median();
                std::memcpy(&raws[i], &med, sizeof(double));
            }
            return emit_fixed_column(raws.data(), okp.data(), n, DRAKEN_FLOAT64, nullptr, err);
        }
        case GBKind::ApproxCountDistinct: {
            // Never NULL, matching exact CountDistinct: an empty group is 0.
            std::vector<int64_t> raws(n);
            for (uint32_t i = 0; i < n; ++i)
                raws[i] = static_cast<int64_t>(L.hll[i].estimate());
            return emit_fixed_column(raws.data(), nullptr, n, DRAKEN_INT64, nullptr, err);
        }
        case GBKind::ApproxPercentile: {
            double q = L.pct_spec->percentile;
            std::vector<int64_t> raws(n, 0);
            std::vector<uint8_t> okp(n, 0);
            for (uint32_t i = 0; i < n; ++i) {
                if (td_size(L.td[i].h) == 0) continue;
                okp[i] = 1;
                double val = td_quantile(L.td[i].h, q);
                std::memcpy(&raws[i], &val, sizeof(double));
            }
            return emit_fixed_column(raws.data(), okp.data(), n, DRAKEN_FLOAT64, nullptr, err);
        }
    }
    return CxxColumn{};   // unreachable
}


// Mark which PHYSICAL dict slots are referenced by at least one VALID logical
// row — one cheap O(rows) pass, no hashing, no set probes. The dict-shape fast
// paths then do their expensive work once per marked slot instead of per row.
inline void mark_referenced_valid(const DrakenVector& v, std::vector<uint8_t>& ref) {
    ref.assign(v.data_length, 0);
    for (uint32_t i = 0; i < v.length; ++i) {
        if (sort_row_valid(v, i)) ref[v.selection[i]] = 1;
    }
}

// Rebuild the key COLUMNS from the serialized keys (GROUP BY output). Walks every
// group's buffer once per key column; strings get a canonical consolidated block.
struct KeyColMeta {
    DrakenType type = DRAKEN_INT64;
    const LogicalType* logical = nullptr;
    bool captured = false;
};


// ---- shared partitioned-dedup machinery (GroupBy + ungrouped COUNT DISTINCT) -------

constexpr size_t kGBParts = 64;
constexpr int kGBPartShift = 58;   // top 6 bits pick the partition
// Low-cardinality GROUP BY: when the planner's NDV estimate for the grouped
// key product is <= this, each partition fronts its CarcharIndex with a
// 64-slot ParviMap (4 group-selected groups of 16 — still a single SIMD-group
// probe per key; scratch/parvi_size_curve/bench.cpp). Overflow fires when a
// key's GROUP is full, not at 64 keys. At estimate <= 64 over 64 hash
// partitions the expected per-partition load is ~1, so a partition
// overflowing needs the estimate to be wrong by well over an order of
// magnitude — and then it one-shot promotes to its CarcharIndex (dense group
// ids preserved), so a misfire costs one drain of <= 64 entries per
// partition, never a wrong answer.
constexpr int64_t kGBParviGateNDV = 64;
// DISTINCT low-cardinality gate: DistinctSink deduplicates through ONE
// per-worker CarcharSet (no hash partitioning), so its parvi front set is a
// single 64-slot ParviSet. The gate sits below the measured p5=40 effective
// capacity (group-full can fire before 64) to keep the promote rate near
// zero. On overflow the set drains into the CarcharSet and the morsel's
// remaining rows rescan on the carchar path; the rows the parvi pass already
// marked new are kept, so dup-vs-new answers are unaffected.
constexpr int64_t kDistinctParviGateNDV = 16;
// High-NDV adaptive flush: when a worker's TOTAL local group count passes this,
// its partitions are queued for the (parallel) merge and reset. Keeps the
// sink-side probe working set cache-resident on 90M-group aggregations —
// probes into an 11MB+ local table were the profiled Q33 wall. Low-cardinality
// aggregations never reach the cap: zero behavior change. Measured curve
// (Q33, 90M groups): 262144 → 2.21s, 131072 → 1.66s, 65536 → 1.62s,
// 32768 → 1.61s; 65536 picked (flat below it, fewer merge chunks).
constexpr size_t kGBFlushEntries = 65536;
constexpr size_t kGBArenaChunk = 1u << 20;   // 1 MiB key-arena chunks (string-key mode)

// Combine a group id with a value hash into one 64-bit dedup key. Both the sink
// and the merge derive the SAME key for the SAME (group, value) pair, so a
// distinct value is counted once per group regardless of which worker saw it.
//
// The two arguments MUST be pre-mixed by DIFFERENT constructions before they are
// combined. This used to be `mix_K(a) ^ b` with mix_K == draken's own value-hash
// finalizer (`x * 0x9E37...15 + 1`, then `^= >>32`). Draken's per-value hash for
// the integer/BOOL family IS mix_K(raw_value) — so `mix_K(a) ^ b` reduced to
// `mix_K(gid) ^ mix_K(raw)`, which is SYMMETRIC: group 0 holding value 1 and
// group 1 holding value 0 produced the SAME dedup key, and the second of the two
// was silently dropped from COUNT(DISTINCT). BOOL hit it on every query — its
// raw seeds are exactly 0 and 1, and 0/1 are the first two group ids in every
// partition — and a small-integer operand collided on ~half of all (group,
// value) pairs. Which pairs actually met in one partition depended on morsel and
// worker layout, so the undercount was non-deterministic run to run.
//
// Pre-mixing `a` with a different multiplier and `b` with a third breaks the
// shared algebra: a collision now needs mix_a(a1) ^ mix_b(b1) == mix_a(a2) ^
// mix_b(b2) across two unrelated constructions, which is the ordinary 64-bit
// hash-identity contract this engine keys on everywhere else. Each stage is a
// bijection, so the key stays injective in either argument with the other fixed.
inline uint64_t gb_mix2(uint64_t a, uint64_t b) {
    uint64_t ga = a * 0xFF51AFD7ED558CCDULL + 0x165667B19E3779F9ULL;
    ga ^= ga >> 32;
    uint64_t vb = b * 0xC4CEB9FE1A85EC53ULL;
    vb ^= vb >> 29;
    uint64_t h = ga ^ vb;
    h *= 0x9E3779B97F4A7C15ULL;
    h ^= h >> 32;
    return h;
}

// Per-(spec, partition) COUNT(DISTINCT) dedup. Draken owns the value hash
// (cxx_hash_c over the operand column, or every column for COUNT(DISTINCT *));
// the (group_id, value_hash) pair is deduped by 64-bit hash identity in a
// CarcharSet. pair_gid/pair_vhash keep the DISTINCT pairs so the merge can
// RE-KEY each against the merged partition's renumbered group ids.
struct GBCountDistinct {
    opteryx::carchar::CarcharSet seen;   // dedup on gb_mix2(gid, value_hash)
    std::vector<uint32_t> pair_gid;      // group id per distinct pair (pre-merge)
    std::vector<uint64_t> pair_vhash;    // value hash per distinct pair

    size_t size() const { return pair_gid.size(); }
    // Record (gid, vhash); returns true iff this pair is NEW to the set.
    bool insert(uint32_t gid, uint64_t vhash) {
        if (seen.insert_or_ignore(gb_mix2(gid, vhash))) {
            pair_gid.push_back(gid);
            pair_vhash.push_back(vhash);
            return true;
        }
        return false;
    }
};

// Ungrouped COUNT(DISTINCT): per-spec, per-hash-partition dedup of distinct VALUE
// hashes (draken owns the value hash via cxx_hash_c). Partitioned by hash so the
// distinct sets union in parallel at finalize; `distinct` keeps the set's hashes
// for that merge. Ungrouped has no group id, so this is a plain value-hash set.
struct UCDPartition {
    opteryx::carchar::CarcharSet seen;
    std::vector<uint64_t> distinct;
    size_t size() const { return distinct.size(); }
    bool insert(uint64_t h) {
        if (seen.insert_or_ignore(h)) { distinct.push_back(h); return true; }
        return false;
    }
};

// ---- UngroupedAggSink ---------------------------------------------------------------

struct UngroupedAggLocal : LocalSinkState {
    std::vector<AggCell> cells;
    std::vector<std::string> strs;   // string MIN/MAX extremes, parallel to cells
    std::vector<opteryx::ungrouped::MedianState> medians;  // MEDIAN buffers, parallel to cells
    std::vector<opteryx::roaring32::Roaring32> cidrs;  // CIDR_AGG address sets, parallel to cells
    std::vector<HllppSketch> hlls;  // APPROX_COUNT_DISTINCT sketches, parallel to cells
    std::vector<TDigestPtr> tds;    // APPROX_PERCENTILE sketches, parallel to cells
    // COUNT(DISTINCT): per-spec, hash-partitioned CarcharSet dedup on draken value
    // hashes (partitioned for the parallel union at finalize).
    std::vector<std::array<UCDPartition, kGBParts>> dparts;
    std::vector<AggColMeta> meta;
    bool init = false;
};
struct UngroupedAggGlobal : GlobalSinkState {
    std::mutex mtx;
    std::vector<AggCell> cells;
    std::vector<std::string> strs;
    std::vector<opteryx::ungrouped::MedianState> medians;
    std::vector<opteryx::roaring32::Roaring32> cidrs;
    std::vector<HllppSketch> hlls;
    std::vector<TDigestPtr> tds;
    // per spec, per partition: queued worker tables (disjoint by hash — merged
    // AND counted in parallel at finalize, mirroring GroupBySink).
    std::vector<std::array<std::vector<UCDPartition>, kGBParts>> dpending;
    std::vector<AggColMeta> meta;
    bool init = false;
};

struct UngroupedAggSink : Sink {
    std::vector<AggSpec2> specs;
    MorselBuffer* out;

    UngroupedAggSink(std::vector<AggSpec2> s, MorselBuffer* b)
        : specs(std::move(s)), out(b) {}

    std::unique_ptr<GlobalSinkState> make_global() override {
        return std::make_unique<UngroupedAggGlobal>();
    }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override {
        return std::make_unique<UngroupedAggLocal>();
    }

    bool capture_meta(std::vector<AggColMeta>& meta, const MorselPtr& in, ErrCtx& err) {
        meta.resize(specs.size());
        for (size_t s = 0; s < specs.size(); ++s) {
            // CountStar (kAggNoOperand) and whole-row CountDistinct
            // (kAggWholeRow) both have no SINGLE operand column to capture a
            // type for — whole-row dedup keys every column at sink() time.
            if (specs[s].col_idx < 0) continue;
            if (static_cast<size_t>(specs[s].col_idx) >= in->columns.size()) {
                err.code = 1;
                err.msg = "aggregate operand column missing from "
                          "input morsel — fail loud, never a silent wrong answer";
                return false;
            }
            const CxxColumn& c = in->columns[static_cast<size_t>(specs[s].col_idx)];
            DrakenType t = c.view.type;
            bool str_minmax = sort_type_is_string(t)
                && (specs[s].fn == AggFn::Min || specs[s].fn == AggFn::Max
                    || specs[s].fn == AggFn::AnyValue);
            // ARRAY_AGG is grouped-only (the binder rejects it without a GROUP BY,
            // and the compiler again at plan time). Reaching the ungrouped sink means
            // one of those gates broke — say so rather than read a lane that the
            // ungrouped AggCell has no room for.
            if (specs[s].fn == AggFn::ArrayAgg) {
                err.code = 1;
                err.msg = "ARRAY_AGG without a GROUP BY reached the "
                          "ungrouped aggregate sink — fail loud, never a silent wrong "
                          "answer";
                return false;
            }
            // CIDR_AGG, unlike ARRAY_AGG above, IS supported ungrouped: its state
            // lives in a side-vector parallel to `cells` (the shape MEDIAN, HLL and
            // t-digest already use), so the fixed-width AggCell never has to hold
            // it. Collapsing a whole address column to one CIDR list is the
            // primary use, not an edge case.
            if (specs[s].fn == AggFn::CidrAgg
                    && !cidr_operand_supported(t, c.own ? c.own->logical_type : nullptr)) {
                err.code = 1;
                err.msg = "CIDR_AGG requires an IPV4 column. A plain integer column "
                          "is refused because folding arbitrary integers into network "
                          "ranges produces a well-formed, confident, wrong answer. "
                          "Use `<column>::IPV4` to cast.";
                return false;
            }
            // STDDEV/STDDEV_SAMP/VAR_POP/VAR_SAMP never descale DECIMAL's fixed-point
            // unscaled integer — reading it as a raw double would compute the
            // variance of the WRONG numbers, a silent wrong answer, not an
            // approximation. CAST to DOUBLE first.
            if (agg_fn_is_stddev_family(specs[s].fn)
                    && (t == DRAKEN_DECIMAL || t == DRAKEN_DECIMAL128)) {
                err.code = 1;
                err.msg = "STDDEV/STDDEV_SAMP/VAR_POP/VAR_SAMP do not support DECIMAL "
                          "operands — CAST to DOUBLE first, never a silently "
                          "mis-scaled variance";
                return false;
            }
            // MEDIAN is numeric-only (see median_operand_supported) — DECIMAL
            // included, unlike STDDEV's DECIMAL-only rejection above.
            if ((specs[s].fn == AggFn::Median || specs[s].fn == AggFn::ApproxPercentile)
                    && !median_operand_supported(t)) {
                err.code = 1;
                err.msg = "MEDIAN/APPROX_PERCENTILE over this column "
                          "type is not supported — only numeric inputs are accepted "
                          "(CAST DECIMAL to DOUBLE first)";
                return false;
            }
            // CORR: both operands numeric — captures the second operand's
            // type/floatness into meta[s] (type2/is_float2).
            if (specs[s].fn == AggFn::Corr
                    && !corr_capture_meta(specs[s], *in, t, meta[s], err)) {
                return false;
            }
            // COUNT only reads validity — any column type is countable.
            // COUNT(DISTINCT) reads serialized value bytes (key_append fails
            // loud on unsupported types at run time).
            if (specs[s].fn != AggFn::Count && specs[s].fn != AggFn::CountDistinct
                    && specs[s].fn != AggFn::ApproxCountDistinct
                    && !str_minmax
                    && !agg2_operand_supported(t)) {
                err.code = 1;
                err.msg = "unsupported aggregate operand type — fail "
                          "loud, never a silent wrong answer";
                return false;
            }
            meta[s].type = t;
            meta[s].logical = c.own ? c.own->logical_type : nullptr;
            meta[s].is_float = (t == DRAKEN_FLOAT32 || t == DRAKEN_FLOAT64);
            meta[s].is_string = str_minmax;
            meta[s].captured = true;
        }
        return true;
    }

    SinkResult sink(const MorselPtr& in, GlobalSinkState&, LocalSinkState& ls,
                    ErrCtx& err) override {
        auto& l = static_cast<UngroupedAggLocal&>(ls);
        // An empty filter result can arrive as a zero-row (possibly zero-column)
        // morsel — nothing to count, nothing safe to capture meta from.
        if (in->num_rows() == 0) return SinkResult::CONTINUE;
        if (!l.init) {
            if (!capture_meta(l.meta, in, err)) return SinkResult::CONTINUE;
            l.cells.assign(specs.size(), AggCell{});
            l.strs.assign(specs.size(), std::string());
            l.medians.resize(specs.size());   // MedianState: not copyable, resize (not assign)
            l.cidrs.resize(specs.size());     // Roaring32: likewise move-only
            l.hlls.resize(specs.size());
            l.tds.resize(specs.size());
            l.dparts.resize(specs.size());
            l.init = true;
        }
        uint32_t rows = in->num_rows();
        for (size_t s = 0; s < specs.size(); ++s) {
            AggCell& c = l.cells[s];
            c.rows += rows;
            if (specs[s].fn == AggFn::CountDistinct && specs[s].col_idx == kAggWholeRow) {
                // COUNT(DISTINCT *): draken hashes EVERY column into one value hash
                // per row; NULL participates as a normal value (whole-row identity),
                // so no per-row null skip. Distinct value hashes are hash-partitioned
                // for the parallel union at finalize.
                std::array<UCDPartition, kGBParts>& DP = l.dparts[s];
                size_t ncols = in->columns.size();
                std::vector<size_t> allcols(ncols);
                for (size_t col = 0; col < ncols; ++col) allcols[col] = col;
                std::vector<uint64_t> vh;
                if (!compute_row_hashes(in, allcols, vh, err)) return SinkResult::CONTINUE;
                for (uint32_t i = 0; i < rows; ++i)
                    DP[vh[i] >> kGBPartShift].insert(vh[i]);
                continue;
            }
            if (specs[s].col_idx == kAggNoOperand) continue;
            const DrakenVector& v = in->columns[static_cast<size_t>(specs[s].col_idx)].view;
            if (specs[s].fn == AggFn::Count) {
                for (uint32_t i = 0; i < v.length; ++i) {
                    if (sort_row_valid(v, i)) c.valid += 1;
                }
            } else if (specs[s].fn == AggFn::CountDistinct) {
                // Draken hashes the operand value per row; dedup distinct value
                // hashes, hash-partitioned for the parallel union at finalize.
                // COUNT(DISTINCT col) ignores NULLs (c.valid unused; count is the
                // union of the partitioned sets).
                std::array<UCDPartition, kGBParts>& DP = l.dparts[s];
                std::vector<size_t> vcol{static_cast<size_t>(specs[s].col_idx)};
                std::vector<uint64_t> vh;
                if (!compute_row_hashes(in, vcol, vh, err)) return SinkResult::CONTINUE;
                for (uint32_t i = 0; i < v.length; ++i) {
                    if (!sort_row_valid(v, i)) continue;
                    DP[vh[i] >> kGBPartShift].insert(vh[i]);
                }
            } else if (l.meta[s].is_string) {
                bool want_max = specs[s].fn == AggFn::Max;
                if (draken_is_compressed(&v)) {
                    // §11: reduce string MIN/MAX over the referenced uniques, not
                    // per row — extreme over rows == extreme over referenced values.
                    std::vector<uint8_t> ref;
                    mark_referenced_valid(v, ref);
                    for (uint32_t j = 0; j < v.data_length; ++j) {
                        if (ref[j] == 0) continue;
                        agg2_update_str_phys(c, l.strs[s], v, j, want_max);
                    }
                } else {
                    for (uint32_t i = 0; i < v.length; ++i) {
                        if (sort_row_valid(v, i)) agg2_update_str(c, l.strs[s], v, i, want_max);
                    }
                }
            } else if (agg_fn_is_stddev_family(specs[s].fn)) {
                bool is_f = l.meta[s].is_float;
                for (uint32_t i = 0; i < v.length; ++i) {
                    if (sort_row_valid(v, i)) agg2_update_stddev(c, v, i, is_f);
                }
            } else if (specs[s].fn == AggFn::CidrAgg) {
                // Operand is validated UINT32+IPV4, so the raw read is never a
                // float and the value IS the address. NULLs are skipped: a NULL
                // is not an address and so is not a member of the set. A refused
                // insert latches `overflowed` for emit to raise on — the same
                // contract the grouped path uses.
                opteryx::roaring32::Roaring32& R = l.cidrs[s];
                for (uint32_t i = 0; i < v.length; ++i) {
                    if (!sort_row_valid(v, i)) continue;
                    (void)R.add(static_cast<uint32_t>(agg2_read_raw(v, i, false)));
                }
            } else if (specs[s].fn == AggFn::Median) {
                bool is_f = l.meta[s].is_float;
                opteryx::ungrouped::MedianState& st = l.medians[s];
                for (uint32_t i = 0; i < v.length; ++i) {
                    if (!sort_row_valid(v, i)) continue;
                    int64_t raw = agg2_read_raw(v, i, is_f);
                    double d;
                    if (is_f) std::memcpy(&d, &raw, sizeof(d));
                    else d = static_cast<double>(raw);
                    if (!st.append(d)) {
                        err.code = 1;
                        err.msg = kMedianCapExceededMsg;
                        return SinkResult::CONTINUE;
                    }
                }
            } else if (specs[s].fn == AggFn::Corr) {
                bool is_fx = l.meta[s].is_float;
                bool is_fy = l.meta[s].is_float2;
                const DrakenVector& v2 =
                    in->columns[static_cast<size_t>(specs[s].col_idx2)].view;
                for (uint32_t i = 0; i < v.length; ++i) {
                    // Pairwise: skip the row unless BOTH operands are non-NULL.
                    if (!sort_row_valid(v, i) || !sort_row_valid(v2, i)) continue;
                    int64_t rx = agg2_read_raw(v, i, is_fx);
                    int64_t ry = agg2_read_raw(v2, i, is_fy);
                    double x, y;
                    if (is_fx) std::memcpy(&x, &rx, sizeof(x));
                    else x = static_cast<double>(rx);
                    if (is_fy) std::memcpy(&y, &ry, sizeof(y));
                    else y = static_cast<double>(ry);
                    agg2_update_corr(c, x, y);
                }
            } else if (specs[s].fn == AggFn::ApproxCountDistinct) {
                std::vector<size_t> vcol{static_cast<size_t>(specs[s].col_idx)};
                std::vector<uint64_t> vh;
                if (!compute_row_hashes(in, vcol, vh, err)) return SinkResult::CONTINUE;
                HllppSketch& sk = l.hlls[s];
                for (uint32_t i = 0; i < v.length; ++i) {
                    if (sort_row_valid(v, i)) sk.add_hash(hll_avalanche(vh[i]));
                }
            } else if (specs[s].fn == AggFn::ApproxPercentile) {
                bool is_f = l.meta[s].is_float;
                td_histogram_t* h = l.tds[s].h;
                for (uint32_t i = 0; i < v.length; ++i) {
                    if (!sort_row_valid(v, i)) continue;
                    int64_t raw = agg2_read_raw(v, i, is_f);
                    double d;
                    if (is_f) std::memcpy(&d, &raw, sizeof(d));
                    else d = static_cast<double>(raw);
                    td_add(h, d, 1);
                }
            } else {
                bool is_f = l.meta[s].is_float;
                bool nm = specs[s].fn == AggFn::Min || specs[s].fn == AggFn::Max
                    || specs[s].fn == AggFn::AnyValue;
                for (uint32_t i = 0; i < v.length; ++i) {
                    if (sort_row_valid(v, i)) agg2_update(c, v, i, is_f, nm);
                }
            }
        }
        return SinkResult::CONTINUE;
    }
    void combine(GlobalSinkState& gs, LocalSinkState& ls, ErrCtx& err) override {
        auto& g = static_cast<UngroupedAggGlobal&>(gs);
        auto& l = static_cast<UngroupedAggLocal&>(ls);
        std::lock_guard<std::mutex> lk(g.mtx);
        if (!g.init) {
            g.cells.assign(specs.size(), AggCell{});
            g.strs.assign(specs.size(), std::string());
            g.medians.resize(specs.size());
            g.cidrs.resize(specs.size());
            g.hlls.resize(specs.size());
            g.tds.resize(specs.size());
            g.dpending.resize(specs.size());
            g.meta.resize(specs.size());
            g.init = true;
        }
        if (l.init) {
            for (size_t s = 0; s < specs.size(); ++s) {
                if (l.meta[s].is_string)   // BEFORE agg2_merge (reads pre-merge valid)
                    agg2_merge_str(g.cells[s], l.cells[s], g.strs[s], l.strs[s],
                                   specs[s].fn == AggFn::Max);
                agg2_merge(g.cells[s], l.cells[s]);
                if (specs[s].fn == AggFn::Median) {
                    opteryx::ungrouped::MedianState& src_st = l.medians[s];
                    for (size_t k = 0; k < src_st.size; ++k) {
                        if (!g.medians[s].append(src_st.buf[k])) {
                            err.code = 1;
                            err.msg = kMedianCapExceededMsg;
                            return;
                        }
                    }
                }
                if (specs[s].fn == AggFn::CidrAgg) {
                    // Union: order- and duplication-insensitive, so the worker
                    // split cannot change the answer.
                    (void)g.cidrs[s].merge_from(l.cidrs[s]);
                }
                if (specs[s].fn == AggFn::ApproxCountDistinct) {
                    if (!g.hlls[s].merge(l.hlls[s])) {
                        err.code = 1;
                        err.msg = "APPROX_COUNT_DISTINCT sketch merge "
                                  "failed (precision mismatch) — unreachable, every "
                                  "sketch shares one fixed precision";
                        return;
                    }
                }
                if (specs[s].fn == AggFn::ApproxPercentile) {
                    td_merge(g.tds[s].h, l.tds[s].h);
                }
                if (specs[s].fn == AggFn::CountDistinct) {
                    for (size_t part = 0; part < kGBParts; ++part) {
                        if (l.dparts[s][part].size() > 0)
                            g.dpending[s][part].push_back(
                                std::move(l.dparts[s][part]));
                    }
                }
                if (l.meta[s].captured && !g.meta[s].captured) g.meta[s] = l.meta[s];
            }
        }
    }
    void finalize(GlobalSinkState& gs, ErrCtx& err) override {
        auto& g = static_cast<UngroupedAggGlobal&>(gs);
        if (!g.init) {
            // zero morsels ever arrived: COUNT()=0, SUM/AVG/MIN/MAX=NULL
            g.cells.assign(specs.size(), AggCell{});
            g.strs.assign(specs.size(), std::string());
            g.medians.resize(specs.size());
            g.cidrs.resize(specs.size());
            g.hlls.resize(specs.size());
            g.tds.resize(specs.size());
            g.dpending.resize(specs.size());
            g.meta.resize(specs.size());
        }
        // COUNT(DISTINCT): union each spec's queued worker sets. Partitions are
        // disjoint by hash, so (spec, partition) sets union AND count in parallel
        // (one-shot pool-let, same pattern as GroupBySink::finalize).
        std::vector<std::atomic<int64_t>> dcounts(specs.size());
        for (auto& dc : dcounts) dc.store(0);
        {
            std::vector<std::pair<size_t, size_t>> items;   // (spec, partition)
            size_t queued = 0;
            for (size_t s = 0; s < specs.size(); ++s) {
                if (specs[s].fn != AggFn::CountDistinct) continue;
                for (size_t part = 0; part < kGBParts; ++part) {
                    if (!g.dpending[s][part].empty()) {
                        items.emplace_back(s, part);
                        for (const UCDPartition& d : g.dpending[s][part])
                            queued += d.size();
                    }
                }
            }
            if (!items.empty()) {
                unsigned hw = std::thread::hardware_concurrency();
                unsigned nt = hw > 2 ? hw - 2 : 1;
                if (nt > 16) nt = 16;
                if (nt > items.size()) nt = static_cast<unsigned>(items.size());
                if (queued < 65536) nt = 1;   // small distinct: inline
                std::atomic<size_t> next{0};
                auto worker = [&](unsigned) {
                    for (;;) {
                        size_t it = next.fetch_add(1);
                        if (it >= items.size()) break;
                        size_t sp = items[it].first;
                        size_t part = items[it].second;
                        auto& list = g.dpending[sp][part];
                        UCDPartition merged = std::move(list[0]);
                        int64_t cnt = static_cast<int64_t>(merged.size());
                        for (size_t i = 1; i < list.size(); ++i) {
                            UCDPartition& src = list[i];
                            for (uint64_t h : src.distinct)
                                if (merged.insert(h)) cnt += 1;
                            src = UCDPartition();
                        }
                        dcounts[sp].fetch_add(cnt);
                    }
                };
                std::vector<std::thread> threads;
                threads.reserve(nt - 1);
                for (unsigned t = 1; t < nt; ++t) threads.emplace_back(worker, t);
                worker(0);
                for (auto& th : threads) th.join();
            }
        }
        auto m = std::make_shared<CxxMorsel>();
        m->zero_col_rows = 1;
        for (size_t s = 0; s < specs.size(); ++s) {
            // One-group lane views over the AggCell scalars — the SAME emitters
            // the grouped sink uses, never a second copy of the emit semantics.
            const AggCell& c = g.cells[s];
            GBKind kind = gb_kind_of(specs[s], g.meta[s]);
            int64_t rows1 = c.rows, valid1 = c.valid, i641 = 0;
            if (kind == GBKind::CountDistinct)
                valid1 = dcounts[s].load();
            double f641 = c.fsum;
            double f64sq1 = c.fsumsq;
            double f64y1 = c.fsumy, f64yy1 = c.fsumyy, f64xy1 = c.fsumxy;
            __int128 i1281 = 0;
            switch (kind) {
                case GBKind::SumI:
                    if (c.isum > static_cast<__int128>(INT64_MAX)
                            || c.isum < static_cast<__int128>(INT64_MIN)) {
                        err.code = 1;
                        err.msg = "SUM overflow: exact integer sum exceeds INT64 — "
                                  "fail loud, never a wrapped answer";
                        return;
                    }
                    i641 = static_cast<int64_t>(c.isum);
                    break;
                case GBKind::MinMaxNum:
                    // Min and AnyValue both read the min lane — see AggFn::AnyValue.
                    i641 = (specs[s].fn == AggFn::Max) ? c.max_raw : c.min_raw;
                    break;
                case GBKind::AvgI:   // averages divide from the exact int128 sum
                case GBKind::SumD128:
                case GBKind::AvgD128:
                    i1281 = c.isum;
                    break;
                case GBKind::MinMaxD128:
                    i1281 = (specs[s].fn == AggFn::Max) ? c.max128 : c.min128;
                    break;
                default:
                    break;
            }
            GBLaneView lv;
            lv.rows = &rows1; lv.valid = &valid1; lv.i64 = &i641;
            lv.f64 = &f641; lv.f64sq = &f64sq1; lv.i128 = &i1281; lv.sval = &g.strs[s];
            lv.f64y = &f64y1; lv.f64yy = &f64yy1; lv.f64xy = &f64xy1;
            lv.median = &g.medians[s];
            lv.cidr = &g.cidrs[s];   // one "group": the whole column
            lv.hll = &g.hlls[s];
            lv.td = &g.tds[s];
            lv.pct_spec = &specs[s];
            m->columns.push_back(emit_lane_column(g.meta[s], kind, lv, 1, err));
            if (err.code != 0) return;
            m->names.push_back(specs[s].name);
        }
        out->morsels.push_back(std::move(m));
    }
};

// ---- GroupBySink v3: hash-partitioned, columnar keys + columnar collectors ----------
// v2 (partitioned flat tables + arena keys) fixed the serial-merge wall but kept
// row-shaped storage: 104-byte AggCells × stride per group (28 GB at clickbench
// Q33's ~90M groups — paging territory), per-row key serialization with length
// framing, and emit-time key re-parsing. v3 adopts the proven pre-rewrite
// operator's shape inside the partitioned skeleton:
//   • collectors are per-spec COLUMNAR LANES (8B each), allocated only for what
//     the spec's GBKind needs — update, merge, and emit are per-COLUMN passes
//     that dispatch on the kind once, never per row.
//   • all-fixed-width keys pack into a constant-stride buffer (null byte + raw
//     bytes, zeros when NULL): serialization is one columnar pass per key
//     column, identity is one memcmp against ONE contiguous array, and emit
//     reads the packed bytes in place. Any string key falls back to the v2
//     arena format. Identity stays hash + full byte compare — never hash-only.
//   • sink: rows hash-partition (top 6 bits of one XXH3 over the key bytes)
//     into kGBParts flat open-addressing tables per worker; the 64-bit hash is
//     STORED so no later phase re-hashes.
//   • combine: O(kGBParts) std::move's under the mutex — queued, never merged
//     inline.
//   • finalize: partitions are disjoint, so they merge AND emit in parallel;
//     output order across partitions is unspecified — exactly SQL's contract
//     for GROUP BY without ORDER BY.


// Per-spec columnar collector lanes. Growth is vector resize (zero-filling) once
// per morsel per partition — never per row, never per group.
struct GBLanes {
    std::vector<int64_t>  valid;   // non-NULL operand rows (every kind but Rows)
    std::vector<int64_t>  i64;     // SumI/AvgI exact sums; MinMaxNum raw containers
    std::vector<double>   f64;     // SumF/AvgF sums; Stddev family/Corr Σx
    std::vector<double>   f64sq;   // Stddev family/Corr Σx²
    std::vector<double>   f64y;    // Corr Σy
    std::vector<double>   f64yy;   // Corr Σy²
    std::vector<double>   f64xy;   // Corr Σxy
    std::vector<uint64_t> mkey;    // MinMaxNum normalized order keys (sort_num_key)
    std::vector<__int128> i128;    // SumD128/AvgD128 sums; MinMaxD128 extremes
    std::vector<std::string> sval; // MinMaxStr extremes
    std::vector<GBArrayAggState> aa;  // ArrayAgg per-group element lists
    std::vector<opteryx::roaring32::Roaring32> cidr;  // CidrAgg per-group address sets
    std::vector<opteryx::ungrouped::MedianState> median;  // Median per-group buffers
    std::vector<HllppSketch> hll;  // ApproxCountDistinct per-group sketches
    std::vector<TDigestPtr> td;    // ApproxPercentile per-group sketches
};

inline void gb_lanes_resize(GBLanes& L, GBKind k, size_t n) {
    switch (k) {
        case GBKind::Rows:
            return;   // shared per-partition rows lane, not per-spec
        case GBKind::Valid:
        case GBKind::CountDistinct:   // the dedup table lives on the partition
            L.valid.resize(n);
            return;
        case GBKind::SumI:
            L.valid.resize(n); L.i64.resize(n);
            return;
        case GBKind::SumF: case GBKind::AvgF:
            L.valid.resize(n); L.f64.resize(n);
            return;
        case GBKind::Stddev:
        case GBKind::StddevSamp:
        case GBKind::VarPop:
        case GBKind::VarSamp:
            L.valid.resize(n); L.f64.resize(n); L.f64sq.resize(n);
            return;
        case GBKind::Corr:
            L.valid.resize(n); L.f64.resize(n); L.f64sq.resize(n);
            L.f64y.resize(n); L.f64yy.resize(n); L.f64xy.resize(n);
            return;
        // AvgI sums exact int128 (overflow-proof); SUM's OUTPUT is INT64 so its
        // int64 lane + loud overflow trap loses nothing.
        case GBKind::AvgI:
        case GBKind::SumD128: case GBKind::AvgD128: case GBKind::MinMaxD128:
            L.valid.resize(n); L.i128.resize(n);
            return;
        case GBKind::MinMaxNum:
            L.valid.resize(n); L.i64.resize(n); L.mkey.resize(n);
            return;
        case GBKind::MinMaxStr:
            L.valid.resize(n); L.sval.resize(n);
            return;
        case GBKind::ArrayAgg:
            // No `valid` lane: an ARRAY_AGG row is never NULL, and the element
            // count is the state's own nulls.size().
            L.aa.resize(n);
            return;
        case GBKind::CidrAgg:
            // No `valid` lane: a CIDR_AGG row is never NULL — a group holding no
            // non-NULL addresses is the EMPTY set, which emits an empty array.
            // resize() default-constructs empty Roaring32s, the right initial
            // state for a new group (they are move-only, like MedianState, so a
            // copy cannot double-release their budget charge).
            L.cidr.resize(n);
            return;
        case GBKind::Median:
            // No `valid` lane: null-ness is the state's own size==0 (see
            // emit_lane_column). resize() default-constructs new MedianStates
            // (empty, size==0) — the correct initial state for a new group.
            L.median.resize(n);
            return;
        case GBKind::ApproxCountDistinct:
            // No `valid` lane: never NULL (see emit_lane_column). resize()
            // default-constructs new HllppSketches (precision 14, matching the
            // legacy collector's default — see AggFn::ApproxCountDistinct).
            L.hll.resize(n);
            return;
        case GBKind::ApproxPercentile:
            // No `valid` lane: null-ness is the digest's own td_size()==0 (see
            // emit_lane_column). resize() default-constructs fresh TDigestPtrs.
            L.td.resize(n);
            return;
    }
}

// Materialize rows [start, start+n) of a per-group key store (GroupKeyColumn) as
// an output column — the group-key emit path, replacing emit_key_columns. Mirrors
// the join's build_output per-column materialization over a contiguous range; a
// group whose key value is NULL is emitted NULL via the validity bitmap.
inline CxxColumn jpc_emit_range(const GroupKeyColumn& col, size_t start,
                                uint32_t n, ErrCtx& err) {
    (void)err;
    size_t vbytes = (static_cast<size_t>(n) + 7) / 8;
    uint8_t* vbits = nullptr;
    auto mark_null = [&](uint32_t i) {
        if (vbits == nullptr) {
            vbits = static_cast<uint8_t*>(draken_malloc(vbytes == 0 ? 1 : vbytes));
            std::memset(vbits, 0xFF, vbytes == 0 ? 1 : vbytes);
        }
        vbits[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7)));
    };
    auto row_is_null = [&](size_t g) {
        return !col.validity.empty() && !((col.validity[g >> 3] >> (g & 7)) & 1u);
    };
    if (gb_key_is_string(col.type)) {
        const auto* src_slots = reinterpret_cast<const DrakenStringSlot*>(col.raw.data());
        const uint8_t* src_arena = col.arena.empty() ? nullptr : col.arena.data();
        size_t total_arena = 0;
        for (uint32_t i = 0; i < n; ++i) {
            size_t g = start + i;
            if (row_is_null(g)) continue;
            const auto* slot = src_slots + g;
            if (!str_is_inline(slot)) total_arena += str_length(slot);
        }
        size_t slots_off = sizeof(DrakenStringArena);
        size_t arena_off = slots_off + static_cast<size_t>(n == 0 ? 1 : n) * sizeof(DrakenStringSlot);
        uint8_t* blk = static_cast<uint8_t*>(draken_malloc(arena_off + total_arena));
        auto* sa = reinterpret_cast<DrakenStringArena*>(blk);
        auto* dst = reinterpret_cast<DrakenStringSlot*>(blk + slots_off);
        uint8_t* out_arena = total_arena > 0 ? blk + arena_off : nullptr;
        sa->slots = dst; sa->arena = out_arena; sa->length = n;
        sa->arena_used = total_arena; sa->arena_cap = total_arena;
        sa->null_bitmap = nullptr; sa->owns_buffers = 0; sa->type = col.type;
        size_t arena_pos = 0;
        for (uint32_t i = 0; i < n; ++i) {
            size_t g = start + i;
            if (row_is_null(g)) {
                std::memset(&dst[i], 0, sizeof(DrakenStringSlot));
                mark_null(i);
                continue;
            }
            const auto* slot = src_slots + g;
            if (str_is_inline(slot)) dst[i] = *slot;
            else {
                uint32_t slen = str_length(slot);
                std::memcpy(out_arena + arena_pos, str_data(slot, src_arena), slen);
                str_clone_with_offset(&dst[i], slot, static_cast<uint32_t>(arena_pos));
                arena_pos += slen;
            }
        }
        uint32_t* sel = static_cast<uint32_t*>(draken_malloc((n == 0 ? 1 : n) * sizeof(uint32_t)));
        for (uint32_t i = 0; i < n; ++i) sel[i] = i;
        DrakenVector v;
        v.data = sa; v.selection = sel; v.data_length = n; v.length = n;
        v.validity = vbits; v.type = col.type;
        v.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
        CxxColumn c;
        c.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(blk),
                                              OwnedBuffer<uint8_t>(vbits),
                                              OwnedBuffer<void>(sel));
        c.own->logical_type = col.logical;
        c.view = c.own->vec;
        return c;
    }
    void* data;
    if (gb_key_is_bool(col.type)) {
        uint8_t* bits = gb_alloc_bool_bits(n);
        for (uint32_t i = 0; i < n; ++i) {
            size_t g = start + i;
            if (row_is_null(g)) { mark_null(i); continue; }
            if (col.raw[g]) bits[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
        data = bits;
    } else {
        data = draken_malloc(static_cast<size_t>(n == 0 ? 1 : n) * col.elem_size);
        uint8_t* d = static_cast<uint8_t*>(data);
        for (uint32_t i = 0; i < n; ++i) {
            size_t g = start + i;
            if (row_is_null(g)) {
                std::memset(d + static_cast<size_t>(i) * col.elem_size, 0, col.elem_size);
                mark_null(i);
                continue;
            }
            std::memcpy(d + static_cast<size_t>(i) * col.elem_size,
                        col.raw.data() + g * col.elem_size, col.elem_size);
        }
    }
    uint32_t* sel = static_cast<uint32_t*>(draken_malloc((n == 0 ? 1 : n) * sizeof(uint32_t)));
    for (uint32_t i = 0; i < n; ++i) sel[i] = i;
    DrakenVector v;
    v.data = data; v.selection = sel; v.data_length = n; v.length = n;
    v.validity = vbits; v.type = col.type;
    v.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
    CxxColumn c;
    c.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(data),
                                          OwnedBuffer<uint8_t>(vbits),
                                          OwnedBuffer<void>(sel));
    c.own->logical_type = col.logical;
    c.view = c.own->vec;
    return c;
}

// Per-partition group table. The ad-hoc open-addressing GBPartition (table + stored
// key bytes + inline lanes) is replaced by: a CarcharIndex (draken hash → dense group
// id, hash identity), per-group key VALUES for emit (GroupKeyColumn per key col),
// and the aggregate lanes indexed by group id. hashes[] keeps each group's key hash
// so the parallel merge can re-insert it into the merged partition.
struct GBPartition {
    opteryx::carchar::CarcharIndex index;
    opteryx::parvi::ParviMap small;   // low-card front map (kGBParviGateNDV gate)
    // MEDIUS (2026-08-14, unratified): the ladder was 64 -> unbounded, so every
    // column above 64 distinct paid full CarcharIndex probe cost. duckdb and
    // clickhouse both special-case this band and size it 8k-20k (see
    // medius.hpp for the sourced numbers). 512 slots PER PARTITION x 64
    // partitions covers a ~26k-distinct column while each map stays 8 KB and
    // L1-resident. Armed unconditionally — no NDV estimate is trusted; a column
    // that outgrows it simply promotes, exactly as parvi does.
    opteryx::medius::MediusMap<128> mid;   // FOOTPRINT TEST: 2KB/partition instead of 8KB
    bool use_mid = true;
    bool use_parvi = false;           // armed by GroupBySink when the NDV estimate is low
    std::vector<uint64_t> hashes;
    std::vector<GroupKeyColumn> keycols;
    std::vector<int64_t> grows;       // COUNT(*) rows lane (any Rows spec)
    std::vector<GBLanes> lanes;       // one per spec
    std::vector<GBCountDistinct> cd;  // one per spec (only CountDistinct fills it)

    size_t size() const { return hashes.size(); }

    // One-shot: move the parvi entries into the CarcharIndex. Group ids are
    // assigned from hashes.size() (dense, monotone), so the carchar continues
    // the same id space — no remap of keycols/lanes/grows.
    void promote_small() {
        small.drain_into(index);
        small.clear();
        use_parvi = false;
    }

    // Group-id find-or-insert for the keying loop. next_id must be
    // static_cast<int64_t>(hashes.size()). Returns true iff the group is new.
    // One-shot: move the mid-tier entries into the CarcharIndex. Same contract as
    // promote_small — group ids are already dense and monotone, so nothing is
    // remapped; the carchar simply continues the same id space.
    void promote_mid() {
        mid.drain_into(index);
        mid.clear();
        use_mid = false;
    }

    inline bool find_or_insert_group(uint64_t h, int64_t next_id, int64_t& gid) {
        if (use_parvi) {
            const auto r = small.find_or_insert_id(h, next_id, gid);
            if (r != opteryx::parvi::ParviInsert::kFull)
                return r == opteryx::parvi::ParviInsert::kInserted;
            groupby_tel::parvi_promotes.fetch_add(1, std::memory_order_relaxed);
            promote_small();  // estimate was wrong for this partition — fall through
            // parvi drained into `index`, so the mid tier must be retired too:
            // leaving it armed would let it mint a SECOND group id for a key that
            // is already in `index` (measured: AdvEngineID 19 -> 36 groups).
            use_mid = false;
        }
        if (use_mid) {
            const auto r = mid.find_or_insert_id(h, next_id, gid);
            if (r != opteryx::medius::MediusInsert::kFull)
                return r == opteryx::medius::MediusInsert::kInserted;
            groupby_tel::mid_promotes.fetch_add(1, std::memory_order_relaxed);
            promote_mid();  // outgrew the bounded tier — fall through to carchar
        }
        return index.find_or_insert_id(h, next_id, gid);
    }
};


struct GroupByLocal : LocalSinkState {
    std::array<GBPartition, kGBParts> parts;
    std::vector<AggColMeta> meta;
    std::vector<GBKind> kinds;
    std::vector<KeyColMeta> key_meta;
    bool has_rows = false;            // any COUNT(*) spec
    bool init = false;
    size_t entries_total = 0;         // Σ partition sizes (adaptive flush trigger)
    // EARLY-PROMOTE (2026-08-14): once any partition has outgrown Medius we know
    // this column is high-cardinality, so fresh partitions after a flush must NOT
    // re-arm it. Without this the bounded tier is refilled and re-promoted on EVERY
    // flush cycle — MEASURED 10,880 promotions on GROUP BY UserID (64 partitions,
    // ~269 flushes) where the ceiling should be 64. That repeated doomed fill is
    // what made Medius a net +0.4% on the suite. duckdb learns the same way
    // (DecideAdaptation -> SkipLookups, decided once from early rows).
    bool mid_disabled = false;
    // per-morsel ingest scratch
    std::vector<uint64_t> mk_hash;    // per row: draken key hash
    std::vector<uint32_t> mk_ent;     // per row: group id within its partition
    // H20 compressed-key path: one entry per DISTINCT hash, not per row.
    std::vector<uint32_t> dict_rep;   // per code: first row using it (representative)
    std::vector<uint8_t>  dict_part;  // per code: partition index
    std::vector<uint32_t> dict_gid;   // per code: group id within that partition
    std::vector<uint64_t> cd_vhash;   // per row: value hash for a CountDistinct spec
};
struct GroupByGlobal : GlobalSinkState {
    std::mutex mtx;
    std::array<std::vector<GBPartition>, kGBParts> pending;   // queued worker partitions
    std::vector<AggColMeta> meta;
    std::vector<GBKind> kinds;
    std::vector<KeyColMeta> key_meta;
    bool has_rows = false;
    bool init = false;
};

struct GroupBySink : Sink {
    std::vector<size_t> key_idx;   // EVERY grouping key — all of them are hashed
    // The EMIT subset of key_idx, in output order. A grouping key's purpose is spent
    // the moment it has been hashed: group identity here is the 64-bit key hash
    // (compute_row_hashes), never a value comparison, so a key that nothing above the
    // aggregate reads still has to be HASHED to separate the groups but its VALUES
    // never have to be stored. The compiler clears such a key's emit flag and the
    // per-group key store then never allocates a column for it — that copy, not the
    // output column, is the cost being killed. `keycols` is parallel to THESE, not to
    // key_idx.
    //
    // store_names carries the identity per emitted column: CxxMorsel::names requires
    // one entry per column (see cxx_morsel.h); this sink is the one output producer
    // that didn't carry them, which stayed invisible until cxx_unnest's drop_source
    // path indexed into it.
    std::vector<size_t> store_col_idx;    // input morsel column index per stored key
    std::vector<size_t> store_key_pos;    // position in key_idx of each stored key
    std::vector<std::string> store_names;
    std::vector<AggSpec2> specs;
    MorselBuffer* out;
    size_t chunk_rows;
    bool low_card;   // planner NDV estimate <= kGBParviGateNDV → parvi front maps

    // `kemit` has one entry per key (invariant enforced at the binding, which is the
    // only construction site and can raise) — false = hash the key, never store it.
    GroupBySink(std::vector<size_t> keys, std::vector<std::string> knames,
                std::vector<uint8_t> kemit,
                std::vector<AggSpec2> s, MorselBuffer* b, int64_t ndv_estimate,
                size_t chunk = 131072)
        : key_idx(std::move(keys)), specs(std::move(s)),
          out(b), chunk_rows(chunk),
          low_card(ndv_estimate >= 0 && ndv_estimate <= kGBParviGateNDV) {
        for (size_t k = 0; k < key_idx.size(); ++k) {
            if (!kemit[k]) continue;
            store_key_pos.push_back(k);
            store_col_idx.push_back(key_idx[k]);
            store_names.push_back(knames[k]);
        }
    }

    std::unique_ptr<GlobalSinkState> make_global() override {
        return std::make_unique<GroupByGlobal>();
    }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override {
        auto l = std::make_unique<GroupByLocal>();
        if (low_card) {
            for (auto& P : l->parts) P.use_parvi = true;
            groupby_tel::parvi_sinks.fetch_add(1, std::memory_order_relaxed);
        }
        return l;
    }

    // Type a partition's per-group key store from the captured key metadata. Called
    // in capture() for all 64 partitions and again after a partition is reset by an
    // adaptive flush (GBPartition() clears the keycol types). `km` is indexed by
    // key_idx position; the store holds only the emitted keys, hence store_key_pos.
    void type_keycols(GBPartition& P, const std::vector<KeyColMeta>& km) {
        P.keycols.resize(store_key_pos.size());
        for (size_t j = 0; j < store_key_pos.size(); ++j) {
            const KeyColMeta& m = km[store_key_pos[j]];
            P.keycols[j].type = m.type;
            P.keycols[j].elem_size = gb_key_elem_size(m.type, m.logical);
            P.keycols[j].logical = m.logical;
        }
    }

    bool capture(GroupByLocal& l, const MorselPtr& in, ErrCtx& err) {
        l.key_meta.resize(key_idx.size());
        for (size_t k = 0; k < key_idx.size(); ++k) {
            if (key_idx[k] >= in->columns.size()) {
                err.code = 1;
                err.msg = "GROUP BY key column missing from input "
                          "morsel — fail loud, never a silent wrong grouping";
                return false;
            }
            const CxxColumn& c = in->columns[key_idx[k]];
            DrakenType t = c.view.type;
            const LogicalType* lt = c.own ? c.own->logical_type : nullptr;
            // ELIGIBILITY, not storage: a GROUP BY key must be hashable/comparable,
            // which is a semantic property (sort_key_type_supported — the same set
            // the compiler's _KEY_COLUMN_TYPES mirrors at plan time). This used to
            // test `elem_size == 0` and so rejected key types by the accident of
            // their width being unknown; that conflation broke the moment a
            // carry-only type (VARIANT) gained a known stride, since being
            // MOVEABLE says nothing about being GROUPABLE.
            if (!sort_key_type_supported(t)) {
                err.code = 1;
                err.msg = "unsupported GROUP BY key column type";
                return false;
            }
            // Checked for EVERY key, including hash-only ones whose values are never
            // materialized. Deliberate: it mirrors the compiler's plan-time
            // _check_key_type over the whole key list, so which keys the projection
            // above happens to read cannot decide whether a query is accepted.
            if (gb_key_elem_size(t, lt) == 0) {
                err.code = 1;
                err.msg = "GROUP BY key column has no materializable "
                          "width — fail loud, never silent corruption";
                return false;
            }
            l.key_meta[k].type = t;
            l.key_meta[k].logical = lt;
            l.key_meta[k].captured = true;
        }
        // Type every partition's per-group key store (GroupKeyColumn per key col).
        for (size_t p = 0; p < kGBParts; ++p) type_keycols(l.parts[p], l.key_meta);
        l.meta.resize(specs.size());
        l.kinds.resize(specs.size());
        l.has_rows = false;
        for (size_t s = 0; s < specs.size(); ++s) {
            if (specs[s].col_idx == kAggNoOperand) {
                l.kinds[s] = GBKind::Rows;
                l.has_rows = true;
                continue;
            }
            if (specs[s].fn == AggFn::CountDistinct && specs[s].col_idx == kAggWholeRow) {
                // Whole-row dedup key is built from every input column at
                // sink() time — no single operand column/type to capture.
                l.kinds[s] = GBKind::CountDistinct;
                continue;
            }
            const CxxColumn& c = in->columns[static_cast<size_t>(specs[s].col_idx)];
            DrakenType t = c.view.type;
            bool str_minmax = sort_type_is_string(t)
                && (specs[s].fn == AggFn::Min || specs[s].fn == AggFn::Max
                    || specs[s].fn == AggFn::AnyValue);
            // ARRAY_AGG copies values instead of ordering/summing them, so it takes
            // the string family too — its own guard, not agg2's.
            if (specs[s].fn == AggFn::ArrayAgg) {
                if (!aa_operand_supported(t)) {
                    err.code = 1;
                    err.msg = "unsupported ARRAY_AGG operand type — "
                              "fail loud, never a silent wrong answer";
                    return false;
                }
            } else if (specs[s].fn == AggFn::CidrAgg) {
                if (!cidr_operand_supported(t, c.own ? c.own->logical_type : nullptr)) {
                    err.code = 1;
                    err.msg = "CIDR_AGG requires an IPV4 column. A plain integer column "
                              "is refused because folding arbitrary integers into network "
                              "ranges produces a well-formed, confident, wrong answer. "
                              "Use `<column>::IPV4` to cast.";
                    return false;
                }
            } else if (specs[s].fn != AggFn::Count
                    && specs[s].fn != AggFn::CountDistinct
                    && specs[s].fn != AggFn::ApproxCountDistinct
                    && !str_minmax
                    && !agg2_operand_supported(t)) {
                // COUNT reads only validity; COUNT(DISTINCT) reads serialized value
                // bytes (key_append fails loud on unsupported types at run time).
                err.code = 1;
                err.msg = "unsupported aggregate operand type — fail "
                          "loud, never a silent wrong answer";
                return false;
            }
            // STDDEV/STDDEV_SAMP/VAR_POP/VAR_SAMP never descale DECIMAL's fixed-point
            // unscaled integer — reading it as a raw double would compute the
            // variance of the WRONG numbers, a silent wrong answer, not an
            // approximation. CAST to DOUBLE first.
            if (agg_fn_is_stddev_family(specs[s].fn)
                    && (t == DRAKEN_DECIMAL || t == DRAKEN_DECIMAL128)) {
                err.code = 1;
                err.msg = "STDDEV/STDDEV_SAMP/VAR_POP/VAR_SAMP do not support DECIMAL "
                          "operands — CAST to DOUBLE first, never a silently "
                          "mis-scaled variance";
                return false;
            }
            // MEDIAN is numeric-only (see median_operand_supported) — DECIMAL
            // included, unlike STDDEV's DECIMAL-only rejection above.
            if ((specs[s].fn == AggFn::Median || specs[s].fn == AggFn::ApproxPercentile)
                    && !median_operand_supported(t)) {
                err.code = 1;
                err.msg = "MEDIAN/APPROX_PERCENTILE over this column "
                          "type is not supported — only numeric inputs are accepted "
                          "(CAST DECIMAL to DOUBLE first)";
                return false;
            }
            // CORR: both operands numeric — captures the second operand's
            // type/floatness into meta[s] (type2/is_float2).
            if (specs[s].fn == AggFn::Corr
                    && !corr_capture_meta(specs[s], *in, t, l.meta[s], err)) {
                return false;
            }
            l.meta[s].type = t;
            l.meta[s].logical = c.own ? c.own->logical_type : nullptr;
            l.meta[s].is_float = (t == DRAKEN_FLOAT32 || t == DRAKEN_FLOAT64);
            l.meta[s].is_string = str_minmax;
            l.meta[s].captured = true;
            l.kinds[s] = gb_kind_of(specs[s], l.meta[s]);
        }
        return true;
    }

    // Queue every non-empty local partition for the parallel merge and reset
    // the local state (adaptive flush + the combine path share this).
    void flush_locals(GroupByGlobal& g, GroupByLocal& l) {
        std::lock_guard<std::mutex> lk(g.mtx);
        if (!g.init) {
            g.meta = l.meta;
            g.kinds = l.kinds;
            g.key_meta = l.key_meta;
            g.has_rows = l.has_rows;
            g.init = true;
        }
        for (size_t p = 0; p < kGBParts; ++p) {
            if (l.parts[p].size() > 0) {
                // The merge probes pending partitions' CarcharIndex directly —
                // promote any live parvi front map so every queued partition's
                // groups are in the index (dense ids preserved).
                if (l.parts[p].use_parvi) l.parts[p].promote_small();
                // A partition that already fell out of Medius proves the column is
                // high-cardinality; remember it before the partition is destroyed.
                if (!l.parts[p].use_mid) l.mid_disabled = true;
                if (l.parts[p].use_mid) l.parts[p].promote_mid();
                g.pending[p].push_back(std::move(l.parts[p]));
                l.parts[p] = GBPartition();
                l.parts[p].use_parvi = low_card;        // fresh partition: re-arm the gate
                l.parts[p].use_mid = !l.mid_disabled;   // ...but never re-arm a lost cause
                type_keycols(l.parts[p], l.key_meta);   // fresh partition needs key types
            }
        }
        l.entries_total = 0;
    }

    SinkResult sink(const MorselPtr& in, GlobalSinkState& gs, LocalSinkState& ls,
                    ErrCtx& err) override {
        auto& l = static_cast<GroupByLocal&>(ls);
        if (in->num_rows() == 0) return SinkResult::CONTINUE;
        if (!l.init) {
            if (!capture(l, in, err)) return SinkResult::CONTINUE;
            l.init = true;
        }
        uint32_t rows = in->num_rows();
        size_t nspecs = specs.size();
        groupby_tel::calls.fetch_add(1, std::memory_order_relaxed);

        // Pass A: draken owns the key hash for the whole morsel (cxx_hash_c is
        // shape-preserving for a single key — it hashes each distinct value once).
        // H20 (2026-08-14): probe once per DISTINCT key, not once per row.
        //
        // For a single key cxx_hash_c is shape-preserving — it returns
        // `data_length` distinct hashes addressed by per-row codes. The dense
        // path below then gathers `out[i] = khashes[codes[i]]` and probes every
        // row, rediscovering groups draken had already separated. On a 65,536-row
        // morsel of RegionID that is ~2-5k distinct hashes turned into 65,536
        // probes; on URL it is ~13.7k turned into 65,536.
        //
        // Compressed path: probe each distinct hash ONCE into (partition, gid),
        // then every row is two small-array lookups indexed by its code. The side
        // arrays are data_length entries, so they stay cache-resident exactly when
        // the win is available. Rows never touch mk_hash/mk_ent at all.
        //
        // Gated: single key only (multi-key hashing is dense per-row by contract),
        // must actually be compressed, and must clear a compression ratio so we
        // never pay the side-array setup for a near-unique column (UserID at ~50k
        // distinct per 65k-row morsel correctly falls through to the dense path).
        // OPTERYX_GB_DICT=0 disables, for A/B from one binary.
        static const bool gb_dict_on = []() {
            const char* v = getenv("OPTERYX_GB_DICT");
            return !(v != nullptr && v[0] == '0' && v[1] == '\0');
        }();
        static const uint32_t kGBDictMaxDistinct = 1u << 14;  // 16,384
        static const uint32_t kGBDictMinRatio = 2;            // distinct*2 <= rows

        // Only the all-Rows shape (plain COUNT(*) GROUP BY) takes the dict path:
        // every other spec kind has a per-row loop further down that indexes the
        // dense mk_hash/mk_ent, and making those range/code-aware is separate work.
        bool gb_rows_only = l.has_rows;
        for (size_t s = 0; s < nspecs && gb_rows_only; ++s)
            if (l.kinds[s] != GBKind::Rows) gb_rows_only = false;

        ShapedKeyHash skh;
        bool dict_path = false;
        if (gb_dict_on && key_idx.size() == 1) {
            GROUPBY_TEL_START(_gbA_t0);
            if (!compute_row_hashes_shaped(in, key_idx, skh, err))
                return SinkResult::CONTINUE;
            GROUPBY_TEL_ACCUM(groupby_tel::hash_ns, _gbA_t0);
            dict_path = skh.compressed() && skh.data_length <= kGBDictMaxDistinct &&
                        static_cast<uint64_t>(skh.data_length) * kGBDictMinRatio <=
                            static_cast<uint64_t>(rows);
            if (!dict_path) {
                // Not worth it: densify from the vector we already have rather
                // than hashing a second time.
                l.mk_hash.resize(rows);
                for (uint32_t i = 0; i < rows; ++i)
                    l.mk_hash[i] = skh.hashes[skh.codes[i]];
                cxx_morsel_delete(skh.owner);
                skh.owner = nullptr;
            }
        } else {
            GROUPBY_TEL_START(_gbA_t0);
            if (!compute_row_hashes(in, key_idx, l.mk_hash, err))
                return SinkResult::CONTINUE;
            GROUPBY_TEL_ACCUM(groupby_tel::hash_ns, _gbA_t0);
        }

        if (dict_path) {
            struct HmGuard {
                CxxMorsel* m;
                ~HmGuard() { if (m != nullptr) cxx_morsel_delete(m); }
            } _hm_guard{skh.owner};

            const uint32_t D = skh.data_length;
            GROUPBY_TEL_START(_gbB_t0);
            // First occurrence of each code — the representative row whose key
            // VALUES a new group stores. Same representative the dense path would
            // have picked (the first row that created the group).
            l.dict_rep.assign(D, UINT32_MAX);
            for (uint32_t i = 0; i < rows; ++i) {
                const uint32_t c = skh.codes[i];
                if (l.dict_rep[c] == UINT32_MAX) l.dict_rep[c] = i;
            }
            // One probe per distinct value.
            l.dict_part.resize(D);
            l.dict_gid.resize(D);
            for (uint32_t d = 0; d < D; ++d) {
                if (l.dict_rep[d] == UINT32_MAX) continue;  // code never used
                const uint64_t h = skh.hashes[d];
                const uint8_t pi = static_cast<uint8_t>(h >> kGBPartShift);
                GBPartition& P = l.parts[pi];
                int64_t gid;
                const bool is_new =
                    P.find_or_insert_group(h, static_cast<int64_t>(P.hashes.size()), gid);
                if (is_new) {
                    P.hashes.push_back(h);
                    for (size_t j = 0; j < store_col_idx.size(); ++j) {
                        P.keycols[j].append_row(in->columns[store_col_idx[j]].view,
                                                l.dict_rep[d], err, "GROUP BY key value");
                        if (err.code != 0) return SinkResult::CONTINUE;
                    }
                }
                l.dict_part[d] = pi;
                l.dict_gid[d] = static_cast<uint32_t>(gid);
            }
            for (size_t p = 0; p < kGBParts; ++p) {
                GBPartition& P = l.parts[p];
                if (P.hashes.empty()) continue;
                size_t nn = P.size();
                if (l.has_rows) P.grows.resize(nn);
                if (P.lanes.size() != nspecs) P.lanes.resize(nspecs);
                if (P.cd.size() != nspecs) P.cd.resize(nspecs);
                for (size_t s = 0; s < nspecs; ++s)
                    gb_lanes_resize(P.lanes[s], l.kinds[s], nn);
            }
            GROUPBY_TEL_ACCUM(groupby_tel::probe_ns, _gbB_t0);

            if (gb_rows_only) {
                // Pass C, dict form: per row, two small-array lookups by code. No
                // mk_hash gather and no mk_ent write anywhere in this path.
                GROUPBY_TEL_START(_gbC_t0);
                for (uint32_t i = 0; i < rows; ++i) {
                    const uint32_t c = skh.codes[i];
                    l.parts[l.dict_part[c]].grows[l.dict_gid[c]] += 1;
                }
                GROUPBY_TEL_ACCUM(groupby_tel::apply_ns, _gbC_t0);
                l.entries_total = 0;
                for (size_t p = 0; p < kGBParts; ++p) l.entries_total += l.parts[p].size();
                if (l.entries_total > kGBFlushEntries)
                    flush_locals(static_cast<GroupByGlobal&>(gs), l);
                return SinkResult::CONTINUE;
            }
            // Other spec kinds (SUM/AVG/MIN/MAX/CountDistinct) each have their own
            // per-row loop below that indexes mk_hash/mk_ent, so fill those from the
            // code arrays and let the generic pass C run unchanged. The expensive
            // part — the PROBES — has already been done once per DISTINCT value;
            // this fill is two sequential array reads and two writes per row, which
            // the hardware prefetcher covers. This is what lets a single-key
            // `SELECT k, sum(v) ... GROUP BY k` use the per-distinct path: every
            // H2O group-by is that shape, and half of TPC-H's single-key ones are.
            l.mk_hash.resize(rows);
            l.mk_ent.resize(rows);
            for (uint32_t i = 0; i < rows; ++i) {
                const uint32_t c = skh.codes[i];
                l.mk_hash[i] = skh.hashes[c];
                l.mk_ent[i] = l.dict_gid[c];
            }
        }

        // Pass B: find-or-insert each row's group into its partition (partition =
        // hash >> kGBPartShift; group id from CarcharIndex, equality by 64-bit hash
        // identity). A NEW group appends the key VALUES nothing above has finished
        // with (this representative row) to the partition's per-column key store —
        // NULL keys collapse to one group via the NULL_HASH sentinel, exactly as SQL
        // GROUP BY requires. A hash-only key stores nothing: it separated the groups
        // in pass A and its purpose is spent.
        GROUPBY_TEL_START(_gbB_t0);
        if (!dict_path) {
        l.mk_ent.resize(rows);

        // ⛔ TESTED AND REJECTED 2026-08-14 — slicing passes B+C.
        // Rationale was sound: morsels cap at 65,536 rows, so the inter-pass scratch
        // (mk_hash 512 KB + mk_ent 256 KB = 768 KB) is 3x the i5-8500's 256 KB
        // per-core L2, written in A, read in B, written in B, both read again in C.
        // Slicing to 2k/8k/16k would have kept it resident.
        // MEASURED: flat on ARM; on x86, flat at 9k groups and ~2% SLOWER at 17.6M
        // and 18.3M, at every slice size, 3 interleaved rounds.
        // WHY IT DOES NOTHING: the scratch is streamed STRICTLY SEQUENTIALLY in all
        // three passes, and hardware prefetch covers that regardless of L2 size — it
        // costs bandwidth, not stalls. The real cost is the probe's RANDOM DEPENDENT
        // chain (control_[slot] -> tag -> hashes_[candidate] -> payload_refs_[slot]),
        // which slicing cannot shorten. Same reason software prefetch failed here.
        // Shorten the chain instead: pack hashes_ and payload_refs_ into one array.
        // ⛔ Compute-path software prefetch stays OUT of this loop. The ban
        // (architect, 2026-07-02) rested on Apple Silicon measurements; it was
        // re-tested HERE on x86 (2026-08-14) in case aggressive Apple hardware
        // prefetchers had been masking a real effect. They were not. x86, 3
        // interleaved rounds, prefetching the control line 8/16 rows ahead
        // (optionally also the hashes_ line):
        //   RegionID  9k groups (L2-resident): 0.570 -> 0.615  = 8% SLOWER
        //   SearchPhrase 6M                  : 1.898 -> 1.898  = flat
        //   UserID   17.6M                   : 1.979 -> 1.923  = 2.8%, ranges barely separate
        //   URL      18.3M                   : 9.564 -> 9.506  = noise
        // Net negative across a mixed workload: the cache-resident regression is
        // large and reproducible, the high-cardinality gain marginal. The ban now
        // holds on BOTH architectures. Restructure the passes instead.
        for (uint32_t i = 0; i < rows; ++i) {
            uint64_t h = l.mk_hash[i];
            GBPartition& P = l.parts[h >> kGBPartShift];
            int64_t gid;
            bool is_new = P.find_or_insert_group(
                h, static_cast<int64_t>(P.hashes.size()), gid);
            if (is_new) {
                P.hashes.push_back(h);
                for (size_t j = 0; j < store_col_idx.size(); ++j) {
                    P.keycols[j].append_row(in->columns[store_col_idx[j]].view, i, err,
                                            "GROUP BY key value");
                    if (err.code != 0) return SinkResult::CONTINUE;
                }
            }
            l.mk_ent[i] = static_cast<uint32_t>(gid);
        }

        // Grow lanes ONCE per morsel to each partition's new entry count
        // (resize zero-fills new groups — the correct initial state).
        for (size_t p = 0; p < kGBParts; ++p) {
            GBPartition& P = l.parts[p];
            if (P.hashes.empty()) continue;
            size_t n = P.size();
            if (l.has_rows) P.grows.resize(n);
            if (P.lanes.size() != nspecs) P.lanes.resize(nspecs);
            if (P.cd.size() != nspecs) P.cd.resize(nspecs);
            for (size_t s = 0; s < nspecs; ++s)
                gb_lanes_resize(P.lanes[s], l.kinds[s], n);
        }
        }  // !dict_path — the dict path probed per DISTINCT and grew lanes already
        GROUPBY_TEL_ACCUM(groupby_tel::probe_ns, _gbB_t0);

        // Pass C: columnar updates — kind dispatched ONCE per spec, tight row loops.
        GROUPBY_TEL_START(_gbC_t0);
        if (l.has_rows) {
            for (uint32_t i = 0; i < rows; ++i)
                l.parts[l.mk_hash[i] >> kGBPartShift].grows[l.mk_ent[i]] += 1;
        }
        for (size_t s = 0; s < nspecs; ++s) {
            GBKind kind = l.kinds[s];
            if (kind == GBKind::Rows) continue;
            if (kind == GBKind::CountDistinct && specs[s].col_idx == kAggWholeRow) {
                // COUNT(DISTINCT *) per group: draken hashes EVERY column into one
                // value hash per row (NULL participates as a normal value — no
                // sort_row_valid skip), then dedup (group, value_hash) per group.
                size_t ncols = in->columns.size();
                std::vector<size_t> allcols(ncols);
                for (size_t c = 0; c < ncols; ++c) allcols[c] = c;
                if (!compute_row_hashes(in, allcols, l.cd_vhash, err))
                    return SinkResult::CONTINUE;
                for (uint32_t i = 0; i < rows; ++i) {
                    GBPartition& P = l.parts[l.mk_hash[i] >> kGBPartShift];
                    uint32_t e = l.mk_ent[i];
                    if (P.cd[s].insert(e, l.cd_vhash[i]))
                        P.lanes[s].valid[e] += 1;
                }
                continue;
            }
            const DrakenVector& v =
                in->columns[static_cast<size_t>(specs[s].col_idx)].view;
            bool want_max = specs[s].fn == AggFn::Max;

            // ---- hoisted out of the row loops (all invariant for this spec) ----
            // lp[p] collapses the per-row `l.parts[p].lanes[s]` chain — array
            // index → vector index → vector data pointer, two dependent loads —
            // into a single array load. Partitions that took no rows this morsel
            // have no lanes; rows can only reach partitions that do.
            GBLanes* lp[kGBParts];
            for (size_t p = 0; p < kGBParts; ++p)
                lp[p] = (l.parts[p].lanes.size() == nspecs) ? &l.parts[p].lanes[s]
                                                            : nullptr;
            const DrakenType   vtype  = v.type;
            const void*        vdata  = v.data;
            const uint32_t*    vsel   = v.selection;
            const uint8_t*     vvalid = v.validity;   // nullptr ⟹ all rows valid
            const bool         is_f   = l.meta[s].is_float;
            // Row-valid test on hoisted locals — same predicate as sort_row_valid.
            auto row_ok = [vvalid](uint32_t i) -> bool {
                return vvalid == nullptr || ((vvalid[i >> 3] >> (i & 7)) & 1u);
            };

            switch (kind) {
                case GBKind::Valid:
                    for (uint32_t i = 0; i < rows; ++i) {
                        if (!row_ok(i)) continue;
                        lp[l.mk_hash[i] >> kGBPartShift]->valid[l.mk_ent[i]] += 1;
                    }
                    break;
                case GBKind::SumI:
                    for (uint32_t i = 0; i < rows; ++i) {
                        if (!row_ok(i)) continue;
                        GBLanes& L = *lp[l.mk_hash[i] >> kGBPartShift];
                        uint32_t e = l.mk_ent[i];
                        int64_t r = agg2_read_raw_at(vtype, vdata, vsel, i, false);
                        if (__builtin_add_overflow(L.i64[e], r, &L.i64[e])) {
                            err.code = 1;
                            err.msg = "SUM overflow: exact integer sum exceeds INT64 "
                                      "— fail loud, never a wrapped answer";
                            return SinkResult::CONTINUE;
                        }
                        L.valid[e] += 1;
                    }
                    break;
                case GBKind::AvgI:
                    for (uint32_t i = 0; i < rows; ++i) {
                        if (!row_ok(i)) continue;
                        GBLanes& L = *lp[l.mk_hash[i] >> kGBPartShift];
                        uint32_t e = l.mk_ent[i];
                        L.i128[e] += agg2_read_raw_at(vtype, vdata, vsel, i, false);
                        L.valid[e] += 1;
                    }
                    break;
                case GBKind::SumF:
                case GBKind::AvgF:
                    for (uint32_t i = 0; i < rows; ++i) {
                        if (!row_ok(i)) continue;
                        GBLanes& L = *lp[l.mk_hash[i] >> kGBPartShift];
                        uint32_t e = l.mk_ent[i];
                        int64_t bits = agg2_read_raw_at(vtype, vdata, vsel, i, true);
                        double d;
                        std::memcpy(&d, &bits, sizeof(d));
                        L.f64[e] += d;
                        L.valid[e] += 1;
                    }
                    break;
                case GBKind::Stddev:
                case GBKind::StddevSamp:
                case GBKind::VarPop:
                case GBKind::VarSamp:
                    // Same Σx/Σx²/count accumulation for all four — only
                    // emit_lane_column's finalize formula differs.
                    for (uint32_t i = 0; i < rows; ++i) {
                        if (!row_ok(i)) continue;
                        GBLanes& L = *lp[l.mk_hash[i] >> kGBPartShift];
                        uint32_t e = l.mk_ent[i];
                        int64_t raw = agg2_read_raw_at(vtype, vdata, vsel, i, is_f);
                        double d;
                        if (is_f) std::memcpy(&d, &raw, sizeof(d));
                        else d = static_cast<double>(raw);
                        L.f64[e] += d;
                        L.f64sq[e] += d * d;
                        L.valid[e] += 1;
                    }
                    break;
                case GBKind::Corr: {
                    const DrakenVector& v2 =
                        in->columns[static_cast<size_t>(specs[s].col_idx2)].view;
                    const DrakenType vtype2 = v2.type;
                    const void*      vdata2 = v2.data;
                    const uint32_t*  vsel2  = v2.selection;
                    const uint8_t*   vvalid2 = v2.validity;
                    const bool       is_f2  = l.meta[s].is_float2;
                    for (uint32_t i = 0; i < rows; ++i) {
                        // Pairwise: skip unless BOTH operands are non-NULL.
                        if (!row_ok(i)) continue;
                        if (vvalid2 != nullptr
                                && ((vvalid2[i >> 3] >> (i & 7)) & 1u) == 0) continue;
                        GBLanes& L = *lp[l.mk_hash[i] >> kGBPartShift];
                        uint32_t e = l.mk_ent[i];
                        int64_t rx = agg2_read_raw_at(vtype, vdata, vsel, i, is_f);
                        int64_t ry = agg2_read_raw_at(vtype2, vdata2, vsel2, i, is_f2);
                        double x, y;
                        if (is_f) std::memcpy(&x, &rx, sizeof(x));
                        else x = static_cast<double>(rx);
                        if (is_f2) std::memcpy(&y, &ry, sizeof(y));
                        else y = static_cast<double>(ry);
                        L.f64[e]   += x;
                        L.f64sq[e] += x * x;
                        L.f64y[e]  += y;
                        L.f64yy[e] += y * y;
                        L.f64xy[e] += x * y;
                        L.valid[e] += 1;
                    }
                    break;
                }
                case GBKind::Median:
                    for (uint32_t i = 0; i < rows; ++i) {
                        if (!row_ok(i)) continue;
                        GBLanes& L = *lp[l.mk_hash[i] >> kGBPartShift];
                        uint32_t e = l.mk_ent[i];
                        int64_t raw = agg2_read_raw_at(vtype, vdata, vsel, i, is_f);
                        double d;
                        if (is_f) std::memcpy(&d, &raw, sizeof(d));
                        else d = static_cast<double>(raw);
                        if (!L.median[e].append(d)) {
                            err.code = 1;
                            err.msg = kMedianCapExceededMsg;
                            return SinkResult::CONTINUE;
                        }
                    }
                    break;
                case GBKind::SumD128:
                case GBKind::AvgD128:
                    for (uint32_t i = 0; i < rows; ++i) {
                        if (!row_ok(i)) continue;
                        GBLanes& L = *lp[l.mk_hash[i] >> kGBPartShift];
                        uint32_t e = l.mk_ent[i];
                        L.i128[e] += agg2_read_i128(v, i);
                        L.valid[e] += 1;
                    }
                    break;
                case GBKind::MinMaxNum:
                    for (uint32_t i = 0; i < rows; ++i) {
                        if (!row_ok(i)) continue;
                        GBLanes& L = *lp[l.mk_hash[i] >> kGBPartShift];
                        uint32_t e = l.mk_ent[i];
                        uint64_t kk = sort_num_key(v, i);
                        if (L.valid[e] == 0
                                || (want_max ? kk > L.mkey[e] : kk < L.mkey[e])) {
                            L.mkey[e] = kk;
                            L.i64[e] = agg2_read_raw(v, i, l.meta[s].is_float);
                        }
                        L.valid[e] += 1;
                    }
                    break;
                case GBKind::MinMaxD128:
                    for (uint32_t i = 0; i < rows; ++i) {
                        if (!row_ok(i)) continue;
                        GBLanes& L = *lp[l.mk_hash[i] >> kGBPartShift];
                        uint32_t e = l.mk_ent[i];
                        __int128 r = agg2_read_i128(v, i);
                        if (L.valid[e] == 0
                                || (want_max ? r > L.i128[e] : r < L.i128[e]))
                            L.i128[e] = r;
                        L.valid[e] += 1;
                    }
                    break;
                case GBKind::ApproxCountDistinct: {
                    // Same draken row hash CountDistinct uses (compute_row_hashes),
                    // fed into a HyperLogLog++ sketch instead of an exact dedup set.
                    // NULLs excluded (sort_row_valid skip), matching CountDistinct.
                    std::vector<size_t> vcol{static_cast<size_t>(specs[s].col_idx)};
                    if (!compute_row_hashes(in, vcol, l.cd_vhash, err))
                        return SinkResult::CONTINUE;
                    for (uint32_t i = 0; i < rows; ++i) {
                        if (!row_ok(i)) continue;
                        GBLanes& L = *lp[l.mk_hash[i] >> kGBPartShift];
                        L.hll[l.mk_ent[i]].add_hash(hll_avalanche(l.cd_vhash[i]));
                    }
                    break;
                }
                case GBKind::ApproxPercentile:
                    for (uint32_t i = 0; i < rows; ++i) {
                        if (!row_ok(i)) continue;
                        GBLanes& L = *lp[l.mk_hash[i] >> kGBPartShift];
                        uint32_t e = l.mk_ent[i];
                        int64_t raw = agg2_read_raw_at(vtype, vdata, vsel, i, is_f);
                        double d;
                        if (is_f) std::memcpy(&d, &raw, sizeof(d));
                        else d = static_cast<double>(raw);
                        td_add(L.td[e].h, d, 1);
                    }
                    break;
                case GBKind::CountDistinct: {
                    // Draken hashes the operand value per row; dedup (group,
                    // value_hash) per group by 64-bit hash identity. COUNT(DISTINCT
                    // col) ignores NULLs (sort_row_valid skip), matching SQL.
                    std::vector<size_t> vcol{static_cast<size_t>(specs[s].col_idx)};
                    if (!compute_row_hashes(in, vcol, l.cd_vhash, err))
                        return SinkResult::CONTINUE;
                    for (uint32_t i = 0; i < rows; ++i) {
                        if (!row_ok(i)) continue;
                        GBPartition& P = l.parts[l.mk_hash[i] >> kGBPartShift];
                        uint32_t e = l.mk_ent[i];
                        if (P.cd[s].insert(e, l.cd_vhash[i]))
                            P.lanes[s].valid[e] += 1;
                    }
                    break;
                }
                case GBKind::MinMaxStr: {
                    const DrakenStringArena* sa = string_arena_of(v);
                    for (uint32_t i = 0; i < rows; ++i) {
                        if (!row_ok(i)) continue;
                        GBLanes& L = *lp[l.mk_hash[i] >> kGBPartShift];
                        uint32_t e = l.mk_ent[i];
                        const DrakenStringSlot* slot = &sa->slots[v.selection[i]];
                        const char* p =
                            reinterpret_cast<const char*>(str_data(slot, sa->arena));
                        uint32_t len = str_length(slot);
                        std::string_view sv(p, len);
                        std::string& cur = L.sval[e];
                        if (L.valid[e] == 0
                                || (want_max ? sv > std::string_view(cur)
                                             : sv < std::string_view(cur)))
                            cur.assign(p, len);
                        L.valid[e] += 1;
                    }
                    break;
                }
                case GBKind::ArrayAgg: {
                    // NO sort_row_valid() skip — NULLs are elements of the list.
                    // DISTINCT/ORDER BY/LIMIT are NOT applied here: this worker
                    // holds an arbitrary row subset, so they must wait for the
                    // merged list at finalize.
                    const AAStore st = aa_store_of(l.meta[s].type);
                    const bool is_f = l.meta[s].is_float;
                    const DrakenStringArena* sa =
                        (st == AAStore::Str) ? string_arena_of(v) : nullptr;
                    for (uint32_t i = 0; i < rows; ++i) {
                        GBArrayAggState& A =
                            l.parts[l.mk_hash[i] >> kGBPartShift].lanes[s].aa[l.mk_ent[i]];
                        bool nul = !sort_row_valid(v, i);
                        if (st == AAStore::Str) {
                            const char* p = nullptr;
                            uint32_t len = 0;
                            if (!nul) {
                                const DrakenStringSlot* slot = &sa->slots[v.selection[i]];
                                p = reinterpret_cast<const char*>(
                                    str_data(slot, sa->arena));
                                len = str_length(slot);
                            }
                            A.push(st, nul, 0, 0, p, len);
                        } else if (st == AAStore::I128) {
                            A.push(st, nul, 0, nul ? 0 : agg2_read_i128(v, i),
                                   nullptr, 0);
                        } else {
                            A.push(st, nul, nul ? 0 : agg2_read_raw_at(vtype, vdata, vsel, i, is_f), 0,
                                   nullptr, 0);
                        }
                    }
                    break;
                }
                case GBKind::CidrAgg: {
                    // NULLs ARE skipped here, unlike ARRAY_AGG one case up. A NULL
                    // is not an address, so it is not a member of the set; a list
                    // keeps NULL because it has a slot to keep it in, a set does
                    // not. The operand is validated UINT32+IPV4, so the raw read
                    // is never a float and the value IS the address (octet A in
                    // bits 31..24 — see draken/core/ipv4.h).
                    for (uint32_t i = 0; i < rows; ++i) {
                        if (!sort_row_valid(v, i)) continue;
                        opteryx::roaring32::Roaring32& R =
                            l.parts[l.mk_hash[i] >> kGBPartShift].lanes[s].cidr[l.mk_ent[i]];
                        // Return ignored deliberately: a refusal latches
                        // R.overflowed, which emit_cidr_lane_column raises on.
                        // Checking per row would branch the hot loop to reach the
                        // same outcome one morsel earlier.
                        (void)R.add(static_cast<uint32_t>(
                            agg2_read_raw_at(vtype, vdata, vsel, i, false)));
                    }
                    break;
                }
                case GBKind::Rows:
                    break;
            }
        }
        l.entries_total = 0;
        for (size_t p = 0; p < kGBParts; ++p) {
            GBPartition& P = l.parts[p];
            l.entries_total += P.size();
            // COUNT(DISTINCT) pair tables ride the same cache-residency
            // economics: a 25M-pair table (Q09: few groups, many pairs) is the
            // same DRAM-probe wall the group tables had.
            for (const GBCountDistinct& d : P.cd) l.entries_total += d.size();
        }
        GROUPBY_TEL_ACCUM(groupby_tel::apply_ns, _gbC_t0);
        if (l.entries_total > kGBFlushEntries)
            flush_locals(static_cast<GroupByGlobal&>(gs), l);
        return SinkResult::CONTINUE;
    }

    void combine(GlobalSinkState& gs, LocalSinkState& ls, ErrCtx&) override {
        auto& l = static_cast<GroupByLocal&>(ls);
        if (!l.init) return;
        flush_locals(static_cast<GroupByGlobal&>(gs), l);
    }

    // Merge one partition's queued worker tables into the first, then emit it in
    // chunk_rows morsels. Partitions are disjoint; runs concurrently across them.
    void merge_and_emit_partition(GroupByGlobal& g, size_t p,
                                  std::vector<MorselPtr>& out_morsels,
                                  ErrCtx& err) {
        auto& list = g.pending[p];
        if (list.empty()) return;
        size_t nspecs = specs.size();
        GBPartition merged = std::move(list[0]);
        // Pre-size ONCE to the (known) worst case — geometric growth during the
        // merge re-inserted every entry ~17 times on 90M-group aggregations.
        size_t merge_total = merged.size();
        for (size_t i = 1; i < list.size(); ++i) merge_total += list[i].size();
        if (merge_total > merged.size()) {
            merged.index.reserve(merge_total);
            merged.hashes.reserve(merge_total);
        }
        std::vector<uint32_t> ge;
        for (size_t i = 1; i < list.size(); ++i) {
            GBPartition& src = list[i];
            uint32_t sn = static_cast<uint32_t>(src.size());
            // Columnar merge: map every src group to its merged group first (hash
            // identity via CarcharIndex; a new merged group copies its key VALUES
            // from src), then combine lane by lane (kind dispatched once per spec).
            ge.resize(sn);
            for (uint32_t e = 0; e < sn; ++e) {
                int64_t mg;
                bool is_new = merged.index.find_or_insert_id(
                    src.hashes[e], static_cast<int64_t>(merged.hashes.size()), mg);
                if (is_new) {
                    merged.hashes.push_back(src.hashes[e]);
                    for (size_t k = 0; k < merged.keycols.size(); ++k)
                        merged.keycols[k].append_from(src.keycols[k], e);
                }
                ge[e] = static_cast<uint32_t>(mg);
            }
            size_t mn = merged.size();
            if (g.has_rows) merged.grows.resize(mn);
            if (merged.lanes.size() != nspecs) merged.lanes.resize(nspecs);
            if (merged.cd.size() != nspecs) merged.cd.resize(nspecs);
            for (size_t s = 0; s < nspecs; ++s)
                gb_lanes_resize(merged.lanes[s], g.kinds[s], mn);
            if (g.has_rows) {
                for (uint32_t e = 0; e < sn; ++e)
                    merged.grows[ge[e]] += src.grows[e];
            }
            for (size_t s = 0; s < nspecs; ++s) {
                GBKind kind = g.kinds[s];
                if (kind == GBKind::Rows) continue;
                GBLanes& D = merged.lanes[s];
                const GBLanes& S = src.lanes[s];
                bool want_max = specs[s].fn == AggFn::Max;
                switch (kind) {
                    case GBKind::Valid:
                        for (uint32_t e = 0; e < sn; ++e)
                            D.valid[ge[e]] += S.valid[e];
                        break;
                    case GBKind::SumI:
                        for (uint32_t e = 0; e < sn; ++e) {
                            uint32_t m = ge[e];
                            if (__builtin_add_overflow(D.i64[m], S.i64[e],
                                                       &D.i64[m])) {
                                err.code = 1;
                                err.msg = "SUM overflow: exact integer sum exceeds "
                                          "INT64 — fail loud, never a wrapped answer";
                                return;
                            }
                            D.valid[m] += S.valid[e];
                        }
                        break;
                    case GBKind::AvgI:
                        for (uint32_t e = 0; e < sn; ++e) {
                            uint32_t m = ge[e];
                            D.i128[m] += S.i128[e];
                            D.valid[m] += S.valid[e];
                        }
                        break;
                    case GBKind::SumF:
                    case GBKind::AvgF:
                        for (uint32_t e = 0; e < sn; ++e) {
                            uint32_t m = ge[e];
                            D.f64[m] += S.f64[e];
                            D.valid[m] += S.valid[e];
                        }
                        break;
                    case GBKind::Stddev:
                    case GBKind::StddevSamp:
                    case GBKind::VarPop:
                    case GBKind::VarSamp:
                        for (uint32_t e = 0; e < sn; ++e) {
                            uint32_t m = ge[e];
                            D.f64[m] += S.f64[e];
                            D.f64sq[m] += S.f64sq[e];
                            D.valid[m] += S.valid[e];
                        }
                        break;
                    case GBKind::Corr:
                        for (uint32_t e = 0; e < sn; ++e) {
                            uint32_t m = ge[e];
                            D.f64[m]   += S.f64[e];
                            D.f64sq[m] += S.f64sq[e];
                            D.f64y[m]  += S.f64y[e];
                            D.f64yy[m] += S.f64yy[e];
                            D.f64xy[m] += S.f64xy[e];
                            D.valid[m] += S.valid[e];
                        }
                        break;
                    case GBKind::Median: {
                        // MedianState has no merge-by-move (unlike ArrayAgg's element
                        // lists) — append each source value into the dest group's
                        // state. Total buffered values are budget-bounded (512MB
                        // global), so this is bounded work, not a hot-path concern.
                        GBLanes& SL = src.lanes[s];
                        for (uint32_t e = 0; e < sn; ++e) {
                            opteryx::ungrouped::MedianState& src_st = SL.median[e];
                            opteryx::ungrouped::MedianState& dst_st = D.median[ge[e]];
                            for (size_t k = 0; k < src_st.size; ++k) {
                                if (!dst_st.append(src_st.buf[k])) {
                                    err.code = 1;
                                    err.msg = kMedianCapExceededMsg;
                                    return;
                                }
                            }
                        }
                        break;
                    }
                    case GBKind::SumD128:
                    case GBKind::AvgD128:
                        for (uint32_t e = 0; e < sn; ++e) {
                            uint32_t m = ge[e];
                            D.i128[m] += S.i128[e];
                            D.valid[m] += S.valid[e];
                        }
                        break;
                    case GBKind::MinMaxNum:
                        for (uint32_t e = 0; e < sn; ++e) {
                            if (S.valid[e] == 0) continue;
                            uint32_t m = ge[e];
                            if (D.valid[m] == 0
                                    || (want_max ? S.mkey[e] > D.mkey[m]
                                                 : S.mkey[e] < D.mkey[m])) {
                                D.mkey[m] = S.mkey[e];
                                D.i64[m] = S.i64[e];
                            }
                            D.valid[m] += S.valid[e];
                        }
                        break;
                    case GBKind::MinMaxD128:
                        for (uint32_t e = 0; e < sn; ++e) {
                            if (S.valid[e] == 0) continue;
                            uint32_t m = ge[e];
                            if (D.valid[m] == 0
                                    || (want_max ? S.i128[e] > D.i128[m]
                                                 : S.i128[e] < D.i128[m]))
                                D.i128[m] = S.i128[e];
                            D.valid[m] += S.valid[e];
                        }
                        break;
                    case GBKind::MinMaxStr:
                        for (uint32_t e = 0; e < sn; ++e) {
                            if (S.valid[e] == 0) continue;
                            uint32_t m = ge[e];
                            if (D.valid[m] == 0
                                    || (want_max ? S.sval[e] > D.sval[m]
                                                 : S.sval[e] < D.sval[m]))
                                D.sval[m] = S.sval[e];
                            D.valid[m] += S.valid[e];
                        }
                        break;
                    case GBKind::ArrayAgg: {
                        // Concatenate the worker's list onto the merged one. The
                        // destination's growth is charged against the global byte
                        // budget here just as it is on append: N workers each holding
                        // a share of one group can cross the budget on merge even when
                        // no single worker did.
                        const AAStore ast = aa_store_of(g.meta[s].type);
                        // The other arms only read the source, so the loop aliases it
                        // const. This one drains it: `src` is released right after the
                        // merge, so its element strings are moved, not copied.
                        GBLanes& SL = src.lanes[s];
                        for (uint32_t e = 0; e < sn; ++e) {
                            D.aa[ge[e]].append_from(ast, SL.aa[e]);
                        }
                        break;
                    }
                    case GBKind::CidrAgg: {
                        // Union — the set operation, so merging is order- and
                        // duplication-insensitive and needs no finalize-time
                        // reconciliation. Two workers that both saw an address
                        // contribute it once, which is exactly what makes the
                        // partitioned plan agree with the serial one.
                        //
                        // Charged against the state budget the same way an insert
                        // is: N workers each under the ceiling can still cross it
                        // combined, and a refusal latches overflowed on the
                        // destination for emit to raise on.
                        for (uint32_t e = 0; e < sn; ++e) {
                            (void)D.cidr[ge[e]].merge_from(src.lanes[s].cidr[e]);
                        }
                        break;
                    }
                    case GBKind::ApproxCountDistinct:
                        for (uint32_t e = 0; e < sn; ++e) {
                            if (!D.hll[ge[e]].merge(S.hll[e])) {
                                err.code = 1;
                                err.msg = "APPROX_COUNT_DISTINCT sketch "
                                          "merge failed (precision mismatch) — "
                                          "unreachable, every sketch shares one fixed "
                                          "precision";
                                return;
                            }
                        }
                        break;
                    case GBKind::ApproxPercentile: {
                        // td_merge's `from` isn't const in the vendored C API —
                        // mutable alias, like Median's src-consuming merge.
                        GBLanes& SL = src.lanes[s];
                        for (uint32_t e = 0; e < sn; ++e)
                            td_merge(D.td[ge[e]].h, SL.td[e].h);
                        break;
                    }
                    case GBKind::CountDistinct: {
                        // Re-key each distinct (group, value_hash) pair under the
                        // merged partition's renumbered group ids; the same pair may
                        // arrive from several workers — only a merged-set MISS counts.
                        GBCountDistinct& SC = src.cd[s];
                        GBCountDistinct& DC = merged.cd[s];
                        for (size_t pi = 0; pi < SC.size(); ++pi) {
                            uint32_t m = ge[SC.pair_gid[pi]];
                            if (DC.insert(m, SC.pair_vhash[pi])) D.valid[m] += 1;
                        }
                        break;
                    }
                    case GBKind::Rows:
                        break;
                }
            }
            src = GBPartition();   // release the merged-in worker table
        }
        // Emit chunk_rows-group morsels — lanes are contiguous vectors, so a
        // chunk is a plain slice.
        size_t total = merged.size();
        for (size_t start = 0; start < total; start += chunk_rows) {
            uint32_t n = static_cast<uint32_t>(std::min(chunk_rows, total - start));
            auto m = std::make_shared<CxxMorsel>();
            m->zero_col_rows = n;
            // Group-key columns: materialize a contiguous [start, start+n) slice of
            // each per-group key store (GroupKeyColumn) into the output morsel. Only
            // the emitted keys have a store — a hash-only key has no column here.
            for (size_t k = 0; k < merged.keycols.size(); ++k) {
                m->columns.push_back(jpc_emit_range(merged.keycols[k], start, n, err));
                if (err.code != 0) return;
                m->names.push_back(store_names[k]);
            }
            for (size_t s = 0; s < nspecs; ++s) {
                GBKind kind = g.kinds[s];
                const GBLanes& L = merged.lanes[s];
                GBLaneView lv;
                if (kind == GBKind::Rows) {
                    lv.rows = merged.grows.data() + start;
                } else if (kind == GBKind::ArrayAgg) {
                    lv.aa = const_cast<GBArrayAggState*>(L.aa.data()) + start;
                    lv.aa_spec = &specs[s];
                } else if (kind == GBKind::CidrAgg) {
                    // No `valid` lane (see gb_lanes_resize) — never NULL.
                    lv.cidr = const_cast<opteryx::roaring32::Roaring32*>(L.cidr.data()) + start;
                } else if (kind == GBKind::Median) {
                    // No `valid` lane (Median never allocates one — see
                    // gb_lanes_resize); null-ness is each state's own size==0.
                    lv.median = const_cast<opteryx::ungrouped::MedianState*>(
                        L.median.data()) + start;
                } else if (kind == GBKind::ApproxCountDistinct) {
                    // No `valid` lane either — never NULL (see emit_lane_column).
                    lv.hll = L.hll.data() + start;
                } else if (kind == GBKind::ApproxPercentile) {
                    // No `valid` lane: null-ness is each digest's own td_size()==0.
                    lv.td = const_cast<TDigestPtr*>(L.td.data()) + start;
                    lv.pct_spec = &specs[s];
                } else {
                    lv.valid = L.valid.data() + start;
                    if (!L.i64.empty())   lv.i64   = L.i64.data() + start;
                    if (!L.f64.empty())   lv.f64   = L.f64.data() + start;
                    if (!L.f64sq.empty()) lv.f64sq = L.f64sq.data() + start;
                    if (!L.f64y.empty())  lv.f64y  = L.f64y.data() + start;
                    if (!L.f64yy.empty()) lv.f64yy = L.f64yy.data() + start;
                    if (!L.f64xy.empty()) lv.f64xy = L.f64xy.data() + start;
                    if (!L.i128.empty())  lv.i128  = L.i128.data() + start;
                    if (!L.sval.empty())  lv.sval  = L.sval.data() + start;
                }
                m->columns.push_back(emit_lane_column(g.meta[s], kind, lv, n, err));
                if (err.code != 0) return;
                m->names.push_back(specs[s].name);
            }
            out_morsels.push_back(std::move(m));
        }
    }

    void finalize(GlobalSinkState& gs, ErrCtx& err) override {
        auto& g = static_cast<GroupByGlobal&>(gs);
        // Adaptive parallelism: thread spawn/join costs ~ms — pure overhead for
        // small aggregations (profiled: sub-50ms queries doubled under 14 idle
        // spawns). Thread only when the queued group count justifies it.
        size_t total_entries = 0;
        size_t nonempty_parts = 0;
        for (size_t p = 0; p < kGBParts; ++p) {
            size_t part_entries = 0;
            for (const GBPartition& pt : g.pending[p]) part_entries += pt.size();
            total_entries += part_entries;
            if (part_entries > 0) ++nonempty_parts;
        }
        unsigned hw = std::thread::hardware_concurrency();
        unsigned nt = hw > 2 ? hw - 2 : 1;
        if (nt > 16) nt = 16;
        if (nt > nonempty_parts) nt = static_cast<unsigned>(nonempty_parts);
        if (total_entries < 65536) nt = 1;   // small agg: inline, no threads
        if (nt < 1) nt = 1;
        std::atomic<size_t> next_part{0};
        std::mutex out_mtx;
        std::vector<ErrCtx> errs(nt);
        auto worker = [&](unsigned tid) {
            std::vector<MorselPtr> local_out;
            for (;;) {
                size_t p = next_part.fetch_add(1);
                if (p >= kGBParts) break;
                merge_and_emit_partition(g, p, local_out, errs[tid]);
                if (errs[tid].code != 0) break;
            }
            if (!local_out.empty()) {
                std::lock_guard<std::mutex> lk(out_mtx);
                for (MorselPtr& m : local_out) out->morsels.push_back(std::move(m));
            }
        };
        std::vector<std::thread> threads;
        threads.reserve(nt > 0 ? nt - 1 : 0);
        for (unsigned t = 1; t < nt; ++t) threads.emplace_back(worker, t);
        worker(0);
        for (auto& th : threads) th.join();
        for (unsigned t = 0; t < nt; ++t) {
            if (errs[t].code != 0) { err = errs[t]; return; }
        }
    }
};

// ---- DistinctSink -------------------------------------------------------------------

// One worker's locally-new rows for ONE hash partition. Bucketed at sink time by
// the top kGBPartShift bits of the dedup hash — the same partitioning GroupBySink
// uses — so the cross-worker dedup can run per-partition in parallel at finalize.
struct DistinctPart {
    std::vector<uint64_t> hashes;         // dedup hash per kept row
    std::vector<uint32_t> ref_m, ref_r;   // (morsel, row) of each locally-new row
};
// One worker's queued contribution to a partition. `base` is where that worker's
// retained morsels landed in the global morsel list, applied to ref_m at merge time
// (not under the combine lock).
struct DistinctChunk {
    uint32_t base;
    DistinctPart part;
};

struct DistinctLocal : LocalSinkState {
    opteryx::carchar::CarcharSet seen;    // per-worker dedup on the 64-bit key hash
    opteryx::parvi::ParviSet small;       // low-card front set (kDistinctParviGateNDV)
    bool use_parvi = false;               // armed by DistinctSink when the estimate is low
    std::vector<MorselPtr> morsels;       // source morsels a kept row references
    std::array<DistinctPart, kGBParts> parts;   // locally-new rows, hash-partitioned
    std::vector<uint64_t> rowh;           // per-morsel scratch: dense per-row hash
    std::vector<int32_t> newidx;          // per-morsel scratch: indices of locally-new rows
};
struct DistinctGlobal : GlobalSinkState {
    std::mutex mtx;
    std::vector<MorselPtr> morsels;
    // Per-partition queues of worker contributions. combine() only MOVES into these
    // (O(kGBParts) under the mutex); the cross-worker dedup itself happens in
    // finalize(), in parallel across disjoint partitions. The old design deduped in
    // combine() through one global CarcharSet under this mutex — measured on TPC-H
    // SF100 q22 (GROUP BY o_custkey, 10M distinct in 150M rows) that serialized
    // ~6M surviving rows PER WORKER through one lock: 3.4 cores busy at dop=16 and
    // wall time RISING past dop=4.
    std::array<std::vector<DistinctChunk>, kGBParts> pending;
};

struct DistinctSink : Sink {
    std::vector<size_t> on_idx;   // dedup key columns; empty = all columns
    MorselBuffer* out;
    size_t chunk_rows;
    bool low_card;   // planner NDV estimate <= kDistinctParviGateNDV → parvi front set

    DistinctSink(std::vector<size_t> on, MorselBuffer* b, int64_t ndv_estimate,
                 size_t chunk = 131072)
        : on_idx(std::move(on)), out(b), chunk_rows(chunk),
          low_card(ndv_estimate >= 0 && ndv_estimate <= kDistinctParviGateNDV) {}

    std::unique_ptr<GlobalSinkState> make_global() override {
        return std::make_unique<DistinctGlobal>();
    }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override {
        auto l = std::make_unique<DistinctLocal>();
        if (low_card) {
            l->use_parvi = true;
            groupby_tel::distinct_parvi_sinks.fetch_add(1, std::memory_order_relaxed);
        }
        return l;
    }

    SinkResult sink(const MorselPtr& in, GlobalSinkState&, LocalSinkState& ls,
                    ErrCtx& err) override {
        auto& l = static_cast<DistinctLocal&>(ls);
        uint32_t rows = in->num_rows();
        if (rows == 0) return SinkResult::CONTINUE;

        // Key columns: explicit on_idx, or every column when empty (SELECT DISTINCT *).
        size_t ncols = in->columns.size();
        size_t nkey = on_idx.empty() ? ncols : on_idx.size();
        std::vector<int32_t> col_idxs(nkey);
        if (on_idx.empty())
            for (size_t c = 0; c < ncols; ++c) col_idxs[c] = static_cast<int32_t>(c);
        else
            for (size_t c = 0; c < nkey; ++c) col_idxs[c] = static_cast<int32_t>(on_idx[c]);

        // Draken owns the hash: cxx_hash_c hashes the key columns into one INT64 hash
        // column. Equality is 64-bit hash identity (the sanctioned contract) — the
        // CarcharSet stores no key bytes. Null rows collapse to the NULL_HASH sentinel,
        // so all-NULL keys form one distinct row exactly as SQL DISTINCT requires.
        CxxMorsel* hashm = cxx_hash_c(in.get(), col_idxs.data(),
                                      static_cast<uint32_t>(nkey));
        if (hashm == nullptr) {
            err.code = 1;
            err.msg = "native DISTINCT: cxx_hash_c allocation failed";
            return SinkResult::CONTINUE;
        }
        const DrakenVector& hv = hashm->columns[0].view;
        const uint64_t* khashes = static_cast<const uint64_t*>(hv.data);
        const uint32_t* codes = hv.selection;   // never NULL (draken invariant)
        // Densify to one hash per row for the batch dedup probe. (The shape-preserving
        // k-probe fast path for a single compressed key — hashing each distinct value
        // once — is a deferred optimization; the uniform path is correct for all shapes.)
        l.rowh.resize(rows);
        for (uint32_t i = 0; i < rows; ++i) l.rowh[i] = khashes[codes[i]];
        cxx_morsel_delete(hashm);

        // Local dedup: the indices of rows whose hash is new to THIS worker.
        l.newidx.resize(rows);
        size_t nnew;
        if (l.use_parvi) {
            const auto [count, overflow] =
                l.small.mark_new_indices(l.rowh.data(), l.newidx.data(), rows);
            nnew = count;
            if (overflow) {
                // Estimate was wrong — drain into the CarcharSet and rescan the
                // full range on it. Rows the parvi pass already marked new are in
                // the drained set (no longer new), so the rescan appends only the
                // unprocessed tail's new rows — index order stays ascending.
                groupby_tel::distinct_parvi_promotes.fetch_add(1, std::memory_order_relaxed);
                l.small.drain_into(l.seen);
                l.use_parvi = false;
                nnew += l.seen.mark_new_indices_32(l.rowh.data(),
                                                   l.newidx.data() + nnew, rows);
            }
        } else {
            nnew = l.seen.mark_new_indices_32(l.rowh.data(), l.newidx.data(), rows);
        }
        if (nnew == 0) return SinkResult::CONTINUE;

        uint32_t mi = static_cast<uint32_t>(l.morsels.size());
        l.morsels.push_back(in);   // retain: a kept row references it
        for (size_t j = 0; j < nnew; ++j) {
            uint32_t row = static_cast<uint32_t>(l.newidx[j]);
            uint64_t h = l.rowh[row];
            DistinctPart& P = l.parts[h >> kGBPartShift];
            P.hashes.push_back(h);
            P.ref_m.push_back(mi);
            P.ref_r.push_back(row);
        }
        return SinkResult::CONTINUE;
    }

    void combine(GlobalSinkState& gs, LocalSinkState& ls, ErrCtx&) override {
        auto& g = static_cast<DistinctGlobal&>(gs);
        auto& l = static_cast<DistinctLocal&>(ls);
        std::lock_guard<std::mutex> lk(g.mtx);
        uint32_t base = static_cast<uint32_t>(g.morsels.size());
        for (MorselPtr& m : l.morsels) g.morsels.push_back(std::move(m));
        // Queue, never merge, under the lock: O(kGBParts) moves per worker. The
        // per-row cross-worker dedup runs partition-parallel in finalize().
        for (size_t p = 0; p < kGBParts; ++p) {
            if (!l.parts[p].hashes.empty())
                g.pending[p].push_back(DistinctChunk{base, std::move(l.parts[p])});
        }
    }

    void finalize(GlobalSinkState& gs, ErrCtx& err) override {
        auto& g = static_cast<DistinctGlobal&>(gs);
        size_t total = 0;
        size_t nonempty_parts = 0;
        for (size_t p = 0; p < kGBParts; ++p) {
            size_t pe = 0;
            for (const DistinctChunk& c : g.pending[p]) pe += c.part.hashes.size();
            total += pe;
            if (pe > 0) ++nonempty_parts;
        }
        if (total == 0 || g.morsels.empty()) return;
        const std::vector<std::string>& names = g.morsels.front()->names;
        // Same adaptive one-shot pool-let as GroupBySink::finalize: partitions are
        // disjoint by hash, so they dedup AND emit in parallel; output order across
        // partitions is unspecified — exactly SQL's contract for DISTINCT / GROUP BY
        // without ORDER BY (and the compiler never dop-1-pins this sink's consumer).
        unsigned hw = std::thread::hardware_concurrency();
        unsigned nt = hw > 2 ? hw - 2 : 1;
        if (nt > 16) nt = 16;
        if (nt > nonempty_parts) nt = static_cast<unsigned>(nonempty_parts);
        if (total < 65536) nt = 1;   // small distinct: inline, no threads
        if (nt < 1) nt = 1;
        std::atomic<size_t> next_part{0};
        std::mutex out_mtx;
        std::vector<ErrCtx> errs(nt);
        auto worker = [&](unsigned tid) {
            std::vector<MorselPtr> local_out;
            std::vector<uint32_t> rm, rr;      // partition survivors: (morsel, row)
            std::vector<int32_t> newi;         // per-chunk scratch
            for (;;) {
                size_t p = next_part.fetch_add(1);
                if (p >= kGBParts) break;
                auto& list = g.pending[p];
                if (list.empty()) continue;
                size_t ptotal = 0;
                for (const DistinctChunk& c : list) ptotal += c.part.hashes.size();
                opteryx::carchar::CarcharSet seen;
                seen.reserve(ptotal);
                rm.clear(); rr.clear();
                for (const DistinctChunk& c : list) {
                    size_t n = c.part.hashes.size();
                    newi.resize(n);
                    size_t nn = seen.mark_new_indices_32(c.part.hashes.data(),
                                                         newi.data(), n);
                    for (size_t j = 0; j < nn; ++j) {
                        uint32_t i = static_cast<uint32_t>(newi[j]);
                        rm.push_back(c.base + c.part.ref_m[i]);
                        rr.push_back(c.part.ref_r[i]);
                    }
                }
                size_t kept = rm.size();
                if (kept == 0) continue;
                // gather_rows indexes rows by GLOBAL row id via (row_m, row_r) maps —
                // here the refs already ARE (morsel, row) pairs, so feed them through.
                std::vector<uint32_t> order(kept);
                for (size_t i = 0; i < kept; ++i) order[i] = static_cast<uint32_t>(i);
                for (size_t start = 0; start < kept; start += chunk_rows) {
                    size_t count = std::min(chunk_rows, kept - start);
                    MorselPtr m = gather_rows(g.morsels, order, start, count, rm, rr,
                                              names, errs[tid]);
                    if (errs[tid].code != 0) return;
                    local_out.push_back(std::move(m));
                }
            }
            if (!local_out.empty()) {
                std::lock_guard<std::mutex> lk(out_mtx);
                for (MorselPtr& m : local_out) out->morsels.push_back(std::move(m));
            }
        };
        std::vector<std::thread> threads;
        threads.reserve(nt > 0 ? nt - 1 : 0);
        for (unsigned t = 1; t < nt; ++t) threads.emplace_back(worker, t);
        worker(0);
        for (std::thread& t : threads) t.join();
        for (unsigned t = 0; t < nt; ++t) {
            if (errs[t].code != 0) { err = errs[t]; return; }
        }
    }
};

// ---- WindowTopKSink (ROW_NUMBER top-K per partition, no full sort) -----------------
// `ROW_NUMBER() OVER (PARTITION BY p... ORDER BY o)` with a WindowTopKFusionStrategy-
// fused `WHERE rn <= K`: streams instead of buffer-then-sort. sink() hashes each
// morsel's partition columns (compute_row_hashes — the same 64-bit hash-only
// identity contract GROUP BY/DISTINCT use) and maintains, per worker, a bounded
// max-heap of the K best rows per partition hash — O(log K) per row instead of an
// O(n log n) sort of every row, bucketed by the top hash bits (kGBParts). combine()
// only QUEUES each worker's bucket maps under the global mutex (O(kGBParts) moves —
// bounded per bucket: at most K survivors per partition per worker). finalize()
// merges the queued worker heaps bucket-parallel (buckets are disjoint by hash),
// sorts each partition's <= K survivors by the order key (cheap — K is small) to
// assign sequential ROW_NUMBERs, and emits.
//
// Scope: the compiler only routes here when there is exactly one ROW_NUMBER
// function (no RANK/DENSE_RANK — tie handling needs the full rank first, see
// WindowSink) and exactly one ORDER BY column of a fixed-width numeric/temporal/
// bool type (sort_num_key's supported set; VARCHAR/DECIMAL128 order keys use
// WindowSink instead). Row order among order-key ties is NOT guaranteed to match
// WindowSink's arrival-order tie-break — SQL doesn't guarantee one for ROW_NUMBER
// either, so this is a legitimate (if different) answer, not a correctness gap.

struct WindowTopKCandidate {
    uint64_t key;         // sort_num_key(), direction-normalized so smaller = better
    uint8_t valid;         // order-key validity; 0 = NULL
    uint32_t morsel_idx;   // index into the owning state's retained-morsels vector
    uint32_t row;
};

// NULLS FIRST under ASC / NULLS LAST under DESC (native_sort.hpp's documented
// convention) — matches SortKeyCmp exactly. true if `a` ranks strictly earlier
// (better) than `b` — a plain ascending "<" over the rank ordering, so feeding
// this directly as push_heap/pop_heap's comparator gives front() == the WORST
// kept candidate (the one nothing else ranks behind), which is what a bounded
// top-K eviction needs: cheap access to "which one goes if a better one shows up".
struct WindowTopKBetter {
    bool ascending;
    bool operator()(const WindowTopKCandidate& a, const WindowTopKCandidate& b) const {
        if (a.valid != b.valid) return ascending ? (a.valid == 0) : (a.valid != 0);
        if (!a.valid) return false;   // both NULL — tie, arbitrary/stable is fine
        return a.key < b.key;         // smaller normalized key = ranks earlier/better
    }
};

// Offer `c` into a bounded (<= k) heap of the current top-K: fills up to k, then
// only replaces the current worst (heap.front(), under WindowTopKBetter) if `c`
// ranks better than it.
inline void window_topk_offer(std::vector<WindowTopKCandidate>& heap,
                              const WindowTopKCandidate& c, size_t k,
                              const WindowTopKBetter& better) {
    if (heap.size() < k) {
        heap.push_back(c);
        std::push_heap(heap.begin(), heap.end(), better);
    } else if (better(c, heap.front())) {   // c ranks better than the current worst -> c survives
        std::pop_heap(heap.begin(), heap.end(), better);
        heap.back() = c;
        std::push_heap(heap.begin(), heap.end(), better);
    }
}

using WindowTopKHeapMap = std::unordered_map<uint64_t, std::vector<WindowTopKCandidate>>;

struct WindowTopKLocal : LocalSinkState {
    std::vector<MorselPtr> morsels;
    // Heaps bucketed by the top kGBPartShift bits of the partition hash (the same
    // partitioning GroupBySink/DistinctSink use) so the cross-worker merge can run
    // per-bucket in parallel at finalize.
    std::array<WindowTopKHeapMap, kGBParts> heaps;
};
struct WindowTopKGlobal : GlobalSinkState {
    std::mutex mtx;
    std::vector<MorselPtr> morsels;
    // Per-bucket queues of (morsel base, worker heap map). combine() only MOVES into
    // these (O(kGBParts) under the mutex); the heap merge itself happens in
    // finalize(), in parallel across disjoint buckets. The old design offered every
    // worker candidate into one global map under this mutex — measured on h2o g8
    // (1M partitions × 16 workers) that serialized ~16M map probes through one lock:
    // 17.2s of combine wall against a 3.5s pipeline.
    std::array<std::vector<std::pair<uint32_t, WindowTopKHeapMap>>, kGBParts> pending;
};

struct WindowTopKSink : Sink {
    std::vector<size_t> part_idx;   // partition-key column indices (hashed, not sorted)
    size_t order_idx;               // single ORDER BY column index
    bool ascending;
    size_t k;                       // WindowTopKFusionStrategy's fused K (>= 1)
    std::string out_name;           // ROW_NUMBER output column name
    MorselBuffer* out;
    size_t chunk_rows;

    WindowTopKSink(std::vector<size_t> p, size_t oi, bool asc, size_t kk,
                   std::string name, MorselBuffer* b, size_t chunk = 131072)
        : part_idx(std::move(p)), order_idx(oi), ascending(asc), k(kk),
          out_name(std::move(name)), out(b), chunk_rows(chunk) {}

    std::unique_ptr<GlobalSinkState> make_global() override {
        return std::make_unique<WindowTopKGlobal>();
    }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override {
        return std::make_unique<WindowTopKLocal>();
    }

    SinkResult sink(const MorselPtr& in, GlobalSinkState&, LocalSinkState& ls,
                    ErrCtx& err) override {
        uint32_t rows = in->num_rows();
        if (rows == 0) return SinkResult::CONTINUE;
        auto& l = static_cast<WindowTopKLocal&>(ls);

        std::vector<uint64_t> phashes;
        if (!compute_row_hashes(in, part_idx, phashes, err)) return SinkResult::CONTINUE;

        const DrakenVector& ov = in->columns[order_idx].view;
        uint32_t mi = static_cast<uint32_t>(l.morsels.size());
        l.morsels.push_back(in);

        WindowTopKBetter better{ascending};
        for (uint32_t r = 0; r < rows; ++r) {
            bool ok = sort_row_valid(ov, r);
            uint64_t raw = ok ? sort_num_key(ov, r) : 0;
            WindowTopKCandidate c;
            c.key = ok ? (ascending ? raw : ~raw) : 0;
            c.valid = ok ? 1 : 0;
            c.morsel_idx = mi;
            c.row = r;
            uint64_t ph = phashes[r];
            window_topk_offer(l.heaps[ph >> kGBPartShift][ph], c, k, better);
        }
        return SinkResult::CONTINUE;
    }

    void combine(GlobalSinkState& gs, LocalSinkState& ls, ErrCtx&) override {
        auto& g = static_cast<WindowTopKGlobal&>(gs);
        auto& l = static_cast<WindowTopKLocal&>(ls);
        std::lock_guard<std::mutex> lk(g.mtx);
        uint32_t base = static_cast<uint32_t>(g.morsels.size());
        for (MorselPtr& m : l.morsels) g.morsels.push_back(std::move(m));
        // Queue, never merge, under the lock: O(kGBParts) moves per worker. The
        // per-candidate merge runs bucket-parallel in finalize().
        for (size_t p = 0; p < kGBParts; ++p) {
            if (!l.heaps[p].empty())
                g.pending[p].emplace_back(base, std::move(l.heaps[p]));
        }
    }

    void finalize(GlobalSinkState& gs, ErrCtx& err) override {
        auto& g = static_cast<WindowTopKGlobal&>(gs);
        if (g.morsels.empty()) return;
        // Gate on queued PARTITION entries (map.size() is O(1)), not candidates —
        // candidates are at most k per entry, so this is the same scale and avoids
        // walking millions of map nodes serially just to pick a thread count.
        size_t entry_total = 0;
        size_t nonempty_buckets = 0;
        for (size_t p = 0; p < kGBParts; ++p) {
            size_t pe = 0;
            for (const auto& [base, hm] : g.pending[p]) pe += hm.size();
            entry_total += pe;
            if (pe > 0) ++nonempty_buckets;
        }
        if (entry_total == 0) return;

        WindowTopKBetter better{ascending};
        // Merge phase: buckets are disjoint by partition-hash bits, so each bucket's
        // worker-heap merge, per-partition sort and ROW_NUMBER assignment run in
        // parallel (adaptive one-shot pool-let — same idiom as GroupBySink::finalize).
        // Each bucket fills its own slot of per-bucket row lists; the slots
        // concatenate afterwards so the emission phase below is untouched.
        std::array<std::vector<uint32_t>, kGBParts> b_row_m, b_row_r;
        std::array<std::vector<int64_t>, kGBParts> b_rn;
        {
            unsigned hw = std::thread::hardware_concurrency();
            unsigned mnt = hw > 2 ? hw - 2 : 1;
            if (mnt > 16) mnt = 16;
            if (mnt > nonempty_buckets) mnt = static_cast<unsigned>(nonempty_buckets);
            if (entry_total < 65536) mnt = 1;   // small window: inline, no threads
            if (mnt < 1) mnt = 1;
            std::atomic<size_t> next_bucket{0};
            auto merge_worker = [&](unsigned) {
                for (;;) {
                    size_t p = next_bucket.fetch_add(1);
                    if (p >= kGBParts) break;
                    auto& list = g.pending[p];
                    if (list.empty()) continue;
                    // Merge worker maps in queue order; rebase morsel refs here (not
                    // under the combine lock).
                    WindowTopKHeapMap merged = std::move(list[0].second);
                    uint32_t base0 = list[0].first;
                    if (base0 != 0) {
                        for (auto& [phash, heap] : merged)
                            for (WindowTopKCandidate& c : heap) c.morsel_idx += base0;
                    }
                    for (size_t i = 1; i < list.size(); ++i) {
                        uint32_t base = list[i].first;
                        for (auto& [phash, local_heap] : list[i].second) {
                            std::vector<WindowTopKCandidate>& heap = merged[phash];
                            for (WindowTopKCandidate c : local_heap) {
                                c.morsel_idx += base;
                                window_topk_offer(heap, c, k, better);
                            }
                        }
                    }
                    for (auto& [phash, heap] : merged) {
                        // Best-first: `a` sorts before `b` iff `a` ranks better.
                        std::sort(heap.begin(), heap.end(), better);
                        int64_t pos = 1;
                        for (const WindowTopKCandidate& c : heap) {
                            b_row_m[p].push_back(c.morsel_idx);
                            b_row_r[p].push_back(c.row);
                            b_rn[p].push_back(pos++);
                        }
                    }
                    // Free this bucket's queued worker maps HERE, on the pool-let,
                    // not at global-state destruction: at 1M partitions × dop
                    // workers that is millions of map nodes, and freeing them
                    // serially after the pipeline measured ~1.8s of invisible
                    // teardown wall.
                    list.clear();
                    list.shrink_to_fit();
                }
            };
            std::vector<std::thread> mthreads;
            mthreads.reserve(mnt > 0 ? mnt - 1 : 0);
            for (unsigned t = 1; t < mnt; ++t) mthreads.emplace_back(merge_worker, t);
            merge_worker(0);
            for (std::thread& t : mthreads) t.join();
        }
        std::vector<uint32_t> row_m, row_r;
        std::vector<int64_t> rn;
        for (size_t p = 0; p < kGBParts; ++p) {
            row_m.insert(row_m.end(), b_row_m[p].begin(), b_row_m[p].end());
            row_r.insert(row_r.end(), b_row_r[p].begin(), b_row_r[p].end());
            rn.insert(rn.end(), b_rn[p].begin(), b_rn[p].end());
        }
        size_t total = row_m.size();
        if (total == 0) return;
        std::vector<uint32_t> order(total);
        for (size_t i = 0; i < total; ++i) order[i] = static_cast<uint32_t>(i);

        // Same one-shot thread pool-let idiom as GroupBySink::finalize /
        // WindowSink::finalize — total is bounded by (#partitions * k), typically
        // small, so this is headroom for large-K/large-cardinality cases rather
        // than the primary win (that's avoiding the O(n log n) sort above).
        const std::vector<std::string>& names = g.morsels.front()->names;
        size_t num_chunks = (total + chunk_rows - 1) / chunk_rows;
        std::vector<MorselPtr> chunk_out(num_chunks);

        unsigned hw = std::thread::hardware_concurrency();
        unsigned nt = hw > 2 ? static_cast<unsigned>(hw - 2) : 1u;
        if (nt > 16) nt = 16;
        if (nt > num_chunks) nt = static_cast<unsigned>(num_chunks);
        if (total < 200000) nt = 1;
        if (nt < 1) nt = 1;

        std::vector<ErrCtx> errs(nt);
        std::atomic<size_t> next_chunk{0};
        auto worker = [&](unsigned tid) {
            for (;;) {
                size_t ci = next_chunk.fetch_add(1);
                if (ci >= num_chunks) break;
                size_t start = ci * chunk_rows;
                size_t count = std::min(chunk_rows, total - start);
                MorselPtr m = gather_rows(g.morsels, order, start, count, row_m, row_r,
                                          names, errs[tid]);
                if (errs[tid].code != 0) return;
                uint32_t cn = static_cast<uint32_t>(count);
                int64_t* data = static_cast<int64_t*>(
                    draken_malloc((cn == 0 ? 1 : cn) * sizeof(int64_t)));
                for (uint32_t j = 0; j < cn; ++j) data[j] = rn[start + j];
                uint32_t* sel = static_cast<uint32_t*>(
                    draken_malloc((cn == 0 ? 1 : cn) * sizeof(uint32_t)));
                for (uint32_t j = 0; j < cn; ++j) sel[j] = j;
                DrakenVector v;
                v.data = data; v.selection = sel; v.data_length = cn; v.length = cn;
                v.validity = nullptr; v.type = DRAKEN_INT64;
                v.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
                CxxColumn c;
                c.own = std::make_shared<VectorOwner>(
                    v, OwnedBuffer<void>(data), OwnedBuffer<uint8_t>(nullptr),
                    OwnedBuffer<void>(sel));
                c.own->logical_type = nullptr;
                c.view = c.own->vec;
                m->columns.push_back(std::move(c));
                m->names.push_back(out_name);
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
        for (MorselPtr& m : chunk_out) out->morsels.push_back(std::move(m));
    }
};

}  // namespace opteryx::engine
