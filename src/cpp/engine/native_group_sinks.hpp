#pragma once
// src/cpp/engine/native_group_sinks.hpp — the engine's general aggregation and
// dedup breakers: UngroupedAggSink (COUNT(*)/COUNT/SUM/AVG/MIN/MAX, any mix),
// GroupBySink (multi-key, string keys, NULL-key groups), DistinctSink.
//
// Semantics (SQL, not demo shortcuts):
//   - COUNT(*) counts rows; COUNT(col) counts non-NULL values.
//   - SUM over integer-family/DECIMAL operands accumulates EXACT int64 (never a
//     double round-trip); float operands accumulate double. SUM/AVG/MIN/MAX over
//     zero valid values is NULL. AVG is FLOAT64.
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
#include "native_sort.hpp"       // sort_num_key, sort_row_valid, sort_type_is_string,
                                 // string_arena_of, gather_elem_size, gather_rows,
                                 // make canonical string blocks
#include "core/string_slot.h"
#include "core/vector_owner.h"
#include "logical_type.h"        // LogicalType definition (owner only forward-declares it)
#include "xxhash.h"              // XXH3_64bits — long-slot hash32 (same as draken's builders)

namespace opteryx::engine {

// ---- aggregate spec + accumulator --------------------------------------------------

enum class AggFn : uint8_t {
    CountStar = 0, Count = 1, Sum = 2, Avg = 3, Min = 4, Max = 5,
    CountDistinct = 6,   // COUNT(DISTINCT col): dedup on serialized value bytes
    ArrayAgg = 7,        // ARRAY_AGG(col): one ARRAY per group; GROUP BY only
};

struct AggSpec2 {
    AggFn fn;
    int col_idx;        // operand column; < 0 only for CountStar
    std::string name;   // output column identity
    // ARRAY_AGG modifiers — ignored by every other fn. DISTINCT/ORDER BY/LIMIT
    // all apply at finalize, AFTER the per-partition lists are merged: a worker
    // sees an arbitrary row subset, so ordering or truncating locally would give
    // a different answer than the serial plan.
    bool    aa_distinct   = false;
    bool    aa_ordered    = false;
    bool    aa_descending = false;
    int64_t aa_limit      = -1;     // < 0 == no LIMIT
    // Hard cap on retained elements per group. Exceeding it fails loud at
    // finalize (MEDIAN's MedianState precedent) — an unbounded per-group list is
    // an OOM the query can't diagnose, and silently truncating would be a wrong
    // answer dressed as a right one.
    int64_t aa_max_per_group = 1000;
};

struct AggCell {
    // Field order matters: the SUM/AVG/COUNT hot path touches ONLY the first
    // 40 bytes (one cache line); the MIN/MAX lanes sit after so aggregations
    // without extremes never pull their lines.
    __int128 isum = 0;   // EXACT integer-domain sum (int64 family AND DECIMAL128 raws)
    double   fsum = 0.0;
    int64_t  valid = 0;                  // non-NULL operand rows
    int64_t  rows = 0;                   // ALL rows — COUNT(*)
    __int128 min128 = 0, max128 = 0;   // DECIMAL128 order extremes (raw == value order)
    uint64_t min_key = 0, max_key = 0;   // normalized order keys (sort_num_key)
    int64_t  min_raw = 0, max_raw = 0;   // raw value container (widened / bit-stored)
};

// Captured once from the first morsel a worker sees; merged into the global.
struct AggColMeta {
    DrakenType type = DRAKEN_INT64;
    const LogicalType* logical = nullptr;
    bool is_float = false;
    bool is_string = false;   // MIN/MAX over a string column (parallel string store)
    bool captured = false;
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

inline __int128 agg2_read_i128(const DrakenVector& v, uint32_t row) {
    __int128 out;
    std::memcpy(&out, static_cast<const uint8_t*>(v.data)
                          + static_cast<size_t>(v.selection[row]) * 16u, 16u);
    return out;
}

// Raw value widened into an int64 container: integer family sign-extended; floats
// stored as the DOUBLE's bit pattern (round-trips losslessly to FLOAT32/64 output).
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

inline void agg2_merge(AggCell& into, const AggCell& from) {
    into.isum += from.isum;
    into.fsum += from.fsum;
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
                            static_cast<uint32_t>(XXH3_64bits(bytes, len)),
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
                                   uint32_t n, DrakenType t, const LogicalType* logical) {
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
    size_t es = gather_elem_size(t);
    size_t alloc_n = (n == 0 ? 1 : n);
    uint8_t* data = static_cast<uint8_t*>(draken_malloc(alloc_n * (es == 0 ? 1 : es)));
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

// One group's ARRAY_AGG elements. Exactly one value lane is populated (the one
// aa_store_of picks); `nulls` is parallel to it and is the authoritative element
// count. NULLs are kept as elements — every other aggregate skips them, but
// ARRAY_AGG(col) over [1, NULL] is [1, NULL], not [1]. Null positions still push
// a placeholder into the value lane so the two stay index-aligned.
struct GBArrayAggState {
    std::vector<int64_t>     raws;
    std::vector<__int128>    i128s;
    std::vector<std::string> strs;
    std::vector<uint8_t>     nulls;   // 1 == element is NULL
    bool overflowed = false;          // hit aa_max_per_group; raised at finalize

    size_t size() const noexcept { return nulls.size(); }

    // Append one element. Returns false once the cap is hit (and latches
    // `overflowed`) so callers stop copying bytes into a doomed group.
    inline bool push(AAStore st, bool is_null, int64_t raw, __int128 big,
                     const char* sp, uint32_t slen, int64_t cap) noexcept {
        if (static_cast<int64_t>(nulls.size()) >= cap) { overflowed = true; return false; }
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
};

// COUNT(DISTINCT) value fits the typed (u32 entry, u64 widened value) pair
// tables — everything fixed-width up to 8 bytes; strings and DECIMAL128 take
// the serialized-bytes tables.
inline bool gb_cd_fixed(DrakenType t) {
    if (sort_type_is_string(t) || t == DRAKEN_DECIMAL128) return false;
    if (t == DRAKEN_BOOL) return true;
    size_t es = gather_elem_size(t);
    return es > 0 && es <= 8;
}

inline GBKind gb_kind_of(const AggSpec2& sp, const AggColMeta& m) {
    switch (sp.fn) {
        case AggFn::CountStar:     return GBKind::Rows;
        case AggFn::Count:         return GBKind::Valid;
        case AggFn::CountDistinct: return GBKind::CountDistinct;
        case AggFn::ArrayAgg:      return GBKind::ArrayAgg;
        case AggFn::Sum:
            if (m.type == DRAKEN_DECIMAL128) return GBKind::SumD128;
            return m.is_float ? GBKind::SumF : GBKind::SumI;
        case AggFn::Avg:
            if (m.type == DRAKEN_DECIMAL128) return GBKind::AvgD128;
            return m.is_float ? GBKind::AvgF : GBKind::AvgI;
        case AggFn::Min:
        case AggFn::Max:
            if (m.is_string) return GBKind::MinMaxStr;
            if (m.type == DRAKEN_DECIMAL128) return GBKind::MinMaxD128;
            return GBKind::MinMaxNum;
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
    const double*      f64   = nullptr;   // float sums
    const __int128*    i128  = nullptr;   // DECIMAL128 sums / extremes
    const std::string* sval  = nullptr;   // string extremes
    GBArrayAggState*   aa    = nullptr;   // ARRAY_AGG element lists
    const AggSpec2*    aa_spec = nullptr; // ARRAY_AGG DISTINCT/ORDER BY/LIMIT modifiers
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
        err.msg = "ARRAY_AGG: a group exceeded the per-group element cap — fail loud, "
                  "never a silently truncated list. Narrow the group, add a LIMIT to "
                  "the aggregate, or raise the cap.";
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
                                          meta.type, meta.logical);
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
            return emit_fixed_column(L.rows, nullptr, n, DRAKEN_INT64, nullptr);
        case GBKind::Valid:
        case GBKind::CountDistinct:
            // COUNT(col) / COUNT(DISTINCT col) over zero valid rows is 0, not NULL.
            return emit_fixed_column(L.valid, nullptr, n, DRAKEN_INT64, nullptr);
        case GBKind::SumI: {
            // exact integer sums (overflow already failed loud at accumulate);
            // DECIMAL keeps its type + descriptor.
            DrakenType t = (meta.type == DRAKEN_DECIMAL) ? DRAKEN_DECIMAL : DRAKEN_INT64;
            return emit_fixed_column(L.i64, valid_ok(), n, t,
                                     (meta.type == DRAKEN_DECIMAL) ? meta.logical
                                                                   : nullptr);
        }
        case GBKind::SumF: {
            std::vector<int64_t> raws(n, 0);
            for (uint32_t i = 0; i < n; ++i)
                std::memcpy(&raws[i], &L.f64[i], sizeof(double));
            return emit_fixed_column(raws.data(), valid_ok(), n, DRAKEN_FLOAT64,
                                     nullptr);
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
            return emit_fixed_column(raws.data(), okp, n, DRAKEN_FLOAT64, nullptr);
        }
        case GBKind::MinMaxNum:
            return emit_fixed_column(L.i64, valid_ok(), n, meta.type, meta.logical);
        case GBKind::MinMaxStr:
            return emit_string_lane_column(meta, L.sval, L.valid, n);
        case GBKind::ArrayAgg:
            return emit_array_lane_column(meta, *L.aa_spec, L.aa, n, err);
    }
    return CxxColumn{};   // unreachable
}

// ---- serialized row keys (GROUP BY keys / DISTINCT rows) ---------------------------
// Per column: 1 null byte; valid fixed-width appends its raw native bytes (bitwise
// equality — draken canonicalizes -0.0/NaN at storage); valid string appends
// u32 length + bytes. BOOL appends one byte.

inline bool key_append(std::string& buf, const DrakenVector& v, uint32_t row,
                       ErrCtx& err) {
    if (!sort_row_valid(v, row)) {
        buf.push_back('\0');
        return true;
    }
    buf.push_back('\1');
    if (sort_type_is_string(v.type)) {
        const DrakenStringArena* sa = string_arena_of(v);
        const DrakenStringSlot* slot = &sa->slots[v.selection[row]];
        uint32_t len = str_length(slot);
        buf.append(reinterpret_cast<const char*>(&len), sizeof(len));
        if (len > 0)
            buf.append(reinterpret_cast<const char*>(str_data(slot, sa->arena)), len);
        return true;
    }
    if (v.type == DRAKEN_BOOL) {
        uint32_t phys = v.selection[row];
        buf.push_back(static_cast<char>(
            (static_cast<const uint8_t*>(v.data)[phys >> 3] >> (phys & 7)) & 1u));
        return true;
    }
    size_t es = gather_elem_size(v.type);
    if (es == 0) {
        err.code = 1;
        err.msg = "native engine: unsupported GROUP BY/DISTINCT key column type — "
                  "fail loud, never a silent wrong grouping";
        return false;
    }
    buf.append(reinterpret_cast<const char*>(static_cast<const uint8_t*>(v.data))
                   + static_cast<size_t>(v.selection[row]) * es,
               es);
    return true;
}

// key_append for a PHYSICAL value index: byte-identical to key_append's valid
// arm ('\1' + payload), but addressed by data slot instead of logical row —
// the §11-sanctioned dict-shape fast paths serialize each UNIQUE value once
// and must land in the same dedup universe as the per-row path. Caller has
// already established the value is referenced by at least one valid row.
inline bool key_append_phys(std::string& buf, const DrakenVector& v, uint32_t phys,
                            ErrCtx& err) {
    buf.push_back('\1');
    if (sort_type_is_string(v.type)) {
        const DrakenStringArena* sa = string_arena_of(v);
        const DrakenStringSlot* slot = &sa->slots[phys];
        uint32_t len = str_length(slot);
        buf.append(reinterpret_cast<const char*>(&len), sizeof(len));
        if (len > 0)
            buf.append(reinterpret_cast<const char*>(str_data(slot, sa->arena)), len);
        return true;
    }
    if (v.type == DRAKEN_BOOL) {
        buf.push_back(static_cast<char>(
            (static_cast<const uint8_t*>(v.data)[phys >> 3] >> (phys & 7)) & 1u));
        return true;
    }
    size_t es = gather_elem_size(v.type);
    if (es == 0) {
        err.code = 1;
        err.msg = "native engine: unsupported GROUP BY/DISTINCT key column type — "
                  "fail loud, never a silent wrong grouping";
        return false;
    }
    buf.append(reinterpret_cast<const char*>(static_cast<const uint8_t*>(v.data))
                   + static_cast<size_t>(phys) * es,
               es);
    return true;
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

// Lightweight view of a serialized group key (bytes may live in a per-partition
// arena rather than an owning std::string).
struct GBKeyRef {
    const char* d;
    size_t n;
};

// `null_rows_carry_bytes`: fixed-stride packed keys (v3 all-fixed-width mode)
// write zeroed value bytes after a '\0' null marker so every key is the same
// length — the parser must skip them. The variable (string-key) format writes
// nothing after '\0'.
inline void emit_key_columns(const std::vector<GBKeyRef>& keys,
                             const std::vector<KeyColMeta>& meta,
                             bool null_rows_carry_bytes,
                             CxxMorsel& out, ErrCtx& err) {
    uint32_t n = static_cast<uint32_t>(keys.size());
    size_t ncols = meta.size();
    // Per-group cursor into its serialized buffer, advanced column by column.
    std::vector<size_t> pos(n, 0);
    for (size_t kc = 0; kc < ncols; ++kc) {
        DrakenType t = meta[kc].type;
        if (sort_type_is_string(t)) {
            // Pass 1: arena size + slot count. Pass 2: fill canonical block.
            size_t total_arena = 0;
            std::vector<size_t> starts(n);
            for (uint32_t g = 0; g < n; ++g) {
                const GBKeyRef& b = *(&keys[g]);
                starts[g] = pos[g];
                if (b.d[pos[g]++] == '\0') continue;
                uint32_t len;
                std::memcpy(&len, b.d + pos[g], sizeof(len));
                pos[g] += sizeof(len) + len;
                if (len > STR_INLINE_MAX) total_arena += len;   // long-form slots only
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
            sa->null_bitmap = nullptr; sa->owns_buffers = 0; sa->type = t;
            size_t vbytes = (static_cast<size_t>(n) + 7) / 8;
            uint8_t* vbits = nullptr;
            size_t arena_pos = 0;
            for (uint32_t g = 0; g < n; ++g) {
                const GBKeyRef& b = *(&keys[g]);
                size_t q = starts[g];
                if (b.d[q++] == '\0') {
                    std::memset(&dst[g], 0, sizeof(DrakenStringSlot));
                    if (vbits == nullptr) {
                        vbits = static_cast<uint8_t*>(draken_malloc(vbytes == 0 ? 1 : vbytes));
                        std::memset(vbits, 0xFF, vbytes == 0 ? 1 : vbytes);
                    }
                    vbits[g >> 3] &= static_cast<uint8_t>(~(1u << (g & 7)));
                    continue;
                }
                uint32_t len;
                std::memcpy(&len, b.d + q, sizeof(len));
                q += sizeof(len);
                const uint8_t* bytes = reinterpret_cast<const uint8_t*>(b.d + q);
                if (len <= STR_INLINE_MAX) {
                    str_init_inline(&dst[g], bytes, len);
                } else {
                    std::memcpy(out_arena + arena_pos, bytes, len);
                    str_init_extern(&dst[g], bytes, len,
                                    static_cast<uint32_t>(XXH3_64bits(bytes, len)),
                                    static_cast<uint32_t>(arena_pos));
                    arena_pos += len;
                }
            }
            uint32_t* sel = static_cast<uint32_t*>(
                draken_malloc((n == 0 ? 1 : n) * sizeof(uint32_t)));
            for (uint32_t i = 0; i < n; ++i) sel[i] = i;
            DrakenVector v;
            v.data = sa; v.selection = sel; v.data_length = n; v.length = n;
            v.validity = vbits; v.type = t;
            v.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
            CxxColumn c;
            c.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(blk),
                                                  OwnedBuffer<uint8_t>(vbits),
                                                  OwnedBuffer<void>(sel));
            c.own->logical_type = meta[kc].logical;
            c.view = c.own->vec;
            out.columns.push_back(std::move(c));
            continue;
        }
        size_t es = (t == DRAKEN_BOOL) ? 1 : gather_elem_size(t);
        if (es == 0) {
            err.code = 1;
            err.msg = "native engine: unsupported key column type at materialization";
            return;
        }
        std::vector<int64_t> raws(n, 0);
        std::vector<uint8_t> ok(n, 1);
        for (uint32_t g = 0; g < n; ++g) {
            const GBKeyRef& b = *(&keys[g]);
            if (b.d[pos[g]++] == '\0') {
                ok[g] = 0;
                if (null_rows_carry_bytes) pos[g] += es;
                continue;
            }
            int64_t raw = 0;
            std::memcpy(&raw, b.d + pos[g], es);
            pos[g] += es;
            if (t == DRAKEN_FLOAT32) {
                // stored as raw 4-byte float; widen to the double-bit container
                float f;
                std::memcpy(&f, &raw, sizeof(f));
                double d = static_cast<double>(f);
                std::memcpy(&raw, &d, sizeof(d));
            } else if (t != DRAKEN_FLOAT64 && es < 8) {
                // sign-extend narrow integers stored as raw bytes
                int shift = static_cast<int>((8 - es) * 8);
                raw = (raw << shift) >> shift;
            }
            raws[g] = raw;
        }
        out.columns.push_back(emit_fixed_column(raws.data(), ok.data(), n, t,
                                                meta[kc].logical));
    }
}

// ---- shared partitioned-dedup machinery (GroupBy + ungrouped COUNT DISTINCT) -------

constexpr size_t kGBParts = 64;
constexpr int kGBPartShift = 58;   // top 6 bits pick the partition
// High-NDV adaptive flush: when a worker's TOTAL local group count passes this,
// its partitions are queued for the (parallel) merge and reset. Keeps the
// sink-side probe working set cache-resident on 90M-group aggregations —
// probes into an 11MB+ local table were the profiled Q33 wall. Low-cardinality
// aggregations never reach the cap: zero behavior change. Measured curve
// (Q33, 90M groups): 262144 → 2.21s, 131072 → 1.66s, 65536 → 1.62s,
// 32768 → 1.61s; 65536 picked (flat below it, fewer merge chunks).
constexpr size_t kGBFlushEntries = 65536;
constexpr size_t kGBArenaChunk = 1u << 20;   // 1 MiB key-arena chunks (string-key mode)

// Per-(spec, partition) dedup table for COUNT(DISTINCT): keys are
// [4-byte group entry || serialized value bytes], full byte identity (stored
// hash + memcmp — never hash-only). pair_ent/val slices are kept so the merge
// can RE-KEY every pair against the merged partition's group entries.
struct GBDedup {
    std::vector<uint32_t> table;      // open addressing; value = pair index + 1
    std::vector<uint64_t> hashes;
    std::vector<uint32_t> pair_ent;   // group entry per pair (pre-merge numbering)
    std::vector<uint64_t> key_off;    // (chunk << 20) | offset — full key bytes
    std::vector<uint32_t> key_len;
    std::vector<std::unique_ptr<char[]>> arena_chunks;
    size_t arena_used = kGBArenaChunk;

    size_t size() const { return hashes.size(); }
    const char* key_ptr(uint32_t e) const {
        uint64_t off = key_off[e];
        return arena_chunks[off >> 20].get() + (off & (kGBArenaChunk - 1));
    }
    void rehash(size_t want) {
        size_t cap = 16;
        while (cap < want * 2) cap <<= 1;
        table.assign(cap, 0);
        size_t mask = cap - 1;
        for (uint32_t e = 0; e < hashes.size(); ++e) {
            size_t slot = hashes[e] & mask;
            while (table[slot] != 0) slot = (slot + 1) & mask;
            table[slot] = e + 1;
        }
    }
    // Insert (ent, key bytes); returns true iff the pair is NEW.
    bool upsert(uint64_t h, uint32_t ent, const char* key, uint32_t klen) {
        if (table.empty() || hashes.size() * 2 >= table.size())
            rehash(hashes.size() + 8);
        size_t mask = table.size() - 1;
        size_t slot = h & mask;
        while (true) {
            uint32_t e1 = table[slot];
            if (e1 == 0) break;
            uint32_t e = e1 - 1;
            if (hashes[e] == h && key_len[e] == klen
                    && std::memcmp(key_ptr(e), key, klen) == 0)
                return false;
            slot = (slot + 1) & mask;
        }
        if (klen >= kGBArenaChunk) {
            arena_chunks.push_back(std::unique_ptr<char[]>(new char[klen]));
            std::memcpy(arena_chunks.back().get(), key, klen);
            key_off.push_back(static_cast<uint64_t>(arena_chunks.size() - 1) << 20);
            arena_used = kGBArenaChunk;
        } else {
            if (arena_used + klen > kGBArenaChunk) {
                arena_chunks.push_back(std::unique_ptr<char[]>(new char[kGBArenaChunk]));
                arena_used = 0;
            }
            std::memcpy(arena_chunks.back().get() + arena_used, key, klen);
            key_off.push_back(
                (static_cast<uint64_t>(arena_chunks.size() - 1) << 20) | arena_used);
            arena_used += klen;
        }
        table[slot] = static_cast<uint32_t>(hashes.size()) + 1;
        hashes.push_back(h);
        pair_ent.push_back(ent);
        key_len.push_back(klen);
        return true;
    }
};

// Fixed-width COUNT(DISTINCT) pair dedup: (u32 group entry, u64 widened value)
// in typed lanes — no serialization, no arena, exact integer identity. The
// value is agg2_read_raw's widened container (raw-byte equality is the dedup
// contract; draken canonicalizes -0.0/NaN at storage).
struct GBDedupF {
    std::vector<uint32_t> table;      // open addressing; value = pair index + 1
    std::vector<uint64_t> hashes;     // stored — no later phase re-hashes
    std::vector<uint32_t> pent;       // group entry per pair
    std::vector<uint64_t> pval;       // widened value per pair

    size_t size() const { return hashes.size(); }
    void rehash(size_t want) {
        size_t cap = 16;
        while (cap < want * 2) cap <<= 1;
        table.assign(cap, 0);
        size_t mask = cap - 1;
        for (uint32_t e = 0; e < hashes.size(); ++e) {
            size_t slot = hashes[e] & mask;
            while (table[slot] != 0) slot = (slot + 1) & mask;
            table[slot] = e + 1;
        }
    }
    static uint64_t hash_pair(uint32_t ent, uint64_t val) {
        struct { uint32_t e; uint64_t v; } kb;
        kb.e = ent; kb.v = val;
        return XXH3_64bits(&kb, sizeof(kb));
    }
    // Insert (ent, val); returns true iff the pair is NEW.
    bool upsert(uint64_t h, uint32_t ent, uint64_t val) {
        if (table.empty() || hashes.size() * 2 >= table.size())
            rehash(hashes.size() + 8);
        size_t mask = table.size() - 1;
        size_t slot = h & mask;
        while (true) {
            uint32_t e1 = table[slot];
            if (e1 == 0) break;
            uint32_t e = e1 - 1;
            if (hashes[e] == h && pent[e] == ent && pval[e] == val) return false;
            slot = (slot + 1) & mask;
        }
        table[slot] = static_cast<uint32_t>(hashes.size()) + 1;
        hashes.push_back(h);
        pent.push_back(ent);
        pval.push_back(val);
        return true;
    }
};

// ---- UngroupedAggSink ---------------------------------------------------------------

struct UngroupedAggLocal : LocalSinkState {
    std::vector<AggCell> cells;
    std::vector<std::string> strs;   // string MIN/MAX extremes, parallel to cells
    // COUNT(DISTINCT): per-spec hash-PARTITIONED flat dedup tables (stored hash
    // + arena + memcmp — profiled: unordered_set<string> was the Q05 wall via
    // per-node malloc, string hashing, and a ~1s destructor). Fixed-width
    // values take the typed GBDedupF twin (no serialization, entry always 0).
    std::vector<std::array<GBDedup, kGBParts>> dparts;
    std::vector<std::array<GBDedupF, kGBParts>> dfparts;
    std::vector<AggColMeta> meta;
    bool init = false;
};
struct UngroupedAggGlobal : GlobalSinkState {
    std::mutex mtx;
    std::vector<AggCell> cells;
    std::vector<std::string> strs;
    // per spec, per partition: queued worker tables (disjoint by hash — merged
    // AND counted in parallel at finalize, mirroring GroupBySink).
    std::vector<std::array<std::vector<GBDedup>, kGBParts>> dpending;
    std::vector<std::array<std::vector<GBDedupF>, kGBParts>> dfpending;
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
            if (specs[s].col_idx < 0) continue;   // CountStar
            if (static_cast<size_t>(specs[s].col_idx) >= in->columns.size()) {
                err.code = 1;
                err.msg = "native engine: aggregate operand column missing from "
                          "input morsel — fail loud, never a silent wrong answer";
                return false;
            }
            const CxxColumn& c = in->columns[static_cast<size_t>(specs[s].col_idx)];
            DrakenType t = c.view.type;
            bool str_minmax = sort_type_is_string(t)
                && (specs[s].fn == AggFn::Min || specs[s].fn == AggFn::Max);
            // ARRAY_AGG is grouped-only (the binder rejects it without a GROUP BY,
            // and the compiler again at plan time). Reaching the ungrouped sink means
            // one of those gates broke — say so rather than read a lane that the
            // ungrouped AggCell has no room for.
            if (specs[s].fn == AggFn::ArrayAgg) {
                err.code = 1;
                err.msg = "native engine: ARRAY_AGG without a GROUP BY reached the "
                          "ungrouped aggregate sink — fail loud, never a silent wrong "
                          "answer";
                return false;
            }
            // COUNT only reads validity — any column type is countable.
            // COUNT(DISTINCT) reads serialized value bytes (key_append fails
            // loud on unsupported types at run time).
            if (specs[s].fn != AggFn::Count && specs[s].fn != AggFn::CountDistinct
                    && !str_minmax
                    && !agg2_operand_supported(t)) {
                err.code = 1;
                err.msg = "native engine: unsupported aggregate operand type — fail "
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
            l.dparts.resize(specs.size());
            l.dfparts.resize(specs.size());
            l.init = true;
        }
        uint32_t rows = in->num_rows();
        for (size_t s = 0; s < specs.size(); ++s) {
            AggCell& c = l.cells[s];
            c.rows += rows;
            if (specs[s].col_idx < 0) continue;
            const DrakenVector& v = in->columns[static_cast<size_t>(specs[s].col_idx)].view;
            if (specs[s].fn == AggFn::Count) {
                for (uint32_t i = 0; i < v.length; ++i) {
                    if (sort_row_valid(v, i)) c.valid += 1;
                }
            } else if (specs[s].fn == AggFn::CountDistinct
                       && gb_cd_fixed(l.meta[s].type)) {
                // typed value path: u64 widened values, no serialization.
                std::array<GBDedupF, kGBParts>& DF = l.dfparts[s];
                bool is_f = l.meta[s].is_float;
                for (uint32_t i = 0; i < v.length; ++i) {
                    if (!sort_row_valid(v, i)) continue;
                    uint64_t val = static_cast<uint64_t>(agg2_read_raw(v, i, is_f));
                    uint64_t dh = GBDedupF::hash_pair(0, val);
                    DF[dh >> kGBPartShift].upsert(dh, 0, val);
                }
            } else if (specs[s].fn == AggFn::CountDistinct) {
                // dedup on serialized value bytes; the count is the union of
                // the partitioned tables, taken at finalize (c.valid unused).
                std::string dkey;
                std::array<GBDedup, kGBParts>& DP = l.dparts[s];
                if (v.data_length < v.length) {
                    // dict/constant shape (§11, architect-approved): serialize +
                    // insert each REFERENCED unique value once — rows sharing a
                    // dict code are known equal without hashing. Bytes are
                    // identical to the per-row path (key_append_phys), so the
                    // dedup universe is exact across shapes and morsels.
                    std::vector<uint8_t> ref;
                    mark_referenced_valid(v, ref);
                    for (uint32_t j = 0; j < v.data_length; ++j) {
                        if (ref[j] == 0) continue;
                        dkey.clear();
                        if (!key_append_phys(dkey, v, j, err))
                            return SinkResult::CONTINUE;
                        uint64_t dh = XXH3_64bits(dkey.data(), dkey.size());
                        DP[dh >> kGBPartShift].upsert(
                            dh, 0, dkey.data(), static_cast<uint32_t>(dkey.size()));
                    }
                } else {
                    for (uint32_t i = 0; i < v.length; ++i) {
                        if (!sort_row_valid(v, i)) continue;
                        dkey.clear();
                        if (!key_append(dkey, v, i, err)) return SinkResult::CONTINUE;
                        uint64_t dh = XXH3_64bits(dkey.data(), dkey.size());
                        DP[dh >> kGBPartShift].upsert(
                            dh, 0, dkey.data(), static_cast<uint32_t>(dkey.size()));
                    }
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
            } else {
                bool is_f = l.meta[s].is_float;
                bool nm = specs[s].fn == AggFn::Min || specs[s].fn == AggFn::Max;
                for (uint32_t i = 0; i < v.length; ++i) {
                    if (sort_row_valid(v, i)) agg2_update(c, v, i, is_f, nm);
                }
            }
        }
        return SinkResult::CONTINUE;
    }
    void combine(GlobalSinkState& gs, LocalSinkState& ls, ErrCtx&) override {
        auto& g = static_cast<UngroupedAggGlobal&>(gs);
        auto& l = static_cast<UngroupedAggLocal&>(ls);
        std::lock_guard<std::mutex> lk(g.mtx);
        if (!g.init) {
            g.cells.assign(specs.size(), AggCell{});
            g.strs.assign(specs.size(), std::string());
            g.dpending.resize(specs.size());
            g.dfpending.resize(specs.size());
            g.meta.resize(specs.size());
            g.init = true;
        }
        if (l.init) {
            for (size_t s = 0; s < specs.size(); ++s) {
                if (l.meta[s].is_string)   // BEFORE agg2_merge (reads pre-merge valid)
                    agg2_merge_str(g.cells[s], l.cells[s], g.strs[s], l.strs[s],
                                   specs[s].fn == AggFn::Max);
                agg2_merge(g.cells[s], l.cells[s]);
                if (specs[s].fn == AggFn::CountDistinct) {
                    for (size_t part = 0; part < kGBParts; ++part) {
                        if (l.dparts[s][part].size() > 0)
                            g.dpending[s][part].push_back(
                                std::move(l.dparts[s][part]));
                        if (l.dfparts[s][part].size() > 0)
                            g.dfpending[s][part].push_back(
                                std::move(l.dfparts[s][part]));
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
            g.dpending.resize(specs.size());
            g.dfpending.resize(specs.size());
            g.meta.resize(specs.size());
        }
        // COUNT(DISTINCT): union each spec's queued worker tables. Partitions
        // are disjoint by hash, so (spec, partition) cells union AND count in
        // parallel (one-shot pool-let, same pattern as GroupBySink::finalize).
        std::vector<std::atomic<int64_t>> dcounts(specs.size());
        for (auto& dc : dcounts) dc.store(0);
        {
            // (spec, partition, fixed?) — byte tables and typed tables union
            // through the same pool-let.
            std::vector<std::tuple<size_t, size_t, bool>> items;
            size_t queued = 0;
            for (size_t s = 0; s < specs.size(); ++s) {
                if (specs[s].fn != AggFn::CountDistinct) continue;
                for (size_t part = 0; part < kGBParts; ++part) {
                    if (!g.dpending[s][part].empty()) {
                        items.emplace_back(s, part, false);
                        for (const GBDedup& d : g.dpending[s][part])
                            queued += d.size();
                    }
                    if (!g.dfpending[s][part].empty()) {
                        items.emplace_back(s, part, true);
                        for (const GBDedupF& d : g.dfpending[s][part])
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
                        size_t sp = std::get<0>(items[it]);
                        size_t part = std::get<1>(items[it]);
                        int64_t cnt = 0;
                        if (std::get<2>(items[it])) {
                            auto& list = g.dfpending[sp][part];
                            GBDedupF merged = std::move(list[0]);
                            cnt = static_cast<int64_t>(merged.size());
                            for (size_t i = 1; i < list.size(); ++i) {
                                GBDedupF& src = list[i];
                                for (uint32_t e = 0;
                                     e < static_cast<uint32_t>(src.size()); ++e) {
                                    if (merged.upsert(src.hashes[e], 0,
                                                      src.pval[e]))
                                        cnt += 1;
                                }
                                src = GBDedupF();
                            }
                        } else {
                            auto& list = g.dpending[sp][part];
                            GBDedup merged = std::move(list[0]);
                            cnt = static_cast<int64_t>(merged.size());
                            for (size_t i = 1; i < list.size(); ++i) {
                                GBDedup& src = list[i];
                                for (uint32_t e = 0;
                                     e < static_cast<uint32_t>(src.size()); ++e) {
                                    if (merged.upsert(src.hashes[e], 0,
                                                      src.key_ptr(e),
                                                      src.key_len[e]))
                                        cnt += 1;
                                }
                                src = GBDedup();
                            }
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
                    i641 = (specs[s].fn == AggFn::Min) ? c.min_raw : c.max_raw;
                    break;
                case GBKind::AvgI:   // averages divide from the exact int128 sum
                case GBKind::SumD128:
                case GBKind::AvgD128:
                    i1281 = c.isum;
                    break;
                case GBKind::MinMaxD128:
                    i1281 = (specs[s].fn == AggFn::Min) ? c.min128 : c.max128;
                    break;
                default:
                    break;
            }
            GBLaneView lv;
            lv.rows = &rows1; lv.valid = &valid1; lv.i64 = &i641;
            lv.f64 = &f641; lv.i128 = &i1281; lv.sval = &g.strs[s];
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
    std::vector<double>   f64;     // SumF/AvgF sums
    std::vector<uint64_t> mkey;    // MinMaxNum normalized order keys (sort_num_key)
    std::vector<__int128> i128;    // SumD128/AvgD128 sums; MinMaxD128 extremes
    std::vector<std::string> sval; // MinMaxStr extremes
    std::vector<GBArrayAggState> aa;  // ArrayAgg per-group element lists
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
    }
}

struct GBPartition {
    std::vector<uint32_t> table;      // open addressing; value = entry index + 1; 0 empty
    std::vector<uint64_t> hashes;     // per entry: full 64-bit key hash (never re-hash)
    // fixed-key mode: packed keys at entry * kstride
    std::vector<char> kfix;
    // int-key mode (fixed keys, RAW width <= 16): the key's raw value bytes
    // (no per-column null framing) packed into an __int128, upper bytes zero.
    // Null-ness is carried out of band in knull (bit k = column k is NULL) so a
    // 16-raw-byte key like (i64,i64) still fits. Identity is a 128-bit compare
    // plus a 1-byte mask compare — no memcmp, no variable-length byte append.
    std::vector<__int128> kint;
    std::vector<uint8_t>   knull;
    // string-key mode: bump arenas + per-entry (chunk << 20 | offset, length)
    std::vector<uint64_t> key_off;
    std::vector<uint32_t> key_len;
    std::vector<std::unique_ptr<char[]>> arena_chunks;
    size_t arena_used = kGBArenaChunk;            // forces first-chunk alloc
    std::vector<int64_t> grows;       // COUNT(*) rows lane (any Rows spec)
    std::vector<GBLanes> lanes;       // one per spec
    std::vector<GBDedup> dedup;       // CountDistinct over string values
    std::vector<GBDedupF> dedupf;     // CountDistinct over fixed-width values

    size_t size() const { return hashes.size(); }

    const char* key_ptr_var(uint32_t e) const {
        uint64_t off = key_off[e];
        return arena_chunks[off >> 20].get() + (off & (kGBArenaChunk - 1));
    }

    void rehash(size_t want) {
        size_t cap = 16;
        while (cap < want * 2) cap <<= 1;
        table.assign(cap, 0);
        size_t mask = cap - 1;
        for (uint32_t e = 0; e < hashes.size(); ++e) {
            size_t slot = hashes[e] & mask;
            while (table[slot] != 0) slot = (slot + 1) & mask;
            table[slot] = e + 1;
        }
    }

    // Find-or-insert a constant-stride packed key; returns the entry index.
    uint32_t upsert_fixed(uint64_t h, const char* key, size_t kstride) {
        if (table.empty() || hashes.size() * 2 >= table.size())
            rehash(hashes.size() + 8);   // rehash() sizes to >= 2x want (pow2)
        size_t mask = table.size() - 1;
        size_t slot = h & mask;
        while (true) {
            uint32_t e1 = table[slot];
            if (e1 == 0) break;
            uint32_t e = e1 - 1;
            if (hashes[e] == h
                    && std::memcmp(kfix.data() + static_cast<size_t>(e) * kstride,
                                   key, kstride) == 0)
                return e;
            slot = (slot + 1) & mask;
        }
        uint32_t e = static_cast<uint32_t>(hashes.size());
        hashes.push_back(h);
        kfix.insert(kfix.end(), key, key + kstride);
        table[slot] = e + 1;
        return e;
    }

    // Find-or-insert an integer-packed key (int-key mode; kstride <= 16 bytes
    // memcpy'd into an __int128, upper bytes zero). The packed integer IS the
    // identity, so a single 128-bit compare replaces the per-row memcmp into the
    // byte-key store AND the variable-length kfix append (profiled together at
    // ~59% of sink time on high-cardinality Q33-class groupings). Hash is
    // unchanged (XXH3 over the same kstride bytes) so partition/slot placement
    // is bit-identical to the byte path.
    uint32_t upsert_int(uint64_t h, __int128 ikey, uint8_t nmask) {
        if (table.empty() || hashes.size() * 2 >= table.size())
            rehash(hashes.size() + 8);
        size_t mask = table.size() - 1;
        size_t slot = h & mask;
        while (true) {
            uint32_t e1 = table[slot];
            if (e1 == 0) break;
            uint32_t e = e1 - 1;
            if (kint[e] == ikey && knull[e] == nmask) return e;
            slot = (slot + 1) & mask;
        }
        uint32_t e = static_cast<uint32_t>(hashes.size());
        hashes.push_back(h);
        kint.push_back(ikey);
        knull.push_back(nmask);
        table[slot] = e + 1;
        return e;
    }

    // Find-or-insert a variable-length key (string-key mode; v2 arena format).
    uint32_t upsert_var(uint64_t h, const char* key, uint32_t klen) {
        if (table.empty() || hashes.size() * 2 >= table.size())
            rehash(hashes.size() + 8);
        size_t mask = table.size() - 1;
        size_t slot = h & mask;
        while (true) {
            uint32_t e1 = table[slot];
            if (e1 == 0) break;
            uint32_t e = e1 - 1;
            if (hashes[e] == h && key_len[e] == klen
                    && std::memcmp(key_ptr_var(e), key, klen) == 0)
                return e;
            slot = (slot + 1) & mask;
        }
        uint32_t e = static_cast<uint32_t>(hashes.size());
        // key bytes: bump-allocate; a key never spans chunks (pad to next chunk).
        // A key >= 1 MiB gets its own exactly-sized chunk (within-offset 0).
        if (klen >= kGBArenaChunk) {
            arena_chunks.push_back(std::unique_ptr<char[]>(new char[klen]));
            std::memcpy(arena_chunks.back().get(), key, klen);
            key_off.push_back(static_cast<uint64_t>(arena_chunks.size() - 1) << 20);
            arena_used = kGBArenaChunk;   // force a fresh chunk for the next key
        } else {
            if (arena_used + klen > kGBArenaChunk) {
                arena_chunks.push_back(std::unique_ptr<char[]>(new char[kGBArenaChunk]));
                arena_used = 0;
            }
            std::memcpy(arena_chunks.back().get() + arena_used, key, klen);
            key_off.push_back(
                (static_cast<uint64_t>(arena_chunks.size() - 1) << 20) | arena_used);
            arena_used += klen;
        }
        hashes.push_back(h);
        key_len.push_back(klen);
        table[slot] = e + 1;
        return e;
    }
};

// One columnar pass: pack a fixed-width key column into the constant-stride key
// buffer (1 null byte + `es` raw bytes; NULL rows write zeros so every packed
// key is exactly kstride bytes and hash/memcmp stay well-defined).
inline void gb_pack_fixed_key(const DrakenVector& v, char* base, size_t kstride,
                              size_t col_off, uint32_t rows) {
    size_t es = (v.type == DRAKEN_BOOL) ? 1 : gather_elem_size(v.type);
    for (uint32_t i = 0; i < rows; ++i) {
        char* p = base + static_cast<size_t>(i) * kstride + col_off;
        if (!sort_row_valid(v, i)) {
            std::memset(p, 0, 1 + es);
            continue;
        }
        p[0] = '\1';
        if (v.type == DRAKEN_BOOL) {
            uint32_t phys = v.selection[i];
            p[1] = static_cast<char>(
                (static_cast<const uint8_t*>(v.data)[phys >> 3] >> (phys & 7)) & 1u);
        } else {
            std::memcpy(p + 1,
                        static_cast<const uint8_t*>(v.data)
                            + static_cast<size_t>(v.selection[i]) * es,
                        es);
        }
    }
}

// Pack ONE PHYSICAL unique value into a single-column fixed key — byte-identical
// to gb_pack_fixed_key's per-row valid arm, addressed by data slot instead of
// logical row, so the compressed-shape fast path lands in the same key universe
// as the per-row path. `p` has 1 + es bytes. NULL is handled by the caller
// (memset 0 over 1 + es, matching gb_pack_fixed_key's invalid arm).
inline void gb_pack_fixed_phys(const DrakenVector& v, char* p, size_t es,
                               uint32_t phys) {
    p[0] = '\1';
    if (v.type == DRAKEN_BOOL) {
        p[1] = static_cast<char>(
            (static_cast<const uint8_t*>(v.data)[phys >> 3] >> (phys & 7)) & 1u);
    } else {
        std::memcpy(p + 1,
                    static_cast<const uint8_t*>(v.data)
                        + static_cast<size_t>(phys) * es,
                    es);
    }
}

// ---- int-key mode packing (raw value bytes, no null framing) -----------------------
// One columnar pass for the int-key path: write column k's RAW value bytes at
// raw offset `roff` into each row's kraw-strided slot, and set bit k of that
// row's null mask when the row is NULL (its value bytes stay zero). No per-column
// null byte — the mask lives out of band (mk_null / knull), so a 16-raw-byte key
// like (i64,i64) fits an __int128 exactly.
inline void gb_pack_raw_key(const DrakenVector& v, char* base, size_t kraw,
                            size_t roff, size_t kbit, uint32_t rows,
                            uint8_t* nmask) {
    size_t es = (v.type == DRAKEN_BOOL) ? 1 : gather_elem_size(v.type);
    for (uint32_t i = 0; i < rows; ++i) {
        char* p = base + static_cast<size_t>(i) * kraw + roff;
        if (!sort_row_valid(v, i)) {
            std::memset(p, 0, es);
            nmask[i] |= static_cast<uint8_t>(1u << kbit);
            continue;
        }
        if (v.type == DRAKEN_BOOL) {
            uint32_t phys = v.selection[i];
            p[0] = static_cast<char>(
                (static_cast<const uint8_t*>(v.data)[phys >> 3] >> (phys & 7)) & 1u);
        } else {
            std::memcpy(p,
                        static_cast<const uint8_t*>(v.data)
                            + static_cast<size_t>(v.selection[i]) * es,
                        es);
        }
    }
}

// Load a row's raw key slot (kraw <= 16 bytes) into an __int128 (upper bytes 0).
inline __int128 gb_int_load(const char* raw, size_t kraw) {
    __int128 ik = 0;
    std::memcpy(&ik, raw, kraw);
    return ik;
}

// Hash a raw int key: XXH3 over the kraw value bytes, folded with the null mask
// so keys that differ only in null-ness hash apart.
inline uint64_t gb_int_hash(const char* raw, size_t kraw, uint8_t nmask) {
    uint64_t h = XXH3_64bits(raw, kraw);
    if (nmask) h ^= (0x9E3779B97F4A7C15ull * (nmask + 1u));
    return h;
}

struct GroupByLocal : LocalSinkState {
    std::array<GBPartition, kGBParts> parts;
    std::vector<AggColMeta> meta;
    std::vector<GBKind> kinds;
    std::vector<KeyColMeta> key_meta;
    bool fixed_keys = true;           // no string key column
    bool int_keys = false;            // fixed_keys && kraw <= 16 (raw int-packed)
    size_t kstride = 0;               // packed key bytes (framed fixed mode)
    size_t kraw = 0;                  // raw key bytes (int-key mode, no framing)
    std::vector<size_t> key_col_off;  // per key column offset into the framed key
    std::vector<size_t> key_raw_off;  // per key column offset into the raw int key
    bool has_rows = false;            // any COUNT(*) spec
    bool init = false;
    size_t entries_total = 0;         // Σ partition sizes (adaptive flush trigger)
    // per-morsel two-pass ingest scratch (serialize+hash all rows, then a
    // tight probe pass — the pass separation is the measured win)
    std::string mk_bytes;
    std::vector<uint32_t> mk_off, mk_len, mk_ent;
    std::vector<uint64_t> mk_hash;
    std::vector<uint8_t> mk_null;   // per-row null mask (int-key mode)
    // COUNT(DISTINCT) pair scratch (pass 1 serialize+hash, pass 2 probe)
    std::string cd_bytes;
    std::vector<uint32_t> cd_off, cd_len, cd_ent;
    std::vector<uint64_t> cd_hash;
    std::vector<uint8_t> cd_part;
    // single-column compressed-key fast path: per-PHYSICAL-slot hash/entry map
    // (sized data_length) + the referenced-slot mask + a reusable key buffer.
    std::vector<uint64_t> u_hash;
    std::vector<uint32_t> u_ent;
    std::vector<uint8_t> u_ref;
    std::string u_key;
};
struct GroupByGlobal : GlobalSinkState {
    std::mutex mtx;
    std::array<std::vector<GBPartition>, kGBParts> pending;   // queued worker partitions
    std::vector<AggColMeta> meta;
    std::vector<GBKind> kinds;
    std::vector<KeyColMeta> key_meta;
    bool fixed_keys = true;
    bool int_keys = false;
    size_t kstride = 0;
    size_t kraw = 0;
    std::vector<size_t> key_raw_off;
    bool has_rows = false;
    bool init = false;
};

struct GroupBySink : Sink {
    std::vector<size_t> key_idx;
    std::vector<AggSpec2> specs;
    MorselBuffer* out;
    size_t chunk_rows;

    GroupBySink(std::vector<size_t> keys, std::vector<AggSpec2> s, MorselBuffer* b,
                size_t chunk = 131072)
        : key_idx(std::move(keys)), specs(std::move(s)), out(b), chunk_rows(chunk) {}

    std::unique_ptr<GlobalSinkState> make_global() override {
        return std::make_unique<GroupByGlobal>();
    }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override {
        return std::make_unique<GroupByLocal>();
    }

    bool capture(GroupByLocal& l, const MorselPtr& in, ErrCtx& err) {
        l.key_meta.resize(key_idx.size());
        l.key_col_off.resize(key_idx.size());
        l.key_raw_off.resize(key_idx.size());
        l.fixed_keys = true;
        l.kstride = 0;
        l.kraw = 0;
        for (size_t k = 0; k < key_idx.size(); ++k) {
            if (key_idx[k] >= in->columns.size()) {
                err.code = 1;
                err.msg = "native engine: GROUP BY key column missing from input "
                          "morsel — fail loud, never a silent wrong grouping";
                return false;
            }
            const CxxColumn& c = in->columns[key_idx[k]];
            DrakenType t = c.view.type;
            if ((!sort_type_is_string(t) && t != DRAKEN_BOOL && gather_elem_size(t) == 0)
                    || t == DRAKEN_DECIMAL128) {
                err.code = 1;
                err.msg = "native engine: unsupported GROUP BY key column type";
                return false;
            }
            l.key_meta[k].type = t;
            l.key_meta[k].logical = c.own ? c.own->logical_type : nullptr;
            l.key_meta[k].captured = true;
            if (sort_type_is_string(t)) {
                l.fixed_keys = false;
            } else {
                size_t es = (t == DRAKEN_BOOL) ? 1 : gather_elem_size(t);
                l.key_col_off[k] = l.kstride;
                l.kstride += 1 + es;
                l.key_raw_off[k] = l.kraw;
                l.kraw += es;
            }
        }
        // All-fixed-width keys whose RAW value bytes fit an __int128 take the
        // integer-key probe: raw pack (no null framing) + 128-bit compare, with
        // null-ness carried out of band. Capped at 8 columns (null mask width).
        l.int_keys = l.fixed_keys && l.kraw <= 16 && key_idx.size() <= 8;
        l.meta.resize(specs.size());
        l.kinds.resize(specs.size());
        l.has_rows = false;
        for (size_t s = 0; s < specs.size(); ++s) {
            if (specs[s].col_idx < 0) {
                l.kinds[s] = GBKind::Rows;
                l.has_rows = true;
                continue;
            }
            const CxxColumn& c = in->columns[static_cast<size_t>(specs[s].col_idx)];
            DrakenType t = c.view.type;
            bool str_minmax = sort_type_is_string(t)
                && (specs[s].fn == AggFn::Min || specs[s].fn == AggFn::Max);
            // ARRAY_AGG copies values instead of ordering/summing them, so it takes
            // the string family too — its own guard, not agg2's.
            if (specs[s].fn == AggFn::ArrayAgg) {
                if (!aa_operand_supported(t)) {
                    err.code = 1;
                    err.msg = "native engine: unsupported ARRAY_AGG operand type — "
                              "fail loud, never a silent wrong answer";
                    return false;
                }
            } else if (specs[s].fn != AggFn::Count
                    && specs[s].fn != AggFn::CountDistinct
                    && !str_minmax
                    && !agg2_operand_supported(t)) {
                // COUNT reads only validity; COUNT(DISTINCT) reads serialized value
                // bytes (key_append fails loud on unsupported types at run time).
                err.code = 1;
                err.msg = "native engine: unsupported aggregate operand type — fail "
                          "loud, never a silent wrong answer";
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
            g.fixed_keys = l.fixed_keys;
            g.int_keys = l.int_keys;
            g.kstride = l.kstride;
            g.kraw = l.kraw;
            g.key_raw_off = l.key_raw_off;
            g.has_rows = l.has_rows;
            g.init = true;
        }
        for (size_t p = 0; p < kGBParts; ++p) {
            if (l.parts[p].size() > 0) {
                g.pending[p].push_back(std::move(l.parts[p]));
                l.parts[p] = GBPartition();
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

        // §11-sanctioned compressed-shape fast path (single key column only):
        // key each of the data_length PHYSICAL uniques (+ NULL) once instead of
        // per row — rows sharing a selection code are known-equal without
        // re-serializing/hashing/probing. Produces the same mk_hash/mk_ent
        // contract the per-row path does (identical grouping), so the entire
        // downstream (lane grow + Pass C aggregate updates) is unchanged.
        // draken_is_compressed covers both dict and constant layouts.
        bool keyed_fast = false;
        if (key_idx.size() == 1
                && draken_is_compressed(&in->columns[key_idx[0]].view)) {
            const DrakenVector& v = in->columns[key_idx[0]].view;
            uint32_t dl = v.data_length;
            mark_referenced_valid(v, l.u_ref);   // sizes to dl; 1 = referenced+valid
            l.u_hash.assign(dl, 0);
            l.u_ent.assign(dl, 0);
            uint64_t null_hash = 0;
            uint32_t null_ent = 0;
            bool have_null = false;
            uint32_t null_row = 0;
            for (uint32_t i = 0; i < rows; ++i) {
                if (!sort_row_valid(v, i)) { have_null = true; null_row = i; break; }
            }
            if (l.int_keys) {
                // single-column raw int key: kraw == es, mask is bit 0.
                size_t es = l.kraw;   // single col: kraw == es
                char rawb[16];
                for (uint32_t j = 0; j < dl; ++j) {
                    if (!l.u_ref[j]) continue;
                    std::memset(rawb, 0, l.kraw);
                    if (v.type == DRAKEN_BOOL) {
                        rawb[0] = static_cast<char>(
                            (static_cast<const uint8_t*>(v.data)[j >> 3] >> (j & 7)) & 1u);
                    } else {
                        std::memcpy(rawb,
                                    static_cast<const uint8_t*>(v.data)
                                        + static_cast<size_t>(j) * es,
                                    es);
                    }
                    uint64_t h = gb_int_hash(rawb, l.kraw, 0);
                    l.u_hash[j] = h;
                    l.u_ent[j] = l.parts[h >> kGBPartShift].upsert_int(
                        h, gb_int_load(rawb, l.kraw), 0);
                }
                if (have_null) {
                    std::memset(rawb, 0, l.kraw);
                    null_hash = gb_int_hash(rawb, l.kraw, 1);
                    null_ent = l.parts[null_hash >> kGBPartShift].upsert_int(
                        null_hash, gb_int_load(rawb, l.kraw), 1);
                }
            } else if (l.fixed_keys) {
                size_t es = l.kstride - 1;   // single col: kstride == 1 + es
                l.u_key.resize(l.kstride);
                char* kb = &l.u_key[0];
                for (uint32_t j = 0; j < dl; ++j) {
                    if (!l.u_ref[j]) continue;
                    gb_pack_fixed_phys(v, kb, es, j);
                    uint64_t h = XXH3_64bits(kb, l.kstride);
                    l.u_hash[j] = h;
                    l.u_ent[j] =
                        l.parts[h >> kGBPartShift].upsert_fixed(h, kb, l.kstride);
                }
                if (have_null) {
                    std::memset(kb, 0, l.kstride);   // matches gb_pack_fixed_key NULL arm
                    null_hash = XXH3_64bits(kb, l.kstride);
                    null_ent =
                        l.parts[null_hash >> kGBPartShift].upsert_fixed(
                            null_hash, kb, l.kstride);
                }
            } else {
                for (uint32_t j = 0; j < dl; ++j) {
                    if (!l.u_ref[j]) continue;
                    l.u_key.clear();
                    if (!key_append_phys(l.u_key, v, j, err))
                        return SinkResult::CONTINUE;
                    uint64_t h = XXH3_64bits(l.u_key.data(), l.u_key.size());
                    l.u_hash[j] = h;
                    l.u_ent[j] = l.parts[h >> kGBPartShift].upsert_var(
                        h, l.u_key.data(), static_cast<uint32_t>(l.u_key.size()));
                }
                if (have_null) {
                    l.u_key.clear();
                    if (!key_append(l.u_key, v, null_row, err))
                        return SinkResult::CONTINUE;
                    null_hash = XXH3_64bits(l.u_key.data(), l.u_key.size());
                    null_ent = l.parts[null_hash >> kGBPartShift].upsert_var(
                        null_hash, l.u_key.data(),
                        static_cast<uint32_t>(l.u_key.size()));
                }
            }
            l.mk_hash.resize(rows);
            l.mk_ent.resize(rows);
            for (uint32_t i = 0; i < rows; ++i) {
                if (sort_row_valid(v, i)) {
                    uint32_t phys = v.selection[i];
                    l.mk_hash[i] = l.u_hash[phys];
                    l.mk_ent[i] = l.u_ent[phys];
                } else {
                    l.mk_hash[i] = null_hash;
                    l.mk_ent[i] = null_ent;
                }
            }
            keyed_fast = true;
        }

        if (!keyed_fast) {
        // Pass A: serialize + hash every row (sequential writes — cheap).
        l.mk_hash.resize(rows);
        if (l.int_keys) {
            // int-key packing: RAW value bytes (kraw stride, no null framing) +
            // an out-of-band per-row null mask. Hash folds the mask in.
            l.mk_bytes.resize(static_cast<size_t>(rows) * l.kraw);
            l.mk_null.assign(rows, 0);
            char* base = l.mk_bytes.data();
            for (size_t k = 0; k < key_idx.size(); ++k)
                gb_pack_raw_key(in->columns[key_idx[k]].view, base, l.kraw,
                                l.key_raw_off[k], k, rows, l.mk_null.data());
            for (uint32_t i = 0; i < rows; ++i)
                l.mk_hash[i] = gb_int_hash(base + static_cast<size_t>(i) * l.kraw,
                                           l.kraw, l.mk_null[i]);
        } else if (l.fixed_keys) {
            // columnar packing: one type-dispatched pass per key column
            l.mk_bytes.resize(static_cast<size_t>(rows) * l.kstride);
            char* base = l.mk_bytes.data();
            for (size_t k = 0; k < key_idx.size(); ++k)
                gb_pack_fixed_key(in->columns[key_idx[k]].view, base, l.kstride,
                                  l.key_col_off[k], rows);
            for (uint32_t i = 0; i < rows; ++i)
                l.mk_hash[i] = XXH3_64bits(base + static_cast<size_t>(i) * l.kstride,
                                           l.kstride);
        } else {
            l.mk_bytes.clear();
            l.mk_off.clear(); l.mk_len.clear();
            l.mk_off.reserve(rows); l.mk_len.reserve(rows);
            for (uint32_t i = 0; i < rows; ++i) {
                uint32_t off0 = static_cast<uint32_t>(l.mk_bytes.size());
                for (size_t k = 0; k < key_idx.size(); ++k) {
                    if (!key_append(l.mk_bytes, in->columns[key_idx[k]].view, i, err)) {
                        return SinkResult::CONTINUE;
                    }
                }
                l.mk_off.push_back(off0);
                l.mk_len.push_back(static_cast<uint32_t>(l.mk_bytes.size()) - off0);
                l.mk_hash[i] = XXH3_64bits(l.mk_bytes.data() + off0,
                                           l.mk_bytes.size() - off0);
            }
        }
        // Pass B: tight probe loop. NO software prefetch — measured a no-op on
        // this machine (repeatedly); the serialize/probe pass SEPARATION is
        // what pays, never the prefetch.
        l.mk_ent.resize(rows);
        const char* ibase = l.mk_bytes.data();
        for (uint32_t i = 0; i < rows; ++i) {
            uint64_t h = l.mk_hash[i];
            GBPartition& P = l.parts[h >> kGBPartShift];
            l.mk_ent[i] = l.int_keys
                ? P.upsert_int(h,
                               gb_int_load(ibase + static_cast<size_t>(i) * l.kraw,
                                           l.kraw),
                               l.mk_null[i])
                : l.fixed_keys
                    ? P.upsert_fixed(h, l.mk_bytes.data()
                                            + static_cast<size_t>(i) * l.kstride,
                                     l.kstride)
                    : P.upsert_var(h, l.mk_bytes.data() + l.mk_off[i], l.mk_len[i]);
        }
        }  // !keyed_fast
        // Grow lanes ONCE per morsel to each partition's new entry count
        // (resize zero-fills new groups — the correct initial state).
        for (size_t p = 0; p < kGBParts; ++p) {
            GBPartition& P = l.parts[p];
            if (P.hashes.empty()) continue;
            size_t n = P.size();
            if (l.has_rows) P.grows.resize(n);
            if (P.lanes.size() != nspecs) P.lanes.resize(nspecs);
            if (P.dedup.size() != nspecs) P.dedup.resize(nspecs);
            if (P.dedupf.size() != nspecs) P.dedupf.resize(nspecs);
            for (size_t s = 0; s < nspecs; ++s)
                gb_lanes_resize(P.lanes[s], l.kinds[s], n);
        }
        // Pass C: columnar updates — kind dispatched ONCE per spec, tight row loops.
        if (l.has_rows) {
            for (uint32_t i = 0; i < rows; ++i)
                l.parts[l.mk_hash[i] >> kGBPartShift].grows[l.mk_ent[i]] += 1;
        }
        for (size_t s = 0; s < nspecs; ++s) {
            GBKind kind = l.kinds[s];
            if (kind == GBKind::Rows) continue;
            const DrakenVector& v =
                in->columns[static_cast<size_t>(specs[s].col_idx)].view;
            bool want_max = specs[s].fn == AggFn::Max;
            switch (kind) {
                case GBKind::Valid:
                    for (uint32_t i = 0; i < rows; ++i) {
                        if (!sort_row_valid(v, i)) continue;
                        l.parts[l.mk_hash[i] >> kGBPartShift]
                            .lanes[s].valid[l.mk_ent[i]] += 1;
                    }
                    break;
                case GBKind::SumI:
                    for (uint32_t i = 0; i < rows; ++i) {
                        if (!sort_row_valid(v, i)) continue;
                        GBLanes& L = l.parts[l.mk_hash[i] >> kGBPartShift].lanes[s];
                        uint32_t e = l.mk_ent[i];
                        int64_t r = agg2_read_raw(v, i, false);
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
                        if (!sort_row_valid(v, i)) continue;
                        GBLanes& L = l.parts[l.mk_hash[i] >> kGBPartShift].lanes[s];
                        uint32_t e = l.mk_ent[i];
                        L.i128[e] += agg2_read_raw(v, i, false);
                        L.valid[e] += 1;
                    }
                    break;
                case GBKind::SumF:
                case GBKind::AvgF:
                    for (uint32_t i = 0; i < rows; ++i) {
                        if (!sort_row_valid(v, i)) continue;
                        GBLanes& L = l.parts[l.mk_hash[i] >> kGBPartShift].lanes[s];
                        uint32_t e = l.mk_ent[i];
                        int64_t bits = agg2_read_raw(v, i, true);
                        double d;
                        std::memcpy(&d, &bits, sizeof(d));
                        L.f64[e] += d;
                        L.valid[e] += 1;
                    }
                    break;
                case GBKind::SumD128:
                case GBKind::AvgD128:
                    for (uint32_t i = 0; i < rows; ++i) {
                        if (!sort_row_valid(v, i)) continue;
                        GBLanes& L = l.parts[l.mk_hash[i] >> kGBPartShift].lanes[s];
                        uint32_t e = l.mk_ent[i];
                        L.i128[e] += agg2_read_i128(v, i);
                        L.valid[e] += 1;
                    }
                    break;
                case GBKind::MinMaxNum:
                    for (uint32_t i = 0; i < rows; ++i) {
                        if (!sort_row_valid(v, i)) continue;
                        GBLanes& L = l.parts[l.mk_hash[i] >> kGBPartShift].lanes[s];
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
                        if (!sort_row_valid(v, i)) continue;
                        GBLanes& L = l.parts[l.mk_hash[i] >> kGBPartShift].lanes[s];
                        uint32_t e = l.mk_ent[i];
                        __int128 r = agg2_read_i128(v, i);
                        if (L.valid[e] == 0
                                || (want_max ? r > L.i128[e] : r < L.i128[e]))
                            L.i128[e] = r;
                        L.valid[e] += 1;
                    }
                    break;
                case GBKind::CountDistinct: {
                    // Count a group's (group, value) pair only on FIRST sighting.
                    if (gb_cd_fixed(l.meta[s].type)) {
                        // typed pair path: (u32 entry, u64 widened value) —
                        // no serialization, no arena, exact integer identity.
                        bool is_f = l.meta[s].is_float;
                        for (uint32_t i = 0; i < rows; ++i) {
                            if (!sort_row_valid(v, i)) continue;
                            GBPartition& P =
                                l.parts[l.mk_hash[i] >> kGBPartShift];
                            uint32_t e = l.mk_ent[i];
                            uint64_t val = static_cast<uint64_t>(
                                agg2_read_raw(v, i, is_f));
                            uint64_t dh = GBDedupF::hash_pair(e, val);
                            if (P.dedupf[s].upsert(dh, e, val))
                                P.lanes[s].valid[e] += 1;
                        }
                        break;
                    }
                    // serialized-bytes path (string / DECIMAL128 values).
                    // TWO passes: (1) serialize + hash every valid row's pair
                    // (sequential appends), (2) a tight probe loop — measured
                    // ~30% faster than probing inline with serialization.
                    // Dict shape (§11-approved) serializes each referenced
                    // unique value ONCE and memcpys it per row — bytes
                    // identical to the per-row arm, dedup exact across shapes.
                    l.cd_bytes.clear();
                    l.cd_off.clear(); l.cd_len.clear();
                    l.cd_ent.clear(); l.cd_hash.clear(); l.cd_part.clear();
                    std::vector<std::string> uv;
                    bool dict_shape = v.data_length < v.length;
                    if (dict_shape) {
                        std::vector<uint8_t> ref;
                        mark_referenced_valid(v, ref);
                        uv.resize(v.data_length);
                        for (uint32_t j = 0; j < v.data_length; ++j) {
                            if (ref[j] == 0) continue;
                            if (!key_append_phys(uv[j], v, j, err))
                                return SinkResult::CONTINUE;
                        }
                    }
                    for (uint32_t i = 0; i < rows; ++i) {
                        if (!sort_row_valid(v, i)) continue;
                        uint32_t e = l.mk_ent[i];
                        uint32_t off0 = static_cast<uint32_t>(l.cd_bytes.size());
                        l.cd_bytes.append(reinterpret_cast<const char*>(&e),
                                          sizeof(e));
                        if (dict_shape) {
                            l.cd_bytes.append(uv[v.selection[i]]);
                        } else if (!key_append(l.cd_bytes, v, i, err)) {
                            return SinkResult::CONTINUE;
                        }
                        uint32_t klen =
                            static_cast<uint32_t>(l.cd_bytes.size()) - off0;
                        l.cd_off.push_back(off0);
                        l.cd_len.push_back(klen);
                        l.cd_ent.push_back(e);
                        l.cd_hash.push_back(
                            XXH3_64bits(l.cd_bytes.data() + off0, klen));
                        l.cd_part.push_back(
                            static_cast<uint8_t>(l.mk_hash[i] >> kGBPartShift));
                    }
                    uint32_t npairs = static_cast<uint32_t>(l.cd_hash.size());
                    for (uint32_t k2 = 0; k2 < npairs; ++k2) {
                        GBPartition& P = l.parts[l.cd_part[k2]];
                        if (P.dedup[s].upsert(l.cd_hash[k2], l.cd_ent[k2],
                                              l.cd_bytes.data() + l.cd_off[k2],
                                              l.cd_len[k2]))
                            P.lanes[s].valid[l.cd_ent[k2]] += 1;
                    }
                    break;
                }
                case GBKind::MinMaxStr: {
                    const DrakenStringArena* sa = string_arena_of(v);
                    for (uint32_t i = 0; i < rows; ++i) {
                        if (!sort_row_valid(v, i)) continue;
                        GBLanes& L = l.parts[l.mk_hash[i] >> kGBPartShift].lanes[s];
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
                    const int64_t cap = specs[s].aa_max_per_group;
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
                            A.push(st, nul, 0, 0, p, len, cap);
                        } else if (st == AAStore::I128) {
                            A.push(st, nul, 0, nul ? 0 : agg2_read_i128(v, i),
                                   nullptr, 0, cap);
                        } else {
                            A.push(st, nul, nul ? 0 : agg2_read_raw(v, i, is_f), 0,
                                   nullptr, 0, cap);
                        }
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
            for (const GBDedup& d : P.dedup) l.entries_total += d.size();
            for (const GBDedupF& d : P.dedupf) l.entries_total += d.size();
        }
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
            merged.rehash(merge_total);
            merged.hashes.reserve(merge_total);
            if (g.int_keys) { merged.kint.reserve(merge_total); merged.knull.reserve(merge_total); }
            else if (g.fixed_keys) merged.kfix.reserve(merge_total * g.kstride);
            else { merged.key_off.reserve(merge_total); merged.key_len.reserve(merge_total); }
        }
        std::vector<uint32_t> ge;
        for (size_t i = 1; i < list.size(); ++i) {
            GBPartition& src = list[i];
            uint32_t sn = static_cast<uint32_t>(src.size());
            // Columnar merge: map every src entry to its merged entry first, then
            // combine lane by lane (kind dispatched once per spec).
            ge.resize(sn);
            for (uint32_t e = 0; e < sn; ++e) {
                ge[e] = g.int_keys
                    ? merged.upsert_int(src.hashes[e], src.kint[e], src.knull[e])
                    : g.fixed_keys
                        ? merged.upsert_fixed(src.hashes[e],
                                              src.kfix.data()
                                                  + static_cast<size_t>(e) * g.kstride,
                                              g.kstride)
                        : merged.upsert_var(src.hashes[e], src.key_ptr_var(e),
                                            src.key_len[e]);
            }
            size_t mn = merged.size();
            if (g.has_rows) merged.grows.resize(mn);
            if (merged.lanes.size() != nspecs) merged.lanes.resize(nspecs);
            if (merged.dedup.size() != nspecs) merged.dedup.resize(nspecs);
            if (merged.dedupf.size() != nspecs) merged.dedupf.resize(nspecs);
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
                        // Concatenate the worker's list onto the merged one. The cap
                        // is re-checked here: N workers may each hold up to `cap` for
                        // the same group, so a merge can cross it even when no single
                        // worker did.
                        const AAStore ast = aa_store_of(g.meta[s].type);
                        const int64_t cap = specs[s].aa_max_per_group;
                        // The other arms only read the source, so the loop aliases it
                        // const. This one drains it: `src` is released right after the
                        // merge, so its element strings are moved, not copied.
                        GBLanes& SL = src.lanes[s];
                        for (uint32_t e = 0; e < sn; ++e) {
                            GBArrayAggState& SA = SL.aa[e];
                            GBArrayAggState& DA = D.aa[ge[e]];
                            if (SA.overflowed) DA.overflowed = true;
                            size_t take = SA.size();
                            if (static_cast<int64_t>(DA.size() + take) > cap) {
                                DA.overflowed = true;
                                take = (static_cast<int64_t>(DA.size()) >= cap)
                                           ? 0u : static_cast<size_t>(cap - DA.size());
                            }
                            if (take == 0) continue;
                            DA.nulls.insert(DA.nulls.end(), SA.nulls.begin(),
                                            SA.nulls.begin() + take);
                            switch (ast) {
                                case AAStore::Raw:
                                    DA.raws.insert(DA.raws.end(), SA.raws.begin(),
                                                   SA.raws.begin() + take);
                                    break;
                                case AAStore::I128:
                                    DA.i128s.insert(DA.i128s.end(), SA.i128s.begin(),
                                                    SA.i128s.begin() + take);
                                    break;
                                case AAStore::Str:
                                    DA.strs.insert(
                                        DA.strs.end(),
                                        std::make_move_iterator(SA.strs.begin()),
                                        std::make_move_iterator(SA.strs.begin() + take));
                                    break;
                            }
                        }
                        break;
                    }
                    case GBKind::CountDistinct: {
                        // Re-key each (group, value) pair: the group entry
                        // renumbers under the merged partition, and the same
                        // pair may arrive from several workers — only a
                        // merged-table MISS counts.
                        if (gb_cd_fixed(g.meta[s].type)) {
                            GBDedupF& SF = src.dedupf[s];
                            GBDedupF& DF = merged.dedupf[s];
                            for (uint32_t pi = 0;
                                 pi < static_cast<uint32_t>(SF.size()); ++pi) {
                                uint32_t m = ge[SF.pent[pi]];
                                uint64_t val = SF.pval[pi];
                                uint64_t dh = GBDedupF::hash_pair(m, val);
                                if (DF.upsert(dh, m, val)) D.valid[m] += 1;
                            }
                            break;
                        }
                        GBDedup& SD = src.dedup[s];
                        GBDedup& DD = merged.dedup[s];
                        std::string dkey;
                        for (uint32_t pi = 0; pi < SD.size(); ++pi) {
                            uint32_t m = ge[SD.pair_ent[pi]];
                            const char* k = SD.key_ptr(pi);
                            uint32_t kl = SD.key_len[pi];
                            dkey.clear();
                            dkey.append(reinterpret_cast<const char*>(&m),
                                        sizeof(m));
                            dkey.append(k + sizeof(uint32_t),
                                        kl - sizeof(uint32_t));
                            uint64_t dh = XXH3_64bits(dkey.data(), dkey.size());
                            if (DD.upsert(dh, m, dkey.data(),
                                          static_cast<uint32_t>(dkey.size())))
                                D.valid[m] += 1;
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
            std::vector<GBKeyRef> chunk_keys(n);
            // int-key mode: rebuild the framed kstride-strided key bytes
            // (per column: '\1'|'\0' + es value/zero bytes) from the raw
            // __int128 store + out-of-band null mask, so emit_key_columns'
            // fixed-stride parser is byte-identical to the kfix path.
            std::vector<char> kint_bytes;
            if (g.int_keys) {
                kint_bytes.assign(static_cast<size_t>(n) * g.kstride, 0);
                for (uint32_t i = 0; i < n; ++i) {
                    uint32_t e = static_cast<uint32_t>(start) + i;
                    const unsigned char* raw =
                        reinterpret_cast<const unsigned char*>(&merged.kint[e]);
                    uint8_t nm = merged.knull[e];
                    char* dst = kint_bytes.data() + static_cast<size_t>(i) * g.kstride;
                    size_t co = 0;
                    for (size_t k = 0; k < g.key_meta.size(); ++k) {
                        DrakenType t = g.key_meta[k].type;
                        size_t es = (t == DRAKEN_BOOL) ? 1 : gather_elem_size(t);
                        if ((nm >> k) & 1u) {
                            dst[co] = '\0';   // value bytes already zero-filled
                        } else {
                            dst[co] = '\1';
                            std::memcpy(dst + co + 1, raw + g.key_raw_off[k], es);
                        }
                        co += 1 + es;
                    }
                }
            }
            for (uint32_t i = 0; i < n; ++i) {
                uint32_t e = static_cast<uint32_t>(start) + i;
                chunk_keys[i] = g.int_keys
                    ? GBKeyRef{kint_bytes.data() + static_cast<size_t>(i) * g.kstride,
                               g.kstride}
                    : g.fixed_keys
                        ? GBKeyRef{merged.kfix.data()
                                       + static_cast<size_t>(e) * g.kstride,
                                   g.kstride}
                        : GBKeyRef{merged.key_ptr_var(e), merged.key_len[e]};
            }
            emit_key_columns(chunk_keys, g.key_meta, g.fixed_keys, *m, err);
            if (err.code != 0) return;
            for (size_t s = 0; s < nspecs; ++s) {
                GBKind kind = g.kinds[s];
                const GBLanes& L = merged.lanes[s];
                GBLaneView lv;
                if (kind == GBKind::Rows) {
                    lv.rows = merged.grows.data() + start;
                } else if (kind == GBKind::ArrayAgg) {
                    lv.aa = const_cast<GBArrayAggState*>(L.aa.data()) + start;
                    lv.aa_spec = &specs[s];
                } else {
                    lv.valid = L.valid.data() + start;
                    if (!L.i64.empty())  lv.i64  = L.i64.data() + start;
                    if (!L.f64.empty())  lv.f64  = L.f64.data() + start;
                    if (!L.i128.empty()) lv.i128 = L.i128.data() + start;
                    if (!L.sval.empty()) lv.sval = L.sval.data() + start;
                }
                m->columns.push_back(emit_lane_column(g.meta[s], kind, lv, n, err));
                if (err.code != 0) return;
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

struct DistinctLocal : LocalSinkState {
    std::unordered_set<std::string> seen;
    std::vector<MorselPtr> morsels;
    std::vector<std::string> kept_keys;   // parallel to kept refs
    std::vector<uint32_t> ref_m, ref_r;
    std::string scratch;
};
struct DistinctGlobal : GlobalSinkState {
    std::mutex mtx;
    std::unordered_set<std::string> seen;
    std::vector<MorselPtr> morsels;
    std::vector<uint32_t> ref_m, ref_r;
};

struct DistinctSink : Sink {
    std::vector<size_t> on_idx;   // dedup key columns; empty = all columns
    MorselBuffer* out;
    size_t chunk_rows;

    DistinctSink(std::vector<size_t> on, MorselBuffer* b, size_t chunk = 131072)
        : on_idx(std::move(on)), out(b), chunk_rows(chunk) {}

    std::unique_ptr<GlobalSinkState> make_global() override {
        return std::make_unique<DistinctGlobal>();
    }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override {
        return std::make_unique<DistinctLocal>();
    }

    SinkResult sink(const MorselPtr& in, GlobalSinkState&, LocalSinkState& ls,
                    ErrCtx& err) override {
        auto& l = static_cast<DistinctLocal&>(ls);
        uint32_t rows = in->num_rows();
        if (rows == 0) return SinkResult::CONTINUE;
        size_t ncols = in->columns.size();
        uint32_t mi = static_cast<uint32_t>(l.morsels.size());
        bool used = false;

        size_t dedup_ncols = on_idx.empty() ? ncols : on_idx.size();
        if (dedup_ncols == 1) {
            const DrakenVector& v =
                in->columns[on_idx.empty() ? 0 : on_idx[0]].view;
            if (draken_is_compressed(&v)) {
                // §11-sanctioned dict-shape fast path: one key per PHYSICAL
                // unique value (+ one for NULL if present) instead of per row —
                // rows sharing a dict code are known equal without hashing.
                std::vector<uint32_t> first_row(v.data_length, UINT32_MAX);
                uint32_t null_row = UINT32_MAX;
                for (uint32_t i = 0; i < rows; ++i) {
                    if (!sort_row_valid(v, i)) {
                        if (null_row == UINT32_MAX) null_row = i;
                        continue;
                    }
                    uint32_t phys = v.selection[i];
                    if (first_row[phys] == UINT32_MAX) first_row[phys] = i;
                }
                if (null_row != UINT32_MAX) {
                    l.scratch.clear();
                    if (!key_append(l.scratch, v, null_row, err))
                        return SinkResult::CONTINUE;
                    if (l.seen.insert(l.scratch).second) {
                        l.kept_keys.push_back(l.scratch);
                        l.ref_m.push_back(mi);
                        l.ref_r.push_back(null_row);
                        used = true;
                    }
                }
                for (uint32_t j = 0; j < v.data_length; ++j) {
                    if (first_row[j] == UINT32_MAX) continue;
                    l.scratch.clear();
                    if (!key_append_phys(l.scratch, v, j, err))
                        return SinkResult::CONTINUE;
                    if (l.seen.insert(l.scratch).second) {
                        l.kept_keys.push_back(l.scratch);
                        l.ref_m.push_back(mi);
                        l.ref_r.push_back(first_row[j]);
                        used = true;
                    }
                }
                if (used) l.morsels.push_back(in);
                return SinkResult::CONTINUE;
            }
        }

        for (uint32_t i = 0; i < rows; ++i) {
            l.scratch.clear();
            if (on_idx.empty()) {
                for (size_t c = 0; c < ncols; ++c) {
                    if (!key_append(l.scratch, in->columns[c].view, i, err))
                        return SinkResult::CONTINUE;
                }
            } else {
                for (size_t c : on_idx) {
                    if (!key_append(l.scratch, in->columns[c].view, i, err))
                        return SinkResult::CONTINUE;
                }
            }
            if (l.seen.insert(l.scratch).second) {
                l.kept_keys.push_back(l.scratch);
                l.ref_m.push_back(mi);
                l.ref_r.push_back(i);
                used = true;
            }
        }
        if (used) l.morsels.push_back(in);   // only retain morsels a kept row references
        return SinkResult::CONTINUE;
    }

    void combine(GlobalSinkState& gs, LocalSinkState& ls, ErrCtx&) override {
        auto& g = static_cast<DistinctGlobal&>(gs);
        auto& l = static_cast<DistinctLocal&>(ls);
        std::lock_guard<std::mutex> lk(g.mtx);
        uint32_t base = static_cast<uint32_t>(g.morsels.size());
        for (MorselPtr& m : l.morsels) g.morsels.push_back(std::move(m));
        for (size_t i = 0; i < l.kept_keys.size(); ++i) {
            if (g.seen.insert(std::move(l.kept_keys[i])).second) {
                g.ref_m.push_back(base + l.ref_m[i]);
                g.ref_r.push_back(l.ref_r[i]);
            }
        }
    }

    void finalize(GlobalSinkState& gs, ErrCtx& err) override {
        auto& g = static_cast<DistinctGlobal&>(gs);
        size_t total = g.ref_m.size();
        if (total == 0 || g.morsels.empty()) return;
        const std::vector<std::string>& names = g.morsels.front()->names;
        // gather_rows indexes rows by GLOBAL row id via (row_m, row_r) maps — here
        // the refs already ARE (morsel, row) pairs, so feed them through directly.
        std::vector<uint32_t> order(total);
        for (size_t i = 0; i < total; ++i) order[i] = static_cast<uint32_t>(i);
        for (size_t start = 0; start < total; start += chunk_rows) {
            size_t count = std::min(chunk_rows, total - start);
            MorselPtr m = gather_rows(g.morsels, order, start, count, g.ref_m, g.ref_r,
                                      names, err);
            if (err.code != 0) return;
            out->morsels.push_back(std::move(m));
        }
    }
};

}  // namespace opteryx::engine
