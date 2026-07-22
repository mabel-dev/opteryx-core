/**
 * Lock-free parquet IO pipeline using BS::thread_pool and moodycamel queue.
 *
 * Pure C++ IO:
 * - Local files: POSIX pread()
 * - HTTP / HTTPS: HttpClient::get() with Range header
 * - GCS gs://: rewritten to https://storage.googleapis.com/... then HTTP range
 *
 * Worker threads read + decode + IPC-serialize without the GIL.
 * Results dequeued via lock-free moodycamel queue.
 */

#pragma once

#include <string>
#include <vector>
#include <memory>
#include <atomic>
#include <deque>
#include <exception>
#include <cstdint>
#include <cstdio>
#include <utility>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <map>
#include <unordered_map>

#include "BS_thread_pool.hpp"
// Remote (HTTP/HTTPS/GCS) reads are gated on RUGO_ENABLE_HTTP — defined by the
// opteryx_core build (which compiles + links http_client.cpp / libcurl), unset
// by the standalone rugo wheel (local filesystem only). When unset, libcurl is
// never included and remote paths fail loud in read_range / decode_row_group.
#ifdef RUGO_ENABLE_HTTP
#include "http_client.hpp"
#endif
#include "decode.hpp"
#include "ipc_serialize.hpp"
#include "metadata.hpp"
#include "core/string_slot.h"   // Stage 4b: build Draken string slots in the worker
#include "core/buffers.h"       // DrakenVector / DrakenStringArena — worker-side pass-1 predicate view
#include "core/vector_alloc.h"  // draken_identity_sel — dense selection for the view
// docs/EXECUTION_TRACING_DESIGN.md: rugo calls the extern "C" bridge
// (trace_bridge_c.h), NEVER draken/core/trace.hpp directly — this file
// compiles into pool_reader.so (a separate .so from _operators.so, which
// also pulls this same io_pipeline.hpp via native_parquet_scan_source.hpp),
// and header-only inline/static C++ state does not merge across .so
// boundaries. See src/cpp/engine/trace.hpp's header comment for the same
// rule on the engine side, and trace_bridge_c.h for why the bridge exists.
#include "core/trace_bridge_c.h"

namespace rugo {

// C-ABI sink for handing decoded columns to the opteryx side. Pure C types only
// — rugo must not depend on opteryx/draken; the opteryx adapter fills these in
// (opteryx/compiled/structures/pool_sink_adapter.hpp).
//   reserve(ctx, size, &ptr) -> ref_id : reserve `size` MemoryPool bytes, set
//       *ptr to the writable region; returns ref_id (>=0) or -1 on exhaustion.
//   finalize(ctx, ref_id, actual_len)  : commit the reserved segment.
//   draken_alloc / draken_free         : the Draken allocator (WP-6b direct
//       path). Non-nullable fixed-width columns are serialized as raw Draken
//       buffers the consumer wraps into a Vector with zero copy; abandoned
//       buffers are freed via draken_free in MorselRef's destructor.
struct PoolSink {
    void*   ctx = nullptr;
    int64_t (*reserve)(void* ctx, int64_t size, void** out_ptr) = nullptr;
    void    (*finalize)(void* ctx, int64_t ref_id, int64_t actual_len) = nullptr;
    void*   (*draken_alloc)(size_t n) = nullptr;
    void    (*draken_free)(void* p) = nullptr;
};

// rugo-local discriminant for a column's handoff form. Kept as a plain int in
// ColumnOut so the .pxd sees a stable layout.
//   0           : pool path — IPC blob in MemoryPool at ref_id (WP-6a).
//   1..5        : direct path — `data` (+ optional `validity`) is a Draken-owned
//                 POSITIONAL buffer of `length` rows the consumer wraps via
//                 draken_vector_own_raw (zero copy). The worker has already done
//                 any compact→positional scatter (WP-6b-2).
enum DirectKind {
    DK_POOL = 0, DK_INT64 = 1, DK_FLOAT32 = 2, DK_FLOAT64 = 3,
    DK_BOOL = 4, DK_DECIMAL128 = 5,
    // Stage 4b — variable-width direct path. `data` is a draken_alloc'd
    // DrakenStringSlot array; `arena` holds the long-string bytes. DK_VARCHAR is
    // one slot per row (plain). DK_VARCHAR_DICT (reserved, not yet emitted) would
    // carry `data_length` unique-value slots + a `codes` selection of `length`.
    DK_VARCHAR = 6, DK_VARCHAR_DICT = 7,
    // Numeric "compressed" (§11 Dict-shaped) direct path. `data` is a
    // draken_alloc'd int64_t[data_length] dictionary of unique values; `codes`
    // is a uint32_t[length] per-row selection. int32 dictionaries widen to
    // int64. The consumer wraps via draken_vector_own_dict_i64 (zero copy).
    DK_INT64_DICT = 8,
    // Float "compressed" (Dict-shaped) direct path — like DK_INT64_DICT but the
    // dictionary holds float64/float32 values (no widening). Consumer wraps via
    // draken_vector_own_dict_f64 / _f32.
    DK_FLOAT64_DICT = 9,
    DK_FLOAT32_DICT = 10,
    // E33 — unsigned integer direct paths. `data` is a draken_alloc'd positional
    // array of the EXACT declared width (1/2/4/8 bytes per row, matching
    // DRAKEN_UINT8/16/32/64) — unlike the signed int32 path, these never widen.
    // Consumer wraps via draken_vector_own_raw with the matching DrakenType tag.
    DK_UINT8 = 11, DK_UINT16 = 12, DK_UINT32 = 13, DK_UINT64 = 14,
    // Unsigned "compressed" (Dict-shaped) direct path, mirroring DK_INT64_DICT:
    // `data` is a draken_alloc'd dictionary of `data_length` unique values at the
    // exact declared element width (1/2/4/8 bytes); `codes` is the uint32_t[length]
    // per-row selection. Consumer wraps via draken_vector_own_dict_u8/16/32/64.
    DK_UINT8_DICT = 15, DK_UINT16_DICT = 16, DK_UINT32_DICT = 17, DK_UINT64_DICT = 18
};

struct ColumnOut {
    int      direct_kind = DK_POOL;  // DirectKind
    void*    data = nullptr;         // direct: draken_alloc'd positional values / string slots
    uint8_t* validity = nullptr;     // direct: draken_alloc'd null bitmap, or NULL
    uint32_t length = 0;             // direct: logical row count
    int64_t  ref_id = -1;            // pool path: MemoryPool ref
    uint8_t  dec_precision = 0;      // DK_DECIMAL128: descriptor
    uint8_t  dec_scale = 0;
    // DK_VARCHAR* string buffers (draken_alloc'd). OWNERSHIP: the consumer hands
    // these to draken_vector_own_string, which COPIES + frees them; so the
    // consumer must `morsel_take_string` (null them) to stop this destructor from
    // double-freeing. Any not taken (decode error / abandonment) are freed here.
    void*    arena = nullptr;        // long-string byte arena
    size_t   arena_len = 0;          // valid bytes in arena
    void*    codes = nullptr;        // DK_VARCHAR_DICT: uint32 code per row
    uint32_t data_length = 0;        // DK_VARCHAR_DICT: number of unique-value slots
    bool     dict_sorted = false;    // dict shapes: `data` is ascending (is_sorted)
};

// Owns the direct-path Draken buffers (data + validity) it carries: any not
// "taken" by the consumer (via morsel_take_direct) are draken_free'd on
// destruction — covering decode-error, LIMIT early-exit, and shutdown-drain
// paths. Move-only; std::vector move leaves the source's columns empty so a
// moved-from MorselRef frees nothing.
struct MorselRef {
    std::string path;
    int rg_idx = -1;
    std::vector<std::string> column_names;
    std::vector<ColumnOut> columns;
    void (*free_fn)(void* p) = nullptr;   // = PoolSink.draken_free
    int64_t bytes_fetched = 0;
    uint64_t read_ns = 0;
    uint64_t decode_ns = 0;
    std::string error;
    bool success = false;
    // Phase 2: this row group yields zero rows (a pushed-conjunct equality
    // column's dictionary lacked every needle). The consumer skips it entirely.
    bool empty_filtered = false;
    int64_t empty_rows = 0;   // pre-filter row count, for telemetry
    // Q24 latmat: bit-packed per-row survivor mask computed on the worker by a
    // pushed pass-1 predicate (opteryx callback). Empty = no predicate pushed / not
    // applicable → the consumer evaluates on the main thread (fallback). std::vector,
    // freed automatically — NOT a draken buffer, so NOT touched by free_fn below.
    std::vector<uint8_t> survivor_mask;

    MorselRef() = default;
    MorselRef(const MorselRef&) = delete;
    MorselRef& operator=(const MorselRef&) = delete;
    MorselRef(MorselRef&&) = default;
    MorselRef& operator=(MorselRef&&) = default;
    ~MorselRef() {
        if (free_fn) {
            for (auto& c : columns) {
                if (c.data) free_fn(c.data);
                if (c.validity) free_fn(c.validity);
                if (c.arena) free_fn(c.arena);
                if (c.codes) free_fn(c.codes);
            }
        }
    }
};

// Q24 latmat pass-1 predicate pushed from opteryx. `fn` is an opaque C-ABI callback
// (opteryx_pass1_predicate_eval): int fn(void* ctx, DrakenVector** cols, int ncols,
// uint32_t num_rows, uint8_t* out_mask). rugo stays opteryx-free — only draken's
// DrakenVector and this fn-ptr cross the boundary. `cols` are the predicate's columns
// by name, in the order the worker passes them (== ctx's resolved col_idx order).
typedef int (*Pass1PredFn)(void*, DrakenVector**, int, uint32_t, uint8_t*);
struct Pass1Pred {
    Pass1PredFn fn = nullptr;
    void*       ctx = nullptr;
    std::vector<std::string> cols;   // predicate column names, in pass-order
};

// Build a NON-owning DrakenVector view over a decoded ColumnOut so the pushed
// predicate can read it without a copy. Returns false for column shapes not yet
// supported worker-side (caller then leaves survivor_mask empty → serial fallback).
// `sa` backs a string column's arena header and must outlive `v`'s use.
static inline bool pass1_build_dv_view(ColumnOut& co, uint32_t nrows,
                                       DrakenStringArena& sa, DrakenVector& v) {
    if (co.direct_kind == DK_VARCHAR) {
        // Plain per-row string slots + separate byte arena (§ wp01 format). Wrap
        // non-owning; str_contains only dereferences sa.slots[sel[i]] + sa.arena.
        sa.slots       = static_cast<DrakenStringSlot*>(co.data);
        sa.arena       = static_cast<uint8_t*>(co.arena);
        sa.length      = nrows;
        sa.arena_used  = co.arena_len;
        sa.arena_cap   = co.arena_len;
        sa.null_bitmap = co.validity;
        sa.owns_buffers = 0;
        sa.type        = DRAKEN_VARCHAR;
        v.data        = &sa;
        v.selection   = draken_identity_sel(nrows);   // dense
        v.data_length = nrows;
        v.length      = nrows;
        v.validity    = co.validity;
        v.type        = DRAKEN_VARCHAR;
        v.flags       = 0;
        return true;
    }
    return false;   // unsupported kind → serial fallback
}

// Run the pushed pass-1 predicate over a fully-decoded row group, filling
// result.survivor_mask (bit-packed, nbytes). Leaves it empty (→ serial fallback) if
// any predicate column is absent or an unsupported shape. Pure C++/no GIL — safe on
// the decode worker thread.
static inline void pass1_run_predicate(MorselRef& result, const Pass1Pred& pred) {
    if (!pred.fn || !result.success || result.columns.empty()) return;
    const uint32_t nrows = result.columns[0].length;
    if (nrows == 0) return;
    const int ncols = static_cast<int>(pred.cols.size());
    if (ncols == 0 || ncols > 64) return;
    DrakenStringArena arenas[64];
    DrakenVector      dvs[64];
    DrakenVector*     dvp[64];
    for (int i = 0; i < ncols; ++i) {
        int ci = -1;
        for (size_t j = 0; j < result.column_names.size(); ++j)
            if (result.column_names[j] == pred.cols[i]) { ci = static_cast<int>(j); break; }
        if (ci < 0) return;                                   // column missing → fallback
        if (result.columns[ci].length != nrows) return;       // width mismatch → fallback
        if (!pass1_build_dv_view(result.columns[ci], nrows, arenas[i], dvs[i]))
            return;                                            // unsupported shape → fallback
        dvp[i] = &dvs[i];
    }
    const size_t nbytes = (static_cast<size_t>(nrows) + 7) >> 3;
    result.survivor_mask.assign(nbytes, 0);
    const int rc = pred.fn(pred.ctx, dvp, ncols, nrows, result.survivor_mask.data());
    if (rc != 0) {
        result.survivor_mask.clear();
        result.success = false;
        result.error = "pass-1 predicate eval failed (rc=" + std::to_string(rc) + ")";
    }
}

// Take ownership of column i's direct buffers: returns `data` and (via out param)
// `validity`, nulling both slots so MorselRef's destructor won't free them (the
// consumer's Vector now owns them). Returns nullptr for a pool-path column.
static inline void* morsel_take_direct(MorselRef& m, size_t i, uint8_t** out_validity) {
    void* p = m.columns[i].data;
    *out_validity = m.columns[i].validity;
    m.columns[i].data = nullptr;
    m.columns[i].validity = nullptr;
    return p;
}

// Take ownership of column i's variable-width string buffers (arena + dict codes),
// nulling the slots so the destructor won't free them — REQUIRED for DK_VARCHAR*
// columns because draken_vector_own_string frees the arena it is handed. Pairs
// with morsel_take_direct (which takes the slots `data` + validity).
static inline void morsel_take_string(MorselRef& m, size_t i,
                                      void** out_arena, void** out_codes) {
    *out_arena = m.columns[i].arena;
    *out_codes = m.columns[i].codes;
    m.columns[i].arena = nullptr;
    m.columns[i].codes = nullptr;
}

// WP-6b: a fixed-width value array qualifies for the direct path if it is either
// already positional (size == num_rows: no nulls, or an OPTIONAL column that
// happens to have none) OR compact with a validity bitmap to scatter against
// (size < num_rows && nullable). The decoder stores value streams COMPACT
// (parquet omits null rows), so a nullable column has size == K (present count).
static inline bool _fixed_eligible(size_t vsize, uint32_t n, bool nullable) {
    if (vsize == n) return true;                 // positional
    if (vsize < n && nullable) return true;      // compact + bitmap → scatter
    return false;                                // size > n, or compact w/o bitmap
}

// WP-6b: classify a decoded column for the direct (zero-copy-into-Draken) path.
// Excludes dict/RLE/list (no plain positional buffer). int32 widens to INT64;
// __int128 payload is DECIMAL128. The logical-type gate (date/timestamp and
// int-backed decimal stay on the IPC path) is applied separately by the caller.
static inline DirectKind direct_kind_for(const DecodedColumn& d) {
    if (!d.rep_levels.empty()) return DK_POOL;           // list
    const std::string& t = d.type;
    // Stage 4b: dict-encoded byte_array → direct DICT VARCHAR (before the generic
    // dict→pool exclusion below, which still sends NUMERIC dicts to the pool).
    // Requires the flat dict arena + a per-row code source, and not RLE.
    if ((t == "string" || t == "byte_array") && !d.string_dict_lens.empty() &&
        d.rle_str_lens.empty() &&
        (!d.dict_indices.empty() || !d.dict_codes_array.empty()))
        return DK_VARCHAR_DICT;
    // Numeric dict → §11 compressed (Dict-shaped) direct. Requires the dictionary
    // payload + a per-row code source (dict_indices for non-nullable via
    // prefer_dict, dict_codes_array for nullable), and NOT a mixed/plain chunk
    // (int*_values empty) nor the rle resolve-to-values path (rle_* empty).
    if ((t == "int64" || t == "int32") &&
        d.int64_values.empty() && d.int32_values.empty() &&
        d.rle_int64_values.empty() &&
        (!d.dict_int64_values.empty() || !d.dict_int32_values.empty()) &&
        (!d.dict_indices.empty() || !d.dict_codes_array.empty())) {
        // E33: an unsigned column preserves its exact declared width instead of
        // widening to INT64 (dict_int32_values/dict_int64_values already hold the
        // correct zero-extended magnitude — see decode_column.cpp's is_unsigned
        // branch — so this is purely a narrower/matching output tag, no new
        // corruption risk).
        if (d.is_unsigned) {
            switch (d.int_bit_width) {
                case 8:  return DK_UINT8_DICT;
                case 16: return DK_UINT16_DICT;
                case 32: return DK_UINT32_DICT;
                default: return DK_UINT64_DICT;
            }
        }
        return DK_INT64_DICT;
    }
    if (t == "float64" && d.float64_values.empty() && d.rle_float64_values.empty() &&
        !d.dict_float64_values.empty() &&
        (!d.dict_indices.empty() || !d.dict_codes_array.empty()))
        return DK_FLOAT64_DICT;
    if (t == "float32" && d.float32_values.empty() && d.rle_float64_values.empty() &&
        !d.dict_float32_values.empty() &&
        (!d.dict_indices.empty() || !d.dict_codes_array.empty()))
        return DK_FLOAT32_DICT;
    if (!d.dict_indices.empty() || !d.dict_codes_array.empty()) return DK_POOL;
    if (!d.rle_int64_values.empty() || !d.rle_float64_values.empty() ||
        !d.rle_str_lens.empty()) return DK_POOL;         // RLE skip-dense
    const uint32_t n = static_cast<uint32_t>(d.num_rows);
    const bool nullable = !d.valid_bits.empty();
    if (!d.int128_values.empty() && _fixed_eligible(d.int128_values.size(), n, nullable))
        return DK_DECIMAL128;
    if (t == "int64"   && _fixed_eligible(d.int64_values.size(),   n, nullable)) {
        if (d.is_unsigned) return DK_UINT64;  // physical int64, declared UINT64: exact width
        return DK_INT64;
    }
    if (t == "int32"   && _fixed_eligible(d.int32_values.size(),   n, nullable)) {
        // E33: preserve exact declared width for unsigned (int32_values already
        // holds the correct bit pattern regardless of interpretation — no widen
        // has happened yet at this point, so no corruption risk from narrowing).
        if (d.is_unsigned) {
            switch (d.int_bit_width) {
                case 8:  return DK_UINT8;
                case 16: return DK_UINT16;
                default: return DK_UINT32;
            }
        }
        return DK_INT64;
    }
    if (t == "float32" && _fixed_eligible(d.float32_values.size(), n, nullable)) return DK_FLOAT32;
    if (t == "float64" && _fixed_eligible(d.float64_values.size(), n, nullable)) return DK_FLOAT64;
    if (t == "boolean" && _fixed_eligible(d.boolean_values.size(), n, nullable)) return DK_BOOL;
    // Stage 4b: plain (non-dict, non-RLE, non-list) byte_array → direct dense
    // VARCHAR. string_values is one entry per row (incl null rows); only take the
    // positional case so the per-row slot build below is exact.
    if ((t == "string" || t == "byte_array") && d.string_values.size() == n)
        return DK_VARCHAR;
    return DK_POOL;
}

// Build a positional DrakenStringSlot array (+ long-string arena + validity) for a
// plain byte_array column, mirroring the Cython _build_string_plain EXACTLY: one
// slot per row; strings > STR_INLINE_MAX live in the arena (hash from the bytes),
// inline strings live in the slot; null rows get an init-null slot. string_values
// has one entry per row. Allocates via `alloc`; frees what it took on failure.
static inline bool build_direct_string_plain(const DecodedColumn& d,
                                             void* (*alloc)(size_t), void (*freefn)(void*),
                                             ColumnOut& out) {
    const uint32_t n = static_cast<uint32_t>(d.num_rows);
    const bool nullable = !d.valid_bits.empty();
    const uint8_t* nb = nullable ? d.valid_bits.data() : nullptr;
    const auto& vals = d.string_values;

    // Pass 1: arena bytes (long, non-null strings only).
    size_t total_arena = 0;
    for (uint32_t i = 0; i < n; ++i) {
        if (nullable && !((nb[i >> 3] >> (i & 7)) & 1)) continue;
        const size_t slen = (i < vals.size()) ? vals[i].size() : 0u;
        if (slen > STR_INLINE_MAX) total_arena += slen;
    }

    DrakenStringSlot* slots = static_cast<DrakenStringSlot*>(
        alloc((n ? n : 1u) * sizeof(DrakenStringSlot)));
    if (!slots) return false;
    uint8_t* arena = static_cast<uint8_t*>(alloc(total_arena ? total_arena : 1u));
    if (!arena) { freefn(slots); return false; }
    uint8_t* validity = nullptr;
    if (nullable) {
        validity = static_cast<uint8_t*>(alloc(d.valid_bits.size()));
        if (!validity) { freefn(arena); freefn(slots); return false; }
        std::memcpy(validity, d.valid_bits.data(), d.valid_bits.size());
    }

    // Pass 2: fill arena + build slots.
    uint32_t arena_pos = 0;
    for (uint32_t i = 0; i < n; ++i) {
        DrakenStringSlot* slot = &slots[i];
        if (nullable && !((nb[i >> 3] >> (i & 7)) & 1)) {
            str_init_null(slot);
            continue;
        }
        const std::string& s = vals[i];
        const uint8_t* sp = reinterpret_cast<const uint8_t*>(s.data());
        const uint32_t slen = static_cast<uint32_t>(s.size());
        if (slen > STR_INLINE_MAX) {
            std::memcpy(arena + arena_pos, sp, slen);
            draken_build_string_slot(slot, sp, slen, arena_pos);
            arena_pos += slen;
        } else {
            draken_build_string_slot(slot, sp, slen, arena_pos);  // inline; offset ignored
        }
    }

    out.data = slots;
    out.validity = validity;
    out.length = n;
    out.arena = arena;
    out.arena_len = arena_pos;
    return true;
}

// Build the DICT-VARCHAR direct buffers for a dict-encoded byte_array column,
// mirroring _build_string_dict (consumer) + serialize_string_dict (source): a
// compact value array of `dict_size` unique slots over a verbatim copy of
// string_dict_arena (slot k references offset string_dict_offsets[k], length =
// the offset delta — matching the deserializer), plus a per-row uint32 `codes`
// selection from dict_codes_array (packed code_width) or dict_indices (sparse,
// null rows → code 0). The result is data_length < length (dict shape) but
// accessed through the same uniform value[codes[i]] path.
static inline bool build_direct_string_dict(const DecodedColumn& d,
                                            void* (*alloc)(size_t), void (*freefn)(void*),
                                            ColumnOut& out) {
    const uint32_t n = static_cast<uint32_t>(d.num_rows);
    const uint32_t dict_size = static_cast<uint32_t>(d.string_dict_lens.size());
    const bool nullable = !d.valid_bits.empty();
    const uint8_t* nb = nullable ? d.valid_bits.data() : nullptr;
    const size_t arena_len = d.string_dict_arena.size();

    DrakenStringSlot* slots = static_cast<DrakenStringSlot*>(
        alloc((dict_size ? dict_size : 1u) * sizeof(DrakenStringSlot)));
    if (!slots) return false;
    uint8_t* arena = static_cast<uint8_t*>(alloc(arena_len ? arena_len : 1u));
    if (!arena) { freefn(slots); return false; }
    if (arena_len) std::memcpy(arena, d.string_dict_arena.data(), arena_len);
    for (uint32_t k = 0; k < dict_size; ++k) {
        const uint32_t s_off = d.string_dict_offsets[k];
        const uint32_t slen = (k + 1u < dict_size)
            ? (d.string_dict_offsets[k + 1u] - s_off)
            : (static_cast<uint32_t>(arena_len) - s_off);
        draken_build_string_slot(&slots[k], arena + s_off, slen, s_off);
    }

    uint32_t* codes = static_cast<uint32_t*>(alloc((n ? n : 1u) * sizeof(uint32_t)));
    if (!codes) { freefn(arena); freefn(slots); return false; }
    const uint8_t cw = d.code_width;
    if (!d.dict_codes_array.empty()) {
        const uint8_t* ca = d.dict_codes_array.data();
        for (uint32_t row = 0; row < n; ++row) {
            uint32_t c;
            if (cw == 1) { c = ca[row]; }
            else if (cw == 2) { uint16_t v; std::memcpy(&v, ca + row * 2, 2); c = v; }
            else { std::memcpy(&c, ca + row * 4, 4); }
            codes[row] = c;
        }
    } else {
        int32_t di = 0;
        for (uint32_t row = 0; row < n; ++row) {
            if (nullable && !((nb[row >> 3] >> (row & 7)) & 1))
                codes[row] = 0u;
            else
                codes[row] = static_cast<uint32_t>(d.dict_indices[di++]);
        }
    }

    uint8_t* validity = nullptr;
    if (nullable) {
        validity = static_cast<uint8_t*>(alloc(d.valid_bits.size()));
        if (!validity) { freefn(codes); freefn(arena); freefn(slots); return false; }
        std::memcpy(validity, d.valid_bits.data(), d.valid_bits.size());
    }

    out.data = slots;
    out.arena = arena;
    out.arena_len = arena_len;
    out.codes = codes;
    out.data_length = dict_size;
    out.validity = validity;
    out.length = n;
    out.dict_sorted = d.dict_ordered;
    return true;
}

// Build a numeric "compressed" (§11 Dict-shaped) direct column: a draken_alloc'd
// int64 dictionary (widening int32 dicts) + a uint32 per-row code selection.
// Mirrors build_direct_string_dict's code-source handling (dict_codes_array for
// nullable, dict_indices for non-nullable). On failure frees what it took.
static inline bool build_direct_int64_dict(const DecodedColumn& d,
                                           void* (*alloc)(size_t), void (*freefn)(void*),
                                           ColumnOut& out) {
    const uint32_t n = static_cast<uint32_t>(d.num_rows);
    const bool is32 = !d.dict_int32_values.empty();
    const uint32_t dsz = is32
        ? static_cast<uint32_t>(d.dict_int32_values.size())
        : static_cast<uint32_t>(d.dict_int64_values.size());
    const bool nullable = !d.valid_bits.empty();
    const uint8_t* nb = nullable ? d.valid_bits.data() : nullptr;

    int64_t* dict = static_cast<int64_t*>(alloc((dsz ? dsz : 1u) * sizeof(int64_t)));
    if (!dict) return false;
    if (is32) {
        for (uint32_t k = 0; k < dsz; ++k)
            dict[k] = static_cast<int64_t>(d.dict_int32_values[k]);
    } else if (dsz) {
        std::memcpy(dict, d.dict_int64_values.data(), static_cast<size_t>(dsz) * sizeof(int64_t));
    }

    uint32_t* codes = static_cast<uint32_t*>(alloc((n ? n : 1u) * sizeof(uint32_t)));
    if (!codes) { freefn(dict); return false; }
    const uint8_t cw = d.code_width;
    if (!d.dict_codes_array.empty()) {
        const uint8_t* ca = d.dict_codes_array.data();
        for (uint32_t row = 0; row < n; ++row) {
            uint32_t c;
            if (cw == 1) { c = ca[row]; }
            else if (cw == 2) { uint16_t v; std::memcpy(&v, ca + row * 2, 2); c = v; }
            else { std::memcpy(&c, ca + row * 4, 4); }
            codes[row] = c;
        }
    } else {
        int32_t di = 0;
        for (uint32_t row = 0; row < n; ++row) {
            if (nullable && !((nb[row >> 3] >> (row & 7)) & 1))
                codes[row] = 0u;
            else
                codes[row] = static_cast<uint32_t>(d.dict_indices[di++]);
        }
    }

    uint8_t* validity = nullptr;
    if (nullable) {
        validity = static_cast<uint8_t*>(alloc(d.valid_bits.size()));
        if (!validity) { freefn(codes); freefn(dict); return false; }
        std::memcpy(validity, d.valid_bits.data(), d.valid_bits.size());
    }

    out.data = dict;
    out.data_length = dsz;
    out.codes = codes;
    out.validity = validity;
    out.length = n;
    out.dict_sorted = d.dict_ordered;
    return true;
}

// E33 — unsigned "compressed" (Dict-shaped) direct column: a draken_alloc'd
// dictionary narrowed/reinterpreted to elem_bytes (1/2/4/8, matching the declared
// DRAKEN_UINT8/16/32/64 width) + a uint32 per-row code selection. Mirrors
// build_direct_int64_dict's code-source handling; unlike it, never widens — the
// source dict payload (dict_int32_values / dict_int64_values) already holds the
// correct unsigned magnitude bit-for-bit (E33's is_unsigned decode never sign-
// extends it), so this only narrows/reinterprets. One function parameterized by
// width rather than four near-duplicates, mirroring build_direct_int64_dict's
// existing is32/dsz branch structure.
static inline bool build_direct_uint_dict(const DecodedColumn& d, int elem_bytes,
                                          void* (*alloc)(size_t), void (*freefn)(void*),
                                          ColumnOut& out) {
    const uint32_t n = static_cast<uint32_t>(d.num_rows);
    const bool is32 = !d.dict_int32_values.empty();
    const uint32_t dsz = is32
        ? static_cast<uint32_t>(d.dict_int32_values.size())
        : static_cast<uint32_t>(d.dict_int64_values.size());
    const bool nullable = !d.valid_bits.empty();
    const uint8_t* nb = nullable ? d.valid_bits.data() : nullptr;

    uint8_t* dict = static_cast<uint8_t*>(alloc((dsz ? dsz : 1u) * static_cast<size_t>(elem_bytes)));
    if (!dict) return false;
    for (uint32_t k = 0; k < dsz; ++k) {
        const uint64_t v = is32 ? static_cast<uint64_t>(static_cast<uint32_t>(d.dict_int32_values[k]))
                                : static_cast<uint64_t>(d.dict_int64_values[k]);
        std::memcpy(dict + static_cast<size_t>(k) * elem_bytes, &v, static_cast<size_t>(elem_bytes));
    }

    uint32_t* codes = static_cast<uint32_t*>(alloc((n ? n : 1u) * sizeof(uint32_t)));
    if (!codes) { freefn(dict); return false; }
    const uint8_t cw = d.code_width;
    if (!d.dict_codes_array.empty()) {
        const uint8_t* ca = d.dict_codes_array.data();
        for (uint32_t row = 0; row < n; ++row) {
            uint32_t c;
            if (cw == 1) { c = ca[row]; }
            else if (cw == 2) { uint16_t v; std::memcpy(&v, ca + row * 2, 2); c = v; }
            else { std::memcpy(&c, ca + row * 4, 4); }
            codes[row] = c;
        }
    } else {
        int32_t di = 0;
        for (uint32_t row = 0; row < n; ++row) {
            if (nullable && !((nb[row >> 3] >> (row & 7)) & 1))
                codes[row] = 0u;
            else
                codes[row] = static_cast<uint32_t>(d.dict_indices[di++]);
        }
    }

    uint8_t* validity = nullptr;
    if (nullable) {
        validity = static_cast<uint8_t*>(alloc(d.valid_bits.size()));
        if (!validity) { freefn(codes); freefn(dict); return false; }
        std::memcpy(validity, d.valid_bits.data(), d.valid_bits.size());
    }

    out.data = dict;
    out.data_length = dsz;
    out.codes = codes;
    out.validity = validity;
    out.length = n;
    out.dict_sorted = d.dict_ordered;
    return true;
}

// Float "compressed" (Dict-shaped) direct column: a draken_alloc'd float64/float32
// dictionary (no widening) + a uint32 per-row code selection. Mirrors
// build_direct_int64_dict's code-source handling.
static inline bool build_direct_float_dict(const DecodedColumn& d, bool is_f32,
                                           void* (*alloc)(size_t), void (*freefn)(void*),
                                           ColumnOut& out) {
    const uint32_t n = static_cast<uint32_t>(d.num_rows);
    const uint32_t dsz = is_f32
        ? static_cast<uint32_t>(d.dict_float32_values.size())
        : static_cast<uint32_t>(d.dict_float64_values.size());
    const size_t elem = is_f32 ? sizeof(float) : sizeof(double);
    const bool nullable = !d.valid_bits.empty();
    const uint8_t* nb = nullable ? d.valid_bits.data() : nullptr;

    void* dict = alloc((dsz ? dsz : 1u) * elem);
    if (!dict) return false;
    if (dsz) {
        const void* src = is_f32 ? static_cast<const void*>(d.dict_float32_values.data())
                                 : static_cast<const void*>(d.dict_float64_values.data());
        std::memcpy(dict, src, static_cast<size_t>(dsz) * elem);
    }

    uint32_t* codes = static_cast<uint32_t*>(alloc((n ? n : 1u) * sizeof(uint32_t)));
    if (!codes) { freefn(dict); return false; }
    const uint8_t cw = d.code_width;
    if (!d.dict_codes_array.empty()) {
        const uint8_t* ca = d.dict_codes_array.data();
        for (uint32_t row = 0; row < n; ++row) {
            uint32_t c;
            if (cw == 1) { c = ca[row]; }
            else if (cw == 2) { uint16_t v; std::memcpy(&v, ca + row * 2, 2); c = v; }
            else { std::memcpy(&c, ca + row * 4, 4); }
            codes[row] = c;
        }
    } else {
        int32_t di = 0;
        for (uint32_t row = 0; row < n; ++row) {
            if (nullable && !((nb[row >> 3] >> (row & 7)) & 1))
                codes[row] = 0u;
            else
                codes[row] = static_cast<uint32_t>(d.dict_indices[di++]);
        }
    }

    uint8_t* validity = nullptr;
    if (nullable) {
        validity = static_cast<uint8_t*>(alloc(d.valid_bits.size()));
        if (!validity) { freefn(codes); freefn(dict); return false; }
        std::memcpy(validity, d.valid_bits.data(), d.valid_bits.size());
    }

    out.data = dict;
    out.data_length = dsz;
    out.codes = codes;
    out.validity = validity;
    out.length = n;
    return true;
}

// Build a positional Draken buffer (+ validity) for a byte-granular fixed-width
// column, mirroring the Cython _wrap_decoded_fixed scatter EXACTLY: when the
// value array is compact (K < N) and a validity bitmap is present, allocate a
// zero-filled N*elem buffer and copy each present value to its row position;
// otherwise the array is already positional and is copied wholesale. Allocates
// via `alloc` (the Draken allocator); on failure frees what it took via `freefn`
// and returns false. dec_* are filled for DK_DECIMAL128.
static inline bool build_direct_fixed(const DecodedColumn& d, DirectKind dk,
                                      void* (*alloc)(size_t), void (*freefn)(void*),
                                      ColumnOut& out) {
    const uint32_t n = static_cast<uint32_t>(d.num_rows);
    const bool nullable = !d.valid_bits.empty();

    uint32_t elem;
    const uint8_t* csrc;
    size_t compact_count;
    std::vector<int64_t> widened;        // int32→int64 staging
    std::vector<uint8_t> narrowed;        // E33: unsigned narrow/reinterpret staging
    if (dk == DK_UINT8 || dk == DK_UINT16 || dk == DK_UINT32 || dk == DK_UINT64) {
        // E33: preserve exact declared width — never widen. int32_values /
        // int64_values already hold the correct unsigned magnitude bit-for-bit (no
        // sign-extending cast has touched them), so this is a value-preserving
        // narrow (uint8/16/32) or a straight reinterpret (uint64), never lossy.
        // Assumes a little-endian host for the low-byte memcpy (matches every
        // other raw byte read in this decoder — ARM64/x86-64 both LE; RISC-V
        // targets are LE too).
        const int elem_bytes = (dk == DK_UINT8) ? 1 : (dk == DK_UINT16) ? 2 : (dk == DK_UINT32) ? 4 : 8;
        const bool src_is_32 = (d.type == "int32");
        const size_t count = src_is_32 ? d.int32_values.size() : d.int64_values.size();
        narrowed.resize(count * static_cast<size_t>(elem_bytes));
        for (size_t i = 0; i < count; ++i) {
            const uint64_t v = src_is_32
                ? static_cast<uint64_t>(static_cast<uint32_t>(d.int32_values[i]))
                : static_cast<uint64_t>(d.int64_values[i]);
            std::memcpy(narrowed.data() + i * elem_bytes, &v, static_cast<size_t>(elem_bytes));
        }
        csrc = narrowed.data();
        elem = static_cast<uint32_t>(elem_bytes); compact_count = count;
    } else if (dk == DK_INT64 && d.type == "int32") {
        widened.resize(d.int32_values.size());
        for (size_t i = 0; i < d.int32_values.size(); ++i)
            widened[i] = static_cast<int64_t>(d.int32_values[i]);
        csrc = reinterpret_cast<const uint8_t*>(widened.data());
        elem = 8; compact_count = widened.size();
    } else if (dk == DK_INT64) {
        csrc = reinterpret_cast<const uint8_t*>(d.int64_values.data());
        elem = 8; compact_count = d.int64_values.size();
    } else if (dk == DK_FLOAT32) {
        csrc = reinterpret_cast<const uint8_t*>(d.float32_values.data());
        elem = 4; compact_count = d.float32_values.size();
    } else if (dk == DK_FLOAT64) {
        csrc = reinterpret_cast<const uint8_t*>(d.float64_values.data());
        elem = 8; compact_count = d.float64_values.size();
    } else {  // DK_DECIMAL128
        csrc = reinterpret_cast<const uint8_t*>(d.int128_values.data());
        elem = 16; compact_count = d.int128_values.size();
    }

    const size_t full_bytes = static_cast<size_t>(n) * elem;
    void* pos = alloc(full_bytes ? full_bytes : 1);
    if (!pos) return false;

    if (nullable && compact_count < n) {
        std::memset(pos, 0, full_bytes);
        const uint8_t* nb = d.valid_bits.data();
        uint8_t* dst = static_cast<uint8_t*>(pos);
        size_t ci = 0;
        for (uint32_t r = 0; r < n; ++r) {
            if ((nb[r >> 3] >> (r & 7)) & 1) {
                if (ci < compact_count)
                    std::memcpy(dst + static_cast<size_t>(r) * elem, csrc + ci * elem, elem);
                ++ci;
            }
        }
    } else if (full_bytes) {
        std::memcpy(pos, csrc, full_bytes);
    }

    uint8_t* val = nullptr;
    if (nullable) {
        val = static_cast<uint8_t*>(alloc(d.valid_bits.size()));
        if (!val) { freefn(pos); return false; }
        std::memcpy(val, d.valid_bits.data(), d.valid_bits.size());
    }
    out.data = pos;
    out.validity = val;
    out.length = n;
    return true;
}

// Build a positional bit-packed DRAKEN_BOOL buffer (+ validity), combining
// serialize_bool's byte→bit packing with _wrap_decoded_bool's compact→positional
// scatter: boolean_values holds K present 0/1 bytes; emit N bits with present
// row r set iff its value is truthy (null rows stay 0, masked by validity).
static inline bool build_direct_bool(const DecodedColumn& d,
                                     void* (*alloc)(size_t), void (*freefn)(void*),
                                     ColumnOut& out) {
    const uint32_t n = static_cast<uint32_t>(d.num_rows);
    const bool nullable = !d.valid_bits.empty();
    const size_t pos_bytes = (static_cast<size_t>(n) + 7) >> 3;

    void* pos = alloc(pos_bytes ? pos_bytes : 1);
    if (!pos) return false;
    std::memset(pos, 0, pos_bytes ? pos_bytes : 1);
    uint8_t* dst = static_cast<uint8_t*>(pos);
    const uint8_t* bv = d.boolean_values.empty() ? nullptr : d.boolean_values.data();
    const size_t k = d.boolean_values.size();

    if (nullable) {
        const uint8_t* nb = d.valid_bits.data();
        size_t ci = 0;
        if (bv) {
            for (uint32_t r = 0; r < n; ++r) {
                if ((nb[r >> 3] >> (r & 7)) & 1) {
                    if (ci < k && (bv[ci] & 1)) dst[r >> 3] |= static_cast<uint8_t>(1u << (r & 7));
                    ++ci;
                }
            }
        }
    } else if (bv) {
        for (uint32_t r = 0; r < n && r < k; ++r)
            if (bv[r] & 1) dst[r >> 3] |= static_cast<uint8_t>(1u << (r & 7));
    }

    uint8_t* val = nullptr;
    if (nullable) {
        val = static_cast<uint8_t*>(alloc(d.valid_bits.size()));
        if (!val) { freefn(pos); return false; }
        std::memcpy(val, d.valid_bits.data(), d.valid_bits.size());
    }
    out.data = pos;
    out.validity = val;
    out.length = n;
    return true;
}

// Parse precision/scale from a "decimal(p,s)" logical_type string.
static inline void parse_decimal_ps(const std::string& lt, uint8_t& precision, uint8_t& scale) {
    precision = 38; scale = 0;
    size_t lp = lt.find('(');
    size_t cm = lt.find(',', lp);
    size_t rp = lt.find(')', cm);
    if (lp != std::string::npos && cm != std::string::npos && rp != std::string::npos) {
        precision = static_cast<uint8_t>(std::stoi(lt.substr(lp + 1, cm - lp - 1)));
        scale     = static_cast<uint8_t>(std::stoi(lt.substr(cm + 1, rp - cm - 1)));
    }
}

class ParquetIOPipeline {
 private:
    struct WorkItem {
        std::string path;
        int rg_idx;
        std::vector<std::string> column_names;
        std::vector<ColumnStats> column_stats;  // absolute file offsets
        std::vector<uint8_t> row_mask;           // empty = no mask (decode all rows)
        // docs/EXECUTION_TRACING_DESIGN.md: 0 unless tracing is armed at enqueue
        // time (enqueue_pending stamps both together) — decode_row_group treats
        // issued_ns == 0 as "don't record spans for this item", so a query that
        // starts untraced never pays for a corr_id allocation either.
        uint64_t issued_ns = 0;
        uint32_t corr_id = 0;
        uint32_t file_id = 0;  // draken_trace_intern_file(path); 0 == untraced
    };

    // Priority-capable pool (Gap #3 Phase 2b): same vendored BS::thread_pool template
    // as the plain BS::light_thread_pool this used to be (light_thread_pool IS
    // thread_pool<tp::none> — see BS_thread_pool.hpp), feature flag on. Decode tasks
    // submit at BS::pr::high so they don't queue behind exec-pool backlog when this
    // pool is SHARED with the execution engine (see owns_pool_ below). A pool with
    // only one priority ever used behaves identically to tp::none, so this is safe
    // for the standalone-constructor (self-owned, no injection) path too.
    std::shared_ptr<BS::thread_pool<BS::tp::priority>> decode_pool_;
    // True when this pipeline constructed decode_pool_ itself (the original,
    // standalone-rugo-compatible path) — safe to decode_pool_->wait() on shutdown,
    // since the pool is exclusive to this pipeline. False when the pool was INJECTED
    // (shared with other work, e.g. the execution engine's aggregate/sort tasks) —
    // decode_pool_->wait() would then block on unrelated tasks finishing, which is
    // wrong; shutdown must instead drain only THIS pipeline's own pending_work_.
    bool owns_pool_ = true;
    // Multi-producer (4 decode workers) / single-consumer (Python-side caller)
    // queue. Lock contention is negligible vs the IO/decode cost per item.
    std::deque<MorselRef> result_queue_;
    // Gap #3 Phase 2b (deadlock fix): claimable queue of not-yet-decoded work,
    // guarded by queue_mutex_. submit_row_group pushes the WorkItem HERE and
    // dispatches a pool ticket that merely CLAIMS from here (run_one_pending) —
    // the ticket is not the work. A puller blocked in wait_and_get_result can
    // then claim and decode an item ITSELF instead of waiting for a free pool
    // worker (which, when this pool is shared with the exec engine, may never
    // exist — the reentrant-pool deadlock). Whoever pops an item under the lock
    // owns it; the paired ticket that finds the queue empty is a no-op.
    std::deque<WorkItem> pending_items_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    size_t queue_capacity_;
    // Gap #3 Phase 2b (teardown safety): count of dispatched pool TICKETS that may
    // still touch `this`, decremented as each ticket's ABSOLUTE last action (after
    // the trailing queue_cv_.notify in decode_row_group). Distinct from pending_work_
    // (which counts undecoded ROWGROUPS): pending_work_ hitting 0 means "all work
    // counted", NOT "all tickets referencing `this` have finished". On the injected
    // (shared) pool we cannot decode_pool_->wait() to guarantee that, so wait_shutdown
    // spins on this reaching 0 — a spin (not a cv-wait) so there is no condvar for a
    // ticket to notify after ~ParquetIOPipeline has destroyed it (a notify-after-free
    // the old pending_work_ cv-wait was latently exposed to).
    std::atomic<int> tickets_inflight_{0};
    // Observability: how many row groups were decoded INLINE by a blocked puller
    // (via the wait_and_get_result help-loop) rather than by a pool worker. A
    // sustained non-zero value is the "exec starved of decode results" signal a
    // future WIP-rebalancing controller would react to.
    std::atomic<uint64_t> inline_decodes_{0};

    // Thread-local HTTP client: each BS worker thread owns its own HttpClient
    // and thus its own CURLSH connection cache. Eliminates CURL_LOCK_DATA_CONNECT
    // mutex contention when N threads simultaneously issue GCS range reads.
#ifdef RUGO_ENABLE_HTTP
    static HttpClient& tl_http_client() {
        thread_local HttpClient client;
        return client;
    }
#else
    // HTTP compiled out (standalone rugo): remote paths fail loud before any
    // client is needed (see read_range / decode_row_group).
    [[noreturn]] static void reject_remote_path(const std::string& path) {
        throw std::runtime_error(
            "rugo: remote paths (gs://, http://, https://) are not supported in "
            "this build — local filesystem only: " + path);
    }
#endif

    std::atomic<int> pending_work_{0};
    std::atomic<bool> shutdown_{false};
    // Cancellation (WP-8): set by cancel() when the consumer abandons the scan
    // early (e.g. LIMIT satisfied, or the result generator is dropped). Queued
    // but not-yet-started decode tasks observe this at the top of
    // decode_row_group and bail before doing any IO / decode / allocation, so
    // the engine stops paying for row groups it will never consume. A task
    // already mid-decode runs to completion (interrupting an in-flight decode
    // is out of scope) but its result is dropped at the enqueue guard.
    std::atomic<bool> cancelled_{false};
    std::atomic<uint64_t> cancelled_skips_{0};

    // docs/EXECUTION_TRACING_DESIGN.md: per-row-group correlation id source
    // (starts at 1; 0 == "no trace" sentinel on WorkItem::corr_id). trace_node_id_
    // is the plan-node identity this pipeline's spans should carry — NOT wired
    // from the compiler yet (each ParquetIOPipeline backs one scan, so a future
    // pass can set_trace_node_id() from the same identity compile_to_native
    // already tags the scan's OpStats with); spans record node_id=0 (untagged)
    // until that wiring lands. Documented gap, not a silent omission.
    std::atomic<uint32_t> next_trace_corr_id_{1};
    uint32_t trace_node_id_ = 0;

    // Destination pool for serialized columns. Set once before any submit via
    // set_pool_sink(); workers reserve+serialize+finalize through it.
    PoolSink pool_sink_;

    // Phase 2 dictionary decode-skip: per-column pushed predicate, keyed by
    // parquet column name. Set once before any submit (workers read it const, no
    // coordination). A worker decoding a dict-encoded column whose dictionary
    // satisfies none of the predicate skips its data pages. Empty = feature off.
    struct ColDictPred {
        int kind = -1;                       // see DictSkipPredicate::kind
        std::vector<int64_t>     int_vals;   // kind 0
        std::vector<std::string> str_vals;   // kinds 1..4
    };
    std::unordered_map<std::string, ColDictPred> dict_preds_;
    // Q24 latmat: pushed pass-1 predicate (opteryx callback). Set once before any
    // submit; workers read it const, no sync.
    Pass1Pred pass1_pred_;

    // Diagnostic counters for queue-contention investigation.
    std::atomic<uint64_t> spin_iterations_{0};
    std::atomic<uint64_t> enqueue_count_{0};
    std::atomic<size_t>   queue_high_watermark_{0};

    // IO/handoff observability counters (all relaxed atomics; aggregated at
    // diagnostics time, zero coordination cost in the hot path).
    //
    // http_request_count_: total individual byte ranges requested from remote
    // storage (one per column chunk), whether issued singly or in a batch.
    // http_lat_buckets_: histogram of fetch *operations* — one entry per
    // read_range() single GET or per get_many() batch — bucketed by wall time
    // (upper bounds in ms below, last bucket is overflow). Request count and
    // operation count differ once batching is in play: a 4-column batch is
    // 4 requests but 1 operation.
    // worker_blocked_ns_: time workers spend blocked on the back-pressure CV
    // waiting for the consumer to drain — the consumer-bound signal.
    // ipc_bytes_serialized_: bytes written by serialize_decoded_column — the
    // first of the handoff copies (serialize → pool commit → deserialize).
    // Only the pool path serializes, so this stays 0 for a direct-path scan
    // (WP-6b); it measures a handoff strategy, NOT the scan's IO volume.
    // bytes_fetched_: the scan's true IO volume — compressed bytes actually
    // pulled from storage (HTTP range GET / local pread), summed across every
    // decoded row group. Unlike the engine's rows*cols*8 bytes_in/bytes_out
    // estimate (src/cpp/engine/executor.hpp telem_nbytes), this is measured at
    // the point of transfer and so is unaffected by downstream filtering or
    // LIMIT truncation. Both scan paths (native + trampoline) route their row
    // groups through decode_one_row_group, so both accrue here.
    static constexpr int kHttpLatBuckets = 9;
    static constexpr uint64_t kHttpLatBoundsMs[kHttpLatBuckets - 1] =
        {1, 10, 50, 100, 250, 500, 1000, 5000};
    std::atomic<uint64_t> http_request_count_{0};
    std::atomic<uint64_t> http_fetch_ops_{0};
    std::atomic<uint64_t> http_lat_buckets_[kHttpLatBuckets] = {};
    std::atomic<uint64_t> worker_blocked_ns_{0};
    std::atomic<uint64_t> ipc_bytes_serialized_{0};
    std::atomic<uint64_t> bytes_fetched_{0};

    // Record one fetch operation covering n_requests byte ranges that took
    // elapsed_ns wall time. n_requests=1 for a single GET, N for a batch.
    void record_http_fetch(uint64_t elapsed_ns, uint64_t n_requests) {
        http_request_count_.fetch_add(n_requests, std::memory_order_relaxed);
        http_fetch_ops_.fetch_add(1, std::memory_order_relaxed);
        const uint64_t ms = elapsed_ns / 1000000ULL;
        int b = 0;
        while (b < kHttpLatBuckets - 1 && ms >= kHttpLatBoundsMs[b]) ++b;
        http_lat_buckets_[b].fetch_add(1, std::memory_order_relaxed);
    }

    /**
     * Convert gs://bucket/path to https://storage.googleapis.com/bucket/path.
     */
    static std::string gcs_to_https(const std::string& path) {
        // gs://bucket/object  →  https://storage.googleapis.com/bucket/object
        return "https://storage.googleapis.com/" + path.substr(5);
    }

    /**
     * Read a byte range from any supported path type.
     * Returns (bytes, elapsed_ns).
     */
    std::pair<std::vector<uint8_t>, uint64_t> read_range(
            const std::string& path, int64_t offset, int64_t size) {

        auto t0 = std::chrono::steady_clock::now();
        std::vector<uint8_t> bytes;
        bool is_remote = false;

#ifdef RUGO_ENABLE_HTTP
        if (path.substr(0, 5) == "gs://") {
            is_remote = true;
            std::string url = gcs_to_https(path);
            std::string range_hdr = "bytes=" + std::to_string(offset) +
                                    "-" + std::to_string(offset + size - 1);
            bytes = tl_http_client().get(url, {{"Range", range_hdr}});

        } else if (path.substr(0, 7) == "http://" || path.substr(0, 8) == "https://") {
            is_remote = true;
            std::string range_hdr = "bytes=" + std::to_string(offset) +
                                    "-" + std::to_string(offset + size - 1);
            bytes = tl_http_client().get(path, {{"Range", range_hdr}});

        } else
#else
        if (path.substr(0, 5) == "gs://" ||
            path.substr(0, 7) == "http://" || path.substr(0, 8) == "https://") {
            reject_remote_path(path);
        } else
#endif
        {
            // Local file: POSIX pread
            bytes.resize(size);
            int fd = open(path.c_str(), O_RDONLY);
            if (fd < 0) {
                throw std::runtime_error("Cannot open file: " + path);
            }
            ssize_t n = pread(fd, bytes.data(), size, offset);
            close(fd);
            if (n < 0) {
                throw std::runtime_error("Read error: " + path);
            }
            if (static_cast<size_t>(n) != static_cast<size_t>(size)) {
                throw std::runtime_error("Short read: " + path +
                    " (expected " + std::to_string(size) +
                    ", got " + std::to_string(n) + ")");
            }
        }

        uint64_t elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - t0).count();
        if (is_remote) {
            record_http_fetch(elapsed, 1);
        }
        return {std::move(bytes), elapsed};
    }

    /**
     * Build the fetch URL for a path: gs:// is rewritten to the GCS HTTPS
     * endpoint; http(s):// is used verbatim. Mirrors read_range()'s dispatch.
     */
    static std::string fetch_url_for(const std::string& path) {
        if (path.substr(0, 5) == "gs://") return gcs_to_https(path);
        return path;
    }

    // Gap #3 Phase 2b: a pool ticket's body — claim ONE pending item and decode it.
    // If the queue is empty (a helper already claimed everything), this is a no-op:
    // items and tickets are counted 1:1 per submit but not identity-paired, so some
    // tickets legitimately find nothing. pending_work_ is balanced by decode_row_group
    // (which the caller only reaches when it actually claimed an item).
    void run_one_pending() {
        WorkItem item;
        {
            std::lock_guard<std::mutex> lk(queue_mutex_);
            if (pending_items_.empty()) return;
            item = std::move(pending_items_.front());
            pending_items_.pop_front();
        }
        decode_row_group(item);
    }

    // Gap #3 Phase 2b: enqueue work + dispatch its claiming ticket. Ordering is
    // load-bearing: pending_work_++ FIRST (before the item is claimable, so a
    // helper/ticket that grabs it and runs decode_row_group's pending_work_-- can
    // never drive the counter negative), THEN publish the item, THEN count+dispatch
    // the ticket. The ticket decrements tickets_inflight_ as its LAST act, after
    // run_one_pending (hence after decode_row_group's trailing queue_cv_.notify) —
    // so tickets_inflight_==0 guarantees no ticket will touch `this` again.
    void enqueue_pending(WorkItem&& item) {
        // docs/EXECUTION_TRACING_DESIGN.md: stamp the gather's issue time (queue-
        // wait span start) and mint its correlation id here, once, rather than in
        // every submit_row_group() overload. Skipped entirely when tracing is
        // off — one relaxed atomic load, no clock read, no counter bump.
        if (draken_trace_enabled()) {
            item.issued_ns = draken_trace_now_ns();
            item.corr_id = next_trace_corr_id_.fetch_add(1, std::memory_order_relaxed);
            item.file_id = draken_trace_intern_file(item.path.data(), item.path.size());
        }
        pending_work_++;
        {
            std::lock_guard<std::mutex> lk(queue_mutex_);
            pending_items_.push_back(std::move(item));
        }
        queue_cv_.notify_one();  // wake a helper blocked in wait_and_get_result
        tickets_inflight_.fetch_add(1, std::memory_order_relaxed);
        decode_pool_->detach_task([this]() {
            run_one_pending();
            tickets_inflight_.fetch_sub(1, std::memory_order_release);
        }, BS::pr::high);
    }

    // Probe an in-memory bloom filter for every needle of a pushed equality/IN
    // predicate. Returns true only when EVERY needle is provably absent — i.e.
    // this row group cannot match the conjunct, so its decode can be skipped.
    // `physical_type` encodes the needle bytes identically to the writer's
    // bloom_hashes (int32=4 LE, int64=8 LE, byte_array=raw). Only kind 0 (int
    // membership) and kind 1 (str membership) can consult a bloom; LIKE kinds
    // and unencodable types return false (keep the row group). Any probe error
    // fails OPEN (false) — a bloom must never drop a live row.
    static bool bloom_needles_all_absent(
            const uint8_t* bloom_data, size_t bloom_len, int kind,
            const std::vector<int64_t>* int_vals,
            const std::vector<std::string>* str_vals,
            const std::string& physical_type) {
        std::string vbytes;
        try {
            if (kind == 0 && int_vals != nullptr) {
                if (int_vals->empty()) return false;
                for (int64_t v : *int_vals) {
                    if (physical_type == "int64") {
                        vbytes.assign(reinterpret_cast<const char*>(&v), 8);
                    } else if (physical_type == "int32") {
                        int32_t v32 = static_cast<int32_t>(v);
                        if (static_cast<int64_t>(v32) != v) return false;
                        vbytes.assign(reinterpret_cast<const char*>(&v32), 4);
                    } else {
                        return false;  // unencodable physical type → keep
                    }
                    if (TestBloomFilterBytes(bloom_data, bloom_len, vbytes))
                        return false;  // may be present → keep
                }
                return true;  // every needle provably absent
            }
            if (kind == 1 && str_vals != nullptr) {
                if (str_vals->empty() || physical_type != "byte_array") return false;
                for (const std::string& s : *str_vals) {
                    if (TestBloomFilterBytes(bloom_data, bloom_len, s))
                        return false;
                }
                return true;
            }
        } catch (...) {
            return false;  // any parse/probe error → keep the row group
        }
        return false;  // LIKE / unknown kind → cannot prune
    }

    void decode_row_group(const WorkItem& item) {
        // WP-8 cancel: a queued task whose work is no longer wanted bails here,
        // before any IO / decode / allocation. Nothing was reserved yet, so
        // there is nothing to release; just balance the pending-work ledger and
        // wake anyone waiting on the queue.
        if (cancelled_.load(std::memory_order_relaxed)) {
            cancelled_skips_.fetch_add(1, std::memory_order_relaxed);
            pending_work_--;
            queue_cv_.notify_one();
            return;
        }

        // docs/EXECUTION_TRACING_DESIGN.md: t_dequeue closes the queue-wait span
        // opened at enqueue_pending's issued_ns — this worker is now actually
        // starting on the item, having claimed it from pending_items_ (whether
        // via a pool ticket's run_one_pending or a blocked puller's inline-help
        // path in wait_and_get_result; both funnel through here). Zero-cost when
        // item.issued_ns == 0 (tracing was off at enqueue time).
        const uint64_t _tr_t_dequeue =
            item.issued_ns != 0 ? draken_trace_now_ns() : 0;

        MorselRef result;
        result.path = item.path;
        result.rg_idx = item.rg_idx;
        result.column_names = item.column_names;
        result.free_fn = pool_sink_.draken_free;   // owns abandoned direct buffers
        result.success = true;

        uint64_t total_read_ns = 0;
        uint64_t total_decode_ns = 0;

        // For local files, mmap the full column-chunk extent of the row group
        // once rather than open/pread/close per column.  Eliminates per-column
        // heap allocation and gives the kernel a sequential-prefetch hint via
        // MADV_SEQUENTIAL.  Falls back to read_range() for HTTP/GCS.
        bool is_local = item.path.rfind("gs://",   0) != 0 &&
                        item.path.rfind("http://",  0) != 0 &&
                        item.path.rfind("https://", 0) != 0;

        void*   mmap_base   = MAP_FAILED;
        size_t  mmap_len    = 0;
        int64_t mmap_offset = 0;  // page-aligned file offset of the mapping

        if (is_local && !item.column_stats.empty()) {
            int64_t span_min = INT64_MAX, span_max = 0;
            for (const auto& cs : item.column_stats) {
                int64_t base = cs.data_page_offset;
                if (cs.dictionary_page_offset >= 0 && cs.dictionary_page_offset < base)
                    base = cs.dictionary_page_offset;
                int64_t end = base + cs.total_compressed_size;
                if (base < span_min) span_min = base;
                if (end   > span_max) span_max = end;
            }
            long page_size  = sysconf(_SC_PAGESIZE);
            mmap_offset     = (span_min / page_size) * page_size;
            mmap_len        = static_cast<size_t>(span_max - mmap_offset);

            auto t_map = std::chrono::steady_clock::now();
            int fd = open(item.path.c_str(), O_RDONLY | O_CLOEXEC);
            if (fd >= 0) {
                mmap_base = mmap(nullptr, mmap_len, PROT_READ, MAP_PRIVATE, fd, mmap_offset);
                close(fd);
                // No madvise: let the OS manage readahead.
                if (mmap_base == MAP_FAILED)
                    mmap_base = MAP_FAILED;
            }
            total_read_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - t_map).count();
        }

        // Precompute mask pointer once — shared across all columns in this row group.
        const uint8_t* mask_ptr = item.row_mask.empty() ? nullptr : item.row_mask.data();

        // Per-column base offset (dictionary page if it precedes the data page,
        // else the data page). Computed once here and reused for both the
        // remote batch request and the in-loop chunk slicing.
        std::vector<int64_t> base_offsets(item.column_stats.size());
        for (size_t i = 0; i < item.column_stats.size(); ++i) {
            int64_t base = item.column_stats[i].data_page_offset;
            if (item.column_stats[i].dictionary_page_offset >= 0 &&
                item.column_stats[i].dictionary_page_offset < base) {
                base = item.column_stats[i].dictionary_page_offset;
            }
            base_offsets[i] = base;
        }

        // Remote bloom decode-skip: for a column carrying a pushed equality/IN
        // predicate whose bloom filter sits immediately before its column chunk
        // (adjacent layout), extend that column's fetch backwards to swallow the
        // bloom bytes. They ride in the same GET we already issue for the chunk,
        // so testing them costs no extra round trip — a probe that proves the
        // needle absent lets us skip the whole row group's decode. Gated on the
        // three conditions: (1) bloom adjacent to the chunk, (2) a pushed =/IN
        // predicate on the column, (3) the row group survived min/max (implicit —
        // manifest pruning already dropped the rest). Remote-only: local files
        // are bloom-pruned at manifest time and never reach here excluded. Not
        // applied under a row_mask (pass-2 late materialization already has
        // survivors). bloom_prefix[i] == 0 means "no bloom in column i's fetch".
        std::vector<int64_t> bloom_prefix(item.column_stats.size(), 0);
        if (!is_local && item.row_mask.empty() && !dict_preds_.empty()) {
            for (size_t i = 0; i < item.column_stats.size(); ++i) {
                const auto& cs = item.column_stats[i];
                if (cs.bloom_offset < 0 || cs.bloom_length <= 0) continue;
                if (cs.bloom_offset + cs.bloom_length != base_offsets[i]) continue;  // not adjacent
                auto it = dict_preds_.find(cs.name);
                if (it == dict_preds_.end()) continue;
                if (it->second.kind != 0 && it->second.kind != 1) continue;  // only =/IN
                bloom_prefix[i] = cs.bloom_length;
            }
        }

        // Remote batch prefetch: for HTTP/GCS, fetch every column chunk for
        // this row group concurrently in a single get_many() call rather than
        // one blocking GET per column (which serialized C round-trips per row
        // group). Local files use mmap (above) or per-column pread (in-loop).
        // The path is already a signed/self-authenticating URL when needed, so
        // no auth header is attached here.
        const bool remote = !is_local;
#ifndef RUGO_ENABLE_HTTP
        if (remote)
            reject_remote_path(item.path);
#endif
        std::vector<std::vector<uint8_t>> remote_buffers;

        try {
#ifdef RUGO_ENABLE_HTTP
            if (remote && !item.column_stats.empty()) {
                const std::string url = fetch_url_for(item.path);
                std::vector<std::pair<std::string, std::map<std::string, std::string>>> reqs;
                reqs.reserve(item.column_stats.size());
                for (size_t i = 0; i < item.column_stats.size(); ++i) {
                    int64_t chunk_size = item.column_stats[i].total_compressed_size;
                    // Extend the start backwards by bloom_prefix[i] (0 unless this
                    // column's adjacent bloom is being fetched for a decode-skip
                    // probe). The chunk end is unchanged, so the bloom rides in
                    // front of the chunk in the same range.
                    int64_t fetch_start = base_offsets[i] - bloom_prefix[i];
                    std::string range_hdr = "bytes=" + std::to_string(fetch_start) +
                        "-" + std::to_string(base_offsets[i] + chunk_size - 1);
                    reqs.emplace_back(url, std::map<std::string, std::string>{{"Range", range_hdr}});
                }
                auto t_fetch = std::chrono::steady_clock::now();
                remote_buffers = tl_http_client().get_many(reqs);
                uint64_t batch_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - t_fetch).count();
                total_read_ns += batch_ns;
                // One fetch operation covering reqs.size() concurrent ranges.
                record_http_fetch(batch_ns, reqs.size());
            }
#endif

            // Reused across the columns of this row group: DecodeColumnFromChunk
            // resets it at entry and retains its vector capacity, so per-column
            // decode stops re-mallocing the DecodedColumn's ~25 buffers. Function-
            // local → one per worker invocation, no cross-thread sharing.
            DecodedColumn scratch;
            for (size_t i = 0; i < item.column_stats.size(); ++i) {
                const auto& col_stats = item.column_stats[i];

                int64_t base_offset = base_offsets[i];
                int64_t chunk_size = col_stats.total_compressed_size;

                ColumnStats adjusted = col_stats;
                adjusted.data_page_offset -= base_offset;
                if (adjusted.dictionary_page_offset >= 0)
                    adjusted.dictionary_page_offset -= base_offset;

                // prefer_dict: keep the dictionary (compressed/Dict shape) for
                // plain int32/int64 dict columns when not masking. Masked (pass-2)
                // decode stays on the existing path — it only touches survivor rows
                // and the masked-dict compaction is out of scope here. The logical
                // gate (date/timestamp/decimal stay pool) is enforced via lt below.
                const std::string& pt = col_stats.physical_type;
                const std::string& cl = col_stats.logical_type;
                // Roll-out: plain int + int-backed TIMESTAMP/DATE + float. Int/temporal
                // coercions are shape-preserving (retag / dict-only reinterpret), so a
                // Dict-shaped int64 stays Dict; float needs no coercion. int-backed
                // DECIMAL stays pool (reinterpret trap). Phase 2 membership-skip stays
                // int-only (decode gate) — float equality membership is out of scope.
                const bool prefer_dict =
                    (mask_ptr == nullptr) &&
                    col_stats.dictionary_page_offset >= 0 &&
                    (((pt == "int64" || pt == "int32") &&
                      (cl.empty() || cl == "int64" || cl == "int32" ||
                       cl.rfind("timestamp", 0) == 0 || cl.rfind("date", 0) == 0)) ||
                     ((pt == "float64" || pt == "float32") &&
                      (cl.empty() || cl == "float64" || cl == "float32")));

                // Phase 2: pushed dictionary decode-skip predicate for this column
                // (if any). Independent of prefer_dict — the probe only needs the
                // dictionary (decoded before any data page), not the dict-shaped
                // surviving representation.
                DictSkipPredicate skip;
                const DictSkipPredicate* skip_ptr = nullptr;
                if (mask_ptr == nullptr && !dict_preds_.empty()) {
                    auto nit = dict_preds_.find(col_stats.name);
                    if (nit != dict_preds_.end()) {
                        skip.kind = nit->second.kind;
                        skip.int_vals = &nit->second.int_vals;
                        skip.str_vals = &nit->second.str_vals;
                        skip_ptr = &skip;
                    }
                }

                DecodedColumn& decoded = scratch;   // reused; reset at decode entry
                if (mmap_base != MAP_FAILED) {
                    // Zero-copy: slice directly into the mmap — no heap allocation.
                    const uint8_t* chunk_ptr =
                        static_cast<const uint8_t*>(mmap_base) + (base_offset - mmap_offset);
                    auto t_dec = std::chrono::steady_clock::now();
                    DecodeColumnFromChunk(scratch,
                        chunk_ptr, static_cast<size_t>(chunk_size), &adjusted, mask_ptr, prefer_dict, skip_ptr);
                    total_decode_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - t_dec).count();
                    result.bytes_fetched += chunk_size;
                } else if (remote) {
                    // Batch-prefetched above: decode straight from the buffer.
                    // When bloom_prefix[i] > 0 the buffer carries the column's
                    // adjacent bloom filter in front of the chunk (bpre bytes).
                    const std::vector<uint8_t>& raw = remote_buffers[i];
                    const size_t bpre = static_cast<size_t>(bloom_prefix[i]);
                    result.bytes_fetched += chunk_size + static_cast<int64_t>(bpre);
                    // Bloom decode-skip: the adjacent bloom proves this row group
                    // holds none of the pushed needles → zero surviving rows.
                    // Skip decode of this and the remaining columns, exactly like
                    // the dictionary decode-skip (dict_all_filtered) below.
                    if (bpre > 0 && skip_ptr != nullptr &&
                        bloom_needles_all_absent(raw.data(), bpre, skip.kind,
                                                 skip.int_vals, skip.str_vals,
                                                 col_stats.physical_type)) {
                        result.empty_filtered = true;
                        result.empty_rows =
                            col_stats.num_values >= 0 ? col_stats.num_values : 0;
                        break;
                    }
                    auto t_dec = std::chrono::steady_clock::now();
                    DecodeColumnFromChunk(scratch,
                        raw.data() + bpre, raw.size() - bpre, &adjusted, mask_ptr, prefer_dict, skip_ptr);
                    total_decode_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - t_dec).count();
                } else {
                    // Local file whose mmap failed: per-column pread fallback.
                    auto [raw_bytes, read_ns] = read_range(item.path, base_offset, chunk_size);
                    result.bytes_fetched += chunk_size;
                    total_read_ns += read_ns;
                    auto t_dec = std::chrono::steady_clock::now();
                    DecodeColumnFromChunk(scratch,
                        raw_bytes.data(), raw_bytes.size(), &adjusted, mask_ptr, prefer_dict, skip_ptr);
                    total_decode_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - t_dec).count();
                }

                if (!decoded.success) {
                    result.success = false;
                    // Surface the specific reason (e.g. a decompression error)
                    // verbatim when the decoder captured one; otherwise fall back
                    // to the generic message for honest "unsupported shape"
                    // rejections that carry no reason.
                    if (!decoded.error_message.empty()) {
                        result.error = "Decode failed for column '" + col_stats.name +
                                       "': " + decoded.error_message;
                    } else {
                        result.error = "Decode failed for column: " + col_stats.name;
                    }
                    break;
                }

                // Phase 2 fast-exit: a pushed-conjunct equality column whose
                // dictionary lacks every needle means the WHOLE row group yields
                // zero rows. Flag it and stop decoding the remaining columns — the
                // consumer skips this row group entirely (no wrap/filter/morsel).
                if (decoded.dict_all_filtered) {
                    result.empty_filtered = true;
                    result.empty_rows = decoded.num_rows;
                    break;
                }

                ColumnOut cout;
                // Direct-path logical gate (Stage 4a). Plain numerics + boolean +
                // int128 DECIMAL128 go direct as their physical kind. DATE and
                // TIMESTAMP decode to a physical int32/int64 stream and are
                // direct-eligible as DK_INT64; the consumer's schema-driven
                // coercion (_coerce_vectors / _coerce_logical_types) reinterprets
                // that INT64 vector to date32/timestamp identically to the pool
                // representation (verified: make q / tpch / clickbench).
                //
                // INT-BACKED DECIMAL (precision<=18) deliberately stays on the
                // pool path: the pool serializer emits a representation the
                // deserializer turns into a directly-usable DECIMAL, which a raw
                // direct INT64 + downstream reinterpret does NOT reproduce
                // (tpch Q01 dec_mul type error — the historic decimal trap).
                //
                // Stage 4b: PLAIN byte_array → direct dense VARCHAR. Dict/RLE/list
                // strings stay pool (direct_kind_for returns DK_POOL for them); a
                // dense VARCHAR is concat/CASE-compatible with a pool dict VARCHAR
                // (§11 uniform access — proven by test_string_dense_dict_concat_compat).
                const std::string& lt = col_stats.logical_type;
                const bool safe_logical =
                    lt.empty() || lt == "int64" || lt == "int32" ||
                    // A1: signed narrow ints (int8/int16) widen to DK_INT64 on decode
                    // exactly like int32 — no consumer-side coercion — so they are
                    // direct-eligible. Without this they fell to DK_POOL, which the
                    // native scan Source cannot decode for a numeric column (only the
                    // trampoline's pool deserializer could), so admitting them to the
                    // native scan raised "unsupported column encoding".
                    lt == "int8" || lt == "int16" ||
                    lt == "float64" || lt == "float32" || lt == "boolean" ||
                    lt.rfind("date", 0) == 0 || lt.rfind("timestamp", 0) == 0 ||
                    lt.rfind("time[", 0) == 0 ||  // WP-11: TIME is an int32/int64 stream,
                                                  // decoded as plain INT64 (the consumer
                                                  // models no TIME coercion) — direct-eligible
                                                  // exactly like date/timestamp.
                    lt.rfind("uint", 0) == 0 ||  // E33: uint8/16/32/64 direct kinds
                    (lt.rfind("decimal", 0) == 0 && !decoded.int128_values.empty());
                DirectKind dk = pool_sink_.draken_alloc ? direct_kind_for(decoded) : DK_POOL;
                // The logical-type gate applies only to FIXED-WIDTH direct (date/
                // timestamp OK; int-backed decimal stays pool). DK_VARCHAR needs no
                // reinterpret, so it bypasses the gate.
                if (dk != DK_POOL && dk != DK_VARCHAR && dk != DK_VARCHAR_DICT && !safe_logical)
                    dk = DK_POOL;

                if (dk != DK_POOL) {
                    // Direct path: the worker builds the positional Draken
                    // buffer (+ validity), doing any compact→positional scatter
                    // itself; the consumer wraps it with zero copy.
                    bool ok;
                    if (dk == DK_BOOL)
                        ok = build_direct_bool(decoded, pool_sink_.draken_alloc, pool_sink_.draken_free, cout);
                    else if (dk == DK_VARCHAR)
                        ok = build_direct_string_plain(decoded, pool_sink_.draken_alloc, pool_sink_.draken_free, cout);
                    else if (dk == DK_VARCHAR_DICT)
                        ok = build_direct_string_dict(decoded, pool_sink_.draken_alloc, pool_sink_.draken_free, cout);
                    else if (dk == DK_INT64_DICT)
                        ok = build_direct_int64_dict(decoded, pool_sink_.draken_alloc, pool_sink_.draken_free, cout);
                    else if (dk == DK_FLOAT64_DICT)
                        ok = build_direct_float_dict(decoded, false, pool_sink_.draken_alloc, pool_sink_.draken_free, cout);
                    else if (dk == DK_FLOAT32_DICT)
                        ok = build_direct_float_dict(decoded, true, pool_sink_.draken_alloc, pool_sink_.draken_free, cout);
                    else if (dk == DK_UINT8_DICT)
                        ok = build_direct_uint_dict(decoded, 1, pool_sink_.draken_alloc, pool_sink_.draken_free, cout);
                    else if (dk == DK_UINT16_DICT)
                        ok = build_direct_uint_dict(decoded, 2, pool_sink_.draken_alloc, pool_sink_.draken_free, cout);
                    else if (dk == DK_UINT32_DICT)
                        ok = build_direct_uint_dict(decoded, 4, pool_sink_.draken_alloc, pool_sink_.draken_free, cout);
                    else if (dk == DK_UINT64_DICT)
                        ok = build_direct_uint_dict(decoded, 8, pool_sink_.draken_alloc, pool_sink_.draken_free, cout);
                    else
                        ok = build_direct_fixed(decoded, dk, pool_sink_.draken_alloc, pool_sink_.draken_free, cout);
                    if (!ok) {
                        result.success = false;
                        result.error = "draken_alloc failed for column: " + col_stats.name;
                        break;
                    }
                    cout.direct_kind = dk;
                    if (dk == DK_DECIMAL128)
                        parse_decimal_ps(col_stats.logical_type, cout.dec_precision, cout.dec_scale);
                    // Direct path emits no IPC bytes — ipc_bytes_serialized only
                    // accrues for pool-path columns, so its drop is the WP-6b signal.
                } else {
                    // Pool path (WP-6a): serialize straight into a MemoryPool
                    // region — no heap buffer, no consumer-side commit() copy.
                    // Parse precision/scale from the logical_type string
                    // (e.g. "decimal(15,2)") for DECIMAL128 columns.
                    uint8_t dec_precision = 38, dec_scale = 0;
                    if (!decoded.int128_values.empty()) {
                        const std::string& lt = col_stats.logical_type;
                        size_t lp = lt.find('(');
                        size_t cm = lt.find(',', lp);
                        size_t rp = lt.find(')', cm);
                        if (lp != std::string::npos && cm != std::string::npos && rp != std::string::npos) {
                            dec_precision = static_cast<uint8_t>(std::stoi(lt.substr(lp + 1, cm - lp - 1)));
                            dec_scale     = static_cast<uint8_t>(std::stoi(lt.substr(cm + 1, rp - cm - 1)));
                        }
                    }
                    // Exact size first (count pass), then one write pass into the
                    // reserved bytes; the two cannot disagree (same code path).
                    size_t sz = rugo::serialized_size(decoded, dec_precision, dec_scale);
                    void* dst = nullptr;
                    int64_t ref_id = pool_sink_.reserve
                        ? pool_sink_.reserve(pool_sink_.ctx, static_cast<int64_t>(sz), &dst)
                        : -1;
                    if (ref_id < 0 || dst == nullptr) {
                        result.success = false;
                        result.error = "MemoryPool exhausted serializing column: " + col_stats.name;
                        break;
                    }
                    size_t written = rugo::serialize_decoded_column_into(
                        decoded, static_cast<uint8_t*>(dst), dec_precision, dec_scale);
                    pool_sink_.finalize(pool_sink_.ctx, ref_id, static_cast<int64_t>(written));
                    ipc_bytes_serialized_.fetch_add(written, std::memory_order_relaxed);
                    cout.direct_kind = DK_POOL;
                    cout.ref_id = ref_id;
                }
                result.columns.push_back(cout);
            }
        } catch (const std::exception& e) {
            result.success = false;
            result.error = e.what();
        }

        if (mmap_base != MAP_FAILED)
            munmap(mmap_base, mmap_len);

        result.read_ns = total_read_ns;
        result.decode_ns = total_decode_ns;
        // Accrue this row group's transferred bytes onto the pipeline. Done here,
        // not by the consumer, because the native scan Source drops MorselRef's
        // telemetry fields on the floor — accumulating at the producer keeps both
        // scan paths honest with one counter.
        if (result.bytes_fetched > 0)
            bytes_fetched_.fetch_add(static_cast<uint64_t>(result.bytes_fetched),
                                     std::memory_order_relaxed);

        // docs/EXECUTION_TRACING_DESIGN.md: reconstruct this row group's spans
        // from the timestamps/durations already computed above — no additional
        // clock reads inside the fetch/decode loop itself. TC_QUEUE_WAIT is the
        // real gap (issued -> a worker actually claimed it); TC_IO_REQUEST and
        // TC_DECODE are total_read_ns/total_decode_ns placed back-to-back after
        // it. There is currently no distinct "bytes arrived but decode hasn't
        // started" stage in this implementation (fetch and decode happen
        // column-by-column in the same loop, immediately adjacent) — so
        // TC_BUFFER_RESIDENT is NOT emitted here; it would always read ~0 and
        // add noise rather than signal. It becomes meaningful if a real
        // buffering stage (e.g. a bounded pending-decode queue) is introduced.
        if (item.issued_ns != 0) {
            const auto _tr_idx = BS::this_thread::get_index();
            const uint16_t _tr_worker =
                _tr_idx.has_value() ? static_cast<uint16_t>(*_tr_idx) : 0xFFFFu;
            const uint32_t _tr_rg = static_cast<uint32_t>(item.rg_idx);
            // Row-group row count from the manifest metadata (stable, known
            // before decode starts) — every column chunk in one row group
            // shares it, so column_stats[0] is representative.
            const uint32_t _tr_rows =
                (!item.column_stats.empty() && item.column_stats[0].num_values >= 0)
                    ? static_cast<uint32_t>(item.column_stats[0].num_values) : 0;
            draken_trace_record(DRAKEN_TC_QUEUE_WAIT, trace_node_id_,
                item.corr_id, _tr_rg, _tr_worker, item.issued_ns, _tr_t_dequeue,
                0, 0, 0, item.file_id);
            const uint64_t t_read_end = _tr_t_dequeue + total_read_ns;
            if (total_read_ns > 0)
                draken_trace_record(DRAKEN_TC_IO_REQUEST, trace_node_id_,
                    item.corr_id, _tr_rg, _tr_worker, _tr_t_dequeue, t_read_end,
                    0, static_cast<uint32_t>(result.bytes_fetched), 0, item.file_id);
            if (total_decode_ns > 0)
                draken_trace_record(DRAKEN_TC_DECODE, trace_node_id_,
                    item.corr_id, _tr_rg, _tr_worker, t_read_end,
                    t_read_end + total_decode_ns, _tr_rows, 0, 0, item.file_id);
        }

        // Q24 latmat: evaluate the pushed pass-1 predicate on this worker thread
        // (parallel across the decode pool) and attach the survivor bitmap. No-op if
        // no predicate pushed / unsupported shape → consumer falls back to serial.
        if (pass1_pred_.fn != nullptr)
            pass1_run_predicate(result, pass1_pred_);
        // Apply soft back-pressure: if the consumer is far behind, block
        // on the condition variable until it drains rather than spin-yielding.
        {
            auto t_bp = std::chrono::steady_clock::now();
            std::unique_lock<std::mutex> lk(queue_mutex_);
            queue_cv_.wait(lk, [this]() {
                return result_queue_.size() < queue_capacity_
                    || shutdown_.load(std::memory_order_relaxed)
                    || cancelled_.load(std::memory_order_relaxed);
            });
            worker_blocked_ns_.fetch_add(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - t_bp).count(),
                std::memory_order_relaxed);
            // Drop the result if cancelled or shutting down: a result decoded
            // after cancel will never be consumed. The MorselRef destructor
            // frees any direct Draken buffers it holds; pool segments stay
            // reserved until the per-pipeline pool is torn down at close().
            if (!shutdown_.load(std::memory_order_relaxed)
                    && !cancelled_.load(std::memory_order_relaxed)) {
                result_queue_.push_back(std::move(result));
                size_t sz = result_queue_.size();
                enqueue_count_.fetch_add(1, std::memory_order_relaxed);
                size_t prev = queue_high_watermark_.load(std::memory_order_relaxed);
                while (sz > prev &&
                       !queue_high_watermark_.compare_exchange_weak(
                           prev, sz, std::memory_order_relaxed)) {}
            }
        }
        pending_work_--;
        queue_cv_.notify_one();
    }

 public:
    // Standalone path (unchanged behaviour): self-constructs an exclusive pool.
    // Kept for the standalone rugo wheel and any caller that doesn't inject one —
    // rugo/ stays opteryx-free; nothing here depends on the execution engine.
    ParquetIOPipeline(int decode_workers = 4,
                      size_t result_queue_capacity = 256)
        : decode_pool_(std::make_shared<BS::thread_pool<BS::tp::priority>>(decode_workers)),
          owns_pool_(true),
          queue_capacity_(result_queue_capacity) {}

    // Injection path (Gap #3 Phase 2b): shares an externally-owned pool (e.g. the
    // execution engine's exec pool) instead of constructing its own. The caller
    // retains ownership and lifetime responsibility for `pool` — it must outlive
    // this pipeline. wait_shutdown() will NOT call pool->wait() (see owns_pool_).
    ParquetIOPipeline(std::shared_ptr<BS::thread_pool<BS::tp::priority>> pool,
                      size_t result_queue_capacity = 256)
        : decode_pool_(std::move(pool)),
          owns_pool_(false),
          queue_capacity_(result_queue_capacity) {}

    ~ParquetIOPipeline() {
        wait_shutdown();
    }

    // Wire the destination MemoryPool. Must be called before any submit; the
    // workers serialize decoded columns directly into pool-reserved regions.
    void set_pool_sink(PoolSink sink) {
        pool_sink_ = sink;
    }

    // docs/EXECUTION_TRACING_DESIGN.md: tag this pipeline's trace spans with the
    // plan-node identity of the scan it backs (id space is opteryx::engine::
    // Engine's node_id counter, shared for the query — the caller passes the
    // same id compile_to_native tagged the scan's OpStats with). 0 (default) =
    // untagged. Call before any submit; not currently wired from the compiler.
    void set_trace_node_id(uint32_t node_id) {
        trace_node_id_ = node_id;
    }

    // Phase 2: register a pushed dictionary decode-skip predicate for a column.
    // Call once per column before any submit. One predicate per column (last
    // wins); a single conjunct is sound for skipping.
    void add_int_needles(const std::string& column, const std::vector<int64_t>& needles) {
        ColDictPred& p = dict_preds_[column];
        p.kind = 0; p.int_vals = needles;
    }
    void add_str_pred(const std::string& column, int kind, const std::vector<std::string>& vals) {
        ColDictPred& p = dict_preds_[column];
        p.kind = kind; p.str_vals = vals;
    }
    void clear_eq_needles() { dict_preds_.clear(); }

    // Q24 latmat: register the pushed pass-1 predicate. `fn`/`ctx` are opaque
    // (opteryx_pass1_predicate_eval + Pass1PredCtx); `cols` are the predicate's
    // column names in the order the ctx's col_idx expects. Set once before submit.
    void set_pass1_predicate(void* fn, void* ctx, const std::vector<std::string>& cols) {
        pass1_pred_.fn = reinterpret_cast<Pass1PredFn>(fn);
        pass1_pred_.ctx = ctx;
        pass1_pred_.cols = cols;
    }
    void clear_pass1_predicate() { pass1_pred_.fn = nullptr; pass1_pred_.ctx = nullptr; pass1_pred_.cols.clear(); }

    /**
     * Submit a row group for read + decode + serialize.
     * column_stats carry absolute file offsets — worker adjusts to buffer-relative.
     */
    void submit_row_group(const std::string& path, int rg_idx,
                          const std::vector<std::string>& column_names,
                          const std::vector<ColumnStats>& column_stats) {
        if (shutdown_) return;

        WorkItem item;
        item.path = path;
        item.rg_idx = rg_idx;
        item.column_names = column_names;
        item.column_stats = column_stats;
        enqueue_pending(std::move(item));
    }

    /**
     * Submit a row group with a per-row mask (1=keep, 0=skip).
     * Workers apply the mask during decode so only surviving rows are serialized.
     * Default-empty mask in the base overload means existing callers are unaffected.
     */
    void submit_row_group(const std::string& path, int rg_idx,
                          const std::vector<std::string>& column_names,
                          const std::vector<ColumnStats>& column_stats,
                          const std::vector<uint8_t>& row_mask) {
        if (shutdown_) return;

        WorkItem item;
        item.path = path;
        item.rg_idx = rg_idx;
        item.column_names = column_names;
        item.column_stats = column_stats;
        item.row_mask = row_mask;
        enqueue_pending(std::move(item));
    }

    bool try_get_result(MorselRef& out) {
        std::lock_guard<std::mutex> lk(queue_mutex_);
        if (result_queue_.empty()) return false;
        out = std::move(result_queue_.front());
        result_queue_.pop_front();
        queue_cv_.notify_one();  // wake a blocked producer if queue was full
        return true;
    }

    /**
     * Block until a result is available or the pipeline is fully drained.
     * Returns true and populates `out` when a result is ready.
     * Returns false when the pipeline is shut down and the queue is empty.
     */
    bool wait_and_get_result(MorselRef& out) {
        std::unique_lock<std::mutex> lk(queue_mutex_);
        while (true) {
            if (!result_queue_.empty()) {
                out = std::move(result_queue_.front());
                result_queue_.pop_front();
                queue_cv_.notify_one();  // wake a blocked producer if queue was full
                return true;
            }
            if (shutdown_.load(std::memory_order_relaxed)) {
                return false;  // shutdown and nothing left
            }
            // Gap #3 Phase 2b (deadlock fix): rather than block waiting for a free
            // pool worker to produce a result — which, on a pool SHARED with the exec
            // engine, may never happen if every worker (including this thread's) is
            // itself blocked here — claim a pending item and decode it OURSELVES.
            // This guarantees progress: if we are blocked, either a result is in
            // flight (we loop and take it), or an item is claimable (we decode it),
            // or pending_work_ is drained (shutdown/return). No all-wait state exists.
            if (!pending_items_.empty()) {
                WorkItem item = std::move(pending_items_.front());
                pending_items_.pop_front();
                inline_decodes_.fetch_add(1, std::memory_order_relaxed);
                lk.unlock();
                decode_row_group(item);  // does its own queue_mutex_ locking + notify
                lk.lock();
                continue;  // our decode likely enqueued a result — re-check
            }
            // Nothing ready and nothing to help with: sleep until a pool worker
            // produces a result, a new item is published, or we shut down.
            queue_cv_.wait(lk, [this]() {
                return !result_queue_.empty()
                    || shutdown_.load(std::memory_order_relaxed)
                    || !pending_items_.empty();
            });
        }
    }

    // WP-8: signal early cancellation. Non-blocking — flips the flag and wakes
    // any back-pressure-blocked workers so queued tasks bail promptly at the
    // top of decode_row_group. The actual wait for in-flight tasks to finish
    // happens in wait_shutdown() (called by close()/destructor). Safe to call
    // more than once and safe to call before wait_shutdown().
    void cancel() {
        cancelled_.store(true, std::memory_order_relaxed);
        queue_cv_.notify_all();
    }

    void wait_shutdown() {
        shutdown_ = true;
        queue_cv_.notify_all();
        if (!decode_pool_) return;
        if (owns_pool_) {
            // Exclusive pool: safe to wait for EVERYTHING in it, nothing else
            // submits here.
            decode_pool_->wait();
        } else {
            // Shared/injected pool (Gap #3 Phase 2b): pool->wait() would block on
            // unrelated work (other operators' tasks, possibly another query's), so
            // we cannot use it. We must still guarantee no dispatched ticket touches
            // `this` after we return (the destructor is about to free queue_mutex_/
            // queue_cv_/pending_items_). pending_work_==0 is INSUFFICIENT for that —
            // it means "all rowgroups decoded", but a ticket's body (run_one_pending
            // + the trailing tickets_inflight_ decrement, and decode_row_group's
            // trailing queue_cv_.notify) can still be executing after the last
            // pending_work_-- lands. So wait on tickets_inflight_ (decremented as
            // each ticket's ABSOLUTE last action) reaching 0. SPIN, not a cv-wait:
            // a cv-wait here would itself be the object a still-running ticket
            // notifies into after we've been destroyed (the notify-after-free the
            // old pending_work_ cv-wait was latently exposed to). Teardown-only, so
            // the brief spin cost is irrelevant.
            while (tickets_inflight_.load(std::memory_order_acquire) != 0) {
                std::this_thread::yield();
            }
        }
    }

    int pending_work_count() const {
        return pending_work_.load(std::memory_order_relaxed);
    }

    // WP-8: number of queued decode tasks that bailed at the top because the
    // pipeline was cancelled — i.e. row groups whose IO/decode was skipped.
    uint64_t cancelled_skips() const {
        return cancelled_skips_.load(std::memory_order_relaxed);
    }

    uint64_t spin_iterations() const {
        return spin_iterations_.load(std::memory_order_relaxed);
    }
    uint64_t enqueue_count() const {
        return enqueue_count_.load(std::memory_order_relaxed);
    }
    size_t queue_high_watermark() const {
        return queue_high_watermark_.load(std::memory_order_relaxed);
    }

    uint64_t http_request_count() const {
        return http_request_count_.load(std::memory_order_relaxed);
    }
    uint64_t http_fetch_ops() const {
        return http_fetch_ops_.load(std::memory_order_relaxed);
    }
    int http_latency_bucket_count() const {
        return kHttpLatBuckets;
    }
    // Upper bound (ms) of bucket i; the final bucket is overflow and returns 0.
    uint64_t http_latency_bucket_bound_ms(int i) const {
        return (i >= 0 && i < kHttpLatBuckets - 1) ? kHttpLatBoundsMs[i] : 0;
    }
    uint64_t http_latency_bucket(int i) const {
        return (i >= 0 && i < kHttpLatBuckets)
            ? http_lat_buckets_[i].load(std::memory_order_relaxed) : 0;
    }
    uint64_t worker_blocked_ns() const {
        return worker_blocked_ns_.load(std::memory_order_relaxed);
    }
    uint64_t ipc_bytes_serialized() const {
        return ipc_bytes_serialized_.load(std::memory_order_relaxed);
    }
    // Compressed bytes read from storage across every row group this pipeline
    // decoded. The scan's real IO volume; see bytes_fetched_ above.
    uint64_t bytes_fetched() const {
        return bytes_fetched_.load(std::memory_order_relaxed);
    }
    // Process-cumulative count of range requests re-issued on transient failure
    // (WP-5). Global across all pipelines/workers; not per-query.
    uint64_t http_retries() const {
#ifdef RUGO_ENABLE_HTTP
        return HttpClient::total_retries();
#else
        return 0;
#endif
    }
};

}  // namespace rugo
