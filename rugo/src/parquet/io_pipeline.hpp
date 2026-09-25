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
#include <stdexcept>
#include <cstdint>
#include <cstdio>
#include <utility>
#include <algorithm>
#include <limits>
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
#if defined(__ARM_NEON)
#include <arm_neon.h>
#elif defined(__AVX2__)
#include <immintrin.h>
#endif

#include "BS_thread_pool.hpp"
// Remote (HTTP/HTTPS/GCS) reads are gated on RUGO_ENABLE_HTTP — defined by the
// opteryx_core build (which compiles + links http_client.cpp / libcurl), unset
// by the standalone rugo wheel (local filesystem only). When unset, libcurl is
// never included and remote paths fail loud in read_range / decode_row_group.
#ifdef RUGO_ENABLE_HTTP
#include "http_client.hpp"
#endif
#include "decode.hpp"
#include "page_index.hpp"
#include "ipc_serialize.hpp"
#include "metadata.hpp"
#include "core/string_slot.h"   // Stage 4b: build Draken string slots in the worker
#include "ops/string_hash.h"    // E37: draken_build_string_slot_seed — slot + carried hash seed
#include "core/buffers.h"       // DrakenVector / DrakenStringArena — worker-side pass-1 predicate view
#include "ops/float_ops.h"      // fp_canon — ingestion canonicalisation of -0.0 / NaN
#include "core/vector_alloc.h"  // draken_identity_sel — dense selection for the view
// docs/EXECUTION_TRACING_DESIGN.md: rugo calls the extern "C" bridge
// (trace_bridge_c.h), NEVER draken/core/trace.hpp directly — this file
// compiles into pool_reader.so (a separate .so from _operators.so, which
// also pulls this same io_pipeline.hpp via native_parquet_scan_source.hpp),
// and header-only inline/static C++ state does not merge across .so
// boundaries. See src/cpp/engine/trace.hpp's header comment for the same
// rule on the engine side, and trace_bridge_c.h for why the bridge exists.
#include "core/trace_bridge_c.h"

// H6 (2026-08-14, unratified): per-architecture default for the whole-file
// local mmap cache — see the use_mmap_cache lambda in decode_row_group for the
// measurements behind each arm. Overridable at build time
// (-DRUGO_LOCAL_MMAP_CACHE_DEFAULT=0|1) and at run time (RUGO_LOCAL_MMAP_CACHE).
#ifndef RUGO_LOCAL_MMAP_CACHE_DEFAULT
  #if defined(__x86_64__) || defined(_M_X64)
    #define RUGO_LOCAL_MMAP_CACHE_DEFAULT 1
  #else
    #define RUGO_LOCAL_MMAP_CACHE_DEFAULT 0
  #endif
#endif

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
    // one slot per row (plain). DK_VARCHAR_DICT carries `data_length` unique-value
    // slots + a `codes` selection of `length` (build_direct_string_dict).
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
    // per-row selection. Consumer wraps via the generic type-parameterised
    // draken_vector_own_dict with the matching DrakenType tag.
    DK_UINT8_DICT = 15, DK_UINT16_DICT = 16, DK_UINT32_DICT = 17, DK_UINT64_DICT = 18,
    // Signed narrow integer direct paths — the signed mirror of DK_UINT8/16/32.
    // `data` is a positional array at the EXACT declared width (1/2/4 bytes,
    // matching DRAKEN_INT8/16/32); these never widen. A parquet int8/int16 is
    // physically int32 with an INTEGER(8|16, signed) annotation, and a BARE
    // physical int32 IS a 32-bit signed column — so an un-annotated int32 lands
    // on DK_INT32, not DK_INT64. Only physical int64 yields DK_INT64.
    DK_INT8 = 19, DK_INT16 = 20, DK_INT32 = 21,
    // Signed narrow "compressed" (Dict-shaped) direct path, mirroring
    // DK_UINT*_DICT: dictionary at the exact declared width + uint32_t codes.
    DK_INT8_DICT = 22, DK_INT16_DICT = 23, DK_INT32_DICT = 24
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
    // 1 = long-form payload bytes were deliberately never materialized (the
    // planner proved every read of this column is length-answerable). Explicit
    // state — never inferred from arena/arena_len. See draken/core/buffers.h.
    bool     payloads_elided = false;
    void*    codes = nullptr;        // DK_VARCHAR_DICT: uint32 code per row
    uint32_t data_length = 0;        // DK_VARCHAR_DICT: number of unique-value slots
    bool     dict_sorted = false;    // dict shapes: `data` is ascending (is_sorted)
    // Row-group-level clustering hint (rugo parquet sorting_columns, trusted
    // only from a rugo-written file — see metadata.cpp's created_by gate), NOT
    // the dictionary-value-array concept above. Applies to any direct_kind, not
    // just dict shapes. Copied verbatim from ColumnStats.is_sorted/sort_descending
    // where this ColumnOut is built (col_stats already in scope there).
    bool     row_sorted = false;
    bool     row_sorted_descending = false;
    // Draken logical descriptor KIND recovered from the file's key-value
    // metadata (metadata.cpp's ApplyDrakenLogicalKV): the side channel for
    // kinds parquet has no logical type to express, today only IPV4 (5).
    // 0 = the file says nothing, which means "don't know", never "no
    // descriptor". Carried, not applied — the consumer decides whether to
    // attach it (see the IPV4 gates on the two opteryx scan paths).
    int      draken_logical_kind = 0;
    // E37 carried key-hash: per-data-element hash seed (str_hash_seed) computed
    // during slot build, one uint64 per slot (plain: length; dict: data_length).
    // draken_vector_own_string* COPIES it, so unlike the buffers above this is NOT
    // "taken" — the MorselRef dtor always frees it. nullptr for non-string columns.
    void*    keyhash = nullptr;
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
    // Memory admission (ParquetIOPipeline::set_memory_budget): the decoded-bytes
    // estimate this result holds on the pipeline's ledger, released by the
    // consumer's pop (try_get_result / wait_and_get_result). 0 = budget off.
    int64_t charged_bytes = 0;

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
                if (c.keyhash) free_fn(c.keyhash);   // E37: always ours (own_string COPIES)
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

// The DrakenType a fixed-width direct kind's buffers hold — the SAME mapping the
// consumer applies (NativeScanColumnBuilder::draken_type_for). Dense and dict-shaped
// variants of a kind share a tag: the shape is in the buffers, not in the type.
// Returns 0 (not a valid DrakenType) for anything this view does not build:
//   DK_POOL       — a serialized IPC blob, not a viewable buffer at all;
//   DK_DECIMAL128 — its precision/scale live in a logical descriptor the consumer
//                   attaches OUTSIDE the DrakenVector (VectorOwner::logical_type),
//                   so a bare view would be a decimal with no scale — a different
//                   answer, not a cheaper one.
static inline DrakenType pass1_natural_type(int dk) {
    switch (dk) {
        case DK_INT8:    case DK_INT8_DICT:    return DRAKEN_INT8;
        case DK_INT16:   case DK_INT16_DICT:   return DRAKEN_INT16;
        case DK_INT32:   case DK_INT32_DICT:   return DRAKEN_INT32;
        case DK_INT64:   case DK_INT64_DICT:   return DRAKEN_INT64;
        case DK_UINT8:   case DK_UINT8_DICT:   return DRAKEN_UINT8;
        case DK_UINT16:  case DK_UINT16_DICT:  return DRAKEN_UINT16;
        case DK_UINT32:  case DK_UINT32_DICT:  return DRAKEN_UINT32;
        case DK_UINT64:  case DK_UINT64_DICT:  return DRAKEN_UINT64;
        case DK_FLOAT32: case DK_FLOAT32_DICT: return DRAKEN_FLOAT32;
        case DK_FLOAT64: case DK_FLOAT64_DICT: return DRAKEN_FLOAT64;
        case DK_BOOL:                          return DRAKEN_BOOL;
        default:                               return static_cast<DrakenType>(0);
    }
}

// Is `dk` one of the §11 Dict-shaped direct kinds (dictionary values in `data`,
// per-row uint32 codes in `codes`)? String dicts are handled separately — their
// `data` is a slot array that has to be fronted by a DrakenStringArena header.
static inline bool pass1_is_numeric_dict(int dk) {
    switch (dk) {
        case DK_INT8_DICT:  case DK_INT16_DICT:  case DK_INT32_DICT: case DK_INT64_DICT:
        case DK_UINT8_DICT: case DK_UINT16_DICT: case DK_UINT32_DICT: case DK_UINT64_DICT:
        case DK_FLOAT32_DICT: case DK_FLOAT64_DICT:
            return true;
        default:
            return false;
    }
}

// Build a NON-owning DrakenVector view over a decoded ColumnOut so the pushed
// predicate can read it without a copy. Returns false for column shapes not yet
// supported worker-side (caller then leaves survivor_mask empty → serial fallback).
// `sa` backs a string column's arena header and must outlive `v`'s use.
//
// CLAUDE.md §11: the view is built with draken's own constructors so `data[sel[i]]`
// holds for every shape and the vector is field-for-field what the consumer would
// have built from the same buffers — the worker-side mask and the serial fallback
// are the same computation over the same vector, not two implementations of it.
// The buffers stay owned by the ColumnOut (nothing here allocates or frees; the
// codes handed to draken_vector_from_dict are borrowed, NOT transferred).
//
// The TAG is the one thing a view cannot derive: a column the plan retags (DATE /
// TIMESTAMP / TIME-unit / DECIMAL) or declares NVARCHAR / VARBINARY reaches the
// consumer as a different type than its physical buffers say. rugo cannot know
// that — it is opteryx plan state — so the opteryx side does not push the
// predicate at all unless every predicate column lands on its natural tag
// (pass1_worker_predicate_admissible, connectors/parquet_io/pass1_predicate_gate.py).
static inline bool pass1_build_dv_view(ColumnOut& co, uint32_t nrows,
                                       DrakenStringArena& sa, DrakenVector& v) {
    const int dk = co.direct_kind;
    if (dk == DK_VARCHAR || dk == DK_VARCHAR_DICT) {
        // DK_VARCHAR: one slot per row (plain). DK_VARCHAR_DICT: `data_length`
        // unique-value slots + a per-row uint32 code selection. Both put long
        // values in `arena` at offsets relative to its base, so the header below
        // fronts them identically — only the slot COUNT and the selection differ.
        const bool is_dict = (dk == DK_VARCHAR_DICT);
        if (is_dict && (co.codes == nullptr || co.data_length == 0)) return false;
        const uint32_t nslots = is_dict ? co.data_length : nrows;
        sa.slots       = static_cast<DrakenStringSlot*>(co.data);
        sa.arena       = static_cast<uint8_t*>(co.arena);
        sa.length      = nslots;
        sa.arena_used  = co.arena_len;
        sa.arena_cap   = co.arena_len;
        // Validity is per LOGICAL ROW and travels on the vector; the arena header's
        // own bitmap is indexed by SLOT, which for a dict is a different space
        // entirely. The consumer leaves it null (consolidate_string_block) — so
        // does this, for both shapes.
        sa.null_bitmap = nullptr;
        sa.owns_buffers = 0;
        // Carry the decoder's state: if the payloads were never materialized the
        // predicate reads lengths only, and anything that would move bytes (the
        // gather that compacts survivors) must see it too.
        sa.payloads_elided = co.payloads_elided ? 1u : 0u;
        sa.type        = DRAKEN_VARCHAR;
        v = is_dict
            ? draken_vector_from_dict(&sa, nslots, static_cast<const uint32_t*>(co.codes),
                                      nrows, DRAKEN_VARCHAR, co.validity)
            : draken_vector_from_dense(&sa, nrows, DRAKEN_VARCHAR, co.validity);
        if (is_dict && co.dict_sorted && draken_is_dict(&v))
            v.flags |= DRAKEN_DICT_KEYS_SORTED;
        return true;
    }
    const DrakenType t = pass1_natural_type(dk);
    if (t == static_cast<DrakenType>(0)) return false;   // unsupported kind → fallback
    if (co.data == nullptr) return false;
    if (pass1_is_numeric_dict(dk)) {
        if (co.codes == nullptr || co.data_length == 0) return false;
        v = draken_vector_from_dict(co.data, co.data_length,
                                    static_cast<const uint32_t*>(co.codes), nrows, t,
                                    co.validity);
        if (co.dict_sorted && draken_is_dict(&v))
            v.flags |= DRAKEN_DICT_KEYS_SORTED;
        return true;
    }
    // Dense: a positional array at the kind's exact width (DK_BOOL is the bit-packed
    // member of this set — same construction, draken reads the bit through the same
    // data[sel[i]] contract).
    v = draken_vector_from_dense(co.data, nrows, t, co.validity);
    return true;
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
        // rc 4 is a KERNEL error — the program ran and the kernel failed. rc 96 is a
        // kernel DATA error, and it deliberately does NOT join it: its message is the
        // user-facing text naming the offending value, which the mask callback has no
        // way to carry back (only `result.error`, a fixed string, reaches the caller).
        // Declining hands the identical program to the consumer's serial path, which
        // fails with that message intact — strictly more informative than the fixed
        // text this arm could set. Every other non-zero rc means the c-native VM does
        // not APPLY to these operands (compare
        // not-available for the pair of types, a NULL operand, a string result, an
        // arena it could not take). That is a capability statement, not a fault, and
        // the consumer's serial path is strictly more capable — the trampoline drops
        // to the GIL Morsel VM for exactly these rcs. So decline (empty mask) and let
        // the consumer answer, which is the SAME rule the main thread already applies
        // in predicate_filter_and_mask_c_native: raise on 4, otherwise fall back.
        // Nothing is hidden — a consumer that is no more capable re-runs the identical
        // program and fails loud there.
        if (rc == 4) {
            result.success = false;
            result.error = "pass-1 predicate eval failed (rc=4, kernel error)";
        }
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
// Apply draken's ingestion canonicalisation to every float buffer a decoded
// column can hand onward: -0.0 -> +0.0, any NaN bit-pattern -> one canonical
// quiet NaN (draken/ops/float_ops.h, architect-locked 2026-05-22).
//
// WHY IT LIVES HERE, and not in decode_column.cpp. This is the boundary where a
// decoded chunk becomes a DrakenVector, and the canonicalisation is DRAKEN's
// contract, not Parquet's. `DecodeColumnFromChunk` is also the standalone rugo
// file-reader's entry point, where a caller converting Parquet to CSV is owed
// the bytes that are in the file — a reader that silently rewrites -0.0 to 0.0
// is lying about the file. Past this line the values belong to the engine, and
// the engine's rule is that -0.0 does not exist.
//
// WHAT IT FIXES. `fp_total_eq` treats -0.0 and 0.0 as equal (IEEE `==` does),
// but hashing, grouping and set operations key on RAW BITS — float_ops.h says
// they may, precisely because canonicalisation is supposed to have happened
// first. It had not, on this path: only the nanobind constructors in
// draken_native.cpp canonicalised, so a float column read from Parquet kept its
// -0.0 and `GROUP BY f` split one value into two groups while `f = 0.0` matched
// both, and `X EXCEPT ALL X` returned rows.
//
// EVERY SHAPE IS COVERED BY THIS ONE CALL because all of them read these same
// buffers: `build_direct_fixed` memcpys from float{32,64}_values,
// `build_direct_float_dict` gathers from dict_float{32,64}_values, the RLE
// expansion reads rle_float64_values, and `pass1_build_dv_view` views whatever
// those produced.
//
// PRECONDITION — the zero-copy `ext_float32` / `ext_float64` buffers are NOT
// handled, because on this path they are always null: all three
// `DecodeColumnFromChunk` calls below use the mask-only overload, which passes
// nullptr for every ext_* pointer, so decode always lands in the vectors above.
// Wiring ext_* through here later (an obvious zero-copy win) would route float
// values around this function and silently reintroduce the -0.0 split — extend
// this function in the same commit, over `ext_written` elements, NOT over the
// caller's capacity.
static inline void canonicalise_decoded_floats(DecodedColumn& d) {
    if (d.type != "float32" && d.type != "float64") return;

    for (float& v : d.float32_values)       v = draken::ops::fp_canon(v);
    for (float& v : d.dict_float32_values)  v = draken::ops::fp_canon(v);
    for (double& v : d.float64_values)      v = draken::ops::fp_canon(v);
    for (double& v : d.dict_float64_values) v = draken::ops::fp_canon(v);
    // RLE runs are held as double for BOTH float32 and float64 dict columns
    // (see DecodedColumn::rle_float64_values). Canonicalising as double is
    // exact for either: every float32 widens without loss, and -0.0/NaN keep
    // their class across the widening.
    for (double& v : d.rle_float64_values)  v = draken::ops::fp_canon(v);
}

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
        // Signed: width comes from the IntType annotation; an un-annotated
        // column (width 0) is exactly what its physical type says.
        if (t == "int32") {
            switch (d.int_bit_width) {
                case 8:  return DK_INT8_DICT;
                case 16: return DK_INT16_DICT;
                default: return DK_INT32_DICT;
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
    // RLE skip-dense NUMERIC → §11 Dict-shaped direct, rebuilt from the run table
    // by build_direct_rle_dict. Previously this fell to DK_POOL below, and the
    // native scan Source cannot consume a POOL column that is not array/decimal/
    // varchar — so a plain `SELECT SUM(<low-cardinality int>)` over any parquet
    // file whose writer RLE-encoded that column failed outright with
    // "unsupported column encoding". Width/signedness rules are identical to the
    // dict branch above; the run table is only emitted for max_definition_level
    // == 0, hence the valid_bits guard.
    if ((t == "int64" || t == "int32") && !d.rle_int64_values.empty() &&
        !d.rle_run_lengths.empty() && d.valid_bits.empty()) {
        if (d.is_unsigned) {
            switch (d.int_bit_width) {
                case 8:  return DK_UINT8_DICT;
                case 16: return DK_UINT16_DICT;
                case 32: return DK_UINT32_DICT;
                default: return DK_UINT64_DICT;
            }
        }
        if (t == "int32") {
            switch (d.int_bit_width) {
                case 8:  return DK_INT8_DICT;
                case 16: return DK_INT16_DICT;
                default: return DK_INT32_DICT;
            }
        }
        return DK_INT64_DICT;
    }
    // RLE skip-dense float / string → §11 Dict-shaped direct, same treatment and
    // same reason as the numeric branch above.
    if (!d.rle_run_lengths.empty() && d.valid_bits.empty()) {
        if (t == "float64" && !d.rle_float64_values.empty()) return DK_FLOAT64_DICT;
        if (t == "float32" && !d.rle_float64_values.empty()) return DK_FLOAT32_DICT;
        if ((t == "string" || t == "byte_array") && !d.rle_str_lens.empty())
            return DK_VARCHAR_DICT;
    }
    if (!d.rle_int64_values.empty() || !d.rle_float64_values.empty() ||
        !d.rle_str_lens.empty()) return DK_POOL;         // RLE skip-dense, unhandled shape
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
        // Signed: int8/int16 carry an IntType annotation; a bare physical int32
        // (width 0) is a 32-bit signed column, so it narrows to DK_INT32 rather
        // than widening. int32_values already holds the exact bits either way.
        switch (d.int_bit_width) {
            case 8:  return DK_INT8;
            case 16: return DK_INT16;
            default: return DK_INT32;
        }
    }
    if (t == "float32" && _fixed_eligible(d.float32_values.size(), n, nullable)) return DK_FLOAT32;
    if (t == "float64" && _fixed_eligible(d.float64_values.size(), n, nullable)) return DK_FLOAT64;
    if (t == "boolean" && _fixed_eligible(d.boolean_values.size(), n, nullable)) return DK_BOOL;
    // Stage 4b: plain (non-dict, non-RLE, non-list) byte_array → direct dense
    // VARCHAR. The string arena triple is one entry per row (incl null rows);
    // only take the positional case so the per-row slot build below is exact.
    if ((t == "string" || t == "byte_array") && d.string_lens.size() == n)
        return DK_VARCHAR;
    return DK_POOL;
}

// Build a positional DrakenStringSlot array (+ long-string arena + validity) for a
// plain byte_array column, mirroring the Cython _build_string_plain EXACTLY: one
// slot per row; strings > STR_INLINE_MAX live in the arena (hash from the bytes),
// inline strings live in the slot; null rows get an init-null slot. The string
// arena triple has one entry per row. Allocates via `alloc`; frees what it took
// on failure.
// `length_only`: the planner proved every read of this column is
// length-answerable, so long-form payload bytes are never read. Each value's
// true length (and its free 4-byte prefix) is still recorded; only the payload
// copy is skipped, and the slot carries STR_ELIDED_PAYLOAD_OFFSET so any misuse
// faults. The STATE is carried explicitly on the arena via out.payloads_elided.
static inline bool build_direct_string_plain(const DecodedColumn& d,
                                             void* (*alloc)(size_t), void (*freefn)(void*),
                                             ColumnOut& out, bool want_seed = false,
                                             bool length_only = false) {
    const uint32_t n = static_cast<uint32_t>(d.num_rows);
    const bool nullable = !d.valid_bits.empty();
    const uint8_t* nb = nullable ? d.valid_bits.data() : nullptr;
    const uint8_t*  vbytes = d.string_arena.data();
    const uint32_t* voffs  = d.string_offsets.data();
    const int32_t*  vlens  = d.string_lens.data();
    const size_t    vcount = d.string_lens.size();

    // Pass 1: arena bytes (long, non-null strings only).
    size_t total_arena = 0;
    if (!length_only) {
        for (uint32_t i = 0; i < n; ++i) {
            if (nullable && !((nb[i >> 3] >> (i & 7)) & 1)) continue;
            const size_t slen = (i < vcount) ? (size_t)vlens[i] : 0u;
            if (slen > STR_INLINE_MAX) total_arena += slen;
        }
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
    // E37: build the hash seed ONLY when the plan marks this column a downstream
    // key (want_seed). Non-key columns take the cheap builder — NO XXH3 at all —
    // which is the whole point of the plan-gated hashing. null-row seeds stay 0
    // (the consumer bakes NULL_HASH from validity, so they are never read).
    uint64_t* keyhash = nullptr;
    if (want_seed) {
        keyhash = static_cast<uint64_t*>(alloc((n ? n : 1u) * sizeof(uint64_t)));
        if (!keyhash) { if (validity) freefn(validity); freefn(arena); freefn(slots); return false; }
    }

    // Pass 2: fill arena + build slots (+ seeds when keyed).
    uint32_t arena_pos = 0;
    for (uint32_t i = 0; i < n; ++i) {
        DrakenStringSlot* slot = &slots[i];
        if (nullable && !((nb[i >> 3] >> (i & 7)) & 1)) {
            str_init_null(slot);
            if (keyhash) keyhash[i] = 0u;   // null row: seed unused
            continue;
        }
        const uint8_t* sp = vbytes + voffs[i];
        const uint32_t slen = static_cast<uint32_t>(vlens[i]);
        if (slen > STR_INLINE_MAX && length_only) {
            if (want_seed) draken::ops::draken_build_string_slot_seed(
                               slot, sp, slen, STR_ELIDED_PAYLOAD_OFFSET, &keyhash[i]);
            else           draken_build_string_slot(
                               slot, sp, slen, STR_ELIDED_PAYLOAD_OFFSET);
        } else if (slen > STR_INLINE_MAX) {
            std::memcpy(arena + arena_pos, sp, slen);
            if (want_seed) draken::ops::draken_build_string_slot_seed(slot, sp, slen, arena_pos, &keyhash[i]);
            else           draken_build_string_slot(slot, sp, slen, arena_pos);  // no XXH3
            arena_pos += slen;
        } else {
            if (want_seed) draken::ops::draken_build_string_slot_seed(slot, sp, slen, arena_pos, &keyhash[i]);
            else           draken_build_string_slot(slot, sp, slen, arena_pos);  // inline
        }
    }

    out.data = slots;
    out.validity = validity;
    out.length = n;
    out.arena = arena;
    out.arena_len = arena_pos;
    out.payloads_elided = length_only;
    out.keyhash = keyhash;
    return true;
}

// Expand a packed dict-code array (1/2/4 bytes per code, LE) into the uint32
// `codes` selection every dict-shaped ColumnOut carries. Shared by all four
// dict builders below, which each open-coded the same per-row scalar switch.
// cw == 4 is a straight bulk copy; 1/2 widen 8/16 → 32 bits, vectorised where
// the target ISA offers it (NEON vmovl / AVX2 cvtepu). Scalar fallback is the
// same loop the builders used before.
static inline void expand_packed_codes(const uint8_t* ca, uint32_t n, uint8_t cw,
                                       uint32_t* codes) {
    if (cw == 4) {
        if (n) std::memcpy(codes, ca, static_cast<size_t>(n) * sizeof(uint32_t));
        return;
    }
    uint32_t i = 0;
    if (cw == 1) {
#if defined(__ARM_NEON)
        for (; i + 8 <= n; i += 8) {
            const uint8x8_t b = vld1_u8(ca + i);
            const uint16x8_t w = vmovl_u8(b);
            vst1q_u32(codes + i,     vmovl_u16(vget_low_u16(w)));
            vst1q_u32(codes + i + 4, vmovl_u16(vget_high_u16(w)));
        }
#elif defined(__AVX2__)
        for (; i + 8 <= n; i += 8) {
            const __m128i b = _mm_loadl_epi64(reinterpret_cast<const __m128i*>(ca + i));
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(codes + i),
                                _mm256_cvtepu8_epi32(b));
        }
#endif
        for (; i < n; ++i) codes[i] = ca[i];
        return;
    }
    // cw == 2
#if defined(__ARM_NEON)
    for (; i + 8 <= n; i += 8) {
        const uint16x8_t w = vld1q_u16(reinterpret_cast<const uint16_t*>(ca + i * 2));
        vst1q_u32(codes + i,     vmovl_u16(vget_low_u16(w)));
        vst1q_u32(codes + i + 4, vmovl_u16(vget_high_u16(w)));
    }
#elif defined(__AVX2__)
    for (; i + 8 <= n; i += 8) {
        const __m128i w = _mm_loadu_si128(reinterpret_cast<const __m128i*>(ca + i * 2));
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(codes + i),
                            _mm256_cvtepu16_epi32(w));
    }
#endif
    for (; i < n; ++i) {
        uint16_t v;
        std::memcpy(&v, ca + i * 2, 2);
        codes[i] = v;
    }
}

// Defined below, next to build_direct_narrow_dict whose payload rules they share.
static inline bool build_direct_rle_dict(const DecodedColumn& d, int elem_bytes,
                                         void* (*alloc)(size_t), void (*freefn)(void*),
                                         ColumnOut& out);
static inline bool build_direct_rle_float_dict(const DecodedColumn& d, bool is_f32,
                                               void* (*alloc)(size_t), void (*freefn)(void*),
                                               ColumnOut& out);
static inline bool build_direct_rle_string_dict(const DecodedColumn& d,
                                                void* (*alloc)(size_t), void (*freefn)(void*),
                                                ColumnOut& out, bool want_seed, bool length_only);

// Build the DICT-VARCHAR direct buffers for a dict-encoded byte_array column,
// mirroring _build_string_dict (consumer) + serialize_string_dict (source): a
// compact value array of `dict_size` unique slots over a verbatim copy of
// string_dict_arena (slot k references offset string_dict_offsets[k], length =
// the offset delta — matching the deserializer), plus a per-row uint32 `codes`
// selection from dict_codes_array (packed code_width) or dict_indices (sparse,
// null rows → code 0). The result is data_length < length (dict shape) but
// accessed through the same uniform value[codes[i]] path.
// `length_only` has the same meaning and the same contract as in
// build_direct_string_plain: the planner proved every read of this column is
// length-answerable, so long-form payload bytes are never copied — the slot
// records the true length and carries STR_ELIDED_PAYLOAD_OFFSET so any misuse
// faults. Without this the DICT path silently defeated the elision: a column the
// planner had proved length-only still copied one payload per DISTINCT value,
// which is how `AVG(length(URL))` regressed when URL started arriving dict-shaped
// instead of plain (it had been copying nothing at all).
static inline bool build_direct_string_dict(const DecodedColumn& d,
                                            void* (*alloc)(size_t), void (*freefn)(void*),
                                            ColumnOut& out, bool want_seed = false,
                                            bool length_only = false) {
    // RLE skip-dense carries one string per RUN, not a dict + code source.
    if (!d.rle_run_lengths.empty())
        return build_direct_rle_string_dict(d, alloc, freefn, out, want_seed, length_only);
    const uint32_t n = static_cast<uint32_t>(d.num_rows);
    const uint32_t dict_size = static_cast<uint32_t>(d.string_dict_lens.size());
    const bool nullable = !d.valid_bits.empty();
    const uint8_t* nb = nullable ? d.valid_bits.data() : nullptr;
    const size_t arena_len = d.string_dict_arena.size();

    DrakenStringSlot* slots = static_cast<DrakenStringSlot*>(
        alloc((dict_size ? dict_size : 1u) * sizeof(DrakenStringSlot)));
    if (!slots) return false;
    // Elided: no payload bytes are copied at all, so the arena is a stub. Slots
    // still carry each value's true length.
    const size_t arena_alloc = length_only ? 0u : arena_len;
    uint8_t* arena = static_cast<uint8_t*>(alloc(arena_alloc ? arena_alloc : 1u));
    if (!arena) { freefn(slots); return false; }
    if (arena_alloc) std::memcpy(arena, d.string_dict_arena.data(), arena_alloc);
    // E37: one seed per DISTINCT value (data_length entries) — only when the plan
    // marks this column a downstream key. Non-key dict columns take the cheap
    // builder (no XXH3). Dict already hashes per-distinct, so the cost is small,
    // but the gate keeps the "no key → no hash" invariant uniform.
    uint64_t* keyhash = nullptr;
    if (want_seed) {
        keyhash = static_cast<uint64_t*>(alloc((dict_size ? dict_size : 1u) * sizeof(uint64_t)));
        if (!keyhash) { freefn(arena); freefn(slots); return false; }
    }
    // Read the bytes from the SOURCE arena, not the copy: when eliding there is
    // no copy to read. Short values still live INLINE in the slot, so their bytes
    // are always taken; only long-form payloads are dropped, and those slots get
    // STR_ELIDED_PAYLOAD_OFFSET (per-slot, as the contract requires — a consumer
    // must never infer "no payload" from the arena-level flag alone).
    const uint8_t* src_arena = d.string_dict_arena.data();
    for (uint32_t k = 0; k < dict_size; ++k) {
        const uint32_t s_off = d.string_dict_offsets[k];
        const uint32_t slen = (k + 1u < dict_size)
            ? (d.string_dict_offsets[k + 1u] - s_off)
            : (static_cast<uint32_t>(arena_len) - s_off);
        const uint8_t* sp = src_arena + s_off;
        const uint32_t slot_off = (slen > STR_INLINE_MAX)
            ? (length_only ? STR_ELIDED_PAYLOAD_OFFSET : s_off)
            : (length_only ? 0u : s_off);
        if (want_seed) draken::ops::draken_build_string_slot_seed(&slots[k], sp, slen, slot_off, &keyhash[k]);
        else           draken_build_string_slot(&slots[k], sp, slen, slot_off);
    }

    uint32_t* codes = static_cast<uint32_t*>(alloc((n ? n : 1u) * sizeof(uint32_t)));
    if (!codes) { freefn(keyhash); freefn(arena); freefn(slots); return false; }
    const uint8_t cw = d.code_width;
    if (!d.dict_codes_array.empty()) {
        expand_packed_codes(d.dict_codes_array.data(), n, cw, codes);
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
        if (!validity) { freefn(keyhash); freefn(codes); freefn(arena); freefn(slots); return false; }
        std::memcpy(validity, d.valid_bits.data(), d.valid_bits.size());
    }

    out.data = slots;
    out.arena = arena;
    out.arena_len = length_only ? 0u : arena_len;
    out.codes = codes;
    out.data_length = dict_size;
    out.validity = validity;
    out.length = n;
    out.dict_sorted = d.dict_ordered;
    out.payloads_elided = length_only;
    out.keyhash = keyhash;
    return true;
}

// Build a numeric "compressed" (§11 Dict-shaped) direct column: a draken_alloc'd
// int64 dictionary (widening int32 dicts) + a uint32 per-row code selection.
// Mirrors build_direct_string_dict's code-source handling (dict_codes_array for
// nullable, dict_indices for non-nullable). On failure frees what it took.
static inline bool build_direct_int64_dict(const DecodedColumn& d,
                                           void* (*alloc)(size_t), void (*freefn)(void*),
                                           ColumnOut& out) {
    // RLE skip-dense carries its values in the run table, not in a code source.
    if (!d.rle_run_lengths.empty()) return build_direct_rle_dict(d, 8, alloc, freefn, out);
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
        expand_packed_codes(d.dict_codes_array.data(), n, cw, codes);
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

// RLE skip-dense → §11 Dict shape. The decoder resolves dict codes to VALUES per
// run (rle_int64_values[r] repeated rle_run_lengths[r] times) and discards the
// codes, so the dictionary has to be rebuilt from the run values.
//
// Values are DEDUPED. Emitting one dict entry per run would be cheaper but would
// hand downstream a Dict-shaped vector whose codes are not a bijection onto
// values; §11 defines data_length as "unique values in data", and both the
// compression-aware group keying and the dict-aware int filter read codes on
// that basis — duplicate values under distinct codes would split one group in
// two. Open-addressed probe table (power-of-two, linear probing) keeps the
// dedupe O(runs) without an std::unordered_map allocation per column.
//
// Returns false rather than writing a partial column if the runs do not cover
// exactly num_rows — a short/long run table means the decode and the row count
// disagree, and scattering that into a positional buffer is the silent-wrong-
// answer case, not something to paper over.
// T is int64_t (int columns) or double (float columns — rle_float64_values holds
// both float32 and float64 runs). Keying on the raw BIT PATTERN rather than on
// operator== is what makes this correct for floats: == would make every NaN run
// a fresh dictionary entry (NaN != NaN), reintroducing exactly the duplicate-value
// codes this dedupe exists to prevent. -0.0 cannot reach here unequal to +0.0 —
// canonicalise_decoded_floats runs over rle_float64_values before the DK split.
// For int64_t the bit pattern and the value are in bijection, so it is a no-op
// change of key.
template <typename T>
static inline bool rle_dedupe_and_expand(const DecodedColumn& d, uint32_t n,
                                         const std::vector<T>& run_values,
                                         std::vector<T>& uniq,
                                         uint32_t* codes) {
    static_assert(sizeof(T) == sizeof(uint64_t), "run value must be 64-bit");
    const size_t runs = d.rle_run_lengths.size();
    if (runs != run_values.size()) return false;
    size_t cap = 16;
    while (cap < runs * 2) cap <<= 1;
    std::vector<uint32_t> table(cap, 0xFFFFFFFFu);
    std::vector<uint64_t> uniq_bits;
    uniq_bits.reserve(runs < 64 ? runs : 64);
    const size_t mask = cap - 1;
    size_t off = 0;
    for (size_t r = 0; r < runs; ++r) {
        const T v = run_values[r];
        uint64_t bits;
        std::memcpy(&bits, &v, sizeof(uint64_t));
        const uint64_t h = bits * 0x9E3779B97F4A7C15ull;
        size_t slot = static_cast<size_t>(h >> 32) & mask;
        uint32_t code;
        for (;;) {
            const uint32_t e = table[slot];
            if (e == 0xFFFFFFFFu) {
                code = static_cast<uint32_t>(uniq.size());
                uniq.push_back(v);
                uniq_bits.push_back(bits);
                table[slot] = code;
                break;
            }
            if (uniq_bits[e] == bits) { code = e; break; }
            slot = (slot + 1) & mask;
        }
        const int32_t cnt = d.rle_run_lengths[r];
        if (cnt < 0 || off + static_cast<size_t>(cnt) > static_cast<size_t>(n)) return false;
        for (int32_t j = 0; j < cnt; ++j) codes[off + static_cast<size_t>(j)] = code;
        off += static_cast<size_t>(cnt);
    }
    return off == static_cast<size_t>(n);
}

// Dict-shaped direct column built from the RLE skip-dense outputs. elem_bytes
// selects the payload width (1/2/4/8) exactly as build_direct_narrow_dict does;
// 8 reproduces build_direct_int64_dict's widened int64 payload.
//
// RLE skip-dense is only produced for max_definition_level == 0 (see decode.hpp),
// so a validity bitmap here means the decoder's own invariant broke — refuse
// rather than silently drop the nulls.
static inline bool build_direct_rle_dict(const DecodedColumn& d, int elem_bytes,
                                         void* (*alloc)(size_t), void (*freefn)(void*),
                                         ColumnOut& out) {
    const uint32_t n = static_cast<uint32_t>(d.num_rows);
    if (!d.valid_bits.empty()) return false;

    uint32_t* codes = static_cast<uint32_t*>(alloc((n ? n : 1u) * sizeof(uint32_t)));
    if (!codes) return false;

    std::vector<int64_t> uniq;
    uniq.reserve(d.rle_run_lengths.size() < 64 ? d.rle_run_lengths.size() : 64);
    if (!rle_dedupe_and_expand(d, n, d.rle_int64_values, uniq, codes)) {
        freefn(codes);
        return false;
    }

    const uint32_t dsz = static_cast<uint32_t>(uniq.size());
    uint8_t* dict = static_cast<uint8_t*>(alloc((dsz ? dsz : 1u) * static_cast<size_t>(elem_bytes)));
    if (!dict) { freefn(codes); return false; }
    // Keeping the low elem_bytes yields the correct value at the declared width
    // for both domains, exactly as in build_direct_narrow_dict: the decode never
    // sign-extends an unsigned magnitude, and a signed value's two's-complement
    // low bytes are the value at that width.
    for (uint32_t k = 0; k < dsz; ++k) {
        const uint64_t v = static_cast<uint64_t>(uniq[k]);
        std::memcpy(dict + static_cast<size_t>(k) * elem_bytes, &v, static_cast<size_t>(elem_bytes));
    }

    out.data = dict;
    out.data_length = dsz;
    out.codes = codes;
    out.validity = nullptr;
    out.length = n;
    // Built in first-appearance order, NOT dictionary order — d.dict_ordered
    // describes the parquet dictionary, which these codes no longer index.
    out.dict_sorted = false;
    return true;
}

// Float counterpart of build_direct_rle_dict. rle_float64_values holds the runs
// as double for BOTH float32 and float64 columns; narrowing back to float for a
// float32 column is exact, because those doubles are float32 dict values that were
// widened losslessly — so two distinct dictionary entries cannot collapse into one
// float and silently re-create a duplicate code.
static inline bool build_direct_rle_float_dict(const DecodedColumn& d, bool is_f32,
                                               void* (*alloc)(size_t), void (*freefn)(void*),
                                               ColumnOut& out) {
    const uint32_t n = static_cast<uint32_t>(d.num_rows);
    if (!d.valid_bits.empty()) return false;

    uint32_t* codes = static_cast<uint32_t*>(alloc((n ? n : 1u) * sizeof(uint32_t)));
    if (!codes) return false;

    std::vector<double> uniq;
    uniq.reserve(d.rle_run_lengths.size() < 64 ? d.rle_run_lengths.size() : 64);
    if (!rle_dedupe_and_expand(d, n, d.rle_float64_values, uniq, codes)) {
        freefn(codes);
        return false;
    }

    const uint32_t dsz = static_cast<uint32_t>(uniq.size());
    const size_t elem = is_f32 ? sizeof(float) : sizeof(double);
    void* dict = alloc((dsz ? dsz : 1u) * elem);
    if (!dict) { freefn(codes); return false; }
    if (is_f32) {
        float* f = static_cast<float*>(dict);
        for (uint32_t k = 0; k < dsz; ++k) f[k] = static_cast<float>(uniq[k]);
    } else if (dsz) {
        std::memcpy(dict, uniq.data(), static_cast<size_t>(dsz) * sizeof(double));
    }

    out.data = dict;
    out.data_length = dsz;
    out.codes = codes;
    out.validity = nullptr;
    out.length = n;
    out.dict_sorted = false;
    return true;
}

// String counterpart. The run table carries one string per RUN
// (rle_str_arena/offsets/lens), so the same value repeats across runs and the
// unique set has to be rebuilt — byte-compared, not just hash-compared, so a hash
// collision cannot merge two different strings into one dictionary entry. The
// emitted arena holds only the unique values, and slot offsets are relative to it
// (build_direct_string_dict copies the parquet dict arena verbatim; here the arena
// is constructed, so offsets are assigned as values are appended).
static inline bool build_direct_rle_string_dict(const DecodedColumn& d,
                                                void* (*alloc)(size_t), void (*freefn)(void*),
                                                ColumnOut& out, bool want_seed = false,
                                                bool length_only = false) {
    const uint32_t n = static_cast<uint32_t>(d.num_rows);
    if (!d.valid_bits.empty()) return false;
    const size_t runs = d.rle_run_lengths.size();
    if (runs != d.rle_str_lens.size() || runs != d.rle_str_offsets.size()) return false;

    uint32_t* codes = static_cast<uint32_t*>(alloc((n ? n : 1u) * sizeof(uint32_t)));
    if (!codes) return false;

    // Dedupe the run strings: uniq_idx[k] is the run that first produced value k.
    size_t cap = 16;
    while (cap < runs * 2) cap <<= 1;
    std::vector<uint32_t> table(cap, 0xFFFFFFFFu);
    const size_t mask = cap - 1;
    std::vector<uint32_t> uniq_run;   // run index of each unique value
    std::vector<uint32_t> run_code(runs, 0u);
    uniq_run.reserve(runs < 64 ? runs : 64);
    size_t uniq_bytes = 0;
    for (size_t r = 0; r < runs; ++r) {
        const int32_t len = d.rle_str_lens[r];
        const uint32_t off = d.rle_str_offsets[r];
        if (len < 0 || off + static_cast<size_t>(len) > d.rle_str_arena.size()) {
            freefn(codes);
            return false;
        }
        const uint8_t* p = d.rle_str_arena.data() + off;
        uint64_t h = 1469598103934665603ull;               // FNV-1a, dedupe only
        for (int32_t j = 0; j < len; ++j) { h ^= p[j]; h *= 1099511628211ull; }
        size_t slot = static_cast<size_t>(h >> 32) & mask;
        for (;;) {
            const uint32_t e = table[slot];
            if (e == 0xFFFFFFFFu) {
                run_code[r] = static_cast<uint32_t>(uniq_run.size());
                table[slot] = run_code[r];
                uniq_run.push_back(static_cast<uint32_t>(r));
                uniq_bytes += static_cast<size_t>(len);
                break;
            }
            const uint32_t er = uniq_run[e];
            if (d.rle_str_lens[er] == len &&
                std::memcmp(d.rle_str_arena.data() + d.rle_str_offsets[er], p,
                            static_cast<size_t>(len)) == 0) {
                run_code[r] = e;
                break;
            }
            slot = (slot + 1) & mask;
        }
    }

    // Expand run codes to per-row codes, validating run coverage.
    size_t pos = 0;
    for (size_t r = 0; r < runs; ++r) {
        const int32_t cnt = d.rle_run_lengths[r];
        if (cnt < 0 || pos + static_cast<size_t>(cnt) > static_cast<size_t>(n)) {
            freefn(codes);
            return false;
        }
        const uint32_t c = run_code[r];
        for (int32_t j = 0; j < cnt; ++j) codes[pos + static_cast<size_t>(j)] = c;
        pos += static_cast<size_t>(cnt);
    }
    if (pos != static_cast<size_t>(n)) { freefn(codes); return false; }

    const uint32_t dsz = static_cast<uint32_t>(uniq_run.size());
    DrakenStringSlot* slots = static_cast<DrakenStringSlot*>(
        alloc((dsz ? dsz : 1u) * sizeof(DrakenStringSlot)));
    if (!slots) { freefn(codes); return false; }
    uint8_t* arena = static_cast<uint8_t*>(alloc(uniq_bytes ? uniq_bytes : 1u));
    if (!arena) { freefn(slots); freefn(codes); return false; }
    uint64_t* keyhash = nullptr;
    if (want_seed) {
        keyhash = static_cast<uint64_t*>(alloc((dsz ? dsz : 1u) * sizeof(uint64_t)));
        if (!keyhash) { freefn(arena); freefn(slots); freefn(codes); return false; }
    }

    // Same elision contract as the other two string builders: when the planner
    // proved every read length-answerable, long-form payloads are not copied and
    // the slot carries STR_ELIDED_PAYLOAD_OFFSET. Short values are inline in the
    // slot, so their bytes are taken either way.
    uint32_t a_off = 0;
    for (uint32_t k = 0; k < dsz; ++k) {
        const uint32_t r = uniq_run[k];
        const uint32_t slen = static_cast<uint32_t>(d.rle_str_lens[r]);
        const uint8_t* sp = d.rle_str_arena.data() + d.rle_str_offsets[r];
        if (slen > STR_INLINE_MAX && length_only) {
            if (want_seed)
                draken::ops::draken_build_string_slot_seed(&slots[k], sp, slen, STR_ELIDED_PAYLOAD_OFFSET, &keyhash[k]);
            else
                draken_build_string_slot(&slots[k], sp, slen, STR_ELIDED_PAYLOAD_OFFSET);
            continue;                      // no arena write, no cursor advance
        }
        std::memcpy(arena + a_off, sp, slen);
        if (want_seed)
            draken::ops::draken_build_string_slot_seed(&slots[k], arena + a_off, slen, a_off, &keyhash[k]);
        else
            draken_build_string_slot(&slots[k], arena + a_off, slen, a_off);
        a_off += slen;
    }

    out.data = slots;
    out.data_length = dsz;
    out.arena = arena;
    out.arena_len = a_off;             // bytes actually written (0 when eliding)
    out.payloads_elided = length_only;
    out.codes = codes;
    out.validity = nullptr;
    out.length = n;
    out.keyhash = keyhash;
    out.dict_sorted = false;
    return true;
}

// E33 — exact-width "compressed" (Dict-shaped) direct column: a draken_alloc'd
// dictionary narrowed/reinterpreted to elem_bytes (1/2/4/8, matching the declared
// DRAKEN_UINT8/16/32/64 or DRAKEN_INT8/16/32 width) + a uint32 per-row code
// selection. Mirrors build_direct_int64_dict's code-source handling; unlike it,
// never widens.
//
// Signedness-agnostic by construction: keeping the low elem_bytes of the source
// dict payload yields the correct value at the declared width for both domains —
// the unsigned magnitude (E33's is_unsigned decode never sign-extends) and the
// two's-complement signed value alike. One function parameterized by width rather
// than seven near-duplicates, mirroring build_direct_int64_dict's existing
// is32/dsz branch structure.
static inline bool build_direct_narrow_dict(const DecodedColumn& d, int elem_bytes,
                                            void* (*alloc)(size_t), void (*freefn)(void*),
                                            ColumnOut& out) {
    // RLE skip-dense carries its values in the run table, not in a code source.
    if (!d.rle_run_lengths.empty())
        return build_direct_rle_dict(d, elem_bytes, alloc, freefn, out);
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
        expand_packed_codes(d.dict_codes_array.data(), n, cw, codes);
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
    // RLE skip-dense carries its values in the run table, not in a code source.
    if (!d.rle_run_lengths.empty())
        return build_direct_rle_float_dict(d, is_f32, alloc, freefn, out);
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
        expand_packed_codes(d.dict_codes_array.data(), n, cw, codes);
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
        if (elem_bytes == (src_is_32 ? 4 : 8)) {
            // Declared width already equals the source width: on a LE host the
            // staging loop above was a byte-for-byte identity copy, so stage
            // nothing and let the single bulk copy below do the work.
            csrc = src_is_32
                ? reinterpret_cast<const uint8_t*>(d.int32_values.data())
                : reinterpret_cast<const uint8_t*>(d.int64_values.data());
        } else {
            narrowed.resize(count * static_cast<size_t>(elem_bytes));
            for (size_t i = 0; i < count; ++i) {
                const uint64_t v = src_is_32
                    ? static_cast<uint64_t>(static_cast<uint32_t>(d.int32_values[i]))
                    : static_cast<uint64_t>(d.int64_values[i]);
                std::memcpy(narrowed.data() + i * elem_bytes, &v, static_cast<size_t>(elem_bytes));
            }
            csrc = narrowed.data();
        }
        elem = static_cast<uint32_t>(elem_bytes); compact_count = count;
    } else if (dk == DK_INT8 || dk == DK_INT16 || dk == DK_INT32) {
        // Signed mirror of the unsigned branch above: preserve the exact
        // declared width instead of widening. The source is always physical
        // int32 (parquet has no narrower integer storage) and int32_values holds
        // the sign-extended value, so truncating to the low 1/2/4 bytes is
        // value-preserving for anything that legitimately fits the declared
        // width. Little-endian host assumed, as elsewhere in this decoder.
        const int elem_bytes = (dk == DK_INT8) ? 1 : (dk == DK_INT16) ? 2 : 4;
        const size_t count = d.int32_values.size();
        if (elem_bytes == 4) {
            // DK_INT32 from physical int32 — the staging loop was an identity
            // copy (one memcpy call per element, then a second full pass). Point
            // straight at the source; the bulk copy below is the only pass.
            csrc = reinterpret_cast<const uint8_t*>(d.int32_values.data());
        } else {
            narrowed.resize(count * static_cast<size_t>(elem_bytes));
            for (size_t i = 0; i < count; ++i) {
                const int32_t v = d.int32_values[i];
                std::memcpy(narrowed.data() + i * elem_bytes, &v, static_cast<size_t>(elem_bytes));
            }
            csrc = narrowed.data();
        }
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
    // PageIndex page pruning (compute_page_prune). The footer's per-page
    // min/max (ColumnIndex) are tested against the pushed per-value predicates
    // (dict_preds_, the same conjuncts the dictionary decode-skip consults); a
    // page no predicate can match contributes its row range as zeros to a
    // row-group-wide mask, and the OffsetIndex then turns that mask into, per
    // column, (a) a PageJumpPlan the decoder advances by without reading the
    // pruned pages and (b) the byte runs the remote fetch actually needs.
    struct PagePrune {
        bool active = false;      // at least one page was pruned → row_mask/jump/extents are live
        bool all_pruned = false;  // no row survives → the row group is empty_filtered
        std::vector<uint8_t> row_mask;              // one byte per row group row, 1 = keep
        std::vector<PageJumpPlan> jump;             // parallel to column_stats; size()==0 → header-walk
        // Absolute [start, end) byte runs to fetch, parallel to column_stats.
        // Empty for a column without an OffsetIndex = the whole chunk.
        std::vector<std::vector<std::pair<int64_t, int64_t>>> extents;
        int64_t pages_pruned = 0;   // across every projected column
        int64_t bytes_pruned = 0;   // header+payload bytes of those pages
    };

    // ── Remote fetch plan ────────────────────────────────────────────────────
    // Built ONCE per fetch block over every member's projected columns. A
    // "slot" is (member, column): slot = member * ncols + col. Column-chunk
    // extents of every member go through the coalescer together, which is what
    // turns a column's byte-adjacent chunks over a block (the grouped layout,
    // docs/PARQUET_GROUPED_COLUMN_MAJOR_DESIGN.md) into ONE range GET.
    struct RemotePlan {
        // One byte run to fetch. A slot is one extent (its whole chunk) unless
        // page pruning split it into the runs that survive.
        struct Extent { int64_t start, end; size_t slot; };
        struct Group { int64_t start, end, useful; std::vector<size_t> extents; };
        std::vector<int64_t> cstart, clen;   // per slot: the chunk frame the decoder sees
        std::vector<Extent>  extents;
        std::vector<Group>   groups;
    };

    struct FetchBlock;   // defined after WorkItem, which it holds

    struct WorkItem {
        std::string path;
        int rg_idx;
        std::vector<std::string> column_names;
        std::vector<ColumnStats> column_stats;  // absolute file offsets
        std::vector<uint8_t> row_mask;           // empty = no mask (decode all rows)
        // PageIndex page pruning: computed ONCE per item by whichever stage
        // reaches it first (the fetch-ahead stage, or decode on the coupled
        // path) and carried so the other stage reuses the identical plan.
        bool page_prune_done = false;
        PagePrune page_prune;
        // Memory admission (set_memory_budget): the footer-derived estimates
        // this item is charged for, and what it CURRENTLY holds on each ledger
        // so every exit path releases exactly what was taken.
        int64_t est_decoded_bytes = 0;
        int64_t est_compressed_bytes = 0;
        int64_t charged_decoded = 0;
        int64_t charged_compressed = 0;
        // docs/EXECUTION_TRACING_DESIGN.md: 0 unless tracing is armed at enqueue
        // time (enqueue_block stamps both together) — decode_row_group treats
        // issued_ns == 0 as "don't record spans for this item", so a query that
        // starts untraced never pays for a corr_id allocation either.
        uint64_t issued_ns = 0;
        uint32_t corr_id = 0;
        uint32_t file_id = 0;  // draken_trace_intern_file(path); 0 == untraced
        // The fetch block this row group belongs to and its member index in it
        // (null for a LOCAL file: served by mmap, nothing to fetch). The block
        // owns the fetched bytes; a fetch failure travels on it and is rethrown
        // by decode_row_group inside its existing catch — one error path, the
        // original message. It is NOT a signal for decode to re-fetch: that
        // would be a hidden second retry round on top of HttpClient's own
        // budget, doubling time-to-failure and burying the first failure.
        std::shared_ptr<FetchBlock> block;
        size_t member = 0;
    };

    // ── Fetch block ──────────────────────────────────────────────────────────
    // The kept row groups of one block of one REMOTE file, fetched as one unit:
    // their extents are planned and coalesced together and the resulting range
    // GETs are issued in one batch. The bytes are SHARED by the block's members
    // — each member is still its own WorkItem, claimed and decoded on its own,
    // one result per row group — and are released when the last member drops
    // its reference. Row-group-major files (every row group its own block) and
    // single row groups are one-member blocks: there is ONE remote fetch path.
    //
    // Two lifecycles, one plan: with a fetch pool (set_fetch_ahead) the fetch
    // stage fills the block and only then publishes its members as decodable;
    // on the coupled path the block's LEAD member is published at once and the
    // worker that claims it fetches the block (ensure_block_fetched), publishes
    // the followers, then decodes its own row group. A follower therefore never
    // waits on another worker's IO — it is not claimable until the bytes exist.
    struct FetchBlock {
        std::string path;
        size_t ncols = 0;
        size_t n_members = 0;
        // Per member: the chunk-frame geometry the decode of that member reads.
        std::vector<std::vector<int64_t>> base_offsets;   // [member][col]
        RemotePlan plan;
        std::vector<std::vector<uint8_t>> buffers;        // per coalesced group
        uint64_t fetch_ns = 0;            // folded into the first member's read_ns only
        std::exception_ptr error;         // a fetch failure, rethrown by every member's decode
        bool fetched = false;             // coupled path: guarded by `mu`
        std::mutex mu;
        // Coupled path only: members held back until the lead's fetch lands.
        std::vector<WorkItem> followers;
        // Cancel: the block's bytes are counted as discarded ONCE, not per member.
        std::atomic<bool> discard_counted{false};
    };


    // Priority-capable pool (Gap #3 Phase 2b): same vendored BS::thread_pool template
    // as the plain BS::light_thread_pool this used to be (light_thread_pool IS
    // thread_pool<tp::none> — see BS_thread_pool.hpp), feature flag on. Decode tasks
    // submit at BS::pr::high so they don't queue behind exec-pool backlog when this
    // pool is SHARED with the execution engine (see owns_pool_ below). A pool with
    // only one priority ever used behaves identically to tp::none, so this is safe
    // for the standalone-constructor (self-owned, no injection) path too.
    std::shared_ptr<BS::thread_pool<BS::tp::priority>> decode_pool_;
    // Fetch-ahead (set_fetch_ahead(N); 0 = off, the default): a pool that ONLY
    // issues the remote range GETs, so the number of concurrent fetches stops
    // being pinned to the decode thread count. Measured motivation: with 4
    // decode workers, deepening the submission window 6 -> 64 moves nothing
    // (4.54s -> 4.45s) because a ticket beyond the pool size merely queues —
    // concurrency == pool size, by construction.
    //
    // MUST be exclusive to this pipeline (never the injected exec pool): the
    // no-deadlock argument in wait_and_get_result relies on a fetch stage that
    // cannot itself be blocked behind a consumer waiting on its output.
    std::unique_ptr<BS::thread_pool<BS::tp::priority>> fetch_pool_;
    int fetch_ahead_ = 0;
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
    // Fetch-ahead: compressed bytes the FETCH stage bought that a cancelled
    // decode then threw away (LIMIT satisfied early, dropped cursor). Billed
    // egress for rows nobody read — surfaced so a wrong depth policy shows up
    // in telemetry rather than on the bill.
    std::atomic<uint64_t> prefetch_discarded_bytes_{0};

    // docs/EXECUTION_TRACING_DESIGN.md: trace_node_id_ is the plan-node
    // identity this pipeline's spans carry, set once via set_trace_node_id()
    // by Engine::set_native_scan_source (engine.hpp) when this pipeline backs
    // a native scan. Row-group correlation ids are NOT minted here — see
    // draken_trace_next_corr_id() (core/trace_bridge_c.h): a per-pipeline
    // counter restarting at 1 for every instance collided across queries that
    // open more than one pipeline (multiple scan passes/retries sharing one
    // query), silently conflating unrelated row groups in the drained trace.
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

    // PROTOTYPE (2026-08-14, unratified) — H6: per-pipeline whole-file mmap
    // cache for LOCAL files. Previously every row-group decode opened the file
    // and mapped its own column span, then unmapped it: with 100 files x 5 row
    // groups that is 500 open/mmap/munmap cycles per query, and each munmap on
    // a ~30-thread process pays cross-CPU TLB-shootdown IPIs. Mapping each file
    // ONCE per pipeline (whole file, PROT_READ) and slicing every row group out
    // of that single mapping cuts the syscall count 5x and drops the shootdowns
    // to scan teardown. Pages of unprojected columns are never touched, so a
    // whole-file mapping costs address space (14GB for the full ClickBench
    // split set) but no memory or IO. Remote paths are untouched.
    // Lifetime: mappings live until the pipeline is destroyed (after
    // wait_shutdown(), so no worker can still hold a slice). Failures are
    // cached too — a file whose open/mmap fails once falls back to per-column
    // pread for every row group without retrying the map per item.
    struct LocalFileMapping {
        void*  base = MAP_FAILED;
        size_t len  = 0;
    };
    std::unordered_map<std::string, LocalFileMapping> local_mmap_cache_;
    std::mutex local_mmap_mutex_;

    LocalFileMapping local_file_mapping(const std::string& path) {
        {
            std::lock_guard<std::mutex> lk(local_mmap_mutex_);
            auto it = local_mmap_cache_.find(path);
            if (it != local_mmap_cache_.end()) return it->second;
        }
        LocalFileMapping m;  // MAP_FAILED unless every step below succeeds
        int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd >= 0) {
            struct stat st;
            if (fstat(fd, &st) == 0 && st.st_size > 0) {
                void* b = mmap(nullptr, static_cast<size_t>(st.st_size),
                               PROT_READ, MAP_PRIVATE, fd, 0);
                if (b != MAP_FAILED) {
                    m.base = b;
                    m.len  = static_cast<size_t>(st.st_size);
                }
            }
            close(fd);
        }
        std::lock_guard<std::mutex> lk(local_mmap_mutex_);
        auto it = local_mmap_cache_.find(path);
        if (it != local_mmap_cache_.end()) {
            // Another worker mapped it while we were outside the lock: keep
            // theirs, release ours.
            if (m.base != MAP_FAILED) munmap(m.base, m.len);
            return it->second;
        }
        local_mmap_cache_.emplace(path, m);
        return m;
    }
    // Q24 latmat: pushed pass-1 predicate (opteryx callback). Set once before any
    // submit; workers read it const, no sync.
    Pass1Pred pass1_pred_;

    // ── PageIndex region cache ───────────────────────────────────────────────
    // A writer lays every ColumnIndex of the file out contiguously, then every
    // OffsetIndex, in the tail before the footer — so the index bytes one row
    // group needs sit inside the same region every other row group of that file
    // needs. One range read per FILE (widened at most once if a later row group
    // asks past it), not one per row group. The bytes are immutable once
    // published (a shared_ptr to a const vector), so a widening never pulls a
    // buffer out from under a worker still reading the previous one.
    struct PageIndexEntry {
        std::mutex mu;
        int64_t lo = -1, hi = -1;   // absolute [lo, hi) the bytes cover
        std::shared_ptr<const std::vector<uint8_t>> bytes;
    };
    std::unordered_map<std::string, std::shared_ptr<PageIndexEntry>> page_index_cache_;
    std::mutex page_index_mutex_;
    std::atomic<uint64_t> page_index_fetches_{0};
    std::atomic<uint64_t> page_index_bytes_fetched_{0};
    std::atomic<uint64_t> page_index_pages_pruned_{0};
    std::atomic<uint64_t> page_index_bytes_pruned_{0};
    std::atomic<uint64_t> page_index_row_groups_pruned_{0};
    std::atomic<uint64_t> page_index_gate_declines_{0};

    // See the cost gate in compute_page_prune for the measurements behind it.
    static constexpr double kPageIndexMaxCostRatio = 0.10;

    // Bytes covering absolute [lo, hi) of `path`'s page-index region, and the
    // absolute offset the returned buffer starts at. Throws on an IO failure —
    // a range the footer said exists and the store cannot serve is an IO error
    // like any other, not a reason to quietly decode every page.
    std::shared_ptr<const std::vector<uint8_t>> page_index_region(
            const std::string& path, int64_t lo, int64_t hi, int64_t& region_lo) {
        std::shared_ptr<PageIndexEntry> entry;
        {
            std::lock_guard<std::mutex> lk(page_index_mutex_);
            auto& slot = page_index_cache_[path];
            if (!slot) slot = std::make_shared<PageIndexEntry>();
            entry = slot;
        }
        std::lock_guard<std::mutex> lk(entry->mu);
        if (entry->bytes && entry->lo <= lo && hi <= entry->hi) {
            region_lo = entry->lo;
            return entry->bytes;
        }
        const int64_t nlo = entry->bytes ? std::min(entry->lo, lo) : lo;
        const int64_t nhi = entry->bytes ? std::max(entry->hi, hi) : hi;
        auto [bytes, ns] = read_range(path, nlo, nhi - nlo);
        (void)ns;
        page_index_fetches_.fetch_add(1, std::memory_order_relaxed);
        page_index_bytes_fetched_.fetch_add(static_cast<uint64_t>(bytes.size()),
                                            std::memory_order_relaxed);
        entry->bytes = std::make_shared<const std::vector<uint8_t>>(std::move(bytes));
        entry->lo = nlo;
        entry->hi = nhi;
        region_lo = nlo;
        return entry->bytes;
    }

    // ── Memory admission ─────────────────────────────────────────────────────
    // set_memory_budget(bytes): a cap on what THIS pipeline holds — decoded
    // results from the moment a worker claims an item until the consumer pops
    // its result (held_decoded_), plus compressed row-group bytes from fetch
    // until decode has consumed them (held_prefetch_). Charges are the footer's
    // estimates (Σ total_uncompressed_size / Σ total_compressed_size of the
    // projected columns) so the ledger is exact by construction: whatever was
    // charged is what gets released, whichever path drops the item.
    //
    // Two ledgers, not one, is what makes the wait deadlock-free: a decode
    // ticket may wait only while something a CONSUMER pop will release is held
    // (held_decoded_ > 0), never on bytes only a decode can release; a fetch
    // ticket waits only while some earlier fetch's bytes are still pending a
    // decode (held_prefetch_ > 0), which the decode admission above guarantees
    // will happen. And the consumer's own inline-help decode in
    // wait_and_get_result never waits on admission at all: it is the drain.
    // 0 (the default) = no budget, nothing changes.
    int64_t memory_budget_bytes_ = 0;
    std::atomic<int64_t>  held_decoded_{0};
    std::atomic<int64_t>  held_prefetch_{0};
    std::atomic<int64_t>  held_high_watermark_{0};
    std::atomic<uint64_t> admission_blocked_ns_{0};
    std::atomic<uint64_t> admission_waits_{0};

    static int64_t sum_column_bytes(const std::vector<ColumnStats>& cs, bool compressed) {
        int64_t n = 0;
        for (const auto& c : cs) {
            const int64_t v = compressed ? c.total_compressed_size : c.total_uncompressed_size;
            if (v > 0) n += v;
        }
        return n;
    }
    void ledger_charge(std::atomic<int64_t>& which, int64_t n) {
        if (n <= 0) return;
        which.fetch_add(n, std::memory_order_relaxed);
        const int64_t total = held_decoded_.load(std::memory_order_relaxed) +
                              held_prefetch_.load(std::memory_order_relaxed);
        int64_t prev = held_high_watermark_.load(std::memory_order_relaxed);
        while (total > prev &&
               !held_high_watermark_.compare_exchange_weak(prev, total, std::memory_order_relaxed)) {}
    }
    // Release + wake every admission waiter. The mutex is taken (and dropped)
    // before the notify so a waiter between its predicate check and its wait
    // cannot miss this release — the classic lost-wakeup ordering.
    void ledger_release(std::atomic<int64_t>& which, int64_t n) {
        if (n <= 0) return;
        which.fetch_sub(n, std::memory_order_relaxed);
        if (memory_budget_bytes_ > 0) {
            { std::lock_guard<std::mutex> lk(queue_mutex_); }
            queue_cv_.notify_all();
        }
    }
    // Under queue_mutex_: the decode-side charge taken at claim time.
    void claim_charge_locked(WorkItem& item) {
        if (memory_budget_bytes_ <= 0) return;
        item.charged_decoded = item.est_decoded_bytes;
        ledger_charge(held_decoded_, item.charged_decoded);
        // Coupled remote path: the compressed bytes are fetched inside decode and
        // live until it returns, so they are charged here rather than by a fetch
        // stage that does not exist for this item. A member whose block was
        // already fetched (by the fetch stage, or by the lead's decode) was
        // charged by whoever fetched it.
        if (item.block && !item.block->fetched && !path_is_local(item.path)) {
            item.charged_compressed = item.est_compressed_bytes;
            ledger_charge(held_prefetch_, item.charged_compressed);
        }
    }

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
    // ── Remote fetch geometry ────────────────────────────────────────────────
    // Pure functions of the WorkItem (+ dict_preds_, set before any submit).
    // Factored out so the fetch-ahead stage and decode_row_group derive the SAME
    // extents from ONE definition rather than two copies that could drift: a
    // drift here would decode a column from the wrong offset.
    static bool path_is_local(const std::string& path) {
        return path.rfind("gs://",    0) != 0 &&
               path.rfind("http://",  0) != 0 &&
               path.rfind("https://", 0) != 0;
    }

    // Per-column base offset (dictionary page if it precedes the data page,
    // else the data page).
    static std::vector<int64_t> compute_base_offsets(const WorkItem& item) {
        std::vector<int64_t> base_offsets(item.column_stats.size());
        for (size_t i = 0; i < item.column_stats.size(); ++i) {
            int64_t base = item.column_stats[i].data_page_offset;
            if (item.column_stats[i].dictionary_page_offset >= 0 &&
                item.column_stats[i].dictionary_page_offset < base) {
                base = item.column_stats[i].dictionary_page_offset;
            }
            base_offsets[i] = base;
        }
        return base_offsets;
    }

    // ── PageIndex page pruning ───────────────────────────────────────────────
    // Fills item.page_prune once (memoised on the item). Pure in the sense the
    // fetch geometry helpers are: the fetch-ahead stage computes it before it
    // plans its ranges and decode reuses the SAME result, so the two stages
    // cannot disagree about which bytes exist in a buffer.
    //
    // Applies only to an UNMASKED item: a pass-2 late-materialization item
    // already carries the exact survivor mask, and nothing here could remove a
    // row it could not — but it also could not add anything, and the pass-2
    // consumer's "a masked submit cannot come back empty" invariant is worth
    // more than the redundant work saved.
    void compute_page_prune(WorkItem& item) {
        if (item.page_prune_done) return;
        item.page_prune_done = true;
        PagePrune& pp = item.page_prune;
        // A/B arm, same convention as RUGO_LOCAL_MMAP_CACHE / RUGO_PREAD_SMALL_CHUNKS
        // above: RUGO_PAGE_INDEX_PRUNE=0 runs the pipeline as if no file carried a
        // page index, so both arms of a measurement can read the SAME file in one
        // binary. Default on.
        static const bool prune_enabled = []() {
            const char* v = getenv("RUGO_PAGE_INDEX_PRUNE");
            return !(v != nullptr && v[0] == '0' && v[1] == '\0');
        }();
        if (!prune_enabled) return;
        if (!item.row_mask.empty() || dict_preds_.empty() || item.column_stats.empty())
            return;

        const size_t ncols = item.column_stats.size();
        auto indexed = [](const ColumnStats& cs) {
            return cs.column_index_offset >= 0 && cs.column_index_length > 0 &&
                   cs.offset_index_offset >= 0 && cs.offset_index_length > 0;
        };
        // 1. Is there anything to test? A predicate column with both indexes.
        int64_t lo = std::numeric_limits<int64_t>::max(), hi = -1;
        bool any_pred = false;
        for (const auto& cs : item.column_stats) {
            if (!indexed(cs)) continue;
            lo = std::min(lo, std::min(cs.column_index_offset, cs.offset_index_offset));
            hi = std::max(hi, std::max(cs.column_index_offset + cs.column_index_length,
                                       cs.offset_index_offset + cs.offset_index_length));
            if (cs.max_repetition_level == 0 && dict_preds_.count(cs.name) != 0) any_pred = true;
        }
        if (!any_pred) return;

        // ── Cost gate ────────────────────────────────────────────────────────
        // The index region has to be READ before the data fetch can be planned,
        // so on a remote path it is a serial round trip in front of the scan,
        // not a background cost. Pay it only when the bytes it could save are
        // large compared with the bytes it costs — both of which the footer
        // already states exactly, so this is arithmetic, not a guess.
        //
        // MEASURED (hits, 1M rows, 5 row groups, clustered UserID point lookup,
        // dev/throttle_server.py; interleaved A/B, median of 3):
        //                                   index     data     ratio   ON vs OFF
        //   wide projection (6 cols)        0.79 MB   64.1 MB   0.012     5.48x FASTER
        //   narrow projection (3 cols)      0.77 MB    0.96 MB  0.80      0.62x SLOWER
        //   narrow, rtt 50ms                0.77 MB    0.96 MB  0.80      0.46x SLOWER
        // The two cases are three orders of magnitude apart in that ratio, so
        // the threshold is not a tuned constant sitting between two close
        // numbers — anything in [0.02, 0.5] separates them identically.
        //
        // 0.10 caps the downside: the index can cost at most a tenth of the
        // bytes it might remove, against an upside of nearly all of them. It
        // NEVER changes the answer — only whether we spend on the index — so
        // no correctness argument rides on the value.
        const int64_t index_bytes = hi - lo;
        int64_t projected_bytes = 0;
        for (const auto& cs : item.column_stats)
            if (cs.total_compressed_size > 0) projected_bytes += cs.total_compressed_size;
        if (index_bytes > 0 &&
            static_cast<double>(index_bytes) >
                kPageIndexMaxCostRatio * static_cast<double>(projected_bytes)) {
            page_index_gate_declines_.fetch_add(1, std::memory_order_relaxed);
            return;
        }

        int64_t region_lo = 0;
        auto region = page_index_region(item.path, lo, hi, region_lo);
        auto at = [&](int64_t off, int32_t len) -> const uint8_t* {
            if (off < region_lo ||
                off + len > region_lo + static_cast<int64_t>(region->size())) {
                throw std::runtime_error("page index range " + std::to_string(off) + "+" +
                                         std::to_string(len) + " lies outside the fetched region");
            }
            return region->data() + (off - region_lo);
        };

        // 2. Offset indexes for every indexed scalar column (the jump plans need
        // them all, not just the predicate columns), validated against the
        // footer's chunk geometry. Row count comes from num_values, which for a
        // scalar column IS the row count (nulls included); every scalar column
        // of one row group must agree on it.
        const std::vector<int64_t> base_offsets = compute_base_offsets(item);
        std::vector<OffsetIndexData> oi(ncols);
        std::vector<uint8_t> has_oi(ncols, 0);
        int64_t num_rows = -1;
        for (size_t i = 0; i < ncols; ++i) {
            const ColumnStats& cs = item.column_stats[i];
            if (!indexed(cs) || cs.max_repetition_level != 0) continue;
            if (cs.num_values < 0) continue;
            oi[i] = ParseOffsetIndex(at(cs.offset_index_offset, cs.offset_index_length),
                                     static_cast<size_t>(cs.offset_index_length));
            const auto& locs = oi[i].page_locations;
            if (locs.empty() || locs.front().first_row_index != 0) {
                throw std::runtime_error("page index: OffsetIndex for column '" + cs.name +
                                         "' does not start at row 0");
            }
            const int64_t chunk_end = base_offsets[i] + cs.total_compressed_size;
            for (const PageLocation& l : locs) {
                if (l.offset < base_offsets[i] || l.offset + l.compressed_page_size > chunk_end) {
                    throw std::runtime_error("page index: a page of column '" + cs.name +
                                             "' lies outside its column chunk");
                }
            }
            if (num_rows < 0) num_rows = cs.num_values;
            else if (num_rows != cs.num_values) {
                throw std::runtime_error("page index: scalar columns of row group " +
                                         std::to_string(item.rg_idx) + " disagree on the row count");
            }
            if (locs.back().first_row_index >= num_rows) {
                throw std::runtime_error("page index: OffsetIndex for column '" + cs.name +
                                         "' lists a page beyond the row group's rows");
            }
            has_oi[i] = 1;
        }
        if (num_rows <= 0) return;

        // 3. Per-page predicate test on each predicate column → row-group mask.
        std::vector<uint8_t> mask(static_cast<size_t>(num_rows), 1);
        bool any_pruned = false;
        std::vector<uint8_t> keep;
        for (size_t i = 0; i < ncols; ++i) {
            if (!has_oi[i]) continue;
            const ColumnStats& cs = item.column_stats[i];
            auto pit = dict_preds_.find(cs.name);
            if (pit == dict_preds_.end()) continue;
            const ColumnIndexData ci = ParseColumnIndex(
                at(cs.column_index_offset, cs.column_index_length),
                static_cast<size_t>(cs.column_index_length));
            const auto& locs = oi[i].page_locations;
            const size_t pruned = EvaluatePagePredicate(
                ci, locs.size(), pit->second.kind, &pit->second.int_vals,
                &pit->second.str_vals, cs.physical_type,
                StatsLogicalIsUnsigned(cs.logical_type), keep);
            if (pruned == 0) continue;
            any_pruned = true;
            for (size_t p = 0; p < locs.size(); ++p) {
                if (keep[p]) continue;
                const int64_t r0 = locs[p].first_row_index;
                const int64_t r1 = (p + 1 < locs.size()) ? locs[p + 1].first_row_index : num_rows;
                std::fill(mask.begin() + r0, mask.begin() + r1, 0);
            }
        }
        if (!any_pruned) return;

        // 4. The mask is final: derive each indexed column's jump plan and the
        // byte runs a remote fetch needs. A page is pruned for a column exactly
        // when no row of its range survives — the same test the decoder's
        // row-mask skip applies, decided here so the bytes can go unfetched.
        pp.active = true;
        pp.all_pruned = std::find(mask.begin(), mask.end(), uint8_t(1)) == mask.end();
        pp.row_mask = std::move(mask);
        pp.jump.resize(ncols);
        pp.extents.resize(ncols);
        for (size_t i = 0; i < ncols; ++i) {
            if (!has_oi[i]) continue;
            const ColumnStats& cs = item.column_stats[i];
            const auto& locs = oi[i].page_locations;
            const int64_t base = base_offsets[i];
            const int64_t chunk_end = base + cs.total_compressed_size;
            PageJumpPlan& jp = pp.jump[i];
            auto& ex = pp.extents[i];
            jp.page_offsets.reserve(locs.size());
            jp.page_sizes.reserve(locs.size());
            jp.page_rows.reserve(locs.size());
            jp.pruned.reserve(locs.size());
            // Everything before the first data page (the dictionary page) is
            // always needed.
            if (locs.front().offset > base) ex.emplace_back(base, locs.front().offset);
            for (size_t p = 0; p < locs.size(); ++p) {
                const int64_t r0 = locs[p].first_row_index;
                const int64_t r1 = (p + 1 < locs.size()) ? locs[p + 1].first_row_index : num_rows;
                const bool page_pruned =
                    std::find(pp.row_mask.begin() + r0, pp.row_mask.begin() + r1, uint8_t(1)) ==
                    pp.row_mask.begin() + r1;
                jp.page_offsets.push_back(locs[p].offset - base);
                jp.page_sizes.push_back(locs[p].compressed_page_size);
                jp.page_rows.push_back(static_cast<int32_t>(r1 - r0));
                jp.pruned.push_back(page_pruned ? 1 : 0);
                if (page_pruned) {
                    pp.pages_pruned += 1;
                    pp.bytes_pruned += locs[p].compressed_page_size;
                    continue;
                }
                const int64_t s = locs[p].offset, e = s + locs[p].compressed_page_size;
                if (!ex.empty() && ex.back().second == s) ex.back().second = e;
                else ex.emplace_back(s, e);
            }
            // Bytes after the last listed page (none, for a well-formed chunk)
            // are kept rather than assumed absent.
            const int64_t last_end = locs.back().offset + locs.back().compressed_page_size;
            if (last_end < chunk_end) {
                if (!ex.empty() && ex.back().second == last_end) ex.back().second = chunk_end;
                else ex.emplace_back(last_end, chunk_end);
            }
        }
        page_index_pages_pruned_.fetch_add(static_cast<uint64_t>(pp.pages_pruned),
                                           std::memory_order_relaxed);
        page_index_bytes_pruned_.fetch_add(static_cast<uint64_t>(pp.bytes_pruned),
                                           std::memory_order_relaxed);
        if (pp.all_pruned)
            page_index_row_groups_pruned_.fetch_add(1, std::memory_order_relaxed);
    }

    // Plan one fetch block: every member's page pruning and base offsets
    // (memoised on the member), then all their extents through the coalescer
    // TOGETHER. Coalescing merges runs of adjacent/near-adjacent
    // extents into single range GETs — see set_coalesce_tuning() for the
    // rationale and the measurements behind both bounds — and, because a
    // grouped file stores a column's chunks for a block back to back, a
    // projected column over the block becomes one merged run whatever the
    // member count. A member every page of which was pruned contributes no
    // extents (its decode emits empty_filtered from the memoised verdict).
    void plan_block(FetchBlock& blk, const std::vector<WorkItem*>& members) {
        blk.n_members = members.size();
        blk.base_offsets.resize(members.size());
        RemotePlan& plan = blk.plan;
        const size_t ncols = blk.ncols;
        plan.cstart.assign(members.size() * ncols, 0);
        plan.clen.assign(members.size() * ncols, 0);
        plan.extents.clear();
        plan.groups.clear();
        for (size_t m = 0; m < members.size(); ++m) {
            WorkItem& item = *members[m];
            compute_page_prune(item);
            blk.base_offsets[m] = compute_base_offsets(item);
            const PagePrune& pp = item.page_prune;
            const std::vector<int64_t>& base_offsets = blk.base_offsets[m];
            for (size_t i = 0; i < ncols; ++i) {
                const size_t slot = m * ncols + i;
                plan.cstart[slot] = base_offsets[i];
                plan.clen[slot]   = item.column_stats[i].total_compressed_size;
                if (pp.active && pp.all_pruned) continue;   // nothing to buy for this member
                const bool sparse = pp.active && i < pp.extents.size() && !pp.extents[i].empty();
                if (!sparse) {
                    plan.extents.push_back(RemotePlan::Extent{
                        plan.cstart[slot], plan.cstart[slot] + plan.clen[slot], slot});
                    continue;
                }
                for (size_t k = 0; k < pp.extents[i].size(); ++k)
                    plan.extents.push_back(RemotePlan::Extent{
                        pp.extents[i][k].first, pp.extents[i][k].second, slot});
            }
        }
        std::vector<size_t> order(plan.extents.size());
        for (size_t i = 0; i < order.size(); ++i) order[i] = i;
        std::sort(order.begin(), order.end(),
                  [&](size_t a, size_t b) { return plan.extents[a].start < plan.extents[b].start; });

        const int64_t max_bytes = coalesce_max_bytes_ > 0
            ? coalesce_max_bytes_ : std::numeric_limits<int64_t>::max();
        for (size_t k = 0; k < order.size(); ++k) {
            const size_t  x = order[k];
            const int64_t st = plan.extents[x].start, e = plan.extents[x].end;
            const int64_t len = e - st;
            bool merged = false;
            if (!plan.groups.empty()) {
                RemotePlan::Group& g = plan.groups.back();
                const int64_t ne      = std::max(g.end, e);
                const int64_t nspan   = ne - g.start;
                const int64_t nuseful = g.useful + len;
                const int64_t nwaste  = nspan - nuseful;
                if (nspan <= max_bytes &&
                    static_cast<double>(nwaste) <=
                        coalesce_waste_ratio_ * static_cast<double>(nuseful)) {
                    g.end = ne; g.useful = nuseful; g.extents.push_back(x);
                    merged = true;
                }
            }
            if (!merged)
                plan.groups.push_back(RemotePlan::Group{st, e, len, {x}});
        }
    }

#ifdef RUGO_ENABLE_HTTP
    // Issue every coalesced range for one fetch block concurrently. Used by the
    // decode stage (coupled, default) and by the fetch-ahead stage (decoupled).
    std::vector<std::vector<uint8_t>> fetch_remote_groups(
            const std::string& path, const RemotePlan& plan, uint64_t* out_ns) {
        const std::string url = fetch_url_for(path);
        std::vector<std::pair<std::string, std::map<std::string, std::string>>> reqs;
        reqs.reserve(plan.groups.size());
        for (const auto& g : plan.groups) {
            reqs.emplace_back(url, http_headers_(
                "bytes=" + std::to_string(g.start) +
                "-" + std::to_string(g.end - 1)));
        }
        auto t_fetch = std::chrono::steady_clock::now();
        auto bufs = tl_http_client().get_many(
            reqs, http_tuning_set_ ? &http_tuning_ : nullptr);
        const uint64_t batch_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - t_fetch).count();
        if (out_ns != nullptr) *out_ns += batch_ns;
        // One fetch operation covering reqs.size() concurrent ranges.
        record_http_fetch(batch_ns, reqs.size());
        return bufs;
    }
#endif

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
            bytes = tl_http_client().get(url, http_headers_(range_hdr),
                                          http_tuning_set_ ? &http_tuning_ : nullptr);

        } else if (path.substr(0, 7) == "http://" || path.substr(0, 8) == "https://") {
            is_remote = true;
            std::string range_hdr = "bytes=" + std::to_string(offset) +
                                    "-" + std::to_string(offset + size - 1);
            bytes = tl_http_client().get(path, http_headers_(range_hdr),
                                          http_tuning_set_ ? &http_tuning_ : nullptr);

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
            std::unique_lock<std::mutex> lk(queue_mutex_);
            if (pending_items_.empty()) return;
            if (memory_budget_bytes_ > 0) {
                // Memory admission: wait while what consumers will release is
                // held AND this item would push the total over budget. Shutdown/
                // cancel admit (the item bails at the top of decode and releases
                // its charge); an empty queue means a helper took the item.
                auto admissible = [this]() {
                    if (pending_items_.empty()) return true;
                    if (shutdown_.load(std::memory_order_relaxed) ||
                        cancelled_.load(std::memory_order_relaxed)) return true;
                    const int64_t held = held_decoded_.load(std::memory_order_relaxed);
                    if (held == 0) return true;
                    return held + held_prefetch_.load(std::memory_order_relaxed) +
                           pending_items_.front().est_decoded_bytes <= memory_budget_bytes_;
                };
                if (!admissible()) {
                    admission_waits_.fetch_add(1, std::memory_order_relaxed);
                    const auto t0 = std::chrono::steady_clock::now();
                    queue_cv_.wait(lk, admissible);
                    admission_blocked_ns_.fetch_add(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - t0).count(),
                        std::memory_order_relaxed);
                }
                if (pending_items_.empty()) return;
            }
            item = std::move(pending_items_.front());
            pending_items_.pop_front();
            claim_charge_locked(item);
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
    // Fetch-ahead: the FETCH stage's ticket body. Issues this row group's
    // range GETs on fetch_pool_, parks the bytes in the item, and only THEN
    // publishes it as decodable work. Hands its ticket over to the decode ticket
    // (tickets_inflight_ is incremented for the successor BEFORE this one is
    // released) so wait_shutdown()'s "0 == nothing will touch `this` again"
    // invariant holds across the two stages.
    //
    // A fetch failure is NOT swallowed and NOT retried here: the exception is
    // carried on the item and rethrown by decode_row_group inside its existing
    // catch, so it surfaces with its original message through the one error
    // path. Re-fetching from decode would be a hidden second retry round.
    // Fetch one block's bytes: plan every member together and issue the merged
    // ranges in one batch. `charge_members`: the members whose compressed bytes
    // this fetch buys and must charge on the prefetch ledger (the fetch stage
    // charges every member after an admission wait; the coupled lead charges
    // only its followers — its own share was charged when it was claimed).
    void fetch_block(FetchBlock& blk, const std::vector<WorkItem*>& members,
                     const std::vector<WorkItem*>& charge_members) {
#ifdef RUGO_ENABLE_HTTP
        try {
            plan_block(blk, members);
            for (WorkItem* it : charge_members) {
                it->charged_compressed = it->est_compressed_bytes;
                ledger_charge(held_prefetch_, it->charged_compressed);
            }
            if (!blk.plan.groups.empty())
                blk.buffers = fetch_remote_groups(blk.path, blk.plan, &blk.fetch_ns);
        } catch (...) {
            blk.buffers.clear();
            blk.error = std::current_exception();
        }
#else
        (void)members; (void)charge_members;
        blk.error = std::make_exception_ptr(std::runtime_error(
            "remote parquet path requires an HTTP-enabled build: " + blk.path));
#endif
        blk.fetched = true;
    }

    // Publish items as claimable and dispatch one claiming ticket per item.
    // Ordering is load-bearing (see enqueue_block): the items are already
    // counted in pending_work_, and each ticket is counted in tickets_inflight_
    // BEFORE it is dispatched, so a caller that holds its own ticket can release
    // it afterwards with the invariant intact.
    void publish_items(std::vector<WorkItem>&& items) {
        const size_t n = items.size();
        if (n == 0) return;
        {
            std::lock_guard<std::mutex> lk(queue_mutex_);
            for (WorkItem& it : items) pending_items_.push_back(std::move(it));
        }
        items.clear();
        // Wake as many helpers blocked in wait_and_get_result as there are new
        // claimable items: one item, one waiter — waking every puller per item
        // (notify_all) is a thundering herd on a local scan that publishes one
        // row group at a time.
        if (n == 1) queue_cv_.notify_one(); else queue_cv_.notify_all();
        tickets_inflight_.fetch_add(static_cast<int64_t>(n), std::memory_order_relaxed);
        for (size_t k = 0; k < n; ++k) {
            decode_pool_->detach_task([this]() {
                run_one_pending();
                tickets_inflight_.fetch_sub(1, std::memory_order_release);
            }, BS::pr::high);
        }
    }

    // Fetch-ahead: the FETCH stage's ticket body. Issues this block's range GETs
    // on fetch_pool_, parks the bytes on the block, and only THEN publishes its
    // members as decodable work. Hands its ticket over to the decode tickets
    // (tickets_inflight_ is incremented for the successors BEFORE this one is
    // released) so wait_shutdown()'s "0 == nothing will touch `this` again"
    // invariant holds across the two stages.
    //
    // A fetch failure is NOT swallowed and NOT retried here: the exception is
    // carried on the block and rethrown by every member's decode_row_group
    // inside its existing catch, so it surfaces with its original message
    // through the one error path. Re-fetching from decode would be a hidden
    // second retry round.
    void run_one_fetch(std::shared_ptr<FetchBlock> blk, std::vector<WorkItem>&& members) {
        if (!cancelled_.load(std::memory_order_relaxed) && blk->ncols > 0) {
            // Memory admission for the compressed bytes this fetch will hold
            // until every member's decode has consumed them. Waits only while an
            // EARLIER fetch's bytes are still pending a decode (see the ledger
            // comment).
            int64_t block_bytes = 0;
            for (const WorkItem& it : members) block_bytes += it.est_compressed_bytes;
            if (memory_budget_bytes_ > 0) {
                std::unique_lock<std::mutex> lk(queue_mutex_);
                auto admissible = [this, block_bytes]() {
                    if (shutdown_.load(std::memory_order_relaxed) ||
                        cancelled_.load(std::memory_order_relaxed)) return true;
                    const int64_t held = held_prefetch_.load(std::memory_order_relaxed);
                    if (held == 0) return true;
                    return held + held_decoded_.load(std::memory_order_relaxed) +
                           block_bytes <= memory_budget_bytes_;
                };
                if (!admissible()) {
                    admission_waits_.fetch_add(1, std::memory_order_relaxed);
                    const auto t0 = std::chrono::steady_clock::now();
                    queue_cv_.wait(lk, admissible);
                    admission_blocked_ns_.fetch_add(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - t0).count(),
                        std::memory_order_relaxed);
                }
            }
            std::vector<WorkItem*> ptrs;
            ptrs.reserve(members.size());
            for (WorkItem& it : members) ptrs.push_back(&it);
            // The budget-off case charges nothing (ledger_charge is a no-op on
            // a zero charge only; guard on the budget itself).
            fetch_block(*blk, ptrs, memory_budget_bytes_ > 0 ? ptrs : std::vector<WorkItem*>{});
        } else {
            blk->fetched = true;   // nothing bought; every member bails or decodes empty
        }
        publish_items(std::move(members));
    }

    // Coupled path (no fetch pool): the block's lead member was published at
    // submit; the worker that claimed it fetches the block here, then publishes
    // the followers — claimable only now that their bytes exist — and returns
    // to decode its own row group. Idempotent: a block is fetched once.
    void ensure_block_fetched(WorkItem& lead) {
        FetchBlock& blk = *lead.block;
        std::vector<WorkItem> followers;
        {
            std::lock_guard<std::mutex> lk(blk.mu);
            if (blk.fetched) return;
            std::vector<WorkItem*> members;
            members.reserve(1 + blk.followers.size());
            members.push_back(&lead);
            std::vector<WorkItem*> charge;
            for (WorkItem& f : blk.followers) { members.push_back(&f); charge.push_back(&f); }
            fetch_block(blk, members, memory_budget_bytes_ > 0 ? charge : std::vector<WorkItem*>{});
            followers = std::move(blk.followers);
            blk.followers.clear();
        }
        publish_items(std::move(followers));
    }

    // Enqueue one fetch block's members. Ordering is load-bearing: pending_work_
    // is advanced by the member count FIRST (before any member is claimable, so
    // a helper/ticket that grabs one and runs decode_row_group's pending_work_--
    // can never drive the counter negative), THEN the members are published,
    // THEN their tickets are counted and dispatched.
    void enqueue_block(std::shared_ptr<FetchBlock> blk, std::vector<WorkItem>&& members) {
        // docs/EXECUTION_TRACING_DESIGN.md: stamp each gather's issue time
        // (queue-wait span start) and mint its correlation id here, once, rather
        // than in every submit overload. Skipped entirely when tracing is off —
        // one relaxed atomic load, no clock read, no counter bump. corr_id comes
        // from the QUERY-WIDE bridge counter (draken_trace_next_corr_id), not a
        // pipeline-local one — see trace_node_id_'s comment above for why that
        // used to collide.
        if (draken_trace_enabled()) {
            for (WorkItem& item : members) {
                item.issued_ns = draken_trace_now_ns();
                item.corr_id = draken_trace_next_corr_id();
                item.file_id = draken_trace_intern_file(item.path.data(), item.path.size());
            }
        }
        pending_work_ += static_cast<int>(members.size());
        const bool remote = blk != nullptr;
        // Fetch-ahead: when the dedicated fetch pool exists, a REMOTE block is
        // NOT yet decodable — its members become claimable only once the bytes
        // are in hand (run_one_fetch publishes them). Local paths skip the stage
        // entirely (served by mmap in decode); routing them through it would add
        // a hand-off for no IO.
        if (fetch_pool_ && remote) {
            tickets_inflight_.fetch_add(1, std::memory_order_relaxed);
            fetch_pool_->detach_task(
                [this, blk, ms = std::move(members)]() mutable {
                    run_one_fetch(blk, std::move(ms));
                    tickets_inflight_.fetch_sub(1, std::memory_order_release);
                }, BS::pr::high);
            return;
        }
        if (remote && members.size() > 1) {
            // Coupled path: only the lead is claimable; the rest ride on the
            // block until the lead's decode has fetched it.
            std::vector<WorkItem> lead;
            lead.push_back(std::move(members.front()));
            blk->followers.reserve(members.size() - 1);
            for (size_t k = 1; k < members.size(); ++k)
                blk->followers.push_back(std::move(members[k]));
            members.clear();
            publish_items(std::move(lead));
            return;
        }
        publish_items(std::move(members));
    }

    // Non-const: decode memoises page pruning on the item and drops its block
    // reference when done (the bytes are dead for this member the moment its
    // decode ends). Both call sites hold a non-const local WorkItem.
    void decode_row_group(WorkItem& item) {
        // WP-8 cancel: a queued task whose work is no longer wanted bails here,
        // before any IO / decode / allocation. Nothing was reserved yet, so
        // there is nothing to release; just balance the pending-work ledger and
        // wake anyone waiting on the queue.
        if (cancelled_.load(std::memory_order_relaxed)) {
            cancelled_skips_.fetch_add(1, std::memory_order_relaxed);
            // Fetch-ahead bought these bytes and nobody will read them: count
            // them so the waste is a number in telemetry, not an inference —
            // once per block, since its members share them.
            if (item.block && item.block->fetched &&
                !item.block->discard_counted.exchange(true, std::memory_order_relaxed)) {
                uint64_t nb = 0;
                for (const auto& b : item.block->buffers) nb += b.size();
                prefetch_discarded_bytes_.fetch_add(nb, std::memory_order_relaxed);
            }
            // A coupled-path lead that never fetched leaves its followers held
            // on the block: they are cancelled work too, and are balanced here
            // rather than left unpublished with their pending_work_ counted.
            if (item.block) {
                std::vector<WorkItem> held;
                {
                    std::lock_guard<std::mutex> lk(item.block->mu);
                    held = std::move(item.block->followers);
                    item.block->followers.clear();
                }
                for (WorkItem& f : held) {
                    cancelled_skips_.fetch_add(1, std::memory_order_relaxed);
                    ledger_release(held_prefetch_, f.charged_compressed);
                    ledger_release(held_decoded_, f.charged_decoded);
                    pending_work_--;
                }
            }
            // Nothing was produced: both ledger charges come back here.
            ledger_release(held_prefetch_, item.charged_compressed);
            ledger_release(held_decoded_, item.charged_decoded);
            item.block.reset();
            pending_work_--;
            queue_cv_.notify_all();
            return;
        }

        // docs/EXECUTION_TRACING_DESIGN.md: t_dequeue closes the queue-wait span
        // opened at enqueue_block's issued_ns — this worker is now actually
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
        result.charged_bytes = item.charged_decoded;   // released by the consumer's pop

        uint64_t total_read_ns = 0;
        uint64_t total_decode_ns = 0;

        // For local files, slice this row group's column chunks out of the
        // pipeline's whole-file mapping (local_file_mapping above — one
        // open+mmap per FILE per pipeline, not per row group). Falls back to
        // read_range() for HTTP/GCS, and to per-column pread when the map
        // failed. PROTOTYPE H6 — see local_file_mapping's comment.
        const bool is_local = path_is_local(item.path);

        void*   mmap_base   = MAP_FAILED;
        size_t  mmap_len    = 0;
        int64_t mmap_offset = 0;  // file offset of the mapping (0: whole file)

        // H6 whole-file mapping cache. The default is a BUILD-TIME decision
        // (§3: prefer compile-time decisions over runtime decisions) because
        // the measured verdict differs by ARCHITECTURE, and the two wheels are
        // already built per-arch:
        //
        //   x86_64 : full ClickBench hot suite 131.5 → 130.0s, narrow-int
        //            scans -11% (ranges separated), string scans +2%.  ON.
        //   arm64  : consistently 2-5% SLOWER across two independent 7-round
        //            interleaved A/Bs, on every query shape measured.  OFF.
        //
        // ⚠ The ARM regression is NOT diagnosed — this split ships the x86 win
        // while that remains open, which is a deliberate call, not a finding.
        // RUGO_LOCAL_MMAP_CACHE=0/1 overrides in either direction so a single
        // binary can still run both arms of an A/B.
        static const bool use_mmap_cache = []() {
            const char* v = getenv("RUGO_LOCAL_MMAP_CACHE");
            if (v != nullptr && v[0] != '\0' && v[1] == '\0') {
                if (v[0] == '1') return true;
                if (v[0] == '0') return false;
            }
            return RUGO_LOCAL_MMAP_CACHE_DEFAULT != 0;
        }();

        bool per_rg_mapped = false;  // true when THIS row group owns the mapping
        if (is_local && !item.column_stats.empty()) {
            auto t_map = std::chrono::steady_clock::now();
            if (use_mmap_cache) {
                LocalFileMapping m = local_file_mapping(item.path);
                mmap_base = m.base;
                mmap_len  = m.len;
            } else {
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
                int fd = open(item.path.c_str(), O_RDONLY | O_CLOEXEC);
                if (fd >= 0) {
                    mmap_base = mmap(nullptr, mmap_len, PROT_READ, MAP_PRIVATE, fd, mmap_offset);
                    close(fd);
                }
                per_rg_mapped = (mmap_base != MAP_FAILED);
                // H13 REJECTED 2026-08-14 — do not re-attempt without new evidence.
                // The read amplification is real: AdvEngineID is 0.96 MB compressed
                // for the whole 100M-row dataset, yet a cold scan of it transfers
                // 56 MB (per-row-group mapping) or 95 MB (whole-file mapping). The
                // arithmetic even fit: read_ahead_kb=128 x 325 row groups = 41.6 MB.
                // But `madvise(MADV_RANDOM)` on the mapped extent changed the bytes
                // read by ZERO — measured cold on x86, both mapping paths, 3 rounds:
                // 56/57 MB with it and 56/57 MB without. So readahead on THIS mapping
                // is not the source, and the fitting arithmetic was a coincidence.
                // Whatever causes the amplification is elsewhere (the footer/bloom
                // read path is the untested candidate — a cold COUNT(*) that projects
                // no column at all already reads 33 MB).
            }
            total_read_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - t_map).count();
        }

        // Remote: the row group's bytes come from its FETCH BLOCK — planned and
        // fetched once for every kept row group of the block (plan_block), so a
        // column's chunks over a grouped block arrive in one range. With a fetch
        // pool the bytes are already here; on the coupled path the first member
        // claimed fetches for the block (ensure_block_fetched). Local files use
        // mmap (above) or per-column pread (in-loop). The path is already a
        // signed/self-authenticating URL when needed, so no auth header is
        // attached here.
        const bool remote = !is_local;
#ifndef RUGO_ENABLE_HTTP
        if (remote)
            reject_remote_path(item.path);
#endif
        if (remote && item.block)
            ensure_block_fetched(item);

        // Per-column base offset — for a remote row group the values the block
        // plan was built from, so decode and fetch cannot derive different
        // extents for the same chunk; for a local one computed here (nothing
        // was planned).
        std::vector<int64_t> base_offsets;
        if (remote && item.block && item.block->fetched && !item.block->error)
            base_offsets = item.block->base_offsets[item.member];
        else
            base_offsets = compute_base_offsets(item);

        // col_ptr/col_len map each column onto its slice within one of the
        // block's coalesced-group buffers.
        std::vector<const uint8_t*>       col_ptr;
        std::vector<size_t>               col_len;
        // Page pruning: a column whose surviving pages arrive as several runs is
        // reassembled into one chunk-sized buffer (unfetched pages stay zero
        // holes the decoder jumps over); a column fetched whole keeps the
        // zero-copy slice into its group buffer.
        std::vector<std::vector<uint8_t>> assembled;
        std::vector<int64_t>              col_fetched;   // bytes actually transferred per column

        try {
            // PageIndex page pruning — memoised on the item, so on the fetch-ahead
            // path this is the fetch stage's result and no work happens here.
            compute_page_prune(item);
            const PagePrune& pp = item.page_prune;
            // A pass-2 mask (item.row_mask) is exact and wins outright; page
            // pruning only ever runs on an unmasked item (compute_page_prune).
            const uint8_t* mask_ptr = !item.row_mask.empty() ? item.row_mask.data()
                                    : (pp.active ? pp.row_mask.data() : nullptr);
            // Every page of every predicate column pruned: no row survives, so
            // the row group is empty_filtered exactly like a dictionary miss —
            // no fetch, no decode, the consumer skips it.
            if (pp.active && pp.all_pruned) {
                result.empty_filtered = true;
                result.empty_rows = static_cast<int64_t>(pp.row_mask.size());
            }
#ifdef RUGO_ENABLE_HTTP
            if (remote && !item.column_stats.empty() && !result.empty_filtered) {
                const size_t ncols = item.column_stats.size();
                if (!item.block)
                    throw std::logic_error("remote row group submitted without a fetch block");
                const FetchBlock& blk = *item.block;
                if (blk.error)
                    std::rethrow_exception(blk.error);
                if (!blk.fetched)
                    throw std::logic_error("remote row group decoded before its block was fetched");
                const RemotePlan& plan = blk.plan;
                const std::vector<std::vector<uint8_t>>& remote_buffers = blk.buffers;
                // The block's GETs cost what they cost: folded into the FIRST
                // member's read_ns, once — a per-member copy would report the
                // same wait G times.
                if (item.member == 0) total_read_ns += blk.fetch_ns;

                // Point each of THIS member's columns at its bytes. A column
                // fetched as ONE extent is a zero-copy slice of its group
                // buffer; one fetched as several runs is reassembled into a
                // chunk-sized buffer at each run's own offset. A short/missing
                // buffer for ANY of a column's runs leaves the column's view
                // null and is caught at the decode site — a half-assembled
                // buffer would read a zero hole as end-of-column, never an
                // error, so it must not reach the decoder.
                const size_t slot0 = item.member * ncols;
                col_ptr.assign(ncols, nullptr);
                col_len.assign(ncols, 0);
                col_fetched.assign(ncols, 0);
                assembled.assign(ncols, {});
                std::vector<int>     n_ext(ncols, 0);
                std::vector<int>     n_ok(ncols, 0);
                // Zero-copy is only sound when the column's ONE extent is the
                // whole chunk frame: the decoder's offsets (and a jump plan's)
                // are relative to cstart, so a single surviving RUN that starts
                // later must still be placed at its own offset in an assembled
                // buffer, never handed over as if it began the chunk.
                std::vector<uint8_t> whole(ncols, 0);
                auto mine = [&](size_t slot) { return slot >= slot0 && slot < slot0 + ncols; };
                for (const auto& e : plan.extents) if (mine(e.slot)) ++n_ext[e.slot - slot0];
                for (const auto& e : plan.extents) {
                    if (!mine(e.slot)) continue;
                    const size_t i = e.slot - slot0;
                    if (n_ext[i] == 1 && e.start == plan.cstart[e.slot] &&
                        e.end == plan.cstart[e.slot] + plan.clen[e.slot])
                        whole[i] = 1;
                }
                for (size_t gi = 0; gi < plan.groups.size() && gi < remote_buffers.size(); ++gi) {
                    const RemotePlan::Group& g = plan.groups[gi];
                    const std::vector<uint8_t>& buf = remote_buffers[gi];
                    for (size_t xi : g.extents) {
                        const RemotePlan::Extent& e = plan.extents[xi];
                        if (!mine(e.slot)) continue;
                        const size_t i = e.slot - slot0;
                        const size_t off = static_cast<size_t>(e.start - g.start);
                        const size_t len = static_cast<size_t>(e.end - e.start);
                        if (off + len > buf.size()) continue;   // short: column stays null
                        ++n_ok[i];
                        col_fetched[i] += static_cast<int64_t>(len);
                        if (whole[i]) {
                            col_ptr[i] = buf.data() + off;
                            col_len[i] = len;
                        } else {
                            auto& a = assembled[i];
                            if (a.empty()) a.assign(static_cast<size_t>(plan.clen[e.slot]), 0);
                            std::memcpy(a.data() + static_cast<size_t>(e.start - plan.cstart[e.slot]),
                                        buf.data() + off, len);
                        }
                    }
                }
                for (size_t i = 0; i < ncols; ++i) {
                    if (!whole[i] && n_ok[i] == n_ext[i] && !assembled[i].empty()) {
                        col_ptr[i] = assembled[i].data();
                        col_len[i] = assembled[i].size();
                    } else if (n_ok[i] != n_ext[i]) {
                        col_ptr[i] = nullptr;
                        col_len[i] = 0;
                    }
                }
            }
#endif

            // Reused across the columns of this row group: DecodeColumnFromChunk
            // resets it at entry and retains its vector capacity, so per-column
            // decode stops re-mallocing the DecodedColumn's ~25 buffers. Function-
            // local → one per worker invocation, no cross-thread sharing.
            DecodedColumn scratch;
            for (size_t i = 0; i < item.column_stats.size() && !result.empty_filtered; ++i) {
                const auto& col_stats = item.column_stats[i];
                // PageIndex jump plan for this column (nullptr = header-walk).
                const PageJumpPlan* jump_ptr =
                    (pp.active && i < pp.jump.size() && pp.jump[i].size() > 0) ? &pp.jump[i] : nullptr;

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
                // Roll-out: plain int + int-backed TIMESTAMP/DATE/DECIMAL + float.
                // Int/temporal coercions are shape-preserving (retag / dict-only
                // reinterpret), so a Dict-shaped int64 stays Dict; float needs no
                // coercion. Phase 2 membership-skip stays int-only (decode gate) —
                // float equality membership is out of scope.
                //
                // int-backed DECIMAL (physical int32/int64, precision <= 18 — an
                // int128 decimal is FLBA/byte_array and so never matches `pt` here)
                // joined this list on 2026-08-18. It is NOT a change to the Stage-4a
                // gate below: such a column still leaves on the POOL path, because
                // `safe_logical` requires int128_values for a decimal logical type.
                // What changes is only WHICH pool encoding it leaves as —
                // serialize_int64/serialize_int32 emit TAG_INT64_DICT (dictionary +
                // per-row codes) instead of expanding the RLE runs to one int64 per
                // row via serialize_rle_int_as_int64. Both consumers of that tag build
                // a DECIMAL-tagged vector, never a bare INT64, so the historic
                // "reinterpret trap" (tpch Q01 dec_mul type error, which came from a
                // consumer taking a DIRECT DK_INT64 at face value) cannot recur:
                // native scan -> build_pool_decimal_column's kTagInt64Dict branch
                // (draken_vector_from_dict, DRAKEN_DECIMAL); trampoline scan ->
                // INT64 dict vector + vector_reinterpret_as_decimal, which is
                // shape-preserving and retags dict->dict.
                // Armed under a row_mask too (pass-2, page pruning): the decoder
                // compacts the codes to the survivors and the column stays
                // Dict-shaped — see DecodeColumnFromChunk's contract in decode.hpp.
                const bool prefer_dict =
                    col_stats.dictionary_page_offset >= 0 &&
                    (((pt == "int64" || pt == "int32") &&
                      (cl.empty() || cl == "int64" || cl == "int32" ||
                       cl.rfind("timestamp", 0) == 0 || cl.rfind("date", 0) == 0 ||
                       cl.rfind("decimal", 0) == 0)) ||
                     ((pt == "float64" || pt == "float32") &&
                      (cl.empty() || cl == "float64" || cl == "float32")));

                // Phase 2: pushed dictionary decode-skip predicate for this column
                // (if any). Independent of prefer_dict — the probe only needs the
                // dictionary (decoded before any data page), not the dict-shaped
                // surviving representation.
                //
                // Armed only when the CALLER supplied no mask. A page-pruned row
                // group qualifies (its mask is derived here, item.row_mask stays
                // empty) and wants the probe. A pass-2 late-materialization item
                // does not: its rows already matched the predicate, so the probe
                // could only ever agree — and leaving it off keeps the pass-2
                // consumer's "a masked submit never comes back empty" invariant
                // exactly as strong as it was.
                DictSkipPredicate skip;
                const DictSkipPredicate* skip_ptr = nullptr;
                if (item.row_mask.empty() && !dict_preds_.empty()) {
                    auto nit = dict_preds_.find(col_stats.name);
                    if (nit != dict_preds_.end()) {
                        skip.kind = nit->second.kind;
                        skip.int_vals = &nit->second.int_vals;
                        skip.str_vals = &nit->second.str_vals;
                        skip_ptr = &skip;
                    }
                }

                DecodedColumn& decoded = scratch;   // reused; reset at decode entry
                // H15 (2026-08-14, unratified): a SMALL column chunk is cheaper to
                // pread than to fault in. MEASURED (x86 cold, `SUM(AdvEngineID)` over
                // 100 files, column = 0.96 MB total): bytes requested via syscalls are
                // IDENTICAL to a zero-column COUNT(*) (11.3 MB both), yet the device
                // delivers 42 MB more — all of it arriving through mmap page faults,
                // ~170 KB faulted per mapping to read a ~3 KB chunk. That is
                // fault-around (filemap_map_pages / fault_around_bytes), which is why
                // the rejected MADV_RANDOM attempt did nothing: MADV_RANDOM suppresses
                // readahead, not fault-around. A pread asks for exactly the bytes we
                // want, at the cost of one copy into a heap buffer — a good trade only
                // while the chunk is small, so large chunks keep the zero-copy slice.
                // Threshold in bytes; RUGO_PREAD_SMALL_CHUNKS=0 disables (A/B arm).
                static const size_t pread_below = []() -> size_t {
                    const char* v = getenv("RUGO_PREAD_SMALL_CHUNKS");
                    if (v != nullptr && *v != '\0') return strtoull(v, nullptr, 10);
                    return 256u * 1024u;
                }();
                const bool small_chunk_pread =
                    is_local && pread_below > 0 &&
                    static_cast<size_t>(chunk_size) < pread_below;
                if (mmap_base != MAP_FAILED && !small_chunk_pread) {
                    // Zero-copy: slice directly into the mmap — no heap allocation.
                    const uint8_t* chunk_ptr =
                        static_cast<const uint8_t*>(mmap_base) + (base_offset - mmap_offset);
                    auto t_dec = std::chrono::steady_clock::now();
                    DecodeColumnFromChunk(scratch,
                        chunk_ptr, static_cast<size_t>(chunk_size), &adjusted, mask_ptr, prefer_dict, skip_ptr, jump_ptr);
                    total_decode_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - t_dec).count();
                    // A jumped-over page is never faulted in from the mapping.
                    result.bytes_fetched += chunk_size -
                        (jump_ptr != nullptr ? pruned_bytes_of(*jump_ptr) : 0);
                } else if (remote) {
                    // Fetched with the block: decode straight from the buffer.
                    // No bloom filter rides here — the writer puts every bloom
                    // in the file tail (docs/PARQUET_GROUPED_COLUMN_MAJOR_DESIGN.md
                    // [D-6]: the remote bloom decode-skip was dropped with it;
                    // bloom pruning is a plan-time, local-footer concern).
                    if (i >= col_ptr.size() || col_ptr[i] == nullptr) {
                        result.success = false;
                        result.error = "coalesced range fetch did not cover column " +
                                       std::to_string(i);
                        break;
                    }
                    const uint8_t* raw_data = col_ptr[i];
                    const size_t   raw_size = col_len[i];
                    result.bytes_fetched += col_fetched[i];   // what was actually transferred
                    auto t_dec = std::chrono::steady_clock::now();
                    DecodeColumnFromChunk(scratch,
                        raw_data, raw_size, &adjusted, mask_ptr, prefer_dict, skip_ptr, jump_ptr);
                    total_decode_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - t_dec).count();
                } else {
                    // Local file, reached either because the mmap failed OR because
                    // H15 chose a pread for a small chunk (see small_chunk_pread).
                    auto [raw_bytes, read_ns] = read_range(item.path, base_offset, chunk_size);
                    result.bytes_fetched += chunk_size;
                    total_read_ns += read_ns;
                    auto t_dec = std::chrono::steady_clock::now();
                    DecodeColumnFromChunk(scratch,
                        raw_bytes.data(), raw_bytes.size(), &adjusted, mask_ptr, prefer_dict, skip_ptr, jump_ptr);
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
                // Clustering hint: copied from this row group's own footer claim
                // (already trust-gated by metadata.cpp's created_by check), applies
                // regardless of which branch below (direct/pool) ends up building
                // this column.
                cout.row_sorted = col_stats.is_sorted;
                cout.row_sorted_descending = col_stats.sort_descending;
                // Same posture as the clustering hint above: copied from this
                // file's footer whichever branch below builds the column.
                cout.draken_logical_kind = col_stats.draken_logical_kind;
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

                // Ingestion canonicalisation, before ANY vector shape is built
                // from `decoded` — see `canonicalise_decoded_floats`. Placed
                // ahead of the DK_POOL/direct split so both paths ingest the
                // same values: a canon on one branch only would make a -0.0's
                // survival depend on how its column happened to be encoded.
                canonicalise_decoded_floats(decoded);

                const std::string& lt = col_stats.logical_type;
                const bool safe_logical =
                    lt.empty() || lt == "int64" || lt == "int32" ||
                    // A1: signed narrow ints (int8/int16) decode to their exact
                    // declared width (DK_INT8/DK_INT16) — no consumer-side coercion —
                    // so they are direct-eligible. Without this they fell to DK_POOL,
                    // which the native scan Source cannot decode for a numeric column
                    // (only the trampoline's pool deserializer could), so admitting
                    // them to the native scan raised "unsupported column encoding".
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
                    // E37: this column carries a hash seed only if the plan flagged
                    // it a downstream key (parallel to the projected column order).
                    const bool want_seed = (i < hash_key_columns_.size()) && (hash_key_columns_[i] != 0);
                    // Every read of this column is length-answerable (proved by
                    // LengthOnlyColumnStrategy) -> its long-value payloads need not
                    // be materialized. Movers that copy payloads without reading them
                    // (string_gather.h materialize/slice/take/compress, concat_string,
                    // consolidate_string_block) determine "no payload" PER SLOT via
                    // STR_ELIDED_PAYLOAD_OFFSET on the slot itself, never by trusting
                    // DrakenStringArena.payloads_elided (that struct byte is not
                    // self-zeroing across every arena constructor in the tree, so an
                    // uninitialised one could misread as elided — see buffers.h).
                    const bool length_only = (i < length_only_columns_.size()) && (length_only_columns_[i] != 0);
                    if (dk == DK_BOOL)
                        ok = build_direct_bool(decoded, pool_sink_.draken_alloc, pool_sink_.draken_free, cout);
                    else if (dk == DK_VARCHAR)
                        ok = build_direct_string_plain(decoded, pool_sink_.draken_alloc, pool_sink_.draken_free, cout, want_seed, length_only);
                    else if (dk == DK_VARCHAR_DICT)
                        ok = build_direct_string_dict(decoded, pool_sink_.draken_alloc, pool_sink_.draken_free, cout, want_seed, length_only);
                    else if (dk == DK_INT64_DICT)
                        ok = build_direct_int64_dict(decoded, pool_sink_.draken_alloc, pool_sink_.draken_free, cout);
                    else if (dk == DK_FLOAT64_DICT)
                        ok = build_direct_float_dict(decoded, false, pool_sink_.draken_alloc, pool_sink_.draken_free, cout);
                    else if (dk == DK_FLOAT32_DICT)
                        ok = build_direct_float_dict(decoded, true, pool_sink_.draken_alloc, pool_sink_.draken_free, cout);
                    else if (dk == DK_UINT8_DICT || dk == DK_INT8_DICT)
                        ok = build_direct_narrow_dict(decoded, 1, pool_sink_.draken_alloc, pool_sink_.draken_free, cout);
                    else if (dk == DK_UINT16_DICT || dk == DK_INT16_DICT)
                        ok = build_direct_narrow_dict(decoded, 2, pool_sink_.draken_alloc, pool_sink_.draken_free, cout);
                    else if (dk == DK_UINT32_DICT || dk == DK_INT32_DICT)
                        ok = build_direct_narrow_dict(decoded, 4, pool_sink_.draken_alloc, pool_sink_.draken_free, cout);
                    else if (dk == DK_UINT64_DICT)
                        ok = build_direct_narrow_dict(decoded, 8, pool_sink_.draken_alloc, pool_sink_.draken_free, cout);
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

        // H6: a cached mapping is pipeline-owned (local_mmap_cache_) — released
        // in the destructor after wait_shutdown(). Only the toggle's per-RG arm
        // unmaps here.
        if (per_rg_mapped)
            munmap(mmap_base, mmap_len);

        // This row group's share of the block's compressed bytes is dead once
        // this scope ends (the buffers themselves go when the last member drops
        // the block); release its charge now so a waiting fetch can proceed.
        assembled.clear();
        item.block.reset();
        ledger_release(held_prefetch_, item.charged_compressed);
        item.charged_compressed = 0;

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
            } else {
                // Dropped: nobody will pop it, so its decoded charge comes back
                // here instead (the release is a no-op when the budget is off).
                held_decoded_.fetch_sub(result.charged_bytes, std::memory_order_relaxed);
            }
        }
        pending_work_--;
        if (memory_budget_bytes_ > 0) queue_cv_.notify_all();
        else queue_cv_.notify_one();
    }

    static int64_t pruned_bytes_of(const PageJumpPlan& jp) {
        int64_t n = 0;
        for (size_t p = 0; p < jp.size(); ++p) if (jp.pruned[p]) n += jp.page_sizes[p];
        return n;
    }

 public:
    // E37: per-projected-column key flag (parallel to the scan's column order).
    // 1 = build the hash seed for this string column (it is a downstream GROUP BY/
    // JOIN/DISTINCT key); 0 (default when unset) = cheap slot build, NO XXH3. Set
    // once by the planner via open_native_scan_plan. Empty → nothing keyed → the
    // standalone-rugo / SELECT-*/ LIKE default of zero string hashing.
    std::vector<uint8_t> hash_key_columns_;
    void set_hash_key_columns(const std::vector<uint8_t>& v) { hash_key_columns_ = v; }
    // Parallel to the projected column order. 1 = the optimizer proved every read
    // of this column is length-answerable (IsEmpty/IsNotEmpty/LENGTH), so its
    // long-value payload is never read and need not be materialized. All-zero
    // (the default) leaves decoding byte-for-byte unchanged.
    std::vector<uint8_t> length_only_columns_;
    void set_length_only_columns(const std::vector<uint8_t>& v) { length_only_columns_ = v; }

#ifdef RUGO_ENABLE_HTTP
    // Query-scoped HTTP tuning (host-connection cap / retries / bandwidth-derived
    // timeout). Set once by the planner, BY VALUE, before any submit — mirrors
    // decode_workers/hash_key_columns_. NOT stored on HttpClient itself: HttpClient
    // is thread_local and outlives any one query (see tl_http_client() below), so
    // read_range() passes &http_tuning_ into get()/get_many() on every call
    // instead of mutating shared client state. http_tuning_set_ == false (the
    // default when unset) means "use HttpClient::default_tuning()" — the
    // env-derived process defaults, unchanged from before this existed.
    HttpTuning http_tuning_;
    bool http_tuning_set_ = false;
    void set_http_tuning(const HttpTuning& t) { http_tuning_ = t; http_tuning_set_ = true; }

    // Query-scoped Authorization header for the remote fetches. Same lifecycle as
    // http_tuning_ above and for the same reason: HttpClient is thread_local and
    // outlives any one query, so the credential travels with each request rather
    // than being stashed on shared client state.
    //
    // This is the alternative to pre-signing every object. A signed URL carries
    // its credential in the query string, which costs one IAM signBlob RPC PER
    // FILE on Compute Engine / Cloud Run — there is no local private key on those,
    // so the client library delegates to the IAM API (measured ~63ms each, ~6.3s
    // for a 100-file scan). A bearer token is one credential for the caller,
    // minted once per query, covering every object it touches.
    //
    // Empty (the default) means no Authorization header is sent, which is correct
    // ONLY while the caller is still pre-signing: the URL then carries its own
    // credential. Leaving this unset on an unsigned URL yields a 401, never an
    // anonymous read.
    std::string auth_header_;
    bool auth_header_set_ = false;
    void set_auth_header(const std::string& v) { auth_header_ = v; auth_header_set_ = true; }

    // All three remote-GET sites build their header map here so they cannot drift
    // apart: a site that silently omitted the credential would 401, and only on
    // the deployments that lack a local signing key.
    std::map<std::string, std::string> http_headers_(const std::string& range_hdr) const {
        std::map<std::string, std::string> h{{"Range", range_hdr}};
        if (auth_header_set_ && !auth_header_.empty()) {
            h.emplace("Authorization", auth_header_);
        }
        return h;
    }

    // Fetch-ahead depth: a dedicated pool of `depth` threads that only issue
    // the remote range GETs, so concurrent fetches are no longer pinned to the
    // decode thread count (measured: with 4 decode workers, deepening the
    // submission window 6 -> 64 moved nothing, 4.54s -> 4.45s, because a ticket
    // beyond the pool size merely queues). 0 = off = the coupled path, exactly.
    // Set once at plan time, before any submit_row_group — the routing decision
    // in enqueue_block reads fetch_pool_ unsynchronised on that promise.
    //
    // Depth is bounded by the caller's SUBMISSION window (in_flight_limit):
    // fetch-ahead can only run as far ahead as there are submitted items, so
    // the caller must size the window >= depth (pool_reader.pyx derives it).
    // Memory: each in-flight item holds its COMPRESSED row-group bytes from
    // fetch until decode — worst case in_flight_limit x row-group bytes on top
    // of the existing pool reservation.
    void set_fetch_ahead(int depth) {
        if (depth <= 0) throw std::invalid_argument(
            "set_fetch_ahead: depth must be positive (got " + std::to_string(depth) + ")");
        if (fetch_pool_) throw std::logic_error("set_fetch_ahead: already set");
        if (pending_work_.load(std::memory_order_relaxed) != 0 ||
            enqueue_count_.load(std::memory_order_relaxed) != 0)
            throw std::logic_error("set_fetch_ahead: called after work was submitted");
        fetch_ahead_ = depth;
        fetch_pool_ = std::make_unique<BS::thread_pool<BS::tp::priority>>(depth);
    }

    // Memory admission budget in bytes (0 = off). Set once at plan time, before
    // any submit — the claim/admission paths read it unsynchronised on that
    // promise, exactly like set_fetch_ahead. The read-back is memory_budget_bytes().
    void set_memory_budget(int64_t bytes) {
        if (bytes < 0) throw std::invalid_argument(
            "set_memory_budget: bytes must be >= 0 (got " + std::to_string(bytes) + ")");
        if (pending_work_.load(std::memory_order_relaxed) != 0 ||
            enqueue_count_.load(std::memory_order_relaxed) != 0)
            throw std::logic_error("set_memory_budget: called after work was submitted");
        memory_budget_bytes_ = bytes;
    }
    // Primitive-args overload: Cython declares HttpTuning-by-struct awkwardly
    // (it's a plain C++ aggregate, not exposed to Python), so the binding calls
    // this instead of constructing an HttpTuning on the Cython side.
    void set_http_tuning(long max_host_connections, int max_retries,
                          double min_bandwidth_bytes_per_s, long timeout_floor_ms,
                          bool use_multiplexing, bool use_pipewait, bool force_http11) {
        HttpTuning t;
        t.max_host_connections = max_host_connections;
        t.max_retries = max_retries;
        t.min_bandwidth_bytes_per_s = min_bandwidth_bytes_per_s;
        t.timeout_floor_ms = timeout_floor_ms;
        t.use_multiplexing = use_multiplexing;
        t.use_pipewait = use_pipewait;
        t.force_http11 = force_http11;
        set_http_tuning(t);
    }
#endif

    // ── Remote range coalescing (see the merge loop in decode_row_group) ─────
    // Parquet stores a row group's column chunks CONTIGUOUSLY, so a wide
    // projection is one unbroken extent that we nonetheless issue as N separate
    // range GETs (measured: 105 columns of ClickBench hits = 105 requests per
    // row group, and merging them all wastes 0.000% of the bytes).
    //   waste_ratio: merge a run while bytes THROWN AWAY stay within this
    //     fraction of bytes actually needed. 0.0 = merge only touching chunks
    //     (byte-neutral). Sparse projections self-limit: skipping a fat column
    //     you did not select blows the budget and splits the run.
    //   max_bytes: ceiling on one merged request. A single huge GET serialises
    //     what were concurrent transfers — measured 1x16MB (1.05s) SLOWER than
    //     8x2MB (0.79s) for identical bytes — so unbounded merging is not free.
    //     0 = unbounded.
    double  coalesce_waste_ratio_ = 0.10;
    int64_t coalesce_max_bytes_   = 0;
    void set_coalesce_tuning(double waste_ratio, int64_t max_bytes) {
        coalesce_waste_ratio_ = waste_ratio;
        coalesce_max_bytes_   = max_bytes;
    }

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
        // H6: release the whole-file mappings only after wait_shutdown() — no
        // ticket can still hold a slice into them. No lock needed: workers are
        // done and the destructor is single-threaded by definition.
        for (auto& kv : local_mmap_cache_) {
            if (kv.second.base != MAP_FAILED)
                munmap(kv.second.base, kv.second.len);
        }
        local_mmap_cache_.clear();
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
    uint32_t trace_node_id() const {
        return trace_node_id_;
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

    // Block ids per row group of `fs` for the projected `column_names`: row
    // group k+1 shares row group k's block when EVERY projected column's chunk
    // in k+1 starts exactly where its chunk in k ends — the byte adjacency the
    // grouped writer produces (write_block). Inferred from the chunk offsets,
    // never from metadata, so a row-major file (rugo's before the grouped
    // layout, pyarrow's, anyone's) simply reads as one block per row group.
    // Ids are 0-based and non-decreasing. A projected column missing from a
    // row group, or no projected column at all, breaks the run: no adjacency
    // evidence, no block.
    static std::vector<int32_t> infer_fetch_blocks(
            const FileStats& fs, const std::vector<std::string>& column_names) {
        const size_t n = fs.row_groups.size();
        std::vector<int32_t> ids(n, 0);
        if (n == 0) return ids;
        auto chunk_span = [&](const RowGroupStats& rg, const std::string& name,
                              int64_t& start, int64_t& end) -> bool {
            for (const ColumnStats& cs : rg.columns) {
                if (cs.name != name) continue;
                if (cs.data_page_offset < 0 || cs.total_compressed_size < 0) return false;
                start = cs.data_page_offset;
                if (cs.dictionary_page_offset >= 0 && cs.dictionary_page_offset < start)
                    start = cs.dictionary_page_offset;
                end = start + cs.total_compressed_size;
                return true;
            }
            return false;
        };
        int32_t id = 0;
        for (size_t k = 1; k < n; ++k) {
            bool adjacent = !column_names.empty();
            for (const std::string& name : column_names) {
                int64_t s0, e0, s1, e1;
                if (!chunk_span(fs.row_groups[k - 1], name, s0, e0) ||
                    !chunk_span(fs.row_groups[k], name, s1, e1) || s1 != e0) {
                    adjacent = false;
                    break;
                }
            }
            if (!adjacent) ++id;
            ids[k] = id;
        }
        return ids;
    }

    /**
     * Submit one FETCH BLOCK: the kept row groups of one block of one file,
     * read together — one coalesced fetch for a remote file — and decoded,
     * claimed and returned one result per row group. `column_stats[m]` carry
     * absolute file offsets for member m (parallel to `rg_idx`); `row_masks`,
     * when non-empty, is parallel too, an empty mask meaning "decode all rows".
     * Which row groups form a block is the caller's decision, made from the
     * file's chunk offsets (infer_fetch_blocks) — the pipeline plans whatever it
     * is handed, and a single row group is a one-member block.
     */
    void submit_block(const std::string& path, const std::vector<int>& rg_idx,
                      const std::vector<std::string>& column_names,
                      const std::vector<std::vector<ColumnStats>>& column_stats,
                      const std::vector<std::vector<uint8_t>>& row_masks = {}) {
        if (shutdown_) return;
        if (rg_idx.empty())
            throw std::invalid_argument("submit_block: a block needs at least one row group");
        if (column_stats.size() != rg_idx.size())
            throw std::invalid_argument("submit_block: column_stats must be parallel to rg_idx");
        if (!row_masks.empty() && row_masks.size() != rg_idx.size())
            throw std::invalid_argument("submit_block: row_masks must be parallel to rg_idx");
        for (const auto& cs : column_stats) {
            if (cs.size() != column_names.size())
                throw std::invalid_argument(
                    "submit_block: every member must carry stats for every projected column");
        }
        const bool remote = !path_is_local(path);
        std::shared_ptr<FetchBlock> blk;
        if (remote) {
            blk = std::make_shared<FetchBlock>();
            blk->path = path;
            blk->ncols = column_names.size();
        }
        std::vector<WorkItem> members;
        members.reserve(rg_idx.size());
        for (size_t m = 0; m < rg_idx.size(); ++m) {
            WorkItem item;
            item.path = path;
            item.rg_idx = rg_idx[m];
            item.column_names = column_names;
            item.column_stats = column_stats[m];
            if (!row_masks.empty()) item.row_mask = row_masks[m];
            item.est_decoded_bytes = sum_column_bytes(column_stats[m], false);
            item.est_compressed_bytes = sum_column_bytes(column_stats[m], true);
            item.block = blk;
            item.member = m;
            members.push_back(std::move(item));
        }
        enqueue_block(std::move(blk), std::move(members));
    }

    /**
     * Submit a row group for read + decode + serialize: a one-member block.
     * column_stats carry absolute file offsets — worker adjusts to buffer-relative.
     */
    void submit_row_group(const std::string& path, int rg_idx,
                          const std::vector<std::string>& column_names,
                          const std::vector<ColumnStats>& column_stats) {
        submit_block(path, std::vector<int>{rg_idx}, column_names,
                     std::vector<std::vector<ColumnStats>>{column_stats});
    }

    /**
     * Submit a row group with a per-row mask (1=keep, 0=skip).
     * Workers apply the mask during decode so only surviving rows are serialized.
     */
    void submit_row_group(const std::string& path, int rg_idx,
                          const std::vector<std::string>& column_names,
                          const std::vector<ColumnStats>& column_stats,
                          const std::vector<uint8_t>& row_mask) {
        submit_block(path, std::vector<int>{rg_idx}, column_names,
                     std::vector<std::vector<ColumnStats>>{column_stats},
                     std::vector<std::vector<uint8_t>>{row_mask});
    }

    bool try_get_result(MorselRef& out) {
        std::lock_guard<std::mutex> lk(queue_mutex_);
        if (result_queue_.empty()) return false;
        out = std::move(result_queue_.front());
        result_queue_.pop_front();
        pop_release_locked(out);
        return true;
    }

    // Under queue_mutex_: a popped result gives its decoded charge back and
    // wakes whoever is blocked — a back-pressured producer (queue was full) or,
    // with a budget, every admission waiter (notify_all: waiters have different
    // thresholds, and the one woken by notify_one may not be the one that fits).
    void pop_release_locked(MorselRef& out) {
        if (memory_budget_bytes_ > 0) {
            held_decoded_.fetch_sub(out.charged_bytes, std::memory_order_relaxed);
            out.charged_bytes = 0;
            queue_cv_.notify_all();
        } else {
            queue_cv_.notify_one();
        }
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
                pop_release_locked(out);
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
                // The consumer IS the drain: it never waits on admission, but it
                // still charges so the release at its own pop stays balanced.
                claim_charge_locked(item);
                inline_decodes_.fetch_add(1, std::memory_order_relaxed);
                lk.unlock();
                decode_row_group(item);  // does its own queue_mutex_ locking + notify
                lk.lock();
                continue;  // our decode likely enqueued a result — re-check
            }
            // Nothing ready and nothing to help with: sleep until a pool worker
            // produces a result, a new item is published, or we shut down. This
            // is the genuine stall case — distinct from TC_QUEUE_WAIT (an item
            // sitting claimable in pending_items_) and from inline_decodes_
            // (the consumer found work and helped instead of blocking) — so it
            // gets its own span, recorded around the actual wait.
            const bool _tr_stall_on = draken_trace_enabled();
            const uint64_t _tr_stall_start = _tr_stall_on ? draken_trace_now_ns() : 0;
            queue_cv_.wait(lk, [this]() {
                return !result_queue_.empty()
                    || shutdown_.load(std::memory_order_relaxed)
                    || !pending_items_.empty();
            });
            if (_tr_stall_on) {
                const auto _tr_idx = BS::this_thread::get_index();
                const uint16_t _tr_worker =
                    _tr_idx.has_value() ? static_cast<uint16_t>(*_tr_idx) : 0xFFFFu;
                draken_trace_record(DRAKEN_TC_QUEUE_STALL, trace_node_id_,
                    0, 0, _tr_worker, _tr_stall_start, draken_trace_now_ns(),
                    0, 0, 0, 0);
            }
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
        // Fetch-ahead: drain the FETCH stage first. Every fetch ticket dispatches
        // a decode ticket as its last act, so decode cannot be quiescent while a
        // fetch ticket is still running. Exclusive pool, so wait() is safe here
        // in a way decode_pool_->wait() is not when the decode pool is injected.
        if (fetch_pool_) fetch_pool_->wait();
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

    // 0 when fetch-ahead is off, else the fetch pool's thread count — the max
    // number of range GETs that can be in flight independent of decode. This
    // is the read-back that proves the knob reached the pipeline.
    int fetch_ahead_depth() const { return fetch_ahead_; }
    uint64_t prefetch_discarded_bytes() const {
        return prefetch_discarded_bytes_.load(std::memory_order_relaxed);
    }

    // Memory admission read-backs: the budget this pipeline actually runs (0 =
    // off), the peak of both ledgers together, and how often / how long a
    // ticket waited at the gate.
    int64_t memory_budget_bytes() const { return memory_budget_bytes_; }
    int64_t memory_held_high_watermark() const {
        return held_high_watermark_.load(std::memory_order_relaxed);
    }
    uint64_t admission_blocked_ns() const {
        return admission_blocked_ns_.load(std::memory_order_relaxed);
    }
    uint64_t admission_waits() const {
        return admission_waits_.load(std::memory_order_relaxed);
    }

    // PageIndex page pruning read-backs. pages/bytes count every projected
    // column's pruned pages (header + payload); row_groups counts the ones
    // where nothing survived at all; fetches/bytes_fetched are the index-region
    // reads themselves (one per file, widened at most once).
    uint64_t page_index_pages_pruned() const {
        return page_index_pages_pruned_.load(std::memory_order_relaxed);
    }
    uint64_t page_index_bytes_pruned() const {
        return page_index_bytes_pruned_.load(std::memory_order_relaxed);
    }
    uint64_t page_index_row_groups_pruned() const {
        return page_index_row_groups_pruned_.load(std::memory_order_relaxed);
    }
    uint64_t page_index_fetches() const {
        return page_index_fetches_.load(std::memory_order_relaxed);
    }
    uint64_t page_index_bytes_fetched() const {
        return page_index_bytes_fetched_.load(std::memory_order_relaxed);
    }
    // Row groups where the index was NOT read because it would have cost more
    // than a tenth of the bytes it could remove (see the cost gate). A scan
    // reporting 0 pages pruned AND 0 declines had no page index to read at all;
    // one reporting declines chose not to look, which is a different thing.
    uint64_t page_index_gate_declines() const {
        return page_index_gate_declines_.load(std::memory_order_relaxed);
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
