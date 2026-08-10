#pragma once
// src/cpp/engine/native_parquet_scan_source.hpp — a genuinely native (zero-Python)
// parquet scan Source for the morsel-driven engine.
//
// This is NOT a nogil-annotated wrapper around the existing Cython scan
// (opteryx/connectors/parquet_io/pool_reader.pyx's IpcRowGroupSource /
// opteryx/operators/parquet_read/parquet_read.pyx's next_morsel). Those stay
// exactly as they are, unmodified, for every query shape they already serve.
// This is new, parallel infrastructure: get_morsel() below never constructs a
// PyObject, never needs a Python thread state, and is safe to call concurrently
// from any number of native worker threads with no GIL/attach cost whatsoever —
// there is no Python to attach to.
//
// What makes this possible: the parquet decode itself (rugo::ParquetIOPipeline,
// rugo/src/parquet/io_pipeline.hpp) is already pure C++ and already thread-safe
// (submit/wait are nogil-capable, MorselRef/ColumnOut are plain structs). And the
// "sanctioned way to populate a DrakenVector" (draken_vector_from_dense /
// draken_vector_from_dict, draken/core/vector_alloc.h) is plain `extern "C"`, no
// nanobind/Python involvement — the PyObject-returning draken_vector_own_* family
// in draken_native.cpp builds the exact same VectorOwner via these, then does ONE
// extra nb::cast to box it for Python. We skip that box entirely and hand the
// VectorOwner straight to a CxxColumn, matching how every other native operator in
// this engine already carries columns.
//
// Scope (first landing — fail loud, not silently, outside it):
//   - Local files only: no GCS signed-URL rewrite, no prefetched-footer dicts.
//   - Single-pass only: no pass-2 late-materialization masks.
//   - No schema evolution: every projected column must be present in every
//     scanned row group (a NativeScanPlan built from a uniform file set).
//   - Fixed-width direct columns: DK_INT64/FLOAT32/FLOAT64/DK_BOOL (dense or
//     dict-shaped, DK_BOOL dense) plus DK_DECIMAL128 (dense only — rugo's decode
//     layer has no "dict-encoded DECIMAL128" direct kind; a dictionary-encoded
//     DECIMAL128 column classifies as DK_POOL and is NOT handled here). A DK_POOL
//     column is decoded only when the plan flagged what it is (decimal / varchar /
//     array, below); an unflagged one sets ErrCtx and stops the scan rather than
//     guessing.
//   - WP-11: a projected DATE / TIMESTAMP / int64-backed DECIMAL column decodes
//     physically as int64 (DK_INT64/DICT) or DK_POOL; the plan flags it via
//     `logical_coerce` (parallel to column_names) and build_column retags it to
//     DRAKEN_DATE32 / TIMESTAMP64 / DECIMAL with the exact unit or precision/scale
//     descriptor, byte-identically to the trampoline scan's `_coerce_vectors`.
//     DECIMAL128 (DK_DECIMAL128) is self-describing via ColumnOut.dec_* and needs
//     no `logical_coerce` entry. BOOL is DK_BOOL dense. Parquet TIME is decoded as
//     plain INT64 (the binder does not model a TIME logical type from a scan), so
//     it flows through the ordinary int path with no coercion — identical to the
//     trampoline.
//   - Exception: DK_POOL columns explicitly flagged via `decimal_columns` (plan-
//     time known to be int64-backed DECIMAL — see native_decimal_pool_decode.hpp
//     for why these are ALWAYS DK_POOL regardless of parquet encoding) are read
//     directly from the wired MemoryPool and built as DRAKEN_DECIMAL.
//   - String columns (VARCHAR / NVARCHAR / VARBINARY) ARE handled, in every shape
//     rugo can emit for a projected byte_array column: DK_VARCHAR (plain dense),
//     DK_VARCHAR_DICT (dict-shaped direct), and DK_POOL (RLE-skip-dense / plain
//     pool fallback, both TAG_STR_DICT and TAG_STR_PLAIN — see
//     native_varchar_pool_decode.hpp). Each is tagged with its declared
//     DrakenType via `string_types` (parallel to column_names), so the general
//     scan path admits string projections (WP-01). A DK_POOL column reaches the
//     VARCHAR decoder when flagged by `string_types` (general scan) or the legacy
//     `varchar_columns` array (agg/join callers).
//   - Exception: DK_POOL columns explicitly flagged via `varchar_columns` (plan-
//     time known to be a GROUP BY VARCHAR key — see native_varchar_pool_decode.hpp
//     for why TPC-H's l_returnflag/l_linestatus land DK_POOL despite being
//     dict-encoded: rugo's "RLE skip-dense" decode path for non-nullable
//     dict-encoded byte_array columns, verified directly against real files)
//     are read from the wired MemoryPool and built as DRAKEN_VARCHAR. Any
//     other DK_POOL column (not flagged either way) still fails loud.
//   - Exception: DK_VARCHAR_DICT columns are supported, built directly via
//     draken_vector_from_dict (type-agnostic — see its .cpp: plain struct
//     population, no per-type branching, so it works for DrakenStringSlot data
//     exactly like it does for int64/float64). Long-string (arena-backed)
//     values ARE supported: DrakenVector itself has no arena field (see
//     draken/core/buffers.h — slots reference it via a byte OFFSET, never an
//     absolute pointer), so the arena transfers into VectorOwner.arena_buf
//     (draken/core/vector_owner.h) instead — any consumer reading a possibly-
//     long VARCHAR value must resolve the arena from the owning CxxColumn
//     (`.own->arena_buf.get()`), not assume inline-only.
//   - R6: ARRAY (parquet LIST) columns ARE handled. A list column always carries
//     repetition levels, so rugo's direct_kind_for classifies it DK_POOL
//     unconditionally (there is no direct list kind) and serializes it as
//     TAG_ARRAY; `array_columns[i]` (parallel to column_names, same convention as
//     decimal_columns / varchar_columns) is the plan-time flag that routes it to
//     native_array_pool_decode.hpp — a faithful port of the trampoline's Cython
//     `_build_array_vector*`. Nested list<list<...>> included. The parent is a
//     dense DRAKEN_ARRAY of int32 offsets whose VectorOwner::child_owner owns the
//     element vector. An ARRAY<TIMESTAMP> leaf is retagged via LC_ARRAY_TIMESTAMP
//     (the trampoline's `vector_retag_array_child_as_timestamp64`). MAP and STRUCT
//     stay fail-closed at the plan-time gate.
// Planning (opening files, fetching footers, pruning row groups, sizing the
// pool) stays exactly where the phase split puts it: Python, done once, before
// any of this runs — see NativeScanPlan / open_native_scan_plan in pool_reader.pyx.

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "operator.hpp"
#include "trace.hpp"              // TC_IO_WAIT / trace_begin / trace_end
#include "io_pipeline.hpp"        // rugo::ParquetIOPipeline, MorselRef, ColumnOut, DK_*
#include "metadata.hpp"           // FileStats, RowGroupStats, ColumnStats
#include "core/vector_alloc.h"    // draken_vector_from_dense / draken_vector_from_dict
#include "core/vector_owner.h"    // VectorOwner, OwnedBuffer
#include "memory_pool.hpp"        // opteryx::MemoryPool
#include "native_decimal_pool_decode.hpp"  // build_pool_decimal_column
#include "native_varchar_pool_decode.hpp"  // build_pool_varchar_dict_column
#include "native_array_pool_decode.hpp"    // build_pool_array_column (R6)
#include "logical_type.h"                  // LogicalType / logical_type_intern (WP-11 descriptors)
#include "core/alloc.h"                    // draken_malloc / draken_free (WP-11 temporal narrow)

// R2 (scan-pushed LIMIT) row truncation. Lives in draken next to the take/slice
// machinery; resolved at load time from draken_native, the same dynamic-lookup
// path native_unnest.hpp's cxx_unnest_c uses.
extern "C" CxxMorsel* cxx_slice_c(const CxxMorsel* m, uint32_t start, uint32_t length);
extern "C" void cxx_morsel_delete(CxxMorsel* m);

namespace opteryx::engine {

// WP-11 per-column logical-coercion packing (parallel to column_names; 0 = none).
// Mirrors the trampoline scan's `_sp_coerce_ops` exactly, packed into one int so it
// threads through the existing `vector<int>`-style plan→native channel:
//   bits [0:4]  kind   0=none 1=decimal(int64-backed) 3=date32 4=timestamp64
//                       5=array-with-timestamp-element
//                       (decimal128 is self-describing via DK_DECIMAL128 +
//                       ColumnOut.dec_* and needs NO entry; parquet TIME is
//                       decoded as plain INT64 — the binder does not model a TIME
//                       logical type — so it takes the ordinary int path, not a
//                       coercion kind)
//   bits [4:8]  unit   0=s 1=ms 2=us 3=ns  (timestamp, and array<timestamp>, only)
//   bits [8:16] precision  (int64-backed decimal only)
//   bits [16:24] scale      (int64-backed decimal only)
enum : int {
    LC_NONE = 0, LC_DECIMAL64 = 1, LC_DATE = 3, LC_TIMESTAMP = 4,
    // R6: ARRAY whose ELEMENT is a TIMESTAMP. The parquet list<timestamp> leaf
    // decodes as physical int64 and the IPC wire format carries no logical type,
    // so the child needs the same unit-carrying retag the scalar case gets —
    // mirroring the trampoline's `_sp_array_ts_unit_map` / op kind 4.
    LC_ARRAY_TIMESTAMP = 5,
    // IPV4 REFINES an already-complete physical type (UINT32) instead of
    // completing a parameterized one, so the column decodes through the ordinary
    // unsigned-int path and only the descriptor is attached — no width change, no
    // conversion. Without it a catalog-declared IPv4 column arrives as bare
    // UINT32: integer rendering instead of dotted-decimal, and a hard refusal from
    // CIDR_AGG, which requires the descriptor.
    LC_IPV4 = 6,
};
static inline int lc_kind(int packed)      { return packed & 0xF; }
static inline int lc_unit(int packed)      { return (packed >> 4) & 0xF; }
static inline int lc_precision(int packed) { return (packed >> 8) & 0xFF; }
static inline int lc_scale(int packed)     { return (packed >> 16) & 0xFF; }

// ---- NativeScanColumnBuilder ------------------------------------------------------
// The decode half of the native scan: rugo ColumnOut -> CxxColumn, for every shape a
// projected parquet column can arrive in. Split out of NativeParquetScanSource (and
// otherwise unchanged) so the R3 two-pass late-materialization Source
// (native_latmat_scan_source.hpp) can build ITS pass-1 and pass-2 columns through the
// same code rather than a second copy of it — one decoder, two callers. The plan-time
// per-column flag arrays below are exactly the ones threaded from open_native_scan_plan;
// the latmat Source holds one builder per pass, each pointing at that pass's arrays.
struct NativeScanColumnBuilder {
    // Decimal pool-path support (see native_decimal_pool_decode.hpp): `pool` is
    // the same MemoryPool wired into the pipeline via wire_pool_sink at planning
    // time; `decimal_columns[i]` (parallel to column_names) marks which
    // projected columns are known (plan-time, from the query's schema) to be
    // int64-backed DECIMAL, and therefore expected to land as DK_POOL. Both
    // may be null/empty when this scan has no decimal columns — every existing
    // caller keeps working with the fixed-width-only direct path unchanged.
    MemoryPool* pool = nullptr;
    const std::vector<uint8_t>* decimal_columns = nullptr;
    // Same convention as `decimal_columns`, for VARCHAR pool-path columns (see
    // native_varchar_pool_decode.hpp): TPC-H's l_returnflag/l_linestatus are
    // non-nullable dict-encoded byte_array columns that decode via rugo's "RLE
    // skip-dense" path and so classify DK_POOL rather than DK_VARCHAR_DICT —
    // verified directly against real files, this is not a hypothetical case.
    const std::vector<uint8_t>* varchar_columns = nullptr;
    // Per-projected-column declared string DrakenType (parallel to column_names):
    // DRAKEN_VARCHAR / NVARCHAR / VARBINARY for string columns, 0 for non-string.
    // Threaded from the plan-time schema so every string column — direct
    // (DK_VARCHAR / DK_VARCHAR_DICT) or pool (DK_POOL) — is tagged with its exact
    // declared type, byte-identically to column_deserializer.pyx's want_string_type.
    // Also the general-scan signal that a DK_POOL column is a VARCHAR (the agg/join
    // callers instead pass varchar_columns and leave this null → DRAKEN_VARCHAR).
    const std::vector<int>* string_types = nullptr;
    // WP-11: per-column packed logical-coercion plan (see the LC_* packing above),
    // parallel to column_names. null/empty → no temporal/decimal coercion (the
    // original numeric+string behaviour). Threaded from the plan-time schema so a
    // projected DATE/TIMESTAMP/TIME/int64-DECIMAL column is retagged natively,
    // byte-identically to the trampoline scan's `_coerce_vectors`.
    const std::vector<int>* logical_coerce = nullptr;
    // E37: per-projected-column flag (parallel to column_names) — 1 iff this column
    // is consumed as a GROUP BY / JOIN / DISTINCT key downstream, so the scan should
    // carry its hash SEED (VectorOwner::keyhash_buf) for hash-once reuse. Default
    // null/empty → no column keyed → NO sidecar built (the pay-for-use gate; plain
    // SELECT / LIKE / standalone rugo build nothing). See E37 §7.2.
    const std::vector<uint8_t>* hash_key_columns = nullptr;
    // R6: same convention as `decimal_columns` / `varchar_columns`, for ARRAY
    // (parquet LIST) columns — `array_columns[i]` = 1 marks a projected column the
    // plan knows (from the schema) is an ARRAY, and which therefore ALWAYS lands
    // DK_POOL: rugo's direct_kind_for classifies any column with repetition levels
    // as pool-routed, regardless of encoding, and serializes it as TAG_ARRAY. The
    // flag is what tells this Source which decoder owns that blob; without it the
    // three pool shapes (decimal / varchar / array) are indistinguishable from the
    // DirectKind alone. null/empty for every scan with no array columns.
    const std::vector<uint8_t>* array_columns = nullptr;

    // R6: is projected column i a plan-flagged ARRAY column?
    bool is_array_column(size_t i) const {
        return array_columns != nullptr && i < array_columns->size()
            && (*array_columns)[i] != 0;
    }

    // E37: does projected column i want its hash seed carried? Default false.
    bool wants_keyhash(size_t i) const {
        return hash_key_columns != nullptr && i < hash_key_columns->size()
            && (*hash_key_columns)[i] != 0;
    }

    // Packed coercion plan for projected column i (0 = none).
    int coerce_for(size_t i) const {
        if (logical_coerce != nullptr && i < logical_coerce->size())
            return (*logical_coerce)[i];
        return 0;
    }

    // Declared string DrakenType for projected column i, defaulting to DRAKEN_VARCHAR
    // when no per-column type was threaded (agg/join callers) — matches the historic
    // hardcoded tag those paths relied on.
    DrakenType string_type_for(size_t i) const {
        if (string_types != nullptr && i < string_types->size() && (*string_types)[i] != 0)
            return static_cast<DrakenType>((*string_types)[i]);
        return DRAKEN_VARCHAR;
    }

    static bool direct_kind_supported(int dk) {
        switch (dk) {
            case rugo::DK_INT64: case rugo::DK_FLOAT32: case rugo::DK_FLOAT64:
            case rugo::DK_INT64_DICT: case rugo::DK_FLOAT64_DICT: case rugo::DK_FLOAT32_DICT:
            case rugo::DK_DECIMAL128: case rugo::DK_BOOL:
            // A1 (E33): exact-width integer direct kinds — dense
            // (DK_UINT8/16/32/64, DK_INT8/16/32) and dict-shaped (DK_*_DICT).
            // Signed narrow ints no longer widen to DK_INT64, so the signed
            // family needs the same tags as the unsigned one.
            case rugo::DK_UINT8:  case rugo::DK_UINT16:
            case rugo::DK_UINT32: case rugo::DK_UINT64:
            case rugo::DK_UINT8_DICT:  case rugo::DK_UINT16_DICT:
            case rugo::DK_UINT32_DICT: case rugo::DK_UINT64_DICT:
            case rugo::DK_INT8:  case rugo::DK_INT16: case rugo::DK_INT32:
            case rugo::DK_INT8_DICT:  case rugo::DK_INT16_DICT:
            case rugo::DK_INT32_DICT:
                return true;
            default:
                return false;
        }
    }

    static DrakenType draken_type_for(int dk) {
        switch (dk) {
            case rugo::DK_INT64:      case rugo::DK_INT64_DICT:   return DRAKEN_INT64;
            case rugo::DK_FLOAT32:    case rugo::DK_FLOAT32_DICT: return DRAKEN_FLOAT32;
            case rugo::DK_DECIMAL128:                             return DRAKEN_DECIMAL128;
            case rugo::DK_BOOL:                                   return DRAKEN_BOOL;
            // A1 (E33): preserve the exact declared width and signedness (dense + dict
            // share the tag), byte-identical to the trampoline's _wrap_direct /
            // _wrap_num_dict_direct.
            case rugo::DK_UINT8:      case rugo::DK_UINT8_DICT:   return DRAKEN_UINT8;
            case rugo::DK_UINT16:     case rugo::DK_UINT16_DICT:  return DRAKEN_UINT16;
            case rugo::DK_UINT32:     case rugo::DK_UINT32_DICT:  return DRAKEN_UINT32;
            case rugo::DK_UINT64:     case rugo::DK_UINT64_DICT:  return DRAKEN_UINT64;
            case rugo::DK_INT8:       case rugo::DK_INT8_DICT:    return DRAKEN_INT8;
            case rugo::DK_INT16:      case rugo::DK_INT16_DICT:   return DRAKEN_INT16;
            case rugo::DK_INT32:      case rugo::DK_INT32_DICT:   return DRAKEN_INT32;
            default:                                              return DRAKEN_FLOAT64;
        }
    }

    // A1 (E33): a numeric dict-shaped direct kind (dictionary values + per-row
    // uint32 codes). Parallel to the DK_INT64_DICT / DK_FLOAT*_DICT set.
    static bool is_numeric_dict_kind(int dk) {
        switch (dk) {
            case rugo::DK_INT64_DICT: case rugo::DK_FLOAT64_DICT: case rugo::DK_FLOAT32_DICT:
            case rugo::DK_UINT8_DICT:  case rugo::DK_UINT16_DICT:
            case rugo::DK_UINT32_DICT: case rugo::DK_UINT64_DICT:
            case rugo::DK_INT8_DICT:   case rugo::DK_INT16_DICT:
            case rugo::DK_INT32_DICT:
                return true;
            default:
                return false;
        }
    }

    // WP-11: build a projected DATE / TIMESTAMP column from a direct/dict column.
    // Mirrors the trampoline scan's reinterpret_as_date32 /
    // retag_int64_as_timestamp64 exactly:
    //   - DATE32 emits an int32 payload (per data_length, so dict-shaped columns
    //     convert the dictionary values and keep their codes) and carries no
    //     logical type,
    //   - TIMESTAMP64 keeps the int64 payload, only changes the tag, and attaches
    //     an interned LogicalType carrying the unit (mandatory: a
    //     DRAKEN_TIMESTAMP64 with a nullptr descriptor is a hard error in draken).
    //
    // The DATE carrier width is read from `dk`, never assumed: a DATE column is
    // physical int32 and now decodes at that width (DK_INT32), though it can also
    // arrive already widened (DK_INT64). Treating a 4-byte payload as int64 would
    // read past the end of the buffer, so this must follow `dk`.
    bool build_temporal_column(rugo::MorselRef& result, size_t i, int dk, int packed,
                               CxxColumn& out) {
        const int kind = lc_kind(packed);
        const bool is_dict = (dk == rugo::DK_INT64_DICT || dk == rugo::DK_INT32_DICT);
        const bool src_is_32 = (dk == rugo::DK_INT32 || dk == rugo::DK_INT32_DICT);
        const uint32_t length = result.columns[i].length;
        const uint32_t data_length = is_dict ? result.columns[i].data_length : length;
        uint8_t* validity = nullptr;
        void* data = rugo::morsel_take_direct(result, i, &validity);
        void* codes = nullptr;
        if (is_dict) {
            void* arena = nullptr;
            rugo::morsel_take_string(result, i, &arena, &codes);  // codes only (numeric dict)
        }

        const bool is_date = (kind == LC_DATE);
        const DrakenType dtype = is_date ? DRAKEN_DATE32 : DRAKEN_TIMESTAMP64;

        void* payload = data;
        OwnedBuffer<void> data_buf;
        if (is_date && !src_is_32) {
            // int64 → int32 over the physical values (data_length), preserving any
            // dict codes: byte-identical to draken's vector_reinterpret_as_date32.
            int32_t* nd = static_cast<int32_t*>(
                draken_malloc((data_length > 0u ? data_length : 1u) * sizeof(int32_t)));
            const int64_t* sd = static_cast<const int64_t*>(data);
            for (uint32_t k = 0; k < data_length; ++k)
                nd[k] = static_cast<int32_t>(sd[k]);
            draken_free(data);
            payload = nd;
            data_buf = OwnedBuffer<void>(nd);
        } else {
            // Already int32 (DATE at its physical width): retag only, no conversion.
            data_buf = OwnedBuffer<void>(data);
        }
        OwnedBuffer<uint8_t> val_buf(validity);
        OwnedBuffer<void> codes_buf(codes);

        DrakenVector v = is_dict
            ? draken_vector_from_dict(payload, data_length,
                                      static_cast<const uint32_t*>(codes), length, dtype, validity)
            : draken_vector_from_dense(payload, length, dtype, validity);
        // Clustering hint (rugo sorting_columns, trust-gated in metadata.cpp) — a
        // DATE/TIMESTAMP column is a very plausible clustering key. See the plain
        // numeric branch in build_column() for the same treatment.
        if (result.columns[i].row_sorted)
            v.flags |= DRAKEN_ROW_SORTED |
                       (result.columns[i].row_sorted_descending ? DRAKEN_ROW_SORTED_DESC : 0);
        out.own = std::make_shared<VectorOwner>(v, std::move(data_buf), std::move(val_buf),
                                                 std::move(codes_buf));
        if (kind == LC_TIMESTAMP) {
            LogicalType lt;
            lt.kind = LogicalKind::TIMESTAMP;
            lt.unit = static_cast<TimestampUnit>(lc_unit(packed));
            lt.offset_minutes = 0;
            out.own->logical_type = logical_type_intern(lt);
        }
        out.view = out.own->vec;
        return true;
    }

    // Build a CxxColumn straight from ColumnOut's owned buffers — no Vector, no
    // PyObject. morsel_take_direct/morsel_take_string transfer ownership out of
    // the MorselRef (nulling its slots so ~MorselRef won't double-free); the
    // OwnedBuffers below take that same ownership over to the VectorOwner.
    //
    // NOTE for DK_DECIMAL128: `out.own->logical_type` is deliberately left
    // nullptr — this narrow pipeline's own DecimalExpr evaluator (native_decimal.hpp)
    // takes each column's scale from the plan-time-known expression tree, never
    // from `VectorOwner.logical_type`, so it doesn't need it. A decimal CxxColumn
    // built here must not be handed to any OTHER consumer that expects a valid
    // logical_type descriptor (draken's own `require_decimal_descriptor` contract).
    bool build_column(rugo::MorselRef& result, size_t i, CxxColumn& out, ErrCtx& err) {
        int dk = result.columns[i].direct_kind;
        // R6: an ARRAY column has repetition levels, so direct_kind_for routes it
        // to the pool unconditionally. Anything else means the plan's schema and
        // the decoded data disagree — a direct-kind buffer read as a list would be
        // silent garbage, so fail loud instead.
        if (is_array_column(i) && dk != rugo::DK_POOL) {
            err.code = 1;
            err.msg = "NativeParquetScanSource: column planned as ARRAY did not decode "
                      "to the pool path";
            return false;
        }
        if (dk == rugo::DK_POOL) {
            if (is_array_column(i)) {
                if (pool == nullptr) {
                    err.code = 1;
                    err.msg = "NativeParquetScanSource: ARRAY column with no wired MemoryPool";
                    return false;
                }
                const int packed = coerce_for(i);
                return build_pool_array_column(
                    pool, result.columns[i].ref_id,
                    lc_kind(packed) == LC_ARRAY_TIMESTAMP ? 1 : 0,
                    lc_unit(packed), out, err);
            }
            bool is_decimal = pool != nullptr && decimal_columns != nullptr &&
                               i < decimal_columns->size() && (*decimal_columns)[i] != 0;
            if (is_decimal) {
                if (!build_pool_decimal_column(pool, result.columns[i].ref_id, out, err))
                    return false;
                // WP-11: int64-backed DECIMAL carries no scale on the wire; attach the
                // plan-time-known precision/scale so a projected decimal reaches output
                // byte-identically to the trampoline's `_int64_to_decimal(v, p, s)`.
                const int packed = coerce_for(i);
                if (lc_kind(packed) == LC_DECIMAL64) {
                    LogicalType lt;
                    lt.kind = LogicalKind::DECIMAL;
                    lt.precision = static_cast<uint8_t>(lc_precision(packed));
                    lt.scale = static_cast<uint8_t>(lc_scale(packed));
                    out.own->logical_type = logical_type_intern(lt);
                }
                return true;
            }
            // A DK_POOL string column (RLE skip-dense, or the plain/non-positional
            // pool fallback — see io_pipeline.hpp direct_kind_for) is flagged either
            // by the general-scan per-column string type or the agg/join
            // varchar_columns array; build_pool_varchar_column handles BOTH pool
            // string tags (6 dict / 7 plain).
            bool is_varchar = pool != nullptr &&
                ((varchar_columns != nullptr && i < varchar_columns->size() &&
                  (*varchar_columns)[i] != 0) ||
                 (string_types != nullptr && i < string_types->size() &&
                  (*string_types)[i] != 0));
            if (is_varchar)
                return build_pool_varchar_column(pool, result.columns[i].ref_id,
                                                 string_type_for(i), out, err);
            return false;
        }
        if (dk == rugo::DK_VARCHAR_DICT) {
            // Dict-shaped byte_array (build_direct_string_dict in io_pipeline.hpp):
            // data_length unique slots + a per-row uint32 codes selection, long
            // values in `arena`. emit_dict_string_column copies slots+arena into the
            // canonical [DrakenStringArena|slots|arena] consolidated block — a raw
            // slot pointer is NOT a valid string vector `data` (str kernels read it
            // as DrakenStringArena*). Arena offsets are relative, so the verbatim
            // copy needs no rebasing.
            uint32_t length = result.columns[i].length;
            uint32_t data_length = result.columns[i].data_length;
            size_t arena_len = result.columns[i].arena_len;
            uint8_t* validity = nullptr;
            void* slots = rugo::morsel_take_direct(result, i, &validity);
            void* arena = nullptr;
            void* codes = nullptr;
            rugo::morsel_take_string(result, i, &arena, &codes);
            // E37: take the carried seed ONLY if this column is a downstream key
            // (else leave it for ~MorselRef to free — no sidecar on non-key columns).
            uint64_t* kh = nullptr;
            if (wants_keyhash(i)) {
                kh = static_cast<uint64_t*>(result.columns[i].keyhash);
                result.columns[i].keyhash = nullptr;
            }
            emit_dict_string_column(static_cast<DrakenStringSlot*>(slots), data_length,
                                    static_cast<uint8_t*>(arena), arena_len,
                                    static_cast<uint32_t*>(codes), length,
                                    validity, string_type_for(i), out,
                                    result.columns[i].dict_sorted, kh,
                                    result.columns[i].row_sorted,
                                    result.columns[i].row_sorted_descending);
            return true;
        }
        if (dk == rugo::DK_VARCHAR) {
            // Plain (non-dict) byte_array → dense positional DrakenStringSlot array
            // (build_direct_string_plain in io_pipeline.hpp), one slot per row; long
            // values live in `arena`, inline values in the slot. No per-row codes.
            uint32_t length = result.columns[i].length;
            size_t arena_len = result.columns[i].arena_len;
            uint8_t* validity = nullptr;
            void* slots = rugo::morsel_take_direct(result, i, &validity);
            void* arena = nullptr;
            void* codes = nullptr;
            rugo::morsel_take_string(result, i, &arena, &codes);  // codes null for plain
            // E37: take the carried seed ONLY if this column is a downstream key
            // (else leave it for ~MorselRef to free — no sidecar on non-key columns).
            uint64_t* kh = nullptr;
            if (wants_keyhash(i)) {
                kh = static_cast<uint64_t*>(result.columns[i].keyhash);
                result.columns[i].keyhash = nullptr;
            }
            emit_dense_string_column(static_cast<DrakenStringSlot*>(slots), length,
                                     static_cast<uint8_t*>(arena), arena_len,
                                     validity, string_type_for(i), out, kh,
                                     result.columns[i].payloads_elided,
                                     result.columns[i].row_sorted,
                                     result.columns[i].row_sorted_descending);
            return true;
        }
        // WP-11: a projected int64 (or widened-int32) column the plan flags as
        // DATE / TIMESTAMP / TIME is retagged natively (narrow + unit descriptor)
        // rather than emitted as plain INT64.
        const int packed = coerce_for(i);
        // DATE decodes at its physical int32 width (E33 exact-width integers) while
        // TIMESTAMP stays int64, so both carriers reach the temporal builder.
        if ((dk == rugo::DK_INT64 || dk == rugo::DK_INT64_DICT ||
             dk == rugo::DK_INT32 || dk == rugo::DK_INT32_DICT) &&
            lc_kind(packed) != LC_NONE && lc_kind(packed) != LC_IPV4)
            return build_temporal_column(result, i, dk, packed, out);
        if (!direct_kind_supported(dk)) return false;
        DrakenType dtype = draken_type_for(dk);
        uint32_t length = result.columns[i].length;
        uint8_t* validity = nullptr;
        void* data = rugo::morsel_take_direct(result, i, &validity);
        DrakenVector v;
        OwnedBuffer<void> data_buf(data);
        OwnedBuffer<uint8_t> val_buf(validity);
        OwnedBuffer<void> codes_buf;
        if (is_numeric_dict_kind(dk)) {
            uint32_t data_length = result.columns[i].data_length;
            void* arena = nullptr;
            void* codes = nullptr;
            rugo::morsel_take_string(result, i, &arena, &codes);  // codes only; arena unused (numeric dict)
            v = draken_vector_from_dict(data, data_length, static_cast<const uint32_t*>(codes),
                                        length, dtype, validity);
            if (result.columns[i].dict_sorted && draken_is_dict(&v))
                v.flags |= DRAKEN_DICT_KEYS_SORTED;
            codes_buf = OwnedBuffer<void>(codes);
        } else {
            v = draken_vector_from_dense(data, length, dtype, validity);
        }
        // Clustering hint (rugo sorting_columns, trust-gated in metadata.cpp).
        // Applies to any shape, unlike DRAKEN_DICT_KEYS_SORTED above, so it is
        // unconditional here (not gated on is_numeric_dict_kind).
        if (result.columns[i].row_sorted)
            v.flags |= DRAKEN_ROW_SORTED |
                       (result.columns[i].row_sorted_descending ? DRAKEN_ROW_SORTED_DESC : 0);
        out.own = std::make_shared<VectorOwner>(v, std::move(data_buf), std::move(val_buf),
                                                 std::move(codes_buf));
        if (dk == rugo::DK_DECIMAL128) {
            // WP-11: DECIMAL128 carries its precision/scale on the footer (rugo's
            // parse_decimal_ps fills ColumnOut.dec_*); attach it so a projected
            // decimal128 reaches output byte-identically to the trampoline's
            // `_wrap_direct` → set_decimal_descriptor(dec_precision, dec_scale).
            LogicalType lt;
            lt.kind = LogicalKind::DECIMAL;
            lt.precision = result.columns[i].dec_precision;
            lt.scale = result.columns[i].dec_scale;
            out.own->logical_type = logical_type_intern(lt);
        }
        if (lc_kind(packed) == LC_IPV4) {
            LogicalType lt;
            lt.kind = LogicalKind::IPV4;
            out.own->logical_type = logical_type_intern(lt);
        }
        out.view = out.own->vec;
        return true;
    }
};

struct NativeParquetScanGlobal : GlobalSourceState {
    std::mutex mtx;
    int next_to_submit = 0;
    int results_received = 0;
    // R2 (scan-pushed LIMIT): rows already handed downstream, across ALL workers.
    // Guarded by `mtx` together with the submit/receive counters — the emit
    // decision and the submit decision must see one consistent view.
    int64_t rows_emitted = 0;
    // R2: work-item frontier beyond which no row group can contribute to the
    // LIMIT (computed once from the footer row counts; == work_items->size()
    // when unlimited). Read-only after make_global.
    int submit_cap = 0;
};

struct NativeParquetScanSource : Source, NativeScanColumnBuilder {
    // All borrowed from the caller's NativeScanPlan, which outlives this Source for
    // the whole pipeline run (the Python planning frame holds it alive). The per-column
    // decode flags (pool / decimal_columns / varchar_columns / string_types /
    // logical_coerce / hash_key_columns / array_columns) live on the
    // NativeScanColumnBuilder base — same arrays, same meaning, shared with the R3
    // latmat Source.
    rugo::ParquetIOPipeline* pipeline;
    const std::unordered_map<std::string, FileStats>* footer_map;
    const std::vector<std::pair<std::string, int>>* work_items;
    const std::vector<std::string>* column_names;
    int in_flight_limit;
    // R2: scan-pushed LIMIT — the maximum number of rows this scan may emit in
    // total, across every worker. -1 == unlimited (every pre-R2 caller).
    //
    // This is a CORRECTNESS obligation, not just an I/O optimization: when
    // LimitPushdownStrategy pushes a LIMIT into a scan it REMOVES the Limit node
    // from the plan (limit_pushdown.py::_apply_to_scan → remove_node(heal=True)),
    // so there is no downstream LimitOperator left to truncate. The scan is the
    // only thing enforcing it — exactly as the trampoline's `_records_to_read`
    // slice in `_commit_morsel_cxx` already does.
    //
    // Pushdown only happens for a scan with NO pushed predicate (limit_pushdown
    // refuses on `scan_node.predicates`) and no OFFSET, so "the first N rows in
    // whatever order the row groups complete" is a valid answer — the trampoline
    // is equally order-nondeterministic at dop>1 (concurrent `_single_pass_next`
    // pulls commit under `_scan_mtx` in completion order, not file order).
    int64_t row_limit;

    NativeParquetScanSource(rugo::ParquetIOPipeline* pipeline_,
                            const std::unordered_map<std::string, FileStats>* footer_map_,
                            const std::vector<std::pair<std::string, int>>* work_items_,
                            const std::vector<std::string>* column_names_,
                            int in_flight_limit_,
                            MemoryPool* pool_ = nullptr,
                            const std::vector<uint8_t>* decimal_columns_ = nullptr,
                            const std::vector<uint8_t>* varchar_columns_ = nullptr,
                            const std::vector<int>* string_types_ = nullptr,
                            const std::vector<int>* logical_coerce_ = nullptr,
                            const std::vector<uint8_t>* hash_key_columns_ = nullptr,
                            const std::vector<uint8_t>* array_columns_ = nullptr,
                            int64_t row_limit_ = -1)
        : pipeline(pipeline_), footer_map(footer_map_), work_items(work_items_),
          column_names(column_names_), in_flight_limit(in_flight_limit_),
          row_limit(row_limit_) {
        pool = pool_;
        decimal_columns = decimal_columns_;
        varchar_columns = varchar_columns_;
        string_types = string_types_;
        logical_coerce = logical_coerce_;
        hash_key_columns = hash_key_columns_;
        array_columns = array_columns_;
    }

    // R2: how many row groups, taken in work-item order, are enough to satisfy
    // `row_limit`? The footer already carries every row group's exact row count
    // (RowGroupStats::num_rows), so this is an EXACT plan-time bound, not a
    // guess — no need to decode a row group to discover it was unnecessary.
    // Returns work_items->size() when unlimited or when the whole scan is needed.
    //
    // Without this, `LIMIT 5` still submits the full prefetch window
    // (`in_flight_limit`, == workers+2) plus one row group per worker that races
    // in before the first morsel is emitted — measured 31 row groups for a
    // LIMIT 5 over tpch_1.lineitem. Capping the frontier here reads exactly the
    // one row group that can actually contribute.
    int limit_submit_cap() const {
        const int n_items = static_cast<int>(work_items->size());
        if (row_limit < 0 || footer_map == nullptr) return n_items;
        int64_t cumulative = 0;
        for (int i = 0; i < n_items; ++i) {
            auto fit = footer_map->find((*work_items)[i].first);
            // A path we cannot resolve here is not a place to guess — fall back to
            // the unbounded frontier and let submit_one fail loud on it.
            if (fit == footer_map->end()) return n_items;
            const size_t rg_idx = static_cast<size_t>((*work_items)[i].second);
            if (rg_idx >= fit->second.row_groups.size()) return n_items;
            cumulative += fit->second.row_groups[rg_idx].num_rows;
            if (cumulative >= row_limit) return i + 1;
        }
        return n_items;
    }

    std::unique_ptr<GlobalSourceState> make_global() override {
        auto g = std::make_unique<NativeParquetScanGlobal>();
        // Computed once, before any worker runs (make_global is called on the
        // driver thread ahead of the fan-out), so get_morsel never re-walks the
        // work list under its lock.
        g->submit_cap = limit_submit_cap();
        return g;
    }
    std::unique_ptr<LocalSourceState> make_local(GlobalSourceState&) override {
        return std::make_unique<LocalSourceState>();
    }

    // Mirrors CppIOPipeline.submit_work_native (pool_reader.pyx) exactly, over
    // plain C++ containers instead of Python list/dict — same parallel-arrays
    // contract (col_names_vec/col_stats_vec built strictly in lockstep so a
    // column absent from this row group's stats is simply skipped, not padded).
    void submit_one(size_t idx, ErrCtx& err) {
        const std::string& path = (*work_items)[idx].first;
        int rg_idx = (*work_items)[idx].second;
        auto fit = footer_map->find(path);
        if (fit == footer_map->end()) {
            err.code = 1;
            err.msg = "NativeParquetScanSource: work item path missing from footer_map";
            return;
        }
        const RowGroupStats& rg = fit->second.row_groups[static_cast<size_t>(rg_idx)];
        std::vector<std::string> col_names_vec;
        std::vector<ColumnStats> col_stats_vec;
        col_names_vec.reserve(column_names->size());
        col_stats_vec.reserve(column_names->size());
        for (const std::string& want : *column_names) {
            for (const ColumnStats& cs : rg.columns) {
                if (cs.name == want) {
                    col_names_vec.push_back(want);
                    col_stats_vec.push_back(cs);
                    break;
                }
            }
        }
        if (col_names_vec.size() != column_names->size()) {
            // Schema evolution (a projected column absent from this row group) is
            // out of scope for this first landing — fail loud, no NULL-fill guess.
            err.code = 1;
            err.msg = "NativeParquetScanSource: row group is missing a projected "
                      "column (schema evolution is not supported on this path)";
            return;
        }
        pipeline->submit_row_group(path, rg_idx, col_names_vec, col_stats_vec);
    }

    SourceResult get_morsel(GlobalSourceState& gs, LocalSourceState&, MorselPtr& out,
                            ErrCtx& err) override {
        auto& g = static_cast<NativeParquetScanGlobal&>(gs);
        while (true) {
            int submit_start, submit_end;
            {
                std::lock_guard<std::mutex> lock(g.mtx);
                submit_start = g.next_to_submit;
                submit_end = submit_start;
                // R2: the frontier is the footer-derived cap (== work_items->size()
                // with no LIMIT), so row groups that provably cannot contribute to
                // the LIMIT are never submitted at all.
                int n_items = g.submit_cap;
                // R2: once a scan-pushed LIMIT is satisfied, stop submitting NEW
                // row-group work. Without this the prefetch window keeps running
                // ahead (`in_flight_limit` == workers+2 row groups) and decodes
                // data no one will ever read — on a large file that is the whole
                // scan, which is precisely what pushing the LIMIT down is for.
                // Already-submitted work is still drained below: we must not
                // abandon the pipeline with results outstanding, so the bounded
                // in-flight window is the (intended, small) overshoot.
                const bool limit_met = (row_limit >= 0 && g.rows_emitted >= row_limit);
                while (!limit_met && submit_end < n_items &&
                       (submit_end - g.results_received) < in_flight_limit) {
                    submit_end += 1;
                }
                g.next_to_submit = submit_end;
                // Done when every row group we actually submitted has been
                // accounted for. With no limit `next_to_submit` reaches `n_items`,
                // so this is identical to the pre-R2 `results_received >= n_items`;
                // with a limit it also terminates on the frozen submit frontier.
                if (g.results_received >= g.next_to_submit) {
                    return SourceResult::FINISHED;
                }
                g.results_received += 1;
            }

            for (int idx = submit_start; idx < submit_end; ++idx) {
                submit_one(static_cast<size_t>(idx), err);
                if (err.code != 0) return SourceResult::FINISHED;
            }

            rugo::MorselRef result;
            // Splits TC_SOURCE_PULL (the whole get_morsel() call, timed by
            // executor.hpp around this Source) into "waiting on the pipeline"
            // vs. everything after — output-column materialization below.
            // Without this, a stall showing up as TC_SOURCE_PULL taking far
            // longer than every row group's own download+decode spans is
            // ambiguous: this pins it to one side or the other.
            const auto _tr_idx = BS::this_thread::get_index();
            const uint16_t _tr_worker =
                _tr_idx.has_value() ? static_cast<uint16_t>(*_tr_idx) : 0xFFFFu;
            TraceHandle _tr_wait = trace_begin(TC_IO_WAIT, pipeline->trace_node_id(), 0,
                                                0xFFFFFFFFu, _tr_worker);
            bool got = pipeline->wait_and_get_result(result);
            trace_end(_tr_wait, 0, 0);
            if (!got) {
                err.code = 1;
                err.msg = "NativeParquetScanSource: pipeline drained with result(s) missing";
                return SourceResult::FINISHED;
            }
            if (!result.success) {
                err.code = 1;
                if (result.error.empty()) {
                    err.msg = "NativeParquetScanSource: parquet pipeline decode error";
                } else {
                    // Surface the pipeline's specific reason (e.g. a decompression
                    // error) verbatim. ErrCtx::msg is a bare const char* that must
                    // outlive this call and `result` is local, so stash it in a
                    // thread_local (not a member: pull() runs on every dop worker
                    // concurrently — a member would race).
                    static thread_local std::string decode_err_buf;
                    decode_err_buf = "NativeParquetScanSource: " + result.error;
                    err.msg = decode_err_buf.c_str();
                }
                return SourceResult::FINISHED;
            }
            if (result.empty_filtered) continue;  // Phase 2 dict-skip; no rows — pull again

            size_t ncols = result.columns.size();
            if (ncols != column_names->size()) {
                err.code = 1;
                err.msg = "NativeParquetScanSource: decoded column count does not "
                          "match the projection (schema evolution is not supported)";
                return SourceResult::FINISHED;
            }

            auto m = std::make_shared<CxxMorsel>();
            m->names = *column_names;
            m->columns.reserve(ncols);
            for (size_t i = 0; i < ncols; ++i) {
                CxxColumn col;
                if (!build_column(result, i, col, err)) {
                    if (err.code == 0) {
                        err.code = 1;
                        err.msg = "NativeParquetScanSource: unsupported column encoding "
                                  "(not a fixed-width numeric direct/dict column, and not "
                                  "a decimal column recognized via decimal_columns)";
                    }
                    return SourceResult::FINISHED;
                }
                m->columns.push_back(std::move(col));
            }
            // R2: claim this morsel's share of the scan-pushed LIMIT. The claim and
            // the truncation must be one atomic decision across workers, otherwise
            // two workers each see "room for 5" and 10 rows escape.
            if (row_limit >= 0) {
                const int64_t nrows = static_cast<int64_t>(m->num_rows());
                int64_t take;
                {
                    std::lock_guard<std::mutex> lock(g.mtx);
                    const int64_t remaining = row_limit - g.rows_emitted;
                    if (remaining <= 0) continue;  // another worker filled the quota
                    take = (nrows < remaining) ? nrows : remaining;
                    g.rows_emitted += take;
                }
                if (take < nrows) {
                    // Keep the first `take` rows. cxx_slice_c resolves from
                    // draken_native at load time, the same dynamic-lookup path
                    // native_unnest.hpp's cxx_unnest_c uses.
                    CxxMorsel* sliced = cxx_slice_c(m.get(), 0u, static_cast<uint32_t>(take));
                    if (sliced == nullptr) {
                        err.code = 1;
                        err.msg = "NativeParquetScanSource: LIMIT slice failed";
                        return SourceResult::FINISHED;
                    }
                    auto sm = std::make_shared<CxxMorsel>(std::move(*sliced));
                    cxx_morsel_delete(sliced);
                    m = std::move(sm);
                }
            }
            out = std::move(m);
            return SourceResult::HAVE_MORE;
        }
    }
};

}  // namespace opteryx::engine
