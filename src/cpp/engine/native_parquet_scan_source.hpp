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
//   - Fixed-width direct columns only: DK_INT64/FLOAT32/FLOAT64 (dense or
//     dict-shaped) plus DK_DECIMAL128 (dense only — rugo's decode layer has no
//     "dict-encoded DECIMAL128" direct kind; a dictionary-encoded DECIMAL128
//     column classifies as DK_POOL and is NOT handled here). DK_BOOL, DK_VARCHAR*,
//     and the DK_POOL (string/list) path are NOT handled here; hitting one sets
//     ErrCtx and stops the scan rather than guessing.
//   - Exception: DK_POOL columns explicitly flagged via `decimal_columns` (plan-
//     time known to be int64-backed DECIMAL — see native_decimal_pool_decode.hpp
//     for why these are ALWAYS DK_POOL regardless of parquet encoding) are read
//     directly from the wired MemoryPool and built as DRAKEN_DECIMAL.
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
#include "io_pipeline.hpp"        // rugo::ParquetIOPipeline, MorselRef, ColumnOut, DK_*
#include "metadata.hpp"           // FileStats, RowGroupStats, ColumnStats
#include "core/vector_alloc.h"    // draken_vector_from_dense / draken_vector_from_dict
#include "core/vector_owner.h"    // VectorOwner, OwnedBuffer
#include "memory_pool.hpp"        // opteryx::MemoryPool
#include "native_decimal_pool_decode.hpp"  // build_pool_decimal_column
#include "native_varchar_pool_decode.hpp"  // build_pool_varchar_dict_column

namespace opteryx::engine {

struct NativeParquetScanGlobal : GlobalSourceState {
    std::mutex mtx;
    int next_to_submit = 0;
    int results_received = 0;
};

struct NativeParquetScanSource : Source {
    // All borrowed from the caller's NativeScanPlan, which outlives this Source for
    // the whole pipeline run (the Python planning frame holds it alive).
    rugo::ParquetIOPipeline* pipeline;
    const std::unordered_map<std::string, FileStats>* footer_map;
    const std::vector<std::pair<std::string, int>>* work_items;
    const std::vector<std::string>* column_names;
    int in_flight_limit;
    // Decimal pool-path support (see native_decimal_pool_decode.hpp): `pool` is
    // the same MemoryPool wired into `pipeline` via wire_pool_sink at planning
    // time; `decimal_columns[i]` (parallel to column_names) marks which
    // projected columns are known (plan-time, from the query's schema) to be
    // int64-backed DECIMAL, and therefore expected to land as DK_POOL. Both
    // may be null/empty when this scan has no decimal columns — every existing
    // caller keeps working with the fixed-width-only direct path unchanged.
    MemoryPool* pool;
    const std::vector<uint8_t>* decimal_columns;
    // Same convention as `decimal_columns`, for VARCHAR pool-path columns (see
    // native_varchar_pool_decode.hpp): TPC-H's l_returnflag/l_linestatus are
    // non-nullable dict-encoded byte_array columns that decode via rugo's "RLE
    // skip-dense" path and so classify DK_POOL rather than DK_VARCHAR_DICT —
    // verified directly against real files, this is not a hypothetical case.
    const std::vector<uint8_t>* varchar_columns;

    NativeParquetScanSource(rugo::ParquetIOPipeline* pipeline_,
                            const std::unordered_map<std::string, FileStats>* footer_map_,
                            const std::vector<std::pair<std::string, int>>* work_items_,
                            const std::vector<std::string>* column_names_,
                            int in_flight_limit_,
                            MemoryPool* pool_ = nullptr,
                            const std::vector<uint8_t>* decimal_columns_ = nullptr,
                            const std::vector<uint8_t>* varchar_columns_ = nullptr)
        : pipeline(pipeline_), footer_map(footer_map_), work_items(work_items_),
          column_names(column_names_), in_flight_limit(in_flight_limit_),
          pool(pool_), decimal_columns(decimal_columns_), varchar_columns(varchar_columns_) {}

    std::unique_ptr<GlobalSourceState> make_global() override {
        return std::make_unique<NativeParquetScanGlobal>();
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

    static bool direct_kind_supported(int dk) {
        switch (dk) {
            case rugo::DK_INT64: case rugo::DK_FLOAT32: case rugo::DK_FLOAT64:
            case rugo::DK_INT64_DICT: case rugo::DK_FLOAT64_DICT: case rugo::DK_FLOAT32_DICT:
            case rugo::DK_DECIMAL128:
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
            default:                                              return DRAKEN_FLOAT64;
        }
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
        if (dk == rugo::DK_POOL) {
            bool is_decimal = pool != nullptr && decimal_columns != nullptr &&
                               i < decimal_columns->size() && (*decimal_columns)[i] != 0;
            if (is_decimal) return build_pool_decimal_column(pool, result.columns[i].ref_id, out, err);
            bool is_varchar = pool != nullptr && varchar_columns != nullptr &&
                              i < varchar_columns->size() && (*varchar_columns)[i] != 0;
            if (is_varchar) return build_pool_varchar_dict_column(pool, result.columns[i].ref_id, out, err);
            return false;
        }
        if (dk == rugo::DK_VARCHAR_DICT) {
            uint32_t length = result.columns[i].length;
            uint32_t data_length = result.columns[i].data_length;
            uint8_t* validity = nullptr;
            void* slots = rugo::morsel_take_direct(result, i, &validity);
            void* arena = nullptr;
            void* codes = nullptr;
            rugo::morsel_take_string(result, i, &arena, &codes);
            // Arena (may be empty — every value inline) transfers into
            // VectorOwner.arena_buf; slots reference it via a byte OFFSET
            // (str_data(slot, arena_base)), never an absolute pointer, so this
            // transfer needs no offset rebasing at all.
            DrakenVector v = draken_vector_from_dict(slots, data_length,
                                                     static_cast<const uint32_t*>(codes),
                                                     length, DRAKEN_VARCHAR, validity);
            out.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(slots),
                                                     OwnedBuffer<uint8_t>(validity),
                                                     OwnedBuffer<void>(codes),
                                                     OwnedBuffer<uint8_t>(static_cast<uint8_t*>(arena)));
            out.view = out.own->vec;
            return true;
        }
        if (!direct_kind_supported(dk)) return false;
        DrakenType dtype = draken_type_for(dk);
        uint32_t length = result.columns[i].length;
        uint8_t* validity = nullptr;
        void* data = rugo::morsel_take_direct(result, i, &validity);
        DrakenVector v;
        OwnedBuffer<void> data_buf(data);
        OwnedBuffer<uint8_t> val_buf(validity);
        OwnedBuffer<void> codes_buf;
        if (dk == rugo::DK_INT64_DICT || dk == rugo::DK_FLOAT64_DICT || dk == rugo::DK_FLOAT32_DICT) {
            uint32_t data_length = result.columns[i].data_length;
            void* arena = nullptr;
            void* codes = nullptr;
            rugo::morsel_take_string(result, i, &arena, &codes);  // codes only; arena unused (numeric dict)
            v = draken_vector_from_dict(data, data_length, static_cast<const uint32_t*>(codes),
                                        length, dtype, validity);
            codes_buf = OwnedBuffer<void>(codes);
        } else {
            v = draken_vector_from_dense(data, length, dtype, validity);
        }
        out.own = std::make_shared<VectorOwner>(v, std::move(data_buf), std::move(val_buf),
                                                 std::move(codes_buf));
        out.view = out.own->vec;
        return true;
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
                int n_items = static_cast<int>(work_items->size());
                while (submit_end < n_items &&
                       (submit_end - g.results_received) < in_flight_limit) {
                    submit_end += 1;
                }
                g.next_to_submit = submit_end;
                if (g.results_received >= n_items) {
                    return SourceResult::FINISHED;
                }
                g.results_received += 1;
            }

            for (int idx = submit_start; idx < submit_end; ++idx) {
                submit_one(static_cast<size_t>(idx), err);
                if (err.code != 0) return SourceResult::FINISHED;
            }

            rugo::MorselRef result;
            bool got = pipeline->wait_and_get_result(result);
            if (!got) {
                err.code = 1;
                err.msg = "NativeParquetScanSource: pipeline drained with result(s) missing";
                return SourceResult::FINISHED;
            }
            if (!result.success) {
                err.code = 1;
                err.msg = "NativeParquetScanSource: parquet pipeline decode error";
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
            out = std::move(m);
            return SourceResult::HAVE_MORE;
        }
    }
};

}  // namespace opteryx::engine
