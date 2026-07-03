#pragma once
// src/cpp/engine/native_grouped_aggregate.hpp — genuinely native (zero-Python)
// GROUP BY aggregate Sink: SUM/COUNT/AVG per group, over the SAME expression
// shapes native_aggregate.hpp already supports (fixed-width numeric, plus exact
// decimal SUM), keyed by one or more VARCHAR group-by columns.
//
// Scope (first landing, stated honestly, not silently extended):
//   - Group-by key columns: VARCHAR only, and only the INLINE (<=12 byte) shape
//     native_parquet_scan_source.hpp's DK_VARCHAR_DICT support produces — this
//     exists specifically for low-cardinality codes like TPC-H's
//     l_returnflag/l_linestatus, not general-purpose string grouping.
//   - A NULL group-by key value is NOT supported — fails loud (ErrCtx), not
//     silently coalesced into some sentinel group.
//   - Aggregates: same closed set as native_aggregate.hpp (SUM/COUNT/AVG over
//     fixed-width numeric expressions, exact decimal SUM), PLUS decimal AVG
//     (ungrouped's scope note explicitly excludes this — added here because
//     Q01 needs it and the semantics are simple: AVG(decimal) is NOT itself a
//     decimal value anywhere in this codebase — see the reference engine's own
//     output type — it's `double(unscaled_sum) / 10^scale / count`, a FLOAT64,
//     exactly like every other AVG). Decimal DIVISION was never implemented and
//     still isn't; this is why decimal AVG is a plain double, not a decimal
//     divide.
//   - No MIN/MAX/MEDIAN/COUNT(DISTINCT)/STDDEV, no HAVING, no rollup.
//
// Output shape: one row per distinct group-key tuple encountered, in
// unspecified order (grouped queries needing a specific order carry their own
// downstream SortNode, reused unchanged by the caller — this Sink never sorts).

#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "executor.hpp"
#include "native_aggregate.hpp"   // AggFunc, AggregateSpec, NativeAggAccum, eval_expr_checked
#include "native_decimal.hpp"     // DecimalValue, eval_decimal_expr_checked, decimal_pow10
#include "core/string_slot.h"     // DrakenStringSlot, str_is_inline, str_length

namespace opteryx::engine {

struct GroupAccumEntry {
    std::vector<std::string> key_values;  // one per group-by column, human-readable bytes
    std::vector<NativeAggAccum> accums;   // one per spec
};

using GroupTable = std::unordered_map<std::string, GroupAccumEntry>;

struct NativeGroupedAggregateLocal : LocalSinkState {
    GroupTable groups;
};
struct NativeGroupedAggregateGlobal : GlobalSinkState {
    std::mutex mtx;
    GroupTable groups;
};

// Flattened (Cython-friendly) finalize output. `num_groups` rows, each with
// `num_key_cols` key strings and `num_specs` result slots. Row-major: index
// [g * num_key_cols + k] / [g * num_specs + s].
struct NativeGroupedAggregateStats {
    uint32_t num_groups = 0;
    std::vector<std::string> group_key_values;  // size num_groups * num_key_cols
    std::vector<double> result;                 // size num_groups * num_specs
    std::vector<int64_t> decimal_hi;            // size num_groups * num_specs
    std::vector<uint64_t> decimal_lo;
    std::vector<uint8_t> decimal_scale;
};

struct NativeGroupedAggregateSink : Sink {
    std::vector<size_t> group_col_idx;   // morsel column indices forming the key, in order
    std::vector<AggregateSpec> specs;

    NativeGroupedAggregateSink(std::vector<size_t> gcols, std::vector<AggregateSpec> s)
        : group_col_idx(std::move(gcols)), specs(std::move(s)) {}

    std::unique_ptr<GlobalSinkState> make_global() override {
        return std::make_unique<NativeGroupedAggregateGlobal>();
    }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override {
        return std::make_unique<NativeGroupedAggregateLocal>();
    }

    static bool accumulate_row(const AggregateSpec& spec, NativeAggAccum& acc,
                               const CxxMorsel& m, uint32_t row, ErrCtx& err) {
        if (spec.is_decimal) {
            DecimalValue v;
            bool overflow = false;
            if (!eval_decimal_expr_checked(*spec.decimal_expr, m, row, v, overflow)) {
                if (overflow) {
                    err.code = 1;
                    err.msg = "NativeGroupedAggregateSink: decimal expression overflowed __int128";
                    return false;
                }
                return true;  // NULL operand — excluded, matching SQL semantics
            }
            acc.decimal_scale = v.scale;
            __int128 next = acc.decimal_sum + v.unscaled;
            if (((acc.decimal_sum ^ next) & (v.unscaled ^ next)) < 0) {
                err.code = 1;
                err.msg = "NativeGroupedAggregateSink: decimal SUM accumulator overflowed __int128";
                return false;
            }
            acc.decimal_sum = next;
            acc.count += 1;  // tracked even for SUM(decimal) — needed if this spec is AVG(decimal)
            return true;
        }
        if (spec.func == AggFunc::Count && spec.expr == nullptr) {
            acc.count += 1;
            return true;
        }
        double v;
        if (eval_expr_checked(*spec.expr, m, row, v)) {
            acc.sum += v;
            acc.count += 1;
        }
        return true;
    }

    SinkResult sink(const MorselPtr& in, GlobalSinkState&, LocalSinkState& ls,
                    ErrCtx& err) override {
        auto& l = static_cast<NativeGroupedAggregateLocal&>(ls);
        const uint32_t n = in->num_rows();
        std::string raw_key;
        raw_key.reserve(16 * group_col_idx.size());
        std::vector<std::string> key_values;
        key_values.reserve(group_col_idx.size());

        for (uint32_t i = 0; i < n; ++i) {
            raw_key.clear();
            key_values.clear();
            bool row_ok = true;
            for (size_t gk : group_col_idx) {
                const DrakenVector& v = in->columns[gk].view;
                if (v.validity != nullptr && !((v.validity[i >> 3] >> (i & 7)) & 1u)) {
                    row_ok = false;
                    break;
                }
                uint32_t phys = v.selection[i];
                const auto* slot = static_cast<const DrakenStringSlot*>(v.data) + phys;
                if (!str_is_inline(slot)) {
                    // native_parquet_scan_source.hpp only ever builds inline-only
                    // DK_VARCHAR_DICT columns — a non-inline slot here would mean
                    // that invariant broke elsewhere. Fail loud, never guess.
                    err.code = 1;
                    err.msg = "NativeGroupedAggregateSink: non-inline group-by key "
                              "value (invariant violated upstream)";
                    return SinkResult::CONTINUE;
                }
                raw_key.append(reinterpret_cast<const char*>(&slot->raw), sizeof(slot->raw));
                key_values.emplace_back(reinterpret_cast<const char*>(slot->inl.data),
                                         str_length(slot));
            }
            if (!row_ok) {
                err.code = 1;
                err.msg = "NativeGroupedAggregateSink: NULL group-by key is not supported";
                return SinkResult::CONTINUE;
            }

            auto& entry = l.groups[raw_key];
            if (entry.accums.empty()) {
                entry.key_values = key_values;
                entry.accums.resize(specs.size());
            }
            for (size_t s = 0; s < specs.size(); ++s) {
                if (!accumulate_row(specs[s], entry.accums[s], *in, i, err)) {
                    return SinkResult::CONTINUE;
                }
            }
        }
        return SinkResult::CONTINUE;
    }

    static void combine_accum(const AggregateSpec& spec, NativeAggAccum& dst,
                              const NativeAggAccum& src, ErrCtx& err) {
        dst.sum += src.sum;
        dst.count += src.count;
        if (spec.is_decimal) {
            __int128 next = dst.decimal_sum + src.decimal_sum;
            if (((dst.decimal_sum ^ next) & (src.decimal_sum ^ next)) < 0) {
                err.code = 1;
                err.msg = "NativeGroupedAggregateSink: decimal SUM combine overflowed __int128";
                return;
            }
            dst.decimal_sum = next;
            dst.decimal_scale = src.decimal_scale;
        }
    }

    void combine(GlobalSinkState& gs, LocalSinkState& ls, ErrCtx& err) override {
        auto& g = static_cast<NativeGroupedAggregateGlobal&>(gs);
        auto& l = static_cast<NativeGroupedAggregateLocal&>(ls);
        std::lock_guard<std::mutex> lk(g.mtx);
        for (auto& [key, local_entry] : l.groups) {
            auto it = g.groups.find(key);
            if (it == g.groups.end()) {
                g.groups.emplace(key, std::move(local_entry));
                continue;
            }
            GroupAccumEntry& dst = it->second;
            for (size_t s = 0; s < specs.size(); ++s) {
                combine_accum(specs[s], dst.accums[s], local_entry.accums[s], err);
                if (err.code != 0) return;
            }
        }
    }

    void finalize(GlobalSinkState& gs, ErrCtx&) override {
        auto& g = static_cast<NativeGroupedAggregateGlobal&>(gs);
        stats_.num_groups = static_cast<uint32_t>(g.groups.size());
        const size_t nspecs = specs.size();
        stats_.group_key_values.reserve(stats_.num_groups * group_col_idx.size());
        stats_.result.assign(stats_.num_groups * nspecs, 0.0);
        stats_.decimal_hi.assign(stats_.num_groups * nspecs, 0);
        stats_.decimal_lo.assign(stats_.num_groups * nspecs, 0);
        stats_.decimal_scale.assign(stats_.num_groups * nspecs, 0);

        uint32_t g_idx = 0;
        for (auto& [key, entry] : g.groups) {
            for (const std::string& kv : entry.key_values) {
                stats_.group_key_values.push_back(kv);
            }
            for (size_t s = 0; s < nspecs; ++s) {
                const NativeAggAccum& acc = entry.accums[s];
                const size_t out_idx = static_cast<size_t>(g_idx) * nspecs + s;
                if (specs[s].is_decimal) {
                    if (specs[s].func == AggFunc::Avg) {
                        double unscaled_d = static_cast<double>(acc.decimal_sum);
                        double scaled = unscaled_d / static_cast<double>(decimal_pow10(acc.decimal_scale));
                        stats_.result[out_idx] = acc.count > 0 ? scaled / static_cast<double>(acc.count) : 0.0;
                    } else {
                        stats_.decimal_hi[out_idx] = static_cast<int64_t>(acc.decimal_sum >> 64);
                        stats_.decimal_lo[out_idx] = static_cast<uint64_t>(
                            static_cast<unsigned __int128>(acc.decimal_sum) & 0xFFFFFFFFFFFFFFFFULL);
                        stats_.decimal_scale[out_idx] = acc.decimal_scale;
                    }
                    continue;
                }
                switch (specs[s].func) {
                    case AggFunc::Sum:   stats_.result[out_idx] = acc.sum; break;
                    case AggFunc::Count: stats_.result[out_idx] = static_cast<double>(acc.count); break;
                    case AggFunc::Avg:
                        stats_.result[out_idx] = acc.count > 0 ? acc.sum / acc.count : 0.0;
                        break;
                }
            }
            ++g_idx;
        }
    }

    NativeGroupedAggregateStats stats_;
};

}  // namespace opteryx::engine
