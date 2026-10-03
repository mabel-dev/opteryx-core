// vector_index_admission.hpp — the vector search scan's row admission
// (docs/VECTOR_INDEX_DESIGN.md §8, D2; D-4 `COSINE_DISTANCE`, D-9 `nprobe`).
//
// `ORDER BY COSINE_DISTANCE(col, 'query') LIMIT k` runs as an ordinary plan — the
// native parquet scan, the projection computing the EXACT distance, the Top-N sink — with
// one difference: the scan decodes only the rows this admits. Decided once, at execution
// start (RowAdmission::prepare runs in the scan's make_global), natively:
//
//   indexed file    the file's index is searched (vector_index_search.hpp): the query is
//                   embedded once and scored against the STORED vectors — every one by
//                   default (exact, ruled 2026-10-03), or only the `nprobe` nearest
//                   clusters' when the caller set `nprobe` (approximate, no distance
//                   bound). The file's top-k rows (deleted rows excluded) are admitted —
//                   k rows of the file, decoded through the pipeline's row masks.
//   uncovered file  a file the index does not cover yet (an async index behind, a file
//                   with no indexable row) is searched EXACTLY (ruled 2026-10-03): every
//                   row that is not deleted is admitted, and the distance is computed for
//                   all of them. Reported in the counts below.
//
// A WHERE (pushed into the scan) is applied BEFORE the search, never after it (§8, "no
// overfetch loop"): pass 1 decodes the predicate columns of every scanned row group and
// evaluates the predicate natively (the latmat pass-1 C ABI), and only its survivors are
// admitted — an indexed file's search scores EVERY survivor from its stored vector
// (TopK's admitted mask over all row groups; `nprobe` is not applied: picking clusters
// by proximity and rows by predicate would keep only rows that pass both, ruled
// 2026-10-03), an uncovered file admits them all.
//
// The union of the per-file top-k sets holds the global top-k (exact by default; at the
// probe's recall when `nprobe` is set); the Top-N sink orders it by the exact distance.
// Deleted rows are excluded here for every file, so this scan needs no delete vector of
// its own.

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "engine/native_parquet_scan_source.hpp"   // RowAdmission
#include "engine/vector_index_search.hpp"         // search_index_file, embed_query

namespace opteryx::engine {

struct AdmissionFile {
    std::string           path;      // as the scan's work items name it (the fetch path)
    std::string           filter_path;  // as the pass-1 (WHERE) plan names it; may differ (signed)
    uint64_t              rows = 0;  // the data file's physical rows
    std::vector<uint32_t> deleted;   // ascending physical ordinals
    bool                  indexed = false;
    IndexFileRef          index;     // when indexed
};

// The pushed WHERE, evaluated in pass 1. All borrowed from the compiler's pass-1 plan
// (a NativeScanPlan over the predicate columns) and its Pass1PredResolver, which the
// native plan holds for the run.
struct AdmissionPredicate {
    rugo::ParquetIOPipeline*                       pipeline = nullptr;
    const ParquetFooterMap*                        footers = nullptr;
    const std::vector<std::pair<std::string, int>>* work_items = nullptr;   // row groups pruning kept
    const std::vector<std::string>*                column_names = nullptr;
    NativeScanColumnBuilder                        builder;                 // the plan's decode flags
    rugo::Pass1PredFn                              fn = nullptr;
    void*                                          ctx = nullptr;
    std::vector<int>                               pred_col_to_p1;
    int                                            in_flight = 4;
};

struct AdmissionCounts {
    uint32_t files_indexed = 0;
    uint32_t files_exact = 0;        // uncovered files, searched exactly
    uint64_t clusters_probed = 0;
    uint64_t row_groups_read = 0;    // vectors-file row groups fetched
    uint64_t candidates = 0;         // rows admitted from indexed files
    uint64_t rows_exact = 0;         // rows admitted from uncovered files
};

class VectorIndexAdmission final : public RowAdmission {
  public:
    VectorIndexAdmission(std::vector<AdmissionFile> files, std::shared_ptr<CxxMorsel> query,
                         EmbedFn embed, uint32_t dims, uint32_t k, uint32_t nprobe)
        : files_(std::move(files)), query_(std::move(query)), embed_(embed), dims_(dims), k_(k),
          nprobe_(nprobe) {}

    // Plan-time only, before the run: the pushed WHERE (see AdmissionPredicate).
    void set_predicate(AdmissionPredicate predicate) {
        predicate_ = std::move(predicate);
        filtered_ = true;
    }

    bool prepare(const ParquetFooterMap& footers, std::string* err) override {
        if (query_ == nullptr || query_->columns.size() != 1u) {
            *err = "vector index search: no query";
            return false;
        }
        std::vector<uint16_t> q;
        bool searchable = false;
        // A filter's survivors are all scored: the probe applies to an unfiltered search only.
        const uint32_t nprobe = filtered_ ? 0u : nprobe_;
        if (!embed_query(embed_, dims_, query_->columns[0].view, &q, &searchable, err)) return false;
        // Pass 1: {fetch path: ordinal-indexed survivor bitmap}. A row group pruning
        // dropped (or the dictionary skip proved empty) contributes no survivor.
        std::unordered_map<std::string, std::vector<uint8_t>> survivors;
        if (filtered_ && !run_pass1(footers, &survivors, err)) return false;
        for (const AdmissionFile& f : files_) {
            auto fit = footers.find(f.path);
            if (fit == footers.end()) {
                *err = "vector index search: " + f.path + " is not one of the scan's files";
                return false;
            }
            const auto& groups = fit->second->row_groups;
            std::vector<uint64_t> first(groups.size() + 1u, 0u);
            for (size_t g = 0; g < groups.size(); ++g) first[g + 1u] = first[g] + static_cast<uint64_t>(groups[g].num_rows);
            if (first.back() != f.rows) {
                *err = "vector index search: " + f.path + " holds " + std::to_string(first.back()) +
                       " rows, its manifest entry " + std::to_string(f.rows);
                return false;
            }
            FileMasks& fm = masks_[f.path];
            fm.kind.assign(groups.size(), kAll);
            fm.masks.assign(groups.size(), {});
            auto set_row = [&](uint64_t ordinal) {
                const size_t g = static_cast<size_t>(
                    std::upper_bound(first.begin(), first.end(), ordinal) - first.begin()) - 1u;
                if (fm.kind[g] != kMask) {
                    fm.kind[g] = kMask;
                    fm.masks[g].assign(static_cast<size_t>(first[g + 1u] - first[g]), 0u);
                }
                fm.masks[g][static_cast<size_t>(ordinal - first[g])] = 1u;
            };
            // The filter's survivors, ordinal-indexed, minus the deleted rows (nullptr
            // when there is no filter: every row is a candidate).
            const std::vector<uint8_t>* admitted = nullptr;
            std::vector<uint8_t> none;
            uint64_t survivors_n = f.rows - f.deleted.size();
            if (filtered_) {
                auto sit = survivors.find(f.filter_path);
                std::vector<uint8_t>& bits = sit == survivors.end() ? none : sit->second;
                bits.resize((f.rows + 7u) / 8u, 0u);
                for (uint32_t ordinal : f.deleted)
                    if (ordinal < f.rows) bits[ordinal >> 3] &= static_cast<uint8_t>(~(1u << (ordinal & 7u)));
                survivors_n = 0;
                for (uint8_t b : bits) survivors_n += static_cast<uint64_t>(__builtin_popcount(b));
                admitted = &bits;
            }
            if (f.indexed) {
                ++counts_.files_indexed;
                fm.kind.assign(groups.size(), kNone);
                if (!searchable || survivors_n == 0) continue;
                std::vector<draken::ann::AnnHit> hits;
                IndexSearchStats stats;
                if (!search_index_file(f.index, q.data(), dims_, k_, nprobe, f.rows, f.deleted,
                                       admitted == nullptr ? nullptr : admitted->data(), &hits, &stats, err))
                    return false;
                counts_.clusters_probed += stats.probed;
                counts_.row_groups_read += stats.row_groups_read;
                counts_.candidates += hits.size();
                for (const auto& h : hits) set_row(h.ordinal);
            } else if (filtered_) {
                ++counts_.files_exact;
                counts_.rows_exact += survivors_n;
                fm.kind.assign(groups.size(), kNone);
                for (uint64_t ordinal = 0; ordinal < f.rows; ++ordinal)
                    if ((*admitted)[ordinal >> 3] >> (ordinal & 7u) & 1u) set_row(ordinal);
            } else {
                ++counts_.files_exact;
                counts_.rows_exact += f.rows - f.deleted.size();
                if (f.deleted.empty()) continue;
                // Every row but the deleted ones: start the touched row groups all-admitted.
                for (uint32_t ordinal : f.deleted) {
                    if (ordinal >= f.rows) { *err = "vector index search: a deleted ordinal is outside " + f.path; return false; }
                    const size_t g = static_cast<size_t>(
                        std::upper_bound(first.begin(), first.end(), ordinal) - first.begin()) - 1u;
                    if (fm.kind[g] != kMask) {
                        fm.kind[g] = kMask;
                        fm.masks[g].assign(static_cast<size_t>(first[g + 1u] - first[g]), 1u);
                    }
                    fm.masks[g][static_cast<size_t>(ordinal - first[g])] = 0u;
                }
            }
        }
        return true;
    }

    const std::vector<uint8_t>* mask(const std::string& path, int rg) const override {
        auto it = masks_.find(path);
        if (it == masks_.end() || rg < 0 || static_cast<size_t>(rg) >= it->second.kind.size()) return nullptr;
        switch (it->second.kind[static_cast<size_t>(rg)]) {
            case kNone: return &kNoRows;
            case kMask: return &it->second.masks[static_cast<size_t>(rg)];
            default:    return nullptr;
        }
    }

    const AdmissionCounts& counts() const noexcept { return counts_; }

  private:
    // Decode the predicate columns of every pass-1 work item, evaluate the predicate, and
    // set each survivor's bit in its file's ordinal-indexed bitmap.
    bool run_pass1(const ParquetFooterMap& footers,
                   std::unordered_map<std::string, std::vector<uint8_t>>* out, std::string* err) {
        AdmissionPredicate& p = predicate_;
        const auto& items = *p.work_items;
        // Row offset of each row group in its file (the bitmap is by file ordinal).
        auto first_row = [&](const std::string& path, int rg) -> int64_t {
            auto fit = p.footers->find(path);
            if (fit == p.footers->end()) return -1;
            int64_t at = 0;
            for (int g = 0; g < rg; ++g) at += fit->second->row_groups[static_cast<size_t>(g)].num_rows;
            return at;
        };
        size_t next = 0;
        int owed = 0;
        while (next < items.size() || owed > 0) {
            while (next < items.size() && owed < std::max(1, p.in_flight)) {
                const auto& item = items[next++];
                auto fit = p.footers->find(item.first);
                if (fit == p.footers->end()) { *err = "vector search: a filtered file has no footer"; return false; }
                std::vector<std::string> names;
                std::vector<std::vector<ColumnStats>> stats;
                std::shared_ptr<const rugo::NestedSpec> nested;
                std::string rerr;
                if (!rugo::resolve_projection(*fit->second, {item.second}, *p.column_names, names, stats, nested, rerr)) {
                    *err = "vector search: " + rerr;
                    return false;
                }
                p.pipeline->submit_block(item.first, {item.second}, names, stats, {}, nested);
                ++owed;
            }
            rugo::MorselRef result;
            if (!p.pipeline->wait_and_get_result(result)) {
                *err = "vector search: the filter's pipeline drained with results missing";
                return false;
            }
            --owed;
            if (!result.success) {
                *err = "vector search: " + (result.error.empty() ? std::string("decode error") : result.error);
                return false;
            }
            if (result.empty_filtered) continue;
            const uint32_t nrows = result.columns.empty() ? 0u : result.columns[0].length;
            if (nrows == 0) continue;
            CxxMorsel m;
            m.names = *p.column_names;
            for (size_t i = 0; i < result.columns.size(); ++i) {
                CxxColumn col;
                ErrCtx ec;
                if (!p.builder.build_column(result, i, col, ec)) {
                    *err = std::string("vector search: ") + (ec.msg ? ec.msg : "cannot decode a filter column");
                    return false;
                }
                m.columns.push_back(std::move(col));
            }
            std::vector<DrakenVector*> cols;
            for (int ci : p.pred_col_to_p1) {
                if (ci < 0 || static_cast<size_t>(ci) >= m.columns.size()) {
                    *err = "vector search: a filter column is outside the filter's layout";
                    return false;
                }
                cols.push_back(&m.columns[static_cast<size_t>(ci)].view);
            }
            std::vector<uint8_t> mask((nrows + 7u) / 8u, 0u);
            if (p.fn(p.ctx, cols.data(), static_cast<int>(cols.size()), nrows, mask.data()) != 0) {
                *err = "vector search: the filter could not be evaluated";
                return false;
            }
            const int64_t base = first_row(result.path, result.rg_idx);
            if (base < 0) { *err = "vector search: a filtered row group has no footer"; return false; }
            std::vector<uint8_t>& bits = (*out)[result.path];
            const uint64_t need = (static_cast<uint64_t>(base) + nrows + 7u) / 8u;
            if (bits.size() < need) bits.resize(need, 0u);
            for (uint32_t r = 0; r < nrows; ++r)
                if ((mask[r >> 3] >> (r & 7u)) & 1u) {
                    const uint64_t o = static_cast<uint64_t>(base) + r;
                    bits[o >> 3] |= static_cast<uint8_t>(1u << (o & 7u));
                }
        }
        return true;
    }

    static constexpr uint8_t kAll = 0, kNone = 1, kMask = 2;
    struct FileMasks {
        std::vector<uint8_t>              kind;
        std::vector<std::vector<uint8_t>> masks;
    };
    inline static const std::vector<uint8_t> kNoRows{0u};

    std::vector<AdmissionFile>                 files_;
    std::shared_ptr<CxxMorsel>                 query_;
    EmbedFn                                    embed_;
    uint32_t                                   dims_;
    uint32_t                                   k_;
    uint32_t                                   nprobe_;
    std::unordered_map<std::string, FileMasks> masks_;
    AdmissionCounts                            counts_;
    AdmissionPredicate                         predicate_;
    bool                                       filtered_ = false;
};

}  // namespace opteryx::engine
