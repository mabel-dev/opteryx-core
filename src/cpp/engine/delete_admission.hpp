#pragma once
// src/cpp/engine/delete_admission.hpp — merge-on-read deletes on the native parquet scan.
//
// A RowAdmission (native_parquet_scan_source.hpp) built from the manifest's resolved
// delete vectors: each file's deleted rows, as ascending file-local ordinals. A row group
// none of whose rows is deleted admits every row (nullptr mask, the scan's undisturbed
// path); a row group with deleted rows admits the rest through a one-byte-per-row mask,
// decoded selectively; a row group whose every row is deleted admits nothing and is
// never submitted. Decided once at execution start (prepare, in the scan's make_global)
// from the scan's own footers — the row-group boundaries come from their row counts.

#include <algorithm>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>
#include <ankerl/unordered_dense.h>

#include "engine/native_parquet_scan_source.hpp"

namespace opteryx::engine {

struct DeleteAdmission : RowAdmission {
    // One entry per delete-bearing file: its FETCH path (the key work items and the
    // footer map use) and its deleted ordinals, ascending.
    struct File {
        std::string path;
        std::vector<uint32_t> deleted;
    };
    std::vector<File> files;

    bool prepare(const ParquetFooterMap& footers, std::string* err) override {
        masks_.clear();
        for (const File& f : files) {
            auto fit = footers.find(f.path);
            if (fit == footers.end()) {
                *err = "merge-on-read deletes: no footer for " + f.path;
                return false;
            }
            const auto& groups = fit->second->row_groups;
            // first[g] = file ordinal of row group g's first row; first[n] = file rows.
            std::vector<uint64_t> first(groups.size() + 1u, 0u);
            for (size_t g = 0; g < groups.size(); ++g)
                first[g + 1u] = first[g] + static_cast<uint64_t>(groups[g].num_rows);
            std::vector<std::vector<uint8_t>>& masks = masks_[f.path];
            masks.assign(groups.size(), std::vector<uint8_t>{});
            for (uint32_t ordinal : f.deleted) {
                if (ordinal >= first.back()) {
                    *err = "merge-on-read deletes: a deleted ordinal is outside " + f.path;
                    return false;
                }
                const size_t g = static_cast<size_t>(
                    std::upper_bound(first.begin(), first.end(), static_cast<uint64_t>(ordinal)) -
                    first.begin()) - 1u;
                if (masks[g].empty())
                    masks[g].assign(static_cast<size_t>(first[g + 1u] - first[g]), 1u);
                masks[g][static_cast<size_t>(ordinal - first[g])] = 0u;
            }
        }
        return true;
    }

    const std::vector<uint8_t>* mask(const std::string& path, int rg) const override {
        auto it = masks_.find(path);
        if (it == masks_.end() || rg < 0 || static_cast<size_t>(rg) >= it->second.size())
            return nullptr;
        const std::vector<uint8_t>& m = it->second[static_cast<size_t>(rg)];
        return m.empty() ? nullptr : &m;
    }

  private:
    // path -> per-row-group mask; an EMPTY mask is "no deleted row" (admit all).
    ankerl::unordered_dense::map<std::string, std::vector<std::vector<uint8_t>>> masks_;
};

}  // namespace opteryx::engine
