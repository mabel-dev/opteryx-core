// vector_index_search.hpp — search ONE data file's vector index (docs/VECTOR_INDEX_DESIGN.md
// §8, D2). Native, called with the GIL released.
//
// The index is IVF-flat in two skene files (§5.2): the centroids file (K rows: the centroid,
// its row count, the vectors-file row groups holding its rows) and the vectors file (row
// groups of one cluster's rows each: the fp16 embedding and the row's PHYSICAL ordinal in
// the data file). The DEFAULT search is EXACT (ruled 2026-10-03): every row group of the
// vectors file is scored and the centroids file is never read. Only when the caller sets
// `nprobe` >= 1 is it approximate: the whole centroids file — K x (2 dims + ...) bytes,
// small — is read, the `nprobe` nearest non-empty clusters picked, and ONLY their row
// groups of the vectors file read. Picking clusters by their centres is a guess with no
// distance bound: a row in an unprobed cluster can be nearer than every row scored.
// Files are read through SkeneRangedFile: a local file by pread, a remote one by range
// GETs on a signed URL.
//
// Rows are scored with draken's TopK::offer — the same cosine distance the SQL kernel
// returns, so the distance a caller sees needs no re-rank — with deleted ordinals excluded
// and, when given, only `admitted` ordinals (a filter's survivors) scored. The result is
// this file's top-k by (distance, ordinal); a caller merges files with the same order.

#pragma once

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

#include "engine/skene_ranged_file.hpp"
#include "engine/vector_index_build.hpp"   // EmbedFn, vib_detail::OwnedVecResult
#include "ops/ann/fp16_cosine_ivf.h"

namespace opteryx::engine {

struct IndexFileRef {
    std::string vectors;            // local path or signed URL
    uint64_t    vectors_bytes = 0;
    std::string centroids;
    uint64_t    centroids_bytes = 0;
};

struct IndexSearchStats {
    uint32_t clusters = 0;          // in the file's index (0 when exact: centroids not read)
    uint32_t probed = 0;            // clusters probed (0 when exact)
    uint32_t row_groups_read = 0;   // vectors-file row groups fetched
    uint64_t rows_scored = 0;       // rows those row groups held
};

namespace search_detail {

inline bool find(const CxxMorsel& m, const char* name, const DrakenVector** out) {
    for (size_t i = 0; i < m.names.size(); ++i)
        if (m.names[i] == name) { *out = &m.columns[i].view; return true; }
    return false;
}

inline bool child_of(const CxxMorsel& m, const char* name, const DrakenVector** out) {
    for (size_t i = 0; i < m.names.size(); ++i)
        if (m.names[i] == name) {
            const CxxColumn& c = m.columns[i];
            if (!c.own || !c.own->child_owner) return false;
            *out = &c.own->child_owner->vec;
            return true;
        }
    return false;
}

}  // namespace search_detail

// Embed ONE query text through the registered kernel - the index's own embedder - into
// `out` (dims fp16). `text` holds exactly that one row. False with `err` when the kernel
// fails or answers with the wrong shape; a null query embeds to nothing (`*ok` false).
inline bool embed_query(EmbedFn embed, uint32_t dims, const DrakenVector& text,
                        std::vector<uint16_t>* out, bool* ok, std::string* err) {
    if (embed == nullptr || dims == 0u) { *err = "vector index search: no embedding kernel"; return false; }
    if (text.length != 1u) { *err = "vector index search: the query is one text"; return false; }
    vector_dim_ctx ctx{dims};
    const DrakenVector* args[1] = {&text};
    vib_detail::OwnedVecResult emb(embed(&ctx, args, 1u));
    if (emb.r.data == nullptr) {
        *err = std::string("vector index search: ") + (emb.r.error_msg ? emb.r.error_msg : "embedding failed");
        return false;
    }
    if (emb.r.type != DRAKEN_VECTOR_FP16 || emb.r.vec_dimension != dims || emb.r.length != 1u) {
        *err = "vector index search: the embedding kernel returned the wrong type, width or row count";
        return false;
    }
    *ok = emb.r.validity == nullptr || vib_detail::bit(emb.r.validity, 0u);
    const uint16_t* data = static_cast<const uint16_t*>(emb.r.data);
    out->assign(data + static_cast<size_t>(emb.r.selection[0]) * dims,
                data + static_cast<size_t>(emb.r.selection[0]) * dims + dims);
    return true;
}

// Approximate search only: read the whole centroids file and name the vectors-file row
// groups of the `nprobe` clusters whose centres are nearest `query`, ascending.
inline bool probe_row_groups(const IndexFileRef& file, const uint16_t* query, uint32_t dims,
                             uint32_t nprobe, std::vector<uint32_t>* wanted, IndexSearchStats* stats,
                             std::string* err) {
    using namespace search_detail;
    SkeneRangedFile centroids;
    if (!centroids.open(file.centroids, file.centroids_bytes, {"centroid", "rows", "row_groups"}, err)) return false;
    std::vector<uint16_t> cents;
    std::vector<uint32_t> counts;
    std::vector<std::vector<uint32_t>> groups;
    for (uint32_t g = 0; g < centroids.row_groups(); ++g) {
        SkeneRangedFile::RowGroup rg;
        if (!centroids.read(g, &rg, err)) return false;
        const DrakenVector *c = nullptr, *n = nullptr, *lists = nullptr, *items = nullptr;
        if (!find(rg.morsel, "centroid", &c) || c->type != DRAKEN_VECTOR_FP16 ||
            !find(rg.morsel, "rows", &n) || n->type != DRAKEN_UINT32 ||
            !find(rg.morsel, "row_groups", &lists) || lists->type != DRAKEN_ARRAY ||
            !child_of(rg.morsel, "row_groups", &items) || items->type != DRAKEN_INT32) {
            *err = "vector index search: " + file.centroids + " is not a centroids file";
            return false;
        }
        const uint16_t* cdata = static_cast<const uint16_t*>(c->data);
        const uint32_t* ndata = static_cast<const uint32_t*>(n->data);
        const int32_t* offsets = static_cast<const int32_t*>(lists->data);
        const int32_t* values = static_cast<const int32_t*>(items->data);
        for (uint32_t i = 0; i < c->length; ++i) {
            const uint16_t* row = cdata + static_cast<size_t>(c->selection[i]) * dims;
            cents.insert(cents.end(), row, row + dims);
            counts.push_back(ndata[n->selection[i]]);
            const uint32_t at = lists->selection[i];
            std::vector<uint32_t> rgs;
            for (int32_t j = offsets[at]; j < offsets[at + 1u]; ++j) {
                const int32_t v = values[items->selection[static_cast<uint32_t>(j)]];
                if (v < 0) { *err = "vector index search: " + file.centroids + " names a negative row group"; return false; }
                rgs.push_back(static_cast<uint32_t>(v));
            }
            groups.push_back(std::move(rgs));
        }
    }
    const uint32_t K = static_cast<uint32_t>(counts.size());
    stats->clusters = K;

    // ── Probe: only the probed clusters' row groups of the vectors file ──
    const std::vector<uint32_t> probe =
        draken::ann::ivf_probe(cents.data(), K, dims, counts.data(), query, nprobe);
    stats->probed = static_cast<uint32_t>(probe.size());
    for (uint32_t c : probe) wanted->insert(wanted->end(), groups[c].begin(), groups[c].end());
    std::sort(wanted->begin(), wanted->end());
    return true;
}

// The top-k rows of one indexed data file nearest `query` (fp16, `dims` wide). `nprobe`
// 0 = exact (every row group); >= 1 = approximate, the `nprobe` nearest clusters only.
// `deleted`: ascending physical ordinals to exclude. `admitted` (nullable): a bitmap by
// ordinal of the rows a filter kept, `data_rows` bits long. Returns false with `err` on any
// failure — an index file that does not match its definition fails loud, never skips.
inline bool search_index_file(const IndexFileRef& file, const uint16_t* query, uint32_t dims,
                              uint32_t k, uint32_t nprobe, uint64_t data_rows,
                              const std::vector<uint32_t>& deleted, const uint8_t* admitted,
                              std::vector<draken::ann::AnnHit>* out, IndexSearchStats* stats,
                              std::string* err) {
    using namespace search_detail;
    out->clear();
    if (dims == 0u) { *err = "vector index search: dims must be >= 1"; return false; }
    if (k == 0u || !draken::ann::ann_row_searchable(query, dims)) return true;

    SkeneRangedFile vectors;
    if (!vectors.open(file.vectors, file.vectors_bytes, {"embedding", "ordinal"}, err)) return false;
    std::vector<uint32_t> wanted;
    if (nprobe == 0u) {
        wanted.resize(vectors.row_groups());
        for (uint32_t g = 0; g < vectors.row_groups(); ++g) wanted[g] = g;
    } else if (!probe_row_groups(file, query, dims, nprobe, &wanted, stats, err)) {
        return false;
    }
    if (wanted.empty()) return true;

    std::vector<uint8_t> excluded;
    if (!deleted.empty()) {
        excluded.assign((data_rows + 7u) / 8u, 0u);
        for (uint32_t ordinal : deleted) {
            if (ordinal >= data_rows) { *err = "vector index search: a deleted ordinal is outside the file"; return false; }
            excluded[ordinal >> 3] |= static_cast<uint8_t>(1u << (ordinal & 7u));
        }
    }

    draken::ann::TopK top(k);
    for (uint32_t g : wanted) {
        if (g >= vectors.row_groups()) {
            *err = "vector index search: " + file.centroids + " names row group " + std::to_string(g) +
                   " that " + file.vectors + " does not have";
            return false;
        }
        SkeneRangedFile::RowGroup rg;
        if (!vectors.read(g, &rg, err)) return false;
        const DrakenVector *emb = nullptr, *ord = nullptr;
        if (!find(rg.morsel, "embedding", &emb) || emb->type != DRAKEN_VECTOR_FP16 ||
            !find(rg.morsel, "ordinal", &ord) || ord->type != DRAKEN_UINT32 || emb->length != ord->length) {
            *err = "vector index search: " + file.vectors + " is not a vectors file";
            return false;
        }
        // Ordinals in selection order, so TopK can map a block row to its file ordinal.
        std::vector<uint32_t> ordinals(ord->length);
        const uint32_t* odata = static_cast<const uint32_t*>(ord->data);
        for (uint32_t i = 0; i < ord->length; ++i) {
            ordinals[i] = odata[ord->selection[i]];
            if (ordinals[i] >= data_rows) {
                *err = "vector index search: " + file.vectors + " holds ordinal " + std::to_string(ordinals[i]) +
                       " beyond its data file's " + std::to_string(data_rows) + " rows";
                return false;
            }
        }
        const draken::ann::Fp16Column block{static_cast<const uint16_t*>(emb->data), emb->selection,
                                            emb->validity, emb->length, dims};
        top.offer(block, nullptr, emb->length, ordinals.data(), query,
                  excluded.empty() ? nullptr : excluded.data(), admitted);
        ++stats->row_groups_read;
        stats->rows_scored += emb->length;
    }
    *out = top.take();
    return true;
}

}  // namespace opteryx::engine
