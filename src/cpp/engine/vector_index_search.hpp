// vector_index_search.hpp — search ONE data file's vector index (docs/VECTOR_INDEX_DESIGN.md
// §8, D2). Native, called with the GIL released.
//
// The index is one flat file (vector_index_file.hpp): the footer holds the centroids and the
// block table, the body the blocks — one cluster's rows each, byte-addressable. A search
// opens the file in ONE read (the catalog records the footer length) and then reads:
//
//   exact (nprobe 0, the default)   every block, as large parallel range reads;
//   approximate (nprobe >= 1)       only the blocks of the `nprobe` clusters whose centres
//                                   are nearest the query — a guess with no distance bound
//                                   (ruled 2026-10-03): a row in an unprobed cluster can be
//                                   nearer than every row scored.
//
// Rows are scored with draken's TopK::offer — the same cosine distance the SQL kernel
// returns, so the distance a caller sees needs no re-rank — with deleted ordinals excluded
// and, when given, only `admitted` ordinals (a filter's survivors) scored. The result is
// this file's top-k by (distance, ordinal); a caller merges files with the same order.

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "engine/vector_index_file.hpp"
#include "engine/vector_index_build.hpp"   // EmbedFn, vib_detail::OwnedVecResult
#include "ops/ann/fp16_cosine_ivf.h"

namespace opteryx::engine {

struct IndexFileRef {
    std::string path;               // local path, gs:// object or presigned URL
    uint64_t    file_bytes = 0;
    uint64_t    footer_bytes = 0;   // 0 = unknown (one more round trip)
    std::string auth_header;        // Authorization for a gs:// read; empty = none
};

struct IndexSearchStats {
    uint32_t clusters = 0;          // in the file's index
    uint32_t probed = 0;            // clusters probed (0 when exact)
    uint32_t blocks_read = 0;
    uint64_t bytes_read = 0;        // of the body
    uint32_t requests = 0;          // range reads issued, the footer's included
    uint64_t rows_scored = 0;
};

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

// The top-k rows of one indexed data file nearest `query` (fp16, `dims` wide). `nprobe`
// 0 = exact (every block); >= 1 = approximate, the `nprobe` nearest clusters' blocks only.
// `deleted`: ascending physical ordinals to exclude. `admitted` (nullable): a bitmap by
// ordinal of the rows a filter kept, `data_rows` bits long. Returns false with `err` on any
// failure — an index file that does not match its definition fails loud, never skips.
inline bool search_index_file(const IndexFileRef& file, const uint16_t* query, uint32_t dims,
                              uint32_t k, uint32_t nprobe, uint64_t data_rows,
                              const std::vector<uint32_t>& deleted, const uint8_t* admitted,
                              std::vector<draken::ann::AnnHit>* out, IndexSearchStats* stats,
                              std::string* err) {
    out->clear();
    if (dims == 0u) { *err = "vector index search: dims must be >= 1"; return false; }
    if (k == 0u || !draken::ann::ann_row_searchable(query, dims)) return true;

    VectorIndexFile index;
    if (!index.open(file.path, file.file_bytes, file.footer_bytes, file.auth_header, err)) return false;
    stats->requests += file.footer_bytes > 0u ? 1u : 2u;
    if (index.dims() != dims) {
        *err = "vector index search: " + file.path + " holds " + std::to_string(index.dims()) +
               "-dimensional vectors, the index is " + std::to_string(dims);
        return false;
    }
    stats->clusters = index.clusters();

    std::vector<uint32_t> wanted;
    if (nprobe == 0u) {
        wanted.resize(index.blocks());
        for (uint32_t b = 0; b < wanted.size(); ++b) wanted[b] = b;
    } else {
        const std::vector<uint32_t> probe = draken::ann::ivf_probe(
            index.centroids(), index.clusters(), dims, index.cluster_rows(), query, nprobe);
        stats->probed = static_cast<uint32_t>(probe.size());
        wanted = index.blocks_of(probe);
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
    std::vector<uint32_t> identity;
    std::string bad;
    VectorIndexReadStats read;
    bool ok = index.for_each_block(wanted, [&](const VectorIndexBlock& b) {
        if (!bad.empty()) return;
        for (uint32_t i = 0; i < b.rows; ++i)
            if (b.ordinals[i] >= data_rows) {
                bad = "vector index search: " + file.path + " holds ordinal " + std::to_string(b.ordinals[i]) +
                      " beyond its data file's " + std::to_string(data_rows) + " rows";
                return;
            }
        if (identity.size() < b.rows) {
            const uint32_t from = static_cast<uint32_t>(identity.size());
            identity.resize(b.rows);
            for (uint32_t i = from; i < b.rows; ++i) identity[i] = i;
        }
        const draken::ann::Fp16Column block{b.vectors, identity.data(), nullptr, b.rows, dims};
        top.offer(block, nullptr, b.rows, b.ordinals, query,
                  excluded.empty() ? nullptr : excluded.data(), admitted);
        stats->rows_scored += b.rows;
    }, &read, err);
    if (!ok) return false;
    if (!bad.empty()) { *err = bad; return false; }
    stats->blocks_read = read.blocks_read;
    stats->bytes_read = read.bytes_read;
    stats->requests += read.requests;
    *out = top.take();
    return true;
}

}  // namespace opteryx::engine
