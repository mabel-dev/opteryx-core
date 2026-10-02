#pragma once
// draken/ops/ann/fp16_cosine_ivf.h — approximate nearest-neighbour search over one
// VECTOR_FP16 column by inverted file (IVF-flat), plus the exact scan
// (docs/VECTOR_INDEX_DESIGN.md §5.2, D-5 ruled 2026-10-02: IVF, because a query reads only
// the probed clusters — on object storage the bytes read decide the cost).
//
// Build: spherical k-means. Centroids are trained on a deterministic sample, then every
// searchable row is assigned to its nearest centroid. The result is a cluster-major row
// ORDER (rows ascending within a cluster) with per-cluster OFFSETS — exactly the layout
// the vectors file is written in, one row group per cluster.
//
// Search: rank the centroids against the query, then score every row of the `nprobe`
// nearest non-empty clusters exactly. Scoring is TopK::offer over a contiguous block,
// the shape a cluster row group arrives in, keyed by data-file ORDINAL.
//
// Distance everywhere is the engine's cosine (ops/vector_cosine_row.h) as
//     distance = 1 - clip(cosine, -1, 1)
// exactly as COSINE_DISTANCE defines it, so a returned distance IS the SQL kernel's value.
// Results are ordered by (distance, ordinal) — total and deterministic.
//
// Searchable domain: a row is searchable when it is valid, not excluded (deleted), and
// finite with non-zero magnitude. Others have no defined cosine; they are never clustered
// and never returned — identically on the exact path.
//
// Deterministic: same input + params ⇒ same centroids and order, for any thread count.
// Errors are thrown with the reason; the engine boundary converts them. No Python, no GIL,
// no opteryx dependency.

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <random>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#include "core/buffers.h"
#include "fp16/fp16.h"
#include "ops/vector_cosine_row.h"

namespace draken { namespace ann {

static inline bool ann_bit(const uint8_t* bits, uint32_t i) noexcept {
    return (bits[i >> 3] >> (i & 7u)) & 1u;
}

// Finite elements and at least one non-zero: the row has a defined cosine. Exact: a non-zero
// fp16 squared is >= 2^-48, never zero in fp64.
static inline bool ann_row_searchable(const uint16_t* row, uint32_t dims) noexcept {
    bool any_nonzero = false;
    for (uint32_t k = 0; k < dims; ++k) {
        const uint16_t h = row[k];
        if ((h & 0x7C00u) == 0x7C00u) return false;   // Inf or NaN
        any_nonzero |= (h & 0x7FFFu) != 0u;
    }
    return any_nonzero;
}

static inline double ann_cosine_distance(const uint16_t* a, const uint16_t* b, uint32_t dims) noexcept {
    double s = ops::cosine_row_fp16(a, b, dims);
    if (s < -1.0) s = -1.0; else if (s > 1.0) s = 1.0;
    return 1.0 - s;
}

// Read-only view of a VECTOR_FP16 column: row r is data[selection[r] * dims].
struct Fp16Column {
    const uint16_t* data;
    const uint32_t* selection;
    const uint8_t*  validity;   // nullptr = all valid
    uint32_t        rows;
    uint32_t        dims;

    static Fp16Column of(const DrakenVector& v, uint32_t dims) {
        if (v.type != DRAKEN_VECTOR_FP16)
            throw std::invalid_argument("ann: the indexed column must be VECTOR_FP16");
        if (dims == 0u)
            throw std::invalid_argument("ann: dimension must be >= 1");
        return Fp16Column{static_cast<const uint16_t*>(v.data), v.selection, v.validity,
                          v.length, dims};
    }
    const uint16_t* row(uint32_t r) const noexcept {
        return data + static_cast<size_t>(selection[r]) * dims;
    }
    bool valid(uint32_t r) const noexcept { return validity == nullptr || ann_bit(validity, r); }
};

struct AnnHit {
    uint32_t ordinal;
    double   distance;
};

static inline bool ann_hit_before(const AnnHit& a, const AnnHit& b) noexcept {
    return a.distance < b.distance || (a.distance == b.distance && a.ordinal < b.ordinal);
}

// Bounded top-k by (distance, ordinal). Offer blocks in any order; the result is the same.
class TopK {
  public:
    explicit TopK(uint32_t k) : k_(k) { heap_.reserve(k + 1u); }

    // Score rows of `block` against `query`. `rows` (nullable) selects which block rows to
    // score (all when null); `ordinals` (nullable) maps a block row to its data-file ordinal
    // (identity when null). Masks are indexed by ORDINAL: `excluded` rows (deleted) and rows
    // whose `admitted` bit is clear are skipped. A NaN distance is exactly "not searchable"
    // (zero-magnitude or non-finite row against a searchable query), so one pass decides it.
    void offer(const Fp16Column& block, const uint32_t* rows, uint32_t count,
               const uint32_t* ordinals, const uint16_t* query, const uint8_t* excluded,
               const uint8_t* admitted) {
        if (k_ == 0u) return;
        for (uint32_t i = 0; i < count; ++i) {
            const uint32_t r = rows ? rows[i] : i;
            const uint32_t ordinal = ordinals ? ordinals[r] : r;
            if (admitted != nullptr && !ann_bit(admitted, ordinal)) continue;
            if (excluded != nullptr && ann_bit(excluded, ordinal)) continue;
            if (!block.valid(r)) continue;
            const AnnHit h{ordinal, ann_cosine_distance(query, block.row(r), block.dims)};
            if (std::isnan(h.distance)) continue;
            if (heap_.size() < k_) {
                heap_.push_back(h);
                std::push_heap(heap_.begin(), heap_.end(), ann_hit_before);
            } else if (ann_hit_before(h, heap_.front())) {
                std::pop_heap(heap_.begin(), heap_.end(), ann_hit_before);
                heap_.back() = h;
                std::push_heap(heap_.begin(), heap_.end(), ann_hit_before);
            }
        }
    }

    std::vector<AnnHit> take() {
        std::sort(heap_.begin(), heap_.end(), ann_hit_before);
        return std::move(heap_);
    }

  private:
    uint32_t k_;
    std::vector<AnnHit> heap_;
};

// The exact path: every searchable, admitted row. The recall reference, and the path for
// small files and selective filters.
static inline std::vector<AnnHit> exact_topk(const Fp16Column& column, const uint16_t* query,
                                             uint32_t k, const uint8_t* excluded,
                                             const uint8_t* admitted) {
    TopK top(k);
    if (ann_row_searchable(query, column.dims))
        top.offer(column, nullptr, column.rows, nullptr, query, excluded, admitted);
    return top.take();
}

struct IvfParams {
    uint32_t clusters           = 0;    // 0 = round(sqrt(searchable rows))
    uint32_t iterations         = 8;
    uint32_t sample_per_cluster = 64;   // training sample = min(rows, this * clusters)
    uint32_t threads            = 1;
    uint64_t seed               = 0x5EEDC0DEull;
};

// The trained index: centroids plus the cluster-major order of the searchable rows.
struct IvfModel {
    uint32_t              dims = 0;
    uint32_t              clusters = 0;
    std::vector<uint16_t> centroids;   // clusters * dims fp16, unit length
    std::vector<uint32_t> order;       // searchable row ids, cluster-major, ascending within
    std::vector<uint32_t> offsets;     // clusters + 1; cluster c is order[offsets[c], offsets[c+1])

    uint32_t cluster_rows(uint32_t c) const noexcept { return offsets[c + 1] - offsets[c]; }
};

namespace detail {

// Run `fn(i)` for i in [0, n) on `threads` threads, interleaved. Each i is independent.
template <typename Fn>
inline void parallel_for(uint32_t n, uint32_t threads, Fn&& fn) {
    if (threads <= 1u || n < 1024u) {
        for (uint32_t i = 0; i < n; ++i) fn(i);
        return;
    }
    std::vector<std::thread> pool;
    pool.reserve(threads);
    for (uint32_t t = 0; t < threads; ++t)
        pool.emplace_back([&, t] { for (uint32_t i = t; i < n; i += threads) fn(i); });
    for (auto& th : pool) th.join();
}

// Nearest centroid; ties to the lowest cluster id.
inline uint32_t nearest_centroid(const uint16_t* row, const std::vector<uint16_t>& centroids,
                                 uint32_t clusters, uint32_t dims) noexcept {
    double best = 0.0;
    uint32_t best_c = 0;
    for (uint32_t c = 0; c < clusters; ++c) {
        const double d = ann_cosine_distance(row, centroids.data() + static_cast<size_t>(c) * dims, dims);
        if (c == 0u || d < best) { best = d; best_c = c; }
    }
    return best_c;
}

}  // namespace detail

static inline IvfModel ivf_build(const Fp16Column& column, const uint8_t* excluded,
                                 const IvfParams& params) {
    if (params.threads == 0u) throw std::invalid_argument("ann: threads must be >= 1");
    if (params.iterations == 0u) throw std::invalid_argument("ann: iterations must be >= 1");
    if (params.sample_per_cluster == 0u) throw std::invalid_argument("ann: sample_per_cluster must be >= 1");
    const uint32_t dims = column.dims;

    std::vector<uint32_t> rows;
    rows.reserve(column.rows);
    for (uint32_t r = 0; r < column.rows; ++r) {
        if (!column.valid(r)) continue;
        if (excluded != nullptr && ann_bit(excluded, r)) continue;
        if (!ann_row_searchable(column.row(r), dims)) continue;
        rows.push_back(r);
    }

    IvfModel model;
    model.dims = dims;
    const uint32_t n = static_cast<uint32_t>(rows.size());
    if (n == 0u) {
        model.offsets.assign(1, 0u);
        return model;
    }
    uint32_t k = params.clusters != 0u
        ? params.clusters
        : static_cast<uint32_t>(std::lround(std::sqrt(static_cast<double>(n))));
    k = std::max(1u, std::min(k, n));
    model.clusters = k;

    // Deterministic training sample (Fisher-Yates prefix with a seeded generator).
    std::mt19937_64 rng(params.seed);
    std::vector<uint32_t> sample(rows);
    const uint64_t want = static_cast<uint64_t>(params.sample_per_cluster) * k;
    const uint32_t m = static_cast<uint32_t>(std::min<uint64_t>(n, want));
    for (uint32_t i = 0; i < m; ++i) {
        std::uniform_int_distribution<uint32_t> pick(i, n - 1u);
        std::swap(sample[i], sample[pick(rng)]);
    }
    sample.resize(m);

    // Initial centroids: the first k sampled rows (distinct rows; m >= k).
    std::vector<double> sums(static_cast<size_t>(k) * dims);
    model.centroids.assign(static_cast<size_t>(k) * dims, 0u);
    for (uint32_t c = 0; c < k; ++c)
        std::memcpy(model.centroids.data() + static_cast<size_t>(c) * dims, column.row(sample[c]),
                    dims * sizeof(uint16_t));

    std::vector<uint32_t> assign(m);
    for (uint32_t it = 0; it < params.iterations; ++it) {
        detail::parallel_for(m, params.threads, [&](uint32_t i) {
            assign[i] = detail::nearest_centroid(column.row(sample[i]), model.centroids, k, dims);
        });
        // Mean in sample order — independent of the thread count.
        std::fill(sums.begin(), sums.end(), 0.0);
        std::vector<uint32_t> members(k, 0u);
        for (uint32_t i = 0; i < m; ++i) {
            const uint16_t* r = column.row(sample[i]);
            double* s = sums.data() + static_cast<size_t>(assign[i]) * dims;
            for (uint32_t d = 0; d < dims; ++d) s[d] += fp16_ieee_to_fp32_value(r[d]);
            ++members[assign[i]];
        }
        for (uint32_t c = 0; c < k; ++c) {
            uint16_t* out = model.centroids.data() + static_cast<size_t>(c) * dims;
            const double* s = sums.data() + static_cast<size_t>(c) * dims;
            double norm = 0.0;
            for (uint32_t d = 0; d < dims; ++d) norm += s[d] * s[d];
            norm = std::sqrt(norm);
            if (members[c] == 0u || norm == 0.0 || !std::isfinite(norm)) {
                // Empty (or degenerate) cluster: reseed from a sample row, deterministically.
                const uint32_t pick = sample[(static_cast<uint64_t>(it) * k + c) % m];
                std::memcpy(out, column.row(pick), dims * sizeof(uint16_t));
                continue;
            }
            for (uint32_t d = 0; d < dims; ++d)
                out[d] = fp16_ieee_from_fp32_value(static_cast<float>(s[d] / norm));
        }
    }

    // Final assignment of every searchable row, then a counting sort into cluster order.
    std::vector<uint32_t> cluster_of(n);
    detail::parallel_for(n, params.threads, [&](uint32_t i) {
        cluster_of[i] = detail::nearest_centroid(column.row(rows[i]), model.centroids, k, dims);
    });
    model.offsets.assign(static_cast<size_t>(k) + 1u, 0u);
    for (uint32_t i = 0; i < n; ++i) ++model.offsets[cluster_of[i] + 1u];
    for (uint32_t c = 0; c < k; ++c) model.offsets[c + 1u] += model.offsets[c];
    model.order.resize(n);
    std::vector<uint32_t> cursor(model.offsets.begin(), model.offsets.end() - 1);
    for (uint32_t i = 0; i < n; ++i) model.order[cursor[cluster_of[i]]++] = rows[i];
    return model;
}

// The `nprobe` nearest NON-EMPTY clusters to `query`, nearest first, ties by cluster id.
// `counts` (nullable) gives rows per cluster; an empty cluster is never probed.
static inline std::vector<uint32_t> ivf_probe(const uint16_t* centroids, uint32_t clusters,
                                              uint32_t dims, const uint32_t* counts,
                                              const uint16_t* query, uint32_t nprobe) {
    std::vector<std::pair<double, uint32_t>> ranked;
    ranked.reserve(clusters);
    if (!ann_row_searchable(query, dims)) return {};
    for (uint32_t c = 0; c < clusters; ++c) {
        if (counts != nullptr && counts[c] == 0u) continue;
        ranked.emplace_back(ann_cosine_distance(query, centroids + static_cast<size_t>(c) * dims, dims), c);
    }
    const size_t take = std::min<size_t>(nprobe, ranked.size());
    std::partial_sort(ranked.begin(), ranked.begin() + take, ranked.end());
    std::vector<uint32_t> out(take);
    for (size_t i = 0; i < take; ++i) out[i] = ranked[i].second;
    return out;
}

// Search an in-memory model over the column it was built from (rows addressed through
// model.order). The engine instead scores each probed cluster's row group as it arrives,
// with the same TopK::offer — this is the reference the engine path must equal.
static inline std::vector<AnnHit> ivf_search(const IvfModel& model, const Fp16Column& column,
                                             const uint16_t* query, uint32_t k, uint32_t nprobe,
                                             const uint8_t* excluded, const uint8_t* admitted) {
    if (model.dims != column.dims)
        throw std::invalid_argument("ann: model dimension does not match the vectors");
    TopK top(k);
    if (model.clusters == 0u || !ann_row_searchable(query, column.dims)) return top.take();
    std::vector<uint32_t> counts(model.clusters);
    for (uint32_t c = 0; c < model.clusters; ++c) counts[c] = model.cluster_rows(c);
    for (uint32_t c : ivf_probe(model.centroids.data(), model.clusters, model.dims, counts.data(),
                                query, nprobe))
        top.offer(column, model.order.data() + model.offsets[c], model.cluster_rows(c), nullptr,
                  query, excluded, admitted);
    return top.take();
}

}}  // namespace draken::ann
