#pragma once
// draken/ops/ann/fp16_cosine_ivf.h — approximate nearest-neighbour search over one
// VECTOR_FP16 column by inverted file (IVF-flat), plus the exact scan
// (docs/VECTOR_INDEX_DESIGN.md §5.2, D-5 ruled 2026-10-02: IVF, because a query reads only
// the probed clusters — on object storage the bytes read decide the cost).
//
// Build: spherical k-means, in three steps that a build too large for memory runs apart:
//   ivf_plan   — choose K and a deterministic sample from the CANDIDATE rows (valid, not
//                deleted), before any row is embedded;
//   ivf_train  — k-means over the embedded sample, in plan order;
//   ivf_assign — the nearest centroid of each row, as it arrives.
// ClusterStream holds assigned rows per cluster and hands back a block every `flush_rows`,
// so a file's vectors never need to be resident at once; a cluster may therefore span
// several row groups of the vectors file. ivf_build is the in-memory composition of the
// same steps (cluster-major ORDER, rows ascending within a cluster, per-cluster OFFSETS) —
// the reference the streaming build must agree with.
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

// K and the training sample, chosen from candidate row ids before any of them is embedded.
struct IvfSamplePlan {
    uint32_t              clusters = 0;   // 0 = no candidates
    std::vector<uint32_t> sample;         // candidate ids, in TRAINING order
};

// Trained centroids.
struct IvfCentroids {
    uint32_t              dims = 0;
    uint32_t              clusters = 0;
    std::vector<uint16_t> centroids;      // clusters * dims fp16, unit length
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

static inline void ivf_check_params(const IvfParams& params) {
    if (params.threads == 0u) throw std::invalid_argument("ann: threads must be >= 1");
    if (params.iterations == 0u) throw std::invalid_argument("ann: iterations must be >= 1");
    if (params.sample_per_cluster == 0u) throw std::invalid_argument("ann: sample_per_cluster must be >= 1");
}

// K = params.clusters, or round(sqrt(n)) when 0, clamped to [1, n]; the sample is the
// first min(n, sample_per_cluster * K) of a seeded Fisher-Yates shuffle of `candidates`.
static inline IvfSamplePlan ivf_plan(const std::vector<uint32_t>& candidates, const IvfParams& params) {
    ivf_check_params(params);
    IvfSamplePlan plan;
    const uint32_t n = static_cast<uint32_t>(candidates.size());
    if (n == 0u) return plan;
    uint32_t k = params.clusters != 0u
        ? params.clusters
        : static_cast<uint32_t>(std::lround(std::sqrt(static_cast<double>(n))));
    plan.clusters = std::max(1u, std::min(k, n));

    std::mt19937_64 rng(params.seed);
    plan.sample = candidates;
    const uint64_t want = static_cast<uint64_t>(params.sample_per_cluster) * plan.clusters;
    const uint32_t m = static_cast<uint32_t>(std::min<uint64_t>(n, want));
    for (uint32_t i = 0; i < m; ++i) {
        std::uniform_int_distribution<uint32_t> pick(i, n - 1u);
        std::swap(plan.sample[i], plan.sample[pick(rng)]);
    }
    plan.sample.resize(m);
    return plan;
}

// k-means over `sample` (row i = the plan's sample[i], embedded). Rows that turn out not to
// be searchable (null, zero-magnitude, non-finite — known only after embedding) are skipped;
// K is clamped to the searchable rows that remain. Initial centroids are the first K of them.
static inline IvfCentroids ivf_train(const Fp16Column& sample, uint32_t clusters, const IvfParams& params) {
    ivf_check_params(params);
    const uint32_t dims = sample.dims;
    std::vector<uint32_t> use;
    use.reserve(sample.rows);
    for (uint32_t i = 0; i < sample.rows; ++i)
        if (sample.valid(i) && ann_row_searchable(sample.row(i), dims)) use.push_back(i);

    IvfCentroids out;
    out.dims = dims;
    const uint32_t m = static_cast<uint32_t>(use.size());
    const uint32_t k = std::min(clusters, m);
    if (k == 0u) return out;
    out.clusters = k;

    std::vector<double> sums(static_cast<size_t>(k) * dims);
    out.centroids.assign(static_cast<size_t>(k) * dims, 0u);
    for (uint32_t c = 0; c < k; ++c)
        std::memcpy(out.centroids.data() + static_cast<size_t>(c) * dims, sample.row(use[c]),
                    dims * sizeof(uint16_t));

    std::vector<uint32_t> assign(m);
    for (uint32_t it = 0; it < params.iterations; ++it) {
        detail::parallel_for(m, params.threads, [&](uint32_t i) {
            assign[i] = detail::nearest_centroid(sample.row(use[i]), out.centroids, k, dims);
        });
        // Mean in sample order — independent of the thread count.
        std::fill(sums.begin(), sums.end(), 0.0);
        std::vector<uint32_t> members(k, 0u);
        for (uint32_t i = 0; i < m; ++i) {
            const uint16_t* r = sample.row(use[i]);
            double* s = sums.data() + static_cast<size_t>(assign[i]) * dims;
            for (uint32_t d = 0; d < dims; ++d) s[d] += fp16_ieee_to_fp32_value(r[d]);
            ++members[assign[i]];
        }
        for (uint32_t c = 0; c < k; ++c) {
            uint16_t* dst = out.centroids.data() + static_cast<size_t>(c) * dims;
            const double* s = sums.data() + static_cast<size_t>(c) * dims;
            double norm = 0.0;
            for (uint32_t d = 0; d < dims; ++d) norm += s[d] * s[d];
            norm = std::sqrt(norm);
            if (members[c] == 0u || norm == 0.0 || !std::isfinite(norm)) {
                // Empty (or degenerate) cluster: reseed from a sample row, deterministically.
                const uint32_t pick = use[(static_cast<uint64_t>(it) * k + c) % m];
                std::memcpy(dst, sample.row(pick), dims * sizeof(uint16_t));
                continue;
            }
            for (uint32_t d = 0; d < dims; ++d)
                dst[d] = fp16_ieee_from_fp32_value(static_cast<float>(s[d] / norm));
        }
    }
    return out;
}

// The cluster a searchable row belongs to: the nearest centroid, ties to the lowest id.
static inline uint32_t ivf_assign(const IvfCentroids& model, const uint16_t* row) noexcept {
    return detail::nearest_centroid(row, model.centroids, model.clusters, model.dims);
}

// Assigned rows held per cluster until `flush_rows` of one cluster are waiting; that block
// (ordinals + vectors, in arrival order) is handed to `emit(cluster, ordinals, vectors, rows)`
// and released. finish() emits every remainder, in cluster order. Peak memory is at most
// clusters * flush_rows rows, whatever the file's size — the caller sizes flush_rows from
// its memory budget. Emitted blocks become the vectors file's row groups, so one cluster may
// be several row groups, interleaved with other clusters'.
class ClusterStream {
  public:
    ClusterStream(uint32_t clusters, uint32_t dims, uint32_t flush_rows)
        : dims_(dims), flush_rows_(flush_rows), ordinals_(clusters), vectors_(clusters) {
        if (clusters == 0u) throw std::invalid_argument("ann: a cluster stream needs clusters");
        if (dims == 0u) throw std::invalid_argument("ann: dimension must be >= 1");
        if (flush_rows == 0u) throw std::invalid_argument("ann: flush_rows must be >= 1");
    }

    template <typename Emit>
    void add(uint32_t cluster, uint32_t ordinal, const uint16_t* row, Emit&& emit) {
        if (cluster >= ordinals_.size()) throw std::invalid_argument("ann: cluster out of range");
        ordinals_[cluster].push_back(ordinal);
        vectors_[cluster].insert(vectors_[cluster].end(), row, row + dims_);
        if (ordinals_[cluster].size() >= flush_rows_) flush(cluster, emit);
    }

    template <typename Emit>
    void finish(Emit&& emit) {
        for (uint32_t c = 0; c < ordinals_.size(); ++c)
            if (!ordinals_[c].empty()) flush(c, emit);
    }

  private:
    template <typename Emit>
    void flush(uint32_t c, Emit& emit) {
        emit(c, ordinals_[c].data(), vectors_[c].data(), static_cast<uint32_t>(ordinals_[c].size()));
        std::vector<uint32_t>().swap(ordinals_[c]);
        std::vector<uint16_t>().swap(vectors_[c]);
    }

    uint32_t                           dims_;
    uint32_t                           flush_rows_;
    std::vector<std::vector<uint32_t>> ordinals_;
    std::vector<std::vector<uint16_t>> vectors_;
};

// The in-memory build: plan + train + assign + a counting sort into cluster order.
static inline IvfModel ivf_build(const Fp16Column& column, const uint8_t* excluded,
                                 const IvfParams& params) {
    ivf_check_params(params);
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
    const IvfSamplePlan plan = ivf_plan(rows, params);
    if (plan.clusters == 0u) {
        model.offsets.assign(1, 0u);
        return model;
    }
    // The sample as a column of its own: row i is column row plan.sample[i]. Zero-copy.
    std::vector<uint32_t> sample_selection(plan.sample.size());
    for (size_t i = 0; i < plan.sample.size(); ++i)
        sample_selection[i] = column.selection[plan.sample[i]];
    const Fp16Column sample{column.data, sample_selection.data(), nullptr,
                            static_cast<uint32_t>(plan.sample.size()), dims};
    const IvfCentroids trained = ivf_train(sample, plan.clusters, params);
    const uint32_t k = trained.clusters;
    model.clusters = k;
    model.centroids = trained.centroids;

    const uint32_t n = static_cast<uint32_t>(rows.size());
    std::vector<uint32_t> cluster_of(n);
    detail::parallel_for(n, params.threads, [&](uint32_t i) {
        cluster_of[i] = ivf_assign(trained, column.row(rows[i]));
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
