#pragma once
// draken/ops/ann/fp16_cosine_hnsw.h — approximate nearest-neighbour search over one
// VECTOR_FP16 column (docs/VECTOR_INDEX_DESIGN.md §5.2).
//
// The graph is usearch's core `index_gt` (header-only, third_party/usearch): it stores
// ONLY the HNSW adjacency. Vectors stay where they already are — the caller's
// VECTOR_FP16 column (in the index: the skene vectors file) — and the metric reads them
// through the uniform data[selection[i]] access. Nothing here owns a second copy.
//
// The metric IS the engine's cosine (ops/vector_cosine_row.h), as
//     distance = 1 - clip(cosine, -1, 1)
// exactly as COSINE_DISTANCE defines it, in double. So every distance this returns is the
// value the SQL kernel would compute for that row — no separate re-rank is needed.
//
// Keys are row ordinals of the column (row i of the vectors file == row i of the data
// file). A row is NOT in the graph when it is excluded by the caller (deleted), null, or
// zero-magnitude/non-finite (its cosine is NaN, which has no place in a distance order).
// Such rows are unreachable through the graph by construction; the exact scan below gives
// them the same treatment so the two paths agree on what is searchable.
//
// Serialized form (the `.graph` object):
//     GraphHeader (fixed, little-endian) | binding bytes | usearch index_gt stream
// The header binds the graph to its column (row count, dimension) and to an opaque
// caller-supplied binding (data file / vectors file / embedding identity). The body is
// checksummed. Any mismatch on open throws — a graph is never used against the wrong data.
//
// Errors are thrown (std::invalid_argument / std::runtime_error) with the reason; the
// engine boundary converts them. No Python, no GIL, no opteryx dependency.

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "core/buffers.h"
#include "ops/vector_cosine_row.h"
#include "usearch/index.hpp"
#include "xxhash.h"

namespace draken { namespace ann {

using hnsw_index_t = unum::usearch::index_gt<double, uint32_t, uint32_t>;
using hnsw_member_cref_t = hnsw_index_t::member_cref_t;

static constexpr char kGraphMagic[8] = {'D', 'K', 'A', 'N', 'N', 'G', 'R', '1'};
static constexpr uint32_t kGraphFormatVersion = 1;
static constexpr uint32_t kMetricCosine = 1;

#pragma pack(push, 1)
struct GraphHeader {
    char     magic[8];
    uint32_t format_version;
    uint32_t metric;
    uint32_t dimension;
    uint32_t connectivity;
    uint32_t expansion_add;
    uint32_t row_count;       // rows in the column the graph was built over
    uint32_t indexed_count;   // rows actually in the graph
    uint32_t binding_length;  // bytes of caller binding that follow the header
    uint64_t body_length;     // bytes of usearch stream after the binding
    uint64_t body_xxh3;       // XXH3_64bits of binding + body
};
#pragma pack(pop)
static_assert(sizeof(GraphHeader) == 56, "GraphHeader layout is part of the file format");

struct HnswBuildParams {
    uint32_t connectivity  = 16;   // M
    uint32_t expansion_add = 128;  // efConstruction
    uint32_t threads       = 1;
};

static inline bool ann_bit(const uint8_t* bits, uint32_t i) noexcept {
    return (bits[i >> 3] >> (i & 7u)) & 1u;
}

// fp16 row has a defined cosine against any finite non-zero vector: finite elements and at
// least one non-zero. Exact: a non-zero fp16 squared is >= 2^-48, never zero in fp64.
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

// Read-only view of the column the graph indexes: row r is data[selection[r] * dims].
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
    // In the searchable domain: valid, not excluded, finite and non-zero.
    bool searchable(uint32_t r, const uint8_t* excluded) const noexcept {
        if (validity != nullptr && !ann_bit(validity, r)) return false;
        if (excluded != nullptr && ann_bit(excluded, r)) return false;
        return ann_row_searchable(row(r), dims);
    }
};

// Metric over graph members: slot -> row ordinal -> vector. The slot map is filled under
// the node lock (add callback) before the node can be reached, as usearch's own dense
// index does.
struct SlotMetric {
    const Fp16Column* column;
    const uint32_t*   slot_rows;

    // get_slot is called unqualified: for usearch's member iterators it is a hidden
    // friend, reachable only through argument-dependent lookup.
    template <typename member_at>
    double operator()(const uint16_t* query, member_at const& m) const noexcept {
        using unum::usearch::get_slot;
        return ann_cosine_distance(query, column->row(slot_rows[get_slot(m)]), column->dims);
    }
    template <typename member_a, typename member_b>
    double operator()(member_a const& a, member_b const& b) const noexcept {
        using unum::usearch::get_slot;
        return ann_cosine_distance(column->row(slot_rows[get_slot(a)]),
                                   column->row(slot_rows[get_slot(b)]), column->dims);
    }
};

// Build the serialized graph for `column`. `excluded` (nullable) is a 1-bit-per-row mask
// of rows to leave out (deleted rows). `binding` is stored verbatim and returned on open.
static inline std::vector<uint8_t> hnsw_build(const Fp16Column& column, const uint8_t* excluded,
                                              const HnswBuildParams& params,
                                              const std::string& binding) {
    if (params.connectivity < 2u)
        throw std::invalid_argument("ann: connectivity must be >= 2");
    if (params.threads == 0u)
        throw std::invalid_argument("ann: threads must be >= 1");

    std::vector<uint32_t> rows;
    rows.reserve(column.rows);
    for (uint32_t r = 0; r < column.rows; ++r)
        if (column.searchable(r, excluded)) rows.push_back(r);

    hnsw_index_t index(unum::usearch::index_config_t(params.connectivity));
    if (!index.try_reserve(unum::usearch::index_limits_t(rows.size(), params.threads)))
        throw std::runtime_error("ann: unable to reserve the graph");

    std::vector<uint32_t> slot_rows(rows.size() > 0 ? rows.size() : 1u, 0u);
    SlotMetric metric{&column, slot_rows.data()};

    std::atomic<const char*> failure{nullptr};
    std::atomic<size_t> next{0};
    auto worker = [&](size_t thread) {
        unum::usearch::index_update_config_t config;
        config.expansion = params.expansion_add;
        config.thread = thread;
        for (size_t i = next.fetch_add(1); i < rows.size(); i = next.fetch_add(1)) {
            if (failure.load(std::memory_order_relaxed) != nullptr) return;
            const uint32_t row = rows[i];
            auto result = index.add(row, column.row(row), metric, config,
                                    [&](hnsw_index_t::member_ref_t m) noexcept {
                                        slot_rows[unum::usearch::get_slot(m)] = row;
                                    });
            if (!result) {
                const char* msg = result.error.release();
                const char* none = nullptr;
                failure.compare_exchange_strong(none, msg);
                return;
            }
        }
    };
    if (params.threads == 1u) {
        worker(0);
    } else {
        std::vector<std::thread> pool;
        pool.reserve(params.threads);
        for (uint32_t t = 0; t < params.threads; ++t) pool.emplace_back(worker, t);
        for (auto& th : pool) th.join();
    }
    if (const char* msg = failure.load()) throw std::runtime_error(std::string("ann: build failed: ") + msg);

    std::vector<uint8_t> body;
    auto saved = index.save_to_stream([&](const void* buf, size_t len) {
        const auto* p = static_cast<const uint8_t*>(buf);
        body.insert(body.end(), p, p + len);
        return true;
    });
    if (!saved) throw std::runtime_error(std::string("ann: serialize failed: ") + saved.error.release());

    GraphHeader h{};
    std::memcpy(h.magic, kGraphMagic, sizeof(kGraphMagic));
    h.format_version = kGraphFormatVersion;
    h.metric         = kMetricCosine;
    h.dimension      = column.dims;
    h.connectivity   = params.connectivity;
    h.expansion_add  = params.expansion_add;
    h.row_count      = column.rows;
    h.indexed_count  = static_cast<uint32_t>(rows.size());
    h.binding_length = static_cast<uint32_t>(binding.size());
    h.body_length    = body.size();

    std::vector<uint8_t> out(sizeof(GraphHeader) + binding.size() + body.size());
    std::memcpy(out.data() + sizeof(GraphHeader), binding.data(), binding.size());
    if (!body.empty())
        std::memcpy(out.data() + sizeof(GraphHeader) + binding.size(), body.data(), body.size());
    h.body_xxh3 = XXH3_64bits(out.data() + sizeof(GraphHeader), binding.size() + body.size());
    std::memcpy(out.data(), &h, sizeof(GraphHeader));
    return out;
}

struct AnnHit {
    uint32_t row;
    double   distance;
};

// An opened graph bound to its column. Searches are concurrent across distinct `thread`
// ids in [0, threads).
class HnswSearcher {
  public:
    HnswSearcher(const uint8_t* bytes, size_t length, const Fp16Column& column, uint32_t threads)
        : column_(column) {
        if (threads == 0u) throw std::invalid_argument("ann: threads must be >= 1");
        if (length < sizeof(GraphHeader)) throw std::runtime_error("ann: graph is truncated");
        std::memcpy(&header_, bytes, sizeof(GraphHeader));
        if (std::memcmp(header_.magic, kGraphMagic, sizeof(kGraphMagic)) != 0)
            throw std::runtime_error("ann: not a graph (bad magic)");
        if (header_.format_version != kGraphFormatVersion)
            throw std::runtime_error("ann: unsupported graph format version");
        if (header_.metric != kMetricCosine)
            throw std::runtime_error("ann: unsupported graph metric");
        const uint64_t tail = static_cast<uint64_t>(header_.binding_length) + header_.body_length;
        if (length != sizeof(GraphHeader) + tail)
            throw std::runtime_error("ann: graph length does not match its header");
        const uint8_t* payload = bytes + sizeof(GraphHeader);
        if (XXH3_64bits(payload, static_cast<size_t>(tail)) != header_.body_xxh3)
            throw std::runtime_error("ann: graph checksum mismatch");
        if (header_.dimension != column.dims)
            throw std::runtime_error("ann: graph dimension does not match the vectors");
        if (header_.row_count != column.rows)
            throw std::runtime_error("ann: graph row count does not match the vectors");
        binding_.assign(reinterpret_cast<const char*>(payload), header_.binding_length);

        const uint8_t* body = payload + header_.binding_length;
        size_t offset = 0;
        auto loaded = index_.load_from_stream([&](void* buf, size_t len) {
            if (offset + len > header_.body_length) return false;
            std::memcpy(buf, body + offset, len);
            offset += len;
            return true;
        });
        if (!loaded) throw std::runtime_error(std::string("ann: graph load failed: ") + loaded.error.release());
        if (offset != header_.body_length || index_.size() != header_.indexed_count)
            throw std::runtime_error("ann: graph body is inconsistent with its header");
        if (!index_.try_reserve(unum::usearch::index_limits_t(index_.size(), threads)))
            throw std::runtime_error("ann: unable to reserve search contexts");

        slot_rows_.resize(index_.size() > 0 ? index_.size() : 1u);
        for (size_t slot = 0; slot < index_.size(); ++slot) {
            const uint32_t row = unum::usearch::get_key(index_.at(static_cast<uint32_t>(slot)));
            if (row >= column.rows)
                throw std::runtime_error("ann: graph references a row past the vectors");
            slot_rows_[slot] = row;
        }
    }

    const std::string& binding() const noexcept { return binding_; }
    const GraphHeader& header() const noexcept { return header_; }

    // Up to `k` nearest admitted rows to `query` (dims fp16 values), nearest first, ties by
    // row. `admitted` (nullable): 1-bit-per-row mask; a row whose bit is clear is never
    // returned and is filtered DURING traversal.
    std::vector<AnnHit> search(const uint16_t* query, uint32_t k, uint32_t expansion,
                               const uint8_t* admitted, uint32_t thread) const {
        std::vector<AnnHit> hits;
        if (k == 0u || index_.size() == 0u || !ann_row_searchable(query, column_.dims)) return hits;
        SlotMetric metric{&column_, slot_rows_.data()};
        unum::usearch::index_search_config_t config;
        config.expansion = expansion > k ? expansion : k;
        config.thread = thread;
        auto run = [&](auto&& predicate) {
            auto result = index_.search(query, k, metric, config, predicate);
            if (!result) throw std::runtime_error(std::string("ann: search failed: ") + result.error.release());
            hits.reserve(result.size());
            for (size_t i = 0; i < result.size(); ++i) {
                auto match = result[i];
                hits.push_back(AnnHit{unum::usearch::get_key(match.member), match.distance});
            }
        };
        if (admitted == nullptr) {
            run(unum::usearch::dummy_predicate_t{});
        } else {
            run([&](hnsw_member_cref_t const& m) noexcept {
                return ann_bit(admitted, unum::usearch::get_key(m));
            });
        }
        std::sort(hits.begin(), hits.end(), [](const AnnHit& a, const AnnHit& b) {
            return a.distance < b.distance || (a.distance == b.distance && a.row < b.row);
        });
        return hits;
    }

  private:
    Fp16Column             column_;
    GraphHeader            header_{};
    std::string            binding_;
    hnsw_index_t           index_;
    std::vector<uint32_t>  slot_rows_;
};

// The exact path: scan every searchable, admitted row. Same domain and same ordering as
// the graph, so it is the recall reference and the small-file / selective-filter path.
static inline std::vector<AnnHit> exact_topk(const Fp16Column& column, const uint16_t* query,
                                             uint32_t k, const uint8_t* excluded,
                                             const uint8_t* admitted) {
    std::vector<AnnHit> hits;
    if (k == 0u || !ann_row_searchable(query, column.dims)) return hits;
    auto worse = [](const AnnHit& a, const AnnHit& b) {
        return a.distance < b.distance || (a.distance == b.distance && a.row < b.row);
    };
    hits.reserve(k + 1u);
    for (uint32_t r = 0; r < column.rows; ++r) {
        if (admitted != nullptr && !ann_bit(admitted, r)) continue;
        if (column.validity != nullptr && !ann_bit(column.validity, r)) continue;
        if (excluded != nullptr && ann_bit(excluded, r)) continue;
        // A NaN distance is exactly "not searchable" (zero-magnitude or non-finite row,
        // against a searchable query), so one pass decides both.
        AnnHit h{r, ann_cosine_distance(query, column.row(r), column.dims)};
        if (std::isnan(h.distance)) continue;
        if (hits.size() < k) {
            hits.push_back(h);
            std::push_heap(hits.begin(), hits.end(), worse);
        } else if (worse(h, hits.front())) {
            std::pop_heap(hits.begin(), hits.end(), worse);
            hits.back() = h;
            std::push_heap(hits.begin(), hits.end(), worse);
        }
    }
    std::sort(hits.begin(), hits.end(), worse);
    return hits;
}

}}  // namespace draken::ann
