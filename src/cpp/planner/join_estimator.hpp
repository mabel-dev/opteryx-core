// Join cardinality estimation and join-order enumeration (DPccp + greedy).
//
// The native half of the planner's cost model for joins. No Python: the Cython
// wrapper (opteryx/compiled/planner/join_estimator.pyx) converts the planner's
// objects into these structs ONCE per call and every loop below runs on them.
//
// BIT-IDENTITY CONTRACT. This replaced a Python implementation whose estimates
// are compared value-for-value against the planner's telemetry, so every float
// expression here keeps the Python operation ORDER (IEEE results then match:
// same operands, same order, same libm `pow`). Two things Python did that C++
// cannot do the same way, both ruled by the architect (2026-09-24):
//
//   * Python ints do not overflow. A row count or truncated float estimate at
//     or above 2^63 is CAPPED at INT64_MAX (`cap_i64`) rather than raising.
//   * The occupancy bound multiplies per-class NDVs into a composite key
//     domain that routinely exceeds 2^64 (three JOB classes of 36M each). That
//     product and its `isqrt` are computed EXACTLY in 128 bits, saturating
//     only past 2^127 — far beyond anything a real key reaches.
//
// Unknown values carry an explicit `has_*` flag rather than a sentinel: an NDV
// of 0 or -1 is a (nonsensical but representable) value the Python accepted,
// and a sentinel would have silently re-read it as "unknown".

#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace opteryx { namespace planner {

// The flat equality selectivity used when either side of a key has no NDV.
// SINGLE SOURCE: fallback_selectivity.py re-exports this value through the
// Cython module rather than declaring its own copy.
constexpr double kEqUnknownNdvFallback = 0.1;

// DPccp refuses graphs above this many vertices (the greedy enumerator covers
// them). Vertex sets are uint64_t bitsets, so no graph may exceed 64.
constexpr int kMaxDpccpVertices = 30;
constexpr int kMaxGraphVertices = 64;

enum NdvProvenance : uint8_t {
    NDV_UNKNOWN = 0,
    NDV_MEASURED = 1,
    NDV_DOMAIN_STANDIN = 2,
};

// One side of one equi-key class. `ndv` is the key's DOMAIN size; `live_ndv`
// the distinct values present after filters (only semi/anti read it).
struct KeyStats {
    int64_t ndv;
    int64_t live_ndv;
    double null_fraction;
    bool has_ndv;
    bool has_live_ndv;
    bool has_null_fraction;
    uint8_t provenance;
};

struct KeyPair {
    KeyStats left;
    KeyStats right;
};

enum JoinType : uint8_t {
    JT_INNER = 0,
    JT_LEFT_OUTER,
    JT_RIGHT_OUTER,
    JT_FULL_OUTER,
    JT_CROSS,
    JT_SEMI,
    JT_ANTI,
    JT_SEMI_NOT_DISTINCT,
    JT_ANTI_NOT_DISTINCT,
    JT_ANTI_NULL_AWARE,
};

// ---- integer helpers --------------------------------------------------------

// Python `int(x)` for a finite, in-range double; INT64_MAX at or above 2^63.
inline int64_t cap_i64(double x) {
    // 9223372036854775808.0 == 2^63, exactly representable; INT64_MAX is not.
    if (!(x < 9223372036854775808.0)) {
        return std::numeric_limits<int64_t>::max();
    }
    return static_cast<int64_t>(x);
}

using i128 = __int128;
using u128 = unsigned __int128;

constexpr i128 kI128Max = static_cast<i128>(~static_cast<u128>(0) >> 1);
constexpr i128 kI128Min = -kI128Max - 1;

inline i128 sat_mul_i128(i128 a, i128 b) {
    i128 out;
    if (__builtin_mul_overflow(a, b, &out)) {
        return ((a < 0) != (b < 0)) ? kI128Min : kI128Max;
    }
    return out;
}

inline int bit_length_u128(u128 n) {
    const uint64_t hi = static_cast<uint64_t>(n >> 64);
    if (hi) {
        return 128 - __builtin_clzll(hi);
    }
    const uint64_t lo = static_cast<uint64_t>(n);
    return lo ? 64 - __builtin_clzll(lo) : 0;
}

// floor(sqrt(n)) exactly — Python's math.isqrt. Newton's method from a start
// at or above the root descends monotonically onto the floor.
inline u128 isqrt_u128(u128 n) {
    if (n < 2) {
        return n;
    }
    u128 x = static_cast<u128>(1) << ((bit_length_u128(n) + 1) / 2);
    while (true) {
        const u128 y = (x + n / x) >> 1;
        if (y >= x) {
            return x;
        }
        x = y;
    }
}

inline int64_t cap_i64_from_i128(i128 v) {
    constexpr i128 max64 = static_cast<i128>(std::numeric_limits<int64_t>::max());
    return v > max64 ? std::numeric_limits<int64_t>::max() : static_cast<int64_t>(v);
}

// ---- scalar estimators ------------------------------------------------------

inline double key_selectivity(const KeyStats& left, const KeyStats& right) {
    if (!left.has_ndv || !right.has_ndv) {
        return kEqUnknownNdvFallback;
    }
    const int64_t denom = std::max(left.ndv, right.ndv);
    if (denom <= 0) {
        return kEqUnknownNdvFallback;
    }
    return 1.0 / static_cast<double>(denom);
}

// Reduce a row count by the worst-case (max) null fraction across one side's
// keys. `side` 0 reads KeyPair.left, 1 reads KeyPair.right.
inline double effective_rows(int64_t rows, const KeyPair* keys, size_t n, int side) {
    if (n == 0) {
        return static_cast<double>(rows);
    }
    double worst_null = 0.0;
    bool first = true;
    for (size_t i = 0; i < n; ++i) {
        const KeyStats& k = side == 0 ? keys[i].left : keys[i].right;
        const double nf = k.has_null_fraction ? k.null_fraction : 0.0;
        if (first || nf > worst_null) {
            worst_null = nf;
            first = false;
        }
    }
    if (worst_null <= 0.0) {
        return static_cast<double>(rows);
    }
    return static_cast<double>(rows) * (1.0 - worst_null);
}

inline double inner_estimate(int64_t left_rows, int64_t right_rows, const KeyPair* keys,
                             size_t n, double extra_selectivity) {
    if (n == 0) {
        return static_cast<double>(left_rows) * static_cast<double>(right_rows) *
               extra_selectivity;
    }
    const double eff_left = effective_rows(left_rows, keys, n, 0);
    const double eff_right = effective_rows(right_rows, keys, n, 1);
    double selectivity = 1.0;
    for (size_t i = 0; i < n; ++i) {
        selectivity *= key_selectivity(keys[i].left, keys[i].right);
    }
    return eff_left * eff_right * selectivity * extra_selectivity;
}

// Fraction of left rows whose key is present in the right relation; false when
// it cannot be estimated (the caller must then decline, not invent one).
inline bool match_fraction(const KeyPair* keys, size_t n, double* out) {
    if (n == 0) {
        return false;
    }
    double fraction = 1.0;
    for (size_t i = 0; i < n; ++i) {
        if (!keys[i].right.has_live_ndv) {
            return false;
        }
        // Python: max(left.ndv or 0, right.ndv or 0)
        const int64_t l = keys[i].left.has_ndv ? keys[i].left.ndv : 0;
        const int64_t r = keys[i].right.has_ndv ? keys[i].right.ndv : 0;
        const int64_t domain = std::max(l, r);
        if (domain <= 0) {
            return false;
        }
        fraction *= std::min(1.0, static_cast<double>(keys[i].right.live_ndv) /
                                      static_cast<double>(domain));
    }
    *out = fraction;
    return true;
}

// Callers validate arguments first (the Cython entry point raises ValueError
// on negative rows / selectivity); the enumerators call inner_estimate directly.
inline int64_t estimate_join_cardinality(int64_t left_rows, int64_t right_rows, JoinType join_type,
                                         const KeyPair* keys, size_t n,
                                         double extra_selectivity) {
    if (join_type == JT_CROSS) {
        const double result = static_cast<double>(left_rows) * static_cast<double>(right_rows) *
                              extra_selectivity;
        return std::max<int64_t>(1, cap_i64(result));
    }
    const double inner = inner_estimate(left_rows, right_rows, keys, n, extra_selectivity);
    double result;
    switch (join_type) {
        case JT_INNER:
            result = inner;
            break;
        case JT_LEFT_OUTER:
            result = std::max(inner, static_cast<double>(left_rows));
            break;
        case JT_RIGHT_OUTER:
            result = std::max(inner, static_cast<double>(right_rows));
            break;
        case JT_FULL_OUTER: {
            const double left_unmatched = std::max(0.0, static_cast<double>(left_rows) - inner);
            const double right_unmatched = std::max(0.0, static_cast<double>(right_rows) - inner);
            result = inner + left_unmatched + right_unmatched;
            break;
        }
        default: {
            // semi / anti family: a semi join emits LEFT ROWS, not matched pairs.
            double fraction;
            if (!match_fraction(keys, n, &fraction)) {
                return std::max<int64_t>(1, left_rows);
            }
            const bool not_distinct =
                join_type == JT_SEMI_NOT_DISTINCT || join_type == JT_ANTI_NOT_DISTINCT;
            double rows = static_cast<double>(left_rows);
            if (!not_distinct) {
                rows = effective_rows(left_rows, keys, n, 0);
            }
            // Truncated ONCE so semi + anti == left_rows exactly.
            const double semi = static_cast<double>(cap_i64(rows * fraction));
            if (join_type == JT_SEMI || join_type == JT_SEMI_NOT_DISTINCT) {
                result = semi;
            } else if (join_type == JT_ANTI_NULL_AWARE) {
                bool any_null = false;
                double worst = 0.0;
                for (size_t i = 0; i < n; ++i) {
                    if (keys[i].right.has_null_fraction) {
                        if (!any_null || keys[i].right.null_fraction > worst) {
                            worst = keys[i].right.null_fraction;
                        }
                        any_null = true;
                    }
                }
                result = (any_null && worst > 0.0) ? 0.0 : static_cast<double>(left_rows) - semi;
            } else {
                result = static_cast<double>(left_rows) - semi;
            }
            result = std::max(0.0, result);
            break;
        }
    }
    return std::max<int64_t>(1, cap_i64(result));
}

// Bound a COMPOSITE key's domain by the rows available to hold it. Returns
// false (and leaves `keys` alone) when unchanged; true when the class list was
// collapsed to the single pair written to `*collapsed`.
//
// Throws std::domain_error where Python's isqrt raised ValueError (a negative
// NDV product) — reachable only from a caller passing negative NDVs.
inline bool apply_occupancy_bound(const KeyPair* keys, size_t n, int64_t left_domain_rows,
                                  int64_t right_domain_rows, KeyPair* collapsed) {
    if (n < 2) {
        return false;
    }
    i128 composite = 1;
    bool any_measured = false;
    for (size_t i = 0; i < n; ++i) {
        const KeyStats& l = keys[i].left;
        const KeyStats& r = keys[i].right;
        if (!l.has_ndv || !r.has_ndv) {
            return false;
        }
        const int64_t factor = std::max(l.ndv, r.ndv);
        if ((l.ndv == factor && l.provenance == NDV_MEASURED) ||
            (r.ndv == factor && r.provenance == NDV_MEASURED)) {
            any_measured = true;
        }
        composite = sat_mul_i128(composite, static_cast<i128>(factor));
    }
    i128 bound = std::max<int64_t>(1, std::min(left_domain_rows, right_domain_rows));
    if (!any_measured) {
        const i128 product = sat_mul_i128(composite, bound);
        if (product < 0) {
            throw std::domain_error("isqrt() argument must be nonnegative");
        }
        const i128 root = static_cast<i128>(isqrt_u128(static_cast<u128>(product)));
        bound = std::max(bound, root);
    }
    if (composite <= bound) {
        return false;
    }
    bool has_left_null = false, has_right_null = false;
    double left_null = 0.0, right_null = 0.0;
    for (size_t i = 0; i < n; ++i) {
        if (keys[i].left.has_null_fraction) {
            if (!has_left_null || keys[i].left.null_fraction > left_null) {
                left_null = keys[i].left.null_fraction;
            }
            has_left_null = true;
        }
        if (keys[i].right.has_null_fraction) {
            if (!has_right_null || keys[i].right.null_fraction > right_null) {
                right_null = keys[i].right.null_fraction;
            }
            has_right_null = true;
        }
    }
    const int64_t ndv = cap_i64_from_i128(bound);
    collapsed->left = KeyStats{ndv, 0, left_null, true, false, has_left_null, NDV_DOMAIN_STANDIN};
    collapsed->right = KeyStats{ndv, 0, right_null, true, false, has_right_null, NDV_DOMAIN_STANDIN};
    return true;
}

inline int64_t estimate_after_filter(int64_t input_rows, double selectivity) {
    return std::max<int64_t>(1, cap_i64(static_cast<double>(input_rows) * selectivity));
}

// Distinct values expected to survive a filter (the caller maps None to None).
inline int64_t surviving_distinct_count(int64_t distinct_count, int64_t input_rows,
                                        double selectivity) {
    if (distinct_count <= 0 || input_rows <= 0) {
        return distinct_count;
    }
    if (selectivity >= 1.0) {
        return distinct_count;
    }
    if (selectivity <= 0.0) {
        return 1;
    }
    const double rows_per_value =
        static_cast<double>(input_rows) / static_cast<double>(distinct_count);
    const double survivors =
        static_cast<double>(distinct_count) * (1.0 - std::pow(1.0 - selectivity, rows_per_value));
    return std::max<int64_t>(1, std::min(distinct_count, cap_i64(survivors)));
}

// min(input rows, product of group-key NDVs); any unknown (has[i] == 0) or
// non-positive NDV makes it the input row count. The running product saturates
// at INT64_MAX: once it exceeds input_rows the answer is input_rows either way.
inline int64_t estimate_group_by_cardinality(int64_t input_rows, const int64_t* ndvs,
                                             const uint8_t* has, size_t n) {
    if (input_rows <= 0) {
        return 1;
    }
    if (n == 0) {
        return 1;
    }
    int64_t cardinality = 1;
    for (size_t i = 0; i < n; ++i) {
        if (!has[i] || ndvs[i] <= 0) {
            return std::max<int64_t>(1, input_rows);
        }
        int64_t next;
        if (__builtin_mul_overflow(cardinality, ndvs[i], &next)) {
            next = std::numeric_limits<int64_t>::max();
        }
        cardinality = next;
    }
    return std::max<int64_t>(1, std::min(cardinality, input_rows));
}

// ---- join graph -------------------------------------------------------------

struct Vertex {
    int64_t row_count;
    int64_t domain_rows;  // PRE-filter row count (base, or row_count when absent)
};

struct Edge {
    int32_t left;
    int32_t right;
    int32_t class_id;
    bool has_class;
    double extra_selectivity;
    uint32_t key_begin;
    uint32_t key_count;
};

inline int lowest_bit(uint64_t m) { return __builtin_ctzll(m); }

struct Graph {
    std::vector<Vertex> vertices;
    std::vector<Edge> edges;
    std::vector<KeyPair> keys;
    std::vector<uint64_t> adj;
    // Edges grouped by unordered endpoint pair, insertion order within a pair.
    // pair_begin/pair_count are indexed by lo * n + hi.
    std::vector<uint32_t> pair_edges;
    std::vector<uint32_t> pair_begin;
    std::vector<uint32_t> pair_count;

    int n() const { return static_cast<int>(vertices.size()); }

    // Validate and build the adjacency index. Messages match the Python
    // JoinGraph they replace.
    void finalize() {
        const int count = n();
        if (count == 0) {
            throw std::invalid_argument("JoinGraph requires at least one vertex");
        }
        if (count > kMaxGraphVertices) {
            throw std::invalid_argument(
                "JoinGraph supports at most 64 vertices (got " + std::to_string(count) + ")");
        }
        adj.assign(count, 0);
        pair_begin.assign(static_cast<size_t>(count) * count, 0);
        pair_count.assign(static_cast<size_t>(count) * count, 0);
        for (const Edge& e : edges) {
            if (!(0 <= e.left && e.left < count && 0 <= e.right && e.right < count)) {
                throw std::invalid_argument("edge endpoints out of range: " +
                                            std::to_string(e.left) + ", " +
                                            std::to_string(e.right));
            }
            if (e.left == e.right) {
                throw std::invalid_argument("self-loop on vertex " + std::to_string(e.left));
            }
            adj[e.left] |= 1ULL << e.right;
            adj[e.right] |= 1ULL << e.left;
            const int lo = std::min(e.left, e.right), hi = std::max(e.left, e.right);
            pair_count[static_cast<size_t>(lo) * count + hi]++;
        }
        uint32_t running = 0;
        for (size_t i = 0; i < pair_count.size(); ++i) {
            pair_begin[i] = running;
            running += pair_count[i];
        }
        pair_edges.assign(running, 0);
        std::vector<uint32_t> fill(pair_count.size(), 0);
        for (uint32_t idx = 0; idx < edges.size(); ++idx) {
            const Edge& e = edges[idx];
            const int lo = std::min(e.left, e.right), hi = std::max(e.left, e.right);
            const size_t slot = static_cast<size_t>(lo) * count + hi;
            pair_edges[pair_begin[slot] + fill[slot]++] = idx;
        }
    }

    uint64_t full_mask() const {
        return n() == 64 ? ~0ULL : ((1ULL << n()) - 1);
    }

    uint64_t neighbors(uint64_t subset) const {
        uint64_t result = 0;
        for (uint64_t s = subset; s; s &= s - 1) {
            result |= adj[lowest_bit(s)];
        }
        return result & ~subset;
    }

    // Edges with one endpoint in lhs and the other in rhs: for each v in lhs
    // ascending, each w in rhs adjacent to v ascending, that pair's edges in
    // insertion order — the Python order, which fixes the float product order.
    void edges_between(uint64_t lhs, uint64_t rhs, std::vector<uint32_t>& out) const {
        if (lhs & rhs) {
            throw std::invalid_argument("edges_between requires disjoint subsets");
        }
        out.clear();
        const int count = n();
        for (uint64_t s = lhs; s; s &= s - 1) {
            const int v = lowest_bit(s);
            for (uint64_t cross = adj[v] & rhs; cross; cross &= cross - 1) {
                const int w = lowest_bit(cross);
                const size_t slot = static_cast<size_t>(std::min(v, w)) * count + std::max(v, w);
                const uint32_t b = pair_begin[slot];
                for (uint32_t k = 0; k < pair_count[slot]; ++k) {
                    out.push_back(pair_edges[b + k]);
                }
            }
        }
    }

    uint64_t component_of(uint64_t start, uint64_t within) const {
        uint64_t visited = start, frontier = start;
        while (frontier) {
            uint64_t next = 0;
            for (uint64_t f = frontier; f; f &= f - 1) {
                next |= adj[lowest_bit(f)];
            }
            next &= within & ~visited;
            visited |= next;
            frontier = next;
        }
        return visited;
    }

    bool is_connected(uint64_t subset) const {
        if (subset == 0) {
            return false;
        }
        return component_of(subset & (~subset + 1), subset) == subset;
    }

    // Components of `subset`, ascending by lowest vertex id.
    std::vector<uint64_t> connected_components(uint64_t subset) const {
        std::vector<uint64_t> out;
        uint64_t remaining = subset;
        while (remaining) {
            const uint64_t comp = component_of(remaining & (~remaining + 1), remaining);
            out.push_back(comp);
            remaining &= ~comp;
        }
        return out;
    }
};

// ---- join trees -------------------------------------------------------------

// A tree node in a Tree's arena. Leaves have vertex >= 0 and left == right ==
// -1. Edge references index Graph::edges when >= 0; a negative reference r is
// the tree-owned synthetic cartesian edge Tree::synthetic[-1 - r].
struct TreeNode {
    int32_t left;
    int32_t right;
    int32_t vertex;
    int64_t rows;
    double cost;  // 0.0 for a leaf, as the Python _tree_cost reported
    int64_t domain_rows;
    uint32_t edge_begin;
    uint32_t edge_count;
};

struct Tree {
    std::vector<TreeNode> nodes;
    std::vector<int32_t> edge_refs;
    std::vector<Edge> synthetic;
    int32_t root = -1;
};

// The costed outcome of joining two subtrees, before it is materialized.
struct Candidate {
    int32_t left;
    int32_t right;
    int64_t rows;
    double cost;
    int64_t domain_rows;
};

class Enumerator {
public:
    explicit Enumerator(const Graph& g) : g_(g) {
        tree_.nodes.reserve(static_cast<size_t>(g.n()) * 4);
        for (int v = 0; v < g.n(); ++v) {
            const Vertex& vx = g.vertices[v];
            tree_.nodes.push_back(TreeNode{-1, -1, v, vx.row_count, 0.0, vx.domain_rows, 0, 0});
        }
    }

    Tree& tree() { return tree_; }

    // _combine: dedupe edges restating one key class, bound the composite key,
    // then cost. `edges` are Graph edge indices (or synthetic refs).
    Candidate combine(int32_t left, int32_t right, const std::vector<int32_t>& edges) {
        scratch_keys_.clear();
        seen_classes_.clear();
        double extra_sel = 1.0;
        for (int32_t ref : edges) {
            const Edge& e = edge(ref);
            if (e.has_class) {
                if (std::find(seen_classes_.begin(), seen_classes_.end(), e.class_id) !=
                    seen_classes_.end()) {
                    extra_sel *= e.extra_selectivity;
                    continue;
                }
                seen_classes_.push_back(e.class_id);
            }
            for (uint32_t k = 0; k < e.key_count; ++k) {
                scratch_keys_.push_back(g_.keys[e.key_begin + k]);
            }
            extra_sel *= e.extra_selectivity;
        }
        const TreeNode& l = tree_.nodes[left];
        const TreeNode& r = tree_.nodes[right];
        const KeyPair* keys = scratch_keys_.data();
        size_t nkeys = scratch_keys_.size();
        KeyPair collapsed;
        if (apply_occupancy_bound(keys, nkeys, l.domain_rows, r.domain_rows, &collapsed)) {
            keys = &collapsed;
            nkeys = 1;
        }
        const double raw = inner_estimate(l.rows, r.rows, keys, nkeys, extra_sel);
        const int64_t rows = std::max<int64_t>(1, cap_i64(raw));
        return Candidate{left, right, rows, l.cost + r.cost + static_cast<double>(rows),
                         std::max(l.domain_rows, r.domain_rows)};
    }

    int32_t materialize(const Candidate& c, const std::vector<int32_t>& edges) {
        const uint32_t begin = static_cast<uint32_t>(tree_.edge_refs.size());
        tree_.edge_refs.insert(tree_.edge_refs.end(), edges.begin(), edges.end());
        tree_.nodes.push_back(TreeNode{c.left, c.right, -1, c.rows, c.cost, c.domain_rows, begin,
                                       static_cast<uint32_t>(edges.size())});
        return static_cast<int32_t>(tree_.nodes.size() - 1);
    }

    int32_t add_synthetic_edge(int32_t left_vertex, int32_t right_vertex) {
        tree_.synthetic.push_back(Edge{left_vertex, right_vertex, 0, false, 1.0, 0, 0});
        return -static_cast<int32_t>(tree_.synthetic.size());
    }

    void edges_between(uint64_t lhs, uint64_t rhs, std::vector<int32_t>& out) {
        g_.edges_between(lhs, rhs, scratch_u32_);
        out.assign(scratch_u32_.begin(), scratch_u32_.end());
    }

private:
    const Edge& edge(int32_t ref) const {
        return ref >= 0 ? g_.edges[ref] : tree_.synthetic[-1 - ref];
    }

    const Graph& g_;
    Tree tree_;
    std::vector<KeyPair> scratch_keys_;
    std::vector<int32_t> seen_classes_;
    std::vector<uint32_t> scratch_u32_;
};

// ---- DPccp ------------------------------------------------------------------
//
// CSG-CMP-pair enumeration of Moerkotte & Neumann (VLDB 2006) §4, cost = sum of
// intermediate cardinalities. The recursion, subset order and tie-breaks are
// the Python enumerator's exactly — they decide which of two equal-cost trees
// survives.

class Dpccp {
public:
    explicit Dpccp(const Graph& g) : g_(g), en_(g) {}

    Tree run() {
        const int n = g_.n();
        if (n == 0) {
            throw std::invalid_argument("DPccp requires at least one vertex");
        }
        if (n > kMaxDpccpVertices) {
            throw std::invalid_argument(
                "DPccp refuses graphs with more than " + std::to_string(kMaxDpccpVertices) +
                " vertices (got " + std::to_string(n) + "); use the greedy fallback instead");
        }
        const uint64_t full = g_.full_mask();
        if (!g_.is_connected(full)) {
            throw std::invalid_argument("DPccp requires a connected join graph");
        }
        for (int v = 0; v < n; ++v) {
            dp_[1ULL << v] = v;  // leaf nodes occupy arena slots 0..n-1
        }
        if (n > 1) {
            for (int i = n - 1; i >= 0; --i) {
                const uint64_t v_bit = 1ULL << i;
                enumerate_cmp(v_bit);
                enumerate_csg_rec(v_bit, (1ULL << (i + 1)) - 1, 0, false);
            }
        }
        auto it = dp_.find(full);
        if (it == dp_.end()) {
            throw std::runtime_error("DPccp failed to compute a tree for the full vertex set");
        }
        Tree& t = en_.tree();
        t.root = it->second;
        return std::move(t);
    }

private:
    void update(uint64_t s1, uint64_t s2) {
        const int32_t left = dp_.at(s1);
        const int32_t right = dp_.at(s2);
        en_.edges_between(s1, s2, edges_);
        const Candidate best = s1 <= s2 ? en_.combine(left, right, edges_)
                                        : en_.combine(right, left, edges_);
        const uint64_t uni = s1 | s2;
        auto it = dp_.find(uni);
        if (it == dp_.end()) {
            dp_.emplace(uni, en_.materialize(best, edges_));
        } else if (best.cost < en_.tree().nodes[it->second].cost) {
            it->second = en_.materialize(best, edges_);
        }
    }

    void enumerate_csg_rec(uint64_t S, uint64_t X, uint64_t cmp_for, bool has_cmp) {
        const uint64_t N = g_.neighbors(S) & ~X;
        if (N == 0) {
            return;
        }
        std::vector<uint64_t> subsets;
        for (uint64_t sub = N; sub; sub = (sub - 1) & N) {
            subsets.push_back(sub);
        }
        if (!has_cmp) {
            // Python's sort is stable; smaller csgs first so DP entries exist
            // before a larger csg is processed.
            std::stable_sort(subsets.begin(), subsets.end(), [](uint64_t a, uint64_t b) {
                return __builtin_popcountll(a) < __builtin_popcountll(b);
            });
            for (uint64_t sub : subsets) {
                enumerate_cmp(S | sub);
            }
        } else {
            for (uint64_t sub : subsets) {
                update(cmp_for, S | sub);
            }
        }
        const uint64_t new_X = X | N;
        for (uint64_t sub : subsets) {
            enumerate_csg_rec(S | sub, new_X, cmp_for, has_cmp);
        }
    }

    void enumerate_cmp(uint64_t S1) {
        const int min_v = lowest_bit(S1);
        const uint64_t X = ((1ULL << (min_v + 1)) - 1) | S1;
        const uint64_t N = g_.neighbors(S1) & ~X;
        // descending vertex order, as the paper and the Python
        for (int v = 63; v >= 0; --v) {
            if (!((N >> v) & 1ULL)) {
                continue;
            }
            const uint64_t v_bit = 1ULL << v;
            update(S1, v_bit);
            const uint64_t B_v = (1ULL << (v + 1)) - 1;
            enumerate_csg_rec(v_bit, X | (N & B_v), S1, true);
        }
    }

    const Graph& g_;
    Enumerator en_;
    std::unordered_map<uint64_t, int32_t> dp_;
    std::vector<int32_t> edges_;
};

inline Tree dpccp(const Graph& g) { return Dpccp(g).run(); }

// ---- greedy -----------------------------------------------------------------
//
// Classical greedy operator-tree builder for graphs above the DPccp threshold:
// seed with the cheapest connected pair, extend with the cheapest neighbouring
// vertex, tie-break on the lower vertex id. Disconnected inputs (test
// scaffolding only) are stitched with synthetic cartesian joins.

class Greedy {
public:
    explicit Greedy(const Graph& g) : g_(g), en_(g) {}

    Tree run() {
        if (g_.n() == 0) {
            throw std::invalid_argument("greedy_join_order requires at least one vertex");
        }
        std::vector<uint64_t> components = g_.connected_components(g_.full_mask());
        std::vector<int32_t> trees;
        std::vector<uint64_t> subsets;
        for (uint64_t c : components) {
            trees.push_back(component(c));
            subsets.push_back(c);
        }
        int32_t tree = trees[0];
        uint64_t tree_subset = subsets[0];
        for (size_t i = 1; i < trees.size(); ++i) {
            // bit_length() - 1: the highest vertex id on each side
            const int32_t lv = 63 - __builtin_clzll(tree_subset);
            const int32_t rv = 63 - __builtin_clzll(subsets[i]);
            edges_.assign(1, en_.add_synthetic_edge(lv, rv));
            tree = en_.materialize(en_.combine(tree, trees[i], edges_), edges_);
            tree_subset |= subsets[i];
        }
        Tree& t = en_.tree();
        t.root = tree;
        return std::move(t);
    }

private:
    // The cheaper orientation of joining a and b; ties keep (a, b).
    Candidate cheaper(int32_t a, int32_t b) {
        const Candidate ca = en_.combine(a, b, edges_);
        const Candidate cb = en_.combine(b, a, edges_);
        return ca.cost <= cb.cost ? ca : cb;
    }

    int32_t component(uint64_t comp) {
        std::vector<int> ids;
        for (uint64_t s = comp; s; s &= s - 1) {
            ids.push_back(lowest_bit(s));
        }
        if (ids.size() == 1) {
            return ids[0];
        }
        bool have = false;
        Candidate best{};
        int bi = 0, bj = 0;
        std::vector<int32_t> best_edges;
        for (size_t a = 0; a < ids.size(); ++a) {
            for (size_t b = a + 1; b < ids.size(); ++b) {
                const int i = ids[a], j = ids[b];
                en_.edges_between(1ULL << i, 1ULL << j, edges_);
                if (edges_.empty()) {
                    continue;
                }
                const Candidate cand = cheaper(i, j);
                if (!have || cand.cost < best.cost ||
                    (cand.cost == best.cost && (i < bi || (i == bi && j < bj)))) {
                    have = true;
                    best = cand;
                    bi = i;
                    bj = j;
                    best_edges = edges_;
                }
            }
        }
        if (!have) {
            throw std::runtime_error(
                "greedy fallback hit an unexpectedly disconnected component");
        }
        int32_t tree = en_.materialize(best, best_edges);
        uint64_t used = (1ULL << bi) | (1ULL << bj);
        uint64_t remaining = comp & ~used;
        while (remaining) {
            bool found = false;
            Candidate step{};
            int step_id = 0;
            std::vector<int32_t> step_edges;
            for (uint64_t s = remaining; s; s &= s - 1) {
                const int v = lowest_bit(s);
                en_.edges_between(used, 1ULL << v, edges_);
                if (edges_.empty()) {
                    continue;
                }
                const Candidate cand = cheaper(tree, v);
                if (!found || cand.cost < step.cost) {
                    found = true;
                    step = cand;
                    step_id = v;
                    step_edges = edges_;
                }
            }
            if (!found) {
                throw std::runtime_error(
                    "greedy fallback could not extend a connected component");
            }
            tree = en_.materialize(step, step_edges);
            used |= 1ULL << step_id;
            remaining &= ~(1ULL << step_id);
        }
        return tree;
    }

    const Graph& g_;
    Enumerator en_;
    std::vector<int32_t> edges_;
};

inline Tree greedy_join_order(const Graph& g) { return Greedy(g).run(); }

// DPccp when BOTH the vertex and edge counts are within threshold (dense
// schemas make DPccp's enumeration explode), otherwise greedy.
inline Tree enumerate_join_tree(const Graph& g, int dp_threshold, int edge_threshold) {
    if (dp_threshold < 1) {
        throw std::invalid_argument("dp_threshold must be >= 1 (got " +
                                    std::to_string(dp_threshold) + ")");
    }
    if (edge_threshold < 0) {
        throw std::invalid_argument("edge_threshold must be >= 0 (got " +
                                    std::to_string(edge_threshold) + ")");
    }
    if (g.n() <= dp_threshold && static_cast<int>(g.edges.size()) <= edge_threshold) {
        return dpccp(g);
    }
    return greedy_join_order(g);
}

}}  // namespace opteryx::planner
