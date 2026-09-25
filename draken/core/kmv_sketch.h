#pragma once
// draken/core/kmv_sketch.h — the ONE K-Minimum-Values distinct-count sketch.
//
// Header-only, zero-dependency, and deliberately in draken/core because draken
// is the only node every wheel shares: the opteryx_core wheel, the standalone
// rugo wheel and the skene wheel all bundle draken, while rugo↔skene is not a
// dependency edge that exists. Promoted here from skene/src/value_order.cpp,
// which was its only good implementation and exported it in no header.
//
// ── What it is ──────────────────────────────────────────────────────────────
//
// Estimates how many DISTINCT values a column holds, in one pass, from the K
// smallest value hashes. Bounded memory (K entries) and — once warm — one
// compare per value, because a hash at or above the current K-th smallest is
// rejected before it can be inserted.
//
// This is what separates it from the deduplicating hashtable it usually exists
// to decide FOR or AGAINST: what makes that table expensive is the table (an
// insert, a probe, and a rehash per distinct value), not the hash. So a sketch
// can afford to look at every row where the table cannot.
//
// EXACT, not estimated, whenever the column holds fewer than K distinct values:
// the sketch then holds all of them and its size IS the answer. That is the
// regime a ratio test has the least room to get wrong, and the regime every
// caller here cares most about.
//
// Above K distinct values it is the standard KMV estimator: with the K-th
// smallest hash at normalized position v in [0,1), the distinct count is
// (K-1)/v. Relative standard error is ~1/sqrt(K-2) — ~18.9% at K=32, ~3% at
// K=1024.
//
// ── K is a template parameter ───────────────────────────────────────────────
//
// Two widths exist and only two: 32 for STORED sketches (skene footers and
// opteryx manifests, where width costs bytes forever and is unioned across
// thousands of files) and 1024 for TRANSIENT decision sketches, which are
// thrown away at the end of the column and buy the accuracy for 8KB.
//
// ── Family is a template parameter, and that is a correctness guard ─────────
//
// ⛔ Two hash families exist in stored sketches and BOTH ARE PERMANENT — neither
// corpus can be re-hashed. They do not merely differ as functions; they
// DISAGREE ABOUT WHAT ONE DISTINCT VALUE IS:
//
//   * nulls — draken's Vector.hash() emits a NULL_HASH sentinel per null row,
//     a real point in the hash space that can land in the bottom-K and add one
//     to the count. A skene v2 sketch never sees a null row at all.
//   * canonicalization — Vector.hash() deliberately collides an int64-decimal
//     with the DECIMAL128 of equal value, and canonicalizes fp16 bit patterns.
//     skene v2 hashes raw bits, so those are distinct values to it.
//
// The families are skene v2's (family 1, XXH3 over value bytes — still read,
// never written) and draken's Vector.hash() (family 2), which ANALYZE and every
// skene v3 file use. So a cross-family union is not an approximation, it is a
// number with no meaning. Architect rulings 2026-08-21 and 2026-09-24, recorded
// in skene/FORMAT.md §8 (SketchRecordHeader.hash_family). Encoding the family in the TYPE makes `merge` across
// families a compile error and makes every declaration state which corpus it
// belongs to. It cannot stop raw hashes from the wrong source being handed to
// `add` — nothing in a uint64 says where it came from.
//
// Merge is EXACT, not approximate: if a hash is among the k smallest of the
// combined set and it came from sketch A, it is necessarily among the k
// smallest of A, so no input can hide a hash the answer needs.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace draken {

// Which hash a sketch's values came from. See the block above: this is a
// correctness discriminant, not a label.
enum class KmvHashFamily : uint8_t {
    // XXH3_64bits over string CONTENT bytes, or over the raw BIT PATTERN for
    // fixed width — skene's own dedup hash (ValueKey in value_order.cpp),
    // chosen so the sketch and the deduplication it gates cannot disagree
    // about what "distinct" means. STORED in skene footers.
    kXxh3ValueBytes = 1,
    // draken's Vector.hash() (draken_hash / simd_hash_i64). STORED in opteryx
    // manifests by ANALYZE and by the canonical catalog stats engine.
    kDrakenVectorHash = 2,
    // rugo's writer-local value hash. DECISION-ONLY: this sketch is destroyed
    // at the end of the column and is never written anywhere, so it is bound
    // by nothing except being consistent with itself for one column.
    kRugoWriterDecision = 3,
};

// 2^64 as a double. The K-th smallest hash divided by this is the fraction of
// the hash space the K smallest distinct values occupy.
inline constexpr double kKmvHashSpace = 18446744073709551616.0;

template <std::size_t K, KmvHashFamily Family>
class KmvSketch {
  public:
    static constexpr std::size_t   kK      = K;
    static constexpr KmvHashFamily kFamily = Family;

    static_assert(K >= 3u, "KMV's (K-1)/v estimator needs K >= 3 to mean anything");

    // One allocation for the life of the sketch: K + 1 because `add` inserts
    // before it trims, so the vector is momentarily one over its bound.
    KmvSketch() { smallest_.reserve(K + 1u); }

    // BACKED BY A SORTED ARRAY, NOT A TREE. This holds exactly what a
    // std::set<uint64_t> held — the K smallest DISTINCT hashes, ascending —
    // and every exit below matches the set's semantics case for case, so the
    // hashes it yields are unchanged. Only the container is different.
    //
    // Why it is worth it: the warm path is the whole loop, and on a std::set it
    // read `*rbegin()`, which walks to the tree's rightmost node on EVERY
    // value. Here the K-th smallest is `smallest_.back()`, cached in
    // `threshold_` so the reject is a single compare against a register.
    //
    // The accepting path memmoves up to K-1 entries (8KB at K=1024) instead of
    // relinking three pointers, which sounds worse and is not: acceptance
    // requires beating the K-th smallest hash, so after the first K values it
    // happens O(K log N / N) of the time, while the reject runs N times.
    void add(uint64_t hash) {
        if (smallest_.size() >= K) {
            // Warm: the overwhelmingly common case, and one compare.
            if (hash >= threshold_) return;
        }
        // Dedup, exactly as std::set did: a hash already present is not a new
        // distinct value and must not displace the current maximum.
        const auto at = std::lower_bound(smallest_.begin(), smallest_.end(), hash);
        if (at != smallest_.end() && *at == hash) return;
        smallest_.insert(at, hash);
        if (smallest_.size() > K) smallest_.pop_back();
        if (smallest_.size() >= K) threshold_ = smallest_.back();
    }

    // Union with another sketch OF THE SAME FAMILY AND WIDTH — the type system
    // enforces both. Exact: see the header block.
    void merge(const KmvSketch& other) {
        for (const uint64_t hash : other.smallest_) add(hash);
    }

    // Union with a narrower sketch of the same family. A K=32 sketch's hashes
    // are the 32 smallest of its column, so folding them into a K=1024 sketch
    // is the same union in the other direction — this is how per-file stored
    // sketches roll up. Widening the other way would be a lie (the wide sketch
    // would claim a precision its inputs never had), so only narrow→wide.
    template <std::size_t OtherK>
    void merge_narrower(const KmvSketch<OtherK, Family>& other) {
        static_assert(OtherK <= K, "merging a WIDER sketch would overstate its precision");
        for (const uint64_t hash : other.hashes()) add(hash);
    }

    // The k smallest hashes, ascending. Taking the k smallest of the K smallest
    // IS the k smallest overall, so a K=1024 sketch yields an EXACT K=32 one —
    // which is why a decision can keep 3% accuracy while the STORED sketch
    // costs 32 hashes.
    std::vector<uint64_t> min_k(std::size_t k) const {
        const std::size_t n = k < smallest_.size() ? k : smallest_.size();
        return std::vector<uint64_t>(smallest_.begin(), smallest_.begin() + n);
    }

    // The retained hashes, ascending and distinct. At most K of them.
    const std::vector<uint64_t>& hashes() const { return smallest_; }

    std::size_t size() const { return smallest_.size(); }

    // Below K the sketch holds EVERY distinct value: size() is the exact
    // answer, not an estimate. Callers that must report exactness (the manifest
    // reader does) branch on this rather than re-deriving it.
    bool is_exact() const { return smallest_.size() < K; }

    uint64_t kth_smallest() const { return smallest_.back(); }

    double estimate() const {
        if (is_exact()) return static_cast<double>(smallest_.size());
        const double v = static_cast<double>(smallest_.back()) / kKmvHashSpace;
        // Guard the divide: v == 0 needs the K-th smallest hash to be 0, which
        // means every sampled hash collided at 0. Report K rather than infinity.
        if (v <= 0.0) return static_cast<double>(K);
        return static_cast<double>(K - 1u) / v;
    }

  private:
    std::vector<uint64_t> smallest_;       // ascending, distinct, at most K
    uint64_t              threshold_ = 0;  // == smallest_.back() once full
};

// The two widths that exist. Stored sketches are 32 wide (skene's kSketchK and
// opteryx's manifest K); transient decision sketches are 1024.
template <KmvHashFamily Family>
using KmvSketch32 = KmvSketch<32u, Family>;
template <KmvHashFamily Family>
using KmvSketch1024 = KmvSketch<1024u, Family>;

}  // namespace draken
