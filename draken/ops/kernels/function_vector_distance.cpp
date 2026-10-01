// draken/ops/kernels/function_vector_distance.cpp — vector/distance function kernels.
//
// Kernels:
//   draken_embed                      VARCHAR            -> VECTOR_FP16(ctx dimension)
//   draken_cosine_similarity_text     (VARCHAR, VARCHAR)   -> FLOAT64
//   draken_cosine_distance_text       (VARCHAR, VARCHAR)   -> FLOAT64
//   draken__match_against_2           (VARCHAR, VARCHAR)   -> BOOL
//
// MATCH (col) AGAINST (str) is `COSINE_SIMILARITY(col, str) >= match_threshold` and runs
// the text cosine body itself to guarantee it. The threshold is the `match_threshold`
// session variable, resolved at bind time (see match_ctx in kernel_context.h). It is
// tunable, not a constant, because a score is only meaningful against the ACTIVE embedder:
// under the core lexical EMBED, scores are bimodal at 1.0 and ~0 (measured over $planets
// against 'Earth': Earth 1.0, Mars 0.043, everything else <= 0.018, some negative), so any
// threshold in (0.3, 1.0] makes MATCH a case-insensitive exact match; under a semantic
// capability the same number is a real similarity cut. One constant could not mean both.
//
// The `_text` suffix is the catalog OVERLOAD ID lowercased
// (COSINE_SIMILARITY_TEXT -> draken_cosine_similarity_text). The bare
// `draken_cosine_similarity` / `draken_cosine_distance` names are deliberately NOT
// registered: a name-level hit would bind the generic arm (all operands, no ctx).
//
// VECTOR is not a SQL type (architect ruling 2026-10-01): it exists only inside vector
// indexes. draken_embed is therefore not a SQL function — it is the active embedding
// capability, reached by the text kernels here and by the index builder.
//
// EMBED semantics (architect decision, 2026-07-16): EMBED is the static hashed
// projection — a total, deterministic, dependency-free function of the input text.
// It is intentionally NOT a semantic transformer embedding: it scores lexical
// n-gram overlap, so COSINE_SIMILARITY('dog','puppy') ~ 0. A MiniLM-backed
// provider is a separately registerable capability, not this kernel.
//
// This kernel is a bit-exact port of _StaticHashEmbeddingProvider
// (opteryx/types/vectors/embeddings.py) + pack_static_hash_row
// (opteryx/types/vectors/vector_math.pyx). Bit-exactness is deliberate: it makes the
// Python provider a usable oracle for verification. The two must not drift — the Python
// side is the one that goes away.
//
// Tokenizer scope: the Python regex is
//     [A-Za-z0-9]+(?:['_-][A-Za-z0-9]+)*|[^\w\s]
// The second alternative only ever matches characters that are neither word nor space
// — i.e. never a Unicode letter or digit — so every token it produces is dropped by the
// `any(ch.isalnum())` filter that follows it. The surviving grammar is therefore pure
// ASCII, and every non-ASCII byte acts as a separator. This port implements exactly that.
// Known divergence: Python's str.lower() is Unicode-aware, so the handful of non-ASCII
// characters whose lowercase form IS ASCII alnum (U+212A KELVIN SIGN -> 'k',
// U+0130 -> 'i' + combining dot) tokenize differently here. Both fold to tokens that the
// len<=1 filter drops in every case checked; no other Unicode character can reach the
// ASCII alnum class.
//
// Zero-magnitude vectors: cosine of a zero-norm vector is 0.0/0.0 -> NaN, per
// draken/ops/vector_cosine.h. That is the engine's answer and it is deliberate: NaN is
// the honest IEEE result of an undefined direction, and it propagates visibly rather
// than masquerading as "perfectly dissimilar". The retired Python path answered 0.0,
// which silently conflated "undefined" with "orthogonal".

#include <cmath>
#include <cstdint>
#include <cstdlib>   // malloc/free — ctx allocation pairs with kernel_free_context's free()
#include <cstring>
#include <limits>
#include <new>
#include <stdexcept>
#include <vector>

#include "core/alloc.h"
#include "core/buffers.h"
#include "core/fp16.h"
#include "core/string_slot.h"
#include "ops/kernels/error_handling.h"
#include "ops/kernels/kernel_context.h"
#include "ops/kernels/result_helpers.h"
#include "ops/vec_result.h"
#include "ops/vector_cosine_row.h"
#include "xxhash.h"  // XXH3_64bits — must match opteryx xxhash.pyx hash_bytes exactly.

namespace {

// The C function-kernel ABI (c_kernel_abi.h) — the shape of the EMBED kernel the text
// overloads delegate to.
typedef VecResult (*func_fn_t)(void* ctx, const DrakenVector* const* args, uint32_t nargs);


// NOTE: this file holds NO embedding-width constant on purpose. The width is decided by
// the active embedding capability (opteryx/types/vectors/embedding_capability.py) and
// handed to every kernel here in a ctx. A constant duplicated here could disagree with
// the capability's declared width. One number, one source.

// _StaticHashEmbeddingProvider._projection_scale == float(2 ** -0.5)
const float PROJECTION_SCALE = static_cast<float>(0.7071067811865476);

constexpr uint32_t CHAR_NGRAM_MIN = 3u;
constexpr uint32_t CHAR_NGRAM_MAX = 4u;

inline bool is_ascii_alnum(uint8_t c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}

// The regex's token-joining class: ['_-]
inline bool is_joiner(uint8_t c) { return c == '\'' || c == '_' || c == '-'; }

inline uint8_t ascii_lower(uint8_t c) {
    return (c >= 'A' && c <= 'Z') ? static_cast<uint8_t>(c + 32) : c;
}

// _STATIC_STOPWORDS. Sorted so lookup is a binary search; keep it sorted.
const char* const STOPWORDS[] = {
    "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "from", "has",
    "have", "i", "if", "in", "is", "it", "its", "me", "my", "of", "on", "or", "our",
    "so", "that", "the", "their", "them", "there", "they", "this", "to", "was", "we",
    "were", "with", "would", "you", "your",
};
constexpr size_t STOPWORD_COUNT = sizeof(STOPWORDS) / sizeof(STOPWORDS[0]);

bool is_stopword(const uint8_t* tok, size_t len) {
    size_t lo = 0, hi = STOPWORD_COUNT;
    while (lo < hi) {
        const size_t mid = (lo + hi) / 2;
        const char* cand = STOPWORDS[mid];
        const size_t clen = std::strlen(cand);
        const size_t n = len < clen ? len : clen;
        int cmp = std::memcmp(cand, tok, n);
        if (cmp == 0) cmp = (clen < len) ? -1 : (clen > len ? 1 : 0);
        if (cmp == 0) return true;
        if (cmp < 0) lo = mid + 1; else hi = mid;
    }
    return false;
}

// _feature_projections: two (slot, signed-scale) pairs per feature. The Python side
// memoises this in an LRU; the hash is cheap enough here that a cache would only add a
// per-row allocation, so it is recomputed.
struct Projection { uint32_t slot; float sign; };

inline void feature_projections(const uint8_t* feat, size_t len, uint32_t dims,
                                Projection out[2]) {
    // hash_bytes(feature) == XXH3_64bits(feature) with the default seed.
    const uint64_t first = XXH3_64bits(feat, len);

    // hash_bytes(b"\x01" + feature) — prefix byte, so the second slot decorrelates.
    uint8_t stack_buf[256];
    uint8_t* tmp = stack_buf;
    bool heap = false;
    if (len + 1u > sizeof(stack_buf)) {
        tmp = static_cast<uint8_t*>(draken_malloc(len + 1u));
        if (!tmp) throw std::bad_alloc();
        heap = true;
    }
    tmp[0] = 0x01u;
    std::memcpy(tmp + 1, feat, len);
    const uint64_t second = XXH3_64bits(tmp, len + 1u);
    if (heap) draken_free(tmp);

    out[0].slot = static_cast<uint32_t>(first % dims);
    out[0].sign = ((first >> 63) & 1u) == 0u ? PROJECTION_SCALE : -PROJECTION_SCALE;
    out[1].slot = static_cast<uint32_t>(second % dims);
    out[1].sign = ((second >> 63) & 1u) == 0u ? PROJECTION_SCALE : -PROJECTION_SCALE;
}

// One token as a range into the lowercased text buffer.
struct Token { uint32_t off; uint32_t len; };

// Greedy match of [A-Za-z0-9]+(?:['_-][A-Za-z0-9]+)* over `text`, then the
// len<=1 and stopword filters _tokenize applies.
void tokenize(const uint8_t* text, uint32_t len, Token* out, uint32_t* out_count,
              uint32_t max_tokens) {
    uint32_t count = 0;
    uint32_t i = 0;
    while (i < len && count < max_tokens) {
        if (!is_ascii_alnum(text[i])) { ++i; continue; }
        const uint32_t start = i;
        while (i < len && is_ascii_alnum(text[i])) ++i;
        // (?:['_-][A-Za-z0-9]+)* — only consume the joiner when an alnum follows it,
        // otherwise the regex would not have matched it either.
        while (i + 1u < len && is_joiner(text[i]) && is_ascii_alnum(text[i + 1u])) {
            ++i;
            while (i < len && is_ascii_alnum(text[i])) ++i;
        }
        const uint32_t tlen = i - start;
        if (tlen <= 1u) continue;                       // _tokenize: len(token) <= 1
        if (is_stopword(text + start, tlen)) continue;  // _tokenize: stopword
        out[count].off = start;
        out[count].len = tlen;
        ++count;
    }
    *out_count = count;
}

// _gather_contributions + pack_static_hash_row, fused.
//
// Contributions are accumulated straight into the fp32 scratch in the SAME order the
// Python builds its flat (indices, contributions) arrays — fp32 addition is not
// associative, so emission order is load-bearing for bit-exactness.
void static_hash_embed_row(const uint8_t* raw, uint32_t raw_len, uint16_t* dst,
                           uint32_t dims, float* scratch, uint8_t* lower_buf,
                           Token* tokens, uint32_t max_tokens, uint8_t* feat_buf) {
    std::memset(scratch, 0, static_cast<size_t>(dims) * sizeof(float));

    for (uint32_t i = 0; i < raw_len; ++i) lower_buf[i] = ascii_lower(raw[i]);

    uint32_t n_tokens = 0;
    tokenize(lower_buf, raw_len, tokens, &n_tokens, max_tokens);

    Projection proj[2];

    for (uint32_t t = 0; t < n_tokens; ++t) {
        const uint8_t* tok = lower_buf + tokens[t].off;
        const uint32_t tlen = tokens[t].len;

        // b"u:" + encoded — weight 1.0
        feat_buf[0] = 'u'; feat_buf[1] = ':';
        std::memcpy(feat_buf + 2, tok, tlen);
        feature_projections(feat_buf, tlen + 2u, dims, proj);
        scratch[proj[0].slot] += proj[0].sign;
        scratch[proj[1].slot] += proj[1].sign;

        // b"b:" + encoded + b" " + next_token — weight 0.5
        if (t + 1u < n_tokens) {
            const uint8_t* nxt = lower_buf + tokens[t + 1u].off;
            const uint32_t nlen = tokens[t + 1u].len;
            feat_buf[0] = 'b'; feat_buf[1] = ':';
            std::memcpy(feat_buf + 2, tok, tlen);
            feat_buf[2 + tlen] = ' ';
            std::memcpy(feat_buf + 3 + tlen, nxt, nlen);
            feature_projections(feat_buf, tlen + nlen + 3u, dims, proj);
            scratch[proj[0].slot] += proj[0].sign * 0.5f;
            scratch[proj[1].slot] += proj[1].sign * 0.5f;
        }

        // b"g:" + wrapped[start:start+size] over wrapped = "<" + token + ">" — weight 0.25
        const uint32_t wrapped_len = tlen + 2u;
        const uint32_t max_ngram = CHAR_NGRAM_MAX < wrapped_len ? CHAR_NGRAM_MAX : wrapped_len;
        for (uint32_t size = CHAR_NGRAM_MIN; size <= max_ngram; ++size) {
            for (uint32_t start = 0; start + size <= wrapped_len; ++start) {
                feat_buf[0] = 'g'; feat_buf[1] = ':';
                for (uint32_t k = 0; k < size; ++k) {
                    const uint32_t w = start + k;   // index into "<token>"
                    feat_buf[2 + k] = (w == 0u) ? static_cast<uint8_t>('<')
                                    : (w == wrapped_len - 1u) ? static_cast<uint8_t>('>')
                                    : tok[w - 1u];
                }
                feature_projections(feat_buf, size + 2u, dims, proj);
                scratch[proj[0].slot] += proj[0].sign * 0.25f;
                scratch[proj[1].slot] += proj[1].sign * 0.25f;
            }
        }
    }

    // pack_static_hash_row: fp32 norm, zero vector stays zero (never NaN here).
    float norm_sq = 0.0f;
    for (uint32_t j = 0; j < dims; ++j) norm_sq += scratch[j] * scratch[j];

    if (norm_sq == 0.0f) {
        std::memset(dst, 0, static_cast<size_t>(dims) * sizeof(uint16_t));
        return;
    }
    // Cython: `cdef float norm = c_sqrt(norm_sq)` — double sqrt rounded back to float.
    const float norm = static_cast<float>(std::sqrt(static_cast<double>(norm_sq)));
    for (uint32_t j = 0; j < dims; ++j)
        dst[j] = fp16_ieee_from_fp32_value(scratch[j] / norm);
}

inline bool vd_is_string(DrakenType t) {
    return t == DRAKEN_VARCHAR || t == DRAKEN_NVARCHAR || t == DRAKEN_VARBINARY;
}

inline bool vd_row_valid(const uint8_t* validity, uint32_t i) {
    return (validity == nullptr) || ((validity[i >> 3] >> (i & 7u)) & 1u);
}

// Scratch buffers sized once per kernel call and reused across rows.
struct EmbedScratch {
    float*   scratch = nullptr;
    uint8_t* lower   = nullptr;
    Token*   tokens  = nullptr;
    uint8_t* feat    = nullptr;
    uint32_t max_tokens = 0;

    ~EmbedScratch() {
        draken_free(scratch); draken_free(lower);
        draken_free(tokens);  draken_free(feat);
    }

    // max_len = longest row payload in bytes.
    void init(uint32_t dims, uint32_t max_len) {
        scratch = static_cast<float*>(draken_malloc(static_cast<size_t>(dims) * sizeof(float)));
        lower   = static_cast<uint8_t*>(draken_malloc(max_len > 0u ? max_len : 1u));
        // Every token is >= 2 bytes and separated by >= 0 bytes, so a text of L bytes
        // yields at most L/2 tokens; +1 keeps L==0/1 safe.
        max_tokens = max_len / 2u + 1u;
        tokens  = static_cast<Token*>(draken_malloc(static_cast<size_t>(max_tokens) * sizeof(Token)));
        // Worst-case feature: "b:" + tok + " " + next  ==  2 + max_len + 1 + max_len.
        feat    = static_cast<uint8_t*>(draken_malloc(static_cast<size_t>(max_len) * 2u + 8u));
        if (!scratch || !lower || !tokens || !feat) throw std::bad_alloc();
    }
};

uint32_t max_payload_len(const DrakenVector* v) {
    const auto* sa = static_cast<const DrakenStringArena*>(v->data);
    uint32_t m = 0;
    for (uint32_t j = 0; j < v->data_length; ++j) {
        const uint32_t l = str_length(&sa->slots[j]);
        if (l > m) m = l;
    }
    return m;
}

// Embed the K PHYSICAL slots of a string vector into an fp16 block of `data_length *
// dims`. Callers then read logical row i as `block[selection[i] * dims]` — the uniform
// data[selection[i]] access pattern.
//
// Embedding the PHYSICAL values (not the logical rows) is what makes this uniform
// rather than shape-specialized: it is one code path that happens to do the right
// amount of work for every encoding. A dense operand has k == n and embeds n texts; a
// constant operand (COSINE_SIMILARITY(col, 'literal') — the common shape) has k == 1
// and embeds ONCE instead of n times; a dict operand embeds each distinct value once.
// No shape discriminant is read and the answer is identical for all three. It also
// mirrors the string kernels (string_trim.cpp), which likewise transform the k
// physical slots and let selection do the mapping.
uint16_t* embed_string_vector(const DrakenVector* v, uint32_t dims) {
    const uint32_t k = v->data_length;
    const size_t cells = static_cast<size_t>(k > 0u ? k : 1u) * dims;
    uint16_t* out = static_cast<uint16_t*>(draken_malloc(cells * sizeof(uint16_t)));
    if (!out) throw std::bad_alloc();
    std::memset(out, 0, cells * sizeof(uint16_t));

    EmbedScratch sc;
    sc.init(dims, max_payload_len(v));

    const auto* sa = static_cast<const DrakenStringArena*>(v->data);
    for (uint32_t j = 0; j < k; ++j) {
        const DrakenStringSlot* slot = &sa->slots[j];
        static_hash_embed_row(str_data(slot, sa->arena), str_length(slot),
                              out + static_cast<size_t>(j) * dims, dims,
                              sc.scratch, sc.lower, sc.tokens, sc.max_tokens, sc.feat);
    }
    return out;
}

// Copy a source vector's null bitmap into a fresh all-valid-initialised bitmap.
// Returns nullptr when every row of both inputs is valid (the all-valid convention).
uint8_t* merged_validity(const DrakenVector* a, const DrakenVector* b, uint32_t n) {
    if (a->validity == nullptr && (b == nullptr || b->validity == nullptr)) return nullptr;
    const uint32_t bm = (n + 7u) >> 3;
    const uint32_t padded = (bm + 7u) & ~7u;
    uint8_t* out = static_cast<uint8_t*>(draken_malloc(padded > 0u ? padded : 8u));
    if (!out) throw std::bad_alloc();
    std::memset(out, 0xFF, padded > 0u ? padded : 8u);
    for (uint32_t i = 0; i < n; ++i) {
        if (!vd_row_valid(a->validity, i) || (b != nullptr && !vd_row_valid(b->validity, i)))
            out[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7u)));
    }
    if (n & 7u) out[bm - 1u] &= static_cast<uint8_t>((1u << (n & 7u)) - 1u);
    return out;
}

// Row-wise cosine over two PHYSICAL fp16 blocks, read through each operand's own
// selection — the uniform data[selection[i]] pattern, correct for any encoding.
// The arithmetic is cosine_row_fp16, the same function cosine_sim_fp16 uses.
VecResult cosine_over_embedded(const uint16_t* pa, const uint32_t* sel_a,
                               const uint16_t* pb, const uint32_t* sel_b,
                               uint32_t n, uint32_t dims, uint8_t* validity,
                               bool as_distance) {
    double* dst = static_cast<double*>(draken_malloc((n > 0u ? n : 1u) * sizeof(double)));
    if (!dst) throw std::bad_alloc();

    for (uint32_t i = 0; i < n; ++i) {
        if (!vd_row_valid(validity, i)) { dst[i] = 0.0; continue; }
        const uint16_t* ra = pa + static_cast<size_t>(sel_a[i]) * dims;
        const uint16_t* rb = pb + static_cast<size_t>(sel_b[i]) * dims;
        double sim = draken::ops::cosine_row_fp16(ra, rb, dims);
        if (as_distance) {
            // 1 - clip(sim, -1, 1); NaN survives the clip (both compares are false).
            if (sim < -1.0) sim = -1.0; else if (sim > 1.0) sim = 1.0;
            sim = 1.0 - sim;
        }
        dst[i] = sim;
    }

    VecResult r;
    r.data           = dst;
    r.validity       = validity;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = DRAKEN_FLOAT64;
    r.flags          = DRAKEN_SEL_IDENTITY;
    return r;
}

// Shared body for the two text overloads.
//
// Delegates BOTH sides to the bind-time-resolved `draken_embed` (ctx->embed_fn) rather
// than embedding here. COSINE_SIMILARITY(a, b) over strings and
// COSINE_SIMILARITY(EMBED(a), EMBED(b)) are the same question, so they must go through
// the same embedder — including when a capability has replaced the core one. An
// embedding implementation of its own here was duplicated logic that agreed with EMBED
// only by coincidence, and stopped the moment MiniLM was installed.
VecResult cosine_text_kernel(void* ctx, const DrakenVector* const* args, uint32_t nargs,
                             bool as_distance, const char* who) {
    if (nargs != 2u) return draken_error_sentinel_fmt("%s: expected 2 arguments", who);
    if (ctx == nullptr)
        return draken_error_sentinel_fmt("%s: missing embedding context", who);
    const auto* c = static_cast<const struct cosine_text_ctx*>(ctx);
    if (c->dimension == 0u)
        return draken_error_sentinel_fmt("%s: vector dimension must be >= 1", who);
    if (c->embed_fn == nullptr)
        return draken_error_sentinel_fmt("%s: no EMBED kernel resolved", who);

    const DrakenVector* a = args[0];
    const DrakenVector* b = args[1];
    if (!vd_is_string(a->type) || !vd_is_string(b->type))
        return draken_error_sentinel_fmt("%s: both operands must be string", who);
    if (a->length != b->length)
        return draken_error_sentinel_fmt("%s: operand lengths must match", who);

    // Stack-local: the embed kernel only reads the width from it, so there is no nested
    // ctx to own or free.
    struct vector_dim_ctx ectx;
    ectx.dimension = c->dimension;
    const auto embed = reinterpret_cast<func_fn_t>(c->embed_fn);

    // Both results are dense VECTOR_FP16 (one row per logical row, identity selection).
    VecResult va = embed(&ectx, &a, 1u);
    if (va.data == nullptr) return va;   // propagate the embed kernel's error verbatim
    VecResult vb = embed(&ectx, &b, 1u);
    if (vb.data == nullptr) { draken_free(va.data); draken_free(va.validity); return vb; }

    uint8_t* val = nullptr;
    try {
        const uint32_t n = a->length;
        val = merged_validity(a, b, n);
        VecResult r = cosine_over_embedded(
            static_cast<const uint16_t*>(va.data), va.selection,
            static_cast<const uint16_t*>(vb.data), vb.selection,
            n, c->dimension, val, as_distance);
        draken_free(va.data); draken_free(va.validity);
        draken_free(vb.data); draken_free(vb.validity);
        return r;
    } catch (const std::exception& e) {
        draken_free(va.data); draken_free(va.validity);
        draken_free(vb.data); draken_free(vb.validity);
        draken_free(val);
        return draken_error_sentinel_fmt("%s: %s", who, e.what());
    }
}

// SQL `MATCH (col) AGAINST (str)` — cosine_text_kernel thresholded to BOOL.
//
// Deliberately calls cosine_text_kernel rather than reimplementing the comparison: MATCH
// is DEFINED as `COSINE_SIMILARITY(col, str) >= threshold`, so running the same code makes
// the two agree by construction rather than by review. A second scoring implementation
// here is exactly the split-brain the text overloads already had once.
//
// NaN (zero-magnitude embedding — empty or stopword-only text) fails `>=` and yields
// false, without a special case: an undefined direction is not a match.
VecResult match_against_kernel(void* ctx, const DrakenVector* const* args, uint32_t nargs,
                               const char* who) {
    if (ctx == nullptr) return draken_error_sentinel_fmt("%s: missing match context", who);
    const auto* m = static_cast<const struct match_ctx*>(ctx);

    struct cosine_text_ctx cctx;
    cctx.dimension = m->dimension;
    cctx.embed_fn  = m->embed_fn;
    // Validates arity/types/lengths and propagates any error sentinel verbatim.
    VecResult sim = cosine_text_kernel(&cctx, args, nargs, /*as_distance=*/false, who);
    if (sim.data == nullptr) return sim;

    const uint32_t n      = sim.length;
    const uint32_t padded = ((((n + 7u) >> 3) + 7u) & ~7u);
    auto* bits = static_cast<uint8_t*>(draken_malloc(padded ? padded : 8u));
    if (!bits) {
        draken_free(sim.data);
        draken_free(sim.validity);
        return draken_error_sentinel_fmt("%s: allocation failed", who);
    }
    std::memset(bits, 0, padded ? padded : 8u);

    // sim is dense with an identity selection (cosine_over_embedded), so row i is data[i];
    // its validity is already the merged operand validity and carries straight over.
    const double* scores = static_cast<const double*>(sim.data);
    for (uint32_t i = 0; i < n; ++i) {
        if (scores[i] >= m->threshold) bits[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
    }

    VecResult r{};
    r.data           = bits;
    r.validity       = sim.validity;   // ownership moves to the result
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = DRAKEN_BOOL;
    r.flags          = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
    draken_free(sim.data);
    return r;
}

}  // namespace

extern "C" {

// malloc, NOT draken_malloc: every ctx is released through kernel_free_context, which
// calls plain free(). Matches kernel_alloc_format_ctx and friends.
struct cosine_text_ctx* kernel_alloc_cosine_text_ctx(uint32_t dimension, void* embed_fn) {
    auto* c = static_cast<struct cosine_text_ctx*>(malloc(sizeof(struct cosine_text_ctx)));
    if (!c) return nullptr;
    c->dimension = dimension;
    c->embed_fn  = embed_fn;
    return c;
}

struct match_ctx* kernel_alloc_match_ctx(uint32_t dimension, void* embed_fn, double threshold) {
    auto* c = static_cast<struct match_ctx*>(malloc(sizeof(struct match_ctx)));
    if (!c) return nullptr;
    c->dimension = dimension;
    c->embed_fn  = embed_fn;
    c->threshold = threshold;
    return c;
}

VecResult draken_embed(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1u) return draken_error_sentinel("draken_embed: expected 1 argument");
    const DrakenVector* v = args[0];
    if (!vd_is_string(v->type))
        return draken_error_sentinel("draken_embed: string operand required");

    // The caller hands down the width the active capability declared, and this kernel
    // produces exactly that width — the declaration is the single source of truth.
    // A hashed projection is width-agnostic by construction (slot = hash % dims), so
    // honouring the declared width costs nothing. A capability whose width is fixed by
    // a model must reject a width it cannot produce rather than silently retype.
    if (ctx == nullptr)
        return draken_error_sentinel("draken_embed: missing vector dimension context");
    const uint32_t dims = static_cast<const struct vector_dim_ctx*>(ctx)->dimension;
    if (dims == 0u || dims > 65535u)
        return draken_error_sentinel("draken_embed: vector dimension must be 1..65535");

    uint16_t* data = nullptr;
    try {
        // SHAPE-PRESERVING: embed the k physical values and keep the operand's
        // encoding, rather than gathering to n dense rows. The uniform contract is
        // data[selection[i]] either way, so the answer is identical — but a constant
        // operand (EMBED('literal'), the shape COSINE_SIMILARITY(col, 'literal')
        // produces) stays k == 1 instead of materialising n identical vectors. At 256
        // dims that is n*512 bytes of memcpy saved per call, and for a model-backed
        // capability it is the difference between 1 inference and n. Densifying is the
        // projection boundary's job (_dv_copy_result_dense gathers through selection),
        // and it only pays for it when the column is actually projected.
        data = embed_string_vector(v, dims);
    } catch (const std::exception& e) {
        draken_free(data);
        return draken_error_sentinel_fmt("draken_embed: %s", e.what());
    }

    VecResult r;
    r.data = data;
    r.type = DRAKEN_VECTOR_FP16;
    // Adopt the operand's shape (length/data_length/flags/selection) and its per-row
    // validity: null in -> null out. Identity and constant operands reuse the global
    // selection arrays; only a genuine dict pays for an owned copy of the codes.
    try {
        kernel_preserve_shape(r, v);
    } catch (const std::exception& e) {
        draken_free(data);
        return draken_error_sentinel_fmt("draken_embed: %s", e.what());
    }
    // VECTOR_FP16 without a dimension descriptor is a hard error in vecresult_to_owner.
    r.vec_dimension  = static_cast<uint16_t>(dims);
    return r;
}

VecResult draken_cosine_similarity_text(void* ctx, const DrakenVector* const* args,
                                        uint32_t nargs) {
    return cosine_text_kernel(ctx, args, nargs, /*as_distance=*/false,
                              "draken_cosine_similarity");
}

VecResult draken_cosine_distance_text(void* ctx, const DrakenVector* const* args,
                                      uint32_t nargs) {
    return cosine_text_kernel(ctx, args, nargs, /*as_distance=*/true,
                              "draken_cosine_distance");
}

// `_match_against_2` is the catalog OVERLOAD ID lowercased (_MATCH_AGAINST_2), so the
// registry name carries the leading underscore: draken + _match_against_2.
VecResult draken__match_against_2(void* ctx, const DrakenVector* const* args,
                                  uint32_t nargs) {
    return match_against_kernel(ctx, args, nargs, "draken_match_against");
}

}  // extern "C"
