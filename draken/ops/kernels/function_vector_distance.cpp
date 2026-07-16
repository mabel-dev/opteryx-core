// draken/ops/kernels/function_vector_distance.cpp — vector/distance function kernels.
//
// Kernels:
//   draken_embed                      VARCHAR            -> VECTOR_FP16(ctx dimension)
//   draken_cosine_similarity_vector   (VEC_FP16, VEC_FP16) -> FLOAT64
//   draken_cosine_distance_vector     (VEC_FP16, VEC_FP16) -> FLOAT64
//   draken_cosine_similarity_text     (VARCHAR, VARCHAR)   -> FLOAT64
//   draken_cosine_distance_text       (VARCHAR, VARCHAR)   -> FLOAT64
//
// The `_vector`/`_text` suffixes are the catalog OVERLOAD IDs lowercased
// (COSINE_SIMILARITY_VECTOR -> draken_cosine_similarity_vector). compiled_expression.pyx
// probes `draken_{overload_id}` before the bare `draken_{name}`, so the two overloads of
// one SQL name reach two different kernels. The bare `draken_cosine_similarity` /
// `draken_cosine_distance` names are deliberately NOT registered: a name-level hit would
// bind the generic arm (all operands, no ctx) and defeat the overload split.
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
#include "ops/vector_cosine.h"
#include "xxhash.h"  // XXH3_64bits — must match opteryx xxhash.pyx hash_bytes exactly.

namespace {

// The C function-kernel ABI (c_kernel_abi.h) — the shape of the EMBED kernel the text
// overloads delegate to.
typedef VecResult (*func_fn_t)(void* ctx, const DrakenVector* const* args, uint32_t nargs);


// NOTE: this file holds NO embedding-width constant on purpose. The width is decided by
// the active EMBED capability (opteryx/types/vectors/embedding_capability.py), declared
// into the plan as EMBED's VECTOR(n) return type, and handed to every kernel here in a
// vector_dim_ctx. A constant duplicated here could disagree with the plan's declared
// type, and the projection boundary copies rows at the DECLARED stride — so a
// disagreement would read the wrong bytes rather than raise. One number, one source.

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
// Mirrors cosine_sim_fp16's math exactly, including zero-norm -> NaN.
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
        double dot = 0.0, sq_a = 0.0, sq_b = 0.0;
        for (uint32_t k = 0; k < dims; ++k) {
            const double fa = static_cast<double>(fp16_ieee_to_fp32_value(ra[k]));
            const double fb = static_cast<double>(fp16_ieee_to_fp32_value(rb[k]));
            dot += fa * fb; sq_a += fa * fa; sq_b += fb * fb;
        }
        const double denom = std::sqrt(sq_a) * std::sqrt(sq_b);
        double sim = (denom == 0.0) ? std::numeric_limits<double>::quiet_NaN() : dot / denom;
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

// Shared body for the two vector overloads. `ctx` carries the bind-time dimension —
// DrakenVector has no dimension field (it is a LogicalType detail), so the kernel
// cannot recover it from its operands.
VecResult cosine_vector_kernel(void* ctx, const DrakenVector* const* args, uint32_t nargs,
                               bool as_distance, const char* who) {
    if (nargs != 2u) return draken_error_sentinel_fmt("%s: expected 2 arguments", who);
    if (ctx == nullptr)
        return draken_error_sentinel_fmt("%s: missing vector dimension context", who);
    const uint32_t dims = static_cast<const struct vector_dim_ctx*>(ctx)->dimension;
    if (dims == 0u)
        return draken_error_sentinel_fmt("%s: vector dimension must be >= 1", who);
    try {
        VecResult r = draken::ops::cosine_sim_fp16(*args[0], *args[1], dims);
        if (!as_distance || r.data == nullptr) return r;
        double* d = static_cast<double*>(r.data);
        for (uint32_t i = 0; i < r.length; ++i) {
            double s = d[i];
            if (s < -1.0) s = -1.0; else if (s > 1.0) s = 1.0;
            d[i] = 1.0 - s;
        }
        return r;
    } catch (const std::exception& e) {
        return draken_error_sentinel_fmt("%s: %s", who, e.what());
    }
}


// ---------------------------------------------------------------------------
// CAST(array AS VECTOR(n)) — draken_cast_array_to_vector
// ---------------------------------------------------------------------------
// An ARRAY's elements do NOT live in `parent->data` (which holds only the
// int32 offsets[k+1]); they hang off the column owner's child vector, reachable only
// via the BC_C_NATIVE_CHILD two-vector dispatch. Same wall, same mechanism, as
// draken_cast_array_to_varchar.
//
// Width comes from the DECLARED type (vector_dim_ctx): an ARRAY column's row lengths
// vary per row and are not knowable at bind time, so `CAST(x AS VECTOR)` with no
// dimension is rejected at bind — the width has to be stated.
//
// Reads one double per element regardless of the child's numeric type, then packs fp16.
inline double vd_child_elem_as_double(const DrakenVector* child, uint32_t e) {
    const uint32_t phys = child->selection[e];
    switch (child->type) {
        case DRAKEN_INT8:    return static_cast<double>(static_cast<const int8_t*>(child->data)[phys]);
        case DRAKEN_INT16:   return static_cast<double>(static_cast<const int16_t*>(child->data)[phys]);
        case DRAKEN_INT32:   return static_cast<double>(static_cast<const int32_t*>(child->data)[phys]);
        case DRAKEN_INT64:   return static_cast<double>(static_cast<const int64_t*>(child->data)[phys]);
        case DRAKEN_UINT8:   return static_cast<double>(static_cast<const uint8_t*>(child->data)[phys]);
        case DRAKEN_UINT16:  return static_cast<double>(static_cast<const uint16_t*>(child->data)[phys]);
        case DRAKEN_UINT32:  return static_cast<double>(static_cast<const uint32_t*>(child->data)[phys]);
        case DRAKEN_UINT64:  return static_cast<double>(static_cast<const uint64_t*>(child->data)[phys]);
        case DRAKEN_FLOAT32: return static_cast<double>(static_cast<const float*>(child->data)[phys]);
        case DRAKEN_FLOAT64: return static_cast<const double*>(child->data)[phys];
        default:
            throw std::runtime_error(
                "CAST to VECTOR: array elements must be numeric — fail loud, never a "
                "silent wrong vector");
    }
}

inline bool vd_child_elem_valid(const DrakenVector* child, uint32_t e) {
    return child->validity == nullptr || ((child->validity[e >> 3] >> (e & 7u)) & 1u);
}

VecResult cast_array_to_vector_core(void* ctx, const DrakenVector* parent,
                                    const DrakenVector* child) {
    if (!parent || !child)
        return draken_error_sentinel("CAST to VECTOR: null input vector");
    if (parent->type != DRAKEN_ARRAY)
        return draken_error_sentinel_fmt(
            "CAST to VECTOR: expected ARRAY operand, got %d", parent->type);
    if (ctx == nullptr)
        return draken_error_sentinel("CAST to VECTOR: missing vector dimension context");
    const uint32_t dims = static_cast<const struct vector_dim_ctx*>(ctx)->dimension;
    if (dims == 0u || dims > 65535u)
        return draken_error_sentinel("CAST to VECTOR: dimension must be 1..65535");

    const uint32_t k = parent->data_length;          // physical rows (offset pairs)
    const int32_t* offsets = static_cast<const int32_t*>(parent->data);

    const size_t cells = static_cast<size_t>(k > 0u ? k : 1u) * dims;
    uint16_t* data = static_cast<uint16_t*>(draken_malloc(cells * sizeof(uint16_t)));
    if (!data) return draken_error_sentinel("CAST to VECTOR: allocation failed");
    std::memset(data, 0, cells * sizeof(uint16_t));

    // Convert the k PHYSICAL rows (the shape-preserving idiom every array/string kernel
    // here uses). A physical row can be referenced by BOTH null and non-null logical
    // rows, and a NULL logical row's offsets are typically an empty range — so a
    // malformed physical row must NOT raise on its own. Record it, and only fail if a
    // VALID logical row actually reads it. Erroring eagerly would reject
    // `CAST(arr AS VECTOR(2))` on a table whose null rows happen to hold no elements.
    std::vector<uint8_t> bad(k > 0u ? k : 1u, 0u);
    try {
        for (uint32_t j = 0; j < k; ++j) {
            const int32_t start = offsets[j];
            const int32_t end   = offsets[j + 1];
            if (end < start || static_cast<uint32_t>(end - start) != dims) { bad[j] = 1u; continue; }
            uint16_t* dst = data + static_cast<size_t>(j) * dims;
            for (uint32_t d = 0; d < dims; ++d) {
                const uint32_t e = static_cast<uint32_t>(start) + d;
                // A null element leaves the vector's direction undefined; there is no
                // honest fp16 for it, and 0.0 would silently move the vector.
                if (!vd_child_elem_valid(child, e)) { bad[j] = 1u; break; }
                dst[d] = fp16_ieee_from_fp32_value(
                    static_cast<float>(vd_child_elem_as_double(child, e)));
            }
        }
    } catch (const std::exception& e) {
        draken_free(data);
        return draken_error_sentinel_fmt("CAST to VECTOR: %s", e.what());
    }

    for (uint32_t i = 0; i < parent->length; ++i) {
        if (!vd_row_valid(parent->validity, i)) continue;
        if (bad[parent->selection[i]]) {
            draken_free(data);
            return draken_error_sentinel_fmt(
                "CAST to VECTOR: row %u is not a %u-element numeric array", i, dims);
        }
    }

    VecResult r;
    r.data = data;
    r.type = DRAKEN_VECTOR_FP16;
    try {
        kernel_preserve_shape(r, parent);
    } catch (const std::exception& e) {
        draken_free(data);
        return draken_error_sentinel_fmt("CAST to VECTOR: %s", e.what());
    }
    r.vec_dimension = static_cast<uint16_t>(dims);
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

struct vector_dim_ctx* kernel_alloc_vector_dim_ctx(uint32_t dimension) {
    auto* c = static_cast<struct vector_dim_ctx*>(malloc(sizeof(struct vector_dim_ctx)));
    if (!c) return nullptr;
    c->dimension = dimension;
    return c;
}

VecResult draken_cast_array_to_vector(void* ctx, const DrakenVector* parent,
                                      const DrakenVector* child) {
    return cast_array_to_vector_core(ctx, parent, child);
}

VecResult draken_embed(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1u) return draken_error_sentinel("draken_embed: expected 1 argument");
    const DrakenVector* v = args[0];
    if (!vd_is_string(v->type))
        return draken_error_sentinel("draken_embed: string operand required");

    // The binder hands down the width it DECLARED for this call (EMBED's return type
    // is VECTOR(n)), and this kernel produces exactly that width. The width is not
    // duplicated as a constant on both sides: the declaration is the single source of
    // truth, so the plan's type and the kernel's output cannot disagree — a
    // disagreement would make the projection boundary copy the wrong stride.
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

VecResult draken_cosine_similarity_vector(void* ctx, const DrakenVector* const* args,
                                          uint32_t nargs) {
    return cosine_vector_kernel(ctx, args, nargs, /*as_distance=*/false,
                                "draken_cosine_similarity");
}

VecResult draken_cosine_distance_vector(void* ctx, const DrakenVector* const* args,
                                        uint32_t nargs) {
    return cosine_vector_kernel(ctx, args, nargs, /*as_distance=*/true,
                                "draken_cosine_distance");
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

}  // extern "C"
