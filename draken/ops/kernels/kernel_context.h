#pragma once

/**
 * Kernel context structures for parameterized kernels in Phase 9.
 *
 * Context structs are passed as void* ctx to C ABI kernels. Each kernel family
 * that needs parameterization (cast unit, binary op code, etc.) has a context
 * struct defined here. Context lifetime ≥ CompiledBytecode; held in _held_refs.
 */

#include <cstdint>
#include <cstddef>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Context for BC_CAST with parameterized unit (TIMESTAMP[ns], TIMESTAMP[us], etc.).
 * Used by vector_cast_int64_to_timestamp when unit is specified.
 */
struct cast_timestamp_ctx {
    int unit;  // 0=none, 1=ns, 2=us, 3=ms, 4=s, 5=days
};

/**
 * Context for BC_CAST with DECIMAL(precision, scale) constraint.
 * Used by _decimal_cast kernels.
 */
struct cast_decimal_ctx {
    int32_t precision;  // e.g., 38
    int32_t scale;      // e.g., 6
};

/**
 * Context for BC_CAST to ARRAY(element_type).
 * Stores the target element type for array cast.
 */
struct cast_array_ctx {
    int element_type;  // DrakenType enum value
};

/**
 * Context for BC_CAST to VARCHAR(length).
 * Stores the max length constraint.
 */
struct cast_varchar_ctx {
    int32_t max_length;
};

/**
 * Context for BC_BINARY_OP arithmetic operations.
 * Stores the operation code (BOP_PLUS, BOP_MINUS, etc.) to dispatch.
 */
struct binary_op_ctx {
    int op_code;  // BOP_PLUS, BOP_MINUS, BOP_MULTIPLY, BOP_DIVIDE, BOP_MODULO
    // DECIMAL/DECIMAL128 only (P9.1b): operand + result scales, supplied by the
    // binder (DrakenVector carries no scale — it lives on the LogicalType
    // descriptor at bind time). Zero for non-decimal ops (aggregate init `{op}`).
    unsigned char left_scale;
    unsigned char right_scale;
    unsigned char result_scale;  // dec_div result scale = max(sa+6,6) capped 18
    unsigned char result_precision;  // DECIMAL/DECIMAL128 result precision (descriptor)
    // TIMESTAMP/TIME only (S-A.2): TimestampUnit (0=s,1=ms,2=us,3=ns) of each
    // temporal operand, supplied by the binder (the unit is a LogicalType detail,
    // not on the physical DrakenVector). Zero for non-temporal ops. date32 operands
    // ignore the unit (days); only the TIMESTAMP64 side's unit is read.
    unsigned char left_unit;
    unsigned char right_unit;
};

/**
 * Context for BC_EXTRACTION operations.
 *
 * Carries everything the extraction kernels need that is known at bind time, so
 * the C ABI's `key` operand is unused: BC_EXTRACTION pops exactly one vector.
 *
 * The navigation path is stored as `nav_len` bytes placed IMMEDIATELY AFTER this
 * struct in the same malloc block (see kernel_alloc_extraction_ctx), which keeps
 * the generic kernel_free_context() -> free(ctx) correct. For JSON sub-ops the
 * bytes are the RFC 6901 pointer ALREADY converted from dot-notation, so
 * dotpath_to_jsonptr runs once per bind rather than once per morsel.
 */
struct extraction_ctx {
    int32_t sub_op_code;  // BC_EXTR_MAP_STRING, BC_EXTR_MAP_ARRAY, BC_EXTR_JSON_PTR, BC_EXTR_JSON_KEY
    int32_t nav_len;      // bytes of path/key following this struct (0 = none)
    int64_t index;        // subscript for BC_EXTR_MAP_STRING / BC_EXTR_MAP_ARRAY
};

/* Path/key bytes trailing the struct. NOT NUL-terminated — pair with nav_len. */
static inline const char* extraction_ctx_nav(const struct extraction_ctx* c) {
    return (const char*)((const unsigned char*)c + sizeof(struct extraction_ctx));
}

/**
 * Context for BC_CASE (not in 9a scope, but defined for completeness).
 * Stores compiled bytecode branches and condition arrays for case evaluation.
 */
struct case_ctx {
    void** cond_bcs;       // Array of CompiledBytecode* for conditions
    uint32_t n_conds;      // Number of condition branches
    void** result_bcs;     // Array of CompiledBytecode* for results
    void* else_bc;         // CompiledBytecode* for else branch (may be NULL)
    int assemble_kind;     // AssembleKind enum (how to assemble result)
};

/**
 * Context for draken_in_list — bind-time membership set, allocated by copying
 * a Python-built blob whose first bytes ARE this header:
 *   [u32 count][u8 kind][u8 negate][u16 pad][payload...]
 * kind 0: count x int64 SORTED ASCENDING (int family raw values; DECIMAL raw
 *         quantized to the column's scale at bind time).
 * kind 1: count x (u32 len + bytes) — UTF-8/ASCII string entries.
 * kind 2: count x float64 (IEEE754 double), in GIVEN order (NOT sorted —
 *         draken_in_list does not binary-search this kind). Consumed today
 *         only by draken_array_contains (function_array_json.cpp), which
 *         always packs a single entry; draken_in_list has no kind-2 arm.
 * The list never contains NULL (the plan compiler rejects those lists).
 */
struct in_list_ctx {
    uint32_t count;
    uint8_t  kind;
    uint8_t  negate;
    uint16_t _pad;
    /* payload bytes follow the struct inline */
};

struct in_list_ctx* kernel_alloc_in_list_ctx(const uint8_t* blob, size_t blob_len);

/**
 * Context for draken_substring — SUBSTRING(str, start, count). `start` is 1-based
 * (SQL); Python-slice semantics apply after `start -= 1` (when start > 0). When
 * has_count is 0 the substring runs to the end of the string.
 */
struct substring_ctx {
    int32_t start;
    int32_t count;
    uint8_t has_count;
};

struct substring_ctx* kernel_alloc_substring_ctx(int32_t start, int32_t count,
                                                 uint8_t has_count);

/**
 * Context for the length-adaptive LIKE kernel (draken_like_adaptive). Carries
 * the op mode, a per-column avg-string-length threshold, and a plan-time
 * compiled LIKE-DFA blob (opteryx.compiled.vector_ops.compile_like_dfa). At run
 * time the kernel estimates the column's average string length (sampled slot
 * lengths) and walks the DFA when it is below the threshold — the DFA wins on
 * SHORT strings (measured ~2.2x), the glob matcher wins on long ones. Both
 * matchers are verified byte-for-byte equivalent, so the length dispatch changes
 * only SPEED, never the answer (§11: a shape discriminant must not change the
 * result). The DFA blob (blob_len bytes) trails this struct.
 */
struct like_dfa_ctx {
    uint16_t op_code;    // bit0 negate, bit1 ci
    uint16_t threshold;  // avg string length (bytes) below which to use the DFA
    uint32_t blob_len;   // trailing LIKE-DFA blob length
};

static inline const uint8_t* like_dfa_ctx_blob(const struct like_dfa_ctx* c) {
    return (const uint8_t*)((const unsigned char*)c + sizeof(struct like_dfa_ctx));
}

/**
 * Context for draken_time_bucket — TIME_BUCKET(magnitude, units, date).
 * magnitude/units are bind-time (literal) operands, consumed here rather than
 * pushed as vector operands; only the `date` operand is pushed. unit_kind
 * selects the bucket period: 1-4 are fixed-width (second/minute/hour/day),
 * 5 is week (7-day, ISO-Monday anchored), 6-8 are epoch-anchored calendar
 * buckets (month/quarter/year). ts_unit is a TIMESTAMP64 operand's TimestampUnit
 * (0=s,1=ms,2=us,3=ns), a LogicalType detail not carried on DrakenVector; a
 * DATE32 operand ignores it (the kernel works in microseconds).
 */
struct time_bucket_ctx {
    int64_t magnitude;
    uint8_t unit_kind;   // 1=second 2=minute 3=hour 4=day 5=week 6=month 7=quarter 8=year
    uint8_t ts_unit;
};

struct time_bucket_ctx* kernel_alloc_time_bucket_ctx(int64_t magnitude,
                                                      uint8_t unit_kind,
                                                      uint8_t ts_unit);

/**
 * Context for draken_date_format — DATE_FORMAT(date, pattern). `pattern` is a
 * LITERAL, consumed here (not pushed); only the `date` operand is pushed.
 * ts_unit: TimestampUnit of a TIMESTAMP64 operand (0=s,1=ms,2=us,3=ns); DATE32
 * operands pass 2 (unused by the kernel, which switches on the operand's
 * DrakenType directly). fmt_len bytes of the pattern trail this struct
 * (same layout technique as extraction_ctx's nav bytes) — NOT NUL-terminated.
 */
struct format_ctx {
    uint8_t ts_unit;
    int32_t fmt_len;
};

static inline const char* format_ctx_fmt(const struct format_ctx* c) {
    return (const char*)((const unsigned char*)c + sizeof(struct format_ctx));
}

struct format_ctx* kernel_alloc_format_ctx(uint8_t ts_unit, const char* fmt,
                                           size_t fmt_len);

/**
 * Context for the VECTOR_FP16 cosine kernels (draken_cosine_{similarity,distance}_vector).
 *
 * dimension: width of both operands, supplied by the binder. A VECTOR's width is a
 * LogicalType detail and is NOT on the physical DrakenVector, so the kernel has no way
 * to recover it from its operands — exactly the wall binary_op_ctx's scale/unit fields
 * exist to cross. Deliberately its own struct rather than a reused binary_op_ctx field:
 * a dimension is neither a scale nor a TimestampUnit, and overloading one of those
 * would be a silent misread waiting to happen.
 */
struct vector_dim_ctx {
    uint32_t dimension;
};

struct vector_dim_ctx* kernel_alloc_vector_dim_ctx(uint32_t dimension);

/**
 * Context for the TEXT overloads (draken_cosine_{similarity,distance}_text).
 *
 * COSINE_SIMILARITY(a, b) over strings means "embed both, then compare" — the same
 * question as COSINE_SIMILARITY(EMBED(a), EMBED(b)). So it must use the SAME embedder,
 * and `embed_fn` is the bind-time-resolved `draken_embed` — whichever kernel the active
 * EMBED capability registered. The text kernel does not embed for itself: a second
 * embedding implementation here would be duplicated logic that silently disagrees the
 * moment a capability replaces the core one (observed: with MiniLM installed,
 * COSINE_SIMILARITY('dog','puppy') answered 0.0 lexically while the EMBED composition
 * answered 0.80).
 *
 * `dimension` is the active capability's width, passed to embed_fn in a stack-local
 * vector_dim_ctx — no nested ctx ownership, so kernel_free_context stays a single free().
 */
struct cosine_text_ctx {
    uint32_t dimension;
    void*    embed_fn;   /* func_fn_t: VecResult (*)(void*, const DrakenVector* const*, uint32_t) */
};

struct cosine_text_ctx* kernel_alloc_cosine_text_ctx(uint32_t dimension, void* embed_fn);

/**
 * Context for draken__match_against_2 (SQL `MATCH (col) AGAINST (str)`).
 *
 * MATCH is defined as `cosine_similarity(col, str) >= threshold`, so the first two fields
 * are cosine_text_ctx's and carry the same meaning — the kernel builds a cosine_text_ctx
 * from them and calls the SAME body the text cosine overloads use. MATCH therefore cannot
 * disagree with COSINE_SIMILARITY on the same inputs: it is that function, thresholded.
 *
 * `threshold` is resolved at BIND time from the `match_threshold` session variable, not
 * read at execution time: a compiled plan must keep answering the question it was compiled
 * for. It is only meaningful relative to the ACTIVE embedder — two embedders' score
 * distributions are not comparable — which is why it is tunable rather than a constant.
 *
 * A NaN similarity (a zero-magnitude embedding: empty or stopword-only text) fails the
 * `>=` and yields false. That is intended: an undefined direction is not a match.
 */
struct match_ctx {
    uint32_t dimension;
    void*    embed_fn;   /* func_fn_t: VecResult (*)(void*, const DrakenVector* const*, uint32_t) */
    double   threshold;
};

struct match_ctx* kernel_alloc_match_ctx(uint32_t dimension, void* embed_fn, double threshold);

#ifdef __cplusplus
}
#endif
