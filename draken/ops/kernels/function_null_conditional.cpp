// draken/ops/kernels/function_null_conditional.cpp — null & conditional function
// kernels on the C ABI:
//
//     VecResult fn(void* ctx, const DrakenVector* const* args, uint32_t nargs)
//
//   draken_coalesce(a, b, ...)   — first non-null across N branches
//   draken_ifnull(v, default)    — v when v is not null, else default
//   draken_ifnotnull(v, result)  — result when v is not null, else null
//   draken_iif(cond, t, f)       — cond selects a branch; null cond selects f
//
// NULLIF needs no kernel: the logical planner lowers NULLIF(a, b) to
// IIF(a = b, NULL, a) before binding (logical_planner_builders.py), so it lands
// on draken_iif via the NULL-branch path below. A draken_nullif entry would be
// unreachable.
//
// Dispatched from the nogil DV* VM — no Python, no nanobind, no GIL. Before this
// file existed these four had no C kernel, so the plan compiler REFUSED every
// query using them ("outside the c-native kernel set"); the nanobind
// vector_coalesce/vector_iif/... they nominally bound to were unreachable from the
// engine. This is their sole implementation; the semantics below are carried over
// from those (now deleted) bindings.
//
// SHAPE: reads are uniform data[selection[i]] over every input (CLAUDE.md §11) —
// no shape discrimination. Output is dense: which branch supplies a row is a
// per-LOGICAL-ROW decision, so a shared input dictionary carries no meaning into
// the result (unlike the shape-preserving string transforms, where the output is a
// pure function of one physical value).
//
// NULL SEMANTICS (the whole game here — see the family-specific notes):
//   - A DRAKEN_NULL-typed branch is a typed NULL literal: "no data, no validity",
//     every row null. nc_row_valid returns false for it UNCONDITIONALLY and its
//     data/selection are never dereferenced. This is what makes the NULLIF
//     lowering (IIF(a = b, NULL, a)) work without a synthetic branch vector.
//   - IIF: a NULL condition row selects the FALSE branch (SQL CASE WHEN NULL).
//   - The output row's validity is the CHOSEN branch's validity; a row with no
//     chosen branch is null.
//
// DECIMAL is deliberately rejected: DrakenVector carries no scale (it is an
// out-of-band logical descriptor, buffers.h), so a kernel cannot see that two
// DECIMAL branches differ in scale, and blending raw unscaled int64s across
// differing scales is silently wrong. CASE gets away with the same raw blend only
// because the plan compiler quantizes its literal branches first; COALESCE/IIF
// have no such rewrite. Failing loud keeps these queries refused exactly as they
// are today rather than answering them incorrectly.

#include <cstdint>
#include <cstring>

#include "core/buffers.h"
#include "core/string_slot.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"     // draken_identity_sel
#include "ops/vec_result.h"
#include "ops/kernels/result_helpers.h"
#include "ops/kernels/error_handling.h"
#include "xxhash.h"                // XXH3_64bits — long-slot hash32, as every builder

namespace {

// ---------------------------------------------------------------------------
// Type predicates / promotion
// ---------------------------------------------------------------------------

inline bool nc_is_bool(DrakenType t) { return t == DRAKEN_BOOL; }

inline bool nc_is_string(DrakenType t) {
    return t == DRAKEN_VARCHAR || t == DRAKEN_NVARCHAR || t == DRAKEN_VARBINARY;
}

// Byte width of the fixed-width types this file blends. DECIMAL/DECIMAL128 are
// absent on purpose (see the header note) and report 0 == "not supported here".
inline size_t nc_fixed_size(DrakenType t) {
    switch (t) {
        case DRAKEN_INT8:  case DRAKEN_UINT8:                       return 1;
        case DRAKEN_INT16: case DRAKEN_UINT16:                      return 2;
        case DRAKEN_INT32: case DRAKEN_UINT32:
        case DRAKEN_FLOAT32: case DRAKEN_DATE32: case DRAKEN_TIME32: return 4;
        case DRAKEN_INT64: case DRAKEN_UINT64:
        case DRAKEN_FLOAT64: case DRAKEN_TIMESTAMP64:
        case DRAKEN_TIME64:                                          return 8;
        default:                                                     return 0;
    }
}

inline bool nc_is_fixed(DrakenType t) { return nc_fixed_size(t) != 0; }

inline bool nc_is_signed_int(DrakenType t) {
    return t == DRAKEN_INT8 || t == DRAKEN_INT16 ||
           t == DRAKEN_INT32 || t == DRAKEN_INT64;
}

inline bool nc_is_float(DrakenType t) {
    return t == DRAKEN_FLOAT32 || t == DRAKEN_FLOAT64;
}

// The binder widens narrow ints to INT64 for IIF/COALESCE/IFNULL results, and the
// DECLARED type drives downstream cast-kernel selection — so the produced vector
// must be INT64 too, or the plan and the data disagree. DATE32/TIME32 are also
// 4-byte but are distinct tags and pass through.
inline DrakenType nc_canon_fixed(DrakenType t) {
    if (t == DRAKEN_INT8 || t == DRAKEN_INT16 || t == DRAKEN_INT32) return DRAKEN_INT64;
    return t;
}

// Common output type for two fixed-width branches, mirroring the binder's
// find_compatible_type: any two signed ints widen to INT64; an int/float mix to
// FLOAT64. Equal types pass through. DRAKEN_NULL == "cannot promote" (caller fails
// loud): temporal/unsigned cross-family scaling is genuinely ambiguous.
inline bool nc_is_unsigned_int(DrakenType t) {
    return t == DRAKEN_UINT8 || t == DRAKEN_UINT16 ||
           t == DRAKEN_UINT32 || t == DRAKEN_UINT64;
}

inline DrakenType nc_promote_fixed(DrakenType a, DrakenType b) {
    if (a == b) return nc_canon_fixed(a);
    if (nc_is_signed_int(a) && nc_is_signed_int(b)) return DRAKEN_INT64;
    // Two unsigned widths widen to the WIDER of the two, not to INT64: INT64
    // cannot hold the top half of UINT64, so the signed rule above would turn
    // a large address or id into a negative number. Widening unsigned->unsigned
    // is always exact (zero-extension), so no value can be lost. Unlike the
    // signed rule this does NOT canonicalise to the widest tier — nc_canon_fixed
    // passes unsigned through, so a UINT8 pair stays UINT8 and a {UINT8, UINT32}
    // pair becomes UINT32.
    if (nc_is_unsigned_int(a) && nc_is_unsigned_int(b))
        return nc_fixed_size(a) >= nc_fixed_size(b) ? a : b;
    if ((nc_is_signed_int(a) || nc_is_float(a)) &&
        (nc_is_signed_int(b) || nc_is_float(b))) return DRAKEN_FLOAT64;
    // A signed/unsigned mix stays unpromotable on purpose: no fixed-width type
    // holds both negative values and the top half of UINT64, so any choice here
    // would silently corrupt one side. The caller fails loud and the user CASTs.
    return DRAKEN_NULL;
}

inline DrakenType nc_canon_string(DrakenType t) {
    if (t == DRAKEN_NVARCHAR)  return DRAKEN_NVARCHAR;
    if (t == DRAKEN_VARBINARY) return DRAKEN_VARBINARY;
    return DRAKEN_VARCHAR;
}

// VARBINARY beats NVARCHAR beats VARCHAR. (Mixing opaque bytes with text is
// nonsense, but it is the pre-existing promotion and no caller can reach it: the
// binder rejects the pair before a kernel ever sees it.)
inline DrakenType nc_promote_string(DrakenType a, DrakenType b) {
    const DrakenType ca = nc_canon_string(a), cb = nc_canon_string(b);
    if (ca == DRAKEN_VARBINARY || cb == DRAKEN_VARBINARY) return DRAKEN_VARBINARY;
    if (ca == DRAKEN_NVARCHAR  || cb == DRAKEN_NVARCHAR)  return DRAKEN_NVARCHAR;
    return DRAKEN_VARCHAR;
}

// ---------------------------------------------------------------------------
// Row access — uniform data[selection[i]]
// ---------------------------------------------------------------------------

// A DRAKEN_NULL branch is all-null by contract and has no buffers to read, so it
// short-circuits to "invalid" before any dereference.
inline bool nc_row_valid(const DrakenVector* v, uint32_t row) {
    if (v->type == DRAKEN_NULL) return false;
    if (!v->validity) return true;
    return ((v->validity[row >> 3] >> (row & 7u)) & 1u) != 0u;
}

inline void nc_set_bit(uint8_t* bits, uint32_t i) {
    bits[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
}

inline bool nc_read_bool(const DrakenVector* v, uint32_t row) {
    const uint32_t c = v->selection[row];
    return ((static_cast<const uint8_t*>(v->data)[c >> 3] >> (c & 7u)) & 1u) != 0u;
}

inline int64_t nc_read_int(const DrakenVector* v, uint32_t row) {
    const uint8_t* p = static_cast<const uint8_t*>(v->data)
        + static_cast<size_t>(v->selection[row]) * nc_fixed_size(v->type);
    switch (v->type) {
        case DRAKEN_INT8:  return *reinterpret_cast<const int8_t*>(p);
        case DRAKEN_INT16: return *reinterpret_cast<const int16_t*>(p);
        case DRAKEN_INT32: return *reinterpret_cast<const int32_t*>(p);
        case DRAKEN_INT64: return *reinterpret_cast<const int64_t*>(p);
        default:           return 0;   // unreachable: nc_promote_fixed gates entry
    }
}

// Unsigned twin of nc_read_int. Separate rather than folded in because the two
// cannot share a return type: UINT64 values above INT64_MAX have no int64
// representation, which is the whole reason the unsigned family promotes among
// itself instead of through INT64.
inline uint64_t nc_read_uint(const DrakenVector* v, uint32_t row) {
    const uint8_t* p = static_cast<const uint8_t*>(v->data)
        + static_cast<size_t>(v->selection[row]) * nc_fixed_size(v->type);
    switch (v->type) {
        case DRAKEN_UINT8:  return *p;
        case DRAKEN_UINT16: return *reinterpret_cast<const uint16_t*>(p);
        case DRAKEN_UINT32: return *reinterpret_cast<const uint32_t*>(p);
        case DRAKEN_UINT64: return *reinterpret_cast<const uint64_t*>(p);
        default:            return 0;   // unreachable: nc_promote_fixed gates entry
    }
}

inline double nc_read_double(const DrakenVector* v, uint32_t row) {
    const uint8_t* p = static_cast<const uint8_t*>(v->data)
        + static_cast<size_t>(v->selection[row]) * nc_fixed_size(v->type);
    if (v->type == DRAKEN_FLOAT32) return static_cast<double>(*reinterpret_cast<const float*>(p));
    if (v->type == DRAKEN_FLOAT64) return *reinterpret_cast<const double*>(p);
    return static_cast<double>(nc_read_int(v, row));
}

// Write one source row at `out_type`. Same-type rows memcpy (the common case);
// only a promoted branch pays a conversion.
inline void nc_write_fixed(uint8_t* out, uint32_t row, DrakenType out_type,
                           const DrakenVector* src) {
    const size_t osz = nc_fixed_size(out_type);
    uint8_t* dst = out + static_cast<size_t>(row) * osz;
    if (src->type == out_type) {
        std::memcpy(dst, static_cast<const uint8_t*>(src->data)
                        + static_cast<size_t>(src->selection[row]) * osz, osz);
        return;
    }
    if (out_type == DRAKEN_INT64) {
        const int64_t v = nc_read_int(src, row);
        std::memcpy(dst, &v, sizeof(int64_t));
    } else if (nc_is_unsigned_int(out_type)) {
        // Narrower unsigned source into a wider unsigned target: zero-extend.
        // nc_read_int must NOT be used — its switch has no unsigned arms and
        // would silently write 0. Written through the exact-width type rather
        // than "the low osz bytes of a uint64" so there is no endianness
        // assumption (§6). The promotion only ever widens, so the narrowing
        // casts below cannot lose a value.
        const uint64_t v = nc_read_uint(src, row);
        switch (out_type) {
            case DRAKEN_UINT8:  { const uint8_t  x = static_cast<uint8_t>(v);
                                  std::memcpy(dst, &x, 1); break; }
            case DRAKEN_UINT16: { const uint16_t x = static_cast<uint16_t>(v);
                                  std::memcpy(dst, &x, 2); break; }
            case DRAKEN_UINT32: { const uint32_t x = static_cast<uint32_t>(v);
                                  std::memcpy(dst, &x, 4); break; }
            default:            { std::memcpy(dst, &v, 8); break; }
        }
    } else {   // DRAKEN_FLOAT64 — the only other promotion target
        const double v = nc_read_double(src, row);
        std::memcpy(dst, &v, sizeof(double));
    }
}

struct NcStrView { const uint8_t* data; uint32_t len; };

inline NcStrView nc_read_string(const DrakenVector* v, uint32_t row) {
    const auto* sa = static_cast<const DrakenStringArena*>(v->data);
    const DrakenStringSlot* slot = &sa->slots[v->selection[row]];
    return { str_data(slot, sa->arena), str_length(slot) };
}

// ---------------------------------------------------------------------------
// Choosers — which branch supplies a row, or -1 for null.
//
// Compile-time functors: each blend below is instantiated per chooser, so the
// per-row decision inlines to a fixed test with no dynamic dispatch (§3).
// ---------------------------------------------------------------------------

// COALESCE / IFNULL: first branch valid at this row. (IFNULL(v, d) is exactly
// COALESCE(v, d); it gets its own registry entry, not its own loop.)
struct CoalesceChooser {
    static inline int pick(const DrakenVector* const* v, uint32_t nargs, uint32_t row) {
        for (uint32_t k = 0; k < nargs; ++k)
            if (nc_row_valid(v[k], row)) return static_cast<int>(k);
        return -1;
    }
};

// IIF(cond, t, f): args are [cond, t, f]. A null cond row selects f (SQL CASE
// WHEN NULL ... = the ELSE branch). The chosen branch's own validity then decides.
struct IifChooser {
    static inline int pick(const DrakenVector* const* v, uint32_t /*nargs*/, uint32_t row) {
        const int k = (nc_row_valid(v[0], row) && nc_read_bool(v[0], row)) ? 1 : 2;
        return nc_row_valid(v[k], row) ? k : -1;
    }
};

// IFNOTNULL(v, result): args are [v, result]. Null v → null; else result (which
// may itself be null at this row).
struct IfNotNullChooser {
    static inline int pick(const DrakenVector* const* v, uint32_t /*nargs*/, uint32_t row) {
        if (!nc_row_valid(v[0], row)) return -1;
        return nc_row_valid(v[1], row) ? 1 : -1;
    }
};

// ---------------------------------------------------------------------------
// Blends — one per family, chooser-parameterised.
//
// `srcs` is the vector set the chooser indexes into and the blend reads values
// from; for IIF it includes the condition at [0], which the chooser consumes and
// the value reads never touch (it only ever returns 1 or 2).
// ---------------------------------------------------------------------------

inline uint8_t* nc_alloc_validity(uint32_t n) {
    const uint32_t bm     = (n + 7u) >> 3;
    const uint32_t padded = (bm + 7u) & ~7u;
    const size_t   sz     = padded ? padded : 8u;
    uint8_t* v = static_cast<uint8_t*>(draken_malloc(sz));
    if (v) std::memset(v, 0, sz);   // 0 = null; valid rows set their bit
    return v;
}

// Dense identity result — the blend's output shape (see the SHAPE note above).
inline VecResult nc_dense_result(void* data, uint8_t* validity, uint32_t n, DrakenType t) {
    VecResult r{};
    r.data           = data;
    r.validity       = validity;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = t;
    r.flags          = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
    return r;
}

template <typename Chooser>
VecResult nc_blend_bool(const DrakenVector* const* srcs, uint32_t nargs, uint32_t n,
                        const char* who) {
    const uint32_t padded = ((((n + 7u) >> 3) + 7u) & ~7u);
    const size_t   sz     = padded ? padded : 8u;
    auto* bits = static_cast<uint8_t*>(draken_malloc(sz));
    if (!bits) return draken_error_sentinel_fmt("%s: allocation failed", who);
    std::memset(bits, 0, sz);

    uint8_t* validity = nc_alloc_validity(n);
    if (!validity) { draken_free(bits); return draken_error_sentinel_fmt("%s: allocation failed", who); }

    bool any_null = false;
    for (uint32_t row = 0; row < n; ++row) {
        const int k = Chooser::pick(srcs, nargs, row);
        if (k < 0) { any_null = true; continue; }
        if (nc_read_bool(srcs[k], row)) nc_set_bit(bits, row);
        nc_set_bit(validity, row);
    }
    // All-valid normalises to validity == nullptr (§00 data model).
    if (!any_null) { draken_free(validity); validity = nullptr; }
    return nc_dense_result(bits, validity, n, DRAKEN_BOOL);
}

template <typename Chooser>
VecResult nc_blend_fixed(const DrakenVector* const* srcs, uint32_t nargs, uint32_t n,
                         DrakenType out_type, const char* who) {
    const size_t isz   = nc_fixed_size(out_type);
    const size_t bytes = (n > 0 ? static_cast<size_t>(n) : 1u) * isz;
    auto* data = static_cast<uint8_t*>(draken_malloc(bytes));
    if (!data) return draken_error_sentinel_fmt("%s: allocation failed", who);
    std::memset(data, 0, bytes);

    uint8_t* validity = nc_alloc_validity(n);
    if (!validity) { draken_free(data); return draken_error_sentinel_fmt("%s: allocation failed", who); }

    bool any_null = false;
    for (uint32_t row = 0; row < n; ++row) {
        const int k = Chooser::pick(srcs, nargs, row);
        if (k < 0) { any_null = true; continue; }
        nc_write_fixed(data, row, out_type, srcs[k]);
        nc_set_bit(validity, row);
    }
    if (!any_null) { draken_free(validity); validity = nullptr; }
    return nc_dense_result(data, validity, n, out_type);
}

// Two passes: budget the long-form arena over the CHOSEN rows, then fill. The
// budget must use the same chooser as the fill, or the arena under-runs.
template <typename Chooser>
VecResult nc_blend_string(const DrakenVector* const* srcs, uint32_t nargs, uint32_t n,
                          DrakenType out_type, const char* who) {
    size_t arena_len = 0;
    for (uint32_t row = 0; row < n; ++row) {
        const int k = Chooser::pick(srcs, nargs, row);
        if (k < 0) continue;
        const uint32_t len = nc_read_string(srcs[k], row).len;
        if (len > STR_INLINE_MAX) arena_len += len;
    }

    DrakenStringSlot* slots;
    uint8_t* arena;
    uint8_t* validity;
    uint8_t* block = vecresult_string_block_alloc(n, arena_len, /*want_validity=*/1,
                                                  &slots, &arena, &validity);
    if (!block) return draken_error_sentinel_fmt("%s: allocation failed", who);
    // Block is zeroed: slots start null and validity starts all-null, so a row the
    // chooser rejects needs no write.

    size_t arena_pos = 0;
    for (uint32_t row = 0; row < n; ++row) {
        const int k = Chooser::pick(srcs, nargs, row);
        if (k < 0) { str_init_null(&slots[row]); continue; }
        const NcStrView sv = nc_read_string(srcs[k], row);
        if (sv.len <= STR_INLINE_MAX) {
            str_init_inline(&slots[row], sv.data, sv.len);
        } else {
            uint8_t* dst = arena + arena_pos;
            std::memcpy(dst, sv.data, sv.len);
            str_init_extern(&slots[row], dst, sv.len,
                            static_cast<uint32_t>(arena_pos));
            arena_pos += sv.len;
        }
        nc_set_bit(validity, row);
    }
    return vecresult_from_string_block(block, n, arena_len, /*has_validity=*/1, out_type);
}

// ---------------------------------------------------------------------------
// Family dispatch — shared by all four entry points.
//
// `vals` are the branches whose TYPES decide the output type (the IIF condition
// is excluded); `srcs`/`nargs` are what the chooser indexes.
// ---------------------------------------------------------------------------

template <typename Chooser>
VecResult nc_dispatch(const DrakenVector* const* srcs, uint32_t nargs,
                      const DrakenVector* const* vals, uint32_t nvals,
                      uint32_t n, const char* who) {
    // A branch may be a typed NULL literal (all rows null, no type to contribute);
    // the output type comes from the real branches. Every branch NULL has no type
    // at all to adopt — the binder narrows that at bind time and never emits it.
    DrakenType t0 = DRAKEN_NULL;
    for (uint32_t i = 0; i < nvals; ++i) {
        if (vals[i]->type != DRAKEN_NULL) { t0 = vals[i]->type; break; }
    }
    if (t0 == DRAKEN_NULL)
        return draken_error_sentinel_fmt("%s: every branch is NULL — no result type", who);

    if (nc_is_bool(t0)) {
        for (uint32_t i = 0; i < nvals; ++i) {
            const DrakenType ti = vals[i]->type;
            if (ti != DRAKEN_NULL && !nc_is_bool(ti))
                return draken_error_sentinel_fmt(
                    "%s: branch %u type %d is not BOOL", who, i, (int)ti);
        }
        return nc_blend_bool<Chooser>(srcs, nargs, n, who);
    }

    if (nc_is_string(t0)) {
        DrakenType out_type = nc_canon_string(t0);
        for (uint32_t i = 0; i < nvals; ++i) {
            const DrakenType ti = vals[i]->type;
            if (ti == DRAKEN_NULL) continue;
            if (!nc_is_string(ti))
                return draken_error_sentinel_fmt(
                    "%s: branch %u type %d is not string-family", who, i, (int)ti);
            out_type = nc_promote_string(out_type, ti);
        }
        return nc_blend_string<Chooser>(srcs, nargs, n, out_type, who);
    }

    if (nc_is_fixed(t0)) {
        DrakenType out_type = t0;
        for (uint32_t i = 0; i < nvals; ++i) {
            const DrakenType ti = vals[i]->type;
            if (ti == DRAKEN_NULL) continue;
            const DrakenType p = nc_is_fixed(ti) ? nc_promote_fixed(out_type, ti)
                                                 : DRAKEN_NULL;
            if (p == DRAKEN_NULL)
                return draken_error_sentinel_fmt(
                    "%s: branch %u type %d cannot be promoted with type %d",
                    who, i, (int)ti, (int)t0);
            out_type = p;
        }
        return nc_blend_fixed<Chooser>(srcs, nargs, n, out_type, who);
    }

    // DECIMAL/DECIMAL128 land here: scale is out-of-band, so a correct blend is
    // not expressible on this signature (see the header note).
    return draken_error_sentinel_fmt("%s: unsupported branch type %d", who, (int)t0);
}

// Every branch must agree on logical row count.
inline bool nc_lengths_match(const DrakenVector* const* v, uint32_t nargs, uint32_t n) {
    for (uint32_t i = 0; i < nargs; ++i)
        if (v[i]->length != n) return false;
    return true;
}

}  // namespace

extern "C" {

VecResult draken_coalesce(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs < 2)
        return draken_error_sentinel("draken_coalesce: expected at least 2 arguments");
    const uint32_t n = args[0]->length;
    if (!nc_lengths_match(args, nargs, n))
        return draken_error_sentinel("draken_coalesce: branch length mismatch");
    return nc_dispatch<CoalesceChooser>(args, nargs, args, nargs, n, "draken_coalesce");
}

// IFNULL(v, default) is COALESCE(v, default) — same chooser, its own entry point
// because dispatch is by name.
VecResult draken_ifnull(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 2)
        return draken_error_sentinel("draken_ifnull: expected 2 arguments");
    const uint32_t n = args[0]->length;
    if (!nc_lengths_match(args, nargs, n))
        return draken_error_sentinel("draken_ifnull: branch length mismatch");
    return nc_dispatch<CoalesceChooser>(args, nargs, args, nargs, n, "draken_ifnull");
}

VecResult draken_ifnotnull(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 2)
        return draken_error_sentinel("draken_ifnotnull: expected 2 arguments");
    const uint32_t n = args[0]->length;
    if (!nc_lengths_match(args, nargs, n))
        return draken_error_sentinel("draken_ifnotnull: branch length mismatch");
    // Only `result` ever supplies a VALUE, but BOTH args decide the TYPE: the
    // binder declares IFNOTNULL's return via _coalesce_return_type over both
    // (registrar/__init__.pyx), so typing on `result` alone would produce INT64
    // where the plan declared FLOAT64 for IFNOTNULL(float_col, int_col) — the
    // declared type drives downstream cast selection, so they must agree.
    return nc_dispatch<IfNotNullChooser>(args, nargs, args, 2, n, "draken_ifnotnull");
}

VecResult draken_iif(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 3)
        return draken_error_sentinel("draken_iif: expected 3 arguments");
    if (args[0]->type != DRAKEN_BOOL)
        return draken_error_sentinel("draken_iif: condition must be BOOLEAN");
    const uint32_t n = args[0]->length;
    if (!nc_lengths_match(args, nargs, n))
        return draken_error_sentinel("draken_iif: branch length mismatch");
    // args[0] is the condition — the chooser reads it, the type dispatch must not.
    return nc_dispatch<IifChooser>(args, nargs, args + 1, 2, n, "draken_iif");
}

}  // extern "C"
