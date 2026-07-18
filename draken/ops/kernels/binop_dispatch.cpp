// draken/ops/kernels/binop_dispatch.cpp — unified binary-op dispatch (P9.1).
//
// See binop_kernels.h. P9.1a: integer arithmetic only. Everything else returns a
// loud error sentinel (no silent fallback) until its sub-stage lands; the live
// binop path is unchanged (this is not yet wired into the executor).

#include <new>        // std::bad_alloc / placement new — not reliably pulled in by <stdexcept> on stricter libc++
#include "ops/kernels/binop_kernels.h"
#include "ops/kernels/error_handling.h"
#include "ops/kernels/kernel_context.h"   // binary_op_ctx
#include "ops/fixed_int_ops.h"            // draken::ops::fi_int_arith, fi_combine_validity
#include "ops/decimal_arith.h"            // draken::ops::dec_add/sub/mul/div/mod
#include "ops/interval_ops.h"             // draken::ops::interval_add/sub (S-A.1)
#include "ops/int_bitwise.h"              // draken::ops::bitwise_and/or/xor/shl/shr
#include "ops/int64_compare.h"            // draken::ops::cmp_alloc_bool_buf (IP-in-CIDR result)
#include "ops/kernels/result_helpers.h"   // vecresult_from_string_buffers
#include "core/buffers.h"
#include "core/string_slot.h"             // DrakenStringSlot, draken_build_string_slot, str_data/length
#include "core/alloc.h"                   // draken_malloc
#include "core/vector_alloc.h"            // draken_identity_sel
#include <cmath>                          // std::fmod
#include <cstring>                        // std::memcpy / std::memset
#include <stdexcept>

// BCBinaryOpCode values (mirror compiled_expression.pxd / arithmetic_dv.h):
//   1=PLUS 2=MINUS 3=MULTIPLY 4=DIVIDE(true) 5=MODULO 6=INT_DIVIDE 7=STRING_CONCAT 8+=bitwise
#define BOP_PLUS          1
#define BOP_MINUS         2
#define BOP_MULTIPLY      3
#define BOP_DIVIDE        4
#define BOP_MODULO        5
#define BOP_INT_DIVIDE    6
#define BOP_STRING_CONCAT 7
#define BOP_BITWISE_OR    8
#define BOP_BITWISE_AND   9
#define BOP_BITWISE_XOR   10
#define BOP_SHIFT_LEFT    11
#define BOP_SHIFT_RIGHT   12

namespace {

// Read the i-th logical value of any numeric vector as double (int8/16/32/64,
// float32/64), via selection so all three shapes work uniformly.
inline double bd_read_f64(const DrakenVector& v, uint32_t i) {
    const uint32_t p = v.selection[i];
    switch (v.type) {
        case DRAKEN_INT8:    return static_cast<double>(static_cast<const int8_t*>(v.data)[p]);
        case DRAKEN_INT16:   return static_cast<double>(static_cast<const int16_t*>(v.data)[p]);
        case DRAKEN_INT32:   return static_cast<double>(static_cast<const int32_t*>(v.data)[p]);
        case DRAKEN_INT64:   return static_cast<double>(static_cast<const int64_t*>(v.data)[p]);
        case DRAKEN_FLOAT32: return static_cast<double>(static_cast<const float*>(v.data)[p]);
        case DRAKEN_FLOAT64: return static_cast<const double*>(v.data)[p];
        default: throw std::invalid_argument("bd_read_f64: non-numeric operand");
    }
}

inline double bd_apply_f64(int op, double x, double y) {
    switch (op) {
        case BOP_PLUS:     return x + y;
        case BOP_MINUS:    return x - y;
        case BOP_MULTIPLY: return x * y;
        case BOP_DIVIDE:   return x / y;           // true divide; x/0 → ±inf, 0/0 → nan (IEEE, matches DuckDB)
        case BOP_MODULO:   return std::fmod(x, y);
        default: throw std::invalid_argument("bd_apply_f64: unsupported op");
    }
}

// Read the i-th logical value of an (int8/16/32/64 | float32) vector as float.
// FLOAT64 never reaches the float32 path (it routes to the FLOAT64 path first).
inline float bd_read_f32(const DrakenVector& v, uint32_t i) {
    const uint32_t p = v.selection[i];
    switch (v.type) {
        case DRAKEN_INT8:    return static_cast<float>(static_cast<const int8_t*>(v.data)[p]);
        case DRAKEN_INT16:   return static_cast<float>(static_cast<const int16_t*>(v.data)[p]);
        case DRAKEN_INT32:   return static_cast<float>(static_cast<const int32_t*>(v.data)[p]);
        case DRAKEN_INT64:   return static_cast<float>(static_cast<const int64_t*>(v.data)[p]);
        case DRAKEN_FLOAT32: return static_cast<const float*>(v.data)[p];
        default: throw std::invalid_argument("bd_read_f32: non-(int/float32) operand");
    }
}

inline float bd_apply_f32(int op, float x, float y) {
    switch (op) {
        case BOP_PLUS:     return x + y;
        case BOP_MINUS:    return x - y;
        case BOP_MULTIPLY: return x * y;
        case BOP_MODULO:   return std::fmod(x, y);  // float overload
        default: throw std::invalid_argument("bd_apply_f32: unsupported op");
    }
}

// FLOAT32 arithmetic at SINGLE precision (DuckDB: FLOAT+FLOAT→FLOAT, int+FLOAT→FLOAT,
// computed in float — large ints round to their float32 representation). Used for
// non-divide ops where a FLOAT32 operand is present and NO FLOAT64 operand is.
VecResult binop_float32(int op, const DrakenVector& a, const DrakenVector& b) {
    const uint32_t n = a.length;
    float* dst = static_cast<float*>(draken_malloc((n ? n : 1) * sizeof(float)));
    if (!dst) throw std::bad_alloc();
    for (uint32_t i = 0; i < n; ++i)
        dst[i] = bd_apply_f32(op, bd_read_f32(a, i), bd_read_f32(b, i));
    VecResult r;
    r.data           = dst;
    r.validity       = draken::ops::fi_combine_validity(a.validity, b.validity, n);
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = DRAKEN_FLOAT32;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

// FLOAT64 arithmetic over two numeric vectors (operands read as double). Dense
// output, per-logical-row validity AND. Used for true DIVIDE (any numeric) and
// for non-divide ops where a FLOAT64 operand is present.
VecResult binop_float64(int op, const DrakenVector& a, const DrakenVector& b) {
    const uint32_t n = a.length;
    double* dst = static_cast<double*>(draken_malloc((n ? n : 1) * sizeof(double)));
    if (!dst) throw std::bad_alloc();
    for (uint32_t i = 0; i < n; ++i)
        dst[i] = bd_apply_f64(op, bd_read_f64(a, i), bd_read_f64(b, i));
    VecResult r;
    r.data           = dst;
    r.validity       = draken::ops::fi_combine_validity(a.validity, b.validity, n);
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = DRAKEN_FLOAT64;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

// 10^e as double, e in [0,38] (DECIMAL128 scale max is 38; DECIMAL int64 max 18).
inline double bd_pow10(unsigned char e) {
    static const double P[39] = {
        1e0,1e1,1e2,1e3,1e4,1e5,1e6,1e7,1e8,1e9,1e10,1e11,1e12,1e13,1e14,1e15,1e16,1e17,1e18,1e19,
        1e20,1e21,1e22,1e23,1e24,1e25,1e26,1e27,1e28,1e29,1e30,1e31,1e32,1e33,1e34,1e35,1e36,1e37,1e38};
    return (e <= 38) ? P[e] : 1.0;
}

// Read the i-th value as double, dividing decimals by 10^scale (mirrors the
// closure's to_float64 on a decimal). Non-decimal operands ignore scale.
inline double bd_read_f64_scaled(const DrakenVector& v, uint32_t i, unsigned char scale) {
    const uint32_t p = v.selection[i];
    switch (v.type) {
        case DRAKEN_DECIMAL:
            return static_cast<double>(static_cast<const int64_t*>(v.data)[p]) / bd_pow10(scale);
        case DRAKEN_DECIMAL128:
            return static_cast<double>(static_cast<const __int128*>(v.data)[p]) / bd_pow10(scale);
        default:
            return bd_read_f64(v, i);
    }
}

// DECIMAL/DECIMAL128 × FLOAT → FLOAT64 (operator_map: {DECIMAL,FLOAT} → FLOAT64).
// The decimal operand is converted to double via its scale (= closure to_float64),
// the float/int operand read plain; then double arithmetic. Covers + - * % / .
VecResult binop_float64_scaled(int op, const DrakenVector& a, unsigned char sa,
                               const DrakenVector& b, unsigned char sb) {
    const uint32_t n = a.length;
    double* dst = static_cast<double*>(draken_malloc((n ? n : 1) * sizeof(double)));
    if (!dst) throw std::bad_alloc();
    for (uint32_t i = 0; i < n; ++i)
        dst[i] = bd_apply_f64(op, bd_read_f64_scaled(a, i, sa), bd_read_f64_scaled(b, i, sb));
    VecResult r;
    r.data           = dst;
    r.validity       = draken::ops::fi_combine_validity(a.validity, b.validity, n);
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = DRAKEN_FLOAT64;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

// ---- string concat (op 7, BOP_STRING_CONCAT) ------------------------------
// Element-wise `a || b`. Ported from the live nanobind impl_concat
// (vector_selection_concat.cpp): 2-pass byte assembly (Pass 1 sizes the arena,
// Pass 2 assembles). Long results are written once, straight into the arena — no
// intermediate scratch copy. The arena budget (Pass 1)
// uses the per-row SUM (ll+rl) vs STR_INLINE_MAX — NOT the per-operand length:
// two individually-inline operands can concatenate past the inline limit (7+7=14)
// and still need arena bytes (the concat_arena_overflow trap). NULL || x = NULL
// (DuckDB). Reads operands via the uniform data[selection[row]] path, so dense /
// dict / constant string shapes all work. Operands must already share the string
// type (the binder coerces); result type = left->type.
static inline bool bsc_row_valid(const DrakenVector* dv, uint32_t row) {
    if (!dv->validity) return true;
    return ((dv->validity[row >> 3] >> (row & 7u)) & 1u) != 0u;
}
static inline void bsc_read_row(const DrakenVector* dv, uint32_t row,
                                const uint8_t** data, uint32_t* len) {
    const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(dv->data);
    const DrakenStringSlot* slot = &sa->slots[dv->selection[row]];
    *data = str_data(slot, sa->arena);
    *len  = str_length(slot);
}

// `x || NULL` / `NULL || x` → NULL for EVERY row (DuckDB semantics; the architect
// ruled that CONCAT() deliberately differs on a NULL arg and is correct as-is —
// do not "fix" that here).
//
// This cannot be left to binop_string_concat: a DRAKEN_NULL operand is
// SELF-DESCRIBING (buffers.h — "type==NULL ⟹ every row null; no data, no
// validity"), so it carries data == NULL and validity == NULL. bsc_row_valid()
// reads a null validity as "all rows valid", and bsc_read_row() would then
// dereference the NULL arena — the exact garbage-read this short-circuit exists
// to prevent. It must be intercepted before any string kernel touches the operand.
//
// Result is a DENSE all-null column of `n` rows typed as the string operand,
// built through vecresult_from_string_buffers with the SAME hand-allocated
// slots/arena/validity shape binop_string_concat itself uses. That is deliberate:
// the direct-write vecresult_string_block_alloc / vecresult_from_string_block
// variant produces a VecResult whose validity points INSIDE the block, and the
// BC_C_NATIVE_STRING result wrap this opcode goes through frees it as a separate
// allocation — an interior free that aborts with
// POINTER_BEING_FREED_WAS_NOT_ALLOCATED at Vector dealloc. Staying on the exact
// construction the sibling concat path already uses keeps the two impossible to
// diverge. Dense (n zeroed slots) rather than a 1-slot constant because
// `x || NULL` is a degenerate expression, not a hot path.
static VecResult binop_string_concat_null(uint32_t n, DrakenType out_type) {
    const size_t slots_sz = (n > 0u ? n : 1u) * sizeof(DrakenStringSlot);
    DrakenStringSlot* slots = static_cast<DrakenStringSlot*>(draken_malloc(slots_sz));
    if (!slots) return draken_error_sentinel("binop_string_concat: slot alloc failed");
    // Zeroed == str_init_null canonical for every row; no arena bytes are needed.
    std::memset(slots, 0, slots_sz);

    const size_t vsz = (n > 0u) ? static_cast<size_t>((((n + 7u) >> 3u) + 7u) & ~7u) : 8u;
    uint8_t* validity = static_cast<uint8_t*>(draken_malloc(vsz));
    if (!validity) {
        draken_free(slots);
        return draken_error_sentinel("binop_string_concat: validity alloc failed");
    }
    // All bits 0 (Arrow convention: bit SET = valid) ⇒ every row NULL.
    std::memset(validity, 0, vsz);

    return vecresult_from_string_buffers(slots, /*arena=*/nullptr, /*arena_len=*/0,
                                         validity, n, out_type);
}

static VecResult binop_string_concat(const DrakenVector* left, const DrakenVector* right) {
    const uint32_t n = left->length;
    const DrakenType out_type = left->type;
    const uint8_t* ld; uint32_t ll; const uint8_t* rd; uint32_t rl;

    // Pass 1: arena budget = sum of per-row concatenated lengths that exceed inline.
    size_t total_bytes = 0u;
    for (uint32_t row = 0u; row < n; ++row) {
        if (!bsc_row_valid(left, row) || !bsc_row_valid(right, row)) continue;
        bsc_read_row(left, row, &ld, &ll);
        bsc_read_row(right, row, &rd, &rl);
        const size_t row_len = static_cast<size_t>(ll) + static_cast<size_t>(rl);
        if (row_len > STR_INLINE_MAX) total_bytes += row_len;
    }

    const size_t slots_sz = (n > 0u ? n : 1u) * sizeof(DrakenStringSlot);
    DrakenStringSlot* slots = static_cast<DrakenStringSlot*>(draken_malloc(slots_sz));
    if (!slots) return draken_error_sentinel("binop_string_concat: slot alloc failed");
    std::memset(slots, 0, slots_sz);

    uint8_t* arena = static_cast<uint8_t*>(draken_malloc(total_bytes > 0u ? total_bytes : 1u));
    if (!arena) { draken_free(slots); return draken_error_sentinel("binop_string_concat: arena alloc failed"); }

    const size_t vsz = (n > 0u) ? static_cast<size_t>((((n + 7u) >> 3u) + 7u) & ~7u) : 8u;
    uint8_t* validity = static_cast<uint8_t*>(draken_malloc(vsz));
    if (!validity) { draken_free(slots); draken_free(arena); return draken_error_sentinel("binop_string_concat: validity alloc failed"); }
    std::memset(validity, 0, vsz);

    bool any_null = false;
    size_t arena_pos = 0u;

    // Pass 2: assemble bytes, build slots (inline vs arena), set validity bits.
    // Long (arena) results are copied ONCE, straight into the arena — the operand
    // bytes go directly to their final destination and draken_build_string_slot
    // hashes from that arena location. Only inline (<=STR_INLINE_MAX) results need
    // a contiguous temp, which fits on the stack — so there is no heap scratch.
    for (uint32_t row = 0u; row < n; ++row) {
        if (!bsc_row_valid(left, row) || !bsc_row_valid(right, row)) {
            str_init_null(&slots[row]); any_null = true; continue;
        }
        bsc_read_row(left, row, &ld, &ll);
        bsc_read_row(right, row, &rd, &rl);
        const uint32_t row_len = ll + rl;
        if (row_len == 0u) {
            draken_build_string_slot(&slots[row], nullptr, 0u, 0u);
            validity[row >> 3u] |= static_cast<uint8_t>(1u << (row & 7u));
            continue;
        }
        if (row_len > STR_INLINE_MAX) {
            const uint32_t off = static_cast<uint32_t>(arena_pos);
            if (ll) std::memcpy(arena + off, ld, ll);
            if (rl) std::memcpy(arena + off + ll, rd, rl);
            draken_build_string_slot(&slots[row], arena + off, row_len, off);
            arena_pos += row_len;
        } else {
            uint8_t inl[STR_INLINE_MAX];
            if (ll) std::memcpy(inl, ld, ll);
            if (rl) std::memcpy(inl + ll, rd, rl);
            draken_build_string_slot(&slots[row], inl, row_len, 0u);
        }
        validity[row >> 3u] |= static_cast<uint8_t>(1u << (row & 7u));
    }
    if (!any_null) { draken_free(validity); validity = nullptr; }

    return vecresult_from_string_buffers(slots, arena, arena_pos, validity, n, out_type);
}

// ---- IP-in-CIDR (op 8 BOP_BITWISE_OR over string operands) ----------------
// `ip_column >> cidr` → BOOL. left = IP string column (N rows); right = CIDR
// string — ONLY row 0 is read (the CIDR is a scalar, matching the live
// vector_ip_in_cidr). Ported byte-identical from vector_misc.cpp: IPv4 only;
// NULL ip → false (result validity null = all "valid"); invalid IP/CIDR → loud
// error sentinel. Reads operands via the uniform data[selection[row]] path.
static int bicidr_parse_ip(const uint8_t* ip, uint32_t length, uint32_t* out) {
    uint32_t result = 0u, num; int shift = 24; uint32_t i = 0u; int oc = 0;
    while (oc < 4) {
        num = 0u; int dc = 0;
        while (i < length) {
            uint8_t c = ip[i];
            if (c < '0' || c > '9') break;
            num = num * 10u + static_cast<uint32_t>(c - '0'); ++dc; ++i;
        }
        if (dc == 0) return -1;
        if (num > 255u) return -1;
        result += (num << shift); shift -= 8; ++oc;
        if (oc < 4) { if (i >= length || ip[i] != '.') return -1; ++i; }
        else        { if (i < length) return -1; }
    }
    *out = result; return 0;
}

static VecResult binop_ip_in_cidr(const DrakenVector* ipv, const DrakenVector* cidrv) {
    if (cidrv->length == 0u) return draken_error_sentinel("binop_ip_in_cidr: cidr vector empty");
    if (!bsc_row_valid(cidrv, 0u)) return draken_error_sentinel("binop_ip_in_cidr: cidr row 0 is NULL");
    const uint8_t* cidr_bytes; uint32_t cidr_len;
    bsc_read_row(cidrv, 0u, &cidr_bytes, &cidr_len);

    uint32_t slash = 0u;
    while (slash < cidr_len && cidr_bytes[slash] != '/') ++slash;
    if (slash == cidr_len) return draken_error_sentinel("binop_ip_in_cidr: CIDR notation missing '/'");

    const uint8_t* mask_str = cidr_bytes + slash + 1u;
    const uint32_t mask_len = cidr_len - slash - 1u;
    uint32_t mask_size = 0u;
    for (uint32_t k = 0u; k < mask_len; ++k) {
        uint8_t c = mask_str[k];
        if (c < '0' || c > '9') return draken_error_sentinel("binop_ip_in_cidr: CIDR mask not an integer");
        mask_size = mask_size * 10u + static_cast<uint32_t>(c - '0');
    }
    if (mask_size > 32u) return draken_error_sentinel("binop_ip_in_cidr: CIDR mask out of range (>32)");
    const uint32_t netmask = mask_size == 0u
        ? 0u : ((0xFFFFFFFFu << (32u - mask_size)) & 0xFFFFFFFFu);

    uint32_t base_ip = 0u;
    if (bicidr_parse_ip(cidr_bytes, slash, &base_ip) != 0)
        return draken_error_sentinel("binop_ip_in_cidr: invalid CIDR base address");
    base_ip &= netmask;

    const uint32_t n = ipv->length;
    uint8_t* dst = draken::ops::cmp_alloc_bool_buf(n);  // zero-initialised
    if (!dst) return draken_error_sentinel("binop_ip_in_cidr: bool buffer alloc failed");

    const uint8_t* ip_bytes; uint32_t ip_len;
    for (uint32_t i = 0u; i < n; ++i) {
        if (!bsc_row_valid(ipv, i)) continue;
        bsc_read_row(ipv, i, &ip_bytes, &ip_len);
        if (ip_len == 0u) continue;
        uint32_t ip_int = 0u;
        if (bicidr_parse_ip(ip_bytes, ip_len, &ip_int) != 0) {
            draken_free(dst);
            return draken_error_sentinel("binop_ip_in_cidr: invalid IP address");
        }
        if ((ip_int & netmask) == base_ip)
            dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
    }

    VecResult r;
    r.data           = dst;
    r.validity       = nullptr;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = DRAKEN_BOOL;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

}  // namespace

extern "C" {

VecResult draken_binop(void* ctx, const DrakenVector* left, const DrakenVector* right) {
    DRAKEN_KERNEL_TRY({
        if (!left || !right) return draken_error_sentinel("draken_binop: null input vector");
        if (!ctx)            return draken_error_sentinel("draken_binop: null context");
        if (left->length != right->length)
            return draken_error_sentinel_fmt(
                "draken_binop: length mismatch: left=%u right=%u", left->length, right->length);

        const int op = static_cast<const binary_op_ctx*>(ctx)->op_code;
        const DrakenType lt = left->type;
        const DrakenType rt = right->type;

        auto is_int = [](DrakenType t) {
            return t == DRAKEN_INT8 || t == DRAKEN_INT16 ||
                   t == DRAKEN_INT32 || t == DRAKEN_INT64;
        };
        // E33 — unsigned integers.
        auto is_uint = [](DrakenType t) {
            return t == DRAKEN_UINT8 || t == DRAKEN_UINT16 ||
                   t == DRAKEN_UINT32 || t == DRAKEN_UINT64;
        };
        auto is_float = [](DrakenType t) {
            return t == DRAKEN_FLOAT32 || t == DRAKEN_FLOAT64;
        };
        const bool both_numeric =
            (is_int(lt) || is_uint(lt) || is_float(lt)) &&
            (is_int(rt) || is_uint(rt) || is_float(rt));

        // DECIMAL × DECIMAL (both int64-backed) → DECIMAL (P9.1b-1). Scale-aware via
        // the existing dec_* kernels (PostgreSQL scale rules, architect E.32 — the
        // same kernels the live closure uses, so byte-identical). Scales come from
        // ctx (the binder fills them; DrakenVector carries no scale). DECIMAL `/`
        // stays DECIMAL (dec_div with result_scale), NOT float — handled here before
        // the float true-divide clause. When the bound result precision exceeds the
        // int64 tier (>18) the result is DECIMAL128: skip this int64 path and fall to
        // the promotion block below (which widens both operands to int128).
        if (lt == DRAKEN_DECIMAL && rt == DRAKEN_DECIMAL &&
            static_cast<const binary_op_ctx*>(ctx)->result_precision <= 18) {
            const auto* c = static_cast<const binary_op_ctx*>(ctx);
            const unsigned char sa = c->left_scale;
            const unsigned char sb = c->right_scale;
            const unsigned char rs = c->result_scale;
            VecResult r;
            switch (op) {
                case BOP_PLUS:     r = draken::ops::dec_add(*left, sa, *right, sb); break;
                case BOP_MINUS:    r = draken::ops::dec_sub(*left, sa, *right, sb); break;
                case BOP_MULTIPLY: r = draken::ops::dec_mul(*left, sa, *right, sb); break;
                case BOP_DIVIDE:   r = draken::ops::dec_div(*left, sa, *right, sb, rs); break;
                case BOP_MODULO:   r = draken::ops::dec_mod(*left, sa, *right, sb); break;
                default:
                    return draken_error_sentinel_fmt(
                        "draken_binop: unsupported op %d for DECIMAL", op);
            }
            // Stamp the result descriptor (precision/scale) so the executor wrap can
            // reattach the LogicalType — the binder's bound result scale equals the
            // kernel's output scale (downstream consumes that type), so it is correct.
            r.dec_precision = c->result_precision;
            r.dec_scale = c->result_scale;
            return r;
        }

        // DECIMAL128 × DECIMAL128 (both int128-backed) → DECIMAL128 (P9.1b-rest),
        // via dec128_* (same scale rules / same kernels as the live path).
        if (lt == DRAKEN_DECIMAL128 && rt == DRAKEN_DECIMAL128) {
            const auto* c = static_cast<const binary_op_ctx*>(ctx);
            const unsigned char sa = c->left_scale;
            const unsigned char sb = c->right_scale;
            const unsigned char rs = c->result_scale;
            VecResult r;
            switch (op) {
                case BOP_PLUS:     r = draken::ops::dec128_add(*left, sa, *right, sb); break;
                case BOP_MINUS:    r = draken::ops::dec128_sub(*left, sa, *right, sb); break;
                case BOP_MULTIPLY: r = draken::ops::dec128_mul(*left, sa, *right, sb); break;
                case BOP_DIVIDE:   r = draken::ops::dec128_div(*left, sa, *right, sb, rs); break;
                case BOP_MODULO:   r = draken::ops::dec128_mod(*left, sa, *right, sb); break;
                default:
                    return draken_error_sentinel_fmt(
                        "draken_binop: unsupported op %d for DECIMAL128", op);
            }
            r.dec_precision = c->result_precision;
            r.dec_scale = c->result_scale;
            return r;
        }

        // DECIMAL(int64) op INT64 → DECIMAL (S-A.3). The INT64 operand is a scale-0
        // decimal: the binder passes scale 0 for it in ctx, so the same dec_* kernels
        // apply directly (they read int64 data + align scales). Order-preserving via
        // ctx left/right scale. Only DRAKEN_DECIMAL (int64) × DRAKEN_INT64 with an
        // int64-tier result (precision ≤ 18); when the bound result precision exceeds
        // 18 the result is DECIMAL128 and falls to the promotion block below (which
        // widens both operands to int128). Narrower ints (INT8/16/32) stay on the
        // closure, which widens them itself.
        if (((lt == DRAKEN_DECIMAL && rt == DRAKEN_INT64) ||
             (lt == DRAKEN_INT64 && rt == DRAKEN_DECIMAL)) &&
            static_cast<const binary_op_ctx*>(ctx)->result_precision <= 18) {
            const auto* c = static_cast<const binary_op_ctx*>(ctx);
            const unsigned char sa = c->left_scale;   // 0 on the INT64 side
            const unsigned char sb = c->right_scale;
            const unsigned char rs = c->result_scale;
            VecResult r;
            switch (op) {
                case BOP_PLUS:     r = draken::ops::dec_add(*left, sa, *right, sb); break;
                case BOP_MINUS:    r = draken::ops::dec_sub(*left, sa, *right, sb); break;
                case BOP_MULTIPLY: r = draken::ops::dec_mul(*left, sa, *right, sb); break;
                case BOP_DIVIDE:   r = draken::ops::dec_div(*left, sa, *right, sb, rs); break;
                case BOP_MODULO:   r = draken::ops::dec_mod(*left, sa, *right, sb); break;
                default:
                    return draken_error_sentinel_fmt(
                        "draken_binop: unsupported op %d for DECIMAL×INT64", op);
            }
            r.dec_precision = c->result_precision;
            r.dec_scale = c->result_scale;
            return r;
        }

        // DECIMAL128 PROMOTION (S-A.3 completion): the result is int128-backed
        // DECIMAL128 and at least one operand is int64-backed, in one of two shapes:
        //   (i)  one operand is already DECIMAL128 and the other is int64-backed
        //        (DRAKEN_DECIMAL or DRAKEN_INT64) — DECIMAL128 × INT64, INT64 × DECIMAL128,
        //        or cross-kind DECIMAL × DECIMAL128 (either order); or
        //   (ii) both operands are int64-backed (at least one a DRAKEN_DECIMAL) but the
        //        bound result precision exceeds the int64 tier (>18) — e.g.
        //        DECIMAL(10,2) × INT64, DECIMAL(15,2) × DECIMAL(15,2).
        // Each int64-backed operand is widened to int128 (widen_i64_to_dec128 —
        // §11-uniform, validity preserved) and the dec128_* kernels run. The result is
        // always DECIMAL128 (the wrap reattaches precision/scale from ctx). The int64
        // DEC×DEC / DEC×INT64 branches above handle only the int64-tier (≤18) results;
        // their DECIMAL128-result variants fall through here. Narrower ints (INT8/16/32)
        // are not int64-stride and stay on the closure (the _c_native_binop guard keeps
        // them off this path).
        {
            auto is_i64_decimalish = [](DrakenType t) {
                return t == DRAKEN_DECIMAL || t == DRAKEN_INT64;
            };
            const unsigned char rprec =
                static_cast<const binary_op_ctx*>(ctx)->result_precision;
            const bool both_i64dec = is_i64_decimalish(lt) && is_i64_decimalish(rt);
            const bool one_is_decimal = (lt == DRAKEN_DECIMAL || rt == DRAKEN_DECIMAL);
            const bool promote128 =
                (lt == DRAKEN_DECIMAL128 && is_i64_decimalish(rt)) ||
                (rt == DRAKEN_DECIMAL128 && is_i64_decimalish(lt)) ||
                (both_i64dec && one_is_decimal && rprec > 18);
            if (promote128) {
                const auto* c = static_cast<const binary_op_ctx*>(ctx);
                const unsigned char sa = c->left_scale;
                const unsigned char sb = c->right_scale;
                const unsigned char rs = c->result_scale;
                // Widen the int64-backed side(s) to int128; the DECIMAL128 side is used in
                // place. Exactly one side widens (the other is already DECIMAL128). Temp
                // buffers are freed after the kernel, including on the throw path.
                VecResult lw{};
                VecResult rw{};
                bool lw_used = false;
                bool rw_used = false;
                DrakenVector lv = *left;
                DrakenVector rv = *right;
                if (lt != DRAKEN_DECIMAL128) {
                    lw = draken::ops::widen_i64_to_dec128(*left);
                    lv = draken_vector_from_dense(lw.data, lw.length, DRAKEN_DECIMAL128, lw.validity);
                    lw_used = true;
                }
                if (rt != DRAKEN_DECIMAL128) {
                    rw = draken::ops::widen_i64_to_dec128(*right);
                    rv = draken_vector_from_dense(rw.data, rw.length, DRAKEN_DECIMAL128, rw.validity);
                    rw_used = true;
                }
                auto free_temps = [&]() {
                    if (lw_used) { draken_free(lw.data); draken_free(lw.validity); }
                    if (rw_used) { draken_free(rw.data); draken_free(rw.validity); }
                };
                VecResult r;
                try {
                    switch (op) {
                        case BOP_PLUS:     r = draken::ops::dec128_add(lv, sa, rv, sb); break;
                        case BOP_MINUS:    r = draken::ops::dec128_sub(lv, sa, rv, sb); break;
                        case BOP_MULTIPLY: r = draken::ops::dec128_mul(lv, sa, rv, sb); break;
                        case BOP_DIVIDE:   r = draken::ops::dec128_div(lv, sa, rv, sb, rs); break;
                        case BOP_MODULO:   r = draken::ops::dec128_mod(lv, sa, rv, sb); break;
                        default:
                            free_temps();
                            return draken_error_sentinel_fmt(
                                "draken_binop: unsupported op %d for DECIMAL128 promotion", op);
                    }
                } catch (...) {
                    free_temps();
                    throw;
                }
                free_temps();
                r.dec_precision = c->result_precision;
                r.dec_scale = c->result_scale;
                return r;
            }
        }

        // INTERVAL ± INTERVAL → INTERVAL (S-A.1). Component-wise months/µs add/sub —
        // the SAME draken::ops kernels the live closure calls (byte-identical), so
        // this just removes the per-morsel Python-closure hop. No scales/units.
        if (lt == DRAKEN_INTERVAL && rt == DRAKEN_INTERVAL) {
            switch (op) {
                case BOP_PLUS:  return draken::ops::interval_add(*left, *right);
                case BOP_MINUS: return draken::ops::interval_sub(*left, *right);
                default:
                    return draken_error_sentinel_fmt(
                        "draken_binop: unsupported op %d for INTERVAL", op);
            }
        }

        // TEMPORAL ± / − (S-A.2). date/ts ± interval → TIMESTAMP64(µs); date/ts −
        // date/ts → INTERVAL. The SAME draken::ops kernels the closure calls. The
        // timestamp unit is a LogicalType detail (not on DrakenVector), so it arrives
        // via ctx (left_unit/right_unit); date32 operands ignore it. signum from op.
        {
            auto is_temporal = [](DrakenType t) {
                return t == DRAKEN_DATE32 || t == DRAKEN_TIMESTAMP64;
            };
            const auto* bctx = static_cast<const binary_op_ctx*>(ctx);
            // temporal ± interval (PLUS either order; MINUS only temporal − interval).
            // Result is always TIMESTAMP64 in MICROSECONDS (unit 2) — stamp the
            // descriptor so the executor wrap reattaches the LogicalType.
            if (is_temporal(lt) && rt == DRAKEN_INTERVAL && (op == BOP_PLUS || op == BOP_MINUS)) {
                VecResult r = draken::ops::interval_apply_to_temporal(
                    *left, *right, lt == DRAKEN_DATE32, bctx->left_unit,
                    op == BOP_PLUS ? 1 : -1);
                r.ts_unit = 2;  // MICROSECONDS
                return r;
            }
            if (lt == DRAKEN_INTERVAL && is_temporal(rt) && op == BOP_PLUS) {
                VecResult r = draken::ops::interval_apply_to_temporal(
                    *right, *left, rt == DRAKEN_DATE32, bctx->right_unit, 1);
                r.ts_unit = 2;  // MICROSECONDS
                return r;
            }
            // temporal − temporal → INTERVAL
            if (is_temporal(lt) && is_temporal(rt) && op == BOP_MINUS) {
                return draken::ops::temporal_minus_temporal(
                    *left, *right, lt == DRAKEN_DATE32, bctx->left_unit,
                    rt == DRAKEN_DATE32, bctx->right_unit);
            }
        }

        // DECIMAL/DECIMAL128 × FLOAT → FLOAT64 (operator_map: {DECIMAL,FLOAT}→FLOAT64;
        // the closure promotes the decimal side via to_float64). Covers + - * % / .
        {
            auto is_decimal = [](DrakenType t) { return t == DRAKEN_DECIMAL || t == DRAKEN_DECIMAL128; };
            const bool dec_float_mix =
                (is_decimal(lt) && is_float(rt)) || (is_float(lt) && is_decimal(rt));
            const bool real_op =
                (op == BOP_PLUS || op == BOP_MINUS || op == BOP_MULTIPLY ||
                 op == BOP_MODULO || op == BOP_DIVIDE);
            if (dec_float_mix && real_op) {
                const unsigned char sa = static_cast<const binary_op_ctx*>(ctx)->left_scale;
                const unsigned char sb = static_cast<const binary_op_ctx*>(ctx)->right_scale;
                return binop_float64_scaled(op, *left, sa, *right, sb);
            }
        }

        // TRUE DIVIDE (any numeric operands) → FLOAT64. DuckDB: `/` is always
        // DOUBLE (even TINYINT/TINYINT), x/0 → ±inf (plain IEEE double division).
        if (op == BOP_DIVIDE && both_numeric) {
            return binop_float64(op, *left, *right);
        }

        // Integer non-divide arithmetic (P9.1a): D.6 widen-to-next-power.
        const bool int_arith_op =
            (op == BOP_PLUS || op == BOP_MINUS || op == BOP_MULTIPLY ||
             op == BOP_MODULO || op == BOP_INT_DIVIDE);
        if (int_arith_op) {
            // E33 — both operands unsigned (any width combination, including
            // UINT64): genuine unsigned semantics (wrapping overflow, unsigned
            // div/mod), computed in uint64_t space.
            if (is_uint(lt) && is_uint(rt)) {
                return draken::ops::fi_uint_arith(op, *left, *right);
            }
            // E33 — one signed, one narrow-unsigned (UINT8/16/32): fits int64_t
            // space, so the existing signed cross-width path handles it once
            // extended (fi_read_i64 zero-extends the unsigned side).
            if ((is_int(lt) && is_uint(rt) && rt != DRAKEN_UINT64) ||
                (is_int(rt) && is_uint(lt) && lt != DRAKEN_UINT64)) {
                return draken::ops::fi_int_arith(op, *left, *right);
            }
            if (is_int(lt) && is_int(rt)) {
                return draken::ops::fi_int_arith(op, *left, *right);
            }
            // E33 — UINT64 paired with INT64 (either order): the DECIMAL128
            // escape from the design matrix (no fixed-width signed type holds
            // the full UINT64 range; __int128 does, with room to spare). Both
            // operands widen to int128 as scale-0 values and run through the
            // existing dec128_* kernels — mirrors the DECIMAL128 promotion
            // block above exactly, just without a DECIMAL-typed operand.
            // UINT64 paired with a narrower signed int (INT8/16/32) is NOT
            // handled here — mirrors the existing restriction on narrow ints
            // combined with DECIMAL (they stay on the closure, which widens
            // them itself) — falls through to the "not yet C-native" error.
            if ((lt == DRAKEN_UINT64 && rt == DRAKEN_INT64) ||
                (lt == DRAKEN_INT64 && rt == DRAKEN_UINT64)) {
                const auto* c = static_cast<const binary_op_ctx*>(ctx);
                const unsigned char sa = c->left_scale;
                const unsigned char sb = c->right_scale;
                VecResult lw = (lt == DRAKEN_UINT64)
                    ? draken::ops::widen_u64_to_dec128(*left)
                    : draken::ops::widen_i64_to_dec128(*left);
                VecResult rw = (rt == DRAKEN_UINT64)
                    ? draken::ops::widen_u64_to_dec128(*right)
                    : draken::ops::widen_i64_to_dec128(*right);
                DrakenVector lv = draken_vector_from_dense(lw.data, lw.length, DRAKEN_DECIMAL128, lw.validity);
                DrakenVector rv = draken_vector_from_dense(rw.data, rw.length, DRAKEN_DECIMAL128, rw.validity);
                auto free_temps = [&]() {
                    draken_free(lw.data); draken_free(lw.validity);
                    draken_free(rw.data); draken_free(rw.validity);
                };
                // PLUS/MINUS/MULTIPLY degenerate exactly to plain integer
                // arithmetic in int128 space at scale 0, unambiguous.
                // INT_DIVIDE/MODULO route through dec128_int_divide/dec128_int_mod
                // (decimal_arith.h) — scale-0-only truncating integer div/mod with
                // the established div-by-zero->0 convention (i64_div/i64_mod),
                // NOT dec128_div/dec128_mod's true-decimal-division / raise-on-zero
                // semantics (those are the wrong operation for a truncating op).
                // BOP_DIVIDE (true `/`) never reaches here — the TRUE DIVIDE
                // branch above (both_numeric -> FLOAT64) always intercepts it
                // first, same as for every other integer pairing.
                VecResult r;
                try {
                    switch (op) {
                        case BOP_PLUS:       r = draken::ops::dec128_add(lv, sa, rv, sb); break;
                        case BOP_MINUS:      r = draken::ops::dec128_sub(lv, sa, rv, sb); break;
                        case BOP_MULTIPLY:   r = draken::ops::dec128_mul(lv, sa, rv, sb); break;
                        case BOP_INT_DIVIDE: r = draken::ops::dec128_int_divide(lv, rv); break;
                        case BOP_MODULO:     r = draken::ops::dec128_int_mod(lv, rv); break;
                        default:
                            free_temps();
                            return draken_error_sentinel_fmt(
                                "draken_binop: unsupported op %d for UINT64 x INT64 promotion", op);
                    }
                } catch (...) {
                    free_temps();
                    throw;
                }
                free_temps();
                r.dec_precision = c->result_precision;
                r.dec_scale = c->result_scale;
                return r;
            }
        }

        // Non-divide arithmetic with a FLOAT64 operand → FLOAT64 (DuckDB: any
        // DOUBLE operand promotes the result to DOUBLE).
        const bool real_arith_op =
            (op == BOP_PLUS || op == BOP_MINUS || op == BOP_MULTIPLY || op == BOP_MODULO);
        if (real_arith_op && both_numeric && (lt == DRAKEN_FLOAT64 || rt == DRAKEN_FLOAT64)) {
            return binop_float64(op, *left, *right);
        }

        // Non-divide arithmetic with a FLOAT32 operand and NO FLOAT64 → FLOAT32 at
        // single precision (DuckDB preserves FLOAT32; only a DOUBLE operand promotes).
        if (real_arith_op && both_numeric && (lt == DRAKEN_FLOAT32 || rt == DRAKEN_FLOAT32)) {
            return binop_float32(op, *left, *right);
        }

        // Bitwise OR/AND/XOR/SHL/SHR on integers (P9.1c). int_bitwise preserves the
        // input type and requires both operands the same type (the binder coerces;
        // a mismatch is an error in the live path too). BOP_BITWISE_OR on VARCHAR is
        // IP-in-CIDR — excluded here by the is_int guard (P9.1e).
        const bool bitwise_op =
            (op == BOP_BITWISE_OR || op == BOP_BITWISE_AND || op == BOP_BITWISE_XOR ||
             op == BOP_SHIFT_LEFT || op == BOP_SHIFT_RIGHT);
        if (bitwise_op && (is_int(lt) || is_uint(lt)) && (is_int(rt) || is_uint(rt))) {
            if (lt != rt)
                return draken_error_sentinel_fmt(
                    "draken_binop: bitwise operands must be the same type: %d vs %d", (int)lt, (int)rt);
            switch (op) {
                case BOP_BITWISE_OR:  return draken::ops::bitwise_or(*left, *right);
                case BOP_BITWISE_AND: return draken::ops::bitwise_and(*left, *right);
                case BOP_BITWISE_XOR: return draken::ops::bitwise_xor(*left, *right);
                case BOP_SHIFT_LEFT:  return draken::ops::bitwise_shl(*left, *right);
                case BOP_SHIFT_RIGHT: return draken::ops::bitwise_shr(*left, *right);
            }
        }

        // String concat (op 7): element-wise a || b over same-type string columns.
        const bool lt_is_string = (lt == DRAKEN_VARCHAR || lt == DRAKEN_NVARCHAR
                                   || lt == DRAKEN_VARBINARY);
        if (op == BOP_STRING_CONCAT && lt_is_string && lt == rt) {
            if (left->length != right->length)
                return draken_error_sentinel("binop_string_concat: length mismatch");
            return binop_string_concat(left, right);
        }

        // Untyped NULL operand: short-circuit to an all-null column of the string
        // operand's type (see binop_string_concat_null). Row count comes from the
        // STRING side — a DRAKEN_NULL operand carries no rows of its own.
        // NULL || NULL falls through to the loud error below: no string operand
        // means no result type to adopt.
        if (op == BOP_STRING_CONCAT && (lt == DRAKEN_NULL || rt == DRAKEN_NULL)) {
            const bool rt_is_str = (rt == DRAKEN_VARCHAR || rt == DRAKEN_NVARCHAR
                                    || rt == DRAKEN_VARBINARY);
            if (lt == DRAKEN_NULL && rt_is_str)
                return binop_string_concat_null(right->length, rt);
            if (rt == DRAKEN_NULL && lt_is_string)
                return binop_string_concat_null(left->length, lt);
        }

        // IP-in-CIDR (op 8 over string operands): left = IP column, right = CIDR
        // scalar (row 0). Distinct from integer bitwise-OR (guarded by is_int below).
        const bool rt_is_string = (rt == DRAKEN_VARCHAR || rt == DRAKEN_NVARCHAR
                                   || rt == DRAKEN_VARBINARY);
        if (op == BOP_BITWISE_OR && lt_is_string && rt_is_string) {
            return binop_ip_in_cidr(left, right);
        }

        // Not yet C-native (later P9.1 sub-stages): decimal × NARROW int (INT8/16/32 —
        // these stay on the closure, which widens them), and any remaining exotic combos.
        return draken_error_sentinel_fmt(
            "draken_binop: combination not yet C-native (covers int/float32/float64 arithmetic, "
            "true-divide, DECIMAL×DECIMAL, DECIMAL128×DECIMAL128, DECIMAL×INT64, "
            "DECIMAL128×INT64, cross-kind DECIMAL×DECIMAL128, int64→int128 promotion to "
            "DECIMAL128, decimal×float): "
            "op=%d left_type=%d right_type=%d", op, (int)lt, (int)rt);
    });
}

}  // extern "C"
