// draken/ops/kernels/binop_dispatch.cpp — unified binary-op dispatch (P9.1).
//
// See binop_kernels.h. P9.1a: integer arithmetic only. Everything else returns a
// loud error sentinel (no silent fallback) until its sub-stage lands; the live
// binop path is unchanged (this is not yet wired into the executor).

#include "ops/kernels/binop_kernels.h"
#include "ops/kernels/error_handling.h"
#include "ops/kernels/kernel_context.h"   // binary_op_ctx
#include "ops/fixed_int_ops.h"            // draken::ops::fi_int_arith, fi_combine_validity
#include "ops/decimal_arith.h"            // draken::ops::dec_add/sub/mul/div/mod
#include "ops/int_bitwise.h"              // draken::ops::bitwise_and/or/xor/shl/shr
#include "core/buffers.h"
#include "core/alloc.h"                   // draken_malloc
#include "core/vector_alloc.h"            // draken_identity_sel
#include <cmath>                          // std::fmod
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
        auto is_float = [](DrakenType t) {
            return t == DRAKEN_FLOAT32 || t == DRAKEN_FLOAT64;
        };
        const bool both_numeric = (is_int(lt) || is_float(lt)) && (is_int(rt) || is_float(rt));

        // DECIMAL × DECIMAL (both int64-backed) → DECIMAL (P9.1b-1). Scale-aware via
        // the existing dec_* kernels (PostgreSQL scale rules, architect E.32 — the
        // same kernels the live closure uses, so byte-identical). Scales come from
        // ctx (the binder fills them; DrakenVector carries no scale). DECIMAL `/`
        // stays DECIMAL (dec_div with result_scale), NOT float — handled here before
        // the float true-divide clause. DECIMAL128, decimal×int, decimal×float are
        // later P9.1b sub-stages.
        if (lt == DRAKEN_DECIMAL && rt == DRAKEN_DECIMAL) {
            const unsigned char sa = static_cast<const binary_op_ctx*>(ctx)->left_scale;
            const unsigned char sb = static_cast<const binary_op_ctx*>(ctx)->right_scale;
            const unsigned char rs = static_cast<const binary_op_ctx*>(ctx)->result_scale;
            switch (op) {
                case BOP_PLUS:     return draken::ops::dec_add(*left, sa, *right, sb);
                case BOP_MINUS:    return draken::ops::dec_sub(*left, sa, *right, sb);
                case BOP_MULTIPLY: return draken::ops::dec_mul(*left, sa, *right, sb);
                case BOP_DIVIDE:   return draken::ops::dec_div(*left, sa, *right, sb, rs);
                case BOP_MODULO:   return draken::ops::dec_mod(*left, sa, *right, sb);
                default:
                    return draken_error_sentinel_fmt(
                        "draken_binop: unsupported op %d for DECIMAL", op);
            }
        }

        // DECIMAL128 × DECIMAL128 (both int128-backed) → DECIMAL128 (P9.1b-rest),
        // via dec128_* (same scale rules / same kernels as the live path).
        if (lt == DRAKEN_DECIMAL128 && rt == DRAKEN_DECIMAL128) {
            const unsigned char sa = static_cast<const binary_op_ctx*>(ctx)->left_scale;
            const unsigned char sb = static_cast<const binary_op_ctx*>(ctx)->right_scale;
            const unsigned char rs = static_cast<const binary_op_ctx*>(ctx)->result_scale;
            switch (op) {
                case BOP_PLUS:     return draken::ops::dec128_add(*left, sa, *right, sb);
                case BOP_MINUS:    return draken::ops::dec128_sub(*left, sa, *right, sb);
                case BOP_MULTIPLY: return draken::ops::dec128_mul(*left, sa, *right, sb);
                case BOP_DIVIDE:   return draken::ops::dec128_div(*left, sa, *right, sb, rs);
                case BOP_MODULO:   return draken::ops::dec128_mod(*left, sa, *right, sb);
                default:
                    return draken_error_sentinel_fmt(
                        "draken_binop: unsupported op %d for DECIMAL128", op);
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
        if (int_arith_op && is_int(lt) && is_int(rt)) {
            return draken::ops::fi_int_arith(op, *left, *right);
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
        if (bitwise_op && is_int(lt) && is_int(rt)) {
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

        // Not yet C-native (later P9.1 sub-stages): decimal×integer, cross-kind
        // DECIMAL×DECIMAL128, string concat, temporal, IP-in-CIDR.
        return draken_error_sentinel_fmt(
            "draken_binop: combination not yet C-native (covers int/float32/float64 arithmetic, "
            "true-divide, DECIMAL×DECIMAL, DECIMAL128×DECIMAL128, decimal×float): "
            "op=%d left_type=%d right_type=%d", op, (int)lt, (int)rt);
    });
}

}  // extern "C"
