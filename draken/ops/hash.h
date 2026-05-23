#pragma once
// draken/ops/hash.h — dispatch table + int64 hash kernel (Milestone C.1).
//
// PUBLIC ENTRY POINT:
//   void draken_hash(const DrakenVector& v, uint64_t* out, uint32_t n)
//   Computes one uint64_t hash per logical row into out[0..n).
//   Throws std::invalid_argument for unsupported types (no silent fallback).
//
// DISPATCH:
//   Single table lookup keyed by v.type (one indirect call per invocation,
//   not per row). The table has one TypeOps entry per DrakenType enum value;
//   unsupported entries carry a null function pointer — caught at call time.
//   Add a type:  fill one entry in OpsTable().
//   Add an op:   add a slot to TypeOps and mirror it in draken_hash and
//                the equivalent per-op dispatcher.
//
// ALGORITHM (int64 — must match draken_old/vectors/integer64_vector.pyx
//            c_hash_single exactly so parity holds):
//   For each chunk of ≤ 1024 rows:
//     fill scratch[j] = data[selection[i+j]] (valid) | NULL_HASH (null)
//     simd_hash_i64(scratch, out+i, block)  — same as zeroed dest + simd_mix_hash
//
// INCLUDE REQUIREMENTS (both dirs in draken_native build include path):
//   "buffers.h"    from draken/core   (draken/core in include_dirs)
//   "simd_hash.h"  from src/cpp       (src/cpp in include_dirs, added in C.1)

#include <cstdint>
#include <cstring>
#include <stdexcept>

#include "buffers.h"              // DrakenVector, DrakenType, DRAKEN_INT64 …
#include "simd_hash.h"            // simd_hash_i64, NULL_HASH, MIX_HASH_CONSTANT
#include "ops/vec_result.h"       // VecResult — owned vector from op kernels
#include "ops/int64_reductions.h" // i64_sum, i64_min, i64_max
#include "ops/int64_arithmetic.h" // i64_add, i64_sub, …
#include "ops/int64_gather.h"     // i64_take, i64_materialize, i64_compress
#include "ops/int64_compare.h"    // i64_compare_scalar, i64_compare_vector
#include "ops/int64_predicates.h" // i64_between, i64_in_list + CarcharSet
#include "ops/fixed_int_ops.h"    // int8/16/32 kernels (D.6)
#include "ops/float_ops.h"        // float32/64 kernels (D.7)
#include "ops/string_hash.h"        // hash_string
#include "ops/string_compare.h"     // str_compare_scalar, str_compare_vector
#include "ops/string_gather.h"      // str_take, str_materialize, str_compress
#include "ops/string_predicates.h"  // str_in_list
#include "ops/interval_ops.h"       // D.12 — DRAKEN_INTERVAL kernels
#include "ops/int_bitwise.h"        // E.2 — AND/OR/XOR/NOT/SHL/SHR across int8/16/32/64

// ---------------------------------------------------------------------------
// TypeOps: one entry per DrakenType in the dispatch table.
//
// Slots are added here as ops are implemented.  A null pointer means the op
// is not yet implemented for this type — the dispatcher throws rather than
// falling through to a boxed/Python fallback.
// ---------------------------------------------------------------------------
// Reduction function type: returns count of non-null contributing rows.
// out_value is always set (0 for empty/all-null sum; undefined for min/max when count==0).
typedef uint32_t (*ReduceFn)(const DrakenVector&, int64_t* out_value);

// Binary arithmetic: a op b → VecResult. Lengths must match.
typedef VecResult (*BinaryArithFn)(const DrakenVector& a, const DrakenVector& b);

// Scalar arithmetic: a op scalar → VecResult.
typedef VecResult (*ScalarArithFn)(const DrakenVector& a, int64_t scalar);

// Unary arithmetic.
typedef VecResult (*UnaryArithFn)(const DrakenVector& a);

// Gather ops.
typedef VecResult (*TakeFn)(const DrakenVector&, const int32_t* indices, uint32_t n);
typedef VecResult (*MatFn)(const DrakenVector&);
typedef VecResult (*CompFn)(const DrakenVector&);

// C.3 — compare ops: int64 scalar/value and int64 × int64 vector.
// op codes: 0=eq 1=ne 2=gt 3=ge 4=lt 5=le
typedef VecResult (*CmpScalarFn)(const DrakenVector& v, int64_t scalar, int op);
typedef VecResult (*CmpVecFn)(const DrakenVector& a, const DrakenVector& b, int op);

// C.4 — predicate ops: between and in_list → DRAKEN_BOOL result.
typedef VecResult (*BetweenFn)(const DrakenVector& v,
                               int64_t lo, int64_t hi,
                               bool lo_incl, bool hi_incl);
typedef VecResult (*InListFn)(const DrakenVector& v,
                              const opteryx::carchar::CarcharSet& set);

// D.2 — string-specific compare_scalar (slot + arena pointer, not int64 scalar).
// For strings the op codes are: 0=eq 1=ne 2=gt 3=ge 4=lt 5=le (same as int64).
// scalar_slot: pre-built DrakenStringSlot (arena_offset==0 for long strings).
// scalar_bytes: literal's UTF-8 bytes (valid for call duration; not stored).
typedef VecResult (*StrCmpScalarFn)(const DrakenVector& v,
                                    const DrakenStringSlot& scalar_slot,
                                    const uint8_t* scalar_bytes,
                                    int op);

// D.7 — float-specific function pointer types (double scalars / double output).
// Used for FLOAT32 and FLOAT64 where the int64 scalar/output types are wrong.
typedef uint32_t (*FloatReduceFn)(const DrakenVector&, double*);
typedef VecResult (*FloatScalarArithFn)(const DrakenVector&, double);
typedef VecResult (*FloatCmpScalarFn)(const DrakenVector&, double, int);
typedef VecResult (*FloatBetweenFn)(const DrakenVector&, double, double, bool, bool);

struct TypeOps {
    // C.1
    void (*hash)(const DrakenVector&, uint64_t*, uint32_t);
    // C.2 — reductions
    ReduceFn sum;
    ReduceFn min_r;
    ReduceFn max_r;
    // C.2 — binary arithmetic (vector × vector)
    BinaryArithFn add;
    BinaryArithFn sub;
    BinaryArithFn mul;
    BinaryArithFn div;
    BinaryArithFn mod;
    // C.2 — scalar arithmetic (vector × int64 scalar)
    ScalarArithFn add_s;
    ScalarArithFn sub_s;
    ScalarArithFn mul_s;
    ScalarArithFn div_s;
    ScalarArithFn mod_s;
    // C.2 — unary
    UnaryArithFn  neg;
    // C.2 — gather / reshape
    TakeFn take;
    MatFn  materialize;
    CompFn compress;
    // C.3 — compare → DRAKEN_BOOL result
    CmpScalarFn compare_scalar;
    CmpVecFn    compare_vector;
    // C.4 — predicate → DRAKEN_BOOL result
    BetweenFn   between;
    InListFn    in_list;
    // D.2 — string-specific compare_scalar (different signature from CmpScalarFn).
    StrCmpScalarFn str_compare_scalar;
    // D.7 — float-specific ops (double scalar/output; incompatible with int64 slots).
    FloatReduceFn    float_sum;
    FloatReduceFn    float_min_r;
    FloatReduceFn    float_max_r;
    FloatScalarArithFn float_add_s;
    FloatScalarArithFn float_sub_s;
    FloatScalarArithFn float_mul_s;
    FloatScalarArithFn float_div_s;
    FloatScalarArithFn float_mod_s;
    FloatCmpScalarFn float_compare_scalar;
    FloatBetweenFn   float_between;
    // E.2 — bitwise ops (int8/16/32/64 only; other types leave these null)
    BinaryArithFn bitwise_and;
    BinaryArithFn bitwise_or;
    BinaryArithFn bitwise_xor;
    UnaryArithFn  bitwise_not;
    BinaryArithFn bitwise_shl;
    BinaryArithFn bitwise_shr;
};

// ---------------------------------------------------------------------------
// int64 hash kernel.
//
// Reads data[selection[i+j]] for valid rows; substitutes NULL_HASH sentinel
// for null rows before passing the scratch block to simd_hash_i64.
// The branchless null-select formula matches c_hash_into in draken_old:
//   scratch = (cast_u64(data[sel]) * is_valid) | (NULL_HASH * (1 - is_valid))
// ---------------------------------------------------------------------------
static inline void hash_int64(const DrakenVector& v, uint64_t* out, uint32_t n) {
    if (n == 0) return;

    const int64_t* data     = static_cast<const int64_t*>(v.data);
    const uint8_t* validity = v.validity;
    uint64_t scratch[1024];

    uint32_t i = 0;
    while (i < n) {
        const uint32_t block = (n - i < 1024u) ? (n - i) : 1024u;

        if (validity != nullptr) {
            for (uint32_t j = 0; j < block; ++j) {
                const uint64_t is_valid =
                    (validity[(i + j) >> 3] >> ((i + j) & 7)) & 1u;
                scratch[j] =
                    (static_cast<uint64_t>(data[v.selection[i + j]]) * is_valid)
                    | (NULL_HASH * (1u - is_valid));
            }
        } else {
            for (uint32_t j = 0; j < block; ++j) {
                scratch[j] = static_cast<uint64_t>(data[v.selection[i + j]]);
            }
        }
        simd_hash_i64(scratch, out + i, block);
        i += block;
    }
}

// ---------------------------------------------------------------------------
// OpsTable: flat array[101] of TypeOps, indexed by DrakenType enum value.
// DRAKEN_NON_NATIVE == 100 is the highest tag → 101 entries cover all types.
// Unfilled entries are zero-initialized (null function pointers).
// ---------------------------------------------------------------------------
struct OpsTable {
    static constexpr unsigned kSize = 101u;
    TypeOps entries[kSize];

    OpsTable() noexcept {
        std::memset(entries, 0, sizeof(entries));
        // C.1
        entries[DRAKEN_INT64].hash       = hash_int64;
        // C.2 — reductions
        entries[DRAKEN_INT64].sum        = draken::ops::i64_sum;
        entries[DRAKEN_INT64].min_r      = draken::ops::i64_min;
        entries[DRAKEN_INT64].max_r      = draken::ops::i64_max;
        // C.2 — binary arithmetic
        entries[DRAKEN_INT64].add        = draken::ops::i64_add;
        entries[DRAKEN_INT64].sub        = draken::ops::i64_sub;
        entries[DRAKEN_INT64].mul        = draken::ops::i64_mul;
        entries[DRAKEN_INT64].div        = draken::ops::i64_div;
        entries[DRAKEN_INT64].mod        = draken::ops::i64_mod;
        // C.2 — scalar arithmetic
        entries[DRAKEN_INT64].add_s      = draken::ops::i64_add_scalar;
        entries[DRAKEN_INT64].sub_s      = draken::ops::i64_sub_scalar;
        entries[DRAKEN_INT64].mul_s      = draken::ops::i64_mul_scalar;
        entries[DRAKEN_INT64].div_s      = draken::ops::i64_div_scalar;
        entries[DRAKEN_INT64].mod_s      = draken::ops::i64_mod_scalar;
        // C.2 — unary
        entries[DRAKEN_INT64].neg        = draken::ops::i64_neg;
        // C.2 — gather
        entries[DRAKEN_INT64].take           = draken::ops::i64_take;
        entries[DRAKEN_INT64].materialize    = draken::ops::i64_materialize;
        entries[DRAKEN_INT64].compress       = draken::ops::i64_compress;
        // C.3 — compare
        entries[DRAKEN_INT64].compare_scalar = draken::ops::i64_compare_scalar;
        entries[DRAKEN_INT64].compare_vector = draken::ops::i64_compare_vector;
        // C.4 — predicates
        entries[DRAKEN_INT64].between        = draken::ops::i64_between;
        entries[DRAKEN_INT64].in_list        = draken::ops::i64_in_list;

        // D.6 — INT8
        entries[DRAKEN_INT8].hash          = draken::ops::hash_int8;
        entries[DRAKEN_INT8].sum           = draken::ops::i8_sum;
        entries[DRAKEN_INT8].min_r         = draken::ops::i8_min;
        entries[DRAKEN_INT8].max_r         = draken::ops::i8_max;
        entries[DRAKEN_INT8].add           = draken::ops::i8_add;
        entries[DRAKEN_INT8].sub           = draken::ops::i8_sub;
        entries[DRAKEN_INT8].mul           = draken::ops::i8_mul;
        entries[DRAKEN_INT8].div           = draken::ops::i8_div;
        entries[DRAKEN_INT8].mod           = draken::ops::i8_mod;
        entries[DRAKEN_INT8].add_s         = draken::ops::i8_add_scalar;
        entries[DRAKEN_INT8].sub_s         = draken::ops::i8_sub_scalar;
        entries[DRAKEN_INT8].mul_s         = draken::ops::i8_mul_scalar;
        entries[DRAKEN_INT8].div_s         = draken::ops::i8_div_scalar;
        entries[DRAKEN_INT8].mod_s         = draken::ops::i8_mod_scalar;
        entries[DRAKEN_INT8].neg           = draken::ops::i8_neg;
        entries[DRAKEN_INT8].take          = draken::ops::i8_take;
        entries[DRAKEN_INT8].materialize   = draken::ops::i8_materialize;
        entries[DRAKEN_INT8].compress      = draken::ops::i8_compress;
        entries[DRAKEN_INT8].compare_scalar = draken::ops::i8_compare_scalar;
        entries[DRAKEN_INT8].compare_vector = draken::ops::i8_compare_vector;
        entries[DRAKEN_INT8].between       = draken::ops::i8_between;
        entries[DRAKEN_INT8].in_list       = draken::ops::i8_in_list;

        // D.6 — INT16
        entries[DRAKEN_INT16].hash          = draken::ops::hash_int16;
        entries[DRAKEN_INT16].sum           = draken::ops::i16_sum;
        entries[DRAKEN_INT16].min_r         = draken::ops::i16_min;
        entries[DRAKEN_INT16].max_r         = draken::ops::i16_max;
        entries[DRAKEN_INT16].add           = draken::ops::i16_add;
        entries[DRAKEN_INT16].sub           = draken::ops::i16_sub;
        entries[DRAKEN_INT16].mul           = draken::ops::i16_mul;
        entries[DRAKEN_INT16].div           = draken::ops::i16_div;
        entries[DRAKEN_INT16].mod           = draken::ops::i16_mod;
        entries[DRAKEN_INT16].add_s         = draken::ops::i16_add_scalar;
        entries[DRAKEN_INT16].sub_s         = draken::ops::i16_sub_scalar;
        entries[DRAKEN_INT16].mul_s         = draken::ops::i16_mul_scalar;
        entries[DRAKEN_INT16].div_s         = draken::ops::i16_div_scalar;
        entries[DRAKEN_INT16].mod_s         = draken::ops::i16_mod_scalar;
        entries[DRAKEN_INT16].neg           = draken::ops::i16_neg;
        entries[DRAKEN_INT16].take          = draken::ops::i16_take;
        entries[DRAKEN_INT16].materialize   = draken::ops::i16_materialize;
        entries[DRAKEN_INT16].compress      = draken::ops::i16_compress;
        entries[DRAKEN_INT16].compare_scalar = draken::ops::i16_compare_scalar;
        entries[DRAKEN_INT16].compare_vector = draken::ops::i16_compare_vector;
        entries[DRAKEN_INT16].between       = draken::ops::i16_between;
        entries[DRAKEN_INT16].in_list       = draken::ops::i16_in_list;

        // D.6 — INT32
        entries[DRAKEN_INT32].hash          = draken::ops::hash_int32;
        entries[DRAKEN_INT32].sum           = draken::ops::i32_sum;
        entries[DRAKEN_INT32].min_r         = draken::ops::i32_min;
        entries[DRAKEN_INT32].max_r         = draken::ops::i32_max;
        entries[DRAKEN_INT32].add           = draken::ops::i32_add;
        entries[DRAKEN_INT32].sub           = draken::ops::i32_sub;
        entries[DRAKEN_INT32].mul           = draken::ops::i32_mul;
        entries[DRAKEN_INT32].div           = draken::ops::i32_div;
        entries[DRAKEN_INT32].mod           = draken::ops::i32_mod;
        entries[DRAKEN_INT32].add_s         = draken::ops::i32_add_scalar;
        entries[DRAKEN_INT32].sub_s         = draken::ops::i32_sub_scalar;
        entries[DRAKEN_INT32].mul_s         = draken::ops::i32_mul_scalar;
        entries[DRAKEN_INT32].div_s         = draken::ops::i32_div_scalar;
        entries[DRAKEN_INT32].mod_s         = draken::ops::i32_mod_scalar;
        entries[DRAKEN_INT32].neg           = draken::ops::i32_neg;
        entries[DRAKEN_INT32].take          = draken::ops::i32_take;
        entries[DRAKEN_INT32].materialize   = draken::ops::i32_materialize;
        entries[DRAKEN_INT32].compress      = draken::ops::i32_compress;
        entries[DRAKEN_INT32].compare_scalar = draken::ops::i32_compare_scalar;
        entries[DRAKEN_INT32].compare_vector = draken::ops::i32_compare_vector;
        entries[DRAKEN_INT32].between       = draken::ops::i32_between;
        entries[DRAKEN_INT32].in_list       = draken::ops::i32_in_list;

        // D.7 — FLOAT32
        entries[DRAKEN_FLOAT32].hash              = draken::ops::hash_float32;
        entries[DRAKEN_FLOAT32].float_sum         = draken::ops::f32_sum;
        entries[DRAKEN_FLOAT32].float_min_r       = draken::ops::f32_min;
        entries[DRAKEN_FLOAT32].float_max_r       = draken::ops::f32_max;
        entries[DRAKEN_FLOAT32].add               = draken::ops::f32_add;
        entries[DRAKEN_FLOAT32].float_add_s       = draken::ops::f32_add_scalar;
        entries[DRAKEN_FLOAT32].sub               = draken::ops::f32_sub;
        entries[DRAKEN_FLOAT32].float_sub_s       = draken::ops::f32_sub_scalar;
        entries[DRAKEN_FLOAT32].mul               = draken::ops::f32_mul;
        entries[DRAKEN_FLOAT32].float_mul_s       = draken::ops::f32_mul_scalar;
        entries[DRAKEN_FLOAT32].div               = draken::ops::f32_div;
        entries[DRAKEN_FLOAT32].float_div_s       = draken::ops::f32_div_scalar;
        entries[DRAKEN_FLOAT32].mod               = draken::ops::f32_mod;
        entries[DRAKEN_FLOAT32].float_mod_s       = draken::ops::f32_mod_scalar;
        entries[DRAKEN_FLOAT32].neg               = draken::ops::f32_neg;
        entries[DRAKEN_FLOAT32].take              = draken::ops::f32_take;
        entries[DRAKEN_FLOAT32].materialize       = draken::ops::f32_materialize;
        entries[DRAKEN_FLOAT32].compress          = draken::ops::f32_compress;
        entries[DRAKEN_FLOAT32].float_compare_scalar = draken::ops::f32_compare_scalar;
        entries[DRAKEN_FLOAT32].compare_vector    = draken::ops::f32_compare_vector;
        entries[DRAKEN_FLOAT32].float_between     = draken::ops::f32_between;
        entries[DRAKEN_FLOAT32].in_list           = draken::ops::f32_in_list;

        // D.7 — FLOAT64
        entries[DRAKEN_FLOAT64].hash              = draken::ops::hash_float64;
        entries[DRAKEN_FLOAT64].float_sum         = draken::ops::f64_sum;
        entries[DRAKEN_FLOAT64].float_min_r       = draken::ops::f64_min;
        entries[DRAKEN_FLOAT64].float_max_r       = draken::ops::f64_max;
        entries[DRAKEN_FLOAT64].add               = draken::ops::f64_add;
        entries[DRAKEN_FLOAT64].float_add_s       = draken::ops::f64_add_scalar;
        entries[DRAKEN_FLOAT64].sub               = draken::ops::f64_sub;
        entries[DRAKEN_FLOAT64].float_sub_s       = draken::ops::f64_sub_scalar;
        entries[DRAKEN_FLOAT64].mul               = draken::ops::f64_mul;
        entries[DRAKEN_FLOAT64].float_mul_s       = draken::ops::f64_mul_scalar;
        entries[DRAKEN_FLOAT64].div               = draken::ops::f64_div;
        entries[DRAKEN_FLOAT64].float_div_s       = draken::ops::f64_div_scalar;
        entries[DRAKEN_FLOAT64].mod               = draken::ops::f64_mod;
        entries[DRAKEN_FLOAT64].float_mod_s       = draken::ops::f64_mod_scalar;
        entries[DRAKEN_FLOAT64].neg               = draken::ops::f64_neg;
        entries[DRAKEN_FLOAT64].take              = draken::ops::f64_take;
        entries[DRAKEN_FLOAT64].materialize       = draken::ops::f64_materialize;
        entries[DRAKEN_FLOAT64].compress          = draken::ops::f64_compress;
        entries[DRAKEN_FLOAT64].float_compare_scalar = draken::ops::f64_compare_scalar;
        entries[DRAKEN_FLOAT64].compare_vector    = draken::ops::f64_compare_vector;
        entries[DRAKEN_FLOAT64].float_between     = draken::ops::f64_between;
        entries[DRAKEN_FLOAT64].in_list           = draken::ops::f64_in_list;

        // D.10 — DECIMAL: physical dispatch reuses INT64 kernels.
        // Logical DECIMAL(p,s) stores int64 unscaled values; all compare/hash/
        // reduction/gather ops are identical to INT64. Scale is handled at
        // ingestion and readback edges only; the hot path never reads it.
        entries[DRAKEN_DECIMAL] = entries[DRAKEN_INT64];

        // D.8 — TIMESTAMP64: physical dispatch reuses INT64 kernels.
        // Hot path dispatches on DRAKEN_TIMESTAMP64 and never reads the logical
        // type — unit / offset are handled at ingestion and readback edges only.
        entries[DRAKEN_TIMESTAMP64] = entries[DRAKEN_INT64];

        // D.9 — DATE32: physical dispatch reuses INT32 kernels. No logical descriptor.
        // Storage is int32 days-since-epoch; no parameterization needed.
        entries[DRAKEN_DATE32] = entries[DRAKEN_INT32];

        // D.9 — TIME32: physical dispatch reuses INT32 kernels.
        // Mandatory logical descriptor carries unit (s/ms); offset field unused.
        entries[DRAKEN_TIME32] = entries[DRAKEN_INT32];

        // D.9 — TIME64: physical dispatch reuses INT64 kernels.
        // Mandatory logical descriptor carries unit (us/ns); offset field unused.
        entries[DRAKEN_TIME64] = entries[DRAKEN_INT64];

        // D.12 — INTERVAL: normalized ops + component-wise arithmetic.
        // compare/hash/between/in_list normalize to total_ms = months×2_592_000_000+ms
        // before comparing.  Arithmetic is component-wise (months and ms independently).
        // sum/min_r/max_r are null: Python edge handles min/max via interval_find_min/max.
        entries[DRAKEN_INTERVAL].hash             = draken::ops::interval_hash;
        entries[DRAKEN_INTERVAL].compare_scalar   = draken::ops::interval_compare_scalar;
        entries[DRAKEN_INTERVAL].compare_vector   = draken::ops::interval_compare_vector;
        entries[DRAKEN_INTERVAL].between          = draken::ops::interval_between;
        entries[DRAKEN_INTERVAL].in_list          = draken::ops::interval_in_list;
        entries[DRAKEN_INTERVAL].add              = draken::ops::interval_add;
        entries[DRAKEN_INTERVAL].sub              = draken::ops::interval_sub;
        entries[DRAKEN_INTERVAL].neg              = draken::ops::interval_neg;
        entries[DRAKEN_INTERVAL].take             = draken::ops::interval_take;
        entries[DRAKEN_INTERVAL].materialize      = draken::ops::interval_materialize;
        entries[DRAKEN_INTERVAL].compress         = draken::ops::interval_compress;

        // D.2 — STRING: hash + compare
        entries[DRAKEN_STRING].hash               = draken::ops::hash_string;
        entries[DRAKEN_STRING].compare_vector     = draken::ops::str_compare_vector;
        entries[DRAKEN_STRING].str_compare_scalar = draken::ops::str_compare_scalar;
        // D.3 — STRING: gather / reshape
        entries[DRAKEN_STRING].take               = draken::ops::str_take;
        entries[DRAKEN_STRING].materialize        = draken::ops::str_materialize;
        entries[DRAKEN_STRING].compress           = draken::ops::str_compress;
        // D.4 — STRING: in_list (hash-only; §1 exception same as str eq/hash)
        entries[DRAKEN_STRING].in_list            = draken::ops::str_in_list;

        // E.2 — INT8 bitwise
        entries[DRAKEN_INT8].bitwise_and = draken::ops::i8_bitwise_and;
        entries[DRAKEN_INT8].bitwise_or  = draken::ops::i8_bitwise_or;
        entries[DRAKEN_INT8].bitwise_xor = draken::ops::i8_bitwise_xor;
        entries[DRAKEN_INT8].bitwise_not = draken::ops::i8_bitwise_not;
        entries[DRAKEN_INT8].bitwise_shl = draken::ops::i8_bitwise_shl;
        entries[DRAKEN_INT8].bitwise_shr = draken::ops::i8_bitwise_shr;

        // E.2 — INT16 bitwise
        entries[DRAKEN_INT16].bitwise_and = draken::ops::i16_bitwise_and;
        entries[DRAKEN_INT16].bitwise_or  = draken::ops::i16_bitwise_or;
        entries[DRAKEN_INT16].bitwise_xor = draken::ops::i16_bitwise_xor;
        entries[DRAKEN_INT16].bitwise_not = draken::ops::i16_bitwise_not;
        entries[DRAKEN_INT16].bitwise_shl = draken::ops::i16_bitwise_shl;
        entries[DRAKEN_INT16].bitwise_shr = draken::ops::i16_bitwise_shr;

        // E.2 — INT32 bitwise
        entries[DRAKEN_INT32].bitwise_and = draken::ops::i32_bitwise_and;
        entries[DRAKEN_INT32].bitwise_or  = draken::ops::i32_bitwise_or;
        entries[DRAKEN_INT32].bitwise_xor = draken::ops::i32_bitwise_xor;
        entries[DRAKEN_INT32].bitwise_not = draken::ops::i32_bitwise_not;
        entries[DRAKEN_INT32].bitwise_shl = draken::ops::i32_bitwise_shl;
        entries[DRAKEN_INT32].bitwise_shr = draken::ops::i32_bitwise_shr;

        // E.2 — INT64 bitwise
        entries[DRAKEN_INT64].bitwise_and = draken::ops::i64_bitwise_and;
        entries[DRAKEN_INT64].bitwise_or  = draken::ops::i64_bitwise_or;
        entries[DRAKEN_INT64].bitwise_xor = draken::ops::i64_bitwise_xor;
        entries[DRAKEN_INT64].bitwise_not = draken::ops::i64_bitwise_not;
        entries[DRAKEN_INT64].bitwise_shl = draken::ops::i64_bitwise_shl;
        entries[DRAKEN_INT64].bitwise_shr = draken::ops::i64_bitwise_shr;
    }
};

// Meyers singleton — constructed once, thread-safe from C++11 onward.
static inline const OpsTable& g_ops_table() {
    static const OpsTable t;
    return t;
}

// ---------------------------------------------------------------------------
// draken_hash: the only public entry point at this milestone.
//
// One table lookup → one indirect call per invocation; the typed kernel loop
// runs without any per-row dispatch.  Unsupported types throw — no boxed or
// Python-object fallback exists or should be added.
// ---------------------------------------------------------------------------
static inline void draken_hash(const DrakenVector& v, uint64_t* out, uint32_t n) {
    const unsigned idx = static_cast<unsigned>(v.type);
    if (idx >= OpsTable::kSize || g_ops_table().entries[idx].hash == nullptr)
        throw std::invalid_argument("draken_hash: unsupported type");
    g_ops_table().entries[idx].hash(v, out, n);
}

// ---------------------------------------------------------------------------
// C.2 dispatch entry points — one indirect table lookup, then typed kernel.
// All throw std::invalid_argument for unsupported types.
// ---------------------------------------------------------------------------

static inline uint32_t draken_sum(const DrakenVector& v, int64_t* out_value) {
    const unsigned idx = static_cast<unsigned>(v.type);
    if (idx >= OpsTable::kSize || g_ops_table().entries[idx].sum == nullptr)
        throw std::invalid_argument("draken_sum: unsupported type");
    return g_ops_table().entries[idx].sum(v, out_value);
}

static inline uint32_t draken_min(const DrakenVector& v, int64_t* out_value) {
    const unsigned idx = static_cast<unsigned>(v.type);
    if (idx >= OpsTable::kSize || g_ops_table().entries[idx].min_r == nullptr)
        throw std::invalid_argument("draken_min: unsupported type");
    return g_ops_table().entries[idx].min_r(v, out_value);
}

static inline uint32_t draken_max(const DrakenVector& v, int64_t* out_value) {
    const unsigned idx = static_cast<unsigned>(v.type);
    if (idx >= OpsTable::kSize || g_ops_table().entries[idx].max_r == nullptr)
        throw std::invalid_argument("draken_max: unsupported type");
    return g_ops_table().entries[idx].max_r(v, out_value);
}

#define DRAKEN_BINARY_ARITH(fn_name, slot) \
static inline VecResult fn_name(const DrakenVector& a, const DrakenVector& b) { \
    const unsigned idx = static_cast<unsigned>(a.type); \
    if (idx >= OpsTable::kSize || g_ops_table().entries[idx].slot == nullptr) \
        throw std::invalid_argument(#fn_name ": unsupported type"); \
    return g_ops_table().entries[idx].slot(a, b); \
}

#define DRAKEN_SCALAR_ARITH(fn_name, slot) \
static inline VecResult fn_name(const DrakenVector& a, int64_t scalar) { \
    const unsigned idx = static_cast<unsigned>(a.type); \
    if (idx >= OpsTable::kSize || g_ops_table().entries[idx].slot == nullptr) \
        throw std::invalid_argument(#fn_name ": unsupported type"); \
    return g_ops_table().entries[idx].slot(a, scalar); \
}

DRAKEN_BINARY_ARITH(draken_add, add)
DRAKEN_BINARY_ARITH(draken_sub, sub)
DRAKEN_BINARY_ARITH(draken_mul, mul)
DRAKEN_BINARY_ARITH(draken_div, div)
DRAKEN_BINARY_ARITH(draken_mod, mod)

DRAKEN_SCALAR_ARITH(draken_add_scalar, add_s)
DRAKEN_SCALAR_ARITH(draken_sub_scalar, sub_s)
DRAKEN_SCALAR_ARITH(draken_mul_scalar, mul_s)
DRAKEN_SCALAR_ARITH(draken_div_scalar, div_s)
DRAKEN_SCALAR_ARITH(draken_mod_scalar, mod_s)

#undef DRAKEN_BINARY_ARITH
#undef DRAKEN_SCALAR_ARITH

static inline VecResult draken_neg(const DrakenVector& v) {
    const unsigned idx = static_cast<unsigned>(v.type);
    if (idx >= OpsTable::kSize || g_ops_table().entries[idx].neg == nullptr)
        throw std::invalid_argument("draken_neg: unsupported type");
    return g_ops_table().entries[idx].neg(v);
}

static inline VecResult draken_take(
    const DrakenVector& v, const int32_t* indices, uint32_t n)
{
    const unsigned idx = static_cast<unsigned>(v.type);
    if (idx >= OpsTable::kSize || g_ops_table().entries[idx].take == nullptr)
        throw std::invalid_argument("draken_take: unsupported type");
    return g_ops_table().entries[idx].take(v, indices, n);
}

static inline VecResult draken_materialize(const DrakenVector& v) {
    const unsigned idx = static_cast<unsigned>(v.type);
    if (idx >= OpsTable::kSize || g_ops_table().entries[idx].materialize == nullptr)
        throw std::invalid_argument("draken_materialize: unsupported type");
    return g_ops_table().entries[idx].materialize(v);
}

static inline VecResult draken_compress(const DrakenVector& v) {
    const unsigned idx = static_cast<unsigned>(v.type);
    if (idx >= OpsTable::kSize || g_ops_table().entries[idx].compress == nullptr)
        throw std::invalid_argument("draken_compress: unsupported type");
    return g_ops_table().entries[idx].compress(v);
}

// C.3 — compare dispatch entry points.
// op codes: 0=eq 1=ne 2=gt 3=ge 4=lt 5=le
// Result type is always DRAKEN_BOOL (bit-packed, 1 bit/logical row, LSB-first).
// Unsupported types throw std::invalid_argument — no fallback exists.

static inline VecResult draken_compare_scalar(
    const DrakenVector& v, int64_t scalar, int op)
{
    const unsigned idx = static_cast<unsigned>(v.type);
    if (idx >= OpsTable::kSize || g_ops_table().entries[idx].compare_scalar == nullptr)
        throw std::invalid_argument("draken_compare_scalar: unsupported type");
    return g_ops_table().entries[idx].compare_scalar(v, scalar, op);
}

static inline VecResult draken_compare_vector(
    const DrakenVector& a, const DrakenVector& b, int op)
{
    const unsigned idx = static_cast<unsigned>(a.type);
    if (idx >= OpsTable::kSize || g_ops_table().entries[idx].compare_vector == nullptr)
        throw std::invalid_argument("draken_compare_vector: unsupported type");
    return g_ops_table().entries[idx].compare_vector(a, b, op);
}

// C.4 — between and in_list dispatch entry points.
// Both produce DRAKEN_BOOL (bit-packed, 1 bit/logical row, LSB-first).
// Unsupported types throw std::invalid_argument — no fallback exists.

static inline VecResult draken_between(
    const DrakenVector& v,
    int64_t lo, int64_t hi,
    bool lo_incl, bool hi_incl)
{
    const unsigned idx = static_cast<unsigned>(v.type);
    if (idx >= OpsTable::kSize || g_ops_table().entries[idx].between == nullptr)
        throw std::invalid_argument("draken_between: unsupported type");
    return g_ops_table().entries[idx].between(v, lo, hi, lo_incl, hi_incl);
}

static inline VecResult draken_in_list(
    const DrakenVector& v,
    const opteryx::carchar::CarcharSet& set)
{
    const unsigned idx = static_cast<unsigned>(v.type);
    if (idx >= OpsTable::kSize || g_ops_table().entries[idx].in_list == nullptr)
        throw std::invalid_argument("draken_in_list: unsupported type");
    return g_ops_table().entries[idx].in_list(v, set);
}

// D.7 — float-specific dispatch entry points.
// Separate from the int64 dispatchers because the scalar/output types differ.

static inline uint32_t draken_float_sum(const DrakenVector& v, double* out) {
    const unsigned idx = static_cast<unsigned>(v.type);
    if (idx >= OpsTable::kSize || g_ops_table().entries[idx].float_sum == nullptr)
        throw std::invalid_argument("draken_float_sum: unsupported type");
    return g_ops_table().entries[idx].float_sum(v, out);
}

static inline uint32_t draken_float_min(const DrakenVector& v, double* out) {
    const unsigned idx = static_cast<unsigned>(v.type);
    if (idx >= OpsTable::kSize || g_ops_table().entries[idx].float_min_r == nullptr)
        throw std::invalid_argument("draken_float_min: unsupported type");
    return g_ops_table().entries[idx].float_min_r(v, out);
}

static inline uint32_t draken_float_max(const DrakenVector& v, double* out) {
    const unsigned idx = static_cast<unsigned>(v.type);
    if (idx >= OpsTable::kSize || g_ops_table().entries[idx].float_max_r == nullptr)
        throw std::invalid_argument("draken_float_max: unsupported type");
    return g_ops_table().entries[idx].float_max_r(v, out);
}

#define DRAKEN_FLOAT_SCALAR_ARITH(fn_name, slot)                              \
static inline VecResult fn_name(const DrakenVector& a, double scalar) {       \
    const unsigned idx = static_cast<unsigned>(a.type);                        \
    if (idx >= OpsTable::kSize || g_ops_table().entries[idx].slot == nullptr)  \
        throw std::invalid_argument(#fn_name ": unsupported type");            \
    return g_ops_table().entries[idx].slot(a, scalar);                         \
}

DRAKEN_FLOAT_SCALAR_ARITH(draken_float_add_scalar, float_add_s)
DRAKEN_FLOAT_SCALAR_ARITH(draken_float_sub_scalar, float_sub_s)
DRAKEN_FLOAT_SCALAR_ARITH(draken_float_mul_scalar, float_mul_s)
DRAKEN_FLOAT_SCALAR_ARITH(draken_float_div_scalar, float_div_s)
DRAKEN_FLOAT_SCALAR_ARITH(draken_float_mod_scalar, float_mod_s)
#undef DRAKEN_FLOAT_SCALAR_ARITH

static inline VecResult draken_float_compare_scalar(
    const DrakenVector& v, double scalar, int op)
{
    const unsigned idx = static_cast<unsigned>(v.type);
    if (idx >= OpsTable::kSize || g_ops_table().entries[idx].float_compare_scalar == nullptr)
        throw std::invalid_argument("draken_float_compare_scalar: unsupported type");
    return g_ops_table().entries[idx].float_compare_scalar(v, scalar, op);
}

static inline VecResult draken_float_between(
    const DrakenVector& v, double lo, double hi, bool lo_incl, bool hi_incl)
{
    const unsigned idx = static_cast<unsigned>(v.type);
    if (idx >= OpsTable::kSize || g_ops_table().entries[idx].float_between == nullptr)
        throw std::invalid_argument("draken_float_between: unsupported type");
    return g_ops_table().entries[idx].float_between(v, lo, hi, lo_incl, hi_incl);
}

// D.2 — string compare_scalar dispatcher.
// scalar_slot: pre-built DrakenStringSlot (arena_offset==0 for long strings).
// scalar_bytes: the literal's UTF-8 bytes (not stored; valid for call duration).
// Ordering ops (gt/ge/lt/le) read arena bytes via scalar_bytes on prefix ties.
// op codes: 0=eq 1=ne 2=gt 3=ge 4=lt 5=le
static inline VecResult draken_str_compare_scalar(
    const DrakenVector&     v,
    const DrakenStringSlot& scalar_slot,
    const uint8_t*          scalar_bytes,
    int                     op)
{
    const unsigned idx = static_cast<unsigned>(v.type);
    if (idx >= OpsTable::kSize || g_ops_table().entries[idx].str_compare_scalar == nullptr)
        throw std::invalid_argument("draken_str_compare_scalar: unsupported type");
    return g_ops_table().entries[idx].str_compare_scalar(v, scalar_slot, scalar_bytes, op);
}

// E.2 — bitwise dispatch entry points (OpsTable-based, for hash.h consumers).
// These throw for any type that has no bitwise slot (floats, strings, bool, etc.).
// Consumers that include only int_bitwise.h use draken::ops::bitwise_* directly.

#define DRAKEN_BITWISE_BINARY(fn_name, slot)                                         \
static inline VecResult fn_name(const DrakenVector& a, const DrakenVector& b) {     \
    const unsigned idx = static_cast<unsigned>(a.type);                              \
    if (idx >= OpsTable::kSize || g_ops_table().entries[idx].slot == nullptr)        \
        throw std::invalid_argument(#fn_name ": unsupported type");                  \
    return g_ops_table().entries[idx].slot(a, b);                                    \
}

DRAKEN_BITWISE_BINARY(draken_bitwise_and, bitwise_and)
DRAKEN_BITWISE_BINARY(draken_bitwise_or,  bitwise_or)
DRAKEN_BITWISE_BINARY(draken_bitwise_xor, bitwise_xor)
DRAKEN_BITWISE_BINARY(draken_bitwise_shl, bitwise_shl)
DRAKEN_BITWISE_BINARY(draken_bitwise_shr, bitwise_shr)

#undef DRAKEN_BITWISE_BINARY

static inline VecResult draken_bitwise_not(const DrakenVector& v) {
    const unsigned idx = static_cast<unsigned>(v.type);
    if (idx >= OpsTable::kSize || g_ops_table().entries[idx].bitwise_not == nullptr)
        throw std::invalid_argument("draken_bitwise_not: unsupported type");
    return g_ops_table().entries[idx].bitwise_not(v);
}
