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
// ALGORITHM (int64):
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

#include <new>        // std::bad_alloc / placement new — not reliably pulled in by <stdexcept> on stricter libc++
#include "buffers.h"              // DrakenVector, DrakenType, DRAKEN_INT64 …
#include "simd_hash.h"            // simd_hash_i64, NULL_HASH, MIX_HASH_CONSTANT
#include "ops/vec_result.h"       // VecResult — owned vector from op kernels
#include "ops/int64_reductions.h" // i64_sum, i64_min, i64_max
#include "ops/int64_arithmetic.h" // i64_add, i64_sub, …
#include "ops/int64_gather.h"     // i64_take, i64_materialize, i64_compress
#include "ops/int128_gather.h"    // i128_take, i128_slice, i128_materialize, i128_compress (DECIMAL128)
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
#include "ops/decimal_arith.h"     // E.32 — scale-aware DECIMAL arithmetic kernels
#include "ops/uint64_arithmetic.h" // E33 — u64_add, u64_sub, … (genuine unsigned semantics)
#include "ops/uint64_compare.h"    // E33 — u64_compare_scalar, u64_compare_vector

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
typedef VecResult (*SliceFn)(const DrakenVector&, uint32_t start, uint32_t length);
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

// D.x — string-specific between (slot+arena bounds, incompatible with int64 BetweenFn).
// lo_slot/lo_bytes: pre-built lower bound slot + literal bytes (same construction as
// StrCmpScalarFn scalar_slot/scalar_bytes — arena_offset==0 for long strings).
// hi_slot/hi_bytes: same for upper bound.
typedef VecResult (*StrBetweenFn)(const DrakenVector& v,
                                  const DrakenStringSlot& lo_slot,
                                  const uint8_t* lo_bytes,
                                  const DrakenStringSlot& hi_slot,
                                  const uint8_t* hi_bytes,
                                  bool lo_incl, bool hi_incl);

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
    TakeFn  take;
    SliceFn slice;
    MatFn   materialize;
    CompFn  compress;
    // C.3 — compare → DRAKEN_BOOL result
    CmpScalarFn compare_scalar;
    CmpVecFn    compare_vector;
    // C.4 — predicate → DRAKEN_BOOL result
    BetweenFn   between;
    InListFn    in_list;
    // D.2 — string-specific compare_scalar (different signature from CmpScalarFn).
    StrCmpScalarFn str_compare_scalar;
    // D.x — string-specific between (slot+arena bounds, incompatible with int64 BetweenFn).
    StrBetweenFn   str_between;
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
// The branchless null-select formula is:
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
        } else if (v.flags & DRAKEN_SEL_IDENTITY) {
            // Identity selection → contiguous copy (vectorisable), no gather.
            for (uint32_t j = 0; j < block; ++j)
                scratch[j] = static_cast<uint64_t>(data[i + j]);
        } else {
            for (uint32_t j = 0; j < block; ++j) {
                scratch[j] = static_cast<uint64_t>(data[v.selection[i + j]]);
            }
        }
        simd_hash_i64(scratch, out + i, block);
        i += block;
    }
}

// DECIMAL128 (int128) row hash. CROSS-TIER CONSISTENT with hash_int64: a value that
// fits int64 hashes via seed = its low 64 bits (identical to the int64-decimal of the
// same value), so a DECIMAL128 key collides with a DECIMAL64 key of equal value in
// mixed-tier group-by / joins. Wider values mix both 64-bit halves. Null → NULL_HASH.
static inline void hash_decimal128(const DrakenVector& v, uint64_t* out, uint32_t n) {
    if (n == 0) return;

    const __int128* data    = static_cast<const __int128*>(v.data);
    const uint8_t* validity = v.validity;
    uint64_t scratch[1024];

    uint32_t i = 0;
    while (i < n) {
        const uint32_t block = (n - i < 1024u) ? (n - i) : 1024u;
        for (uint32_t j = 0; j < block; ++j) {
            if (validity != nullptr &&
                !((validity[(i + j) >> 3] >> ((i + j) & 7)) & 1u)) {
                scratch[j] = NULL_HASH;
            } else {
                const __int128 x  = data[v.selection[i + j]];
                const uint64_t lo = static_cast<uint64_t>(x);
                const uint64_t hi = static_cast<uint64_t>(x >> 64);
                scratch[j] = (hi == static_cast<uint64_t>(static_cast<int64_t>(lo) >> 63))
                    ? lo
                    : (lo ^ (hi * 0x9E3779B97F4A7C15ULL));
            }
        }
        simd_hash_i64(scratch, out + i, block);
        i += block;
    }
}

// ---------------------------------------------------------------------------
// OpsTable: flat array[108] of TypeOps, indexed by DrakenType enum value.
// The D.11 tail runs NULL=101, VECTOR_FP16=102, DECIMAL128=103, UINT8=104,
// UINT16=105, UINT32=106, UINT64=107, so 108 entries cover all tags. Index 100 is
// a permanently-burned hole (the retired DRAKEN_NON_NATIVE; see core/buffers.h) and
// stays zero-filled like any unregistered type. NULL / VECTOR_FP16 are handled at the nanobind boundary and keep zero
// (null) slots here; DECIMAL128 fills only the gather slots (its arithmetic/hash/
// reduction/compare are boundary-intercepted too). UINT8/16/32/64 are unregistered
// (zero slots) pending Stage 2 kernel parity (E33) — dispatching an unregistered op
// on them fails loudly via the kSize/nullptr guard below, not silently.
// Unfilled entries are zero-initialized (null function pointers).
// ---------------------------------------------------------------------------
struct OpsTable {
    static constexpr unsigned kSize = 108u;
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
        entries[DRAKEN_INT64].slice          = draken::ops::i64_slice;
        entries[DRAKEN_INT64].materialize    = draken::ops::i64_materialize;
        entries[DRAKEN_INT64].compress       = draken::ops::i64_compress;
        // C.3 — compare
        entries[DRAKEN_INT64].compare_scalar = draken::ops::i64_compare_scalar;
        entries[DRAKEN_INT64].compare_vector = draken::ops::i64_compare_vector;
        // C.4 — predicates
        entries[DRAKEN_INT64].between        = draken::ops::i64_between;
        entries[DRAKEN_INT64].in_list        = draken::ops::i64_in_list;

        // BOOL — bit-packed hash kernel; keying for GROUP BY / DISTINCT / JOIN
        // on a boolean key. Storage/compare/take handled elsewhere; only the
        // keying hash slot lives here.
        entries[DRAKEN_BOOL].hash          = draken::ops::hash_bool;

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
        entries[DRAKEN_INT8].slice         = draken::ops::i8_slice;
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
        entries[DRAKEN_INT16].slice         = draken::ops::i16_slice;
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
        entries[DRAKEN_INT32].slice         = draken::ops::i32_slice;
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
        entries[DRAKEN_FLOAT32].slice             = draken::ops::f32_slice;
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
        entries[DRAKEN_FLOAT64].slice             = draken::ops::f64_slice;
        entries[DRAKEN_FLOAT64].materialize       = draken::ops::f64_materialize;
        entries[DRAKEN_FLOAT64].compress          = draken::ops::f64_compress;
        entries[DRAKEN_FLOAT64].float_compare_scalar = draken::ops::f64_compare_scalar;
        entries[DRAKEN_FLOAT64].compare_vector    = draken::ops::f64_compare_vector;
        entries[DRAKEN_FLOAT64].float_between     = draken::ops::f64_between;
        entries[DRAKEN_FLOAT64].in_list           = draken::ops::f64_in_list;

        // D.10 / E.32 — DECIMAL: hash/compare/gather/reduction are identical to INT64
        // (scale is transparent to these ops). Arithmetic is scale-aware and intercepted
        // at the nanobind boundary in draken_native.cpp BEFORE OpsTable dispatch; the
        // arithmetic slots are explicitly cleared here to prevent wrong-scale dispatch if
        // reached through OpsTable directly. See decimal_arith.h for the kernels.
        entries[DRAKEN_DECIMAL] = entries[DRAKEN_INT64];
        entries[DRAKEN_DECIMAL].add   = nullptr;
        entries[DRAKEN_DECIMAL].sub   = nullptr;
        entries[DRAKEN_DECIMAL].mul   = nullptr;
        entries[DRAKEN_DECIMAL].div   = nullptr;
        entries[DRAKEN_DECIMAL].mod   = nullptr;
        entries[DRAKEN_DECIMAL].add_s = nullptr;
        entries[DRAKEN_DECIMAL].sub_s = nullptr;
        entries[DRAKEN_DECIMAL].mul_s = nullptr;
        entries[DRAKEN_DECIMAL].div_s = nullptr;
        entries[DRAKEN_DECIMAL].mod_s = nullptr;
        entries[DRAKEN_DECIMAL].neg   = nullptr;

        // DECIMAL128 (int128, 16-byte): only the GATHER ops dispatch through OpsTable
        // (slice/take/materialize/compress). Arithmetic, hash, reductions, and compare
        // are all intercepted at the nanobind boundary BEFORE OpsTable (decimal_arith.h
        // kernels), so those slots stay null. These four are itemsize-specific (16-byte),
        // so they can't reuse the int64 slots the way DRAKEN_DECIMAL does.
        entries[DRAKEN_DECIMAL128].take        = draken::ops::i128_take;
        entries[DRAKEN_DECIMAL128].slice       = draken::ops::i128_slice;
        entries[DRAKEN_DECIMAL128].materialize = draken::ops::i128_materialize;
        entries[DRAKEN_DECIMAL128].compress    = draken::ops::i128_compress;
        // hash slot: needed by the multi-column key-hash path (c_hash → draken_hash)
        // and group-by/join keying. Cross-tier consistent with the int64 hash.
        entries[DRAKEN_DECIMAL128].hash        = hash_decimal128;

        // E33 — UINT8/16/32/64. Gather ops (take/slice/materialize/compress),
        // hash (GROUP BY/JOIN keys), arithmetic/compare/reductions/between/
        // in_list for all four widths — UINT8/16/32 fit safely in the existing
        // int64_t-based templates; UINT64 uses dedicated genuine-unsigned
        // kernels for anything that does a real ORDER comparison (compare_*,
        // between, sum/min/max's reported value) since the int64_t cast those
        // templates use would misorder a value >= 2^63 as negative. hash and
        // in_list are the one exception: safe to reuse the existing templates
        // even at 64-bit width, because they only need bit-pattern consistency
        // (same value hashes/matches the same way), not a correctly-ordered
        // numeric interpretation. NOT registered (stays null, fails loud via
        // the kSize/nullptr guard rather than silently miscomputing):
        //   - neg: unsigned has no negation.
        // UINT64 sum/min/max ARE registered — see u64_sum/min/max in
        // uint64_arithmetic.h — but the Python-boxing sites (sum()/min()/max()
        // in this file's nanobind Vector class) must reinterpret the returned
        // int64_t bits as uint64_t; verify that's done wherever they're wired.
        entries[DRAKEN_UINT8].hash        = draken::ops::hash_uint8;
        entries[DRAKEN_UINT8].take        = draken::ops::u8_take;
        entries[DRAKEN_UINT8].slice       = draken::ops::u8_slice;
        entries[DRAKEN_UINT8].materialize = draken::ops::u8_materialize;
        entries[DRAKEN_UINT8].compress    = draken::ops::u8_compress;
        entries[DRAKEN_UINT8].add           = draken::ops::u8_add;
        entries[DRAKEN_UINT8].sub           = draken::ops::u8_sub;
        entries[DRAKEN_UINT8].mul           = draken::ops::u8_mul;
        entries[DRAKEN_UINT8].div           = draken::ops::u8_div;
        entries[DRAKEN_UINT8].mod           = draken::ops::u8_mod;
        entries[DRAKEN_UINT8].add_s         = draken::ops::u8_add_scalar;
        entries[DRAKEN_UINT8].sub_s         = draken::ops::u8_sub_scalar;
        entries[DRAKEN_UINT8].mul_s         = draken::ops::u8_mul_scalar;
        entries[DRAKEN_UINT8].div_s         = draken::ops::u8_div_scalar;
        entries[DRAKEN_UINT8].mod_s         = draken::ops::u8_mod_scalar;
        entries[DRAKEN_UINT8].compare_scalar = draken::ops::u8_compare_scalar;
        entries[DRAKEN_UINT8].compare_vector = draken::ops::u8_compare_vector;
        entries[DRAKEN_UINT8].between        = draken::ops::u8_between;
        entries[DRAKEN_UINT8].in_list        = draken::ops::u8_in_list;
        entries[DRAKEN_UINT8].sum           = draken::ops::u8_sum;
        entries[DRAKEN_UINT8].min_r         = draken::ops::u8_min;
        entries[DRAKEN_UINT8].max_r         = draken::ops::u8_max;

        entries[DRAKEN_UINT16].hash        = draken::ops::hash_uint16;
        entries[DRAKEN_UINT16].take        = draken::ops::u16_take;
        entries[DRAKEN_UINT16].slice       = draken::ops::u16_slice;
        entries[DRAKEN_UINT16].materialize = draken::ops::u16_materialize;
        entries[DRAKEN_UINT16].compress    = draken::ops::u16_compress;
        entries[DRAKEN_UINT16].add           = draken::ops::u16_add;
        entries[DRAKEN_UINT16].sub           = draken::ops::u16_sub;
        entries[DRAKEN_UINT16].mul           = draken::ops::u16_mul;
        entries[DRAKEN_UINT16].div           = draken::ops::u16_div;
        entries[DRAKEN_UINT16].mod           = draken::ops::u16_mod;
        entries[DRAKEN_UINT16].add_s         = draken::ops::u16_add_scalar;
        entries[DRAKEN_UINT16].sub_s         = draken::ops::u16_sub_scalar;
        entries[DRAKEN_UINT16].mul_s         = draken::ops::u16_mul_scalar;
        entries[DRAKEN_UINT16].div_s         = draken::ops::u16_div_scalar;
        entries[DRAKEN_UINT16].mod_s         = draken::ops::u16_mod_scalar;
        entries[DRAKEN_UINT16].compare_scalar = draken::ops::u16_compare_scalar;
        entries[DRAKEN_UINT16].compare_vector = draken::ops::u16_compare_vector;
        entries[DRAKEN_UINT16].between        = draken::ops::u16_between;
        entries[DRAKEN_UINT16].in_list        = draken::ops::u16_in_list;
        entries[DRAKEN_UINT16].sum           = draken::ops::u16_sum;
        entries[DRAKEN_UINT16].min_r         = draken::ops::u16_min;
        entries[DRAKEN_UINT16].max_r         = draken::ops::u16_max;

        entries[DRAKEN_UINT32].hash        = draken::ops::hash_uint32;
        entries[DRAKEN_UINT32].take        = draken::ops::u32_take;
        entries[DRAKEN_UINT32].slice       = draken::ops::u32_slice;
        entries[DRAKEN_UINT32].materialize = draken::ops::u32_materialize;
        entries[DRAKEN_UINT32].compress    = draken::ops::u32_compress;
        entries[DRAKEN_UINT32].add           = draken::ops::u32_add;
        entries[DRAKEN_UINT32].sub           = draken::ops::u32_sub;
        entries[DRAKEN_UINT32].mul           = draken::ops::u32_mul;
        entries[DRAKEN_UINT32].div           = draken::ops::u32_div;
        entries[DRAKEN_UINT32].mod           = draken::ops::u32_mod;
        entries[DRAKEN_UINT32].add_s         = draken::ops::u32_add_scalar;
        entries[DRAKEN_UINT32].sub_s         = draken::ops::u32_sub_scalar;
        entries[DRAKEN_UINT32].mul_s         = draken::ops::u32_mul_scalar;
        entries[DRAKEN_UINT32].div_s         = draken::ops::u32_div_scalar;
        entries[DRAKEN_UINT32].mod_s         = draken::ops::u32_mod_scalar;
        entries[DRAKEN_UINT32].compare_scalar = draken::ops::u32_compare_scalar;
        entries[DRAKEN_UINT32].compare_vector = draken::ops::u32_compare_vector;
        entries[DRAKEN_UINT32].between        = draken::ops::u32_between;
        entries[DRAKEN_UINT32].in_list        = draken::ops::u32_in_list;
        entries[DRAKEN_UINT32].sum           = draken::ops::u32_sum;
        entries[DRAKEN_UINT32].min_r         = draken::ops::u32_min;
        entries[DRAKEN_UINT32].max_r         = draken::ops::u32_max;

        entries[DRAKEN_UINT64].hash        = draken::ops::hash_uint64;
        entries[DRAKEN_UINT64].take        = draken::ops::u64_take;
        entries[DRAKEN_UINT64].slice       = draken::ops::u64_slice;
        entries[DRAKEN_UINT64].materialize = draken::ops::u64_materialize;
        entries[DRAKEN_UINT64].compress    = draken::ops::u64_compress;
        entries[DRAKEN_UINT64].add           = draken::ops::u64_add;
        entries[DRAKEN_UINT64].sub           = draken::ops::u64_sub;
        entries[DRAKEN_UINT64].mul           = draken::ops::u64_mul;
        entries[DRAKEN_UINT64].div           = draken::ops::u64_div;
        entries[DRAKEN_UINT64].mod           = draken::ops::u64_mod;
        entries[DRAKEN_UINT64].add_s         = draken::ops::u64_add_scalar;
        entries[DRAKEN_UINT64].sub_s         = draken::ops::u64_sub_scalar;
        entries[DRAKEN_UINT64].mul_s         = draken::ops::u64_mul_scalar;
        entries[DRAKEN_UINT64].div_s         = draken::ops::u64_div_scalar;
        entries[DRAKEN_UINT64].mod_s         = draken::ops::u64_mod_scalar;
        entries[DRAKEN_UINT64].compare_scalar = draken::ops::u64_compare_scalar;
        entries[DRAKEN_UINT64].compare_vector = draken::ops::u64_compare_vector;
        entries[DRAKEN_UINT64].sum           = draken::ops::u64_sum;
        entries[DRAKEN_UINT64].min_r         = draken::ops::u64_min;
        entries[DRAKEN_UINT64].max_r         = draken::ops::u64_max;
        entries[DRAKEN_UINT64].between        = draken::ops::u64_between;
        entries[DRAKEN_UINT64].in_list        = draken::ops::u64_in_list;

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
        entries[DRAKEN_INTERVAL].slice            = draken::ops::interval_slice;
        entries[DRAKEN_INTERVAL].materialize      = draken::ops::interval_materialize;
        entries[DRAKEN_INTERVAL].compress         = draken::ops::interval_compress;

        // D.2 — VARCHAR: hash + compare
        entries[DRAKEN_VARCHAR].hash               = draken::ops::hash_string;
        entries[DRAKEN_VARCHAR].compare_vector     = draken::ops::str_compare_vector;
        entries[DRAKEN_VARCHAR].str_compare_scalar = draken::ops::str_compare_scalar;
        // D.3 — VARCHAR: gather / reshape
        entries[DRAKEN_VARCHAR].take               = draken::ops::str_take;
        entries[DRAKEN_VARCHAR].slice              = draken::ops::str_slice;
        entries[DRAKEN_VARCHAR].materialize        = draken::ops::str_materialize;
        entries[DRAKEN_VARCHAR].compress           = draken::ops::str_compress;
        // D.4 — VARCHAR: in_list (hash-only; §1 exception same as str eq/hash)
        entries[DRAKEN_VARCHAR].in_list            = draken::ops::str_in_list;
        // D.x — VARCHAR: between (lexicographic; slot+arena bounds)
        entries[DRAKEN_VARCHAR].str_between        = draken::ops::str_between;

        // E.7 — NVARCHAR: identical storage; same ops as VARCHAR
        entries[DRAKEN_NVARCHAR].hash               = draken::ops::hash_string;
        entries[DRAKEN_NVARCHAR].compare_vector     = draken::ops::str_compare_vector;
        entries[DRAKEN_NVARCHAR].str_compare_scalar = draken::ops::str_compare_scalar;
        entries[DRAKEN_NVARCHAR].take               = draken::ops::str_take;
        entries[DRAKEN_NVARCHAR].slice              = draken::ops::str_slice;
        entries[DRAKEN_NVARCHAR].materialize        = draken::ops::str_materialize;
        entries[DRAKEN_NVARCHAR].compress           = draken::ops::str_compress;
        entries[DRAKEN_NVARCHAR].in_list            = draken::ops::str_in_list;
        entries[DRAKEN_NVARCHAR].str_between        = draken::ops::str_between;

        // E.7 — VARBINARY: identical storage; same ops as VARCHAR
        entries[DRAKEN_VARBINARY].hash               = draken::ops::hash_string;
        entries[DRAKEN_VARBINARY].compare_vector     = draken::ops::str_compare_vector;
        entries[DRAKEN_VARBINARY].str_compare_scalar = draken::ops::str_compare_scalar;
        entries[DRAKEN_VARBINARY].take               = draken::ops::str_take;
        entries[DRAKEN_VARBINARY].slice              = draken::ops::str_slice;
        entries[DRAKEN_VARBINARY].materialize        = draken::ops::str_materialize;
        entries[DRAKEN_VARBINARY].compress           = draken::ops::str_compress;
        entries[DRAKEN_VARBINARY].in_list            = draken::ops::str_in_list;

        // VARIANT — German-string storage (JSON text); shares the string kernels so
        // VARIANT vectors flow through take/slice/materialize/compress/hash/joins.
        entries[DRAKEN_VARIANT].hash               = draken::ops::hash_string;
        entries[DRAKEN_VARIANT].compare_vector     = draken::ops::str_compare_vector;
        entries[DRAKEN_VARIANT].str_compare_scalar = draken::ops::str_compare_scalar;
        entries[DRAKEN_VARIANT].take               = draken::ops::str_take;
        entries[DRAKEN_VARIANT].slice              = draken::ops::str_slice;
        entries[DRAKEN_VARIANT].materialize        = draken::ops::str_materialize;
        entries[DRAKEN_VARIANT].compress           = draken::ops::str_compress;
        entries[DRAKEN_VARIANT].in_list            = draken::ops::str_in_list;

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
// draken_hash_distinct — hash the data_length distinct VALUES of v into
// out[0..data_length).  Ignores selection and validity: it hashes the value
// array as if it were a dense vector of data_length rows.  Reuses the per-type
// kernel via a dense view, so there is ONE place that hashes distinct values
// regardless of type.  The dense view is by construction dense (data_length ==
// length), so no recursion into a compressed fast-path occurs.
// ---------------------------------------------------------------------------
static inline void draken_hash_distinct(const DrakenVector& v, uint64_t* out) {
    if (v.data_length == 0u) return;
    DrakenVector dv = v;
    dv.selection   = draken_identity_sel(v.data_length);
    dv.length      = v.data_length;
    dv.data_length = v.data_length;
    dv.validity    = nullptr;  // distinct values carry no per-row nullness
    dv.flags       = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    draken_hash(dv, out, v.data_length);
}

// ---------------------------------------------------------------------------
// draken_hash_shaped — shape-preserving hash producer; the keying entry point
// for group-by / join / distinct.
//
// INVARIANT: hashing preserves shape.  Null keys are baked to NULL_HASH in the
// distinct set so they collide by default.  The hash vector is PURE: every slot
// holds a real hash value, so its validity is always NULL (a hash is never
// "absent").  Consumers that need per-row null semantics (join: non-matching;
// group-by: NULL group label) read the KEY column's validity — which they hold —
// NOT the hash vector.  Multi-column mixing is the caller's job and collapses to
// dense.
//
// Returns an INT64 VecResult, self-contained (owned data/selection), validity
// always NULL:
//   dense (dl == n)        -> data = n hashes (NULL_HASH baked per-row by kernel),
//                             global identity selection
//   non-null compressed    -> data = k hashes, selection = OWNED copy of codes
//   nullable compressed    -> data = (k+1) hashes, data[k] = NULL_HASH,
//                             selection = OWNED codes (null rows -> k)
// ---------------------------------------------------------------------------
static inline VecResult draken_hash_shaped(const DrakenVector& v) {
    VecResult r;
    r.type             = DRAKEN_INT64;
    r.flags            = 0u;
    r.owns_selection   = false;
    r.validity         = nullptr;   // a hash vector is always fully valid
    r.validity_embedded = 0u;
    r.ts_unit          = 0xFFu;
    const uint32_t n   = v.length;

    // Dense (and the n==0 corner): one hash per row, NULL_HASH baked by the
    // kernel for null rows. Global identity selection.
    if (n == 0u || draken_is_dense(&v)) {
        uint64_t* data = static_cast<uint64_t*>(
            draken_malloc((n > 0u ? n : 1u) * sizeof(uint64_t)));
        if (data == nullptr) throw std::bad_alloc();
        if (n > 0u) draken_hash(v, data, n);
        r.data           = data;
        r.selection      = draken_identity_sel(n);
        r.owns_selection = false;
        r.data_length    = n;
        r.length         = n;
        r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
        return r;
    }

    // Compressed: hash the k distinct values once.
    const uint32_t k = v.data_length;
    const bool nullable = (v.validity != nullptr);
    const uint32_t out_k = nullable ? (k + 1u) : k;

    uint64_t* data = static_cast<uint64_t*>(draken_malloc(out_k * sizeof(uint64_t)));
    if (data == nullptr) throw std::bad_alloc();
    draken_hash_distinct(v, data);          // fills data[0..k)

    uint32_t* codes = static_cast<uint32_t*>(draken_malloc(n * sizeof(uint32_t)));
    if (codes == nullptr) { draken_free(data); throw std::bad_alloc(); }

    if (!nullable) {
        // Non-null: codes are the source codes verbatim (owned copy).
        std::memcpy(codes, v.selection, static_cast<size_t>(n) * sizeof(uint32_t));
    } else {
        // Nullable: bake a null slot at index k, remap null rows to it so they
        // collide. The null slot's hash must match how null rows hash on the
        // dense path — i.e. the NULL_HASH sentinel run through simd_hash_i64,
        // not the raw sentinel (the k distinct values are mixed, so this must
        // be too). The key's per-row null mask stays on the key column.
        uint64_t null_seed = static_cast<uint64_t>(NULL_HASH);
        simd_hash_i64(&null_seed, &data[k], 1u);
        const uint8_t* val = v.validity;
        for (uint32_t i = 0; i < n; ++i) {
            const uint64_t is_valid = (val[i >> 3] >> (i & 7u)) & 1u;
            codes[i] = is_valid ? v.selection[i] : k;
        }
    }

    r.data           = data;
    r.selection      = codes;
    r.owns_selection = true;
    r.data_length    = out_k;
    r.length         = n;
    return r;
}

// ---------------------------------------------------------------------------
// E37 carried key-hash — the reuse twins of draken_hash / draken_hash_shaped.
//
// When a producer has pre-computed the per-data-element hash SEED
// (str_hash_seed) into keyhash[k], these skip re-seeding from the arena and only
// run the identical simd_hash_i64 mix + NULL baking. The output is byte-identical
// to draken_hash / draken_hash_shaped by construction (pure hoisting of the seed
// step). keyhash is indexed by data-element: keyhash[selection[i]] is row i's
// seed, so it is uniform across dense/dict/constant shapes. See
// draken/docs/design/E37_carried_key_hash.md.
// ---------------------------------------------------------------------------

// Dense per-column hash from carried seeds: out[i] = simd_hash_i64(seed_i),
// NULL_HASH baked for null rows. Mirrors hash_string's dense loop exactly, with
// the str_hash_seed(...) call replaced by a keyhash[selection[i]] load. Used both
// by the multi-column key mix and the dense branch of draken_hash_shaped_carried.
static inline void draken_hash_carried_dense(const DrakenVector& v,
                                             const uint64_t* keyhash,
                                             uint64_t* out, uint32_t n) {
    if (n == 0u) return;
    const uint8_t*  val = v.validity;
    const uint32_t* sel = v.selection;
    uint64_t scratch[1024];
    uint32_t i = 0u;
    while (i < n) {
        const uint32_t block = (n - i < 1024u) ? (n - i) : 1024u;
        if (val != nullptr) {
            for (uint32_t j = 0u; j < block; ++j) {
                const uint32_t lr = i + j;
                const uint64_t ok = (val[lr >> 3] >> (lr & 7u)) & 1u;
                scratch[j] = ok ? keyhash[sel[lr]] : static_cast<uint64_t>(NULL_HASH);
            }
        } else {
            for (uint32_t j = 0u; j < block; ++j) scratch[j] = keyhash[sel[i + j]];
        }
        simd_hash_i64(scratch, out + i, block);
        i += block;
    }
}

// Shape-preserving twin of draken_hash_shaped sourcing seeds from keyhash. Same
// dense/compressed output contract, same null baking and owned-codes remap.
static inline VecResult draken_hash_shaped_carried(const DrakenVector& v,
                                                   const uint64_t* keyhash) {
    VecResult r;
    r.type              = DRAKEN_INT64;
    r.flags             = 0u;
    r.owns_selection    = false;
    r.validity          = nullptr;
    r.validity_embedded = 0u;
    r.ts_unit           = 0xFFu;
    const uint32_t n    = v.length;

    if (n == 0u || draken_is_dense(&v)) {
        uint64_t* data = static_cast<uint64_t*>(
            draken_malloc((n > 0u ? n : 1u) * sizeof(uint64_t)));
        if (data == nullptr) throw std::bad_alloc();
        draken_hash_carried_dense(v, keyhash, data, n);
        r.data           = data;
        r.selection      = draken_identity_sel(n);
        r.owns_selection = false;
        r.data_length    = n;
        r.length         = n;
        r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
        return r;
    }

    // Compressed: mix the k distinct seeds once (draken_hash_distinct builds a
    // synthetic identity/no-validity view, so distinct hashing reads keyhash[j]).
    const uint32_t k = v.data_length;
    const bool nullable = (v.validity != nullptr);
    const uint32_t out_k = nullable ? (k + 1u) : k;

    uint64_t* data = static_cast<uint64_t*>(draken_malloc(out_k * sizeof(uint64_t)));
    if (data == nullptr) throw std::bad_alloc();
    {
        uint64_t scratch[1024];
        uint32_t j = 0u;
        while (j < k) {
            const uint32_t block = (k - j < 1024u) ? (k - j) : 1024u;
            for (uint32_t t = 0u; t < block; ++t) scratch[t] = keyhash[j + t];
            simd_hash_i64(scratch, data + j, block);
            j += block;
        }
    }

    uint32_t* codes = static_cast<uint32_t*>(draken_malloc(n * sizeof(uint32_t)));
    if (codes == nullptr) { draken_free(data); throw std::bad_alloc(); }
    if (!nullable) {
        std::memcpy(codes, v.selection, static_cast<size_t>(n) * sizeof(uint32_t));
    } else {
        uint64_t null_seed = static_cast<uint64_t>(NULL_HASH);
        simd_hash_i64(&null_seed, &data[k], 1u);
        const uint8_t* val = v.validity;
        for (uint32_t i = 0u; i < n; ++i) {
            const uint64_t is_valid = (val[i >> 3] >> (i & 7u)) & 1u;
            codes[i] = is_valid ? v.selection[i] : k;
        }
    }

    r.data           = data;
    r.selection      = codes;
    r.owns_selection = true;
    r.data_length    = out_k;
    r.length         = n;
    return r;
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

static inline VecResult draken_slice(
    const DrakenVector& v, uint32_t start, uint32_t length)
{
    const unsigned idx = static_cast<unsigned>(v.type);
    if (idx >= OpsTable::kSize || g_ops_table().entries[idx].slice == nullptr)
        throw std::invalid_argument("draken_slice: unsupported type");
    return g_ops_table().entries[idx].slice(v, start, length);
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

// D.x — string between dispatcher.
// lo_slot/lo_bytes and hi_slot/hi_bytes: pre-built bound slots (arena_offset==0
// for long strings) + literal bytes. Built at the Python edge the same way as
// draken_str_compare_scalar's scalar_slot/scalar_bytes.
static inline VecResult draken_str_between(
    const DrakenVector&     v,
    const DrakenStringSlot& lo_slot, const uint8_t* lo_bytes,
    const DrakenStringSlot& hi_slot, const uint8_t* hi_bytes,
    bool lo_incl, bool hi_incl)
{
    const unsigned idx = static_cast<unsigned>(v.type);
    if (idx >= OpsTable::kSize || g_ops_table().entries[idx].str_between == nullptr)
        throw std::invalid_argument("draken_str_between: unsupported type");
    return g_ops_table().entries[idx].str_between(
        v, lo_slot, lo_bytes, hi_slot, hi_bytes, lo_incl, hi_incl);
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
