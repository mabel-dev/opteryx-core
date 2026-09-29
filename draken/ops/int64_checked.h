#pragma once
// draken/ops/int64_checked.h — overflow-checked INT64 arithmetic primitives.
//
// Shared by both INT64 kernel families (ops/int64_arithmetic.h — the registry
// kernels — and ops/fixed_int_ops.h — the draken_binop cross-width path) so the
// overflow rule cannot diverge between them.
//
// RULE: INT64 add / sub / mul / neg / INT_DIVIDE overflow FAILS LOUD. There is
// no wrapped answer. Same contract as SUM ("never a wrapped answer") and every
// DECIMAL kernel. The failure is PER BATCH: a std::overflow_error carrying no
// row detail, which DRAKEN_KERNEL_TRY turns into the error sentinel with the
// message intact (identical to the decimal_arith.h channel).
//
// UNSIGNED (ruling 2026-09-29, reversing E33's "wrapping unsigned semantics"):
// unsigned add / sub / mul overflow — including a subtraction that would go
// below zero — FAILS LOUD the same way. `0u - 1u` is an error, never 2^64-1.
//
// DIVISION / MODULO BY ZERO (ruling 2026-09-29, reversing the "x DIV 0 = 0" convention):
// integer `DIV` and `%` with a zero divisor on a LIVE row raise std::domain_error
// ("... division by zero ...") — never a silent 0. It was 0 only because CASE/IIF
// evaluated every branch, so a guarded `CASE WHEN d = 0 THEN 0 ELSE n DIV d END` would
// have crashed; lazy branch evaluation (BC_LAZY) removed that reason. DECIMAL true
// division keeps its NULL row (its own ruling) and float division stays IEEE.
//
// Row liveness. The hot loops OR-accumulate an overflow flag and check it once
// after the loop, so the loop body carries no branch. A flagged overflow is an
// ERROR only if it happened on a LIVE row:
//   * a row NULL in either operand — its data slot is arbitrary, may overflow;
//   * a dict/constant physical value no logical row references (dead entry).
// So a flagged batch is re-scanned by LOGICAL row (validity honoured, selection
// followed) and only a live overflow raises. That rescan runs only when an
// overflow already occurred, never on the success path.

#include <stdint.h>
#include <stddef.h>
#include <stdexcept>
#include <string>
#include <type_traits>
#include "core/buffers.h"
#include "core/alloc.h"

namespace draken { namespace ops {

static inline bool i64c_row_valid(const uint8_t* validity, uint32_t i) noexcept {
    return validity == nullptr || ((validity[i >> 3] >> (i & 7)) & 1u) != 0u;
}

// Each functor returns true on overflow and always writes the wrapped result to
// `r` (harmless: the batch raises if any live row overflowed). Names are the
// SQL operation, used in the error message.
struct I64OvfAdd {
    static constexpr bool kDivMod = false;
    static constexpr const char* kName = "addition";
    static inline bool apply(int64_t x, int64_t y, int64_t& r) noexcept {
        return __builtin_add_overflow(x, y, &r);
    }
};
struct I64OvfSub {
    static constexpr bool kDivMod = false;
    static constexpr const char* kName = "subtraction";
    static inline bool apply(int64_t x, int64_t y, int64_t& r) noexcept {
        return __builtin_sub_overflow(x, y, &r);
    }
};
struct I64OvfMul {
    static constexpr bool kDivMod = false;
    static constexpr const char* kName = "multiplication";
    static inline bool apply(int64_t x, int64_t y, int64_t& r) noexcept {
        return __builtin_mul_overflow(x, y, &r);
    }
};
// INT_DIVIDE. y == 0 is a divide-by-zero error. y == -1 is answered by
// checked negation, never idiv: INT64_MIN / -1 is UB in C and raises SIGFPE
// (#DE) on x86; the mathematical result 2^63 does not fit, so it overflows.
struct I64OvfDiv {
    static constexpr bool kDivMod = true;
    static constexpr const char* kName = "division";
    static inline bool apply(int64_t x, int64_t y, int64_t& r) noexcept {
        if (y == 0)  { r = 0; return true; }     // divide by zero: an ERROR on a live row
        if (y == -1) { return __builtin_sub_overflow(static_cast<int64_t>(0), x, &r); }
        r = x / y;
        return false;
    }
};
// MODULO. x % -1 == 0 for every x (including INT64_MIN): never overflows, and
// idiv is never issued for it. y == 0 is a divide-by-zero error.
struct I64OvfMod {
    static constexpr bool kDivMod = true;
    static constexpr const char* kName = "modulo";
    static inline bool apply(int64_t x, int64_t y, int64_t& r) noexcept {
        if (y == 0)  { r = 0; return true; }
        if (y == -1) { r = 0; return false; }
        r = x % y;
        return false;
    }
};
// Unary negation. `y` is ignored.
struct I64OvfNeg {
    static constexpr bool kDivMod = false;
    static constexpr const char* kName = "negation";
    static inline bool apply(int64_t x, int64_t, int64_t& r) noexcept {
        return __builtin_sub_overflow(static_cast<int64_t>(0), x, &r);
    }
};

// `dividend` / `divisor` are the failing row's operands (for a unary or scalar op the
// caller passes what it has; only their VALUES select the wording).
//
// `-x` reaches the kernels as `0 - x` (the planner lowers unary minus that way), so an
// overflowing subtraction whose left operand is 0 IS a negation — and the only value
// it can happen for is INT64_MIN. Say so, instead of naming a subtraction the reader
// never wrote. A message-only distinction: the arithmetic is unchanged.
template <typename Op>
[[noreturn]] static inline void i64c_throw(int64_t* owned, int64_t dividend, int64_t divisor) {
    draken_free(owned);
    if (std::is_same<Op, I64OvfSub>::value && dividend == 0)
        throw std::overflow_error(
            "INT64 negation overflow: -(" + std::to_string(divisor) +
            ") does not fit INT64 — fail loud, never a wrapped answer");
    if (Op::kDivMod && divisor == 0)
        throw std::domain_error(
            std::string("INT64 ") + Op::kName +
            " by zero: the divisor is 0 — fail loud, never a silent 0");
    throw std::overflow_error(
        std::string("INT64 ") + Op::kName +
        " overflow: exact integer result exceeds INT64 — fail loud, never a wrapped answer");
}

// Uniform logical-row loop: dst[i] = Op(xat(i), yat(i)) for i in [0, n).
// `xat`/`yat` return the int64 operand of logical row i. `av`/`bv` are the
// operand validity bitmaps (nullptr = all valid). Frees `dst` and throws
// std::overflow_error if any LIVE row overflows.
template <typename Op, typename XAt, typename YAt>
static inline void i64_checked_rows(uint32_t n, int64_t* dst, XAt xat, YAt yat,
                                    const uint8_t* av, const uint8_t* bv) {
    bool any = false;
    for (uint32_t i = 0; i < n; ++i)
        any |= Op::apply(xat(i), yat(i), dst[i]);
    if (!any) return;
    for (uint32_t i = 0; i < n; ++i) {
        int64_t t;
        if (Op::apply(xat(i), yat(i), t) && i64c_row_valid(av, i) && i64c_row_valid(bv, i))
            i64c_throw<Op>(dst, xat(i), yat(i));
    }
}

// Physical-value loop for shape-preserving results (scalar variants, unary
// neg): dst[j] = Op(ad[j], scalar) over src.data_length values. An overflow in
// a physical value is an error only if some LIVE logical row of `src` reads
// that value (validity honoured, selection followed). Frees `dst` and throws
// otherwise-live overflow.
template <typename Op>
static inline void i64_checked_physical(const DrakenVector& src, const int64_t* ad,
                                        int64_t scalar, int64_t* dst) {
    const uint32_t k = src.data_length;
    bool any = false;
    for (uint32_t j = 0; j < k; ++j)
        any |= Op::apply(ad[j], scalar, dst[j]);
    if (!any) return;
    for (uint32_t i = 0; i < src.length; ++i) {
        int64_t t;
        if (i64c_row_valid(src.validity, i) &&
            Op::apply(ad[src.selection[i]], scalar, t))
            i64c_throw<Op>(dst, ad[src.selection[i]], scalar);
    }
}

// ---------------------------------------------------------------------------
// UNSIGNED. Each functor works in uint64_t; a narrower result width W is enforced
// by the caller's range check (r > max(W)), so a UINT8 x UINT8 -> UINT16 result can
// never silently narrow. Subtraction "overflows" when it would go below zero.
// ---------------------------------------------------------------------------
struct U64OvfAdd {
    static constexpr bool kDivMod = false;
    static constexpr const char* kName = "addition";
    static inline bool apply(uint64_t x, uint64_t y, uint64_t& r) noexcept {
        return __builtin_add_overflow(x, y, &r);
    }
};
struct U64OvfSub {
    static constexpr bool kDivMod = false;
    static constexpr const char* kName = "subtraction";
    static inline bool apply(uint64_t x, uint64_t y, uint64_t& r) noexcept {
        return __builtin_sub_overflow(x, y, &r);
    }
};
struct U64OvfMul {
    static constexpr bool kDivMod = false;
    static constexpr const char* kName = "multiplication";
    static inline bool apply(uint64_t x, uint64_t y, uint64_t& r) noexcept {
        return __builtin_mul_overflow(x, y, &r);
    }
};

template <typename Op>
[[noreturn]] static inline void u64c_throw(void* owned, size_t bits, uint64_t divisor) {
    draken_free(owned);
    const std::string w = "UINT" + std::to_string(bits);
    if (Op::kDivMod && divisor == 0)
        throw std::domain_error(
            w + " " + Op::kName + " by zero: the divisor is 0 — fail loud, never a silent 0");
    throw std::overflow_error(
        w + " " + Op::kName + " overflow: exact integer result is outside " + w +
        " — fail loud, never a wrapped answer");
}

// Unsigned DIV / MOD: a zero divisor is an error on a live row.
struct U64OvfDiv {
    static constexpr bool kDivMod = true;
    static constexpr const char* kName = "division";
    static inline bool apply(uint64_t x, uint64_t y, uint64_t& r) noexcept {
        if (y == 0) { r = 0; return true; }
        r = x / y;
        return false;
    }
};
struct U64OvfMod {
    static constexpr bool kDivMod = true;
    static constexpr const char* kName = "modulo";
    static inline bool apply(uint64_t x, uint64_t y, uint64_t& r) noexcept {
        if (y == 0) { r = 0; return true; }
        r = x % y;
        return false;
    }
};

// dst[i] = Op(xat(i), yat(i)) as W, over logical rows; overflow (or a result above
// W's range) on a LIVE row frees `dst` and throws. Same flag-then-rescan scheme as
// i64_checked_rows.
template <typename Op, typename W, typename XAt, typename YAt>
static inline void u_checked_rows(uint32_t n, W* dst, XAt xat, YAt yat,
                                  const uint8_t* av, const uint8_t* bv) {
    constexpr uint64_t wmax = static_cast<uint64_t>(~static_cast<W>(0));
    bool any = false;
    for (uint32_t i = 0; i < n; ++i) {
        uint64_t r;
        bool o = Op::apply(xat(i), yat(i), r);
        o |= (r > wmax);
        dst[i] = static_cast<W>(r);
        any |= o;
    }
    if (!any) return;
    for (uint32_t i = 0; i < n; ++i) {
        uint64_t r;
        bool o = Op::apply(xat(i), yat(i), r);
        o |= (r > wmax);
        if (o && i64c_row_valid(av, i) && i64c_row_valid(bv, i))
            u64c_throw<Op>(dst, sizeof(W) * 8, yat(i));
    }
}

// Shape-preserving scalar loop over an unsigned source's PHYSICAL values; an
// overflow is an error only if some LIVE logical row reads that value.
template <typename Op>
static inline void u64_checked_physical(const DrakenVector& src, const uint64_t* ad,
                                        uint64_t scalar, uint64_t* dst) {
    const uint32_t k = src.data_length;
    bool any = false;
    for (uint32_t j = 0; j < k; ++j)
        any |= Op::apply(ad[j], scalar, dst[j]);
    if (!any) return;
    for (uint32_t i = 0; i < src.length; ++i) {
        uint64_t t;
        if (i64c_row_valid(src.validity, i) && Op::apply(ad[src.selection[i]], scalar, t))
            u64c_throw<Op>(dst, 64, scalar);
    }
}

// For the loops that compute `y == 0 ? 0 : x / y` themselves (narrow widths, DECIMAL
// int div/mod): call AFTER the loop with `any_zero` (OR of y == 0 over the loop).
// Rescans by logical row and throws only if a LIVE row has a zero divisor — a NULL
// row's divisor slot is arbitrary. Frees `owned` on the way out.
template <typename YAt>
static inline void div_zero_rescan(bool any_zero, uint32_t n, YAt yat,
                                   const uint8_t* av, const uint8_t* bv, void* owned,
                                   const char* what, const std::string& type) {
    if (!any_zero) return;
    for (uint32_t i = 0; i < n; ++i) {
        if (yat(i) == 0 && i64c_row_valid(av, i) && i64c_row_valid(bv, i)) {
            draken_free(owned);
            throw std::domain_error(type + " " + what +
                " by zero: the divisor is 0 — fail loud, never a silent 0");
        }
    }
}

}} // namespace draken::ops
