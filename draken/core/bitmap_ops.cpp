// Bitmap operations for the bytecode VM evaluator.
#include "bitmap_ops.h"
#include "alloc.h"
#include "buffers.h"
#include "draken_bridge.h"
#include <cstring>
#include <cstdint>

namespace {

// Word-wide binary bitmap op with the null-shape dispatch hoisted out of the
// loop. Data bits are combined 8 bytes at a time (memcpy'd words — alignment
// safe, and the loops auto-vectorize under -O3 with the target's SIMD); the
// value-blind null merge (OR of the operand null masks — see the Kleene note
// below for why the VM does NOT use these) runs the same way, accumulating
// has_null as a running OR tested once instead of a branch per byte.
template <typename OpFn>
inline int bitmap_binop(
    uint8_t* out, uint8_t* out_null,
    const uint8_t* left, const uint8_t* left_null,
    const uint8_t* right, const uint8_t* right_null,
    size_t nbytes, uint32_t num_rows, OpFn op)
{
    const size_t nw = nbytes >> 3;

    for (size_t i = 0; i < nw; i++) {
        uint64_t l, r, w;
        std::memcpy(&l, left + i * 8, 8);
        std::memcpy(&r, right + i * 8, 8);
        w = op(l, r);
        std::memcpy(out + i * 8, &w, 8);
    }
    for (size_t i = nw * 8; i < nbytes; i++)
        out[i] = static_cast<uint8_t>(op((uint64_t)left[i], (uint64_t)right[i]));

    uint64_t null_acc = 0;
    if (left_null == nullptr && right_null == nullptr) {
        std::memset(out_null, 0, nbytes);
    } else if (left_null != nullptr && right_null != nullptr) {
        for (size_t i = 0; i < nw; i++) {
            uint64_t l, r, w;
            std::memcpy(&l, left_null + i * 8, 8);
            std::memcpy(&r, right_null + i * 8, 8);
            w = l | r;
            null_acc |= w;
            std::memcpy(out_null + i * 8, &w, 8);
        }
        for (size_t i = nw * 8; i < nbytes; i++) {
            out_null[i] = static_cast<uint8_t>(left_null[i] | right_null[i]);
            null_acc |= out_null[i];
        }
    } else {
        const uint8_t* only = (left_null != nullptr) ? left_null : right_null;
        for (size_t i = 0; i < nw; i++) {
            uint64_t w;
            std::memcpy(&w, only + i * 8, 8);
            null_acc |= w;
            std::memcpy(out_null + i * 8, &w, 8);
        }
        for (size_t i = nw * 8; i < nbytes; i++) {
            out_null[i] = only[i];
            null_acc |= only[i];
        }
    }

    if ((num_rows & 7) != 0 && nbytes > 0) {
        uint8_t mask = (1 << (num_rows & 7)) - 1;
        out[nbytes - 1] &= mask;
        out_null[nbytes - 1] &= mask;
    }

    return null_acc != 0;
}

}  // namespace

#ifdef __cplusplus
extern "C" {
#endif

/* bool_vector_from_bits — wrap a caller-owned bitmap into a Draken-owned
 * DRAKEN_BOOL Vector by COPYING into draken_malloc'd memory.
 *
 * The caller retains ownership of the input pointers (typically Cython
 * typed-memoryview / libc-malloc'd buffers) and may free them after this
 * call. The returned Vector owns its own (draken-allocated) bitmap copies.
 *
 * Why copy: callers (operator join inner loops, bytecode VM postpass) use
 * libc/Cython allocators for their working buffers; draken_vector_own_raw
 * requires draken_malloc-allocated memory because the Vector's destructor
 * will draken_free it. Mixing allocators is UB. The copy is the legitimate
 * bridge between the two ownership models.
 */
PyObject* bool_vector_from_bits(uint8_t* bitmap, uint8_t* null_bitmap, uint32_t num_rows) {
    const uint32_t nbytes = (num_rows + 7u) >> 3;
    // SIMD-padded allocation: round up to 8-byte alignment, minimum 8 bytes
    // so even zero-row vectors get a valid buffer.
    const uint32_t padded = (nbytes + 7u) & ~7u;
    const size_t alloc = padded > 0u ? padded : 8u;

    uint8_t* draken_bitmap = static_cast<uint8_t*>(draken_malloc(alloc));
    if (!draken_bitmap) {
        PyErr_NoMemory();
        return NULL;
    }
    std::memset(draken_bitmap, 0, alloc);
    if (nbytes > 0u) std::memcpy(draken_bitmap, bitmap, nbytes);

    uint8_t* draken_validity = NULL;
    if (null_bitmap != NULL) {
        draken_validity = static_cast<uint8_t*>(draken_malloc(alloc));
        if (!draken_validity) {
            draken_free(draken_bitmap);
            PyErr_NoMemory();
            return NULL;
        }
        std::memset(draken_validity, 0, alloc);
        if (nbytes > 0u) std::memcpy(draken_validity, null_bitmap, nbytes);
    }

    PyObject* result = draken_vector_own_raw(
        draken_bitmap, draken_validity, num_rows, DRAKEN_BOOL);
    if (!result) {
        draken_free(draken_bitmap);
        if (draken_validity) draken_free(draken_validity);
        return NULL;  // exception already set by draken_vector_own_raw
    }
    return result;
}

/* Count set bits in a bitmap using std::popcount (C++20).
 *
 * Accumulates over nbytes bytes (each byte contains 8 bits).
 * For performance on large bitmaps, this could use SIMD intrinsics (POPCNT,
 * AVX2 _mm256_sad_epu8, etc.), but the simple byte-loop is correct and sufficient.
 */
size_t simd_popcount(const uint8_t* data, size_t nbytes) {
    // Word-wide: __builtin_popcountll lowers to POPCNT / CNT+ADDV — 8 bytes
    // per iteration instead of one.
    const size_t nw = nbytes >> 3;
    size_t count = 0;
    for (size_t i = 0; i < nw; i++) {
        uint64_t w;
        std::memcpy(&w, data + i * 8, 8);
        count += (size_t)__builtin_popcountll(w);
    }
    for (size_t i = nw * 8; i < nbytes; i++) {
        count += (size_t)__builtin_popcount(data[i]);
    }
    return count;
}

/* In-place AND: dst &= src, word-wide. Shared by the filter-span validity
 * merge (evaluation.pyx) — one implementation, not a per-call-site byte loop. */
void c_bitmap_and_inplace(uint8_t* dst, const uint8_t* src, size_t nbytes) {
    const size_t nw = nbytes >> 3;
    for (size_t i = 0; i < nw; i++) {
        uint64_t d, s;
        std::memcpy(&d, dst + i * 8, 8);
        std::memcpy(&s, src + i * 8, 8);
        d &= s;
        std::memcpy(dst + i * 8, &d, 8);
    }
    for (size_t i = nw * 8; i < nbytes; i++) dst[i] &= src[i];
}

/* AND two bitmaps: out = left & right, with NULL merging. */
int c_and_bitmap(
    uint8_t* out, uint8_t* out_null,
    const uint8_t* left, const uint8_t* left_null,
    const uint8_t* right, const uint8_t* right_null,
    size_t nbytes, uint32_t num_rows
) {
    return bitmap_binop(out, out_null, left, left_null, right, right_null,
                        nbytes, num_rows, [](uint64_t l, uint64_t r) { return l & r; });
}

/* OR two bitmaps: out = left | right, with NULL merging. */
int c_or_bitmap(
    uint8_t* out, uint8_t* out_null,
    const uint8_t* left, const uint8_t* left_null,
    const uint8_t* right, const uint8_t* right_null,
    size_t nbytes, uint32_t num_rows
) {
    return bitmap_binop(out, out_null, left, left_null, right, right_null,
                        nbytes, num_rows, [](uint64_t l, uint64_t r) { return l | r; });
}

/* XOR two bitmaps: out = left ^ right, with NULL merging. */
int c_xor_bitmap(
    uint8_t* out, uint8_t* out_null,
    const uint8_t* left, const uint8_t* left_null,
    const uint8_t* right, const uint8_t* right_null,
    size_t nbytes, uint32_t num_rows
) {
    return bitmap_binop(out, out_null, left, left_null, right, right_null,
                        nbytes, num_rows, [](uint64_t l, uint64_t r) { return l ^ r; });
}

/* NOT a bitmap: out = ~src (within num_rows bits), with NULL propagation. */
int c_not_bitmap(
    uint8_t* out, uint8_t* out_null,
    const uint8_t* src, const uint8_t* src_null,
    size_t nbytes, uint32_t num_rows
) {
    const size_t nw = nbytes >> 3;
    for (size_t i = 0; i < nw; i++) {
        uint64_t w;
        std::memcpy(&w, src + i * 8, 8);
        w = ~w;
        std::memcpy(out + i * 8, &w, 8);
    }
    for (size_t i = nw * 8; i < nbytes; i++)
        out[i] = static_cast<uint8_t>(~src[i]);

    uint64_t null_acc = 0;
    if (src_null == nullptr) {
        std::memset(out_null, 0, nbytes);
    } else {
        for (size_t i = 0; i < nw; i++) {
            uint64_t w;
            std::memcpy(&w, src_null + i * 8, 8);
            null_acc |= w;
            std::memcpy(out_null + i * 8, &w, 8);
        }
        for (size_t i = nw * 8; i < nbytes; i++) {
            out_null[i] = src_null[i];
            null_acc |= src_null[i];
        }
    }

    if ((num_rows & 7) != 0 && nbytes > 0) {
        uint8_t mask = (1 << (num_rows & 7)) - 1;
        out[nbytes - 1] &= mask;
        out_null[nbytes - 1] &= mask;
    }

    return null_acc != 0;
}

/* Stub: extract bitmap pointers from a DrakenVector.
 *
 * Currently unused. Placeholder for future VM work.
 */
void c_get_bitmap_ptrs(void* draken_vector) {
    // No-op stub.
}

#ifdef __cplusplus
}
#endif

// ===========================================================================
// Kleene boolean ops for the bytecode VM.
//
// The bytecode VM's AND / OR / XOR / NOT route through these, NOT through the
// value-blind c_*_bitmap family above. c_*_bitmap merges nulls as a plain OR of
// the operand null masks, which cannot express three-valued logic — F∧N=F but
// T∧N=N depends on the DATA, not just the masks. The canonical, tested Kleene
// implementation lives in draken::ops::bool_{and,or,xor,not}; these thin C-ABI
// shims expose it to the nogil VM and normalize a bare NULL literal (DRAKEN_NULL:
// data==NULL, no validity) into a proper all-null BOOL operand first, so bool_*
// never dereferences a null data pointer.
// ===========================================================================

#include "ops/bool_logical.h"   // draken::ops::bool_{and,or,xor,not}

namespace {

// If `v` is a real BOOL vector, return it unchanged. If it is a DRAKEN_NULL
// literal (or otherwise carries no data), synthesize an all-null BOOL vector of
// `n` logical rows: zeroed data (values irrelevant — every row is null) and a
// zeroed, full-length validity bitmap (0 = null). The two scratch buffers are
// returned via out params for the caller to free after the Kleene op reads them.
// Throws std::bad_alloc on OOM (the outer wrapper catches).
inline DrakenVector vm_normalize_bool(const DrakenVector* v, uint32_t n,
                                      uint8_t*& scratch_data, uint8_t*& scratch_vld) {
    if (v->type != DRAKEN_NULL && v->data != nullptr)
        return *v;
    const uint32_t bm    = (n + 7u) >> 3;
    const size_t   alloc = bm > 0u ? bm : 1u;
    scratch_data = static_cast<uint8_t*>(draken_malloc(alloc));
    scratch_vld  = static_cast<uint8_t*>(draken_malloc(alloc));
    if (scratch_data == nullptr || scratch_vld == nullptr) throw std::bad_alloc();
    std::memset(scratch_data, 0, alloc);   // all bits 0 — value undefined under null
    std::memset(scratch_vld,  0, alloc);   // all rows null
    DrakenVector s{};
    s.data        = scratch_data;
    s.selection   = draken_identity_sel(n);
    s.data_length = n;
    s.length      = n;
    s.validity    = scratch_vld;
    s.type        = DRAKEN_BOOL;
    s.flags       = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return s;
}

inline VecResult vm_bool_error() {
    VecResult r{};
    r.data      = nullptr;
    r.error_msg = "bool op: allocation failed";
    return r;
}

}  // namespace

extern "C" {

// op: 0 = AND, 1 = OR, 2 = XOR. Owned VecResult (draken_malloc buffers) for the
// VM to adopt into its frame arena. data == nullptr signals an error.
VecResult draken_vm_bool_binop(int op, const DrakenVector* a, const DrakenVector* b,
                               uint32_t num_rows) {
    uint8_t *sda = nullptr, *sva = nullptr, *sdb = nullptr, *svb = nullptr;
    VecResult r;
    try {
        const DrakenVector na = vm_normalize_bool(a, num_rows, sda, sva);
        const DrakenVector nb = vm_normalize_bool(b, num_rows, sdb, svb);
        if (op == 0)      r = draken::ops::bool_and(na, nb);
        else if (op == 1) r = draken::ops::bool_or(na, nb);
        else              r = draken::ops::bool_xor(na, nb);
    } catch (...) {
        r = vm_bool_error();
    }
    if (sda) draken_free(sda);
    if (sva) draken_free(sva);
    if (sdb) draken_free(sdb);
    if (svb) draken_free(svb);
    return r;
}

VecResult draken_vm_bool_not(const DrakenVector* a, uint32_t num_rows) {
    uint8_t *sda = nullptr, *sva = nullptr;
    VecResult r;
    try {
        const DrakenVector na = vm_normalize_bool(a, num_rows, sda, sva);
        r = draken::ops::bool_not(na);
    } catch (...) {
        r = vm_bool_error();
    }
    if (sda) draken_free(sda);
    if (sva) draken_free(sva);
    return r;
}

VecResult draken_vm_bool_truth_test(int op, const DrakenVector* a, uint32_t num_rows) {
    uint8_t *sda = nullptr, *sva = nullptr;
    VecResult r;
    try {
        const DrakenVector na = vm_normalize_bool(a, num_rows, sda, sva);
        r = draken::ops::bool_truth_test(na, op);
    } catch (...) {
        r = vm_bool_error();
    }
    if (sda) draken_free(sda);
    if (sva) draken_free(sva);
    return r;
}

}  // extern "C"
