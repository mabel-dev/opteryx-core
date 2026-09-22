#pragma once
// draken/ops/slice_shape.h — the ONE rule every fixed-width slice uses to decide
// whether a contiguous slice KEEPS its source's dictionary encoding or flattens it.
//
// Every fixed-width slicer (i64_slice, fixed_int_slice<T>, float_slice<T>,
// i128_slice, interval_slice) had the same body: memcpy when the source is dense
// and identity, otherwise walk the selection and write one value per output row —
// i.e. ALWAYS flatten. That is right for a dense source and wrong for a
// dict-encoded one, where it expands a small value block into a full row's worth
// of bytes per row and throws the encoding away at the same time. `str_slice`
// already had a dict-preserving path (its `k <= n` branch), so a dict-shaped slice
// result is an established, supported outcome — this brings the fixed-width
// families in line rather than inventing a new shape.
//
// THE ARITHMETIC (not a tuned constant):
//   flatten        costs  n*w                    bytes
//   keep the dict  costs  k*w + n*4              bytes   (whole value block + codes)
//   so keeping pays exactly when   k*w + 4n < n*w   <=>   k < n*(w-4)/w
//
// That test EXCLUDES THE NARROW TYPES BY ITSELF: at w == 4 the right-hand side is
// zero and at w < 4 it is negative, so INT8/16/32, UINT8/16/32 and FLOAT32 can
// never take this path — a 4-byte code is not smaller than the value it replaces.
// Only the 8-byte (INT64/UINT64/FLOAT64/TIMESTAMP64/DECIMAL) and 16-byte
// (DECIMAL128/INTERVAL) families can qualify, and only when the dictionary is
// genuinely smaller than the slice. Nothing here is fitted to a workload.
//
// MEASURED, AND THE BYTES ARE NOT THE REASON. A/B on the same input (n = 65536):
//
//   INT64   k=16     18.6us -> 15.3us  1.22x      FLOAT64 k=16     18.5 -> 15.4  1.20x
//   INT64   k=32000  30.4us -> 18.4us  1.65x      FLOAT64 k=32000  30.5 -> 17.8  1.71x
//
// The win GROWS as the dictionary grows, which the byte model predicts backwards:
// at k=16 keeping copies half the bytes (2x) and gains 1.22x, while at k=32000 it
// copies ~99% of them (1.01x on bytes) and gains 1.65x. What actually differs is
// the ACCESS PATTERN — flattening is a random gather `dst[i] = data[selection[i]]`
// whose working set is the whole value block, and the bigger that block the more
// it misses; keeping the dictionary is two sequential copies.
//
// The byte test is therefore CONSERVATIVE, not tight: it refuses cases between
// n*(w-4)/w and n where keeping would still be faster. That is deliberate — past
// that point the kept block is larger than the flattened output, so refusing
// trades a little speed for a bound on memory. Widening it is a memory decision,
// not a performance one, and needs the architect.
//
// What it does NOT do: share the source's value block. `VecResult.data` is
// contractually OWNED (ops/vec_result.h), so a zero-copy window over the source
// cannot be expressed through this interface. Copying the value block once per
// slice is still strictly better than copying a value per row whenever the test
// above passes.
//
// DO NOT "FIX" THAT WITH A BORROW FLAG ON VecResult. That is the obvious next
// thought (VectorOwner::data_source models exactly it one level up) and it was
// investigated and MEASURED on 2026-09-22. It does not pay:
//
//   - Time. Timing the exact memcpys in-process, with the copies still happening
//     so the query does identical work: JOB 1a 1.97ms of 187ms, 8a 2.35/324,
//     13a 3.20/471, 26a 6.05/427, 6a 0.31/259, 20a 0.51/431. That is 0.1-1.4% of
//     wall, and it OVERSTATES the prize — those are thread-nanoseconds summed
//     across workers measured against single-threaded wall. In a profile the
//     whole of str_slice+str_take is ~0.5% of non-idle CPU and every draken
//     fixed-width op together is 2.6-4.1%; parquet decode is 62-67%.
//
//   - The bytes look big and the time is not. 13a copies 101.6 MB in 34 calls —
//     ~3ms at memcpy bandwidth. This is the same trap as the paragraph above,
//     running the other way, so do not argue this one from bytes saved either.
//
//   - A kernel cannot even NAME the keepalive. Kernels receive
//     `const DrakenVector&`, which carries no ownership handle, and
//     vector_slice_impl / vector_take_impl take `const VectorOwner&`. Only the
//     CxxMorsel path has a shared_ptr to give (CxxColumn::own); the nanobind
//     `Vector` holder has none at all. Scope would be CxxMorsel-only, i.e. one
//     kernel with two ownership regimes chosen by its caller.
//
//   - The migration hazard is WORSE than the `arena` precedent that vec_result.h
//     documents. A consumer that ignores `arena` leaks; a consumer that ignores a
//     borrow flag calls draken_free on borrowed memory.
//
//   - It pins. A borrowed window keeps the whole source block alive for the
//     lifetime of the smallest derived slice (LIMIT 10 over a 262k-row morsel
//     holds the morsel), on an 8GiB worker where OOM surfaces as an opaque 503.
//
// Most of the copies are not borrowable anyway: fixed_dict_compact_take can only
// borrow in its `d == k` branch, and the string COMPACT path (k > n) builds a new
// arena layout by construction — on JOB 6a/20a that is 100% of the rows.
// If this is ever revisited, revisit it as a MEMORY proposal (~85-100 MB of
// transient allocation per JOB query) with an RSS measurement, not a speed one.

#include <cstdint>
#include <cstring>
#include <new>
#include <stdexcept>

#include "core/alloc.h"
#include "core/buffers.h"
#include "ops/vec_result.h"

namespace draken {
namespace ops {

// Fills `out` and returns true when keeping the dictionary copies strictly fewer
// bytes than flattening; returns false when the caller should take its own
// flattening path.
//
// `out_null` is the caller's already-built per-row validity for the sliced range.
// It is per LOGICAL row and therefore identical for both output shapes, which is
// why callers build it BEFORE choosing. Ownership moves into `out` on success; on
// an allocation failure here it is freed before throwing, so the caller never has
// to unwind it.
template <typename T>
static inline bool slice_keep_dict(const DrakenVector& v, uint32_t start, uint32_t n,
                                   uint8_t* out_null, DrakenType tag, VecResult& out) {
    constexpr size_t w = sizeof(T);
    if constexpr (w <= sizeof(uint32_t)) {
        // Compile-time exit: the break-even above is unreachable for these widths.
        (void)v; (void)start; (void)n; (void)out_null; (void)tag; (void)out;
        return false;
    } else {
        // A dense+identity source has a straight memcpy available, which beats both
        // shapes — leave it to the caller's fast path.
        if (draken_is_dense(&v) && (v.flags & DRAKEN_SEL_IDENTITY)) return false;
        const uint32_t k = v.data_length;
        if (k == 0u || n == 0u) return false;
        const size_t keep = static_cast<size_t>(k) * w
                          + static_cast<size_t>(n) * sizeof(uint32_t);
        const size_t flat = static_cast<size_t>(n) * w;
        if (keep >= flat) return false;

        T* dst = static_cast<T*>(draken_malloc(static_cast<size_t>(k) * w));
        if (!dst) { if (out_null) draken_free(out_null); throw std::bad_alloc(); }
        std::memcpy(dst, v.data, static_cast<size_t>(k) * w);

        uint32_t* codes = static_cast<uint32_t*>(
            draken_malloc(static_cast<size_t>(n) * sizeof(uint32_t)));
        if (!codes) {
            draken_free(dst);
            if (out_null) draken_free(out_null);
            throw std::bad_alloc();
        }
        for (uint32_t i = 0; i < n; ++i) codes[i] = v.selection[start + i];

        out.data           = dst;
        out.validity       = out_null;
        out.selection      = codes;
        out.owns_selection = true;
        out.data_length    = k;
        out.length         = n;
        out.type           = tag;
        // Owned codes into a smaller value block: neither identity nor a permutation.
        out.flags          = 0u;
        return true;
    }
}

}  // namespace ops
}  // namespace draken
