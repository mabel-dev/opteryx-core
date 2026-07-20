# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
"""Matched A/B microbench: dense take vs dict-fused (dedup) take.

Arm A  (dense)  : gather data[selection[idx[i]]] into a fresh dense int64 vector
                  — one payload copy per OUTPUT row (current take semantics).
Arm B  (dict)   : walk idx, resolve source code c = selection[idx[i]]; copy the
                  payload the FIRST time a code is seen, emit codes thereafter
                  — one payload copy per DISTINCT REFERENCED value (proposed take).

Both arms produce a REAL owned draken Vector (no borrowing) so footprint is
measured identically via draken_vector_nbytes. Sources are built in-module so
we control NDV exactly: dense (100%), 50%, 10%, constant (1).
"""

from libc.stdint cimport int64_t, uint8_t, uint32_t
from libc.string cimport memset

from draken.core.buffers cimport (
    DrakenVector,
    DrakenType,
    DRAKEN_INT64,
    draken_vector_nbytes,
)
from draken.vectors.vector cimport (
    Vector,
    from_decoded,
    dict_int64_from_decoded,
)

cdef extern from "core/alloc.h" nogil:
    void* draken_malloc(size_t n) nogil
    void  draken_free(void* p) nogil


# ── source builders (own their data) ────────────────────────────────────────

cpdef Vector make_dense_source(uint32_t n):
    """A true dense int64 source: identity selection, data_length == n."""
    cdef int64_t* data = <int64_t*>draken_malloc(<size_t>n * sizeof(int64_t))
    cdef uint32_t i
    for i in range(n):
        data[i] = <int64_t>i
    return from_decoded(<void*>data, NULL, n, DRAKEN_INT64)


cpdef Vector make_dict_source(uint32_t n, uint32_t ndv):
    """A dict-encoded int64 source with exactly `ndv` distinct values.

    codes[i] = i % ndv  → every code is referenced; data_length == ndv."""
    cdef int64_t* vals = <int64_t*>draken_malloc(<size_t>ndv * sizeof(int64_t))
    cdef uint32_t* codes = <uint32_t*>draken_malloc(<size_t>n * sizeof(uint32_t))
    cdef uint32_t i
    for i in range(ndv):
        vals[i] = <int64_t>i
    for i in range(n):
        codes[i] = i % ndv
    return dict_int64_from_decoded(<void*>vals, ndv, codes, n, NULL)


# ── the two take arms (both build owned results) ─────────────────────────────

cpdef Vector take_dense(Vector src, uint32_t[::1] idx):
    """Arm A — dense owned output; one payload copy per output row."""
    cdef DrakenVector* dv = src.unified()
    cdef const int64_t* data = <const int64_t*>dv.data
    cdef const uint32_t* sel = dv.selection
    cdef uint32_t n = <uint32_t>idx.shape[0]
    cdef int64_t* out = <int64_t*>draken_malloc(<size_t>n * sizeof(int64_t))
    cdef uint32_t i
    with nogil:
        for i in range(n):
            out[i] = data[sel[idx[i]]]
    return from_decoded(<void*>out, NULL, n, DRAKEN_INT64)


cpdef Vector take_dict(Vector src, uint32_t[::1] idx):
    """Arm B — dict owned output; one payload copy per distinct referenced value.

    Dedup is a direct-mapped remap array (source codes are dense 0..data_length),
    so no hashing. `remap` is transient scratch (data_length * 4 bytes)."""
    cdef DrakenVector* dv = src.unified()
    cdef const int64_t* data = <const int64_t*>dv.data
    cdef const uint32_t* sel = dv.selection
    cdef uint32_t dl = dv.data_length
    cdef uint32_t n = <uint32_t>idx.shape[0]

    cdef uint32_t* remap = <uint32_t*>draken_malloc(<size_t>dl * sizeof(uint32_t))
    cdef uint32_t* codes = <uint32_t*>draken_malloc(<size_t>n * sizeof(uint32_t))
    cdef int64_t* vals = <int64_t*>draken_malloc(<size_t>dl * sizeof(int64_t))
    cdef uint32_t i, c, nxt = 0
    cdef uint32_t UNSEEN = <uint32_t>0xFFFFFFFF
    with nogil:
        memset(remap, 0xFF, <size_t>dl * sizeof(uint32_t))  # 0xFFFFFFFF = unseen
        for i in range(n):
            c = sel[idx[i]]
            if remap[c] == UNSEEN:
                remap[c] = nxt
                vals[nxt] = data[c]
                nxt += 1
            codes[i] = remap[c]
    draken_free(remap)
    # `nxt` distinct values actually used; the vals buffer is oversized by
    # (dl - nxt) slots. A production impl would realloc/shrink; for footprint
    # honesty we report the LOGICAL owned size (nxt) via a shrunk dict result.
    cdef int64_t* vals_fit = <int64_t*>draken_malloc(<size_t>nxt * sizeof(int64_t))
    cdef uint32_t j
    for j in range(nxt):
        vals_fit[j] = vals[j]
    draken_free(vals)
    return dict_int64_from_decoded(<void*>vals_fit, nxt, codes, n, NULL)


# ── memory A/B (deterministic — safe to run under device load) ───────────────

def bench_memory(uint32_t n):
    """Build the four sources, run both arms (identity gather), report footprint.

    Returns a list of (label, ndv, A_bytes, B_bytes, B_scratch_bytes)."""
    from array import array
    cdef uint32_t[::1] idx = array("I", range(n))  # identity gather, output length n

    specs = [
        ("dense/100%", n),
        ("50% NDV",    n // 2),
        ("10% NDV",    n // 10),
        ("constant",   1),
    ]
    rows = []
    cdef Vector src, ra, rb
    cdef uint32_t ndv
    for label, ndv in specs:
        if label == "dense/100%":
            src = make_dense_source(n)
        else:
            src = make_dict_source(n, ndv)
        ra = take_dense(src, idx)
        rb = take_dict(src, idx)
        a_bytes = draken_vector_nbytes(ra._dv)
        b_bytes = draken_vector_nbytes(rb._dv)
        b_scratch = ndv * 4  # transient remap during the op
        rows.append((label, int(ndv), int(a_bytes), int(b_bytes), int(b_scratch)))
    return rows


# ── timing A/B (GATED — only call when the device is quiet) ───────────────────

def bench_time(uint32_t n, int iters):
    """Time both arms per source shape. Do NOT call under device load."""
    import time
    from array import array
    cdef uint32_t[::1] idx = array("I", range(n))
    specs = [("dense/100%", n), ("50% NDV", n // 2), ("10% NDV", n // 10), ("constant", 1)]
    rows = []
    cdef Vector src, r
    cdef int k
    cdef uint32_t ndv
    for label, ndv in specs:
        if label == "dense/100%":
            src = make_dense_source(n)
        else:
            src = make_dict_source(n, ndv)
        t0 = time.perf_counter()
        for k in range(iters):
            r = take_dense(src, idx)
        ta = (time.perf_counter() - t0) / iters
        t0 = time.perf_counter()
        for k in range(iters):
            r = take_dict(src, idx)
        tb = (time.perf_counter() - t0) / iters
        rows.append((label, int(ndv), ta, tb))
    return rows
