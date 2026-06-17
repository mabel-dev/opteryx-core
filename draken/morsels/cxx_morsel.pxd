# Cython view of the C++-first morsel substrate (draken/morsels/cxx_morsel.h).
#
# This is the C-level read surface the relational operators use: a Cxx-backed
# Morsel caches a `const CxxMorsel*` and reads `columns[i].view` (DrakenVector*)
# with NO PyObject and NO nanobind per access — the data path is pure C++ and
# GIL-releasable. nanobind is crossed only at the boundary (scan-emit /
# cursor-materialize / once-per-morsel transforms), never per column.
#
# Hand-written to mirror the header (nanobind emits no cimportable .pxd). Must
# stay consistent with cxx_morsel.h.

from libcpp.vector cimport vector
from libcpp.string cimport string
from libc.stdint cimport uint32_t, uint8_t, int32_t

from cpython.object cimport PyObject

from draken.core.buffers cimport DrakenVector


cdef extern from "morsels/cxx_morsel.h" nogil:
    # S-B: stream state carried by every morsel (EOS-as-flag, read nogil).
    cdef enum class MorselState(uint8_t):
        DATA
        END_OF_STREAM

    # S-B: per-pipeline error context — nogil ops return status + set this; the
    # gil boundary raises once (status-code model, validated by the spike).
    cdef cppclass ErrCtx:
        int code
        const char* msg

    cdef cppclass CxxColumn:
        DrakenVector view
        # `own` (shared_ptr<VectorOwner>) is intentionally not declared: the
        # C-level read path uses only `view`; owner access happens at the
        # nanobind boundary.

    cdef cppclass CxxMorsel:
        vector[CxxColumn] columns
        vector[string] names
        MorselState state
        uint32_t num_rows() noexcept
        size_t num_columns() noexcept


cdef extern from * nogil:
    """
    extern "C" const CxxMorsel* cxx_morsel_raw_ptr(PyObject* handle);
    extern "C" CxxMorsel* cxx_take_c(const CxxMorsel*, const int32_t*, uint32_t);
    extern "C" CxxMorsel* cxx_slice_c(const CxxMorsel*, uint32_t, uint32_t);
    extern "C" CxxMorsel* cxx_mask_c(const CxxMorsel*, const DrakenVector*);
    extern "C" CxxMorsel* cxx_hash_c(const CxxMorsel*, const int32_t*, uint32_t);
    extern "C" void cxx_morsel_delete(CxxMorsel*);
    extern "C" CxxMorsel* cxx_morsel_shallow_copy(const CxxMorsel*);
    extern "C" CxxMorsel* cxx_morsel_new_eos();
    extern "C" PyObject* cxx_morsel_to_handle(const CxxMorsel*);
    """
    const CxxMorsel* cxx_morsel_raw_ptr(PyObject* handle)
    # S-B.0(a) C-ABI transform surface — nogil, no PyObject. Caller owns the
    # returned CxxMorsel (free via cxx_morsel_delete). Wired by S-B.1.
    CxxMorsel* cxx_take_c(const CxxMorsel* m, const int32_t* idx, uint32_t n) nogil
    CxxMorsel* cxx_slice_c(const CxxMorsel* m, uint32_t start, uint32_t length) nogil
    # S1: whole-morsel mask (keep rows valid AND true) — derives indices once,
    # type-takes each column nogil. mask is the predicate BoolVector's view.
    CxxMorsel* cxx_mask_c(const CxxMorsel* m, const DrakenVector* mask) nogil
    # S-B.3a keying hash — single col → shape-preserving, multi → dense mix.
    # Returns a 1-column CxxMorsel (read columns[0].view; free via cxx_morsel_delete).
    CxxMorsel* cxx_hash_c(const CxxMorsel* m, const int32_t* col_idxs, uint32_t n_cols) nogil
    void cxx_morsel_delete(CxxMorsel* m) nogil
    # S-B.1a boundary bridges. shallow_copy: owned heap CxxMorsel sharing owners.
    # to_handle: NEW-ref nanobind handle wrapping a shallow copy (boundary out).
    CxxMorsel* cxx_morsel_shallow_copy(const CxxMorsel* m) nogil
    CxxMorsel* cxx_morsel_new_eos() nogil
    PyObject* cxx_morsel_to_handle(const CxxMorsel* m)
