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

    # ARRAY child access (BC_C_NATIVE_CHILD): the elements of an ARRAY column
    # live on the VectorOwner's child_owner subtree — this is the sanctioned
    # C-level read of it (`own` itself stays undeclared above). NULL when the
    # column is out of range, unowned, or has no child.
    const DrakenVector* cxx_column_child_vec(const CxxMorsel* m, uint32_t idx) noexcept


cdef extern from * nogil:
    """
    extern "C" const CxxMorsel* cxx_morsel_raw_ptr(PyObject* handle);
    extern "C" CxxMorsel* cxx_take_c(const CxxMorsel*, const int32_t*, uint32_t);
    extern "C" CxxMorsel* cxx_slice_c(const CxxMorsel*, uint32_t, uint32_t);
    extern "C" CxxMorsel* cxx_align_c(const CxxMorsel*, const CxxMorsel*,
                                      const int32_t*, const int32_t*, uint32_t);
    extern "C" CxxMorsel* cxx_cast_column_c(const CxxMorsel*, uint32_t, int);
    extern "C" CxxMorsel* cxx_mask_c(const CxxMorsel*, const DrakenVector*);
    extern "C" CxxMorsel* cxx_mask_with_consts_c(const CxxMorsel*, const DrakenVector*,
                                                  const int32_t*, const DrakenVector* const*, uint32_t);
    extern "C" CxxMorsel* cxx_select_c(const CxxMorsel*, const char**, const uint32_t*, uint32_t);
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
    # WP-07 two-sided inner-join align: gather left cols by lidx + right cols by
    # ridx (both non-negative), concat into one CxxMorsel. Free via cxx_morsel_delete.
    CxxMorsel* cxx_align_c(const CxxMorsel* l, const CxxMorsel* r,
                           const int32_t* lidx, const int32_t* ridx, uint32_t n) nogil
    # WP-07 nogil join-key cast: cast columns[col_idx] to FLOAT64 (target=0) or
    # INT64 (target=1) via phase-9c dispatch kernels; new CxxMorsel shares the
    # other columns. NULL on cast error / bad idx. Free via cxx_morsel_delete.
    CxxMorsel* cxx_cast_column_c(const CxxMorsel* m, uint32_t col_idx, int target) nogil
    # S1: whole-morsel mask (keep rows valid AND true) — derives indices once,
    # type-takes each column nogil. mask is the predicate BoolVector's view.
    CxxMorsel* cxx_mask_c(const CxxMorsel* m, const DrakenVector* mask) nogil
    # S1 twin: same, but columns in const_col_idx are known-constant post-filter
    # (WHERE col = <literal>) and are broadcast O(1) from a pre-resolved scalar
    # DrakenVector* instead of being gathered. See cxx_mask_with_consts in
    # draken_native.cpp for the caller-owns-the-scalar contract.
    CxxMorsel* cxx_mask_with_consts_c(const CxxMorsel* m, const DrakenVector* mask,
                                      const int32_t* const_col_idx,
                                      const DrakenVector* const* const_scalar_dv,
                                      uint32_t n_consts) nogil
    # S-B.2 column select/reorder by identity name (bytes via ptr+len arrays).
    CxxMorsel* cxx_select_c(const CxxMorsel* m, const char** name_ptrs,
                            const uint32_t* name_lens, uint32_t n) nogil
    # S-B.3a keying hash — single col → shape-preserving, multi → dense mix.
    # Returns a 1-column CxxMorsel (read columns[0].view; free via cxx_morsel_delete).
    CxxMorsel* cxx_hash_c(const CxxMorsel* m, const int32_t* col_idxs, uint32_t n_cols) nogil
    void cxx_morsel_delete(CxxMorsel* m) nogil
    # S-B.1a boundary bridges. shallow_copy: owned heap CxxMorsel sharing owners.
    # to_handle: NEW-ref nanobind handle wrapping a shallow copy (boundary out).
    CxxMorsel* cxx_morsel_shallow_copy(const CxxMorsel* m) nogil
    CxxMorsel* cxx_morsel_new_eos() nogil
    PyObject* cxx_morsel_to_handle(const CxxMorsel* m)
