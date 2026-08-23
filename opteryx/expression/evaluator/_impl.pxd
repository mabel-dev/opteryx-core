# Public cdef surface of evaluation.pyx for cross-module nogil use.
#
# S-B (GIL release): the filter operator (opteryx/operators/filter/filter.pyx, a
# separate compilation unit) converts its push body to genuine nogil by caching the
# predicate column/literal resolve ONCE (the only GIL-needing step) and then calling
# the pure-nogil filter span per morsel. These two cdef functions are the shared
# machinery; everything else in evaluation.pyx stays module-private.

from opteryx.compiled.expression.compiled_expression cimport CompiledBytecode, BytecodeInstr
from draken.morsels.cxx_morsel cimport CxxMorsel
from draken.core.buffers cimport DrakenVector, DrakenType
from libc.stdint cimport int32_t, uint32_t, uint8_t

# SINGLE SOURCE for this extern type: declaring it again (even opaquely) inside
# evaluation.pyx — which is textually `include`d into _impl.pyx, this .pxd's own
# implementation — creates a conflicting duplicate cdef-extern-type declaration in
# the same compilation unit. Cython resolves that by keeping whichever declaration
# it sees first (this .pxd's, processed before the .pyx body), so a duplicate
# elsewhere silently degrades every `VecResult` use in evaluation.pyx to an
# untyped Python object — every `vr.data == NULL` etc. Field list must stay in
# sync with the real C++ struct (ops/vec_result.h); Cython only needs the fields
# actually dereferenced from Cython, not every field.
cdef extern from "ops/vec_result.h":
    ctypedef struct VecResult:
        void*             data
        uint8_t*          validity
        const uint32_t*   selection
        bint              owns_selection
        uint32_t          data_length
        uint32_t          length
        DrakenType        type
        uint8_t           flags
        uint8_t           validity_embedded
        const char*       error_msg
        void*             child   # VecResult* — void* here, self-reference breaks
                                   # ctypedef-struct parsing; cast at use sites.
        uint8_t           data_error  # 1 = the message is user-facing data-error
                                      # text (see ops/vec_result.h); the spans turn
                                      # that into rc 96 instead of rc 4.

# GIL: resolve LOAD_COL identity → column index in the CxxMorsel and LOAD_LIT_CONST
# → DV*. Stable for a fixed pipeline schema → resolve once, reuse. 0 ok, -1 column
# not found (caller falls back to the Morsel VM path).
cdef int _dv_cxx_resolve_caches(CompiledBytecode bc, const CxxMorsel* m,
                                int* col_idx, DrakenVector** lit_dv) except -2

# Pure-nogil filter span over a PRE-RESOLVED (col_idx, lit_dv). Owns its frame arena.
# rc 0 → *out_filtered is a NEW owned CxxMorsel; 4 → kernel error (*err_msg set —
# a pointer into the failing kernel's thread; valid until the next kernel call on
# THIS thread, copy/decode it before that); 96 → kernel DATA error, same *err_msg
# contract but the message is complete user-facing text that must be surfaced
# VERBATIM and never treated as a decline (see c_execute_dv_inner); 99 → arena
# OOM; other → not applicable (caller falls back).
cdef int _dv_filter_span_cxx(BytecodeInstr* instrs, int count, const CxxMorsel* m,
                             int* col_idx, DrakenVector** lit_dv,
                             CxxMorsel** out_filtered, int* err_op,
                             const char** err_msg) noexcept nogil

# _dv_filter_span_cxx twin for FilterNode `IDENTIFIER = LITERAL` const-replacements:
# columns in (const_col_idx, const_scalar_dv) are broadcast O(1) from a pre-resolved
# scalar DrakenVector* (data_length == 1, validity == nullptr) instead of taken, since
# the predicate already guarantees their value on every surviving row. Same
# resolve-once-reuse and rc/err_msg contract as _dv_filter_span_cxx.
cdef int _dv_filter_span_with_consts_cxx(
    BytecodeInstr* instrs, int count, const CxxMorsel* m,
    int* col_idx, DrakenVector** lit_dv,
    int32_t* const_col_idx, DrakenVector** const_scalar_dv, uint32_t n_consts,
    CxxMorsel** out_filtered, int* err_op, const char** err_msg) noexcept nogil

# Pure-nogil expression span for a COMPUTED column (projection twin of the filter
# span): evaluate + deep-copy the arena result into fresh draken_malloc'd buffers
# the caller owns. rc 0 → out_vec/out_data/out_validity/out_sel filled; 4 → kernel
# error (*err_msg set, same contract as _dv_filter_span_cxx); 96 → kernel DATA
# error (ditto); 98 → non-fixed-width result; 99 → arena OOM; other → not
# applicable.
from libc.stdint cimport uint8_t
cdef int _dv_eval_span_cxx(BytecodeInstr* instrs, int count, const CxxMorsel* m,
                           int* col_idx, DrakenVector** lit_dv,
                           DrakenVector* out_vec, void** out_data,
                           uint8_t** out_validity, void** out_sel,
                           int* err_op, const char** err_msg,
                           bint preserve_shape,
                           VecResult** out_child) noexcept nogil
