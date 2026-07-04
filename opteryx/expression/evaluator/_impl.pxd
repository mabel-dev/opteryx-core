# Public cdef surface of evaluation.pyx for cross-module nogil use.
#
# S-B (GIL release): the filter operator (opteryx/operators/filter/filter.pyx, a
# separate compilation unit) converts its push body to genuine nogil by caching the
# predicate column/literal resolve ONCE (the only GIL-needing step) and then calling
# the pure-nogil filter span per morsel. These two cdef functions are the shared
# machinery; everything else in evaluation.pyx stays module-private.

from opteryx.compiled.expression.compiled_expression cimport CompiledBytecode, BytecodeInstr
from draken.morsels.cxx_morsel cimport CxxMorsel
from draken.core.buffers cimport DrakenVector

# GIL: resolve LOAD_COL identity → column index in the CxxMorsel and LOAD_LIT_CONST
# → DV*. Stable for a fixed pipeline schema → resolve once, reuse. 0 ok, -1 column
# not found (caller falls back to the Morsel VM path).
cdef int _dv_cxx_resolve_caches(CompiledBytecode bc, const CxxMorsel* m,
                                int* col_idx, DrakenVector** lit_dv) except -2

# Pure-nogil filter span over a PRE-RESOLVED (col_idx, lit_dv). Owns its frame arena.
# rc 0 → *out_filtered is a NEW owned CxxMorsel; 4 → kernel error (*err_msg set —
# a pointer into the failing kernel's thread; valid until the next kernel call on
# THIS thread, copy/decode it before that); 99 → arena OOM; other → not applicable
# (caller falls back).
cdef int _dv_filter_span_cxx(BytecodeInstr* instrs, int count, const CxxMorsel* m,
                             int* col_idx, DrakenVector** lit_dv,
                             CxxMorsel** out_filtered, int* err_op,
                             const char** err_msg) noexcept nogil

# Pure-nogil expression span for a COMPUTED column (projection twin of the filter
# span): evaluate + deep-copy the arena result into fresh draken_malloc'd buffers
# the caller owns. rc 0 → out_vec/out_data/out_validity/out_sel filled; 4 → kernel
# error (*err_msg set, same contract as _dv_filter_span_cxx); 98 → non-fixed-width
# result; 99 → arena OOM; other → not applicable.
from libc.stdint cimport uint8_t
cdef int _dv_eval_span_cxx(BytecodeInstr* instrs, int count, const CxxMorsel* m,
                           int* col_idx, DrakenVector** lit_dv,
                           DrakenVector* out_vec, void** out_data,
                           uint8_t** out_validity, void** out_sel,
                           int* err_op, const char** err_msg) noexcept nogil
