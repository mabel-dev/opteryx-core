# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Selection Node

This is a SQL Query Execution Plan Node.

This node is responsible for applying filters to datasets.
"""

from typing import Generator, Optional
from opteryx.compiled.expression.compiled_expression import build_bytecode as _build_bytecode
from opteryx.compiled.expression.compiled_expression import lower as _lower_expr
from opteryx.expression import NodeType
from opteryx.expression import format_expression
from opteryx.expression import get_all_nodes_of_type
from opteryx.expression.evaluator import execute_bytecode as _execute_bytecode
from opteryx.expression.evaluator.evaluation import filter_morsel_c_native as _filter_morsel_c_native
from opteryx.models import QueryProperties

from libc.stdint cimport int32_t, uint32_t
from libc.stdlib cimport malloc, free
from opteryx.compiled.expression.compiled_expression cimport CompiledBytecode, BytecodeInstr
from opteryx.expression.evaluator._impl cimport (
    _dv_cxx_resolve_caches,
    _dv_filter_span_cxx,
    _dv_filter_span_with_consts_cxx,
)
from draken.vectors.vector cimport Vector
from draken.core.buffers cimport DrakenVector
from draken.morsels.cxx_morsel cimport cxx_select_c

# BasePlanNode is defined at the top of _operators.pyx (the umbrella unit) and
# is in scope here via textual include.


def _extract_constant_replacements(filter_expr):
    """Find IDENTIFIER == LITERAL predicates that force a column to be constant
    in all rows surviving the filter.

    Descends through AND and NESTED only — OR, NOT, function calls, etc.
    terminate the walk on that branch. Returns a list of (identity, value)
    tuples. The identity is the bytes used as the morsel column key.
    """
    if filter_expr is None:
        return []

    preds = []
    stack = [filter_expr]
    while stack:
        n = stack.pop()
        nt = n.node_type
        if nt == NodeType.NESTED:
            if n.centre is not None:
                stack.append(n.centre)
            continue
        if nt == NodeType.AND:
            if n.left is not None:
                stack.append(n.left)
            if n.right is not None:
                stack.append(n.right)
            continue
        if nt == NodeType.DNF:
            # Despite the name, DNF here is a flat AND-list of sub-predicates
            # (see opteryx.expression.evaluator.evaluation: each parameter is
            # combined via and_vector). It is the planner's normalized form for
            # multi-predicate filters.
            params = getattr(n, "parameters", None) or []
            for sub in params:
                if sub is not None:
                    stack.append(sub)
            continue
        if nt != NodeType.COMPARISON_OPERATOR or n.value != "Eq":
            continue
        left = n.left
        right = n.right
        if left is None or right is None:
            continue
        if (left.node_type == NodeType.IDENTIFIER
                and right.node_type == NodeType.LITERAL):
            ident_node, lit_node = left, right
        elif (right.node_type == NodeType.IDENTIFIER
                and left.node_type == NodeType.LITERAL):
            ident_node, lit_node = right, left
        else:
            continue
        sc = getattr(ident_node, "schema_column", None)
        if sc is None:
            continue
        lit_val = lit_node.value
        if lit_val is None:
            continue
        preds.append((sc.identity, lit_val))
    return preds


cdef Vector _build_constant_vector_for_type(DrakenType t, object value, Py_ssize_t length):
    """Produce a constant-encoded vector of concrete type `t`.

    Returns None for vector types we don't yet handle (temporal, decimal, etc.)
    or when the literal's Python type can't safely map onto the column dtype.

    Wraps the nanobind result in a Cython-shim Vector so callers that
    declare `cdef Vector new_vec` and downstream code that does
    `morsel._get_column(idx)` see a consistent type.
    """
    if t == DRAKEN_BOOL:
        if not isinstance(value, bool):
            return None
        return Vector(_draken_native.vector_from_bool_constant(value, length))
    if t == DRAKEN_INT64:
        if isinstance(value, bool) or not isinstance(value, int):
            return None
        return Vector(_draken_native.vector_from_constant(value, length))
    if t == DRAKEN_FLOAT64:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        return Vector(_draken_native.vector_float64_from_constant(float(value), length))
    if t == DRAKEN_VARCHAR or t == DRAKEN_NVARCHAR:
        if not isinstance(value, (str, bytes)):
            return None
        # The string edge is bytes-only — encode str to bytes (str must not reach
        # the Draken edge). Bytes are stored verbatim (no decode).
        if isinstance(value, str):
            value = value.encode("utf-8")
        if t == DRAKEN_NVARCHAR:
            return Vector(_draken_native.vector_nvarchar_from_constant(value, length))
        return Vector(_draken_native.vector_varchar_from_constant(value, length))
    if t == DRAKEN_VARBINARY:
        if not isinstance(value, (str, bytes)):
            return None
        if isinstance(value, str):
            value = value.encode()
        return Vector(_draken_native.vector_varbinary_from_constant(value, length))
    return None


cdef Vector _build_constant_vector(Vector cur, object value, Py_ssize_t length):
    """_build_constant_vector_for_type, keyed off an existing column's concrete type."""
    return _build_constant_vector_for_type(cur.unified().type, value, length)


cdef Morsel _apply_constant_replacements(Morsel morsel, list replacements):
    cdef Py_ssize_t length = morsel.ptr.num_rows
    cdef Py_ssize_t idx, i, ncols
    cdef Vector cur
    cdef Vector new_vec
    cdef dict mapping
    cdef object py_idx
    cdef list col_vecs
    cdef bint changed = False

    if length == 0 or not replacements:
        return morsel

    mapping = morsel._ensure_name_map()

    # A Cxx-backed morsel has no PyObject column store to mutate (_set_column
    # would deref an empty vector). Rebuild it from the substrate columns with
    # the replaced column swapped in. PyObject-backed morsels mutate in place.
    if morsel._cxx is not None:
        ncols = morsel._num_columns()
        col_vecs = []
        for i in range(ncols):
            col_vecs.append(morsel._get_column(i))
        for identity, value in replacements:
            py_idx = mapping.get(identity)
            if py_idx is None:
                continue
            idx = <Py_ssize_t>py_idx
            cur = <Vector>col_vecs[idx]
            if cur is None:
                continue
            new_vec = _build_constant_vector(cur, value, length)
            if new_vec is None:
                continue
            col_vecs[idx] = new_vec
            changed = True
        if not changed:
            return morsel
        return Morsel.from_cxx_vectors(list(morsel._col_names), col_vecs)

    for identity, value in replacements:
        py_idx = mapping.get(identity)
        if py_idx is None:
            continue
        idx = <Py_ssize_t>py_idx
        cur = morsel._get_column(idx)
        if cur is None:
            continue
        new_vec = _build_constant_vector(cur, value, length)
        if new_vec is None:
            continue
        morsel._set_column(idx, new_vec)
    return morsel


cdef class FilterNode(BasePlanNode):
    cdef public object filter
    cdef public object post_filter_columns
    cdef public list function_evaluations
    cdef public list _const_replacements
    cdef CompiledBytecode _compiled_filter
    # S-B genuine-nogil filter path: cache the predicate resolve ONCE and run the
    # pure-nogil span per morsel (no GIL re-acquire in the hot body).
    cdef bint _flt_nogil_ok            # c-native predicate (post-filter/const-repl handled nogil too)
    cdef bint _flt_resolved            # caches populated (first push)
    cdef BytecodeInstr* _flt_instrs    # cached bc.instrs (read nogil from self)
    cdef int _flt_count                # cached bc.count
    cdef int _flt_col_idx[256]         # LOAD_COL identity → column index (resolved once)
    cdef DrakenVector* _flt_lit_dv[256]  # LOAD_LIT_CONST literal DV* (resolved once)
    # post-filter column projection applied natively (cxx_select_c) in the nogil span;
    # the keep-column names are resolved ONCE (first push) from the morsel schema.
    cdef bint _flt_has_post            # apply the post-filter select
    cdef const char** _flt_keep_ptrs   # keep-column name pointers (owned malloc)
    cdef uint32_t* _flt_keep_lens      # keep-column name lengths (owned malloc)
    cdef uint32_t _flt_keep_n          # keep-column count
    cdef list _flt_keep_buffers        # keep the name bytes alive for the ptrs
    # `IDENTIFIER = LITERAL` const-replacements applied natively (cxx_mask_with_consts_c)
    # in the nogil span: a column proven constant on every surviving row is broadcast
    # O(1) from a pre-resolved scalar DrakenVector* instead of taken and discarded.
    cdef int32_t _flt_const_col_idx[256]  # replacement column index (resolved once)
    cdef DrakenVector* _flt_const_dv[256]  # replacement scalar DV* (resolved once, length==1)
    cdef uint32_t _flt_const_n         # replacement count actually applicable nogil (0 = none)
    cdef list _flt_const_vecs          # keep the scalar Vector objects alive for the DV* pointers
    cdef public unsigned long long nogil_filter_morsels   # telemetry: nogil-path count

    def __cinit__(self, *args, **kwargs):
        # Owned-pointer + resolve-state init at ALLOCATION (runs for both __init__
        # construction and `make_worker`'s `__new__`). Without this, a worker built
        # via `__new__` (which skips __init__) would carry garbage in the malloc'd
        # keep-buffers and `__dealloc__` would free() garbage. The first-push caches
        # (`_flt_col_idx`/`_flt_lit_dv`) are gated by `_flt_resolved`, so they are
        # never read before being written.
        self._flt_keep_ptrs = NULL
        self._flt_keep_lens = NULL
        self._flt_keep_n = 0
        self._flt_keep_buffers = None
        self._flt_const_n = 0
        self._flt_const_vecs = None
        self._flt_resolved = False
        self._flt_has_post = False
        self._flt_nogil_ok = False
        self.nogil_filter_morsels = 0

    def __init__(self, properties=None, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.filter = parameters.get("filter")
        self.post_filter_columns = parameters.get("pre_update_columns")

        self.function_evaluations = get_all_nodes_of_type(
            self.filter,
            select_nodes=(NodeType.FUNCTION,),
        )

        self._const_replacements = _extract_constant_replacements(self.filter)

        # Lower the filter predicate to a C++ arena and linearise it into a
        # typed CompiledBytecode at bind time.  Every morsel iterates a C
        # struct array — no Python Node tree traversal at execute time.
        if self.filter is not None:
            self._compiled_filter = _build_bytecode(_lower_expr(self.filter))
        else:
            self._compiled_filter = None

        # S-B nogil path eligibility: the predicate must be all-C-native. The
        # post-filter column select (cxx_select_c) AND `IDENTIFIER = LITERAL`
        # const-replacements (cxx_mask_with_consts_c) are BOTH supported nogil —
        # their resolved state (keep-columns / (col_idx, scalar DV*)) is built
        # once, below. Ineligible cases keep the transitional gil-wrapped _push_impl.
        self._flt_resolved = False
        self._flt_has_post = False
        self._flt_keep_ptrs = NULL
        self._flt_keep_lens = NULL
        self._flt_keep_n = 0
        self._flt_keep_buffers = None
        self._flt_const_n = 0
        self._flt_const_vecs = None
        self.nogil_filter_morsels = 0
        # Tentative eligibility. is_all_c_native is read at FIRST PUSH, not here: the
        # flag is finalised after construction, so reading it in __init__ is stale.
        # The runtime resolve below confirms it and pins the caches; an ineligible
        # predicate sets _flt_nogil_ok False and never retries.
        if self._compiled_filter is not None:
            self._flt_nogil_ok = True
            self._flt_instrs = self._compiled_filter.instrs
            self._flt_count = self._compiled_filter.count
        else:
            self._flt_nogil_ok = False

    def __dealloc__(self):
        if self._flt_keep_ptrs != NULL:
            free(self._flt_keep_ptrs)
        if self._flt_keep_lens != NULL:
            free(self._flt_keep_lens)

    cdef BasePlanNode make_worker(self):
        # SPEC: the compiled predicate (`_compiled_filter`) + its derived metadata,
        # shared by reference — the bytecode lower+build is NOT redone (the pattern-1
        # win). STATE: the first-push caches stay per-worker; `__cinit__` already
        # NULLed the keep-buffers and cleared `_flt_resolved`, so each worker
        # re-resolves them on its first push exactly as `_clone_op` did via __init__.
        # The tentative nogil eligibility is recomputed from the shared compiled
        # filter (cheap), byte-identical to __init__.
        cdef FilterNode w = FilterNode.__new__(FilterNode)
        self._copy_worker_base(w)
        w.filter = self.filter
        w.post_filter_columns = self.post_filter_columns
        w.function_evaluations = self.function_evaluations
        w._const_replacements = self._const_replacements
        w._compiled_filter = self._compiled_filter
        if w._compiled_filter is not None:
            w._flt_nogil_ok = True
            w._flt_instrs = w._compiled_filter.instrs
            w._flt_count = w._compiled_filter.count
        return w

    cdef void _flt_resolve_keep(self, const CxxMorsel* m):
        """GIL: resolve the post-filter keep-columns ONCE from the morsel schema
        (input column order ∩ post_filter_columns). Only applied if it actually
        reduces the column count (mirrors _push_impl)."""
        cdef Py_ssize_t i, nn = <Py_ssize_t>m.names.size()
        cdef bytes nm
        cdef list keep = []
        if self.post_filter_columns:
            for i in range(nn):
                nm = m.names[i]
                if nm in self.post_filter_columns:
                    keep.append(nm)
        if not self.post_filter_columns or len(keep) >= nn:
            self._flt_has_post = False
            return
        self._flt_has_post = True
        self._flt_keep_n = <uint32_t>len(keep)
        self._flt_keep_buffers = keep   # keep bytes alive for the ptrs
        self._flt_keep_ptrs = <const char**>malloc(<size_t>len(keep) * sizeof(char*))
        self._flt_keep_lens = <uint32_t*>malloc(<size_t>len(keep) * sizeof(uint32_t))
        cdef bytes b
        for i in range(len(keep)):
            b = keep[i]
            self._flt_keep_ptrs[i] = <const char*>b
            self._flt_keep_lens[i] = <uint32_t>len(b)

    cdef void _flt_resolve_consts(self, const CxxMorsel* m):
        """GIL: resolve `IDENTIFIER = LITERAL` const-replacements to (col_idx,
        scalar DrakenVector*) ONCE from the morsel schema — mirrors
        _flt_resolve_keep. A replacement is dropped here (that column falls
        through to the ordinary take in cxx_mask_with_consts_c, same as it
        would in plain cxx_mask_c) when its column identity isn't found, or
        _build_constant_vector_for_type doesn't support the concrete type
        (temporal/decimal/etc.) — the GIL _push_impl fallback still folds
        those via _apply_constant_replacements."""
        cdef Py_ssize_t i, nn = <Py_ssize_t>m.names.size()
        cdef bytes nm, identity
        cdef object value
        cdef Py_ssize_t found
        cdef DrakenType t
        cdef Vector scalar_vec
        cdef list vecs = []
        cdef list idxs = []
        self._flt_const_n = 0
        if not self._const_replacements:
            return
        for identity, value in self._const_replacements:
            found = -1
            for i in range(nn):
                nm = m.names[i]
                if nm == identity:
                    found = i
                    break
            if found < 0:
                continue
            t = m.columns[found].view.type
            scalar_vec = _build_constant_vector_for_type(t, value, 1)
            if scalar_vec is None:
                continue
            idxs.append(<int>found)
            vecs.append(scalar_vec)
        if not idxs:
            return
        self._flt_const_vecs = vecs   # keep the scalar Vectors alive for the DV* pointers
        self._flt_const_n = <uint32_t>len(idxs)
        for i in range(len(idxs)):
            self._flt_const_col_idx[i] = <int32_t>idxs[i]
            self._flt_const_dv[i] = (<Vector>vecs[i]).unified()

    @property
    def config(self):  # pragma: no cover
        return format_expression(self.filter)

    @property
    def name(self):  # pragma: no cover
        return "Filter"

    cdef int _dispatch_push(self, shared_ptr[CxxMorsel] m, ErrCtx* err) noexcept nogil:
        """S-B genuine-nogil filter. For an all-c-native predicate (and no constant
        replacements / post-filter select), evaluate the predicate and gather the
        survivors ENTIRELY nogil, then forward the native carrier via _emit_cdef — no
        GIL re-acquire in the hot body, so parallel workers don't serialize here. The
        column resolve needs the GIL ONCE (first morsel); thereafter the span is pure
        nogil. EOS passes through; ineligible predicates and any kernel rc != 0 fall
        back to the transitional gil-wrapped _push_impl."""
        cdef CxxMorsel* raw = m.get()
        cdef bint is_eos = (raw != NULL and raw.state == MorselState.END_OF_STREAM)
        cdef CxxMorsel* filtered = NULL
        cdef CxxMorsel* selected = NULL
        cdef int err_op = 0
        cdef const char* err_msg_ptr = NULL
        cdef int rc
        if is_eos:
            return self._emit_cdef(m, err)        # forward the EOS carrier
        if self._flt_nogil_ok:
            if not self._flt_resolved:
                with gil:
                    # is_all_c_native is only reliable now (finalised post-construction).
                    if (not self._compiled_filter.is_all_c_native
                            or _dv_cxx_resolve_caches(self._compiled_filter, raw,
                                                      self._flt_col_idx, self._flt_lit_dv) != 0):
                        self._flt_nogil_ok = False   # not c-native / column not found
                    else:
                        self._flt_resolve_keep(raw)    # post-filter keep-columns (once)
                        self._flt_resolve_consts(raw)  # eq-literal const-replacements (once)
                        self._flt_resolved = True
            if self._flt_nogil_ok:
                # rc != 0 (kernel error / not applicable) falls through to the gil
                # path below, which re-runs the predicate through the GIL VM and
                # raises there with its own message — err_msg_ptr is unused here.
                if self._flt_const_n > 0:
                    rc = _dv_filter_span_with_consts_cxx(
                        self._flt_instrs, self._flt_count, raw,
                        self._flt_col_idx, self._flt_lit_dv,
                        self._flt_const_col_idx, self._flt_const_dv, self._flt_const_n,
                        &filtered, &err_op, &err_msg_ptr)
                else:
                    rc = _dv_filter_span_cxx(self._flt_instrs, self._flt_count, raw,
                                             self._flt_col_idx, self._flt_lit_dv,
                                             &filtered, &err_op, &err_msg_ptr)
                if rc == 0:
                    self.nogil_filter_morsels += 1
                    if filtered != NULL and filtered.num_rows() > 0:
                        if self._flt_has_post:
                            selected = cxx_select_c(filtered, self._flt_keep_ptrs,
                                                    self._flt_keep_lens, self._flt_keep_n)
                            cxx_morsel_delete(filtered)
                            return self._emit_cdef(shared_ptr[CxxMorsel](selected), err)
                        return self._emit_cdef(shared_ptr[CxxMorsel](filtered), err)
                    if filtered != NULL:
                        cxx_morsel_delete(filtered)   # empty result → drop
                    return 0
                # rc != 0 (kernel error / not applicable): fall through to the gil path.
        with gil:
            try:
                self._push_impl(cxx_to_morsel(m))
            except BaseException as exc:  # noqa: BLE001 — surfaced via ErrCtx
                self._stash_exc(exc, err)
        return err.code if err != NULL else 0

    cpdef void _push_impl(self, Morsel morsel) except *:
        # Body runs GIL-held: the base nogil `_dispatch_push` decodes the C++
        # carrier and calls this, surfacing any exception via the ErrCtx path.
        cdef BoolVector mask
        cdef Morsel filtered
        cdef list keep
        if morsel is _EOS_SENTINEL:
            self.emit(morsel)
            return

        # S3.2: for all-C-native predicates, evaluate AND mask in ONE nogil span
        # over the CxxMorsel (columns straight from columns[idx].view; the
        # predicate result feeds cxx_mask_c without a Python BoolVector). Returns
        # None when not applicable → fall back to the Morsel VM + filter_mask.
        filtered = None
        if self._compiled_filter.is_all_c_native:
            filtered = _filter_morsel_c_native(self._compiled_filter, morsel)
        if filtered is None:
            mask = _execute_bytecode(self._compiled_filter, morsel)
            filtered = morsel.filter_mask(mask)

        if self._const_replacements:
            filtered = _apply_constant_replacements(filtered, self._const_replacements)

        if self.post_filter_columns:
            keep = [c for c in filtered.column_names if c in self.post_filter_columns]
            if len(keep) < filtered.num_columns:
                filtered = filtered.select(keep)

        if filtered.num_rows > 0:
            self.emit(filtered)
        # Empty-output filters: do nothing (drop the morsel). Previous code
        # emitted morsel.slice(0,0); under push semantics EMPTY-like outputs
        # are suppressed and the downstream sees fewer morsels.
