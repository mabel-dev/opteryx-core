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
ASOF Join Node — Pure Draken

ASOF (As-Of) joins match each left row to the nearest right row by value
(typically a timestamp) rather than by exact equality. Standard usage:

    ASOF JOIN quotes MATCH_CONDITION(trades.ts >= quotes.ts) USING (symbol)

For each left row, we find the right row whose ASOF column value is the
closest that satisfies the inequality — i.e. the most recent quote at or
before the trade time, within the same symbol partition.

Algorithm:
  Build phase: buffer all left morsels; buffer all right morsels.
  Probe phase (on right EOS):
    - Optionally group right rows by equi-partition key.
    - Sort each partition by the ASOF column.
    - For each left row: binary-search for the nearest right match.
    - Emit aligned combined morsel for matched rows.
    - Emit left rows with null right columns for unmatched rows (LEFT semantics).

Supported MATCH_CONDITION operators: <, <=, >, >=
"""

from array import array as _pyarray
from bisect import bisect_left, bisect_right

from draken.morsels.morsel cimport align_tables
from draken.morsels.morsel cimport Morsel


# BasePlanNode / JoinNode / _EOS_SENTINEL in scope via _operators.pyx include.


cdef tuple _bisect_params(object op):
    """Return (bisect_fn, offset) for the ASOF operator."""
    if op == "GtEq":
        return bisect_right, -1   # largest right ≤ left
    if op == "Gt":
        return bisect_left, -1    # largest right < left
    if op == "LtEq":
        return bisect_left, 0     # smallest right ≥ left
    if op == "Lt":
        return bisect_right, 0    # smallest right > left
    raise ValueError(f"Unsupported ASOF operator: {op}")



cdef Morsel _perform_asof_join(
    Morsel left_morsel,
    Morsel right_morsel,
    object asof_left_col,
    object asof_right_col,
    object asof_op,
    list partition_left_cols,
    list partition_right_cols,
):
    """Core ASOF join kernel — LEFT semantics (every left row emitted)."""
    cdef Py_ssize_t left_rows = left_morsel.num_rows
    cdef Py_ssize_t right_rows = right_morsel.num_rows
    cdef Py_ssize_t i, pos
    cdef bint has_partition = len(partition_left_cols) > 0
    cdef int bisect_offset
    cdef object bisect_fn, right_partitions, part
    cdef object right_asof_vals, left_asof_vals
    cdef object right_part_keys, valid_pairs, paired
    cdef object global_sorted_vals, global_sorted_idxs
    cdef object sorted_vals, sorted_idxs
    cdef object matched_left_idx, matched_right_idx, unmatched_left_idx
    cdef object result_morsels
    cdef object right_asof_raw, left_asof_raw
    cdef object left_val, pkey, left_pkey, val
    cdef object matched_morsel, null_right, unmatched_morsel
    cdef object left_arr, right_arr, unmatched_arr, null_arr

    if left_rows == 0:
        return None

    bisect_fn, bisect_offset = _bisect_params(asof_op)

    right_asof_raw = right_morsel._cxx_column(asof_right_col)
    right_asof_vals = [right_asof_raw[i] for i in range(right_rows)]

    # Build sorted right-side structure (per-partition or global).
    if has_partition:
        right_partitions = {}
        right_part_keys = [
            tuple(right_morsel._cxx_column(c)[i] for c in partition_right_cols)
            for i in range(right_rows)
        ]
        for i in range(right_rows):
            pkey = right_part_keys[i]
            val  = right_asof_vals[i]
            if val is None:
                continue
            if pkey not in right_partitions:
                right_partitions[pkey] = ([], [])
            right_partitions[pkey][0].append(val)
            right_partitions[pkey][1].append(i)
        for pkey in right_partitions:
            sorted_vals, sorted_idxs = right_partitions[pkey]
            paired = sorted(zip(sorted_vals, sorted_idxs))
            right_partitions[pkey] = (
                [p[0] for p in paired],
                [p[1] for p in paired],
            )
        global_sorted_vals = []
        global_sorted_idxs = []
    else:
        valid_pairs = [
            (right_asof_vals[i], i)
            for i in range(right_rows)
            if right_asof_vals[i] is not None
        ]
        valid_pairs.sort()
        global_sorted_vals = [p[0] for p in valid_pairs]
        global_sorted_idxs = [p[1] for p in valid_pairs]
        right_partitions = {}

    # Probe: match each left row to the nearest right row.
    left_asof_raw = left_morsel._cxx_column(asof_left_col)
    left_asof_vals = [left_asof_raw[i] for i in range(left_rows)]

    matched_left_idx  = []
    matched_right_idx = []
    unmatched_left_idx = []

    for i in range(left_rows):
        left_val = left_asof_vals[i]
        if left_val is None:
            unmatched_left_idx.append(i)
            continue

        if has_partition:
            left_pkey = tuple(
                left_morsel._cxx_column(c)[i] for c in partition_left_cols
            )
            part = right_partitions.get(left_pkey)
            if part is None:
                unmatched_left_idx.append(i)
                continue
            sorted_vals, sorted_idxs = part
        else:
            sorted_vals = global_sorted_vals
            sorted_idxs = global_sorted_idxs

        if not sorted_vals:
            unmatched_left_idx.append(i)
            continue

        pos = bisect_fn(sorted_vals, left_val) + bisect_offset
        if pos < 0 or pos >= len(sorted_vals):
            unmatched_left_idx.append(i)
            continue

        matched_left_idx.append(i)
        matched_right_idx.append(sorted_idxs[pos])

    # Assemble output: build combined left/right index arrays.
    # Unmatched left rows use -1 as the right index; align_tables treats any
    # negative right index as null for all right columns (LEFT join semantics).
    # This mirrors how OuterJoinNode handles unmatched left rows.
    cdef Py_ssize_t n_total = len(matched_left_idx) + len(unmatched_left_idx)
    if n_total == 0:
        return None

    left_arr  = _pyarray('i', matched_left_idx  + unmatched_left_idx)
    right_arr = _pyarray('i', matched_right_idx + ([-1] * len(unmatched_left_idx)))

    return align_tables(
        left_morsel, right_morsel,
        memoryview(left_arr), memoryview(right_arr),
    )


# ---------------------------------------------------------------------------
# Node
# ---------------------------------------------------------------------------

cdef class AsofJoinNode(JoinNode):
    cdef public object asof_left_column
    cdef public object asof_right_column
    cdef public object asof_op
    cdef public list left_columns
    cdef public list right_columns
    cdef public Morsel left_morsel
    cdef public list left_morsels
    cdef public list right_morsels

    join_type = "asof"

    def __init__(self, properties=None, **parameters):
        JoinNode.__init__(self, properties=properties, **parameters)

        self.asof_left_column  = parameters.get("asof_left_column")
        self.asof_right_column = parameters.get("asof_right_column")
        self.asof_op           = parameters.get("asof_op")

        # The optional USING equi-partition keys. These arrive from the binder's
        # extract_join_fields, which already appends `schema_column.identity` —
        # they are column IDENTITIES, not bound nodes. Every other join operator
        # (inner, nested loop, filter) consumes them directly; do the same.
        #
        # They MUST be stored under `left_columns`/`right_columns`: the native
        # compiler's _compile_asof_join reads those names off this node to build
        # the build/probe key indices. Storing them under any other name makes
        # USING silently vanish and the join degrade to an unpartitioned ASOF.
        self.left_columns  = list(parameters.get("left_columns") or [])
        self.right_columns = list(parameters.get("right_columns") or [])

        self.left_morsel  = None
        self.left_morsels = []
        self.right_morsels = []

        if not self.asof_left_column or not self.asof_right_column or not self.asof_op:
            raise ValueError(
                "AsofJoinNode requires asof_left_column, asof_right_column, and asof_op"
            )

    @property
    def name(self):  # pragma: no cover
        return "ASOF Join"

    @property
    def config(self):  # pragma: no cover
        op_map = {"Lt": "<", "LtEq": "<=", "Gt": ">", "GtEq": ">="}
        op_sym = op_map.get(self.asof_op, self.asof_op)
        base = f"MATCH_CONDITION({self.asof_left_column} {op_sym} {self.asof_right_column})"
        if self.left_columns:
            # Identities are bytes — decode for display.
            names = ", ".join(
                c.decode("utf8") if isinstance(c, bytes) else str(c)
                for c in self.left_columns
            )
            base += f" USING ({names})"
        return base

    cdef int push_left(self, shared_ptr[CxxMorsel] m, ErrCtx* err) noexcept nogil:
        cdef CxxMorsel* raw = m.get()
        cdef bint is_eos = (raw != NULL and raw.state == MorselState.END_OF_STREAM)
        with gil:
            try:
                if is_eos:
                    self._push_left_gil(_EOS_SENTINEL)
                else:
                    self._push_left_gil(cxx_to_morsel(m))
            except BaseException as exc:  # noqa: BLE001 — surfaced via ErrCtx
                self._stash_exc(exc, err)
        return err.code if err != NULL else 0

    cdef void _push_left_gil(self, Morsel morsel) except *:
        if morsel is _EOS_SENTINEL:
            self._build_complete = True
            if self.left_morsels:
                self.left_morsel = Morsel.combine(self.left_morsels)
                self.left_morsels = []
            return
        if morsel is not None:
            self.left_morsels.append(morsel)

    cdef int push_right(self, shared_ptr[CxxMorsel] m, ErrCtx* err) noexcept nogil:
        cdef CxxMorsel* raw = m.get()
        cdef bint is_eos = (raw != NULL and raw.state == MorselState.END_OF_STREAM)
        with gil:
            try:
                if is_eos:
                    self._push_right_gil(_EOS_SENTINEL)
                else:
                    self._push_right_gil(cxx_to_morsel(m))
            except BaseException as exc:  # noqa: BLE001 — surfaced via ErrCtx
                self._stash_exc(exc, err)
        return err.code if err != NULL else 0

    cdef void _push_right_gil(self, Morsel morsel) except *:
        cdef Morsel right_morsel, result
        self._require_build_complete()
        if morsel is _EOS_SENTINEL:
            if self.left_morsel is None or self.left_morsel.num_rows == 0:
                self.emit(_EOS_SENTINEL)
                return

            if not self.right_morsels:
                self.emit(_EOS_SENTINEL)
                return

            right_morsel = Morsel.combine(self.right_morsels)
            self.right_morsels = []

            result = _perform_asof_join(
                self.left_morsel,
                right_morsel,
                self.asof_left_column,
                self.asof_right_column,
                self.asof_op,
                self.left_columns,
                self.right_columns,
            )
            if result is not None:
                self.emit(result)
            self.emit(_EOS_SENTINEL)
            return

        if morsel is not None:
            self.right_morsels.append(morsel)
