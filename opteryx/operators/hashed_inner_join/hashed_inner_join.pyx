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
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Draken-native inner join node.

This node is deliberately narrower than the legacy Arrow-first inner join:
- it keeps both sides in Draken morsels
- it uses the compiled Carchar join state directly
- it aligns output with Draken align_tables

Unsupported shapes fail in the physical planner rather than adding more
Arrow conversions here.
"""
from typing import Generator, Optional

import time
from threading import Lock

from array import array

from cpython.mem cimport PyMem_Malloc, PyMem_Free

from libc.stddef cimport size_t
from libc.stdint cimport int32_t, int64_t, uint64_t
from libcpp.utility cimport pair
from libcpp.vector cimport vector

from time import perf_counter_ns

from opteryx.compiled.structures.bloom_filter cimport BloomFilter, create_bloom_filter_from_hashes
from draken.morsels.morsel cimport align_tables
from draken.vectors.vector cimport NULL_HASH

from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.expression import NodeType
from opteryx.expression import get_all_nodes_of_type
from opteryx.expression.evaluator import compile_eval_nodes, execute_and_append
from opteryx.models import QueryProperties

# EOS sentinel available as _EOS_SENTINEL via the umbrella unit.
from opteryx import config

# BasePlanNode/JoinNode in scope via _operators.pyx include.


cdef class JoinReadings:
    """Zero-overhead telemetry accumulator for the inner join hot path.

    Replaces per-morsel Python dict writes (self.readings) with direct
    C-level field assignments. flush_into() transfers everything to the Python
    dict once at probe-side EOS.
    """
    # ── Build phase (left EOS, called once) ──────────────────────────────────
    cdef public int64_t time_inner_join_left_combine
    cdef public int64_t time_inner_join_left_accumulate
    cdef public int64_t time_inner_join_build_side_hash_map
    cdef public int64_t feature_inner_join_backend_carchar
    cdef public int64_t feature_inner_join_draken
    cdef public int64_t feature_bloom_filter
    cdef public int64_t time_build_bloom_filter
    cdef public int64_t build_unique_keys
    cdef public int64_t build_total_rows
    cdef public double  build_avg_chain_length

    # ── Per-projection (called per morsel projection) ─────────────────────────
    cdef public int64_t feature_eliminate_join_columns_draken

    # ── Probe phase (per right-side morsel) ───────────────────────────────────
    cdef public int64_t time_inner_join_hash
    cdef public int64_t time_inner_join_probe
    cdef public int64_t time_inner_join_indices
    cdef public int64_t time_bloom_filtering
    cdef public int64_t rows_inner_join_hashed
    cdef public int64_t rows_inner_join_candidates
    cdef public int64_t rows_inner_join_matched
    cdef public int64_t rows_eliminated_by_bloom_filter
    cdef public int64_t time_inner_join_total_kernel
    cdef public int64_t time_inner_join_align

    cpdef void flush_into(self, object readings):
        readings["time_inner_join_left_combine"]          = self.time_inner_join_left_combine
        readings["time_inner_join_left_accumulate"]       = self.time_inner_join_left_accumulate
        readings["time_inner_join_build_side_hash_map"]   = self.time_inner_join_build_side_hash_map
        readings["feature_inner_join_backend_carchar"]    = self.feature_inner_join_backend_carchar
        readings["feature_inner_join_draken"]             = self.feature_inner_join_draken
        readings["feature_bloom_filter"]                  = self.feature_bloom_filter
        readings["time_build_bloom_filter"]               = self.time_build_bloom_filter
        readings["build_unique_keys"]                     = self.build_unique_keys
        readings["build_total_rows"]                      = self.build_total_rows
        readings["build_avg_chain_length"]                = self.build_avg_chain_length
        readings["feature_eliminate_join_columns_draken"] = self.feature_eliminate_join_columns_draken
        readings["time_inner_join_hash"]                  = self.time_inner_join_hash
        readings["time_inner_join_probe"]                 = self.time_inner_join_probe
        readings["time_inner_join_indices"]               = self.time_inner_join_indices
        readings["time_bloom_filtering"]                  = self.time_bloom_filtering
        readings["rows_inner_join_hashed"]                = self.rows_inner_join_hashed
        readings["rows_inner_join_candidates"]            = self.rows_inner_join_candidates
        readings["rows_inner_join_matched"]               = self.rows_inner_join_matched
        readings["rows_eliminated_by_bloom_filter"]       = self.rows_eliminated_by_bloom_filter
        readings["time_inner_join_total_kernel"]          = self.time_inner_join_total_kernel
        readings["time_inner_join_align"]                 = self.time_inner_join_align


cdef class DrakenInnerJoinNode(JoinNode):
    cdef public list left_columns
    cdef public list right_columns
    cdef public object columns
    cdef public Morsel left_morsel
    cdef public list left_morsels
    cdef public object left_hash
    cdef public bint left_is_empty
    cdef public object lock
    cdef public bint _build_phase
    cdef public double carchar_probe_load_factor
    cdef public JoinReadings join_readings
    cdef public list _compiled_left_evals
    cdef public list _compiled_right_evals

    join_type = "inner"

    def __init__(self, properties=None, **parameters):
        JoinNode.__init__(self, properties=properties, **parameters)
        self.left_columns = list(parameters.get("left_columns") or [])
        self.right_columns = list(parameters.get("right_columns") or [])
        self.on = parameters.get("on")
        self.columns = parameters.get("columns")
        self.left_relation_names = parameters.get("left_relation_names") or []
        self.right_relation_names = parameters.get("right_relation_names") or []

        self.left_morsel = None
        self.left_morsels = []
        self.left_hash = None
        self.left_is_empty = False
        self.lock = Lock()
        self._build_phase = True
        self.carchar_probe_load_factor = float(
            config.get("FEATURE_CARCHAR_PROBE_LOAD_FACTOR", 0.35)
        )
        self.join_readings = JoinReadings()

        # Compile ON-clause expressions for each side at bind time.
        # _collect_expression_nodes_for_side is structural (no runtime data needed).
        self._compiled_left_evals = compile_eval_nodes(
            self._collect_expression_nodes_for_side(self.left_relation_names)
        )
        self._compiled_right_evals = compile_eval_nodes(
            self._collect_expression_nodes_for_side(self.right_relation_names)
        )

    @staticmethod
    def supports(**parameters) -> bool:
        on = parameters.get("on")
        if on is None:
            return True

        left_relation_names = set(parameters.get("left_relation_names") or [])
        right_relation_names = set(parameters.get("right_relation_names") or [])
        comparisons = get_all_nodes_of_type(on, (NodeType.COMPARISON_OPERATOR,))
        if not comparisons:
            return False

        for comparison in comparisons:
            if comparison.value != "Eq":
                return False
            if comparison.left is None or comparison.right is None:
                return False
            if comparison.left.node_type != NodeType.IDENTIFIER:
                return False
            if comparison.right.node_type != NodeType.IDENTIFIER:
                return False
            if not comparison.left.schema_column or not comparison.right.schema_column:
                return False

            left = comparison.left
            right = comparison.right
            if left.source in left_relation_names and right.source in right_relation_names:
                left_type = left.schema_column.type
                right_type = right.schema_column.type
            elif left.source in right_relation_names and right.source in left_relation_names:
                left_type = right.schema_column.type
                right_type = left.schema_column.type
            else:
                return False

            if (
                left_type != right_type
                and JoinNode._join_numeric_target_type(left_type, right_type) is not None
            ):
                return False

        return True

    @property
    def name(self):  # pragma: no cover
        return "Inner Join Draken"

    @property
    def config(self):  # pragma: no cover
        return "draken+carchar"

    @staticmethod
    def _encode_columns(columns):
        encoded = []
        for column in columns:
            if isinstance(column, bytes):
                encoded.append(column)
            else:
                encoded.append(str(column).encode("utf8"))
        return encoded

    cdef list _collect_expression_nodes_for_side(self, list relation_names):
        """Collect ON-clause expressions that should be evaluated on one side."""
        if not self.on:
            return []

        exprs = []
        comparisons = get_all_nodes_of_type(self.on, (NodeType.COMPARISON_OPERATOR,))
        side_relations = set(relation_names)

        for comparison in comparisons:
            if comparison.value != "Eq":
                continue
            left = comparison.left
            right = comparison.right

            def _refs_only(node):
                rels = getattr(node, "relations", None)
                if not rels:
                    return False
                return side_relations.issuperset(set(rels))

            if left is not None and left.node_type != NodeType.IDENTIFIER and _refs_only(left):
                exprs.append(left)
            if right is not None and right.node_type != NodeType.IDENTIFIER and _refs_only(right):
                exprs.append(right)

        return exprs

    cdef Morsel _project_morsel(self, Morsel morsel, list keep_names):
        encoded_keep = [name if isinstance(name, bytes) else name.encode("utf8") for name in keep_names]
        available = set(morsel.column_names)
        selected = [name for name in encoded_keep if name in available]
        if not selected or len(selected) == len(morsel.column_names):
            return morsel
        self.join_readings.feature_eliminate_join_columns_draken += 1
        return morsel.select(selected)

    cpdef void push_left(self, Morsel morsel) except *:
        with self.lock:
            if morsel is _EOS_SENTINEL:
                # Build-side EOS — finalise hash table. Do NOT emit downstream.
                if not self.left_morsels:
                    self.left_is_empty = True
                    return
                start = time.monotonic_ns()
                self.left_morsel = Morsel.combine(self.left_morsels)
                self.join_readings.time_inner_join_left_combine += time.monotonic_ns() - start
                self.left_morsels = []

                if self._compiled_left_evals and self.left_morsel.num_rows > 0:
                    old_cols = set(self.left_morsel.column_names)
                    try:
                        self.left_morsel = execute_and_append(
                            self._compiled_left_evals, self.left_morsel
                        )
                    except (NotImplementedError, TypeError, UnsupportedSyntaxError) as err:
                        raise UnsupportedSyntaxError(
                            f"Draken inner join expression evaluation does not support this query shape: {err}"
                        ) from err
                    new_cols = set(self.left_morsel.column_names) - old_cols
                    if new_cols:
                        for col in new_cols:
                            if col not in self.left_columns:
                                self.left_columns.append(col)

                if self.columns is not None and self.left_morsel.num_rows > 0:
                    candidate_names = [c.schema_column.identity for c in self.columns]
                    available_cols = set(self.left_morsel.column_names)
                    left_keep = [name for name in candidate_names if name in available_cols]
                    for join_col in self.left_columns:
                        join_bytes = join_col if isinstance(join_col, bytes) else str(join_col).encode("utf8")
                        if join_bytes not in left_keep:
                            left_keep.append(join_bytes)
                    if left_keep:
                        self.left_morsel = self._project_morsel(self.left_morsel, left_keep)

                start = time.monotonic_ns()
                self.left_hash = build_side_carchar_morsel_map(
                    self.left_morsel,
                    self.left_columns,
                    self.carchar_probe_load_factor,
                )
                self.join_readings.time_inner_join_build_side_hash_map += (
                    time.monotonic_ns() - start
                )
                self.join_readings.feature_inner_join_backend_carchar += 1
                self.join_readings.feature_inner_join_draken += 1
                (
                    _hash_time,
                    _probe_time,
                    _bloom_time,
                    _rows_hashed,
                    _candidate_rows,
                    _matched_rows,
                    _materialize_time,
                    _align_time,
                    _rows_eliminated,
                    bloom_build_time,
                    build_unique_keys,
                    build_total_rows,
                    build_avg_chain_length,
                ) = get_last_draken_inner_join_metrics()
                if self.left_hash.has_bloom_filter():
                    self.join_readings.feature_bloom_filter += 1
                    self.join_readings.time_build_bloom_filter += bloom_build_time
                self.join_readings.build_unique_keys += build_unique_keys
                self.join_readings.build_total_rows += build_total_rows
                self.join_readings.build_avg_chain_length = build_avg_chain_length
                return

            # Build-side data morsel — accumulate.
            if morsel is None or morsel.num_rows == 0:
                return
            start = time.monotonic_ns()
            self.left_morsels.append(morsel)
            self.join_readings.time_inner_join_left_accumulate += time.monotonic_ns() - start

    cpdef void push_right(self, Morsel morsel) except *:
        with self.lock:
            if morsel is _EOS_SENTINEL:
                # Probe-side EOS — flush telemetry then terminate downstream chain.
                self.join_readings.flush_into(self.readings)
                self.emit(_EOS_SENTINEL)
                return

            if self.left_is_empty:
                # Inner join with empty build side produces nothing.
                return

            if morsel is None or morsel.num_rows == 0:
                return

            right_chunk = morsel
            if self._compiled_right_evals and right_chunk.num_rows > 0:
                old_cols = set(right_chunk.column_names)
                try:
                    right_chunk = execute_and_append(self._compiled_right_evals, right_chunk)
                except (NotImplementedError, TypeError, UnsupportedSyntaxError) as err:
                    raise UnsupportedSyntaxError(
                        f"Draken inner join expression evaluation does not support this query shape: {err}"
                    ) from err
                new_cols = set(right_chunk.column_names) - old_cols
                if new_cols:
                    for col in new_cols:
                        if col not in self.right_columns:
                            self.right_columns.append(col)
            if self.columns is not None:
                candidate_names = [c.schema_column.identity for c in self.columns]
                available_cols = set(right_chunk.column_names)
                right_keep = [name for name in candidate_names if name in available_cols]
                for join_col in self.right_columns:
                    join_bytes = join_col if isinstance(join_col, bytes) else str(join_col).encode("utf8")
                    if join_bytes not in right_keep:
                        right_keep.append(join_bytes)
                if right_keep:
                    right_chunk = self._project_morsel(right_chunk, right_keep)

            start = time.monotonic_ns()
            aligned = inner_join_carchar_morsel_aligned(
                self.left_morsel,
                right_chunk,
                self.right_columns,
                self.left_hash,
            )
            total_join_ns = time.monotonic_ns() - start

            (
                hash_time,
                probe_time,
                bloom_time,
                rows_hashed,
                candidate_rows,
                matched_rows,
                materialize_time,
                align_time,
                rows_eliminated_by_bloom_filter,
                _bloom_build_time,
                _build_unique_keys,
                _build_total_rows,
                _build_avg_chain_length,
            ) = get_last_draken_inner_join_metrics()
            self.join_readings.time_inner_join_hash            += hash_time
            self.join_readings.time_inner_join_probe           += probe_time
            self.join_readings.time_inner_join_indices         += materialize_time
            self.join_readings.time_bloom_filtering            += bloom_time
            self.join_readings.rows_inner_join_hashed          += rows_hashed
            self.join_readings.rows_inner_join_candidates      += candidate_rows
            self.join_readings.rows_inner_join_matched         += matched_rows
            self.join_readings.rows_eliminated_by_bloom_filter += rows_eliminated_by_bloom_filter
            self.join_readings.time_inner_join_total_kernel    += total_join_ns
            self.join_readings.time_inner_join_align           += align_time
            if aligned is not None:
                self.emit(aligned)


# ---------------------------------------------------------------------------
# Draken-native Carchar inner-join kernels (moved from compiled/joins/inner_join.pyx)
# ---------------------------------------------------------------------------

cdef extern from "carchar.hpp" namespace "opteryx::carchar":
    cdef cppclass CarcharJoinEngine:
        CarcharJoinEngine(
            size_t expected_entries,
            size_t partition_bits,
            double load_factor,
            double probe_load_factor
        ) except +
        void insert_batch(const uint64_t* keys, const int64_t* row_ids, size_t length) except +
        void seal() except +
        size_t unique_key_count() noexcept
        uint64_t total_row_count() noexcept
        double average_chain_length() noexcept
        pair[vector[int64_t], vector[int64_t]] probe_join_indices(
            const uint64_t* keys,
            const int64_t* probe_rows,
            size_t length
        ) except +


cdef long long NULL_INT64_SENTINEL = -9223372036854775808

cdef public long long last_draken_inner_join_hash_time_ns = 0
cdef public long long last_draken_inner_join_probe_time_ns = 0
cdef public long long last_draken_inner_join_materialize_time_ns = 0
cdef public long long last_draken_inner_join_align_time_ns = 0
cdef public long long last_draken_inner_join_build_bloom_time_ns = 0
cdef public long long last_draken_inner_join_bloom_filter_time_ns = 0
cdef public Py_ssize_t last_draken_inner_join_rows_hashed = 0
cdef public Py_ssize_t last_draken_inner_join_candidate_rows = 0
cdef public Py_ssize_t last_draken_inner_join_result_rows = 0
cdef public Py_ssize_t last_draken_inner_join_rows_eliminated_by_bloom_filter = 0
# Adaptive join statistics — Phase 1 (per docs/adaptive_join_statistics.md).
cdef public Py_ssize_t last_draken_inner_join_build_unique_keys = 0
cdef public Py_ssize_t last_draken_inner_join_build_total_rows = 0
cdef public double last_draken_inner_join_build_avg_chain_length = 0.0


cdef class DrakenCarcharJoinMap:
    cdef CarcharJoinEngine* engine
    cdef object bloom_filter

    def __cinit__(self, Py_ssize_t expected_entries=0, double probe_load_factor=0.35):
        self.engine = new CarcharJoinEngine(
            <size_t>max(0, expected_entries),
            <size_t>0,
            <double>0.80,
            probe_load_factor,
        )
        self.bloom_filter = None

    def __dealloc__(self):
        if self.engine is not NULL:
            del self.engine
            self.engine = NULL

    cpdef void seal(self):
        self.engine.seal()

    cpdef bint has_bloom_filter(self):
        return self.bloom_filter is not None


cdef inline void _append_valid_rows_and_hashes(
    uint64_t[::1] row_hashes,
    vector[uint64_t]& valid_hashes,
    vector[int64_t]& valid_rows,
):
    cdef Py_ssize_t num_rows = row_hashes.shape[0]
    cdef Py_ssize_t i
    cdef uint64_t h
    cdef const uint64_t* hashes_ptr

    if num_rows == 0:
        return

    valid_hashes.reserve(num_rows)
    valid_rows.reserve(num_rows)
    hashes_ptr = &row_hashes[0]

    with nogil:
        for i in range(num_rows):
            h = hashes_ptr[i]
            if h != NULL_HASH:
                valid_rows.push_back(i)
                valid_hashes.push_back(h)


cdef inline void _append_bloom_filtered_rows_and_hashes(
    uint64_t[::1] row_hashes,
    BloomFilter bloom_filter,
    vector[uint64_t]& candidate_hashes,
    vector[int64_t]& candidate_rows,
):
    cdef Py_ssize_t num_rows = row_hashes.shape[0]
    cdef Py_ssize_t i
    cdef uint64_t h
    cdef const uint64_t* hashes_ptr

    if num_rows == 0:
        return

    candidate_hashes.reserve(num_rows)
    candidate_rows.reserve(num_rows)
    hashes_ptr = &row_hashes[0]

    with nogil:
        for i in range(num_rows):
            h = hashes_ptr[i]
            if h != NULL_HASH and bloom_filter._possibly_contains_fast(h):
                candidate_rows.push_back(i)
                candidate_hashes.push_back(h)


cdef object _int32_array_from_vector(const vector[int64_t]& values):
    cdef Py_ssize_t length = <Py_ssize_t> values.size()
    cdef object out = array('i', [0]) * length
    cdef int32_t[::1] out_view = out
    cdef Py_ssize_t i

    for i in range(length):
        out_view[i] = <int32_t> values[i]

    return out


cdef inline int32_t[::1] _int32_view_from_vector(
    const vector[int64_t]& values,
    int32_t** buffer_out,
) except *:
    cdef Py_ssize_t length = <Py_ssize_t> values.size()
    cdef int32_t* out_ptr = NULL
    cdef Py_ssize_t i

    buffer_out[0] = NULL
    if length == 0:
        return <int32_t[:0]> NULL

    out_ptr = <int32_t*>PyMem_Malloc(length * sizeof(int32_t))
    if out_ptr == NULL:
        raise MemoryError()

    for i in range(length):
        out_ptr[i] = <int32_t> values[i]

    buffer_out[0] = out_ptr
    return <int32_t[:length]> out_ptr


cpdef DrakenCarcharJoinMap build_side_carchar_morsel_map(
    Morsel relation,
    list join_columns,
    double probe_load_factor=0.35,
):
    global last_draken_inner_join_build_bloom_time_ns
    global last_draken_inner_join_build_unique_keys
    global last_draken_inner_join_build_total_rows
    global last_draken_inner_join_build_avg_chain_length
    cdef DrakenCarcharJoinMap ht
    cdef Py_ssize_t num_rows = relation.num_rows
    cdef uint64_t[::1] row_hashes
    cdef vector[uint64_t] valid_hashes
    cdef vector[int64_t] valid_rows
    cdef long long bloom_start
    cdef uint64_t* hashes_ptr
    cdef Py_ssize_t hashes_len

    ht = DrakenCarcharJoinMap(num_rows, probe_load_factor)
    last_draken_inner_join_build_bloom_time_ns = 0
    last_draken_inner_join_build_unique_keys = 0
    last_draken_inner_join_build_total_rows = 0
    last_draken_inner_join_build_avg_chain_length = 0.0
    if num_rows == 0:
        ht.seal()
        return ht

    row_hashes = relation.hash(join_columns)
    _append_valid_rows_and_hashes(row_hashes, valid_hashes, valid_rows)

    if valid_rows.size() != 0:
        ht.engine.insert_batch(
            &valid_hashes[0],
            &valid_rows[0],
            <size_t> valid_rows.size(),
        )
        if valid_hashes.size() != 0 and valid_hashes.size() <= <size_t>16_000_000:
            bloom_start = perf_counter_ns()
            hashes_ptr = valid_hashes.data()
            hashes_len = <Py_ssize_t> valid_hashes.size()
            ht.bloom_filter = create_bloom_filter_from_hashes(<uint64_t[:hashes_len:1]>hashes_ptr)
            last_draken_inner_join_build_bloom_time_ns = perf_counter_ns() - bloom_start

    ht.seal()
    last_draken_inner_join_build_unique_keys = <Py_ssize_t> ht.engine.unique_key_count()
    last_draken_inner_join_build_total_rows = <Py_ssize_t> ht.engine.total_row_count()
    last_draken_inner_join_build_avg_chain_length = ht.engine.average_chain_length()
    return ht


cpdef tuple inner_join_carchar_morsel(
    Morsel right_relation,
    list join_columns,
    DrakenCarcharJoinMap left_hash_table,
):
    global last_draken_inner_join_hash_time_ns
    global last_draken_inner_join_probe_time_ns
    global last_draken_inner_join_materialize_time_ns
    global last_draken_inner_join_align_time_ns
    global last_draken_inner_join_bloom_filter_time_ns
    global last_draken_inner_join_rows_hashed
    global last_draken_inner_join_candidate_rows
    global last_draken_inner_join_result_rows
    global last_draken_inner_join_rows_eliminated_by_bloom_filter

    cdef Py_ssize_t num_rows = right_relation.num_rows
    cdef uint64_t[::1] row_hashes
    cdef vector[uint64_t] probe_hashes
    cdef vector[int64_t] probe_rows
    cdef pair[vector[int64_t], vector[int64_t]] matches
    cdef object left_indices
    cdef object right_indices
    cdef long long t_start
    cdef long long t_after_hash
    cdef long long t_after_probe
    cdef long long t_after_materialize
    cdef long long bloom_start
    cdef long long bloom_end
    cdef BloomFilter bloom_filter

    if num_rows == 0:
        last_draken_inner_join_hash_time_ns = 0
        last_draken_inner_join_probe_time_ns = 0
        last_draken_inner_join_materialize_time_ns = 0
        last_draken_inner_join_align_time_ns = 0
        last_draken_inner_join_bloom_filter_time_ns = 0
        last_draken_inner_join_rows_hashed = 0
        last_draken_inner_join_candidate_rows = 0
        last_draken_inner_join_result_rows = 0
        last_draken_inner_join_rows_eliminated_by_bloom_filter = 0
        return array('i'), array('i')

    t_start = perf_counter_ns()
    row_hashes = right_relation.hash(join_columns)
    t_after_hash = perf_counter_ns()

    if left_hash_table.bloom_filter is not None:
        bloom_filter = <BloomFilter>left_hash_table.bloom_filter
        bloom_start = perf_counter_ns()
        _append_bloom_filtered_rows_and_hashes(
            row_hashes,
            bloom_filter,
            probe_hashes,
            probe_rows,
        )
        bloom_end = perf_counter_ns()
        last_draken_inner_join_bloom_filter_time_ns = bloom_end - bloom_start
        last_draken_inner_join_rows_eliminated_by_bloom_filter = (
            num_rows - <Py_ssize_t>probe_rows.size()
        )
    else:
        _append_valid_rows_and_hashes(
            row_hashes,
            probe_hashes,
            probe_rows,
        )
        bloom_end = perf_counter_ns()
        last_draken_inner_join_bloom_filter_time_ns = 0
        last_draken_inner_join_rows_eliminated_by_bloom_filter = 0

    if probe_rows.size() == 0:
        last_draken_inner_join_hash_time_ns = t_after_hash - t_start
        last_draken_inner_join_probe_time_ns = 0
        last_draken_inner_join_materialize_time_ns = 0
        last_draken_inner_join_align_time_ns = 0
        last_draken_inner_join_bloom_filter_time_ns = last_draken_inner_join_bloom_filter_time_ns
        last_draken_inner_join_rows_hashed = num_rows
        last_draken_inner_join_candidate_rows = 0
        last_draken_inner_join_result_rows = 0
        return array('i'), array('i')

    matches = left_hash_table.engine.probe_join_indices(
        &probe_hashes[0],
        &probe_rows[0],
        <size_t> probe_rows.size(),
    )
    t_after_probe = perf_counter_ns()
    left_indices = _int32_array_from_vector(matches.first)
    right_indices = _int32_array_from_vector(matches.second)
    t_after_materialize = perf_counter_ns()

    last_draken_inner_join_hash_time_ns = t_after_hash - t_start
    last_draken_inner_join_probe_time_ns = t_after_probe - bloom_end
    last_draken_inner_join_materialize_time_ns = t_after_materialize - t_after_probe
    last_draken_inner_join_align_time_ns = 0
    last_draken_inner_join_rows_hashed = num_rows
    last_draken_inner_join_candidate_rows = probe_rows.size()
    last_draken_inner_join_result_rows = matches.first.size()

    return left_indices, right_indices


cpdef object inner_join_carchar_morsel_aligned(
    Morsel left_relation,
    Morsel right_relation,
    list join_columns,
    DrakenCarcharJoinMap left_hash_table,
):
    global last_draken_inner_join_hash_time_ns
    global last_draken_inner_join_probe_time_ns
    global last_draken_inner_join_materialize_time_ns
    global last_draken_inner_join_align_time_ns
    global last_draken_inner_join_bloom_filter_time_ns
    global last_draken_inner_join_rows_hashed
    global last_draken_inner_join_candidate_rows
    global last_draken_inner_join_result_rows
    global last_draken_inner_join_rows_eliminated_by_bloom_filter

    cdef Py_ssize_t num_rows = right_relation.num_rows
    cdef uint64_t[::1] row_hashes
    cdef vector[uint64_t] probe_hashes
    cdef vector[int64_t] probe_rows
    cdef pair[vector[int64_t], vector[int64_t]] matches
    cdef long long t_start
    cdef long long t_after_hash
    cdef long long t_after_probe
    cdef long long t_before_align = 0
    cdef long long bloom_start
    cdef long long bloom_end
    cdef BloomFilter bloom_filter
    cdef int32_t* left_indices_ptr = NULL
    cdef int32_t* right_indices_ptr = NULL
    cdef int32_t[::1] left_indices_view
    cdef int32_t[::1] right_indices_view

    if num_rows == 0:
        last_draken_inner_join_hash_time_ns = 0
        last_draken_inner_join_probe_time_ns = 0
        last_draken_inner_join_materialize_time_ns = 0
        last_draken_inner_join_align_time_ns = 0
        last_draken_inner_join_bloom_filter_time_ns = 0
        last_draken_inner_join_rows_hashed = 0
        last_draken_inner_join_candidate_rows = 0
        last_draken_inner_join_result_rows = 0
        last_draken_inner_join_rows_eliminated_by_bloom_filter = 0
        return None

    t_start = perf_counter_ns()
    row_hashes = right_relation.hash(join_columns)
    t_after_hash = perf_counter_ns()

    if left_hash_table.bloom_filter is not None:
        bloom_filter = <BloomFilter>left_hash_table.bloom_filter
        bloom_start = perf_counter_ns()
        _append_bloom_filtered_rows_and_hashes(
            row_hashes,
            bloom_filter,
            probe_hashes,
            probe_rows,
        )
        bloom_end = perf_counter_ns()
        last_draken_inner_join_bloom_filter_time_ns = bloom_end - bloom_start
        last_draken_inner_join_rows_eliminated_by_bloom_filter = (
            num_rows - <Py_ssize_t>probe_rows.size()
        )
    else:
        _append_valid_rows_and_hashes(
            row_hashes,
            probe_hashes,
            probe_rows,
        )
        bloom_end = perf_counter_ns()
        last_draken_inner_join_bloom_filter_time_ns = 0
        last_draken_inner_join_rows_eliminated_by_bloom_filter = 0

    if probe_rows.size() == 0:
        last_draken_inner_join_hash_time_ns = t_after_hash - t_start
        last_draken_inner_join_probe_time_ns = 0
        last_draken_inner_join_materialize_time_ns = 0
        last_draken_inner_join_align_time_ns = 0
        last_draken_inner_join_rows_hashed = num_rows
        last_draken_inner_join_candidate_rows = 0
        last_draken_inner_join_result_rows = 0
        return None

    matches = left_hash_table.engine.probe_join_indices(
        &probe_hashes[0],
        &probe_rows[0],
        <size_t> probe_rows.size(),
    )
    t_after_probe = perf_counter_ns()

    last_draken_inner_join_hash_time_ns = t_after_hash - t_start
    last_draken_inner_join_probe_time_ns = t_after_probe - bloom_end
    last_draken_inner_join_rows_hashed = num_rows
    last_draken_inner_join_candidate_rows = probe_rows.size()
    last_draken_inner_join_result_rows = matches.first.size()

    if matches.first.size() == 0:
        last_draken_inner_join_materialize_time_ns = 0
        last_draken_inner_join_align_time_ns = 0
        return None

    try:
        left_indices_view = _int32_view_from_vector(matches.first, &left_indices_ptr)
        right_indices_view = _int32_view_from_vector(matches.second, &right_indices_ptr)
        last_draken_inner_join_materialize_time_ns = perf_counter_ns() - t_after_probe
        t_before_align = perf_counter_ns()
        return align_tables(left_relation, right_relation, left_indices_view, right_indices_view)
    finally:
        if left_indices_ptr != NULL:
            PyMem_Free(left_indices_ptr)
        if right_indices_ptr != NULL:
            PyMem_Free(right_indices_ptr)
        if t_before_align != 0:
            last_draken_inner_join_align_time_ns = perf_counter_ns() - t_before_align


cpdef tuple get_last_draken_inner_join_metrics():
    return (
        last_draken_inner_join_hash_time_ns,
        last_draken_inner_join_probe_time_ns,
        last_draken_inner_join_bloom_filter_time_ns,
        last_draken_inner_join_rows_hashed,
        last_draken_inner_join_candidate_rows,
        last_draken_inner_join_result_rows,
        last_draken_inner_join_materialize_time_ns,
        last_draken_inner_join_align_time_ns,
        last_draken_inner_join_rows_eliminated_by_bloom_filter,
        last_draken_inner_join_build_bloom_time_ns,
        last_draken_inner_join_build_unique_keys,
        last_draken_inner_join_build_total_rows,
        last_draken_inner_join_build_avg_chain_length,
    )
