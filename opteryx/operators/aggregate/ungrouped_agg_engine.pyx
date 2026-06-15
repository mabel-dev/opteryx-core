# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

# included by ungrouped_agg.pyx — do not compile standalone


cdef class AvgFinalizer:
    """Typed record of an AVG(sum_alias, count_alias) → output_alias plan.

    Stored as cdef-class so the hot path can read fields without dict lookups.
    """
    cdef bytes sum_alias
    cdef bytes count_alias
    cdef bytes output_alias

    def __cinit__(self, bytes sum_alias, bytes count_alias, bytes output_alias):
        self.sum_alias = sum_alias
        self.count_alias = count_alias
        self.output_alias = output_alias


cdef class UngroupedAggregateEngine:
    cdef list                 _aggregates_pyrefs
    cdef void**               _agg_ptrs
    cdef Py_ssize_t           _n_aggregates
    cdef Py_ssize_t           _agg_capacity

    cdef list                 _avg_finalizers_pyrefs
    cdef void**               _avg_ptrs
    cdef Py_ssize_t           _n_avgs
    cdef Py_ssize_t           _avg_capacity

    cdef set                  _internal_aliases

    """
    Drives ungrouped (global) aggregation over a stream of Draken morsels.

    Hot path is Python-free:
      - aggregates iterate over a C array of borrowed PyObject* pointers
      - finalize() builds the result morsel without dict/Python-list churn

    Usage:
        engine = UngroupedAggregateEngine()
        engine.add_aggregate(CountStarAggregate(b"count_star"))
        engine.add_aggregate(SumInt64Aggregate(b"col", b"sum_col"))
        engine.add_avg_finalizer(b"_avg_sum_x", b"_avg_cnt_x", b"avg_x")
        for morsel in stream:
            engine.ingest(morsel)
        result_morsel = engine.finalize()
    """

    def __cinit__(self):
        self._aggregates_pyrefs = []
        self._agg_ptrs = NULL
        self._n_aggregates = 0
        self._agg_capacity = 0

        self._avg_finalizers_pyrefs = []
        self._avg_ptrs = NULL
        self._n_avgs = 0
        self._avg_capacity = 0

        self._internal_aliases = set()

    def __dealloc__(self):
        if self._agg_ptrs is not NULL:
            free(self._agg_ptrs)
            self._agg_ptrs = NULL
        if self._avg_ptrs is not NULL:
            free(self._avg_ptrs)
            self._avg_ptrs = NULL

    cdef void _grow_agg_array(self) except *:
        cdef Py_ssize_t new_capacity = self._agg_capacity * 2 if self._agg_capacity else 4
        cdef void** new_ptrs = <void**>malloc(<size_t>new_capacity * sizeof(void*))
        if new_ptrs is NULL:
            raise MemoryError("UngroupedAggregateEngine: out of memory")
        cdef Py_ssize_t i
        for i in range(self._n_aggregates):
            new_ptrs[i] = self._agg_ptrs[i]
        if self._agg_ptrs is not NULL:
            free(self._agg_ptrs)
        self._agg_ptrs = new_ptrs
        self._agg_capacity = new_capacity

    cdef void _grow_avg_array(self) except *:
        cdef Py_ssize_t new_capacity = self._avg_capacity * 2 if self._avg_capacity else 4
        cdef void** new_ptrs = <void**>malloc(<size_t>new_capacity * sizeof(void*))
        if new_ptrs is NULL:
            raise MemoryError("UngroupedAggregateEngine: out of memory")
        cdef Py_ssize_t i
        for i in range(self._n_avgs):
            new_ptrs[i] = self._avg_ptrs[i]
        if self._avg_ptrs is not NULL:
            free(self._avg_ptrs)
        self._avg_ptrs = new_ptrs
        self._avg_capacity = new_capacity

    cpdef void add_aggregate(self, UngroupedAggregate agg):
        if self._n_aggregates >= self._agg_capacity:
            self._grow_agg_array()
        # Hold strong reference in the Python list, borrow into the C array.
        self._aggregates_pyrefs.append(agg)
        self._agg_ptrs[self._n_aggregates] = <void*>agg
        self._n_aggregates += 1

    cpdef void add_avg_finalizer(self, bytes sum_alias, bytes count_alias, object output_alias):
        """
        Register an AVG finalizer.

        The planner creates a SumXxx aggregate (alias=sum_alias) and a Count
        aggregate (alias=count_alias).  On finalize(), this engine computes
        sum / count and emits it as output_alias, suppressing the internal
        sum and count columns.
        """
        cdef bytes out_alias
        if isinstance(output_alias, bytes):
            out_alias = <bytes>output_alias
        else:
            out_alias = (<str>output_alias).encode("utf-8")

        cdef AvgFinalizer fin = AvgFinalizer(sum_alias, count_alias, out_alias)

        if self._n_avgs >= self._avg_capacity:
            self._grow_avg_array()
        self._avg_finalizers_pyrefs.append(fin)
        self._avg_ptrs[self._n_avgs] = <void*>fin
        self._n_avgs += 1
        self._internal_aliases.add(sum_alias)
        self._internal_aliases.add(count_alias)

    cpdef void ingest(self, Morsel morsel) except *:
        """Apply all aggregates to one morsel — Python-free dispatch."""
        cdef Py_ssize_t i
        cdef UngroupedAggregate agg
        for i in range(self._n_aggregates):
            agg = <UngroupedAggregate>self._agg_ptrs[i]
            agg.apply(morsel)

    cpdef bint is_mergeable(self):
        """True iff every aggregate supports partition-parallel merge
        (COUNT/SUM/MIN/MAX). COUNT DISTINCT and MEDIAN are not yet mergeable, so
        an engine containing them must stay serial. AVG is mergeable because its
        SUM and COUNT components are in _agg_ptrs and the finalizer recomputes
        from the merged columns."""
        cdef Py_ssize_t i
        for i in range(self._n_aggregates):
            if not (<UngroupedAggregate>self._agg_ptrs[i]).is_mergeable():
                return False
        return True

    cpdef void merge(self, UngroupedAggregateEngine other) except *:
        """Combine another engine's partial accumulators into this one. Both
        engines must come from the same plan over disjoint input partitions, so
        their aggregate lists line up positionally and by type. AVG finalizers
        need no merge — they recompute from the merged sum/count columns at
        finalize(). After merge, finalize() yields the exact total."""
        if self._n_aggregates != other._n_aggregates:
            raise ValueError(
                f"cannot merge engines with {self._n_aggregates} vs "
                f"{other._n_aggregates} aggregates"
            )
        if self._n_avgs != other._n_avgs:
            raise ValueError("cannot merge engines with different AVG finalizers")
        if not self.is_mergeable():
            raise NotImplementedError(
                "engine contains a non-mergeable aggregate (COUNT DISTINCT / MEDIAN)"
            )
        cdef Py_ssize_t i
        for i in range(self._n_aggregates):
            # The cast in merge_from is unchecked; verify the types match first
            # (cheap — once per aggregate, not per row) so a misconfiguration
            # fails loud instead of reading the wrong struct layout.
            if type(self._aggregates_pyrefs[i]) is not type(other._aggregates_pyrefs[i]):
                raise ValueError(
                    f"aggregate type mismatch at position {i}: "
                    f"{type(self._aggregates_pyrefs[i]).__name__} vs "
                    f"{type(other._aggregates_pyrefs[i]).__name__}"
                )
            (<UngroupedAggregate>self._agg_ptrs[i]).merge_from(
                <UngroupedAggregate>other._agg_ptrs[i]
            )

    cpdef Morsel finalize(self):
        """
        Collect results from all aggregates and return a single-row Morsel.

        AVG finalizers are computed here; their internal sum/count columns
        are excluded from the output.
        """
        cdef Py_ssize_t i
        cdef UngroupedAggregate agg
        cdef AvgFinalizer afin
        cdef set internal = self._internal_aliases
        cdef list names = []
        cdef list vectors = []
        cdef object value, s, c

        # Engine-aggregate columns (skip internal sum/count of any AVG)
        for i in range(self._n_aggregates):
            agg = <UngroupedAggregate>self._agg_ptrs[i]
            if agg.alias in internal:
                continue
            names.append(agg.alias)
            # Dispatch the builder by the aggregate's result type — the default
            # path builds INT64 and errors on a float (e.g. SUM(DECIMAL)/SUM(DOUBLE)
            # without GROUP BY, which routes through SumFloat64Aggregate).
            if agg.result_type == AGG_RESULT_F64:
                vectors.append(vector_from_sequence([agg.get_result()], dtype=_draken_native.DrakenType.FLOAT64))
            else:
                value = agg.get_result()
                # DECIMAL results must build a real DECIMAL vector — the generic int64
                # builder truncates a Decimal to int (see _decimal_result_vector).
                if type(value).__name__ == "Decimal":
                    vectors.append(_decimal_result_vector(value))
                else:
                    vectors.append(vector_from_sequence([value]))

        # AVG output columns, in registration order
        for i in range(self._n_avgs):
            afin = <AvgFinalizer>self._avg_ptrs[i]
            s = self._result_for_alias(afin.sum_alias)
            c = self._result_for_alias(afin.count_alias)
            if s is None or c is None or c == 0:
                value = None
            else:
                # s is an exact Decimal (DECIMAL columns) or a double (INTEGER/FLOAT).
                # float(s) realizes the exact sum as a double, then the division is in
                # double — AVG is DOUBLE. float(float) is identity, so non-decimal AVG
                # is unchanged.
                value = float(s) / c
            names.append(afin.output_alias)
            # AVG always produces a float result (or None). Dispatch through the
            # float64 constructor explicitly — the int64-default path errors on
            # the float value.
            vectors.append(vector_from_sequence([value], dtype=_draken_native.DrakenType.FLOAT64))

        return Morsel.from_vectors(names, vectors)

    cdef object _result_for_alias(self, bytes alias):
        cdef Py_ssize_t i
        cdef UngroupedAggregate agg
        for i in range(self._n_aggregates):
            agg = <UngroupedAggregate>self._agg_ptrs[i]
            if agg.alias == alias:
                return agg.get_result()
        return None
