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
            vectors.append(vector_from_sequence([agg.get_result()]))

        # AVG output columns, in registration order
        for i in range(self._n_avgs):
            afin = <AvgFinalizer>self._avg_ptrs[i]
            s = self._result_for_alias(afin.sum_alias)
            c = self._result_for_alias(afin.count_alias)
            if s is None or c is None or c == 0:
                value = None
            else:
                value = s / c
            names.append(afin.output_alias)
            vectors.append(vector_from_sequence([value]))

        return Morsel.from_vectors(names, vectors)

    cdef object _result_for_alias(self, bytes alias):
        cdef Py_ssize_t i
        cdef UngroupedAggregate agg
        for i in range(self._n_aggregates):
            agg = <UngroupedAggregate>self._agg_ptrs[i]
            if agg.alias == alias:
                return agg.get_result()
        return None
