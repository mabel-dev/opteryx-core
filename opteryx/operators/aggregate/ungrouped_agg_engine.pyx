# included by ungrouped_agg.pyx — do not compile standalone


cdef class UngroupedAggregateEngine:
    """
    Drives ungrouped (global) aggregation over a stream of Draken morsels.

    Usage:
        engine = UngroupedAggregateEngine()
        engine.add_aggregate(CountStarAggregate(b"count_star"))
        engine.add_aggregate(SumInt64Aggregate(b"col", b"sum_col"))
        engine.add_avg_finalizer(b"_avg_sum_x", b"_avg_cnt_x", b"avg_x")
        for morsel in stream:
            engine.ingest(morsel)
        result_morsel = engine.finalize()
    """
    cdef list _aggregates
    cdef list _avg_finalizers
    cdef set  _internal_aliases

    def __cinit__(self):
        self._aggregates = []
        self._avg_finalizers = []
        self._internal_aliases = set()

    cpdef void add_aggregate(self, UngroupedAggregate agg):
        self._aggregates.append(agg)

    cpdef void add_avg_finalizer(self, bytes sum_alias, bytes count_alias, object output_alias):
        """
        Register an AVG finalizer.

        The planner creates a SumXxx aggregate (alias=sum_alias) and a Count
        aggregate (alias=count_alias).  On finalize(), this engine computes
        sum / count and emits it as output_alias, suppressing the internal
        sum and count columns.
        """
        self._avg_finalizers.append((sum_alias, count_alias, output_alias))
        self._internal_aliases.add(sum_alias)
        self._internal_aliases.add(count_alias)

    cpdef void ingest(self, Morsel morsel) except *:
        """Apply all aggregates to one morsel."""
        cdef UngroupedAggregate agg
        for agg in self._aggregates:
            agg.apply(morsel)

    cpdef Morsel finalize(self):
        """
        Collect results from all aggregates and return a single-row Morsel.

        AVG finalizers are computed here; their internal sum/count columns
        are excluded from the output.
        """
        # Build alias → result dict
        cdef dict results = {}
        cdef UngroupedAggregate agg

        for agg in self._aggregates:
            results[agg.alias] = agg.get_result()

        # Compute AVG finalizers
        for (sum_alias, count_alias, output_alias) in self._avg_finalizers:
            s = results.get(sum_alias)
            c = results.get(count_alias)
            if s is None or c is None or c == 0:
                results[output_alias] = None
            else:
                results[output_alias] = s / c

        # Build output lists, skipping internal aliases
        names = []
        vectors = []
        for agg in self._aggregates:
            if agg.alias not in self._internal_aliases:
                names.append(agg.alias)
                vectors.append(vector_from_sequence([results[agg.alias]]))

        # Append AVG output columns (in the order finalizers were added)
        for (sum_alias, count_alias, output_alias) in self._avg_finalizers:
            names.append(output_alias if isinstance(output_alias, (bytes, str)) else output_alias)
            vectors.append(vector_from_sequence([results[output_alias]]))

        return Morsel.from_vectors(names, vectors)
