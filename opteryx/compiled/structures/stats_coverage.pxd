# cython: language_level=3
"""
One scan's statistics-coverage request (docs/MANIFEST_SUM_STATISTIC_DESIGN.md
§7, P3/P4) as the Cython glue sees it: src/cpp/engine/stats_coverage_request.hpp,
plus the two conversions every format's glue needs - filling the request from the
compiler's ``(terms, aggs, keys)`` and reading the folded seed back as Python
values. Shared by pool_reader.pyx (parquet) and _operators.pyx (skene), so the
build and the read-back exist once.
"""

from libc.stdint cimport int64_t, uint64_t
from libcpp cimport bool as cbool
from libcpp.string cimport string
from libcpp.vector cimport vector


cdef extern from "engine/stats_coverage_request.hpp" namespace "opteryx::engine":
    cdef cppclass CoveragePartial:
        int64_t rows
        int64_t valid
        cbool any_extreme
        int64_t min_value "min"
        int64_t max_value "max"

    cdef cppclass CoverageSpec:
        pass

    cdef cppclass CoverageAccumulator:
        pass

    cdef cppclass CoverageRequest:
        CoverageSpec spec
        CoverageAccumulator acc
        int64_t covered
        int64_t disjoint
        string err
        size_t name(const string& name) except +
        void term(size_t column, int op, const vector[int64_t]& ordinals) except +
        void agg(int need, size_t column) except +
        void key(size_t column) except +
        const CoveragePartial& partial(size_t k) except +
        size_t group_count() except +
        cbool group_key_null(size_t g, size_t k) except +
        int64_t group_key_value(size_t g, size_t k) except +
        const CoveragePartial& group_partial(size_t g, size_t k) except +

    int64_t coverage_sum_hi(const CoveragePartial& p)
    uint64_t coverage_sum_lo(const CoveragePartial& p)


cdef inline void coverage_fill(CoverageRequest* request, object coverage) except *:
    """Fill ``request`` from the compiler's ``coverage`` = (terms, aggs, keys):
    terms: [(physical column name, op, [ordinals])] - coverage_terms.py;
    aggs:  [(need, physical column name or None)] - stats_coverage.hpp CoverageNeed;
    keys:  [physical column name] - the GROUP BY keys, empty when ungrouped."""
    terms, aggs, keys = coverage
    cdef vector[int64_t] ordinals
    for name, op, values in terms:
        ordinals.clear()
        for value in values:
            ordinals.push_back(<int64_t?>value)
        request.term(request.name(name.encode("utf-8")), <int?>op, ordinals)
    for need, name in aggs:
        request.agg(<int?>need, 0 if name is None else request.name(name.encode("utf-8")))
    for name in keys:
        request.key(request.name(name.encode("utf-8")))


cdef inline tuple coverage_partial(const CoveragePartial& p):
    """(rows, valid, sum, any_extreme, min, max) - the seed entry of one aggregate."""
    return (
        p.rows,
        p.valid,
        ((<object>coverage_sum_hi(p)) << 64) + (<object>coverage_sum_lo(p)),
        p.any_extreme,
        p.min_value,
        p.max_value,
    )


cdef inline object coverage_seed(CoverageRequest* request, size_t n_aggs, size_t n_keys):
    """The folded seed: ungrouped, one partial tuple per aggregate; grouped, a list
    of (key tuple - None for a NULL key - , [partial tuple per aggregate]).
    Plain loops: a closure (generator expression) cannot live in a .pxd inline."""
    cdef size_t g, k
    cdef list partials
    cdef list key
    if n_keys == 0:
        partials = []
        for k in range(n_aggs):
            partials.append(coverage_partial(request.partial(k)))
        return partials
    groups = []
    for g in range(request.group_count()):
        key = []
        for k in range(n_keys):
            key.append(None if request.group_key_null(g, k) else request.group_key_value(g, k))
        partials = []
        for k in range(n_aggs):
            partials.append(coverage_partial(request.group_partial(g, k)))
        groups.append((tuple(key), partials))
    return groups
