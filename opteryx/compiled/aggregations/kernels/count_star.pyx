# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: cdivision=True
# cython: infer_types=True

from libc.stdint cimport int64_t
from opteryx.compiled.aggregations.aggregations_state_classes cimport PerAggregateCountState


cdef void count_star_accumulate(
    int64_t* counts,
    const int64_t* state_indices,
    Py_ssize_t row_count,
) noexcept nogil:
    """
    Increment counts[state_indices[i]] for every i in [0, row_count).

    Called by single-aggregate COUNT(*) ingest paths after group-key state
    indices have been resolved.  Encoding type (plain / dict-encoded /
    constant) is irrelevant — COUNT(*) has no value column and is therefore
    key-agnostic.
    """
    cdef Py_ssize_t i
    for i in range(row_count):
        counts[state_indices[i]] += 1


cdef void count_star_multi_accumulate(
    int64_t* multi_counts,
    const int64_t* state_indices,
    Py_ssize_t row_count,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
) noexcept nogil:
    """
    Increment multi_counts at offset = state_indices[i] * multi_agg_count + agg_idx
    for every i in [0, row_count).

    Called by multi-aggregate inner loops when agg_mode == AGG_COUNT_STAR.
    The offset formula mirrors CarcharGroupStateEngine._multi_offset.
    """
    cdef Py_ssize_t i
    cdef Py_ssize_t offset
    for i in range(row_count):
        offset = state_indices[i] * multi_agg_count + agg_idx
        multi_counts[offset] += 1


cdef void count_star_multi_accumulate_per_aggregate(
    object state_obj,
    const int64_t* state_indices,
    Py_ssize_t row_count,
) noexcept:
    """
    Increment counts in PerAggregateCountState for every group index.

    For each row i in [0, row_count):
      counts[state_indices[i]] += 1

    Called by multi-aggregate inner loops with per-aggregate state objects.
    No offset math needed — direct indexing by state_index.
    """
    cdef Py_ssize_t i
    cdef int64_t* counts = (<PerAggregateCountState>state_obj).counts.data()
    for i in range(row_count):
        counts[state_indices[i]] += 1
