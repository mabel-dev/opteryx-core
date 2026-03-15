# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

import pyarrow


cdef extern from "tdigest.h":
    ctypedef struct td_histogram_t:
        pass

    td_histogram_t *td_new(double compression)
    void td_free(td_histogram_t *h)
    int td_add(td_histogram_t *h, double val, long long weight)
    int td_merge(td_histogram_t *h, td_histogram_t *other)
    double td_quantile(td_histogram_t *h, double q)
    long long td_size(td_histogram_t *h)


cdef class ApproximatePercentileState:
    cdef td_histogram_t* _hist
    cdef double _compression
    cdef double _percentile

    def __cinit__(self, double percentile=0.5, double compression=100.0):
        if percentile < 0.0 or percentile > 1.0:
            raise ValueError("percentile must be between 0.0 and 1.0")
        self._percentile = percentile
        self._compression = compression
        self._hist = td_new(compression)
        if self._hist == NULL:
            raise MemoryError("Failed to allocate t-digest histogram")

    def __dealloc__(self):
        if self._hist != NULL:
            td_free(self._hist)
            self._hist = NULL

    cpdef void add_value(self, object value):
        if value is None:
            return
        td_add(self._hist, float(value), 1)

    cpdef void add_repeated_value(self, object value, long long count):
        if value is None or count <= 0:
            return
        td_add(self._hist, float(value), count)

    cpdef void update_arrow(self, object column):
        cdef list chunks
        cdef object chunk
        cdef object value

        if column is None:
            return

        if isinstance(column, pyarrow.ChunkedArray):
            chunks = column.chunks
        else:
            chunks = [column]

        for chunk in chunks:
            for value in chunk.to_pylist():
                if value is not None:
                    td_add(self._hist, float(value), 1)

    cpdef void update_draken(self, object vector):
        cdef object value
        if vector is None:
            return
        for value in vector.to_pylist():
            if value is not None:
                td_add(self._hist, float(value), 1)

    cpdef void merge(self, ApproximatePercentileState other):
        td_merge(self._hist, other._hist)

    cpdef object quantile(self, object percentile=None):
        cdef double q
        if td_size(self._hist) == 0:
            return None
        q = self._percentile if percentile is None else float(percentile)
        if q < 0.0 or q > 1.0:
            raise ValueError("percentile must be between 0.0 and 1.0")
        return td_quantile(self._hist, q)

    cpdef object median(self):
        return self.quantile(0.5)


ApproximateMedianState = ApproximatePercentileState


cpdef object approximate_percentile(object column, object sketch, double percentile=0.5):
    if sketch is None:
        sketch = ApproximatePercentileState(percentile)
    sketch.update_arrow(column)
    return sketch


cpdef object approximate_percentile_draken(object column, object sketch, double percentile=0.5):
    if sketch is None:
        sketch = ApproximatePercentileState(percentile)
    sketch.update_draken(column)
    return sketch


cpdef object approximate_median(object column, object sketch):
    return approximate_percentile(column, sketch, 0.5)


cpdef object approximate_median_draken(object column, object sketch):
    return approximate_percentile_draken(column, sketch, 0.5)
