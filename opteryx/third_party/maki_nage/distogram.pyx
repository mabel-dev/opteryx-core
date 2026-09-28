# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: wraparound=False
# cython: boundscheck=False
# distutils: language = c++

"""
Distogram - a compressed, streaming histogram.

Originally distogram 3.0.0 by Romain Picard (romain.picard@oakbits.com, MIT).
Opteryx's port lives in _distogram.hpp; this module is its Python face. The
changes made for Opteryx: difference weighting removed, the implementation
moved to native code, and the unused statistics API (quantile, mean,
variance, bounds, histogram, bulkload) dropped.
"""

from libc.stdint cimport int64_t
from libcpp.memory cimport make_shared
from libcpp.memory cimport shared_ptr

BIN_COUNT: int = 50


cdef class Distogram:
    """Compressed representation of a distribution."""

    def __init__(self, int64_t bin_count=BIN_COUNT):
        self.core = make_shared[DistogramCore](bin_count)

    def count(self):
        """Count total elements in distribution."""
        return self.core.get().count()

    @property
    def min(self):
        return self.core.get().min()

    @property
    def max(self):
        return self.core.get().max()

    @property
    def max_bin_count(self):
        return self.core.get().max_bin_count()

    @property
    def bin_count(self):
        return self.core.get().bin_count()

    @property
    def bins(self):
        cdef const DistogramBin* b = self.core.get().bins().data()
        return [(b[i].value, b[i].count) for i in range(self.core.get().bin_count())]


cdef Distogram wrap_distogram(shared_ptr[DistogramCore] core):
    cdef Distogram d = Distogram.__new__(Distogram)
    d.core = core
    return d


def load_counts_i64(const int64_t[::1] counts, double minimum, double maximum):
    """Load an equi-width histogram from contiguous native int64 counts."""
    cdef const int64_t* data = &counts[0] if counts.shape[0] > 0 else NULL
    return wrap_distogram(
        make_shared[DistogramCore](DistogramCore.from_counts(data, counts.shape[0], minimum, maximum))
    )


def update(Distogram h not None, double value, int64_t count=1):
    """Add a value to the distribution."""
    h.core.get().update(value, count)
    return h


def merge(Distogram h1 not None, Distogram h2 not None):
    """Fold h2's bins into h1."""
    h1.core.get().merge(h2.core.get()[0])
    return h1


def count_up_to(Distogram h not None, double value):
    """Count elements up to a given value."""
    return h.core.get().count_up_to(value)
