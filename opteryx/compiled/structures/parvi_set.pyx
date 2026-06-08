# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

"""
opteryx/compiled/structures/parvi_set.pyx

Owning handle for opteryx::parvi::ParviSet — a fixed-capacity 16-slot inline
hash set optimized for small distinct/COUNT(DISTINCT) workloads.

Design notes
------------
- ParviSet is stack-allocated inline: no heap allocation overhead.
- Single SIMD-group probe: entire table is one 16-byte control group.
- Overflow handling: insert_or_ignore() returns {is_new: false} when full.
- Promotion: drain_into(CarcharSet) copies live entries for seamless migration.

This wrapper exists only to own the ParviSet* (construct/destruct) and expose
a Python surface for tests and promotion. The DISTINCT/GROUP BY hot path calls
ParviSet::mark_new_indices directly on the _ptr under nogil — see distinct.pyx.
No Cython method wraps the hot path.
"""

from libc.stdint cimport uint64_t
from libc.stddef cimport size_t
from opteryx.compiled.structures.carchar_set cimport CarcharSetWrapper

# ParviSet and CarcharSet are declared in parvi_set.pxd


# Owning handle for DISTINCT/COUNT(DISTINCT)
cdef class ParviSetWrapper:
    """
    Owning handle for ParviSet.

    Owns the ParviSet* and exposes a Python surface for tests and promotion.
    The DISTINCT hot path calls ParviSet::mark_new_indices directly on _ptr
    under nogil; no method here wraps it.
    """

    def __cinit__(self):
        self._ptr = new ParviSet()

    def __dealloc__(self):
        if self._ptr is not NULL:
            del self._ptr
            self._ptr = NULL

    def __len__(self):
        return <Py_ssize_t>self._ptr.size()

    def __repr__(self):
        return f"ParviSetWrapper(size={self._ptr.size()}, full={self._ptr.full()})"

    # cpdef methods (callable from Python)
    cpdef size_t size(self):
        """Return the number of entries currently in the set."""
        return self._ptr.size()

    cpdef bint full(self):
        """Return True if the set is at capacity (16 entries)."""
        return self._ptr.full()

    cpdef bint contains(self, uint64_t key):
        """Check if a key is present."""
        return self._ptr.contains(key)

    cpdef bint insert(self, uint64_t key):
        """Insert a key. Returns True if newly inserted."""
        cdef ParviSetResult result = self._ptr.insert_or_ignore(key)
        return result.is_new

    cpdef void drain_into_carchar(self, CarcharSetWrapper target):
        """Drain all current Parvi keys into the provided Carchar set."""
        if self._ptr is NULL or target is None or target._ptr is NULL:
            return
        self._ptr.drain_into(target._ptr[0])

    cpdef void clear(self):
        """Clear the set."""
        self._ptr.clear()
