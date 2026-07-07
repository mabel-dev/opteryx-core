# cython: language_level=3
"""Shared cdef interface for PyMorselQueue/MorselQueue — lets native execution code
(e.g. the C++ morsel-driven engine bridges) cimport the raw C++ queue pointer and call
into it directly (the real, production output edge), instead of only the Python-callable
``put``/``get`` wrappers. Mirrors ``thread_pool.pxd``'s split (extern + cdef class decl
in the .pxd, method bodies in the .pyx)."""

from libcpp cimport bool as cbool
from libcpp.memory cimport shared_ptr

from draken.morsels.cxx_morsel cimport CxxMorsel


cdef extern from "morsel_queue.hpp" nogil:
    cdef enum class MorselQueueStatus "MorselQueue::Status":
        DATA
        FINISHED
        ABANDONED

    cdef cppclass MorselQueue:
        MorselQueue(size_t capacity) except +
        cbool put(shared_ptr[CxxMorsel] m)
        MorselQueueStatus get(shared_ptr[CxxMorsel]& out)
        void finish()
        void wait_finished()
        void close()
        cbool closed()
        size_t capacity()
        size_t size_approx()


cdef class PyMorselQueue:
    cdef MorselQueue* _q

    cdef cbool _put_cxx(self, shared_ptr[CxxMorsel] m) noexcept nogil
    cdef MorselQueueStatus _get_cxx(self, shared_ptr[CxxMorsel]& out) noexcept nogil
    cdef void _finish_cxx(self) noexcept nogil
