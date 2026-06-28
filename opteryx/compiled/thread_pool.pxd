# cython: language_level=3
"""Shared cdef interface for CppThreadPool — lets native execution code
(``_operators.pyx``) submit NATIVE tasks (C function pointer + opaque arg) to the
pool with no Python callable, no Future, and drive the worker fan-out from Cython.

``native_task_fn`` is the worker signature: a ``noexcept nogil`` C function taking
one ``void*`` arg (a per-worker struct). The worker body re-acquires the GIL
(``with gil``) only for the PyObject work it needs; in free-threaded 3.14t that is
uncontended."""

from cpython.ref cimport PyObject
from libcpp.string cimport string

ctypedef void (*native_task_fn)(void*) noexcept nogil


cdef extern from "bs_pool_bridge.hpp":
    cdef cppclass BSThreadPoolBridge:
        BSThreadPoolBridge(int max_workers, const string& name) except +
        PyObject* submit(PyObject* callable, PyObject* args, PyObject* kwargs)
        void submit_native(native_task_fn fn, void* arg) nogil
        void wait_native() nogil
        void shutdown(bint wait) nogil
        int max_workers()


cdef class CppThreadPool:
    cdef BSThreadPoolBridge* _pool
    cdef str _name
    cdef int _max_workers
    cdef bint _shut_down

    cdef void submit_native(self, native_task_fn fn, void* arg) noexcept nogil
    cdef void wait_native(self) noexcept nogil
