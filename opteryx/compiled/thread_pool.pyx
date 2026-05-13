# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

"""Cython wrapper for BSThreadPoolBridge (BS::thread_pool, C++17).

CppThreadPool is a typed cdef class — no `object` members.  The executor is
held as a raw C++ pointer; Python callables are dispatched through
BSThreadPoolBridge::submit() which returns a concurrent.futures.Future.
"""

from cpython.ref cimport PyObject
from libcpp.string cimport string

# Py_DECREF from cpython.ref takes `object`, not `PyObject*`.
# Declare a raw alias so we can balance new-reference returns from C++.
cdef extern from "Python.h":
    void _raw_decref "Py_DECREF"(PyObject* op)


cdef extern from "bs_pool_bridge.hpp":
    cdef cppclass BSThreadPoolBridge:
        BSThreadPoolBridge(int max_workers, const string& name) except +
        PyObject* submit(PyObject* callable, PyObject* args, PyObject* kwargs)
        void shutdown(bint wait) nogil
        int max_workers()


cdef class CppThreadPool:
    cdef BSThreadPoolBridge* _pool
    cdef str _name
    cdef int _max_workers
    cdef bint _shut_down

    def __cinit__(self, int max_workers, str name="cpp-pool"):
        cdef bytes _name_bytes = name.encode("utf-8")
        cdef string _cname = _name_bytes
        self._name = name
        self._max_workers = max_workers
        self._shut_down = False
        self._pool = new BSThreadPoolBridge(max_workers, _cname)

    def __dealloc__(self):
        if self._pool != NULL:
            del self._pool
            self._pool = NULL

    def submit(self, fn, *args, **kwargs):
        cdef PyObject* fut_ptr
        if self._shut_down:
            raise RuntimeError("Thread pool has been shut down")
        fut_ptr = self._pool.submit(
            <PyObject*>fn,
            <PyObject*>args,
            <PyObject*>kwargs if kwargs else NULL,
        )
        if fut_ptr == NULL:
            raise RuntimeError("BS::thread_pool submit failed")
        # fut_ptr is a new reference from C++.
        # <object> cast increments refcount; _raw_decref balances the C++ new ref.
        cdef object fut = <object>fut_ptr
        _raw_decref(fut_ptr)
        return fut

    def shutdown(self, bint wait=True):
        if self._pool != NULL and not self._shut_down:
            self._shut_down = True
            with nogil:
                self._pool.shutdown(wait)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown(wait=True)
        return False

    @property
    def name(self) -> str:
        return self._name

    @property
    def max_workers(self) -> int:
        return self._max_workers
