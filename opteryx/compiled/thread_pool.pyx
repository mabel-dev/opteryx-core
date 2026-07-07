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
from libcpp.memory cimport shared_ptr

# Py_DECREF from cpython.ref takes `object`, not `PyObject*`.
# Declare a raw alias so we can balance new-reference returns from C++.
cdef extern from "Python.h":
    void _raw_decref "Py_DECREF"(PyObject* op)


# BSThreadPoolBridge + the CppThreadPool cdef class (attributes + native method
# signatures) are declared in thread_pool.pxd so native execution code can cimport
# and submit native tasks. This module supplies the method bodies.
cdef class CppThreadPool:
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

    cdef void submit_native(self, native_task_fn fn, void* arg) noexcept nogil:
        """Submit a NATIVE worker task (no Python callable, no Future). Cython-only
        seam for the native worker-drive — the caller submits N tasks then barriers
        with ``wait_native``. No GIL is taken by the dispatch itself."""
        self._pool.submit_native(fn, arg)

    cdef void wait_native(self) noexcept nogil:
        """Block until all native tasks submitted so far complete, WITHOUT tearing
        the pool down (reusable for a second native fan-out)."""
        self._pool.wait_native()

    cdef shared_ptr[PriorityPool] pool_handle(self) noexcept nogil:
        """Gap #3 Phase 2b: hand out the underlying priority pool so a parquet
        scan can share it instead of constructing its own decode pool (see
        ParquetIOPipeline's injecting constructor, rugo/src/parquet/io_pipeline.hpp).
        Caller must not outlive this pool's teardown (see submit_native's note)."""
        return self._pool.pool_handle()

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
