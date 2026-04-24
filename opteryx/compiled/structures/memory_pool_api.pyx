# distutils: language = c++
# cython: language_level=3

"""
memory_pool_api.pyx

Cython wrapper that builds a C-compatible MemoryPoolApi struct and forwards
callbacks into the existing Cython `MemoryPool` object.

This module provides:
  - create_memory_pool_api(pool: MemoryPool) -> MemoryPoolApi*
      Allocate and populate a MemoryPoolApi struct whose `ctx` is the given
      Python MemoryPool instance. The returned pointer is owned by the caller
      and must be freed with `free_memory_pool_api`.
  - free_memory_pool_api(api: MemoryPoolApi*)
      Free the MemoryPoolApi struct and drop the Python reference to the pool.

Callback semantics:
  - All callbacks acquire the Python GIL internally (they may be called from
    native C++ threads without the GIL). Callers should expect these callbacks
    to be safe to call from native code; errors are trapped and reported via
    sentinel return values (e.g. -1 or NULL) rather than allowed to raise.
"""

from cpython.buffer cimport PyObject_GetBuffer, PyBuffer_Release, Py_buffer, PyBUF_SIMPLE
from cpython.bytes cimport PyBytes_FromStringAndSize
from cpython.ref cimport Py_INCREF, Py_DECREF
from cpython.pystate cimport PyGILState_STATE, PyGILState_Ensure, PyGILState_Release
from libc.stdlib cimport malloc, free
from libc.stdint cimport int64_t, uintptr_t, int32_t
from libc.stdint cimport uint64_t
from libc.string cimport memset
from libcpp.string cimport string

# Import the Python-level MemoryPool Cython class so we can call its methods.
# This assumes `opteryx.compiled.structures.memory_pool` is a compiled Cython module.
import opteryx.compiled.structures.memory_pool as memory_pool

# Import the C header types for MemoryPoolApi (created earlier in the tree).
cdef extern from "memory_pool_api.hpp":
    ctypedef struct MemoryPoolApi:
        void* ctx
        int64_t (*reserve_for_write_ptr)(void* ctx, int64_t size, uintptr_t* out_ptr, int64_t* out_capacity) except? -1
        int (*finalize_commit)(void* ctx, int64_t ref_id, int64_t actual_length) except? -1
        int64_t (*commit_bytes)(void* ctx, const void* data, int64_t length) except? -1
        void* (*read_latched)(void* ctx, int64_t ref_id, int64_t* out_len) except? NULL
        int (*unlatch)(void* ctx, int64_t ref_id) except? -1
        int (*release)(void* ctx, int64_t ref_id) except? -1
        int32_t version
        int32_t reserved

# Maintain a mapping of latched ref_id -> Python memoryview object to keep the
# view alive while native code may hold a raw pointer into the pool's memory.
# This prevents the Python GC from freeing wrapper objects while native code
# holds pointers protected by the pool's latch semantics.
cdef dict _latched_views = {}

#
# Helper: Safe GIL-managed call wrappers
#

cdef inline void _safe_incref_python_obj(object obj):
    # This helper must be called with the GIL held. It performs a simple
    # incref of a Python object; keep the implementation minimal and explicit.
    Py_INCREF(obj)

#
# Callback implementations
# Each callback is a plain C-callable function (C ABI) and manages the GIL
# explicitly via the PyGILState_* APIs so native callers may invoke them
# without holding the GIL.
#

cdef int64_t _reserve_for_write_ptr_cb(void* ctx, int64_t size, uintptr_t* out_ptr, int64_t* out_capacity):
    cdef PyGILState_STATE gilstate
    gilstate = PyGILState_Ensure()
    try:
        if ctx is NULL:
            return -1
        try:
            pool = <object>ctx
            # Call MemoryPool.reserve_for_write_ptr(size) -> (ref_id, ptr_int, cap)
            tup = pool.reserve_for_write_ptr(size)
            if tup is None:
                return -1
            # Ensure tuple length >= 3
            ref_id = int(tup[0])
            ptr_val = int(tup[1])
            cap = int(tup[2])
            out_ptr[0] = <uintptr_t>ptr_val
            out_capacity[0] = <int64_t>cap
            return <int64_t>ref_id
        except Exception:
            # Fail silently across C boundary - return -1
            return -1
    finally:
        PyGILState_Release(gilstate)


cdef int _finalize_commit_cb(void* ctx, int64_t ref_id, int64_t actual_length):
    cdef PyGILState_STATE gilstate
    gilstate = PyGILState_Ensure()
    try:
        if ctx is NULL:
            return -1
        try:
            pool = <object>ctx
            pool.finalize_commit(ref_id, actual_length)
            return 0
        except Exception:
            return -1
    finally:
        PyGILState_Release(gilstate)


cdef int64_t _commit_bytes_cb(void* ctx, const void* data, int64_t length):
    cdef PyGILState_STATE gilstate
    cdef object pybytes = None
    gilstate = PyGILState_Ensure()
    try:
        if ctx is NULL:
            return -1
        try:
            pool = <object>ctx
            # Create Python bytes from the raw data pointer
            if data == NULL or length <= 0:
                return -1
            pybytes = PyBytes_FromStringAndSize(<const char*>data, <Py_ssize_t>length)
            if pybytes is None:
                return -1
            try:
                ref_id = pool.commit(pybytes)
                return <int64_t>ref_id
            finally:
                # Ensure Python-owned temporary released
                Py_DECREF(pybytes)
        except Exception:
            return -1
    finally:
        PyGILState_Release(gilstate)


cdef void* _read_latched_cb(void* ctx, int64_t ref_id, int64_t* out_len):
    """
    Read a latched pointer into the pool memory.

    Returns:
      - pointer to data on success (void*), and sets *out_len
      - NULL on failure
    Side effects:
      - increases the MemoryPool latch count for ref_id (via read(..., latch=True))
      - stores the returned memoryview in _latched_views to keep it alive until
        unlatch/release is called.
    """
    # Declare all C variables at function scope to satisfy Cython
    cdef PyGILState_STATE gilstate
    cdef Py_buffer buf
    cdef object mv = None
    cdef void* ptr = NULL
    cdef int64_t length = 0

    gilstate = PyGILState_Ensure()
    try:
        if ctx is NULL:
            return NULL
        try:
            pool = <object>ctx
            # Request a zero-copy latched read: returns a memoryview (or bytes)
            mv = pool.read(ref_id, True, True)
            if mv is None:
                return NULL

            # Obtain raw pointer and length via PyObject_GetBuffer
            if PyObject_GetBuffer(mv, &buf, PyBUF_SIMPLE) != 0:
                # Could not get buffer - still keep mv latched, but fail pointer
                # The caller can still call unlatch/release; return NULL
                # Keep mv alive to preserve latch until unlatch
                _latched_views[ref_id] = mv
                Py_INCREF(mv)
                return NULL

            # Extract pointer and length
            ptr = buf.buf
            length = <int64_t>buf.len

            # Release the temporary buffer view (we still keep the memoryview object alive
            # in _latched_views to prevent object-level GC). The underlying pool memory
            # remains valid while latched; releasing the Py_buffer is safe.
            PyBuffer_Release(&buf)

            # Store the memoryview in the latched registry to keep it alive until unlatch
            _latched_views[ref_id] = mv
            Py_INCREF(mv)

            out_len[0] = length
            return ptr
        except Exception:
            return NULL
    finally:
        PyGILState_Release(gilstate)


cdef int _unlatch_cb(void* ctx, int64_t ref_id):
    """
    Unlatch a previously latched reference. This will call MemoryPool.unlatch()
    and drop the stored memoryview object (if any) so the Python GC may reclaim it.
    """
    cdef PyGILState_STATE gilstate
    gilstate = PyGILState_Ensure()
    try:
        if ctx is NULL:
            return -1
        try:
            pool = <object>ctx
            # Call underlying unlatch; if it raises, report failure
            pool.unlatch(ref_id)
        except Exception:
            # Even if unlatch failed, attempt to drop stored view to avoid leak
            try:
                obj = _latched_views.pop(ref_id, None)
                if obj is not None:
                    Py_DECREF(obj)
            except Exception:
                pass
            return -1

        # Drop stored memoryview if present
        try:
            obj = _latched_views.pop(ref_id, None)
            if obj is not None:
                Py_DECREF(obj)
        except Exception:
            # If DECREF fails, continue - we don't want to propagate exception across C boundary
            pass

        return 0
    finally:
        PyGILState_Release(gilstate)


cdef int _release_cb(void* ctx, int64_t ref_id):
    """
    Release (free) a previously committed ref_id in the pool. Also drop any stored
    memoryview object for this ref_id.
    """
    cdef PyGILState_STATE gilstate
    gilstate = PyGILState_Ensure()
    try:
        if ctx is NULL:
            return -1
        try:
            pool = <object>ctx
            pool.release(ref_id)
        except Exception:
            # Attempt to drop stored view anyway
            try:
                obj = _latched_views.pop(ref_id, None)
                if obj is not None:
                    Py_DECREF(obj)
            except Exception:
                pass
            return -1

        # Drop stored memoryview if present
        try:
            obj = _latched_views.pop(ref_id, None)
            if obj is not None:
                Py_DECREF(obj)
        except Exception:
            pass

        return 0
    finally:
        PyGILState_Release(gilstate)


#
# Factory / destructor for MemoryPoolApi
#

cdef MemoryPoolApi* _alloc_memory_pool_api() nogil:
    # allocate raw memory for the struct; zero it
    cdef MemoryPoolApi* api = <MemoryPoolApi*> malloc(sizeof(MemoryPoolApi))
    if api != NULL:
        memset(api, 0, sizeof(MemoryPoolApi))
    return api


def create_memory_pool_api(object pool):
    """
    Create and return a pointer to a MemoryPoolApi struct that forwards to the
    given Python `MemoryPool` instance.

    The returned pointer must be freed using `free_memory_pool_api`.
    """
    cdef MemoryPoolApi* api
    if pool is None:
        raise ValueError("pool must be a MemoryPool instance")

    # Plain allocation (we must acquire GIL to manipulate Python refs)
    api = _alloc_memory_pool_api()
    if api is NULL:
        raise MemoryError("Failed to allocate MemoryPoolApi")

    # Store pool as opaque ctx (PyObject*); increase refcount so it remains valid
    api.ctx = <void*>pool
    Py_INCREF(pool)

    # Wire function pointers
    api.reserve_for_write_ptr = _reserve_for_write_ptr_cb
    api.finalize_commit = _finalize_commit_cb
    api.commit_bytes = _commit_bytes_cb
    api.read_latched = _read_latched_cb
    api.unlatch = _unlatch_cb
    api.release = _release_cb

    api.version = 1
    api.reserved = 0

    return <size_t>api  # return pointer as Python-int-compatible value


def free_memory_pool_api(size_t api_ptr):
    """
    Free a MemoryPoolApi previously returned by create_memory_pool_api.

    Parameter is the integer pointer returned earlier (for portability with
    usage from pure-Python). If you used the raw pointer, you can pass it
    unchanged.
    """
    cdef MemoryPoolApi* api = <MemoryPoolApi*>api_ptr
    cdef PyGILState_STATE gilstate
    if api is NULL:
        return

    # Drop the Python reference to ctx
    try:
        if api.ctx != NULL:
            gilstate = PyGILState_Ensure()
            try:
                pool_obj = <object>api.ctx
                try:
                    Py_DECREF(pool_obj)
                except Exception:
                    # ignore
                    pass
            finally:
                PyGILState_Release(gilstate)
    except Exception:
        # Be conservative: continue cleanup
        pass

    # Clear any outstanding latched views (best-effort)
    try:
        # Acquire GIL to safely DECREF python objects
        gilstate = PyGILState_Ensure()
        try:
            for ref_id, mv in list(_latched_views.items()):
                try:
                    Py_DECREF(mv)
                except Exception:
                    pass
                _latched_views.pop(ref_id, None)
        finally:
            PyGILState_Release(gilstate)
    except Exception:
        pass

    free(api)
