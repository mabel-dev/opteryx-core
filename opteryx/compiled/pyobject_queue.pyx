# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True

"""Lock-free SPSC queue for Python objects backed by moodycamel::ReaderWriterQueue.

Replaces queue.Queue for single-producer / single-consumer intra-process use.
No mutex, no condition variable — try_put / try_get are non-blocking.

Usage:
    q = MoodycamelQueue(maxsize=256)

    # Producer
    if not q.try_put(item):
        # full — use put(item, timeout=...) to spin

    # Consumer
    item = q.try_get()      # None if empty
    item = q.get(timeout=0.001)  # spin with timeout
"""

import time
from cpython.ref cimport PyObject, Py_XDECREF


cdef extern from "pyobject_queue.hpp":
    cdef cppclass PyObjectQueue:
        PyObjectQueue(size_t capacity) except +
        bint try_enqueue(object item)
        PyObject* try_dequeue()
        size_t size_approx()
        size_t capacity()


cdef class MoodycamelQueue:
    """Lock-free SPSC queue for Python objects.

    Mimics the subset of queue.Queue used by pool_reader:
        put(item, block, timeout)  — enqueue with optional spin
        get(block, timeout)        — dequeue with optional spin
        qsize()                    — approximate size
        empty()                    — True if empty
        full()                     — True if at capacity

    Not safe for multiple producers or multiple consumers simultaneously.
    """

    cdef PyObjectQueue* _q
    cdef int _maxsize

    def __cinit__(self, int maxsize=256):
        if maxsize <= 0:
            maxsize = 256
        self._maxsize = maxsize
        self._q = new PyObjectQueue(<size_t>maxsize)

    def __dealloc__(self):
        if self._q != NULL:
            del self._q
            self._q = NULL

    def try_put(self, object item):
        """Non-blocking enqueue. Returns True on success, False if full."""
        return self._q.try_enqueue(item)

    def try_get(self):
        """Non-blocking dequeue. Returns item or None if empty."""
        cdef PyObject* raw = self._q.try_dequeue()
        if raw == NULL:
            return None
        # try_dequeue transferred ownership; convert to Python object
        # and release the extra refcount that was held by the queue.
        result = <object>raw
        Py_XDECREF(raw)
        return result

    def put(self, object item, bint block=True, object timeout=None):
        """Enqueue item. Spins if block=True until space or timeout expires."""
        if self._q.try_enqueue(item):
            return
        if not block:
            import queue as _q
            raise _q.Full
        cdef double deadline = -1.0
        if timeout is not None:
            deadline = time.monotonic() + <double>timeout
        while True:
            if self._q.try_enqueue(item):
                return
            if deadline >= 0.0 and time.monotonic() > deadline:
                import queue as _q
                raise _q.Full
            time.sleep(0.0001)

    def get(self, bint block=True, object timeout=None):
        """Dequeue item. Spins if block=True until item arrives or timeout expires."""
        cdef PyObject* raw = self._q.try_dequeue()
        if raw != NULL:
            result = <object>raw
            Py_XDECREF(raw)
            return result
        if not block:
            import queue as _q
            raise _q.Empty
        cdef double deadline = -1.0
        if timeout is not None:
            deadline = time.monotonic() + <double>timeout
        while True:
            raw = self._q.try_dequeue()
            if raw != NULL:
                result = <object>raw
                Py_XDECREF(raw)
                return result
            if deadline >= 0.0 and time.monotonic() > deadline:
                import queue as _q
                raise _q.Empty
            time.sleep(0.0001)

    def put_nowait(self, object item):
        """Non-blocking enqueue; raises queue.Full if no space."""
        if not self._q.try_enqueue(item):
            import queue as _q
            raise _q.Full

    def get_nowait(self):
        """Non-blocking dequeue; raises queue.Empty if no items."""
        cdef PyObject* raw = self._q.try_dequeue()
        if raw == NULL:
            import queue as _q
            raise _q.Empty
        result = <object>raw
        Py_XDECREF(raw)
        return result

    def task_done(self):
        """No-op: compatibility shim for queue.Queue callers."""
        pass

    def join(self):
        """No-op: compatibility shim."""
        pass

    def qsize(self):
        return <int>self._q.size_approx()

    def empty(self):
        return self._q.size_approx() == 0

    def full(self):
        return self._q.size_approx() >= <size_t>self._maxsize
