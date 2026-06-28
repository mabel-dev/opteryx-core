# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""Cython wrapper for MorselQueue (src/cpp/morsel_queue.hpp).

Slice 1 of the native execution scheduler rewrite. The queue carries the C++
substrate carrier `shared_ptr[CxxMorsel]` — no PyObject ever sits on it. The
`put`/`get`/`close` C++ methods are `nogil`; this turn exposes a Python-callable
edge (`PyMorselQueue`) for isolation testing — the scheduler will later call the
`cdef` surface directly, also `nogil`, with no shim materialisation.
"""

from libcpp cimport bool as cbool
from libcpp.memory cimport shared_ptr
from libc.stddef cimport size_t

from draken.morsels.cxx_morsel cimport CxxMorsel
from draken.morsels.morsel cimport Morsel, morsel_to_cxx, cxx_to_morsel


cdef extern from "morsel_queue.hpp" nogil:
    cdef cppclass MorselQueue:
        MorselQueue(size_t capacity) except +
        cbool put(shared_ptr[CxxMorsel] m)
        cbool get(shared_ptr[CxxMorsel]& out)
        void close()
        cbool closed()
        size_t capacity()
        size_t size_approx()


# Returned by PyMorselQueue.get() when the producer has gracefully FINISHED (all
# data drained, no more coming) — distinct from None, which means the consumer
# ABANDONED the queue (close() dropped the remainder). The in-band sentinel is a
# null shared_ptr, enqueued after the last data morsel, so it can never overtake or
# drop real data the way close() does.
MQ_FINISHED = object()


cdef class PyMorselQueue:
    """Python-callable edge over MorselQueue — TEST/boundary use only.

    The engine drives the `cdef` surface (`_put_cxx`/`_get_cxx`) directly with the
    GIL released; this class adds the Morsel↔CxxMorsel conversion only so tests can
    round-trip draken Morsels through the native queue.
    """

    cdef MorselQueue* _q

    def __cinit__(self, size_t capacity=8):
        self._q = new MorselQueue(capacity)

    def __dealloc__(self):
        if self._q != NULL:
            self._q.close()
            del self._q
            self._q = NULL

    # ---- cdef surface the scheduler will use (no Python, GIL-released) ----------

    cdef cbool _put_cxx(self, shared_ptr[CxxMorsel] m) noexcept nogil:
        return self._q.put(m)

    cdef cbool _get_cxx(self, shared_ptr[CxxMorsel]& out) noexcept nogil:
        return self._q.get(out)

    cdef cbool _finish_cxx(self) noexcept nogil:
        # Graceful end-of-data: enqueue a null shared_ptr sentinel AFTER the last
        # data morsel. Unlike close(), this drops nothing — the consumer drains all
        # real morsels, then dequeues the sentinel and stops. Single producer only
        # (the terminal drive), so the sentinel is strictly last in its stream.
        cdef shared_ptr[CxxMorsel] sentinel  # default-constructed: NULL
        return self._q.put(sentinel)

    # ---- Python test edge -------------------------------------------------------

    def put(self, Morsel m):
        """Enqueue one Morsel. Returns False if the queue is closed."""
        cdef shared_ptr[CxxMorsel] cxm = morsel_to_cxx(m)
        cdef cbool ok
        with nogil:
            ok = self._q.put(cxm)
        return bool(ok)

    def finish(self):
        """Signal graceful end-of-data (producer side). Returns False if the queue
        was abandoned (closed) first."""
        cdef cbool ok
        with nogil:
            ok = self._finish_cxx()
        return bool(ok)

    def get(self):
        """Dequeue one item: a Morsel (data), `MQ_FINISHED` (producer finished, all
        data drained), or None (consumer abandoned via close(), remainder dropped)."""
        cdef shared_ptr[CxxMorsel] out
        cdef cbool ok
        with nogil:
            ok = self._q.get(out)
        if not ok:
            return None                  # abandoned + drained
        if out.get() == NULL:
            return MQ_FINISHED           # graceful finish sentinel
        return cxx_to_morsel(out)

    def close(self):
        with nogil:
            self._q.close()

    @property
    def is_closed(self):
        return bool(self._q.closed())

    @property
    def capacity(self):
        return self._q.capacity()

    def __len__(self):
        return self._q.size_approx()
