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
from libc.stdio cimport fprintf, fflush, stderr

from draken.morsels.cxx_morsel cimport CxxMorsel
from draken.morsels.morsel cimport Morsel, morsel_to_cxx, cxx_to_morsel

import os as _os

# Crash-bisect breadcrumbs. When OPTERYX_QUEUE_TRACE is set, get() writes an
# unbuffered, immediately-flushed line to stderr around each of its two distinct
# failure surfaces — the native moodycamel dequeue and the CxxMorsel->Morsel
# materialize — so that on a SIGSEGV the LAST line printed names which one was
# executing. fprintf+fflush to the C-level stderr survives a segfault (no Python
# buffering, no async writer thread) where the OPTERYX_TRACE recorder's in-memory
# list would be lost. Off (zero overhead past the branch) unless explicitly armed.
cdef int _QTRACE = 1 if _os.environ.get("OPTERYX_QUEUE_TRACE") else 0


# Returned by PyMorselQueue.get() when the producer has gracefully FINISHED (all
# data drained, no more coming) — distinct from None, which means the consumer
# ABANDONED the queue (close() dropped the remainder). Reported via an out-of-band
# atomic counter on the C++ side (MorselQueue::finish()/Status::FINISHED), not an
# in-band sentinel — see src/cpp/morsel_queue.hpp's class doc for why the old
# null-shared_ptr sentinel could be dequeued ahead of real data still sitting in a
# different producer thread's sub-queue (a genuine, reproduced production bug).
MQ_FINISHED = object()


cdef class PyMorselQueue:
    """Python-callable edge over MorselQueue — TEST/boundary use only.

    The engine drives the `cdef` surface (`_put_cxx`/`_get_cxx`) directly with the
    GIL released; this class adds the Morsel↔CxxMorsel conversion only so tests can
    round-trip draken Morsels through the native queue.
    """

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

    cdef MorselQueueStatus _get_cxx(self, shared_ptr[CxxMorsel]& out) noexcept nogil:
        return self._q.get(out)

    cdef void _finish_cxx(self) noexcept nogil:
        # Graceful end-of-data: out-of-band atomic signal (MorselQueue::finish()),
        # not an in-band sentinel — safe to call once a producer's writes are
        # provably complete, regardless of which thread wrote the data vs which
        # thread calls finish(). See morsel_queue.hpp's class doc.
        self._q.finish()

    # ---- Python test edge -------------------------------------------------------

    def put(self, Morsel m):
        """Enqueue one Morsel. Returns False if the queue is closed."""
        cdef shared_ptr[CxxMorsel] cxm = morsel_to_cxx(m)
        cdef cbool ok
        with nogil:
            ok = self._q.put(cxm)
        return bool(ok)

    def finish(self):
        """Signal graceful end-of-data (producer side). Returns True (the signal is
        unconditional — unlike put(), finish() cannot be refused)."""
        with nogil:
            self._q.finish()
        return True

    def wait_finished(self):
        """Block (GIL released) until the producer has signalled finish() at least
        once — the consumer's teardown gate on the early-abandon path, AFTER close().
        No-op if finish() already happened. On the normal path the consumer observes
        FINISHED from get() instead and never calls this."""
        with nogil:
            self._q.wait_finished()

    def get(self):
        """Dequeue one item: a Morsel (data), `MQ_FINISHED` (producer finished, all
        data drained), or None (consumer abandoned via close(), remainder dropped)."""
        cdef shared_ptr[CxxMorsel] out
        cdef MorselQueueStatus status
        if _QTRACE:
            fprintf(stderr, b"[QTRACE] get: calling native dequeue\n")
            fflush(stderr)
        with nogil:
            status = self._q.get(out)
        if _QTRACE:
            fprintf(stderr, b"[QTRACE] get: dequeue returned status=%d\n", <int>status)
            fflush(stderr)
        if status == MorselQueueStatus.ABANDONED:
            return None
        if status == MorselQueueStatus.FINISHED:
            return MQ_FINISHED
        if _QTRACE:
            fprintf(stderr, b"[QTRACE] get: materializing CxxMorsel -> Morsel\n")
            fflush(stderr)
        result = cxx_to_morsel(out)
        if _QTRACE:
            fprintf(stderr, b"[QTRACE] get: materialize done\n")
            fflush(stderr)
        return result

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
