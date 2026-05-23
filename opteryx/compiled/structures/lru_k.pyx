# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

# distutils: language = c++

"""
LRU-2 cache backed by a C++ implementation (K=2 fixed).

API is identical to the old Python-based LRU_K so callers need no changes.
The k parameter is accepted for compatibility but ignored; behaviour is always
K=2.
"""

from libc.stdint cimport int64_t
from libcpp.string cimport string


cdef class LRU_K:

    def __cinit__(self, int64_t k=2, int64_t max_size=0, int64_t max_memory=0):
        self.k          = k        # stored for compatibility; C++ always uses K=2
        self.max_size   = max_size
        self.max_memory = max_memory
        self._lru = new CppLRU2(max_size, max_memory)

    def __dealloc__(self):
        if self._lru is not NULL:
            del self._lru
            self._lru = NULL

    def __len__(self):
        return self._lru.size()

    def __contains__(self, bytes key):
        cdef const char* kp  = key
        cdef int64_t     kn  = <int64_t>len(key)
        cdef bint found
        with nogil:
            found = self._lru.contains(kp, kn)
        return found

    cpdef object get(self, bytes key):
        """Return cached value for key, or None on miss."""
        cdef const char* kp      = key
        cdef int64_t     kn      = <int64_t>len(key)
        cdef const char* out_data = NULL
        cdef int64_t     out_len  = 0
        cdef bint        hit

        with nogil:
            hit = self._lru.get_into(kp, kn, &out_data, &out_len)

        if not hit:
            return None
        return out_data[:out_len]

    cpdef tuple set(self, bytes key, bytes value, bint evict):
        """
        Store key-value pair, optionally evicting if limits are exceeded.

        Returns (None, None) — eviction details not surfaced in the hot path.
        """
        cdef const char* kp = key
        cdef int64_t     kn = <int64_t>len(key)
        cdef const char* vp = value
        cdef int64_t     vn = <int64_t>len(value)

        with nogil:
            self._lru.set(kp, kn, vp, vn, evict)
        return (None, None)

    cpdef object evict(self, bint details):
        """
        Evict one item.

        Always returns (key, value) tuple or (None, None).
        When details is False, value is None (not fetched from C++).
        """
        cdef string key_out
        cdef string val_out
        cdef bint   evicted

        with nogil:
            evicted = self._lru.evict_one_into(details, key_out, val_out)

        if not evicted:
            return (None, None)

        cdef bytes py_key = key_out
        if details:
            return (py_key, <bytes>val_out)
        return (py_key, None)

    cpdef bint delete(self, bytes key):
        """Remove key from cache; returns True if it was present."""
        cdef const char* kp = key
        cdef int64_t     kn = <int64_t>len(key)
        cdef bint removed
        with nogil:
            removed = self._lru.erase(kp, kn)
        return removed

    cpdef void clear(self, bint reset_stats):
        """Remove all items; optionally reset hit/miss/eviction counters."""
        with nogil:
            self._lru.clear(reset_stats)

    @property
    def size(self):
        return self._lru.size()

    @property
    def current_memory(self):
        return self._lru.current_memory()

    @property
    def hits(self):
        return self._lru.hits()

    @property
    def misses(self):
        return self._lru.misses()

    @property
    def evictions(self):
        return self._lru.evictions()

    @property
    def inserts(self):
        return self._lru.inserts()

    @property
    def stats(self):
        """(hits, misses, evictions, inserts) — matches old API."""
        return (self._lru.hits(), self._lru.misses(),
                self._lru.evictions(), self._lru.inserts())

    @property
    def keys(self):
        raise NotImplementedError("keys property not available in C++ backend")

    def items(self):
        raise NotImplementedError("items() not available in C++ backend")

    @property
    def memory_usage(self):
        return self._lru.current_memory()

    def reset(self, bint reset_stats=False):
        self.clear(reset_stats)
