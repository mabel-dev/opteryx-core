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
Grouped aggregate (hashed) — single compiled module.

Compilation order matters: each .pxi file depends on symbols from the ones
included before it.  The include order below is topologically sorted.

  _key_store.pxi         → KeyStore
  _collectors_base.pxi   → BaseCollector
  _collectors_numeric.pxi → numeric collectors (depends on BaseCollector)
  _collectors_distinct.pxi → distinct collectors (depends on numeric helpers)
  _collectors_approx.pxi  → approx + array_agg collectors
  _engine.pxi            → GroupHashEngine (depends on all collectors + KeyStore)
  _factory.pxi           → create_collectors / resolve_deferred_collectors
  _node.pxi              → GroupedAggregateHashedNode (Python class)
"""

from libc.stdint cimport int8_t, int32_t, int64_t, uint8_t, uint16_t, uint32_t, uint64_t, INT64_MAX, INT64_MIN
from libc.math cimport HUGE_VAL
from libc.stdlib cimport malloc, free
from libc.string cimport memset
from libcpp.string cimport string
from libcpp.vector cimport vector

from draken.core.buffers cimport DrakenFixedBuffer, DrakenVarBuffer, DrakenType, DrakenVector
from draken.morsels.morsel cimport Morsel
from draken.vectors.vector cimport Vector, NULL_HASH, mix_hash

cdef extern from "core/alloc.h" nogil:
    void* draken_malloc(size_t n)
    void  draken_free(void* ptr)
include "_key_store.pxi"
include "_collectors_base.pxi"
include "_collectors_numeric.pxi"
include "_collectors_distinct.pxi"
include "_collectors_approx.pxi"
include "_collectors_buffered.pxi"
include "_engine.pxi"
include "_factory.pxi"
include "_node.pxi"
