# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False

"""Per-aggregate state classes for grouped aggregation.

These classes are declared in aggregations_state_classes.pxd and hold
per-aggregate state vectors for the finalize path.
"""

from libcpp.vector cimport vector
from libc.stdint cimport int64_t
