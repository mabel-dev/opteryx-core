# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: initializedcheck=False

"""Kernel implementations for scalar functions.

Kernels are organised by semantic domain:
- text: LENGTH, UPPER, LOWER, …
- arithmetic: ROUND, FLOOR, CEILING, ABS, SIGN, SQRT, …
- temporal: TRUNC, DATEDIFF, EXTRACT, YEAR, MONTH, …
- logical: COALESCE, IFNULL, NULLIF, CASE
- utility: GET_STRING, SORT, GREATEST, LEAST, …

The leaf files are textually included here so the whole package compiles
to a single .so. Their symbols live directly in this module's namespace.

For backwards compatibility, the historical `from implementations import
arithmetic as ...` pattern still works: each leaf name is bound to this
same module after the includes complete, so `arithmetic.round1` resolves
the same way as a top-level `round1`.

Note: Binary operators (Plus, Minus, …) are handled separately via
binary_operators. Aggregate functions are handled by the operators subsystem.
"""

include "logical.pyx"
include "text.pyx"
include "utility.pyx"


# Submodule-alias shims so legacy import patterns keep working after
# consolidation. Both `from impl import LEAF as X` (attribute access) and
# `from impl.LEAF import name` (submodule access) resolve to this same
# module — the kernel names from every leaf are in this namespace via the
# includes above.
#
# Note: arithmetic and temporal are excluded from this list because they're
# now separate Python modules (.py files) that re-export from nanobind C++.
import sys as _sys
_self = _sys.modules[__name__]
for _leaf in ("logical", "text", "utility"):
    globals()[_leaf] = _self
    _sys.modules[f"{__name__}.{_leaf}"] = _self
del _leaf
