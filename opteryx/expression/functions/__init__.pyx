# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: initializedcheck=False

"""Expression functions catalog and kernel specifications.

The catalog leaf is textually included so the package compiles to a single
.so. All classes, helpers and the `functions` / `get_catalog` / `is_function`
APIs live directly in this module's namespace.
"""

include "catalog.pyx"


__all__ = [
    "FunctionCatalog",
    "FunctionDefinition",
    "FunctionOverload",
    "FunctionResolutionContext",
    "functions",
    "get_catalog",
    "is_function",
    "KernelSpec",
    "LifecycleSpec",
    "ParameterSpec",
    "ResolvedArg",
    "ResolvedFunction",
    "ReturnSpec",
]


# Submodule-alias shim for legacy `from opteryx.expression.functions.catalog
# import name` callers.
import sys as _sys
_self = _sys.modules[__name__]
catalog = _self
_sys.modules[f"{__name__}.catalog"] = _self
del _self, _sys
