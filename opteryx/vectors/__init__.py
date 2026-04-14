"""
opteryx.vectors package

This package groups vector-related implementations (embeddings, vector type helpers,
and other vector utilities). It intentionally re-exports the primary submodules
and their public symbols at package level so callers can do either:

    import opteryx.vectors as v
    v.embeddings.get_embedding_provider(...)

or

    from opteryx.vectors import get_embedding_provider

Adding this __init__ makes the directory an explicit package and centralizes
vector-related exports.
"""

from __future__ import annotations

import importlib
from types import ModuleType
from typing import Any, List, Set

# Submodules that compose the vectors package. Add future vector-specific modules
# here to have them automatically imported and re-exported.
_SUBMODULE_NAMES = ("embeddings", "vector_types", "vector_ranking")

# Import submodules and bind them into this package namespace.
for _name in _SUBMODULE_NAMES:
    # Import as a package-local module (e.g. opteryx.vectors.embeddings)
    _mod = importlib.import_module(f"{__name__}.{_name}")
    globals()[_name] = _mod  # type: ignore

# Build a package-level __all__ that exposes the submodule names and the public
# symbols from each submodule so `from opteryx.vectors import *` behaves sensibly.
__all__: List[str] = list(_SUBMODULE_NAMES)

# Track public symbol names we've already exported to avoid duplicates.
_exported: Set[str] = set(__all__)

for _name in _SUBMODULE_NAMES:
    _mod: ModuleType = globals()[_name]  # type: ignore
    for _attr in dir(_mod):
        if _attr.startswith("_"):
            continue
        # Expose the attribute at package level and include it in __all__
        if _attr not in globals():
            globals()[_attr] = getattr(_mod, _attr)
        if _attr not in _exported:
            __all__.append(_attr)
            _exported.add(_attr)


def __getattr__(name: str) -> Any:
    """
    Lazy attribute access fallback.

    This function is consulted for attribute access on the package when the
    attribute is not found in the package's globals. It will attempt to resolve
    the attribute from the known submodules and return it if present.
    """
    # First check if we've exported it already
    if name in globals():
        return globals()[name]

    # Otherwise, search submodules in order and return the first match
    for _name in _SUBMODULE_NAMES:
        _mod = globals().get(_name)
        if _mod is not None and hasattr(_mod, name):
            return getattr(_mod, name)

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> List[str]:
    """Return a sorted list of names available on the package for tooling/IDE support."""
    # include module-level globals and the exported names
    return sorted(set(list(globals().keys()) + __all__))
