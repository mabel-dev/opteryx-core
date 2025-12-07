"""
opteryx_core shim package

This package provides an alias 'opteryx_core' for the existing 'opteryx'
package. It makes the same public API available under the `opteryx_core`
import name without duplicating code. This is implemented as a thin shim
that imports opteryx and delegates attribute access.

The module sets its __path__ to the opteryx package path so submodules like
`opteryx_core.draken` resolve to files under the original `opteryx/` directory.
"""
from __future__ import annotations

import importlib
import warnings
from typing import Any

# Import the real package
opteryx = importlib.import_module("opteryx")

# Set our __path__ to opteryx.__path__ so submodules are discoverable as
# `opteryx_core.<submodule>` from the same source tree as `opteryx`.
try:
    # If opteryx is a package, make our package path match it so submodules
    # are discoverable as opteryx_core.<submodule>.
    __path__ = opteryx.__path__  # type: ignore
except ImportError:
    # If the real package wasn't importable, leave our path alone and allow
    # standard import-time errors when users try to access real API attributes.
    pass

# Re-export public attributes from the opteryx module. This is intentionally
# lightweight to avoid copying heavy state on import.
__all__ = getattr(opteryx, "__all__", [])
for _name in __all__:
    try:
        globals()[_name] = getattr(opteryx, _name)
    except AttributeError:
        # If an attribute can't be resolved, skip it — end users will get the
        # original import exception when they access it.
        pass

def __getattr__(name: str) -> Any:
    """Delegate attribute access to the original opteryx module."""
    return getattr(opteryx, name)

def __dir__() -> list[str]:
    return list(globals().keys()) + [n for n in getattr(opteryx, "__all__", []) if n not in globals()]

# A friendly runtime message (non-fatal) so users are aware this is an alias.
warnings.warn(
    "opteryx_core is an alias of 'opteryx'. Prefer using the canonical 'opteryx' package name for imports.",
    DeprecationWarning,
    stacklevel=2,
)
