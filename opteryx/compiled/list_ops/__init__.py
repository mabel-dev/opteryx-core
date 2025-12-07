"""
Expose compiled list_ops functions at package level.

When the consolidated C/C++ extension `function_definitions` is built it provides
the implementations (e.g. `list_contains_all`). Import them here so code can do:

        from opteryx.compiled.list_ops import list_contains_all

If the compiled extension is not available (during development before running
`make c`) the import will be ignored so tooling and static analysis can still
import this package without raising an ImportError.
"""

try:
    # Consolidated compiled module name created by setup.py
    from .function_definitions import *  # noqa: F401,F403
except (ImportError, ModuleNotFoundError):
    # If the extension isn't built, silently continue; callers should only use
    # these functions after building the extensions (e.g. via `make c`).
    pass
