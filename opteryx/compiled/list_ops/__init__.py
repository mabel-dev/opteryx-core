"""Package init for compiled list ops

This module provides the compiled (Cython/C++) function implementations as
convenience re-exports at the package level. The build process creates a
compiled extension named `function_definitions`, so we import all public
symbols from it into the package namespace for compatibility with code that
imports directly from `opteryx.compiled.list_ops`.
"""

try:
    # Re-export compiled functions from the compiled extension
    from .function_definitions import *  # noqa: F401,F403
except ImportError:
    # If the extension isn't available (e.g., pre-build environment), don't
    # prevent the package from importing. Callers can import the extension
    # directly or build the extension using `make c`.
    pass
