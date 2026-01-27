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

    _compiled_present = True
except Exception:  # Broad: catch ImportError and any other failure during import
    # If the extension isn't available or failed to load, don't prevent the
    # package from importing. We'll provide pure-Python fallbacks below so
    # callers don't get ImportError/AttributeError at import-time.
    _compiled_present = False


# Pure-Python fallbacks for key functions. These are used only if the
# compiled extension isn't available or doesn't expose the symbol.
# Implemented to match the behavior of the Cython versions.
if "list_contains_all" not in globals():
    import numpy as _np

    def _py_list_contains_all(array, items):
        """Pure-Python fallback for list_contains_all.

        Parameters
        - array: numpy.ndarray of object arrays
        - items: set

        Returns
        - numpy.ndarray of dtype uint8 with 1 where all items present, else 0
        """
        size = int(array.shape[0]) if getattr(array, "shape", None) else len(array)
        res = _np.zeros(size, dtype=_np.uint8)

        if not items:
            res[:] = 1
            return res

        items_set = set(items)
        for i in range(size):
            test_set = array[i]
            if test_set is None:
                continue
            # If test_set has a shape, assume it's array-like; otherwise iterate
            try:
                length = test_set.shape[0]
            except Exception:
                length = len(test_set) if hasattr(test_set, "__len__") else 0
            if length == 0:
                continue

            found = set()
            for element in test_set:
                if element in items_set:
                    found.add(element)
                    if len(found) == len(items_set):
                        res[i] = 1
                        break
        return res

    # Export the fallback under the expected name
    list_contains_all = _py_list_contains_all
