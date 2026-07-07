import ctypes
import os
import sys

# Load draken_native with RTLD_GLOBAL so bridge symbols (draken_vector_unwrap,
# draken_vector_own_raw, draken_vector_own) are visible to consumer extensions
# compiled against draken/core/draken_bridge.h at runtime.
# Must happen before any consumer extension (e.g. vector_bitwise) is imported.
_flags = sys.getdlopenflags()
sys.setdlopenflags(ctypes.RTLD_GLOBAL | os.RTLD_NOW)
from draken import draken_native  # noqa: F401, E402
sys.setdlopenflags(_flags)

from draken.vectors import Vector  # noqa: E402
from draken.morsels import Morsel  # noqa: E402


def preload_library_path():
    """Absolute path to the bundled standalone mimalloc shared library.

    This is an independent .so (vendored mimalloc 3.3, built by build_common.py),
    linked into nothing. Set it as ``LD_PRELOAD`` at process launch to swap the
    process allocator to mimalloc and avoid glibc per-thread-arena fragmentation
    OOM under the multi-threaded native engine, e.g. in a container entrypoint:

        LD_PRELOAD=$(python -c 'import draken; print(draken.preload_library_path())')

    ld.so reads LD_PRELOAD at exec, before the interpreter — so it cannot be set
    from Python for the running process; it must be in the environment at launch.

    Returns None if the library is not present (e.g. an unsupported platform).
    """
    _here = os.path.dirname(os.path.abspath(__file__))
    for _name in ("libmimalloc.so", "libmimalloc.dylib"):
        _path = os.path.join(_here, _name)
        if os.path.exists(_path):
            return _path
    return None


__all__ = ["Vector", "Morsel", "preload_library_path"]
