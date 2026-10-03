import ctypes
import os
import sys

# Load draken_native with RTLD_GLOBAL so bridge symbols (draken_vector_unwrap,
# draken_vector_own_raw, draken_vector_own) are visible to consumer extensions
# compiled against draken/vectors/_vector_bridge.h / draken/core/draken_capi.h at runtime.
# Must happen before any consumer extension (e.g. vector_bitwise) is imported.
_flags = sys.getdlopenflags()
sys.setdlopenflags(ctypes.RTLD_GLOBAL | os.RTLD_NOW)
from draken import draken_native  # noqa: F401, E402
sys.setdlopenflags(_flags)

# draken_native.so also carries draken's Cython modules (one PyInit_<name> each —
# see build_common.draken_rugo_extensions). Register them under their dotted names
# from that one file, in dependency order: a Cython module's init type-imports the
# modules it cimports (bool_vector -> vector, morsel -> vector, sort -> morsel), so
# each must already be in sys.modules. Their parent packages are imported after,
# because draken.morsels/__init__ itself imports draken.morsels.morsel.
import importlib  # noqa: E402
import importlib.machinery  # noqa: E402
import importlib.util  # noqa: E402

_NATIVE_CYTHON_MODULES = (
    "draken.vectors.vector",
    "draken.vectors.bool_vector",
    "draken.morsels.morsel",
    "draken.morsels.sort",
    "draken.ops.kernels._kernel_registry",
)

for _name in _NATIVE_CYTHON_MODULES:
    _spec = importlib.util.spec_from_file_location(
        _name,
        draken_native.__file__,
        loader=importlib.machinery.ExtensionFileLoader(_name, draken_native.__file__),
    )
    _module = importlib.util.module_from_spec(_spec)
    sys.modules[_name] = _module
    _spec.loader.exec_module(_module)

for _name in _NATIVE_CYTHON_MODULES:
    _parent, _, _leaf = _name.rpartition(".")
    setattr(importlib.import_module(_parent), _leaf, sys.modules[_name])

from draken.vectors import Vector  # noqa: E402
from draken.morsels import Morsel  # noqa: E402

# The supported Python-list ingestion entry point: dispatches on dtype and
# encodes str → UTF-8 bytes for the bytes-only VARCHAR/NVARCHAR native edge.
# `vector_from_sequence` means THIS function; the raw INT64-only native builder
# is spelled `draken.draken_native.vector_int64_from_sequence`.
from draken.interop.vector_sequence import vector_from_sequence  # noqa: E402


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


__all__ = ["Vector", "Morsel", "vector_from_sequence", "preload_library_path"]
