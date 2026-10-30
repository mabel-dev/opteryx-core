import ctypes
import os
import sys

# x86-64 builds target -march=haswell (build_common.py). On an older CPU the first
# native instruction is a SIGILL with no message, so check the floor here, in
# Python, before any extension loads. The flags are those -march=haswell lets the
# compiler emit on its own (cpuinfo names; `abm` is LZCNT).
_HASWELL_FLAGS = frozenset(
    {"sse4_2", "popcnt", "avx", "avx2", "fma", "f16c", "bmi1", "bmi2", "movbe", "abm"}
)


def _missing_x86_flags(cpuinfo_text: str) -> frozenset:
    """The Haswell-floor flags absent from a /proc/cpuinfo text."""
    for line in cpuinfo_text.splitlines():
        if line.startswith("flags"):
            return _HASWELL_FLAGS - set(line.split(":", 1)[1].split())
    return frozenset()


def _require_x86_floor() -> None:
    import platform

    if sys.platform != "linux" or platform.machine() != "x86_64":
        return
    # shortcut: no readable /proc/cpuinfo (exotic sandboxes) skips the check and
    # leaves the old SIGILL behaviour; add a cpuid probe if that ever matters.
    if not os.path.exists("/proc/cpuinfo"):
        return
    with open("/proc/cpuinfo") as cpuinfo:
        missing = _missing_x86_flags(cpuinfo.read())
    if missing:
        raise ImportError(
            "draken (opteryx/rugo) is built for x86-64 CPUs with the Haswell (2013) "
            f"instruction set or newer; this CPU lacks {', '.join(sorted(missing))}. "
            "Run on a newer CPU, or a VM that exposes AVX2/BMI2/FMA to the guest."
        )


_require_x86_floor()

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
