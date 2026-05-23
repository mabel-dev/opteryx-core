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
