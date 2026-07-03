import ctypes
import os
import sys

# Load thread_pool with RTLD_GLOBAL so bs_pool_submit_native / bs_pool_wait_native
# (src/cpp/bs_pool_bridge_c.h, implemented in opteryx/compiled/thread_pool_bridge.cpp)
# are visible to consumer extensions (e.g. opteryx.operators._operators) at runtime.
# Must happen before any consumer extension is imported. Mirrors draken/__init__.py's
# RTLD_GLOBAL load of draken_native for the same reason.
_flags = sys.getdlopenflags()
sys.setdlopenflags(ctypes.RTLD_GLOBAL | os.RTLD_NOW)
from opteryx.compiled import thread_pool  # noqa: F401, E402
sys.setdlopenflags(_flags)
