# Cython declarations for draken_compare_dv (Stage B+C delivered).
#
# Coverage as of Stage C:
#   INT64, FLOAT64, TIMESTAMP64 — ordinal compare on native int64/float64 storage
#   DATE32                       — ordinal compare on int32 days-since-epoch
#   VARCHAR, NVARCHAR, VARBINARY — german-string layout; bytewise for ordering
#   DECIMAL                      — NOT supported (needs scale from logical-type
#                                  descriptor, not on DrakenVector). Python fallback.
#
# C API documented in draken/ops/compare_dv.h. The native eval engine
# cimports this surface for BC_COMPARE handling in the DrakenVector*
# stack rewrite.
#
# Symbol locality: implementation lives in draken_native.so; reachable
# via the RTLD_GLOBAL bridge pattern set up at draken/__init__.py.

from libc.stdint cimport int16_t, uint32_t

from draken.core.buffers cimport DrakenVector
from draken.core.frame_arena cimport DrakenFrameArena


cdef extern from "ops/compare_dv.h":
    # Compare two DrakenVectors element-wise.
    #
    # op_code: 0=EQ, 1=NE, 2=GT, 3=GE, 4=LT, 5=LE.
    # Returns NULL on:
    #   - unsupported type (anything other than INT64 / FLOAT64 in Stage B)
    #   - cross-type operands (left.type != right.type)
    #   - length mismatch, OOM, or NULL inputs
    # Caller falls back to Python-mediated path on NULL.
    DrakenVector* draken_compare_dv(
        int                op_code,
        DrakenVector*      left,
        DrakenVector*      right,
        int16_t            left_type_hint,
        int16_t            right_type_hint,
        uint32_t           n_rows,
        DrakenFrameArena*  arena
    ) nogil
