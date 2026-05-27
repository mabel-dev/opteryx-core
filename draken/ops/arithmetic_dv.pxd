# Cython declarations for draken_arithmetic_dv (Stage B: INT64 + FLOAT64).
#
# C API documented in draken/ops/arithmetic_dv.h. The native eval engine
# cimports this surface for BC_BINARY_OP handling in the DrakenVector*
# stack rewrite.

from libc.stdint cimport uint32_t

from draken.core.buffers cimport DrakenVector
from draken.core.frame_arena cimport DrakenFrameArena


cdef extern from "ops/arithmetic_dv.h":
    # Op-code convention (BCBinaryOpCode):
    #   1 = PLUS, 2 = MINUS, 3 = MULTIPLY, 4 = DIVIDE, 5 = MODULO
    # Returns NULL on unsupported type/op combination, length mismatch,
    # cross-type operands, OOM, or NULL inputs. Caller falls back to
    # Python on NULL.
    DrakenVector* draken_arithmetic_dv(
        int                op_code,
        DrakenVector*      left,
        DrakenVector*      right,
        uint32_t           n_rows,
        DrakenFrameArena*  arena
    ) nogil
