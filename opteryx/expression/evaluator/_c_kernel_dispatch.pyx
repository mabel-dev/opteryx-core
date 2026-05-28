# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True

"""C kernel dispatcher for Phase 9b bytecode executor.

Provides efficient dispatch to C function-pointer kernels with DrakenVector*.
Called from evaluation.pyx when opcode has BC_INSTR_C_NATIVE flag set.

Signature pattern (from Phase 9a):
  typedef VecResult (*kernel_fn_t)(void* ctx, const DrakenVector* const* args, uint32_t nargs)

Wraps VecResult to Cython Vector for result compatibility with legacy wrapping code.
"""

from libc.stdint cimport uint32_t
from draken.core.buffers cimport DrakenVector, VecResult
from draken.vectors.vector cimport Vector

# Import VecResult handling from error_handling
cdef extern from "ops/kernels/error_handling.h":
    bint draken_has_error()
    const char* draken_get_error_message()
    void draken_error_message_clear()

# Kernel function typedef (from kernel_registry.h)
ctypedef VecResult (*kernel_fn_t)(void* ctx, const DrakenVector* const* args, uint32_t nargs)


cdef class VecResultWrapper:
    """Wrapper for VecResult to safely bridge C ↔ Cython boundary."""
    cdef VecResult _result

    def __cinit__(self, VecResult result):
        self._result = result

    cdef VecResult get_result(self):
        return self._result

    property data:
        def __get__(self):
            return <unsigned long long>self._result.data

    property validity:
        def __get__(self):
            return <unsigned long long>self._result.validity

    property selection:
        def __get__(self):
            return <unsigned long long>self._result.selection

    property type:
        def __get__(self):
            return self._result.type

    property length:
        def __get__(self):
            return self._result.length


cdef VecResult call_c_kernel_variadic(kernel_fn_t fn, void* ctx, DrakenVector** dv_stack, int sp, int arity):
    """
    Call C kernel with variadic arguments from DrakenVector* stack.

    Args:
        fn: C function pointer
        ctx: context struct (or NULL)
        dv_stack: execution stack of DrakenVector* pointers
        sp: stack pointer (top of stack for args)
        arity: argument count

    Returns:
        VecResult from kernel
    """
    cdef const DrakenVector** args
    cdef int i
    cdef VecResult result

    # Allocate args array on stack
    args = <const DrakenVector**>PyMem_Malloc((arity + 1) * sizeof(DrakenVector*))
    if args == NULL:
        raise MemoryError()

    try:
        # Fill args array in reverse order (bottom of arity args on stack)
        for i in range(arity):
            args[i] = dv_stack[sp - arity + i]

        # Call kernel
        result = fn(ctx, args, <uint32_t>arity)
        return result

    finally:
        PyMem_Free(args)


cdef VecResult call_c_kernel_binary(kernel_fn_t fn, void* ctx, DrakenVector* left, DrakenVector* right):
    """
    Call C kernel with 2 DrakenVector arguments.
    Optimized path for binary operations (avoids allocation).
    """
    cdef const DrakenVector* args[2]
    args[0] = left
    args[1] = right
    return fn(ctx, args, 2)


cdef VecResult call_c_kernel_unary(kernel_fn_t fn, void* ctx, DrakenVector* vec):
    """
    Call C kernel with 1 DrakenVector argument.
    Optimized path for unary operations (avoids allocation).
    """
    cdef const DrakenVector* args[1]
    args[0] = vec
    return fn(ctx, args, 1)


def dispatch_c_kernel(unsigned long long kernel_fn_ptr, unsigned long long ctx_ptr, list dv_stack_list, int sp, int arity):
    """
    Dispatch to C kernel and wrap result for executor compatibility.

    Public interface for evaluation.pyx:
        result_vec = dispatch_c_kernel(kernel_fn, ctx, dv_stack_list, sp, arity)

    Args:
        kernel_fn_ptr: Opaque int from slot.kernel_fn (C function pointer cast to uint64)
        ctx_ptr: Opaque int from slot.ctx_ptr (context struct pointer or 0)
        dv_stack_list: Python list of DrakenVector* (as opaque ints)
        sp: Stack pointer (points past top item)
        arity: Argument count

    Returns:
        Cython Vector wrapping the VecResult from the kernel
        Raises if kernel returns error (data == NULL)

    Note: DrakenVector* values in dv_stack_list are stored as opaque Python ints
          because DrakenVector* is C-only and cannot be Python-level object.
    """
    cdef kernel_fn_t fn = <kernel_fn_t>kernel_fn_ptr
    cdef void* ctx = <void*>ctx_ptr if ctx_ptr != 0 else NULL
    cdef DrakenVector* dv_stack[512]  # Reasonable upper bound for stack depth
    cdef VecResult result
    cdef int i

    # Convert opaque ints back to DrakenVector*
    if sp > 512:
        raise RuntimeError(f"Stack depth {sp} exceeds maximum {512}")

    for i in range(sp):
        dv_stack[i] = <DrakenVector*><unsigned long long>dv_stack_list[i]

    # Call kernel
    if arity == 1:
        result = call_c_kernel_unary(fn, ctx, dv_stack[sp - 1])
    elif arity == 2:
        result = call_c_kernel_binary(fn, ctx, dv_stack[sp - 2], dv_stack[sp - 1])
    else:
        result = call_c_kernel_variadic(fn, ctx, dv_stack, sp, arity)

    # Check for error
    if result.data == NULL:
        error_msg = ""
        if draken_has_error():
            error_msg = draken_get_error_message().decode('utf-8', errors='replace')
            draken_error_message_clear()
        raise RuntimeError(f"C kernel error: {error_msg}")

    # Wrap VecResult in Vector and return
    # Note: Vector constructor expects a nanobind::handle, so we reconstruct from VecResult
    # For now, return as opaque result wrapper for compatibility with executor wrapping
    return VecResultWrapper(result)


from cpython.mem cimport PyMem_Malloc, PyMem_Free
