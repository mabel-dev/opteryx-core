# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True

"""Cython wrapper for C kernel registry lookup.

Provides bytecode builder and executor access to C kernel function pointers
and context struct allocation. Used during bind-time kernel resolution in
Phase 9b.

Public interface:
  lookup_kernel(name: str) -> (kernel_fn, ctx_ptr) or (None, None)
  - Looks up a kernel by name (uppercase string)
  - Returns function pointer (as opaque int) and context pointer (or None)

Context allocation (for parameterized kernels):
  alloc_cast_timestamp_ctx(unit: int) -> ctypes-opaque pointer
  alloc_binary_op_ctx(op_code: int) -> ctypes-opaque pointer
  alloc_extraction_ctx(sub_op_code: int) -> ctypes-opaque pointer
  free_context(ctx) -> None
"""

from libc.stdint cimport uint16_t
from cpython.ref cimport PyObject

# C declarations from kernel_registry.h
cdef extern from "ops/kernels/kernel_registry.h":
    ctypedef void* kernel_fn_t
    bint kernel_registry_lookup(const char* name, kernel_fn_t* out_fn, void** out_ctx)

    ctypedef struct cast_timestamp_ctx_:
        int unit
    ctypedef cast_timestamp_ctx_ cast_timestamp_ctx

    ctypedef struct binary_op_ctx_:
        uint16_t op_code
    ctypedef binary_op_ctx_ binary_op_ctx

    ctypedef struct extraction_ctx_:
        uint16_t sub_op_code
    ctypedef extraction_ctx_ extraction_ctx

    cast_timestamp_ctx* kernel_alloc_cast_timestamp_ctx(int unit)
    binary_op_ctx* kernel_alloc_binary_op_ctx(uint16_t op_code)
    extraction_ctx* kernel_alloc_extraction_ctx(uint16_t sub_op_code)
    void kernel_free_context(void* ctx)


def lookup_kernel(str kernel_name):
    """
    Lookup kernel by name and return function pointer.

    Args:
        kernel_name: Uppercase kernel name (e.g., "ABS", "LENGTH", "ADD")

    Returns:
        Tuple (kernel_fn, ctx_ptr) where:
        - kernel_fn is an opaque integer (C function pointer)
        - ctx_ptr is an opaque integer (context struct pointer) or None
        If kernel not found, returns (None, None)

    Examples:
        fn, ctx = lookup_kernel("ABS")  # Single-arg function, no context
        fn, ctx = lookup_kernel("ADD")  # Binary op, might have context for op_code
    """
    cdef kernel_fn_t fn = NULL
    cdef void* ctx = NULL
    cdef bytes c_name = kernel_name.encode('utf-8')

    if kernel_registry_lookup(c_name, &fn, &ctx):
        return (<unsigned long long>fn, <unsigned long long>ctx if ctx != NULL else None)
    else:
        return (None, None)


def alloc_cast_timestamp_ctx(int unit):
    """
    Allocate context for INT64 → TIMESTAMP cast with unit parameter.

    Args:
        unit: 0=none, 1=ns, 2=us, 3=ms, 4=s, 5=days

    Returns:
        Opaque integer pointer to cast_timestamp_ctx struct
        Caller must free via free_context() when done
    """
    cdef cast_timestamp_ctx* ctx = kernel_alloc_cast_timestamp_ctx(unit)
    return <unsigned long long>ctx


def alloc_binary_op_ctx(int op_code):
    """
    Allocate context for binary operation with op_code dispatch.

    Args:
        op_code: BCBinaryOpCode (BOP_PLUS=1, BOP_MINUS=2, etc.)

    Returns:
        Opaque integer pointer to binary_op_ctx struct
        Caller must free via free_context() when done
    """
    cdef binary_op_ctx* ctx = kernel_alloc_binary_op_ctx(<uint16_t>op_code)
    return <unsigned long long>ctx


def alloc_extraction_ctx(int sub_op_code):
    """
    Allocate context for extraction operation with sub_op_code dispatch.

    Args:
        sub_op_code: BCExtractionOpCode (BC_EXTR_MAP_STRING=1, etc.)

    Returns:
        Opaque integer pointer to extraction_ctx struct
        Caller must free via free_context() when done
    """
    cdef extraction_ctx* ctx = kernel_alloc_extraction_ctx(<uint16_t>sub_op_code)
    return <unsigned long long>ctx


def free_context(unsigned long long ctx_ptr):
    """
    Free allocated context struct.

    Args:
        ctx_ptr: Opaque integer pointer from alloc_*_ctx()
    """
    if ctx_ptr != 0:
        kernel_free_context(<void*>ctx_ptr)
