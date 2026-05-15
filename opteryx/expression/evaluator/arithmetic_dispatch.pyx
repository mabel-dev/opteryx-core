"""Arithmetic operation dispatch for Draken vectors.

Architecture:
- VectorType-based routing to kernel registry
- Only processes Draken vectors (at least one operand must be Draken)
- Materialize encoded vectors (DICTIONARY_ENCODED, CONSTANT_ENCODED) to base types
- Kernels handle null propagation and type coercion at Python level

Cython migration: hoists the per-call `from draken.vectors.arithmetic_kernels
import get_arithmetic_kernel` to module load, caches type lookups locally, and
removes the bounce back to a Python module from arithmetic.pyx.
"""

from opteryx.utils.vector_types import (
    VectorType,
    get_vector_type,
    is_draken_vector,
    is_scalar,
)

from draken.vectors.arithmetic_kernels import get_arithmetic_kernel


cpdef call_arithmetic_op(str op, left, right):
    """Execute arithmetic operation with VectorType-based dispatch.

    Routes to native Draken kernels when at least one operand is a Draken
    vector. Returns None when no kernel is registered for the operand
    combination (the caller in arithmetic.pyx surfaces that as
    NotImplementedError) or when the kernel raises a recognised type error.
    """
    cdef bint left_is_draken = is_draken_vector(left)
    cdef bint right_is_draken = is_draken_vector(right)

    if not (left_is_draken or right_is_draken):
        # Both scalars — caller falls back to the binary_operations path.
        return None

    left_type = get_vector_type(left) if left_is_draken else None
    right_type = get_vector_type(right) if right_is_draken else None

    # When exactly one operand is CONSTANT_ENCODED, extract its scalar instead
    # of materializing a full N-row vector per morsel — the kernel registry
    # has vector⊕scalar handlers that avoid the allocation. Both sides
    # CONSTANT_ENCODED, or any side DICTIONARY_ENCODED, still materialize as
    # before so the kernel sees concrete numeric vectors.
    cdef bint left_const = left_type == VectorType.CONSTANT_ENCODED
    cdef bint right_const = right_type == VectorType.CONSTANT_ENCODED
    cdef bint extract_left = left_const and not right_const and len(left) > 0
    cdef bint extract_right = right_const and not left_const and len(right) > 0

    if extract_left:
        left = left[0]
        left_is_draken = False
        left_type = None
    elif left_type == VectorType.DICTIONARY_ENCODED or left_const:
        left = left.materialize()
        left_type = get_vector_type(left)

    if extract_right:
        right = right[0]
        right_is_draken = False
        right_type = None
    elif right_type == VectorType.DICTIONARY_ENCODED or right_const:
        right = right.materialize()
        right_type = get_vector_type(right)

    # DECIMAL vectors are int64-backed with a scale; convert to float64 so the
    # existing float64 kernels can handle DECIMAL ± numeric.
    if left_type == VectorType.DECIMAL:
        left = left.to_float64_vector()
        left_type = get_vector_type(left)
    if right_type == VectorType.DECIMAL:
        right = right.to_float64_vector()
        right_type = get_vector_type(right)

    # Re-discriminate Draken-ness post-materialisation (no-op in practice but
    # makes the subsequent type-pair derivation correct even if a materialize
    # path ever returned a non-Draken object).
    left_is_draken = is_draken_vector(left)
    right_is_draken = is_draken_vector(right)

    if left_is_draken and right_is_draken:
        pass  # both types already resolved above
    elif left_is_draken:
        # Right is scalar / non-Draken. Treat scalar as matching the left type
        # so the kernel registry can pick a vector⊕scalar handler.
        right_type = right_type if not is_scalar(right) else left_type
    elif right_is_draken:
        left_type = left_type if not is_scalar(left) else right_type
    else:
        return None

    kernel = get_arithmetic_kernel(left_type, right_type, op)
    if kernel is None:
        return None

    try:
        return kernel(left, right)
    except (TypeError, ValueError, AttributeError):
        # Kernel rejected the operand pair (e.g. dtype mismatch the registry
        # didn't catch). Caller will raise a typed NotImplementedError.
        return None
