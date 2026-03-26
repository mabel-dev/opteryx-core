"""List operations (InList, NotInList)."""

import pyarrow

from opteryx.compiled import vector_ops
from opteryx.expression.operations.fastpath_constant import _coerce_in_list_values
from opteryx.expression.operations.fastpath_dictionary import (
    dictionary_fastpath,
    has_dictionary_candidate,
)
from opteryx.expression.operations.fastpath_telemetry import record_dict_fastpath_hit


def in_list(arr, value, dict_candidate=False):
    """Check if elements are in a list of values (InList)."""
    if dict_candidate:
        fast = dictionary_fastpath(arr, "InList", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `InList`.")

    from opteryx.compiled.draken.interop.arrow import vector_from_arrow

    to_pylist = getattr(value, "to_pylist", None)
    if to_pylist is not None:
        value = to_pylist()

    to_numpy = getattr(value, "to_numpy", None)
    if to_numpy is not None:
        value = to_numpy(zero_copy_only=False)

    values = set(value)

    if isinstance(arr, pyarrow.ChunkedArray):
        arr = arr.combine_chunks()

    if not isinstance(arr, (pyarrow.Array, pyarrow.ChunkedArray)):
        arr = pyarrow.array(arr)

    if isinstance(arr, pyarrow.Array):
        arr = vector_from_arrow(arr)

    return vector_ops.vector_in_list(arr, values)


def not_in_list(arr, value, dict_candidate=False):
    """Check if elements are not in a list of values (NotInList)."""
    if dict_candidate:
        fast = dictionary_fastpath(arr, "NotInList", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `NotInList`.")

    from opteryx.compiled.draken.interop.arrow import vector_from_arrow

    to_pylist = getattr(value, "to_pylist", None)
    if to_pylist is not None:
        value = to_pylist()

    to_numpy = getattr(value, "to_numpy", None)
    if to_numpy is not None:
        value = to_numpy(zero_copy_only=False)

    values = set(value)

    if isinstance(arr, pyarrow.ChunkedArray):
        arr = arr.combine_chunks()

    if not isinstance(arr, (pyarrow.Array, pyarrow.ChunkedArray)):
        arr = pyarrow.array(arr)

    if isinstance(arr, pyarrow.Array):
        arr = vector_from_arrow(arr)

    matches = vector_ops.vector_in_list(arr, values)
    return matches.not_vector()
