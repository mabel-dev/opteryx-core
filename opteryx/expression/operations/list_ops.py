"""List operations (InList, NotInList) - Draken-native only.

Array input must be Draken vector. Values parameter can be any iterable or have to_pylist().
If array input isn't Draken, you get AttributeError - that's a bug upstream.
"""

from opteryx.compiled import vector_ops
from opteryx.expression.operations.fastpath_dictionary import dictionary_fastpath
from opteryx.expression.operations.fastpath_telemetry import record_dict_fastpath_hit


def in_list(arr, value, dict_candidate=False):
    """Check if elements are in a list of values (InList). Array must be Draken vector."""
    if dict_candidate:
        fast = dictionary_fastpath(arr, "InList", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `InList`.")

    # Convert values to set - try to_pylist() if available, else treat as iterable
    try:
        values = set(value.to_pylist())
    except AttributeError:
        values = set(value)

    return vector_ops.vector_in_list(arr, values)


def not_in_list(arr, value, dict_candidate=False):
    """Check if elements are not in a list of values (NotInList). Array must be Draken vector."""
    if dict_candidate:
        fast = dictionary_fastpath(arr, "NotInList", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `NotInList`.")

    # Convert values to set - try to_pylist() if available, else treat as iterable
    try:
        values = set(value.to_pylist())
    except AttributeError:
        values = set(value)

    matches = vector_ops.vector_in_list(arr, values)
    return matches.not_vector()
