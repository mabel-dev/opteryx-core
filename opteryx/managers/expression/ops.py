"""
Code was originally taken from
https://github.com/TomScheffers/pyarrow_ops/blob/main/pyarrow_ops/ops.py
and has been extensively modified for Opteryx.
"""

import re

import numpy
import pyarrow
from orso.types import OrsoTypes
from pyarrow import compute

from opteryx.compiled import list_ops

_DICT_EXPR_TEL = {
    "draken_dict_expr_fastpath_hits": 0,
    "draken_dict_expr_fastpath_fallbacks": 0,
}

_DICT_FASTPATH_OPS = frozenset(
    (
        "Eq",
        "NotEq",
        "InList",
        "NotInList",
        "Like",
        "NotLike",
        "ILike",
        "NotILike",
        "RLike",
        "NotRLike",
    )
)
_DICT_NUMERIC_FASTPATH_OPS = frozenset(("Lt", "Gt", "LtEq", "GtEq"))
# Dictionary child type ids from third_party/mabel/draken/core/buffers.h.
_DICT_NUMERIC_CHILD_TYPES = frozenset((1, 2, 3, 4, 20, 21, 50))

# Operators where null compression isn't safe
skip_compression_ops = {
    "InList",
    "NotInList",
    "AnyOpEq",
    "AnyOpNotEq",
    "AnyOpGt",
    "AnyOpGtEq",
    "AnyOpLt",
    "AnyOpLtEq",
    "AnyOpLike",
    "AnyOpNotLike",
    "AnyOpILike",
    "AnyOpNotILike",
    "AnyOpRLike",
    "AnyOpNotRLike",
    "AllOpEq",
    "AllOpNotEq",
    "AtArrow",
    "ArrayContainsAll",
}


def reset_dict_expr_telemetry():
    _DICT_EXPR_TEL["draken_dict_expr_fastpath_hits"] = 0
    _DICT_EXPR_TEL["draken_dict_expr_fastpath_fallbacks"] = 0


def get_dict_expr_telemetry():
    return dict(_DICT_EXPR_TEL)


def _record_dict_hit():
    _DICT_EXPR_TEL["draken_dict_expr_fastpath_hits"] += 1


def _dictionary_vector(arr):
    if arr.__class__.__name__ == "DictionaryVector":
        return arr

    if isinstance(arr, pyarrow.ChunkedArray):
        if not pyarrow.types.is_dictionary(arr.type):
            return None
        if arr.num_chunks != 1:
            raise NotImplementedError(
                "Dictionary motor path does not support multi-chunk dictionary arrays."
            )
        arr = arr.chunk(0)

    if isinstance(arr, pyarrow.DictionaryArray):
        from opteryx.draken.interop.arrow import vector_from_arrow

        vec = vector_from_arrow(arr)
        if vec.__class__.__name__ == "DictionaryVector":
            return vec
        raise TypeError("Dictionary fastpath expected DictionaryVector conversion result.")

    return None


def _coerce_in_list_values(value):
    to_pylist = getattr(value, "to_pylist", None)
    if to_pylist is not None:
        value = to_pylist()
    if isinstance(value, (list, tuple, set)):
        return value
    return [value]


def _dictionary_fastpath(arr, operator, value):
    vec = _dictionary_vector(arr)
    if vec is None:
        return None

    if operator == "Eq":
        return vec.equals(value)
    if operator == "NotEq":
        return vec.not_equals(value)
    if operator in ("InList", "NotInList"):
        result = vec.in_list(_coerce_in_list_values(value))
        if operator == "NotInList":
            result = result.not_vector()
        return result
    if operator in ("Like", "NotLike"):
        result = vec.like(value, False)
        if operator == "NotLike":
            result = result.not_vector()
        return result
    if operator in ("ILike", "NotILike"):
        result = vec.like(value, True)
        if operator == "NotILike":
            result = result.not_vector()
        return result
    if operator in ("RLike", "NotRLike"):
        result = vec.rlike(value)
        if operator == "NotRLike":
            result = result.not_vector()
        return result
    if operator == "Lt":
        return vec.less_than(value)
    if operator == "Gt":
        return vec.greater_than(value)
    if operator == "LtEq":
        return vec.less_than_or_equals(value)
    if operator == "GtEq":
        return vec.greater_than_or_equals(value)

    return None


def _dictionary_supports_numeric_fastpath(arr):
    vec = _dictionary_vector(arr)
    if vec is None:
        return False
    return getattr(vec, "dictionary_value_type", None) in _DICT_NUMERIC_CHILD_TYPES


def _has_dictionary_candidate(arr):
    if isinstance(arr, pyarrow.ChunkedArray) and pyarrow.types.is_dictionary(arr.type):
        return True
    if isinstance(arr, pyarrow.DictionaryArray):
        return True
    return arr.__class__.__name__ == "DictionaryVector"


def filter_operations(left_arr, left_type, operator, right_arr, right_type):
    """
    Wrapped for Opteryx added to correctly handle null semantics.

    This returns an array with tri-state boolean (tue/false/none);
    if being used for display use as is, if being used for filtering, none is false.
    """
    if len(left_arr) == 0 or len(right_arr) == 0:
        return numpy.empty(0, dtype=bool)

    # INTEGERS and DECIMALS don't play nicely so we cast the INTS to DOUBLES
    if left_type == OrsoTypes.DECIMAL and right_type == OrsoTypes.INTEGER:
        right_arr = compute.cast(right_arr, pyarrow.float64())
        right_type = OrsoTypes.DOUBLE
    elif right_type == OrsoTypes.DECIMAL and left_type == OrsoTypes.INTEGER:
        left_arr = compute.cast(left_arr, pyarrow.float64())
        left_type = OrsoTypes.DOUBLE

    morsel_size = len(left_arr)
    compressed = False

    if operator not in skip_compression_ops:
        # compressing ARRAY columns is VERY SLOW

        # compute null positions
        left_null_positions = compute.is_null(left_arr, nan_is_null=True)

        # if the right side is an array, combine the null positions
        if len(right_arr) > 1:
            right_null_positions = compute.is_null(right_arr, nan_is_null=True)
            null_positions = numpy.logical_or(left_null_positions, right_null_positions)
        # if the right side is a scalar and is null, we can just return all nulls
        elif len(right_arr) == 1 and right_arr[0] is None:
            return pyarrow.nulls(morsel_size, type=pyarrow.bool_())
        # if the right side is a scalar and is not null, we can just use the left nulls
        else:
            null_positions = left_null_positions.to_numpy(False)

        # Early exit if all values are null
        if null_positions.all():
            return pyarrow.nulls(morsel_size, type=pyarrow.bool_())

        if null_positions.any():
            # if we have nulls and both columns are numpy arrays, we can speed things
            # up by removing the nulls from the calculations, we add the rows back in
            # later
            valid_positions = ~null_positions
            compressed = True

            if isinstance(left_arr, numpy.ndarray):
                left_arr = left_arr.compress(valid_positions)
            else:
                left_arr = compute.filter(left_arr, valid_positions)

            if len(right_arr) > 1:
                if isinstance(right_arr, numpy.ndarray):
                    right_arr = right_arr.compress(valid_positions)
                else:
                    right_arr = compute.filter(right_arr, valid_positions)

    if (
        OrsoTypes.TIMESTAMP in (left_type, right_type) or OrsoTypes.DATE in (left_type, right_type)
    ) and OrsoTypes.INTEGER in (left_type, right_type):
        from opteryx.functions.date_functions import convert_int64_array_to_pyarrow_datetime

        if left_type == OrsoTypes.INTEGER:
            left_arr = convert_int64_array_to_pyarrow_datetime(left_arr)
            left_type = OrsoTypes.TIMESTAMP
        if right_type == OrsoTypes.INTEGER:
            right_arr = convert_int64_array_to_pyarrow_datetime(right_arr)
            right_type = OrsoTypes.TIMESTAMP

    if OrsoTypes.INTERVAL in (left_type, right_type):
        from opteryx.datatypes.intervals import INTERVAL_KERNELS

        function = INTERVAL_KERNELS.get((left_type, right_type, operator))
        if function is None:
            from opteryx.exceptions import UnsupportedTypeError

            raise UnsupportedTypeError(
                f"Cannot perform {operator.upper()} on {left_type} and {right_type}."
            )

        results_mask = function(left_arr, left_type, right_arr, right_type, operator)
    else:
        # do the evaluation
        results_mask = _inner_filter_operations(left_arr, operator, right_arr)

    if compressed:
        # build tri-state response, PyArrow supports tristate, numpy does not
        full_result = numpy.full(morsel_size, None, dtype=object)
        numpy.place(full_result, valid_positions, results_mask)
        return pyarrow.array(full_result, type=pyarrow.bool_())

    return results_mask


# Filter functionality
def _inner_filter_operations(arr, operator, value):
    """
    Execute filter operations, this returns an array of the indexes of the rows that
    match the filter
    """
    if not operator.startswith(("AnyOp", "AllOp")):
        try:
            if len(value) == 1:
                value = value[0]
                try:
                    value = value.item()
                except AttributeError:
                    pass
                if isinstance(value, (tuple, list)):
                    value = pyarrow.array(value)
        except TypeError:
            # Scalar literal (e.g. int/float/bool) - keep as-is.
            pass

    dict_candidate = _has_dictionary_candidate(arr)
    numeric_dict_candidate = (
        dict_candidate
        and operator in _DICT_NUMERIC_FASTPATH_OPS
        and _dictionary_supports_numeric_fastpath(arr)
    )

    if dict_candidate and operator not in _DICT_FASTPATH_OPS and not numeric_dict_candidate:
        raise NotImplementedError(f"Dictionary motor path does not support operator `{operator}`.")

    if operator == "Eq":
        if dict_candidate:
            fast = _dictionary_fastpath(arr, operator, value)
            if fast is not None:
                _record_dict_hit()
                return fast
            raise RuntimeError("Dictionary fastpath failed for `Eq`.")
        return compute.equal(arr, value).to_numpy(False).astype(dtype=numpy.bool_)
    if operator == "NotEq":
        if dict_candidate:
            fast = _dictionary_fastpath(arr, operator, value)
            if fast is not None:
                _record_dict_hit()
                return fast
            raise RuntimeError("Dictionary fastpath failed for `NotEq`.")
        return compute.not_equal(arr, value).to_numpy(False).astype(dtype=numpy.bool_)
    if operator == "Lt":
        if numeric_dict_candidate:
            fast = _dictionary_fastpath(arr, operator, value)
            if fast is not None:
                _record_dict_hit()
                return fast
            raise RuntimeError("Dictionary fastpath failed for `Lt`.")
        return compute.less(arr, value).to_numpy(False).astype(dtype=numpy.bool_)
    if operator == "Gt":
        if numeric_dict_candidate:
            fast = _dictionary_fastpath(arr, operator, value)
            if fast is not None:
                _record_dict_hit()
                return fast
            raise RuntimeError("Dictionary fastpath failed for `Gt`.")
        return compute.greater(arr, value).to_numpy(False).astype(dtype=numpy.bool_)
    if operator == "LtEq":
        if numeric_dict_candidate:
            fast = _dictionary_fastpath(arr, operator, value)
            if fast is not None:
                _record_dict_hit()
                return fast
            raise RuntimeError("Dictionary fastpath failed for `LtEq`.")
        return compute.less_equal(arr, value).to_numpy(False).astype(dtype=numpy.bool_)
    if operator == "GtEq":
        if numeric_dict_candidate:
            fast = _dictionary_fastpath(arr, operator, value)
            if fast is not None:
                _record_dict_hit()
                return fast
            raise RuntimeError("Dictionary fastpath failed for `GtEq`.")
        return compute.greater_equal(arr, value).to_numpy(False).astype(dtype=numpy.bool_)
    if operator == "InList":
        if dict_candidate:
            fast = _dictionary_fastpath(arr, operator, value)
            if fast is not None:
                _record_dict_hit()
                return fast
            raise RuntimeError("Dictionary fastpath failed for `InList`.")
        from opteryx.draken.interop.arrow import vector_from_arrow

        to_pylist = getattr(value, "to_pylist", None)
        if to_pylist is not None:
            value = to_pylist()

        to_numpy = getattr(value, "to_numpy", None)
        if to_numpy is not None:
            value = to_numpy(zero_copy_only=False)

        values = set(value)

        if isinstance(arr, numpy.ndarray):
            arr = pyarrow.array(arr)

        if isinstance(arr, pyarrow.ChunkedArray):
            arr = arr.combine_chunks()

        if isinstance(arr, pyarrow.Array):
            arr = vector_from_arrow(arr)

        return list_ops.list_in_list(arr, values)
    if operator == "NotInList":
        if dict_candidate:
            fast = _dictionary_fastpath(arr, operator, value)
            if fast is not None:
                _record_dict_hit()
                return fast
            raise RuntimeError("Dictionary fastpath failed for `NotInList`.")
        from opteryx.draken.interop.arrow import vector_from_arrow

        to_pylist = getattr(value, "to_pylist", None)
        if to_pylist is not None:
            value = to_pylist()

        to_numpy = getattr(value, "to_numpy", None)
        if to_numpy is not None:
            value = to_numpy(zero_copy_only=False)

        values = set(value)

        if isinstance(arr, numpy.ndarray):
            arr = pyarrow.array(arr)

        if isinstance(arr, pyarrow.ChunkedArray):
            arr = arr.combine_chunks()

        if isinstance(arr, pyarrow.Array):
            arr = vector_from_arrow(arr)

        matches = list_ops.list_in_list(arr, values)
        return numpy.invert(matches.astype(dtype=numpy.bool_))
    if operator == "InStr":
        needle = str(value)
        if not isinstance(arr, (pyarrow.Array, pyarrow.ChunkedArray)):
            arr = pyarrow.array(arr, type=pyarrow.binary())
        matches = list_ops.list_in_string(arr, needle)
        return numpy.frombuffer(matches, dtype=numpy.bool_)
    if operator == "NotInStr":
        needle = str(value)
        if not isinstance(arr, (pyarrow.Array, pyarrow.ChunkedArray)):
            arr = pyarrow.array(arr, type=pyarrow.binary())
        matches = list_ops.list_in_string(arr, needle)
        matches = numpy.frombuffer(matches, dtype=numpy.bool_)
        return numpy.invert(matches)
    if operator == "IInStr":
        needle = str(value)
        if not isinstance(arr, (pyarrow.Array, pyarrow.ChunkedArray)):
            arr = pyarrow.array(arr, type=pyarrow.binary())
        matches = list_ops.list_in_string_case_insensitive(arr, needle)
        return numpy.frombuffer(matches, dtype=numpy.bool_)
    if operator == "NotIInStr":
        needle = str(value)
        if not isinstance(arr, (pyarrow.Array, pyarrow.ChunkedArray)):
            arr = pyarrow.array(arr, type=pyarrow.binary())
        matches = list_ops.list_in_string_case_insensitive(arr, needle)
        matches = numpy.frombuffer(matches, dtype=numpy.bool_)
        return numpy.invert(matches)
    if operator == "Like":
        if dict_candidate:
            fast = _dictionary_fastpath(arr, operator, value)
            if fast is not None:
                _record_dict_hit()
                return fast
            raise RuntimeError("Dictionary fastpath failed for `Like`.")
        return compute.match_like(arr, value).to_numpy(False).astype(dtype=numpy.bool_)
    if operator == "NotLike":
        if dict_candidate:
            fast = _dictionary_fastpath(arr, operator, value)
            if fast is not None:
                _record_dict_hit()
                return fast
            raise RuntimeError("Dictionary fastpath failed for `NotLike`.")
        matches = compute.match_like(arr, value).to_numpy(False).astype(dtype=numpy.bool_)
        return numpy.invert(matches)
    if operator == "ILike":
        if dict_candidate:
            fast = _dictionary_fastpath(arr, operator, value)
            if fast is not None:
                _record_dict_hit()
                return fast
            raise RuntimeError("Dictionary fastpath failed for `ILike`.")
        return (
            compute.match_like(arr, value, ignore_case=True)
            .to_numpy(False)
            .astype(dtype=numpy.bool_)
        )
    if operator == "NotILike":
        if dict_candidate:
            fast = _dictionary_fastpath(arr, operator, value)
            if fast is not None:
                _record_dict_hit()
                return fast
            raise RuntimeError("Dictionary fastpath failed for `NotILike`.")
        matches = compute.match_like(arr, value, ignore_case=True)
        return numpy.invert(matches)
    if operator == "RLike":
        if dict_candidate:
            fast = _dictionary_fastpath(arr, operator, value)
            if fast is not None:
                _record_dict_hit()
                return fast
            raise RuntimeError("Dictionary fastpath failed for `RLike`.")
        return compute.match_substring_regex(arr, value).to_numpy(False).astype(dtype=numpy.bool_)
    if operator == "NotRLike":
        if dict_candidate:
            fast = _dictionary_fastpath(arr, operator, value)
            if fast is not None:
                _record_dict_hit()
                return fast
            raise RuntimeError("Dictionary fastpath failed for `NotRLike`.")
        matches = compute.match_substring_regex(arr, value)  # [#325]
        return numpy.invert(matches)
    if operator == "AnyOpEq":
        return list_ops.list_anyop_eq(literal=arr[0], column=value)
    if operator == "AnyOpNotEq":
        return list_ops.list_anyop_neq(literal=arr[0], column=value)
    if operator == "AnyOpGt":
        return list_ops.list_anyop_gt(arr[0], value)
    if operator == "AnyOpLt":
        return list_ops.list_anyop_lt(arr[0], value)
    if operator == "AnyOpGtEq":
        return list_ops.list_anyop_gte(arr[0], value)
    if operator == "AnyOpLtEq":
        return list_ops.list_anyop_lte(arr[0], value)
    if operator == "AllOpEq":
        return list_ops.list_allop_eq(arr[0], value)
    if operator == "AllOpNotEq":
        return list_ops.list_allop_neq(arr[0], value)

    if operator == "AnyOpILike":
        from opteryx.utils.sql import regex_match_any

        return regex_match_any(arr, value, flags=re.IGNORECASE)

    if operator == "AnyOpLike":
        from opteryx.utils.sql import regex_match_any

        return regex_match_any(arr, value)

    if operator == "AnyOpNotLike":
        from opteryx.utils.sql import regex_match_any

        return regex_match_any(arr, value, invert=True)

    if operator == "AnyOpNotILike":
        from opteryx.utils.sql import regex_match_any

        return regex_match_any(arr, value, flags=re.IGNORECASE, invert=True)

    if operator == "AtQuestion":
        from opteryx.third_party.tktech import csimdjson as simdjson

        to_numpy = getattr(arr, "to_numpy", None)
        if to_numpy is not None:
            arr = to_numpy(zero_copy_only=False)

        parser = simdjson.Parser()

        if not value.startswith("$."):
            # Not a JSONPath, treat as a simple key existence check
            return pyarrow.array(
                [value in parser.parse(doc).keys() for doc in arr],
                type=pyarrow.bool_(),  # type: ignore
            )

        # Convert "$.key1.list[0]" to JSON Pointer "/key1/list/0"
        def jsonpath_to_pointer(jsonpath: str) -> str:
            # Remove "$." prefix
            json_pointer = jsonpath[1:]
            # Replace "." with "/" for dict navigation
            json_pointer = json_pointer.replace(".", "/")
            # Replace "[index]" with "/index" for list access
            json_pointer = json_pointer.replace("[", "/").replace("]", "")
            return json_pointer

        # Convert "$.key1.key2" to JSON Pointer "/key1/key2"
        json_pointer = jsonpath_to_pointer(value)

        def check_json_pointer(doc, pointer):
            try:
                # Try accessing the path via JSON Pointer
                parser.parse(doc).at_pointer(pointer)
                return True  # If successful, the path exists
            except Exception:
                return False  # If an error occurs, the path does not exist

        # Apply the JSON Pointer check
        return pyarrow.array(
            [check_json_pointer(doc, json_pointer) for doc in arr],
            type=pyarrow.bool_(),
        )

    if operator == "AtArrow":
        from opteryx.compiled.list_ops import list_contains_any

        if len(arr) == 0:
            return numpy.array([], dtype=numpy.bool_)

        if len(arr) == 1:
            # Fixed: Handle None element
            elem = arr[0]
            if elem is None:
                return numpy.array([False], dtype=numpy.bool_)

            value_set = set(value) if value is not None else set()
            try:
                elem_set = set(elem)
            except TypeError:
                elem_set = {elem}

            result = bool(elem_set.intersection(value_set))
            return numpy.array([result], dtype=numpy.bool_)

        to_numpy = getattr(arr, "to_numpy", None)
        if to_numpy is not None:
            arr = to_numpy(zero_copy_only=False)

        to_pylist = getattr(value, "to_pylist", None)
        if to_pylist is not None:
            value = to_pylist()

        return list_contains_any(arr, set(value))

    if operator == "ArrayContainsAll":
        from opteryx.compiled.list_ops import list_contains_all

        to_pylist = getattr(value, "to_pylist", None)
        if to_pylist is not None:
            value = to_pylist()

        to_numpy = getattr(arr, "to_numpy", None)
        if to_numpy is not None:
            arr = to_numpy(zero_copy_only=False)

        if len(arr) == 1 and len(value) != 0:
            raise ValueError("Unable to execute @>>, check form matches `column @>> (values)`.")

        return list_contains_all(arr, set(value))

    raise NotImplementedError(f"Operator {operator} is not implemented!")  # pragma: no cover
