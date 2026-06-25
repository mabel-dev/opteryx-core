"""JSON and array vector operations.

Cython migration of the former json_ops.py. Called from comparisons.pyx
for the @>, @?, AtArrow and AtQuestion operators on ArrayVector /
StringVector-of-JSON columns.
"""

from opteryx.compiled.nanobind.vectors import (
    vector_contains_all,
    vector_contains_any,
)
from opteryx.third_party import yyjson

from draken.draken_native import vector_from_sequence
from draken.draken_native import vector_from_bool_sequence
from draken.vectors.bool_vector import BoolVector


cpdef _json_at_question(left, right):
    """Check whether each JSON document in `left` contains the path `right`.

    Returns a Draken vector parallel to `left`; entries are True/False where
    the doc was non-null, and None where the doc was null.

    `right` is the path literal. It may arrive as a str, bytes, or a constant
    Vector (the executor materialises scalar literals as constant vectors); all
    are coerced to a single path string here.
    """
    cdef Py_ssize_t n = len(left)
    cdef Py_ssize_t i
    cdef list result
    cdef str json_pointer
    cdef str right_path

    # Coerce the path operand to a scalar str.
    if isinstance(right, str):
        right_path = right
    elif isinstance(right, (bytes, bytearray)):
        right_path = bytes(right).decode("utf-8")
    else:
        # Vector / sequence carrying the constant literal — take row 0.
        scalar = right[0] if len(right) else None
        if scalar is None:
            raise ValueError("@? requires a non-null path literal")
        right_path = scalar.decode("utf-8") if isinstance(scalar, (bytes, bytearray)) else str(scalar)
    right = right_path

    parser = yyjson.Parser()

    # Normalise the path to an RFC-6901 JSON Pointer once. Accepts a bare key
    # ("a"), dotted path ("a.b[0]"), JSONPath ("$.a.b"), or an existing pointer
    # ("/a/b"). at_pointer() returns the value for a present path (even when the
    # value is JSON null) and None for a missing path — so existence is
    # `at_pointer(ptr) is not None`.
    cdef str pp = right_path
    if pp.startswith("$"):
        pp = pp[1:]
    if pp.startswith("/"):
        json_pointer = pp
    else:
        pp = pp.lstrip(".")
        json_pointer = "/" + pp.replace(".", "/").replace("[", "/").replace("]", "")

    result = [None] * n
    for i in range(n):
        doc = left[i]
        if doc is None:
            result[i] = None
            continue
        try:
            result[i] = parser.parse(doc).at_pointer(json_pointer) is not None
        except Exception:
            # Malformed path/doc → treat as "does not contain".
            result[i] = False

    # Must be a BOOL vector AND a Cython Vector wrapper: the comparison executor
    # casts the result to `Vector` and calls `.unified()`, so a raw nanobind
    # vector (what vector_from_bool_sequence returns) would crash. Wrap it.
    return BoolVector(vector_from_bool_sequence(result))


cdef set _encode_items(right):
    """Coerce a Python iterable of items into a set of byte/native values."""
    if right is None:
        return set()
    cdef set out = set()
    for v in right:
        if isinstance(v, str):
            out.add(v.encode())
        else:
            out.add(v)
    return out


cpdef _json_at_arrow(left, right):
    """ArrayVector @> any-of: True where the row's array contains any item."""
    cdef set items = _encode_items(right)
    return vector_contains_any(left, items)


cpdef _json_array_contains_all(left, right):
    """ArrayVector contains-all: True where the row's array contains all items."""
    cdef set items = _encode_items(right)
    return vector_contains_all(left, items)
