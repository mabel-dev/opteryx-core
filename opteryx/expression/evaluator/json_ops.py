"""JSON and array vector operations.

Operations on vectors of JSON-encoded objects and arrays.
"""

from draken.interop.vector_sequence import vector_from_sequence


def _json_at_question(left, right):
    """Check if JSON pointer path exists in document strings.

    Iterates element-wise through the vector instead of materializing to list.

    Args:
        left: StringVector containing JSON-encoded documents
        right: JSON pointer path (string) to check for existence

    Returns:
        BoolVector with True where path exists, None where doc is None
    """
    from opteryx.third_party import yyjson

    parser = yyjson.Parser()
    path = right
    n = len(left)

    if path.startswith("$."):
        # JSONPath format ($.foo.bar)
        result = [None if (doc := left[i]) is None else path in parser.parse(doc) for i in range(n)]
    else:
        # JSON Pointer format (/foo/bar)
        json_pointer = path[1:].replace(".", "/").replace("[", "/").replace("]", "")

        def _check_path(doc):
            if doc is None:
                return None
            try:
                parser.parse(doc).at_pointer(json_pointer)
                return True
            except Exception:
                return False

        result = [_check_path(left[i]) for i in range(n)]

    return vector_from_sequence(result)


def _json_at_arrow(left, right):
    """Check if JSON array contains any of the specified items (@ operator).

    Args:
        left: ArrayVector containing arrays
        right: Set/list of items to check for

    Returns:
        BoolVector with True where array contains any item
    """
    from opteryx.compiled.vector_ops import vector_contains_any

    items = set(right) if right is not None else set()
    items = {v.encode() if isinstance(v, str) else v for v in items}
    return vector_contains_any(left, items)


def _json_array_contains_all(left, right):
    """Check if JSON array contains all of the specified items.

    Args:
        left: ArrayVector containing arrays
        right: Set/list of items to check for

    Returns:
        BoolVector with True where array contains all items
    """
    from opteryx.compiled.vector_ops import vector_contains_all

    items = set(right) if right is not None else set()
    items = {v.encode() if isinstance(v, str) else v for v in items}
    return vector_contains_all(left, items)
