"""JSON vector operations.

Operations on vectors of JSON-encoded objects.
"""

from opteryx.compiled.draken.interop.arrow import vector_from_sequence


def _json_at_question(left, right):
    """Check if JSON pointer path exists in document strings.

    Args:
        left: StringVector containing JSON-encoded documents
        right: JSON pointer path (string) to check for existence

    Returns:
        BoolVector with True where path exists, False/None where it doesn't
    """
    from opteryx.third_party.tktech import csimdjson as simdjson

    docs = left.to_pylist()
    path = right
    parser = simdjson.Parser()

    if path.startswith("$."):
        result = [None if doc is None else path in parser.parse(doc) for doc in docs]
    else:

        def _pointer(jsonpath: str) -> str:
            return jsonpath[1:].replace(".", "/").replace("[", "/").replace("]", "")

        json_pointer = _pointer(path)

        def _check(doc):
            if doc is None:
                return None
            try:
                parser.parse(doc).at_pointer(json_pointer)
                return True
            except Exception:
                return False

        result = [_check(doc) for doc in docs]

    return vector_from_sequence(result)
