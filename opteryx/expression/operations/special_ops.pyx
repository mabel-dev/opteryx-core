"""Special filter operations (JSON path, etc.)."""

from opteryx.third_party import yyjson

import draken.draken_native as _draken_native_special


cdef str _jsonpath_to_pointer(str jsonpath):
    """Translate `$.key1.list[0]` → `/key1/list/0` (JSON Pointer)."""
    cdef str pointer = jsonpath[1:]
    pointer = pointer.replace(".", "/").replace("[", "/").replace("]", "")
    return pointer


cdef bint _check_json_pointer(parser, doc, str pointer):
    try:
        parser.parse(doc).at_pointer(pointer)
        return True
    except Exception:
        return False


cpdef json_path_exists(arr, str value):
    """Vectorised `@?` (AtQuestion): True per row where `value` resolves in the
    JSON document. Returns a Draken vector.
    """
    # Normalise to a Python sequence we can iterate. Draken vectors expose
    # to_pylist; lists/tuples pass through; everything else gets materialised
    # via list(). hasattr is intentionally avoided per project convention.
    pylist_fn = getattr(arr, "tolist", None)
    if pylist_fn is not None:
        arr = pylist_fn()
    elif not isinstance(arr, (list, tuple)):
        arr = list(arr)

    parser = yyjson.Parser()
    cdef Py_ssize_t n = len(arr)
    cdef Py_ssize_t i
    cdef list result = [False] * n
    cdef str json_pointer

    if not value.startswith("$."):
        # Plain key existence — `in` on the parsed doc is the yyjson contract.
        for i in range(n):
            result[i] = value in parser.parse(arr[i])
        return _draken_native_special.vector_from_sequence(result)

    json_pointer = _jsonpath_to_pointer(value)
    for i in range(n):
        result[i] = _check_json_pointer(parser, arr[i], json_pointer)
    return _draken_native_special.vector_from_sequence(result)
