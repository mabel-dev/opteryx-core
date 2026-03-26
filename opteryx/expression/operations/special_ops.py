"""Special filter operations (JSON path, etc.)."""

import pyarrow


def json_path_exists(arr, value):
    """Check if JSON path exists in document (AtQuestion operator @?)."""
    from opteryx.third_party.tktech import csimdjson as simdjson

    to_numpy = getattr(arr, "to_numpy", None)
    if to_numpy is not None:
        arr = to_numpy(zero_copy_only=False)

    parser = simdjson.Parser()

    if not value.startswith("$."):
        # Not a JSONPath, treat as a simple key existence check
        return pyarrow.array(
            [value in parser.parse(doc) for doc in arr],
            type=pyarrow.bool_(),
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

    json_pointer = jsonpath_to_pointer(value)

    def check_json_pointer(doc, pointer):
        try:
            parser.parse(doc).at_pointer(pointer)
            return True
        except Exception:
            return False

    return pyarrow.array(
        [check_json_pointer(doc, json_pointer) for doc in arr],
        type=pyarrow.bool_(),
    )
