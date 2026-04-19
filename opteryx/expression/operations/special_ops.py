"""Special filter operations (JSON path, etc.)."""


def json_path_exists(arr, value):
    """Check if JSON path exists in document (AtQuestion operator @?).

    Returns BoolVector to match registrar expectations.
    """
    from opteryx.compiled.draken.interop.vector_sequence import vector_from_sequence
    from opteryx.third_party import yyjson

    # Convert to list of strings for processing
    if hasattr(arr, "tolist"):
        arr = arr.tolist()
    elif not isinstance(arr, (list, tuple)):
        arr = list(arr)

    parser = yyjson.Parser()

    if not value.startswith("$."):
        # Not a JSONPath, treat as a simple key existence check
        result = [value in parser.parse(doc) for doc in arr]
        return vector_from_sequence(result)

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

    result = [check_json_pointer(doc, json_pointer) for doc in arr]
    return vector_from_sequence(result)
