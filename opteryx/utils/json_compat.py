"""
JSON compatibility layer with performance fallback chain:

1. orjson (fastest, but not available on free-threaded Python 3.14t)
2. simdjson (fast parsing, vendored, always available)
3. stdlib json (slowest, always available)

For serialization (dumps): orjson -> stdlib json
For deserialization (loads): orjson -> simdjson -> stdlib json
"""

import sys

# Try orjson first (fastest overall)
try:
    import orjson

    HAS_ORJSON = True

    def dumps(obj, default=None, option=None):
        """Serialize obj to JSON bytes using orjson."""
        if option is not None:
            return orjson.dumps(obj, default=default, option=option)
        return orjson.dumps(obj, default=default)

    def loads(data):
        """Deserialize JSON bytes/str to Python object using orjson."""
        return orjson.loads(data)

    # Export orjson options
    OPT_INDENT_2 = orjson.OPT_INDENT_2
    OPT_SORT_KEYS = orjson.OPT_SORT_KEYS

except ImportError:
    import json

    HAS_ORJSON = False

    # Try simdjson for parsing (much faster than stdlib json)
    try:
        from opteryx.third_party.tktech.csimdjson import Parser

        _simdjson_parser = Parser()
        HAS_SIMDJSON = True

        def loads(data):
            """Deserialize JSON bytes/str to Python object using simdjson."""
            # Parse with recursive=True to return full Python objects
            return _simdjson_parser.parse(data, recursive=True)

    except (ImportError, Exception):
        # simdjson not available or failed to initialize
        HAS_SIMDJSON = False

        def loads(data):
            """Deserialize JSON string/bytes to Python object using stdlib json."""
            if isinstance(data, bytes):
                data = data.decode("utf-8")
            return json.loads(data)

    # Emit warning once about performance
    if not getattr(sys, "_opteryx_json_warning_shown", False):
        import warnings

        if HAS_SIMDJSON:
            warnings.warn(
                "orjson not available, using simdjson for parsing and stdlib json for serialization. "
                "This is expected on free-threaded Python 3.14t.",
                ImportWarning,
                stacklevel=2,
            )
        else:
            warnings.warn(
                "orjson and simdjson not available, falling back to stdlib json (slower performance). "
                "This is expected on free-threaded Python 3.14t.",
                ImportWarning,
                stacklevel=2,
            )
        sys._opteryx_json_warning_shown = True

    def dumps(obj, default=None, option=None):
        """Serialize obj to JSON bytes using stdlib json."""
        kwargs = {}
        if default is not None:
            kwargs["default"] = default
        if option == OPT_INDENT_2:
            kwargs["indent"] = 2
        if option == OPT_SORT_KEYS or (option is not None and option & OPT_SORT_KEYS):
            kwargs["sort_keys"] = True

        result = json.dumps(obj, **kwargs)
        # Return bytes to match orjson's interface
        return result.encode("utf-8")

    # Define option constants (as bit flags like orjson)
    OPT_INDENT_2 = 1 << 0
    OPT_SORT_KEYS = 1 << 1


__all__ = ["dumps", "loads", "HAS_ORJSON", "OPT_INDENT_2", "OPT_SORT_KEYS"]
