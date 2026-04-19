"""
JSON compatibility layer using yyjson for both parsing and serialization.

yyjson provides fast JSON parsing and writing with a single vendored library.
"""

import json

from opteryx.third_party.tktech.cyyjson import Parser

_yyjson_parser = Parser()


def loads(data):
    """Deserialize JSON bytes/str to Python object using yyjson."""
    return _yyjson_parser.parse(data)


def dumps(obj, default=None, option=None):
    """Serialize obj to JSON bytes.

    Uses stdlib json for now as yyjson's mutable document API is more complex.
    Can be optimized later if needed.
    """
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


# Option constants (as bit flags like orjson)
OPT_INDENT_2 = 1 << 0
OPT_SORT_KEYS = 1 << 1

# Kept for API compatibility (not used with yyjson)
HAS_ORJSON = False

__all__ = ["dumps", "loads", "HAS_ORJSON", "OPT_INDENT_2", "OPT_SORT_KEYS"]
