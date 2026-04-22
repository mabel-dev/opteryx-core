"""Minimal yyjson package shim.

This module intentionally fails fast if the Cython extension is not available.
Per .claude/CLAUDE.md: no hidden fallbacks, no try/except-imports, and no Python-only
fallback implementations for Cython functionality.
"""

from .cyyjson import Parser

# Option constants (kept for compatibility)
OPT_INDENT_2 = 1 << 0
OPT_SORT_KEYS = 1 << 1

__all__ = ["Parser", "loads", "dumps", "OPT_INDENT_2", "OPT_SORT_KEYS"]


def loads(s):
    """Deserialize JSON using the Cython Parser.

    This will raise ImportError if the cyyjson extension is not built.
    """
    return Parser().parse(s)


def dumps(obj, default=None, option=None):
    """Serialize to JSON bytes using the Cython Parser's mutable API.

    Args:
        obj: Python object to serialize
        default: Optional callable(obj) for non-serializable types
        option: yyjson write flags (e.g., OPT_INDENT_2)

    Returns:
        JSON as bytes
    """
    return Parser().dumps(obj, default_handler=default, options=option or 0)
