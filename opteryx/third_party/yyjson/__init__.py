"""Minimal yyjson package shim.

This module intentionally fails fast if the Cython extension is not available.
Per .claude/CLAUDE.md: no hidden fallbacks, no try/except-imports, and no Python-only
fallback implementations for Cython functionality.
"""

from .cyyjson import Parser

# Option constants (kept for compatibility)
OPT_INDENT_2 = 1 << 0
OPT_SORT_KEYS = 1 << 1

# Indicate orjson is not present
HAS_ORJSON = False

__all__ = ["Parser", "loads", "dumps", "HAS_ORJSON", "OPT_INDENT_2", "OPT_SORT_KEYS"]


def loads(s):
    """Deserialize JSON using the Cython Parser.

    This will raise ImportError if the cyyjson extension is not built.
    """
    return Parser().parse(s)


def dumps(obj, default=None, option=None):
    """Serialize to JSON bytes using the Cython Parser's dump implementation.

    No stdlib fallback is provided here; callers must ensure the extension is built.
    """
    return Parser().dump(obj)
