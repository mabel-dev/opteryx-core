"""yyjson package shim exposing a simple API compatible with previous json_compat."""
import json

try:
    from . import cyyjson as _cyy
except Exception:
    _cyy = None


def _ensure_cyy():
    global _cyy
    if _cyy is None:
        from importlib import import_module
        _cyy = import_module("opteryx.third_party.yyjson.cyyjson")
    return _cyy


class Parser:
    """Thin wrapper exposing the same methods used by callers."""

    def __init__(self):
        cy = _ensure_cyy()
        self._p = cy.Parser()

    def parse(self, data, recursive=True):
        return self._p.parse(data, recursive=recursive)

    def dump(self, obj):
        return self._p.dump(obj)


# Module-level convenience functions to match the old json_compat API
def loads(s):
    cy = _ensure_cyy()
    return cy.Parser().parse(s)


# Option constants (for compatibility)
OPT_INDENT_2 = 1 << 0
OPT_SORT_KEYS = 1 << 1


def dumps(obj, default=None, option=None):
    """Serialize to JSON bytes. Supports optional 'default' and 'option' flags for compatibility.

    Prefer the underlying cyyjson serializer when available; fall back to stdlib json.
    """
    try:
        cy = _ensure_cyy()
        # cy.Parser().dump currently serializes via json.dumps internally and returns bytes
        return cy.Parser().dump(obj)
    except Exception:
        kwargs = {}
        if default is not None:
            kwargs["default"] = default
        if option == OPT_INDENT_2:
            kwargs["indent"] = 2
        if option == OPT_SORT_KEYS or (option is not None and option & OPT_SORT_KEYS):
            kwargs["sort_keys"] = True
        result = json.dumps(obj, **kwargs)
        return result.encode("utf-8")


# Backwards compat: expose HAS_ORJSON flag
HAS_ORJSON = False

__all__ = ["dumps", "loads", "Parser", "HAS_ORJSON", "OPT_INDENT_2", "OPT_SORT_KEYS"]