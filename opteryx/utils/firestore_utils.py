"""Helpers for preparing Python objects for Firestore writes.

This module provides a small sanitizer that coerces common numeric
and array types (notably NumPy) to native Python types that the
Firestore client accepts.
"""

from typing import Any


def sanitize_for_firestore(value: Any) -> Any:
    """Recursively convert values that Firestore cannot handle.

    - NumPy scalar -> native Python scalar via `.item()`
    - NumPy ndarray -> Python list via `.tolist()`
    - dict/list/tuple/set are traversed and sanitized recursively

    If NumPy is not available the function is a no-op for numeric types.
    """
    try:
        import numpy as _np
    except Exception:
        _np = None

    # Dict-like
    if isinstance(value, dict):
        return {k: sanitize_for_firestore(v) for k, v in value.items()}

    # Iterable containers -> lists (Firestore expects lists)
    if isinstance(value, (list, tuple, set)):
        return [sanitize_for_firestore(v) for v in value]

    # NumPy: scalars and arrays
    if _np is not None:
        if isinstance(value, _np.generic):
            try:
                return value.item()
            except Exception:
                # Fallback to cast to Python int/float/bool where possible
                try:
                    if _np.issubdtype(value.dtype, _np.integer):
                        return int(value)
                    if _np.issubdtype(value.dtype, _np.floating):
                        return float(value)
                    if _np.issubdtype(value.dtype, _np.bool_):
                        return bool(value)
                except Exception:
                    pass
        if isinstance(value, _np.ndarray):
            try:
                return value.tolist()
            except Exception:
                # fall through to default
                pass

    # Primitive types that Firestore accepts unchanged
    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    # Fallback: return the value unchanged; Firestore client will raise if unsupported
    return value
