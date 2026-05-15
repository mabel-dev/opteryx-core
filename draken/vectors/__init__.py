from array import array

# Fallback helper for environments where the Cython `compress` method
# isn't available (e.g., before a local extension rebuild). Tests and
# callers can use this to get the compressed int64 representation.
from opteryx.compiled.structures.relation_statistics import to_int

def _compress_vector(vec):
    """Return an array('q') with the compressed int64 values for `vec`.

    This is a pure-Python fallback; preferred fast path is the Cython
    `vec.compress()` cpdef once the extension has been rebuilt.
    """
    n = len(vec)
    if n == 0:
        return array("q")

    try:
        vals = vec.to_pylist()
    except Exception:
        vals = [vec[i] for i in range(n)]

    # Date32 to_pylist() returns datetime.date objects; pass directly to to_int.
    if vec.__class__.__name__ == "Date32Vector":
        return array("q", [to_int(None) if v is None else to_int(v) for v in vals])

    return array("q", [to_int(v) for v in vals])


__all__ = ["_compress_vector"]
