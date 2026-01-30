from array import array

# Fallback helper for environments where the Cython `compress` method
# isn't available (e.g., before a local extension rebuild). Tests and
# callers can use this to get the compressed int64 representation.
from opteryx.compiled.structures.relation_statistics import to_int

from .bool_mask import BoolMask


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

    # Date32 vectors store days since epoch as integers; convert those to
    # date objects before calling `to_int` so the result matches datetime
    # scaling (microseconds since epoch, including local timezone handling).
    if vec.__class__.__name__ == "Date32Vector":
        import datetime

        base = datetime.date(1970, 1, 1)
        return array(
            "q",
            [
                to_int(None) if v is None else to_int(base + datetime.timedelta(days=int(v)))
                for v in vals
            ],
        )

    return array("q", [to_int(v) for v in vals])


__all__ = ["BoolMask", "_compress_vector"]
