# cython: language_level=3
"""Stub for opteryx.compiled.morsel_ops.distinct — E.24 bridge.

morsel_ops.distinct requires Morsel.c_hash() which is deferred to E.21b.
Providing an importable stub prevents the import error from cascading to
break all tests. DISTINCT queries will raise NotImplementedError at runtime.
"""


def distinct(morsel, seen_hashes, columns=None):
    """Filter a Draken Morsel to distinct rows, in place.

    Not yet ported to the new DrakenVector/Morsel shim API (deferred E.21b).
    """
    raise NotImplementedError(
        "DISTINCT (morsel_ops.distinct) requires Morsel.c_hash() — deferred to E.21b"
    )
