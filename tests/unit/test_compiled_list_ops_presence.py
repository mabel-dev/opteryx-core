"""Presence checks for the compiled extension symbols the engine depends on.

These exist to fail loudly and early when a symbol moves module or a build is
partial, rather than letting the failure surface deep in query execution.

Symbols are asserted at their REAL homes. In particular the hash and array
membership kernels live in the `opteryx.compiled.nanobind.vectors` nanobind
extension, NOT in `opteryx.compiled.vector_ops` — that module is an
auto-generated consolidation of the regex/DFA/case-helper pyx sources only
(see the generated header in vector_ops.pyx).

Imports here are direct rather than attribute probes: a missing symbol raises
ImportError at collection, which names the symbol and the module.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest


def test_nanobind_vectors_exposes_hash_kernels():
    from opteryx.compiled.nanobind.vectors import (  # noqa: F401
        vector_md5,
        vector_sha1,
        vector_sha224,
        vector_sha256,
        vector_sha384,
        vector_sha512,
    )

    assert callable(vector_md5)
    assert callable(vector_sha256)


def test_nanobind_vectors_exposes_array_membership_kernels():
    """Imported by opteryx/expression/evaluator/json_ops.pyx."""
    from opteryx.compiled.nanobind.vectors import (  # noqa: F401
        vector_contains_all,
        vector_contains_any,
    )

    assert callable(vector_contains_all)
    assert callable(vector_contains_any)


def test_vector_ops_exposes_the_regex_and_dfa_kernels():
    """vector_ops is the consolidated regex/DFA module — these are its symbols."""
    from opteryx.compiled.vector_ops import (  # noqa: F401
        compile_like_program,
        compile_rlike_program,
        vector_dfa_extract,
        vector_like,
        vector_rlike,
    )

    assert callable(vector_like)
    assert callable(vector_rlike)


def test_hash_kernels_are_not_on_vector_ops():
    """Pins where these DON'T live, so the old wrong import cannot creep back."""
    import opteryx.compiled.vector_ops as vector_ops

    with pytest.raises(AttributeError):
        vector_ops.vector_md5  # noqa: B018


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-v"]))
