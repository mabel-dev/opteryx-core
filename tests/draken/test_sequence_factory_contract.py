"""Contract tests for draken's Python-facing sequence factories.

These pin the three things an external consumer tripped over, because each is a
contract the docstrings now state explicitly:

1. The scalar string factories are BYTES-ONLY and must keep rejecting `str` —
   a Python str must not reach the native edge (engineering contract §1).
2. The ARRAY factory accepts BOTH `str` and `bytes` children, deliberately,
   because there str/bytes is what SELECTS the child type (VARCHAR vs VARBINARY).
3. `element_type` accepts draken's own `DrakenType` enum as well as a raw int.

Plus the wrapper that reconciles (1) for list[str] callers is importable from
the package, not just from the engine's private import path.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import draken
import draken.draken_native as dn
import draken.interop
from draken.vectors.vector import Vector

BYTES_ONLY_STRING_FACTORIES = [
    "vector_from_string_sequence",
    "vector_from_nvarchar_sequence",
    "vector_from_string_dict_sequence",
]


@pytest.mark.parametrize("factory", BYTES_ONLY_STRING_FACTORIES)
def test_scalar_string_factories_reject_str(factory):
    """str must not reach the native edge — the docstring promises this refusal."""
    with pytest.raises(ValueError, match="not bytes or None"):
        getattr(dn, factory)(["a"])


@pytest.mark.parametrize("factory", BYTES_ONLY_STRING_FACTORIES)
def test_scalar_string_factories_accept_bytes(factory):
    assert getattr(dn, factory)([b"a", None]) is not None


def test_array_factory_accepts_str_child_as_varchar():
    vec = Vector(dn.vector_array_from_sequence([["a", "bb"], None]))
    assert vec.to_pylist() == [["a", "bb"], None]


def test_array_factory_accepts_bytes_child_as_varbinary():
    """bytes children stay opaque binary — NOT decoded as UTF-8."""
    vec = Vector(dn.vector_array_from_sequence([[b"a", b"bb"], None]))
    assert vec.to_pylist() == [[b"a", b"bb"], None]


def test_array_element_type_accepts_draken_enum():
    """The factory must not refuse draken's own type enum."""
    vec = Vector(
        dn.vector_array_from_sequence(
            [[None]], element_type=dn.DrakenType.VARCHAR, nesting_depth=1
        )
    )
    assert vec.to_pylist() == [[None]]


def test_array_element_type_enum_and_int_agree():
    as_enum = dn.vector_array_from_sequence(
        [[None]], element_type=dn.DrakenType.VARCHAR, nesting_depth=1
    )
    as_int = dn.vector_array_from_sequence(
        [[None]], element_type=int(dn.DrakenType.VARCHAR.value), nesting_depth=1
    )
    assert Vector(as_enum).to_pylist() == Vector(as_int).to_pylist()


def test_array_element_type_omitted_is_still_inferred():
    """Default must keep the legacy 'not supplied' behaviour."""
    assert Vector(dn.vector_array_from_sequence([[1, 2]])).to_pylist() == [[1, 2]]
    assert Vector(dn.vector_array_from_sequence([[1, 2]], element_type=-1)).to_pylist() == [[1, 2]]


def test_array_element_type_rejects_nonsense():
    with pytest.raises(TypeError, match="element_type must be a DrakenType"):
        dn.vector_array_from_sequence([[1]], element_type="VARCHAR")


def test_vector_from_sequence_is_exported_from_the_package():
    """The str-encoding wrapper must be reachable without reading engine Cython."""
    assert draken.vector_from_sequence is not None
    assert draken.interop.vector_from_sequence is draken.vector_from_sequence


def test_exported_wrapper_encodes_str_for_the_bytes_only_edge():
    vec = Vector(draken.vector_from_sequence(["a", "b", None], "VARCHAR"))
    assert vec.to_pylist() == ["a", "b", None]


def test_exported_wrapper_takes_the_draken_enum_as_dtype():
    vec = Vector(draken.vector_from_sequence(["a", None], dn.DrakenType.VARCHAR))
    assert vec.to_pylist() == ["a", None]


def test_native_int64_builder_is_named_for_its_type():
    """The no-suffix native name is GONE — it collided with the wrapper.

    `vector_from_sequence` must mean the dispatching wrapper and nothing else;
    the raw INT64 constructor is spelled like its int8/16/32 siblings.
    """
    assert draken.vector_from_sequence is not dn.vector_int64_from_sequence
    with pytest.raises(AttributeError):
        dn.vector_from_sequence  # noqa: B018 — the old colliding name must be gone
    assert dn.vector_int64_from_sequence([1, 2, None]) is not None


if __name__ == "__main__":  # pragma: no cover
    import pytest as _pytest

    raise SystemExit(_pytest.main([__file__, "-v"]))
