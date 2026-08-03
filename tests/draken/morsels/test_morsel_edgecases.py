"""Morsel.take edge cases — empty selections, awkward column contents, and the
index-container forms the engine actually hands to `take`.

Fixtures are built natively (`Morsel.from_vectors` over typed draken vector
constructors). `take` returns a NEW morsel; it does not mutate in place.
PyArrow appears only where a test asserts on Arrow OUTPUT via `to_arrow`.
"""

import array
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import draken.draken_native as dn
from draken.morsels.morsel import Morsel


def test_take_empty_preserves_schema_and_to_arrow():
    """Emptying a morsel keeps its columns and their names, and converts cleanly."""
    morsel = Morsel.from_vectors(
        [b"s", b"i"],
        [dn.vector_from_string_sequence([b"a"]), dn.vector_from_sequence([1])],
    )
    assert morsel.num_rows == 1

    emptied = morsel.take([])
    assert emptied.num_rows == 0
    assert emptied.num_columns == 2
    assert emptied.column_names == [b"s", b"i"]

    out = emptied.to_arrow()
    assert out.num_rows == 0
    assert out.schema.names == ["s", "i"]


def test_take_empty_with_all_null_strings():
    """An all-null string column has no data buffer to gather from — emptying it
    must still produce a well-formed zero-row morsel."""
    morsel = Morsel.from_vectors(
        [b"s", b"i"],
        [
            dn.vector_from_string_sequence([None, None]),
            dn.vector_from_sequence([1, 2]),
        ],
    )
    assert morsel.num_rows == 2

    emptied = morsel.take([])
    assert emptied.num_rows == 0
    assert emptied.column_names == [b"s", b"i"]

    out = emptied.to_arrow()
    assert out.num_rows == 0
    assert out.schema.names == ["s", "i"]


def test_take_empty_with_array_column():
    """ARRAY columns own a nested child; emptying one must not leave it dangling."""
    morsel = Morsel.from_vectors(
        [b"a", b"s"],
        [
            dn.vector_array_from_sequence([[1, 2], None]),
            dn.vector_from_string_sequence([b"x", b"y"]),
        ],
    )

    emptied = morsel.take([])
    assert emptied.num_rows == 0
    assert emptied.column_types == [dn.DrakenType.ARRAY, dn.DrakenType.VARCHAR]

    out = emptied.to_arrow()
    assert out.num_rows == 0
    assert out.schema.names == ["a", "s"]


def test_take_accepts_list_and_typed_array_indices():
    """`take` accepts a plain list of indices and a typed `array('i')` buffer —
    the two forms the engine produces (planner-side lists, and the int32 buffers
    emitted by morsel_sort / join align). Both must gather identically."""
    def _morsel():
        return Morsel.from_vectors(
            [b"s"], [dn.vector_from_string_sequence([b"a", b"b", b"c"])]
        )

    from_list = _morsel().take([0, 2])
    assert from_list.num_rows == 2
    assert from_list.column(b"s").to_pylist() == ["a", "c"]

    from_buffer = _morsel().take(array.array("i", [0, 2]))
    assert from_buffer.num_rows == 2
    assert from_buffer.column(b"s").to_pylist() == ["a", "c"]


def test_take_single_index_preserves_data():
    """A one-row take carries the values through, including to Arrow."""
    morsel = Morsel.from_vectors(
        [b"s", b"n"],
        [dn.vector_from_string_sequence([b"z"]), dn.vector_from_sequence([42])],
    )

    taken = morsel.take([0])
    assert taken.num_rows == 1
    assert taken.column(b"s").to_pylist() == ["z"]
    assert taken.column(b"n").to_pylist() == [42]

    out = taken.to_arrow()
    assert out.num_rows == 1
    assert out.column(0).to_pylist() == ["z"]
    assert out.column(1).to_pylist() == [42]


if __name__ == "__main__":  # pragma: no cover
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"✓ {name}")
    print("✅ okay")
