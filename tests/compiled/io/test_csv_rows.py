import pytest

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

pa = pytest.importorskip("pyarrow")
cio = pytest.importorskip("opteryx.compiled.io")

if not hasattr(cio, "morsel_to_csv_rows") or not hasattr(cio, "morsel_to_csv_strings"):
    pytest.skip("compiled csv row functions not available")

from opteryx.compiled.io import morsel_to_csv_rows
from opteryx.compiled.io import morsel_to_csv_strings
from opteryx.compiled.draken.morsels.morsel import Morsel
from opteryx.compiled.draken.vectors.int64_vector import Int64Vector
from opteryx.compiled.draken.vectors.string_vector import StringVector


def test_morsel_to_csv_strings_basic_scalars():
    morsel = Morsel.from_arrow(
        pa.table(
            {
                "id": [1, 2],
                "name": ["alice", "bob"],
                "score": [1.5, None],
                "active": [True, False],
            }
        )
    )

    rows = morsel_to_csv_strings(morsel)

    assert rows == [
        "1,alice,1.5,true",
        "2,bob,,false",
    ]


def test_morsel_to_csv_rows_quotes_fields_and_header():
    morsel = Morsel.from_arrow(
        pa.table(
            {
                "name": ['a"b', "c,d"],
                "note": ["line\nbreak", "plain"],
            }
        )
    )

    rows = morsel_to_csv_rows(morsel, include_header=True).to_pylist()

    assert rows == [
        b"name,note",
        b'"a""b","line\nbreak"',
        b'"c,d",plain',
    ]


def test_morsel_to_csv_rows_supports_dictionary_columns():
    dictionary = pa.array(["north", "south"], type=pa.string())
    indices = pa.array([0, 1, None, 0], type=pa.int8())
    morsel = Morsel.from_arrow(pa.table({"region": pa.DictionaryArray.from_arrays(indices, dictionary)}))

    rows = morsel_to_csv_rows(morsel, include_header=True).to_pylist()

    assert rows == [
        b"region",
        b"north",
        b"south",
        b"",
        b"north",
    ]


def test_morsel_to_csv_rows_supports_typed_constant_columns():
    morsel = Morsel.from_vectors(
        ["id", "name"],
        [
            Int64Vector.from_constant(7, 3),
            StringVector.from_constant("north", 3),
        ],
    )

    rows = morsel_to_csv_rows(morsel, include_header=True).to_pylist()

    assert rows == [
        b"id,name",
        b"7,north",
        b"7,north",
        b"7,north",
    ]


def test_morsel_to_csv_rows_supports_typed_all_null_constant_columns():
    morsel = Morsel.from_vectors(
        ["name"],
        [StringVector.from_constant(None, 2, is_null=True)],
    )

    rows = morsel_to_csv_rows(morsel, include_header=True).to_pylist()

    assert rows == [
        b"name",
        b"",
        b"",
    ]


def test_morsel_to_csv_strings_supports_custom_separator():
    morsel = Morsel.from_arrow(
        pa.table(
            {
                "id": [1],
                "name": ["a|b"],
                "active": [True],
            }
        )
    )

    rows = morsel_to_csv_strings(morsel, include_header=True, separator="|")

    assert rows == [
        "id|name|active",
        '1|"a|b"|true',
    ]


def test_morsel_to_csv_rows_rejects_unsupported_types():
    morsel = Morsel.from_arrow(
        pa.table(
            {
                "id": [1, 2],
                "tags": pa.array([["a"], ["b"]], type=pa.list_(pa.string())),
            }
        )
    )

    with pytest.raises(NotImplementedError):
        morsel_to_csv_rows(morsel)


if __name__ == "__main__":
    from tests import run_tests

    run_tests()
    
