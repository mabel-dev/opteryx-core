import pytest

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))


pa = pytest.importorskip("pyarrow")
cio = pytest.importorskip("opteryx.compiled.io")

if not hasattr(cio, "morsel_to_json_rows") or not hasattr(cio, "morsel_to_json_strings"):
    pytest.skip("compiled json row functions not available")

from draken.morsels.morsel import Morsel
from opteryx.compiled.io import morsel_to_json_rows
from opteryx.compiled.io import morsel_to_json_strings
from draken.vectors.int64_vector import Int64Vector
from draken.vectors.string_vector import StringVector


def test_morsel_to_json_strings_basic_scalars():
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

    rows = morsel_to_json_strings(morsel)

    assert rows == [
        '{"id":1,"name":"alice","score":1.5,"active":true}',
        '{"id":2,"name":"bob","score":null,"active":false}',
    ]


def test_morsel_to_json_rows_escapes_strings_and_omits_nulls():
    morsel = Morsel.from_arrow(
        pa.table(
            {
                "name": ['a"b', None],
                "note": ["line\nbreak", "tab\tchar"],
            }
        )
    )

    rows = morsel_to_json_rows(morsel, omit_null_fields=True).to_pylist()

    assert rows == [
        b'{"name":"a\\"b","note":"line\\nbreak"}',
        b'{"note":"tab\\tchar"}',
    ]


def test_morsel_to_json_rows_supports_dictionary_columns():
    dictionary = pa.array(["north", "south"], type=pa.string())
    indices = pa.array([0, 1, None, 0], type=pa.int8())
    morsel = Morsel.from_arrow(pa.table({"region": pa.DictionaryArray.from_arrays(indices, dictionary)}))

    rows = morsel_to_json_rows(morsel, omit_null_fields=False).to_pylist()

    assert rows == [
        b'{"region":"north"}',
        b'{"region":"south"}',
        b'{"region":null}',
        b'{"region":"north"}',
    ]


def test_morsel_to_json_rows_supports_typed_constant_columns():
    morsel = Morsel.from_vectors(
        ["id", "name"],
        [
            Int64Vector.from_constant(7, 2),
            StringVector.from_constant("north", 2),
        ],
    )

    rows = morsel_to_json_rows(morsel, omit_null_fields=False).to_pylist()

    assert rows == [
        b'{"id":7,"name":"north"}',
        b'{"id":7,"name":"north"}',
    ]


def test_morsel_to_json_rows_supports_typed_all_null_constant_columns():
    morsel = Morsel.from_vectors(
        ["name"],
        [StringVector.from_constant(None, 2, is_null=True)],
    )

    rows = morsel_to_json_rows(morsel, omit_null_fields=False).to_pylist()

    assert rows == [
        b'{"name":null}',
        b'{"name":null}',
    ]


def test_morsel_to_json_strings_supports_raw_json_columns():
    morsel = Morsel.from_arrow(
        pa.table(
            {
                "id": [1, 2],
                "payload": ['{"a":1}', "[1,2,3]"],
            }
        )
    )

    rows = morsel_to_json_strings(morsel, raw_json_columns=["payload"])

    assert rows == [
        '{"id":1,"payload":{"a":1}}',
        '{"id":2,"payload":[1,2,3]}',
    ]


def test_morsel_to_json_strings_supports_typed_constant_raw_json_columns():
    morsel = Morsel.from_vectors(
        ["payload"],
        [StringVector.from_constant('{"a":1}', 2)],
    )

    rows = morsel_to_json_strings(morsel, raw_json_columns=["payload"])

    assert rows == [
        '{"payload":{"a":1}}',
        '{"payload":{"a":1}}',
    ]


def test_morsel_to_json_rows_rejects_unsupported_types():
    morsel = Morsel.from_arrow(
        pa.table(
            {
                "id": [1, 2],
                "tags": pa.array([["a"], ["b"]], type=pa.list_(pa.string())),
            }
        )
    )

    with pytest.raises(NotImplementedError):
        morsel_to_json_rows(morsel)

if __name__ == "__main__":
    from tests import run_tests

    run_tests()
    
