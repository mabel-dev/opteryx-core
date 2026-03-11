import pytest

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))


pa = pytest.importorskip("pyarrow")
cio = pytest.importorskip("opteryx.compiled.io")

if not hasattr(cio, "morsel_to_json_rows") or not hasattr(cio, "morsel_to_json_strings"):
    pytest.skip("compiled json row functions not available")

from opteryx.draken.morsels.morsel import Morsel
from opteryx.compiled.io import morsel_to_json_rows
from opteryx.compiled.io import morsel_to_json_strings


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
    