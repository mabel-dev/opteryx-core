"""
An INSERT type mismatch names the types, not their enum repr.

It used to read "target LogicalCategory.FLOAT"; both sides now render the declared
ColumnType's canonical name.
"""

import pytest

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.local_store_connector import LocalStoreConnector
from opteryx.exceptions import UnsupportedSyntaxError


@pytest.mark.parametrize(
    "sql",
    (
        "INSERT INTO ws.t VALUES ('a', 'b')",
        "INSERT INTO ws.t SELECT 'a', 'b'",
    ),
)
def test_insert_type_mismatch_names_the_types(tmp_path, sql):
    register_workspace("ws", LocalStoreConnector, store_root=str(tmp_path))
    session = opteryx.session()
    list(session.execute_to_morsels("CREATE TABLE ws.t (x DOUBLE, n VARCHAR)"))

    with pytest.raises(UnsupportedSyntaxError) as error:
        list(session.execute_to_morsels(sql))

    message = str(error.value)
    assert "LogicalCategory" not in message
    assert "source VARCHAR is not compatible with target FLOAT64" in message


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
