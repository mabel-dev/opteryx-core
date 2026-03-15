import pytest

import opteryx
from opteryx.exceptions import UnsupportedSyntaxError


def test_interval_prefix_literal_still_returns_interval_type():
    arrow_result = opteryx.session().execute_to_arrow("SELECT INTERVAL '1' MONTH AS iv")
    assert arrow_result.num_rows == 1
    assert arrow_result.column(0)[0].as_py() == [1, 0]


@pytest.mark.parametrize(
    "query",
    [
        "SELECT DATE '2025-02-02' AS d",
        "SELECT TIMESTAMP '2025-02-02 00:00:00' AS ts",
        "SELECT INTEGER '22' AS n",
        "SELECT BOOLEAN 'true' AS b",
        "SELECT DECIMAL '1.2' AS d",
    ],
)
def test_non_interval_prefix_typed_literals_are_rejected(query):
    with pytest.raises(
        UnsupportedSyntaxError,
        match="Type-prefixed string literals are no longer supported",
    ):
        opteryx.session().execute_to_arrow(query)
