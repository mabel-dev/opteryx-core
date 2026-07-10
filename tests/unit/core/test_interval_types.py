import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.exceptions import UnsupportedSyntaxError
from tests.helpers import execute_and_get_arrow


def test_interval_prefix_literal_still_returns_interval_type():
    arrow_result = execute_and_get_arrow("SELECT INTERVAL '1' MONTH AS iv")
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
        execute_and_get_arrow(query)
