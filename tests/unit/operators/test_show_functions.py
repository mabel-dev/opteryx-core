"""
SHOW FUNCTIONS is not a supported statement — it must be refused, loudly.

This test used to reach the statement through `opteryx.connect()`/`cursor()`, a
DBAPI-style surface that no longer exists — the resulting AttributeError is not
an UnsupportedSyntaxError, so `pytest.raises` did not catch it and the test went
red without SHOW FUNCTIONS ever being executed. Driven through the current
session API instead, so the refusal itself is what is asserted.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))


def test_show_functions_is_unsupported():
    import opteryx
    from opteryx.exceptions import UnsupportedSyntaxError

    session = opteryx.session()
    with pytest.raises(UnsupportedSyntaxError):
        for _ in session.execute_to_morsels("SHOW FUNCTIONS"):
            pass


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
