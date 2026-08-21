"""
Planner estimates cross into NativePlan setters as int64_t. An estimator bug once
produced 3.6e19 rows (TPC-DS Q54, DNF selectivity) and the implicit Cython
coercion died with a bare `OverflowError: Python int too large to convert to C
long` naming neither the operator nor the number. The boundary now fails loudly
at plan time through `_estimate_to_int64` — a clamp to INT64_MAX would hide the
next estimator bug (architect ruling, 2026-08-21).
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pytest

from opteryx.exceptions import InvalidInternalStateError
from opteryx.managers.execution.compiler import _estimate_to_int64

INT64_MAX = (1 << 63) - 1


def test_unknown_estimate_crosses_as_sentinel():
    assert _estimate_to_int64(None, "output-row estimate for the inner join") == -1


def test_in_range_estimates_pass_through():
    assert _estimate_to_int64(0, "output-row estimate for the inner join") == 0
    assert _estimate_to_int64(1_000_000, "group-count estimate for GROUP BY") == 1_000_000
    assert _estimate_to_int64(INT64_MAX, "output-row estimate for the inner join") == INT64_MAX


def test_overflowing_estimate_fails_loudly_naming_operator_and_value():
    # The actual pre-fix Q54 number: with DNF selectivity dropped, the BETWEEN
    # bounds' scalar subqueries were costed at full date_dim and cross-joined —
    # 6,839,012,639 × 73,049 × 73,049. It must raise InvalidInternalStateError
    # (not OverflowError), and the message must name the operator and the number.
    with pytest.raises(InvalidInternalStateError) as exc:
        _estimate_to_int64(36494041070119752239, "output-row estimate for the cross join")
    message = str(exc.value)
    assert "output-row estimate for the cross join" in message
    assert "36494041070119752239" in message
    assert "64-bit" in message


def test_negative_estimate_is_refused():
    # -1 is minted only from None above; a negative estimator output is a bug.
    with pytest.raises(InvalidInternalStateError):
        _estimate_to_int64(-5, "distinct-count estimate for DISTINCT")


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
