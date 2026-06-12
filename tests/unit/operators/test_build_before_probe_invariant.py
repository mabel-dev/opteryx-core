"""WP-2 — build-before-probe ordering invariant.

Every two-input join must fully drain its BUILD side (including EOS) before any
PROBE-side morsel arrives. The serial engine guarantees this by driving the
build chain to completion first; this test asserts that the invariant is now
*checked* rather than assumed — a probe arriving early raises
InvalidInternalStateError instead of silently probing an absent/partial build
table.

Two polarities are covered directly:
  * CrossJoinNode   — build side is LEFT  (probe = push_right)
  * FilterJoinNode  — build side is RIGHT (probe = push_left)

Inner / outer / nested-loop / non-equi / asof joins share the same
JoinNode._require_build_complete mechanism and are exercised end-to-end by the
regression suite (make q / tpch).
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pytest

from draken.morsels.morsel import Morsel
from draken.draken_native import vector_from_sequence

from opteryx import EOS
from opteryx.exceptions import InvalidInternalStateError
from opteryx.models import QueryProperties
from opteryx.operators.cross_join import CrossJoinNode
from opteryx.operators.filter_join import FilterJoinNode


def _props():
    return QueryProperties("test-wp2", {})


def _morsel():
    return Morsel.from_vectors([b"c"], [vector_from_sequence([1, 2, 3])])


# ---------------------------------------------------------------------------
# CrossJoinNode — build = LEFT, probe = RIGHT
# ---------------------------------------------------------------------------

def test_cross_probe_before_build_raises():
    node = CrossJoinNode(properties=_props(), columns=[])
    with pytest.raises(InvalidInternalStateError):
        node.push_right(_morsel())


def test_cross_probe_after_build_ok():
    node = CrossJoinNode(properties=_props(), columns=[])
    # Build side closes (empty build is legal and must still set the flag).
    node.push_left(EOS)
    # Probe morsel now accepted without raising.
    node.push_right(_morsel())
    node.push_right(EOS)


def test_cross_empty_build_then_probe_ok():
    node = CrossJoinNode(properties=_props(), columns=[])
    node.push_left(EOS)            # zero build rows
    node.push_right(EOS)           # probe EOS — must not raise


# ---------------------------------------------------------------------------
# FilterJoinNode — build = RIGHT, probe = LEFT
# ---------------------------------------------------------------------------

def test_filter_probe_before_build_raises():
    node = FilterJoinNode(properties=_props(), type="left semi", columns=[],
                          left_columns=[], right_columns=[])
    with pytest.raises(InvalidInternalStateError):
        node.push_left(_morsel())


def test_filter_probe_after_build_ok():
    node = FilterJoinNode(properties=_props(), type="left semi", columns=[],
                          left_columns=[], right_columns=[])
    node.push_right(EOS)           # build (right) closes
    node.push_left(EOS)            # probe EOS — must not raise


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
