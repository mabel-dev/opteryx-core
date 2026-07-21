"""Scan timing coverage: scans are not driven via push(), so before this fix
they reported 0ms in EXPLAIN ANALYZE.

  * Scans are driven via drive_scan -> scan.next_morsel(); drive_scan now times
    that call into the scan's execution_time.

(Join timing-attribution coverage — JoinLeft/RightAdapter calling
push_left/push_right and attributing time to the join, not the adapter — was
dropped with CrossJoinNode's push_left/push_right execution body, which is
dead in production: the native engine never drives it. push_left/push_right
are `cdef` methods, so a lightweight Python-level synthetic JoinNode subclass
can't override them the way `_Scan`/`_Sink` below override plain methods.)
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pytest

from draken.morsels.morsel import Morsel
from draken.draken_native import vector_from_sequence

from opteryx import EOS
from opteryx.operators import BasePlanNode, PipelineContext
from opteryx.operators._operators import drive_scan


def _morsel(n=3):
    return Morsel.from_vectors([b"c"], [vector_from_sequence(list(range(n)))])


# ---------------------------------------------------------------------------
# Scans: drive_scan times next_morsel() into the scan's execution_time.
# ---------------------------------------------------------------------------

class _Scan(BasePlanNode):
    def __init__(self, n):
        self._n = n

    def read_morsels(self):
        for _ in range(self._n):
            # a little work so the timed next_morsel is non-zero
            acc = 0
            for k in range(5000):
                acc += k
            yield _morsel(3)


class _Sink(BasePlanNode):
    def __init__(self):
        self.rows = 0

    def _push_impl(self, morsel):
        if morsel is not EOS:
            self.rows += morsel.num_rows


def test_scan_time_recorded_by_drive_scan():
    scan = _Scan(5)
    sink = _Sink()
    ctx = PipelineContext()
    list(drive_scan(scan, sink, None, ctx))

    # next_morsel was timed into the scan's execution_time, and calls counted
    # (one per next_morsel, including the terminal None).
    assert scan.execution_time > 0
    assert scan.calls == 6                       # 5 morsels + 1 terminal None
    # Scan never emits via _emit_cdef, so it has no downstream_time and its
    # self_time equals execution_time.
    s = scan.sensors()
    assert s["downstream_time"] == 0
    assert s["self_time"] == s["execution_time"]
    assert sink.rows == 15                        # 5 morsels x 3 rows survived


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
