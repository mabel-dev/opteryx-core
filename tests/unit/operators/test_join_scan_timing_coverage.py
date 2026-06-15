"""Join/scan timing coverage: joins and scans are not driven via push(), so
before this fix they reported 0ms in EXPLAIN ANALYZE.

  * Scans are driven via drive_scan -> scan.next_morsel(); drive_scan now times
    that call into the scan's execution_time.
  * Joins are driven via JoinLeft/RightAdapter calling push_left/push_right
    directly; the adapters' push() now attributes time + input counters to the
    JOIN (not the hidden, is_not_explained adapter).
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pytest

from draken.morsels.morsel import Morsel
from draken.draken_native import vector_from_sequence

from opteryx import EOS
from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode, JoinLeftAdapter, PipelineContext
from opteryx.operators._operators import drive_scan
from opteryx.operators.cross_join import CrossJoinNode


def _morsel(n=3):
    return Morsel.from_vectors([b"c"], [vector_from_sequence(list(range(n)))])


# ---------------------------------------------------------------------------
# Joins: adapter attributes timing + input counters to the join, not itself.
# ---------------------------------------------------------------------------

def test_join_adapter_attributes_to_join_not_adapter():
    join = CrossJoinNode(properties=QueryProperties("t", {}), columns=[])
    adapter = JoinLeftAdapter(join)
    adapter.enable_tracing(True)
    join.enable_tracing(True)

    # Push enough build-side data that the EOS combine does measurable work
    # (the timing assertion below would be flaky on a trivial single morsel).
    n_morsels, rows = 50, 1000
    for _ in range(n_morsels):
        adapter.push(_morsel(rows))
    adapter.push(EOS)                 # build EOS -> combine + _build_complete

    s_join = join.sensors()
    # Input counters are attributed to the join (deterministic).
    assert s_join["records_in"] == n_morsels * rows
    assert s_join["calls"] == n_morsels + 1    # data pushes + one EOS push
    # And the timed work lands on the join, not the hidden adapter.
    assert s_join["execution_time"] > 0

    # The adapter itself stays at zero — its work was attributed to the join.
    assert adapter.execution_time == 0
    assert adapter.records_in == 0
    assert adapter.calls == 0


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
