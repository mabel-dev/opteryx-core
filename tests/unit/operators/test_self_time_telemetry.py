"""WP-5 — exclusive self-time telemetry + fixed trace outputs.

`push()` times the entire downstream call stack, so `execution_time` is
INCLUSIVE — summing operators overcounts and the dominant operator is
unidentifiable. WP-5 adds `downstream_time` (accumulated in `_emit_cdef`, gated
on tracing) so `self_time = execution_time - downstream_time` gives each
operator's own work. It also fixes `TraceEvent.rows_out/bytes_out`, previously
hardcoded to 0.

The tests assert STRUCTURAL invariants (no flaky wall-clock thresholds):

  * inclusive nesting: head.execution_time >= mid >= tail
  * self_time = execution_time - downstream_time, clamped >= 0
  * head.downstream_time >= mid.execution_time (head times the full mid.push,
    which is a superset of mid's own _dispatch_push)
  * tracing OFF  -> downstream_time == 0 and self_time == execution_time
  * trace events carry the rows actually emitted by each push
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pytest

from draken.morsels.morsel import Morsel
from draken.draken_native import vector_from_sequence

from opteryx import EOS
from opteryx.operators import BasePlanNode


def _morsel(i):
    return Morsel.from_vectors([b"c"], [vector_from_sequence([i, i + 1, i + 2])])


class _WorkOp(BasePlanNode):
    """Forwarding operator that does a measurable slice of work per morsel so
    timings are non-zero, then emits the morsel downstream. Skips
    BasePlanNode.__init__: only __cinit__ defaults (counters zero-initialised)
    plus a Python __dict__ are needed."""

    def __init__(self, downstream=None):
        if downstream is not None:
            self.set_downstream(downstream)

    def _push_impl(self, morsel):
        if morsel is not EOS:
            acc = 0
            for k in range(20000):     # busy work → non-zero self time
                acc += k
            self._spin = acc
        self.emit(morsel)


class _Collector(BasePlanNode):
    """Terminal operator: records morsels, never emits (downstream_time stays 0)."""

    def __init__(self):
        self.collected = []

    def _push_impl(self, morsel):
        if morsel is not EOS:
            self.collected.append(morsel)


def _chain(trace):
    tail = _Collector()
    mid = _WorkOp(tail)
    head = _WorkOp(mid)
    for op in (head, mid, tail):
        op.enable_tracing(trace)
    return head, mid, tail


def _drive(head, n=8):
    for i in range(n):
        head.push(_morsel(i))
    head.push(EOS)


def test_inclusive_nesting_and_self_time_decomposition():
    head, mid, tail = _chain(trace=True)
    _drive(head)

    h, m, t = head.sensors(), mid.sensors(), tail.sensors()

    # Inclusive nesting: each operator's execution_time includes its downstream.
    assert h["execution_time"] >= m["execution_time"] >= t["execution_time"] > 0

    # self_time = execution_time - downstream_time, never negative.
    for s in (h, m, t):
        assert s["self_time"] == max(0, s["execution_time"] - s["downstream_time"])
        assert s["self_time"] >= 0
        assert s["self_time"] <= s["execution_time"]

    # head/mid drive a downstream chain (non-zero downstream_time); tail does not.
    assert h["downstream_time"] > 0
    assert m["downstream_time"] > 0
    assert t["downstream_time"] == 0
    assert t["self_time"] == t["execution_time"]

    # head's downstream_time is the time of mid.push(), a superset of mid's own
    # _dispatch_push, so it is >= mid.execution_time.
    assert h["downstream_time"] >= m["execution_time"]

    # The decomposition telescopes to roughly the chain head's inclusive time.
    sum_self = h["self_time"] + m["self_time"] + t["self_time"]
    # Within 5% (the only slack is per-call push() bookkeeping/clock overhead).
    assert abs(sum_self - h["execution_time"]) <= 0.05 * h["execution_time"]


def test_tracing_off_self_equals_inclusive_and_zero_overhead_counters():
    head, mid, tail = _chain(trace=False)
    _drive(head)

    for op in (head, mid, tail):
        s = op.sensors()
        assert s["downstream_time"] == 0
        assert s["self_time"] == s["execution_time"]


def test_trace_records_emitted_rows():
    head, mid, tail = _chain(trace=True)
    _drive(head, n=4)

    # Each non-EOS push emitted a 3-row morsel; the trace must record rows_out=3
    # (previously hardcoded 0), bytes_out>0, and produced_output=True.
    events = [e for e in head.get_trace_events() if e["rows_in"] > 0]
    assert len(events) == 4
    for e in events:
        assert e["rows_out"] == 3
        assert e["bytes_out"] > 0
        assert e["produced_output"] is True


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
