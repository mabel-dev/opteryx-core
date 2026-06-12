"""WP-1 — error-path and abandonment cleanup in the serial engine.

`drive_scan` must close its scan's source iterator on EVERY exit path so the
source's own finally-block cleanup (the rugo C++ IO pipeline shutdown, open
file handles) never leaks:

  * normal exhaustion,
  * an exception raised mid-chain,
  * the caller abandoning the result generator (GeneratorExit).

The original exception / GeneratorExit must not be suppressed.

The test drives `drive_scan` directly with lightweight BasePlanNode subclasses
so it is independent of SQL planning. A `read_morsels` generator records in its
finally-block whether it was closed; that is the observable proxy for "the
source was cleaned up".
"""

import os
import sys
from collections import deque

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pytest

from draken.morsels.morsel import Morsel
from draken.draken_native import vector_from_sequence

from opteryx import EOS
from opteryx.operators import BasePlanNode, PipelineContext
from opteryx.operators._operators import drive_scan


def _morsel(i):
    return Morsel.from_vectors([b"c"], [vector_from_sequence([i])])


class _RecordingScan(BasePlanNode):
    """Source whose generator records that its finally-block ran (i.e. it was
    closed). Skips BasePlanNode.__init__ — only the __cinit__ defaults plus a
    Python __dict__ (present on Python subclasses of an extension type) are
    needed for next_morsel / close_source."""

    def __init__(self, n, closed_flag):
        self._n = n
        self._closed = closed_flag

    def read_morsels(self):
        try:
            for i in range(self._n):
                yield _morsel(i)
        finally:
            self._closed.append(True)


class _ForwardingHead(BasePlanNode):
    """Chain head that forwards morsels downstream, optionally raising on the
    Nth data morsel to simulate a mid-chain failure."""

    def __init__(self, downstream, raise_on=0):
        self.set_downstream(downstream)
        self._raise_on = raise_on
        self._seen = 0

    def _push_impl(self, morsel):
        if morsel is not EOS:
            self._seen += 1
            if self._raise_on and self._seen == self._raise_on:
                raise RuntimeError("mid-chain boom")
        self.emit(morsel)


class _FakeExit(BasePlanNode):
    """Minimal exit node exposing the has_pending/pop_pending drain contract."""

    def __init__(self):
        self._q = deque()

    def _push_impl(self, morsel):
        if morsel is not EOS:
            self._q.append(morsel)

    def has_pending(self):
        return len(self._q) > 0

    def pop_pending(self):
        return self._q.popleft()


def _wire(n, raise_on=0):
    closed = []
    scan = _RecordingScan(n, closed)
    exit_node = _FakeExit()
    head = _ForwardingHead(exit_node, raise_on=raise_on)
    ctx = PipelineContext()
    return scan, head, exit_node, ctx, closed


def test_normal_exhaustion_closes_source():
    scan, head, exit_node, ctx, closed = _wire(3)
    out = list(drive_scan(scan, head, exit_node, ctx))
    assert len(out) == 3
    assert closed == [True]


def test_midchain_exception_propagates_and_closes_source():
    scan, head, exit_node, ctx, closed = _wire(5, raise_on=2)
    gen = drive_scan(scan, head, exit_node, ctx)
    with pytest.raises(RuntimeError, match="mid-chain boom"):
        list(gen)
    assert closed == [True], "source generator was not closed on exception"


def test_caller_abandonment_closes_source():
    scan, head, exit_node, ctx, closed = _wire(5)
    gen = drive_scan(scan, head, exit_node, ctx)
    first = next(gen)              # consume one result, then abandon
    assert first is not None
    gen.close()                   # GeneratorExit propagates into drive_scan
    assert closed == [True], "source generator was not closed on abandonment"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
