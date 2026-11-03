"""Session.cancel(): a statement running on the native engine stops at its next morsel
and raises QueryCancelledError; the session then refuses further statements.

The query is a cross join of two one-morsel inputs, so the whole run is one input
fanning out (HAVE_MORE) — the case a between-source-morsels check alone misses.
"""

import os
import sys
import threading
import time

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pytest

import opteryx
from opteryx.exceptions import QueryCancelledError

# Runs for seconds uncancelled (~7 s on an M-series Mac).
SLOW = "SELECT SUM(a.a * b.b) FROM generate_series(1, 30000) AS a CROSS JOIN generate_series(1, 30000) AS b"


def test_cancel_stops_a_running_statement():
    session = opteryx.session()
    threading.Timer(0.2, session.cancel).start()
    started = time.monotonic()
    with pytest.raises(QueryCancelledError):
        list(session.execute_to_morsels(SLOW))
    assert time.monotonic() - started < 2.0


def test_a_cancelled_session_runs_nothing_more():
    session = opteryx.session()
    session.cancel()
    with pytest.raises(QueryCancelledError):
        list(session.execute_to_morsels("SELECT COUNT(*) FROM generate_series(1, 10)"))


def test_cancel_is_per_session():
    cancelled = opteryx.session()
    cancelled.cancel()
    rows = opteryx.session().execute_to_morsels("SELECT COUNT(*) FROM generate_series(1, 10)")
    assert sum(m.num_rows for m in rows) == 1

