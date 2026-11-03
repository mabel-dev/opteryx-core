"""Session.cancel() during plan compile: the statement stops in the compile, not after it.

The engine checks the cancel token per morsel once it runs, but compiling comes first and
on a cold scan of many files it is minutes of footer fetches and per-file pruning
(time_engine_compile 315 s + time_engine_footer_fetch 291 s on a GROUP BY over
opteryx.test.pypi_large, 2026-11-03) - a cancel in that window went unanswered until the
compile finished. These pin the compile-time checkpoints.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pytest

import opteryx
from opteryx.exceptions import QueryCancelledError
from opteryx.managers.execution import compiler
from opteryx.operators._operators import CancelToken
from opteryx.utils.cancellation import cancellable
from opteryx.utils.cancellation import raise_if_cancelled

PLANETS = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "../../../testdata/planets/planets.parquet")
)


def test_the_checkpoint_answers_only_to_the_installed_token():
    token = CancelToken()
    token.cancel()

    raise_if_cancelled()  # no token installed: nothing to answer to
    with cancellable(CancelToken()):
        raise_if_cancelled()  # installed, not cancelled
    with pytest.raises(QueryCancelledError):
        with cancellable(token):
            raise_if_cancelled()
    raise_if_cancelled()  # the block put the previous (no) token back


def test_scan_planning_stops_for_a_cancelled_statement():
    from opteryx.connectors.parquet_io.pool_reader import open_native_scan_plan

    token = CancelToken()
    token.cancel()
    with pytest.raises(QueryCancelledError):
        with cancellable(token):
            open_native_scan_plan([PLANETS], ["id"], decode_workers=1)

    plan = open_native_scan_plan([PLANETS], ["id"], decode_workers=1)
    plan.close()


def test_a_cancel_during_compile_stops_the_compile(monkeypatch):
    """Cancelled as the compile starts, the compile itself raises - the run's own checks
    never get the chance, which is what separates this from the engine's per-morsel stop."""
    session = opteryx.session()
    original = compiler.compile_to_native
    raised_in_compile = []

    def cancel_then_compile(plan, pool=None):
        session.cancel()
        try:
            return original(plan, pool=pool)
        except QueryCancelledError:
            raised_in_compile.append(True)
            raise

    monkeypatch.setattr(compiler, "compile_to_native", cancel_then_compile)
    with pytest.raises(QueryCancelledError):
        list(session.execute_to_morsels("SELECT id FROM $planets"))
    assert raised_in_compile == [True]
