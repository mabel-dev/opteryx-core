"""Query HINTS — `SELECT ... FROM rel WITH(HINT, ...)` — are parsed and ignored.

History, because this test used to assert something that no longer exists:

* It called `opteryx.connect().cursor()`. The DB-API surface is gone; the public
  entry point is `opteryx.session()`. `opteryx.connect` is meant to be missing.
* It asserted `cur.messages == ["All HINTS are currently ignored"]`. That warning
  string no longer exists anywhere in the engine — hints are now ignored
  SILENTLY.

The warnings channel itself survived, moving from the cursor to
`session.messages` (backed by `QueryTelemetry.add_message`). So this test keeps
the original intent — "hints are accepted and change nothing" — expressed
against the current API, and pins the silent-ignore behaviour so that restoring
a warning has to be a deliberate change to this test.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx

BASE = "SELECT name FROM $planets"


def _run(sql):
    session = opteryx.session()
    rows = sum(morsel.num_rows for morsel in session.execute_to_morsels(sql))
    return rows, session.messages


def test_opteryx_has_no_connect():
    """The DB-API surface is deliberately absent — use opteryx.session()."""
    with pytest.raises(AttributeError):
        opteryx.connect  # noqa: B018


@pytest.mark.parametrize(
    "hints",
    [
        "NO_PARTITION",
        "NO_CACHE",
        "NO_PARTITION,NO_CACHE",
    ],
)
def test_hints_do_not_change_the_result(hints):
    plain_rows, _ = _run(BASE)
    hinted_rows, _ = _run(f"{BASE} WITH({hints})")
    assert hinted_rows == plain_rows


@pytest.mark.parametrize("hints", ["NO_PARTITION", "NO_PARTITION,NO_CACHE"])
def test_hints_emit_no_warning(hints):
    """Pins current behaviour: hints are ignored silently, not warned about.

    This previously asserted the warning "All HINTS are currently ignored".
    """
    _, messages = _run(f"{BASE} WITH({hints})")
    assert messages == []


@pytest.mark.parametrize("hints", ["NO_CACH", "TOTALLY_MADE_UP", "FETCH_AHEAD=128"])
def test_unknown_hints_are_rejected_not_swallowed(hints):
    """A hint the engine has never heard of must fail, not be silently dropped."""
    from opteryx.exceptions import UnsupportedSyntaxError

    with pytest.raises(UnsupportedSyntaxError):
        _run(f"{BASE} WITH({hints})")


def test_unknown_hint_suggests_the_near_miss():
    from opteryx.exceptions import UnsupportedSyntaxError

    with pytest.raises(UnsupportedSyntaxError, match="NO_CACHE"):
        _run(f"{BASE} WITH(NO_CACH)")


def test_session_messages_is_the_warnings_channel():
    _, messages = _run(BASE)
    assert isinstance(messages, list)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-v"]))
