"""The per-scan Parquet IO knobs must reach the NATIVE scan path.

Until 2026-09-16 only the trampoline operator resolved these variables;
`open_native_scan_plan` — the production default — took no `http_tuning`,
`coalesce_tuning` or `in_flight_limit_override` at all. So every one of these
SETs was silently inert in production while appearing to work in the tests that
exercised the trampoline.

Two things are pinned here:

1. The resolvers are shared (`connectors/parquet_io/io_tuning`), so the two scan
   paths cannot drift apart again.
2. `parquet_io_in_flight_limit` MOVES the window the native scan actually runs,
   read back from telemetry — not merely that the parameter is accepted.

The knobs are `Visibility.RESTRICTED`, so `SET` needs the `platform_admin`
entitlement. The end-to-end test therefore drives the env layer of the same
default -> env -> SET chain, in a subprocess because config is read at import.
"""

import inspect
import os
import subprocess
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import pytest

from opteryx.connectors.parquet_io import io_tuning
from opteryx.connectors.parquet_io.pool_reader import open_native_scan_plan

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../.."))

PROBE = """
import sys
sys.path.insert(1, {root!r})
import opteryx
session = opteryx.session()
for _ in session.execute_to_morsels(
    "SELECT user_id FROM testdata.flat.formats.parquet WHERE user_id > 0"
):
    pass
diagnostics = session.telemetry.get("io_scan_diagnostics") or []
print([d.get("in_flight_limit") for d in diagnostics])
"""


def _in_flight_limit_for(env_value):
    env = dict(os.environ)
    if env_value is None:
        env.pop("PARQUET_IO_IN_FLIGHT_LIMIT", None)
    else:
        env["PARQUET_IO_IN_FLIGHT_LIMIT"] = str(env_value)
    result = subprocess.run(
        [sys.executable, "-c", PROBE.format(root=REPO_ROOT)],
        capture_output=True, text=True, env=env, cwd=REPO_ROOT,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    return eval(result.stdout.strip().splitlines()[-1])


def test_native_scan_plan_accepts_the_per_scan_tuning():
    """The parameters exist — the plumbing that was missing entirely."""
    parameters = inspect.signature(open_native_scan_plan).parameters
    assert "in_flight_limit_override" in parameters
    assert "http_tuning" in parameters
    assert "coalesce_tuning" in parameters


@pytest.mark.parametrize("override", [5, 19, 31])
def test_in_flight_limit_moves_the_native_submission_window(override):
    """Prove the knob MOVES the measured value, not just that it is accepted."""
    assert _in_flight_limit_for(override) == [override]


def test_in_flight_limit_unset_is_auto_and_not_the_override():
    """Auto is max(workers, fetch_ahead) + 2 — a real window, never 0."""
    auto = _in_flight_limit_for(None)
    assert len(auto) == 1
    assert auto[0] >= 3
    assert auto[0] not in (5, 19, 31)


def test_http_tuning_shape_matches_set_http_tuning():
    """7 fields, in the order set_http_tuning/CppIOPipeline.__cinit__ unpack."""
    tuning = io_tuning.resolve_http_tuning(None)
    assert len(tuning) == 7
    max_conns, retries, min_bw_bytes, timeout_floor, multiplexing, pipewait, http11 = tuning
    assert isinstance(max_conns, int) and isinstance(retries, int)
    # Stored/SET in Mbps, handed over in bytes/s.
    assert isinstance(min_bw_bytes, float) and min_bw_bytes > 0
    assert isinstance(timeout_floor, int)
    assert isinstance(multiplexing, bool) and isinstance(pipewait, bool)
    assert isinstance(http11, bool)


def test_coalesce_tuning_shape():
    waste_ratio, max_bytes = io_tuning.resolve_coalesce_tuning(None)
    assert isinstance(waste_ratio, float)
    assert isinstance(max_bytes, int) and max_bytes > 0


def test_in_flight_limit_default_is_auto_sentinel():
    """0 means auto — the absolute-vs-delta decision recorded in variables.py."""
    assert io_tuning.resolve_in_flight_limit(None) == 0


def test_trampoline_and_native_share_one_resolver():
    """The duplication that let the two paths drift must not come back."""
    source = open(
        os.path.join(REPO_ROOT, "opteryx/operators/parquet_read/parquet_read.pyx"),
        encoding="utf-8",
    ).read()
    assert "from opteryx.connectors.parquet_io.io_tuning import" in source
    # The trampoline must not carry its own copy of the resolution any more.
    assert "config.HTTP_MAX_CONNECTIONS_PER_HOST" not in source


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-v"]))
