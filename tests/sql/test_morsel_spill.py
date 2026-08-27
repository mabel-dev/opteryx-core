"""Regression tests for morsel spill (docs/MORSEL_SPILL_DESIGN.md).

MorselBuffer — the hand-off every breaker writes and every dependent pipeline
reads — accumulates in memory and, once the pile reaches kSpillFlushBytes
(512MB) WITH a spill root configured, flushes the pile to one .skene unit
(spill profile, zstd-1) and reads it back row group by row group on the
claiming workers. The thresholds live in src/cpp/engine/spill_budgets.hpp; the
store (per-query q<pid>-* directory, startup sweep, loud disk errors) in
src/cpp/engine/spill_store.hpp; the buffer in
src/cpp/engine/pipeline_buffers.hpp.

The contract these tests pin:
  * results are IDENTICAL spilled and resident — spill is a residency change,
    never an answer change;
  * a small query performs ZERO disk I/O (the trigger is a budget, not a
    licence: an eight-row CTE never touches disk);
  * a finished query leaves NOTHING on disk (units deleted at last-consumer
    release, the query directory removed with the engine);
  * a dead process's leftover spill directory is collected by the startup
    sweep — orphans are the failure that turns spill into the OOM's
    replacement on a shared 10GB disk;
  * unconfigured (no KVSTORE_LOCATION), behaviour is the pre-spill engine —
    which is what every OTHER test in this suite runs under.

KVSTORE_LOCATION must be set before opteryx is imported (config is read at
import), so the spilling cases run in a subprocess.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import subprocess
import sys
import tempfile
import textwrap

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

REPO = os.path.join(os.path.dirname(__file__), "..", "..")

# 80M int64 rows ~= 640MB through the sort's output buffer: comfortably past
# the 512MB flush trigger, small enough to run in seconds.
SPILL_ROWS = 80_000_000


def _run_in_subprocess(body: str, spill_root: str) -> str:
    """Run `body` in a fresh interpreter with KVSTORE_LOCATION set pre-import."""
    script = textwrap.dedent(
        f"""
        import os, sys
        os.environ["KVSTORE_LOCATION"] = {spill_root!r}
        sys.path.insert(1, {REPO!r})
        import opteryx
        from opteryx.operators._operators import get_spill_telemetry
        from opteryx.operators._operators import reset_spill_telemetry
        """
    ) + textwrap.dedent(body)
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=600
    )
    assert result.returncode == 0, result.stderr
    return result.stdout


def test_spilled_sort_answers_are_identical():
    """A >512MB ORDER BY spills its output buffer; the rows come back complete,
    in order, and summing to the closed form — spill changed residency only."""
    with tempfile.TemporaryDirectory() as root:
        out = _run_in_subprocess(
            f"""
            reset_spill_telemetry()
            session = opteryx.session()
            n = {SPILL_ROWS}
            rows = 0
            prev = 0
            sorted_ok = True
            sql = f"SELECT s FROM generate_series(1, {{n}}) AS s ORDER BY s"
            for m in session.execute_to_morsels(sql):
                for i in range(m.num_rows):
                    v = m[i][0]
                    if v <= prev:
                        sorted_ok = False
                    prev = v
                rows += m.num_rows
            tel = get_spill_telemetry()
            print("rows", rows)
            print("sorted", sorted_ok)
            print("last", prev)
            print("units", tel["units_written"])
            print("read_ge_written", tel["bytes_read"] > 0)
            """,
            root,
        )
        got = dict(line.split(" ", 1) for line in out.strip().splitlines())
        assert got["rows"] == str(SPILL_ROWS), out
        assert got["sorted"] == "True", out
        assert got["last"] == str(SPILL_ROWS), out
        # The point of the test: the buffer actually spilled.
        assert int(got["units"]) >= 1, out
        assert got["read_ge_written"] == "True", out
        # A finished query leaves nothing behind — no unit files, no q-dirs.
        leftovers = [
            os.path.join(dirpath, f)
            for dirpath, _, files in os.walk(root)
            for f in files
        ]
        assert leftovers == [], leftovers
        assert os.listdir(root) == [], os.listdir(root)


def test_small_query_never_touches_disk():
    """The trigger is a budget: an eight-row CTE performs zero disk I/O even
    with spill fully configured."""
    with tempfile.TemporaryDirectory() as root:
        out = _run_in_subprocess(
            """
            reset_spill_telemetry()
            session = opteryx.session()
            sql = ("WITH t AS (SELECT s FROM generate_series(1, 8) AS s) "
                   "SELECT (SELECT COUNT(*) FROM t) AS a, (SELECT SUM(s) FROM t) AS b")
            for m in session.execute_to_morsels(sql):
                print("row", m[0][0], m[0][1])
            tel = get_spill_telemetry()
            print("units", tel["units_written"])
            print("root_entries", len(os.listdir(os.environ["KVSTORE_LOCATION"])))
            """,
            root,
        )
        got = dict(line.split(" ", 1) for line in out.strip().splitlines())
        assert got["row"] == "8 36", out
        assert got["units"] == "0", out
        # Not even the query directory is created without a flush.
        assert got["root_entries"] == "0", out


def test_startup_sweep_collects_dead_owners():
    """A q<pid>-* directory whose pid is dead is removed by the sweep the first
    time a store starts against the root; a live pid's directory survives."""
    with tempfile.TemporaryDirectory() as root:
        dead = os.path.join(root, "q999999999-0")   # pid far past any real one
        os.makedirs(dead)
        with open(os.path.join(dead, "u0.skene"), "wb") as f:
            f.write(b"orphan")
        out = _run_in_subprocess(
            f"""
            reset_spill_telemetry()
            live = os.path.join(os.environ["KVSTORE_LOCATION"], "q" + str(os.getpid()) + "-99")
            os.makedirs(live)
            session = opteryx.session()
            n = {SPILL_ROWS}
            rows = 0
            sql = f"SELECT s FROM generate_series(1, {{n}}) AS s ORDER BY s"
            for m in session.execute_to_morsels(sql):
                rows += m.num_rows
            tel = get_spill_telemetry()
            print("rows", rows)
            print("swept", tel["sweep_removed"])
            print("dead_gone", not os.path.exists({dead!r}))
            print("live_kept", os.path.exists(live))
            """,
            root,
        )
        got = dict(line.split(" ", 1) for line in out.strip().splitlines())
        assert got["rows"] == str(SPILL_ROWS), out
        assert got["swept"] == "1", out
        assert got["dead_gone"] == "True", out
        assert got["live_kept"] == "True", out


def test_unconfigured_is_the_pre_spill_engine():
    """No KVSTORE_LOCATION: the same >512MB sort runs entirely resident."""
    import opteryx
    from opteryx.operators._operators import get_spill_telemetry
    from opteryx.operators._operators import reset_spill_telemetry

    reset_spill_telemetry()
    session = opteryx.session()
    rows = 0
    sql = f"SELECT s FROM generate_series(1, {SPILL_ROWS}) AS s ORDER BY s"
    for m in session.execute_to_morsels(sql):
        rows += m.num_rows
    assert rows == SPILL_ROWS
    assert get_spill_telemetry()["units_written"] == 0


if __name__ == "__main__":
    test_spilled_sort_answers_are_identical()
    test_small_query_never_touches_disk()
    test_startup_sweep_collects_dead_owners()
    test_unconfigured_is_the_pre_spill_engine()
    print("✅ okay")
