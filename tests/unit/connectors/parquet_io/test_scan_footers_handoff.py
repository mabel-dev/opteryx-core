"""The footer gate hands its parsed footers to `open_native_scan_plan` by reference.

Both walk every file after acquiring every footer. With the parsed cache too small
for the scan's footers, each walk used to find its footers evicted and re-read every
file serially (2026-11-05: 429 remote footers, 2 x 32 s on the fleet). With a
`ScanFooters` passed from gate to plan, the plan reads nothing.
"""

import os
import subprocess
import sys
import textwrap

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../../.."))

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../.."))


def test_plan_reads_no_footer_the_gate_already_holds(tmp_path):
    # PARQUET_FOOTER_CACHE_BYTES=1 keeps every footer out of the parsed cache, so
    # nothing but the hand-off can spare the plan its own reads.
    paths = [str(tmp_path / f"part-{i}.parquet") for i in range(3)]
    script = textwrap.dedent(f"""
        import os
        import sys
        sys.path.insert(1, {REPO_ROOT!r})
        import opteryx
        from opteryx.connectors.parquet_io.pool_reader import ScanFooters
        from opteryx.connectors.parquet_io.pool_reader import native_scan_rejection
        from opteryx.connectors.parquet_io.pool_reader import open_native_scan_plan
        from rugo import parquet as rugo_parquet

        paths = {paths!r}
        morsels = list(opteryx.session().execute_to_morsels("SELECT id AS c FROM $planets"))
        for path in paths:
            with open(path, "wb") as handle:
                handle.write(rugo_parquet.write_parquet(morsels[0]))

        footers = ScanFooters()
        assert native_scan_rejection(paths, ["c"], ["int"], footers=footers) is None
        # The files are gone: the plan can only be built from the footers it was handed.
        for path in paths:
            os.remove(path)
        plan = open_native_scan_plan(paths, ["c"], decode_workers=1, footers=footers)
        plan.close()

        # Control: without the hand-off the plan reads them, so the check above bites.
        try:
            open_native_scan_plan(paths, ["c"], decode_workers=1)
        except Exception:
            print("OK")
    """)
    env = dict(os.environ, PARQUET_FOOTER_CACHE_BYTES="1")
    result = subprocess.run([sys.executable, "-c", script], env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip().endswith("OK")
