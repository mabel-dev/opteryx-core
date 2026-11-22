"""HOST_CPU_COUNT (draken/core/host_cpus.h): the one host CPU count every width derives from.

Read once per process, so every case runs in a fresh interpreter. The `make wide`
floor leg depends on this override actually moving the widths - an override that
silently did nothing would leave that leg running at the runner's real width.
"""

import os
import subprocess
import sys

REPO_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

PROBE = f"""
import sys
sys.path.insert(1, {REPO_ROOT!r})
import opteryx
from opteryx import config
from opteryx.compiled.platform import cpu_count
from rugo import rugo_native
from rugo.parquet import _available_cpus
print(cpu_count(), config.resolve_max_execution_workers(None),
      config.resolve_parquet_local_io_workers(None), rugo_native.host_cpu_override(),
      _available_cpus())
"""


def _probe(value):
    env = dict(os.environ)
    env.pop("HOST_CPU_COUNT", None)
    if value is not None:
        env["HOST_CPU_COUNT"] = value
    return subprocess.run(
        [sys.executable, "-c", PROBE], cwd=REPO_ROOT, env=env, capture_output=True, text=True, timeout=120
    )


def test_override_moves_every_width():
    done = _probe("190")
    assert done.returncode == 0, done.stderr[-2000:]
    cpus, exec_workers, io_workers, rugo_override, rugo_cpus = map(int, done.stdout.split())
    # resolve_*: max(floor, cpu - 2, 0.8 * cpu) -> 188 at 190
    assert (cpus, exec_workers, io_workers, rugo_override, rugo_cpus) == (190, 188, 188, 190, 190)


def test_unset_is_the_hardware():
    done = _probe(None)
    assert done.returncode == 0, done.stderr[-2000:]
    cpus, _, _, rugo_override, _ = map(int, done.stdout.split())
    assert cpus == os.cpu_count()
    assert rugo_override == 0


def test_invalid_value_aborts_loudly():
    for bad in ("abc", "0", "-4", "12x"):
        done = _probe(bad)
        assert done.returncode != 0, (bad, done.stdout)
        assert f"HOST_CPU_COUNT={bad} is not a positive integer" in done.stderr, (bad, done.stderr[-500:])


if __name__ == "__main__":  # pragma: no cover
    test_override_moves_every_width()
    test_unset_is_the_hardware()
    test_invalid_value_aborts_loudly()
    print("okay")
