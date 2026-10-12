"""The footer gate caches per-file ADMIT verdicts; the cache must never change an answer.

`native_scan_rejection` remembers, per (path, request signature), that a file was
proven admissible against one parsed footer. These tests pin that a cached admit:
  - gives the same answer, including the absent-column record, as the full walk;
  - is keyed on the whole request (kinds, declared types, absent-fillable);
  - is never reused for a footer other than the one it was proven against;
and that rejections are not cached.
"""

import os
import subprocess
import sys
import textwrap

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../../.."))

import pytest

import opteryx
from opteryx.connectors.parquet_io.pool_reader import native_scan_rejection
from rugo import parquet as rugo_parquet

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../.."))


def _write(path, sql):
    morsels = list(opteryx.session().execute_to_morsels(sql))
    with open(path, "wb") as handle:
        handle.write(rugo_parquet.write_parquet(morsels[0]))
    return path


@pytest.fixture(scope="module")
def files(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("admit_cache")
    return {
        "both": _write(str(tmp_path / "both.parquet"),
                       "SELECT id AS i, name AS s FROM $planets"),
        "only_i": _write(str(tmp_path / "only_i.parquet"),
                         "SELECT id AS i FROM $planets"),
    }


def test_repeated_admit_is_stable(files):
    for _ in range(3):
        assert native_scan_rejection([files["both"]], ["i", "s"], ["int", "varchar"]) is None


def test_cached_admit_still_records_absent_columns(files):
    first, second = {}, {}
    assert native_scan_rejection([files["only_i"]], ["i", "s"], ["int", "varchar"], absent=first) is None
    assert native_scan_rejection([files["only_i"]], ["i", "s"], ["int", "varchar"], absent=second) is None
    assert first == {files["only_i"]: [1]}
    assert second == first


def test_admit_with_fillable_absent_does_not_admit_without(files):
    assert native_scan_rejection([files["only_i"]], ["i", "s"], ["int", "varchar"], absent={}) is None
    reason = native_scan_rejection([files["only_i"]], ["i", "s"], ["int", "varchar"])
    assert reason is not None and "'s' is not in" in reason


def test_admit_for_one_kind_does_not_admit_another(files):
    assert native_scan_rejection([files["both"]], ["s"], ["varchar"]) is None
    reason = native_scan_rejection([files["both"]], ["s"], ["int"])
    assert reason is not None and "'s'" in reason


def test_rejection_is_repeated_not_cached(files):
    first = native_scan_rejection([files["both"]], ["s"], ["float64"])
    second = native_scan_rejection([files["both"]], ["s"], ["float64"])
    assert first is not None
    assert second == first


def test_rewritten_file_is_proven_again(tmp_path):
    # PARQUET_FOOTER_CACHE_BYTES=1 keeps every footer out of the parsed cache, so each
    # call parses afresh: the admit proven against the old footer must not be reused
    # once the file at the same path holds another type.
    path = str(tmp_path / "rewritten.parquet")
    script = textwrap.dedent(f"""
        import sys
        sys.path.insert(1, {REPO_ROOT!r})
        import opteryx
        from opteryx.connectors.parquet_io.pool_reader import native_scan_rejection
        from rugo import parquet as rugo_parquet

        def write(sql):
            morsels = list(opteryx.session().execute_to_morsels(sql))
            with open({path!r}, "wb") as handle:
                handle.write(rugo_parquet.write_parquet(morsels[0]))

        write("SELECT id AS c FROM $planets")
        assert native_scan_rejection([{path!r}], ["c"], ["int"]) is None
        write("SELECT name AS c FROM $planets")
        reason = native_scan_rejection([{path!r}], ["c"], ["int"])
        assert reason is not None, "stale admit reused for a rewritten file"
        print("OK")
    """)
    env = dict(os.environ, PARQUET_FOOTER_CACHE_BYTES="1")
    result = subprocess.run([sys.executable, "-c", script], env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip().endswith("OK")
