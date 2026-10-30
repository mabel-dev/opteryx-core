"""A local dataset listing must fail on a directory it cannot read. It used to
skip it, so a `chmod 000` partition made COUNT(*) silently short."""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.compiled.nanobind.vectors import list_files_info


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads a chmod 000 directory")
def test_unreadable_partition_fails_the_listing(tmp_path):
    (tmp_path / "ok").mkdir()
    (tmp_path / "ok" / "a.parquet").write_bytes(b"x")
    locked = tmp_path / "locked"
    locked.mkdir()
    (locked / "b.parquet").write_bytes(b"x")
    locked.chmod(0)
    try:
        with pytest.raises(Exception):
            list_files_info(str(tmp_path), ())
    finally:
        locked.chmod(0o755)


def test_readable_tree_lists_every_file(tmp_path):
    (tmp_path / "p1").mkdir()
    (tmp_path / "p1" / "a.parquet").write_bytes(b"x")
    (tmp_path / "b.parquet").write_bytes(b"xy")
    files = sorted(path for path, _d, is_file, _s, _m in list_files_info(str(tmp_path), ()) if is_file)
    assert files == sorted([str(tmp_path / "p1" / "a.parquet"), str(tmp_path / "b.parquet")])


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
