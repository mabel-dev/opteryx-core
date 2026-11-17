"""
A local read failure inside the column fan-out decode raises; it does not abort (2026-11-15).

The fan-out's helper tasks ran `decode_col` on the column-fan pool with nothing to
catch what it threw, so a failed or short `pread` reached std::terminate and killed
the process ("libc++abi: terminating due to uncaught exception ... Short read").
That is how one `make q` run died in test_fused_topn_no_predicate_now_native.

The file here has its footer intact but 200 KB cut from the end of its data, so the
last column chunks run past EOF and their read comes back short.
"""

import os
import shutil
import struct
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx

SOURCE = os.path.join(os.path.dirname(__file__), "../../testdata/clickbench_tiny/hits_48.parquet")


def test_short_local_read_raises_not_aborts(tmp_path, monkeypatch):
    raw = open(SOURCE, "rb").read()
    footer_length = struct.unpack("<I", raw[-8:-4])[0]
    footer = raw[-(footer_length + 8):]
    data = raw[: -(footer_length + 8)]
    os.makedirs(tmp_path / "short")
    with open(tmp_path / "short" / "hits.parquet", "wb") as f:
        f.write(data[:-200_000] + footer)

    monkeypatch.chdir(tmp_path)
    # The chunk straddling the cut reads the appended footer bytes and fails to
    # decompress; the chunks past it read short. Which column's worker throws first
    # is timing, and either one proves the fan-out raises instead of aborting.
    with pytest.raises(Exception, match="Short read|Snappy decompression failed"):
        for _ in opteryx.session().execute_to_morsels("SELECT * FROM short ORDER BY EventTime LIMIT 10"):
            pass
    shutil.rmtree(tmp_path / "short")


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-q"])
