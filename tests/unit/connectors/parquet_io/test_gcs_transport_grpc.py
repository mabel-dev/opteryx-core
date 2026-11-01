"""
GCS_TRANSPORT=grpc reaches the native pipeline (design docs/GCS_DIRECTPATH_READS_DESIGN.md
§3/§5). Offline: a gs:// row group on a pipeline armed for DirectPath must go to
the DirectPath reader, not REST, and that reader refuses a scan with no bearer
credential before touching the network. Also the config refuses unknown values.
"""

import os
import subprocess
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import pytest

PLANETS = os.path.join(os.path.dirname(__file__), "../../../../testdata/planets/planets.parquet")


def _first_column_stats():
    from opteryx.connectors.parquet_io.pool_reader import fetch_column_chunk_info

    info = fetch_column_chunk_info(os.path.abspath(PLANETS), 0, ["name"])
    return [info["name"]]


def test_grpc_transport_takes_gs_reads_and_needs_a_credential(monkeypatch):
    from opteryx import config
    from opteryx.connectors.parquet_io.pool_reader import CppIOPipeline

    monkeypatch.setattr(config, "GCS_TRANSPORT", "grpc")
    pipe = CppIOPipeline(decode_workers=1, queue_capacity=4)
    try:
        assert pipe.diagnostics()["gcs_transport"] == "grpc"
        pipe.submit_work("gs://no-such-bucket/planets.parquet", 0, ["name"], _first_column_stats())
        result = pipe.wait_result()
        assert result is not None, "pipeline drained without a result"
        assert result["success"] is False
        assert "DirectPath (GCS_TRANSPORT=grpc) needs a bearer credential" in result["error"], result["error"]
    finally:
        pipe.close()


def test_default_transport_reports_rest():
    from opteryx.connectors.parquet_io.pool_reader import CppIOPipeline

    pipe = CppIOPipeline(decode_workers=1, queue_capacity=4)
    try:
        assert pipe.diagnostics()["gcs_transport"] == "rest"
    finally:
        pipe.close()


@pytest.mark.parametrize("value", ["auto", "http", "GRPCX"])
def test_unknown_transport_is_refused(value):
    env = {**os.environ, "GCS_TRANSPORT": value}
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../.."))
    proc = subprocess.run([sys.executable, "-c", "import opteryx.config"], env=env, cwd=root,
                          capture_output=True, text=True)
    assert proc.returncode != 0
    assert "GCS_TRANSPORT must be 'rest' or 'grpc'" in proc.stderr


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
