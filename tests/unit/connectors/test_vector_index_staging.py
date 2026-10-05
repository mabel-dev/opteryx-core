"""VECTOR_INDEX_STAGE_DIR: a GCS index build reads a local copy of its data file.

The build's GCS reads use a bearer token minted once and never refreshed, so a build
longer than the token's life failed mid-read. With the stage directory set, the data file
is copied there first and the build reads it locally, with no Authorization header;
the copy is removed afterwards, and a copy whose size disagrees with the manifest is
refused. Unset, nothing changes.
"""

import io as _io
import os
import sys

import pytest

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from opteryx import config
from opteryx.connectors import opteryx_connector

DATA = b"PAR1" + bytes(range(256)) * 64 + b"PAR1"


class _Task:
    data_file = "gs://bucket/table/data/f-1.parquet"
    data_bytes = len(DATA)
    deleted = ()
    path = "gs://bucket/table/index/abc/f-1-01.vidx"


class _FakeIO:
    def __init__(self, data=DATA):
        self.data = data
        self.cancelled = []

    def new_input(self, location):
        assert location == _Task.data_file
        data = self.data

        class _Input:
            def open(self):
                return _io.BytesIO(data)

        return _Input()

    def open_upload_session(self, path):
        return f"session:{path}"

    def cancel_upload_session(self, session):
        self.cancelled.append(session)


def _builder(seen):
    def build_to_session(data_file, column, deleted, embed_fn, dims, session, **options):
        seen.update(data_file=data_file, auth=options["auth_header"], session=session)
        with open(data_file, "rb") as handle:
            seen["bytes"] = handle.read()
        return {"file_bytes": 1}

    return build_to_session


def test_a_staged_build_reads_a_local_copy_without_a_token_and_removes_it(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "VECTOR_INDEX_STAGE_DIR", str(tmp_path / "stage"))
    seen = {}
    built = opteryx_connector._build_index_on_gcs(
        _FakeIO(), _Task(), "body", None, 384, {}, _builder(seen)
    )
    assert built == {"file_bytes": 1}
    assert seen["bytes"] == DATA
    assert seen["auth"] == ""
    assert seen["data_file"].startswith(str(tmp_path / "stage"))
    assert seen["session"] == f"session:{_Task.path}"
    assert os.listdir(tmp_path / "stage") == []          # the copy is gone


def test_a_staged_copy_of_the_wrong_size_is_refused(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "VECTOR_INDEX_STAGE_DIR", str(tmp_path / "stage"))
    with pytest.raises(RuntimeError, match="the manifest records"):
        opteryx_connector._build_index_on_gcs(
            _FakeIO(DATA[:-1]), _Task(), "body", None, 384, {}, _builder({})
        )
    assert os.listdir(tmp_path / "stage") == []


def test_unset_the_build_reads_gcs_with_the_bearer(monkeypatch):
    monkeypatch.setattr(config, "VECTOR_INDEX_STAGE_DIR", "")
    monkeypatch.setattr(opteryx_connector, "_index_reads",
                        lambda: (lambda path: (path, "Bearer t0k3n")))
    seen = {}

    def build_to_session(data_file, column, deleted, embed_fn, dims, session, **options):
        seen.update(data_file=data_file, auth=options["auth_header"])
        return {"file_bytes": 1}

    opteryx_connector._build_index_on_gcs(_FakeIO(), _Task(), "body", None, 384, {}, build_to_session)
    assert seen == {"data_file": _Task.data_file, "auth": "Bearer t0k3n"}
