"""The per-column layout through the engine's own writes and OPTIMIZE
(PARQUET_WRITE_NARROW_CHUNK_BYTES, docs/PARQUET_WRITE_FOR_READ_SPEED.md §4.7.3).

On the local store:
  * off (the default), a written file carries no `rugo.narrow_chunk_bytes`
    stamp — the grouped layout, as before;
  * on, OPTIMIZE finds the file written without it (layout debt) and rewrites
    it in place, through `open_data_file_writer`, with the stamp; the data is
    unchanged; a second OPTIMIZE has nothing to do — the migration terminates;
  * an INSERT made with the knob on writes the stamp itself.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx import config
from opteryx.connectors import register_workspace
from rugo import parquet as rp

KEY = "rugo.narrow_chunk_bytes"


@pytest.fixture
def ws(tmp_path):
    from opteryx.connectors.local_store_connector import LocalStoreConnector

    name = f"nl{os.getpid()}"
    root = str(tmp_path)
    register_workspace(name, lambda **kw: LocalStoreConnector(store_root=root, **kw))
    return name, root


def _data_files(root):
    return sorted(
        os.path.join(d, f) for d, _, fs in os.walk(root) for f in fs
        if f.startswith("data-") and f.endswith(".parquet"))


def _stamp(path):
    import pyarrow.parquet as pq  # oracle only

    kv = pq.ParquetFile(path).metadata.metadata or {}
    value = kv.get(KEY.encode())
    return None if value is None else value.decode()


def _q(sql):
    """Run one statement; its rows as tuples."""
    session = opteryx.session()
    rows = []
    for m in session.execute_to_morsels(sql):
        rows.extend(zip(*[m.column(n).to_pylist() for n in m.column_names]))
    session.close()
    return rows


def _rows(sql):
    return sorted(_q(sql))


def test_optimize_moves_existing_files_to_the_layout(ws, monkeypatch):
    name, root = ws
    rel = f"{name}.layout_t"
    _q(f"CREATE TABLE {rel} (a BIGINT, b VARCHAR)")
    _q(f"INSERT INTO {rel} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    before = _rows(f"SELECT a, b FROM {rel}")
    files = _data_files(root)
    assert len(files) == 1 and _stamp(files[0]) is None

    # Off: OPTIMIZE has nothing to do with a single settled file.
    _q(f"OPTIMIZE TABLE {rel}")
    assert _data_files(root) == files

    monkeypatch.setattr(config, "PARQUET_WRITE_NARROW_CHUNK_BYTES", 32768)
    _q(f"OPTIMIZE TABLE {rel}")
    after = _data_files(root)
    assert after != files and len(after) == 1, after
    assert _stamp(after[0]) == "32768"
    assert rp.read_metadata(after[0]).num_rows == 3
    assert _rows(f"SELECT a, b FROM {rel}") == before

    # The migration terminates: the rewritten file is current.
    _q(f"OPTIMIZE TABLE {rel}")
    assert _data_files(root) == after

    # A threshold change makes it stale again.
    monkeypatch.setattr(config, "PARQUET_WRITE_NARROW_CHUNK_BYTES", 65536)
    _q(f"OPTIMIZE TABLE {rel}")
    again = _data_files(root)
    assert again != after and _stamp(again[0]) == "65536"
    assert _rows(f"SELECT a, b FROM {rel}") == before


def test_insert_with_the_knob_on_writes_the_layout(ws, monkeypatch):
    name, root = ws
    rel = f"{name}.layout_i"
    monkeypatch.setattr(config, "PARQUET_WRITE_NARROW_CHUNK_BYTES", 32768)
    _q(f"CREATE TABLE {rel} (a BIGINT, b VARCHAR)")
    _q(f"INSERT INTO {rel} VALUES (1, 'a'), (2, 'b')")
    files = _data_files(root)
    assert len(files) == 1 and _stamp(files[0]) == "32768"
    assert _rows(f"SELECT a, b FROM {rel}") == [(1, "a"), (2, "b")]


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
