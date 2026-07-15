"""
Stable-surface regression: the public rugo actions the `space_missions.ipynb`
notebook advertises. The notebook itself isn't CI-runnable (it `!pip install
rugo` and `!wget`s data); this exercises the SAME public API against a
self-generated fixture so the advertised workflow can't silently break.

Covered (mirrors the notebook cells):
  - parquet.read_metadata(path)          → row/column counts, schema
  - parquet.read_parquet(path)           → streaming morsels, values
  - parquet.read_parquet(predicates=...) → row-group predicate pushdown
  - parquet.read_parquet(columns=...)    → column projection
  - jsonl.write_jsonl(morsel)            → Parquet → JSONL round-trip

PyArrow is used only to mint the fixture (allowed in tests per §4).
"""

import io
import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

import pyarrow as pa  # fixture mint only
import pyarrow.parquet as pq  # fixture mint only

from rugo import parquet
from rugo import jsonl


@pytest.fixture(scope="module")
def missions_path(tmp_path_factory):
    """A small 'space missions'-shaped parquet, written with PyArrow."""
    t = pa.table(
        {
            "Company": ["SpaceX", "NASA", "SpaceX", "Roscosmos", "SpaceX", "NASA"],
            "Location": ["LC-39A", "LC-39B", "SLC-40", "Site 1/5", "SLC-4E", "LC-39B"],
            "Price": [50.0, 450.0, 62.0, None, 50.0, 1160.0],
            "Success": [True, True, True, False, True, None],
        }
    )
    p = tmp_path_factory.mktemp("nb") / "space_missions.parquet"
    pq.write_table(t, str(p), compression="zstd")
    return str(p)


def _rows(reader):
    out = []
    for m in reader:
        names = [n.decode() if isinstance(n, bytes) else n for n in m.column_names]
        cols = {n: m.column(n.encode()).to_pylist() for n in names}
        for i in range(len(next(iter(cols.values())))):
            out.append({n: cols[n][i] for n in names})
    return out


def test_read_metadata(missions_path):
    meta = parquet.read_metadata(missions_path)
    assert meta.num_rows == 6
    names = [c.name for c in meta.schema_columns]
    assert set(names) == {"Company", "Location", "Price", "Success"}


def test_streaming_read_all(missions_path):
    with parquet.read_parquet(missions_path) as reader:
        rows = _rows(reader)
    assert len(rows) == 6
    assert rows[0]["Company"] == "SpaceX"
    assert rows[1]["Price"] == 450.0


def test_predicate_pushdown(missions_path):
    # Row-group pruning: a predicate that no row matches yields no rows; the
    # notebook's headline "filter without reading everything" behaviour.
    with parquet.read_parquet(missions_path, predicates=[("Company", "==", "NoSuchCo")]) as reader:
        assert _rows(reader) == []
    with parquet.read_parquet(missions_path, predicates=[("Price", ">", 1000.0)]) as reader:
        rows = _rows(reader)
    assert all(r["Price"] is None or r["Price"] > 1000.0 for r in rows)
    assert any(r["Price"] == 1160.0 for r in rows)


def test_column_projection(missions_path):
    with parquet.read_parquet(missions_path, columns=["Company", "Price"]) as reader:
        for m in reader:
            names = {n.decode() if isinstance(n, bytes) else n for n in m.column_names}
            assert names == {"Company", "Price"}


def test_parquet_to_jsonl_roundtrip(missions_path):
    lines = []
    with parquet.read_parquet(missions_path) as reader:
        for m in reader:
            lines.append(jsonl.write_jsonl(m))
    blob = b"".join(lines)
    records = [json.loads(l) for l in blob.decode("utf-8").splitlines() if l.strip()]
    assert len(records) == 6
    assert records[0]["Company"] == "SpaceX"
    assert records[5]["Company"] == "NASA"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
