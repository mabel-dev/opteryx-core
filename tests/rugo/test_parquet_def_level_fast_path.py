"""
The all-present definition-level fast path (decode_column.cpp, "All-present
definition-level fast path") skips expanding a page's def levels when the
stream is one RLE run at max_definition_level, and keeps all_def_levels
EITHER empty OR complete for every row so far. These tests pin that
invariant on chunks that MIX clean pages with pages that carry nulls, in
every order: a clean page after a null page must still append real levels,
and clean pages before a null page must be materialised before it appends.

Each clean segment of a chunk is checked to hold at least one WHOLE page, so the
order the fast path sees is the order the test intends. Fixtures are written
with rugo and with PyArrow (allowed in tests/ only; v1 and v2 pages), and
read back with rugo and through the engine.
"""

import io
import os
import random
import sys
import tempfile

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import thriftpy2
from thriftpy2.protocol import TCompactProtocolFactory
from thriftpy2.protocol.compact import TCompactProtocol
from thriftpy2.utils import deserialize

import opteryx
import rugo.parquet as rp
from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from opteryx.connectors import DiskConnector

SEG = 1000  # rows per segment; pages are smaller, so every segment holds whole pages
_SPEC = os.path.join(os.path.dirname(__file__), "../../rugo/src/parquet_spec/parquet.thrift")
_THRIFT = thriftpy2.load(_SPEC, module_name="parquet_def_level_fast_path_thrift")
_DATA_PAGE, _DATA_PAGE_V2 = 0, 3

# C = no nulls, N = some nulls, E = every row null
PATTERNS = ["CN", "NC", "CNC", "NCN", "CCNNC", "EC", "CE", "CEC", "CCC", "NNN"]


def _values(pattern, kind):
    out = []
    for s, seg in enumerate(pattern):
        for i in range(SEG):
            row = s * SEG + i
            null = seg == "E" or (seg == "N" and i % 7 == 3)
            out.append(None if null else (row if kind == "int" else f"v{row % 50}"))
    return out


def _data_page_rows(buf, name="x"):
    """num_values of every data page of column `name` in row group 0, from the page headers."""
    length = int.from_bytes(buf[-8:-4], "little")
    fmd = deserialize(_THRIFT.FileMetaData(), buf[-8 - length : -8], TCompactProtocolFactory())
    meta = next(c.meta_data for c in fmd.row_groups[0].columns if c.meta_data.path_in_schema[0] == name)
    pos = meta.dictionary_page_offset if meta.dictionary_page_offset else meta.data_page_offset
    end = pos + meta.total_compressed_size
    rows = []
    while pos < end:
        stream = io.BytesIO(buf[pos:end])  # pure-Python protocol: tell() is the header length
        header = _THRIFT.PageHeader()
        TCompactProtocol(stream).read_struct(header)
        pos += stream.tell() + header.compressed_page_size
        if header.type == _DATA_PAGE:
            rows.append(header.data_page_header.num_values)
        elif header.type == _DATA_PAGE_V2:
            rows.append(header.data_page_header_v2.num_values)
    return rows


def _assert_every_clean_segment_holds_a_whole_page(buf, pattern):
    starts, at = [], 0
    for n in _data_page_rows(buf):
        starts.append((at, at + n))
        at += n
    assert at == len(pattern) * SEG
    # Only clean segments must hold a whole page: that page is what takes the fast
    # path. PyArrow PLAIN never closes a page on an all-null batch (no value bytes),
    # so an E segment can share its page with the next segment — still a null page.
    for s in range(len(pattern)):
        if pattern[s] != "C":
            continue
        lo, hi = s * SEG, (s + 1) * SEG
        assert any(lo <= a and b <= hi for a, b in starts), (pattern, s, starts)


def _rugo_write(values, kind, dictionary):
    dtype = DrakenType.INT64 if kind == "int" else DrakenType.VARCHAR
    m = Morsel.from_vectors(["x"], [vector_from_sequence(values, dtype=dtype)])
    return rp.write_parquet(m, dictionary=dictionary, max_page_bytes=1024)


def _pyarrow_write(values, kind, dictionary, page_version):
    arr = pa.array(values, type=pa.int64() if kind == "int" else pa.string())
    sink = io.BytesIO()
    # data_page_size=1 + write_batch_size closes a page after every batch of rows.
    pq.write_table(pa.table({"x": arr}), sink, use_dictionary=dictionary, data_page_size=1,
                   write_batch_size=SEG // 4, data_page_version=page_version, compression="zstd")
    return sink.getvalue()


def _engine_rows(buf, sql):
    with tempfile.TemporaryDirectory() as tmp:
        workspace = f"ws_deflvl_{os.getpid()}_{random.randrange(1 << 30)}"
        data_dir = os.path.join(tmp, workspace, "t")
        os.makedirs(data_dir)
        with open(os.path.join(data_dir, "data.parquet"), "wb") as f:
            f.write(buf)
        opteryx.register_workspace(workspace, DiskConnector)
        cwd = os.getcwd()
        os.chdir(tmp)
        try:
            rows = []
            for morsel in opteryx.session().execute_to_morsels(sql.format(t=f"{workspace}.t")):
                rows.extend(zip(*[morsel.column(c).to_pylist() for c in morsel.column_names]))
            return rows
        finally:
            os.chdir(cwd)


_WRITERS = [
    pytest.param(lambda v, k, d: _rugo_write(v, k, d), id="rugo"),
    pytest.param(lambda v, k, d: _pyarrow_write(v, k, d, "1.0"), id="pyarrow-v1"),
    pytest.param(lambda v, k, d: _pyarrow_write(v, k, d, "2.0"), id="pyarrow-v2"),
]


@pytest.mark.parametrize("write", _WRITERS)
@pytest.mark.parametrize("dictionary", [False, True], ids=["plain", "dict"])
@pytest.mark.parametrize("kind", ["int", "str"])
@pytest.mark.parametrize("pattern", PATTERNS)
def test_mixed_clean_and_null_pages_round_trip(pattern, kind, dictionary, write):
    values = _values(pattern, kind)
    buf = write(values, kind, dictionary)
    _assert_every_clean_segment_holds_a_whole_page(buf, pattern)
    got = [v for m in rp.read_parquet(buf) for v in m.column(b"x").to_pylist()]
    assert got == values


@pytest.mark.parametrize("write", _WRITERS)
@pytest.mark.parametrize("pattern", ["CNC", "NCN", "CEC"])
def test_mixed_clean_and_null_pages_through_engine(pattern, write):
    values = _values(pattern, "int")
    buf = write(values, "int", False)
    _assert_every_clean_segment_holds_a_whole_page(buf, pattern)
    present = [v for v in values if v is not None]
    got = _engine_rows(buf, "SELECT COUNT(*), COUNT(x), SUM(x) FROM {t}")
    assert got == [(len(values), len(present), sum(present))]
    nulls = _engine_rows(buf, "SELECT COUNT(*) FROM {t} WHERE x IS NULL")
    assert nulls == [(len(values) - len(present),)]


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-q"])
