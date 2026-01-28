import pyarrow as pa
import pyarrow.parquet as pq

from opteryx.utils.file_decoders import parquet_decoder


def make_parquet_bytes():
    table = pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    buf = sink.getvalue().to_pybytes()
    return buf


def test_parquet_decoder_can_read_from_memoryview():
    data = make_parquet_bytes()
    mv = memoryview(data)

    num_rows, num_columns, raw_bytes, table = parquet_decoder(mv)

    assert num_rows == 3
    assert num_columns >= 1
    assert raw_bytes >= 0
    assert list(table.column_names) == ["a", "b"]
    assert table.num_rows == 3
