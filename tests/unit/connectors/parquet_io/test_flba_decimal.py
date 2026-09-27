"""
Regression test: FIXED_LEN_BYTE_ARRAY-encoded DECIMAL columns.

PyArrow writes DECIMAL(p,s) as FLBA(min_bytes_for_p) when min_bytes < 8, while
parquet-rs (tpchgen-cli) writes them as INT64. Both are spec-valid. Until this
fix, the rugo decoder rejected FLBA outright, so any PyArrow-written parquet
containing decimals failed with "Decoder returned None".
"""

import os
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import opteryx
from opteryx.connectors import DiskConnector


def _read_one_column(parquet_path, column):
    """Read `column` from the parquet file at `parquet_path`. Sets cwd so
    that DiskConnector resolves the workspace prefix to the parent of the
    table directory."""
    table_dir = os.path.dirname(parquet_path)            # /tmp/.../<workspace>/<table>
    workspace_dir = os.path.dirname(table_dir)           # /tmp/.../<workspace>
    workspace_name = os.path.basename(workspace_dir)
    cwd_dir = os.path.dirname(workspace_dir)             # /tmp/...
    table_name = os.path.basename(table_dir)

    cwd = os.getcwd()
    os.chdir(cwd_dir)
    try:
        opteryx.register_workspace(workspace_name, DiskConnector)
        rows = []
        for morsel in opteryx.session().execute_to_morsels(
            f"SELECT {column} FROM {workspace_name}.{table_name}"
        ):
            rows.extend(morsel.column(column.encode()).to_pylist())
        return rows
    finally:
        os.chdir(cwd)


def test_flba_decimal_roundtrip():
    """DECIMAL(15,2) written as FLBA(7) by PyArrow decodes correctly."""
    from decimal import Decimal
    decimals = [Decimal(v) for v in
                ("0.00", "1.50", "-2.25", "12345.67", "-9999999999999.99")]
    table = pa.table({"x": pa.array(decimals, type=pa.decimal128(15, 2))})

    with tempfile.TemporaryDirectory() as tmp:
        data_dir = os.path.join(tmp, "ws_a", "decimal_table")
        os.makedirs(data_dir)
        parquet_path = os.path.join(data_dir, "data.parquet")
        pq.write_table(table, parquet_path, compression="snappy")

        # Confirm PyArrow chose FLBA physical type (otherwise this test isn't
        # exercising the path it claims to).
        meta = pq.read_metadata(parquet_path)
        col0 = meta.schema.column(0)
        assert col0.physical_type == "FIXED_LEN_BYTE_ARRAY", col0.physical_type

        rows = _read_one_column(parquet_path, "x")
        assert rows == decimals, rows


def test_flba_decimal_dict_encoded():
    """Dict-encoded FLBA DECIMAL column: the same value repeated triggers the
    dict-page parse path, which decodes FLBA bytes into the int64 dictionary."""
    from decimal import Decimal
    table = pa.table({"y": pa.array([Decimal("3.14")] * 100, type=pa.decimal128(15, 2))})
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = os.path.join(tmp, "ws_b", "dict_decimal")
        os.makedirs(data_dir)
        parquet_path = os.path.join(data_dir, "data.parquet")
        pq.write_table(table, parquet_path, compression="snappy", use_dictionary=True)

        rows = _read_one_column(parquet_path, "y")
        assert rows == [Decimal("3.14")] * 100


def test_flba_wide_decimal_reads_exactly():
    """FLBA wider than 8 bytes (DECIMAL(20,0) -> FLBA(9)) decodes to DECIMAL128.

    pyarrow DICTIONARY-encodes it by default, which rugo used to route to the pool
    path the native scan could not consume ("unsupported column encoding"); it is
    now the DK_DECIMAL128_DICT direct kind. This test used to assert that the read
    raised - it now asserts the exact values, dictionary-encoded and plain."""
    from decimal import Decimal

    values = [1, 2, 3, 2, 1, None, 10**19 + 7]
    for use_dictionary in (True, False):
        with tempfile.TemporaryDirectory() as tmp:
            # A distinct workspace per iteration: _read_one_column resolves a
            # RELATIVE path, and footers are cached per path within the process.
            workspace = "flba_wide_dict" if use_dictionary else "flba_wide_plain"
            path = os.path.join(tmp, workspace, "wide_dec", "data.parquet")
            os.makedirs(os.path.dirname(path))
            pq.write_table(
                pa.table({"y": pa.array(values, type=pa.decimal128(20, 0))}),
                path,
                use_dictionary=use_dictionary,
            )
            rows = _read_one_column(path, "y")
            expected = [None if v is None else Decimal(v) for v in values]
            assert rows == expected, (use_dictionary, rows)


def test_flba_non_decimal_fails_cleanly():
    """Non-DECIMAL FLBA (UUID-style FLBA(16)) must raise rather than silently
    produce garbage."""
    with tempfile.TemporaryDirectory() as tmp:
        ws_dir = os.path.join(tmp, "flba_unsupported")
        os.makedirs(os.path.join(ws_dir, "uuid"))
        pq.write_table(
            pa.table({"x": pa.array([b"\x01" * 16, b"\x02" * 16], type=pa.binary(16))}),
            os.path.join(ws_dir, "uuid", "data.parquet"),
        )

        cwd = os.getcwd()
        os.chdir(tmp)
        try:
            opteryx.register_workspace("flba_unsupported", DiskConnector)
            raised = False
            try:
                for _ in opteryx.session().execute_to_morsels(
                    "SELECT * FROM flba_unsupported.uuid"
                ):
                    pass
            except Exception:
                raised = True
            assert raised, "unsupported FLBA in uuid silently succeeded"
        finally:
            os.chdir(cwd)


if __name__ == "__main__":
    test_flba_decimal_roundtrip()
    test_flba_decimal_dict_encoded()
    test_flba_wide_decimal_reads_exactly()
    test_flba_non_decimal_fails_cleanly()
    print("✅ FLBA DECIMAL regression tests passed")
