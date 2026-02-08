import sys
import datetime

import pytest

from opteryx.connectors.io_systems import s3_filesystem as s3mod


def test_format_value_for_sql_py_date():
    formatted = s3mod._format_value_for_sql(datetime.date(2021, 1, 2))
    assert formatted == "'2021-01-02'"


def test_format_value_for_sql_py_datetime():
    formatted = s3mod._format_value_for_sql(datetime.datetime(2021, 1, 2, 3, 4, 5))
    assert formatted == "'2021-01-02T03:04:05'"


def test_format_value_for_sql_none_becomes_null():
    formatted = s3mod._format_value_for_sql(None)
    assert formatted == "NULL"


def test_format_value_for_sql_numpy_datetime64():
    np = pytest.importorskip("numpy")
    val = np.datetime64("2021-01-02T03:04:05")
    formatted = s3mod._format_value_for_sql(val)
    assert "2021-01-02" in formatted
    assert formatted.startswith("'") and formatted.endswith("'")


def test_opteryx_build_select_query_quotes_date(monkeypatch):
    # Provide fake minio module so OpteryxS3FileSystem can be instantiated
    class _FakeMinioModule:
        class Minio:
            def __init__(self, *args, **kwargs):
                pass

    sys.modules["minio"] = _FakeMinioModule()

    fs = s3mod.OpteryxS3FileSystem(S3_END_POINT="dummy", S3_ACCESS_KEY="a", S3_SECRET_KEY="b", S3_SECURE=False)
    sql = fs._build_select_query(None, [[("d", "=", datetime.date(2021, 1, 2))]])
    assert "'2021-01-02'" in sql


def test_format_value_for_sql_pandas_timestamp():
    pd = pytest.importorskip("pandas")
    val = pd.Timestamp("2021-01-02T03:04:05")
    formatted = s3mod._format_value_for_sql(val)
    assert "2021-01-02" in formatted
    assert formatted.startswith("'") and formatted.endswith("'")
