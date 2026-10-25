# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
rugo's ORC reader (docs/ORC_READER_DESIGN.md, basic support) against files written by
pyarrow.orc - the Apache ORC C++ library, an independent writer. Every expected value
is the value the test wrote.

PyArrow is test-only here (CLAUDE.md §4).
"""

import datetime
import decimal
import io
import os
import random
import sys
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

# pyarrow's ORC writer records the machine's zone; a TIMESTAMP is wall-clock in it.
os.environ["TZ"] = "UTC"
time.tzset()

import pyarrow as pa  # noqa: E402
import pyarrow.orc as po  # noqa: E402
from draken.draken_native import DrakenType  # noqa: E402

from rugo import orc  # noqa: E402

UTC = datetime.timezone.utc
CODECS = ["uncompressed", "zlib", "snappy", "lz4", "zstd"]


def _write(table, **options):
    buf = io.BytesIO()
    po.write_table(table, buf, **options)
    return buf.getvalue()


def _read(data, **kwargs):
    """{column: [values]} across every stripe."""
    out = {}
    for morsel in orc.read_orc(data, **kwargs):
        for name in morsel.column_names:
            key = name.decode() if isinstance(name, bytes) else name
            out.setdefault(key, []).extend(morsel.column(name).to_pylist())
    return out


def _naive(values):
    return [None if v is None else v.replace(tzinfo=None) for v in values]


def _wide_table(n=5000, seed=1):
    rng = random.Random(seed)
    pattern = (
        [7] * 600 + list(range(1000)) + list(range(1000, 0, -1)) + [rng.randrange(10**6) for _ in range(1500)]
        + [5 if i % 97 else 10**12 for i in range(1000)]
    )
    runs = pattern * (n // len(pattern) + 1)
    return pa.table(
        {
            "i64": pa.array([None if i % 7 == 0 else i * 3 - 100 for i in range(n)], pa.int64()),
            "i32": pa.array([i % 1000 - 500 for i in range(n)], pa.int32()),
            "i16": pa.array([i % 100 for i in range(n)], pa.int16()),
            "i8": pa.array([i % 100 - 50 for i in range(n)], pa.int8()),
            "f64": pa.array([None if i % 5 == 0 else i / 3 for i in range(n)], pa.float64()),
            "f32": pa.array([i / 8 for i in range(n)], pa.float32()),
            "b": pa.array([None if i % 17 == 0 else i % 3 == 0 for i in range(n)], pa.bool_()),
            "s": pa.array([None if i % 11 == 0 else f"name-{i % 50}" for i in range(n)], pa.string()),
            "long": pa.array(["x" * (i % 40) + str(i) for i in range(n)]),
            "bin": pa.array([bytes([i % 256]) * (i % 5) for i in range(n)], pa.binary()),
            "d": pa.array(
                [datetime.date(2026, 1, 1) + datetime.timedelta(days=i % 400) for i in range(n)]
            ),
            "ts": pa.array(
                [
                    datetime.datetime(1999, 1, 1) + datetime.timedelta(seconds=i * 12345, microseconds=i * 7)
                    for i in range(n)
                ],
                pa.timestamp("us"),
            ),
            "dec": pa.array([decimal.Decimal(i - 2500) / 100 for i in range(n)], pa.decimal128(9, 2)),
            "dec38": pa.array([decimal.Decimal(i) * 10**20 / 100 for i in range(n)], pa.decimal128(38, 2)),
            # runs of identical, ascending, descending and random values exercise every
            # integer run-length sub-encoding; the outliers force patched-base runs
            "runs": pa.array(runs[:n], pa.int64()),
        }
    )


def _expect(table):
    expected = table.to_pydict()
    expected["ts"] = _naive(expected["ts"])
    return expected


def _assert_same(got, expected):
    got = dict(got)
    got["ts"] = _naive(got["ts"])
    for name, values in expected.items():
        assert got[name] == values, name


@pytest.mark.parametrize("codec", CODECS)
def test_every_type_reads_back_as_written_under_each_codec(codec):
    table = _wide_table()
    data = _write(table, compression=codec, stripe_size=64 * 1024)
    meta = orc.read_metadata(data)
    assert meta.compression == ("none" if codec == "uncompressed" else codec)
    assert meta.num_rows == 5000
    assert meta.stripes > 1 or codec != "uncompressed"
    _assert_same(_read(data), _expect(table))


@pytest.mark.parametrize("version", ["0.11", "0.12"])
@pytest.mark.parametrize("dictionary", [0.0, 1.0], ids=["direct", "dictionary"])
def test_run_length_v1_and_v2_with_direct_and_dictionary_strings(version, dictionary):
    table = _wide_table()
    data = _write(
        table, file_version=version, dictionary_key_size_threshold=dictionary, compression="zlib"
    )
    _assert_same(_read(data), _expect(table))


def test_streams_that_span_many_compression_chunks():
    # 64 KiB is the smallest block the writer accepts; this table's streams run to
    # several hundred KiB, so runs cross chunk boundaries.
    table = _wide_table(60000)
    data = _write(table, compression="zlib", compression_block_size=65536, stripe_size=64 * 1024 * 1024)
    _assert_same(_read(data), _expect(table))


@pytest.mark.parametrize("nulls", [0.0, 0.5, 1.0])
def test_null_density(nulls):
    n = 3000
    rng = random.Random(7)
    keep = [rng.random() >= nulls for _ in range(n)]
    table = pa.table(
        {
            "i": pa.array([i if k else None for i, k in zip(range(n), keep)], pa.int64()),
            "s": pa.array([f"v{i}" if k else None for i, k in zip(range(n), keep)], pa.string()),
            "d": pa.array([decimal.Decimal(i) if k else None for i, k in zip(range(n), keep)], pa.decimal128(12, 0)),
            "f": pa.array([float(i) if k else None for i, k in zip(range(n), keep)], pa.float64()),
            "b": pa.array([i % 2 == 0 if k else None for i, k in zip(range(n), keep)], pa.bool_()),
        }
    )
    assert _read(_write(table)) == table.to_pydict()


def test_integer_extremes_and_signs():
    values = [0, 1, -1, 2**63 - 1, -(2**63), 2**31, -(2**31), 255, -256, 65535, 123456789012345]
    table = pa.table({"x": pa.array(values * 50, pa.int64())})
    assert _read(_write(table, compression="snappy"))["x"] == values * 50


def test_an_empty_file():
    table = pa.table({"x": pa.array([], pa.int64()), "s": pa.array([], pa.string())})
    data = _write(table)
    assert orc.read_metadata(data).num_rows == 0
    assert _read(data).get("x", []) == []


def test_projection_reads_only_the_named_columns():
    table = _wide_table(500)
    data = _write(table)
    got = _read(data, columns=["s", "i32"])
    assert list(got) == ["s", "i32"]
    assert got["s"] == table.to_pydict()["s"]


def test_a_timestamp_instant_and_a_utc_timestamp():
    values = [datetime.datetime(2026, 3, 1, 12, 30, 45, 123456 * 1) + datetime.timedelta(hours=i) for i in range(300)]
    table = pa.table(
        {
            "naive": pa.array(values, pa.timestamp("us")),
            "instant": pa.array([v.replace(tzinfo=UTC) for v in values], pa.timestamp("us", tz="UTC")),
        }
    )
    meta = orc.read_metadata(_write(table))
    assert {c["name"]: c["kind"] for c in meta.columns} == {"naive": "TIMESTAMP", "instant": "TIMESTAMP_INSTANT"}
    got = _read(_write(table))
    assert _naive(got["naive"]) == values
    assert _naive(got["instant"]) == values


def test_a_timestamp_written_in_another_zone_is_refused_naming_it():
    # pyarrow always records the writer zone as "GMT", so name another one by patching the
    # stripe footer (uncompressed; same length, so no offset moves).
    data = _write(
        pa.table({"t": pa.array([datetime.datetime(2026, 1, 1)], pa.timestamp("us"))}), compression="uncompressed"
    )
    marker = b"\x1a\x03GMT"
    assert data.count(marker) == 1
    patched = data.replace(marker, b"\x1a\x03EST")
    with pytest.raises(RuntimeError, match=r"column 't'.*time zone 'EST'.*only UTC"):
        _read(patched)
    assert _naive(_read(data)["t"]) == [datetime.datetime(2026, 1, 1)]


class TestReaderSchema:
    """The caller's types, name matching, and what a file may be read as."""

    def _file(self):
        table = pa.table(
            {
                "a": pa.array([1, 2, 3], pa.int32()),
                "f": pa.array([0.5, 1.5, 2.5], pa.float32()),
                "s": pa.array(["x", "y", None]),
                "d": pa.array([decimal.Decimal("1.25")] * 3, pa.decimal128(5, 2)),
            }
        )
        return _write(table)

    def test_integers_and_floats_widen_with_conversion(self):
        reader = [
            ("a", None, DrakenType.INT64.value, 0, 0, True),
            ("f", None, DrakenType.FLOAT64.value, 0, 0, True),
        ]
        got = _read(self._file(), reader=reader)
        assert got["a"] == [1, 2, 3]
        assert got["f"] == [0.5, 1.5, 2.5]

    def test_a_column_the_file_lacks_is_null_when_nullable(self):
        reader = [
            ("a", None, DrakenType.INT32.value, 0, 0, True),
            ("added", None, DrakenType.INT64.value, 0, 0, True),
            ("added_s", None, DrakenType.VARCHAR.value, 0, 0, True),
        ]
        got = _read(self._file(), reader=reader)
        assert got["added"] == [None] * 3
        assert got["added_s"] == [None] * 3

    def test_a_required_column_the_file_lacks_is_an_error(self):
        reader = [("added", None, DrakenType.INT64.value, 0, 0, False)]
        with pytest.raises(RuntimeError, match="column 'added' is required but the file does not have it"):
            _read(self._file(), reader=reader)

    @pytest.mark.parametrize(
        "name, dtype, message",
        [
            ("a", DrakenType.INT16, "INT cannot be read as INT16"),
            ("a", DrakenType.VARCHAR, "INT cannot be read as VARCHAR"),
            ("s", DrakenType.INT64, "STRING cannot be read as INT64"),
            ("f", DrakenType.INT64, "FLOAT cannot be read as INT64"),
        ],
    )
    def test_incompatible_types_name_the_column(self, name, dtype, message):
        with pytest.raises(RuntimeError, match=f"column '{name}': the file's {message}"):
            _read(self._file(), reader=[(name, None, dtype.value, 0, 0, True)])

    def test_a_decimal_of_another_scale_is_refused(self):
        reader = [("d", None, DrakenType.DECIMAL.value, 5, 3, True)]
        with pytest.raises(RuntimeError, match="decimal scale 2 cannot be read as scale 3"):
            _read(self._file(), reader=reader)

    def test_a_wider_decimal_precision_reads(self):
        reader = [("d", None, DrakenType.DECIMAL128.value, 30, 2, True)]
        assert _read(self._file(), reader=reader)["d"] == [decimal.Decimal("1.25")] * 3

    def test_matching_by_iceberg_id_needs_the_file_to_carry_them(self):
        reader = [("a", 1, DrakenType.INT32.value, 0, 0, True)]
        with pytest.raises(RuntimeError, match="none of the file's columns carries an `iceberg.id`"):
            _read(self._file(), reader=reader, match_by_id=True)


def test_a_column_that_cannot_be_surfaced_is_refused_but_does_not_stop_the_others():
    # STRUCT / LIST / MAP read (tests/rugo/test_orc_nested.py); a MAP with non-string keys cannot be a
    # JSON object, so it is refused, naming the column and why.
    table = pa.table(
        {
            "ok": pa.array([1, 2], pa.int64()),
            "lst": pa.array([[1, 2], [3]], pa.list_(pa.int64())),
            "bad": pa.array([[(1, 2)], []], pa.map_(pa.int64(), pa.int64())),
        }
    )
    data = _write(table)
    meta = orc.read_metadata(data)
    by_name = {c["name"]: c for c in meta.columns}
    assert by_name["ok"]["readable"] is True
    assert by_name["lst"]["readable"] is True and by_name["lst"]["nested"] is True
    assert by_name["bad"]["readable"] is False and "keys" in by_name["bad"]["refusal"]
    assert _read(data, columns=["ok", "lst"]) == {"ok": [1, 2], "lst": [[1, 2], [3]]}
    with pytest.raises(RuntimeError, match="column 'bad': its MAP has LONG keys"):
        _read(data, columns=["bad"])
    with pytest.raises(RuntimeError, match="column 'bad': its MAP has LONG keys"):
        _read(data)


class TestCorruption:
    """A damaged file raises; it never crashes, hangs or returns zeros."""

    def _data(self):
        return _write(_wide_table(400), compression="zlib")

    def test_not_an_orc_file(self):
        with pytest.raises(RuntimeError, match='magic "ORC"'):
            orc.read_metadata(b"not an orc file at all")

    def test_empty(self):
        with pytest.raises(RuntimeError):
            orc.read_metadata(b"")

    def test_every_truncation_raises(self):
        data = self._data()
        for cut in list(range(0, 64)) + list(range(64, len(data), max(1, len(data) // 150))):
            with pytest.raises(RuntimeError):
                _read(data[:cut])

    def test_flipped_bytes_either_raise_or_read_without_crashing(self):
        data = bytearray(self._data())
        rng = random.Random(3)
        for _ in range(300):
            damaged = bytearray(data)
            for _ in range(rng.randrange(1, 4)):
                damaged[rng.randrange(len(damaged))] ^= 1 << rng.randrange(8)
            try:
                _read(bytes(damaged))
            except RuntimeError:
                pass


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
