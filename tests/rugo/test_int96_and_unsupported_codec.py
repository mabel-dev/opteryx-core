# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Regression: a non-empty Parquet file must never read back as zero rows.

rugo 0.4.30 had two columns it could not decode — BROTLI-compressed chunks and
INT96 timestamps — and in both cases the reader dropped the column instead of
failing. For a single-column file that left one morsel with no columns, so
`read_parquet` reported zero rows for a file whose own footer said 1000 (or
100), raising nothing. A wrong answer, not a missing feature.

The split now is:

  * INT96 is SUPPORTED. The Parquet spec assigns it exactly one meaning — a
    nanosecond timestamp — so it decodes to int64 nanos and the schema reports
    `timestamp[ns]`.
  * BROTLI is NOT supported (the codec is not vendored) and must raise with the
    codec named, the way an unsupported map column already raises.

The invariant the two share, and the one this file really guards, is that a
column rugo cannot decode is a loud failure — never a silently shorter answer.

PyArrow writes the fixtures (tests/ only) and is the value oracle.
"""

import datetime
import io
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import draken.draken_native as _dn
from rugo import parquet

EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


def _write(table, **kwargs) -> bytes:
    buf = io.BytesIO()
    pq.write_table(table, buf, **kwargs)
    return buf.getvalue()


def _read_column(data: bytes, name: str):
    """Read one column, returning (values, draken type, rows yielded)."""
    values = []
    dtype = None
    with parquet.read_parquet(data) as reader:
        for morsel in reader:
            column = morsel.column(name)
            dtype = column._nb.type
            values.extend(column.to_pylist())
    return values, dtype, len(values)


def _int96_table(instants, unit="us"):
    return pa.table({"ts": pa.array(instants, type=pa.timestamp(unit))})


def _int96_bytes(instants, **kwargs):
    return _write(
        _int96_table(instants),
        use_deprecated_int96_timestamps=True,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# INT96 — supported, and correct
# ---------------------------------------------------------------------------


def test_int96_yields_every_row():
    """The footer's num_rows and the rows actually yielded must agree."""
    base = datetime.datetime(2023, 11, 14, 22, 13, 20)
    instants = [base + datetime.timedelta(seconds=i) for i in range(1000)]
    data = _int96_bytes(instants)

    assert parquet.read_metadata(data).num_rows == 1000
    _, _, rows = _read_column(data, "ts")
    assert rows == 1000


def test_int96_decodes_as_timestamp_with_correct_values():
    """INT96 is a nanosecond timestamp, not an opaque 12-byte blob."""
    base = datetime.datetime(2023, 11, 14, 22, 13, 20)
    instants = [base + datetime.timedelta(seconds=i) for i in range(5)]

    values, dtype, _ = _read_column(_int96_bytes(instants), "ts")

    assert dtype == _dn.DrakenType.TIMESTAMP64
    assert values == [i.replace(tzinfo=datetime.timezone.utc) for i in instants]


def test_int96_schema_reports_timestamp_not_int96():
    """The advertised logical type must match what the decoder hands back."""
    data = _int96_bytes([datetime.datetime(2023, 11, 14)])
    logical = {c.name: c.logical_type for c in parquet.read_metadata(data).schema_columns}
    assert logical["ts"] == "timestamp[ns]"


@pytest.mark.parametrize(
    "instant",
    [
        datetime.datetime(1970, 1, 1),          # the Julian-day pivot itself
        datetime.datetime(1900, 3, 4, 5, 6, 7),  # pre-epoch: negative day count
        datetime.datetime(2262, 4, 11),          # last day int64 ns can hold
    ],
    ids=["epoch", "pre_epoch", "int64_ns_ceiling"],
)
def test_int96_epoch_boundaries(instant):
    """Julian day → Unix nanos must hold on both sides of the epoch."""
    values, _, _ = _read_column(_int96_bytes([instant]), "ts")
    assert values == [instant.replace(tzinfo=datetime.timezone.utc)]


def test_int96_dictionary_encoded_column():
    """Repeated values take the dictionary path; entries convert there too."""
    base = datetime.datetime(2023, 11, 14, 22, 13, 20)
    later = base + datetime.timedelta(days=1)
    values, _, rows = _read_column(_int96_bytes([base] * 300 + [later] * 200), "ts")

    assert rows == 500
    assert values[0] == base.replace(tzinfo=datetime.timezone.utc)
    assert values[-1] == later.replace(tzinfo=datetime.timezone.utc)


def test_int96_nullable_column():
    """Definition levels are independent of the 12-byte value stride."""
    base = datetime.datetime(2023, 11, 14, 22, 13, 20)
    table = pa.table({"ts": pa.array([base, None, base], type=pa.timestamp("us"))})
    data = _write(table, use_deprecated_int96_timestamps=True)

    values, _, rows = _read_column(data, "ts")
    assert rows == 3
    assert values[1] is None
    assert values[0] == values[2] == base.replace(tzinfo=datetime.timezone.utc)


def test_int96_multiple_row_groups():
    """Every row group contributes; none is quietly skipped."""
    base = datetime.datetime(2023, 11, 14, 22, 13, 20)
    instants = [base + datetime.timedelta(seconds=i) for i in range(5000)]
    data = _int96_bytes(instants, row_group_size=1000)

    _, _, rows = _read_column(data, "ts")
    assert rows == 5000


def test_int96_matches_int64_timestamps():
    """The deprecated and current encodings of the same instants must agree."""
    base = datetime.datetime(2023, 11, 14, 22, 13, 20)
    instants = [base + datetime.timedelta(seconds=i) for i in range(200)]

    as_int96, _, _ = _read_column(_int96_bytes(instants), "ts")
    as_int64, _, _ = _read_column(_write(_int96_table(instants)), "ts")

    assert as_int96 == as_int64


# ---------------------------------------------------------------------------
# BROTLI — unsupported, and loud about it
# ---------------------------------------------------------------------------


def test_brotli_raises_naming_the_codec():
    """Not vendored, so it must refuse — with an actionable reason."""
    data = _write(pa.table({"a": list(range(1000))}), compression="brotli")

    assert parquet.read_metadata(data).num_rows == 1000
    with pytest.raises(RuntimeError, match="BROTLI"):
        with parquet.read_parquet(data) as reader:
            for _morsel in reader:
                pass


def test_brotli_does_not_yield_a_short_answer():
    """The failure must arrive before any morsel — no partial read, no zero rows."""
    table = pa.table({"a": list(range(1000)), "b": [f"v{i}" for i in range(1000)]})
    data = _write(table, compression="brotli")

    yielded = 0
    with pytest.raises(RuntimeError):
        with parquet.read_parquet(data) as reader:
            for morsel in reader:
                yielded += len(morsel)
    assert yielded == 0


# ---------------------------------------------------------------------------
# The shared invariant
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "write_kwargs",
    [
        {"compression": "zstd"},
        {"compression": "gzip"},
        {"compression": "snappy"},
        {"compression": "lz4"},
        {"compression": "brotli"},
        {"compression": "zstd", "use_deprecated_int96_timestamps": True},
    ],
    ids=["zstd", "gzip", "snappy", "lz4", "brotli", "int96"],
)
def test_never_silently_short(write_kwargs):
    """Read every row the footer promises, or raise. Never quietly fewer."""
    base = datetime.datetime(2023, 11, 14, 22, 13, 20)
    table = pa.table(
        {"ts": pa.array([base + datetime.timedelta(seconds=i) for i in range(400)],
                        type=pa.timestamp("us"))}
    )
    data = _write(table, **write_kwargs)
    expected = parquet.read_metadata(data).num_rows
    assert expected == 400

    try:
        _, _, rows = _read_column(data, "ts")
    except (RuntimeError, NotImplementedError):
        return  # an honest refusal is the other acceptable outcome
    assert rows == expected


if __name__ == "__main__":
    test_int96_yields_every_row()
    test_int96_decodes_as_timestamp_with_correct_values()
    test_int96_schema_reports_timestamp_not_int96()
    for _instant in (datetime.datetime(1970, 1, 1),
                     datetime.datetime(1900, 3, 4, 5, 6, 7),
                     datetime.datetime(2262, 4, 11)):
        test_int96_epoch_boundaries(_instant)
    test_int96_dictionary_encoded_column()
    test_int96_nullable_column()
    test_int96_multiple_row_groups()
    test_int96_matches_int64_timestamps()
    test_brotli_raises_naming_the_codec()
    test_brotli_does_not_yield_a_short_answer()
    for _kw in ({"compression": "zstd"}, {"compression": "gzip"},
                {"compression": "snappy"}, {"compression": "lz4"},
                {"compression": "brotli"},
                {"compression": "zstd", "use_deprecated_int96_timestamps": True}):
        test_never_silently_short(_kw)
    print("✅ okay")
