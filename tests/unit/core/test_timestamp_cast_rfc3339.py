"""CAST(<string> AS TIMESTAMP) accepts the full RFC 3339 range, identically on
the constant-folded path and the native vector path.

Cloud Logging emits RFC 3339 with a variable number of fractional digits (it
trims trailing zeros, and goes to nanoseconds) and a 'Z' suffix. Before this,
the literal path (value_parsing._parse_timestamp, via strptime '%f') rejected
more than 6 fractional digits with "unconverted data", and the column path
(draken/core/iso_datetime.h parse_iso_timestamp) additionally rejected any
'Z' or offset — so `CAST('...03Z' AS TIMESTAMP)` worked while the same text in
a column failed with "Cannot cast string to TIMESTAMP".

The rules both paths now share:
- 0..N fractional digits; past microseconds they are TRUNCATED (as
  datetime.fromisoformat does), never rounded or rejected;
- an optional 'Z' / '+HH:MM' / '-HH:MM' / '+HHMM' suffix after a time, which
  is honoured by normalising to UTC (timestamps are stored naive UTC), never
  silently discarded.
"""

import datetime
import json
import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx

UTC = datetime.timezone.utc


def _ts(*args):
    return datetime.datetime(*args, tzinfo=UTC)


ACCEPTED = [
    # Cloud Logging shapes — the reason for the change.
    ("2026-10-01T19:00:03.088320942Z", _ts(2026, 10, 1, 19, 0, 3, 88320)),
    ("2026-10-01T19:00:03.088320942", _ts(2026, 10, 1, 19, 0, 3, 88320)),
    ("2026-10-01T19:00:03.0812Z", _ts(2026, 10, 1, 19, 0, 3, 81200)),
    ("2026-10-01T19:00:03.081226Z", _ts(2026, 10, 1, 19, 0, 3, 81226)),
    ("2026-10-01T19:00:03.081226", _ts(2026, 10, 1, 19, 0, 3, 81226)),
    ("2026-10-01T19:00:03Z", _ts(2026, 10, 1, 19, 0, 3)),
    ("2026-10-01T19:00:03", _ts(2026, 10, 1, 19, 0, 3)),
    # Every fractional width 1..9, plus a long tail, truncated not rounded.
    ("2026-10-01T19:00:03.9Z", _ts(2026, 10, 1, 19, 0, 3, 900000)),
    ("2026-10-01T19:00:03.99Z", _ts(2026, 10, 1, 19, 0, 3, 990000)),
    ("2026-10-01T19:00:03.999Z", _ts(2026, 10, 1, 19, 0, 3, 999000)),
    ("2026-10-01T19:00:03.99999Z", _ts(2026, 10, 1, 19, 0, 3, 999990)),
    ("2026-10-01T19:00:03.9999999Z", _ts(2026, 10, 1, 19, 0, 3, 999999)),
    ("2026-10-01T19:00:03.99999999Z", _ts(2026, 10, 1, 19, 0, 3, 999999)),
    ("2026-10-01T19:00:03.999999999Z", _ts(2026, 10, 1, 19, 0, 3, 999999)),
    ("2026-10-01T19:00:03.123456789012Z", _ts(2026, 10, 1, 19, 0, 3, 123456)),
    # Space separator and minute precision take a suffix too.
    ("2026-10-01 19:00:03.0812Z", _ts(2026, 10, 1, 19, 0, 3, 81200)),
    ("2026-10-01T19:00Z", _ts(2026, 10, 1, 19, 0)),
    ("2026-10-01", _ts(2026, 10, 1)),
    # Offsets: zero is a no-op, non-zero normalises to UTC (incl. day rollover).
    ("2026-10-01T19:00:03+00:00", _ts(2026, 10, 1, 19, 0, 3)),
    ("2026-10-01T19:00:03-00:00", _ts(2026, 10, 1, 19, 0, 3)),
    ("2026-10-01T19:00:03.088320942+00:00", _ts(2026, 10, 1, 19, 0, 3, 88320)),
    ("2026-10-01T19:00:03+0000", _ts(2026, 10, 1, 19, 0, 3)),
    ("2026-10-01T19:00:03+05:30", _ts(2026, 10, 1, 13, 30, 3)),
    ("2026-10-01T19:00:03.5-0700", _ts(2026, 10, 2, 2, 0, 3, 500000)),
    ("2026-10-01T01:00:00+02:00", _ts(2026, 9, 30, 23, 0, 0)),
]

REJECTED = [
    "2026-10-01T19:00:03.Z",  # '.' with no digits
    "2026-10-01T19:00:03.",
    "2026-10-01T19:00:03 Z",  # whitespace before the zone
    "2026-10-01T19:00:03z",  # RFC 3339 lowercase z — not accepted on either path
    "2026-10-01T19:00:03+5:30",  # offset hour must be two digits
    "2026-10-01T19:00:03+05",  # minutes required
    "2026-10-01T19:00:03+24:00",  # offset hour out of range
    "2026-10-01T19:00:03+05:60",  # offset minute out of range
    "2026-10-01T19:00:03Zjunk",
    "2026-10-01T19:00:03.0812x",
    "2026-10-01Z",  # zone only valid after a time
    "2026-10-01T19:00.5Z",  # fraction without seconds
]


def _rows(sql):
    out = []
    for morsel in opteryx.session().execute_to_morsels(sql):
        out.extend(tuple(row) for row in morsel)
    return out


def _literal(text, func="CAST"):
    return _rows(f"SELECT {func}('{text}' AS TIMESTAMP)")[0][0]


def _column(tmp_path, texts, func="CAST"):
    path = tmp_path / "t.jsonl"
    path.write_text("".join(json.dumps({"t": t}) + "\n" for t in texts))
    return [row[0] for row in _rows(f"SELECT {func}(t AS TIMESTAMP) FROM READ_JSONL('{path}')")]


@pytest.mark.parametrize("text,expected", ACCEPTED, ids=[t for t, _ in ACCEPTED])
def test_literal_and_column_agree(tmp_path, text, expected):
    literal = _literal(text)
    column = _column(tmp_path, [text])
    assert literal == expected, f"literal {text!r} -> {literal!r}"
    assert column == [expected], f"column {text!r} -> {column!r}"


def test_mixed_forms_in_one_vector(tmp_path):
    # One vector holding every shape at once — the per-row parse must not carry
    # state (offset, digit count) from one row into the next.
    texts = [t for t, _ in ACCEPTED]
    assert _column(tmp_path, texts) == [e for _, e in ACCEPTED]


@pytest.mark.parametrize("text", REJECTED)
def test_rejected_on_both_paths(tmp_path, text):
    with pytest.raises(Exception):
        _literal(text)
    with pytest.raises(Exception, match="Cannot cast string to TIMESTAMP"):
        _column(tmp_path, [text])


@pytest.mark.parametrize("text", REJECTED)
def test_try_cast_nulls_on_both_paths(tmp_path, text):
    assert _literal(text, "TRY_CAST") is None
    assert _column(tmp_path, [text], "TRY_CAST") == [None]


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
