"""UUID type (docs/UUID_TYPE_DESIGN.md, rulings U1-U15), phase 1: the type itself.

A UUID is 16 bytes in RFC 9562 network order — the order it is written in text — so
byte order, canonical-text order and Postgres's order are one order. Text in is
strict (36-char 8-4-4-4-12, either case), text out is lowercase. VARBINARY <-> UUID
is the raw 16 bytes. A string LITERAL compared with a UUID column is parsed at bind;
a string COLUMN is not.
"""

import os
import sys
import uuid

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from draken.draken_native import DrakenType
from draken.draken_native import vector_uuid_from_constant, vector_uuid_from_sequence
from opteryx.types.logical_type import UUID, LogicalCategory, parse_column_type, serialize_column_type

A = "550e8400-e29b-41d4-a716-446655440000"
B = "00000000-0000-0000-0000-000000000001"
C = "ffffffff-0000-0000-0000-000000000000"
# n=4 repeats A so grouping and joins have a duplicate to find.
SOURCE = (
    "SELECT CAST(u AS UUID) AS id, n FROM ("
    f"SELECT '{A}' AS u, 1 AS n UNION ALL SELECT '{B}', 2 "
    f"UNION ALL SELECT '{C.upper()}', 3 UNION ALL SELECT '{A}', 4) AS s"
)


def rows(sql):
    out = []
    for morsel in opteryx.session().execute_to_morsels(sql):
        columns = [morsel.column(name).to_pylist() for name in morsel.column_names]
        out.extend(zip(*columns))
    return out


def one(sql):
    return [r[0] for r in rows(sql)]


# ---------------------------------------------------------------------------
# The type
# ---------------------------------------------------------------------------


def test_type_vocabulary():
    assert UUID.physical == DrakenType.UUID
    assert UUID.category == LogicalCategory.UUID
    assert str(UUID) == "UUID"
    assert parse_column_type("UUID") == UUID
    assert parse_column_type(serialize_column_type(UUID)) == UUID


def test_vector_round_trip_and_python_edge():
    vec = vector_uuid_from_sequence([uuid.UUID(A), B, None, uuid.UUID(C).bytes])
    assert vec.to_pylist() == [uuid.UUID(A), uuid.UUID(B), None, uuid.UUID(C)]
    assert vector_uuid_from_constant(None, 3).to_pylist() == [None, None, None]


@pytest.mark.parametrize("bad", ["", "nope", A[:-1], "{" + A + "}", A.replace("-", ""), "urn:uuid:" + A, A.replace("5", "g", 1)])
def test_vector_construction_is_strict(bad):
    with pytest.raises(ValueError):
        vector_uuid_from_sequence([bad])


def test_vector_bytes_must_be_sixteen():
    with pytest.raises(ValueError):
        vector_uuid_from_sequence([b"\x00" * 15])


# ---------------------------------------------------------------------------
# Casts
# ---------------------------------------------------------------------------


def test_cast_literal_and_column():
    assert one(f"SELECT CAST('{A}' AS UUID) AS u") == [uuid.UUID(A)]
    assert one(f"SELECT CAST('{A.upper()}' AS UUID) AS u") == [uuid.UUID(A)]
    assert sorted(one(f"SELECT id FROM ({SOURCE}) AS t"), key=str) == sorted(
        [uuid.UUID(A), uuid.UUID(B), uuid.UUID(C), uuid.UUID(A)], key=str
    )


def test_text_out_is_lowercase_canonical():
    assert one(f"SELECT CAST(CAST('{C.upper()}' AS UUID) AS VARCHAR) AS u") == [C]


def test_varbinary_is_the_raw_sixteen_bytes():
    assert one(f"SELECT CAST(CAST('{A}' AS UUID) AS VARBINARY) AS b") == [uuid.UUID(A).bytes]
    assert one(f"SELECT CAST(CAST(CAST('{A}' AS UUID) AS VARBINARY) AS UUID) AS u") == [uuid.UUID(A)]


@pytest.mark.parametrize("bad", ["nope", "{" + A + "}", A.replace("-", "")])
def test_cast_is_strict_and_try_cast_nulls(bad):
    with pytest.raises(Exception, match="UUID"):
        rows(f"SELECT CAST('{bad}' AS UUID) AS u")
    assert one(f"SELECT TRY_CAST('{bad}' AS UUID) AS u") == [None]


def test_cast_from_a_string_column_is_strict():
    with pytest.raises(Exception, match="UUID"):
        rows("SELECT CAST(name AS UUID) AS u FROM $planets")


def test_wrong_length_bytes_refuse():
    with pytest.raises(Exception, match="16"):
        rows("SELECT CAST(CAST('abc' AS VARBINARY) AS UUID) AS u")


# ---------------------------------------------------------------------------
# Comparison, ordering, grouping, joins, membership
# ---------------------------------------------------------------------------


def test_equality_against_a_string_literal():
    assert sorted(one(f"SELECT n FROM ({SOURCE}) AS t WHERE id = '{A}'")) == [1, 4]
    assert sorted(one(f"SELECT n FROM ({SOURCE}) AS t WHERE '{A.upper()}' = id")) == [1, 4]
    assert sorted(one(f"SELECT n FROM ({SOURCE}) AS t WHERE id <> '{A}'")) == [2, 3]


def test_ordering_is_byte_order_not_signed():
    # ffffffff-... is the LARGEST value: a signed 128-bit compare would put it first.
    assert sorted(one(f"SELECT n FROM ({SOURCE}) AS t WHERE id > '{A}'")) == [3]
    assert one(f"SELECT n FROM ({SOURCE}) AS t ORDER BY id, n") == [2, 1, 4, 3]
    assert one(f"SELECT n FROM ({SOURCE}) AS t ORDER BY id DESC, n") == [3, 1, 4, 2]


def test_non_uuid_literal_fails_at_bind():
    with pytest.raises(Exception, match="not a UUID"):
        rows(f"SELECT n FROM ({SOURCE}) AS t WHERE id = 'nope'")


def test_string_column_against_uuid_is_refused():
    with pytest.raises(Exception):
        rows(f"SELECT n FROM ({SOURCE}) AS t WHERE id = CAST(n AS VARCHAR)")


def test_group_by_and_count_distinct():
    assert sorted(rows(f"SELECT id, COUNT(*) AS c FROM ({SOURCE}) AS t GROUP BY id"), key=str) == sorted(
        [(uuid.UUID(A), 2), (uuid.UUID(B), 1), (uuid.UUID(C), 1)], key=str
    )
    assert one(f"SELECT COUNT(DISTINCT id) AS c FROM ({SOURCE}) AS t") == [3]


def test_join_on_uuid():
    pairs = rows(f"SELECT a.n, b.n FROM ({SOURCE}) AS a INNER JOIN ({SOURCE}) AS b ON a.id = b.id")
    assert sorted(pairs) == [(1, 1), (1, 4), (2, 2), (3, 3), (4, 1), (4, 4)]


def test_in_list():
    assert sorted(one(f"SELECT n FROM ({SOURCE}) AS t WHERE id IN ('{B}', '{C}')")) == [2, 3]
    assert sorted(one(f"SELECT n FROM ({SOURCE}) AS t WHERE id NOT IN ('{B}', '{C}')")) == [1, 4]


def test_coalesce_and_case_blend_uuid():
    assert sorted(one(f"SELECT COALESCE(id, CAST('{B}' AS UUID)) AS c FROM ({SOURCE}) AS t"), key=str) == sorted(
        [uuid.UUID(A), uuid.UUID(B), uuid.UUID(C), uuid.UUID(A)], key=str
    )
    assert sorted(one(f"SELECT CASE WHEN n = 2 THEN NULL ELSE id END AS c FROM ({SOURCE}) AS t"), key=str) == sorted(
        [uuid.UUID(A), None, uuid.UUID(C), uuid.UUID(A)], key=str
    )


def test_no_arithmetic():
    with pytest.raises(Exception):
        rows(f"SELECT id + 1 FROM ({SOURCE}) AS t")


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


def test_csv_and_jsonl_render_canonical_text():
    from rugo.csv import write_csv
    from rugo.jsonl import write_jsonl

    morsel = next(iter(opteryx.session().execute_to_morsels(f"SELECT CAST('{C.upper()}' AS UUID) AS u")))
    assert C.encode() in write_csv(morsel)
    assert f'"u":"{C}"'.encode() in write_jsonl(morsel)


# ---------------------------------------------------------------------------
# Aggregates (MIN/MAX/ANY_VALUE ride the 16-byte extreme lanes as an order key)
# ---------------------------------------------------------------------------


def test_min_max_use_byte_order():
    # ffffffff-... is the MAX: a signed compare of the raw bytes would call it the MIN.
    assert rows(f"SELECT MIN(id), MAX(id) FROM ({SOURCE}) AS t") == [(uuid.UUID(B), uuid.UUID(C))]


def test_grouped_min_max_and_any_value():
    got = sorted(rows(f"SELECT n % 2 AS k, MIN(id), MAX(id) FROM ({SOURCE}) AS t GROUP BY k"))
    assert got == [(0, uuid.UUID(B), uuid.UUID(A)), (1, uuid.UUID(A), uuid.UUID(C))]
    picked = rows(f"SELECT n % 2 AS k, ANY_VALUE(id) FROM ({SOURCE}) AS t GROUP BY k")
    assert all(type(v) is uuid.UUID for _, v in picked)


def test_min_over_no_rows_is_null():
    assert one(f"SELECT MIN(id) FROM ({SOURCE}) AS t WHERE n > 10") == [None]


@pytest.mark.parametrize("agg", ["SUM", "AVG"])
def test_sum_and_avg_are_refused(agg):
    with pytest.raises(Exception, match="UUID"):
        rows(f"SELECT {agg}(id) FROM ({SOURCE}) AS t")
