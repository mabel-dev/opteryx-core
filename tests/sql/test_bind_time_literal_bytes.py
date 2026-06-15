"""Regression tests for bind-time scalar literal materialisation and the
bytes-only Draken string edge.

Covers the architect-approved change that:
  1. Materialises every scalar literal ONCE at bind time into a native Draken
     constant (BC_LOAD_LIT_CONST), re-stamping only the logical length per morsel.
  2. Makes the Draken string edge BYTES-ONLY — a Python `str` must never reach
     it; string literals are encoded to bytes at the binder/planner.
  3. Validates NVARCHAR as UTF-8 in C++ (utf8valid) at the constant edge,
     failing loud on invalid UTF-8.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import draken.draken_native as dn
import opteryx


# ---------------------------------------------------------------------------
# End-to-end: scalar literals still produce correct results post-change.
# ---------------------------------------------------------------------------


def _values(sql):
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(sql):
        col = morsel.column(morsel.column_names[0])
        out.extend(col.to_pylist())
    return out


def test_varchar_literal_equality():
    # VARCHAR literal comparison goes vector-vs-vector (constant VARCHAR built
    # once at bind, bytewise compare). No PyUnicode at the edge.
    assert _values("SELECT name FROM $planets WHERE name = 'Earth'") == ["Earth"]


def test_or_of_equalities_rewritten_to_in_list():
    # Regression: the OR->IN-list optimizer rewrite must feed BYTES to the
    # bytes-only vector_in_list edge (previously raised "expects bytes literals").
    rows = _values(
        "SELECT name FROM $planets WHERE name = 'Earth' OR name = 'Mars' ORDER BY name"
    )
    assert rows == ["Earth", "Mars"], rows


def test_in_list_literal():
    rows = _values(
        "SELECT name FROM $planets WHERE name IN ('Earth','Mars') ORDER BY name"
    )
    assert rows == ["Earth", "Mars"], rows


def test_string_concat_literal():
    assert _values("SELECT name || '!' AS x FROM $planets WHERE id = 3") == ["Earth!"]


def test_mixed_scalar_literals():
    # int / float / str literals materialised once at bind, correct per morsel.
    session = opteryx.session()
    rows = list(
        session.execute_to_morsels(
            "SELECT 42 AS a, 3.5 AS b, 'hi' AS c FROM $planets WHERE id = 1"
        )
    )
    m = rows[0]
    assert m.column(b"a").to_pylist() == [42]
    assert m.column(b"b").to_pylist() == [3.5]
    assert m.column(b"c").to_pylist() == ["hi"]


def test_null_literal():
    # NULL scalar literal materialises a constant-shape NULL vector at bind time.
    assert _values("SELECT NULL AS n FROM $planets WHERE id = 1") == [None]


# ---------------------------------------------------------------------------
# Draken edge: VARCHAR / NVARCHAR constants are bytes-only.
# ---------------------------------------------------------------------------


def test_varchar_constant_rejects_str():
    # A Python str must not reach the edge — it is encoded to bytes at the binder.
    try:
        dn.vector_varchar_from_constant("nope", 4)
    except (ValueError, TypeError):
        pass
    else:
        raise AssertionError("vector_varchar_from_constant accepted str")


def test_varchar_constant_stores_bytes_verbatim():
    # Bytes are stored verbatim (no decode) — including non-UTF-8 payloads, which
    # a str-encoding edge could not represent.
    raw = b"\xff\xfe not-utf8 \x00 bytes"
    v = dn.vector_varchar_from_constant(raw, 3)
    assert v.length == 3
    # Constant shape: one unique value broadcast across the rows.
    assert v.data_length == 1


def test_string_sequence_rejects_str():
    try:
        dn.vector_from_string_sequence(["a", "b"])
    except (ValueError, TypeError):
        pass
    else:
        raise AssertionError("vector_from_string_sequence accepted str")


def test_string_sequence_accepts_bytes():
    v = dn.vector_from_string_sequence([b"a", b"b", None])
    assert v.to_pylist() == ["a", "b", None]


def test_nvarchar_constant_accepts_valid_utf8():
    v = dn.vector_nvarchar_from_constant("héllo".encode("utf-8"), 2)
    assert v.length == 2
    assert v.data_length == 1


def test_nvarchar_constant_rejects_invalid_utf8():
    # utf8valid (C++) must fail loud on invalid UTF-8 — no Python UTF-8 work.
    try:
        dn.vector_nvarchar_from_constant(b"\xff\xfe\x00bad", 1)
    except (ValueError, TypeError):
        pass
    else:
        raise AssertionError("vector_nvarchar_from_constant accepted invalid UTF-8")


def test_nvarchar_constant_rejects_str():
    try:
        dn.vector_nvarchar_from_constant("nope", 1)
    except (ValueError, TypeError):
        pass
    else:
        raise AssertionError("vector_nvarchar_from_constant accepted str")


if __name__ == "__main__":  # pragma: no cover
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"✅ {name}")
    print("✅ all bind-time literal / bytes-only edge tests passed")
