"""
SQL:2016 `<expr> IS [NOT] JSON [VALUE|SCALAR|ARRAY|OBJECT]`.

The predicate answers "is this JSON TEXT well-formed, and is its root the shape
asked for". Values matter here, not shapes, so this asserts the booleans.

NULLS: `IS [NOT] JSON` is TOTAL — it carries no validity and every row gets a
definite answer. A NULL document is not well-formed, so `NULL IS JSON` is FALSE
and `NULL IS NOT JSON` is TRUE. This follows the `IS TRUE`/`IS FALSE` family,
which is likewise never-null, and (also following that family) the BARE `NULL`
literal is a type error rather than a row of answers — it has no operand type.
Ruled by the architect 2026-09-22.

Well-formedness is yyjson's answer, so a row that passes IS JSON is a row the
engine's JSON functions can parse: invalid UTF-8 and unpaired `\\u` surrogates are
NOT JSON, there is no nesting limit, and numbers are syntax-checked but never
range-checked (`1e999` is JSON).

`WITH`/`WITHOUT UNIQUE KEYS` parses but is refused: duplicate-key detection is
not implemented, and answering the well-formedness question when the unique-key
question was asked would be a wrong answer rather than a slow one.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.exceptions import IncorrectTypeError, UnsupportedSyntaxError


def _scalar(sql):
    """Run `sql` and return the single value of its single column."""
    session = opteryx.session()
    values = []
    for morsel in session.execute_to_morsels(sql):
        name = morsel.column_names[0]
        values.extend(morsel.column(name).to_pylist())
    assert len(values) == 1, f"expected one row from {sql!r}, got {len(values)}"
    return values[0]


def _is_json(document: str, kind: str = "", negated: bool = False):
    literal = "NULL" if document is None else "'" + document.replace("'", "''") + "'"
    predicate = "IS NOT JSON" if negated else "IS JSON"
    return _scalar(f"SELECT CAST({literal} AS VARCHAR) {predicate} {kind}")


# (document, VALUE, SCALAR, ARRAY, OBJECT)
# fmt:off
DOCUMENTS = [
    # --- well formed -------------------------------------------------------
    ('{"a":1}',                    True,  False, False, True),
    ('{}',                         True,  False, False, True),
    ('{"a":{"b":[1,{"c":null}]}}', True,  False, False, True),
    ('[1,2,3]',                    True,  False, True,  False),
    ('[]',                         True,  False, True,  False),
    ('[[],{},null]',               True,  False, True,  False),
    ('"hello"',                    True,  True,  False, False),
    ('""',                         True,  True,  False, False),
    ('"\\u00e9\\n\\\\"',           True,  True,  False, False),
    ('42',                         True,  True,  False, False),
    ('-0.5e+10',                   True,  True,  False, False),
    ('1e999',                      True,  True,  False, False),  # syntax only: never range-checked
    ('"\\ud83d\\ude00"',             True,  True,  False, False),  # a surrogate PAIR is fine
    ('0',                          True,  True,  False, False),
    ('true',                       True,  True,  False, False),
    ('false',                      True,  True,  False, False),
    ('null',                       True,  True,  False, False),
    ('  \t\n {"a":1} \r\n ',       True,  False, False, True),   # surrounding whitespace is fine
    # --- malformed ---------------------------------------------------------
    ('',                           False, False, False, False),  # empty string: no root value
    ('   ',                        False, False, False, False),  # whitespace only
    ('{oops',                      False, False, False, False),
    ('{"a":1,}',                   False, False, False, False),  # trailing comma, object
    ('[1,2,]',                     False, False, False, False),  # trailing comma, array
    ('[1,2',                       False, False, False, False),  # unclosed array
    ('{"a":1',                     False, False, False, False),  # unclosed object
    ('{"a" 1}',                    False, False, False, False),  # missing colon
    ('{a:1}',                      False, False, False, False),  # unquoted key
    ("{'a':1}",                    False, False, False, False),  # single-quoted string
    ('01',                         False, False, False, False),  # leading zero
    ('-',                          False, False, False, False),  # bare sign
    ('.5',                         False, False, False, False),  # no integer part
    ('1.',                         False, False, False, False),  # no fraction digits
    ('1e',                         False, False, False, False),  # no exponent digits
    ('{"a":1} trailing',           False, False, False, False),  # trailing content
    ('1 2',                        False, False, False, False),  # two root values
    ('"unterminated',              False, False, False, False),
    ('"bad \\x escape"',           False, False, False, False),
    ('"\\u00zz"',                  False, False, False, False),  # \u without four hex digits
    ('"\\ud800"',                  False, False, False, False),  # unpaired high surrogate
    ('"\\udc00"',                  False, False, False, False),  # unpaired low surrogate
    ('"\\ud800\\u0041"',            False, False, False, False),  # high surrogate, wrong partner
    ('TRUE',                       False, False, False, False),  # JSON keywords are lower case
    ('NaN',                        False, False, False, False),
    ('[1,2]]',                     False, False, False, False),  # unbalanced close
    # --- last-byte gate edges: the root's last significant byte vs its opener --
    ('{',                          False, False, False, False),  # opener is also the last byte
    ('[ \n',                       False, False, False, False),  # trailing-ws skip stops at the opener
    ('[{"a":1}',                   False, False, False, False),  # ends in the WRONG close
    ('{"a":{"b":1}',               False, False, False, False),  # truncated after an inner close: passes the gate, parse rejects
    ('{"a":[1}',                   False, False, False, False),  # right last byte, mismatched inside
]
# fmt:on

KINDS = ["", "VALUE", "SCALAR", "ARRAY", "OBJECT"]


@pytest.mark.parametrize("document,value,scalar,array,object_", DOCUMENTS)
def test_is_json_shapes(document, value, scalar, array, object_):
    """Each shape answers for each document, and the bare form means VALUE."""
    expected = {
        "": value,  # bare IS JSON == IS JSON VALUE
        "VALUE": value,
        "SCALAR": scalar,
        "ARRAY": array,
        "OBJECT": object_,
    }
    for kind in KINDS:
        assert _is_json(document, kind) is expected[kind], (
            f"{document!r} IS JSON {kind}".strip()
        )


@pytest.mark.parametrize("document,value,scalar,array,object_", DOCUMENTS)
def test_is_not_json_is_the_exact_negation(document, value, scalar, array, object_):
    """IS NOT JSON is the complement of IS JSON — never null, never a third answer."""
    expected = {"": value, "VALUE": value, "SCALAR": scalar, "ARRAY": array, "OBJECT": object_}
    for kind in KINDS:
        assert _is_json(document, kind, negated=True) is (not expected[kind]), (
            f"{document!r} IS NOT JSON {kind}".strip()
        )


def test_null_document_is_a_definite_false():
    """A NULL document is not well-formed. The predicate is total: no NULL result."""
    for kind in KINDS:
        assert _is_json(None, kind) is False
        assert _is_json(None, kind, negated=True) is True


def test_null_rows_in_a_column_answer_definitely():
    """The same, reached through a column rather than a CAST literal."""
    session = opteryx.session()
    rows = []
    sql = """
        SELECT doc IS JSON AS ok, doc IS NOT JSON AS bad
        FROM (VALUES ('{"a":1}'), ('nope'), (NULL), ('')) AS t(doc)
    """
    for morsel in session.execute_to_morsels(sql):
        ok = morsel.column(b"ok").to_pylist()
        bad = morsel.column(b"bad").to_pylist()
        rows.extend(zip(ok, bad))

    assert rows == [(True, False), (False, True), (False, True), (False, True)]


def test_null_predicate_over_is_json_is_never_true():
    """`(x IS JSON) IS NULL` is FALSE for every row — the result carries no validity."""
    session = opteryx.session()
    values = []
    sql = """
        SELECT (doc IS JSON) IS NULL AS unknown
        FROM (VALUES ('{"a":1}'), ('nope'), (NULL)) AS t(doc)
    """
    for morsel in session.execute_to_morsels(sql):
        values.extend(morsel.column(b"unknown").to_pylist())
    assert values == [False, False, False]


def test_filters_select_the_expected_rows():
    """The ingest-quality use case: find the bad rows, keep the good ones."""
    source = """(VALUES ('{"a":1}'), ('[1]'), ('nope'), (NULL), ('')) AS t(doc)"""

    def docs(where):
        session = opteryx.session()
        out = []
        for morsel in session.execute_to_morsels(f"SELECT doc FROM {source} WHERE {where}"):
            out.extend(morsel.column(b"doc").to_pylist())
        return out

    assert docs("doc IS JSON") == ['{"a":1}', "[1]"]
    assert docs("doc IS NOT JSON") == ["nope", None, ""]
    # `NOT (x IS JSON)` must agree with `x IS NOT JSON` — the optimizer inverts
    # the operator rather than keeping a NOT root, and a total predicate makes
    # the two spellings identical for NULL rows too.
    assert docs("NOT (doc IS JSON)") == ["nope", None, ""]
    assert docs("doc IS JSON OBJECT") == ['{"a":1}']
    assert docs("doc IS JSON ARRAY") == ["[1]"]


def test_variant_operand_composes_with_arrow_extraction():
    """`->` yields JSON text, so IS JSON composes with it."""
    assert _scalar("""SELECT ('{"a":[1,2]}' -> 'a') IS JSON ARRAY""") is True
    assert _scalar("""SELECT ('{"a":[1,2]}' -> 'a') IS JSON OBJECT""") is False
    # `-> 'a'` on this document yields the JSON STRING "[1,2]", not an array.
    assert _scalar("""SELECT ('{"a":"[1,2]"}' -> 'a') IS JSON ARRAY""") is False
    assert _scalar("""SELECT ('{"a":"[1,2]"}' -> 'a') IS JSON SCALAR""") is True


@pytest.mark.parametrize(
    "hex_bytes,expected",
    [
        ("227822", True),        # "x" — control: same type, same path, valid bytes
        ("22c3a922", True),      # "é" as valid two-byte UTF-8
        ("22ff22", False),       # 0xFF is never UTF-8
        ("22c022", False),       # truncated two-byte sequence
        ("22c0af22", False),     # overlong encoding of '/'
        ("22eda08022", False),   # UTF-8-encoded surrogate U+D800
        ("22f490808022", False), # past U+10FFFF
    ],
)
def test_invalid_utf8_is_not_json(hex_bytes, expected):
    """Bytes that are not UTF-8 are not JSON — VARBINARY is held to the same rule."""
    assert _scalar(f"SELECT HEX_DECODE('{hex_bytes}') IS JSON") is expected


@pytest.mark.parametrize("depth", [200, 2000])
def test_deeply_nested_document_is_well_formed(depth):
    """There is no nesting limit: depth alone never makes a document malformed."""
    document = "[" * depth + "1" + "]" * depth
    assert _is_json(document, "ARRAY") is True


@pytest.mark.parametrize("clause", ["WITH UNIQUE KEYS", "WITHOUT UNIQUE KEYS"])
@pytest.mark.parametrize("kind", ["", "VALUE", "ARRAY", "OBJECT"])
def test_unique_keys_is_refused_not_ignored(kind, clause):
    """Parsed, then refused by name. Never silently answered as well-formedness."""
    with pytest.raises(UnsupportedSyntaxError, match="UNIQUE KEYS"):
        _scalar(f"SELECT '{{}}' IS JSON {kind} {clause}")


@pytest.mark.parametrize(
    "operand",
    [
        "1",
        "1.5",
        "TRUE",
        "NULL",  # the bare literal has no operand type — as with `NULL IS TRUE`
        "CAST('2020-01-01' AS DATE)",
        "['a', 'b']",
    ],
)
def test_non_json_text_operand_is_a_type_error(operand):
    with pytest.raises(IncorrectTypeError, match="JSON text"):
        _scalar(f"SELECT {operand} IS JSON")


@pytest.mark.parametrize(
    "cast_to", ["VARCHAR", "NVARCHAR", "VARBINARY"]
)
def test_every_accepted_operand_type_works(cast_to):
    assert _scalar(f"""SELECT CAST('{{"a":1}}' AS {cast_to}) IS JSON OBJECT""") is True
    assert _scalar(f"""SELECT CAST('nope' AS {cast_to}) IS JSON""") is False


def test_predicate_lowers_fully_c_native():
    """A pushed `IS JSON` must not drop the scan off the native path.

    The kernel is mandatory and lowers to BC_FUNCTION with BC_INSTR_C_NATIVE, so
    `bytecode_is_all_c_native` should still hold and the native Source should be
    selected with no residual reason. If this starts reporting
    `unlowerable_predicate`, the lowering stopped resolving the kernel.
    """
    sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../..", "dev"))
    import native_residual_census as census

    source = "testdata/flat/formats/parquet"
    for predicate in ("text IS JSON", "text IS NOT JSON OBJECT", "text IS JSON ARRAY"):
        sources, reasons, err = census.scan_residuals(
            f"SELECT followers FROM '{source}' WHERE {predicate}"
        )
        assert err is None, f"{predicate}: {err}"
        assert set(sources.values()) == {"NativeParquetScanSource"}, (predicate, sources)
        assert reasons == {}, (predicate, reasons)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-q"]))
