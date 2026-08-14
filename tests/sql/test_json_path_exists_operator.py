"""`@?` — JSON path existence — must EXECUTE, in a filter and in a projection.

The operator parsed, and the binder typed it (operator_map carried the
VARCHAR/NVARCHAR/VARBINARY/VARIANT pairs), but nothing lowered it: there was no
kernel behind it. So it died at the very last step, with a message about the
c-native kernel set that named neither the operator nor anything the caller could
change:

    SELECT doc FROM t WHERE doc @? 'city'
      -> NotSupportedError: a filter predicate outside the c-native kernel set
    SELECT doc @? 'city' AS x FROM t
      -> NotSupportedError: a comparison in `... @? 'city'`, outside the
         c-native kernel set

It appeared to work only against a fully constant expression, where constant
folding answered it before the engine ever saw it — which is the worst possible
signal, because the smallest test of the feature passed.

It has a kernel now (draken_json_path_exists, function_array_json.cpp), bound as
an arity-1 function whose PATH rides the same bind-time extraction_ctx `->` and
`->>` use. That shared ctx is why the path spellings below are guaranteed to agree
with `->`'s: there is one path resolver, not two.

EXISTENCE IS NOT EXTRACTION, and the difference is the point of the operator. The
workaround the docs used to recommend, `doc -> 'key' IS NOT NULL`, is not a
synonym: extraction maps a JSON `null` onto SQL NULL, so it answers FALSE for a
key that is present and explicitly null. `@?` answers TRUE — the node is there.
test_json_null_value_exists_and_differs_from_the_arrow_rewrite pins that.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.exceptions import IncorrectTypeError
from opteryx.exceptions import UnsupportedSyntaxError

# Four rows: a full document, a partial one, one sharing no keys, and a SQL NULL.
# `n` is present and JSON-null in row 1 — the case `->` cannot express.
DOCS = """(SELECT * FROM (VALUES
    ('{"city":"x","contact":{"email":"a@b"},"tags":["p","q"],"n":null}'),
    ('{"city":"y"}'),
    ('{"other":1}'),
    (NULL)
) AS v(doc))"""


def results(sql):
    session = opteryx.session()
    out: dict = {}
    for morsel in session.execute_to_morsels(sql):
        if morsel is None:
            continue
        for key, values in morsel.to_arrow().to_pydict().items():
            out.setdefault(key, []).extend(values)
    return out


def exists(path):
    """The `@?` answer for `path`, one entry per row of DOCS."""
    return results(f"SELECT doc @? '{path}' AS x FROM {DOCS}")["x"]


def test_projection_position_runs():
    # Was: NotSupportedError, "a comparison in `...`, outside the c-native kernel set".
    assert exists("city") == [True, True, False, None]


def test_filter_position_runs():
    # Was: NotSupportedError, "a filter predicate outside the c-native kernel set".
    assert results(f"SELECT COUNT(*) AS n FROM {DOCS} WHERE doc @? 'city'") == {"n": [2]}
    # NULL rows are neither kept by the predicate nor by its negation — the
    # three-valued behaviour every other predicate has.
    assert results(f"SELECT COUNT(*) AS n FROM {DOCS} WHERE NOT (doc @? 'city')") == {"n": [1]}


@pytest.mark.parametrize(
    "path,expected",
    [
        # Bare key — a top-level object key.
        ("city", [True, True, False, None]),
        ("other", [False, False, True, None]),
        ("nope", [False, False, False, None]),
        # The `$.`-prefixed JSON Path form the docs advertised.
        ("$.city", [True, True, False, None]),
        ("$.contact.email", [True, False, False, None]),
        # A raw RFC 6901 pointer — `->` accepts one, so this does too.
        ("/contact/email", [True, False, False, None]),
        # Array subscripts, in range and out.
        ("$.tags[0]", [True, False, False, None]),
        ("$.tags[1]", [True, False, False, None]),
        ("$.tags[9]", [False, False, False, None]),
        # A token applied to a scalar is a MISS, not an error: `city` is a string,
        # so it has no member `deeper`.
        ("$.city.deeper", [False, False, False, None]),
        # The empty path is the document root, which exists in every parseable row.
        ("$", [True, True, True, None]),
    ],
)
def test_path_spellings(path, expected):
    assert exists(path) == expected


def test_the_three_spellings_of_one_path_agree():
    """Bare key, `$.`-path and pointer are one path, resolved by one resolver.

    They share `dotpath_to_jsonptr` with `->`, so this asserts the operators cannot
    drift apart on what a path means — not merely that each spelling parses.
    """
    assert exists("contact") == exists("$.contact") == exists("/contact")


def test_json_null_value_exists_and_differs_from_the_arrow_rewrite():
    """The one case `doc -> 'key' IS NOT NULL` gets wrong, and why `@?` is not it.

    Row 1 has `"n":null` — the key is PRESENT and its value is JSON null. `@?` asks
    whether the node exists, so it is TRUE. `->` extracts, and extraction maps JSON
    null onto SQL NULL, so `IS NOT NULL` is FALSE. Both are right about their own
    question; they are not synonyms, and documentation that offers one as a
    substitute for the other is offering a different answer.
    """
    assert exists("n") == [True, False, False, None]
    arrow = results(f"SELECT doc -> 'n' IS NOT NULL AS x FROM {DOCS}")["x"]
    assert arrow == [False, False, False, False]


def test_null_document_row_is_null_not_false():
    # A row with no document has no answer about its keys — three-valued, exactly
    # as `->` is. A FALSE here would silently fold NULL into "no such key".
    assert exists("city")[3] is None
    assert exists("nope")[3] is None


def test_invalid_json_is_an_error_not_a_silent_false():
    """A document the engine cannot parse must fail loudly.

    Answering FALSE would be indistinguishable from "the key is absent", which is
    the wrong answer dressed as a real one. `->` fails on the same row, and `@?`
    must not be the softer of the two.
    """
    bad = "(SELECT * FROM (VALUES ('not json')) AS v(doc))"
    with pytest.raises(Exception) as exc:
        results(f"SELECT doc @? 'a' AS x FROM {bad}")
    assert "invalid JSON" in str(exc.value), str(exc.value)


def test_composes_over_an_extracted_variant():
    # `->` yields VARIANT, and the kernel accepts VARIANT, so extraction chains
    # into existence. Row 1's `contact` object has `email`; rows 2-3 have no
    # `contact` at all, so the extraction is NULL and the existence test is NULL.
    rows = results(f"SELECT (doc -> 'contact') @? 'email' AS x FROM {DOCS}")["x"]
    assert rows == [True, None, None, None]


def test_a_non_literal_path_is_refused_at_bind_time():
    """A per-row path has no kernel — and must say so, not fail late and generically.

    The path is resolved to RFC 6901 tokens ONCE, when the query is planned. A path
    expression that varies per row is a capability we have not built. Refusing it in
    the binder is what lets the message name the requirement; left to fall through
    it types cleanly and dies as "outside the c-native kernel set", which tells the
    reader nothing.
    """
    with pytest.raises(UnsupportedSyntaxError) as exc:
        results(f"SELECT doc @? doc AS x FROM {DOCS}")
    message = str(exc.value)
    assert "@?" in message, message
    assert "literal" in message, message


def test_over_a_dictionary_encoded_parquet_column(tmp_path):
    """A repeated document read from parquet — not a VALUES literal.

    Every other case here builds its column from literals. A low-cardinality string
    column read from a dictionary-encoded parquet file is the shape a real JSON
    document column arrives in, and it exercises the kernel's row access through
    `data[selection[i]]` over a column whose data buffer holds fewer values than it
    has rows. A kernel that indexed the data buffer directly would answer the wrong
    row here and be right everywhere above.
    """
    import pyarrow as pa  # test-only dep (allowed in tests/)
    import pyarrow.parquet as pq

    dataset = tmp_path / "docs"
    dataset.mkdir()
    docs = ['{"city":"x","n":null}', '{"city":"y"}', '{"other":1}', None] * 5
    table = pa.table(
        {
            "doc": pa.array(docs, type=pa.string()),
            "id": pa.array(list(range(20)), type=pa.int64()),
        }
    )
    pq.write_table(table, str(dataset / "part.parquet"), use_dictionary=True)

    assert results(f"SELECT COUNT(*) AS n FROM '{dataset}' WHERE doc @? 'city'") == {"n": [10]}
    head = results(f"SELECT doc @? 'city' AS x, id FROM '{dataset}' ORDER BY id LIMIT 4")
    assert head == {"x": [True, True, False, None], "id": [0, 1, 2, 3]}
    # The JSON-null key, over the same column.
    head = results(f"SELECT doc @? 'n' AS x, id FROM '{dataset}' ORDER BY id LIMIT 4")
    assert head == {"x": [True, False, False, None], "id": [0, 1, 2, 3]}


def test_a_non_json_document_is_refused_by_name():
    """The left operand must be a document, and saying so is `->`'s job description.

    An untyped NULL document reached the kernel and came back with an internal
    message about vector types. `NULL -> 'a'` has always answered
    "-> requires a string/JSON operand", and the two operators refuse in one shape.
    A non-string TYPE never gets that far — the operator map has no INTEGER pair for
    `@?` and refuses it a step earlier, which is the better message of the two.
    """
    with pytest.raises(IncorrectTypeError) as exc:
        results("SELECT NULL @? 'a' AS x")
    assert "@?" in str(exc.value), str(exc.value)

    with pytest.raises(IncorrectTypeError):
        results("SELECT id @? 'a' AS x FROM $planets")


def test_a_malformed_path_is_refused_when_the_query_is_planned():
    # The path is tokenized once, at bind. A path that is not a path fails there,
    # not per row — and not as a silent miss.
    with pytest.raises(Exception) as exc:
        results("SELECT '{\"a\":1}' @? '$.a[' AS x")
    assert "JSON path" in str(exc.value), str(exc.value)


def test_constant_folded_form_still_answers():
    # The one shape that always "worked", because constant folding answered it
    # before the engine was involved. It must keep working, and it must agree with
    # the kernel — a folder and a kernel that disagree is the wrong-answer class
    # this whole operator was in.
    assert results("SELECT '{\"a\":1}' @? 'a' AS x")["x"] == [True]
    assert results("SELECT '{\"a\":1}' @? 'b' AS x")["x"] == [False]


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
