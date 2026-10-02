"""Raw Volnitsky prefilter (rugo/src/jsonl/core/jsonl_reader.cpp maybe_prefilter).

The prefilter drops records that cannot match a pushed string-equality predicate before
any structural parsing. It must never drop a real match: every result here is checked
against the same read with use_prefilter=False.

Whether the prefilter actually ran is observed through a malformed record that does not
contain the needle: with fail_on_error=True an unfiltered read raises on it, while a
prefiltered read never parses it.

Covered:
  * several AND-ed predicates — the selective one is used, the rest still apply;
  * nested `->>` predicates — matched on the UNQUOTED value, so a numeric field holding
    the literal's text survives, and every `\\u`-escaped line is kept because its decoded
    text need not appear in its bytes;
  * literals outside [A-Za-z0-9._:-] (or shorter than 6 bytes) do not arm it for nested
    columns.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from rugo.rugo_native import read_jsonl

COLL = "commit->>'collection'"
OP = "commit->>'operation'"
MALFORMED = b'{"id":-1 oops}'   # structurally invalid; no needle, no \u escape


def _ids(data, predicates, **kwargs):
    result = read_jsonl(data, columns=["id"], predicates=predicates, fail_on_error=True, **kwargs)
    cols = {n: v.to_pylist() for n, v in zip(result["column_names"], result["columns"])}
    return sorted(cols.get("id", [])) if result["num_rows"] else []


def _bluesky_like(extra_rows):
    rows = [
        b'{"id":%d,"kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.like"}}' % i
        for i in range(2000)
    ]
    rows += extra_rows
    return b"\n".join(rows) + b"\n"


MATCHES = [
    # plain match
    b'{"id":5001,"kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.post"}}',
    # value spelled with a \u escape: decodes to app.bsky.feed.post, bytes do not contain it
    b'{"id":5002,"kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.\\u0070ost"}}',
    # needle present but in another field: a false positive the predicate must reject
    b'{"id":5003,"kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.like","note":"app.bsky.feed.post"}}',
    # matches collection but fails another AND-ed predicate
    b'{"id":5004,"kind":"commit","commit":{"operation":"delete","collection":"app.bsky.feed.post"}}',
    b'{"id":5005,"kind":"identity","commit":{"operation":"create","collection":"app.bsky.feed.post"}}',
]

PREDICATES = [("kind", "==", "commit"), (OP, "==", "create"), (COLL, "==", "app.bsky.feed.post")]


@pytest.mark.parametrize("use_prefilter", [True, False])
def test_nested_and_multi_predicate_results_are_exact(use_prefilter):
    data = _bluesky_like(MATCHES)
    assert _ids(data, PREDICATES, use_prefilter=use_prefilter) == [5001, 5002]


def test_nested_multi_predicate_arms_the_prefilter():
    data = _bluesky_like(MATCHES + [MALFORMED])
    # The malformed record lacks the needle and any \u escape: prefiltered away, never parsed.
    assert _ids(data, PREDICATES, use_prefilter=True) == [5001, 5002]
    with pytest.raises(ValueError):
        _ids(data, PREDICATES, use_prefilter=False)


def test_nested_numeric_value_survives_unquoted_needle():
    rows = [b'{"id":%d,"c":{"n":%d}}' % (i, i) for i in range(2000)]
    rows += [b'{"id":9001,"c":{"n":123456}}', b'{"id":9002,"c":{"n":"123456"}}', MALFORMED]
    data = b"\n".join(rows) + b"\n"
    assert _ids(data, [("c->>'n'", "==", "123456")], use_prefilter=True) == [9001, 9002]


@pytest.mark.parametrize("literal", ["app.bsky/post", "short", "has space"])
def test_nested_unsafe_literal_does_not_arm_the_prefilter(literal):
    data = _bluesky_like([MALFORMED])
    with pytest.raises(ValueError):
        _ids(data, [(COLL, "==", literal)], use_prefilter=True)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
