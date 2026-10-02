"""Raw Volnitsky prefilter (rugo/src/jsonl/core/jsonl_reader.cpp maybe_prefilter).

The prefilter drops records that cannot match a pushed string-equality predicate before
any structural parsing. It must never drop a real match: every result here is checked
against the same read with use_prefilter=False.

Whether the prefilter actually ran is observed through a malformed record that does not
contain the needle: with fail_on_error=True an unfiltered read raises on it, while a
prefiltered read never parses it.

Covered:
  * several AND-ed predicates — the selective one is used, the rest still apply;
  * nested `->>` predicates — matched on the QUOTED value, so the value embedded in a
    longer string (a URI) does not count against selectivity; a literal that could be a
    JSON number is matched UNQUOTED, so a numeric field holding it survives; every
    `\\u`-escaped line is kept because its decoded text need not appear in its bytes;
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
MALFORMED = b'not json at all'   # rejected even under projection; no needle, no \u escape


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


def test_nested_quoted_needle_ignores_value_inside_uris():
    # Every filler row carries the bare value inside a URI — an unquoted needle would hit
    # 100% of rows and never arm. The quoted needle hits only the real matches.
    rows = [
        b'{"id":%d,"commit":{"collection":"app.bsky.feed.like","subject":"at://did:plc:x/app.bsky.feed.post/%d"}}' % (i, i)
        for i in range(2000)
    ]
    rows += MATCHES[:1] + [MALFORMED]
    data = b"\n".join(rows) + b"\n"
    assert _ids(data, [(COLL, "==", "app.bsky.feed.post")], use_prefilter=True) == [5001]


def _collections_data(n, last_newline=True):
    # ~8% posts, the rest likes/reposts whose subject URI embeds the post collection; every
    # 997th line is malformed (not a match), every 1009th carries a \\u escape.
    rows = []
    for i in range(n):
        if i % 997 == 500:
            rows.append(b"not json at all")
            continue
        coll = b"app.bsky.feed.post" if i % 12 == 0 else b"app.bsky.feed.like"
        note = b"\\u00e9" if i % 1009 == 7 else b"x"
        rows.append(
            b'{"id":%d,"kind":"commit","commit":{"operation":"create","collection":"%s",'
            b'"subject":"at://did:plc:%d/app.bsky.feed.post/%d","note":"%s"}}' % (i, coll, i, i, note)
        )
    return b"\n".join(rows) + (b"\n" if last_newline else b"")


@pytest.mark.parametrize("last_newline", [True, False])
def test_prefilter_across_parallel_ranges_matches_unfiltered(last_newline):
    # ~40MB: well past the 4MB-per-thread floor, so interpret_jsonl_threaded splits it and
    # every range task prefilters its own lines.
    data = _collections_data(300_000, last_newline)
    assert len(data) > 32 << 20

    def read(pf):
        r = read_jsonl(data, columns=["id"], predicates=PREDICATES, fail_on_error=False, use_prefilter=pf)
        return sorted(r["columns"][0].to_pylist()), r["malformed_count"]

    on_ids, on_bad = read(True)
    off_ids, off_bad = read(False)
    assert on_ids == off_ids
    assert on_ids == [i for i in range(300_000) if i % 12 == 0 and i % 997 != 500]
    # The malformed lines never contain the needle, so the prefilter never parses them.
    assert on_bad == 0 and off_bad == len([i for i in range(300_000) if i % 997 == 500])


def test_final_line_without_newline_is_read():
    data = _bluesky_like(MATCHES)[:-1]  # last line (id 5005) has no trailing newline
    data += b'\n' + MATCHES[0].replace(b"5001", b"5006")  # a match as the unterminated last line
    assert _ids(data, PREDICATES, use_prefilter=True) == [5001, 5002, 5006]


def test_malformed_line_containing_the_needle_is_still_parsed():
    bad = b'{"id":-2,"commit":{"collection":"app.bsky.feed.post"'  # truncated, holds the needle
    data = _bluesky_like(MATCHES + [bad])
    with pytest.raises(ValueError):
        _ids(data, PREDICATES, use_prefilter=True)


def test_no_surviving_lines_reads_zero_rows():
    data = _bluesky_like([MALFORMED])
    assert _ids(data, [(COLL, "==", "app.bsky.graph.block")], use_prefilter=True) == []


@pytest.mark.parametrize("literal", ["app.bsky/post", "short", "has space"])
def test_nested_unsafe_literal_does_not_arm_the_prefilter(literal):
    data = _bluesky_like([MALFORMED])
    with pytest.raises(ValueError):
        _ids(data, [(COLL, "==", literal)], use_prefilter=True)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
