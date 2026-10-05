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
  * literals outside [A-Za-z0-9._:-] (or whose needle is under 4 bytes) do not arm it for
    nested columns;
  * IN lists — one needle per member, a record survives if it holds ANY of them; one
    ineligible member disarms the whole IN;
  * AND-ed clauses — the selective clause drives the scan and every other eligible clause
    is CONFIRMED on the surviving line, so a line holding the driver's needle but not a
    confirming clause's is dropped unparsed;
  * short literals (needle >= 4 bytes) — the SIMD sieve driver (needles <= 16 bytes) and the
    Volnitsky driver (longer) are both exercised;
  * a randomized differential check of many shapes against use_prefilter=False;
  * skewed data: the gate judges the head, but every 4MB window is re-judged, so a
    non-selective region later in the buffer is parsed by the normal path.
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
    # truncated, holds every clause's needle (driver + confirms): a candidate, so parsed
    bad = b'{"id":-2,"kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.post"'
    data = _bluesky_like(MATCHES + [bad])
    with pytest.raises(ValueError):
        _ids(data, PREDICATES, use_prefilter=True)


def test_no_surviving_lines_reads_zero_rows():
    data = _bluesky_like([MALFORMED])
    assert _ids(data, [(COLL, "==", "app.bsky.graph.block")], use_prefilter=True) == []


@pytest.mark.parametrize("literal", ["app.bsky/post", "x", "has space"])
def test_nested_unsafe_literal_does_not_arm_the_prefilter(literal):
    data = _bluesky_like([MALFORMED])
    with pytest.raises(ValueError):
        _ids(data, [(COLL, "==", literal)], use_prefilter=True)


# ---------------------------------------------------------------- IN lists ---

IN_ROWS = [
    b'{"id":6001,"commit":{"collection":"app.bsky.graph.block"}}',
    b'{"id":6002,"commit":{"collection":"app.bsky.graph.listblock"}}',
    # second member spelled with a \u escape: kept for its escape, matched downstream
    b'{"id":6003,"commit":{"collection":"app.bsky.graph.list\\u0062lock"}}',
    # a member's bytes elsewhere in the line: false positive, verified away
    b'{"id":6004,"commit":{"collection":"app.bsky.feed.like","x":"app.bsky.graph.block"}}',
    # the bare member inside a URI: not even a candidate (quoted needle)
    b'{"id":6005,"commit":{"collection":"app.bsky.feed.like","u":"at://x/app.bsky.graph.block/1"}}',
]
IN_PRED = [(COLL, "in", ["app.bsky.graph.block", "app.bsky.graph.listblock"])]


@pytest.mark.parametrize("use_prefilter", [True, False])
def test_in_list_results_are_exact(use_prefilter):
    data = _bluesky_like(IN_ROWS)
    assert _ids(data, IN_PRED, use_prefilter=use_prefilter) == [6001, 6002, 6003]


def test_in_list_arms_the_prefilter():
    data = _bluesky_like(IN_ROWS + [MALFORMED])
    assert _ids(data, IN_PRED, use_prefilter=True) == [6001, 6002, 6003]
    with pytest.raises(ValueError):
        _ids(data, IN_PRED, use_prefilter=False)


def test_in_list_with_an_ineligible_member_does_not_arm():
    data = _bluesky_like(IN_ROWS + [MALFORMED])
    with pytest.raises(ValueError):
        _ids(data, [(COLL, "in", ["app.bsky.graph.block", "has space"])], use_prefilter=True)


def test_in_list_top_level_column():
    rows = [b'{"id":%d,"kind":"commit"}' % i for i in range(2000)]
    rows += [b'{"id":7001,"kind":"identity"}', b'{"id":7002,"kind":"account"}',
             b'{"id":7003,"kind":"accountx"}', MALFORMED]
    data = b"\n".join(rows) + b"\n"
    assert _ids(data, [("kind", "in", ["identity", "account"])], use_prefilter=True) == [7001, 7002]


def test_in_list_bare_numeric_needles_one_a_prefix_of_another():
    # Both members could be JSON numbers, so both needles are BARE, and "12345" is a prefix
    # of "123456": the multi-pattern window is the shorter one and both must still be found.
    rows = [b'{"id":%d,"c":{"n":%d}}' % (i, i) for i in range(2000)]
    rows += [b'{"id":9101,"c":{"n":123456}}', b'{"id":9102,"c":{"n":12345}}',
             b'{"id":9103,"c":{"n":"12345"}}', b'{"id":9104,"c":{"n":1234}}', MALFORMED]
    data = b"\n".join(rows) + b"\n"
    assert _ids(data, [("c->>'n'", "in", ["123456", "12345"])], use_prefilter=True) == [9101, 9102, 9103]


# ------------------------------------------------------ AND: driver + confirm ---

def test_and_confirming_clause_drops_lines_unparsed():
    # Malformed lines holding the DRIVER needle ("delete") but not the confirming one
    # ("app.bsky.feed.post"): parsed (and fatal) only if the confirm step is missing.
    bad = b'{"id":-3,"commit":{"operation":"delete"'
    rows = [b'{"id":%d,"commit":{"operation":"create","collection":"app.bsky.feed.post"}}' % i
            for i in range(2000)]
    rows += [
        b'{"id":8001,"commit":{"operation":"delete","collection":"app.bsky.feed.post"}}',
        b'{"id":8002,"commit":{"operation":"delete","collection":"app.bsky.feed.like"}}',
        # confirming clause satisfied only through a \u escape
        b'{"id":8003,"commit":{"operation":"delete","collection":"app.bsky.feed.\\u0070ost"}}',
        bad,
    ]
    data = b"\n".join(rows) + b"\n"
    preds = [(OP, "==", "delete"), (COLL, "==", "app.bsky.feed.post")]
    assert _ids(data, preds, use_prefilter=True) == [8001, 8003]
    with pytest.raises(ValueError):
        _ids(data, preds, use_prefilter=False)


def test_and_top_level_confirm_is_not_satisfied_by_an_escape():
    # A top-level clause compares RAW bytes: a \u spelling never matches it, so an escape
    # does not excuse the line from that clause.
    rows = [b'{"id":%d,"kind":"commit","commit":{"operation":"create"}}' % i for i in range(2000)]
    rows += [
        b'{"id":8101,"kind":"commit","commit":{"operation":"delete"}}',
        b'{"id":8102,"kind":"identity","commit":{"operation":"delete","n":"\\u0041"}}',
    ]
    data = b"\n".join(rows) + b"\n"
    preds = [("kind", "==", "commit"), (OP, "==", "delete")]
    assert _ids(data, preds, use_prefilter=True) == _ids(data, preds, use_prefilter=False) == [8101]


# ------------------------------------------------------------ short literals ---

def test_short_literal_arms_and_is_exact():
    rows = [b'{"id":%d,"commit":{"rkey":"3lbh%d"}}' % (i, i) for i in range(2000)]
    rows += [
        b'{"id":9201,"commit":{"rkey":"self"}}',
        b'{"id":9202,"commit":{"rkey":"\\u0073elf"}}',
        b'{"id":9203,"commit":{"rkey":"x","t":"self"}}',         # false positive
        b'{"id":9204,"commit":{"rkey":"selfie"}}',                # not a candidate
        MALFORMED,
    ]
    data = b"\n".join(rows) + b"\n"
    assert _ids(data, [("commit->>'rkey'", "==", "self")], use_prefilter=True) == [9201, 9202]


def test_two_byte_literal_arms():
    rows = [b'{"id":%d,"c":{"k":"zz"}}' % i for i in range(2000)]
    rows += [b'{"id":9301,"c":{"k":"ab"}}', MALFORMED]
    data = b"\n".join(rows) + b"\n"
    assert _ids(data, [("c->>'k'", "==", "ab")], use_prefilter=True) == [9301]


@pytest.mark.parametrize("literal", ["abcd", "a" * 14, "a" * 15, "b" * 40])
def test_both_drivers_find_needles_at_every_alignment(literal):
    # Needles <= 16 bytes (quoted) take the SIMD sieve, longer ones Volnitsky. Pad each match
    # by 0..80 bytes so its start lands at every offset of a 64-byte sieve block, and end the
    # buffer on a match with no trailing newline (the scalar tail).
    lit = literal.encode()
    rows = [b'{"id":%d,"c":{"k":"zz"}}' % i for i in range(2000)]
    want = []
    for pad in range(81):
        rows.append(b'{"id":%d,"p":"%s","c":{"k":"%s"}}' % (10000 + pad, b"y" * pad, lit))
        want.append(10000 + pad)
    rows.append(b'{"id":20000,"c":{"k":"%s"}}' % lit)
    want.append(20000)
    data = b"\n".join(rows)
    assert _ids(data, [("c->>'k'", "==", literal)], use_prefilter=True) == want


# ---------------------------------------------------- randomized differential ---

def _random_corpus(seed, n):
    import random

    rnd = random.Random(seed)
    colls = ["app.bsky.feed.like", "app.bsky.feed.post", "app.bsky.graph.block",
             "app.bsky.graph.listblock", "app.bsky.graph.list", "self", "ab", "12345"]
    ops = ["create", "delete", "update"]
    rows = []
    for i in range(n):
        r = rnd.random()
        if r < 0.003:
            rows.append(b"not json at all")
            continue
        c = rnd.choice(colls).encode()
        if rnd.random() < 0.01:   # \u-escape the first byte
            c = b"\\u%04x" % c[0] + c[1:]
        num = rnd.random() < 0.05 and c == b"12345"
        cv = c if num else b'"' + c + b'"'
        noise = rnd.choice(colls).encode()
        rows.append(b'{"id":%d,"kind":"%s","commit":{"operation":"%s","collection":%s,'
                    b'"rkey":"%s","note":"%s"}}' % (i, rnd.choice([b"commit", b"identity"]),
                                                    rnd.choice(ops).encode(), cv,
                                                    rnd.choice([b"self", b"3lbh"]), noise))
    return b"\n".join(rows) + b"\n"


RANDOM_SHAPES = [
    [(COLL, "==", "app.bsky.graph.block")],
    [(COLL, "in", ["app.bsky.graph.block", "app.bsky.graph.listblock", "app.bsky.graph.list"])],
    [(COLL, "in", ["self", "ab"])],
    [(COLL, "in", ["12345", "ab"])],
    [(OP, "==", "delete"), (COLL, "==", "app.bsky.feed.post")],
    [("kind", "==", "identity"), (COLL, "in", ["ab", "app.bsky.graph.block"])],
    [("commit->>'rkey'", "==", "self"), (OP, "==", "update")],
]


@pytest.mark.parametrize("shape", range(len(RANDOM_SHAPES)))
@pytest.mark.parametrize("n", [5_000, 400_000])
def test_random_shapes_match_unfiltered(shape, n):
    # 400k rows is ~60MB: split across range tasks, each prefiltering its own lines.
    data = _random_corpus(shape * 7 + n, n)
    preds = RANDOM_SHAPES[shape]

    def read(pf):
        r = read_jsonl(data, columns=["id"], predicates=preds, fail_on_error=False, use_prefilter=pf)
        return sorted(r["columns"][0].to_pylist()) if r["num_rows"] else []

    assert read(True) == read(False)



# ---------------------------------------------------------------- skewed data ---

def _skewed(malformed_at):
    # ~6MB where the post collection is rare (the gate arms on this head), then ~6MB where
    # EVERY line is a post (non-selective). A malformed line without the needle is skipped
    # unparsed inside a filtered window, but parsed — and fatal — inside an unfiltered one.
    head = [b'{"id":%d,"commit":{"collection":"app.bsky.feed.like","pad":"%s"}}' % (i, b"x" * 40)
            for i in range(80_000)]
    tail = [b'{"id":%d,"commit":{"collection":"app.bsky.feed.post","pad":"%s"}}' % (i, b"x" * 40)
            for i in range(80_000, 160_000)]
    rows = head + tail
    if malformed_at is not None:
        rows.insert(malformed_at, MALFORMED)
    return b"\n".join(rows) + b"\n"


@pytest.mark.parametrize("use_prefilter", [True, False])
def test_skewed_results_are_exact(use_prefilter):
    data = _skewed(None)
    assert len(data) > 8 << 20
    assert _ids(data, [(COLL, "==", "app.bsky.feed.post")], use_prefilter=use_prefilter) == list(range(80_000, 160_000))


def test_skewed_head_window_is_filtered():
    data = _skewed(1_000)
    assert _ids(data, [(COLL, "==", "app.bsky.feed.post")], use_prefilter=True) == list(range(80_000, 160_000))


def test_skewed_tail_window_is_parsed_whole():
    data = _skewed(150_000)
    with pytest.raises(ValueError):
        _ids(data, [(COLL, "==", "app.bsky.feed.post")], use_prefilter=True)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
