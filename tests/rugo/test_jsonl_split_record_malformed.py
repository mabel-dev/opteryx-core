"""A JSONL record split in two by a raw newline is two malformed lines — wherever it falls.

The JSONBench Bluesky dump carries records cut by a raw 0x0A inside a long string value
(file_0005/0006/0007 at exactly 65536 bytes). Each half is one physical line that is not
one JSON object:

  * the first half opens a record and ends inside a string — truncated;
  * the second half starts inside a string, and still holds well-formed-looking
    `{...}` fragments (nested objects of the cut record).

rugo used to judge these by where they landed: fed alone, the orphaned second half became
one row per nested object (30 rows, malformed_count 0) and the truncated first half was
banked as a row once its projected columns were found (minimal extent never looked at the
rest of the line); in large buffers the threaded range split and the masked scan's
in-string state carried across the bad newline happened to drop them instead. The outcome
moved with chunk size — Q1 counted 10,000,024 rows from 10,000,000 lines at 8MB chunks.

The contract asserted here: a line that is not exactly one object is malformed — dropped
and counted with fail_on_error=False, an error with fail_on_error=True — independent of
its position in the buffer, of how the input is chunked, of the projection, of the threaded
range split and of how much structure its strings hold. Rows never exceed lines.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from rugo.rugo_native import read_jsonl

# Plain records, and dense records whose string values are full of in-string commas/colons
# that the structural index must mask out. Both must give the same answer.
PLAIN_TEXT = b"hello world"
DENSE_TEXT = b",:" * 40

PROJECTIONS = [None, ["kind"], ["n"], ["kind", "obj"], ["obj->>'b'"]]


def good_line(n, text):
    return (
        b'{"kind":"commit","n":%d,"text":"%s","obj":{"a":[1,2],"b":{"c":3}}}\n' % (n, text)
    )


def split_halves(first_len, text):
    """The two physical lines of one record cut by a raw newline inside "text"."""
    first = b'{"kind":"commit","n":-1,"text":"' + text + b"A" * first_len + b"\n"
    second = (
        b"BBBB exporting " + text
        + b'","obj":{"a":[1,{"x":1}],"b":{"c":3}},"o2":{"k":1},"o3":{"k":{"z":2}}}\n'
    )
    return first, second


def n_values(res):
    names = res["column_names"]
    if "n" not in names:
        return None
    return res["columns"][names.index("n")].to_pylist()


def check(data, columns, expected_ns, expected_malformed):
    res = read_jsonl(data, columns=columns, fail_on_error=False)
    assert res["num_rows"] == len(expected_ns)
    assert res["malformed_count"] == expected_malformed
    ns = n_values(res)
    if ns is not None:
        assert ns == expected_ns
    if expected_malformed:
        with pytest.raises(ValueError, match="Malformed JSONL"):
            read_jsonl(data, columns=columns, fail_on_error=True)
    else:
        assert read_jsonl(data, columns=columns, fail_on_error=True)["num_rows"] == len(expected_ns)


@pytest.mark.parametrize("text", [PLAIN_TEXT, DENSE_TEXT], ids=["plain", "dense"])
@pytest.mark.parametrize("columns", PROJECTIONS, ids=str)
def test_each_half_alone_is_one_malformed_line(text, columns):
    first, second = split_halves(65536 - 40, text)
    for half in (first, second, first.rstrip(b"\n"), second.rstrip(b"\n")):
        res = read_jsonl(half, columns=columns, fail_on_error=False)
        assert res["num_rows"] == 0
        assert res["malformed_count"] == 1
        with pytest.raises(ValueError, match="Malformed JSONL at line 1"):
            read_jsonl(half, columns=columns, fail_on_error=True)


@pytest.mark.parametrize("text", [PLAIN_TEXT, DENSE_TEXT], ids=["plain", "dense"])
@pytest.mark.parametrize("columns", PROJECTIONS, ids=str)
@pytest.mark.parametrize("where", ["start", "middle", "end", "end_no_newline"])
def test_split_record_at_any_position(text, columns, where):
    good = [good_line(n, text) for n in range(50)]
    first, second = split_halves(2000, text)
    at = {"start": 0, "middle": 25, "end": 50, "end_no_newline": 50}[where]
    lines = good[:at] + [first, second] + good[at:]
    data = b"".join(lines)
    if where == "end_no_newline":
        data = data[:-1]
    check(data, columns, list(range(50)), 2)


def newline_chunks(data, size):
    """Cut like the READ_JSONL chunkers: each boundary pushed forward to the next newline."""
    out, start = [], 0
    while start < len(data):
        end = data.find(b"\n", min(start + size, len(data)) - 1)
        end = len(data) if end < 0 else end + 1
        out.append(data[start:end])
        start = end
    return out


@pytest.mark.parametrize("text", [PLAIN_TEXT, DENSE_TEXT], ids=["plain", "dense"])
@pytest.mark.parametrize("columns", PROJECTIONS, ids=str)
def test_outcome_does_not_depend_on_chunking(text, columns):
    good = [good_line(n, text) for n in range(200)]
    first, second = split_halves(3000, text)
    lines = good[:60] + [first, second] + good[60:140] + [first, second] + good[140:]
    data = b"".join(lines)
    raw_newline = len(b"".join(good[:60] + [first])) - 1
    assert data[raw_newline:raw_newline + 1] == b"\n"

    sizes = [1, 97, 1000, 2500, 4096, 65536, len(data)]
    chunkings = [newline_chunks(data, s) for s in sizes]
    # One cut exactly at the raw newline, as an 8MB READ_JSONL chunk did on file_0006.
    chunkings.append([data[: raw_newline + 1], data[raw_newline + 1:]])
    for chunks in chunkings:
        assert b"".join(chunks) == data
        rows, malformed, ns = 0, 0, []
        for chunk in chunks:
            res = read_jsonl(chunk, columns=columns, fail_on_error=False)
            assert res["num_rows"] <= chunk.count(b"\n") + (not chunk.endswith(b"\n"))
            rows += res["num_rows"]
            malformed += res["malformed_count"]
            ns.extend(n_values(res) or [])
        assert rows == 200
        assert malformed == 4
        if columns is None or "n" in columns:
            assert ns == list(range(200))


@pytest.mark.parametrize("text", [PLAIN_TEXT, DENSE_TEXT], ids=["plain", "dense"])
@pytest.mark.parametrize("columns", [None, ["kind"], ["n"]], ids=str)
def test_threaded_range_split_inside_split_record(text, columns):
    """interpret_jsonl_threaded cuts ~12.6MB into newline-aligned ranges at the first
    newline at/after len*i/nt (nt <= 3 here: one range per 4MB). Every candidate cut
    point for nt = 2 and 3 is made to fall inside a 64KB first half, so the cut lands on
    the raw newline — the range then starts on the orphaned second half."""
    first, second = split_halves(65536 - 40, text)
    unit = len(first) + len(second)
    line = good_line(0, text)
    width = len(good_line(10**6, text))  # pad every n to the same width
    total_target = 12_600_000
    fractions = [1 / 3, 1 / 2, 2 / 3]
    n_good = (total_target - len(fractions) * unit) // width
    total = n_good * width + len(fractions) * unit

    lines, starts, n = [], [], 0
    offset = 0
    for k, f in enumerate(fractions):
        # Start the split record ~32KB before the cut so the cut falls in its first half.
        stop = int(f * total) - 32_768
        while offset + width <= stop:
            lines.append(good_line(10**6 + n, text))
            offset += width
            n += 1
        starts.append(offset)
        lines += [first, second]
        offset += unit
    while n < n_good:
        lines.append(good_line(10**6 + n, text))
        n += 1
    data = b"".join(lines)
    assert len(data) == total and len(line) <= width

    # Premise: for every nt this test targets, each cut lands on a split record's raw newline.
    raw_newlines = {s + len(first) - 1 for s in starts}
    for nt in (2, 3):
        for i in range(1, nt):
            assert data.find(b"\n", len(data) * i // nt) in raw_newlines

    res = read_jsonl(data, columns=columns, fail_on_error=False)
    assert res["num_rows"] == n_good
    assert res["malformed_count"] == 2 * len(fractions)
    ns = n_values(res)
    if ns is not None:
        assert ns == [10**6 + i for i in range(n_good)]
    with pytest.raises(ValueError, match="Malformed JSONL"):
        read_jsonl(data, columns=columns, fail_on_error=True)


@pytest.mark.parametrize(
    "line",
    [
        b'  {"n":1}  ',             # whitespace around the object is fine
        b'{"n":1}\r',               # CRLF line ending
    ],
)
def test_whitespace_around_one_object_is_valid(line):
    res = read_jsonl(line + b"\n" + line, fail_on_error=True)
    assert res["num_rows"] == 2
    assert res["malformed_count"] == 0


@pytest.mark.parametrize(
    "line",
    [
        b'x{"n":1}',                # content before the object
        b'{"n":1}x',                # content after the object
        b'{"n":1},',                # a marker after the object
        b'{"n":1}{"n":2}',          # two objects on one line
        b'{"n":1} {"n":2}',
        b'[{"n":1}]',               # not an object
        b'"n":1}',                  # starts mid-record
        b'{"n":1,"o":{"k":1}',      # nested object closes, record does not
        b'{"n":1,"s":"a\\',         # escape at end of line cannot carry the record over
    ],
)
@pytest.mark.parametrize("columns", [None, ["n"]], ids=str)
def test_line_that_is_not_one_object_is_malformed(line, columns):
    data = b'{"n":0}\n' + line + b'\n{"n":2}\n'
    res = read_jsonl(data, columns=columns, fail_on_error=False)
    assert res["num_rows"] == 2
    assert res["malformed_count"] == 1
    assert n_values(res) == [0, 2]
    with pytest.raises(ValueError, match="Malformed JSONL at line 2"):
        read_jsonl(data, columns=columns, fail_on_error=True)


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])


@pytest.mark.parametrize("columns", [None, ["n"], ["obj->>'b'"]], ids=str)
@pytest.mark.parametrize("newline_at", range(56, 72))
def test_line_ending_inside_string_at_any_block_offset(columns, newline_at):
    """A line that ends inside a string (odd quote count) must not carry its string state
    into the next line, wherever its newline falls in the structural scan's 64-byte blocks.
    A newline as a block's last byte (offset 63) used to hand the next block an in-string
    carry, so the following line was read with its quote parity inverted."""
    prefix = b'{"n":0,"text":"'
    first = prefix + b"x" * (newline_at - len(prefix)) + b"\n"
    assert first.index(b"\n") == newline_at
    data = first + b'{"n":1,"obj":{"b":"y"}}\n{"n":2,"obj":{"b":"z"}}\n'
    check(data, columns, [1, 2], 1)
