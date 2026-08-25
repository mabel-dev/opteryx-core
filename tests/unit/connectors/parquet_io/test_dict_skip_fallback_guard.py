"""
The dictionary decode-skip's SOUNDNESS GUARD: a chunk with dictionary FALLBACK.

The scan's dictionary decode-skip (rugo `decode_column.cpp`) probes a row group's
dictionary page for a pushed per-value predicate and, when no dictionary entry can
satisfy it, skips decoding that row group's data pages entirely.

That is only sound if the dictionary covers EVERY value in the chunk. Parquet
writers (Arrow, parquet-mr, Spark) fall back to PLAIN once a dictionary outgrows
its page-size limit, and the values in those fallback pages are NOT in the
dictionary page. Probing a dictionary that does not describe the whole chunk and
concluding "absent" silently DROPS REAL MATCHES — the wrong-answer direction, not
the slow direction.

rugo guards this by pre-scanning the data-page headers (header-only, no
decompression) and permitting the skip only when every data page is
RLE_DICTIONARY / PLAIN_DICTIONARY — `dict_covers_all_rows` in
`rugo/src/parquet/decode_column.cpp`.

These tests pin that guard. Every fixture below places the needle AFTER the
dictionary has spilled, so the needle lives in a PLAIN page and is absent from
the dictionary page — exactly the shape that makes an unguarded probe answer
zero. They are behavioural: with the guard defeated, each `expected` below
becomes 0.

This matters beyond a hypothetical. 4 of ClickBench's 105 columns carry a PLAIN
fallback page, so this is the ordinary shape of real-world files, not a corner.

Each fixture also asserts the written file actually contains BOTH encodings. A
fixture that quietly stopped spilling would still pass every behavioural
assertion while testing nothing at all.
"""

import os
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import opteryx
from opteryx.connectors import DiskConnector

# Small enough that a few thousand padding values overflow it, large enough that
# the dictionary is genuinely used first (so the chunk is a real MIXED chunk and
# not simply a PLAIN one, which the probe declines for a different reason).
DICT_PAGE_LIMIT = 64 * 1024
PAD_ROWS = 30_000
NEEDLE_ROWS = 50

STR_NEEDLE = "zzz-needle-value"
INT_NEEDLE = 999_000_111

_WS_COUNTER = [0]


def _unique_ws():
    _WS_COUNTER[0] += 1
    return f"ws_dictfallback_{_WS_COUNTER[0]}"


def _write(values, dtype):
    """One row group whose dictionary SPILLS, with the needle after the spill.

    `row_group_size` is deliberately larger than the data: the whole column must
    be one chunk, so that the dictionary and the fallback pages are part of the
    same chunk and the probe is faced with the mixed case.
    """
    tbl = pa.table({"v": pa.array(values, type=dtype)})
    tmp = tempfile.mkdtemp()
    ws = _unique_ws()
    data_dir = os.path.join(tmp, ws, "t")
    os.makedirs(data_dir)
    path = os.path.join(data_dir, "data.parquet")
    pq.write_table(
        tbl,
        path,
        use_dictionary=True,
        dictionary_pagesize_limit=DICT_PAGE_LIMIT,
        row_group_size=len(values) * 2,
        compression="none",
    )

    # The fixture must actually reproduce the condition it exists to test.
    encodings = set(pq.ParquetFile(path).metadata.row_group(0).column(0).encodings)
    assert "RLE_DICTIONARY" in encodings, encodings   # a dictionary was used
    assert "PLAIN" in encodings, encodings            # ...and then it spilled

    return tmp, ws


def _count(tmp, ws, where):
    cwd = os.getcwd()
    os.chdir(tmp)
    try:
        opteryx.register_workspace(ws, DiskConnector)
        sql = f"SELECT COUNT(*) AS c FROM {ws}.t WHERE {where}"
        for morsel in opteryx.session().execute_to_morsels(sql):
            return morsel.column(b"c").to_pylist()[0]
        return None
    finally:
        os.chdir(cwd)


@pytest.fixture(scope="module")
def string_spill():
    """Wide, all-distinct padding strings overflow the dictionary; the needle is
    appended afterwards, so it can only be in a PLAIN page."""
    values = [f"unique-padding-value-{i:012d}" for i in range(PAD_ROWS)]
    values += [STR_NEEDLE] * NEEDLE_ROWS
    return _write(values, pa.string())


@pytest.fixture(scope="module")
def int_spill():
    """The same shape at int64. Ints reach the probe through a different branch
    (`int64_dict_mode`, comparing int64 needles) than strings do, so the guard
    has to hold on both and one passing does not imply the other."""
    values = [1_000_000 + i for i in range(PAD_ROWS)]
    values += [INT_NEEDLE] * NEEDLE_ROWS
    return _write(values, pa.int64())


# --- the guard ---------------------------------------------------------------


def test_string_equality_finds_a_needle_that_spilled(string_spill):
    """`= <value in a PLAIN fallback page>` must return its rows.

    This is the assertion the whole file exists for: the needle is NOT in the
    dictionary page, so an unguarded probe declares the dictionary disjoint,
    skips every data page, and answers 0.
    """
    tmp, ws = string_spill
    assert _count(tmp, ws, f"v = '{STR_NEEDLE}'") == NEEDLE_ROWS


def test_string_in_list_finds_a_needle_that_spilled(string_spill):
    """IN reaches the probe as the same membership kind as `=` and must agree."""
    tmp, ws = string_spill
    assert _count(tmp, ws, f"v IN ('{STR_NEEDLE}', 'no-such-value')") == NEEDLE_ROWS


@pytest.mark.parametrize(
    "predicate",
    [
        "v LIKE 'zzz-needle%'",   # _STARTS_WITH
        "v LIKE '%-needle-value'",  # _ENDS_WITH
        "v LIKE '%needle%'",      # InStr
    ],
)
def test_like_forms_find_a_needle_that_spilled(string_spill, predicate):
    """The LIKE lowerings (`_STARTS_WITH` / `_ENDS_WITH` / `InStr`) feed the same
    probe as equality does, so the guard has to cover them too. LIKE is the case
    where the decode-skip earns the most — and therefore the case where losing
    the guard costs the most.
    """
    tmp, ws = string_spill
    assert _count(tmp, ws, predicate) == NEEDLE_ROWS


def test_int_equality_finds_a_needle_that_spilled(int_spill):
    """The int branch of the probe, same shape."""
    tmp, ws = int_spill
    assert _count(tmp, ws, f"v = {INT_NEEDLE}") == NEEDLE_ROWS


def test_int_in_list_finds_a_needle_that_spilled(int_spill):
    tmp, ws = int_spill
    assert _count(tmp, ws, f"v IN ({INT_NEEDLE}, 7)") == NEEDLE_ROWS


# --- the guard must not be achieved by disabling the optimisation ------------


def test_a_genuinely_absent_value_is_still_zero(string_spill):
    """The guard keeps rows; it must not invent them. A value that is in neither
    the dictionary nor any fallback page still answers 0 — otherwise "fix the
    guard" could be satisfied by making the probe match everything.
    """
    tmp, ws = string_spill
    assert _count(tmp, ws, "v = 'genuinely-absent-value'") == 0
    assert _count(tmp, ws, "v LIKE 'no-such-prefix%'") == 0


def test_a_padding_value_still_answers_from_the_dictionary(string_spill):
    """A value that IS in the dictionary answers correctly in the same chunk, so
    the mixed chunk is not simply failing open into a wrong count.
    """
    tmp, ws = string_spill
    assert _count(tmp, ws, "v = 'unique-padding-value-000000000000'") == 1
