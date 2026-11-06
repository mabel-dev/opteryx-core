"""The per-column block size (docs/PARQUET_WRITE_FOR_READ_SPEED.md §4.5 item 3).

`narrow_chunk_bytes` > 0 gives a rugo-written file two classes of column,
decided from the FIRST block's LARGEST encoded chunk: WIDE columns keep
the grouped layout (column-major in blocks of `row_groups_per_block`, schema
order); NARROW columns are held back and placed as one byte-adjacent RUN per
column — every row group's chunk in order — after the wide blocks, either one
run per file (`narrow_run_blocks` = 0, before the bloom tail) or one every N
blocks. Nothing in the footer names the layout: every chunk is located by its
own offsets, so any reader reads the file, and the native scan infers the
fetch blocks from adjacency alone (`infer_fetch_blocks`).

Pinned here, with pyarrow as the metadata oracle:
  * the byte order: wide blocks, then the narrow runs (schema order, row-group
    order inside each), then the blooms; with N > 0 a run after every N blocks;
  * the class is decided from the first block's largest chunk (not its mean);
    a narrow column whose later chunk exceeds 4 x the threshold is DEMOTED to
    wide, its run so far placed at that block boundary;
  * `narrow_held_budget_bytes` closes every run at a block boundary when the
    held chunks would pass it — the same bytes as a fixed run length when the
    budget falls on the same boundary — and the peak stays within the budget
    plus one block;
  * a narrow-only projection is adjacent across the whole run (one fetch
    block under the reader's rule), a projection with a wide column breaks at
    every block boundary (blocks of G) — the rule reproduced here, the reader
    itself pinned in tests/unit/connectors/parquet_io/test_fetch_blocks_per_column.py;
  * values are identical to the default layout's in pyarrow and rugo;
  * the streaming writer and the one-shot writer produce the same bytes, and
    the streaming writer reports the memory it holds back;
  * narrow_chunk_bytes = 0 (the default) is the grouped layout, byte for byte.
"""

import io
import os
import random
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pyarrow.parquet as pq  # test oracle only

from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from rugo import parquet as rp
from rugo.rugo_native import read_rowgroup_stats

R = 1000          # rows per row group
G = 4             # row groups per block
T = 2048          # narrow threshold, bytes: the tiny columns are far below, the rest far above
NAMES = [b"a", b"s", b"lo", b"flag", b"f", b"grow"]
NARROW = {"lo", "flag", "grow"}


def _morsel(n, seed=7, grow=False):
    """Uncompressed chunk sizes at R rows: a 8 KB, s ~16 KB, f 8 KB (wide);
    lo (3 values, dictionary) and flag (bool) a few hundred bytes (narrow).
    `grow` is one value throughout (narrow) unless grow=True, when it is one
    value for the first block's rows and random strings after — tiny in block
    0, ~20 KB per chunk later (the sorted-data case that demotes)."""
    rng = random.Random(seed)
    return Morsel.from_vectors(
        NAMES,
        [
            vector_from_sequence(list(range(n)), DrakenType.INT64),
            vector_from_sequence([f"{rng.randrange(10**12):012d}-{i}" for i in range(n)], DrakenType.VARCHAR),
            vector_from_sequence([("x", "y", "z")[i % 3] for i in range(n)], DrakenType.VARCHAR),
            vector_from_sequence([i % 7 == 0 for i in range(n)], DrakenType.BOOL),
            vector_from_sequence([i * 0.25 for i in range(n)], DrakenType.FLOAT64),
            vector_from_sequence(["same" if (not grow or i < G * R) else f"{rng.randrange(10**12):012d}-{i}"
                                  for i in range(n)], DrakenType.VARCHAR),
        ],
    )


def _write(m, **kw):
    kw.setdefault("compression", "none")   # deterministic chunk sizes
    kw.setdefault("max_rows_per_row_group", R)
    kw.setdefault("row_groups_per_block", G)
    return rp.write_parquet(m, **kw)


def _chunks(data):
    """[rg][col] -> (start, end) of the column chunk, from the footer."""
    md = pq.ParquetFile(io.BytesIO(data)).metadata
    out = []
    for rg in range(md.num_row_groups):
        row = []
        for c in range(md.num_columns):
            col = md.row_group(rg).column(c)
            start = col.dictionary_page_offset if col.has_dictionary_page else col.data_page_offset
            row.append((start, start + col.total_compressed_size))
        out.append(row)
    return out


def _names(data):
    return [c.name for c in rp.read_metadata(data).schema_columns]


def _classes(data, threshold=T):
    """The writer's rule, from the footer: narrow = first block's largest chunk <= threshold."""
    ch = _chunks(data)
    first = ch[:G]
    return {name: max(e - s for s, e in (row[c] for row in first)) <= threshold
            for c, name in enumerate(_names(data))}


def _blocks(n_rg, g):
    return [list(range(b, min(b + g, n_rg))) for b in range(0, n_rg, g)]


def _expected_order(n_rg, narrow, run_blocks):
    """The sequence of (rg, col) chunks the file must hold, in byte order."""
    cols = [n.decode() for n in NAMES]
    wide = [c for c in range(len(cols)) if cols[c] not in narrow]
    narrow_idx = [c for c in range(len(cols)) if cols[c] in narrow]
    order, held, since = [], [], 0
    for block in _blocks(n_rg, G):
        for c in wide:
            order.extend((rg, c) for rg in block)
        held.extend(block)
        since += 1
        if run_blocks and since == run_blocks:
            for c in narrow_idx:
                order.extend((rg, c) for rg in held)
            held, since = [], 0
    for c in narrow_idx:
        order.extend((rg, c) for rg in held)
    return order


def _assert_layout(data, n_rg, run_blocks):
    assert _classes(data) == {n.decode(): n.decode() in NARROW for n in NAMES}
    ch = _chunks(data)
    assert len(ch) == n_rg
    cursor = 4  # first byte after the PAR1 magic
    for rg, c in _expected_order(n_rg, NARROW, run_blocks):
        start, end = ch[rg][c]
        assert start == cursor, (rg, c, start, cursor)
        cursor = end
    # The blooms follow the last chunk.
    for rg in read_rowgroup_stats(data):
        for col in rg["columns"]:
            if col["bloom_offset"] is not None and col["bloom_offset"] >= 0:
                assert col["bloom_offset"] >= cursor


def _infer_blocks(ch, cols):
    """The reader's rule (ParquetIOPipeline::infer_fetch_blocks): row group k+1
    joins k's block when every projected column's chunk starts where its
    predecessor's ends. Returns the number of blocks."""
    blocks = 1
    for k in range(1, len(ch)):
        if any(ch[k][c][0] != ch[k - 1][c][1] for c in cols):
            blocks += 1
    return blocks


# ── byte order ────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("n_rg", [10, 8, 5, 4, 1])
def test_narrow_runs_follow_the_wide_blocks(n_rg):
    data = _write(_morsel(n_rg * R), narrow_chunk_bytes=T)
    _assert_layout(data, n_rg, run_blocks=0)


@pytest.mark.parametrize("n_rg,run_blocks", [(10, 2), (10, 1), (12, 3), (8, 2), (3, 2)])
def test_a_run_every_n_blocks(n_rg, run_blocks):
    data = _write(_morsel(n_rg * R), narrow_chunk_bytes=T, narrow_run_blocks=run_blocks)
    _assert_layout(data, n_rg, run_blocks=run_blocks)


def test_a_column_that_grows_is_demoted_at_the_block_boundary():
    """`grow` is one value for block 0 and ~20 KB a chunk after: narrow on the
    first block, demoted when block 1 shows a chunk over 4 x T. Its run so far
    (row groups 0-3) is placed at the block-1 boundary, before block 1's wide
    chunks, and from block 1 on it is placed with the wide columns in schema
    order. The other narrow columns keep their whole-file runs."""
    n_rg = 12
    data = _write(_morsel(n_rg * R, grow=True), narrow_chunk_bytes=T)
    ch = _chunks(data)
    names = _names(data)
    c = {n: names.index(n) for n in names}
    assert all(e - s <= T for s, e in (ch[rg][c["grow"]] for rg in range(G)))
    assert all(e - s > 4 * T for s, e in (ch[rg][c["grow"]] for rg in range(G, n_rg)))
    wide_after = [c["a"], c["s"], c["f"], c["grow"]]   # schema order
    order = [(rg, col) for col in (c["a"], c["s"], c["f"]) for rg in range(0, 4)]
    order += [(rg, c["grow"]) for rg in range(0, 4)]              # the demoted run
    for block in ([4, 5, 6, 7], [8, 9, 10, 11]):
        order += [(rg, col) for col in wide_after for rg in block]
    order += [(rg, c["lo"]) for rg in range(n_rg)] + [(rg, c["flag"]) for rg in range(n_rg)]
    cursor = 4
    for rg, col in order:
        assert ch[rg][col][0] == cursor, (rg, names[col], ch[rg][col][0], cursor)
        cursor = ch[rg][col][1]
    # What the reader infers: the demoted column is a run, then blocks of four.
    assert _infer_blocks(ch, [c["grow"]]) == 3
    assert _infer_blocks(ch, [c["lo"]]) == 1
    assert _infer_blocks(ch, [c["lo"], c["grow"]]) == 3
    # The streaming writer reports it, and writes the same bytes.
    m = _morsel(n_rg * R, grow=True)
    chunks = []
    with rp.open_parquet_writer(chunks.append, compression="none", narrow_chunk_bytes=T) as w:
        for rg in range(n_rg):
            w.write_row_group(m.slice(rg * R, R))
        assert w.narrow_demoted_columns == 1
    assert b"".join(chunks) == data
    assert pq.read_table(io.BytesIO(data)).equals(m.to_arrow())


def test_a_column_is_classified_by_its_largest_first_block_chunk():
    """`spike` is random strings for row group 0 and one value after: its first
    block MEAN is under the threshold, its largest chunk is not — wide, placed
    in blocks of four (rg 3 and rg 4 not adjacent)."""
    import random as _r

    rng = _r.Random(1)
    n = 8 * R
    m = Morsel.from_vectors(
        [b"a", b"spike"],
        [vector_from_sequence(list(range(n)), DrakenType.INT64),
         vector_from_sequence([f"{rng.randrange(10**12):012d}-{i}" if i < R else "same"
                               for i in range(n)], DrakenType.VARCHAR)])
    big = 6 * 1024
    data = _write(m, narrow_chunk_bytes=big)
    ch = _chunks(data)
    sizes = [e - s for s, e in (ch[rg][1] for rg in range(G))]
    assert max(sizes) > big and sum(sizes) / len(sizes) <= big, sizes
    assert ch[4][1][0] != ch[3][1][1], "spike must be wide: blocks of four, not a run"
    assert _infer_blocks(ch, [1]) == 2


@pytest.mark.parametrize("n_rg", [12, 16])
def test_the_held_budget_closes_runs_at_block_boundaries(n_rg):
    """A budget of 2.5 blocks of narrow chunks closes every run before the block
    that would take the held bytes past it — here every two blocks, the same
    bytes a fixed run of two blocks writes — and the peak held stays within the
    budget plus one block."""
    m = _morsel(n_rg * R)
    whole = _write(m, narrow_chunk_bytes=T)
    ch = _chunks(whole)
    narrow_idx = [c for c, n in enumerate(_names(whole)) if n in NARROW]
    per_block = sum(ch[rg][c][1] - ch[rg][c][0] for rg in range(G) for c in narrow_idx)
    budget = int(per_block * 2.5)
    data = _write(m, narrow_chunk_bytes=T, narrow_held_budget_bytes=budget)
    assert data == _write(m, narrow_chunk_bytes=T, narrow_run_blocks=2)
    _assert_layout(data, n_rg, run_blocks=2)
    lo = _names(data).index("lo")
    runs = -(-n_rg // (2 * G))   # a run per two blocks, the last may be partial
    assert _infer_blocks(_chunks(data), [lo]) == runs
    chunks = []
    with rp.open_parquet_writer(chunks.append, compression="none", narrow_chunk_bytes=T,
                                narrow_held_budget_bytes=budget) as w:
        for rg in range(n_rg):
            w.write_row_group(m.slice(rg * R, R))
        assert w.peak_held_narrow_bytes == 2 * per_block
        assert w.peak_held_narrow_bytes <= budget + per_block
        assert w.narrow_budget_closes == runs - 1
    assert b"".join(chunks) == data
    # A budget under one block's narrow chunks degrades to a run per block.
    tiny = _write(m, narrow_chunk_bytes=T, narrow_held_budget_bytes=1)
    assert tiny == _write(m, narrow_chunk_bytes=T, narrow_run_blocks=1)


def test_every_column_narrow_is_a_whole_file_column_major_layout():
    data = _write(_morsel(10 * R), narrow_chunk_bytes=1 << 30)
    ch = _chunks(data)
    cursor = 4
    for c in range(len(NAMES)):
        for rg in range(10):
            assert ch[rg][c][0] == cursor
            cursor = ch[rg][c][1]
    assert _infer_blocks(ch, range(len(NAMES))) == 1


def test_threshold_zero_is_the_grouped_layout_byte_for_byte():
    m = _morsel(10 * R, grow=True)
    assert (_write(m) == _write(m, narrow_chunk_bytes=0)
            == _write(m, narrow_chunk_bytes=0, narrow_run_blocks=3)
            == _write(m, narrow_chunk_bytes=0, narrow_held_budget_bytes=1))
    chunks = []
    with rp.open_parquet_writer(chunks.append, compression="none", narrow_held_budget_bytes=1) as w:
        for rg in range(10):
            w.write_row_group(m.slice(rg * R, R))
        assert w.peak_held_narrow_bytes == w.narrow_demoted_columns == w.narrow_budget_closes == 0
    assert b"".join(chunks) == _write(m)


# ── what the reader will infer ────────────────────────────────────────────────

def test_fetch_blocks_inferred_from_adjacency():
    n_rg = 10
    data = _write(_morsel(n_rg * R), narrow_chunk_bytes=T)
    ch = _chunks(data)
    names = _names(data)
    idx = {n: names.index(n) for n in names}
    # narrow-only projections: the run is one block
    assert _infer_blocks(ch, [idx["lo"]]) == 1
    assert _infer_blocks(ch, [idx["lo"], idx["flag"]]) == 1
    # any wide column: blocks of G, as the grouped layout
    assert _infer_blocks(ch, [idx["a"]]) == len(_blocks(n_rg, G))
    assert _infer_blocks(ch, [idx["a"], idx["lo"]]) == len(_blocks(n_rg, G))
    assert _infer_blocks(ch, range(len(names))) == len(_blocks(n_rg, G))
    # a run every 2 blocks: the narrow projection sees one block per run
    data2 = _write(_morsel(n_rg * R), narrow_chunk_bytes=T, narrow_run_blocks=2)
    assert _infer_blocks(_chunks(data2), [idx["lo"]]) == 2   # rg 0-7, rg 8-9


# ── values ────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("compression", ["none", "zstd"])
@pytest.mark.parametrize("run_blocks", [0, 2])
def test_values_match_the_default_layout(compression, run_blocks):
    m = _morsel(10 * R)
    base = _write(m, compression=compression)
    pc = _write(m, compression=compression, narrow_chunk_bytes=T, narrow_run_blocks=run_blocks)
    assert pq.read_table(io.BytesIO(pc)).equals(pq.read_table(io.BytesIO(base)))
    for cols in (["lo"], ["lo", "flag"], ["a", "lo"], ["s"]):
        assert pq.read_table(io.BytesIO(pc), columns=cols).equals(pq.read_table(io.BytesIO(base), columns=cols))
    with rp.read_parquet(pc) as it:
        got = Morsel.combine(list(it))
    assert got.to_arrow().equals(m.to_arrow())
    with rp.read_parquet(pc, columns=["lo", "flag"]) as it:
        got = Morsel.combine(list(it))
    assert got.to_arrow().equals(m.to_arrow().select(["lo", "flag"]))
    # footer fields stay honest: the row group's first byte and the sum of its chunks
    stats = read_rowgroup_stats(pc)
    for rg, row in enumerate(_chunks(pc)):
        assert stats[rg]["file_offset"] == min(s for s, _ in row)
        assert stats[rg]["total_compressed_size"] == sum(e - s for s, e in row)


# ── streaming == one-shot, and the memory it holds ────────────────────────────

@pytest.mark.parametrize("run_blocks", [0, 2])
def test_streaming_writer_matches_one_shot_and_reports_held_bytes(run_blocks):
    n_rg = 10
    whole = _morsel(n_rg * R)
    one_shot = _write(whole, narrow_chunk_bytes=T, narrow_run_blocks=run_blocks)
    ch = _chunks(one_shot)
    narrow_idx = [c for c, n in enumerate(_names(one_shot)) if n in NARROW]
    chunks, held = [], []
    with rp.open_parquet_writer(chunks.append, compression="none", narrow_chunk_bytes=T,
                                narrow_run_blocks=run_blocks) as w:
        for rg in range(n_rg):
            w.write_row_group(whole.slice(rg * R, R))
            held.append(w.held_narrow_bytes)
        assert w.held_narrow_bytes == held[-1]
    assert w.held_narrow_bytes == 0, "the last run is placed on close"
    streamed = b"".join(chunks)
    assert streamed == one_shot
    # After k complete blocks the writer holds exactly the narrow chunks of the
    # row groups not yet placed in a run.
    def expect(k_rgs):
        placed = (k_rgs // G) // run_blocks * run_blocks * G if run_blocks else 0
        done = (k_rgs // G) * G   # only whole blocks have been classified and held
        return sum(ch[rg][c][1] - ch[rg][c][0] for rg in range(placed, done) for c in narrow_idx)
    for k in range(1, n_rg + 1):
        assert held[k - 1] == expect(k), (k, held[k - 1], expect(k))
    assert max(held) > 0


def test_with_bounds_and_stream_accept_the_option():
    m = _morsel(6 * R)
    data, bounds = rp.write_parquet_with_bounds(m, compression="none", max_rows_per_row_group=R,
                                                narrow_chunk_bytes=T)
    assert bounds[0] == (0, 6 * R - 1)
    assert _classes(data)["lo"] and not _classes(data)["a"]
    chunks = []
    n = rp.write_parquet_stream((m.slice(lo, R) for lo in range(0, 6 * R, R)), chunks.append,
                                compression="none", narrow_chunk_bytes=T)
    assert n == 6
    assert b"".join(chunks) == data
    assert pq.read_table(io.BytesIO(b"".join(chunks))).equals(m.to_arrow())


def test_the_footer_records_the_threshold_the_file_was_written_with():
    """`rugo.narrow_chunk_bytes` in the footer's key-value metadata whenever the
    layout was on — even when no column turned out narrow — and absent when it
    was off, so compaction can tell the two apart (§4.7.3)."""
    m = _morsel(6 * R)
    def stamp(data):
        kv = pq.ParquetFile(io.BytesIO(data)).metadata.metadata or {}
        v = kv.get(b"rugo.narrow_chunk_bytes")
        return None if v is None else v.decode()
    assert stamp(_write(m)) is None
    assert stamp(_write(m, narrow_chunk_bytes=T)) == str(T)
    assert stamp(_write(m, narrow_chunk_bytes=1)) == "1"   # nothing narrow, still stamped
    chunks = []
    with rp.open_parquet_writer(chunks.append, compression="none", narrow_chunk_bytes=T) as w:
        w.write_row_group(m)
    assert stamp(b"".join(chunks)) == str(T)


def test_options_are_validated():
    m = _morsel(R)
    with pytest.raises(ValueError, match="narrow_chunk_bytes"):
        rp.write_parquet(m, narrow_chunk_bytes=-1)
    with pytest.raises(ValueError, match="narrow_run_blocks"):
        rp.write_parquet(m, narrow_chunk_bytes=T, narrow_run_blocks=-1)
    with pytest.raises(ValueError, match="narrow_chunk_bytes"):
        rp.open_parquet_writer(lambda b: None, narrow_chunk_bytes=-1)
    with pytest.raises(ValueError, match="narrow_held_budget_bytes"):
        rp.write_parquet(m, narrow_chunk_bytes=T, narrow_held_budget_bytes=-1)


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
