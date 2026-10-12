"""Rewrite a directory of parquet files with rugo's writer at a chosen layout.

    python dev/rewrite_parquet_layout.py SRC_DIR DST_DIR --rows 65536 --block 4 \
        [--procs 4] [--no-dictionary] [--compression zstd|none] [--limit N]

Every file is read whole (rugo), then written with write_parquet(
max_rows_per_row_group=rows, row_groups_per_block=block) and the writer's
other defaults (zstd, bloom filters, dictionary, no page splitting);
--no-dictionary forces PLAIN everywhere, --limit writes only the first N files
in numeric order. Row counts are verified per file. Dev tooling only.

    python dev/rewrite_parquet_layout.py SRC_DIR DST_DIR --rows 65536 --block 4 \
        --file-bytes 4294967296

--file-bytes N REPACKS instead: every source file, in numeric order
(hits_0, hits_1, ... hits_99 - the dataset's own order), streams through one
writer into files closed at the first block boundary at or past N bytes (the
engine's own write target and the skene mirror's are 4 GiB). File boundaries
are NOT preserved; the output is part-000.parquet, part-001.parquet, ... and
the total row count is verified against the source.

--retype-clickbench gives ClickBench's untyped time columns their logical
types, as ClickBench/opteryx-parquet-rewritten/convert.py does: EventTime,
ClientEventTime and LocalEventTime (integer epoch seconds) become TIMESTAMP and
EventDate (integer day count) becomes DATE. Each source file is read through
Opteryx (READ_PARQUET with the casts), every other column untouched in source
order. Each source file's rows are written sorted by the primary key
(CounterID, EventDate, UserID, EventTime, WatchID). 92 of the 100 upstream
hits_N files are already in that order; hits_57/68/80/90/92/94/96/98 are each
two sorted runs, which the sort merges. Verified per source file: its rows come
out strictly ascending by the key, with the source's row count and WatchIDs - a
mismatch is a hard failure. Works in both modes.
"""

import argparse
import os
import sys
from multiprocessing import Pool

REPO = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
sys.path.insert(0, REPO)


# --retype-clickbench: column -> the cast that gives it its logical type.
CLICKBENCH_RETYPE = {
    "EventTime": "TIMESTAMP[s]",
    "ClientEventTime": "TIMESTAMP[s]",
    "LocalEventTime": "TIMESTAMP[s]",
    "EventDate": "DATE",
}
CLICKBENCH_PRIMARY_KEY = "CounterID, EventDate, UserID, EventTime, WatchID"


def _read_retyped(src):
    import opteryx
    import rugo.parquet as rp

    names = [column.name for column in rp.read_metadata(src).schema_columns]
    missing = sorted(set(CLICKBENCH_RETYPE) - set(names))
    if missing:
        raise RuntimeError(f"{src}: no column(s) {missing} to retype")
    projection = ", ".join(
        f"{name}::{CLICKBENCH_RETYPE[name]} AS {name}" if name in CLICKBENCH_RETYPE else name
        for name in names)
    return list(opteryx.session().execute_to_morsels(
        f"SELECT {projection} FROM READ_PARQUET('{src}') ORDER BY {CLICKBENCH_PRIMARY_KEY}"))


def _key_rows(paths):
    import rugo.parquet as rp

    columns = [c.strip() for c in CLICKBENCH_PRIMARY_KEY.split(",")]
    for path in paths:
        with rp.read_parquet(path, columns=columns) as it:
            for morsel in it:
                yield from zip(*(morsel.column(c.encode()).to_pylist() for c in columns))


def _verify_sorted(sources, outputs):
    """The outputs are the sources' rows, source by source, each source's rows
    strictly ascending by the primary key (WatchID is the key's last column)."""
    import itertools

    import rugo.parquet as rp

    written = _key_rows(outputs)
    for src in sources:
        n = rp.read_metadata(src).num_rows
        segment = list(itertools.islice(written, n))
        if len(segment) != n:
            raise RuntimeError(f"{src}: {n:,} rows in the source, {len(segment):,} written")
        if any(segment[i] >= segment[i + 1] for i in range(n - 1)):
            raise RuntimeError(f"{src}: written rows are not strictly ascending by the key")
        if sorted(r[-1] for r in segment) != sorted(r[-1] for r in _key_rows([src])):
            raise RuntimeError(f"{src}: written WatchIDs differ from the source's")
    if next(written, None) is not None:
        raise RuntimeError("more rows written than the sources hold")


def one(job):
    src, dst, rows, block, dictionary, compression, retype = job
    import rugo.parquet as rp
    from draken.morsels.morsel import Morsel

    if retype:
        morsels = _read_retyped(src)
    else:
        with rp.read_parquet(src) as it:
            morsels = list(it)
    m = Morsel.combine(morsels) if len(morsels) > 1 else morsels[0]
    data = rp.write_parquet(m, compression=compression, max_rows_per_row_group=rows,
                            row_groups_per_block=block, dictionary=dictionary)
    with open(dst + ".tmp", "wb") as f:
        f.write(data)
    os.replace(dst + ".tmp", dst)
    n = rp.read_metadata(dst).num_rows
    if n != m.num_rows:
        raise RuntimeError(f"{dst}: wrote {n} rows, expected {m.num_rows}")
    if retype:
        _verify_sorted([src], [dst])
    return os.path.basename(dst), n, len(data)


def _numeric_order(paths):
    import re

    def key(path):
        found = re.findall(r"(\d+)\.parquet$", path)
        return (int(found[0]) if found else -1, path)

    return sorted(paths, key=key)


def repack(src_dir, dst_dir, rows, block, dictionary, file_bytes, compression="zstd",
           retype=False):
    """Stream every source file, in numeric order, into files closed at the first
    BLOCK boundary at or past `file_bytes` - never mid-block, so every file but
    the last holds whole column-major blocks."""
    import rugo.parquet as rp
    from draken.morsels.morsel import Morsel

    sources = _numeric_order(
        os.path.join(src_dir, f) for f in os.listdir(src_dir)
        if f.endswith(".parquet") and "manifest" not in f)
    expected = sum(rp.read_metadata(path).num_rows for path in sources)
    print(f"{len(sources)} source files, {expected:,} rows -> {dst_dir} "
          f"({rows}-row row groups, blocks of {block}, files closed at {file_bytes:,} bytes)",
          flush=True)

    state = {"fh": None, "writer": None, "bytes": 0, "rgs": 0, "index": 0, "path": None}
    written = []

    def sink(chunk):
        state["fh"].write(chunk)
        state["bytes"] += len(chunk)

    def open_file():
        state["path"] = os.path.join(dst_dir, f"part-{state['index']:03d}.parquet")
        state["fh"] = open(state["path"] + ".tmp", "wb")
        state["bytes"] = 0
        state["rgs"] = 0
        state["writer"] = rp.open_parquet_writer(sink, compression=compression, dictionary=dictionary,
                                                 row_groups_per_block=block)

    def close_file():
        state["writer"].close()
        state["fh"].close()
        os.replace(state["path"] + ".tmp", state["path"])
        n = rp.read_metadata(state["path"]).num_rows
        written.append(n)
        print(f"  {os.path.basename(state['path'])}: {n:,} rows, {state['rgs']} rg, "
              f"{os.path.getsize(state['path']) / 2**30:.2f} GiB", flush=True)
        state["writer"] = None
        state["index"] += 1

    def emit(row_group):
        if state["writer"] is None:
            open_file()
        state["writer"].write_row_group(row_group)
        state["rgs"] += 1
        if state["rgs"] % block == 0 and state["bytes"] >= file_bytes:
            close_file()

    pending, pending_rows = [], 0
    def source_morsels(path):
        if retype:
            yield from _read_retyped(path)
            return
        with rp.read_parquet(path) as it:
            yield from it

    for path in sources:
        for morsel in source_morsels(path):
            if morsel.num_rows == 0:
                continue
            pending.append(morsel)
            pending_rows += morsel.num_rows
            while pending_rows >= rows:
                merged = pending[0] if len(pending) == 1 else Morsel.combine(pending)
                emit(merged.slice(0, rows))
                remainder = merged.num_rows - rows
                pending = [merged.slice(rows, remainder)] if remainder > 0 else []
                pending_rows = remainder
    # the tail row group is SHORT - padding would change the row count
    if pending_rows > 0:
        emit(pending[0] if len(pending) == 1 else Morsel.combine(pending))
    if state["writer"] is not None:
        close_file()
    if sum(written) != expected:
        raise RuntimeError(f"wrote {sum(written):,} rows, expected {expected:,}")
    if retype:
        _verify_sorted(sources, [os.path.join(dst_dir, f"part-{i:03d}.parquet")
                                for i in range(len(written))])
    print(f"DONE files={len(written)} rows={sum(written):,}", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("src")
    ap.add_argument("dst")
    ap.add_argument("--rows", type=int, default=65536)
    ap.add_argument("--block", type=int, default=4)
    ap.add_argument("--procs", type=int, default=4)
    ap.add_argument("--no-dictionary", action="store_true")
    ap.add_argument("--compression", choices=("zstd", "none"), default="zstd",
                    help="page codec (rugo writes zstd or none; default zstd)")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--file-bytes", type=int, default=0)
    ap.add_argument("--retype-clickbench", action="store_true",
                    help="cast ClickBench's time columns to TIMESTAMP/DATE, rows sorted by its primary key")
    args = ap.parse_args()
    os.makedirs(args.dst, exist_ok=True)
    if args.file_bytes:
        if args.limit:
            ap.error("--limit does not apply to --file-bytes (a repack reads every source)")
        if os.listdir(args.dst):
            ap.error(f"{args.dst} is not empty - a repack never writes over a corpus")
        repack(args.src, args.dst, args.rows, args.block, not args.no_dictionary,
               args.file_bytes, args.compression, args.retype_clickbench)
        return
    jobs = []
    for root, _dirs, files in os.walk(args.src):
        rel = os.path.relpath(root, args.src)
        for f in sorted(files):
            if not f.endswith(".parquet") or "manifest" in f:
                continue
            out_dir = os.path.join(args.dst, rel) if rel != "." else args.dst
            os.makedirs(out_dir, exist_ok=True)
            dst = os.path.join(out_dir, f)
            if os.path.exists(dst):
                continue
            jobs.append((os.path.join(root, f), dst, args.rows, args.block,
                         not args.no_dictionary, args.compression, args.retype_clickbench))
    if args.limit:
        import re

        def _num(job):
            m = re.findall(r"(\d+)\.parquet$", job[0])
            return (int(m[0]) if m else 0, job[0])

        jobs = sorted(jobs, key=_num)[: args.limit]
    print(f"{len(jobs)} files to write", flush=True)
    total_rows = total_bytes = 0
    with Pool(args.procs) as pool:
        for i, (name, n, nb) in enumerate(pool.imap_unordered(one, jobs)):
            total_rows += n
            total_bytes += nb
            if (i + 1) % 10 == 0 or i + 1 == len(jobs):
                print(f"{i + 1}/{len(jobs)} {name} rows={n} bytes={nb}", flush=True)
    print(f"DONE rows={total_rows} bytes={total_bytes}", flush=True)


if __name__ == "__main__":
    main()
