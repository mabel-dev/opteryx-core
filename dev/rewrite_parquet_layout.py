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
"""

import argparse
import os
import sys
from multiprocessing import Pool

REPO = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
sys.path.insert(0, REPO)


def one(job):
    src, dst, rows, block, dictionary, compression = job
    import rugo.parquet as rp
    from draken.morsels.morsel import Morsel

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
    return os.path.basename(dst), n, len(data)


def _numeric_order(paths):
    import re

    def key(path):
        found = re.findall(r"(\d+)\.parquet$", path)
        return (int(found[0]) if found else -1, path)

    return sorted(paths, key=key)


def repack(src_dir, dst_dir, rows, block, dictionary, file_bytes, compression="zstd"):
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
    for path in sources:
        with rp.read_parquet(path) as it:
            for morsel in it:
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
    args = ap.parse_args()
    os.makedirs(args.dst, exist_ok=True)
    if args.file_bytes:
        if args.limit:
            ap.error("--limit does not apply to --file-bytes (a repack reads every source)")
        if os.listdir(args.dst):
            ap.error(f"{args.dst} is not empty - a repack never writes over a corpus")
        repack(args.src, args.dst, args.rows, args.block, not args.no_dictionary,
               args.file_bytes, args.compression)
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
                         not args.no_dictionary, args.compression))
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
