"""Rewrite a directory of parquet files with rugo's writer at a chosen layout.

    python dev/rewrite_parquet_layout.py SRC_DIR DST_DIR --rows 65536 --block 4 \
        [--procs 4] [--no-dictionary] [--limit N]

Every file is read whole (rugo), then written with write_parquet(
max_rows_per_row_group=rows, row_groups_per_block=block) and the writer's
other defaults (zstd, bloom filters, dictionary, no page splitting);
--no-dictionary forces PLAIN everywhere, --limit writes only the first N files
in numeric order. Row counts are verified per file. Dev tooling only.
"""

import argparse
import os
import sys
from multiprocessing import Pool

REPO = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
sys.path.insert(0, REPO)


def one(job):
    src, dst, rows, block, dictionary = job
    import rugo.parquet as rp
    from draken.morsels.morsel import Morsel

    with rp.read_parquet(src) as it:
        morsels = list(it)
    m = Morsel.combine(morsels) if len(morsels) > 1 else morsels[0]
    data = rp.write_parquet(m, max_rows_per_row_group=rows, row_groups_per_block=block,
                            dictionary=dictionary)
    with open(dst + ".tmp", "wb") as f:
        f.write(data)
    os.replace(dst + ".tmp", dst)
    n = rp.read_metadata(dst).num_rows
    if n != m.num_rows:
        raise RuntimeError(f"{dst}: wrote {n} rows, expected {m.num_rows}")
    return os.path.basename(dst), n, len(data)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("src")
    ap.add_argument("dst")
    ap.add_argument("--rows", type=int, default=65536)
    ap.add_argument("--block", type=int, default=4)
    ap.add_argument("--procs", type=int, default=4)
    ap.add_argument("--no-dictionary", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    os.makedirs(args.dst, exist_ok=True)
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
                         not args.no_dictionary))
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
