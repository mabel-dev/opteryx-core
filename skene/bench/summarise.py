#!/usr/bin/env python3
"""Aggregate skenify.sh TSV output into the size and throughput comparison.

Reads the converter's per-file lines and reports totals plus MB/s, because a
raw millisecond count says nothing without the volume it moved.

usage: summarise.py <label> <file.tsv> [<label> <file.tsv> ...]
"""
import sys

MB = 1024.0 * 1024.0


def load(path):
    rows = []
    with open(path) as handle:
        for line in handle:
            if not line.startswith("TSV\t"):
                continue
            _, src, n_rows, source, skene, write_ms, read_ms, pq_ms = \
                line.rstrip("\n").split("\t")
            rows.append({
                "src": src.rsplit("/", 1)[-1],
                "rows": int(n_rows),
                "source": int(source),
                "skene": int(skene),
                "write_ms": float(write_ms),
                "read_ms": float(read_ms),
                "pq_ms": float(pq_ms),
            })
    return rows


def report(label, rows):
    if not rows:
        print(f"{label}: no rows")
        return

    total = {k: sum(r[k] for r in rows) for k in
             ("rows", "source", "skene", "write_ms", "read_ms", "pq_ms")}

    # Throughput is quoted against the LOGICAL volume each side moved: skene's
    # own bytes for its read and write, the Parquet file's bytes for Parquet's
    # read. Quoting both against one number would flatter whichever format is
    # smaller.
    skene_mb = total["skene"] / MB
    source_mb = total["source"] / MB

    print(f"\n=== {label} ===")
    print(f"{len(rows)} files, {total['rows']:,} rows")
    print(f"  parquet source   {total['source']:>15,} bytes")
    print(f"  skene            {total['skene']:>15,} bytes"
          f"   {total['skene'] / total['source']:.2f}x")
    print(f"  skene write      {total['write_ms']:>10,.0f} ms"
          f"   {skene_mb / (total['write_ms'] / 1000.0):>7.0f} MB/s")
    print(f"  skene read       {total['read_ms']:>10,.0f} ms"
          f"   {skene_mb / (total['read_ms'] / 1000.0):>7.0f} MB/s")
    print(f"  parquet read     {total['pq_ms']:>10,.0f} ms"
          f"   {source_mb / (total['pq_ms'] / 1000.0):>7.0f} MB/s  (rugo, threaded)")
    print(f"  read speedup     {total['pq_ms'] / total['read_ms']:>10.2f}x"
          "   same rows, skene single-threaded")

    worst = max(rows, key=lambda r: r["skene"] / max(r["source"], 1))
    best = min(rows, key=lambda r: r["skene"] / max(r["source"], 1))
    print(f"  per-file range   {best['skene'] / best['source']:.2f}x ({best['src']})"
          f" .. {worst['skene'] / worst['source']:.2f}x ({worst['src']})")


def main():
    args = sys.argv[1:]
    if not args or len(args) % 2 != 0:
        print(__doc__, file=sys.stderr)
        return 1
    for i in range(0, len(args), 2):
        report(args[i], load(args[i + 1]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
