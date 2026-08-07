#!/bin/sh
# Convert a corpus of Parquet to .skene and record size and timing per file.
#
# Emits the converter's TSV line for each input:
#   TSV <source> <rows> <source_bytes> <skene_bytes> <write_ms> <read_ms> <pq_read_ms>
#
# Output is DELETED after each file is measured. The full ClickBench set is ~14 GB
# of skene, and the numbers come from the converter's own read-back rather than
# from the files persisting, so keeping them buys nothing but a full disk.
#
# usage: skenify.sh <out.tsv> <parquet> [parquet ...]
set -e

out="$1"
shift
: > "$out"

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT INT TERM

for src in "$@"; do
    SKENE_ZSTD=1 ./build/convert_parquet "$src" "$work/part" 100000 2>&1 \
        | grep '^TSV' >> "$out" || {
            echo "FAILED $src" >&2
            exit 1
        }
    rm -f "$work"/part*.skene
done

echo "wrote $(grep -c '^TSV' "$out") rows to $out" >&2
