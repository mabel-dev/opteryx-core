#!/bin/bash
# Split ClickBench's hits.json.gz (one gzip NDJSON file, ~100M rows) into
# 1,000,000-row zstd shards: <out_dir>/hits_000.jsonl.zst .. hits_099.jsonl.zst.
#
# WHY: a gzip stream decompresses in order, so a single file is scanned by ONE
# worker (measured locally: 152s for COUNT(*) at ~1.7 cores). The file is the
# unit of parallel scan work; 100 shards let the scan use the machine.
#
# zstd level 1 (JSONBench's posture), -T0 = all cores for compression. The
# gzip decompress is the serial floor of this script.
#
# Written into <out_dir>.partial and renamed only on success, so an
# interrupted run never leaves a short tree that looks complete. Lines are
# counted in-stream (tee, no second decompress) and the shard count must equal
# ceil(lines / 1M); JSONEachRow escapes newlines, so lines == rows.
#
# Usage: split_hits_json.sh <hits.json.gz> <out_dir>
# Needs GNU split (--filter) and zstd. On macOS: brew install coreutils (gsplit).
set -euo pipefail

if [ $# -ne 2 ]; then
    echo "usage: $0 <hits.json.gz> <out_dir>" >&2
    exit 2
fi
src="$1"
out="$2"
rows_per_file=1000000

if command -v gsplit >/dev/null; then
    split_cmd=gsplit
else
    split_cmd=split
fi
"$split_cmd" --version 2>/dev/null | grep -q GNU || {
    echo "$0: needs GNU split (--filter); on macOS: brew install coreutils" >&2
    exit 1
}
command -v zstd >/dev/null || { echo "$0: needs zstd" >&2; exit 1; }
[ -f "$src" ] || { echo "$0: $src not found" >&2; exit 1; }
[ ! -e "$out" ] || { echo "$0: $out already exists — remove it first" >&2; exit 1; }

rm -rf "$out.partial"
mkdir -p "$out.partial"

gzip -dc "$src" \
    | tee >(wc -l | tr -d ' ' > "$out.partial.lines") \
    | "$split_cmd" -l "$rows_per_file" -d -a 3 \
        --additional-suffix=.jsonl.zst \
        --filter='zstd -1 -T0 -q -o "$FILE"' \
        - "$out.partial/hits_"
# tee's >(...) runs asynchronously; wait for the count to land.
while [ ! -s "$out.partial.lines" ]; do sleep 1; done
lines=$(cat "$out.partial.lines")
rm -f "$out.partial.lines"

shards=$(ls "$out.partial" | wc -l | tr -d ' ')
expected=$(( (lines + rows_per_file - 1) / rows_per_file ))
echo "$lines rows -> $shards shards"
if [ "$lines" -eq 0 ] || [ "$shards" -ne "$expected" ]; then
    echo "$0: expected $expected shards for $lines rows, got $shards" >&2
    exit 1
fi

mv "$out.partial" "$out"
