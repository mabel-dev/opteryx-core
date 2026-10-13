"""
Cut the tweet fixtures down to their first 10,000 rows, rewritten by pyarrow.

These files are the "parquet written by another writer" oracle: pyarrow writes them,
rugo/opteryx must read them. Each rewrite keeps the file's original codec and
parquet format version and stays one row group. The misnamed copy is the parquet
fixture's bytes under a .jsonl.parquet name, so it is copied, not rewritten.

Idempotent: a file already at 10,000 rows is rewritten to the same rows.

    python dev/shrink_tweets_fixtures.py
"""

import shutil
from pathlib import Path

import pyarrow.parquet as pq

ROWS = 10_000
ROOT = Path(__file__).resolve().parent.parent / "testdata"

FIXTURES = [
    "tweets/tweets.parquet",
    "flat/formats/parquet/tweets.parquet",
    "flat/formats/parquet_lz4/tweets.parquet",
    "flat/formats/parquet_snappy/tweets.parquet",
    "flat/different/170cd0bcd4b9a1f2-155d5225af-52a5.jsonl.parquet",
    "parquet_tests/opteryx_store_test_tweets_data_data-1771432047080.parquet",
]
MISNAMED = ("flat/formats/parquet/tweets.parquet", "flat/formats/misnamed_parquet/tweets.jsonl.parquet")


def shrink(path: Path) -> None:
    source = pq.ParquetFile(path)
    meta = source.metadata
    codecs = {meta.row_group(0).column(i).compression for i in range(meta.num_columns)}
    if len(codecs) != 1:
        raise ValueError(f"{path}: mixed codecs {codecs}, expected one")
    codec = codecs.pop()
    table = source.read().slice(0, ROWS)
    tmp = path.with_suffix(".tmp")
    pq.write_table(table, tmp, compression=codec, version=meta.format_version, row_group_size=ROWS)
    tmp.replace(path)
    print(f"{path.relative_to(ROOT)}: {meta.num_rows} -> {table.num_rows} rows, {codec}, v{meta.format_version}")


def main():
    for rel in FIXTURES:
        shrink(ROOT / rel)
    src, dst = (ROOT / p for p in MISNAMED)
    shutil.copyfile(src, dst)
    print(f"{MISNAMED[1]}: copy of {MISNAMED[0]}")


if __name__ == "__main__":
    main()
