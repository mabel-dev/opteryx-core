# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Compressed JSONL / CSV input: gzip, zstd and lz4 are decompressed transparently
(rugo/src/compression/stream_decompress.hpp); anything else fails loud.

The contract under test:
  - a compressed file reads to EXACTLY the rows of its uncompressed twin — whole
    scans, COUNT(*), projection and predicate pushdown, globs mixing plain and
    compressed files, directory (dataset) scans, and many-chunk streaming;
  - the codec is detected by magic bytes, so the extension does not have to say it;
  - a truncated or corrupt stream, an unsupported codec (bzip2 / xz / zip), and an
    extension the bytes contradict each raise, naming the file — compressed bytes
    are never parsed as text. The regression this closes: READ_JSONL over a .json.gz
    with ignore_errors returned COUNT(*) = 1 instead of every row.
"""

import bz2
import gzip
import json
import lzma
import os
import shutil
import subprocess
import sys
from compression import zstd

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.exceptions import DataError
from opteryx.exceptions import DatasetReadError

ROWS = 3000


def _jsonl_text(start=0, rows=ROWS):
    lines = []
    for i in range(start, start + rows):
        record = {
            "id": i,
            "name": f"name-{i % 97}",
            "score": (i * 7) % 1000 / 10,
            "flag": i % 3 == 0,
            "tags": [f"t{i % 5}", f"u{i % 11}"],
            "kind": {"collection": f"app.bsky.{('post', 'like', 'repost')[i % 3]}"},
        }
        if i % 13 == 0:
            record["name"] = None
        lines.append(json.dumps(record))
    return "\n".join(lines) + "\n"


def _csv_text(rows=ROWS):
    lines = ["id,name,score"]
    for i in range(rows):
        lines.append(f"{i},name-{i % 97},{(i * 7) % 1000 / 10}")
    return "\n".join(lines) + "\n"


# --- an lz4 FRAME writer for tests: stored (uncompressed) blocks only, so it needs
# no lz4 library. The lz4 CLI, when present, adds a real compressed-block case. ---


def _xxh32(data: bytes, seed: int = 0) -> int:
    P1, P2, P3, P4, P5 = 2654435761, 2246822519, 3266489917, 668265263, 374761393
    M = 0xFFFFFFFF

    def rotl(x, r):
        return ((x << r) | (x >> (32 - r))) & M

    def rnd(acc, lane):
        return (rotl((acc + lane * P2) & M, 13) * P1) & M

    n = len(data)
    i = 0
    if n >= 16:
        v = [(seed + P1 + P2) & M, (seed + P2) & M, seed, (seed - P1) & M]
        while i + 16 <= n:
            for k in range(4):
                v[k] = rnd(v[k], int.from_bytes(data[i + 4 * k : i + 4 * k + 4], "little"))
            i += 16
        h = (rotl(v[0], 1) + rotl(v[1], 7) + rotl(v[2], 12) + rotl(v[3], 18)) & M
    else:
        h = (seed + P5) & M
    h = (h + n) & M
    while i + 4 <= n:
        h = (rotl((h + int.from_bytes(data[i : i + 4], "little") * P3) & M, 17) * P4) & M
        i += 4
    while i < n:
        h = (rotl((h + data[i] * P5) & M, 11) * P1) & M
        i += 1
    h ^= h >> 15
    h = (h * P2) & M
    h ^= h >> 13
    h = (h * P3) & M
    h ^= h >> 16
    return h


def _lz4_frame_stored(data: bytes) -> bytes:
    flg = (1 << 6) | (1 << 5) | (1 << 2)  # version 1, independent blocks, content checksum
    bd = 4 << 4  # 64KB blocks
    out = bytearray((0x184D2204).to_bytes(4, "little"))
    out += bytes([flg, bd, (_xxh32(bytes([flg, bd])) >> 8) & 0xFF])
    for i in range(0, len(data), 65536):
        block = data[i : i + 65536]
        out += (len(block) | 0x80000000).to_bytes(4, "little") + block
    out += (0).to_bytes(4, "little") + _xxh32(data).to_bytes(4, "little")
    return bytes(out)


def _compress(codec: str, data: bytes) -> bytes:
    if codec == "gz":
        return gzip.compress(data)
    if codec == "zst":
        return zstd.compress(data)
    if codec == "lz4":
        return _lz4_frame_stored(data)
    raise AssertionError(codec)


CODECS = ["gz", "zst", "lz4"]


def _rows(sql):
    session = opteryx.session()
    rows = []
    for morsel in session.execute_to_morsels(sql):
        columns = [morsel.column(name).to_pylist() for name in morsel.column_names]
        rows.extend(zip(*columns))
    return sorted(rows, key=repr)


def _write(path, data: bytes):
    path.write_bytes(data)
    return path


QUERIES = [
    "SELECT * FROM {src}",
    "SELECT COUNT(*) FROM {src}",
    "SELECT id, name FROM {src} WHERE id >= 1500 AND name = 'name-3'",
    "SELECT kind ->> 'collection' AS c, COUNT(*) FROM {src} GROUP BY kind ->> 'collection'",
    "SELECT MAX(score), MIN(id) FROM {src} WHERE flag = true",
]


@pytest.mark.parametrize("codec", CODECS)
def test_compressed_jsonl_reads_identically_to_plain(tmp_path, codec):
    raw = _jsonl_text().encode()
    plain = _write(tmp_path / "data.jsonl", raw)
    packed = _write(tmp_path / f"data.jsonl.{codec}", _compress(codec, raw))
    for query in QUERIES:
        expected = _rows(query.format(src=f"READ_JSONL('{plain}')"))
        assert expected, query
        assert _rows(query.format(src=f"READ_JSONL('{packed}')")) == expected, query


@pytest.mark.parametrize("codec", CODECS)
def test_many_chunk_streaming_loses_and_invents_nothing(tmp_path, monkeypatch, codec):
    # Tiny chunks: the native Source cuts each decompressed stream into many
    # newline-aligned chunks, one producer at a time per file, across several files.
    from opteryx.connectors import jsonl_io

    monkeypatch.setattr(jsonl_io, "DEFAULT_CHUNK_SIZE", 4096)
    for part in range(4):
        raw = _jsonl_text(start=part * ROWS).encode()
        _write(tmp_path / f"p{part}.jsonl.{codec}", _compress(codec, raw))
    src = f"READ_JSONL('{tmp_path}/*.jsonl.{codec}')"
    assert _rows(f"SELECT COUNT(*) FROM {src}") == [(4 * ROWS,)]
    ids = [row[0] for row in _rows(f"SELECT id FROM {src}")]
    assert sorted(ids) == list(range(4 * ROWS))
    assert _rows(f"SELECT id FROM {src} WHERE id < 3") == [(0,), (1,), (2,)]


def test_glob_mixing_plain_and_compressed_files(tmp_path):
    texts = [_jsonl_text(start=i * ROWS) for i in range(4)]
    _write(tmp_path / "a.jsonl", texts[0].encode())
    _write(tmp_path / "b.jsonl.gz", gzip.compress(texts[1].encode()))
    _write(tmp_path / "c.jsonl.zst", zstd.compress(texts[2].encode()))
    _write(tmp_path / "d.jsonl.lz4", _lz4_frame_stored(texts[3].encode()))
    src = f"READ_JSONL('{tmp_path}/*')"
    assert _rows(f"SELECT COUNT(*) FROM {src}") == [(4 * ROWS,)]
    assert sorted(r[0] for r in _rows(f"SELECT id FROM {src}")) == list(range(4 * ROWS))


def test_multi_member_gzip(tmp_path):
    first, second = _jsonl_text(0, 1000).encode(), _jsonl_text(1000, 1000).encode()
    path = _write(tmp_path / "mm.jsonl.gz", gzip.compress(first) + gzip.compress(second))
    assert _rows(f"SELECT COUNT(*) FROM READ_JSONL('{path}')") == [(2000,)]
    assert sorted(r[0] for r in _rows(f"SELECT id FROM READ_JSONL('{path}')")) == list(range(2000))


def test_codec_detected_by_magic_not_extension(tmp_path):
    raw = _jsonl_text().encode()
    path = _write(tmp_path / "no_hint.jsonl", zstd.compress(raw))
    assert _rows(f"SELECT COUNT(*) FROM READ_JSONL('{path}')") == [(ROWS,)]


def test_previously_silent_json_gz_counts_every_row(tmp_path):
    # The JSONBench shape: a .json.gz read with ignore_errors used to parse the gzip
    # bytes as one malformed record (COUNT(*) = 1) and fail projections with
    # ColumnNotFoundError.
    path = _write(tmp_path / "file_0001.json.gz", gzip.compress(_jsonl_text().encode()))
    assert _rows(f"SELECT COUNT(*) FROM READ_JSONL('{path}', ignore_errors => true)") == [(ROWS,)]
    assert len(_rows(f"SELECT name FROM READ_JSONL('{path}')")) == ROWS


@pytest.mark.skipif(shutil.which("lz4") is None, reason="lz4 CLI not installed")
@pytest.mark.parametrize("flags", [[], ["-BD", "-B4"], ["--no-frame-crc", "-BX"]])
def test_lz4_cli_compressed_blocks(tmp_path, flags):
    raw = _jsonl_text().encode()
    plain = _write(tmp_path / "data.jsonl", raw)
    packed = tmp_path / "data.jsonl.lz4"
    subprocess.run(["lz4", "-q", "-f", *flags, str(plain), str(packed)], check=True)
    assert _rows(f"SELECT * FROM READ_JSONL('{packed}')") == _rows(
        f"SELECT * FROM READ_JSONL('{plain}')"
    )


# --- failures: loud, naming the file ---


@pytest.mark.parametrize("codec", CODECS)
def test_truncated_file_fails_naming_it(tmp_path, codec):
    packed = _compress(codec, _jsonl_text().encode())
    path = _write(tmp_path / f"cut.jsonl.{codec}", packed[: len(packed) * 2 // 3])
    with pytest.raises(DatasetReadError, match=r"cut\.jsonl\.\w+.*truncated"):
        _rows(f"SELECT COUNT(*) FROM READ_JSONL('{path}')")


@pytest.mark.parametrize("codec", CODECS)
def test_truncated_later_file_fails_mid_execution(tmp_path, monkeypatch, codec):
    # The binder only reads the first file; the native Source must fail on the second.
    from opteryx.connectors import jsonl_io

    monkeypatch.setattr(jsonl_io, "DEFAULT_CHUNK_SIZE", 4096)
    _write(tmp_path / f"a.jsonl.{codec}", _compress(codec, _jsonl_text().encode()))
    packed = _compress(codec, _jsonl_text(start=ROWS).encode())
    _write(tmp_path / f"b.jsonl.{codec}", packed[: len(packed) // 2])
    with pytest.raises(DatasetReadError, match=r"b\.jsonl\.\w+.*truncated"):
        _rows(f"SELECT id FROM READ_JSONL('{tmp_path}/*.jsonl.{codec}')")


def test_corrupt_gzip_fails(tmp_path):
    packed = bytearray(gzip.compress(_jsonl_text().encode()))
    packed[len(packed) // 2] ^= 0x55
    path = _write(tmp_path / "bad.jsonl.gz", bytes(packed))
    with pytest.raises(DatasetReadError, match=r"bad\.jsonl\.gz.*gzip"):
        _rows(f"SELECT COUNT(*) FROM READ_JSONL('{path}')")


def test_corrupt_zstd_fails(tmp_path):
    packed = bytearray(zstd.compress(_jsonl_text().encode()))
    packed[len(packed) // 2] ^= 0x55
    path = _write(tmp_path / "bad.jsonl.zst", bytes(packed))
    with pytest.raises(DatasetReadError, match=r"bad\.jsonl\.zst.*zstd"):
        _rows(f"SELECT COUNT(*) FROM READ_JSONL('{path}')")


@pytest.mark.parametrize(
    "suffix, pack, codec",
    [("bz2", bz2.compress, "bzip2"), ("xz", lzma.compress, "xz")],
)
def test_unsupported_codec_fails_naming_file_and_codec(tmp_path, suffix, pack, codec):
    raw = _jsonl_text(rows=50).encode()
    path = _write(tmp_path / f"data.jsonl.{suffix}", pack(raw))
    with pytest.raises(
        DatasetReadError,
        match=rf"data\.jsonl\.{suffix}.* is {codec}-compressed, which is not supported",
    ):
        _rows(f"SELECT COUNT(*) FROM READ_JSONL('{path}')")
    # The same bytes under a plain name: magic bytes still refuse them.
    hidden = _write(tmp_path / "hidden.jsonl", pack(raw))
    with pytest.raises(DatasetReadError, match=rf"hidden\.jsonl.* is {codec}-compressed"):
        _rows(f"SELECT COUNT(*) FROM READ_JSONL('{hidden}', ignore_errors => true)")


def test_unsupported_codec_in_a_later_file_fails_mid_execution(tmp_path):
    _write(tmp_path / "a.jsonl", _jsonl_text(rows=50).encode())
    _write(tmp_path / "b.jsonl", bz2.compress(_jsonl_text(rows=50).encode()))
    with pytest.raises(DatasetReadError, match=r"b\.jsonl.* is bzip2-compressed"):
        _rows(f"SELECT id FROM READ_JSONL('{tmp_path}/*.jsonl')")


def test_extension_the_bytes_contradict_fails(tmp_path):
    path = _write(tmp_path / "plain.jsonl.gz", _jsonl_text(rows=50).encode())
    with pytest.raises(
        DatasetReadError,
        match=r"plain\.jsonl\.gz.*gzip file extension but does not start with gzip data",
    ):
        _rows(f"SELECT COUNT(*) FROM READ_JSONL('{path}')")


def test_empty_compressed_stream_is_an_empty_relation(tmp_path):
    _write(tmp_path / "a.jsonl", _jsonl_text(rows=10).encode())
    _write(tmp_path / "b.jsonl.gz", gzip.compress(b""))
    _write(tmp_path / "c.jsonl.zst", zstd.compress(b""))
    assert _rows(f"SELECT COUNT(*) FROM READ_JSONL('{tmp_path}/*')") == [(10,)]


# --- dataset (directory) scans ---


def test_directory_dataset_of_compressed_jsonl(tmp_path):
    texts = [_jsonl_text(start=i * ROWS) for i in range(3)]
    _write(tmp_path / "part-0.jsonl.gz", gzip.compress(texts[0].encode()))
    _write(tmp_path / "part-1.jsonl.zst", zstd.compress(texts[1].encode()))
    _write(tmp_path / "part-2.jsonl", texts[2].encode())
    assert _rows(f"SELECT COUNT(*) FROM '{tmp_path}'") == [(3 * ROWS,)]
    assert _rows(f"SELECT id FROM '{tmp_path}' WHERE id < 2") == [(0,), (1,)]


def test_directory_dataset_with_unsupported_codec_fails(tmp_path):
    _write(tmp_path / "part-0.jsonl.bz2", bz2.compress(_jsonl_text(rows=50).encode()))
    with pytest.raises(DataError, match=r"part-0\.jsonl\.bz2.* is bzip2-compressed"):
        _rows(f"SELECT COUNT(*) FROM '{tmp_path}'")


# --- CSV ---


@pytest.mark.parametrize("codec", CODECS)
def test_compressed_csv_reads_identically_to_plain(tmp_path, codec):
    raw = _csv_text().encode()
    plain = _write(tmp_path / "data.csv", raw)
    packed = _write(tmp_path / f"data.csv.{codec}", _compress(codec, raw))
    for query in [
        "SELECT * FROM {src}",
        "SELECT COUNT(*) FROM {src}",
        "SELECT id FROM {src} WHERE score > 50.0 AND name = 'name-4'",
    ]:
        expected = _rows(query.format(src=f"READ_CSV('{plain}')"))
        assert expected, query
        assert _rows(query.format(src=f"READ_CSV('{packed}')")) == expected, query


def test_csv_failures_name_the_file(tmp_path):
    bad = _write(tmp_path / "data.csv.bz2", bz2.compress(_csv_text(rows=10).encode()))
    with pytest.raises(DatasetReadError, match=r"data\.csv\.bz2.* is bzip2-compressed"):
        _rows(f"SELECT COUNT(*) FROM READ_CSV('{bad}')")
    packed = gzip.compress(_csv_text().encode())
    cut = _write(tmp_path / "cut.csv.gz", packed[: len(packed) // 2])
    with pytest.raises(DatasetReadError, match=r"cut\.csv\.gz.*truncated"):
        _rows(f"SELECT COUNT(*) FROM READ_CSV('{cut}')")


# --- rugo standalone ---


@pytest.mark.parametrize("codec", CODECS)
def test_rugo_readers_decompress_paths_and_buffers(tmp_path, codec):
    from rugo.csv import read_csv
    from rugo.jsonl import read_jsonl

    raw = _jsonl_text(rows=200).encode()
    path = _write(tmp_path / f"d.jsonl.{codec}", _compress(codec, raw))
    for source in (str(path), path.read_bytes()):
        with read_jsonl(source, columns=["id"]) as reader:
            assert sum(m.num_rows for m in reader) == 200
    raw_csv = _csv_text(rows=200).encode()
    with read_csv(_compress(codec, raw_csv)) as reader:
        assert sum(m.num_rows for m in reader) == 200


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
