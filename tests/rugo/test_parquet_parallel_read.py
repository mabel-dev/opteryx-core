# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Multi-core Parquet decode: `rugo.parquet.read_parquet(..., threads=N)`.

The contract under test is that the parallel path is the serial path, faster: the same
Morsels, in the same (file) order, with the same errors, whatever the thread count — plus
the lifecycle guarantees a worker pool adds (an abandoned reader releases the file, two
readers iterated by one thread cannot deadlock, concurrent readers do not interfere).

Small files stay serial by design (the hand-off to the pool costs more than a megabyte of
decode), so every test lowers the size floor to 0 to drive the parallel path.
"""

import datetime
import decimal
import os
import sys
import threading

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from rugo import parquet
from rugo import rugo_native


@pytest.fixture(autouse=True)
def _always_parallel(monkeypatch):
    monkeypatch.setattr(parquet, "_PARALLEL_MIN_BYTES", 0)


def _table(n=20_000):
    ints = list(range(n))
    return pa.table(
        {
            "id": pa.array(ints, pa.int64()),
            "i32": pa.array([i % 1000 for i in ints], pa.int32()),
            "i8": pa.array([i % 100 for i in ints], pa.int8()),
            "u16": pa.array([i % 60000 for i in ints], pa.uint16()),
            "f64": pa.array([i * 0.5 for i in ints], pa.float64()),
            "f32": pa.array([i * 0.25 for i in ints], pa.float32()),
            "flag": pa.array([i % 3 == 0 for i in ints], pa.bool_()),
            # dictionary-encoded (few distinct) and plain (many distinct) strings
            "s_dict": pa.array([f"grp-{i % 7}" for i in ints], pa.string()),
            "s_plain": pa.array([f"value-{i}-{'x' * (i % 40)}" for i in ints], pa.string()),
            "raw": pa.array([f"b{i}".encode() for i in ints], pa.binary()),
            "nullable": pa.array([None if i % 5 == 0 else i for i in ints], pa.int64()),
            "nullable_s": pa.array([None if i % 4 == 0 else f"s{i}" for i in ints], pa.string()),
            "const": pa.array([42] * n, pa.int64()),
            "dec": pa.array([decimal.Decimal(i) / 100 for i in ints], pa.decimal128(12, 2)),
            "ts": pa.array(
                [datetime.datetime(2020, 1, 1) + datetime.timedelta(seconds=i) for i in ints],
                pa.timestamp("us"),
            ),
            "lst": pa.array([[i, i + 1] if i % 3 else [] for i in ints], pa.list_(pa.int64())),
        }
    )


@pytest.fixture(scope="module")
def wide_file(tmp_path_factory):
    path = str(tmp_path_factory.mktemp("parallel") / "wide.parquet")
    pq.write_table(_table(), path, row_group_size=1_500, compression="zstd")
    return path


def _read(source, threads, **kw):
    with parquet.read_parquet(source, threads=threads, **kw) as reader:
        return list(reader)


def _same(a, b):
    assert len(a) == len(b)
    for ma, mb in zip(a, b):
        assert list(ma.column_names) == list(mb.column_names)
        assert len(ma) == len(mb)
        for name in ma.column_names:
            assert ma.column(name).to_pylist() == mb.column(name).to_pylist(), name


@pytest.mark.parametrize("threads", [2, 4, 64])
def test_parallel_equals_serial_every_column_type(wide_file, threads):
    _same(_read(wide_file, 1), _read(wide_file, threads))


def test_parallel_matches_pyarrow(wide_file):
    # `lst` has no flat oracle form here and rugo returns timestamps as UTC-aware; both are
    # covered by the serial-equality tests.
    truth = pq.read_table(wide_file).drop(["lst", "ts"]).to_pydict()
    morsels = _read(wide_file, 8)
    for name, expected in truth.items():
        got = []
        for m in morsels:
            got.extend(m.column(name.encode()).to_pylist())
        assert got == expected, name


def test_row_groups_arrive_in_file_order(wide_file):
    ids = []
    for m in _read(wide_file, 8, columns=["id"]):
        ids.extend(m.column(b"id").to_pylist())
    assert ids == list(range(len(ids)))


def test_default_threads_matches_serial(wide_file):
    _same(_read(wide_file, 1), _read(wide_file, None))


def test_projection_order_and_missing_projection(wide_file):
    cols = ["s_plain", "id", "f64"]
    serial = _read(wide_file, 1, columns=cols)
    assert [c.decode() for c in serial[0].column_names] == cols
    _same(serial, _read(wide_file, 8, columns=cols))


def test_bytes_source(wide_file):
    data = open(wide_file, "rb").read()
    _same(_read(wide_file, 1, columns=["id", "s_dict"]), _read(data, 8, columns=["id", "s_dict"]))


def test_predicates_and_row_group_pruning(wide_file):
    # `id` is sorted, so the footer statistics prune every row group but the first two.
    preds = [("id", "<", 2_000)]
    serial = _read(wide_file, 1, columns=["id", "s_dict"], predicates=preds)
    parallel = _read(wide_file, 8, columns=["id", "s_dict"], predicates=preds)
    _same(serial, parallel)
    assert sum(len(m) for m in parallel) == 2_000
    assert len(parallel) == 2  # pruned row groups yield no Morsel at all

    # An unsorted predicate column that is not projected is read and projected away.
    preds = [("i8", "<", 5)]
    _same(
        _read(wide_file, 1, columns=["id"], predicates=preds),
        _read(wide_file, 8, columns=["id"], predicates=preds),
    )


def test_pruning_everything_yields_nothing(wide_file):
    assert _read(wide_file, 8, columns=["id"], predicates=[("id", ">", 10**9)]) == []


def test_single_row_group_file_many_columns(tmp_path):
    path = str(tmp_path / "one.parquet")
    pq.write_table(_table(3_000), path, row_group_size=3_000)
    _same(_read(path, 1), _read(path, 8))


def test_zero_column_projection(wide_file):
    assert [len(m) for m in _read(wide_file, 8, columns=[])] == [
        len(m) for m in _read(wide_file, 1, columns=[])
    ]


def test_binary_vs_text_tag_is_preserved(wide_file):
    # The worker decides VARCHAR vs VARBINARY from the logical type, as the Python edge does.
    serial = _read(wide_file, 1, columns=["s_plain", "raw"])[0]
    parallel = _read(wide_file, 8, columns=["s_plain", "raw"])[0]
    assert isinstance(serial.column(b"s_plain").to_pylist()[0], str)
    assert isinstance(serial.column(b"raw").to_pylist()[0], bytes)
    assert isinstance(parallel.column(b"s_plain").to_pylist()[0], str)
    assert isinstance(parallel.column(b"raw").to_pylist()[0], bytes)


def test_repeated_runs_are_deterministic(wide_file):
    first = _read(wide_file, 8, columns=["id", "nullable_s"])
    for _ in range(10):
        _same(first, _read(wide_file, 8, columns=["id", "nullable_s"]))


# --- lifecycle -----------------------------------------------------------------------------


def test_abandoned_reader_releases_the_file(wide_file, tmp_path):
    # Breaking out early cancels the job and joins its in-flight chunks BEFORE the mapping
    # is released; a worker still reading a freed mapping would crash the interpreter.
    for _ in range(50):
        with parquet.read_parquet(wide_file, threads=8) as reader:
            for _morsel in reader:
                break
    # ...and the pool still works afterwards.
    _same(_read(wide_file, 1, columns=["id"]), _read(wide_file, 8, columns=["id"]))


def test_unstarted_and_closed_generators(wide_file):
    reader = parquet.read_parquet(wide_file, threads=8)
    gen = iter(reader)
    next(gen)
    gen.close()
    del gen
    # never iterated at all
    parquet.read_parquet(wide_file, threads=8)


def test_two_readers_interleaved_on_one_thread_do_not_deadlock(wide_file):
    # Runners never block waiting for the consumer, so a reader nobody is pulling from
    # cannot starve another reader of pool threads.
    result = {}

    def run():
        a = parquet.read_parquet(wide_file, columns=["id"], threads=64)
        b = parquet.read_parquet(wide_file, columns=["s_plain"], threads=64)
        with a, b:
            result["rows"] = sum(len(x) + len(y) for x, y in zip(a, b))

    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    worker.join(60)
    assert not worker.is_alive(), "interleaved parallel readers deadlocked"
    assert result["rows"] == 2 * 20_000


def test_concurrent_readers_from_many_threads(wide_file):
    expected = [m.column(b"id").to_pylist() for m in _read(wide_file, 1, columns=["id"])]
    failures = []

    def run():
        try:
            for _ in range(5):
                got = [m.column(b"id").to_pylist() for m in _read(wide_file, 8, columns=["id"])]
                assert got == expected
        except BaseException as exc:  # noqa: BLE001 — surfaced below
            failures.append(exc)

    threads = [threading.Thread(target=run) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(120)
    assert not failures, failures


# --- errors --------------------------------------------------------------------------------


def _corrupt_one_chunk(src, dst, row_group, column):
    meta = pq.ParquetFile(src).metadata
    chunk = meta.row_group(row_group).column(column)
    start = chunk.data_page_offset
    if chunk.has_dictionary_page:
        start = min(start, chunk.dictionary_page_offset)
    data = bytearray(open(src, "rb").read())
    # Garble the page bytes (headers and payload) of that one column chunk.
    for i in range(start, start + min(chunk.total_compressed_size, 256)):
        data[i] = 0xFF
    open(dst, "wb").write(bytes(data))


def test_decode_error_matches_serial_and_earlier_row_groups_still_arrive(wide_file, tmp_path):
    bad = str(tmp_path / "bad.parquet")
    names = pq.ParquetFile(wide_file).schema_arrow.names
    _corrupt_one_chunk(wide_file, bad, row_group=5, column=names.index("s_plain"))

    def run(threads):
        got = 0
        try:
            with parquet.read_parquet(bad, columns=["id", "s_plain"], threads=threads) as r:
                for m in r:
                    got += 1
        except Exception as exc:  # noqa: BLE001 — compared below
            return got, type(exc), str(exc)
        return got, None, None

    serial = run(1)
    parallel = run(8)
    assert serial[1] is not None, "corruption was not detected"
    assert parallel == serial
    assert serial[0] == 5  # row groups 0-4 were delivered before the failure


def test_reading_after_an_error_still_works(wide_file, tmp_path):
    bad = str(tmp_path / "bad.parquet")
    names = pq.ParquetFile(wide_file).schema_arrow.names
    _corrupt_one_chunk(wide_file, bad, row_group=2, column=names.index("f64"))
    for _ in range(5):
        with pytest.raises(Exception):
            _read(bad, 8, columns=["f64"])
    _same(_read(wide_file, 1, columns=["f64"]), _read(wide_file, 8, columns=["f64"]))


# --- argument handling and sizing ------------------------------------------------------------


@pytest.mark.parametrize("bad", [0, -1, 1.5, "4", True])
def test_invalid_thread_counts_are_rejected(wide_file, bad):
    with pytest.raises(ValueError):
        parquet.read_parquet(wide_file, threads=bad)


def test_window_is_bounded_by_memory_budget():
    window = rugo_native.parallel_window
    # A narrow projection of small row groups queues two chunks per worker...
    assert window(18, 2, 800_000) == 18
    # ...a wide one only needs a couple of row groups to keep every worker busy...
    assert window(18, 100, 1_000_000) == 2
    # ...a row group bigger than the 256 MB budget is held one at a time...
    assert window(4, 100, 300_000_000) == 1
    # ...and the window never collapses below two row groups while they are affordable.
    assert window(1, 1, 0) >= 2
    assert window(8, 50, 10_000_000) >= 2


def test_available_cpus_honours_lambda_memory(monkeypatch):
    monkeypatch.setattr(parquet, "_available_cpus", parquet._available_cpus.__wrapped__)
    monkeypatch.setattr(os, "sched_getaffinity", lambda _pid: set(range(16)), raising=False)
    monkeypatch.setenv("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "1769")
    assert parquet._available_cpus() == 1
    monkeypatch.setenv("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "512")
    assert parquet._available_cpus() == 1
    monkeypatch.setenv("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "10240")
    assert parquet._available_cpus() == 6
    monkeypatch.delenv("AWS_LAMBDA_FUNCTION_MEMORY_SIZE")
    assert parquet._available_cpus() >= 1


def test_one_cpu_available_stays_serial(wide_file, monkeypatch):
    monkeypatch.setattr(parquet, "_available_cpus", lambda: 1)
    assert parquet._resolve_threads(None) == 1
    _same(_read(wide_file, 1), _read(wide_file, None))
