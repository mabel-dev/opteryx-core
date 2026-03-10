import os
import sys
import threading
import time
from collections import Counter
from copy import deepcopy

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.parquet_io import reader
from opteryx.parquet_io.cache import InMemoryParquetCache


def _build_footers(paths, rowgroups, columns):
    footers = {}
    base_by_path = {}
    for path_index, path in enumerate(paths):
        base = path_index * 1_000_000
        base_by_path[path] = base
        groups = []
        for rg_idx in range(rowgroups):
            rg_columns = []
            for col_idx in range(columns):
                offset = base + (rg_idx * 10_000) + (col_idx * 100)
                rg_columns.append(
                    {
                        "name": f"c{col_idx}",
                        "data_page_offset": offset,
                        "total_compressed_size": 32,
                        "dictionary_page_offset": None,
                        "compression_codec": "none",
                        "encodings": ["PLAIN"],
                    }
                )
            groups.append({"columns": rg_columns})
        footers[path] = {"row_groups": groups}
    return footers, base_by_path


def _patch_footer_helpers(monkeypatch, footers):
    def _fake_read_footer_payload(filesystem, path, file_size=None, connector=None):
        return path.encode("utf8"), 64, 1

    def _fake_parse_footer(path, envelope, footer_bytes):
        meta = deepcopy(footers[path])
        meta["__footer_bytes__"] = footer_bytes
        return meta

    monkeypatch.setattr(reader, "_read_footer_payload", _fake_read_footer_payload)
    monkeypatch.setattr(reader, "_parse_footer_envelope", _fake_parse_footer)


def _set_scheduler_caps(
    monkeypatch,
    *,
    files_in_flight,
    rowgroups_per_file,
    rowgroups_in_flight=None,
    global_ranges,
    per_rowgroup_ranges,
):
    import opteryx.config as cfg

    if rowgroups_in_flight is None:
        rowgroups_in_flight = files_in_flight * rowgroups_per_file

    monkeypatch.setattr(cfg, "PARQUET_FILES_IN_FLIGHT", files_in_flight)
    monkeypatch.setattr(cfg, "PARQUET_ROWGROUPS_PER_FILE_IN_FLIGHT", rowgroups_per_file)
    monkeypatch.setattr(cfg, "PARQUET_ROWGROUPS_IN_FLIGHT", rowgroups_in_flight)
    monkeypatch.setattr(cfg, "PARQUET_GLOBAL_RANGE_READERS", global_ranges)
    monkeypatch.setattr(cfg, "PARQUET_RANGE_READERS_PER_ROWGROUP", per_rowgroup_ranges)


def _fake_decoder(raw_bytes, col_stats):
    return (col_stats["data_page_offset"], len(raw_bytes))


class _TrackingFilesystem:
    def __init__(
        self,
        base_by_path,
        *,
        sleep_s=0.0,
        startup_target=0,
        release_event=None,
    ):
        self._base_by_path = base_by_path
        self._sleep_s = sleep_s
        self._startup_target = startup_target
        self._release_event = release_event

        self.lock = threading.Lock()
        self.active_global = 0
        self.peak_global = 0
        self.active_by_rowgroup = Counter()
        self.peak_by_rowgroup = Counter()
        self.started_by_rowgroup = Counter()
        self.started_total = 0
        self.read_order = []
        self.started_at_by_rowgroup = {}
        self.startup_reached = threading.Event()

    def _rowgroup_for_offset(self, path, offset):
        base = self._base_by_path[path]
        return (offset - base) // 10_000

    def read_ranges(self, path, ranges):
        out = []
        for offset, length in ranges:
            rg_idx = self._rowgroup_for_offset(path, offset)
            rg_key = (path, rg_idx)

            with self.lock:
                self.active_global += 1
                self.peak_global = max(self.peak_global, self.active_global)
                self.active_by_rowgroup[rg_key] += 1
                self.peak_by_rowgroup[rg_key] = max(
                    self.peak_by_rowgroup[rg_key], self.active_by_rowgroup[rg_key]
                )
                self.started_by_rowgroup[rg_key] += 1
                self.started_total += 1
                self.read_order.append(rg_key)
                self.started_at_by_rowgroup.setdefault(rg_key, []).append(time.monotonic())
                if self._startup_target and self.started_total >= self._startup_target:
                    self.startup_reached.set()

            if self._release_event is not None and not self._release_event.wait(timeout=5):
                raise TimeoutError("timed out waiting for test release_event")

            if self._sleep_s:
                time.sleep(self._sleep_s)

            out.append(bytes([offset % 251]) * length)

            with self.lock:
                self.active_global -= 1
                self.active_by_rowgroup[rg_key] -= 1

        return out


def test_scheduler_v2_respects_hierarchical_caps(monkeypatch):
    paths = ["a.parquet", "b.parquet"]
    footers, base_by_path = _build_footers(paths, rowgroups=2, columns=20)
    _patch_footer_helpers(monkeypatch, footers)
    _set_scheduler_caps(
        monkeypatch,
        files_in_flight=2,
        rowgroups_per_file=2,
        global_ranges=24,
        per_rowgroup_ranges=10,
    )

    fs = _TrackingFilesystem(base_by_path, sleep_s=0.001)
    rows = list(
        reader._iter_row_groups_v2(
            fs,
            paths,
            [f"c{i}" for i in range(20)],
            cache=InMemoryParquetCache(),
            max_workers=32,
            decoder=_fake_decoder,
        )
    )

    assert len(rows) == 4
    assert fs.peak_global <= 24
    assert max(fs.peak_by_rowgroup.values()) <= 10
    assert max(row["__ranges_in_flight_peak__"] for row in rows) <= 24
    assert max(row["__rowgroup_peak_in_flight__"] for row in rows) <= 10
    assert max(row["__active_files_peak__"] for row in rows) <= 2
    assert max(row["__active_rowgroups_peak__"] for row in rows) <= 4


def test_scheduler_v2_rowgroups_start_concurrently(monkeypatch):
    """All admitted row groups start before any complete when global_ranges_cap >= row-group count.

    With the batched design each row group task calls read_ranges once for all columns.
    The mock blocks after the first column of each call, so startup_target=4 fires once
    all four row-group tasks have entered read_ranges.
    """
    paths = ["a.parquet", "b.parquet"]
    footers, base_by_path = _build_footers(paths, rowgroups=2, columns=20)
    _patch_footer_helpers(monkeypatch, footers)
    _set_scheduler_caps(
        monkeypatch,
        files_in_flight=2,
        rowgroups_per_file=2,
        global_ranges=4,          # allow all 4 row groups in flight simultaneously
        per_rowgroup_ranges=20,
    )

    release_event = threading.Event()
    # Each batched read_ranges call increments started_total per column but blocks after
    # the first column.  startup_target=4 fires when one column from each of the four
    # row-group tasks has started, proving concurrent dispatch.
    fs = _TrackingFilesystem(
        base_by_path,
        startup_target=4,
        release_event=release_event,
    )

    results = []
    errors = []

    def _consume():
        try:
            results.extend(
                reader._iter_row_groups_v2(
                    fs,
                    paths,
                    [f"c{i}" for i in range(20)],
                    cache=InMemoryParquetCache(),
                    max_workers=32,
                    decoder=_fake_decoder,
                )
            )
        except Exception as err:  # pragma: no cover
            errors.append(err)

    worker = threading.Thread(target=_consume, daemon=True)
    worker.start()

    assert fs.startup_reached.wait(timeout=3), "Row groups did not all start concurrently"

    release_event.set()
    worker.join(timeout=5)

    assert not worker.is_alive()
    assert not errors
    assert len(results) == 4


def test_scheduler_v2_single_inflight_sequential_dispatch(monkeypatch):
    """With global_ranges=1 only one row-group task runs at a time.

    All columns of row group 0 must be read before any column of row group 1 starts,
    and the first yielded result must be row group 0.
    """
    paths = ["single.parquet"]
    footers, base_by_path = _build_footers(paths, rowgroups=3, columns=4)
    _patch_footer_helpers(monkeypatch, footers)
    _set_scheduler_caps(
        monkeypatch,
        files_in_flight=1,
        rowgroups_per_file=3,
        global_ranges=1,          # one row-group task in flight at a time
        per_rowgroup_ranges=4,
    )

    fs = _TrackingFilesystem(base_by_path, sleep_s=0.002)

    rows = list(
        reader._iter_row_groups_v2(
            fs,
            paths,
            [f"c{i}" for i in range(4)],
            cache=InMemoryParquetCache(),
            max_workers=4,
            decoder=_fake_decoder,
        )
    )

    # With one slot, all 4 columns of rg0 run (in the single batched read_ranges call)
    # before the task for rg1 is submitted.
    first_four = [rg_idx for _, rg_idx in fs.read_order[:4]]
    assert first_four == [0, 0, 0, 0]
    assert rows[0]["__row_group__"] == 0


def test_scheduler_v2_refills_while_draining_ready_rows(monkeypatch):
    paths = ["single.parquet"]
    footers, base_by_path = _build_footers(paths, rowgroups=8, columns=1)
    _patch_footer_helpers(monkeypatch, footers)
    _set_scheduler_caps(
        monkeypatch,
        files_in_flight=1,
        rowgroups_per_file=8,
        global_ranges=4,
        per_rowgroup_ranges=1,
    )

    fs = _TrackingFilesystem(base_by_path, sleep_s=0.001)

    yielded_at = []
    for _ in reader._iter_row_groups_v2(
        fs,
        paths,
        ["c0"],
        cache=InMemoryParquetCache(),
        max_workers=8,
        decoder=_fake_decoder,
    ):
        yielded_at.append(time.monotonic())
        # Simulate downstream work while the scheduler should keep filling.
        time.sleep(0.02)

    assert len(yielded_at) == 8

    with fs.lock:
        first_start_by_rg = {
            rg_idx: min(starts)
            for (_, rg_idx), starts in fs.started_at_by_rowgroup.items()
        }

    assert len(first_start_by_rg) == 8
    second_wave_start = min(
        start_ts for rg_idx, start_ts in first_start_by_rg.items() if rg_idx >= 4
    )
    # If refill is continuous, second-wave reads start before we drain the first
    # four emitted rows from the generator.
    assert second_wave_start < yielded_at[3]


def test_scheduler_v2_respects_global_rowgroups_in_flight_cap(monkeypatch):
    paths = ["single.parquet"]
    footers, base_by_path = _build_footers(paths, rowgroups=8, columns=6)
    _patch_footer_helpers(monkeypatch, footers)
    _set_scheduler_caps(
        monkeypatch,
        files_in_flight=1,
        rowgroups_per_file=8,
        rowgroups_in_flight=3,
        global_ranges=8,
        per_rowgroup_ranges=6,
    )

    fs = _TrackingFilesystem(base_by_path, sleep_s=0.001)

    rows = list(
        reader._iter_row_groups_v2(
            fs,
            paths,
            [f"c{i}" for i in range(6)],
            cache=InMemoryParquetCache(),
            max_workers=8,
            decoder=_fake_decoder,
        )
    )

    assert len(rows) == 8
    assert max(row["__active_rowgroups_peak__"] for row in rows) <= 3
    assert max(row["__rowgroups_in_flight_cap__"] for row in rows) == 3


def test_scheduler_v2_reuses_prefetched_footers(monkeypatch):
    paths = ["a.parquet", "b.parquet"]
    footers, base_by_path = _build_footers(paths, rowgroups=2, columns=4)
    _set_scheduler_caps(
        monkeypatch,
        files_in_flight=2,
        rowgroups_per_file=2,
        global_ranges=8,
        per_rowgroup_ranges=4,
    )

    def _should_not_fetch(*_args, **_kwargs):
        raise AssertionError("_read_footer_payload should not be called with prefetched_footers")

    monkeypatch.setattr(reader, "_read_footer_payload", _should_not_fetch)

    fs = _TrackingFilesystem(base_by_path, sleep_s=0.001)
    rows = list(
        reader._iter_row_groups_v2(
            fs,
            paths,
            [f"c{i}" for i in range(4)],
            cache=InMemoryParquetCache(),
            max_workers=8,
            decoder=_fake_decoder,
            prefetched_footers=footers,
        )
    )

    assert len(rows) == 4
    assert {row["__path__"] for row in rows} == set(paths)


def test_scheduler_v2_parity_with_v1(monkeypatch):
    paths = ["a.parquet", "b.parquet"]
    footers, base_by_path = _build_footers(paths, rowgroups=2, columns=5)
    _patch_footer_helpers(monkeypatch, footers)

    fs_v1 = _TrackingFilesystem(base_by_path)
    fs_v2 = _TrackingFilesystem(base_by_path)
    columns = [f"c{i}" for i in range(5)]

    rows_v1 = list(
        reader._iter_row_groups_v1(
            fs_v1,
            paths,
            columns,
            cache=InMemoryParquetCache(),
            max_workers=8,
            decoder=_fake_decoder,
        )
    )

    _set_scheduler_caps(
        monkeypatch,
        files_in_flight=2,
        rowgroups_per_file=2,
        global_ranges=24,
        per_rowgroup_ranges=10,
    )
    rows_v2 = list(
        reader._iter_row_groups_v2(
            fs_v2,
            paths,
            columns,
            cache=InMemoryParquetCache(),
            max_workers=8,
            decoder=_fake_decoder,
        )
    )

    normalized_v1 = {
        (row["__path__"], row["__row_group__"]): tuple(row[col] for col in columns) for row in rows_v1
    }
    normalized_v2 = {
        (row["__path__"], row["__row_group__"]): tuple(row[col] for col in columns) for row in rows_v2
    }

    assert normalized_v1 == normalized_v2


def test_scheduler_v2_early_close_cancels_pending_work(monkeypatch):
    paths = ["a.parquet", "b.parquet"]
    footers, base_by_path = _build_footers(paths, rowgroups=2, columns=12)
    _patch_footer_helpers(monkeypatch, footers)
    _set_scheduler_caps(
        monkeypatch,
        files_in_flight=2,
        rowgroups_per_file=2,
        global_ranges=1,          # one row-group task at a time so remaining tasks
        per_rowgroup_ranges=12,   # can be cancelled before they start
    )

    fs = _TrackingFilesystem(base_by_path, sleep_s=0.01)
    generator = reader._iter_row_groups_v2(
        fs,
        paths,
        [f"c{i}" for i in range(12)],
        cache=InMemoryParquetCache(),
        max_workers=4,
        decoder=_fake_decoder,
    )

    first = next(generator)
    assert "__path__" in first

    close_start = time.monotonic()
    generator.close()
    close_elapsed = time.monotonic() - close_start

    time.sleep(0.05)
    with fs.lock:
        started_after_close = fs.started_total

    # 4 row groups × 12 columns = 48 total column reads if everything ran.
    # With global_ranges=1, at most 2 row-group tasks (the one that produced the
    # first result + at most one more dispatched after) can have started, so < 48.
    total_planned_ranges = len(paths) * 2 * 12

    assert close_elapsed < 0.5
    assert started_after_close < total_planned_ranges


def test_iter_row_groups_prefers_local_serial_reader_over_io_process_ring(monkeypatch):
    import opteryx.config as cfg
    import opteryx.parquet_io.io_process_ring as io_ring

    rows = [{"__path__": "x.parquet", "__row_group__": 0, "c0": (1, 1)}]

    def _fake_local(*args, **kwargs):
        return iter(rows)

    def _fail_io_ring(*args, **kwargs):
        raise AssertionError("io_process_rowgroup_ring should be bypassed for local fast path")

    monkeypatch.setattr(cfg.features, "use_serial_reader", frozenset({"LOCAL"}))
    monkeypatch.setattr(cfg.features, "io_process_rowgroup_ring", True)
    monkeypatch.setattr(reader, "_iter_row_groups_local_serial", _fake_local)
    monkeypatch.setattr(io_ring, "iter_row_groups_io_process_v2", _fail_io_ring)

    result = list(
        reader.iter_row_groups(
            filesystem=object(),
            paths=["x.parquet"],
            column_names=["c0"],
            connector="LOCAL",
        )
    )

    assert result == rows


def test_iter_row_groups_uses_io_process_ring_when_serial_reader_disabled(monkeypatch):
    import opteryx.config as cfg
    import opteryx.parquet_io.io_process_ring as io_ring

    rows = [{"__path__": "x.parquet", "__row_group__": 0, "c0": (1, 1)}]

    def _fail_local(*args, **kwargs):
        raise AssertionError("local serial reader should be bypassed when selector is empty")

    def _fake_io_ring(*args, **kwargs):
        return iter(rows)

    monkeypatch.setattr(cfg.features, "use_serial_reader", frozenset())
    monkeypatch.setattr(cfg.features, "io_process_rowgroup_ring", True)
    monkeypatch.setattr(reader, "_iter_row_groups_local_serial", _fail_local)
    monkeypatch.setattr(io_ring, "iter_row_groups_io_process_v2", _fake_io_ring)

    result = list(
        reader.iter_row_groups(
            filesystem=object(),
            paths=["x.parquet"],
            column_names=["c0"],
            connector="LOCAL",
        )
    )

    assert result[0]["__parquet_scan_strategy__"] == "io_process_ring"


def test_local_serial_fastpath_reads_columns_serially(monkeypatch):
    paths = ["single.parquet"]
    footers, base_by_path = _build_footers(paths, rowgroups=2, columns=7)
    _patch_footer_helpers(monkeypatch, footers)

    class _SerialFilesystem:
        def __init__(self):
            self.calls = []

        def read_ranges(self, path, ranges):
            self.calls.append((path, list(ranges)))
            return [bytes([offset % 251]) * length for offset, length in ranges]

    fs = _SerialFilesystem()
    rows = list(
        reader._iter_row_groups_local_serial(
            fs,
            paths,
            ["c0", "c1", "c2"],
            cache=InMemoryParquetCache(),
            decoder=_fake_decoder,
            connector="LOCAL",
        )
    )

    expected_offsets = [
        base_by_path["single.parquet"] + (rg_idx * 10_000) + (col_idx * 100)
        for rg_idx in range(2)
        for col_idx in range(3)
    ]
    actual_offsets = [ranges[0][0] for _, ranges in fs.calls]

    assert len(rows) == 2
    assert len(fs.calls) == 6
    assert all(len(ranges) == 1 for _, ranges in fs.calls)
    assert actual_offsets == expected_offsets
    assert rows[0]["__range_request_count__"] == 3
    assert rows[0]["__active_files_peak__"] == 1
    assert rows[0]["__active_rowgroups_peak__"] == 1
    assert rows[0]["__rowgroups_in_flight_cap__"] == 1
    assert rows[0]["__ranges_in_flight_peak__"] == 1


def test_local_serial_fastpath_combines_reads_when_projection_is_majority(monkeypatch):
    paths = ["single.parquet"]
    footers, base_by_path = _build_footers(paths, rowgroups=2, columns=4)
    _patch_footer_helpers(monkeypatch, footers)

    class _SerialFilesystem:
        def __init__(self):
            self.calls = []

        def read_ranges(self, path, ranges):
            self.calls.append((path, list(ranges)))
            return [bytes([offset % 251]) * length for offset, length in ranges]

    fs = _SerialFilesystem()
    rows = list(
        reader._iter_row_groups_local_serial(
            fs,
            paths,
            ["c0", "c1", "c2"],
            cache=InMemoryParquetCache(),
            decoder=_fake_decoder,
            connector="LOCAL",
        )
    )

    expected_spans = [
        [(base_by_path["single.parquet"] + (rg_idx * 10_000), 232)]
        for rg_idx in range(2)
    ]
    actual_spans = [ranges for _, ranges in fs.calls]

    assert len(rows) == 2
    assert len(fs.calls) == 2
    assert actual_spans == expected_spans
    assert rows[0]["__range_request_count__"] == 1
    assert rows[0]["__ranges_in_flight_peak__"] == 1
