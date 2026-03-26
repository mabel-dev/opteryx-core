# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from typing import Any

from opteryx.compiled.draken.morsels.morsel import Morsel
from opteryx.compiled.structures.shuffle_partition import row_indexes_by_bin_flat
from opteryx.managers.kvstores import create_kv_store
from opteryx.models import QueryProperties
from opteryx.operators.shuffle import BinStore
from opteryx.operators.shuffle.partitioning import normalize_num_bins
from opteryx.operators.shuffle.partitioning import select_num_bins_from_rows

from opteryx import EOS

from . import BasePlanNode

_DATA_FORMAT = "draken"


class ShuffleNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        super().__init__(properties=properties, **parameters)

        self.partition_columns = self._normalize_partition_columns(parameters.get("columns", []))
        self.shift_bits = int(parameters.get("shift_bits", 0) or 0)
        if self.shift_bits < 0:
            raise ValueError("shift_bits must be zero or positive")

        explicit_bins = parameters.get("num_bins")
        if explicit_bins is None:
            estimate = self._extract_row_count_estimate(parameters)
            self.num_bins = select_num_bins_from_rows(estimate)
        else:
            self.num_bins = normalize_num_bins(int(explicit_bins))

        self.memory_budget_bytes = int(parameters.get("memory_budget_bytes", 256 * 1024 * 1024))
        self.target_bin_buffer_bytes = int(
            parameters.get("target_bin_buffer_bytes", 8 * 1024 * 1024)
        )

        self.spill_enabled = bool(parameters.get("spill_enabled", True))
        self.spill_codec_default = str(parameters.get("spill_codec_default", "lz4")).lower()
        self._spill_options = {
            "codec_default": self.spill_codec_default,
            "checksum_enabled": True,
        }

        self._pass_id = str(parameters.get("pass_id", self.identity))
        self._scope_key = str(parameters.get("scope_key", f"shuffle/{self._pass_id}"))

        self._bin_store = self._initialize_bin_store(
            parameters.get("spill_store"),
            parameters.get("spill_location", parameters.get("spill_store_location")),
        )
        if self._bin_store is None:
            self.spill_enabled = False

        self._bin_buffers: dict[int, list] = defaultdict(list)
        self._bin_buffer_bytes: dict[int, int] = defaultdict(int)
        self._total_buffer_bytes = 0

    @property
    def name(self):  # pragma: no cover
        return "Shuffle"

    @property
    def config(self):  # pragma: no cover
        partition = ", ".join(
            col.decode("utf8") if isinstance(col, bytes) else str(col)
            for col in (self.partition_columns or [])
        )
        return f"BINS = {self.num_bins}, SHIFT = {self.shift_bits}, PARTITION BY = [{partition}]"

    @staticmethod
    def _normalize_partition_columns(columns) -> list[bytes]:
        normalized = []
        for column in columns or []:
            if column is None:
                continue
            if hasattr(column, "schema_column") and hasattr(column.schema_column, "identity"):
                identity = column.schema_column.identity
            else:
                identity = column
            if isinstance(identity, bytes):
                normalized.append(identity)
            else:
                normalized.append(str(identity).encode("utf-8"))
        return normalized

    @staticmethod
    def _extract_row_count_estimate(parameters: dict[str, Any]) -> int | None:
        keys = (
            "row_count",
            "row_count_estimate",
            "rows_estimate",
            "estimated_row_count",
            "n_rows",
        )

        def _coerce(value):
            if value is None:
                return None
            try:
                coerced = int(value)
            except (TypeError, ValueError):
                return None
            return coerced if coerced > 0 else None

        for key in keys:
            direct = _coerce(parameters.get(key))
            if direct is not None:
                return direct

        statistics = parameters.get("statistics")
        if statistics is None:
            return None

        if isinstance(statistics, dict):
            for key in keys:
                direct = _coerce(statistics.get(key))
                if direct is not None:
                    return direct
            return None

        for key in keys:
            direct = _coerce(getattr(statistics, key, None))
            if direct is not None:
                return direct
        return None

    def _initialize_bin_store(self, configured_store, spill_location):
        if not self.spill_enabled:
            return None
        if isinstance(configured_store, BinStore):
            return configured_store
        if configured_store is not None and hasattr(configured_store, "set"):
            return BinStore(configured_store)
        kv_store = create_kv_store(
            configured_store if configured_store is not None else spill_location
        )
        if kv_store is None:
            return None
        return BinStore(kv_store)

    def _bin_key(self, bin_id: int) -> str:
        return f"pass/{self._pass_id}/bin/{bin_id}"

    def _append_fragment(self, bin_id: int, fragment) -> None:
        size_bytes = int(getattr(fragment, "nbytes", 0) or 0)
        self._bin_buffers[bin_id].append(fragment)
        self._bin_buffer_bytes[bin_id] += size_bytes
        self._total_buffer_bytes += size_bytes
        self.readings["shuffle_rows_buffered"] += int(fragment.num_rows)
        self.readings["shuffle_bytes_buffered"] += size_bytes

        if not self.spill_enabled:
            return

        if self._bin_buffer_bytes[bin_id] >= self.target_bin_buffer_bytes:
            self._spill_bin(bin_id)
        while self._total_buffer_bytes > self.memory_budget_bytes:
            hottest = self._hottest_bin_id()
            if hottest is None:
                break
            self._spill_bin(hottest)

    def _hottest_bin_id(self) -> int | None:
        hottest = None
        hottest_size = 0
        for bin_id, size_bytes in self._bin_buffer_bytes.items():
            if size_bytes > hottest_size and self._bin_buffers[bin_id]:
                hottest = bin_id
                hottest_size = size_bytes
        return hottest

    def _spill_bin(self, bin_id: int) -> None:
        if not self.spill_enabled or self._bin_store is None:
            return
        fragments = self._bin_buffers.get(bin_id)
        if not fragments:
            return

        from opteryx.compiled.draken.storage import write_morsel

        bin_key = self._bin_key(bin_id)
        query_id = self.properties.query_id
        operator_id = self.identity

        for fragment in fragments:
            payload = write_morsel(None, fragment, self._spill_options)
            meta = self._bin_store.append_chunk(
                pass_id=self._pass_id,
                bin_id=bin_id,
                payload=payload,
                query_id=query_id,
                operator_id=operator_id,
                scope_key=self._scope_key,
            )
            meta["rows"] = int(fragment.num_rows)
            meta["payload_bytes"] = len(payload)
            self._bin_store.append_manifest(
                bin_key=bin_key,
                chunk_meta=meta,
                query_id=query_id,
                operator_id=operator_id,
                scope_key=self._scope_key,
            )
            self.readings["shuffle_spill_chunks"] += 1
            self.readings["shuffle_spill_bytes"] += len(payload)

        self._total_buffer_bytes -= self._bin_buffer_bytes[bin_id]
        self._bin_buffer_bytes[bin_id] = 0
        self._bin_buffers[bin_id] = []
        self.readings["shuffle_bins_spilled"] += 1

    def _iter_spilled_bin(self, bin_id: int):
        if self._bin_store is None:
            return ()
        entries = self._bin_store.iter_manifest(
            self._bin_key(bin_id),
            query_id=self.properties.query_id,
            operator_id=self.identity,
        )
        if not entries:
            return ()

        from opteryx.compiled.draken.storage import read_morsel

        decoded = []
        for entry in entries:
            chunk_key = entry.get("chunk_key")
            if not chunk_key:
                continue
            payload = self._bin_store.get_chunk(
                chunk_key,
                query_id=self.properties.query_id,
                operator_id=self.identity,
            )
            if payload is None:
                continue
            decoded.append(read_morsel(payload, {"checksum_enabled": True}))
            self.readings["shuffle_spill_replay_chunks"] += 1
        return decoded

    def _cleanup_spill_scope(self) -> None:
        if self._bin_store is None:
            return
        try:
            deleted = self._bin_store.delete_scope(
                self._scope_key,
                query_id=self.properties.query_id,
                operator_id=self.identity,
            )
            self.readings["shuffle_spill_deleted_keys"] += int(deleted)
        except Exception:
            self.readings["shuffle_cleanup_failures"] += 1

    def _reset_buffers(self) -> None:
        self._bin_buffers.clear()
        self._bin_buffer_bytes.clear()
        self._total_buffer_bytes = 0

    def _partition_chunk(self, chunk) -> None:
        self.readings["shuffle_chunks_in"] += 1
        self.readings["shuffle_rows_in"] += int(chunk.num_rows)

        if self.num_bins == 1:
            self._append_fragment(0, chunk)
            return

        columns = self.partition_columns if self.partition_columns else None
        hashes = chunk.hash(columns)
        flat, offsets = row_indexes_by_bin_flat(hashes, self.num_bins, self.shift_bits)

        # Iterate over bins using the offsets array
        for bin_id in range(self.num_bins):
            start = offsets[bin_id]
            end = offsets[bin_id + 1]
            if start == end:  # empty bin
                continue

            # `row_indexes` is a memoryview slice – it behaves like a sequence
            row_indexes = flat[start:end]
            fragment = chunk.copy(mask=row_indexes)
            self._append_fragment(bin_id, fragment)

    def _drain(self):
        emitted = 0
        for bin_id in range(self.num_bins):
            for morsel in self._iter_spilled_bin(bin_id):
                emitted += 1
                yield morsel
            for morsel in self._bin_buffers.get(bin_id, []):
                emitted += 1
                self.readings["shuffle_memory_replay_chunks"] += 1
                yield morsel
        self.readings["shuffle_chunks_out"] += emitted

    def execute(self, morsel, **kwargs):
        morsel = self.ensure_draken_morsel(morsel)
        _ = kwargs

        if morsel is EOS:
            for result in self._drain():
                yield result
            self._cleanup_spill_scope()
            self._reset_buffers()
            yield EOS
            return

        if isinstance(morsel, Morsel):
            morsels = (morsel,)
        elif isinstance(morsel, Iterable):
            morsels = morsel
        else:  # pragma: no cover
            yield None
            return

        for chunk in morsels:
            if chunk.num_rows == 0:
                continue
            self._partition_chunk(chunk)

        yield None
