# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
IOPS Read Node

SQL Query Execution Plan Node that uses the IO-process-isolation subsystem
(``opteryx/iops/``) to read blobs.

Downloads run in a dedicated spawned process with a lock-free ring buffer;
decoding runs synchronously in the EXEC process as each slot becomes ready.
This avoids asyncio overhead and GIL contention on the download path.

Early termination (LIMIT / filter short-circuit)
-------------------------------------------------
Breaking from the ``for payload in reader.iter_blobs(...)`` loop sends
``GeneratorExit`` into the generator, which propagates to
``IOPSReader.__exit__`` → ``_stop()`` → SHUTDOWN pipe message.  The worker
drains its in-flight thread pool (at most ``cfg.max_inflight`` blobs) and
exits.  The ``_shutdown_event`` in the worker ensures any thread stuck in the
CAS spin exits within a bounded number of iterations rather than hanging.
"""

from __future__ import annotations

import time
import warnings
from typing import Generator

import pyarrow
from orso.schema import convert_orso_schema_to_arrow_schema

from opteryx import EOS
from opteryx.models import QueryProperties
from opteryx.utils.file_decoders import get_decoder

from .read_node import ReaderNode
from .read_node import normalize_morsel
from .read_node import struct_to_jsonb


class IopsReadNode(ReaderNode):
    """Read node backed by the IOPS ring-buffer worker process.

    Used for all asynchronous (blob) connectors: GCS, S3, and any connector
    that exposes ``stream_to(path, sink)`` / ``async_stream_to(...)`` on its
    filesystem object.  Uses the same ``**parameters`` bag as all other
    ``ReaderNode`` subclasses.
    """

    def __init__(self, properties: QueryProperties, **parameters) -> None:
        ReaderNode.__init__(self, properties=properties, **parameters)
        self.predicates = parameters.get("predicates")

    @property
    def name(self) -> str:  # pragma: no cover
        return "IOPS Read"

    def execute(self, morsel, **kwargs) -> Generator:
        if morsel == EOS:
            yield None
            return

        from opteryx.iops import IOPSReader

        orso_schema = self.parameters["schema"]

        # ── Empty manifest ────────────────────────────────────────────────────
        if not self.manifest or self.manifest.get_file_count() == 0:
            from orso import DataFrame

            as_arrow = DataFrame(rows=[], schema=orso_schema).arrow()
            renames = [orso_schema.column(col).identity for col in as_arrow.column_names]
            as_arrow = as_arrow.rename_columns(renames)
            yield as_arrow
            return

        # ── Project schema to requested columns only ──────────────────────────
        orso_schema_cols = [
            col
            for col in orso_schema.columns
            if col.identity in {c.schema_column.identity for c in self.columns}
        ]
        orso_schema.columns = orso_schema_cols
        self.readings["columns_read"] += len(orso_schema.columns)

        arrow_schema = convert_orso_schema_to_arrow_schema(orso_schema, use_identities=True)
        records_to_read = self.limit if self.limit is not None else float("inf")

        # ── Determine protocol from first blob path ───────────────────────────
        file_zero = self.manifest.files[0].file_path
        protocol = file_zero.split("://")[0] if "://" in file_zero else "file"

        blob_paths = self.manifest.get_file_paths()
        result_morsel = None

        with IOPSReader(protocol=protocol) as reader:
            for payload in reader.iter_blobs(blob_paths):
                # ── Error path ────────────────────────────────────────────────
                if payload.error:
                    self.telemetry.add_message(
                        f"failed to read {payload.blob_path} ({payload.error})"
                    )
                    self.readings["failed_reads"] += 1
                    warnings.warn(f"iops: failed to read {payload.blob_path} — {payload.error}")
                    payload.release()
                    continue

                blob_name = payload.blob_path
                decoder = get_decoder(blob_name)

                # ── Decode phase ──────────────────────────────────────────────
                decode_start = time.monotonic_ns()
                try:
                    self.readings["bytes_decoded"] = (
                        self.readings.get("bytes_decoded", 0) + payload.length
                    )
                    decoded = decoder(
                        payload.data,
                        projection=self.columns,
                        selection=self.predicates,
                    )
                    # Slot is no longer needed once Arrow has decoded into its
                    # own heap memory.
                    payload.release()
                except Exception as err:
                    payload.release()
                    self.telemetry.add_message(
                        f"failed to decode {blob_name} ({err.__class__.__name__})"
                    )
                    self.readings["failed_reads"] += 1
                    warnings.warn(f"iops: failed to decode {blob_name} — {err}")
                    continue

                decode_ns = time.monotonic_ns() - decode_start
                self.readings["time_decoding_blobs"] += decode_ns
                self.telemetry.time_decoding_blobs += decode_ns

                # Accumulate download time from the worker's wall-clock measurement.
                download_ns = int(payload.gcs_latency_s * 1e9)
                self.telemetry.time_downloading_blobs += download_ns

                num_rows, _, raw_bytes, result_morsel = decoded
                self.readings["rows_seen"] += num_rows
                self.readings["blobs_seen"] += 1

                # ── LIMIT enforcement ─────────────────────────────────────────
                if records_to_read < result_morsel.num_rows:
                    result_morsel = result_morsel.slice(0, int(records_to_read))
                    records_to_read = 0
                else:
                    records_to_read -= result_morsel.num_rows

                # ── Normalise and cast ────────────────────────────────────────
                result_morsel = struct_to_jsonb(result_morsel)
                result_morsel = normalize_morsel(orso_schema, result_morsel)
                if result_morsel.column_names != ["*"]:
                    result_morsel = result_morsel.cast(arrow_schema)

                self.readings["blobs_read"] += 1
                self.telemetry.blobs_read += 1
                self.readings["rows_read"] += result_morsel.num_rows
                self.telemetry.rows_read += result_morsel.num_rows
                self.readings["bytes_processed"] += result_morsel.nbytes
                self.telemetry.bytes_processed += result_morsel.nbytes
                self.readings["bytes_raw"] += raw_bytes
                self.telemetry.bytes_raw = getattr(self.telemetry, "bytes_raw", 0) + raw_bytes

                yield result_morsel

                # ── Early termination ─────────────────────────────────────────
                # Breaking here sends GeneratorExit into iter_blobs which
                # propagates to IOPSReader._stop() → SHUTDOWN pipe message.
                if records_to_read <= 0:
                    break

        # ── Empty result guard ────────────────────────────────────────────────
        if result_morsel is None:
            self.readings["empty_datasets"] += 1
            arrow_schema = convert_orso_schema_to_arrow_schema(orso_schema, use_identities=True)
            yield pyarrow.Table.from_arrays(
                [pyarrow.array([]) for _ in arrow_schema], schema=arrow_schema
            )
