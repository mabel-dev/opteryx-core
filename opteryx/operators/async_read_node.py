# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Async Read Node

This is the SQL Query Execution Plan Node responsible for asynchronous reading of data.

This node is used for connectors that implement async_read_blob() for improved I/O
performance. It uses asyncio to fetch multiple blobs concurrently, which significantly
improves read performance for connectors with async support (OpteryxConnector,
FileSystemConnector, etc.).
"""

import asyncio
import queue
import threading
import time
from typing import Generator

import pyarrow
from orso.schema import convert_orso_schema_to_arrow_schema

from opteryx import EOS
from opteryx import config
from opteryx.exceptions import DataError
from opteryx.models import QueryProperties
from opteryx.shared import AsyncMemoryPool
from opteryx.shared import MemoryPool
from opteryx.utils.file_decoders import get_decoder

from .read_node import ReaderNode
from .read_node import normalize_morsel
from .read_node import struct_to_jsonb

CONCURRENT_READS = config.CONCURRENT_READS
MAX_READ_BUFFER_CAPACITY = config.MAX_READ_BUFFER_CAPACITY
ENABLE_ZERO_COPY = config.ENABLE_ZERO_COPY

# Shared read buffer pool - persistent across queries
_shared_read_buffer = None
_buffer_lock = threading.Lock()


def get_shared_read_buffer():
    """
    Get or create the shared read buffer pool.

    The read buffer is a persistent, shared resource used across all
    queries and datasets. Using PagedMemoryPool allows concurrent commits
    from multiple async workers without serialization.

    Returns:
        PagedMemoryPool: The shared read buffer instance
    """
    global _shared_read_buffer

    if _shared_read_buffer is None:
        with _buffer_lock:
            # Double-check pattern
            if _shared_read_buffer is None:
                try:
                    # Try to use PagedMemoryPool for better concurrency
                    from opteryx.compiled.structures.paged_memory_pool import PagedMemoryPool

                    _shared_read_buffer = PagedMemoryPool(name="Shared Read Buffer")
                except ImportError:
                    # Fall back to single MemoryPool if PagedMemoryPool not available
                    _shared_read_buffer = MemoryPool(MAX_READ_BUFFER_CAPACITY, "Shared Read Buffer")

    return _shared_read_buffer


async def fetch_data(manifest, pool, connector, reply_queue, telemetry, columns=None, filters=None):
    """
    Externalized async fetch loop.

    IMPORTANT: this expects the connector to implement `async_read_blob(...)`
    which is responsible for reading the blob and committing bytes into the
    provided `AsyncMemoryPool` (returning a pool reference). Connectors that
    don't implement `async_read_blob` will cause a read error for each blob.

    The connector provided should implement `async_read_blob(blob_name, pool, telemetry, **kwargs)`.

    Args:
        manifest: File manifest with list of blobs to read
        pool: AsyncMemoryPool to commit data to
        connector: Connector implementing async_read_blob
        reply_queue: Queue for returning (blob_name, reference) tuples
        telemetry: Telemetry object for tracking
        columns: Optional columns to project
        filters: Optional filters to push down (DNF format)
    """
    import aiohttp

    semaphore = asyncio.Semaphore(CONCURRENT_READS)

    # Create aiohttp session for filesystem readers that need it
    async with aiohttp.ClientSession() as http_session:

        async def fetch_and_process(blob_name):
            async with semaphore:
                start_per_blob = time.monotonic_ns()
                try:
                    # Call the async reader. We pass the commonly-used args; filesystem
                    # readers that require other parameters should handle defaults or
                    # raise a clear exception which we'll forward.
                    # Try with session and statistics for filesystem readers
                    reference = await connector.async_read_blob(
                        blob_name=blob_name,
                        pool=pool,
                        session=http_session,
                        telemetry=telemetry,
                        columns=columns,
                        filters=filters,
                    )
                    if reference is not None:
                        reply_queue.put((blob_name, reference))
                except Exception as err:
                    # Pass the exception back so the reader loop can handle it
                    reply_queue.put((blob_name, err))
                finally:
                    telemetry.time_reading_blobs += time.monotonic_ns() - start_per_blob

        tasks = [fetch_and_process(blob) for blob in manifest.get_file_paths()]
        await asyncio.gather(*tasks, return_exceptions=True)
        reply_queue.put(None)


class AsyncReadNode(ReaderNode):
    def __init__(self, properties: QueryProperties, **parameters):
        ReaderNode.__init__(self, properties=properties, **parameters)
        # Use the shared, persistent read buffer instead of creating per-node pools
        self.pool = get_shared_read_buffer()

        self.predicates = parameters.get("predicates")

        self.rows_seen = 0
        self.blobs_seen = 0

    @property
    def name(self):  # pragma: no cover
        """friendly name for this step"""
        return "Async Read"

    def execute(self, morsel, **kwargs) -> Generator:
        if morsel == EOS:
            yield None
            return

        from opteryx.connectors.io_systems import create_filesystem

        # Perform this step, time how long is spent doing work
        orso_schema = self.parameters["schema"]

        # Instantiate filesystem from protocol
        if self.manifest and self.manifest.get_file_count() > 0:
            file_zero = self.manifest.files[0].file_path
            protocol = file_zero.split("://")[0] if "://" in file_zero else "file"
            reader = create_filesystem(protocol)
        else:
            reader = create_filesystem("file")

        orso_schema_cols = []
        for col in orso_schema.columns:
            if col.identity in [c.schema_column.identity for c in self.columns]:
                orso_schema_cols.append(col)
        orso_schema.columns = orso_schema_cols

        self.readings["columns_read"] += len(orso_schema.columns)

        if self.manifest.get_file_count() == 0:
            # if we don't have any matching blobs, create an empty dataset
            from orso import DataFrame

            as_arrow = DataFrame(rows=[], schema=orso_schema).arrow()
            renames = [orso_schema.column(col).identity for col in as_arrow.column_names]
            as_arrow = as_arrow.rename_columns(renames)
            yield as_arrow

        data_queue: queue.Queue = queue.Queue()

        loop = asyncio.new_event_loop()
        read_thread = threading.Thread(
            target=lambda: loop.run_until_complete(
                fetch_data(
                    self.manifest,
                    AsyncMemoryPool(self.pool),
                    reader,
                    data_queue,
                    self.telemetry,
                    columns=self.columns,
                    filters=self.predicates,
                )
            ),
            daemon=True,
        )
        read_thread.start()

        morsel = None
        arrow_schema = convert_orso_schema_to_arrow_schema(orso_schema, use_identities=True)
        records_to_read = self.limit if self.limit is not None else float("inf")

        while True:
            try:
                # Attempt to get an item with a timeout.
                item = data_queue.get(timeout=0.1)
            except queue.Empty:
                # Increment stall count if the queue is empty (engine waiting on data).
                self.telemetry.stalls_engine_waiting_on_data += 1
                self.readings["stalls_engine_waiting_on_data"] += 1
                self.telemetry.io_wait_seconds += 0.1
                continue  # Skip the rest of the loop and try to get an item again.

            if item is None:
                # Break out of the loop if the item is None, indicating a termination condition.
                break

            blob_name, reference = item
            decoder = get_decoder(blob_name)

            # If the connector signalled an exception, handle it here rather than
            # treating the exception object as a pool reference.
            if isinstance(reference, Exception):
                import warnings

                self.telemetry.add_message(
                    f"failed to read {blob_name} ({reference.__class__.__name__})"
                )
                self.readings["failed_reads"] += 1
                warnings.warn(f"failed to read {blob_name} - {reference}")
                continue

            # Some connectors may return a tuple (ref, filters_applied). Support both styles.
            filters_applied = False
            if isinstance(reference, tuple):
                reference, filters_applied = reference

            try:
                # the sync readers include the decode time as part of the read time
                try:
                    # zero copy reduces copy overhead, but we need to latch the segment
                    # to ensure it is not overwritten while we are reading it.
                    start = time.monotonic_ns()
                    blob_memory_view = self.pool.read(
                        reference, zero_copy=ENABLE_ZERO_COPY, latch=ENABLE_ZERO_COPY
                    )
                    self.telemetry.bytes_read += len(blob_memory_view)
                    self.readings["bytes_read"] += len(blob_memory_view)
                    selection_to_pass = [] if filters_applied else self.predicates
                    decoded = decoder(
                        blob_memory_view, projection=self.columns, selection=selection_to_pass
                    )

                    self.pool.release(reference)  # release also unlatches the segment
                except Exception as err:
                    from pyarrow import ArrowInvalid

                    if isinstance(err, ArrowInvalid) and "No match for" in str(err):
                        raise DataError(f"Unable to read blob {blob_name}")
                    raise DataError(f"Unable to read blob {blob_name} - error {err}") from err
                self.readings["time_reading_blobs"] += time.monotonic_ns() - start
                num_rows, _, raw_bytes, morsel = decoded
                self.readings["rows_seen"] += num_rows

                if records_to_read < morsel.num_rows:
                    morsel = morsel.slice(0, records_to_read)
                    records_to_read = 0
                else:
                    records_to_read -= morsel.num_rows

                morsel = struct_to_jsonb(morsel)
                morsel = normalize_morsel(orso_schema, morsel)
                if morsel.column_names != ["*"]:
                    morsel = morsel.cast(arrow_schema)

                self.readings["blobs_read"] += 1
                self.readings["rows_read"] += morsel.num_rows
                self.readings["bytes_processed"] += morsel.nbytes
                self.readings["bytes_raw"] += raw_bytes

                self.readings["rows_seen"] += num_rows
                self.readings["blobs_seen"] += 1

                yield morsel

                if records_to_read <= 0:
                    break
            except Exception as err:
                # Report and accumulate failure stats
                self.telemetry.add_message(f"failed to read {blob_name} ({err.__class__.__name__})")
                self.readings["failed_reads"] += 1
                import warnings

                warnings.warn(f"failed to read {blob_name} - {err}")

        # Ensure the thread is closed
        read_thread.join()

        if morsel is None:
            self.readings["empty_datasets"] += 1
            arrow_schema = convert_orso_schema_to_arrow_schema(orso_schema, use_identities=True)
            yield pyarrow.Table.from_arrays(
                [pyarrow.array([]) for _ in arrow_schema], schema=arrow_schema
            )
