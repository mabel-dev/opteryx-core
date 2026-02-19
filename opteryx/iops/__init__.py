# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
opteryx.iops
============
IO Process Isolation Subsystem.

Provides a two-process architecture in which one OS process fetches blobs from
object storage (GCS) and another process performs CPU-bound Parquet decoding.
Blobs are passed between the processes via a shared-memory ring buffer, enabling
a zero-copy path from the network into the Arrow decoder.

Public API
----------
``IOPSReader``
    Context manager.  Manages the IO worker process and ring buffer lifetime.
    Use ``iter_blobs()`` to stream blob payloads in order.

``SlotPayload``
    A single blob payload ready for zero-copy decode.  ``payload.data`` is a
    ``memoryview`` of the shared-memory slot.  Call ``payload.release()`` once
    the data has been consumed.

``RingConfig``
    Frozen dataclass describing ring geometry (slot size, slot count, inflight
    limit, shared-memory name).

State constants
---------------
``FREE``, ``WRITING``, ``READY``, ``READING``
    Integer slot state values.  Exposed so callers can inspect ring diagnostics
    via ``ring.slot_state_counts()``.

Example
-------
::

    import pyarrow.parquet as pq
    import pyarrow as pa
    from opteryx.iops import IOPSReader, RingConfig

    cfg = RingConfig(slot_size=64 << 20, slot_count=16, max_inflight=8)
    blob_paths = ["opteryx_store/test/tweets/data/part-0.parquet", ...]

    with IOPSReader(protocol="gs", cfg=cfg) as reader:
        for payload in reader.iter_blobs(blob_paths):
            table = pq.read_table(pa.BufferReader(payload.data))
            payload.release()  # return slot to the ring immediately
            # ... process table ...
"""

from opteryx.iops.messages import SHUTDOWN
from opteryx.iops.messages import ReadComplete
from opteryx.iops.reader import IOPSReader
from opteryx.iops.reader import SlotPayload
from opteryx.iops.ring import FREE
from opteryx.iops.ring import READING
from opteryx.iops.ring import READY
from opteryx.iops.ring import WRITING
from opteryx.iops.ring import RingConfig

__all__ = [
    # Core API
    "IOPSReader",
    "SlotPayload",
    "RingConfig",
    # Message types (useful for type annotations / testing)
    "ReadComplete",
    "SHUTDOWN",
    # Slot state constants
    "FREE",
    "WRITING",
    "READY",
    "READING",
]
