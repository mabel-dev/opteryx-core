# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Configuration for the IO tracing system.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class TraceConfig:
    """Configuration for IO layer tracing."""

    # Master switches
    enabled: bool = False
    """Whether tracing is enabled."""

    output_file: Optional[str] = None
    """Path to write trace file. None = no tracing."""

    # Performance tuning
    buffer_size_per_thread: int = 10000
    """Size of ring buffer per thread. Larger = fewer flushes but more memory."""

    flush_interval_ms: int = 100
    """Milliseconds between background flushes."""

    sample_rate: float = 1.0
    """Sampling rate: 1.0 = 100%, 0.1 = 10% of files traced."""

    # Selective tracing
    min_file_size_bytes: int = 0
    """Minimum file size to trace (0 = all files)."""

    max_file_size_bytes: int = 0
    """Maximum file size to trace (0 = unlimited)."""

    # What to record
    include_file_sizes: bool = True
    """Include file size in events."""

    include_decode_stats: bool = True
    """Include decode statistics (rows, batches)."""

    include_metadata: bool = True
    """Include session metadata events."""

    # Cleanup
    max_trace_file_mb: int = 1000
    """Rotate trace file if larger than this."""

    compress_older_than_hours: int = 1
    """Auto-compress trace files older than this many hours."""
