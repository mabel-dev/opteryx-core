# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Native execution-trace interpretation (docs/EXECUTION_TRACING_DESIGN.md).

``Session.trace()`` is the capture/retrieval surface: it returns the raw
``(blob, node_symbols, file_symbols)`` bundle for a query run with
``OPTERYX_TRACE=1``, produced natively (Cython/C++) since it records
per-operator and per-IO-request timing from the GIL-free execution engine.

This package is the interpretation surface: ``interpret_trace()`` turns that
raw bundle into a flat, JSON-serializable list of resolved span dicts. A
consumer that only needs to PERSIST a trace (e.g. a worker service uploading
it alongside a query's results) never needs to import this — it can treat the
bundle as opaque bytes + two small dicts. A consumer that wants to actually
look at a trace calls interpret_trace().

A prior version of this package held a completely different, coarser
mechanism (dataset/file-discovery events keyed by session id, "Session.trace()
yields those events" instead of the span bundle). It added no real diagnostic
value over the native span waterfall and has been removed.
"""

from opteryx.tracing.spans import CATEGORY_NAMES
from opteryx.tracing.spans import interpret_trace
from opteryx.tracing.spans import parse_spans
from opteryx.tracing.timelines import TraceTimelines

__all__ = ["interpret_trace", "parse_spans", "CATEGORY_NAMES", "TraceTimelines"]
