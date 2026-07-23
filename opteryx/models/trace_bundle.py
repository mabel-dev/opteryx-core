# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The raw payload of one query's native execution trace.

See docs/EXECUTION_TRACING_DESIGN.md. Deliberately NOT part of QueryTelemetry:
telemetry is aggregates that exist for every query (bytes read, time executing);
a trace is an event stream that exists ONLY when OPTERYX_TRACE=1, produced by an
entirely different subsystem (the native span tracer, not the planner/executor's
running counters). Bolting it onto QueryTelemetry meant QueryTelemetry.as_dict()
had to explicitly exclude it — a sign it never belonged there. This object holds
exactly, and only, the trace: every field on it is meant to be read, so there is
no equivalent "excluded from as_dict()" special-casing here.

Populated as a side effect of execute_native()'s teardown
(opteryx/managers/execution/compiler.py) when tracing is armed; otherwise stays
at its reset() defaults (empty blob, no symbols). ``blob`` is a packed array of
fixed-layout span records — see opteryx.tracing.spans for the wire format and
the interpreter that turns it into something meaningful.
"""

from __future__ import annotations

from typing import Dict


class TraceBundle:
    """One query's raw trace payload: the span blob plus the id->name symbol
    tables needed to interpret it. Session.trace() is the read surface."""

    __slots__ = ("blob", "node_symbols", "file_symbols", "truncated", "host_info")

    def __init__(self):
        self.reset()

    def reset(self) -> None:
        self.blob: bytes = b""
        self.node_symbols: Dict[int, str] = {}
        self.file_symbols: Dict[int, str] = {}
        self.truncated: bool = False
        # "arch=...;host=..." identity of the process that captured this
        # trace — see native_trace_host_info(). Lets two trace bundles be
        # compared honestly (e.g. telling a genuine perf difference apart
        # from an ARM-vs-x86 difference) without out-of-band knowledge of
        # where each one came from.
        self.host_info: str = ""
