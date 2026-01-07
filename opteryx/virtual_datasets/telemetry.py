# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
This is a virtual dataset which is calculated at access time.
"""

import time

from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.types import OrsoTypes

__all__ = ("read", "schema")


def read(at_date=None, variables=None):
    import pyarrow

    variables = variables or {}

    from opteryx import system_telemetry  # type: ignore[attr-defined]

    # fmt:off
    buffer = [
        {"key": "queries_executed", "value": str(system_telemetry.queries_executed)},
        {"key": "uptime_seconds","value": str((time.time_ns() - system_telemetry.start_time) / 1e9)},
        {"key": "io_wait_seconds", "value": str(system_telemetry.io_wait_seconds)},
        {"key": "cpu_wait_seconds", "value": str(system_telemetry.cpu_wait_seconds)},
        {"key": "origin_reads", "value": str(system_telemetry.origin_reads)},
    ]
    # fmt:on

    return pyarrow.Table.from_pylist(buffer)


def schema():
    # fmt:off
    return  RelationSchema(
        name="$telemetry",
        columns=[
            FlatColumn(name="key", type=OrsoTypes.VARCHAR),
            FlatColumn(name="value", type=OrsoTypes.VARCHAR),
        ],
    )
    # fmt:on
