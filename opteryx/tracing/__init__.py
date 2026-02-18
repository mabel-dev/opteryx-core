# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Opteryx IO Layer Tracing System

Provides low-overhead event recording for tracking IO operations (file discovery,
downloading, buffering, decoding) using # TRACE: comments in source code.

When OPTERYX_TRACE=1, the import system removes these comments and events are recorded.
When disabled, the comments remain and have zero overhead.
"""

from opteryx.tracing.event_recorder import flush_all
from opteryx.tracing.event_recorder import record_event

__all__ = ["record_event", "flush_all"]
