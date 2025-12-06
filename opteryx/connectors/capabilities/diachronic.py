# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.


class Diachronic:
    """Capability for connectors that support diachronic (time-travel) reads.

    Historically this capability was named `Partitionable`; it stores a partition
    scheme and optional start/end date attributes used by connectors to support
    date-range reads and time-travel queries.
    """

    partitioned = True

    def __init__(self, **kwargs):
        self.start_date = None
        self.end_date = None
