# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.


class Diachronic:
    """Capability for connectors that support diachronic (time-travel) reads.

    This capability enables connectors to support time-travel queries via the AT syntax,
    allowing queries to read data as it existed at a specific point in time.
    """

    partitioned = True

    def __init__(self, **kwargs):
        self.at_date = kwargs.get("at_date")
        # A snapshot id, or 0 meaning "the parent of the current snapshot" - see
        # extract_timetravel_version. None unless the query used VERSION AS OF.
        self.version = kwargs.get("version")
