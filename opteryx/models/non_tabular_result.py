# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
"""
Non tabular results are usually the result of management tasks, like CREATE or DELETE
they tend to be the result of actions that operate on the structure of the data rather
than data elements themselves.
"""

from opteryx.constants import QueryStatus


class NonTabularResult:
    """
    Class to encapsulate non-tabular query results.
    """

    def __init__(self, record_count: int = None, status: QueryStatus = QueryStatus._UNDEFINED):
        self.record_count = record_count
        self.status = status
