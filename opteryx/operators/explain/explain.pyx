# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Explain Node

This is a SQL Query Execution Plan Node.

This writes out a query plan
"""

from typing import Generator, Optional
from opteryx.models import QueryProperties

# BasePlanNode/JoinNode in scope via _operators.pyx include.


class ExplainNode(BasePlanNode):

    def __init__(self, properties: QueryProperties, step):
        BasePlanNode.__init__(self, properties, step, step.columns, step.pre_update_columns)
        self.analyze = bool(step.analyze)
        self.format = step.format or "TEXT"

    @property
    def name(self):  # pragma: no cover
        return "Explain"

    @property  # pragma: no cover
    def config(self):
        return ""

