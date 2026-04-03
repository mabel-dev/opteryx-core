# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Edge-based execution model for Opteryx.

This module provides a prototype implementation of a three-state pipeline execution
model that separates data flow (edges) from control signals (state transitions).

The model is self-contained but uses real Opteryx constructs and is designed to
eventually integrate with the serial engine.
"""

from opteryx.execution.edge import Edge
from opteryx.execution.scheduler import Scheduler

__all__ = [
    "Edge",
    "Scheduler",
]
