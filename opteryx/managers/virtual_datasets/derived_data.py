# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
derived
---------

This is used as the source relation for derived values.
"""

from opteryx.types.schema import RelationSchema

__all__ = ("schema",)


def schema():
    # Scratch space for computed expressions — never scanned as a relation.
    return RelationSchema(name="$derived", columns=[], row_count_metric=0)
