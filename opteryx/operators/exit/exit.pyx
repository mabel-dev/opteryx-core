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
Exit Node

This is a SQL Query Execution Plan Node.

This does the final preparation before returning results to users.

This does two things that the projection node doesn't do:
    - renames columns from the internal names
    - removes all columns not being returned to the user

This node doesn't do any calculations, it is a pure Projection.

Execution is 100% native (see opteryx/managers/execution/compiler.py's
_compile_plan, which reads `.final_columns`/`.final_names`/`.columns` off this
class and wires the native queue sink directly — results are drained from a
PyMorselQueue, not from this class). This class is plan-time config only.
"""

from opteryx.exceptions import AmbiguousIdentifierError

# BasePlanNode in scope via textual include from _operators.pyx.


cdef class ExitNode(BasePlanNode):
    cdef public list final_columns
    cdef public list final_names

    def __init__(self, properties=None, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)

        final_columns = []
        final_names = []
        for column in self.columns:
            final_columns.append(column.schema_column.identity)
            final_names.append(column.alias)

        # A result with two columns sharing an output name is ambiguous — reject it.
        # Note: once column identities became genuinely unique, `SELECT *` over a join
        # of relations that share a column name produces distinct columns with the same
        # name; per architect decision this errors here (rather than emit duplicate
        # names or auto-suffix). Queries must qualify/alias such columns explicitly.
        if len(set(final_names)) != len(final_names):
            from collections import Counter

            duplicates = [name for name, count in Counter(final_names).items() if count > 1]
            raise AmbiguousIdentifierError(
                message=f"Query result contains multiple instances of the same column(s) - `{'`, `'.join(duplicates)}`"
            )

        self.final_columns = list(final_columns)
        self.final_names = final_names

    cdef BasePlanNode make_worker(self):
        # SPEC: final_columns/final_names (the validated output identities + names —
        # the ambiguous-name check ran once at __init__, not re-run per worker).
        cdef ExitNode w = ExitNode.__new__(ExitNode)
        self._copy_worker_base(w)
        w.final_columns = self.final_columns
        w.final_names = self.final_names
        return w

    @property
    def config(self):  # pragma: no cover
        return None

    @property
    def name(self):  # pragma: no cover
        return "Exit"
