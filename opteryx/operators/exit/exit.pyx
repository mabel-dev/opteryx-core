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
"""

from typing import Generator, Optional
from collections.abc import Iterable

from opteryx.exceptions import AmbiguousIdentifierError
from opteryx.exceptions import InvalidInternalStateError
from opteryx.models import QueryProperties

from opteryx import EOS

from . import BasePlanNode


class ExitNode(BasePlanNode):

    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.at_least_one = False

        final_columns = []
        final_names = []
        for column in self.columns:
            final_columns.append(column.schema_column.identity)
            final_names.append(column.alias)

        if len(final_columns) != len(set(final_columns)):  # pragma: no cover
            from collections import Counter

            duplicates = [column for column, count in Counter(final_columns).items() if count > 1]
            matches = {a for a, b in zip(final_names, final_columns) if b in duplicates}
            raise AmbiguousIdentifierError(
                message=f"Query result contains multiple instances of the same column(s) - `{'`, `'.join(matches)}`"
            )

        if len(set(final_names)) != len(final_names):  # we have duplicate names
            final_names = []
            for column in self.columns:
                # if column.schema_column.origin:
                #    final_names.append(f"{column.schema_column.origin[0]}.{column.current_name}")
                # else:
                final_names.append(column.alias)

        # identity is already bytes; no encode needed
        self.final_columns = list(final_columns)
        self.final_names = final_names

    @property
    def config(self):  # pragma: no cover
        return None

    @property
    def name(self):  # pragma: no cover
        return "Exit"

    def execute(self, Morsel morsel):
        """Execute exit node: Draken-native column projection.

        The query engine (motor) is Draken-native throughout. Exit node formats results
        for the cursor layer, which is responsible for converting to the user's desired
        output format (Arrow, JSON, CSV, MessagePack, etc).
        """

        # Exit doesn't return EOS
        if morsel == EOS:
            if not self.at_least_one:
                # Return empty Draken morsel with correct schema
                from draken.interop.vector_sequence import vector_from_sequence

                # Create empty vectors with correct types
                vectors = []
                for _ in self.columns:
                    # Empty vector with correct type info
                    vectors.append(vector_from_sequence([]))

                morsel = Morsel.from_vectors(self.final_names, vectors)
                yield morsel

            return

        # Handle both single Morsel and Iterable of Morsels (from streaming)
        if isinstance(morsel, Morsel):
            morsels = (morsel,)
        elif isinstance(morsel, Iterable):
            morsels = morsel
        else:  # pragma: no cover
            yield None
            return

        for chunk in morsels:
            if chunk is EOS or chunk.num_rows == 0:
                continue

            self.at_least_one = True

            # Column validation on morsel
            morsel_column_names = chunk.column_names
            if not set(self.final_columns).issubset(morsel_column_names):  # pragma: no cover
                mapping = {
                    name: int_name for name, int_name in zip(self.final_columns, self.final_names)
                }
                missing_references = {
                    mapping.get(ref): ref
                    for ref in self.final_columns
                    if ref not in morsel_column_names
                }

                raise InvalidInternalStateError(
                    f"The following fields were not in the resultset - {', '.join(missing_references.keys())}"
                )

            # column selection and renaming
            chunk = chunk.select(self.final_columns)
            chunk = chunk.rename(self.final_names)

            yield chunk
