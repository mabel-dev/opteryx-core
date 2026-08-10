# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Optional


class LogicalColumn:
    """
    Represents a logical column in the binding phase, tied to schema columns later.

    Parameters:
        source_column: str
            The original name of the column in its logical source (e.g., table, subquery).
        source: str
            The originating logical source for the column.
        alias: Optional[str]
            A temporary name assigned in the SQL query for the column, defaults to None.
        is_outer_reference: bool
            True when this reference resolved to an ENCLOSING query's scope rather
            than the local one — i.e. it is what makes a subquery correlated. Set by
            the binder (which is the only thing that knows), and read by
            decorrelation to orient the correlation predicate.
        outer_relation:
            The schema this reference resolved against when `is_outer_reference`.
        span: Optional[Tuple[int, int, int, int]]
            Where this name was written, as (start_line, start_column, end_line,
            end_column), 1-based, indexing the statement the PARSER was given.
            sqlparser hangs one off every identifier; it is captured here so an error
            about this column can point at it instead of describing it. None when the
            column was synthesized rather than written (a wildcard expansion, a
            rewritten predicate, a plan the optimizer built).

            It is a plain tuple, not the parser's nested dict, because this is carried
            on every column in every plan and copied with them - the dict is four
            allocations to say the same thing.

    NOTE: `__getattr__` returns None for anything unset, so an ad-hoc attribute
    assigned from outside LOOKS like it works — but `copy()` rebuilds from the
    explicit field list below and silently drops it. Anything that must survive a
    plan copy has to be a real field here.
    """

    def __init__(
        self,
        node_type,
        source_column: str,
        source_connector: Optional[str] = None,
        source: Optional[str] = None,
        alias: Optional[str] = None,
        schema_column=None,
        query_column: Optional[str] = None,
        is_outer_reference: bool = False,
        outer_relation=None,
        span=None,
    ):
        self.node_type = node_type
        self.source_column = source_column
        self.source_connector = source_connector
        self.source = source
        self.alias = alias
        self.schema_column = schema_column
        self.query_column = query_column
        self.is_outer_reference = is_outer_reference
        self.outer_relation = outer_relation
        self.span = span

    @property
    def qualified_name(self) -> str:
        """
        Returns the fully qualified column name based on the logical source and source_column.
        Return nothing as the table name if it's not set, 'None' may be a table name.

        Returns:
            The fully qualified column name as a string.
        """
        if self.source:
            return f"{self.source}.{self.source_column}"
        return f".{self.source_column}"

    @property
    def current_name(self) -> str:
        """
        Returns the current name of the column, considering any alias.

        Returns:
            The current name of the column as a string.
        """
        return self.alias or self.source_column

    @property
    def value(self) -> str:
        return self.current_name

    def __getattr__(self, name: str):
        return None

    def copy(self):
        return LogicalColumn(
            node_type=self.node_type,
            source_column=self.source_column,
            source_connector=self.source_connector,
            source=self.source,
            alias=self.alias,
            schema_column=(
                None if self.schema_column is None else self.schema_column.to_schema_column()
            ),
            query_column=self.query_column,
            is_outer_reference=self.is_outer_reference,
            outer_relation=self.outer_relation,
            span=self.span,
        )

    def __repr__(self) -> str:
        return f"<LogicalColumn name: '{self.current_name}' fullname: '{self.qualified_name}'>"

    def __hash__(self):
        return hash(
            (
                self.node_type,
                self.source_column,
                self.source_connector,
                self.source,
                self.alias,
                self.schema_column.identity if self.schema_column else None,
            )
        )
