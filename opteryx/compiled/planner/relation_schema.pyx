# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: wraparound=False
# cython: boundscheck=False
# distutils: language = c++

"""A bound relation's schema: its name, its bound columns (rows of the query's
ColumnTable) and what is known of its size.

IMMUTABLE (architect ruling 2026-09-28, native plan graph P5): a plan step's native
row mirrors the schema it holds, so an edit is a NEW schema (`replace`,
`with_columns`) assigned through the step - and, in the binder, re-bound wherever
the old one was shared (BindingContext.rebind_schema). Equality and hashing are by
value, field by field, as they were for the dataclass this replaces.
"""

from libc.stdint cimport int64_t

from opteryx.compiled.planner.column_table cimport SchemaColumn

# One bit per optional count in `_present`.
cdef enum:
    ROW_COUNT_METRIC = 1
    ROW_COUNT_ESTIMATE = 2
    DATA_SIZE_METRIC = 4
    DATA_SIZE_ESTIMATE = 8

_FIELDS = (
    "name",
    "columns",
    "aliases",
    "primary_key",
    "row_count_metric",
    "row_count_estimate",
    "data_size_metric",
    "data_size_estimate",
)


cdef inline int64_t _count(object value, str field) except? -1:
    """A count field: None (unknown) or an int. Anything else is refused rather
    than truncated."""
    if type(value) is not int:
        raise TypeError(f"RelationSchema.{field} is an int or None, not {type(value).__name__}.")
    return <int64_t>value


cdef class LazyColumns:
    """A scan's columns, minted on first use. ONE holder per bound relation, shared by
    every clone of its schema, so a column is minted once whichever clone asks."""

    def __cinit__(self, table, str alias, tuple descriptor):
        cdef Py_ssize_t i
        cdef str name
        self.table = table
        self.alias = alias
        self.descriptor = descriptor
        self.minted = [None] * len(descriptor)
        self.pending = len(descriptor)
        self.exact = {}
        self.lower = {}
        for i in range(len(descriptor)):
            name = descriptor[i].name
            self.exact.setdefault(name, i)
            self.lower.setdefault(name.lower(), i)

    cdef object mint(self, Py_ssize_t position):
        column = self.minted[position]
        if column is None:
            if self.frozen:
                from opteryx.exceptions import InvalidInternalStateError

                raise InvalidInternalStateError(
                    f"Column {self.descriptor[position].name!r} of {self.alias!r} was first referenced "
                    "after binding finished - its scan no longer carries it."
                )
            source = self.descriptor[position]
            column = self.table.relation_column(
                self.alias,
                source.name,
                column_type=source.column_type,
                nullable=source.nullable,
                field_id=source.field_id,
                origin=(self.alias,),
            )
            self.minted[position] = column
            self.pending -= 1
        return column

    cdef tuple all(self):
        cdef Py_ssize_t i
        if self.pending:
            for i in range(len(self.descriptor)):
                self.mint(i)
        return tuple(self.minted)


cdef class RelationSchema:
    """Table/relation schema definition.

    Attributes:
        name: Schema/table name (required)
        columns: bound SchemaColumns (a tuple)
        aliases: Alternative names for this schema (a tuple)
        primary_key: Name of primary key column (default: None)
        row_count_metric: Actual row count if known (default: None)
        row_count_estimate: Estimated row count (default: None)
        data_size_metric: Actual data size in bytes (default: None)
        data_size_estimate: Estimated data size in bytes (default: None)
    """

    def __init__(
        self,
        str name,
        columns=(),
        aliases=(),
        str primary_key=None,
        row_count_metric=None,
        row_count_estimate=None,
        data_size_metric=None,
        data_size_estimate=None,
    ):
        cdef SchemaColumn column
        self.name = name
        self._columns = tuple(columns)
        self._lazy = None
        # every column is a bound column: the typed assignment refuses anything else
        for column in self._columns:
            pass
        self.aliases = tuple(aliases)
        self.primary_key = primary_key
        self._present = 0
        if row_count_metric is not None:
            self._row_count_metric = _count(row_count_metric, "row_count_metric")
            self._present |= ROW_COUNT_METRIC
        if row_count_estimate is not None:
            self._row_count_estimate = _count(row_count_estimate, "row_count_estimate")
            self._present |= ROW_COUNT_ESTIMATE
        if data_size_metric is not None:
            self._data_size_metric = _count(data_size_metric, "data_size_metric")
            self._present |= DATA_SIZE_METRIC
        if data_size_estimate is not None:
            self._data_size_estimate = _count(data_size_estimate, "data_size_estimate")
            self._present |= DATA_SIZE_ESTIMATE
        self._lowercase = None

    @staticmethod
    def lazy(table, str alias, descriptor):
        """`descriptor` bound as relation `alias`, each column minted on first use."""
        cdef RelationSchema new = RelationSchema(
            descriptor.name,
            (),
            list(descriptor.aliases),
            descriptor.primary_key,
            descriptor.row_count_metric,
            descriptor.row_count_estimate,
            descriptor.data_size_metric,
            descriptor.data_size_estimate,
        )
        new._lazy = LazyColumns(table, alias, tuple(descriptor.columns))
        return new

    @property
    def columns(self):
        if self._lazy is not None:
            if self._lazy.frozen:
                self._columns = self.minted_columns()
                self._lazy = None
            else:
                self._force()
        return self._columns

    @property
    def is_lazy(self):
        return self._lazy is not None

    def freeze(self):
        """Binding is over: this scan carries the columns minted so far, and no more."""
        if self._lazy is not None:
            self._lazy.frozen = True

    cdef void _force(self) except *:
        self._columns = self._lazy.all()
        self._lazy = None
        self._lowercase = None

    def minted_columns(self):
        """The columns minted so far - all of them on an eager schema."""
        if self._lazy is not None:
            return tuple(c for c in self._lazy.minted if c is not None)
        return self._columns

    cdef bint shares_columns(self, RelationSchema other):
        """Whether `other` holds this schema's columns by construction (a clone)."""
        if self._lazy is not None or other._lazy is not None:
            return self._lazy is other._lazy
        return self._columns is other._columns

    cpdef object find_lower(self, str lowered):
        """The first column whose lowercased name (or alias) is `lowered`, or None -
        minting only that column on a lazy schema."""
        if self._lazy is not None:
            position = self._lazy.lower.get(lowered)
            return None if position is None else self._lazy.mint(position)
        return self._lowercase_index().get(lowered)

    @property
    def row_count_metric(self):
        return self._row_count_metric if self._present & ROW_COUNT_METRIC else None

    @property
    def row_count_estimate(self):
        return self._row_count_estimate if self._present & ROW_COUNT_ESTIMATE else None

    @property
    def data_size_metric(self):
        return self._data_size_metric if self._present & DATA_SIZE_METRIC else None

    @property
    def data_size_estimate(self):
        return self._data_size_estimate if self._present & DATA_SIZE_ESTIMATE else None

    cdef RelationSchema _clone(self):
        cdef RelationSchema new = RelationSchema.__new__(RelationSchema)
        new.name = self.name
        new._columns = self._columns
        new._lazy = self._lazy
        new.aliases = self.aliases
        new.primary_key = self.primary_key
        new._row_count_metric = self._row_count_metric
        new._row_count_estimate = self._row_count_estimate
        new._data_size_metric = self._data_size_metric
        new._data_size_estimate = self._data_size_estimate
        new._present = self._present
        new._lowercase = self._lowercase
        return new

    def clone(self) -> RelationSchema:
        """A distinct schema object of the same value; the fields are values, so
        the new object shares them."""
        return self._clone()

    def with_columns(self, columns) -> RelationSchema:
        """This schema with `columns` in place of its own - a new schema."""
        cdef SchemaColumn column
        cdef RelationSchema new = self._clone()
        new._columns = tuple(columns)
        new._lazy = None
        for column in new._columns:
            pass
        new._lowercase = None
        return new

    def with_appended(self, SchemaColumn column) -> RelationSchema:
        """This schema with `column` added after its own - a new schema. The
        lookup index carries over (extended by `column`'s name and aliases, which
        cannot displace an earlier column's) instead of being rebuilt on the next
        lookup: the binder appends to `$derived` once per computed column."""
        cdef str alias
        cdef dict index
        cdef RelationSchema new = self._clone()
        new._columns = self.columns + (column,)
        new._lazy = None
        if self._lowercase is not None:
            index = dict(self._lowercase)
            index.setdefault(column.name.lower(), column)
            if column.aliases:
                for alias in column.aliases:
                    index.setdefault(alias.lower(), column)
            new._lowercase = index
        return new

    def replace(self, **changes) -> RelationSchema:
        """This schema with `changes` (field name -> value) applied - a new schema,
        built and checked as any other. An unknown field name raises."""
        for field in changes:
            if field not in _FIELDS:
                raise TypeError(f"RelationSchema has no field {field!r}.")
        return RelationSchema(
            changes.get("name", self.name),
            changes.get("columns", self.columns),
            changes.get("aliases", self.aliases),
            changes.get("primary_key", self.primary_key),
            changes.get("row_count_metric", self.row_count_metric),
            changes.get("row_count_estimate", self.row_count_estimate),
            changes.get("data_size_metric", self.data_size_metric),
            changes.get("data_size_estimate", self.data_size_estimate),
        )

    def branch_copy(self, dict memo) -> RelationSchema:
        """A distinct schema object of the same value, for binder branch isolation.

        The binder binds a join's two legs (and a filter's guarded scope)
        against their own schema objects, because binding RE-BINDS a scope's
        schema (narrowing, alias rows swapped in) and BindingContext.rebind_schema
        replaces a schema by object IDENTITY wherever it is shared: a branch's
        object must not be the scan's, or a branch's narrowing would reach the
        scan. The columns themselves are shared - a column is a row of the
        query's ColumnTable. One shared `memo` per BindingContext.copy keeps a
        schema reachable under two keys copying to one new schema.
        """
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        clone = self._clone()
        memo[id(self)] = clone
        return clone

    def _field_values(self) -> tuple:
        return (
            self.name,
            self.columns,
            self.aliases,
            self.primary_key,
            self.row_count_metric,
            self.row_count_estimate,
            self.data_size_metric,
            self.data_size_estimate,
        )

    def __eq__(self, other):
        if type(other) is not RelationSchema:
            return NotImplemented
        return self._field_values() == (<RelationSchema>other)._field_values()

    def __hash__(self):
        return hash(self._field_values())

    def __reduce__(self):
        return (RelationSchema, self._field_values())

    def __str__(self) -> str:
        """String representation: schema_name(col1, col2, ...)."""
        col_list = ", ".join(str(c) for c in self.columns)
        return f"{self.name}({col_list})"

    def __repr__(self) -> str:
        return f"RelationSchema(name={self.name!r}, num_columns={self.num_columns})"

    @property
    def column_names(self) -> list:
        """Get list of all column names."""
        cdef SchemaColumn col
        if self._lazy is not None:
            return [source.name for source in self._lazy.descriptor]
        return [col.name for col in self.columns]

    @property
    def all_column_names(self) -> list:
        """Get all column names including aliases."""
        cdef SchemaColumn col
        names = []
        for col in self.columns:
            names.extend(col.all_names)
        return names

    @property
    def num_columns(self) -> int:
        if self._lazy is not None:
            return len(self._lazy.descriptor)
        return len(self._columns)

    cdef dict _lowercase_index(self):
        """Lowercased column name or alias -> the FIRST column carrying it (column
        order, a column's name before its aliases), built on first use. Sound
        because a column's name and aliases are fixed once minted and `columns`
        only changes through `with_columns`, which drops it."""
        cdef SchemaColumn col
        cdef str alias
        cdef dict index = self._lowercase
        if index is None:
            index = {}
            for col in self.columns:
                index.setdefault(col.name.lower(), col)
                if col.aliases:
                    for alias in col.aliases:
                        index.setdefault(alias.lower(), col)
            self._lowercase = index
        return index

    def column(self, str name, bint case_insensitive=False):
        """The column named `name` (by name, then aliases), or None."""
        cdef SchemaColumn col
        if case_insensitive:
            return self.find_lower(name.lower())
        if self._lazy is not None:
            position = self._lazy.exact.get(name)
            return None if position is None else self._lazy.mint(position)
        for col in self.columns:
            if col.name == name or (col.aliases and name in col.aliases):
                return col
        return None

    def find_column(self, str name, bint case_insensitive=False):
        """Alias for column() for API compatibility."""
        return self.column(name, case_insensitive)

    def without_column(self, str name) -> RelationSchema:
        """This schema without its first column named `name` - a new schema (or
        this one, when it has no such column)."""
        cdef SchemaColumn col
        cdef Py_ssize_t i = 0
        for col in self.columns:
            if col.name == name:
                return self.with_columns(self.columns[:i] + self.columns[i + 1 :])
            i += 1
        return self
