# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: wraparound=False
# cython: boundscheck=False
# distutils: language = c++

"""One query's bound columns: native rows (src/cpp/planner/column_table.hpp) and
the canonical Python façade of each.

Every bound column of a query is minted here, and only here (architect ruling
2026-09-26, option C): a source describes its columns without identities and the
query - through this table, reached by every phase from the AST builders to the
compiler as `PlanContext.columns` - mints each column's identity and slot. The
identity is the engine's column key (random bytes with a traceable prefix); the
slot is the column's row number in its query.

Native plan graph P1 (architect rulings 2026-09-27):
  - a row holds everything the column is, and is FIXED once minted: a column that
    differs - renamed in another scope (`alias`), retyped (`retype`), standing in
    for another (`adopt`), read under another name and type (`reference`) - is a
    NEW row whose `alias_of` names the row it derives from, and which keeps that
    row's identity (the stream key);
  - a column that is a new column of a relation takes a new identity (`remint`);
  - each row has ONE façade object (`SchemaColumn` or a kind subclass) - copying
    a column returns it - whose Python views (name, identity, aliases, origin)
    were built at mint, when the row was; `aliases`/`origin` are tuples.
"""

cimport cython
from libc.stdint cimport int64_t
from libc.stdint cimport uint8_t
from libc.stdint cimport uint32_t
from libcpp cimport bool as cbool
from libcpp.string cimport string
from libcpp.utility cimport move
from libcpp.vector cimport vector

from opteryx.compiled.planner.column_type cimport ColumnType

from opteryx.exceptions import InvalidInternalStateError
from opteryx.utils import random_string


def mint_column_identity(relation, column) -> bytes:
    """Mint a unique, opaque column identity with a traceable prefix.

    Identities are the engine's per-column handles; they MUST be unique (the
    name is not — two relations can share a column name). The random suffix
    guarantees uniqueness; the ``rel_col_`` prefix is a debugging affordance so
    that an identity leaked into an error/stack trace can be traced back to a
    physical column. The query's ColumnTable is the only caller.
    """
    rel = (relation or "")[:3]
    col = (column or "")[:3]
    return f"{rel}_{col}_{random_string(8)}".encode("utf-8")


cdef bytes _mint_tagged_identity(str tag):
    """A unique identity for a non-relation column (`$const`, `$derived`)."""
    return f"{tag}_{random_string(8)}".encode("utf-8")


@cython.auto_pickle(False)  # a row belongs to its query
cdef class SchemaColumn:
    """A bound column: the canonical façade of one row of its query's ColumnTable.

    Minted by the table, never constructed directly, and fixed once minted. The
    attributes are the row's Python views, built when the row was.
    """

    def __init__(self, *args, **kwargs):
        raise InvalidInternalStateError(
            f"{type(self).__name__} was constructed directly - bound columns are minted "
            "by the query's ColumnTable (PlanContext.columns), never constructed."
        )

    @property
    def field_id(self):
        """Stable, catalog-assigned column identifier (Iceberg-style field-id),
        distinct from `identity` (a random, per-query handle). Keys per-file
        manifest statistics so they survive schema evolution; None for sources
        with no catalog-assigned id."""
        if self._has_field_id:
            return self._field_id
        return None

    @property
    def category(self):
        """Operator-dispatch category projection of `column_type` (the one type
        carrier); None when no type is resolved yet."""
        if self.column_type is None:
            return None
        return self.column_type.category

    @property
    def all_names(self):
        """All names for this column (name + aliases)."""
        names = [self.name]
        if self.aliases:
            names.extend(self.aliases)
        return names

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self

    def __str__(self):
        if self.column_type is not None:
            return f"{self.name}:{self.column_type}"
        return self.name

    def __repr__(self):
        return f"{type(self).__name__}(name={self.name!r}, column_type={self.column_type}, nullable={self.nullable})"


cdef class ConstantColumn(SchemaColumn):
    """A literal's column. `value` is the literal - a Python value native code
    never reads (architect ruling 2026-09-24: Python-only attributes live beside
    the row)."""

    cdef readonly object value

    def __str__(self):
        return f"{self.name}={self.value}"


cdef class FunctionColumn(SchemaColumn):
    """A function call's column."""

    def __str__(self):
        return f"{self.name}(computed)"


cdef class ExpressionColumn(SchemaColumn):
    """A computed expression or predicate's column."""


_KEEP = object()  # alias()/adopt(): "keep the source column's type"

_COLUMN_FIELDS = frozenset(("column_type", "nullable", "field_id", "aliases", "origin", "value"))


cdef uint8_t _kind_of(type cls) except 255:
    if cls is SchemaColumn:
        return COLUMN_PLAIN
    if cls is ConstantColumn:
        return COLUMN_CONSTANT
    if cls is FunctionColumn:
        return COLUMN_FUNCTION
    if cls is ExpressionColumn:
        return COLUMN_EXPRESSION
    raise TypeError(f"{cls.__name__} is not a column kind")


cdef tuple _names(value):
    """A caller's name sequence as the row stores it."""
    if value is None:
        return None
    if type(value) is tuple:
        return <tuple>value
    return tuple(value)


cdef dict _fields_of(SchemaColumn column):
    """Everything `column`'s row is, beyond its name/identity/slot, as mint fields."""
    fields = {
        "column_type": column.column_type,
        "nullable": column.nullable,
        "field_id": column.field_id,
        "aliases": column.aliases,
        "origin": column.origin,
    }
    if type(column) is ConstantColumn:
        fields["value"] = (<ConstantColumn>column).value
    return fields


@cython.auto_pickle(False)
@cython.final
cdef class ColumnTable:
    """Every bound column of one query, in the order it was minted — a column's
    `slot` is its position here. See the module docstring."""

    def __cinit__(self):
        self._columns = []
        self._slot_of = {}

    def __len__(self):
        return self._rows.size()

    def alias_of(self, uint32_t slot):
        """The slot `slot` derives from, or None for a root slot."""
        cdef uint32_t source = self._rows.row(slot).alias_of
        if source == kNoSlot:
            return None
        return source

    def root_slot(self, uint32_t slot):
        """The root of `slot`'s derivation chain - the slot that minted its identity."""
        return self._rows.root(slot)

    cdef SchemaColumn _mint(self, type cls, str name, bytes identity, dict fields, uint32_t alias_of):
        # `name` is never None here: every public mint takes it `not None`.
        """Append the row, and make its façade. The one constructor of a column."""
        cdef ColumnRow row
        cdef SchemaColumn column
        cdef uint8_t kind = _kind_of(cls)
        cdef str part

        unknown = fields.keys() - _COLUMN_FIELDS
        if unknown:
            raise TypeError(f"unknown column field(s) {sorted(unknown)} for {name!r}")
        if "value" in fields and kind != COLUMN_CONSTANT:
            raise TypeError(f"only a ConstantColumn carries a value ({name!r})")

        column_type = fields.get("column_type")
        if column_type is not None and type(column_type) is not ColumnType:
            raise TypeError(
                f"column {name!r} column_type must be a ColumnType; got {type(column_type).__name__}"
            )
        aliases = _names(fields.get("aliases"))
        origin = _names(fields.get("origin"))
        field_id = fields.get("field_id")

        column = <SchemaColumn>cls.__new__(cls)
        column.name = name
        column.identity = identity
        column.aliases = aliases
        column.origin = origin
        column.nullable = fields.get("nullable", True)
        column.column_type = column_type
        column._has_field_id = field_id is not None
        column._field_id = field_id if field_id is not None else 0
        if kind == COLUMN_CONSTANT:
            (<ConstantColumn>column).value = fields.get("value")

        row.name = name.encode("utf-8")
        row.identity = identity
        row.has_aliases = aliases is not None
        if aliases is not None:
            for part in aliases:
                row.aliases.push_back(part.encode("utf-8"))
        row.has_origin = origin is not None
        if origin is not None:
            for part in origin:
                row.origin.push_back(part.encode("utf-8"))
        row.nullable = column.nullable
        row.has_field_id = column._has_field_id
        row.field_id = column._field_id
        row.type_id = kNoColumnType if column_type is None else (<ColumnType>column_type).type_id
        row.kind = kind
        row.alias_of = alias_of

        column.slot = self._rows.append(move(row))
        self._columns.append(column)
        if alias_of == kNoSlot:
            self._slot_of[identity] = column.slot
        return column

    def relation_column(self, relation, str name not None, **fields):
        """A column read from (or produced as) `relation`: identity `rel_col_…`."""
        return self._mint(SchemaColumn, name, mint_column_identity(relation, name), fields, kNoSlot)

    def constant(self, str name not None, **fields):
        """A constant (literal) column: identity `$const_…`."""
        return self._mint(ConstantColumn, name, _mint_tagged_identity("$const"), fields, kNoSlot)

    def computed(self, type column_class, str name not None, **fields):
        """A computed column (a FunctionColumn or ExpressionColumn): identity
        `$derived_…`."""
        return self._mint(column_class, name, _mint_tagged_identity("$derived"), fields, kNoSlot)

    def remint(self, SchemaColumn column not None, relation, **fields):
        """A copy of `column` that is a NEW column of `relation`: same row except
        `fields`, fresh identity and slot."""
        merged = _fields_of(column)
        merged.update(fields)
        return self._mint(
            type(column), column.name, mint_column_identity(relation, column.name), merged, kNoSlot
        )

    def alias(self, SchemaColumn column not None, str name not None, *, aliases=(), origin=None, column_type=_KEEP):
        """`column` seen under another name in another scope: a NEW slot with its
        own name set, the same identity (the stream key), kind and type facts, and
        `alias_of` pointing at `column`'s slot.

        `column_type` retypes the alias - a set operation's output settled to the
        type its legs were coerced to (a retype is a retyped alias row, so each
        slot keeps one type)."""
        fields = _fields_of(column)
        fields["aliases"] = aliases
        fields["origin"] = origin
        if column_type is not _KEEP:
            fields["column_type"] = column_type
        return self._mint(type(column), name, column.identity, fields, column.slot)

    def retype(self, SchemaColumn column not None, column_type):
        """`column` settled to `column_type`: a retyped alias row with `column`'s
        identity and names. A column already of `column_type` is returned as is:
        it is already that row."""
        if column.column_type == column_type:
            return column
        return self.alias(
            column, column.name, aliases=column.aliases, origin=column.origin, column_type=column_type
        )

    def reference(self, bytes identity not None, str name not None, column_type):
        """The already-minted column `identity` read under `name` and `column_type`
        (e.g. a join key the compiler casts): an alias row of the column that
        minted the identity. An identity this query never minted is refused: it
        would be a column from some other binding."""
        slot = self._slot_of.get(identity)
        if slot is None:
            raise InvalidInternalStateError(
                f"Column {name!r} ({identity!r}) was not minted in this query's column table."
            )
        return self.alias(
            self._columns[slot], name, aliases=None, origin=None, column_type=column_type
        )

    def adopt(self, SchemaColumn column not None, SchemaColumn owner not None, *, column_type=_KEEP):
        """`column` standing in for `owner` (e.g. a folded literal answering the
        aggregate it replaces): `column`'s kind and row carried under `owner`'s
        identity - the stream key consumers match on - as a NEW slot deriving from
        `owner`'s, typed `column_type` when given."""
        fields = _fields_of(column)
        if column_type is not _KEEP:
            fields["column_type"] = column_type
        return self._mint(type(column), column.name, owner.identity, fields, owner.slot)

    def bind_relation(self, descriptor, str alias):
        """Bind a source's `RelationDescriptor` as relation `alias`: every column
        becomes a bound column of `alias` minted here, its origin `alias`.

        A source that hands over anything but a descriptor is refused - a bound
        schema from a connector would carry another binding's identities into this
        query (architect ruling 2026-09-26: no dual path)."""
        from opteryx.types.schema import RelationDescriptor
        from opteryx.types.schema import RelationSchema

        if type(descriptor) is not RelationDescriptor:
            raise InvalidInternalStateError(
                f"Relation '{alias}' was described by a {type(descriptor).__name__}; "
                "sources describe relations with a RelationDescriptor and only the "
                "binder makes bound columns."
            )
        origin = (alias,)
        columns = [
            self.relation_column(
                alias,
                column.name,
                column_type=column.column_type,
                nullable=column.nullable,
                field_id=column.field_id,
                origin=origin,
            )
            for column in descriptor.columns
        ]
        return RelationSchema(
            name=descriptor.name,
            columns=columns,
            aliases=list(descriptor.aliases),
            primary_key=descriptor.primary_key,
            row_count_metric=descriptor.row_count_metric,
            row_count_estimate=descriptor.row_count_estimate,
            data_size_metric=descriptor.data_size_metric,
            data_size_estimate=descriptor.data_size_estimate,
        )
