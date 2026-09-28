# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: wraparound=False
# cython: boundscheck=False
# distutils: language = c++

"""The Opteryx column/value type (CLAUDE.md §14), native.

A `ColumnType` is a physical `DrakenType`, an optional draken `LogicalType`
descriptor, and for ARRAY an element `ColumnType`. Each distinct value is interned
once per process in src/cpp/planner/column_type.hpp and a `ColumnType` object is
nothing but its interned id: two types are equal exactly when their ids are, and a
native column row stores the id (architect rulings 2026-09-27, native plan graph
P1-c2).

The Python views a type hands out are built once per interned type and reused:
`physical` is the DrakenType member, `logical` the draken LogicalType first seen
for that type (a value - any equal descriptor is the same type), `element` the
canonical ColumnType of the element id.

`opteryx.types.logical_type` re-exports everything here and keeps the factories
(DECIMAL, TIMESTAMP, ...), the canonical instances and the parsing.
"""

cimport cython
from libc.stdint cimport int16_t
from libc.stdint cimport uint8_t
from libc.stdint cimport uint32_t
from libcpp cimport bool as cbool

from draken.core.buffers cimport DrakenType as CDrakenType

from draken.draken_native import DrakenType
from draken.draken_native import LogicalKind
from draken.draken_native import LogicalType
from draken.draken_native import TimestampUnit
from opteryx.compiled.planner.logical_category import LogicalCategory
# Draken owns the physical+descriptor -> SQL name mapping; this is the one entry
# point onto it. Never reimplement the table here (see ColumnType.__str__).
from draken.vectors.vector import type_display_name as _draken_type_display_name


# The process's type table. Grows only with the GIL held (see the header).
cdef ColumnTypeTable _TABLE


cdef const ColumnTypeTable* column_type_table():
    """The process's type table, for native readers of interned type ids."""
    return &_TABLE


# Physical type -> dispatch category. Integer/float widths collapse here.
_CATEGORY_OF = {
    DrakenType.INT8: LogicalCategory.INTEGER,
    DrakenType.INT16: LogicalCategory.INTEGER,
    DrakenType.INT32: LogicalCategory.INTEGER,
    DrakenType.INT64: LogicalCategory.INTEGER,
    DrakenType.UINT8: LogicalCategory.INTEGER,
    DrakenType.UINT16: LogicalCategory.INTEGER,
    DrakenType.UINT32: LogicalCategory.INTEGER,
    DrakenType.UINT64: LogicalCategory.INTEGER,
    DrakenType.DECIMAL: LogicalCategory.DECIMAL,
    DrakenType.DECIMAL128: LogicalCategory.DECIMAL,
    DrakenType.FLOAT32: LogicalCategory.FLOAT,
    DrakenType.FLOAT64: LogicalCategory.FLOAT,
    DrakenType.DATE32: LogicalCategory.DATE,
    DrakenType.TIMESTAMP64: LogicalCategory.TIMESTAMP,
    DrakenType.TIME32: LogicalCategory.TIME,
    DrakenType.TIME64: LogicalCategory.TIME,
    DrakenType.INTERVAL: LogicalCategory.INTERVAL,
    DrakenType.BOOL: LogicalCategory.BOOLEAN,
    DrakenType.VARCHAR: LogicalCategory.VARCHAR,
    DrakenType.NVARCHAR: LogicalCategory.NVARCHAR,
    DrakenType.VARBINARY: LogicalCategory.VARBINARY,
    DrakenType.VARIANT: LogicalCategory.VARIANT,
    DrakenType.ARRAY: LogicalCategory.ARRAY,
    DrakenType.VECTOR_FP16: LogicalCategory.VECTOR,
    DrakenType.NULL: LogicalCategory.NULL,
}

# The canonical spelling of a TimestampUnit, both directions. It matches the SQL
# surface (`TIMESTAMP[ms]`), so a serialized type string is also a valid declared
# type — the property DECIMAL(p, s), ARRAY<T> and VECTOR(n) already have.
_UNIT_TO_SQL = {
    TimestampUnit.SECONDS: "s",
    TimestampUnit.MILLISECONDS: "ms",
    TimestampUnit.MICROSECONDS: "us",
    TimestampUnit.NANOSECONDS: "ns",
}

# Python views indexed by the physical tag's value (a uint8).
cdef list _PHYSICAL_BY_VALUE = [None] * 256
cdef list _CATEGORY_BY_VALUE = [None] * 256
for _member in DrakenType:
    _PHYSICAL_BY_VALUE[_member.value] = _member
    _CATEGORY_BY_VALUE[_member.value] = _CATEGORY_OF.get(_member)

# Python views indexed by type id: the descriptor first seen for the type (or
# None) and the canonical ColumnType object (the first constructed for the id).
# `_LOGICAL_VIEWS` is a MODULE global, not a cdef one: it holds nanobind
# LogicalType instances, and a cdef global is never released at interpreter exit,
# so nanobind's shutdown leak check reported every view as leaked.
_LOGICAL_VIEWS = []
cdef list _BY_ID = []


cdef str _check_message(uint8_t check, physical, logical):
    if check == 1:
        return (
            f"{physical!r} is a parameterized physical type and requires a "
            f"LogicalType descriptor"
        )
    if check == 2 or check == 6:
        return f"{physical!r} must not carry an `element` (that is ARRAY-only)"
    if check == 3:
        return "ARRAY physical type requires an `element` ColumnType descriptor"
    if check == 4:
        return "ARRAY must not carry a LogicalType (the array child lives in `element`)"
    if check == 5:
        return (
            f"{physical!r} permits only ['IPV4'] as a LogicalType kind; "
            f"got {logical.kind!r}"
        )
    if check == 7:
        return (
            f"{physical!r} is unparameterized and must not carry a LogicalType "
            f"descriptor"
        )
    return f"{physical!r} is unparameterized and must not carry an `element`"


def physical_is_parameterized(physical) -> bool:
    """Whether the physical tag `physical` needs a LogicalType descriptor to mean
    anything (DECIMAL, TIMESTAMP, TIME, VECTOR) - the native rule ColumnType
    construction applies."""
    if type(physical) is not DrakenType:
        raise TypeError(f"physical must be a DrakenType; got {type(physical).__name__}")
    return column_type_is_parameterized(<CDrakenType><int>physical.value)


@cython.auto_pickle(False)  # an id means nothing outside this process
@cython.final
cdef class ColumnType:
    """An Opteryx column/value type: a physical tag + optional logical descriptor (D1).

    `logical` is a Draken `LogicalType` for parameterized physical types (DECIMAL,
    TIMESTAMP, TIME, VECTOR_FP16) and the IPv4 refinement of UINT32; `None`
    otherwise. `element` is the element `ColumnType` of an ARRAY; `None` otherwise.

    Immutable and hashable; equality is value equality (the interned id).
    """

    def __init__(self, physical, logical=None, element=None):
        cdef ColumnTypeEntry entry
        cdef uint8_t check
        cdef cbool inserted = False
        cdef uint32_t type_id

        if type(physical) is not DrakenType:
            raise TypeError(f"ColumnType physical must be a DrakenType; got {type(physical).__name__}")
        if logical is not None and type(logical) is not LogicalType:
            raise TypeError(f"ColumnType logical must be a LogicalType; got {type(logical).__name__}")
        if element is not None and type(element) is not ColumnType:
            raise TypeError(f"ColumnType element must be a ColumnType; got {type(element).__name__}")

        entry.physical = <CDrakenType><int>physical.value
        entry.has_logical = logical is not None
        if entry.has_logical:
            entry.logical.kind = <CLogicalKind><uint8_t>logical.kind.value
            entry.logical.unit = <CTimestampUnit><uint8_t>logical.unit.value
            entry.logical.offset_minutes = <int16_t>logical.offset_minutes
            entry.logical.precision = <uint8_t>logical.precision
            entry.logical.scale = <uint8_t>logical.scale
            entry.logical.dimension = <uint32_t>logical.dimension
        else:
            entry.logical.kind = <CLogicalKind>0
            entry.logical.unit = <CTimestampUnit>0
            entry.logical.offset_minutes = 0
            entry.logical.precision = 0
            entry.logical.scale = 0
            entry.logical.dimension = 0
        entry.element = kNoColumnType if element is None else (<ColumnType>element).type_id

        check = column_type_check_code(entry)
        if check != 0:
            raise ValueError(_check_message(check, physical, logical))

        type_id = _TABLE.intern(entry, &inserted)
        if inserted:
            _LOGICAL_VIEWS.append(logical)
            _BY_ID.append(self)
        self.type_id = type_id

    @property
    def physical(self):
        return _PHYSICAL_BY_VALUE[<uint8_t>_TABLE.entry(self.type_id).physical]

    @property
    def logical(self):
        return _LOGICAL_VIEWS[self.type_id]

    @property
    def element(self):
        cdef uint32_t element = _TABLE.entry(self.type_id).element
        if element == kNoColumnType:
            return None
        return _BY_ID[element]

    @property
    def category(self):
        """Operator-dispatch category (Decision B)."""
        category = _CATEGORY_BY_VALUE[<uint8_t>_TABLE.entry(self.type_id).physical]
        if category is None:
            raise NotImplementedError(
                f"no dispatch category for physical type {self.physical!r} "
                f"(unsupported)"
            )
        return category

    def __eq__(self, other):
        if type(other) is not ColumnType:
            return NotImplemented
        return self.type_id == (<ColumnType>other).type_id

    def __ne__(self, other):
        if type(other) is not ColumnType:
            return NotImplemented
        return self.type_id != (<ColumnType>other).type_id

    def __hash__(self):
        return self.type_id

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self

    def __repr__(self):
        return f"ColumnType(physical={self.physical!r}, logical={self.logical!r}, element={self.element!r})"

    def ordinalize(self, value):
        """Scalar ordinal key for `value`, in the same int64 space
        `Vector.ordinalize()` produces for a column of this physical type
        (see draken/ops/ordinalize.h). Lets plan-time code — file pruning
        against ordinalize()-encoded manifest min/max bounds — compare a
        predicate literal against those bounds without materialising a
        Vector.

        Mostly a passthrough to `DrakenType.ordinalize`, with two cases that
        physical-only entry point deliberately refuses because it cannot see
        the `LogicalType` descriptor this class carries:

        DATE32/TIMESTAMP64/TIME32/TIME64 — the physical entry point wants a
        `datetime.date`/`datetime`/`time` OBJECT (and refuses TIMESTAMP/TIME
        outright, since their unit lives on `LogicalType` and cannot be
        guessed from the physical tag). That is not the situation here: by
        the time a literal reaches file pruning the binder has already
        normalised it to the column's own raw physical integer — a DATE
        literal binds to `-7305`, days since epoch, NOT a `datetime.date`;
        a TIMESTAMP literal binds to raw micros. For all four types
        `ordinalize` is an identity widen from INT32/INT64, so that
        already-raw integer IS the ordinal key and no conversion is wanted.
        Passing it to the physical entry point would raise, and pruning would
        silently stop happening on exactly the columns most often filtered
        (dates and timestamps on log tables). A non-integer reaching here
        means the bind-time normalisation assumption no longer holds, so it
        raises rather than guessing a unit — the caller then skips pruning,
        which costs speed, never correctness.

        DECIMAL — rescales. A stored DECIMAL bound is the unscaled mantissa at
        the COLUMN's scale, while `DrakenType.DECIMAL.ordinalize` returns the
        mantissa at the LITERAL's own natural scale (`Decimal("1.5")` -> 15,
        never 1500 for a scale-2 column), so the literal is put on the column's
        gridline first, via `rescale_decimal_literal`. Returns None (caller
        skips pruning) when it does not land there exactly.

        DECIMAL128 — raises. Not a rescaling question: draken produces no
        ordinal key for it at all, so no stored bound in this space exists to
        compare against.
        """
        physical = self.physical

        if physical in (
            DrakenType.DATE32,
            DrakenType.TIMESTAMP64,
            DrakenType.TIME32,
            DrakenType.TIME64,
        ):
            if isinstance(value, int) and not isinstance(value, bool):
                return value
            raise ValueError(
                f"ordinalize: {physical!r} expects a bind-normalised integer literal "
                f"(the raw physical value at the column's unit); got "
                f"{type(value).__name__}"
            )

        if physical == DrakenType.DECIMAL:
            # No operator context here, so this is the EXACT case only: a literal
            # that does not land on the column's gridline returns None and the
            # caller skips pruning (see rescale_decimal_literal).
            from opteryx.types.logical_type import rescale_decimal_literal

            rescaled = rescale_decimal_literal(self, value)
            if rescaled is None:
                return None
            return int(rescaled.scaleb(int(self.logical.scale)))

        if physical == DrakenType.DECIMAL128:
            # draken has no ordinalize entry for DECIMAL128 at all, deliberately:
            # a saturated low-resolution int64 proxy for a 128-bit type is worse
            # than refusing (draken/ops/ordinalize.h).
            raise ValueError(
                f"ordinalize: {physical!r} is not supported — draken produces no "
                "ordinal key for it, so a stored bound in this space cannot exist"
            )

        return physical.ordinalize(value)

    def __str__(self):
        """The SQL type name — DELEGATED to draken, which owns that mapping.

        Draken is the single source (architect's ruling, 2026-08-08): the
        descriptor is what decides the name, and draken owns LogicalType. This
        string is PERSISTED into stored schemas, so it is a format, not a display
        choice — see tests/unit/types/test_type_name_parity.py.

        ARRAY stays here: its element is a nested ColumnType, which draken has no
        concept of, so draken names the tag and this composes the rest.
        """
        physical = self.physical
        if physical == DrakenType.ARRAY:
            return f"ARRAY<{self.element}>"
        logical = self.logical
        name = _draken_type_display_name(
            physical,
            kind=(logical.kind if logical is not None else None),
            unit=(_UNIT_TO_SQL.get(logical.unit) if logical is not None else None),
            precision=(logical.precision if logical is not None else 0),
            scale=(logical.scale if logical is not None else 0),
            dimension=(logical.dimension if logical is not None else 0),
        )
        if not name:
            raise NotImplementedError(f"no display name for {physical!r}")
        return name
