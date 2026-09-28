# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: auto_pickle=False

"""
Expression nodes with FIXED, declared, enforced attributes.

One class per expression NodeType (architect ruling 2026-09-25). A class
declares exactly the fields that kind of expression has: an undeclared name
raises AttributeError, a wrong-typed value raises TypeError, and there is no
dynamic bag. This replaces the attribute-bag `Node` for expressions.

Children are reached through ONE API, never by probing field names:

    children()        every child expression, in declared field order, list
                      fields flattened, absent (None) children skipped
    map_children(fn)  replace every child in place with fn(child)

A walker that needs every child uses these; a walker that needs a SPECIFIC
child of a SPECIFIC kind reads that declared field after checking node_type.

`copy(memo)` deep-copies with the same semantics the attribute-bag Node had
(`_copy_value`): containers are copied recursively, child expressions through
the shared memo (a child reachable from two fields copies to ONE new object),
objects with a `copy()` method (a subquery's LogicalPlan, sets) are copied with
it, everything else is shared.

Every expression is minted in its query's `ExprArena` and identified by the int
`expr_id` it mints (one node per id; `origin_id` names the expression as written,
shared by its copies). When binding ends the arena is SEALED (native plan graph P3,
architect rulings 2026-09-27): from then on an expression that has been placed -
in a plan step, or under another expression - is immutable. Its setters raise,
`copy()` returns it (sharing replaces copying), and a rewrite builds a new
expression (`replace`, `with_fields`, `rewrite_children`). An expression minted
after the seal is a draft, writable until it is placed.
"""

from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport uint16_t
from libc.stdint cimport uint32_t
from libcpp cimport bool as cbool
from libcpp.string cimport string
from libcpp.utility cimport pair
from libcpp.vector cimport vector
from cpython.unicode cimport PyUnicode_AsUTF8AndSize



cdef class ExprArena:
    """The query's expressions: where each is minted and gets its id.

    One per query, held by PlanContext (native plan graph P3, architect rulings
    2026-09-27). Ids start at 1 and are never reused within the query.
    """


    def __cinit__(self):
        self._table = new ExprTable()
        self._sealed = False

    def __dealloc__(self):
        del self._table

    def seal(self, plans):
        """End of binding: from here every expression that has been placed (in a
        plan step, or under another expression) is immutable. A rewrite builds a
        new expression; an expression minted after the seal is a draft, writable,
        until it is placed (architect ruling 2026-09-27, P3).

        `plans` are the query's plans; every literal placed in them is checked
        against its declared type first (see `_check_literal`)."""
        cdef set seen = set()
        cdef list stack = []
        for plan in plans:
            for _nid, step in plan.nodes(True):
                stack.extend(step.expressions(True))
        while stack:
            expression = stack.pop()
            if id(expression) in seen:
                continue
            seen.add(id(expression))
            if type(expression) is Literal:
                _check_literal(<Literal>expression)
            stack.extend(expression.children())
        self._sealed = True

    @property
    def sealed(self):
        return self._sealed

    def bind_columns(self, columns):
        """Bind the query's ColumnTable: every expression's bound column is a slot
        of it (PlanContext binds its own table once, at construction)."""
        if columns is None:
            raise ValueError("an expression arena binds a column table, not None")
        if self._columns is not None and self._columns is not columns:
            raise ValueError("this expression arena is already bound to another query's column table")
        self._columns = columns

    @property
    def columns(self):
        """The query's ColumnTable, or None for an arena no query has bound."""
        return self._columns

    def __len__(self):
        return self._table.size()

    def native_row(self, int64_t expr_id):
        """The arena's row for `expr_id`, as a dict - how the native side holds
        the expression. For checking the rows against their expression objects."""
        cdef ExprRow* row = &self._table.row(expr_id)
        return {
            "origin": row.origin,
            "kind": row.kind,
            "flags": row.flags,
            "value": row.value.decode("utf-8"),
            "alias": row.alias.decode("utf-8") if row.flags & FLAG_HAS_ALIAS else None,
            "query_column": row.query_column.decode("utf-8") if row.flags & FLAG_HAS_QUERY_COLUMN else None,
            "column_slot": None if row.column_slot == kNoColumnSlot else row.column_slot,
            "relations": frozenset(self._table.relation_name(r).decode("utf-8") for r in row.relations),
            "left": row.left,
            "right": row.right,
            "centre": row.centre,
            "else_result": row.else_result,
            "format": row.format,
            "parameters": tuple(row.parameters),
            "conditions": tuple(row.conditions),
            "results": tuple(row.results),
            "order": tuple((item.first, item.second) for item in row.order),
            "source": row.source.decode("utf-8"),
            "source_column": row.source_column.decode("utf-8"),
            "outer_relation": row.outer_relation.decode("utf-8"),
            "qualified_name": row.qualified_name.decode("utf-8"),
            "duplicate_treatment": row.duplicate_treatment.decode("utf-8"),
            "null_treatment": row.null_treatment.decode("utf-8"),
            "limit": row.limit if row.flags & FLAG_HAS_LIMIT else None,
            "like_selectivity_decay": row.like_selectivity_decay if row.flags & FLAG_HAS_LIKE_DECAY else None,
            "match_threshold": row.match_threshold if row.flags & FLAG_HAS_MATCH_THRESHOLD else None,
            "span": (row.span[0], row.span[1], row.span[2], row.span[3]) if row.flags & FLAG_HAS_SPAN else None,
            "type_id": None if row.type_id == kNoTypeId else row.type_id,
            "literal": _literal_from_native(row.literal),
        }

    def __reduce__(self):
        raise TypeError("ExprArena belongs to one query and cannot be pickled")


cdef object _NodeType = None


cdef inline object _node_types():
    # Imported lazily: opteryx.expression imports this module.
    global _NodeType
    if _NodeType is None:
        from opteryx.expression import NodeType

        _NodeType = NodeType
    return _NodeType


cdef object _LogicalPlan = None


cdef void _load_lazy_types():
    # Imported lazily: the planner imports this module.
    global _LogicalPlan
    from opteryx.planner.logical_planner import LogicalPlan

    _LogicalPlan = LogicalPlan


cpdef object _copy_value(object value, dict memo):
    """Deep-copy one field value: containers recursively, child expressions
    through `memo`, a set and a subquery's LogicalPlan with their own copy,
    everything else shared. Dispatch is on the EXACT type."""
    cdef type value_type = type(value)
    if value is None or value_type is int or value_type is float or value_type is str or value_type is bool:
        return value
    if value_type in EXPRESSION_TYPES:
        return (<Expression>value).copy(memo)
    if value_type is list:
        return [_copy_value(v, memo) for v in value]
    if value_type is tuple:
        return tuple([_copy_value(v, memo) for v in value])
    if value_type is dict:
        return {k: _copy_value(v, memo) for k, v in value.items()}
    if value_type is set:
        return set(value)
    if _LogicalPlan is None:
        _load_lazy_types()
    if value_type is _LogicalPlan:
        return value.copy()
    return value


cdef inline void _require_bool(str name, object value):
    if type(value) is not bool:
        raise TypeError(f"{name} must be a bool, got {type(value).__name__}")


cdef inline void _require_optional_float(str name, object value):
    if value is not None and type(value) is not float:
        raise TypeError(f"{name} must be a float or None, got {type(value).__name__}")


cdef inline void _require_optional_int(str name, object value):
    if value is not None and type(value) is not int:
        raise TypeError(f"{name} must be an int or None, got {type(value).__name__}")


cdef inline bint _is_expression(object value):
    return type(value) in EXPRESSION_TYPES


cdef object _LC = None


cdef void _check_literal(Literal literal):
    """A placed literal's value must be the native form of its declared type
    (architect rulings 2026-09-27, P3-c): an untyped literal, or a value of another
    kind, is refused - never coerced."""
    global _LC
    if _LC is None:
        from opteryx.types.logical_category import LogicalCategory

        _LC = LogicalCategory
    column_type = literal._type
    if column_type is None:
        raise TypeError(f"Literal #{literal.expr_id} has no type (value {literal._value!r})")
    if not _value_matches(literal._value, column_type):
        raise TypeError(
            f"Literal #{literal.expr_id} of type {column_type} holds a "
            f"{type(literal._value).__name__} ({literal._value!r}); a {column_type} literal "
            "holds its native value"
        )


cdef bint _value_matches(object value, object column_type):
    if value is None:
        return True  # a typed NULL
    cdef object category = column_type.category
    cdef type kind = type(value)
    if category is _LC.NULL:
        return False
    if category is _LC.BOOLEAN:
        return kind is bool
    if category is _LC.INTEGER or category is _LC.DATE or category is _LC.TIME or category is _LC.TIMESTAMP:
        return kind is int
    if category is _LC.FLOAT:
        return kind is float
    if category is _LC.DECIMAL:
        from decimal import Decimal

        return kind is Decimal
    if category is _LC.VARCHAR or category is _LC.NVARCHAR or category is _LC.VARBINARY:
        return kind is bytes
    if category is _LC.INTERVAL:
        # DrakenIntervalSlot: (months, microseconds)
        return kind is tuple and len(value) == 2 and type(value[0]) is int and type(value[1]) is int
    if category is _LC.ARRAY:
        # a tuple: a literal's value is as immutable as the literal
        if kind is not tuple:
            return False
        element = column_type.element
        for item in value:
            if not _value_matches(item, element):
                return False
        return True
    if category is _LC.VECTOR:
        if kind is not tuple:
            return False
        for item in value:
            if type(item) is not float:
                return False
        return True
    return False


cdef void _publish(Expression expression):
    """`expression` has been placed: a draft stops being one, and so does every
    draft beneath it. Placed expressions of a sealed arena are immutable; a placed
    literal is checked against its type (see `_check_literal`)."""
    if not expression._draft:
        return
    expression._draft = False
    _flag(&expression._arena._table.row(expression.expr_id), FLAG_DRAFT, False)
    if type(expression) is Literal:
        _check_literal(<Literal>expression)
    for child in expression.children():
        _publish(<Expression>child)


cpdef object publish(object value):
    """Place `value` if it is an expression (see `_publish`); returns it. The one
    hook through which a plan step's fields place the expressions they hold."""
    if _is_expression(value):
        _publish(<Expression>value)
    return value


cdef inline void _require_expression(str name, object value):
    if value is None:
        return
    if not _is_expression(value):
        raise TypeError(f"{name} must be an expression or None, got {type(value).__name__}")
    _publish(<Expression>value)


cdef inline tuple _require_expression_list(str name, object value):
    """A list field's value as the expression holds it: a tuple of placed
    expressions (list fields are immutable, architect ruling Q3)."""
    if value is None:
        return None
    if type(value) is not list and type(value) is not tuple:
        raise TypeError(f"{name} must be a list or tuple of expressions, got {type(value).__name__}")
    for item in value:
        if not _is_expression(item):
            raise TypeError(f"{name} must hold expressions, got {type(item).__name__}")
        _publish(<Expression>item)
    return tuple(value)


cdef inline tuple _require_order_list(str name, object value):
    """An aggregate's ORDER BY: (expression, ascending) pairs, as a tuple."""
    if value is None:
        return None
    if type(value) is not list and type(value) is not tuple:
        raise TypeError(f"{name} must be a list or tuple of (expression, bool) pairs, got {type(value).__name__}")
    for item in value:
        if type(item) is not tuple or len(item) != 2 or not _is_expression(item[0]) or type(item[1]) is not bool:
            raise TypeError(f"{name} must hold (expression, bool) pairs, got {item!r}")
        _publish(<Expression>item[0])
    return tuple(value)


cdef inline frozenset _frozen_relations(object value):
    if value is None:
        return None
    if type(value) is frozenset:
        return value
    if type(value) is not set:
        raise TypeError(f"relations must be a set or frozenset, got {type(value).__name__}")
    return frozenset(value)



# ---------------------------------------------------------------------------
# writing an expression's native row (see src/cpp/planner/expr_arena.hpp)
# ---------------------------------------------------------------------------


cdef inline string _utf8(object text):
    """`text` (a str, or None) as the row's UTF-8 string - read from the str's own
    UTF-8 buffer, no intermediate bytes object."""
    cdef const char* data
    cdef Py_ssize_t size
    if text is None:
        return string()
    data = PyUnicode_AsUTF8AndSize(<str?>text, &size)
    return string(data, size)


cdef inline int64_t _child_id(Expression owner, object child):
    if child is None:
        return 0
    cdef Expression expression = <Expression>child
    if expression._arena is not owner._arena:
        raise ValueError(
            f"{type(owner).__name__} #{owner.expr_id} and its child "
            f"{type(child).__name__} #{expression.expr_id} belong to different queries"
        )
    return expression.expr_id


cdef inline void _child_ids(vector[int64_t]& out, Expression owner, tuple children):
    out.clear()
    if children is None:
        return
    for child in children:
        out.push_back(_child_id(owner, child))


cdef inline void _flag(ExprRow* row, uint16_t flag, bint on):
    if on:
        row.flags |= flag
    else:
        row.flags &= ~flag


cdef inline void _span_into(ExprRow* row, tuple span):
    if span is None:
        return
    row.flags |= FLAG_HAS_SPAN
    row.span[0] = span[0]
    row.span[1] = span[1]
    row.span[2] = span[2]
    row.span[3] = span[3]


cdef NodeKinds _NODE_KINDS
cdef bint _NODE_KINDS_LOADED = False


cdef const NodeKinds* node_kinds() except NULL:
    """opteryx.expression.NodeType's values, read once - never restated. Loaded
    on first use: opteryx.expression imports this module."""
    global _NODE_KINDS_LOADED
    if not _NODE_KINDS_LOADED:
        from opteryx.expression import NodeType

        _NODE_KINDS.and_ = NodeType.AND.value
        _NODE_KINDS.or_ = NodeType.OR.value
        _NODE_KINDS.xor_ = NodeType.XOR.value
        _NODE_KINDS.not_ = NodeType.NOT.value
        _NODE_KINDS.dnf = NodeType.DNF.value
        _NODE_KINDS.cnf = NodeType.CNF.value
        _NODE_KINDS.case_ = NodeType.CASE.value
        _NODE_KINDS.comparison = NodeType.COMPARISON_OPERATOR.value
        _NODE_KINDS.binary = NodeType.BINARY_OPERATOR.value
        _NODE_KINDS.unary = NodeType.UNARY_OPERATOR.value
        _NODE_KINDS.function = NodeType.FUNCTION.value
        _NODE_KINDS.identifier = NodeType.IDENTIFIER.value
        _NODE_KINDS.nested = NodeType.NESTED.value
        _NODE_KINDS.aggregator = NodeType.AGGREGATOR.value
        _NODE_KINDS.literal = NodeType.LITERAL.value
        _NODE_KINDS.cast = NodeType.CAST.value
        _NODE_KINDS.extraction = NodeType.EXTRACTION_OPERATOR.value
        _NODE_KINDS.between = NodeType.BETWEEN.value
        _NODE_KINDS_LOADED = True
    return &_NODE_KINDS


cdef object _LC_NATIVE = None
# Each class's NodeType value (one kind per class), cached: an enum's `.value` is a
# Python attribute read, paid on every row write otherwise.
cdef dict _KIND_OF = {}
from decimal import Decimal as _DECIMAL


cdef void _literal_to_native(object value, object column_type, LiteralValue& out) except *:
    """`value` - a literal's native Python value (see _check_literal) - as the
    arena's tagged value. The tag follows the value; the declared type only tells
    an INTERVAL's (months, microseconds) from an ARRAY's elements. A value with no
    native form is recorded as NONE: mid-construction a literal can hold a value
    and a type that do not agree yet, and a placed literal whose value is not its
    type's native form is refused where it is placed (_check_literal)."""
    global _LC_NATIVE
    if _LC_NATIVE is None:
        from opteryx.types.logical_category import LogicalCategory

        _LC_NATIVE = LogicalCategory
    cdef LiteralValue item
    out.items.clear()
    out.bytes.clear()
    if column_type is None:
        out.tag = LITERAL_NONE  # a draft mid-construction
        return
    if value is None:
        out.tag = LITERAL_NULL
        return
    cdef object category = column_type.category
    cdef type kind = type(value)
    if kind is bool:
        out.tag = LITERAL_BOOL
        out.i = 1 if value else 0
    elif kind is int:
        if -9223372036854775808 <= value <= 9223372036854775807:
            out.tag = LITERAL_INT64
            out.i = value
        elif 0 <= value <= 18446744073709551615:
            out.tag = LITERAL_UINT64
            out.i = value - 18446744073709551616 if value > 9223372036854775807 else value
        else:
            raise OverflowError(f"literal {value} does not fit 64 bits")
    elif kind is float:
        out.tag = LITERAL_DOUBLE
        out.d = value
    elif kind is bytes:
        out.tag = LITERAL_BYTES
        out.bytes = value
    elif kind is _DECIMAL:
        sign, digits, exponent = value.as_tuple()
        if type(exponent) is not int:
            raise ValueError(f"DECIMAL literal {value!r} is not a finite number")
        unscaled = int("".join(str(digit) for digit in digits) or "0")
        if sign:
            unscaled = -unscaled
        bits = unscaled & ((1 << 128) - 1)
        lo = bits & 0xFFFFFFFFFFFFFFFF
        hi = bits >> 64
        out.tag = LITERAL_DECIMAL
        out.i = lo - (1 << 64) if lo > 9223372036854775807 else lo
        out.j = hi - (1 << 64) if hi > 9223372036854775807 else hi
        out.k = exponent
    elif kind is tuple and category is _LC_NATIVE.INTERVAL and len(value) == 2:
        out.tag = LITERAL_INTERVAL
        out.i = value[0]
        out.j = value[1]
    elif kind is tuple:
        out.tag = LITERAL_ITEMS
        element = column_type.element
        for member in value:
            _literal_to_native(member, element if element is not None else column_type, item)
            out.items.push_back(item)
    else:
        out.tag = LITERAL_NONE


cdef object _literal_from_native(LiteralValue& value):
    if value.tag == LITERAL_NONE:
        return ("none",)
    if value.tag == LITERAL_NULL:
        return None
    if value.tag == LITERAL_BOOL:
        return value.i != 0
    if value.tag == LITERAL_INT64:
        return value.i
    if value.tag == LITERAL_UINT64:
        return value.i + 18446744073709551616 if value.i < 0 else value.i
    if value.tag == LITERAL_DOUBLE:
        return value.d
    if value.tag == LITERAL_BYTES:
        return <bytes>value.bytes
    if value.tag == LITERAL_DECIMAL:
        return ("decimal", value.j, value.i, value.k)
    if value.tag == LITERAL_INTERVAL:
        return (value.i, value.j)
    return tuple(_literal_from_native(item) for item in value.items)

cdef class Expression:
    """What every expression has: identity, naming, the column it binds to."""

    cdef readonly object node_type
    cdef readonly int64_t expr_id
    cdef readonly int64_t origin_id
    cdef ExprArena _arena
    cdef bint _draft
    cdef bint _building  # inside __init__: the row is written once, at its end
    cdef str _alias
    cdef str _query_column
    cdef object _schema_column
    cdef frozenset _relations
    cdef bint _do_not_create_column

    cdef void _bind_arena(self, ExprArena arena, object origin):
        # Every expression belongs to its query's arena and is identified by an id
        # minted there - one node per id. `origin_id` is the id of the expression as
        # it was WRITTEN: a copy, or a node rebuilt as another kind in its place,
        # shares its origin (`origin`) and has its own id.
        self._arena = arena
        self._building = True
        self.expr_id = arena._mint()
        self.origin_id = self.expr_id if origin is None else <int64_t>origin
        self._draft = arena._sealed

    cdef void _sync(self) except *:
        """Write this expression's row in its arena: what every expression has.
        Each kind extends it with its own fields."""
        cdef ExprRow* row = &self._arena._table.row(self.expr_id)
        row.origin = self.origin_id
        kind = _KIND_OF.get(type(self))
        if kind is None:
            kind = _KIND_OF[type(self)] = self.node_type.value
        row.kind = kind
        row.flags = 0
        if self._draft:
            row.flags |= FLAG_DRAFT
        if self._do_not_create_column:
            row.flags |= FLAG_DO_NOT_CREATE_COLUMN
        if self._alias is not None:
            row.flags |= FLAG_HAS_ALIAS
        if self._query_column is not None:
            row.flags |= FLAG_HAS_QUERY_COLUMN
        row.alias = _utf8(self._alias)
        row.query_column = _utf8(self._query_column)
        row.column_slot = kNoColumnSlot if self._schema_column is None else <uint32_t>self._schema_column.slot
        row.relations.clear()
        if self._relations is not None:
            for name in self._relations:
                row.relations.push_back(self._arena._table.relation(_utf8(name)))

    cdef inline void _check_writable(self):
        if self._arena._sealed and not self._draft:
            from opteryx.exceptions import InvalidInternalStateError

            raise InvalidInternalStateError(
                f"{type(self).__name__} #{self.expr_id} is sealed: expressions are immutable "
                "once placed after binding - build a new one (replace / rewrite_children)"
            )

    @property
    def arena(self):
        """The query's expression arena this expression was minted in."""
        return self._arena

    cdef void _set_common(self, str alias, str query_column, object schema_column, object relations):
        self._alias = alias
        self._query_column = query_column
        self._schema_column = schema_column
        self._relations = _frozen_relations(relations)

    @property
    def alias(self):
        return self._alias

    @alias.setter
    def alias(self, str value):
        cdef ExprRow* row
        self._check_writable()
        self._alias = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.alias = _utf8(self._alias)
            _flag(row, FLAG_HAS_ALIAS, self._alias is not None)

    @property
    def query_column(self):
        return self._query_column

    @query_column.setter
    def query_column(self, str value):
        cdef ExprRow* row
        self._check_writable()
        self._query_column = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.query_column = _utf8(self._query_column)
            _flag(row, FLAG_HAS_QUERY_COLUMN, self._query_column is not None)

    @property
    def schema_column(self):
        return self._schema_column

    @schema_column.setter
    def schema_column(self, value):
        cdef ExprRow* row
        self._check_writable()
        self._schema_column = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.column_slot = kNoColumnSlot if self._schema_column is None else <uint32_t>self._schema_column.slot

    @property
    def relations(self):
        return self._relations

    @relations.setter
    def relations(self, value):
        cdef ExprRow* row
        self._check_writable()
        self._relations = _frozen_relations(value)
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.relations.clear()
            if self._relations is not None:
                for name in self._relations:
                    row.relations.push_back(self._arena._table.relation(_utf8(name)))

    @property
    def do_not_create_column(self):
        """Planner-to-binder directive: bind this expression without minting a
        derived output column for it (a synthesized join/filter condition)."""
        return self._do_not_create_column

    @do_not_create_column.setter
    def do_not_create_column(self, value):
        cdef ExprRow* row
        self._check_writable()
        _require_bool("do_not_create_column", value)
        self._do_not_create_column = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            _flag(row, FLAG_DO_NOT_CREATE_COLUMN, self._do_not_create_column)

    cpdef tuple children(self):
        return ()

    cpdef map_children(self, object fn):
        return None

    cdef void _copy_common_into(self, Expression target, dict memo):
        target._arena = self._arena
        target.expr_id = self._arena._mint()
        target.origin_id = self.origin_id
        target._draft = False
        target._alias = self._alias
        target._query_column = self._query_column
        target._schema_column = self._schema_column
        target._relations = self._relations
        target._do_not_create_column = self._do_not_create_column

    cpdef Expression copy(self, dict memo=None):
        raise NotImplementedError(f"{type(self).__name__}.copy")

    cdef Expression _shallow_copy(self):
        raise NotImplementedError(f"{type(self).__name__}._shallow_copy")

    cdef void _share_common_into(self, Expression target):
        target._arena = self._arena
        target._draft = self._arena._sealed
        target._alias = self._alias
        target._query_column = self._query_column
        target._schema_column = self._schema_column
        target._relations = self._relations
        target._do_not_create_column = self._do_not_create_column

    def replace(self, **overrides):
        """A NEW node of the same type with this one's fields, `overrides` applied
        through the fields' own (enforcing) setters, and a fresh expr_id. Children not
        overridden are SHARED; an undeclared field name raises. After the arena is
        sealed the new node is a draft until it is placed. The way to change an
        expression other holders can see."""
        cdef Expression new = self._shallow_copy()
        new.expr_id = self._arena._mint()
        new.origin_id = new.expr_id
        for name, value in overrides.items():
            setattr(new, name, value)
        new._sync()
        return new

    def with_fields(self, **changes):
        """This expression with `changes` applied: ITSELF when every change is the
        value the field already holds (the same object, or for a list field the same
        objects in the same order), else `replace(**changes)`. The copy-on-write
        form of a field write - a rewrite that changes nothing keeps the node."""
        for name, value in changes.items():
            current = getattr(self, name)
            if value is current:
                continue
            if (type(value) is list or type(value) is tuple) and type(current) is tuple and _same_items(value, current):
                continue
            return self.replace(**changes)
        return self

    def __repr__(self):
        return f"<{type(self).__name__} {self.node_type.name}>"


cdef inline bint _same_items(object items, tuple current):
    cdef Py_ssize_t i
    if len(items) != len(current):
        return False
    for i in range(len(current)):
        if items[i] is not current[i]:
            return False
    return True


cdef inline Expression _memo_hit(Expression self, dict memo):
    return memo.get(id(self))


# ---------------------------------------------------------------------------
# helpers shared by the child-carrying classes
# ---------------------------------------------------------------------------


cdef inline void _append_child(list out, object child):
    if child is not None:
        out.append(child)


cdef inline void _extend_children(list out, tuple children):
    if children is not None:
        for child in children:
            out.append(child)


cdef inline object _map_one(object fn, object child):
    return None if child is None else fn(child)


cdef inline tuple _map_list(object fn, tuple children):
    if children is None:
        return None
    return tuple([fn(child) for child in children])


# ---------------------------------------------------------------------------
# column reference
# ---------------------------------------------------------------------------


cdef class LogicalColumn(Expression):
    """A column reference (IDENTIFIER), tied to its schema column by the binder.

    source_column: the column's name in its logical source (table, subquery).
    source: the logical source it comes from.
    is_outer_reference / outer_relation: set by the binder when the reference
        resolved to an ENCLOSING query's scope — what makes a subquery correlated.
    span: where the name was written, (start_line, start_column, end_line,
        end_column), 1-based, or None for a synthesized reference.
    """

    cdef str _source_column
    cdef str _source
    cdef bint _is_outer_reference
    cdef object _outer_relation
    cdef tuple _span

    def __init__(
        self,
        node_type,
        str source_column,
        str source=None,
        str alias=None,
        schema_column=None,
        str query_column=None,
        is_outer_reference=False,
        outer_relation=None,
        tuple span=None,
        *,
        origin=None,
        ExprArena arena not None,
    ):
        if node_type != _node_types().IDENTIFIER:
            raise TypeError(f"LogicalColumn is an IDENTIFIER, got {node_type!r}")
        self.node_type = node_type
        self._bind_arena(arena, origin)
        self._set_common(alias, query_column, schema_column, None)
        self._source_column = source_column
        self._source = source
        self.is_outer_reference = is_outer_reference
        self._outer_relation = outer_relation
        self._span = span
        self._building = False
        self._sync()

    @property
    def source_column(self):
        return self._source_column

    @source_column.setter
    def source_column(self, str value):
        cdef ExprRow* row
        self._check_writable()
        self._source_column = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.source_column = _utf8(self._source_column)

    @property
    def source(self):
        return self._source

    @source.setter
    def source(self, str value):
        cdef ExprRow* row
        self._check_writable()
        self._source = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.source = _utf8(self._source)

    @property
    def is_outer_reference(self):
        return self._is_outer_reference

    @is_outer_reference.setter
    def is_outer_reference(self, value):
        cdef ExprRow* row
        self._check_writable()
        _require_bool("LogicalColumn.is_outer_reference", value)
        self._is_outer_reference = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            _flag(row, FLAG_OUTER_REFERENCE, self._is_outer_reference)

    @property
    def outer_relation(self):
        return self._outer_relation

    @outer_relation.setter
    def outer_relation(self, value):
        cdef ExprRow* row
        self._check_writable()
        self._outer_relation = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.outer_relation = _utf8(None if self._outer_relation is None else self._outer_relation.name)

    @property
    def span(self):
        return self._span

    @span.setter
    def span(self, tuple value):
        cdef ExprRow* row
        self._check_writable()
        self._span = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            _flag(row, FLAG_HAS_SPAN, False)
            _span_into(row, self._span)

    @property
    def qualified_name(self) -> str:
        """`source.source_column`, or `.source_column` with no source (None
        may itself be a table name, so it is never rendered)."""
        if self._source:
            return f"{self._source}.{self._source_column}"
        return f".{self._source_column}"

    @property
    def current_name(self) -> str:
        """The column's name here: its alias, else its source name."""
        return self.alias or self._source_column

    @property
    def value(self) -> str:
        return self.current_name

    cdef void _sync(self) except *:
        """Write the row: the common fields and a LogicalColumn's own."""
        Expression._sync(self)
        cdef ExprRow* row = &self._arena._table.row(self.expr_id)
        row.source = _utf8(self._source)
        row.source_column = _utf8(self._source_column)
        # the enclosing scope's RelationSchema stays on this object; the row names it
        row.outer_relation = _utf8(None if self._outer_relation is None else self._outer_relation.name)
        if self._is_outer_reference:
            row.flags |= FLAG_OUTER_REFERENCE
        _span_into(row, self._span)

    cdef Expression _shallow_copy(self):
        cdef LogicalColumn new = LogicalColumn.__new__(LogicalColumn)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._source_column = self._source_column
        new._source = self._source
        new._is_outer_reference = self._is_outer_reference
        new._outer_relation = self._outer_relation
        new._span = self._span
        return new

    cpdef Expression copy(self, dict memo=None):
        if self._arena._sealed:
            return self
        # A column reference copies as a FRESH reference with a detached copy of
        # its schema column; it is not memoized, matching the memo-less copy().
        # The copy SHARES the column: a column is a row of the query's ColumnTable,
        # fixed once minted (architect ruling 2026-09-27).
        return LogicalColumn(
            self.node_type,
            self._source_column,
            self._source,
            self.alias,
            self.schema_column,
            self.query_column,
            self._is_outer_reference,
            self._outer_relation,
            self._span,
            origin=self.origin_id,
            arena=self._arena,
        )

    def __repr__(self) -> str:
        return f"<LogicalColumn name: '{self.current_name}' fullname: '{self.qualified_name}'>"

    def __hash__(self):
        return hash(
            (
                self.node_type,
                self._source_column,
                self._source,
                self.alias,
                self.schema_column.identity if self.schema_column is not None else None,
            )
        )


cdef class Literal(Expression):
    """LITERAL expression.

    `rlike_compiled` marks a VARBINARY pattern literal that IS a compiled RLIKE
    program (predicate_rewriter), so it is never compiled a second time.
    """

    cdef object _value
    cdef object _type
    cdef bint _is_wildcard_order_position
    cdef bint _rlike_compiled

    def __init__(self, value=None, type=None, is_wildcard_order_position=False, rlike_compiled=False, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, origin=None, ExprArena arena not None):
        self.node_type = _node_types().LITERAL
        self._bind_arena(arena, origin)
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.value = value
        self.type = type
        self.is_wildcard_order_position = is_wildcard_order_position
        self.rlike_compiled = rlike_compiled
        self._building = False
        self._sync()

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        cdef ExprRow* row
        self._check_writable()
        self._value = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            _literal_to_native(self._value, self._type, row.literal)

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, value):
        cdef ExprRow* row
        self._check_writable()
        self._type = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.type_id = kNoTypeId if self._type is None else <uint32_t>self._type.type_id
            _literal_to_native(self._value, self._type, row.literal)

    def text(self):
        """A string literal's value as text: its UTF-8 bytes decoded. String
        literals hold bytes (P3-c); this is the one way to read one as `str`, for a
        consumer that needs text (a path, an interval, a value to parse). Any other
        literal is refused."""
        if type(self._value) is not bytes:
            raise TypeError(
                f"Literal #{self.expr_id} of type {self._type} is not a string literal"
            )
        return (<bytes>self._value).decode("utf-8")

    @property
    def is_wildcard_order_position(self):
        return self._is_wildcard_order_position

    @is_wildcard_order_position.setter
    def is_wildcard_order_position(self, value):
        cdef ExprRow* row
        self._check_writable()
        _require_bool("Literal.is_wildcard_order_position", value)
        self._is_wildcard_order_position = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            _flag(row, FLAG_WILDCARD_ORDER_POSITION, self._is_wildcard_order_position)

    @property
    def rlike_compiled(self):
        return self._rlike_compiled

    @rlike_compiled.setter
    def rlike_compiled(self, value):
        cdef ExprRow* row
        self._check_writable()
        _require_bool("Literal.rlike_compiled", value)
        self._rlike_compiled = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            _flag(row, FLAG_RLIKE_COMPILED, self._rlike_compiled)

    cpdef tuple children(self):
        return ()

    cpdef map_children(self, object fn):
        return None

    cdef void _sync(self) except *:
        """Write the row: the common fields and a Literal's own."""
        Expression._sync(self)
        cdef ExprRow* row = &self._arena._table.row(self.expr_id)
        row.type_id = kNoTypeId if self._type is None else <uint32_t>self._type.type_id
        _literal_to_native(self._value, self._type, row.literal)
        if self._is_wildcard_order_position:
            row.flags |= FLAG_WILDCARD_ORDER_POSITION
        if self._rlike_compiled:
            row.flags |= FLAG_RLIKE_COMPILED

    cdef Expression _shallow_copy(self):
        cdef Literal new = Literal.__new__(Literal)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._value = self._value
        new._type = self._type
        new._is_wildcard_order_position = self._is_wildcard_order_position
        new._rlike_compiled = self._rlike_compiled
        return new

    cpdef Expression copy(self, dict memo=None):
        if self._arena._sealed:
            return self
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef Literal new = Literal.__new__(Literal)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._value = _copy_value(self._value, memo)
        new._type = _copy_value(self._type, memo)
        new._is_wildcard_order_position = self._is_wildcard_order_position
        new._rlike_compiled = self._rlike_compiled
        new._sync()
        return new


cdef class Comparison(Expression):
    """COMPARISON_OPERATOR expression."""

    cdef str _value
    cdef object _left
    cdef object _right
    cdef bint _negated
    cdef object _like_selectivity_decay

    def __init__(self, value=None, left=None, right=None, negated=False, like_selectivity_decay=None, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, origin=None, ExprArena arena not None):
        self.node_type = _node_types().COMPARISON_OPERATOR
        self._bind_arena(arena, origin)
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.value = value
        self.left = left
        self.right = right
        self.negated = negated
        self.like_selectivity_decay = like_selectivity_decay
        self._building = False
        self._sync()

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        cdef ExprRow* row
        self._check_writable()
        self._value = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.value = _utf8(self._value)

    @property
    def left(self):
        return self._left

    @left.setter
    def left(self, value):
        cdef ExprRow* row
        self._check_writable()
        _require_expression("Comparison.left", value)
        self._left = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.left = _child_id(self, self._left)

    @property
    def right(self):
        return self._right

    @right.setter
    def right(self, value):
        cdef ExprRow* row
        self._check_writable()
        _require_expression("Comparison.right", value)
        self._right = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.right = _child_id(self, self._right)

    @property
    def negated(self):
        return self._negated

    @negated.setter
    def negated(self, value):
        cdef ExprRow* row
        self._check_writable()
        _require_bool("Comparison.negated", value)
        self._negated = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            _flag(row, FLAG_NEGATED, self._negated)

    @property
    def like_selectivity_decay(self):
        return self._like_selectivity_decay

    @like_selectivity_decay.setter
    def like_selectivity_decay(self, value):
        cdef ExprRow* row
        self._check_writable()
        _require_optional_float("Comparison.like_selectivity_decay", value)
        self._like_selectivity_decay = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            _flag(row, FLAG_HAS_LIKE_DECAY, self._like_selectivity_decay is not None)
            row.like_selectivity_decay = 0.0 if self._like_selectivity_decay is None else self._like_selectivity_decay

    cpdef tuple children(self):
        cdef list out = []
        _append_child(out, self._left)
        _append_child(out, self._right)
        return tuple(out)

    cpdef map_children(self, object fn):
        self.left = _map_one(fn, self._left)
        self.right = _map_one(fn, self._right)

    cdef void _sync(self) except *:
        """Write the row: the common fields and a Comparison's own."""
        Expression._sync(self)
        cdef ExprRow* row = &self._arena._table.row(self.expr_id)
        row.value = _utf8(self._value)
        row.left = _child_id(self, self._left)
        row.right = _child_id(self, self._right)
        if self._negated:
            row.flags |= FLAG_NEGATED
        if self._like_selectivity_decay is not None:
            row.flags |= FLAG_HAS_LIKE_DECAY
            row.like_selectivity_decay = self._like_selectivity_decay

    cdef Expression _shallow_copy(self):
        cdef Comparison new = Comparison.__new__(Comparison)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._value = self._value
        new._left = self._left
        new._right = self._right
        new._negated = self._negated
        new._like_selectivity_decay = self._like_selectivity_decay
        return new

    cpdef Expression copy(self, dict memo=None):
        if self._arena._sealed:
            return self
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef Comparison new = Comparison.__new__(Comparison)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._value = self._value
        new._left = _copy_value(self._left, memo)
        new._right = _copy_value(self._right, memo)
        new._negated = self._negated
        new._like_selectivity_decay = _copy_value(self._like_selectivity_decay, memo)
        new._sync()
        return new


cdef class BinaryOperator(Expression):
    """BINARY_OPERATOR expression."""

    cdef str _value
    cdef object _left
    cdef object _right

    def __init__(self, value=None, left=None, right=None, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, origin=None, ExprArena arena not None):
        self.node_type = _node_types().BINARY_OPERATOR
        self._bind_arena(arena, origin)
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.value = value
        self.left = left
        self.right = right
        self._building = False
        self._sync()

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        cdef ExprRow* row
        self._check_writable()
        self._value = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.value = _utf8(self._value)

    @property
    def left(self):
        return self._left

    @left.setter
    def left(self, value):
        cdef ExprRow* row
        self._check_writable()
        _require_expression("BinaryOperator.left", value)
        self._left = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.left = _child_id(self, self._left)

    @property
    def right(self):
        return self._right

    @right.setter
    def right(self, value):
        cdef ExprRow* row
        self._check_writable()
        _require_expression("BinaryOperator.right", value)
        self._right = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.right = _child_id(self, self._right)

    cpdef tuple children(self):
        cdef list out = []
        _append_child(out, self._left)
        _append_child(out, self._right)
        return tuple(out)

    cpdef map_children(self, object fn):
        self.left = _map_one(fn, self._left)
        self.right = _map_one(fn, self._right)

    cdef void _sync(self) except *:
        """Write the row: the common fields and a BinaryOperator's own."""
        Expression._sync(self)
        cdef ExprRow* row = &self._arena._table.row(self.expr_id)
        row.value = _utf8(self._value)
        row.left = _child_id(self, self._left)
        row.right = _child_id(self, self._right)

    cdef Expression _shallow_copy(self):
        cdef BinaryOperator new = BinaryOperator.__new__(BinaryOperator)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._value = self._value
        new._left = self._left
        new._right = self._right
        return new

    cpdef Expression copy(self, dict memo=None):
        if self._arena._sealed:
            return self
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef BinaryOperator new = BinaryOperator.__new__(BinaryOperator)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._value = self._value
        new._left = _copy_value(self._left, memo)
        new._right = _copy_value(self._right, memo)
        new._sync()
        return new


cdef class UnaryOperator(Expression):
    """UNARY_OPERATOR expression.

    EXISTS is a UNARY_OPERATOR too, and carries its SUBQUERY in `parameters` rather
    than `centre` (logical_planner_builders' exists builder).
    """

    cdef str _value
    cdef object _centre
    cdef tuple _parameters
    cdef bint _negated

    def __init__(self, value=None, centre=None, parameters=None, negated=False, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, origin=None, ExprArena arena not None):
        self.node_type = _node_types().UNARY_OPERATOR
        self._bind_arena(arena, origin)
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.value = value
        self.centre = centre
        self.parameters = parameters
        self.negated = negated
        self._building = False
        self._sync()

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        cdef ExprRow* row
        self._check_writable()
        self._value = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.value = _utf8(self._value)

    @property
    def centre(self):
        return self._centre

    @centre.setter
    def centre(self, value):
        cdef ExprRow* row
        self._check_writable()
        _require_expression("UnaryOperator.centre", value)
        self._centre = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.centre = _child_id(self, self._centre)

    @property
    def parameters(self):
        return self._parameters

    @parameters.setter
    def parameters(self, value):
        cdef ExprRow* row
        self._check_writable()
        self._parameters = _require_expression_list("UnaryOperator.parameters", value)
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            _child_ids(row.parameters, self, self._parameters)

    @property
    def negated(self):
        return self._negated

    @negated.setter
    def negated(self, value):
        cdef ExprRow* row
        self._check_writable()
        _require_bool("UnaryOperator.negated", value)
        self._negated = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            _flag(row, FLAG_NEGATED, self._negated)

    cpdef tuple children(self):
        cdef list out = []
        _append_child(out, self._centre)
        _extend_children(out, self._parameters)
        return tuple(out)

    cpdef map_children(self, object fn):
        self.centre = _map_one(fn, self._centre)
        self.parameters = _map_list(fn, self._parameters)

    cdef void _sync(self) except *:
        """Write the row: the common fields and a UnaryOperator's own."""
        Expression._sync(self)
        cdef ExprRow* row = &self._arena._table.row(self.expr_id)
        row.value = _utf8(self._value)
        row.centre = _child_id(self, self._centre)
        _child_ids(row.parameters, self, self._parameters)
        if self._negated:
            row.flags |= FLAG_NEGATED

    cdef Expression _shallow_copy(self):
        cdef UnaryOperator new = UnaryOperator.__new__(UnaryOperator)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._value = self._value
        new._centre = self._centre
        new._parameters = self._parameters
        new._negated = self._negated
        return new

    cpdef Expression copy(self, dict memo=None):
        if self._arena._sealed:
            return self
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef UnaryOperator new = UnaryOperator.__new__(UnaryOperator)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._value = self._value
        new._centre = _copy_value(self._centre, memo)
        new._parameters = _copy_value(self._parameters, memo)
        new._negated = self._negated
        new._sync()
        return new


cdef class ExtractionOperator(Expression):
    """EXTRACTION_OPERATOR expression."""

    cdef str _value
    cdef object _left
    cdef object _right

    def __init__(self, value=None, left=None, right=None, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, origin=None, ExprArena arena not None):
        self.node_type = _node_types().EXTRACTION_OPERATOR
        self._bind_arena(arena, origin)
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.value = value
        self.left = left
        self.right = right
        self._building = False
        self._sync()

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        cdef ExprRow* row
        self._check_writable()
        self._value = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.value = _utf8(self._value)

    @property
    def left(self):
        return self._left

    @left.setter
    def left(self, value):
        cdef ExprRow* row
        self._check_writable()
        _require_expression("ExtractionOperator.left", value)
        self._left = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.left = _child_id(self, self._left)

    @property
    def right(self):
        return self._right

    @right.setter
    def right(self, value):
        cdef ExprRow* row
        self._check_writable()
        _require_expression("ExtractionOperator.right", value)
        self._right = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.right = _child_id(self, self._right)

    cpdef tuple children(self):
        cdef list out = []
        _append_child(out, self._left)
        _append_child(out, self._right)
        return tuple(out)

    cpdef map_children(self, object fn):
        self.left = _map_one(fn, self._left)
        self.right = _map_one(fn, self._right)

    cdef void _sync(self) except *:
        """Write the row: the common fields and a ExtractionOperator's own."""
        Expression._sync(self)
        cdef ExprRow* row = &self._arena._table.row(self.expr_id)
        row.value = _utf8(self._value)
        row.left = _child_id(self, self._left)
        row.right = _child_id(self, self._right)

    cdef Expression _shallow_copy(self):
        cdef ExtractionOperator new = ExtractionOperator.__new__(ExtractionOperator)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._value = self._value
        new._left = self._left
        new._right = self._right
        return new

    cpdef Expression copy(self, dict memo=None):
        if self._arena._sealed:
            return self
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef ExtractionOperator new = ExtractionOperator.__new__(ExtractionOperator)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._value = self._value
        new._left = _copy_value(self._left, memo)
        new._right = _copy_value(self._right, memo)
        new._sync()
        return new


cdef class And(Expression):
    """AND expression."""

    cdef str _value
    cdef object _left
    cdef object _right

    def __init__(self, value=None, left=None, right=None, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, origin=None, ExprArena arena not None):
        self.node_type = _node_types().AND
        self._bind_arena(arena, origin)
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.value = value
        self.left = left
        self.right = right
        self._building = False
        self._sync()

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        cdef ExprRow* row
        self._check_writable()
        self._value = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.value = _utf8(self._value)

    @property
    def left(self):
        return self._left

    @left.setter
    def left(self, value):
        cdef ExprRow* row
        self._check_writable()
        _require_expression("And.left", value)
        self._left = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.left = _child_id(self, self._left)

    @property
    def right(self):
        return self._right

    @right.setter
    def right(self, value):
        cdef ExprRow* row
        self._check_writable()
        _require_expression("And.right", value)
        self._right = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.right = _child_id(self, self._right)

    cpdef tuple children(self):
        cdef list out = []
        _append_child(out, self._left)
        _append_child(out, self._right)
        return tuple(out)

    cpdef map_children(self, object fn):
        self.left = _map_one(fn, self._left)
        self.right = _map_one(fn, self._right)

    cdef void _sync(self) except *:
        """Write the row: the common fields and a And's own."""
        Expression._sync(self)
        cdef ExprRow* row = &self._arena._table.row(self.expr_id)
        row.value = _utf8(self._value)
        row.left = _child_id(self, self._left)
        row.right = _child_id(self, self._right)

    cdef Expression _shallow_copy(self):
        cdef And new = And.__new__(And)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._value = self._value
        new._left = self._left
        new._right = self._right
        return new

    cpdef Expression copy(self, dict memo=None):
        if self._arena._sealed:
            return self
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef And new = And.__new__(And)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._value = self._value
        new._left = _copy_value(self._left, memo)
        new._right = _copy_value(self._right, memo)
        new._sync()
        return new


cdef class Or(Expression):
    """OR expression."""

    cdef str _value
    cdef object _left
    cdef object _right

    def __init__(self, value=None, left=None, right=None, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, origin=None, ExprArena arena not None):
        self.node_type = _node_types().OR
        self._bind_arena(arena, origin)
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.value = value
        self.left = left
        self.right = right
        self._building = False
        self._sync()

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        cdef ExprRow* row
        self._check_writable()
        self._value = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.value = _utf8(self._value)

    @property
    def left(self):
        return self._left

    @left.setter
    def left(self, value):
        cdef ExprRow* row
        self._check_writable()
        _require_expression("Or.left", value)
        self._left = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.left = _child_id(self, self._left)

    @property
    def right(self):
        return self._right

    @right.setter
    def right(self, value):
        cdef ExprRow* row
        self._check_writable()
        _require_expression("Or.right", value)
        self._right = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.right = _child_id(self, self._right)

    cpdef tuple children(self):
        cdef list out = []
        _append_child(out, self._left)
        _append_child(out, self._right)
        return tuple(out)

    cpdef map_children(self, object fn):
        self.left = _map_one(fn, self._left)
        self.right = _map_one(fn, self._right)

    cdef void _sync(self) except *:
        """Write the row: the common fields and a Or's own."""
        Expression._sync(self)
        cdef ExprRow* row = &self._arena._table.row(self.expr_id)
        row.value = _utf8(self._value)
        row.left = _child_id(self, self._left)
        row.right = _child_id(self, self._right)

    cdef Expression _shallow_copy(self):
        cdef Or new = Or.__new__(Or)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._value = self._value
        new._left = self._left
        new._right = self._right
        return new

    cpdef Expression copy(self, dict memo=None):
        if self._arena._sealed:
            return self
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef Or new = Or.__new__(Or)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._value = self._value
        new._left = _copy_value(self._left, memo)
        new._right = _copy_value(self._right, memo)
        new._sync()
        return new


cdef class Xor(Expression):
    """XOR expression."""

    cdef str _value
    cdef object _left
    cdef object _right

    def __init__(self, value=None, left=None, right=None, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, origin=None, ExprArena arena not None):
        self.node_type = _node_types().XOR
        self._bind_arena(arena, origin)
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.value = value
        self.left = left
        self.right = right
        self._building = False
        self._sync()

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        cdef ExprRow* row
        self._check_writable()
        self._value = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.value = _utf8(self._value)

    @property
    def left(self):
        return self._left

    @left.setter
    def left(self, value):
        cdef ExprRow* row
        self._check_writable()
        _require_expression("Xor.left", value)
        self._left = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.left = _child_id(self, self._left)

    @property
    def right(self):
        return self._right

    @right.setter
    def right(self, value):
        cdef ExprRow* row
        self._check_writable()
        _require_expression("Xor.right", value)
        self._right = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.right = _child_id(self, self._right)

    cpdef tuple children(self):
        cdef list out = []
        _append_child(out, self._left)
        _append_child(out, self._right)
        return tuple(out)

    cpdef map_children(self, object fn):
        self.left = _map_one(fn, self._left)
        self.right = _map_one(fn, self._right)

    cdef void _sync(self) except *:
        """Write the row: the common fields and a Xor's own."""
        Expression._sync(self)
        cdef ExprRow* row = &self._arena._table.row(self.expr_id)
        row.value = _utf8(self._value)
        row.left = _child_id(self, self._left)
        row.right = _child_id(self, self._right)

    cdef Expression _shallow_copy(self):
        cdef Xor new = Xor.__new__(Xor)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._value = self._value
        new._left = self._left
        new._right = self._right
        return new

    cpdef Expression copy(self, dict memo=None):
        if self._arena._sealed:
            return self
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef Xor new = Xor.__new__(Xor)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._value = self._value
        new._left = _copy_value(self._left, memo)
        new._right = _copy_value(self._right, memo)
        new._sync()
        return new


cdef class Not(Expression):
    """NOT expression."""

    cdef object _centre

    def __init__(self, centre=None, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, origin=None, ExprArena arena not None):
        self.node_type = _node_types().NOT
        self._bind_arena(arena, origin)
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.centre = centre
        self._building = False
        self._sync()

    @property
    def centre(self):
        return self._centre

    @centre.setter
    def centre(self, value):
        cdef ExprRow* row
        self._check_writable()
        _require_expression("Not.centre", value)
        self._centre = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.centre = _child_id(self, self._centre)

    cpdef tuple children(self):
        cdef list out = []
        _append_child(out, self._centre)
        return tuple(out)

    cpdef map_children(self, object fn):
        self.centre = _map_one(fn, self._centre)

    cdef void _sync(self) except *:
        """Write the row: the common fields and a Not's own."""
        Expression._sync(self)
        cdef ExprRow* row = &self._arena._table.row(self.expr_id)
        row.centre = _child_id(self, self._centre)

    cdef Expression _shallow_copy(self):
        cdef Not new = Not.__new__(Not)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._centre = self._centre
        return new

    cpdef Expression copy(self, dict memo=None):
        if self._arena._sealed:
            return self
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef Not new = Not.__new__(Not)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._centre = _copy_value(self._centre, memo)
        new._sync()
        return new


cdef class Dnf(Expression):
    """DNF expression."""

    cdef tuple _parameters

    def __init__(self, parameters=None, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, origin=None, ExprArena arena not None):
        self.node_type = _node_types().DNF
        self._bind_arena(arena, origin)
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.parameters = parameters
        self._building = False
        self._sync()

    @property
    def parameters(self):
        return self._parameters

    @parameters.setter
    def parameters(self, value):
        cdef ExprRow* row
        self._check_writable()
        self._parameters = _require_expression_list("Dnf.parameters", value)
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            _child_ids(row.parameters, self, self._parameters)

    cpdef tuple children(self):
        cdef list out = []
        _extend_children(out, self._parameters)
        return tuple(out)

    cpdef map_children(self, object fn):
        self.parameters = _map_list(fn, self._parameters)

    cdef void _sync(self) except *:
        """Write the row: the common fields and a Dnf's own."""
        Expression._sync(self)
        cdef ExprRow* row = &self._arena._table.row(self.expr_id)
        _child_ids(row.parameters, self, self._parameters)

    cdef Expression _shallow_copy(self):
        cdef Dnf new = Dnf.__new__(Dnf)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._parameters = self._parameters
        return new

    cpdef Expression copy(self, dict memo=None):
        if self._arena._sealed:
            return self
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef Dnf new = Dnf.__new__(Dnf)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._parameters = _copy_value(self._parameters, memo)
        new._sync()
        return new


cdef class Cnf(Expression):
    """CNF expression."""

    cdef tuple _parameters

    def __init__(self, parameters=None, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, origin=None, ExprArena arena not None):
        self.node_type = _node_types().CNF
        self._bind_arena(arena, origin)
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.parameters = parameters
        self._building = False
        self._sync()

    @property
    def parameters(self):
        return self._parameters

    @parameters.setter
    def parameters(self, value):
        cdef ExprRow* row
        self._check_writable()
        self._parameters = _require_expression_list("Cnf.parameters", value)
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            _child_ids(row.parameters, self, self._parameters)

    cpdef tuple children(self):
        cdef list out = []
        _extend_children(out, self._parameters)
        return tuple(out)

    cpdef map_children(self, object fn):
        self.parameters = _map_list(fn, self._parameters)

    cdef void _sync(self) except *:
        """Write the row: the common fields and a Cnf's own."""
        Expression._sync(self)
        cdef ExprRow* row = &self._arena._table.row(self.expr_id)
        _child_ids(row.parameters, self, self._parameters)

    cdef Expression _shallow_copy(self):
        cdef Cnf new = Cnf.__new__(Cnf)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._parameters = self._parameters
        return new

    cpdef Expression copy(self, dict memo=None):
        if self._arena._sealed:
            return self
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef Cnf new = Cnf.__new__(Cnf)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._parameters = _copy_value(self._parameters, memo)
        new._sync()
        return new


cdef class Between(Expression):
    """BETWEEN expression."""

    cdef tuple _value
    cdef object _left
    cdef object _right
    cdef object _centre

    def __init__(self, value=None, left=None, right=None, centre=None, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, origin=None, ExprArena arena not None):
        self.node_type = _node_types().BETWEEN
        self._bind_arena(arena, origin)
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.value = value
        self.left = left
        self.right = right
        self.centre = centre
        self._building = False
        self._sync()

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        cdef ExprRow* row
        self._check_writable()
        self._value = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            _flag(row, FLAG_LOWER_INCLUSIVE, self._value is not None and self._value[0])
            _flag(row, FLAG_UPPER_INCLUSIVE, self._value is not None and self._value[1])

    @property
    def left(self):
        return self._left

    @left.setter
    def left(self, value):
        cdef ExprRow* row
        self._check_writable()
        _require_expression("Between.left", value)
        self._left = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.left = _child_id(self, self._left)

    @property
    def right(self):
        return self._right

    @right.setter
    def right(self, value):
        cdef ExprRow* row
        self._check_writable()
        _require_expression("Between.right", value)
        self._right = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.right = _child_id(self, self._right)

    @property
    def centre(self):
        return self._centre

    @centre.setter
    def centre(self, value):
        cdef ExprRow* row
        self._check_writable()
        _require_expression("Between.centre", value)
        self._centre = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.centre = _child_id(self, self._centre)

    cpdef tuple children(self):
        cdef list out = []
        _append_child(out, self._left)
        _append_child(out, self._right)
        _append_child(out, self._centre)
        return tuple(out)

    cpdef map_children(self, object fn):
        self.left = _map_one(fn, self._left)
        self.right = _map_one(fn, self._right)
        self.centre = _map_one(fn, self._centre)

    cdef void _sync(self) except *:
        """Write the row: the common fields and a Between's own."""
        Expression._sync(self)
        cdef ExprRow* row = &self._arena._table.row(self.expr_id)
        row.left = _child_id(self, self._left)
        row.right = _child_id(self, self._right)
        row.centre = _child_id(self, self._centre)
        if self._value is not None:
            if self._value[0]:
                row.flags |= FLAG_LOWER_INCLUSIVE
            if self._value[1]:
                row.flags |= FLAG_UPPER_INCLUSIVE

    cdef Expression _shallow_copy(self):
        cdef Between new = Between.__new__(Between)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._value = self._value
        new._left = self._left
        new._right = self._right
        new._centre = self._centre
        return new

    cpdef Expression copy(self, dict memo=None):
        if self._arena._sealed:
            return self
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef Between new = Between.__new__(Between)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._value = _copy_value(self._value, memo)
        new._left = _copy_value(self._left, memo)
        new._right = _copy_value(self._right, memo)
        new._centre = _copy_value(self._centre, memo)
        new._sync()
        return new


cdef class Case(Expression):
    """CASE expression."""

    cdef tuple _conditions
    cdef tuple _results
    cdef object _else_result

    def __init__(self, conditions=None, results=None, else_result=None, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, origin=None, ExprArena arena not None):
        self.node_type = _node_types().CASE
        self._bind_arena(arena, origin)
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.conditions = conditions
        self.results = results
        self.else_result = else_result
        self._building = False
        self._sync()

    @property
    def conditions(self):
        return self._conditions

    @conditions.setter
    def conditions(self, value):
        cdef ExprRow* row
        self._check_writable()
        self._conditions = _require_expression_list("Case.conditions", value)
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            _child_ids(row.conditions, self, self._conditions)

    @property
    def results(self):
        return self._results

    @results.setter
    def results(self, value):
        cdef ExprRow* row
        self._check_writable()
        self._results = _require_expression_list("Case.results", value)
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            _child_ids(row.results, self, self._results)

    @property
    def else_result(self):
        return self._else_result

    @else_result.setter
    def else_result(self, value):
        cdef ExprRow* row
        self._check_writable()
        _require_expression("Case.else_result", value)
        self._else_result = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.else_result = _child_id(self, self._else_result)

    cpdef tuple children(self):
        cdef list out = []
        _extend_children(out, self._conditions)
        _extend_children(out, self._results)
        _append_child(out, self._else_result)
        return tuple(out)

    cpdef map_children(self, object fn):
        self.conditions = _map_list(fn, self._conditions)
        self.results = _map_list(fn, self._results)
        self.else_result = _map_one(fn, self._else_result)

    cdef void _sync(self) except *:
        """Write the row: the common fields and a Case's own."""
        Expression._sync(self)
        cdef ExprRow* row = &self._arena._table.row(self.expr_id)
        _child_ids(row.conditions, self, self._conditions)
        _child_ids(row.results, self, self._results)
        row.else_result = _child_id(self, self._else_result)

    cdef Expression _shallow_copy(self):
        cdef Case new = Case.__new__(Case)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._conditions = self._conditions
        new._results = self._results
        new._else_result = self._else_result
        return new

    cpdef Expression copy(self, dict memo=None):
        if self._arena._sealed:
            return self
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef Case new = Case.__new__(Case)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._conditions = _copy_value(self._conditions, memo)
        new._results = _copy_value(self._results, memo)
        new._else_result = _copy_value(self._else_result, memo)
        new._sync()
        return new


cdef class Cast(Expression):
    """CAST expression."""

    cdef str _value
    cdef object _left
    cdef tuple _parameters
    cdef object _format

    def __init__(self, value=None, left=None, parameters=None, format=None, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, origin=None, ExprArena arena not None):
        self.node_type = _node_types().CAST
        self._bind_arena(arena, origin)
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.value = value
        self.left = left
        self.parameters = parameters
        self.format = format
        self._building = False
        self._sync()

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        cdef ExprRow* row
        self._check_writable()
        self._value = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.value = _utf8(self._value)

    @property
    def left(self):
        return self._left

    @left.setter
    def left(self, value):
        cdef ExprRow* row
        self._check_writable()
        _require_expression("Cast.left", value)
        self._left = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.left = _child_id(self, self._left)

    @property
    def parameters(self):
        return self._parameters

    @parameters.setter
    def parameters(self, value):
        cdef ExprRow* row
        self._check_writable()
        self._parameters = _require_expression_list("Cast.parameters", value)
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            _child_ids(row.parameters, self, self._parameters)

    @property
    def format(self):
        return self._format

    @format.setter
    def format(self, value):
        cdef ExprRow* row
        self._check_writable()
        _require_expression("Cast.format", value)
        self._format = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.format = _child_id(self, self._format)

    cpdef tuple children(self):
        cdef list out = []
        _append_child(out, self._left)
        _extend_children(out, self._parameters)
        _append_child(out, self._format)
        return tuple(out)

    cpdef map_children(self, object fn):
        self.left = _map_one(fn, self._left)
        self.parameters = _map_list(fn, self._parameters)
        self.format = _map_one(fn, self._format)

    cdef void _sync(self) except *:
        """Write the row: the common fields and a Cast's own."""
        Expression._sync(self)
        cdef ExprRow* row = &self._arena._table.row(self.expr_id)
        row.value = _utf8(self._value)
        row.left = _child_id(self, self._left)
        _child_ids(row.parameters, self, self._parameters)
        row.format = _child_id(self, self._format)

    cdef Expression _shallow_copy(self):
        cdef Cast new = Cast.__new__(Cast)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._value = self._value
        new._left = self._left
        new._parameters = self._parameters
        new._format = self._format
        return new

    cpdef Expression copy(self, dict memo=None):
        if self._arena._sealed:
            return self
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef Cast new = Cast.__new__(Cast)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._value = self._value
        new._left = _copy_value(self._left, memo)
        new._parameters = _copy_value(self._parameters, memo)
        new._format = _copy_value(self._format, memo)
        new._sync()
        return new


cdef class Function(Expression):
    """FUNCTION expression."""

    cdef str _value
    cdef tuple _parameters
    cdef object _function_ref
    cdef str _qualified_name
    cdef tuple _span
    cdef object _match_threshold

    def __init__(self, value=None, parameters=None, function_ref=None, qualified_name=None, span=None, match_threshold=None, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, origin=None, ExprArena arena not None):
        self.node_type = _node_types().FUNCTION
        self._bind_arena(arena, origin)
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.value = value
        self.parameters = parameters
        self.function_ref = function_ref
        self.qualified_name = qualified_name
        self.span = span
        self.match_threshold = match_threshold
        self._building = False
        self._sync()

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        cdef ExprRow* row
        self._check_writable()
        self._value = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.value = _utf8(self._value)

    @property
    def parameters(self):
        return self._parameters

    @parameters.setter
    def parameters(self, value):
        cdef ExprRow* row
        self._check_writable()
        self._parameters = _require_expression_list("Function.parameters", value)
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            _child_ids(row.parameters, self, self._parameters)

    @property
    def function_ref(self):
        return self._function_ref

    @function_ref.setter
    def function_ref(self, value):
        self._check_writable()
        self._function_ref = value

    @property
    def qualified_name(self):
        return self._qualified_name

    @qualified_name.setter
    def qualified_name(self, value):
        cdef ExprRow* row
        self._check_writable()
        self._qualified_name = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.qualified_name = _utf8(self._qualified_name)

    @property
    def span(self):
        return self._span

    @span.setter
    def span(self, value):
        cdef ExprRow* row
        self._check_writable()
        self._span = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            _flag(row, FLAG_HAS_SPAN, False)
            _span_into(row, self._span)

    @property
    def match_threshold(self):
        return self._match_threshold

    @match_threshold.setter
    def match_threshold(self, value):
        cdef ExprRow* row
        self._check_writable()
        _require_optional_float("Function.match_threshold", value)
        self._match_threshold = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            _flag(row, FLAG_HAS_MATCH_THRESHOLD, self._match_threshold is not None)
            row.match_threshold = 0.0 if self._match_threshold is None else self._match_threshold

    cpdef tuple children(self):
        cdef list out = []
        _extend_children(out, self._parameters)
        return tuple(out)

    cpdef map_children(self, object fn):
        self.parameters = _map_list(fn, self._parameters)

    cdef void _sync(self) except *:
        """Write the row: the common fields and a Function's own. `function_ref` stays on this object - native code never reads it."""
        Expression._sync(self)
        cdef ExprRow* row = &self._arena._table.row(self.expr_id)
        row.value = _utf8(self._value)
        _child_ids(row.parameters, self, self._parameters)
        row.qualified_name = _utf8(self._qualified_name)
        _span_into(row, self._span)
        if self._match_threshold is not None:
            row.flags |= FLAG_HAS_MATCH_THRESHOLD
            row.match_threshold = self._match_threshold

    cdef Expression _shallow_copy(self):
        cdef Function new = Function.__new__(Function)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._value = self._value
        new._parameters = self._parameters
        new._function_ref = self._function_ref
        new._qualified_name = self._qualified_name
        new._span = self._span
        new._match_threshold = self._match_threshold
        return new

    cpdef Expression copy(self, dict memo=None):
        if self._arena._sealed:
            return self
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef Function new = Function.__new__(Function)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._value = self._value
        new._parameters = _copy_value(self._parameters, memo)
        new._function_ref = _copy_value(self._function_ref, memo)
        new._qualified_name = self._qualified_name
        new._span = _copy_value(self._span, memo)
        new._match_threshold = _copy_value(self._match_threshold, memo)
        new._sync()
        return new


cdef class Aggregator(Expression):
    """AGGREGATOR expression.

    `order` is NOT a child. ARRAY_AGG may only ORDER BY its own aggregated column
    (the binder checks that by name), so the ORDER BY expression is never bound or
    evaluated — execution sorts the aggregated values and reads only the direction.
    It is ordering metadata, and walkers must not reach it: its identifiers are
    unbound. Ordering by any other expression would make it a real, bound child.
    """

    cdef str _value
    cdef tuple _parameters
    cdef tuple _order
    cdef str _qualified_name
    cdef tuple _span
    cdef str _duplicate_treatment
    cdef object _limit
    cdef str _null_treatment
    cdef dict _over

    def __init__(self, value=None, parameters=None, order=None, qualified_name=None, span=None, duplicate_treatment=None, limit=None, null_treatment=None, over=None, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, origin=None, ExprArena arena not None):
        self.node_type = _node_types().AGGREGATOR
        self._bind_arena(arena, origin)
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.value = value
        self.parameters = parameters
        self.order = order
        self.qualified_name = qualified_name
        self.span = span
        self.duplicate_treatment = duplicate_treatment
        self.limit = limit
        self.null_treatment = null_treatment
        self.over = over
        self._building = False
        self._sync()

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        cdef ExprRow* row
        self._check_writable()
        self._value = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.value = _utf8(self._value)

    @property
    def parameters(self):
        return self._parameters

    @parameters.setter
    def parameters(self, value):
        cdef ExprRow* row
        self._check_writable()
        self._parameters = _require_expression_list("Aggregator.parameters", value)
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            _child_ids(row.parameters, self, self._parameters)

    @property
    def order(self):
        return self._order

    @order.setter
    def order(self, value):
        cdef ExprRow* row
        self._check_writable()
        self._order = _require_order_list("Aggregator.order", value)
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.order.clear()
            if self._order is not None:
                for expression, ascending in self._order:
                    row.order.push_back(pair[int64_t, cbool](_child_id(self, expression), ascending))

    @property
    def qualified_name(self):
        return self._qualified_name

    @qualified_name.setter
    def qualified_name(self, value):
        cdef ExprRow* row
        self._check_writable()
        self._qualified_name = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.qualified_name = _utf8(self._qualified_name)

    @property
    def span(self):
        return self._span

    @span.setter
    def span(self, value):
        cdef ExprRow* row
        self._check_writable()
        self._span = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            _flag(row, FLAG_HAS_SPAN, False)
            _span_into(row, self._span)

    @property
    def duplicate_treatment(self):
        return self._duplicate_treatment

    @duplicate_treatment.setter
    def duplicate_treatment(self, value):
        cdef ExprRow* row
        self._check_writable()
        self._duplicate_treatment = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.duplicate_treatment = _utf8(self._duplicate_treatment)

    @property
    def limit(self):
        return self._limit

    @limit.setter
    def limit(self, value):
        cdef ExprRow* row
        self._check_writable()
        _require_optional_int("Aggregator.limit", value)
        self._limit = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            _flag(row, FLAG_HAS_LIMIT, self._limit is not None)
            row.limit = 0 if self._limit is None else self._limit

    @property
    def null_treatment(self):
        return self._null_treatment

    @null_treatment.setter
    def null_treatment(self, value):
        cdef ExprRow* row
        self._check_writable()
        self._null_treatment = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.null_treatment = _utf8(self._null_treatment)

    @property
    def over(self):
        return self._over

    @over.setter
    def over(self, value):
        self._check_writable()
        self._over = value

    cpdef tuple children(self):
        cdef list out = []
        _extend_children(out, self._parameters)
        return tuple(out)

    cpdef map_children(self, object fn):
        self.parameters = _map_list(fn, self._parameters)

    cdef void _sync(self) except *:
        """Write the row: the common fields and a Aggregator's own. Over (the window spec) stays on this object - native code never reads it."""
        Expression._sync(self)
        cdef ExprRow* row = &self._arena._table.row(self.expr_id)
        row.value = _utf8(self._value)
        _child_ids(row.parameters, self, self._parameters)
        row.order.clear()
        if self._order is not None:
            for expression, ascending in self._order:
                row.order.push_back(pair[int64_t, cbool](_child_id(self, expression), ascending))
        row.qualified_name = _utf8(self._qualified_name)
        _span_into(row, self._span)
        row.duplicate_treatment = _utf8(self._duplicate_treatment)
        row.null_treatment = _utf8(self._null_treatment)
        if self._limit is not None:
            row.flags |= FLAG_HAS_LIMIT
            row.limit = self._limit

    cdef Expression _shallow_copy(self):
        cdef Aggregator new = Aggregator.__new__(Aggregator)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._value = self._value
        new._parameters = self._parameters
        new._order = self._order
        new._qualified_name = self._qualified_name
        new._span = self._span
        new._duplicate_treatment = self._duplicate_treatment
        new._limit = self._limit
        new._null_treatment = self._null_treatment
        new._over = self._over
        return new

    cpdef Expression copy(self, dict memo=None):
        if self._arena._sealed:
            return self
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef Aggregator new = Aggregator.__new__(Aggregator)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._value = self._value
        new._parameters = _copy_value(self._parameters, memo)
        new._order = _copy_value(self._order, memo)
        new._qualified_name = self._qualified_name
        new._span = _copy_value(self._span, memo)
        new._duplicate_treatment = self._duplicate_treatment
        new._limit = _copy_value(self._limit, memo)
        new._null_treatment = self._null_treatment
        new._over = _copy_value(self._over, memo)
        new._sync()
        return new


cdef class Nested(Expression):
    """NESTED expression."""

    cdef object _centre

    def __init__(self, centre=None, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, origin=None, ExprArena arena not None):
        self.node_type = _node_types().NESTED
        self._bind_arena(arena, origin)
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.centre = centre
        self._building = False
        self._sync()

    @property
    def centre(self):
        return self._centre

    @centre.setter
    def centre(self, value):
        cdef ExprRow* row
        self._check_writable()
        _require_expression("Nested.centre", value)
        self._centre = value
        if not self._building:
            row = &self._arena._table.row(self.expr_id)
            row.centre = _child_id(self, self._centre)

    cpdef tuple children(self):
        cdef list out = []
        _append_child(out, self._centre)
        return tuple(out)

    cpdef map_children(self, object fn):
        self.centre = _map_one(fn, self._centre)

    cdef void _sync(self) except *:
        """Write the row: the common fields and a Nested's own."""
        Expression._sync(self)
        cdef ExprRow* row = &self._arena._table.row(self.expr_id)
        row.centre = _child_id(self, self._centre)

    cdef Expression _shallow_copy(self):
        cdef Nested new = Nested.__new__(Nested)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._centre = self._centre
        return new

    cpdef Expression copy(self, dict memo=None):
        if self._arena._sealed:
            return self
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef Nested new = Nested.__new__(Nested)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._centre = _copy_value(self._centre, memo)
        new._sync()
        return new


cdef class Subquery(Expression):
    """SUBQUERY expression."""

    cdef object _value

    def __init__(self, value=None, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, origin=None, ExprArena arena not None):
        self.node_type = _node_types().SUBQUERY
        self._bind_arena(arena, origin)
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.value = value
        self._building = False
        self._sync()

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._check_writable()
        self._value = value

    cpdef tuple children(self):
        return ()

    cpdef map_children(self, object fn):
        return None

    cdef void _sync(self) except *:
        """Write the row: the common fields and a Subquery's own. The subquery's plan stays on this object - native code never reads it."""
        Expression._sync(self)

    cdef Expression _shallow_copy(self):
        cdef Subquery new = Subquery.__new__(Subquery)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._value = self._value
        return new

    cpdef Expression copy(self, dict memo=None):
        if self._arena._sealed:
            return self
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef Subquery new = Subquery.__new__(Subquery)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._value = _copy_value(self._value, memo)
        new._sync()
        return new


cdef class Wildcard(Expression):
    """WILDCARD expression."""

    cdef object _value
    cdef object _except_columns

    def __init__(self, value=None, except_columns=None, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, origin=None, ExprArena arena not None):
        self.node_type = _node_types().WILDCARD
        self._bind_arena(arena, origin)
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.value = value
        self.except_columns = except_columns
        self._building = False
        self._sync()

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._check_writable()
        self._value = value

    @property
    def except_columns(self):
        return self._except_columns

    @except_columns.setter
    def except_columns(self, value):
        self._check_writable()
        self._except_columns = value

    cpdef tuple children(self):
        return ()

    cpdef map_children(self, object fn):
        return None

    cdef void _sync(self) except *:
        """Write the row: the common fields and a Wildcard's own. The wildcard's qualifier and exclusions stays on this object - native code never reads it."""
        Expression._sync(self)

    cdef Expression _shallow_copy(self):
        cdef Wildcard new = Wildcard.__new__(Wildcard)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._value = self._value
        new._except_columns = self._except_columns
        return new

    cpdef Expression copy(self, dict memo=None):
        if self._arena._sealed:
            return self
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef Wildcard new = Wildcard.__new__(Wildcard)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._value = _copy_value(self._value, memo)
        new._except_columns = _copy_value(self._except_columns, memo)
        new._sync()
        return new


cdef class Evaluated(Expression):
    """EVALUATED expression."""

    cdef object _value

    def __init__(self, value=None, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, origin=None, ExprArena arena not None):
        self.node_type = _node_types().EVALUATED
        self._bind_arena(arena, origin)
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.value = value
        self._building = False
        self._sync()

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._check_writable()
        self._value = value

    cpdef tuple children(self):
        return ()

    cpdef map_children(self, object fn):
        return None

    cdef void _sync(self) except *:
        """Write the row: the common fields and a Evaluated's own. The evaluated value stays on this object - native code never reads it."""
        Expression._sync(self)

    cdef Expression _shallow_copy(self):
        cdef Evaluated new = Evaluated.__new__(Evaluated)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._value = self._value
        return new

    cpdef Expression copy(self, dict memo=None):
        if self._arena._sealed:
            return self
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef Evaluated new = Evaluated.__new__(Evaluated)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._value = _copy_value(self._value, memo)
        new._sync()
        return new


EXPRESSION_TYPES = frozenset({LogicalColumn, Literal, Comparison, BinaryOperator, UnaryOperator, ExtractionOperator, And, Or, Xor, Not, Dnf, Cnf, Between, Case, Cast, Function, Aggregator, Nested, Subquery, Wildcard, Evaluated})

# The expression classes that declare each field — for the generic walkers that
# read one field off expressions of any type (`type(expr) in
# expressions_with("left")`). Mirrors plan_steps.steps_with. Built once from the
# classes' own declared attributes (cdef public/readonly fields and properties,
# which are the only attributes an expression has); an unknown field name is a
# KeyError, never an empty set, so a misspelt field cannot read as "declared by
# nothing".
cdef dict _EXPRESSIONS_WITH = None
cdef tuple _FIELD_DESCRIPTORS = (type(Expression.expr_id), property)


cpdef frozenset expressions_with(str field):
    global _EXPRESSIONS_WITH
    cdef dict by_field
    if _EXPRESSIONS_WITH is None:
        by_field = {}
        for cls in EXPRESSION_TYPES:
            for ancestor in cls.__mro__:
                for name, member in vars(ancestor).items():
                    if name[0] != "_" and type(member) in _FIELD_DESCRIPTORS:
                        by_field.setdefault(name, set()).add(cls)
        _EXPRESSIONS_WITH = {name: frozenset(classes) for name, classes in by_field.items()}
    return _EXPRESSIONS_WITH[field]



cpdef object current_name_of(object node):
    """The name a column reference answers to here (its alias, else its source
    name); None for any other expression, which has no such name."""
    if node.node_type == _node_types().IDENTIFIER:
        return node.current_name
    return None


cpdef bint is_expression(object value):
    """Whether `value` is an expression node — the one test for it (exact types)."""
    return _is_expression(value)


cpdef object rewrite_children(object expr, object fn, bint share=False):
    """Copy-on-write child rewrite: `fn` over every child of `expr`; `expr` itself
    when every child comes back identical, else a copy of `expr` carrying the
    rewritten children. The input tree is never modified — for rewrites whose input
    may be shared with other holders.

    share=False: the copy is a deep `copy()` (same expr_id; unchanged children are the
    copy's own). share=True: the copy is `replace()` (fresh expr_id; unchanged children
    are the ORIGINAL objects, shared with the input). Once the arena is sealed there
    are no deep copies (a sealed expression IS its copy), so the rebuild is always
    the `replace()` form."""
    cdef tuple old_children = expr.children()
    cdef list new_children = [fn(child) for child in old_children]
    cdef Py_ssize_t i
    cdef bint changed = False
    for i in range(len(old_children)):
        if new_children[i] is not old_children[i]:
            changed = True
            break
    if not changed:
        return expr
    rebuilt = expr.replace() if share or (<Expression>expr)._arena._sealed else expr.copy()
    rebuilt.map_children(_ChildPicker(old_children, new_children))
    return rebuilt


cdef class _ChildPicker:
    """map_children callback for rewrite_children: walks the copy's children in
    the same order children() listed the original's, keeping the copy's own child
    where it was not rewritten and substituting the rewritten one where it was."""

    cdef tuple _old
    cdef list _new
    cdef Py_ssize_t _index

    def __init__(self, tuple old_children, list new_children):
        self._old = old_children
        self._new = new_children
        self._index = 0

    def __call__(self, copied_child):
        cdef Py_ssize_t index = self._index
        self._index = index + 1
        if self._new[index] is self._old[index]:
            return copied_child
        return self._new[index]
