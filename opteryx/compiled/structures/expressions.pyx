# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

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
"""

from opteryx.compiled.functions.random_helper import random_string_c


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


cdef inline void _require_expression(str name, object value):
    if value is not None and not _is_expression(value):
        raise TypeError(f"{name} must be an expression or None, got {type(value).__name__}")


cdef inline list _require_expression_list(str name, object value):
    if value is None:
        return None
    if type(value) is not list:
        raise TypeError(f"{name} must be a list of expressions, got {type(value).__name__}")
    for item in value:
        if not _is_expression(item):
            raise TypeError(f"{name} must hold expressions, got {type(item).__name__}")
    return value


cdef inline list _require_order_list(str name, object value):
    """An aggregate's ORDER BY: a list of (expression, ascending) pairs."""
    if value is None:
        return None
    if type(value) is not list:
        raise TypeError(f"{name} must be a list of (expression, bool) pairs, got {type(value).__name__}")
    for item in value:
        if type(item) is not tuple or len(item) != 2 or not _is_expression(item[0]) or type(item[1]) is not bool:
            raise TypeError(f"{name} must hold (expression, bool) pairs, got {item!r}")
    return value


cdef class Expression:
    """What every expression has: identity, naming, the column it binds to."""

    cdef readonly object node_type
    cdef public str uuid
    cdef public str alias
    cdef public str query_column
    cdef public object schema_column
    cdef public set relations
    cdef bint _folded_by_constant_folding
    cdef bint _do_not_create_column

    cdef void _set_common(self, str alias, str query_column, object schema_column, set relations):
        self.alias = alias
        self.query_column = query_column
        self.schema_column = schema_column
        self.relations = relations

    @property
    def folded_by_constant_folding(self):
        """Set by constant folding on a tree it folded, so its second pass skips it."""
        return self._folded_by_constant_folding

    @folded_by_constant_folding.setter
    def folded_by_constant_folding(self, value):
        _require_bool("folded_by_constant_folding", value)
        self._folded_by_constant_folding = value

    @property
    def do_not_create_column(self):
        """Planner-to-binder directive: bind this expression without minting a
        derived output column for it (a synthesized join/filter condition)."""
        return self._do_not_create_column

    @do_not_create_column.setter
    def do_not_create_column(self, value):
        _require_bool("do_not_create_column", value)
        self._do_not_create_column = value

    cpdef tuple children(self):
        return ()

    cpdef map_children(self, object fn):
        return None

    cdef void _copy_common_into(self, Expression target, dict memo):
        target.uuid = self.uuid
        target.alias = self.alias
        target.query_column = self.query_column
        target.schema_column = _copy_value(self.schema_column, memo)
        target.relations = _copy_value(self.relations, memo)
        target._folded_by_constant_folding = self._folded_by_constant_folding
        target._do_not_create_column = self._do_not_create_column

    cpdef Expression copy(self, dict memo=None):
        raise NotImplementedError(f"{type(self).__name__}.copy")

    cdef Expression _shallow_copy(self):
        raise NotImplementedError(f"{type(self).__name__}._shallow_copy")

    cdef void _share_common_into(self, Expression target):
        target.alias = self.alias
        target.query_column = self.query_column
        target.schema_column = self.schema_column
        target.relations = self.relations
        target._folded_by_constant_folding = self._folded_by_constant_folding
        target._do_not_create_column = self._do_not_create_column

    def replace(self, **overrides):
        """A NEW node of the same type with this one's fields, `overrides` applied
        through the fields' own (enforcing) setters, and a fresh uuid. Children not
        overridden are SHARED (list fields get their own list); an undeclared field
        name raises. For rewrites that must not edit a node other holders can see."""
        cdef Expression new = self._shallow_copy()
        new.uuid = random_string_c(32, None)
        for name, value in overrides.items():
            setattr(new, name, value)
        return new

    def __repr__(self):
        return f"<{type(self).__name__} {self.node_type.name}>"


cdef inline Expression _memo_hit(Expression self, dict memo):
    return memo.get(id(self))


# ---------------------------------------------------------------------------
# helpers shared by the child-carrying classes
# ---------------------------------------------------------------------------


cdef inline void _append_child(list out, object child):
    if child is not None:
        out.append(child)


cdef inline void _extend_children(list out, list children):
    if children is not None:
        for child in children:
            out.append(child)


cdef inline object _map_one(object fn, object child):
    return None if child is None else fn(child)


cdef inline list _map_list(object fn, list children):
    if children is None:
        return None
    return [fn(child) for child in children]


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
    ):
        if node_type != _node_types().IDENTIFIER:
            raise TypeError(f"LogicalColumn is an IDENTIFIER, got {node_type!r}")
        self.node_type = node_type
        self._set_common(alias, query_column, schema_column, None)
        self._source_column = source_column
        self._source = source
        self.is_outer_reference = is_outer_reference
        self._outer_relation = outer_relation
        self._span = span

    @property
    def source_column(self):
        return self._source_column

    @source_column.setter
    def source_column(self, str value):
        self._source_column = value

    @property
    def source(self):
        return self._source

    @source.setter
    def source(self, str value):
        self._source = value

    @property
    def is_outer_reference(self):
        return self._is_outer_reference

    @is_outer_reference.setter
    def is_outer_reference(self, value):
        _require_bool("LogicalColumn.is_outer_reference", value)
        self._is_outer_reference = value

    @property
    def outer_relation(self):
        return self._outer_relation

    @outer_relation.setter
    def outer_relation(self, value):
        self._outer_relation = value

    @property
    def span(self):
        return self._span

    @span.setter
    def span(self, tuple value):
        self._span = value

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
        # A column reference copies as a FRESH reference with a detached plain
        # schema column (to_schema_column), exactly as the pre-typed class did;
        # it is not memoized, matching that class's memo-less copy().
        return LogicalColumn(
            self.node_type,
            self._source_column,
            self._source,
            self.alias,
            None if self.schema_column is None else self.schema_column.to_schema_column(),
            self.query_column,
            self._is_outer_reference,
            self._outer_relation,
            self._span,
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

    def __init__(self, value=None, type=None, is_wildcard_order_position=False, rlike_compiled=False, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, uuid=None):
        self.node_type = _node_types().LITERAL
        self.uuid = random_string_c(32, None) if uuid is None else uuid
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.value = value
        self.type = type
        self.is_wildcard_order_position = is_wildcard_order_position
        self.rlike_compiled = rlike_compiled

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, value):
        self._type = value

    @property
    def is_wildcard_order_position(self):
        return self._is_wildcard_order_position

    @is_wildcard_order_position.setter
    def is_wildcard_order_position(self, value):
        _require_bool("Literal.is_wildcard_order_position", value)
        self._is_wildcard_order_position = value

    @property
    def rlike_compiled(self):
        return self._rlike_compiled

    @rlike_compiled.setter
    def rlike_compiled(self, value):
        _require_bool("Literal.rlike_compiled", value)
        self._rlike_compiled = value

    cpdef tuple children(self):
        return ()

    cpdef map_children(self, object fn):
        return None

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
        return new


cdef class Comparison(Expression):
    """COMPARISON_OPERATOR expression."""

    cdef str _value
    cdef object _left
    cdef object _right
    cdef bint _negated
    cdef object _like_selectivity_decay

    def __init__(self, value=None, left=None, right=None, negated=False, like_selectivity_decay=None, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, uuid=None):
        self.node_type = _node_types().COMPARISON_OPERATOR
        self.uuid = random_string_c(32, None) if uuid is None else uuid
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.value = value
        self.left = left
        self.right = right
        self.negated = negated
        self.like_selectivity_decay = like_selectivity_decay

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    @property
    def left(self):
        return self._left

    @left.setter
    def left(self, value):
        _require_expression("Comparison.left", value)
        self._left = value

    @property
    def right(self):
        return self._right

    @right.setter
    def right(self, value):
        _require_expression("Comparison.right", value)
        self._right = value

    @property
    def negated(self):
        return self._negated

    @negated.setter
    def negated(self, value):
        _require_bool("Comparison.negated", value)
        self._negated = value

    @property
    def like_selectivity_decay(self):
        return self._like_selectivity_decay

    @like_selectivity_decay.setter
    def like_selectivity_decay(self, value):
        _require_optional_float("Comparison.like_selectivity_decay", value)
        self._like_selectivity_decay = value

    cpdef tuple children(self):
        cdef list out = []
        _append_child(out, self._left)
        _append_child(out, self._right)
        return tuple(out)

    cpdef map_children(self, object fn):
        self.left = _map_one(fn, self._left)
        self.right = _map_one(fn, self._right)

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
        return new


cdef class BinaryOperator(Expression):
    """BINARY_OPERATOR expression."""

    cdef str _value
    cdef object _left
    cdef object _right

    def __init__(self, value=None, left=None, right=None, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, uuid=None):
        self.node_type = _node_types().BINARY_OPERATOR
        self.uuid = random_string_c(32, None) if uuid is None else uuid
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.value = value
        self.left = left
        self.right = right

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    @property
    def left(self):
        return self._left

    @left.setter
    def left(self, value):
        _require_expression("BinaryOperator.left", value)
        self._left = value

    @property
    def right(self):
        return self._right

    @right.setter
    def right(self, value):
        _require_expression("BinaryOperator.right", value)
        self._right = value

    cpdef tuple children(self):
        cdef list out = []
        _append_child(out, self._left)
        _append_child(out, self._right)
        return tuple(out)

    cpdef map_children(self, object fn):
        self.left = _map_one(fn, self._left)
        self.right = _map_one(fn, self._right)

    cdef Expression _shallow_copy(self):
        cdef BinaryOperator new = BinaryOperator.__new__(BinaryOperator)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._value = self._value
        new._left = self._left
        new._right = self._right
        return new

    cpdef Expression copy(self, dict memo=None):
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
        return new


cdef class UnaryOperator(Expression):
    """UNARY_OPERATOR expression.

    EXISTS is a UNARY_OPERATOR too, and carries its SUBQUERY in `parameters` rather
    than `centre` (logical_planner_builders' exists builder).
    """

    cdef str _value
    cdef object _centre
    cdef list _parameters
    cdef bint _negated

    def __init__(self, value=None, centre=None, parameters=None, negated=False, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, uuid=None):
        self.node_type = _node_types().UNARY_OPERATOR
        self.uuid = random_string_c(32, None) if uuid is None else uuid
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.value = value
        self.centre = centre
        self.parameters = parameters
        self.negated = negated

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    @property
    def centre(self):
        return self._centre

    @centre.setter
    def centre(self, value):
        _require_expression("UnaryOperator.centre", value)
        self._centre = value

    @property
    def parameters(self):
        return self._parameters

    @parameters.setter
    def parameters(self, value):
        self._parameters = _require_expression_list("UnaryOperator.parameters", value)

    @property
    def negated(self):
        return self._negated

    @negated.setter
    def negated(self, value):
        _require_bool("UnaryOperator.negated", value)
        self._negated = value

    cpdef tuple children(self):
        cdef list out = []
        _append_child(out, self._centre)
        _extend_children(out, self._parameters)
        return tuple(out)

    cpdef map_children(self, object fn):
        self.centre = _map_one(fn, self._centre)
        self.parameters = _map_list(fn, self._parameters)

    cdef Expression _shallow_copy(self):
        cdef UnaryOperator new = UnaryOperator.__new__(UnaryOperator)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._value = self._value
        new._centre = self._centre
        new._parameters = None if self._parameters is None else list(self._parameters)
        new._negated = self._negated
        return new

    cpdef Expression copy(self, dict memo=None):
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
        return new


cdef class ExtractionOperator(Expression):
    """EXTRACTION_OPERATOR expression."""

    cdef str _value
    cdef object _left
    cdef object _right

    def __init__(self, value=None, left=None, right=None, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, uuid=None):
        self.node_type = _node_types().EXTRACTION_OPERATOR
        self.uuid = random_string_c(32, None) if uuid is None else uuid
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.value = value
        self.left = left
        self.right = right

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    @property
    def left(self):
        return self._left

    @left.setter
    def left(self, value):
        _require_expression("ExtractionOperator.left", value)
        self._left = value

    @property
    def right(self):
        return self._right

    @right.setter
    def right(self, value):
        _require_expression("ExtractionOperator.right", value)
        self._right = value

    cpdef tuple children(self):
        cdef list out = []
        _append_child(out, self._left)
        _append_child(out, self._right)
        return tuple(out)

    cpdef map_children(self, object fn):
        self.left = _map_one(fn, self._left)
        self.right = _map_one(fn, self._right)

    cdef Expression _shallow_copy(self):
        cdef ExtractionOperator new = ExtractionOperator.__new__(ExtractionOperator)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._value = self._value
        new._left = self._left
        new._right = self._right
        return new

    cpdef Expression copy(self, dict memo=None):
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
        return new


cdef class And(Expression):
    """AND expression."""

    cdef str _value
    cdef object _left
    cdef object _right

    def __init__(self, value=None, left=None, right=None, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, uuid=None):
        self.node_type = _node_types().AND
        self.uuid = random_string_c(32, None) if uuid is None else uuid
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.value = value
        self.left = left
        self.right = right

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    @property
    def left(self):
        return self._left

    @left.setter
    def left(self, value):
        _require_expression("And.left", value)
        self._left = value

    @property
    def right(self):
        return self._right

    @right.setter
    def right(self, value):
        _require_expression("And.right", value)
        self._right = value

    cpdef tuple children(self):
        cdef list out = []
        _append_child(out, self._left)
        _append_child(out, self._right)
        return tuple(out)

    cpdef map_children(self, object fn):
        self.left = _map_one(fn, self._left)
        self.right = _map_one(fn, self._right)

    cdef Expression _shallow_copy(self):
        cdef And new = And.__new__(And)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._value = self._value
        new._left = self._left
        new._right = self._right
        return new

    cpdef Expression copy(self, dict memo=None):
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
        return new


cdef class Or(Expression):
    """OR expression."""

    cdef str _value
    cdef object _left
    cdef object _right

    def __init__(self, value=None, left=None, right=None, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, uuid=None):
        self.node_type = _node_types().OR
        self.uuid = random_string_c(32, None) if uuid is None else uuid
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.value = value
        self.left = left
        self.right = right

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    @property
    def left(self):
        return self._left

    @left.setter
    def left(self, value):
        _require_expression("Or.left", value)
        self._left = value

    @property
    def right(self):
        return self._right

    @right.setter
    def right(self, value):
        _require_expression("Or.right", value)
        self._right = value

    cpdef tuple children(self):
        cdef list out = []
        _append_child(out, self._left)
        _append_child(out, self._right)
        return tuple(out)

    cpdef map_children(self, object fn):
        self.left = _map_one(fn, self._left)
        self.right = _map_one(fn, self._right)

    cdef Expression _shallow_copy(self):
        cdef Or new = Or.__new__(Or)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._value = self._value
        new._left = self._left
        new._right = self._right
        return new

    cpdef Expression copy(self, dict memo=None):
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
        return new


cdef class Xor(Expression):
    """XOR expression."""

    cdef str _value
    cdef object _left
    cdef object _right

    def __init__(self, value=None, left=None, right=None, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, uuid=None):
        self.node_type = _node_types().XOR
        self.uuid = random_string_c(32, None) if uuid is None else uuid
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.value = value
        self.left = left
        self.right = right

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    @property
    def left(self):
        return self._left

    @left.setter
    def left(self, value):
        _require_expression("Xor.left", value)
        self._left = value

    @property
    def right(self):
        return self._right

    @right.setter
    def right(self, value):
        _require_expression("Xor.right", value)
        self._right = value

    cpdef tuple children(self):
        cdef list out = []
        _append_child(out, self._left)
        _append_child(out, self._right)
        return tuple(out)

    cpdef map_children(self, object fn):
        self.left = _map_one(fn, self._left)
        self.right = _map_one(fn, self._right)

    cdef Expression _shallow_copy(self):
        cdef Xor new = Xor.__new__(Xor)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._value = self._value
        new._left = self._left
        new._right = self._right
        return new

    cpdef Expression copy(self, dict memo=None):
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
        return new


cdef class Not(Expression):
    """NOT expression."""

    cdef object _centre

    def __init__(self, centre=None, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, uuid=None):
        self.node_type = _node_types().NOT
        self.uuid = random_string_c(32, None) if uuid is None else uuid
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.centre = centre

    @property
    def centre(self):
        return self._centre

    @centre.setter
    def centre(self, value):
        _require_expression("Not.centre", value)
        self._centre = value

    cpdef tuple children(self):
        cdef list out = []
        _append_child(out, self._centre)
        return tuple(out)

    cpdef map_children(self, object fn):
        self.centre = _map_one(fn, self._centre)

    cdef Expression _shallow_copy(self):
        cdef Not new = Not.__new__(Not)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._centre = self._centre
        return new

    cpdef Expression copy(self, dict memo=None):
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
        return new


cdef class Dnf(Expression):
    """DNF expression."""

    cdef list _parameters

    def __init__(self, parameters=None, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, uuid=None):
        self.node_type = _node_types().DNF
        self.uuid = random_string_c(32, None) if uuid is None else uuid
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.parameters = parameters

    @property
    def parameters(self):
        return self._parameters

    @parameters.setter
    def parameters(self, value):
        self._parameters = _require_expression_list("Dnf.parameters", value)

    cpdef tuple children(self):
        cdef list out = []
        _extend_children(out, self._parameters)
        return tuple(out)

    cpdef map_children(self, object fn):
        self.parameters = _map_list(fn, self._parameters)

    cdef Expression _shallow_copy(self):
        cdef Dnf new = Dnf.__new__(Dnf)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._parameters = None if self._parameters is None else list(self._parameters)
        return new

    cpdef Expression copy(self, dict memo=None):
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
        return new


cdef class Cnf(Expression):
    """CNF expression."""

    cdef list _parameters

    def __init__(self, parameters=None, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, uuid=None):
        self.node_type = _node_types().CNF
        self.uuid = random_string_c(32, None) if uuid is None else uuid
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.parameters = parameters

    @property
    def parameters(self):
        return self._parameters

    @parameters.setter
    def parameters(self, value):
        self._parameters = _require_expression_list("Cnf.parameters", value)

    cpdef tuple children(self):
        cdef list out = []
        _extend_children(out, self._parameters)
        return tuple(out)

    cpdef map_children(self, object fn):
        self.parameters = _map_list(fn, self._parameters)

    cdef Expression _shallow_copy(self):
        cdef Cnf new = Cnf.__new__(Cnf)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._parameters = None if self._parameters is None else list(self._parameters)
        return new

    cpdef Expression copy(self, dict memo=None):
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
        return new


cdef class Between(Expression):
    """BETWEEN expression."""

    cdef tuple _value
    cdef object _left
    cdef object _right
    cdef object _centre

    def __init__(self, value=None, left=None, right=None, centre=None, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, uuid=None):
        self.node_type = _node_types().BETWEEN
        self.uuid = random_string_c(32, None) if uuid is None else uuid
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.value = value
        self.left = left
        self.right = right
        self.centre = centre

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    @property
    def left(self):
        return self._left

    @left.setter
    def left(self, value):
        _require_expression("Between.left", value)
        self._left = value

    @property
    def right(self):
        return self._right

    @right.setter
    def right(self, value):
        _require_expression("Between.right", value)
        self._right = value

    @property
    def centre(self):
        return self._centre

    @centre.setter
    def centre(self, value):
        _require_expression("Between.centre", value)
        self._centre = value

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
        return new


cdef class Case(Expression):
    """CASE expression."""

    cdef list _conditions
    cdef list _results
    cdef object _else_result

    def __init__(self, conditions=None, results=None, else_result=None, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, uuid=None):
        self.node_type = _node_types().CASE
        self.uuid = random_string_c(32, None) if uuid is None else uuid
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.conditions = conditions
        self.results = results
        self.else_result = else_result

    @property
    def conditions(self):
        return self._conditions

    @conditions.setter
    def conditions(self, value):
        self._conditions = _require_expression_list("Case.conditions", value)

    @property
    def results(self):
        return self._results

    @results.setter
    def results(self, value):
        self._results = _require_expression_list("Case.results", value)

    @property
    def else_result(self):
        return self._else_result

    @else_result.setter
    def else_result(self, value):
        _require_expression("Case.else_result", value)
        self._else_result = value

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

    cdef Expression _shallow_copy(self):
        cdef Case new = Case.__new__(Case)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._conditions = None if self._conditions is None else list(self._conditions)
        new._results = None if self._results is None else list(self._results)
        new._else_result = self._else_result
        return new

    cpdef Expression copy(self, dict memo=None):
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
        return new


cdef class Cast(Expression):
    """CAST expression."""

    cdef str _value
    cdef object _left
    cdef list _parameters
    cdef object _format

    def __init__(self, value=None, left=None, parameters=None, format=None, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, uuid=None):
        self.node_type = _node_types().CAST
        self.uuid = random_string_c(32, None) if uuid is None else uuid
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.value = value
        self.left = left
        self.parameters = parameters
        self.format = format

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    @property
    def left(self):
        return self._left

    @left.setter
    def left(self, value):
        _require_expression("Cast.left", value)
        self._left = value

    @property
    def parameters(self):
        return self._parameters

    @parameters.setter
    def parameters(self, value):
        self._parameters = _require_expression_list("Cast.parameters", value)

    @property
    def format(self):
        return self._format

    @format.setter
    def format(self, value):
        _require_expression("Cast.format", value)
        self._format = value

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

    cdef Expression _shallow_copy(self):
        cdef Cast new = Cast.__new__(Cast)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._value = self._value
        new._left = self._left
        new._parameters = None if self._parameters is None else list(self._parameters)
        new._format = self._format
        return new

    cpdef Expression copy(self, dict memo=None):
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
        return new


cdef class Function(Expression):
    """FUNCTION expression."""

    cdef str _value
    cdef list _parameters
    cdef object _function_ref
    cdef str _qualified_name
    cdef tuple _span
    cdef object _match_threshold

    def __init__(self, value=None, parameters=None, function_ref=None, qualified_name=None, span=None, match_threshold=None, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, uuid=None):
        self.node_type = _node_types().FUNCTION
        self.uuid = random_string_c(32, None) if uuid is None else uuid
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.value = value
        self.parameters = parameters
        self.function_ref = function_ref
        self.qualified_name = qualified_name
        self.span = span
        self.match_threshold = match_threshold

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    @property
    def parameters(self):
        return self._parameters

    @parameters.setter
    def parameters(self, value):
        self._parameters = _require_expression_list("Function.parameters", value)

    @property
    def function_ref(self):
        return self._function_ref

    @function_ref.setter
    def function_ref(self, value):
        self._function_ref = value

    @property
    def qualified_name(self):
        return self._qualified_name

    @qualified_name.setter
    def qualified_name(self, value):
        self._qualified_name = value

    @property
    def span(self):
        return self._span

    @span.setter
    def span(self, value):
        self._span = value

    @property
    def match_threshold(self):
        return self._match_threshold

    @match_threshold.setter
    def match_threshold(self, value):
        _require_optional_float("Function.match_threshold", value)
        self._match_threshold = value

    cpdef tuple children(self):
        cdef list out = []
        _extend_children(out, self._parameters)
        return tuple(out)

    cpdef map_children(self, object fn):
        self.parameters = _map_list(fn, self._parameters)

    cdef Expression _shallow_copy(self):
        cdef Function new = Function.__new__(Function)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._value = self._value
        new._parameters = None if self._parameters is None else list(self._parameters)
        new._function_ref = self._function_ref
        new._qualified_name = self._qualified_name
        new._span = self._span
        new._match_threshold = self._match_threshold
        return new

    cpdef Expression copy(self, dict memo=None):
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
    cdef list _parameters
    cdef list _order
    cdef str _qualified_name
    cdef tuple _span
    cdef str _duplicate_treatment
    cdef object _limit
    cdef str _null_treatment
    cdef dict _over

    def __init__(self, value=None, parameters=None, order=None, qualified_name=None, span=None, duplicate_treatment=None, limit=None, null_treatment=None, over=None, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, uuid=None):
        self.node_type = _node_types().AGGREGATOR
        self.uuid = random_string_c(32, None) if uuid is None else uuid
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

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    @property
    def parameters(self):
        return self._parameters

    @parameters.setter
    def parameters(self, value):
        self._parameters = _require_expression_list("Aggregator.parameters", value)

    @property
    def order(self):
        return self._order

    @order.setter
    def order(self, value):
        self._order = _require_order_list("Aggregator.order", value)

    @property
    def qualified_name(self):
        return self._qualified_name

    @qualified_name.setter
    def qualified_name(self, value):
        self._qualified_name = value

    @property
    def span(self):
        return self._span

    @span.setter
    def span(self, value):
        self._span = value

    @property
    def duplicate_treatment(self):
        return self._duplicate_treatment

    @duplicate_treatment.setter
    def duplicate_treatment(self, value):
        self._duplicate_treatment = value

    @property
    def limit(self):
        return self._limit

    @limit.setter
    def limit(self, value):
        _require_optional_int("Aggregator.limit", value)
        self._limit = value

    @property
    def null_treatment(self):
        return self._null_treatment

    @null_treatment.setter
    def null_treatment(self, value):
        self._null_treatment = value

    @property
    def over(self):
        return self._over

    @over.setter
    def over(self, value):
        self._over = value

    cpdef tuple children(self):
        cdef list out = []
        _extend_children(out, self._parameters)
        return tuple(out)

    cpdef map_children(self, object fn):
        self.parameters = _map_list(fn, self._parameters)

    cdef Expression _shallow_copy(self):
        cdef Aggregator new = Aggregator.__new__(Aggregator)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._value = self._value
        new._parameters = None if self._parameters is None else list(self._parameters)
        new._order = None if self._order is None else list(self._order)
        new._qualified_name = self._qualified_name
        new._span = self._span
        new._duplicate_treatment = self._duplicate_treatment
        new._limit = self._limit
        new._null_treatment = self._null_treatment
        new._over = self._over
        return new

    cpdef Expression copy(self, dict memo=None):
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
        return new


cdef class Nested(Expression):
    """NESTED expression."""

    cdef object _centre

    def __init__(self, centre=None, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, uuid=None):
        self.node_type = _node_types().NESTED
        self.uuid = random_string_c(32, None) if uuid is None else uuid
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.centre = centre

    @property
    def centre(self):
        return self._centre

    @centre.setter
    def centre(self, value):
        _require_expression("Nested.centre", value)
        self._centre = value

    cpdef tuple children(self):
        cdef list out = []
        _append_child(out, self._centre)
        return tuple(out)

    cpdef map_children(self, object fn):
        self.centre = _map_one(fn, self._centre)

    cdef Expression _shallow_copy(self):
        cdef Nested new = Nested.__new__(Nested)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._centre = self._centre
        return new

    cpdef Expression copy(self, dict memo=None):
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
        return new


cdef class Subquery(Expression):
    """SUBQUERY expression."""

    cdef object _value

    def __init__(self, value=None, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, uuid=None):
        self.node_type = _node_types().SUBQUERY
        self.uuid = random_string_c(32, None) if uuid is None else uuid
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.value = value

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    cpdef tuple children(self):
        return ()

    cpdef map_children(self, object fn):
        return None

    cdef Expression _shallow_copy(self):
        cdef Subquery new = Subquery.__new__(Subquery)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._value = self._value
        return new

    cpdef Expression copy(self, dict memo=None):
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
        return new


cdef class Wildcard(Expression):
    """WILDCARD expression."""

    cdef object _value
    cdef object _except_columns

    def __init__(self, value=None, except_columns=None, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, uuid=None):
        self.node_type = _node_types().WILDCARD
        self.uuid = random_string_c(32, None) if uuid is None else uuid
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.value = value
        self.except_columns = except_columns

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    @property
    def except_columns(self):
        return self._except_columns

    @except_columns.setter
    def except_columns(self, value):
        self._except_columns = value

    cpdef tuple children(self):
        return ()

    cpdef map_children(self, object fn):
        return None

    cdef Expression _shallow_copy(self):
        cdef Wildcard new = Wildcard.__new__(Wildcard)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._value = self._value
        new._except_columns = self._except_columns
        return new

    cpdef Expression copy(self, dict memo=None):
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
        return new


cdef class Evaluated(Expression):
    """EVALUATED expression."""

    cdef object _value

    def __init__(self, value=None, *, alias=None, query_column=None, schema_column=None, relations=None, do_not_create_column=False, uuid=None):
        self.node_type = _node_types().EVALUATED
        self.uuid = random_string_c(32, None) if uuid is None else uuid
        self._set_common(alias, query_column, schema_column, relations)
        self.do_not_create_column = do_not_create_column
        self.value = value

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    cpdef tuple children(self):
        return ()

    cpdef map_children(self, object fn):
        return None

    cdef Expression _shallow_copy(self):
        cdef Evaluated new = Evaluated.__new__(Evaluated)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._value = self._value
        return new

    cpdef Expression copy(self, dict memo=None):
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
cdef tuple _FIELD_DESCRIPTORS = (type(Expression.uuid), property)


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

    share=False: the copy is a deep `copy()` (same uuid; unchanged children are the
    copy's own). share=True: the copy is `replace()` (fresh uuid; unchanged children
    are the ORIGINAL objects, shared with the input)."""
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
    rebuilt = expr.replace() if share else expr.copy()
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
