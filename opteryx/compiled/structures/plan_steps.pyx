# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=True
# cython: boundscheck=False

"""
Typed logical plan steps.

One declared class per LogicalPlanStepType, replacing the attribute-bag Node for
plan nodes (architect ruling 2026-09-25: fixed, declared, enforced attributes).
Each class declares exactly the fields that kind of step has: an undeclared name
raises AttributeError, and a wrong-typed value raises TypeError at the write.

Every step has `columns`, `all_relations` and `pre_update_columns`. The
expressions a step holds are reached through `expressions()` /
`map_expressions(fn)`: every declared expression-bearing field, in declared
order. A walker descends from those roots with the expressions' own
`children()`.
"""

from opteryx.compiled.functions.random_helper import random_string_c
from opteryx.compiled.structures.expressions import EXPRESSION_TYPES
from opteryx.compiled.structures.expressions import is_expression


cdef object _StepType = None


cdef inline object _step_types():
    # Imported lazily: the logical planner imports this module.
    global _StepType
    if _StepType is None:
        from opteryx.planner.logical_planner import LogicalPlanStepType

        _StepType = LogicalPlanStepType
    return _StepType


# ---------------------------------------------------------------------------
# field validation
# ---------------------------------------------------------------------------


cdef inline void _require_optional_bool(str name, object value):
    if value is not None and type(value) is not bool:
        raise TypeError(f"{name} must be a bool or None, got {type(value).__name__}")


cdef inline void _require_optional_int(str name, object value):
    if value is not None and type(value) is not int:
        raise TypeError(f"{name} must be an int or None, got {type(value).__name__}")


cdef inline void _require_expression(str name, object value):
    if value is not None and not is_expression(value):
        raise TypeError(f"{name} must be an expression or None, got {type(value).__name__}")


cdef inline list _require_expression_list(str name, object value):
    if value is None:
        return None
    if type(value) is not list:
        raise TypeError(f"{name} must be a list of expressions, got {type(value).__name__}")
    for item in value:
        if not is_expression(item):
            raise TypeError(f"{name} must hold expressions, got {type(item).__name__}")
    return value


cdef inline list _require_order_list(str name, object value):
    """ORDER BY: a list of (expression, ascending) pairs."""
    if value is None:
        return None
    if type(value) is not list:
        raise TypeError(f"{name} must be a list of (expression, bool) pairs, got {type(value).__name__}")
    for item in value:
        if type(item) is not tuple or len(item) != 2 or not is_expression(item[0]) or type(item[1]) is not bool:
            raise TypeError(f"{name} must hold (expression, bool) pairs, got {item!r}")
    return value


cdef inline dict _require_expression_dict(str name, object value):
    if value is None:
        return None
    if type(value) is not dict:
        raise TypeError(f"{name} must be a dict of expressions, got {type(value).__name__}")
    for item in value.values():
        if not is_expression(item):
            raise TypeError(f"{name} must hold expressions, got {type(item).__name__}")
    return value


cdef inline list _require_rows(str name, object value):
    """VALUES rows: a list of tuples of expressions."""
    if value is None:
        return None
    if type(value) is not list:
        raise TypeError(f"{name} must be a list of rows, got {type(value).__name__}")
    for row in value:
        if type(row) is not tuple:
            raise TypeError(f"{name} rows must be tuples, got {type(row).__name__}")
        for item in row:
            if not is_expression(item):
                raise TypeError(f"{name} must hold expressions, got {type(item).__name__}")
    return value


cdef inline list _require_window_functions(str name, object value):
    """Window functions: (name, output identity, operand expression or None, extra)."""
    if value is None:
        return None
    if type(value) is not list:
        raise TypeError(f"{name} must be a list of window functions, got {type(value).__name__}")
    for item in value:
        if type(item) is not tuple or len(item) != 4:
            raise TypeError(f"{name} must hold 4-tuples, got {item!r}")
        if item[2] is not None and not is_expression(item[2]):
            raise TypeError(f"{name} operand must be an expression or None, got {type(item[2]).__name__}")
    return value


cdef inline void _require_plan_step(str name, object value):
    if value is not None and type(value) not in PLAN_STEP_TYPES:
        raise TypeError(f"{name} must be a plan step or None, got {type(value).__name__}")


# ---------------------------------------------------------------------------
# expression access
# ---------------------------------------------------------------------------


cdef inline void _append_expression(list out, object expression):
    if expression is not None:
        out.append(expression)


cdef inline void _extend_expressions(list out, list expressions):
    if expressions is not None:
        for expression in expressions:
            out.append(expression)


cdef inline void _extend_order_expressions(list out, list order):
    if order is not None:
        for expression, _ascending in order:
            out.append(expression)


cdef inline void _extend_dict_expressions(list out, dict expressions):
    if expressions is not None:
        for expression in expressions.values():
            out.append(expression)


cdef inline void _extend_row_expressions(list out, list rows):
    if rows is not None:
        for row in rows:
            for expression in row:
                out.append(expression)


cdef inline void _extend_window_expressions(list out, list window_functions):
    if window_functions is not None:
        for item in window_functions:
            if item[2] is not None:
                out.append(item[2])


cdef inline object _map_expression(object fn, object expression):
    return None if expression is None else fn(expression)


cdef inline list _map_expressions(object fn, list expressions):
    if expressions is None:
        return None
    return [fn(expression) for expression in expressions]


cdef inline list _map_order_expressions(object fn, list order):
    if order is None:
        return None
    return [(fn(expression), ascending) for expression, ascending in order]


cdef inline dict _map_dict_expressions(object fn, dict expressions):
    if expressions is None:
        return None
    return {key: fn(expression) for key, expression in expressions.items()}


cdef inline list _map_row_expressions(object fn, list rows):
    if rows is None:
        return None
    return [tuple([fn(expression) for expression in row]) for row in rows]


cdef inline list _map_window_expressions(object fn, list window_functions):
    if window_functions is None:
        return None
    return [
        (item[0], item[1], None if item[2] is None else fn(item[2]), item[3])
        for item in window_functions
    ]


# ---------------------------------------------------------------------------
# copying
# ---------------------------------------------------------------------------


cpdef object _copy_field(object value, dict memo):
    """Deep-copy one field value, with the semantics the attribute-bag Node's
    copy had: containers recursively; expressions and plan steps through `memo`
    (a value reachable from two slots copies to ONE new object); a value with its
    own `copy()` (a schema, a set, a manifest) by that; everything else shared."""
    cdef type value_type = type(value)
    if value is None or value_type is int or value_type is float or value_type is str or value_type is bool:
        return value
    if value_type is list:
        return [_copy_field(item, memo) for item in value]
    if value_type is tuple:
        return tuple([_copy_field(item, memo) for item in value])
    if value_type is dict:
        return {key: _copy_field(item, memo) for key, item in value.items()}
    if value_type in EXPRESSION_TYPES:
        return value.copy(memo)
    if value_type in PLAN_STEP_TYPES:
        return (<PlanStep>value).copy(memo)
    # hasattr is intentional here, as it was in the attribute-bag Node's copy:
    # a field may hold an arbitrary object (schema, manifest, connector) that
    # carries its own copy(). No known type can be checked statically, so this is
    # the approved exception to §9 carried over.
    if hasattr(value, "copy"):
        return value.copy()
    return value


# ---------------------------------------------------------------------------
# the base step
# ---------------------------------------------------------------------------


cdef class PlanStep:
    """What every logical plan step has: its type, identity, output columns and
    the relation bookkeeping the planner carries on every step."""

    cdef readonly object node_type
    cdef public str uuid
    cdef list _columns
    cdef set _all_relations
    cdef set _pre_update_columns

    cdef void _init_common(self, object columns, object all_relations, object pre_update_columns, object uuid):
        self.uuid = random_string_c(32, None) if uuid is None else uuid
        self.columns = columns
        self.all_relations = all_relations
        self.pre_update_columns = pre_update_columns

    @property
    def columns(self):
        return self._columns

    @columns.setter
    def columns(self, value):
        self._columns = _require_expression_list(f"{type(self).__name__}.columns", value)

    @property
    def all_relations(self):
        return self._all_relations

    @all_relations.setter
    def all_relations(self, set value):
        self._all_relations = value

    @property
    def pre_update_columns(self):
        return self._pre_update_columns

    @pre_update_columns.setter
    def pre_update_columns(self, set value):
        self._pre_update_columns = value

    cpdef tuple expressions(self, bint include_columns=True):
        """Every expression the step holds (roots only), declared fields in order;
        `include_columns=False` leaves out the output `columns`."""
        raise NotImplementedError(f"{type(self).__name__}.expressions")

    cpdef map_expressions(self, object fn):
        raise NotImplementedError(f"{type(self).__name__}.map_expressions")

    cpdef dict field_values(self):
        raise NotImplementedError(f"{type(self).__name__}.field_values")

    cdef dict _common_values(self):
        return {
            "columns": self._columns,
            "all_relations": self._all_relations,
            "pre_update_columns": self._pre_update_columns,
        }

    cpdef PlanStep copy(self, dict memo=None):
        raise NotImplementedError(f"{type(self).__name__}.copy")

    cdef PlanStep _shallow_copy(self):
        raise NotImplementedError(f"{type(self).__name__}._shallow_copy")

    cdef void _copy_common_into(self, PlanStep target, dict memo):
        target.uuid = self.uuid
        target._columns = _copy_field(self._columns, memo)
        target._all_relations = _copy_field(self._all_relations, memo)
        target._pre_update_columns = _copy_field(self._pre_update_columns, memo)

    cdef void _share_common_into(self, PlanStep target):
        target._columns = self._columns
        target._all_relations = self._all_relations
        target._pre_update_columns = self._pre_update_columns

    def shallow_copy(self):
        """A new step of the same type sharing this one's field values, SAME uuid —
        the optimizer's copy-on-write working plan re-adds steps this way."""
        cdef PlanStep new = self._shallow_copy()
        new.uuid = self.uuid
        return new

    def replace(self, **overrides):
        """A NEW step of the same type with this one's fields, `overrides` applied
        through the fields' own (enforcing) setters, and a fresh uuid."""
        cdef PlanStep new = self._shallow_copy()
        new.uuid = random_string_c(32, None)
        for name, value in overrides.items():
            setattr(new, name, value)
        return new

    def operator_parameters(self):
        """TRANSITIONAL (stage 3A → 3B): the keyword arguments the physical planner
        splats into an operator descriptor, exactly as the attribute-bag Node's
        `properties` gave them — node_type, uuid and every field that is set (an
        unset field was ABSENT from the bag, never None). Deleted when the
        descriptors are retired and the compiler reads the step itself."""
        cdef dict out = {"node_type": self.node_type, "uuid": self.uuid}
        for name, value in self.field_values().items():
            if value is not None:
                out[name] = value
        return out

    def __str__(self):  # pragma: no cover
        from opteryx.planner.logical_planner.logical_planner_renderers import _render_registry

        render_fn = _render_registry.get(self.node_type)
        if render_fn is not None:
            return render_fn(self)
        return self.node_type.name

    def __repr__(self):
        return f"<{type(self).__name__}>"


cpdef bint is_plan_step(object value):
    """Whether `value` is a logical plan step — the one test for it (exact types)."""
    return type(value) in PLAN_STEP_TYPES


cdef class AddColumnStep(PlanStep):
    """The AddColumn logical plan step."""

    cdef str _column_name
    cdef object _column_type
    cdef object _connector
    cdef object _default
    cdef object _if_exists
    cdef object _if_not_exists
    cdef object _nullable
    cdef str _relation_name

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, column_name=None, column_type=None, connector=None, default=None, if_exists=None, if_not_exists=None, nullable=None, relation_name=None):
        self.node_type = _step_types().AddColumn
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.column_name = column_name
        self.column_type = column_type
        self.connector = connector
        self.default = default
        self.if_exists = if_exists
        self.if_not_exists = if_not_exists
        self.nullable = nullable
        self.relation_name = relation_name

    @property
    def column_name(self):
        return self._column_name

    @column_name.setter
    def column_name(self, value):
        self._column_name = value

    @property
    def column_type(self):
        return self._column_type

    @column_type.setter
    def column_type(self, value):
        self._column_type = value

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def default(self):
        return self._default

    @default.setter
    def default(self, value):
        self._default = value

    @property
    def if_exists(self):
        return self._if_exists

    @if_exists.setter
    def if_exists(self, value):
        _require_optional_bool("AddColumnStep.if_exists", value)
        self._if_exists = value

    @property
    def if_not_exists(self):
        return self._if_not_exists

    @if_not_exists.setter
    def if_not_exists(self, value):
        _require_optional_bool("AddColumnStep.if_not_exists", value)
        self._if_not_exists = value

    @property
    def nullable(self):
        return self._nullable

    @nullable.setter
    def nullable(self, value):
        _require_optional_bool("AddColumnStep.nullable", value)
        self._nullable = value

    @property
    def relation_name(self):
        return self._relation_name

    @relation_name.setter
    def relation_name(self, value):
        self._relation_name = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["column_name"] = self._column_name
        out["column_type"] = self._column_type
        out["connector"] = self._connector
        out["default"] = self._default
        out["if_exists"] = self._if_exists
        out["if_not_exists"] = self._if_not_exists
        out["nullable"] = self._nullable
        out["relation_name"] = self._relation_name
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef AddColumnStep new = AddColumnStep.__new__(AddColumnStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._column_name = _copy_field(self._column_name, memo)
        new._column_type = _copy_field(self._column_type, memo)
        new._connector = _copy_field(self._connector, memo)
        new._default = _copy_field(self._default, memo)
        new._if_exists = _copy_field(self._if_exists, memo)
        new._if_not_exists = _copy_field(self._if_not_exists, memo)
        new._nullable = _copy_field(self._nullable, memo)
        new._relation_name = _copy_field(self._relation_name, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef AddColumnStep new = AddColumnStep.__new__(AddColumnStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._column_name = self._column_name
        new._column_type = self._column_type
        new._connector = self._connector
        new._default = self._default
        new._if_exists = self._if_exists
        new._if_not_exists = self._if_not_exists
        new._nullable = self._nullable
        new._relation_name = self._relation_name
        return new


cdef class AddRelationshipStep(PlanStep):
    """The AddRelationship logical plan step."""

    cdef str _cardinality
    cdef str _column_name
    cdef object _connector
    cdef str _constraint_name
    cdef object _if_exists
    cdef str _references_column_name
    cdef str _references_relation_name
    cdef list _references_relation_parts
    cdef str _relation_name
    cdef list _relation_parts

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, cardinality=None, column_name=None, connector=None, constraint_name=None, if_exists=None, references_column_name=None, references_relation_name=None, references_relation_parts=None, relation_name=None, relation_parts=None):
        self.node_type = _step_types().AddRelationship
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.cardinality = cardinality
        self.column_name = column_name
        self.connector = connector
        self.constraint_name = constraint_name
        self.if_exists = if_exists
        self.references_column_name = references_column_name
        self.references_relation_name = references_relation_name
        self.references_relation_parts = references_relation_parts
        self.relation_name = relation_name
        self.relation_parts = relation_parts

    @property
    def cardinality(self):
        return self._cardinality

    @cardinality.setter
    def cardinality(self, value):
        self._cardinality = value

    @property
    def column_name(self):
        return self._column_name

    @column_name.setter
    def column_name(self, value):
        self._column_name = value

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def constraint_name(self):
        return self._constraint_name

    @constraint_name.setter
    def constraint_name(self, value):
        self._constraint_name = value

    @property
    def if_exists(self):
        return self._if_exists

    @if_exists.setter
    def if_exists(self, value):
        _require_optional_bool("AddRelationshipStep.if_exists", value)
        self._if_exists = value

    @property
    def references_column_name(self):
        return self._references_column_name

    @references_column_name.setter
    def references_column_name(self, value):
        self._references_column_name = value

    @property
    def references_relation_name(self):
        return self._references_relation_name

    @references_relation_name.setter
    def references_relation_name(self, value):
        self._references_relation_name = value

    @property
    def references_relation_parts(self):
        return self._references_relation_parts

    @references_relation_parts.setter
    def references_relation_parts(self, value):
        self._references_relation_parts = value

    @property
    def relation_name(self):
        return self._relation_name

    @relation_name.setter
    def relation_name(self, value):
        self._relation_name = value

    @property
    def relation_parts(self):
        return self._relation_parts

    @relation_parts.setter
    def relation_parts(self, value):
        self._relation_parts = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["cardinality"] = self._cardinality
        out["column_name"] = self._column_name
        out["connector"] = self._connector
        out["constraint_name"] = self._constraint_name
        out["if_exists"] = self._if_exists
        out["references_column_name"] = self._references_column_name
        out["references_relation_name"] = self._references_relation_name
        out["references_relation_parts"] = self._references_relation_parts
        out["relation_name"] = self._relation_name
        out["relation_parts"] = self._relation_parts
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef AddRelationshipStep new = AddRelationshipStep.__new__(AddRelationshipStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._cardinality = _copy_field(self._cardinality, memo)
        new._column_name = _copy_field(self._column_name, memo)
        new._connector = _copy_field(self._connector, memo)
        new._constraint_name = _copy_field(self._constraint_name, memo)
        new._if_exists = _copy_field(self._if_exists, memo)
        new._references_column_name = _copy_field(self._references_column_name, memo)
        new._references_relation_name = _copy_field(self._references_relation_name, memo)
        new._references_relation_parts = _copy_field(self._references_relation_parts, memo)
        new._relation_name = _copy_field(self._relation_name, memo)
        new._relation_parts = _copy_field(self._relation_parts, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef AddRelationshipStep new = AddRelationshipStep.__new__(AddRelationshipStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._cardinality = self._cardinality
        new._column_name = self._column_name
        new._connector = self._connector
        new._constraint_name = self._constraint_name
        new._if_exists = self._if_exists
        new._references_column_name = self._references_column_name
        new._references_relation_name = self._references_relation_name
        new._references_relation_parts = self._references_relation_parts
        new._relation_name = self._relation_name
        new._relation_parts = self._relation_parts
        return new


cdef class AggregateStep(PlanStep):
    """The Aggregate logical plan step."""

    cdef list _aggregates
    cdef list _groups
    cdef list _projection
    cdef object _schema

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, aggregates=None, groups=None, projection=None, schema=None):
        self.node_type = _step_types().Aggregate
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.aggregates = aggregates
        self.groups = groups
        self.projection = projection
        self.schema = schema

    @property
    def aggregates(self):
        return self._aggregates

    @aggregates.setter
    def aggregates(self, value):
        self._aggregates = _require_expression_list("AggregateStep.aggregates", value)

    @property
    def groups(self):
        return self._groups

    @groups.setter
    def groups(self, value):
        self._groups = _require_expression_list("AggregateStep.groups", value)

    @property
    def projection(self):
        return self._projection

    @projection.setter
    def projection(self, value):
        self._projection = _require_expression_list("AggregateStep.projection", value)

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, value):
        self._schema = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        _extend_expressions(out, self._aggregates)
        _extend_expressions(out, self._groups)
        _extend_expressions(out, self._projection)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)
        self.aggregates = _map_expressions(fn, self._aggregates)
        self.groups = _map_expressions(fn, self._groups)
        self.projection = _map_expressions(fn, self._projection)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["aggregates"] = self._aggregates
        out["groups"] = self._groups
        out["projection"] = self._projection
        out["schema"] = self._schema
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef AggregateStep new = AggregateStep.__new__(AggregateStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._aggregates = _copy_field(self._aggregates, memo)
        new._groups = _copy_field(self._groups, memo)
        new._projection = _copy_field(self._projection, memo)
        new._schema = _copy_field(self._schema, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef AggregateStep new = AggregateStep.__new__(AggregateStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._aggregates = self._aggregates
        new._groups = self._groups
        new._projection = self._projection
        new._schema = self._schema
        return new


cdef class AggregateAndGroupStep(PlanStep):
    """The AggregateAndGroup logical plan step."""

    cdef list _aggregates
    cdef list _grouping_set_identities
    cdef list _grouping_sets
    cdef list _groups
    cdef object _having_condition
    cdef list _projection
    cdef object _schema

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, aggregates=None, grouping_set_identities=None, grouping_sets=None, groups=None, having_condition=None, projection=None, schema=None):
        self.node_type = _step_types().AggregateAndGroup
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.aggregates = aggregates
        self.grouping_set_identities = grouping_set_identities
        self.grouping_sets = grouping_sets
        self.groups = groups
        self.having_condition = having_condition
        self.projection = projection
        self.schema = schema

    @property
    def aggregates(self):
        return self._aggregates

    @aggregates.setter
    def aggregates(self, value):
        self._aggregates = _require_expression_list("AggregateAndGroupStep.aggregates", value)

    @property
    def grouping_set_identities(self):
        return self._grouping_set_identities

    @grouping_set_identities.setter
    def grouping_set_identities(self, value):
        self._grouping_set_identities = value

    @property
    def grouping_sets(self):
        return self._grouping_sets

    @grouping_sets.setter
    def grouping_sets(self, value):
        self._grouping_sets = value

    @property
    def groups(self):
        return self._groups

    @groups.setter
    def groups(self, value):
        self._groups = _require_expression_list("AggregateAndGroupStep.groups", value)

    @property
    def having_condition(self):
        return self._having_condition

    @having_condition.setter
    def having_condition(self, value):
        _require_expression("AggregateAndGroupStep.having_condition", value)
        self._having_condition = value

    @property
    def projection(self):
        return self._projection

    @projection.setter
    def projection(self, value):
        self._projection = _require_expression_list("AggregateAndGroupStep.projection", value)

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, value):
        self._schema = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        _extend_expressions(out, self._aggregates)
        _extend_expressions(out, self._groups)
        _append_expression(out, self._having_condition)
        _extend_expressions(out, self._projection)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)
        self.aggregates = _map_expressions(fn, self._aggregates)
        self.groups = _map_expressions(fn, self._groups)
        self.having_condition = _map_expression(fn, self._having_condition)
        self.projection = _map_expressions(fn, self._projection)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["aggregates"] = self._aggregates
        out["grouping_set_identities"] = self._grouping_set_identities
        out["grouping_sets"] = self._grouping_sets
        out["groups"] = self._groups
        out["having_condition"] = self._having_condition
        out["projection"] = self._projection
        out["schema"] = self._schema
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef AggregateAndGroupStep new = AggregateAndGroupStep.__new__(AggregateAndGroupStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._aggregates = _copy_field(self._aggregates, memo)
        new._grouping_set_identities = _copy_field(self._grouping_set_identities, memo)
        new._grouping_sets = _copy_field(self._grouping_sets, memo)
        new._groups = _copy_field(self._groups, memo)
        new._having_condition = _copy_field(self._having_condition, memo)
        new._projection = _copy_field(self._projection, memo)
        new._schema = _copy_field(self._schema, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef AggregateAndGroupStep new = AggregateAndGroupStep.__new__(AggregateAndGroupStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._aggregates = self._aggregates
        new._grouping_set_identities = self._grouping_set_identities
        new._grouping_sets = self._grouping_sets
        new._groups = self._groups
        new._having_condition = self._having_condition
        new._projection = self._projection
        new._schema = self._schema
        return new


cdef class AlterColumnTypeStep(PlanStep):
    """The AlterColumnType logical plan step."""

    cdef str _column_name
    cdef object _connector
    cdef object _current_column_type
    cdef object _if_exists
    cdef object _new_column_type
    cdef str _relation_name

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, column_name=None, connector=None, current_column_type=None, if_exists=None, new_column_type=None, relation_name=None):
        self.node_type = _step_types().AlterColumnType
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.column_name = column_name
        self.connector = connector
        self.current_column_type = current_column_type
        self.if_exists = if_exists
        self.new_column_type = new_column_type
        self.relation_name = relation_name

    @property
    def column_name(self):
        return self._column_name

    @column_name.setter
    def column_name(self, value):
        self._column_name = value

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def current_column_type(self):
        return self._current_column_type

    @current_column_type.setter
    def current_column_type(self, value):
        self._current_column_type = value

    @property
    def if_exists(self):
        return self._if_exists

    @if_exists.setter
    def if_exists(self, value):
        _require_optional_bool("AlterColumnTypeStep.if_exists", value)
        self._if_exists = value

    @property
    def new_column_type(self):
        return self._new_column_type

    @new_column_type.setter
    def new_column_type(self, value):
        self._new_column_type = value

    @property
    def relation_name(self):
        return self._relation_name

    @relation_name.setter
    def relation_name(self, value):
        self._relation_name = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["column_name"] = self._column_name
        out["connector"] = self._connector
        out["current_column_type"] = self._current_column_type
        out["if_exists"] = self._if_exists
        out["new_column_type"] = self._new_column_type
        out["relation_name"] = self._relation_name
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef AlterColumnTypeStep new = AlterColumnTypeStep.__new__(AlterColumnTypeStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._column_name = _copy_field(self._column_name, memo)
        new._connector = _copy_field(self._connector, memo)
        new._current_column_type = _copy_field(self._current_column_type, memo)
        new._if_exists = _copy_field(self._if_exists, memo)
        new._new_column_type = _copy_field(self._new_column_type, memo)
        new._relation_name = _copy_field(self._relation_name, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef AlterColumnTypeStep new = AlterColumnTypeStep.__new__(AlterColumnTypeStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._column_name = self._column_name
        new._connector = self._connector
        new._current_column_type = self._current_column_type
        new._if_exists = self._if_exists
        new._new_column_type = self._new_column_type
        new._relation_name = self._relation_name
        return new


cdef class AlterMaterializedViewOwnerStep(PlanStep):
    """The AlterMaterializedViewOwner logical plan step."""

    cdef object _connector
    cdef str _new_owner
    cdef object _owner_is_current_user
    cdef str _relation_name

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, connector=None, new_owner=None, owner_is_current_user=None, relation_name=None):
        self.node_type = _step_types().AlterMaterializedViewOwner
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.connector = connector
        self.new_owner = new_owner
        self.owner_is_current_user = owner_is_current_user
        self.relation_name = relation_name

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def new_owner(self):
        return self._new_owner

    @new_owner.setter
    def new_owner(self, value):
        self._new_owner = value

    @property
    def owner_is_current_user(self):
        return self._owner_is_current_user

    @owner_is_current_user.setter
    def owner_is_current_user(self, value):
        _require_optional_bool("AlterMaterializedViewOwnerStep.owner_is_current_user", value)
        self._owner_is_current_user = value

    @property
    def relation_name(self):
        return self._relation_name

    @relation_name.setter
    def relation_name(self, value):
        self._relation_name = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["connector"] = self._connector
        out["new_owner"] = self._new_owner
        out["owner_is_current_user"] = self._owner_is_current_user
        out["relation_name"] = self._relation_name
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef AlterMaterializedViewOwnerStep new = AlterMaterializedViewOwnerStep.__new__(AlterMaterializedViewOwnerStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._connector = _copy_field(self._connector, memo)
        new._new_owner = _copy_field(self._new_owner, memo)
        new._owner_is_current_user = _copy_field(self._owner_is_current_user, memo)
        new._relation_name = _copy_field(self._relation_name, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef AlterMaterializedViewOwnerStep new = AlterMaterializedViewOwnerStep.__new__(AlterMaterializedViewOwnerStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._connector = self._connector
        new._new_owner = self._new_owner
        new._owner_is_current_user = self._owner_is_current_user
        new._relation_name = self._relation_name
        return new


cdef class AlterMaterializedViewSuspendedStep(PlanStep):
    """The AlterMaterializedViewSuspended logical plan step."""

    cdef object _connector
    cdef str _relation_name
    cdef object _suspended

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, connector=None, relation_name=None, suspended=None):
        self.node_type = _step_types().AlterMaterializedViewSuspended
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.connector = connector
        self.relation_name = relation_name
        self.suspended = suspended

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def relation_name(self):
        return self._relation_name

    @relation_name.setter
    def relation_name(self, value):
        self._relation_name = value

    @property
    def suspended(self):
        return self._suspended

    @suspended.setter
    def suspended(self, value):
        _require_optional_bool("AlterMaterializedViewSuspendedStep.suspended", value)
        self._suspended = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["connector"] = self._connector
        out["relation_name"] = self._relation_name
        out["suspended"] = self._suspended
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef AlterMaterializedViewSuspendedStep new = AlterMaterializedViewSuspendedStep.__new__(AlterMaterializedViewSuspendedStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._connector = _copy_field(self._connector, memo)
        new._relation_name = _copy_field(self._relation_name, memo)
        new._suspended = _copy_field(self._suspended, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef AlterMaterializedViewSuspendedStep new = AlterMaterializedViewSuspendedStep.__new__(AlterMaterializedViewSuspendedStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._connector = self._connector
        new._relation_name = self._relation_name
        new._suspended = self._suspended
        return new


cdef class AlterRelationStep(PlanStep):
    """The AlterRelation logical plan step."""

    cdef list _cluster_columns
    cdef object _connector
    cdef object _if_exists
    cdef str _relation_name

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, cluster_columns=None, connector=None, if_exists=None, relation_name=None):
        self.node_type = _step_types().AlterRelation
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.cluster_columns = cluster_columns
        self.connector = connector
        self.if_exists = if_exists
        self.relation_name = relation_name

    @property
    def cluster_columns(self):
        return self._cluster_columns

    @cluster_columns.setter
    def cluster_columns(self, value):
        self._cluster_columns = value

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def if_exists(self):
        return self._if_exists

    @if_exists.setter
    def if_exists(self, value):
        _require_optional_bool("AlterRelationStep.if_exists", value)
        self._if_exists = value

    @property
    def relation_name(self):
        return self._relation_name

    @relation_name.setter
    def relation_name(self, value):
        self._relation_name = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["cluster_columns"] = self._cluster_columns
        out["connector"] = self._connector
        out["if_exists"] = self._if_exists
        out["relation_name"] = self._relation_name
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef AlterRelationStep new = AlterRelationStep.__new__(AlterRelationStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._cluster_columns = _copy_field(self._cluster_columns, memo)
        new._connector = _copy_field(self._connector, memo)
        new._if_exists = _copy_field(self._if_exists, memo)
        new._relation_name = _copy_field(self._relation_name, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef AlterRelationStep new = AlterRelationStep.__new__(AlterRelationStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._cluster_columns = self._cluster_columns
        new._connector = self._connector
        new._if_exists = self._if_exists
        new._relation_name = self._relation_name
        return new


cdef class AlterTaskStep(PlanStep):
    """The AlterTask logical plan step."""

    cdef object _connector
    cdef list _source_tables
    cdef str _statement
    cdef list _target_tables
    cdef str _task_name

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, connector=None, source_tables=None, statement=None, target_tables=None, task_name=None):
        self.node_type = _step_types().AlterTask
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.connector = connector
        self.source_tables = source_tables
        self.statement = statement
        self.target_tables = target_tables
        self.task_name = task_name

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def source_tables(self):
        return self._source_tables

    @source_tables.setter
    def source_tables(self, value):
        self._source_tables = value

    @property
    def statement(self):
        return self._statement

    @statement.setter
    def statement(self, value):
        self._statement = value

    @property
    def target_tables(self):
        return self._target_tables

    @target_tables.setter
    def target_tables(self, value):
        self._target_tables = value

    @property
    def task_name(self):
        return self._task_name

    @task_name.setter
    def task_name(self, value):
        self._task_name = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["connector"] = self._connector
        out["source_tables"] = self._source_tables
        out["statement"] = self._statement
        out["target_tables"] = self._target_tables
        out["task_name"] = self._task_name
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef AlterTaskStep new = AlterTaskStep.__new__(AlterTaskStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._connector = _copy_field(self._connector, memo)
        new._source_tables = _copy_field(self._source_tables, memo)
        new._statement = _copy_field(self._statement, memo)
        new._target_tables = _copy_field(self._target_tables, memo)
        new._task_name = _copy_field(self._task_name, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef AlterTaskStep new = AlterTaskStep.__new__(AlterTaskStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._connector = self._connector
        new._source_tables = self._source_tables
        new._statement = self._statement
        new._target_tables = self._target_tables
        new._task_name = self._task_name
        return new


cdef class AlterTriggerMinimumIntervalStep(PlanStep):
    """The AlterTriggerMinimumInterval logical plan step."""

    cdef object _connector
    cdef object _minimum_interval_seconds
    cdef str _table_name
    cdef str _trigger_name

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, connector=None, minimum_interval_seconds=None, table_name=None, trigger_name=None):
        self.node_type = _step_types().AlterTriggerMinimumInterval
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.connector = connector
        self.minimum_interval_seconds = minimum_interval_seconds
        self.table_name = table_name
        self.trigger_name = trigger_name

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def minimum_interval_seconds(self):
        return self._minimum_interval_seconds

    @minimum_interval_seconds.setter
    def minimum_interval_seconds(self, value):
        _require_optional_int("AlterTriggerMinimumIntervalStep.minimum_interval_seconds", value)
        self._minimum_interval_seconds = value

    @property
    def table_name(self):
        return self._table_name

    @table_name.setter
    def table_name(self, value):
        self._table_name = value

    @property
    def trigger_name(self):
        return self._trigger_name

    @trigger_name.setter
    def trigger_name(self, value):
        self._trigger_name = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["connector"] = self._connector
        out["minimum_interval_seconds"] = self._minimum_interval_seconds
        out["table_name"] = self._table_name
        out["trigger_name"] = self._trigger_name
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef AlterTriggerMinimumIntervalStep new = AlterTriggerMinimumIntervalStep.__new__(AlterTriggerMinimumIntervalStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._connector = _copy_field(self._connector, memo)
        new._minimum_interval_seconds = _copy_field(self._minimum_interval_seconds, memo)
        new._table_name = _copy_field(self._table_name, memo)
        new._trigger_name = _copy_field(self._trigger_name, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef AlterTriggerMinimumIntervalStep new = AlterTriggerMinimumIntervalStep.__new__(AlterTriggerMinimumIntervalStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._connector = self._connector
        new._minimum_interval_seconds = self._minimum_interval_seconds
        new._table_name = self._table_name
        new._trigger_name = self._trigger_name
        return new


cdef class AlterTriggerOwnerStep(PlanStep):
    """The AlterTriggerOwner logical plan step."""

    cdef object _connector
    cdef str _new_owner
    cdef object _owner_is_current_user
    cdef str _resolved_owner
    cdef str _table_name
    cdef str _trigger_name

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, connector=None, new_owner=None, owner_is_current_user=None, resolved_owner=None, table_name=None, trigger_name=None):
        self.node_type = _step_types().AlterTriggerOwner
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.connector = connector
        self.new_owner = new_owner
        self.owner_is_current_user = owner_is_current_user
        self.resolved_owner = resolved_owner
        self.table_name = table_name
        self.trigger_name = trigger_name

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def new_owner(self):
        return self._new_owner

    @new_owner.setter
    def new_owner(self, value):
        self._new_owner = value

    @property
    def owner_is_current_user(self):
        return self._owner_is_current_user

    @owner_is_current_user.setter
    def owner_is_current_user(self, value):
        _require_optional_bool("AlterTriggerOwnerStep.owner_is_current_user", value)
        self._owner_is_current_user = value

    @property
    def resolved_owner(self):
        return self._resolved_owner

    @resolved_owner.setter
    def resolved_owner(self, value):
        self._resolved_owner = value

    @property
    def table_name(self):
        return self._table_name

    @table_name.setter
    def table_name(self, value):
        self._table_name = value

    @property
    def trigger_name(self):
        return self._trigger_name

    @trigger_name.setter
    def trigger_name(self, value):
        self._trigger_name = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["connector"] = self._connector
        out["new_owner"] = self._new_owner
        out["owner_is_current_user"] = self._owner_is_current_user
        out["resolved_owner"] = self._resolved_owner
        out["table_name"] = self._table_name
        out["trigger_name"] = self._trigger_name
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef AlterTriggerOwnerStep new = AlterTriggerOwnerStep.__new__(AlterTriggerOwnerStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._connector = _copy_field(self._connector, memo)
        new._new_owner = _copy_field(self._new_owner, memo)
        new._owner_is_current_user = _copy_field(self._owner_is_current_user, memo)
        new._resolved_owner = _copy_field(self._resolved_owner, memo)
        new._table_name = _copy_field(self._table_name, memo)
        new._trigger_name = _copy_field(self._trigger_name, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef AlterTriggerOwnerStep new = AlterTriggerOwnerStep.__new__(AlterTriggerOwnerStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._connector = self._connector
        new._new_owner = self._new_owner
        new._owner_is_current_user = self._owner_is_current_user
        new._resolved_owner = self._resolved_owner
        new._table_name = self._table_name
        new._trigger_name = self._trigger_name
        return new


cdef class AlterTriggerSuspendedStep(PlanStep):
    """The AlterTriggerSuspended logical plan step."""

    cdef object _connector
    cdef object _suspended
    cdef str _table_name
    cdef str _trigger_name

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, connector=None, suspended=None, table_name=None, trigger_name=None):
        self.node_type = _step_types().AlterTriggerSuspended
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.connector = connector
        self.suspended = suspended
        self.table_name = table_name
        self.trigger_name = trigger_name

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def suspended(self):
        return self._suspended

    @suspended.setter
    def suspended(self, value):
        _require_optional_bool("AlterTriggerSuspendedStep.suspended", value)
        self._suspended = value

    @property
    def table_name(self):
        return self._table_name

    @table_name.setter
    def table_name(self, value):
        self._table_name = value

    @property
    def trigger_name(self):
        return self._trigger_name

    @trigger_name.setter
    def trigger_name(self, value):
        self._trigger_name = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["connector"] = self._connector
        out["suspended"] = self._suspended
        out["table_name"] = self._table_name
        out["trigger_name"] = self._trigger_name
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef AlterTriggerSuspendedStep new = AlterTriggerSuspendedStep.__new__(AlterTriggerSuspendedStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._connector = _copy_field(self._connector, memo)
        new._suspended = _copy_field(self._suspended, memo)
        new._table_name = _copy_field(self._table_name, memo)
        new._trigger_name = _copy_field(self._trigger_name, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef AlterTriggerSuspendedStep new = AlterTriggerSuspendedStep.__new__(AlterTriggerSuspendedStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._connector = self._connector
        new._suspended = self._suspended
        new._table_name = self._table_name
        new._trigger_name = self._trigger_name
        return new


cdef class AlterViewStep(PlanStep):
    """The AlterView logical plan step."""

    cdef object _connector
    cdef dict _query
    cdef object _view_name
    cdef object _view_schema
    cdef str _view_sql

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, connector=None, query=None, view_name=None, view_schema=None, view_sql=None):
        self.node_type = _step_types().AlterView
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.connector = connector
        self.query = query
        self.view_name = view_name
        self.view_schema = view_schema
        self.view_sql = view_sql

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def query(self):
        return self._query

    @query.setter
    def query(self, value):
        self._query = value

    @property
    def view_name(self):
        return self._view_name

    @view_name.setter
    def view_name(self, value):
        self._view_name = value

    @property
    def view_schema(self):
        return self._view_schema

    @view_schema.setter
    def view_schema(self, value):
        self._view_schema = value

    @property
    def view_sql(self):
        return self._view_sql

    @view_sql.setter
    def view_sql(self, value):
        self._view_sql = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["connector"] = self._connector
        out["query"] = self._query
        out["view_name"] = self._view_name
        out["view_schema"] = self._view_schema
        out["view_sql"] = self._view_sql
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef AlterViewStep new = AlterViewStep.__new__(AlterViewStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._connector = _copy_field(self._connector, memo)
        new._query = _copy_field(self._query, memo)
        new._view_name = _copy_field(self._view_name, memo)
        new._view_schema = _copy_field(self._view_schema, memo)
        new._view_sql = _copy_field(self._view_sql, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef AlterViewStep new = AlterViewStep.__new__(AlterViewStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._connector = self._connector
        new._query = self._query
        new._view_name = self._view_name
        new._view_schema = self._view_schema
        new._view_sql = self._view_sql
        return new


cdef class AlterWorkspaceStep(PlanStep):
    """The AlterWorkspace logical plan step."""

    cdef object _connector
    cdef object _execution_context
    cdef str _property_name
    cdef object _property_value
    cdef str _workspace_name

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, connector=None, execution_context=None, property_name=None, property_value=None, workspace_name=None):
        self.node_type = _step_types().AlterWorkspace
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.connector = connector
        self.execution_context = execution_context
        self.property_name = property_name
        self.property_value = property_value
        self.workspace_name = workspace_name

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def execution_context(self):
        return self._execution_context

    @execution_context.setter
    def execution_context(self, value):
        self._execution_context = value

    @property
    def property_name(self):
        return self._property_name

    @property_name.setter
    def property_name(self, value):
        self._property_name = value

    @property
    def property_value(self):
        return self._property_value

    @property_value.setter
    def property_value(self, value):
        _require_optional_bool("AlterWorkspaceStep.property_value", value)
        self._property_value = value

    @property
    def workspace_name(self):
        return self._workspace_name

    @workspace_name.setter
    def workspace_name(self, value):
        self._workspace_name = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["connector"] = self._connector
        out["execution_context"] = self._execution_context
        out["property_name"] = self._property_name
        out["property_value"] = self._property_value
        out["workspace_name"] = self._workspace_name
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef AlterWorkspaceStep new = AlterWorkspaceStep.__new__(AlterWorkspaceStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._connector = _copy_field(self._connector, memo)
        new._execution_context = _copy_field(self._execution_context, memo)
        new._property_name = _copy_field(self._property_name, memo)
        new._property_value = _copy_field(self._property_value, memo)
        new._workspace_name = _copy_field(self._workspace_name, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef AlterWorkspaceStep new = AlterWorkspaceStep.__new__(AlterWorkspaceStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._connector = self._connector
        new._execution_context = self._execution_context
        new._property_name = self._property_name
        new._property_value = self._property_value
        new._workspace_name = self._workspace_name
        return new


cdef class AlterWorkspaceSecureStep(PlanStep):
    """The AlterWorkspaceSecure logical plan step."""

    cdef object _connector
    cdef list _secure_destinations
    cdef str _secure_object
    cdef str _workspace_name

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, connector=None, secure_destinations=None, secure_object=None, workspace_name=None):
        self.node_type = _step_types().AlterWorkspaceSecure
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.connector = connector
        self.secure_destinations = secure_destinations
        self.secure_object = secure_object
        self.workspace_name = workspace_name

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def secure_destinations(self):
        return self._secure_destinations

    @secure_destinations.setter
    def secure_destinations(self, value):
        self._secure_destinations = value

    @property
    def secure_object(self):
        return self._secure_object

    @secure_object.setter
    def secure_object(self, value):
        self._secure_object = value

    @property
    def workspace_name(self):
        return self._workspace_name

    @workspace_name.setter
    def workspace_name(self, value):
        self._workspace_name = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["connector"] = self._connector
        out["secure_destinations"] = self._secure_destinations
        out["secure_object"] = self._secure_object
        out["workspace_name"] = self._workspace_name
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef AlterWorkspaceSecureStep new = AlterWorkspaceSecureStep.__new__(AlterWorkspaceSecureStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._connector = _copy_field(self._connector, memo)
        new._secure_destinations = _copy_field(self._secure_destinations, memo)
        new._secure_object = _copy_field(self._secure_object, memo)
        new._workspace_name = _copy_field(self._workspace_name, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef AlterWorkspaceSecureStep new = AlterWorkspaceSecureStep.__new__(AlterWorkspaceSecureStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._connector = self._connector
        new._secure_destinations = self._secure_destinations
        new._secure_object = self._secure_object
        new._workspace_name = self._workspace_name
        return new


cdef class AnalyzeStep(PlanStep):
    """The Analyze logical plan step."""

    cdef str _action
    cdef list _analyze_columns
    cdef object _connector
    cdef str _table_name

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, action=None, analyze_columns=None, connector=None, table_name=None):
        self.node_type = _step_types().Analyze
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.action = action
        self.analyze_columns = analyze_columns
        self.connector = connector
        self.table_name = table_name

    @property
    def action(self):
        return self._action

    @action.setter
    def action(self, value):
        self._action = value

    @property
    def analyze_columns(self):
        return self._analyze_columns

    @analyze_columns.setter
    def analyze_columns(self, value):
        self._analyze_columns = value

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def table_name(self):
        return self._table_name

    @table_name.setter
    def table_name(self, value):
        self._table_name = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["action"] = self._action
        out["analyze_columns"] = self._analyze_columns
        out["connector"] = self._connector
        out["table_name"] = self._table_name
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef AnalyzeStep new = AnalyzeStep.__new__(AnalyzeStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._action = _copy_field(self._action, memo)
        new._analyze_columns = _copy_field(self._analyze_columns, memo)
        new._connector = _copy_field(self._connector, memo)
        new._table_name = _copy_field(self._table_name, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef AnalyzeStep new = AnalyzeStep.__new__(AnalyzeStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._action = self._action
        new._analyze_columns = self._analyze_columns
        new._connector = self._connector
        new._table_name = self._table_name
        return new


cdef class CallProcedureStep(PlanStep):
    """The CallProcedure logical plan step."""

    cdef list _arguments
    cdef str _procedure_name

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, arguments=None, procedure_name=None):
        self.node_type = _step_types().CallProcedure
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.arguments = arguments
        self.procedure_name = procedure_name

    @property
    def arguments(self):
        return self._arguments

    @arguments.setter
    def arguments(self, value):
        self._arguments = value

    @property
    def procedure_name(self):
        return self._procedure_name

    @procedure_name.setter
    def procedure_name(self, value):
        self._procedure_name = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["arguments"] = self._arguments
        out["procedure_name"] = self._procedure_name
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef CallProcedureStep new = CallProcedureStep.__new__(CallProcedureStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._arguments = _copy_field(self._arguments, memo)
        new._procedure_name = _copy_field(self._procedure_name, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef CallProcedureStep new = CallProcedureStep.__new__(CallProcedureStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._arguments = self._arguments
        new._procedure_name = self._procedure_name
        return new


cdef class CloneCollectionStep(PlanStep):
    """The CloneCollection logical plan step."""

    cdef str _collection_name
    cdef object _connector
    cdef str _source_collection

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, collection_name=None, connector=None, source_collection=None):
        self.node_type = _step_types().CloneCollection
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.collection_name = collection_name
        self.connector = connector
        self.source_collection = source_collection

    @property
    def collection_name(self):
        return self._collection_name

    @collection_name.setter
    def collection_name(self, value):
        self._collection_name = value

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def source_collection(self):
        return self._source_collection

    @source_collection.setter
    def source_collection(self, value):
        self._source_collection = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["collection_name"] = self._collection_name
        out["connector"] = self._connector
        out["source_collection"] = self._source_collection
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef CloneCollectionStep new = CloneCollectionStep.__new__(CloneCollectionStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._collection_name = _copy_field(self._collection_name, memo)
        new._connector = _copy_field(self._connector, memo)
        new._source_collection = _copy_field(self._source_collection, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef CloneCollectionStep new = CloneCollectionStep.__new__(CloneCollectionStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._collection_name = self._collection_name
        new._connector = self._connector
        new._source_collection = self._source_collection
        return new


cdef class CloneRelationStep(PlanStep):
    """The CloneRelation logical plan step."""

    cdef object _if_not_exists
    cdef str _relation_name
    cdef str _source_relation

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, if_not_exists=None, relation_name=None, source_relation=None):
        self.node_type = _step_types().CloneRelation
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.if_not_exists = if_not_exists
        self.relation_name = relation_name
        self.source_relation = source_relation

    @property
    def if_not_exists(self):
        return self._if_not_exists

    @if_not_exists.setter
    def if_not_exists(self, value):
        _require_optional_bool("CloneRelationStep.if_not_exists", value)
        self._if_not_exists = value

    @property
    def relation_name(self):
        return self._relation_name

    @relation_name.setter
    def relation_name(self, value):
        self._relation_name = value

    @property
    def source_relation(self):
        return self._source_relation

    @source_relation.setter
    def source_relation(self, value):
        self._source_relation = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["if_not_exists"] = self._if_not_exists
        out["relation_name"] = self._relation_name
        out["source_relation"] = self._source_relation
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef CloneRelationStep new = CloneRelationStep.__new__(CloneRelationStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._if_not_exists = _copy_field(self._if_not_exists, memo)
        new._relation_name = _copy_field(self._relation_name, memo)
        new._source_relation = _copy_field(self._source_relation, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef CloneRelationStep new = CloneRelationStep.__new__(CloneRelationStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._if_not_exists = self._if_not_exists
        new._relation_name = self._relation_name
        new._source_relation = self._source_relation
        return new


cdef class CommentStep(PlanStep):
    """The Comment logical plan step."""

    cdef str _comment
    cdef object _connector
    cdef object _if_exists
    cdef str _object_name
    cdef str _object_type

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, comment=None, connector=None, if_exists=None, object_name=None, object_type=None):
        self.node_type = _step_types().Comment
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.comment = comment
        self.connector = connector
        self.if_exists = if_exists
        self.object_name = object_name
        self.object_type = object_type

    @property
    def comment(self):
        return self._comment

    @comment.setter
    def comment(self, value):
        self._comment = value

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def if_exists(self):
        return self._if_exists

    @if_exists.setter
    def if_exists(self, value):
        _require_optional_bool("CommentStep.if_exists", value)
        self._if_exists = value

    @property
    def object_name(self):
        return self._object_name

    @object_name.setter
    def object_name(self, value):
        self._object_name = value

    @property
    def object_type(self):
        return self._object_type

    @object_type.setter
    def object_type(self, value):
        self._object_type = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["comment"] = self._comment
        out["connector"] = self._connector
        out["if_exists"] = self._if_exists
        out["object_name"] = self._object_name
        out["object_type"] = self._object_type
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef CommentStep new = CommentStep.__new__(CommentStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._comment = _copy_field(self._comment, memo)
        new._connector = _copy_field(self._connector, memo)
        new._if_exists = _copy_field(self._if_exists, memo)
        new._object_name = _copy_field(self._object_name, memo)
        new._object_type = _copy_field(self._object_type, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef CommentStep new = CommentStep.__new__(CommentStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._comment = self._comment
        new._connector = self._connector
        new._if_exists = self._if_exists
        new._object_name = self._object_name
        new._object_type = self._object_type
        return new


cdef class CompactionCommitStep(PlanStep):
    """The CompactionCommit logical plan step."""

    cdef object _baseline_snapshot_id
    cdef object _connector
    cdef str _relation_name
    cdef list _retired_files
    cdef object _sorted_by
    cdef str _source_tail_id

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, baseline_snapshot_id=None, connector=None, relation_name=None, retired_files=None, sorted_by=None, source_tail_id=None):
        self.node_type = _step_types().CompactionCommit
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.baseline_snapshot_id = baseline_snapshot_id
        self.connector = connector
        self.relation_name = relation_name
        self.retired_files = retired_files
        self.sorted_by = sorted_by
        self.source_tail_id = source_tail_id

    @property
    def baseline_snapshot_id(self):
        return self._baseline_snapshot_id

    @baseline_snapshot_id.setter
    def baseline_snapshot_id(self, value):
        _require_optional_int("CompactionCommitStep.baseline_snapshot_id", value)
        self._baseline_snapshot_id = value

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def relation_name(self):
        return self._relation_name

    @relation_name.setter
    def relation_name(self, value):
        self._relation_name = value

    @property
    def retired_files(self):
        return self._retired_files

    @retired_files.setter
    def retired_files(self, value):
        self._retired_files = value

    @property
    def sorted_by(self):
        return self._sorted_by

    @sorted_by.setter
    def sorted_by(self, value):
        self._sorted_by = value

    @property
    def source_tail_id(self):
        return self._source_tail_id

    @source_tail_id.setter
    def source_tail_id(self, value):
        self._source_tail_id = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["baseline_snapshot_id"] = self._baseline_snapshot_id
        out["connector"] = self._connector
        out["relation_name"] = self._relation_name
        out["retired_files"] = self._retired_files
        out["sorted_by"] = self._sorted_by
        out["source_tail_id"] = self._source_tail_id
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef CompactionCommitStep new = CompactionCommitStep.__new__(CompactionCommitStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._baseline_snapshot_id = _copy_field(self._baseline_snapshot_id, memo)
        new._connector = _copy_field(self._connector, memo)
        new._relation_name = _copy_field(self._relation_name, memo)
        new._retired_files = _copy_field(self._retired_files, memo)
        new._sorted_by = _copy_field(self._sorted_by, memo)
        new._source_tail_id = _copy_field(self._source_tail_id, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef CompactionCommitStep new = CompactionCommitStep.__new__(CompactionCommitStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._baseline_snapshot_id = self._baseline_snapshot_id
        new._connector = self._connector
        new._relation_name = self._relation_name
        new._retired_files = self._retired_files
        new._sorted_by = self._sorted_by
        new._source_tail_id = self._source_tail_id
        return new


cdef class CreateCollectionStep(PlanStep):
    """The CreateCollection logical plan step."""

    cdef str _collection_name
    cdef object _connector
    cdef object _if_not_exists

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, collection_name=None, connector=None, if_not_exists=None):
        self.node_type = _step_types().CreateCollection
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.collection_name = collection_name
        self.connector = connector
        self.if_not_exists = if_not_exists

    @property
    def collection_name(self):
        return self._collection_name

    @collection_name.setter
    def collection_name(self, value):
        self._collection_name = value

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def if_not_exists(self):
        return self._if_not_exists

    @if_not_exists.setter
    def if_not_exists(self, value):
        _require_optional_bool("CreateCollectionStep.if_not_exists", value)
        self._if_not_exists = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["collection_name"] = self._collection_name
        out["connector"] = self._connector
        out["if_not_exists"] = self._if_not_exists
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef CreateCollectionStep new = CreateCollectionStep.__new__(CreateCollectionStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._collection_name = _copy_field(self._collection_name, memo)
        new._connector = _copy_field(self._connector, memo)
        new._if_not_exists = _copy_field(self._if_not_exists, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef CreateCollectionStep new = CreateCollectionStep.__new__(CreateCollectionStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._collection_name = self._collection_name
        new._connector = self._connector
        new._if_not_exists = self._if_not_exists
        return new


cdef class CreateRelationStep(PlanStep):
    """The CreateRelation logical plan step."""

    cdef object _connector
    cdef object _if_not_exists
    cdef object _relation_name
    cdef list _relationships
    cdef object _schema

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, connector=None, if_not_exists=None, relation_name=None, relationships=None, schema=None):
        self.node_type = _step_types().CreateRelation
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.connector = connector
        self.if_not_exists = if_not_exists
        self.relation_name = relation_name
        self.relationships = relationships
        self.schema = schema

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def if_not_exists(self):
        return self._if_not_exists

    @if_not_exists.setter
    def if_not_exists(self, value):
        _require_optional_bool("CreateRelationStep.if_not_exists", value)
        self._if_not_exists = value

    @property
    def relation_name(self):
        return self._relation_name

    @relation_name.setter
    def relation_name(self, value):
        self._relation_name = value

    @property
    def relationships(self):
        return self._relationships

    @relationships.setter
    def relationships(self, value):
        self._relationships = value

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, value):
        self._schema = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["connector"] = self._connector
        out["if_not_exists"] = self._if_not_exists
        out["relation_name"] = self._relation_name
        out["relationships"] = self._relationships
        out["schema"] = self._schema
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef CreateRelationStep new = CreateRelationStep.__new__(CreateRelationStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._connector = _copy_field(self._connector, memo)
        new._if_not_exists = _copy_field(self._if_not_exists, memo)
        new._relation_name = _copy_field(self._relation_name, memo)
        new._relationships = _copy_field(self._relationships, memo)
        new._schema = _copy_field(self._schema, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef CreateRelationStep new = CreateRelationStep.__new__(CreateRelationStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._connector = self._connector
        new._if_not_exists = self._if_not_exists
        new._relation_name = self._relation_name
        new._relationships = self._relationships
        new._schema = self._schema
        return new


cdef class CreateTagStep(PlanStep):
    """The CreateTag logical plan step."""

    cdef object _if_exists
    cdef str _relation_name
    cdef str _tag_name
    cdef str _version_spec

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, if_exists=None, relation_name=None, tag_name=None, version_spec=None):
        self.node_type = _step_types().CreateTag
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.if_exists = if_exists
        self.relation_name = relation_name
        self.tag_name = tag_name
        self.version_spec = version_spec

    @property
    def if_exists(self):
        return self._if_exists

    @if_exists.setter
    def if_exists(self, value):
        _require_optional_bool("CreateTagStep.if_exists", value)
        self._if_exists = value

    @property
    def relation_name(self):
        return self._relation_name

    @relation_name.setter
    def relation_name(self, value):
        self._relation_name = value

    @property
    def tag_name(self):
        return self._tag_name

    @tag_name.setter
    def tag_name(self, value):
        self._tag_name = value

    @property
    def version_spec(self):
        return self._version_spec

    @version_spec.setter
    def version_spec(self, value):
        self._version_spec = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["if_exists"] = self._if_exists
        out["relation_name"] = self._relation_name
        out["tag_name"] = self._tag_name
        out["version_spec"] = self._version_spec
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef CreateTagStep new = CreateTagStep.__new__(CreateTagStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._if_exists = _copy_field(self._if_exists, memo)
        new._relation_name = _copy_field(self._relation_name, memo)
        new._tag_name = _copy_field(self._tag_name, memo)
        new._version_spec = _copy_field(self._version_spec, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef CreateTagStep new = CreateTagStep.__new__(CreateTagStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._if_exists = self._if_exists
        new._relation_name = self._relation_name
        new._tag_name = self._tag_name
        new._version_spec = self._version_spec
        return new


cdef class CreateTaskStep(PlanStep):
    """The CreateTask logical plan step."""

    cdef object _connector
    cdef object _if_not_exists
    cdef str _on_table
    cdef object _or_replace
    cdef list _source_tables
    cdef str _statement
    cdef list _target_tables
    cdef str _task_name

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, connector=None, if_not_exists=None, on_table=None, or_replace=None, source_tables=None, statement=None, target_tables=None, task_name=None):
        self.node_type = _step_types().CreateTask
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.connector = connector
        self.if_not_exists = if_not_exists
        self.on_table = on_table
        self.or_replace = or_replace
        self.source_tables = source_tables
        self.statement = statement
        self.target_tables = target_tables
        self.task_name = task_name

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def if_not_exists(self):
        return self._if_not_exists

    @if_not_exists.setter
    def if_not_exists(self, value):
        _require_optional_bool("CreateTaskStep.if_not_exists", value)
        self._if_not_exists = value

    @property
    def on_table(self):
        return self._on_table

    @on_table.setter
    def on_table(self, value):
        self._on_table = value

    @property
    def or_replace(self):
        return self._or_replace

    @or_replace.setter
    def or_replace(self, value):
        _require_optional_bool("CreateTaskStep.or_replace", value)
        self._or_replace = value

    @property
    def source_tables(self):
        return self._source_tables

    @source_tables.setter
    def source_tables(self, value):
        self._source_tables = value

    @property
    def statement(self):
        return self._statement

    @statement.setter
    def statement(self, value):
        self._statement = value

    @property
    def target_tables(self):
        return self._target_tables

    @target_tables.setter
    def target_tables(self, value):
        self._target_tables = value

    @property
    def task_name(self):
        return self._task_name

    @task_name.setter
    def task_name(self, value):
        self._task_name = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["connector"] = self._connector
        out["if_not_exists"] = self._if_not_exists
        out["on_table"] = self._on_table
        out["or_replace"] = self._or_replace
        out["source_tables"] = self._source_tables
        out["statement"] = self._statement
        out["target_tables"] = self._target_tables
        out["task_name"] = self._task_name
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef CreateTaskStep new = CreateTaskStep.__new__(CreateTaskStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._connector = _copy_field(self._connector, memo)
        new._if_not_exists = _copy_field(self._if_not_exists, memo)
        new._on_table = _copy_field(self._on_table, memo)
        new._or_replace = _copy_field(self._or_replace, memo)
        new._source_tables = _copy_field(self._source_tables, memo)
        new._statement = _copy_field(self._statement, memo)
        new._target_tables = _copy_field(self._target_tables, memo)
        new._task_name = _copy_field(self._task_name, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef CreateTaskStep new = CreateTaskStep.__new__(CreateTaskStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._connector = self._connector
        new._if_not_exists = self._if_not_exists
        new._on_table = self._on_table
        new._or_replace = self._or_replace
        new._source_tables = self._source_tables
        new._statement = self._statement
        new._target_tables = self._target_tables
        new._task_name = self._task_name
        return new


cdef class CreateTriggerStep(PlanStep):
    """The CreateTrigger logical plan step."""

    cdef object _connector
    cdef str _event_kind
    cdef object _if_not_exists
    cdef object _or_replace
    cdef str _schedule
    cdef str _table_name
    cdef str _task_name
    cdef str _time_zone
    cdef str _trigger_name
    cdef str _window_source

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, connector=None, event_kind=None, if_not_exists=None, or_replace=None, schedule=None, table_name=None, task_name=None, time_zone=None, trigger_name=None, window_source=None):
        self.node_type = _step_types().CreateTrigger
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.connector = connector
        self.event_kind = event_kind
        self.if_not_exists = if_not_exists
        self.or_replace = or_replace
        self.schedule = schedule
        self.table_name = table_name
        self.task_name = task_name
        self.time_zone = time_zone
        self.trigger_name = trigger_name
        self.window_source = window_source

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def event_kind(self):
        return self._event_kind

    @event_kind.setter
    def event_kind(self, value):
        self._event_kind = value

    @property
    def if_not_exists(self):
        return self._if_not_exists

    @if_not_exists.setter
    def if_not_exists(self, value):
        _require_optional_bool("CreateTriggerStep.if_not_exists", value)
        self._if_not_exists = value

    @property
    def or_replace(self):
        return self._or_replace

    @or_replace.setter
    def or_replace(self, value):
        _require_optional_bool("CreateTriggerStep.or_replace", value)
        self._or_replace = value

    @property
    def schedule(self):
        return self._schedule

    @schedule.setter
    def schedule(self, value):
        self._schedule = value

    @property
    def table_name(self):
        return self._table_name

    @table_name.setter
    def table_name(self, value):
        self._table_name = value

    @property
    def task_name(self):
        return self._task_name

    @task_name.setter
    def task_name(self, value):
        self._task_name = value

    @property
    def time_zone(self):
        return self._time_zone

    @time_zone.setter
    def time_zone(self, value):
        self._time_zone = value

    @property
    def trigger_name(self):
        return self._trigger_name

    @trigger_name.setter
    def trigger_name(self, value):
        self._trigger_name = value

    @property
    def window_source(self):
        return self._window_source

    @window_source.setter
    def window_source(self, value):
        self._window_source = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["connector"] = self._connector
        out["event_kind"] = self._event_kind
        out["if_not_exists"] = self._if_not_exists
        out["or_replace"] = self._or_replace
        out["schedule"] = self._schedule
        out["table_name"] = self._table_name
        out["task_name"] = self._task_name
        out["time_zone"] = self._time_zone
        out["trigger_name"] = self._trigger_name
        out["window_source"] = self._window_source
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef CreateTriggerStep new = CreateTriggerStep.__new__(CreateTriggerStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._connector = _copy_field(self._connector, memo)
        new._event_kind = _copy_field(self._event_kind, memo)
        new._if_not_exists = _copy_field(self._if_not_exists, memo)
        new._or_replace = _copy_field(self._or_replace, memo)
        new._schedule = _copy_field(self._schedule, memo)
        new._table_name = _copy_field(self._table_name, memo)
        new._task_name = _copy_field(self._task_name, memo)
        new._time_zone = _copy_field(self._time_zone, memo)
        new._trigger_name = _copy_field(self._trigger_name, memo)
        new._window_source = _copy_field(self._window_source, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef CreateTriggerStep new = CreateTriggerStep.__new__(CreateTriggerStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._connector = self._connector
        new._event_kind = self._event_kind
        new._if_not_exists = self._if_not_exists
        new._or_replace = self._or_replace
        new._schedule = self._schedule
        new._table_name = self._table_name
        new._task_name = self._task_name
        new._time_zone = self._time_zone
        new._trigger_name = self._trigger_name
        new._window_source = self._window_source
        return new


cdef class CreateViewStep(PlanStep):
    """The CreateView logical plan step."""

    cdef object _connector
    cdef object _if_not_exists
    cdef object _or_replace
    cdef dict _query
    cdef object _view_name
    cdef object _view_schema
    cdef str _view_sql

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, connector=None, if_not_exists=None, or_replace=None, query=None, view_name=None, view_schema=None, view_sql=None):
        self.node_type = _step_types().CreateView
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.connector = connector
        self.if_not_exists = if_not_exists
        self.or_replace = or_replace
        self.query = query
        self.view_name = view_name
        self.view_schema = view_schema
        self.view_sql = view_sql

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def if_not_exists(self):
        return self._if_not_exists

    @if_not_exists.setter
    def if_not_exists(self, value):
        _require_optional_bool("CreateViewStep.if_not_exists", value)
        self._if_not_exists = value

    @property
    def or_replace(self):
        return self._or_replace

    @or_replace.setter
    def or_replace(self, value):
        _require_optional_bool("CreateViewStep.or_replace", value)
        self._or_replace = value

    @property
    def query(self):
        return self._query

    @query.setter
    def query(self, value):
        self._query = value

    @property
    def view_name(self):
        return self._view_name

    @view_name.setter
    def view_name(self, value):
        self._view_name = value

    @property
    def view_schema(self):
        return self._view_schema

    @view_schema.setter
    def view_schema(self, value):
        self._view_schema = value

    @property
    def view_sql(self):
        return self._view_sql

    @view_sql.setter
    def view_sql(self, value):
        self._view_sql = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["connector"] = self._connector
        out["if_not_exists"] = self._if_not_exists
        out["or_replace"] = self._or_replace
        out["query"] = self._query
        out["view_name"] = self._view_name
        out["view_schema"] = self._view_schema
        out["view_sql"] = self._view_sql
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef CreateViewStep new = CreateViewStep.__new__(CreateViewStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._connector = _copy_field(self._connector, memo)
        new._if_not_exists = _copy_field(self._if_not_exists, memo)
        new._or_replace = _copy_field(self._or_replace, memo)
        new._query = _copy_field(self._query, memo)
        new._view_name = _copy_field(self._view_name, memo)
        new._view_schema = _copy_field(self._view_schema, memo)
        new._view_sql = _copy_field(self._view_sql, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef CreateViewStep new = CreateViewStep.__new__(CreateViewStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._connector = self._connector
        new._if_not_exists = self._if_not_exists
        new._or_replace = self._or_replace
        new._query = self._query
        new._view_name = self._view_name
        new._view_schema = self._view_schema
        new._view_sql = self._view_sql
        return new


cdef class DetachRelationStep(PlanStep):
    """The DetachRelation logical plan step."""

    cdef str _relation_name

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, relation_name=None):
        self.node_type = _step_types().DetachRelation
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.relation_name = relation_name

    @property
    def relation_name(self):
        return self._relation_name

    @relation_name.setter
    def relation_name(self, value):
        self._relation_name = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["relation_name"] = self._relation_name
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef DetachRelationStep new = DetachRelationStep.__new__(DetachRelationStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._relation_name = _copy_field(self._relation_name, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef DetachRelationStep new = DetachRelationStep.__new__(DetachRelationStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._relation_name = self._relation_name
        return new


cdef class DistinctStep(PlanStep):
    """The Distinct logical plan step."""

    cdef str _alias
    cdef list _on

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, alias=None, on=None):
        self.node_type = _step_types().Distinct
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.alias = alias
        self.on = on

    @property
    def alias(self):
        return self._alias

    @alias.setter
    def alias(self, value):
        self._alias = value

    @property
    def on(self):
        return self._on

    @on.setter
    def on(self, value):
        self._on = _require_expression_list("DistinctStep.on", value)

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        _extend_expressions(out, self._on)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)
        self.on = _map_expressions(fn, self._on)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["alias"] = self._alias
        out["on"] = self._on
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef DistinctStep new = DistinctStep.__new__(DistinctStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._alias = _copy_field(self._alias, memo)
        new._on = _copy_field(self._on, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef DistinctStep new = DistinctStep.__new__(DistinctStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._alias = self._alias
        new._on = self._on
        return new


cdef class DropCollectionStep(PlanStep):
    """The DropCollection logical plan step."""

    cdef list _collection_names
    cdef dict _connectors
    cdef object _if_exists

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, collection_names=None, connectors=None, if_exists=None):
        self.node_type = _step_types().DropCollection
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.collection_names = collection_names
        self.connectors = connectors
        self.if_exists = if_exists

    @property
    def collection_names(self):
        return self._collection_names

    @collection_names.setter
    def collection_names(self, value):
        self._collection_names = value

    @property
    def connectors(self):
        return self._connectors

    @connectors.setter
    def connectors(self, value):
        self._connectors = value

    @property
    def if_exists(self):
        return self._if_exists

    @if_exists.setter
    def if_exists(self, value):
        _require_optional_bool("DropCollectionStep.if_exists", value)
        self._if_exists = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["collection_names"] = self._collection_names
        out["connectors"] = self._connectors
        out["if_exists"] = self._if_exists
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef DropCollectionStep new = DropCollectionStep.__new__(DropCollectionStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._collection_names = _copy_field(self._collection_names, memo)
        new._connectors = _copy_field(self._connectors, memo)
        new._if_exists = _copy_field(self._if_exists, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef DropCollectionStep new = DropCollectionStep.__new__(DropCollectionStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._collection_names = self._collection_names
        new._connectors = self._connectors
        new._if_exists = self._if_exists
        return new


cdef class DropColumnStep(PlanStep):
    """The DropColumn logical plan step."""

    cdef object _column_if_exists
    cdef str _column_name
    cdef object _connector
    cdef object _if_exists
    cdef str _relation_name

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, column_if_exists=None, column_name=None, connector=None, if_exists=None, relation_name=None):
        self.node_type = _step_types().DropColumn
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.column_if_exists = column_if_exists
        self.column_name = column_name
        self.connector = connector
        self.if_exists = if_exists
        self.relation_name = relation_name

    @property
    def column_if_exists(self):
        return self._column_if_exists

    @column_if_exists.setter
    def column_if_exists(self, value):
        _require_optional_bool("DropColumnStep.column_if_exists", value)
        self._column_if_exists = value

    @property
    def column_name(self):
        return self._column_name

    @column_name.setter
    def column_name(self, value):
        self._column_name = value

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def if_exists(self):
        return self._if_exists

    @if_exists.setter
    def if_exists(self, value):
        _require_optional_bool("DropColumnStep.if_exists", value)
        self._if_exists = value

    @property
    def relation_name(self):
        return self._relation_name

    @relation_name.setter
    def relation_name(self, value):
        self._relation_name = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["column_if_exists"] = self._column_if_exists
        out["column_name"] = self._column_name
        out["connector"] = self._connector
        out["if_exists"] = self._if_exists
        out["relation_name"] = self._relation_name
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef DropColumnStep new = DropColumnStep.__new__(DropColumnStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._column_if_exists = _copy_field(self._column_if_exists, memo)
        new._column_name = _copy_field(self._column_name, memo)
        new._connector = _copy_field(self._connector, memo)
        new._if_exists = _copy_field(self._if_exists, memo)
        new._relation_name = _copy_field(self._relation_name, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef DropColumnStep new = DropColumnStep.__new__(DropColumnStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._column_if_exists = self._column_if_exists
        new._column_name = self._column_name
        new._connector = self._connector
        new._if_exists = self._if_exists
        new._relation_name = self._relation_name
        return new


cdef class DropRelationStep(PlanStep):
    """The DropRelation logical plan step."""

    cdef dict _connectors
    cdef object _if_exists
    cdef object _is_materialized_view
    cdef list _relation_names

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, connectors=None, if_exists=None, is_materialized_view=None, relation_names=None):
        self.node_type = _step_types().DropRelation
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.connectors = connectors
        self.if_exists = if_exists
        self.is_materialized_view = is_materialized_view
        self.relation_names = relation_names

    @property
    def connectors(self):
        return self._connectors

    @connectors.setter
    def connectors(self, value):
        self._connectors = value

    @property
    def if_exists(self):
        return self._if_exists

    @if_exists.setter
    def if_exists(self, value):
        _require_optional_bool("DropRelationStep.if_exists", value)
        self._if_exists = value

    @property
    def is_materialized_view(self):
        return self._is_materialized_view

    @is_materialized_view.setter
    def is_materialized_view(self, value):
        _require_optional_bool("DropRelationStep.is_materialized_view", value)
        self._is_materialized_view = value

    @property
    def relation_names(self):
        return self._relation_names

    @relation_names.setter
    def relation_names(self, value):
        self._relation_names = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["connectors"] = self._connectors
        out["if_exists"] = self._if_exists
        out["is_materialized_view"] = self._is_materialized_view
        out["relation_names"] = self._relation_names
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef DropRelationStep new = DropRelationStep.__new__(DropRelationStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._connectors = _copy_field(self._connectors, memo)
        new._if_exists = _copy_field(self._if_exists, memo)
        new._is_materialized_view = _copy_field(self._is_materialized_view, memo)
        new._relation_names = _copy_field(self._relation_names, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef DropRelationStep new = DropRelationStep.__new__(DropRelationStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._connectors = self._connectors
        new._if_exists = self._if_exists
        new._is_materialized_view = self._is_materialized_view
        new._relation_names = self._relation_names
        return new


cdef class DropRelationshipStep(PlanStep):
    """The DropRelationship logical plan step."""

    cdef object _connector
    cdef object _constraint_if_exists
    cdef str _constraint_name
    cdef object _if_exists
    cdef str _relation_name
    cdef list _relation_parts

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, connector=None, constraint_if_exists=None, constraint_name=None, if_exists=None, relation_name=None, relation_parts=None):
        self.node_type = _step_types().DropRelationship
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.connector = connector
        self.constraint_if_exists = constraint_if_exists
        self.constraint_name = constraint_name
        self.if_exists = if_exists
        self.relation_name = relation_name
        self.relation_parts = relation_parts

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def constraint_if_exists(self):
        return self._constraint_if_exists

    @constraint_if_exists.setter
    def constraint_if_exists(self, value):
        _require_optional_bool("DropRelationshipStep.constraint_if_exists", value)
        self._constraint_if_exists = value

    @property
    def constraint_name(self):
        return self._constraint_name

    @constraint_name.setter
    def constraint_name(self, value):
        self._constraint_name = value

    @property
    def if_exists(self):
        return self._if_exists

    @if_exists.setter
    def if_exists(self, value):
        _require_optional_bool("DropRelationshipStep.if_exists", value)
        self._if_exists = value

    @property
    def relation_name(self):
        return self._relation_name

    @relation_name.setter
    def relation_name(self, value):
        self._relation_name = value

    @property
    def relation_parts(self):
        return self._relation_parts

    @relation_parts.setter
    def relation_parts(self, value):
        self._relation_parts = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["connector"] = self._connector
        out["constraint_if_exists"] = self._constraint_if_exists
        out["constraint_name"] = self._constraint_name
        out["if_exists"] = self._if_exists
        out["relation_name"] = self._relation_name
        out["relation_parts"] = self._relation_parts
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef DropRelationshipStep new = DropRelationshipStep.__new__(DropRelationshipStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._connector = _copy_field(self._connector, memo)
        new._constraint_if_exists = _copy_field(self._constraint_if_exists, memo)
        new._constraint_name = _copy_field(self._constraint_name, memo)
        new._if_exists = _copy_field(self._if_exists, memo)
        new._relation_name = _copy_field(self._relation_name, memo)
        new._relation_parts = _copy_field(self._relation_parts, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef DropRelationshipStep new = DropRelationshipStep.__new__(DropRelationshipStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._connector = self._connector
        new._constraint_if_exists = self._constraint_if_exists
        new._constraint_name = self._constraint_name
        new._if_exists = self._if_exists
        new._relation_name = self._relation_name
        new._relation_parts = self._relation_parts
        return new


cdef class DropTagStep(PlanStep):
    """The DropTag logical plan step."""

    cdef object _if_exists
    cdef str _relation_name
    cdef str _tag_name

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, if_exists=None, relation_name=None, tag_name=None):
        self.node_type = _step_types().DropTag
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.if_exists = if_exists
        self.relation_name = relation_name
        self.tag_name = tag_name

    @property
    def if_exists(self):
        return self._if_exists

    @if_exists.setter
    def if_exists(self, value):
        _require_optional_bool("DropTagStep.if_exists", value)
        self._if_exists = value

    @property
    def relation_name(self):
        return self._relation_name

    @relation_name.setter
    def relation_name(self, value):
        self._relation_name = value

    @property
    def tag_name(self):
        return self._tag_name

    @tag_name.setter
    def tag_name(self, value):
        self._tag_name = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["if_exists"] = self._if_exists
        out["relation_name"] = self._relation_name
        out["tag_name"] = self._tag_name
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef DropTagStep new = DropTagStep.__new__(DropTagStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._if_exists = _copy_field(self._if_exists, memo)
        new._relation_name = _copy_field(self._relation_name, memo)
        new._tag_name = _copy_field(self._tag_name, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef DropTagStep new = DropTagStep.__new__(DropTagStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._if_exists = self._if_exists
        new._relation_name = self._relation_name
        new._tag_name = self._tag_name
        return new


cdef class DropTaskStep(PlanStep):
    """The DropTask logical plan step."""

    cdef object _connector
    cdef object _if_exists
    cdef str _task_name

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, connector=None, if_exists=None, task_name=None):
        self.node_type = _step_types().DropTask
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.connector = connector
        self.if_exists = if_exists
        self.task_name = task_name

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def if_exists(self):
        return self._if_exists

    @if_exists.setter
    def if_exists(self, value):
        _require_optional_bool("DropTaskStep.if_exists", value)
        self._if_exists = value

    @property
    def task_name(self):
        return self._task_name

    @task_name.setter
    def task_name(self, value):
        self._task_name = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["connector"] = self._connector
        out["if_exists"] = self._if_exists
        out["task_name"] = self._task_name
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef DropTaskStep new = DropTaskStep.__new__(DropTaskStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._connector = _copy_field(self._connector, memo)
        new._if_exists = _copy_field(self._if_exists, memo)
        new._task_name = _copy_field(self._task_name, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef DropTaskStep new = DropTaskStep.__new__(DropTaskStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._connector = self._connector
        new._if_exists = self._if_exists
        new._task_name = self._task_name
        return new


cdef class DropTriggerStep(PlanStep):
    """The DropTrigger logical plan step."""

    cdef object _connector
    cdef object _if_exists
    cdef str _table_name
    cdef str _trigger_name

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, connector=None, if_exists=None, table_name=None, trigger_name=None):
        self.node_type = _step_types().DropTrigger
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.connector = connector
        self.if_exists = if_exists
        self.table_name = table_name
        self.trigger_name = trigger_name

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def if_exists(self):
        return self._if_exists

    @if_exists.setter
    def if_exists(self, value):
        _require_optional_bool("DropTriggerStep.if_exists", value)
        self._if_exists = value

    @property
    def table_name(self):
        return self._table_name

    @table_name.setter
    def table_name(self, value):
        self._table_name = value

    @property
    def trigger_name(self):
        return self._trigger_name

    @trigger_name.setter
    def trigger_name(self, value):
        self._trigger_name = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["connector"] = self._connector
        out["if_exists"] = self._if_exists
        out["table_name"] = self._table_name
        out["trigger_name"] = self._trigger_name
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef DropTriggerStep new = DropTriggerStep.__new__(DropTriggerStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._connector = _copy_field(self._connector, memo)
        new._if_exists = _copy_field(self._if_exists, memo)
        new._table_name = _copy_field(self._table_name, memo)
        new._trigger_name = _copy_field(self._trigger_name, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef DropTriggerStep new = DropTriggerStep.__new__(DropTriggerStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._connector = self._connector
        new._if_exists = self._if_exists
        new._table_name = self._table_name
        new._trigger_name = self._trigger_name
        return new


cdef class DropViewStep(PlanStep):
    """The DropView logical plan step."""

    cdef dict _connectors
    cdef object _if_exists
    cdef list _view_names

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, connectors=None, if_exists=None, view_names=None):
        self.node_type = _step_types().DropView
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.connectors = connectors
        self.if_exists = if_exists
        self.view_names = view_names

    @property
    def connectors(self):
        return self._connectors

    @connectors.setter
    def connectors(self, value):
        self._connectors = value

    @property
    def if_exists(self):
        return self._if_exists

    @if_exists.setter
    def if_exists(self, value):
        _require_optional_bool("DropViewStep.if_exists", value)
        self._if_exists = value

    @property
    def view_names(self):
        return self._view_names

    @view_names.setter
    def view_names(self, value):
        self._view_names = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["connectors"] = self._connectors
        out["if_exists"] = self._if_exists
        out["view_names"] = self._view_names
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef DropViewStep new = DropViewStep.__new__(DropViewStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._connectors = _copy_field(self._connectors, memo)
        new._if_exists = _copy_field(self._if_exists, memo)
        new._view_names = _copy_field(self._view_names, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef DropViewStep new = DropViewStep.__new__(DropViewStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._connectors = self._connectors
        new._if_exists = self._if_exists
        new._view_names = self._view_names
        return new


cdef class DropWorkspaceStep(PlanStep):
    """The DropWorkspace logical plan step."""

    cdef object _connector
    cdef object _if_exists
    cdef str _workspace_name

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, connector=None, if_exists=None, workspace_name=None):
        self.node_type = _step_types().DropWorkspace
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.connector = connector
        self.if_exists = if_exists
        self.workspace_name = workspace_name

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def if_exists(self):
        return self._if_exists

    @if_exists.setter
    def if_exists(self, value):
        _require_optional_bool("DropWorkspaceStep.if_exists", value)
        self._if_exists = value

    @property
    def workspace_name(self):
        return self._workspace_name

    @workspace_name.setter
    def workspace_name(self, value):
        self._workspace_name = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["connector"] = self._connector
        out["if_exists"] = self._if_exists
        out["workspace_name"] = self._workspace_name
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef DropWorkspaceStep new = DropWorkspaceStep.__new__(DropWorkspaceStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._connector = _copy_field(self._connector, memo)
        new._if_exists = _copy_field(self._if_exists, memo)
        new._workspace_name = _copy_field(self._workspace_name, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef DropWorkspaceStep new = DropWorkspaceStep.__new__(DropWorkspaceStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._connector = self._connector
        new._if_exists = self._if_exists
        new._workspace_name = self._workspace_name
        return new


cdef class ExceptStep(PlanStep):
    """The Except logical plan step."""

    cdef list _left_relation_names
    cdef str _modifier
    cdef list _right_relation_names

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, left_relation_names=None, modifier=None, right_relation_names=None):
        self.node_type = _step_types().Except
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.left_relation_names = left_relation_names
        self.modifier = modifier
        self.right_relation_names = right_relation_names

    @property
    def left_relation_names(self):
        return self._left_relation_names

    @left_relation_names.setter
    def left_relation_names(self, value):
        self._left_relation_names = value

    @property
    def modifier(self):
        return self._modifier

    @modifier.setter
    def modifier(self, value):
        self._modifier = value

    @property
    def right_relation_names(self):
        return self._right_relation_names

    @right_relation_names.setter
    def right_relation_names(self, value):
        self._right_relation_names = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["left_relation_names"] = self._left_relation_names
        out["modifier"] = self._modifier
        out["right_relation_names"] = self._right_relation_names
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef ExceptStep new = ExceptStep.__new__(ExceptStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._left_relation_names = _copy_field(self._left_relation_names, memo)
        new._modifier = _copy_field(self._modifier, memo)
        new._right_relation_names = _copy_field(self._right_relation_names, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef ExceptStep new = ExceptStep.__new__(ExceptStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._left_relation_names = self._left_relation_names
        new._modifier = self._modifier
        new._right_relation_names = self._right_relation_names
        return new


cdef class ExitStep(PlanStep):
    """The Exit logical plan step."""

    cdef list _hidden_columns
    cdef str _relation_name

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, hidden_columns=None, relation_name=None):
        self.node_type = _step_types().Exit
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.hidden_columns = hidden_columns
        self.relation_name = relation_name

    @property
    def hidden_columns(self):
        return self._hidden_columns

    @hidden_columns.setter
    def hidden_columns(self, value):
        self._hidden_columns = value

    @property
    def relation_name(self):
        return self._relation_name

    @relation_name.setter
    def relation_name(self, value):
        self._relation_name = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["hidden_columns"] = self._hidden_columns
        out["relation_name"] = self._relation_name
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef ExitStep new = ExitStep.__new__(ExitStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._hidden_columns = _copy_field(self._hidden_columns, memo)
        new._relation_name = _copy_field(self._relation_name, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef ExitStep new = ExitStep.__new__(ExitStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._hidden_columns = self._hidden_columns
        new._relation_name = self._relation_name
        return new


cdef class ExplainStep(PlanStep):
    """The Explain logical plan step."""

    cdef object _analyze
    cdef str _format

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, analyze=None, format=None):
        self.node_type = _step_types().Explain
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.analyze = analyze
        self.format = format

    @property
    def analyze(self):
        return self._analyze

    @analyze.setter
    def analyze(self, value):
        _require_optional_bool("ExplainStep.analyze", value)
        self._analyze = value

    @property
    def format(self):
        return self._format

    @format.setter
    def format(self, value):
        self._format = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["analyze"] = self._analyze
        out["format"] = self._format
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef ExplainStep new = ExplainStep.__new__(ExplainStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._analyze = _copy_field(self._analyze, memo)
        new._format = _copy_field(self._format, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef ExplainStep new = ExplainStep.__new__(ExplainStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._analyze = self._analyze
        new._format = self._format
        return new


cdef class FilterStep(PlanStep):
    """The Filter logical plan step."""

    cdef str _alias
    cdef object _condition
    cdef str _deep_restore_target
    cdef object _from_join_on
    cdef list _pre_inline_columns
    cdef object _pre_inline_condition
    cdef set _pre_inline_relations
    cdef object _relations
    cdef dict _sources

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, alias=None, condition=None, deep_restore_target=None, from_join_on=None, pre_inline_columns=None, pre_inline_condition=None, pre_inline_relations=None, relations=None, sources=None):
        self.node_type = _step_types().Filter
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.alias = alias
        self.condition = condition
        self.deep_restore_target = deep_restore_target
        self.from_join_on = from_join_on
        self.pre_inline_columns = pre_inline_columns
        self.pre_inline_condition = pre_inline_condition
        self.pre_inline_relations = pre_inline_relations
        self.relations = relations
        self.sources = sources

    @property
    def alias(self):
        return self._alias

    @alias.setter
    def alias(self, value):
        self._alias = value

    @property
    def condition(self):
        return self._condition

    @condition.setter
    def condition(self, value):
        _require_expression("FilterStep.condition", value)
        self._condition = value

    @property
    def deep_restore_target(self):
        return self._deep_restore_target

    @deep_restore_target.setter
    def deep_restore_target(self, value):
        self._deep_restore_target = value

    @property
    def from_join_on(self):
        return self._from_join_on

    @from_join_on.setter
    def from_join_on(self, value):
        _require_optional_bool("FilterStep.from_join_on", value)
        self._from_join_on = value

    @property
    def pre_inline_columns(self):
        return self._pre_inline_columns

    @pre_inline_columns.setter
    def pre_inline_columns(self, value):
        self._pre_inline_columns = _require_expression_list("FilterStep.pre_inline_columns", value)

    @property
    def pre_inline_condition(self):
        return self._pre_inline_condition

    @pre_inline_condition.setter
    def pre_inline_condition(self, value):
        _require_expression("FilterStep.pre_inline_condition", value)
        self._pre_inline_condition = value

    @property
    def pre_inline_relations(self):
        return self._pre_inline_relations

    @pre_inline_relations.setter
    def pre_inline_relations(self, value):
        self._pre_inline_relations = value

    @property
    def relations(self):
        return self._relations

    @relations.setter
    def relations(self, value):
        self._relations = value

    @property
    def sources(self):
        return self._sources

    @sources.setter
    def sources(self, value):
        self._sources = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        _append_expression(out, self._condition)
        _extend_expressions(out, self._pre_inline_columns)
        _append_expression(out, self._pre_inline_condition)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)
        self.condition = _map_expression(fn, self._condition)
        self.pre_inline_columns = _map_expressions(fn, self._pre_inline_columns)
        self.pre_inline_condition = _map_expression(fn, self._pre_inline_condition)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["alias"] = self._alias
        out["condition"] = self._condition
        out["deep_restore_target"] = self._deep_restore_target
        out["from_join_on"] = self._from_join_on
        out["pre_inline_columns"] = self._pre_inline_columns
        out["pre_inline_condition"] = self._pre_inline_condition
        out["pre_inline_relations"] = self._pre_inline_relations
        out["relations"] = self._relations
        out["sources"] = self._sources
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef FilterStep new = FilterStep.__new__(FilterStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._alias = _copy_field(self._alias, memo)
        new._condition = _copy_field(self._condition, memo)
        new._deep_restore_target = _copy_field(self._deep_restore_target, memo)
        new._from_join_on = _copy_field(self._from_join_on, memo)
        new._pre_inline_columns = _copy_field(self._pre_inline_columns, memo)
        new._pre_inline_condition = _copy_field(self._pre_inline_condition, memo)
        new._pre_inline_relations = _copy_field(self._pre_inline_relations, memo)
        new._relations = _copy_field(self._relations, memo)
        new._sources = _copy_field(self._sources, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef FilterStep new = FilterStep.__new__(FilterStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._alias = self._alias
        new._condition = self._condition
        new._deep_restore_target = self._deep_restore_target
        new._from_join_on = self._from_join_on
        new._pre_inline_columns = self._pre_inline_columns
        new._pre_inline_condition = self._pre_inline_condition
        new._pre_inline_relations = self._pre_inline_relations
        new._relations = self._relations
        new._sources = self._sources
        return new


cdef class FramedWindowStep(PlanStep):
    """The FramedWindow logical plan step."""

    cdef list _order_by
    cdef str _output_relation
    cdef list _outputs
    cdef list _partition_by
    cdef list _window_functions

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, order_by=None, output_relation=None, outputs=None, partition_by=None, window_functions=None):
        self.node_type = _step_types().FramedWindow
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.order_by = order_by
        self.output_relation = output_relation
        self.outputs = outputs
        self.partition_by = partition_by
        self.window_functions = window_functions

    @property
    def order_by(self):
        return self._order_by

    @order_by.setter
    def order_by(self, value):
        self._order_by = _require_order_list("FramedWindowStep.order_by", value)

    @property
    def output_relation(self):
        return self._output_relation

    @output_relation.setter
    def output_relation(self, value):
        self._output_relation = value

    @property
    def outputs(self):
        return self._outputs

    @outputs.setter
    def outputs(self, value):
        self._outputs = value

    @property
    def partition_by(self):
        return self._partition_by

    @partition_by.setter
    def partition_by(self, value):
        self._partition_by = _require_expression_list("FramedWindowStep.partition_by", value)

    @property
    def window_functions(self):
        return self._window_functions

    @window_functions.setter
    def window_functions(self, value):
        self._window_functions = _require_window_functions("FramedWindowStep.window_functions", value)

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        _extend_order_expressions(out, self._order_by)
        _extend_expressions(out, self._partition_by)
        _extend_window_expressions(out, self._window_functions)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)
        self.order_by = _map_order_expressions(fn, self._order_by)
        self.partition_by = _map_expressions(fn, self._partition_by)
        self.window_functions = _map_window_expressions(fn, self._window_functions)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["order_by"] = self._order_by
        out["output_relation"] = self._output_relation
        out["outputs"] = self._outputs
        out["partition_by"] = self._partition_by
        out["window_functions"] = self._window_functions
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef FramedWindowStep new = FramedWindowStep.__new__(FramedWindowStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._order_by = _copy_field(self._order_by, memo)
        new._output_relation = _copy_field(self._output_relation, memo)
        new._outputs = _copy_field(self._outputs, memo)
        new._partition_by = _copy_field(self._partition_by, memo)
        new._window_functions = _copy_field(self._window_functions, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef FramedWindowStep new = FramedWindowStep.__new__(FramedWindowStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._order_by = self._order_by
        new._output_relation = self._output_relation
        new._outputs = self._outputs
        new._partition_by = self._partition_by
        new._window_functions = self._window_functions
        return new


cdef class FunctionDatasetStep(PlanStep):
    """The FunctionDataset logical plan step."""

    cdef str _alias
    cdef list _args
    cdef tuple _column_aliases
    cdef object _connector
    cdef object _csv_fail_on_error
    cdef list _csv_files
    cdef object _csv_has_header_row
    cdef object _csv_infer_sample_size
    cdef dict _csv_physical_by_identity
    cdef list _csv_physical_columns
    cdef str _csv_separator
    cdef str _dataset
    cdef str _function
    cdef list _hints
    cdef object _jsonl_fail_on_error
    cdef list _jsonl_files
    cdef object _jsonl_infer_sample_size
    cdef object _jsonl_infer_schema
    cdef dict _jsonl_physical_by_identity
    cdef list _jsonl_physical_columns
    cdef object _manifest
    cdef dict _named_args
    cdef list _predicates
    cdef str _relation
    cdef str _relation_name
    cdef object _schema
    cdef str _series_column
    cdef str _unnest_target
    cdef list _values

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, alias=None, args=None, column_aliases=None, connector=None, csv_fail_on_error=None, csv_files=None, csv_has_header_row=None, csv_infer_sample_size=None, csv_physical_by_identity=None, csv_physical_columns=None, csv_separator=None, dataset=None, function=None, hints=None, jsonl_fail_on_error=None, jsonl_files=None, jsonl_infer_sample_size=None, jsonl_infer_schema=None, jsonl_physical_by_identity=None, jsonl_physical_columns=None, manifest=None, named_args=None, predicates=None, relation=None, relation_name=None, schema=None, series_column=None, unnest_target=None, values=None):
        self.node_type = _step_types().FunctionDataset
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.alias = alias
        self.args = args
        self.column_aliases = column_aliases
        self.connector = connector
        self.csv_fail_on_error = csv_fail_on_error
        self.csv_files = csv_files
        self.csv_has_header_row = csv_has_header_row
        self.csv_infer_sample_size = csv_infer_sample_size
        self.csv_physical_by_identity = csv_physical_by_identity
        self.csv_physical_columns = csv_physical_columns
        self.csv_separator = csv_separator
        self.dataset = dataset
        self.function = function
        self.hints = hints
        self.jsonl_fail_on_error = jsonl_fail_on_error
        self.jsonl_files = jsonl_files
        self.jsonl_infer_sample_size = jsonl_infer_sample_size
        self.jsonl_infer_schema = jsonl_infer_schema
        self.jsonl_physical_by_identity = jsonl_physical_by_identity
        self.jsonl_physical_columns = jsonl_physical_columns
        self.manifest = manifest
        self.named_args = named_args
        self.predicates = predicates
        self.relation = relation
        self.relation_name = relation_name
        self.schema = schema
        self.series_column = series_column
        self.unnest_target = unnest_target
        self.values = values

    @property
    def alias(self):
        return self._alias

    @alias.setter
    def alias(self, value):
        self._alias = value

    @property
    def args(self):
        return self._args

    @args.setter
    def args(self, value):
        self._args = _require_expression_list("FunctionDatasetStep.args", value)

    @property
    def column_aliases(self):
        return self._column_aliases

    @column_aliases.setter
    def column_aliases(self, value):
        self._column_aliases = value

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def csv_fail_on_error(self):
        return self._csv_fail_on_error

    @csv_fail_on_error.setter
    def csv_fail_on_error(self, value):
        _require_optional_bool("FunctionDatasetStep.csv_fail_on_error", value)
        self._csv_fail_on_error = value

    @property
    def csv_files(self):
        return self._csv_files

    @csv_files.setter
    def csv_files(self, value):
        self._csv_files = value

    @property
    def csv_has_header_row(self):
        return self._csv_has_header_row

    @csv_has_header_row.setter
    def csv_has_header_row(self, value):
        _require_optional_bool("FunctionDatasetStep.csv_has_header_row", value)
        self._csv_has_header_row = value

    @property
    def csv_infer_sample_size(self):
        return self._csv_infer_sample_size

    @csv_infer_sample_size.setter
    def csv_infer_sample_size(self, value):
        _require_optional_int("FunctionDatasetStep.csv_infer_sample_size", value)
        self._csv_infer_sample_size = value

    @property
    def csv_physical_by_identity(self):
        return self._csv_physical_by_identity

    @csv_physical_by_identity.setter
    def csv_physical_by_identity(self, value):
        self._csv_physical_by_identity = value

    @property
    def csv_physical_columns(self):
        return self._csv_physical_columns

    @csv_physical_columns.setter
    def csv_physical_columns(self, value):
        self._csv_physical_columns = value

    @property
    def csv_separator(self):
        return self._csv_separator

    @csv_separator.setter
    def csv_separator(self, value):
        self._csv_separator = value

    @property
    def dataset(self):
        return self._dataset

    @dataset.setter
    def dataset(self, value):
        self._dataset = value

    @property
    def function(self):
        return self._function

    @function.setter
    def function(self, value):
        self._function = value

    @property
    def hints(self):
        return self._hints

    @hints.setter
    def hints(self, value):
        self._hints = value

    @property
    def jsonl_fail_on_error(self):
        return self._jsonl_fail_on_error

    @jsonl_fail_on_error.setter
    def jsonl_fail_on_error(self, value):
        _require_optional_bool("FunctionDatasetStep.jsonl_fail_on_error", value)
        self._jsonl_fail_on_error = value

    @property
    def jsonl_files(self):
        return self._jsonl_files

    @jsonl_files.setter
    def jsonl_files(self, value):
        self._jsonl_files = value

    @property
    def jsonl_infer_sample_size(self):
        return self._jsonl_infer_sample_size

    @jsonl_infer_sample_size.setter
    def jsonl_infer_sample_size(self, value):
        _require_optional_int("FunctionDatasetStep.jsonl_infer_sample_size", value)
        self._jsonl_infer_sample_size = value

    @property
    def jsonl_infer_schema(self):
        return self._jsonl_infer_schema

    @jsonl_infer_schema.setter
    def jsonl_infer_schema(self, value):
        _require_optional_bool("FunctionDatasetStep.jsonl_infer_schema", value)
        self._jsonl_infer_schema = value

    @property
    def jsonl_physical_by_identity(self):
        return self._jsonl_physical_by_identity

    @jsonl_physical_by_identity.setter
    def jsonl_physical_by_identity(self, value):
        self._jsonl_physical_by_identity = value

    @property
    def jsonl_physical_columns(self):
        return self._jsonl_physical_columns

    @jsonl_physical_columns.setter
    def jsonl_physical_columns(self, value):
        self._jsonl_physical_columns = value

    @property
    def manifest(self):
        return self._manifest

    @manifest.setter
    def manifest(self, value):
        self._manifest = value

    @property
    def named_args(self):
        return self._named_args

    @named_args.setter
    def named_args(self, value):
        self._named_args = _require_expression_dict("FunctionDatasetStep.named_args", value)

    @property
    def predicates(self):
        return self._predicates

    @predicates.setter
    def predicates(self, value):
        self._predicates = _require_expression_list("FunctionDatasetStep.predicates", value)

    @property
    def relation(self):
        return self._relation

    @relation.setter
    def relation(self, value):
        self._relation = value

    @property
    def relation_name(self):
        return self._relation_name

    @relation_name.setter
    def relation_name(self, value):
        self._relation_name = value

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, value):
        self._schema = value

    @property
    def series_column(self):
        return self._series_column

    @series_column.setter
    def series_column(self, value):
        self._series_column = value

    @property
    def unnest_target(self):
        return self._unnest_target

    @unnest_target.setter
    def unnest_target(self, value):
        self._unnest_target = value

    @property
    def values(self):
        return self._values

    @values.setter
    def values(self, value):
        self._values = _require_rows("FunctionDatasetStep.values", value)

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        _extend_expressions(out, self._args)
        _extend_dict_expressions(out, self._named_args)
        _extend_expressions(out, self._predicates)
        _extend_row_expressions(out, self._values)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)
        self.args = _map_expressions(fn, self._args)
        self.named_args = _map_dict_expressions(fn, self._named_args)
        self.predicates = _map_expressions(fn, self._predicates)
        self.values = _map_row_expressions(fn, self._values)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["alias"] = self._alias
        out["args"] = self._args
        out["column_aliases"] = self._column_aliases
        out["connector"] = self._connector
        out["csv_fail_on_error"] = self._csv_fail_on_error
        out["csv_files"] = self._csv_files
        out["csv_has_header_row"] = self._csv_has_header_row
        out["csv_infer_sample_size"] = self._csv_infer_sample_size
        out["csv_physical_by_identity"] = self._csv_physical_by_identity
        out["csv_physical_columns"] = self._csv_physical_columns
        out["csv_separator"] = self._csv_separator
        out["dataset"] = self._dataset
        out["function"] = self._function
        out["hints"] = self._hints
        out["jsonl_fail_on_error"] = self._jsonl_fail_on_error
        out["jsonl_files"] = self._jsonl_files
        out["jsonl_infer_sample_size"] = self._jsonl_infer_sample_size
        out["jsonl_infer_schema"] = self._jsonl_infer_schema
        out["jsonl_physical_by_identity"] = self._jsonl_physical_by_identity
        out["jsonl_physical_columns"] = self._jsonl_physical_columns
        out["manifest"] = self._manifest
        out["named_args"] = self._named_args
        out["predicates"] = self._predicates
        out["relation"] = self._relation
        out["relation_name"] = self._relation_name
        out["schema"] = self._schema
        out["series_column"] = self._series_column
        out["unnest_target"] = self._unnest_target
        out["values"] = self._values
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef FunctionDatasetStep new = FunctionDatasetStep.__new__(FunctionDatasetStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._alias = _copy_field(self._alias, memo)
        new._args = _copy_field(self._args, memo)
        new._column_aliases = _copy_field(self._column_aliases, memo)
        new._connector = _copy_field(self._connector, memo)
        new._csv_fail_on_error = _copy_field(self._csv_fail_on_error, memo)
        new._csv_files = _copy_field(self._csv_files, memo)
        new._csv_has_header_row = _copy_field(self._csv_has_header_row, memo)
        new._csv_infer_sample_size = _copy_field(self._csv_infer_sample_size, memo)
        new._csv_physical_by_identity = _copy_field(self._csv_physical_by_identity, memo)
        new._csv_physical_columns = _copy_field(self._csv_physical_columns, memo)
        new._csv_separator = _copy_field(self._csv_separator, memo)
        new._dataset = _copy_field(self._dataset, memo)
        new._function = _copy_field(self._function, memo)
        new._hints = _copy_field(self._hints, memo)
        new._jsonl_fail_on_error = _copy_field(self._jsonl_fail_on_error, memo)
        new._jsonl_files = _copy_field(self._jsonl_files, memo)
        new._jsonl_infer_sample_size = _copy_field(self._jsonl_infer_sample_size, memo)
        new._jsonl_infer_schema = _copy_field(self._jsonl_infer_schema, memo)
        new._jsonl_physical_by_identity = _copy_field(self._jsonl_physical_by_identity, memo)
        new._jsonl_physical_columns = _copy_field(self._jsonl_physical_columns, memo)
        new._manifest = _copy_field(self._manifest, memo)
        new._named_args = _copy_field(self._named_args, memo)
        new._predicates = _copy_field(self._predicates, memo)
        new._relation = _copy_field(self._relation, memo)
        new._relation_name = _copy_field(self._relation_name, memo)
        new._schema = _copy_field(self._schema, memo)
        new._series_column = _copy_field(self._series_column, memo)
        new._unnest_target = _copy_field(self._unnest_target, memo)
        new._values = _copy_field(self._values, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef FunctionDatasetStep new = FunctionDatasetStep.__new__(FunctionDatasetStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._alias = self._alias
        new._args = self._args
        new._column_aliases = self._column_aliases
        new._connector = self._connector
        new._csv_fail_on_error = self._csv_fail_on_error
        new._csv_files = self._csv_files
        new._csv_has_header_row = self._csv_has_header_row
        new._csv_infer_sample_size = self._csv_infer_sample_size
        new._csv_physical_by_identity = self._csv_physical_by_identity
        new._csv_physical_columns = self._csv_physical_columns
        new._csv_separator = self._csv_separator
        new._dataset = self._dataset
        new._function = self._function
        new._hints = self._hints
        new._jsonl_fail_on_error = self._jsonl_fail_on_error
        new._jsonl_files = self._jsonl_files
        new._jsonl_infer_sample_size = self._jsonl_infer_sample_size
        new._jsonl_infer_schema = self._jsonl_infer_schema
        new._jsonl_physical_by_identity = self._jsonl_physical_by_identity
        new._jsonl_physical_columns = self._jsonl_physical_columns
        new._manifest = self._manifest
        new._named_args = self._named_args
        new._predicates = self._predicates
        new._relation = self._relation
        new._relation_name = self._relation_name
        new._schema = self._schema
        new._series_column = self._series_column
        new._unnest_target = self._unnest_target
        new._values = self._values
        return new


cdef class GrantAccessStep(PlanStep):
    """The GrantAccess logical plan step."""

    cdef object _execution_context
    cdef str _object_kind
    cdef str _object_name
    cdef str _pattern
    cdef str _principal
    cdef str _role

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, execution_context=None, object_kind=None, object_name=None, pattern=None, principal=None, role=None):
        self.node_type = _step_types().GrantAccess
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.execution_context = execution_context
        self.object_kind = object_kind
        self.object_name = object_name
        self.pattern = pattern
        self.principal = principal
        self.role = role

    @property
    def execution_context(self):
        return self._execution_context

    @execution_context.setter
    def execution_context(self, value):
        self._execution_context = value

    @property
    def object_kind(self):
        return self._object_kind

    @object_kind.setter
    def object_kind(self, value):
        self._object_kind = value

    @property
    def object_name(self):
        return self._object_name

    @object_name.setter
    def object_name(self, value):
        self._object_name = value

    @property
    def pattern(self):
        return self._pattern

    @pattern.setter
    def pattern(self, value):
        self._pattern = value

    @property
    def principal(self):
        return self._principal

    @principal.setter
    def principal(self, value):
        self._principal = value

    @property
    def role(self):
        return self._role

    @role.setter
    def role(self, value):
        self._role = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["execution_context"] = self._execution_context
        out["object_kind"] = self._object_kind
        out["object_name"] = self._object_name
        out["pattern"] = self._pattern
        out["principal"] = self._principal
        out["role"] = self._role
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef GrantAccessStep new = GrantAccessStep.__new__(GrantAccessStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._execution_context = _copy_field(self._execution_context, memo)
        new._object_kind = _copy_field(self._object_kind, memo)
        new._object_name = _copy_field(self._object_name, memo)
        new._pattern = _copy_field(self._pattern, memo)
        new._principal = _copy_field(self._principal, memo)
        new._role = _copy_field(self._role, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef GrantAccessStep new = GrantAccessStep.__new__(GrantAccessStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._execution_context = self._execution_context
        new._object_kind = self._object_kind
        new._object_name = self._object_name
        new._pattern = self._pattern
        new._principal = self._principal
        new._role = self._role
        return new


cdef class HeapSortStep(PlanStep):
    """The HeapSort logical plan step."""

    cdef object _limit
    cdef list _order_by
    cdef object _vector_topk_candidate

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, limit=None, order_by=None, vector_topk_candidate=None):
        self.node_type = _step_types().HeapSort
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.limit = limit
        self.order_by = order_by
        self.vector_topk_candidate = vector_topk_candidate

    @property
    def limit(self):
        return self._limit

    @limit.setter
    def limit(self, value):
        _require_optional_int("HeapSortStep.limit", value)
        self._limit = value

    @property
    def order_by(self):
        return self._order_by

    @order_by.setter
    def order_by(self, value):
        self._order_by = _require_order_list("HeapSortStep.order_by", value)

    @property
    def vector_topk_candidate(self):
        return self._vector_topk_candidate

    @vector_topk_candidate.setter
    def vector_topk_candidate(self, value):
        _require_optional_bool("HeapSortStep.vector_topk_candidate", value)
        self._vector_topk_candidate = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        _extend_order_expressions(out, self._order_by)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)
        self.order_by = _map_order_expressions(fn, self._order_by)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["limit"] = self._limit
        out["order_by"] = self._order_by
        out["vector_topk_candidate"] = self._vector_topk_candidate
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef HeapSortStep new = HeapSortStep.__new__(HeapSortStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._limit = _copy_field(self._limit, memo)
        new._order_by = _copy_field(self._order_by, memo)
        new._vector_topk_candidate = _copy_field(self._vector_topk_candidate, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef HeapSortStep new = HeapSortStep.__new__(HeapSortStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._limit = self._limit
        new._order_by = self._order_by
        new._vector_topk_candidate = self._vector_topk_candidate
        return new


cdef class InsertStep(PlanStep):
    """The Insert logical plan step."""

    cdef list _column_mapping
    cdef object _connector
    cdef object _create_target
    cdef dict _defining_query
    cdef str _executing_task
    cdef tuple _explicit_columns
    cdef object _if_not_exists
    cdef object _is_materialized_view
    cdef object _is_noop
    cdef object _is_refresh
    cdef object _is_replace
    cdef object _or_replace
    cdef str _produced_by
    cdef list _read_sources
    cdef str _relation_name
    cdef list _source_tables
    cdef str _source_tail_id
    cdef list _target_column_names
    cdef object _target_schema
    cdef object _values_feeder
    cdef object _write_coalesce_rows

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, column_mapping=None, connector=None, create_target=None, defining_query=None, executing_task=None, explicit_columns=None, if_not_exists=None, is_materialized_view=None, is_noop=None, is_refresh=None, is_replace=None, or_replace=None, produced_by=None, read_sources=None, relation_name=None, source_tables=None, source_tail_id=None, target_column_names=None, target_schema=None, values_feeder=None, write_coalesce_rows=None):
        self.node_type = _step_types().Insert
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.column_mapping = column_mapping
        self.connector = connector
        self.create_target = create_target
        self.defining_query = defining_query
        self.executing_task = executing_task
        self.explicit_columns = explicit_columns
        self.if_not_exists = if_not_exists
        self.is_materialized_view = is_materialized_view
        self.is_noop = is_noop
        self.is_refresh = is_refresh
        self.is_replace = is_replace
        self.or_replace = or_replace
        self.produced_by = produced_by
        self.read_sources = read_sources
        self.relation_name = relation_name
        self.source_tables = source_tables
        self.source_tail_id = source_tail_id
        self.target_column_names = target_column_names
        self.target_schema = target_schema
        self.values_feeder = values_feeder
        self.write_coalesce_rows = write_coalesce_rows

    @property
    def column_mapping(self):
        return self._column_mapping

    @column_mapping.setter
    def column_mapping(self, value):
        self._column_mapping = value

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def create_target(self):
        return self._create_target

    @create_target.setter
    def create_target(self, value):
        _require_optional_bool("InsertStep.create_target", value)
        self._create_target = value

    @property
    def defining_query(self):
        return self._defining_query

    @defining_query.setter
    def defining_query(self, value):
        self._defining_query = value

    @property
    def executing_task(self):
        return self._executing_task

    @executing_task.setter
    def executing_task(self, value):
        self._executing_task = value

    @property
    def explicit_columns(self):
        return self._explicit_columns

    @explicit_columns.setter
    def explicit_columns(self, value):
        self._explicit_columns = value

    @property
    def if_not_exists(self):
        return self._if_not_exists

    @if_not_exists.setter
    def if_not_exists(self, value):
        _require_optional_bool("InsertStep.if_not_exists", value)
        self._if_not_exists = value

    @property
    def is_materialized_view(self):
        return self._is_materialized_view

    @is_materialized_view.setter
    def is_materialized_view(self, value):
        _require_optional_bool("InsertStep.is_materialized_view", value)
        self._is_materialized_view = value

    @property
    def is_noop(self):
        return self._is_noop

    @is_noop.setter
    def is_noop(self, value):
        _require_optional_bool("InsertStep.is_noop", value)
        self._is_noop = value

    @property
    def is_refresh(self):
        return self._is_refresh

    @is_refresh.setter
    def is_refresh(self, value):
        _require_optional_bool("InsertStep.is_refresh", value)
        self._is_refresh = value

    @property
    def is_replace(self):
        return self._is_replace

    @is_replace.setter
    def is_replace(self, value):
        _require_optional_bool("InsertStep.is_replace", value)
        self._is_replace = value

    @property
    def or_replace(self):
        return self._or_replace

    @or_replace.setter
    def or_replace(self, value):
        _require_optional_bool("InsertStep.or_replace", value)
        self._or_replace = value

    @property
    def produced_by(self):
        return self._produced_by

    @produced_by.setter
    def produced_by(self, value):
        self._produced_by = value

    @property
    def read_sources(self):
        return self._read_sources

    @read_sources.setter
    def read_sources(self, value):
        self._read_sources = value

    @property
    def relation_name(self):
        return self._relation_name

    @relation_name.setter
    def relation_name(self, value):
        self._relation_name = value

    @property
    def source_tables(self):
        return self._source_tables

    @source_tables.setter
    def source_tables(self, value):
        self._source_tables = value

    @property
    def source_tail_id(self):
        return self._source_tail_id

    @source_tail_id.setter
    def source_tail_id(self, value):
        self._source_tail_id = value

    @property
    def target_column_names(self):
        return self._target_column_names

    @target_column_names.setter
    def target_column_names(self, value):
        self._target_column_names = value

    @property
    def target_schema(self):
        return self._target_schema

    @target_schema.setter
    def target_schema(self, value):
        self._target_schema = value

    @property
    def values_feeder(self):
        return self._values_feeder

    @values_feeder.setter
    def values_feeder(self, value):
        _require_plan_step("InsertStep.values_feeder", value)
        self._values_feeder = value

    @property
    def write_coalesce_rows(self):
        return self._write_coalesce_rows

    @write_coalesce_rows.setter
    def write_coalesce_rows(self, value):
        _require_optional_int("InsertStep.write_coalesce_rows", value)
        self._write_coalesce_rows = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["column_mapping"] = self._column_mapping
        out["connector"] = self._connector
        out["create_target"] = self._create_target
        out["defining_query"] = self._defining_query
        out["executing_task"] = self._executing_task
        out["explicit_columns"] = self._explicit_columns
        out["if_not_exists"] = self._if_not_exists
        out["is_materialized_view"] = self._is_materialized_view
        out["is_noop"] = self._is_noop
        out["is_refresh"] = self._is_refresh
        out["is_replace"] = self._is_replace
        out["or_replace"] = self._or_replace
        out["produced_by"] = self._produced_by
        out["read_sources"] = self._read_sources
        out["relation_name"] = self._relation_name
        out["source_tables"] = self._source_tables
        out["source_tail_id"] = self._source_tail_id
        out["target_column_names"] = self._target_column_names
        out["target_schema"] = self._target_schema
        out["values_feeder"] = self._values_feeder
        out["write_coalesce_rows"] = self._write_coalesce_rows
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef InsertStep new = InsertStep.__new__(InsertStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._column_mapping = _copy_field(self._column_mapping, memo)
        new._connector = _copy_field(self._connector, memo)
        new._create_target = _copy_field(self._create_target, memo)
        new._defining_query = _copy_field(self._defining_query, memo)
        new._executing_task = _copy_field(self._executing_task, memo)
        new._explicit_columns = _copy_field(self._explicit_columns, memo)
        new._if_not_exists = _copy_field(self._if_not_exists, memo)
        new._is_materialized_view = _copy_field(self._is_materialized_view, memo)
        new._is_noop = _copy_field(self._is_noop, memo)
        new._is_refresh = _copy_field(self._is_refresh, memo)
        new._is_replace = _copy_field(self._is_replace, memo)
        new._or_replace = _copy_field(self._or_replace, memo)
        new._produced_by = _copy_field(self._produced_by, memo)
        new._read_sources = _copy_field(self._read_sources, memo)
        new._relation_name = _copy_field(self._relation_name, memo)
        new._source_tables = _copy_field(self._source_tables, memo)
        new._source_tail_id = _copy_field(self._source_tail_id, memo)
        new._target_column_names = _copy_field(self._target_column_names, memo)
        new._target_schema = _copy_field(self._target_schema, memo)
        new._values_feeder = _copy_field(self._values_feeder, memo)
        new._write_coalesce_rows = _copy_field(self._write_coalesce_rows, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef InsertStep new = InsertStep.__new__(InsertStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._column_mapping = self._column_mapping
        new._connector = self._connector
        new._create_target = self._create_target
        new._defining_query = self._defining_query
        new._executing_task = self._executing_task
        new._explicit_columns = self._explicit_columns
        new._if_not_exists = self._if_not_exists
        new._is_materialized_view = self._is_materialized_view
        new._is_noop = self._is_noop
        new._is_refresh = self._is_refresh
        new._is_replace = self._is_replace
        new._or_replace = self._or_replace
        new._produced_by = self._produced_by
        new._read_sources = self._read_sources
        new._relation_name = self._relation_name
        new._source_tables = self._source_tables
        new._source_tail_id = self._source_tail_id
        new._target_column_names = self._target_column_names
        new._target_schema = self._target_schema
        new._values_feeder = self._values_feeder
        new._write_coalesce_rows = self._write_coalesce_rows
        return new


cdef class IntersectStep(PlanStep):
    """The Intersect logical plan step."""

    cdef list _left_relation_names
    cdef str _modifier
    cdef list _right_relation_names

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, left_relation_names=None, modifier=None, right_relation_names=None):
        self.node_type = _step_types().Intersect
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.left_relation_names = left_relation_names
        self.modifier = modifier
        self.right_relation_names = right_relation_names

    @property
    def left_relation_names(self):
        return self._left_relation_names

    @left_relation_names.setter
    def left_relation_names(self, value):
        self._left_relation_names = value

    @property
    def modifier(self):
        return self._modifier

    @modifier.setter
    def modifier(self, value):
        self._modifier = value

    @property
    def right_relation_names(self):
        return self._right_relation_names

    @right_relation_names.setter
    def right_relation_names(self, value):
        self._right_relation_names = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["left_relation_names"] = self._left_relation_names
        out["modifier"] = self._modifier
        out["right_relation_names"] = self._right_relation_names
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef IntersectStep new = IntersectStep.__new__(IntersectStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._left_relation_names = _copy_field(self._left_relation_names, memo)
        new._modifier = _copy_field(self._modifier, memo)
        new._right_relation_names = _copy_field(self._right_relation_names, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef IntersectStep new = IntersectStep.__new__(IntersectStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._left_relation_names = self._left_relation_names
        new._modifier = self._modifier
        new._right_relation_names = self._right_relation_names
        return new


cdef class JoinStep(PlanStep):
    """The Join logical plan step."""

    cdef str _alias
    cdef object _asof_condition
    cdef bytes _asof_left_column
    cdef str _asof_op
    cdef bytes _asof_right_column
    cdef bytes _band_column
    cdef str _band_column_name
    cdef object _band_lower
    cdef object _band_lower_closed
    cdef object _band_upper
    cdef object _band_upper_closed
    cdef object _existence_column
    cdef object _existence_three_valued
    cdef object _implied_join
    cdef object _is_window_join
    cdef bytes _left_column
    cdef list _left_columns
    cdef list _left_readers
    cdef list _left_relation_names
    cdef object _on
    cdef object _reducer_applied
    cdef list _relation_names
    cdef object _residual
    cdef bytes _right_column
    cdef list _right_columns
    cdef list _right_readers
    cdef list _right_relation_names
    cdef dict _schemas
    cdef list _setop_leg_columns
    cdef object _swap_build_side
    cdef str _type
    cdef list _using
    cdef list _using_merged

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, alias=None, asof_condition=None, asof_left_column=None, asof_op=None, asof_right_column=None, band_column=None, band_column_name=None, band_lower=None, band_lower_closed=None, band_upper=None, band_upper_closed=None, existence_column=None, existence_three_valued=None, implied_join=None, is_window_join=None, left_column=None, left_columns=None, left_readers=None, left_relation_names=None, on=None, reducer_applied=None, relation_names=None, residual=None, right_column=None, right_columns=None, right_readers=None, right_relation_names=None, schemas=None, setop_leg_columns=None, swap_build_side=None, type=None, using=None, using_merged=None):
        self.node_type = _step_types().Join
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.alias = alias
        self.asof_condition = asof_condition
        self.asof_left_column = asof_left_column
        self.asof_op = asof_op
        self.asof_right_column = asof_right_column
        self.band_column = band_column
        self.band_column_name = band_column_name
        self.band_lower = band_lower
        self.band_lower_closed = band_lower_closed
        self.band_upper = band_upper
        self.band_upper_closed = band_upper_closed
        self.existence_column = existence_column
        self.existence_three_valued = existence_three_valued
        self.implied_join = implied_join
        self.is_window_join = is_window_join
        self.left_column = left_column
        self.left_columns = left_columns
        self.left_readers = left_readers
        self.left_relation_names = left_relation_names
        self.on = on
        self.reducer_applied = reducer_applied
        self.relation_names = relation_names
        self.residual = residual
        self.right_column = right_column
        self.right_columns = right_columns
        self.right_readers = right_readers
        self.right_relation_names = right_relation_names
        self.schemas = schemas
        self.setop_leg_columns = setop_leg_columns
        self.swap_build_side = swap_build_side
        self.type = type
        self.using = using
        self.using_merged = using_merged

    @property
    def alias(self):
        return self._alias

    @alias.setter
    def alias(self, value):
        self._alias = value

    @property
    def asof_condition(self):
        return self._asof_condition

    @asof_condition.setter
    def asof_condition(self, value):
        _require_expression("JoinStep.asof_condition", value)
        self._asof_condition = value

    @property
    def asof_left_column(self):
        return self._asof_left_column

    @asof_left_column.setter
    def asof_left_column(self, value):
        self._asof_left_column = value

    @property
    def asof_op(self):
        return self._asof_op

    @asof_op.setter
    def asof_op(self, value):
        self._asof_op = value

    @property
    def asof_right_column(self):
        return self._asof_right_column

    @asof_right_column.setter
    def asof_right_column(self, value):
        self._asof_right_column = value

    @property
    def band_column(self):
        return self._band_column

    @band_column.setter
    def band_column(self, value):
        self._band_column = value

    @property
    def band_column_name(self):
        return self._band_column_name

    @band_column_name.setter
    def band_column_name(self, value):
        self._band_column_name = value

    @property
    def band_lower(self):
        return self._band_lower

    @band_lower.setter
    def band_lower(self, value):
        _require_expression("JoinStep.band_lower", value)
        self._band_lower = value

    @property
    def band_lower_closed(self):
        return self._band_lower_closed

    @band_lower_closed.setter
    def band_lower_closed(self, value):
        _require_optional_bool("JoinStep.band_lower_closed", value)
        self._band_lower_closed = value

    @property
    def band_upper(self):
        return self._band_upper

    @band_upper.setter
    def band_upper(self, value):
        _require_expression("JoinStep.band_upper", value)
        self._band_upper = value

    @property
    def band_upper_closed(self):
        return self._band_upper_closed

    @band_upper_closed.setter
    def band_upper_closed(self, value):
        _require_optional_bool("JoinStep.band_upper_closed", value)
        self._band_upper_closed = value

    @property
    def existence_column(self):
        return self._existence_column

    @existence_column.setter
    def existence_column(self, value):
        _require_expression("JoinStep.existence_column", value)
        self._existence_column = value

    @property
    def existence_three_valued(self):
        return self._existence_three_valued

    @existence_three_valued.setter
    def existence_three_valued(self, value):
        _require_optional_bool("JoinStep.existence_three_valued", value)
        self._existence_three_valued = value

    @property
    def implied_join(self):
        return self._implied_join

    @implied_join.setter
    def implied_join(self, value):
        _require_optional_bool("JoinStep.implied_join", value)
        self._implied_join = value

    @property
    def is_window_join(self):
        return self._is_window_join

    @is_window_join.setter
    def is_window_join(self, value):
        _require_optional_bool("JoinStep.is_window_join", value)
        self._is_window_join = value

    @property
    def left_column(self):
        return self._left_column

    @left_column.setter
    def left_column(self, value):
        self._left_column = value

    @property
    def left_columns(self):
        return self._left_columns

    @left_columns.setter
    def left_columns(self, value):
        self._left_columns = value

    @property
    def left_readers(self):
        return self._left_readers

    @left_readers.setter
    def left_readers(self, value):
        self._left_readers = value

    @property
    def left_relation_names(self):
        return self._left_relation_names

    @left_relation_names.setter
    def left_relation_names(self, value):
        self._left_relation_names = value

    @property
    def on(self):
        return self._on

    @on.setter
    def on(self, value):
        _require_expression("JoinStep.on", value)
        self._on = value

    @property
    def reducer_applied(self):
        return self._reducer_applied

    @reducer_applied.setter
    def reducer_applied(self, value):
        _require_optional_bool("JoinStep.reducer_applied", value)
        self._reducer_applied = value

    @property
    def relation_names(self):
        return self._relation_names

    @relation_names.setter
    def relation_names(self, value):
        self._relation_names = value

    @property
    def residual(self):
        return self._residual

    @residual.setter
    def residual(self, value):
        _require_expression("JoinStep.residual", value)
        self._residual = value

    @property
    def right_column(self):
        return self._right_column

    @right_column.setter
    def right_column(self, value):
        self._right_column = value

    @property
    def right_columns(self):
        return self._right_columns

    @right_columns.setter
    def right_columns(self, value):
        self._right_columns = value

    @property
    def right_readers(self):
        return self._right_readers

    @right_readers.setter
    def right_readers(self, value):
        self._right_readers = value

    @property
    def right_relation_names(self):
        return self._right_relation_names

    @right_relation_names.setter
    def right_relation_names(self, value):
        self._right_relation_names = value

    @property
    def schemas(self):
        return self._schemas

    @schemas.setter
    def schemas(self, value):
        self._schemas = value

    @property
    def setop_leg_columns(self):
        return self._setop_leg_columns

    @setop_leg_columns.setter
    def setop_leg_columns(self, value):
        self._setop_leg_columns = value

    @property
    def swap_build_side(self):
        return self._swap_build_side

    @swap_build_side.setter
    def swap_build_side(self, value):
        _require_optional_bool("JoinStep.swap_build_side", value)
        self._swap_build_side = value

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, value):
        self._type = value

    @property
    def using(self):
        return self._using

    @using.setter
    def using(self, value):
        self._using = _require_expression_list("JoinStep.using", value)

    @property
    def using_merged(self):
        return self._using_merged

    @using_merged.setter
    def using_merged(self, value):
        self._using_merged = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        _append_expression(out, self._asof_condition)
        _append_expression(out, self._band_lower)
        _append_expression(out, self._band_upper)
        _append_expression(out, self._existence_column)
        _append_expression(out, self._on)
        _append_expression(out, self._residual)
        _extend_expressions(out, self._using)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)
        self.asof_condition = _map_expression(fn, self._asof_condition)
        self.band_lower = _map_expression(fn, self._band_lower)
        self.band_upper = _map_expression(fn, self._band_upper)
        self.existence_column = _map_expression(fn, self._existence_column)
        self.on = _map_expression(fn, self._on)
        self.residual = _map_expression(fn, self._residual)
        self.using = _map_expressions(fn, self._using)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["alias"] = self._alias
        out["asof_condition"] = self._asof_condition
        out["asof_left_column"] = self._asof_left_column
        out["asof_op"] = self._asof_op
        out["asof_right_column"] = self._asof_right_column
        out["band_column"] = self._band_column
        out["band_column_name"] = self._band_column_name
        out["band_lower"] = self._band_lower
        out["band_lower_closed"] = self._band_lower_closed
        out["band_upper"] = self._band_upper
        out["band_upper_closed"] = self._band_upper_closed
        out["existence_column"] = self._existence_column
        out["existence_three_valued"] = self._existence_three_valued
        out["implied_join"] = self._implied_join
        out["is_window_join"] = self._is_window_join
        out["left_column"] = self._left_column
        out["left_columns"] = self._left_columns
        out["left_readers"] = self._left_readers
        out["left_relation_names"] = self._left_relation_names
        out["on"] = self._on
        out["reducer_applied"] = self._reducer_applied
        out["relation_names"] = self._relation_names
        out["residual"] = self._residual
        out["right_column"] = self._right_column
        out["right_columns"] = self._right_columns
        out["right_readers"] = self._right_readers
        out["right_relation_names"] = self._right_relation_names
        out["schemas"] = self._schemas
        out["setop_leg_columns"] = self._setop_leg_columns
        out["swap_build_side"] = self._swap_build_side
        out["type"] = self._type
        out["using"] = self._using
        out["using_merged"] = self._using_merged
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef JoinStep new = JoinStep.__new__(JoinStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._alias = _copy_field(self._alias, memo)
        new._asof_condition = _copy_field(self._asof_condition, memo)
        new._asof_left_column = _copy_field(self._asof_left_column, memo)
        new._asof_op = _copy_field(self._asof_op, memo)
        new._asof_right_column = _copy_field(self._asof_right_column, memo)
        new._band_column = _copy_field(self._band_column, memo)
        new._band_column_name = _copy_field(self._band_column_name, memo)
        new._band_lower = _copy_field(self._band_lower, memo)
        new._band_lower_closed = _copy_field(self._band_lower_closed, memo)
        new._band_upper = _copy_field(self._band_upper, memo)
        new._band_upper_closed = _copy_field(self._band_upper_closed, memo)
        new._existence_column = _copy_field(self._existence_column, memo)
        new._existence_three_valued = _copy_field(self._existence_three_valued, memo)
        new._implied_join = _copy_field(self._implied_join, memo)
        new._is_window_join = _copy_field(self._is_window_join, memo)
        new._left_column = _copy_field(self._left_column, memo)
        new._left_columns = _copy_field(self._left_columns, memo)
        new._left_readers = _copy_field(self._left_readers, memo)
        new._left_relation_names = _copy_field(self._left_relation_names, memo)
        new._on = _copy_field(self._on, memo)
        new._reducer_applied = _copy_field(self._reducer_applied, memo)
        new._relation_names = _copy_field(self._relation_names, memo)
        new._residual = _copy_field(self._residual, memo)
        new._right_column = _copy_field(self._right_column, memo)
        new._right_columns = _copy_field(self._right_columns, memo)
        new._right_readers = _copy_field(self._right_readers, memo)
        new._right_relation_names = _copy_field(self._right_relation_names, memo)
        new._schemas = _copy_field(self._schemas, memo)
        new._setop_leg_columns = _copy_field(self._setop_leg_columns, memo)
        new._swap_build_side = _copy_field(self._swap_build_side, memo)
        new._type = _copy_field(self._type, memo)
        new._using = _copy_field(self._using, memo)
        new._using_merged = _copy_field(self._using_merged, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef JoinStep new = JoinStep.__new__(JoinStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._alias = self._alias
        new._asof_condition = self._asof_condition
        new._asof_left_column = self._asof_left_column
        new._asof_op = self._asof_op
        new._asof_right_column = self._asof_right_column
        new._band_column = self._band_column
        new._band_column_name = self._band_column_name
        new._band_lower = self._band_lower
        new._band_lower_closed = self._band_lower_closed
        new._band_upper = self._band_upper
        new._band_upper_closed = self._band_upper_closed
        new._existence_column = self._existence_column
        new._existence_three_valued = self._existence_three_valued
        new._implied_join = self._implied_join
        new._is_window_join = self._is_window_join
        new._left_column = self._left_column
        new._left_columns = self._left_columns
        new._left_readers = self._left_readers
        new._left_relation_names = self._left_relation_names
        new._on = self._on
        new._reducer_applied = self._reducer_applied
        new._relation_names = self._relation_names
        new._residual = self._residual
        new._right_column = self._right_column
        new._right_columns = self._right_columns
        new._right_readers = self._right_readers
        new._right_relation_names = self._right_relation_names
        new._schemas = self._schemas
        new._setop_leg_columns = self._setop_leg_columns
        new._swap_build_side = self._swap_build_side
        new._type = self._type
        new._using = self._using
        new._using_merged = self._using_merged
        return new


cdef class LimitStep(PlanStep):
    """The Limit logical plan step."""

    cdef str _alias
    cdef object _limit
    cdef object _offset

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, alias=None, limit=None, offset=None):
        self.node_type = _step_types().Limit
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.alias = alias
        self.limit = limit
        self.offset = offset

    @property
    def alias(self):
        return self._alias

    @alias.setter
    def alias(self, value):
        self._alias = value

    @property
    def limit(self):
        return self._limit

    @limit.setter
    def limit(self, value):
        _require_optional_int("LimitStep.limit", value)
        self._limit = value

    @property
    def offset(self):
        return self._offset

    @offset.setter
    def offset(self, value):
        _require_optional_int("LimitStep.offset", value)
        self._offset = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["alias"] = self._alias
        out["limit"] = self._limit
        out["offset"] = self._offset
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef LimitStep new = LimitStep.__new__(LimitStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._alias = _copy_field(self._alias, memo)
        new._limit = _copy_field(self._limit, memo)
        new._offset = _copy_field(self._offset, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef LimitStep new = LimitStep.__new__(LimitStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._alias = self._alias
        new._limit = self._limit
        new._offset = self._offset
        return new


cdef class ListenStep(PlanStep):
    """The Listen logical plan step."""

    cdef object _connector
    cdef object _execution_context
    cdef str _object_kind
    cdef str _outcome
    cdef str _task_name

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, connector=None, execution_context=None, object_kind=None, outcome=None, task_name=None):
        self.node_type = _step_types().Listen
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.connector = connector
        self.execution_context = execution_context
        self.object_kind = object_kind
        self.outcome = outcome
        self.task_name = task_name

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def execution_context(self):
        return self._execution_context

    @execution_context.setter
    def execution_context(self, value):
        self._execution_context = value

    @property
    def object_kind(self):
        return self._object_kind

    @object_kind.setter
    def object_kind(self, value):
        self._object_kind = value

    @property
    def outcome(self):
        return self._outcome

    @outcome.setter
    def outcome(self, value):
        self._outcome = value

    @property
    def task_name(self):
        return self._task_name

    @task_name.setter
    def task_name(self, value):
        self._task_name = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["connector"] = self._connector
        out["execution_context"] = self._execution_context
        out["object_kind"] = self._object_kind
        out["outcome"] = self._outcome
        out["task_name"] = self._task_name
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef ListenStep new = ListenStep.__new__(ListenStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._connector = _copy_field(self._connector, memo)
        new._execution_context = _copy_field(self._execution_context, memo)
        new._object_kind = _copy_field(self._object_kind, memo)
        new._outcome = _copy_field(self._outcome, memo)
        new._task_name = _copy_field(self._task_name, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef ListenStep new = ListenStep.__new__(ListenStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._connector = self._connector
        new._execution_context = self._execution_context
        new._object_kind = self._object_kind
        new._outcome = self._outcome
        new._task_name = self._task_name
        return new


cdef class MaterializedCteRefStep(PlanStep):
    """The MaterializedCteRef logical plan step."""

    cdef str _alias
    cdef dict _cte_column_map
    cdef str _cte_key
    cdef str _cte_name
    cdef dict _hint_settings
    cdef list _hints
    cdef str _relation
    cdef object _schema

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, alias=None, cte_column_map=None, cte_key=None, cte_name=None, hint_settings=None, hints=None, relation=None, schema=None):
        self.node_type = _step_types().MaterializedCteRef
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.alias = alias
        self.cte_column_map = cte_column_map
        self.cte_key = cte_key
        self.cte_name = cte_name
        self.hint_settings = hint_settings
        self.hints = hints
        self.relation = relation
        self.schema = schema

    @property
    def alias(self):
        return self._alias

    @alias.setter
    def alias(self, value):
        self._alias = value

    @property
    def cte_column_map(self):
        return self._cte_column_map

    @cte_column_map.setter
    def cte_column_map(self, value):
        self._cte_column_map = value

    @property
    def cte_key(self):
        return self._cte_key

    @cte_key.setter
    def cte_key(self, value):
        self._cte_key = value

    @property
    def cte_name(self):
        return self._cte_name

    @cte_name.setter
    def cte_name(self, value):
        self._cte_name = value

    @property
    def hint_settings(self):
        return self._hint_settings

    @hint_settings.setter
    def hint_settings(self, value):
        self._hint_settings = _require_expression_dict("MaterializedCteRefStep.hint_settings", value)

    @property
    def hints(self):
        return self._hints

    @hints.setter
    def hints(self, value):
        self._hints = value

    @property
    def relation(self):
        return self._relation

    @relation.setter
    def relation(self, value):
        self._relation = value

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, value):
        self._schema = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        _extend_dict_expressions(out, self._hint_settings)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)
        self.hint_settings = _map_dict_expressions(fn, self._hint_settings)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["alias"] = self._alias
        out["cte_column_map"] = self._cte_column_map
        out["cte_key"] = self._cte_key
        out["cte_name"] = self._cte_name
        out["hint_settings"] = self._hint_settings
        out["hints"] = self._hints
        out["relation"] = self._relation
        out["schema"] = self._schema
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef MaterializedCteRefStep new = MaterializedCteRefStep.__new__(MaterializedCteRefStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._alias = _copy_field(self._alias, memo)
        new._cte_column_map = _copy_field(self._cte_column_map, memo)
        new._cte_key = _copy_field(self._cte_key, memo)
        new._cte_name = _copy_field(self._cte_name, memo)
        new._hint_settings = _copy_field(self._hint_settings, memo)
        new._hints = _copy_field(self._hints, memo)
        new._relation = _copy_field(self._relation, memo)
        new._schema = _copy_field(self._schema, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef MaterializedCteRefStep new = MaterializedCteRefStep.__new__(MaterializedCteRefStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._alias = self._alias
        new._cte_column_map = self._cte_column_map
        new._cte_key = self._cte_key
        new._cte_name = self._cte_name
        new._hint_settings = self._hint_settings
        new._hints = self._hints
        new._relation = self._relation
        new._schema = self._schema
        return new


cdef class MergeStep(PlanStep):
    """The Merge logical plan step."""

    cdef object _connector
    cdef list _file_paths
    cdef str _operation
    cdef str _produced_by
    cdef list _read_sources
    cdef str _relation_name
    cdef str _source_tail_id
    cdef str _statement_name
    cdef str _target_alias
    cdef tuple _target_column_names
    cdef object _target_schema

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, connector=None, file_paths=None, operation=None, produced_by=None, read_sources=None, relation_name=None, source_tail_id=None, statement_name=None, target_alias=None, target_column_names=None, target_schema=None):
        self.node_type = _step_types().Merge
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.connector = connector
        self.file_paths = file_paths
        self.operation = operation
        self.produced_by = produced_by
        self.read_sources = read_sources
        self.relation_name = relation_name
        self.source_tail_id = source_tail_id
        self.statement_name = statement_name
        self.target_alias = target_alias
        self.target_column_names = target_column_names
        self.target_schema = target_schema

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def file_paths(self):
        return self._file_paths

    @file_paths.setter
    def file_paths(self, value):
        self._file_paths = value

    @property
    def operation(self):
        return self._operation

    @operation.setter
    def operation(self, value):
        self._operation = value

    @property
    def produced_by(self):
        return self._produced_by

    @produced_by.setter
    def produced_by(self, value):
        self._produced_by = value

    @property
    def read_sources(self):
        return self._read_sources

    @read_sources.setter
    def read_sources(self, value):
        self._read_sources = value

    @property
    def relation_name(self):
        return self._relation_name

    @relation_name.setter
    def relation_name(self, value):
        self._relation_name = value

    @property
    def source_tail_id(self):
        return self._source_tail_id

    @source_tail_id.setter
    def source_tail_id(self, value):
        self._source_tail_id = value

    @property
    def statement_name(self):
        return self._statement_name

    @statement_name.setter
    def statement_name(self, value):
        self._statement_name = value

    @property
    def target_alias(self):
        return self._target_alias

    @target_alias.setter
    def target_alias(self, value):
        self._target_alias = value

    @property
    def target_column_names(self):
        return self._target_column_names

    @target_column_names.setter
    def target_column_names(self, value):
        self._target_column_names = value

    @property
    def target_schema(self):
        return self._target_schema

    @target_schema.setter
    def target_schema(self, value):
        self._target_schema = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["connector"] = self._connector
        out["file_paths"] = self._file_paths
        out["operation"] = self._operation
        out["produced_by"] = self._produced_by
        out["read_sources"] = self._read_sources
        out["relation_name"] = self._relation_name
        out["source_tail_id"] = self._source_tail_id
        out["statement_name"] = self._statement_name
        out["target_alias"] = self._target_alias
        out["target_column_names"] = self._target_column_names
        out["target_schema"] = self._target_schema
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef MergeStep new = MergeStep.__new__(MergeStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._connector = _copy_field(self._connector, memo)
        new._file_paths = _copy_field(self._file_paths, memo)
        new._operation = _copy_field(self._operation, memo)
        new._produced_by = _copy_field(self._produced_by, memo)
        new._read_sources = _copy_field(self._read_sources, memo)
        new._relation_name = _copy_field(self._relation_name, memo)
        new._source_tail_id = _copy_field(self._source_tail_id, memo)
        new._statement_name = _copy_field(self._statement_name, memo)
        new._target_alias = _copy_field(self._target_alias, memo)
        new._target_column_names = _copy_field(self._target_column_names, memo)
        new._target_schema = _copy_field(self._target_schema, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef MergeStep new = MergeStep.__new__(MergeStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._connector = self._connector
        new._file_paths = self._file_paths
        new._operation = self._operation
        new._produced_by = self._produced_by
        new._read_sources = self._read_sources
        new._relation_name = self._relation_name
        new._source_tail_id = self._source_tail_id
        new._statement_name = self._statement_name
        new._target_alias = self._target_alias
        new._target_column_names = self._target_column_names
        new._target_schema = self._target_schema
        return new


cdef class OrderStep(PlanStep):
    """The Order logical plan step."""

    cdef str _alias
    cdef list _order_by

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, alias=None, order_by=None):
        self.node_type = _step_types().Order
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.alias = alias
        self.order_by = order_by

    @property
    def alias(self):
        return self._alias

    @alias.setter
    def alias(self, value):
        self._alias = value

    @property
    def order_by(self):
        return self._order_by

    @order_by.setter
    def order_by(self, value):
        self._order_by = _require_order_list("OrderStep.order_by", value)

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        _extend_order_expressions(out, self._order_by)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)
        self.order_by = _map_order_expressions(fn, self._order_by)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["alias"] = self._alias
        out["order_by"] = self._order_by
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef OrderStep new = OrderStep.__new__(OrderStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._alias = _copy_field(self._alias, memo)
        new._order_by = _copy_field(self._order_by, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef OrderStep new = OrderStep.__new__(OrderStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._alias = self._alias
        new._order_by = self._order_by
        return new


cdef class ProjectStep(PlanStep):
    """The Project logical plan step."""

    cdef str _alias
    cdef object _estimated_row_count
    cdef list _except_columns
    cdef list _hidden_columns
    cdef list _hoisted_columns
    cdef list _passthrough_columns
    cdef object _schema
    cdef dict _sources

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, alias=None, estimated_row_count=None, except_columns=None, hidden_columns=None, hoisted_columns=None, passthrough_columns=None, schema=None, sources=None):
        self.node_type = _step_types().Project
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.alias = alias
        self.estimated_row_count = estimated_row_count
        self.except_columns = except_columns
        self.hidden_columns = hidden_columns
        self.hoisted_columns = hoisted_columns
        self.passthrough_columns = passthrough_columns
        self.schema = schema
        self.sources = sources

    @property
    def alias(self):
        return self._alias

    @alias.setter
    def alias(self, value):
        self._alias = value

    @property
    def estimated_row_count(self):
        return self._estimated_row_count

    @estimated_row_count.setter
    def estimated_row_count(self, value):
        _require_optional_int("ProjectStep.estimated_row_count", value)
        self._estimated_row_count = value

    @property
    def except_columns(self):
        return self._except_columns

    @except_columns.setter
    def except_columns(self, value):
        self._except_columns = _require_expression_list("ProjectStep.except_columns", value)

    @property
    def hidden_columns(self):
        return self._hidden_columns

    @hidden_columns.setter
    def hidden_columns(self, value):
        self._hidden_columns = value

    @property
    def hoisted_columns(self):
        return self._hoisted_columns

    @hoisted_columns.setter
    def hoisted_columns(self, value):
        self._hoisted_columns = _require_expression_list("ProjectStep.hoisted_columns", value)

    @property
    def passthrough_columns(self):
        return self._passthrough_columns

    @passthrough_columns.setter
    def passthrough_columns(self, value):
        self._passthrough_columns = _require_expression_list("ProjectStep.passthrough_columns", value)

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, value):
        self._schema = value

    @property
    def sources(self):
        return self._sources

    @sources.setter
    def sources(self, value):
        self._sources = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        _extend_expressions(out, self._except_columns)
        _extend_expressions(out, self._hoisted_columns)
        _extend_expressions(out, self._passthrough_columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)
        self.except_columns = _map_expressions(fn, self._except_columns)
        self.hoisted_columns = _map_expressions(fn, self._hoisted_columns)
        self.passthrough_columns = _map_expressions(fn, self._passthrough_columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["alias"] = self._alias
        out["estimated_row_count"] = self._estimated_row_count
        out["except_columns"] = self._except_columns
        out["hidden_columns"] = self._hidden_columns
        out["hoisted_columns"] = self._hoisted_columns
        out["passthrough_columns"] = self._passthrough_columns
        out["schema"] = self._schema
        out["sources"] = self._sources
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef ProjectStep new = ProjectStep.__new__(ProjectStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._alias = _copy_field(self._alias, memo)
        new._estimated_row_count = _copy_field(self._estimated_row_count, memo)
        new._except_columns = _copy_field(self._except_columns, memo)
        new._hidden_columns = _copy_field(self._hidden_columns, memo)
        new._hoisted_columns = _copy_field(self._hoisted_columns, memo)
        new._passthrough_columns = _copy_field(self._passthrough_columns, memo)
        new._schema = _copy_field(self._schema, memo)
        new._sources = _copy_field(self._sources, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef ProjectStep new = ProjectStep.__new__(ProjectStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._alias = self._alias
        new._estimated_row_count = self._estimated_row_count
        new._except_columns = self._except_columns
        new._hidden_columns = self._hidden_columns
        new._hoisted_columns = self._hoisted_columns
        new._passthrough_columns = self._passthrough_columns
        new._schema = self._schema
        new._sources = self._sources
        return new


cdef class RenameColumnStep(PlanStep):
    """The RenameColumn logical plan step."""

    cdef str _column_name
    cdef object _connector
    cdef object _if_exists
    cdef str _new_column_name
    cdef str _relation_name

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, column_name=None, connector=None, if_exists=None, new_column_name=None, relation_name=None):
        self.node_type = _step_types().RenameColumn
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.column_name = column_name
        self.connector = connector
        self.if_exists = if_exists
        self.new_column_name = new_column_name
        self.relation_name = relation_name

    @property
    def column_name(self):
        return self._column_name

    @column_name.setter
    def column_name(self, value):
        self._column_name = value

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def if_exists(self):
        return self._if_exists

    @if_exists.setter
    def if_exists(self, value):
        _require_optional_bool("RenameColumnStep.if_exists", value)
        self._if_exists = value

    @property
    def new_column_name(self):
        return self._new_column_name

    @new_column_name.setter
    def new_column_name(self, value):
        self._new_column_name = value

    @property
    def relation_name(self):
        return self._relation_name

    @relation_name.setter
    def relation_name(self, value):
        self._relation_name = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["column_name"] = self._column_name
        out["connector"] = self._connector
        out["if_exists"] = self._if_exists
        out["new_column_name"] = self._new_column_name
        out["relation_name"] = self._relation_name
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef RenameColumnStep new = RenameColumnStep.__new__(RenameColumnStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._column_name = _copy_field(self._column_name, memo)
        new._connector = _copy_field(self._connector, memo)
        new._if_exists = _copy_field(self._if_exists, memo)
        new._new_column_name = _copy_field(self._new_column_name, memo)
        new._relation_name = _copy_field(self._relation_name, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef RenameColumnStep new = RenameColumnStep.__new__(RenameColumnStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._column_name = self._column_name
        new._connector = self._connector
        new._if_exists = self._if_exists
        new._new_column_name = self._new_column_name
        new._relation_name = self._relation_name
        return new


cdef class RenameRelationStep(PlanStep):
    """The RenameRelation logical plan step."""

    cdef object _connector
    cdef object _if_exists
    cdef str _new_relation_name
    cdef str _relation_name

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, connector=None, if_exists=None, new_relation_name=None, relation_name=None):
        self.node_type = _step_types().RenameRelation
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.connector = connector
        self.if_exists = if_exists
        self.new_relation_name = new_relation_name
        self.relation_name = relation_name

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def if_exists(self):
        return self._if_exists

    @if_exists.setter
    def if_exists(self, value):
        _require_optional_bool("RenameRelationStep.if_exists", value)
        self._if_exists = value

    @property
    def new_relation_name(self):
        return self._new_relation_name

    @new_relation_name.setter
    def new_relation_name(self, value):
        self._new_relation_name = value

    @property
    def relation_name(self):
        return self._relation_name

    @relation_name.setter
    def relation_name(self, value):
        self._relation_name = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["connector"] = self._connector
        out["if_exists"] = self._if_exists
        out["new_relation_name"] = self._new_relation_name
        out["relation_name"] = self._relation_name
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef RenameRelationStep new = RenameRelationStep.__new__(RenameRelationStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._connector = _copy_field(self._connector, memo)
        new._if_exists = _copy_field(self._if_exists, memo)
        new._new_relation_name = _copy_field(self._new_relation_name, memo)
        new._relation_name = _copy_field(self._relation_name, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef RenameRelationStep new = RenameRelationStep.__new__(RenameRelationStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._connector = self._connector
        new._if_exists = self._if_exists
        new._new_relation_name = self._new_relation_name
        new._relation_name = self._relation_name
        return new


cdef class ResyncRelationStep(PlanStep):
    """The ResyncRelation logical plan step."""

    cdef object _force
    cdef str _relation_name

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, force=None, relation_name=None):
        self.node_type = _step_types().ResyncRelation
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.force = force
        self.relation_name = relation_name

    @property
    def force(self):
        return self._force

    @force.setter
    def force(self, value):
        _require_optional_bool("ResyncRelationStep.force", value)
        self._force = value

    @property
    def relation_name(self):
        return self._relation_name

    @relation_name.setter
    def relation_name(self, value):
        self._relation_name = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["force"] = self._force
        out["relation_name"] = self._relation_name
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef ResyncRelationStep new = ResyncRelationStep.__new__(ResyncRelationStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._force = _copy_field(self._force, memo)
        new._relation_name = _copy_field(self._relation_name, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef ResyncRelationStep new = ResyncRelationStep.__new__(ResyncRelationStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._force = self._force
        new._relation_name = self._relation_name
        return new


cdef class RevokeAccessStep(PlanStep):
    """The RevokeAccess logical plan step."""

    cdef object _execution_context
    cdef str _object_kind
    cdef str _object_name
    cdef str _pattern
    cdef str _principal
    cdef str _role

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, execution_context=None, object_kind=None, object_name=None, pattern=None, principal=None, role=None):
        self.node_type = _step_types().RevokeAccess
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.execution_context = execution_context
        self.object_kind = object_kind
        self.object_name = object_name
        self.pattern = pattern
        self.principal = principal
        self.role = role

    @property
    def execution_context(self):
        return self._execution_context

    @execution_context.setter
    def execution_context(self, value):
        self._execution_context = value

    @property
    def object_kind(self):
        return self._object_kind

    @object_kind.setter
    def object_kind(self, value):
        self._object_kind = value

    @property
    def object_name(self):
        return self._object_name

    @object_name.setter
    def object_name(self, value):
        self._object_name = value

    @property
    def pattern(self):
        return self._pattern

    @pattern.setter
    def pattern(self, value):
        self._pattern = value

    @property
    def principal(self):
        return self._principal

    @principal.setter
    def principal(self, value):
        self._principal = value

    @property
    def role(self):
        return self._role

    @role.setter
    def role(self, value):
        self._role = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["execution_context"] = self._execution_context
        out["object_kind"] = self._object_kind
        out["object_name"] = self._object_name
        out["pattern"] = self._pattern
        out["principal"] = self._principal
        out["role"] = self._role
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef RevokeAccessStep new = RevokeAccessStep.__new__(RevokeAccessStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._execution_context = _copy_field(self._execution_context, memo)
        new._object_kind = _copy_field(self._object_kind, memo)
        new._object_name = _copy_field(self._object_name, memo)
        new._pattern = _copy_field(self._pattern, memo)
        new._principal = _copy_field(self._principal, memo)
        new._role = _copy_field(self._role, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef RevokeAccessStep new = RevokeAccessStep.__new__(RevokeAccessStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._execution_context = self._execution_context
        new._object_kind = self._object_kind
        new._object_name = self._object_name
        new._pattern = self._pattern
        new._principal = self._principal
        new._role = self._role
        return new


cdef class RollbackRelationStep(PlanStep):
    """The RollbackRelation logical plan step."""

    cdef object _if_exists
    cdef str _relation_name
    cdef str _version_spec

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, if_exists=None, relation_name=None, version_spec=None):
        self.node_type = _step_types().RollbackRelation
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.if_exists = if_exists
        self.relation_name = relation_name
        self.version_spec = version_spec

    @property
    def if_exists(self):
        return self._if_exists

    @if_exists.setter
    def if_exists(self, value):
        _require_optional_bool("RollbackRelationStep.if_exists", value)
        self._if_exists = value

    @property
    def relation_name(self):
        return self._relation_name

    @relation_name.setter
    def relation_name(self, value):
        self._relation_name = value

    @property
    def version_spec(self):
        return self._version_spec

    @version_spec.setter
    def version_spec(self, value):
        self._version_spec = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["if_exists"] = self._if_exists
        out["relation_name"] = self._relation_name
        out["version_spec"] = self._version_spec
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef RollbackRelationStep new = RollbackRelationStep.__new__(RollbackRelationStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._if_exists = _copy_field(self._if_exists, memo)
        new._relation_name = _copy_field(self._relation_name, memo)
        new._version_spec = _copy_field(self._version_spec, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef RollbackRelationStep new = RollbackRelationStep.__new__(RollbackRelationStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._if_exists = self._if_exists
        new._relation_name = self._relation_name
        new._version_spec = self._version_spec
        return new


cdef class ScalarSubqueryGuardStep(PlanStep):
    """The ScalarSubqueryGuard logical plan step."""


    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None):
        self.node_type = _step_types().ScalarSubqueryGuard
        self._init_common(columns, all_relations, pre_update_columns, uuid)

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef ScalarSubqueryGuardStep new = ScalarSubqueryGuardStep.__new__(ScalarSubqueryGuardStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef ScalarSubqueryGuardStep new = ScalarSubqueryGuardStep.__new__(ScalarSubqueryGuardStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        return new


cdef class ScanStep(PlanStep):
    """The Scan logical plan step."""

    cdef str _alias
    cdef object _at_date
    cdef object _connector
    cdef object _dataset_committed_at
    cdef object _emit_row_identity
    cdef object _end_date
    cdef object _for_manifest_only
    cdef object _for_snapshots_only
    cdef dict _hint_settings
    cdef list _hints
    cdef str _history_view
    cdef object _internal_relation
    cdef set _length_only_columns
    cdef object _limit
    cdef object _manifest
    cdef str _pending_cte_key
    cdef list _predicates
    cdef list _pushed_aggregates
    cdef object _pushed_distinct
    cdef list _pushed_groups
    cdef str _relation
    cdef object _resolved_dataset
    cdef str _row_identity_statement
    cdef object _schema
    cdef str _source
    cdef object _start_date
    cdef object _topn_descending
    cdef object _topn_limit
    cdef list _topn_order_by
    cdef bytes _topn_sort_identity
    cdef str _topn_sort_name
    cdef list _unpruned_columns
    cdef object _version
    cdef str _version_tag
    cdef str _via_view

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, alias=None, at_date=None, connector=None, dataset_committed_at=None, emit_row_identity=None, end_date=None, for_manifest_only=None, for_snapshots_only=None, hint_settings=None, hints=None, history_view=None, internal_relation=None, length_only_columns=None, limit=None, manifest=None, pending_cte_key=None, predicates=None, pushed_aggregates=None, pushed_distinct=None, pushed_groups=None, relation=None, resolved_dataset=None, row_identity_statement=None, schema=None, source=None, start_date=None, topn_descending=None, topn_limit=None, topn_order_by=None, topn_sort_identity=None, topn_sort_name=None, unpruned_columns=None, version=None, version_tag=None, via_view=None):
        self.node_type = _step_types().Scan
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.alias = alias
        self.at_date = at_date
        self.connector = connector
        self.dataset_committed_at = dataset_committed_at
        self.emit_row_identity = emit_row_identity
        self.end_date = end_date
        self.for_manifest_only = for_manifest_only
        self.for_snapshots_only = for_snapshots_only
        self.hint_settings = hint_settings
        self.hints = hints
        self.history_view = history_view
        self.internal_relation = internal_relation
        self.length_only_columns = length_only_columns
        self.limit = limit
        self.manifest = manifest
        self.pending_cte_key = pending_cte_key
        self.predicates = predicates
        self.pushed_aggregates = pushed_aggregates
        self.pushed_distinct = pushed_distinct
        self.pushed_groups = pushed_groups
        self.relation = relation
        self.resolved_dataset = resolved_dataset
        self.row_identity_statement = row_identity_statement
        self.schema = schema
        self.source = source
        self.start_date = start_date
        self.topn_descending = topn_descending
        self.topn_limit = topn_limit
        self.topn_order_by = topn_order_by
        self.topn_sort_identity = topn_sort_identity
        self.topn_sort_name = topn_sort_name
        self.unpruned_columns = unpruned_columns
        self.version = version
        self.version_tag = version_tag
        self.via_view = via_view

    @property
    def alias(self):
        return self._alias

    @alias.setter
    def alias(self, value):
        self._alias = value

    @property
    def at_date(self):
        return self._at_date

    @at_date.setter
    def at_date(self, value):
        self._at_date = value

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def dataset_committed_at(self):
        return self._dataset_committed_at

    @dataset_committed_at.setter
    def dataset_committed_at(self, value):
        _require_optional_int("ScanStep.dataset_committed_at", value)
        self._dataset_committed_at = value

    @property
    def emit_row_identity(self):
        return self._emit_row_identity

    @emit_row_identity.setter
    def emit_row_identity(self, value):
        _require_optional_bool("ScanStep.emit_row_identity", value)
        self._emit_row_identity = value

    @property
    def end_date(self):
        return self._end_date

    @end_date.setter
    def end_date(self, value):
        self._end_date = value

    @property
    def for_manifest_only(self):
        return self._for_manifest_only

    @for_manifest_only.setter
    def for_manifest_only(self, value):
        _require_optional_bool("ScanStep.for_manifest_only", value)
        self._for_manifest_only = value

    @property
    def for_snapshots_only(self):
        return self._for_snapshots_only

    @for_snapshots_only.setter
    def for_snapshots_only(self, value):
        _require_optional_bool("ScanStep.for_snapshots_only", value)
        self._for_snapshots_only = value

    @property
    def hint_settings(self):
        return self._hint_settings

    @hint_settings.setter
    def hint_settings(self, value):
        self._hint_settings = _require_expression_dict("ScanStep.hint_settings", value)

    @property
    def hints(self):
        return self._hints

    @hints.setter
    def hints(self, value):
        self._hints = value

    @property
    def history_view(self):
        return self._history_view

    @history_view.setter
    def history_view(self, value):
        self._history_view = value

    @property
    def internal_relation(self):
        return self._internal_relation

    @internal_relation.setter
    def internal_relation(self, value):
        _require_optional_bool("ScanStep.internal_relation", value)
        self._internal_relation = value

    @property
    def length_only_columns(self):
        return self._length_only_columns

    @length_only_columns.setter
    def length_only_columns(self, value):
        self._length_only_columns = value

    @property
    def limit(self):
        return self._limit

    @limit.setter
    def limit(self, value):
        _require_optional_int("ScanStep.limit", value)
        self._limit = value

    @property
    def manifest(self):
        return self._manifest

    @manifest.setter
    def manifest(self, value):
        self._manifest = value

    @property
    def pending_cte_key(self):
        return self._pending_cte_key

    @pending_cte_key.setter
    def pending_cte_key(self, value):
        self._pending_cte_key = value

    @property
    def predicates(self):
        return self._predicates

    @predicates.setter
    def predicates(self, value):
        self._predicates = _require_expression_list("ScanStep.predicates", value)

    @property
    def pushed_aggregates(self):
        return self._pushed_aggregates

    @pushed_aggregates.setter
    def pushed_aggregates(self, value):
        self._pushed_aggregates = _require_expression_list("ScanStep.pushed_aggregates", value)

    @property
    def pushed_distinct(self):
        return self._pushed_distinct

    @pushed_distinct.setter
    def pushed_distinct(self, value):
        _require_optional_bool("ScanStep.pushed_distinct", value)
        self._pushed_distinct = value

    @property
    def pushed_groups(self):
        return self._pushed_groups

    @pushed_groups.setter
    def pushed_groups(self, value):
        self._pushed_groups = _require_expression_list("ScanStep.pushed_groups", value)

    @property
    def relation(self):
        return self._relation

    @relation.setter
    def relation(self, value):
        self._relation = value

    @property
    def resolved_dataset(self):
        return self._resolved_dataset

    @resolved_dataset.setter
    def resolved_dataset(self, value):
        self._resolved_dataset = value

    @property
    def row_identity_statement(self):
        return self._row_identity_statement

    @row_identity_statement.setter
    def row_identity_statement(self, value):
        self._row_identity_statement = value

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, value):
        self._schema = value

    @property
    def source(self):
        return self._source

    @source.setter
    def source(self, value):
        self._source = value

    @property
    def start_date(self):
        return self._start_date

    @start_date.setter
    def start_date(self, value):
        self._start_date = value

    @property
    def topn_descending(self):
        return self._topn_descending

    @topn_descending.setter
    def topn_descending(self, value):
        _require_optional_bool("ScanStep.topn_descending", value)
        self._topn_descending = value

    @property
    def topn_limit(self):
        return self._topn_limit

    @topn_limit.setter
    def topn_limit(self, value):
        _require_optional_int("ScanStep.topn_limit", value)
        self._topn_limit = value

    @property
    def topn_order_by(self):
        return self._topn_order_by

    @topn_order_by.setter
    def topn_order_by(self, value):
        self._topn_order_by = value

    @property
    def topn_sort_identity(self):
        return self._topn_sort_identity

    @topn_sort_identity.setter
    def topn_sort_identity(self, value):
        self._topn_sort_identity = value

    @property
    def topn_sort_name(self):
        return self._topn_sort_name

    @topn_sort_name.setter
    def topn_sort_name(self, value):
        self._topn_sort_name = value

    @property
    def unpruned_columns(self):
        return self._unpruned_columns

    @unpruned_columns.setter
    def unpruned_columns(self, value):
        self._unpruned_columns = value

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, value):
        _require_optional_int("ScanStep.version", value)
        self._version = value

    @property
    def version_tag(self):
        return self._version_tag

    @version_tag.setter
    def version_tag(self, value):
        self._version_tag = value

    @property
    def via_view(self):
        return self._via_view

    @via_view.setter
    def via_view(self, value):
        self._via_view = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        _extend_dict_expressions(out, self._hint_settings)
        _extend_expressions(out, self._predicates)
        _extend_expressions(out, self._pushed_aggregates)
        _extend_expressions(out, self._pushed_groups)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)
        self.hint_settings = _map_dict_expressions(fn, self._hint_settings)
        self.predicates = _map_expressions(fn, self._predicates)
        self.pushed_aggregates = _map_expressions(fn, self._pushed_aggregates)
        self.pushed_groups = _map_expressions(fn, self._pushed_groups)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["alias"] = self._alias
        out["at_date"] = self._at_date
        out["connector"] = self._connector
        out["dataset_committed_at"] = self._dataset_committed_at
        out["emit_row_identity"] = self._emit_row_identity
        out["end_date"] = self._end_date
        out["for_manifest_only"] = self._for_manifest_only
        out["for_snapshots_only"] = self._for_snapshots_only
        out["hint_settings"] = self._hint_settings
        out["hints"] = self._hints
        out["history_view"] = self._history_view
        out["internal_relation"] = self._internal_relation
        out["length_only_columns"] = self._length_only_columns
        out["limit"] = self._limit
        out["manifest"] = self._manifest
        out["pending_cte_key"] = self._pending_cte_key
        out["predicates"] = self._predicates
        out["pushed_aggregates"] = self._pushed_aggregates
        out["pushed_distinct"] = self._pushed_distinct
        out["pushed_groups"] = self._pushed_groups
        out["relation"] = self._relation
        out["resolved_dataset"] = self._resolved_dataset
        out["row_identity_statement"] = self._row_identity_statement
        out["schema"] = self._schema
        out["source"] = self._source
        out["start_date"] = self._start_date
        out["topn_descending"] = self._topn_descending
        out["topn_limit"] = self._topn_limit
        out["topn_order_by"] = self._topn_order_by
        out["topn_sort_identity"] = self._topn_sort_identity
        out["topn_sort_name"] = self._topn_sort_name
        out["unpruned_columns"] = self._unpruned_columns
        out["version"] = self._version
        out["version_tag"] = self._version_tag
        out["via_view"] = self._via_view
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef ScanStep new = ScanStep.__new__(ScanStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._alias = _copy_field(self._alias, memo)
        new._at_date = _copy_field(self._at_date, memo)
        new._connector = _copy_field(self._connector, memo)
        new._dataset_committed_at = _copy_field(self._dataset_committed_at, memo)
        new._emit_row_identity = _copy_field(self._emit_row_identity, memo)
        new._end_date = _copy_field(self._end_date, memo)
        new._for_manifest_only = _copy_field(self._for_manifest_only, memo)
        new._for_snapshots_only = _copy_field(self._for_snapshots_only, memo)
        new._hint_settings = _copy_field(self._hint_settings, memo)
        new._hints = _copy_field(self._hints, memo)
        new._history_view = _copy_field(self._history_view, memo)
        new._internal_relation = _copy_field(self._internal_relation, memo)
        new._length_only_columns = _copy_field(self._length_only_columns, memo)
        new._limit = _copy_field(self._limit, memo)
        new._manifest = _copy_field(self._manifest, memo)
        new._pending_cte_key = _copy_field(self._pending_cte_key, memo)
        new._predicates = _copy_field(self._predicates, memo)
        new._pushed_aggregates = _copy_field(self._pushed_aggregates, memo)
        new._pushed_distinct = _copy_field(self._pushed_distinct, memo)
        new._pushed_groups = _copy_field(self._pushed_groups, memo)
        new._relation = _copy_field(self._relation, memo)
        new._resolved_dataset = _copy_field(self._resolved_dataset, memo)
        new._row_identity_statement = _copy_field(self._row_identity_statement, memo)
        new._schema = _copy_field(self._schema, memo)
        new._source = _copy_field(self._source, memo)
        new._start_date = _copy_field(self._start_date, memo)
        new._topn_descending = _copy_field(self._topn_descending, memo)
        new._topn_limit = _copy_field(self._topn_limit, memo)
        new._topn_order_by = _copy_field(self._topn_order_by, memo)
        new._topn_sort_identity = _copy_field(self._topn_sort_identity, memo)
        new._topn_sort_name = _copy_field(self._topn_sort_name, memo)
        new._unpruned_columns = _copy_field(self._unpruned_columns, memo)
        new._version = _copy_field(self._version, memo)
        new._version_tag = _copy_field(self._version_tag, memo)
        new._via_view = _copy_field(self._via_view, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef ScanStep new = ScanStep.__new__(ScanStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._alias = self._alias
        new._at_date = self._at_date
        new._connector = self._connector
        new._dataset_committed_at = self._dataset_committed_at
        new._emit_row_identity = self._emit_row_identity
        new._end_date = self._end_date
        new._for_manifest_only = self._for_manifest_only
        new._for_snapshots_only = self._for_snapshots_only
        new._hint_settings = self._hint_settings
        new._hints = self._hints
        new._history_view = self._history_view
        new._internal_relation = self._internal_relation
        new._length_only_columns = self._length_only_columns
        new._limit = self._limit
        new._manifest = self._manifest
        new._pending_cte_key = self._pending_cte_key
        new._predicates = self._predicates
        new._pushed_aggregates = self._pushed_aggregates
        new._pushed_distinct = self._pushed_distinct
        new._pushed_groups = self._pushed_groups
        new._relation = self._relation
        new._resolved_dataset = self._resolved_dataset
        new._row_identity_statement = self._row_identity_statement
        new._schema = self._schema
        new._source = self._source
        new._start_date = self._start_date
        new._topn_descending = self._topn_descending
        new._topn_limit = self._topn_limit
        new._topn_order_by = self._topn_order_by
        new._topn_sort_identity = self._topn_sort_identity
        new._topn_sort_name = self._topn_sort_name
        new._unpruned_columns = self._unpruned_columns
        new._version = self._version
        new._version_tag = self._version_tag
        new._via_view = self._via_view
        return new


cdef class SetStep(PlanStep):
    """The Set logical plan step."""

    cdef object _value
    cdef str _variable
    cdef object _variables

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, value=None, variable=None, variables=None):
        self.node_type = _step_types().Set
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.value = value
        self.variable = variable
        self.variables = variables

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        _require_expression("SetStep.value", value)
        self._value = value

    @property
    def variable(self):
        return self._variable

    @variable.setter
    def variable(self, value):
        self._variable = value

    @property
    def variables(self):
        return self._variables

    @variables.setter
    def variables(self, value):
        self._variables = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        _append_expression(out, self._value)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)
        self.value = _map_expression(fn, self._value)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["value"] = self._value
        out["variable"] = self._variable
        out["variables"] = self._variables
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef SetStep new = SetStep.__new__(SetStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._value = _copy_field(self._value, memo)
        new._variable = _copy_field(self._variable, memo)
        new._variables = _copy_field(self._variables, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef SetStep new = SetStep.__new__(SetStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._value = self._value
        new._variable = self._variable
        new._variables = self._variables
        return new


cdef class ShowStep(PlanStep):
    """The Show logical plan step."""

    cdef object _connector
    cdef object _object_name
    cdef str _object_type
    cdef str _trigger_name

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, connector=None, object_name=None, object_type=None, trigger_name=None):
        self.node_type = _step_types().Show
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.connector = connector
        self.object_name = object_name
        self.object_type = object_type
        self.trigger_name = trigger_name

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def object_name(self):
        return self._object_name

    @object_name.setter
    def object_name(self, value):
        self._object_name = value

    @property
    def object_type(self):
        return self._object_type

    @object_type.setter
    def object_type(self, value):
        self._object_type = value

    @property
    def trigger_name(self):
        return self._trigger_name

    @trigger_name.setter
    def trigger_name(self, value):
        self._trigger_name = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["connector"] = self._connector
        out["object_name"] = self._object_name
        out["object_type"] = self._object_type
        out["trigger_name"] = self._trigger_name
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef ShowStep new = ShowStep.__new__(ShowStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._connector = _copy_field(self._connector, memo)
        new._object_name = _copy_field(self._object_name, memo)
        new._object_type = _copy_field(self._object_type, memo)
        new._trigger_name = _copy_field(self._trigger_name, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef ShowStep new = ShowStep.__new__(ShowStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._connector = self._connector
        new._object_name = self._object_name
        new._object_type = self._object_type
        new._trigger_name = self._trigger_name
        return new


cdef class ShowColumnsStep(PlanStep):
    """The ShowColumns logical plan step."""

    cdef object _extended
    cdef object _full
    cdef str _relation
    cdef object _schema

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, extended=None, full=None, relation=None, schema=None):
        self.node_type = _step_types().ShowColumns
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.extended = extended
        self.full = full
        self.relation = relation
        self.schema = schema

    @property
    def extended(self):
        return self._extended

    @extended.setter
    def extended(self, value):
        _require_optional_bool("ShowColumnsStep.extended", value)
        self._extended = value

    @property
    def full(self):
        return self._full

    @full.setter
    def full(self, value):
        _require_optional_bool("ShowColumnsStep.full", value)
        self._full = value

    @property
    def relation(self):
        return self._relation

    @relation.setter
    def relation(self, value):
        self._relation = value

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, value):
        self._schema = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["extended"] = self._extended
        out["full"] = self._full
        out["relation"] = self._relation
        out["schema"] = self._schema
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef ShowColumnsStep new = ShowColumnsStep.__new__(ShowColumnsStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._extended = _copy_field(self._extended, memo)
        new._full = _copy_field(self._full, memo)
        new._relation = _copy_field(self._relation, memo)
        new._schema = _copy_field(self._schema, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef ShowColumnsStep new = ShowColumnsStep.__new__(ShowColumnsStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._extended = self._extended
        new._full = self._full
        new._relation = self._relation
        new._schema = self._schema
        return new


cdef class ShowEffectiveGrantsOnStep(PlanStep):
    """The ShowEffectiveGrantsOn logical plan step."""

    cdef object _effective
    cdef object _execution_context
    cdef str _object_kind
    cdef str _object_name
    cdef str _pattern

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, effective=None, execution_context=None, object_kind=None, object_name=None, pattern=None):
        self.node_type = _step_types().ShowEffectiveGrantsOn
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.effective = effective
        self.execution_context = execution_context
        self.object_kind = object_kind
        self.object_name = object_name
        self.pattern = pattern

    @property
    def effective(self):
        return self._effective

    @effective.setter
    def effective(self, value):
        _require_optional_bool("ShowEffectiveGrantsOnStep.effective", value)
        self._effective = value

    @property
    def execution_context(self):
        return self._execution_context

    @execution_context.setter
    def execution_context(self, value):
        self._execution_context = value

    @property
    def object_kind(self):
        return self._object_kind

    @object_kind.setter
    def object_kind(self, value):
        self._object_kind = value

    @property
    def object_name(self):
        return self._object_name

    @object_name.setter
    def object_name(self, value):
        self._object_name = value

    @property
    def pattern(self):
        return self._pattern

    @pattern.setter
    def pattern(self, value):
        self._pattern = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["effective"] = self._effective
        out["execution_context"] = self._execution_context
        out["object_kind"] = self._object_kind
        out["object_name"] = self._object_name
        out["pattern"] = self._pattern
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef ShowEffectiveGrantsOnStep new = ShowEffectiveGrantsOnStep.__new__(ShowEffectiveGrantsOnStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._effective = _copy_field(self._effective, memo)
        new._execution_context = _copy_field(self._execution_context, memo)
        new._object_kind = _copy_field(self._object_kind, memo)
        new._object_name = _copy_field(self._object_name, memo)
        new._pattern = _copy_field(self._pattern, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef ShowEffectiveGrantsOnStep new = ShowEffectiveGrantsOnStep.__new__(ShowEffectiveGrantsOnStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._effective = self._effective
        new._execution_context = self._execution_context
        new._object_kind = self._object_kind
        new._object_name = self._object_name
        new._pattern = self._pattern
        return new


cdef class ShowGrantsOnStep(PlanStep):
    """The ShowGrantsOn logical plan step."""

    cdef object _effective
    cdef object _execution_context
    cdef str _object_kind
    cdef str _object_name
    cdef str _pattern

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, effective=None, execution_context=None, object_kind=None, object_name=None, pattern=None):
        self.node_type = _step_types().ShowGrantsOn
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.effective = effective
        self.execution_context = execution_context
        self.object_kind = object_kind
        self.object_name = object_name
        self.pattern = pattern

    @property
    def effective(self):
        return self._effective

    @effective.setter
    def effective(self, value):
        _require_optional_bool("ShowGrantsOnStep.effective", value)
        self._effective = value

    @property
    def execution_context(self):
        return self._execution_context

    @execution_context.setter
    def execution_context(self, value):
        self._execution_context = value

    @property
    def object_kind(self):
        return self._object_kind

    @object_kind.setter
    def object_kind(self, value):
        self._object_kind = value

    @property
    def object_name(self):
        return self._object_name

    @object_name.setter
    def object_name(self, value):
        self._object_name = value

    @property
    def pattern(self):
        return self._pattern

    @pattern.setter
    def pattern(self, value):
        self._pattern = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["effective"] = self._effective
        out["execution_context"] = self._execution_context
        out["object_kind"] = self._object_kind
        out["object_name"] = self._object_name
        out["pattern"] = self._pattern
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef ShowGrantsOnStep new = ShowGrantsOnStep.__new__(ShowGrantsOnStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._effective = _copy_field(self._effective, memo)
        new._execution_context = _copy_field(self._execution_context, memo)
        new._object_kind = _copy_field(self._object_kind, memo)
        new._object_name = _copy_field(self._object_name, memo)
        new._pattern = _copy_field(self._pattern, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef ShowGrantsOnStep new = ShowGrantsOnStep.__new__(ShowGrantsOnStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._effective = self._effective
        new._execution_context = self._execution_context
        new._object_kind = self._object_kind
        new._object_name = self._object_name
        new._pattern = self._pattern
        return new


cdef class ShowLineageStep(PlanStep):
    """The ShowLineage logical plan step."""

    cdef str _history_view
    cdef list _lineage
    cdef str _relation
    cdef object _schema

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, history_view=None, lineage=None, relation=None, schema=None):
        self.node_type = _step_types().ShowLineage
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.history_view = history_view
        self.lineage = lineage
        self.relation = relation
        self.schema = schema

    @property
    def history_view(self):
        return self._history_view

    @history_view.setter
    def history_view(self, value):
        self._history_view = value

    @property
    def lineage(self):
        return self._lineage

    @lineage.setter
    def lineage(self, value):
        self._lineage = value

    @property
    def relation(self):
        return self._relation

    @relation.setter
    def relation(self, value):
        self._relation = value

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, value):
        self._schema = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["history_view"] = self._history_view
        out["lineage"] = self._lineage
        out["relation"] = self._relation
        out["schema"] = self._schema
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef ShowLineageStep new = ShowLineageStep.__new__(ShowLineageStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._history_view = _copy_field(self._history_view, memo)
        new._lineage = _copy_field(self._lineage, memo)
        new._relation = _copy_field(self._relation, memo)
        new._schema = _copy_field(self._schema, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef ShowLineageStep new = ShowLineageStep.__new__(ShowLineageStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._history_view = self._history_view
        new._lineage = self._lineage
        new._relation = self._relation
        new._schema = self._schema
        return new


cdef class ShowManifestStep(PlanStep):
    """The ShowManifest logical plan step."""

    cdef object _manifest
    cdef str _relation
    cdef object _schema

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, manifest=None, relation=None, schema=None):
        self.node_type = _step_types().ShowManifest
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.manifest = manifest
        self.relation = relation
        self.schema = schema

    @property
    def manifest(self):
        return self._manifest

    @manifest.setter
    def manifest(self, value):
        self._manifest = value

    @property
    def relation(self):
        return self._relation

    @relation.setter
    def relation(self, value):
        self._relation = value

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, value):
        self._schema = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["manifest"] = self._manifest
        out["relation"] = self._relation
        out["schema"] = self._schema
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef ShowManifestStep new = ShowManifestStep.__new__(ShowManifestStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._manifest = _copy_field(self._manifest, memo)
        new._relation = _copy_field(self._relation, memo)
        new._schema = _copy_field(self._schema, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef ShowManifestStep new = ShowManifestStep.__new__(ShowManifestStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._manifest = self._manifest
        new._relation = self._relation
        new._schema = self._schema
        return new


cdef class ShowSnapshotsStep(PlanStep):
    """The ShowSnapshots logical plan step."""

    cdef str _history_view
    cdef str _relation
    cdef object _schema
    cdef object _snapshots

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, history_view=None, relation=None, schema=None, snapshots=None):
        self.node_type = _step_types().ShowSnapshots
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.history_view = history_view
        self.relation = relation
        self.schema = schema
        self.snapshots = snapshots

    @property
    def history_view(self):
        return self._history_view

    @history_view.setter
    def history_view(self, value):
        self._history_view = value

    @property
    def relation(self):
        return self._relation

    @relation.setter
    def relation(self, value):
        self._relation = value

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, value):
        self._schema = value

    @property
    def snapshots(self):
        return self._snapshots

    @snapshots.setter
    def snapshots(self, value):
        self._snapshots = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["history_view"] = self._history_view
        out["relation"] = self._relation
        out["schema"] = self._schema
        out["snapshots"] = self._snapshots
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef ShowSnapshotsStep new = ShowSnapshotsStep.__new__(ShowSnapshotsStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._history_view = _copy_field(self._history_view, memo)
        new._relation = _copy_field(self._relation, memo)
        new._schema = _copy_field(self._schema, memo)
        new._snapshots = _copy_field(self._snapshots, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef ShowSnapshotsStep new = ShowSnapshotsStep.__new__(ShowSnapshotsStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._history_view = self._history_view
        new._relation = self._relation
        new._schema = self._schema
        new._snapshots = self._snapshots
        return new


cdef class ShowSourcesStep(PlanStep):
    """The ShowSources logical plan step."""

    cdef str _history_view
    cdef str _relation
    cdef object _schema
    cdef list _sources

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, history_view=None, relation=None, schema=None, sources=None):
        self.node_type = _step_types().ShowSources
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.history_view = history_view
        self.relation = relation
        self.schema = schema
        self.sources = sources

    @property
    def history_view(self):
        return self._history_view

    @history_view.setter
    def history_view(self, value):
        self._history_view = value

    @property
    def relation(self):
        return self._relation

    @relation.setter
    def relation(self, value):
        self._relation = value

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, value):
        self._schema = value

    @property
    def sources(self):
        return self._sources

    @sources.setter
    def sources(self, value):
        self._sources = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["history_view"] = self._history_view
        out["relation"] = self._relation
        out["schema"] = self._schema
        out["sources"] = self._sources
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef ShowSourcesStep new = ShowSourcesStep.__new__(ShowSourcesStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._history_view = _copy_field(self._history_view, memo)
        new._relation = _copy_field(self._relation, memo)
        new._schema = _copy_field(self._schema, memo)
        new._sources = _copy_field(self._sources, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef ShowSourcesStep new = ShowSourcesStep.__new__(ShowSourcesStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._history_view = self._history_view
        new._relation = self._relation
        new._schema = self._schema
        new._sources = self._sources
        return new


cdef class SubqueryStep(PlanStep):
    """The Subquery logical plan step."""

    cdef str _alias
    cdef dict _hint_settings
    cdef list _hints
    cdef str _relation
    cdef object _schema
    cdef set _source_relations
    cdef list _unpruned_columns

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, alias=None, hint_settings=None, hints=None, relation=None, schema=None, source_relations=None, unpruned_columns=None):
        self.node_type = _step_types().Subquery
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.alias = alias
        self.hint_settings = hint_settings
        self.hints = hints
        self.relation = relation
        self.schema = schema
        self.source_relations = source_relations
        self.unpruned_columns = unpruned_columns

    @property
    def alias(self):
        return self._alias

    @alias.setter
    def alias(self, value):
        self._alias = value

    @property
    def hint_settings(self):
        return self._hint_settings

    @hint_settings.setter
    def hint_settings(self, value):
        self._hint_settings = _require_expression_dict("SubqueryStep.hint_settings", value)

    @property
    def hints(self):
        return self._hints

    @hints.setter
    def hints(self, value):
        self._hints = value

    @property
    def relation(self):
        return self._relation

    @relation.setter
    def relation(self, value):
        self._relation = value

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, value):
        self._schema = value

    @property
    def source_relations(self):
        return self._source_relations

    @source_relations.setter
    def source_relations(self, value):
        self._source_relations = value

    @property
    def unpruned_columns(self):
        return self._unpruned_columns

    @unpruned_columns.setter
    def unpruned_columns(self, value):
        self._unpruned_columns = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        _extend_dict_expressions(out, self._hint_settings)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)
        self.hint_settings = _map_dict_expressions(fn, self._hint_settings)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["alias"] = self._alias
        out["hint_settings"] = self._hint_settings
        out["hints"] = self._hints
        out["relation"] = self._relation
        out["schema"] = self._schema
        out["source_relations"] = self._source_relations
        out["unpruned_columns"] = self._unpruned_columns
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef SubqueryStep new = SubqueryStep.__new__(SubqueryStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._alias = _copy_field(self._alias, memo)
        new._hint_settings = _copy_field(self._hint_settings, memo)
        new._hints = _copy_field(self._hints, memo)
        new._relation = _copy_field(self._relation, memo)
        new._schema = _copy_field(self._schema, memo)
        new._source_relations = _copy_field(self._source_relations, memo)
        new._unpruned_columns = _copy_field(self._unpruned_columns, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef SubqueryStep new = SubqueryStep.__new__(SubqueryStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._alias = self._alias
        new._hint_settings = self._hint_settings
        new._hints = self._hints
        new._relation = self._relation
        new._schema = self._schema
        new._source_relations = self._source_relations
        new._unpruned_columns = self._unpruned_columns
        return new


cdef class TruncateRelationStep(PlanStep):
    """The TruncateRelation logical plan step."""

    cdef object _connector
    cdef object _if_exists
    cdef str _relation_name

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, connector=None, if_exists=None, relation_name=None):
        self.node_type = _step_types().TruncateRelation
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.connector = connector
        self.if_exists = if_exists
        self.relation_name = relation_name

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def if_exists(self):
        return self._if_exists

    @if_exists.setter
    def if_exists(self, value):
        _require_optional_bool("TruncateRelationStep.if_exists", value)
        self._if_exists = value

    @property
    def relation_name(self):
        return self._relation_name

    @relation_name.setter
    def relation_name(self, value):
        self._relation_name = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["connector"] = self._connector
        out["if_exists"] = self._if_exists
        out["relation_name"] = self._relation_name
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef TruncateRelationStep new = TruncateRelationStep.__new__(TruncateRelationStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._connector = _copy_field(self._connector, memo)
        new._if_exists = _copy_field(self._if_exists, memo)
        new._relation_name = _copy_field(self._relation_name, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef TruncateRelationStep new = TruncateRelationStep.__new__(TruncateRelationStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._connector = self._connector
        new._if_exists = self._if_exists
        new._relation_name = self._relation_name
        return new


cdef class UnionStep(PlanStep):
    """The Union logical plan step."""

    cdef str _alias
    cdef list _left_relation_names
    cdef str _modifier
    cdef list _right_relation_names
    cdef dict _sources

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, alias=None, left_relation_names=None, modifier=None, right_relation_names=None, sources=None):
        self.node_type = _step_types().Union
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.alias = alias
        self.left_relation_names = left_relation_names
        self.modifier = modifier
        self.right_relation_names = right_relation_names
        self.sources = sources

    @property
    def alias(self):
        return self._alias

    @alias.setter
    def alias(self, value):
        self._alias = value

    @property
    def left_relation_names(self):
        return self._left_relation_names

    @left_relation_names.setter
    def left_relation_names(self, value):
        self._left_relation_names = value

    @property
    def modifier(self):
        return self._modifier

    @modifier.setter
    def modifier(self, value):
        self._modifier = value

    @property
    def right_relation_names(self):
        return self._right_relation_names

    @right_relation_names.setter
    def right_relation_names(self, value):
        self._right_relation_names = value

    @property
    def sources(self):
        return self._sources

    @sources.setter
    def sources(self, value):
        self._sources = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["alias"] = self._alias
        out["left_relation_names"] = self._left_relation_names
        out["modifier"] = self._modifier
        out["right_relation_names"] = self._right_relation_names
        out["sources"] = self._sources
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef UnionStep new = UnionStep.__new__(UnionStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._alias = _copy_field(self._alias, memo)
        new._left_relation_names = _copy_field(self._left_relation_names, memo)
        new._modifier = _copy_field(self._modifier, memo)
        new._right_relation_names = _copy_field(self._right_relation_names, memo)
        new._sources = _copy_field(self._sources, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef UnionStep new = UnionStep.__new__(UnionStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._alias = self._alias
        new._left_relation_names = self._left_relation_names
        new._modifier = self._modifier
        new._right_relation_names = self._right_relation_names
        new._sources = self._sources
        return new


cdef class UnlistenStep(PlanStep):
    """The Unlisten logical plan step."""

    cdef object _connector
    cdef object _execution_context
    cdef str _object_kind
    cdef str _task_name

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, connector=None, execution_context=None, object_kind=None, task_name=None):
        self.node_type = _step_types().Unlisten
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.connector = connector
        self.execution_context = execution_context
        self.object_kind = object_kind
        self.task_name = task_name

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def execution_context(self):
        return self._execution_context

    @execution_context.setter
    def execution_context(self, value):
        self._execution_context = value

    @property
    def object_kind(self):
        return self._object_kind

    @object_kind.setter
    def object_kind(self, value):
        self._object_kind = value

    @property
    def task_name(self):
        return self._task_name

    @task_name.setter
    def task_name(self, value):
        self._task_name = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["connector"] = self._connector
        out["execution_context"] = self._execution_context
        out["object_kind"] = self._object_kind
        out["task_name"] = self._task_name
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef UnlistenStep new = UnlistenStep.__new__(UnlistenStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._connector = _copy_field(self._connector, memo)
        new._execution_context = _copy_field(self._execution_context, memo)
        new._object_kind = _copy_field(self._object_kind, memo)
        new._task_name = _copy_field(self._task_name, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef UnlistenStep new = UnlistenStep.__new__(UnlistenStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._connector = self._connector
        new._execution_context = self._execution_context
        new._object_kind = self._object_kind
        new._task_name = self._task_name
        return new


cdef class UnnestStep(PlanStep):
    """The Unnest logical plan step."""

    cdef str _alias
    cdef object _distinct_target
    cdef list _filter_conditions
    cdef str _type
    cdef str _unnest_alias
    cdef object _unnest_column
    cdef str _unnest_function
    cdef object _unnest_target

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, alias=None, distinct_target=None, filter_conditions=None, type=None, unnest_alias=None, unnest_column=None, unnest_function=None, unnest_target=None):
        self.node_type = _step_types().Unnest
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.alias = alias
        self.distinct_target = distinct_target
        self.filter_conditions = filter_conditions
        self.type = type
        self.unnest_alias = unnest_alias
        self.unnest_column = unnest_column
        self.unnest_function = unnest_function
        self.unnest_target = unnest_target

    @property
    def alias(self):
        return self._alias

    @alias.setter
    def alias(self, value):
        self._alias = value

    @property
    def distinct_target(self):
        return self._distinct_target

    @distinct_target.setter
    def distinct_target(self, value):
        _require_optional_bool("UnnestStep.distinct_target", value)
        self._distinct_target = value

    @property
    def filter_conditions(self):
        return self._filter_conditions

    @filter_conditions.setter
    def filter_conditions(self, value):
        self._filter_conditions = _require_expression_list("UnnestStep.filter_conditions", value)

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, value):
        self._type = value

    @property
    def unnest_alias(self):
        return self._unnest_alias

    @unnest_alias.setter
    def unnest_alias(self, value):
        self._unnest_alias = value

    @property
    def unnest_column(self):
        return self._unnest_column

    @unnest_column.setter
    def unnest_column(self, value):
        _require_expression("UnnestStep.unnest_column", value)
        self._unnest_column = value

    @property
    def unnest_function(self):
        return self._unnest_function

    @unnest_function.setter
    def unnest_function(self, value):
        self._unnest_function = value

    @property
    def unnest_target(self):
        return self._unnest_target

    @unnest_target.setter
    def unnest_target(self, value):
        _require_expression("UnnestStep.unnest_target", value)
        self._unnest_target = value

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        _extend_expressions(out, self._filter_conditions)
        _append_expression(out, self._unnest_column)
        _append_expression(out, self._unnest_target)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)
        self.filter_conditions = _map_expressions(fn, self._filter_conditions)
        self.unnest_column = _map_expression(fn, self._unnest_column)
        self.unnest_target = _map_expression(fn, self._unnest_target)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["alias"] = self._alias
        out["distinct_target"] = self._distinct_target
        out["filter_conditions"] = self._filter_conditions
        out["type"] = self._type
        out["unnest_alias"] = self._unnest_alias
        out["unnest_column"] = self._unnest_column
        out["unnest_function"] = self._unnest_function
        out["unnest_target"] = self._unnest_target
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef UnnestStep new = UnnestStep.__new__(UnnestStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._alias = _copy_field(self._alias, memo)
        new._distinct_target = _copy_field(self._distinct_target, memo)
        new._filter_conditions = _copy_field(self._filter_conditions, memo)
        new._type = _copy_field(self._type, memo)
        new._unnest_alias = _copy_field(self._unnest_alias, memo)
        new._unnest_column = _copy_field(self._unnest_column, memo)
        new._unnest_function = _copy_field(self._unnest_function, memo)
        new._unnest_target = _copy_field(self._unnest_target, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef UnnestStep new = UnnestStep.__new__(UnnestStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._alias = self._alias
        new._distinct_target = self._distinct_target
        new._filter_conditions = self._filter_conditions
        new._type = self._type
        new._unnest_alias = self._unnest_alias
        new._unnest_column = self._unnest_column
        new._unnest_function = self._unnest_function
        new._unnest_target = self._unnest_target
        return new


cdef class WindowStep(PlanStep):
    """The Window logical plan step."""

    cdef list _aggregates
    cdef list _order_by
    cdef str _output_relation
    cdef list _outputs
    cdef list _partition_by
    cdef object _top_k
    cdef list _window_functions

    def __init__(self, *, columns=None, all_relations=None, pre_update_columns=None, uuid=None, aggregates=None, order_by=None, output_relation=None, outputs=None, partition_by=None, top_k=None, window_functions=None):
        self.node_type = _step_types().Window
        self._init_common(columns, all_relations, pre_update_columns, uuid)
        self.aggregates = aggregates
        self.order_by = order_by
        self.output_relation = output_relation
        self.outputs = outputs
        self.partition_by = partition_by
        self.top_k = top_k
        self.window_functions = window_functions

    @property
    def aggregates(self):
        return self._aggregates

    @aggregates.setter
    def aggregates(self, value):
        self._aggregates = _require_expression_list("WindowStep.aggregates", value)

    @property
    def order_by(self):
        return self._order_by

    @order_by.setter
    def order_by(self, value):
        self._order_by = _require_order_list("WindowStep.order_by", value)

    @property
    def output_relation(self):
        return self._output_relation

    @output_relation.setter
    def output_relation(self, value):
        self._output_relation = value

    @property
    def outputs(self):
        return self._outputs

    @outputs.setter
    def outputs(self, value):
        self._outputs = value

    @property
    def partition_by(self):
        return self._partition_by

    @partition_by.setter
    def partition_by(self, value):
        self._partition_by = _require_expression_list("WindowStep.partition_by", value)

    @property
    def top_k(self):
        return self._top_k

    @top_k.setter
    def top_k(self, value):
        _require_optional_int("WindowStep.top_k", value)
        self._top_k = value

    @property
    def window_functions(self):
        return self._window_functions

    @window_functions.setter
    def window_functions(self, value):
        self._window_functions = _require_window_functions("WindowStep.window_functions", value)

    cpdef tuple expressions(self, bint include_columns=True):
        cdef list out = []
        if include_columns:
            _extend_expressions(out, self._columns)
        _extend_expressions(out, self._aggregates)
        _extend_order_expressions(out, self._order_by)
        _extend_expressions(out, self._partition_by)
        _extend_window_expressions(out, self._window_functions)
        return tuple(out)

    cpdef map_expressions(self, object fn):
        self.columns = _map_expressions(fn, self._columns)
        self.aggregates = _map_expressions(fn, self._aggregates)
        self.order_by = _map_order_expressions(fn, self._order_by)
        self.partition_by = _map_expressions(fn, self._partition_by)
        self.window_functions = _map_window_expressions(fn, self._window_functions)

    cpdef dict field_values(self):
        cdef dict out = self._common_values()
        out["aggregates"] = self._aggregates
        out["order_by"] = self._order_by
        out["output_relation"] = self._output_relation
        out["outputs"] = self._outputs
        out["partition_by"] = self._partition_by
        out["top_k"] = self._top_k
        out["window_functions"] = self._window_functions
        return out

    cpdef PlanStep copy(self, dict memo=None):
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        cdef WindowStep new = WindowStep.__new__(WindowStep)
        new.node_type = self.node_type
        memo[id(self)] = new
        self._copy_common_into(new, memo)
        new._aggregates = _copy_field(self._aggregates, memo)
        new._order_by = _copy_field(self._order_by, memo)
        new._output_relation = _copy_field(self._output_relation, memo)
        new._outputs = _copy_field(self._outputs, memo)
        new._partition_by = _copy_field(self._partition_by, memo)
        new._top_k = _copy_field(self._top_k, memo)
        new._window_functions = _copy_field(self._window_functions, memo)
        return new

    cdef PlanStep _shallow_copy(self):
        cdef WindowStep new = WindowStep.__new__(WindowStep)
        new.node_type = self.node_type
        self._share_common_into(new)
        new._aggregates = self._aggregates
        new._order_by = self._order_by
        new._output_relation = self._output_relation
        new._outputs = self._outputs
        new._partition_by = self._partition_by
        new._top_k = self._top_k
        new._window_functions = self._window_functions
        return new


PLAN_STEP_TYPES = frozenset({AddColumnStep, AddRelationshipStep, AggregateStep, AggregateAndGroupStep, AlterColumnTypeStep, AlterMaterializedViewOwnerStep, AlterMaterializedViewSuspendedStep, AlterRelationStep, AlterTaskStep, AlterTriggerMinimumIntervalStep, AlterTriggerOwnerStep, AlterTriggerSuspendedStep, AlterViewStep, AlterWorkspaceStep, AlterWorkspaceSecureStep, AnalyzeStep, CallProcedureStep, CloneCollectionStep, CloneRelationStep, CommentStep, CompactionCommitStep, CreateCollectionStep, CreateRelationStep, CreateTagStep, CreateTaskStep, CreateTriggerStep, CreateViewStep, DetachRelationStep, DistinctStep, DropCollectionStep, DropColumnStep, DropRelationStep, DropRelationshipStep, DropTagStep, DropTaskStep, DropTriggerStep, DropViewStep, DropWorkspaceStep, ExceptStep, ExitStep, ExplainStep, FilterStep, FramedWindowStep, FunctionDatasetStep, GrantAccessStep, HeapSortStep, InsertStep, IntersectStep, JoinStep, LimitStep, ListenStep, MaterializedCteRefStep, MergeStep, OrderStep, ProjectStep, RenameColumnStep, RenameRelationStep, ResyncRelationStep, RevokeAccessStep, RollbackRelationStep, ScalarSubqueryGuardStep, ScanStep, SetStep, ShowStep, ShowColumnsStep, ShowEffectiveGrantsOnStep, ShowGrantsOnStep, ShowLineageStep, ShowManifestStep, ShowSnapshotsStep, ShowSourcesStep, SubqueryStep, TruncateRelationStep, UnionStep, UnlistenStep, UnnestStep, WindowStep})

# The class for each LogicalPlanStepType, for the few sites that build a step
# whose type is only known at run time.
cdef dict _STEP_CLASSES = None


cpdef dict step_classes():
    global _STEP_CLASSES
    if _STEP_CLASSES is None:
        T = _step_types()
        _STEP_CLASSES = {T.AddColumn: AddColumnStep, T.AddRelationship: AddRelationshipStep, T.Aggregate: AggregateStep, T.AggregateAndGroup: AggregateAndGroupStep, T.AlterColumnType: AlterColumnTypeStep, T.AlterMaterializedViewOwner: AlterMaterializedViewOwnerStep, T.AlterMaterializedViewSuspended: AlterMaterializedViewSuspendedStep, T.AlterRelation: AlterRelationStep, T.AlterTask: AlterTaskStep, T.AlterTriggerMinimumInterval: AlterTriggerMinimumIntervalStep, T.AlterTriggerOwner: AlterTriggerOwnerStep, T.AlterTriggerSuspended: AlterTriggerSuspendedStep, T.AlterView: AlterViewStep, T.AlterWorkspace: AlterWorkspaceStep, T.AlterWorkspaceSecure: AlterWorkspaceSecureStep, T.Analyze: AnalyzeStep, T.CallProcedure: CallProcedureStep, T.CloneCollection: CloneCollectionStep, T.CloneRelation: CloneRelationStep, T.Comment: CommentStep, T.CompactionCommit: CompactionCommitStep, T.CreateCollection: CreateCollectionStep, T.CreateRelation: CreateRelationStep, T.CreateTag: CreateTagStep, T.CreateTask: CreateTaskStep, T.CreateTrigger: CreateTriggerStep, T.CreateView: CreateViewStep, T.DetachRelation: DetachRelationStep, T.Distinct: DistinctStep, T.DropCollection: DropCollectionStep, T.DropColumn: DropColumnStep, T.DropRelation: DropRelationStep, T.DropRelationship: DropRelationshipStep, T.DropTag: DropTagStep, T.DropTask: DropTaskStep, T.DropTrigger: DropTriggerStep, T.DropView: DropViewStep, T.DropWorkspace: DropWorkspaceStep, T.Except: ExceptStep, T.Exit: ExitStep, T.Explain: ExplainStep, T.Filter: FilterStep, T.FramedWindow: FramedWindowStep, T.FunctionDataset: FunctionDatasetStep, T.GrantAccess: GrantAccessStep, T.HeapSort: HeapSortStep, T.Insert: InsertStep, T.Intersect: IntersectStep, T.Join: JoinStep, T.Limit: LimitStep, T.Listen: ListenStep, T.MaterializedCteRef: MaterializedCteRefStep, T.Merge: MergeStep, T.Order: OrderStep, T.Project: ProjectStep, T.RenameColumn: RenameColumnStep, T.RenameRelation: RenameRelationStep, T.ResyncRelation: ResyncRelationStep, T.RevokeAccess: RevokeAccessStep, T.RollbackRelation: RollbackRelationStep, T.ScalarSubqueryGuard: ScalarSubqueryGuardStep, T.Scan: ScanStep, T.Set: SetStep, T.Show: ShowStep, T.ShowColumns: ShowColumnsStep, T.ShowEffectiveGrantsOn: ShowEffectiveGrantsOnStep, T.ShowGrantsOn: ShowGrantsOnStep, T.ShowLineage: ShowLineageStep, T.ShowManifest: ShowManifestStep, T.ShowSnapshots: ShowSnapshotsStep, T.ShowSources: ShowSourcesStep, T.Subquery: SubqueryStep, T.TruncateRelation: TruncateRelationStep, T.Union: UnionStep, T.Unlisten: UnlistenStep, T.Unnest: UnnestStep, T.Window: WindowStep}
    return _STEP_CLASSES


# The step types that declare each field — for the planner sites that read one
# field across steps of any type. An unknown field name is a KeyError.
cdef dict _STEPS_WITH = None


cpdef frozenset steps_with(str field):
    global _STEPS_WITH
    if _STEPS_WITH is None:
        T = _step_types()
        _STEPS_WITH = {
            "action": frozenset({T.Analyze}),
            "aggregates": frozenset({T.Aggregate, T.AggregateAndGroup, T.Window}),
            "alias": frozenset({T.Distinct, T.Filter, T.FunctionDataset, T.Join, T.Limit, T.MaterializedCteRef, T.Order, T.Project, T.Scan, T.Subquery, T.Union, T.Unnest}),
            "all_relations": frozenset({T.AddColumn, T.AddRelationship, T.Aggregate, T.AggregateAndGroup, T.AlterColumnType, T.AlterMaterializedViewOwner, T.AlterMaterializedViewSuspended, T.AlterRelation, T.AlterTask, T.AlterTriggerMinimumInterval, T.AlterTriggerOwner, T.AlterTriggerSuspended, T.AlterView, T.AlterWorkspace, T.AlterWorkspaceSecure, T.Analyze, T.CallProcedure, T.CloneCollection, T.CloneRelation, T.Comment, T.CompactionCommit, T.CreateCollection, T.CreateRelation, T.CreateTag, T.CreateTask, T.CreateTrigger, T.CreateView, T.DetachRelation, T.Distinct, T.DropCollection, T.DropColumn, T.DropRelation, T.DropRelationship, T.DropTag, T.DropTask, T.DropTrigger, T.DropView, T.DropWorkspace, T.Except, T.Exit, T.Explain, T.Filter, T.FramedWindow, T.FunctionDataset, T.GrantAccess, T.HeapSort, T.Insert, T.Intersect, T.Join, T.Limit, T.Listen, T.MaterializedCteRef, T.Merge, T.Order, T.Project, T.RenameColumn, T.RenameRelation, T.ResyncRelation, T.RevokeAccess, T.RollbackRelation, T.ScalarSubqueryGuard, T.Scan, T.Set, T.Show, T.ShowColumns, T.ShowEffectiveGrantsOn, T.ShowGrantsOn, T.ShowLineage, T.ShowManifest, T.ShowSnapshots, T.ShowSources, T.Subquery, T.TruncateRelation, T.Union, T.Unlisten, T.Unnest, T.Window}),
            "analyze": frozenset({T.Explain}),
            "analyze_columns": frozenset({T.Analyze}),
            "args": frozenset({T.FunctionDataset}),
            "arguments": frozenset({T.CallProcedure}),
            "asof_condition": frozenset({T.Join}),
            "asof_left_column": frozenset({T.Join}),
            "asof_op": frozenset({T.Join}),
            "asof_right_column": frozenset({T.Join}),
            "at_date": frozenset({T.Scan}),
            "band_column": frozenset({T.Join}),
            "band_column_name": frozenset({T.Join}),
            "band_lower": frozenset({T.Join}),
            "band_lower_closed": frozenset({T.Join}),
            "band_upper": frozenset({T.Join}),
            "band_upper_closed": frozenset({T.Join}),
            "baseline_snapshot_id": frozenset({T.CompactionCommit}),
            "cardinality": frozenset({T.AddRelationship}),
            "cluster_columns": frozenset({T.AlterRelation}),
            "collection_name": frozenset({T.CloneCollection, T.CreateCollection}),
            "collection_names": frozenset({T.DropCollection}),
            "column_aliases": frozenset({T.FunctionDataset}),
            "column_if_exists": frozenset({T.DropColumn}),
            "column_mapping": frozenset({T.Insert}),
            "column_name": frozenset({T.AddColumn, T.AddRelationship, T.AlterColumnType, T.DropColumn, T.RenameColumn}),
            "column_type": frozenset({T.AddColumn}),
            "columns": frozenset({T.AddColumn, T.AddRelationship, T.Aggregate, T.AggregateAndGroup, T.AlterColumnType, T.AlterMaterializedViewOwner, T.AlterMaterializedViewSuspended, T.AlterRelation, T.AlterTask, T.AlterTriggerMinimumInterval, T.AlterTriggerOwner, T.AlterTriggerSuspended, T.AlterView, T.AlterWorkspace, T.AlterWorkspaceSecure, T.Analyze, T.CallProcedure, T.CloneCollection, T.CloneRelation, T.Comment, T.CompactionCommit, T.CreateCollection, T.CreateRelation, T.CreateTag, T.CreateTask, T.CreateTrigger, T.CreateView, T.DetachRelation, T.Distinct, T.DropCollection, T.DropColumn, T.DropRelation, T.DropRelationship, T.DropTag, T.DropTask, T.DropTrigger, T.DropView, T.DropWorkspace, T.Except, T.Exit, T.Explain, T.Filter, T.FramedWindow, T.FunctionDataset, T.GrantAccess, T.HeapSort, T.Insert, T.Intersect, T.Join, T.Limit, T.Listen, T.MaterializedCteRef, T.Merge, T.Order, T.Project, T.RenameColumn, T.RenameRelation, T.ResyncRelation, T.RevokeAccess, T.RollbackRelation, T.ScalarSubqueryGuard, T.Scan, T.Set, T.Show, T.ShowColumns, T.ShowEffectiveGrantsOn, T.ShowGrantsOn, T.ShowLineage, T.ShowManifest, T.ShowSnapshots, T.ShowSources, T.Subquery, T.TruncateRelation, T.Union, T.Unlisten, T.Unnest, T.Window}),
            "comment": frozenset({T.Comment}),
            "condition": frozenset({T.Filter}),
            "connector": frozenset({T.AddColumn, T.AddRelationship, T.AlterColumnType, T.AlterMaterializedViewOwner, T.AlterMaterializedViewSuspended, T.AlterRelation, T.AlterTask, T.AlterTriggerMinimumInterval, T.AlterTriggerOwner, T.AlterTriggerSuspended, T.AlterView, T.AlterWorkspace, T.AlterWorkspaceSecure, T.Analyze, T.CloneCollection, T.Comment, T.CompactionCommit, T.CreateCollection, T.CreateRelation, T.CreateTask, T.CreateTrigger, T.CreateView, T.DropColumn, T.DropRelationship, T.DropTask, T.DropTrigger, T.DropWorkspace, T.FunctionDataset, T.Insert, T.Listen, T.Merge, T.RenameColumn, T.RenameRelation, T.Scan, T.Show, T.TruncateRelation, T.Unlisten}),
            "connectors": frozenset({T.DropCollection, T.DropRelation, T.DropView}),
            "constraint_if_exists": frozenset({T.DropRelationship}),
            "constraint_name": frozenset({T.AddRelationship, T.DropRelationship}),
            "create_target": frozenset({T.Insert}),
            "csv_fail_on_error": frozenset({T.FunctionDataset}),
            "csv_files": frozenset({T.FunctionDataset}),
            "csv_has_header_row": frozenset({T.FunctionDataset}),
            "csv_infer_sample_size": frozenset({T.FunctionDataset}),
            "csv_physical_by_identity": frozenset({T.FunctionDataset}),
            "csv_physical_columns": frozenset({T.FunctionDataset}),
            "csv_separator": frozenset({T.FunctionDataset}),
            "cte_column_map": frozenset({T.MaterializedCteRef}),
            "cte_key": frozenset({T.MaterializedCteRef}),
            "cte_name": frozenset({T.MaterializedCteRef}),
            "current_column_type": frozenset({T.AlterColumnType}),
            "dataset": frozenset({T.FunctionDataset}),
            "dataset_committed_at": frozenset({T.Scan}),
            "deep_restore_target": frozenset({T.Filter}),
            "default": frozenset({T.AddColumn}),
            "defining_query": frozenset({T.Insert}),
            "distinct_target": frozenset({T.Unnest}),
            "effective": frozenset({T.ShowEffectiveGrantsOn, T.ShowGrantsOn}),
            "emit_row_identity": frozenset({T.Scan}),
            "end_date": frozenset({T.Scan}),
            "estimated_row_count": frozenset({T.Project}),
            "event_kind": frozenset({T.CreateTrigger}),
            "except_columns": frozenset({T.Project}),
            "executing_task": frozenset({T.Insert}),
            "execution_context": frozenset({T.AlterWorkspace, T.GrantAccess, T.Listen, T.RevokeAccess, T.ShowEffectiveGrantsOn, T.ShowGrantsOn, T.Unlisten}),
            "existence_column": frozenset({T.Join}),
            "existence_three_valued": frozenset({T.Join}),
            "explicit_columns": frozenset({T.Insert}),
            "extended": frozenset({T.ShowColumns}),
            "file_paths": frozenset({T.Merge}),
            "filter_conditions": frozenset({T.Unnest}),
            "for_manifest_only": frozenset({T.Scan}),
            "for_snapshots_only": frozenset({T.Scan}),
            "force": frozenset({T.ResyncRelation}),
            "format": frozenset({T.Explain}),
            "from_join_on": frozenset({T.Filter}),
            "full": frozenset({T.ShowColumns}),
            "function": frozenset({T.FunctionDataset}),
            "grouping_set_identities": frozenset({T.AggregateAndGroup}),
            "grouping_sets": frozenset({T.AggregateAndGroup}),
            "groups": frozenset({T.Aggregate, T.AggregateAndGroup}),
            "having_condition": frozenset({T.AggregateAndGroup}),
            "hidden_columns": frozenset({T.Exit, T.Project}),
            "hint_settings": frozenset({T.MaterializedCteRef, T.Scan, T.Subquery}),
            "hints": frozenset({T.FunctionDataset, T.MaterializedCteRef, T.Scan, T.Subquery}),
            "history_view": frozenset({T.Scan, T.ShowLineage, T.ShowSnapshots, T.ShowSources}),
            "hoisted_columns": frozenset({T.Project}),
            "if_exists": frozenset({T.AddColumn, T.AddRelationship, T.AlterColumnType, T.AlterRelation, T.Comment, T.CreateTag, T.DropCollection, T.DropColumn, T.DropRelation, T.DropRelationship, T.DropTag, T.DropTask, T.DropTrigger, T.DropView, T.DropWorkspace, T.RenameColumn, T.RenameRelation, T.RollbackRelation, T.TruncateRelation}),
            "if_not_exists": frozenset({T.AddColumn, T.CloneRelation, T.CreateCollection, T.CreateRelation, T.CreateTask, T.CreateTrigger, T.CreateView, T.Insert}),
            "implied_join": frozenset({T.Join}),
            "internal_relation": frozenset({T.Scan}),
            "is_materialized_view": frozenset({T.DropRelation, T.Insert}),
            "is_noop": frozenset({T.Insert}),
            "is_refresh": frozenset({T.Insert}),
            "is_replace": frozenset({T.Insert}),
            "is_window_join": frozenset({T.Join}),
            "jsonl_fail_on_error": frozenset({T.FunctionDataset}),
            "jsonl_files": frozenset({T.FunctionDataset}),
            "jsonl_infer_sample_size": frozenset({T.FunctionDataset}),
            "jsonl_infer_schema": frozenset({T.FunctionDataset}),
            "jsonl_physical_by_identity": frozenset({T.FunctionDataset}),
            "jsonl_physical_columns": frozenset({T.FunctionDataset}),
            "left_column": frozenset({T.Join}),
            "left_columns": frozenset({T.Join}),
            "left_readers": frozenset({T.Join}),
            "left_relation_names": frozenset({T.Except, T.Intersect, T.Join, T.Union}),
            "length_only_columns": frozenset({T.Scan}),
            "limit": frozenset({T.HeapSort, T.Limit, T.Scan}),
            "lineage": frozenset({T.ShowLineage}),
            "manifest": frozenset({T.FunctionDataset, T.Scan, T.ShowManifest}),
            "minimum_interval_seconds": frozenset({T.AlterTriggerMinimumInterval}),
            "modifier": frozenset({T.Except, T.Intersect, T.Union}),
            "named_args": frozenset({T.FunctionDataset}),
            "new_column_name": frozenset({T.RenameColumn}),
            "new_column_type": frozenset({T.AlterColumnType}),
            "new_owner": frozenset({T.AlterMaterializedViewOwner, T.AlterTriggerOwner}),
            "new_relation_name": frozenset({T.RenameRelation}),
            "nullable": frozenset({T.AddColumn}),
            "object_kind": frozenset({T.GrantAccess, T.Listen, T.RevokeAccess, T.ShowEffectiveGrantsOn, T.ShowGrantsOn, T.Unlisten}),
            "object_name": frozenset({T.Comment, T.GrantAccess, T.RevokeAccess, T.Show, T.ShowEffectiveGrantsOn, T.ShowGrantsOn}),
            "object_type": frozenset({T.Comment, T.Show}),
            "offset": frozenset({T.Limit}),
            "on": frozenset({T.Distinct, T.Join}),
            "on_table": frozenset({T.CreateTask}),
            "operation": frozenset({T.Merge}),
            "or_replace": frozenset({T.CreateTask, T.CreateTrigger, T.CreateView, T.Insert}),
            "order_by": frozenset({T.FramedWindow, T.HeapSort, T.Order, T.Window}),
            "outcome": frozenset({T.Listen}),
            "output_relation": frozenset({T.FramedWindow, T.Window}),
            "outputs": frozenset({T.FramedWindow, T.Window}),
            "owner_is_current_user": frozenset({T.AlterMaterializedViewOwner, T.AlterTriggerOwner}),
            "partition_by": frozenset({T.FramedWindow, T.Window}),
            "passthrough_columns": frozenset({T.Project}),
            "pattern": frozenset({T.GrantAccess, T.RevokeAccess, T.ShowEffectiveGrantsOn, T.ShowGrantsOn}),
            "pending_cte_key": frozenset({T.Scan}),
            "pre_inline_columns": frozenset({T.Filter}),
            "pre_inline_condition": frozenset({T.Filter}),
            "pre_inline_relations": frozenset({T.Filter}),
            "pre_update_columns": frozenset({T.AddColumn, T.AddRelationship, T.Aggregate, T.AggregateAndGroup, T.AlterColumnType, T.AlterMaterializedViewOwner, T.AlterMaterializedViewSuspended, T.AlterRelation, T.AlterTask, T.AlterTriggerMinimumInterval, T.AlterTriggerOwner, T.AlterTriggerSuspended, T.AlterView, T.AlterWorkspace, T.AlterWorkspaceSecure, T.Analyze, T.CallProcedure, T.CloneCollection, T.CloneRelation, T.Comment, T.CompactionCommit, T.CreateCollection, T.CreateRelation, T.CreateTag, T.CreateTask, T.CreateTrigger, T.CreateView, T.DetachRelation, T.Distinct, T.DropCollection, T.DropColumn, T.DropRelation, T.DropRelationship, T.DropTag, T.DropTask, T.DropTrigger, T.DropView, T.DropWorkspace, T.Except, T.Exit, T.Explain, T.Filter, T.FramedWindow, T.FunctionDataset, T.GrantAccess, T.HeapSort, T.Insert, T.Intersect, T.Join, T.Limit, T.Listen, T.MaterializedCteRef, T.Merge, T.Order, T.Project, T.RenameColumn, T.RenameRelation, T.ResyncRelation, T.RevokeAccess, T.RollbackRelation, T.ScalarSubqueryGuard, T.Scan, T.Set, T.Show, T.ShowColumns, T.ShowEffectiveGrantsOn, T.ShowGrantsOn, T.ShowLineage, T.ShowManifest, T.ShowSnapshots, T.ShowSources, T.Subquery, T.TruncateRelation, T.Union, T.Unlisten, T.Unnest, T.Window}),
            "predicates": frozenset({T.FunctionDataset, T.Scan}),
            "principal": frozenset({T.GrantAccess, T.RevokeAccess}),
            "procedure_name": frozenset({T.CallProcedure}),
            "produced_by": frozenset({T.Insert, T.Merge}),
            "projection": frozenset({T.Aggregate, T.AggregateAndGroup}),
            "property_name": frozenset({T.AlterWorkspace}),
            "property_value": frozenset({T.AlterWorkspace}),
            "pushed_aggregates": frozenset({T.Scan}),
            "pushed_distinct": frozenset({T.Scan}),
            "pushed_groups": frozenset({T.Scan}),
            "query": frozenset({T.AlterView, T.CreateView}),
            "read_sources": frozenset({T.Insert, T.Merge}),
            "reducer_applied": frozenset({T.Join}),
            "references_column_name": frozenset({T.AddRelationship}),
            "references_relation_name": frozenset({T.AddRelationship}),
            "references_relation_parts": frozenset({T.AddRelationship}),
            "relation": frozenset({T.FunctionDataset, T.MaterializedCteRef, T.Scan, T.ShowColumns, T.ShowLineage, T.ShowManifest, T.ShowSnapshots, T.ShowSources, T.Subquery}),
            "relation_name": frozenset({T.AddColumn, T.AddRelationship, T.AlterColumnType, T.AlterMaterializedViewOwner, T.AlterMaterializedViewSuspended, T.AlterRelation, T.CloneRelation, T.CompactionCommit, T.CreateRelation, T.CreateTag, T.DetachRelation, T.DropColumn, T.DropRelationship, T.DropTag, T.Exit, T.FunctionDataset, T.Insert, T.Merge, T.RenameColumn, T.RenameRelation, T.ResyncRelation, T.RollbackRelation, T.TruncateRelation}),
            "relation_names": frozenset({T.DropRelation, T.Join}),
            "relation_parts": frozenset({T.AddRelationship, T.DropRelationship}),
            "relations": frozenset({T.Filter}),
            "relationships": frozenset({T.CreateRelation}),
            "residual": frozenset({T.Join}),
            "resolved_dataset": frozenset({T.Scan}),
            "resolved_owner": frozenset({T.AlterTriggerOwner}),
            "retired_files": frozenset({T.CompactionCommit}),
            "right_column": frozenset({T.Join}),
            "right_columns": frozenset({T.Join}),
            "right_readers": frozenset({T.Join}),
            "right_relation_names": frozenset({T.Except, T.Intersect, T.Join, T.Union}),
            "role": frozenset({T.GrantAccess, T.RevokeAccess}),
            "row_identity_statement": frozenset({T.Scan}),
            "schedule": frozenset({T.CreateTrigger}),
            "schema": frozenset({T.Aggregate, T.AggregateAndGroup, T.CreateRelation, T.FunctionDataset, T.MaterializedCteRef, T.Project, T.Scan, T.ShowColumns, T.ShowLineage, T.ShowManifest, T.ShowSnapshots, T.ShowSources, T.Subquery}),
            "schemas": frozenset({T.Join}),
            "secure_destinations": frozenset({T.AlterWorkspaceSecure}),
            "secure_object": frozenset({T.AlterWorkspaceSecure}),
            "series_column": frozenset({T.FunctionDataset}),
            "setop_leg_columns": frozenset({T.Join}),
            "snapshots": frozenset({T.ShowSnapshots}),
            "sorted_by": frozenset({T.CompactionCommit}),
            "source": frozenset({T.Scan}),
            "source_collection": frozenset({T.CloneCollection}),
            "source_relation": frozenset({T.CloneRelation}),
            "source_relations": frozenset({T.Subquery}),
            "source_tables": frozenset({T.AlterTask, T.CreateTask, T.Insert}),
            "source_tail_id": frozenset({T.CompactionCommit, T.Insert, T.Merge}),
            "sources": frozenset({T.Filter, T.Project, T.ShowSources, T.Union}),
            "start_date": frozenset({T.Scan}),
            "statement": frozenset({T.AlterTask, T.CreateTask}),
            "statement_name": frozenset({T.Merge}),
            "suspended": frozenset({T.AlterMaterializedViewSuspended, T.AlterTriggerSuspended}),
            "swap_build_side": frozenset({T.Join}),
            "table_name": frozenset({T.AlterTriggerMinimumInterval, T.AlterTriggerOwner, T.AlterTriggerSuspended, T.Analyze, T.CreateTrigger, T.DropTrigger}),
            "tag_name": frozenset({T.CreateTag, T.DropTag}),
            "target_alias": frozenset({T.Merge}),
            "target_column_names": frozenset({T.Insert, T.Merge}),
            "target_schema": frozenset({T.Insert, T.Merge}),
            "target_tables": frozenset({T.AlterTask, T.CreateTask}),
            "task_name": frozenset({T.AlterTask, T.CreateTask, T.CreateTrigger, T.DropTask, T.Listen, T.Unlisten}),
            "time_zone": frozenset({T.CreateTrigger}),
            "top_k": frozenset({T.Window}),
            "topn_descending": frozenset({T.Scan}),
            "topn_limit": frozenset({T.Scan}),
            "topn_order_by": frozenset({T.Scan}),
            "topn_sort_identity": frozenset({T.Scan}),
            "topn_sort_name": frozenset({T.Scan}),
            "trigger_name": frozenset({T.AlterTriggerMinimumInterval, T.AlterTriggerOwner, T.AlterTriggerSuspended, T.CreateTrigger, T.DropTrigger, T.Show}),
            "type": frozenset({T.Join, T.Unnest}),
            "unnest_alias": frozenset({T.Unnest}),
            "unnest_column": frozenset({T.Unnest}),
            "unnest_function": frozenset({T.Unnest}),
            "unnest_target": frozenset({T.FunctionDataset, T.Unnest}),
            "unpruned_columns": frozenset({T.Scan, T.Subquery}),
            "using": frozenset({T.Join}),
            "using_merged": frozenset({T.Join}),
            "value": frozenset({T.Set}),
            "values": frozenset({T.FunctionDataset}),
            "values_feeder": frozenset({T.Insert}),
            "variable": frozenset({T.Set}),
            "variables": frozenset({T.Set}),
            "vector_topk_candidate": frozenset({T.HeapSort}),
            "version": frozenset({T.Scan}),
            "version_spec": frozenset({T.CreateTag, T.RollbackRelation}),
            "version_tag": frozenset({T.Scan}),
            "via_view": frozenset({T.Scan}),
            "view_name": frozenset({T.AlterView, T.CreateView}),
            "view_names": frozenset({T.DropView}),
            "view_schema": frozenset({T.AlterView, T.CreateView}),
            "view_sql": frozenset({T.AlterView, T.CreateView}),
            "window_functions": frozenset({T.FramedWindow, T.Window}),
            "window_source": frozenset({T.CreateTrigger}),
            "workspace_name": frozenset({T.AlterWorkspace, T.AlterWorkspaceSecure, T.DropWorkspace}),
            "write_coalesce_rows": frozenset({T.Insert}),
        }
    return _STEPS_WITH[field]
