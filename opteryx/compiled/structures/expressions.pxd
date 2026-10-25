# cython: language_level=3
# distutils: language = c++

# The query's expression arena (src/cpp/planner/expr_arena.hpp) - declared here
# so native consumers (the native manifest's pruning) read the rows in C.

from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport uint16_t
from libc.stdint cimport uint32_t
from libcpp cimport bool as cbool
from libcpp.string cimport string
from libcpp.utility cimport pair
from libcpp.vector cimport vector


cdef extern from "planner/expr_arena.hpp" namespace "opteryx::planner":
    cdef enum LiteralTag:
        LITERAL_NONE
        LITERAL_NULL
        LITERAL_BOOL
        LITERAL_INT64
        LITERAL_UINT64
        LITERAL_DOUBLE
        LITERAL_BYTES
        LITERAL_DECIMAL
        LITERAL_INTERVAL
        LITERAL_ITEMS

    cdef cppclass LiteralValue:
        LiteralTag tag
        int64_t i
        int64_t j
        int32_t k
        double d
        string bytes
        vector[LiteralValue] items

    cdef enum ExprFlag:
        FLAG_DRAFT
        FLAG_DO_NOT_CREATE_COLUMN
        FLAG_NEGATED
        FLAG_OUTER_REFERENCE
        FLAG_WILDCARD_ORDER_POSITION
        FLAG_RLIKE_COMPILED
        FLAG_LOWER_INCLUSIVE
        FLAG_UPPER_INCLUSIVE
        FLAG_HAS_ALIAS
        FLAG_HAS_QUERY_COLUMN
        FLAG_HAS_SPAN
        FLAG_HAS_LIKE_DECAY
        FLAG_HAS_MATCH_THRESHOLD
        FLAG_HAS_LIMIT

    cdef cppclass ExprRow:
        int64_t origin
        int32_t kind
        uint16_t flags
        string value
        string alias
        string query_column
        uint32_t column_slot
        vector[uint32_t] relations
        int64_t left
        int64_t right
        int64_t centre
        int64_t else_result
        int64_t format
        vector[int64_t] parameters
        vector[int64_t] conditions
        vector[int64_t] results
        vector[pair[int64_t, cbool]] order
        string source
        string source_column
        string outer_relation
        string qualified_name
        string duplicate_treatment
        string null_treatment
        int64_t limit
        double like_selectivity_decay
        double match_threshold
        int32_t span[4]
        uint32_t type_id
        LiteralValue literal

    cdef cppclass ExprTable:
        int64_t add()
        ExprRow& row(int64_t expr_id) except +
        size_t size()
        void note_kind(int32_t kind) except +
        cbool has_kind(int32_t kind)
        cbool any_value(int32_t kind, const vector[string]& values)
        uint32_t relation(const string& name)
        const string& relation_name(uint32_t relation_id)

    cdef uint32_t kNoColumnSlot
    cdef uint32_t kNoTypeId

    cdef cppclass NodeKinds:
        int32_t and_ "and_"
        int32_t or_ "or_"
        int32_t xor_ "xor_"
        int32_t not_ "not_"
        int32_t dnf
        int32_t cnf
        int32_t case_ "case_"
        int32_t comparison
        int32_t binary
        int32_t unary
        int32_t function
        int32_t identifier
        int32_t nested
        int32_t aggregator
        int32_t literal
        int32_t cast
        int32_t extraction
        int32_t between


# opteryx.expression.NodeType's values as native code reads them (loaded once).
cdef const NodeKinds* node_kinds() except NULL


cdef class ExprArena:
    cdef ExprTable* _table
    cdef bint _sealed
    cdef object _columns   # the query's ColumnTable: every row's column_slot is a slot of it

    cdef inline int64_t _mint(self):
        return self._table.add()


# The expression classes, declared here so compiled planner code (the binder core)
# reads their fields in C (architect ruling D2, 2026-10-25). Fields are READ
# directly; every write goes through the class's own setters, which keep the
# expression's arena row in step and refuse writes to a sealed expression.

cdef class Expression:
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

    cdef void _bind_arena(self, ExprArena arena, object origin)
    cdef void _sync(self) except *
    cdef inline void _check_writable(self):
        if self._arena._sealed and not self._draft:
            from opteryx.exceptions import InvalidInternalStateError

            raise InvalidInternalStateError(
                f"{type(self).__name__} #{self.expr_id} is sealed: expressions are immutable "
                "once placed after binding - build a new one (replace / rewrite_children)"
            )
    cdef void _set_common(self, str alias, str query_column, object schema_column, object relations)
    cpdef tuple children(self)
    cpdef map_children(self, object fn)
    cdef void _copy_common_into(self, Expression target, dict memo)
    cpdef Expression copy(self, dict memo=*)
    cdef Expression _shallow_copy(self)
    cdef void _share_common_into(self, Expression target)


cdef class LogicalColumn(Expression):
    cdef str _source_column
    cdef str _source
    cdef bint _is_outer_reference
    cdef object _outer_relation
    cdef tuple _span

    cdef void _sync(self) except *
    cdef Expression _shallow_copy(self)
    cpdef Expression copy(self, dict memo=*)


cdef class Literal(Expression):
    cdef object _value
    cdef object _type
    cdef bint _is_wildcard_order_position
    cdef bint _rlike_compiled

    cpdef tuple children(self)
    cpdef map_children(self, object fn)
    cdef void _sync(self) except *
    cdef Expression _shallow_copy(self)
    cpdef Expression copy(self, dict memo=*)


cdef class Comparison(Expression):
    cdef str _value
    cdef object _left
    cdef object _right
    cdef bint _negated
    cdef object _like_selectivity_decay

    cpdef tuple children(self)
    cpdef map_children(self, object fn)
    cdef void _sync(self) except *
    cdef Expression _shallow_copy(self)
    cpdef Expression copy(self, dict memo=*)


cdef class BinaryOperator(Expression):
    cdef str _value
    cdef object _left
    cdef object _right

    cpdef tuple children(self)
    cpdef map_children(self, object fn)
    cdef void _sync(self) except *
    cdef Expression _shallow_copy(self)
    cpdef Expression copy(self, dict memo=*)


cdef class UnaryOperator(Expression):
    cdef str _value
    cdef object _centre
    cdef tuple _parameters
    cdef bint _negated

    cpdef tuple children(self)
    cpdef map_children(self, object fn)
    cdef void _sync(self) except *
    cdef Expression _shallow_copy(self)
    cpdef Expression copy(self, dict memo=*)


cdef class ExtractionOperator(Expression):
    cdef str _value
    cdef object _left
    cdef object _right

    cpdef tuple children(self)
    cpdef map_children(self, object fn)
    cdef void _sync(self) except *
    cdef Expression _shallow_copy(self)
    cpdef Expression copy(self, dict memo=*)


cdef class And(Expression):
    cdef str _value
    cdef object _left
    cdef object _right

    cpdef tuple children(self)
    cpdef map_children(self, object fn)
    cdef void _sync(self) except *
    cdef Expression _shallow_copy(self)
    cpdef Expression copy(self, dict memo=*)


cdef class Or(Expression):
    cdef str _value
    cdef object _left
    cdef object _right

    cpdef tuple children(self)
    cpdef map_children(self, object fn)
    cdef void _sync(self) except *
    cdef Expression _shallow_copy(self)
    cpdef Expression copy(self, dict memo=*)


cdef class Xor(Expression):
    cdef str _value
    cdef object _left
    cdef object _right

    cpdef tuple children(self)
    cpdef map_children(self, object fn)
    cdef void _sync(self) except *
    cdef Expression _shallow_copy(self)
    cpdef Expression copy(self, dict memo=*)


cdef class Not(Expression):
    cdef object _centre

    cpdef tuple children(self)
    cpdef map_children(self, object fn)
    cdef void _sync(self) except *
    cdef Expression _shallow_copy(self)
    cpdef Expression copy(self, dict memo=*)


cdef class Dnf(Expression):
    cdef tuple _parameters

    cpdef tuple children(self)
    cpdef map_children(self, object fn)
    cdef void _sync(self) except *
    cdef Expression _shallow_copy(self)
    cpdef Expression copy(self, dict memo=*)


cdef class Cnf(Expression):
    cdef tuple _parameters

    cpdef tuple children(self)
    cpdef map_children(self, object fn)
    cdef void _sync(self) except *
    cdef Expression _shallow_copy(self)
    cpdef Expression copy(self, dict memo=*)


cdef class Between(Expression):
    cdef tuple _value
    cdef object _left
    cdef object _right
    cdef object _centre

    cpdef tuple children(self)
    cpdef map_children(self, object fn)
    cdef void _sync(self) except *
    cdef Expression _shallow_copy(self)
    cpdef Expression copy(self, dict memo=*)


cdef class Case(Expression):
    cdef tuple _conditions
    cdef tuple _results
    cdef object _else_result

    cpdef tuple children(self)
    cpdef map_children(self, object fn)
    cdef void _sync(self) except *
    cdef Expression _shallow_copy(self)
    cpdef Expression copy(self, dict memo=*)


cdef class Cast(Expression):
    cdef str _value
    cdef object _left
    cdef tuple _parameters
    cdef object _format

    cpdef tuple children(self)
    cpdef map_children(self, object fn)
    cdef void _sync(self) except *
    cdef Expression _shallow_copy(self)
    cpdef Expression copy(self, dict memo=*)


cdef class Function(Expression):
    cdef str _value
    cdef tuple _parameters
    cdef object _function_ref
    cdef str _qualified_name
    cdef tuple _span
    cdef object _match_threshold

    cpdef tuple children(self)
    cpdef map_children(self, object fn)
    cdef void _sync(self) except *
    cdef Expression _shallow_copy(self)
    cpdef Expression copy(self, dict memo=*)


cdef class Aggregator(Expression):
    cdef str _value
    cdef tuple _parameters
    cdef tuple _order
    cdef str _qualified_name
    cdef tuple _span
    cdef str _duplicate_treatment
    cdef object _limit
    cdef str _null_treatment
    cdef dict _over

    cpdef tuple children(self)
    cpdef map_children(self, object fn)
    cdef void _sync(self) except *
    cdef Expression _shallow_copy(self)
    cpdef Expression copy(self, dict memo=*)


cdef class Nested(Expression):
    cdef object _centre

    cpdef tuple children(self)
    cpdef map_children(self, object fn)
    cdef void _sync(self) except *
    cdef Expression _shallow_copy(self)
    cpdef Expression copy(self, dict memo=*)


cdef class Subquery(Expression):
    cdef object _value

    cpdef tuple children(self)
    cpdef map_children(self, object fn)
    cdef void _sync(self) except *
    cdef Expression _shallow_copy(self)
    cpdef Expression copy(self, dict memo=*)


cdef class Wildcard(Expression):
    cdef object _value
    cdef object _except_columns

    cpdef tuple children(self)
    cpdef map_children(self, object fn)
    cdef void _sync(self) except *
    cdef Expression _shallow_copy(self)
    cpdef Expression copy(self, dict memo=*)


cdef class Evaluated(Expression):
    cdef object _value

    cpdef tuple children(self)
    cpdef map_children(self, object fn)
    cdef void _sync(self) except *
    cdef Expression _shallow_copy(self)
    cpdef Expression copy(self, dict memo=*)
