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
