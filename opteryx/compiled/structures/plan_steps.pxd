# cython: language_level=3
# distutils: language = c++

# The plan step base and its native row (src/cpp/planner/step_row.hpp) - declared
# here so native readers (the statistics refresh) reach a step's row in C.

from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport uint32_t
from libcpp cimport bool as cbool
from libcpp.string cimport string
from libcpp.vector cimport vector

from opteryx.compiled.structures.expressions cimport ExprTable


cdef extern from "planner/step_row.hpp" namespace "opteryx::planner":
    cdef int64_t kNoStepValue

    # the C++ NativeManifest, opaque here: a row only carries a borrowed pointer
    cdef cppclass StepManifest "opteryx::planner::NativeManifest":
        pass

    cdef cppclass StepKey:
        int64_t expr
        string identity

    cdef cppclass StepRow:
        int32_t kind
        const ExprTable* arena
        cbool arena_conflict
        vector[int64_t] columns
        string relation
        string alias
        vector[int64_t] predicates
        cbool has_schema
        vector[uint32_t] schema_slots
        int64_t schema_row_count_metric
        int64_t schema_row_count_estimate
        const StepManifest* manifest
        vector[uint32_t] manifest_schema_slots
        int64_t limit
        cbool has_pushed_aggregates
        vector[StepKey] pushed_groups
        cbool pushed_distinct
        int64_t condition
        string join_type
        cbool has_join_type
        vector[StepKey] left_keys
        vector[StepKey] right_keys
        vector[StepKey] groups
        int64_t offset
        string cte_key


cdef class PlanStep:
    cdef readonly object node_type
    cdef public str uuid
    cdef readonly unsigned long long write_count
    cdef tuple _columns
    cdef set _all_relations
    cdef set _pre_update_columns
    cdef StepRow* _row

    cdef void _init_common(self, object columns, object all_relations, object pre_update_columns, object uuid)
    cdef void _sync_row(self) except *
    cdef void _check_row_arena(self) except *
    cdef void _resync_on_arena_conflict(self) except *
    cpdef tuple expressions(self, bint include_columns=*)
    cpdef map_expressions(self, object fn)
    cpdef dict field_values(self)
    cdef dict _common_values(self)
    cpdef PlanStep copy(self, dict memo=*)
    cdef PlanStep _shallow_copy(self)
    cdef void _copy_common_into(self, PlanStep target, dict memo)
    cdef void _share_common_into(self, PlanStep target)
