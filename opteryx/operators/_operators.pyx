# Umbrella compilation unit for all operator plan nodes.
# Individual .pyx files are kept in per-operator subdirectories for authoring clarity;
# all are compiled into this single extension module.
#
# Common cimports declared here are visible to all included files.

from opteryx.compiled.draken.morsels.morsel cimport Morsel
from opteryx.compiled.draken.vectors.bool_vector cimport BoolVector
from opteryx.compiled.draken.vectors.float64_vector cimport Float64Vector
from opteryx.compiled.draken.vectors.int64_vector cimport Int64Vector
from opteryx.compiled.draken.vectors.string_vector cimport StringVector
from opteryx.compiled.structures.carchar_set cimport CarcharSetWrapper
from opteryx.compiled.structures.buffers cimport IntBuffer, Int32Buffer
from opteryx.compiled.draken.interop.vector_sequence cimport vector_from_sequence
from cpython.array cimport array

# Include order: base classes / shared types before their consumers.

# ReaderNode is subclassed by function_dataset, parquet_read, show_value
include "read/read.pyx"

include "cross_join/cross_join.pyx"
include "distinct/distinct.pyx"
include "hashed_inner_join/hashed_inner_join.pyx"
include "exit/exit.pyx"
include "explain/explain.pyx"
include "filter_join/filter_join.pyx"
include "filter/filter.pyx"
include "function_dataset/function_dataset.pyx"
include "heap_sort/heap_sort.pyx"
include "limit/limit.pyx"
include "nested_loop_join/nested_loop_join.pyx"
include "non_equi_join/non_equi_join.pyx"
include "null_reader/null_reader.pyx"
include "outer_join/outer_join.pyx"
include "parquet_read/parquet_read.pyx"
include "projection/projection.pyx"
include "set_variable/set_variable.pyx"
include "show_columns/show_columns.pyx"
include "show_create/show_create.pyx"
include "show_value/show_value.pyx"
include "sort/sort.pyx"
include "table_management/table_management.pyx"
include "union/union.pyx"
include "unnest_join/unnest_join.pyx"
include "view_management/view_management.pyx"

# Aggregate: ungrouped engine first (aggregate_node uses its accumulator classes)
include "aggregate/ungrouped_agg.pyx"
include "aggregate/aggregate_node.pyx"

# Grouped aggregate (self-contained via .pxi includes inside _grouped_agg.pyx)
include "grouped_aggregate_hashed/_grouped_agg.pyx"
