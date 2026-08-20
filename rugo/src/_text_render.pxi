
# distutils: language = c++
#
# Shared per-column render-descriptor plumbing for the native text writers
# (jsonl/_jsonl_writer.pxi, csv/_csv_writer.pxi).
#
# Both writers hand C++ ONE descriptor per column rather than a fan of parallel
# int arrays, so this is the single place the nanobind logical-type properties
# are read and packed. Declared here (included ahead of both writers) because
# duplicating the packing in each writer is how the two drift apart.

from libc.stdint cimport uint8_t
from libcpp.vector cimport vector

from draken.core.buffers cimport DrakenVector, DRAKEN_ARRAY
from draken.vectors.vector cimport Vector

# Draken's logical-type vocabulary — imported, never copied (see CLAUDE.md §14).
cdef extern from "logical_type.h":
    cdef enum class LogicalKind(uint8_t):
        NONE
        TIMESTAMP
        TIME
        DECIMAL
        VECTOR
        IPV4

cdef extern from "interop/value_format.hpp" namespace "rugo_text":
    cdef struct LogicalDesc:
        LogicalKind kind
        int unit
        int scale
        int dim

    # One nesting level below a column's own ARRAY-ness — see ColumnDesc and
    # render_json_value in value_format.hpp for how these chain for
    # ARRAY<ARRAY<...>>.
    cdef struct ArrayLevel:
        const DrakenVector* vec
        LogicalDesc desc

    cdef struct ColumnDesc:
        LogicalDesc column
        vector[ArrayLevel] levels


cdef inline int _text_unit_code(object u):
    # Temporal unit -> renderer code (s=0, ms=1, us=2, ns=3).
    if u == "s": return 0
    if u == "ms": return 1
    if u == "ns": return 3
    return 2  # us / default


cdef inline void _fill_logical_desc(LogicalDesc* d, object nb) except *:
    """Pack one nanobind Vector handle's logical type into `d`.

    A vector with no descriptor leaves `d` at the caller's zero fill — kind
    NONE with no parameters — which is exactly what an unparameterized column
    means. Every parameter is read from the kind that owns it, so a UINT32
    carrying IPV4 arrives with kind IPV4 and nothing else set.
    """
    cdef object kind = nb.logical_type_kind
    cdef object unit
    cdef object scale
    cdef object dim
    if kind is None:
        return
    d.kind = <LogicalKind><int>kind.value
    unit = nb.logical_type_unit
    if unit is not None:
        d.unit = _text_unit_code(unit)
    scale = nb.logical_type_scale
    if scale is not None:
        d.scale = <int>scale
    dim = nb.logical_type_dimension
    if dim is not None:
        d.dim = <int>dim


cdef inline void _fill_array_levels(ColumnDesc* desc, Vector col, list keepalive) except *:
    """Walk `col`'s ARRAY nesting chain into `desc.levels`, one entry per depth.

    `col` must already be known DRAKEN_ARRAY-typed. Descends through
    array_child/array_child_type to whatever depth the data actually has
    (ARRAY<ARRAY<T>> etc.) — mirrors row_array_to_pylist's descent through
    VectorOwner::child_owner in draken_native.cpp, the reference for correct
    nested-array handling (see CLAUDE.md and value_format.hpp::ColumnDesc).
    `keepalive` must outlive the render call: each level's Vector owns the
    nanobind handle the C++ side borrows a raw DrakenVector* out of.
    """
    cdef Vector cur = col
    cdef Vector cv
    cdef ArrayLevel lvl
    while cur.unified().type == DRAKEN_ARRAY and cur._nb.array_child_type is not None:
        cv = Vector(cur._nb.array_child)
        keepalive.append(cv)
        lvl.vec = cv.unified()
        lvl.desc.kind = LogicalKind.NONE
        lvl.desc.unit = 0
        lvl.desc.scale = 0
        lvl.desc.dim = 0
        _fill_logical_desc(&lvl.desc, cv._nb)
        desc.levels.push_back(lvl)
        cur = cv
