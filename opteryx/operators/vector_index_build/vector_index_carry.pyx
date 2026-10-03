# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Vector carry — compaction's index build without a model (docs/VECTOR_INDEX_DESIGN.md §5.6).

Two halves:

  RowOriginRecorder  rides the compaction writer (DataFileStream). Each row group it is
                     handed carries the scan's row identity (`$file`, `$ordinal`) under the
                     names OPTIMIZE gave them; it appends both to native arrays and returns
                     the row group WITHOUT them, so they are never written. One recorder per
                     output file: its arrays are that file's output-ordinal -> (input,
                     input ordinal) map. The ordinals never become Python objects.

  carry_vector_index_local / carry_vector_index_to_sessions
                     the entry points into src/cpp/engine/vector_index_carry.hpp: ONE native
                     call per compaction, GIL released, reading every input's vectors file
                     row group by row group and writing every output's index files.
"""

cdef extern from "engine/vector_index_carry.hpp" namespace "opteryx::engine" nogil:
    cdef cppclass CarryInput:
        string vectors
        uint64_t vectors_bytes
        string auth_header
        cppvector[uint32_t] deleted

    cdef cppclass CarryOutput:
        cppvector[uint32_t] src_file
        cppvector[uint32_t] src_ordinal

    cdef cppclass CarrySpec:
        cppvector[CarryInput] inputs
        uint32_t dims
        VibIvfParams ivf
        uint32_t flush_rows

    bint c_carry_local "opteryx::engine::carry_vector_index_local"(const CarrySpec& spec, const cppvector[CarryOutput]& outputs,
                                  const cppvector[string]& vectors, const cppvector[string]& centroids,
                                  cppvector[VectorIndexBuildResult]* results, string* err)
    bint c_carry_to_sessions "opteryx::engine::carry_vector_index_to_sessions"(const CarrySpec& spec, const cppvector[CarryOutput]& outputs,
                                        const cppvector[string]& sessions, size_t chunk_bytes,
                                        cppvector[VectorIndexBuildResult]* results, string* err)


cdef extern from * nogil:
    """
    #include <utility>
    template <typename T> static inline void vib_move_into(T& dst, T& src) { dst = std::move(src); }
    """
    void vib_move_into[T](T& dst, T& src)


cdef class RowOriginRecorder:
    """Where each row of ONE output file came from: input file index and input ordinal,
    in output order. See the module docstring."""

    cdef cppvector[uint32_t] files
    cdef cppvector[uint32_t] ordinals
    cdef str _file_column
    cdef str _ordinal_column

    def __init__(self, str file_column, str ordinal_column):
        self._file_column = file_column
        self._ordinal_column = ordinal_column

    @property
    def rows(self) -> int:
        return self.files.size()

    def copy(self):
        """An independent copy: a carry SPENDS the recorder it is given, and a relation
        with several indexes carries each from the same map."""
        cdef RowOriginRecorder out = RowOriginRecorder(self._file_column, self._ordinal_column)
        out.files = self.files
        out.ordinals = self.ordinals
        return out

    def take(self, Morsel morsel):
        """Record this row group's origins; return it without the two origin columns."""
        cdef shared_ptr[CxxMorsel] sp = morsel_to_cxx(morsel)
        cdef const CxxMorsel* m = sp.get()
        cdef string file_name = self._file_column.encode("utf-8")
        cdef string ordinal_name = self._ordinal_column.encode("utf-8")
        cdef Py_ssize_t file_at = -1, ordinal_at = -1
        cdef size_t i
        for i in range(m.names.size()):
            if m.names[i] == file_name:
                file_at = <Py_ssize_t>i
            elif m.names[i] == ordinal_name:
                ordinal_at = <Py_ssize_t>i
        if file_at < 0 or ordinal_at < 0:
            raise InvalidInternalStateError(
                f"vector carry: the compaction row group has no {self._file_column!r} / "
                f"{self._ordinal_column!r} columns to record row origins from"
            )
        cdef const DrakenVector* fv = &m.columns[<size_t>file_at].view
        cdef const DrakenVector* ov = &m.columns[<size_t>ordinal_at].view
        if fv.type != DRAKEN_INT64 or ov.type != DRAKEN_INT64 or fv.validity != NULL or ov.validity != NULL:
            raise InvalidInternalStateError("vector carry: row origins must be non-null INT64")
        cdef const int64_t* fdata = <const int64_t*>fv.data
        cdef const int64_t* odata = <const int64_t*>ov.data
        cdef uint32_t n = fv.length
        cdef uint32_t r
        cdef int64_t f, o
        cdef int64_t limit = 0xFFFFFFFF
        cdef bint bad = False
        with nogil:
            self.files.reserve(self.files.size() + n)
            self.ordinals.reserve(self.ordinals.size() + n)
            for r in range(n):
                f = fdata[fv.selection[r]]
                o = odata[ov.selection[r]]
                if f < 0 or o < 0 or f > limit or o > limit:
                    bad = True
                    break
                self.files.push_back(<uint32_t>f)
                self.ordinals.push_back(<uint32_t>o)
        if bad:
            raise InvalidInternalStateError("vector carry: a row origin is outside the uint32 range")
        # Column names are identities, as bytes.
        cdef bytes drop_file = file_name
        cdef bytes drop_ordinal = ordinal_name
        keep = [name for name in morsel.column_names if name != drop_file and name != drop_ordinal]
        return morsel.select(keep)


cdef CarrySpec _carry_spec(list inputs, uint32_t dims, uint32_t clusters, uint32_t iterations,
                           uint32_t sample_per_cluster, unsigned long long seed, uint32_t flush_rows,
                           uint32_t train_threads):
    """`inputs`: one (vectors location, vectors bytes, deleted ordinals, Authorization
    header - "" for none) per input file, in the order the recorded file indexes refer to."""
    cdef CarrySpec spec
    cdef uint32_t ordinal
    cdef size_t k = 0
    spec.inputs.resize(len(inputs))
    for location, size, deleted, auth_header in inputs:
        spec.inputs[k].vectors = (<str>location).encode("utf-8")
        spec.inputs[k].vectors_bytes = <uint64_t>size
        spec.inputs[k].auth_header = (<str>auth_header).encode("utf-8")
        for ordinal in deleted:
            spec.inputs[k].deleted.push_back(ordinal)
        k += 1
    spec.dims = dims
    spec.ivf.clusters = clusters
    spec.ivf.iterations = iterations
    spec.ivf.sample_per_cluster = sample_per_cluster
    spec.ivf.threads = train_threads
    spec.ivf.seed = seed
    spec.flush_rows = flush_rows
    return spec


cdef cppvector[CarryOutput] _carry_outputs(list recorders):
    """The recorders' arrays, MOVED into the carry (a recorder is spent once carried)."""
    cdef cppvector[CarryOutput] outputs
    cdef RowOriginRecorder recorder
    outputs.resize(len(recorders))
    cdef size_t j = 0
    for recorder in recorders:
        vib_move_into(outputs[j].src_file, recorder.files)
        vib_move_into(outputs[j].src_ordinal, recorder.ordinals)
        j += 1
    return outputs


cdef dict _carry_result(VectorIndexBuildResult* r, bint with_bytes):
    if r.empty:
        return None
    out = {
        "body_bytes": r.vectors_body_bytes,
        "vectors_bytes": r.vectors_prefix.size() + r.vectors_body_bytes,
        "centroids_bytes": r.centroids.size(),
        "logical_bytes": r.logical_bytes,
        "rows_indexed": r.rows_indexed,
        "clusters": r.clusters,
        "vectors_row_groups": r.vectors_row_groups,
    }
    if with_bytes:
        out["prefix"] = (<char*>r.vectors_prefix.data())[:r.vectors_prefix.size()]
        out["centroids"] = (<char*>r.centroids.data())[:r.centroids.size()]
    return out


def carry_vector_index_local(
    list inputs,
    list recorders,
    uint32_t dims,
    list vectors_paths,
    list centroids_paths,
    uint32_t clusters=0,
    uint32_t iterations=8,
    uint32_t sample_per_cluster=64,
    unsigned long long seed=0x5EEDC0DE,
    uint32_t flush_rows=512,
    uint32_t train_threads=1,
):
    """Carry one index for every output of a compaction into local files. Returns one
    entry per output: None when it carried no vector (no files written), else the sizes
    the commit records. Raises on any failure, including a broken carry invariant."""
    cdef CarrySpec spec = _carry_spec(inputs, dims, clusters, iterations, sample_per_cluster, seed,
                                      flush_rows, train_threads)
    cdef cppvector[CarryOutput] outputs = _carry_outputs(recorders)
    cdef cppvector[string] c_vectors, c_centroids
    for path in vectors_paths:
        c_vectors.push_back((<str>path).encode("utf-8"))
    for path in centroids_paths:
        c_centroids.push_back((<str>path).encode("utf-8"))
    cdef cppvector[VectorIndexBuildResult] results
    cdef string err
    cdef bint ok
    with nogil:
        ok = c_carry_local(spec, outputs, c_vectors, c_centroids, &results, &err)
    if not ok:
        raise RuntimeError(err.decode("utf-8", "replace"))
    return [_carry_result(&results[j], False) for j in range(results.size())]


def carry_vector_index_to_sessions(
    list inputs,
    list recorders,
    uint32_t dims,
    list sessions,
    size_t chunk_bytes=32 * 1024 * 1024,
    uint32_t clusters=0,
    uint32_t iterations=8,
    uint32_t sample_per_cluster=64,
    unsigned long long seed=0x5EEDC0DE,
    uint32_t flush_rows=512,
    uint32_t train_threads=1,
):
    """Carry one index for every output of a compaction, streaming output j's vectors
    BODY into the open resumable session `sessions[j]`. Returns one entry per output: None
    when it carried no vector (its session is left unfinished), else a dict holding
    `prefix` and `centroids` (bytes) for the caller to upload and compose."""
    cdef CarrySpec spec = _carry_spec(inputs, dims, clusters, iterations, sample_per_cluster, seed,
                                      flush_rows, train_threads)
    cdef cppvector[CarryOutput] outputs = _carry_outputs(recorders)
    cdef cppvector[string] c_sessions
    for uri in sessions:
        c_sessions.push_back((<str>uri).encode("utf-8"))
    cdef cppvector[VectorIndexBuildResult] results
    cdef string err
    cdef bint ok
    with nogil:
        ok = c_carry_to_sessions(spec, outputs, c_sessions, chunk_bytes, &results, &err)
    if not ok:
        raise RuntimeError(err.decode("utf-8", "replace"))
    return [_carry_result(&results[j], True) for j in range(results.size())]
