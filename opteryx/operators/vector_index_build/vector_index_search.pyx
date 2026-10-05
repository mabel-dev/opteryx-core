# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Vector index search — one data file (docs/VECTOR_INDEX_DESIGN.md §8, D2).

The entry point into src/cpp/engine/vector_index_search.hpp: embed the query text with the
index's own embedder, score it against the file's stored vectors - every one when `nprobe`
is 0 (exact, the default), only the `nprobe` nearest clusters' otherwise (approximate) -
reading the index file locally or remotely in large parallel range GETs (gs:// with
`auth_header`, or a presigned URL), and return the file's top-k. One native call, GIL
released. The library the vector search scan is built from, and how it is tested.
"""

from libcpp cimport bool as cppbool

cdef extern from "ops/ann/fp16_cosine_ivf.h" namespace "draken::ann" nogil:
    cdef cppclass AnnHit:
        uint32_t ordinal
        double distance

cdef extern from "engine/vector_index_search.hpp" namespace "opteryx::engine" nogil:
    cdef cppclass IndexFileRef:
        string path
        uint64_t file_bytes
        uint64_t footer_bytes
        string auth_header

    cdef cppclass IndexSearchStats:
        uint32_t clusters
        uint32_t probed
        uint32_t blocks_read
        uint64_t bytes_read
        uint32_t requests
        uint64_t rows_scored

    bint embed_query(VibEmbedFn embed, uint32_t dims, const DrakenVector& text,
                     cppvector[uint16_t]* out, cppbool* ok, string* err)
    bint search_index_file(const IndexFileRef& file, const uint16_t* query, uint32_t dims,
                           uint32_t k, uint32_t nprobe, uint64_t data_rows,
                           const cppvector[uint32_t]& deleted, const uint8_t* admitted,
                           cppvector[AnnHit]* out, IndexSearchStats* stats, string* err)


def search_vector_index_file(
    str path,
    unsigned long long file_bytes,
    unsigned long long footer_bytes,
    str query,
    unsigned long long embed_fn,
    uint32_t dims,
    uint32_t k,
    uint32_t nprobe,
    unsigned long long data_rows,
    list deleted=None,
    str auth_header="",
):
    """The data file's top-`k` rows nearest `query` through its index: a list of
    (ordinal, cosine distance), nearest first, ties by ordinal; and the search's counts.
    `nprobe` 0 = exact (every stored vector); >= 1 = the `nprobe` nearest clusters only.
    `footer_bytes` 0 = unknown (the open costs one more round trip)."""
    from draken.interop.vector_sequence import vector_from_sequence

    if embed_fn == 0:
        raise ValueError("vector index search: no embedding kernel")
    cdef Morsel one = Morsel()
    one.append_vector(b"q", vector_from_sequence([query], dtype="VARCHAR"))
    cdef shared_ptr[CxxMorsel] sp = morsel_to_cxx(one)
    cdef IndexFileRef ref
    ref.path = path.encode("utf-8")
    ref.file_bytes = file_bytes
    ref.footer_bytes = footer_bytes
    ref.auth_header = auth_header.encode("utf-8")
    cdef cppvector[uint32_t] c_deleted
    cdef uint32_t ordinal
    for ordinal in (deleted or ()):
        c_deleted.push_back(ordinal)
    cdef cppvector[uint16_t] q
    cdef cppbool has_query = False
    cdef cppvector[AnnHit] hits
    cdef IndexSearchStats stats
    cdef string err
    cdef bint ok
    cdef VibEmbedFn fn = <VibEmbedFn><void*>embed_fn
    with nogil:
        ok = embed_query(fn, dims, sp.get().columns[0].view, &q, &has_query, &err)
        if ok and has_query:
            ok = search_index_file(ref, q.data(), dims, k, nprobe, data_rows, c_deleted, NULL,
                                   &hits, &stats, &err)
    if not ok:
        raise RuntimeError(err.decode("utf-8", "replace"))
    return (
        [(hits[i].ordinal, hits[i].distance) for i in range(hits.size())],
        {"clusters": stats.clusters, "probed": stats.probed, "blocks_read": stats.blocks_read,
         "bytes_read": stats.bytes_read, "requests": stats.requests, "rows_scored": stats.rows_scored},
    )


cdef extern from "io_pipeline.hpp" namespace "rugo" nogil:
    ctypedef int (*Pass1PredFn)(void*, DrakenVector**, int, uint32_t, uint8_t*)

cdef extern from "engine/native_parquet_scan_source.hpp" namespace "opteryx::engine" nogil:
    cdef cppclass NativeScanColumnBuilder:
        CppMemoryPool* pool
        const cppvector[uint8_t]* decimal_columns
        const cppvector[int]* string_types
        const cppvector[int]* logical_coerce
        const cppvector[uint8_t]* array_columns
        const cppvector[int]* widen_types

cdef extern from "engine/vector_index_admission.hpp" namespace "opteryx::engine" nogil:
    cdef cppclass AdmissionFile:
        string path
        string filter_path
        uint64_t rows
        cppvector[uint32_t] deleted
        cppbool indexed
        IndexFileRef index

    cdef cppclass AdmissionCounts:
        uint32_t files_indexed
        uint32_t files_exact
        uint64_t clusters_probed
        uint64_t index_bytes_read
        uint64_t index_requests
        uint64_t candidates
        uint64_t rows_exact

    cdef cppclass AdmissionPredicate:
        ParquetIOPipeline* pipeline
        const ParquetFooterMap* footers
        const cppvector[pair[string, int]]* work_items
        const cppvector[string]* column_names
        NativeScanColumnBuilder builder
        Pass1PredFn fn
        void* ctx
        cppvector[int] pred_col_to_p1
        int in_flight

    cdef cppclass VectorIndexAdmission:
        VectorIndexAdmission(cppvector[AdmissionFile] files, shared_ptr[CxxMorsel] query,
                             VibEmbedFn embed, uint32_t dims, uint32_t k, uint32_t nprobe)
        const AdmissionCounts& counts()
        void set_predicate(AdmissionPredicate predicate)


cdef class VectorIndexAdmissionHandle:
    """Owns one approximate scan's VectorIndexAdmission for the run (see
    src/cpp/engine/vector_index_admission.hpp). Built by the compiler from plan data -
    the scan's files with their index files and deletes, the query text, k, nprobe - and
    handed to the native scan by address. Nothing is searched until the scan starts."""

    cdef VectorIndexAdmission* admission

    def __cinit__(self, list files, str query, unsigned long long embed_fn, uint32_t dims,
                  uint32_t k, uint32_t nprobe):
        """`files`: (fetch path, filter fetch path, physical rows, deleted ordinals, index
        or None), where index = (index file location, file bytes, footer bytes,
        Authorization header - "" for none). The filter fetch path is how the WHERE's
        pass-1 plan names the file (it may be signed separately); pass the fetch path when
        there is no WHERE."""
        from draken.interop.vector_sequence import vector_from_sequence

        if embed_fn == 0:
            raise ValueError("vector index search: no embedding kernel")
        cdef cppvector[AdmissionFile] c_files
        cdef size_t i = 0
        cdef uint32_t ordinal
        c_files.resize(len(files))
        for path, filter_path, rows, deleted, index in files:
            c_files[i].path = (<str>path).encode("utf-8")
            c_files[i].filter_path = (<str>filter_path).encode("utf-8")
            c_files[i].rows = <uint64_t>rows
            for ordinal in deleted:
                c_files[i].deleted.push_back(ordinal)
            c_files[i].indexed = index is not None
            if index is not None:
                c_files[i].index.path = (<str>index[0]).encode("utf-8")
                c_files[i].index.file_bytes = <uint64_t>index[1]
                c_files[i].index.footer_bytes = <uint64_t>index[2]
                c_files[i].index.auth_header = (<str>index[3]).encode("utf-8")
            i += 1
        cdef Morsel one = Morsel()
        one.append_vector(b"q", vector_from_sequence([query], dtype="VARCHAR"))
        cdef shared_ptr[CxxMorsel] q = morsel_to_cxx(one)
        self.admission = new VectorIndexAdmission(c_files, q, <VibEmbedFn><void*>embed_fn, dims, k, nprobe)

    def __dealloc__(self):
        if self.admission != NULL:
            del self.admission
            self.admission = NULL

    def address(self) -> int:
        return <size_t><void*>self.admission

    def counts(self) -> dict:
        """What the search did - read after the run (EXPLAIN / telemetry)."""
        cdef const AdmissionCounts* c = &self.admission.counts()
        return {
            "files_indexed": c.files_indexed, "files_exact": c.files_exact,
            "clusters_probed": c.clusters_probed, "index_bytes_read": c.index_bytes_read,
            "index_requests": c.index_requests, "candidates": c.candidates, "rows_exact": c.rows_exact,
        }

    def set_predicate(self, NativeScanPlan plan, size_t fn, size_t ctx, list pred_col_to_p1,
                      object holder):
        """The pushed WHERE: `plan` is a NativeScanPlan over the predicate columns, (fn,
        ctx) the Pass1PredResolver's C ABI over them; `holder` (the plan and resolver)
        must be kept alive by the caller for the run."""
        cdef AdmissionPredicate pred
        pred.pipeline = plan.pipeline_ptr
        pred.footers = plan.footer_map
        pred.work_items = &plan.work_items
        pred.column_names = &plan.column_names
        if plan._pool is not None:
            pred.builder.pool = plan._pool._pool
        pred.builder.decimal_columns = &plan.decimal_columns
        pred.builder.string_types = &plan.string_types
        pred.builder.logical_coerce = &plan.logical_coerce
        pred.builder.array_columns = &plan.array_columns
        pred.builder.widen_types = &plan.widen_types
        pred.fn = <Pass1PredFn><void*>fn
        pred.ctx = <void*>ctx
        for i in pred_col_to_p1:
            pred.pred_col_to_p1.push_back(<int>i)
        pred.in_flight = plan.in_flight_limit
        self.admission.set_predicate(pred)
