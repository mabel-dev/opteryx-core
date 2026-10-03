# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Vector index build — one data file (docs/VECTOR_INDEX_DESIGN.md §5.2, §10).

The entry point into src/cpp/engine/vector_index_build.hpp. The boundary is crossed ONCE
per data file and the GIL is released for the whole build: reading the text column,
embedding, clustering and writing both index files are native. What the caller hands in
is plan data — the file, its column, its deleted ordinals (decoded at plan time as for
every scan) and the resolved `draken_embed` kernel.

Two outputs: local files (`build_vector_index_local`), or the vectors body streamed into an
already-open GCS resumable upload session (`build_vector_index_to_session`) — the caller
opened the session, and afterwards uploads the returned prefix and centroids and composes
prefix + body into the vectors file.
"""

cdef extern from "ops/ann/fp16_cosine_ivf.h" namespace "draken::ann" nogil:
    cdef cppclass VibIvfParams "draken::ann::IvfParams":
        uint32_t clusters
        uint32_t iterations
        uint32_t sample_per_cluster
        uint32_t threads
        uint64_t seed

cdef extern from *:
    ctypedef void* VibEmbedFn "opteryx::engine::EmbedFn"

cdef extern from "engine/vector_index_build.hpp" namespace "opteryx::engine" nogil:
    cdef cppclass VectorIndexBuildSpec:
        string data_path
        int64_t data_bytes
        string column
        cppvector[uint32_t] deleted
        VibEmbedFn embed
        uint32_t dims
        VibIvfParams ivf
        uint32_t flush_rows
        uint32_t embed_batch
        uint32_t embed_threads
        uint32_t decode_workers

    cdef cppclass VectorIndexBuildResult:
        bint empty
        cppvector[uint8_t] vectors_prefix
        uint64_t vectors_body_bytes
        cppvector[uint8_t] centroids
        uint64_t rows_indexed
        uint32_t clusters
        uint32_t vectors_row_groups
        uint64_t logical_bytes

    bint build_vector_index_file_local(const VectorIndexBuildSpec& spec, const string& vectors_path,
                                       const string& centroids_path, VectorIndexBuildResult* out,
                                       string* err)
    bint build_vector_index_file_to_session(const VectorIndexBuildSpec& spec, const string& session_uri,
                                            size_t chunk_bytes, VectorIndexBuildResult* out, string* err)


cdef VectorIndexBuildSpec _vib_spec(str data_path, str column, list deleted, unsigned long long embed_fn,
                                    uint32_t dims, uint32_t clusters, uint32_t iterations,
                                    uint32_t sample_per_cluster, unsigned long long seed, uint32_t flush_rows,
                                    uint32_t embed_batch, uint32_t embed_threads, uint32_t decode_workers,
                                    uint32_t train_threads, long long data_bytes):
    cdef VectorIndexBuildSpec spec
    cdef uint32_t ordinal
    if embed_fn == 0:
        raise ValueError("vector index build: no embedding kernel")
    spec.data_path = data_path.encode("utf-8")
    spec.data_bytes = data_bytes
    spec.column = column.encode("utf-8")
    for ordinal in deleted:
        spec.deleted.push_back(ordinal)
    spec.embed = <VibEmbedFn><void*>embed_fn
    spec.dims = dims
    spec.ivf.clusters = clusters
    spec.ivf.iterations = iterations
    spec.ivf.sample_per_cluster = sample_per_cluster
    spec.ivf.threads = train_threads
    spec.ivf.seed = seed
    spec.flush_rows = flush_rows
    spec.embed_batch = embed_batch
    spec.embed_threads = embed_threads
    spec.decode_workers = decode_workers
    return spec


def build_vector_index_local(
    str data_path,
    str column,
    list deleted,
    unsigned long long embed_fn,
    uint32_t dims,
    str vectors_path,
    str centroids_path,
    uint32_t clusters=0,
    uint32_t iterations=8,
    uint32_t sample_per_cluster=64,
    unsigned long long seed=0x5EEDC0DE,
    uint32_t flush_rows=512,
    uint32_t embed_batch=1,
    uint32_t embed_threads=1,
    uint32_t decode_workers=2,
    uint32_t train_threads=1,
    long long data_bytes=-1,
):
    """Build one parquet data file's vector index into two local skene files.

    `data_path` is a local file or a self-authenticating (signed) https URL; a remote one
    needs `data_bytes`, its size.

    Returns None when the file has no indexable row (nothing is written), otherwise a
    dict of the sizes and counts the catalog commit and the logs need. Raises on any
    failure, after which neither output file exists.
    """
    cdef VectorIndexBuildSpec spec = _vib_spec(
        data_path, column, deleted, embed_fn, dims, clusters, iterations, sample_per_cluster, seed,
        flush_rows, embed_batch, embed_threads, decode_workers, train_threads, data_bytes)
    cdef VectorIndexBuildResult result
    cdef string err
    cdef string c_vectors = vectors_path.encode("utf-8")
    cdef string c_centroids = centroids_path.encode("utf-8")
    cdef bint ok

    with nogil:
        ok = build_vector_index_file_local(spec, c_vectors, c_centroids, &result, &err)
    if not ok:
        raise RuntimeError(err.decode("utf-8", "replace"))
    if result.empty:
        return None
    return {
        "vectors_bytes": result.vectors_prefix.size() + result.vectors_body_bytes,
        "centroids_bytes": result.centroids.size(),
        "logical_bytes": result.logical_bytes,
        "rows_indexed": result.rows_indexed,
        "clusters": result.clusters,
        "vectors_row_groups": result.vectors_row_groups,
    }


def build_vector_index_to_session(
    str data_path,
    str column,
    list deleted,
    unsigned long long embed_fn,
    uint32_t dims,
    str session_uri,
    size_t chunk_bytes=32 * 1024 * 1024,
    uint32_t clusters=0,
    uint32_t iterations=8,
    uint32_t sample_per_cluster=64,
    unsigned long long seed=0x5EEDC0DE,
    uint32_t flush_rows=512,
    uint32_t embed_batch=1,
    uint32_t embed_threads=1,
    uint32_t decode_workers=2,
    uint32_t train_threads=1,
    long long data_bytes=-1,
):
    """Build one parquet data file's vector index, streaming the vectors BODY into the open
    resumable upload session `session_uri`.

    Returns None when the file has no indexable row (the session is left unfinished, so no
    object exists), otherwise a dict holding `prefix` and `centroids` (bytes — the caller
    uploads both and composes prefix + body), the body's size, and the counts. Raises on
    any failure; the session is then never finished.
    """
    cdef VectorIndexBuildSpec spec = _vib_spec(
        data_path, column, deleted, embed_fn, dims, clusters, iterations, sample_per_cluster, seed,
        flush_rows, embed_batch, embed_threads, decode_workers, train_threads, data_bytes)
    cdef VectorIndexBuildResult result
    cdef string err
    cdef string c_uri = session_uri.encode("utf-8")
    cdef bint ok

    with nogil:
        ok = build_vector_index_file_to_session(spec, c_uri, chunk_bytes, &result, &err)
    if not ok:
        raise RuntimeError(err.decode("utf-8", "replace"))
    if result.empty:
        return None
    prefix = (<char*>result.vectors_prefix.data())[:result.vectors_prefix.size()]
    centroids = (<char*>result.centroids.data())[:result.centroids.size()]
    return {
        "prefix": prefix,
        "centroids": centroids,
        "body_bytes": result.vectors_body_bytes,
        "vectors_bytes": result.vectors_prefix.size() + result.vectors_body_bytes,
        "centroids_bytes": result.centroids.size(),
        "logical_bytes": result.logical_bytes,
        "rows_indexed": result.rows_indexed,
        "clusters": result.clusters,
        "vectors_row_groups": result.vectors_row_groups,
    }
