# distutils: language = c++
# cython: language_level=3
"""
Cython binding for the minimal C++ ParquetIOPipeline.

This module exposes a small, safe Python wrapper around the native
`rugo::ParquetIOPipeline` implemented in C++. It is intended to be imported
as `rugo.io_pipeline`.

The wrapper expects a C ABI MemoryPoolApi pointer (created by
`opteryx.compiled.structures.memory_pool_api.create_memory_pool_api`) to be
passed as an integer (pointer-sized) value when constructing the pipeline.
"""

from libcpp.string cimport string as cpp_string
from libcpp cimport bool as cpp_bool
from libc.stddef cimport size_t
from libc.stdint cimport int64_t, int32_t, uintptr_t

cdef extern from "memory_pool_api.hpp":
    ctypedef struct MemoryPoolApi:
        void* ctx
        int64_t (*reserve_for_write_ptr)(void* ctx, int64_t size, uintptr_t* out_ptr, int64_t* out_capacity)
        int (*finalize_commit)(void* ctx, int64_t ref_id, int64_t actual_length)
        int64_t (*commit_bytes)(void* ctx, const void* data, int64_t length)
        void* (*read_latched)(void* ctx, int64_t ref_id, int64_t* out_len)
        int (*unlatch)(void* ctx, int64_t ref_id)
        int (*release)(void* ctx, int64_t ref_id)
        int32_t version
        int32_t reserved

cdef extern from "io_pipeline.hpp" namespace "rugo":
    cdef cppclass MorselRef:
        int64_t ref_id
        int64_t bytes_written
        cpp_string error
        cpp_string path
        int rg_idx

    cdef cppclass ParquetIOPipeline_Config "ParquetIOPipeline::Config":
        ParquetIOPipeline_Config() except +
        int read_workers
        int decode_workers
        size_t result_queue_capacity

    cdef cppclass ParquetIOPipeline:
        ParquetIOPipeline(MemoryPoolApi* mem_api, const ParquetIOPipeline_Config& cfg) except +
        void submit_row_group(const cpp_string& path, int rg_idx, const cpp_string& serialized_morsel) except +
        cpp_bool try_get_result(MorselRef& out) except +
        void wait_shutdown() except +

# Python wrapper

cdef class ParquetIOPipelineWrapper:
    """
    Python wrapper for rugo::ParquetIOPipeline.

    Construction:
        ParquetIOPipelineWrapper(api_ptr: int, read_workers: int = 16, decode_workers: int = 4, result_queue_capacity: int = 1024)

    Methods:
        submit_row_group(path: str, rg_idx: int, serialized_morsel: bytes)
        try_get_result() -> Optional[dict]
        wait_shutdown()
    """

    cdef ParquetIOPipeline* _cpp_pipeline

    def __cinit__(self, api_ptr, int read_workers=16, int decode_workers=4, result_queue_capacity=1024):
        """
        Create a native ParquetIOPipeline bound to the given MemoryPoolApi pointer.

        api_ptr: integer (pointer) returned by create_memory_pool_api(...)
        """
        if api_ptr is None:
            raise ValueError("api_ptr must be a non-null pointer value")

        cdef MemoryPoolApi* api = <MemoryPoolApi*>api_ptr
        if api == NULL:
            raise ValueError("api_ptr is NULL")

        cdef ParquetIOPipeline_Config cfg = ParquetIOPipeline_Config()
        cfg.read_workers = read_workers
        cfg.decode_workers = decode_workers
        cfg.result_queue_capacity = <size_t>result_queue_capacity

        # Construct native object (may raise C++ exceptions which Cython translates)
        self._cpp_pipeline = new ParquetIOPipeline(api, cfg)

    def __dealloc__(self):
        if self._cpp_pipeline is not NULL:
            try:
                # Best-effort shutdown. wait_shutdown() is noexcept/except+; let it propagate
                self._cpp_pipeline.wait_shutdown()
            except Exception:
                # suppress in dealloc
                pass
            del self._cpp_pipeline
            self._cpp_pipeline = NULL

    def submit_row_group(self, path, int rg_idx, serialized_morsel):
        """
        Submit a pre-serialized morsel for emit.

        `path` should be a string identifying the file (for metadata).
        `serialized_morsel` must be bytes (the serialized morsel payload).
        """
        if self._cpp_pipeline is NULL:
            raise RuntimeError("native pipeline not initialized")

        if not isinstance(path, (bytes, str)):
            raise TypeError("path must be str or bytes")

        if not isinstance(serialized_morsel, (bytes, bytearray)):
            raise TypeError("serialized_morsel must be bytes or bytearray")

        cdef bytes path_bytes = path.encode("utf8") if isinstance(path, str) else path
        cdef cpp_string p = cpp_string(path_bytes)

        cdef bytes payload = bytes(serialized_morsel)
        cdef cpp_string s = cpp_string(payload)

        # Call C++ method - exceptions will be translated to Python exceptions
        self._cpp_pipeline.submit_row_group(p, rg_idx, s)

    def try_get_result(self):
        """
        Non-blocking pop of a result.

        Returns:
            None if no result is available, or a dict:
            {
                "ref_id": int,
                "bytes_written": int,
                "error": str (empty if none),
                "path": str,
                "rg_idx": int
            }
        """
        if self._cpp_pipeline is NULL:
            raise RuntimeError("native pipeline not initialized")

        cdef MorselRef out
        cdef cpp_bool ok = self._cpp_pipeline.try_get_result(out)
        if not ok:
            return None

        # Marshal fields
        try:
            py_error = out.error.c_str() if out.error.size() > 0 else ""
        except Exception:
            py_error = ""

        try:
            py_path = out.path.c_str() if out.path.size() > 0 else ""
        except Exception:
            py_path = ""

        return {
            "ref_id": int(out.ref_id),
            "bytes_written": int(out.bytes_written),
            "error": py_error,
            "path": py_path,
            "rg_idx": int(out.rg_idx),
        }

    def wait_shutdown(self):
        """
        Block until pipeline completes and workers shut down.
        """
        if self._cpp_pipeline is NULL:
            return
        self._cpp_pipeline.wait_shutdown()


# Expose module-level names for convenience and compatibility.
# This module is intended to be imported as:
#   from rugo import io_pipeline
#
# Provide a thin factory that mirrors the C++ wrapper class.

def create_pipeline(api_ptr, int read_workers=16, int decode_workers=4, result_queue_capacity=1024):
    """
    Convenience factory returning a ParquetIOPipelineWrapper instance.

    api_ptr: pointer-sized integer (MemoryPoolApi*)
    """
    return ParquetIOPipelineWrapper(api_ptr, read_workers, decode_workers, result_queue_capacity)
