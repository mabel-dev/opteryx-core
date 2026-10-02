# Transparent decompression for the JSONL and CSV readers — the Python surface of
# rugo/src/compression/stream_decompress.hpp (the same decoders NativeJsonlScanSource
# streams through). Magic bytes decide the codec; an unsupported codec, an extension
# the bytes contradict, or a corrupt/truncated stream raises RuntimeError naming the
# file — compressed bytes are never parsed as text.

from libc.stdint cimport uint8_t
from libcpp.memory cimport unique_ptr
from libcpp.string cimport string
from libcpp.utility cimport move
from cpython.bytes cimport PyBytes_FromStringAndSize


cdef extern from "compression/stream_decompress.hpp" namespace "rugo::compression":
    cdef enum class Codec(uint8_t):
        NONE
        GZIP
        ZSTD
        LZ4
        BZIP2
        XZ
        ZIP
        SNAPPY

    const char* codec_name(Codec c)
    Codec resolve_codec(const string& path, const uint8_t* p, size_t n) except +

    cdef cppclass StreamDecoder:
        size_t read(uint8_t* dst, size_t cap) except + nogil

    unique_ptr[StreamDecoder] make_decoder(Codec codec, const uint8_t* src, size_t len) except +

    cdef cppclass ByteBuffer:
        unique_ptr[uint8_t] data   # unique_ptr<uint8_t[]> in C++; only .get() is used
        size_t len
        size_t cap
        void reserve(size_t want) except + nogil

    cdef cppclass LineChunker:
        LineChunker(unique_ptr[StreamDecoder] decoder)
        bint next(size_t target, ByteBuffer& out) except + nogil


cdef Codec _resolve_codec(str path, const uint8_t* ptr, size_t n) except *:
    cdef string c_path = path.encode("utf-8")
    return resolve_codec(c_path, ptr, n)


cdef bytes _decompress_all(Codec codec, const uint8_t* src, size_t n):
    """The whole decompressed stream as one bytes object."""
    cdef unique_ptr[StreamDecoder] decoder = make_decoder(codec, src, n)
    cdef ByteBuffer buf
    cdef size_t got
    with nogil:
        buf.reserve(n * 4 if n * 4 > (1 << 20) else (1 << 20))
        while True:
            if buf.len == buf.cap:
                buf.reserve(buf.cap + buf.cap // 2)
            got = decoder.get().read(buf.data.get() + buf.len, buf.cap - buf.len)
            if got == 0:
                break
            buf.len += got
    return PyBytes_FromStringAndSize(<char*>buf.data.get(), <Py_ssize_t>buf.len)


def detect_compression(data, str path=""):
    """The codec `data` (the whole file's bytes) is compressed with — 'gzip', 'zstd'
    or 'lz4' — or None for plain input. Raises RuntimeError for an unsupported codec
    (bzip2, xz, zip, snappy) or when `path`'s extension claims a codec the bytes do
    not carry."""
    cdef const uint8_t[::1] view = memoryview(data).cast("B")
    cdef size_t n = view.shape[0]
    cdef Codec codec = _resolve_codec(path, &view[0] if n > 0 else NULL, n)
    if codec == Codec.NONE:
        return None
    return codec_name(codec).decode("ascii")


def decompress(data, str path=""):
    """`data` decompressed whole, or `data` itself when it is not compressed. For
    readers with no chunked entry point (CSV); JSONL scans stream instead."""
    cdef const uint8_t[::1] view = memoryview(data).cast("B")
    cdef size_t n = view.shape[0]
    cdef const uint8_t* ptr = &view[0] if n > 0 else NULL
    cdef Codec codec = _resolve_codec(path, ptr, n)
    if codec == Codec.NONE:
        return data
    return _decompress_all(codec, ptr, n)


def decompressed_first_chunk(data, str path, size_t chunk_size):
    """The first newline-aligned chunk of the DECOMPRESSED stream — at least
    `chunk_size` bytes, cut at the first newline at or after that offset, or the whole
    stream — exactly the chunk a streaming JSONL scan decodes first. None for an empty
    stream. Only the bytes needed are decompressed. `data` must be compressed (see
    detect_compression)."""
    cdef const uint8_t[::1] view = memoryview(data).cast("B")
    cdef size_t n = view.shape[0]
    cdef const uint8_t* ptr = &view[0] if n > 0 else NULL
    cdef Codec codec = _resolve_codec(path, ptr, n)
    if codec == Codec.NONE:
        raise ValueError(f"decompressed_first_chunk: '{path}' is not compressed")
    cdef LineChunker* chunker = new LineChunker(move(make_decoder(codec, ptr, n)))
    cdef ByteBuffer buf
    cdef bint got
    try:
        with nogil:
            got = chunker.next(chunk_size, buf)
    finally:
        del chunker
    if not got:
        return None
    return PyBytes_FromStringAndSize(<char*>buf.data.get(), <Py_ssize_t>buf.len)
