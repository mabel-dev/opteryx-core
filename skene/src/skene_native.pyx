# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
#
# skene/src/skene_native.pyx — Python boundary for the skene file format.
#
# The core is C++ (skene/src/*.cpp) and Status-coded end to end; this module is
# the ONLY place a Status becomes an exception and a CxxMorsel becomes a draken
# Morsel. No logic lives here — it marshals options in and results out.
#
# draken is imported, never copied: morsels cross the boundary through
# draken.morsels.morsel's cxx_to_morsel / morsel_to_cxx, so the vectors inside
# a returned Morsel are the same buffers skene reconstructed — zero copy.

from libc.stdint cimport int16_t, int64_t, uint8_t, uint16_t, uint32_t, uint64_t
from libcpp cimport bool as cbool
from libcpp.memory cimport make_shared, shared_ptr
from libcpp.string cimport string
from libcpp.utility cimport move
from libcpp.vector cimport vector

from cython.operator cimport dereference as deref
from cpython.bytes cimport PyBytes_FromStringAndSize

from draken.morsels.cxx_morsel cimport CxxMorsel
from draken.morsels.morsel cimport Morsel, cxx_to_morsel, morsel_to_cxx


# ─── skene C++ API ───────────────────────────────────────────────────────────

cdef extern from "skene/status.h" namespace "skene" nogil:
    cdef enum class Code(int):
        kOk
        kNotSkene
        kTruncated
        kUnsupportedVersion
        kWrongEndianness
        kUnknownChecksum
        kChecksumMismatch
        kMalformed
        kUnsupportedSection
        kUnsupportedEncoding
        kUnsupportedType
        kOutOfMemory

    cdef cppclass Status:
        cbool is_ok() const
        Code code() const
        string message() const


cdef extern from "skene/format.h" namespace "skene" nogil:
    cdef enum class SelectionKind(uint8_t):
        kConstant
        kIdentity
        kStored

    cdef enum class ValueOrder(uint8_t):
        kAsWritten
        kAscending

    cdef cppclass LogicalTypeDescriptor:
        uint8_t  kind
        uint8_t  unit
        int16_t  offset_minutes
        uint8_t  precision
        uint8_t  scale
        uint32_t dimension

    cdef cppclass ColumnStatistics:
        uint32_t flags
        int64_t  min_ordinal
        int64_t  max_ordinal
        uint64_t null_count
        int64_t  sum_low
        int64_t  sum_high

    cdef cppclass ZoneMapEntry:
        int64_t min_ordinal
        int64_t max_ordinal


cdef extern from "skene/probe.h" namespace "skene" nogil:
    const size_t kProbeBytes
    Status c_probe_version "skene::probe_version"(const void* head, size_t head_bytes,
                                                  uint16_t* out_version)


cdef extern from "skene/reader.h" namespace "skene" nogil:
    cdef cppclass ZoneMap:
        uint32_t chunk_rows
        vector[ZoneMapEntry] chunks
        cbool present() const

    cdef cppclass ColumnMetadata:
        string   name
        uint32_t field_id
        uint32_t type
        cbool    logical_present
        LogicalTypeDescriptor logical
        uint32_t length
        uint32_t data_length
        uint8_t  vector_flags
        SelectionKind selection_kind
        ValueOrder value_order
        uint64_t byte_offset
        uint64_t byte_bytes
        cbool    has_statistics
        ColumnStatistics statistics
        ZoneMap  zone_map
        vector[uint8_t] bloom
        vector[ColumnMetadata] children

    cdef cppclass FileMetadata:
        uint16_t version
        uint64_t row_count
        uint8_t  file_uuid[16]
        uint64_t created_at_unix_us
        string   writer_tag
        vector[ColumnMetadata] columns

    cdef cppclass ReadOptions:
        vector[string] columns

    Status c_footer_extent "skene::footer_extent"(const void* tail, size_t tail_bytes,
                                                  uint64_t file_bytes,
                                                  uint64_t* out_offset, uint64_t* out_bytes)
    Status c_read_metadata "skene::read_metadata"(const void* file, size_t file_bytes,
                                                  FileMetadata* out)
    Status c_read_morsel "skene::read_morsel"(const void* file, size_t file_bytes,
                                              const ReadOptions& options, CxxMorsel* out)


cdef extern from "skene/writer.h" namespace "skene" nogil:
    cdef cppclass WriteOptions:
        cbool read_acceleration
        int zstd_level
        vector[string] bloom_columns
        double bloom_false_positive_rate
        vector[uint32_t] field_ids
        uint8_t file_uuid[16]
        uint64_t created_at_unix_us
        string writer_tag

    Status c_write_morsel "skene::write_morsel"(const CxxMorsel& morsel,
                                                const WriteOptions& options,
                                                vector[uint8_t]* out)


# ─── Error model ─────────────────────────────────────────────────────────────

class SkeneError(Exception):
    """A skene read or write failed. `code` is the Status code name."""

    def __init__(self, code, message):
        self.code = code
        super().__init__(f"{code}: {message}")


_CODE_NAMES = {
    <int>Code.kNotSkene: "NotSkene",
    <int>Code.kTruncated: "Truncated",
    <int>Code.kUnsupportedVersion: "UnsupportedVersion",
    <int>Code.kWrongEndianness: "WrongEndianness",
    <int>Code.kUnknownChecksum: "UnknownChecksum",
    <int>Code.kChecksumMismatch: "ChecksumMismatch",
    <int>Code.kMalformed: "Malformed",
    <int>Code.kUnsupportedSection: "UnsupportedSection",
    <int>Code.kUnsupportedEncoding: "UnsupportedEncoding",
    <int>Code.kUnsupportedType: "UnsupportedType",
    <int>Code.kOutOfMemory: "OutOfMemory",
}


cdef int _check(const Status& st) except -1:
    if st.is_ok():
        return 0
    raise SkeneError(_CODE_NAMES.get(<int>st.code(), str(<int>st.code())),
                     st.message().decode("utf-8", "replace"))


# ─── Metadata marshalling ────────────────────────────────────────────────────

cdef dict _column_to_dict(const ColumnMetadata& c):
    cdef dict out = {
        "name": c.name.decode("utf-8"),
        "field_id": c.field_id,
        "type": c.type,  # DrakenType tag
        "logical": None,
        "length": c.length,
        "data_length": c.data_length,  # under value ordering: exact distinct count
        "vector_flags": c.vector_flags,
        "selection_kind": <int>c.selection_kind,
        "value_order": <int>c.value_order,
        "byte_offset": c.byte_offset,
        "byte_bytes": c.byte_bytes,
        "statistics": None,
        "zone_map": None,
        "has_bloom": c.bloom.size() > 0,
        "children": [],
    }
    if c.logical_present:
        out["logical"] = {
            "kind": c.logical.kind,
            "unit": c.logical.unit,
            "offset_minutes": c.logical.offset_minutes,
            "precision": c.logical.precision,
            "scale": c.logical.scale,
            "dimension": c.logical.dimension,
        }
    if c.has_statistics:
        # int128 sum from little-endian int64 halves.
        out["statistics"] = {
            "flags": c.statistics.flags,
            "min_ordinal": c.statistics.min_ordinal,
            "max_ordinal": c.statistics.max_ordinal,
            "null_count": c.statistics.null_count,
            # int128 from little-endian int64 halves — as Python big-int math
            # (<object> casts; a C shift of an int64 by 64 is UB, not a widening).
            "sum": ((<object>c.statistics.sum_high) << 64)
                   + ((<object>c.statistics.sum_low) & 0xFFFFFFFFFFFFFFFF),
        }
    if c.zone_map.present():
        out["zone_map"] = {
            "chunk_rows": c.zone_map.chunk_rows,
            "chunks": [(c.zone_map.chunks[i].min_ordinal, c.zone_map.chunks[i].max_ordinal)
                       for i in range(c.zone_map.chunks.size())],
        }
    cdef size_t i
    for i in range(c.children.size()):
        out["children"].append(_column_to_dict(c.children[i]))
    return out


# ─── Public API ──────────────────────────────────────────────────────────────

def probe_version(const unsigned char[::1] head not None):
    """Format/version probe on the first bytes of a file (needs >= 8 bytes).

    Succeeds for versions this build cannot read — dispatch on the result.
    Raises SkeneError(NotSkene) when the bytes are not a skene file.
    """
    cdef uint16_t version = 0
    cdef Status st
    if head.shape[0] == 0:
        raise SkeneError("Truncated", "empty buffer")
    st = c_probe_version(<const void*>&head[0], <size_t>head.shape[0], &version)
    _check(st)
    return version


def footer_extent(const unsigned char[::1] tail not None, uint64_t file_bytes):
    """Given the last kFileTailBytes of an object and its total size, return
    (offset, nbytes) of the footer — the tail-then-footer remote read path."""
    cdef uint64_t offset = 0, nbytes = 0
    cdef Status st
    if tail.shape[0] == 0:
        raise SkeneError("Truncated", "empty buffer")
    st = c_footer_extent(<const void*>&tail[0], <size_t>tail.shape[0],
                         file_bytes, &offset, &nbytes)
    _check(st)
    return offset, nbytes


def read_metadata(const unsigned char[::1] file not None):
    """Parse the footer only (cheap; never touches the data region).

    Returns a dict: version, row_count, file_uuid, created_at_unix_us,
    writer_tag, columns — each column carrying type/logical/stats/zone_map.
    """
    cdef FileMetadata meta
    cdef Status st
    if file.shape[0] == 0:
        raise SkeneError("Truncated", "empty buffer")
    with nogil:
        st = c_read_metadata(<const void*>&file[0], <size_t>file.shape[0], &meta)
    _check(st)
    cdef size_t i
    return {
        "version": meta.version,
        "row_count": meta.row_count,
        "file_uuid": PyBytes_FromStringAndSize(<char*>meta.file_uuid, 16),
        "created_at_unix_us": meta.created_at_unix_us,
        "writer_tag": meta.writer_tag.decode("utf-8", "replace"),
        "columns": [_column_to_dict(meta.columns[i]) for i in range(meta.columns.size())],
    }


def read_morsel(const unsigned char[::1] file not None, columns=None):
    """Reconstruct the file's morsel. `columns` narrows to those identities
    (a missing name is an error, not a silently absent column)."""
    cdef ReadOptions options
    cdef shared_ptr[CxxMorsel] sp = make_shared[CxxMorsel]()
    cdef Status st
    if file.shape[0] == 0:
        raise SkeneError("Truncated", "empty buffer")
    if columns is not None:
        for name in columns:
            options.columns.push_back(name.encode("utf-8"))
    with nogil:
        st = c_read_morsel(<const void*>&file[0], <size_t>file.shape[0],
                           options, sp.get())
    _check(st)
    return cxx_to_morsel(sp)


def write_morsel(Morsel morsel not None, *, read_acceleration=False, zstd_level=0,
                 bloom_columns=None, bloom_false_positive_rate=0.05,
                 field_ids=None, created_at_unix_us=0, writer_tag=""):
    """Serialize a draken Morsel to .skene bytes.

    read_acceleration=True enables value ordering + statistics + zone maps
    (the writer's for_storage posture); the default is the spill posture.
    """
    cdef WriteOptions options
    cdef vector[uint8_t] out
    cdef Status st
    options.read_acceleration = read_acceleration
    options.zstd_level = zstd_level
    options.bloom_false_positive_rate = bloom_false_positive_rate
    options.created_at_unix_us = created_at_unix_us
    options.writer_tag = writer_tag.encode("utf-8")
    if bloom_columns is not None:
        for name in bloom_columns:
            options.bloom_columns.push_back(name.encode("utf-8"))
    if field_ids is not None:
        for fid in field_ids:
            options.field_ids.push_back(<uint32_t>fid)
    cdef shared_ptr[CxxMorsel] sp = morsel_to_cxx(morsel)
    with nogil:
        st = c_write_morsel(deref(sp), options, &out)
    _check(st)
    return PyBytes_FromStringAndSize(<char*>out.data(), <Py_ssize_t>out.size())
