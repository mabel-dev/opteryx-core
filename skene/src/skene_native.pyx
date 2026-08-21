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
        uint64_t ndv

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
        vector[uint64_t] sketch
        ZoneMap  zone_map
        vector[uint8_t] bloom
        vector[ColumnMetadata] children

    cdef cppclass ColumnSchema:
        string   name
        uint32_t field_id
        uint32_t type
        cbool    logical_present
        LogicalTypeDescriptor logical
        vector[ColumnSchema] children

    cdef cppclass RowGroupColumnStatistics:
        cbool present
        ColumnStatistics statistics
        vector[uint64_t] sketch

    cdef cppclass RowGroupSummary:
        uint64_t row_count
        uint64_t first_row
        uint64_t byte_offset
        uint64_t byte_bytes
        uint64_t footer_offset
        uint32_t footer_bytes
        vector[RowGroupColumnStatistics] column_statistics

    cdef cppclass FileMetadata:
        uint16_t version
        uint64_t row_count
        uint8_t  file_uuid[16]
        uint64_t created_at_unix_us
        string   writer_tag
        vector[ColumnSchema]    columns
        vector[RowGroupSummary] row_groups

    cdef cppclass RowGroupMetadata:
        uint64_t row_count
        vector[ColumnMetadata] columns

    cdef cppclass ReadOptions:
        vector[string] columns

    Status c_footer_extent "skene::footer_extent"(const void* tail, size_t tail_bytes,
                                                  uint64_t file_bytes,
                                                  uint64_t* out_offset, uint64_t* out_bytes)
    Status c_read_metadata "skene::read_metadata"(const void* file, size_t file_bytes,
                                                  FileMetadata* out)
    Status c_read_row_group_metadata "skene::read_row_group_metadata"(
        const void* file, size_t file_bytes, uint32_t row_group, RowGroupMetadata* out)
    Status c_read_morsel "skene::read_morsel"(const void* file, size_t file_bytes,
                                              uint32_t row_group,
                                              const ReadOptions& options, CxxMorsel* out)


cdef extern from "skene/file_io.h" namespace "skene" nogil:
    Status c_write_file "skene::write_file"(const string& path,
                                            const vector[uint8_t]& bytes)


cdef extern from "skene/writer.h" namespace "skene" nogil:

    cdef enum SectionCodec "skene::SectionCodec":
        kCodecNone "skene::SectionCodec::kNone"
        kCodecZstd "skene::SectionCodec::kZstd"
        kCodecLz4  "skene::SectionCodec::kLz4"

    cdef cppclass WriteOptions:
        cbool read_acceleration
        SectionCodec codec
        int zstd_level
        vector[string] bloom_columns
        double bloom_false_positive_rate
        vector[uint32_t] field_ids
        uint8_t file_uuid[16]
        uint64_t created_at_unix_us
        string writer_tag

    cdef cppclass CFileWriter "skene::FileWriter":
        CFileWriter()
        Status begin(const WriteOptions& options, vector[uint8_t]* out)
        Status add_row_group(const CxxMorsel& morsel)
        Status finish()
        uint32_t row_group_count()

    Status c_write_morsel "skene::write_morsel"(const CxxMorsel& morsel,
                                                const WriteOptions& options,
                                                vector[uint8_t]* out)


cdef extern from "skene/migrate.h" namespace "skene" nogil:
    Status c_migrate_file "skene::migrate_file"(const void* file, size_t file_bytes,
                                                const WriteOptions& posture,
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

cdef dict _schema_to_dict(const ColumnSchema& c):
    """One column of the FILE footer's schema directory: identity and type only.

    The per-row-group facts (lengths, encoding shape, byte extents, zone maps,
    blooms) are deliberately absent — they vary per row group and live in
    read_row_group_metadata(), which costs a row group footer parse.
    """
    cdef dict out = {
        "name": c.name.decode("utf-8"),
        "field_id": c.field_id,
        "type": c.type,  # DrakenType tag
        "logical": None,
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
    cdef size_t i
    for i in range(c.children.size()):
        out["children"].append(_schema_to_dict(c.children[i]))
    return out


# skene format.h StatFlag bits this emitter reads by name rather than by number.
cdef enum:
    _K_STAT_NDV = 0x40        # kStatNdv       — `ndv` holds a distinct count
    _K_STAT_NDV_EXACT = 0x80  # kStatNdvExact  — ...and it is a BOUND, not a sketch
    _K_STAT_SKETCH = 0x100    # kStatSketch    — KMV min-hashes follow the struct


cdef dict _statistics_to_dict(const ColumnStatistics& s, const vector[uint64_t]& sketch):
    """One statistics blob as a plain dict.

    `ndv` is a v2 growth field. A v1 blob is a 48-byte PREFIX of the struct and
    the reader memcpy's only the bytes it was given, so `s.ndv` on a v1 blob is
    the zero-init and means nothing — kStatNdv is the only honest reader of it.
    Absent is NOT TRACKED, never 0 (draken's cardinal statistics rule), so an
    untracked NDV is spelled None.

    The value travels with WHICH FLAG produced it, because the two are not the
    same kind of number and a consumer must not have to guess:
      * ndv_exact True  — value ordering deduplicated the column, so the count
        is EXACT for this row group. A consumer needing a BOUND requires this.
      * ndv_exact False — the write-side KMV sketch measured it and ordering
        declined. An ESTIMATE, ~+/-3% at K=1024.
    """
    cdef object ndv = None
    cdef object ndv_exact = None
    if s.flags & _K_STAT_NDV:
        ndv = s.ndv
        ndv_exact = bool(s.flags & _K_STAT_NDV_EXACT)
    return {
        "flags": s.flags,
        "min_ordinal": s.min_ordinal,
        "max_ordinal": s.max_ordinal,
        "null_count": s.null_count,
        # int128 from little-endian int64 halves — as Python big-int math
        # (<object> casts; a C shift of an int64 by 64 is UB, not a widening).
        "sum": ((<object>s.sum_high) << 64) + ((<object>s.sum_low) & 0xFFFFFFFFFFFFFFFF),
        "ndv": ndv,
        "ndv_exact": ndv_exact,
        # The MERGEABLE form of the same fact: the K smallest value hashes,
        # ascending. None when untracked; a list (possibly shorter than K, which
        # means the column holds exactly that many distinct values) otherwise.
        # skene's own XXH3 dedup hashes — never mix with an ANALYZE/catalog
        # sketch, which is hashed differently (format.h, ColumnSketchHeader).
        "sketch": [sketch[i] for i in range(sketch.size())] if (s.flags & _K_STAT_SKETCH) else None,
    }


cdef dict _row_group_to_dict(const RowGroupSummary& g):
    cdef list stats = []
    cdef size_t i
    for i in range(g.column_statistics.size()):
        if g.column_statistics[i].present:
            stats.append(_statistics_to_dict(g.column_statistics[i].statistics,
                                             g.column_statistics[i].sketch))
        else:
            # Absent means NOT TRACKED, never zero — draken's cardinal
            # statistics rule. None is the only honest spelling of that.
            stats.append(None)
    return {
        "row_count": g.row_count,
        "first_row": g.first_row,
        "byte_offset": g.byte_offset,
        "byte_bytes": g.byte_bytes,
        "footer_offset": g.footer_offset,
        "footer_bytes": g.footer_bytes,
        # Depth-first over `columns`, ARRAY children included.
        "column_statistics": stats,
    }


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
        # ONE emitter for the blob (the row-group path uses the same call): a
        # second inline copy is how the two dict shapes drift apart on the next
        # growth field.
        out["statistics"] = _statistics_to_dict(c.statistics, c.sketch)
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
    """Parse the FILE footer only — cheap; touches no data region and no row
    group footer.

    Returns a dict: version, row_count (the file TOTAL), file_uuid,
    created_at_unix_us, writer_tag, columns (the schema: identity and type),
    and row_groups — each carrying its row count, its byte extents, and its
    per-column statistics.

    Those statistics are what makes this the pruning call: a reader decides
    which row groups it wants from this one read, then pays for only their
    directories via read_row_group_metadata().
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
        "columns": [_schema_to_dict(meta.columns[i]) for i in range(meta.columns.size())],
        "row_groups": [_row_group_to_dict(meta.row_groups[i])
                       for i in range(meta.row_groups.size())],
    }


def read_row_group_metadata(const unsigned char[::1] file not None, uint32_t row_group):
    """Parse ONE row group's own footer: per-column lengths, encoding shape,
    byte extents, zone maps and blooms.

    Separate from read_metadata() because it is the expensive half — a row group
    directory is tens of kilobytes on a wide schema, and a pruning reader should
    never pay it for a row group it has already excluded.
    """
    cdef RowGroupMetadata meta
    cdef Status st
    if file.shape[0] == 0:
        raise SkeneError("Truncated", "empty buffer")
    with nogil:
        st = c_read_row_group_metadata(<const void*>&file[0], <size_t>file.shape[0],
                                       row_group, &meta)
    _check(st)
    cdef size_t i
    return {
        "row_count": meta.row_count,
        "columns": [_column_to_dict(meta.columns[i]) for i in range(meta.columns.size())],
    }


def read_morsel(const unsigned char[::1] file not None, uint32_t row_group,
                columns=None):
    """Reconstruct ONE row group as a Morsel. `columns` narrows to those
    identities (a missing name is an error, not a silently absent column).

    `row_group` is required and has no default: a default of 0 would silently
    read one row group of a packed file and hand back a perfectly well-formed
    morsel holding a fraction of the data.
    """
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
                           row_group, options, sp.get())
    _check(st)
    return cxx_to_morsel(sp)



# ─── Write options ───────────────────────────────────────────────────────────

# One place that turns Python keywords into a WriteOptions, shared by the
# single-row-group write_morsel and the multi-row-group SkeneWriter — so the two
# cannot come to disagree about what a codec name or a level means.
cdef int _fill_write_options(WriteOptions* options, read_acceleration, codec,
                             zstd_level, bloom_columns, bloom_false_positive_rate,
                             field_ids, created_at_unix_us, writer_tag) except -1:
    cdef SectionCodec chosen

    if codec == "none":
        chosen = kCodecNone
    elif codec == "zstd":
        chosen = kCodecZstd
    elif codec == "lz4":
        chosen = kCodecLz4
    else:
        raise ValueError(
            f"unknown section codec {codec!r} — expected 'none', 'zstd' or 'lz4'"
        )

    options.read_acceleration = read_acceleration
    options.codec = chosen
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
    return 0


cdef class SkeneWriter:
    """Builds a .skene file of one or more row groups, a row group at a time.

    Streaming by construction: a caller decodes a row group, hands it over, and
    drops it. Only the output buffer and a few kilobytes of per-row-group
    metadata grow with the row group count — which is the difference between
    writing a 16-row-group file and holding sixteen wide morsels at once.

        writer = SkeneWriter(read_acceleration=True, codec="lz4")
        for morsel in row_groups:
            writer.add_row_group(morsel)
        writer.write_to("part-0000.skene")

    Every row group must share one schema; a divergent one is rejected rather
    than written into a file whose index does not describe it.
    """

    cdef CFileWriter* _writer
    cdef vector[uint8_t] _out
    cdef bint _finished

    def __cinit__(self, *, read_acceleration=False, codec="none", zstd_level=0,
                  bloom_columns=None, bloom_false_positive_rate=0.05,
                  field_ids=None, created_at_unix_us=0, writer_tag=""):
        cdef WriteOptions options
        _fill_write_options(&options, read_acceleration, codec, zstd_level,
                            bloom_columns, bloom_false_positive_rate, field_ids,
                            created_at_unix_us, writer_tag)
        self._writer = new CFileWriter()
        self._finished = False
        _check(self._writer.begin(options, &self._out))

    def __dealloc__(self):
        if self._writer != NULL:
            del self._writer
            self._writer = NULL

    def add_row_group(self, Morsel morsel not None):
        cdef shared_ptr[CxxMorsel] sp = morsel_to_cxx(morsel)
        cdef Status st
        with nogil:
            st = self._writer.add_row_group(deref(sp))
        _check(st)

    @property
    def row_group_count(self):
        return self._writer.row_group_count()

    @property
    def nbytes(self):
        """Bytes written so far. Meaningful before finish() as well as after —
        it is what a caller watches to decide a file is big enough."""
        return <object>self._out.size()

    cdef int _finish_once(self) except -1:
        cdef Status st
        if self._finished:
            raise SkeneError("Malformed", "this SkeneWriter is already finished")
        with nogil:
            st = self._writer.finish()
        _check(st)
        self._finished = True
        return 0

    def finish(self):
        """Complete the file and return it as bytes.

        This COPIES the whole image. Prefer write_to() for anything large — a
        packed file of a wide schema is hundreds of megabytes and this doubles
        the peak for no reason.
        """
        self._finish_once()
        return PyBytes_FromStringAndSize(<char*>self._out.data(),
                                         <Py_ssize_t>self._out.size())

    def write_to(self, str path not None):
        """Complete the file and write it to `path`, with no intermediate copy.

        Writes to a temporary alongside the target and renames, so a concurrent
        reader never observes a half-written file.
        """
        cdef string target = path.encode("utf-8")
        cdef Status st
        self._finish_once()
        with nogil:
            st = c_write_file(target, self._out)
        _check(st)
        return <object>self._out.size()


def write_morsel(Morsel morsel not None, *, read_acceleration=False,
                 codec="none", zstd_level=0,
                 bloom_columns=None, bloom_false_positive_rate=0.05,
                 field_ids=None, created_at_unix_us=0, writer_tag=""):
    """Serialize a draken Morsel to .skene bytes.

    read_acceleration=True enables value ordering + statistics + zone maps
    (the writer's for_storage posture); the default is the spill posture.

    codec selects the per-section general-purpose compressor: "none" (the
    default), "zstd" (ratio-first, and the only one zstd_level applies to), or
    "lz4" (read-first — ~70% of zstd's ratio decoding ~2.7x faster). A
    zstd_level given without codec="zstd" is an error rather than a silently
    ignored argument.
    """
    cdef WriteOptions options
    cdef vector[uint8_t] out
    cdef Status st

    _fill_write_options(&options, read_acceleration, codec, zstd_level,
                        bloom_columns, bloom_false_positive_rate, field_ids,
                        created_at_unix_us, writer_tag)
    cdef shared_ptr[CxxMorsel] sp = morsel_to_cxx(morsel)
    with nogil:
        st = c_write_morsel(deref(sp), options, &out)
    _check(st)
    return PyBytes_FromStringAndSize(<char*>out.data(), <Py_ssize_t>out.size())


def migrate(const unsigned char[::1] file not None, *,
            read_acceleration=True, codec="none", zstd_level=0):
    """Rewrite a version-(N-1) .skene file as version N, returning the bytes.

    One hop only, per the format's migration contract (format.h): a build
    migrates exactly its predecessor version. Provenance (file uuid, creation
    time, writer tag, field ids) is carried from the source file; the posture
    arguments here choose the REWRITE's encoding exactly as write_morsel's do.
    A file already at the current version is refused — there is nothing to
    migrate, and silently copying it would misreport what happened.
    """
    cdef WriteOptions options
    cdef vector[uint8_t] out
    cdef Status st

    _fill_write_options(&options, read_acceleration, codec, zstd_level,
                        None, 0.05, None, 0, "")
    with nogil:
        st = c_migrate_file(<const void*>&file[0], <size_t>file.shape[0],
                            options, &out)
    _check(st)
    return PyBytes_FromStringAndSize(<char*>out.data(), <Py_ssize_t>out.size())
