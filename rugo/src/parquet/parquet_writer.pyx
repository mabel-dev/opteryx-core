# cython: language_level=3
# distutils: language = c++
#
# Python/Cython edge for the rugo parquet writer.
#
# write_parquet(morsel) -> bytes
#
# Reads each column's DrakenVector in LOGICAL ROW ORDER (data[selection[i]]),
# gathers it into a contiguous logical-order buffer, and hands plain typed
# buffers + the (already logical-row-indexed) validity mask to the pure-C++
# encoder in _parquet_writer.hpp. The encoder knows nothing about draken — this
# is the only place vectors are touched, mirroring how the reader keeps decode
# independent of vector construction.
#
# Phase 1 scope: INT64, FLOAT64, BOOL, VARCHAR/NVARCHAR/VARBINARY; PLAIN,
# UNCOMPRESSED, single row group. Other physical types fail loud.

from libc.stdint cimport uint8_t, uint32_t, int8_t, int16_t, int32_t, int64_t
from libc.string cimport memcpy
from libcpp.string cimport string
from libcpp.vector cimport vector

from cpython.bytes cimport PyBytes_FromStringAndSize

from draken.core.buffers cimport (
    DrakenVector,
    DrakenType,
    DRAKEN_INT8,
    DRAKEN_INT16,
    DRAKEN_INT32,
    DRAKEN_INT64,
    DRAKEN_FLOAT32,
    DRAKEN_FLOAT64,
    DRAKEN_BOOL,
    DRAKEN_VARCHAR,
    DRAKEN_NVARCHAR,
    DRAKEN_VARBINARY,
    DRAKEN_VARIANT,
    DRAKEN_DATE32,
    DRAKEN_TIMESTAMP64,
    DRAKEN_TIME32,
    DRAKEN_TIME64,
    DRAKEN_INTERVAL,
    DRAKEN_DECIMAL,
    DRAKEN_DECIMAL128,
    DRAKEN_ARRAY,
    DRAKEN_NULL,
    DrakenStringArena,
    DrakenStringSlot,
    str_data,
    str_length,
)
from draken.morsels.morsel cimport Morsel
from draken.vectors.vector cimport Vector

cimport parquet_writer
from parquet_writer cimport (
    ColumnInput,
    ColumnStats,
    StrSlice,
    WriteParquet,
    PT_INT32,
    PT_INT64,
    PT_DOUBLE,
    PT_BOOLEAN,
    PT_BYTE_ARRAY,
    PT_FLBA,
    LK_NONE,
    LK_DATE,
    LK_TIMESTAMP,
    LK_DECIMAL,
    LK_TIME,
    LK_INTERVAL,
    TU_MILLIS,
    TU_MICROS,
    TU_NANOS,
    CODEC_UNCOMPRESSED,
    CODEC_ZSTD,
)


cdef inline void _put_u32le(uint8_t* dst, size_t off, uint32_t v) noexcept nogil:
    dst[off] = <uint8_t>(v & 0xFF)
    dst[off + 1] = <uint8_t>((v >> 8) & 0xFF)
    dst[off + 2] = <uint8_t>((v >> 16) & 0xFF)
    dst[off + 3] = <uint8_t>((v >> 24) & 0xFF)


cdef inline string _to_std_string(object name):
    """Coerce a column name (bytes or str) to a std::string."""
    cdef bytes b
    if isinstance(name, bytes):
        b = name
    elif isinstance(name, str):
        b = (<str>name).encode("utf-8")
    else:
        b = bytes(name)
    return string(<const char*>b, len(b))


def write_parquet(Morsel morsel not None, str compression="zstd",
                  bloom_filters=True, bint dictionary=True):
    """Serialize a Morsel to a parquet file (bytes).

    compression: "zstd" (default) or "none". Anything else raises ValueError.
    bloom_filters: True (default) writes split-block bloom filters for all
        equality-friendly columns (ints, strings/binary, date, timestamp,
        decimal); False writes none; an iterable of column names limits it to
        those. Float and bool columns never get a bloom filter.
    dictionary: True (default) dictionary-encodes eligible columns
        (VARCHAR/NVARCHAR/VARBINARY/VARIANT, INT8/16/32/64, FLOAT32/64, DATE32,
        TIMESTAMP64). A column arriving dict/constant-shaped keeps its existing
        dictionary (zero re-hash); a low-cardinality dense column is
        auto-dictionaried; otherwise PLAIN. False forces PLAIN everywhere.

    Supported column types: INT8/16/32/64, FLOAT64, BOOL, VARCHAR, NVARCHAR,
    VARBINARY, DATE32, TIMESTAMP64, DECIMAL, DECIMAL128, and all-null (NULL)
    columns. Any other physical type raises ValueError (fail loud).
    """
    return _encode(morsel, compression, False, bloom_filters, dictionary)[0]


def write_parquet_with_bounds(Morsel morsel not None, str compression="zstd",
                              bloom_filters=True, bint dictionary=True):
    """Like write_parquet, but also returns per-column min/max bounds.

    Returns (data_bytes, bounds) where bounds is {col_index: (min, max)} of
    typed Python values for bound-eligible plain columns (INT64/FLOAT64/BOOL/
    UTF8 string). Logical-typed and VARBINARY columns are omitted.
    """
    return _encode(morsel, compression, True, bloom_filters, dictionary)


cdef _encode(Morsel morsel, str compression, bint want_bounds, object bloom_filters,
             bint use_dict=True):
    cdef int codec
    # Resolve the bloom-filter request: all-eligible / none / a name set.
    cdef bint bloom_all = (bloom_filters is True)
    cdef set bloom_names
    cdef bint bloom_by_name = False
    if not bloom_all and bloom_filters and bloom_filters is not True:
        bloom_by_name = True
        bloom_names = set()
        for _bn in bloom_filters:
            bloom_names.add(_bn.decode("utf-8") if isinstance(_bn, bytes) else _bn)
    if compression == "zstd":
        codec = CODEC_ZSTD
    elif compression == "none" or compression == "uncompressed":
        codec = CODEC_UNCOMPRESSED
    else:
        raise ValueError(
            "write_parquet: compression must be 'zstd' or 'none', got %r"
            % (compression,)
        )

    cdef Py_ssize_t ncols = morsel._num_columns()
    if ncols == 0:
        raise ValueError("write_parquet: morsel has no columns")
    cdef Py_ssize_t nrows = morsel.num_rows
    cdef list names = morsel._col_names

    cdef vector[ColumnInput] cols
    cols.reserve(ncols)

    # Owning logical-order buffers. Reserved to ncols so push_back never
    # reallocates — the .data() pointers handed to ColumnInput stay valid for
    # the lifetime of the WriteParquet call below.
    cdef vector[vector[int32_t]] i32_store
    cdef vector[vector[int64_t]] i64_store
    cdef vector[vector[double]] f64_store
    cdef vector[vector[uint8_t]] bool_store
    cdef vector[vector[StrSlice]] str_store
    cdef vector[vector[uint8_t]] dec_store   # raw native-endian unscaled bytes
    cdef vector[vector[uint8_t]] null_store  # synthesized all-null validity masks
    cdef vector[vector[uint8_t]] level_store # array rep/def level buffers
    cdef vector[vector[uint32_t]] codes_store # per-row dict codes (preserve path)
    codes_store.reserve(ncols)
    i32_store.reserve(ncols)
    i64_store.reserve(ncols)
    f64_store.reserve(ncols)
    bool_store.reserve(ncols)
    str_store.reserve(ncols)
    dec_store.reserve(ncols)
    null_store.reserve(ncols)
    level_store.reserve(2 * ncols)

    # Per-column (physical type, is_utf8, logical kind) for bound decoding.
    cdef list kinds = []

    cdef Vector v
    cdef const DrakenVector* dv
    cdef const uint32_t* sel
    cdef DrakenType t
    cdef Py_ssize_t i, j
    cdef uint32_t p
    cdef ColumnInput ci

    cdef const int32_t* src32
    cdef const int64_t* src64
    cdef const double* srcf
    cdef const uint8_t* srcb
    cdef DrakenStringArena* arena
    cdef const DrakenStringSlot* slot
    cdef vector[int32_t] tmp32
    cdef vector[int64_t] tmp64
    cdef vector[double] tmpf
    cdef vector[uint8_t] tmpb
    cdef vector[StrSlice] tmps
    cdef vector[uint8_t] tmpdec
    cdef vector[uint8_t] tmpnull
    cdef vector[uint32_t] tmpc
    cdef StrSlice ss
    cdef bint did_preserve
    cdef bint dict_shape
    cdef uint32_t dict_n
    cdef int dec_w
    cdef long mul
    cdef int64_t months, us, days, millis
    cdef object unit, scale_obj, prec_obj
    # ARRAY locals
    cdef Vector child
    cdef const DrakenVector* cdv
    cdef const uint32_t* child_sel
    cdef const uint8_t* child_val
    cdef const int32_t* offs
    cdef DrakenStringArena* child_arena
    cdef const DrakenStringSlot* child_slot
    cdef DrakenType ct
    cdef int elem_kind
    cdef int32_t a_start, a_end
    cdef Py_ssize_t k
    cdef uint32_t cp
    cdef uint8_t rlev
    cdef vector[uint8_t] rep_v, def_v
    cdef vector[int64_t] elem_i64
    cdef vector[double] elem_f64
    cdef vector[uint8_t] elem_b
    cdef vector[StrSlice] elem_s

    # Keep every column Vector (and ARRAY child Vector) referenced until after
    # WriteParquet: StrSlice pointers and preserved dict buffers point into
    # vector-owned memory, and a rebound loop-local would free a child arena
    # mid-build (zeroing already-captured string element bytes).
    cdef list keepalive = []

    for i in range(ncols):
        v = morsel._get_column(i)
        keepalive.append(v)
        dv = v.unified()
        sel = dv.selection
        t = dv.type

        ci = ColumnInput()
        ci.name = _to_std_string(names[i])
        ci.validity = dv.validity

        # A compressed-shape vector (constant or true dict) already carries a
        # dictionary: `data` holds dict_n unique values and `selection` the
        # per-row codes. Eligible-type branches preserve it (set did_preserve);
        # the codes are stamped centrally after the type dispatch.
        did_preserve = False
        dict_n = dv.data_length
        dict_shape = use_dict and dict_n >= 1 and dict_n < <uint32_t>nrows

        if t == DRAKEN_INT64:
            tmp64 = vector[int64_t]()
            src64 = <const int64_t*>dv.data
            if dict_shape:
                tmp64.resize(dict_n)
                for j in range(dict_n):
                    tmp64[j] = src64[j]
                did_preserve = True
            else:
                tmp64.resize(nrows)
                for j in range(nrows):
                    tmp64[j] = src64[sel[j]]
                ci.dict_enabled = use_dict
            i64_store.push_back(tmp64)
            ci.type = PT_INT64
            ci.i64 = i64_store.back().data()

        elif t == DRAKEN_INT8 or t == DRAKEN_INT16 or t == DRAKEN_INT32:
            # Narrow integers widen losslessly to INT64.
            tmp64 = vector[int64_t]()
            if dict_shape:
                tmp64.resize(dict_n)
                if t == DRAKEN_INT32:
                    for j in range(dict_n):
                        tmp64[j] = (<const int32_t*>dv.data)[j]
                elif t == DRAKEN_INT16:
                    for j in range(dict_n):
                        tmp64[j] = (<const int16_t*>dv.data)[j]
                else:
                    for j in range(dict_n):
                        tmp64[j] = (<const int8_t*>dv.data)[j]
                did_preserve = True
            else:
                tmp64.resize(nrows)
                if t == DRAKEN_INT32:
                    for j in range(nrows):
                        tmp64[j] = (<const int32_t*>dv.data)[sel[j]]
                elif t == DRAKEN_INT16:
                    for j in range(nrows):
                        tmp64[j] = (<const int16_t*>dv.data)[sel[j]]
                else:
                    for j in range(nrows):
                        tmp64[j] = (<const int8_t*>dv.data)[sel[j]]
                ci.dict_enabled = use_dict
            i64_store.push_back(tmp64)
            ci.type = PT_INT64
            ci.i64 = i64_store.back().data()

        elif t == DRAKEN_FLOAT64:
            tmpf = vector[double]()
            srcf = <const double*>dv.data
            if dict_shape:
                tmpf.resize(dict_n)
                for j in range(dict_n):
                    tmpf[j] = srcf[j]
                did_preserve = True
            else:
                tmpf.resize(nrows)
                for j in range(nrows):
                    tmpf[j] = srcf[sel[j]]
                ci.dict_enabled = use_dict
            f64_store.push_back(tmpf)
            ci.type = PT_DOUBLE
            ci.f64 = f64_store.back().data()

        elif t == DRAKEN_BOOL:
            tmpb = vector[uint8_t]()
            tmpb.resize(nrows)
            srcb = <const uint8_t*>dv.data  # bit-packed, LSB-first, by physical idx
            for j in range(nrows):
                p = sel[j]
                tmpb[j] = (srcb[p >> 3] >> (p & 7)) & 1
            bool_store.push_back(tmpb)
            ci.type = PT_BOOLEAN
            ci.boolean = bool_store.back().data()

        elif (t == DRAKEN_VARCHAR or t == DRAKEN_NVARCHAR
              or t == DRAKEN_VARBINARY or t == DRAKEN_VARIANT):
            # VARIANT is German-string-backed JSON text -> written as a STRING.
            arena = <DrakenStringArena*>dv.data
            tmps = vector[StrSlice]()
            if dict_shape:
                tmps.resize(dict_n)
                for j in range(dict_n):
                    slot = &arena.slots[j]
                    ss.ptr = str_data(slot, arena.arena)
                    ss.len = str_length(slot)
                    tmps[j] = ss
                did_preserve = True
            else:
                tmps.resize(nrows)
                for j in range(nrows):
                    slot = &arena.slots[sel[j]]
                    ss.ptr = str_data(slot, arena.arena)
                    ss.len = str_length(slot)
                    tmps[j] = ss
                ci.dict_enabled = use_dict
            str_store.push_back(tmps)
            ci.type = PT_BYTE_ARRAY
            ci.is_utf8 = (t != DRAKEN_VARBINARY)  # VARIANT/VARCHAR/NVARCHAR -> UTF8
            ci.strs = str_store.back().data()

        elif t == DRAKEN_FLOAT32:
            # widen losslessly to DOUBLE (every float32 is exact in float64)
            tmpf = vector[double]()
            if dict_shape:
                tmpf.resize(dict_n)
                for j in range(dict_n):
                    tmpf[j] = (<const float*>dv.data)[j]
                did_preserve = True
            else:
                tmpf.resize(nrows)
                for j in range(nrows):
                    tmpf[j] = (<const float*>dv.data)[sel[j]]
                ci.dict_enabled = use_dict
            f64_store.push_back(tmpf)
            ci.type = PT_DOUBLE
            ci.f64 = f64_store.back().data()

        elif t == DRAKEN_TIME32 or t == DRAKEN_TIME64:
            unit = v._nb.logical_type_unit
            if unit is None:
                raise ValueError(
                    "write_parquet: TIME column %r missing logical-type unit"
                    % (names[i],))
            ci.logical = LK_TIME
            if t == DRAKEN_TIME32:
                # parquet TIME(INT32) is MILLIS only; seconds -> millis.
                mul = 1000 if unit == "s" else 1
                ci.ts_unit = TU_MILLIS
                tmp32 = vector[int32_t]()
                tmp32.resize(nrows)
                for j in range(nrows):
                    tmp32[j] = (<const int32_t*>dv.data)[sel[j]] * mul
                i32_store.push_back(tmp32)
                ci.type = PT_INT32
                ci.i32 = i32_store.back().data()
            else:
                ci.ts_unit = TU_NANOS if unit == "ns" else TU_MICROS
                tmp64 = vector[int64_t]()
                tmp64.resize(nrows)
                for j in range(nrows):
                    tmp64[j] = (<const int64_t*>dv.data)[sel[j]]
                i64_store.push_back(tmp64)
                ci.type = PT_INT64
                ci.i64 = i64_store.back().data()

        elif t == DRAKEN_INTERVAL:
            # draken slot = [int64 months][int64 us]; parquet INTERVAL =
            # FLBA(12) of 3 LE uint32 (months, days, millis). Sub-ms is dropped
            # (parquet INTERVAL is millisecond resolution).
            srcb = <const uint8_t*>dv.data  # reinterpret as int64 pairs below
            tmpdec = vector[uint8_t]()
            tmpdec.resize(<size_t>nrows * 12)
            for j in range(nrows):
                p = sel[j]
                months = (<const int64_t*>dv.data)[2 * p]
                us = (<const int64_t*>dv.data)[2 * p + 1]
                days = us // 86400000000
                millis = (us % 86400000000) // 1000
                _put_u32le(tmpdec.data(), <size_t>j * 12, <uint32_t>months)
                _put_u32le(tmpdec.data(), <size_t>j * 12 + 4, <uint32_t>days)
                _put_u32le(tmpdec.data(), <size_t>j * 12 + 8, <uint32_t>millis)
            dec_store.push_back(tmpdec)
            ci.type = PT_FLBA
            ci.logical = LK_INTERVAL
            ci.dec_width = 12
            ci.dec_raw = dec_store.back().data()

        elif t == DRAKEN_DATE32:
            tmp32 = vector[int32_t]()
            src32 = <const int32_t*>dv.data
            if dict_shape:
                tmp32.resize(dict_n)
                for j in range(dict_n):
                    tmp32[j] = src32[j]
                did_preserve = True
            else:
                tmp32.resize(nrows)
                for j in range(nrows):
                    tmp32[j] = src32[sel[j]]
                ci.dict_enabled = use_dict
            i32_store.push_back(tmp32)
            ci.type = PT_INT32
            ci.logical = LK_DATE
            ci.i32 = i32_store.back().data()

        elif t == DRAKEN_TIMESTAMP64:
            # Stored int64 is in the descriptor's unit. Map us/ms/ns to the
            # matching parquet TimeUnit (lossless); convert seconds -> micros.
            unit = v._nb.logical_type_unit
            if unit is None:
                raise ValueError(
                    "write_parquet: TIMESTAMP column %r missing logical-type "
                    "descriptor (unit)" % (names[i],))
            mul = 1
            if unit == "us":
                ci.ts_unit = TU_MICROS
            elif unit == "ms":
                ci.ts_unit = TU_MILLIS
            elif unit == "ns":
                ci.ts_unit = TU_NANOS
            elif unit == "s":
                ci.ts_unit = TU_MICROS
                mul = 1000000
            else:
                raise ValueError(
                    "write_parquet: unsupported timestamp unit %r for %r"
                    % (unit, names[i]))
            tmp64 = vector[int64_t]()
            src64 = <const int64_t*>dv.data
            if dict_shape:
                tmp64.resize(dict_n)
                for j in range(dict_n):
                    tmp64[j] = src64[j] * mul
                did_preserve = True
            else:
                tmp64.resize(nrows)
                for j in range(nrows):
                    tmp64[j] = src64[sel[j]] * mul
                ci.dict_enabled = use_dict
            i64_store.push_back(tmp64)
            ci.type = PT_INT64
            ci.logical = LK_TIMESTAMP
            ci.i64 = i64_store.back().data()

        elif t == DRAKEN_DECIMAL or t == DRAKEN_DECIMAL128:
            scale_obj = v._nb.logical_type_scale
            prec_obj = v._nb.logical_type_precision
            if scale_obj is None or prec_obj is None:
                raise ValueError(
                    "write_parquet: DECIMAL column %r missing logical-type "
                    "descriptor (scale/precision)" % (names[i],))
            dec_w = 8 if t == DRAKEN_DECIMAL else 16
            tmpdec = vector[uint8_t]()
            tmpdec.resize(<size_t>nrows * dec_w)
            srcb = <const uint8_t*>dv.data
            for j in range(nrows):
                memcpy(tmpdec.data() + <size_t>j * dec_w,
                       srcb + <size_t>sel[j] * dec_w, dec_w)
            dec_store.push_back(tmpdec)
            ci.type = PT_FLBA
            ci.logical = LK_DECIMAL
            ci.dec_width = dec_w
            ci.dec_scale = <int>scale_obj
            ci.dec_precision = <int>prec_obj
            ci.dec_raw = dec_store.back().data()

        elif t == DRAKEN_NULL:
            # Typeless all-null column: emit INT32 with an all-null validity
            # mask (no values written). The INT32 buffer is never read.
            tmpnull = vector[uint8_t]()
            tmpnull.resize((nrows + 7) // 8)  # zero-initialized => all null
            null_store.push_back(tmpnull)
            tmp32 = vector[int32_t]()
            tmp32.resize(nrows)
            i32_store.push_back(tmp32)
            ci.type = PT_INT32
            ci.validity = null_store.back().data()  # override: all-null
            ci.i32 = i32_store.back().data()

        elif t == DRAKEN_ARRAY:
            # LIST: 3-level parquet encoding. data = int32 offsets[len+1];
            # child via array_child. def: 0=null list, 1=empty, 2=null elem,
            # 3=present; rep: 0=new row, 1=continuation.
            offs = <const int32_t*>dv.data
            child = Vector(v._nb.array_child)
            keepalive.append(child)
            cdv = child.unified()
            ct = cdv.type
            child_sel = cdv.selection
            child_val = cdv.validity
            if ct in (DRAKEN_INT8, DRAKEN_INT16, DRAKEN_INT32, DRAKEN_INT64):
                elem_kind = 0
                ci.elem_type = PT_INT64
            elif ct == DRAKEN_FLOAT32 or ct == DRAKEN_FLOAT64:
                elem_kind = 1
                ci.elem_type = PT_DOUBLE
            elif ct == DRAKEN_BOOL:
                elem_kind = 2
                ci.elem_type = PT_BOOLEAN
            elif ct in (DRAKEN_VARCHAR, DRAKEN_NVARCHAR, DRAKEN_VARBINARY,
                        DRAKEN_VARIANT):
                elem_kind = 3
                ci.elem_type = PT_BYTE_ARRAY
                ci.elem_is_utf8 = (ct != DRAKEN_VARBINARY)
                child_arena = <DrakenStringArena*>cdv.data
            else:
                raise ValueError(
                    "write_parquet: unsupported ARRAY element type %d for %r "
                    "(supports int/float/bool/string elements)"
                    % (<int>ct, names[i]))
            rep_v = vector[uint8_t]()
            def_v = vector[uint8_t]()
            elem_i64 = vector[int64_t]()
            elem_f64 = vector[double]()
            elem_b = vector[uint8_t]()
            elem_s = vector[StrSlice]()
            for j in range(nrows):
                p = sel[j]
                if dv.validity != NULL and not ((dv.validity[p >> 3] >> (p & 7)) & 1):
                    rep_v.push_back(0); def_v.push_back(0)   # null list
                    continue
                a_start = offs[p]
                a_end = offs[p + 1]
                if a_start == a_end:
                    rep_v.push_back(0); def_v.push_back(1)   # empty list
                    continue
                for k in range(a_start, a_end):
                    rlev = 0 if k == a_start else 1  # ternary-in-push_back miscompiles
                    rep_v.push_back(rlev)
                    if child_val != NULL and not ((child_val[k >> 3] >> (k & 7)) & 1):
                        def_v.push_back(2)                   # null element
                        continue
                    def_v.push_back(3)                       # present element
                    cp = child_sel[k]
                    if elem_kind == 0:
                        if ct == DRAKEN_INT64:
                            elem_i64.push_back((<const int64_t*>cdv.data)[cp])
                        elif ct == DRAKEN_INT32:
                            elem_i64.push_back((<const int32_t*>cdv.data)[cp])
                        elif ct == DRAKEN_INT16:
                            elem_i64.push_back((<const int16_t*>cdv.data)[cp])
                        else:
                            elem_i64.push_back((<const int8_t*>cdv.data)[cp])
                    elif elem_kind == 1:
                        if ct == DRAKEN_FLOAT64:
                            elem_f64.push_back((<const double*>cdv.data)[cp])
                        else:
                            elem_f64.push_back((<const float*>cdv.data)[cp])
                    elif elem_kind == 2:
                        elem_b.push_back(((<const uint8_t*>cdv.data)[cp >> 3] >> (cp & 7)) & 1)
                    else:
                        child_slot = &child_arena.slots[cp]
                        ss.ptr = str_data(child_slot, child_arena.arena)
                        ss.len = str_length(child_slot)
                        elem_s.push_back(ss)
            level_store.push_back(rep_v)
            ci.rep_levels = level_store.back().data()
            level_store.push_back(def_v)
            ci.def_levels = level_store.back().data()
            ci.num_levels = def_v.size()
            ci.is_array = True
            ci.type = PT_BYTE_ARRAY  # placeholder; element type drives output
            if elem_kind == 0:
                i64_store.push_back(elem_i64); ci.i64 = i64_store.back().data()
                ci.num_elements = i64_store.back().size()
            elif elem_kind == 1:
                f64_store.push_back(elem_f64); ci.f64 = f64_store.back().data()
                ci.num_elements = f64_store.back().size()
            elif elem_kind == 2:
                bool_store.push_back(elem_b); ci.boolean = bool_store.back().data()
                ci.num_elements = bool_store.back().size()
            else:
                str_store.push_back(elem_s); ci.strs = str_store.back().data()
                ci.num_elements = str_store.back().size()

        else:
            raise ValueError(
                "write_parquet: unsupported column type %d for column %r "
                "(supports INT8/16/32/64, FLOAT32/64, BOOL, VARCHAR/NVARCHAR/"
                "VARBINARY/VARIANT, DATE32, TIME32/64, TIMESTAMP64, INTERVAL, "
                "DECIMAL/DECIMAL128, ARRAY of those, NULL; FP16 not yet)"
                % (<int>t, names[i])
            )

        # PRESERVE path: stamp the per-row dict codes (= the vector's selection)
        # and dictionary entry count. The typed buffer above holds dict_n values.
        if did_preserve:
            tmpc = vector[uint32_t]()
            tmpc.resize(nrows)
            for j in range(nrows):
                tmpc[j] = sel[j]
            codes_store.push_back(tmpc)
            ci.codes = codes_store.back().data()
            ci.dict_count = dict_n

        # Bloom filter: equality-friendly types only (skip FLOAT/BOOL/INTERVAL).
        if t in (DRAKEN_INT8, DRAKEN_INT16, DRAKEN_INT32, DRAKEN_INT64,
                 DRAKEN_DATE32, DRAKEN_TIMESTAMP64, DRAKEN_TIME32, DRAKEN_TIME64,
                 DRAKEN_DECIMAL, DRAKEN_DECIMAL128, DRAKEN_VARCHAR,
                 DRAKEN_NVARCHAR, DRAKEN_VARBINARY, DRAKEN_VARIANT):
            if bloom_all:
                ci.bloom = True
            elif bloom_by_name:
                nm = names[i]
                nm = nm.decode("utf-8") if isinstance(nm, bytes) else nm
                ci.bloom = nm in bloom_names

        kinds.append((<int>ci.type, bool(ci.is_utf8), <int>ci.logical))
        cols.push_back(ci)

    cdef vector[ColumnStats] stats
    cdef vector[uint8_t] out = WriteParquet(cols, <size_t>nrows, codec, 3, &stats)
    cdef bytes data = PyBytes_FromStringAndSize(<const char*>out.data(), out.size())
    if not want_bounds:
        return data, None
    return data, _decode_bounds(kinds, stats)


cdef object _decode_bounds(list kinds, vector[ColumnStats]& stats):
    """Build {col_index: (min_value, max_value)} of typed Python values for
    bound-eligible plain columns (INT64/FLOAT64/BOOL/UTF8 string). Logical-typed
    columns (date/timestamp/decimal), VARBINARY, and stat-less columns are
    skipped — matching the storage layer's bound contract."""
    import struct
    cdef dict bounds = {}
    cdef Py_ssize_t i
    cdef int ptype, logical
    cdef bint is_utf8
    cdef bytes mn, mx
    for i in range(len(kinds)):
        if not stats[i].has_minmax:
            continue
        ptype, is_utf8, logical = kinds[i]
        if logical != <int>LK_NONE:
            continue
        mn = PyBytes_FromStringAndSize(<const char*>stats[i].min_bytes.data(),
                                       stats[i].min_bytes.size())
        mx = PyBytes_FromStringAndSize(<const char*>stats[i].max_bytes.data(),
                                       stats[i].max_bytes.size())
        if ptype == <int>PT_INT64:
            bounds[i] = (int.from_bytes(mn, "little", signed=True),
                         int.from_bytes(mx, "little", signed=True))
        elif ptype == <int>PT_DOUBLE:
            bounds[i] = (struct.unpack("<d", mn)[0], struct.unpack("<d", mx)[0])
        elif ptype == <int>PT_BOOLEAN:
            bounds[i] = (mn[0] != 0, mx[0] != 0)
        elif ptype == <int>PT_BYTE_ARRAY and is_utf8:
            bounds[i] = (mn.decode("utf-8"), mx.decode("utf-8"))
    return bounds
