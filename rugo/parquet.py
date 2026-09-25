# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
rugo.parquet — unified read/write facade for Parquet.

A thin, dependency-free wrapper over the native reader/writer extensions
(`rugo.parquet_reader`, `rugo.parquet_writer`). It gives reading and writing a
single, symmetric surface that accepts either a filename or an in-memory
buffer, supports streaming iteration over row-group Morsels, and applies
predicate pushdown at the row-group level followed by row-level filtering on
surviving morsels.

    from rugo import parquet

    # read (streaming, projected, with row-group pruning + row-level filtering)
    with parquet.read_parquet("planets.parquet",
                              columns=["id", "name"],
                              predicates=[("id", ">", 4)]) as reader:
        for morsel in reader:
            ...

    # write
    data = parquet.write_parquet(morsel)            # -> bytes (ZSTD)
    with open("out.parquet", "wb") as f:
        f.write(data)

`predicates` are applied in two stages:
  1. ROW-GROUP elimination via footer min/max statistics, and bloom filter
     probing for equality predicates on file-backed sources.
  2. ROW-LEVEL filtering on each surviving morsel — only rows that satisfy
     all predicates are included in the yielded Morsel.
"""

import contextlib
import mmap
import struct
from typing import List, Optional, Sequence, Tuple, Union

import draken.draken_native as _draken_native
from rugo import rugo_native as _native

__all__ = [
    "read_parquet",
    "read_metadata",
    "read_metadata_from_memoryview",
    "write_parquet",
    "write_parquet_with_bounds",
    "patch_columns",
    "decode_value",
    "_make_scan_row_group",
]

# Re-export internals used by opteryx's parquet connector
from rugo.rugo_native import read_metadata_from_memoryview
# The written layout's shape (docs/PARQUET_GROUPED_COLUMN_MAJOR_DESIGN.md §3):
# 64k-row row groups in column-major blocks of 4. Defined once, in the native
# module, and re-exported here so the sinks size their batches from the same
# numbers the writer defaults to.
from rugo.rugo_native import DEFAULT_ROWS_PER_ROW_GROUP
from rugo.rugo_native import DEFAULT_ROW_GROUPS_PER_BLOCK
from rugo.rugo_native import DEFAULT_BLOCK_ROWS
from rugo.rugo_native import decode_value
from rugo.rugo_native import _make_scan_row_group

Source = Union[str, bytes, bytearray, memoryview]
Predicate = Tuple[str, str, object]

# op -> predicate that returns True when a row group [mn, mx] CANNOT match.
_EXCLUDE = {
    "=":      lambda v, mn, mx: v < mn or v > mx,
    "==":     lambda v, mn, mx: v < mn or v > mx,
    "!=":     lambda v, mn, mx: mn == mx == v,
    ">":      lambda v, mn, mx: mx <= v,
    ">=":     lambda v, mn, mx: mx < v,
    "<":      lambda v, mn, mx: mn >= v,
    "<=":     lambda v, mn, mx: mn > v,
    "in":     lambda v, mn, mx: not any(mn <= x <= mx for x in v),
    "not in": lambda v, mn, mx: mn == mx and mn in v,
}

# Row-level comparison: returns a Python callable (value, row_value) -> bool
_ROW_OP_CODE = {
    "=":  0, "==": 0,
    "!=": 1,
    ">":  2,
    ">=": 3,
    "<":  4,
    "<=": 5,
}


def _to_bytes(source: Source) -> bytes:
    if isinstance(source, str):
        with open(source, "rb") as f:
            return f.read()
    if isinstance(source, (bytes, bytearray, memoryview)):
        return bytes(source)
    raise TypeError("source must be a filename (str) or bytes/bytearray/memoryview")


@contextlib.contextmanager
def _mapped(path: str):
    """Map `path` read-only and yield a memoryview over it.

    `_row_group_mask` only ever reads the FOOTER, but `read_rowgroup_stats`
    takes a buffer spanning the whole file (parquet's footer offsets are
    absolute), so this used to slurp the entire file into a bytes object to
    reach its last few KB — and then hand the same path to
    stream_parquet_from_path, which mmaps it again to decode. On an 80 MB file
    that is 80 MB of resident Python heap before one row group is decoded, in a
    reader whose whole point is that peak memory tracks ONE row group.

    A mapping does no I/O for pages nobody touches, so the footer parse pages in
    the footer and leaves the rest alone. The view is released before the
    mapping closes — a live memoryview makes mmap.close() raise BufferError.
    """
    with open(path, "rb") as handle:
        mapping = mmap.mmap(handle.fileno(), 0, access=mmap.ACCESS_READ)
        try:
            view = memoryview(mapping)
            try:
                yield view
            finally:
                view.release()
        finally:
            mapping.close()


def _bloom_plain_encode(value) -> Optional[bytes]:
    """Encode a scalar value to its Parquet PLAIN bytes for bloom filter probing.
    Returns None if the type is not encodable (bloom probe is skipped)."""
    if isinstance(value, str):
        return value.encode("utf-8")
    if isinstance(value, bytes):
        return value
    if isinstance(value, int):
        # Python ints are unbounded; the writer hashed 8 little-endian bytes. A
        # value outside int64 is a real "not encodable" — it is not a value the
        # column can hold, so there is nothing to probe for — but it is a RANGE,
        # not a failure. struct.error fires at exactly these two bounds, so the
        # bounds say the same thing without routing control flow through an
        # exception (§9).
        if not (-(1 << 63) <= value < (1 << 63)):
            return None
        return struct.pack("<q", value)       # int64 little-endian
    if isinstance(value, float):
        return struct.pack("<d", value)       # float64 little-endian
    return None


def _column_name_str(col) -> str:
    """A predicate's column name as str.

    A caller may spell a predicate column either way, and the two stages key
    columns differently: footer statistics are keyed by str, Morsel columns by
    bytes. Each boundary normalises rather than assuming a spelling — a bytes
    name that missed the stats lookup silently reduced stage-1 pruning to a
    no-op (stage 2 still filtered, so the ANSWER stayed right and only the
    decode work grew, which is exactly the kind of loss nothing reports).
    """
    return col.decode("utf-8") if isinstance(col, bytes) else col


def _as_bytes(v):
    """A str as its UTF-8 bytes; anything else unchanged."""
    return v.encode("utf-8") if isinstance(v, str) else v


def _align_text_domain(value, mn, mx):
    """Return (value, mn, mx) with a str/bytes TEXT mismatch resolved to bytes.

    Either side can be spelled either way. `decode_value` hands back str for a
    STRING-annotated BYTE_ARRAY but bytes for an unannotated or non-printable
    one, and callers write literals both ways. A mixed pair raises TypeError
    inside the comparison, which the caller reads as "type mismatch — don't
    prune": safe, but it quietly abandoned pruning on an ordinary predicate.

    Resolving to bytes rather than str is not a coin toss. Encoding a str to
    UTF-8 always succeeds, where decoding arbitrary bytes does not, so this
    direction cannot turn a working predicate into an error. UTF-8 also orders
    bytewise exactly as Python orders code points, so every comparison here
    returns what it would have returned on the two strs — the ANSWER is
    unchanged, only its ability to run. It is the domain stage 2 already
    compares in.

    `in` / `not in` carry a COLLECTION rather than a scalar, and each member is
    aligned the same way: one bytes member among strs (or the reverse) made the
    whole row-group test raise, abandoning pruning for every member at once.
    A member that is not text is left exactly as it is — it still raises, and
    still lands in the caller's don't-prune guard.

    Bounds that are not a matching pair of text values are left alone, and so
    is a value that is neither text nor a collection.
    """
    if not (isinstance(mn, (str, bytes)) and type(mn) is type(mx)):
        return value, mn, mx

    if isinstance(value, (str, bytes)):
        if isinstance(value, type(mn)):
            return value, mn, mx
        return _as_bytes(value), _as_bytes(mn), _as_bytes(mx)

    if isinstance(value, (list, tuple, set, frozenset)):
        text = [v for v in value if isinstance(v, (str, bytes))]
        if all(isinstance(v, type(mn)) for v in text):
            return value, mn, mx
        return [_as_bytes(v) for v in value], _as_bytes(mn), _as_bytes(mx)

    return value, mn, mx


def _row_group_mask(data, path: Optional[str], predicates: Sequence[Predicate]) -> List[int]:
    """1 = keep, 0 = prune.

    `data` is any buffer spanning the whole file — bytes for a memory source, or
    the mapping `_mapped` yields for a file, which never materialises it.

    Two pruning stages:
      - Min/max statistics (all operators).
      - Bloom filter probing for == and 'in' on file-backed sources.
    A row group is pruned when ANY predicate proves it cannot match.
    """
    row_groups = _native.read_rowgroup_stats(data)
    mask: List[int] = [1] * len(row_groups)
    for rg_idx, rg in enumerate(row_groups):
        if mask[rg_idx] == 0:
            continue
        by_name = {c["name"]: c for c in rg["columns"]}
        for col, op, value in predicates:
            if op not in _NULL_OPS and op not in _EXCLUDE:
                raise ValueError(f"unsupported predicate operator: {op!r}")
            col_stats = by_name.get(_column_name_str(col))
            if col_stats is None:
                continue

            if op in _NULL_OPS:
                # null_count is -1 when the file did not record one. Absent means
                # "don't know", never "zero": pruning on a missing count would
                # discard row groups that do hold nulls.
                null_count = col_stats["null_count"]
                if null_count >= 0:
                    if op == "is null" and null_count == 0:
                        mask[rg_idx] = 0          # no null here to find
                        break
                    # For a NESTED leaf (max_repetition_level > 0) null_count
                    # counts null LEAF VALUES, not null rows: a NULL list, an
                    # EMPTY list and a null element inside a non-null list all
                    # add to it. So it shares no denominator with num_rows, and
                    # `null_count == num_rows` does not mean "every row is null"
                    # — it can coincide while non-null rows are present, and
                    # pruning then DROPS ROWS THAT MATCH. Measured on
                    # testdata/flat/null_lists/00002.parquet: 5 rows, leaf
                    # null_count 2, row-level IS NULL 1.
                    #
                    # The `is null` prune above is sound for nested columns and
                    # is kept: a null row always writes a null leaf entry, so
                    # null_count == 0 really does mean no null rows.
                    if (op == "is not null"
                            and col_stats["max_repetition_level"] == 0
                            and null_count == rg["num_rows"]):
                        mask[rg_idx] = 0          # every row is null
                        break
                continue

            excl = _EXCLUDE[op]
            # Min/max pruning
            if col_stats["min"] is not None and col_stats["max"] is not None:
                pt = col_stats["physical_type"].encode("utf-8")
                lt = col_stats["logical_type"].encode("utf-8")
                mn = _native.decode_value(pt, lt, col_stats["min"], True)
                mx = _native.decode_value(pt, lt, col_stats["max"], True)
                bound_value, mn, mx = _align_text_domain(value, mn, mx)
                try:
                    if excl(bound_value, mn, mx):
                        mask[rg_idx] = 0
                        break
                except TypeError:
                    pass  # type mismatch — don't prune
            # Bloom filter pruning (equality only, file-backed)
            if mask[rg_idx] and path is not None and op in ("=", "==", "in"):
                # A column with NO bloom filter is reported as offset -1
                # (ColumnStats::bloom_offset's initialiser in metadata.hpp), and
                # read_rowgroup_stats passes that sentinel through verbatim — the
                # key is always present, so it is never None. `is not None`
                # therefore let every bloom-less column reach the probe, which
                # raised ValueError("offset must be non-negative"), and a blanket
                # `except Exception` turned that entirely ordinary case into
                # "might match". The catch was load-bearing for the common file,
                # and it swallowed every REAL failure with it — a truncated
                # bitset, an unsupported filter type or hash, an unreadable file
                # (bloom_filter.cpp throws for each) — so pruning could stop
                # working and the only symptom was a slower query.
                #
                # "No filter to probe" is a VALUE, and it is tested here as one.
                # Everything the probe raises is a genuine failure and is left to
                # propagate.
                bloom_offset = col_stats["bloom_offset"]
                if bloom_offset >= 0:
                    bloom_length = col_stats["bloom_length"]
                    candidates = value if op == "in" else [value]
                    # Prune only if NONE of the candidates could be present
                    any_maybe = False
                    for candidate in candidates:
                        encoded = _bloom_plain_encode(candidate)
                        if encoded is None:
                            any_maybe = True  # can't encode → can't prune
                            break
                        if _native.bloom_filter_maybe_contains(
                            path, bloom_offset, bloom_length, encoded
                        ):
                            any_maybe = True
                            break
                    if not any_maybe:
                        mask[rg_idx] = 0
    return mask


def _predicate_column_names(predicates: Sequence[Predicate]) -> List[str]:
    """Column names referenced by `predicates`, as str, in first-seen order."""
    names: List[str] = []
    for col, _op, _value in predicates:
        name = _column_name_str(col)
        if name not in names:
            names.append(name)
    return names


_MEMBERSHIP_OPS = ("in", "not in")

# Null tests carry no value to compare against — they read the validity bitmap,
# at both stages. Kept out of _EXCLUDE and _ROW_OP_CODE because neither a
# min/max bound nor a comparison kernel can answer them.
_NULL_OPS = ("is null", "is not null")

# A BOOLEAN column needs no comparison kernel — the column IS the mask.
#
# ⚠ This table PREDATES the kernel it was working around, and is no longer
# load-bearing. Draken registered only `hash` and `ordinalize` for DRAKEN_BOOL,
# so draken_compare_scalar refused a bool column outright and every predicate
# here died with "unsupported type" — but only AFTER row-group min/max pruning
# had run, which is why `> True` and `< False` looked supported: pruning
# eliminated every row group before the row-level filter was reached, so those
# two returned 0 rows without ever touching the kernel.
#
# `entries[DRAKEN_BOOL].compare_scalar` is registered as of 2026-09-16
# (draken/ops/bool_compare.h), so _scalar_mask's ordinary `_compare_scalar`
# path now answers all six ops on a bool column directly. Measured equivalent
# to this table on testdata/flat/formats/parquet/tweets.parquet (user_verified,
# 711 TRUE / 99289 FALSE): identical masks for all 14 (op, value) pairs. This
# table is kept only because collapsing it belongs to the rugo facade's own
# work, not to draken's — it is redundancy, not a fallback.
#
# Each (op, value) resolves to one of four shapes, with FALSE < TRUE as SQL
# orders booleans:
#   "true"  rows that are TRUE            -> the column itself
#   "false" rows that are FALSE           -> its complement
#   "any"   any NON-NULL row              -> TRUE or FALSE
#   "none"  no row at all
# Nulls need no special case: the column carries its own validity, complement
# and OR preserve it, and filter_mask keeps only rows that are valid AND true —
# so a null row satisfies no predicate here either, matching every other type.
_BOOL_MASK_RULE = {
    ("=",  True):  "true",  ("==", True):  "true",
    ("=",  False): "false", ("==", False): "false",
    ("!=", True):  "false", ("!=", False): "true",
    (">",  False): "true",  (">",  True):  "none",   # nothing exceeds TRUE
    (">=", True):  "true",  (">=", False): "any",    # every bool is >= FALSE
    ("<",  True):  "false", ("<",  False): "none",   # nothing precedes FALSE
    ("<=", False): "false", ("<=", True):  "any",
}


def _bool_mask(vec, op, value):
    """DRAKEN_BOOL mask for `vec op value` where vec is a BOOLEAN column."""
    from draken.vectors.bool_vector import BoolVector

    column = BoolVector(vec._nb)
    rule = _BOOL_MASK_RULE[(op, value)]
    if rule == "true":
        return column
    if rule == "false":
        return column.not_vector()
    if rule == "any":
        return column.or_vector(column.not_vector())
    return BoolVector.from_constant(False, vec.length)


def _scalar_mask(vec, op, value):
    """DRAKEN_BOOL mask for one scalar comparison, by column type."""
    if vec.type == _draken_native.DrakenType.BOOL:
        return _bool_mask(vec, op, value)
    # compare_scalar expects bytes for string columns
    scalar = value.encode() if isinstance(value, str) else value
    return vec._compare_scalar(scalar, _ROW_OP_CODE[op])



def _membership_mask(vec, values, negate: bool):
    """DRAKEN_BOOL mask for `in` / `not in`: an OR of exact equality masks.

    Draken exposes a single-pass `Vector.in_list()`, and it is NOT used here.
    Its own binding says why: CarcharSet stores 64-bit hashes with no key
    verification, so a collision can admit a row that is not in the list. Stage
    2 is this facade's EXACT stage — the stage row-group pruning is allowed to
    be sloppy precisely because this one is not — so it is built from the same
    verified compare kernel the scalar operators use. Every surviving row really
    does equal a member.

    The engine's own IN lowering gets both exactness and a single pass by
    binary-searching a sorted payload blob, but that kernel is reached through
    opteryx's packer, and rugo is opteryx-free. So the cost here is one native
    pass per member: fine for the handful of values a pushdown predicate
    carries, and worth watching if a caller ever pushes a very large list.

    Nulls follow the compare kernel's own 3VL: a null row is null in every
    member's mask, stays null through OR and NOT, and `filter_mask` keeps only
    rows that are valid AND true — so a null row satisfies neither `in` nor
    `not in`, which is what SQL says.
    """
    from draken.vectors.bool_vector import BoolVector

    mask = None
    for member in values:
        member_mask = _scalar_mask(vec, "=", member)
        mask = member_mask if mask is None else mask.or_vector(member_mask)

    if mask is None:
        # `x IN ()` matches nothing and `x NOT IN ()` matches everything —
        # including null rows, since an empty list raises no 3VL question.
        # Stage 1 agrees: _EXCLUDE["in"] prunes every row group for an empty
        # list, and _EXCLUDE["not in"] prunes none.
        return BoolVector.from_constant(negate, vec.length)

    return mask.not_vector() if negate else mask


def _row_filter(morsel, predicates: Sequence[Predicate]):
    """Apply row-level predicates to a Morsel via native compare kernels.

    Each predicate produces a DRAKEN_BOOL vector via Vector._compare_scalar()
    (which calls into the C++ compare kernel — no Python loop over rows);
    `in` / `not in` reduce to the same kernel through _membership_mask.
    Multi-predicate masks are reduced with and_vector(), also native.

    Every predicate column MUST be present in the morsel. _ParquetReader.__iter__
    guarantees that by reading the predicate columns whether or not the caller
    projected them; a column missing here is one that is not in the FILE, and
    the only honest answer is to fail. Skipping it (the old behaviour) dropped
    the predicate and returned a plausible-looking UNFILTERED result.
    """
    col_names = list(morsel.column_names)
    mask = None

    for col, op, value in predicates:
        if (op not in _MEMBERSHIP_OPS and op not in _ROW_OP_CODE
                and op not in _NULL_OPS):
            raise ValueError(f"unsupported predicate operator: {op!r}")
        col_bytes = col.encode() if isinstance(col, str) else col
        if col_bytes not in col_names:
            raise ValueError(
                "predicate column %r is not in this parquet file; "
                "columns present are %r"
                % (col, [n.decode("utf-8") for n in col_names])
            )

        vec = morsel.column(col_bytes)
        if op in _NULL_OPS:
            pred_mask = (vec.is_null_mask() if op == "is null"
                         else vec.is_not_null_mask())
        elif op in _MEMBERSHIP_OPS:
            pred_mask = _membership_mask(vec, value, negate=(op == "not in"))
        else:
            pred_mask = _scalar_mask(vec, op, value)

        if mask is None:
            mask = pred_mask
        else:
            mask = mask.and_vector(pred_mask)

    if mask is None:
        return morsel
    return morsel.filter_mask(mask)


def _parse_timestamp_unit(logical_type: str) -> Optional[str]:
    """Return the draken unit string ("ms"/"us"/"ns") if `logical_type` denotes a
    Parquet TIMESTAMP column, else None. Handles both the modern LogicalType
    annotation ("timestamp[ms]" / "timestamp[us,UTC]") and the legacy
    ConvertedType spelling ("TIMESTAMP_MILLIS" / "TIMESTAMP_MICROS")."""
    if logical_type.startswith("timestamp["):
        unit = logical_type[len("timestamp["):].split(",", 1)[0].rstrip("]")
        return unit if unit in ("s", "ms", "us", "ns") else None
    if logical_type == "TIMESTAMP_MILLIS":
        return "ms"
    if logical_type == "TIMESTAMP_MICROS":
        return "us"
    return None


def _is_date_logical_type(logical_type: str) -> bool:
    """True if `logical_type` denotes a Parquet DATE column. Handles both the
    modern LogicalType annotation ("date32[day]") and the legacy ConvertedType
    spelling ("DATE")."""
    return logical_type == "date32[day]" or logical_type == "DATE"


# Logical types whose predicate values are checked before pruning, by the value
# domain they accept. An ALLOWLIST: a column whose type is not named here
# (temporal, decimal, fixed-width binary, nested, ...) has its own accepted value
# forms and is left to the compare kernel.
_NUMERIC_LOGICAL_TYPES = frozenset((
    "int8", "int16", "int32", "int64",
    "uint8", "uint16", "uint32", "uint64",
    "float32", "float64",
))
_TEXT_LOGICAL_TYPES = frozenset(("varchar", "binary", "enum"))


def _schema_column_maps(source: Source):
    """({timestamp_col: unit_str}, {date_col}, {col: value_domain}) — column
    names (bytes) keyed by their Parquet schema annotation, read once per
    _ParquetReader from the footer metadata.

    The domain map ("boolean" / "numeric" / "text") is not used for coercion;
    it is what lets a predicate value be type-checked BEFORE row groups are
    pruned (see _check_predicate_values). Checking it later would be too late:
    `b = 5` prunes every row group on min/max, and `a = 'x'` on an int column
    prunes every row group on its bloom filter — both return zero rows without
    the row filter ever running, so an invalid predicate would answer "no
    rows" instead of failing.
    """
    meta = read_metadata(source)
    units = {}
    dates = set()
    domains = {}
    for col in meta.schema_columns:
        name = col.name.encode("utf-8")
        unit = _parse_timestamp_unit(col.logical_type)
        if unit is not None:
            units[name] = unit
        elif _is_date_logical_type(col.logical_type):
            dates.add(name)
        elif col.physical_type == "boolean":
            domains[name] = "boolean"
        elif col.logical_type in _NUMERIC_LOGICAL_TYPES:
            domains[name] = "numeric"
        elif col.physical_type == "byte_array" and col.logical_type in _TEXT_LOGICAL_TYPES:
            domains[name] = "text"
    return units, dates, domains


def _check_predicate_values(predicates: Sequence[Predicate], domains) -> None:
    """Reject predicate values no comparison can answer, BEFORE row groups prune.

    Two classes, both of which would otherwise answer "no rows" rather than
    fail — `b = 5` prunes every row group on min/max, and the row filter never
    runs to object:

    None — SQL's `x = NULL` is UNKNOWN for every row, so a comparison against it
    can only ever match nothing. Passed through it reached the compare kernel as
    a raw TypeError. `is null` / `is not null` are how a null test is spelled.

    A value outside the column's domain — `bool(value)` would quietly make
    `b = "false"` mean TRUE on a BOOLEAN column, and a bare int would make
    `b = 5` mean something the column cannot hold. A str against a numeric
    column (or a number against a text one) has no comparison at all: the bloom
    probe hashes it, finds nothing, and prunes the whole file to zero rows —
    and on a memory source, where there is no bloom stage, the compare kernel
    failed with a bare `std::bad_cast`. bool is an int subclass, so `a = True`
    would quietly mean `a = 1` on a numeric column; it is refused there for the
    same reason an int is refused on a BOOLEAN one. None of these is a guess
    worth making for the caller.
    """
    for col, op, value in predicates:
        if op in _NULL_OPS:
            continue
        if op in _MEMBERSHIP_OPS and not isinstance(value, (list, tuple, set, frozenset)):
            # Iterating a bare str/bytes walks its CHARACTERS: `s in "xy"` ran as
            # `s in ['x', 'y']` and answered rows that were never asked for.
            raise ValueError(
                "predicate %r on %r takes a list, tuple or set of values, got %s"
                % (op, col, type(value).__name__)
            )
        name = col.encode("utf-8") if isinstance(col, str) else col
        domain = domains.get(name)
        for member in (value if op in _MEMBERSHIP_OPS else [value]):
            if member is None:
                raise ValueError(
                    "predicate %r on %r cannot take None — a comparison with "
                    "NULL matches no row; use 'is null' / 'is not null'"
                    % (op, col)
                )
            if domain == "boolean" and not isinstance(member, bool):
                raise ValueError(
                    "predicate on BOOLEAN column %r needs True or False, "
                    "got %r (%s)" % (col, member, type(member).__name__)
                )
            if domain == "numeric" and (isinstance(member, bool)
                                        or not isinstance(member, (int, float))):
                raise ValueError(
                    "predicate on numeric column %r needs an int or float, "
                    "got %r (%s)" % (col, member, type(member).__name__)
                )
            if domain == "text" and not isinstance(member, (str, bytes)):
                raise ValueError(
                    "predicate on text column %r needs a str or bytes, "
                    "got %r (%s)" % (col, member, type(member).__name__)
                )


def _coerce_temporal_columns(morsel, unit_map: dict, date_set: set):
    """Retag/reinterpret INT64 columns that carry a Parquet TIMESTAMP or DATE
    annotation to DRAKEN_TIMESTAMP64/DATE32, mirroring the schema-driven
    coercion the SQL engine's own scan applies
    (opteryx/operators/parquet_read/parquet_read.pyx). The IPC/direct decode
    paths serialise DATE/TIMESTAMP as their bare physical INT64 stream — the
    logical type never crosses the wire — so this reinterpret has to happen
    here, against the file's own schema, once per morsel."""
    if not unit_map and not date_set:
        return morsel
    from draken.morsels import Morsel

    names = list(morsel.column_names)
    vectors = []
    changed = False
    for name in names:
        v_nb = morsel.column(name)._nb
        # TIMESTAMP is physical int64; DATE is physical int32 and decodes at that
        # width, so the date branch must accept INT32 as well as INT64.
        if v_nb.type == _draken_native.DrakenType.INT64:
            unit = unit_map.get(name)
            if unit is not None:
                v_nb = _draken_native.vector_retag_int64_as_timestamp64(v_nb, unit)
                changed = True
            elif name in date_set:
                v_nb = _draken_native.vector_reinterpret_as_date32(v_nb)
                changed = True
        elif v_nb.type == _draken_native.DrakenType.INT32 and name in date_set:
            v_nb = _draken_native.vector_reinterpret_as_date32(v_nb)
            changed = True
        vectors.append(v_nb)
    if not changed:
        return morsel
    return Morsel.from_vectors(names, vectors)


class _ParquetReader:
    """Context-managed, streaming reader over row-group Morsels.

    Decode is performed lazily on iteration. For file sources, row groups are
    pruned by footer statistics and bloom filters before decoding. Each
    surviving morsel is then filtered at the row level.
    """

    def __init__(self, source: Source, columns, predicates):
        self._path = source if isinstance(source, str) else None
        self._data = None if self._path else _to_bytes(source)
        # Normalise the projection to str at the boundary: the native reader
        # stringifies whatever it is handed, so a bytes name arrived as the
        # literal "b'x'", matched no column, and yielded a ZERO-COLUMN morsel
        # instead of failing.
        self._columns = ([_column_name_str(c) for c in columns]
                         if columns is not None else None)
        self._predicates = list(predicates) if predicates else None
        # A predicate on a column the caller did NOT project still has to be
        # evaluated: "project narrow, filter on something else" is an ordinary
        # access pattern. So read the union (projection + every predicate
        # column) and project back down to `_columns` after filtering — see
        # __iter__. `_columns is None` already reads everything.
        self._read_columns = self._columns
        self._reproject = False
        if self._columns is not None and self._predicates:
            extra = [c for c in _predicate_column_names(self._predicates)
                     if c not in self._columns]
            if extra:
                self._read_columns = self._columns + extra
                self._reproject = True

    def __enter__(self) -> "_ParquetReader":
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def __iter__(self):
        # stream_parquet[_from_path] decode and yield one row group at a time
        # (DecodeRowGroupColumns), unlike read_parquet[_from_path] which decode
        # and retain every row group of the file before returning anything —
        # see decode.hpp's DecodeRowGroupColumns docstring. This is the actual
        # streaming implementation behind this class's own "Decode is performed
        # lazily on iteration" claim above.
        source = self._path if self._path is not None else self._data
        unit_map, date_set, domains = _schema_column_maps(source)
        if self._predicates:
            # Before pruning: a row group mask built from an invalid predicate
            # can empty the file and never reach the row filter.
            _check_predicate_values(self._predicates, domains)

        if self._path is not None:
            if self._predicates:
                # Footer only — mapped, never read into the heap (see _mapped).
                with _mapped(self._path) as mapped:
                    mask = _row_group_mask(mapped, self._path, self._predicates)
            else:
                mask = None
            morsels = _native.stream_parquet_from_path(
                self._path, column_names=self._read_columns, row_group_mask=mask
            )
        else:
            mask = (_row_group_mask(self._data, None, self._predicates)
                    if self._predicates else None)
            morsels = _native.stream_parquet(
                self._data, column_names=self._read_columns, row_group_mask=mask
            )

        for morsel in morsels:
            morsel = _coerce_temporal_columns(morsel, unit_map, date_set)
            if self._predicates:
                morsel = _row_filter(morsel, self._predicates)
                if self._reproject:
                    # Drop the predicate-only columns the caller never asked
                    # for, restoring the requested projection and its order.
                    morsel = morsel.select(self._columns)
            if morsel is not None:
                yield morsel


def read_parquet(
    source: Source,
    columns: Optional[Sequence[str]] = None,
    predicates: Optional[Sequence[Predicate]] = None,
) -> _ParquetReader:
    """Open a Parquet file or buffer for streaming reads.

    Args:
        source: filename (str) OR bytes/bytearray/memoryview of the whole file.
        columns: column names to project, or None for all.
        predicates: list of (column, op, value) tuples.
            Stage 1 — ROW-GROUP pruning via footer min/max statistics and bloom
            filters (equality ops on file sources). Coarse: whole row groups.
            Stage 2 — ROW-LEVEL filtering on each surviving morsel. Exact.
            Ops: =, ==, !=, <, <=, >, >=, in, not in, is null, is not null.
            `in` / `not in` take a collection; `is null` / `is not null` take
            no value (pass None); every other op takes a scalar.
            Comparisons follow SQL three-valued logic, so a NULL row satisfies
            no predicate — not even `!=` or `not in`. An empty `in` collection
            matches nothing and an empty `not in` matches everything, null rows
            included, since neither asks a comparison.

    Column names may be str or bytes in both `columns` and `predicates`.
    A predicate column need NOT appear in `columns`: it is read internally and
    projected away after filtering. A predicate on a column that is not in the
    FILE raises — it cannot be evaluated, and answering unfiltered would be a
    wrong answer dressed as a right one.

    Returns a context manager that yields one Morsel per surviving row group.
    A row group PRUNED at stage 1 is never decoded and yields no Morsel at all.
    A row group that survives pruning but whose every row is rejected at stage 2
    yields an EMPTY (zero-row) Morsel — it is NOT skipped. Iterating rows sees
    the same thing either way, but a caller counting morsels must not read one
    as "a row group that matched".
    """
    return _ParquetReader(source, columns, predicates)


def read_metadata(source: Source):
    """Return ParquetMetadata (num_rows, schema_columns) for a file or buffer."""
    if isinstance(source, str):
        return _native.read_metadata(source)
    return _native.read_metadata_from_bytes(_to_bytes(source))


def write_parquet(morsel, compression: str = "zstd", bloom_filters=True,
                  dictionary: bool = True,
                  max_rows_per_row_group: int = DEFAULT_ROWS_PER_ROW_GROUP,
                  max_page_bytes: int = 0,
                  sorted_by=None, sorted_descending: bool = False,
                  profile: str = "fast", page_index: bool = True,
                  row_groups_per_block: int = DEFAULT_ROW_GROUPS_PER_BLOCK) -> bytes:
    """Serialize a Morsel to Parquet bytes.

    compression: "zstd" (default) or "none".
    profile: "fast" (default) or "storage" — how hard to compress. The zstd
        level is not a caller knob; it is chosen per column from the column's
        physical type, because only BYTE_ARRAY columns respond to it. "fast"
        suits CTAS and uploads; "storage" raises only the string level and is
        for the defragmenter. Requires compression="zstd".
    bloom_filters: True (all equality-friendly columns), False, or an iterable
        of column names. Split-block bloom filters; floats/bools are excluded.
    dictionary: True (default) dictionary-encodes eligible columns; False
        forces PLAIN everywhere.
    max_rows_per_row_group: maximum rows per row group (default 2^16 = 65536,
        the engine's measured best morsel size). Pass 0 to write a single row
        group regardless of size.
    row_groups_per_block: row groups per column-major BLOCK (default 4, so a
        block is 262144 rows). Within a block every column's chunks for the
        block's row groups are byte-adjacent, columns in schema order, so a
        reader projecting a column over the block fetches ONE range instead
        of one per row group; the last block of a file may be partial. 1 =
        conventional row-major placement. Bloom filters always go in the
        file tail after the last block, column-major over the whole file.
        The row group stays the unit of decode, statistics and pruning. See
        docs/PARQUET_GROUPED_COLUMN_MAJOR_DESIGN.md.
    max_page_bytes: split each column chunk into multiple data pages once its
        estimated size exceeds this many bytes (default 0 = single page per
        chunk). Independent per column. Dictionary-encoded chunks split on the
        same row grid, behind one shared dictionary page.
    page_index: True (default) writes a PageIndex (ColumnIndex + OffsetIndex)
        in the file tail so a reader with a pushed predicate can skip whole
        data pages — and, remotely, not fetch them. Only has an effect when
        max_page_bytes > 0: over a single-page chunk the index would restate
        what the footer statistics already say.
    sorted_by: name of a column the CALLER asserts is already ordered within
        every row group of this morsel (e.g. a clustering key merged from
        pre-sorted runs). Written verbatim into each row group's parquet
        sorting_columns field — rugo does NOT verify it; an untrue hint is a
        correctness bug in the caller. A reader only trusts this claim back
        from a file whose created_by identifies rugo as the writer: the check
        is a substring search for "rugo", so BOTH spellings below pass it.

        On created_by: rugo is built into two distributions, which stamp
        different text into the footer —

            opteryx_core wheel : "opteryx-rugo version <opteryx ver> (build <n>)"
            standalone rugo    : "rugo version <rugo ver>"

        Both are rugo, and the versions are NOT comparable: the bundled build
        carries the opteryx_core version because that is what it is released
        as, while ``rugo.__version__`` always reports the standalone rugo
        source version. To identify the writer from Python, read
        ``rugo.__writer_id__`` — the exact string this build stamps — and
        ``rugo.__distribution__``, which says which of the two you have.
    sorted_descending: sort direction for sorted_by (default ascending).
        nulls_first is implied (True for ascending, False for descending).
    """
    return _native.write_parquet(morsel, compression=compression,
                                 bloom_filters=bloom_filters,
                                 dictionary=dictionary,
                                 max_rows_per_row_group=max_rows_per_row_group,
                                 max_page_bytes=max_page_bytes,
                                 sorted_by=sorted_by,
                                 sorted_descending=sorted_descending,
                                 profile=profile, page_index=page_index,
                                 row_groups_per_block=row_groups_per_block)


def write_parquet_with_bounds(morsel, compression: str = "zstd", bloom_filters=True,
                              dictionary: bool = True,
                              max_rows_per_row_group: int = DEFAULT_ROWS_PER_ROW_GROUP,
                              max_page_bytes: int = 0,
                              sorted_by=None, sorted_descending: bool = False,
                              profile: str = "fast", page_index: bool = True,
                              row_groups_per_block: int = DEFAULT_ROW_GROUPS_PER_BLOCK):
    """Like write_parquet but also returns {col_index: (min, max)} bounds.

    The bounds span the whole file (every row group).
    sorted_by / sorted_descending / profile / row_groups_per_block: see
    write_parquet.
    """
    return _native.write_parquet_with_bounds(morsel, compression=compression,
                                             bloom_filters=bloom_filters,
                                             dictionary=dictionary,
                                             max_rows_per_row_group=max_rows_per_row_group,
                                             max_page_bytes=max_page_bytes,
                                             sorted_by=sorted_by,
                                             sorted_descending=sorted_descending,
                                             profile=profile, page_index=page_index,
                                             row_groups_per_block=row_groups_per_block)


def open_parquet_writer(sink, compression: str = "zstd", bloom_filters=True,
                        dictionary: bool = True, max_page_bytes: int = 0,
                        sorted_by=None, sorted_descending: bool = False,
                        profile: str = "fast", page_index: bool = True,
                        row_groups_per_block: int = DEFAULT_ROW_GROUPS_PER_BLOCK):
    """Open a streaming, bounded-memory Parquet writer.

    Unlike write_parquet (whole morsel in, whole file out), this writes one row
    group per write_row_group(morsel) call and pushes each completed BLOCK of
    row_groups_per_block row groups (column-major, see write_parquet) to
    `sink` as it goes, so peak memory stays ~one block plus the file's bloom
    filters regardless of the total file size. The footer/statistics are
    accumulated incrementally; the bloom tail, the page index and the footer
    are emitted on close().

    Args:
        sink: a callable taking bytes. Called with each block of the file as it
            completes, and once more with the tail + footer on close. A file
            object's .write bound method, or a GCS resumable-upload adapter, both
            satisfy this.
        compression: "zstd" (default) or "none".
        profile: "fast" (default) or "storage"; as write_parquet, applied to
            every row group.
        bloom_filters / dictionary / max_page_bytes: as write_parquet; applied to
            every row group.
        sorted_by / sorted_descending: as write_parquet; applied to every row
            group written by this writer.
        row_groups_per_block: as write_parquet. The caller controls the
            row-group size by how much it passes per call
            (DEFAULT_ROWS_PER_ROW_GROUP is the measured best).

    Returns a context manager:

        with open_parquet_writer(f.write) as w:
            for batch in batches:
                w.write_row_group(batch)   # one row group per call

    Every batch must share the same column schema (names/types).
    """
    return _native.open_parquet_writer(sink, compression=compression,
                                       bloom_filters=bloom_filters,
                                       dictionary=dictionary,
                                       max_page_bytes=max_page_bytes,
                                       sorted_by=sorted_by,
                                       sorted_descending=sorted_descending,
                                       profile=profile, page_index=page_index,
                                       row_groups_per_block=row_groups_per_block)


def write_parquet_stream(morsel_iter, sink, compression: str = "zstd",
                         bloom_filters=True, dictionary: bool = True,
                         max_page_bytes: int = 0,
                         sorted_by=None, sorted_descending: bool = False,
                         profile: str = "fast", page_index: bool = True,
                         row_groups_per_block: int = DEFAULT_ROW_GROUPS_PER_BLOCK) -> int:
    """Stream an iterable of Morsels to a byte-chunk `sink` as one Parquet file.

    Thin wrapper over open_parquet_writer: one row group per yielded morsel,
    bounded memory. Empty morsels (no rows) are skipped. Returns the number of
    row groups written. See open_parquet_writer for the `sink` contract and
    write_parquet for sorted_by / sorted_descending / row_groups_per_block.
    """
    return _native.write_parquet_stream(morsel_iter, sink, compression=compression,
                                        bloom_filters=bloom_filters,
                                        dictionary=dictionary,
                                        max_page_bytes=max_page_bytes,
                                        sorted_by=sorted_by,
                                        sorted_descending=sorted_descending,
                                        profile=profile, page_index=page_index,
                                        row_groups_per_block=row_groups_per_block)


def patch_columns(source: bytes, drop=None, rename=None, add=None, retype=None) -> bytes:
    """Rewrite a parquet file's SHAPE without decoding the columns it keeps.

    Drops, renames, adds and/or retypes columns, returning the new file's bytes.
    Untouched columns' encoded pages are copied byte-for-byte rather than
    decoded and re-encoded, so the cost tracks file SIZE, not the number of
    values, and the result's pages are bit-identical to the source's.

    The source bytes are never modified: callers write the result to a new path
    so older snapshots keep pointing at what they were written against.

    Args:
        source: the complete parquet file to patch
        drop: column names to remove
        rename: {old_name: new_name}
        add: donor files, one per column to append - each a single-column,
            single-row parquet file written with compression="none",
            dictionary=False, carrying the new column's name and type and the
            value to fill existing rows with (a null row fills with NULL).
            Going through a donor means the added column is annotated by the
            same code that writes that type normally, rather than by a second
            copy of the type mapping that could drift from it. The value is
            stored once per row group, so an added column costs a few bytes
            however many rows the file holds.
        retype: {column_name: donor} - re-declare an existing column as the
            donor's type (only the donor's annotation is used, not its value).
            Free when parquet's physical type is unchanged, which covers most
            of the widening lattice; only physical int32 -> int64 needs real
            work, and then only that one column is decoded and re-encoded.

    Returns:
        The patched parquet file.

    Raises:
        RuntimeError: a named column is absent, an added name collides,
            dropping would leave no columns, a donor is not the shape described
            above, a retype asks for an unsupported physical change, or the
            file uses a shape the patcher cannot reproduce exactly. It refuses
            rather than relabelling real data.

            A LIST column is carried: it is one leaf chunk however deep it
            nests, so drop/rename copy its pages verbatim like a primitive's.
            The refusals that remain are STRUCT columns, a LIST whose element
            is not OPTIONAL (its definition levels are encoded against a
            different nesting scheme, so re-declaring them would misread every
            row), a LIST element carrying a logical type the writer cannot
            annotate (DECIMAL/DATE/TIMESTAMP/FLBA), and any logical type on a
            primitive it would have to approximate. `add` still takes only
            primitive donors.
    """
    return _native.patch_columns(
        source, drop=drop, rename=rename, add=add, retype=retype
    )
