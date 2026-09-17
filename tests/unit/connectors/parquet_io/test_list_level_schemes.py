"""
Regression test: LIST columns whose nesting is not all-nullable.

A parquet list column's definition levels depend on WHICH schema nodes are
OPTIONAL. Both list readers (the native scan's serialize_list_column in
rugo/src/parquet/ipc_serialize.hpp, and the standalone reader's
_make_array_vector in rugo/src/parquet/parquet_reader.pxi) used to hardcode the
thresholds for the all-OPTIONAL case -- "list k non-null when def >= 2k-1",
"non-empty when def >= 2k" -- and guard them with `max_def_level == 2*D + 1`.

That guard refused a file pyarrow writes without complaint:

    list<item: string not null>     -> max_rep_level=1, max_def_level=2

and with it every other shape whose path is not uniformly OPTIONAL: a `required`
LIST group, a `required` intermediate level in a nested list, and the legacy
2-level `repeated <leaf>` encoding.

The thresholds are now derived from the schema in metadata.cpp's WalkLeaves and
carried per column as `list_def_thresholds`. These tests pin the four shapes
pyarrow can produce, and -- more importantly -- that a required element and a
nullable element yield the SAME values: a level scheme is a layout difference,
never an answer difference.
"""

import os
import struct
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import opteryx
from opteryx.connectors import DiskConnector

_WS_COUNTER = [0]


def _unique_ws():
    """A fresh workspace name per call -- opteryx caches dataset metadata by
    workspace.table, so reusing one across TemporaryDirectory teardowns would
    serve a path to an already-deleted file."""
    _WS_COUNTER[0] += 1
    return f"ws_listlvl_{_WS_COUNTER[0]}"


def _roundtrip(values, list_type, *, use_dictionary=False):
    """Write a single list column `tags` and read it back through the engine."""
    ws = _unique_ws()
    schema = pa.schema([pa.field("tags", list_type, nullable=True)])
    table = pa.table({"tags": pa.array(values, type=list_type)}, schema=schema)
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = os.path.join(tmp, ws, "list_table")
        os.makedirs(data_dir)
        pq.write_table(
            table,
            os.path.join(data_dir, "data.parquet"),
            compression="none",
            use_dictionary=use_dictionary,
        )
        cwd = os.getcwd()
        os.chdir(tmp)
        try:
            opteryx.register_workspace(ws, DiskConnector)
            rows = []
            for morsel in opteryx.session().execute_to_morsels(
                f"SELECT tags FROM {ws}.list_table"
            ):
                rows.extend(morsel.column(b"tags").to_pylist())
            return rows
        finally:
            os.chdir(cwd)


def _list_of(element_type, *, element_nullable):
    return pa.list_(pa.field("item", element_type, nullable=element_nullable))


# Null list, empty list and populated list all present: each exercises a
# different definition level below the innermost threshold.
_VALUES = [["a", "b"], ["c"], [], None]


def test_nullable_element_unchanged():
    """The all-OPTIONAL shape -- max_def_level == 2*D + 1 -- still reads."""
    assert _roundtrip(_VALUES, _list_of(pa.string(), element_nullable=True)) == _VALUES


def test_required_element():
    """`list<item: string not null>` -- max_def_level == 2*D, previously refused."""
    assert _roundtrip(_VALUES, _list_of(pa.string(), element_nullable=False)) == _VALUES


def test_required_and_nullable_elements_agree():
    """A level scheme is a layout difference, never an answer difference."""
    nullable = _roundtrip(_VALUES, _list_of(pa.string(), element_nullable=True))
    required = _roundtrip(_VALUES, _list_of(pa.string(), element_nullable=False))
    assert nullable == required == _VALUES


def test_required_element_numeric_dictionary_encoded():
    """A required leaf on the dictionary-encoded path (distinct value stream)."""
    values = [[1, 2], [3], [], None]
    list_type = _list_of(pa.int64(), element_nullable=False)
    assert _roundtrip(values, list_type, use_dictionary=True) == values


def test_nested_list_required_inner_levels():
    """list<list<string not null> not null>: D == 2 with required inner levels,
    so the per-depth thresholds are 1 and 2 rather than the all-nullable 1 and 3."""
    inner = _list_of(pa.string(), element_nullable=False)
    outer = pa.list_(pa.field("item", inner, nullable=False))
    values = [[["a", "b"], ["c"]], [[]], [], None]
    assert _roundtrip(values, outer) == values


def test_null_elements_inside_lists_still_read():
    """The null-element branch is only reachable when the element IS optional --
    it must not have been lost when the branch stopped keying off max_def - 1."""
    values = [["a", None, "b"], [None], [], None, ["z"]]
    assert _roundtrip(values, _list_of(pa.string(), element_nullable=True)) == values




# ---------------------------------------------------------------------------
# Legacy 2-level list encoding.
#
# Pre-LIST-annotation writers (old parquet-mr / Hive / Impala) encode a list as a
# bare REPEATED leaf with no wrapper group:
#
#     message schema { repeated binary tags (UTF8); }
#
# giving max_rep_level == 1, max_def_level == 1 and a threshold of 0 -- a list
# that can never be null (def 0 is an EMPTY list, since a repeated field has no
# null state). The old `max_def_level == 2*D + 1` guard refused it outright.
#
# pyarrow cannot WRITE this shape (it always emits the 3-level LIST form), so the
# file is built here by hand. pyarrow READS it correctly, which is what makes it
# a usable oracle: see test_two_level_matches_pyarrow_oracle.
# ---------------------------------------------------------------------------

_T_I32, _T_I64, _T_BINARY, _T_LIST, _T_STRUCT = 5, 6, 8, 9, 12


class _Compact:
    """The sliver of thrift compact protocol a parquet footer needs."""

    def __init__(self):
        self.buf = bytearray()
        self._stack = []
        self.last = 0

    def _varint(self, v):
        while True:
            b = v & 0x7F
            v >>= 7
            if v:
                self.buf.append(b | 0x80)
            else:
                self.buf.append(b)
                return

    def _zig(self, v, bits):
        self._varint(((v << 1) ^ (v >> (bits - 1))) & ((1 << bits) - 1) if v < 0 else v << 1)

    def field(self, fid, ttype):
        delta = fid - self.last
        if 1 <= delta <= 15:
            self.buf.append((delta << 4) | ttype)
        else:
            self.buf.append(ttype)
            self._zig(fid, 16)
        self.last = fid

    def i32(self, fid, v):
        self.field(fid, _T_I32)
        self._zig(v, 32)

    def i64(self, fid, v):
        self.field(fid, _T_I64)
        self._zig(v, 64)

    def string(self, fid, s):
        self.field(fid, _T_BINARY)
        b = s.encode()
        self._varint(len(b))
        self.buf += b

    def list_header(self, fid, elem_type, size):
        self.field(fid, _T_LIST)
        if size < 15:
            self.buf.append((size << 4) | elem_type)
        else:
            self.buf.append(0xF0 | elem_type)
            self._varint(size)

    def elem_string(self, s):
        b = s.encode()
        self._varint(len(b))
        self.buf += b

    def elem_i32(self, v):
        self._zig(v, 32)

    def struct_field(self, fid):
        self.field(fid, _T_STRUCT)
        self._stack.append(self.last)
        self.last = 0

    def struct_elem(self):
        """Begin a struct that is a LIST element (no field header of its own)."""
        self._stack.append(self.last)
        self.last = 0

    def end(self):
        """Close a nested struct, restoring the enclosing field id."""
        self.buf.append(0)
        self.last = self._stack.pop()

    def stop(self):
        """Terminator for the OUTERMOST struct, which has no enclosing field."""
        self.buf.append(0)


def _rle_levels(levels, bit_width):
    """Levels as ONE bit-packed run (groups of 8, LSB-first), u32-length-prefixed."""
    padded = list(levels) + [0] * ((-len(levels)) % 8)
    body = bytearray([((len(padded) // 8) << 1) | 1])  # bit-packed run header
    mask = (1 << bit_width) - 1
    for g in range(len(padded) // 8):
        acc = 0
        for i in range(8):
            acc |= (padded[g * 8 + i] & mask) << (i * bit_width)
        body += bytes((acc >> (8 * k)) & 0xFF for k in range((8 * bit_width + 7) // 8))
    return struct.pack("<I", len(body)) + bytes(body)


def _write_two_level_parquet(path, values, rep_levels, def_levels):
    """A minimal valid parquet holding one `repeated binary tags (UTF8)` column."""
    plain = b"".join(struct.pack("<I", len(v.encode())) + v.encode() for v in values)
    page_body = _rle_levels(rep_levels, 1) + _rle_levels(def_levels, 1) + plain

    ph = _Compact()
    ph.i32(1, 0)                        # type = DATA_PAGE
    ph.i32(2, len(page_body))           # uncompressed_page_size
    ph.i32(3, len(page_body))           # compressed_page_size
    ph.struct_field(5)                  # data_page_header
    ph.i32(1, len(def_levels))          #   num_values (one per level entry)
    ph.i32(2, 0)                        #   encoding = PLAIN
    ph.i32(3, 3)                        #   definition_level_encoding = RLE
    ph.i32(4, 3)                        #   repetition_level_encoding = RLE
    ph.end()                            # end data_page_header
    ph.stop()                           # end PageHeader
    page = bytes(ph.buf) + page_body

    data_page_offset = 4                # straight after the "PAR1" magic
    num_rows = sum(1 for r in rep_levels if r == 0)

    f = _Compact()
    f.i32(1, 1)                                     # version
    f.list_header(2, _T_STRUCT, 2)                  # schema: root + leaf
    f.struct_elem()
    f.i32(3, 0)                                     #   repetition_type = REQUIRED
    f.string(4, "schema")
    f.i32(5, 1)                                     #   num_children
    f.end()
    f.struct_elem()
    f.i32(1, 6)                                     #   type = BYTE_ARRAY
    f.i32(3, 2)                                     #   repetition_type = REPEATED
    f.string(4, "tags")
    f.i32(6, 0)                                     #   converted_type = UTF8
    f.end()
    f.last = 2
    f.i64(3, num_rows)
    f.list_header(4, _T_STRUCT, 1)                  # row_groups
    f.struct_elem()
    f.list_header(1, _T_STRUCT, 1)                  #   columns
    f.struct_elem()
    f.i64(2, data_page_offset)                      #     file_offset
    f.struct_field(3)                               #     meta_data
    f.i32(1, 6)                                     #       type = BYTE_ARRAY
    f.list_header(2, _T_I32, 1)
    f.elem_i32(0)                                   #       encodings = [PLAIN]
    f.list_header(3, _T_BINARY, 1)
    f.elem_string("tags")                           #       path_in_schema
    f.i32(4, 0)                                     #       codec = UNCOMPRESSED
    f.i64(5, len(def_levels))                       #       num_values
    f.i64(6, len(page))                             #       total_uncompressed_size
    f.i64(7, len(page))                             #       total_compressed_size
    f.i64(9, data_page_offset)                      #       data_page_offset
    f.end()
    f.end()
    f.last = 1
    f.i64(2, len(page))                             #   total_byte_size
    f.i64(3, num_rows)
    f.end()
    f.last = 4
    f.string(6, "hand-built-2level")                # created_by
    f.stop()                                        # end FileMetaData

    footer = bytes(f.buf)
    with open(path, "wb") as fh:
        fh.write(b"PAR1" + page + footer + struct.pack("<I", len(footer)) + b"PAR1")
    return path


# [["a","b"], ["c"], []] in 2-level levels: rep 0 starts a row, def 0 is an
# empty list (a repeated field has no null state).
_TWO_LEVEL_VALUES = ["a", "b", "c"]
_TWO_LEVEL_REP = [0, 1, 0, 0]
_TWO_LEVEL_DEF = [1, 1, 1, 0]
_TWO_LEVEL_EXPECTED = [["a", "b"], ["c"], []]


def _two_level_file(tmp):
    return _write_two_level_parquet(
        os.path.join(tmp, "data.parquet"),
        _TWO_LEVEL_VALUES, _TWO_LEVEL_REP, _TWO_LEVEL_DEF,
    )


def test_two_level_matches_pyarrow_oracle():
    """The hand-built file is a real 2-level list, not an artefact of our reader."""
    with tempfile.TemporaryDirectory() as tmp:
        table = pq.read_table(_two_level_file(tmp))
        assert table.column("tags").to_pylist() == _TWO_LEVEL_EXPECTED


def test_two_level_thresholds_are_derived():
    """T[1] == 0: the list can never be null, and def 0 means EMPTY not NULL.
    max_def_level is 1 here -- the old guard demanded 2*D + 1 == 3."""
    from opteryx.connectors.parquet_io.pool_reader import fetch_column_chunk_info

    with tempfile.TemporaryDirectory() as tmp:
        info = fetch_column_chunk_info(_two_level_file(tmp), 0, ["tags"])["tags"]
        assert info["max_repetition_level"] == 1
        assert info["max_definition_level"] == 1
        assert info["list_def_thresholds"] == [0, 0]   # index 0 unused; T[1] == 0


def test_two_level_reads_through_the_standalone_reader():
    """The level walk handles the 2-level scheme -- same values as pyarrow."""
    from rugo.rugo_native import read_parquet_from_path

    with tempfile.TemporaryDirectory() as tmp:
        rows = []
        for morsel in read_parquet_from_path(_two_level_file(tmp)):
            rows.extend(morsel.column("tags").to_pylist())
        assert rows == _TWO_LEVEL_EXPECTED


def test_two_level_reads_through_the_engine():
    """The engine path reads a legacy 2-level list, same values as pyarrow.

    This needs TWO things, and they are separate:
      * the per-depth thresholds (T[1] == 0, max_def_level == 1), and
      * the schema layer recognising a bare REPEATED leaf as a list at all.

    The second was a distinct gap: EmitSchemaEntry decided array-ness from
    `elem.logical_type == "array"`, set only by the LIST annotation, so a 2-level
    column was advertised as scalar `varchar` while the decoder produced
    list-shaped output, and the VARCHAR pool path rejected the mismatch.
    """
    ws = _unique_ws()
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = os.path.join(tmp, ws, "list_table")
        os.makedirs(data_dir)
        _two_level_file(data_dir)
        cwd = os.getcwd()
        os.chdir(tmp)
        try:
            opteryx.register_workspace(ws, DiskConnector)
            rows = []
            for morsel in opteryx.session().execute_to_morsels(
                f"SELECT tags FROM {ws}.list_table"
            ):
                rows.extend(morsel.column(b"tags").to_pylist())
            assert rows == _TWO_LEVEL_EXPECTED
        finally:
            os.chdir(cwd)


def test_two_level_is_typed_as_a_list_not_a_scalar():
    """A 2-level column must be INDISTINGUISHABLE from a 3-level one at the
    schema layer -- `array<varchar>`, not the leaf's own `varchar`. If this
    regresses, the decoder emits list-shaped output while the binder expects a
    scalar, which is the collision the annotation gap used to cause."""
    from opteryx.connectors.parquet_io.pool_reader import fetch_column_chunk_info

    with tempfile.TemporaryDirectory() as tmp:
        info = fetch_column_chunk_info(_two_level_file(tmp), 0, ["tags"])["tags"]
        assert info["logical_type"] == "array<varchar>"


if __name__ == "__main__":
    test_nullable_element_unchanged()
    test_required_element()
    test_required_and_nullable_elements_agree()
    test_required_element_numeric_dictionary_encoded()
    test_nested_list_required_inner_levels()
    test_null_elements_inside_lists_still_read()
    test_two_level_matches_pyarrow_oracle()
    test_two_level_thresholds_are_derived()
    test_two_level_reads_through_the_standalone_reader()
    test_two_level_reads_through_the_engine()
    test_two_level_is_typed_as_a_list_not_a_scalar()
    print("✅ LIST level scheme regression tests passed")
