# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Merge Node

Streaming sink for MERGE INTO. Consumes the desugared join's morsels, each row
already carrying the action its arms decided (see merge_desugar), and turns
them into the two halves of one commit:

    $merge_action ∈ {UPDATE, DELETE}  → a delete position (file, ordinal)
    $merge_action ∈ {INSERT, UPDATE}  → a row to append
    $merge_action == NOOP             → dropped, costs nothing

Both halves land in a single `merge_commit`. Neither is observable without the
other: a reader seeing the append alone would see the row twice, and the delete
alone would see it not at all.

The per-morsel work here is a handful of vector operations, never a row loop -
classification and blending already happened natively in the projection above.

UPDATE and DELETE reuse this sink whole. They are MERGE with a degenerate
source: no join, so every scanned row that survives the WHERE is matched, and
the action is a constant. A DELETE names no target columns at all, which puts
the control columns at 0, 1, 2 and leaves the append stream empty for every
row - no data file is written and no payload column is read.
"""

from typing import Optional

from array import array as _array_i32
from cython.operator cimport dereference as deref
from libcpp.memory cimport shared_ptr
from libcpp.vector cimport vector as cppvector
from libc.stdint cimport int32_t, int64_t

from opteryx.constants import QueryStatus
from opteryx.models import NonTabularResult
from opteryx.models import QueryProperties

# Kept in step with rugo's row-group default, exactly as InsertNode does - see
# the note there for why a flushed file must not span more than one row group.
_MAX_ROWS_PER_ROW_GROUP = 262144


cdef class _MergeAddresses:
    """Owner for the statement's native address state.

    A holder rather than a member of MergeNode: the sink is a plain Python class
    (like every other operator here), which cannot carry a C++ pointer directly.
    RAII lives here so the set is freed with the node whatever unwinds it.
    """

    cdef MergeAddressState* ptr

    def __cinit__(self):
        self.ptr = new MergeAddressState()

    def __dealloc__(self):
        if self.ptr is not NULL:
            del self.ptr
            self.ptr = NULL


class MergeNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        # The statement this sink is serving. UPDATE and DELETE are MERGE with a
        # degenerate source and reuse this node whole, so every message it
        # raises must name the SQL the user actually wrote. Set BEFORE the base
        # initialiser, which reads `name` to build its timing stat key.
        self.statement_name: str = parameters.get("statement_name") or "MERGE INTO"
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.relation_name: str = parameters.get("relation_name")
        # What the catalog records this commit AS. The three statements are one
        # physical operation, so without it the snapshot log and the audit trail
        # could not say which one a reader is looking at.
        self.operation: str = parameters.get("operation") or "merge"
        self.connector = parameters.get("connector")
        self.target_schema = parameters.get("target_schema")
        self.target_column_names = parameters.get("target_column_names")
        # Ordered data-file paths, positionally indexed by `$merge_file`. The
        # scan carries an INDEX rather than a path per row: the list is already
        # held, so the index is exact and free where a per-row string would be
        # dragged through the join for no information gain.
        self.file_paths = parameters.get("file_paths")

        self._file_entries = []
        # Every acted-on address lives in NATIVE state for the whole statement
        # (native_merge_sink.hpp) and becomes Python exactly once, at EOS. It
        # tracks MATCHED rather than merely retired rows, because a duplicate
        # address is the cardinality violation: a target row matched by two
        # source rows appears twice carrying the SAME address, and a double
        # match whose arms both yielded NOOP would otherwise go unnoticed.
        self._addresses = _MergeAddresses()
        self.result: Optional[NonTabularResult] = None

        self.coalesce_rows = min(
            int(parameters.get("write_coalesce_rows", _MAX_ROWS_PER_ROW_GROUP)),
            _MAX_ROWS_PER_ROW_GROUP,
        )
        self._pending = []
        self._pending_rows = 0

    @property
    def name(self):
        return self.statement_name.split()[0].title()

    @property
    def config(self):
        return f"{self.statement_name.lower()} {self.relation_name}"

    @property
    def _author(self):
        """The session user this merge is attributed to, or None.

        None is passed through rather than substituted, so a store that requires
        attribution rejects the write instead of recording an invented identity.
        """
        from opteryx.variables import resolve

        return resolve("external_user", self.properties.variables, None) or None

    def _push_impl(self, morsel):
        if morsel is _EOS_SENTINEL:
            self._flush_pending()
            # Every data file is durably written before any catalog mutation, so
            # a failure above this point leaves the target completely untouched.
            # Nothing is committed until here, which is also why the cardinality
            # check below can raise this late and still be safe.
            delete_positions = self._collect_delete_positions()
            if not self._file_entries and not delete_positions:
                # Every row was NOOP - a feed that republished nothing changed.
                # That is a successful merge that did no work, not a failure,
                # and committing a snapshot describing nothing would be a lie
                # about what happened.
                self.result = NonTabularResult(
                    record_count=0,
                    status=QueryStatus.SQL_SUCCESS,
                )
                return
            self.connector.merge_commit(
                self.relation_name,
                self._file_entries,
                delete_positions,
                author=self._author,
                operation=self.operation,
            )
            self.result = NonTabularResult(
                record_count=self._acted_row_count(),
                status=QueryStatus.SQL_SUCCESS,
            )
            return

        self._consume(morsel)

    def _consume(self, morsel):
        """Split one joined morsel into delete addresses and rows to append.

        Every per-row read happens in C++ under nogil (native_merge_sink.hpp).
        The morsel arriving here is Cxx-backed, and its values are readable ONLY
        through that substrate — PyObject column access on one is refused by
        design. Nothing about the payload columns is read at all; only the three
        narrow control columns the projection appended.
        """
        cdef _MergeAddresses state = self._addresses
        cdef shared_ptr[CxxMorsel] cxm = morsel_to_cxx(<Morsel>morsel)
        cdef cppvector[int32_t] write_rows
        cdef int32_t n_target = <int32_t>len(self.target_column_names)
        cdef int status
        cdef Py_ssize_t written
        cdef Py_ssize_t i

        with nogil:
            status = merge_split_morsel(
                deref(cxm.get()),
                n_target,          # $merge_action
                n_target + 1,      # $merge_file
                n_target + 2,      # $merge_ordinal
                deref(state.ptr),
                write_rows,
            )

        if status != 0:
            self._raise_split_error(status)

        written = write_rows.size()
        if written == 0:
            return

        indices = _array_i32("i", [write_rows[i] for i in range(written)])
        # `take` gathers natively off an int32 array without leaving the
        # substrate; `select` narrows to the target's own columns, dropping the
        # three control columns the sink has now consumed.
        rows = morsel.take(indices).select(morsel.column_names[:n_target])
        self._pending.append(rows)
        self._pending_rows += written
        if self._pending_rows >= self.coalesce_rows:
            self._flush_pending()

    def _raise_split_error(self, int status):
        from opteryx.exceptions import InvalidInternalStateError
        from opteryx.exceptions import UnsupportedSyntaxError

        if status == 1:
            # Unreachable for UPDATE and DELETE - with no join no target row can
            # be emitted twice - so the message names MERGE's fix directly.
            raise UnsupportedSyntaxError(
                f"**MERGE INTO** cardinality violation: a row of {self.relation_name} "
                "is matched by more than one source row, so the statement would act "
                "on it twice. De-duplicate the source on the **ON** key."
            )
        if status == 2:
            raise InvalidInternalStateError(
                "MergeNode: a control column is not INT64; the merge projection has "
                "drifted from the shape the binder proved."
            )
        if status == 3:
            raise InvalidInternalStateError(
                "MergeNode: a matched row carried a file but no ordinal; the target "
                "scan did not emit a complete row address."
            )
        if status == 4:
            raise InvalidInternalStateError(
                "MergeNode: a row ordinal is outside the addressable range; the "
                "target scan produced a corrupt row address."
            )
        if status == 5:
            from opteryx.exceptions import MergeTooLargeError

            raise MergeTooLargeError(
                f"**{self.statement_name}** ran out of address budget tracking which "
                f"rows of {self.relation_name} it has acted on. The statement holds "
                "every acted-on row's address until it commits, because the commit "
                "is atomic. Act on fewer rows, or compact the target so its rows "
                "pack denser."
            )
        raise InvalidInternalStateError(f"MergeNode: unknown split status {status}")

    def _acted_row_count(self):
        cdef _MergeAddresses state = self._addresses
        return state.ptr.rows_inserted + state.ptr.rows_updated + state.ptr.rows_deleted

    def _collect_delete_positions(self):
        """Read the accumulated addresses out of native state — the ONE crossing."""
        cdef _MergeAddresses state = self._addresses
        cdef cppvector[int64_t] files = merge_retired_files(deref(state.ptr))
        cdef cppvector[int64_t] ordinals
        cdef Py_ssize_t i, j
        out = {}
        for i in range(files.size()):
            ordinals = merge_retired_ordinals(deref(state.ptr), files[i])
            out[self.file_paths[files[i]]] = [ordinals[j] for j in range(ordinals.size())]
        return out

    def _flush_pending(self):
        """Write whatever rows have been collected since the last flush as one file.

        The pending list holds references only - one bulk concat over the whole
        list, never an incremental concat per arrival (which re-copies the
        growing buffer every time).
        """
        if not self._pending:
            return
        merged = Morsel.combine(self._pending)
        file_entry = self.connector.write_morsel(self.relation_name, merged)
        self._file_entries.append(file_entry)
        self._pending = []
        self._pending_rows = 0
