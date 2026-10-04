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
Parquet Read Node

The physical-plan node for a parquet scan. It carries the plan-time facts the
native compiler (`compiler.py::_compile_scan`) reads to build a native Source —
manifest, columns, pushed predicates, scan overrides, the pushed top-N spec and
the length-only column set. It does not execute: every parquet scan runs on a
native Source, and a scan neither native Source admits is refused at compile
time.
"""

from opteryx.compiled.structures.footer_cache import ParquetFooterBytesCache
from opteryx.models import QueryProperties

_FOOTER_CACHE = ParquetFooterBytesCache()


def scan_footer_bytes_cache():
    """The process-wide footer-envelope byte cache the native-scan path reads
    through."""
    return _FOOTER_CACHE


def resolve_scan_filesystem(connector, blob_paths):
    """Resolve (filesystem, connector_type) for a parquet scan's blobs.

    Used by the plan-time native-scan gate (the compiler) and the binder: the
    filesystem supplies the signed-URL rewrite that decides whether a remote scan is
    eligible for the native Source, and the connector type picks the IO worker
    budget."""
    filesystem = getattr(connector, "filesystem", None)
    if filesystem is not None:
        return filesystem, (
            getattr(connector, "storage_type", None) or connector.__type__
        )
    from opteryx.connectors.io_systems import create_filesystem

    first_path = blob_paths[0] if blob_paths else ""
    protocol = first_path.split("://")[0] if "://" in first_path else ""
    return create_filesystem(protocol), (protocol.upper() if protocol else "FILESYSTEM")


cdef class ParquetReadNode(ReaderNode):
    """Plan node for a parquet scan; compiled to a native Source, never executed."""

    # Per-scan IO settings from this relation's `WITH(name = value)` hints,
    # already name-checked and permission-gated by the planner. None = none set.
    cdef public object scan_overrides
    # WP-2 top-N scan pushdown spec (set by TopNScanPushdownStrategy via node properties).
    cdef public object _topn_sort_name
    cdef public bint _topn_descending
    cdef public bint _topn_nulls_first
    cdef public object _topn_limit
    # Column identities proven to be read only through length-answerable
    # operations (set by LengthOnlyColumnStrategy via node properties). The
    # decoder skips long-value byte copies for these.
    cdef public object _length_only_columns

    def __init__(self, properties: QueryProperties, step, scan_overrides=None) -> None:
        """`scan_overrides`: the validated per-scan `WITH(name = value)` settings
        (the physical planner gates them), or None."""
        ReaderNode.__init__(self, properties, step)
        self.predicates = step.predicates
        self.scan_overrides = scan_overrides
        # WP-2: physical sort column name, direction, and N. None unless the
        # optimizer matched ORDER BY <physical col> LIMIT n directly over this scan.
        # A pushed top-N and the length-only column set are a Scan step's only
        # (READ_PARQUET arrives as a FunctionDataset step).
        is_scan_step = step.node_type in steps_with("topn_sort_name")
        self._topn_sort_name = step.topn_sort_name if is_scan_step else None
        self._topn_descending = bool(step.topn_descending) if is_scan_step else False
        if self._topn_sort_name is not None and step.topn_nulls_first is None:
            raise RuntimeError(
                "ScanStep carries a top-n sort key without its resolved null placement"
            )
        self._topn_nulls_first = bool(step.topn_nulls_first) if is_scan_step else False
        self._topn_limit = step.topn_limit if is_scan_step else None
        self._length_only_columns = step.length_only_columns if is_scan_step else None

    @property
    def name(self) -> str:  # pragma: no cover
        return "Parquet Read"

    @property
    def honours_scan_overrides(self):
        """This reader resolves them — see `scan_overrides` and io_tuning."""
        return True

    def sensors(self):
        base = super().sensors()
        # ReaderNode.sensors() sets base["dataset"] from self.dataset, which is
        # never populated on this class (the planner only passes "connector",
        # not "dataset", for Parquet scans) — self.connector.dataset is the
        # only place the real dataset name lives.
        if self.connector is not None:
            base["dataset"] = self.connector.dataset
        return base
