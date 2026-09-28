"""The catalog's manifest parquet, decoded natively (M-e): how a row's
per-column statistic lists are keyed to load-time positions.

The bytes come from opteryx-catalog's own manifest writer, so these are the
rows a catalog snapshot really holds. The rules (manifest_decode.hpp
`positions_of`):
  - a row carrying `field_ids` keys each list element by its id;
  - a list whose length is not the id list's cannot be lined up and is
    DROPPED whole - stats keyed by the wrong column would be a wrong answer;
  - a row with no `field_ids` was written in schema order: positional, and
    when every schema column has an id, only if the list covers them all.
"""

import io
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

_CATALOG_REPO = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "opteryx-catalog")
)
if os.path.isdir(_CATALOG_REPO) and _CATALOG_REPO not in sys.path:
    sys.path.insert(1, _CATALOG_REPO)

from opteryx_catalog.opteryx_catalog import OpteryxCatalog

from opteryx.compiled.planner.native_manifest import decode_manifest_parquet
from opteryx.compiled.structures.plan_steps import ExitStep
from opteryx.compiled.structures.plan_steps import ScanStep
from opteryx.models.manifest import Manifest
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.optimizer.statistics_refresh import refresh_statistics
from opteryx.planner.plan_context import PlanContext
from opteryx.types.logical_type import INT64
from opteryx.types.schema import RelationSchema


class _CaptureIO:
    def __init__(self):
        self.written = {}

    def new_output(self, path):
        written = self.written

        class _Output:
            def create(self):
                buffer = io.BytesIO()
                close = buffer.close

                def _close():
                    written[path] = buffer.getvalue()
                    close()

                buffer.close = _close
                return buffer

        return _Output()


class _ManifestWriter:
    write_parquet_manifest = OpteryxCatalog.write_parquet_manifest

    def __init__(self):
        self.io = _CaptureIO()


def _manifest_bytes(entries):
    writer = _ManifestWriter()
    path = writer.write_parquet_manifest(1000, entries, "mem://ds")
    return writer.io.written[path]


def _entry(field_ids, mins, maxes, nulls):
    return {
        "file_path": "mem://ds/data/f.parquet",
        "file_format": "parquet",
        "record_count": 10,
        "file_size_in_bytes": 100,
        "uncompressed_size_in_bytes": 200,
        "column_uncompressed_sizes_in_bytes": [],
        "null_counts": nulls,
        "min_k_hashes": [],
        "histogram_counts": [],
        "histogram_bins": 0,
        "min_values": mins,
        "max_values": maxes,
        "min_lengths": [],
        "max_lengths": [],
        "field_ids": field_ids,
        "char_class_counts": [],
        "char_total_bytes": [],
    }


# Bound columns are minted by a query's ColumnTable; these tests share one.
_PLAN_CONTEXT = PlanContext()


def _schema(field_ids):
    return RelationSchema(
        name="t",
        columns=[
            _PLAN_CONTEXT.columns.relation_column("t", name, column_type=INT64, field_id=field_id)
            for name, field_id in zip(("a", "b"), field_ids)
        ],
    )


def _decode(schema, entry):
    columns = schema.columns
    native = decode_manifest_parquet(
        _manifest_bytes([entry]),
        tuple(c.name for c in columns),
        tuple(c.column_type.physical for c in columns),
        {c.field_id: p for p, c in enumerate(columns) if c.field_id is not None},
        True,
        True,
    )
    return Manifest(native, schema)


def _ordinal_bounds(manifest, name):
    """Column `name`'s relation-wide ordinal bounds as the planner reads them:
    the base statistics the statistics refresh gives a Scan over `manifest`
    (all of its columns) in _PLAN_CONTEXT."""
    plan = LogicalPlan(_PLAN_CONTEXT)
    scan = plan.add_node(ScanStep(relation="t", schema=manifest.schema, manifest=manifest))
    plan.add_edge(scan, plan.add_node(ExitStep()))
    refresh_statistics(plan, _PLAN_CONTEXT)
    return _PLAN_CONTEXT.statistics.ordinal_bounds(scan, manifest.schema.find_column(name).identity)


def test_row_field_ids_key_the_lists_whatever_their_order():
    """Written in the FILE's column order (b then a): each element lands on the
    column its id names, not on the column at its index."""
    manifest = _decode(_schema([1, 5]), _entry([5, 1], [50, 10], [59, 19], [2, 1]))
    assert _ordinal_bounds(manifest, "a") == (10, 19)
    assert _ordinal_bounds(manifest, "b") == (50, 59)
    assert manifest.get_total_null_count("a") == 1
    assert manifest.get_total_null_count("b") == 2


def test_a_list_that_does_not_line_up_with_the_ids_is_dropped():
    """Two ids, one bound each way: which column the bound belongs to is
    unknowable, so there is none - the null counts, which DO line up, stay."""
    manifest = _decode(_schema([1, 5]), _entry([5, 1], [50], [59], [2, 1]))
    assert _ordinal_bounds(manifest, "a") is None
    assert _ordinal_bounds(manifest, "b") is None
    assert manifest.get_total_null_count("a") == 1


def test_a_row_with_no_field_ids_is_positional():
    manifest = _decode(_schema([1, 5]), _entry([], [10, 50], [19, 59], [1, 2]))
    assert _ordinal_bounds(manifest, "a") == (10, 19)
    assert _ordinal_bounds(manifest, "b") == (50, 59)


def test_a_short_positional_list_under_a_keyed_schema_is_dropped():
    """No ids, and a list covering fewer columns than the schema has: it could
    belong to any of them."""
    manifest = _decode(_schema([1, 5]), _entry([], [10], [19], [1, 2]))
    assert _ordinal_bounds(manifest, "a") is None
    assert _ordinal_bounds(manifest, "b") is None
    assert manifest.get_total_null_count("b") == 2


def test_the_catalogs_lower_case_format_is_the_engines_format():
    manifest = _decode(_schema([1, 5]), _entry([1, 5], [10, 50], [19, 59], [1, 2]))
    assert manifest.file_formats() == ["PARQUET"]


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
