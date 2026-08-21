# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
FileEntry - represents a single file in a table manifest with its statistics.
"""

from dataclasses import dataclass
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple


@dataclass
class FileEntry:
    """
    Represents a single file in the manifest with its statistics.

    This is a simple data holder - all logic lives in the Manifest class.
    Created from catalog DataFile objects during binding phase.
    """

    file_path: str
    file_format: str  # "PARQUET", "ORC", etc.
    # None means UNKNOWN, and unknown is NOT zero. A producer with no row count
    # for a file - one that computes no plan-time statistics (MabelTable), or a
    # footer read that failed - must pass None rather than 0: a fabricated 0 is
    # indistinguishable from a genuinely empty file, and Manifest.get_record_count
    # feeds strategies that answer COUNT(*) straight from the manifest and delete
    # LIMIT nodes. Both turn a fabricated 0 into a silent wrong answer.
    record_count: Optional[int]
    file_size_in_bytes: int
    uncompressed_size_in_bytes: Optional[int] = None

    # How many row groups the file holds. None means UNKNOWN, on the same
    # doctrine as record_count above — never a fabricated 1.
    #
    # A .skene file used to BE one row group, so file count and row group count
    # were the same number and neither had to be carried. They are not the same
    # number any more (a .skene file holds up to 16), and the scan's unit of work
    # is the row group — so a telemetry field reporting the file count as the row
    # group count would understate the work by the packing factor. Populated by
    # the producer that reads footers to build the manifest.
    row_group_count: Optional[int] = None

    # Per-column statistics indexed by field_id
    # Values are serialized bytes (catalog format)
    lower_bounds: Optional[Dict[int, bytes]] = None
    upper_bounds: Optional[Dict[int, bytes]] = None
    null_value_counts: Optional[Dict[int, int]] = None

    # NOTE: min-k hash / histogram sketches are deliberately NOT held here. They
    # live only as whole-column native draken vectors on the Manifest, which the
    # planner's kernels read directly (see Manifest._min_k_vector). A boxed
    # per-file copy would be a second representation to keep in step — and the
    # vectors' rows are positional to the file list, so a copy that drifted would
    # read another file's sketch. Producers pass sketches explicitly to
    # manifest_io.write_manifest_parquet(sketches=...).
    #
    # raw min/max lists (for direct access if needed)
    min_values: Optional[List] = None
    max_values: Optional[List] = None
    # Positional (by field_id, parallel to min_values/max_values), populated by
    # ANALYZE's native per-file statistics pass (opteryx.operators.table_management
    # ._analyze) — see manifest_io.py's _MANIFEST_COLUMNS. Also populated directly
    # from the manifest row by from_datafile (catalog path) — manifest_io.py's
    # _file_entry_to_manifest_dict (used by SHOW MANIFEST) reads these plain
    # lists, not the field_id-keyed dict forms below. None for a FileEntry no
    # producer has touched (e.g. parquet-footer origin with no stats pass).
    null_counts: Optional[List[Optional[int]]] = None
    min_lengths: Optional[List[Optional[int]]] = None
    max_lengths: Optional[List[Optional[int]]] = None
    # Field_id-correct dict form of min_lengths/max_lengths (mirrors
    # lower_bounds/upper_bounds alongside min_values/max_values above) — the
    # positional list form is NOT safely indexable by a real catalog field_id
    # (it's positional by write order, parallel to a separate field_ids list;
    # see the exact bug this fixed: Manifest.get_ordinal_bounds indexing
    # min_values[field_id] directly silently read a different column's
    # bound). Populated by from_datafile (catalog path, field_id-keyed via
    # zip(field_ids, min_lengths)) and manifest_io.read_manifest_file_entries
    # (local path, positionally-keyed via enumerate() since local field_id
    # == position). None where neither producer has touched a column.
    min_length_bounds: Optional[Dict[int, int]] = None
    max_length_bounds: Optional[Dict[int, int]] = None
    # How many equi-width bins each per-column histogram in this file's
    # `histogram_counts` row holds. Stored by every manifest producer alongside
    # the counts themselves (manifest_io._MANIFEST_COLUMNS) and carried here so
    # readers HONOUR it rather than assuming manifest_io.HISTOGRAM_BINS: a
    # manifest written with a different bin count is a differently-shaped
    # histogram, and silently reading it as 32 bins mis-places every bin
    # boundary -- a wrong selectivity, not a missing one. None means the
    # producer recorded no bin count (nothing to check against).
    histogram_bins: Optional[int] = None
    # Total string byte count per column (string-family columns only; None
    # elsewhere) — the numerator half of avg_length = char_total_bytes /
    # true_non_null_count, computed at read time from the now-real null_counts.
    char_total_bytes: Optional[List[Optional[int]]] = None
    # Per-column distinct-value count, field_id-keyed, as (ndv, is_exact).
    #
    # The two halves are ONE value on purpose. An exact NDV (skene's
    # kStatNdvExact — value ordering deduplicated the column, so the count is a
    # BOUND) and a sketched NDV (kStatNdv alone — a KMV estimate, ~+/-3% at
    # K=1024) are not the same kind of number, and the codebase's metric-vs-
    # estimate lingo says a bound must never be reachable as a bare int that
    # has lost its provenance. Splitting these into two dicts is how the flag
    # gets dropped at the first call site that only wanted "the number".
    #
    # Populated by the filesystem connector's SKENE branch (aggregated from the
    # per-ROW-GROUP blobs the file footer carries; see the aggregation rule
    # there). The parquet path carries its NDV inside `column_stats` instead and
    # leaves this None. None means NOT TRACKED for the whole file; a column
    # absent from the dict means not tracked for that column.
    distinct_value_counts: Optional[Dict[int, Tuple[int, bool]]] = None
    # Per-column KMV min-hash sketch, field_id-keyed — this FILE's union of its
    # row groups' stored sketches (skene format.h, ColumnSketchHeader).
    #
    # Carried as well as the count above because a count cannot be merged and
    # these can: unioning two files' sketches gives the K smallest of the
    # combined hashes exactly, so `Manifest.estimate_cardinality` measures the
    # overlap between files instead of guessing it from min/max. Fewer than K
    # hashes means the union holds every distinct value, so the answer is exact.
    #
    # ⛔ skene's XXH3 value hashes. NEVER merge with ANALYZE's sketches
    # (`Manifest._min_k_vector`), which are draken `Vector.hash()`.
    distinct_sketches: Optional[Dict[int, list]] = None
    # Largest EXACT per-row-group distinct count in this file, field_id-keyed.
    #
    # A PROVEN LOWER BOUND, kept apart from `distinct_value_counts` because it is
    # a different claim: that count describes the whole file and may be an
    # estimate, where this is a number some row group demonstrated. A row group
    # is a subset of the file and the file of the relation, so a subset's exact
    # distinct count can never exceed the whole's — which makes this the right
    # floor for the K=32 sketch estimator, whose ~18% error can otherwise land
    # under a value the footers already prove.
    distinct_floors: Optional[Dict[int, int]] = None
    # Lazy typed column stats from Parquet footer (FileColumnStats Cython object).
    # Populated by the filesystem connector; None for catalog/datafile path.
    # Access via column_stats.get_min(field_id) etc — no Python dicts created
    # until a consumer actually asks for a value.
    column_stats: Optional[object] = None
    # Per-column uncompressed sizes (aligned with schema field order)
    column_uncompressed_sizes_in_bytes: Optional[List[int]] = None

    @classmethod
    def from_datafile(cls, datafile, file_format: str = "PARQUET", schema_field_ids=None):
        """
        Create FileEntry from a catalog DataFile object.

        Args:
            datafile: PyIceberg DataFile or similar from catalog
            file_format: File format (default: PARQUET)
            schema_field_ids: The field_id of each column of the schema this
                manifest row was written against, in schema order — i.e. the
                key for `min_values[i]` when the row carries no `field_ids`
                list of its own. REQUIRED whenever the schema assigns field_ids
                at all, because that is the key space `Manifest._resolve_field_id`
                will look these stats up by; see the keying note below.

        Returns:
            FileEntry instance
        """
        # Handle different datafile structures
        # PyIceberg-style catalogs return Datafile with an 'entry' attribute
        # carrying the manifest row - a plain dict, or (the current
        # opteryx_catalog bulk-scan path) an ArrowManifestRow, a dict-like
        # view over already-materialized columns (see manifest_arrow.py) that
        # is NOT a dict subclass. Branch on presence, not on isinstance(dict):
        # every known producer that sets `.entry` supports `.get()`, and a
        # producer that didn't would be better served by a loud AttributeError
        # here than by silently falling into the wrong branch below and
        # returning a FileEntry with no bounds/lengths/null-counts at all.
        entry = getattr(datafile, "entry", None)
        if entry is not None:
            file_path = entry.get("file_path")
            record_count = entry.get("record_count", 0)
            file_size = entry.get("file_size_in_bytes", 0)
            uncompressed_size = entry.get("uncompressed_size_in_bytes")

            # Convert min_values/max_values to bounds.
            min_values = entry.get("min_values")
            max_values = entry.get("max_values")
            lower_bounds = None
            upper_bounds = None

            column_uncompressed_sizes = entry.get("column_uncompressed_sizes_in_bytes")

            # Every per-column stat the manifest row carries — min_values,
            # max_values, min_lengths, max_lengths, null_counts — is a
            # POSITIONAL list in the row's own column order. The key each one
            # must end up under is whatever `Manifest._resolve_field_id` will
            # look it up by, and that is the column's catalog field_id whenever
            # the schema assigns one (position only when it does not). So the
            # key list is resolved ONCE here, for all five:
            #
            #  * `field_ids[i]` — the stable, catalog-assigned id for whichever
            #    column produced `min_values[i]`; present for manifest rows
            #    written after field-ids existed. A file's own write-time column
            #    order need not match "position in today's schema" once schema
            #    evolution has happened, which is why the row's own list wins.
            #  * `schema_field_ids` — the caller's schema in schema order, used
            #    for rows that predate the row-level list. The current
            #    opteryx_catalog writes NO `field_ids` key at all, so this is
            #    the live path, not a legacy one.
            #  * positional — only when neither is available, which means the
            #    schema assigns no field_ids either, so `_resolve_field_id`
            #    falls back to load-time position and the two spaces agree.
            #
            # ⛔ Keying positionally while the reader resolves by field_id is a
            # SILENT WRONG ANSWER, not a missed optimisation: with 1-based
            # catalog ids every column reads its NEIGHBOUR's stats — a pushed
            # `id = 3` compared against `name`'s string ordinals and pruned
            # every file, and `SELECT MIN(id)` answered out of the manifest with
            # a string ordinal. Hence: when a key list exists but does not line
            # up with a stat list, that stat is DROPPED (no stats = no pruning =
            # correct but slower) rather than keyed by position.
            field_ids = entry.get("field_ids")
            if not (field_ids and isinstance(field_ids, list)):
                # A schema that assigns no field_id to some column cannot key
                # these stats: `_resolve_field_id` answers that column with its
                # load-time POSITION, so a partially-keyed dict would put some
                # columns in one key space and the rest in another. All-or-
                # nothing keeps the two sides in step - and all-None (no field
                # ids anywhere, e.g. the filesystem path) means both sides use
                # position, which the `field_ids is None` branch below does.
                field_ids = schema_field_ids
                if field_ids is not None and any(fid is None for fid in field_ids):
                    field_ids = None
            if not (field_ids and isinstance(field_ids, list)):
                field_ids = None

            def _key_by_field_id(values):
                """Key a positional per-column stat list by `field_ids`, or by
                position when there are no field_ids at all. None when the two
                cannot be lined up - see the ⛔ note above."""
                if not values or not isinstance(values, list):
                    return None
                if field_ids is None:
                    return {i: val for i, val in enumerate(values) if val is not None}
                if len(field_ids) != len(values):
                    return None
                return {
                    fid: val
                    for fid, val in zip(field_ids, values)
                    if fid is not None and val is not None
                }

            lower_bounds = _key_by_field_id(min_values)
            upper_bounds = _key_by_field_id(max_values)

            # The catalog's ParquetManifestEntry.to_dict() carries "min_lengths"/
            # "max_lengths" as positional lists parallel to the values lists
            # (verified against the installed opteryx_catalog package). Before
            # they were read here, from_datafile dropped them entirely, so the
            # length-aware selectivity guards had no signal at all for
            # catalog-backed datasets.
            min_lengths = entry.get("min_lengths")
            max_lengths = entry.get("max_lengths")
            min_length_bounds = _key_by_field_id(min_lengths)
            max_length_bounds = _key_by_field_id(max_lengths)

            # Likewise "null_counts". Before it was read here, from_datafile
            # hardcoded null_value_counts=None for every catalog-backed
            # FileEntry, so anything gated on Manifest.get_total_null_count
            # (e.g. TopNManifestPruningStrategy's NULL-safety check) silently
            # treated every catalog-backed column as "unknown nullability" and
            # never fired.
            catalog_null_counts = entry.get("null_counts")
            null_value_counts = _key_by_field_id(catalog_null_counts)

            # Raw positional-by-field_id lists, kept alongside the field_id-keyed
            # dict forms above — mirrors min_values/max_values, which are passed
            # through as both a dict (lower_bounds/upper_bounds) and the raw list.
            # SHOW MANIFEST (manifest_io._file_entry_to_manifest_dict) reads these
            # plain list attributes directly, not the dict forms; without this,
            # a catalog-backed FileEntry always displayed empty null_counts/
            # min_lengths/max_lengths/char_total_bytes regardless of what the
            # manifest actually stored.
            char_total_bytes = entry.get("char_total_bytes")

            # The bin count the histogram in this row was actually built with.
            # 0 is the writer's "no histogram" marker (manifest_io writes
            # `HISTOGRAM_BINS if histogram else 0`), which is not a bin count --
            # keep it None so a reader can tell "no histogram here" from a real
            # width and never validates against a fabricated 0.
            histogram_bins = entry.get("histogram_bins") or None

        else:
            # Fallback: try direct attribute access. No known producer of this
            # shape carries string-length stats, so length_bounds stay None —
            # the length-aware selectivity guards degrade to "no signal" here,
            # same as any other FileEntry no stats pass has touched.
            min_length_bounds = None
            max_length_bounds = None
            histogram_bins = None
            null_value_counts = None
            min_lengths = None
            max_lengths = None
            catalog_null_counts = None
            char_total_bytes = None
            file_path = getattr(datafile, "file_path", None)
            record_count = getattr(datafile, "record_count", 0)
            file_size = getattr(datafile, "file_size_in_bytes", 0)
            uncompressed_size = getattr(datafile, "uncompressed_size_in_bytes", None)

            # Try lower_bounds/upper_bounds first
            lower_bounds = getattr(datafile, "lower_bounds", None)
            upper_bounds = getattr(datafile, "upper_bounds", None)

            # Try raw min/max lists and column sizes
            min_values = getattr(datafile, "min_values", None)
            max_values = getattr(datafile, "max_values", None)
            column_uncompressed_sizes = getattr(
                datafile, "column_uncompressed_sizes_in_bytes", None
            )

            # Convert to dict if needed
            if lower_bounds and not isinstance(lower_bounds, dict):
                lower_bounds = dict(lower_bounds) if getattr(lower_bounds, "__iter__", None) is not None else None
            if upper_bounds and not isinstance(upper_bounds, dict):
                upper_bounds = dict(upper_bounds) if getattr(upper_bounds, "__iter__", None) is not None else None

            # If we have raw min_values/max_values but no lower_bounds/upper_bounds,
            # convert them to bounds mapping for backward compatibility
            if (lower_bounds is None or upper_bounds is None) and isinstance(min_values, list):
                lb = {i: val for i, val in enumerate(min_values) if val is not None}
                lower_bounds = lower_bounds or lb
            if (upper_bounds is None) and isinstance(max_values, list):
                ub = {i: val for i, val in enumerate(max_values) if val is not None}
                upper_bounds = upper_bounds or ub

        return cls(
            file_path=file_path,
            file_format=file_format,
            record_count=record_count,
            file_size_in_bytes=file_size,
            uncompressed_size_in_bytes=uncompressed_size,
            lower_bounds=lower_bounds,
            upper_bounds=upper_bounds,
            null_value_counts=null_value_counts,
            column_uncompressed_sizes_in_bytes=column_uncompressed_sizes,
            min_values=min_values,
            max_values=max_values,
            min_length_bounds=min_length_bounds,
            max_length_bounds=max_length_bounds,
            null_counts=catalog_null_counts,
            min_lengths=min_lengths,
            max_lengths=max_lengths,
            char_total_bytes=char_total_bytes,
            histogram_bins=histogram_bins,
        )

    def to_dict(self) -> dict:
        """Convert to dictionary (useful for debugging/logging)."""
        return {
            "file_path": self.file_path,
            "file_format": self.file_format,
            "record_count": self.record_count,
            "file_size_in_bytes": self.file_size_in_bytes,
            "uncompressed_size_in_bytes": self.uncompressed_size_in_bytes,
            "column_uncompressed_sizes_in_bytes": self.column_uncompressed_sizes_in_bytes,
            "min_values": self.min_values,
            "max_values": self.max_values,
            "has_bounds": self.lower_bounds is not None or self.upper_bounds is not None,
            "has_null_counts": self.null_value_counts is not None or (self.column_stats is not None and self.column_stats.has_any_null_counts()),
            "has_column_stats": self.column_stats is not None and self.column_stats.has_stats(),
        }
