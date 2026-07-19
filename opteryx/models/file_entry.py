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


@dataclass
class FileEntry:
    """
    Represents a single file in the manifest with its statistics.

    This is a simple data holder - all logic lives in the Manifest class.
    Created from catalog DataFile objects during binding phase.
    """

    file_path: str
    file_format: str  # "PARQUET", "ORC", etc.
    record_count: int
    file_size_in_bytes: int
    uncompressed_size_in_bytes: Optional[int] = None

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
    # Lazy typed column stats from Parquet footer (FileColumnStats Cython object).
    # Populated by the filesystem connector; None for catalog/datafile path.
    # Access via column_stats.get_min(field_id) etc — no Python dicts created
    # until a consumer actually asks for a value.
    column_stats: Optional[object] = None
    # Per-column uncompressed sizes (aligned with schema field order)
    column_uncompressed_sizes_in_bytes: Optional[List[int]] = None

    @classmethod
    def from_datafile(cls, datafile, file_format: str = "PARQUET"):
        """
        Create FileEntry from a catalog DataFile object.

        Args:
            datafile: PyIceberg DataFile or similar from catalog
            file_format: File format (default: PARQUET)

        Returns:
            FileEntry instance
        """
        # Handle different datafile structures
        # PyIceberg catalog returns Datafile with an 'entry' attribute
        if isinstance(getattr(datafile, "entry", None), dict):
            entry = datafile.entry
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

            # `field_ids[i]` is the stable, catalog-assigned id for whichever
            # column produced `min_values[i]`/`max_values[i]` — present for
            # manifest rows written after field-ids existed. When present,
            # bounds MUST be keyed by that id, not by raw list position: a
            # file's own write-time column order need not match "position in
            # today's schema" once schema evolution has happened (that
            # mismatch is exactly what previously caused MIN/MAX on one
            # column to silently read another column's bound). Fall back to
            # positional indexing only for older manifest rows with no
            # field_ids at all.
            field_ids = entry.get("field_ids")
            if (
                field_ids
                and isinstance(field_ids, list)
                and isinstance(min_values, list)
                and len(field_ids) == len(min_values)
            ):
                lower_bounds = {
                    fid: val
                    for fid, val in zip(field_ids, min_values)
                    if fid is not None and val is not None
                }
            elif min_values and isinstance(min_values, list):
                lower_bounds = {i: val for i, val in enumerate(min_values) if val is not None}

            if (
                field_ids
                and isinstance(field_ids, list)
                and isinstance(max_values, list)
                and len(field_ids) == len(max_values)
            ):
                upper_bounds = {
                    fid: val
                    for fid, val in zip(field_ids, max_values)
                    if fid is not None and val is not None
                }
            elif max_values and isinstance(max_values, list):
                upper_bounds = {i: val for i, val in enumerate(max_values) if val is not None}

        else:
            # Fallback: try direct attribute access
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
            null_value_counts=None,  # Not available in this format
            column_uncompressed_sizes_in_bytes=column_uncompressed_sizes,
            min_values=min_values,
            max_values=max_values,
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
