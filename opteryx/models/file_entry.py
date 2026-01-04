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

    # Extended statistics - may be None for bulk-loaded data
    min_k_hashes: Optional[List[List[int]]] = None
    histogram_counts: Optional[List[List[int]]] = None
    histogram_bins: Optional[int] = None
    # raw min/max lists (for direct access if needed)
    min_values: Optional[List] = None
    max_values: Optional[List] = None
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
        if hasattr(datafile, "entry") and isinstance(datafile.entry, dict):
            entry = datafile.entry
            file_path = entry.get("file_path")
            record_count = entry.get("record_count", 0)
            file_size = entry.get("file_size_in_bytes", 0)
            uncompressed_size = entry.get("uncompressed_size_in_bytes")

            # Convert min_values/max_values to bounds
            # These are lists indexed by field_id
            min_values = entry.get("min_values")
            max_values = entry.get("max_values")
            lower_bounds = None
            upper_bounds = None

            column_uncompressed_sizes = entry.get("column_uncompressed_sizes_in_bytes")

            if min_values and isinstance(min_values, list):
                # Build dict indexed by position (0-based field_id)
                lower_bounds = {i: val for i, val in enumerate(min_values) if val is not None}
            if max_values and isinstance(max_values, list):
                upper_bounds = {i: val for i, val in enumerate(max_values) if val is not None}

            min_k_hashes = entry.get("min_k_hashes")
            histogram_counts = entry.get("histogram_counts")
            histogram_bins = entry.get("histogram_bins")

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
                lower_bounds = dict(lower_bounds) if hasattr(lower_bounds, "__iter__") else None
            if upper_bounds and not isinstance(upper_bounds, dict):
                upper_bounds = dict(upper_bounds) if hasattr(upper_bounds, "__iter__") else None

            min_k_hashes = getattr(datafile, "min_k_hashes", None)
            histogram_counts = getattr(datafile, "histogram_counts", None)
            histogram_bins = getattr(datafile, "histogram_bins", None)

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
            min_k_hashes=min_k_hashes,
            histogram_counts=histogram_counts,
            histogram_bins=histogram_bins,
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
            "has_null_counts": self.null_value_counts is not None,
            "has_k_hashes": self.min_k_hashes is not None,
            "has_histograms": self.histogram_counts is not None,
        }
