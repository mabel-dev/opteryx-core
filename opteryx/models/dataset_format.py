# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Dataset format identity — the single place file formats are recognised and
dispatched for catalog/filesystem Scans.

A dataset is SINGLE-FORMAT by decree: format is a property of the dataset, not
of individual files, and a dataset whose data files disagree is a hard error at
discovery — never a silent drop of the minority files, never a pick-a-winner.

`FileEntry.file_format` carries these strings in manifests. The physical
planner dispatches Scan nodes on the manifest's format through SCAN_READERS —
adding a reader for a new format means a FileEntry stamp, a suffix here, and a
registry-name entry here; nothing else grows an if/elif chain.
"""

from typing import Iterable
from typing import Optional

from opteryx.exceptions import DataError

PARQUET = "PARQUET"
JSONL = "JSONL"
SKENE = "SKENE"

# Recognised DATA file suffixes. Anything not listed here is not a data file
# (manifests, sidecars, readmes) and takes no part in format discovery.
SUFFIX_TO_FORMAT = {
    ".parquet": PARQUET,
    ".jsonl": JSONL,
    ".skene": SKENE,
}

# Format → operator-registry reader name for catalog/filesystem Scans.
# A format absent here is readable as a manifest but has no Scan operator yet.
SCAN_READERS = {
    PARQUET: "Parquet Reader",
    JSONL: "JSONL Reader",
    SKENE: "Skene Reader",
}


class MixedFormatDatasetError(DataError):
    """A dataset's data files carry more than one format.

    Datasets are single-format; a mixed listing means the dataset is
    malformed, and silently dropping the minority format would misreport
    the data. Fix the dataset, not the query.
    """


def format_for_path(path: str) -> Optional[str]:
    """The format a file path's suffix declares, or None for a non-data file."""
    lowered = path.lower()
    for suffix, file_format in SUFFIX_TO_FORMAT.items():
        if lowered.endswith(suffix):
            return file_format
    return None


def dataset_format(paths: Iterable[str], dataset: str = "") -> Optional[str]:
    """Discover a dataset's format from its data-file suffixes.

    Non-data suffixes are ignored; the data files present must agree.
    Returns None for a listing with no data files (an empty relation).
    Raises MixedFormatDatasetError when they disagree.
    """
    found: Optional[str] = None
    for path in paths:
        file_format = format_for_path(path)
        if file_format is None:
            continue
        if found is None:
            found = file_format
        elif file_format != found:
            raise MixedFormatDatasetError(
                f"Dataset {dataset or '(unnamed)'} mixes {found} and {file_format} "
                "data files. Datasets are single-format; separate the formats into "
                "distinct datasets."
            )
    return found


def manifest_format(manifest, dataset: str = "") -> Optional[str]:
    """The single format a manifest's entries carry.

    Returns None for an empty manifest (an empty relation — any reader yields
    nothing). Raises MixedFormatDatasetError when entries disagree: a mixed
    manifest describes a malformed dataset.
    """
    if manifest is None or not manifest.files:
        return None
    found = manifest.files[0].file_format
    for entry in manifest.files[1:]:
        if entry.file_format != found:
            raise MixedFormatDatasetError(
                f"Dataset {dataset or '(unnamed)'} manifest mixes {found} and "
                f"{entry.file_format} files. Datasets are single-format."
            )
    return found
