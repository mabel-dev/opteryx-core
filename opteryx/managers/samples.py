# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The sample bundles `LOAD SAMPLE` can copy into a collection.

A sample is a fixed set of tables, staged once as parquet under the deployment's
`SAMPLE_DATA_LOCATION` and copied - never referenced in place - into the collection
the caller names. Copying is what makes the loaded datasets ordinary datasets: the
caller owns the bytes, is billed for them, and may drop, compact or expire them
without any of that reaching the staged originals every other workspace loads from.

WHAT IS LOADABLE IS DESCRIBED BY THE STAGING ROOT, NOT BY THIS MODULE
--------------------------------------------------------------------
The bundles are listed in a manifest at `<SAMPLE_DATA_LOCATION>/samples.json`,
written by whoever stages them. Nothing about which samples exist, which tables they
hold, or which scale factors are built is compiled into the engine.

That is deliberate, and it is the difference between staging being self-service and
staging requiring a release. A registry in this file would mean a bundle sitting
complete in storage - or a scale factor someone spent a day generating - staying
unloadable until an engine version shipped that mentioned it by name. It would also
mean this Apache-licensed engine carrying one operator's inventory.

The cost is that planning a LOAD SAMPLE reads one small object. It is cached for
`MANIFEST_CACHE_SECONDS`, so a burst of statements pays for it once, and a newly
staged bundle becomes loadable within that window without anything being restarted.

A sample may be staged at several SCALE FACTORS, each a complete copy of the same
tables at a different size. The manifest lists them by the LABEL in the path, not by
the number it stands for: `sf001` is scale factor 0.01 and `sf01` is 0.1. Matching
what a reader wrote against those labels is `scale_label`'s whole job.
"""

import json
import time
from dataclasses import dataclass
from typing import Dict
from typing import Optional
from typing import Tuple

from opteryx.config import get as get_config

# The manifest's name under the staging root. Fixed rather than configurable: a
# deployment already says WHERE its samples are, and a second setting for what the
# listing inside that location is called buys nothing.
MANIFEST_NAME = "samples.json"

# How long a read manifest is trusted. Matches opteryx-config-client's own
# configuration cache, so "how long until a change takes effect" is one number
# across the platform rather than two that have to be remembered separately.
MANIFEST_CACHE_SECONDS = 300


@dataclass(frozen=True)
class Sample:
    """One loadable bundle, as the manifest describes it."""

    name: str
    """The name as `LOAD SAMPLE <name>` spells it, upper case."""

    description: str
    """One line, for the error that lists what can be loaded."""

    path: str
    """This bundle's directory under the deployment's sample root."""

    tables: Tuple[str, ...]
    """Every table the bundle creates."""

    scales: Tuple[str, ...]
    """Staged scale factor labels. Empty when the sample has no scales."""

    default_scale: Optional[str]
    """The label used when the statement names no scale."""


def sample_root() -> str:
    """The deployment's staging root, or raise if it has not configured one.

    Resolved at CALL time, live environment first, with the import-time constant
    only as a fallback - and in that order on purpose. On this platform the
    configuration document is promoted into `os.environ` by opteryx_config at ITS
    import, which may happen after `opteryx.config` has already snapshotted its
    constants; and the document is re-read on a 5 minute cache, so its value can
    change while the process runs. Consulting the constant first would pin the
    process to whatever was set at the instant opteryx was imported - which is the
    module-level-constant trap opteryx-config-client's own README warns about. The
    constant still answers for a host that sets it programmatically.
    """
    from opteryx.config import SAMPLE_DATA_LOCATION

    root = str(get_config("SAMPLE_DATA_LOCATION", "")).rstrip("/") or SAMPLE_DATA_LOCATION
    if not root:
        raise ValueError(
            "No sample data location is configured, so there is nothing to load "
            "from. Set SAMPLE_DATA_LOCATION to the storage prefix the sample "
            "bundles are staged under (e.g. gs://my-bucket/samples)."
        )
    return root


def _read_bytes(location: str) -> bytes:
    """Read one object, by scheme.

    Deliberately not routed through the catalog's FileIO: this runs at PLAN time,
    before a connector has been bound to the statement, so there is no catalog to
    borrow one from. The two schemes that matter are handled directly - the
    manifest is a single small object, not a data path.
    """
    if location.startswith("gs://"):
        from opteryx_catalog.iops.gcs import GcsFileIO

        with GcsFileIO().new_input(location).open() as stream:
            return stream.read()

    path = location[len("file://") :] if location.startswith("file://") else location
    with open(path, "rb") as handle:
        return handle.read()


# (root, samples, read_at). Keyed by root so that changing SAMPLE_DATA_LOCATION
# takes effect at once rather than serving the previous location's inventory.
_CACHE: Optional[Tuple[str, Dict[str, "Sample"], float]] = None


def _parse_manifest(payload: bytes, location: str) -> Dict[str, Sample]:
    """Turn the manifest's bytes into samples, or say precisely what is wrong with it.

    Every failure here names the manifest's location. It is a file an operator
    wrote, in storage, out of sight of whoever is running the statement - so the
    error has to be enough for the person reading it to go and look at the right
    thing.
    """
    try:
        document = json.loads(payload)
    except ValueError as err:
        raise ValueError(f"The sample manifest at {location} is not valid JSON: {err}") from err

    entries = document.get("samples") if isinstance(document, dict) else None
    if not isinstance(entries, list):
        raise ValueError(
            f"The sample manifest at {location} has no 'samples' list. It should be "
            '{"version": 1, "samples": [...]}.'
        )

    samples: Dict[str, Sample] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError(f"The sample manifest at {location} holds a non-object entry.")
        try:
            name = str(entry["name"]).upper()
            path = str(entry["path"])
            tables = tuple(str(table) for table in entry["tables"])
        except (KeyError, TypeError) as err:
            raise ValueError(
                f"An entry in the sample manifest at {location} is missing a required "
                f"field ({err}). Each needs at least 'name', 'path' and 'tables'."
            ) from err

        # An absolute path would let one manifest entry point outside the root the
        # deployment configured - at another bucket, or at another tenant's data.
        # The root is the boundary; a bundle names a directory inside it.
        if "://" in path or path.startswith("/") or ".." in path.split("/"):
            raise ValueError(
                f"Sample '{name}' in the manifest at {location} has path '{path}'. A "
                "bundle's path is relative to the sample root and cannot leave it."
            )

        scales = tuple(str(scale) for scale in entry.get("scales", ()))
        default_scale = entry.get("default_scale")
        if default_scale is not None:
            default_scale = str(default_scale)
            if scales and default_scale not in scales:
                raise ValueError(
                    f"Sample '{name}' in the manifest at {location} defaults to scale "
                    f"'{default_scale}', which is not among its staged scales "
                    f"{list(scales)}."
                )

        samples[name] = Sample(
            name=name,
            description=str(entry.get("description", "")),
            path=path,
            tables=tables,
            scales=scales,
            default_scale=default_scale,
        )

    return samples


def load_manifest(refresh: bool = False) -> Dict[str, Sample]:
    """Every loadable sample, from the staging root's manifest.

    Cached for `MANIFEST_CACHE_SECONDS`. A failed read is NOT cached: a manifest
    that was briefly unreachable should start working again as soon as it is,
    rather than staying broken for the rest of the cache window.
    """
    global _CACHE  # noqa: PLW0603 - module-level memo, keyed by the root it was read from

    root = sample_root()
    now = time.monotonic()
    if not refresh and _CACHE is not None:
        cached_root, cached_samples, read_at = _CACHE
        if cached_root == root and (now - read_at) < MANIFEST_CACHE_SECONDS:
            return cached_samples

    location = f"{root}/{MANIFEST_NAME}"
    try:
        payload = _read_bytes(location)
    except FileNotFoundError as err:
        raise ValueError(
            f"There is no sample manifest at {location}, so nothing can be loaded. "
            "Whoever stages the bundles writes it; check SAMPLE_DATA_LOCATION points "
            "at the staging root."
        ) from err
    if not payload:
        raise ValueError(f"The sample manifest at {location} is empty.")

    samples = _parse_manifest(payload, location)
    _CACHE = (root, samples, now)
    return samples


def get_sample(name: str) -> Optional[Sample]:
    """Look a sample up by the name the reader wrote, case-insensitively."""
    return load_manifest().get(name.upper())


def sample_names() -> Tuple[str, ...]:
    """Every loadable sample name, for error messages."""
    return tuple(sorted(load_manifest()))


def _label_as_number(label: str) -> float:
    """The scale factor a staged label stands for (`001` -> 0.01, `10` -> 10)."""
    return float(f"0.{label[1:]}" if label.startswith("0") else label)


def scale_label(sample: Sample, scale: Optional[str]) -> Optional[str]:
    """Turn the scale the reader wrote into the staged label it names.

    The reader writes a NUMBER (`AT SCALE 0.1`), the manifest holds a LABEL
    (`01`), and the two are not the same string. Matching numerically is what
    makes `0.1`, `.1` and `0.10` all find `01` - comparing the text would accept
    only whichever spelling the label happened to resemble.

    Parameters:
        sample: the bundle being loaded.
        scale:  the number as written, or None for the bundle's default.

    Returns:
        The staged label, or None when nothing staged matches.
    """
    if scale is None:
        return sample.default_scale
    try:
        wanted = float(scale)
    except ValueError:
        return None
    for label in sample.scales:
        try:
            if _label_as_number(label) == wanted:
                return label
        except ValueError:
            # A label that is not a number cannot be named by `AT SCALE` at all.
            # Skipped rather than fatal: one odd entry should not make the labels
            # beside it unreachable.
            continue
    return None


def scale_numbers(sample: Sample) -> Tuple[str, ...]:
    """The staged scale factors as the reader would write them, for error messages."""
    numbers = []
    for label in sample.scales:
        try:
            numbers.append(f"{_label_as_number(label):g}")
        except ValueError:
            continue
    return tuple(numbers)


def table_location(sample: Sample, label: str, table: str) -> str:
    """Where one table's parquet files are staged for one scale factor."""
    return f"{sample_root()}/{sample.path}/sf{label}/{table}"
