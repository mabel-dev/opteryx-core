"""DatasetReadError says WHICH half of a read failed.

One class used to cover both a dataset whose metadata (schema, snapshot,
manifest) could not be read and a planned data file whose bytes could not be
fetched. A caller reporting the failure had to guess, and guessed "metadata"
for a data-file range read that failed - sending the diagnosis to the wrong
place. The two subclasses let a caller say which happened; both remain
DatasetReadError so every existing handler still catches them.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.exceptions import DatasetDataReadError
from opteryx.exceptions import DatasetMetadataReadError
from opteryx.exceptions import DatasetReadError


def test_both_halves_are_still_dataset_read_errors():
    assert issubclass(DatasetMetadataReadError, DatasetReadError)
    assert issubclass(DatasetDataReadError, DatasetReadError)


def test_the_halves_are_distinct():
    assert not issubclass(DatasetMetadataReadError, DatasetDataReadError)
    assert not issubclass(DatasetDataReadError, DatasetMetadataReadError)


def test_binder_wraps_a_failed_dataset_description_as_metadata():
    """The binder's catch-all around reading a relation's description raises the
    metadata half - nothing has been planned, so no data file was touched."""
    from opteryx.planner.binder import dataset

    # the planner is compiled, so inspect cannot reach its source - read the .py
    source = open(os.path.join(os.path.dirname(dataset.__file__), "dataset.py")).read()
    assert 'raise DatasetMetadataReadError(f"Cannot read information for dataset' in source

