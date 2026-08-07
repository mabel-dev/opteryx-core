"""Dataset format discovery — single-format enforcement and Scan dispatch."""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "..", "..", ".."))

import pytest

from opteryx.models.dataset_format import JSONL
from opteryx.models.dataset_format import PARQUET
from opteryx.models.dataset_format import SKENE
from opteryx.models.dataset_format import MixedFormatDatasetError
from opteryx.models.dataset_format import dataset_format
from opteryx.models.dataset_format import format_for_path


def test_suffix_recognition_is_case_insensitive():
    assert format_for_path("a/b/data-1.parquet") == PARQUET
    assert format_for_path("a/b/DATA-1.PARQUET") == PARQUET
    assert format_for_path("a/b/data.jsonl") == JSONL
    assert format_for_path("a/b/data.skene") == SKENE


def test_non_data_files_are_not_formats():
    assert format_for_path("a/b/manifest.json") is None
    assert format_for_path("a/b/readme.md") is None
    assert format_for_path("a/b/$dropped") is None


def test_dataset_format_ignores_non_data_files():
    assert dataset_format(["x.parquet", "readme.md", "y.parquet"]) == PARQUET
    assert dataset_format(["x.jsonl"]) == JSONL
    assert dataset_format(["x.skene", "manifest.json"]) == SKENE


def test_empty_listing_has_no_format():
    assert dataset_format([]) is None
    assert dataset_format(["readme.md"]) is None


def test_mixed_dataset_raises_never_drops():
    with pytest.raises(MixedFormatDatasetError):
        dataset_format(["x.parquet", "y.skene"], "mixed")
    with pytest.raises(MixedFormatDatasetError):
        dataset_format(["x.jsonl", "y.parquet"], "mixed")


if __name__ == "__main__":
    test_suffix_recognition_is_case_insensitive()
    test_non_data_files_are_not_formats()
    test_dataset_format_ignores_non_data_files()
    test_empty_listing_has_no_format()
    test_mixed_dataset_raises_never_drops()
    print("✅ okay")
