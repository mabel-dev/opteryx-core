# isort: skip_file
import sys
import os
from array import array

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

from opteryx.third_party.maki_nage import distogram


def test_count():
    h = distogram.Distogram(bin_count=3)
    assert h.count() == 0

    distogram.update(h, 16, count=4)
    assert h.count() == 4
    distogram.update(h, 23, count=3)
    assert h.count() == 7
    distogram.update(h, 28, count=5)
    assert h.count() == 12


def test_count_preserved_when_trimming():
    h = distogram.Distogram(bin_count=3)

    for i in range(100):
        distogram.update(h, float(i))

    assert h.count() == 100


def test_load_counts_matches_equivalent_bins():
    counts = [0, 3, 5, 0, 7]
    minimum = 10.0
    maximum = 20.0
    span = maximum - minimum
    bins = [
        (minimum + (idx + 0.5) * span / len(counts), count)
        for idx, count in enumerate(counts)
        if count
    ]

    h_from_bins = distogram.load(bins, minimum, maximum)
    h_from_counts = distogram.load_counts(counts, minimum, maximum)

    assert h_from_counts.count() == sum(counts)
    assert h_from_counts.bins == h_from_bins.bins
    assert distogram.count_up_to(h_from_counts, 15.0) == distogram.count_up_to(h_from_bins, 15.0)


def test_load_counts_i64_matches_equivalent_bins():
    counts = array("q", [0, 3, 5, 0, 7])
    minimum = 10.0
    maximum = 20.0
    span = maximum - minimum
    bins = [
        (minimum + (idx + 0.5) * span / len(counts), count)
        for idx, count in enumerate(counts)
        if count
    ]

    h_from_bins = distogram.load(bins, minimum, maximum)
    h_from_counts = distogram.load_counts_i64(counts, minimum, maximum)

    assert h_from_counts.count() == sum(counts)
    assert h_from_counts.bins == h_from_bins.bins
    assert distogram.count_up_to(h_from_counts, 15.0) == distogram.count_up_to(h_from_bins, 15.0)
