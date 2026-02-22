import opteryx.operators.shuffle.partitioning as partitioning
from opteryx.operators.shuffle.partitioning import normalize_num_bins
from opteryx.operators.shuffle.partitioning import row_indexes_by_bin
from opteryx.operators.shuffle.partitioning import round_down_to_allowed_bins
from opteryx.operators.shuffle.partitioning import select_num_bins_from_rows


def test_round_down_to_allowed_bins():
    assert round_down_to_allowed_bins(1) == 1
    assert round_down_to_allowed_bins(3) == 2
    assert round_down_to_allowed_bins(7) == 4
    assert round_down_to_allowed_bins(12) == 8
    assert round_down_to_allowed_bins(16) == 16
    assert round_down_to_allowed_bins(99) == 16


def test_normalize_num_bins_only_allows_power_of_two_values():
    assert normalize_num_bins(1) == 1
    assert normalize_num_bins(2) == 2
    assert normalize_num_bins(4) == 4
    assert normalize_num_bins(8) == 8
    assert normalize_num_bins(16) == 16


def test_select_num_bins_from_rows_logarithmic_scale():
    assert select_num_bins_from_rows(None) == 1
    assert select_num_bins_from_rows(1_000) == 1
    assert select_num_bins_from_rows(10_000) == 1
    assert select_num_bins_from_rows(100_000) == 1
    assert select_num_bins_from_rows(1_000_000) == 4
    assert select_num_bins_from_rows(10_000_000) == 8
    assert select_num_bins_from_rows(100_000_000) == 8
    assert select_num_bins_from_rows(1_000_000_000) == 8


def test_row_indexes_by_bin_uses_shift_and_mask(monkeypatch):
    def _emulated_compiled(hashes, num_bins, shift_bits):
        mask = num_bins - 1
        bins = [[] for _ in range(num_bins)]
        for row_index, hash_value in enumerate(hashes):
            bin_id = (int(hash_value) >> shift_bits) & mask
            bins[bin_id].append(row_index)
        flat = []
        offsets = [0]
        running = 0
        for bucket in bins:
            flat.extend(bucket)
            running += len(bucket)
            offsets.append(running)
        return flat, offsets

    monkeypatch.setattr(partitioning, "_row_indexes_by_bin_flat", _emulated_compiled)
    hashes = [0, 1, 2, 3, 4, 5, 6, 7]

    bins_no_shift = row_indexes_by_bin(hashes, num_bins=4, shift_bits=0)
    assert bins_no_shift == [[0, 4], [1, 5], [2, 6], [3, 7]]

    bins_shifted = row_indexes_by_bin(hashes, num_bins=4, shift_bits=1)
    assert bins_shifted == [[0, 1], [2, 3], [4, 5], [6, 7]]


def test_row_indexes_by_bin_uses_compiled_kernel_when_available(monkeypatch):
    def _fake_compiled(hashes, num_bins, shift_bits):
        _ = (hashes, num_bins, shift_bits)
        return ["compiled"], [0, 1]

    monkeypatch.setattr(partitioning, "_row_indexes_by_bin_flat", _fake_compiled)
    result = partitioning.row_indexes_by_bin([1, 2, 3], num_bins=1, shift_bits=0)
    assert result == [["compiled"]]


def test_row_indexes_by_bin_fails_when_compiled_kernel_missing(monkeypatch):
    monkeypatch.setattr(partitioning, "_row_indexes_by_bin_flat", None)
    try:
        partitioning.row_indexes_by_bin([0, 1, 2, 3], num_bins=2, shift_bits=0)
        assert False, "expected RuntimeError"
    except RuntimeError as err:
        assert "Compiled shuffle partition kernel unavailable" in str(err)
