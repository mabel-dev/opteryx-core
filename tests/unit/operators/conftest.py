import pytest

import opteryx.operators.shuffle.partitioning as partitioning


@pytest.fixture(autouse=True)
def _inject_partition_kernel_for_tests(monkeypatch):
    """
    Unit tests run without compiling new extensions in some environments.
    Inject a deterministic test kernel only when the compiled module is absent.
    """
    if partitioning.row_indexes_by_bin_flat is not None:
        return

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

    monkeypatch.setattr(partitioning, "row_indexes_by_bin_flat", _emulated_compiled)
