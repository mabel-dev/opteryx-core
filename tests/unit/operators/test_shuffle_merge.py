import pyarrow as pa

from opteryx import EOS
from opteryx.draken.morsels.morsel import Morsel
from opteryx.models import QueryProperties
from opteryx.operators.shuffle import ShuffleMergeOperation
from opteryx.operators.shuffle import ShuffleMergeSortOperation
from opteryx.operators.shuffle import SortKey
from opteryx.operators.shuffle_node import ShuffleNode


def _morsel(data: dict) -> Morsel:
    return Morsel.from_arrow(pa.table(data))


def _rows(morsel: Morsel):
    return morsel.to_arrow().to_pylist()


def test_shuffle_merge_operation_concatenates_streams_without_reordering():
    m1 = _morsel({"k": [1, 2], "v": ["a", "b"]})
    m2 = _morsel({"k": [3], "v": ["c"]})
    m3 = _morsel({"k": [4, 5], "v": ["d", "e"]})

    merged = ShuffleMergeOperation.merge_streams([m1, [m2, m3]])
    merged_rows = []
    for morsel in merged:
        merged_rows.extend(_rows(morsel))

    assert [row["k"] for row in merged_rows] == [1, 2, 3, 4, 5]


def test_shuffle_merge_sort_operation_kway_asc():
    s1 = _morsel({"k": [1, 3, 5], "v": ["a", "c", "e"]})
    s2 = _morsel({"k": [2, 4, 6], "v": ["b", "d", "f"]})

    sorter = ShuffleMergeSortOperation(order_by=[SortKey(column="k", direction="ASC")])
    merged = sorter.merge_sorted_streams([[s1], [s2]])

    assert [row["k"] for row in _rows(merged)] == [1, 2, 3, 4, 5, 6]


def test_shuffle_merge_sort_operation_kway_desc_with_nulls_last():
    s1 = _morsel({"k": [9, 4, None], "v": ["a", "b", "x"]})
    s2 = _morsel({"k": [8, 5, None], "v": ["c", "d", "y"]})

    sorter = ShuffleMergeSortOperation(order_by=[SortKey(column="k", direction="DESC")])
    merged = sorter.merge_sorted_streams([[s1], [s2]])

    assert [row["k"] for row in _rows(merged)] == [9, 8, 5, 4, None, None]


def test_shuffle_merge_sort_after_shuffle_bins():
    table = pa.table(
        {
            "k": [5, 1, 7, 3, 6, 2, 4, 8],
            "payload": [f"row-{i}" for i in range(8)],
        }
    )
    properties = QueryProperties(query_id="merge-after-shuffle", variables={})
    shuffle = ShuffleNode(properties, columns=["k"], num_bins=4, spill_enabled=False)

    for _ in shuffle.execute(table):
        pass

    bin_streams = []
    for output in shuffle.execute(EOS):
        if output is None or output is EOS:
            continue
        bin_streams.append([output])

    sorter = ShuffleMergeSortOperation(order_by=[("k", "ASC")])
    sorted_bin_streams = []
    for stream in bin_streams:
        sorted_bin_streams.append([sorter.sort_single_stream(stream)])

    merged = sorter.merge_sorted_streams(sorted_bin_streams)
    assert [row["k"] for row in _rows(merged)] == [1, 2, 3, 4, 5, 6, 7, 8]
