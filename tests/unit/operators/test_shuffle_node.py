from uuid import uuid4

import pyarrow as pa

from opteryx import EOS
from opteryx.draken.morsels.morsel import Morsel
from opteryx.draken.vectors.int64_vector import Int64Vector
from opteryx.managers.kvstores import create_kv_store
from opteryx.models import QueryProperties
from opteryx.operators.shuffle import BinStore
from opteryx.operators.shuffle_node import ShuffleNode


def _collect_node_outputs(node):
    rows = []
    for output in node.execute(EOS):
        if output is None or output is EOS:
            continue
        table = output.to_arrow()
        rows.extend(table.to_pydict()["k"])
    return rows


def test_shuffle_node_replays_all_rows_without_spill():
    properties = QueryProperties(query_id="test-shuffle-memory", variables={})
    node = ShuffleNode(
        properties,
        columns=["k"],
        num_bins=4,
        spill_enabled=False,
    )

    table = pa.table({"k": list(range(100)), "v": [f"v-{i}" for i in range(100)]})
    for _ in node.execute(table):
        pass

    rows = _collect_node_outputs(node)
    assert sorted(rows) == list(range(100))
    assert node.readings["shuffle_spill_chunks"] == 0


def test_shuffle_node_spills_and_replays_rows():
    properties = QueryProperties(query_id="test-shuffle-spill", variables={})
    pool_name = f"shuffle-node-{uuid4().hex}"
    kv_store = create_kv_store(f"memory://{pool_name}?pool_size_bytes=8388608")
    bin_store = BinStore(kv_store)

    node = ShuffleNode(
        properties,
        columns=["k"],
        num_bins=4,
        spill_enabled=True,
        spill_store=bin_store,
        memory_budget_bytes=256,
        target_bin_buffer_bytes=128,
        spill_codec_default="lz4",
    )

    table = pa.table(
        {
            "k": list(range(120)),
            "v": [f"value-{i:04d}" for i in range(120)],
        }
    )
    for _ in node.execute(table):
        pass

    rows = _collect_node_outputs(node)
    assert sorted(rows) == list(range(120))
    assert node.readings["shuffle_spill_chunks"] > 0
    assert node.readings["shuffle_spill_bytes"] > 0

    for bin_id in range(node.num_bins):
        assert (
            node._bin_store.iter_manifest(
                node._bin_key(bin_id),
                query_id=properties.query_id,
                operator_id=node.identity,
            )
            == []
        )


def test_shuffle_node_accepts_spill_store_uri():
    properties = QueryProperties(query_id="test-shuffle-uri", variables={})
    pool_name = f"shuffle-node-uri-{uuid4().hex}"

    node = ShuffleNode(
        properties,
        columns=["k"],
        num_bins=2,
        spill_enabled=True,
        spill_store=f"memory://{pool_name}?pool_size_bytes=8388608",
        memory_budget_bytes=256,
        target_bin_buffer_bytes=128,
    )

    table = pa.table({"k": list(range(40)), "v": [f"value-{i:04d}" for i in range(40)]})
    for _ in node.execute(table):
        pass

    rows = _collect_node_outputs(node)
    assert sorted(rows) == list(range(40))
    assert node.readings["shuffle_spill_chunks"] > 0


def test_shuffle_node_spill_replays_typed_dictionary_encoding():
    properties = QueryProperties(query_id="test-shuffle-typed-dict-spill", variables={})
    pool_name = f"shuffle-node-typed-dict-{uuid4().hex}"
    kv_store = create_kv_store(f"memory://{pool_name}?pool_size_bytes=8388608")
    bin_store = BinStore(kv_store)

    node = ShuffleNode(
        properties,
        columns=["k"],
        num_bins=4,
        spill_enabled=True,
        spill_store=bin_store,
        memory_budget_bytes=1,
        target_bin_buffer_bytes=1,
        spill_codec_default="lz4",
    )

    morsel = Morsel.from_vectors(
        ["k", "v"],
        [
            Int64Vector.from_dict([0, 1, 2, 1, 0, 2, 1, 0], [10, 20, 30]),
            Int64Vector.from_arrow(pa.array([1, 2, 3, 4, 5, 6, 7, 8], type=pa.int64())),
        ],
    )
    for _ in node.execute(morsel):
        pass

    outputs = [output for output in node.execute(EOS) if output is not None and output is not EOS]
    assert outputs

    replayed_rows = []
    saw_dictionary_sidecar = False
    for output in outputs:
        key_vector = output.column(b"k")
        replayed_rows.extend(output.to_arrow().to_pydict()["k"])
        if getattr(key_vector, "dictionary_value_type", None) is not None:
            saw_dictionary_sidecar = True

    assert sorted(replayed_rows) == [10, 10, 10, 20, 20, 20, 30, 30]
    assert saw_dictionary_sidecar
    assert node.readings["shuffle_spill_chunks"] > 0
