"""Tests for `LOAD SAMPLE <sample> INTO <workspace>.<collection> [AT SCALE <n>]`.

LOAD SAMPLE copies a staged sample bundle into a collection the caller owns. It is
recognized before the parser (pre_parse) rather than re-spelled onto another
statement's grammar, because sqlparser's Opteryx dialect has no LOAD statement and
there is no statement shape with a name, a target and a scale to borrow.

The rules these hold:

- The sample and the scale are both settled at PLAN time. Both sets are closed and
  known without touching storage, so naming one that does not exist reads as a syntax
  problem with the alternatives listed - not as a copy that starts and finds an empty
  prefix.
- The target is a collection, always `workspace.collection`. A bare name would resolve
  to some default workspace and write a whole bundle somewhere the caller did not name.
- The scale is matched NUMERICALLY against the staged labels. The reader writes a
  number (`AT SCALE 0.1`), the bucket holds a label (`sf01`), and the two are not the
  same string.
- WHERE bundles are staged, and WHICH ones exist, are both properties of the
  deployment rather than of the engine. The root is configuration; the inventory is
  a manifest at that root. A build with no `SAMPLE_DATA_LOCATION`, or a root with no
  manifest, says so rather than reaching for whichever bucket the author used.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.getcwd()))

import pytest

import json

import opteryx
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.managers.samples import get_sample
from opteryx.managers.samples import scale_label
from opteryx.planner.logical_planner.logical_planner import plan_load_sample
from opteryx.planner.pre_parse import pre_parse

# The bundle these tests plan against. A local manifest, not a constant in the
# engine - which is the point: staging a sample must not need a release.
MANIFEST = {
    "version": 1,
    "samples": [
        {
            "name": "TPCH",
            "path": "tpch",
            "description": "The TPC-H decision support schema",
            "tables": [
                "region",
                "nation",
                "supplier",
                "customer",
                "part",
                "partsupp",
                "orders",
                "lineitem",
            ],
            "scales": ["001", "01", "1", "5", "10"],
            "default_scale": "1",
        }
    ],
}


def _stage(tmp_path, monkeypatch, document=None):
    """Point the engine at a root holding `document` as its manifest."""
    import opteryx.managers.samples as samples

    (tmp_path / samples.MANIFEST_NAME).write_text(
        json.dumps(MANIFEST if document is None else document)
    )
    monkeypatch.setenv("SAMPLE_DATA_LOCATION", str(tmp_path))
    monkeypatch.setattr(samples, "_CACHE", None)
    return tmp_path


@pytest.fixture(autouse=True)
def sample_root(tmp_path, monkeypatch):
    """Every test here plans a statement, and planning needs a root with a manifest."""
    _stage(tmp_path, monkeypatch)


def _plan(sql):
    statements = pre_parse(sql)
    assert statements is not None, f"pre_parse did not recognize: {sql}"
    plan = plan_load_sample(statements[0])
    return list(plan.nodes(True))[0][1]


def test_load_sample_is_recognized_before_the_parser():
    assert pre_parse("LOAD SAMPLE TPCH INTO personal.demo") is not None
    # Everything else is an ordinary statement the parser handles.
    assert pre_parse("SELECT * FROM $planets") is None


def test_load_sample_defaults_to_the_samples_own_scale():
    node = _plan("LOAD SAMPLE TPCH INTO personal.demo")
    assert node.sample_name == "TPCH"
    assert node.collection_name == "personal.demo"
    assert node.scale_label == "1"
    assert node.tables == tuple(MANIFEST["samples"][0]["tables"])


@pytest.mark.parametrize(
    "written, label",
    [("0.01", "001"), ("0.1", "01"), (".1", "01"), ("1", "1"), ("5", "5"), ("10", "10")],
)
def test_scale_is_matched_numerically_not_textually(written, label):
    """`0.1`, `.1` and `0.10` all name sf01; the label is not the number's spelling."""
    node = _plan(f"LOAD SAMPLE TPCH INTO personal.demo AT SCALE {written}")
    assert node.scale_label == label


def test_sample_name_is_case_insensitive():
    assert _plan("load sample tpch into personal.demo").sample_name == "TPCH"


def test_unstaged_scale_is_refused_with_the_staged_ones():
    with pytest.raises(UnsupportedSyntaxError) as err:
        _plan("LOAD SAMPLE TPCH INTO personal.demo AT SCALE 3")
    message = str(err.value)
    assert "3" in message
    # The refusal lists what IS staged, so the reader can correct it in one step.
    assert "0.01" in message and "10" in message


def test_unknown_sample_is_refused_with_a_suggestion():
    with pytest.raises(UnsupportedSyntaxError) as err:
        _plan("LOAD SAMPLE TPCD INTO personal.demo")
    assert "TPCH" in str(err.value)


def test_target_must_name_a_collection():
    # A bare name has no workspace, a three-part name is a relation not a collection.
    for target in ("demo", "personal.demo.orders"):
        with pytest.raises(UnsupportedSyntaxError):
            _plan(f"LOAD SAMPLE TPCH INTO {target}")


def test_load_is_not_a_general_keyword():
    """A LOAD that is not a LOAD SAMPLE is named here, not left to the parser."""
    with pytest.raises(UnsupportedSyntaxError) as err:
        pre_parse("LOAD TPCH")
    assert "LOAD SAMPLE" in str(err.value)


def test_at_scale_not_bare_at():
    """`AT` alone after an object name is the version space, so it is not accepted here."""
    with pytest.raises(UnsupportedSyntaxError):
        pre_parse("LOAD SAMPLE TPCH INTO personal.demo AT 10")


def test_statement_classifies_as_writer_tier_ddl():
    """The jobs API pre-flights with this; 'denied' would reject it at submission."""
    info = opteryx.analyze_query("LOAD SAMPLE TPCH INTO personal.demo AT SCALE 1")
    assert info["query_type"] == "LoadSample"
    assert info["is_ddl"] is True
    assert info["permission_required"] == "writer"
    # The permission target is the collection written to, not the sample's name.
    assert info["tables"] == ["personal.demo"]


def test_bundle_location_is_relative_to_the_configured_root(monkeypatch):
    """The bundle carries a relative path; the deployment supplies the root."""
    from opteryx.managers.samples import table_location

    sample = get_sample("TPCH")
    monkeypatch.setenv("SAMPLE_DATA_LOCATION", "gs://somewhere/else/")
    # A trailing slash on the configured root does not double up.
    assert table_location(sample, "10", "lineitem") == "gs://somewhere/else/tpch/sf10/lineitem"

    monkeypatch.setenv("SAMPLE_DATA_LOCATION", "file:///data/samples")
    assert table_location(sample, "1", "orders") == "file:///data/samples/tpch/sf1/orders"


def test_a_later_config_change_is_picked_up(monkeypatch):
    """The value is resolved per call, not snapshotted when opteryx was imported.

    opteryx_config promotes the configuration document into os.environ at its own
    import, and re-reads it on a 5 minute cache - so a root captured at opteryx's
    import can be both late and stale.
    """
    from opteryx.managers.samples import table_location

    sample = get_sample("TPCH")
    monkeypatch.setattr("opteryx.config.SAMPLE_DATA_LOCATION", "gs://captured-at-import")
    monkeypatch.setenv("SAMPLE_DATA_LOCATION", "gs://set-afterwards")
    assert table_location(sample, "1", "region") == "gs://set-afterwards/tpch/sf1/region"


def test_unconfigured_deployment_is_refused_at_plan_time(monkeypatch):
    """Refused before the collection is created, not part-way through copying."""
    from opteryx.config import get as get_config  # noqa: F401 - import parity with samples

    monkeypatch.setenv("SAMPLE_DATA_LOCATION", "")
    monkeypatch.setattr("opteryx.config.SAMPLE_DATA_LOCATION", "")
    with pytest.raises(ValueError) as err:
        _plan("LOAD SAMPLE TPCH INTO personal.demo")
    assert "SAMPLE_DATA_LOCATION" in str(err.value)


def test_a_bundle_cannot_escape_the_sample_root(tmp_path, monkeypatch):
    """The configured root is the boundary; a manifest entry names a place inside it."""
    for escape in ("gs://another-bucket/tpch", "/etc", "../../elsewhere"):
        document = json.loads(json.dumps(MANIFEST))
        document["samples"][0]["path"] = escape
        _stage(tmp_path, monkeypatch, document)
        with pytest.raises(ValueError) as err:
            get_sample("TPCH")
        assert "cannot leave it" in str(err.value)


def test_a_newly_staged_sample_needs_no_engine_change(tmp_path, monkeypatch):
    """The whole reason the inventory is a manifest: staging is self-service."""
    document = json.loads(json.dumps(MANIFEST))
    document["samples"].append(
        {"name": "MOVR", "path": "movr", "tables": ["users", "vehicles"], "scales": ["1"],
         "default_scale": "1"}
    )
    _stage(tmp_path, monkeypatch, document)
    node = _plan("LOAD SAMPLE MOVR INTO personal.demo")
    assert node.sample_name == "MOVR"
    assert node.tables == ("users", "vehicles")


def test_a_broken_manifest_names_itself(tmp_path, monkeypatch):
    """It is a file in storage the reader cannot see; the error has to point at it."""
    import opteryx.managers.samples as samples

    (tmp_path / samples.MANIFEST_NAME).write_text("{not json")
    monkeypatch.setenv("SAMPLE_DATA_LOCATION", str(tmp_path))
    monkeypatch.setattr(samples, "_CACHE", None)
    with pytest.raises(ValueError) as err:
        _plan("LOAD SAMPLE TPCH INTO personal.demo")
    assert samples.MANIFEST_NAME in str(err.value)


def test_a_root_with_no_manifest_is_refused(tmp_path, monkeypatch):
    import opteryx.managers.samples as samples

    monkeypatch.setenv("SAMPLE_DATA_LOCATION", str(tmp_path / "empty"))
    monkeypatch.setattr(samples, "_CACHE", None)
    with pytest.raises(ValueError) as err:
        _plan("LOAD SAMPLE TPCH INTO personal.demo")
    assert "no sample manifest" in str(err.value)


def test_every_staged_scale_resolves():
    """No label in the registry is unreachable from something a reader could write."""
    sample = get_sample("TPCH")
    for label in sample.scales:
        number = f"{float(f'0.{label[1:]}' if label.startswith('0') else label):g}"
        assert scale_label(sample, number) == label


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
