import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))
sys.path.insert(1, os.path.join(sys.path[0], "../../../pyiceberg-firestore-gcs"))
sys.path.insert(1, os.path.join(sys.path[0], "../../../opteryx-catalog"))

FIRESTORE_DATABASE = os.environ.get("FIRESTORE_DATABASE")
BUCKET_NAME = os.environ.get("GCS_BUCKET")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")

import pyarrow as pa
import opteryx
from opteryx.planner.logical_planner import do_logical_planning_phase
from opteryx.third_party import sqloxide

from opteryx.connectors import OpteryxConnector
from opteryx import set_default_connector
from opteryx_catalog import OpteryxCatalog

set_default_connector(
    OpteryxConnector,
    catalog=OpteryxCatalog,
    firestore_project=GCP_PROJECT_ID,
    firestore_database=FIRESTORE_DATABASE,
    gcs_bucket=BUCKET_NAME,
)


def run_logical(sql: str) -> pa.Table:
    parsed = sqloxide.parse_sql(sql, _dialect="opteryx")
    logical_plan, ast, ctes = do_logical_planning_phase(parsed[0])
    return opteryx.execute_logical_plan(logical_plan)


def test_count_returns_one_row():
    tbl = run_logical("SELECT COUNT(1) AS cnt")
    assert isinstance(tbl, pa.Table)
    assert tbl.num_rows == 1
    data = tbl.to_pydict()
    assert "cnt" in data
    assert data["cnt"][0] == 1


def test_where_returns_no_rows():
    tbl = run_logical("SELECT one FROM (SELECT 1 AS one) AS t WHERE 1=0")
    assert isinstance(tbl, pa.Table)
    assert tbl.num_rows == 0


def test_order_by_returns_row():
    tbl = run_logical("SELECT 1 AS one ORDER BY one")
    assert isinstance(tbl, pa.Table)
    assert tbl.num_rows == 1
    data = tbl.to_pydict()["one"]
    assert data == [1]


def test_where_and_order_by_combined():
    sql = "SELECT one FROM (SELECT 1 AS one) AS t WHERE one=1 ORDER BY one"
    tbl = run_logical(sql)
    assert isinstance(tbl, pa.Table)
    assert tbl.num_rows == 1
    assert tbl.to_pydict()["one"][0] == 1


def test_planets_count():
    tbl = run_logical("SELECT COUNT(*) AS c FROM $planets")
    assert isinstance(tbl, pa.Table)
    assert tbl.num_rows == 1
    assert tbl.to_pydict()["c"][0] == 9


def test_planets_where_name():
    tbl = run_logical("SELECT * FROM public.examples.planets WHERE name = 'Earth'")
    assert isinstance(tbl, pa.Table)
    assert tbl.num_rows == 1
    d = tbl.to_pydict()
    assert d["name"][0] == "Earth"


def test_planets_order_by_id_limit():
    tbl = run_logical("SELECT id FROM $planets ORDER BY id DESC LIMIT 2")
    assert isinstance(tbl, pa.Table)
    assert tbl.num_rows == 2
    assert tbl.to_pydict()["id"] == [9, 8]

if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
