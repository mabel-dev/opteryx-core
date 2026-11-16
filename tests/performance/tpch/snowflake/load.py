"""
Load TPC-H SF0.01 / SF0.1 into Snowflake (its sample share stops at SF1).

Source: testdata/tpch_001 and testdata/tpch_01 (local parquet). Target:
OPTERYX_TPCH.TPCH_SF001 / TPCH_SF01, each table created LIKE the share's
SNOWFLAKE_SAMPLE_DATA.TPCH_SF1 table so column types match the larger scales.
Tables are replaced on every run; row counts are checked against the TPC-H spec.

Usage:
    python tests/performance/tpch/snowflake/load.py
"""

import glob
import os

import snowflake.connector

HERE = os.path.dirname(__file__)
TESTDATA = os.path.join(HERE, "..", "..", "..", "..", "testdata")
CONNECTION = "opteryx_tpch"
DATABASE = "OPTERYX_TPCH"
SCALES = {"001": 0.01, "01": 0.1}
# Rows per scale factor 1; nation and region are fixed. lineitem is not linear, so it is not checked.
ROWS_PER_SF = {"customer": 150_000, "orders": 1_500_000, "part": 200_000, "partsupp": 800_000, "supplier": 10_000}
FIXED_ROWS = {"nation": 25, "region": 5}


def main() -> None:
    conn = snowflake.connector.connect(connection_name=CONNECTION)
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    for label, sf in SCALES.items():
        schema = f"{DATABASE}.TPCH_SF{label}"
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        cur.execute(f"CREATE OR REPLACE STAGE {schema}.load FILE_FORMAT = (TYPE = PARQUET)")
        for table_dir in sorted(glob.glob(os.path.join(TESTDATA, f"tpch_{label}", "*"))):
            table = os.path.basename(table_dir)
            files = sorted(glob.glob(os.path.join(table_dir, "*.parquet")))
            assert files, f"no parquet in {table_dir}"
            cur.execute(f"CREATE OR REPLACE TABLE {schema}.{table} LIKE SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.{table}")
            for path in files:
                cur.execute(f"PUT 'file://{os.path.abspath(path)}' @{schema}.load/{table}/ AUTO_COMPRESS = FALSE OVERWRITE = TRUE")
            cur.execute(
                f"COPY INTO {schema}.{table} FROM @{schema}.load/{table}/"
                " MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE ON_ERROR = ABORT_STATEMENT"
            )
            rows = cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}").fetchone()[0]
            expected = FIXED_ROWS.get(table, round(ROWS_PER_SF.get(table, 0) * sf))
            if table != "lineitem" and rows != expected:
                raise RuntimeError(f"{schema}.{table}: {rows} rows, TPC-H SF{sf} has {expected}")
            print(f"{schema}.{table}: {rows} rows")
    conn.close()


if __name__ == "__main__":
    main()
