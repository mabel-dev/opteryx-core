"""
Test we can read from S3

(note this is using MinIO to hit the Opteryx cache service on GCP)
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.connectors import AwsS3Connector
from tests import is_arm, is_mac, is_windows, skip_if


@skip_if(is_arm() or is_windows() or is_mac())  # reduce cost
def test_minio_storage():
    opteryx.register_store("opteryx_data", AwsS3Connector)

    conn = opteryx.connect()

    # SELECT EVERYTHING
    cur = conn.cursor()
    cur.execute("SELECT * FROM opteryx_data.public.space.planets.data;")
    assert cur.rowcount == 9, cur.rowcount

    # PROCESS THE DATA IN SOME WAY
    cur = conn.cursor()
    cur.execute(
        f"SELECT COUNT(*) AS names, name FROM opteryx_data.public.space.planets.data GROUP BY name;"
    )
    assert cur.rowcount == 9, cur.rowcount

    conn.close()


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
