import faulthandler

#!/usr/bin/env python
import os
import sys

from opteryx_catalog import OpteryxCatalog

import opteryx

# Set the default connector
from opteryx import config
from opteryx import register_workspace
from opteryx import set_default_connector
from opteryx.connectors import DiskConnector
from opteryx.connectors import OpteryxConnector

# Enable fault handler to get better debugging info
faulthandler.enable()
# Write to file for better output
with open('/tmp/faulthandler.log', 'w') as f:
    faulthandler.dump_traceback(file=f)

os.environ["OPTERYX_DEBUG"] = "1"
os.environ["OPTERYX_TRACE"] = "1"

sys.path.insert(1, os.path.join(sys.path[0], "../../../mabel/orso"))
sys.path.insert(1, os.path.join(sys.path[0], "."))
sys.path.insert(1, os.path.join(sys.path[0], "../../pyiceberg-firestore-gcs"))



FIRESTORE_DATABASE = os.environ.get("FIRESTORE_DATABASE")
BUCKET_NAME = os.environ.get("GCS_BUCKET")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")

set_default_connector(
    OpteryxConnector,
    catalog=OpteryxCatalog,
    firestore_project=GCP_PROJECT_ID,
    firestore_database=FIRESTORE_DATABASE,
    gcs_bucket=BUCKET_NAME,
)

register_workspace("scratch", DiskConnector)
register_workspace("testdata", DiskConnector)

print("Starting test...")

try:
    session = opteryx.session(user="justin", query_id="0000", memberships=["Apollo 11", "opteryx"])
    sql = "SELECT * FROM $planets WHERE id = 1"
    print(f"Executing: {sql}")
    result = session.execute_to_arrow(sql, visibility_filters={'.opteryx.ops.billing': [('billing_account', 'NotEq', 'free')]})
    print(f"Success! Got {len(result)} rows, {result.num_columns} columns")
    session.close()
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
