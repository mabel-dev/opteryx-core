import datetime
import os
from enum import Enum

from opteryx.third_party.yyjson import dumps as json_dumps


# Usage which arrives without an identified billing account is attributed here.
# This is the house account: internal queries, system tasks and anything running
# without a caller-supplied account land on it rather than going unattributed.
DEFAULT_BILLING_ACCOUNT = "opteryx"


class BillingEventType(Enum):
    QUERY_EXECUTION = "QUERY_EXECUTION"
    DATA_PROCESSED_BYTES = "DATA_PROCESSED_BYTES"
    DATA_STORAGE_BYTES = "DATA_STORAGE_BYTES"


def write_billing_event(
    billing_event: BillingEventType,
    billing_account: str,
    event_details: dict,
    actor: str = None,
    workspace: str = None,
):
    """Emit one billing event.

    Three fields answer three different questions and must not be conflated -
    the `billing_account` column used to hold whichever of them the emitter
    happened to know, which made it unusable as a payer:

      billing_account  WHO PAYS. Always resolved by the time it gets here.
      actor            WHO DID IT. The identity the session runs as; None for
                       usage with no actor at all (the storage sampler).
      workspace        WHERE IT HAPPENED, but only where that is a SINGLE
                       value. A query can read four workspaces and write a
                       fifth, so it is None for caller-submitted SQL and set
                       only for single-target platform work (a materialized
                       view refresh, an OPTIMIZE) where the submitting path
                       knows the one workspace involved.
    """
    structured_log = {
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat() + "Z",
        "logName": "projects/opteryx/logs/billing_events",
        "severity": "BILLING",
        "billing_account": billing_account,
        "billing_event": billing_event.value,
        "actor": actor,
        "workspace": workspace,
    }

    if billing_event == BillingEventType.QUERY_EXECUTION:
        if "query" not in event_details:
            raise ValueError("Missing 'query' in event_details for QUERY_EXECUTION billing event")
        if "user" not in event_details:
            raise ValueError("Missing 'user' in event_details for QUERY_EXECUTION billing event")

    if billing_event == BillingEventType.DATA_PROCESSED_BYTES:
        if "bytes_processed" not in event_details:
            raise ValueError(
                "Missing 'bytes_processed' in event_details for DATA_PROCESSED_BYTES billing event"
            )
        if "user" not in event_details:
            raise ValueError(
                "Missing 'user' in event_details for DATA_PROCESSED_BYTES billing event"
            )

    if billing_event == BillingEventType.DATA_STORAGE_BYTES and "bytes_stored" not in event_details:
        raise ValueError(
            "Missing 'bytes_stored' in event_details for DATA_STORAGE_BYTES billing event"
        )

    structured_log["event"] = event_details

    if os.environ.get("K_SERVICE"):
        payload = json_dumps(structured_log).decode()
        print(payload, flush=True)
