import datetime
from enum import Enum

import orjson


class BillingEventType(Enum):
    QUERY_EXECUTION = "QUERY_EXECUTION"
    DATA_PROCESSED_BYTES = "DATA_PROCESSED_BYTES"
    DATA_STORAGE_BYTES = "DATA_STORAGE_BYTES"


def write_billing_event(billing_event: BillingEventType, billing_account: str, event_details: dict):
    structured_log = {
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat() + "Z",
        "logName": "projects/opteryx/logs/billing_events",
        "severity": "BILLING",
        "billing_account": billing_account,
        "billing_event": billing_event.value,
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

    if billing_event == BillingEventType.DATA_STORAGE_BYTES:
        if "bytes_stored" not in event_details:
            raise ValueError(
                "Missing 'bytes_stored' in event_details for DATA_STORAGE_BYTES billing event"
            )

    structured_log["event"] = event_details

    payload = orjson.dumps(structured_log).decode()
    print(payload, flush=True)
