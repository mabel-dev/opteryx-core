import datetime
from enum import Enum

import orjson


class BillingEventType(Enum):
    QUERY_EXECUTION = "QUERY_EXECUTION"
    DATA_PROCESSED_BYTES = "DATA_PROCESSED_BYTES"


def write_billing_event(user: str, query_id: str, billing_event: BillingEventType):
    structured_log = {
        "timestamp": datetime.datetime.now().isoformat() + "Z",
        "spanId": query_id,
        "logName": "projects/opteryx/logs/billing_events",
        "severity": "NOTICE",
        "labels": {"billing_account": "opteryx", "billing_event": billing_event.value},
    }

    message = {
        "user": user,
        "query": "SELECT 1",
        "bytes_processed": 1,
        "billing_rate": "free-tier",
    }

    if query_id:
        structured_log["logging.googleapis.com/spanId"] = query_id

    structured_log["jsonPayload"] = message
    structured_log.update(message)

    payload = orjson.dumps(structured_log).decode()
    print(payload, flush=True)
