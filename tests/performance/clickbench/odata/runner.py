"""
OData ClickBench Benchmark (opteryx.app)
========================================
Runs the 43 ClickBench queries against opteryx.app as OData reads of the views
benchmarks.clickbench.q_00 .. q_42 (created by dev/clickbench/web_views.sql):

    GET https://odata.opteryx.app/api/v4/benchmarks/clickbench/q_NN

Each try is timed from request to the last page of the response body parsed
(following @odata.nextLink), so it measures what an OData client sees: auth
excluded, network and serialisation included.

Credentials follow the ClickBench convention for hosted systems - they come
from the environment, never from the repo or the result file:

    CLIENT_ID      opteryx.app client id
    CLIENT_SECRET  opteryx.app client secret

`--env-file PATH` loads them from a dotenv-style file first (the make target
passes the gitignored repo-root .env). A variable already set in the
environment is not overridden.

Output: odata/results.local.json (ClickBench result format; a failed try is
null and the run exits non-zero).

Usage:
    CLIENT_ID=... CLIENT_SECRET=... python tests/performance/clickbench/odata/runner.py
    python tests/performance/clickbench/odata/runner.py --env-file .env
"""

import argparse
import datetime
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

AUTH_URL = "https://authenticate.opteryx.app/token"
ODATA_BASE = "https://odata.opteryx.app/api/v4/benchmarks/clickbench"
QUERY_COUNT = 43
TRIES = 3
TIMEOUT = 600
# Re-authenticate this many seconds before the token says it expires.
TOKEN_EXPIRY_SKEW = 60


def load_env_file(path: str) -> None:
    with open(path) as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            val = val.strip()
            if len(val) >= 2 and val[0] == val[-1] and val[0] in "\"'":
                val = val[1:-1]
            os.environ.setdefault(key.strip(), val)


class Token:
    def __init__(self, client_id: str, client_secret: str) -> None:
        self._client_id = client_id
        self._client_secret = client_secret
        self._header = None
        self._expires_at = 0.0

    def header(self) -> str:
        if self._header is None or time.monotonic() >= self._expires_at:
            self._refresh()
        return self._header

    def _refresh(self) -> None:
        body = urllib.parse.urlencode(
            {
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            }
        ).encode()
        req = urllib.request.Request(
            AUTH_URL,
            data=body,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            payload = json.loads(resp.read())
        token = payload["access_token"]
        token_type = payload.get("token_type", "bearer")
        self._header = f"{token_type[0].upper()}{token_type[1:].lower()} {token}"
        self._expires_at = time.monotonic() + float(payload["expires_in"]) - TOKEN_EXPIRY_SKEW


def read_view(url: str, token: Token) -> int:
    """Read every page of an OData entity set; returns the row count."""
    rows = 0
    while url is not None:
        req = urllib.request.Request(
            url, headers={"Accept": "application/json", "Authorization": token.header()}
        )
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            payload = json.loads(resp.read())
        rows += len(payload["value"])
        url = payload.get("@odata.nextLink")
    return rows


def describe_error(exc: Exception) -> str:
    if isinstance(exc, urllib.error.HTTPError):
        detail = exc.read().decode(errors="replace")[:500]
        return f"HTTP {exc.code}: {detail}"
    return f"{type(exc).__name__}: {exc}"


def run_benchmark(token: Token) -> tuple[list, int]:
    results = []
    failures = 0
    for idx in range(QUERY_COUNT):
        url = f"{ODATA_BASE}/q_{idx:02d}"
        times = []
        print(f"  Q{idx:02d}/{QUERY_COUNT} ", end="", flush=True)
        for attempt in range(TRIES):
            t0 = time.monotonic()
            try:
                read_view(url, token)
                elapsed = round(time.monotonic() - t0, 3)
            except (urllib.error.URLError, TimeoutError) as exc:
                print(f"\n    ERROR on attempt {attempt + 1}: {describe_error(exc)}")
                elapsed = None
                failures += 1
            times.append(elapsed)
            status = f"{elapsed:.3f}s" if elapsed is not None else "ERR"
            print(status, end="  " if attempt < TRIES - 1 else "\n", flush=True)
        results.append(times)
    return results, failures


def main() -> int:
    parser = argparse.ArgumentParser(description="OData ClickBench benchmark against opteryx.app")
    parser.add_argument("--env-file", default=None, help="dotenv file to load CLIENT_ID/CLIENT_SECRET from")
    parser.add_argument(
        "--output",
        default=os.path.join(os.path.dirname(__file__), "results.local.json"),
        help="Output JSON path (default: results.local.json next to this script)",
    )
    args = parser.parse_args()

    if args.env_file is not None:
        load_env_file(args.env_file)
    missing = [v for v in ("CLIENT_ID", "CLIENT_SECRET") if not os.environ.get(v)]
    if missing:
        print(f"ERROR: {', '.join(missing)} not set", file=sys.stderr)
        return 1

    token = Token(os.environ["CLIENT_ID"], os.environ["CLIENT_SECRET"])
    token.header()  # authenticate up front: a credential failure stops the run before any timing

    print("Opteryx ClickBench — OData (opteryx.app)")
    print(f"  Endpoint       : {ODATA_BASE}/q_NN")
    print(f"  Tries per query: {TRIES}")
    print(f"  Output         : {args.output}")
    print()

    start = time.monotonic()
    results, failures = run_benchmark(token)
    print(f"\nTotal benchmark time: {time.monotonic() - start:.1f}s")

    payload = {
        "system": "Opteryx (opteryx.app, OData)",
        "date": datetime.date.today().isoformat(),
        "machine": "opteryx.app",
        "cluster_size": "serverless",
        "proprietary": "no",
        "hardware": "cpu",
        "tuned": "no",
        "tags": ["C++", "column-oriented", "serverless", "managed", "no-cold"],
        "load_time": 0,
        "data_size": 0,
        "result": results,
    }
    with open(args.output, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"Results written to {args.output}")

    if failures:
        print(f"ERROR: {failures} of {QUERY_COUNT * TRIES} tries failed", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
