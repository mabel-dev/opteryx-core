"""Flatten a Bandit JSON report and upload it to Opteryx.

Usage:
    python scripts/upload_bandit_results.py <bandit-report.json>

Authenticates with the workflow's own GitHub OIDC token - no stored
credential. The job needs `permissions: id-token: write`, and this repository
has to be registered against a service account in authenticate.opteryx.
Nothing here holds a secret, so there is nothing to rotate and nothing to leak.

Reads scan metadata from environment variables (all required, set by the CI
workflow):
    GITHUB_REPOSITORY, GITHUB_SHA, GITHUB_REF_NAME  - provided by GitHub Actions
    ACTIONS_ID_TOKEN_REQUEST_URL / _TOKEN           - provided by GitHub Actions
                                                      when id-token: write is set

The upload target is a fixed workspace/collection/dataset:
    opteryx / ops / sast_bandit

Findings are flattened to NDJSON, converted to Parquet with `rugo`, and the
Parquet file is what gets uploaded.

Depends on the `rugo` and `opteryx-upload` packages (PyPI).
"""

import json
import os
import sys
from datetime import datetime
from datetime import timezone

from opteryx_upload import ConflictResolution
from opteryx_upload import GitHubOIDCAuthenticator
from opteryx_upload import Target
from opteryx_upload import UploadClient
from rugo.jsonl import read_jsonl
from rugo.parquet import write_parquet

UPLOAD_TARGET = Target(workspace="opteryx", collection="ops", dataset="sast_bandit")


def flatten_report(report: dict, *, scan_time: str, repo: str, commit_sha: str, ref: str) -> list[dict]:
    rows = []
    for result in report.get("results", []) or []:
        cwe = result.get("issue_cwe") or {}
        rows.append(
            {
                "scan_time": scan_time,
                "repo": repo,
                "commit_sha": commit_sha,
                "ref": ref,
                "test_id": result.get("test_id"),
                "test_name": result.get("test_name"),
                "filename": result.get("filename"),
                "line_number": result.get("line_number"),
                "severity": result.get("issue_severity"),
                "confidence": result.get("issue_confidence"),
                "message": result.get("issue_text"),
                "cwe": str(cwe.get("id")) if cwe.get("id") is not None else None,
                "more_info": result.get("more_info"),
            }
        )
    return rows


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: upload_bandit_results.py <bandit-report.json>", file=sys.stderr)
        return 2

    report_path = sys.argv[1]
    with open(report_path, "r", encoding="utf-8") as fh:
        report = json.load(fh)

    scan_time = datetime.now(tz=timezone.utc).isoformat()
    rows = flatten_report(
        report,
        scan_time=scan_time,
        repo=os.environ["GITHUB_REPOSITORY"],
        commit_sha=os.environ["GITHUB_SHA"],
        ref=os.environ.get("GITHUB_REF_NAME", ""),
    )

    if not rows:
        print("no findings; nothing to upload")
        return 0

    ndjson_path = "bandit_findings.ndjson"
    with open(ndjson_path, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")

    parquet_path = "bandit_findings.parquet"
    with read_jsonl(ndjson_path) as reader:
        for morsel in reader:
            with open(parquet_path, "wb") as fh:
                fh.write(write_parquet(morsel))

    # GitHub signs a token describing this job; authenticate.opteryx verifies
    # it against GitHub's JWKS, matches the repository to a registered service
    # account, and returns the same short-lived assertion a PAT would have
    # bought. Replaced UPLOAD_CLIENT/UPLOAD_TOKEN, which were repository
    # secrets that had to be minted, stored and rotated in every repo here.
    authenticator = GitHubOIDCAuthenticator()
    client = UploadClient(token=authenticator)
    commit = client.upload_and_commit(
        [parquet_path],
        UPLOAD_TARGET,
        snapshot_message=f"bandit scan {os.environ['GITHUB_REPOSITORY']}@{os.environ['GITHUB_SHA'][:12]}",
        conflict_resolution=ConflictResolution.APPEND,
    )
    print(f"uploaded {len(rows)} findings -> {commit.table} (commit {commit.commit_id})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
