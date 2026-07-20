"""Flatten a Semgrep JSON report and upload it to Opteryx.

Usage:
    python scripts/upload_semgrep_results.py <semgrep-report.json>

Reads scan metadata from environment variables (all required, set by the CI
workflow):
    UPLOAD_CLIENT / UPLOAD_TOKEN                    - upload.opteryx.app PAT
    GITHUB_REPOSITORY, GITHUB_SHA, GITHUB_REF_NAME  - provided by GitHub Actions

The upload target is a fixed workspace/collection/dataset:
    opteryx / ops / sast_findings

Depends on the `opteryx-upload` package (PyPI).
"""

import json
import os
import sys
from datetime import datetime
from datetime import timezone

from opteryx_upload import ConflictResolution
from opteryx_upload import PATAuthenticator
from opteryx_upload import Target
from opteryx_upload import UploadClient

UPLOAD_TARGET = Target(workspace="opteryx", collection="ops", dataset="sast_findings")


def flatten_report(report: dict, *, scan_time: str, repo: str, commit_sha: str, ref: str) -> list[dict]:
    rows = []
    for result in report.get("results", []) or []:
        extra = result.get("extra", {}) or {}
        metadata = extra.get("metadata", {}) or {}
        start = result.get("start", {}) or {}
        end = result.get("end", {}) or {}
        rows.append(
            {
                "scan_time": scan_time,
                "repo": repo,
                "commit_sha": commit_sha,
                "ref": ref,
                "check_id": result.get("check_id"),
                "path": result.get("path"),
                "start_line": start.get("line"),
                "end_line": end.get("line"),
                "severity": extra.get("severity"),
                "message": extra.get("message"),
                "cwe": ", ".join(metadata.get("cwe", []) or []),
                "owasp": ", ".join(metadata.get("owasp", []) or []),
            }
        )
    return rows


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: upload_semgrep_results.py <semgrep-report.json>", file=sys.stderr)
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

    ndjson_path = "semgrep_findings.ndjson"
    with open(ndjson_path, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")

    authenticator = PATAuthenticator(
        client_id=os.environ["UPLOAD_CLIENT"],
        client_secret=os.environ["UPLOAD_TOKEN"],
    )
    client = UploadClient(token=authenticator)
    commit = client.upload_and_commit(
        [ndjson_path],
        UPLOAD_TARGET,
        snapshot_message=f"semgrep scan {os.environ['GITHUB_REPOSITORY']}@{os.environ['GITHUB_SHA'][:12]}",
        conflict_resolution=ConflictResolution.APPEND,
    )
    print(f"uploaded {len(rows)} findings -> {commit.table} (commit {commit.commit_id})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
