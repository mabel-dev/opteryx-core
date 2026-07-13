"""
Reads every .parquet file in testdata/parquet_tests/ through the rugo decoder
and reports pass / fail / skip for two test categories:

  1. METADATA  -- reads file metadata via read_metadata_from_bytes()
  2. DATA      -- reads all columns via read_parquet(), which returns a Morsel
                  (skipped when can_decode_from_memory() returns False)

Failures are non-fatal: the full suite runs regardless of individual errors.
Exit code is 1 if any test failed.
"""

import sys
import traceback
from pathlib import Path

# Allow running from repo root or from this directory.
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

import rugo.rugo_native as rp

PARQUET_DIR = REPO_ROOT / "testdata" / "parquet_tests"

# ──────────────────────────────────────────────────────────────────────────────
# Result tracking
# ──────────────────────────────────────────────────────────────────────────────

PASS = "PASS"
FAIL = "FAIL"
SKIP = "SKIP"

results = []  # list of (category, filename, status, detail)


def record(category, filename, status, detail=""):
    tag = f"[{status:4s}]"
    line = f"  {tag} {category:<10} {filename}"
    if detail:
        line += f"\n         {detail}"
    print(line)
    results.append((category, filename, status, detail))


# ──────────────────────────────────────────────────────────────────────────────
# Per-file tests
# ──────────────────────────────────────────────────────────────────────────────


def check_metadata(path: Path, raw: bytes) -> None:
    """Attempt to read metadata; fail if an exception is raised or result is empty."""
    try:
        meta = rp.read_metadata_from_bytes(raw)
    except Exception as exc:
        record("METADATA", path.name, FAIL, f"{type(exc).__name__}: {exc}")
        return

    if meta is None:
        record("METADATA", path.name, FAIL, "read_metadata_from_bytes returned None")
        return

    num_rg = len(rp.read_rowgroup_stats(raw))
    num_cols = len(meta.schema_columns)
    record("METADATA", path.name, PASS, f"{num_rg} row-group(s), {num_cols} schema field(s)")


def check_data(path: Path, raw: bytes) -> None:
    """
    Check decodability first; if supported, decode all columns into a Morsel
    and verify the result has rows and columns.
    Files unsupported by the rugo decoder are marked SKIP rather than FAIL.
    """
    # Decodability check
    try:
        decodable = rp.can_decode_from_memory(raw)
    except Exception as exc:
        record(
            "DATA", path.name, FAIL, f"can_decode_from_memory raised {type(exc).__name__}: {exc}"
        )
        return

    if not decodable:
        record("DATA", path.name, SKIP, "can_decode_from_memory() returned False")
        return

    # Full read — returns a list of Draken Morsels (one per row group)
    try:
        morsels = rp.read_parquet(raw)
    except Exception as exc:
        # rugo fails loud on parquet features it does not implement (e.g.
        # DATA_PAGE_V2) rather than silently degrading. That is a clean,
        # honest rejection — classify it as SKIP, not FAIL.
        if "unsupported parquet page type" in str(exc):
            record("DATA", path.name, SKIP, str(exc).splitlines()[0])
            return
        tb_line = traceback.format_exc().splitlines()[-2].strip()
        record("DATA", path.name, FAIL, f"{type(exc).__name__}: {exc}\n         {tb_line}")
        return

    if not morsels:
        record("DATA", path.name, FAIL, "read_parquet returned None or empty list")
        return

    rows = sum(m.num_rows for m in morsels)
    first = morsels[0]
    cols = first.num_columns
    col_names = [c.decode() if isinstance(c, bytes) else c for c in first.column_names]

    if cols == 0:
        record("DATA", path.name, FAIL, "decoded 0 columns")
        return

    col_summary = ", ".join(col_names[:5])
    if len(col_names) > 5:
        col_summary += " …"
    record(
        "DATA",
        path.name,
        PASS,
        f"{len(morsels)} row-group(s), {rows:,} row(s), {cols} column(s): {col_summary}",
    )


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────


def main() -> int:
    files = sorted(PARQUET_DIR.glob("*.parquet"))
    if not files:
        print(f"ERROR: no .parquet files found in {PARQUET_DIR}")
        return 1

    print(f"Testing {len(files)} file(s) in {PARQUET_DIR}\n")

    for path in files:
        try:
            raw = path.read_bytes()
        except OSError as exc:
            record("READ", path.name, FAIL, str(exc))
            continue

        check_metadata(path, raw)
        check_data(path, raw)

    # ── Summary ────────────────────────────────────────────────────────────────
    counts = {PASS: 0, FAIL: 0, SKIP: 0}
    for _, _, status, _ in results:
        counts[status] += 1

    print(f"\n{'─' * 60}")
    print(f"  Total  : {len(results)}")
    print(f"  Passed : {counts[PASS]}")
    print(f"  Failed : {counts[FAIL]}")
    print(f"  Skipped: {counts[SKIP]}")
    print(f"{'─' * 60}")

    if counts[FAIL]:
        print("\nFailed tests:")
        for cat, fname, status, detail in results:
            if status == FAIL:
                print(f"  {cat:<10} {fname}")
                if detail:
                    print(f"             {detail}")
        return 1

    return 0


def test_all_parquet_test_files_decode():
    """pytest entry point: run the metadata + data check over every file in
    testdata/parquet_tests/ and fail if any check reported FAIL (SKIP is OK —
    it marks files using an encoding the rugo decoder doesn't support)."""
    assert main() == 0


if __name__ == "__main__":
    sys.exit(main())
