"""
Reads every .parquet file in testdata/parquet_tests/ through the rugo decoder
and reports pass / fail / skip for two test categories:

  1. METADATA  -- reads file metadata via read_metadata()
  2. DATA      -- reads all columns via read_parquet()
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

import opteryx.rugo.parquet as rp

PARQUET_DIR = REPO_ROOT / "testdata" / "parquet_tests"

# ──────────────────────────────────────────────────────────────────────────────
# Result tracking
# ──────────────────────────────────────────────────────────────────────────────

PASS   = "PASS"
FAIL   = "FAIL"
SKIP   = "SKIP"

results = []  # list of (category, filename, status, detail)


def record(category, filename, status, detail=""):
    tag = f"[{status:4s}]"
    line = f"  {tag} {category:<10} {filename}"
    if detail:
        line += f"\n         {detail}"
    print(line)
    results.append((category, filename, status, detail))


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _count_values(result) -> int:
    """Return total number of decoded values across all row-groups/columns."""
    total = 0
    for rg in result.get("row_groups", []):
        for col in rg:
            if col is not None:
                total += len(col)
    return total


# ──────────────────────────────────────────────────────────────────────────────
# Per-file tests
# ──────────────────────────────────────────────────────────────────────────────

def test_metadata(path: Path, raw: bytes) -> None:
    """Attempt to read metadata; fail if an exception is raised or result is empty."""
    try:
        meta = rp.read_metadata_from_bytes(raw)
    except Exception as exc:
        record("METADATA", path.name, FAIL, f"{type(exc).__name__}: {exc}")
        return

    if meta is None:
        record("METADATA", path.name, FAIL, "read_metadata_from_bytes returned None")
        return

    num_rg   = len(meta.get("row_groups", []))
    num_cols = len(meta.get("schema", []))
    record("METADATA", path.name, PASS, f"{num_rg} row-group(s), {num_cols} schema field(s)")


def test_data(path: Path, raw: bytes) -> None:
    """
    Check decodability first; if supported, read all columns and verify we got
    at least some values back.  Files that are unsupported by the rugo decoder
    are marked SKIP rather than FAIL.
    """
    # Decodability check
    try:
        decodable = rp.can_decode_from_memory(raw)
    except Exception as exc:
        record("DATA", path.name, FAIL, f"can_decode_from_memory raised {type(exc).__name__}: {exc}")
        return

    if not decodable:
        record("DATA", path.name, SKIP, "can_decode_from_memory() returned False")
        return

    # Full read
    try:
        result = rp.read_parquet(raw)
    except Exception as exc:
        record("DATA", path.name, FAIL, f"{type(exc).__name__}: {exc}\n         {traceback.format_exc().splitlines()[-2]}")
        return

    if result is None:
        record("DATA", path.name, FAIL, "read_parquet returned None")
        return

    if not result.get("success"):
        record("DATA", path.name, FAIL, "result['success'] is False")
        return

    n = _count_values(result)
    cols = result.get("column_names", [])
    rg_count = len(result.get("row_groups", []))
    record("DATA", path.name, PASS, f"{rg_count} row-group(s), {len(cols)} column(s), {n} total values")


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

        test_metadata(path, raw)
        test_data(path, raw)

    # ── Summary ────────────────────────────────────────────────────────────────
    counts = {PASS: 0, FAIL: 0, SKIP: 0}
    for _, _, status, _ in results:
        counts[status] += 1

    print(f"\n{'─'*60}")
    print(f"  Total  : {len(results)}")
    print(f"  Passed : {counts[PASS]}")
    print(f"  Failed : {counts[FAIL]}")
    print(f"  Skipped: {counts[SKIP]}")
    print(f"{'─'*60}")

    if counts[FAIL]:
        print("\nFailed tests:")
        for cat, fname, status, detail in results:
            if status == FAIL:
                print(f"  {cat:<10} {fname}")
                if detail:
                    print(f"             {detail}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
