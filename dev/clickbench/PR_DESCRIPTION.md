### What changes

- **Pinned release.** `install` for both `opteryx/` and `opteryx-skene/` now
  installs `opteryx-core==0.9.155`, the latest release when this PR was drafted,
  instead of whatever is latest at run time. It also fails
  if the imported version is anything else, so a published result names the
  build that produced it.
- **Process model (`opteryx/` only).** `opteryx/` now runs the engine as a
  long-lived service, the same shape as the pandas/polars entries. Before this,
  each query started a fresh Python process:
  - `start` launches `server.py`, a standard-library HTTP wrapper on
    127.0.0.1:8421. It imports `opteryx` and waits. There is no warm-up query
    and it does not touch the dataset.
  - `check` reads `/health`, which returns the version and runs no query. It
    fails once the server is down, as the restart loop requires.
  - `query` POSTs the statement to the server. The timing on stderr is measured
    by the server over the drain of `execute_to_morsels`, the same span the
    old per-process entry timed. TSV rendering happens after the clock stops.
  - `BENCH_RESTARTABLE=yes`: before every query the driver stops the server,
    drops caches and restarts it, so try 1 is a cold process. `BENCH_DURABLE=yes`:
    the data is Parquet on disk, and nothing is loaded into process memory.
  - There is no result cache. Tries 2-3 get imported modules and the engine's
    Parquet footer and schema caches.
  - The concurrent-QPS test stays off, because the server handles one request
    at a time.
- `opteryx-skene/` keeps its current process-per-query model. Only its pin
  changes.

### Validation

All 43 queries were run through `start` / `check` / `query` / `stop` against
the `opteryx-core==0.9.155` wheel on the partitioned `hits` files: 43/43
completed, no failures. `check` fails after `stop` as expected.

### Results

This PR includes one ClickBench-compliant run of each entry on `c6a.4xlarge`
(2026-10-04, `opteryx-core==0.9.155`): `opteryx/results/20261004/c6a.4xlarge.json`
and `opteryx-skene/results/20261004/c6a.4xlarge.json`. Each run used a fresh
instance with this repo's `cloud-init.sh.in` steps. Both completed 43/43 with
no nulls.

We have not run an exhaustive sweep across the other machines. Previous PRs
triggered the benchmark fleet, which re-ran them and overwrote the measurements
we had taken.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
