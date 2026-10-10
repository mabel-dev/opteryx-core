<!-- TITLE -->
Opteryx: pin opteryx-core 0.9.164, add Opteryx (Parquet, rewritten), retire opteryx-skene

<!-- BODY -->
### Why

- **The current entries can't be installed.** `opteryx/install` and
  `opteryx-skene/install` pin `opteryx-core==0.9.155`, which is no longer on
  PyPI.
- **0.9.155 failed on large machines.** In the fleet results from #2387
  (`opteryx/results/20261004`), `c6a.metal`, `c7a.metal-48xl` and
  `c8g.metal-48xl` show null for 41 of 43 queries. Only Q1 and Q7 completed,
  because they are answered from Parquet metadata. Opteryx sized its local read
  workers from the core count. From 66 vCPUs up, that number reached the remote
  fetch-ahead depth (64), and a startup check then rejected every local Parquet
  scan, even though fetch-ahead only applies to remote reads. The check now
  validates against the remote worker count only. 0.9.164 includes the fix.

### What changes

- **`opteryx/` (Opteryx (Parquet, partitioned))**: the pin moves to
  `opteryx-core==0.9.164`. Nothing else changes: same server process model,
  same queries, same `load`.
- **`opteryx-parquet-rewritten/` (Opteryx (Parquet, rewritten)), new**:
  - Same engine, same server process model and same `queries.sql` as `opteryx/`.
  - `load` rewrites each `hits_N.parquet` with Opteryx's own Parquet writer, one
    output file per input file. It uses the writer's defaults: zstd, 65,536-row
    row groups, row groups laid out column-major in blocks of 4. Row counts are
    checked against the source.
  - The rewrite is the load step, so it is reported as `Load time`.
  - The source files are deleted afterwards, so `data-size` measures only the
    rewritten dataset.
  - Only the layout and encoding change. The content is the same and nothing is
    precomputed.
  - `tuned: no`: writer defaults, nothing tuned for this benchmark.
- **`opteryx-skene/` is removed.** That entry measured Opteryx on its internal
  spill format. The rewritten-Parquet entry replaces it as the "Opteryx on its
  own layout" measurement.

**A question on classification.** We weren't sure how to label "Parquet, but
rewritten on load". The data is still Parquet, read by the same Parquet reader
as the partitioned entry, and the full conversion cost is in `Load time`. So we
named the directory `opteryx-parquet-rewritten`, which puts it under Parquet in
the storage filter. If you'd rather it were classified differently, tell us and
we'll rename it.

### Validation

- **Large-machine fix:** all 43 queries were run against the 0.9.164 release
  wheel with the local read width forced to 190 workers (the setting on a
  192-vCPU host) and fetch-ahead at its default of 64. Result: 43/43 completed.
- **`convert.py`:** run against the 0.9.164 wheel on a 1M-row ClickBench
  partition. The output row count matches the source.

### Results

This PR includes one ClickBench-compliant `c6a.4xlarge` run of each entry
(2026-10-09, `opteryx-core==0.9.164`), each on a fresh instance with a 500 GB
gp2 volume and this repo's `cloud-init.sh.in` steps:

- `opteryx/results/20261009/c6a.4xlarge.json`: 43/43, no nulls.
- `opteryx-parquet-rewritten/results/20261009/c6a.4xlarge.json`: 43/43, no
  nulls. The rewrite (`Load time`) took 299 s on 12 worker processes. The
  rewritten dataset is 9.50 GB, against 14.74 GB for the provided files.

We have not swept the other machine types. The benchmark fleet re-runs PRs, and
for the metal machines we'd like those runs to replace the null results above.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

<!-- COMMIT MESSAGE -->
Opteryx: pin 0.9.164, add opteryx-parquet-rewritten, retire opteryx-skene

- opteryx/: install pins opteryx-core==0.9.164. 0.9.155 is no longer on PyPI,
  and it refused local Parquet scans on >=66 vCPU hosts (41/43 nulls on the
  192-vCPU metal machines).
- opteryx-parquet-rewritten/: new entry. load rewrites the partitioned hits
  with Opteryx's own Parquet writer (zstd, 64k-row groups, blocks of 4) and
  reports that as load time. Same server and queries as opteryx/.
- opteryx-skene/: removed.
- Results: one compliant c6a.4xlarge run of each entry (2026-10-09).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
