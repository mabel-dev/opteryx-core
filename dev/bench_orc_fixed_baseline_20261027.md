# ORC fixed-width stripe decode baseline — 2026-10-27

Baseline captured **before rebuilding the reader**. Machine: Apple M5 Pro, macOS
26.6.2 arm64, Python 3.14.5, PyArrow 25.0.1. The existing
`rugo/rugo_native.cpython-314-darwin.so` SHA-256 was
`49289dd556df03fd1f8b14f661ceedf04f1dd8e2f9c0307798c63886feb2a83c`;
`draken/draken_native.cpython-314-darwin.so` SHA-256 was
`3d64e1a4e516c4eea9971c492b4314ad8eb5b8e8b3b7fcdfdbfa3edbb041dd1c`.
The reader was not rebuilt for this run; its exact original build options are
not independently established here. The benchmark bridge was rebuilt with
Apple clang 21.0.0 (clang-2100.3.34.2), `-O3 -std=c++20 -fPIC`.

## Reproduction

```sh
make bench-orc-fixed ORC_FIXED_ARGS='--rows 65536 --rounds 9 --iterations 64 --output dev/bench_orc_fixed_baseline_20261027.json'
```

The command writes the full nine per-round samples, actual null counts, file
sizes and SHA-256 fixture fingerprints to
[`bench_orc_fixed_baseline_20261027.json`](bench_orc_fixed_baseline_20261027.json).
Use a different output path for a future run to preserve this baseline. The
make target compiles only the benchmark bridge, dynamically resolves the
existing native reader, and sets `TZ=UTC`. It does not rebuild the reader.
It checks the timestamps of the four ORC source/header files against the
reader binary, but this timestamp check is not proof that every dependency was
built from the present tree. The printed binary hash identifies what was
actually measured.

The Python development harness generates deterministic values and null masks
with `mix()` from fixed seeds, then uses the existing PyArrow development/test
installation as an independent Apache ORC writer. Each case is a single-column,
single-stripe ORC 0.12 file, no compression, 64 MiB stripe setting, and 10,000
row index stride. The harness asserts the actual ORC kind, source/output type,
row count, stripe count and no compression. Integer fixtures span each signed
width; dates span ±36,500 days around the Unix epoch; timestamps are random
microseconds in 2020 UTC; floats are deterministic finite signed values.
Widening cases use byte-identical files to their non-widening counterparts and
request the wider destination through `OrcFile::bind`.

For each target null density of 0%, 1%, 50%, and 90%, the same pseudorandom mask
is used across types. Actual counts for 65,536 rows are respectively 0, 701,
32,869 and 59,106. BYTE uses byte RLE; SHORT/INT/LONG/DATE and timestamp streams
use the writer's ORC 0.12 integer RLE; FLOAT/DOUBLE use fixed-width data. The
benchmark does not independently classify each encoded integer run.

Before timing, the native bridge decodes each case and checks every valid value
and every validity bit against the generator's expected values. FLOAT values
are checked exactly after conversion to binary64, including FLOAT→DOUBLE.
Each case is then warmed up once. In each of nine measured rounds, case order
alternates forward/reverse; a native C++ loop performs 64 decodes. Per-round
nanoseconds are divided by 64 × 65,536 **logical rows** (including nulls), and
the table reports the median. A sampled checksum and compiler barrier consume
outputs. No Python or FFI call occurs inside that native loop.

Timing includes `decode_stripe`'s stripe footer parsing, stream lookup,
presence decoding, value decode, scatter/widening, buffer allocation and
batch destruction. It excludes fixture generation, opening/binding the file,
validation, and file I/O. Inputs are in memory and repeatedly reused. This is
a hot, single-threaded synthetic stripe microbenchmark, not a compressed,
I/O-bound, cold-cache, end-to-end file, scan or SQL benchmark. It does not
cover all-null stripes, sparse clustered null patterns, other encodings,
floating-point special values, or production x86/AVX2. Values below are not
an estimate of workload-weighted impact. For a meaningful comparison, use the
same writer version, input fingerprints, machine and measurement budget;
consider run-to-run and min/max variability in the JSON.

## Baseline results

Median ns per logical row; null columns are target densities. All 64 cases
were validated before timing. The checksum for this run was
`16199414057743213280`.

| ORC source → output | 0% | 1% | 50% | 90% |
|---|---:|---:|---:|---:|
| BYTE → BYTE | 0.742 | 1.348 | 2.786 | 1.288 |
| BYTE → SHORT | 0.634 | 1.238 | 2.705 | 1.246 |
| BYTE → INT | 0.866 | 1.175 | 3.065 | 1.338 |
| BYTE → LONG | 0.877 | 1.385 | 3.035 | 1.323 |
| SHORT → SHORT | 0.858 | 1.463 | 3.162 | 1.272 |
| SHORT → INT | 1.085 | 1.390 | 3.409 | 1.342 |
| SHORT → LONG | 0.883 | 1.489 | 3.508 | 1.294 |
| INT → INT | 1.085 | 1.390 | 3.501 | 1.346 |
| INT → LONG | 0.886 | 1.496 | 3.379 | 1.327 |
| LONG → LONG | 1.378 | 1.991 | 3.671 | 1.337 |
| FLOAT → FLOAT | 0.466 | 1.090 | 3.148 | 1.286 |
| FLOAT → DOUBLE | 0.704 | 1.174 | 3.327 | 1.349 |
| DOUBLE → DOUBLE | 0.483 | 1.000 | 3.164 | 1.332 |
| DATE → DATE | 1.207 | 1.507 | 3.514 | 1.371 |
| TIMESTAMP → TIMESTAMP | 1.952 | 2.566 | 3.812 | 1.210 |
| TIMESTAMP_INSTANT → TIMESTAMP | 1.953 | 2.555 | 3.966 | 1.183 |

## Validation status

A prior `make q` passed, with the known `unpack_bitpacked_groups_scalar`
unused-function compiler warning. That run preceded the expanded benchmark.
The architect requested no test runs for this benchmark-only change and will
run validation after the changes. `make slt` has not been run; project sign-off
therefore remains pending. The 8,192-row one-round smoke benchmark and the full
baseline above both completed with all decoded values and validity bits checked.
