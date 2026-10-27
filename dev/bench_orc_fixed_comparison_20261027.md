# ORC fixed-width decode: post-change comparison — 2026-10-27

## Commands

```sh
make bench-orc-fixed ORC_FIXED_ARGS='--rows 65536 --rounds 9 --iterations 64 --output dev/bench_orc_fixed_baseline_20261027.json'
make bench-orc-fixed ORC_FIXED_ARGS='--rows 65536 --rounds 9 --iterations 64 --output dev/bench_orc_fixed_post_20261027.json'
python3.14 dev/compare_orc_fixed.py dev/bench_orc_fixed_baseline_20261027.json dev/bench_orc_fixed_post_20261027.json dev/bench_orc_fixed_comparison_20261027.md
```

Baseline samples: `dev/bench_orc_fixed_baseline_20261027.json`. Post-change samples: `dev/bench_orc_fixed_post_20261027.json`.
The first command was run before the reader change; the second was run after rebuilding.

## Matching and methodology

All 64 case keys and their order, target densities, actual null counts, file sizes and
SHA-256 fixture hashes matched. Row count, iteration/round budget, PyArrow version,
Python version, platform and final output checksum matched. Both benchmark runs checked
every validity bit and valid value, plus source/output types, ORC kind, row count,
single-stripe layout and lack of compression, before timing. The JSON does not retain
those individual metadata assertions; they were checked by the native bridge each run.

Reader SHA-256: baseline `49289dd556df03fd1f8b14f661ceedf04f1dd8e2f9c0307798c63886feb2a83c`; post-change `fae7cb1538d8776d89abded8ff6841965bf5c85a96ae0893d02aa90c30637048`.
Draken binary SHA-256 was `3d64e1a4e516c4eea9971c492b4314ad8eb5b8e8b3b7fcdfdbfa3edbb041dd1c`
in both runs. Apple M5 Pro, macOS 26.6.2 arm64; PyArrow 25.0.1; bridge compiled by
Apple clang 21.0.0, `-O3 -std=c++20 -fPIC`. Reader binary build options were not
independently established. See `dev/bench_orc_fixed_baseline_20261027.md` for fixture
generation, measurement methodology and limitations.

Before the post-change run, a process-list check found no matching test processes. A
separate monitor sampled `ps -axo pid,ppid,etime,command` every 0.5 seconds while the
benchmark process existed and reported no matching pytest, sqllogictest, make-test,
test-script or native `*_test` processes. This is sampling, not process isolation:
short-lived or differently named tests could have been missed; the monitor itself can
add system noise. The prior baseline did not use this monitor.

The runs were sequential, not interleaved A/B. Ranges are minima/maxima of nine rounds,
not confidence intervals; overlapping ranges signal ambiguity, and separate ranges do
not prove causality. Background activity, frequency and run-to-run effects can affect
results. Synthetic hot in-memory decoding is not end-to-end or representative of x86.

## Summary

Using an arbitrary 5% median-change reporting threshold, 55 cases improved
and 0 regressed; the other 9 were within ±5%.
Positive delta means the post-change decode is slower; negative means faster.
Regressions over 5%:

## All cases

Times are ns per logical row; each range is min–max across nine rounds.

| Case | Target nulls | Baseline median | Baseline range | Post median | Post range | Delta | Ranges |
|---|---:|---:|---:|---:|---:|---:|---|
| BYTE | 0% | 0.742 | 0.685–0.757 | 0.036 | 0.035–0.038 | -95.1% | separate |
| BYTE | 1% | 1.348 | 1.239–1.366 | 0.917 | 0.893–0.938 | -32.0% | separate |
| BYTE | 50% | 2.786 | 2.604–3.162 | 1.204 | 1.164–1.379 | -56.8% | separate |
| BYTE | 90% | 1.288 | 1.271–1.392 | 0.661 | 0.651–0.696 | -48.7% | separate |
| BYTE->SHORT | 0% | 0.634 | 0.625–0.639 | 0.050 | 0.047–0.051 | -92.1% | separate |
| BYTE->SHORT | 1% | 1.238 | 1.223–1.258 | 0.925 | 0.907–0.960 | -25.3% | separate |
| BYTE->SHORT | 50% | 2.705 | 2.627–2.875 | 1.245 | 1.178–1.338 | -54.0% | separate |
| BYTE->SHORT | 90% | 1.246 | 1.231–1.276 | 0.654 | 0.624–0.669 | -47.5% | separate |
| BYTE->INT | 0% | 0.866 | 0.854–0.878 | 0.074 | 0.069–0.078 | -91.4% | separate |
| BYTE->INT | 1% | 1.175 | 1.160–1.199 | 1.051 | 1.023–1.069 | -10.6% | separate |
| BYTE->INT | 50% | 3.065 | 2.809–3.405 | 1.308 | 1.260–1.400 | -57.3% | separate |
| BYTE->INT | 90% | 1.338 | 1.320–1.345 | 0.683 | 0.657–0.699 | -49.0% | separate |
| BYTE->LONG | 0% | 0.877 | 0.868–0.895 | 0.118 | 0.113–0.120 | -86.6% | separate |
| BYTE->LONG | 1% | 1.385 | 1.377–1.399 | 0.952 | 0.934–0.968 | -31.3% | separate |
| BYTE->LONG | 50% | 3.035 | 2.827–3.281 | 1.245 | 1.198–1.311 | -59.0% | separate |
| BYTE->LONG | 90% | 1.323 | 1.311–1.370 | 0.679 | 0.654–0.701 | -48.7% | separate |
| SHORT | 0% | 0.858 | 0.850–0.872 | 0.298 | 0.287–0.304 | -65.3% | separate |
| SHORT | 1% | 1.463 | 1.448–1.478 | 1.094 | 1.062–1.118 | -25.2% | separate |
| SHORT | 50% | 3.162 | 2.999–3.503 | 2.469 | 2.045–2.943 | -21.9% | separate |
| SHORT | 90% | 1.272 | 1.219–1.460 | 1.021 | 0.984–1.049 | -19.7% | separate |
| SHORT->INT | 0% | 1.085 | 1.080–1.100 | 0.305 | 0.302–0.319 | -71.9% | separate |
| SHORT->INT | 1% | 1.390 | 1.371–1.413 | 1.086 | 1.073–1.118 | -21.9% | separate |
| SHORT->INT | 50% | 3.409 | 3.181–3.585 | 2.934 | 2.669–3.622 | -14.0% | overlap |
| SHORT->INT | 90% | 1.342 | 1.314–1.392 | 0.968 | 0.940–1.260 | -27.9% | separate |
| SHORT->LONG | 0% | 0.883 | 0.878–0.909 | 0.360 | 0.343–0.366 | -59.2% | separate |
| SHORT->LONG | 1% | 1.489 | 1.446–1.496 | 1.118 | 1.083–1.138 | -24.9% | separate |
| SHORT->LONG | 50% | 3.508 | 3.187–3.599 | 2.906 | 2.634–3.412 | -17.2% | overlap |
| SHORT->LONG | 90% | 1.294 | 1.255–1.498 | 1.042 | 0.950–1.094 | -19.5% | separate |
| INT | 0% | 1.085 | 0.981–1.104 | 0.312 | 0.303–0.321 | -71.3% | separate |
| INT | 1% | 1.390 | 1.374–1.415 | 1.103 | 1.089–1.131 | -20.6% | separate |
| INT | 50% | 3.501 | 3.281–3.666 | 2.839 | 2.507–3.370 | -18.9% | overlap |
| INT | 90% | 1.346 | 1.322–1.366 | 0.982 | 0.920–1.058 | -27.0% | separate |
| INT->LONG | 0% | 0.886 | 0.872–0.898 | 0.357 | 0.342–0.372 | -59.7% | separate |
| INT->LONG | 1% | 1.496 | 1.449–1.510 | 1.117 | 1.095–1.141 | -25.4% | separate |
| INT->LONG | 50% | 3.379 | 3.112–3.644 | 3.072 | 2.587–3.234 | -9.1% | overlap |
| INT->LONG | 90% | 1.327 | 1.251–1.509 | 1.022 | 0.993–1.214 | -23.0% | separate |
| LONG | 0% | 1.378 | 1.363–1.402 | 0.869 | 0.859–0.891 | -36.9% | separate |
| LONG | 1% | 1.991 | 1.965–2.001 | 1.643 | 1.619–1.652 | -17.5% | separate |
| LONG | 50% | 3.671 | 3.402–3.838 | 3.169 | 2.847–3.926 | -13.7% | overlap |
| LONG | 90% | 1.337 | 1.317–1.607 | 1.078 | 1.052–1.130 | -19.4% | separate |
| FLOAT | 0% | 0.466 | 0.461–0.478 | 0.047 | 0.046–0.049 | -90.0% | separate |
| FLOAT | 1% | 1.090 | 0.991–1.097 | 1.068 | 1.025–1.159 | -2.0% | overlap |
| FLOAT | 50% | 3.148 | 3.039–3.475 | 3.094 | 2.821–3.424 | -1.7% | overlap |
| FLOAT | 90% | 1.286 | 1.217–1.476 | 1.319 | 1.299–1.344 | +2.6% | overlap |
| FLOAT->DOUBLE | 0% | 0.704 | 0.481–0.715 | 0.688 | 0.459–0.698 | -2.3% | overlap |
| FLOAT->DOUBLE | 1% | 1.174 | 0.992–1.238 | 1.140 | 1.041–1.245 | -3.0% | overlap |
| FLOAT->DOUBLE | 50% | 3.327 | 3.237–3.445 | 3.391 | 2.910–3.554 | +1.9% | overlap |
| FLOAT->DOUBLE | 90% | 1.349 | 1.317–1.466 | 1.311 | 1.274–1.440 | -2.8% | overlap |
| DOUBLE | 0% | 0.483 | 0.470–0.489 | 0.092 | 0.086–0.098 | -80.9% | separate |
| DOUBLE | 1% | 1.000 | 0.904–1.012 | 1.029 | 0.913–1.052 | +2.9% | overlap |
| DOUBLE | 50% | 3.164 | 2.876–3.345 | 2.918 | 2.839–3.323 | -7.8% | overlap |
| DOUBLE | 90% | 1.332 | 1.285–1.492 | 1.267 | 1.236–1.481 | -4.9% | overlap |
| DATE | 0% | 1.207 | 1.196–1.221 | 0.435 | 0.428–0.443 | -64.0% | separate |
| DATE | 1% | 1.507 | 1.497–1.524 | 1.228 | 1.205–1.242 | -18.5% | separate |
| DATE | 50% | 3.514 | 3.389–3.758 | 2.923 | 2.669–3.402 | -16.8% | overlap |
| DATE | 90% | 1.371 | 1.341–1.393 | 0.996 | 0.952–1.103 | -27.4% | separate |
| TIMESTAMP | 0% | 1.952 | 1.933–1.967 | 1.416 | 1.405–1.425 | -27.5% | separate |
| TIMESTAMP | 1% | 2.566 | 2.538–2.585 | 2.065 | 2.051–2.073 | -19.5% | separate |
| TIMESTAMP | 50% | 3.812 | 3.703–4.038 | 2.978 | 2.714–3.446 | -21.9% | separate |
| TIMESTAMP | 90% | 1.210 | 1.147–1.417 | 1.049 | 1.023–1.110 | -13.3% | separate |
| TIMESTAMP_INSTANT | 0% | 1.953 | 1.932–1.969 | 1.409 | 1.403–1.432 | -27.9% | separate |
| TIMESTAMP_INSTANT | 1% | 2.555 | 2.539–2.565 | 2.056 | 2.044–2.065 | -19.6% | separate |
| TIMESTAMP_INSTANT | 50% | 3.966 | 3.727–4.190 | 2.910 | 2.466–3.593 | -26.6% | separate |
| TIMESTAMP_INSTANT | 90% | 1.183 | 1.137–1.218 | 1.012 | 0.970–1.242 | -14.4% | overlap |
