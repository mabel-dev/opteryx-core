# Third-party benchmark material

Opteryx-Core is licensed under the Apache License 2.0 (see [LICENSE](LICENSE)).
**That licence does not cover the third-party benchmark material listed below.**
Each item keeps the terms of its upstream source. The material is used in the
development and testing of the Opteryx engine: performance tests, regression
detection, and correctness checks against known answers. The performance
harnesses also contain runners for other engines, used to check Opteryx's
results and timings against them during development.

Related: [wrenchy-bench](https://github.com/mabel-dev/wrenchy-bench) tracks
Opteryx's performance over time using the same benchmarks, and carries the same
attributions.

| Benchmark | Source | Terms |
|---|---|---|
| ClickBench | [ClickHouse, Inc.](https://github.com/ClickHouse/ClickBench) | [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/) |
| TPC-H | [Transaction Processing Performance Council](https://www.tpc.org/) | TPC fair use policy |
| Join Order Benchmark (JOB) | Leis et al., VLDB 2015; data from [IMDb](https://www.imdb.com) | IMDb non-commercial terms |
| H2O db-benchmark | [H2O.ai](https://github.com/h2oai/db-benchmark) | [MPL-2.0](https://www.mozilla.org/MPL/2.0/) |

## ClickBench

ClickBench is by ClickHouse, Inc. and is licensed under the Creative Commons
Attribution-NonCommercial-ShareAlike 4.0 International licence (CC BY-NC-SA
4.0). Attribution, non-commercial use, and ShareAlike apply to the material
below, and to anything adapted from it.

- `tests/performance/clickbench/` and `dev/clickbench/`: the 43 queries and the
  harnesses that run them. The queries are adapted: table names and some
  expressions are changed for Opteryx's SQL dialect.
- `testdata/clickbench_tiny/hits_48.parquet`: a copy of one of the 100 files
  of the published `hits` dataset
  (`hits_compatible/athena_partitioned/hits_48.parquet`). It is redistributed
  here for testing, as published. The dataset is anonymised web-analytics data;
  see the ClickBench repository for its description.
- `tests/integration/sql_battery/test_data/clickbench/`: expected answers to the
  ClickBench queries over the data above.
- `dev/clickbench/opteryx/` is Opteryx's entry for the ClickBench results
  table. It follows ClickBench's entry format, and entries submitted there are
  subject to ClickBench's own terms.

## TPC-H

"TPC-H" and "TPC Benchmark" are trademarks of the Transaction Processing
Performance Council (TPC). The TPC-H specification and its copyright are the
TPC's.

**Nothing here is a TPC-H result.** The queries and data are derived from TPC-H,
the runs are not audited or reviewed by the TPC, and the numbers are not
comparable to published TPC-H results. The name "TPC-H" is used to identify the
workload the material derives from, in line with the TPC's
[fair use policy](https://www.tpc.org/TPC_Documents_Current_Versions/pdf/Fair_Use_Quick_Reference_v1.0.0.pdf).

- `tests/performance/tpch/`: the 22 queries (parameterised) and the runners.
  Q13 and Q15 deviate from the specification text because of engine limits;
  the reasons are in the header of each query file.
- `testdata/tpch_001/`: TPC-H data at a small scale factor, produced with a
  TPC-H data generator and stored as Parquet.
- `tests/integration/sql_battery/test_data/tpch/` and `dev/tpch/`: expected
  answers and the scripts that generate them.

## Join Order Benchmark (JOB)

> Viktor Leis, Andrey Gubichev, Atanas Mirchev, Peter Boncz, Alfons Kemper,
> Thomas Neumann. "How Good Are Query Optimizers, Really?" *Proceedings of the
> VLDB Endowment* 9(3), 2015.

- `tests/performance/job/queries/`: the 113 queries, as published in
  [gregrahn/join-order-benchmark](https://github.com/gregrahn/join-order-benchmark),
  which states no licence. They are credited to their authors and are not
  covered by this repository's Apache 2.0 licence.
- The benchmark data is derived from IMDb. It is **not** committed to this
  repository; `tests/performance/job/fetch_data.py` downloads it for local use.
  IMDb's [conditions](https://help.imdb.com/article/imdb/general-information/can-i-use-imdb-data-in-my-software/G5JTRESSHJBBHTGX)
  for its datasets apply to it.

> Information courtesy of IMDb (https://www.imdb.com). Used with permission.

## H2O db-benchmark

The groupby (`g1`–`g10`) and join (`j1`–`j5`) queries in
`tests/performance/h2o/queries/` are based on the H2O.ai
[db-benchmark](https://github.com/h2oai/db-benchmark), licensed under the
Mozilla Public License 2.0. Those `.sql` files remain under the MPL-2.0; the
MPL-2.0 is file-level and does not extend to the rest of this repository.
`tests/performance/h2o/generate_data.py` generates synthetic data of the same
shape as the benchmark's data generator; no benchmark data is committed.
