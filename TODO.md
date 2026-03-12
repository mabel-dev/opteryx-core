✅ **FIXED**: SQL-92 function naming compliance - removed non-standard functions
- Arithmetic: CEILING, FLOOR, ROUND, TRUNCATE, POWER (SQL-92 standard only)
- Temporal: CURRENT_DATE(), CURRENT_TIME(), CURRENT_TIMESTAMP() (SQL-92 standard only)
- Removed non-standard extensions: NOW, TODAY, YESTERDAY, POW
- All 33 CEILING/FLOOR/ROUND tests pass ✓
- CURRENT_TIMESTAMP() and CURRENT_DATE() tests pass ✓

SQL-92 Compliance Achieved:
- Only SQL-92 standard named functions registered
- No non-standard aliases pretending to be standards
- Clean, standards-focused function catalog

-----

✅ **FIXED**: Production error with undefined symbol `cpu_supports_avx2`
- Root cause: `carchar_group_state_engine` extension included carchar_simd.hpp (which references cpu_supports_avx2) but didn't link cpu_features.cpp
- Fix: Added `src/cpp/cpu_features.cpp` to the extension sources in setup.py
- carchar_native and simd_probe extensions already had the correct configuration

-----

**INVESTIGATING**: Regression errors and segfaults in test suite
- Observable failures when running 'make t' (test suite):
  - Test 0049 in test_shapes_basic.py segfaults (GROUP BY operation)
  - Many testdata.* queries fail with DatasetNotFoundError
  - GENERATE_SERIES tests fail
  - Array operaton tests fail
- **Quick Win - FIXED**: testdata path resolution issue
  - Root cause: Tests were run from test directory, not repo root
  - Fix: Added `os.chdir()` to repo root in __main__ blocks of test files
  - Fixed files: test_shapes_basic.py, test_shapes_data_sources.py
  - Result: testdata.* queries now work, noise greatly reduced
- **Segfault Root Cause Identified**: Multiple aggregates in GROUP BY
  - Trigger: `SELECT planetId, COUNT(*), MAX(id) FROM testdata.satellites GROUP BY planetId`
  - Single aggregates work fine
  - Virtual data ($planets) with multi-aggregates works fine
  - Only fails with multiple aggregates on parquet data
  - Root cause: In `carchar_group_state_engine.pyx` at line 4894, `_build_chunk_morsel_multi()`
  - Issue: Logic for handling multiple aggregates with object state (MIN/MAX on strings) appears faulty
  - Likely cause: Improper vector initialization or bounds checking in multi-aggregate output building
  - Next: Need to debug the finalize path in carchar_group_state_engine for multi-agg case

---- 

'make clickbench' gets killed on Q14

----

This query:
~~~sql
SELECT billing_account, CEILING(CEILING((SUM((event ->> 'bytes_processed')::INTEGER) / 1_000_000_000)) * 0.001, 2) AS processing_cost_gbp, SUM((event ->> 'bytes_processed')::INTEGER) / 1_000_000_000 AS gigabyte_processed, DATE_TRUNC('DAY', TIMESTAMP) AS billing_date FROM opteryx.ops.billing WHERE billing_event = 'DATA_PROCESSED_BYTES' GROUP BY ALL
~~~

throws this error:
> KeyError: "Column '(event ->> 'bytes_processed')::INTEGER' not found"

----

Introduce a vector index

----

README

~~~
TOON           ████████████████████   27.7 acc%/1K tok  │  76.4% acc  │  2,759 tokens
JSON compact   █████████████████░░░   23.7 acc%/1K tok  │  73.7% acc  │  3,104 tokens
YAML           ██████████████░░░░░░   19.9 acc%/1K tok  │  74.5% acc  │  3,749 tokens
JSON           ████████████░░░░░░░░   16.4 acc%/1K tok  │  75.0% acc  │  4,587 tokens
XML            ██████████░░░░░░░░░░   13.8 acc%/1K tok  │  72.1% acc  │  5,221 tokens
~~~

> [!TIP]
> TOON achieves **76.4%** accuracy (vs JSON's 75.0%) while using **39.9% fewer tokens**.
