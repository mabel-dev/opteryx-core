# Opteryx Sqllogictest Driver

Run sqllogictest `.slt` files against Opteryx Core via the `external` engine bridge.

## How it works

`opteryx_driver.py` is a stdin/stdout subprocess that speaks the JSON protocol
used by sqllogictest's external engine:

- in : `{"sql": "..."}`
- out: `{"result": [["c1","c2"], ...]}` or `{"err": "..."}`

It keeps a single `opteryx.session()` alive for the lifetime of the process so
session-local state (variables bound with `let`, etc.) survives across queries
within a `.slt` file.

## Running

Set `PYTHONPATH` so `import opteryx` works, and `OPTERYX_HOME` so relative connector paths like `testdata/satellites` resolve. Then point sqllogictest at one or more `.slt` files:

```sh
export OPTERYX_HOME=/path/to/opteryx-core
export PYTHONPATH=$OPTERYX_HOME

cargo run --release -p sqllogictest-bin -- \
  --engine external \
  --external-engine-command-template "python3 tests/tools/sqllogictest/opteryx_driver.py" \
  'tests/tools/sqllogictest/tests/results/*.slt'
```

## Converting Opteryx's `.results_tests` fixtures

Opteryx ships hand-curated SQL/expected-result pairs under
`tests/integration/sql_battery/test_data/tests/results/`. The conversion
script reads them, runs each query through Opteryx as the oracle (to
disambiguate column ordering, which the JSON does not encode), and emits
matching `.slt` files:

```sh
PYTHONPATH=$OPTERYX_HOME OPTERYX_HOME=$OPTERYX_HOME \
  python3 tests/tools/sqllogictest/convert_results_tests.py \
    --src "$OPTERYX_HOME/tests/integration/sql_battery/test_data/tests/results" \
    --dest tests/tools/sqllogictest/tests/results
```

Tests that error in Opteryx, fail to parse, or whose values disagree with
the recorded JSON are skipped with a reason — this is intentional, the
script never fabricates expected output.

## Converting Opteryx's `.run_tests` files

The run-only battery (one SQL statement per line, success-only checking)
converts to `statement ok` / `statement error` records. The converter
validates each statement against current Opteryx so the resulting `.slt`
file is a *snapshot* of present behavior — passing queries land as
`statement ok`, currently-failing queries land as
`statement error <ExceptionClass>`. Both directions of regression
(previously-passing query starts failing, or known-broken query starts
passing) become test failures.

Crash recovery is built in: if Opteryx segfaults on a particular statement,
the worker is respawned past the offending line and that statement is
emitted as a comment.

```sh
PYTHONPATH=$OPTERYX_HOME OPTERYX_HOME=$OPTERYX_HOME \
  python3 tests/tools/sqllogictest/convert_run_tests.py \
    --src "$OPTERYX_HOME/tests/integration/sql_battery/test_data/tests" \
    --dest tests/tools/sqllogictest/tests/run_only \
    --exclude clickbench --exclude tpch_data
```

The `clickbench` and `tpch_data` files are external benchmarks and stay in
opteryx-core (driven by `make tpch` and the dedicated benchmark harnesses).

## Shape-only assertions

For queries where the *dimensions* matter but the values are too large or
volatile to pin literally, use `query shape <rows> [<cols>]` (added to
sqllogictest by this project). [tests/shape.slt](tests/shape.slt)
exercises it against `$planets`.

```text
# Asserts both dimensions.
query shape 9 20
SELECT * FROM $planets

# Skip the row count, just check the column count.
query shape - 20
SELECT * FROM $planets
```

All of the Opteryx `test_shapes_*` batteries (other than `test_shapes_basic.py`,
which `make q` runs in opteryx-core) have been migrated to `tests/shapes/*.slt`
using this directive. `test_casts_battery.py` is migrated alongside them.

## Converting `test_shapes_*.py` / `test_casts_battery.py`

Each module exposes a `STATEMENTS` list of `(sql, rows, cols, exception_or_None)`
tuples. The converter is a pure text transform (no live execution), so it
runs in seconds:

```sh
PYTHONPATH=$OPTERYX_HOME \
  python3 tests/tools/sqllogictest/convert_shape_tests.py \
    --src-dir "$OPTERYX_HOME/tests/integration/sql_battery" \
    --dest tests/tools/sqllogictest/tests/shapes
```

Validation happens at slt-run time. When current Opteryx behavior diverges
from a recorded expectation, run sqllogictest with `--override` against the
file to snapshot the new behavior:

```sh
./target/release/sqllogictest --engine external \
  --external-engine-command-template "python3 tests/tools/sqllogictest/opteryx_driver.py" \
  --override tests/tools/sqllogictest/tests/shapes/<file>.slt
```

Volatile bits in error messages (UUIDs in `(QID:...)`, embedded timestamps,
list-literal orderings) need to be stripped to a stable substring after
`--override`, since they change every run. The current shape suite has these
already normalized in place.

## Running the suite

Use the repository-specific workflow or invoke the sqllogictest binary directly. If you use Makefile targets, confirm them with `make help`; this repository's target names have changed over time.

```sh
make help
```

Override `OPTERYX_HOME=/path/to/opteryx-core` if the checkout isn't a
sibling directory, and `JOBS=N` for parallel runs.

## Cell formatting

`_format_cell` in the driver applies sqllogictest conventions:

- `None` -> `NULL`
- empty string -> `(empty)`
- `bool` -> `1` / `0`
- `float` / `Decimal` -> `%.3f`
- `bytes` -> UTF-8 decoded (Opteryx string columns come back as bytes)
- lists/tuples -> bracketed comma-separated

Adjust to taste if your `.slt` corpus expects different conventions
(e.g. integer-only floats or different precision).

## Known caveats

- The external-engine bridge does not currently propagate column types back to
  sqllogictest (`external.rs` returns an empty types vector), so the
  `query III` type-letter header is parsed but not strictly enforced.
- Opteryx is primarily an analytical engine over external connectors; many
  `.slt` corpora that rely on `CREATE TABLE` / `INSERT` will need fixtures
  set up via Opteryx's memory connectors instead.
