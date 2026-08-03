# Opteryx Core

Opteryx Core is the SQL execution engine behind [opteryx.app](https://opteryx.app). It is a fork of [Opteryx](https://github.com/mabel-dev/opteryx) with a smaller, more opinionated API and configuration surface, shaped around the workloads used by the hosted service.

This library is designed for fast, read-heavy analytical queries over Parquet-backed data. It handles SQL parsing, planning, predicate pushdown, projection pruning, and execution so you can query datasets from Python without standing up a separate warehouse.

This project is opinionated toward the needs of `opteryx.app`. It is still useful as a standalone library if you want to query local Parquet, NDJSON, and CSV datasets, embed SQL into a Python service or notebook, or experiment with engine internals directly.

## Requirements

- Python 3.11 or later
- A C/C++ toolchain for local source builds
- Rust/Cargo for the Rust extension in `src/`

## Install

```bash
pip install opteryx-core
```

Import it as:

```python
import opteryx
```

## Quick Start: Query Local Files

If your current working directory contains local Parquet data, the simplest way to use Opteryx Core is to register a local workspace and query it with dot-separated names.

```python
import opteryx
from opteryx.connectors import DiskConnector

opteryx.register_workspace("data", DiskConnector)

session = opteryx.session()
result = session.execute_to_arrow(
    "SELECT id, name FROM data.planets WHERE id < 5"
)

print(result)
```

In this model, dataset names are resolved relative to the current working directory. For example, `data.planets` resolves to `./data/planets`, and Opteryx Core reads the Parquet files it finds there.

## What It Is For

- Powering the execution layer used by `opteryx.app`
- Running analytical SQL against local Parquet-backed datasets
- Embedding a query engine inside Python applications, scripts, notebooks, and services
- Working on engine internals such as planning, execution, and Parquet performance

## Local Development

The supported local build path is the repository Makefile:

```bash
make dev-install
make compile
make q
```

Useful targets:

| Target | Purpose |
|--------|---------|
| `make compile` | Clean in-place build of Cython, C++, and Rust extensions |
| `make c` | Incremental extension build |
| `make q` | Fast SQL shape smoke test |
| `make test` | Full pytest suite after compiling |
| `make dt` | Draken native unit tests |
| `make check` | Ruff and import-order checks without modifying files |

Do not use `pip install .` as the primary development build path; `make compile` matches the layout expected by this repository.

## Repository Layout

| Path | Purpose |
|------|---------|
| `opteryx/` | Python package, planner, operators, connectors, expression evaluation, and Cython modules |
| `draken/` | Native columnar vector substrate used by the execution engine |
| `rugo/` | Internal Parquet and JSONL reader used by scans and metadata paths |
| `src/` | Rust extension code, currently including the SQL dialect integration |
| `tests/` | Unit, integration, fuzzing, sqllogictest, and benchmark harnesses |
| `testdata/` | Local datasets and benchmark fixtures |
| `dev/` | Development, release, vendoring, and analysis scripts |
| `scratch/` | Experimental prototypes and one-off investigations |
| `third_party/` | Vendored native dependencies |

## Best With Opteryx Catalog

Opteryx Core works best when paired with the `opteryx_catalog` library. That is the intended model for named datasets, catalog-backed tables, and the general experience used in `opteryx.app`.

Typical setup:

```python
import os

import opteryx

from opteryx import set_default_connector
from opteryx.connectors import OpteryxConnector
from opteryx_catalog import OpteryxCatalog

set_default_connector(
    OpteryxConnector,
    catalog=OpteryxCatalog,
    firestore_project=os.environ["GCP_PROJECT_ID"],
    firestore_database=os.environ["FIRESTORE_DATABASE"],
    gcs_bucket=os.environ["GCS_BUCKET"],
)
```

Once configured, you can query catalog-backed datasets using dot-separated names such as `public.space.planets` or `opteryx.ops.billing`.

For local data, Opteryx Core is typically used through registered workspaces such as `testdata`, `scratch`, or `data`. Queries refer to datasets by dot-separated names relative to the workspace root, for example `testdata.planets`, `testdata.satellites`, or `scratch.signals`.

## Where It Fits

Opteryx Core is best thought of as an embedded analytical engine rather than a full end-user platform. If you want a hosted experience, multi-tenant service features, and the broader product workflow, use [opteryx.app](https://opteryx.app). If you want the core engine in your own environment, this package gives you that engine directly. If you want the intended table-resolution model, pair it with `opteryx_catalog`.

## Contributing

If you use Opteryx-Core yourself, we want to hear from you.

- Use it on your own datasets
- Raise bugs when queries, schemas, or performance do not behave as expected
- Open pull requests for fixes, tests, docs, or performance improvements
- Share repro cases, failing queries, and edge-case Parquet files

This project is being actively built, and outside usage helps make it better.

Docs: https://docs.opteryx.app/
Source: https://github.com/mabel-dev/opteryx-core
License: Apache-2.0
