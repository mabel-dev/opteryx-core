# Rugo Examples

Runnable examples covering the core rugo API.  All scripts assume they are run
from the **repo root** (`opteryx-core/`) so the `rugo/` and `testdata/`
directories are reachable.

```sh
python rugo/examples/01_read_parquet.py
```

---

## Index

| File | What it shows |
|------|--------------|
| [01_read_parquet.py](01_read_parquet.py) | Read a Parquet file — schema, column projection, row iteration |
| [02_morsel_api.py](02_morsel_api.py) | Morsel schema accessor, named-tuple row iterator, column access |
| [03_to_arrow.py](03_to_arrow.py) | Export Vectors and Morsels to PyArrow via the C Data Interface |
| [04_write_parquet.py](04_write_parquet.py) | Write a Parquet file from scratch and round-trip it |
| [05_read_csv.py](05_read_csv.py) | Read CSV — projection, predicate pushdown, TSV, null handling |
| [06_read_jsonl.py](06_read_jsonl.py) | Read JSONL — schema inference, projection, predicate pushdown |

---

## Notes

- PyArrow is only needed for the `to_arrow()` example.  All other examples run with
  rugo alone.
- The `testdata/` directory ships with the opteryx-core repo and is used by
  the examples that read real files.  Inline `bytes` data is used elsewhere so
  the examples are self-contained.
