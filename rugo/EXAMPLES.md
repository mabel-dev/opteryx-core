# Rugo — Examples

Runnable examples for all three rugo readers. All examples assume the project root is on `sys.path`.

```python
import os, sys
sys.path.insert(1, os.path.join(sys.path[0], ".."))
```

---

## Parquet

### Read schema from a file

```python
import os, sys
sys.path.insert(1, os.path.join(sys.path[0], ".."))

from rugo.parquet_reader import read_metadata

# Footer parse only — no column data read
meta = read_metadata("testdata/planets/planets.parquet")

print(meta["num_rows"])  # e.g. 9

for col in meta["schema_columns"]:
    print(col["name"], col["physical_type"], col["logical_type"], col["nullable"])
```

---

### Read schema from bytes

```python
import os, sys
sys.path.insert(1, os.path.join(sys.path[0], ".."))

from rugo.parquet_reader import read_metadata_from_bytes

with open("testdata/planets/planets.parquet", "rb") as f:
    meta = read_metadata_from_bytes(f.read())

print(meta["num_rows"])
for col in meta["schema_columns"]:
    print(col["name"], col["physical_type"], col["logical_type"], col["nullable"])
```

---

### Decode columns from a file

```python
import os, sys
sys.path.insert(1, os.path.join(sys.path[0], ".."))

from rugo.parquet_reader import read_parquet

with open("testdata/planets/planets.parquet", "rb") as f:
    data = f.read()

morsels = read_parquet(data, column_names=["id", "name"])
# morsels is list[Morsel] (one per row group), or None on failure

for morsel in morsels:
    for vec in morsel.vectors:
        if vec is None:
            # Partial decode failure — individual column may be None
            continue
        print(vec.type, vec.length)
        print(vec.to_pylist())
```

---

### Check bloom filter

```python
import os, sys
sys.path.insert(1, os.path.join(sys.path[0], ".."))

from rugo.parquet_reader import read_metadata, test_bloom_filter

meta = read_metadata("testdata/planets/planets.parquet")

# bloom_offset and bloom_length come from the per-column stats dict
for rg in meta["row_groups"]:
    for col_stats in rg["columns"]:
        if col_stats["name"] == "name" and col_stats["bloom_length"] > 0:
            present = test_bloom_filter(
                "testdata/planets/planets.parquet",
                col_stats["bloom_offset"],
                col_stats["bloom_length"],
                "Earth",
            )
            # True means the value MAY be present; False means definitely absent
            print("bloom hit:", present)
```

---

### Telemetry

```python
import os, sys
sys.path.insert(1, os.path.join(sys.path[0], ".."))

from rugo.parquet_reader import (
    read_parquet,
    reset_telemetry, get_telemetry,
    reset_cpp_telemetry, get_cpp_telemetry,
)

# Reset both accumulators before the workload
reset_telemetry()
reset_cpp_telemetry()

with open("testdata/planets/planets.parquet", "rb") as f:
    data = f.read()

read_parquet(data, column_names=["id", "name"])

# Cython-side: per-type timing + call/row-group/column counts
print(get_telemetry())

# C++-side: phase breakdown in seconds
print(get_cpp_telemetry())
# Keys: metadata_s, decompress_s, dict_parse_s, prescan_s,
#       page_parallel_s, rle_s, val_expand_s, mask_filter_s,
#       validity_bmp_s, calls
```

---

## JSONL

### Infer schema

```python
import os, sys
sys.path.insert(1, os.path.join(sys.path[0], ".."))

from rugo.jsonl import get_jsonl_schema

# Inline bytes — no file needed
data = (
    b'{"id": 1, "name": "Alice", "score": 9.5, "active": true}\n'
    b'{"id": 2, "name": "Bob",   "score": 8.1, "active": false}\n'
    b'{"id": 3, "name": "Carol", "score": 7.7, "active": true}\n'
)

schema = get_jsonl_schema(data, sample_size=5)
# -> {"columns": [{"name": str, "type": str, "nullable": bool}, ...]}

for col in schema["columns"]:
    print(col["name"], col["type"], col["nullable"])
```

---

### Read all columns

```python
import os, sys
sys.path.insert(1, os.path.join(sys.path[0], ".."))

from rugo.jsonl import read_jsonl

data = (
    b'{"id": 1, "name": "Alice", "score": 9.5}\n'
    b'{"id": 2, "name": "Bob",   "score": 8.1}\n'
    b'{"id": 3, "name": "Carol", "score": 7.7}\n'
)

result = read_jsonl(data)

if result["success"]:
    print(result["num_rows"])       # 3
    print(result["column_names"])   # ["id", "name", "score"]
    for vec in result["columns"]:
        print(vec.to_pylist())
```

---

### Column projection

```python
import os, sys
sys.path.insert(1, os.path.join(sys.path[0], ".."))

from rugo.jsonl import read_jsonl

data = (
    b'{"id": 1, "name": "Alice", "score": 9.5}\n'
    b'{"id": 2, "name": "Bob",   "score": 8.1}\n'
    b'{"id": 3, "name": "Carol", "score": 7.7}\n'
)

result = read_jsonl(data, columns=["name", "score"])

if result["success"]:
    print(result["column_names"])  # ["name", "score"]
    for vec in result["columns"]:
        print(vec.to_pylist())
```

---

### Predicate pushdown

```python
import os, sys
sys.path.insert(1, os.path.join(sys.path[0], ".."))

from rugo.jsonl import read_jsonl

data = b"".join(
    f'{{"id": {i}, "val": {i * 10}}}\n'.encode() for i in range(1, 2001)
)

result = read_jsonl(data, predicates=[("id", "<", 1000)])

if result["success"]:
    # num_rows reflects survivors only — rows where id < 1000
    print(result["num_rows"])  # 999
```

---

### Projection + predicate combined

```python
import os, sys
sys.path.insert(1, os.path.join(sys.path[0], ".."))

from rugo.jsonl import read_jsonl

data = (
    b'{"id": 1, "name": "Alice", "score": 9.5}\n'
    b'{"id": 2, "name": "Bob",   "score": 8.1}\n'
    b'{"id": 3, "name": "Carol", "score": 7.7}\n'
    b'{"id": 4, "name": "Dave",  "score": 6.2}\n'
)

# Predicate column (score) need not appear in the projection list
result = read_jsonl(data, columns=["name"], predicates=[("score", ">", 8.0)])

if result["success"]:
    print(result["column_names"])  # ["name"]
    print(result["columns"][0].to_pylist())  # ["Alice", "Bob"]
```

---

### Explicit schema

```python
import os, sys
sys.path.insert(1, os.path.join(sys.path[0], ".."))

from rugo.jsonl import read_jsonl

data = (
    b'{"id": 1, "name": "Alice"}\n'
    b'{"id": 2, "name": "Bob"}\n'
)

# Pass explicit_schema to skip inference — use when schema is known
result = read_jsonl(
    data,
    explicit_schema={"id": "int64", "name": "string"},
)

if result["success"]:
    print(result["schema"])  # {"id": "int64", "name": "string"}
```

---

### Read from a file path

```python
import os, sys
sys.path.insert(1, os.path.join(sys.path[0], ".."))

from rugo.jsonl import read_jsonl

# Pass a str path directly instead of a bytes buffer
result = read_jsonl(
    "testdata/example.jsonl",
    columns=["id", "name"],
    predicates=[("id", "<", 500)],
)

if result["success"]:
    print(result["num_rows"])
    for vec in result["columns"]:
        print(vec.to_pylist())
```

---

## CSV

### Basic read

```python
import os, sys
sys.path.insert(1, os.path.join(sys.path[0], ".."))

from rugo.csv import read_csv

data = (
    b"id,name,score\n"
    b"1,Alice,95\n"
    b"2,Bob,82\n"
    b"3,Carol,77\n"
)

result = read_csv(data)

if result["success"]:
    print(result["column_names"])  # ["id", "name", "score"]
    print(result["num_rows"])      # 3
    for vec in result["columns"]:
        print(vec.to_pylist())
```

---

### Column projection

```python
import os, sys
sys.path.insert(1, os.path.join(sys.path[0], ".."))

from rugo.csv import read_csv

data = (
    b"id,name,score\n"
    b"1,Alice,95\n"
    b"2,Bob,82\n"
    b"3,Carol,77\n"
)

result = read_csv(data, columns=["id", "score"])

if result["success"]:
    print(result["column_names"])  # ["id", "score"]
    for vec in result["columns"]:
        print(vec.to_pylist())
```

---

### Predicate pushdown

```python
import os, sys
sys.path.insert(1, os.path.join(sys.path[0], ".."))

from rugo.csv import read_csv

data = (
    b"id,name,score\n"
    b"1,Alice,95\n"
    b"2,Bob,82\n"
    b"3,Carol,61\n"
    b"4,Dave,45\n"
)

result = read_csv(data, predicates=[("score", ">", 60)])

if result["success"]:
    # num_rows reflects survivors only — rows where score > 60
    print(result["num_rows"])  # 3
    for vec in result["columns"]:
        print(vec.to_pylist())
```

---

### Projection + predicate combined

```python
import os, sys
sys.path.insert(1, os.path.join(sys.path[0], ".."))

from rugo.csv import read_csv

data = (
    b"id,name,score\n"
    b"1,Alice,95\n"
    b"2,Bob,82\n"
    b"3,Carol,61\n"
    b"4,Dave,45\n"
)

# Predicate column (score) need not appear in the projection list
result = read_csv(data, columns=["name"], predicates=[("score", ">", 60)])

if result["success"]:
    print(result["column_names"])  # ["name"]
    print(result["columns"][0].to_pylist())  # ["Alice", "Bob", "Carol"]
```

---

### No header (headerless CSV)

```python
import os, sys
sys.path.insert(1, os.path.join(sys.path[0], ".."))

from rugo.csv import read_csv

data = (
    b"1,Alice,95\n"
    b"2,Bob,82\n"
    b"3,Carol,77\n"
)

# Columns are auto-named col_0, col_1, col_2, ...
result = read_csv(data, has_header=False)

if result["success"]:
    print(result["column_names"])  # ["col_0", "col_1", "col_2"]
    for vec in result["columns"]:
        print(vec.to_pylist())
```

---

### TSV

```python
import os, sys
sys.path.insert(1, os.path.join(sys.path[0], ".."))

from rugo.csv import read_csv

data = (
    b"id\tname\tscore\n"
    b"1\tAlice\t95\n"
    b"2\tBob\t82\n"
    b"3\tCarol\t77\n"
)

result = read_csv(data, delimiter="\t")

if result["success"]:
    print(result["column_names"])  # ["id", "name", "score"]
    for vec in result["columns"]:
        print(vec.to_pylist())
```

---

### Null handling

```python
import os, sys
sys.path.insert(1, os.path.join(sys.path[0], ".."))

from rugo.csv import read_csv

# Empty unquoted fields become null in the output vector (to_pylist returns None)
data = (
    b"id,name,score\n"
    b"1,Alice,95\n"
    b"2,,82\n"
    b"3,Carol,\n"
)

result = read_csv(data)

if result["success"]:
    for name, vec in zip(result["column_names"], result["columns"]):
        print(name, vec.to_pylist())
# id    [1, 2, 3]
# name  ["Alice", None, "Carol"]
# score [95, 82, None]
```
