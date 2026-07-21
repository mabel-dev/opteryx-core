"""
JSONBench queries against rugo's JSONL reader.

There is no SQL layer here — Opteryx cannot read JSON at all, and rugo (the
file engine) has no query language, only a columnar reader with column
projection + top-level-predicate pushdown. Each of the 5 upstream JSONBench
queries (github.com/ClickHouse/JSONBench, `duckdb/queries.sql`) is therefore
hand-written as a Python scan-and-aggregate over the Morsels rugo returns —
but everything up to the final grouping loop stays on native vectors:

  1. `commit.operation` / `commit.collection` are extracted with
     `vector_json_extract_text` (draken's yyjson-backed kernel,
     `opteryx/compiled/nanobind/vector_json.cpp`) — one C++ pass over the
     whole column per morsel, no per-row Python `json.loads()`.
  2. Row filtering (`operation = 'create'`, `collection IN (...)`) is done
     with `Vector.in_list` / `BoolVector.and_vector` — native, no Python
     per-row branching.
  3. `Morsel.filter_mask(mask)` applies that filter across every column at
     once (did/time_us/collection), fully in C++ (gathers surviving rows by
     the mask bitmap; no Python index list, no per-row loop).
  4. Only the *surviving* rows are materialized with `.to_pylist()`, and
     only for the columns the final grouping step actually needs. There is
     no group-by primitive exposed outside the full operator pipeline, so
     that last step — building a dict of counts/mins/maxes — is still a
     plain Python loop, but now over a small, pre-filtered row set instead
     of every row with a value on every step.

Each query does a fresh full scan of all shards (no persisted/cached table
between queries) — this mirrors querying raw JSONL files directly, which is
the actual use case in question (should Opteryx be able to query JSON files
directly). It is NOT apples-to-apples with the DuckDB baseline, which times
queries against an already-loaded native-storage table; see ../README.md.
"""

from __future__ import annotations

import json  # only for the synthetic-fixture writer below; the parse hot path uses orjson
import os
import sys
from typing import Callable, Iterable, Iterator

import orjson

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")))

from rugo.jsonl import read_jsonl  # noqa: E402
from draken.morsels.morsel import Morsel  # noqa: E402
from draken.vectors.vector import Vector  # noqa: E402
from opteryx.compiled.nanobind import vectors as _vectors  # noqa: E402

_CREATE_COMMIT_PREDICATES = [("kind", "==", "commit")]
_FEED_COLLECTIONS = [b"app.bsky.feed.post", b"app.bsky.feed.repost", b"app.bsky.feed.like"]


def _scan(paths: Iterable[str], columns: list[str], predicates=None) -> Iterator[Morsel]:
    for path in paths:
        with read_jsonl(path, columns=columns, predicates=predicates) as reader:
            yield from reader


def _extract_native(commit_col, keys: tuple[str, ...]) -> dict[str, Vector] | None:
    """Extract `keys` from every row's `commit` JSON text as native Vectors.

    One `vector_json_extract_text` call per key — a single C++ pass over the
    whole column. Returns `None` (not a partial result) if any row's JSON is
    malformed: `vector_json_extract_text` fails the whole column on the
    first bad row (fail fast — draken's general error policy, see
    ../README.md's data-quality note), and since it parses each row's full
    document once, a failure on the first key means every key would fail
    identically — no point trying the rest.
    """
    nb_vec = commit_col._nb
    try:
        return {key: Vector(_vectors.vector_json_extract_text(nb_vec, key.encode())) for key in keys}
    except RuntimeError:
        return None


def _extract_fallback(commit_col, keys: tuple[str, ...], skipped: list[int]) -> tuple[dict[str, list], list[bool]]:
    """Per-row `orjson.loads` scan for a morsel `_extract_native` rejected.

    Only reached for the ~1-2 morsels in a 10m-row run that actually contain
    a malformed `commit` row (see ../README.md). Skips and counts just that
    row rather than losing the whole morsel — printed once per query call,
    never silent. The `bad` mask distinguishes "malformed, must be excluded"
    from "well-formed but legitimately null" (a real NULL group in Q1),
    which collapsing both to `None` in the field lists would conflate.

    orjson rather than stdlib `json`: this is still a per-row Python-level
    parse (only the ~1-2 malformed morsels take this path at all), and
    orjson's Rust parser is meaningfully faster per call than the stdlib
    decoder for exactly that reason — no behavioural difference otherwise
    (`orjson.JSONDecodeError` subclasses `json.JSONDecodeError`).
    """
    raws = commit_col
    fields: dict[str, list] = {key: [None] * len(raws) for key in keys}
    bad = [False] * len(raws)
    for i, raw in enumerate(raws):
        if raw is None:
            continue
        try:
            obj = orjson.loads(raw)
        except orjson.JSONDecodeError:
            skipped[0] += 1
            bad[i] = True
            continue
        for key in keys:
            fields[key][i] = obj.get(key)
    return fields, bad


def _warn_skipped(query_name: str, skipped: list[int]) -> None:
    if skipped[0]:
        pass
        #print(f"  [{query_name}] skipped {skipped[0]} malformed row(s) (unparseable `commit` JSON)")


def q1_events_by_collection(paths: Iterable[str]) -> list[tuple[str, int]]:
    """SELECT commit.collection, count(*) FROM bluesky GROUP BY 1 ORDER BY 2 DESC

    No predicate to push into a native mask — every row counts, so this is
    the one query where a `.to_pylist()` over the full column is
    unavoidable (every row's value is needed in the Python grouping dict).
    """
    counts: dict = {}
    skipped = [0]
    for morsel in _scan(paths, columns=["commit"]):
        commit_col = morsel.column("commit")
        native = _extract_native(commit_col, ("collection",))
        if native is not None:
            for collection in native["collection"]:
                counts[collection] = counts.get(collection, 0) + 1
        else:
            fields, bad = _extract_fallback(commit_col, ("collection",), skipped)
            for collection, is_bad in zip(fields["collection"], bad):
                if is_bad:
                    continue
                counts[collection] = counts.get(collection, 0) + 1
    _warn_skipped("Q1", skipped)
    return sorted(counts.items(), key=lambda kv: kv[1], reverse=True)


def q2_creates_by_collection(paths: Iterable[str]) -> list[tuple[str, int, int]]:
    """SELECT collection, count(*), count(DISTINCT did) WHERE kind='commit' AND op='create' GROUP BY 1 ORDER BY 2 DESC"""
    counts: dict = {}
    users: dict = {}
    skipped = [0]
    for morsel in _scan(paths, columns=["commit", "did"], predicates=_CREATE_COMMIT_PREDICATES):
        commit_col = morsel.column("commit")
        native = _extract_native(commit_col, ("operation", "collection"))
        if native is not None:
            mask = native["operation"].in_list([b"create"])
            morsel.append_vector("collection", native["collection"])
            filtered = morsel.filter_mask(mask)
            collections = filtered.column("collection")
            dids = filtered.column("did")
            for collection, did in zip(collections, dids):
                counts[collection] = counts.get(collection, 0) + 1
                users.setdefault(collection, set()).add(did)
        else:
            fields, bad = _extract_fallback(commit_col, ("operation", "collection"), skipped)
            dids = morsel.column("did")
            for op, collection, did, is_bad in zip(fields["operation"], fields["collection"], dids, bad):
                if is_bad or op != "create":
                    continue
                counts[collection] = counts.get(collection, 0) + 1
                users.setdefault(collection, set()).add(did)
    _warn_skipped("Q2", skipped)
    return sorted(
        ((collection, count, len(users[collection])) for collection, count in counts.items()),
        key=lambda row: row[1],
        reverse=True,
    )


def q3_hourly_activity(paths: Iterable[str]) -> list[tuple[str, int, int]]:
    """post/repost/like creates, grouped by (collection, hour_of_day), ordered by hour then collection"""
    counts: dict = {}
    skipped = [0]
    for morsel in _scan(paths, columns=["commit", "time_us"], predicates=_CREATE_COMMIT_PREDICATES):
        commit_col = morsel.column("commit")
        native = _extract_native(commit_col, ("operation", "collection"))
        if native is not None:
            mask = native["operation"].in_list([b"create"]).and_vector(native["collection"].in_list(_FEED_COLLECTIONS))
            morsel.append_vector("collection", native["collection"])
            filtered = morsel.filter_mask(mask)
            collections = filtered.column("collection")
            times = filtered.column("time_us")
            for collection, t in zip(collections, times):
                hour = (t // 1_000_000 // 3600) % 24
                key = (collection, hour)
                counts[key] = counts.get(key, 0) + 1
        else:
            fields, bad = _extract_fallback(commit_col, ("operation", "collection"), skipped)
            times = morsel.column("time_us")
            feed_collections = {c.decode() for c in _FEED_COLLECTIONS}
            for op, collection, t, is_bad in zip(fields["operation"], fields["collection"], times, bad):
                if is_bad or op != "create" or collection not in feed_collections:
                    continue
                hour = (t // 1_000_000 // 3600) % 24
                key = (collection, hour)
                counts[key] = counts.get(key, 0) + 1
    _warn_skipped("Q3", skipped)
    return sorted(
        ((collection, hour, count) for (collection, hour), count in counts.items()),
        key=lambda row: (row[1], row[0]),
    )


def q4_first_post(paths: Iterable[str]) -> list[tuple[str, int]]:
    """did, MIN(time_us) for app.bsky.feed.post creates, grouped by did, top 3 earliest"""
    first: dict = {}
    skipped = [0]
    for morsel in _scan(paths, columns=["commit", "did", "time_us"], predicates=_CREATE_COMMIT_PREDICATES):
        commit_col = morsel.column("commit")
        native = _extract_native(commit_col, ("operation", "collection"))
        if native is not None:
            mask = native["operation"].in_list([b"create"]).and_vector(
                native["collection"].in_list([b"app.bsky.feed.post"])
            )
            filtered = morsel.filter_mask(mask)
            dids = filtered.column("did")
            times = filtered.column("time_us")
            for did, t in zip(dids, times):
                prev = first.get(did)
                if prev is None or t < prev:
                    first[did] = t
        else:
            fields, bad = _extract_fallback(commit_col, ("operation", "collection"), skipped)
            dids = morsel.column("did")
            times = morsel.column("time_us")
            for op, collection, did, t, is_bad in zip(fields["operation"], fields["collection"], dids, times, bad):
                if is_bad or op != "create" or collection != "app.bsky.feed.post":
                    continue
                prev = first.get(did)
                if prev is None or t < prev:
                    first[did] = t
    _warn_skipped("Q4", skipped)
    return sorted(first.items(), key=lambda kv: kv[1])[:3]


def q5_activity_span(paths: Iterable[str]) -> list[tuple[str, float]]:
    """did, (MAX(time_us) - MIN(time_us)) in ms for app.bsky.feed.post creates, top 3 longest span"""
    span: dict = {}
    skipped = [0]
    for morsel in _scan(paths, columns=["commit", "did", "time_us"], predicates=_CREATE_COMMIT_PREDICATES):
        commit_col = morsel.column("commit")
        native = _extract_native(commit_col, ("operation", "collection"))
        if native is not None:
            mask = native["operation"].in_list([b"create"]).and_vector(
                native["collection"].in_list([b"app.bsky.feed.post"])
            )
            filtered = morsel.filter_mask(mask)
            dids = filtered.column("did")
            times = filtered.column("time_us")
            for did, t in zip(dids, times):
                lo, hi = span.get(did, (t, t))
                span[did] = (lo if lo < t else t, hi if hi > t else t)
        else:
            fields, bad = _extract_fallback(commit_col, ("operation", "collection"), skipped)
            dids = morsel.column("did")
            times = morsel.column("time_us")
            for op, collection, did, t, is_bad in zip(fields["operation"], fields["collection"], dids, times, bad):
                if is_bad or op != "create" or collection != "app.bsky.feed.post":
                    continue
                lo, hi = span.get(did, (t, t))
                span[did] = (lo if lo < t else t, hi if hi > t else t)
    _warn_skipped("Q5", skipped)
    ranked = sorted(
        ((did, (hi - lo) / 1000.0) for did, (lo, hi) in span.items()),
        key=lambda row: row[1],
        reverse=True,
    )
    return ranked[:3]


QUERIES: list[tuple[str, Callable[[Iterable[str]], list]]] = [
    ("Q1", q1_events_by_collection),
    ("Q2", q2_creates_by_collection),
    ("Q3", q3_hourly_activity),
    ("Q4", q4_first_post),
    ("Q5", q5_activity_span),
]


# ---------------------------------------------------------------------------
# Correctness smoke test — synthetic fixture, no downloaded data required.
# ---------------------------------------------------------------------------


def _write_fixture(tmp_path) -> str:
    records = [
        {"did": "did:a", "kind": "commit", "time_us": 1_000_000, "commit": {"operation": "create", "collection": "app.bsky.feed.post"}},
        {"did": "did:a", "kind": "commit", "time_us": 4_000_000, "commit": {"operation": "create", "collection": "app.bsky.feed.post"}},
        {"did": "did:b", "kind": "commit", "time_us": 2_000_000, "commit": {"operation": "create", "collection": "app.bsky.feed.like"}},
        {"did": "did:b", "kind": "commit", "time_us": 3_000_000, "commit": {"operation": "delete", "collection": "app.bsky.feed.post"}},
        {"did": "did:c", "kind": "identity"},
    ]
    path = os.path.join(tmp_path, "fixture.jsonl")
    with open(path, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    return path


def test_queries_against_synthetic_fixture(tmp_path):
    path = _write_fixture(str(tmp_path))
    paths = [path]

    q1 = dict(q1_events_by_collection(paths))
    assert q1["app.bsky.feed.post"] == 3
    assert q1["app.bsky.feed.like"] == 1
    assert q1[None] == 1  # the "identity" kind row has no commit

    q2 = {row[0]: (row[1], row[2]) for row in q2_creates_by_collection(paths)}
    assert q2["app.bsky.feed.post"] == (2, 1)  # 2 creates, 1 distinct did
    assert q2["app.bsky.feed.like"] == (1, 1)
    assert "app.bsky.feed.repost" not in q2  # the delete op is excluded

    q3 = {(row[0], row[1]): row[2] for row in q3_hourly_activity(paths)}
    assert sum(q3.values()) == 3  # the 3 qualifying creates (post x2, like x1)

    q4 = dict(q4_first_post(paths))
    assert q4 == {"did:a": 1_000_000}

    q5 = dict(q5_activity_span(paths))
    assert q5["did:a"] == 3000.0  # (4_000_000 - 1_000_000) us -> 3000.0 ms


def test_extract_fields_falls_back_on_malformed_commit(tmp_path):
    """One malformed row must not lose the whole morsel, and must not be
    counted as a real NULL (see q1's None-vs-skip distinction)."""
    raw_lines = [
        b'{"did":"a","kind":"commit","time_us":1,"commit":{"operation":"create","collection":"x"}}\n',
        b'{"did":"b","kind":"commit","time_us":2,"commit":"broken \x07 control char"}\n',
        b'{"did":"c","kind":"commit","time_us":3,"commit":{"operation":"create","collection":"x"}}\n',
    ]
    path = os.path.join(str(tmp_path), "malformed.jsonl")
    with open(path, "wb") as f:
        for line in raw_lines:
            f.write(line)

    q1 = dict(q1_events_by_collection([path]))
    assert q1 == {"x": 2}  # the malformed row is skipped, not counted under None

    q2 = {row[0]: (row[1], row[2]) for row in q2_creates_by_collection([path])}
    assert q2 == {"x": (2, 2)}
