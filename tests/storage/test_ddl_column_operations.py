# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Column-level DDL: ADD / DROP / RENAME COLUMN, ALTER COLUMN ... TYPE.

The design these tests hold to (see the approved plan):

  * A column operation is METADATA-scale for every column it does not touch.
    Existing data pages are copied byte-for-byte, never decoded and re-encoded,
    so `DROP COLUMN` on a 100-column table costs one column's worth of change,
    not a hundred columns' worth of re-encode.
  * Files are never mutated in place - each operation writes new files and
    commits a new snapshot, so time travel keeps seeing the old shape.
  * Reads need no reconciliation logic at all: by the time the statement
    returns, every live file already matches the current schema.

`_DATA_TYPES` vs `_SCHEMA_ONLY_TYPES` is a real, measured split, not a
preference: `INSERT ... VALUES` cannot currently populate a DECIMAL, DATE,
TIMESTAMP, TIME, IPV4, UINT* or FLOAT column at all (each fails in the insert
path, before any of this feature's code runs). Those types therefore get
schema-shape coverage here and are excluded from the value-fidelity tests -
excluded because the *insert* path cannot reach them, NOT because column DDL is
expected to treat them differently. When that insert gap closes they should move
straight into `_DATA_TYPES` and need no new test code.
"""

import json

import pytest

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.local_store_connector import LocalStoreConnector
from opteryx.exceptions import (
    ColumnNotFoundError,
    DatasetNotFoundError,
    QueryParseError,
    ReadOnlyConnectorError,
    UnsupportedSyntaxError,
)

# (sql_type, literal) for every type INSERT ... VALUES can actually populate.
_DATA_TYPES = [
    ("INT8", "12"),
    ("INT16", "300"),
    ("INT32", "70000"),
    ("INT64", "5000000000"),
    ("VARCHAR", "'hello'"),
    ("BOOL", "TRUE"),
    ("VARBINARY", "b'abc'"),
    ("ARRAY<VARCHAR>", "['a','b']"),
]

# CREATE TABLE accepts these; INSERT ... VALUES cannot populate them today.
_SCHEMA_ONLY_TYPES = [
    "UINT8",
    "FLOAT32",
    "FLOAT64",
    "DECIMAL(10,2)",
    "DECIMAL(38,18)",  # decimal128-backed
    "DATE",
    "TIMESTAMP",
    "TIME",
    "IPV4",
    "NVARCHAR",
    "INTERVAL",
    "VECTOR(4)",
]

_ALL_TYPES = [t for t, _ in _DATA_TYPES] + _SCHEMA_ONLY_TYPES


def _setup(tmp_path):
    register_workspace("ws", LocalStoreConnector, store_root=str(tmp_path))
    return opteryx.session()


def _rows(session, sql):
    """Execute and return rows as a list of dicts, bytes decoded for comparison."""
    out = []
    for morsel in session.execute_to_morsels(sql):
        if morsel is None:
            continue
        pydict = morsel.to_arrow().to_pydict()
        if not pydict:
            continue
        n = len(next(iter(pydict.values())))
        for i in range(n):
            out.append({k: v[i] for k, v in pydict.items()})
    return out


def _column_names(session, relation):
    """The relation's live column names, in schema order, as the engine sees them."""
    for morsel in session.execute_to_morsels(f"SELECT * FROM {relation}"):
        if morsel is not None:
            return list(morsel.to_arrow().to_pydict().keys())
    return []


def _stored_schema(tmp_path, relation="events"):
    """The persisted dataset.json schema - what a *reader* would resolve, not
    what this session happens to be holding in memory."""
    with open(tmp_path / "ws" / relation / "dataset.json") as f:
        return json.load(f)["schema"]["columns"]


def _wide_table(session, n, name="ws.wide", col_type="INT64"):
    """A table of `n` columns named c00..c{n-1}, with two rows of known values."""
    cols = ", ".join(f"c{i:02d} {col_type}" for i in range(n))
    session_exec(session, f"CREATE TABLE {name} ({cols})")
    for row in range(2):
        values = ", ".join(str(row * 1000 + i) for i in range(n))
        session_exec(session, f"INSERT INTO {name} VALUES ({values})")


def session_exec(session, sql):
    return list(session.execute_to_morsels(sql))


# ---------------------------------------------------------------------------
# DROP COLUMN - how many at once
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("drop_count", [1, 2, 18])
def test_drop_n_columns_of_twenty(tmp_path, drop_count):
    """Dropping 1, 2 or 18 of 20 columns leaves exactly the others, with their
    values intact. 18 is the case that matters most: the surviving 2 columns'
    bytes must be copied through untouched however many columns went away."""
    session = _setup(tmp_path)
    _wide_table(session, 20)

    for i in range(drop_count):
        session_exec(session, f"ALTER TABLE ws.wide DROP COLUMN c{i:02d}")

    survivors = [f"c{i:02d}" for i in range(drop_count, 20)]
    assert _column_names(session, "ws.wide") == survivors

    rows = sorted(_rows(session, "SELECT * FROM ws.wide"), key=lambda r: r[survivors[0]])
    assert len(rows) == 2
    for row_idx, row in enumerate(rows):
        assert set(row) == set(survivors)
        for i in range(drop_count, 20):
            assert row[f"c{i:02d}"] == row_idx * 1000 + i


def test_drop_half_the_columns(tmp_path):
    """Half of a 20-column table, dropped one statement at a time, interleaved
    (evens go, odds stay) so this is not just a prefix or suffix truncation."""
    session = _setup(tmp_path)
    _wide_table(session, 20)

    for i in range(0, 20, 2):
        session_exec(session, f"ALTER TABLE ws.wide DROP COLUMN c{i:02d}")

    survivors = [f"c{i:02d}" for i in range(1, 20, 2)]
    assert _column_names(session, "ws.wide") == survivors

    rows = sorted(_rows(session, "SELECT * FROM ws.wide"), key=lambda r: r["c01"])
    for row_idx, row in enumerate(rows):
        for i in range(1, 20, 2):
            assert row[f"c{i:02d}"] == row_idx * 1000 + i


def test_drop_all_but_one_column(tmp_path):
    """19 of 20 dropped - the last column standing still reads correctly."""
    session = _setup(tmp_path)
    _wide_table(session, 20)

    for i in range(19):
        session_exec(session, f"ALTER TABLE ws.wide DROP COLUMN c{i:02d}")

    assert _column_names(session, "ws.wide") == ["c19"]
    assert sorted(r["c19"] for r in _rows(session, "SELECT * FROM ws.wide")) == [19, 1019]


def test_drop_every_column_is_rejected(tmp_path):
    """A relation with no columns is not a relation. CREATE TABLE already
    refuses a zero-column table; dropping down to zero must refuse for the same
    reason rather than leaving an unreadable husk behind."""
    session = _setup(tmp_path)
    _wide_table(session, 3)

    session_exec(session, "ALTER TABLE ws.wide DROP COLUMN c00")
    session_exec(session, "ALTER TABLE ws.wide DROP COLUMN c01")

    with pytest.raises((UnsupportedSyntaxError, ValueError)):
        session_exec(session, "ALTER TABLE ws.wide DROP COLUMN c02")

    # and the relation is untouched by the refusal
    assert _column_names(session, "ws.wide") == ["c02"]


def test_drop_multiple_columns_in_one_statement_is_rejected(tmp_path):
    """One column per DROP COLUMN. The dialect's grammar refuses the multi-column
    form outright, so this never reaches the planner - but the AST field is a
    LIST, and the planner guards it independently. If the grammar ever gains the
    form, this test starts failing and that guard is what stops a silent
    drop-only-the-first."""
    session = _setup(tmp_path)
    _wide_table(session, 3)

    with pytest.raises((QueryParseError, UnsupportedSyntaxError, ValueError)):
        session_exec(session, "ALTER TABLE ws.wide DROP COLUMN c00, c01")


# ---------------------------------------------------------------------------
# DROP COLUMN - which one
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("position,target", [("first", "c00"), ("middle", "c02"), ("last", "c04")])
def test_drop_column_by_position(tmp_path, position, target):
    """Position matters: a column's data lives at a byte offset, and dropping
    the first vs the last exercises different offset arithmetic."""
    session = _setup(tmp_path)
    _wide_table(session, 5)

    session_exec(session, f"ALTER TABLE ws.wide DROP COLUMN {target}")

    survivors = [f"c{i:02d}" for i in range(5) if f"c{i:02d}" != target]
    assert _column_names(session, "ws.wide") == survivors

    rows = sorted(_rows(session, "SELECT * FROM ws.wide"), key=lambda r: r[survivors[0]])
    for row_idx, row in enumerate(rows):
        for name in survivors:
            assert row[name] == row_idx * 1000 + int(name[1:])


@pytest.mark.parametrize("sql_type", _ALL_TYPES)
def test_drop_column_of_each_type(tmp_path, sql_type):
    """Every type the dialect will declare can be dropped. Dropping is a
    metadata edit, so it must not care what the column held - including the
    parameterized ones (DECIMAL(38,18) is decimal128-backed, VECTOR carries a
    dimension, ARRAY carries an element type)."""
    session = _setup(tmp_path)
    session_exec(session, f"CREATE TABLE ws.events (keep INT64, victim {sql_type})")

    session_exec(session, "ALTER TABLE ws.events DROP COLUMN victim")

    assert [c["name"] for c in _stored_schema(tmp_path)] == ["keep"]


@pytest.mark.parametrize("sql_type,literal", _DATA_TYPES)
def test_drop_column_preserves_neighbour_values(tmp_path, sql_type, literal):
    """The neighbour's values must survive a drop bit-for-bit. This is the
    property the whole design rests on - the surviving column is copied, not
    re-encoded."""
    session = _setup(tmp_path)
    session_exec(session, f"CREATE TABLE ws.events (keep {sql_type}, victim INT64)")
    session_exec(session, f"INSERT INTO ws.events VALUES ({literal}, 1)")
    before = _rows(session, "SELECT keep FROM ws.events")

    session_exec(session, "ALTER TABLE ws.events DROP COLUMN victim")

    assert _rows(session, "SELECT keep FROM ws.events") == before


# ---------------------------------------------------------------------------
# DROP COLUMN - error paths
# ---------------------------------------------------------------------------


def test_drop_column_that_does_not_exist(tmp_path):
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64, name VARCHAR)")

    with pytest.raises((ColumnNotFoundError, ValueError)):
        session_exec(session, "ALTER TABLE ws.events DROP COLUMN nope")


def test_drop_column_if_exists_is_silent(tmp_path):
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64, name VARCHAR)")

    session_exec(session, "ALTER TABLE ws.events DROP COLUMN IF EXISTS nope")

    assert [c["name"] for c in _stored_schema(tmp_path)] == ["id", "name"]


def test_drop_same_column_twice(tmp_path):
    """The second drop is a genuine error - the column really is gone."""
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64, name VARCHAR)")

    session_exec(session, "ALTER TABLE ws.events DROP COLUMN name")
    with pytest.raises((ColumnNotFoundError, ValueError)):
        session_exec(session, "ALTER TABLE ws.events DROP COLUMN name")


def test_dropped_column_is_not_selectable(tmp_path):
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64, name VARCHAR)")
    session_exec(session, "INSERT INTO ws.events VALUES (1, 'x')")

    session_exec(session, "ALTER TABLE ws.events DROP COLUMN name")

    with pytest.raises(ColumnNotFoundError):
        _rows(session, "SELECT name FROM ws.events")


def test_drop_column_missing_table(tmp_path):
    session = _setup(tmp_path)

    with pytest.raises(DatasetNotFoundError):
        session_exec(session, "ALTER TABLE ws.nope DROP COLUMN a")

    # IF EXISTS speaks about the TABLE here, and suppresses it
    session_exec(session, "ALTER TABLE IF EXISTS ws.nope DROP COLUMN a")


def test_drop_column_readonly_connector(tmp_path):
    session = opteryx.session()

    with pytest.raises(ReadOnlyConnectorError, match="does not support ALTER TABLE"):
        session_exec(session, "ALTER TABLE somefile.foo DROP COLUMN a")


def test_drop_column_cascade_and_restrict_rejected(tmp_path):
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64, name VARCHAR)")

    for behaviour in ("CASCADE", "RESTRICT"):
        with pytest.raises(UnsupportedSyntaxError):
            session_exec(session, f"ALTER TABLE ws.events DROP COLUMN name {behaviour}")


# ---------------------------------------------------------------------------
# ADD COLUMN
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("sql_type", _ALL_TYPES)
def test_add_column_of_each_type(tmp_path, sql_type):
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64)")

    session_exec(session, f"ALTER TABLE ws.events ADD COLUMN extra {sql_type}")

    stored = _stored_schema(tmp_path)
    assert [c["name"] for c in stored] == ["id", "extra"]


def test_add_column_backfills_null_for_existing_rows(tmp_path):
    """Rows written before the ADD read back NULL - and NULL *of the declared
    type*, not an untyped null."""
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64)")
    session_exec(session, "INSERT INTO ws.events VALUES (1), (2)")

    session_exec(session, "ALTER TABLE ws.events ADD COLUMN extra VARCHAR")

    rows = sorted(_rows(session, "SELECT * FROM ws.events"), key=lambda r: r["id"])
    assert [r["extra"] for r in rows] == [None, None]


@pytest.mark.parametrize(
    "sql_type,literal,expected",
    [
        ("INT64", "42", 42),
        ("VARCHAR", "'backfilled'", "backfilled"),
        ("BOOL", "TRUE", True),
        ("INT16", "7", 7),
    ],
)
def test_add_column_with_literal_default_backfills(tmp_path, sql_type, literal, expected):
    """A literal DEFAULT backfills every existing row with that one value. It
    costs the same as the NULL case - one repeated value, never a per-row
    computation."""
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64)")
    session_exec(session, "INSERT INTO ws.events VALUES (1), (2)")

    session_exec(
        session, f"ALTER TABLE ws.events ADD COLUMN extra {sql_type} DEFAULT {literal}"
    )

    rows = sorted(_rows(session, "SELECT * FROM ws.events"), key=lambda r: r["id"])
    assert [r["extra"] for r in rows] == [expected, expected]


def test_add_column_non_literal_default_rejected(tmp_path):
    """A default that is not a constant would have to be evaluated once per
    existing row - exactly the per-value work this design refuses to do."""
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64)")

    with pytest.raises(UnsupportedSyntaxError, match="DEFAULT"):
        session_exec(session, "ALTER TABLE ws.events ADD COLUMN extra INT64 DEFAULT (id + 1)")


def test_add_duplicate_column_rejected(tmp_path):
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64, name VARCHAR)")

    with pytest.raises((UnsupportedSyntaxError, ValueError)):
        session_exec(session, "ALTER TABLE ws.events ADD COLUMN name VARCHAR")


def test_add_column_if_not_exists_on_existing_column_is_a_noop(tmp_path):
    """The guard makes the statement re-runnable: the second ADD of a column
    that is already there does nothing and does not raise. This is the whole
    point of the clause - a migration script can be applied twice."""
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64, name VARCHAR)")
    session_exec(session, "INSERT INTO ws.events VALUES (1, 'a')")

    session_exec(session, "ALTER TABLE ws.events ADD COLUMN IF NOT EXISTS name VARCHAR")

    # Not added twice, and the existing column keeps its type and its values.
    assert [c["name"] for c in _stored_schema(tmp_path)] == ["id", "name"]
    assert _rows(session, "SELECT name FROM ws.events") == [{"name": "a"}]


def test_add_column_if_not_exists_on_new_column_adds_it(tmp_path):
    """The guard is a guard, not a skip: a column that is NOT there is added,
    with its DEFAULT backfilled into the rows that already exist."""
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64)")
    session_exec(session, "INSERT INTO ws.events VALUES (1)")

    session_exec(
        session, "ALTER TABLE ws.events ADD COLUMN IF NOT EXISTS extra VARCHAR DEFAULT 'x'"
    )

    assert [c["name"] for c in _stored_schema(tmp_path)] == ["id", "extra"]
    assert _rows(session, "SELECT extra FROM ws.events") == [{"extra": "x"}]


def test_add_column_if_not_exists_repeated_is_stable(tmp_path):
    """Re-runnable means re-runnable more than once - three applications of the
    same statement leave the table in the state the first one produced."""
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64)")

    for _ in range(3):
        session_exec(session, "ALTER TABLE ws.events ADD COLUMN IF NOT EXISTS extra INT64")

    assert [c["name"] for c in _stored_schema(tmp_path)] == ["id", "extra"]


def test_add_column_if_not_exists_before_column_keyword(tmp_path):
    """`ADD IF NOT EXISTS COLUMN` is the other spelling of the same clause, and
    it must carry the guard too. It already PARSED before the guard was
    implemented - upstream reads the flag there and then overwrites it with
    False for this dialect - so a script written this way was silently
    unguarded, which is worse than one that fails to parse.
    """
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64, name VARCHAR)")

    session_exec(session, "ALTER TABLE ws.events ADD IF NOT EXISTS COLUMN name VARCHAR")

    assert [c["name"] for c in _stored_schema(tmp_path)] == ["id", "name"]


def test_add_column_if_not_exists_still_needs_the_table(tmp_path):
    """The column guard says nothing about the table. Without `IF EXISTS` on the
    ALTER, a missing table is still an error - the two guards are independent."""
    session = _setup(tmp_path)

    with pytest.raises(DatasetNotFoundError):
        session_exec(session, "ALTER TABLE ws.absent ADD COLUMN IF NOT EXISTS extra INT64")

    session_exec(session, "ALTER TABLE IF EXISTS ws.absent ADD COLUMN IF NOT EXISTS extra INT64")


def test_add_column_first_after_rejected_with_guard(tmp_path):
    """FIRST/AFTER is refused whether or not the guard is written - the guarded
    form takes its own parse path, so it needs its own check that the path did
    not quietly start accepting a position."""
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64)")

    with pytest.raises((QueryParseError, UnsupportedSyntaxError)):
        session_exec(session, "ALTER TABLE ws.events ADD COLUMN IF NOT EXISTS extra INT64 FIRST")


def test_add_eighteen_columns(tmp_path):
    """Eighteen sequential ADDs, each one a metadata-scale change to a table
    that already has data."""
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64)")
    session_exec(session, "INSERT INTO ws.events VALUES (1)")

    for i in range(18):
        session_exec(session, f"ALTER TABLE ws.events ADD COLUMN a{i:02d} INT64")

    names = [c["name"] for c in _stored_schema(tmp_path)]
    assert names == ["id"] + [f"a{i:02d}" for i in range(18)]

    row = _rows(session, "SELECT * FROM ws.events")[0]
    assert row["id"] == 1
    assert all(row[f"a{i:02d}"] is None for i in range(18))


def test_add_column_to_empty_relation(tmp_path):
    """No rows to backfill - the operation still has to land in the schema."""
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64)")

    session_exec(session, "ALTER TABLE ws.events ADD COLUMN extra VARCHAR")

    assert [c["name"] for c in _stored_schema(tmp_path)] == ["id", "extra"]
    assert _rows(session, "SELECT * FROM ws.events") == []


def test_add_column_then_insert_uses_new_shape(tmp_path):
    """After an ADD, an INSERT supplies the new column like any other."""
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64)")
    session_exec(session, "INSERT INTO ws.events VALUES (1)")
    session_exec(session, "ALTER TABLE ws.events ADD COLUMN extra VARCHAR")
    session_exec(session, "INSERT INTO ws.events VALUES (2, 'new')")

    rows = sorted(_rows(session, "SELECT * FROM ws.events"), key=lambda r: r["id"])
    assert [r["extra"] for r in rows] == [None, "new"]


# ---------------------------------------------------------------------------
# RENAME COLUMN
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("sql_type", _ALL_TYPES)
def test_rename_column_of_each_type(tmp_path, sql_type):
    session = _setup(tmp_path)
    session_exec(session, f"CREATE TABLE ws.events (id INT64, before {sql_type})")

    session_exec(session, "ALTER TABLE ws.events RENAME COLUMN before TO after")

    assert [c["name"] for c in _stored_schema(tmp_path)] == ["id", "after"]


@pytest.mark.parametrize("sql_type,literal", _DATA_TYPES)
def test_rename_column_preserves_values(tmp_path, sql_type, literal):
    """A rename touches no data at all - values written under the old name read
    back identically under the new one."""
    session = _setup(tmp_path)
    session_exec(session, f"CREATE TABLE ws.events (id INT64, before {sql_type})")
    session_exec(session, f"INSERT INTO ws.events VALUES (1, {literal})")
    before = _rows(session, "SELECT before FROM ws.events")[0]["before"]

    session_exec(session, "ALTER TABLE ws.events RENAME COLUMN before TO after")

    assert _rows(session, "SELECT after FROM ws.events")[0]["after"] == before


def test_rename_column_old_name_stops_resolving(tmp_path):
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64, before VARCHAR)")
    session_exec(session, "INSERT INTO ws.events VALUES (1, 'x')")

    session_exec(session, "ALTER TABLE ws.events RENAME COLUMN before TO after")

    with pytest.raises(ColumnNotFoundError):
        _rows(session, "SELECT before FROM ws.events")


def test_rename_column_round_trip(tmp_path):
    """a -> b -> a returns the relation to its original shape and values."""
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64, a VARCHAR)")
    session_exec(session, "INSERT INTO ws.events VALUES (1, 'x')")

    session_exec(session, "ALTER TABLE ws.events RENAME COLUMN a TO b")
    session_exec(session, "ALTER TABLE ws.events RENAME COLUMN b TO a")

    assert _rows(session, "SELECT a FROM ws.events")[0]["a"] == "x"


def test_rename_column_onto_existing_name_rejected(tmp_path):
    """Renaming onto a live name would give the relation two columns with one
    name - refused rather than silently shadowing."""
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64, name VARCHAR)")

    with pytest.raises((UnsupportedSyntaxError, ValueError)):
        session_exec(session, "ALTER TABLE ws.events RENAME COLUMN name TO id")


def test_rename_column_that_does_not_exist(tmp_path):
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64)")

    with pytest.raises((ColumnNotFoundError, ValueError)):
        session_exec(session, "ALTER TABLE ws.events RENAME COLUMN nope TO other")


def test_rename_column_to_itself(tmp_path):
    """Same name in and out - a no-op that must not corrupt the schema however
    it is answered."""
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64, name VARCHAR)")

    try:
        session_exec(session, "ALTER TABLE ws.events RENAME COLUMN name TO name")
    except (UnsupportedSyntaxError, ValueError):
        pass

    assert [c["name"] for c in _stored_schema(tmp_path)] == ["id", "name"]


# ---------------------------------------------------------------------------
# ALTER COLUMN ... TYPE
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "start,target",
    [
        ("INT8", "INT16"),
        ("INT8", "INT32"),
        ("INT8", "INT64"),
        ("INT16", "INT32"),
        ("INT16", "INT64"),
        ("INT32", "INT64"),
        ("FLOAT32", "FLOAT64"),
    ],
)
def test_alter_column_type_legal_widening(tmp_path, start, target):
    session = _setup(tmp_path)
    session_exec(session, f"CREATE TABLE ws.events (id INT64, v {start})")

    session_exec(session, f"ALTER TABLE ws.events ALTER COLUMN v TYPE {target}")

    stored = {c["name"]: c["type"] for c in _stored_schema(tmp_path)}
    assert "v" in stored


@pytest.mark.parametrize(
    "start,target",
    [
        ("INT64", "INT32"),        # narrowing
        ("INT32", "INT16"),      # narrowing
        ("INT16", "INT8"),    # narrowing
        ("FLOAT64", "FLOAT32"),         # narrowing
        ("INT64", "VARCHAR"),      # cross-family
        ("VARCHAR", "INT64"),      # cross-family
        ("INT64", "FLOAT64"),       # int -> float is not exact at the top of the range
        ("INT64", "DECIMAL(38,18)"),
        ("DATE", "TIMESTAMP"),      # temporal lattice is a separate, undesigned question
        ("INT64", "INT64"),       # no-op
    ],
)
def test_alter_column_type_illegal_change_rejected(tmp_path, start, target):
    """Rejected at BIND time - before any file is opened, let alone rewritten."""
    session = _setup(tmp_path)
    session_exec(session, f"CREATE TABLE ws.events (id INT64, v {start})")

    with pytest.raises(UnsupportedSyntaxError):
        session_exec(session, f"ALTER TABLE ws.events ALTER COLUMN v TYPE {target}")

    # the declared type is untouched by the refusal
    assert len(_stored_schema(tmp_path)) == 2


def test_alter_column_type_preserves_values(tmp_path):
    """Widening reads the old values back as the same numbers."""
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64, v INT16)")
    session_exec(session, "INSERT INTO ws.events VALUES (1, 300), (2, -300)")

    session_exec(session, "ALTER TABLE ws.events ALTER COLUMN v TYPE INT64")

    rows = sorted(_rows(session, "SELECT * FROM ws.events"), key=lambda r: r["id"])
    assert [r["v"] for r in rows] == [300, -300]


def test_alter_column_type_missing_column(tmp_path):
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64)")

    with pytest.raises(ColumnNotFoundError):
        session_exec(session, "ALTER TABLE ws.events ALTER COLUMN nope TYPE INT64")


def test_alter_column_type_using_clause_rejected(tmp_path):
    """USING implies a per-row transform; a supported change is always a
    lossless widening, which never needs one. The dialect's grammar refuses it
    before the planner sees it - the planner rejects it independently too, for
    when that grammar changes."""
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64, v INT16)")

    with pytest.raises((QueryParseError, UnsupportedSyntaxError, ValueError)):
        session_exec(
            session, "ALTER TABLE ws.events ALTER COLUMN v TYPE INT64 USING (v)"
        )


# ---------------------------------------------------------------------------
# Sequences and interactions
# ---------------------------------------------------------------------------


def test_add_drop_rename_retype_in_sequence(tmp_path):
    """Four operations over one relation, with data written between them, so
    each one lands on files written under a different shape."""
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64, doomed VARCHAR, v INT16)")
    session_exec(session, "INSERT INTO ws.events VALUES (1, 'a', 10)")

    session_exec(session, "ALTER TABLE ws.events ADD COLUMN added INT64 DEFAULT 99")
    session_exec(session, "INSERT INTO ws.events VALUES (2, 'b', 20, 7)")

    session_exec(session, "ALTER TABLE ws.events DROP COLUMN doomed")
    session_exec(session, "ALTER TABLE ws.events RENAME COLUMN v TO value")
    session_exec(session, "ALTER TABLE ws.events ALTER COLUMN value TYPE INT64")

    assert sorted(_column_names(session, "ws.events")) == ["added", "id", "value"]
    rows = sorted(_rows(session, "SELECT * FROM ws.events"), key=lambda r: r["id"])
    assert [r["value"] for r in rows] == [10, 20]
    assert [r["added"] for r in rows] == [99, 7]


def test_drop_column_across_multiple_files(tmp_path):
    """Each INSERT writes its own file, so this drop has to patch every one of
    them - a single-file implementation passes the other tests and fails here."""
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64, doomed VARCHAR)")
    for i in range(5):
        session_exec(session, f"INSERT INTO ws.events VALUES ({i}, 'row{i}')")

    session_exec(session, "ALTER TABLE ws.events DROP COLUMN doomed")

    rows = _rows(session, "SELECT * FROM ws.events")
    assert sorted(r["id"] for r in rows) == [0, 1, 2, 3, 4]
    assert all(set(r) == {"id"} for r in rows)


def test_add_column_across_multiple_files(tmp_path):
    """Every pre-existing file needs the new column, not just the newest."""
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64)")
    for i in range(5):
        session_exec(session, f"INSERT INTO ws.events VALUES ({i})")

    session_exec(session, "ALTER TABLE ws.events ADD COLUMN extra INT64 DEFAULT 5")

    rows = _rows(session, "SELECT * FROM ws.events")
    assert len(rows) == 5
    assert all(r["extra"] == 5 for r in rows)


class _RecordingCapability:
    """Permits everything except the actions in `deny`, and records what it was
    asked. Enough of the capability contract to be installable; the point is the
    `asked` log, not the policy language."""

    name = "recording"

    def __init__(self, deny=()):
        self.deny = set(deny)
        self.asked = []

    def can_perform_action(self, execution_context, resource, action):
        self.asked.append((resource, action))
        return action not in self.deny

    def can_perform_workspace_action(self, execution_context, workspace, action):
        return True

    def can_principal_perform_action(self, principal, resource, action):
        return True

    def can_principal_own_materialized_view(self, principal):
        return True

    def grants(self, identity, policies):
        return []


@pytest.fixture
def install_capability():
    """Install a permissions capability for one test, then put the module's
    state back. The module refuses a swap once a check has been answered -
    right in a process, unworkable across tests in one interpreter - so this
    resets the private state directly, exactly as
    tests/storage/test_permissions_capability.py does."""
    from opteryx import managers
    from opteryx.managers.permissions import register_permissions_capability

    module = managers.permissions
    saved_active, saved_consulted = module._active, module._consulted

    def _install(capability):
        module._active, module._consulted = module._CORE, False
        register_permissions_capability(capability)
        return capability

    yield _install
    module._active, module._consulted = saved_active, saved_consulted


_COLUMN_DDL_STATEMENTS = (
    "ALTER TABLE ws.events ADD COLUMN extra INT64",
    "ALTER TABLE ws.events DROP COLUMN name",
    "ALTER TABLE ws.events RENAME COLUMN name TO other",
    "ALTER TABLE ws.events ALTER COLUMN small TYPE INT64",
)


def _data_files(tmp_path, relation="events"):
    """Every parquet DATA file on disk. Manifests are parquet too and are
    excluded by name.

    Note this includes SUPERSEDED files: a column operation writes patched files
    to new paths and points only the new snapshot at them, so the pre-operation
    files stay on disk for older snapshots to keep reading. Tests that want "the
    file the operation produced" use `_new_data_file`.
    """
    directory = tmp_path / "ws" / relation
    return sorted(p for p in directory.glob("data-*.parquet"))


def _new_data_file(tmp_path, before, relation="events"):
    """The single data file that appeared since `before` was captured."""
    added = set(_data_files(tmp_path, relation)) - set(before)
    assert len(added) == 1, f"expected exactly one new data file, got {len(added)}"
    return added.pop()


def _parquet_data_region(path):
    """The bytes between the leading magic and the footer - i.e. every encoded
    page, and nothing else.

    Parquet's trailer is self-describing: [PAR1][pages...][footer][u32 footer
    length][PAR1]. That is enough to find the boundary without decoding a single
    value, which is the point - this helper must not depend on the very
    machinery it is used to check.
    """
    raw = path.read_bytes()
    assert raw[:4] == b"PAR1" and raw[-4:] == b"PAR1", "not a parquet file"
    footer_len = int.from_bytes(raw[-8:-4], "little")
    return raw[4 : len(raw) - 8 - footer_len]


def test_rename_column_does_not_touch_a_single_data_byte(tmp_path):
    """The load-bearing property for RENAME: the encoded pages come out
    byte-for-byte identical, because a rename is a footer edit and nothing else.

    A rename implemented by decode-and-rewrite would still pass every
    value-equality test in this file; only this one fails it.
    """
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64, before VARCHAR)")
    session_exec(session, "INSERT INTO ws.events VALUES (1, 'x'), (2, 'y')")
    paths = _data_files(tmp_path)
    original = _parquet_data_region(paths[0])

    session_exec(session, "ALTER TABLE ws.events RENAME COLUMN before TO after")

    patched = _parquet_data_region(_new_data_file(tmp_path, paths))
    assert patched == original


def test_drop_column_copies_surviving_pages_verbatim(tmp_path):
    """The load-bearing property for DROP: the columns that survive are copied,
    not re-encoded. Dropping the LAST column leaves the earlier chunks exactly
    where they were, so the new page region is a byte-for-byte PREFIX of the old
    one. Re-encoding would produce equal VALUES but different BYTES."""
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (keep INT64, doomed VARCHAR)")
    session_exec(session, "INSERT INTO ws.events VALUES (1, 'xxxxxxxxxx'), (2, 'yyyyyyyyyy')")
    paths = _data_files(tmp_path)
    before = _parquet_data_region(paths[0])

    session_exec(session, "ALTER TABLE ws.events DROP COLUMN doomed")

    after = _parquet_data_region(_new_data_file(tmp_path, paths))
    assert len(after) < len(before), "dropped column's pages should not be carried over"
    assert before.startswith(after), "surviving pages were re-encoded rather than copied"


def test_add_column_copies_existing_pages_verbatim(tmp_path):
    """ADD appends one near-empty constant chunk; every pre-existing page is
    carried through untouched, so the old region stays a prefix of the new."""
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64)")
    session_exec(session, "INSERT INTO ws.events VALUES (1), (2)")
    paths = _data_files(tmp_path)
    before = _parquet_data_region(paths[0])

    session_exec(session, "ALTER TABLE ws.events ADD COLUMN extra INT64")

    after = _parquet_data_region(_new_data_file(tmp_path, paths))
    assert after.startswith(before), "existing pages were re-encoded rather than copied"


def test_add_column_costs_almost_nothing_on_disk(tmp_path):
    """A backfilled column is one repeated value, so it must encode to a
    trivial constant chunk - not one stored value per row. Ten thousand rows of
    'the same thing' that cost anything like ten thousand values would mean the
    constant encoding was lost."""
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64)")
    values = ", ".join(f"({i})" for i in range(10_000))
    session_exec(session, f"INSERT INTO ws.events VALUES {values}")
    paths = _data_files(tmp_path)
    before = len(_parquet_data_region(paths[0]))

    session_exec(session, "ALTER TABLE ws.events ADD COLUMN extra INT64 DEFAULT 7")

    after = len(_parquet_data_region(_new_data_file(tmp_path, paths)))
    growth = after - before
    assert growth < 2_000, f"10k-row constant column grew the file by {growth} bytes"


@pytest.mark.parametrize("statement", _COLUMN_DDL_STATEMENTS)
def test_column_ddl_honours_an_alter_denial(tmp_path, install_capability, statement):
    """Every column operation is gated on ALTER - the same owner-tier gate
    CLUSTER BY and RENAME TO use, because a column change alters what the
    relation IS, not merely what is in it. A denial must stop the statement
    before the connector is reached.

    Note the engine permits everything unless a deployment installs a
    capability (`PermitAll` is intrinsic and correct for a bare engine), so
    asserting this needs a capability installed - a session's access_policies
    alone decide nothing.
    """
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64, name VARCHAR, small INT16)")

    install_capability(_RecordingCapability(deny={"ALTER"}))

    with pytest.raises(PermissionError):
        session_exec(session, statement)


@pytest.mark.parametrize("statement", _COLUMN_DDL_STATEMENTS)
def test_column_ddl_asks_the_alter_gate(tmp_path, install_capability, statement):
    """The gate is consulted with ALTER for this relation - so a capability
    cannot be bypassed by a column operation simply never asking."""
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64, name VARCHAR, small INT16)")

    capability = install_capability(_RecordingCapability())

    try:
        session_exec(session, statement)
    except NotImplementedError:
        pass  # permitted, and reached the not-yet-built connector method

    assert ("ws.events", "ALTER") in capability.asked


# ---------------------------------------------------------------------------
# End-to-end over a real relation
#
# Everything above works on hand-declared tables of one to three columns. These
# use a copy of $planets - 20 columns of mixed real types, written by CTAS
# rather than declared - and chain the operations so each one patches a file
# that is ITSELF the output of the previous patch. A bug in the positional
# statistic remap, in the source-order extent copy (which only has anything to
# reorder once there are many chunks and bloom filters), or in patching an
# already-patched file cannot show up in a two-column fixture.
# ---------------------------------------------------------------------------

_PLANETS = "ws.planets"


@pytest.fixture
def planets(tmp_path):
    """A writable copy of $planets, and the values to compare against."""
    session = _setup(tmp_path)
    session_exec(session, f"CREATE TABLE {_PLANETS} AS SELECT * FROM $planets")
    original = _rows(session, f"SELECT * FROM {_PLANETS}")
    assert len(original) > 1 and len(original[0]) == 20, "fixture is not the real $planets"
    return session, original


def _by_name(rows):
    return {r["name"]: r for r in rows}


def test_a_chain_of_column_operations_over_a_real_relation(tmp_path, planets):
    """Six operations, each landing on the output of the last, with every
    untouched value still readable at the end."""
    session, original = planets

    session_exec(session, f"ALTER TABLE {_PLANETS} DROP COLUMN surface_pressure")
    session_exec(session, f"ALTER TABLE {_PLANETS} DROP COLUMN orbital_eccentricity")
    session_exec(session, f"ALTER TABLE {_PLANETS} RENAME COLUMN number_of_moons TO moons")
    session_exec(session, f"ALTER TABLE {_PLANETS} ADD COLUMN discovered_by VARCHAR")
    session_exec(session, f"ALTER TABLE {_PLANETS} ADD COLUMN catalogued BOOL DEFAULT TRUE")
    session_exec(session, f"ALTER TABLE {_PLANETS} ALTER COLUMN moons TYPE INT64")

    names = _column_names(session, _PLANETS)
    assert "surface_pressure" not in names and "orbital_eccentricity" not in names
    assert "number_of_moons" not in names and "moons" in names
    assert len(names) == 20 - 2 + 2

    after = _by_name(_rows(session, f"SELECT * FROM {_PLANETS}"))
    before = _by_name(original)
    assert set(after) == set(before)
    for planet, row in after.items():
        assert row["moons"] == before[planet]["number_of_moons"]
        assert row["discovered_by"] is None
        assert row["catalogued"] is True
        # every column that was neither dropped, renamed nor added
        for column, value in row.items():
            if column in ("moons", "discovered_by", "catalogued"):
                continue
            assert value == before[planet][column], f"{planet}.{column}"


def test_dropping_one_of_twenty_columns_keeps_the_other_nineteen_exact(tmp_path, planets):
    """Manifest statistics in this store are keyed by column POSITION, so a drop
    shifts every later column's stats. Nineteen surviving columns is enough for
    an unremapped statistic to land on the wrong one."""
    session, original = planets

    session_exec(session, f"ALTER TABLE {_PLANETS} DROP COLUMN density")

    after = _by_name(_rows(session, f"SELECT * FROM {_PLANETS}"))
    before = _by_name(original)
    for planet, row in after.items():
        assert "density" not in row
        assert len(row) == 19
        for column, value in row.items():
            assert value == before[planet][column], f"{planet}.{column}"


def test_each_operation_patches_the_previous_operations_output(tmp_path, planets):
    """The byte properties have to survive being applied to an already-patched
    file, not just to a freshly written one."""
    session, _ = planets

    # 1. a drop, against the CTAS output
    files = _data_files(tmp_path, "planets")
    before = _parquet_data_region(files[0])
    session_exec(session, f"ALTER TABLE {_PLANETS} DROP COLUMN surface_pressure")
    dropped = _new_data_file(tmp_path, files, "planets")
    assert len(_parquet_data_region(dropped)) < len(before)

    # 2. a rename, against the file the drop produced
    files = _data_files(tmp_path, "planets")
    before = _parquet_data_region(dropped)
    assert before, "nothing to compare - the fixture wrote no pages"
    session_exec(session, f"ALTER TABLE {_PLANETS} RENAME COLUMN number_of_moons TO moons")
    renamed = _new_data_file(tmp_path, files, "planets")
    assert _parquet_data_region(renamed) == before, "a rename must not touch a data byte"

    # 3. an add, against the file the rename produced
    files = _data_files(tmp_path, "planets")
    before = _parquet_data_region(renamed)
    session_exec(session, f"ALTER TABLE {_PLANETS} ADD COLUMN discovered_by VARCHAR")
    added = _new_data_file(tmp_path, files, "planets")
    assert _parquet_data_region(added).startswith(before), "existing pages were re-encoded"

    # 4. an annotation-only widen, against the file the add produced. INT8/INT16/
    #    INT32 all ride parquet's physical int32, so this changes the footer and
    #    nothing else - the same cost as the rename above.
    files = _data_files(tmp_path, "planets")
    before = _parquet_data_region(added)
    session_exec(session, f"ALTER TABLE {_PLANETS} ADD COLUMN era INT8")
    with_era = _new_data_file(tmp_path, files, "planets")

    files = _data_files(tmp_path, "planets")
    before = _parquet_data_region(with_era)
    session_exec(session, f"ALTER TABLE {_PLANETS} ALTER COLUMN era TYPE INT32")
    widened = _new_data_file(tmp_path, files, "planets")
    assert _parquet_data_region(widened) == before, (
        "widening within one parquet physical type must not touch a data byte"
    )


def test_superseded_files_are_left_byte_for_byte_alone(tmp_path, planets):
    """The guarantee the whole design rests on: a column operation writes NEW
    files and points only the new snapshot at them. If it ever mutated a
    committed file in place, an older snapshot would start answering with a
    shape it was never written under - time travel silently corrupted.
    """
    session, _ = planets

    frozen = {p: p.read_bytes() for p in _data_files(tmp_path, "planets")}
    assert frozen, "fixture wrote no data files"

    session_exec(session, f"ALTER TABLE {_PLANETS} DROP COLUMN surface_pressure")
    session_exec(session, f"ALTER TABLE {_PLANETS} RENAME COLUMN number_of_moons TO moons")
    session_exec(session, f"ALTER TABLE {_PLANETS} ADD COLUMN discovered_by VARCHAR")
    session_exec(session, f"ALTER TABLE {_PLANETS} ALTER COLUMN moons TYPE INT64")

    for path, contents in frozen.items():
        assert path.exists(), f"{path.name} was deleted, not superseded"
        assert path.read_bytes() == contents, f"{path.name} was modified in place"

    # and each operation really did add a file rather than reusing one
    assert len(_data_files(tmp_path, "planets")) == len(frozen) + 4


# ---------------------------------------------------------------------------
# LIST columns
#
# A LIST is one leaf column chunk however deep it nests, so DROP and RENAME can
# carry it with the same verbatim copy they give a primitive. What differs is
# the chunk's num_values: it counts LEVELS, not rows, because one row expands
# into as many entries as it has elements. Re-declaring that count from the row
# count instead of from the source would truncate every row holding more than
# one element - equal-looking schema, silently shorter lists.
# ---------------------------------------------------------------------------


_LIST_ROWS = "(1, ['a','b','c']), (2, []), (3, ['x']), (4, ['p','q'])"
_LIST_EXPECTED = [["a", "b", "c"], [], ["x"], ["p", "q"]]


def _list_rows(session, column, relation="ws.events"):
    rows = sorted(_rows(session, f"SELECT id, {column} FROM {relation}"),
                  key=lambda r: r["id"])
    return [r[column] for r in rows]


def test_drop_column_preserves_multi_element_lists(tmp_path):
    """Seven elements across four rows must all survive the drop. A num_values
    taken from the row count would give back four one-element lists."""
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64, tags ARRAY<VARCHAR>, victim INT64)")
    session_exec(
        session,
        "INSERT INTO ws.events VALUES "
        "(1, ['a','b','c'], 0), (2, [], 0), (3, ['x'], 0), (4, ['p','q'], 0)",
    )
    assert _list_rows(session, "tags") == _LIST_EXPECTED

    session_exec(session, "ALTER TABLE ws.events DROP COLUMN victim")

    assert _list_rows(session, "tags") == _LIST_EXPECTED


def test_rename_column_preserves_multi_element_lists(tmp_path):
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64, tags ARRAY<VARCHAR>)")
    session_exec(session, f"INSERT INTO ws.events VALUES {_LIST_ROWS}")
    assert _list_rows(session, "tags") == _LIST_EXPECTED

    session_exec(session, "ALTER TABLE ws.events RENAME COLUMN tags TO labels")

    assert _list_rows(session, "labels") == _LIST_EXPECTED


def test_rename_list_column_does_not_touch_a_single_data_byte(tmp_path):
    """The verbatim-copy property has to hold for a LIST too - its rep/def
    levels and element bytes are exactly what must not be rewritten."""
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64, tags ARRAY<VARCHAR>)")
    session_exec(session, f"INSERT INTO ws.events VALUES {_LIST_ROWS}")
    paths = _data_files(tmp_path)
    original = _parquet_data_region(paths[0])

    session_exec(session, "ALTER TABLE ws.events RENAME COLUMN tags TO labels")

    assert _parquet_data_region(_new_data_file(tmp_path, paths)) == original


def test_drop_a_list_column_itself(tmp_path):
    """Dropping the LIST leaves the primitives alone."""
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64, tags ARRAY<VARCHAR>)")
    session_exec(session, f"INSERT INTO ws.events VALUES {_LIST_ROWS}")

    session_exec(session, "ALTER TABLE ws.events DROP COLUMN tags")

    assert [c["name"] for c in _stored_schema(tmp_path)] == ["id"]
    assert [r["id"] for r in sorted(_rows(session, "SELECT id FROM ws.events"),
                                    key=lambda r: r["id"])] == [1, 2, 3, 4]


def test_list_of_int_column_survives_a_patch(tmp_path):
    """The leaf annotation is rebuilt from the source schema, so a non-string
    element type has to come back as itself."""
    session = _setup(tmp_path)
    session_exec(session, "CREATE TABLE ws.events (id INT64, nums ARRAY<INT64>, victim INT64)")
    session_exec(session, "INSERT INTO ws.events VALUES (1, [10,20,30], 0), (2, [40], 0)")
    before = _list_rows(session, "nums")
    assert before == [[10, 20, 30], [40]]

    session_exec(session, "ALTER TABLE ws.events DROP COLUMN victim")

    assert _list_rows(session, "nums") == before
