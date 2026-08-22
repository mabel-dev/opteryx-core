"""
Reference-catalog-driven SQL generator for the single-table SELECT fuzzer.

WHY THE CATALOG DRIVES THIS
---------------------------
`reference/` is the generated snapshot of what the engine *claims* to support:
`function_signatures.json` carries every function's arity, parameter types,
which parameters must be literals, and the return type; `operators.json` and
`unary_ops.json` carry the operator set; `aggregates.json` carries which
aggregates work globally and which need a GROUP BY. A fuzzer built on a
hand-written list of "SQL I remember Opteryx supporting" drifts silently: new
functions never get fuzzed, removed ones generate noise, and nobody notices
either. Generating from `reference/` means the fuzzer's reach tracks the
engine's claims automatically, and `EXCLUSIONS` below is a *visible* ledger of
everything in the catalog this generator deliberately does not emit.

THE WELL-TYPED CONTRACT
-----------------------
Everything this module emits is intended to execute. That is a deliberate
choice, and it is what makes "did not raise" a real assertion instead of
decoration: if the generator only produced SQL it believed to be valid, then
*any* exception is a finding — either an engine defect or a `reference/`
inaccuracy, and both are worth knowing about. The alternative (emit anything,
classify errors into expected/unexpected) needs an allowlist of "errors that
are fine", and an allowlist of fine errors is exactly how a regression that
turns a working query into `IncompatibleTypesError` gets absorbed silently.

Generation is therefore type-directed: every expression is built to a requested
type from the types actually present in the chosen relation, and literals are
minted to match. Where the engine and the catalog disagree today, the construct
sits in `single_table_known_gaps.REGISTER` — a register, not a silence: a test asserts each
entry still fails, so a fix turns the register red and forces its removal.
"""

from __future__ import annotations

import datetime
import json
import random
from dataclasses import dataclass
from dataclasses import field
from enum import Enum
from pathlib import Path
from typing import Dict
from typing import List
from typing import Optional
from typing import Sequence
from typing import Set
from typing import Tuple

_REFERENCE_DIR = Path(__file__).resolve().parents[2] / "reference"


# ─────────────────────────────────────────────────────────────────────────────
# Type lattice
# ─────────────────────────────────────────────────────────────────────────────


class Ty(Enum):
    """The generator's type vocabulary.

    Deliberately coarser than `DrakenType` and coarser than `LogicalType`: the
    generator only needs to answer "may this expression be an operand here?",
    and the catalog's own parameter vocabulary (`integer`, `number`, `varchar`,
    `temporal`, `boolean`, `array`, `any`) is at this granularity too.

    `UNKNOWN` is not "any type" — it is "a type this generator cannot reason
    about", produced by catalog return types like `dynamic` or
    `integer | double | date`. An UNKNOWN expression may appear in a SELECT list
    and nowhere else, because anywhere else needs a type to check against.
    """

    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    DECIMAL = "DECIMAL"
    VARCHAR = "VARCHAR"
    VARBINARY = "VARBINARY"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    ARRAY = "ARRAY"
    UNKNOWN = "UNKNOWN"


NUMERIC: Tuple[Ty, ...] = (Ty.INTEGER, Ty.FLOAT, Ty.DECIMAL)
TEMPORAL: Tuple[Ty, ...] = (Ty.DATE, Ty.TIMESTAMP)
# Types the engine will sort, group, DISTINCT or compare. ARRAY is excluded
# because the native engine rejects it as an ORDER BY / GROUP BY / DISTINCT key
# outright (`ORDER BY on column 'x' (DrakenType.ARRAY) is not supported yet`),
# and `types.json` records that ARRAY has no `=` operator at all.
SCALAR: Tuple[Ty, ...] = (
    Ty.INTEGER,
    Ty.FLOAT,
    Ty.DECIMAL,
    Ty.VARCHAR,
    Ty.VARBINARY,
    Ty.BOOLEAN,
    Ty.DATE,
    Ty.TIMESTAMP,
)

# DrakenType name -> generator type. Keyed by name so this module does not have
# to import draken; the fuzzer asks the engine for a relation's schema and reads
# the enum's name off it.
_DRAKEN_TO_TY: Dict[str, Ty] = {
    "INT64": Ty.INTEGER,
    "INT32": Ty.INTEGER,
    "UINT64": Ty.INTEGER,
    "FLOAT64": Ty.FLOAT,
    "FLOAT32": Ty.FLOAT,
    "DECIMAL": Ty.DECIMAL,
    "VARCHAR": Ty.VARCHAR,
    "NVARCHAR": Ty.VARCHAR,
    "VARBINARY": Ty.VARBINARY,
    "BOOL": Ty.BOOLEAN,
    "BOOLEAN": Ty.BOOLEAN,
    "DATE32": Ty.DATE,
    "TIMESTAMP64": Ty.TIMESTAMP,
    "ARRAY": Ty.ARRAY,
}

# Catalog parameter-type name -> the generator types that satisfy it.
_PARAM_TO_TYPES: Dict[str, Tuple[Ty, ...]] = {
    "integer": (Ty.INTEGER,),
    "number": NUMERIC,
    "varchar": (Ty.VARCHAR,),
    # CONCAT/CONCAT_WS declare one overload per string type, so these two labels
    # appear alongside `varchar`. `nvarchar` has no satisfying value — Ty has no
    # NVARCHAR member and no relation in the corpus carries one — so overloads
    # using it are skipped, exactly like `vector` below.
    "nvarchar": (),
    "varbinary": (Ty.VARBINARY,),
    "boolean": (Ty.BOOLEAN,),
    "temporal": TEMPORAL,
    "array": (Ty.ARRAY,),
    "any": SCALAR,
    # `vector` has no satisfying value: no relation carries a VECTOR column and
    # the only vector-producing function (EMBED) needs a model. Functions with a
    # vector parameter are excluded wholesale — see EXCLUSIONS.
    "vector": (),
}

# Catalog return-type string -> generator type. `None` marks a polymorphic
# return resolved from the argument list at call-construction time.
_RETURN_TO_TY: Dict[str, Optional[Ty]] = {
    "BOOLEAN": Ty.BOOLEAN,
    "DATE": Ty.DATE,
    "FLOAT": Ty.FLOAT,
    "INTEGER": Ty.INTEGER,
    "TIMESTAMP[US]": Ty.TIMESTAMP,
    "VARBINARY": Ty.VARBINARY,
    "VARCHAR": Ty.VARCHAR,
    "ARRAY<VARIANT>": Ty.ARRAY,
    "ARRAY<INT64>": Ty.ARRAY,
    "array<element type of `string`>": Ty.ARRAY,
    "same as `arr`": Ty.ARRAY,
    # Resolved from argument 0.
    "same as `num`": None,
    "same as `value`": None,
    "compatible input type": None,
    # Genuinely not knowable from the signature.
    "IPV4": Ty.UNKNOWN,
    "TIME[US]": Ty.UNKNOWN,
    "dynamic": Ty.UNKNOWN,
    "element type of `arr`": Ty.UNKNOWN,
    # NOTE: lower-case `integer` is EXTRACT's return override, and it is a second
    # spelling of the "INTEGER" label above rather than a different type. Both are
    # mapped so this table matches the catalog as it stands; the two spellings of
    # one type are worth collapsing at the source (reference/signatures.py's
    # _RETURN_OVERRIDES), which is not this file's call to make.
    "integer": Ty.INTEGER,
    "vector": Ty.UNKNOWN,
}


# ─────────────────────────────────────────────────────────────────────────────
# The exclusion ledger
# ─────────────────────────────────────────────────────────────────────────────

# Catalog entries this generator does not emit, each with the reason. Anything
# in `reference/` that is neither generated nor listed here is drift, and
# `test_catalog_coverage_is_accounted_for` fails on it. The point is that
# shrinking the fuzzer's reach has to be a visible, argued act.
EXCLUSIONS: Dict[str, str] = {
    # ── Not deterministic across two executions of the same SQL ──────────────
    # Every oracle here compares two executions. A function whose value changes
    # between them makes the comparison meaningless, not merely noisy.
    "RANDOM": "volatile: differs between the two executions every oracle compares",
    "RAND": "alias of RANDOM",
    "NORMAL": "volatile: differs between the two executions every oracle compares",
    "RANDOM_STRING": "volatile: differs between the two executions every oracle compares",
    "MATCH": "catalog volatility=stable; not immutable, so not safe for differential oracles",
    # The catalog marks these `immutable`, which is wrong for a differential
    # oracle's purposes — they are constant *within* a query but vary *between*
    # queries, and the oracles run the same logical query twice. Reported to the
    # architect as a `reference/` accuracy issue rather than worked around
    # silently.
    "CURRENT_DATE": "clock-dependent: constant within a query, differs between the oracle's two runs",
    "CURRENT_TIME": "clock-dependent: constant within a query, differs between the oracle's two runs",
    "CURRENT_TIMESTAMP": "clock-dependent: constant within a query, differs between the oracle's two runs",
    "NOW": "alias of CURRENT_TIMESTAMP",
    "UTC_TIMESTAMP": "clock-dependent: constant within a query, differs between the oracle's two runs",
    # ── Session/deployment identity, not data ────────────────────────────────
    "CONNECTION_ID": "session identity, not a data function",
    "DATABASE": "session identity, not a data function",
    "USER": "session identity, not a data function",
    # ── No satisfying argument exists in any fuzzed relation ─────────────────
    "EMBED": "returns VECTOR and needs an embedding model; no VECTOR value is constructible here",
    "COSINE_SIMILARITY": "VECTOR parameters; no VECTOR column or literal exists in the corpus",
    "COSINE_DISTANCE": "VECTOR parameters; no VECTOR column or literal exists in the corpus",
    # ── Operators declared in operators.json but not emitted ─────────────────
    # `ShiftLeft`/`ShiftRight` are gone because they now WORK — the dialect
    # gained the infix parse and _bitwise emits them. `AtQuestion` is gone for
    # the same reason (draken_json_path_exists), and stays unemitted for the
    # reason JSONB_OBJECT_KEYS is: its left operand must be a JSON document, and
    # this generator cannot mint one — the same `value_format: "json"` wall.
    "IPContains": "no IPV4 column in the corpus; the operator needs an IPV4 operand",
    "IPContainedBy": "no IPV4 column in the corpus; the operator needs an IPV4 operand",
    "MapAccess": "STRUCT subscript; no STRUCT-typed column in the fuzzed relations",
    "Xor": "boolean XOR is generated as a top-level connective only, never over a literal pair",
    # ── Aggregates ───────────────────────────────────────────────────────────
    "APPROX_COUNT_DISTINCT": "approximate: no exact identity to assert it against",
    "APPROX_PERCENTILE": "approximate: no exact identity to assert it against",
    "CORR": "two-column aggregate whose float result is not stable enough for multiset equality",
    "STDDEV": "float accumulation order is not fixed, so multiset equality across plans is not sound",
    # ANY_VALUE is not listed: aggregates.json now records `deterministic: false`
    # for it, and _load_aggregates drops non-deterministic aggregates on that
    # flag. Every oracle here compares two executions, so a value that may
    # legitimately differ between them is not fuzzable — but that is now the
    # catalog's statement about the engine, not this file's.
    "ARRAY_AGG": (
        "excluded when the per-group element cap (ARRAY_AGG_MAX_VALUES_PER_GROUP=1000) made it "
        "trip on the corpus's skewed relations. That cap is gone — the guard is now a 512MB "
        "global byte budget, which this corpus cannot reach — so the original reason no longer "
        "holds. Re-enabling it is a fuzz-scope decision for the architect, not a silent flip"
    ),
}

# Entries that used to live in EXCLUSIONS and are now read from `reference/`
# instead, because the catalog gained a field that can state them:
#
#   (element_of)                   the probe must be of the array's element
#                                  type. No registered function declares this
#                                  today — the containment tests are OPERATORS
#                                  (`= ANY`, `@>`, `@>>`), generated by
#                                  _array_predicate, which knows the corpus's
#                                  element types. The rule stays in the catalog
#                                  for the next function that needs it.
#   JSONB_OBJECT_KEYS              parameter `value_format: "json"`.
#   BASE64/BASE85/HEX_DECODE       parameter `value_format`, ditto. Still emitted
#                                  as the outer half of a DECODE(ENCODE(x)) round
#                                  trip, which asserts the identity.
#   REGEXP_REPLACE                 parameter `value_format: "dfa-regex"` on the
#                                  pattern and `domain: ["\\1"]` on the
#                                  replacement — only whole-match capture
#                                  extraction is implemented.
#   ANY_VALUE                      aggregate `deterministic: false`.
#
# A `value_format` this generator cannot mint drops the overload in
# `_load_function_overloads`; `element_of` does the same, because a probe whose
# type must match an array's element type cannot be built type-directed.

# Constructs the engine rejects today are recorded in
# `single_table_known_gaps.REGISTER`, with a minimal repro that a test requires
# to keep failing. Comments below reference those entries by id.

# ─────────────────────────────────────────────────────────────────────────────
# Catalog loading
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Param:
    accepts: Tuple[Ty, ...]
    constant_only: bool
    #: The complete set of legal values, from the catalog's `domain`. Empty means
    #: the parameter is not an enumeration.
    domain: Tuple[str, ...] = ()
    #: Inclusive value bounds, from the catalog's `minimum` / `maximum`.
    minimum: Optional[float] = None
    maximum: Optional[float] = None


@dataclass(frozen=True)
class Overload:
    """One callable function signature, resolved into generator types."""

    name: str
    params: Tuple[Param, ...]
    returns: Optional[Ty]  # None = resolve from argument 0
    minimum_arity: int
    #: The catalog's `homogeneous`: every `any`-typed parameter must resolve to
    #: one shared type. Replaces a hardcoded list of COALESCE/IFNULL/IFNOTNULL/
    #: NULLIF — the rule was always real, it just was not in `reference/`.
    homogeneous: bool = False


# Parameter-type exclusions the CATALOG should carry but does not yet, because
# whether the function accepts the type is an open question rather than a
# settled no. Each must name a live register entry; `test_pending_exclusion_
# cites_a_live_register_entry` fails when the entry goes, which forces the
# decision back into the open instead of leaving a silent narrowing behind.
#
#: function name -> (types withheld, id of the register entry that justifies it)
_PENDING_EXCLUSIONS: Dict[str, Tuple[Tuple["Ty", ...], str]] = {}


# Canonical types.json spellings (the vocabulary of a parameter's `excludes`)
# mapped to the generator types they rule out.
_EXCLUDED_SPELLING_TO_TYPES: Dict[str, Tuple[Ty, ...]] = {
    "ARRAY": (Ty.ARRAY,),
    "BOOLEAN": (Ty.BOOLEAN,),
    "DATE": (Ty.DATE,),
    "DECIMAL": (Ty.DECIMAL,),
    "FLOAT": (Ty.FLOAT,),
    "INTEGER": (Ty.INTEGER,),
    "NVARCHAR": (Ty.VARCHAR,),
    "TIMESTAMP": (Ty.TIMESTAMP,),
    "VARBINARY": (Ty.VARBINARY,),
    "VARCHAR": (Ty.VARCHAR,),
}

# `value_format`s this generator cannot mint a satisfying value for. An overload
# with one of these on any parameter is not emitted as a standalone call: a
# type-directed generator would hand it an arbitrary string, which is exactly the
# call that binds and then dies inside the kernel.
#
# "base64"/"base85"/"hex" are still reached through _CODEC_ROUND_TRIPS, which
# feeds each decoder its own encoder's output — a stronger test than a random
# string. "json" and "dfa-regex" have no such round trip.
_UNSATISFIABLE_VALUE_FORMATS = frozenset({"base64", "base85", "hex", "json", "dfa-regex"})

# DECODE(ENCODE(x)) round trips. Emitted as a pair because the decoders raise on
# anything that is not valid in their encoding, and this shape asserts something
# real: the round trip must be the identity.
_CODEC_ROUND_TRIPS: Tuple[Tuple[str, str], ...] = (
    ("BASE64_ENCODE", "BASE64_DECODE"),
    ("BASE85_ENCODE", "BASE85_DECODE"),
    ("HEX_ENCODE", "HEX_DECODE"),
)

# The character SET for SQL-92's `TRIM([BOTH|LEADING|TRAILING] <chars> FROM str)`.
#
# A SET, not a substring — `TRIM(BOTH 'ab' FROM 'baXab')` is 'X' — so the
# multi-character entries are the ones carrying the semantic, and the single
# characters are the degenerate case that must not regress.
#
# ASCII only, and deliberately: VARCHAR is ASCII bytes and non-ASCII content in
# one is undefined (RATIFIED/varchar-is-ascii-bytes-and-non-ascii-content-is-
# undefined), so a non-ASCII set over a VARCHAR corpus would be asking a question
# the type does not answer. The kernel's codepoint scan is the NVARCHAR path, and
# this generator has no NVARCHAR relation to point it at.
#
# No quote and no backslash: these are interpolated straight into a SQL literal.
_TRIM_CHARACTER_SETS: Tuple[str, ...] = ("a", "e", " ", "0", "-", "ab", "aeiou", "xyz", " -0")


# Functions the CATALOG itself rules out, name -> the catalog fact that did it.
# Populated by `_load_function_overloads`. Distinct from `EXCLUSIONS` on purpose:
# an entry here needs no argument from anybody, because `reference/` already says
# why. `test_catalog_coverage_is_accounted_for` counts both, so a function that
# is neither generated, nor declined by the catalog, nor argued for in EXCLUSIONS
# is still drift.
CATALOG_DECLINED: Dict[str, str] = {}


def _load_json(name: str) -> dict:
    with (_REFERENCE_DIR / name).open() as handle:
        return json.load(handle)


def _load_function_overloads() -> List[Overload]:
    """Every scalar function overload this generator is willing to emit.

    Drops, in order: catalog names in `EXCLUSIONS`; non-immutable or
    non-deterministic entries; overloads with a parameter no value in the corpus
    can satisfy; overloads whose parameters carry a `value_format` or an
    `element_of` link this generator cannot honour. Every one of those is the
    catalog's own statement — there is no private list of "functions we skip".
    """
    catalog = _load_json("function_signatures.json")
    overloads: List[Overload] = []
    for name, entry in catalog.items():
        if name in EXCLUSIONS:
            continue
        if entry["volatility"] != "immutable" or not entry["deterministic"]:
            CATALOG_DECLINED.setdefault(
                name,
                f"catalog volatility={entry['volatility']}, deterministic={entry['deterministic']}",
            )
            continue
        for overload in entry["overloads"]:
            params: List[Param] = []
            usable = True
            for spec in overload["parameters"]:
                if spec["value_format"] in _UNSATISFIABLE_VALUE_FORMATS:
                    # A value this generator cannot mint: JSON text, base85, a
                    # DFA-compilable regex. Emitting the call anyway is how
                    # `JSONB_OBJECT_KEYS('delta')` got generated.
                    CATALOG_DECLINED.setdefault(
                        name,
                        f"parameter `{spec['label']}` declares value_format="
                        f"'{spec['value_format']}', which this generator cannot mint",
                    )
                    usable = False
                    break
                if spec["element_of"] is not None:
                    # The parameter's type is pinned to another parameter's ARRAY
                    # ELEMENT type, and no schema the engine exposes records that.
                    # No registered function declares `element_of` today (the
                    # containment tests are operators, generated by
                    # _array_predicate) — this arm is kept for the next one.
                    CATALOG_DECLINED.setdefault(
                        name,
                        f"parameter `{spec['label']}` must be of `{spec['element_of']}`'s ARRAY "
                        "ELEMENT type, which no schema the engine exposes records; generated by "
                        "_array_predicate instead",
                    )
                    usable = False
                    break
                accepts = _PARAM_TO_TYPES.get(spec["type"])
                if not accepts:
                    CATALOG_DECLINED.setdefault(
                        name,
                        f"parameter `{spec['label']}` is `{spec['type']}`, which no value in "
                        "the corpus satisfies",
                    )
                    usable = False
                    break
                for spelling in spec["excludes"]:
                    barred = _EXCLUDED_SPELLING_TO_TYPES.get(spelling, ())
                    accepts = tuple(ty for ty in accepts if ty not in barred)
                pending, _ = _PENDING_EXCLUSIONS.get(name, ((), ""))
                for barred in pending:
                    accepts = tuple(ty for ty in accepts if ty is not barred)
                if not accepts:
                    CATALOG_DECLINED.setdefault(
                        name,
                        f"parameter `{spec['label']}` excludes every type its family admits",
                    )
                    usable = False
                    break
                # A variadic tail is emitted at the overload's minimum arity and
                # no further: fuzzing arity is a separate axis from fuzzing
                # types, and an over-long variadic call would only ever test the
                # binder's arity check.
                if spec["variadic"] or spec["optional"]:
                    continue
                params.append(
                    Param(
                        accepts=accepts,
                        constant_only=spec["constant_only"],
                        domain=tuple(spec["domain"]),
                        minimum=spec["minimum"],
                        maximum=spec["maximum"],
                    )
                )
            if not usable:
                continue
            minimum = overload["arity"]["minimum"]
            # A declared minimum can exceed the fixed-parameter list when the
            # tail is variadic. Pad from the last fixed parameter so the call is
            # emitted at its real minimum.
            while len(params) < minimum and params:
                params.append(params[-1])
            overloads.append(
                Overload(
                    name=name,
                    params=tuple(params),
                    returns=_RETURN_TO_TY[overload["return_type"]],
                    minimum_arity=minimum,
                    homogeneous=overload["homogeneous"],
                )
            )
    if not overloads:
        raise AssertionError("no callable function overloads loaded from reference/")
    return overloads


def _load_aggregates() -> Tuple[List[str], List[str]]:
    """(usable globally, usable only with GROUP BY)."""
    catalog = _load_json("aggregates.json")
    global_ok: List[str] = []
    grouped_only: List[str] = []
    for name, entry in catalog.items():
        if name in EXCLUSIONS:
            continue
        # Every oracle here compares two executions of the same logical query, so
        # an aggregate whose answer may legitimately differ between them cannot
        # be asserted against. The catalog now says which those are.
        if not entry["deterministic"]:
            continue
        if entry["support"]["global"]:
            global_ok.append(name)
        elif entry["support"]["grouped"]:
            grouped_only.append(name)
    if not global_ok:
        raise AssertionError("no global aggregates loaded from reference/")
    return global_ok, grouped_only


def _load_aggregate_input_types() -> Dict[str, Tuple[Ty, ...]]:
    """Aggregate name -> the generator types its first parameter accepts.

    Was a hardcoded table here, with a comment saying the catalog recorded SQL
    forms and no parameter types. It does now: `parameters[].type` plus
    `excludes`, the same vocabulary function_signatures.json uses.
    """
    catalog = _load_json("aggregates.json")
    accepted: Dict[str, Tuple[Ty, ...]] = {}
    for name, entry in catalog.items():
        parameters = entry["parameters"]
        if not parameters:
            continue
        spec = parameters[0]
        types = _PARAM_TO_TYPES.get(spec["type"], ())
        for spelling in spec["excludes"]:
            barred = _EXCLUDED_SPELLING_TO_TYPES.get(spelling, ())
            types = tuple(ty for ty in types if ty not in barred)
        accepted[name] = types
    return accepted


FUNCTION_OVERLOADS: List[Overload] = _load_function_overloads()
GLOBAL_AGGREGATES, GROUPED_ONLY_AGGREGATES = _load_aggregates()

# There was an UNIMPLEMENTED_OPERATORS dict here, built from operators.json's
# `implemented: false`. It is gone: `@?` was its last entry, and `@?` now has a
# kernel, so the dict was empty — and it had never had a consumer. Nothing read
# it, so it excluded nothing; the exclusion it appeared to express was doing no
# work. An empty ledger nobody consults is worse than no ledger, because it reads
# like a gate. If an operator is ever genuinely unrunnable again, exclude it in
# EXCLUSIONS above, where the reason is visible and something acts on it.

# Overloads indexed by the type they return, so "build me a VARCHAR" is a
# lookup rather than a rejection loop.
_OVERLOADS_BY_RETURN: Dict[Ty, List[Overload]] = {}
for _overload in FUNCTION_OVERLOADS:
    _returns = _overload.returns
    if _returns is None:
        # Polymorphic in argument 0: reachable for every type argument 0 accepts.
        _targets = _overload.params[0].accepts if _overload.params else ()
    else:
        _targets = (_returns,)
    for _target in _targets:
        _OVERLOADS_BY_RETURN.setdefault(_target, []).append(_overload)


# Aggregates restricted by input type — read from aggregates.json's `parameters`,
# which now records them (MEDIAN's DECIMAL exclusion included, as an explicit
# `excludes`, rather than as a contradiction of types.json's numeric family left
# for the reader to find).
_AGGREGATE_INPUT_TYPES: Dict[str, Tuple[Ty, ...]] = _load_aggregate_input_types()

# `COUNT(DISTINCT x)` is supported; `SUM(DISTINCT x)` is not
# ("native engine: SUM(DISTINCT ...) is not supported yet").
_DISTINCT_CAPABLE_AGGREGATES = frozenset({"COUNT"})
# Columns of the corpus that carry NaN. `dev/generate_fuzz_testdata.py` keeps the
# float specials in dedicated columns precisely so a query can choose whether to
# touch them; this is the reader's side of that arrangement.
NAN_BEARING_COLUMNS = frozenset({"f_special", "f_special_null", "val_special"})

#: aggregate -> the register entry that stops it being generated over NaN.
#: test_nan_withholding_cites_a_live_register_entry fails when the entry goes.
_AGGREGATES_WITHHELD_FROM_NAN: Dict[str, str] = {}

_AGGREGATE_RETURNS: Dict[str, Optional[Ty]] = {
    "SUM": None,  # same as input
    "AVG": Ty.FLOAT,
    "MEDIAN": Ty.FLOAT,
    "MIN": None,
    "MAX": None,
    "COUNT": Ty.INTEGER,
    "COUNT_DISTINCT": Ty.INTEGER,
    "ANY_VALUE": None,
    "ARRAY_AGG": Ty.ARRAY,
}

# CAST targets, from types.json's canonical spellings. Only pairs the engine
# actually implements: the map is source type -> target spellings.
CAST_TARGETS: Dict[Ty, Tuple[str, ...]] = {
    # INTEGER -> TIMESTAMP is omitted: it needs an explicit unit
    # ("Ambiguous cast: INTEGER → TIMESTAMP requires a unit"), which is a
    # widthed spelling this generator does not mint.
    Ty.INTEGER: ("VARCHAR", "FLOAT", "BOOLEAN"),
    Ty.FLOAT: ("VARCHAR", "INTEGER", "BOOLEAN"),
    Ty.DECIMAL: ("VARCHAR", "FLOAT", "INTEGER"),
    Ty.VARCHAR: ("VARBINARY",),
    # VARBINARY -> VARCHAR is omitted, and permanently: VARCHAR is ASCII bytes and
    # non-ASCII content in one is undefined behaviour, so casting the corpus's
    # arbitrary binary columns to VARCHAR produces a string this generator has no
    # grounds to assert anything about (single_table_known_gaps/RATIFIED/
    # varchar-is-ascii-bytes-and-non-ascii-content-is-undefined). The reverse
    # direction, VARCHAR -> VARBINARY, is always well defined.
    Ty.VARBINARY: (),
    Ty.BOOLEAN: ("VARCHAR", "INTEGER"),
    Ty.DATE: ("TIMESTAMP", "VARCHAR"),
    Ty.TIMESTAMP: ("VARCHAR", "DATE"),
}
_CAST_TARGET_TY: Dict[str, Ty] = {
    "VARCHAR": Ty.VARCHAR,
    "VARBINARY": Ty.VARBINARY,
    "FLOAT": Ty.FLOAT,
    "INTEGER": Ty.INTEGER,
    "BOOLEAN": Ty.BOOLEAN,
    "TIMESTAMP": Ty.TIMESTAMP,
    "DATE": Ty.DATE,
}


# ─────────────────────────────────────────────────────────────────────────────
# Relations and expressions
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Column:
    name: str
    ty: Ty

    @property
    def quoted(self) -> str:
        return f'"{self.name}"'


@dataclass(frozen=True)
class Relation:
    """A FROM source and the columns it exposes."""

    sql: str  # what goes after FROM
    columns: Tuple[Column, ...]

    def of(self, *types: Ty) -> List[Column]:
        wanted = set(types)
        return [column for column in self.columns if column.ty in wanted]


@dataclass(frozen=True)
class Expr:
    sql: str
    ty: Ty


# Interval units accepted by the temporal arithmetic path.
_INTERVAL_UNITS = ("DAY", "HOUR", "MINUTE", "SECOND", "MONTH", "YEAR")

# Date parts are no longer listed here: every part-taking parameter carries its
# own `domain` in function_signatures.json, and they are NOT the same set —
# DATEDIFF takes millisecond and microsecond, TRUNC and TIME_BUCKET do not, and
# EXTRACT takes neither those nor `week`.
#
# The one coupling a per-parameter domain cannot express: EXTRACT's sub-day parts
# need a TIMESTAMP operand (draken_date_part refuses "sub-day part of a DATE").
# The catalog states it in that parameter's documentation; _narrow_for_drawn_constants
# applies it.
_SUB_DAY_PARTS = frozenset({"hour", "minute", "second", "millisecond", "microsecond"})

_COMPARISONS = ("=", "!=", "<>", "<", "<=", ">", ">=")
_EQUALITY = ("=", "!=", "<>")

# EXTRACT's part domain, read from the catalog rather than restated: the four
# date-part-taking functions accept four DIFFERENT sets, and EXTRACT's is the
# narrowest (no week, no millisecond, no microsecond). Read once at import so the
# SQL-92 infix spelling below cannot drift from the call spelling above.
_EXTRACT_PARTS: Tuple[str, ...] = tuple(
    _load_json("function_signatures.json")["EXTRACT"]["overloads"][0]["parameters"][0]["domain"]
)
if not _EXTRACT_PARTS:
    raise AssertionError("reference/ records no `part` domain for EXTRACT")

# Functions whose trailing integer arguments are precision/scale/length/position
# rather than data. An unbounded literal there is legal and useless.
_SMALL_INTEGER_ARGUMENT_FUNCTIONS = frozenset(
    {"ROUND", "TRUNC", "LEFT", "RIGHT", "LPAD", "RPAD", "SUBSTRING", "SPLIT", "IP_TRUNC"}
)

# TIME_BUCKET's `magnitude` is a bucket WIDTH. The catalog bounds it below (>= 1)
# but not above, and correctly so — there is no constant ceiling, because what
# overflows depends on the unit and on the data. What DOES bound it is the
# TIMESTAMP type: `TIME_BUCKET(999999999, 'year', ts)` computes year 173949 and
# raises `year must be in 1..9999` (types.json records that window on the type).
# A width of up to 12 units is what a caller writes; the overflow itself is
# registered as from-unixtime-out-of-range, which shares the message.
_BUCKET_WIDTH_FUNCTIONS = frozenset({"TIME_BUCKET"})

# Element types of the ARRAY columns in the corpus. The schema the engine
# reports collapses every list to `DrakenType.ARRAY`, so the element type is not
# discoverable from it — this is knowledge about the test data, stated where it
# can be checked, not a claim about the engine. A column absent here simply does
# not get an ARRAY predicate generated against it.
ARRAY_ELEMENT_TYPES: Dict[str, Ty] = {
    "arr_int": Ty.INTEGER,  # testdata.fuzzing.mixed
    "arr_str": Ty.VARCHAR,  # testdata.fuzzing.mixed
    "alma_mater": Ty.VARCHAR,  # testdata.astronauts
    "missions": Ty.VARCHAR,  # testdata.astronauts
}


class Names:
    """Mints output aliases that are unique across an entire statement.

    Per-SELECT counters are not enough. A subquery that exposes `w0` under an
    outer SELECT that also aliases something `w0` produces
    `AmbiguousIdentifierError`, and a CTE exposing `a0` under an outer
    `MEDIAN(a0) AS a0` produces the same. Both are generator faults that look
    exactly like binder bugs in a failure report, so they have to be impossible
    rather than rare.
    """

    def __init__(self) -> None:
        self._counter = 0

    #: Every generated alias carries this prefix. It is not decoration: the
    #: defect register matches some entries on "Unknown column '<alias>'", and a
    #: bare `a1`/`e3` namespace would let those signatures also match a real
    #: column (`albedo`, `escape_velocity`). `oz_` cannot collide with anything
    #: in the corpus, so an alias-scoped signature stays alias-scoped.
    PREFIX = "oz_"

    def next(self, prefix: str) -> str:
        self._counter += 1
        return f"{self.PREFIX}{prefix}{self._counter}"


class Generator:
    """Builds one query. One instance per fuzz case, holding that case's RNG."""

    def __init__(self, rng: random.Random, relation: Relation, names: Names) -> None:
        self.rng = rng
        self.relation = relation
        self.names = names
        self.tags: Set[str] = set()
        # True while building a predicate that is an *operand* (a CASE/IIF
        # condition) rather than a WHERE clause. RLIKE evaluates correctly as a
        # WHERE predicate and as a bare projection, but fails inside CASE — see
        # single_table_known_gaps/rlike-outside-top-level-predicate-position.
        self._predicate_is_an_operand = False

    # ── literals ─────────────────────────────────────────────────────────────

    def literal(self, ty: Ty) -> str:
        """A literal of `ty`, spelled the way the binder requires.

        Temporal literals carry an explicit cast. Opteryx does not implicitly
        coerce a string to a temporal column type — `dt_col > '2000-01-01'`
        raises IncompatibleTypesError — so an uncast temporal literal would send
        every temporal predicate into the binder's error path instead of the
        executor.
        """
        rng = self.rng
        if ty is Ty.INTEGER:
            return str(rng.randint(-1_000_000, 1_000_000))
        if ty is Ty.FLOAT:
            return f"{rng.uniform(-1_000_000, 1_000_000):.6f}"
        if ty is Ty.DECIMAL:
            # Quarters: exactly representable in DECIMAL(18,4) and in FLOAT64,
            # so a DECIMAL/FLOAT comparison is not decided by binary rounding.
            return f"{rng.randint(-4_000_000, 4_000_000) / 4:.4f}"
        if ty is Ty.VARCHAR:
            return "'" + rng.choice(_STRING_LITERALS) + "'"
        if ty is Ty.VARBINARY:
            return "b'" + rng.choice(_STRING_LITERALS) + "'"
        if ty is Ty.BOOLEAN:
            return rng.choice(("TRUE", "FALSE"))
        if ty is Ty.DATE:
            day = _REFERENCE_DATE + datetime.timedelta(days=rng.randint(-20_000, 20_000))
            return f"'{day.date()}'::DATE"
        if ty is Ty.TIMESTAMP:
            moment = _REFERENCE_DATE + datetime.timedelta(seconds=rng.randint(-1_000_000_000, 1_000_000_000))
            return f"'{moment}'::TIMESTAMP"
        raise AssertionError(f"no literal form for {ty}")

    def like_pattern(self) -> str:
        """A LIKE pattern with real metacharacters in it.

        A pattern of pure random characters matches nothing, and a predicate
        that matches nothing exercises the filter's empty path and nothing else.
        These are built from substrings that occur in the corpus.
        """
        rng = self.rng
        stem = rng.choice(_STRING_LITERALS)
        shape = rng.random()
        if shape < 0.35:
            return f"'%{stem}%'"
        if shape < 0.6:
            return f"'{stem[:2]}%'"
        if shape < 0.8:
            return f"'%{stem[-2:]}'"
        return f"'{stem[:1]}_%'"

    # ── expressions ──────────────────────────────────────────────────────────

    def can_produce(self, ty: Ty) -> bool:
        """Whether a value of `ty` is constructible over the current relation.

        ARRAY has no literal spelling that is valid in every position — types.json
        records that `[1, 2, 3]` is an operand of IN / `@>` / CAST and nothing
        else — so an ARRAY expression can only be a column. A relation with no
        ARRAY column therefore cannot satisfy an `array` parameter at all, and
        functions taking one are not chosen against it.
        """
        if ty is Ty.ARRAY:
            return bool(self.relation.of(Ty.ARRAY))
        return True

    def expression(self, ty: Ty, depth: int = 0) -> Expr:
        """A scalar expression of exactly `ty` over the current relation."""
        rng = self.rng
        columns = self.relation.of(ty)

        if ty is Ty.ARRAY:
            # ARRAY is handled entirely here and never falls through, because
            # every path below can end at `literal(ty)` and there is no ARRAY
            # literal that is valid in an arbitrary position (types.json: an
            # array literal is an operand of IN / `@>` / CAST and nothing else).
            if not columns:
                raise AssertionError(
                    "an ARRAY expression was requested over a relation with no ARRAY column; "
                    "can_produce() should have excluded this call site"
                )
            column = Expr(rng.choice(columns).quoted, Ty.ARRAY)
            if depth >= 2 or rng.random() < 0.6:
                return column
            return self._function_call(ty, depth) or column

        # Past the depth budget, or with no deeper form available, fall back to
        # a column or a literal — both are always well-typed.
        if depth >= 2 or rng.random() < 0.45:
            if columns and rng.random() < 0.75:
                return Expr(rng.choice(columns).quoted, ty)
            return Expr(self.literal(ty), ty)

        builders = []
        if ty in NUMERIC:
            builders.append(self._arithmetic)
        if ty is Ty.INTEGER:
            builders.append(self._bitwise)
        if ty is Ty.VARCHAR:
            builders.append(self._string_concat)
        if ty in TEMPORAL:
            builders.append(self._interval_arithmetic)
        if _OVERLOADS_BY_RETURN.get(ty):
            builders.append(self._function_call)
        if ty is Ty.VARBINARY:
            builders.append(self._codec_round_trip)
        if ty in (Ty.VARCHAR, Ty.INTEGER):
            builders.append(self._sql92_spelling)
        if ty is Ty.VARCHAR:
            builders.append(self._overlay)
        builders.append(self._cast)
        builders.append(self._case)

        for builder in rng.sample(builders, len(builders)):
            built = builder(ty, depth)
            if built is not None:
                return built

        if columns:
            return Expr(rng.choice(columns).quoted, ty)
        return Expr(self.literal(ty), ty)

    def _arithmetic(self, ty: Ty, depth: int) -> Optional[Expr]:
        rng = self.rng
        # Division and modulo by an expression can divide by zero; only integer
        # literals with a guaranteed non-zero value are used as the divisor, so
        # the query's *result* is not a question about division semantics.
        operators = ["+", "-", "*"]
        # `/` yields FLOAT whatever the operands are, so it can only be used when
        # FLOAT is what the caller asked for — returning an Expr whose declared
        # type is not its real type produces `(119280 / -9) | 112`, which the
        # binder rightly rejects.
        if ty is Ty.FLOAT:
            operators.append("/")
        # DECIMAL has no modulo kernel ("Unable to perform `d_value % 7`").
        if ty is not Ty.DECIMAL:
            operators.append("%")
        operator = rng.choice(operators)
        left = self.expression(ty, depth + 1)
        if operator in ("/", "%"):
            divisor = rng.choice([n for n in range(-9, 10) if n != 0])
            self.tags.add(f"arith{operator}")
            return Expr(f"({left.sql} {operator} {divisor})", ty)
        right = self.expression(ty, depth + 1)
        self.tags.add(f"arith{operator}")
        return Expr(f"({left.sql} {operator} {right.sql})", ty)

    def _bitwise(self, ty: Ty, depth: int) -> Optional[Expr]:
        operator = self.rng.choice(("&", "|", "^", "<<", ">>"))
        left = self.expression(Ty.INTEGER, depth + 1)
        self.tags.add(f"bitwise{operator}")
        # A shift COUNT must be 0..63, which operators.json records in the
        # ShiftLeft/ShiftRight notes — the operands are 64-bit and a count
        # outside that range fails loud rather than wrapping.
        right = self.rng.randint(0, 63) if operator in ("<<", ">>") else self.rng.randint(0, 255)
        return Expr(f"({left.sql} {operator} {right})", Ty.INTEGER)

    def _string_concat(self, ty: Ty, depth: int) -> Optional[Expr]:
        left = self.expression(Ty.VARCHAR, depth + 1)
        right = self.expression(Ty.VARCHAR, depth + 1)
        self.tags.add("string_concat")
        return Expr(f"({left.sql} || {right.sql})", Ty.VARCHAR)

    def _interval_arithmetic(self, ty: Ty, depth: int) -> Optional[Expr]:
        rng = self.rng
        # DATE +/- INTERVAL widens to TIMESTAMP, so a DATE result cannot be
        # built this way — claiming otherwise makes the enclosing expression
        # mistype ("IFNOTNULL: expression is TIMESTAMP[us] but column
        # 'birth_date' is DATE").
        if ty is not Ty.TIMESTAMP:
            return None
        base = self.expression(rng.choice(TEMPORAL), depth + 1)
        unit = rng.choice(_INTERVAL_UNITS)
        operator = rng.choice(("+", "-"))
        self.tags.add("interval")
        return Expr(f"({base.sql} {operator} INTERVAL '{rng.randint(1, 30)}' {unit})", Ty.TIMESTAMP)

    def _function_call(self, ty: Ty, depth: int) -> Optional[Expr]:
        candidates = _OVERLOADS_BY_RETURN.get(ty)
        if not candidates:
            return None
        satisfiable = [
            candidate
            for candidate in candidates
            if all(any(self.can_produce(t) for t in param.accepts) for param in candidate.params)
        ]
        if not satisfiable:
            return None
        overload = self.rng.choice(satisfiable)
        # `homogeneous` and the per-parameter `accepts` narrowing both come from
        # the catalog now — `overload["homogeneous"]` and `parameters[].excludes`
        # respectively, applied in _load_function_overloads.
        shared_ty: Optional[Ty] = None
        arguments: List[str] = []
        drawn_constants: Dict[int, str] = {}
        for index, param in enumerate(overload.params):
            allowed = tuple(t for t in param.accepts if self.can_produce(t))
            allowed = self._narrow_for_drawn_constants(overload, index, allowed, drawn_constants)
            if not allowed:
                return None
            # A polymorphic return ("same as `num`") is pinned by argument 0:
            # to return `ty`, argument 0 must BE `ty`.
            if overload.returns is None and index == 0:
                if ty not in allowed:
                    return None
                argument_ty = ty
            elif overload.homogeneous and shared_ty is not None:
                if shared_ty not in allowed:
                    return None
                argument_ty = shared_ty
            else:
                argument_ty = self.rng.choice(allowed)
            if shared_ty is None:
                shared_ty = argument_ty
            if param.constant_only:
                constant = self._constant_argument(overload.name, param, argument_ty)
                drawn_constants[index] = constant
                arguments.append(constant)
            elif argument_ty is Ty.BOOLEAN:
                # A BOOLEAN function argument is a predicate in operand
                # position (IIF's condition), which is where RLIKE breaks.
                was_operand = self._predicate_is_an_operand
                self._predicate_is_an_operand = True
                try:
                    arguments.append(self.predicate(depth + 1))
                finally:
                    self._predicate_is_an_operand = was_operand
            else:
                arguments.append(
                    self._function_argument(overload.name, param, index, argument_ty, depth)
                )
        self.tags.add(f"fn:{overload.name}")
        return Expr(f"{overload.name}({', '.join(arguments)})", ty)

    def _narrow_for_drawn_constants(
        self,
        overload: Overload,
        index: int,
        allowed: Tuple[Ty, ...],
        drawn: Dict[int, str],
    ) -> Tuple[Ty, ...]:
        """Narrow a parameter's types given the constants already drawn.

        One case, and it is stated in the catalog rather than discovered here:
        EXTRACT's `part` parameter documents that "sub-day parts (hour, minute,
        second) require a TIMESTAMP operand - over a DATE the kernel refuses
        them". The domain is machine-readable; the coupling between the drawn
        part and the operand's type is not expressible as a per-parameter type,
        so it is applied here, against the same seven-part domain the catalog
        publishes.
        """
        if overload.name != "EXTRACT" or index == 0:
            return allowed
        part = drawn.get(0, "").strip("'").lower()
        if part not in _SUB_DAY_PARTS:
            return allowed
        return tuple(t for t in allowed if t is Ty.TIMESTAMP)

    def _function_argument(
        self, function: str, param: Param, index: int, ty: Ty, depth: int
    ) -> str:
        """One non-constant argument.

        A parameter carrying catalog `minimum`/`maximum` bounds gets a literal
        drawn from inside them. Those bounds were four hardcoded special cases
        here (FROM_UNIXTIME's year-9999 ceiling, TO_CHAR's codepoint range,
        TIME_BUCKET's positive magnitude) written from the exceptions the engine
        raised; `reference/` records them now, so the rule is one branch instead
        of a list of function names.
        """
        if function in _BUCKET_WIDTH_FUNCTIONS and index == 0:
            return str(self.rng.randint(1, 12))
        if param.minimum is not None or param.maximum is not None:
            return self._bounded_literal(function, param, ty)
        # Precision, scale, length and position arguments are integers, and the
        # catalog types them as plain `integer` — so an unconstrained integer
        # literal produces `ROUND(x, -321178)`. That binds and executes, but it
        # spends the case on an argument no caller would write instead of on the
        # function's actual behaviour. Not a correctness rule, so not a catalog
        # constraint: a taste rule about where to spend a fuzz case.
        if ty is Ty.INTEGER and index > 0 and function in _SMALL_INTEGER_ARGUMENT_FUNCTIONS:
            return str(self.rng.randint(0, 12))
        return self.expression(ty, depth + 1).sql

    def _bounded_literal(self, function: str, param: Param, ty: Ty) -> str:
        """A literal inside the catalog's declared bounds for this parameter.

        Kept well inside them rather than at the endpoints: the bounds say where
        the engine stops accepting values, and a generator that only ever emits
        boundary values is testing the bound, not the function. The endpoints
        themselves are covered by the type-and-literal tests.
        """
        low = -1_000_000_000 if param.minimum is None else int(param.minimum)
        high = 1_000_000_000 if param.maximum is None else int(param.maximum)
        # Clamp a huge declared range to a plausible window. FROM_UNIXTIME's is
        # ~316 billion seconds wide; drawing uniformly across it puts almost
        # every value in a year nobody stores data for, and clamping from the
        # LOW end instead pins every draw near year 1. Centre on zero when zero
        # is admissible — for an epoch parameter that is 1906..2033.
        if low <= 0 <= high:
            low, high = max(low, -2_000_000_000), min(high, 2_000_000_000)
        else:
            high = min(high, low + 2_000_000_000)
        value = self.rng.randint(low, high)
        # TO_CHAR returns a VARCHAR, and VARCHAR is ASCII bytes — non-ASCII content
        # in one is undefined behaviour, not a supported case
        # (single_table_known_gaps/RATIFIED/
        # varchar-is-ascii-bytes-and-non-ascii-content-is-undefined). A codepoint
        # above 127 manufactures exactly that: `LEFT(TO_CHAR(952883), 1)` encodes a
        # 4-byte sequence and then takes the first BYTE of it, and reading the
        # result raises UnicodeDecodeError. The engine is behaving to contract
        # there, so the generator stays inside the range where the contract holds.
        # This also subsumes the surrogate hole (U+D800..U+DFFF) the catalog
        # documents but cannot express as a bound — it is far above 127.
        if function == "TO_CHAR":
            value = self.rng.randint(0, 127)
        return str(float(value)) if ty in (Ty.FLOAT, Ty.DECIMAL) else str(value)

    def _constant_argument(self, function: str, param: Param, ty: Ty) -> str:
        """A literal for a `constant_only` parameter.

        A parameter with an enumerated `domain` draws from it. The date-part
        domains used to be one tuple in this file covering EXTRACT, DATEDIFF,
        TRUNC and TIME_BUCKET at once — which was wrong for all four, since the
        four accept different sets. Each parameter now carries its own.
        """
        if param.domain:
            return "'" + self.rng.choice(param.domain) + "'"
        if param.minimum is not None or param.maximum is not None:
            return self._bounded_literal(function, param, ty)
        if function == "FORMAT_TIMESTAMP":
            # A strftime pattern is not a closed set, so there is no domain to
            # record; these are three patterns worth exercising.
            return "'" + self.rng.choice(("%Y-%m-%d", "%Y", "%H:%M:%S")) + "'"
        return self.literal(ty)

    def _temporal_branch(self, ty: Ty) -> str:
        """A temporal CASE branch: a column or a literal, never a function call."""
        columns = self.relation.of(ty)
        if columns and self.rng.random() < 0.7:
            return self.rng.choice(columns).quoted
        return self.literal(ty)

    def _cast(self, ty: Ty, depth: int) -> Optional[Expr]:
        rng = self.rng
        sources = [source for source, targets in CAST_TARGETS.items() if _cast_yields(targets, ty)]
        if not sources:
            return None
        source_ty = rng.choice(sources)
        target = rng.choice([t for t in CAST_TARGETS[source_ty] if _CAST_TARGET_TY[t] is ty])
        operand = self.expression(source_ty, depth + 1)
        # TRY_CAST and CAST must agree on every value that CAST accepts, which
        # is what makes emitting both worthwhile rather than decorative.
        keyword = "TRY_CAST" if rng.random() < 0.3 else "CAST"
        self.tags.add(keyword.lower())
        return Expr(f"{keyword}({operand.sql} AS {target})", ty)

    def _overlay(self, ty: Ty, depth: int) -> Optional[Expr]:
        """`OVERLAY(s PLACING r FROM start [FOR length])` — SQL-92 string splice.

        Its own production in the dialect, like the SUBSTRING/POSITION/EXTRACT
        infix forms, and lowered in the planner to
        `SUBSTRING(s,1,start-1) || r || SUBSTRING(s,start+len,LENGTH(s))`. Both
        the optional and the explicit FOR form are emitted, because they take
        different paths through that lowering — the optional one synthesises
        LENGTH(r) as the length.
        """
        rng = self.rng
        source = self.expression(Ty.VARCHAR, depth + 1)
        replacement = self.expression(Ty.VARCHAR, depth + 1)
        start = rng.randint(1, 8)
        if rng.random() < 0.5:
            self.tags.add("sql92:OVERLAY")
            return Expr(f"OVERLAY({source.sql} PLACING {replacement.sql} FROM {start})", Ty.VARCHAR)
        self.tags.add("sql92:OVERLAY/FOR")
        return Expr(
            f"OVERLAY({source.sql} PLACING {replacement.sql} FROM {start} FOR {rng.randint(0, 8)})",
            Ty.VARCHAR,
        )

    def _codec_round_trip(self, ty: Ty, depth: int) -> Optional[Expr]:
        """DECODE(ENCODE(x)) — an identity the engine has to preserve.

        The decoders raise on input that is not valid in their encoding, and no
        column holds valid base85, so pairing them with their encoder is the
        only way to reach them at all. It also asserts something: the round trip
        must give the input back.
        """
        if ty is not Ty.VARBINARY:
            return None
        encode, decode = self.rng.choice(_CODEC_ROUND_TRIPS)
        inner = self.expression(Ty.VARBINARY, depth + 1)
        self.tags.add(f"codec:{encode}")
        return Expr(f"{decode}({encode}({inner.sql}))", Ty.VARBINARY)

    def _sql92_spelling(self, ty: Ty, depth: int) -> Optional[Expr]:
        """A function reached by its SQL-92 spelling rather than its call form.

        THIS IS A DIFFERENT PARSER PATH, which is the entire reason it is here.
        Everything else in this generator renders `NAME(arg, arg)` off the
        catalog's `catalog_name`, so three things were never exercised at all:

          * the infix argument syntax the catalog itself publishes in `label` —
            `SUBSTRING(str FROM start FOR length)`, `POSITION(needle IN
            haystack)`, `EXTRACT(part FROM date)` — which the dialect parses into
            the same function node by a separate production;
          * the catalog's `aliases`, since generation keys on the canonical name.
            `CHARACTER_LENGTH`/`CHAR_LENGTH` are LENGTH's aliases and had zero
            coverage;
          * EXTRACT in any form. Its `return_type` is `integer | double | date`,
            which resolves to Ty.UNKNOWN, and UNKNOWN is never a REQUESTED type —
            so the catalog path loaded EXTRACT, counted it as generated, and
            could never emit it. The infix form pins the part instead, and every
            part in the catalog's domain measurably returns INT64.

        The spellings the engine does NOT accept are registered rather than
        quietly skipped: single_table_known_gaps/trim-with-no-trim-character-is-
        unparseable, /is-distinct-from-is-unhandled and /overlay-is-unhandled.
        """
        rng = self.rng
        if ty is Ty.VARCHAR:
            # TRIM's three directions are tagged separately because they are three
            # different functions below the parser — logical_planner_builders
            # .trim_string maps BOTH/LEADING/TRAILING onto TRIM/LTRIM/RTRIM — so
            # one tag would report coverage the run did not necessarily have.
            form = rng.choice(("substring", "substring_for", "trim"))
            operand = self.expression(Ty.VARCHAR, depth + 1)
            if form == "trim":
                where = rng.choice(("BOTH", "LEADING", "TRAILING"))
                characters = rng.choice(_TRIM_CHARACTER_SETS)
                self.tags.add(f"sql92:TRIM/{where}")
                return Expr(f"TRIM({where} '{characters}' FROM {operand.sql})", Ty.VARCHAR)
            start = rng.randint(0, 12)
            if form == "substring":
                self.tags.add("sql92:SUBSTRING/FROM")
                return Expr(f"SUBSTRING({operand.sql} FROM {start})", Ty.VARCHAR)
            self.tags.add("sql92:SUBSTRING/FROM-FOR")
            return Expr(
                f"SUBSTRING({operand.sql} FROM {start} FOR {rng.randint(0, 12)})", Ty.VARCHAR
            )

        if ty is not Ty.INTEGER:
            return None

        forms = ["position", "length_alias"]
        if self.relation.of(*TEMPORAL):
            forms.append("extract")
        form = rng.choice(forms)

        if form == "position":
            needle = self.expression(Ty.VARCHAR, depth + 1)
            haystack = self.expression(Ty.VARCHAR, depth + 1)
            self.tags.add("sql92:POSITION/IN")
            return Expr(f"POSITION({needle.sql} IN {haystack.sql})", Ty.INTEGER)

        if form == "length_alias":
            alias = rng.choice(("CHARACTER_LENGTH", "CHAR_LENGTH"))
            self.tags.add(f"sql92:{alias}")
            return Expr(f"{alias}({self.expression(Ty.VARCHAR, depth + 1).sql})", Ty.INTEGER)

        # EXTRACT carries the same part/operand coupling the call form does, and
        # for the same reason: draken_date_part refuses a sub-day part of a DATE
        # ("expression evaluation failed (err_op=15): draken_date_part: sub-day
        # part of a DATE"). _narrow_for_drawn_constants applies it on the call
        # side; here the part is drawn first, so the operand type follows it.
        part = rng.choice(_EXTRACT_PARTS)
        operand_types = (Ty.TIMESTAMP,) if part in _SUB_DAY_PARTS else TEMPORAL
        self.tags.add("sql92:EXTRACT/FROM")
        operand = self.expression(rng.choice(operand_types), depth + 1)
        return Expr(f"EXTRACT({part.upper()} FROM {operand.sql})", Ty.INTEGER)

    def _case(self, ty: Ty, depth: int) -> Optional[Expr]:
        # DECIMAL branches are omitted: a CASE blending a DECIMAL column with a
        # DECIMAL literal raises OverflowError from the rescale
        # (single_table_known_gaps/decimal-case-blend-with-a-literal-overflows).
        # ARRAY branches are omitted because CASE rejects ARRAY outright, even
        # when both branches are the SAME column
        # (single_table_known_gaps/case-rejects-two-identical-array-branches).
        if ty in (Ty.DECIMAL, Ty.ARRAY):
            return None
        was_operand = self._predicate_is_an_operand
        self._predicate_is_an_operand = True
        try:
            condition = self.predicate(depth + 1)
        finally:
            self._predicate_is_an_operand = was_operand
        # A temporal-returning FUNCTION CALL in a CASE branch has no native
        # implementation ("a function call in `IF_THEN_ELSE(...)`"), though the
        # same call outside a CASE is fine and numeric/string calls inside one
        # are fine — see
        # single_table_known_gaps/temporal-function-call-inside-a-case-branch.
        if ty in TEMPORAL:
            then = Expr(self._temporal_branch(ty), ty)
            otherwise = Expr(self._temporal_branch(ty), ty)
        else:
            then = self.expression(ty, depth + 1)
            otherwise = self.expression(ty, depth + 1)
        self.tags.add("case")
        return Expr(f"(CASE WHEN {condition} THEN {then.sql} ELSE {otherwise.sql} END)", ty)

    # ── predicates ───────────────────────────────────────────────────────────

    def predicate(self, depth: int = 0, *, negated_forms_allowed: bool = True) -> str:
        """A BOOLEAN-valued expression usable as a WHERE clause.

        Never a bare column or a bare literal: the planner rejects both
        (`WHERE clause cannot be a bare column name`), so a BOOLEAN column is
        always spelled with an explicit `= TRUE` / `IS TRUE`. A bare CASE IS
        generated — it is a value expression, not a bare reference, and the planner
        admits it; see `_case_predicate`.

        `negated_forms_allowed=False` suppresses the NOT LIKE / NOT IN / NOT
        BETWEEN spellings. It is set when this predicate is about to be wrapped
        in a `NOT (...)`, because `NOT (float_col NOT IN (...))` has no native
        filter kernel — see
        single_table_known_gaps/float-in-list-only-works-at-top-level.
        """
        rng = self.rng
        if depth < 2 and rng.random() < 0.3:
            connective = rng.choice(("AND", "OR", "AND NOT", "OR NOT"))
            self.tags.add("connective")
            right_negations = negated_forms_allowed and not connective.endswith("NOT")
            left = self.predicate(depth + 1, negated_forms_allowed=negated_forms_allowed)
            right = self.predicate(depth + 1, negated_forms_allowed=right_negations)
            # `X OR NOT X` and `X AND X` are degenerate: they test constant
            # folding rather than the predicate, and the corpus has few enough
            # BOOLEAN columns and forms that the two sides collide by chance
            # fairly often. One retry, then fall back to the left operand alone.
            if right == left:
                right = self.predicate(depth + 1, negated_forms_allowed=right_negations)
                if right == left:
                    return left
            return f"({left} {connective} {right})"
        if depth < 2 and rng.random() < 0.08:
            self.tags.add("not")
            # `(NOT {child})`, not `(NOT ({child}))`: every predicate this class
            # returns is already parenthesised, and a SECOND pair around a
            # FLOAT IN-list drops the query out of the native kernel set
            # (single_table_known_gaps/float-in-list-only-works-at-top-level).
            return f"(NOT {self.predicate(depth + 1, negated_forms_allowed=False)})"

        builders = [
            self._comparison_predicate,
            self._null_predicate,
            self._between_predicate,
            self._in_list_predicate,
            self._like_predicate,
            self._boolean_column_predicate,
            self._distinct_from_predicate,
            self._array_predicate,
            self._json_predicate,
            self._case_predicate,
        ]
        for builder in rng.sample(builders, len(builders)):
            built = builder(depth, negated_forms_allowed)
            if built is not None:
                return built
        # Every relation has at least one scalar column, so this is reachable
        # only if the corpus is misconfigured — fail loudly rather than emitting
        # a tautology that would quietly weaken every predicate oracle.
        raise AssertionError(f"no predicate constructible over relation {self.relation.sql!r}")

    def _comparison_predicate(self, depth: int, negated: bool) -> Optional[str]:
        rng = self.rng
        candidates = [c for c in self.relation.columns if c.ty in SCALAR]
        if not candidates:
            return None
        ty = rng.choice(candidates).ty
        left = self.expression(ty, depth + 1)
        right = self.expression(ty, depth + 1)
        # BOOLEAN has no ordering: types.json lists BOOLEAN as comparable only
        # with BOOLEAN, and the engine rejects `bool <= bool` outright.
        operators = _EQUALITY if ty is Ty.BOOLEAN else _COMPARISONS
        self.tags.add("comparison")
        return f"({left.sql} {rng.choice(operators)} {right.sql})"

    def _distinct_from_predicate(self, depth: int, negated: bool) -> Optional[str]:
        """`a IS [NOT] DISTINCT FROM b` — null-safe, and never UNKNOWN.

        Worth generating precisely because it is TOTAL: the predicate-partition
        oracle asserts |p| + |NOT p| + |p IS NULL| == |R|, and for this operator
        the third bucket must always be empty. A lowering that leaked UNKNOWN
        would show up there immediately.
        """
        rng = self.rng
        candidates = [c for c in self.relation.columns if c.ty in SCALAR]
        if not candidates:
            return None
        ty = rng.choice(candidates).ty
        left = self.expression(ty, depth + 1)
        right = self.expression(ty, depth + 1)
        form = "IS NOT DISTINCT FROM" if negated and rng.random() < 0.5 else "IS DISTINCT FROM"
        self.tags.add("distinct_from")
        return f"({left.sql} {form} {right.sql})"

    def _null_predicate(self, depth: int, negated: bool) -> Optional[str]:
        rng = self.rng
        candidates = [c for c in self.relation.columns if c.ty in SCALAR]
        if not candidates:
            return None
        column = rng.choice(candidates)
        self.tags.add("is_null")
        return f"({column.quoted} {rng.choice(('IS NULL', 'IS NOT NULL') if negated else ('IS NULL',))})"

    def _between_predicate(self, depth: int, negated: bool) -> Optional[str]:
        rng = self.rng
        candidates = [c for c in self.relation.columns if c.ty in NUMERIC + TEMPORAL]
        if not candidates:
            return None
        column = rng.choice(candidates)
        low = self.literal(column.ty)
        high = self.literal(column.ty)
        negate = "NOT " if negated and rng.random() < 0.3 else ""
        self.tags.add("between")
        return f"({column.quoted} {negate}BETWEEN {low} AND {high})"

    def _in_list_predicate(self, depth: int, negated: bool) -> Optional[str]:
        rng = self.rng
        # A FLOAT IN-list only has a native kernel when it IS the whole
        # predicate: as a disjunct, or under a NOT, or wrapped in one extra
        # paren pair, it raises 'a comparison in a filter predicate `...`,
        # outside the c-native kernel set'. INTEGER and VARCHAR are unaffected. See
        # single_table_known_gaps/float-in-list-only-works-at-top-level.
        types = (Ty.INTEGER, Ty.VARCHAR) if depth > 0 else (Ty.INTEGER, Ty.FLOAT, Ty.VARCHAR)
        candidates = [c for c in self.relation.columns if c.ty in types]
        if not candidates:
            return None
        column = rng.choice(candidates)
        members = ", ".join(self.literal(column.ty) for _ in range(rng.randint(1, 4)))
        negate = "NOT " if negated and rng.random() < 0.3 else ""
        self.tags.add("in_list")
        return f"({column.quoted} {negate}IN ({members}))"

    def _like_predicate(self, depth: int, negated: bool) -> Optional[str]:
        rng = self.rng
        # LIKE works on VARBINARY; ILIKE and RLIKE do not
        # ("Unable to perform `json_doc ILIKE ...` because the values are not
        # acceptable types"), so the operand pool narrows with the operator.
        text = self.relation.of(Ty.VARCHAR)
        binary = self.relation.of(Ty.VARBINARY)
        if not text and not binary:
            return None
        # RLIKE evaluates correctly only as the whole predicate or as a direct
        # child of one connective; nested any deeper — or in operand position
        # inside CASE/IIF — it fails at execution with err_op=15. See
        # single_table_known_gaps/rlike-outside-top-level-predicate-position.
        rlike_ok = not self._predicate_is_an_operand and depth <= 1
        if text and rlike_ok and rng.random() < 0.2:
            self.tags.add("rlike")
            operator = rng.choice(("RLIKE", "NOT RLIKE") if negated else ("RLIKE",))
            pattern = "'" + rng.choice(("^a", "[aeiou]", "z$", "[0-9]")) + "'"
            column = rng.choice(text)
        elif text and rng.random() < 0.5:
            self.tags.add("ilike")
            operator = rng.choice(("ILIKE", "NOT ILIKE") if negated else ("ILIKE",))
            pattern = self.like_pattern()
            column = rng.choice(text)
        else:
            self.tags.add("like")
            operator = rng.choice(("LIKE", "NOT LIKE") if negated else ("LIKE",))
            pattern = self.like_pattern()
            column = rng.choice(text + binary)
        return f"({column.quoted} {operator} {pattern})"

    def _boolean_column_predicate(self, depth: int, negated: bool) -> Optional[str]:
        rng = self.rng
        candidates = self.relation.of(Ty.BOOLEAN)
        if not candidates:
            return None
        column = rng.choice(candidates)
        forms = ("IS TRUE", "IS FALSE", "= TRUE", "= FALSE")
        if negated:
            forms += ("IS NOT TRUE", "IS NOT FALSE")
        form = rng.choice(forms)
        self.tags.add("boolean_predicate")
        return f"({column.quoted} {form})"

    def _case_predicate(self, depth: int, negated: bool) -> Optional[str]:
        """A bare CASE used directly as a WHERE predicate.

        Admitted by the RULING recorded on
        `logical_planner._validate_where_clause_expression` (architect, 2026-08-10):
        a WHERE clause must be a boolean VALUE EXPRESSION, and a CASE is one.

        The MULTI-branch form is the load-bearing coverage. The optimizer's
        `CASE -> IIF` rewrite only ever collapses a SINGLE-branch CASE, so a switch
        can never reach the filter as an IIF — it arrives as the folded
        `draken_if_then_else` chain that `BC_RESULT_WRAP_AS_BOOL` makes bool-final,
        and no other generated shape exercises that path.

        Conditions come from the three narrow builders rather than from
        `predicate()`: a CASE condition is not a top-level filter predicate, so
        routing the full grammar through here would nest shapes that are only
        supported at top level (a FLOAT IN-list — see
        single_table_known_gaps/float-in-list-only-works-at-top-level) and report
        registered defects instead of testing CASE.
        """
        rng = self.rng
        if depth >= 2:
            return None
        condition_builders = [
            self._comparison_predicate,
            self._null_predicate,
            self._boolean_column_predicate,
        ]
        # These three cannot emit RLIKE today, but a CASE condition IS operand
        # position, so it carries the same flag every other CASE/IIF condition
        # carries — the guard belongs to the position, not to today's builders.
        conditions: List[str] = []
        was_operand = self._predicate_is_an_operand
        self._predicate_is_an_operand = True
        try:
            for builder in rng.sample(condition_builders, len(condition_builders)):
                built = builder(depth + 1, negated)
                if built is not None:
                    conditions.append(built)
                if len(conditions) == rng.randint(1, 3):
                    break
        finally:
            self._predicate_is_an_operand = was_operand
        if not conditions:
            return None
        self.tags.add("case_predicate")
        if len(conditions) > 1:
            self.tags.add("case_switch_predicate")
        # The first arm is always TRUE and the ELSE is never TRUE, so the CASE can
        # never fold to a constant — a constant predicate would test constant
        # folding rather than the CASE, and `WHERE <constant>` is refused as a bare
        # literal in the spelling this builder is here to cover.
        arms = " ".join(
            f"WHEN {condition} THEN {'TRUE' if index % 2 == 0 else 'FALSE'}"
            for index, condition in enumerate(conditions)
        )
        # No ELSE and `ELSE NULL` are the 3VL shapes: an unmatched row evaluates to
        # NULL, which a WHERE drops exactly as it drops FALSE.
        tail = rng.choice(("ELSE FALSE", "ELSE NULL", ""))
        if tail:
            self.tags.add(f"case_{tail.split()[1].lower()}_branch")
            return f"(CASE {arms} {tail} END)"
        self.tags.add("case_no_else_branch")
        return f"(CASE {arms} END)"

    def _array_predicate(self, depth: int, negated: bool) -> Optional[str]:
        rng = self.rng
        candidates = [c for c in self.relation.of(Ty.ARRAY) if c.name in ARRAY_ELEMENT_TYPES]
        if not candidates:
            return None
        column = rng.choice(candidates)
        element_ty = ARRAY_ELEMENT_TYPES[column.name]
        probe = self.literal(element_ty)
        # `NOT (x = ANY(arr))` has no native filter kernel, so an ARRAY
        # predicate is never generated where a NOT could reach it — see
        # single_table_known_gaps/negated-array-contains-has-no-kernel.
        if not negated:
            return None
        self.tags.add("array_contains")
        if rng.random() < 0.4:
            self.tags.add("array_containment")
            members = ", ".join(self.literal(element_ty) for _ in range(rng.randint(1, 3)))
            return f"({column.quoted} {rng.choice(('@>', '@>>'))} [{members}])"
        return f"({probe} = ANY({column.quoted}))"

    def _json_predicate(self, depth: int, negated: bool) -> Optional[str]:
        rng = self.rng
        candidates = [c for c in self.relation.columns if c.name in _JSON_COLUMNS]
        if not candidates:
            return None
        column = rng.choice(candidates)
        key = rng.choice(_JSON_KEYS)
        self.tags.add("json_accessor")
        return f"({column.quoted} ->> '{key}' {rng.choice(_COMPARISONS)} '{rng.choice(_STRING_LITERALS)}')"


def _cast_yields(targets: Sequence[str], ty: Ty) -> bool:
    return any(_CAST_TARGET_TY[target] is ty for target in targets)


_REFERENCE_DATE = datetime.datetime(2005, 6, 15, 12, 0, 0)

# Literal strings drawn from what the corpus actually contains, so LIKE and
# equality predicates select non-empty subsets rather than always matching zero
# rows. A predicate that never matches exercises only the filter's empty path.
#
# ASCII-ONLY, and deliberately so rather than for want of imagination. A quoted
# string literal binds VARCHAR, VARCHAR is ASCII bytes, and non-ASCII content in
# one is undefined behaviour — so a non-ASCII literal here would generate input
# the engine makes no promise about, and every oracle downstream would be
# asserting on an answer that was never specified. `REVERSE('ÅΩ漢字')` returning
# an undecodable byte sequence is the contract, not a finding
# (single_table_known_gaps/RATIFIED/
# varchar-is-ascii-bytes-and-non-ascii-content-is-undefined). Unicode belongs in
# a fuzzer over NVARCHAR, where the promise exists.
_STRING_LITERALS = (
    "alpha",
    "beta",
    "gamma",
    "delta",
    "epsilon",
    "zeta",
    "eta",
    "theta",
    "a",
    "e",
    "row",
    "item",
    "0",
)

# Columns known to hold JSON documents, and the keys those documents carry.
# Column *content* is not in any schema the engine exposes, so this is stated
# here; it is data about the corpus, not about the engine's capabilities.
_JSON_COLUMNS = frozenset({"json_doc", "birth_place"})
_JSON_KEYS = ("name", "n", "nested", "town")


# ─────────────────────────────────────────────────────────────────────────────
# Query shapes
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class SelectQuery:
    """A single SELECT, held structurally rather than as a string.

    The oracles need variants of the query — the same query with an extra
    conjunct, with the ORDER BY dropped, wrapped in a subquery. Producing those
    by string surgery on rendered SQL is how the previous metamorphic fuzzer
    ended up transforming queries into different queries (its `add_is_not_null`
    appended a predicate after the LIMIT). Re-rendering from the structure
    cannot do that.
    """

    source: str
    projection: List[str]
    output_columns: Tuple[Column, ...]
    distinct: bool = False
    where: Optional[str] = None
    group_by: List[str] = field(default_factory=list)
    having: Optional[str] = None
    #: The whole QUALIFY clause, without the keyword. Held structurally like every
    #: other clause so the oracles' re-renders carry it.
    qualify: Optional[str] = None
    #: The predicate of an aggregate `FILTER (WHERE ...)` in the projection, if
    #: one was emitted. Read by the aggregate_filter_matches_where oracle, which
    #: needs the predicate itself rather than the rendered call.
    aggregate_filter: Optional[str] = None
    order_by: List[str] = field(default_factory=list)
    limit: Optional[int] = None
    offset: Optional[int] = None
    has_aggregate: bool = False
    has_ranking_window: bool = False
    tags: Set[str] = field(default_factory=set)

    def render(
        self,
        *,
        extra_where: Optional[str] = None,
        replace_where: Optional[str] = None,
        drop_order: bool = False,
        drop_limit: bool = False,
    ) -> str:
        parts = ["SELECT"]
        if self.distinct:
            parts.append("DISTINCT")
        parts.append(", ".join(self.projection))
        parts.append(f"FROM {self.source}")

        where = self.where if replace_where is None else replace_where
        if extra_where is not None:
            where = extra_where if where is None else f"({where}) AND ({extra_where})"
        if where is not None:
            parts.append(f"WHERE {where}")
        if self.group_by:
            parts.append("GROUP BY " + ", ".join(self.group_by))
        if self.having is not None:
            parts.append(f"HAVING {self.having}")
        # QUALIFY sits between HAVING and ORDER BY: it filters on window values,
        # which are computed after grouping and before ordering.
        if self.qualify is not None:
            parts.append(f"QUALIFY {self.qualify}")
        if self.order_by and not drop_order:
            parts.append("ORDER BY " + ", ".join(self.order_by))
        if not drop_limit:
            if self.limit is not None:
                parts.append(f"LIMIT {self.limit}")
            if self.offset is not None:
                parts.append(f"OFFSET {self.offset}")
        return " ".join(parts)

    @property
    def sql(self) -> str:
        return self.render()

    @property
    def row_multiset_is_determined(self) -> bool:
        """Whether two executions must return the same rows.

        A LIMIT or OFFSET without a total order picks an arbitrary subset, so
        two runs may legitimately return different rows. Comparing those would
        make an oracle fire on non-bugs, which trains everyone to ignore it.
        """
        return self.limit is None and self.offset is None


def build_select(rng: random.Random, relation: Relation, names: Names) -> SelectQuery:
    """Generate one SELECT over `relation`."""
    generator = Generator(rng, relation, names)
    shape = rng.random()

    if shape < 0.30:
        query = _build_projection(generator, relation)
    elif shape < 0.55:
        query = _build_aggregate(generator, relation)
    elif shape < 0.70:
        query = _build_distinct(generator, relation)
    elif shape < 0.85:
        query = _build_window(generator, relation)
    else:
        query = _build_projection(generator, relation)

    # A derived relation can expose nothing a predicate could be built over —
    # every column an ARRAY, say. `predicate()` raises rather than silently
    # emitting a tautology, so the caller has to check first.
    if _scalar_columns(relation) and rng.random() < 0.7:
        query.where = generator.predicate()

    _apply_order_limit(rng, generator, query)
    query.tags |= generator.tags
    return query


def _scalar_columns(relation: Relation) -> List[Column]:
    return [column for column in relation.columns if column.ty in SCALAR]


def _build_projection(generator: Generator, relation: Relation) -> SelectQuery:
    rng = generator.rng
    if rng.random() < 0.12:
        return SelectQuery(
            source=relation.sql,
            projection=["*"],
            output_columns=relation.columns,
            tags={"select_star"},
        )
    projection: List[str] = []
    outputs: List[Column] = []
    # Two columns with the same OUTPUT name raise AmbiguousIdentifierError, so each
    # output name may appear at most once. `used` holds output names only — a source
    # column may be projected any number of times as long as each appearance is
    # differently named, so `SELECT id AS e1, id` is generated on purpose.
    used: Set[str] = set()
    for _ in range(rng.randint(1, 4)):
        if rng.random() < 0.55:
            column = rng.choice(relation.columns)
            if column.name in used:
                continue
            used.add(column.name)
            projection.append(column.quoted)
            outputs.append(column)
        else:
            ty = rng.choice(_projectable_types(relation))
            expression = generator.expression(ty)
            # Written as built, parentheses and all. `_unparenthesise` used to strip
            # one redundant enclosing pair here because aliasing a parenthesised
            # expression lost the alias; that defect is fixed (`_strip_outer_nesting`
            # in the logical planner) and its register entry is gone, so the
            # suppression went with it.
            rendered = expression.sql
            alias = generator.names.next("e")
            used.add(alias)
            projection.append(f"{rendered} AS {alias}")
            outputs.append(Column(alias, expression.ty))
    if not projection:
        column = relation.columns[0]
        projection.append(column.quoted)
        outputs.append(column)
    return SelectQuery(
        source=relation.sql,
        projection=projection,
        output_columns=tuple(outputs),
        tags={"projection"},
    )


def _projectable_types(relation: Relation) -> List[Ty]:
    present = {column.ty for column in relation.columns}
    return [ty for ty in SCALAR if ty in present] or [Ty.INTEGER]


def _build_aggregate(generator: Generator, relation: Relation) -> SelectQuery:
    rng = generator.rng
    groupable = [c for c in _scalar_columns(relation) if c.ty is not Ty.ARRAY]
    grouped = bool(groupable) and rng.random() < 0.6

    pool = list(GLOBAL_AGGREGATES) + (list(GROUPED_ONLY_AGGREGATES) if grouped else [])
    projection: List[str] = []
    outputs: List[Column] = []
    group_by: List[str] = []

    if grouped:
        keys = rng.sample(groupable, rng.randint(1, min(2, len(groupable))))
        for key in keys:
            projection.append(key.quoted)
            outputs.append(key)
            group_by.append(key.quoted)

    aggregate_count = 0
    # Two identical aggregate expressions under different aliases collapse into
    # one output column: `SELECT MAX(ts) AS a1, MAX(ts) AS a2 FROM ...` raises
    # AmbiguousIdentifierError on `a2`. Keeping the set distinct sidesteps a
    # generator-shaped failure; the engine behaviour is registered separately.
    emitted: Set[str] = set()
    for _ in range(rng.randint(1, 2)):
        name = rng.choice(pool)
        alias = generator.names.next("a")
        if name == "COUNT" and rng.random() < 0.4:
            if "COUNT(*)" in emitted:
                continue
            emitted.add("COUNT(*)")
            projection.append(f"COUNT(*) AS {alias}")
            outputs.append(Column(alias, Ty.INTEGER))
            generator.tags.add("agg:COUNT(*)")
            aggregate_count += 1
            continue
        accepted = _AGGREGATE_INPUT_TYPES.get(name, SCALAR)
        candidates = [c for c in relation.columns if c.ty in accepted]
        if name in _AGGREGATES_WITHHELD_FROM_NAN:
            # An aggregate whose answer over a NaN-bearing column is unstable
            # disarms every oracle that compares two runs, so it is withheld
            # from the specials columns only — it still runs over every other
            # column. The narrowing expires with the register entry that
            # justifies it (test_nan_withholding_cites_a_live_register_entry).
            candidates = [c for c in candidates if c.name not in NAN_BEARING_COLUMNS]
        if not candidates:
            continue
        column = rng.choice(candidates)
        distinct = "DISTINCT " if name in _DISTINCT_CAPABLE_AGGREGATES and rng.random() < 0.3 else ""
        call = f"{name}({distinct}{column.quoted})"
        if call in emitted:
            continue
        emitted.add(call)
        projection.append(f"{call} AS {alias}")
        returns = _AGGREGATE_RETURNS.get(name)
        outputs.append(Column(alias, column.ty if returns is None else returns))
        generator.tags.add(f"agg:{name}{'/DISTINCT' if distinct else ''}")
        aggregate_count += 1

    if aggregate_count == 0:
        alias = generator.names.next("a")
        projection.append(f"COUNT(*) AS {alias}")
        outputs.append(Column(alias, Ty.INTEGER))
        generator.tags.add("agg:COUNT(*)")

    # `COUNT(*) FILTER (WHERE p)`. COUNT is the only aggregate that accepts a
    # FILTER at all — every other one is refused at plan-build time ("Filters are
    # not supported with aggregate function 'SUM'"), and so is COUNT(DISTINCT x)
    # — so this is the whole of the construct's reachable surface, not a sample
    # of it. Emitted at a high rate on purpose: it is the only shape the
    # aggregate_filter_matches_where oracle can run against.
    #
    # `"COUNT(*)"` is put into `emitted` because the FILTER'd count and a plain
    # one COLLIDE: the filter is dropped from the aggregate's identity, so
    # `COUNT(*) FILTER (WHERE p) AS n, COUNT(*) AS m` raises
    # AmbiguousIdentifierError on `m`. Same root as
    # single_table_known_gaps/aggregate-filter-is-silently-ignored, and it goes
    # away when that does.
    aggregate_filter: Optional[str] = None
    if _scalar_columns(relation) and "COUNT(*)" not in emitted and rng.random() < 0.55:
        # depth=1, not the default 0. A FILTER predicate is lowered to the
        # condition of an IIF, so it is NESTED by construction — and an IN-list
        # on a FLOAT column only has a native kernel as the WHOLE predicate
        # (single_table_known_gaps/float-in-list-only-works-at-top-level).
        # Generating at depth 1 applies that rule, the same way a predicate under
        # a connective gets it.
        aggregate_filter = generator.predicate(depth=1)
        emitted.add("COUNT(*)")
        alias = generator.names.next("a")
        projection.append(f"COUNT(*) FILTER (WHERE {aggregate_filter}) AS {alias}")
        outputs.append(Column(alias, Ty.INTEGER))
        generator.tags.add("agg:COUNT(*)/FILTER")

    query = SelectQuery(
        source=relation.sql,
        projection=projection,
        output_columns=tuple(outputs),
        group_by=group_by,
        aggregate_filter=aggregate_filter,
        has_aggregate=True,
        tags={"aggregate"},
    )
    if grouped and rng.random() < 0.35:
        query.having = f"COUNT(*) {rng.choice(('>', '>=', '<', '<='))} {rng.randint(0, 5)}"
        generator.tags.add("having")
    return query


def _build_distinct(generator: Generator, relation: Relation) -> SelectQuery:
    rng = generator.rng
    candidates = _scalar_columns(relation)
    if not candidates:
        return _build_projection(generator, relation)
    columns = rng.sample(candidates, rng.randint(1, min(3, len(candidates))))
    return SelectQuery(
        source=relation.sql,
        projection=[column.quoted for column in columns],
        output_columns=tuple(columns),
        distinct=True,
        tags={"distinct"},
    )


def _build_window(generator: Generator, relation: Relation) -> SelectQuery:
    """A ranking or aggregate window.

    Only ROW_NUMBER / RANK / DENSE_RANK exist as ranking functions — LEAD, LAG,
    NTILE and FIRST_VALUE all raise FunctionNotFoundError — and the planner
    requires a ranking window to carry an ORDER BY inside its OVER clause.
    Window functions have no entry in `reference/`; that catalog gap is why this
    set is spelled out here rather than loaded.
    """
    rng = generator.rng
    orderable = [c for c in _scalar_columns(relation)]
    if not orderable:
        return _build_projection(generator, relation)

    order_column = rng.choice(orderable)
    partition_column = rng.choice(orderable) if rng.random() < 0.6 else None
    partition = f"PARTITION BY {partition_column.quoted} " if partition_column else ""

    projection: List[str] = []
    outputs: List[Column] = []
    # Carry the window's own columns in the projection. It costs nothing and
    # keeps the query readable when it fails.
    for column in {order_column, partition_column} - {None}:
        projection.append(column.quoted)
        outputs.append(column)

    alias = generator.names.next("w")
    ranking = rng.random() < 0.6
    qualify: Optional[str] = None
    if ranking:
        function = rng.choice(("ROW_NUMBER", "RANK", "DENSE_RANK"))
        direction = rng.choice(("ASC", "DESC"))
        window = f"{function}() OVER ({partition}ORDER BY {order_column.quoted} {direction})"
        # QUALIFY filters on the window's value instead of projecting it. Same
        # ORDER-BY-required rule as any ranking window — the planner rejects a
        # ranking OVER () with no ORDER BY — so the clause is built from the same
        # window text either way.
        #
        # NOTE FOR WHEN single_table_known_gaps/qualify-is-silently-ignored IS
        # FIXED: `ROW_NUMBER() = 1` over a ties-bearing ORDER BY picks an
        # ARBITRARY row of each tied set, so which rows survive may differ
        # between the two executions every oracle compares. That is the same
        # exposure the already-generated ROW_NUMBER PROJECTION carries (a tied
        # ordering assigns the numbers arbitrarily too), not a new one — but it
        # only starts biting when the clause does something. RANK and DENSE_RANK
        # are tie-stable in both positions.
        if rng.random() < 0.5:
            qualify = f"{window} {rng.choice(('=', '<=', '<'))} {rng.randint(1, 4)}"
            generator.tags.add(f"qualify:{function}")
        else:
            projection.append(f"{window} AS {alias}")
            outputs.append(Column(alias, Ty.INTEGER))
            generator.tags.add(f"window:{function}")
    else:
        numeric = [c for c in relation.columns if c.ty in NUMERIC]
        if not numeric:
            return _build_projection(generator, relation)
        column = rng.choice(numeric)
        function = rng.choice(("SUM", "MIN", "MAX", "COUNT", "AVG"))
        over = partition or f"PARTITION BY {order_column.quoted} "
        projection.append(f"{function}({column.quoted}) OVER ({over.strip()}) AS {alias}")
        outputs.append(Column(alias, Ty.FLOAT if function == "AVG" else column.ty))
        generator.tags.add(f"window_agg:{function}")

    return SelectQuery(
        source=relation.sql,
        projection=projection,
        output_columns=tuple(outputs),
        qualify=qualify,
        has_ranking_window=ranking,
        tags={"window"},
    )


def _apply_order_limit(rng: random.Random, generator: Generator, query: SelectQuery) -> None:
    """Attach ORDER BY / LIMIT / OFFSET, obeying what each shape permits."""
    # BOOLEAN-valued generated expressions (Names.PREFIX aliases) were excluded here
    # for `order-by-a-boolean-expression-has-no-sort-key`. That defect is fixed, and
    # it was never the class its name claims — see the note where it was registered
    # in single_table_known_gaps. Excluding them cost the coverage that would catch
    # it coming back, which is the whole point of pinning it, so they sort again.
    sortable = [c for c in query.output_columns if c.ty in SCALAR]
    if sortable and rng.random() < 0.5:
        keys = rng.sample(sortable, rng.randint(1, min(2, len(sortable))))
        rendered = []
        for key in keys:
            clause = key.quoted
            if rng.random() < 0.7:
                clause += " " + rng.choice(("ASC", "DESC"))
            if rng.random() < 0.35:
                clause += " NULLS " + rng.choice(("FIRST", "LAST"))
                generator.tags.add("nulls_ordering")
            rendered.append(clause)
        query.order_by = rendered
        generator.tags.add("order_by")
        if len(keys) > 1:
            generator.tags.add("order_by_multikey")

    if rng.random() < 0.25:
        query.limit = rng.randint(0, 20)
        generator.tags.add("limit")
        if rng.random() < 0.4:
            query.offset = rng.randint(0, 10)
            generator.tags.add("offset")


# ── composite shapes ─────────────────────────────────────────────────────────


@dataclass
class Statement:
    """One generated statement plus what the oracles need to know about it.

    `select` is the structural form when the statement is a plain SELECT, and
    None for CTE and set-operation shapes — those get the oracles that only need
    SQL text (the COUNT(*) and optimizer-differential oracles) and are honestly
    excluded from the ones that need to rewrite the query's WHERE clause.
    """

    sql: str
    relation: Relation
    select: Optional[SelectQuery]
    deterministic_multiset: bool
    has_ranking_window: bool
    tags: Set[str]
    # A statement that is itself a `WITH ...` cannot be nested inside another
    # query: `SELECT * FROM (WITH c AS (...) SELECT ...) AS x` and a WITH inside
    # a WITH both fail to resolve the inner CTE name. Oracles that wrap the
    # statement have to know.
    is_cte: bool = False
    # Whether ANY level of the statement carries a LIMIT/OFFSET. LIMIT selects an
    # arbitrary subset (see RATIFIED/limit-and-offset-select-an-arbitrary-subset),
    # so an oracle that compares two separate executions declines them
    # structurally rather than by matching the violation text — see
    # applicable_oracles().
    contains_limit: bool = False
    contains_offset: bool = False


def generate(rng: random.Random, relation: Relation) -> Statement:
    """Generate one complete statement over `relation`."""
    names = Names()
    shape = rng.random()
    if shape < 0.10:
        return _wrap_cte(rng, relation, names)
    if shape < 0.20:
        return _wrap_set_operation(rng, relation, names)
    if shape < 0.30:
        return _wrap_subquery(rng, relation, names)

    select = build_select(rng, relation, names)
    return Statement(
        sql=select.sql,
        relation=relation,
        select=select,
        deterministic_multiset=select.row_multiset_is_determined,
        has_ranking_window=select.has_ranking_window,
        tags=select.tags,
        contains_limit=_has_limit(select),
        contains_offset=select.offset is not None,
    )


def _wrap_cte(rng: random.Random, relation: Relation, names: Names) -> Statement:
    inner = build_select(rng, relation, names)
    derived = Relation(sql="cte_source", columns=_derived_columns(inner))
    outer = build_select(rng, derived, names)
    sql = f"WITH cte_source AS ({inner.sql}) {outer.sql}"
    return Statement(
        sql=sql,
        relation=relation,
        select=None,
        deterministic_multiset=inner.row_multiset_is_determined and outer.row_multiset_is_determined,
        has_ranking_window=inner.has_ranking_window or outer.has_ranking_window,
        tags=inner.tags | outer.tags | {"cte"},
        is_cte=True,
        contains_limit=_has_limit(inner) or _has_limit(outer),
        contains_offset=inner.offset is not None or outer.offset is not None,
    )


def _wrap_subquery(rng: random.Random, relation: Relation, names: Names) -> Statement:
    inner = build_select(rng, relation, names)
    derived = Relation(sql=f"({inner.sql}) AS sub", columns=_derived_columns(inner))
    outer = build_select(rng, derived, names)
    return Statement(
        sql=outer.sql,
        relation=relation,
        select=None,
        deterministic_multiset=inner.row_multiset_is_determined and outer.row_multiset_is_determined,
        has_ranking_window=inner.has_ranking_window or outer.has_ranking_window,
        tags=inner.tags | outer.tags | {"subquery"},
        contains_limit=_has_limit(inner) or _has_limit(outer),
        contains_offset=inner.offset is not None or outer.offset is not None,
    )


def _wrap_set_operation(rng: random.Random, relation: Relation, names: Names) -> Statement:
    """UNION / INTERSECT / EXCEPT over two same-shape legs.

    Both legs project the same columns so the set operation is well-typed; the
    legs differ in their WHERE clause, which is what makes the result
    interesting rather than trivially equal to one leg.
    """
    generator = Generator(rng, relation, names)
    candidates = _scalar_columns(relation)
    if not candidates:
        raise AssertionError(f"relation {relation.sql!r} exposes no scalar column to project")
    columns = rng.sample(candidates, rng.randint(1, min(2, len(candidates))))
    projection = ", ".join(column.quoted for column in columns)

    left_where = generator.predicate()
    right_where = generator.predicate()
    operator = rng.choice(("UNION", "UNION ALL", "INTERSECT", "EXCEPT", "INTERSECT ALL", "EXCEPT ALL"))
    sql = (
        f"SELECT {projection} FROM {relation.sql} WHERE {left_where} "
        f"{operator} "
        f"SELECT {projection} FROM {relation.sql} WHERE {right_where}"
    )
    return Statement(
        sql=sql,
        relation=relation,
        select=None,
        deterministic_multiset=True,
        has_ranking_window=False,
        tags=generator.tags | {"set_operation", f"setop:{operator}"},
    )


def _has_limit(query: SelectQuery) -> bool:
    return query.limit is not None or query.offset is not None


def _derived_columns(query: SelectQuery) -> Tuple[Column, ...]:
    """The columns a subquery/CTE exposes to the level above.

    `SELECT *` forwards the source's columns; anything else exposes exactly the
    projection's outputs. Getting this wrong produces ColumnNotFoundError at the
    outer level, which would look like an engine bug and is not one.
    """
    if query.projection == ["*"]:
        return query.output_columns
    return tuple(column for column in query.output_columns if _is_simple_name(column.name))


def _is_simple_name(name: str) -> bool:
    return name.replace("_", "").isalnum()


# ─────────────────────────────────────────────────────────────────────────────
# The corpus
# ─────────────────────────────────────────────────────────────────────────────

# (relation, selection weight). Weights, not a uniform choice: the oracles run
# several queries per case, so the cheap relation has to dominate or the nightly
# 100,000-iteration run never finishes. `wide` is the only relation that crosses
# a morsel boundary, so it still has to appear often enough to matter.
CORPUS: Tuple[Tuple[str, int], ...] = (
    ("testdata.fuzzing.mixed", 40),  # every type, NULL-heavy, 2,000 rows
    ("testdata.fuzzing.wide", 10),  # 200,000 rows / 4 morsels / 4 row groups
    ("testdata.planets", 10),  # 9 rows: the degenerate single-morsel case
    ("testdata.satellites", 10),
    ("testdata.missions", 15),  # TIMESTAMP, 4,630 rows, real-world skew
    ("testdata.astronauts", 15),  # DATE, VARBINARY(JSON), ARRAY, real NULLs
)

_RELATION_CACHE: Dict[str, Relation] = {}


def load_relation(name: str) -> Relation:
    """Read a relation's schema from the engine.

    Asking the engine rather than carrying a hardcoded schema means the fuzzer
    cannot drift from the test data: a column that changes type changes what
    gets generated, immediately. A relation that returns no rows is an error,
    not an empty corpus entry — a fuzzer quietly running against nothing is the
    failure mode this whole rewrite exists to remove.
    """
    cached = _RELATION_CACHE.get(name)
    if cached is not None:
        return cached

    import opteryx

    session = opteryx.session()
    morsels = list(session.execute_to_morsels(f"SELECT * FROM {name} LIMIT 1"))
    if not morsels:
        raise AssertionError(f"fuzz corpus relation {name!r} returned no rows")

    columns = []
    for column_name, physical in morsels[0].schema.items():
        ty = _DRAKEN_TO_TY.get(physical.name)
        if ty is None:
            raise AssertionError(
                f"{name}.{column_name} has DrakenType {physical.name}, which the fuzzer's type "
                f"lattice does not cover — add it to _DRAKEN_TO_TY rather than skipping the column"
            )
        columns.append(Column(column_name, ty))

    relation = Relation(sql=name, columns=tuple(columns))
    _RELATION_CACHE[name] = relation
    return relation


def choose_relation(rng: random.Random) -> Relation:
    names = [name for name, _ in CORPUS]
    weights = [weight for _, weight in CORPUS]
    return load_relation(rng.choices(names, weights=weights, k=1)[0])
