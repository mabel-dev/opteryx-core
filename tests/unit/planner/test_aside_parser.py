"""
The aside parser's productions: task DDL (step 1), trigger DDL (step 2), the
materialized-view surface and SAVE (step 3), subscriptions (step 4), access
administration (step 5), and the remainder (step 6).

With step 6 `opteryx/planner/pre_parse.py` is DELETED: there is one front door.

These moved out of `opteryx/planner/pre_parse.py`, where they were matched by
regex, into a state machine over sqlparser's tokens (`src/aside/`). See
`docs/ASIDE_PARSER_DESIGN.md`.

`tests/storage/test_create_task_ddl.py` and `test_triggers_ddl.py` already pin
what these statements DO. This file pins what the move bought and what it must
not have broken: names the regex could not express, statements that are not
ours passing through untouched, and the one thing the move deliberately
refuses.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.models import ExecutionContext
from opteryx.planner.ast_rewriter import do_ast_rewriter
from opteryx.planner.sql_rewriter import do_sql_rewrite
from opteryx.third_party import sqloxide


def parse(statement: str):
    return sqloxide.parse_sql(do_sql_rewrite(statement), _dialect="opteryx")


def rewrite(statement: str, user: str = "alice"):
    context = ExecutionContext(query_id="test", user=user, billing_account="acme")
    return do_ast_rewriter(parse(statement), [], context.variables)


def name_of(statement: dict, key: str = "name"):
    root = next(iter(statement))
    return ".".join(part["Identifier"]["value"] for part in statement[root][key])


# --- the shape the planner reads -------------------------------------------


def test_create_task_carries_an_object_name_not_a_string():
    # The point of the move: a name is sqlparser's `ObjectName`, so every part
    # is an `Identifier` that carries its own quoting and span.
    statement = parse("CREATE TASK ws.copier AS SELECT 1")[0]
    assert "CreateTask" in statement
    assert name_of(statement) == "ws.copier"
    assert statement["CreateTask"]["table"] is None
    assert statement["CreateTask"]["or_replace"] is False
    assert statement["CreateTask"]["if_not_exists"] is False


def test_the_on_clause_is_an_object_name_too():
    statement = parse("CREATE OR REPLACE TASK ws.t ON ws.src AS SELECT 1")[0]
    assert name_of(statement, "table") == "ws.src"
    assert statement["CreateTask"]["or_replace"] is True


def test_drop_and_alter_carry_the_same_shape():
    dropped = parse("DROP TASK IF EXISTS ws.t")[0]
    assert name_of(dropped) == "ws.t"
    assert dropped["DropTask"]["if_exists"] is True

    altered = parse("ALTER TASK ws.t AS SELECT 1")[0]
    assert name_of(altered) == "ws.t"
    assert altered["AlterTask"]["statement"] == "SELECT 1"


# --- what the regex could not express --------------------------------------


def test_a_quoted_name_part_is_accepted():
    # The regex name slot was `[A-Za-z_][\w.$]*`, which admits no backtick - and
    # a backtick is the ONLY way to write a hyphenated name in this dialect (see
    # OpteryxDialect::is_identifier_part). So this was unwritable.
    statement = parse("DROP TASK personal.ada.`my-task`")[0]
    assert name_of(statement) == "personal.ada.my-task"
    parts = statement["DropTask"]["name"]
    assert parts[-1]["Identifier"]["quote_style"] == "`"


def test_a_session_variable_in_the_task_name_resolves():
    # The reason this work started: `@@external_user` is substituted by the AST
    # rewriter, which walks ObjectName positions. A regex-synthesized statement
    # never became an AST, so it never reached the rewriter.
    statement = rewrite("CREATE TASK personal.@@external_user.add_body AS SELECT 1")[0]
    assert name_of(statement) == "personal.alice.add_body"


def test_a_session_variable_in_the_on_clause_resolves():
    statement = rewrite("CREATE TASK personal.@@external_user.t ON personal.@@external_user.src AS SELECT 1")[0]
    assert name_of(statement, "table") == "personal.alice.src"


def test_a_task_statement_can_sit_in_a_batch():
    # The regexes anchored on `$`, so a task statement could not be followed by
    # anything. The token loop ends a statement at its semicolon like any other.
    statements = parse("CREATE TASK ws.t AS SELECT 1; SELECT 2")
    assert len(statements) == 2
    assert "CreateTask" in statements[0]
    assert "Query" in statements[1]
    assert statements[0]["CreateTask"]["statement"] == "SELECT 1"


# --- the body is text, and stays the reader's own text ---------------------


def test_the_body_is_the_readers_own_bytes():
    # Not regenerated from a parsed node: the SQL rewriter lifts a temporal
    # clause out of the text before the parser sees it, so a regenerated body
    # would silently lose one. See ASIDE_PARSER_DESIGN.md §9.1.
    body = "INSERT  INTO ws.sink   SELECT a FROM ws.src WHERE a > :low"
    statement = parse(f"CREATE TASK ws.copier AS {body}")[0]
    assert statement["CreateTask"]["statement"] == body


def test_a_trailing_semicolon_is_not_part_of_the_body():
    statement = parse("CREATE TASK ws.t AS SELECT 1;")[0]
    assert statement["CreateTask"]["statement"] == "SELECT 1"


def test_a_semicolon_inside_a_literal_does_not_end_the_body():
    # Token-level, not text-level: a `;` in a string is a literal token.
    statement = parse("CREATE TASK ws.t AS SELECT 'a;b' AS x")[0]
    assert statement["CreateTask"]["statement"] == "SELECT 'a;b' AS x"


def test_a_variable_in_the_body_is_refused():
    # A body is stored as text and re-parsed when the task fires, as whoever the
    # trigger names. Resolving it now pins the author into it; leaving it pins
    # nobody. Neither is chosen silently - see ASIDE_PARSER_DESIGN.md §9.1.
    session = opteryx.session(user="alice")
    with pytest.raises(UnsupportedSyntaxError, match="cannot be used inside the statement a task runs"):
        list(
            session.execute_to_morsels(
                "CREATE TASK ws.t AS INSERT INTO personal.@@external_user.log SELECT 1"
            )
        )


# --- refusals keep their wording -------------------------------------------


@pytest.mark.parametrize(
    "statement, expected",
    [
        ("CREATE TASK ws.t", "A task is a statement the platform runs for you"),
        ("CREATE TASK ws.t AS", "A task is a statement the platform runs for you"),
        ("DROP TASK ws.t CASCADE", "a task owns no storage"),
        ("ALTER TASK ws.t ON ws.src AS SELECT 1", "it takes no **ON** <table>"),
        (
            "CREATE OR REPLACE TASK IF NOT EXISTS ws.t AS SELECT 1",
            "cannot combine **OR REPLACE** and **IF NOT EXISTS**",
        ),
    ],
)
def test_a_malformed_task_statement_is_refused_by_name(statement, expected):
    # The aside parser has one error channel to Python, so a GRAMMAR refusal
    # marks itself and is re-typed here. If that mapping breaks these arrive as
    # QueryParseError with sqlparser's own wording instead.
    session = opteryx.session(user="alice")
    with pytest.raises(UnsupportedSyntaxError) as refusal:
        list(session.execute_to_morsels(statement))
    assert expected in str(refusal.value)
    assert "OPTERYX-SYNTAX" not in str(refusal.value)


# --- trigger DDL: three CREATE forms, three ALTER branches -----------------


def test_the_commit_form_holds_the_trigger_under_its_dataset():
    statement = parse("CREATE TRIGGER tick ON ws.t EXECUTE ws.job")[0]["CreateTrigger"]
    assert statement["name"]["value"] == "tick"
    assert statement["event_kind"] == "commit"
    assert ".".join(p["Identifier"]["value"] for p in statement["table"]) == "ws.t"
    assert ".".join(p["Identifier"]["value"] for p in statement["task"]) == "ws.job"
    assert statement["schedule"] is None
    assert statement["window_source"] is None


def test_the_schedule_form_holds_the_trigger_under_its_task():
    # A clock trigger has no source dataset to hang off, so the HOLDER is the
    # task itself - `table` and `task` are deliberately the same name here.
    statement = parse(
        "CREATE OR REPLACE TRIGGER tick ON SCHEDULE '0 * * * *' "
        "AT TIME ZONE 'Europe/London' OVER ws.src EXECUTE ws.job"
    )[0]["CreateTrigger"]
    assert statement["event_kind"] == "schedule"
    assert statement["schedule"] == "0 * * * *"
    assert statement["time_zone"] == "Europe/London"
    assert ".".join(p["Identifier"]["value"] for p in statement["table"]) == "ws.job"
    assert ".".join(p["Identifier"]["value"] for p in statement["window_source"]) == "ws.src"
    assert statement["or_replace"] is True


def test_the_signal_form_carries_no_schedule():
    statement = parse("CREATE TRIGGER IF NOT EXISTS tick ON SIGNAL OVER ws.src EXECUTE ws.job")[0][
        "CreateTrigger"
    ]
    assert statement["event_kind"] == "signal"
    assert statement["schedule"] is None
    assert statement["time_zone"] is None
    assert statement["if_not_exists"] is True


def test_a_doubled_quote_in_a_schedule_is_unescaped_once():
    # The tokenizer collapses `''` already; doing it again here would eat a
    # quote the reader wrote.
    statement = parse("CREATE TRIGGER t ON SCHEDULE '0 * * * *' AT TIME ZONE 'a''b' EXECUTE ws.j")[
        0
    ]["CreateTrigger"]
    assert statement["time_zone"] == "a'b"


@pytest.mark.parametrize(
    "statement, key, expected",
    [
        ("ALTER TRIGGER tick ON ws.t SUSPEND", "suspended", True),
        ("ALTER TRIGGER tick ON ws.t RESUME", "suspended", False),
    ],
)
def test_alter_trigger_suspend_and_resume(statement, key, expected):
    root = parse(statement)[0]["AlterTriggerSuspended"]
    assert root[key] is expected
    assert root["name"]["value"] == "tick"


@pytest.mark.parametrize(
    "clause, seconds",
    [
        ("TO 5 MINUTES", 300),
        ("TO 5 MINUTE", 300),
        ("TO 90 SECONDS", 90),
        ("TO 90", 90),
        ("TO 0", 0),
    ],
)
def test_minimum_interval_reduces_to_seconds(clause, seconds):
    root = parse(f"ALTER TRIGGER tick ON ws.t SET MINIMUM INTERVAL {clause}")[0][
        "AlterTriggerMinimumInterval"
    ]
    assert root["minimum_interval_seconds"] == seconds


@pytest.mark.parametrize(
    "principal, owner, current_user",
    [
        ("rhea", "rhea", False),
        ("CURRENT_USER", "CURRENT_USER", True),
        ("'svc:account'", "svc:account", False),
        ("justin.joyce@joocer.com", "justin.joyce@joocer.com", False),
    ],
)
def test_owner_to_reads_a_principal_as_text(principal, owner, current_user):
    # A principal is not an identifier: `@` and `.` are identifier characters
    # here and `:` is not, so the tokenizer splits every one of these. The slot
    # is read as SOURCE TEXT for exactly that reason.
    root = parse(f"ALTER TRIGGER tick ON ws.t OWNER TO {principal}")[0]["AlterTriggerOwner"]
    assert root["new_owner"] == owner
    assert root["owner_is_current_user"] is current_user


def test_owner_to_still_takes_a_parameter():
    # The one VALUE slot in trigger DDL. It emits the node
    # `ast_rewriter.parameter_dict_binder` binds, which is what the regex layer
    # had to hand-build with `_slot_value`.
    root = parse("ALTER TRIGGER tick ON ws.t OWNER TO :who")[0]["AlterTriggerOwner"]
    assert root["new_owner"] == {"Placeholder": ":who"}


def test_a_trigger_name_may_be_quoted_but_not_dotted():
    # Quoted: new, and the reason the move is worth making.
    assert parse("DROP TRIGGER `my-trigger` ON ws.t")[0]["DropTrigger"]["name"]["value"] == (
        "my-trigger"
    )
    # Dotted: refused, because a trigger name is unique only within its holder,
    # so `a.b` is a reader mistaking a trigger for a relation.
    with pytest.raises(ValueError, match="OPTERYX-SYNTAX"):
        parse("DROP TRIGGER a.b ON ws.t")


def test_session_variables_resolve_in_every_relation_slot_a_trigger_names():
    statement = rewrite(
        "CREATE TRIGGER tick ON SCHEDULE '0 * * * *' OVER personal.@@external_user.src "
        "EXECUTE personal.@@external_user.job"
    )[0]["CreateTrigger"]
    for key in ("table", "task", "window_source"):
        joined = ".".join(p["Identifier"]["value"] for p in statement[key])
        assert "@@" not in joined, (key, joined)
        assert "alice" in joined, (key, joined)


@pytest.mark.parametrize(
    "statement, expected",
    [
        ("CREATE TRIGGER t ON EVERY HOUR EXECUTE ws.j", "**ON EVERY** is not a trigger event"),
        ("CREATE TRIGGER t ON EVENT x EXECUTE ws.j", "**ON EVENT** is not a trigger event"),
        ("CREATE TRIGGER t ON SCHEDULE 'bad' EXECUTE ws.j", "is not a cron expression"),
        (
            "CREATE TRIGGER t ON ws.t OVER ws.s EXECUTE ws.j",
            "**OVER** does not apply to a commit trigger",
        ),
        (
            "CREATE TRIGGER t ON ws.t AT TIME ZONE 'UTC' EXECUTE ws.j",
            "**AT TIME ZONE** does not apply to a commit trigger",
        ),
        (
            "CREATE OR REPLACE TRIGGER IF NOT EXISTS t ON ws.t EXECUTE ws.j",
            "cannot combine **OR REPLACE** and **IF NOT EXISTS**",
        ),
        ("DROP TRIGGER t ON ws.t CASCADE", "no CASCADE/RESTRICT"),
        ("ALTER TRIGGER t ON ws.t FROBNICATE", "**SUSPEND**|**RESUME**"),
        ("CREATE TRIGGER t ON ws.t", "The table is the dataset whose commits fire it"),
    ],
)
def test_a_malformed_trigger_statement_is_refused_by_name(statement, expected):
    # Each CREATE form is refused AS THAT FORM: the branch happens where the
    # reader writes it, so a bad schedule does not get told it is a bad commit
    # trigger. That took two extra regexes to approximate before.
    session = opteryx.session(user="alice")
    with pytest.raises(UnsupportedSyntaxError) as refusal:
        list(session.execute_to_morsels(statement))
    assert expected in str(refusal.value)
    assert "OPTERYX-SYNTAX" not in str(refusal.value)


# --- the materialized-view surface, and SAVE -------------------------------


def test_refresh_is_the_only_refresh_statement():
    statement = parse("REFRESH MATERIALIZED VIEW ws.v")[0]["RefreshMaterializedView"]
    assert ".".join(p["Identifier"]["value"] for p in statement["name"]) == "ws.v"


@pytest.mark.parametrize(
    "statement, suspended",
    [("ALTER MATERIALIZED VIEW ws.v SUSPEND", True), ("ALTER MATERIALIZED VIEW ws.v RESUME", False)],
)
def test_alter_materialized_view_suspend_and_resume(statement, suspended):
    assert parse(statement)[0]["AlterMaterializedViewSuspended"]["suspended"] is suspended


def test_alter_materialized_view_owner_drops_the_principal_for_current_user():
    # This form is shaped differently from the trigger form beside it: it
    # carries None rather than the keyword, because there is no principal to
    # record. Kept as the planner already reads it.
    bare = parse("ALTER MATERIALIZED VIEW ws.v OWNER TO CURRENT_USER")[0][
        "AlterMaterializedViewOwner"
    ]
    assert bare["owner"] is None
    assert bare["current_user"] is True

    named = parse("ALTER MATERIALIZED VIEW ws.v OWNER TO bob")[0]["AlterMaterializedViewOwner"]
    assert named["owner"] == "bob"
    assert named["current_user"] is False


@pytest.mark.parametrize(
    "statement, root, key",
    [
        ("ALTER MATERIALIZED VIEW ws.v OWNER TO 'CURRENT_USER'", "AlterMaterializedViewOwner", "current_user"),
        ("ALTER TRIGGER t ON ws.x OWNER TO 'CURRENT_USER'", "AlterTriggerOwner", "owner_is_current_user"),
    ],
)
def test_a_quoted_current_user_is_a_principal_not_the_keyword(statement, root, key):
    # Bare CURRENT_USER means "me"; quoting it asks for a principal literally
    # named that - the usual SQL distinction, and the only way to name such a
    # principal if one exists. Both unquote to the same string, so the decision
    # has to be made on the RAW text. It was not, briefly, and the quoted form
    # was read as the keyword.
    assert parse(statement)[0][root][key] is False


def test_save_reads_a_job_handle_that_is_not_an_identifier():
    # A job id opens with a digit and carries a hyphen, so the tokenizer sees a
    # number, a minus and a word. It is one name, read as source text.
    statement = parse("SAVE RESULTS OF 20260921123456-abc123 AS ws.out")[0]["SaveResults"]
    assert statement["handle"] == "20260921123456-abc123"
    assert ".".join(p["Identifier"]["value"] for p in statement["name"]) == "ws.out"


def test_save_is_classified_but_not_planned():
    # Parsed so the jobs API can authorize it; refused here because the engine
    # does not own the results bucket. Reaching the planner is deployment skew.
    session = opteryx.session(user="alice")
    with pytest.raises(UnsupportedSyntaxError, match="run by the platform"):
        list(session.execute_to_morsels("SAVE RESULTS OF 20260921123456-abc AS ws.out"))


def test_alter_that_is_not_a_materialized_view_passes_through():
    # `ALTER` opens statements this parser does not own; the gate is all three
    # words, so a near-miss rewinds rather than claiming the statement.
    assert "AlterTable" in parse("ALTER TABLE ws.t ADD COLUMN z INTEGER")[0]
    assert "AlterTable" in parse("ALTER TABLE ws.t CREATE TAG r AS OF VERSION 7")[0]


@pytest.mark.parametrize(
    "statement, expected",
    [
        ("REFRESH VIEW ws.v", "only **REFRESH** statement"),
        ("REFRESH MATERIALIZED VIEW ws.v CASCADE", "only **REFRESH** statement"),
        ("SAVE ws.out", "only **SAVE** statement"),
        ("SAVE RESULTS OF job", "only **SAVE** statement"),
        ("ALTER MATERIALIZED VIEW ws.v FROBNICATE", "**SUSPEND**|**RESUME**"),
    ],
)
def test_a_malformed_view_statement_is_refused_by_name(statement, expected):
    session = opteryx.session(user="alice")
    with pytest.raises(UnsupportedSyntaxError) as refusal:
        list(session.execute_to_morsels(statement))
    assert expected in str(refusal.value)
    assert "OPTERYX-SYNTAX" not in str(refusal.value)


# --- subscriptions ---------------------------------------------------------


@pytest.mark.parametrize(
    "statement, outcome",
    [
        ("LISTEN TO ws.job", "EVERYTHING"),
        ("LISTEN TO ws.job FOR ERROR", "ERROR"),
        ("LISTEN TO ws.job FOR SUCCESS", "SUCCESS"),
        # Keywords are matched case-insensitively and normalised on the way out,
        # so one spelling reaches the catalog.
        ("LISTEN TO ws.job FOR everything", "EVERYTHING"),
    ],
)
def test_listen_resolves_the_outcome_filter(statement, outcome):
    body = parse(statement)[0]["Listen"]
    assert body["outcome"] == outcome
    assert ".".join(p["Identifier"]["value"] for p in body["name"]) == "ws.job"


def test_show_listeners_carries_nothing():
    # It answers for the session and takes no arguments, so the body is empty.
    assert parse("SHOW LISTENERS")[0] == {"ShowListeners": {}}


@pytest.mark.parametrize(
    "statement, key",
    [
        # Every other SHOW must rewind: sqlparser owns two of these, pre_parse
        # still owns the third, and the rest are the parser's own catch-all.
        ("SHOW COLUMNS FROM ws.t", "ShowColumns"),
        ("SHOW CREATE TABLE ws.t", "ShowCreate"),
        ("SHOW VARIABLES", "ShowVariable"),
        ("SHOW SNAPSHOTS FOR ws.t", "ShowVariable"),
    ],
)
def test_show_listeners_does_not_claim_its_neighbours(statement, key):
    assert key in parse(statement)[0]


@pytest.mark.parametrize(
    "statement, expected",
    [
        ("LISTEN ws.job", "**LISTEN TO** <task>"),
        ("LISTEN TO ws.job FOR NONSENSE", "**LISTEN TO** <task>"),
        ("UNLISTEN", "there is no wildcard form"),
        ("UNLISTEN *", "there is no wildcard form"),
        ("UNLISTEN ws.job FOR ERROR", "takes no **FOR** clause"),
        ("SHOW LISTENERS ON ws.t", "takes no arguments"),
    ],
)
def test_a_malformed_subscription_is_refused_by_name(statement, expected):
    session = opteryx.session(user="alice")
    with pytest.raises(UnsupportedSyntaxError) as refusal:
        list(session.execute_to_morsels(statement))
    assert expected in str(refusal.value)
    assert "OPTERYX-SYNTAX" not in str(refusal.value)


# --- access administration -------------------------------------------------


def test_grant_and_revoke_keep_their_keywords_literal():
    # The role and the object kind are keywords from a closed set, lowercased
    # for the planner. A parameter in either would make the SHAPE of the
    # statement - which authority, how many name parts - a runtime decision.
    granted = parse("GRANT reader ON DATASET a.b.c TO USER bob")[0]["GrantAccess"]
    assert granted == {
        "role": "reader",
        "object_kind": "dataset",
        "object_name": "a.b.c",
        "principal": "bob",
    }
    revoked = parse("REVOKE OWNER ON WORKSPACE ws FROM USER 'x@y.z'")[0]["RevokeAccess"]
    assert revoked["role"] == "owner"
    assert revoked["principal"] == "x@y.z"


def test_a_grants_object_and_principal_are_value_slots():
    # Both take placeholders - a deliberate property of this surface, and the
    # reason neither is read as an ObjectName.
    body = parse("GRANT reader ON DATASET :ds TO USER :who")[0]["GrantAccess"]
    assert body["object_name"] == {"Placeholder": ":ds"}
    assert body["principal"] == {"Placeholder": ":who"}


def test_an_identity_containing_a_quote_survives():
    body = parse("GRANT reader ON DATASET a.b.c TO USER 'o''brien'")[0]["GrantAccess"]
    assert body["principal"] == "o'brien"


def test_the_two_listings_are_two_statements():
    assert "ShowGrantsOn" in parse("SHOW GRANTS ON COLLECTION ws.coll")[0]
    assert "ShowEffectiveGrantsOn" in parse("SHOW EFFECTIVE GRANTS ON DATASET ws.c.d")[0]
    # Bare SHOW GRANTS is the session's own and belongs to the parser's
    # catch-all: the gate reads as far as `ON` before claiming anything.
    assert "ShowVariable" in parse("SHOW GRANTS")[0]


@pytest.mark.parametrize(
    "statement, expected",
    [
        ("GRANT reader ON DATASET a.b.c FROM USER bob", "mixes the two"),
        ("REVOKE reader ON DATASET a.b.c TO USER bob", "mixes the two"),
        ("GRANT admin ON DATASET a.b.c TO USER bob", "READER|WRITER|OWNER"),
        ("GRANT reader ON TABLE a.b.c TO USER bob", "WORKSPACE|COLLECTION|DATASET"),
        ("GRANT reader ON DATASET a.b.c TO bob", "**TO USER** <user>"),
        ("SHOW EFFECTIVE GRANTS", "**SHOW EFFECTIVE GRANTS ON**"),
        ("SHOW EFFECTIVE GRANTS ON TABLE ws.c.d", "WORKSPACE|COLLECTION|DATASET"),
    ],
)
def test_a_malformed_grant_is_refused_by_name(statement, expected):
    # The crossed-preposition refusal is its own message: a statement whose
    # preposition disagrees with its verb was not the one anyone meant to run,
    # and "expected TO" would not say that.
    session = opteryx.session(user="alice")
    with pytest.raises(UnsupportedSyntaxError) as refusal:
        list(session.execute_to_morsels(statement))
    assert expected in str(refusal.value)
    assert "OPTERYX-SYNTAX" not in str(refusal.value)


def test_a_grants_object_is_a_value_so_a_variable_is_not_resolved_in_it():
    # The one place `@@name` does NOT resolve, and deliberately: a grant's
    # object is a VALUE slot (it takes placeholders), not a relation name.
    # Unchanged from the regex this replaced - see src/aside/grant.rs.
    body = rewrite("GRANT reader ON DATASET personal.@@external_user.x TO USER bob")[0][
        "GrantAccess"
    ]
    assert body["object_name"] == "personal.@@external_user.x"


# --- the remainder ---------------------------------------------------------


def test_drop_statistics_reads_a_column_list():
    body = parse("DROP STATISTICS ON ws.t FOR COLUMNS a, b")[0]["DropStatistics"]
    assert ".".join(p["Identifier"]["value"] for p in body["table_name"]) == "ws.t"
    assert body["columns"] == ["a", "b"]
    # No FOR clause means every column.
    assert parse("DROP STATISTICS ON ws.t")[0]["DropStatistics"]["columns"] == []


def test_a_quoted_column_arrives_unquoted():
    # The regex stripped quote characters by hand; the tokenizer does it.
    body = parse("DROP STATISTICS ON ws.t FOR COLUMNS `odd-col`")[0]["DropStatistics"]
    assert body["columns"] == ["odd-col"]


def test_the_secure_forms_differ_by_their_destinations():
    granted = parse("ALTER WORKSPACE src SET SECURE a.b.c TO dst1, dst2")[0][
        "AlterWorkspaceSecure"
    ]
    assert granted["workspace"]["value"] == "src"
    assert [d["value"] for d in granted["destinations"]] == ["dst1", "dst2"]
    # None is DROP SECURE - withdraw the sanction, not grant it to nobody.
    dropped = parse("ALTER WORKSPACE src DROP SECURE a.b.c")[0]["AlterWorkspaceSecure"]
    assert dropped["destinations"] is None


def test_the_workspace_property_form_is_not_ours():
    # The SQL rewriter turns `ALTER WORKSPACE <ws> SET <property> TO <value>`
    # into ALTER FUNCTION before the parser sees it, and looks ahead to leave
    # the SECURE forms alone. Both halves of that have to keep working.
    assert "AlterFunction" in parse("ALTER WORKSPACE ws SET egress_protection TO OFF")[0]


@pytest.mark.parametrize(
    "statement, obj_type",
    [
        ("SHOW CREATE MATERIALIZED VIEW ws.v", "MaterializedView"),
        ("SHOW CREATE TASK ws.t", "Task"),
        ("SHOW CREATE TRIGGER tick ON ws.t", "Trigger"),
        # sqlparser's own, and they must stay its own.
        ("SHOW CREATE TABLE ws.t", "Table"),
        ("SHOW CREATE VIEW ws.v", "View"),
    ],
)
def test_show_create_is_one_key_for_five_object_types(statement, obj_type):
    # Deliberately the same `ShowCreate` key sqlparser emits, so the planner
    # has one path rather than one per spelling.
    assert parse(statement)[0]["ShowCreate"]["obj_type"] == obj_type


def test_show_create_trigger_carries_the_holder_and_the_trigger():
    body = parse("SHOW CREATE TRIGGER tick ON ws.t")[0]["ShowCreate"]
    assert body["trigger_name"]["value"] == "tick"
    assert ".".join(p["Identifier"]["value"] for p in body["obj_name"]) == "ws.t"


@pytest.mark.parametrize(
    "statement, key, force",
    [
        ("ALTER TABLE ws.f RESYNC", "ResyncRelation", False),
        ("ALTER TABLE ws.f RESYNC FORCE", "ResyncRelation", True),
        ("ALTER TABLE ws.f DETACH", "DetachRelation", None),
    ],
)
def test_fork_maintenance(statement, key, force):
    body = parse(statement)[0][key]
    assert ".".join(p["Identifier"]["value"] for p in body["relation"]) == "ws.f"
    if force is not None:
        assert body["force"] is force


def test_the_save_handle_takes_no_placeholder():
    # Read as TEXT because a job id is not identifier-shaped, but an IDENTIFIER
    # slot all the same: it names whose results land in the caller's workspace.
    # Reading it through the value-slot reader made it accept `:job` for a
    # while, which is the whole reason the check is explicit.
    session = opteryx.session(user="alice")
    with pytest.raises(UnsupportedSyntaxError):
        list(session.execute_to_morsels("SAVE RESULTS OF :job AS ws.out"))


# --- what a pre-flight is told ---------------------------------------------


@pytest.mark.parametrize(
    "statement, target",
    [
        ("DROP TRIGGER t ON ws.src", "ws.src"),
        ("REFRESH MATERIALIZED VIEW ws.v", "ws.v"),
        ("ALTER MATERIALIZED VIEW ws.v OWNER TO bob", "ws.v"),
        ("SAVE RESULTS OF 20260921123456-abc AS ws.out", "ws.out"),
        ("DROP STATISTICS ON ws.t", "ws.t"),
        ("ALTER TABLE ws.f RESYNC", "ws.f"),
        ("ALTER TABLE ws.f DETACH", "ws.f"),
    ],
)
def test_analyze_query_still_names_the_permission_target(statement, target):
    # `tables` is what the jobs API pre-flights permissions against, and
    # `_SYNTHESIZED_TARGETS` used to read only a dotted STRING - so the moment
    # a moved form started carrying an ObjectName, its target silently became
    # empty and the pre-flight checked nothing. It did, for DROP TRIGGER,
    # between steps 2 and 3.
    assert list(opteryx.analyze_query(statement)["tables"]) == [target]


# --- everything else is untouched ------------------------------------------


@pytest.mark.parametrize(
    "statement, key",
    [
        ("SELECT * FROM $planets", "Query"),
        ("CREATE TABLE ws.t (a INTEGER)", "CreateTable"),
        ("DROP TABLE ws.t", "Drop"),
        ("ALTER TABLE ws.t ADD COLUMN z INTEGER", "AlterTable"),
        # The dialect's own hook productions still run: this one is only
        # reachable through `Dialect::parse_statement`.
        ("ALTER TABLE ws.t ADD COLUMN IF NOT EXISTS z INTEGER", "AlterTable"),
        ("ALTER TABLE ws.t CREATE TAG release AS OF VERSION 7", "AlterTable"),
        ("EXECUTE ws.t USING 'x' AS a", "Execute"),
        # `TRIGGER` is sqlparser's keyword, and DROP TRIGGER is its statement -
        # the dispatch must claim it before upstream's gated version refuses it.
        ("DROP TABLE triggers", "Drop"),
    ],
)
def test_statements_that_are_not_ours_pass_through(statement, key):
    assert key in parse(statement)[0]


def test_a_word_that_merely_starts_like_a_task_statement_is_not_one():
    # `DROP TABLE` and `CREATE TABLE` share their first token with the task
    # forms; the dispatch rewinds when the second token is not TASK.
    assert "Drop" in parse("DROP TABLE tasks")[0]
    # ... and `task` is not reserved, so it stays usable as a name.
    assert "Query" in parse("SELECT task FROM ws.t")[0]


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
