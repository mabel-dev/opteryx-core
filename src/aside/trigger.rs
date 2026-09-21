// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// See the License at http://www.apache.org/licenses/LICENSE-2.0
// Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

//! Trigger DDL.
//!
//! ```text
//! CREATE [OR REPLACE] TRIGGER [IF NOT EXISTS] <name> ON <table> EXECUTE <task>
//! CREATE [OR REPLACE] TRIGGER [IF NOT EXISTS] <name>
//!     ON SCHEDULE '<cron>' [AT TIME ZONE '<zone>'] [OVER <table>] EXECUTE <task>
//! CREATE [OR REPLACE] TRIGGER [IF NOT EXISTS] <name>
//!     ON SIGNAL [OVER <table>] EXECUTE <task>
//! DROP TRIGGER [IF EXISTS] <name> ON <table>
//! ALTER TRIGGER <name> ON <table> SUSPEND | RESUME
//! ALTER TRIGGER <name> ON <table> OWNER TO <principal> | CURRENT_USER
//! ALTER TRIGGER <name> ON <table> SET MINIMUM INTERVAL TO <n> [SECONDS|MINUTES]
//! ```
//!
//! The richest grammar in the pre-parse layer, and the reason it is second to
//! move: three CREATE forms told apart by the word after `ON`, and an ALTER
//! that branches three ways on the word after the holder. Regexes could only
//! express that as four patterns tried in order, with two more patterns whose
//! only job was to make a failure name the right form (`_TRIGGER_EVENT_LEAD`,
//! `_CREATE_TRIGGER_COMMIT_MODIFIER_RE`). Here the branch is where the reader
//! writes it, so those two exist as the ordinary `else` of a `match`.
//!
//! `table` is the HOLDER the trigger lives under: the dataset for a commit
//! trigger; the TASK ITSELF for a schedule or signal trigger, which has no
//! source dataset to hang off. The grammar does not try to tell a dataset from
//! a task, and neither does the planner - the binder asks the connector which
//! the name is.
//!
//! sqlparser HAS `CreateTrigger`/`DropTrigger`, and they are not used: upstream's
//! is the Postgres shape (`BEFORE INSERT ... FOR EACH ROW EXECUTE FUNCTION`),
//! gated behind a hard-coded `dialect_of!` with no trait flag. Its struct has no
//! slot for a cron expression, a time zone or a window source, so filling it in
//! would be the same smuggling the `Dialect` hook forces - see the module note
//! in `super`.

use serde::Serialize;

use sqlparser::ast::{Ident, ObjectName};
use sqlparser::parser::ParserError;

use super::cursor::{grammar_error, Cursor, ValueSlot};
use super::OpteryxOnly;

/// A cron expression has five whitespace-separated fields. Checked for SHAPE
/// only: the catalog parses it properly when the trigger is armed, and a
/// malformed field is refused there before anything is stored.
const CRON_FIELD_COUNT: usize = 5;

const COMMIT_GRAMMAR: &str = "Expected: **CREATE** [**OR REPLACE**] **TRIGGER** \
[**IF NOT EXISTS**] <name> **ON** <table> **EXECUTE** <task>, or **ON SCHEDULE** \
'<cron>' [**AT TIME ZONE** '<zone>'] [**OVER** <table>], or **ON SIGNAL** \
[**OVER** <table>]. The table is the dataset whose commits fire it; the task is \
what it runs.";

const SCHEDULE_GRAMMAR: &str = "Expected: **CREATE** [**OR REPLACE**] **TRIGGER** \
[**IF NOT EXISTS**] <name> **ON SCHEDULE** '<cron>' [**AT TIME ZONE** '<zone>'] \
[**OVER** <table>] **EXECUTE** <task>. The cron expression has five fields \
(minute, hour, day of month, month, day of week); the table, if given, is the \
dataset the run is windowed over.";

const SIGNAL_GRAMMAR: &str = "Expected: **CREATE** [**OR REPLACE**] **TRIGGER** \
[**IF NOT EXISTS**] <name> **ON SIGNAL** [**OVER** <table>] **EXECUTE** <task>. \
A signal carries no schedule and no time zone; the table, if given, is the \
dataset the run is windowed over.";

const BOTH_GUARDS: &str = "**CREATE TRIGGER** cannot combine **OR REPLACE** and \
**IF NOT EXISTS** - the first always redefines, the second only ever no-ops.";

const DROP_GRAMMAR: &str = "Expected: DROP TRIGGER [IF **EXISTS**] <name> ON \
<table> (no CASCADE/RESTRICT; the table name is required)";

const ALTER_GRAMMAR: &str = "Expected: **ALTER TRIGGER** <name> **ON** <table> \
**SUSPEND**|**RESUME**, **... OWNER TO** <principal>, or \
**... SET MINIMUM INTERVAL TO** <n> [**SECONDS**|**MINUTES**] (a whole number; \
0 removes the floor). What a trigger runs is changed by recreating it, not \
altered in place.";

#[derive(Debug, Serialize)]
pub struct CreateTrigger {
    pub name: Ident,
    /// The HOLDER - see the module note.
    pub table: ObjectName,
    pub task: ObjectName,
    pub or_replace: bool,
    pub if_not_exists: bool,
    /// "commit", "schedule" or "signal".
    pub event_kind: &'static str,
    pub schedule: Option<String>,
    pub time_zone: Option<String>,
    /// The dataset a clock- or signal-fired run is windowed over. A fourth
    /// relation-bearing key, distinct from `table`, because a schedule trigger
    /// names two relations that mean different things.
    pub window_source: Option<ObjectName>,
}

#[derive(Debug, Serialize)]
pub struct DropTrigger {
    pub name: Ident,
    pub table: ObjectName,
    pub if_exists: bool,
}

#[derive(Debug, Serialize)]
pub struct AlterTriggerSuspended {
    pub name: Ident,
    pub table: ObjectName,
    pub suspended: bool,
}

#[derive(Debug, Serialize)]
pub struct AlterTriggerOwner {
    pub name: Ident,
    pub table: ObjectName,
    pub new_owner: ValueSlot,
    pub owner_is_current_user: bool,
}

#[derive(Debug, Serialize)]
pub struct AlterTriggerMinimumInterval {
    pub name: Ident,
    pub table: ObjectName,
    /// Already reduced to seconds: MINUTES is converted here so one field
    /// reaches the planner. SECONDS is the unit the catalog stores.
    pub minimum_interval_seconds: u64,
}

pub fn parse(cursor: &mut Cursor) -> Result<Option<OpteryxOnly>, ParserError> {
    let start = cursor.index();

    if cursor.peek_word("CREATE") {
        cursor.advance(1);
        let or_replace = cursor.take_words(&["OR", "REPLACE"]);
        if !cursor.take_word("TRIGGER") {
            cursor.seek(start);
            return Ok(None);
        }
        return Ok(Some(OpteryxOnly::CreateTrigger(create(cursor, or_replace)?)));
    }

    if cursor.peek_word("DROP") {
        cursor.advance(1);
        if !cursor.take_word("TRIGGER") {
            cursor.seek(start);
            return Ok(None);
        }
        return Ok(Some(OpteryxOnly::DropTrigger(drop(cursor)?)));
    }

    if cursor.peek_word("ALTER") {
        cursor.advance(1);
        if !cursor.take_word("TRIGGER") {
            cursor.seek(start);
            return Ok(None);
        }
        return alter(cursor).map(Some);
    }

    Ok(None)
}

fn create(cursor: &mut Cursor, or_replace: bool) -> Result<CreateTrigger, ParserError> {
    let if_not_exists = cursor.take_words(&["IF", "NOT", "EXISTS"]);
    if or_replace && if_not_exists {
        return Err(grammar_error(BOTH_GUARDS));
    }
    let name = cursor.identifier(COMMIT_GRAMMAR)?;
    cursor.expect_word("ON", COMMIT_GRAMMAR)?;

    // The word after ON picks the event, and picking it here is what lets a
    // malformed schedule form be refused AS a schedule form rather than as a
    // commit form missing its table.
    for not_an_event in ["EVERY", "EVENT"] {
        if cursor.peek_word(not_an_event) {
            // `ON EVERY <interval>` is a schedule spelled without a cron
            // expression, `ON EVENT <name>` a signal spelled with one. Refused
            // by name so the refusal can say what the forms ARE.
            return Err(grammar_error(&format!(
                "**ON {not_an_event}** is not a trigger event. A trigger fires on a \
                 commit (**ON** <table>), on a clock (**ON SCHEDULE** '<cron>' \
                 [**AT TIME ZONE** '<zone>'] [**OVER** <table>]) or on a signal \
                 (**ON SIGNAL** [**OVER** <table>]), and then **EXECUTE** <task>."
            )));
        }
    }

    if cursor.take_word("SCHEDULE") {
        let schedule = cursor.string_literal(SCHEDULE_GRAMMAR)?;
        let trimmed = schedule.trim().to_string();
        if trimmed.split_whitespace().count() != CRON_FIELD_COUNT {
            return Err(grammar_error(&format!(
                "**ON SCHEDULE** '{trimmed}' is not a cron expression: expected \
                 {CRON_FIELD_COUNT} whitespace-separated fields (minute, hour, day \
                 of month, month, day of week), e.g. '0 * * * *' for every hour."
            )));
        }
        let time_zone = if cursor.take_words(&["AT", "TIME", "ZONE"]) {
            Some(cursor.string_literal(SCHEDULE_GRAMMAR)?)
        } else {
            None
        };
        let window_source = window(cursor, SCHEDULE_GRAMMAR)?;
        let task = execute(cursor, SCHEDULE_GRAMMAR)?;
        return Ok(CreateTrigger {
            name,
            // A schedule trigger has no source dataset, so it lives under the
            // task it fires - the holder IS the task.
            table: task.clone(),
            task,
            or_replace,
            if_not_exists,
            event_kind: "schedule",
            schedule: Some(trimmed),
            time_zone,
            window_source,
        });
    }

    if cursor.take_word("SIGNAL") {
        let window_source = window(cursor, SIGNAL_GRAMMAR)?;
        let task = execute(cursor, SIGNAL_GRAMMAR)?;
        return Ok(CreateTrigger {
            name,
            table: task.clone(),
            task,
            or_replace,
            if_not_exists,
            event_kind: "signal",
            schedule: None,
            time_zone: None,
            window_source,
        });
    }

    let table = cursor.object_name(COMMIT_GRAMMAR)?;
    // The commit form takes neither modifier: a commit supplies its own window
    // (the commit itself) and happens in no time zone. Named in the refusal
    // rather than reported as a stray token.
    let modifier = if cursor.peek_word("OVER") {
        Some("OVER")
    } else if cursor.peek_word("AT") {
        Some("AT TIME ZONE")
    } else {
        None
    };
    if let Some(spelled) = modifier {
        return Err(grammar_error(&format!(
            "**{spelled}** does not apply to a commit trigger. A trigger **ON** \
             <table> is windowed by the commit that fires it and fires at the moment \
             of that commit; **OVER** and **AT TIME ZONE** belong to the **ON SCHEDULE** \
             and **ON SIGNAL** forms, which have no commit to take either from."
        )));
    }
    let task = execute(cursor, COMMIT_GRAMMAR)?;
    Ok(CreateTrigger {
        name,
        table,
        task,
        or_replace,
        if_not_exists,
        event_kind: "commit",
        schedule: None,
        time_zone: None,
        window_source: None,
    })
}

fn window(cursor: &mut Cursor, grammar: &str) -> Result<Option<ObjectName>, ParserError> {
    if cursor.take_word("OVER") {
        Ok(Some(cursor.object_name(grammar)?))
    } else {
        Ok(None)
    }
}

fn execute(cursor: &mut Cursor, grammar: &str) -> Result<ObjectName, ParserError> {
    cursor.expect_word("EXECUTE", grammar)?;
    let task = cursor.object_name(grammar)?;
    end_of_statement(cursor, grammar)?;
    Ok(task)
}

fn drop(cursor: &mut Cursor) -> Result<DropTrigger, ParserError> {
    let if_exists = cursor.take_words(&["IF", "EXISTS"]);
    let name = cursor.identifier(DROP_GRAMMAR)?;
    cursor.expect_word("ON", DROP_GRAMMAR)?;
    let table = cursor.object_name(DROP_GRAMMAR)?;
    end_of_statement(cursor, DROP_GRAMMAR)?;
    Ok(DropTrigger {
        name,
        table,
        if_exists,
    })
}

fn alter(cursor: &mut Cursor) -> Result<OpteryxOnly, ParserError> {
    let name = cursor.identifier(ALTER_GRAMMAR)?;
    cursor.expect_word("ON", ALTER_GRAMMAR)?;
    let table = cursor.object_name(ALTER_GRAMMAR)?;

    for (word, suspended) in [("SUSPEND", true), ("RESUME", false)] {
        if cursor.take_word(word) {
            end_of_statement(cursor, ALTER_GRAMMAR)?;
            return Ok(OpteryxOnly::AlterTriggerSuspended(AlterTriggerSuspended {
                name,
                table,
                suspended,
            }));
        }
    }

    if cursor.take_words(&["OWNER", "TO"]) {
        // Runs to the end of the statement: a principal is text, not an
        // identifier. See `Cursor::value_slot_tail`.
        let owner = cursor.value_slot_tail(ALTER_GRAMMAR)?;
        return Ok(OpteryxOnly::AlterTriggerOwner(AlterTriggerOwner {
            name,
            table,
            new_owner: owner.value,
            owner_is_current_user: owner.is_current_user,
        }));
    }

    if cursor.take_words(&["SET", "MINIMUM", "INTERVAL", "TO"]) {
        // The value is a LITERAL non-negative integer, deliberately not a value
        // slot: how often unattended work may run is a property of the
        // trigger's definition, not something runtime data decides.
        let value = cursor.unsigned_integer(ALTER_GRAMMAR)?;
        let minutes = cursor.take_word("MINUTES") || cursor.take_word("MINUTE");
        if !minutes {
            // SECONDS is the default unit and the one the catalog stores.
            let _ = cursor.take_word("SECONDS") || cursor.take_word("SECOND");
        }
        end_of_statement(cursor, ALTER_GRAMMAR)?;
        return Ok(OpteryxOnly::AlterTriggerMinimumInterval(
            AlterTriggerMinimumInterval {
                name,
                table,
                minimum_interval_seconds: if minutes { value.saturating_mul(60) } else { value },
            },
        ));
    }

    Err(grammar_error(ALTER_GRAMMAR))
}

/// Nothing may follow. Every one of these grammars accepts a fixed number of
/// clauses, so a trailing word (`CASCADE`, a stray modifier) is refused BY NAME
/// with the form quoted back, exactly as the anchored regexes did.
fn end_of_statement(cursor: &Cursor, grammar: &str) -> Result<(), ParserError> {
    if cursor.at_end() || cursor.peek_is(&sqlparser::tokenizer::Token::SemiColon) {
        Ok(())
    } else {
        Err(grammar_error(grammar))
    }
}
