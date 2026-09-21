// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// See the License at http://www.apache.org/licenses/LICENSE-2.0
// Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

//! The aside parser — Opteryx-only statements, parsed BESIDE sqlparser.
//!
//! See `docs/ASIDE_PARSER_DESIGN.md`. The short version:
//!
//! sqlparser has no grammar for a handful of statements this engine supports
//! (`CREATE TASK`, `REFRESH`, trigger DDL, ...). They used to be recognised in
//! Python by regex, in `opteryx/planner/pre_parse.py`, which synthesized the
//! planner's dict directly. That bypassed the parser, and so bypassed
//! everything the parser provides: quoted identifiers, source positions,
//! parameter placeholders, and `@@name` parts inside a relation name. Each had
//! to be re-implemented by hand there, or was simply missing.
//!
//! These grammars are FLAT — keyword sequences with name and literal slots — so
//! this is a state machine over a token vector, not a parser. The tokens come
//! from sqlparser's own tokenizer, which is where the four facilities above
//! actually live; we never write a tokenizer of our own.
//!
//! ```text
//! sql ─► Tokenizer (sqlparser, our dialect) ─► Vec<TokenWithSpan>
//!         ├─ leading tokens name an Opteryx form ─► state machine ─► OpteryxOnly
//!         └─ otherwise ───────────────────────────► Parser ─────────► Statement
//! ```
//!
//! What this is NOT: a fork of sqlparser (the Python boundary is serde dicts,
//! so a fork buys nothing the planner can see), and not another production on
//! the `Dialect::parse_statement` hook (that hook returns `Statement`, which is
//! exactly what forces an Opteryx statement to travel disguised as an
//! `AlterTable` carrying properties — fine for tag DDL, which really is an
//! ALTER TABLE, wrong for `CREATE TASK`, which is not).

mod admin;
mod cursor;
mod grant;
mod listen;
mod task;
mod trigger;
mod view;

use serde::Serialize;

use sqlparser::ast::Statement;
use sqlparser::dialect::Dialect;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Token, TokenWithSpan, Tokenizer};

pub use cursor::Cursor;

/// One statement: either sqlparser's, or ours.
///
/// `untagged` on the outside and externally tagged on the inside is what makes
/// the Python side see no difference. An upstream statement serialises exactly
/// as it does today (`{"Query": ...}`), and one of ours arrives as a new
/// top-level key (`{"CreateTask": ...}`) whose fields are sqlparser's own node
/// types. Same vocabulary, new words.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum OpteryxStatement {
    Sql(Box<Statement>),
    Opteryx(OpteryxOnly),
}

/// The statements sqlparser has no grammar for.
///
/// Externally tagged, like `sqlparser::ast::Statement` itself, so the planner's
/// dispatch table keys off the variant name exactly as it does for upstream
/// statements.
// Each variant name IS the serialised key the Python planner dispatches on
// (`QUERY_BUILDERS["CreateTask"]`), so these are a contract, not decoration.
#[derive(Debug, Serialize)]
pub enum OpteryxOnly {
    CreateTask(task::CreateTask),
    DropTask(task::DropTask),
    AlterTask(task::AlterTask),
    CreateTrigger(trigger::CreateTrigger),
    DropTrigger(trigger::DropTrigger),
    AlterTriggerSuspended(trigger::AlterTriggerSuspended),
    AlterTriggerOwner(trigger::AlterTriggerOwner),
    AlterTriggerMinimumInterval(trigger::AlterTriggerMinimumInterval),
    RefreshMaterializedView(view::RefreshMaterializedView),
    AlterMaterializedViewOwner(view::AlterMaterializedViewOwner),
    AlterMaterializedViewSuspended(view::AlterMaterializedViewSuspended),
    SaveResults(view::SaveResults),
    Listen(listen::Listen),
    Unlisten(listen::Unlisten),
    ShowListeners(listen::ShowListeners),
    GrantAccess(grant::GrantAccess),
    RevokeAccess(grant::RevokeAccess),
    ShowGrantsOn(grant::ShowGrantsOn),
    ShowEffectiveGrantsOn(grant::ShowEffectiveGrantsOn),
    DropStatistics(admin::DropStatistics),
    AlterWorkspaceSecure(admin::AlterWorkspaceSecure),
    ShowCreate(admin::ShowCreate),
    ResyncRelation(admin::ResyncRelation),
    DetachRelation(admin::DetachRelation),
}

/// Tokenize `sql`, then run each statement through the aside productions before
/// handing it to sqlparser.
///
/// The loop is `Parser::parse_statements` with one dispatch inserted: the same
/// delimiter handling, the same "expected end of statement" refusal, so a batch
/// behaves identically whether or not it contains one of ours.
pub fn parse_statements(
    dialect: &dyn Dialect,
    sql: &str,
) -> Result<Vec<OpteryxStatement>, ParserError> {
    // Whitespace and comments are dropped here rather than skipped at every
    // peek. `Parser::advance_token` skips them too, so handing the filtered
    // vector to sqlparser is equivalent - and it makes `Parser::index()` count
    // the same tokens this cursor does, which is what lets the loop resume
    // after sqlparser has consumed a statement.
    let tokens: Vec<TokenWithSpan> = Tokenizer::new(dialect, sql)
        .tokenize_with_location()
        .map_err(|err| ParserError::TokenizerError(err.to_string()))?
        .into_iter()
        .filter(|t| !matches!(t.token, Token::Whitespace(_)))
        .collect();

    let mut out: Vec<OpteryxStatement> = Vec::new();
    let mut cursor = Cursor::new(&tokens, sql);

    loop {
        while cursor.take_token(&Token::SemiColon) {}
        if cursor.at_end() {
            break;
        }

        let start = cursor.index();
        // Each family gates on its own opening keyword pair and rewinds on a
        // miss, so the order of these is not significant - `CREATE TASK` and
        // `CREATE TRIGGER` differ at the second token.
        let mut recognised = task::parse(&mut cursor, dialect)?;
        if recognised.is_none() {
            recognised = trigger::parse(&mut cursor)?;
        }
        if recognised.is_none() {
            recognised = view::parse(&mut cursor)?;
        }
        if recognised.is_none() {
            recognised = listen::parse(&mut cursor)?;
        }
        if recognised.is_none() {
            recognised = grant::parse(&mut cursor)?;
        }
        if recognised.is_none() {
            recognised = admin::parse(&mut cursor)?;
        }
        match recognised {
            Some(statement) => out.push(OpteryxStatement::Opteryx(statement)),
            None => {
                // Not one of ours. Hand sqlparser the remaining tokens, let it
                // parse ONE statement, and advance by however many it consumed.
                // `Parser::index()` is what makes resuming possible without
                // re-tokenizing.
                cursor.seek(start);
                let tail = cursor.remaining().to_vec();
                let mut parser = Parser::new(dialect).with_tokens_with_locations(tail);
                let statement = parser.parse_statement()?;
                cursor.advance(parser.index());
                out.push(OpteryxStatement::Sql(Box::new(statement)));
            }
        }

        // Exactly `parse_statements`' rule: a statement must be followed by a
        // delimiter or the end of input.
        if !cursor.at_end() && !cursor.peek_is(&Token::SemiColon) {
            return Err(cursor.expected("end of statement"));
        }
    }

    Ok(out)
}

/// The tokens a statement spans, as the reader wrote them.
///
/// Used where a statement carries another statement as TEXT rather than as a
/// parsed node - a task's body. See `task::CreateTask::statement`.
pub(crate) fn source_slice(sql: &str, tokens: &[TokenWithSpan]) -> String {
    if tokens.is_empty() {
        return String::new();
    }
    let start = offset_of(sql, tokens[0].span.start.line, tokens[0].span.start.column);
    let last = &tokens[tokens.len() - 1];
    let end = offset_of(sql, last.span.end.line, last.span.end.column);
    sql.chars().skip(start).take(end.saturating_sub(start)).collect()
}

/// The 0-based CHARACTER offset of a 1-based (line, column).
///
/// Characters, not bytes: sqlparser's tokenizer counts columns in chars, and a
/// statement containing a multi-byte character would otherwise slice mid-glyph.
/// Mirrors `opteryx.utils.sql.offset_of` on the Python side.
fn offset_of(text: &str, line: u64, column: u64) -> usize {
    let mut offset = 0usize;
    let mut current_line = 1u64;
    for ch in text.chars() {
        if current_line == line {
            break;
        }
        offset += 1;
        if ch == '\n' {
            current_line += 1;
        }
    }
    let total = text.chars().count();
    (offset + column.saturating_sub(1) as usize).min(total)
}
