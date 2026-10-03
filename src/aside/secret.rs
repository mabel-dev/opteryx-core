// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// See the License at http://www.apache.org/licenses/LICENSE-2.0
// Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

//! `CREATE SECRET`, `DROP SECRET`, `SHOW SECRETS` - customer credentials
//! (jobs.opteryx docs/design/secrets.md §2).
//!
//! ```text
//! CREATE [OR REPLACE] SECRET [IF NOT EXISTS] <name> IN <workspace>
//!     ( TYPE '<type>', <KEY> <:param | 'literal'> [, ...] )
//! DROP SECRET [IF EXISTS] <name> { IN | FROM } <workspace>
//! SHOW SECRETS { IN | FROM } <workspace>
//! ```
//!
//! sqlparser parses `CREATE SECRET` natively for DuckDB, but its option grammar
//! refuses placeholders (`URL :url` -> `Expected: identifier, found: :`), and a
//! placeholder is the form the design wants. So the statement is claimed here,
//! on the same token stream, before sqlparser sees it.
//!
//! The engine never EXECUTES `CREATE SECRET` - jobs.opteryx performs it at
//! submission - but it must recognise it, because jobs authorises a statement
//! by classifying it through `analyze_query`.
//!
//! **A literal value never leaves this module through the parse.** In the AST
//! handed to Python an option's value is its placeholder or `null`; the literal
//! text is kept only as a span (`#[serde(skip)]`), and the one way it comes out
//! is `redact`, which jobs calls to lift it. `parse_sql`'s output is logged,
//! cached and compared by code with no idea it might hold a credential, so it
//! must never hold one.
//!
//! Identifier slots (the name, the workspace, TYPE) refuse placeholders: a
//! parameterised one would let runtime data decide where a secret lands or what
//! shape it claims to be.

use std::collections::BTreeSet;

use serde::Serialize;

use sqlparser::ast::Ident;
use sqlparser::dialect::Dialect;
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::{Location, Token};

use super::cursor::{grammar_error, Cursor, Placeholder};
use super::{OpteryxOnly, OpteryxStatement};

const CREATE_GRAMMAR: &str = "Expected: **CREATE** [**OR REPLACE**] **SECRET** \
[**IF NOT EXISTS**] <name> **IN** <workspace> ( **TYPE** '<type>', <KEY> <:param | \
'value'> [, ...] ). Bind credential values as parameters (`URL :url`) rather than \
writing them into the statement.";

const DROP_GRAMMAR: &str =
    "Expected: **DROP SECRET** [**IF EXISTS**] <name> **IN** <workspace>.";

const SHOW_GRAMMAR: &str = "Expected: **SHOW SECRETS IN** <workspace>.";

const CONTRADICTION: &str =
    "**CREATE OR REPLACE SECRET ... IF NOT EXISTS** is a contradiction; use one or the other.";

const TYPE_IS_IDENTIFIER: &str = "A secret's **TYPE** is written into the statement \
('http_endpoint', 'gcs_service_account', 'aws_access_key'); it cannot be a parameter.";

const DUPLICATE_OPTION: &str = "Each option may be given once in **CREATE SECRET**.";

const REDACTED_COLLISION: &str = "Placeholders named `:redacted_<key>` are reserved \
for **CREATE SECRET**'s own use; rename the parameter.";

/// Prefix of the placeholder a lifted literal is replaced by.
const REDACTED_PREFIX: &str = "redacted_";

#[derive(Debug, Serialize)]
pub struct CreateSecret {
    pub name: Ident,
    pub workspace: Ident,
    pub secret_type: String,
    pub options: Vec<SecretOption>,
    pub or_replace: bool,
    pub if_not_exists: bool,
}

#[derive(Debug, Serialize)]
pub struct SecretOption {
    /// Upper-cased option key, as written.
    pub key: String,
    /// The placeholder bound here, or `null` where the statement wrote a
    /// literal. The literal itself is deliberately not serialised.
    pub value: Option<Placeholder>,
    /// Where a literal value sits in the source, for `redact`. Never serialised.
    #[serde(skip)]
    pub literal: Option<LiteralSpan>,
}

#[derive(Debug, Clone)]
pub struct LiteralSpan {
    pub start: Location,
    pub end: Location,
    pub value: String,
}

#[derive(Debug, Serialize)]
pub struct DropSecret {
    pub name: Ident,
    pub workspace: Ident,
    pub if_exists: bool,
}

#[derive(Debug, Serialize)]
pub struct ShowSecrets {
    pub workspace: Ident,
}

pub fn parse(cursor: &mut Cursor) -> Result<Option<OpteryxOnly>, ParserError> {
    let start = cursor.index();

    if cursor.take_word("CREATE") {
        let or_replace = cursor.take_words(&["OR", "REPLACE"]);
        if !cursor.take_word("SECRET") {
            cursor.seek(start);
            return Ok(None);
        }
        return Ok(Some(OpteryxOnly::CreateSecret(create(cursor, or_replace)?)));
    }

    if cursor.peek_word("DROP") {
        cursor.advance(1);
        if !cursor.take_word("SECRET") {
            cursor.seek(start);
            return Ok(None);
        }
        let if_exists = cursor.take_words(&["IF", "EXISTS"]);
        let name = cursor.identifier(DROP_GRAMMAR)?;
        let workspace = workspace_clause(cursor, DROP_GRAMMAR)?;
        end_of_statement(cursor, DROP_GRAMMAR)?;
        return Ok(Some(OpteryxOnly::DropSecret(DropSecret {
            name,
            workspace,
            if_exists,
        })));
    }

    if cursor.peek_word("SHOW") {
        cursor.advance(1);
        if !cursor.take_word("SECRETS") {
            cursor.seek(start);
            return Ok(None);
        }
        let workspace = workspace_clause(cursor, SHOW_GRAMMAR)?;
        end_of_statement(cursor, SHOW_GRAMMAR)?;
        return Ok(Some(OpteryxOnly::ShowSecrets(ShowSecrets { workspace })));
    }

    Ok(None)
}

/// `IN <workspace>`, or `FROM <workspace>` - DuckDB spells the storage
/// specifier IN on create and FROM on drop; both are accepted wherever one is.
fn workspace_clause(cursor: &mut Cursor, grammar: &str) -> Result<Ident, ParserError> {
    if !(cursor.take_word("IN") || cursor.take_word("FROM")) {
        return Err(grammar_error(grammar));
    }
    cursor.identifier(grammar)
}

fn create(cursor: &mut Cursor, or_replace: bool) -> Result<CreateSecret, ParserError> {
    let if_not_exists = cursor.take_words(&["IF", "NOT", "EXISTS"]);
    if or_replace && if_not_exists {
        return Err(grammar_error(CONTRADICTION));
    }
    let name = cursor.identifier(CREATE_GRAMMAR)?;
    let workspace = workspace_clause(cursor, CREATE_GRAMMAR)?;

    if !cursor.take_token(&Token::LParen) {
        return Err(grammar_error(CREATE_GRAMMAR));
    }

    // TYPE first, as every engine that has this statement writes it.
    cursor.expect_word("TYPE", CREATE_GRAMMAR)?;
    let secret_type = match cursor.peek().map(|t| &t.token) {
        Some(Token::SingleQuotedString(value)) => value.clone(),
        Some(Token::Word(word)) if word.quote_style.is_none() => word.value.clone(),
        Some(Token::Colon) | Some(Token::Placeholder(_)) => {
            return Err(grammar_error(TYPE_IS_IDENTIFIER))
        }
        _ => return Err(grammar_error(CREATE_GRAMMAR)),
    };
    cursor.advance(1);

    let mut options: Vec<SecretOption> = Vec::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    seen.insert("TYPE".to_string());
    while cursor.take_token(&Token::Comma) {
        let key = match cursor.peek().map(|t| &t.token) {
            Some(Token::Word(word)) if word.quote_style.is_none() => word.value.to_uppercase(),
            _ => return Err(grammar_error(CREATE_GRAMMAR)),
        };
        cursor.advance(1);
        if !seen.insert(key.clone()) {
            return Err(grammar_error(if key == "TYPE" {
                TYPE_IS_IDENTIFIER
            } else {
                DUPLICATE_OPTION
            }));
        }
        options.push(option_value(cursor, key)?);
    }

    if !cursor.take_token(&Token::RParen) {
        return Err(grammar_error(CREATE_GRAMMAR));
    }
    end_of_statement(cursor, CREATE_GRAMMAR)?;

    Ok(CreateSecret {
        name,
        workspace,
        secret_type,
        options,
        or_replace,
        if_not_exists,
    })
}

/// One option's value: a single-quoted literal, or a NAMED placeholder `:name`
/// written as one unbroken token pair. Positional `?` / `$1` are refused - a
/// credential bound by position is one reordering away from going to the
/// wrong key.
fn option_value(cursor: &mut Cursor, key: String) -> Result<SecretOption, ParserError> {
    let token = match cursor.peek() {
        Some(t) => t.clone(),
        None => return Err(grammar_error(CREATE_GRAMMAR)),
    };
    match &token.token {
        Token::SingleQuotedString(value) => {
            cursor.advance(1);
            Ok(SecretOption {
                key,
                value: None,
                literal: Some(LiteralSpan {
                    start: token.span.start,
                    end: token.span.end,
                    value: value.clone(),
                }),
            })
        }
        Token::Colon => {
            cursor.advance(1);
            let word = match cursor.peek() {
                Some(t) => t.clone(),
                None => return Err(grammar_error(CREATE_GRAMMAR)),
            };
            match &word.token {
                Token::Word(w) if w.quote_style.is_none() && word.span.start == token.span.end => {
                    cursor.advance(1);
                    Ok(SecretOption {
                        key,
                        value: Some(Placeholder {
                            name: format!(":{}", w.value),
                        }),
                        literal: None,
                    })
                }
                _ => Err(grammar_error(CREATE_GRAMMAR)),
            }
        }
        _ => Err(grammar_error(CREATE_GRAMMAR)),
    }
}

fn end_of_statement(cursor: &Cursor, grammar: &str) -> Result<(), ParserError> {
    if cursor.at_end() || cursor.peek_is(&Token::SemiColon) {
        Ok(())
    } else {
        Err(grammar_error(grammar))
    }
}

/// The lift (§2.3 step 2): every literal option value in every `CREATE SECRET`
/// in `sql`, replaced in the text by `:redacted_<key>`, and returned beside the
/// rewritten text keyed by that placeholder's name (without the colon).
///
/// `Ok(None)` when `sql` holds no `CREATE SECRET`. The rewritten statement
/// parses to the same shape - each placeholder occupies the slot its literal
/// did - so it can be classified and authorised in place of the original, and
/// the values bound back as parameters.
pub fn redact(
    dialect: &dyn Dialect,
    sql: &str,
) -> Result<Option<(String, Vec<(String, String)>)>, ParserError> {
    let statements = super::parse_statements(dialect, sql)?;

    let mut existing: BTreeSet<String> = BTreeSet::new();
    let mut spans: Vec<(LiteralSpan, String)> = Vec::new();
    let mut found = false;
    for statement in &statements {
        if let OpteryxStatement::Opteryx(OpteryxOnly::CreateSecret(create)) = statement {
            found = true;
            for option in &create.options {
                if let Some(placeholder) = &option.value {
                    existing.insert(placeholder.name.trim_start_matches(':').to_lowercase());
                }
                if let Some(span) = &option.literal {
                    let name = format!("{REDACTED_PREFIX}{}", option.key.to_lowercase());
                    spans.push((span.clone(), name));
                }
            }
        }
    }
    if !found {
        return Ok(None);
    }

    let mut names: BTreeSet<String> = BTreeSet::new();
    for (_, name) in &spans {
        if existing.contains(name) || !names.insert(name.clone()) {
            return Err(grammar_error(REDACTED_COLLISION));
        }
    }
    if existing.iter().any(|name| name.starts_with(REDACTED_PREFIX)) {
        return Err(grammar_error(REDACTED_COLLISION));
    }

    // Rewrite back to front so earlier offsets stay valid.
    let chars: Vec<char> = sql.chars().collect();
    let mut cuts: Vec<(usize, usize, String, String)> = spans
        .into_iter()
        .map(|(span, name)| {
            let start = super::offset_of(sql, span.start.line, span.start.column);
            let end = super::offset_of(sql, span.end.line, span.end.column);
            (start, end, name, span.value)
        })
        .collect();
    cuts.sort_by(|a, b| b.0.cmp(&a.0));

    let mut out = chars;
    let mut values: Vec<(String, String)> = Vec::new();
    for (start, end, name, value) in cuts {
        let replacement: Vec<char> = format!(":{name}").chars().collect();
        out.splice(start..end, replacement);
        values.push((name, value));
    }
    values.reverse();
    Ok(Some((out.into_iter().collect(), values)))
}
