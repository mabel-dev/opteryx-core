// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// See the License at http://www.apache.org/licenses/LICENSE-2.0
// Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

// This is a custom dialect for Opteryx using the DataFusion SQL parser as the engine.
// Opteryx originally used the MySQL dialect, but it has been modified to support
// features available in other syntaxes.
//
// Extends:
//  https://github.com/apache/datafusion-sqlparser-rs/blob/main/src/dialect/mod.rs

use std::boxed::Box;

use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::{
    AlterTable, AlterTableOperation, BinaryOperator, Expr, Ident, ObjectName, SqlOption,
    Statement, Value,
};
use sqlparser::dialect::{Dialect, Precedence};
use sqlparser::keywords::Keyword;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::Token;

/// A [`Dialect`] for [Opteryx](https://www.opteryx.dev/)
#[derive(Debug)]
pub struct OpteryxDialect {}

/// The `TRIM` production, in full, because Opteryx accepts a combination of
/// spellings no single upstream branch covers:
///
/// ```sql
/// TRIM(str)                                -- strip whitespace, both ends
/// TRIM([BOTH|LEADING|TRAILING] chars FROM str)   -- SQL-92
/// TRIM(str, chars)                         -- Postgres / DuckDB / Snowflake / BigQuery
/// ```
///
/// Upstream has both of the last two, but they are mutually exclusive branches of
/// `Parser::parse_trim_expr`, and the comma one throws the direction away: it
/// builds `trim_where: None` even when `LEADING` was written. Downstream that is
/// indistinguishable from no direction at all, so `TRIM(LEADING x, 'a')` would
/// quietly trim BOTH ends. This function keeps the two spellings separate and
/// REFUSES the mixture, which is also what every dialect that has the comma form
/// does — the direction belongs to the `FROM` form.
///
/// The one-argument and `FROM` branches are byte-for-byte upstream's behaviour,
/// including `TRIM(LEADING str)` (no `FROM`, no characters), which has always
/// parsed as LTRIM and stays parsing that way.
fn parse_opteryx_trim(parser: &mut Parser) -> Result<Expr, ParserError> {
    parser.next_token(); // TRIM
    parser.expect_token(&Token::LParen)?;

    let mut trim_where = None;
    if let Token::Word(word) = &parser.peek_token_ref().token {
        if [Keyword::BOTH, Keyword::LEADING, Keyword::TRAILING].contains(&word.keyword) {
            trim_where = Some(parser.parse_trim_where()?);
        }
    }

    let expr = parser.parse_expr()?;

    if parser.parse_keyword(Keyword::FROM) {
        // TRIM([direction] characters FROM string)
        let trim_what = Box::new(expr);
        let string = parser.parse_expr()?;
        parser.expect_token(&Token::RParen)?;
        return Ok(Expr::Trim {
            expr: Box::new(string),
            trim_where,
            trim_what: Some(trim_what),
            trim_characters: None,
        });
    }

    if parser.consume_token(&Token::Comma) {
        // TRIM(string, characters)
        if trim_where.is_some() {
            return Err(ParserError::ParserError(
                "TRIM: a direction (BOTH, LEADING, TRAILING) belongs to the FROM form, \
                 as `TRIM(LEADING 'x' FROM str)`. The comma form `TRIM(str, 'x')` always \
                 trims both ends; write `LTRIM(str, 'x')` or `RTRIM(str, 'x')` for one end."
                    .to_string(),
            ));
        }
        let characters = parser.parse_comma_separated(Parser::parse_expr)?;
        parser.expect_token(&Token::RParen)?;
        return Ok(Expr::Trim {
            expr: Box::new(expr),
            trim_where: None,
            trim_what: None,
            trim_characters: Some(characters),
        });
    }

    // TRIM(string) / TRIM([direction] string)
    parser.expect_token(&Token::RParen)?;
    Ok(Expr::Trim {
        expr: Box::new(expr),
        trim_where,
        trim_what: None,
        trim_characters: None,
    })
}

/// What `parse_guarded_add_column_prefix` recovered before the column definition:
/// everything from `ALTER` up to and including the `IF NOT EXISTS` guard.
struct GuardedAddColumn {
    name: ObjectName,
    if_exists: bool,
    only: bool,
    column_keyword: bool,
}

/// `ALTER TABLE [IF EXISTS] [ONLY] name ADD [COLUMN] IF NOT EXISTS`, or nothing.
///
/// Errors mean "this is not that statement" and are discarded by the caller's
/// `maybe_parse`, which rewinds the parser so upstream sees an untouched token
/// stream. Nothing here is a diagnostic anyone reads.
fn parse_guarded_add_column_prefix(parser: &mut Parser) -> Result<GuardedAddColumn, ParserError> {
    parser.expect_keywords(&[Keyword::ALTER, Keyword::TABLE])?;
    let if_exists = parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
    let only = parser.parse_keyword(Keyword::ONLY);
    let name = parser.parse_object_name(false)?;
    parser.expect_keyword(Keyword::ADD)?;

    // Both orderings are accepted, because upstream accepts both and Postgres
    // writes the second: `ADD IF NOT EXISTS COLUMN x` already parsed here (the
    // guard was then silently dropped, see the `parse_statement` note), and
    // `ADD COLUMN IF NOT EXISTS x` is the documented form.
    let guard_before_column = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
    let column_keyword = parser.parse_keyword(Keyword::COLUMN);
    let guard_after_column = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
    if !guard_before_column && !guard_after_column {
        return Err(ParserError::ParserError(
            "not a guarded ADD COLUMN".to_string(),
        ));
    }

    Ok(GuardedAddColumn {
        name,
        if_exists,
        only,
        column_keyword,
    })
}

/// The rest of a guarded `ADD COLUMN`, once the guard has been seen and the
/// statement can no longer be anything else. Errors from here ARE diagnostics -
/// they name the column definition that failed to parse.
fn parse_guarded_add_column(
    parser: &mut Parser,
    prefix: GuardedAddColumn,
) -> Result<Statement, ParserError> {
    let column_def = parser.parse_column_def()?;

    // Same rule upstream's `parse_alter_table` uses: the statement ends at its
    // semicolon when it has one, otherwise at the last token consumed.
    let end_token = if parser.peek_token_ref().token == Token::SemiColon {
        parser.peek_token_ref().clone()
    } else {
        parser.get_current_token().clone()
    };

    // The statement ends with its column definition: no `column_position`, no
    // comma-separated second operation. Both are refused by the planner anyway
    // (a column is always appended; one operation per statement), so leaving
    // their tokens unconsumed costs nothing and gets them refused - `ADD COLUMN
    // IF NOT EXISTS x INT FIRST` gets the parser's own "expected end of
    // statement" rather than a silent acceptance. Only the message differs from
    // the unguarded path, never the outcome.
    Ok(Statement::AlterTable(AlterTable {
        name: prefix.name,
        if_exists: prefix.if_exists,
        only: prefix.only,
        operations: vec![AlterTableOperation::AddColumn {
            column_keyword: prefix.column_keyword,
            if_not_exists: true,
            column_def,
            column_position: None,
        }],
        location: None,
        on_cluster: None,
        table_type: None,
        end_token: AttachedToken(end_token),
    }))
}

// Reserved property keys: an INTERNAL transport, not anything a reader may
// write. See `parse_tag_ddl`. The `__opteryx.` prefix is refused from reader
// text by the planner, which is the side that can tell the two apart.
const TAG_ACTION_KEY: &str = "__opteryx.tag.action";
const TAG_NAME_KEY: &str = "__opteryx.tag.name";
const TAG_VERSION_KEY: &str = "__opteryx.tag.version";

/// `ALTER TABLE [IF EXISTS] [ONLY] name (CREATE|DROP) TAG ...`, or nothing.
struct TagDdl {
    name: ObjectName,
    if_exists: bool,
    only: bool,
    create: bool,
}

/// Errors mean "this is not that statement" and are discarded by the caller's
/// `maybe_parse`, which rewinds the parser. Nothing here is a diagnostic.
fn parse_tag_ddl_prefix(parser: &mut Parser) -> Result<TagDdl, ParserError> {
    parser.expect_keywords(&[Keyword::ALTER, Keyword::TABLE])?;
    let if_exists = parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
    let only = parser.parse_keyword(Keyword::ONLY);
    let name = parser.parse_object_name(false)?;

    let create = if parser.parse_keyword(Keyword::CREATE) {
        true
    } else if parser.parse_keyword(Keyword::DROP) {
        false
    } else {
        return Err(ParserError::ParserError("not tag DDL".to_string()));
    };

    // TAG is what commits us. Without it this is an ordinary DROP COLUMN (or
    // anything else beginning CREATE/DROP) and must rewind to upstream untouched.
    if !parser.parse_keyword(Keyword::TAG) {
        return Err(ParserError::ParserError("not tag DDL".to_string()));
    }

    Ok(TagDdl {
        name,
        if_exists,
        only,
        create,
    })
}

/// A tag name: a quoted string or a bare identifier, which mean the same thing.
///
/// Not normalized here. Tag names fold to lowercase, but that rule belongs to the
/// one place that resolves them; a second copy in the parser is a second place for
/// it to drift.
fn parse_tag_name(parser: &mut Parser) -> Result<String, ParserError> {
    let token = parser.peek_token();
    match &token.token {
        Token::SingleQuotedString(value) => {
            parser.next_token();
            Ok(value.clone())
        }
        Token::Word(_) => Ok(parser.parse_identifier()?.value),
        _ => parser.expected("a tag name", token),
    }
}

/// `AS OF VERSION <snapshot id | CURRENT | PREVIOUS>`, defaulting to CURRENT.
///
/// Returned as text for the planner to interpret, because these are three
/// different KINDS of answer - a literal id, and two instructions to go and look
/// one up - and flattening them to a number here would need the catalog, which a
/// parser does not have and must not acquire.
///
/// CURRENT and PREVIOUS are matched as identifiers rather than keywords: PREVIOUS
/// is not one, and reading both the same way keeps the two spellings symmetrical.
fn parse_tag_version(parser: &mut Parser) -> Result<String, ParserError> {
    if !parser.parse_keywords(&[Keyword::AS, Keyword::OF, Keyword::VERSION]) {
        return Ok("current".to_string());
    }

    let token = parser.peek_token();
    match &token.token {
        Token::Number(digits, _) => {
            parser.next_token();
            Ok(digits.clone())
        }
        Token::Word(_) => {
            let word = parser.parse_identifier()?.value.to_uppercase();
            match word.as_str() {
                "CURRENT" => Ok("current".to_string()),
                "PREVIOUS" => Ok("previous".to_string()),
                _ => parser.expected("a snapshot id, CURRENT or PREVIOUS", token),
            }
        }
        _ => parser.expected("a snapshot id, CURRENT or PREVIOUS", token),
    }
}

/// The rest of a tag statement, once `TAG` has been seen and it can be nothing
/// else. Errors from here ARE diagnostics.
///
/// Opteryx owns this statement outright: sqlparser has no tag operation on ALTER
/// TABLE, and its `Tag` AST node is Snowflake's key-value governance tag, a
/// different concept wearing our word. Rather than re-spell tag DDL onto some
/// other slot in the rewriter - every slot large enough to carry a name AND a
/// version is one we may want later, and a re-spelling makes the parser report
/// errors about a feature the reader never mentioned - the dialect parses it, the
/// way the Snowflake dialect parses its own statements.
///
/// What it CANNOT do is invent an AST node: `AlterTableOperation` has no
/// `CreateTag`, and adding one is the only part of this that would need a fork of
/// sqlparser. So the parsed result travels to the planner inside
/// `SetTblProperties` under reserved `__opteryx.tag.*` keys - an internal
/// transport, produced only here and read by exactly one branch of
/// `plan_alter_table`, never a second spelling anybody may write. The planner
/// refuses those keys from any statement it did not build itself.
fn parse_tag_ddl(parser: &mut Parser, prefix: TagDdl) -> Result<Statement, ParserError> {
    let tag = parse_tag_name(parser)?;
    let version = if prefix.create {
        parse_tag_version(parser)?
    } else {
        String::new()
    };

    let end_token = if parser.peek_token_ref().token == Token::SemiColon {
        parser.peek_token_ref().clone()
    } else {
        parser.get_current_token().clone()
    };

    let mut properties = vec![
        property(
            TAG_ACTION_KEY,
            if prefix.create { "create" } else { "drop" },
        ),
        property(TAG_NAME_KEY, &tag),
    ];
    if prefix.create {
        properties.push(property(TAG_VERSION_KEY, &version));
    }

    Ok(Statement::AlterTable(AlterTable {
        name: prefix.name,
        if_exists: prefix.if_exists,
        only: prefix.only,
        operations: vec![AlterTableOperation::SetTblProperties {
            table_properties: properties,
        }],
        location: None,
        on_cluster: None,
        table_type: None,
        end_token: AttachedToken(end_token),
    }))
}

/// One transport property. The key is an UNQUOTED identifier containing dots,
/// which is a shape reader text cannot produce - a bare key cannot contain a dot,
/// and a quoted one arrives carrying its quote style - so the planner can tell a
/// statement this dialect built from one somebody typed.
fn property(key: &str, value: &str) -> SqlOption {
    SqlOption::KeyValue {
        key: Ident::new(key),
        value: Expr::Value(Value::SingleQuotedString(value.to_string()).with_empty_span()),
    }
}

impl Dialect for OpteryxDialect {
    /// Opteryx owns two `ALTER TABLE` productions: snapshot tag DDL, which
    /// upstream has no grammar for at all (see `parse_tag_ddl`), and
    /// `ADD COLUMN IF NOT EXISTS` - see `parse_guarded_add_column_prefix`.
    ///
    /// Upstream gates the column-level guard on `dialect_of!(self is PostgreSql |
    /// BigQuery | DuckDb | Generic)` with no trait flag to opt into, so a custom
    /// dialect cannot have it any other way. The gate does two things to a
    /// dialect outside that list, and BOTH are wrong here:
    ///
    ///   ADD COLUMN IF NOT EXISTS x INT   fails to parse ("expected a data type,
    ///                                    found: NOT"), though every layer below
    ///                                    the parser implements the guard - the
    ///                                    planner reads `if_not_exists`, the
    ///                                    operator passes it, both connectors
    ///                                    honour it.
    ///   ADD IF NOT EXISTS COLUMN x INT   parses, and the guard is DISCARDED:
    ///                                    upstream parses it, then overwrites the
    ///                                    flag with `false`. A re-run of a
    ///                                    migration script written that way fails
    ///                                    on the duplicate column exactly as if
    ///                                    the guard had not been written.
    ///
    /// The hook takes over only statements that carry the guard; every other
    /// `ALTER TABLE` rewinds and parses upstream, unchanged.
    fn parse_statement(&self, parser: &mut Parser) -> Option<Result<Statement, ParserError>> {
        // Cheap gate: this runs in front of EVERY statement, so anything not
        // opening with ALTER is rejected on one peek, before `maybe_parse`.
        match &parser.peek_token_ref().token {
            Token::Word(word) if word.keyword == Keyword::ALTER => {}
            _ => return None,
        }

        // Tag DDL first: `ALTER TABLE t DROP TAG x` and `ALTER TABLE t DROP COLUMN x`
        // share a prefix, and only the word after DROP tells them apart. Both
        // probes rewind on a miss, so the order costs nothing but a peek.
        match parser.maybe_parse(parse_tag_ddl_prefix) {
            Ok(Some(prefix)) => return Some(parse_tag_ddl(parser, prefix)),
            Ok(None) => {}
            Err(err) => return Some(Err(err)),
        }

        match parser.maybe_parse(parse_guarded_add_column_prefix) {
            Ok(Some(prefix)) => Some(parse_guarded_add_column(parser, prefix)),
            Ok(None) => None,
            // Only RecursionLimitExceeded reaches here; it is not recoverable by
            // handing the statement to another parser.
            Err(err) => Some(Err(err)),
        }
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        // Identifiers which begin with a digit are recognized while tokenizing numbers,
        // so they can be distinguished from exponent numeric literals.
        ch.is_alphabetic()
            || ch == '_'
            || ch == '$'
            || ch == '@'
            || ('\u{0080}'..='\u{ffff}').contains(&ch)
    }

    // Unquoted identifiers do NOT contain `-`: a bare `-` is always the subtraction
    // operator, so `a-b` / `1-x` parse as arithmetic (standard SQL). Hyphenated names
    // (e.g. blob-store paths like `my-bucket`) must be backtick-quoted, which bypasses
    // this rule via `is_delimited_identifier_start`. (Reverts [#2376].)
    fn is_identifier_part(&self, ch: char) -> bool {
        self.is_identifier_start(ch) || ch.is_ascii_digit()
    }

    // Double quotes are keyword/identifier escapes (standard SQL): `"group"` is the
    // column named `group`, not a string literal. Single quotes are the only string
    // delimiter. Backticks are also accepted for identifiers (MySQL heritage, and the
    // only way to quote hyphenated names - see `is_identifier_part`).
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '`' || ch == '"'
    }

    fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
        Some('`')
    }

    // See https://dev.mysql.com/doc/refman/8.0/en/string-literals.html#character-escape-sequences
    fn supports_string_literal_backslash_escape(&self) -> bool {
        false
    }

    fn supports_numeric_prefix(&self) -> bool {
        true
    }

    // COMMENT ON TABLE table_name IS 'This is a comment';
    fn supports_comment_on(&self) -> bool {
        true
    }

    // SELECT COUNT(*) FILTER (WHERE ID < 4)
    fn supports_filter_during_aggregation(&self) -> bool {
        true
    }

    // GROUP BY ROLLUP(a, b) / CUBE(...) / GROUPING SETS (...).
    //
    // Without this the grouping-set productions never run and `ROLLUP(a, b)` parses as
    // an ordinary scalar function call, which then fails catalog lookup with the
    // misleading "Function **ROLLUP** cannot be found". ROLLUP is a GROUP BY modifier,
    // not a function, and has to be recognised as one at the parser rather than be left
    // to fall through to function resolution.
    //
    // This is the `GROUP BY <construct>(...)` spelling only. The `GROUP BY x WITH ROLLUP`
    // spelling is a separate MySQL-heritage flag (`supports_group_by_with_modifier`) and
    // stays OFF - it is a different production with different semantics for the modifier
    // list, and nothing asks for it.
    fn supports_group_by_expr(&self) -> bool {
        true
    }

    // The accessor / containment family (`->`, `->>`, `@>`, `@>>`, `@?`) binds tighter
    // than everything except member access (`.`).
    //
    // sqlparser rates all of these at Precedence::PgOther (16), BELOW Is (17), Like (19),
    // Eq/Between (20), PlusMinus (30), MulDivModOp (40) and DoubleColon/`[ ]` (50). Since
    // the right operand is parsed with parse_subexpr(precedence), that low rating let the
    // operand run away and swallow whatever followed:
    //
    //     a->>'b' = 'x'         became  a ->> ('b' = 'x')
    //     a->>'b' IS NULL       became  a ->> ('b' IS NULL)
    //     a->'b' * 2            became  a -> ('b' * 2)
    //     a->>'id'::INTEGER     became  a ->> ('id'::INTEGER)
    //     a @> ['x'] = true     became  a @>  (['x'] = true)
    //
    // The comparison and arithmetic cases are plainly wrong - Postgres itself puts the
    // "any other operator" band ABOVE comparison and LIKE, so `payload->>'a' = 'x'` groups
    // as `(payload->>'a') = 'x'` there too; sqlparser's table is what disagrees.
    //
    // The cast/subscript cases ARE Postgres behaviour (`::` outranks `->` there, so
    // `payload->>'id'::INTEGER` casts the KEY) and diverging from it is deliberate: that
    // is a well-known Postgres footgun, giving a confusing error for a non-numeric key and
    // silently degrading into an array index for a numeric-looking one. Opteryx binds the
    // extraction first, so the cast applies to the extracted value - what the syntax reads
    // like and what it is always written to mean. Likewise `a->'b'[1]` is `(a->'b')[1]`.
    //
    // The whole family is raised to ONE level, by architect's decision, so the rule is a
    // single sentence rather than a per-operator table. The consequence to know about is
    // that a cast on a containment operand regroups too: `a @> ['x']::ARRAY<VARCHAR>` is
    // `(a @> ['x'])::ARRAY<VARCHAR>`, not a cast of the operand. Parenthesise to get the
    // other reading.
    //
    // `@>` and `@>>` share a leading Token::AtArrow, so raising AtArrow covers both.
    //
    // This replaced opteryx/planner/ast_rewriter/rewrite_json_accessors, a downstream AST
    // rewriter that existed solely to undo the mangling above (comparison, LIKE and
    // IS NULL forms). It has been deleted - the parse is now correct at the source.
    fn get_next_precedence(
        &self,
        parser: &sqlparser::parser::Parser,
    ) -> Option<Result<u8, ParserError>> {
        match parser.peek_token_ref().token {
            Token::Arrow | Token::LongArrow | Token::AtArrow | Token::AtQuestion => {
                // One step above the `::` / `[ ]` band, so the right operand stops before
                // a trailing cast or subscript and the outer expression applies it to the
                // accessor's result instead.
                Some(Ok(self.prec_value(Precedence::DoubleColon) + 1))
            }
            // fall back to the default precedence table
            _ => None,
        }
    }

    fn parse_infix(
        &self,
        parser: &mut sqlparser::parser::Parser,
        expr: &sqlparser::ast::Expr,
        precedence: u8,
    ) -> Option<Result<sqlparser::ast::Expr, ParserError>> {
        // IPv4 CIDR containment: `addr <<= '10.0.0.0/8'` (contained by or equal)
        // and `'10.0.0.0/8' >>= addr` (contains or equal). This is the Postgres
        // spelling, also used by CockroachDB and DuckDB's inet extension.
        //
        // The tokenizer emits `<<=` as ShiftLeft + Eq, so both tokens are PEEKED
        // before either is consumed - the two-token form has to win over the bare
        // `<<` shift handled below, and a lone `next_token()` here would consume
        // the `<<` out from under it. Peeking decides before anything is consumed.
        //
        // The right operand is parsed with parse_subexpr(precedence), NOT
        // parse_expr(): parse_expr consumes the whole remaining expression, so
        // `ip <<= '10/8' AND x = 1` would bind as `ip <<= ('10/8' AND x = 1)`.
        //
        // BinaryOperator::Custom carries the SQL SPELLING, never the operator's
        // internal name. sqlparser's Display for Custom writes the string back
        // verbatim, and that Display is how a view is serialised for storage
        // (ViewManagementNode -> restore_ast). Naming the variant "IPContainedBy"
        // saved `ip <<= '10/8'` as `ip IPContainedBy '10/8'` - a view that could
        // never be re-parsed. The symbol round-trips. `binary_op` in the logical
        // planner maps the symbol back to the canonical operator name.
        if matches!(parser.peek_token().token, Token::ShiftLeft)
            && matches!(parser.peek_nth_token(1).token, Token::Eq)
        {
            parser.next_token(); // <<
            parser.next_token(); // =
            return Some(match parser.parse_subexpr(precedence) {
                Ok(right) => Ok(Expr::BinaryOp {
                    left: Box::new(expr.clone()),
                    op: BinaryOperator::Custom("<<=".to_string()),
                    right: Box::new(right),
                }),
                Err(e) => Err(e),
            });
        }
        if matches!(parser.peek_token().token, Token::ShiftRight)
            && matches!(parser.peek_nth_token(1).token, Token::Eq)
        {
            parser.next_token(); // >>
            parser.next_token(); // =
            return Some(match parser.parse_subexpr(precedence) {
                Ok(right) => Ok(Expr::BinaryOp {
                    left: Box::new(expr.clone()),
                    op: BinaryOperator::Custom(">>=".to_string()),
                    right: Box::new(right),
                }),
                Err(e) => Err(e),
            });
        }
        // Bitwise shifts: `i << 2`, `i >> 2`. Reached only once the `<<=` / `>>=`
        // arms above have declined, so the two-token IPv4 forms still win.
        //
        // sqlparser maps `<<` / `>>` to PGBitwiseShiftLeft/Right for its Postgres
        // and Generic dialects only, so this dialect got "No infix parser for token
        // ShiftLeft" - while draken's bitwise_shl/bitwise_shr kernels, the
        // BOP_SHIFT_LEFT/BOP_SHIFT_RIGHT opcodes and the INTEGER-INTEGER operator_map
        // entries were all in place. The operator was implemented end to end except
        // for being reachable.
        //
        // Custom carries the SQL SPELLING for the same round-trip reason as `<<=`
        // above; `get_operator_for_sql_symbol` maps `<<` back to ShiftLeft, which is
        // the name operators.json publishes.
        if matches!(parser.peek_token().token, Token::ShiftLeft) {
            parser.next_token(); // <<
            return Some(match parser.parse_subexpr(precedence) {
                Ok(right) => Ok(Expr::BinaryOp {
                    left: Box::new(expr.clone()),
                    op: BinaryOperator::Custom("<<".to_string()),
                    right: Box::new(right),
                }),
                Err(e) => Err(e),
            });
        }
        if matches!(parser.peek_token().token, Token::ShiftRight) {
            parser.next_token(); // >>
            return Some(match parser.parse_subexpr(precedence) {
                Ok(right) => Ok(Expr::BinaryOp {
                    left: Box::new(expr.clone()),
                    op: BinaryOperator::Custom(">>".to_string()),
                    right: Box::new(right),
                }),
                Err(e) => Err(e),
            });
        }
        // As above, the right operand of every custom operator below is parsed with
        // parse_subexpr(precedence), NOT parse_expr(): parse_expr consumes the whole
        // remaining expression regardless of binding power, so `a DIV 2 = 1 AND b = 1`
        // would bind as `a DIV (2 = 1 AND b = 1)` - integer division by a boolean, a
        // silently wrong query rather than an error. Errors are propagated, never
        // unwrapped: a panic here unwinds out of the extension into Python instead of
        // surfacing as a ParserError.
        //
        // Unlike `<<=` / `>>=`, these operators consume via parse_keyword/consume_token,
        // which do not leave the parser in a partially-consumed state on a non-match, so
        // no peeking is needed.

        // Parse DIV as an operator
        if parser.parse_keyword(Keyword::DIV) {
            Some(match parser.parse_subexpr(precedence) {
                Ok(right) => Ok(Expr::BinaryOp {
                    left: Box::new(expr.clone()),
                    op: BinaryOperator::MyIntegerDivide,
                    right: Box::new(right),
                }),
                Err(e) => Err(e),
            })
        // Parse `@>>` as "ArrayContainsAll"
        } else if parser.consume_token(&Token::AtArrow) {
            // we just consumed @>
            if parser.consume_token(&Token::Gt) {
                // Actually saw @>>
                return Some(match parser.parse_subexpr(precedence) {
                    Ok(right) => Ok(Expr::BinaryOp {
                        left: Box::new(expr.clone()),
                        // As with `<<=` above: the SQL spelling, so Display round-trips.
                        op: BinaryOperator::Custom("@>>".to_string()),
                        right: Box::new(right),
                    }),
                    Err(e) => Err(e),
                });
            } else {
                // Just plain @>
                return Some(match parser.parse_subexpr(precedence) {
                    Ok(right) => Ok(Expr::BinaryOp {
                        left: Box::new(expr.clone()),
                        op: BinaryOperator::AtArrow,
                        right: Box::new(right),
                    }),
                    Err(e) => Err(e),
                });
            }
        } else {
            None
        }
    }

    fn require_interval_qualifier(&self) -> bool {
        true
    }

    /// Returns true if the dialect supports an `EXCEPT` clause following a
    /// wildcard in a select list.
    ///
    /// For example
    /// ```sql
    /// SELECT * EXCEPT order_id FROM orders;
    /// ```
    fn supports_select_wildcard_except(&self) -> bool {
        true
    }

    /// Returns true if the dialect supports subscripting arrays (field['key'])
    fn supports_partiql(&self) -> bool {
        true
    }

    // Returns true if the dialect supports numbers containing underscores, e.g. `10_000_000`
    fn supports_numeric_literal_underscores(&self) -> bool {
        true
    }

    // Does the dialect support the `MATCH() AGAINST()` syntax?
    fn supports_match_against(&self) -> bool {
        true
    }

    /// Opteryx owns the whole `TRIM` production — see `parse_opteryx_trim`.
    ///
    /// `supports_comma_separated_trim` is deliberately LEFT FALSE. That flag would
    /// have been the one-line way to make `TRIM(str, chars)` parse, and it is not
    /// used because sqlparser's own comma branch DISCARDS a direction keyword:
    /// `TRIM(LEADING x, 'a')` comes back with `trim_where: None`, which the planner
    /// cannot distinguish from `TRIM(x, 'a')` and would silently answer as BOTH.
    /// A silent wrong answer is not an acceptable price for a shorter diff.
    fn parse_prefix(&self, parser: &mut Parser) -> Option<Result<Expr, ParserError>> {
        // TRIM followed by `(` and nothing else. Everything falls through to the
        // default prefix parser, including a column or alias that happens to be
        // named `trim`.
        match &parser.peek_token_ref().token {
            Token::Word(word) if word.keyword == Keyword::TRIM => {}
            _ => return None,
        }
        if parser.peek_nth_token_ref(1).token != Token::LParen {
            return None;
        }
        Some(parse_opteryx_trim(parser))
    }

    /// Returns true if the dialect supports timestamp versioning for time-travel queries.
    /// This enables syntax like:
    /// - `SELECT * FROM table TIMESTAMP AS OF '2024-12-15 00:00:00'`
    fn supports_table_versioning(&self) -> bool {
        true
    }

    /// See <https://docs.databricks.com/en/sql/language-manual/delta-optimize.html>
    /// OPTIMIZE TABLE table [WHERE ]
    /// TODO - not implemented
    fn supports_optimize_table(&self) -> bool {
        true
    }

    /// Returns true if the dialect supports modifiers on SELECT statements, such as `DISTINCT` or `ALL`.
    /// This enables syntax like:
    /// - SELECT
    ///    [HIGH_PRIORITY]
    ///    [STRAIGHT_JOIN]
    ///    [SQL_SMALL_RESULT]
    ///    [SQL_BIG_RESULT]
    ///    [SQL_BUFFER_RESULT]
    ///    [SQL_NO_CACHE]
    ///    [SQL_CALC_FOUND_ROWS]
    /// TODO - not implemented
    fn supports_select_modifiers(&self) -> bool {
        true
    }
}
