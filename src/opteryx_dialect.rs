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

use sqlparser::ast::{BinaryOperator, Expr};
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

impl Dialect for OpteryxDialect {
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
