use pythonize::pythonize;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pythonize::PythonizeError;

use sqlparser::ast::Statement;

mod aside;
mod opteryx_dialect;

pub use opteryx_dialect::OpteryxDialect;

/// Function to parse SQL statements from a string. Returns a list with
/// one item per query statement.
///
/// We always use 'opteryx' as the dialect for parsing, to help anyone
/// who is familiar with sqloxide to not assume the default behaviour
/// we have a _dialect parameter that is not used.
#[pyfunction]
#[pyo3(text_signature = "(sql, dialect)")]
fn parse_sql(py: Python, sql: String, _dialect: String) -> PyResult<Py<PyAny>> {
    let chosen_dialect = Box::new(OpteryxDialect {});
    // The aside parser, not `Parser::parse_sql`: statements sqlparser has no
    // grammar for are recognised on the token stream before it is handed over.
    // Everything else reaches Python exactly as it did - see `aside`.
    let parse_result = aside::parse_statements(&*chosen_dialect, &sql);

    let output = match parse_result {
        Ok(statements) => pythonize(py, &statements).map_err(|e| {
            let msg = e.to_string();
            PyValueError::new_err(format!("Python object serialization failed.\n\t{msg}"))
        })?,
        Err(e) => {
            let msg = e.to_string();
            return Err(PyValueError::new_err(format!(
                "Query parsing failed.\n\t{msg}"
            )));
        }
    };

    Ok(output.into())
}


/// This utility function allows reconstituing a modified AST back into list of SQL queries.
#[pyfunction]
#[pyo3(text_signature = "(ast)")]
fn restore_ast(_py: Python, ast: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    let parse_result: Result<Vec<Statement>, PythonizeError> = pythonize::depythonize(ast);

    let output = match parse_result {
        Ok(statements) => statements,
        Err(e) => {
            let msg = e.to_string();
            return Err(PyValueError::new_err(format!(
                "Query serialization failed.\n\t{msg}"
            )));
        }
    };

    Ok(output
        .iter()
        .map(std::string::ToString::to_string)
        .collect::<Vec<String>>())
}


/// The `CREATE SECRET` lift (jobs.opteryx docs/design/secrets.md §2.3).
///
/// Returns `None` when `sql` holds no `CREATE SECRET`; otherwise
/// `(redacted_sql, values)`, where every literal option value has been replaced
/// in the text by `:redacted_<key>` and `values` maps each such placeholder name
/// (no colon) to the literal it replaced.
///
/// Errors never quote the statement: a grammar refusal quotes the grammar, and
/// any other failure is reported without its text, because the input to this
/// function is by construction a statement that may hold a credential.
#[pyfunction]
#[pyo3(text_signature = "(sql)")]
fn redact_secret_statement(py: Python, sql: String) -> PyResult<Py<PyAny>> {
    let chosen_dialect = Box::new(OpteryxDialect {});
    match aside::redact_secret_statement(&*chosen_dialect, &sql) {
        Ok(None) => Ok(py.None()),
        Ok(Some((redacted, values))) => {
            let dict = pyo3::types::PyDict::new(py);
            for (name, value) in values {
                dict.set_item(name, value)?;
            }
            Ok((redacted, dict).into_pyobject(py)?.into_any().unbind())
        }
        Err(e) => {
            let msg = e.to_string();
            let marker = "OPTERYX-SYNTAX: ";
            let detail = match msg.find(marker) {
                Some(at) => msg[at..].to_string(),
                None => "the statement could not be parsed".to_string(),
            };
            Err(PyValueError::new_err(format!("Query parsing failed.\n\t{detail}")))
        }
    }
}


// gil_used = false declares this module safe to import under a free-threaded
// (PEP 703) CPython without forcing the GIL back on. The functions here are
// pure (SQL parse / AST restore, no shared mutable state), so this is sound.
#[pymodule(gil_used = false)]
fn compute(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_sql, m)?)?;
    m.add_function(wrap_pyfunction!(restore_ast, m)?)?;
    m.add_function(wrap_pyfunction!(redact_secret_statement, m)?)?;
    Ok(())
}