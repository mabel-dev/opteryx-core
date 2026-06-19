use pythonize::pythonize;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pythonize::PythonizeError;

use sqlparser::ast::Statement;
use sqlparser::parser::Parser;

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
    let parse_result = Parser::parse_sql(&*chosen_dialect, &sql);

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


// gil_used = false declares this module safe to import under a free-threaded
// (PEP 703) CPython without forcing the GIL back on. The functions here are
// pure (SQL parse / AST restore, no shared mutable state), so this is sound.
#[pymodule(gil_used = false)]
fn compute(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_sql, m)?)?;
    m.add_function(wrap_pyfunction!(restore_ast, m)?)?;
    Ok(())
}