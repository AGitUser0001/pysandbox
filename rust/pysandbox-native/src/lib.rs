use std::time::Duration;

use pyo3::prelude::*;

#[pyfunction]
fn protocol_version() -> u16 {
    pysandbox_protocol::PROTOCOL_VERSION
}

#[pyfunction]
fn sleep<'py>(py: Python<'py>, milliseconds: u64) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        tokio::time::sleep(Duration::from_millis(milliseconds)).await;
        Python::attach(|py| Ok(py.None()))
    })
}

#[pymodule]
fn _core(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(protocol_version, module)?)?;
    module.add_function(wrap_pyfunction!(sleep, module)?)?;
    Ok(())
}
