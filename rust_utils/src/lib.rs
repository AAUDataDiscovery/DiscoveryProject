mod hashing;

use pyo3::prelude::*;
use std::fs::File;
use std::io::{BufRead, BufReader};
use crc32fast::Hasher;
use adler32::RollingAdler32;






/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

/// A Python module implemented in Rust.
#[pymodule]
fn rust_utils(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add_function(wrap_pyfunction!(hashing::crc32_hash, m)?)?;
    m.add_function(wrap_pyfunction!(hashing::multithreaded_crc32_hash_with_uring, m)?)?;
    m.add_function(wrap_pyfunction!(hashing::multithreaded_crc32_hash_with_single_io_thread, m)?)?;
    m.add_function(wrap_pyfunction!(hashing::adler32_hash, m)?)?;
    m.add_function(wrap_pyfunction!(hashing::multithreaded_adler32_hash, m)?)?;
    m.add_function(wrap_pyfunction!(hashing::fletcher16_hash, m)?)?;
    m.add_function(wrap_pyfunction!(hashing::multithreaded_crc32_hash, m)?)?;


    Ok(())
}