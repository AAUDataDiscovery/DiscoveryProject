use pyo3::prelude::*;
use std::fs::File;
use std::io::{BufRead, BufReader};
use crc32fast::Hasher;


/// returns the CRC32 hash of a file
#[pyfunction]
fn hash_file(paths: Vec<String>) -> Vec<u32> {
    const CAP: usize = 1024 * 128;
    let mut results: Vec<u32> = Vec::new();
    for path in paths {
        let mut hasher = Hasher::new();
        let file = File::open(path).unwrap();

        let mut reader = BufReader::with_capacity(CAP, file);

        loop {
            let length = {
                let buffer = reader.fill_buf().unwrap();
                hasher.update(buffer);
                buffer.len()
            };
            if length == 0 {
                break;
            }
            reader.consume(length);
        }
        results.push(hasher.finalize());
    }
    return results;
}


/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

/// A Python module implemented in Rust.
#[pymodule]
fn rust_utils(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add_function(wrap_pyfunction!(hash_file, m)?)?;
    Ok(())
}