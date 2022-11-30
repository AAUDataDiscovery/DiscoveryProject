use std::fs::File;
use std::io::{BufRead, BufReader};
use adler32::RollingAdler32;
use crc32fast::Hasher;
use pyo3::pyfunction;

#[pyfunction]
pub fn multithreaded_crc32_hash(paths: Vec<String>, number_of_threads: usize) -> Vec<u32> {
    let mut results: Vec<u32> = Vec::new();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(number_of_threads)
        .build()
        .unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    for f in paths.into_iter() {
        let tx = tx.clone();
        pool.spawn(move || {
            tx.send(crc32_hash_single(f)).unwrap();
        });
    }
    drop(tx); // need to close all senders, otherwise...
    let results: Vec<u32> = rx.into_iter().collect();
    return results;
}



fn crc32_hash_single(path: String) -> u32 {
    const CAP: usize = 1024 * 128;
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
    return hasher.finalize();
}

/// returns the CRC32 hash of a file
#[pyfunction]
pub fn crc32_hash(paths: Vec<String>) -> Vec<u32> {
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

#[pyfunction]
pub fn adler32_hash(paths: Vec<String>) -> Vec<u32> {
    const CAP: usize = 1024 * 128;
    let mut results: Vec<u32> = Vec::new();
    for path in paths {
        let mut hasher = RollingAdler32::new();
        let file = File::open(path).unwrap();

        let mut reader = BufReader::with_capacity(CAP, file);

        loop {
            let length = {
                let buffer = reader.fill_buf().unwrap();
                hasher.update_buffer(buffer);
                buffer.len()
            };
            if length == 0 {
                break;
            }
            reader.consume(length);
        }
        results.push(hasher.hash());
    }
    return results;
}


#[pyfunction]
pub fn multithreaded_adler32_hash(paths: Vec<String>, number_of_threads: usize) -> Vec<u32> {
    let mut results: Vec<u32> = Vec::new();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(number_of_threads)
        .build()
        .unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    for f in paths.into_iter() {
        let tx = tx.clone();
        pool.spawn(move || {
            tx.send(adler32_hash_single(f)).unwrap();
        });
    }
    drop(tx); // need to close all senders, otherwise...
    let results: Vec<u32> = rx.into_iter().collect();
    return results;
}

pub fn adler32_hash_single(path: String) -> u32 {
    const CAP: usize = 1024 * 128;
    let mut hasher = RollingAdler32::new();
    let file = File::open(path).unwrap();

    let mut reader = BufReader::with_capacity(CAP, file);

    loop {
        let length = {
            let buffer = reader.fill_buf().unwrap();
            hasher.update_buffer(buffer);
            buffer.len()
        };
        if length == 0 {
            break;
        }
        reader.consume(length);
    }
    return hasher.hash();
}

#[pyfunction]
pub fn fletcher16_hash(paths: Vec<String>) -> Vec<u16> {
    const CAP: usize = 1024 * 128;
    let mut results: Vec<u16> = Vec::new();
    for path in paths {
        let mut hasher = fletcher::Fletcher16::new();
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
        results.push(hasher.value());
    }
    return results;
}
