use std::fs::{File, read};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::os::unix::prelude::FileExt;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::thread;
use adler32::RollingAdler32;
use crc32fast::Hasher;
use pyo3::pyfunction;
use std::fs::*;
use std::os::unix::io::AsRawFd;
use nix::fcntl::*;
use tokio_uring::fs::File as TokioFile;
const CAP: usize = 1024 * 128;


#[pyfunction]
pub fn multithreaded_crc32_hash(paths: Vec<String>, number_of_threads: usize) -> Vec<u32> {
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


#[pyfunction]
pub fn multithreaded_crc32_hash_with_single_io_thread(paths: Vec<String>, number_of_threads: usize) -> Vec<u32> {

    let (io_tx, io_rx) = mpsc::channel();
    thread::spawn(move || {
        start_listening_to_io_requests(io_rx)
    });

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(number_of_threads)
        .build()
        .unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    for f in paths.into_iter() {
        let tx = tx.clone();
        let io_tx = io_tx.clone();
        pool.spawn(move || {
            tx.send(crc32_hash_single_with_io_thread(f, io_tx)).unwrap();
        });
    }
    drop(tx); // need to close all senders, otherwise...
    drop(io_tx); // need to close all senders, otherwise...
    let results: Vec<u32> = rx.into_iter().collect();
    return results;
}

#[pyfunction]
pub fn multithreaded_crc32_hash_with_uring(paths: Vec<String>, number_of_threads: usize) -> Vec<u32> {

    let pool = threadpool::ThreadPool::new(number_of_threads);
    let (tx, rx) = std::sync::mpsc::channel();
    for f in paths.into_iter() {
        let tx = tx.clone();
        pool.execute(move || {
            tx.send(crc32_hash_single_with_uring(f)).unwrap();
        });
    }
    drop(tx); // need to close all senders, otherwise...
    let results: Vec<u32> = rx.into_iter().collect();
    return results;
}


fn crc32_hash_single_with_uring(path: String) -> u32 {
    let mut hasher = Hasher::new();
        tokio_uring::builder()
        .entries(1)
        .uring_builder(
            tokio_uring::uring_builder()
            .setup_cqsize(4))
        .start(  async {
            let file = TokioFile::open(&path[0..]).await.unwrap();
            let mut buffers0 = vec![Vec::<u8>::with_capacity(CAP), Vec::<u8>::with_capacity(CAP)];
            let mut buffers1 = vec![Vec::<u8>::with_capacity(CAP), Vec::<u8>::with_capacity(CAP)];
            let mut switcher = false;
            let mut offset = 0;

            loop {
                if (switcher) {
                    let future = file.readv_at(buffers0, offset);
                    let no_op_future = tokio_uring::no_op();
                    for buf in &buffers1 {
                        hasher.update(&buf[0..]);
                    }
                    let (res, ret) = future.await;
                    let n = res.unwrap();

                    if n == 0 {
                        break;
                    }

                    offset += n as u64;
                    buffers0 = ret;
                } else {
                    let future = file.readv_at(buffers1, offset);
                    let no_op_future = tokio_uring::no_op();
                    if (0 < offset) {
                    for buf in &buffers0 {
                        hasher.update(&buf[0..]);
                    }                    }
                    let (res, ret) = future.await;
                    let n = res.unwrap();

                    if n == 0 {
                        break;
                    }

                    offset += n as u64;
                    buffers1 = ret;
                }
                switcher = !switcher;
                // Don't await the no_op future otherwise a similar problem.
                // Just let the no_op_future get cancelled. The uring driver
                // should handle the cleanup elegantly without having to stall
                // the thread.
                //
                //     let _ = no_op_future.await
            }
        });
    return hasher.finalize();
}


// pub fn start_listening_to_io_requests(receiver: Receiver<(&mut BufReader<File>, Sender<Vec<u8>>)>) {
//     for received in receiver {
//         match received {
//             (buff_reader, sender) => {
//                 let buffer = buff_reader.fill_buf().unwrap();
//                 sender.send(buffer.to_vec());
//             }
//         }
//     }


// pub fn start_listening_to_io_requests(receiver: Receiver<(String, u64, Sender<(&[u8], u64, bool)>)>) {
//     for received in receiver {
//         match received {
//             (path, offset, sender) => {
//                 let mut opened_file = File::open(path).unwrap();
//                 let mut reader = BufReader::with_capacity(CAP, opened_file);
//                 reader.seek(SeekFrom::Start(offset)).unwrap();
//                 let buffer = reader.fill_buf().unwrap();
//                 let new_offset = reader.
//                 sender.send(buffer,)
//             }
//         }
//     }
// }

pub fn start_listening_to_io_requests(receiver: Receiver<(String, u64, Sender<(Vec<u8>, usize)>)>) {
    for received in receiver {
        match received {
            (path, offset, sender) => {
                let mut opened_file = File::open(path).unwrap();
                let mut buffer = [0u8; CAP];
                let read_count = opened_file.read_at(&mut buffer, offset).unwrap();
                sender.send((buffer.to_vec(), read_count));
            }
        }
    }
}



fn crc32_hash_single_with_io_thread(path: String, io_tx: Sender<(String, u64, Sender<(Vec<u8>, usize)>)>) -> u32 {
    let (local_tx, local_rx) = mpsc::channel();
    let mut hasher = Hasher::new();
    let mut offset = 0;

    loop {
    let local_path = path.clone();
    io_tx.send((local_path, offset, local_tx.clone())).unwrap();
        let result = local_rx.recv().unwrap();
        if result.1 == 0 {
            break;
        }
        else {
            offset += u64::try_from(result.1).unwrap();
            let local_local_path = path.clone();
            io_tx.send((local_local_path, offset, local_tx.clone())).unwrap();
            hasher.update(&result.0[0..]);
        }
    }
    return hasher.finalize();
}



fn crc32_hash_single(path: String) -> u32 {
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
