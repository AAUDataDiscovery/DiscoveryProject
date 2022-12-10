use std::fs::File;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::process::exit;
use async_std::io::WriteExt;
use pyo3::pyfunction;
use rand::Rng;
use crate::daemon_common::{DaemonCommand, DaemonResponse, SOCKET_PATH};
pub use daemonize_me::Daemon;
use crate::daemon_server::daemon;

#[pyfunction]
pub fn create_daemon(paths: Vec<String>) {
    let stdout = File::create("info.log").unwrap();
    let stderr = File::create("err.log").unwrap();
    let file_daemon = Daemon::new()
        .pid_file("/tmp/file_daemon.pid", Some(false))
        .umask(0o000)
        .work_dir(".")
        .stdout(stdout)
        .stderr(stderr)
        .start();

    match file_daemon {
        Ok(_) => println!("Daemonized with success"),
        Err(e) => {
            eprintln!("Error, {}", e);
            exit(-1);
        },
    }
    daemon(paths);
}

#[pyfunction]
pub fn stop_daemon() -> bool {
    let id = create_new_id();
    let command = DaemonCommand::SHUTDOWN(id);

    println!("sending shutdown signal");
    let mut unix_stream = UnixStream::connect(SOCKET_PATH).unwrap();
    let message = command.to_string();
    unix_stream.write(message.as_bytes()).unwrap();
    unix_stream.shutdown(std::net::Shutdown::Write);
    true
}

fn send_and_receive_message(command: DaemonCommand, mut unix_stream: UnixStream) {
    let message = command.to_string();
    unix_stream.write(message.as_bytes()).unwrap();
    unix_stream.shutdown(std::net::Shutdown::Write);

    let response = read_from_stream(&mut unix_stream);
    let decoded: DaemonResponse = bincode::deserialize(&response[..]).unwrap();
}

#[pyfunction]
pub fn get_daemon_status() -> String {
    let id = create_new_id();
    let command = DaemonCommand::STATUS(id);

    let mut unix_stream = UnixStream::connect(SOCKET_PATH).unwrap();
    let message = command.to_string();
    unix_stream.write(message.as_bytes()).unwrap();
    println!("sent status command with id: {}",id);
        unix_stream
        .shutdown(std::net::Shutdown::Write);

    let response = read_from_stream(&mut unix_stream);
    let decoded: DaemonResponse = bincode::deserialize(&response[..]).unwrap();
     serde_json::to_string(&decoded).unwrap()
}

fn read_from_stream(unix_stream: &mut UnixStream) -> Vec<u8> {
    let mut response = &mut vec![];
    unix_stream
        .read_to_end(response);
    response.clone()
}


fn create_new_id() -> u32 {
    let mut rng = rand::thread_rng();
    rng.gen_range(0..99999)
}