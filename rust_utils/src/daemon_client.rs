use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use async_std::io::WriteExt;
use rand::Rng;
use crate::daemon_common::{DaemonCommand, DaemonResponse, SOCKET_PATH};


pub (crate) fn create_daemon(directory: String) -> u32{
    0
}
pub fn stop_daemon() -> bool {
    let id = create_new_id();
    let command = DaemonCommand::SHUTDOWN(id);

    println!("sending shutdown signal");
    let mut unix_stream = UnixStream::connect(SOCKET_PATH).unwrap();
    let response = send_and_receive_message(command, unix_stream);
    true

}

fn send_and_receive_message(command: DaemonCommand, mut unix_stream: UnixStream) {
    let message = command.to_string();
    unix_stream.write(message.as_bytes()).unwrap();
    unix_stream.shutdown(std::net::Shutdown::Write);

    let response = read_from_stream(&mut unix_stream);
    let decoded: DaemonResponse = bincode::deserialize(&response[..]).unwrap();
}

pub fn get_daemon_status() -> () {
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
    println!("{:#?}", decoded.status);
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