use std::{thread, time};
use rust_utils::daemon_client::create_daemon;
use rust_utils::hashing;

fn main () {
    // thread::spawn(|| {
    //
    //     rust_utils::daemon_server::start_daemon(files);
    // });
    // let sleepy = time::Duration::from_secs(7);
    // thread::sleep(sleepy);
    // rust_utils::daemon_client::get_daemon_status();
    // thread::sleep(sleepy);
    // rust_utils::daemon_client::get_daemon_status();
    // thread::sleep(sleepy);
    // rust_utils::daemon_client::get_daemon_status();
    // thread::sleep(sleepy);
    // rust_utils::daemon_client::get_daemon_status();
    // thread::sleep(sleepy);
    // rust_utils::daemon_client::get_daemon_status();
    //     thread::sleep(sleepy);
    // rust_utils::daemon_client::get_daemon_status();
    //     thread::sleep(sleepy);
    // rust_utils::daemon_client::get_daemon_status();


    // let files = Vec::from([ String::from("/home/balazs/Downloads/test_data/archive/kaka")]);
    //
    // //create_daemon(files);
    // let status = rust_utils::daemon_client::get_daemon_status();
    // //rust_utils::daemon_client::stop_daemon();
    // println!("aaaaaa");
    let files = Vec::from([ String::from("/home/balazs/Downloads/test_data/archive/HINDALCO_60minute_data.csv")]);
    hashing::multithreaded_crc32_hash_with_uring(files, 4);
}