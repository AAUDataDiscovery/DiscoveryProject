use std::{thread, time};
use rust_utils::daemon_client::create_daemon;

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


    let files = Vec::from([String::from("/home/balazs/Downloads/test_data/archive/ACC_3minute_data.csv"),
            String::from("/home/balazs/Downloads/test_data/archive/ACC_5minute_data.csv"),
        String::from("/home/balazs/Downloads/test_data/archive/ACC_10minute_data.csv"), String::from("/home/balazs/Downloads/test_data/archive/kaka")]);

    //create_daemon(files);
    //rust_utils::daemon_client::get_daemon_status();
    rust_utils::daemon_client::stop_daemon();
}