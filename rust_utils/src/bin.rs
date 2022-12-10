use std::{thread, time};

fn main () {
    thread::spawn(|| {
        let files = Vec::from([String::from("/home/balazs/Downloads/test_data/archive/ACC_3minute_data.csv"),
            String::from("/home/balazs/Downloads/test_data/archive/ACC_5minute_data.csv"),
        String::from("/home/balazs/Downloads/test_data/archive/ACC_10minute_data.csv")]);
        rust_utils::daemon_server::start_daemon(files);
    });
    let sleepy = time::Duration::from_secs(5);
    thread::sleep(sleepy);
    rust_utils::daemon_client::get_daemon_status();
    thread::sleep(sleepy);
    rust_utils::daemon_client::get_daemon_status();

}