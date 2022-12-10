use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};

pub static SOCKET_PATH: &'static str = "/tmp/file-monitor-daemon-socket";
pub(crate) enum DaemonCommand {
    STATUS(u32),
    SHUTDOWN(u32)

}

impl DaemonCommand {
    pub(crate) fn to_string(&self) -> String {
        match self {
            DaemonCommand::STATUS(id) => format!("STATUS;{}",id),
            DaemonCommand::SHUTDOWN(id) => format!("SHUTDOWN;{}",id),
        }
    }
    pub(crate) fn from_string(string: String) -> DaemonCommand {
        let split = string.split(';');
        let vec: Vec<&str> = split.collect();
        let command = vec[0];
        let id = vec[1].parse::<u32>().unwrap();
        match vec[0] {
            "STATUS" => DaemonCommand::STATUS(id),
            "SHUTDOWN" => DaemonCommand::SHUTDOWN(id),
            _ => {DaemonCommand::SHUTDOWN(0)}
        }
    }
}


#[derive(Serialize, Deserialize)]
pub(crate) struct DaemonResponse {
    pub(crate) responseId: u32,
    pub(crate) status: DaemonStatus,
    pub(crate) events: Vec<DaemonEvent>,
    pub(crate) monitored: Vec<DaemonMonitoredItem>,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct DaemonEvent {
    pub(crate) path: String,
    pub(crate) crc32: u32,
    pub(crate) triggered: u128
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct DaemonMonitoredItem {
    pub(crate) path: String,
    pub(crate) crc32: u32,
    pub(crate) last_changed: u128
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug)]
pub(crate) enum DaemonStatus {
    INITIALIZING(u64),
    PROCESSING(u64),
    IDLING
}

pub fn get_current_time_as_millis() -> u128 {
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    since_the_epoch.as_millis()
}