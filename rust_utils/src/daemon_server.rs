use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::process::exit;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::thread;
use std::time::SystemTime;
use inotify::{
    EventMask,
    WatchMask,
    Inotify
};use crate::daemon_common::{DaemonCommand, DaemonEvent, DaemonMonitoredItem, DaemonResponse, DaemonStatus, get_current_time_as_millis, InotifyEvent, SOCKET_PATH};
use crate::daemon_common::DaemonCommand::{SHUTDOWN, STATUS};
use crate::daemon_common::InotifyEvent::MODIFIED_PENDING;
use crate::hashing::{crc32_hash_single_with_uring_for_daemon};

pub fn daemon(files: Vec<String>) {
    //initialize

    let files_clone= files.clone();

    let (inotify_tx, inotify_rx) = mpsc::channel();
    thread::spawn(move || {
        inotify_thread(files_clone, inotify_tx);
    });
    let command_rx = setup_command_listener();

    let mut status = DaemonStatus::INITIALIZING(files.len() as u64);
    let mut monitored: Vec<DaemonMonitoredItem> = create_starting_monitored_vec(files.clone());
    let mut events: Vec<DaemonEvent> = vec![];

    let (file_tx, file_rx) = mpsc::channel();
    let (hash_tx, hash_rx) = mpsc::channel();

    thread::spawn(move || {
        hasher_thread(file_rx, hash_tx);
    });
    file_tx.send(files).expect("error sending init file list");


    //initialization loop
    'init_loop: loop {
        //listen to commands
        for command in command_rx.try_iter() {
            let status_copy = status.clone();
            let monitored_copy = monitored.clone();

            //overwrite events with new empty vec
            events = handle_incoming_command(command, status_copy, monitored_copy, events);
        }

        //listen to hash results
        for result in hash_rx.try_iter() {
            for item in monitored.iter_mut() {
                if item.path == result.0 {
                    item.crc32 = result.1;
                    item.last_changed = get_current_time_as_millis();
                }
            }

            if let DaemonStatus::INITIALIZING(count) = status {
                let newCount = count-1;
                if newCount == 0 {
                    status = DaemonStatus::IDLING;
                    println!("exiting initialization");
                    break 'init_loop;
                }
                else {
                    status = DaemonStatus::INITIALIZING(newCount);
                }

            }
        }
    }


    //main loop
    loop {
        //listen to inofity events
        for inotify_event in inotify_rx.try_iter() {
            match inotify_event {
                InotifyEvent::MODIFIED(path) => {

                    if let DaemonStatus::PROCESSING(count) = status {
                        let newCount = count+1;
                        status = DaemonStatus::PROCESSING(newCount);
                    };

                    if let DaemonStatus::IDLING = status {
                        status = DaemonStatus::PROCESSING(1);
                    };

                    events.push(DaemonEvent{
                        path: path.clone(),
                        crc32: 0,
                        triggered: get_current_time_as_millis(),
                        kind: InotifyEvent::MODIFIED_PENDING
                    });
                    file_tx.send(Vec::from([path]));
                }
                InotifyEvent::DELETED(path) => {
                    events.push(DaemonEvent{
                        path: path.clone(),
                        crc32: 0,
                        triggered: get_current_time_as_millis(),
                        kind: InotifyEvent::DELETED(path.clone())
                    });
                   let index = monitored.iter().position(|item| item.path == path).unwrap();
                    monitored.remove(index);
                }
                _ => {}
            }
        }


        //listen to commands
        for command in command_rx.try_iter() {
            let status_copy = status.clone();
            let monitored_copy = monitored.clone();

            //overwrite events with new empty vec
            events = handle_incoming_command(command, status_copy, monitored_copy, events);
        }

        //listen to hash results
        for result in hash_rx.try_iter() {
            let mut hasChange = false;
            for item in monitored.iter_mut() {
                if item.path == result.0 {
                    if(item.crc32 != result.1) {
                        item.crc32 = result.1;
                        hasChange = true;
                        item.last_changed = get_current_time_as_millis();
                    }
                }
            }
            //updated the events too
            for item in events.iter_mut() {
                if item.path == result.0 {
                   item.crc32 = result.1;
                    if(hasChange) {
                        item.kind = InotifyEvent::MODIFIED(item.path.clone())
                    }
                    else {
                        item.kind = InotifyEvent::MODIFIED_NO_CHANGE;
                    }
                }
            }
            if let DaemonStatus::PROCESSING(count) = status {
                let newCount = count-1;
                if(newCount == 0) {
                    status = DaemonStatus::IDLING;
                }
                else {
                    status = DaemonStatus::PROCESSING(newCount);
                }
            };
        }
    }
}


fn inotify_thread(files: Vec<String>, tx: Sender<InotifyEvent>) {
    let mut inotify = inotify::Inotify::init().expect("Failed to initialize inotify");
    let mut watched: Vec<(String, inotify::WatchDescriptor)> = vec![];
    for file in files {
        let watch = inotify.add_watch(
                file.clone(),
                inotify::WatchMask::MODIFY | inotify::WatchMask::DELETE
        ).expect("couldn't add file watch");
        watched.push((file, watch));
    }

    loop {
        let mut buffer = [0; 1024];
        let inotify_events = inotify.read_events_blocking(&mut buffer)
            .expect("Error while reading events");

        for event in inotify_events {
            let index = watched.iter().position(|(file, wd)| *wd == event.wd).unwrap();
            let path = &watched[index].0;
            let localPath = String::from(path);
            let localPath2 = localPath.as_str();
            let deleted = !std::path::Path::new(localPath2).exists();
            if(deleted) {
                watched.remove(index);
                tx.send(InotifyEvent::DELETED(localPath)).expect("failed to send delete event");
            }
            else {
                let watch = inotify.add_watch(
                    localPath.clone(),
                    inotify::WatchMask::MODIFY | inotify::WatchMask::DELETE
                ).expect("couldn't add file watch");
                watched[index] = (localPath.clone(), watch);
                tx.send(InotifyEvent::MODIFIED(localPath)).expect("failed to send modified event");
            }
        }
    }
}
fn hasher_thread(file_rx: Receiver<Vec<String>>, hash_tx: Sender<(String, u32)>) {
    let pool = rayon::ThreadPoolBuilder::new()
    .num_threads(4)
    .build()
    .unwrap();

    loop {
        let paths = file_rx.recv().unwrap();
        for path in paths {
            let hash_tx = hash_tx.clone();
            pool.spawn(move || {
                hash_tx.send(crc32_hash_single_with_uring_for_daemon(path)).unwrap();
            });
        }
    }

}


fn handle_incoming_command(command: (DaemonCommand, UnixStream), status: DaemonStatus, monitored: Vec<DaemonMonitoredItem>, events: Vec<DaemonEvent>) -> Vec<DaemonEvent> {
    match command {
        ( STATUS(id), mut stream) => {
            let response = DaemonResponse {
                responseId: id,
                status: status,
                events: events.clone(),
                monitored: monitored
            };
            let serialized_response = bincode::serialize(&response).unwrap();
            stream.write(&serialized_response);
        }
        ( SHUTDOWN(id), mut stream) => {
            println!("server exiting: {}",id);
            exit(0);
        }
    }
    //events must be cleared after sent
    let mut new_events:Vec<DaemonEvent> = vec![];

    //only keep events that haven't finished yet
    for event in events {
        match event.kind {
            MODIFIED_PENDING => {new_events.push(event)}
            _ => {},
        }
    }

    new_events
}


fn create_starting_monitored_vec(files: Vec<String>) -> Vec<DaemonMonitoredItem> {
    let now = get_current_time_as_millis();
    let mut monitored: Vec<DaemonMonitoredItem> = Vec::new();
    for file in files {
        monitored.push(DaemonMonitoredItem {path: file, crc32: 0, last_changed: now })
    }
    monitored
}

fn setup_command_listener() -> Receiver<(DaemonCommand, UnixStream)> {
    let (tx, rx) = mpsc::channel();
    let unix_listener = create_unix_listener();
    thread::spawn(move || {
        loop {
            for stream in unix_listener.incoming() {
                let mut unix_stream = stream.unwrap();
                let mut message = String::new();
                unix_stream.read_to_string(&mut message).unwrap();
                println!("Server received: {}", message);
                tx.send((DaemonCommand::from_string(message), unix_stream));
            }
        }
    });
    rx
}

fn create_unix_listener() -> UnixListener {
    if std::fs::metadata(SOCKET_PATH).is_ok() {
        println!("A socket is already present. Deleting...");
        std::fs::remove_file(SOCKET_PATH);
    }

    let unix_listener = UnixListener::bind(SOCKET_PATH).expect("socket already exists, delete /tmp/file-monitor-daemon-socket");
    unix_listener
}


