mod file_handling;
mod metadata;
mod utils;

use std::sync::mpsc::{Receiver, Sender};
use std::thread;
use std::thread::JoinHandle;
use thread::spawn;
use file_handling::filesystem_explorer;
use crate::file_handling::file_loader::{FileLoader, FileLoaderAction};
use crate::metadata::metadata_handler::{MetadataHandler, MetadataHandlerAction};


fn main() {
    let (metadata_tx, listener_handle) = start_processing_metadata();
    let (file_reader_tx, listener_handle) = start_listening_to_file_operation_requests(metadata_tx);
    let explorer_handle = start_exploring_files("/home/balazs/Documents/repos/DiscoveryProject/test/mock_filesystem", file_reader_tx);

    explorer_handle.join().expect("Something went wrong");
    listener_handle.join().expect("Something went wrong");
}

fn start_processing_metadata() -> (Sender<MetadataHandlerAction>, JoinHandle<()>) {
    let (tx, metadata_handler) = MetadataHandler::new();
    let handle = spawn(move || {
        metadata_handler.start_listening_to_events();
    });
    return (tx, handle);
}

fn start_exploring_files(path: &str, tx: Sender<FileLoaderAction>) -> JoinHandle<()> {
    let local_path = String::from(path);
    let explorer_handle = spawn(move || {
        filesystem_explorer::recursively_walk_and_broadcast_files(local_path.as_str(), tx)
    });
    return explorer_handle
}

fn start_listening_to_file_operation_requests(tx: Sender<MetadataHandlerAction>) -> (Sender<FileLoaderAction>, JoinHandle<()>) {
    let (tx, file_loader) = FileLoader::new(tx);
    let handle = spawn(move || {
        file_loader.start_listening_to_events()
    });
    return (tx, handle)
}

