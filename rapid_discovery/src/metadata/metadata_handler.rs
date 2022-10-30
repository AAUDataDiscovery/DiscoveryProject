use std::collections::HashMap;
use std::rc::Rc;
use std::sync::{Arc, mpsc, Mutex};
use std::sync::mpsc::{Receiver, Sender};
use polars::frame::DataFrame;
use polars::prelude::LazyFrame;
use crate::metadata::metadata::Metadata;
use crate::metadata::metadata_handler::MetadataHandlerAction::CreateMetadata;
use crate::utils::file_extension::FileExtension;
use crate::utils::file_size_unit::FileSizeUnit;

pub (crate) struct MetadataHandler {
    rx: Receiver<MetadataHandlerAction>,
    communication_broker: Option<Arc<Mutex<CommunicationBroker>>>
}

impl MetadataHandler {
    pub(crate) fn new() -> (Sender<MetadataHandlerAction>, MetadataHandler) {
        let (tx, rx) = mpsc::channel();
        let metadata_handler = MetadataHandler {
            rx,
            communication_broker: None
        };
        (tx, metadata_handler)
    }
    pub (crate) fn start_listening_to_events(&self) {
        for received in &self.rx {
            match received {
                CreateMetadata {path, size, df } =>
                {
                    let metadata = handle_create_metadata(path, size, df);
                }
            }
        }
    }
    pub (crate) fn set_communication_broker(&mut self, communication_broker: Arc<Mutex<CommunicationBroker>>) {
        self.communication_broker = Option::from(communication_broker);
    }
}

fn handle_create_metadata(path: String, size: u64, df: LazyFrame) {
    let metadata = construct_metadata(&*path, size, df);
    println!("Metadata path is: {}", metadata.file_path)
}
fn construct_metadata(path: &str, size: u64, df: LazyFrame) -> Metadata {
    Metadata::new(
        path.to_string(),
        FileExtension::get_extension_from_path(path.to_string()).unwrap(),
         (size, FileSizeUnit::Byte),
        0,
        HashMap::new()
    )
}
pub (crate) enum MetadataHandlerAction {
    CreateMetadata {path: String, size: u64,  df: LazyFrame},
}