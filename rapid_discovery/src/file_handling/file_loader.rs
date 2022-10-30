use std::rc::Rc;
use std::sync::{Arc, mpsc, Mutex};
use std::sync::mpsc::{Receiver, Sender};
use polars::prelude::LazyCsvReader;
use crate::file_handling::file_loader::FileLoaderAction::LoadFile;
use crate::metadata::metadata_handler::MetadataHandlerAction;


pub (crate) struct FileLoader {
    rx: Receiver<FileLoaderAction>,
    communication_broker: Option<Arc<Mutex<CommunicationBroker>>>
}

impl FileLoader {
    pub(crate) fn new() -> (Sender<FileLoaderAction>, FileLoader) {
        let (tx, rx) = mpsc::channel();
        let file_loader = FileLoader {
            rx,
            communication_broker: None
        };
        (tx, file_loader)
    }

    pub (crate) fn start_listening_to_events(&self) {
        for received in &self.rx {
            match received {
                LoadFile { path, size } => self.handle_load_file_request(path, size),
            }
        }
    }

    fn handle_load_file_request(&self, path: String, size: u64) {
        let df = LazyCsvReader::new(path.as_str());
        let tx = self.communication_broker
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .get_metadata_handler_tx();

        let message = MetadataHandlerAction::CreateMetadata {
            path,
            size,
            df: df.finish().unwrap()
        };
        tx.send(message);
    }
    pub (crate) fn set_communication_broker(&mut self, communication_broker: Arc<Mutex<CommunicationBroker>>) {
        self.communication_broker = Option::from(communication_broker);
    }
}


pub (crate) enum FileLoaderAction {
    LoadFile {path: String, size: u64 },
}

