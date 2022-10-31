use std::collections::HashMap;
use std::fs::Metadata;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use polars::prelude::LazyCsvReader;
use crate::file_handling::file_loader::FileLoaderAction::LoadFile;
use crate::metadata::metadata_handler::MetadataHandlerAction;
use crate::utils::file_extension::FileExtension;
use crate::utils::file_size_unit::FileSizeUnit;


pub (crate) struct FileLoader {
    rx: Receiver<FileLoaderAction>,
    pub(crate) metadata_handler_tx: Sender<MetadataHandlerAction>
}

impl FileLoader {
    pub(crate) fn new( metadata_handler_tx: Sender<MetadataHandlerAction>) -> (Sender<FileLoaderAction>, FileLoader) {
        let (tx, rx) = mpsc::channel();
        let file_loader = FileLoader {
            rx,
            metadata_handler_tx
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
        let df = LazyCsvReader::new(path.as_str()).finish().unwrap();
        self.metadata_handler_tx.send(MetadataHandlerAction::CreateMetadata {path, size, df});
    }
}

pub (crate) enum FileLoaderAction {
    LoadFile {path: String, size: u64 },
}

