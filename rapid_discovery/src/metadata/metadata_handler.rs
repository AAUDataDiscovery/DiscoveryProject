use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use polars::prelude::LazyFrame;
use crate::file_handling::utils::hash_file;
use crate::metadata::columns::construct_column_list_from_lazy_df;
use crate::metadata::metadata::Metadata;
use crate::metadata::metadata_handler::MetadataHandlerAction::CreateMetadata;
use crate::utils::file_extension::FileExtension;
use crate::utils::file_size_unit::FileSizeUnit;

pub (crate) struct MetadataHandler {
    rx: Receiver<MetadataHandlerAction>
}

impl MetadataHandler {
    pub(crate) fn new() -> (Sender<MetadataHandlerAction>, MetadataHandler) {
        let (tx, rx) = mpsc::channel();
        let metadata_handler = MetadataHandler {
            rx
        };
        (tx, metadata_handler)
    }
    pub (crate) fn start_listening_to_events(&self) {
        for received in &self.rx {
            match received {
                CreateMetadata {path, size, df } => create_metadata(path, size, df),
            }
        }
    }
}

fn create_metadata(path: String, size: u64, df: LazyFrame) {
    let mut metadata = construct_base_metadata(&*path, size);
    println!("Metadata crc is: {}", metadata.hash);
    metadata.columns = construct_column_list_from_lazy_df(df);
}
fn construct_base_metadata(path: &str, size: u64) -> Metadata {
    Metadata::new(
        path.to_string(),
        FileExtension::get_extension_from_path(path.to_string()).unwrap(),
        (size, FileSizeUnit::Byte),
        hash_file(String::from(path)).expect("wut wut"),
        HashMap::new()
    )
}

pub (crate) enum MetadataHandlerAction {
    CreateMetadata {path: String, size: u64,  df: LazyFrame},
}