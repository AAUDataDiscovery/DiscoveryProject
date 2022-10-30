extern crate core;

mod file_handling;
mod metadata;
mod utils;

use std::borrow::BorrowMut;
use std::os::unix::fs::MetadataExt;
use std::sync::{Arc, Mutex};
use std::thread::spawn;
use jwalk::WalkDirGeneric;
use crate::file_handling::file_loader::{FileLoader, FileLoaderAction};
use crate::file_handling::filesystem_explorer::create_file_walker;
use crate::metadata::metadata_handler::MetadataHandler;


fn main() {
    let walker = create_file_walker();
    for path in walker {
        match path {
            Ok(v) => {
                if v.file_type.is_file() {
                    let size = v.metadata().unwrap().size();
                }
            },
            Err(_) => continue
        }
    }
}