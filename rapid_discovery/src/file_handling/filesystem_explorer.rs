use std::os::unix::fs::MetadataExt;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::Sender;
use jwalk::{DirEntry, Error, WalkDirGeneric};
use jwalk::{Parallelism};
use crate::file_handling::file_loader::FileLoaderAction;

pub (crate) fn create_file_walker(path: &str) -> WalkDirGeneric<((), ())> {
    return WalkDirGeneric::<((),())>::new(path)
        .parallelism(Parallelism::RayonNewPool(4))
        .process_read_dir(|_, _, _, children|  {
            filter_metadata_files(children);
    });
}

fn filter_metadata_files(children: &mut Vec<Result<DirEntry<((), ())>, Error>>) {
    children.retain(|dir_entry_result| {
        match dir_entry_result {
            Ok(v) => {
                !v.file_name().to_str().unwrap().ends_with(".metadata.json")
            },
            Err(e) => {println!("{}",e); false},
        }
    });
}