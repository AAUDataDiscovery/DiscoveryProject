use std::os::unix::fs::MetadataExt;
use std::sync::mpsc::Sender;
use jwalk::{DirEntry, Error, WalkDirGeneric};
use jwalk::{Parallelism};
use crate::file_handling::file_loader::FileLoaderAction;

pub (crate) fn recursively_walk_and_broadcast_files(path: &str, tx: Sender<FileLoaderAction>) {
    let walker = create_file_walker(path);
    iterate_over_walker_files(walker, tx)
}


fn create_file_walker(path: &str) -> WalkDirGeneric<((), ())> {
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

fn iterate_over_walker_files(walker: WalkDirGeneric<((), ())>, tx: Sender<FileLoaderAction>) {
    for path in walker {
        match path {
            Ok(v) => {
                if v.file_type.is_file() {
                    let size = v.metadata().unwrap().size();
                    broadcast_to_file_loader(
                        v.path().to_str().unwrap_or(""), size, &tx
                    )
                }
            },
            Err(_) => continue
        }
    }

}

fn broadcast_to_file_loader(path: &str, size: u64, tx: &Sender<FileLoaderAction>) {
    let action = FileLoaderAction::LoadFile { path: String::from(path), size };
    tx.send(action).expect("Failed to broadcast file path to file loader");
}
