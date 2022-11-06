use std::collections::HashMap;
use polars::export::ahash::RandomState;
use crate::metadata::columns::ColMetadata;
use crate::utils::file_extension::FileExtension;
use crate::utils::file_size_unit::FileSizeUnit;

pub(crate) struct Metadata {
    pub(crate) file_path: String,
    pub(crate) extension: FileExtension,
    pub(crate) size: (u64, FileSizeUnit),
    pub(crate) hash: u32,
    pub(crate) columns: HashMap<String, Box<dyn ColMetadata>>,
}
impl Metadata {
    pub (crate) fn new(
        file_path: String,
        extension: FileExtension,
        size: (u64, FileSizeUnit),
        hash: u32,
        columns: HashMap<String, Box<dyn ColMetadata>>
    ) -> Metadata {
        return Metadata {
            file_path,
            extension,
            size,
            hash,
            columns
        }
    }
}
