pub(crate) enum FileExtension {
    CSV,
    JSON
}

impl FileExtension{
    pub(crate) fn get_extension_from_path(path: String) -> Option<FileExtension> {
        let lowercase = path.to_lowercase();
        if lowercase.ends_with(".json") {
            return Some(FileExtension::JSON);
        }
        if lowercase.ends_with(".csv") {
            return Some(FileExtension::CSV);
        }
        return None;
    }
}