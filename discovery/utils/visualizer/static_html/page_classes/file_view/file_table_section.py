from utils.metadata.metadata import Metadata


class FileTableSection:
    def __init__(self, metadatum: Metadata):
        self.metadatum = metadatum

    def build_dictionary(self):
        return {
            "content_file": "file/file_table_section.html",
            "filename": self.metadatum.file_path,
            "columns": self._build_col_list()
        }
    def _build_col_list(self):
        columns = []
        for column in self.metadatum.columns:
            columns.append({
                "name": column.name,
                "col_type": column.col_type,
                "min": column.minimum,
                "max": column.maximum,
                "mean": column.mean or "N/A"
            })
        return columns