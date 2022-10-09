"""
Data storage in memory
"""
from typing import Optional, Union

import pandas

from utils.metadata_enums import FileSizeUnit, FileExtension


class ColMetadata:
    name: str
    col_type: str
    mean: Union[int, float, None]
    min: any
    max: any
    columns: any  # [ColMetadata]

    def __init__(self, name: str, col_type: str, mean: Union[int, float, None], min_val, max_val, columns = None):
        self.name = name
        self.col_type = col_type
        self.mean = mean
        self.min = min_val
        self.max = max_val
        self.columns = columns

    def set_columns(self, columns):
        self.columns = columns


class Metadata:
    filepath: str
    extension: FileExtension
    size: (int, FileSizeUnit)
    hash: int
    columns: [ColMetadata]

    def __init__(self, filepath: str, extension: FileExtension,
                 size: (int, FileSizeUnit), file_hash: int, columns: [] = []):
        self.filepath = filepath
        self.extension = extension
        self.size = size
        self.hash = file_hash
        self.columns = columns

    def set_columns(self, columns):
        self.columns = columns
