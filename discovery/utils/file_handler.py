"""
Reads a given file path into a dataframe
"""
import os
from typing import Optional

import pandas
import logging

from utils.custom_exceptions import UnsupportedFileExtension, FileNotFoundError
from utils.metadata_enums import FileExtension, FileSizeUnit
from pandas.util import hash_pandas_object

logger = logging.getLogger(__name__)


class FileDescriptor:
    dataframe: Optional[pandas.DataFrame]
    path: str
    extension: FileExtension
    size: (int, FileSizeUnit)
    hash: int
    def __init__(self, file_hash: int, extension: FileExtension,
                 path: str, size: (int, FileSizeUnit), dataframe: Optional[pandas.DataFrame] = None):
        self.dataframe = dataframe
        self.hash = file_hash
        self.extension = extension
        self.path = path
        self.size = size

    def set_dataframe(self, dataframe: pandas.DataFrame):
        self.dataframe = dataframe


class FileHandler:
    def __init__(self):
        self.supported_extensions = {m.split('_')[-1]: getattr(self, m) for m in dir(self) if m.startswith('_handle')}
        self.loaded_files = {}

    def scan_filesystem(self, file_path):
        """
        Loads a given file system into memory
        """
        for root, dirs, files in os.walk(file_path):
            for filename in files:
                if any([filename.endswith(extension) for extension in self.supported_extensions]):
                    self.load_file(os.path.join(root, filename))

    def load_file(self, file_path):
        """
        Loads a given file if a reader exists for its extension
        """
        extension = file_path.split('.')[-1] if '.' in file_path else 'txt'
        # file doesn't exist
        if not os.path.exists(file_path):
            raise FileNotFoundError(file_path)

        # file doesn't have a supported extension
        if extension not in self.supported_extensions:
            raise UnsupportedFileExtension(file_path)

        # Nothing in place to prevent reloading files for now
        handler = self.supported_extensions[extension]
        logger.debug(f"Loading file {file_path} using \"{handler.__name__}\" handler")
        self.loaded_files[file_path] = handler(file_path)

    @staticmethod
    def _handle_csv(file_path):
        dataframe = pandas.read_csv(file_path)
        return FileHandler.construct_file_descriptor(file_path, FileExtension.CSV, dataframe)

    @staticmethod
    def _handle_json(file_path):
        return pandas.read_json(file_path)
        return FileHandler.construct_file_descriptor(file_path, FileExtension.JSON, dataframe)

    @staticmethod
    def construct_file_descriptor(file_path: str, extension: FileExtension, dataframe: pandas.DataFrame):
        size = os.stat(file_path).st_size
        file_hash = FileHandler.get_dataframe_hash(dataframe)
        return FileDescriptor(file_hash, extension, file_path, (size, FileSizeUnit.BYTE), dataframe)

    @staticmethod
    def get_dataframe_hash(dataframe: pandas.DataFrame):
        return hash_pandas_object(dataframe).sum()
