"""
Reads a given file path into a dataframe
"""
import os

import pandas
import logging

from utils.custom_exceptions import UnsupportedFileExtension, FileNotFoundError
from utils.metadata_enums import FileExtension, FileSizeUnit
from pandas.util import hash_pandas_object

logger = logging.getLogger(__name__)


class FileHandler:
    def __init__(self):
        self.supported_extensions = {m.split('_')[-1]: getattr(self, m) for m in dir(self) if m.startswith('_handle')}
        self.ignored_extensions = [".metadata.json"]
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

        if self._should_file_be_ignored(file_path):
            return
        # Nothing in place to prevent reloading files for now
        handler = self.supported_extensions[extension]
        logger.debug(f"Loading file {file_path} using \"{handler.__name__}\" handler")
        self.loaded_files[file_path] = handler(file_path)

    def _should_file_be_ignored(self, path):
        return any(path.endswith(ignored_extension) for ignored_extension in self.ignored_extensions)

    @staticmethod
    def _handle_csv(file_path):
        dataframe = pandas.read_csv(file_path)
        return FileHandler.construct_file_descriptor(file_path, FileExtension.CSV, dataframe)

    @staticmethod
    def _handle_json(file_path):
        dataframe = pandas.read_json(file_path)
        return FileHandler.construct_file_descriptor(file_path, FileExtension.JSON, dataframe)

    @staticmethod
    def construct_file_descriptor(file_path: str, extension: FileExtension, dataframe: pandas.DataFrame):
        size = os.stat(file_path).st_size
        file_hash = FileHandler.get_dataframe_hash(dataframe)
        return {
            "file_path": file_path,
            "extension": extension,
            "dataframe": dataframe,
            "size": (size, FileSizeUnit.BYTE),
            "file_hash": file_hash
        }

    @staticmethod
    def get_dataframe_hash(dataframe: pandas.DataFrame):
        return hash_pandas_object(dataframe).sum()
